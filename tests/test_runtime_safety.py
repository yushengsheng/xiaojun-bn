"""Crash, lost reply, storage failure and repeat-start acceptance checks."""
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from api_clients import EvmClient, PreparedSignedTransaction
from exchange_binance_client import BinanceClient, UnconfirmedOperationError
from onchain_transaction_recovery import UnconfirmedTransactionError, resume_entry
from operation_journal import OperationJournal
from operation_review import resolve, snapshot
from process_lock import AlreadyRunningError, ProcessLease
from secret_box import SecretBox


class RuntimeSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="xiaojun-safety-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.box = SecretBox(self.root / ".key")
        self.journal = OperationJournal(self.root / "operations.json", secret_box=self.box)

    def exchange(self, journal=None):
        with patch.object(BinanceClient, "_build_session", return_value=Mock()):
            client = BinanceClient("test-key", "test-secret", operation_journal=journal or self.journal)
        self.addCleanup(client.close)
        return client

    def chain(self):
        client = EvmClient(operation_journal=self.journal)
        client.send_raw_transaction = Mock(return_value="0x" + "a" * 64)
        client.get_transaction_receipt = Mock(return_value=None)
        client.get_transaction_by_hash = Mock(return_value=None)
        return client

    def prepared(self, nonce=1, letter="a"):
        return PreparedSignedTransaction({"nonce": nonce, "gas": 21000, "gasPrice": 1}, "0x12", "0x" + letter * 64)

    def test_second_instance_is_blocked_until_first_exits(self):
        path = self.root / "desktop.lock"
        with ProcessLease(path):
            with self.assertRaises(AlreadyRunningError):
                ProcessLease(path).acquire()
        with ProcessLease(path):
            self.assertTrue(path.exists())

    def test_two_journal_objects_do_not_lose_concurrent_intents(self):
        other = OperationJournal(self.journal.file_path, secret_box=self.box)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda pair: pair[0].begin("exchange-funds", "scope", {"n": pair[1]}),
                          [(self.journal, 1), (other, 2)]))
        self.assertEqual(len(self.journal.active_entries()), 2)

    def test_backup_failure_prevents_financial_post(self):
        import stores
        original = stores._atomic_write_text
        def fail_backup(path, *args, **kwargs):
            if str(path).endswith(".bak"):
                raise OSError("disk full")
            return original(path, *args, **kwargs)
        client = self.exchange()
        client._request_impl = Mock()
        with patch("stores._atomic_write_text", side_effect=fail_backup), self.assertRaises(OSError):
            client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "USDT", "amount": "1"})
        client._request_impl.assert_not_called()
        self.assertEqual(len(self.journal.active_entries()), 1)

    def test_lost_funds_reply_blocks_new_client_after_restart(self):
        client = self.exchange()
        client._request_impl = Mock(side_effect=requests.ReadTimeout("lost"))
        with self.assertRaises(UnconfirmedOperationError):
            client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "USDT", "amount": "1"})
        other = self.exchange(OperationJournal(self.journal.file_path, secret_box=self.box))
        other._request_impl = Mock()
        with self.assertRaises(UnconfirmedOperationError):
            other.request(other.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "USDT", "amount": "1"})
        other._request_impl.assert_not_called()
        entry = self.journal.active_entries()[0]
        with self.assertRaises(ValueError):
            resolve(self.journal, entry["id"], "failed", "确认", "exchange history")
        resolve(self.journal, entry["id"], "failed", "已在交易所核对", "交易所历史无此划转，2026-10-08")
        other._request_impl.return_value = {"tranId": 123}
        other.request(other.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "USDT", "amount": "1"})
        other._request_impl.assert_called_once()
        self.assertEqual(self.journal.active_entries(), [])

    def test_exchange_rejection_does_not_set_uncertain_guard(self):
        client = self.exchange()
        client._request_impl = Mock(return_value={"success": False})
        with self.assertRaisesRegex(RuntimeError, "拒绝"):
            client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"amount": "1"})
        self.assertIsNone(client._uncertain_write_error)
        self.assertEqual(self.journal.active_entries(), [])

    def test_withdraw_history_clears_durable_and_memory_guard(self):
        client = self.exchange()
        client._request_impl = Mock(side_effect=requests.ReadTimeout("accepted but reply lost"))
        params = {"coin": "USDT", "address": "target", "network": "BSC", "amount": "2"}
        with self.assertRaises(UnconfirmedOperationError):
            client.request(client.spot, "POST", "/sapi/v1/capital/withdraw/apply", params)
        entry = self.journal.active_entries()[0]
        posted = entry["payload"]["params"]
        client.request_query = Mock(return_value=[{**posted, "status": 4, "id": "original"}])
        client._request_impl = Mock(return_value={"tranId": 123})
        client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "BNB", "amount": "1"})
        client._request_impl.assert_called_once()
        self.assertIsNone(client._uncertain_write_error)
        self.assertEqual(self.journal.active_entries(), [])

    def test_malformed_money_receipts_remain_unconfirmed(self):
        client = self.exchange()
        for receipt in ({"tranId": True}, {"tranId": []}, {"tranId": {}}, {"tranId": -1}, {"tranId": " "}, {"orderId": 123}):
            with self.subTest(receipt=receipt):
                client._request_impl = Mock(return_value=receipt)
                with self.assertRaises(UnconfirmedOperationError):
                    client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"amount": "1"})
                entry = self.journal.active_entries()[0]
                self.assertEqual(entry["state"], "unconfirmed")
                self.journal.update(entry["id"], "failed")  # Fixture never submitted a real request.

    def test_dust_requires_nonempty_identifiable_transfer_results(self):
        client = self.exchange()
        for rows in ([], [{}], [{"tranId": True}], "invalid"):
            with self.subTest(rows=rows):
                client._request_impl = Mock(return_value={"transferResult": rows})
                with self.assertRaises(UnconfirmedOperationError):
                    client.request(client.spot, "POST", "/sapi/v1/asset/dust", {"asset": ["ETH"]})
                entry = self.journal.active_entries()[0]
                self.assertEqual(entry["state"], "unconfirmed")
                self.journal.update(entry["id"], "failed")
        client._request_impl = Mock(return_value={"transferResult": [{"tranId": 123, "fromAsset": "ETH"}]})
        client.request(client.spot, "POST", "/sapi/v1/asset/dust", {"asset": ["ETH"]})
        self.assertEqual(self.journal.active_entries(), [])
        saved = self.journal.recovery_snapshot()["entries"][-1]["payload"]["receipt"]
        self.assertEqual(saved["transferResult"][0]["tranId"], 123)

    def test_withdraw_reconciliation_accepts_evm_case_only_and_rejects_other_address_changes(self):
        from exchange_funds_recovery import reconcile_funds_entry
        client = self.exchange()
        for address, returned, expected in (("0x" + "A" * 40, "0x" + "a" * 40, True),
                                             ("CaseSensitiveAddress", "casesensitiveaddress", False),
                                             ("0x" + "a" * 40, "0x" + "b" * 40, False)):
            entry_id = self.journal.begin("exchange-funds", "scope", {"path": "/sapi/v1/capital/withdraw/apply",
                "params": {"coin": "USDT", "address": address, "amount": "1", "withdrawOrderId": "original"}, "started_ms": 1000})
            entry = next(e for e in self.journal.active_entries() if e["id"] == entry_id)
            client.request_query = Mock(return_value=[{"coin": "USDT", "address": returned, "amount": "1", "withdrawOrderId": "original", "id": "receipt", "status": 6}])
            if expected:
                self.assertTrue(reconcile_funds_entry(self.journal, client, entry))
            else:
                with self.assertRaisesRegex(RuntimeError, "不符"):
                    reconcile_funds_entry(self.journal, client, entry)
                self.assertEqual(next(e for e in self.journal.active_entries() if e["id"] == entry_id)["state"], "prepared")

    def test_convert_reconciliation_requires_receipt_and_matching_quote_when_returned(self):
        from exchange_funds_recovery import reconcile_funds_entry
        client = self.exchange()
        entry_id = self.journal.begin("exchange-funds", "scope", {"path": "/sapi/v1/convert/acceptQuote", "params": {"quoteId": "original"}})
        entry = self.journal.active_entries()[0]
        client.get_convert_order_status = Mock(return_value={"orderStatus": "SUCCESS"})
        self.assertFalse(reconcile_funds_entry(self.journal, client, entry))
        client.get_convert_order_status.return_value = {"orderStatus": "SUCCESS", "orderId": 123, "quoteId": "other"}
        with self.assertRaisesRegex(RuntimeError, "不符"):
            reconcile_funds_entry(self.journal, client, entry)
        client.get_convert_order_status.return_value = {"orderStatus": "SUCCESS", "orderId": 123}
        self.assertTrue(reconcile_funds_entry(self.journal, client, entry))
        self.assertEqual(self.journal.active_entries(), [])

    def test_processing_convert_remains_protected_after_restart_until_terminal_receipt(self):
        client = self.exchange()
        client._request_impl = Mock(return_value={"orderId": 123, "orderStatus": "PROCESS"})
        client.accept_convert_quote("original")
        self.assertEqual(self.journal.active_entries()[0]["state"], "submitted")
        other = self.exchange(OperationJournal(self.journal.file_path, secret_box=self.box))
        other._request_impl = Mock(return_value={"orderId": 123, "orderStatus": "PROCESS"})
        with self.assertRaises(UnconfirmedOperationError):
            other.request(other.spot, "POST", "/sapi/v1/asset/transfer", {"amount": "1"})
        self.assertTrue(all(call.args[1] == "GET" for call in other._request_impl.call_args_list))
        other._request_impl.return_value = {"orderId": 456, "orderStatus": "SUCCESS"}
        with self.assertRaisesRegex(RuntimeError, "编号"):
            other.get_convert_order_status(quote_id="original")
        self.assertEqual(self.journal.active_entries()[0]["state"], "submitted")
        other._request_impl.return_value = {"orderId": 123, "orderStatus": "SUCCESS"}
        other.get_convert_order_status(quote_id="original")
        self.assertEqual(self.journal.active_entries(), [])
        other._request_impl.return_value = {"tranId": 456}
        other.request(other.spot, "POST", "/sapi/v1/asset/transfer", {"amount": "1"})
        self.assertEqual(other._request_impl.call_args.args[1], "POST")

    def test_empty_or_wrong_rpc_ack_is_not_success(self):
        for ack in (None, "0x", "0x" + "b" * 64):
            client = self.chain()
            client.send_raw_transaction.return_value = ack
            with self.assertRaises(UnconfirmedTransactionError):
                client.submit_prepared_transaction("BSC", self.prepared(), source_address="source", nonce=1, recovery_timeout_seconds=1)
            self.assertEqual(self.journal.active_entries()[0]["state"], "unconfirmed")

    def test_lost_rpc_reply_uses_transaction_lookup(self):
        client = self.chain()
        client.send_raw_transaction.side_effect = requests.ReadTimeout("lost")
        client.get_transaction_by_hash.return_value = {"hash": self.prepared().tx_hash}
        result = client.submit_prepared_transaction("BSC", self.prepared(), source_address="source", nonce=1, recovery_timeout_seconds=1)
        self.assertTrue(result.recovered)
        self.assertFalse(result.acknowledged)
        client.get_transaction_by_hash.assert_called_once()

    def test_retry_reuses_original_signed_bytes_and_nonce(self):
        client = self.chain()
        with client.transaction_context(batch_id="batch", task_key="task"):
            client.send_raw_transaction.side_effect = requests.ReadTimeout("lost")
            with self.assertRaises(UnconfirmedTransactionError):
                client.submit_prepared_transaction("BSC", self.prepared(), source_address="source", nonce=1, recovery_timeout_seconds=1)
            client.send_raw_transaction.side_effect = None
            result = client.submit_prepared_transaction("BSC", self.prepared(nonce=2, letter="b"), source_address="source", nonce=2)
        self.assertEqual(result.tx_hash, self.prepared().tx_hash)
        self.assertEqual(result.nonce, 1)
        self.assertEqual(client.send_raw_transaction.call_args.args[1], "0x12")

    def test_other_task_cannot_bypass_unknown_wallet_transaction(self):
        client = self.chain()
        client.send_raw_transaction.return_value = None
        with self.assertRaises(UnconfirmedTransactionError):
            client.submit_prepared_transaction("BSC", self.prepared(), source_address="source", nonce=1, recovery_timeout_seconds=1)
        client.send_raw_transaction.reset_mock()
        with self.assertRaises(UnconfirmedTransactionError):
            client.submit_prepared_transaction("BSC", self.prepared(nonce=2), source_address="source", nonce=2)
        client.send_raw_transaction.assert_not_called()

    def test_stop_is_preserved_when_resuming_original_transaction(self):
        client = self.chain()
        client.submit_prepared_transaction("BSC", self.prepared(), source_address="source", nonce=1)
        stop = threading.Event(); stop.set()
        client.send_raw_transaction.reset_mock()
        with client.transaction_context(stop_event=stop), self.assertRaises(UnconfirmedTransactionError):
            resume_entry(client, self.journal.active_entries()[0])
        client.send_raw_transaction.assert_not_called()

    def test_restore_guard_survives_reload_and_requires_exact_confirmation(self):
        self.journal.begin("exchange-funds", "scope", {"amount": "1"})
        self.journal.file_path.unlink()
        with self.assertRaises(RuntimeError):
            self.journal.active_entries()
        other = OperationJournal(self.journal.file_path, secret_box=self.box)
        self.assertTrue(snapshot(other)["recovery_required"])
        with self.assertRaises(ValueError):
            other.acknowledge_recovery("确认")
        other.acknowledge_recovery("已核对全部操作")
        self.assertEqual(len(other.active_entries()), 1)

    def test_withdraw_network_validation_precedes_post(self):
        client = self.exchange()
        client.spot_asset_balance_decimal = Mock(return_value=10)
        client.request_query = Mock(return_value=[{"coin": "USDT", "networkList": [{"network": "BSC", "withdrawEnable": False}]}])
        client._request_impl = Mock()
        with self.assertRaisesRegex(RuntimeError, "不可提现"):
            client.withdraw_all_coin("USDT", "target", "BSC")
        client._request_impl.assert_not_called()

    def test_config_second_file_failure_restores_both_files(self):
        import stores
        from config_transaction import update_group, recover
        first, second, marker = [self.root / n for n in ("accounts.json", "settings.json", "pending.json")]
        with patch.object(stores, "SECRET_BOX", self.box):
            stores._save_authenticated_config(first, {"value": "old-account"})
            stores._save_authenticated_config(second, {"value": "old-settings"})
            before = {p: p.read_bytes() for p in (first, second, Path(str(first)+".bak"), Path(str(second)+".bak"))}
            with self.assertRaises(OSError):
                with update_group(marker, [first, second]):
                    stores._save_authenticated_config(first, {"value": "new-account"})
                    raise OSError("settings save failed")
            self.assertEqual({p: p.read_bytes() for p in before}, before)
            self.assertFalse(marker.exists())
            recover(marker, [first, second])

    def test_interrupted_config_group_restores_on_next_start(self):
        import stores
        from config_transaction import update_group, recover
        first, second, marker = [self.root / n for n in ("accounts.json", "settings.json", "pending.json")]
        class SimulatedPowerLoss(BaseException):
            pass
        with patch.object(stores, "SECRET_BOX", self.box):
            stores._save_authenticated_config(first, {"value": "old-account"})
            stores._save_authenticated_config(second, {"value": "old-settings"})
            paths = (first, second, Path(str(first) + ".bak"), Path(str(second) + ".bak"))
            before = {p: p.read_bytes() for p in paths}
            with self.assertRaises(SimulatedPowerLoss):
                with update_group(marker, [first, second]):
                    stores._save_authenticated_config(first, {"value": "new-account"})
                    raise SimulatedPowerLoss()
            self.assertTrue(marker.exists())
            recover(marker, [first, second])
            self.assertEqual({p: p.read_bytes() for p in paths}, before)
            self.assertFalse(marker.exists())
            self.assertFalse(Path(str(marker) + ".bak").exists())

    def test_withdraw_rounds_to_current_network_step_and_saves_receipt(self):
        from decimal import Decimal
        client = self.exchange()
        client.spot_asset_balance_decimal = Mock(return_value=Decimal("10.129"))
        client.request_query = Mock(return_value=[{"coin": "USDT", "networkList": [{"network": "BSC", "withdrawEnable": True, "withdrawIntegerMultiple": "0.01", "withdrawMin": "1", "withdrawFee": "0.1"}]}])
        client._request_impl = Mock(return_value={"id": "withdraw-receipt"})
        result = client.withdraw_all_coin("USDT", "target", "BSC", fee_buffer=0)
        self.assertEqual(result, 10.12)
        posted = client._request_impl.call_args.args[3]
        self.assertEqual(posted["amount"], "10.12")
        self.assertTrue(posted["withdrawOrderId"].startswith("xjf"))
        self.assertEqual(self.journal.active_entries(), [])

    def test_rule_cache_refreshes_after_five_minutes(self):
        client = self.exchange()
        client.public_get = Mock(side_effect=[{"symbols": [{"symbol": "BTCUSDT", "version": 1}]}, {"symbols": [{"symbol": "BTCUSDT", "version": 2}]}])
        with patch("exchange_binance_client.time.monotonic", return_value=100):
            self.assertEqual(client.get_exchange_info("BTCUSDT")["version"], 1)
            self.assertEqual(client.get_exchange_info("BTCUSDT")["version"], 1)
        with patch("exchange_binance_client.time.monotonic", return_value=401):
            self.assertEqual(client.get_exchange_info("BTCUSDT")["version"], 2)
        self.assertEqual(client.public_get.call_count, 2)

    def test_incomplete_install_retry_refuses_runtime_data_and_service_phase(self):
        import argparse, json
        from contextlib import ExitStack
        from linux_exchange import install
        args = argparse.Namespace(source="unused", host="8.8.8.8", port=26483, backend_port=18880)
        roots = {name: self.root / name.lower() for name in ("ROOT", "CONFIG", "DATA", "UNITS")}
        for p in roots.values(): p.mkdir()
        marker = roots["ROOT"] / ".install-incomplete.json"
        saved = dict(version="v1.2.3", host=args.host, port=args.port, backend_port=args.backend_port, phase="bootstrap")
        marker.write_text(json.dumps(saved))
        data = roots["DATA"] / "accounts.json";data.write_text("protected")
        with ExitStack() as stack:
            for name, p in roots.items(): stack.enter_context(patch.object(install, name, p))
            stack.enter_context(patch.object(install.platform, "system", return_value="Linux"))
            stack.enter_context(patch.object(install.os, "geteuid", return_value=0, create=True))
            stack.enter_context(patch.object(install, "verify_payload", return_value=("v1.2.3", {})))
            with self.assertRaisesRegex(ValueError, "运行数据"):
                install.retry_incomplete(args)
            self.assertEqual(data.read_text(), "protected")
            data.unlink(); saved["phase"] = "services";marker.write_text(json.dumps(saved))
            with self.assertRaisesRegex(ValueError, "服务配置阶段"):
                install.retry_incomplete(args)
            saved["phase"] = "bootstrap";marker.write_text(json.dumps(saved))
            install.retry_incomplete(args)
            self.assertFalse(roots["ROOT"].exists())
            self.assertTrue(roots["UNITS"].exists())

    def test_uncertain_funds_do_not_block_cleanup_sell(self):
        from tests.test_exchange_execution_safety import _Client
        client = _Client()
        client._uncertain_write_error = UnconfirmedOperationError("funds pending")
        client.journal.begin("exchange-funds", client._order_scope(), {"path": "/sapi/v1/asset/transfer"})
        with client.cleanup_requests_after_stop():
            order = client.request(client.spot, "POST", "/api/v3/order", {"symbol": "BTCUSDT", "type": "MARKET", "side": "SELL", "quantity": "1"})
        self.assertEqual(order["status"], "FILLED")
        self.assertEqual(len(client.journal.active_entries()), 1)
        self.assertIsNotNone(client._uncertain_write_error)


if __name__ == "__main__":
    unittest.main()
