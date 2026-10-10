"""Crash, lost reply, storage failure and repeat-start acceptance checks."""
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from exchange_binance_client import BinanceClient, ExchangeOperationFailedError
from operation_journal import OperationJournal
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

    def test_live_funds_submission_does_not_falsely_block_same_account_client(self):
        first, second = self.exchange(), self.exchange(OperationJournal(self.journal.file_path, secret_box=self.box))
        started, release, second_started = threading.Event(), threading.Event(), threading.Event()
        def post(*_args, **_kwargs):
            started.set()
            if not release.wait(3):
                raise AssertionError("test release missing")
            return {"tranId": 1}
        first._request_impl = Mock(side_effect=post)
        second._request_impl = Mock(return_value={"tranId": 2})
        def run(client, is_second=False):
            if is_second:
                second_started.set()
            return client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"asset": "USDT", "amount": "1"})
        with ThreadPoolExecutor(max_workers=2) as pool:
            future1 = pool.submit(run, first)
            try:
                self.assertTrue(started.wait(2))
                future2 = pool.submit(run, second, True)
                self.assertTrue(second_started.wait(2))
                second._request_impl.assert_not_called()
            finally:
                release.set()
            self.assertEqual(future1.result(), {"tranId": 1})
            self.assertEqual(future2.result(), {"tranId": 2})
        self.assertEqual(self.journal.active_entries(), [])

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


    def test_exchange_rejection_does_not_set_uncertain_guard(self):
        client = self.exchange()
        client._request_impl = Mock(return_value={"success": False})
        with self.assertRaisesRegex(RuntimeError, "拒绝"):
            client.request(client.spot, "POST", "/sapi/v1/asset/transfer", {"amount": "1"})
        self.assertFalse(hasattr(client, "_uncertain_write_error"))
        self.assertEqual(self.journal.active_entries(), [])


    def test_every_funds_module_new_action_cannot_reuse_completed_receipt(self):
        from exchange_funds_recovery import FUNDS_WRITE_PATHS, _RECEIPT_KEYS
        for index, path in enumerate(sorted(FUNDS_WRITE_PATHS)):
            with self.subTest(path=path):
                journal = OperationJournal(self.root / f"funds-new-{index}.json", secret_box=self.box)
                client = self.exchange(journal)
                if path.endswith("/dust"):
                    replies = [{"transferResult": [{"tranId": number}]} for number in (1, 2)]
                else:
                    replies = [{_RECEIPT_KEYS[path]: number, "orderStatus": "SUCCESS"} for number in (1, 2)]
                client._request_impl = Mock(side_effect=replies)
                for number in (1, 2):
                    client.request(client.spot, "POST", path, {"asset": "USDT", "amount": str(number), "quoteId": f"quote-{number}"})
                self.assertEqual(client._request_impl.call_count, 2)
                entries = journal.recovery_snapshot()["entries"]
                self.assertEqual(len(entries), 2)
                self.assertEqual(len({entry["id"] for entry in entries}), 2)
                self.assertTrue(all(entry["state"] == "completed" for entry in entries))



    def test_order_identity_preserves_slash_symbol_query_compatibility(self):
        client = self.exchange()
        order = {"symbol": "BTCETH", "orderId": 123, "clientOrderId": "original", "status": "FILLED"}
        client.request_query = Mock(return_value=order)
        for market in ("spot", "um_futures"):
            with self.subTest(market=market):
                self.assertEqual(client.get_order_by_client_id(" btc / eth ", "original", market=market), order)
                self.assertEqual(client.request_query.call_args.args[3]["symbol"], "BTCETH")
        entry_id = self.journal.begin("exchange-order", client._order_scope(), {"symbol": "BTCETH"})
        client.record_order_state("/api/v3/order", order, entry_id=entry_id,
                                  request_params={"symbol": " btc / eth ", "origClientOrderId": "original"})
        self.assertEqual(self.journal.active_entries(), [])



    def test_dust_requires_nonempty_identifiable_transfer_results(self):
        client = self.exchange()
        for rows in ([], [{}], [{"tranId": True}], "invalid"):
            with self.subTest(rows=rows):
                client._request_impl = Mock(return_value={"transferResult": rows})
                with self.assertRaises(ExchangeOperationFailedError):
                    client.request(client.spot, "POST", "/sapi/v1/asset/dust", {"asset": ["ETH"]})
                entry = self.journal.recovery_snapshot()["entries"][-1]
                self.assertEqual(entry["state"], "failed")
                self.assertEqual(client._request_impl.call_count, 2)
        client._request_impl = Mock(return_value={"transferResult": [{"tranId": 123, "fromAsset": "ETH"}]})
        client.request(client.spot, "POST", "/sapi/v1/asset/dust", {"asset": ["ETH"]})
        self.assertEqual(self.journal.active_entries(), [])
        saved = self.journal.recovery_snapshot()["entries"][-1]["payload"]["receipt"]
        self.assertEqual(saved["transferResult"][0]["tranId"], 123)






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
            for path in (first, second, Path(str(first)+".bak"), Path(str(second)+".bak")):
                path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
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
            # Existing files may mix Windows CRLF and LF from earlier versions.
            for path in paths:
                data = path.read_bytes().replace(b"\r\n", b"\n")
                path.write_bytes(data.replace(b"\n", b"\r\n") if path in (first, paths[2]) else data)
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



if __name__ == "__main__":
    unittest.main()
