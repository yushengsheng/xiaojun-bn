"""Real signing + durable journal acceptance for sequential transfer identities."""
import copy
import tempfile
import threading
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from api_clients import EvmClient, RpcTransactionRejected
from core_models import EvmToken, GeneratedWalletEntry, WithdrawRuntimeParams
from onchain_relay_runner import _RelayBatchItem, _pick_or_create_relay_records, _relay_batch_id, _relay_submit_with_retry, _relay_resume_pending_transactions, run_relay_fee_recovery
from onchain_relay_wallets import RelayWalletFileStore
from onchain_transaction_recovery import UnconfirmedTransactionError, verify_transfer_receipt
from operation_journal import OperationJournal
from secret_box import SecretBox
from onchain_transfer_runner import OnchainTransferRunnerMixin
from tests.test_onchain_relay_pipeline import _FakeOwner, _close_sweep_log_after


CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
TARGET = "0x" + "2" * 40
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class _DirectOwner(OnchainTransferRunnerMixin, _FakeOwner):
    MODE_1M = "1对多"

    def __init__(self, client, journal):
        super().__init__()
        self.client, self.operation_journal = client, journal
        self.wallet_cache_lock = threading.Lock()
        self.source_private_key_cache, self.source_address_cache = {}, {}

    def _start_rpc_worker_thread(self, target, **_kwargs):
        thread = threading.Thread(target=target)
        thread.start()
        return thread

    def _row_number_by_key_snapshot(self):
        return {}

    def _token_desc_from_params(self, params):
        return params.coin

    def _row_context_for_values(self, key, source, target):
        return f"{key}:{source}:{target}"

    def _mark_row_status_context(self, *_args):
        pass

    def _begin_progress(self, *_args):
        pass

    def _set_progress_metrics(self, **_kwargs):
        pass

    def _finish_progress(self, *_args):
        pass

    def _estimated_gas_fee_text(self, network, amount):
        return self._gas_fee_amount_text(network, amount)

    def _success_status_text(self, coin, amount):
        return f"{amount} {coin}"


class TransferIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="xiaojun-transfer-identity-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.box = SecretBox(root / ".key")
        self.journal = OperationJournal(root / "operations.json", secret_box=self.box)
        self.client = EvmClient(operation_journal=self.journal)
        self.addCleanup(self.client.close)
        self.client.get_erc20_balance = Mock(return_value=0)
        self.client.send_raw_transaction = Mock(side_effect=lambda network, raw: "0x" + bytes(self.client._ensure_eth_utils().keccak(hexstr=raw)).hex())
        self.client.get_transaction_receipt = Mock(return_value=None)
        self.client.get_transaction_by_hash = Mock(return_value=None)
        key = "1" * 64
        self.source = self.client.address_from_private_key(key)
        self.store = RelayWalletFileStore(root / "relay.txt")
        self.patch_box = patch("onchain_relay_wallets.SECRET_BOX", self.box)
        self.patch_box.start()
        self.addCleanup(self.patch_box.stop)
        self.record = self.store.build_record(
            batch_id="original-batch", network="BSC", source_address=self.source,
            target_address=TARGET, relay_wallet=GeneratedWalletEntry(self.source, key),
            token=EvmToken("BNB", "", 18, True), relay_fee_reserve=Decimal("0.0001"),
            sweep_enabled=False, sweep_target=self.source,
        )
        self.store.append_records([self.record])
        self.owner = SimpleNamespace(client=self.client, relay_wallet_store=self.store,
                                     stop_requested=threading.Event(), _decimal_to_text=str,
                                     _dispatch_ui=lambda callback: callback(), log=lambda _: None)
        self.params = WithdrawRuntimeParams(coin="USDT", amount="20", network="BSC", delay=0,
                                            threads=3, mode="1对多", token_is_native=False,
                                            token_contract=CONTRACT, token_decimals=18,
                                            receipt_retry_count=1)

    def prepare(self, *, native=False, nonce=1, amount=20, target=TARGET):
        if native:
            return self.client.prepare_native_transfer("BSC", "1" * 64, target, amount, nonce, 50_000_000, 21_000)
        return self.client.prepare_erc20_transfer("BSC", "1" * 64, CONTRACT, target, amount * 10**18, nonce, 50_000_000, 70_000)

    def submit(self, prepared, record=None):
        record = record or self.record
        item = _RelayBatchItem(1, "row", "test", TARGET, "1" * 64, self.source,
                               record, "1" * 64, "test", "B1", "C1", "A", "20", 20 * 10**18)
        return _relay_submit_with_retry(self.owner, self.params, item, "中转第二跳",
            lambda: self.client.submit_prepared_transaction("BSC", prepared, source_address=self.source,
                                                            nonce=prepared.tx["nonce"]))

    def mark_completed(self):
        entry = self.journal.active_entries(kind="onchain-transfer")[0]
        self.journal.update(entry["id"], "completed")
        self.record = self.store.update_record(self.source, batch_id=self.record.batch_id,
                                               status="completed", token_forward_txid=entry["payload"]["tx_hash"])

    def reuse(self):
        rows, _, batch_id = _pick_or_create_relay_records(self.owner, [("row", "test", TARGET)],
            self.params, {"row": ("1" * 64, self.source)}, {(self.source.lower(), TARGET.lower()): [self.record]})
        return rows["row"], batch_id

    def test_bnb_then_usdt_reuses_wallet_but_broadcasts_new_payment(self):
        old_hash = self.submit(self.prepare(native=True))
        self.mark_completed()
        reused, batch_id = self.reuse()
        # Reload the encrypted wallet/journal, as after restarting the desktop app.
        reloaded = RelayWalletFileStore(self.store.file_path).load_records()[0]
        self.assertEqual(reloaded.batch_id, batch_id)
        self.assertNotEqual(batch_id, "original-batch")
        self.assertEqual(reloaded.relay_address, self.record.relay_address)
        self.assertEqual(reloaded.token_forward_txid, "")
        self.assertEqual(reloaded.private_key, self.record.private_key)
        self.client.operation_journal = OperationJournal(self.journal.file_path, secret_box=self.box)
        new_hash = self.submit(self.prepare(nonce=2), reused)
        self.assertNotEqual(old_hash, new_hash)
        self.assertEqual(self.client.send_raw_transaction.call_count, 2)
        saved = self.journal.batch_task_states(batch_id, kind="onchain-transfer")
        self.assertEqual(next(iter(saved.values()))["payload"]["tx"]["to"].lower(), CONTRACT)

    def test_same_coin_new_run_cannot_return_previous_receipt(self):
        old_hash = self.submit(self.prepare())
        self.mark_completed()
        reused, _ = self.reuse()
        new_hash = self.submit(self.prepare(nonce=2), reused)
        self.assertNotEqual(old_hash, new_hash)
        self.assertEqual(self.client.send_raw_transaction.call_count, 2)

    def test_same_task_with_changed_asset_amount_or_recipient_is_blocked(self):
        self.submit(self.prepare())
        self.mark_completed()
        self.client.send_raw_transaction.reset_mock()
        for prepared in (self.prepare(native=True, nonce=2), self.prepare(amount=21, nonce=2),
                         self.prepare(target="0x" + "3" * 40, nonce=2)):
            with self.subTest(tx=prepared.tx), self.assertRaisesRegex(UnconfirmedTransactionError, "不同的转账内容"):
                self.submit(prepared)
        self.client.send_raw_transaction.assert_not_called()

    def test_same_task_on_another_network_cannot_recover_old_chain_payment(self):
        self.submit(self.prepare())
        self.mark_completed()
        self.client.send_raw_transaction.reset_mock()
        with self.client.transaction_context(batch_id="original-batch", task_key=f"{self.source.lower()}:中转第二跳"):
            with self.assertRaisesRegex(UnconfirmedTransactionError, "不同的转账内容"):
                self.client.submit_prepared_transaction("ETH", self.prepare(nonce=2), source_address=self.source, nonce=2)
        self.client.send_raw_transaction.assert_not_called()

    def test_completed_wallet_with_stranded_tokens_is_not_reset_or_refunded(self):
        self.submit(self.prepare())
        self.mark_completed()
        self.client.send_raw_transaction.reset_mock()
        self.client.get_erc20_balance.return_value = 20 * 10**18
        before = self.store.file_path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "仍有.*余额"):
            self.reuse()
        self.assertEqual(self.store.file_path.read_bytes(), before)
        self.client.send_raw_transaction.assert_not_called()

    def test_unfinished_task_preserves_identity_and_signed_bytes(self):
        original = self.submit(self.prepare())
        self.record = self.store.update_record(self.source, batch_id=self.record.batch_id, status="failed")
        reused, _ = self.reuse()
        self.assertEqual(reused.batch_id, "original-batch")
        self.assertEqual(self.submit(self.prepare(nonce=2), reused), original)
        calls = self.client.send_raw_transaction.call_args_list
        self.assertEqual(calls[0].args, calls[1].args)

    def test_batch_identity_is_unique_even_in_same_millisecond(self):
        with patch("onchain_relay_runner.time.time", return_value=1):
            self.assertEqual(len({_relay_batch_id(3) for _ in range(100)}), 100)

    def test_successful_receipt_requires_matching_token_transfer(self):
        prepared = self.prepare()
        receipt = {"status": "0x1", "transactionHash": prepared.tx_hash, "from": self.source,
                   "to": CONTRACT, "logs": [{"address": CONTRACT, "topics": [TRANSFER_TOPIC,
                       "0x" + self.source[2:].lower().rjust(64, "0"), "0x" + TARGET[2:].rjust(64, "0")],
                       "data": f"0x{20 * 10**18:064x}"}]}
        verify_transfer_receipt(receipt, prepared.tx, self.source, tx_hash=prepared.tx_hash)
        mutations = [lambda r: r.update(to=TARGET, logs=[]), lambda r: r.update(logs=[]),
                     lambda r: r.update({"from": TARGET}),
                     lambda r: r.update(transactionHash="0x" + "b" * 64),
                     lambda r: r["logs"][0].update(data=f"0x{21 * 10**18:064x}"),
                     lambda r: r["logs"][0].update(address=TARGET),
                     lambda r: r["logs"][0]["topics"].__setitem__(2, "0x" + self.source[2:].lower().rjust(64, "0")),
                     lambda r: r["logs"][0].update(removed=True)]
        for mutate in mutations:
            bad = copy.deepcopy(receipt)
            mutate(bad)
            with self.subTest(receipt=bad), self.assertRaises(RuntimeError):
                verify_transfer_receipt(bad, prepared.tx, self.source, tx_hash=prepared.tx_hash)

    def test_mismatched_receipt_cannot_complete_durable_journal(self):
        prepared = self.prepare()
        self.submit(prepared)
        # Restore the production receipt reader; feed a successful but unrelated RPC reply.
        self.client.get_transaction_receipt = EvmClient.get_transaction_receipt.__get__(self.client)
        self.client._rpc_call = Mock(return_value={"status": "0x1", "transactionHash": prepared.tx_hash,
                                                   "from": self.source, "to": TARGET, "logs": []})
        with self.assertRaises(RuntimeError):
            self.client.get_transaction_receipt("BSC", prepared.tx_hash)
        self.assertEqual(self.journal.active_entries()[0]["state"], "submitted")

    def test_concurrent_source_tasks_do_not_mistake_live_submission_for_lost_reply(self):
        started, release, second_started = threading.Event(), threading.Event(), threading.Event()
        other = EvmClient(operation_journal=OperationJournal(self.journal.file_path, secret_box=self.box))
        self.addCleanup(other.close)
        first = self.prepare(native=True, nonce=1)
        second = self.prepare(native=True, nonce=2)
        def broadcast(_network, raw):
            started.set()
            if not release.wait(3):
                raise AssertionError("test release missing")
            return first.tx_hash
        self.client.send_raw_transaction.side_effect = broadcast
        other.send_raw_transaction = Mock(return_value=second.tx_hash)
        def run(client, prepared, key):
            if key == "second":
                second_started.set()
            with client.transaction_context(batch_id="parallel", task_key=key):
                return client.submit_prepared_transaction("BSC", prepared, source_address=self.source, nonce=prepared.tx["nonce"])
        with ThreadPoolExecutor(max_workers=2) as pool:
            future1 = pool.submit(run, self.client, first, "first")
            try:
                self.assertTrue(started.wait(2))
                future2 = pool.submit(run, other, second, "second")
                self.assertTrue(second_started.wait(2))
                other.send_raw_transaction.assert_not_called()
            finally:
                release.set()
            self.assertEqual(future1.result().tx_hash, first.tx_hash)
            self.assertEqual(future2.result().tx_hash, second.tx_hash)
        self.assertEqual(len(self.journal.active_entries()), 2)

    def test_recovery_after_lost_reply_reuses_saved_bytes_without_new_balance_quote(self):
        prepared = self.prepare()
        self.client.send_raw_transaction.side_effect = TimeoutError("lost reply")
        with self.assertRaises(UnconfirmedTransactionError):
            self.submit(prepared)
        self.client.operation_journal = OperationJournal(self.journal.file_path, secret_box=self.box)
        self.client.send_raw_transaction.side_effect = None
        self.client.send_raw_transaction.return_value = prepared.tx_hash
        self.client.get_transaction_receipt = EvmClient.get_transaction_receipt.__get__(self.client)
        receipt = {"status": "0x1", "transactionHash": prepared.tx_hash, "from": self.source,
                   "to": CONTRACT, "logs": [{"address": CONTRACT, "topics": [TRANSFER_TOPIC,
                       "0x" + self.source[2:].lower().rjust(64, "0"), "0x" + TARGET[2:].rjust(64, "0")],
                       "data": f"0x{20 * 10**18:064x}"}]}
        self.client._rpc_call = Mock(side_effect=[None, receipt])
        self.client.get_nonce = Mock(side_effect=AssertionError("must keep saved nonce"))
        _relay_resume_pending_transactions(self.owner, "BSC", self.source, dry_run=False, timeout_seconds=1)
        calls = self.client.send_raw_transaction.call_args_list
        self.assertEqual(calls[0].args, calls[1].args)
        self.assertEqual(self.journal.active_entries(), [])

    def test_empty_relay_with_pending_signed_incoming_payment_keeps_key_and_journal(self):
        for native in (True, False):
            with self.subTest(native=native):
                sender_key = "2" * 64
                sender = self.client.address_from_private_key(sender_key)
                if native:
                    prepared = self.client.prepare_native_transfer("BSC", sender_key, self.source, 10**18, 1, 50_000_000, 21_000)
                else:
                    prepared = self.client.prepare_erc20_transfer("BSC", sender_key, CONTRACT, self.source, 20 * 10**18, 1, 50_000_000, 70_000)
                self.client.send_raw_transaction.side_effect = TimeoutError("funding reply lost")
                with self.assertRaises(UnconfirmedTransactionError):
                    self.client.submit_prepared_transaction("BSC", prepared, source_address=sender, nonce=1)
                before = self.journal.file_path.read_bytes()
                owner = _DirectOwner(self.client, self.journal)
                owner.relay_wallet_store = self.store
                self.client.get_balance_wei = Mock(return_value=0)
                root = Path(self.temp.name)
                with patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", root / "manual.txt"), patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", root / "sweep.jsonl"), _close_sweep_log_after():
                    run_relay_fee_recovery(owner, False, use_progress=False, manage_running_state=False)
                self.assertEqual(self.store.load_records(), [self.record])
                self.assertEqual(self.journal.file_path.read_bytes(), before)
                self.client.get_balance_wei.assert_not_called()
                self.assertIn("确认中 1", owner.result_dialogs[0]["detail_text"])
                for entry in self.journal.active_entries():
                    self.journal.update(entry["id"], "completed")

    def test_unresolved_signed_outgoing_payment_prevents_empty_relay_cleanup(self):
        prepared = self.prepare(native=True)
        self.client.send_raw_transaction.side_effect = TimeoutError("outgoing reply lost")
        with self.assertRaises(UnconfirmedTransactionError):
            self.submit(prepared)
        self.client.get_transaction_receipt = Mock(return_value={"status": "0x0", "transactionHash": prepared.tx_hash})
        self.client.get_balance_wei = Mock(return_value=0)
        owner = _DirectOwner(self.client, self.journal)
        owner.relay_wallet_store = self.store
        root = Path(self.temp.name)
        before = self.journal.file_path.read_bytes()
        with patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", root / "manual.txt"), patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", root / "sweep.jsonl"), _close_sweep_log_after():
            run_relay_fee_recovery(owner, False, use_progress=False, manage_running_state=False)
        records = self.store.load_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].private_key, self.record.private_key)
        self.assertEqual(records[0].status, self.record.status)
        self.assertEqual(self.journal.file_path.read_bytes(), before)

    def test_old_recovery_without_batch_metadata_can_resume_exact_signed_transaction(self):
        prepared = self.prepare(native=True)
        self.client.send_raw_transaction.side_effect = TimeoutError("recovery reply lost")
        with self.assertRaises(UnconfirmedTransactionError):
            self.client.submit_prepared_transaction("BSC", prepared, source_address=self.source, nonce=1)
        self.client.send_raw_transaction.side_effect = None
        self.client.send_raw_transaction.return_value = prepared.tx_hash
        self.client.get_transaction_receipt = EvmClient.get_transaction_receipt.__get__(self.client)
        self.client._rpc_call = Mock(side_effect=[None, {"status": "0x1", "transactionHash": prepared.tx_hash,
                                                      "from": self.source, "to": TARGET, "logs": []}])
        _relay_resume_pending_transactions(self.owner, "BSC", self.source, dry_run=False, timeout_seconds=1)
        calls = self.client.send_raw_transaction.call_args_list
        self.assertEqual(calls[0].args, calls[1].args)
        self.assertEqual(self.journal.active_entries(), [])

    def test_new_batch_cannot_claim_identical_uncertain_transaction_without_metadata(self):
        prepared = self.prepare(native=True)
        self.client.send_raw_transaction.side_effect = TimeoutError("reply lost")
        with self.assertRaises(UnconfirmedTransactionError):
            self.client.submit_prepared_transaction("BSC", prepared, source_address=self.source, nonce=1)
        self.client.send_raw_transaction.reset_mock()
        with self.client.transaction_context(batch_id="new-batch", task_key="new-task"):
            with self.assertRaises(UnconfirmedTransactionError):
                self.client.submit_prepared_transaction("BSC", prepared, source_address=self.source, nonce=1)
        self.client.send_raw_transaction.assert_not_called()

    def test_definite_new_sweep_rejection_allows_smaller_value_same_nonce(self):
        first = self.prepare(native=True, amount=30)
        self.client.send_raw_transaction.side_effect = RpcTransactionRejected("insufficient funds")
        with self.assertRaises(RpcTransactionRejected):
            self.client.submit_prepared_transaction("BSC", first, source_address=self.source, nonce=1)
        self.assertEqual(self.journal.active_entries(), [])
        smaller = self.prepare(native=True, amount=20)
        self.client.send_raw_transaction.side_effect = None
        self.client.send_raw_transaction.return_value = smaller.tx_hash
        result = self.client.submit_prepared_transaction("BSC", smaller, source_address=self.source, nonce=1)
        self.assertEqual(result.tx_hash, smaller.tx_hash)
        self.assertEqual(self.client.send_raw_transaction.call_count, 2)

    def test_rejection_after_lost_reply_cannot_release_original_guard(self):
        prepared = self.prepare(native=True)
        self.client.send_raw_transaction.side_effect = TimeoutError("lost reply")
        with self.assertRaises(UnconfirmedTransactionError):
            self.submit(prepared)
        self.client.send_raw_transaction.side_effect = RpcTransactionRejected("insufficient funds")
        with self.assertRaises(UnconfirmedTransactionError):
            self.submit(prepared)
        self.assertEqual(self.journal.active_entries()[0]["state"], "unconfirmed")

    def direct_network(self):
        # Only replace RPC. Resolve amounts, allocate nonces, sign, persist,
        # submit, confirm and finish the batch through production code.
        payments, broadcasts, nonces, hidden = {}, [], {}, set()
        self.client.get_erc20_balance = EvmClient.get_erc20_balance.__get__(self.client)
        self.client.get_transaction_receipt = EvmClient.get_transaction_receipt.__get__(self.client)
        self.client.get_transaction_by_hash = EvmClient.get_transaction_by_hash.__get__(self.client)
        self.client.send_raw_transaction = EvmClient.send_raw_transaction.__get__(self.client)
        def rpc(network, method, params, **_kwargs):
            if method == "eth_gasPrice":
                return hex(50_000_000)
            if method == "eth_feeHistory":
                raise RuntimeError("fixture has no fee history")
            if method == "eth_getBalance":
                return hex(100 * 10**18)
            if method == "eth_call":
                return hex(1000 * 10**18)
            if method == "eth_estimateGas":
                return hex(70_000)
            if method == "eth_getTransactionCount":
                return hex(nonces.get((network, params[0].lower()), 0))
            if method == "eth_sendRawTransaction":
                raw = params[0]
                tx_hash = "0x" + bytes(self.client._ensure_eth_utils().keccak(hexstr=raw)).hex()
                broadcasts.append(raw)
                if tx_hash not in payments:
                    entry = next(e for e in self.journal.active_entries(kind="onchain-transfer") if e["payload"]["tx_hash"] == tx_hash)
                    payload = entry["payload"]
                    payments[tx_hash] = payload
                    nonces[(network, payload["source"].lower())] = payload["tx"]["nonce"] + 1
                    if len(payments) == 3 and getattr(self, "lose_third_direct_reply", False):
                        hidden.add(tx_hash)
                if tx_hash in hidden:
                    raise TimeoutError("accepted payment but reply lost")
                return tx_hash
            if method == "eth_getTransactionByHash":
                return None
            if method == "eth_getTransactionReceipt":
                tx_hash = params[0]
                payload = payments.get(tx_hash)
                if not payload or tx_hash in hidden:
                    return None
                tx = payload["tx"]
                receipt = {"status": "0x1", "transactionHash": tx_hash, "from": payload["source"], "to": tx["to"], "logs": []}
                data = str(tx.get("data") or "0x")
                if data.startswith("0xa9059cbb"):
                    receipt["logs"] = [{"address": tx["to"], "topics": [TRANSFER_TOPIC,
                        "0x" + payload["source"][2:].lower().rjust(64, "0"), "0x" + data[10:74]], "data": "0x" + data[74:]}]
                return receipt
            raise AssertionError(f"unexpected RPC: {method}")
        self.client._rpc_call = Mock(side_effect=rpc)
        return payments, broadcasts, hidden

    def test_direct_partial_batch_restart_then_new_payment_uses_correct_identity(self):
        self.lose_third_direct_reply = True
        payments, broadcasts, hidden = self.direct_network()
        owner = _DirectOwner(self.client, self.journal)
        params = replace(self.params, one_to_many_source_threads=1, relay_enabled=False, confirm_timeout_seconds=1)
        jobs = [(f"row{i}", "1" * 64, "0x" + str(i + 2) * 40) for i in range(3)]
        with patch("onchain_transfer_runner.messagebox.showerror", side_effect=AssertionError("unexpected controller failure")):
            owner._run_batch_transfer(jobs, params, False)
            self.assertEqual(owner.result_dialogs[-1]["success"], 2)
            self.assertEqual(len(payments), 3)
            hidden.clear()
            reloaded = OperationJournal(self.journal.file_path, secret_box=self.box)
            self.client.operation_journal = reloaded
            restarted = _DirectOwner(self.client, reloaded)
            restarted._run_batch_transfer(jobs[-1:], params, False)
            self.assertEqual(restarted.result_dialogs[-1]["success"], 1)
            self.assertEqual(len(payments), 3)
            self.assertEqual(broadcasts[2], broadcasts[3])
            restarted._run_batch_transfer(jobs, params, False)
            self.assertEqual(restarted.result_dialogs[-1]["success"], 3)
            self.assertEqual(len(payments), 6)
        self.assertEqual(sorted(p["tx"]["nonce"] for p in payments.values()), list(range(6)))

    def test_all_direct_modes_can_change_asset_amount_and_target_between_runs(self):
        payments, _broadcasts, _hidden = self.direct_network()
        owner = _DirectOwner(self.client, self.journal)
        with patch("onchain_transfer_runner.messagebox.showerror", side_effect=AssertionError("unexpected controller failure")):
            for mode in ("1对多", "多对1", "多对多"):
                jobs = [(f"row{i}", ("1" if mode == "1对多" else str(i + 1)) * 64,
                         "0x" + ("2" if mode == "多对1" else str(i + 2)) * 40) for i in range(3)]
                for native, amount in ((True, "0.001"), (False, "20"), (False, "21")):
                    params = replace(self.params, mode=mode, coin="BNB" if native else "USDT", amount=amount,
                                     token_is_native=native, token_contract="" if native else CONTRACT,
                                     one_to_many_source_threads=3, relay_enabled=False, confirm_timeout_seconds=1)
                    before = len(payments)
                    owner._run_batch_transfer(jobs, params, False)
                    self.assertEqual(owner.result_dialogs[-1]["success"], 3, owner.logs[-5:])
                    self.assertEqual(len(payments), before + 3)
                    recent = list(payments.values())[-3:]
                    if native:
                        self.assertTrue(all(p["tx"]["value"] == 10**15 for p in recent))
                    else:
                        self.assertTrue(all(int(p["tx"]["data"][74:], 16) == int(amount) * 10**18 for p in recent))
                changed_jobs = [(key, source, "0x" + "9" * 40) for key, source, _target in jobs]
                before = len(payments)
                owner._run_batch_transfer(changed_jobs, params, False)
                self.assertEqual(owner.result_dialogs[-1]["success"], 3, owner.logs[-5:])
                self.assertEqual(len(payments), before + 3)
                self.assertTrue(all(p["tx"]["data"][34:74] == "9" * 40 for p in list(payments.values())[-3:]))


if __name__ == "__main__":
    unittest.main()
