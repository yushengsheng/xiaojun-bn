#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
import tempfile
import threading
from concurrent.futures import Future
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from api_clients import SubmittedTransaction
from operation_journal import OperationJournal
from operation_recovery import build_onchain_recovery_plan
from onchain_transfer_runner import OnchainTransferRunnerMixin
from secret_box import SecretBox


class OnchainRecoveryPlanTests(unittest.TestCase):
    def _plan(self, source: str = "private-key-must-not-be-persisted"):
        return build_onchain_recovery_plan(
            network="bsc",
            mode="多对多",
            coin="USDT",
            amount="1.5",
            random_enabled=False,
            random_min="",
            random_max="",
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
            jobs=[
                ("row-a", source, "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa"),
                ("row-b", source, "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa"),
            ],
        )

    def test_fingerprint_uses_public_identity_not_source_credential(self) -> None:
        first = self._plan("private-key-one")
        second = self._plan("private-key-two")
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertNotIn("private-key-one", first.fingerprint)
        self.assertNotEqual(first.tasks[0].task_key, first.tasks[1].task_key)

    def test_changed_transfer_parameters_start_a_different_plan(self) -> None:
        first = self._plan()
        changed = build_onchain_recovery_plan(
            network="bsc",
            mode="多对多",
            coin="USDT",
            amount="2",
            random_enabled=False,
            random_min="",
            random_max="",
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
            jobs=[("row-a", "secret", "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa")],
        )
        self.assertNotEqual(first.fingerprint, changed.fingerprint)


class _RecoveryClient:
    def __init__(self, *, receipt=None, nonce: int = 0):
        self.receipt = receipt
        self.nonce = nonce
        self.receipt_failures_remaining = 0
        self.nonce_failures_remaining = 0
        self.receipt_calls = 0
        self.nonce_calls = 0

    def get_transaction_receipt(self, _network, _txid, **_kwargs):
        self.receipt_calls += 1
        if self.receipt_failures_remaining > 0:
            self.receipt_failures_remaining -= 1
            raise RuntimeError("BSC RPC 请求失败：Read timed out")
        return self.receipt

    def get_nonce(self, _network, _source, **_kwargs):
        self.nonce_calls += 1
        if self.nonce_failures_remaining > 0:
            self.nonce_failures_remaining -= 1
            raise RuntimeError("BSC RPC 请求失败：Read timed out")
        return self.nonce

    @staticmethod
    def _int_from_hex(value):
        return int(str(value), 16)


class _RecoveryRunner(OnchainTransferRunnerMixin):
    def __init__(self, journal: OperationJournal, *, receipt=None, nonce: int = 0):
        self.operation_journal = journal
        self.client = _RecoveryClient(receipt=receipt, nonce=nonce)
        self.logs: list[str] = []

    def _resolve_wallet(self, source: str):
        return "derived-private-key", "0x00000000000000000000000000000000000000aa"

    @staticmethod
    def _validate_recipient_address(value: str, _label: str) -> str:
        return str(value).lower()

    @staticmethod
    def _decimal_to_text(value) -> str:
        return str(value)

    @staticmethod
    def _mode() -> str:
        return "多对多"

    def log(self, message: str) -> None:
        self.logs.append(message)

    @staticmethod
    def _dispatch_ui(_callback) -> None:
        return None


class _DirectTransferClient:
    NATIVE_GAS_LIMIT = 21000

    def __init__(self):
        self.tx_hash = "0x" + "b" * 64
        self.source = "0x00000000000000000000000000000000000000aa"
        self.target = "0x0000000000000000000000000000000000000022"
        self.amount_units = 1500000000000000000
        self.source_token_balance = 3000000000000000000
        self.target_before = 7000000000000000000
        self.sent = False
        self.receipt_kwargs: dict[str, object] = {}
        self.receipt_kwargs_history: list[dict[str, object]] = []
        self.receipt_sequence: list[dict | None] = []
        self.submission = SubmittedTransaction(tx_hash=self.tx_hash, acknowledged=True, recovered=False, recovery_reason="rpc-ack")
        self.gas_failures_remaining = 0
        self.gas_calls = 0

    @staticmethod
    def _int_from_hex(value) -> int:
        return int(str(value), 16)

    def get_gas_price_wei(self, _network: str) -> int:
        self.gas_calls += 1
        if self.gas_failures_remaining > 0:
            self.gas_failures_remaining -= 1
            raise RuntimeError("BSC RPC 请求失败：Read timed out")
        return 1

    def get_erc20_balance(self, _network: str, _contract: str, address: str) -> int:
        if str(address).lower() == self.target.lower():
            return self.target_before + (self.amount_units if self.sent else 0)
        return self.source_token_balance

    @staticmethod
    def get_balance_wei(_network: str, _address: str) -> int:
        return 10**18

    @staticmethod
    def estimate_erc20_transfer_gas(
        _network: str,
        _from_address: str,
        _contract: str,
        _to_address: str,
        _amount_units: int,
    ) -> int:
        return 70000

    def get_nonce(self, _network: str, _source: str) -> int:
        return 1 if self.sent else 0

    def submit_erc20_transfer_reliably(self, **_kwargs) -> SubmittedTransaction:
        self.sent = True
        return self.submission

    def get_transaction_receipt(self, _network: str, _tx_hash: str, **kwargs):
        self.receipt_kwargs = dict(kwargs)
        self.receipt_kwargs_history.append(dict(kwargs))
        if self.receipt_sequence:
            return self.receipt_sequence.pop(0)
        return None


class _RecordingStopEvent:
    def __init__(self):
        self.wait_calls: list[float] = []
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True

    def wait(self, seconds: float) -> bool:
        self.wait_calls.append(seconds)
        return self._set


class _DirectTransferRunner(OnchainTransferRunnerMixin):
    AMOUNT_ALL_LABEL = "全部"

    def __init__(self, journal: OperationJournal):
        self.operation_journal = journal
        self.client = _DirectTransferClient()
        self.logs: list[str] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.stop_requested = threading.Event()
        self.root = SimpleNamespace(_closing=False)
        self._closing = False
        self.is_running = True
        self.source_private_key_cache = {}
        self.source_address_cache = {}
        self.wallet_cache_lock = threading.Lock()

    @staticmethod
    def _mode() -> str:
        return "多对多"

    def _resolve_wallet(self, _source: str):
        return "derived-private-key", self.client.source

    def _validate_recipient_address(self, value: str, _label: str) -> str:
        return self.client.target if str(value).lower() == self.client.target.lower() else str(value)

    @staticmethod
    def _amount_to_units(amount: Decimal, decimals: int) -> int:
        return int(amount * (Decimal(10) ** int(decimals)))

    @staticmethod
    def _units_to_amount(units: int, decimals: int) -> Decimal:
        return Decimal(int(units)) / (Decimal(10) ** int(decimals))

    @staticmethod
    def _decimal_to_text(value) -> str:
        return format(Decimal(str(value)), "f").rstrip("0").rstrip(".") or "0"

    def _token_amount_text(self, symbol: str, amount: Decimal) -> str:
        return f"{self._decimal_to_text(amount)} {symbol}"

    @staticmethod
    def _token_desc_from_params(params) -> str:
        return str(params.coin)

    @staticmethod
    def _gas_fee_amount_text(_network: str, fee_wei: int) -> str:
        return f"{fee_wei} wei"

    @staticmethod
    def _estimated_gas_fee_text(_network: str, fee_wei: int) -> str:
        return f"预估 {fee_wei} wei"

    @staticmethod
    def _row_number_by_key_snapshot() -> dict[str, int]:
        return {}

    @staticmethod
    def _row_log_prefix(_row_key: str, index: int, total: int, _row_numbers: dict[str, int]) -> str:
        return f"[{index}/{total}]"

    @staticmethod
    def _unique_row_keys(keys: list[str]) -> list[str]:
        return list(dict.fromkeys(keys))

    @staticmethod
    def _row_context_for_values(_row_key: str, source: str, target: str) -> str:
        return f"{source}->{target}"

    def _dispatch_ui(self, callback) -> None:
        callback()

    def log(self, message: str) -> None:
        self.logs.append(message)

    @staticmethod
    def _begin_progress(_kind: str, _keys: list[str]) -> None:
        return None

    @staticmethod
    def _set_progress_metrics(**_kwargs) -> None:
        return None

    @staticmethod
    def _mark_row_status_context(_row_key: str, _context: str) -> None:
        return None

    def _set_status(self, row_key: str, status: str, text: str = "") -> None:
        self.statuses.append((row_key, status, text))

    @staticmethod
    def _success_status_text(coin: str, amount_text: str) -> str:
        return f"{coin} {amount_text}"

    @staticmethod
    def _show_result_summary_dialog(**_kwargs) -> None:
        return None

    @staticmethod
    def _finish_progress(_kind: str, _success: int, _failed: int) -> None:
        return None

    def _clear_source_private_key_cache(self) -> None:
        self.source_private_key_cache.clear()
        self.source_address_cache.clear()

    @staticmethod
    def _submit_confirmation_task(target, *, name: str = "onchain-confirm") -> Future:
        future: Future = Future()
        try:
            target()
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(None)
        return future


class _StoppingPreflightRunner(_DirectTransferRunner):
    def __init__(self, journal: OperationJournal):
        super().__init__(journal)
        self.resolve_count = 0

    def _resolve_wallet(self, source: str):
        self.resolve_count += 1
        if self.resolve_count == 1:
            self.stop_requested.set()
        return super()._resolve_wallet(source)


class OnchainRecoveryIntegrationTests(unittest.TestCase):
    @staticmethod
    def _params():
        return SimpleNamespace(
            network="BSC",
            coin="USDT",
            amount="1.5",
            random_enabled=False,
            random_min=None,
            random_max=None,
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
        )

    def test_completed_task_is_returned_when_same_batch_resumes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _RecoveryRunner(journal)
            jobs = [("row-1", "private-credential", "0x0000000000000000000000000000000000000022")]
            batch_id, plan, completed, resumed = runner._prepare_onchain_recovery_batch(self._params(), jobs)
            self.assertFalse(resumed)
            self.assertEqual(completed, {})

            entry_id = journal.begin(
                "onchain-transfer",
                "BSC:0x00000000000000000000000000000000000000aa",
                {"batch_id": batch_id, "task_key": plan.tasks[0].task_key, "amount": "1.5"},
            )
            journal.update(entry_id, "completed")
            journal.finish_batch(batch_id, success=1, failed=0, interrupted=True)

            resumed_id, resumed_plan, resumed_completed, resumed = runner._prepare_onchain_recovery_batch(
                self._params(), jobs
            )
            self.assertTrue(resumed)
            self.assertEqual(resumed_id, batch_id)
            self.assertEqual(resumed_plan.tasks[0].task_key, plan.tasks[0].task_key)
            self.assertIn(plan.tasks[0].task_key, resumed_completed)

    def test_unconfirmed_nonce_blocks_duplicate_broadcast(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _RecoveryRunner(journal, receipt=None, nonce=7)
            journal.begin(
                "onchain-transfer",
                "BSC:0x00000000000000000000000000000000000000aa",
                {
                    "network": "BSC",
                    "source_address": "0x00000000000000000000000000000000000000aa",
                    "nonce": 7,
                    "txid": "0x" + "a" * 64,
                },
            )
            jobs = [("row-1", "private-credential", "0x0000000000000000000000000000000000000022")]

            self.assertFalse(runner._reconcile_pending_onchain_operations("BSC", jobs))
            self.assertTrue(any("已阻止重复转账" in line for line in runner.logs))

    def test_reconcile_pending_operations_retries_transient_rpc_errors(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _RecoveryRunner(journal, receipt=None, nonce=7)
            runner._onchain_transfer_retry_delay_seconds = lambda _params=None: 0  # type: ignore[method-assign]
            runner.client.receipt_failures_remaining = 1
            runner.client.nonce_failures_remaining = 1
            journal.begin(
                "onchain-transfer",
                "BSC:0x00000000000000000000000000000000000000aa",
                {
                    "network": "BSC",
                    "source_address": "0x00000000000000000000000000000000000000aa",
                    "nonce": 7,
                    "txid": "0x" + "a" * 64,
                },
            )
            jobs = [("row-1", "private-credential", "0x0000000000000000000000000000000000000022")]

            self.assertFalse(runner._reconcile_pending_onchain_operations("BSC", jobs))

            self.assertGreaterEqual(runner.client.receipt_calls, 2)
            self.assertGreaterEqual(runner.client.nonce_calls, 2)
            self.assertTrue(any("准备重试 2/10" in line for line in runner.logs))
            self.assertTrue(any("已阻止重复转账" in line for line in runner.logs))

    def test_direct_erc20_batch_rpc_ack_finishes_without_waiting_for_receipt(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _DirectTransferRunner(journal)
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.2,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [("row-1", "private-credential", runner.client.target)]
            runner.client.receipt_sequence = [{"status": "0x1"}]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertIn(("row-1", "success", "USDT 1.5"), runner.statuses)
            self.assertFalse(any("余额已到账" in line for line in runner.logs))
            self.assertFalse(any("转账广播完成，等待确认" in line for line in runner.logs))
            self.assertTrue(any("金额=USDT 1.5" in line for line in runner.logs))
            self.assertTrue(any("后台确认中" in line for line in runner.logs))
            self.assertTrue(any("后台确认完成" in line for line in runner.logs))
            self.assertTrue(any("链上转账准备：账本对账完成" in line for line in runner.logs))
            self.assertTrue(any("链上转账准备完成" in line for line in runner.logs))
            self.assertEqual(journal.active_entries(kind="onchain-transfer"), [])
            self.assertEqual(runner.client.receipt_kwargs.get("max_attempts"), 1)
            self.assertLessEqual(float(runner.client.receipt_kwargs.get("read_timeout", 99)), 3.0)

    def test_direct_submitted_confirmation_uses_configured_poll_and_short_timeout(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _DirectTransferRunner(journal)
            stop_event = _RecordingStopEvent()
            runner.stop_requested = stop_event
            runner.client.submission = SubmittedTransaction(
                tx_hash=runner.client.tx_hash,
                acknowledged=False,
                recovered=True,
                recovery_reason="tx-hash",
            )
            runner.client.receipt_sequence = [None, {"status": "0x1"}]
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.25,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [("row-1", "private-credential", runner.client.target)]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertIn(0.25, stop_event.wait_calls)
            self.assertGreaterEqual(len(runner.client.receipt_kwargs_history), 2)
            kwargs = runner.client.receipt_kwargs_history[0]
            self.assertEqual(kwargs.get("max_attempts"), 1)
            self.assertLessEqual(float(kwargs.get("read_timeout", 99)), 3.0)
            self.assertLessEqual(float(kwargs.get("connect_timeout", 99)), float(kwargs.get("read_timeout", 0)))
            self.assertIn(("row-1", "submitted", ""), runner.statuses)
            self.assertIn(("row-1", "success", "USDT 1.5"), runner.statuses)

    def test_direct_transfer_retries_transient_rpc_error_per_record(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _DirectTransferRunner(journal)
            runner.client.gas_failures_remaining = 1
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.2,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [("row-1", "private-credential", runner.client.target)]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertGreaterEqual(runner.client.gas_calls, 2)
            self.assertTrue(any("准备重试 2/10" in line for line in runner.logs))
            self.assertIn(("row-1", "success", "USDT 1.5"), runner.statuses)

    def test_direct_transfer_failure_status_shows_balance_reason(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _DirectTransferRunner(journal)
            runner.client.source_token_balance = 0
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.2,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [("row-1", "private-credential", runner.client.target)]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertIn(("row-1", "failed", "❌代币余额不足"), runner.statuses)

    def test_direct_transfer_failure_status_shows_network_reason_after_retries(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _DirectTransferRunner(journal)
            runner._onchain_transfer_retry_delay_seconds = lambda _params=None: 0  # type: ignore[method-assign]
            runner.client.gas_failures_remaining = 20
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.2,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [("row-1", "private-credential", runner.client.target)]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertIn(("row-1", "failed", "❌网络异常：Read timed out"), runner.statuses)

    def test_batch_preflight_shows_preparing_and_stops_cleanly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _StoppingPreflightRunner(journal)
            params = SimpleNamespace(
                network="BSC",
                coin="USDT",
                amount="1.5",
                delay=0,
                threads=1,
                confirm_timeout_seconds=3.0,
                confirm_poll_interval_seconds=0.2,
                random_enabled=False,
                random_min=None,
                random_max=None,
                token_contract="0x0000000000000000000000000000000000000011",
                token_decimals=18,
                token_is_native=False,
                relay_enabled=False,
            )
            jobs = [
                ("row-1", "private-credential-1", runner.client.target),
                ("row-2", "private-credential-2", "0x0000000000000000000000000000000000000033"),
            ]

            runner._run_batch_transfer(jobs, params, dry_run=False)

            self.assertEqual(runner.statuses[0], ("row-1", "running", "准备中"))
            self.assertEqual(runner.statuses[1], ("row-2", "running", "准备中"))
            self.assertIn(("row-1", "failed", "已停止"), runner.statuses)
            self.assertIn(("row-2", "failed", "已停止"), runner.statuses)
            self.assertTrue(any("已停止" in line for line in runner.logs))
            self.assertEqual(journal.active_entries(kind="onchain-transfer"), [])


if __name__ == "__main__":
    unittest.main()
