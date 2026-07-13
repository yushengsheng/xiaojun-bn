#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from core_models import EvmToken, WithdrawRuntimeParams
from onchain_relay_runner import (
    _RelayBatchItem,
    _receipt_state,
    _relay_confirm_auto_sweep,
    _relay_manual_account_copy_lines,
    _relay_native_main_recovery_status_text,
    _relay_prepare_items,
    _relay_native_dust_below_threshold,
    _relay_submit_auto_sweep,
    _relay_snapshot_cleanup_allowed,
    _relay_source_stage,
    _relay_terminal_snapshot_can_be_ignored,
    _relay_token_dust_below_threshold,
    _relay_token_dust_requires_native_recovery,
    _wait_for_receipt_success,
    run_relay_fee_recovery,
)
from onchain_relay_wallets import RelayWalletRecord


class _FakeClient:
    NATIVE_GAS_LIMIT = 21000

    def __init__(self):
        self.nonce_by_addr: dict[str, int] = {}
        self.receipts: list[dict | None] = [{"status": "0x1"}]
        self.receipt_requests: list[str] = []
        self.native_balance_wei: int | None = None
        self.native_balance_responses: list[int | Exception] = []
        self.erc20_balance_units: int | None = None
        self.gas_price_wei: int | None = None
        self.submitted_native_transfers: list[dict[str, object]] = []
        self.native_transfer_txid = "0x" + "a" * 64

    def credential_to_private_key(self, value: str) -> str:
        return str(value or "")

    def get_nonce(self, _network: str, address: str) -> int:
        return self.nonce_by_addr.setdefault(address, 0)

    def get_transaction_receipt(self, _network: str, _txid: str) -> dict | None:
        self.receipt_requests.append(str(_txid or ""))
        if len(self.receipts) > 1:
            return self.receipts.pop(0)
        return self.receipts[0]

    def get_gas_price_wei(self, *_args, **_kwargs) -> int:
        if self.gas_price_wei is None:
            raise AssertionError("prepare_items must not query gas price")
        return self.gas_price_wei

    def get_balance_wei(self, *_args, **_kwargs) -> int:
        if self.native_balance_responses:
            result = self.native_balance_responses.pop(0)
            if isinstance(result, Exception):
                raise result
            return int(result)
        if self.native_balance_wei is None:
            raise AssertionError("prepare_items must not query balances")
        return self.native_balance_wei

    def get_erc20_balance(self, *_args, **_kwargs) -> int:
        if self.erc20_balance_units is not None:
            return self.erc20_balance_units
        raise AssertionError("prepare_items must not query token balances")

    def get_default_tokens(self, _network: str) -> list[EvmToken]:
        return [
            EvmToken(symbol="BNB", contract="", decimals=18, is_native=True),
            EvmToken(
                symbol="USDT",
                contract="0x55d398326f99059ff775485246999027b3197955",
                decimals=18,
                is_native=False,
            ),
            EvmToken(
                symbol="USDC",
                contract="0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
                decimals=18,
                is_native=False,
            ),
        ]

    def estimate_erc20_transfer_gas(self, *_args, **_kwargs) -> int:
        raise AssertionError("prepare_items must not estimate token gas")

    def get_transaction_by_hash(self, *_args, **_kwargs) -> dict | None:
        raise AssertionError("prepare_items must not query transaction history")

    def submit_native_transfer_reliably(self, **kwargs):
        self.submitted_native_transfers.append(dict(kwargs))
        return type("Submission", (), {"tx_hash": self.native_transfer_txid})()


class _FakeStore:
    def load_records(self) -> list[RelayWalletRecord]:
        return list(_RECORDS.values())

    def update_record(self, relay_address: str, *, batch_id: str = "", **changes: object) -> RelayWalletRecord:
        record = _RECORDS[(relay_address, batch_id)]
        raw = dict(record.__dict__)
        raw.update(changes)
        updated = RelayWalletRecord.from_raw(raw)
        _RECORDS[(relay_address, batch_id)] = updated
        return updated

    def remove_records(self, record_keys: set[tuple[str, str]]) -> int:
        removed = 0
        for batch_id, relay_address in record_keys:
            if _RECORDS.pop((relay_address, batch_id), None) is not None:
                removed += 1
        return removed

    def flush(self) -> None:
        return None


class _FakeRoot:
    def __init__(self):
        self.clipboard_text = ""

    def clipboard_clear(self) -> None:
        self.clipboard_text = ""

    def clipboard_append(self, text: str) -> None:
        self.clipboard_text += str(text or "")

    def update_idletasks(self) -> None:
        return None


class _FakeOwner:
    AMOUNT_ALL_LABEL = "全部"

    def __init__(self, *, mode_1m: bool = False, mode_m1: bool = False):
        self.client = _FakeClient()
        self.stop_requested = threading.Event()
        self.relay_wallet_store = _FakeStore()
        self.logs: list[str] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.recovery_statuses: list[tuple[str, str, str]] = []
        self.mode_1m = mode_1m
        self.mode_m1 = mode_m1
        self.result_dialogs: list[dict[str, object]] = []
        self.is_running = False
        self.root = _FakeRoot()

    def _is_mode_1m(self) -> bool:
        return self.mode_1m

    def _is_mode_m1(self) -> bool:
        return self.mode_m1

    def _mode(self) -> str:
        if self.mode_1m:
            return "1M"
        if self.mode_m1:
            return "M1"
        return "M2M"

    def _dispatch_ui(self, callback):
        callback()

    def _runtime_worker_threads(self) -> int:
        return 4

    @staticmethod
    def _unique_row_keys(row_keys: list[str]) -> list[str]:
        return list(dict.fromkeys(row_keys))

    @staticmethod
    def _validate_recipient_address(value: str, _field_label: str = "收款地址") -> str:
        return str(value or "").strip()

    def _show_result_summary_dialog(self, **kwargs: object) -> None:
        self.result_dialogs.append(dict(kwargs))

    def _clear_source_private_key_cache(self) -> None:
        return None

    def _set_status(self, row_key: str, status: str, status_text: str = ""):
        self.statuses.append((row_key, status, status_text))

    def _set_recovery_status(self, row_key: str, status: str, status_text: str = ""):
        self.recovery_statuses.append((row_key, status, status_text))

    def log(self, message: str):
        self.logs.append(str(message))

    def _gas_fee_amount_text(self, _network: str, units: int) -> str:
        amount = Decimal(max(0, int(units))) / (Decimal(10) ** 18)
        return f"{self._decimal_to_text(amount)} BNB"

    def _units_to_amount(self, units: int, decimals: int) -> Decimal:
        return Decimal(max(0, int(units))) / (Decimal(10) ** max(0, int(decimals)))

    def _amount_to_units(self, amount: Decimal, decimals: int) -> int:
        return int(Decimal(amount) * (Decimal(10) ** max(0, int(decimals))))

    def _decimal_to_text(self, value: Decimal) -> str:
        number = Decimal(value)
        return format(number.normalize(), "f") if number != number.to_integral() else format(number, "f")

    def _token_amount_text(self, symbol: str, amount: Decimal) -> str:
        return f"{self._decimal_to_text(amount)} {str(symbol or '').strip().upper()}"

    def _row_log_prefix(self, _row_key: str, index: int, total: int, _row_number_by_key: dict[str, int]) -> str:
        return f"[{index}/{total}]"

    def _mask(self, value: str, *, head: int = 6, tail: int = 4) -> str:
        text = str(value or "")
        if len(text) <= head + tail:
            return text
        return f"{text[:head]}...{text[-tail:]}"

    def _mask_credential(self, value: str) -> str:
        return self._mask(value, head=4, tail=4)


_RECORDS: dict[tuple[str, str], RelayWalletRecord] = {}


def _params(
    *,
    delay: float = 0.0,
    one_to_many_source_threads: int = 5,
    nonce_window: int = 5,
    coin: str = "USDT",
    token_is_native: bool = False,
) -> WithdrawRuntimeParams:
    return WithdrawRuntimeParams(
        coin=coin,
        amount="1",
        network="BSC",
        delay=delay,
        threads=4,
        one_to_many_source_threads=one_to_many_source_threads,
        receipt_poll_interval_seconds=0.001,
        receipt_retry_count=3,
        nonce_window=nonce_window,
        token_contract="0x55d398326f99059ff775485246999027b3197955",
        token_decimals=18,
        token_is_native=token_is_native,
        relay_enabled=True,
        relay_fee_reserve=Decimal("0.001"),
    )


def _item(index: int) -> _RelayBatchItem:
    relay_address = f"0x{index:040x}"
    record = RelayWalletRecord(
        created_at="",
        expires_at="",
        network="BSC",
        source="0x00000000000000000000000000000000000000aa",
        target=f"0x{index + 100:040x}",
        relay_address=relay_address,
        private_key="1" * 64,
        status="created",
        batch_id="batch",
    )
    _RECORDS[(relay_address, "batch")] = record
    return _RelayBatchItem(
        index=index,
        row_key=f"row-{index}",
        source_raw="source-private-key",
        target=record.target,
        source_private_key="source-private-key",
        source_addr=record.source,
        relay_record=record,
        relay_private_key=record.private_key,
        prefix=f"[{index}/3]",
        relay_name=f"B{index}",
        target_name=f"C{index}",
        source_label="source",
        amount_text="1",
        value_units=10**18,
    )


class OnchainRelayPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        _RECORDS.clear()

    def test_receipt_state_requires_status_one(self) -> None:
        self.assertEqual(_receipt_state({"status": "0x1"}), "success")
        self.assertEqual(_receipt_state({"status": "1"}), "success")
        self.assertEqual(_receipt_state({"status": "0x0"}), "failed")
        self.assertEqual(_receipt_state(None), "pending")

    def test_relay_dust_thresholds_match_cleanup_policy(self) -> None:
        owner = _FakeOwner()
        bnb_threshold_units = 5_000_000_000_000
        self.assertTrue(_relay_native_dust_below_threshold(owner, "BSC", bnb_threshold_units - 1)[0])
        self.assertFalse(_relay_native_dust_below_threshold(owner, "BSC", bnb_threshold_units)[0])

        usdt = EvmToken(
            symbol="USDT",
            contract="0x55d398326f99059ff775485246999027b3197955",
            decimals=18,
            is_native=False,
        )
        usdt_threshold_units = 100_000_000_000_000
        self.assertTrue(_relay_token_dust_below_threshold(owner, {"USDT": (usdt, usdt_threshold_units - 1)})[0])
        self.assertFalse(_relay_token_dust_below_threshold(owner, {"USDT": (usdt, usdt_threshold_units)})[0])

    def test_relay_cleanup_requires_every_asset_below_threshold(self) -> None:
        owner = _FakeOwner()
        bnb = EvmToken(symbol="BNB", contract="", decimals=18, is_native=True)
        usdt = EvmToken(
            symbol="USDT",
            contract="0x55d398326f99059ff775485246999027b3197955",
            decimals=18,
            is_native=False,
        )
        bnb_threshold_units = 5_000_000_000_000
        usdt_threshold_units = 100_000_000_000_000

        self.assertTrue(
            _relay_snapshot_cleanup_allowed(
                owner,
                "BSC",
                {
                    "BNB": (bnb, bnb_threshold_units - 1),
                    "USDT": (usdt, usdt_threshold_units - 1),
                },
            )
        )
        self.assertFalse(
            _relay_snapshot_cleanup_allowed(
                owner,
                "BSC",
                {
                    "BNB": (bnb, bnb_threshold_units),
                    "USDT": (usdt, usdt_threshold_units - 1),
                },
            )
        )
        self.assertFalse(
            _relay_snapshot_cleanup_allowed(
                owner,
                "BSC",
                {
                    "BNB": (bnb, bnb_threshold_units - 1),
                    "USDT": (usdt, usdt_threshold_units),
                },
            )
        )

    def test_manual_account_copy_lines_match_wallet_generator_format(self) -> None:
        rows = [
            {"relay_address": "0xAbC", "private_key": "key-1"},
            {"relay_address": "0xabc", "private_key": "duplicate-key"},
            {"relay_address": "0xDef", "private_key": "key-2"},
            {"relay_address": "0xMissing"},
        ]

        self.assertEqual(
            _relay_manual_account_copy_lines(rows),
            ["0xAbC key-1", "0xDef key-2"],
        )

    def test_manual_recovery_retries_rpc_failure_after_first_pass(self) -> None:
        owner = _FakeOwner()
        relay_addr = "0x00000000000000000000000000000000000000cc"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address=relay_addr,
            private_key="relay-private-key",
            status="forwarded",
            batch_id="batch-retry",
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_responses = [RuntimeError("temporary RPC failure"), 0]
        owner.client.erc20_balance_units = 0

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
            ):
                run_relay_fee_recovery(
                    owner,
                    False,
                    show_dialog=True,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="manual",
                )

        self.assertEqual(_RECORDS, {})
        self.assertTrue(any("进行一次二次验证" in message for message in owner.logs))
        self.assertEqual(len(owner.result_dialogs), 1)
        result = owner.result_dialogs[0]
        self.assertEqual(result["success"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertIn("已清空 1", str(result["detail_text"]))
        self.assertEqual(result["action_buttons"][0][0], "复制待人工账号")
        self.assertFalse(result["action_buttons"][0][2])

    def test_manual_recovery_counts_persistent_rpc_failure_once(self) -> None:
        owner = _FakeOwner()
        relay_addr = "0x00000000000000000000000000000000000000cd"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address=relay_addr,
            private_key="relay-private-key",
            status="forwarded",
            batch_id="batch-failed",
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_responses = [
            RuntimeError("temporary RPC failure"),
            RuntimeError("persistent RPC failure"),
        ]

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
            ):
                run_relay_fee_recovery(
                    owner,
                    False,
                    show_dialog=True,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="manual",
                )

        self.assertEqual(len(_RECORDS), 1)
        self.assertEqual(_RECORDS[(relay_addr, record.batch_id)].status, "failed")
        result = owner.result_dialogs[0]
        self.assertEqual(result["success"], 0)
        self.assertEqual(result["failed"], 1)

    def test_batch_recovery_does_not_enable_manual_second_pass(self) -> None:
        owner = _FakeOwner()
        relay_addr = "0x00000000000000000000000000000000000000cf"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address=relay_addr,
            private_key="relay-private-key",
            status="forwarded",
            batch_id="batch-dialog",
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_responses = [RuntimeError("temporary RPC failure"), 0]

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
            ):
                run_relay_fee_recovery(
                    owner,
                    False,
                    batch_ids={record.batch_id},
                    show_dialog=True,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="batch-dialog",
                )

        self.assertEqual(owner.client.native_balance_responses, [0])
        self.assertFalse(any("进行一次二次验证" in message for message in owner.logs))
        result = owner.result_dialogs[0]
        self.assertEqual(result["failed"], 1)
        self.assertIsNone(result["action_buttons"])

    def test_manual_recovery_copy_button_contains_final_manual_accounts(self) -> None:
        owner = _FakeOwner()
        relay_addr = "0x00000000000000000000000000000000000000ce"
        private_key = "manual-private-key"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address=relay_addr,
            private_key=private_key,
            status="created",
            batch_id="batch-manual",
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_responses = [5_000_000_000_000, 5_000_000_000_000]
        owner.client.erc20_balance_units = 0
        owner.client.gas_price_wei = 1_000_000_000

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
            ):
                run_relay_fee_recovery(
                    owner,
                    False,
                    show_dialog=True,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="manual",
                )

        result = owner.result_dialogs[0]
        action_label, action_callback, action_enabled = result["action_buttons"][0]
        self.assertEqual(action_label, "复制待人工账号")
        self.assertTrue(action_enabled)
        action_callback()
        self.assertEqual(owner.root.clipboard_text, f"{relay_addr} {private_key}")
        self.assertNotIn(" - ", owner.root.clipboard_text)

    def test_terminal_dust_record_does_not_ignore_bnb_at_threshold(self) -> None:
        owner = _FakeOwner()
        bnb = EvmToken(symbol="BNB", contract="", decimals=18, is_native=True)
        bnb_threshold_units = 5_000_000_000_000
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address="0x00000000000000000000000000000000000000cc",
            private_key="relay-private-key",
            status="completed",
            batch_id="batch",
            sweep_resolution="dust_left",
        )

        self.assertTrue(
            _relay_terminal_snapshot_can_be_ignored(
                owner,
                "BSC",
                record,
                {"BNB": (bnb, bnb_threshold_units - 1)},
            )
        )
        self.assertFalse(
            _relay_terminal_snapshot_can_be_ignored(
                owner,
                "BSC",
                record,
                {"BNB": (bnb, bnb_threshold_units)},
            )
        )
        self.assertFalse(
            _relay_terminal_snapshot_can_be_ignored(
                owner,
                "BSC",
                record,
                {"BNB": (bnb, bnb_threshold_units + 1)},
            )
        )

    def test_token_dust_does_not_block_native_recovery(self) -> None:
        owner = _FakeOwner()
        bnb = EvmToken(symbol="BNB", contract="", decimals=18, is_native=True)
        usdt = EvmToken(
            symbol="USDT",
            contract="0x55d398326f99059ff775485246999027b3197955",
            decimals=18,
            is_native=False,
        )
        bnb_threshold_units = 5_000_000_000_000
        usdt_dust_units = 50_000_000_000_000

        self.assertFalse(
            _relay_token_dust_requires_native_recovery(
                owner,
                "BSC",
                {
                    "BNB": (bnb, bnb_threshold_units - 1),
                    "USDT": (usdt, usdt_dust_units),
                },
            )
        )
        self.assertTrue(
            _relay_token_dust_requires_native_recovery(
                owner,
                "BSC",
                {
                    "BNB": (bnb, bnb_threshold_units + 1),
                    "USDT": (usdt, usdt_dust_units),
                },
            )
        )

    def test_wait_for_receipt_retries_until_success(self) -> None:
        owner = _FakeOwner()
        owner.client.receipts = [None, {"status": "0x1"}]

        receipt = _wait_for_receipt_success(owner, _params(), "0x" + "1" * 64, label="测试交易")

        self.assertEqual(receipt["status"], "0x1")

    def test_auto_sweep_waits_for_receipt_before_success(self) -> None:
        owner = _FakeOwner()
        owner.client.native_balance_wei = 100_000
        owner.client.gas_price_wei = 1
        owner.client.receipts = [None, {"status": "0x1"}]
        item = _item(1)

        _relay_submit_auto_sweep(owner, _params(), item, item.source_addr)

        self.assertEqual(owner.client.receipt_requests, [owner.client.native_transfer_txid] * 2)
        self.assertEqual(getattr(item, "_auto_sweep_value_wei"), 79_000)
        self.assertEqual(owner.recovery_statuses[-1], (item.row_key, "success", "回收0.000000000000079 BNB"))
        self.assertEqual(_RECORDS[(item.relay_addr, "batch")].sweep_resolution, "confirmed")

    def test_auto_sweep_can_submit_all_before_receipt_confirmation(self) -> None:
        owner = _FakeOwner()
        owner.client.native_balance_wei = 100_000
        owner.client.gas_price_wei = 1
        items = [_item(1), _item(2)]

        for item in items:
            _relay_submit_auto_sweep(owner, _params(), item, item.source_addr, confirm_receipt=False)

        self.assertEqual(len(owner.client.submitted_native_transfers), 2)
        self.assertEqual(owner.client.receipt_requests, [])
        self.assertTrue(all(getattr(item, "_auto_sweep_pending", False) for item in items))

        for item in items:
            _relay_confirm_auto_sweep(owner, _params(), item)

        self.assertEqual(len(owner.client.receipt_requests), 2)
        self.assertTrue(all(not getattr(item, "_auto_sweep_pending", False) for item in items))
        self.assertTrue(all(_RECORDS[(item.relay_addr, "batch")].sweep_resolution == "confirmed" for item in items))

    def test_auto_sweep_failed_receipt_does_not_count_recovered_amount(self) -> None:
        owner = _FakeOwner()
        owner.client.native_balance_wei = 100_000
        owner.client.gas_price_wei = 1
        owner.client.receipts = [{"status": "0x0"}]
        item = _item(1)

        _relay_submit_auto_sweep(owner, _params(), item, item.source_addr)

        self.assertEqual(getattr(item, "_auto_sweep_value_wei"), 0)
        self.assertEqual(owner.recovery_statuses[-1][1], "incomplete")
        self.assertIn("receipt status=0", _RECORDS[(item.relay_addr, "batch")].last_error)

    def test_native_relay_recovery_status_shows_amount(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        text = _relay_native_main_recovery_status_text(
            owner,
            _params(coin="BNB", token_is_native=True),
            "0.000123",
        )

        self.assertEqual(text, "回收0.000123 BNB")
        self.assertNotEqual(text, "随主流程完成")

    def test_source_stage_respects_nonce_window_and_delay(self) -> None:
        owner = _FakeOwner(mode_1m=False)
        params = _params(delay=0.01, nonce_window=2)
        items = [_item(1), _item(2), _item(3)]
        submissions: list[tuple[int, int]] = []
        wait_intervals: list[float | None] = []

        def record_wait(timeout: float | None = None) -> bool:
            wait_intervals.append(timeout)
            return False

        owner.stop_requested.wait = record_wait

        def submit_one(item: _RelayBatchItem, nonce: int) -> str:
            submissions.append((item.index, nonce))
            return "0x" + f"{item.index:064x}"

        _relay_source_stage(
            owner,
            params,
            items,
            stage_label="测试阶段",
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            submitted_status="token_funding_submitted",
            done_status="token_funded",
            done_statuses={"token_funded"},
            submit_one=submit_one,
            status_text="测试",
        )

        self.assertEqual([item[0] for item in submissions], [1, 2, 3])
        self.assertEqual([item[1] for item in submissions], [0, 1, 2])
        self.assertEqual(wait_intervals, [0.01, 0.01])
        self.assertEqual([_RECORDS[(item.relay_addr, "batch")].status for item in items], ["token_funded"] * 3)

    def test_source_stage_1m_uses_dedicated_source_threads(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        params = _params(delay=0.0, one_to_many_source_threads=2, nonce_window=2)
        items = [_item(1), _item(2), _item(3), _item(4)]
        submissions: list[int] = []
        active = 0
        peak = 0
        lock = threading.Lock()

        def submit_one(item: _RelayBatchItem, nonce: int) -> str:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.02)
                submissions.append(item.index)
                return "0x" + f"{item.index:064x}"
            finally:
                with lock:
                    active -= 1

        _relay_source_stage(
            owner,
            params,
            items,
            stage_label="测试阶段",
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            submitted_status="token_funding_submitted",
            done_status="token_funded",
            done_statuses={"token_funded"},
            submit_one=submit_one,
            status_text="测试",
        )

        self.assertEqual(sorted(submissions), [1, 2, 3, 4])
        self.assertEqual(peak, 2)

    def test_source_stage_confirms_after_all_submissions(self) -> None:
        owner = _FakeOwner()
        params = _params(delay=0.0, nonce_window=2)
        items = [_item(1), _item(2), _item(3), _item(4)]
        submissions: list[int] = []
        events: list[str] = []

        def submit_one(item: _RelayBatchItem, nonce: int) -> str:
            submissions.append(item.index)
            events.append(f"submit-{item.index}")
            return "0x" + f"{item.index:064x}"

        original_receipt = owner.client.get_transaction_receipt

        def receipt_after_submits(network: str, txid: str):
            events.append("receipt")
            return original_receipt(network, txid)

        owner.client.get_transaction_receipt = receipt_after_submits

        _relay_source_stage(
            owner,
            params,
            items,
            stage_label="测试阶段",
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            submitted_status="token_funding_submitted",
            done_status="token_funded",
            done_statuses={"token_funded"},
            submit_one=submit_one,
            status_text="测试",
        )

        self.assertEqual(submissions, [1, 2, 3, 4])
        self.assertEqual(events[:4], ["submit-1", "submit-2", "submit-3", "submit-4"])
        self.assertEqual(events.count("receipt"), 4)

    def test_prepare_items_does_not_perform_rpc_work(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        params = _params()
        row_key = "row-1"
        source_addr = "0x00000000000000000000000000000000000000aa"
        target_addr = "0x00000000000000000000000000000000000000bb"
        relay_addr = "0x00000000000000000000000000000000000000cc"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source=source_addr,
            target=target_addr,
            relay_address=relay_addr,
            private_key="relay-private-key",
            status="created",
            batch_id="batch",
        )
        _RECORDS[(relay_addr, "batch")] = record

        items = _relay_prepare_items(
            owner,
            [(row_key, "source-private-key", target_addr)],
            params,
            {row_key: 1},
            {row_key: record},
            {row_key: ("source-private-key", source_addr)},
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].value_units, 10**18)
        self.assertEqual(items[0].amount_text, "1")


if __name__ == "__main__":
    unittest.main()
