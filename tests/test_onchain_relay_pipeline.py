#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
import unittest
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from core_models import EvmToken, GeneratedWalletEntry, WithdrawRuntimeParams
from onchain_relay_runner import (
    _RelayBatchItem,
    _receipt_state,
    _relay_confirm_auto_sweep,
    _relay_clear_missing_source_txids,
    _relay_manual_account_copy_lines,
    _relay_native_main_recovery_status_text,
    _relay_preflight_1m_native_source,
    _relay_resolve_1m_native_transfer,
    _resolve_record_amount_local,
    _pick_or_create_relay_records,
    _relay_prepare_items,
    _relay_native_dust_below_threshold,
    _relay_submit_auto_sweep,
    _relay_snapshot_cleanup_allowed,
    _relay_source_stage,
    _relay_simple_stage,
    _relay_submit_with_retry,
    _relay_terminal_snapshot_can_be_ignored,
    _relay_token_dust_below_threshold,
    _relay_token_dust_requires_native_recovery,
    _wait_for_receipt_success,
    run_relay_fee_recovery,
)
from onchain_relay_wallets import RelayWalletRecord


@contextmanager
def _close_sweep_log_after():
    """Release the fixture's file before Windows removes its directory."""
    try:
        yield
    finally:
        import onchain_relay_runner as runner
        with runner._RELAY_SWEEP_LOG_LOCK:
            if runner._RELAY_SWEEP_LOG_HANDLER is not None:
                runner._RELAY_SWEEP_LOG_HANDLER.close()
                runner._RELAY_SWEEP_LOG_HANDLER = None


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

    def get_confirmed_nonce(self, _network: str, address: str) -> int:
        return self.nonce_by_addr.setdefault(address, 0)

    def get_transaction_receipt(self, _network: str, _txid: str, **_kwargs) -> dict | None:
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
    def test_relay_wallet_preparation_uses_shared_wallet_generator(self) -> None:
        owner = Mock()
        owner._is_mode_m1.return_value = False
        owner._mode.return_value = "1M"
        owner._decimal_to_text.side_effect = lambda value: str(value)
        wallets = [
            GeneratedWalletEntry(address="0x" + "1" * 40, private_key="a" * 64),
            GeneratedWalletEntry(address="0x" + "2" * 40, private_key="b" * 64),
        ]
        owner.client.create_wallets.return_value = wallets
        owner.relay_wallet_store.build_record.side_effect = lambda **kwargs: kwargs["relay_wallet"]
        jobs = [
            ("row-1", "source-1", "0x" + "3" * 40),
            ("row-2", "source-2", "0x" + "4" * 40),
        ]
        source_wallets = {
            "row-1": ("private-1", "0x" + "5" * 40),
            "row-2": ("private-2", "0x" + "6" * 40),
        }
        params = WithdrawRuntimeParams(
            coin="BNB",
            amount="0.01",
            network="BSC",
            delay=0,
            threads=64,
            relay_enabled=True,
            relay_fee_reserve=Decimal("0.001"),
        )

        records, resumed_count, batch_id = _pick_or_create_relay_records(
            owner,
            jobs,
            params,
            source_wallets,
            {},
        )

        owner.client.create_wallets.assert_called_once_with(2, worker_threads=2)
        owner.relay_wallet_store.append_records.assert_called_once_with(wallets)
        self.assertEqual(records, {"row-1": wallets[0], "row-2": wallets[1]})
        self.assertEqual(resumed_count, 0)
        self.assertTrue(batch_id.startswith("relay-"))

    def setUp(self) -> None:
        _RECORDS.clear()

    def test_receipt_state_requires_status_one(self) -> None:
        self.assertEqual(_receipt_state({"status": "0x1"}), "success")
        self.assertEqual(_receipt_state({"status": "1"}), "success")
        self.assertEqual(_receipt_state({"status": "0x0"}), "failed")
        self.assertEqual(_receipt_state(None), "pending")

    def test_stop_during_retry_retains_original_unconfirmed_transaction(self) -> None:
        from onchain_transaction_recovery import UnconfirmedTransactionError
        owner, item = _FakeOwner(), _item(1)
        original = UnconfirmedTransactionError("reply lost", "0x" + "a" * 64, 7, "intent")
        def lost_reply():
            owner.stop_requested.set()
            raise original
        submitter = Mock(side_effect=lost_reply)
        with self.assertRaises(UnconfirmedTransactionError) as raised:
            _relay_submit_with_retry(owner, _params(), item, "source transfer", submitter)
        self.assertIs(raised.exception, original)
        self.assertEqual(raised.exception.nonce, 7)
        submitter.assert_called_once()

    def test_relay_amount_branch_uses_started_mode_even_after_ui_mode_changes(self) -> None:
        owner = _FakeOwner(mode_m1=True)
        owner._is_mode_m1 = Mock(side_effect=AssertionError("worker must use its mode snapshot"))
        owner._is_mode_1m = Mock(side_effect=AssertionError("worker must use its mode snapshot"))
        item = _item(1)
        params = replace(_params(coin="BNB", token_is_native=True), mode="1对多")
        items = _relay_prepare_items(owner, [(item.row_key, "source-key", item.target)], params,
                                     {item.row_key: 1}, {item.row_key: item.relay_record},
                                     {item.row_key: ("source-key", item.source_addr)})
        self.assertEqual(items[0].value_units, 10**18)
        self.assertEqual(items[0].amount_text, "1")
        params = replace(params, mode="多对1")
        items = _relay_prepare_items(owner, [(item.row_key, "source-key", item.target)], params,
                                     {item.row_key: 1}, {item.row_key: item.relay_record},
                                     {item.row_key: ("source-key", item.source_addr)})
        self.assertEqual(items[0].value_units, 0)  # The many-to-one amount is resolved from balance later.

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
            token_forward_txid="0x" + "f" * 64,
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_responses = [RuntimeError("temporary RPC failure"), 0]
        owner.client.erc20_balance_units = 0

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
                _close_sweep_log_after(),
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

    def test_manual_recovery_does_not_treat_empty_relay_as_completed_transfer(self) -> None:
        owner = _FakeOwner()
        relay_addr = "0x00000000000000000000000000000000000000d0"
        record = RelayWalletRecord(
            created_at="",
            expires_at="",
            network="BSC",
            source="0x00000000000000000000000000000000000000aa",
            target="0x00000000000000000000000000000000000000bb",
            relay_address=relay_addr,
            private_key="relay-private-key",
            status="forwarded",
            batch_id="batch-empty-without-forward",
        )
        _RECORDS[(relay_addr, record.batch_id)] = record
        owner.client.native_balance_wei = 0
        owner.client.erc20_balance_units = 0

        with TemporaryDirectory() as tmp_dir:
            with (
                patch("onchain_relay_runner.RELAY_MANUAL_EXPORT_FILE", Path(tmp_dir) / "manual.txt"),
                patch("onchain_relay_runner.RELAY_SWEEP_LOG_FILE", Path(tmp_dir) / "sweep.jsonl"),
                _close_sweep_log_after(),
            ):
                run_relay_fee_recovery(
                    owner,
                    False,
                    batch_ids={record.batch_id},
                    show_dialog=True,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="batch-empty-without-forward",
                )

        self.assertIn((relay_addr, record.batch_id), _RECORDS)
        saved = _RECORDS[(relay_addr, record.batch_id)]
        self.assertEqual(saved.status, "failed")
        self.assertIn("缺少第二跳交易记录", saved.last_error)
        self.assertFalse(any("无需回收" in message for message in owner.logs))
        result = owner.result_dialogs[0]
        self.assertEqual(result["success"], 0)
        self.assertEqual(result["failed"], 1)

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
                _close_sweep_log_after(),
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
                _close_sweep_log_after(),
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
                _close_sweep_log_after(),
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

    def test_receipt_after_tenth_poll_still_succeeds_within_confirmation_timeout(self) -> None:
        owner = _FakeOwner()
        owner.client.receipts = [None] * 10 + [{"status": "0x1"}]
        params = replace(_params(), confirm_timeout_seconds=180, receipt_retry_count=10,
                         receipt_poll_interval_seconds=3)
        now = [0.0]

        def wait(seconds):
            now[0] += seconds
            return False

        with (
            patch("onchain_relay_runner.time.monotonic", side_effect=lambda: now[0]),
            patch.object(owner.stop_requested, "wait", side_effect=wait),
        ):
            receipt = _wait_for_receipt_success(owner, params, "0x" + "1" * 64, label="测试交易")
        self.assertEqual(receipt["status"], "0x1")
        self.assertEqual(len(owner.client.receipt_requests), 11)
        self.assertEqual(now[0], 30)

    def test_pending_receipt_waits_until_deadline_and_preserves_hash_in_error(self) -> None:
        owner = _FakeOwner()
        owner.client.receipts = [None]
        params = replace(_params(), confirm_timeout_seconds=10, receipt_retry_count=2,
                         receipt_poll_interval_seconds=3)
        now = [0.0]
        txid = "0x" + "2" * 64

        def wait(seconds):
            now[0] += seconds
            return False

        with (
            patch("onchain_relay_runner.time.monotonic", side_effect=lambda: now[0]),
            patch.object(owner.stop_requested, "wait", side_effect=wait),
            self.assertRaisesRegex(RuntimeError, f"等待 10.0 秒.*{txid}.*尚未确认"),
        ):
            _wait_for_receipt_success(owner, params, txid, label="测试交易")
        self.assertEqual(now[0], 10)
        self.assertEqual(len(owner.client.receipt_requests), 4)

    def test_receipt_wait_stops_promptly_on_user_stop(self) -> None:
        owner = _FakeOwner()
        owner.client.receipts = [None]
        with patch.object(owner.stop_requested, "wait", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "任务已停止"):
                _wait_for_receipt_success(owner, _params(), "0x" + "1" * 64, label="测试交易")
        self.assertEqual(len(owner.client.receipt_requests), 1)

    def test_slow_rpc_does_not_extend_confirmation_to_minimum_retry_count(self) -> None:
        owner = _FakeOwner()
        params = replace(_params(), confirm_timeout_seconds=10, receipt_retry_count=10,
                         receipt_poll_interval_seconds=3)
        now = [0.0]

        def slow_receipt(*_args, timeout_seconds, stop_event):
            self.assertIs(stop_event, owner.stop_requested)
            now[0] += timeout_seconds
            raise RuntimeError("RPC 查询超时")

        with (
            patch("onchain_relay_runner.time.monotonic", side_effect=lambda: now[0]),
            patch.object(owner.client, "get_transaction_receipt", side_effect=slow_receipt) as query,
            patch.object(owner.stop_requested, "wait") as wait,
            self.assertRaisesRegex(RuntimeError, "等待 10.0 秒.*查询 receipt 1 次"),
        ):
            _wait_for_receipt_success(owner, params, "0x" + "1" * 64, label="慢节点")
        query.assert_called_once()
        wait.assert_not_called()

    def test_failed_onchain_receipt_is_not_retried_until_deadline(self) -> None:
        owner = _FakeOwner()
        owner.client.receipts = [{"status": "0x0"}]
        with self.assertRaisesRegex(RuntimeError, "receipt status=0"):
            _wait_for_receipt_success(owner, _params(), "0x" + "1" * 64, label="测试交易")
        self.assertEqual(len(owner.client.receipt_requests), 1)

    def test_second_hop_mixed_receipts_finish_and_retry_without_rebroadcast(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        params = replace(_params(coin="BNB", token_is_native=True), threads=8,
                         confirm_timeout_seconds=1, receipt_retry_count=10)
        items = [_item(i) for i in range(1, 9)]
        calls = {}

        def receipt(_network, txid, **_kwargs):
            index = int(txid, 16)
            calls[index] = calls.get(index, 0) + 1
            if index == 7:
                return None
            if index == 8:
                raise RuntimeError("RPC 节点超时")
            if index == 6 and calls[index] == 1:
                return None
            return {"status": "0x1"}

        submit = Mock(side_effect=lambda item, nonce: f"0x{item.index:064x}")
        stage_args = dict(
            stage_label="中转第二跳", tx_attr="token_forward_txid", nonce_attr="token_forward_nonce",
            submitted_status="token_forward_submitted", done_status="forwarded",
            done_statuses={"forwarded", "completed"}, submit_one=submit, status_text="第二跳",
        )
        owner.client.get_transaction_receipt = receipt
        _relay_simple_stage(owner, params, items, **stage_args)
        self.assertEqual(submit.call_count, 8)
        self.assertEqual([item.relay_record.status for item in items], ["forwarded"] * 6 + ["failed"] * 2)
        for item in items[6:]:
            self.assertEqual(item.relay_record.token_forward_txid, f"0x{item.index:064x}")
            self.assertIn("尚未确认", item.relay_record.last_error)

        # 模拟重新载入保存记录：全部记录都复核第二跳回执，已提交交易不再次转账。
        resumed = [replace(item, failed=False, failure_reason="") for item in items]
        owner.client.get_transaction_receipt = Mock(return_value={"status": "0x1"})
        submit.reset_mock()
        _relay_simple_stage(owner, params, resumed, **stage_args)
        submit.assert_not_called()
        self.assertEqual(owner.client.get_transaction_receipt.call_count, 8)
        self.assertTrue(all(item.relay_record.status == "forwarded" for item in resumed))

    def test_second_hop_forwarded_status_without_txid_is_failed(self) -> None:
        owner = _FakeOwner(mode_m1=True)
        item = _item(1)
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id="batch",
            status="forwarded",
            token_forward_txid="",
        )
        submit = Mock(side_effect=AssertionError("不得凭本地 forwarded 状态跳过回执核验"))

        _relay_simple_stage(
            owner,
            _params(),
            [item],
            stage_label="中转第二跳",
            tx_attr="token_forward_txid",
            nonce_attr="token_forward_nonce",
            submitted_status="token_forward_submitted",
            done_status="forwarded",
            done_statuses={"forwarded", "completed"},
            submit_one=submit,
            status_text="第二跳",
        )

        submit.assert_not_called()
        self.assertTrue(item.failed)
        self.assertEqual(item.relay_record.status, "failed")
        self.assertIn("未拿到 txid", item.relay_record.last_error)

    def test_second_hop_stop_retains_transaction_for_resume(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        submit = Mock(return_value="0x" + "a" * 64)
        stage_args = dict(
            stage_label="中转第二跳", tx_attr="token_forward_txid", nonce_attr="token_forward_nonce",
            submitted_status="token_forward_submitted", done_status="forwarded",
            done_statuses={"forwarded"}, submit_one=submit, status_text="第二跳",
        )

        def stopped_receipt(*_args, **_kwargs):
            owner.stop_requested.set()
            raise RuntimeError("任务已停止")

        owner.client.get_transaction_receipt = stopped_receipt
        _relay_simple_stage(owner, _params(), [item], **stage_args)
        self.assertEqual(item.relay_record.last_error, "任务已停止")
        self.assertEqual(item.relay_record.token_forward_txid, "0x" + "a" * 64)
        owner.stop_requested.clear()
        resumed = replace(item, failed=False, failure_reason="")
        owner.client.get_transaction_receipt = Mock(return_value={"status": "0x1"})
        submit.reset_mock()
        _relay_simple_stage(owner, _params(), [resumed], **stage_args)
        submit.assert_not_called()
        self.assertEqual(resumed.relay_record.status, "forwarded")

    def test_1m_retry_confirms_original_successful_transaction_without_resubmitting(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        txid = "0x" + "b" * 64
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr, batch_id="batch", status="failed",
            token_funded_txid=txid, token_funded_nonce="7", last_error="receipt 未返回",
        )
        params = _params(coin="BNB", token_is_native=True)
        _relay_clear_missing_source_txids(
            owner, params, [item], tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce", done_status="token_funded", done_statuses={"token_funded"},
        )
        submit = Mock(side_effect=AssertionError("不得重复转账"))
        _relay_source_stage(
            owner, params, [item], stage_label="源钱包原生币转入中转",
            tx_attr="token_funded_txid", nonce_attr="token_funded_nonce",
            submitted_status="token_funding_submitted", done_status="token_funded",
            done_statuses={"token_funded"}, submit_one=submit, status_text="原生币转入",
        )
        submit.assert_not_called()
        self.assertEqual(item.relay_record.status, "token_funded")
        self.assertEqual(item.relay_record.token_funded_txid, txid)
        self.assertEqual(item.relay_record.last_error, "")

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

    def test_1m_native_preflight_rejects_insufficient_batch_balance(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        owner.client.gas_price_wei = 2
        items = [_item(1), _item(2)]
        for item in items:
            item.value_units = 100
            item.reserve_units = 10
        required = 2 * (100 + 10 + 2 * owner.client.NATIVE_GAS_LIMIT)
        owner.client.native_balance_wei = required - 1

        with self.assertRaisesRegex(RuntimeError, "一对多中转整批余额不足"):
            _relay_preflight_1m_native_source(
                owner,
                _params(coin="BNB", token_is_native=True),
                items,
                tx_attr="token_funded_txid",
                done_statuses={"token_funded"},
            )

        self.assertEqual(owner.client.submitted_native_transfers, [])

    def test_1m_native_transfer_refreshes_preflight_gas_price_without_changing_amount(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        item.reserve_units = 10**15
        item.source_token_gas_price = 50_000_000
        owner.client.gas_price_wei = 110_000_000
        owner.client.native_balance_wei = item.value_units + item.reserve_units + 110_000_000 * 21000
        original = (item.value_units, item.amount_text, item.reserve_units, item.source_addr, item.relay_addr, item.target)

        _relay_resolve_1m_native_transfer(owner, _params(coin="BNB", token_is_native=True), item)

        self.assertEqual(item.source_token_gas_price, 110_000_000)
        self.assertEqual(item.source_token_gas_limit, 21000)
        self.assertEqual((item.value_units, item.amount_text, item.reserve_units, item.source_addr, item.relay_addr, item.target), original)
        self.assertEqual(owner.client.submitted_native_transfers, [])

    def test_1m_native_transfer_rechecks_balance_after_gas_price_increases(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        item.source_token_gas_price = 50_000_000
        owner.client.gas_price_wei = 110_000_000
        owner.client.native_balance_wei = item.value_units + item.reserve_units + 50_000_000 * 21000

        with self.assertRaisesRegex(RuntimeError, "源钱包原生币余额不足"):
            _relay_resolve_1m_native_transfer(owner, _params(coin="BNB", token_is_native=True), item)

        self.assertEqual(owner.client.submitted_native_transfers, [])
        self.assertEqual(item.value_units, 10**18)

    def test_1m_native_preflight_failure_uses_new_amount_on_next_start(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        owner.client.gas_price_wei = 2
        items = [_item(1), _item(2)]
        for item in items:
            item.value_units = 10**18
            item.amount_text = "1"
            item.relay_record = owner.relay_wallet_store.update_record(
                item.relay_addr,
                batch_id=item.relay_record.batch_id,
                transfer_amount="1",
                transfer_units=str(10**18),
            )
        owner.client.native_balance_wei = 1

        with self.assertRaisesRegex(RuntimeError, "一对多中转整批余额不足"):
            _relay_preflight_1m_native_source(
                owner,
                _params(coin="BNB", token_is_native=True),
                items,
                tx_attr="token_funded_txid",
                done_statuses={"token_funded"},
            )

        updated_params = replace(_params(coin="BNB", token_is_native=True), amount="0.0001")
        for item in items:
            saved = _RECORDS[(item.relay_addr, "batch")]
            self.assertEqual(saved.transfer_amount, "")
            self.assertEqual(saved.transfer_units, "")
            value_units, amount_text = _resolve_record_amount_local(owner, saved, updated_params)
            self.assertEqual(value_units, 10**14)
            self.assertEqual(amount_text, "0.0001")

    def test_1m_native_resume_ignores_stale_unsubmitted_amount(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        stale_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id=item.relay_record.batch_id,
            transfer_amount="1",
            transfer_units=str(10**18),
        )
        params = replace(_params(coin="BNB", token_is_native=True), amount="0.0001")
        jobs = [(item.row_key, item.source_raw, item.target)]
        source_wallets = {item.row_key: (item.source_private_key, item.source_addr)}
        candidates = {(item.source_addr.lower(), item.target.lower()): [stale_record]}

        records, resumed_count, _batch_id = _pick_or_create_relay_records(
            owner,
            jobs,
            params,
            source_wallets,
            candidates,
        )
        prepared = _relay_prepare_items(
            owner,
            jobs,
            params,
            {item.row_key: 1},
            records,
            source_wallets,
        )

        self.assertEqual(resumed_count, 1)
        self.assertEqual(prepared[0].amount_text, "0.0001")
        self.assertEqual(prepared[0].value_units, 10**14)

    def test_1m_native_retry_clears_missing_txid_and_keeps_nonce(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        missing_txid = "0x" + "b" * 64
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id="batch",
            status="failed",
            token_funded_txid=missing_txid,
            token_funded_nonce="7",
        )
        owner.client.receipts = [None]
        owner.client.get_transaction_by_hash = lambda *_args, **_kwargs: None
        owner.client.native_balance_wei = 0
        owner.client.nonce_by_addr[item.source_addr] = 7

        _relay_clear_missing_source_txids(
            owner,
            _params(coin="BNB", token_is_native=True),
            [item],
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            done_status="token_funded",
            done_statuses={"token_funded"},
        )

        self.assertEqual(item.relay_record.token_funded_txid, "")
        self.assertEqual(item.relay_record.token_funded_nonce, "7")
        self.assertTrue(any("保留 nonce=7 重新广播" in message for message in owner.logs))

    def test_1m_native_retry_uses_new_nonce_when_old_nonce_is_confirmed(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        missing_txid = "0x" + "d" * 64
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id="batch",
            status="failed",
            token_funded_txid=missing_txid,
            token_funded_nonce="7",
        )
        owner.client.receipts = [None]
        owner.client.get_transaction_by_hash = lambda *_args, **_kwargs: None
        owner.client.native_balance_wei = 0
        owner.client.nonce_by_addr[item.source_addr] = 8

        _relay_clear_missing_source_txids(
            owner,
            _params(coin="BNB", token_is_native=True),
            [item],
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            done_status="token_funded",
            done_statuses={"token_funded"},
        )

        self.assertEqual(item.relay_record.token_funded_txid, "")
        self.assertEqual(item.relay_record.token_funded_nonce, "")
        self.assertTrue(any("改用当前 nonce 重新广播" in message for message in owner.logs))

    def test_1m_native_retry_recovers_when_relay_is_already_funded(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        item.value_units = 100
        item.reserve_units = 10
        missing_txid = "0x" + "e" * 64
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id="batch",
            status="failed",
            token_funded_txid=missing_txid,
            token_funded_nonce="7",
        )
        owner.client.receipts = [None]
        owner.client.get_transaction_by_hash = lambda *_args, **_kwargs: None
        owner.client.native_balance_wei = 110

        _relay_clear_missing_source_txids(
            owner,
            _params(coin="BNB", token_is_native=True),
            [item],
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            done_status="token_funded",
            done_statuses={"token_funded"},
        )

        self.assertEqual(item.relay_record.status, "token_funded")
        self.assertEqual(item.relay_record.token_funded_txid, missing_txid)
        self.assertTrue(any("中转钱包已足额到账" in message for message in owner.logs))

    def test_1m_native_retry_keeps_txid_when_transaction_still_exists(self) -> None:
        owner = _FakeOwner(mode_1m=True)
        item = _item(1)
        pending_txid = "0x" + "c" * 64
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id="batch",
            status="failed",
            token_funded_txid=pending_txid,
            token_funded_nonce="7",
        )
        owner.client.receipts = [None]
        owner.client.get_transaction_by_hash = lambda *_args, **_kwargs: {"hash": pending_txid}

        _relay_clear_missing_source_txids(
            owner,
            _params(coin="BNB", token_is_native=True),
            [item],
            tx_attr="token_funded_txid",
            nonce_attr="token_funded_nonce",
            done_status="token_funded",
            done_statuses={"token_funded"},
        )

        self.assertEqual(item.relay_record.token_funded_txid, pending_txid)
        self.assertFalse(any("重新广播" in message for message in owner.logs))

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

        def receipt_after_submits(network: str, txid: str, **kwargs):
            events.append("receipt")
            return original_receipt(network, txid, **kwargs)

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

    def test_unknown_source_submission_keeps_original_hash_and_nonce(self) -> None:
        from onchain_transaction_recovery import UnconfirmedTransactionError
        for own in (True, False):
            with self.subTest(own_transaction=own):
                owner = _FakeOwner()
                item = _item(10 if own else 11)
                txid = "0x" + "a" * 64
                def submit(*_args):
                    raise UnconfirmedTransactionError("reply lost", txid, 7, own_transaction=own)
                _relay_source_stage(owner, _params(delay=0), [item], stage_label="测试",
                                    tx_attr="token_funded_txid", nonce_attr="token_funded_nonce",
                                    submitted_status="token_funding_submitted", done_status="token_funded",
                                    done_statuses={"token_funded"}, submit_one=submit, status_text="测试")
                self.assertTrue(item.failed)
                self.assertEqual(item.relay_record.token_funded_txid, txid if own else "")
                if own:
                    self.assertEqual(item.relay_record.token_funded_nonce, "7")

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
