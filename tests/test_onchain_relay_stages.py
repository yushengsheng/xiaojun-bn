#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import unittest
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

from api_clients import SubmittedTransaction
from core_models import WithdrawRuntimeParams
from onchain_relay_runner import run_relay_batch
from onchain_relay_wallets import RelayWalletRecord


SOURCE_ADDR = "0x00000000000000000000000000000000000000aa"
RELAY_ADDR = "0x00000000000000000000000000000000000000bb"
TARGET_ADDR = "0x00000000000000000000000000000000000000cc"
TOKEN_CONTRACT = "0x0000000000000000000000000000000000000011"


class _RelayStageClient:
    NATIVE_GAS_LIMIT = 21000

    def __init__(self):
        self.gas_failures_remaining = 1
        self.gas_calls = 0
        self.target_balance_reads = 0
        self.submissions: list[str] = []

    def credential_to_private_key(self, credential: str) -> str:
        return str(credential or "").strip() or "relay-private-key"

    def get_gas_price_wei(self, _network: str) -> int:
        self.gas_calls += 1
        if self.gas_failures_remaining > 0:
            self.gas_failures_remaining -= 1
            raise RuntimeError("BSC RPC 请求失败：Read timed out")
        return 1

    def get_nonce(self, _network: str, _address: str) -> int:
        return 0

    def get_balance_wei(self, _network: str, _address: str, **_kwargs) -> int:
        return 10**24

    def get_erc20_balance(self, _network: str, _contract: str, address: str, **_kwargs) -> int:
        if str(address).lower() == TARGET_ADDR.lower():
            self.target_balance_reads += 1
            return 0 if self.target_balance_reads == 1 else 10**18
        return 10**18

    def estimate_erc20_transfer_gas(
        self,
        _network: str,
        _from_address: str,
        _contract: str,
        _to_address: str,
        _amount_units: int,
    ) -> int:
        return 70000

    def submit_native_transfer_reliably(self, **_kwargs) -> SubmittedTransaction:
        txid = "0x" + f"{len(self.submissions) + 1:064x}"[-64:]
        self.submissions.append(txid)
        return SubmittedTransaction(tx_hash=txid)

    def submit_erc20_transfer_reliably(self, **_kwargs) -> SubmittedTransaction:
        txid = "0x" + f"{len(self.submissions) + 1:064x}"[-64:]
        self.submissions.append(txid)
        return SubmittedTransaction(tx_hash=txid)

    def get_transaction_receipt(self, _network: str, _txid: str, **_kwargs):
        return {"status": "0x1"}

    @staticmethod
    def _int_from_hex(value) -> int:
        return int(str(value), 16)


class _RelayStore:
    def __init__(self, record: RelayWalletRecord):
        self.record = record

    def cleanup_expired_empty_records(self, *_args, **_kwargs):
        return 0, 0, 0

    def load_records(self):
        return []

    def update_record(self, _relay_addr: str, *, batch_id: str = "", status: str = "", **changes):
        self.record = replace(self.record, status=status or self.record.status, **changes)
        return self.record


class _RelayOwner:
    def __init__(self, record: RelayWalletRecord):
        self.client = _RelayStageClient()
        self.relay_wallet_store = _RelayStore(record)
        self.stop_requested = threading.Event()
        self.logs: list[str] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.root = type("Root", (), {"_closing": False})()
        self._closing = False

    def _dispatch_ui(self, callback):
        callback()

    @staticmethod
    def _row_number_by_key_snapshot() -> dict[str, int]:
        return {}

    @staticmethod
    def _row_log_prefix(_row_key: str, index: int, total: int, _row_numbers: dict[str, int]) -> str:
        return f"[{index}/{total}]"

    @staticmethod
    def _token_desc_from_params(params) -> str:
        return str(params.coin)

    @staticmethod
    def _unique_row_keys(keys: list[str]) -> list[str]:
        return list(dict.fromkeys(keys))

    @staticmethod
    def _row_context_for_values(_row_key: str, source: str, target: str) -> str:
        return f"{source}->{target}"

    @staticmethod
    def _is_mode_m1() -> bool:
        return False

    @staticmethod
    def _mode() -> str:
        return "多对多"

    @staticmethod
    def _runtime_worker_threads() -> int:
        return 1

    @staticmethod
    def _runtime_confirmation_poll_interval_seconds() -> float:
        return 0.2

    def _resolve_amount_and_gas(self, _params, _source_addr: str, _target_addr: str):
        return 10**18, 1, 70000, "1"

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
    def _estimated_gas_fee_text(_network: str, fee_wei: int) -> str:
        return f"预估 {fee_wei} wei"

    @staticmethod
    def _gas_fee_amount_text(_network: str, fee_wei: int) -> str:
        return f"{fee_wei} wei"

    @staticmethod
    def _network_fee_symbol(_network: str) -> str:
        return "BNB"

    @staticmethod
    def _mask(value: str, *, head: int = 6, tail: int = 4) -> str:
        text = str(value)
        return f"{text[:head]}...{text[-tail:]}"

    def _mask_credential(self, value: str) -> str:
        return self._mask(value)

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

    @staticmethod
    def _mark_recovery_status_context(_row_key: str, _context: str) -> None:
        return None

    @staticmethod
    def _set_recovery_status(_row_key: str, _status: str, _text: str = "") -> None:
        return None

    def _set_status(self, row_key: str, status: str, text: str = "") -> None:
        self.statuses.append((row_key, status, text))

    @staticmethod
    def _show_result_summary_dialog(**_kwargs) -> None:
        return None

    @staticmethod
    def _finish_progress(_kind: str, _success: int, _failed: int) -> None:
        return None

    @staticmethod
    def _clear_source_private_key_cache() -> None:
        return None


def _relay_record() -> RelayWalletRecord:
    return RelayWalletRecord(
        created_at="2026-06-29T00:00:00Z",
        expires_at="2026-07-01T00:00:00Z",
        network="BSC",
        source=SOURCE_ADDR,
        target=TARGET_ADDR,
        relay_address=RELAY_ADDR,
        private_key="relay-private-key",
        status="created",
        batch_id="relay-stage-retry-test",
        token_symbol="USDT",
        token_contract=TOKEN_CONTRACT,
        token_decimals=18,
        token_is_native=False,
        relay_fee_reserve="1",
        transfer_amount="1",
        transfer_units=str(10**18),
    )


class RelayStageRetryTests(unittest.TestCase):
    def test_relay_batch_retries_transient_stage_rpc_error_without_failing_record(self) -> None:
        record = _relay_record()
        owner = _RelayOwner(record)
        params = WithdrawRuntimeParams(
            coin="USDT",
            amount="1",
            network="BSC",
            delay=0,
            threads=1,
            confirm_timeout_seconds=3.0,
            confirm_poll_interval_seconds=0.2,
            token_contract=TOKEN_CONTRACT,
            token_decimals=18,
            token_is_native=False,
            relay_enabled=True,
            relay_fee_reserve=Decimal("1"),
        )
        jobs = [("row-1", "source-private-key", TARGET_ADDR)]

        with patch("onchain_relay_runner._resolve_job_source_wallets", return_value={"row-1": ("source-private-key", SOURCE_ADDR)}), \
            patch("onchain_relay_runner._build_resume_candidates", return_value={}), \
            patch("onchain_relay_runner._pick_or_create_relay_records", return_value=({"row-1": record}, 0, record.batch_id)), \
            patch("onchain_relay_runner._reconcile_relay_record_stage", side_effect=lambda _owner, rec, *_args, **_kwargs: rec):
            run_relay_batch(owner, jobs, params, dry_run=False)

        self.assertGreaterEqual(owner.client.gas_calls, 2)
        self.assertTrue(any("准备重试 2/10" in line for line in owner.logs))
        self.assertIn(("row-1", "success", "✅1usdt"), owner.statuses)
        self.assertEqual(owner.relay_wallet_store.record.status, "completed")

    def test_relay_batch_failure_status_shows_balance_reason(self) -> None:
        record = _relay_record()
        owner = _RelayOwner(record)
        owner.client.gas_failures_remaining = 0

        def fail_amount_and_gas(*_args, **_kwargs):
            raise RuntimeError("源钱包原生币余额不足，无法支付 gas")

        owner._resolve_amount_and_gas = fail_amount_and_gas  # type: ignore[method-assign]
        params = WithdrawRuntimeParams(
            coin="USDT",
            amount="1",
            network="BSC",
            delay=0,
            threads=1,
            confirm_timeout_seconds=3.0,
            confirm_poll_interval_seconds=0.2,
            token_contract=TOKEN_CONTRACT,
            token_decimals=18,
            token_is_native=False,
            relay_enabled=True,
            relay_fee_reserve=Decimal("1"),
        )
        jobs = [("row-1", "source-private-key", TARGET_ADDR)]

        with patch("onchain_relay_runner._resolve_job_source_wallets", return_value={"row-1": ("source-private-key", SOURCE_ADDR)}), \
            patch("onchain_relay_runner._build_resume_candidates", return_value={}), \
            patch("onchain_relay_runner._pick_or_create_relay_records", return_value=({"row-1": record}, 0, record.batch_id)), \
            patch("onchain_relay_runner._reconcile_relay_record_stage", side_effect=lambda _owner, rec, *_args, **_kwargs: rec):
            run_relay_batch(owner, jobs, params, dry_run=False)

        self.assertIn(("row-1", "failed", "❌源钱包原生币不足"), owner.statuses)


if __name__ == "__main__":
    unittest.main()
