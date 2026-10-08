#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from contextlib import nullcontext
import json
import logging
import queue
import threading
import time
from dataclasses import dataclass, replace
from decimal import Decimal
from tkinter import messagebox

from app_defaults import (
    ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
    ONCHAIN_RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
)
from app_paths import RELAY_FAILED_EXPORT_FILE, RELAY_MANUAL_EXPORT_FILE, RELAY_SWEEP_LOG_FILE
from core_models import EvmToken, WithdrawRuntimeParams
from daily_logging import DailyFileHandler
from onchain_relay_wallets import RelayWalletRecord, _atomic_write_text
from shared_utils import set_ui_batch_size

RELAY_CONFIRM_TIMEOUT_SECONDS = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_BALANCE_TIMEOUT_SECONDS = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS = ONCHAIN_RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_POLL_INTERVAL_SECONDS = 2.0
RELAY_SWEEP_SAFETY_GAS_MULTIPLIER = 1
RELAY_SWEEP_TERMINAL_RESOLUTIONS = {"manual_empty", "dust_left"}
RELAY_TRACKED_TOKEN_SYMBOLS = {"USDT", "USDC"}
RELAY_STABLE_TOKEN_SYMBOLS = {"USDT", "USDC"}
RELAY_STABLE_TOKEN_DUST_THRESHOLD = Decimal("0.0001")
RELAY_BNB_DUST_THRESHOLD = Decimal("0.000005")
_RELAY_SWEEP_LOG_LOCK = threading.Lock()
_RELAY_SWEEP_LOG_HANDLER = None


def _relay_mode_is(owner, params: WithdrawRuntimeParams, mode: str) -> bool:
    # Older direct callers may omit mode; GUI tasks always supply an immutable snapshot.
    if params.mode:
        return params.mode in ({"1对多", "1M"} if mode == "1m" else {"多对1", "M1"})
    checker = getattr(owner, "_is_mode_" + mode, None)
    return bool(checker()) if callable(checker) else False


def _start_relay_rpc_worker(owner, target, *, name: str) -> threading.Thread:
    starter = getattr(owner, "_start_rpc_worker_thread", None)
    if callable(starter):
        return starter(target, name=name)
    thread = threading.Thread(target=target, daemon=True, name=name)
    thread.start()
    return thread


class RelayAutoForwardPending(RuntimeError):
    def __init__(self, message: str, *, txid: str = ""):
        super().__init__(message)
        self.txid = str(txid or "").strip()


def _utc_now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _relay_sweep_timeout_seconds(base_timeout_seconds: float | None = None) -> float:
    try:
        base_value = float(base_timeout_seconds) if base_timeout_seconds is not None else 0.0
    except Exception:
        base_value = 0.0
    return max(RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS, max(1.0, base_value))


def _append_relay_sweep_log(event: str, **fields: object) -> None:
    global _RELAY_SWEEP_LOG_HANDLER
    payload = {
        "ts": _utc_now_text(),
        "event": str(event or "").strip() or "unknown",
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[str(key)] = value
    from sensitive_data import redact_sensitive_text
    line = redact_sensitive_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    path = RELAY_SWEEP_LOG_FILE
    try:
        with _RELAY_SWEEP_LOG_LOCK:
            if _RELAY_SWEEP_LOG_HANDLER is None or _RELAY_SWEEP_LOG_HANDLER.path != path:
                if _RELAY_SWEEP_LOG_HANDLER is not None:
                    _RELAY_SWEEP_LOG_HANDLER.close()
                _RELAY_SWEEP_LOG_HANDLER = DailyFileHandler(path)
            _RELAY_SWEEP_LOG_HANDLER.handle(logging.LogRecord("relay.sweep", logging.INFO, __file__, 0, line, (), None))
    except Exception:
        pass


def _relay_success_status_text(owner, params: WithdrawRuntimeParams, amount_text: str) -> str:
    amount = str(amount_text or "").strip()
    if amount:
        try:
            formatter = getattr(owner, "_stat_decimal_to_text", owner._decimal_to_text)
            amount = formatter(Decimal(amount))
        except Exception:
            pass
    coin_text = str(getattr(params, "coin", "") or "").strip().lower()
    if amount and coin_text:
        return f"✅{amount}{coin_text}"
    if amount:
        return f"✅{amount}"
    return "✅"


def _relay_native_main_recovery_status_text(owner, params: WithdrawRuntimeParams, amount_text: str) -> str:
    amount = str(amount_text or "").strip()
    if not amount:
        return "回收-"
    try:
        return f"回收{owner._token_amount_text(params.coin, Decimal(amount))}"
    except Exception:
        symbol = str(getattr(params, "coin", "") or _relay_native_symbol(owner, params.network)).strip().upper()
        return f"回收{amount} {symbol}".strip()


def _relay_recovery_row_key(owner, record: RelayWalletRecord, *, fallback_index: int = 0) -> str:
    batch_id = str(getattr(record, "batch_id", "") or "").strip()
    network = str(getattr(record, "network", "") or "").strip().upper()
    relay_addr = str(getattr(record, "relay_address", "") or "").strip()
    if batch_id:
        return f"relay-sweep:{batch_id}:{network}:{relay_addr}"
    if fallback_index > 0:
        return f"relay-sweep:{fallback_index}:{network}:{relay_addr}"
    return f"relay-sweep:{network}:{relay_addr}"


def _relay_recovery_display_row_key(owner, record: RelayWalletRecord) -> str:
    target = str(getattr(record, "target", "") or "").strip()
    source = str(getattr(record, "source", "") or "").strip()
    if not source:
        return ""
    if hasattr(owner, "_is_mode_1m") and owner._is_mode_1m():
        if not target or not hasattr(owner, "_one_to_many_key"):
            return ""
        try:
            current_source = str(owner.source_credential_var.get() or "").strip()
        except Exception:
            current_source = ""
        if not current_source:
            return ""
        resolved_current_source = current_source
        if hasattr(owner, "_resolve_source_address"):
            try:
                resolved_current_source = str(owner._resolve_source_address(current_source) or "").strip()
            except Exception:
                resolved_current_source = ""
        if resolved_current_source.lower() != source.lower():
            return ""
        try:
            targets = set(getattr(owner.store, "one_to_many_addresses", []) or [])
        except Exception:
            targets = set()
        if target not in targets:
            return ""
        try:
            return str(owner._one_to_many_key(target))
        except Exception:
            return ""
    if hasattr(owner, "_is_mode_m1") and owner._is_mode_m1():
        if not target or not hasattr(owner, "_many_to_one_key"):
            return ""
        try:
            current_target = str(owner.target_address_var.get() or "").strip()
        except Exception:
            current_target = ""
        if hasattr(owner, "_normalize_context_target"):
            try:
                current_target = str(owner._normalize_context_target(current_target) or "").strip()
            except Exception:
                current_target = str(current_target or "").strip()
        if str(current_target or "").lower() != target.lower():
            return ""
        try:
            sources = list(getattr(owner.store, "many_to_one_sources", []) or [])
        except Exception:
            sources = []
        matched_source = ""
        for raw_source in sources:
            raw_source_text = str(raw_source or "").strip()
            if not raw_source_text:
                continue
            resolved_source = raw_source_text
            if hasattr(owner, "_resolve_source_address"):
                try:
                    resolved_source = str(owner._resolve_source_address(raw_source_text) or "").strip()
                except Exception:
                    resolved_source = raw_source_text
            if resolved_source.lower() != source.lower():
                continue
            matched_source = raw_source_text
            break
        if not matched_source:
            return ""
        try:
            return str(owner._many_to_one_key(matched_source))
        except Exception:
            return ""
    return ""


def _relay_recovery_row_context(owner, row_key: str, record: RelayWalletRecord) -> str:
    source = str(getattr(record, "source", "") or "").strip()
    target = str(getattr(record, "target", "") or "").strip()
    if str(row_key or "").strip().startswith("1m:"):
        try:
            source = str(owner.source_credential_var.get() or "").strip()
        except Exception:
            pass
    if str(row_key or "").strip().startswith("m1:"):
        try:
            target = str(owner.target_address_var.get() or "").strip()
        except Exception:
            pass
    if hasattr(owner, "_row_context_for_values"):
        try:
            return str(owner._row_context_for_values(row_key, source, target) or "")
        except Exception:
            pass
    return source


def _relay_recovery_row_status(outcome: str) -> str:
    if outcome in {"recovered", "already_empty", "kept_margin", "ignored"}:
        return "success"
    if outcome == "pending":
        return "running"
    if outcome in {"warning", "skipped", "failed"}:
        return "failed"
    return "success"


def _relay_recovery_row_status_text(outcome: str, status_text: str) -> str:
    custom = str(status_text or "").strip()
    if outcome in {"recovered", "already_empty", "kept_margin", "ignored"}:
        return custom if custom.startswith("✅") else "✅"
    if outcome == "pending":
        return custom or "进行中"
    if outcome in {"warning", "skipped", "failed"}:
        return "❌"
    return custom or "✅"


def _relay_recovery_amount_status_text(owner, network: str, amount_wei: int, *, kept_margin: bool = False) -> str:
    amount = owner._units_to_amount(max(0, int(amount_wei)), 18)
    symbol = str(owner._network_fee_symbol(network) or "").strip().lower()
    formatter = getattr(owner, "_stat_decimal_to_text", owner._decimal_to_text)
    amount_text = formatter(amount)
    text = f"✅{amount_text}{symbol}" if symbol else f"✅{amount_text}"
    if kept_margin:
        text = f"{text}(留边际)"
    return text


def _relay_apply_recovery_row_status(owner, row_key: str, context_sig: str, outcome: str, status_text: str) -> None:
    key = str(row_key or "").strip()
    if not key or not hasattr(owner, "_set_recovery_status"):
        return
    if hasattr(owner, "_mark_recovery_status_context"):
        owner._mark_recovery_status_context(key, context_sig)
    owner._set_recovery_status(
        key,
        _relay_recovery_row_status(outcome),
        _relay_recovery_row_status_text(outcome, status_text),
    )


def _token_balance_units(owner, params: WithdrawRuntimeParams, address: str) -> int:
    if params.token_is_native:
        return owner.client.get_balance_wei(params.network, address)
    return owner.client.get_erc20_balance(params.network, params.token_contract, address)


def _relay_sweep_plan(
    native_balance: int,
    gas_price_wei: int,
    gas_limit: int,
    *,
    keep_safety: bool = False,
) -> tuple[int, int, int]:
    gas_cost = max(0, int(gas_price_wei)) * max(0, int(gas_limit))
    safety_units = gas_cost * RELAY_SWEEP_SAFETY_GAS_MULTIPLIER if keep_safety else 0
    sweep_value = max(0, int(native_balance) - gas_cost - safety_units)
    return sweep_value, gas_cost, safety_units


def _relay_recovery_record_needs_scan(record: RelayWalletRecord, *, batch_scoped: bool = False) -> bool:
    relay_address = str(getattr(record, "relay_address", "") or "").strip()
    if not relay_address:
        return False
    if not batch_scoped:
        return True
    status = str(getattr(record, "status", "") or "").strip().lower()
    last_error = str(getattr(record, "last_error", "") or "").strip()
    sweep_resolution = str(getattr(record, "sweep_resolution", "") or "").strip().lower()
    if status == "completed" and not last_error and sweep_resolution in RELAY_SWEEP_TERMINAL_RESOLUTIONS:
        return False
    return True


def _relay_record_recovery_token(record: RelayWalletRecord | None) -> EvmToken | None:
    if record is None or bool(getattr(record, "token_is_native", True)):
        return None
    contract = str(getattr(record, "token_contract", "") or "").strip()
    if not contract:
        return None
    symbol = str(getattr(record, "token_symbol", "") or "").strip().upper() or "TOKEN"
    try:
        decimals = max(0, int(getattr(record, "token_decimals", 18) or 18))
    except Exception:
        decimals = 18
    return EvmToken(symbol=symbol, contract=contract, decimals=decimals, is_native=False)


def _relay_recovery_tokens(owner, network: str, record: RelayWalletRecord | None = None) -> list[EvmToken]:
    tracked: list[EvmToken] = []
    seen_symbols: set[str] = set()
    seen_contracts: set[str] = set()
    for token in owner.client.get_default_tokens(network):
        symbol = str(token.symbol or "").strip().upper()
        if not symbol:
            continue
        if not token.is_native and symbol not in RELAY_TRACKED_TOKEN_SYMBOLS:
            continue
        contract_key = str(token.contract or "").strip().lower()
        if symbol in seen_symbols or (contract_key and contract_key in seen_contracts):
            continue
        tracked.append(token)
        seen_symbols.add(symbol)
        if contract_key:
            seen_contracts.add(contract_key)
    record_token = _relay_record_recovery_token(record)
    if record_token is not None:
        symbol = str(record_token.symbol or "").strip().upper()
        contract_key = str(record_token.contract or "").strip().lower()
        if symbol and symbol not in seen_symbols and contract_key not in seen_contracts:
            tracked.append(record_token)
    return tracked


def _relay_tracked_balance_snapshot(
    owner,
    network: str,
    relay_addr: str,
    record: RelayWalletRecord | None = None,
) -> dict[str, tuple[EvmToken, int]]:
    snapshot: dict[str, tuple[EvmToken, int]] = {}
    for token in _relay_recovery_tokens(owner, network, record):
        if token.is_native:
            units = owner.client.get_balance_wei(network, relay_addr)
        else:
            units = owner.client.get_erc20_balance(network, token.contract, relay_addr)
        snapshot[str(token.symbol or "").strip().upper()] = (token, max(0, int(units)))
    return snapshot


def _relay_balance_text(owner, token: EvmToken, units: int) -> str:
    amount = owner._units_to_amount(int(units), int(token.decimals))
    return owner._token_amount_text(str(token.symbol or "").strip().upper(), amount)


def _relay_positive_balance_text(owner, snapshot: dict[str, tuple[EvmToken, int]]) -> str:
    parts: list[str] = []
    for symbol in sorted(snapshot):
        token, units = snapshot[symbol]
        if units > 0:
            parts.append(_relay_balance_text(owner, token, units))
    return " / ".join(parts) if parts else "-"


def _relay_positive_tracked_tokens(snapshot: dict[str, tuple[EvmToken, int]]) -> dict[str, tuple[EvmToken, int]]:
    return {
        symbol: (token, units)
        for symbol, (token, units) in snapshot.items()
        if not bool(token.is_native) and int(units) > 0
    }


def _relay_threshold_units(amount: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** max(0, int(decimals))
    return max(0, int(amount * scale))


def _relay_network_fee_symbol(owner, network: str) -> str:
    resolver = getattr(owner, "_network_fee_symbol", None)
    if callable(resolver):
        try:
            value = str(resolver(network) or "").strip().upper()
            if value:
                return value
        except Exception:
            pass
    return {"BSC": "BNB", "ETH": "ETH"}.get(str(network or "").strip().upper(), "")


def _relay_token_dust_below_threshold(
    owner,
    positive_tracked_tokens: dict[str, tuple[EvmToken, int]],
) -> tuple[bool, str]:
    if not positive_tracked_tokens:
        return False, ""
    details: list[str] = []
    for symbol in sorted(positive_tracked_tokens):
        token, units = positive_tracked_tokens[symbol]
        token_units = max(0, int(units))
        token_symbol = str(getattr(token, "symbol", "") or symbol or "").strip().upper()
        if token_units <= 0:
            continue
        if token_symbol not in RELAY_STABLE_TOKEN_SYMBOLS or bool(getattr(token, "is_native", False)):
            return False, ""
        threshold_units = _relay_threshold_units(RELAY_STABLE_TOKEN_DUST_THRESHOLD, token.decimals)
        if token_units >= threshold_units:
            return False, ""
        balance_text = _relay_balance_text(owner, token, token_units)
        threshold_text = _relay_balance_text(owner, token, threshold_units)
        details.append(f"{balance_text} < dust阈值 {threshold_text}")
    if not details:
        return False, ""
    return True, " / ".join(details)


def _relay_native_dust_below_threshold(owner, network: str, native_units: int) -> tuple[bool, str]:
    units = max(0, int(native_units))
    if units <= 0:
        return False, ""
    symbol = _relay_network_fee_symbol(owner, network)
    if symbol != "BNB":
        return False, ""
    threshold_units = _relay_threshold_units(RELAY_BNB_DUST_THRESHOLD, 18)
    if units >= threshold_units:
        return False, ""
    balance_text = owner._gas_fee_amount_text(network, units)
    threshold_text = owner._gas_fee_amount_text(network, threshold_units)
    return True, f"{balance_text} < dust阈值 {threshold_text}"


def _relay_native_cleanup_allowed(owner, network: str, native_units: int) -> bool:
    units = max(0, int(native_units))
    if units <= 0:
        return True
    return _relay_native_dust_below_threshold(owner, network, units)[0]


def _relay_snapshot_cleanup_allowed(owner, network: str, snapshot: dict[str, tuple[EvmToken, int]]) -> bool:
    native_token, native_units = _relay_native_balance_entry(snapshot)
    if native_token is not None and not _relay_native_cleanup_allowed(owner, network, native_units):
        return False
    positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
    if not positive_tracked_tokens:
        return True
    return _relay_token_dust_below_threshold(owner, positive_tracked_tokens)[0]


def _relay_snapshot_has_positive_balance(snapshot: dict[str, tuple[EvmToken, int]]) -> bool:
    return any(int(units) > 0 for _token, units in snapshot.values())


def _relay_is_terminal_dust_state(
    record: RelayWalletRecord,
    positive_tracked_tokens: dict[str, tuple[EvmToken, int]],
) -> bool:
    status = str(getattr(record, "status", "") or "").strip().lower()
    last_error = str(getattr(record, "last_error", "") or "").strip()
    sweep_resolution = str(getattr(record, "sweep_resolution", "") or "").strip().lower()
    return (
        status == "completed"
        and not last_error
        and sweep_resolution in RELAY_SWEEP_TERMINAL_RESOLUTIONS
        and not positive_tracked_tokens
    )


def _relay_terminal_snapshot_can_be_ignored(
    owner,
    network: str,
    record: RelayWalletRecord,
    snapshot: dict[str, tuple[EvmToken, int]],
) -> bool:
    return (
        _relay_is_terminal_dust_state(record, _relay_positive_tracked_tokens(snapshot))
        and _relay_snapshot_cleanup_allowed(owner, network, snapshot)
    )


def _relay_token_dust_requires_native_recovery(
    owner,
    network: str,
    snapshot: dict[str, tuple[EvmToken, int]],
) -> bool:
    positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
    return (
        bool(positive_tracked_tokens)
        and _relay_token_dust_below_threshold(owner, positive_tracked_tokens)[0]
        and not _relay_snapshot_cleanup_allowed(owner, network, snapshot)
    )


def _relay_native_balance_entry(snapshot: dict[str, tuple[EvmToken, int]]) -> tuple[EvmToken | None, int]:
    for token, units in snapshot.values():
        if token.is_native:
            return token, max(0, int(units))
    return None, 0


def _relay_manual_token_key(network: str, token: EvmToken) -> tuple[str, str, int]:
    return (
        str(network or "").strip().upper(),
        str(token.symbol or "").strip().upper(),
        max(0, int(token.decimals)),
    )


def _relay_manual_token_summary_text(
    owner,
    symbol: str,
    totals: dict[tuple[str, str, int], int],
    wallet_counts: dict[tuple[str, str, int], int],
) -> str:
    parts: list[str] = []
    total_wallets = 0
    target_symbol = str(symbol or "").strip().upper()
    for key in sorted(totals):
        network, token_symbol, decimals = key
        if token_symbol != target_symbol:
            continue
        total_units = max(0, int(totals.get(key) or 0))
        if total_units <= 0:
            continue
        total_wallets += max(0, int(wallet_counts.get(key) or 0))
        amount = owner._units_to_amount(total_units, decimals)
        parts.append(f"{owner._token_amount_text(token_symbol, amount)}({network})")
    if not parts:
        return "无"
    return f"{total_wallets} 个钱包，合计 {' / '.join(parts)}"


def _relay_build_manual_export_row(
    owner,
    record: RelayWalletRecord,
    snapshot: dict[str, tuple[EvmToken, int]],
    *,
    sweep_target: str,
    reason: str,
) -> dict[str, object]:
    native_token, native_units = _relay_native_balance_entry(snapshot)
    positive_tokens = _relay_positive_tracked_tokens(snapshot)
    row: dict[str, object] = {
        "exported_at": _utc_now_text(),
        "reason": str(reason or "").strip(),
        "network": str(record.network or "").strip().upper(),
        "batch_id": str(record.batch_id or "").strip(),
        "source": str(record.source or "").strip(),
        "target": str(record.target or "").strip(),
        "relay_address": str(record.relay_address or "").strip(),
        "private_key": str(record.private_key or "").strip(),
        "status": str(record.status or "").strip(),
        "sweep_target": str(sweep_target or record.sweep_target or record.source or "").strip(),
        "balances_text": _relay_positive_balance_text(owner, snapshot),
        "token_forward_txid": str(record.token_forward_txid or "").strip(),
        "gas_sweep_txid": str(record.gas_sweep_txid or "").strip(),
        "last_error": str(record.last_error or "").strip(),
    }
    if native_token is not None:
        native_symbol = str(native_token.symbol or "").strip().upper()
        row[f"{native_symbol.lower()}_balance_units"] = str(native_units)
        row[f"{native_symbol.lower()}_balance_text"] = _relay_balance_text(owner, native_token, native_units)
    for symbol in sorted(RELAY_TRACKED_TOKEN_SYMBOLS):
        token, units = positive_tokens.get(symbol, (None, 0))
        if token is None:
            row[f"{symbol.lower()}_balance_units"] = "0"
            row[f"{symbol.lower()}_balance_text"] = f"0 {symbol}"
            continue
        row[f"{symbol.lower()}_balance_units"] = str(max(0, int(units)))
        row[f"{symbol.lower()}_balance_text"] = _relay_balance_text(owner, token, units)
    return row


def _relay_manual_account_copy_lines(rows: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for row in rows:
        address = str(row.get("relay_address", "") or "").strip()
        private_key = str(row.get("private_key", "") or "").strip()
        address_key = address.lower()
        if not address or not private_key or address_key in seen:
            continue
        seen.add(address_key)
        lines.append(f"{address} {private_key}")
    return lines


def _relay_copy_manual_accounts(owner, lines: list[str]) -> None:
    values = [str(line or "").strip() for line in lines if str(line or "").strip()]
    if not values:
        return
    root = getattr(owner, "root", None)
    if root is None:
        return
    root.clipboard_clear()
    root.clipboard_append("\n".join(values))
    root.update_idletasks()
    owner.log(f"已复制待人工账号 {len(values)} 条到剪贴板")


def _relay_export_line_address(line: str) -> str:
    text = str(line or "").strip()
    if not text or " - " not in text:
        return ""
    return str(text.split(" - ", 1)[0] or "").strip().lower()


def _relay_write_manual_export_file(
    rows: list[dict[str, object]],
    *,
    preserve_existing: bool = False,
    replace_addresses: set[str] | None = None,
) -> str:
    export_path = RELAY_MANUAL_EXPORT_FILE
    export_path.parent.mkdir(parents=True, exist_ok=True)
    replace_keys = {
        str(address or "").strip().lower()
        for address in (replace_addresses or set())
        if str(address or "").strip()
    }
    if preserve_existing and not rows and not replace_keys:
        return ""
    if not rows:
        if not preserve_existing:
            try:
                if export_path.exists():
                    export_path.unlink()
            except Exception:
                pass
            return ""
    seen: set[str] = set()
    lines: list[str] = []
    if preserve_existing and export_path.exists():
        try:
            for raw_line in export_path.read_text(encoding="utf-8").splitlines():
                line = str(raw_line or "").strip()
                if not line:
                    continue
                address_key = _relay_export_line_address(line)
                if address_key and address_key in replace_keys:
                    continue
                dedupe_key = address_key or line
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                lines.append(line)
        except Exception:
            pass
    for row in rows:
        address = str(row.get("relay_address", "") or "").strip()
        private_key = str(row.get("private_key", "") or "").strip()
        if not address or not private_key:
            continue
        line = f"{address} - {private_key}"
        dedupe_key = address.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        lines.append(line)
    if not lines:
        try:
            if export_path.exists():
                export_path.unlink()
        except Exception:
            pass
        return ""
    content = "\n".join(lines)
    if content:
        content += "\n"
    _atomic_write_text(export_path, content, encoding="utf-8")
    return str(export_path)


def _relay_write_failed_export_file(records: list[RelayWalletRecord]) -> str:
    export_path = RELAY_FAILED_EXPORT_FILE
    export_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    seen: set[str] = set()
    for record in records:
        address = str(getattr(record, "relay_address", "") or "").strip()
        private_key = str(getattr(record, "private_key", "") or "").strip()
        if not address or not private_key:
            continue
        line = f"{address} - {private_key}"
        if line in seen:
            continue
        seen.add(line)
        lines.append(line)
    if not lines:
        try:
            if export_path.exists():
                export_path.unlink()
        except Exception:
            pass
        return ""
    content = "\n".join(lines) + "\n"
    _atomic_write_text(export_path, content, encoding="utf-8")
    return str(export_path)


def _wait_for_transaction_success(owner, network: str, txid: str, *, label: str, timeout_seconds: float | None = None) -> dict:
    tx_hash = str(txid or "").strip()
    if owner.stop_requested.is_set():
        raise RuntimeError("任务已停止")
    if not tx_hash:
        raise RuntimeError(f"{label}失败：未拿到 txid")
    return {"transactionHash": tx_hash, "status": "0x1"}


def _wait_for_token_balance_at_least(
    owner,
    params: WithdrawRuntimeParams,
    address: str,
    minimum_units: int,
    *,
    label: str,
    timeout_seconds: float | None = None,
) -> int:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_BALANCE_TIMEOUT_SECONDS))
    deadline = time.time() + timeout_value
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        current_units = _token_balance_units(owner, params, address)
        if current_units >= int(minimum_units):
            return current_units
        if time.time() >= deadline:
            raise RuntimeError(f"{label}超时：等待余额达到目标值超过 {timeout_value:g} 秒")
        if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
            raise RuntimeError("任务已停止")


def _wait_for_native_balance_at_most(
    owner,
    network: str,
    address: str,
    maximum_units: int,
    *,
    label: str,
    timeout_seconds: float | None = None,
) -> int:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_BALANCE_TIMEOUT_SECONDS))
    deadline = time.time() + timeout_value
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        current_units = owner.client.get_balance_wei(network, address)
        if current_units <= int(maximum_units):
            return current_units
        if time.time() >= deadline:
            raise RuntimeError(f"{label}超时：等待原生币余额降至目标值以下超过 {timeout_value:g} 秒")
        if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
            raise RuntimeError("任务已停止")


def _relay_many_to_one_source_keep_units(owner, params: WithdrawRuntimeParams, record: RelayWalletRecord) -> int:
    source_keep_dec = _record_relay_fee_reserve_decimal(record, params)
    return max(0, owner._amount_to_units(source_keep_dec, 18))


def _relay_batch_id(job_count: int) -> str:
    return f"relay-{int(time.time() * 1000)}-{max(1, int(job_count))}"


def _relay_totals_text(owner, totals_by_network: dict[str, int], *, prefix: str = "") -> str:
    parts: list[str] = []
    for network in sorted(totals_by_network):
        amount_wei = max(0, int(totals_by_network.get(network) or 0))
        parts.append(owner._gas_fee_amount_text(network, amount_wei))
    if not parts:
        return "-"
    body = "；".join(parts)
    return f"{prefix}{body}" if prefix else body


def _relay_native_symbol(owner, network: str) -> str:
    try:
        symbol = str(owner._network_fee_symbol(network) or "").strip().upper()
    except Exception:
        symbol = ""
    return symbol or str(network or "").strip().upper() or "BNB"


def _relay_asset_total_add(totals: dict[tuple[str, int], int], symbol: str, decimals: int, units: int) -> None:
    value = max(0, int(units or 0))
    if value <= 0:
        return
    key = (str(symbol or "").strip().upper(), max(0, int(decimals or 0)))
    if not key[0]:
        return
    totals[key] = totals.get(key, 0) + value


def _relay_asset_totals_merge(
    *totals_list: dict[tuple[str, int], int],
) -> dict[tuple[str, int], int]:
    merged: dict[tuple[str, int], int] = {}
    for totals in totals_list:
        for key, value in (totals or {}).items():
            value_int = max(0, int(value or 0))
            if value_int > 0:
                merged[key] = merged.get(key, 0) + value_int
    return merged


def _relay_asset_totals_text(owner, totals: dict[tuple[str, int], int]) -> str:
    parts: list[str] = []
    for (symbol, decimals), units in sorted((totals or {}).items(), key=lambda item: item[0][0]):
        value = max(0, int(units or 0))
        if value <= 0:
            continue
        try:
            amount = owner._units_to_amount(value, decimals)
            parts.append(owner._token_amount_text(symbol, amount))
        except Exception:
            parts.append(f"{value} {symbol}")
    return "；".join(parts) if parts else "-"


def _transaction_receipt_state(owner, network: str, txid: str) -> str:
    tx_hash = str(txid or "").strip()
    if not tx_hash:
        return "missing"
    receipt = owner.client.get_transaction_receipt(network, tx_hash)
    if receipt is None:
        return "pending"
    return _receipt_state(receipt)


def _receipt_state(receipt: dict | None) -> str:
    if not receipt:
        return "pending"
    raw_status = receipt.get("status")
    if raw_status is None:
        return "pending"
    try:
        if isinstance(raw_status, str):
            value = int(raw_status, 16) if raw_status.lower().startswith("0x") else int(raw_status)
        else:
            value = int(raw_status)
    except Exception:
        return "failed"
    return "success" if value == 1 else "failed"


def _receipt_poll_interval_seconds(params: WithdrawRuntimeParams) -> float:
    try:
        value = float(getattr(params, "receipt_poll_interval_seconds", RELAY_POLL_INTERVAL_SECONDS))
    except Exception:
        value = RELAY_POLL_INTERVAL_SECONDS
    return max(0.1, value)


def _receipt_retry_count(params: WithdrawRuntimeParams) -> int:
    try:
        value = int(getattr(params, "receipt_retry_count", 10))
    except Exception:
        value = 10
    return max(1, value)


def _nonce_window(params: WithdrawRuntimeParams) -> int:
    try:
        value = int(getattr(params, "nonce_window", 5))
    except Exception:
        value = 5
    return max(1, value)


def _one_to_many_source_threads(params: WithdrawRuntimeParams) -> int:
    try:
        value = int(getattr(params, "one_to_many_source_threads", 1) or 1)
    except Exception:
        value = 1
    return max(1, value)


def _source_stage_submit_interval(params: WithdrawRuntimeParams) -> float:
    try:
        return max(0.0, float(params.delay or 0.0))
    except Exception:
        return 0.0


def _wait_for_receipt_success(owner, params: WithdrawRuntimeParams, txid: str, *, label: str) -> dict:
    tx_hash = str(txid or "").strip()
    if not tx_hash:
        raise RuntimeError(f"{label}失败：未拿到 txid")
    interval = _receipt_poll_interval_seconds(params)
    started_at = time.monotonic()
    deadline = started_at + _confirm_timeout_seconds(params)
    next_progress_at = started_at + 15.0
    attempt = 0
    last_detail = "未查询到 receipt"
    # 确认时间包含 RPC 请求耗时；提交重试次数不能延长确认期限。
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempt += 1
        try:
            receipt = owner.client.get_transaction_receipt(
                params.network, tx_hash, timeout_seconds=min(15.0, remaining),
                stop_event=owner.stop_requested,
            )
            state = _receipt_state(receipt)
            if state == "success":
                return receipt or {}
            if state == "failed":
                raise RuntimeError(f"{label}失败：receipt status=0，txid={tx_hash}")
            last_detail = "receipt 未返回"
        except Exception as exc:
            last_detail = str(exc)
            if owner.stop_requested.is_set():
                raise RuntimeError("任务已停止") from exc
            if "receipt status=0" in last_detail:
                raise
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        now = time.monotonic()
        if now >= next_progress_at:
            owner._dispatch_ui(
                lambda m=(f"{label}等待链上确认：已等待 {now - started_at:.1f} 秒，"
                          f"剩余 {remaining:.1f} 秒，查询 {attempt} 次，txid={tx_hash}，{last_detail}"): owner.log(m)
            )
            next_progress_at = now + 15.0
        if owner.stop_requested.wait(min(interval, remaining)):
            raise RuntimeError("任务已停止")
    elapsed = time.monotonic() - started_at
    raise RuntimeError(
        f"{label}确认超时：等待 {elapsed:.1f} 秒，查询 receipt {attempt} 次仍未确认，"
        f"txid={tx_hash}，最后结果：{last_detail}。链上结果尚未确认，请保留原交易记录续查"
    )


def _update_relay_record(owner, record, *, status: str, **changes: object):
    if status in {"completed", "completed_pending_sweep"}:
        changes.setdefault("completed_at", str(getattr(record, "completed_at", "") or _utc_now_text()))
    if status == "failed":
        changes.setdefault("last_error", str(changes.get("last_error") or ""))
    elif status == "completed":
        changes.setdefault("last_error", "")
    updated = owner.relay_wallet_store.update_record(
        record.relay_address,
        batch_id=record.batch_id,
        status=status,
        **changes,
    )
    return updated


def _confirm_timeout_seconds(params: WithdrawRuntimeParams) -> float:
    try:
        value = float(getattr(params, "confirm_timeout_seconds", RELAY_CONFIRM_TIMEOUT_SECONDS))
    except Exception:
        value = RELAY_CONFIRM_TIMEOUT_SECONDS
    return max(1.0, value)


def _broadcast_recovery_timeout_seconds(params: WithdrawRuntimeParams) -> float:
    return min(max(6.0, _confirm_timeout_seconds(params) / 3.0), 20.0)


def _record_relay_fee_reserve_value(record: RelayWalletRecord) -> Decimal | None:
    raw = str(getattr(record, "relay_fee_reserve", "") or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except Exception:
        return None


def _record_matches_job(record: RelayWalletRecord, params: WithdrawRuntimeParams, source_addr: str, target: str) -> bool:
    if owner_network := str(record.network or "").strip().upper():
        if owner_network != str(params.network or "").strip().upper():
            return False
    if str(record.source or "").strip().lower() != str(source_addr or "").strip().lower():
        return False
    if str(record.target or "").strip().lower() != str(target or "").strip().lower():
        return False
    if bool(record.token_is_native) != bool(params.token_is_native):
        return False
    record_contract = str(record.token_contract or "").strip().lower()
    params_contract = str(params.token_contract or "").strip().lower()
    if record_contract != params_contract:
        return False
    record_reserve = _record_relay_fee_reserve_value(record)
    if record_reserve is not None:
        params_reserve = params.relay_fee_reserve or Decimal("0")
        if record_reserve != params_reserve:
            return False
    return True


def _relay_resolved_source_label(owner, source_addr: str, source_raw: str) -> str:
    if str(source_addr or "").strip():
        return owner._mask(source_addr, head=8, tail=6)
    if str(source_raw or "").strip():
        return owner._mask_credential(source_raw)
    return "-"


def _relay_job_key(source_addr: str, target: str) -> tuple[str, str]:
    return (str(source_addr or "").strip().lower(), str(target or "").strip().lower())


def _relay_reuse_key(owner, network: str, source_addr: str, target: str, *, mode: str = "") -> str:
    mode = mode or (owner._mode() if hasattr(owner, "_mode") else "")
    return "|".join(
        (
            str(mode or "").strip(),
            str(network or "").strip().upper(),
            str(source_addr or "").strip().lower(),
            str(target or "").strip().lower(),
        )
    )


def _resolve_job_source_wallets(owner, jobs_data: list[tuple[str, str, str]]) -> dict[str, tuple[str, str]]:
    row_source_wallets: dict[str, tuple[str, str]] = {}
    source_wallet_cache: dict[str, tuple[str, str]] = {}
    for row_key, source, _target in jobs_data:
        source_key = str(source or "").strip()
        wallet = source_wallet_cache.get(source_key)
        if wallet is None:
            wallet = owner._resolve_wallet(source)
            source_wallet_cache[source_key] = wallet
        row_source_wallets[row_key] = wallet
    return row_source_wallets


def _build_resume_candidates(
    owner,
    existing_records: list[RelayWalletRecord],
    jobs_data: list[tuple[str, str, str]],
    params: WithdrawRuntimeParams,
    row_source_wallets: dict[str, tuple[str, str]],
) -> dict[tuple[str, str], list[RelayWalletRecord]]:
    wanted_pairs = {
        _relay_job_key(row_source_wallets[row_key][1], target)
        for row_key, _source, target in jobs_data
        if row_key in row_source_wallets
    }
    wanted_reuse_keys = {
        _relay_reuse_key(owner, params.network, row_source_wallets[row_key][1], target, mode=params.mode)
        for row_key, _source, target in jobs_data
        if row_key in row_source_wallets
    }
    candidates_by_pair: dict[tuple[str, str], list[RelayWalletRecord]] = {}
    for record in existing_records:
        record_reuse_key = str(getattr(record, "reuse_key", "") or "").strip()
        if record_reuse_key and record_reuse_key not in wanted_reuse_keys:
            continue
        pair_key = _relay_job_key(record.source, record.target)
        if pair_key not in wanted_pairs:
            continue
        status = str(record.status or "").strip()
        if status != "completed" and not _record_matches_job(record, params, record.source, record.target):
            continue
        candidates_by_pair.setdefault(pair_key, []).append(record)
    return candidates_by_pair


def _pick_or_create_relay_records(
    owner,
    jobs_data: list[tuple[str, str, str]],
    params: WithdrawRuntimeParams,
    row_source_wallets: dict[str, tuple[str, str]],
    resume_candidates_by_pair: dict[tuple[str, str], list[RelayWalletRecord]],
) -> tuple[dict[str, RelayWalletRecord], int, str]:
    relay_records_by_row_key: dict[str, RelayWalletRecord] = {}
    jobs_missing_relay: list[tuple[str, str, str]] = []
    resumed_count = 0
    batch_id = _relay_batch_id(len(jobs_data))
    for row_key, _source, target in jobs_data:
        source_wallet = row_source_wallets.get(row_key)
        if source_wallet is None:
            raise RuntimeError("未找到源钱包映射")
        _source_private_key, source_addr = source_wallet
        pair_key = _relay_job_key(source_addr, target)
        candidates = resume_candidates_by_pair.get(pair_key) or []
        if candidates:
            candidate = candidates.pop()
            reuse_key = _relay_reuse_key(owner, params.network, source_addr, target, mode=params.mode)
            candidate_status = str(candidate.status or "").strip()
            reset_unsubmitted_1m_native = bool(
                params.token_is_native
                and _relay_mode_is(owner, params, "1m")
                and candidate_status in {"", "created", "failed"}
                and not str(candidate.token_funded_txid or "").strip()
                and not str(candidate.token_funded_nonce or "").strip()
            )
            if candidate_status == "completed":
                candidate = owner.relay_wallet_store.update_record(
                    candidate.relay_address,
                    batch_id=candidate.batch_id,
                    reuse_key=reuse_key,
                    status="created",
                    token_symbol=str(params.coin or "").strip().upper(),
                    token_contract=str(params.token_contract or "").strip(),
                    token_decimals=max(0, int(params.token_decimals)),
                    token_is_native=bool(params.token_is_native),
                    relay_fee_reserve=owner._decimal_to_text(params.relay_fee_reserve or Decimal("0")),
                    transfer_amount="",
                    transfer_units="",
                    fee_funded_txid="",
                    fee_funded_nonce="",
                    token_funded_txid="",
                    token_funded_nonce="",
                    token_forward_txid="",
                    token_forward_nonce="",
                    gas_sweep_txid="",
                    gas_sweep_nonce="",
                    sweep_resolution="",
                    last_error="",
                    completed_at="",
                )
            elif reset_unsubmitted_1m_native:
                candidate = owner.relay_wallet_store.update_record(
                    candidate.relay_address,
                    batch_id=candidate.batch_id,
                    reuse_key=reuse_key,
                    transfer_amount="",
                    transfer_units="",
                    last_error="",
                )
            relay_records_by_row_key[row_key] = candidate
            resumed_count += 1
        else:
            jobs_missing_relay.append((row_key, source_addr, target))
    token = EvmToken(
        symbol=params.coin,
        contract=params.token_contract,
        decimals=params.token_decimals,
        is_native=params.token_is_native,
    )
    relay_records = []
    is_many_to_one_mode = bool(_relay_mode_is(owner, params, "m1"))
    if jobs_missing_relay:
        relay_wallets = owner.client.create_wallets(
            len(jobs_missing_relay),
            worker_threads=max(1, min(params.threads, len(jobs_missing_relay))),
        )
        for (row_key, source_addr, target), relay_wallet in zip(jobs_missing_relay, relay_wallets):
            sweep_target = target if is_many_to_one_mode else source_addr
            reuse_key = _relay_reuse_key(owner, params.network, source_addr, target, mode=params.mode)
            relay_record = owner.relay_wallet_store.build_record(
                batch_id=batch_id,
                network=params.network,
                source_address=source_addr,
                target_address=target,
                relay_wallet=relay_wallet,
                token=token,
                relay_fee_reserve=params.relay_fee_reserve,
                sweep_enabled=False,
                sweep_target=sweep_target,
                reuse_key=reuse_key,
            )
            relay_records.append(relay_record)
            relay_records_by_row_key[row_key] = relay_record
        owner.relay_wallet_store.append_records(relay_records)
    return relay_records_by_row_key, resumed_count, batch_id


def _record_relay_fee_reserve_decimal(record: RelayWalletRecord, params: WithdrawRuntimeParams) -> Decimal:
    value = _record_relay_fee_reserve_value(record)
    if value is not None and value > 0:
        return value
    return params.relay_fee_reserve or Decimal("0")


def _erc20_transfer_units_from_input(data: str) -> int:
    raw = str(data or "").strip()
    if raw.startswith("0x") or raw.startswith("0X"):
        raw = raw[2:]
    raw = raw.lower()
    if not raw.startswith("a9059cbb"):
        return 0
    if len(raw) < 8 + 64 + 64:
        return 0
    try:
        return int(raw[-64:], 16)
    except Exception:
        return 0


def _infer_transfer_units_from_chain(owner, record: RelayWalletRecord) -> int:
    for txid in (record.token_forward_txid, record.token_funded_txid):
        tx_hash = str(txid or "").strip()
        if not tx_hash:
            continue
        try:
            tx = owner.client.get_transaction_by_hash(record.network, tx_hash)
        except Exception:
            tx = None
        if not tx:
            continue
        if record.token_is_native:
            try:
                return int(owner.client._int_from_hex(tx.get("value")))
            except Exception:
                continue
        units = _erc20_transfer_units_from_input(str(tx.get("input") or tx.get("data") or ""))
        if units > 0:
            return units
    return 0


def _resolve_record_amount(owner, record: RelayWalletRecord, params: WithdrawRuntimeParams, source_addr: str, relay_addr: str) -> tuple[int, str, bool]:
    transfer_units_raw = str(getattr(record, "transfer_units", "") or "").strip()
    transfer_amount_raw = str(getattr(record, "transfer_amount", "") or "").strip()
    if transfer_units_raw:
        value_units = int(transfer_units_raw)
        amount_text = transfer_amount_raw or owner._decimal_to_text(owner._units_to_amount(value_units, params.token_decimals))
        return value_units, amount_text, False
    if transfer_amount_raw:
        amount_dec = Decimal(transfer_amount_raw)
        value_units = owner._amount_to_units(amount_dec, params.token_decimals)
        return value_units, owner._decimal_to_text(amount_dec), True
    inferred_units = _infer_transfer_units_from_chain(owner, record)
    if inferred_units > 0:
        amount_text = owner._decimal_to_text(owner._units_to_amount(inferred_units, params.token_decimals))
        return inferred_units, amount_text, True
    value_units, _gas_price, _gas_limit, amount_text = owner._resolve_amount_and_gas(params, source_addr, relay_addr)
    return value_units, amount_text, True


def _relay_amount_all_label(owner) -> str:
    return str(getattr(owner, "AMOUNT_ALL_LABEL", "全部") or "全部")


def _relay_amount_to_units_from_text(owner, amount_text: str, decimals: int, *, prefix: str = "") -> tuple[int, str]:
    try:
        amount_dec = Decimal(str(amount_text or "").strip())
    except Exception:
        label = f"{prefix} " if prefix else ""
        raise RuntimeError(f"{label}转账数量无效")
    if amount_dec <= 0:
        label = f"{prefix} " if prefix else ""
        raise RuntimeError(f"{label}转账数量必须大于 0")
    value_units = owner._amount_to_units(amount_dec, decimals)
    if value_units <= 0:
        label = f"{prefix} " if prefix else ""
        raise RuntimeError(f"{label}转账数量过小")
    return value_units, owner._decimal_to_text(amount_dec)


def _resolve_record_amount_local(owner, record: RelayWalletRecord, params: WithdrawRuntimeParams) -> tuple[int, str]:
    transfer_units_raw = str(getattr(record, "transfer_units", "") or "").strip()
    transfer_amount_raw = str(getattr(record, "transfer_amount", "") or "").strip()
    if transfer_units_raw:
        value_units = int(transfer_units_raw)
        amount_text = transfer_amount_raw or owner._decimal_to_text(owner._units_to_amount(value_units, params.token_decimals))
        return value_units, amount_text
    if transfer_amount_raw:
        return _relay_amount_to_units_from_text(owner, transfer_amount_raw, params.token_decimals)
    if params.random_enabled and params.random_min is not None and params.random_max is not None:
        amount_dec = owner._random_decimal_between(params.random_min, params.random_max, params.token_decimals)
        if amount_dec <= 0:
            raise RuntimeError("随机金额生成失败：结果必须大于 0")
        return owner._amount_to_units(amount_dec, params.token_decimals), owner._decimal_to_text(amount_dec)
    if str(params.amount or "").strip() == _relay_amount_all_label(owner):
        return 0, ""
    return _relay_amount_to_units_from_text(owner, params.amount, params.token_decimals)


def _relay_source_amount_params(
    params: WithdrawRuntimeParams,
    item: "_RelayBatchItem",
    *,
    token_is_native: bool,
    relay_fee_reserve: Decimal | None = None,
) -> WithdrawRuntimeParams:
    changes: dict[str, object] = {
        "token_is_native": token_is_native,
        "relay_enabled": True,
    }
    if relay_fee_reserve is not None:
        changes["relay_fee_reserve"] = relay_fee_reserve
    if str(item.amount_text or "").strip():
        changes.update(
            {
                "amount": str(item.amount_text).strip(),
                "random_enabled": False,
                "random_min": None,
                "random_max": None,
            }
        )
    return replace(params, **changes)


def _relay_update_item_transfer_amount(owner, item: "_RelayBatchItem", amount_text: str, value_units: int) -> None:
    item.amount_text = str(amount_text or "").strip()
    item.value_units = max(0, int(value_units or 0))
    if item.amount_text:
        item.final_amount_text = item.amount_text
    _relay_update_item_record(
        owner,
        item,
        _relay_record_status(item) or "created",
        transfer_amount=item.amount_text,
        transfer_units=(str(item.value_units) if item.value_units > 0 else ""),
        last_error="",
    )


def _relay_resolve_source_token_transfer(owner, params: WithdrawRuntimeParams, item: "_RelayBatchItem") -> None:
    reserve_dec = _record_relay_fee_reserve_decimal(item.relay_record, params)
    stage_params = _relay_source_amount_params(
        params,
        item,
        token_is_native=False,
        relay_fee_reserve=reserve_dec,
    )
    value_units, gas_price, gas_limit, amount_text = owner._resolve_amount_and_gas(
        stage_params,
        item.source_addr,
        item.relay_addr,
    )
    item.source_token_gas_price = gas_price
    item.source_token_gas_limit = gas_limit
    _relay_update_item_transfer_amount(owner, item, amount_text, value_units)


def _relay_resolve_1m_native_transfer(owner, params: WithdrawRuntimeParams, item: "_RelayBatchItem") -> None:
    if item.value_units <= 0:
        raise RuntimeError(f"{item.prefix} 转账数量必须大于 0")
    # 预检查报价可能已过期；签名前刷新，并按当前手续费重新检查余额。
    gas_price = owner.client.get_gas_price_wei(params.network)
    gas_limit = int(item.source_token_gas_limit or owner.client.NATIVE_GAS_LIMIT)
    balance_units = owner.client.get_balance_wei(params.network, item.source_addr)
    total_send_units = item.value_units + item.reserve_units
    if balance_units < total_send_units + gas_price * gas_limit:
        raise RuntimeError(f"{item.prefix} 源钱包原生币余额不足，无法发送转账金额和中转预留")
    item.source_token_gas_price = gas_price
    item.source_token_gas_limit = gas_limit
    _relay_update_item_transfer_amount(owner, item, item.amount_text, item.value_units)


def _relay_resolve_m1_native_transfer(owner, params: WithdrawRuntimeParams, item: "_RelayBatchItem") -> None:
    source_keep_units = _relay_many_to_one_source_keep_units(owner, params, item.relay_record)
    gas_price = owner.client.get_gas_price_wei(params.network)
    gas_limit = owner.client.NATIVE_GAS_LIMIT
    balance_units = owner.client.get_balance_wei(params.network, item.source_addr)
    value_units = balance_units - source_keep_units - (gas_price * gas_limit)
    if value_units <= 0:
        raise RuntimeError(f"{item.prefix} 源钱包原生币余额不足，无法在保留预留后转入中转钱包")
    amount_text = owner._decimal_to_text(owner._units_to_amount(value_units, params.token_decimals))
    item.reserve_units = 0
    item.source_token_gas_price = gas_price
    item.source_token_gas_limit = gas_limit
    _relay_update_item_transfer_amount(owner, item, amount_text, value_units)


def _relay_resolve_1m_native_fee_transfer(owner, params: WithdrawRuntimeParams, item: "_RelayBatchItem") -> None:
    gas_price = owner.client.get_gas_price_wei(params.network)
    gas_limit = owner.client.NATIVE_GAS_LIMIT
    balance_units = owner.client.get_balance_wei(params.network, item.source_addr)
    if balance_units < item.reserve_units + gas_price * gas_limit:
        raise RuntimeError(f"{item.prefix} 源钱包原生币余额不足，无法发送中转手续费预留")
    item.source_fee_gas_price = gas_price
    item.source_fee_gas_limit = gas_limit


def _tx_nonce_text(tx_nonce: int | None) -> str:
    if tx_nonce is None:
        return ""
    return str(int(tx_nonce))


def _tx_nonce_value(text: str) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _relay_auto_forward_tracked_tokens(
    owner,
    record: RelayWalletRecord,
    *,
    network: str,
    relay_addr: str,
    target: str,
    positive_tracked_tokens: dict[str, tuple[EvmToken, int]],
    timeout_seconds: float,
    recovery_timeout_seconds: float,
) -> tuple[RelayWalletRecord, int, list[str]]:
    relay_private_key = owner.client.credential_to_private_key(record.private_key)
    next_nonce = owner.client.get_nonce(network, relay_addr)
    current = record
    gas_fee_total = 0
    forwarded_parts: list[str] = []
    for symbol in sorted(positive_tracked_tokens):
        token, units = positive_tracked_tokens[symbol]
        transfer_units = max(0, int(units))
        if transfer_units <= 0:
            continue
        target_before_units = owner.client.get_erc20_balance(network, token.contract, target)
        gas_price = owner.client.get_gas_price_wei(network)
        gas_limit = owner.client.estimate_erc20_transfer_gas(
            network,
            relay_addr,
            token.contract,
            target,
            transfer_units,
        )
        gas_cost = gas_price * gas_limit
        native_balance = owner.client.get_balance_wei(network, relay_addr)
        if native_balance < gas_cost:
            current_text = owner._gas_fee_amount_text(network, native_balance)
            required_text = owner._gas_fee_amount_text(network, gas_cost)
            raise RuntimeError(f"中转钱包原生币不足，无法自动续转 {symbol}：当前 {current_text}，至少需要 {required_text}")
        submission = owner.client.submit_erc20_transfer_reliably(
            network=network,
            private_key=relay_private_key,
            token_contract=token.contract,
            to_address=target,
            amount_units=transfer_units,
            nonce=next_nonce,
            gas_price_wei=gas_price,
            gas_limit=gas_limit,
            source_address=relay_addr,
            recovery_timeout_seconds=recovery_timeout_seconds,
        )
        current = _update_relay_record(
            owner,
            current,
            status="token_forward_submitted",
            token_forward_txid=submission.tx_hash,
            token_forward_nonce=_tx_nonce_text(next_nonce),
            sweep_target=str(current.sweep_target or "").strip(),
            last_error="",
        )
        _append_relay_sweep_log(
            "recovery_auto_forward_submitted",
            network=network,
            relay_address=relay_addr,
            target=target,
            token=symbol,
            amount_units=str(transfer_units),
            txid=submission.tx_hash,
        )
        try:
            _wait_for_transaction_success(
                owner,
                network,
                submission.tx_hash,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转资产自动续转确认",
                timeout_seconds=timeout_seconds,
            )
            _wait_for_token_balance_at_least(
                owner,
                WithdrawRuntimeParams(
                    coin=symbol,
                    amount="0",
                    network=network,
                    delay=0.0,
                    threads=1,
                    token_contract=token.contract,
                    token_decimals=token.decimals,
                    token_is_native=False,
                ),
                target,
                target_before_units + transfer_units,
                label=f"{owner._mask(target, head=8, tail=6)} 自动续转到账",
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            raise RelayAutoForwardPending(
                f"中转资产自动续转已提交，但确认较慢：{exc}",
                txid=submission.tx_hash,
            ) from exc
        amount_text = owner._token_amount_text(
            symbol,
            owner._units_to_amount(transfer_units, token.decimals),
        )
        forwarded_parts.append(amount_text)
        gas_fee_total += gas_cost
        next_nonce += 1
    if forwarded_parts:
        current = _update_relay_record(owner, current, status="forwarded", last_error="")
    return current, gas_fee_total, forwarded_parts


def _relay_auto_forward_native_balance(
    owner,
    record: RelayWalletRecord,
    *,
    network: str,
    relay_addr: str,
    target: str,
    native_balance: int,
    timeout_seconds: float,
    recovery_timeout_seconds: float,
) -> tuple[RelayWalletRecord, int, int, str]:
    gas_price = owner.client.get_gas_price_wei(network)
    forward_value, gas_cost, _safety_units = _relay_sweep_plan(
        native_balance,
        gas_price,
        owner.client.NATIVE_GAS_LIMIT,
        keep_safety=False,
    )
    if forward_value <= 0:
        return record, 0, gas_cost, ""
    relay_private_key = owner.client.credential_to_private_key(record.private_key)
    nonce = owner.client.get_nonce(network, relay_addr)
    target_before_units = owner.client.get_balance_wei(network, target)
    submission = owner.client.submit_native_transfer_reliably(
        network=network,
        private_key=relay_private_key,
        to_address=target,
        value_wei=forward_value,
        nonce=nonce,
        gas_price_wei=gas_price,
        gas_limit=owner.client.NATIVE_GAS_LIMIT,
        source_address=relay_addr,
        recovery_timeout_seconds=recovery_timeout_seconds,
    )
    current = _update_relay_record(
        owner,
        record,
        status="token_forward_submitted",
        token_forward_txid=submission.tx_hash,
        token_forward_nonce=_tx_nonce_text(nonce),
        sweep_target=str(record.sweep_target or "").strip(),
        last_error="",
    )
    _append_relay_sweep_log(
        "recovery_auto_forward_submitted",
        network=network,
        relay_address=relay_addr,
        target=target,
        token=owner._network_fee_symbol(network),
        amount_units=str(forward_value),
        txid=submission.tx_hash,
    )
    try:
        _wait_for_transaction_success(
            owner,
            network,
            submission.tx_hash,
            label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转原生资产自动续转确认",
            timeout_seconds=timeout_seconds,
        )
        _wait_for_token_balance_at_least(
            owner,
            WithdrawRuntimeParams(
                coin=owner._network_fee_symbol(network),
                amount="0",
                network=network,
                delay=0.0,
                threads=1,
                token_decimals=18,
                token_is_native=True,
            ),
            target,
            target_before_units + forward_value,
            label=f"{owner._mask(target, head=8, tail=6)} 自动续转到账",
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        raise RelayAutoForwardPending(
            f"中转原生资产自动续转已提交，但确认较慢：{exc}",
            txid=submission.tx_hash,
        ) from exc
    current = _update_relay_record(owner, current, status="forwarded", last_error="")
    return current, forward_value, gas_cost, submission.tx_hash


def run_relay_fee_recovery(
    owner,
    dry_run: bool,
    *,
    batch_ids: set[str] | None = None,
    show_dialog: bool = True,
    use_progress: bool = True,
    manage_running_state: bool = True,
    run_label: str = "manual",
    sweep_timeout_seconds: float | None = None,
    sweep_target_override: str = "",
):
    dispatch_ui = owner._dispatch_ui
    try:
        batch_id_filter = {str(value or "").strip() for value in (batch_ids or set()) if str(value or "").strip()}
        records = owner.relay_wallet_store.load_records()
        if batch_id_filter:
            records = [record for record in records if str(getattr(record, "batch_id", "") or "").strip() in batch_id_filter]
        if not records:
            dispatch_ui(lambda: owner.log("中转手续费回收结束：中转钱包.txt 中没有可处理记录"))
            if show_dialog:
                dispatch_ui(lambda: messagebox.showinfo("提示", "中转钱包.txt 中没有可处理记录"))
            return

        jobs: list[tuple[str, str, RelayWalletRecord]] = []
        display_row_counts: dict[str, int] = {}
        display_context_by_key: dict[str, str] = {}
        for idx, record in enumerate(records, start=1):
            if not _relay_recovery_record_needs_scan(record, batch_scoped=bool(batch_id_filter)):
                continue
            progress_key = _relay_recovery_row_key(owner, record, fallback_index=idx)
            display_row_key = _relay_recovery_display_row_key(owner, record)
            if display_row_key:
                display_row_counts[display_row_key] = display_row_counts.get(display_row_key, 0) + 1
            jobs.append((progress_key, display_row_key, record))
        normalized_jobs: list[tuple[str, str, RelayWalletRecord]] = []
        for progress_key, display_row_key, record in jobs:
            resolved_display_key = display_row_key if display_row_counts.get(display_row_key, 0) <= 1 else ""
            if resolved_display_key:
                display_context_by_key[resolved_display_key] = _relay_recovery_row_context(owner, resolved_display_key, record)
            normalized_jobs.append((progress_key, resolved_display_key, record))
        jobs = normalized_jobs
        if not jobs:
            dispatch_ui(lambda: owner.log("中转手续费回收结束：中转钱包.txt 中没有有效记录"))
            if show_dialog:
                dispatch_ui(lambda: messagebox.showinfo("提示", "中转钱包.txt 中没有有效记录"))
            return

        override_target = str(sweep_target_override or "").strip()
        if override_target:
            override_target = owner._validate_recipient_address(override_target, "统一回收地址")
        worker_count = max(1, min(owner._runtime_worker_threads(), len(jobs)))
        sweep_timeout_value = _relay_sweep_timeout_seconds(sweep_timeout_seconds)
        progress_keys = owner._unique_row_keys([progress_key for progress_key, _display_row_key, _record in jobs]) if use_progress else []
        if use_progress:
            dispatch_ui(lambda n=worker_count: set_ui_batch_size(owner, n))
            dispatch_ui(lambda keys=progress_keys: owner._begin_progress("recovery", keys))
            dispatch_ui(lambda: owner._set_progress_metrics(amount_text="-", gas_text="-"))
        target_log = f"，override_target={override_target}" if override_target else ""
        dispatch_ui(lambda: owner.log(f"开始中转手续费回收：扫描 {len(jobs)} 条记录，dry_run={dry_run}，threads={worker_count}，label={run_label}{target_log}"))
        _append_relay_sweep_log(
            "recovery_start",
            run_label=run_label,
            dry_run=bool(dry_run),
            worker_threads=worker_count,
            record_count=len(jobs),
            batch_ids=sorted(batch_id_filter),
            sweep_target_override=override_target,
        )

        for progress_key, display_row_key, _record in jobs:
            if use_progress:
                dispatch_ui(lambda k=progress_key: owner._set_recovery_status(k, "waiting"))
            if display_row_key:
                owner._mark_recovery_status_context(display_row_key, display_context_by_key.get(display_row_key, ""))
                dispatch_ui(lambda k=display_row_key: owner._set_recovery_status(k, "waiting"))

        handled = 0
        failed = 0
        recovered = 0
        skipped = 0
        already_empty = 0
        kept_margin = 0
        pending = 0
        warnings = 0
        recovered_totals: dict[str, int] = {}
        gas_totals: dict[str, int] = {}
        manual_token_totals: dict[tuple[str, str, int], int] = {}
        manual_token_wallet_counts: dict[tuple[str, str, int], int] = {}
        manual_export_rows: list[dict[str, object]] = []
        cleanup_enabled = (not dry_run) and str(run_label or "").strip() == "manual" and not batch_id_filter
        cleanup_empty_record_keys: set[tuple[str, str]] = set()
        retry_collect_enabled = cleanup_enabled
        retry_candidates: dict[tuple[str, str], tuple[int, str, str, RelayWalletRecord]] = {}
        job_index_by_progress_key = {
            progress_key: index
            for index, (progress_key, _display_row_key, _record) in enumerate(jobs, start=1)
        }
        lock = threading.Lock()
        jobs_q: queue.Queue[tuple[int, str, str, RelayWalletRecord]] = queue.Queue()
        for index, (progress_key, display_row_key, record) in enumerate(jobs, start=1):
            jobs_q.put((index, progress_key, display_row_key, record))

        def finish_job(
            progress_key: str,
            display_row_key: str,
            *,
            outcome: str,
            msg: str,
            status_text: str,
            network: str = "",
            recovered_value: int = 0,
            gas_fee: int = 0,
            manual_tokens: dict[str, tuple[EvmToken, int]] | None = None,
            export_row: dict[str, object] | None = None,
            cleanup_record: RelayWalletRecord | None = None,
            retry_record: RelayWalletRecord | None = None,
        ) -> None:
            nonlocal handled, failed, recovered, skipped, already_empty, kept_margin, pending, warnings
            with lock:
                defer_for_retry = retry_collect_enabled and outcome in {"failed", "warning"} and retry_record is not None
                if defer_for_retry:
                    relay_address = str(getattr(retry_record, "relay_address", "") or "").strip()
                    retry_key = (str(getattr(retry_record, "batch_id", "") or "").strip(), relay_address.lower())
                    if relay_address:
                        retry_candidates[retry_key] = (
                            job_index_by_progress_key.get(progress_key, 0),
                            progress_key,
                            display_row_key,
                            retry_record,
                        )
                else:
                    if outcome == "failed":
                        failed += 1
                    elif outcome != "ignored":
                        handled += 1
                        if outcome == "recovered":
                            recovered += 1
                            if network and recovered_value > 0:
                                recovered_totals[network] = recovered_totals.get(network, 0) + recovered_value
                            if network and gas_fee > 0:
                                gas_totals[network] = gas_totals.get(network, 0) + gas_fee
                        elif outcome == "skipped":
                            skipped += 1
                        elif outcome == "already_empty":
                            already_empty += 1
                        elif outcome == "kept_margin":
                            kept_margin += 1
                        elif outcome == "pending":
                            pending += 1
                        elif outcome == "warning":
                            warnings += 1
                    if manual_tokens:
                        for _symbol, (token, units) in manual_tokens.items():
                            token_key = _relay_manual_token_key(network, token)
                            manual_token_totals[token_key] = manual_token_totals.get(token_key, 0) + max(0, int(units))
                            manual_token_wallet_counts[token_key] = manual_token_wallet_counts.get(token_key, 0) + 1
                    if export_row:
                        manual_export_rows.append(dict(export_row))
                    if cleanup_enabled and cleanup_record is not None:
                        relay_address = str(getattr(cleanup_record, "relay_address", "") or "").strip()
                        if relay_address:
                            cleanup_empty_record_keys.add((str(getattr(cleanup_record, "batch_id", "") or "").strip(), relay_address))
                amount_total_text = _relay_totals_text(owner, recovered_totals)
                gas_total_text = _relay_totals_text(owner, gas_totals, prefix="预估 ")
            dispatch_ui(lambda m=msg: owner.log(m))
            if use_progress:
                dispatch_ui(
                    lambda k=progress_key,
                    s=_relay_recovery_row_status(outcome): owner._set_recovery_status(k, s, "")
                )
            if display_row_key:
                dispatch_ui(
                    lambda k=display_row_key,
                    c=display_context_by_key.get(display_row_key, ""),
                    o=outcome,
                    t=status_text: _relay_apply_recovery_row_status(owner, k, c, o, t)
                )
            if use_progress:
                dispatch_ui(lambda a=amount_total_text, g=gas_total_text: owner._set_progress_metrics(amount_text=a, gas_text=g))

        def worker():
            while True:
                if owner.stop_requested.is_set():
                    return
                try:
                    index, progress_key, display_row_key, record = jobs_q.get_nowait()
                except queue.Empty:
                    return
                prefix = f"[{index}/{len(jobs)}][{owner._mask(record.relay_address, head=8, tail=6)}]"
                if use_progress:
                    dispatch_ui(lambda k=progress_key: owner._set_recovery_status(k, "running"))
                if display_row_key:
                    dispatch_ui(lambda k=display_row_key: owner._set_recovery_status(k, "running"))
                try:
                    network = str(record.network or "").strip().upper()
                    relay_addr = owner._validate_recipient_address(record.relay_address, "中转钱包地址")
                    sweep_target = owner._validate_recipient_address(override_target or record.sweep_target or record.source, "手续费回收地址")
                    snapshot = _relay_tracked_balance_snapshot(owner, network, relay_addr, record)
                    _native_token, native_balance = _relay_native_balance_entry(snapshot)
                    positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
                    positive_balance_text = _relay_positive_balance_text(owner, snapshot)
                    has_positive_balance = _relay_snapshot_has_positive_balance(snapshot)
                    current_status = str(record.status or "").strip()
                    sweep_resolution = str(record.sweep_resolution or "").strip().lower()
                    auto_forward_gas_fee = 0
                    forward_state = _transaction_receipt_state(owner, network, record.token_forward_txid)
                    state_text = {
                        "missing": "缺少第二跳交易记录",
                        "pending": "第二跳交易仍在确认中",
                        "failed": "第二跳交易失败",
                    }.get(forward_state, f"状态 {forward_state}")

                    if (
                        forward_state == "success"
                        and _relay_terminal_snapshot_can_be_ignored(owner, network, record, snapshot)
                    ):
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="ignored",
                            msg=f"{prefix} 已核对：中转钱包已是终态，仅剩已标记的原生币边际",
                            status_text="已核对",
                            cleanup_record=record,
                        )
                        continue

                    if not has_positive_balance:
                        if forward_state != "success":
                            if dry_run:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome=("pending" if forward_state == "pending" else "skipped"),
                                    msg=f"{prefix} 模拟跳过：主转账未完成（{state_text}），中转钱包为空不能作为成功依据",
                                    status_text=("确认中" if forward_state == "pending" else "归集未完成"),
                                )
                                continue
                            if forward_state == "pending":
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="pending",
                                    msg=f"{prefix} 等待确认：{state_text}，暂不判定为无需回收",
                                    status_text="确认中",
                                )
                                continue
                            failure_reason = f"主转账未完成：{state_text}；中转钱包为空不能作为转账成功依据"
                            record = owner.relay_wallet_store.update_record(
                                relay_addr,
                                batch_id=record.batch_id,
                                status="failed",
                                last_error=failure_reason,
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="failed",
                                msg=f"{prefix} 归集失败：{failure_reason}",
                                status_text="归集未完成",
                                retry_record=record,
                            )
                            continue
                        if dry_run:
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome=("ignored" if current_status == "completed" else "already_empty"),
                                msg=f"{prefix} 模拟核对完成：中转钱包无 BNB/USDT/USDC 余额",
                                status_text=("已核对" if current_status == "completed" else "已清空"),
                            )
                            continue
                        if current_status == "completed" and (
                            sweep_resolution in RELAY_SWEEP_TERMINAL_RESOLUTIONS or bool(str(record.gas_sweep_txid or "").strip())
                        ):
                            if record.last_error:
                                owner.relay_wallet_store.update_record(
                                    relay_addr,
                                    batch_id=record.batch_id,
                                    status=current_status,
                                    last_error="",
                                )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="ignored",
                                msg=f"{prefix} 已核对：中转钱包无 BNB/USDT/USDC 余额",
                                status_text="已核对",
                                cleanup_record=record,
                            )
                            continue
                        _update_relay_record(
                            owner,
                            record,
                            status="completed",
                            sweep_target=sweep_target,
                            sweep_resolution="manual_empty",
                            last_error="",
                        )
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="already_empty",
                            msg=f"{prefix} 无需回收：中转钱包无 BNB/USDT/USDC 余额",
                            status_text="已清空",
                            cleanup_record=record,
                        )
                        continue

                    if (
                        _relay_token_dust_requires_native_recovery(owner, network, snapshot)
                        and forward_state != "pending"
                        and not dry_run
                    ):
                        positive_tracked_tokens = {}

                    if positive_tracked_tokens:
                        if forward_state == "pending":
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 等待确认：当前仍有 {positive_balance_text}，第二跳交易尚未确认完成",
                                status_text="确认中",
                                network=network,
                                manual_tokens=positive_tracked_tokens,
                            )
                            continue
                        if dry_run:
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="skipped",
                                msg=f"{prefix} 模拟跳过：检测到 {positive_balance_text}，正式运行会自动续转到收款地址",
                                status_text="已跳过",
                                network=network,
                                manual_tokens=positive_tracked_tokens,
                            )
                            continue
                        try:
                            rescue_target = sweep_target
                            dust_below_threshold, dust_detail = _relay_token_dust_below_threshold(owner, positive_tracked_tokens)
                            if dust_below_threshold:
                                cleanup_record = record if _relay_snapshot_cleanup_allowed(owner, network, snapshot) else None
                                _update_relay_record(
                                    owner,
                                    record,
                                    status="completed",
                                    sweep_target=sweep_target,
                                    sweep_resolution="dust_left",
                                    last_error="",
                                )
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="kept_margin",
                                    msg=f"{prefix} 无需人工处理：残留资产低于 dust 阈值，已标记 dust_left（{dust_detail}）",
                                    status_text="已忽略dust",
                                    cleanup_record=cleanup_record,
                                )
                                continue
                            updated_record, token_gas_fee, forwarded_parts = _relay_auto_forward_tracked_tokens(
                                owner,
                                record,
                                network=network,
                                relay_addr=relay_addr,
                                target=rescue_target,
                                positive_tracked_tokens=positive_tracked_tokens,
                                timeout_seconds=sweep_timeout_value,
                                recovery_timeout_seconds=min(max(6.0, sweep_timeout_value / 3.0), 20.0),
                            )
                            record = updated_record
                            auto_forward_gas_fee += token_gas_fee
                            if forwarded_parts:
                                dispatch_ui(
                                    lambda m=(
                                        f"{prefix} 中转资产自动续转完成："
                                        f"{owner._mask(relay_addr, head=8, tail=6)} -> "
                                        f"{owner._mask(rescue_target, head=8, tail=6)}，"
                                        f"金额={' / '.join(forwarded_parts)}"
                                    ): owner.log(m)
                                )
                            snapshot = _relay_tracked_balance_snapshot(owner, network, relay_addr, record)
                            _native_token, native_balance = _relay_native_balance_entry(snapshot)
                            positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
                            positive_balance_text = _relay_positive_balance_text(owner, snapshot)
                            has_positive_balance = _relay_snapshot_has_positive_balance(snapshot)
                            forward_state = "success"
                        except RelayAutoForwardPending as exc:
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} {exc}",
                                status_text="确认中",
                            )
                            continue
                        except Exception as exc:
                            manual_reason = f"自动续转失败：{exc}；当前余额：{positive_balance_text}"
                            owner.relay_wallet_store.update_record(
                                relay_addr,
                                batch_id=record.batch_id,
                                status=current_status or "created",
                                sweep_target=sweep_target,
                                last_error=manual_reason,
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="warning",
                                msg=f"{prefix} 待人工处理：{manual_reason}",
                                status_text="待人工",
                                network=network,
                                manual_tokens=positive_tracked_tokens,
                                export_row=_relay_build_manual_export_row(
                                    owner,
                                    record,
                                    snapshot,
                                    sweep_target=sweep_target,
                                    reason=manual_reason,
                                ),
                                retry_record=record,
                            )
                            continue
                        if positive_tracked_tokens:
                            manual_reason = f"自动续转后仍有余额：{positive_balance_text}"
                            owner.relay_wallet_store.update_record(
                                relay_addr,
                                batch_id=record.batch_id,
                                status=current_status or "created",
                                sweep_target=sweep_target,
                                last_error=manual_reason,
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="warning",
                                msg=f"{prefix} 待人工处理：{manual_reason}",
                                status_text="待人工",
                                network=network,
                                manual_tokens=positive_tracked_tokens,
                                export_row=_relay_build_manual_export_row(
                                    owner,
                                    record,
                                    snapshot,
                                    sweep_target=sweep_target,
                                    reason=manual_reason,
                                ),
                                retry_record=record,
                            )
                            continue
                        if not has_positive_balance:
                            _update_relay_record(
                                owner,
                                record,
                                status="completed",
                                sweep_target=sweep_target,
                                sweep_resolution="manual_empty",
                                last_error="",
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="already_empty",
                                msg=f"{prefix} 自动续转完成：中转钱包已清空",
                                status_text="已清空",
                                network=network,
                                gas_fee=auto_forward_gas_fee,
                                cleanup_record=record,
                            )
                            continue

                    sweep_state = _transaction_receipt_state(owner, network, record.gas_sweep_txid)
                    if sweep_state == "pending":
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="pending",
                            msg=f"{prefix} 等待确认：已有手续费回收交易在链上确认中，当前余额 {positive_balance_text}",
                            status_text="确认中",
                        )
                        continue

                    if forward_state != "success":
                        if forward_state == "pending":
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 等待确认：{state_text}，当前余额 {positive_balance_text}",
                                status_text="确认中",
                            )
                            continue
                        if not bool(record.token_is_native):
                            if dry_run:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="skipped",
                                    msg=f"{prefix} 模拟跳过：{state_text}，未检测到代币余额也不能证明第二跳成功",
                                    status_text="已跳过",
                                )
                                continue
                            failure_reason = f"主转账未完成：{state_text}；未检测到代币余额，不能据此判定第二跳成功"
                            record = owner.relay_wallet_store.update_record(
                                relay_addr,
                                batch_id=record.batch_id,
                                status="failed",
                                sweep_target=sweep_target,
                                last_error=failure_reason,
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="failed",
                                msg=f"{prefix} 归集失败：{failure_reason}",
                                status_text="归集未完成",
                                retry_record=record,
                            )
                            continue
                        else:
                            if dry_run:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="skipped",
                                    msg=f"{prefix} 模拟跳过：{state_text}，正式运行会尝试将中转原生资产续转到收款地址",
                                    status_text="已跳过",
                                )
                                continue
                            try:
                                rescue_target = sweep_target
                                record, forward_value, native_forward_gas, _txid = _relay_auto_forward_native_balance(
                                    owner,
                                    record,
                                    network=network,
                                    relay_addr=relay_addr,
                                    target=rescue_target,
                                    native_balance=native_balance,
                                    timeout_seconds=sweep_timeout_value,
                                    recovery_timeout_seconds=min(max(6.0, sweep_timeout_value / 3.0), 20.0),
                                )
                                auto_forward_gas_fee += native_forward_gas
                                if forward_value <= 0:
                                    native_dust, native_dust_detail = _relay_native_dust_below_threshold(
                                        owner,
                                        network,
                                        native_balance,
                                    )
                                    if not native_dust:
                                        raise RuntimeError(
                                            "中转原生资产余额不足以支付转账 gas，且不低于 dust 阈值，需人工确认"
                                        )
                                    cleanup_record = record if _relay_snapshot_cleanup_allowed(owner, network, snapshot) else None
                                    _update_relay_record(
                                        owner,
                                        record,
                                        status="completed",
                                        sweep_target=sweep_target,
                                        sweep_resolution="dust_left",
                                        last_error="",
                                    )
                                    finish_job(
                                        progress_key,
                                        display_row_key,
                                        outcome="kept_margin",
                                        msg=(
                                            f"{prefix} 暂不继续清空：{state_text}，"
                                            f"当前余额低于 dust 阈值，已标记 dust_left（{native_dust_detail}）"
                                        ),
                                        status_text="已留边际",
                                        cleanup_record=cleanup_record,
                                    )
                                    continue
                                dispatch_ui(
                                    lambda m=(
                                        f"{prefix} 中转原生资产自动续转完成："
                                        f"{owner._mask(relay_addr, head=8, tail=6)} -> "
                                        f"{owner._mask(rescue_target, head=8, tail=6)}，"
                                        f"金额={owner._gas_fee_amount_text(network, forward_value)}"
                                    ): owner.log(m)
                                )
                                snapshot = _relay_tracked_balance_snapshot(owner, network, relay_addr, record)
                                _native_token, native_balance = _relay_native_balance_entry(snapshot)
                                positive_balance_text = _relay_positive_balance_text(owner, snapshot)
                                has_positive_balance = _relay_snapshot_has_positive_balance(snapshot)
                                forward_state = "success"
                            except RelayAutoForwardPending as exc:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="pending",
                                    msg=f"{prefix} {exc}",
                                    status_text="确认中",
                                )
                                continue
                            except Exception as exc:
                                manual_reason = f"自动续转失败：{exc}；当前余额 {positive_balance_text}"
                                owner.relay_wallet_store.update_record(
                                    relay_addr,
                                    batch_id=record.batch_id,
                                    status=current_status or "created",
                                    sweep_target=sweep_target,
                                    last_error=manual_reason,
                                )
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="warning",
                                    msg=f"{prefix} 待人工处理：{manual_reason}",
                                    status_text="待人工",
                                    network=network,
                                    export_row=_relay_build_manual_export_row(
                                        owner,
                                        record,
                                        snapshot,
                                        sweep_target=sweep_target,
                                        reason=manual_reason,
                                    ),
                                    retry_record=record,
                                )
                                continue

                    if dry_run:
                        if forward_state != "success":
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="skipped",
                                msg=f"{prefix} 模拟跳过：第二跳未确认成功，当前状态={forward_state}",
                                status_text="已跳过",
                            )
                            continue
                        if sweep_state == "pending":
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 模拟跳过：已有手续费回收交易确认中",
                                status_text="确认中",
                            )
                            continue
                        sweep_gas_price = owner.client.get_gas_price_wei(network)
                        sweep_value, sweep_gas_cost, safety_units = _relay_sweep_plan(
                            native_balance,
                            sweep_gas_price,
                            owner.client.NATIVE_GAS_LIMIT,
                            keep_safety=True,
                        )
                        if native_balance <= 0:
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="already_empty",
                                msg=f"{prefix} 模拟结果：中转钱包已清空，无需回收",
                                status_text="已清空",
                            )
                        elif sweep_value <= 0:
                            native_dust, native_dust_detail = _relay_native_dust_below_threshold(
                                owner,
                                network,
                                native_balance,
                            )
                            if not native_dust:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="skipped",
                                    msg=(
                                        f"{prefix} 模拟跳过：当前余额 {owner._gas_fee_amount_text(network, native_balance)} "
                                        "不足以支付回收 gas，且不低于 dust 阈值"
                                    ),
                                    status_text="已跳过",
                                )
                                continue
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="kept_margin",
                                msg=(
                                    f"{prefix} 模拟结果：BNB 残留低于 dust 阈值，正式运行会标记 dust_left"
                                    f"（{native_dust_detail}）"
                                ),
                                status_text="已留边际",
                            )
                        else:
                            status_text = _relay_recovery_amount_status_text(
                                owner,
                                network,
                                sweep_value,
                                kept_margin=safety_units > 0,
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="recovered",
                                msg=(
                                    f"{prefix} 模拟可回收：{owner._mask(relay_addr, head=8, tail=6)} -> "
                                    f"{owner._mask(sweep_target, head=8, tail=6)}，金额={owner._gas_fee_amount_text(network, sweep_value)}"
                                ),
                                status_text=status_text,
                                network=network,
                                recovered_value=sweep_value,
                                gas_fee=sweep_gas_cost,
                            )
                        continue

                    if sweep_state == "pending":
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="pending",
                            msg=f"{prefix} 跳过：已有手续费回收交易确认中，等待链上确认",
                            status_text="确认中",
                        )
                        continue

                    if forward_state != "success":
                        state_text = {
                            "missing": "缺少第二跳交易记录",
                            "pending": "第二跳交易仍在确认中",
                            "failed": "第二跳交易失败",
                        }.get(forward_state, f"状态={forward_state}")
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="skipped",
                            msg=f"{prefix} 跳过：{state_text}，为避免误操作，本次不自动回收",
                            status_text="已跳过",
                        )
                        continue

                    if native_balance <= 0:
                        _update_relay_record(
                            owner,
                            record,
                            status="completed",
                            sweep_target=sweep_target,
                            sweep_resolution="manual_empty",
                            last_error="",
                        )
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="already_empty",
                            msg=f"{prefix} 无需回收：中转钱包已清空",
                            status_text="已清空",
                            cleanup_record=record,
                        )
                        continue

                    sweep_txid = ""
                    sweep_gas_cost = 0
                    sweep_expected_remaining = 0
                    sweep_submitted = False
                    last_sweep_error: Exception | None = None
                    for attempt in range(1, 4):
                        native_balance = owner.client.get_balance_wei(network, relay_addr)
                        sweep_gas_price = owner.client.get_gas_price_wei(network)
                        keep_safety = attempt >= 2
                        sweep_value, sweep_gas_cost, safety_units = _relay_sweep_plan(
                            native_balance,
                            sweep_gas_price,
                            owner.client.NATIVE_GAS_LIMIT,
                            keep_safety=keep_safety,
                        )
                        sweep_expected_remaining = safety_units
                        if sweep_value <= 0:
                            break
                        sweep_nonce = owner.client.get_nonce(network, relay_addr)
                        try:
                            sweep_submission = owner.client.submit_native_transfer_reliably(
                                network=network,
                                private_key=owner.client.credential_to_private_key(record.private_key),
                                to_address=sweep_target,
                                value_wei=sweep_value,
                                nonce=sweep_nonce,
                                gas_price_wei=sweep_gas_price,
                                gas_limit=owner.client.NATIVE_GAS_LIMIT,
                                source_address=relay_addr,
                                recovery_timeout_seconds=min(max(6.0, sweep_timeout_value / 3.0), 20.0),
                            )
                            sweep_txid = sweep_submission.tx_hash
                            sweep_submitted = True
                            break
                        except Exception as sweep_exc:
                            last_sweep_error = sweep_exc
                            err_text = str(sweep_exc).lower()
                            if "insufficient funds" not in err_text or attempt >= 3:
                                raise
                            dispatch_ui(
                                lambda m=f"{prefix} 中转手续费回收余额未同步，准备重试第 {attempt + 1} 次": owner.log(m)
                            )
                            if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
                                raise RuntimeError("任务已停止")

                    if sweep_submitted:
                        _update_relay_record(
                            owner,
                            record,
                            status="sweep_submitted",
                            gas_sweep_txid=sweep_txid,
                            gas_sweep_nonce=_tx_nonce_text(sweep_nonce),
                            sweep_target=sweep_target,
                            last_error="",
                        )
                        dispatch_ui(
                            lambda m=(
                                f"{prefix} 中转手续费回收已提交：回收地址={sweep_target}，txid={sweep_txid}"
                            ): owner.log(m)
                        )
                        try:
                            _wait_for_transaction_success(
                                owner,
                                network,
                                sweep_txid,
                                label=f"{prefix} 中转手续费回收确认",
                                timeout_seconds=sweep_timeout_value,
                            )
                            _wait_for_native_balance_at_most(
                                owner,
                                network,
                                relay_addr,
                                sweep_expected_remaining,
                                label=f"{prefix} 中转钱包清空",
                                timeout_seconds=sweep_timeout_value,
                            )
                        except Exception as exc:
                            _update_relay_record(
                                owner,
                                record,
                                status="completed_pending_sweep",
                                gas_sweep_txid=sweep_txid,
                                sweep_target=sweep_target,
                                last_error=str(exc),
                            )
                            _append_relay_sweep_log(
                                "recovery_submitted_pending",
                                run_label=run_label,
                                network=network,
                                relay_address=relay_addr,
                                  target=sweep_target,
                                txid=sweep_txid,
                                error=str(exc),
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 手续费回收已提交，但余额确认较慢；请稍后重试回收或按 txid 检查",
                                status_text="确认中",
                            )
                            continue
                        sweep_resolution = "dust_left" if sweep_expected_remaining > 0 else ""
                        _update_relay_record(
                            owner,
                            record,
                            status="completed",
                            gas_sweep_txid=sweep_txid,
                            sweep_target=sweep_target,
                            sweep_resolution=sweep_resolution,
                            last_error="",
                        )
                        status_text = _relay_recovery_amount_status_text(
                            owner,
                            network,
                            sweep_value,
                            kept_margin=sweep_expected_remaining > 0,
                        )
                        msg = (
                            f"{prefix} 中转手续费回收完成："
                            f"{owner._mask(relay_addr, head=8, tail=6)} -> {owner._mask(sweep_target, head=8, tail=6)}，"
                            f"金额={owner._gas_fee_amount_text(network, sweep_value)}"
                        )
                        if sweep_expected_remaining > 0:
                            margin_text = owner._gas_fee_amount_text(network, sweep_expected_remaining)
                            msg = f"{msg}，保留安全边际={margin_text}"
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="recovered",
                            msg=msg,
                            status_text=status_text,
                            network=network,
                            recovered_value=sweep_value,
                            gas_fee=sweep_gas_cost + auto_forward_gas_fee,
                            cleanup_record=(record if _relay_native_cleanup_allowed(owner, network, sweep_expected_remaining) else None),
                        )
                        continue

                    if last_sweep_error is not None:
                        raise last_sweep_error

                    native_balance = owner.client.get_balance_wei(network, relay_addr)
                    sweep_gas_price = owner.client.get_gas_price_wei(network)
                    _sweep_value, _sweep_gas_cost, _safety_units = _relay_sweep_plan(
                        native_balance,
                        sweep_gas_price,
                        owner.client.NATIVE_GAS_LIMIT,
                        keep_safety=True,
                    )
                    native_dust, native_dust_detail = _relay_native_dust_below_threshold(
                        owner,
                        network,
                        native_balance,
                    )
                    if not native_dust:
                        manual_reason = (
                            f"中转钱包原生币余额 {owner._gas_fee_amount_text(network, native_balance)} "
                            "不足以支付回收 gas，且不低于 dust 阈值，需人工确认"
                        )
                        owner.relay_wallet_store.update_record(
                            relay_addr,
                            batch_id=record.batch_id,
                            status=current_status or "forwarded",
                            sweep_target=sweep_target,
                            last_error=manual_reason,
                        )
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="warning",
                            msg=f"{prefix} 待人工处理：{manual_reason}",
                            status_text="待人工",
                            network=network,
                            export_row=_relay_build_manual_export_row(
                                owner,
                                record,
                                snapshot,
                                sweep_target=sweep_target,
                                reason=manual_reason,
                            ),
                            retry_record=record,
                        )
                        continue
                    cleanup_record = record if _relay_snapshot_cleanup_allowed(owner, network, snapshot) else None
                    _update_relay_record(
                        owner,
                        record,
                        status="completed",
                        sweep_target=sweep_target,
                        sweep_resolution="dust_left",
                        last_error="",
                    )
                    finish_job(
                        progress_key,
                        display_row_key,
                        outcome="kept_margin",
                        msg=(
                            f"{prefix} 暂不继续清空：BNB 残留低于 dust 阈值，"
                            f"已标记 dust_left（{native_dust_detail}）"
                        ),
                        status_text="已留边际",
                        cleanup_record=cleanup_record,
                    )
                except Exception as exc:
                    try:
                        _update_relay_record(
                            owner,
                            record,
                            status="failed",
                            last_error=str(exc),
                        )
                    except Exception:
                        pass
                    _append_relay_sweep_log(
                        "recovery_record_failed",
                        run_label=run_label,
                        network=str(getattr(record, "network", "") or "").strip().upper(),
                        relay_address=str(getattr(record, "relay_address", "") or "").strip(),
                        target=str(getattr(record, "target", "") or "").strip(),
                        error=str(exc),
                    )
                    finish_job(
                        progress_key,
                        display_row_key,
                        outcome="failed",
                        msg=f"{prefix} 中转手续费回收失败：{exc}",
                        status_text="失败",
                        retry_record=record,
                    )
                finally:
                    jobs_q.task_done()

        def run_workers(count: int) -> None:
            workers: list[threading.Thread] = []
            for _ in range(max(1, int(count))):
                t = _start_relay_rpc_worker(owner, worker, name="onchain-relay-recovery-worker")
                workers.append(t)
            for t in workers:
                t.join()

        with _relay_defer_record_writes(owner):
            run_workers(worker_count)
            retry_collect_enabled = False
            if retry_candidates and not owner.stop_requested.is_set():
                latest_records = {
                    (
                        str(getattr(record, "batch_id", "") or "").strip(),
                        str(getattr(record, "relay_address", "") or "").strip().lower(),
                    ): record
                    for record in owner.relay_wallet_store.load_records()
                    if str(getattr(record, "relay_address", "") or "").strip()
                }
                retry_jobs = sorted(retry_candidates.items(), key=lambda item: item[1][0])
                for retry_key, (index, progress_key, display_row_key, original_record) in retry_jobs:
                    jobs_q.put((index, progress_key, display_row_key, latest_records.get(retry_key, original_record)))
                retry_worker_count = min(worker_count, len(retry_jobs))
                dispatch_ui(
                    lambda n=len(retry_jobs), w=retry_worker_count: owner.log(
                        f"首轮回收已完成，对 {n} 个失败/待人工账号进行一次二次验证，threads={w}"
                    )
                )
                _append_relay_sweep_log(
                    "recovery_retry_start",
                    run_label=run_label,
                    record_count=len(retry_jobs),
                    worker_threads=retry_worker_count,
                )
                run_workers(retry_worker_count)
            _relay_flush_record_writes(owner)

        if owner.stop_requested.is_set():
            dispatch_ui(lambda: owner.log("中转手续费回收任务已停止"))

        cleanup_removed = 0
        if cleanup_empty_record_keys and not owner.stop_requested.is_set():
            cleanup_removed = owner.relay_wallet_store.remove_records(cleanup_empty_record_keys)
            if cleanup_removed > 0:
                dispatch_ui(lambda n=cleanup_removed: owner.log(f"手动回收已清理无效中转钱包记录：{n} 条"))

        summary = (
            f"中转手续费回收结束：已处理 {handled}，失败 {failed}，"
            f"已回收 {recovered}，已清空 {already_empty}，已留边际 {kept_margin}，跳过 {skipped}，确认中 {pending}，待手动 {warnings}"
        )
        if cleanup_removed > 0:
            summary = f"{summary}，已清理无效记录 {cleanup_removed}"
        recovered_text = _relay_totals_text(owner, recovered_totals)
        gas_text = _relay_totals_text(owner, gas_totals, prefix="预估 ")
        if recovered_text != "-" or gas_text != "-":
            summary = f"{summary}，回收总额={recovered_text}，gas合计={gas_text}"
        export_path = ""
        if batch_id_filter:
            replace_addresses = {
                str(getattr(record, "relay_address", "") or "").strip()
                for _progress_key, _display_row_key, record in jobs
                if str(getattr(record, "relay_address", "") or "").strip()
            }
            _relay_write_manual_export_file(
                manual_export_rows,
                preserve_existing=True,
                replace_addresses=replace_addresses,
            )
            if manual_export_rows and RELAY_MANUAL_EXPORT_FILE.exists():
                export_path = str(RELAY_MANUAL_EXPORT_FILE)
        elif manual_export_rows:
            export_path = _relay_write_manual_export_file(manual_export_rows)
        else:
            _relay_write_manual_export_file([])
        gas_text = _relay_totals_text(owner, gas_totals, prefix="预计 ")
        usdt_text = _relay_manual_token_summary_text(owner, "USDT", manual_token_totals, manual_token_wallet_counts)
        usdc_text = _relay_manual_token_summary_text(owner, "USDC", manual_token_totals, manual_token_wallet_counts)
        summary_lines = [
            (
                f"中转手续费回收结束：已处理 {handled}，失败 {failed}，已回收 {recovered}，"
                f"已清空 {already_empty}，已留边际 {kept_margin}，跳过 {skipped}，确认中 {pending}，待人工 {warnings}"
            ),
            f"总回收：{recovered_text}",
            f"Gas 合计：{gas_text}",
            f"残留 USDT：{usdt_text}",
            f"残留 USDC：{usdc_text}",
        ]
        if cleanup_removed > 0:
            summary_lines.append(f"已清理无效记录：{cleanup_removed} 条")
        if export_path:
            summary_lines.append(f"待人工私钥导出：{export_path}")
        summary = "\n".join(summary_lines)
        dispatch_ui(lambda m="；".join(summary_lines): owner.log(m))
        _append_relay_sweep_log(
            "recovery_finish",
            run_label=run_label,
            handled=handled,
            failed=failed,
            recovered=recovered,
            already_empty=already_empty,
            kept_margin=kept_margin,
            skipped=skipped,
            pending=pending,
            warnings=warnings,
            recovered_text=recovered_text,
            gas_text=gas_text,
            cleanup_removed=cleanup_removed,
            export_path=export_path,
        )
        if show_dialog:
            manual_copy_lines = _relay_manual_account_copy_lines(manual_export_rows) if cleanup_enabled else []
            action_buttons = [
                (
                    "复制待人工账号",
                    lambda values=list(manual_copy_lines): _relay_copy_manual_accounts(owner, values),
                    bool(manual_copy_lines),
                )
            ] if cleanup_enabled else None
            dispatch_ui(
                lambda s=handled, f=failed, detail=summary, actions=action_buttons: owner._show_result_summary_dialog(
                    title="执行完成",
                    summary_title="中转手续费回收完成",
                    success=s,
                    failed=f,
                    detail_text=detail,
                    action_buttons=actions,
                    success_label="已处理",
                )
            )
        if use_progress:
            dispatch_ui(lambda s=handled, f=failed: owner._finish_progress("recovery", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"中转手续费回收异常终止：{err_text}": owner.log(m))
        _append_relay_sweep_log("recovery_error", run_label=run_label, error=err_text)
        if show_dialog:
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        if use_progress:
            dispatch_ui(lambda: owner._finish_progress("recovery", 0, 1))
    finally:
        owner._clear_source_private_key_cache()
        if manage_running_state:
            owner.is_running = False



@dataclass
class _RelayBatchItem:
    index: int
    row_key: str
    source_raw: str
    target: str
    source_private_key: str
    source_addr: str
    relay_record: RelayWalletRecord
    relay_private_key: str
    prefix: str
    relay_name: str
    target_name: str
    source_label: str
    amount_text: str
    value_units: int
    reserve_units: int = 0
    source_fee_gas_price: int = 0
    source_fee_gas_limit: int = 0
    source_token_gas_price: int = 0
    source_token_gas_limit: int = 0
    gas_fee_wei: int = 0
    final_amount_text: str = ""
    failed: bool = False
    failure_reason: str = ""

    @property
    def relay_addr(self) -> str:
        return str(self.relay_record.relay_address or "").strip()


def _relay_record_status(item: _RelayBatchItem) -> str:
    return str(getattr(item.relay_record, "status", "") or "").strip()


def _relay_record_txid(item: _RelayBatchItem, attr: str) -> str:
    return str(getattr(item.relay_record, attr, "") or "").strip()


def _relay_defer_record_writes(owner):
    store = getattr(owner, "relay_wallet_store", None)
    manager = getattr(store, "defer_writes", None)
    if callable(manager):
        return manager()
    return nullcontext()


def _relay_flush_record_writes(owner) -> None:
    store = getattr(owner, "relay_wallet_store", None)
    flush = getattr(store, "flush", None)
    if callable(flush):
        flush()


def _relay_elapsed_text(started_at: float) -> str:
    elapsed = max(0.0, time.monotonic() - float(started_at))
    return f"{elapsed:.3f}s"


def _relay_update_item_record(owner, item: _RelayBatchItem, status: str, **changes: object) -> RelayWalletRecord:
    updated = owner.relay_wallet_store.update_record(
        item.relay_addr,
        batch_id=item.relay_record.batch_id,
        status=status,
        **changes,
    )
    item.relay_record = updated
    return updated


def _relay_dispatch_row_status(owner, item: _RelayBatchItem, status: str, status_text: str = "") -> None:
    owner._dispatch_ui(lambda k=item.row_key, s=status, t=status_text: owner._set_status(k, s, t))


def _relay_dispatch_recovery_status(owner, item: _RelayBatchItem, status: str, status_text: str = "") -> None:
    owner._dispatch_ui(lambda k=item.row_key, s=status, t=status_text: owner._set_recovery_status(k, s, t))


def _relay_clear_missing_source_txids(
    owner,
    params: WithdrawRuntimeParams,
    items: list[_RelayBatchItem],
    *,
    tx_attr: str,
    nonce_attr: str,
    done_status: str,
    done_statuses: set[str],
) -> None:
    for item in items:
        if item.failed or _relay_record_status(item) in done_statuses:
            continue
        txid = _relay_record_txid(item, tx_attr)
        stored_nonce = _tx_nonce_value(_relay_record_txid(item, nonce_attr))
        if not txid or stored_nonce is None:
            continue
        try:
            if owner.client.get_transaction_receipt(params.network, txid) is not None:
                continue
            if owner.client.get_transaction_by_hash(params.network, txid) is not None:
                continue
            relay_balance = owner.client.get_balance_wei(params.network, item.relay_addr)
            expected_units = item.value_units + item.reserve_units
            if relay_balance >= expected_units:
                _relay_update_item_record(owner, item, done_status, last_error="")
                owner._dispatch_ui(
                    lambda m=f"{item.prefix} 旧交易哈希不可查，但中转钱包已足额到账，按已确认继续": owner.log(m)
                )
                continue
            confirmed_nonce = owner.client.get_confirmed_nonce(params.network, item.source_addr)
        except Exception:
            continue
        nonce_changes = {nonce_attr: ""} if confirmed_nonce > stored_nonce else {}
        _relay_update_item_record(
            owner,
            item,
            _relay_record_status(item) or "created",
            **{tx_attr: "", "last_error": "", **nonce_changes},
        )
        retry_text = (
            "原 nonce 已被确认交易占用，改用当前 nonce 重新广播"
            if nonce_changes
            else f"保留 nonce={stored_nonce} 重新广播"
        )
        owner._dispatch_ui(
            lambda m=f"{item.prefix} 旧交易未上链，{retry_text}：txid={txid}": owner.log(m)
        )


def _relay_preflight_1m_native_source(
    owner,
    params: WithdrawRuntimeParams,
    items: list[_RelayBatchItem],
    *,
    tx_attr: str,
    done_statuses: set[str],
) -> None:
    active_items = [
        item
        for item in items
        if not item.failed
        and _relay_record_status(item) not in done_statuses
        and not _relay_record_txid(item, tx_attr)
    ]
    if not active_items:
        return
    gas_price = owner.client.get_gas_price_wei(params.network)
    gas_limit = owner.client.NATIVE_GAS_LIMIT
    grouped: dict[str, list[_RelayBatchItem]] = {}
    for item in active_items:
        if item.value_units <= 0:
            raise RuntimeError(f"{item.prefix} 转账数量必须大于 0")
        grouped.setdefault(item.source_addr.lower(), []).append(item)
        item.source_token_gas_price = gas_price
        item.source_token_gas_limit = gas_limit
    for source_items in grouped.values():
        source_addr = source_items[0].source_addr
        required_units = sum(
            item.value_units + item.reserve_units + gas_price * gas_limit
            for item in source_items
        )
        balance_units = owner.client.get_balance_wei(params.network, source_addr)
        if balance_units < required_units:
            current_text = owner._gas_fee_amount_text(params.network, balance_units)
            required_text = owner._gas_fee_amount_text(params.network, required_units)
            with _relay_defer_record_writes(owner):
                for item in source_items:
                    item.relay_record = owner.relay_wallet_store.update_record(
                        item.relay_addr,
                        batch_id=item.relay_record.batch_id,
                        status="created",
                        transfer_amount="",
                        transfer_units="",
                        last_error="",
                    )
                    item.amount_text = ""
                    item.value_units = 0
                _relay_flush_record_writes(owner)
            raise RuntimeError(
                f"一对多中转整批余额不足：当前 {current_text}，本批 {len(source_items)} 笔至少需要 {required_text}"
            )


def _relay_fail_item(owner, item: _RelayBatchItem, reason: object, *, log_prefix: str = "中转失败") -> None:
    if item.failed:
        return
    text = str(reason or "未知错误").strip() or "未知错误"
    item.failed = True
    item.failure_reason = text
    try:
        _relay_update_item_record(owner, item, "failed", last_error=text)
    except Exception:
        pass
    _relay_dispatch_row_status(owner, item, "failed", text)
    owner._dispatch_ui(lambda m=f"{item.prefix} {log_prefix}：{text}": owner.log(m))


def _relay_run_parallel(owner, units: list, worker_count: int, func) -> None:
    if not units:
        return
    jobs_q: queue.Queue = queue.Queue()
    for unit in units:
        jobs_q.put(unit)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def worker() -> None:
        while not owner.stop_requested.is_set():
            try:
                unit = jobs_q.get_nowait()
            except queue.Empty:
                return
            try:
                func(unit)
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)
            finally:
                jobs_q.task_done()

    workers: list[threading.Thread] = []
    count = max(1, min(int(worker_count or 1), len(units)))
    for _ in range(count):
        t = _start_relay_rpc_worker(owner, worker, name="onchain-relay-worker")
        workers.append(t)
    for t in workers:
        t.join()
    if errors:
        raise errors[0]


def _relay_submit_with_retry(owner, params: WithdrawRuntimeParams, item: _RelayBatchItem, label: str, submitter) -> str:
    from onchain_transaction_recovery import transaction_context, UnconfirmedTransactionError
    attempts = _receipt_retry_count(params)
    interval = _receipt_poll_interval_seconds(params)
    last_error = ""
    last_unconfirmed = None
    for attempt in range(1, attempts + 1):
        if owner.stop_requested.is_set():
            if last_unconfirmed is not None:
                raise last_unconfirmed
            raise RuntimeError("任务已停止")
        try:
            with transaction_context(owner.client, batch_id=item.relay_record.batch_id,
                                     task_key=f"{item.relay_addr.lower()}:{label}", stop_event=owner.stop_requested):
                submission = submitter()
            item._last_submitted_nonce = getattr(submission, "nonce", None)
            txid = str(getattr(submission, "tx_hash", "") or "").strip()
            if txid:
                return txid
            last_error = "未拿到 txid"
        except Exception as exc:
            last_error = str(exc)
            if isinstance(exc, UnconfirmedTransactionError):
                last_unconfirmed = exc
        if attempt < attempts:
            owner._dispatch_ui(lambda m=f"{item.prefix} {label}提交重试 {attempt}/{attempts}：{last_error}": owner.log(m))
            if owner.stop_requested.wait(interval):
                if last_unconfirmed is not None:
                    raise last_unconfirmed
                raise RuntimeError("任务已停止")
    if last_unconfirmed is not None:
        raise last_unconfirmed
    raise RuntimeError(f"{label}提交失败：{last_error}")


def _relay_source_stage(
    owner,
    params: WithdrawRuntimeParams,
    items: list[_RelayBatchItem],
    *,
    stage_label: str,
    tx_attr: str,
    nonce_attr: str,
    submitted_status: str,
    done_status: str,
    done_statuses: set[str],
    submit_one,
    status_text: str,
) -> None:
    source_groups: dict[str, list[_RelayBatchItem]] = {}
    for item in items:
        if item.failed:
            continue
        if _relay_record_status(item) in done_statuses:
            continue
        source_groups.setdefault(item.source_addr.lower(), []).append(item)
    if not source_groups:
        return
    source_nonce_next: dict[str, int] = {}
    source_nonce_locks: dict[str, threading.Lock] = {}
    source_guard = threading.Lock()

    def alloc_nonce(source_addr: str, stored_nonce_text: str = "") -> int:
        stored = _tx_nonce_value(stored_nonce_text)
        if stored is not None:
            with source_guard:
                source_lock = source_nonce_locks.setdefault(source_addr, threading.Lock())
            with source_lock:
                cached_next = source_nonce_next.get(source_addr)
                if cached_next is None or cached_next <= stored:
                    source_nonce_next[source_addr] = stored + 1
            return stored
        with source_guard:
            source_lock = source_nonce_locks.setdefault(source_addr, threading.Lock())
        with source_lock:
            nonce = source_nonce_next.get(source_addr)
            if nonce is None:
                nonce = owner.client.get_nonce(params.network, source_addr)
            source_nonce_next[source_addr] = nonce + 1
            return nonce

    def rollback_nonce(source_addr: str, used_nonce: int) -> None:
        with source_guard:
            source_lock = source_nonce_locks.setdefault(source_addr, threading.Lock())
        with source_lock:
            cached_next = source_nonce_next.get(source_addr)
            if cached_next == used_nonce + 1:
                source_nonce_next.pop(source_addr, None)

    def submit_item(item: _RelayBatchItem) -> None:
        if item.failed or _relay_record_status(item) in done_statuses:
            return
        txid = _relay_record_txid(item, tx_attr)
        if txid:
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已提交")
            return
        used_nonce: int | None = None
        try:
            used_nonce = alloc_nonce(item.source_addr, _relay_record_txid(item, nonce_attr))
            txid = submit_one(item, used_nonce)
            used_nonce = getattr(item, "_last_submitted_nonce", None) if getattr(item, "_last_submitted_nonce", None) is not None else used_nonce
            _relay_update_item_record(
                owner,
                item,
                submitted_status,
                **{tx_attr: txid, nonce_attr: _tx_nonce_text(used_nonce), "last_error": ""},
            )
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已提交")
            owner._dispatch_ui(lambda m=f"{item.prefix} {stage_label}已提交：txid={txid}": owner.log(m))
        except Exception as exc:
            from onchain_transaction_recovery import UnconfirmedTransactionError
            if isinstance(exc, UnconfirmedTransactionError) and exc.own_transaction:
                _relay_update_item_record(owner, item, submitted_status,
                                          **{tx_attr: exc.tx_hash, nonce_attr: _tx_nonce_text(exc.nonce), "last_error": str(exc)})
            elif used_nonce is not None:
                rollback_nonce(item.source_addr, used_nonce)
            _relay_fail_item(owner, item, exc, log_prefix=f"{stage_label}提交失败")

    def confirm_item(item: _RelayBatchItem) -> None:
        if item.failed or _relay_record_status(item) in done_statuses:
            return
        txid = _relay_record_txid(item, tx_attr)
        if not txid:
            _relay_fail_item(owner, item, f"{stage_label}未拿到 txid")
            return
        try:
            _wait_for_receipt_success(owner, params, txid, label=f"{item.prefix} {stage_label}")
            _relay_update_item_record(owner, item, done_status, last_error="")
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已确认")
        except Exception as exc:
            _relay_fail_item(owner, item, exc, log_prefix=f"{stage_label}确认失败")

    def run_group(group_items: list[_RelayBatchItem]) -> None:
        active_items = [item for item in group_items if not item.failed]
        if not active_items:
            return
        submit_interval = _source_stage_submit_interval(params)
        if _relay_mode_is(owner, params, "1m"):
            worker_count = max(
                1,
                min(
                    int(params.threads or 1),
                    _one_to_many_source_threads(params),
                    _nonce_window(params),
                    len(active_items),
                ),
            )

            def submit_with_delay(item: _RelayBatchItem) -> None:
                if owner.stop_requested.is_set():
                    raise RuntimeError("任务已停止")
                submit_item(item)
                if submit_interval > 0 and owner.stop_requested.wait(submit_interval):
                    raise RuntimeError("任务已停止")

            _relay_run_parallel(owner, active_items, worker_count, submit_with_delay)
            _relay_flush_record_writes(owner)
            return

        flush_every = max(1, min(int(params.threads or 1), 20))
        for pos, item in enumerate(active_items):
            if owner.stop_requested.is_set():
                raise RuntimeError("任务已停止")
            submit_item(item)
            if (pos + 1) % flush_every == 0:
                _relay_flush_record_writes(owner)
            if pos < len(active_items) - 1 and submit_interval > 0:
                if owner.stop_requested.wait(submit_interval):
                    raise RuntimeError("任务已停止")

    with _relay_defer_record_writes(owner):
        _relay_run_parallel(owner, list(source_groups.values()), min(params.threads, len(source_groups)), run_group)
        _relay_flush_record_writes(owner)
        confirm_candidates = [
            item
            for group_items in source_groups.values()
            for item in group_items
            if not item.failed and _relay_record_txid(item, tx_attr)
        ]
        _relay_run_parallel(owner, confirm_candidates, params.threads, confirm_item)
        _relay_flush_record_writes(owner)


def _relay_simple_stage(
    owner,
    params: WithdrawRuntimeParams,
    items: list[_RelayBatchItem],
    *,
    stage_label: str,
    tx_attr: str,
    nonce_attr: str,
    submitted_status: str,
    done_status: str,
    done_statuses: set[str],
    submit_one,
    status_text: str,
    confirm_receipt: bool = True,
) -> None:
    # 已标记完成的历史记录也必须重新核验第二跳回执。仅凭本地 status 跳过会让
    # “中转钱包为空”等错误恢复状态被当成真实转账成功。
    stage_items = [item for item in items if not item.failed]
    if not stage_items:
        return

    def submit_item(item: _RelayBatchItem) -> None:
        if item.failed or _relay_record_status(item) in done_statuses:
            return
        txid = _relay_record_txid(item, tx_attr)
        if txid:
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已提交")
            return
        try:
            stored_nonce = _tx_nonce_value(_relay_record_txid(item, nonce_attr))
            nonce = stored_nonce if stored_nonce is not None else owner.client.get_nonce(params.network, item.relay_addr)
            txid = submit_one(item, nonce)
            nonce = getattr(item, "_last_submitted_nonce", None) if getattr(item, "_last_submitted_nonce", None) is not None else nonce
            item.gas_fee_wei += int(getattr(item, "_last_forward_gas_fee_wei", 0) or 0)
            _relay_update_item_record(
                owner,
                item,
                submitted_status,
                **{tx_attr: txid, nonce_attr: _tx_nonce_text(nonce), "last_error": ""},
            )
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已提交")
            owner._dispatch_ui(lambda m=f"{item.prefix} {stage_label}已提交：txid={txid}": owner.log(m))
        except Exception as exc:
            from onchain_transaction_recovery import UnconfirmedTransactionError
            if isinstance(exc, UnconfirmedTransactionError) and exc.own_transaction:
                _relay_update_item_record(owner, item, submitted_status,
                                          **{tx_attr: exc.tx_hash, nonce_attr: _tx_nonce_text(exc.nonce), "last_error": str(exc)})
            _relay_fail_item(owner, item, exc, log_prefix=f"{stage_label}提交失败")

    def confirm_item(item: _RelayBatchItem) -> None:
        if item.failed:
            return
        txid = _relay_record_txid(item, tx_attr)
        if not txid:
            _relay_fail_item(owner, item, f"{stage_label}未拿到 txid")
            return
        try:
            if confirm_receipt:
                _wait_for_receipt_success(owner, params, txid, label=f"{item.prefix} {stage_label}")
            _relay_update_item_record(owner, item, done_status, last_error="")
            _relay_dispatch_row_status(owner, item, "running", f"{status_text}已确认" if confirm_receipt else f"{status_text}已提交")
        except Exception as exc:
            _relay_fail_item(owner, item, exc, log_prefix=f"{stage_label}确认失败")

    with _relay_defer_record_writes(owner):
        _relay_run_parallel(owner, stage_items, params.threads, submit_item)
        _relay_flush_record_writes(owner)
        if confirm_receipt:
            _relay_run_parallel(owner, [item for item in stage_items if not item.failed], params.threads, confirm_item)
            _relay_flush_record_writes(owner)


def _relay_prepare_items(
    owner,
    jobs_data: list[tuple[str, str, str]],
    params: WithdrawRuntimeParams,
    row_number_by_key: dict[str, int],
    relay_records_by_row_key: dict[str, RelayWalletRecord],
    row_source_wallets: dict[str, tuple[str, str]],
) -> list[_RelayBatchItem]:
    is_many_to_one_mode = bool(_relay_mode_is(owner, params, "m1"))
    items: list[_RelayBatchItem] = []
    for index, (row_key, source, target) in enumerate(jobs_data, start=1):
        record = relay_records_by_row_key[row_key]
        source_private_key, source_addr = row_source_wallets[row_key]
        relay_private_key = owner.client.credential_to_private_key(record.private_key)
        prefix = f"{owner._row_log_prefix(row_key, index, len(jobs_data), row_number_by_key)}[{owner._mask(record.relay_address, head=8, tail=6)}]"
        relay_name = f"B{index}"
        target_name = f"C{index}"
        source_label = _relay_resolved_source_label(owner, source_addr, source)
        reserve_dec = _record_relay_fee_reserve_decimal(record, params)
        reserve_units = owner._amount_to_units(reserve_dec, 18)
        amount_text = ""
        value_units = 0
        source_fee_gas_price = 0
        source_fee_gas_limit = owner.client.NATIVE_GAS_LIMIT
        source_token_gas_price = 0
        source_token_gas_limit = owner.client.NATIVE_GAS_LIMIT

        if params.token_is_native and is_many_to_one_mode:
            reserve_units = 0
        elif params.token_is_native:
            value_units, amount_text = _resolve_record_amount_local(owner, record, params)
        else:
            value_units, amount_text = _resolve_record_amount_local(owner, record, params)

        item = _RelayBatchItem(
            index=index,
            row_key=row_key,
            source_raw=source,
            target=target,
            source_private_key=source_private_key,
            source_addr=source_addr,
            relay_record=record,
            relay_private_key=relay_private_key,
            prefix=prefix,
            relay_name=relay_name,
            target_name=target_name,
            source_label=source_label,
            amount_text=amount_text,
            value_units=value_units,
            reserve_units=reserve_units,
            source_fee_gas_price=source_fee_gas_price,
            source_fee_gas_limit=source_fee_gas_limit,
            source_token_gas_price=source_token_gas_price,
            source_token_gas_limit=source_token_gas_limit,
            final_amount_text=amount_text,
        )
        item.relay_record = owner.relay_wallet_store.update_record(
            item.relay_addr,
            batch_id=item.relay_record.batch_id,
            status=str(item.relay_record.status or "created") or "created",
            reuse_key=_relay_reuse_key(owner, params.network, source_addr, target, mode=params.mode),
            token_symbol=str(params.coin or "").strip().upper(),
            token_contract=str(params.token_contract or "").strip(),
            token_decimals=max(0, int(params.token_decimals)),
            token_is_native=bool(params.token_is_native),
            relay_fee_reserve=owner._decimal_to_text(reserve_dec),
            transfer_amount=amount_text,
            transfer_units=(str(value_units) if value_units > 0 else ""),
            sweep_target=(target if is_many_to_one_mode else source_addr),
            last_error="",
        )
        items.append(item)
    return items


def _relay_auto_sweep_incomplete(owner, item: _RelayBatchItem, target: str, exc: object) -> None:
    setattr(item, "_auto_sweep_pending", False)
    _relay_dispatch_recovery_status(owner, item, "incomplete", f"回收待处理：{exc}")
    try:
        _relay_update_item_record(owner, item, "completed", sweep_target=target, sweep_resolution="manual", last_error=str(exc))
    except Exception:
        pass
    owner._dispatch_ui(lambda m=f"{item.prefix} 中转原生币自动回收待处理：{exc}": owner.log(m))


def _relay_confirm_auto_sweep(owner, params: WithdrawRuntimeParams, item: _RelayBatchItem) -> None:
    if not bool(getattr(item, "_auto_sweep_pending", False)):
        return
    target = str(getattr(item, "_auto_sweep_target", "") or "").strip()
    txid = str(getattr(item, "_auto_sweep_txid", "") or "").strip()
    nonce = getattr(item, "_auto_sweep_nonce", None)
    sweep_value = max(0, int(getattr(item, "_auto_sweep_submitted_value_wei", 0) or 0))
    try:
        _wait_for_receipt_success(
            owner,
            params,
            txid,
            label=f"{item.prefix} 中转原生币回收",
        )
        _relay_update_item_record(
            owner,
            item,
            "completed",
            gas_sweep_txid=txid,
            gas_sweep_nonce=_tx_nonce_text(nonce),
            sweep_target=target,
            sweep_resolution="confirmed",
            last_error="",
        )
        setattr(item, "_auto_sweep_pending", False)
        setattr(item, "_auto_sweep_value_wei", sweep_value)
        recovered_text = owner._gas_fee_amount_text(params.network, sweep_value)
        _relay_dispatch_recovery_status(owner, item, "success", f"回收{recovered_text}")
        owner._dispatch_ui(lambda m=f"{item.prefix} 中转原生币回收成功：to={owner._mask(target, head=8, tail=6)}，金额={recovered_text}，txid={txid}": owner.log(m))
    except Exception as exc:
        _relay_auto_sweep_incomplete(owner, item, target, exc)


def _relay_submit_auto_sweep(
    owner,
    params: WithdrawRuntimeParams,
    item: _RelayBatchItem,
    target: str,
    *,
    confirm_receipt: bool = True,
) -> None:
    setattr(item, "_auto_sweep_value_wei", 0)
    setattr(item, "_auto_sweep_pending", False)
    try:
        native_balance = owner.client.get_balance_wei(params.network, item.relay_addr)
        gas_price = owner.client.get_gas_price_wei(params.network)
        gas_limit = owner.client.NATIVE_GAS_LIMIT
        sweep_value, gas_cost, _safety = _relay_sweep_plan(native_balance, gas_price, gas_limit)
        if sweep_value <= 0:
            _relay_update_item_record(owner, item, "completed", sweep_target=target, sweep_resolution="manual_empty", last_error="")
            _relay_dispatch_recovery_status(owner, item, "success", "无需回收")
            return
        nonce = owner.client.get_nonce(params.network, item.relay_addr)
        txid = _relay_submit_with_retry(
            owner,
            params,
            item,
            "中转原生币回收",
            lambda: owner.client.submit_native_transfer_reliably(
                network=params.network,
                private_key=item.relay_private_key,
                to_address=target,
                value_wei=sweep_value,
                nonce=nonce,
                gas_price_wei=gas_price,
                gas_limit=gas_limit,
                source_address=item.relay_addr,
                recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
            ),
        )
        item.gas_fee_wei += gas_cost
        _relay_update_item_record(
            owner,
            item,
            "completed",
            gas_sweep_txid=txid,
            gas_sweep_nonce=_tx_nonce_text(nonce),
            sweep_target=target,
            sweep_resolution="submitted",
            last_error="",
        )
        setattr(item, "_auto_sweep_target", target)
        setattr(item, "_auto_sweep_txid", txid)
        setattr(item, "_auto_sweep_nonce", nonce)
        setattr(item, "_auto_sweep_submitted_value_wei", sweep_value)
        setattr(item, "_auto_sweep_pending", True)
        _relay_dispatch_recovery_status(owner, item, "submitted", "回收已提交")
        owner._dispatch_ui(lambda m=f"{item.prefix} 中转原生币回收已提交，等待receipt确认：to={owner._mask(target, head=8, tail=6)}，txid={txid}": owner.log(m))
        if confirm_receipt:
            _relay_confirm_auto_sweep(owner, params, item)
    except Exception as exc:
        _relay_auto_sweep_incomplete(owner, item, target, exc)


def run_relay_batch(owner, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
    dispatch_ui = owner._dispatch_ui
    success = 0
    failed = 0
    relay_records_by_row_key: dict[str, RelayWalletRecord] = {}
    final_result_by_row_key: dict[str, str] = {}
    try:
        set_ui_batch_size(owner, params.threads)
        row_number_by_key = owner._row_number_by_key_snapshot()
        token_desc = owner._token_desc_from_params(params)
        progress_keys = owner._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
        context_by_row_key = {
            row_key: owner._row_context_for_values(row_key, source, target)
            for row_key, source, target in jobs_data
        }
        is_many_to_one_mode = bool(_relay_mode_is(owner, params, "m1"))
        dispatch_ui(lambda keys=progress_keys: owner._begin_progress("transfer", keys))
        dispatch_ui(
            lambda a=owner._token_amount_text(params.coin, Decimal("0")), g=("-" if dry_run else owner._estimated_gas_fee_text(params.network, 0)): owner._set_progress_metrics(
                amount_text=a,
                gas_text=g,
            ),
        )
        dispatch_ui(
            lambda: owner.log(
                  f"开始批量链上中转：mode={params.mode or owner._mode()}，任务={len(jobs_data)}，network={params.network}，"
                  f"coin={token_desc}，amount={params.amount}，delay={params.delay}，threads={params.threads}，"
                  f"1m_source_threads={params.one_to_many_source_threads}，"
                f"receipt_interval={params.receipt_poll_interval_seconds}，receipt_retries={params.receipt_retry_count}，"
                f"nonce_window={params.nonce_window}，dry_run={dry_run}"
            ),
        )
        for row_key, source, target in jobs_data:
            owner._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
            owner._mark_recovery_status_context(row_key, context_by_row_key.get(row_key, ""))
            dispatch_ui(lambda k=row_key: owner._set_recovery_status(k, "", ""))
            dispatch_ui(lambda k=row_key: owner._set_status(k, "waiting"))

        if dry_run:
            total_amount = Decimal("0")
            for index, (row_key, source, target) in enumerate(jobs_data, start=1):
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = owner._decimal_to_text(owner._random_decimal_between(params.random_min, params.random_max, params.token_decimals))
                else:
                    amount_text = params.amount
                try:
                    total_amount += Decimal(str(amount_text))
                except Exception:
                    pass
                prefix = owner._row_log_prefix(row_key, index, len(jobs_data), row_number_by_key)
                dispatch_ui(lambda k=row_key, t=_relay_success_status_text(owner, params, amount_text): owner._set_status(k, "success", t))
                dispatch_ui(lambda k=row_key: owner._set_recovery_status(k, "success", "模拟完成"))
                dispatch_ui(lambda m=f"{prefix} 模拟中转成功：A({owner._mask_credential(source)}) -> B{index}(中转钱包) -> C{index}({owner._mask(target, head=8, tail=6)})，金额={params.coin} {amount_text}": owner.log(m))
            success = len(jobs_data)
            summary = (
                f"链上中转任务结束：总量 {len(jobs_data)}，成功 {success}，失败 0，"
                f"余额总数={owner._token_amount_text(params.coin, total_amount)}，"
                f"转账总数={owner._token_amount_text(params.coin, total_amount)}，"
                "回收总数=-，预估gas=-"
            )
            dispatch_ui(
                lambda a=owner._token_amount_text(params.coin, total_amount): owner._set_progress_metrics(
                    balance_text=a,
                    amount_text=a,
                    recovery_text="-",
                    gas_text="-",
                )
            )
            dispatch_ui(lambda: owner.log(summary))
            dispatch_ui(lambda s=success: owner._finish_progress("transfer", s, 0))
            dispatch_ui(
                lambda s=success, detail=summary: owner._show_result_summary_dialog(
                    title="执行完成",
                    summary_title="链上中转批量转账完成",
                    success=s,
                    failed=0,
                    detail_text=detail,
                    action_buttons=[],
                )
            )
            return

        prepare_started_at = time.monotonic()
        stage_started_at = prepare_started_at
        dispatch_ui(lambda: owner.log(f"中转钱包准备开始：任务={len(jobs_data)}"))
        row_source_wallets = _resolve_job_source_wallets(owner, jobs_data)
        source_count = len({str(address).strip().lower() for _private_key, address in row_source_wallets.values() if str(address or "").strip()})
        dispatch_ui(
            lambda elapsed=_relay_elapsed_text(stage_started_at), count=source_count: owner.log(
                f"中转钱包准备：源钱包解析完成，源钱包={count}，耗时={elapsed}"
            )
        )
        stage_started_at = time.monotonic()
        existing_records = owner.relay_wallet_store.load_records()
        dispatch_ui(
            lambda elapsed=_relay_elapsed_text(stage_started_at), count=len(existing_records): owner.log(
                f"中转钱包准备：历史记录读取/解密完成，记录={count}，耗时={elapsed}"
            )
        )
        stage_started_at = time.monotonic()
        resume_candidates_by_pair = _build_resume_candidates(owner, existing_records, jobs_data, params, row_source_wallets)
        resume_candidate_count = sum(len(records) for records in resume_candidates_by_pair.values())
        dispatch_ui(
            lambda elapsed=_relay_elapsed_text(stage_started_at), count=resume_candidate_count: owner.log(
                f"中转钱包准备：复用候选匹配完成，候选={count}，耗时={elapsed}"
            )
        )
        stage_started_at = time.monotonic()
        with _relay_defer_record_writes(owner):
            relay_records_by_row_key, resumed_count, batch_id = _pick_or_create_relay_records(
                owner,
                jobs_data,
                params,
                row_source_wallets,
                resume_candidates_by_pair,
            )
            created_count = len([r for r in relay_records_by_row_key.values() if str(r.batch_id or "").strip() == str(batch_id or "").strip()])
            dispatch_ui(
                lambda elapsed=_relay_elapsed_text(stage_started_at), generated=created_count, resumed=resumed_count: owner.log(
                    f"中转钱包准备：复用/创建记录完成，复用/续跑={resumed}，新建={generated}，耗时={elapsed}"
                )
            )
            stage_started_at = time.monotonic()
            items = _relay_prepare_items(owner, jobs_data, params, row_number_by_key, relay_records_by_row_key, row_source_wallets)
            dispatch_ui(
                lambda elapsed=_relay_elapsed_text(stage_started_at), count=len(items): owner.log(
                    f"中转钱包准备：任务对象构建完成，任务={count}，耗时={elapsed}"
                )
            )
            stage_started_at = time.monotonic()
            _relay_flush_record_writes(owner)
            dispatch_ui(
                lambda elapsed=_relay_elapsed_text(stage_started_at): owner.log(
                    f"中转钱包准备：加密写回完成，耗时={elapsed}"
                )
            )
        dispatch_ui(
            lambda elapsed=_relay_elapsed_text(prepare_started_at), generated=created_count, resumed=resumed_count, bid=batch_id: owner.log(
                f"中转钱包准备完成：复用/续跑 {resumed} 个，新建 {generated} 个，批次号={bid}，总耗时={elapsed}"
            )
        )
        for item in items:
            dispatch_ui(lambda k=item.row_key: owner._set_status(k, "running", "中转准备完成"))

        if params.token_is_native and is_many_to_one_mode:
            def submit_m1_native_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                _relay_resolve_m1_native_transfer(owner, params, item)
                item.gas_fee_wei += item.source_token_gas_price * item.source_token_gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "源钱包原生币转入中转",
                    lambda: owner.client.submit_native_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        to_address=item.relay_addr,
                        value_wei=item.value_units,
                        nonce=nonce,
                        gas_price_wei=item.source_token_gas_price,
                        gas_limit=item.source_token_gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="源钱包原生币转入中转",
                tx_attr="token_funded_txid",
                nonce_attr="token_funded_nonce",
                submitted_status="token_funding_submitted",
                done_status="token_funded",
                done_statuses={"token_funded", "forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_m1_native_to_relay,
                status_text="原生币转入",
            )
        elif params.token_is_native:
            native_done_statuses = {"token_funded", "forwarded", "completed", "completed_pending_sweep"}
            if _relay_mode_is(owner, params, "1m"):
                _relay_clear_missing_source_txids(
                    owner,
                    params,
                    items,
                    tx_attr="token_funded_txid",
                    nonce_attr="token_funded_nonce",
                    done_status="token_funded",
                    done_statuses=native_done_statuses,
                )
                _relay_preflight_1m_native_source(
                    owner,
                    params,
                    items,
                    tx_attr="token_funded_txid",
                    done_statuses=native_done_statuses,
                )

            def submit_1m_native_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                _relay_resolve_1m_native_transfer(owner, params, item)
                total_units = item.value_units + item.reserve_units
                item.gas_fee_wei += item.source_token_gas_price * item.source_token_gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "源钱包原生币转入中转",
                    lambda: owner.client.submit_native_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        to_address=item.relay_addr,
                        value_wei=total_units,
                        nonce=nonce,
                        gas_price_wei=item.source_token_gas_price,
                        gas_limit=item.source_token_gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="源钱包原生币转入中转",
                tx_attr="token_funded_txid",
                nonce_attr="token_funded_nonce",
                submitted_status="token_funding_submitted",
                done_status="token_funded",
                done_statuses=native_done_statuses,
                submit_one=submit_1m_native_to_relay,
                status_text="原生币转入",
            )
        elif is_many_to_one_mode:
            def submit_m1_token_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                _relay_resolve_source_token_transfer(owner, params, item)
                item.gas_fee_wei += item.source_token_gas_price * item.source_token_gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "源钱包代币转入中转",
                    lambda: owner.client.submit_erc20_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        token_contract=params.token_contract,
                        to_address=item.relay_addr,
                        amount_units=item.value_units,
                        nonce=nonce,
                        gas_price_wei=item.source_token_gas_price,
                        gas_limit=item.source_token_gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="源钱包代币转入中转",
                tx_attr="token_funded_txid",
                nonce_attr="token_funded_nonce",
                submitted_status="token_funding_submitted",
                done_status="token_funded",
                done_statuses={"token_funded", "forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_m1_token_to_relay,
                status_text="代币转入",
            )

            def submit_m1_native_fee_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                source_keep_units = _relay_many_to_one_source_keep_units(owner, params, item.relay_record)
                gas_price = owner.client.get_gas_price_wei(params.network)
                gas_limit = owner.client.NATIVE_GAS_LIMIT
                balance_units = owner.client.get_balance_wei(params.network, item.source_addr)
                reserve_units = balance_units - source_keep_units - (gas_price * gas_limit)
                if reserve_units <= 0:
                    raise RuntimeError("源钱包可归集原生币不足，无法为中转钱包提供手续费")
                item.reserve_units = reserve_units
                item.source_fee_gas_price = gas_price
                item.source_fee_gas_limit = gas_limit
                item.gas_fee_wei += gas_price * gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "源钱包原生币转入中转",
                    lambda: owner.client.submit_native_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        to_address=item.relay_addr,
                        value_wei=reserve_units,
                        nonce=nonce,
                        gas_price_wei=gas_price,
                        gas_limit=gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="源钱包原生币转入中转",
                tx_attr="fee_funded_txid",
                nonce_attr="fee_funded_nonce",
                submitted_status="fee_funding_submitted",
                done_status="fee_funded",
                done_statuses={"fee_funded", "forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_m1_native_fee_to_relay,
                status_text="手续费转入",
            )
        else:
            def submit_1m_native_fee_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                _relay_resolve_1m_native_fee_transfer(owner, params, item)
                item.gas_fee_wei += item.source_fee_gas_price * item.source_fee_gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "中转手续费转入",
                    lambda: owner.client.submit_native_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        to_address=item.relay_addr,
                        value_wei=item.reserve_units,
                        nonce=nonce,
                        gas_price_wei=item.source_fee_gas_price,
                        gas_limit=item.source_fee_gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="中转手续费转入",
                tx_attr="fee_funded_txid",
                nonce_attr="fee_funded_nonce",
                submitted_status="fee_funding_submitted",
                done_status="fee_funded",
                done_statuses={"fee_funded", "token_funded", "forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_1m_native_fee_to_relay,
                status_text="手续费转入",
            )

            def submit_1m_token_to_relay(item: _RelayBatchItem, nonce: int) -> str:
                _relay_resolve_source_token_transfer(owner, params, item)
                item.gas_fee_wei += item.source_token_gas_price * item.source_token_gas_limit
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "源钱包代币转入中转",
                    lambda: owner.client.submit_erc20_transfer_reliably(
                        network=params.network,
                        private_key=item.source_private_key,
                        token_contract=params.token_contract,
                        to_address=item.relay_addr,
                        amount_units=item.value_units,
                        nonce=nonce,
                        gas_price_wei=item.source_token_gas_price,
                        gas_limit=item.source_token_gas_limit,
                        source_address=item.source_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_source_stage(
                owner,
                params,
                items,
                stage_label="源钱包代币转入中转",
                tx_attr="token_funded_txid",
                nonce_attr="token_funded_nonce",
                submitted_status="token_funding_submitted",
                done_status="token_funded",
                done_statuses={"token_funded", "forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_1m_token_to_relay,
                status_text="代币转入",
            )

        if params.token_is_native:
            def submit_native_forward(item: _RelayBatchItem, nonce: int) -> str:
                gas_price = owner.client.get_gas_price_wei(params.network)
                gas_limit = owner.client.NATIVE_GAS_LIMIT
                native_balance = owner.client.get_balance_wei(params.network, item.relay_addr)
                forward_units, gas_cost, _safety = _relay_sweep_plan(native_balance, gas_price, gas_limit)
                if forward_units <= 0:
                    raise RuntimeError("中转钱包原生币余额不足，无法完成第二跳转账")
                item.final_amount_text = owner._decimal_to_text(owner._units_to_amount(forward_units, params.token_decimals))
                setattr(item, "_last_forward_gas_fee_wei", gas_cost)
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "中转第二跳",
                    lambda: owner.client.submit_native_transfer_reliably(
                        network=params.network,
                        private_key=item.relay_private_key,
                        to_address=item.target,
                        value_wei=forward_units,
                        nonce=nonce,
                        gas_price_wei=gas_price,
                        gas_limit=gas_limit,
                        source_address=item.relay_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_simple_stage(
                owner,
                params,
                items,
                stage_label="中转第二跳",
                tx_attr="token_forward_txid",
                nonce_attr="token_forward_nonce",
                submitted_status="token_forward_submitted",
                done_status="forwarded",
                done_statuses={"forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_native_forward,
                status_text="第二跳",
            )
        else:
            def submit_token_forward(item: _RelayBatchItem, nonce: int) -> str:
                if item.value_units <= 0 or not str(item.amount_text or "").strip():
                    value_units, amount_text, _amount_changed = _resolve_record_amount(
                        owner,
                        item.relay_record,
                        params,
                        item.source_addr,
                        item.relay_addr,
                    )
                    _relay_update_item_transfer_amount(owner, item, amount_text, value_units)
                gas_price = owner.client.get_gas_price_wei(params.network)
                gas_limit = owner.client.estimate_erc20_transfer_gas(
                    params.network,
                    item.relay_addr,
                    params.token_contract,
                    item.target,
                    item.value_units,
                )
                setattr(item, "_last_forward_gas_fee_wei", gas_price * gas_limit)
                item.final_amount_text = item.amount_text
                return _relay_submit_with_retry(
                    owner,
                    params,
                    item,
                    "中转第二跳",
                    lambda: owner.client.submit_erc20_transfer_reliably(
                        network=params.network,
                        private_key=item.relay_private_key,
                        token_contract=params.token_contract,
                        to_address=item.target,
                        amount_units=item.value_units,
                        nonce=nonce,
                        gas_price_wei=gas_price,
                        gas_limit=gas_limit,
                        source_address=item.relay_addr,
                        recovery_timeout_seconds=_broadcast_recovery_timeout_seconds(params),
                    ),
                )

            _relay_simple_stage(
                owner,
                params,
                items,
                stage_label="中转第二跳",
                tx_attr="token_forward_txid",
                nonce_attr="token_forward_nonce",
                submitted_status="token_forward_submitted",
                done_status="forwarded",
                done_statuses={"forwarded", "completed", "completed_pending_sweep"},
                submit_one=submit_token_forward,
                status_text="第二跳",
            )

        total_amount = Decimal("0")
        total_gas_fee_wei = 0
        with _relay_defer_record_writes(owner):
            for item in items:
                if item.failed:
                    final_result_by_row_key[item.row_key] = "failed"
                    continue
                try:
                    _relay_update_item_record(
                        owner,
                        item,
                        "completed",
                        sweep_target=(item.target if is_many_to_one_mode else item.source_addr),
                        sweep_resolution="",
                        completed_at=_utc_now_text(),
                        last_error="",
                    )
                except Exception as exc:
                    _relay_fail_item(owner, item, exc, log_prefix="完成状态更新失败")
                    final_result_by_row_key[item.row_key] = "failed"
                    continue
                final_amount_text = item.final_amount_text or item.amount_text
                item.success_text = _relay_success_status_text(owner, params, final_amount_text)
                final_result_by_row_key[item.row_key] = "success"
                try:
                    total_amount += Decimal(str(final_amount_text))
                except Exception:
                    pass
                total_gas_fee_wei += item.gas_fee_wei
                _relay_dispatch_row_status(owner, item, "success", item.success_text)
                if params.token_is_native:
                    _relay_dispatch_recovery_status(
                        owner,
                        item,
                        "success",
                        _relay_native_main_recovery_status_text(owner, params, final_amount_text),
                    )
                owner._dispatch_ui(
                    lambda m=(
                        f"{item.prefix} 中转主流程完成：A({item.source_label}) -> "
                        f"{item.relay_name}({owner._mask(item.relay_addr, head=8, tail=6)}) -> "
                        f"{item.target_name}({owner._mask(item.target, head=8, tail=6)})，"
                        f"金额={params.coin} {final_amount_text}"
                    ): owner.log(m)
                )
            _relay_flush_record_writes(owner)

        if not params.token_is_native:
            sweep_items = [item for item in items if not item.failed and final_result_by_row_key.get(item.row_key) == "success"]
            def submit_sweep_item(item: _RelayBatchItem) -> None:
                target = item.target if is_many_to_one_mode else item.source_addr
                _relay_submit_auto_sweep(owner, params, item, target, confirm_receipt=False)
            with _relay_defer_record_writes(owner):
                _relay_run_parallel(owner, sweep_items, params.threads, submit_sweep_item)
                _relay_flush_record_writes(owner)
                sweep_confirm_items = [
                    item for item in sweep_items
                    if bool(getattr(item, "_auto_sweep_pending", False))
                ]
                _relay_run_parallel(owner, sweep_confirm_items, params.threads, lambda item: _relay_confirm_auto_sweep(owner, params, item))
                _relay_flush_record_writes(owner)
            total_gas_fee_wei = sum(item.gas_fee_wei for item in items if not item.failed)

        success = sum(1 for item in items if final_result_by_row_key.get(item.row_key) == "success")
        failed = len(jobs_data) - success
        failed_jobs = [job for job in jobs_data if final_result_by_row_key.get(job[0], "") != "success"]
        failed_export_records = [relay_records_by_row_key[row_key] for row_key, _source, _target in failed_jobs if row_key in relay_records_by_row_key]
        failed_export_path = _relay_write_failed_export_file(failed_export_records) if failed_export_records else ""
        transfer_totals: dict[tuple[str, int], int] = {}
        recovery_totals: dict[tuple[str, int], int] = {}
        native_symbol = _relay_native_symbol(owner, params.network)
        for item in items:
            if final_result_by_row_key.get(item.row_key) != "success":
                continue
            final_amount_text = item.final_amount_text or item.amount_text
            transfer_units = max(0, int(item.value_units or 0))
            if final_amount_text:
                try:
                    transfer_units = owner._amount_to_units(Decimal(str(final_amount_text)), params.token_decimals)
                except Exception:
                    pass
            _relay_asset_total_add(transfer_totals, params.coin, params.token_decimals, transfer_units)
            if not params.token_is_native:
                _relay_asset_total_add(
                    recovery_totals,
                    native_symbol,
                    18,
                    int(getattr(item, "_auto_sweep_value_wei", 0) or 0),
                )
        balance_totals = _relay_asset_totals_merge(transfer_totals, recovery_totals)
        balance_text = _relay_asset_totals_text(owner, balance_totals)
        transfer_text = _relay_asset_totals_text(owner, transfer_totals)
        recovery_text = _relay_asset_totals_text(owner, recovery_totals)
        gas_text = f"预估 {owner._gas_fee_amount_text(params.network, total_gas_fee_wei)}"
        summary = (
            f"链上中转任务结束：总量 {len(jobs_data)}，成功 {success}，失败 {failed}，"
            f"余额总数={balance_text}，"
            f"转账总数={transfer_text}，"
            f"回收总数={recovery_text}，"
            f"预估gas={owner._gas_fee_amount_text(params.network, total_gas_fee_wei)}"
        )
        if failed_export_path:
            summary = f"{summary}\n失败中转钱包导出：{failed_export_path}"
        dispatch_ui(
            lambda b=balance_text, a=transfer_text, r=recovery_text, g=gas_text: owner._set_progress_metrics(
                balance_text=b,
                amount_text=a,
                recovery_text=r,
                gas_text=g,
            )
        )
        dispatch_ui(lambda: owner.log(summary))
        dispatch_ui(
            lambda s=success, f=failed, detail=summary, retry_jobs=list(failed_jobs): owner._show_result_summary_dialog(
                title="执行完成",
                summary_title="链上中转批量转账完成",
                success=s,
                failed=f,
                detail_text=detail,
                action_buttons=[
                    ("失败重试", (lambda jobs=list(retry_jobs), p=params, d=dry_run: owner._launch_retry_jobs(jobs, p, d, confirm=False)), f > 0),
                ],
            )
        )
        dispatch_ui(lambda s=success, f=failed: owner._finish_progress("transfer", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"链上中转任务异常终止：{err_text}": owner.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        dispatch_ui(lambda s=success, f=max(1, failed): owner._finish_progress("transfer", s, f))
    finally:
        owner._clear_source_private_key_cache()
        owner.is_running = False
