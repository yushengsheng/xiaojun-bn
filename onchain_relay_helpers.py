#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import threading
import time
from decimal import Decimal

from onchain_rpc_runtime import (
    ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS,
    close_owner_thread_rpc_session,
    format_onchain_failure_status,
    is_transient_rpc_error,
    onchain_poll_interval_seconds,
    onchain_rpc_call,
    onchain_rpc_timeout_kwargs,
)
from app_defaults import (
    ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
    ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
    ONCHAIN_RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
)
from app_paths import RELAY_FAILED_EXPORT_FILE, RELAY_MANUAL_EXPORT_FILE, RELAY_SWEEP_LOG_FILE
from core_models import EvmToken, WithdrawRuntimeParams
from onchain_relay_wallets import RelayWalletRecord, _atomic_write_text

RELAY_CONFIRM_TIMEOUT_SECONDS = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_BALANCE_TIMEOUT_SECONDS = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS = ONCHAIN_RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS_DEFAULT
RELAY_POLL_INTERVAL_SECONDS = ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT
RELAY_SWEEP_SAFETY_GAS_MULTIPLIER = 1
RELAY_SWEEP_TERMINAL_RESOLUTIONS = {"manual_empty", "dust_left"}
RELAY_TRACKED_TOKEN_SYMBOLS = {"USDT", "USDC"}
RELAY_STABLE_TOKEN_SYMBOLS = {"USDT", "USDC"}
RELAY_BACKGROUND_SWEEP_START_DELAY_SECONDS = 3.0
RELAY_STABLE_TOKEN_DUST_THRESHOLD = Decimal("0.0001")
RELAY_BNB_DUST_THRESHOLD = Decimal("0.00001")
RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS = ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS
_RELAY_SWEEP_LOG_LOCK = threading.Lock()

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


def _relay_poll_interval_seconds(owner, configured_value: object | None = None) -> float:
    return onchain_poll_interval_seconds(owner, configured_value, default=RELAY_POLL_INTERVAL_SECONDS)


def _relay_rpc_timeout_kwargs(owner, poll_interval_seconds: float | None = None) -> dict[str, object]:
    return onchain_rpc_timeout_kwargs(owner, poll_interval_seconds, default=RELAY_POLL_INTERVAL_SECONDS)


def _relay_rpc_call(method, *args, timeout_kwargs: dict[str, object] | None = None):
    return onchain_rpc_call(method, *args, timeout_kwargs=timeout_kwargs)


def _close_owner_thread_rpc_session(owner) -> None:
    close_owner_thread_rpc_session(owner)


def _relay_is_transient_rpc_error(exc: Exception) -> bool:
    return is_transient_rpc_error(exc)


def _append_relay_sweep_log(event: str, **fields: object) -> None:
    payload = {
        "ts": _utc_now_text(),
        "event": str(event or "").strip() or "unknown",
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[str(key)] = value
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
    path = RELAY_SWEEP_LOG_FILE
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _RELAY_SWEEP_LOG_LOCK:
            with path.open("a", encoding="utf-8") as fp:
                fp.write(line)
    except Exception:
        pass


def _relay_success_status_text(owner, params: WithdrawRuntimeParams, amount_text: str, *, sweep_pending: bool = False) -> str:
    amount = str(amount_text or "").strip()
    coin_text = str(getattr(params, "coin", "") or "").strip().lower()
    if amount and coin_text:
        return f"✅{amount}{coin_text}"
    if amount:
        return f"✅{amount}"
    return "✅"


def _relay_record_transfer_status_text(record: RelayWalletRecord) -> str:
    amount = str(getattr(record, "transfer_amount", "") or "").strip()
    symbol = str(getattr(record, "token_symbol", "") or "").strip().lower()
    if amount and symbol:
        return f"✅{amount}{symbol}"
    if amount:
        return f"✅{amount}"
    return "✅"


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
    text = f"✅{owner._decimal_to_text(amount)}{symbol}" if symbol else f"✅{owner._decimal_to_text(amount)}"
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


def _token_balance_units(
    owner,
    params: WithdrawRuntimeParams,
    address: str,
    *,
    timeout_kwargs: dict[str, object] | None = None,
) -> int:
    if params.token_is_native:
        return _relay_rpc_call(owner.client.get_balance_wei, params.network, address, timeout_kwargs=timeout_kwargs)
    return _relay_rpc_call(
        owner.client.get_erc20_balance,
        params.network,
        params.token_contract,
        address,
        timeout_kwargs=timeout_kwargs,
    )


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
    *,
    timeout_kwargs: dict[str, object] | None = None,
) -> dict[str, tuple[EvmToken, int]]:
    snapshot: dict[str, tuple[EvmToken, int]] = {}
    for token in _relay_recovery_tokens(owner, network, record):
        if token.is_native:
            units = _relay_rpc_call(owner.client.get_balance_wei, network, relay_addr, timeout_kwargs=timeout_kwargs)
        else:
            units = _relay_rpc_call(
                owner.client.get_erc20_balance,
                network,
                token.contract,
                relay_addr,
                timeout_kwargs=timeout_kwargs,
            )
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
        parts.append(f"{owner._decimal_to_text(amount)} {token_symbol}({network})")
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
def _wait_for_transaction_success(
    owner,
    network: str,
    txid: str,
    *,
    label: str,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
) -> dict:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_CONFIRM_TIMEOUT_SECONDS))
    poll_interval = _relay_poll_interval_seconds(owner, poll_interval_seconds)
    deadline = time.time() + timeout_value
    last_error = ""
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        try:
            receipt = _relay_rpc_call(
                owner.client.get_transaction_receipt,
                network,
                txid,
                timeout_kwargs=_relay_rpc_timeout_kwargs(owner, poll_interval),
            )
            last_error = ""
        except Exception as exc:
            receipt = None
            last_error = str(exc)
        if receipt is not None:
            status_raw = receipt.get("status")
            status_ok = True
            if status_raw is not None:
                status_ok = owner.client._int_from_hex(status_raw) == 1
            if not status_ok:
                raise RuntimeError(f"{label}失败：交易已上链但执行状态异常，txid={txid}")
            return receipt
        if time.time() >= deadline:
            detail = f"，最后一次RPC错误：{last_error}" if last_error else ""
            raise RuntimeError(f"{label}超时：等待链上确认超过 {timeout_value:g} 秒，txid={txid}{detail}")
        if owner.stop_requested.wait(poll_interval):
            raise RuntimeError("任务已停止")


def _wait_for_token_balance_at_least(
    owner,
    params: WithdrawRuntimeParams,
    address: str,
    minimum_units: int,
    *,
    label: str,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
) -> int:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_BALANCE_TIMEOUT_SECONDS))
    poll_interval = _relay_poll_interval_seconds(owner, poll_interval_seconds)
    deadline = time.time() + timeout_value
    last_error = ""
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        try:
            current_units = _token_balance_units(
                owner,
                params,
                address,
                timeout_kwargs=_relay_rpc_timeout_kwargs(owner, poll_interval),
            )
            last_error = ""
        except Exception as exc:
            current_units = -1
            last_error = str(exc)
        if current_units >= int(minimum_units):
            return current_units
        if time.time() >= deadline:
            detail = f"，最后一次RPC错误：{last_error}" if last_error else ""
            raise RuntimeError(f"{label}超时：等待余额达到目标值超过 {timeout_value:g} 秒{detail}")
        if owner.stop_requested.wait(poll_interval):
            raise RuntimeError("任务已停止")


def _wait_for_native_balance_at_most(
    owner,
    network: str,
    address: str,
    maximum_units: int,
    *,
    label: str,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
) -> int:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_BALANCE_TIMEOUT_SECONDS))
    poll_interval = _relay_poll_interval_seconds(owner, poll_interval_seconds)
    deadline = time.time() + timeout_value
    last_error = ""
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        try:
            current_units = _relay_rpc_call(
                owner.client.get_balance_wei,
                network,
                address,
                timeout_kwargs=_relay_rpc_timeout_kwargs(owner, poll_interval),
            )
            last_error = ""
        except Exception as exc:
            current_units = int(maximum_units) + 1
            last_error = str(exc)
        if current_units <= int(maximum_units):
            return current_units
        if time.time() >= deadline:
            detail = f"，最后一次RPC错误：{last_error}" if last_error else ""
            raise RuntimeError(f"{label}超时：等待原生币余额降至目标值以下超过 {timeout_value:g} 秒{detail}")
        if owner.stop_requested.wait(poll_interval):
            raise RuntimeError("任务已停止")


def _relay_reserve_requirements(
    owner,
    params: WithdrawRuntimeParams,
    relay_addr: str,
    final_target_addr: str,
    amount_units: int,
) -> tuple[int, int, int]:
    reserve_dec = params.relay_fee_reserve or Decimal("0")
    reserve_units = owner._amount_to_units(reserve_dec, 18)
    forward_gas_price = owner.client.get_gas_price_wei(params.network)
    if params.token_is_native:
        forward_gas_limit = owner.client.NATIVE_GAS_LIMIT
    else:
        forward_gas_limit = owner.client.estimate_erc20_transfer_gas(
            params.network,
            relay_addr,
            params.token_contract,
            final_target_addr,
            amount_units,
        )
    sweep_gas_limit = owner.client.NATIVE_GAS_LIMIT if params.relay_sweep_enabled else 0
    minimum_reserve_units = (forward_gas_limit + sweep_gas_limit) * forward_gas_price * 2
    if reserve_units < minimum_reserve_units:
        reserve_text = owner._gas_fee_amount_text(params.network, reserve_units)
        minimum_text = owner._gas_fee_amount_text(params.network, minimum_reserve_units)
        raise RuntimeError(f"中转手续费预留不足：当前设置 {reserve_text}，最低建议 {minimum_text}")
    return reserve_units, forward_gas_limit, minimum_reserve_units


def _infer_native_transfer_value_from_chain(owner, network: str, txid: str) -> int:
    tx_hash = str(txid or "").strip()
    if not tx_hash:
        return 0
    try:
        tx = _relay_rpc_call(
            owner.client.get_transaction_by_hash,
            network,
            tx_hash,
            timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
        )
    except Exception:
        tx = None
    if not tx:
        return 0
    try:
        return int(owner.client._int_from_hex(tx.get("value")))
    except Exception:
        return 0


def _relay_many_to_one_source_keep_units(owner, params: WithdrawRuntimeParams, record: RelayWalletRecord) -> int:
    source_keep_dec = _record_relay_fee_reserve_decimal(record, params)
    return max(0, owner._amount_to_units(source_keep_dec, 18))


def _relay_many_to_one_reserve_requirements(
    owner,
    record: RelayWalletRecord,
    params: WithdrawRuntimeParams,
    *,
    source_addr: str,
    relay_addr: str,
    target: str,
    value_units: int,
    source_token_gas_price: int,
    source_token_gas_limit: int,
) -> tuple[int, int, int]:
    source_keep_units = _relay_many_to_one_source_keep_units(owner, params, record)
    source_fee_gas_price = owner.client.get_gas_price_wei(params.network)
    source_fee_gas_limit = owner.client.NATIVE_GAS_LIMIT
    relay_forward_gas_limit = owner.client.NATIVE_GAS_LIMIT if params.token_is_native else owner.client.estimate_erc20_transfer_gas(
        params.network,
        relay_addr,
        params.token_contract,
        target,
        value_units,
    )
    relay_forward_gas_price = owner.client.get_gas_price_wei(params.network)
    if params.token_is_native:
        reserve_units = _infer_native_transfer_value_from_chain(owner, params.network, record.fee_funded_txid)
        if reserve_units <= 0:
            reserve_units = relay_forward_gas_price * relay_forward_gas_limit
        return reserve_units, relay_forward_gas_limit, reserve_units
    reserve_units = _infer_native_transfer_value_from_chain(owner, params.network, record.fee_funded_txid)
    if reserve_units <= 0:
        source_native_balance_units = owner.client.get_balance_wei(params.network, source_addr)
        reserve_units = (
            source_native_balance_units
            - source_keep_units
            - (source_fee_gas_price * source_fee_gas_limit)
            - (source_token_gas_price * source_token_gas_limit)
        )
    minimum_reserve_units = relay_forward_gas_price * relay_forward_gas_limit
    if reserve_units <= 0:
        raise RuntimeError("源钱包可归集原生币不足，无法为中转钱包提供手续费")
    if reserve_units < minimum_reserve_units:
        reserve_text = owner._gas_fee_amount_text(params.network, reserve_units)
        minimum_text = owner._gas_fee_amount_text(params.network, minimum_reserve_units)
        raise RuntimeError(f"源钱包可归集原生币不足：当前可转 {reserve_text}，至少需要 {minimum_text}")
    return reserve_units, relay_forward_gas_limit, minimum_reserve_units


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


def _transaction_receipt_state(owner, network: str, txid: str) -> str:
    tx_hash = str(txid or "").strip()
    if not tx_hash:
        return "missing"
    try:
        receipt = _relay_rpc_call(
            owner.client.get_transaction_receipt,
            network,
            tx_hash,
            timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
        )
    except Exception:
        return "pending"
    if receipt is None:
        return "pending"
    status_raw = receipt.get("status")
    if status_raw is not None and owner.client._int_from_hex(status_raw) != 1:
        return "failed"
    return "success"


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
    candidates_by_pair: dict[tuple[str, str], list[RelayWalletRecord]] = {}
    for record in existing_records:
        if str(record.status or "").strip() == "completed":
            continue
        pair_key = _relay_job_key(record.source, record.target)
        if pair_key not in wanted_pairs:
            continue
        if not _record_matches_job(record, params, record.source, record.target):
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
    for row_key, _source, target in jobs_data:
        source_wallet = row_source_wallets.get(row_key)
        if source_wallet is None:
            raise RuntimeError("未找到源钱包映射")
        _source_private_key, source_addr = source_wallet
        pair_key = _relay_job_key(source_addr, target)
        candidates = resume_candidates_by_pair.get(pair_key) or []
        if candidates:
            relay_records_by_row_key[row_key] = candidates.pop()
            resumed_count += 1
        else:
            jobs_missing_relay.append((row_key, source_addr, target))
    batch_id = _relay_batch_id(len(jobs_data))
    token = EvmToken(
        symbol=params.coin,
        contract=params.token_contract,
        decimals=params.token_decimals,
        is_native=params.token_is_native,
    )
    relay_records = []
    is_many_to_one_mode = bool(hasattr(owner, "_is_mode_m1") and owner._is_mode_m1())
    if jobs_missing_relay:
        relay_wallets = owner.client.create_wallets(
            len(jobs_missing_relay),
            worker_threads=max(1, min(params.threads, len(jobs_missing_relay))),
        )
        for (row_key, source_addr, target), relay_wallet in zip(jobs_missing_relay, relay_wallets):
            sweep_target = target if is_many_to_one_mode else source_addr
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
            tx = _relay_rpc_call(
                owner.client.get_transaction_by_hash,
                record.network,
                tx_hash,
                timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
            )
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


def _resolve_many_to_one_native_amount(
    owner,
    record: RelayWalletRecord,
    params: WithdrawRuntimeParams,
    *,
    source_addr: str,
    reserve_units: int,
) -> tuple[int, str, bool]:
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
    source_keep_units = _relay_many_to_one_source_keep_units(owner, params, record)
    source_fee_gas_price = owner.client.get_gas_price_wei(params.network)
    source_fee_gas_limit = owner.client.NATIVE_GAS_LIMIT
    source_token_gas_price = owner.client.get_gas_price_wei(params.network)
    source_token_gas_limit = owner.client.NATIVE_GAS_LIMIT
    if params.random_enabled and params.random_min is not None and params.random_max is not None:
        amount_dec = owner._random_decimal_between(params.random_min, params.random_max, params.token_decimals)
        if amount_dec <= 0:
            raise RuntimeError("随机金额生成失败：结果必须大于 0")
        value_units = owner._amount_to_units(amount_dec, params.token_decimals)
        amount_text = owner._decimal_to_text(amount_dec)
    elif params.amount == owner.AMOUNT_ALL_LABEL:
        source_native_balance_units = owner.client.get_balance_wei(params.network, source_addr)
        value_units = (
            source_native_balance_units
            - source_keep_units
            - (source_fee_gas_price * source_fee_gas_limit)
            - (source_token_gas_price * source_token_gas_limit)
            - reserve_units
        )
        if value_units <= 0:
            raise RuntimeError("源钱包原生币余额不足，无法在保留预留手续费后完成多对1中转归集")
        amount_text = owner._decimal_to_text(owner._units_to_amount(value_units, params.token_decimals))
    else:
        amount_dec = Decimal(params.amount)
        if amount_dec <= 0:
            raise RuntimeError("转账数量必须大于 0")
        value_units = owner._amount_to_units(amount_dec, params.token_decimals)
        amount_text = owner._decimal_to_text(amount_dec)
    if value_units <= 0:
        raise RuntimeError("转账数量过小")
    source_native_balance_units = owner.client.get_balance_wei(params.network, source_addr)
    total_required_units = (
        source_keep_units
        + (source_fee_gas_price * source_fee_gas_limit)
        + (source_token_gas_price * source_token_gas_limit)
        + reserve_units
        + value_units
    )
    if source_native_balance_units < total_required_units:
        raise RuntimeError("源钱包原生币余额不足，无法完成多对1中转归集")
    return value_units, amount_text, True


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


def _reconcile_relay_record_stage(
    owner,
    record: RelayWalletRecord,
    params: WithdrawRuntimeParams,
    *,
    reserve_units: int,
    value_units: int,
    target: str,
) -> RelayWalletRecord:
    timeout_seconds = _confirm_timeout_seconds(params)
    sweep_timeout_seconds = _relay_sweep_timeout_seconds(timeout_seconds)
    current = record
    current_status = str(current.status or "").strip()
    relay_addr = current.relay_address
    fee_wait_params = WithdrawRuntimeParams(
        coin=owner._network_fee_symbol(params.network),
        amount="0",
        network=params.network,
        delay=0.0,
        threads=1,
        confirm_timeout_seconds=timeout_seconds,
        token_decimals=18,
        token_is_native=True,
    )

    if current.gas_sweep_txid:
        sweep_state = _transaction_receipt_state(owner, params.network, current.gas_sweep_txid)
        if sweep_state == "success":
            try:
                _wait_for_native_balance_at_most(
                    owner,
                    params.network,
                    relay_addr,
                    0,
                    label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包清空",
                    timeout_seconds=sweep_timeout_seconds,
                )
            except Exception:
                pass
            return _update_relay_record(owner, current, status="completed")
        if sweep_state == "pending" or current_status == "sweep_submitted":
            _wait_for_transaction_success(
                owner,
                params.network,
                current.gas_sweep_txid,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转手续费清空确认",
                timeout_seconds=sweep_timeout_seconds,
            )
            _wait_for_native_balance_at_most(
                owner,
                params.network,
                relay_addr,
                0,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包清空",
                timeout_seconds=sweep_timeout_seconds,
            )
            return _update_relay_record(owner, current, status="completed")

    if current.token_forward_txid:
        forward_state = _transaction_receipt_state(owner, params.network, current.token_forward_txid)
        if forward_state == "success":
            current = _update_relay_record(owner, current, status="forwarded")
            current_status = "forwarded"
        elif forward_state == "pending" or current_status == "token_forward_submitted":
            _wait_for_transaction_success(
                owner,
                params.network,
                current.token_forward_txid,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转第二跳确认",
                timeout_seconds=timeout_seconds,
            )
            current = _update_relay_record(owner, current, status="forwarded")
            current_status = "forwarded"

    if current.token_funded_txid:
        token_state = _transaction_receipt_state(owner, params.network, current.token_funded_txid)
        if token_state == "success":
            if params.token_is_native:
                _wait_for_token_balance_at_least(
                    owner,
                    fee_wait_params,
                    relay_addr,
                    reserve_units + value_units,
                    label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包原生币到账",
                    timeout_seconds=timeout_seconds,
                )
            else:
                _wait_for_token_balance_at_least(
                    owner,
                    params,
                    relay_addr,
                    value_units,
                    label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包代币到账",
                    timeout_seconds=timeout_seconds,
                )
            if current_status not in {"forwarded", "completed", "completed_pending_sweep"}:
                current = _update_relay_record(owner, current, status="token_funded")
                current_status = "token_funded"
        elif token_state == "pending" or current_status == "token_funding_submitted":
            _wait_for_transaction_success(
                owner,
                params.network,
                current.token_funded_txid,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转代币转入确认",
                timeout_seconds=timeout_seconds,
            )
            if params.token_is_native:
                _wait_for_token_balance_at_least(
                    owner,
                    fee_wait_params,
                    relay_addr,
                    reserve_units + value_units,
                    label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包原生币到账",
                    timeout_seconds=timeout_seconds,
                )
            else:
                _wait_for_token_balance_at_least(
                    owner,
                    params,
                    relay_addr,
                    value_units,
                    label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包代币到账",
                    timeout_seconds=timeout_seconds,
                )
            if current_status not in {"forwarded", "completed", "completed_pending_sweep"}:
                current = _update_relay_record(owner, current, status="token_funded")
                current_status = "token_funded"

    if current.fee_funded_txid:
        fee_state = _transaction_receipt_state(owner, params.network, current.fee_funded_txid)
        if fee_state == "success":
            _wait_for_token_balance_at_least(
                owner,
                fee_wait_params,
                relay_addr,
                reserve_units,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包手续费到账",
                timeout_seconds=timeout_seconds,
            )
            if current_status not in {"token_funded", "forwarded", "completed", "completed_pending_sweep"}:
                current = _update_relay_record(owner, current, status="fee_funded")
                current_status = "fee_funded"
        elif fee_state == "pending" or current_status == "fee_funding_submitted":
            _wait_for_transaction_success(
                owner,
                params.network,
                current.fee_funded_txid,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转手续费转入确认",
                timeout_seconds=timeout_seconds,
            )
            _wait_for_token_balance_at_least(
                owner,
                fee_wait_params,
                relay_addr,
                reserve_units,
                label=f"{owner._mask(relay_addr, head=8, tail=6)} 中转钱包手续费到账",
                timeout_seconds=timeout_seconds,
            )
            if current_status not in {"token_funded", "forwarded", "completed", "completed_pending_sweep"}:
                current = _update_relay_record(owner, current, status="fee_funded")

    relay_native_balance = owner.client.get_balance_wei(params.network, relay_addr)
    if current.status in {"completed_pending_sweep", "forwarded"} and relay_native_balance <= 0:
        current = _update_relay_record(owner, current, status="completed")
    return current


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


__all__ = [name for name in globals() if not name.startswith("__")]
