#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import queue
import threading
import time
from decimal import Decimal
from tkinter import messagebox

from app_paths import RELAY_FAILED_EXPORT_FILE, RELAY_MANUAL_EXPORT_FILE, RELAY_SWEEP_LOG_FILE
from core_models import EvmToken, WithdrawRuntimeParams
from onchain_relay_wallets import RelayWalletRecord, _atomic_write_text
from shared_utils import set_ui_batch_size

RELAY_CONFIRM_TIMEOUT_SECONDS = 180.0
RELAY_BALANCE_TIMEOUT_SECONDS = 180.0
RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS = 600.0
RELAY_POLL_INTERVAL_SECONDS = 2.0
RELAY_SWEEP_SAFETY_GAS_MULTIPLIER = 1
RELAY_SWEEP_TERMINAL_RESOLUTIONS = {"manual_empty", "dust_left"}
RELAY_TRACKED_TOKEN_SYMBOLS = {"USDT", "USDC"}
RELAY_BACKGROUND_SWEEP_START_DELAY_SECONDS = 3.0
_RELAY_SWEEP_LOG_LOCK = threading.Lock()


def _utc_now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _relay_sweep_timeout_seconds(base_timeout_seconds: float | None = None) -> float:
    try:
        base_value = float(base_timeout_seconds) if base_timeout_seconds is not None else 0.0
    except Exception:
        base_value = 0.0
    return max(RELAY_SWEEP_CONFIRM_TIMEOUT_SECONDS, max(1.0, base_value))


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
    if not hasattr(owner, "_is_mode_1m") or not owner._is_mode_1m():
        return ""
    target = str(getattr(record, "target", "") or "").strip()
    source = str(getattr(record, "source", "") or "").strip()
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


def _relay_recovery_row_context(owner, row_key: str, record: RelayWalletRecord) -> str:
    source = str(getattr(record, "source", "") or "").strip()
    target = str(getattr(record, "target", "") or "").strip()
    if str(row_key or "").strip().startswith("1m:"):
        try:
            source = str(owner.source_credential_var.get() or "").strip()
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


def _relay_recovery_tokens(owner, network: str) -> list[EvmToken]:
    tracked: list[EvmToken] = []
    seen_symbols: set[str] = set()
    for token in owner.client.get_default_tokens(network):
        symbol = str(token.symbol or "").strip().upper()
        if not symbol:
            continue
        if not token.is_native and symbol not in RELAY_TRACKED_TOKEN_SYMBOLS:
            continue
        if symbol in seen_symbols:
            continue
        tracked.append(token)
        seen_symbols.add(symbol)
    return tracked


def _relay_tracked_balance_snapshot(owner, network: str, relay_addr: str) -> dict[str, tuple[EvmToken, int]]:
    snapshot: dict[str, tuple[EvmToken, int]] = {}
    for token in _relay_recovery_tokens(owner, network):
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
        if symbol in RELAY_TRACKED_TOKEN_SYMBOLS and int(units) > 0
    }


def _relay_snapshot_has_positive_balance(snapshot: dict[str, tuple[EvmToken, int]]) -> bool:
    return any(int(units) > 0 for _token, units in snapshot.values())


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


def _ensure_relay_background_recovery_state(owner) -> threading.Lock:
    lock = getattr(owner, "_relay_sweep_bg_lock", None)
    if lock is None:
        lock = threading.Lock()
        setattr(owner, "_relay_sweep_bg_lock", lock)
    if not hasattr(owner, "_relay_sweep_bg_pending_batches"):
        setattr(owner, "_relay_sweep_bg_pending_batches", set())
    if not hasattr(owner, "_relay_sweep_bg_active"):
        setattr(owner, "_relay_sweep_bg_active", False)
    return lock


def _schedule_background_relay_sweep_recovery(
    owner,
    batch_ids: set[str],
    *,
    timeout_seconds: float | None = None,
) -> bool:
    pending_batches = {str(value or "").strip() for value in (batch_ids or set()) if str(value or "").strip()}
    if not pending_batches:
        return False
    lock = _ensure_relay_background_recovery_state(owner)
    with lock:
        owner._relay_sweep_bg_pending_batches.update(pending_batches)
        already_active = bool(getattr(owner, "_relay_sweep_bg_active", False))
        if not already_active:
            owner._relay_sweep_bg_active = True

    dispatch_ui = owner._dispatch_ui
    dispatch_ui(
        lambda: owner.log(
            f"已加入后台中转手续费回收队列：批次 {len(pending_batches)} 个，timeout={_relay_sweep_timeout_seconds(timeout_seconds):g}s"
        )
    )

    if already_active:
        return True

    def worker() -> None:
        try:
            while True:
                while bool(getattr(owner, "is_running", False)):
                    if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
                        return
                if owner.stop_requested.wait(RELAY_BACKGROUND_SWEEP_START_DELAY_SECONDS):
                    return
                while bool(getattr(owner, "is_running", False)):
                    if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
                        return
                with lock:
                    current_batches = set(getattr(owner, "_relay_sweep_bg_pending_batches", set()))
                    owner._relay_sweep_bg_pending_batches.clear()
                if not current_batches:
                    return
                _append_relay_sweep_log(
                    "background_queue_run",
                    batch_ids=sorted(current_batches),
                    timeout_seconds=_relay_sweep_timeout_seconds(timeout_seconds),
                )
                run_relay_fee_recovery(
                    owner,
                    False,
                    batch_ids=current_batches,
                    show_dialog=False,
                    use_progress=False,
                    manage_running_state=False,
                    run_label="background",
                    wait_pending_sweep=True,
                    sweep_timeout_seconds=timeout_seconds,
                )
                with lock:
                    if not getattr(owner, "_relay_sweep_bg_pending_batches", set()):
                        return
        finally:
            with lock:
                owner._relay_sweep_bg_active = False

    threading.Thread(
        target=worker,
        daemon=True,
        name="onchain-relay-sweep-background",
    ).start()
    return True


def _wait_for_transaction_success(owner, network: str, txid: str, *, label: str, timeout_seconds: float | None = None) -> dict:
    timeout_value = max(1.0, float(timeout_seconds if timeout_seconds is not None else RELAY_CONFIRM_TIMEOUT_SECONDS))
    deadline = time.time() + timeout_value
    while True:
        if owner.stop_requested.is_set():
            raise RuntimeError("任务已停止")
        receipt = owner.client.get_transaction_receipt(network, txid)
        if receipt is not None:
            status_raw = receipt.get("status")
            status_ok = True
            if status_raw is not None:
                status_ok = owner.client._int_from_hex(status_raw) == 1
            if not status_ok:
                raise RuntimeError(f"{label}失败：交易已上链但执行状态异常，txid={txid}")
            return receipt
        if time.time() >= deadline:
            raise RuntimeError(f"{label}超时：等待链上确认超过 {timeout_value:g} 秒，txid={txid}")
        if owner.stop_requested.wait(RELAY_POLL_INTERVAL_SECONDS):
            raise RuntimeError("任务已停止")


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
    receipt = owner.client.get_transaction_receipt(network, tx_hash)
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
    return record_contract == params_contract


def _record_relay_fee_reserve_decimal(record: RelayWalletRecord, params: WithdrawRuntimeParams) -> Decimal:
    raw = str(getattr(record, "relay_fee_reserve", "") or "").strip()
    if raw:
        try:
            value = Decimal(raw)
            if value > 0:
                return value
        except Exception:
            pass
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


def run_relay_fee_recovery(
    owner,
    dry_run: bool,
    *,
    batch_ids: set[str] | None = None,
    show_dialog: bool = True,
    use_progress: bool = True,
    manage_running_state: bool = True,
    run_label: str = "manual",
    wait_pending_sweep: bool = False,
    sweep_timeout_seconds: float | None = None,
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

        worker_count = max(1, min(owner._runtime_worker_threads(), len(jobs)))
        sweep_timeout_value = _relay_sweep_timeout_seconds(sweep_timeout_seconds)
        progress_keys = owner._unique_row_keys([progress_key for progress_key, _display_row_key, _record in jobs]) if use_progress else []
        if use_progress:
            dispatch_ui(lambda n=worker_count: set_ui_batch_size(owner, n))
            dispatch_ui(lambda keys=progress_keys: owner._begin_progress("recovery", keys))
            dispatch_ui(lambda: owner._set_progress_metrics(amount_text="-", gas_text="-"))
        dispatch_ui(lambda: owner.log(f"开始中转手续费回收：扫描 {len(jobs)} 条记录，dry_run={dry_run}，threads={worker_count}，label={run_label}"))
        _append_relay_sweep_log(
            "recovery_start",
            run_label=run_label,
            dry_run=bool(dry_run),
            worker_threads=worker_count,
            record_count=len(jobs),
            batch_ids=sorted(batch_id_filter),
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
        ) -> None:
            nonlocal handled, failed, recovered, skipped, already_empty, kept_margin, pending, warnings
            with lock:
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
                    sweep_target = owner._validate_recipient_address(record.sweep_target or record.source, "手续费回收地址")
                    snapshot = _relay_tracked_balance_snapshot(owner, network, relay_addr)
                    _native_token, native_balance = _relay_native_balance_entry(snapshot)
                    positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
                    positive_balance_text = _relay_positive_balance_text(owner, snapshot)
                    has_positive_balance = _relay_snapshot_has_positive_balance(snapshot)
                    current_status = str(record.status or "").strip()
                    sweep_resolution = str(record.sweep_resolution or "").strip().lower()

                    if not has_positive_balance:
                        if dry_run:
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome=("ignored" if current_status == "completed" else "already_empty"),
                                msg=f"{prefix} 妯拟核对完成：中转钱包无 BNB/USDT/USDC 余额",
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
                        )
                        continue

                    forward_state = _transaction_receipt_state(owner, network, record.token_forward_txid)
                    state_text = {
                        "missing": "缺少第二跳交易记录",
                        "pending": "第二跳交易仍在确认中",
                        "failed": "第二跳交易失败",
                    }.get(forward_state, f"状态 {forward_state}")

                    if positive_tracked_tokens:
                        if forward_state == "pending":
                            pending_reason = f"第二跳交易确认中，当前余额：{positive_balance_text}"
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 等待确认：当前仍有 {positive_balance_text}，第二跳交易尚未确认完成",
                                status_text="确认中",
                                network=network,
                                manual_tokens=positive_tracked_tokens,
                                export_row=_relay_build_manual_export_row(
                                    owner,
                                    record,
                                    snapshot,
                                    sweep_target=sweep_target,
                                    reason=pending_reason,
                                ),
                            )
                            continue
                        manual_reason = f"中转钱包仍有余额：{positive_balance_text}"
                        if not dry_run:
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
                            msg=f"{prefix} 待人工处理：检测到 {positive_balance_text}，已导出对应私钥",
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
                        )
                        continue

                    sweep_state = _transaction_receipt_state(owner, network, record.gas_sweep_txid)
                    if sweep_state == "pending":
                        if wait_pending_sweep and str(record.gas_sweep_txid or "").strip():
                            _append_relay_sweep_log(
                                "recovery_wait_pending",
                                run_label=run_label,
                                network=network,
                                relay_address=relay_addr,
                                target=str(record.target or "").strip(),
                                txid=str(record.gas_sweep_txid or "").strip(),
                                timeout_seconds=sweep_timeout_value,
                            )
                            try:
                                _wait_for_transaction_success(
                                    owner,
                                    network,
                                    str(record.gas_sweep_txid or "").strip(),
                                    label=f"{prefix} 中转手续费回收确认",
                                    timeout_seconds=sweep_timeout_value,
                                )
                            except Exception as exc:
                                if not dry_run:
                                    owner.relay_wallet_store.update_record(
                                        relay_addr,
                                        batch_id=record.batch_id,
                                        status="completed_pending_sweep",
                                        sweep_target=sweep_target,
                                        last_error=str(exc),
                                    )
                                _append_relay_sweep_log(
                                    "recovery_pending_timeout",
                                    run_label=run_label,
                                    network=network,
                                    relay_address=relay_addr,
                                    target=str(record.target or "").strip(),
                                    txid=str(record.gas_sweep_txid or "").strip(),
                                    error=str(exc),
                                )
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="pending",
                                    msg=f"{prefix} 等待确认：手续费回收交易仍未确认完成，后台稍后继续处理",
                                    status_text="确认中",
                                )
                                continue
                            native_balance = owner.client.get_balance_wei(network, relay_addr)
                            if native_balance <= 0:
                                _update_relay_record(
                                    owner,
                                    record,
                                    status="completed",
                                    sweep_target=sweep_target,
                                    last_error="",
                                )
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="already_empty",
                                    msg=f"{prefix} 后台确认完成：手续费回收交易已确认，中转钱包已清空",
                                    status_text="已清空",
                                )
                                continue
                            sweep_state = "success"
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
                        manual_reason = f"{state_text}，当前余额 {positive_balance_text}"
                        if not dry_run:
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
                            msg=f"{prefix} 待人工处理：{state_text}，中转钱包仍有 {positive_balance_text}",
                            status_text="待人工",
                            network=network,
                            export_row=_relay_build_manual_export_row(
                                owner,
                                record,
                                snapshot,
                                sweep_target=sweep_target,
                                reason=manual_reason,
                            ),
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
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="kept_margin",
                                msg=(
                                    f"{prefix} 模拟结果：余额 {owner._gas_fee_amount_text(network, native_balance)} "
                                    f"将保留为安全边际（约 {owner._gas_fee_amount_text(network, safety_units)}）"
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
                                target=str(record.target or "").strip(),
                                txid=sweep_txid,
                                error=str(exc),
                            )
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 手续费回收已提交，但确认较慢；后台稍后继续处理",
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
                            gas_fee=sweep_gas_cost,
                        )
                        continue

                    if last_sweep_error is not None:
                        raise last_sweep_error

                    native_balance = owner.client.get_balance_wei(network, relay_addr)
                    sweep_gas_price = owner.client.get_gas_price_wei(network)
                    _sweep_value, _sweep_gas_cost, safety_units = _relay_sweep_plan(
                        native_balance,
                        sweep_gas_price,
                        owner.client.NATIVE_GAS_LIMIT,
                        keep_safety=True,
                    )
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
                            f"{prefix} 暂不继续清空：当前余额 {owner._gas_fee_amount_text(network, native_balance)}，"
                            f"保留安全边际 {owner._gas_fee_amount_text(network, safety_units)}"
                        ),
                        status_text="已留边际",
                    )
                except Exception as exc:
                    try:
                        _update_relay_record(
                            owner,
                            record,
                            status=("completed_pending_sweep" if wait_pending_sweep else "failed"),
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
                        outcome=("pending" if wait_pending_sweep else "failed"),
                        msg=(
                            f"{prefix} 中转手续费回收待继续处理：{exc}"
                            if wait_pending_sweep
                            else f"{prefix} 中转手续费回收失败：{exc}"
                        ),
                        status_text=("确认中" if wait_pending_sweep else "失败"),
                    )
                finally:
                    jobs_q.task_done()

        workers: list[threading.Thread] = []
        for _ in range(worker_count):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        for t in workers:
            t.join()

        if owner.stop_requested.is_set():
            dispatch_ui(lambda: owner.log("中转手续费回收任务已停止"))

        summary = (
            f"中转手续费回收结束：已处理 {handled}，失败 {failed}，"
            f"已回收 {recovered}，已清空 {already_empty}，已留边际 {kept_margin}，跳过 {skipped}，确认中 {pending}，待手动 {warnings}"
        )
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
            export_path=export_path,
        )
        if show_dialog:
            dispatch_ui(
                lambda s=handled, f=failed, detail=summary: owner._show_result_summary_dialog(
                    title="执行完成",
                    summary_title="中转手续费回收完成",
                    success=s,
                    failed=f,
                    detail_text=detail,
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
        if manage_running_state:
            owner.is_running = False


def run_relay_batch(owner, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
    dispatch_ui = owner._dispatch_ui
    try:
        set_ui_batch_size(owner, params.threads)

        def job_prefix(index: int) -> str:
            return f"[{index}/{len(jobs_data)}]"

        token_desc = owner._token_desc_from_params(params)
        progress_keys = owner._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
        context_by_row_key = {
            row_key: owner._row_context_for_values(row_key, source, target)
            for row_key, source, target in jobs_data
        }
        dispatch_ui(lambda keys=progress_keys: owner._begin_progress("transfer", keys))
        dispatch_ui(
            lambda a=owner._token_amount_text(params.coin, Decimal("0")), g=("-" if dry_run else owner._estimated_gas_fee_text(params.network, 0)): owner._set_progress_metrics(
                amount_text=a,
                gas_text=g,
            ),
        )
        dispatch_ui(
            lambda: owner.log(
                f"开始批量链上中转：mode={owner._mode()}，任务={len(jobs_data)}，network={params.network}，"
                f"coin={token_desc}，amount={params.amount}，delay={params.delay}，threads={params.threads}，dry_run={dry_run}"
            ),
        )

        fallback_prefixes: dict[str, str] = {}
        for row_key, _source, _target in jobs_data:
            fallback_prefixes[row_key] = job_prefix(len(fallback_prefixes) + 1)
            owner._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
            owner._mark_recovery_status_context(row_key, context_by_row_key.get(row_key, ""))
            dispatch_ui(lambda k=row_key: owner._set_recovery_status(k, "", ""))
            dispatch_ui(lambda k=row_key: owner._set_status(k, "waiting"))

        success = 0
        failed = 0
        warnings = 0
        resolved = 0
        total_amount = Decimal("0")
        total_gas_fee_wei = 0
        lock = threading.Lock()
        resolved_event = threading.Event()
        resolved_row_keys: set[str] = set()
        final_result_by_row_key: dict[str, str] = {}
        nonce_lock_map: dict[str, threading.Lock] = {}
        nonce_next_map: dict[str, int] = {}
        nonce_guard = threading.Lock()

        def alloc_nonce(source_addr: str) -> int:
            with nonce_guard:
                source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
            with source_lock:
                nonce = nonce_next_map.get(source_addr)
                if nonce is None:
                    nonce = owner.client.get_nonce(params.network, source_addr)
                nonce_next_map[source_addr] = nonce + 1
                return nonce

        def rollback_nonce(source_addr: str, used_nonce: int):
            with nonce_guard:
                source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
            with source_lock:
                cached_next = nonce_next_map.get(source_addr)
                if cached_next == used_nonce + 1:
                    nonce_next_map.pop(source_addr, None)

        def reuse_or_alloc_source_nonce(source_addr: str, stored_nonce_text: str, *, stage_label: str) -> int:
            stored_nonce = _tx_nonce_value(stored_nonce_text)
            if stored_nonce is None:
                return alloc_nonce(source_addr)
            pending_nonce = owner.client.get_nonce(params.network, source_addr)
            if pending_nonce > stored_nonce:
                raise RuntimeError(
                    f"{stage_label}发现源钱包 nonce 已前进到 {pending_nonce}，历史 nonce={stored_nonce}，"
                    "无法安全自动续跑，请人工核对后处理"
                )
            with nonce_guard:
                source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
            with source_lock:
                cached_next = nonce_next_map.get(source_addr, pending_nonce)
                nonce_next_map[source_addr] = max(int(cached_next), stored_nonce + 1)
            return stored_nonce

        def resolve_relay_stage_nonce(relay_addr: str, stored_nonce_text: str, *, stage_label: str) -> int:
            stored_nonce = _tx_nonce_value(stored_nonce_text)
            pending_nonce = owner.client.get_nonce(params.network, relay_addr)
            if stored_nonce is None:
                return pending_nonce
            if pending_nonce == stored_nonce:
                return stored_nonce
            if pending_nonce < stored_nonce:
                raise RuntimeError(
                    f"{stage_label}发现中转钱包 nonce 回退到 {pending_nonce}，历史 nonce={stored_nonce}，"
                    "无法安全自动续跑，请人工核对后处理"
                )
            raise RuntimeError(
                f"{stage_label}发现中转钱包 nonce 已前进到 {pending_nonce}，历史 nonce={stored_nonce}，"
                "无法安全自动续跑，请人工核对后处理"
            )

        def finalize_job(
            row_key: str,
            result_status: str,
            msg: str,
            *,
            amount_text: str = "",
            gas_fee_wei: int = 0,
            success_text: str = "",
            warning: bool = False,
        ):
            nonlocal success, failed, warnings, resolved, total_amount, total_gas_fee_wei
            context_sig = context_by_row_key.get(row_key, "")
            row_status_text = ""
            with lock:
                if row_key in resolved_row_keys:
                    return
                resolved_row_keys.add(row_key)
                if result_status == "success":
                    success += 1
                    if warning:
                        warnings += 1
                    row_status_text = success_text or _relay_success_status_text(owner, params, amount_text, sweep_pending=warning)
                    if amount_text:
                        try:
                            total_amount += Decimal(amount_text)
                        except Exception:
                            pass
                else:
                    failed += 1
                final_result_by_row_key[row_key] = result_status
                total_gas_fee_wei += gas_fee_wei
                amount_total_text = owner._token_amount_text(params.coin, total_amount)
                gas_total_text = "-" if dry_run else owner._estimated_gas_fee_text(params.network, total_gas_fee_wei)
                resolved += 1
                done = resolved >= len(jobs_data)
            if done:
                resolved_event.set()
            dispatch_ui(lambda m=msg: owner.log(m))
            owner._mark_row_status_context(row_key, context_sig)
            dispatch_ui(lambda k=row_key, s=result_status, t=row_status_text: owner._set_status(k, s, t))
            dispatch_ui(lambda a=amount_total_text, g=gas_total_text: owner._set_progress_metrics(amount_text=a, gas_text=g))

        source_addr = ""
        source_private_key = ""
        relay_records_by_row_key: dict[str, RelayWalletRecord] = {}
        batch_scope_ids: set[str] = set()
        if not dry_run:
            source_private_key, source_addr = owner._resolve_wallet(jobs_data[0][1])
            cleanup_worker_threads = max(1, int(owner._runtime_worker_threads()))
            checked, removed, kept = owner.relay_wallet_store.cleanup_expired_empty_records(
                owner.client,
                log=lambda message: dispatch_ui(lambda m=message: owner.log(m)),
                worker_threads=cleanup_worker_threads,
            )
            if checked > 0:
                dispatch_ui(
                    lambda: owner.log(
                        f"中转钱包过期清理完成：检查 {checked}，移除 {removed}，保留 {kept}，线程数={cleanup_worker_threads}"
                    )
                )
            existing_records = owner.relay_wallet_store.load_records()
            resume_candidates_by_target: dict[str, list[RelayWalletRecord]] = {}
            for record in existing_records:
                if str(record.status or "").strip() == "completed":
                    continue
                if not _record_matches_job(record, params, source_addr, record.target):
                    continue
                resume_candidates_by_target.setdefault(str(record.target or "").strip().lower(), []).append(record)

            jobs_missing_relay: list[tuple[str, str]] = []
            resumed_count = 0
            for row_key, _source, target in jobs_data:
                target_key = str(target or "").strip().lower()
                candidates = resume_candidates_by_target.get(target_key) or []
                if candidates:
                    relay_records_by_row_key[row_key] = candidates.pop()
                    resumed_count += 1
                else:
                    jobs_missing_relay.append((row_key, target))

            batch_id = _relay_batch_id(len(jobs_data))
            token = EvmToken(
                symbol=params.coin,
                contract=params.token_contract,
                decimals=params.token_decimals,
                is_native=params.token_is_native,
            )
            relay_records = []
            if jobs_missing_relay:
                relay_wallets = owner.client.create_wallets(
                    len(jobs_missing_relay),
                    worker_threads=max(1, min(params.threads, len(jobs_missing_relay))),
                )
                for (row_key, target), relay_wallet in zip(jobs_missing_relay, relay_wallets):
                    relay_record = owner.relay_wallet_store.build_record(
                        batch_id=batch_id,
                        network=params.network,
                        source_address=source_addr,
                        target_address=target,
                        relay_wallet=relay_wallet,
                        token=token,
                        relay_fee_reserve=params.relay_fee_reserve,
                        sweep_enabled=False,
                        sweep_target=source_addr,
                    )
                    relay_records.append(relay_record)
                    relay_records_by_row_key[row_key] = relay_record
                owner.relay_wallet_store.append_records(relay_records)
            batch_scope_ids = {
                str(record.batch_id or "").strip()
                for record in relay_records_by_row_key.values()
                if str(getattr(record, "batch_id", "") or "").strip()
            }
            dispatch_ui(
                lambda generated=len(relay_records), resumed=resumed_count: owner.log(
                    f"中转钱包准备完成：续跑 {resumed} 个，新建 {generated} 个，批次号={batch_id}"
                )
            )

        jobs_q: queue.Queue[tuple[int, str, str, str]] = queue.Queue()
        for i, item in enumerate(jobs_data, start=1):
            row_key, source, target = item
            jobs_q.put((i, row_key, source, target))

        def worker():
            total = len(jobs_data)
            while True:
                if owner.stop_requested.is_set():
                    return
                try:
                    i, row_key, source, target = jobs_q.get_nowait()
                except queue.Empty:
                    return

                result_status = "failed"
                msg = ""
                amount_text = ""
                gas_fee_wei = 0
                success_text = ""
                warning = False
                owner._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(lambda k=row_key: owner._set_status(k, "running"))
                prefix = job_prefix(i)
                try:
                    if dry_run:
                        relay_label = f"B{i}"
                        target_label = f"C{i}"
                        if params.random_enabled and params.random_min is not None and params.random_max is not None:
                            amount_text = owner._decimal_to_text(
                                owner._random_decimal_between(params.random_min, params.random_max, params.token_decimals)
                            )
                        else:
                            amount_text = params.amount
                        success_text = _relay_success_status_text(owner, params, amount_text)
                        msg = (
                            f"{prefix} 模拟中转成功：A({owner._mask_credential(source)}) -> "
                            f"{relay_label}(新中转钱包) -> {target_label}({owner._mask(target, head=8, tail=6)})，"
                            f"金额={params.coin} {amount_text}"
                        )
                        result_status = "success"
                    else:
                        relay_record = relay_records_by_row_key.get(row_key)
                        if relay_record is None:
                            raise RuntimeError("未找到中转钱包记录")
                        relay_addr = relay_record.relay_address
                        relay_private_key = owner.client.credential_to_private_key(relay_record.private_key)
                        prefix = f"{prefix}[{owner._mask(relay_addr, head=8, tail=6)}]"
                        relay_name = f"B{i}"
                        target_name = f"C{i}"

                        def update_record(status: str, **changes: object):
                            if status == "failed":
                                changes.setdefault("last_error", str(changes.get("last_error") or ""))
                            else:
                                changes.setdefault("last_error", "")
                            if status in {"completed", "completed_pending_sweep"}:
                                changes.setdefault("completed_at", _utc_now_text())
                            updated = owner.relay_wallet_store.update_record(
                                relay_addr,
                                batch_id=relay_record.batch_id,
                                status=status,
                                **changes,
                            )
                            relay_records_by_row_key[row_key] = updated
                            return updated

                        reserve_dec = _record_relay_fee_reserve_decimal(relay_record, params)
                        value_units, amount_text, amount_changed = _resolve_record_amount(
                            owner,
                            relay_record,
                            params,
                            source_addr,
                            relay_addr,
                        )
                        stage_params = WithdrawRuntimeParams(
                            coin=params.coin,
                            amount=amount_text,
                            network=params.network,
                            delay=params.delay,
                            threads=params.threads,
                            confirm_timeout_seconds=params.confirm_timeout_seconds,
                            token_contract=params.token_contract,
                            token_decimals=params.token_decimals,
                            token_is_native=params.token_is_native,
                            relay_enabled=True,
                            relay_fee_reserve=reserve_dec,
                            relay_sweep_enabled=bool(relay_record.sweep_enabled),
                            relay_sweep_target=str(relay_record.sweep_target or params.relay_sweep_target or source_addr),
                        )
                        if amount_changed or relay_record.relay_fee_reserve != owner._decimal_to_text(reserve_dec):
                            relay_record = update_record(
                                str(relay_record.status or "created") or "created",
                                relay_fee_reserve=owner._decimal_to_text(reserve_dec),
                                transfer_amount=amount_text,
                                transfer_units=str(value_units),
                            )
                        value_units, source_token_gas_price, source_token_gas_limit, amount_text = owner._resolve_amount_and_gas(
                            stage_params,
                            source_addr,
                            relay_addr,
                        )
                        reserve_units, _forward_gas_limit_quote, _minimum_reserve_units = _relay_reserve_requirements(
                            owner,
                            stage_params,
                            relay_addr,
                            target,
                            value_units,
                        )
                        relay_record = _reconcile_relay_record_stage(
                            owner,
                            relay_record,
                            stage_params,
                            reserve_units=reserve_units,
                            value_units=value_units,
                            target=target,
                        )
                        confirm_timeout_seconds = _confirm_timeout_seconds(stage_params)
                        sweep_timeout_seconds = _relay_sweep_timeout_seconds(confirm_timeout_seconds)
                        recovery_timeout_seconds = _broadcast_recovery_timeout_seconds(stage_params)
                        fee_wait_params = WithdrawRuntimeParams(
                            coin=owner._network_fee_symbol(params.network),
                            amount="0",
                            network=params.network,
                            delay=0.0,
                            threads=1,
                            confirm_timeout_seconds=confirm_timeout_seconds,
                            token_decimals=18,
                            token_is_native=True,
                        )
                        current_status = str(relay_record.status or "").strip()
                        if current_status in {"completed", "completed_pending_sweep"}:
                            success_text = _relay_success_status_text(owner, stage_params, amount_text)
                            msg = (
                                f"{prefix} 检测到中转任务已完成：A({owner._mask(source_addr, head=8, tail=6)}) -> "
                                f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) -> "
                                f"{target_name}({owner._mask(target, head=8, tail=6)})，金额={stage_params.coin} {amount_text}"
                            )
                            result_status = "success"
                            continue
                        source_fee_gas_price = owner.client.get_gas_price_wei(params.network)
                        source_fee_gas_limit = owner.client.NATIVE_GAS_LIMIT
                        needs_fee_stage = current_status not in {"fee_funded", "token_funded", "forwarded", "completed", "completed_pending_sweep"}
                        needs_token_stage = current_status not in {"token_funded", "forwarded", "completed", "completed_pending_sweep"}
                        if needs_fee_stage or needs_token_stage:
                            source_native_balance_units = owner.client.get_balance_wei(params.network, source_addr)
                            total_required_units = 0
                            if needs_fee_stage:
                                total_required_units += reserve_units + source_fee_gas_price * source_fee_gas_limit
                            if needs_token_stage:
                                total_required_units += source_token_gas_price * source_token_gas_limit
                                if stage_params.token_is_native:
                                    total_required_units += value_units
                            if source_native_balance_units < total_required_units:
                                raise RuntimeError("源钱包原生币余额不足，无法完成当前中转续跑阶段")

                        if needs_fee_stage:
                            fee_nonce = reuse_or_alloc_source_nonce(
                                source_addr,
                                relay_record.fee_funded_nonce,
                                stage_label="中转手续费转入",
                            )
                            fee_submission = owner.client.submit_native_transfer_reliably(
                                network=params.network,
                                private_key=source_private_key,
                                to_address=relay_addr,
                                value_wei=reserve_units,
                                nonce=fee_nonce,
                                gas_price_wei=source_fee_gas_price,
                                gas_limit=source_fee_gas_limit,
                                source_address=source_addr,
                                recovery_timeout_seconds=recovery_timeout_seconds,
                            )
                            gas_fee_wei += source_fee_gas_price * source_fee_gas_limit
                            relay_record = update_record(
                                "fee_funding_submitted",
                                fee_funded_txid=fee_submission.tx_hash,
                                fee_funded_nonce=_tx_nonce_text(fee_nonce),
                            )
                            dispatch_ui(
                                lambda m=(
                                    f"{prefix} A({owner._mask(source_addr, head=8, tail=6)}) -> "
                                    f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) 手续费预留已提交："
                                    f"reserve={owner._gas_fee_amount_text(params.network, reserve_units)}，"
                                    f"txid={fee_submission.tx_hash}"
                                ): owner.log(m)
                            )
                            _wait_for_transaction_success(
                                owner,
                                params.network,
                                fee_submission.tx_hash,
                                label=f"{prefix} 中转手续费转入确认",
                                timeout_seconds=confirm_timeout_seconds,
                            )
                            _wait_for_token_balance_at_least(
                                owner,
                                fee_wait_params,
                                relay_addr,
                                reserve_units,
                                label=f"{prefix} 中转钱包手续费到账",
                                timeout_seconds=confirm_timeout_seconds,
                            )
                            relay_record = update_record("fee_funded")

                        if needs_token_stage:
                            token_nonce = reuse_or_alloc_source_nonce(
                                source_addr,
                                relay_record.token_funded_nonce,
                                stage_label="中转代币转入",
                            )
                            if stage_params.token_is_native:
                                token_submission = owner.client.submit_native_transfer_reliably(
                                    network=params.network,
                                    private_key=source_private_key,
                                    to_address=relay_addr,
                                    value_wei=value_units,
                                    nonce=token_nonce,
                                    gas_price_wei=source_token_gas_price,
                                    gas_limit=source_token_gas_limit,
                                    source_address=source_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            else:
                                token_submission = owner.client.submit_erc20_transfer_reliably(
                                    network=params.network,
                                    private_key=source_private_key,
                                    token_contract=params.token_contract,
                                    to_address=relay_addr,
                                    amount_units=value_units,
                                    nonce=token_nonce,
                                    gas_price_wei=source_token_gas_price,
                                    gas_limit=source_token_gas_limit,
                                    source_address=source_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            gas_fee_wei += source_token_gas_price * source_token_gas_limit
                            relay_record = update_record(
                                "token_funding_submitted",
                                token_funded_txid=token_submission.tx_hash,
                                token_funded_nonce=_tx_nonce_text(token_nonce),
                            )
                            dispatch_ui(
                                lambda m=(
                                    f"{prefix} A({owner._mask(source_addr, head=8, tail=6)}) -> "
                                    f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) 主转账已提交："
                                    f"金额={stage_params.coin} {amount_text}，txid={token_submission.tx_hash}"
                                ): owner.log(m)
                            )
                            _wait_for_transaction_success(
                                owner,
                                params.network,
                                token_submission.tx_hash,
                                label=f"{prefix} 中转代币转入确认",
                                timeout_seconds=confirm_timeout_seconds,
                            )
                            if stage_params.token_is_native:
                                _wait_for_token_balance_at_least(
                                    owner,
                                    fee_wait_params,
                                    relay_addr,
                                    reserve_units + value_units,
                                    label=f"{prefix} 中转钱包原生币到账",
                                    timeout_seconds=confirm_timeout_seconds,
                                )
                            else:
                                _wait_for_token_balance_at_least(
                                    owner,
                                    stage_params,
                                    relay_addr,
                                    value_units,
                                    label=f"{prefix} 中转钱包代币到账",
                                    timeout_seconds=confirm_timeout_seconds,
                                )
                            relay_record = update_record("token_funded")

                        current_status = str(relay_record.status or "").strip()
                        if current_status not in {"forwarded", "completed", "completed_pending_sweep"}:
                            target_before_units = _token_balance_units(owner, stage_params, target)
                            relay_nonce = resolve_relay_stage_nonce(
                                relay_addr,
                                relay_record.token_forward_nonce,
                                stage_label="中转第二跳",
                            )
                            relay_forward_gas_price = owner.client.get_gas_price_wei(params.network)
                            if stage_params.token_is_native:
                                relay_forward_gas_limit = owner.client.NATIVE_GAS_LIMIT
                                relay_native_balance = owner.client.get_balance_wei(params.network, relay_addr)
                                relay_forward_gas_cost = relay_forward_gas_price * relay_forward_gas_limit
                                if relay_native_balance < value_units + relay_forward_gas_cost:
                                    raise RuntimeError("中转钱包原生币余额不足，无法完成第二跳转账")
                                forward_submission = owner.client.submit_native_transfer_reliably(
                                    network=params.network,
                                    private_key=relay_private_key,
                                    to_address=target,
                                    value_wei=value_units,
                                    nonce=relay_nonce,
                                    gas_price_wei=relay_forward_gas_price,
                                    gas_limit=relay_forward_gas_limit,
                                    source_address=relay_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            else:
                                relay_forward_gas_limit = owner.client.estimate_erc20_transfer_gas(
                                    params.network,
                                    relay_addr,
                                    params.token_contract,
                                    target,
                                    value_units,
                                )
                                relay_forward_gas_cost = relay_forward_gas_price * relay_forward_gas_limit
                                relay_native_balance = owner.client.get_balance_wei(params.network, relay_addr)
                                if relay_native_balance < relay_forward_gas_cost:
                                    raise RuntimeError("中转钱包原生币余额不足，无法支付第二跳 gas")
                                relay_token_balance = owner.client.get_erc20_balance(params.network, params.token_contract, relay_addr)
                                if relay_token_balance < value_units:
                                    raise RuntimeError("中转钱包代币余额不足，无法完成第二跳转账")
                                forward_submission = owner.client.submit_erc20_transfer_reliably(
                                    network=params.network,
                                    private_key=relay_private_key,
                                    token_contract=params.token_contract,
                                    to_address=target,
                                    amount_units=value_units,
                                    nonce=relay_nonce,
                                    gas_price_wei=relay_forward_gas_price,
                                    gas_limit=relay_forward_gas_limit,
                                    source_address=relay_addr,
                                    recovery_timeout_seconds=recovery_timeout_seconds,
                                )
                            gas_fee_wei += relay_forward_gas_price * relay_forward_gas_limit
                            relay_record = update_record(
                                "token_forward_submitted",
                                token_forward_txid=forward_submission.tx_hash,
                                token_forward_nonce=_tx_nonce_text(relay_nonce),
                            )
                            dispatch_ui(
                                lambda m=(
                                    f"{prefix} {relay_name}({owner._mask(relay_addr, head=8, tail=6)}) -> "
                                    f"{target_name}({owner._mask(target, head=8, tail=6)}) 已提交："
                                    f"金额={stage_params.coin} {amount_text}，txid={forward_submission.tx_hash}"
                                ): owner.log(m)
                            )
                            _wait_for_transaction_success(
                                owner,
                                params.network,
                                forward_submission.tx_hash,
                                label=f"{prefix} 中转第二跳确认",
                                timeout_seconds=confirm_timeout_seconds,
                            )
                            _wait_for_token_balance_at_least(
                                owner,
                                stage_params,
                                target,
                                target_before_units + value_units,
                                label=f"{prefix} 目标地址到账",
                                timeout_seconds=confirm_timeout_seconds,
                            )
                            relay_record = update_record("forwarded")

                        relay_record = update_record(
                            "completed",
                            sweep_target=source_addr,
                            sweep_resolution="",
                            last_error="",
                        )
                        success_text = _relay_success_status_text(owner, stage_params, amount_text)
                        msg = (
                            f"{prefix} 中转主流程完成："
                            f"A({owner._mask(source_addr, head=8, tail=6)}) -> "
                            f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) -> "
                            f"{target_name}({owner._mask(target, head=8, tail=6)})，"
                            f"金额={stage_params.coin} {amount_text}"
                        )
                        result_status = "success"
                except Exception as exc:
                    relay_record = relay_records_by_row_key.get(row_key)
                    if relay_record is not None and not dry_run:
                        try:
                            owner.relay_wallet_store.update_record(
                                relay_record.relay_address,
                                batch_id=relay_record.batch_id,
                                status="failed",
                                last_error=str(exc),
                            )
                        except Exception:
                            pass
                    msg = f"{prefix} 中转失败：{exc}"
                finally:
                    jobs_q.task_done()

                finalize_job(
                    row_key,
                    result_status,
                    msg,
                    amount_text=amount_text,
                    gas_fee_wei=gas_fee_wei,
                    success_text=success_text,
                    warning=warning,
                )
                if params.delay > 0 and owner.stop_requested.wait(params.delay):
                    return

        workers: list[threading.Thread] = []
        worker_count = max(1, min(params.threads, len(jobs_data)))
        for _ in range(worker_count):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        for t in workers:
            t.join()
        if owner.stop_requested.is_set():
            with lock:
                pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
            for row_key in pending_row_keys:
                prefix = fallback_prefixes.get(row_key, "")
                stop_msg = f"{prefix} 已停止" if prefix else "任务已停止"
                finalize_job(row_key, "failed", stop_msg)
            resolved_event.set()
            dispatch_ui(lambda: owner.log("链上中转任务已停止"))
        if len(jobs_data) == 0:
            resolved_event.set()
        if not resolved_event.wait(max(0.2, RELAY_POLL_INTERVAL_SECONDS)):
            with lock:
                pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
            for row_key in pending_row_keys:
                prefix = fallback_prefixes.get(row_key, "")
                timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                finalize_job(row_key, "failed", timeout_msg)

        summary = f"链上中转任务结束：成功 {success}，失败 {failed}"
        batch_scope_ids = {
            str(record.batch_id or "").strip()
            for record in relay_records_by_row_key.values()
            if str(getattr(record, "batch_id", "") or "").strip()
        } or batch_scope_ids
        failed_jobs = [job for job in jobs_data if final_result_by_row_key.get(job[0], "") == "failed"]
        failed_export_records = [
            relay_records_by_row_key[row_key]
            for row_key, _source, _target in failed_jobs
            if row_key in relay_records_by_row_key
        ]
        failed_export_path = _relay_write_failed_export_file(failed_export_records) if not dry_run else ""
        if not dry_run:
            summary = (
                f"{summary}，转账总额={owner._token_amount_text(params.coin, total_amount)}，"
                f"预估gas合计={owner._gas_fee_amount_text(params.network, total_gas_fee_wei)}"
            )
        else:
            summary = f"{summary}，转账总额={owner._token_amount_text(params.coin, total_amount)}，预估gas合计=-"
        if batch_scope_ids:
            summary = f"{summary}，可回收批次={len(batch_scope_ids)}"
        if failed_export_path:
            summary = f"{summary}\n失败中转钱包导出：{failed_export_path}"
        dispatch_ui(lambda: owner.log(summary))
        dispatch_ui(
            lambda s=success, f=failed, detail=summary, batches=set(batch_scope_ids), retry_jobs=list(failed_jobs): owner._show_result_summary_dialog(
                title="执行完成",
                summary_title="链上中转批量转账完成",
                success=s,
                failed=f,
                detail_text=detail,
                action_buttons=[
                    ("中转手续费回收", (lambda b=set(batches): owner.start_batch_relay_fee_recovery(b)), bool(batches)),
                    ("失败重试", (lambda jobs=list(retry_jobs), p=params, d=dry_run: owner._launch_retry_jobs(jobs, p, d, confirm=False)), f > 0),
                ],
            )
        )
        dispatch_ui(lambda s=success, f=failed: owner._finish_progress("transfer", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"链上中转任务异常终止：{err_text}": owner.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        dispatch_ui(
            lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: owner._finish_progress("transfer", s, f),
        )
    finally:
        owner.is_running = False
