#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import queue
import threading
import time
from decimal import Decimal
from tkinter import messagebox

from core_models import EvmToken, WithdrawRuntimeParams
from onchain_relay_helpers import *
from onchain_relay_wallets import RelayWalletRecord
from shared_utils import set_ui_batch_size


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
                    if owner.stop_requested.wait(_relay_poll_interval_seconds(owner)):
                        return
                if owner.stop_requested.wait(RELAY_BACKGROUND_SWEEP_START_DELAY_SECONDS):
                    return
                while bool(getattr(owner, "is_running", False)):
                    if owner.stop_requested.wait(_relay_poll_interval_seconds(owner)):
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
        jobs_q: queue.Queue[tuple[int, str, str, RelayWalletRecord, int]] = queue.Queue()
        for index, (progress_key, display_row_key, record) in enumerate(jobs, start=1):
            jobs_q.put((index, progress_key, display_row_key, record, 1))

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
                    index, progress_key, display_row_key, record, attempt_no = jobs_q.get_nowait()
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
                    snapshot = _relay_tracked_balance_snapshot(
                        owner,
                        network,
                        relay_addr,
                        record,
                        timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
                    )
                    _native_token, native_balance = _relay_native_balance_entry(snapshot)
                    positive_tracked_tokens = _relay_positive_tracked_tokens(snapshot)
                    positive_balance_text = _relay_positive_balance_text(owner, snapshot)
                    has_positive_balance = _relay_snapshot_has_positive_balance(snapshot)
                    current_status = str(record.status or "").strip()
                    sweep_resolution = str(record.sweep_resolution or "").strip().lower()
                    auto_forward_gas_fee = 0

                    if _relay_is_terminal_dust_state(record, positive_tracked_tokens):
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="ignored",
                            msg=f"{prefix} 已核对：中转钱包已是终态，仅剩已标记的原生币边际",
                            status_text="已核对",
                        )
                        continue

                    if not has_positive_balance:
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
                            rescue_target = owner._validate_recipient_address(record.target, "收款地址")
                            dust_below_threshold, dust_detail = _relay_token_dust_below_threshold(owner, positive_tracked_tokens)
                            if dust_below_threshold:
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
                            snapshot = _relay_tracked_balance_snapshot(
                                owner,
                                network,
                                relay_addr,
                                record,
                                timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
                            )
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
                        if not bool(record.token_is_native):
                            if dry_run:
                                finish_job(
                                    progress_key,
                                    display_row_key,
                                    outcome="skipped",
                                    msg=f"{prefix} 模拟跳过：{state_text}，但中转钱包已无 USDT/USDC，正式运行会清理陈旧状态",
                                    status_text="已跳过",
                                )
                                continue
                            record = _update_relay_record(
                                owner,
                                record,
                                status="forwarded",
                                sweep_target=sweep_target,
                                last_error="",
                            )
                            forward_state = "success"
                            if native_balance > 0:
                                dispatch_ui(
                                    lambda m=(
                                        f"{prefix} 已清理陈旧第二跳状态：中转钱包无 USDT/USDC，"
                                        f"继续处理剩余 {owner._gas_fee_amount_text(network, native_balance)}"
                                    ): owner.log(m)
                                )
                            else:
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
                                    msg=f"{prefix} 已清理陈旧状态：中转钱包已无 USDT/USDC 余额",
                                    status_text="已清空",
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
                                rescue_target = owner._validate_recipient_address(record.target, "收款地址")
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
                                snapshot = _relay_tracked_balance_snapshot(
                                    owner,
                                    network,
                                    relay_addr,
                                    record,
                                    timeout_kwargs=_relay_rpc_timeout_kwargs(owner),
                                )
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
                            if owner.stop_requested.wait(_relay_poll_interval_seconds(owner)):
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
                            gas_fee=sweep_gas_cost + auto_forward_gas_fee,
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
                        )
                        continue
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
                    )
                except Exception as exc:
                    if (
                        _relay_is_transient_rpc_error(exc)
                        and attempt_no < RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS
                        and not owner.stop_requested.is_set()
                    ):
                        next_attempt = attempt_no + 1
                        retry_delay = _relay_poll_interval_seconds(owner)
                        dispatch_ui(
                            lambda m=(
                                f"{prefix} 中转手续费回收遇到临时RPC错误，"
                                f"准备重试 {next_attempt}/{RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS}：{exc}"
                            ): owner.log(m)
                        )
                        if owner.stop_requested.wait(retry_delay):
                            try:
                                _update_relay_record(
                                    owner,
                                    record,
                                    status=str(getattr(record, "status", "") or "created"),
                                    last_error="任务已停止",
                                )
                            except Exception:
                                pass
                            finish_job(
                                progress_key,
                                display_row_key,
                                outcome="pending",
                                msg=f"{prefix} 中转手续费回收已停止，待下次继续处理",
                                status_text="待重试",
                            )
                        else:
                            jobs_q.put((index, progress_key, display_row_key, record, next_attempt))
                        continue
                    if _relay_is_transient_rpc_error(exc):
                        try:
                            _update_relay_record(
                                owner,
                                record,
                                status=str(getattr(record, "status", "") or "created"),
                                last_error=str(exc),
                            )
                        except Exception:
                            pass
                        _append_relay_sweep_log(
                            "recovery_record_retry_pending",
                            run_label=run_label,
                            network=str(getattr(record, "network", "") or "").strip().upper(),
                            relay_address=str(getattr(record, "relay_address", "") or "").strip(),
                            target=str(getattr(record, "target", "") or "").strip(),
                            attempts=attempt_no,
                            error=str(exc),
                        )
                        finish_job(
                            progress_key,
                            display_row_key,
                            outcome="pending",
                            msg=(
                                f"{prefix} 中转手续费回收遇到临时RPC错误，"
                                f"已重试 {attempt_no}/{RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS}，待下次继续处理：{exc}"
                            ),
                            status_text="待重试",
                        )
                        continue
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
                    _close_owner_thread_rpc_session(owner)

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
        owner._clear_source_private_key_cache()
        if manage_running_state:
            owner.is_running = False


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("__")]


__all__ = _build_module_star_exports()
