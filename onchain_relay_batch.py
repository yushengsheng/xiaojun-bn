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


from onchain_relay_recovery import _schedule_background_relay_sweep_recovery


def run_relay_batch(owner, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
    dispatch_ui = owner._dispatch_ui
    try:
        set_ui_batch_size(owner, params.threads)
        row_number_by_key = owner._row_number_by_key_snapshot()

        def job_prefix(index: int, row_key: str = "") -> str:
            return owner._row_log_prefix(row_key, index, len(jobs_data), row_number_by_key)

        token_desc = owner._token_desc_from_params(params)
        progress_keys = owner._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
        context_by_row_key = {
            row_key: owner._row_context_for_values(row_key, source, target)
            for row_key, source, target in jobs_data
        }
        is_many_to_one_mode = bool(hasattr(owner, "_is_mode_m1") and owner._is_mode_m1())
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
            fallback_prefixes[row_key] = job_prefix(len(fallback_prefixes) + 1, row_key)
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

        row_source_wallets: dict[str, tuple[str, str]] = {}
        relay_records_by_row_key: dict[str, RelayWalletRecord] = {}
        batch_scope_ids: set[str] = set()
        if not dry_run:
            row_source_wallets = _resolve_job_source_wallets(owner, jobs_data)
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
            resume_candidates_by_pair = _build_resume_candidates(existing_records, jobs_data, params, row_source_wallets)
            relay_records_by_row_key, resumed_count, batch_id = _pick_or_create_relay_records(
                owner,
                jobs_data,
                params,
                row_source_wallets,
                resume_candidates_by_pair,
            )
            relay_records = [
                record
                for record in relay_records_by_row_key.values()
                if str(record.batch_id or "").strip() == str(batch_id or "").strip()
            ]
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

        jobs_q: queue.Queue[tuple[int, str, str, str, int]] = queue.Queue()
        for i, item in enumerate(jobs_data, start=1):
            row_key, source, target = item
            jobs_q.put((i, row_key, source, target, 1))

        def worker():
            while True:
                if owner.stop_requested.is_set():
                    return
                try:
                    i, row_key, source, target, attempt_no = jobs_q.get_nowait()
                except queue.Empty:
                    return

                result_status = "failed"
                msg = ""
                amount_text = ""
                gas_fee_wei = 0
                success_text = ""
                warning = False
                retry_scheduled = False
                owner._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(lambda k=row_key: owner._set_status(k, "running"))
                prefix = job_prefix(i, row_key)
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
                        source_wallet = row_source_wallets.get(row_key)
                        if source_wallet is None:
                            raise RuntimeError("未找到源钱包映射")
                        source_private_key, source_addr = source_wallet
                        relay_addr = relay_record.relay_address
                        relay_private_key = owner.client.credential_to_private_key(relay_record.private_key)
                        prefix = f"{prefix}[{owner._mask(relay_addr, head=8, tail=6)}]"
                        relay_name = f"B{i}"
                        target_name = f"C{i}"
                        source_label = _relay_resolved_source_label(owner, source_addr, source)

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
                        if is_many_to_one_mode and params.token_is_native:
                            reserve_units, _forward_gas_limit_quote, _minimum_reserve_units = _relay_many_to_one_reserve_requirements(
                                owner,
                                relay_record,
                                params,
                                source_addr=source_addr,
                                relay_addr=relay_addr,
                                target=target,
                                value_units=0,
                                source_token_gas_price=owner.client.get_gas_price_wei(params.network),
                                source_token_gas_limit=owner.client.NATIVE_GAS_LIMIT,
                            )
                            value_units, amount_text, amount_changed = _resolve_many_to_one_native_amount(
                                owner,
                                relay_record,
                                params,
                                source_addr=source_addr,
                                reserve_units=reserve_units,
                            )
                        else:
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
                            confirm_poll_interval_seconds=params.confirm_poll_interval_seconds,
                            token_contract=params.token_contract,
                            token_decimals=params.token_decimals,
                            token_is_native=params.token_is_native,
                            relay_enabled=True,
                            relay_fee_reserve=reserve_dec,
                            relay_sweep_enabled=bool(relay_record.sweep_enabled),
                            relay_sweep_target=str(relay_record.sweep_target or params.relay_sweep_target or (target if is_many_to_one_mode else source_addr)),
                        )
                        if amount_changed or relay_record.relay_fee_reserve != owner._decimal_to_text(reserve_dec):
                            relay_record = update_record(
                                str(relay_record.status or "created") or "created",
                                relay_fee_reserve=owner._decimal_to_text(reserve_dec),
                                transfer_amount=amount_text,
                                transfer_units=str(value_units),
                            )
                        if is_many_to_one_mode and params.token_is_native:
                            source_token_gas_price = owner.client.get_gas_price_wei(params.network)
                            source_token_gas_limit = owner.client.NATIVE_GAS_LIMIT
                        else:
                            value_units, source_token_gas_price, source_token_gas_limit, amount_text = owner._resolve_amount_and_gas(
                                stage_params,
                                source_addr,
                                relay_addr,
                            )
                        if is_many_to_one_mode:
                            reserve_units, _forward_gas_limit_quote, _minimum_reserve_units = _relay_many_to_one_reserve_requirements(
                                owner,
                                relay_record,
                                stage_params,
                                source_addr=source_addr,
                                relay_addr=relay_addr,
                                target=target,
                                value_units=value_units,
                                source_token_gas_price=source_token_gas_price,
                                source_token_gas_limit=source_token_gas_limit,
                            )
                        else:
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
                        recovery_timeout_seconds = _broadcast_recovery_timeout_seconds(stage_params)
                        fee_wait_params = WithdrawRuntimeParams(
                            coin=owner._network_fee_symbol(params.network),
                            amount="0",
                            network=params.network,
                            delay=0.0,
                            threads=1,
                            confirm_timeout_seconds=confirm_timeout_seconds,
                            confirm_poll_interval_seconds=stage_params.confirm_poll_interval_seconds,
                            token_decimals=18,
                            token_is_native=True,
                        )
                        current_status = str(relay_record.status or "").strip()
                        if current_status in {"completed", "completed_pending_sweep"}:
                            success_text = _relay_success_status_text(owner, stage_params, amount_text)
                            msg = (
                                f"{prefix} 检测到中转任务已完成：A({source_label}) -> "
                                f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) -> "
                                f"{target_name}({owner._mask(target, head=8, tail=6)})，金额={stage_params.coin} {amount_text}"
                            )
                            result_status = "success"
                            continue
                        source_fee_gas_price = owner.client.get_gas_price_wei(params.network)
                        source_fee_gas_limit = owner.client.NATIVE_GAS_LIMIT
                        fee_stage_complete = current_status in {"fee_funded", "forwarded", "completed", "completed_pending_sweep"}
                        token_stage_complete = current_status in {"token_funded", "forwarded", "completed", "completed_pending_sweep"}
                        if is_many_to_one_mode and not stage_params.token_is_native:
                            fee_txid = str(relay_record.fee_funded_txid or "").strip()
                            token_txid = str(relay_record.token_funded_txid or "").strip()
                            if fee_txid and _transaction_receipt_state(owner, params.network, fee_txid) == "success":
                                fee_stage_complete = True
                            if token_txid and _transaction_receipt_state(owner, params.network, token_txid) == "success":
                                token_stage_complete = True
                        needs_fee_stage = not fee_stage_complete
                        needs_token_stage = not token_stage_complete
                        fee_stage_label = "中转原生币转入" if is_many_to_one_mode else "中转手续费转入"
                        if needs_fee_stage or needs_token_stage:
                            source_native_balance_units = owner.client.get_balance_wei(params.network, source_addr)
                            total_required_units = 0
                            if is_many_to_one_mode:
                                total_required_units += _relay_many_to_one_source_keep_units(owner, stage_params, relay_record)
                            if needs_fee_stage:
                                total_required_units += reserve_units + source_fee_gas_price * source_fee_gas_limit
                            if needs_token_stage:
                                total_required_units += source_token_gas_price * source_token_gas_limit
                                if stage_params.token_is_native:
                                    total_required_units += value_units
                            if source_native_balance_units < total_required_units:
                                raise RuntimeError("源钱包原生币余额不足，无法完成当前中转续跑阶段")

                        def run_fee_stage() -> None:
                            nonlocal gas_fee_wei, relay_record
                            fee_nonce = reuse_or_alloc_source_nonce(
                                source_addr,
                                relay_record.fee_funded_nonce,
                                stage_label=fee_stage_label,
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
                                    f"{prefix} A({source_label}) -> "
                                    f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) "
                                    f"{'原生币归集中转已提交' if is_many_to_one_mode else '手续费预留已提交'}："
                                    f"reserve={owner._gas_fee_amount_text(params.network, reserve_units)}，"
                                    f"txid={fee_submission.tx_hash}"
                                ): owner.log(m)
                            )
                            _wait_for_transaction_success(
                                owner,
                                params.network,
                                fee_submission.tx_hash,
                                label=f"{prefix} {fee_stage_label}确认",
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

                        def run_token_stage() -> None:
                            nonlocal gas_fee_wei, relay_record
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
                                    f"{prefix} A({source_label}) -> "
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

                        if is_many_to_one_mode and not stage_params.token_is_native:
                            if needs_token_stage:
                                run_token_stage()
                            if needs_fee_stage:
                                run_fee_stage()
                        else:
                            if needs_fee_stage:
                                run_fee_stage()
                            if needs_token_stage:
                                run_token_stage()

                        current_status = str(relay_record.status or "").strip()
                        if current_status not in {"forwarded", "completed", "completed_pending_sweep"}:
                            target_before_units = _token_balance_units(owner, stage_params, target)
                            relay_nonce = resolve_relay_stage_nonce(
                                relay_addr,
                                relay_record.token_forward_nonce,
                                stage_label="中转第二跳",
                            )
                            if stage_params.token_is_native:
                                relay_forward_gas_limit = owner.client.NATIVE_GAS_LIMIT
                                if is_many_to_one_mode and reserve_units > 0:
                                    relay_forward_gas_price = max(1, reserve_units // relay_forward_gas_limit)
                                else:
                                    relay_forward_gas_price = owner.client.get_gas_price_wei(params.network)
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
                                relay_forward_gas_price = owner.client.get_gas_price_wei(params.network)
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
                            sweep_target=(target if is_many_to_one_mode else source_addr),
                            sweep_resolution="",
                            last_error="",
                        )
                        success_text = _relay_success_status_text(owner, stage_params, amount_text)
                        msg = (
                            f"{prefix} 中转主流程完成："
                            f"A({source_label}) -> "
                            f"{relay_name}({owner._mask(relay_addr, head=8, tail=6)}) -> "
                            f"{target_name}({owner._mask(target, head=8, tail=6)})，"
                            f"金额={stage_params.coin} {amount_text}"
                        )
                        result_status = "success"
                except Exception as exc:
                    if (
                        _relay_is_transient_rpc_error(exc)
                        and attempt_no < RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS
                        and not owner.stop_requested.is_set()
                    ):
                        next_attempt = attempt_no + 1
                        retry_scheduled = True
                        retry_delay = _relay_poll_interval_seconds(owner, params.confirm_poll_interval_seconds)
                        dispatch_ui(
                            lambda m=(
                                f"{prefix} 链上中转遇到临时RPC错误，"
                                f"准备重试 {next_attempt}/{RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS}：{exc}"
                            ): owner.log(m)
                        )
                        if owner.stop_requested.wait(retry_delay):
                            retry_scheduled = False
                            msg = f"{prefix} 已停止"
                        else:
                            jobs_q.put((i, row_key, source, target, next_attempt))
                            continue
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
                    _close_owner_thread_rpc_session(owner)

                if retry_scheduled:
                    continue
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
        if not resolved_event.wait(max(0.2, _relay_poll_interval_seconds(owner, params.confirm_poll_interval_seconds))):
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
        owner._clear_source_private_key_cache()
        owner.is_running = False


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("__")]


__all__ = _build_module_star_exports()
