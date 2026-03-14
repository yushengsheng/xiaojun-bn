#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from shared_utils import UI_BATCH_DELAY_MS, current_ui_batch_size


def _task_title(kind: str, finished: bool) -> str:
    if kind == "query":
        return "查询完成" if finished else "查询进度"
    if kind == "transfer":
        return "转账完成" if finished else "转账进度"
    if kind == "withdraw":
        return "提现完成" if finished else "提现进度"
    return "任务完成" if finished else "任务进度"


def unique_keys(keys: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in keys:
        key = str(raw or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def ensure_state(owner, *, amount_label: str = "总额") -> None:
    if not hasattr(owner, "_active_progress_kind"):
        owner._active_progress_kind = ""
    if not hasattr(owner, "_active_progress_keys"):
        owner._active_progress_keys = []
    if not hasattr(owner, "_progress_amount_label"):
        owner._progress_amount_label = amount_label
    if not hasattr(owner, "_summary_balance_text"):
        owner._summary_balance_text = "-"
    if not hasattr(owner, "_summary_amount_text"):
        owner._summary_amount_text = "-"
    if not hasattr(owner, "_summary_gas_text"):
        owner._summary_gas_text = "-"
    if not hasattr(owner, "_progress_refresh_pending"):
        owner._progress_refresh_pending = False
    if not hasattr(owner, "_progress_refresh_token"):
        owner._progress_refresh_token = None
    if not hasattr(owner, "_progress_refresh_requests"):
        owner._progress_refresh_requests = 0


def idle_text(amount_label: str = "总额") -> str:
    class Holder:
        pass

    owner = Holder()
    ensure_state(owner, amount_label=amount_label)
    owner._progress_amount_label = amount_label
    return _summary_suffix(owner, include_prefix=True)


def _summary_suffix(owner, *, include_prefix: bool = False) -> str:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    prefix = "进度：空闲 | " if include_prefix else ""
    return (
        f"{prefix}余额总额={owner._summary_balance_text} | "
        f"{owner._progress_amount_label}={owner._summary_amount_text} | "
        f"gas总额={owner._summary_gas_text}"
    )


def progress_counts(owner, kind: str, row_keys: list[str]) -> tuple[int, int, int, int, int]:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    store_fn = getattr(owner, "_progress_store", None)
    store = store_fn(kind) if callable(store_fn) else {}
    waiting = 0
    running = 0
    success = 0
    failed = 0
    submitted = 0
    for row_key in unique_keys(row_keys):
        status = str(store.get(row_key, "")).strip()
        if status == "waiting":
            waiting += 1
        elif status == "running":
            running += 1
        elif status == "success":
            success += 1
        elif status == "failed":
            failed += 1
        elif status == "submitted":
            submitted += 1
    return waiting, running, success, failed, submitted


def reset_metrics(owner, *, amount_label: str | None = None) -> None:
    ensure_state(owner, amount_label=amount_label or getattr(owner, "_progress_amount_label", "总额"))
    if amount_label:
        owner._progress_amount_label = amount_label
    owner._summary_balance_text = "-"
    owner._summary_amount_text = "-"
    owner._summary_gas_text = "-"
    _render_display_now(owner)


def set_metrics(
    owner,
    *,
    balance_text: str | None = None,
    amount_text: str | None = None,
    gas_text: str | None = None,
    amount_label: str | None = None,
) -> None:
    ensure_state(owner, amount_label=amount_label or getattr(owner, "_progress_amount_label", "总额"))
    if amount_label:
        owner._progress_amount_label = amount_label
    if balance_text is not None:
        owner._summary_balance_text = str(balance_text or "-")
    if amount_text is not None:
        owner._summary_amount_text = str(amount_text or "-")
    if gas_text is not None:
        owner._summary_gas_text = str(gas_text or "-")
    refresh_display(owner)


def begin(owner, kind: str, row_keys: list[str], *, amount_label: str) -> None:
    ensure_state(owner, amount_label=amount_label)
    owner._active_progress_kind = str(kind or "").strip()
    owner._active_progress_keys = unique_keys(row_keys)
    owner._progress_amount_label = amount_label
    owner._summary_balance_text = "-"
    owner._summary_amount_text = "-"
    owner._summary_gas_text = "-"
    _render_display_now(owner)


def _render_display(owner) -> None:
    if not hasattr(owner, "progress_var"):
        return
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    kind = str(getattr(owner, "_active_progress_kind", "")).strip()
    row_keys = list(getattr(owner, "_active_progress_keys", []))
    suffix = _summary_suffix(owner)
    if not kind or not row_keys:
        owner.progress_var.set(f"进度：空闲 | {suffix}")
        return
    waiting, running, success, failed, submitted = progress_counts(owner, kind, row_keys)
    done = success + failed
    submitted_segment = f" | 确认中{submitted}" if submitted > 0 else ""
    owner.progress_var.set(
        f"{_task_title(kind, False)}：{done}/{len(row_keys)} | "
        f"等待{waiting} | 进行中{running} | 成功{success} | 失败{failed}{submitted_segment} | {suffix}"
    )


def _run_scheduled_refresh(owner) -> None:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    owner._progress_refresh_pending = False
    owner._progress_refresh_token = None
    owner._progress_refresh_requests = 0
    _render_display(owner)


def _cancel_scheduled_refresh(owner) -> None:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    token = getattr(owner, "_progress_refresh_token", None)
    root = getattr(owner, "root", None)
    after_cancel = getattr(root, "after_cancel", None)
    if token is not None and callable(after_cancel):
        try:
            after_cancel(token)
        except Exception:
            pass
    owner._progress_refresh_pending = False
    owner._progress_refresh_token = None


def _render_display_now(owner) -> None:
    _cancel_scheduled_refresh(owner)
    owner._progress_refresh_requests = 0
    _render_display(owner)


def refresh_display(owner) -> None:
    if not hasattr(owner, "progress_var"):
        return
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    owner._progress_refresh_requests += 1
    if owner._progress_refresh_requests >= current_ui_batch_size(owner, default=1):
        _render_display_now(owner)
        return
    if getattr(owner, "_progress_refresh_pending", False):
        return
    root = getattr(owner, "root", None)
    after = getattr(root, "after", None)
    if callable(after):
        try:
            owner._progress_refresh_pending = True
            owner._progress_refresh_token = after(max(1, int(UI_BATCH_DELAY_MS)), lambda: _run_scheduled_refresh(owner))
            return
        except Exception:
            owner._progress_refresh_pending = False
            owner._progress_refresh_token = None
    _render_display(owner)


def refresh_if_active(owner, kind: str, row_key: str) -> None:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    if getattr(owner, "_active_progress_kind", "") != kind:
        return
    if row_key not in getattr(owner, "_active_progress_keys", []):
        return
    refresh_display(owner)


def finish(owner, kind: str, success: int, failed: int) -> None:
    ensure_state(owner, amount_label=getattr(owner, "_progress_amount_label", "总额"))
    if not hasattr(owner, "progress_var"):
        return
    _cancel_scheduled_refresh(owner)
    if getattr(owner, "_active_progress_kind", "") == kind:
        row_keys = list(getattr(owner, "_active_progress_keys", []))
        _waiting, _running, actual_success, actual_failed, actual_submitted = progress_counts(owner, kind, row_keys)
        if (actual_success + actual_failed + actual_submitted) > 0 or (success + failed) == 0:
            success = actual_success
            failed = actual_failed
            submitted = actual_submitted
        else:
            submitted = 0
        total = success + failed + submitted
        owner._active_progress_kind = ""
        owner._active_progress_keys = []
    else:
        total = success + failed
        submitted = 0
    suffix = _summary_suffix(owner)
    submitted_segment = f" | 确认中{submitted}" if submitted > 0 else ""
    owner.progress_var.set(f"{_task_title(kind, True)}：总{total} | 成功{success} | 失败{failed}{submitted_segment} | {suffix}")
