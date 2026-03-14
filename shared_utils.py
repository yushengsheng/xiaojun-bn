#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import random
import threading
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from tkinter import END, Scrollbar


LOG_MAX_ROWS = 500
UI_BATCH_DELAY_MS = 50


def parse_worker_threads(value, default: int = 2) -> int:
    try:
        return max(1, int(str(value).strip()))
    except Exception:
        return max(1, int(default))


def append_log_row(log_tree, text: str, max_rows: int = LOG_MAX_ROWS) -> None:
    append_log_rows(log_tree, [text], max_rows=max_rows)


def append_log_rows(log_tree, texts: list[str], max_rows: int = LOG_MAX_ROWS) -> None:
    rows_to_add = [str(text or "") for text in texts if text is not None]
    if not rows_to_add:
        return
    for text in rows_to_add:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_tree.insert("", END, values=(now, text))
    rows = list(log_tree.get_children())
    overflow = len(rows) - max_rows
    if overflow > 0:
        log_tree.delete(*rows[:overflow])
        rows = rows[overflow:]
    if rows:
        log_tree.see(rows[-1])


def current_ui_batch_size(owner, batch_size: int | None = None, default: int = 1) -> int:
    if batch_size is not None:
        return parse_worker_threads(batch_size, default=default)
    override = getattr(owner, "_ui_batch_size_override", None)
    if override is not None:
        return parse_worker_threads(override, default=default)
    resolver = getattr(owner, "_runtime_worker_threads", None)
    if callable(resolver):
        try:
            return parse_worker_threads(resolver(), default=default)
        except Exception:
            pass
    return parse_worker_threads(default, default=default)


def set_ui_batch_size(owner, batch_size: int) -> None:
    owner._ui_batch_size_override = parse_worker_threads(batch_size, default=1)


def clear_ui_batch_size(owner) -> None:
    if hasattr(owner, "_ui_batch_size_override"):
        owner._ui_batch_size_override = None


def _ensure_log_buffer_state(owner) -> None:
    if not hasattr(owner, "_log_buffer"):
        owner._log_buffer = []
    if not hasattr(owner, "_log_state_lock"):
        owner._log_state_lock = threading.Lock()
    if not hasattr(owner, "_log_flush_requested"):
        owner._log_flush_requested = False
    if not hasattr(owner, "_log_timer_requested"):
        owner._log_timer_requested = False


def flush_queued_log_rows(owner, log_tree, max_rows: int = LOG_MAX_ROWS) -> None:
    _ensure_log_buffer_state(owner)
    with owner._log_state_lock:
        pending = [str(text or "") for text in owner._log_buffer]
        owner._log_buffer = []
        owner._log_flush_requested = False
        owner._log_timer_requested = False
    if not pending:
        return
    append_log_rows(log_tree, pending, max_rows=max_rows)


def queue_log_row(
    owner,
    log_tree,
    text: str,
    *,
    root=None,
    max_rows: int = LOG_MAX_ROWS,
    batch_size: int | None = None,
    delay_ms: int = UI_BATCH_DELAY_MS,
) -> None:
    _ensure_log_buffer_state(owner)
    schedule_mode = ""
    with owner._log_state_lock:
        owner._log_buffer.append(str(text or ""))
        threshold = current_ui_batch_size(owner, batch_size=batch_size, default=1)
        if len(owner._log_buffer) >= threshold:
            if not owner._log_flush_requested:
                owner._log_flush_requested = True
                schedule_mode = "now"
        elif (not owner._log_flush_requested) and (not owner._log_timer_requested):
            owner._log_timer_requested = True
            schedule_mode = "later"
    if not schedule_mode:
        return
    root_widget = root if root is not None else getattr(owner, "root", None)
    after = getattr(root_widget, "after", None)
    if callable(after):
        try:
            after(
                0 if schedule_mode == "now" else max(1, int(delay_ms)),
                lambda o=owner, tree=log_tree, limit=max_rows: flush_queued_log_rows(o, tree, max_rows=limit),
            )
            return
        except Exception:
            pass
    flush_queued_log_rows(owner, log_tree, max_rows=max_rows)


def _ensure_ui_render_state(owner) -> None:
    if not hasattr(owner, "_ui_render_callbacks"):
        owner._ui_render_callbacks = []
    if not hasattr(owner, "_ui_render_state_lock"):
        owner._ui_render_state_lock = threading.Lock()
    if not hasattr(owner, "_ui_render_flush_requested"):
        owner._ui_render_flush_requested = False
    if not hasattr(owner, "_ui_render_timer_requested"):
        owner._ui_render_timer_requested = False


def flush_queued_ui_renders(owner) -> None:
    _ensure_ui_render_state(owner)
    with owner._ui_render_state_lock:
        callbacks = list(owner._ui_render_callbacks)
        owner._ui_render_callbacks = []
        owner._ui_render_flush_requested = False
        owner._ui_render_timer_requested = False
    for callback in callbacks:
        try:
            callback()
        except Exception:
            pass


def queue_ui_render(
    owner,
    callback,
    *,
    root=None,
    batch_size: int | None = None,
    delay_ms: int = UI_BATCH_DELAY_MS,
) -> None:
    _ensure_ui_render_state(owner)
    schedule_mode = ""
    with owner._ui_render_state_lock:
        owner._ui_render_callbacks.append(callback)
        threshold = current_ui_batch_size(owner, batch_size=batch_size, default=1)
        if len(owner._ui_render_callbacks) >= threshold:
            if not owner._ui_render_flush_requested:
                owner._ui_render_flush_requested = True
                schedule_mode = "now"
        elif (not owner._ui_render_flush_requested) and (not owner._ui_render_timer_requested):
            owner._ui_render_timer_requested = True
            schedule_mode = "later"
    if not schedule_mode:
        return
    root_widget = root if root is not None else getattr(owner, "root", None)
    after = getattr(root_widget, "after", None)
    if callable(after):
        try:
            after(
                0 if schedule_mode == "now" else max(1, int(delay_ms)),
                lambda o=owner: flush_queued_ui_renders(o),
            )
            return
        except Exception:
            pass
    flush_queued_ui_renders(owner)


def dispatch_ui_callback(owner, callback, *, root=None) -> None:
    queue_ui_render(owner, callback, root=root if root is not None else getattr(owner, "root", None))


def _ensure_ui_debounce_state(owner) -> None:
    if not hasattr(owner, "_ui_debounce_tokens"):
        owner._ui_debounce_tokens = {}
    if not hasattr(owner, "_ui_debounce_lock"):
        owner._ui_debounce_lock = threading.Lock()


def schedule_ui_callback(
    owner,
    key: str,
    callback,
    *,
    root=None,
    delay_ms: int = UI_BATCH_DELAY_MS,
) -> None:
    _ensure_ui_debounce_state(owner)
    callback_key = str(key or "").strip() or "_default"
    root_widget = root if root is not None else getattr(owner, "root", None)
    after = getattr(root_widget, "after", None)
    after_cancel = getattr(root_widget, "after_cancel", None)
    if not callable(after):
        callback()
        return
    with owner._ui_debounce_lock:
        prev_token = owner._ui_debounce_tokens.get(callback_key)
        if prev_token is not None and callable(after_cancel):
            try:
                after_cancel(prev_token)
            except Exception:
                pass

        def run():
            with owner._ui_debounce_lock:
                owner._ui_debounce_tokens.pop(callback_key, None)
            callback()

        try:
            token = after(max(1, int(delay_ms)), run)
        except Exception:
            owner._ui_debounce_tokens.pop(callback_key, None)
            callback()
            return
        owner._ui_debounce_tokens[callback_key] = token


def make_scrollbar(parent, orient, command):
    bar = Scrollbar(parent, orient=orient, command=command, width=14)
    for k, v in {
        "bg": "#bcbcbc",
        "activebackground": "#8f8f8f",
        "troughcolor": "#ececec",
        "relief": "raised",
        "bd": 1,
    }.items():
        try:
            bar.configure(**{k: v})
        except Exception:
            pass
    return bar


def handle_paste_shortcut(event):
    widget = getattr(event, "widget", None)
    if widget is None:
        return "break"
    try:
        widget.event_generate("<<Paste>>")
        return "break"
    except Exception:
        pass

    try:
        text = widget.clipboard_get()
    except Exception:
        return "break"

    try:
        try:
            start = widget.index("sel.first")
            end = widget.index("sel.last")
            widget.delete(start, end)
            insert_at = start
        except Exception:
            try:
                insert_at = widget.index("insert")
            except Exception:
                insert_at = END
        widget.insert(insert_at, text)
    except Exception:
        pass
    return "break"


def bind_paste_shortcuts(widget) -> None:
    for sequence in ("<Command-v>", "<Control-v>", "<Command-V>", "<Control-V>"):
        try:
            widget.bind(sequence, handle_paste_shortcut, add="+")
        except Exception:
            continue


def random_decimal_between(low: Decimal, high: Decimal, unit: Decimal = Decimal("0.01")) -> Decimal:
    low_i = int((low / unit).to_integral_value(rounding=ROUND_CEILING))
    high_i = int((high / unit).to_integral_value(rounding=ROUND_FLOOR))
    if low_i > high_i:
        raise RuntimeError("随机金额范围至少要包含 0.01")
    return (unit * Decimal(random.randint(low_i, high_i))).quantize(unit)


def decimal_to_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def mask_text(value: str, head: int = 6, tail: int = 4) -> str:
    if len(value) <= head + tail:
        return "*" * len(value)
    return f"{value[:head]}...{value[-tail:]}"
