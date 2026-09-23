#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import random
import re
import threading
import time
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal, localcontext
import tkinter as tk
from tkinter import END, Scrollbar

from app_defaults import LOG_MAX_ROWS_DEFAULT, UI_BATCH_DELAY_MS_DEFAULT
from sensitive_data import redact_sensitive_text


LOG_MAX_ROWS = LOG_MAX_ROWS_DEFAULT
UI_BATCH_DELAY_MS = UI_BATCH_DELAY_MS_DEFAULT
logger = logging.getLogger(__name__)
LOG_AMOUNT_TAG = "log_amount_value"
LOG_AMOUNT_ROW_TAG = "log_amount_row"
LOG_AMOUNT_COLOR = "#C2410C"
_LOG_NUMBER_RE = r"[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?"
_LOG_COIN_RE = r"(?:USDT|USDC|BUSD|BNB|BTC|ETH|SOL|TRX|XRP|DOGE|ADA|DOT|MATIC|POL|LTC)"
_LOG_AMOUNT_LABEL_RE = (
    r"(?:预买BNB金额|剩余bnb手续费|提现总额|余额总额|转账总额|预估gas合计|"
    r"总资产|可提余额|提现额度|下单金额|当前参考价|重挂阈值|"
    r"金额|余额|总额|预估gas|gas费|gas合计|手续费|均价|价格|旧价|新价|溢价|数量|"
    r"amount|balance|total|price|qty|quantity|fee)"
)
_LOG_AMOUNT_VALUE_PATTERNS = (
    re.compile(
        rf"{_LOG_AMOUNT_LABEL_RE}\s*(?:[=:：]|为|约)?\s*(?:{_LOG_COIN_RE}\s*)?"
        rf"(?P<value>{_LOG_NUMBER_RE})(?![\w.])",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?<![A-Z0-9]){_LOG_COIN_RE}(?![A-Z0-9])\s*"
        rf"(?:(?:[=:：])|(?:合计\s*[=:：])|约)?\s*(?P<value>{_LOG_NUMBER_RE})(?![\w.])",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?P<value>{_LOG_NUMBER_RE})\s*(?<![A-Z0-9]){_LOG_COIN_RE}(?![A-Z0-9])",
        re.IGNORECASE,
    ),
)


def log_amount_value_spans(text: str) -> list[tuple[int, int]]:
    message = str(text or "")
    if not message:
        return []
    spans: list[tuple[int, int]] = []
    for pattern in _LOG_AMOUNT_VALUE_PATTERNS:
        for match in pattern.finditer(message):
            start, end = match.span("value")
            if start >= 0 and end > start:
                spans.append((start, end))
    if not spans:
        return []
    merged: list[tuple[int, int]] = []
    for start, end in sorted(set(spans)):
        if not merged or start >= merged[-1][1]:
            merged.append((start, end))
        else:
            prev_start, prev_end = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end))
    return merged


def configure_log_amount_tag(text_widget) -> None:
    if bool(getattr(text_widget, "_log_amount_tag_configured", False)):
        return
    try:
        text_widget.tag_configure(LOG_AMOUNT_TAG, foreground=LOG_AMOUNT_COLOR)
        text_widget.tag_raise(LOG_AMOUNT_TAG)
        setattr(text_widget, "_log_amount_tag_configured", True)
    except Exception:
        pass


def configure_log_amount_row_tag(log_tree) -> None:
    if bool(getattr(log_tree, "_log_amount_row_tag_configured", False)):
        return
    try:
        log_tree.tag_configure(LOG_AMOUNT_ROW_TAG, foreground=LOG_AMOUNT_COLOR)
        setattr(log_tree, "_log_amount_row_tag_configured", True)
    except Exception:
        pass


def insert_log_message_with_amount_highlights(text_widget, message: str) -> None:
    text = str(message)
    start_index = text_widget.index("end-1c")
    text_widget.insert("end", text)
    for start, end in log_amount_value_spans(text):
        try:
            text_widget.tag_add(LOG_AMOUNT_TAG, f"{start_index}+{start}c", f"{start_index}+{end}c")
        except Exception:
            pass
    text_widget.insert("end", "\n")


def _widget_class(widget) -> str:
    widget_class_getter = getattr(widget, "winfo_class", None)
    return str(widget_class_getter()).strip() if callable(widget_class_getter) else ""


def _parse_hex_color(value: str) -> tuple[int, int, int] | None:
    text = str(value or "").strip()
    if not text.startswith("#"):
        return None
    digits = text[1:]
    if len(digits) == 3:
        digits = "".join(ch * 2 for ch in digits)
    if len(digits) != 6:
        return None
    try:
        return tuple(int(digits[i : i + 2], 16) for i in (0, 2, 4))
    except ValueError:
        return None


def _format_hex_color(rgb: tuple[int, int, int]) -> str:
    return "#" + "".join(f"{max(0, min(255, int(channel))):02X}" for channel in rgb)


def _blend_hex_color(source: str, target: str, ratio: float) -> str:
    source_rgb = _parse_hex_color(source)
    target_rgb = _parse_hex_color(target)
    if source_rgb is None or target_rgb is None:
        return source
    mix = max(0.0, min(1.0, float(ratio)))
    return _format_hex_color(
        tuple(round(src * (1.0 - mix) + dst * mix) for src, dst in zip(source_rgb, target_rgb))
    )


class SolidButton(tk.Label):
    # macOS may ignore custom tk.Button backgrounds, so use a label-backed button
    # when a stable accent color matters.
    def __init__(self, master=None, **kwargs):
        self._command = kwargs.pop("command", None)
        state = str(kwargs.pop("state", "normal") or "normal")
        normal_bg = kwargs.pop("bg", kwargs.pop("background", "#1E8449"))
        normal_fg = kwargs.pop("fg", kwargs.pop("foreground", "#FFFFFF"))
        active_bg = kwargs.pop("activebackground", None)
        active_fg = kwargs.pop("activeforeground", None)
        disabled_bg = kwargs.pop("disabledbackground", None)
        disabled_fg = kwargs.pop("disabledforeground", None)
        default_cursor = kwargs.get("cursor", "hand2")
        self._disabled_cursor = kwargs.pop("disabledcursor", "arrow")
        kwargs.setdefault("cursor", default_cursor)
        kwargs.setdefault("bd", 0)
        kwargs.setdefault("highlightthickness", 0)
        kwargs.setdefault("takefocus", 1)
        super().__init__(master, **kwargs)
        self._default_cursor = default_cursor
        self._normal_bg = str(normal_bg)
        self._normal_fg = str(normal_fg)
        self._active_bg = str(active_bg or _blend_hex_color(self._normal_bg, "#000000", 0.10))
        self._active_fg = str(active_fg or self._normal_fg)
        self._disabled_bg = str(disabled_bg or _blend_hex_color(self._normal_bg, "#FFFFFF", 0.45))
        self._disabled_fg = str(disabled_fg or _blend_hex_color(self._normal_fg, "#FFFFFF", 0.25))
        self._state = "normal"
        self._hover = False
        self._pressed = False
        self.bind("<Enter>", self._on_enter, add="+")
        self.bind("<Leave>", self._on_leave, add="+")
        self.bind("<ButtonPress-1>", self._on_press, add="+")
        self.bind("<ButtonRelease-1>", self._on_release, add="+")
        self.bind("<Return>", self._on_key_activate, add="+")
        self.bind("<space>", self._on_key_activate, add="+")
        self.configure(state=state)

    def _apply_visual_state(self) -> None:
        if self._state == "disabled":
            super().configure(bg=self._disabled_bg, fg=self._disabled_fg, cursor=self._disabled_cursor)
            return
        bg = self._active_bg if (self._hover or self._pressed) else self._normal_bg
        fg = self._active_fg if (self._hover or self._pressed) else self._normal_fg
        super().configure(bg=bg, fg=fg, cursor=self._default_cursor)

    def _on_enter(self, _event):
        if self._state == "disabled":
            return
        self._hover = True
        self._apply_visual_state()

    def _on_leave(self, _event):
        self._hover = False
        self._pressed = False
        self._apply_visual_state()

    def _on_press(self, _event):
        if self._state == "disabled":
            return "break"
        self._pressed = True
        self._apply_visual_state()
        return "break"

    def _on_release(self, event):
        if self._state == "disabled":
            return "break"
        inside = 0 <= event.x < self.winfo_width() and 0 <= event.y < self.winfo_height()
        self._pressed = False
        self._hover = inside
        self._apply_visual_state()
        if inside:
            self.invoke()
        return "break"

    def _on_key_activate(self, _event):
        if self._state == "disabled":
            return "break"
        self.invoke()
        return "break"

    def invoke(self):
        if self._state == "disabled" or not callable(self._command):
            return None
        return self._command()

    def configure(self, cnf=None, **kwargs):
        if cnf is None and not kwargs:
            return super().configure()
        merged = {}
        if cnf:
            merged.update(dict(cnf))
        merged.update(kwargs)
        if "command" in merged:
            self._command = merged.pop("command")
        if "state" in merged:
            self._state = str(merged.pop("state") or "normal")
        normal_bg = merged.pop("bg", merged.pop("background", None))
        normal_fg = merged.pop("fg", merged.pop("foreground", None))
        active_bg = merged.pop("activebackground", None)
        active_fg = merged.pop("activeforeground", None)
        disabled_bg = merged.pop("disabledbackground", None)
        disabled_fg = merged.pop("disabledforeground", None)
        if "cursor" in merged:
            self._default_cursor = str(merged["cursor"])
        if "disabledcursor" in merged:
            self._disabled_cursor = str(merged.pop("disabledcursor"))
        if normal_bg is not None:
            self._normal_bg = str(normal_bg)
            if active_bg is None:
                active_bg = _blend_hex_color(self._normal_bg, "#000000", 0.10)
            if disabled_bg is None:
                disabled_bg = _blend_hex_color(self._normal_bg, "#FFFFFF", 0.45)
        if normal_fg is not None:
            self._normal_fg = str(normal_fg)
            if active_fg is None:
                active_fg = self._normal_fg
            if disabled_fg is None:
                disabled_fg = _blend_hex_color(self._normal_fg, "#FFFFFF", 0.25)
        if active_bg is not None:
            self._active_bg = str(active_bg)
        if active_fg is not None:
            self._active_fg = str(active_fg)
        if disabled_bg is not None:
            self._disabled_bg = str(disabled_bg)
        if disabled_fg is not None:
            self._disabled_fg = str(disabled_fg)
        result = super().configure(**merged) if merged else None
        self._apply_visual_state()
        return result

    config = configure

    def cget(self, key):
        lookup = str(key)
        if lookup in {"bg", "background"}:
            return self._normal_bg
        if lookup in {"fg", "foreground"}:
            return self._normal_fg
        if lookup == "activebackground":
            return self._active_bg
        if lookup == "activeforeground":
            return self._active_fg
        if lookup == "disabledbackground":
            return self._disabled_bg
        if lookup == "disabledforeground":
            return self._disabled_fg
        if lookup == "command":
            return self._command
        if lookup == "state":
            return self._state
        if lookup == "disabledcursor":
            return self._disabled_cursor
        return super().cget(key)


def parse_worker_threads(value, default: int = 2) -> int:
    try:
        return max(1, int(str(value).strip()))
    except Exception:
        return max(1, int(default))


def append_log_row(log_tree, text: str, max_rows: int = LOG_MAX_ROWS) -> None:
    append_log_rows(log_tree, [text], max_rows=max_rows)


def capture_vertical_view_state(widget, *, follow_threshold: float = 0.995) -> tuple[bool, tuple[str, object] | None]:
    yview = getattr(widget, "yview", None)
    if not callable(yview):
        return True, None
    try:
        first, last = yview()
        last_fraction = float(last)
    except Exception:
        return True, None
    follow_tail = last_fraction >= float(follow_threshold)
    widget_class = _widget_class(widget)
    if widget_class == "Text":
        index = getattr(widget, "index", None)
        if callable(index):
            try:
                return follow_tail, ("text", str(index("@0,0")))
            except Exception:
                pass
    if widget_class == "Treeview":
        identify_row = getattr(widget, "identify_row", None)
        if callable(identify_row):
            try:
                anchor = str(identify_row(0) or "").strip()
                return follow_tail, ("tree", anchor)
            except Exception:
                pass
    try:
        first_fraction = float(first)
    except Exception:
        first_fraction = None
    return follow_tail, ("fraction", first_fraction)


def restore_vertical_view_state(widget, state: tuple[str, object] | None) -> None:
    if state is None:
        return
    kind, payload = state
    if kind == "text":
        yview = getattr(widget, "yview", None)
        if not callable(yview):
            return
        try:
            yview(str(payload))
        except Exception:
            pass
        return
    if kind == "tree":
        anchor = str(payload or "").strip()
        if not anchor:
            return
        get_children = getattr(widget, "get_children", None)
        see = getattr(widget, "see", None)
        identify_row = getattr(widget, "identify_row", None)
        yview_scroll = getattr(widget, "yview_scroll", None)
        if not (callable(get_children) and callable(see) and callable(identify_row)):
            return
        try:
            children = list(get_children())
        except Exception:
            return
        if anchor not in children:
            return
        try:
            see(anchor)
            current_top = str(identify_row(0) or "").strip()
            if current_top and current_top in children and callable(yview_scroll):
                delta = children.index(anchor) - children.index(current_top)
                if delta:
                    yview_scroll(delta, "units")
        except Exception:
            pass
        return
    if kind == "fraction":
        yview_moveto = getattr(widget, "yview_moveto", None)
        if not callable(yview_moveto):
            return
        try:
            fraction = max(0.0, min(1.0, float(payload)))
            yview_moveto(fraction)
        except Exception:
            pass


def _shift_text_view_state_after_trim(state: tuple[str, object] | None, trimmed_lines: int) -> tuple[str, object] | None:
    if trimmed_lines <= 0 or state is None:
        return state
    kind, payload = state
    if kind != "text":
        return state
    index_text = str(payload or "").strip()
    if not index_text:
        return state
    line_text, dot, col_text = index_text.partition(".")
    try:
        new_line = max(1, int(line_text) - int(trimmed_lines))
    except Exception:
        return state
    return kind, f"{new_line}{dot or '.'}{col_text or '0'}"


def append_log_rows(log_tree, texts: list[str], max_rows: int = LOG_MAX_ROWS) -> None:
    rows_to_add = [redact_sensitive_text(text) for text in texts if text is not None]
    if not rows_to_add:
        return
    if _widget_class(log_tree) == "Text":
        append_text_log_messages(log_tree, rows_to_add, max_rows=max_rows, prepend_time=True)
        return
    follow_tail, first_fraction = capture_vertical_view_state(log_tree)
    configure_log_amount_row_tag(log_tree)
    for text in rows_to_add:
        now = datetime.now().strftime("%H:%M:%S")
        tags = (LOG_AMOUNT_ROW_TAG,) if log_amount_value_spans(text) else ()
        log_tree.insert("", END, values=(now, text), tags=tags)
    rows = list(log_tree.get_children())
    overflow = len(rows) - max_rows
    if overflow > 0:
        log_tree.delete(*rows[:overflow])
        rows = rows[overflow:]
    if rows:
        if follow_tail:
            log_tree.see(rows[-1])
        else:
            restore_vertical_view_state(log_tree, first_fraction)


def append_text_log_messages(
    text_widget,
    messages: list[str],
    *,
    max_rows: int = LOG_MAX_ROWS,
    prepend_time: bool = False,
) -> None:
    rows_to_add = [str(message) for message in messages if message is not None]
    if not rows_to_add:
        return
    follow_tail, view_state = capture_vertical_view_state(text_widget)
    try:
        text_widget.configure(state="normal")
        configure_log_amount_tag(text_widget)
        for text in rows_to_add:
            display_text = f"{datetime.now().strftime('%H:%M:%S')}  {text}" if prepend_time else text
            insert_log_message_with_amount_highlights(text_widget, display_text)
        trimmed_lines = 0
        try:
            total_lines = max(0, int(str(text_widget.index("end-1c")).split(".", 1)[0]) - 1)
        except Exception:
            total_lines = 0
        if total_lines > max_rows:
            trimmed_lines = total_lines - max_rows
            try:
                text_widget.delete("1.0", f"{trimmed_lines + 1}.0")
            except Exception:
                trimmed_lines = 0
        if follow_tail:
            text_widget.see("end")
        else:
            restore_vertical_view_state(text_widget, _shift_text_view_state_after_trim(view_state, trimmed_lines))
    finally:
        try:
            text_widget.configure(state="disabled")
        except Exception:
            pass


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


def _ensure_ui_bridge_state(owner) -> None:
    if not hasattr(owner, "_ui_bridge_lock"):
        owner._ui_bridge_lock = threading.Lock()
    if not hasattr(owner, "_ui_bridge_started"):
        owner._ui_bridge_started = False
    if not hasattr(owner, "_ui_bridge_root"):
        owner._ui_bridge_root = None
    if not hasattr(owner, "_ui_bridge_interval_ms"):
        owner._ui_bridge_interval_ms = UI_BATCH_DELAY_MS
    if not hasattr(owner, "_ui_bridge_token"):
        owner._ui_bridge_token = None
    if not hasattr(owner, "_ui_main_thread_id"):
        owner._ui_main_thread_id = None


def _drain_scheduled_ui_callbacks(owner) -> None:
    _ensure_ui_debounce_state(owner)
    now = time.monotonic()
    callbacks = []
    with owner._ui_debounce_lock:
        due_keys = [
            key
            for key, (due_at, _callback) in owner._ui_debounce_entries.items()
            if float(due_at) <= now
        ]
        for key in due_keys:
            _due_at, callback = owner._ui_debounce_entries.pop(key, (0.0, None))
            if callback is not None:
                callbacks.append(callback)
    for callback in callbacks:
        try:
            callback()
        except Exception:
            logger.exception("UI render callback failed")


def _run_ui_bridge_tick(owner) -> None:
    _ensure_ui_bridge_state(owner)
    _drain_scheduled_ui_callbacks(owner)
    flush_queued_ui_renders(owner)
    log_tree = getattr(owner, "log_tree", None)
    if log_tree is not None:
        flush_queued_log_rows(owner, log_tree, max_rows=LOG_MAX_ROWS)

    with owner._ui_bridge_lock:
        if not owner._ui_bridge_started:
            owner._ui_bridge_token = None
            return
        root_widget = owner._ui_bridge_root
        interval_ms = max(1, int(owner._ui_bridge_interval_ms))
    after = getattr(root_widget, "after", None)
    if not callable(after):
        with owner._ui_bridge_lock:
            owner._ui_bridge_started = False
            owner._ui_bridge_token = None
        return
    try:
        token = after(interval_ms, lambda o=owner: _run_ui_bridge_tick(o))
    except Exception:
        with owner._ui_bridge_lock:
            owner._ui_bridge_started = False
            owner._ui_bridge_token = None
        return
    with owner._ui_bridge_lock:
        owner._ui_bridge_token = token


def start_ui_bridge(owner, *, root=None, interval_ms: int = UI_BATCH_DELAY_MS) -> None:
    _ensure_ui_bridge_state(owner)
    root_widget = root if root is not None else getattr(owner, "root", None)
    after = getattr(root_widget, "after", None)
    if not callable(after):
        return
    should_start = False
    with owner._ui_bridge_lock:
        owner._ui_bridge_root = root_widget
        owner._ui_bridge_interval_ms = max(1, int(interval_ms))
        owner._ui_main_thread_id = threading.get_ident()
        if not owner._ui_bridge_started:
            owner._ui_bridge_started = True
            should_start = True
    if should_start:
        _run_ui_bridge_tick(owner)


def _ui_bridge_active(owner) -> bool:
    _ensure_ui_bridge_state(owner)
    with owner._ui_bridge_lock:
        return bool(owner._ui_bridge_started)


def _ui_call_is_main_thread(owner) -> bool:
    _ensure_ui_bridge_state(owner)
    main_thread_id = getattr(owner, "_ui_main_thread_id", None)
    return main_thread_id is None or threading.get_ident() == main_thread_id


def _ui_shutdown_requested(owner) -> bool:
    if bool(getattr(owner, "_closing", False)):
        return True
    root = getattr(owner, "root", None)
    if root is not None and bool(getattr(root, "_closing", False)):
        return True
    widget = root if root is not None else owner
    if not _ui_call_is_main_thread(owner):
        # Background workers must not touch Tk widget methods directly.
        # We rely on the `_closing` flags above and let the main-thread UI bridge
        # determine final widget liveness.
        return False
    winfo_exists = getattr(widget, "winfo_exists", None)
    if callable(winfo_exists):
        try:
            return not bool(winfo_exists())
        except Exception:
            return True
    return False


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
    if _ui_shutdown_requested(owner):
        return
    _ensure_log_buffer_state(owner)
    with owner._log_state_lock:
        owner._log_buffer.append(str(text or ""))
        threshold = current_ui_batch_size(owner, batch_size=batch_size, default=1)
        if len(owner._log_buffer) >= threshold:
            owner._log_flush_requested = True
        else:
            owner._log_timer_requested = True
    if _ui_bridge_active(owner):
        return
    if not _ui_call_is_main_thread(owner):
        return
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
            logger.exception("UI render callback failed")


def queue_ui_render(
    owner,
    callback,
    *,
    root=None,
    batch_size: int | None = None,
    delay_ms: int = UI_BATCH_DELAY_MS,
) -> None:
    if _ui_shutdown_requested(owner):
        return
    _ensure_ui_render_state(owner)
    with owner._ui_render_state_lock:
        owner._ui_render_callbacks.append(callback)
        threshold = current_ui_batch_size(owner, batch_size=batch_size, default=1)
        if len(owner._ui_render_callbacks) >= threshold:
            owner._ui_render_flush_requested = True
        else:
            owner._ui_render_timer_requested = True
    if _ui_bridge_active(owner):
        return
    if not _ui_call_is_main_thread(owner):
        return
    flush_queued_ui_renders(owner)


def dispatch_ui_callback(owner, callback, *, root=None) -> None:
    queue_ui_render(owner, callback, root=root if root is not None else getattr(owner, "root", None))


def stop_ui_bridge(owner) -> None:
    _ensure_ui_bridge_state(owner)
    token = None
    root_widget = None
    with owner._ui_bridge_lock:
        token = getattr(owner, "_ui_bridge_token", None)
        root_widget = getattr(owner, "_ui_bridge_root", None)
        owner._ui_bridge_started = False
        owner._ui_bridge_token = None
        owner._ui_bridge_root = None
    after_cancel = getattr(root_widget, "after_cancel", None)
    if token is not None and callable(after_cancel):
        try:
            after_cancel(token)
        except Exception:
            pass
    _ensure_ui_render_state(owner)
    with owner._ui_render_state_lock:
        owner._ui_render_callbacks = []
        owner._ui_render_flush_requested = False
        owner._ui_render_timer_requested = False
    _ensure_log_buffer_state(owner)
    with owner._log_state_lock:
        owner._log_buffer = []
        owner._log_flush_requested = False
        owner._log_timer_requested = False
    _ensure_ui_debounce_state(owner)
    with owner._ui_debounce_lock:
        owner._ui_debounce_entries = {}


def _ensure_ui_debounce_state(owner) -> None:
    if not hasattr(owner, "_ui_debounce_entries"):
        owner._ui_debounce_entries = {}
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
    if _ui_shutdown_requested(owner):
        return
    _ensure_ui_debounce_state(owner)
    callback_key = str(key or "").strip() or "_default"
    with owner._ui_debounce_lock:
        owner._ui_debounce_entries[callback_key] = (
            time.monotonic() + (max(1, int(delay_ms)) / 1000.0),
            callback,
        )
    if _ui_bridge_active(owner):
        return
    if _ui_call_is_main_thread(owner):
        _drain_scheduled_ui_callbacks(owner)


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
    if unit <= 0:
        raise RuntimeError("随机金额最小精度必须大于 0")
    low_i = int((low / unit).to_integral_value(rounding=ROUND_CEILING))
    high_i = int((high / unit).to_integral_value(rounding=ROUND_FLOOR))
    if low_i > high_i:
        raise RuntimeError(f"随机金额范围至少要包含 1 个最小精度单位（{decimal_to_text(unit)}）")
    return (unit * Decimal(random.randint(low_i, high_i))).quantize(unit)


def decimal_to_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def decimal_to_stat_text(value, *, precision: int = 5, fixed: bool = False) -> str:
    """Format a displayed statistic without changing the underlying calculation value."""
    digits = max(0, int(precision))
    amount = value if isinstance(value, Decimal) else Decimal(str(value or "0"))
    if not amount.is_finite():
        return decimal_to_text(amount)
    quantum = Decimal(1).scaleb(-digits)
    with localcontext() as context:
        context.prec = max(28, amount.adjusted() + digits + 3, len(amount.as_tuple().digits) + digits + 2)
        rounded = amount.quantize(quantum, rounding=ROUND_HALF_UP)
    if rounded == 0:
        rounded = rounded.copy_abs()
    text = format(rounded, "f")
    if not fixed and "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def mask_text(value: str, head: int = 6, tail: int = 4) -> str:
    if len(value) <= head + tail:
        return "*" * len(value)
    return f"{value[:head]}...{value[-tail:]}"
