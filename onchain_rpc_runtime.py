#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math

from api_clients import (
    ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS,
    call_with_transient_rpc_retries,
    is_transient_rpc_error,
)
from app_defaults import (
    ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
    ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_MIN,
)


def onchain_poll_interval_seconds(
    owner=None,
    configured_value: object | None = None,
    *,
    default: float = ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
) -> float:
    value = configured_value
    if value is None and owner is not None:
        getter = getattr(owner, "_runtime_confirmation_poll_interval_seconds", None)
        if callable(getter):
            try:
                value = getter()
            except Exception:
                value = None
    try:
        interval = float(value) if value is not None else float(default)
    except Exception:
        interval = float(default)
    if not math.isfinite(interval) or interval < ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_MIN:
        return float(default)
    return interval


def onchain_rpc_timeout_kwargs(
    owner=None,
    poll_interval_seconds: float | None = None,
    *,
    default: float = ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
) -> dict[str, object]:
    interval = onchain_poll_interval_seconds(owner, poll_interval_seconds, default=default)
    read_timeout = min(3.0, max(1.0, interval))
    return {
        "connect_timeout": min(3.0, read_timeout),
        "read_timeout": read_timeout,
        "max_attempts": 1,
    }


def onchain_rpc_call(method, *args, timeout_kwargs: dict[str, object] | None = None):
    if not timeout_kwargs:
        return method(*args)
    try:
        return method(*args, **timeout_kwargs)
    except TypeError as exc:
        message = str(exc)
        if any(name in message for name in ("connect_timeout", "read_timeout", "max_attempts")):
            return method(*args)
        raise


def close_owner_thread_rpc_session(owner) -> None:
    closer = getattr(getattr(owner, "client", None), "_close_thread_rpc_session", None)
    if callable(closer):
        try:
            closer()
        except Exception:
            pass


def onchain_record_retry_delay_seconds(owner=None, params=None) -> float:
    if params is not None:
        try:
            return max(0.2, float(getattr(params, "confirm_poll_interval_seconds", 1.0) or 1.0))
        except Exception:
            pass
    return max(0.2, onchain_poll_interval_seconds(owner))


def run_onchain_record_rpc_with_retries(
    owner,
    action,
    *,
    prefix: str,
    params=None,
    dispatch_ui: bool = False,
):
    label = str(prefix or "链上记录").strip()
    stop_event = getattr(owner, "stop_requested", None)
    should_stop = stop_event.is_set if stop_event is not None else None
    wait = stop_event.wait if stop_event is not None else None

    def emit_log(message: str) -> None:
        if dispatch_ui:
            dispatcher = getattr(owner, "_dispatch_ui", None)
            if callable(dispatcher):
                dispatcher(lambda m=message: owner.log(m))
                return
        owner.log(message)

    def on_retry(next_attempt: int, total_attempts: int, exc: Exception) -> None:
        emit_log(f"{label} 临时RPC错误，准备重试 {next_attempt}/{total_attempts}：{exc}")

    return call_with_transient_rpc_retries(
        action,
        attempts=ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS,
        should_stop=should_stop,
        wait=wait,
        retry_delay_seconds=onchain_record_retry_delay_seconds(owner, params),
        on_retry=on_retry,
    )


__all__ = [
    "ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS",
    "call_with_transient_rpc_retries",
    "close_owner_thread_rpc_session",
    "is_transient_rpc_error",
    "onchain_poll_interval_seconds",
    "onchain_record_retry_delay_seconds",
    "onchain_rpc_call",
    "onchain_rpc_timeout_kwargs",
    "run_onchain_record_rpc_with_retries",
]
