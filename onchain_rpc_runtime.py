#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
import re

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


def _clean_onchain_failure_text(error: object) -> str:
    text = str(error or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^(?:\[[^\]]+\])+\s*", "", text).strip()
    for marker in (
        "转账失败：",
        "中转失败：",
        "执行失败：",
        "任务异常终止：",
        "链上转账任务异常终止：",
        "链上中转任务异常终止：",
    ):
        if marker in text:
            text = text.split(marker, 1)[1].strip()
    return text


def _ellipsize_status_text(text: str, max_chars: int) -> str:
    value = str(text or "").strip()
    limit = max(8, int(max_chars or 8))
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _network_error_detail(text: str) -> str:
    cleaned = _clean_onchain_failure_text(text)
    for marker in ("最后一次RPC错误：", "最后错误：", "RPC 请求失败：", "RPC请求失败："):
        if marker in cleaned:
            cleaned = cleaned.split(marker, 1)[1].strip()
    lowered = cleaned.lower()
    if "read timed out" in lowered:
        return "Read timed out"
    if "timed out" in lowered or "timeout" in lowered:
        return "timeout"
    if "429" in lowered or "too many requests" in lowered:
        return "请求过频"
    for code in ("503", "502", "504"):
        if code in lowered:
            return f"HTTP {code}"
    if "proxy" in lowered:
        return "代理连接失败"
    if "ssl" in lowered:
        return "SSL连接异常"
    if "connection refused" in lowered:
        return "连接被拒绝"
    if "connection reset" in lowered:
        return "连接重置"
    if "max retries exceeded" in lowered:
        return "连接重试耗尽"
    return _ellipsize_status_text(cleaned, 18)


def format_onchain_failure_status(error: object, *, max_chars: int = 36) -> str:
    text = _clean_onchain_failure_text(error)
    if not text:
        return "❌失败"
    lowered = text.lower()

    if "任务已停止" in text or text == "已停止":
        return "❌已停止"
    if "待恢复" in text:
        return "❌待恢复"

    if "中转手续费预留不足" in text:
        return _ellipsize_status_text("❌中转手续费预留不足", max_chars)
    balance_markers = (
        "insufficient funds",
        "not enough funds",
        "does not have enough funds",
        "gas required exceeds allowance",
        "exceeds balance",
    )
    if "余额不足" in text or any(marker in lowered for marker in balance_markers):
        if "中转钱包" in text and "代币" in text:
            label = "❌中转钱包代币不足"
        elif "中转钱包" in text and ("原生" in text or "gas" in lowered):
            label = "❌中转钱包原生币不足"
        elif "源钱包" in text and "原生" in text:
            label = "❌源钱包原生币不足"
        elif "源钱包" in text:
            label = "❌源钱包余额不足"
        elif "代币" in text:
            label = "❌代币余额不足"
        elif "原生" in text or "gas" in lowered:
            label = "❌原生币不足(gas)"
        else:
            label = "❌余额不足"
        return _ellipsize_status_text(label, max_chars)

    if "确认中超过" in text or "等待链上确认超过" in text:
        if is_transient_rpc_error(RuntimeError(text)):
            return _ellipsize_status_text("❌确认超时(RPC无响应)", max_chars)
        return "❌确认超时"
    if "等待余额达到目标值超过" in text:
        return _ellipsize_status_text("❌到账确认超时", max_chars)
    if "任务收尾超时" in text:
        return _ellipsize_status_text("❌收尾超时", max_chars)

    nonce_markers = ("nonce too low", "nonce 已前进", "nonce 回退", "replacement transaction underpriced")
    if any(marker in lowered or marker in text for marker in nonce_markers):
        return _ellipsize_status_text("❌Nonce异常", max_chars)

    if "执行状态异常" in text or "execution reverted" in lowered or "reverted" in lowered:
        return _ellipsize_status_text("❌链上执行失败", max_chars)

    network_markers = (
        "connection",
        "proxy",
        "ssl",
        "eof",
        "remote disconnected",
        "remote end closed",
        "read timed out",
        "max retries exceeded",
    )
    if any(marker in lowered for marker in network_markers):
        return _ellipsize_status_text(f"❌网络异常：{_network_error_detail(text)}", max_chars)
    if is_transient_rpc_error(RuntimeError(text)):
        return _ellipsize_status_text(f"❌RPC不稳定：{_network_error_detail(text)}", max_chars)

    if "地址" in text and "格式" in text:
        return _ellipsize_status_text("❌地址格式错误", max_chars)
    if "私钥" in text or "助记词" in text or "签名" in text:
        return _ellipsize_status_text("❌凭证错误", max_chars)

    return _ellipsize_status_text(f"❌{text}", max_chars)


__all__ = [
    "ONCHAIN_TRANSIENT_RPC_RETRY_ATTEMPTS",
    "call_with_transient_rpc_retries",
    "close_owner_thread_rpc_session",
    "format_onchain_failure_status",
    "is_transient_rpc_error",
    "onchain_poll_interval_seconds",
    "onchain_record_retry_delay_seconds",
    "onchain_rpc_call",
    "onchain_rpc_timeout_kwargs",
    "run_onchain_record_rpc_with_retries",
]
