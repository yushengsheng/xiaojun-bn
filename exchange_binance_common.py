#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import functools
import hashlib
import hmac
import logging
import random
import re
import threading
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from system_proxy import configure_requests_session_for_proxy
from retry_utils import RetryPolicy, run_with_retries


FUTURES_MARGIN_TYPE_DEFAULT = "CROSSED"
WITHDRAW_FEE_BUFFER_DEFAULT = 0
SUPPORTED_QUOTE_ASSET_SUFFIXES = (
    "FDUSD",
    "USDT",
    "USDC",
    "USD1",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "TRY",
    "EUR",
    "U",
)

logger = logging.getLogger("bot")

def explain_binance_api_error(code: int, msg: str) -> str:
    message_text = str(msg or "").strip()
    message_lower = message_text.lower()

    if code == -9000 and (
        "rw00508" in message_lower
        or "face recognition" in message_lower
        or "re-login" in message_lower
        or "account may be at risk" in message_lower
    ):
        return "账号被交易所风控，需重新登录 Binance 并完成人脸验证后再尝试资金操作。若仍无法处理，请联系 Binance 客服。"
    if code == -1021:
        return "请求时间戳与 Binance 服务器时间不一致，或代理链路过慢导致请求到达过晚。"
    if code == -1001:
        return "Binance 服务暂时不可用，或当前代理/网络链路不稳定。"
    if code == -1003:
        return "请求过于频繁，或 Binance 当前触发限流/繁忙保护。"
    if code == -2015:
        return "API Key / Secret 可能无效，或权限、IP 白名单、账号限制与当前请求不匹配。"
    if code == -2010:
        if "insufficient balance" in message_lower:
            return "余额不足，无法完成当前下单、划转或提现操作。"
        if "filter failure" in message_lower:
            return "下单数量、价格精度或最小成交额不满足交易所规则。"
        return "订单或资金操作被交易所拒绝，请检查余额、交易对规则和账号权限。"
    if "face recognition" in message_lower:
        return "交易所要求重新登录并完成人脸验证后才能继续当前操作。"
    if "insufficient balance" in message_lower:
        return "余额不足，无法完成当前操作。"
    if "filter failure" in message_lower:
        return "数量、价格精度或最小成交额不满足交易所规则。"
    return ""

def short_binance_api_error(code: int, msg: str) -> str:
    message_text = str(msg or "").strip()
    message_lower = message_text.lower()

    if code == -9000 and (
        "rw00508" in message_lower
        or "face recognition" in message_lower
        or "re-login" in message_lower
        or "account may be at risk" in message_lower
    ):
        return "交易所风控/需人脸验证"
    if code == -1021:
        return "时间戳不一致/链路慢"
    if code == -1001:
        return "服务不可用/网络不稳"
    if code == -1003:
        return "限流/系统繁忙"
    if code == -2015:
        return "API权限/IP白名单问题"
    if code == -1135:
        return "账号状态不允许交易"
    if code == -4411:
        return "需签署合约交易协议"
    if code == 32020:
        return "合约交易/划转临时暂停"
    if code == -2010:
        if "insufficient balance" in message_lower:
            return "余额不足"
        if "filter failure" in message_lower:
            return "交易规则不满足"
        return "交易所拒绝请求"
    return ""

def summarize_exchange_exception(exc: object, *, fallback: str = "异常", max_len: int = 24) -> str:
    if isinstance(exc, BinanceAPIError):
        short_text = short_binance_api_error(exc.code, exc.msg)
        if short_text:
            return short_text

    text = str(exc or "").strip().replace("\n", " ")
    if not text:
        return fallback

    code_match = re.search(r"Binance API error\s+(-?\d+)", text, flags=re.IGNORECASE)
    if code_match:
        try:
            short_text = short_binance_api_error(int(code_match.group(1)), text)
        except Exception:
            short_text = ""
        if short_text:
            return short_text

    lower_text = text.lower()
    if "read timed out" in lower_text or "connect timeout" in lower_text or "timed out" in lower_text:
        return "网络超时/代理慢"
    if "proxyerror" in lower_text or "socks" in lower_text:
        return "代理连接异常"
    if "face recognition" in lower_text or "rw00508" in lower_text:
        return "交易所风控/需人脸验证"
    if "insufficient balance" in lower_text:
        return "余额不足"
    if "filter failure" in lower_text:
        return "交易规则不满足"

    return text if len(text) <= max_len else (text[: max_len - 1] + "…")

class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance API error {code}: {msg}")

    def __str__(self) -> str:
        base_text = f"Binance API error {self.code}: {self.msg}"
        explanation = explain_binance_api_error(self.code, self.msg)
        if not explanation:
            return base_text
        return f"{base_text} | 中文说明: {explanation}"


def retry_request(max_retries=10, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = max(1, int(max_retries or 1))
            retry_delay = max(0.0, float(delay or 0))

            def should_retry(exc: Exception) -> bool:
                if isinstance(exc, (requests.exceptions.RequestException, ConnectionError, TimeoutError)):
                    return True
                return isinstance(exc, BinanceAPIError) and exc.code in {-1001, -1003, -2015}

            def on_retry(failed_attempt: int, total: int, exc: Exception) -> None:
                if isinstance(exc, BinanceAPIError):
                    logger.warning(f"Binance 可重试错误 ({failed_attempt}/{total}): {exc}，将在 {retry_delay:g} 秒后重试...")
                else:
                    logger.warning(f"网络请求失败 ({failed_attempt}/{total}): {exc}，将在 {retry_delay:g} 秒后重试...")

            def on_give_up(total: int, _exc: Exception) -> None:
                logger.error(f"重试 {total} 次后仍然失败。")

            return run_with_retries(
                lambda: func(*args, **kwargs),
                policy=RetryPolicy(attempts=retry_count, delay_seconds=retry_delay),
                should_retry=should_retry,
                on_retry=on_retry,
                on_give_up=on_give_up,
                sleep_fn=time.sleep,
            )
        return wrapper
    return decorator
