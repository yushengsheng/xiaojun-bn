#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import hmac
import logging
import re
import threading
import time
import uuid
from contextlib import contextmanager, nullcontext
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode

import requests
from urllib3.exceptions import NewConnectionError

from system_proxy import configure_requests_session_for_proxy


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
MONTHLY_VOLUME_QUOTE_ASSETS = frozenset({"USDT", "USDC", "ETH", "BNB", "BTC"})

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
    def __init__(
        self,
        code: int,
        msg: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        self.code = code
        self.msg = msg
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"Binance API error {code}: {msg}")

    def __str__(self) -> str:
        base_text = f"Binance API error {self.code}: {self.msg}"
        explanation = explain_binance_api_error(self.code, self.msg)
        if not explanation:
            return base_text
        return f"{base_text} | 中文说明: {explanation}"


class UnconfirmedOperationError(RuntimeError):
    """A financial write may have executed; replaying it is unsafe."""


class RequestStoppedError(RuntimeError):
    """Stopped at the request gate, before a request was sent."""


class UnconfirmedOrderError(UnconfirmedOperationError):
    def __init__(self, symbol: str, client_order_id: str, *, market: str = "spot", journal_entry_id: str = ""):
        self.symbol = symbol
        self.client_order_id = client_order_id
        self.market = market
        self.journal_entry_id = journal_entry_id
        super().__init__(f"{symbol} 订单 {client_order_id} 的交易所执行状态未确认，已停止自动提交；请核对交易所订单")


class _OrderConfirmationTimeout(UnconfirmedOrderError):
    """One submission exhausted its grace period; its original intent remains active."""


class SharedRateLimitGate:
    def __init__(self, wait_seconds: float, *, stop_event=None):
        self.wait_seconds = max(0.0, float(wait_seconds or 0.0))
        self.stop_event = stop_event
        self._lock = threading.Lock()
        self._pause_until = 0.0

    def pause(self, retry_after: float | None = None) -> float:
        try:
            required_wait = max(0.0, float(retry_after or 0.0))
        except (TypeError, ValueError):
            required_wait = 0.0
        pause_seconds = max(self.wait_seconds, required_wait)
        now = time.monotonic()
        with self._lock:
            self._pause_until = max(self._pause_until, now + pause_seconds)
            return max(0.0, self._pause_until - now)

    def wait(self, stop_event=None, *, ignore_stop: bool = False) -> bool:
        effective_stop_event = None if ignore_stop else (stop_event or self.stop_event)

        def is_stopped() -> bool:
            if effective_stop_event is None:
                return False
            try:
                return bool(effective_stop_event.is_set())
            except Exception:
                return False

        while True:
            if is_stopped():
                return False
            with self._lock:
                remaining = self._pause_until - time.monotonic()
            if remaining <= 0:
                return not is_stopped()
            if effective_stop_event is not None:
                try:
                    if effective_stop_event.wait(remaining):
                        return False
                    continue
                except Exception:
                    pass
            time.sleep(remaining)


# ====================== Binance 客户端 ======================
class BinanceClient:
    SERVER_TIME_CACHE_TTL_SECONDS = 30.0
    DEFAULT_RECV_WINDOW_MS = 20000
    DEFAULT_REQUEST_RETRY_COUNT = 10
    DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 1.0
    ORDER_SUBMISSION_CONFIRMATION_GRACE_SECONDS = 30.0
    INVALID_PERMISSION_RETRY_COUNT = 3
    RETRYABLE_BINANCE_ERROR_CODES = {-1001, -1003, -2015}
    SHARED_RATE_LIMIT_ERROR_CODES = {-1003, -1008, -1015}
    SHARED_RATE_LIMIT_HTTP_STATUS_CODES = {418, 429}
    # These POST endpoints read data or set an absolute value, rather than move funds.
    REPLAY_SAFE_POST_PATHS = frozenset({
        "/sapi/v3/asset/getUserAsset", "/sapi/v1/asset/get-funding-asset",
        "/sapi/v1/asset/dust-btc", "/sapi/v1/convert/getQuote",
        "/sapi/v1/simple-earn/flexible/setAutoSubscribe",
        "/fapi/v1/positionSide/dual", "/fapi/v1/marginType", "/fapi/v1/leverage",
    })
    ORDER_PATHS = {"/api/v3/order": "spot", "/fapi/v1/order": "um_futures"}
    ORDER_TERMINAL_STATUSES = frozenset({"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"})
    _shared_convert_exchange_info_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    _shared_convert_asset_info_cache: dict[str, int] | None = None
    _shared_convert_cache_lock = threading.Lock()
    _shared_spot_market_direct_routes: dict[str, dict[str, str]] | None = None
    _shared_spot_market_reverse_routes: dict[str, list[dict[str, str]]] | None = None
    _shared_spot_market_route_lock = threading.Lock()
    _shared_server_time_offset_ms: dict[str, int] = {}
    _shared_server_time_synced_at: dict[str, float] = {}
    _shared_server_time_lock = threading.Lock()

    def __init__(
        self,
        key: str,
        secret: str,
        proxy_url: str = "",
        *,
        request_gate: SharedRateLimitGate | None = None,
        request_stop_event=None,
        operation_journal=None,
    ):
        if not key or not secret:
            raise ValueError("API KEY / SECRET 不能为空")

        self.key = key
        self.secret = secret.encode()
        self._proxy_url = str(proxy_url or "").strip()
        self._request_gate = request_gate
        self._request_stop_event = request_stop_event
        self._cleanup_request_depth = 0
        self.operation_journal = operation_journal
        self._order_journal_entries: dict[tuple[str, str, str], str] = {}
        self._pending_order_requests: dict[str, dict] = {}
        self._uncertain_write_error: UnconfirmedOperationError | None = None
        self._order_submission_serial = 0
        self._continuous_trading = False
        self._recovery_check = None
        self._bounded_request_depth = 0
        self.spot = "https://api.binance.com"
        self.um_futures = "https://fapi.binance.com"
        self.cm_futures = "https://dapi.binance.com"
        self.session = self._build_session()
        try:
            self.recv_window_ms = max(1000, int(self.DEFAULT_RECV_WINDOW_MS))
        except Exception:
            self.recv_window_ms = self.DEFAULT_RECV_WINDOW_MS

        self._exchange_info_cache = {}
        self._price_cache = {}
        self._um_futures_exchange_info_cache = {}
        self._um_futures_price_cache = {}
        self._um_futures_pretrade_cleared_assets: set[str] = set()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"X-MBX-APIKEY": self.key})
        configure_requests_session_for_proxy(
            session,
            explicit_proxy=self._proxy_url,
            target_url=self.spot,
            allow_system_proxy=True,
        )
        return session

    def _reset_session_after_transport_error(self) -> None:
        previous = getattr(self, "session", None)
        try:
            self.session = self._build_session()
        except Exception:
            self.session = previous
            return
        if previous is not None:
            try:
                previous.close()
            except Exception:
                pass
    def close(self) -> None:
        session = getattr(self, "session", None)
        self.session = None
        if session is None:
            return
        try:
            session.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    @staticmethod
    def _normalize_base_url(base: str) -> str:
        return str(base or "").rstrip("/")

    def _server_time_path(self, base: str) -> str:
        base_n = self._normalize_base_url(base)
        if base_n == self._normalize_base_url(self.um_futures):
            return "/fapi/v1/time"
        if base_n == self._normalize_base_url(self.cm_futures):
            return "/dapi/v1/time"
        return "/api/v3/time"

    @staticmethod
    def _local_timestamp_ms() -> int:
        return int(time.time() * 1000)

    def _fetch_server_time_ms(self, base: str) -> int:
        base_n = self._normalize_base_url(base)
        url = base_n + self._server_time_path(base_n)
        start_ms = self._local_timestamp_ms()
        r = self.session.get(url, timeout=10)
        end_ms = self._local_timestamp_ms()
        r.raise_for_status()
        data = r.json()
        try:
            server_time_ms = int(data.get("serverTime"))
        except Exception as exc:
            raise RuntimeError(f"服务器时间接口返回异常：{data}") from exc
        midpoint_ms = (start_ms + end_ms) // 2
        return int(server_time_ms - midpoint_ms)

    def _sync_server_time_offset(self, base: str, *, force: bool = False) -> int:
        base_n = self._normalize_base_url(base)
        now_mono = time.monotonic()
        cache_lock = type(self)._shared_server_time_lock
        with cache_lock:
            cached_offset = type(self)._shared_server_time_offset_ms.get(base_n)
            synced_at = type(self)._shared_server_time_synced_at.get(base_n, 0.0)
            if (not force) and cached_offset is not None and (now_mono - synced_at) < self.SERVER_TIME_CACHE_TTL_SECONDS:
                return int(cached_offset)

        offset_ms = self._fetch_server_time_ms(base_n)
        with cache_lock:
            type(self)._shared_server_time_offset_ms[base_n] = int(offset_ms)
            type(self)._shared_server_time_synced_at[base_n] = time.monotonic()
        return int(offset_ms)

    def current_server_time_ms(self, base: str | None = None) -> int:
        """Return an exchange-aligned timestamp for exact trade-history windows."""
        base_n = self._normalize_base_url(base or self.spot)
        offset_ms = 0
        try:
            offset_ms = self._sync_server_time_offset(base_n)
        except Exception:
            with type(self)._shared_server_time_lock:
                cached_offset = type(self)._shared_server_time_offset_ms.get(base_n)
            if cached_offset is not None:
                offset_ms = int(cached_offset)
        return self._local_timestamp_ms() + int(offset_ms)

    def _signed_params(self, base: str, params=None, *, force_time_sync: bool = False) -> dict[str, Any]:
        signed_params = dict(params or {})
        offset_ms = 0
        try:
            offset_ms = self._sync_server_time_offset(base, force=force_time_sync)
        except Exception:
            if force_time_sync:
                raise
            base_n = self._normalize_base_url(base)
            with type(self)._shared_server_time_lock:
                cached_offset = type(self)._shared_server_time_offset_ms.get(base_n)
            if cached_offset is not None:
                offset_ms = int(cached_offset)

        signed_params["timestamp"] = self._local_timestamp_ms() + int(offset_ms)
        signed_params["recvWindow"] = self.recv_window_ms
        signed_params["signature"] = self.sign(signed_params)
        return signed_params

    def sign(self, params: Dict[str, Any]):
        return hmac.new(
            self.secret, urlencode(params, True).encode(), hashlib.sha256
        ).hexdigest()

    @staticmethod
    def _retry_after_from_response(response) -> float | None:
        if response is None:
            return None
        try:
            value = response.headers.get("Retry-After")
            if value is None:
                return None
            return max(0.0, float(value))
        except (AttributeError, TypeError, ValueError):
            return None

    def _shared_rate_limit_details(self, exc: Exception, *, base: str, path: str) -> tuple[bool, float | None]:
        status_code = None
        retry_after = None
        if isinstance(exc, BinanceAPIError):
            status_code = exc.status_code
            retry_after = exc.retry_after
            if exc.code in self.SHARED_RATE_LIMIT_ERROR_CODES:
                return True, retry_after
            is_convert_request = (
                self._normalize_base_url(base) == self._normalize_base_url(self.spot)
                and str(path or "").startswith("/sapi/v1/convert/")
            )
            if exc.code == -1004 and is_convert_request:
                return True, retry_after
        elif isinstance(exc, requests.exceptions.HTTPError):
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            retry_after = self._retry_after_from_response(response)
        return status_code in self.SHARED_RATE_LIMIT_HTTP_STATUS_CODES, retry_after

    def _wait_for_request_gate(self, *, ignore_stop_event: bool = False) -> None:
        gate = getattr(self, "_request_gate", None)
        cleanup_active = int(getattr(self, "_cleanup_request_depth", 0) or 0) > 0
        stop = getattr(self, "_request_stop_event", None)
        if not (ignore_stop_event or cleanup_active) and stop is not None and stop.is_set():
            raise RequestStoppedError("检测到停止信号，停止当前请求")
        if gate is not None and not gate.wait(
            getattr(self, "_request_stop_event", None),
            ignore_stop=bool(ignore_stop_event or cleanup_active),
        ):
            raise RequestStoppedError("检测到停止信号，已停止限流等待")

    @contextmanager
    def bounded_requests(self):
        """Keep probes and shutdown cleanup bounded; their owner schedules retries."""
        self._bounded_request_depth = int(getattr(self, "_bounded_request_depth", 0)) + 1
        try:
            yield
        finally:
            self._bounded_request_depth -= 1

    @staticmethod
    def is_temporary_request_error(exc: Exception) -> bool:
        if isinstance(exc, BinanceAPIError):
            return exc.code in {-1001, -1003, -1006, -1007, -1008} or (exc.status_code or 0) >= 500
        if isinstance(exc, requests.exceptions.HTTPError):
            response = exc.response
            return response is None or response.status_code in {418, 429} or response.status_code >= 500
        return isinstance(exc, (requests.exceptions.RequestException, ConnectionError, TimeoutError))

    @staticmethod
    def _order_definitely_not_accepted(exc: Exception, market: str) -> bool:
        # ReadTimeout, reset-after-send and -2013 are NOT evidence of non-delivery.
        seen = set()
        pending = [exc]
        while pending:
            error = pending.pop()
            if id(error) in seen:
                continue
            seen.add(id(error))
            if isinstance(error, (requests.exceptions.ConnectTimeout, NewConnectionError)):
                return True
            if market == "um_futures":
                if isinstance(error, BinanceAPIError) and error.code == -1008:
                    return True
                response = getattr(error, "response", None)
                status = getattr(error, "status_code", None) or getattr(response, "status_code", None)
                message = getattr(error, "msg", "") or (response.text if response is not None else "")
                if status == 503 and message.strip() in {
                    "Service Unavailable.",
                    "Internal error; unable to process your request. Please try again.",
                }:
                    return True
            pending.extend(value for value in (
                getattr(error, "__cause__", None),
                getattr(error, "reason", None), *getattr(error, "args", ()),
            ) if isinstance(value, BaseException))
        return False

    def _wait_for_recovery(self, context: str, attempt: int, exc: Exception, *, ignore_stop_event=False, deadline=None) -> None:
        cleanup = bool(ignore_stop_event or getattr(self, "_cleanup_request_depth", 0))
        self._wait_for_request_gate(ignore_stop_event=cleanup)
        check = getattr(self, "_recovery_check", None)
        if not cleanup and callable(check):
            if deadline is None:
                check()
            else:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    check(timeout=min(6.0, remaining))
        self._wait_for_request_gate(ignore_stop_event=cleanup)
        delay = max(0.0, float(self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS))
        if deadline is not None:
            delay = min(delay, max(0.0, deadline - time.monotonic()))
        if attempt == 1 or attempt % 10 == 0:
            logger.warning("%s 暂未恢复（第 %d 次），保留当前步骤，%g 秒后继续：%s", context, attempt, delay, exc)
        stop = None if cleanup else getattr(self, "_request_stop_event", None)
        if stop is not None:
            if stop.wait(delay):
                raise RequestStoppedError("检测到停止信号，结束恢复等待并清理状态")
        elif delay:
            time.sleep(delay)

    @contextmanager
    def cleanup_requests_after_stop(self):
        self._cleanup_request_depth = int(getattr(self, "_cleanup_request_depth", 0) or 0) + 1
        try:
            yield
        finally:
            self._cleanup_request_depth = max(0, int(getattr(self, "_cleanup_request_depth", 1) or 1) - 1)

    def _pause_for_shared_rate_limit(self, exc: Exception, retry_after: float | None) -> None:
        gate = self._request_gate
        if gate is None:
            return
        remaining = gate.pause(retry_after)
        logger.warning(
            "检测到 Binance API 限流/繁忙保护，当前任务暂停 %.3g 秒后重试当前请求: %s",
            remaining,
            exc,
        )

    def _run_request_with_retry(
        self,
        request_fn,
        *,
        max_retries: int,
        delay: float,
        base: str = "",
        path: str = "",
        ignore_stop_event: bool = False,
        replay_safe: bool = True,
    ):
        last_exception = None
        retry_count = max(1, int(max_retries or self.DEFAULT_REQUEST_RETRY_COUNT))
        final_attempt_limit = retry_count
        retry_delay = max(0.0, float(delay if delay is not None else self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS))

        def sleep_before_retry() -> None:
            if retry_delay <= 0:
                return
            time.sleep(retry_delay)

        def retryable_binance_error_label(error_code: int) -> str:
            if error_code == -2015:
                return "Binance API 权限校验失败"
            return "Binance 系统繁忙"

        attempt = 0
        while attempt < retry_count:
            self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
            try:
                return request_fn()
            except requests.exceptions.HTTPError as e:
                is_rate_limited, retry_after = self._shared_rate_limit_details(e, base=base, path=path)
                if self._request_gate is not None and is_rate_limited:
                    self._pause_for_shared_rate_limit(e, retry_after)
                    continue
                if not replay_safe:
                    raise UnconfirmedOperationError(f"{path} 响应未确认，停止自动重发，请核对交易所状态") from e
                attempt += 1
                last_exception = e
                self._reset_session_after_transport_error()
                if attempt >= retry_count:
                    break
                logger.warning(f"网络请求失败 ({attempt}/{retry_count}): {e}，将在 {retry_delay:g} 秒后重试...")
                sleep_before_retry()
            except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
                if not replay_safe:
                    raise UnconfirmedOperationError(f"{path} 响应未确认，停止自动重发，请核对交易所状态") from e
                attempt += 1
                last_exception = e
                self._reset_session_after_transport_error()
                if attempt >= retry_count:
                    break
                logger.warning(f"网络请求失败 ({attempt}/{retry_count}): {e}，将在 {retry_delay:g} 秒后重试...")
                sleep_before_retry()
            except BinanceAPIError as e:
                last_exception = e
                if not replay_safe and self._is_uncertain_write_error(e):
                    raise UnconfirmedOperationError(f"{path} 执行状态未确认：{e}；停止自动重发") from e
                is_rate_limited, retry_after = self._shared_rate_limit_details(e, base=base, path=path)
                if self._request_gate is not None and is_rate_limited:
                    self._pause_for_shared_rate_limit(e, retry_after)
                    continue
                attempt += 1
                if e.code in self.RETRYABLE_BINANCE_ERROR_CODES:
                    attempt_limit = (
                        min(retry_count, self.INVALID_PERMISSION_RETRY_COUNT)
                        if e.code == -2015
                        else retry_count
                    )
                    final_attempt_limit = attempt_limit
                    if attempt >= attempt_limit:
                        break
                    logger.warning(f"{retryable_binance_error_label(e.code)} ({attempt}/{attempt_limit}): {e}，将在 {retry_delay:g} 秒后重试...")
                    sleep_before_retry()
                    continue
                raise e
        if getattr(self, "_continuous_trading", False):
            logger.debug("本次请求未恢复，继续保留当前步骤")
        else:
            logger.error(f"重试 {final_attempt_limit} 次后仍然失败。")
        if last_exception:
            raise last_exception
        raise RuntimeError("未知错误：重试失败")

    def _request_impl(self, base, method, path, params=None, *, timeout: float = 15):
        url = base + path
        last_error = None

        for attempt in range(2):
            signed_params = self._signed_params(base, params, force_time_sync=(attempt > 0))

            if method == "GET":
                r = self.session.get(url, params=signed_params, timeout=timeout)
            else:
                r = self.session.request(method, url, data=signed_params, timeout=timeout)

            try:
                data = r.json()
            except Exception:
                r.raise_for_status()
                raise

            if r.status_code == 200:
                return data

            err = BinanceAPIError(
                data.get("code", -1),
                data.get("msg", "Unknown"),
                status_code=r.status_code,
                retry_after=self._retry_after_from_response(r),
            )
            last_error = err
            if err.code == -1021 and attempt == 0:
                try:
                    self._sync_server_time_offset(base, force=True)
                    logger.warning("Binance 返回 -1021，已重新同步服务器时间并重试一次")
                    continue
                except Exception as sync_exc:
                    logger.warning("Binance 返回 -1021，但服务器时间同步失败: %s", sync_exc)
            raise err

        if last_error:
            raise last_error
        raise RuntimeError("签名请求失败")

    def request(self, base, method, path, params=None, *, ignore_stop_event: bool = False):
        if method == "POST" and path in self.ORDER_PATHS:
            return self._submit_order_with_recovery(base, path, params or {}, ignore_stop_event=ignore_stop_event)
        result = self._run_signed_request(
            base, method, path, params, timeout=15,
            max_retries=self.DEFAULT_REQUEST_RETRY_COUNT, ignore_stop_event=ignore_stop_event,
        )
        if path in self.ORDER_PATHS and isinstance(result, dict):
            self.record_order_state(path, result, request_params=params)
        return result

    @staticmethod
    def _is_uncertain_write_error(exc: Exception) -> bool:
        if isinstance(exc, BinanceAPIError):
            return exc.code in {-1001, -1006, -1007, -1008} or (exc.status_code or 0) >= 500
        return isinstance(exc, (requests.exceptions.RequestException, ConnectionError, TimeoutError, ValueError))

    def _run_signed_request(self, base, method, path, params, *, timeout, max_retries, ignore_stop_event=False):
        continuous = bool(
            getattr(self, "_continuous_trading", False)
            and not getattr(self, "_bounded_request_depth", 0)
            and not getattr(self, "_cleanup_request_depth", 0)
            and not ignore_stop_event
        )
        attempt = 0
        while True:
            try:
                return self._run_signed_request_once(
                    base, method, path, params, timeout=timeout,
                    max_retries=1 if continuous and (method != "POST" or path in self.REPLAY_SAFE_POST_PATHS) else max_retries,
                    ignore_stop_event=ignore_stop_event,
                )
            except Exception as exc:
                replay_safe = method != "POST" or path in self.REPLAY_SAFE_POST_PATHS
                missing_order = method == "GET" and path in self.ORDER_PATHS and isinstance(exc, BinanceAPIError) and exc.code == -2013
                if not continuous or not replay_safe or not (self.is_temporary_request_error(exc) or missing_order or isinstance(exc, ValueError)):
                    raise
                attempt += 1
                self._wait_for_recovery(path, attempt, exc)

    def _run_signed_request_once(self, base, method, path, params, *, timeout, max_retries, ignore_stop_event=False):
        replay_safe = method != "POST" or path in self.REPLAY_SAFE_POST_PATHS
        if not replay_safe and getattr(self, "_uncertain_write_error", None) is not None:
            raise self._uncertain_write_error
        try:
            return self._run_request_with_retry(
                lambda: self._request_impl(base, method, path, params, timeout=timeout),
                max_retries=max_retries,
                delay=self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS,
                base=base,
                path=path,
                ignore_stop_event=ignore_stop_event,
                replay_safe=replay_safe,
            )
        except UnconfirmedOperationError as exc:
            if path not in self.ORDER_PATHS:
                self._uncertain_write_error = exc
            raise
        except Exception as exc:
            if not replay_safe and self._is_uncertain_write_error(exc):
                uncertain = UnconfirmedOperationError(f"{path} 响应未确认，停止自动重发，请核对交易所状态")
                if path not in self.ORDER_PATHS:
                    self._uncertain_write_error = uncertain
                raise uncertain from exc
            raise

    def request_query(self, base, method, path, params=None, *, timeout: float = 6, max_retries: int | None = None):
        return self._run_signed_request(
            base, method, path, params, timeout=timeout,
            max_retries=self.DEFAULT_REQUEST_RETRY_COUNT if max_retries is None else max_retries,
        )

    def public_get(self, base, path, params=None):
        attempt = 0
        while True:
            try:
                return self.public_get_query(
                    base, path, params, timeout=15,
                    max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
                    delay=self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS,
                )
            except Exception as exc:
                if not getattr(self, "_continuous_trading", False) or getattr(self, "_bounded_request_depth", 0) or getattr(self, "_cleanup_request_depth", 0) or not (self.is_temporary_request_error(exc) or isinstance(exc, ValueError)):
                    raise
                attempt += 1
                self._wait_for_recovery(path, attempt, exc)

    def _public_get_impl(self, base, path, params=None, *, timeout: float = 15):
        params = params or {}
        url = base + path
        r = self.session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def public_get_query(self, base, path, params=None, *, timeout: float = 6, max_retries: int | None = None, delay: float | None = None):
        return self._run_request_with_retry(
            lambda: self._public_get_impl(base, path, params, timeout=timeout),
            max_retries=self.DEFAULT_REQUEST_RETRY_COUNT if max_retries is None else max_retries,
            delay=self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS if delay is None else delay,
            base=base,
            path=path,
        )

    # -------- 余额 --------
    def spot_balance(self, asset: str) -> float:
        data = self.request(self.spot, "GET", "/api/v3/account")
        for b in data.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", 0))
        return 0.0

    def spot_all_balances(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        data = requester(self.spot, "GET", "/api/v3/account", {}, timeout=6, max_retries=1) if fast else requester(self.spot, "GET", "/api/v3/account")
        result = []
        for b in data.get("balances", []):
            free_amt = float(b.get("free", 0) or 0)
            locked_amt = float(b.get("locked", 0) or 0)
            total = free_amt + locked_amt
            if total > 0:
                result.append({
                    "asset": b.get("asset"),
                    "free": free_amt,
                    "locked": locked_amt,
                    "total": total,
                })
        return result

    def spot_balance_snapshot(self, assets: set[str] | list[str] | tuple[str, ...] | None = None) -> dict[str, dict[str, Decimal]]:
        requested_assets = {
            str(asset or "").strip().upper()
            for asset in (assets or ())
            if str(asset or "").strip()
        }
        data = self.request(self.spot, "GET", "/api/v3/account")
        result: dict[str, dict[str, Decimal]] = {}
        for item in data.get("balances", []):
            asset_u = str(item.get("asset") or "").strip().upper()
            if not asset_u or (requested_assets and asset_u not in requested_assets):
                continue
            free_amount = self._decimal_from_str(item.get("free", "0"), "0")
            locked_amount = self._decimal_from_str(item.get("locked", "0"), "0")
            result[asset_u] = {
                "free": free_amount,
                "locked": locked_amount,
                "total": free_amount + locked_amount,
            }
        for asset_u in requested_assets:
            result.setdefault(
                asset_u,
                {"free": Decimal("0"), "locked": Decimal("0"), "total": Decimal("0")},
            )
        return result

    def spot_trade_volume_summary(
        self,
        symbol: str,
        *,
        start_time_ms: int,
        end_time_ms: int,
        stop_event=None,
        valuation_price_lookup=None,
        quote_asset: str | None = None,
    ) -> dict[str, object]:
        symbol_u = self.normalize_trade_symbol(symbol)
        quote_asset_u = str(quote_asset or "").strip().upper() or self.get_spot_quote_asset(symbol_u)
        start_ms = int(start_time_ms)
        end_ms = int(end_time_ms)
        if not symbol_u:
            raise RuntimeError("现货交易对不能为空")
        if start_ms < 0 or end_ms < start_ms:
            raise RuntimeError("成交额查询时间范围无效")

        limit = 1000
        params: dict[str, object] = {
            "symbol": symbol_u,
            "startTime": start_ms,
            "limit": limit,
        }
        seen_trade_ids: set[str] = set()
        order_ids: set[str] = set()
        quote_volume = Decimal("0")
        buy_quote_volume = Decimal("0")
        sell_quote_volume = Decimal("0")
        commission_by_asset: dict[str, Decimal] = {}
        usdt_quote_volume = Decimal("0")
        usdt_buy_quote_volume = Decimal("0")
        usdt_sell_quote_volume = Decimal("0")
        usdt_commission = Decimal("0")
        unvalued_quote_volume_by_asset: dict[str, Decimal] = {}
        unvalued_commission_by_asset: dict[str, Decimal] = {}
        unvalued_hourly_amounts: dict[tuple[str, int], dict[str, Decimal]] = {}
        trade_count = 0
        last_trade_id: int | None = None

        def remember_unvalued(asset: str, timestamp: int, kind: str, amount: Decimal) -> None:
            if amount <= 0:
                return
            hour = timestamp - timestamp % 3_600_000
            amounts = unvalued_hourly_amounts.setdefault((asset, hour), {})
            amounts[kind] = amounts.get(kind, Decimal("0")) + amount

        def usdt_price(asset: str, trade_time: int) -> Decimal | None:
            asset_u = str(asset or "").strip().upper()
            if asset_u == "USDT":
                return Decimal("1")
            if valuation_price_lookup is None:
                return None
            try:
                price = Decimal(str(valuation_price_lookup(asset_u, trade_time) or "0"))
            except (InvalidOperation, TypeError, ValueError):
                return None
            return price if price.is_finite() and price > 0 else None

        while True:
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("成交额查询已停止")
            page = self.request(self.spot, "GET", "/api/v3/myTrades", params)
            if not isinstance(page, list):
                raise RuntimeError("成交明细返回格式异常")
            if not page:
                break

            reached_end = False
            for item in page:
                if not isinstance(item, dict):
                    continue
                trade_time = int(item.get("time", 0) or 0)
                if trade_time > end_ms:
                    reached_end = True
                    continue
                if trade_time < start_ms:
                    continue
                raw_trade_id = item.get("id")
                trade_id = str(raw_trade_id) if raw_trade_id is not None else ""
                if trade_id and trade_id in seen_trade_ids:
                    continue
                if trade_id:
                    seen_trade_ids.add(trade_id)
                quote_qty_raw = item.get("quoteQty")
                if quote_qty_raw in (None, ""):
                    quote_qty = self._decimal_from_str(item.get("price"), "0") * self._decimal_from_str(
                        item.get("qty"),
                        "0",
                    )
                else:
                    quote_qty = self._decimal_from_str(quote_qty_raw, "0")
                quote_qty = max(Decimal("0"), quote_qty)
                quote_volume += quote_qty
                if self._bool_from_value(item.get("isBuyer")):
                    buy_quote_volume += quote_qty
                else:
                    sell_quote_volume += quote_qty
                quote_usdt_price = usdt_price(quote_asset_u, trade_time)
                if quote_usdt_price is None:
                    if quote_qty > 0:
                        unvalued_quote_volume_by_asset[quote_asset_u] = (
                            unvalued_quote_volume_by_asset.get(quote_asset_u, Decimal("0")) + quote_qty
                        )
                    remember_unvalued(
                        quote_asset_u, trade_time,
                        "buy" if self._bool_from_value(item.get("isBuyer")) else "sell", quote_qty,
                    )
                else:
                    quote_usdt_value = quote_qty * quote_usdt_price
                    usdt_quote_volume += quote_usdt_value
                    if self._bool_from_value(item.get("isBuyer")):
                        usdt_buy_quote_volume += quote_usdt_value
                    else:
                        usdt_sell_quote_volume += quote_usdt_value
                commission_asset = str(item.get("commissionAsset") or "").strip().upper()
                commission = max(Decimal("0"), self._decimal_from_str(item.get("commission"), "0"))
                if commission_asset and commission > 0:
                    commission_by_asset[commission_asset] = (
                        commission_by_asset.get(commission_asset, Decimal("0")) + commission
                    )
                    commission_usdt_price = usdt_price(commission_asset, trade_time)
                    if commission_usdt_price is None:
                        unvalued_commission_by_asset[commission_asset] = (
                            unvalued_commission_by_asset.get(commission_asset, Decimal("0")) + commission
                        )
                        remember_unvalued(commission_asset, trade_time, "commission", commission)
                    else:
                        usdt_commission += commission * commission_usdt_price
                trade_count += 1
                raw_order_id = item.get("orderId")
                if raw_order_id not in (None, ""):
                    order_ids.add(str(raw_order_id))

            if reached_end or len(page) < limit:
                break
            page_trade_ids = [
                int(item["id"])
                for item in page
                if isinstance(item, dict) and item.get("id") not in (None, "")
            ]
            if not page_trade_ids:
                raise RuntimeError("成交明细缺少分页ID")
            next_trade_id = max(page_trade_ids) + 1
            if last_trade_id is not None and next_trade_id <= last_trade_id:
                raise RuntimeError("成交明细分页未向前推进")
            last_trade_id = next_trade_id
            params = {"symbol": symbol_u, "fromId": next_trade_id, "limit": limit}

        return {
            "symbol": symbol_u,
            "quoteAsset": quote_asset_u,
            "startTime": start_ms,
            "endTime": end_ms,
            "quoteVolume": quote_volume,
            "buyQuoteVolume": buy_quote_volume,
            "sellQuoteVolume": sell_quote_volume,
            "orderPnl": sell_quote_volume - buy_quote_volume,
            "commissionByAsset": commission_by_asset,
            "usdtQuoteVolume": usdt_quote_volume,
            "usdtBuyQuoteVolume": usdt_buy_quote_volume,
            "usdtSellQuoteVolume": usdt_sell_quote_volume,
            "usdtOrderPnl": usdt_sell_quote_volume - usdt_buy_quote_volume,
            "usdtCommission": usdt_commission,
            "unvaluedQuoteVolumeByAsset": unvalued_quote_volume_by_asset,
            "unvaluedCommissionByAsset": unvalued_commission_by_asset,
            "unvaluedHourlyAmounts": unvalued_hourly_amounts,
            "tradeCount": trade_count,
            "orderCount": len(order_ids),
        }

    @staticmethod
    def revalue_spot_trade_summary(summary: dict[str, object], hourly_rates) -> None:
        """补算缺价桶并移除已处理桶；重复调用不会重复累计，不改变原币种明细。"""
        pending = summary.get("unvaluedHourlyAmounts", {})
        for (asset, hour), amounts in list(pending.items()):
            price = BinanceClient.historical_usdt_price_lookup(hourly_rates, asset, hour)
            if price is None or not price.is_finite() or price <= 0:
                continue
            buy = amounts.get("buy", Decimal("0"))
            sell = amounts.get("sell", Decimal("0"))
            fee = amounts.get("commission", Decimal("0"))
            for key, amount in (
                ("usdtBuyQuoteVolume", buy), ("usdtSellQuoteVolume", sell),
                ("usdtQuoteVolume", buy + sell), ("usdtCommission", fee),
            ):
                summary[key] += amount * price
            for key, amount in (
                ("unvaluedQuoteVolumeByAsset", buy + sell),
                ("unvaluedCommissionByAsset", fee),
            ):
                remaining = summary[key]
                if amount > 0:
                    remaining[asset] -= amount
                    if remaining[asset] == 0:
                        del remaining[asset]
            del pending[(asset, hour)]
        summary["usdtOrderPnl"] = summary["usdtSellQuoteVolume"] - summary["usdtBuyQuoteVolume"]

    def query_total_wallet_balance(self, quote_asset="USDT", *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 8, "max_retries": 2} if fast else {}
        data = requester(
            self.spot,
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": quote_asset},
            **request_kwargs,
        )
        total = Decimal("0")
        rows = []
        for item in data:
            bal = Decimal(str(item.get("balance", "0")))
            wallet_name = item.get("walletName", "Unknown")
            active = bool(item.get("activate", False))
            rows.append({
                "walletName": wallet_name,
                "balance": bal,
                "activate": active,
            })
            total += bal
        return total, rows

    def get_simple_earn_flexible_positions(
        self,
        *,
        asset: str | None = None,
        size: int = 100,
    ) -> list[dict[str, Any]]:
        asset_u = str(asset or "").strip().upper()
        page_size = max(1, min(100, int(size or 100)))
        current = 1
        positions: list[dict[str, Any]] = []

        while True:
            params: dict[str, Any] = {
                "current": current,
                "size": page_size,
            }
            if asset_u:
                params["asset"] = asset_u

            data = self.request(
                self.spot,
                "GET",
                "/sapi/v1/simple-earn/flexible/position",
                params,
            )
            rows = []
            total = 0
            if isinstance(data, dict):
                rows = data.get("rows") or []
                try:
                    total = max(0, int(data.get("total", 0) or 0))
                except Exception:
                    total = 0
            elif isinstance(data, list):
                rows = data

            normalized_rows: list[dict[str, Any]] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                product_id = str(item.get("productId") or "").strip()
                row_asset = str(item.get("asset") or "").strip().upper()
                if asset_u and row_asset != asset_u:
                    continue
                total_amount = self._decimal_from_str(item.get("totalAmount", "0"), "0")
                redeeming_amount = self._decimal_from_str(item.get("redeemingAmount", "0"), "0")
                normalized_rows.append(
                    {
                        "productId": product_id,
                        "asset": row_asset,
                        "totalAmount": total_amount,
                        "redeemingAmount": redeeming_amount,
                        "canRedeem": self._bool_from_value(item.get("canRedeem")),
                        "autoSubscribe": self._bool_from_value(item.get("autoSubscribe")),
                    }
                )
            positions.extend(normalized_rows)

            if total > 0 and len(positions) >= total:
                break
            if len(rows) < page_size and total <= 0:
                break
            if not rows:
                break
            current += 1
            if current > 100:
                logger.warning("活期理财持仓分页超过 100 页，提前停止拉取")
                break

        return positions

    def get_simple_earn_flexible_products(
        self,
        *,
        asset: str | None = None,
        size: int = 100,
    ) -> list[dict[str, Any]]:
        asset_u = str(asset or "").strip().upper()
        page_size = max(1, min(100, int(size or 100)))
        current = 1
        products: list[dict[str, Any]] = []

        while True:
            params: dict[str, Any] = {
                "current": current,
                "size": page_size,
            }
            if asset_u:
                params["asset"] = asset_u

            data = self.request(
                self.spot,
                "GET",
                "/sapi/v1/simple-earn/flexible/list",
                params,
            )
            rows = []
            total = 0
            if isinstance(data, dict):
                rows = data.get("rows") or []
                try:
                    total = max(0, int(data.get("total", 0) or 0))
                except Exception:
                    total = 0
            elif isinstance(data, list):
                rows = data

            normalized_rows: list[dict[str, Any]] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                product_id = str(item.get("productId") or "").strip()
                row_asset = str(item.get("asset") or "").strip().upper()
                if asset_u and row_asset != asset_u:
                    continue
                min_purchase_amount = self._decimal_from_str(item.get("minPurchaseAmount", "0"), "0")
                normalized_rows.append(
                    {
                        "productId": product_id,
                        "asset": row_asset,
                        "canPurchase": self._bool_from_value(item.get("canPurchase")),
                        "isSoldOut": self._bool_from_value(item.get("isSoldOut")),
                        "status": str(item.get("status") or "").strip(),
                        "minPurchaseAmount": min_purchase_amount,
                    }
                )
            products.extend(normalized_rows)

            if total > 0 and len(products) >= total:
                break
            if len(rows) < page_size and total <= 0:
                break
            if not rows:
                break
            current += 1
            if current > 100:
                logger.warning("活期理财产品分页超过 100 页，提前停止拉取")
                break

        return products

    def find_simple_earn_flexible_product(self, asset: str, amount: Decimal | str | float | int | None = None) -> dict[str, Any]:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            raise RuntimeError("理财币种不能为空")
        amount_dec = self._decimal_from_str(amount, "0")
        products = self.get_simple_earn_flexible_products(asset=asset_u)
        candidates: list[dict[str, Any]] = []
        unavailable_found = False
        for item in products:
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            product_id = str(item.get("productId") or "").strip()
            if not product_id:
                continue
            can_purchase = item.get("canPurchase")
            is_sold_out = item.get("isSoldOut")
            if can_purchase is False or is_sold_out is True:
                unavailable_found = True
                continue
            candidates.append(item)

        if not candidates:
            if unavailable_found or products:
                raise RuntimeError(f"{asset_u} 活期理财当前不可申购")
            raise RuntimeError(f"未找到 {asset_u} 活期理财产品")

        product = candidates[0]
        min_purchase_amount = self._decimal_from_str(product.get("minPurchaseAmount", "0"), "0")
        if amount_dec > 0 and min_purchase_amount > 0 and amount_dec < min_purchase_amount:
            raise RuntimeError(
                f"{asset_u} 活期理财最小存入数量为 {self._format_decimal(min_purchase_amount)}"
            )
        return product

    def subscribe_simple_earn_flexible_product(
        self,
        product_id: str,
        amount: Decimal | str | float | int,
        *,
        source_account: str = "SPOT",
        auto_subscribe: bool = False,
    ) -> dict[str, Any]:
        product_id_text = str(product_id or "").strip()
        if not product_id_text:
            raise RuntimeError("活期理财 productId 为空")
        amount_dec = self._decimal_from_str(amount, "0")
        if amount_dec <= 0:
            raise RuntimeError("活期理财存入数量必须大于 0")

        params: dict[str, Any] = {
            "productId": product_id_text,
            "amount": self._format_decimal(amount_dec),
            "autoSubscribe": "true" if bool(auto_subscribe) else "false",
        }
        source_account_text = str(source_account or "").strip().upper()
        if source_account_text:
            params["sourceAccount"] = source_account_text

        result = self.request(
            self.spot,
            "POST",
            "/sapi/v1/simple-earn/flexible/subscribe",
            params,
        )
        if isinstance(result, dict) and str(result.get("success", "")).strip().lower() == "false":
            raise RuntimeError("活期理财存入返回失败")
        return result

    def redeem_simple_earn_flexible_product(
        self,
        product_id: str,
        *,
        redeem_all: bool = True,
        amount: Decimal | str | float | int | None = None,
        dest_account: str = "SPOT",
    ) -> dict[str, Any]:
        product_id_text = str(product_id or "").strip()
        if not product_id_text:
            raise RuntimeError("活期理财 productId 为空")

        params: dict[str, Any] = {
            "productId": product_id_text,
            "redeemAll": "true" if redeem_all else "false",
        }
        dest_account_text = str(dest_account or "").strip().upper()
        if dest_account_text:
            params["destAccount"] = dest_account_text

        if not redeem_all:
            amount_dec = self._decimal_from_str(amount, "0")
            if amount_dec <= 0:
                raise RuntimeError("活期理财部分赎回 amount 必须大于 0")
            params["amount"] = self._format_decimal(amount_dec)

        return self.request(
            self.spot,
            "POST",
            "/sapi/v1/simple-earn/flexible/redeem",
            params,
        )

    def set_simple_earn_flexible_auto_subscribe(
        self,
        product_id: str,
        auto_subscribe: bool,
    ) -> dict[str, Any]:
        product_id_text = str(product_id or "").strip()
        if not product_id_text:
            raise RuntimeError("活期理财 productId 为空")
        return self.request(
            self.spot,
            "POST",
            "/sapi/v1/simple-earn/flexible/setAutoSubscribe",
            {
                "productId": product_id_text,
                "autoSubscribe": "true" if bool(auto_subscribe) else "false",
            },
        )

    def query_asset_balances_breakdown(self, *, fast: bool = False) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}

        def add_amount(asset: str, amount) -> None:
            asset_u = str(asset or "").strip().upper()
            if not asset_u:
                return
            amount_dec = Decimal(str(amount or "0"))
            if amount_dec <= 0:
                return
            totals[asset_u] = totals.get(asset_u, Decimal("0")) + amount_dec

        try:
            for item in self.spot_all_balances(fast=fast):
                add_amount(item.get("asset", ""), item.get("total", 0))
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询现货资产明细失败: %s", e)

        try:
            for item in self.funding_positive_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("free", 0))
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询资金账户资产明细失败: %s", e)

        try:
            for item in self.um_futures_transferable_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询 U本位资产明细失败: %s", e)

        try:
            for item in self.cm_futures_transferable_assets(fast=fast):
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询 币本位资产明细失败: %s", e)

        return totals

    # -------- 工具：从 symbol 推断现货基础币种 --------
    @staticmethod
    def normalize_trade_symbol(symbol: str) -> str:
        symbol_u = str(symbol or "").strip().upper()
        if "/" not in symbol_u:
            return symbol_u
        parts = [part.strip() for part in symbol_u.split("/") if part.strip()]
        if len(parts) == 2 and all(re.fullmatch(r"[A-Z0-9]+", part) for part in parts):
            return "".join(parts)
        return symbol_u

    @staticmethod
    def split_spot_symbol(symbol: str) -> tuple[str, str]:
        symbol_u = BinanceClient.normalize_trade_symbol(symbol)
        for suffix in SUPPORTED_QUOTE_ASSET_SUFFIXES:
            if symbol_u.endswith(suffix) and len(symbol_u) > len(suffix):
                return symbol_u[:-len(suffix)], suffix
        return "", ""

    @classmethod
    def _validated_spot_symbol_parts(cls, symbol: str) -> tuple[str, str, str]:
        symbol_u = cls.normalize_trade_symbol(symbol)
        if not symbol_u:
            raise RuntimeError("现货交易对不能为空")
        base_asset, quote_asset = cls.split_spot_symbol(symbol_u)
        if not base_asset or not quote_asset:
            raise RuntimeError(f"现货交易对格式不正确：{symbol_u}")
        return symbol_u, base_asset, quote_asset

    @classmethod
    def get_spot_base_asset(cls, symbol: str) -> str:
        _symbol_u, base_asset, _quote_asset = cls._validated_spot_symbol_parts(symbol)
        return base_asset

    @classmethod
    def get_spot_quote_asset(cls, symbol: str) -> str:
        _symbol_u, _base_asset, quote_asset = cls._validated_spot_symbol_parts(symbol)
        return quote_asset

    def ensure_spot_symbol_supported(self, symbol: str) -> tuple[str, str]:
        symbol_u, base_asset, quote_asset = self._validated_spot_symbol_parts(symbol)
        try:
            info = self.get_exchange_info(symbol_u)
        except Exception as exc:
            raise RuntimeError(f"现货交易对校验失败：{symbol_u}") from exc
        if not info:
            raise RuntimeError(f"找不到现货交易对：{symbol_u}")
        if str(info.get("status") or "").strip().upper() != "TRADING":
            raise RuntimeError(f"现货交易对不可交易：{symbol_u}")
        return base_asset, quote_asset

    def _spot_market_conversion_routes(
        self,
    ) -> tuple[dict[str, dict[str, str]], dict[str, list[dict[str, str]]]]:
        client_type = type(self)
        with self._shared_spot_market_route_lock:
            direct_cached = getattr(client_type, "_shared_spot_market_direct_routes", None)
            reverse_cached = getattr(client_type, "_shared_spot_market_reverse_routes", None)
            if direct_cached is not None and reverse_cached is not None:
                return direct_cached, reverse_cached

            data = self.public_get(
                self.spot,
                "/api/v3/exchangeInfo",
                {"permissions": "SPOT", "showPermissionSets": "false"},
            )
            direct_routes: dict[str, dict[str, str]] = {}
            reverse_routes: dict[str, list[dict[str, str]]] = {}
            for item in data.get("symbols", []) if isinstance(data, dict) else []:
                if not isinstance(item, dict):
                    continue
                symbol_u = str(item.get("symbol") or "").strip().upper()
                base_asset = str(item.get("baseAsset") or "").strip().upper()
                quote_asset = str(item.get("quoteAsset") or "").strip().upper()
                order_types = {str(value or "").strip().upper() for value in item.get("orderTypes", [])}
                if (
                    not symbol_u
                    or not base_asset
                    or not quote_asset
                    or str(item.get("status") or "").strip().upper() != "TRADING"
                    or item.get("isSpotTradingAllowed") is False
                    or "MARKET" not in order_types
                ):
                    continue

                direct_routes[symbol_u] = {
                    "symbol": symbol_u,
                    "fromAsset": base_asset,
                    "toAsset": quote_asset,
                    "side": "SELL",
                }
                if self._bool_from_value(item.get("quoteOrderQtyMarketAllowed")):
                    reverse_key = quote_asset + base_asset
                    reverse_routes.setdefault(reverse_key, []).append(
                        {
                            "symbol": symbol_u,
                            "fromAsset": quote_asset,
                            "toAsset": base_asset,
                            "side": "BUY",
                        }
                    )

            client_type._shared_spot_market_direct_routes = direct_routes
            client_type._shared_spot_market_reverse_routes = reverse_routes
            return direct_routes, reverse_routes

    def resolve_spot_market_conversion(self, symbol: str) -> dict[str, str]:
        direction_key = self.normalize_trade_symbol(symbol)
        if not direction_key:
            raise RuntimeError("现货0轮兑换方向不能为空")

        direct_routes, reverse_routes = self._spot_market_conversion_routes()
        direct_route = direct_routes.get(direction_key)
        if direct_route is not None:
            return dict(direct_route)

        reverse_candidates = reverse_routes.get(direction_key, [])
        if len(reverse_candidates) == 1:
            return dict(reverse_candidates[0])
        if len(reverse_candidates) > 1:
            raise RuntimeError(f"现货0轮兑换方向存在歧义：{direction_key}")
        raise RuntimeError(f"找不到支持市价0轮兑换的方向：{direction_key}")

    # -------- 公共行情 / 交易规则 --------
    def get_spot_symbol_catalog(self) -> list[dict[str, str]]:
        data = self.public_get(
            self.spot,
            "/api/v3/exchangeInfo",
            {"permissions": "SPOT", "showPermissionSets": "false"},
        )
        symbols = data.get("symbols", []) if isinstance(data, dict) else []
        normalized: list[dict[str, str]] = []
        for item in symbols:
            if not isinstance(item, dict) or item.get("isSpotTradingAllowed") is False:
                continue
            item_symbol = self.normalize_trade_symbol(item.get("symbol"))
            base_asset = str(item.get("baseAsset") or "").strip().upper()
            quote_asset = str(item.get("quoteAsset") or "").strip().upper()
            if not item_symbol or not base_asset or not quote_asset:
                continue
            info = {
                "symbol": item_symbol,
                "baseAsset": base_asset,
                "quoteAsset": quote_asset,
                "status": str(item.get("status") or "").strip().upper(),
            }
            normalized.append(info)
        return normalized

    def get_spot_symbols_for_base_asset(
        self,
        symbol: str,
        *,
        spot_symbols: list[dict[str, str]] | None = None,
    ) -> tuple[str, list[dict[str, str]]]:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            raise RuntimeError("现货交易对不能为空")
        catalog = spot_symbols if spot_symbols is not None else self.get_spot_symbol_catalog()
        selected_info = next(
            (item for item in catalog if str(item.get("symbol") or "").strip().upper() == symbol_u),
            None,
        )
        if selected_info is None:
            raise RuntimeError(f"找不到现货交易对：{symbol_u}")

        base_asset = selected_info["baseAsset"]
        matched = [
            item
            for item in catalog
            if item["baseAsset"] == base_asset
            and item["quoteAsset"] in MONTHLY_VOLUME_QUOTE_ASSETS
        ]
        matched.sort(key=lambda item: (item["symbol"] != symbol_u, item["symbol"]))
        return base_asset, matched

    def get_historical_usdt_hourly_rates(
        self,
        assets: set[str] | list[str] | tuple[str, ...],
        *,
        start_time_ms: int,
        end_time_ms: int,
        spot_symbols: list[dict[str, str]],
        stop_event=None,
        max_retries: int | None = None,
    ) -> dict[str, dict[int, Decimal]]:
        start_ms = int(start_time_ms)
        end_ms = int(end_time_ms)
        if start_ms < 0 or end_ms < start_ms:
            raise RuntimeError("历史价格查询时间范围无效")

        hour_ms = 60 * 60 * 1000
        first_hour_ms = start_ms - (start_ms % hour_ms)
        symbol_map = {
            str(item.get("symbol") or "").strip().upper(): item
            for item in spot_symbols
            if isinstance(item, dict) and str(item.get("symbol") or "").strip()
        }
        rates: dict[str, dict[int, Decimal]] = {}
        for raw_asset in assets:
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("成交额查询已停止")
            asset = str(raw_asset or "").strip().upper()
            if not asset:
                continue
            if asset == "USDT":
                rates[asset] = {}
                continue

            route_symbol = asset + "USDT"
            inverse = False
            if route_symbol not in symbol_map:
                route_symbol = "USDT" + asset
                inverse = True
            if route_symbol not in symbol_map:
                continue
            try:
                requester = self.public_get if max_retries is None else self.public_get_query
                rows = requester(
                    self.spot,
                    "/api/v3/klines",
                    {
                        "symbol": route_symbol,
                        "interval": "1h",
                        "startTime": first_hour_ms,
                        "endTime": end_ms,
                        "limit": 1000,
                    },
                    **({"timeout": 15, "max_retries": max_retries} if max_retries is not None else {}),
                )
            except InterruptedError:
                raise
            except Exception as exc:
                if stop_event is not None and stop_event.is_set():
                    raise InterruptedError("成交额查询已停止") from exc
                logger.warning("查询 %s 历史USDT价格失败，保留原币种明细: %s", asset, exc)
                continue

            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("成交额查询已停止")
            asset_rates: dict[int, Decimal] = {}
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, (list, tuple)) or len(row) < 5:
                    continue
                try:
                    open_time = int(row[0])
                    close_price = Decimal(str(row[4]))
                except (InvalidOperation, TypeError, ValueError):
                    continue
                if not close_price.is_finite() or close_price <= 0:
                    continue
                asset_rates[open_time] = (Decimal("1") / close_price) if inverse else close_price
            if asset_rates:
                rates[asset] = asset_rates
        return rates

    @staticmethod
    def historical_usdt_price_lookup(
        hourly_rates: dict[str, dict[int, Decimal]],
        asset: str,
        timestamp_ms: int,
    ) -> Decimal | None:
        asset_u = str(asset or "").strip().upper()
        if asset_u == "USDT":
            return Decimal("1")
        hour_ms = 60 * 60 * 1000
        bucket = int(timestamp_ms) - (int(timestamp_ms) % hour_ms)
        return hourly_rates.get(asset_u, {}).get(bucket)

    def get_exchange_info(self, symbol: str):
        symbol = self.normalize_trade_symbol(symbol)
        if symbol in self._exchange_info_cache:
            return self._exchange_info_cache[symbol]
        data = self.public_get(self.spot, "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = symbols[0]
        self._exchange_info_cache[symbol] = info
        return info

    def get_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol = self.normalize_trade_symbol(symbol)
        try:
            data = self.public_get(self.spot, "/api/v3/ticker/price", {"symbol": symbol})
            price = Decimal(str(data.get("price")))
            self._price_cache[symbol] = price
            return price
        except Exception:
            return None

    @staticmethod
    def _quantize_to_fraction(value: Decimal | str | float | int, fraction: int) -> Decimal:
        fraction_i = max(0, int(fraction or 0))
        step = Decimal("1").scaleb(-fraction_i)
        return Decimal(str(value)).quantize(step, rounding=ROUND_DOWN)

    def get_convert_pairs(self, from_asset: str | None = None, to_asset: str | None = None) -> list[dict[str, Any]]:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        cache_key = (from_asset_u, to_asset_u)
        params: dict[str, str] = {}
        if from_asset_u:
            params["fromAsset"] = from_asset_u
        if to_asset_u:
            params["toAsset"] = to_asset_u

        with self._shared_convert_cache_lock:
            if cache_key in self._shared_convert_exchange_info_cache:
                return list(self._shared_convert_exchange_info_cache[cache_key])
            data = self.public_get(self.spot, "/sapi/v1/convert/exchangeInfo", params)
            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])

            normalized: list[dict[str, Any]] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                normalized.append(
                    {
                        "fromAsset": str(item.get("fromAsset") or "").strip().upper(),
                        "toAsset": str(item.get("toAsset") or "").strip().upper(),
                        "fromAssetMinAmount": self._decimal_from_str(item.get("fromAssetMinAmount", "0"), "0"),
                        "fromAssetMaxAmount": self._decimal_from_str(item.get("fromAssetMaxAmount", "0"), "0"),
                        "toAssetMinAmount": self._decimal_from_str(item.get("toAssetMinAmount", "0"), "0"),
                        "toAssetMaxAmount": self._decimal_from_str(item.get("toAssetMaxAmount", "0"), "0"),
                    }
                )
            self._shared_convert_exchange_info_cache[cache_key] = list(normalized)
            return list(normalized)

    def get_convert_pair_info(self, from_asset: str, to_asset: str) -> dict[str, Any] | None:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        if not from_asset_u or not to_asset_u:
            return None
        for item in self.get_convert_pairs(from_asset_u, to_asset_u):
            if item.get("fromAsset") == from_asset_u and item.get("toAsset") == to_asset_u:
                return item
        return None

    def ensure_convert_symbol_supported(self, symbol: str) -> tuple[str, str]:
        _symbol_u, base_asset, quote_asset = self._validated_spot_symbol_parts(symbol)
        if not self.get_convert_pair_info(quote_asset, base_asset):
            raise RuntimeError(f"闪兑不支持 {quote_asset} -> {base_asset}")
        if not self.get_convert_pair_info(base_asset, quote_asset):
            raise RuntimeError(f"闪兑不支持 {base_asset} -> {quote_asset}")
        return base_asset, quote_asset

    def get_convert_asset_info(self) -> dict[str, int]:
        with self._shared_convert_cache_lock:
            cached = self._shared_convert_asset_info_cache
            if isinstance(cached, dict) and cached:
                return dict(cached)
            data = self.request(self.spot, "GET", "/sapi/v1/convert/assetInfo", {})
            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            result: dict[str, int] = {}
            for item in rows:
                if not isinstance(item, dict):
                    continue
                asset = str(item.get("asset") or "").strip().upper()
                if not asset:
                    continue
                try:
                    result[asset] = max(0, int(item.get("fraction", 8) or 8))
                except Exception:
                    result[asset] = 8
            self._shared_convert_asset_info_cache = dict(result)
            return dict(result)

    def get_convert_asset_fraction(self, asset: str) -> int:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return 8
        return int(self.get_convert_asset_info().get(asset_u, 8) or 8)

    def _normalize_convert_from_amount(self, from_asset: str, amount: Decimal | str | float | int) -> Decimal:
        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return Decimal("0")
        fraction = self.get_convert_asset_fraction(from_asset)
        return self._quantize_to_fraction(amount_dec, fraction)

    def _validate_convert_from_amount(
        self,
        from_asset: str,
        to_asset: str,
        amount: Decimal | str | float | int,
    ) -> tuple[Decimal, dict[str, Any]]:
        pair_info = self.get_convert_pair_info(from_asset, to_asset)
        if not pair_info:
            raise RuntimeError(f"闪兑不支持 {str(from_asset or '').upper()} -> {str(to_asset or '').upper()}")

        amount_dec = self._normalize_convert_from_amount(from_asset, amount)
        min_amount = Decimal(str(pair_info.get("fromAssetMinAmount", "0") or "0"))
        max_amount = Decimal(str(pair_info.get("fromAssetMaxAmount", "0") or "0"))

        if amount_dec <= 0:
            return Decimal("0"), pair_info
        if min_amount > 0 and amount_dec < min_amount:
            return Decimal("0"), pair_info
        if max_amount > 0 and amount_dec > max_amount:
            raise RuntimeError(
                f"闪兑 {str(from_asset or '').upper()} -> {str(to_asset or '').upper()} 金额过大："
                f"{self._format_decimal(amount_dec)} > 最大 {self._format_decimal(max_amount)}"
            )
        return amount_dec, pair_info

    def get_convert_quote(
        self,
        from_asset: str,
        to_asset: str,
        *,
        from_amount: Decimal | str | float | int | None = None,
        to_amount: Decimal | str | float | int | None = None,
        wallet_type: str = "SPOT",
        valid_time: str = "10s",
    ) -> dict[str, Any]:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        params: dict[str, Any] = {
            "fromAsset": from_asset_u,
            "toAsset": to_asset_u,
            "walletType": str(wallet_type or "SPOT").strip().upper(),
            "validTime": str(valid_time or "10s").strip(),
        }
        if from_amount is not None:
            params["fromAmount"] = self._format_decimal(Decimal(str(from_amount)))
        elif to_amount is not None:
            params["toAmount"] = self._format_decimal(Decimal(str(to_amount)))
        else:
            raise RuntimeError("闪兑报价必须指定 fromAmount 或 toAmount")
        return self.request(self.spot, "POST", "/sapi/v1/convert/getQuote", params)

    def accept_convert_quote(self, quote_id: str) -> dict[str, Any]:
        quote_id_text = str(quote_id or "").strip()
        if not quote_id_text:
            raise RuntimeError("闪兑 quoteId 为空")
        try:
            return self.request(self.spot, "POST", "/sapi/v1/convert/acceptQuote", {"quoteId": quote_id_text})
        except UnconfirmedOperationError as original:
            try:
                status = self.get_convert_order_status(quote_id=quote_id_text)
                if str(status.get("orderStatus") or "").upper() == "SUCCESS":
                    self._uncertain_write_error = None
                    return status
            except Exception:
                pass
            raise original

    def get_convert_order_status(self, *, order_id: str | int | None = None, quote_id: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if order_id is not None and str(order_id).strip():
            params["orderId"] = str(order_id).strip()
        if quote_id is not None and str(quote_id).strip():
            params["quoteId"] = str(quote_id).strip()
        if not params:
            raise RuntimeError("闪兑订单状态查询必须提供 orderId 或 quoteId")
        return self.request(self.spot, "GET", "/sapi/v1/convert/orderStatus", params)

    def wait_convert_order_success(
        self,
        *,
        order_id: str | int | None = None,
        quote_id: str | None = None,
        timeout_sec: float = 15.0,
        poll_interval: float = 0.4,
    ) -> dict[str, Any]:
        deadline = time.time() + max(1.0, float(timeout_sec))
        last_status = ""
        while time.time() < deadline:
            data = self.get_convert_order_status(order_id=order_id, quote_id=quote_id)
            status = str(data.get("orderStatus") or "").strip().upper()
            if status == "SUCCESS":
                return data
            if status in {"FAIL", "FAILED", "EXPIRED", "REJECTED", "CANCELED", "CANCELLED"}:
                raise RuntimeError(f"闪兑订单失败，状态={status}")
            last_status = status
            time.sleep(max(0.2, float(poll_interval)))
        if last_status:
            raise RuntimeError(f"闪兑订单等待超时，当前状态={last_status}")
        raise RuntimeError("闪兑订单等待超时")

    def convert_with_from_amount(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal | str | float | int,
        *,
        wallet_type: str = "SPOT",
        valid_time: str = "10s",
    ) -> dict[str, Any] | None:
        from_asset_u = str(from_asset or "").strip().upper()
        to_asset_u = str(to_asset or "").strip().upper()
        amount_dec, pair_info = self._validate_convert_from_amount(from_asset_u, to_asset_u, from_amount)
        if amount_dec <= 0:
            min_amount = Decimal(str(pair_info.get("fromAssetMinAmount", "0") or "0"))
            logger.info(
                "闪兑 %s -> %s 金额过小，跳过：%s < 最小 %s",
                from_asset_u,
                to_asset_u,
                self._format_decimal(self._normalize_convert_from_amount(from_asset_u, from_amount)),
                self._format_decimal(min_amount),
            )
            return None

        quote = self.get_convert_quote(
            from_asset_u,
            to_asset_u,
            from_amount=amount_dec,
            wallet_type=wallet_type,
            valid_time=valid_time,
        )
        quote_id = str(quote.get("quoteId") or "").strip()
        if not quote_id:
            raise RuntimeError(f"闪兑报价返回异常：{quote}")

        self.accept_convert_quote(quote_id)
        try:
            order = self.wait_convert_order_success(quote_id=quote_id)
        except Exception as exc:
            error = UnconfirmedOperationError(f"闪兑报价 {quote_id} 已接受，成交状态未确认，停止重新兑换：{exc}")
            self._uncertain_write_error = error
            raise error from exc
        logger.info(
            "闪兑完成 %s -> %s：支出=%s 到账=%s",
            from_asset_u,
            to_asset_u,
            self._format_decimal(self._decimal_from_str(order.get("fromAmount", amount_dec), "0")),
            self._format_decimal(self._decimal_from_str(order.get("toAmount", quote.get("toAmount", "0")), "0")),
        )
        return order

    def convert_quote_to_base_all(self, symbol: str, buffer=0.2) -> dict[str, Any] | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        base_asset = self.get_spot_base_asset(symbol_u)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.ensure_convert_symbol_supported(symbol_u)
        self.collect_asset_to_spot(quote_asset, context="交易前")

        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        buffer_dec = Decimal(str(buffer or "0"))
        amount = quote_balance - buffer_dec
        if amount <= 0:
            logger.info(
                "闪兑买入跳过：现货 %s 余额 %s <= 预留 %s",
                quote_asset,
                self._format_decimal(quote_balance),
                self._format_decimal(buffer_dec),
            )
            return None
        return self.convert_with_from_amount(quote_asset, base_asset, amount)

    def convert_base_to_quote_all(self, symbol: str) -> dict[str, Any] | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        base_asset = self.get_spot_base_asset(symbol_u)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.ensure_convert_symbol_supported(symbol_u)
        self.collect_asset_to_spot(base_asset, context="交易前")

        base_balance = self.spot_asset_balance_decimal(base_asset)
        if base_balance <= 0:
            logger.info("闪兑卖出跳过：现货 %s 余额为 0", base_asset)
            return None
        return self.convert_with_from_amount(base_asset, quote_asset, base_balance)

    @staticmethod
    def _extract_filter(symbol_info: dict, filter_type: str):
        for f in symbol_info.get("filters", []):
            if f.get("filterType") == filter_type:
                return f
        return None

    @staticmethod
    def _decimal_from_str(v, default="0"):
        try:
            value = Decimal(str(v))
            return value if value.is_finite() else Decimal(default)
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(default)

    @staticmethod
    def _floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return qty
        return (qty // step) * step

    @staticmethod
    def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        if value <= 0:
            return Decimal("0")
        units = (value / step).to_integral_value(rounding=ROUND_UP)
        return units * step

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        value = Decimal(str(value))
        if not value.is_finite():
            raise RuntimeError("数量或价格必须是有限数字")
        text = format(value.normalize(), "f")
        if text in {"-0", "-0.0"}:
            return "0"
        return text

    @staticmethod
    def _positive_int(value, default: int = 8) -> int:
        try:
            parsed = int(value)
        except Exception:
            return int(default)
        return parsed if parsed >= 0 else int(default)

    @staticmethod
    def _bool_from_value(value: object) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y"}

    def get_symbol_trade_rules(self, symbol: str):
        info = self.get_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")
        price_filter = self._extract_filter(info, "PRICE_FILTER")

        step_size = self._decimal_from_str((lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((lot or {}).get("maxQty", "999999999"), "999999999")
        tick_size = self._decimal_from_str((price_filter or {}).get("tickSize", "0.00000001"), "0.00000001")
        min_price = self._decimal_from_str((price_filter or {}).get("minPrice", "0"), "0")
        max_price = self._decimal_from_str((price_filter or {}).get("maxPrice", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("minNotional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        quote_precision_candidates = []
        for key in ("quoteAssetPrecision", "quotePrecision"):
            raw_value = info.get(key)
            try:
                parsed = int(raw_value)
            except Exception:
                continue
            if parsed >= 0:
                quote_precision_candidates.append(parsed)
        quote_order_qty_fraction = min(quote_precision_candidates) if quote_precision_candidates else 8

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "tickSize": tick_size,
            "minPrice": min_price,
            "maxPrice": max_price,
            "minNotional": min_notional_val,
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
            "quoteOrderQtyFraction": self._positive_int(quote_order_qty_fraction, 8),
        }

    def get_symbol_tick_size(self, symbol: str) -> Decimal:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return Decimal("0")
        tick_size = self._decimal_from_str(rules.get("tickSize", "0"), "0")
        return tick_size if tick_size > 0 else Decimal("0")

    def normalize_price_delta(self, symbol: str, delta: Decimal | str | float | int, *, min_one_tick: bool = True) -> Decimal:
        value = self._decimal_from_str(delta, "0")
        if value < 0:
            value = Decimal("0")
        tick_size = self.get_symbol_tick_size(symbol)
        if tick_size <= 0:
            return value
        if min_one_tick and value <= 0:
            return tick_size
        normalized = self._ceil_to_step(value, tick_size)
        if min_one_tick and normalized < tick_size:
            normalized = tick_size
        return normalized

    @staticmethod
    def _error_text_contains(exc: Exception, *snippets: str) -> bool:
        text = str(exc or "").lower()
        return any(str(snippet or "").lower() in text for snippet in snippets if snippet)

    def get_um_futures_exchange_info(self, symbol: str):
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        if symbol_u in self._um_futures_exchange_info_cache:
            return self._um_futures_exchange_info_cache[symbol_u]
        data = self.public_get(self.um_futures, "/fapi/v1/exchangeInfo", {})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = None
        for item in symbols:
            if str(item.get("symbol") or "").strip().upper() == symbol_u:
                info = item
                break
        if info is None:
            return None
        self._um_futures_exchange_info_cache[symbol_u] = info
        return info

    def ensure_um_futures_symbol_supported(self, symbol: str) -> dict[str, Any]:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            raise RuntimeError("合约交易对不能为空")
        info = self.get_um_futures_exchange_info(symbol_u)
        if not info:
            raise RuntimeError(f"找不到合约交易对：{symbol_u}")
        if str(info.get("status") or "").strip().upper() != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")
        return info

    def get_um_futures_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        try:
            data = self.public_get(self.um_futures, "/fapi/v1/ticker/price", {"symbol": symbol_u})
            price = Decimal(str(data.get("price")))
            self._um_futures_price_cache[symbol_u] = price
            return price
        except Exception:
            return None

    def get_um_futures_trade_rules(self, symbol: str):
        info = self.get_um_futures_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        market_lot = self._extract_filter(info, "MARKET_LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")
        price_filter = self._extract_filter(info, "PRICE_FILTER")

        market_lot = market_lot or lot or {}
        lot = lot or market_lot or {}
        price_filter = price_filter or {}

        step_size = self._decimal_from_str((market_lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((market_lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((market_lot or {}).get("maxQty", "999999999"), "999999999")
        tick_size = self._decimal_from_str((price_filter or {}).get("tickSize", "0.00000001"), "0.00000001")
        min_price = self._decimal_from_str((price_filter or {}).get("minPrice", "0"), "0")
        max_price = self._decimal_from_str((price_filter or {}).get("maxPrice", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("notional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "tickSize": tick_size,
            "minPrice": min_price,
            "maxPrice": max_price,
            "minNotional": min_notional_val,
            "quantityPrecision": max(0, int(info.get("quantityPrecision", 8) or 8)),
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
            "marginAsset": info.get("marginAsset"),
            "marketTakeBound": self._decimal_from_str(info.get("marketTakeBound", "0"), "0"),
        }

    def get_um_futures_margin_asset(self, symbol: str) -> str:
        symbol_u = self.normalize_trade_symbol(symbol)
        info = self.ensure_um_futures_symbol_supported(symbol_u)
        margin_asset = str(info.get("marginAsset") or info.get("quoteAsset") or "").strip().upper()
        if not margin_asset:
            raise RuntimeError(f"合约交易对保证金币种无效：{symbol_u}")
        return margin_asset

    def get_um_futures_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get(self.um_futures, "/fapi/v1/ticker/bookTicker", {"symbol": symbol_u})
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"读取合约盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }

    def get_um_futures_symbol_config(self, symbol: str) -> dict | None:
        symbol_u = self.normalize_trade_symbol(symbol)
        if not symbol_u:
            return None
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v1/symbolConfig",
            {"symbol": symbol_u},
        )
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict):
            return data
        return None

    def get_um_futures_position_mode(self) -> bool:
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v1/positionSide/dual",
            {},
        )
        return bool(data.get("dualSidePosition"))

    def ensure_um_futures_one_way_mode(self) -> bool:
        dual_mode = self.get_um_futures_position_mode()
        if not dual_mode:
            return False
        try:
            self.request(
                self.um_futures,
                "POST",
                "/fapi/v1/positionSide/dual",
                {"dualSidePosition": "false"},
            )
            logger.info("U本位合约已切换为单向仓模式")
            return True
        except Exception as exc:
            if self._error_text_contains(exc, "no need to change position side"):
                return False
            raise

    def ensure_um_futures_margin_type(self, symbol: str, margin_type: str) -> bool:
        symbol_u = self.normalize_trade_symbol(symbol)
        target_margin_type = str(margin_type or FUTURES_MARGIN_TYPE_DEFAULT).strip().upper()
        config = self.get_um_futures_symbol_config(symbol_u)
        current_margin_type = str((config or {}).get("marginType") or "").strip().upper()
        if current_margin_type == target_margin_type:
            return False
        try:
            self.request(
                self.um_futures,
                "POST",
                "/fapi/v1/marginType",
                {
                    "symbol": symbol_u,
                    "marginType": target_margin_type,
                },
            )
            logger.info("U本位合约 %s 保证金模式已设置为 %s", symbol_u, target_margin_type)
            return True
        except Exception as exc:
            if self._error_text_contains(exc, "no need to change margin type"):
                return False
            raise

    def ensure_um_futures_leverage(self, symbol: str, leverage: int) -> int:
        symbol_u = self.normalize_trade_symbol(symbol)
        leverage_i = int(leverage)
        if leverage_i < 1 or leverage_i > 125:
            raise RuntimeError("合约杠杆必须在 1-125 之间")
        config = self.get_um_futures_symbol_config(symbol_u)
        try:
            current_leverage = int((config or {}).get("leverage"))
        except Exception:
            current_leverage = 0
        if current_leverage == leverage_i:
            return leverage_i
        data = self.request(
            self.um_futures,
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol_u,
                "leverage": leverage_i,
            },
        )
        final_leverage = leverage_i
        try:
            final_leverage = int(data.get("leverage", leverage_i))
        except Exception:
            pass
        logger.info("U本位合约 %s 杠杆已设置为 %s", symbol_u, final_leverage)
        return final_leverage

    def um_futures_account_info(self) -> dict:
        return self.request(
            self.um_futures,
            "GET",
            "/fapi/v3/account",
            {},
        )

    def um_futures_asset_balance(self, asset: str) -> dict[str, Decimal]:
        asset_u = str(asset or "").strip().upper()
        info = self.um_futures_account_info()
        for item in info.get("assets", []):
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            return {
                "walletBalance": self._decimal_from_str(item.get("walletBalance", "0"), "0"),
                "availableBalance": self._decimal_from_str(item.get("availableBalance", "0"), "0"),
                "marginBalance": self._decimal_from_str(item.get("marginBalance", "0"), "0"),
                "crossWalletBalance": self._decimal_from_str(item.get("crossWalletBalance", "0"), "0"),
                "maxWithdrawAmount": self._decimal_from_str(item.get("maxWithdrawAmount", "0"), "0"),
            }
        return {
            "walletBalance": Decimal("0"),
            "availableBalance": Decimal("0"),
            "marginBalance": Decimal("0"),
            "crossWalletBalance": Decimal("0"),
            "maxWithdrawAmount": Decimal("0"),
        }

    def ensure_um_futures_asset_clear_before_trade(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        cleared_assets = getattr(self, "_um_futures_pretrade_cleared_assets", set())
        if asset_u in cleared_assets:
            return Decimal("0")

        def load_asset_balances() -> list[dict[str, object]]:
            info = self.um_futures_account_info()
            balances: list[dict[str, object]] = []
            for item in info.get("assets", []):
                balance_asset = str(item.get("asset") or "").strip().upper()
                if not balance_asset:
                    continue
                balances.append({
                    "asset": balance_asset,
                    "walletBalance": self._decimal_from_str(item.get("walletBalance", "0"), "0"),
                    "availableBalance": self._decimal_from_str(item.get("availableBalance", "0"), "0"),
                    "marginBalance": self._decimal_from_str(item.get("marginBalance", "0"), "0"),
                    "crossWalletBalance": self._decimal_from_str(item.get("crossWalletBalance", "0"), "0"),
                    "maxWithdrawAmount": self._decimal_from_str(item.get("maxWithdrawAmount", "0"), "0"),
                })
            return balances

        def has_actual_balance(balance: dict[str, object]) -> bool:
            # 多资产模式下 availableBalance 可能是其他抵押资产折算后的账户总可用余额。
            return any(
                Decimal(str(balance.get(name, "0"))) > 0
                for name in ("walletBalance", "marginBalance", "crossWalletBalance", "maxWithdrawAmount")
            )

        try:
            balances = load_asset_balances()
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            raise RuntimeError(f"合约有问题：查询 U本位全部资产余额失败: {e}") from e

        actual_balances = [balance for balance in balances if has_actual_balance(balance)]
        transferable: list[tuple[str, Decimal]] = []
        for balance in actual_balances:
            balance_asset = str(balance["asset"])
            max_withdraw_amount = Decimal(str(balance["maxWithdrawAmount"]))
            if max_withdraw_amount > 0:
                transferable.append((balance_asset, max_withdraw_amount))

        if not actual_balances:
            cleared_assets.add(asset_u)
            self._um_futures_pretrade_cleared_assets = cleared_assets
            logger.info("合约交易前检查：U本位无残留资金")
            return Decimal("0")

        transferred_total = Decimal("0")
        transfer_failures: list[str] = []
        for balance_asset, max_withdraw_amount in transferable:
            try:
                self._universal_transfer_with_retry(
                    "UMFUTURE_MAIN",
                    balance_asset,
                    max_withdraw_amount,
                    context="合约交易前 U本位清场",
                )
                transferred_total += max_withdraw_amount
                logger.info(
                    "合约交易前检测到 U本位 %s 余额，已先划转回现货：%s",
                    balance_asset,
                    self._format_decimal(max_withdraw_amount),
                )
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                transfer_failures.append(f"{balance_asset}: {e}")
                logger.error("合约交易前 U本位 %s 余额划回现货失败: %s", balance_asset, e)
        if transferable:
            time.sleep(0.5)

        try:
            remaining = [balance for balance in load_asset_balances() if has_actual_balance(balance)]
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            raise RuntimeError(f"合约有问题：划转后复查 U本位余额失败: {e}") from e
        if remaining:
            details = "，".join(
                f"{balance['asset']} wallet={self._format_decimal(Decimal(str(balance['walletBalance'])))}"
                for balance in remaining
            )
            if transfer_failures:
                details = f"{details}；划转失败={'；'.join(transfer_failures)}"
                raise RuntimeError(f"合约有问题：U本位资产划转后仍有余额：{details}")
            if any(Decimal(str(balance["maxWithdrawAmount"])) > 0 for balance in remaining):
                raise RuntimeError(f"合约有问题：U本位资产划转后仍有可划余额：{details}")

            probe_amount = Decimal("0.01")
            try:
                spot_balance = self.spot_asset_balance_decimal(asset_u)
                if spot_balance < probe_amount:
                    raise RuntimeError(
                        f"现货 {asset_u} 余额不足 {self._format_decimal(probe_amount)}，无法验证合约划转"
                    )
                self._universal_transfer_with_retry(
                    "MAIN_UMFUTURE",
                    asset_u,
                    probe_amount,
                    context="合约交易前划转探测转入",
                )
                time.sleep(0.5)
                self._universal_transfer_with_retry(
                    "UMFUTURE_MAIN",
                    asset_u,
                    probe_amount,
                    context="合约交易前划转探测转出",
                )
                time.sleep(0.5)
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                raise RuntimeError(
                    f"合约有问题：U本位存在不可划余额，且 {asset_u} 0.01 往返划转验证失败: {e}"
                ) from e
            logger.warning(
                "U本位存在不可划转余额（可能为合约赠金），%s 0.01 往返划转验证通过，继续交易：%s",
                asset_u,
                details,
            )

        cleared_assets.add(asset_u)
        self._um_futures_pretrade_cleared_assets = cleared_assets
        return transferred_total

    @staticmethod
    def _normalize_um_futures_position_side(position_side: str | None) -> str:
        value = str(position_side or "").strip().upper()
        return value if value in {"BOTH", "LONG", "SHORT"} else ""

    def get_um_futures_positions(self, symbol: str) -> list[dict]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v3/positionRisk",
            {"symbol": symbol_u},
        )
        items = data if isinstance(data, list) else [data]
        return [
            item
            for item in items
            if str(item.get("symbol") or "").strip().upper() == symbol_u
        ]

    def get_um_futures_position(self, symbol: str) -> dict | None:
        items = self.get_um_futures_positions(symbol)
        selected = None
        for item in items:
            position_side = self._normalize_um_futures_position_side(item.get("positionSide"))
            position_amt = self._decimal_from_str(item.get("positionAmt", "0"), "0")
            if position_side == "BOTH":
                return item
            if selected is None or position_amt != 0:
                selected = item
        return selected

    def calculate_um_futures_order_quantity(self, symbol: str, notional_amount, direction_side: str) -> Decimal:
        symbol_u = self.normalize_trade_symbol(symbol)
        amount = Decimal(str(notional_amount))
        if amount <= 0:
            raise RuntimeError("合约下单金额必须大于 0")

        rules = self.get_um_futures_trade_rules(symbol_u)
        if not rules:
            raise RuntimeError(f"找不到合约交易对规则：{symbol_u}")
        if rules["status"] != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")

        book_ticker = self.get_um_futures_book_ticker(symbol_u)
        side_u = str(direction_side or "").strip().upper()
        reference_price = Decimal(str(book_ticker["askPrice"])) if side_u == "BUY" else Decimal(str(book_ticker["bidPrice"]))
        if reference_price <= 0:
            raise RuntimeError(f"合约参考价格无效：{symbol_u}")

        effective_min_qty = rules["minQty"]
        if rules["minNotional"] > 0:
            effective_min_qty = max(
                effective_min_qty,
                self._ceil_to_step(rules["minNotional"] / reference_price, rules["stepSize"]),
            )
        effective_min_notional = effective_min_qty * reference_price

        qty = self._floor_to_step(amount / reference_price, rules["stepSize"])
        quantity_precision = int(rules.get("quantityPrecision", 8) or 8)
        if quantity_precision >= 0:
            qty = qty.quantize(Decimal("1").scaleb(-quantity_precision), rounding=ROUND_DOWN)
        if qty <= 0 or qty < rules["minQty"]:
            raise RuntimeError(
                f"{symbol_u} 合约下单数量过小：{self._format_decimal(qty)} < 最小数量 {self._format_decimal(rules['minQty'])}"
            )
        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if rules["minNotional"] > 0 and (qty * reference_price) < rules["minNotional"]:
            margin_asset = str(rules.get("marginAsset") or rules.get("quoteAsset") or "USDT").strip().upper()
            raise RuntimeError(
                f"{symbol_u} 当前市价下最小可下金额约为 {self._format_decimal(effective_min_notional)} {margin_asset}，"
                f"你填写的是 {self._format_decimal(amount)} {margin_asset}"
            )
        return qty

    def place_um_futures_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal | str | float | int,
        *,
        reduce_only: bool = False,
        position_side: str | None = None,
    ):
        symbol_u = self.normalize_trade_symbol(symbol)
        side_u = str(side or "").strip().upper()
        position_side_u = self._normalize_um_futures_position_side(position_side)
        rules = self.get_um_futures_trade_rules(symbol_u)
        if not rules:
            raise RuntimeError(f"找不到合约交易对规则：{symbol_u}")
        if rules["status"] != "TRADING":
            raise RuntimeError(f"合约交易对不可交易：{symbol_u}")

        qty_raw = Decimal(str(quantity))
        qty = self._floor_to_step(qty_raw, rules["stepSize"])
        quantity_precision = int(rules.get("quantityPrecision", 8) or 8)
        if quantity_precision >= 0:
            qty = qty.quantize(Decimal("1").scaleb(-quantity_precision), rounding=ROUND_DOWN)
        if qty <= 0 or qty < rules["minQty"]:
            raise RuntimeError(
                f"{symbol_u} 合约下单数量过小：{self._format_decimal(qty)} < 最小数量 {self._format_decimal(rules['minQty'])}"
            )
        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if not reduce_only:
            ref_price = self.get_um_futures_symbol_price(symbol_u)
            if ref_price and rules["minNotional"] > 0 and (qty * ref_price) < rules["minNotional"]:
                raise RuntimeError(
                    f"{symbol_u} 合约下单金额过小：{self._format_decimal(qty * ref_price)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
                )

        params = {
            "symbol": symbol_u,
            "side": side_u,
            "type": "MARKET",
            "quantity": self._format_decimal(qty),
            "newOrderRespType": "RESULT",
        }
        if position_side_u and position_side_u != "BOTH":
            params["positionSide"] = position_side_u
        if reduce_only:
            if position_side_u and position_side_u != "BOTH":
                raise RuntimeError("对冲模式平仓请使用 positionSide 定向平仓，不支持同时传 reduceOnly")
            params["reduceOnly"] = "true"

        data = self.request(
            self.um_futures,
            "POST",
            "/fapi/v1/order",
            params,
        )
        status = str(data.get("status") or "").upper()
        if status and status != "FILLED":
            raise BinanceAPIError(-2010, f"合约市价订单未完整成交，状态={status}")
        logger.info(
            "U本位合约市价下单 %s %s，数量=%s，positionSide=%s，reduceOnly=%s",
            symbol_u,
            side_u,
            params["quantity"],
            params.get("positionSide", "BOTH"),
            "true" if reduce_only else "false",
        )
        return data

    def close_all_um_futures_positions_market(self, symbol: str) -> list[dict]:
        close_orders = []
        for position in self.get_um_futures_positions(symbol):
            position_amt = self._decimal_from_str(position.get("positionAmt", "0"), "0")
            if position_amt == 0:
                continue

            position_side = self._normalize_um_futures_position_side(position.get("positionSide"))
            if position_side == "LONG":
                side = "SELL"
            elif position_side == "SHORT":
                side = "BUY"
            else:
                side = "SELL" if position_amt > 0 else "BUY"
                position_side = "BOTH"

            close_orders.append(
                self.place_um_futures_market_order(
                    symbol,
                    side,
                    abs(position_amt),
                    reduce_only=(position_side == "BOTH"),
                    position_side=position_side,
                )
            )
        return close_orders

    def close_um_futures_position_market(self, symbol: str):
        close_orders = self.close_all_um_futures_positions_market(symbol)
        return close_orders[0] if close_orders else None

    def _get_recent_minute_klines(self, symbol: str, count: int) -> tuple[str, int, list[dict[str, object]]]:
        count = int(count)
        if count < 1 or count > 70:
            raise ValueError("自动分仓分钟K线窗口必须在1至70分钟之间")
        symbol_u = self.normalize_trade_symbol(symbol)
        rows = self.public_get_query(
            self.spot, "/api/v3/klines",
            {"symbol": symbol_u, "interval": "1m", "limit": count + 1},
            timeout=4, max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
        )
        if not isinstance(rows, list):
            raise RuntimeError(f"{symbol_u} 分钟K线返回格式无效")
        now_ms = int(time.time() * 1000)
        result: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 8:
                raise RuntimeError(f"{symbol_u} 分钟K线字段不完整")
            open_ms = int(row[0])
            close_ms = int(row[6])
            if open_ms % 60000 or close_ms != open_ms + 59999:
                raise RuntimeError(f"{symbol_u} 分钟K线时间格式无效")
            high, low, volume = (Decimal(str(row[2])), Decimal(str(row[3])), Decimal(str(row[7])))
            if not all(value.is_finite() for value in (high, low, volume)) or low <= 0 or high < low or volume < 0:
                raise RuntimeError(f"{symbol_u} 分钟K线价格或成交额无效")
            result.append({"open_time": open_ms, "high": high, "low": low, "quote_volume": volume})
        return symbol_u, now_ms, result

    @staticmethod
    def _validate_recent_closed_klines(symbol_u: str, rows: list[dict[str, object]], count: int, now_ms: int) -> list[dict[str, object]]:
        if len(rows) < count:
            raise RuntimeError(f"{symbol_u} 已收盘分钟K线不足 {count} 根")
        result = rows[-count:]
        if any(result[i]["open_time"] - result[i - 1]["open_time"] != 60000 for i in range(1, len(result))):
            raise RuntimeError(f"{symbol_u} 分钟K线不连续")
        if now_ms - (int(result[-1]["open_time"]) + 60000) > 75000:
            raise RuntimeError(f"{symbol_u} 分钟K线已过期")
        return result

    def get_closed_minute_klines(self, symbol: str, count: int) -> list[dict[str, object]]:
        """Fetch closed, contiguous one-minute trade candles for auto ladder decisions."""
        symbol_u, now_ms, rows = self._get_recent_minute_klines(symbol, count)
        closed = [row for row in rows if row["open_time"] + 60000 <= now_ms]
        return self._validate_recent_closed_klines(symbol_u, closed, count, now_ms)

    def get_minute_klines_with_live(self, symbol: str, closed_count: int) -> tuple[list[dict[str, object]], dict[str, object]]:
        """Return recent closed minutes and the still-open current minute from one response."""
        symbol_u, now_ms, rows = self._get_recent_minute_klines(symbol, closed_count)
        current_open = (now_ms // 60000) * 60000
        live = next((row for row in reversed(rows) if row["open_time"] == current_open), None)
        if live is None:
            raise RuntimeError(f"{symbol_u} 当前分钟K线不可用")
        closed = [row for row in rows if row["open_time"] < current_open]
        recent_closed = self._validate_recent_closed_klines(symbol_u, closed, closed_count, now_ms)
        if live["open_time"] - recent_closed[-1]["open_time"] != 60000:
            raise RuntimeError(f"{symbol_u} 当前分钟K线与历史不连续")
        return recent_closed, live

    def get_last_spot_trade_time_ms(self, symbol: str) -> int:
        """avgPrice.closeTime is the latest trade time, independent of candle closure."""
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get_query(
            self.spot, "/api/v3/avgPrice", {"symbol": symbol_u},
            timeout=4, max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
        )
        if not isinstance(data, dict):
            raise RuntimeError(f"{symbol_u} 最近成交时间返回格式无效")
        try:
            trade_ms = int(data["closeTime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"{symbol_u} 最近成交时间无效") from exc
        if trade_ms <= 0:
            raise RuntimeError(f"{symbol_u} 最近成交时间无效")
        return trade_ms

    def get_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get(self.spot, "/api/v3/ticker/bookTicker", {"symbol": symbol_u})
        return self._parse_book_ticker(symbol_u, data)

    def get_observation_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        """Read the shared book with the same retry count as other market requests."""
        symbol_u = self.normalize_trade_symbol(symbol)
        data = self.public_get_query(
            self.spot, "/api/v3/ticker/bookTicker", {"symbol": symbol_u},
            timeout=4, max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
        )
        return self._parse_book_ticker(symbol_u, data)

    def _parse_book_ticker(self, symbol_u: str, data) -> dict[str, Decimal]:
        if not isinstance(data, dict):
            raise RuntimeError(f"读取盘口失败：{symbol_u} 返回格式无效")
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if not bid_price.is_finite() or not ask_price.is_finite() or bid_price <= 0 or ask_price < bid_price:
            raise RuntimeError(f"读取盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }

    def get_order_by_client_id(self, symbol: str, client_order_id: str, *, market: str = "spot", timeout: float = 6):
        futures = market == "um_futures"
        return self.request_query(
            self.um_futures if futures else self.spot, "GET", "/fapi/v1/order" if futures else "/api/v3/order",
            {"symbol": self.normalize_trade_symbol(symbol), "origClientOrderId": client_order_id},
            timeout=timeout, max_retries=1,
        )

    def _order_scope(self) -> str:
        from exchange_order_recovery import exchange_operation_scope
        return getattr(self, "operation_scope", "") or exchange_operation_scope(getattr(self, "key", ""))

    def record_order_state(self, path: str, order: dict, *, entry_id: str = "", request_params: dict | None = None) -> None:
        """Close journal entries at the same point an exchange terminal state is observed."""
        journal = getattr(self, "operation_journal", None)
        market = self.ORDER_PATHS[path]
        params = request_params or {}
        symbol = self.normalize_trade_symbol(order.get("symbol") or params.get("symbol") or "")
        order_id = str(order.get("orderId") or "")
        if not order_id:
            raise ValueError(f"{symbol} 订单回复缺少 orderId，保留恢复记录")
        entries = getattr(self, "_order_journal_entries", None)
        if entries is None:
            entries = self._order_journal_entries = {}
        key = (market, symbol, order_id)
        entry_id = entry_id or entries.get(key, "")
        if not entry_id:
            # An unconfirmed submit may first be resolved by a cleanup query.
            for client_id, payload in getattr(self, "_pending_order_requests", {}).items():
                if payload.get("market") == market and payload.get("symbol") == symbol and (
                    str(payload.get("order_id") or "") == order_id
                    or client_id == str(order.get("clientOrderId") or order.get("origClientOrderId") or "")
                ):
                    entry_id = str(payload.get("entry_id") or "")
                    break
        status = str(order.get("status") or "").upper()
        terminal = status in self.ORDER_TERMINAL_STATUSES
        if journal is not None and entry_id:
            journal.update(
                entry_id, "completed" if status == "FILLED" else ("cancelled" if terminal else "open"),
                order_id=order_id, exchange_status=status,
            )
        if terminal:
            entries.pop(key, None)
            for client_id, payload in list(getattr(self, "_pending_order_requests", {}).items()):
                if payload.get("market") == market and payload.get("symbol") == symbol and (
                    (entry_id and payload.get("entry_id") == entry_id)
                    or (order_id and str(payload.get("order_id") or "") == order_id)
                    or client_id == str(order.get("clientOrderId") or params.get("newClientOrderId") or "")
                ):
                    self._pending_order_requests.pop(client_id, None)
        elif order_id:
            if entry_id:
                entries[key] = entry_id
            for payload in getattr(self, "_pending_order_requests", {}).values():
                if (entry_id and payload.get("entry_id") == entry_id) or payload.get("client_order_id") == params.get("newClientOrderId"):
                    payload["order_id"] = order_id

    def pending_order_requests(self) -> list[dict]:
        return [dict(value) for value in getattr(self, "_pending_order_requests", {}).values()]

    def get_pending_order_for_cleanup(self, payload: dict):
        """Reconcile abandoned attempts; cancel by client ID before accepting absence."""
        market = payload.get("market", "spot")
        symbol = payload["symbol"]
        client_id = payload["client_order_id"]
        try:
            order = self.get_order_by_client_id(symbol, client_id, **({"market": market} if market != "spot" else {}))
        except BinanceAPIError as exc:
            if exc.code != -2013 or not payload.get("confirmation_timed_out"):
                raise
            path = "/fapi/v1/order" if market == "um_futures" else "/api/v3/order"
            base = self.um_futures if market == "um_futures" else self.spot
            try:
                order = self.request_query(base, "DELETE", path, {"symbol": symbol, "origClientOrderId": client_id}, timeout=6, max_retries=1)
            except BinanceAPIError as exc:
                if exc.code not in {-2011, -2013}:
                    raise
                journal = getattr(self, "operation_journal", None)
                if journal is not None:
                    journal.update(payload.get("entry_id", ""), "failed", reason="confirmation-timeout-and-cancel-not-found", exchange_error_code=exc.code)
                self._pending_order_requests.pop(client_id, None)
                logger.warning("超时旧单 %s 查询和撤单均返回不存在，按提交失败策略结束该记录", client_id)
                return None
        if not isinstance(order, dict) or not order.get("orderId") or not order.get("status"):
            raise RuntimeError(f"{symbol} 原订单 {client_id} 清理查询返回不完整")
        returned_client_id = str(order.get("origClientOrderId") or order.get("clientOrderId") or "")
        if returned_client_id and returned_client_id != client_id:
            raise RuntimeError(f"{symbol} 清理查询返回的订单标识与原订单不匹配")
        return order

    def _submit_order_with_recovery(self, base: str, path: str, params: dict, *, ignore_stop_event: bool = False, retry_guard=None):
        """Retry a submission after its 30-second confirmation grace expires."""
        previous_client_id = ""
        while True:
            self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
            if previous_client_id and not (ignore_stop_event or getattr(self, "_cleanup_request_depth", 0)):
                check = getattr(self, "_recovery_check", None)
                if callable(check):
                    check()
                self._wait_for_request_gate()
            try:
                return self._submit_order_attempt(
                    base, path, params, ignore_stop_event=ignore_stop_event,
                    retry_guard=retry_guard, previous_client_id=previous_client_id,
                )
            except _OrderConfirmationTimeout as exc:
                previous_client_id = exc.client_order_id
                logger.warning(
                    "%s 订单 %s 超过 %g 秒仍无法确认，本次提交尝试按失败处理，重新提交当前步骤；旧单仍保留核对记录，重提可能重复成交",
                    exc.symbol, exc.client_order_id, self.ORDER_SUBMISSION_CONFIRMATION_GRACE_SECONDS,
                )

    def _submit_order_attempt(self, base: str, path: str, params: dict, *, ignore_stop_event=False, retry_guard=None, previous_client_id=""):
        """Persist each distinct attempt before sending it."""
        self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
        if getattr(self, "_uncertain_write_error", None) is not None:
            raise self._uncertain_write_error
        for payload in self.pending_order_requests():
            if not payload.get("confirmation_timed_out") and (not payload.get("order_id") or payload.get("type") == "MARKET"):
                raise UnconfirmedOrderError(payload["symbol"], payload["client_order_id"], market=payload.get("market", "spot"), journal_entry_id=payload.get("entry_id", ""))
        symbol = self.normalize_trade_symbol(params.get("symbol") or "")
        market = self.ORDER_PATHS[path]
        client_order_id = "xja" + uuid.uuid4().hex
        params = {**params, "newClientOrderId": client_order_id}
        journal = getattr(self, "operation_journal", None)
        payload = {"market": market, "symbol": symbol, "client_order_id": client_order_id, "side": params.get("side", ""), "type": params.get("type", "")}
        if previous_client_id:
            payload["retry_of_client_order_id"] = previous_client_id
        payload.update({key: str(params[key]) for key in ("quantity", "quoteOrderQty", "price", "timeInForce", "positionSide", "reduceOnly") if key in params})
        entry_id = journal.begin("exchange-order", self._order_scope(), payload) if journal is not None else ""
        if not hasattr(self, "_pending_order_requests"):
            self._pending_order_requests = {}
        self._pending_order_requests[client_order_id] = {**payload, "entry_id": entry_id}
        def discard_unsent(reason):
            if journal is not None:
                journal.update(entry_id, "failed", reason=reason)
            self._pending_order_requests.pop(client_order_id, None)

        def accept(response):
            # Even a local journal write failure must not replay an accepted order.
            self._order_submission_serial = int(getattr(self, "_order_submission_serial", 0)) + 1
            self.record_order_state(path, response, entry_id=entry_id, request_params=params)
            if params.get("type") == "MARKET":
                return self._wait_submitted_market_order(base, path, params, response, ignore_stop_event=ignore_stop_event)
            return response

        submit_attempt = 0
        while True:
            try:
                self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
                if retry_guard is not None and not retry_guard():
                    discard_unsent("market-guard-before-submit")
                    raise RuntimeError("自动分仓提交前行情不再达标")
            except RequestStoppedError:
                discard_unsent("stopped-before-submit")
                raise
            except Exception:
                discard_unsent("pre-submit-check-failed")
                raise
            try:
                context = self.cleanup_requests_after_stop() if ignore_stop_event else nullcontext()
                with context:
                    response = self.request_query(base, "POST", path, params, timeout=15, max_retries=1)
            except RequestStoppedError:
                discard_unsent("stopped-before-submit")
                raise
            except Exception as exc:
                if self._order_definitely_not_accepted(exc, market):
                    submit_attempt += 1
                    self._reset_session_after_transport_error()
                    try:
                        self._wait_for_recovery(f"{symbol} 请求未被接收，重发原请求", submit_attempt, exc, ignore_stop_event=ignore_stop_event)
                    except RequestStoppedError:
                        discard_unsent("stopped-before-submit")
                        raise
                    continue
                if isinstance(exc, BinanceAPIError) and not self._is_uncertain_write_error(exc):
                    discard_unsent(f"exchange-rejected:{exc.code}")
                    raise
                # The matching engine may have accepted a request whose reply was lost.
                break
            else:
                if isinstance(response, dict) and response.get("orderId"):
                    return accept(response)
                break

        deadline = time.monotonic() + self.ORDER_SUBMISSION_CONFIRMATION_GRACE_SECONDS
        logger.warning("订单回复未确认，保留当前动作并在 %g 秒宽限内查询原订单：%s", self.ORDER_SUBMISSION_CONFIRMATION_GRACE_SECONDS, client_order_id)
        lookup = 0
        while True:
            self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if journal is not None:
                    journal.update(entry_id, "unconfirmed", confirmation_timed_out=True, reason="confirmation-timeout-resubmit")
                self._pending_order_requests[client_order_id]["confirmation_timed_out"] = True
                raise _OrderConfirmationTimeout(symbol, client_order_id, market=market, journal_entry_id=entry_id)
            try:
                context = self.cleanup_requests_after_stop() if ignore_stop_event else nullcontext()
                with context, self.bounded_requests():
                    found = self.get_order_by_client_id(symbol, client_order_id, timeout=min(6.0, remaining), **({"market": market} if market != "spot" else {}))
                if not isinstance(found, dict) or not found.get("orderId"):
                    raise ValueError("订单查询返回格式无效")
            except RequestStoppedError:
                # Leave the durable intent for stop cleanup; do not mark it rejected.
                raise
            except Exception as exc:
                missing = isinstance(exc, BinanceAPIError) and exc.code == -2013
                if not (missing or self.is_temporary_request_error(exc) or isinstance(exc, ValueError)):
                    raise UnconfirmedOrderError(symbol, client_order_id, market=market, journal_entry_id=entry_id) from exc
                lookup += 1
                self._wait_for_recovery(f"{symbol} 查询原订单 {client_order_id}", lookup, exc, ignore_stop_event=ignore_stop_event, deadline=deadline)
            else:
                return accept(found)

    def _wait_submitted_market_order(self, base, path, params, order, *, ignore_stop_event=False):
        attempt = 0
        while str(order.get("status") or "").upper() not in self.ORDER_TERMINAL_STATUSES:
            self._wait_for_request_gate(ignore_stop_event=ignore_stop_event)
            try:
                context = self.cleanup_requests_after_stop() if ignore_stop_event else nullcontext()
                with context, self.bounded_requests():
                    found = self.request_query(base, "GET", path, {"symbol": params["symbol"], "orderId": order["orderId"]}, timeout=6, max_retries=1)
                if not isinstance(found, dict) or not found.get("status") or str(found.get("orderId") or "") != str(order["orderId"]):
                    raise ValueError("订单成交状态返回格式无效")
                order = found
                self.record_order_state(path, order, request_params=params)
                if str(order.get("status") or "").upper() in self.ORDER_TERMINAL_STATUSES:
                    return order
                error = RuntimeError(f"订单状态={order.get('status')}")
            except RequestStoppedError:
                raise
            except Exception as exc:
                if not (self.is_temporary_request_error(exc) or isinstance(exc, ValueError) or isinstance(exc, BinanceAPIError) and exc.code == -2013):
                    raise UnconfirmedOrderError(params["symbol"], params["newClientOrderId"], market=self.ORDER_PATHS[path]) from exc
                error = exc
            attempt += 1
            self._wait_for_recovery(f"{params['symbol']} 等待原市价订单成交", attempt, error, ignore_stop_event=ignore_stop_event)
        return order

    def _place_limit_order_with_recovery(self, symbol: str, params: dict, retry_guard: Callable[[], bool] | None):
        return self._submit_order_with_recovery(self.spot, "/api/v3/order", {**params, "symbol": symbol}, retry_guard=retry_guard)

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Decimal,
        *,
        maker_only: bool = False,
        safe_retry: bool = False,
        retry_guard: Callable[[], bool] | None = None,
    ):
        symbol_u = self.normalize_trade_symbol(symbol)
        side_u = side.upper()
        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过挂单 %s", symbol_u)
            return None
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过挂单", symbol_u)
            return None

        qty = self._floor_to_step(Decimal(str(quantity)), rules["stepSize"])
        order_price = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0 and self._floor_to_step(order_price, tick_size) != order_price:
            raise RuntimeError(f"挂单价格不符合最小价格精度：{order_price}")

        if qty <= 0 or qty < rules["minQty"]:
            logger.info("交易对 %s 挂单数量过小，跳过", symbol_u)
            return None

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if rules["minNotional"] > 0 and (qty * order_price) < rules["minNotional"]:
            logger.info(
                "交易对 %s 挂单金额 %s 小于最小下单额 %s，跳过",
                symbol_u,
                self._format_decimal(qty * order_price),
                self._format_decimal(rules["minNotional"]),
            )
            return None

        params = {
            "symbol": symbol_u,
            "side": side_u,
            "type": "LIMIT_MAKER" if maker_only else "LIMIT",
            "quantity": self._format_decimal(qty),
            "price": self._format_decimal(order_price),
        }
        if not maker_only:
            params["timeInForce"] = "GTC"
        data = (
            self._place_limit_order_with_recovery(symbol_u, params, retry_guard)
            if safe_retry else self.request(self.spot, "POST", "/api/v3/order", params)
        )
        logger.info(
            "限价挂单 %s %s：数量=%s 价格=%s",
            symbol_u,
            side_u,
            params["quantity"],
            params["price"],
        )
        return data

    def get_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "GET",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
        )

    def cancel_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "DELETE",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
        )

    def get_order_for_cleanup(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "GET",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
            ignore_stop_event=True,
        )

    def cancel_order_for_cleanup(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "DELETE",
            "/api/v3/order",
            {
                "symbol": self.normalize_trade_symbol(symbol),
                "orderId": int(order_id),
            },
            ignore_stop_event=True,
        )

    def wait_order_filled(self, symbol: str, order_id: int | str, stop_event=None, poll_interval: float = 1.0):
        symbol_u = self.normalize_trade_symbol(symbol)
        failed_queries = 0
        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RequestStoppedError("收到停止信号，已停止等待挂单成交")

            try:
                order = self.get_order(symbol_u, order_id)
            except Exception as exc:
                if not (self.is_temporary_request_error(exc) or isinstance(exc, BinanceAPIError) and exc.code == -2013):
                    raise
                failed_queries += 1
                self._wait_for_recovery(f"{symbol_u} 查询订单 #{order_id}", failed_queries, exc)
                continue
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")
            time.sleep(max(0.2, float(poll_interval)))

    def _confirm_spot_market_order_filled(self, symbol: str, order_data: dict):
        if not isinstance(order_data, dict):
            raise RuntimeError("市价订单返回格式异常，无法确认成交")
        status = str(order_data.get("status") or "").strip().upper()
        if status == "FILLED":
            return order_data
        if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
            raise BinanceAPIError(-2010, f"市价订单未完整成交，状态={status}")
        order_id = order_data.get("orderId")
        if order_id in (None, ""):
            raise RuntimeError("市价订单返回缺少 orderId，无法确认成交")
        try:
            return self.wait_order_filled(symbol, order_id, stop_event=self._request_stop_event, poll_interval=0.2)
        except (BinanceAPIError, RequestStoppedError):
            raise
        except Exception as exc:
            client_id = str(order_data.get("clientOrderId") or f"order#{order_id}")
            raise UnconfirmedOrderError(symbol, client_id) from exc

    def get_order_average_price(self, order_data: dict) -> Decimal | None:
        if not isinstance(order_data, dict):
            return None
        try:
            executed_qty = Decimal(str(order_data.get("executedQty", "0")))
        except Exception:
            executed_qty = Decimal("0")
        try:
            quote_qty = Decimal(str(order_data.get("cummulativeQuoteQty", "0")))
        except Exception:
            quote_qty = Decimal("0")
        if executed_qty > 0 and quote_qty > 0:
            try:
                return quote_qty / executed_qty
            except Exception:
                pass
        try:
            price = Decimal(str(order_data.get("price", "0")))
        except Exception:
            price = Decimal("0")
        return price if price > 0 else None

    def spot_limit_buy_all_usdt(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        quote_asset = self.get_spot_quote_asset(symbol)
        self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = Decimal(str(self.spot_balance(quote_asset)))
        if quote_balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单买入", quote_asset)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单买入价格无效，跳过")
            return None

        amount_quote = quote_balance * Decimal(str(reserve_ratio))
        qty = self._floor_to_step(amount_quote / price_dec, rules["stepSize"])
        return self.place_limit_order(symbol, "BUY", qty, price_dec)

    def spot_limit_buy_quote_amount(
        self,
        symbol: str,
        price: Decimal,
        quote_amount: Decimal | str | float | int,
        *,
        maker_only: bool = False,
        safe_retry: bool = False,
        retry_guard: Callable[[], bool] | None = None,
    ):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        amount_quote = Decimal(str(quote_amount))
        if amount_quote <= 0:
            logger.info("挂单买入金额 <= 0，跳过 %s", symbol)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单买入价格无效，跳过")
            return None

        qty = self._floor_to_step(amount_quote / price_dec, rules["stepSize"])
        return self.place_limit_order(
            symbol, "BUY", qty, price_dec, maker_only=maker_only,
            safe_retry=safe_retry, retry_guard=retry_guard,
        )

    def spot_limit_sell_quantity(
        self,
        symbol: str,
        price: Decimal,
        quantity: Decimal | str | float | int,
        *,
        maker_only: bool = False,
        safe_retry: bool = False,
        retry_guard: Callable[[], bool] | None = None,
    ):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        qty = Decimal(str(quantity))
        if qty <= 0:
            logger.info("挂单卖出数量 <= 0，跳过 %s", symbol)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单卖出价格无效，跳过")
            return None

        qty = self._floor_to_step(qty, rules["stepSize"])
        return self.place_limit_order(
            symbol, "SELL", qty, price_dec, maker_only=maker_only,
            safe_retry=safe_retry, retry_guard=retry_guard,
        )

    def spot_buy_quote_amount(
        self,
        symbol: str,
        quote_amount: Decimal | str | float | int,
        *,
        assume_spot_funds_ready: bool = False,
        quote_asset_override: str = "",
    ):
        symbol_u = self.normalize_trade_symbol(symbol)
        amount = Decimal(str(quote_amount))
        if amount <= 0:
            logger.info("买入金额 <= 0，跳过买入 %s", symbol_u)
            return False

        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过买入 %s", symbol_u)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过买入", symbol_u)
            return False

        quote_asset = str(quote_asset_override or "").strip().upper()
        if not quote_asset:
            quote_asset = self.get_spot_quote_asset(symbol_u)
        rules_quote_asset = str(rules.get("quoteAsset") or "").strip().upper()
        if rules_quote_asset and quote_asset != rules_quote_asset:
            raise RuntimeError(
                f"{symbol_u} 后置币种不匹配：交易规则={rules_quote_asset}，下单资产={quote_asset}"
            )
        if not assume_spot_funds_ready:
            self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        quote_order_qty_fraction = int(rules.get("quoteOrderQtyFraction", 8) or 8)
        amount = self._quantize_to_fraction(amount, quote_order_qty_fraction)
        quote_balance = self._quantize_to_fraction(quote_balance, quote_order_qty_fraction)
        if quote_balance < amount:
            raise RuntimeError(
                f"现货 {quote_asset} 余额不足：需要 {self._format_decimal(amount)}，当前 {self._format_decimal(quote_balance)}"
            )
        if amount <= 0:
            logger.info("买入金额按精度截断后 <= 0，跳过买入 %s", symbol_u)
            return False

        if rules["minNotional"] > 0 and amount < rules["minNotional"]:
            raise RuntimeError(
                f"{symbol_u} 买入金额过小：{self._format_decimal(amount)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
            )

        order = self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol_u,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": self._format_decimal(amount),
                "newOrderRespType": "RESULT",
            },
        )
        self._confirm_spot_market_order_filled(symbol_u, order)
        logger.info("现货市价买入 %s，使用 %s 金额 %s", symbol_u, quote_asset, self._format_decimal(amount))
        return True

    def spot_market_convert_base_to_quote_all(self, symbol: str):
        route = self.resolve_spot_market_conversion(symbol)
        actual_symbol = route["symbol"]
        from_asset = route["fromAsset"]
        to_asset = route["toAsset"]
        side = route["side"]
        logger.info(
            "现货0轮方向解析：%s -> %s，实际交易对=%s，方向=%s",
            from_asset,
            to_asset,
            actual_symbol,
            side,
        )
        if side == "SELL":
            base_balance = self.spot_asset_balance_decimal(from_asset)
            if base_balance <= 0:
                logger.info("现货 %s 余额不足，跳过0轮市价兑换", from_asset)
                return False
            return self.sell_asset_market(actual_symbol, base_balance, reserve_ratio=Decimal("1"))

        self.collect_asset_to_spot(from_asset, context="交易前")
        quote_balance = self.spot_asset_balance_decimal(from_asset)
        if quote_balance <= 0:
            logger.info("现货 %s 余额不足，跳过0轮市价兑换", from_asset)
            return False
        return self.spot_buy_quote_amount(
            actual_symbol,
            quote_balance,
            assume_spot_funds_ready=True,
            quote_asset_override=from_asset,
        )

    def spot_market_conversion_sellable_source_amount(self, symbol: str) -> Decimal:
        """Confirm the input asset, including inverse routes, against market order minima."""
        route = self.resolve_spot_market_conversion(symbol)
        if route["side"] == "SELL":
            return self.spot_market_sellable_base_quantity(route["symbol"])
        asset = route["fromAsset"]
        balance = self.spot_balance_snapshot({asset})[asset]["total"]
        if balance <= 0:
            return Decimal("0")
        rules = self.get_symbol_trade_rules(route["symbol"])
        if not rules or rules.get("status") != "TRADING":
            raise RuntimeError(f"无法确认 {symbol} 清仓：交易对规则不可用或不可交易")
        amount = self._quantize_to_fraction(balance, int(rules.get("quoteOrderQtyFraction", 8) or 8))
        if amount <= 0 or amount < rules["minNotional"]:
            return Decimal("0")
        return amount

    def buy_bnb_with_quote_amount(
        self,
        quote_asset: str,
        amount: Decimal | str | float | int,
        *,
        assume_spot_funds_ready: bool = False,
        spot_balance_snapshot: Decimal | str | float | int | None = None,
        return_order: bool = False,
    ):
        quote_asset_u = str(quote_asset or "").strip().upper()
        if not quote_asset_u:
            raise RuntimeError("后置币种为空，无法预买 BNB")
        if quote_asset_u == "BNB":
            logger.info("后置币种为 BNB，无需预买 BNB")
            return None if return_order else False

        amount_dec = Decimal(str(amount or "0"))
        if amount_dec <= 0:
            logger.info("预买 BNB 金额 <= 0，跳过")
            return None if return_order else False

        if not assume_spot_funds_ready:
            self.collect_asset_to_spot(quote_asset_u, context="交易前")
        if spot_balance_snapshot is None:
            quote_balance = self.spot_asset_balance_decimal(quote_asset_u)
        else:
            quote_balance = Decimal(str(spot_balance_snapshot or "0"))
        if quote_balance < amount_dec:
            raise RuntimeError(
                f"现货 {quote_asset_u} 余额不足：需要 {self._format_decimal(amount_dec)}，当前 {self._format_decimal(quote_balance)}"
            )

        order = self.convert_with_from_amount(quote_asset_u, "BNB", amount_dec)
        if not order:
            logger.info(
                "闪兑预买 BNB 未执行：%s 金额 %s",
                quote_asset_u,
                self._format_decimal(amount_dec),
            )
            return None if return_order else False

        logger.info(
            "已通过闪兑预买 BNB：使用 %s 金额 %s",
            quote_asset_u,
            self._format_decimal(amount_dec),
        )
        return order if return_order else True

    def spot_limit_sell_all_base(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        base = self.get_spot_base_asset(symbol)
        balance = Decimal(str(self.spot_balance(base)))
        if balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单卖出", base)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单卖出价格无效，跳过")
            return None

        qty = self._floor_to_step(balance * Decimal(str(reserve_ratio)), rules["stepSize"])
        return self.place_limit_order(symbol, "SELL", qty, price_dec)

    def adjust_price_to_valid_tick(self, symbol: str, target_price: Decimal, *, round_up: bool = False) -> Decimal:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return Decimal(str(target_price))
        price = Decimal(str(target_price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size <= 0:
            return price
        normalized = self._floor_to_step(price, tick_size)
        if normalized == price:
            return price
        if round_up:
            return self._ceil_to_step(price, tick_size)
        return normalized

    def sell_asset_market(
        self,
        symbol: str,
        free_balance: Decimal | str | float | int,
        reserve_ratio=Decimal("0.999"),
        *,
        small_qty_log_text: str | None = None,
    ) -> bool:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            logger.info("找不到交易对规则，跳过卖出 %s", symbol)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过", symbol)
            return False

        qty = Decimal(str(free_balance)) * reserve_ratio
        qty = self._floor_to_step(qty, rules["stepSize"])

        if qty <= 0 or qty < rules["minQty"]:
            if small_qty_log_text:
                logger.info("%s", small_qty_log_text)
            else:
                logger.info("交易对 %s 数量过小，跳过卖出", symbol)
            return False

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol)
        if price:
            notional = qty * price
            if rules["minNotional"] > 0 and notional < rules["minNotional"]:
                logger.info(
                    "交易对 %s 名义价值 %s 小于最小下单额 %s，跳过",
                    symbol,
                    self._format_decimal(notional),
                    self._format_decimal(rules["minNotional"]),
                )
                return False

        order = self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty.normalize(), "f"),
                "newOrderRespType": "RESULT",
            },
        )
        self._confirm_spot_market_order_filled(symbol, order)
        logger.info("现货市价卖出 %s，数量 %s", symbol, format(qty.normalize(), "f"))
        return True

    def find_usdt_symbol_for_asset(self, asset: str) -> Optional[str]:
        asset = asset.upper()
        if asset in ("USDT", "BNB"):
            return None
        symbol = f"{asset}USDT"
        info = self.get_exchange_info(symbol)
        if info and info.get("status") == "TRADING":
            return symbol
        return None

    def sell_large_spot_assets_to_usdt(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        sold_assets = []
        balances = self.spot_all_balances()
        for item in balances:
            asset = (item.get("asset") or "").upper()
            free_amt = float(item.get("free", 0) or 0)

            if asset in skip_assets or free_amt <= 0:
                continue

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                logger.info("未找到 %sUSDT 交易对，跳过 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    sold_assets.append(asset)
                    time.sleep(0.3)
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                logger.warning("卖出大额币 %s 失败: %s", asset, e)

        logger.info("大额币卖出为 USDT 完成，成功处理 %d 个币种", len(sold_assets))
        return sold_assets

    def convert_large_spot_assets_to_bnb(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        direct_converted: list[str] = []
        usdt_fallback_assets: list[str] = []
        residual_assets: list[str] = []
        balances = self.spot_all_balances()

        for item in balances:
            asset = str(item.get("asset") or "").strip().upper()
            free_amt = self._decimal_from_str(item.get("free", "0"), "0")

            if asset in skip_assets or free_amt <= 0:
                continue

            direct_supported = bool(self.get_convert_pair_info(asset, "BNB"))
            if direct_supported:
                try:
                    order = self.convert_with_from_amount(asset, "BNB", free_amt)
                    if order:
                        direct_converted.append(asset)
                        logger.info("大额资产已直接闪兑为 BNB：%s", asset)
                        time.sleep(0.3)
                        continue
                    logger.info("大额资产 %s 直兑 BNB 未执行，回退到 USDT 路线", asset)
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning("大额资产 %s 直兑 BNB 失败，回退到 USDT 路线: %s", asset, e)
            else:
                logger.info("闪兑不支持 %s -> BNB，回退到 USDT 路线", asset)

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                residual_assets.append(asset)
                logger.warning("未找到 %sUSDT 交易对，暂时无法处理 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    usdt_fallback_assets.append(asset)
                    time.sleep(0.3)
                else:
                    residual_assets.append(asset)
                    logger.warning("大额资产 %s 回退卖出 USDT 未执行，该资产仍未处理", asset)
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                residual_assets.append(asset)
                logger.warning("大额资产 %s 回退卖出 USDT 失败: %s", asset, e)

        logger.info(
            "大额资产处理完成：直接闪兑BNB=%d，回退卖USDT=%d，残留=%d",
            len(direct_converted),
            len(usdt_fallback_assets),
            len(residual_assets),
        )
        return {
            "direct_bnb": direct_converted,
            "fallback_usdt": usdt_fallback_assets,
            "residual_assets": residual_assets,
        }

    # -------- 现货买入（全部 USDT） --------
    def spot_buy_all_usdt(self, buffer=0.2, symbol="BTCUSDT", *, assume_spot_funds_ready: bool = False):
        symbol_u = self.normalize_trade_symbol(symbol)
        quote_asset = self.get_spot_quote_asset(symbol_u)
        if not assume_spot_funds_ready:
            self.collect_asset_to_spot(quote_asset, context="交易前")
        quote_balance = self.spot_asset_balance_decimal(quote_asset)
        buffer_dec = Decimal(str(buffer or "0"))
        if quote_balance <= buffer_dec:
            logger.info(
                "现货 %s %s <= buffer %s，跳过买入",
                quote_asset,
                self._format_decimal(quote_balance),
                self._format_decimal(buffer_dec),
            )
            return False

        amount = (quote_balance - buffer_dec) * Decimal("0.999")
        if amount <= 0:
            logger.info("可用 %s 金额太小，跳过买入", quote_asset)
            return False

        return self.spot_buy_quote_amount(symbol_u, amount, assume_spot_funds_ready=True)

    # -------- 现货卖出（全部基础币） --------
    def spot_sell_all_base(self, symbol: str, *, small_qty_log_text: str | None = None):
        base = self.get_spot_base_asset(symbol)
        balance = self.spot_asset_balance_decimal(base)

        if balance <= 0:
            return False

        # 现货市价卖出按交易所 stepSize 取整，避免高价币因手填精度或额外预留被截到低于最小下单额。
        return self.sell_asset_market(
            symbol,
            balance,
            reserve_ratio=Decimal("1"),
            small_qty_log_text=small_qty_log_text,
        )

    def spot_market_sellable_base_quantity(self, symbol: str) -> Decimal:
        symbol_u = self.normalize_trade_symbol(symbol)
        base = self.get_spot_base_asset(symbol_u)
        snapshot = self.spot_balance_snapshot({base})
        balance = Decimal(str((snapshot.get(base) or {}).get("total", "0") or "0"))
        if balance <= 0:
            return Decimal("0")

        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules or rules.get("status") != "TRADING":
            raise RuntimeError(f"无法确认 {symbol_u} 清仓：交易对规则不可用或不可交易")

        qty = self._floor_to_step(balance, rules["stepSize"])
        if qty <= 0 or qty < rules["minQty"]:
            return Decimal("0")
        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol_u)
        if rules["minNotional"] > 0:
            if not price:
                raise RuntimeError(f"无法确认 {symbol_u} 清仓：行情不可用")
            if qty * price < rules["minNotional"]:
                return Decimal("0")
        return qty

    # -------- 提现 --------
    def withdraw_all_coin(
        self,
        coin: str,
        address: str,
        network: str,
        fee_buffer: float = WITHDRAW_FEE_BUFFER_DEFAULT,
        enable_withdraw: bool = True,
        auto_collect_to_spot: bool = False,
    ) -> float:
        coin = str(coin or "").strip().upper()
        if auto_collect_to_spot and coin:
            try:
                moved_count = self.collect_asset_to_spot(coin, context="提现前")
                if moved_count > 0:
                    # Give Binance a brief moment to reflect the transfer in spot balance.
                    time.sleep(0.5)
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                logger.warning("提现前归集 %s 到现货失败: %s", coin, e)
        balance = self.spot_asset_balance_decimal(coin)
        fee_buffer_dec = Decimal(str(fee_buffer or "0"))
        amount = balance - fee_buffer_dec
        if amount <= 0:
            logger.info(
                "%s 余额 %s 不足以提现（需 > buffer %s）",
                coin,
                self._format_decimal(balance),
                self._format_decimal(fee_buffer_dec),
            )
            return 0.0

        params = {
            "coin": coin,
            "address": address,
            "network": network,
            "amount": self._format_decimal(amount),
        }

        if enable_withdraw:
            self.request(
                self.spot,
                "POST",
                "/sapi/v1/capital/withdraw/apply",
                params,
            )
            logger.info(
                "已提交提现 %s %s → %s (%s)",
                self._format_decimal(amount),
                coin,
                address,
                network,
            )
            return float(amount)
        else:
            logger.info("提现开关关闭，仅打印参数: %s", params)
            return 0.0

    # -------- 现货正余额资产 --------
    def spot_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v3/asset/getUserAsset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- 资金账户正余额资产 --------
    def funding_positive_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
            **request_kwargs,
        )
        result = []
        for item in data:
            free_amt = self._decimal_from_str(item.get("free", "0"), "0")
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- U本位合约可转出余额 --------
    def funding_asset_balance(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
        )
        total = Decimal("0")
        for item in data:
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            total += self._decimal_from_str(item.get("free", "0"), "0")
        return total

    def collect_funding_asset_to_spot(
        self,
        asset: str,
        amount: Decimal | str | float | int | None = None,
    ) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        try:
            available_amount = self.funding_asset_balance(asset_u)
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询资金账户 %s 余额失败: %s", asset_u, e)
            return Decimal("0")
        if available_amount <= 0:
            return Decimal("0")
        transfer_amount = available_amount
        if amount is not None:
            requested_amount = self._decimal_from_str(amount, "0")
            if requested_amount <= 0:
                return Decimal("0")
            transfer_amount = min(available_amount, requested_amount)
        if transfer_amount <= 0:
            return Decimal("0")
        try:
            self._universal_transfer_with_retry("FUNDING_MAIN", asset_u, transfer_amount, context="资金账户划转到现货")
            logger.info("检测到资金账户 %s 余额，已自动划转到现货：%s", asset_u, self._format_decimal(transfer_amount))
            time.sleep(0.5)
            return transfer_amount
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("资金账户划转到现货失败 %s %s: %s", asset_u, self._format_decimal(transfer_amount), e)
            return Decimal("0")

    def ensure_bnb_fee_ready_in_spot(
        self,
        *,
        min_spot_balance: Decimal | str | float | int | None = None,
        max_transfer_amount: Decimal | str | float | int | None = None,
        timeout_seconds: float = 6.0,
    ) -> Decimal:
        asset_u = "BNB"
        deadline = time.monotonic() + max(0.5, float(timeout_seconds or 0))
        required_spot = self._decimal_from_str(min_spot_balance, "0") if min_spot_balance is not None else Decimal("0")
        if required_spot < 0:
            required_spot = Decimal("0")
        remaining_transfer_cap = (
            self._decimal_from_str(max_transfer_amount, "0") if max_transfer_amount is not None else None
        )
        if remaining_transfer_cap is not None and remaining_transfer_cap < 0:
            remaining_transfer_cap = Decimal("0")
        last_spot = Decimal("0")
        last_funding = Decimal("0")

        while True:
            last_spot = self.spot_asset_balance_decimal(asset_u)
            last_funding = self.funding_asset_balance(asset_u)
            if required_spot > 0 and last_spot >= required_spot:
                return last_spot
            if required_spot <= 0 and last_funding <= 0:
                return last_spot
            if last_funding <= 0:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "预买BNB后现货 BNB 余额仍不足："
                        f"现货={self._format_decimal(last_spot)}，"
                        f"目标={self._format_decimal(required_spot)}"
                    )
                time.sleep(0.4)
                continue

            transfer_amount = last_funding
            if required_spot > 0:
                missing_amount = required_spot - last_spot
                if missing_amount > 0:
                    transfer_amount = min(transfer_amount, missing_amount)
            if remaining_transfer_cap is not None:
                if remaining_transfer_cap <= 0:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            "预买BNB后检测到 BNB 仍停留在资金账户，但本次可划转额度已用尽："
                            f"现货={self._format_decimal(last_spot)}，"
                            f"资金={self._format_decimal(last_funding)}，"
                            f"目标={self._format_decimal(required_spot)}"
                        )
                    time.sleep(0.4)
                    continue
                transfer_amount = min(transfer_amount, remaining_transfer_cap)
            if transfer_amount <= 0:
                return last_spot

            moved_amount = self.collect_funding_asset_to_spot(asset_u, amount=transfer_amount)
            if remaining_transfer_cap is not None and moved_amount > 0:
                remaining_transfer_cap = max(Decimal("0"), remaining_transfer_cap - moved_amount)
            last_spot = self.spot_asset_balance_decimal(asset_u)
            last_funding = self.funding_asset_balance(asset_u)
            if required_spot > 0 and last_spot >= required_spot:
                logger.info(
                    "预买BNB后已确认手续费BNB位于现货账户：现货=%s，资金=%s，目标=%s",
                    self._format_decimal(last_spot),
                    self._format_decimal(last_funding),
                    self._format_decimal(required_spot),
                )
                return last_spot
            if required_spot <= 0 and last_funding <= 0:
                logger.info(
                    "预买BNB后已确认手续费BNB位于现货账户：现货=%s，资金=%s",
                    self._format_decimal(last_spot),
                    self._format_decimal(last_funding),
                )
                return last_spot

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "预买BNB后检测到 BNB 仍停留在资金账户，自动划转到现货失败："
                    f"现货={self._format_decimal(last_spot)}，"
                    f"资金={self._format_decimal(last_funding)}，"
                    f"目标={self._format_decimal(required_spot)}，"
                    f"本次划转={self._format_decimal(moved_amount)}"
                )
            time.sleep(0.4)

    def spot_asset_balance_decimal(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        data = self.request(self.spot, "GET", "/api/v3/account")
        for item in data.get("balances", []):
            if str(item.get("asset") or "").strip().upper() != asset_u:
                continue
            return self._decimal_from_str(item.get("free", "0"), "0")
        return Decimal("0")

    def transfer_spot_asset_to_um_futures(
        self,
        asset: str,
        amount: Decimal | str | float | int | None = None,
    ) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")

        # Reuse the existing funding->spot helper so futures mode can consume资金账户中的保证金币种。
        self.collect_funding_asset_to_spot(asset_u)

        try:
            spot_balance = self.spot_asset_balance_decimal(asset_u)
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询现货 %s 余额失败: %s", asset_u, e)
            return Decimal("0")
        if spot_balance <= 0:
            return Decimal("0")

        # 用户要求：现货中该保证金币种一旦存在，直接整笔划转到 U 本位，
        # 避免按差额补划时产生极小精度尾差，导致交易所拒绝划转。
        transfer_amount = Decimal(str(spot_balance))
        if transfer_amount <= 0:
            return Decimal("0")

        try:
            self._universal_transfer_with_retry("MAIN_UMFUTURE", asset_u, transfer_amount, context="现货划转到 U本位")
            logger.info("检测到现货 %s 余额，已自动划转到 U本位：%s", asset_u, self._format_decimal(transfer_amount))
            time.sleep(0.5)
            return transfer_amount
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("现货划转到 U本位失败 %s %s: %s", asset_u, self._format_decimal(transfer_amount), e)
            return Decimal("0")

    def um_futures_transferable_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.um_futures,
            "GET",
            "/fapi/v2/balance",
            {},
            **request_kwargs,
        )
        result = []
        for item in data:
            amt = self._decimal_from_str(item.get("maxWithdrawAmount", "0"), "0")
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 币本位合约可转出余额 --------
    def cm_futures_transferable_assets(self, *, fast: bool = False):
        requester = self.request_query if fast else self.request
        request_kwargs = {"timeout": 5, "max_retries": 1} if fast else {}
        data = requester(
            self.cm_futures,
            "GET",
            "/dapi/v1/balance",
            {},
            **request_kwargs,
        )
        result = []
        for item in data:
            amt = self._decimal_from_str(item.get("withdrawAvailable", "0"), "0")
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 通用划转 --------
    def universal_transfer(self, transfer_type: str, asset: str, amount: Decimal | str | float | int):
        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return False

        amt_str = format(amount_dec.normalize(), "f")
        if amt_str == "0":
            return False

        self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/transfer",
            {
                "type": transfer_type,
                "asset": asset,
                "amount": amt_str,
            },
        )
        logger.info("划转成功: %s %s %s", transfer_type, asset, amt_str)
        return True

    def _universal_transfer_with_retry(
        self,
        transfer_type: str,
        asset: str,
        amount: Decimal | str | float | int,
        *,
        context: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        retries = max(0, int(max_retries or 0))
        for attempt in range(retries + 1):
            try:
                return self.universal_transfer(transfer_type, asset, amount)
            except Exception as e:
                if isinstance(e, UnconfirmedOperationError):
                    raise
                # Definite temporary rejections can be retried without replaying a completed transfer.
                if isinstance(e, BinanceAPIError) and e.code not in {-1003, 32020}:
                    raise
                if attempt >= retries:
                    raise
                logger.warning(
                    "%s失败 %s %s（重试 %d/%d）: %s",
                    context,
                    str(asset or "").strip().upper(),
                    self._format_decimal(Decimal(str(amount))),
                    attempt + 1,
                    retries,
                    e,
                )
                time.sleep(max(0.0, float(retry_delay or 0)))
        return False

    def collect_asset_to_spot(self, asset: str, *, context: str = "提现前") -> int:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return 0
        context_text = str(context or "提现前").strip() or "提现前"

        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("UMFUTURE_MAIN", asset_u, amount, context=f"{context_text} U本位划转")
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning(
                        "%s U本位划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("%s查询 U本位 %s 余额失败: %s", context_text, asset_u, e)

        try:
            items = self.cm_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("CMFUTURE_MAIN", asset_u, amount, context=f"{context_text} 币本位划转")
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning(
                        "%s 币本位划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("%s查询 币本位 %s 余额失败: %s", context_text, asset_u, e)

        try:
            items = self.funding_positive_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = self._decimal_from_str(item.get("free", "0"), "0")
                if amount <= 0:
                    continue
                try:
                    self._universal_transfer_with_retry("FUNDING_MAIN", asset_u, amount, context=f"{context_text} 资金账户划转")
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning(
                        "%s 资金账户划转失败 %s %s: %s",
                        context_text,
                        asset_u,
                        self._format_decimal(amount),
                        e,
                    )
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("%s查询 资金账户 %s 余额失败: %s", context_text, asset_u, e)

        if total_count > 0:
            logger.info("%s归集 %s 到现货完成，共处理 %d 项", context_text, asset_u, total_count)
        return total_count

    # -------- 归集合约/资金到现货 --------
    def collect_all_to_spot(self):
        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            if items:
                logger.info("检测到 U本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning("U本位划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询 U本位合约余额失败: %s", e)

        try:
            items = self.cm_futures_transferable_assets()
            if items:
                logger.info("检测到 币本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("amount", "0"), "0")
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning("币本位划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询 币本位合约余额失败: %s", e)

        try:
            items = self.funding_positive_assets()
            if items:
                logger.info("检测到 资金账户可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = self._decimal_from_str(item.get("free", "0"), "0")
                try:
                    self.universal_transfer("FUNDING_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    if isinstance(e, UnconfirmedOperationError):
                        raise
                    logger.warning("资金账户划转失败 %s %s: %s", asset, self._format_decimal(amount), e)
        except Exception as e:
            if isinstance(e, UnconfirmedOperationError):
                raise
            logger.warning("查询资金账户余额失败: %s", e)

        logger.info("归集到现货完成，共处理 %d 项资产", total_count)
        return total_count

    # -------- 查询现货可小额兑换资产 --------
    def get_spot_dust_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust-btc",
            {"accountType": "SPOT"},
        )

        assets = []
        for item in data.get("details", []):
            asset = item.get("asset")
            amount_free = float(item.get("amountFree", 0) or 0)
            if asset and amount_free > 0 and asset != "BNB":
                assets.append(asset)
        return assets

    # -------- 小额兑换：现货可兑换资产 -> BNB --------
    def convert_spot_dust_to_bnb(self):
        assets = self.get_spot_dust_assets()
        if not assets:
            logger.info("没有可进行小额兑换的现货资产")
            return []

        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust",
            {
                "asset": assets,
                "accountType": "SPOT",
            },
        )
        result = data.get("transferResult", [])
        logger.info("小额兑换完成，兑换资产数: %d", len(result))
        return result


# ====================== 策略 ======================

__all__ = [
    "BinanceAPIError",
    "UnconfirmedOperationError",
    "UnconfirmedOrderError",
    "SharedRateLimitGate",
    "BinanceClient",
    "explain_binance_api_error",
    "short_binance_api_error",
    "summarize_exchange_exception",
]
