#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import *
from retry_utils import RetryPolicy, run_with_retries


class BinanceClientRequestMixin(object):
    DEFAULT_REQUEST_RETRY_COUNT = 10
    DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 1
    RETRYABLE_BINANCE_ERROR_CODES = {-1001, -1003, -2015}

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
    @classmethod
    def _request_timeout(cls, timeout: float | tuple[float, float] | None = None) -> tuple[float, float]:
        if isinstance(timeout, tuple):
            connect, read = timeout
            return (max(0.2, float(connect)), max(0.2, float(read)))
        read_timeout = cls.DEFAULT_READ_TIMEOUT_SECONDS if timeout is None else max(0.2, float(timeout))
        connect_timeout = min(cls.DEFAULT_CONNECT_TIMEOUT_SECONDS, read_timeout)
        return (connect_timeout, read_timeout)
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
        try:
            r = self.session.get(url, timeout=self._request_timeout(5))
        except requests.exceptions.RequestException:
            self._reset_session_after_transport_error()
            raise
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
    def _run_request_with_retry(self, request_fn, *, max_retries: int, delay: float):
        retry_count = max(1, int(max_retries or 1))
        retry_delay = max(0.0, float(delay or 0))

        def retryable_binance_error_label(error_code: int) -> str:
            if error_code == -2015:
                return "Binance API 权限校验失败"
            return "Binance 系统繁忙"

        def should_retry(exc: Exception) -> bool:
            if isinstance(exc, (requests.exceptions.RequestException, ConnectionError, TimeoutError)):
                self._reset_session_after_transport_error()
                return True
            if isinstance(exc, BinanceAPIError):
                return exc.code in getattr(self, "RETRYABLE_BINANCE_ERROR_CODES", {-1001, -1003, -2015})
            return False

        def on_retry(failed_attempt: int, total: int, exc: Exception) -> None:
            if isinstance(exc, BinanceAPIError):
                logger.warning(
                    f"{retryable_binance_error_label(exc.code)} ({failed_attempt}/{total}): {exc}，将在 {retry_delay:g} 秒后重试..."
                )
            else:
                logger.warning(f"网络请求失败 ({failed_attempt}/{total}): {exc}，将在 {retry_delay:g} 秒后重试...")

        def on_give_up(total: int, _exc: Exception) -> None:
            logger.error(f"重试 {total} 次后仍然失败。")

        return run_with_retries(
            request_fn,
            policy=RetryPolicy(attempts=retry_count, delay_seconds=retry_delay),
            should_retry=should_retry,
            on_retry=on_retry,
            on_give_up=on_give_up,
            sleep_fn=time.sleep,
        )
    def _request_impl(self, base, method, path, params=None, *, timeout: float | tuple[float, float] | None = None):
        url = base + path
        last_error = None
        timeout_value = self._request_timeout(timeout)

        for attempt in range(2):
            signed_params = self._signed_params(base, params, force_time_sync=(attempt > 0))

            if method == "GET":
                r = self.session.get(url, params=signed_params, timeout=timeout_value)
            else:
                r = self.session.request(method, url, data=signed_params, timeout=timeout_value)

            try:
                data = r.json()
            except Exception:
                r.raise_for_status()

            if r.status_code == 200:
                return data

            err = BinanceAPIError(data.get("code", -1), data.get("msg", "Unknown"))
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
    def request(self, base, method, path, params=None):
        return self._run_request_with_retry(
            lambda: self._request_impl(base, method, path, params, timeout=self.DEFAULT_READ_TIMEOUT_SECONDS),
            max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
            delay=self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS,
        )
    def request_query(self, base, method, path, params=None, *, timeout: float = 6, max_retries: int | None = None, delay: float | None = None):
        retry_count = self.DEFAULT_REQUEST_RETRY_COUNT if max_retries is None else max_retries
        retry_delay = self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS if delay is None else delay
        return self._run_request_with_retry(
            lambda: self._request_impl(base, method, path, params, timeout=timeout),
            max_retries=retry_count,
            delay=retry_delay,
        )
    def public_get(self, base, path, params=None):
        return self.public_get_query(
            base,
            path,
            params,
            timeout=self.DEFAULT_READ_TIMEOUT_SECONDS,
            max_retries=self.DEFAULT_REQUEST_RETRY_COUNT,
            delay=self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS,
        )
    def _public_get_impl(self, base, path, params=None, *, timeout: float | tuple[float, float] | None = None):
        params = params or {}
        url = base + path
        r = self.session.get(url, params=params, timeout=self._request_timeout(timeout))
        r.raise_for_status()
        return r.json()
    def public_get_query(self, base, path, params=None, *, timeout: float = 6, max_retries: int | None = None, delay: float | None = None):
        retry_count = self.DEFAULT_REQUEST_RETRY_COUNT if max_retries is None else max_retries
        retry_delay = self.DEFAULT_REQUEST_RETRY_DELAY_SECONDS if delay is None else delay
        return self._run_request_with_retry(
            lambda: self._public_get_impl(base, path, params, timeout=timeout),
            max_retries=retry_count,
            delay=retry_delay,
        )

    # -------- 余额 --------
    @staticmethod
    def _error_text_contains(exc: Exception, *snippets: str) -> bool:
        text = str(exc or "").lower()
        return any(str(snippet or "").lower() in text for snippet in snippets if snippet)


__all__ = ["BinanceClientRequestMixin"]
