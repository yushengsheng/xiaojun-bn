#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_binance_common import (
    BinanceAPIError,
    explain_binance_api_error,
    retry_request,
    short_binance_api_error,
    summarize_exchange_exception,
    threading,
)
from exchange_binance_account import BinanceClientAccountMixin
from exchange_binance_convert import BinanceClientConvertMixin
from exchange_binance_earn import BinanceClientEarnMixin
from exchange_binance_funding import BinanceClientFundingMixin
from exchange_binance_futures import BinanceClientFuturesMixin
from exchange_binance_request import BinanceClientRequestMixin
from exchange_binance_spot import BinanceClientSpotMixin


class BinanceClient(
    BinanceClientRequestMixin,
    BinanceClientAccountMixin,
    BinanceClientEarnMixin,
    BinanceClientConvertMixin,
    BinanceClientSpotMixin,
    BinanceClientFuturesMixin,
    BinanceClientFundingMixin,
):
    SERVER_TIME_CACHE_TTL_SECONDS = 30.0
    DEFAULT_RECV_WINDOW_MS = 20000
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 3.0
    DEFAULT_READ_TIMEOUT_SECONDS = 6.0
    _shared_convert_exchange_info_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    _shared_convert_asset_info_cache: dict[str, int] | None = None
    _shared_convert_cache_lock = threading.Lock()
    _shared_server_time_offset_ms: dict[str, int] = {}
    _shared_server_time_synced_at: dict[str, float] = {}
    _shared_server_time_lock = threading.Lock()
    def __init__(self, key: str, secret: str, proxy_url: str = ""):
        if not key or not secret:
            raise ValueError("API KEY / SECRET 不能为空")

        self.key = key
        self.secret = secret.encode()
        self.spot = "https://api.binance.com"
        self.um_futures = "https://fapi.binance.com"
        self.cm_futures = "https://dapi.binance.com"
        self._proxy_url = str(proxy_url or "").strip()
        self.session = self._build_session()
        try:
            self.recv_window_ms = max(1000, int(self.DEFAULT_RECV_WINDOW_MS))
        except Exception:
            self.recv_window_ms = self.DEFAULT_RECV_WINDOW_MS

        self._exchange_info_cache = {}
        self._price_cache = {}
        self._um_futures_exchange_info_cache = {}
        self._um_futures_price_cache = {}


__all__ = [
    "BinanceAPIError",
    "retry_request",
    "BinanceClient",
    "explain_binance_api_error",
    "short_binance_api_error",
    "summarize_exchange_exception",
]
