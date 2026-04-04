#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import functools
import hashlib
import hmac
import logging
import random
import threading
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests


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
)

logger = logging.getLogger("bot")

class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance API error {code}: {msg}")


def retry_request(max_retries=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    logger.warning(f"网络请求失败 ({i + 1}/{max_retries}): {e}，将在 {delay} 秒后重试...")
                    time.sleep(delay)
                except BinanceAPIError as e:
                    last_exception = e
                    if e.code in [-1001, -1003]:
                        logger.warning(f"Binance 系统繁忙 ({i + 1}/{max_retries}): {e}，重试中...")
                        time.sleep(delay)
                    else:
                        raise e
            logger.error(f"重试 {max_retries} 次后仍然失败。")
            if last_exception:
                raise last_exception
            raise RuntimeError("未知错误：重试失败")
        return wrapper
    return decorator


# ====================== Binance 客户端 ======================
class BinanceClient:
    SERVER_TIME_CACHE_TTL_SECONDS = 30.0
    DEFAULT_RECV_WINDOW_MS = 10000

    def __init__(self, key: str, secret: str, proxy_url: str = ""):
        if not key or not secret:
            raise ValueError("API KEY / SECRET 不能为空")

        self.key = key
        self.secret = secret.encode()
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": key})
        try:
            self.recv_window_ms = max(1000, int(self.DEFAULT_RECV_WINDOW_MS))
        except Exception:
            self.recv_window_ms = self.DEFAULT_RECV_WINDOW_MS
        proxy = str(proxy_url or "").strip()
        self.session.trust_env = not bool(proxy)
        if proxy:
            self.session.proxies.update({
                "http": proxy,
                "https": proxy,
            })

        self.spot = "https://api.binance.com"
        self.um_futures = "https://fapi.binance.com"
        self.cm_futures = "https://dapi.binance.com"

        self._exchange_info_cache = {}
        self._price_cache = {}
        self._um_futures_exchange_info_cache = {}
        self._um_futures_price_cache = {}
        self._server_time_offset_ms = {}
        self._server_time_synced_at = {}
        self._server_time_lock = threading.Lock()

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
        with self._server_time_lock:
            cached_offset = self._server_time_offset_ms.get(base_n)
            synced_at = self._server_time_synced_at.get(base_n, 0.0)
            if (not force) and cached_offset is not None and (now_mono - synced_at) < self.SERVER_TIME_CACHE_TTL_SECONDS:
                return int(cached_offset)

        offset_ms = self._fetch_server_time_ms(base_n)
        with self._server_time_lock:
            self._server_time_offset_ms[base_n] = int(offset_ms)
            self._server_time_synced_at[base_n] = time.monotonic()
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
            with self._server_time_lock:
                cached_offset = self._server_time_offset_ms.get(base_n)
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

    @retry_request(max_retries=3, delay=1)
    def request(self, base, method, path, params=None):
        url = base + path
        last_error = None

        for attempt in range(2):
            signed_params = self._signed_params(base, params, force_time_sync=(attempt > 0))

            if method == "GET":
                r = self.session.get(url, params=signed_params, timeout=15)
            else:
                r = self.session.request(method, url, data=signed_params, timeout=15)

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

    @retry_request(max_retries=3, delay=1)
    def public_get(self, base, path, params=None):
        params = params or {}
        url = base + path
        r = self.session.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # -------- 余额 --------
    def spot_balance(self, asset: str) -> float:
        data = self.request(self.spot, "GET", "/api/v3/account")
        for b in data.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", 0))
        return 0.0

    def spot_all_balances(self):
        data = self.request(self.spot, "GET", "/api/v3/account")
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

    def query_total_wallet_balance(self, quote_asset="USDT"):
        data = self.request(
            self.spot,
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": quote_asset},
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

    def query_asset_balances_breakdown(self) -> dict[str, Decimal]:
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
            for item in self.spot_all_balances():
                add_amount(item.get("asset", ""), item.get("total", 0))
        except Exception as e:
            logger.warning("查询现货资产明细失败: %s", e)

        try:
            for item in self.funding_positive_assets():
                add_amount(item.get("asset", ""), item.get("free", 0))
        except Exception as e:
            logger.warning("查询资金账户资产明细失败: %s", e)

        try:
            for item in self.um_futures_transferable_assets():
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            logger.warning("查询 U本位资产明细失败: %s", e)

        try:
            for item in self.cm_futures_transferable_assets():
                add_amount(item.get("asset", ""), item.get("amount", 0))
        except Exception as e:
            logger.warning("查询 币本位资产明细失败: %s", e)

        return totals

    # -------- 工具：从 symbol 推断现货基础币种 --------
    @staticmethod
    def split_spot_symbol(symbol: str) -> tuple[str, str]:
        symbol_u = str(symbol or "").strip().upper()
        for suffix in SUPPORTED_QUOTE_ASSET_SUFFIXES:
            if symbol_u.endswith(suffix) and len(symbol_u) > len(suffix):
                return symbol_u[:-len(suffix)], suffix
        return "BTC", "USDT"

    @classmethod
    def get_spot_base_asset(cls, symbol: str) -> str:
        base_asset, _quote_asset = cls.split_spot_symbol(symbol)
        return base_asset

    @classmethod
    def get_spot_quote_asset(cls, symbol: str) -> str:
        _base_asset, quote_asset = cls.split_spot_symbol(symbol)
        return quote_asset

    # -------- 公共行情 / 交易规则 --------
    def get_exchange_info(self, symbol: str):
        symbol = symbol.upper()
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
        symbol = symbol.upper()
        try:
            data = self.public_get(self.spot, "/api/v3/ticker/price", {"symbol": symbol})
            price = Decimal(str(data.get("price")))
            self._price_cache[symbol] = price
            return price
        except Exception:
            return None

    @staticmethod
    def _extract_filter(symbol_info: dict, filter_type: str):
        for f in symbol_info.get("filters", []):
            if f.get("filterType") == filter_type:
                return f
        return None

    @staticmethod
    def _decimal_from_str(v, default="0"):
        try:
            return Decimal(str(v))
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
        text = format(Decimal(str(value)).normalize(), "f")
        if text in {"-0", "-0.0"}:
            return "0"
        return text

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
        symbol_u = str(symbol or "").strip().upper()
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

    def get_um_futures_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol_u = str(symbol or "").strip().upper()
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
        rules = self.get_um_futures_trade_rules(symbol)
        if not rules:
            return "USDT"
        return str(rules.get("marginAsset") or rules.get("quoteAsset") or "USDT").strip().upper()

    def get_um_futures_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = str(symbol or "").strip().upper()
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
        symbol_u = str(symbol or "").strip().upper()
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
        symbol_u = str(symbol or "").strip().upper()
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
        symbol_u = str(symbol or "").strip().upper()
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

    @staticmethod
    def _normalize_um_futures_position_side(position_side: str | None) -> str:
        value = str(position_side or "").strip().upper()
        return value if value in {"BOTH", "LONG", "SHORT"} else ""

    def get_um_futures_positions(self, symbol: str) -> list[dict]:
        symbol_u = str(symbol or "").strip().upper()
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
        symbol_u = str(symbol or "").strip().upper()
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
        symbol_u = str(symbol or "").strip().upper()
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

    def get_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = symbol.upper()
        data = self.public_get(self.spot, "/api/v3/ticker/bookTicker", {"symbol": symbol_u})
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"读取盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }

    def place_limit_order(self, symbol: str, side: str, quantity: Decimal, price: Decimal):
        symbol_u = symbol.upper()
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
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self._format_decimal(qty),
            "price": self._format_decimal(order_price),
        }
        data = self.request(self.spot, "POST", "/api/v3/order", params)
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
                "symbol": symbol.upper(),
                "orderId": int(order_id),
            },
        )

    def cancel_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "DELETE",
            "/api/v3/order",
            {
                "symbol": symbol.upper(),
                "orderId": int(order_id),
            },
        )

    def wait_order_filled(self, symbol: str, order_id: int | str, stop_event=None, poll_interval: float = 1.0):
        symbol_u = symbol.upper()
        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")
            time.sleep(max(0.2, float(poll_interval)))

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
        self.collect_funding_asset_to_spot(quote_asset)
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

    def spot_limit_buy_quote_amount(self, symbol: str, price: Decimal, quote_amount: Decimal | str | float | int):
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
        return self.place_limit_order(symbol, "BUY", qty, price_dec)

    def spot_limit_sell_quantity(self, symbol: str, price: Decimal, quantity: Decimal | str | float | int):
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
        return self.place_limit_order(symbol, "SELL", qty, price_dec)

    def spot_buy_quote_amount(self, symbol: str, quote_amount: Decimal | str | float | int):
        symbol_u = str(symbol or "").strip().upper()
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

        quote_asset = self.get_spot_quote_asset(symbol_u)
        self.collect_funding_asset_to_spot(quote_asset)
        quote_balance = Decimal(str(self.spot_balance(quote_asset)))
        if quote_balance < amount:
            raise RuntimeError(
                f"现货 {quote_asset} 余额不足：需要 {self._format_decimal(amount)}，当前 {self._format_decimal(quote_balance)}"
            )

        if rules["minNotional"] > 0 and amount < rules["minNotional"]:
            raise RuntimeError(
                f"{symbol_u} 买入金额过小：{self._format_decimal(amount)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
            )

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol_u,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": self._format_decimal(amount),
            },
        )
        logger.info("现货市价买入 %s，使用 %s 金额 %s", symbol_u, quote_asset, self._format_decimal(amount))
        return True

    def buy_bnb_with_quote_amount(self, quote_asset: str, amount: Decimal | str | float | int):
        quote_asset_u = str(quote_asset or "").strip().upper()
        if not quote_asset_u:
            raise RuntimeError("后置币种为空，无法预买 BNB")
        if quote_asset_u == "BNB":
            logger.info("后置币种为 BNB，无需预买 BNB")
            return False
        symbol = f"BNB{quote_asset_u}"
        return self.spot_buy_quote_amount(symbol, amount)

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

    def sell_asset_market(self, symbol: str, free_balance: float, reserve_ratio=Decimal("0.999")) -> bool:
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
            logger.info("交易对 %s 数量过小，跳过卖出", symbol)
            return False

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol)
        if price:
            notional = qty * price
            if rules["minNotional"] > 0 and notional < rules["minNotional"]:
                logger.info("交易对 %s 名义价值 %.8f 小于最小下单额 %.8f，跳过",
                            symbol, float(notional), float(rules["minNotional"]))
                return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty.normalize(), "f"),
            },
        )
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
                logger.warning("卖出大额币 %s 失败: %s", asset, e)

        logger.info("大额币卖出为 USDT 完成，成功处理 %d 个币种", len(sold_assets))
        return sold_assets

    # -------- 现货买入（全部 USDT） --------
    def spot_buy_all_usdt(self, buffer=0.2, symbol="BTCUSDT"):
        quote_asset = self.get_spot_quote_asset(symbol)
        self.collect_funding_asset_to_spot(quote_asset)
        quote_balance = self.spot_balance(quote_asset)
        if quote_balance <= buffer:
            logger.info("现货 %s %.8f <= buffer %.8f，跳过买入", quote_asset, quote_balance, buffer)
            return False

        amount = (quote_balance - buffer) * 0.999
        if amount <= 0:
            logger.info("可用 %s 金额太小，跳过买入", quote_asset)
            return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{amount:.8f}",
            },
        )
        logger.info("现货市价买入 %s，使用 %s 金额 %.8f", symbol, quote_asset, amount)
        return True

    # -------- 现货卖出（全部基础币；precision 参数仅为兼容旧配置保留） --------
    def spot_sell_all_base(self, symbol: str, precision: int):
        base = self.get_spot_base_asset(symbol)
        balance = self.spot_balance(base)

        if balance <= 0:
            return False

        # 现货市价卖出按交易所 stepSize 取整，避免高价币因手填精度或额外预留被截到低于最小下单额。
        return self.sell_asset_market(symbol, balance, reserve_ratio=Decimal("1"))

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
                moved_count = self.collect_asset_to_spot(coin)
                if moved_count > 0:
                    # Give Binance a brief moment to reflect the transfer in spot balance.
                    time.sleep(0.5)
            except Exception as e:
                logger.warning("提现前归集 %s 到现货失败: %s", coin, e)
        balance = self.spot_balance(coin)
        amount = balance - fee_buffer
        if amount <= 0:
            logger.info("%s 余额 %.8f 不足以提现（需 > buffer %.8f）", coin, balance, fee_buffer)
            return 0.0

        params = {
            "coin": coin,
            "address": address,
            "network": network,
            "amount": f"{amount:.8f}",
        }

        if enable_withdraw:
            self.request(
                self.spot,
                "POST",
                "/sapi/v1/capital/withdraw/apply",
                params,
            )
            logger.info("已提交提现 %.8f %s → %s (%s)", amount, coin, address, network)
            return amount
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
    def funding_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
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

    def collect_funding_asset_to_spot(self, asset: str) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return Decimal("0")
        try:
            amount = self.funding_asset_balance(asset_u)
        except Exception as e:
            logger.warning("查询资金账户 %s 余额失败: %s", asset_u, e)
            return Decimal("0")
        if amount <= 0:
            return Decimal("0")
        try:
            self.universal_transfer("FUNDING_MAIN", asset_u, amount)
            logger.info("检测到资金账户 %s 余额，已自动划转到现货：%s", asset_u, self._format_decimal(amount))
            time.sleep(0.5)
            return amount
        except Exception as e:
            logger.warning("资金账户划转到现货失败 %s %s: %s", asset_u, self._format_decimal(amount), e)
            return Decimal("0")

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
            self.universal_transfer("MAIN_UMFUTURE", asset_u, transfer_amount)
            logger.info("检测到现货 %s 余额，已自动划转到 U本位：%s", asset_u, self._format_decimal(transfer_amount))
            time.sleep(0.5)
            return transfer_amount
        except Exception as e:
            logger.warning("现货划转到 U本位失败 %s %s: %s", asset_u, self._format_decimal(transfer_amount), e)
            return Decimal("0")

    def um_futures_transferable_assets(self):
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v2/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("maxWithdrawAmount", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 币本位合约可转出余额 --------
    def cm_futures_transferable_assets(self):
        data = self.request(
            self.cm_futures,
            "GET",
            "/dapi/v1/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("withdrawAvailable", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 通用划转 --------
    def universal_transfer(self, transfer_type: str, asset: str, amount: float):
        if amount <= 0:
            return False

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

    def collect_asset_to_spot(self, asset: str) -> int:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            return 0

        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = float(item.get("amount", 0) or 0)
                if amount <= 0:
                    continue
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset_u, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("提现前 U本位划转失败 %s %.8f: %s", asset_u, amount, e)
        except Exception as e:
            logger.warning("提现前查询 U本位 %s 余额失败: %s", asset_u, e)

        try:
            items = self.cm_futures_transferable_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = float(item.get("amount", 0) or 0)
                if amount <= 0:
                    continue
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset_u, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("提现前 币本位划转失败 %s %.8f: %s", asset_u, amount, e)
        except Exception as e:
            logger.warning("提现前查询 币本位 %s 余额失败: %s", asset_u, e)

        try:
            items = self.funding_positive_assets()
            for item in items:
                if str(item.get("asset") or "").strip().upper() != asset_u:
                    continue
                amount = float(item.get("free", 0) or 0)
                if amount <= 0:
                    continue
                try:
                    self.universal_transfer("FUNDING_MAIN", asset_u, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("提现前 资金账户划转失败 %s %.8f: %s", asset_u, amount, e)
        except Exception as e:
            logger.warning("提现前查询 资金账户 %s 余额失败: %s", asset_u, e)

        if total_count > 0:
            logger.info("提现前归集 %s 到现货完成，共处理 %d 项", asset_u, total_count)
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
                amount = item["amount"]
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("U本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 U本位合约余额失败: %s", e)

        try:
            items = self.cm_futures_transferable_assets()
            if items:
                logger.info("检测到 币本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["amount"]
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("币本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 币本位合约余额失败: %s", e)

        try:
            items = self.funding_positive_assets()
            if items:
                logger.info("检测到 资金账户可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["free"]
                try:
                    self.universal_transfer("FUNDING_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("资金账户划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
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

__all__ = ["BinanceAPIError", "retry_request", "BinanceClient"]
