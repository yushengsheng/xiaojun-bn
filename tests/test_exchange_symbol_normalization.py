#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from decimal import Decimal

from exchange_app_config import ExchangeAppConfigMixin
from exchange_binance_client import BinanceClient
from exchange_constants import TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET


class _DirectionalMarketClient(BinanceClient):
    _shared_spot_market_direct_routes = None
    _shared_spot_market_reverse_routes = None

    def __init__(self) -> None:
        super().__init__("test-key", "test-secret")
        self.balances = {
            "USDC": Decimal("20"),
            "USDT": Decimal("25"),
            "BTC": Decimal("0.5"),
            "ALPHA": Decimal("7"),
            "XYZ": Decimal("12"),
        }
        self.orders: list[dict[str, object]] = []

    @staticmethod
    def _symbol_info(symbol: str) -> dict[str, object] | None:
        symbol_u = str(symbol or "").strip().upper()
        pairs = {
            "USDCUSDT": ("USDC", "USDT"),
            "ETHBTC": ("ETH", "BTC"),
            "ALPHAXYZ": ("ALPHA", "XYZ"),
        }
        assets = pairs.get(symbol_u)
        if assets is None:
            return None
        base_asset, quote_asset = assets
        return {
            "symbol": symbol_u,
            "status": "TRADING",
            "baseAsset": base_asset,
            "quoteAsset": quote_asset,
            "quoteAssetPrecision": 8,
            "quotePrecision": 8,
            "quoteOrderQtyMarketAllowed": True,
            "isSpotTradingAllowed": True,
            "orderTypes": ["MARKET", "LIMIT"],
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001", "maxQty": "999999"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.0001", "minPrice": "0.0001", "maxPrice": "999999"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "0.0001"},
            ],
        }

    def public_get(self, _base, path, params=None):
        if path != "/api/v3/exchangeInfo" or params != {"permissions": "SPOT", "showPermissionSets": "false"}:
            raise RuntimeError(f"unexpected public request: {path} {params}")
        return {
            "symbols": [
                self._symbol_info("USDCUSDT"),
                self._symbol_info("ETHBTC"),
                self._symbol_info("ALPHAXYZ"),
            ]
        }

    def get_exchange_info(self, symbol: str):
        return self._symbol_info(symbol)

    def collect_asset_to_spot(self, _asset: str, *, context: str = "交易前") -> int:
        return 0

    def spot_asset_balance_decimal(self, asset: str) -> Decimal:
        return self.balances.get(str(asset or "").strip().upper(), Decimal("0"))

    def get_symbol_price(self, _symbol: str):
        return Decimal("1")

    def request(self, _base, method, path, params=None, **_kwargs):
        if method == "POST" and path == "/api/v3/order":
            order = dict(params or {})
            self.orders.append(order)
            return {"status": "FILLED", **order}
        raise RuntimeError(f"unexpected request: {method} {path}")


class ExchangeSymbolNormalizationTests(unittest.TestCase):
    def test_normalizes_slash_pair_format(self) -> None:
        self.assertEqual(BinanceClient.normalize_trade_symbol("XLM/USDT"), "XLMUSDT")
        self.assertEqual(BinanceClient.normalize_trade_symbol("xlm / usdt"), "XLMUSDT")

    def test_keeps_native_binance_symbol_format(self) -> None:
        self.assertEqual(BinanceClient.normalize_trade_symbol("XLMUSDT"), "XLMUSDT")

    def test_spot_symbol_parts_accept_slash_pair_format(self) -> None:
        self.assertEqual(BinanceClient.split_spot_symbol("XLM/USDT"), ("XLM", "USDT"))
        self.assertEqual(BinanceClient.get_spot_base_asset("XLM/USDT"), "XLM")
        self.assertEqual(BinanceClient.get_spot_quote_asset("XLM/USDT"), "USDT")

    def test_zero_round_market_resolves_both_directions_from_one_real_pair(self) -> None:
        client = _DirectionalMarketClient()
        try:
            direct = client.resolve_spot_market_conversion("USDC/USDT")
            reverse = client.resolve_spot_market_conversion("USDT/USDC")
            btc_to_eth = client.resolve_spot_market_conversion("BTC/ETH")
            xyz_to_alpha = client.resolve_spot_market_conversion("XYZ/ALPHA")
        finally:
            client.close()

        self.assertEqual(direct, {
            "symbol": "USDCUSDT",
            "fromAsset": "USDC",
            "toAsset": "USDT",
            "side": "SELL",
        })
        self.assertEqual(reverse, {
            "symbol": "USDCUSDT",
            "fromAsset": "USDT",
            "toAsset": "USDC",
            "side": "BUY",
        })
        self.assertEqual(btc_to_eth["symbol"], "ETHBTC")
        self.assertEqual(btc_to_eth["side"], "BUY")
        self.assertEqual(xyz_to_alpha, {
            "symbol": "ALPHAXYZ",
            "fromAsset": "XYZ",
            "toAsset": "ALPHA",
            "side": "BUY",
        })

    def test_zero_round_market_executes_reverse_as_buy_and_direct_as_sell(self) -> None:
        client = _DirectionalMarketClient()
        try:
            self.assertTrue(client.spot_market_convert_base_to_quote_all("USDT/USDC"))
            self.assertTrue(client.spot_market_convert_base_to_quote_all("USDC/USDT"))
            self.assertTrue(client.spot_market_convert_base_to_quote_all("XYZ/ALPHA"))
            self.assertTrue(client.spot_market_convert_base_to_quote_all("ALPHA/XYZ"))
        finally:
            client.close()

        self.assertEqual(client.orders[0]["symbol"], "USDCUSDT")
        self.assertEqual(client.orders[0]["side"], "BUY")
        self.assertEqual(client.orders[0]["quoteOrderQty"], "25")
        self.assertEqual(client.orders[1]["symbol"], "USDCUSDT")
        self.assertEqual(client.orders[1]["side"], "SELL")
        self.assertEqual(client.orders[1]["quantity"], "20")
        self.assertEqual(client.orders[2]["symbol"], "ALPHAXYZ")
        self.assertEqual(client.orders[2]["side"], "BUY")
        self.assertEqual(client.orders[2]["quoteOrderQty"], "12")
        self.assertEqual(client.orders[3]["symbol"], "ALPHAXYZ")
        self.assertEqual(client.orders[3]["side"], "SELL")
        self.assertEqual(client.orders[3]["quantity"], "7")

    def test_reverse_resolution_is_limited_to_market_zero_round(self) -> None:
        client = _DirectionalMarketClient()
        try:
            target_asset = ExchangeAppConfigMixin._ensure_trade_symbol_supported(
                client,
                TRADE_ACCOUNT_TYPE_SPOT,
                TRADE_MODE_MARKET,
                "USDTUSDC",
                "BTCUSDT",
                0,
            )
            self.assertEqual(target_asset, "USDC")
            with self.assertRaisesRegex(RuntimeError, "找不到现货交易对"):
                ExchangeAppConfigMixin._ensure_trade_symbol_supported(
                    client,
                    TRADE_ACCOUNT_TYPE_SPOT,
                    TRADE_MODE_MARKET,
                    "USDTUSDC",
                    "BTCUSDT",
                    1,
                )
        finally:
            client.close()

    def test_zero_round_market_rejects_unknown_direction(self) -> None:
        client = _DirectionalMarketClient()
        try:
            with self.assertRaisesRegex(RuntimeError, "找不到支持市价0轮兑换的方向"):
                client.resolve_spot_market_conversion("AAABBB")
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
