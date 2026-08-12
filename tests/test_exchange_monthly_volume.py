#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from decimal import Decimal

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceClient


class _TradeHistoryClient(BinanceClient):
    def __init__(self, pages):
        self.spot = "spot"
        self.pages = list(pages)
        self.requests: list[dict[str, object]] = []

    def request(self, _base, method, path, params=None, **_kwargs):
        self.assert_request(method, path)
        self.requests.append(dict(params or {}))
        return self.pages.pop(0) if self.pages else []

    @staticmethod
    def assert_request(method: str, path: str) -> None:
        if method != "GET" or path != "/api/v3/myTrades":
            raise AssertionError(f"unexpected request: {method} {path}")


class ExchangeMonthlyVolumeTests(unittest.TestCase):
    def test_context_menu_queries_only_the_context_account(self) -> None:
        account = {"api_key": "context-key"}

        class _App:
            def __init__(self):
                self.calls = []

            def _get_context_account_or_warn(self):
                return account

            def run_query_monthly_trade_volume(self, **kwargs):
                self.calls.append(kwargs)

        app = _App()
        ExchangeAppBatchMixin.run_context_account_monthly_trade_volume(app)

        self.assertEqual(app.calls, [{"accounts_to_run": [account]}])

    def test_sums_quote_volume_and_paginates_by_trade_id(self) -> None:
        first_page = [
            {
                "id": trade_id,
                "orderId": trade_id,
                "price": "10",
                "qty": "0.125",
                "quoteQty": "1.25",
                "isBuyer": trade_id % 2 == 1,
                "time": 1_000 + trade_id,
            }
            for trade_id in range(1, 1001)
        ]
        second_page = [
            {"id": 1001, "orderId": 1001, "quoteQty": "2.5", "isBuyer": True, "time": 2_500},
            {"id": 1002, "orderId": 1002, "quoteQty": "99", "isBuyer": False, "time": 3_001},
        ]
        client = _TradeHistoryClient([first_page, second_page])

        summary = client.spot_trade_volume_summary(
            "btcusdt",
            start_time_ms=1_000,
            end_time_ms=3_000,
        )

        self.assertEqual(summary["quoteVolume"], Decimal("1252.50"))
        self.assertEqual(summary["buyQuoteVolume"], Decimal("627.50"))
        self.assertEqual(summary["sellQuoteVolume"], Decimal("625.00"))
        self.assertEqual(summary["orderPnl"], Decimal("-2.50"))
        self.assertEqual(summary["tradeCount"], 1001)
        self.assertEqual(summary["orderCount"], 1001)
        self.assertEqual(client.requests[0], {"symbol": "BTCUSDT", "startTime": 1_000, "limit": 1000})
        self.assertEqual(client.requests[1], {"symbol": "BTCUSDT", "fromId": 1001, "limit": 1000})

    def test_counts_distinct_orders_and_falls_back_to_price_times_quantity(self) -> None:
        client = _TradeHistoryClient([[
            {
                "id": 1,
                "orderId": 7,
                "price": "10",
                "qty": "2",
                "isBuyer": True,
                "commission": "0.002",
                "commissionAsset": "BTC",
                "time": 1_100,
            },
            {
                "id": 2,
                "orderId": 7,
                "price": "12",
                "qty": "1",
                "quoteQty": "12",
                "isBuyer": False,
                "commission": "0.01",
                "commissionAsset": "USDT",
                "time": 1_200,
            },
        ]])

        summary = client.spot_trade_volume_summary(
            "BTCUSDT",
            start_time_ms=1_000,
            end_time_ms=2_000,
        )

        self.assertEqual(summary["quoteVolume"], Decimal("32"))
        self.assertEqual(summary["buyQuoteVolume"], Decimal("20"))
        self.assertEqual(summary["sellQuoteVolume"], Decimal("12"))
        self.assertEqual(summary["orderPnl"], Decimal("-8"))
        self.assertEqual(summary["commissionByAsset"], {
            "BTC": Decimal("0.002"),
            "USDT": Decimal("0.01"),
        })
        self.assertEqual(summary["tradeCount"], 2)
        self.assertEqual(summary["orderCount"], 1)
        self.assertEqual(summary["quoteAsset"], "USDT")


if __name__ == "__main__":
    unittest.main()
