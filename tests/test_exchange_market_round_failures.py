#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from decimal import Decimal

from exchange_app_accounts import ExchangeAppAccountsMixin
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_constants import TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET
from exchange_strategy import Strategy


class _NoDelayStop:
    def is_set(self) -> bool:
        return False

    def wait(self, _seconds) -> bool:
        return False


class _MarketRoundTransientFailureClient:
    def __init__(self):
        self.buy_calls = 0
        self.calls: list[tuple[str, str]] = []

    def get_spot_base_asset(self, _symbol: str) -> str:
        return "BTC"

    def get_spot_quote_asset(self, _symbol: str) -> str:
        return "USDT"

    def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
        self.calls.append(("sell", str(symbol or "").strip().upper()))
        return False

    def spot_buy_all_usdt(self, symbol: str):
        self.buy_calls += 1
        self.calls.append(("buy", str(symbol or "").strip().upper()))
        if self.buy_calls == 2:
            raise RuntimeError("temporary network error")
        return True

    def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
        self.calls.append(("withdraw", str(coin or "").strip().upper()))
        return 0


class _MarketRoundPermanentFailureClient(_MarketRoundTransientFailureClient):
    def __init__(self):
        super().__init__()
        self.fail_until_call = 10

    def spot_buy_all_usdt(self, symbol: str):
        self.buy_calls += 1
        self.calls.append(("buy", str(symbol or "").strip().upper()))
        if self.buy_calls <= self.fail_until_call:
            raise RuntimeError("persistent network error")
        return True


class ExchangeMarketRoundFailureTests(unittest.TestCase):
    def test_market_round_transient_failure_retries_without_failed_round(self):
        client = _MarketRoundTransientFailureClient()
        strategy = Strategy(
            client=client,
            spot_rounds=3,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
        )

        result = strategy.run(stop_event=_NoDelayStop())

        self.assertEqual(client.buy_calls, 4)
        self.assertIn(("withdraw", "USDT"), client.calls)
        self.assertFalse(result.get("trade_failed"))
        self.assertTrue(result.get("withdraw_attempted"))
        self.assertEqual(result.get("market_round_total"), 3)
        self.assertEqual(result.get("market_round_completed"), 3)
        self.assertEqual(result.get("market_round_failed_count"), 0)
        self.assertEqual(result.get("market_round_failed_rounds"), [])

    def test_market_round_permanent_failure_continues_and_reports_structured_result(self):
        client = _MarketRoundPermanentFailureClient()
        strategy = Strategy(
            client=client,
            spot_rounds=2,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
        )

        result = strategy.run(stop_event=_NoDelayStop())

        self.assertEqual(client.buy_calls, 11)
        self.assertIn(("withdraw", "USDT"), client.calls)
        self.assertFalse(result.get("trade_failed"))
        self.assertTrue(result.get("withdraw_attempted"))
        self.assertEqual(result.get("market_round_total"), 2)
        self.assertEqual(result.get("market_round_completed"), 2)
        self.assertEqual(result.get("market_round_failed_count"), 1)
        self.assertEqual(result.get("market_round_failed_rounds"), [1])
        self.assertIn("persistent network error", result.get("market_round_failure_details", [{}])[0].get("reason", ""))

    def test_market_round_failure_status_and_summary_are_visible(self):
        strategy_result = {
            "market_round_total": 6,
            "market_round_completed": 6,
            "market_round_failed_count": 1,
            "market_round_failed_rounds": [3],
        }
        status, success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
            0,
            "USDT",
            enable_withdraw=False,
            strategy_result=strategy_result,
        )
        self.assertTrue(success)
        self.assertIn("6轮完成，失败1轮", status)
        self.assertEqual(ExchangeAppAccountsMixin._account_row_color_by_status(status), "#ffe3b8")

        withdraw_status, withdraw_success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
            1.25,
            "USDT",
            enable_withdraw=True,
            strategy_result=strategy_result,
        )
        self.assertTrue(withdraw_success)
        self.assertIn("6轮完成，失败1轮", withdraw_status)
        self.assertIn("提现", withdraw_status)

        mixin = object.__new__(ExchangeAppBatchMixin)
        summary_text = mixin._build_batch_summary_text(
            {
                "results": {"a": True, "b": True},
                "withdraw_by_account": {"a": (Decimal("1.25"), "USDT")},
                "balance_by_account": {},
                "market_failed_rounds_by_account": {"a": 1, "b": 0},
            }
        )
        self.assertIn("现货市价失败轮次=1(账号1)", summary_text)
        self.assertEqual(mixin._build_batch_result_extra_lines({"market_failed_rounds_by_account": {"a": 1}}), [
            "现货市价失败轮次：1（涉及账号 1 个）"
        ])


if __name__ == "__main__":
    unittest.main()
