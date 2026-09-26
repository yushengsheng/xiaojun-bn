#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from exchange_app_accounts import ExchangeAppAccountsMixin
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceClient
from exchange_constants import (
    MARKET_STOP_MODE_BNB,
    MARKET_STOP_MODE_DEFAULT,
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_MODE_CONVERT,
    TRADE_MODE_LADDER,
    TRADE_MODE_LIMIT,
    TRADE_MODE_MARKET,
    TRADE_MODE_PREMIUM,
)
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
        self.buy_assume_ready_flags: list[bool] = []

    def get_spot_base_asset(self, _symbol: str) -> str:
        return "BTC"

    def get_spot_quote_asset(self, _symbol: str) -> str:
        return "USDT"

    def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
        self.calls.append(("sell", str(symbol or "").strip().upper()))
        return False

    def resolve_spot_market_conversion(self, symbol: str):
        return {
            "symbol": str(symbol or "").strip().upper(),
            "fromAsset": "BTC",
            "toAsset": "USDT",
            "side": "SELL",
        }

    def spot_market_convert_base_to_quote_all(self, symbol: str):
        return self.spot_sell_all_base(symbol)

    def collect_asset_to_spot(self, asset: str, *, context: str = "提现前"):
        self.calls.append(("collect", "%s:%s" % (str(context or "").strip(), str(asset or "").strip().upper())))
        return 1

    def spot_buy_all_usdt(self, symbol: str, *, assume_spot_funds_ready: bool = False):
        self.buy_calls += 1
        self.buy_assume_ready_flags.append(bool(assume_spot_funds_ready))
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

    def spot_buy_all_usdt(self, symbol: str, *, assume_spot_funds_ready: bool = False):
        self.buy_calls += 1
        self.buy_assume_ready_flags.append(bool(assume_spot_funds_ready))
        self.calls.append(("buy", str(symbol or "").strip().upper()))
        if self.buy_calls <= self.fail_until_call:
            raise RuntimeError("persistent network error")
        return True


class _MarketBnbStopClient:
    def __init__(self, bnb_balances):
        self.bnb_balances = list(bnb_balances)
        self.calls: list[tuple[str, str]] = []
        self.buy_calls = 0

    def get_spot_base_asset(self, _symbol: str) -> str:
        return "BTC"

    def get_spot_quote_asset(self, _symbol: str) -> str:
        return "USDT"

    def spot_asset_balance_decimal(self, asset: str) -> Decimal:
        self.calls.append(("topup-balance", str(asset or "").strip().upper()))
        return Decimal("0.01")

    def buy_bnb_with_quote_amount(self, quote_asset: str, quote_amount, *, return_order: bool = False):
        self.calls.append(("topup", str(quote_asset or "").strip().upper()))
        return {"toAmount": "0.01"} if return_order else True

    @staticmethod
    def _decimal_from_str(value, default="0") -> Decimal:
        return Decimal(str(value or default))

    def ensure_bnb_fee_ready_in_spot(self, **_kwargs) -> Decimal:
        self.calls.append(("topup-ready", "BNB"))
        return Decimal("0.02")

    def spot_balance(self, asset: str):
        self.calls.append(("bnb-check", str(asset or "").strip().upper()))
        return self.bnb_balances.pop(0)

    def collect_asset_to_spot(self, asset: str, *, context: str = "提现前"):
        self.calls.append(("collect", str(asset or "").strip().upper()))
        return 1

    def spot_buy_all_usdt(self, symbol: str, *, assume_spot_funds_ready: bool = False):
        self.buy_calls += 1
        self.calls.append(("buy", str(symbol or "").strip().upper()))
        return True

    def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
        self.calls.append(("sell", str(symbol or "").strip().upper()))
        return True

    def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
        self.calls.append(("withdraw", "%s:%s" % (str(coin or "").strip().upper(), fee_buffer)))
        return 0


class ExchangeMarketRoundFailureTests(unittest.TestCase):
    def test_exchange_statistics_use_five_decimal_places(self):
        self.assertEqual(ExchangeAppBatchMixin._format_amount(Decimal("1.234565")), "1.23457")
        self.assertEqual(
            ExchangeAppBatchMixin._format_batch_metric_totals(
                {
                    "a": (Decimal("1.111114"), "USDT"),
                    "b": (Decimal("2.222225"), "USDT"),
                }
            ),
            "3.33334 USDT",
        )

    def test_exchange_asset_breakdown_uses_five_decimal_places(self):
        self.assertEqual(
            ExchangeAppBatchMixin._format_asset_breakdown_text(
                {"USDT": Decimal("10.123456"), "BNB": Decimal("0.000006")}
            ),
            "USDT 10.12346 | BNB 0.00001",
        )

    def test_exchange_result_files_use_fixed_five_decimal_places(self):
        recorder = object.__new__(ExchangeAppAccountsMixin)
        recorder._result_file_lock = threading.Lock()
        recorder.total_asset_results = {}
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            withdraw_path = root / "withdraw.txt"
            asset_path = root / "assets.txt"
            csv_path = root / "assets.csv"
            with patch("exchange_app_accounts.WITHDRAW_SUCCESS_FILE", withdraw_path), \
                    patch("exchange_app_accounts.TOTAL_ASSET_RESULT_FILE", asset_path), \
                    patch("exchange_app_accounts.filedialog.asksaveasfilename", return_value=str(csv_path)), \
                    patch("exchange_app_accounts.messagebox.showinfo"):
                recorder.record_withdraw(1, "example-key", "0xabc", Decimal("1.234565"))
                recorder.record_total_asset(1, "example-key", "0xabc", "BSC", Decimal("9.8"))
                recorder.export_total_asset_csv()
            self.assertTrue(withdraw_path.read_text(encoding="utf-8").rstrip().endswith("+1.23457"))
            self.assertTrue(asset_path.read_text(encoding="utf-8").rstrip().endswith("+9.80000"))
            self.assertIn("9.80000", csv_path.read_text(encoding="utf-8-sig"))

    def test_batch_market_stagger_enables_strict_coordination_for_multiple_workers(self):
        self.assertTrue(
            ExchangeAppBatchMixin._batch_market_stagger_enabled(
                enabled=True,
                account_count=5,
                max_threads=5,
                trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
                trade_mode=TRADE_MODE_MARKET,
                spot_rounds=3,
                market_stop_mode=MARKET_STOP_MODE_DEFAULT,
            )
        )

    def test_batch_market_stagger_is_disabled_outside_multi_thread_market_rounds(self):
        common = {
            "enabled": True,
            "account_count": 2,
            "max_threads": 2,
            "trade_account_type": TRADE_ACCOUNT_TYPE_SPOT,
            "trade_mode": TRADE_MODE_MARKET,
            "spot_rounds": 1,
            "market_stop_mode": MARKET_STOP_MODE_DEFAULT,
        }
        for override in (
            {"enabled": False},
            {"account_count": 1},
            {"max_threads": 1},
            {"trade_mode": TRADE_MODE_LIMIT},
            {"spot_rounds": 0},
        ):
            with self.subTest(override=override):
                self.assertFalse(
                    ExchangeAppBatchMixin._batch_market_stagger_enabled(**(common | override))
                )

    def test_market_initial_stagger_adds_one_wait_before_first_buy(self):
        client = _MarketRoundTransientFailureClient()
        strategy = Strategy(
            client=client,
            spot_rounds=1,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: client.calls.append(("sleep", "")),
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
            market_initial_stagger=True,
        )

        strategy.run(stop_event=_NoDelayStop())

        first_sleep = client.calls.index(("sleep", ""))
        first_buy = client.calls.index(("buy", "BTCUSDT"))
        self.assertLess(first_sleep, first_buy)
        self.assertEqual(client.calls.count(("sleep", "")), 3)

    def test_market_default_keeps_first_buy_before_random_wait(self):
        client = _MarketRoundTransientFailureClient()
        strategy = Strategy(
            client=client,
            spot_rounds=1,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: client.calls.append(("sleep", "")),
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
        )

        strategy.run(stop_event=_NoDelayStop())

        self.assertLess(client.calls.index(("buy", "BTCUSDT")), client.calls.index(("sleep", "")))
        self.assertEqual(client.calls.count(("sleep", "")), 2)

    def test_account_bnb_topup_selection_controls_only_that_account(self):
        enabled_account = {"api_key": "enabled-key"}
        recreated_enabled_account = {"api_key": "enabled-key"}
        disabled_account = {"api_key": "disabled-key"}
        enabled_keys = {ExchangeAppBatchMixin._account_batch_key(enabled_account)}

        self.assertEqual(
            ExchangeAppBatchMixin._batch_account_bnb_topup_amount(
                recreated_enabled_account,
                Decimal("5"),
                enabled_keys,
            ),
            Decimal("5"),
        )
        self.assertEqual(
            ExchangeAppBatchMixin._batch_account_bnb_topup_amount(disabled_account, Decimal("5"), enabled_keys),
            Decimal("0"),
        )

    def test_account_bnb_topup_covers_every_existing_supported_spot_mode(self):
        for trade_mode in (TRADE_MODE_MARKET, TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM, TRADE_MODE_LADDER):
            with self.subTest(trade_mode=trade_mode):
                self.assertTrue(
                    ExchangeAppBatchMixin._batch_supports_bnb_topup(
                        TRADE_ACCOUNT_TYPE_SPOT,
                        trade_mode,
                        1,
                        MARKET_STOP_MODE_DEFAULT,
                    )
                )
        self.assertTrue(
            ExchangeAppBatchMixin._batch_supports_bnb_topup(
                TRADE_ACCOUNT_TYPE_SPOT,
                TRADE_MODE_MARKET,
                0,
                MARKET_STOP_MODE_BNB,
            )
        )
        self.assertFalse(
            ExchangeAppBatchMixin._batch_supports_bnb_topup(
                TRADE_ACCOUNT_TYPE_SPOT,
                TRADE_MODE_CONVERT,
                1,
                MARKET_STOP_MODE_DEFAULT,
            )
        )

    def test_market_bnb_stop_ignores_rounds_and_keeps_topup_before_trading(self):
        client = _MarketBnbStopClient([Decimal("0.02"), Decimal("0.005")])
        strategy = Strategy(
            client=client,
            spot_rounds=0,
            withdraw_coin="USDT",
            withdraw_address="0x0000000000000000000000000000000000000001",
            withdraw_network="BSC",
            withdraw_fee_buffer=0.5,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=True,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
            market_stop_mode=MARKET_STOP_MODE_BNB,
            bnb_fee_stop_value=Decimal("0.01"),
            bnb_topup_amount=Decimal("10"),
        )

        result = strategy.run(stop_event=_NoDelayStop())

        self.assertEqual(client.buy_calls, 1)
        self.assertEqual(client.calls.count(("bnb-check", "BNB")), 2)
        self.assertLess(client.calls.index(("topup", "USDT")), client.calls.index(("bnb-check", "BNB")))
        self.assertIn(("withdraw", "USDT:0.5"), client.calls)
        self.assertTrue(result.get("withdraw_attempted"))
        self.assertEqual(result.get("market_round_total"), 1)
        self.assertEqual(result.get("market_round_completed"), 1)

    def test_market_bnb_stop_before_first_buy_when_balance_is_below_threshold(self):
        client = _MarketBnbStopClient([Decimal("0.005")])
        strategy = Strategy(
            client=client,
            spot_rounds=50,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
            market_stop_mode=MARKET_STOP_MODE_BNB,
            bnb_fee_stop_value=Decimal("0.01"),
        )

        result = strategy.run(stop_event=_NoDelayStop())

        self.assertEqual(client.buy_calls, 0)
        self.assertEqual(result.get("market_round_completed"), 0)

    def test_batch_summary_shows_market_bnb_stop_instead_of_rounds(self):
        lines = ExchangeAppBatchMixin()._build_batch_execute_summary_lines(
            account_count=2,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
            spot_symbol="BTCUSDT",
            spot_rounds=20,
            market_stop_mode=MARKET_STOP_MODE_BNB,
            bnb_fee_stop_value=Decimal("0.01"),
            futures_symbol="BTCUSDT",
            futures_rounds=1,
            min_delay=1000,
            max_delay=2000,
            withdraw_coin="USDT",
            withdraw_network="BSC",
            enable_withdraw=False,
        )
        self.assertIn("停止方式：剩余BNB低于 0.01", lines)
        self.assertNotIn("轮次：20", lines)

    def test_batch_confirm_shows_bnb_topup_account_counts(self):
        text = ExchangeAppBatchMixin._format_bnb_topup_account_count_text(15, 10)

        self.assertEqual(text, "预购：10 个账号；不预购：5 个账号。")
        self.assertNotIn("BNB", text)

    def test_zero_round_uses_selected_trade_mode(self):
        market_client = _MarketRoundTransientFailureClient()
        market_strategy = Strategy(
            client=market_client,
            spot_rounds=0,
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
        market_strategy.run(stop_event=_NoDelayStop())
        self.assertEqual(market_client.calls.count(("sell", "BTCUSDT")), 1)

        convert_client = _MarketRoundTransientFailureClient()
        convert_client.convert_base_to_quote_all = lambda symbol: convert_client.calls.append(
            ("convert", str(symbol or "").strip().upper())
        ) or True
        convert_strategy = Strategy(
            client=convert_client,
            spot_rounds=0,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_CONVERT,
        )
        convert_strategy.run(stop_event=_NoDelayStop())
        self.assertEqual(convert_client.calls.count(("convert", "BTCUSDT")), 1)
        self.assertNotIn(("sell", "BTCUSDT"), convert_client.calls)

    def test_zero_round_failure_is_reported_and_blocks_withdrawal(self):
        class _FailingZeroRoundClient(_MarketRoundTransientFailureClient):
            def spot_market_convert_base_to_quote_all(self, symbol: str):
                self.calls.append(("sell", str(symbol or "").strip().upper()))
                raise RuntimeError("zero-round order rejected")

        client = _FailingZeroRoundClient()
        strategy = Strategy(
            client=client,
            spot_rounds=0,
            withdraw_coin="USDT",
            withdraw_address="0x0000000000000000000000000000000000000001",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol="BTCUSDT",
            sleep_fn=lambda: None,
            enable_withdraw=True,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
        )

        result = strategy.run(stop_event=_NoDelayStop())

        self.assertTrue(result.get("trade_failed"))
        self.assertEqual(result.get("trade_error_stage"), "0轮现货兑换")
        self.assertIn("zero-round order rejected", result.get("trade_error", ""))
        self.assertFalse(result.get("withdraw_attempted"))
        self.assertNotIn(("withdraw", "USDT"), client.calls)

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
        self.assertEqual(client.calls.count(("collect", "交易前:USDT")), 1)
        self.assertEqual(client.buy_assume_ready_flags, [True, True, True, True])
        self.assertNotIn(("withdraw", "USDT"), client.calls)
        self.assertFalse(result.get("trade_failed"))
        self.assertFalse(result.get("withdraw_attempted"))
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
        self.assertEqual(client.calls.count(("collect", "交易前:USDT")), 1)
        self.assertTrue(client.buy_assume_ready_flags)
        self.assertTrue(all(client.buy_assume_ready_flags))
        self.assertNotIn(("withdraw", "USDT"), client.calls)
        self.assertFalse(result.get("trade_failed"))
        self.assertFalse(result.get("withdraw_attempted"))
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

    def test_ladder_completion_status_is_visible_when_withdraw_is_disabled(self):
        status, success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
            0,
            "USDT",
            enable_withdraw=False,
            completion_text="分仓挂单结束",
        )

        self.assertTrue(success)
        self.assertIn("分仓挂单结束", status)

    def test_spot_completion_status_appends_current_pair_volume_and_pnl(self):
        summary = {
            "quoteAsset": "USDT",
            "quoteVolume": Decimal("123.456789"),
            "orderPnl": Decimal("0.123456"),
        }
        stats = ExchangeAppBatchMixin._format_spot_run_trade_statistics("btcusdt", summary)
        self.assertEqual(stats, "BTCUSDT 交易量≈123.45679 USDT｜买卖收支差≈+0.12346 USDT")
        completion = ExchangeAppBatchMixin._spot_strategy_completion_text(
            TRADE_MODE_MARKET,
            3,
            {"market_round_completed": 3},
        )
        status, success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
            0,
            "USDT",
            enable_withdraw=False,
            completion_text=completion,
            trade_stats_text=stats,
        )
        self.assertTrue(success)
        self.assertEqual(status, f"✔市价3轮结束｜{stats}")

        withdraw_status, withdraw_success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
            9.876543,
            "USDT",
            enable_withdraw=True,
            completion_text=completion,
            trade_stats_text=stats,
        )
        self.assertTrue(withdraw_success)
        self.assertEqual(withdraw_status, f"✔提现9.87654 USDT｜{stats}")

    def test_spot_completion_labels_cover_limit_modes(self):
        self.assertEqual(
            ExchangeAppBatchMixin._spot_strategy_completion_text(TRADE_MODE_LIMIT, 0),
            "挂单结束",
        )
        self.assertEqual(
            ExchangeAppBatchMixin._spot_strategy_completion_text(TRADE_MODE_PREMIUM, 0),
            "溢价挂单结束",
        )
        self.assertEqual(
            ExchangeAppBatchMixin._spot_strategy_completion_text(TRADE_MODE_LADDER, 0),
            "分仓挂单结束",
        )

    def test_run_statistics_use_current_pair_and_exchange_time_window(self):
        app = ExchangeAppBatchMixin()
        client = Mock()
        client.current_server_time_ms.side_effect = [1000, 2000]
        client.spot_trade_volume_summary.return_value = {
            "quoteAsset": "USDT", "quoteVolume": "20.123456", "orderPnl": "-0.123456",
        }
        strategy = Mock()
        strategy.run.return_value = {"withdraw_amount": 9.5}
        stop = threading.Event()
        progress = Mock()
        result, stats = app._run_strategy_with_spot_statistics(
            strategy, client, stop, progress,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT, trade_mode=TRADE_MODE_LADDER,
            zero_round_spot_conversion=False, spot_symbol="BTCUSDT", quote_asset="USDT",
        )
        strategy.run.assert_called_once_with(stop, progress_cb=progress)
        self.assertIs(result, strategy.run.return_value)
        client.spot_trade_volume_summary.assert_called_once_with(
            "BTCUSDT", start_time_ms=1000, end_time_ms=2000, stop_event=stop, quote_asset="USDT",
        )
        self.assertEqual(stats, "BTCUSDT 交易量≈20.12346 USDT｜买卖收支差≈-0.12346 USDT")

    def test_statistics_failure_preserves_completed_trade_and_withdrawal(self):
        app = ExchangeAppBatchMixin()
        for error in (RuntimeError("network unavailable"), InterruptedError("stopped")):
            with self.subTest(error=type(error).__name__):
                client = Mock()
                client.spot_trade_volume_summary.side_effect = error
                strategy = Mock()
                strategy.run.return_value = {"withdraw_amount": 9.5}
                result, stats = app._run_strategy_with_spot_statistics(
                    strategy, client, threading.Event(), None,
                    trade_account_type=TRADE_ACCOUNT_TYPE_SPOT, trade_mode=TRADE_MODE_MARKET,
                    zero_round_spot_conversion=False, spot_symbol="BTCUSDT", quote_asset="USDT",
                )
                self.assertIs(result, strategy.run.return_value)
                strategy.run.assert_called_once()
                status, success = app._format_batch_strategy_completion_status(
                    result["withdraw_amount"], "USDT", enable_withdraw=True, trade_stats_text=stats,
                )
                self.assertTrue(success)
                self.assertEqual(status, "✔提现9.5 USDT｜BTCUSDT 交易统计暂不可用")

    def test_statistics_skip_other_modes_failed_and_stopped_runs(self):
        for account_type, mode, zero_round, failed, stopped in (
            (TRADE_ACCOUNT_TYPE_FUTURES, TRADE_MODE_MARKET, False, False, False),
            (TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_CONVERT, False, False, False),
            (TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET, True, False, False),
            (TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET, False, True, False),
            (TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_LADDER, False, False, True),
        ):
            with self.subTest(account_type=account_type, mode=mode, zero_round=zero_round, failed=failed, stopped=stopped):
                client, strategy = Mock(), Mock()
                strategy.run.return_value = {"trade_error": "order failed"} if failed else {}
                stop = threading.Event()
                if stopped:
                    stop.set()
                result, stats = ExchangeAppBatchMixin()._run_strategy_with_spot_statistics(
                    strategy, client, stop, None,
                    trade_account_type=account_type, trade_mode=mode,
                    zero_round_spot_conversion=zero_round, spot_symbol="BTCUSDT", quote_asset="USDT",
                )
                self.assertIs(result, strategy.run.return_value)
                self.assertEqual(stats, "")
                client.spot_trade_volume_summary.assert_not_called()
                if account_type == TRADE_ACCOUNT_TYPE_FUTURES or mode == TRADE_MODE_CONVERT or zero_round:
                    client.current_server_time_ms.assert_not_called()

    def test_invalid_statistics_are_not_reported_as_zero(self):
        for summary in ({}, {"quoteVolume": "NaN", "orderPnl": "0"}, {"quoteVolume": "1", "orderPnl": "bad"}):
            self.assertEqual(
                ExchangeAppBatchMixin._format_spot_run_trade_statistics("BTCUSDT", summary),
                "BTCUSDT 交易统计暂不可用",
            )

    def test_single_account_completion_survives_ui_reset_and_failure_takes_priority(self):
        app = ExchangeAppBatchMixin()
        app._restore_batch_ui_to_idle = Mock()
        app.api_key_var = Mock()
        app.api_key_var.get.return_value = ""
        app._on_worker_finished(completion_status="✔市价3轮结束｜BTCUSDT 交易量≈20 USDT｜买卖收支差≈0 USDT")
        self.assertIn("市价3轮结束", app._restore_batch_ui_to_idle.call_args.kwargs["status_text"])
        app._on_worker_finished("停止清理失败", "✔市价3轮结束")
        self.assertEqual(app._restore_batch_ui_to_idle.call_args.kwargs["status_text"], "状态：失败：停止清理失败")

    def test_trade_window_timestamp_uses_exchange_offset_and_cached_fallback(self):
        client = object.__new__(BinanceClient)
        client.spot = "https://clock-test.invalid"
        with patch.object(client, "_local_timestamp_ms", return_value=1000), \
             patch.object(client, "_sync_server_time_offset", return_value=5000):
            self.assertEqual(client.current_server_time_ms(), 6000)
        with patch.object(client, "_local_timestamp_ms", return_value=1000), \
             patch.object(client, "_sync_server_time_offset", side_effect=RuntimeError("offline")), \
             patch.dict(BinanceClient._shared_server_time_offset_ms, {client.spot: -500}):
            self.assertEqual(client.current_server_time_ms(), 500)


if __name__ == "__main__":
    unittest.main()
