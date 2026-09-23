#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import threading
import unittest
from decimal import Decimal
from unittest.mock import patch

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_app_base import ExchangeAppBase
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


class _PublicMarketClient(BinanceClient):
    def __init__(self, exchange_symbols, klines_by_symbol=None):
        self.spot = "spot"
        self.exchange_symbols = list(exchange_symbols)
        self.klines_by_symbol = dict(klines_by_symbol or {})
        self.public_requests: list[tuple[str, dict[str, object]]] = []

    def public_get(self, _base, path, params=None, **_kwargs):
        request_params = dict(params or {})
        self.public_requests.append((path, request_params))
        if path == "/api/v3/exchangeInfo":
            return {"symbols": self.exchange_symbols}
        if path == "/api/v3/klines":
            return self.klines_by_symbol.get(str(request_params.get("symbol")), [])
        raise AssertionError(f"unexpected public request: {path}")


_HOUR = 1_800_000_000_000
_END = _HOUR + 1_800_000


def _fill(trade_id, amount, *, buy=True, fee="0", fee_asset="BNB", timestamp=_HOUR + 1000, order_id=None):
    return {
        "id": trade_id, "orderId": trade_id if order_id is None else order_id,
        "time": timestamp, "quoteQty": str(amount), "isBuyer": buy,
        "commission": fee, "commissionAsset": fee_asset,
    }


class _Widget(dict):
    def __init__(self, value=None):
        super().__init__()
        self.value = value

    def config(self, **kwargs):
        self.update(kwargs)

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class _MonthlyClient(BinanceClient):
    """Run the real pagination/valuation code, with no network or credentials."""
    def __init__(self, app, key):
        self.spot = "spot"
        self.app, self.key = app, key
        self.positions = {}

    def get_spot_symbol_catalog(self):
        return [
            {"symbol": "XPLUSDT", "baseAsset": "XPL", "quoteAsset": "USDT"},
            {"symbol": "XPLUSDC", "baseAsset": "XPL", "quoteAsset": "USDC"},
            {"symbol": "BNBUSDT", "baseAsset": "BNB", "quoteAsset": "USDT"},
            {"symbol": "USDCUSDT", "baseAsset": "USDC", "quoteAsset": "USDT"},
        ]

    def request(self, _base, method, path, params=None, **kwargs):
        assert method == "GET" and path == "/api/v3/myTrades"
        symbol = params["symbol"]
        self.app.trade_requests.append((self.key, symbol, dict(params)))
        pages = self.app.pages.get((self.key, symbol), [[]])
        pos = self.positions.get(symbol, 0)
        self.positions[symbol] = pos + 1
        page = pages[pos] if pos < len(pages) else []
        if callable(page):
            page = page()
        if isinstance(page, Exception):
            raise page
        return copy.deepcopy(page)

    def public_get(self, _base, path, params=None):
        assert path == "/api/v3/klines"
        return self._prices(self.app.initial_rates, params)

    def public_get_query(self, _base, path, params=None, **kwargs):
        assert path == "/api/v3/klines"
        self.app.repair_requests.append((dict(params), kwargs))
        if self.app.repair_hook:
            self.app.repair_hook()
        return self._prices(self.app.repair_rates, params)

    @staticmethod
    def _prices(rates, params):
        asset = params["symbol"][:-4]
        rows = rates.get(asset, {})
        if isinstance(rows, Exception):
            raise rows
        return [[hour, "0", "0", "0", str(price)] for hour, price in rows.items()
                if params["startTime"] <= hour <= params["endTime"]]


class _MonthlyApp(ExchangeAppBatchMixin):
    _delay_var_value = ExchangeAppBase._delay_var_value
    _normalized_entry_text = staticmethod(ExchangeAppBase._normalized_entry_text)

    def __init__(self, account_count=1, threads=1):
        self.worker_thread = None
        self.accounts = [{"api_key": str(i), "api_secret": "dummy"} for i in range(account_count)]
        self.account_stops = {acc["api_key"]: threading.Event() for acc in self.accounts}
        self.pages, self.statuses, self.active = {}, {}, {}
        self.initial_rates = {"BNB": {_HOUR: Decimal("300")}, "USDC": {_HOUR: Decimal("1")}}
        self.repair_rates = {}
        self.repair_hook = None
        self.trade_requests, self.repair_requests, self.closed = [], [], []
        self.created_gates = []
        self.spot_symbol_var, self.max_threads_var = _Widget("XPLUSDT"), _Widget(str(threads))
        self.min_delay_var, self.max_delay_var = _Widget("0"), _Widget("0")
        self.status_var, self.progress = _Widget(), _Widget()
        self.final_status = None
        for name in ("btn_start", "btn_stop", "btn_run_accounts", "btn_query_all_assets",
                     "btn_collect_bnb_combo", "btn_batch_withdraw", "btn_batch_redeem_earn",
                     "btn_refresh", "btn_withdraw"):
            setattr(self, name, _Widget())

    def _prepare_accounts_for_batch(self, accounts):
        self.active.update({acc["api_key"]: True for acc in accounts})

    def _set_account_manage_buttons_state(self, state):
        pass

    def _set_combo_states_for_run(self, running):
        pass

    def _account_display_index(self, acc, pos):
        return pos

    def _get_account_stop_event(self, acc):
        return self.account_stops[acc["api_key"]]

    def _schedule_account_status(self, acc, status):
        self.statuses[acc["api_key"]] = status

    def _create_binance_client(self, key, secret, **kwargs):
        self.created_gates.append(kwargs["request_gate"])
        return _MonthlyClient(self, key)

    def _close_binance_client_instance(self, client):
        if client:
            self.closed.append(client)

    def _set_account_batch_active(self, acc, active):
        self.active[acc["api_key"]] = active

    def _dispatch_ui(self, fn):
        fn()

    def _restore_batch_ui_to_idle(self, *, status_text, **kwargs):
        self.final_status = status_text
        super()._restore_batch_ui_to_idle(status_text=status_text, **kwargs)

    def after(self, delay, fn):
        pass

    def _start_managed_thread(self, target, **kwargs):
        target()
        return None

    def query(self):
        with patch("exchange_app_batch.time.time", return_value=_END / 1000), patch("exchange_app_batch.logger") as log:
            self.run_query_monthly_trade_volume(accounts_to_run=self.accounts)
        self.logs = "\n".join(str(call.args) for call in log.method_calls)


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

    def test_values_volume_and_commission_with_hourly_usdt_prices(self) -> None:
        client = _TradeHistoryClient([[
            {
                "id": 1,
                "orderId": 1,
                "quoteQty": "10",
                "isBuyer": True,
                "commission": "0.001",
                "commissionAsset": "BNB",
                "time": 3_600_100,
            },
            {
                "id": 2,
                "orderId": 2,
                "quoteQty": "15",
                "isBuyer": False,
                "commission": "0.1",
                "commissionAsset": "USDC",
                "time": 3_600_200,
            },
        ]])
        prices = {"USDC": Decimal("0.999"), "BNB": Decimal("300")}

        summary = client.spot_trade_volume_summary(
            "XPLUSDC",
            start_time_ms=3_600_000,
            end_time_ms=3_700_000,
            valuation_price_lookup=lambda asset, _timestamp: prices.get(asset),
        )

        self.assertEqual(summary["usdtQuoteVolume"], Decimal("24.975"))
        self.assertEqual(summary["usdtBuyQuoteVolume"], Decimal("9.990"))
        self.assertEqual(summary["usdtSellQuoteVolume"], Decimal("14.985"))
        self.assertEqual(summary["usdtOrderPnl"], Decimal("4.995"))
        self.assertEqual(summary["usdtCommission"], Decimal("0.3999"))
        self.assertEqual(summary["unvaluedQuoteVolumeByAsset"], {})
        self.assertEqual(summary["unvaluedCommissionByAsset"], {})

    def test_uses_catalog_quote_asset_outside_legacy_suffixes(self) -> None:
        client = _TradeHistoryClient([[
            {
                "id": 1,
                "orderId": 1,
                "quoteQty": "125000",
                "isBuyer": False,
                "time": 1_100,
            },
        ]])

        summary = client.spot_trade_volume_summary(
            "BTCARS",
            start_time_ms=1_000,
            end_time_ms=2_000,
            quote_asset="ARS",
        )

        self.assertEqual(summary["quoteAsset"], "ARS")
        self.assertEqual(summary["quoteVolume"], Decimal("125000"))
        self.assertEqual(summary["unvaluedQuoteVolumeByAsset"], {"ARS": Decimal("125000")})

    def test_discovers_only_supported_quote_pairs_for_selected_base_asset(self) -> None:
        symbols = [
            {"symbol": "XPLUSDC", "baseAsset": "XPL", "quoteAsset": "USDC", "status": "TRADING"},
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "XPLUSDT", "baseAsset": "XPL", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "XPLBNB", "baseAsset": "XPL", "quoteAsset": "BNB", "status": "BREAK"},
            {"symbol": "XPLETH", "baseAsset": "XPL", "quoteAsset": "ETH", "status": "TRADING"},
            {"symbol": "XPLBTC", "baseAsset": "XPL", "quoteAsset": "BTC", "status": "TRADING"},
            {"symbol": "XPLFDUSD", "baseAsset": "XPL", "quoteAsset": "FDUSD", "status": "TRADING"},
            {"symbol": "XPLTRY", "baseAsset": "XPL", "quoteAsset": "TRY", "status": "TRADING"},
            {
                "symbol": "XPLUSDCOLD",
                "baseAsset": "XPL",
                "quoteAsset": "USDC",
                "status": "TRADING",
                "isSpotTradingAllowed": False,
            },
        ]
        client = _PublicMarketClient(symbols)

        catalog = client.get_spot_symbol_catalog()
        base_asset, pairs = client.get_spot_symbols_for_base_asset("xplusdt", spot_symbols=catalog)

        self.assertEqual(base_asset, "XPL")
        self.assertEqual(
            [item["symbol"] for item in pairs],
            ["XPLUSDT", "XPLBNB", "XPLBTC", "XPLETH", "XPLUSDC"],
        )
        self.assertEqual(client.public_requests[0][0], "/api/v3/exchangeInfo")

    def test_loads_direct_and_inverse_hourly_usdt_rates(self) -> None:
        symbols = [
            {"symbol": "BNBUSDT", "baseAsset": "BNB", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "USDCUSDT", "baseAsset": "USDC", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "USDTTRY", "baseAsset": "USDT", "quoteAsset": "TRY", "status": "TRADING"},
        ]
        client = _PublicMarketClient(
            symbols,
            {
                "BNBUSDT": [[0, "0", "0", "0", "300"]],
                "USDCUSDT": [[0, "0", "0", "0", "0.999"]],
                "USDTTRY": [[0, "0", "0", "0", "40"]],
            },
        )

        rates = client.get_historical_usdt_hourly_rates(
            {"USDT", "BNB", "USDC", "TRY", "MISSING"},
            start_time_ms=1_000,
            end_time_ms=3_000,
            spot_symbols=client.get_spot_symbol_catalog(),
        )

        self.assertEqual(BinanceClient.historical_usdt_price_lookup(rates, "USDT", 1_500), Decimal("1"))
        self.assertEqual(BinanceClient.historical_usdt_price_lookup(rates, "BNB", 1_500), Decimal("300"))
        self.assertEqual(BinanceClient.historical_usdt_price_lookup(rates, "USDC", 1_500), Decimal("0.999"))
        self.assertEqual(BinanceClient.historical_usdt_price_lookup(rates, "TRY", 1_500), Decimal("0.025"))
        self.assertIsNone(BinanceClient.historical_usdt_price_lookup(rates, "MISSING", 1_500))

    def test_merges_multiple_pair_summaries_without_mixing_native_quote_assets(self) -> None:
        summaries = [
            {
                "quoteAsset": "USDT",
                "quoteVolume": Decimal("100"),
                "commissionByAsset": {"BNB": Decimal("0.01")},
                "usdtQuoteVolume": Decimal("100"),
                "usdtBuyQuoteVolume": Decimal("60"),
                "usdtSellQuoteVolume": Decimal("40"),
                "usdtCommission": Decimal("3"),
                "tradeCount": 2,
                "orderCount": 2,
            },
            {
                "quoteAsset": "USDC",
                "quoteVolume": Decimal("50"),
                "commissionByAsset": {"XPL": Decimal("2")},
                "usdtQuoteVolume": Decimal("49.95"),
                "usdtBuyQuoteVolume": Decimal("20"),
                "usdtSellQuoteVolume": Decimal("29.95"),
                "usdtCommission": Decimal("0.2"),
                "tradeCount": 3,
                "orderCount": 1,
            },
        ]

        merged = ExchangeAppBatchMixin._merge_monthly_trade_volume_summaries(summaries)

        self.assertEqual(merged["quoteVolumes"], {"USDT": Decimal("100"), "USDC": Decimal("50")})
        self.assertEqual(merged["commissionByAsset"], {"BNB": Decimal("0.01"), "XPL": Decimal("2")})
        self.assertEqual(merged["usdtQuoteVolume"], Decimal("149.95"))
        self.assertEqual(merged["usdtOrderPnl"], Decimal("-10.05"))
        self.assertEqual(merged["usdtNetResult"], Decimal("-13.25"))
        self.assertEqual(merged["tradeCount"], 5)
        self.assertEqual(merged["orderCount"], 3)

    def test_missing_prices_revalue_once_and_match_initially_complete_result(self):
        rows = [
            _fill(1, "10", fee="0.001"),
            _fill(2, "15", buy=False, fee="0.1", fee_asset="USDC"),
            _fill(3, "20", timestamp=_HOUR - 1000, fee="0.002"),
        ]
        rates = {
            "USDC": {_HOUR: Decimal("0.999"), _HOUR - 3_600_000: Decimal("0.998")},
            "BNB": {_HOUR: Decimal("300"), _HOUR - 3_600_000: Decimal("290")},
        }

        def summarize(lookup=None):
            return _TradeHistoryClient([rows]).spot_trade_volume_summary(
                "XPLUSDC", start_time_ms=_HOUR - 3_600_000, end_time_ms=_END,
                valuation_price_lookup=lookup,
            )

        expected = summarize(lambda asset, ts: BinanceClient.historical_usdt_price_lookup(rates, asset, ts))
        result = summarize()
        native_before = copy.deepcopy(result["commissionByAsset"])
        self.assertEqual(len(result["unvaluedHourlyAmounts"]), 4)
        BinanceClient.revalue_spot_trade_summary(result, {"BNB": rates["BNB"]})
        self.assertEqual(result["unvaluedCommissionByAsset"], {"USDC": Decimal("0.1")})
        self.assertEqual(result["usdtQuoteVolume"], 0)
        BinanceClient.revalue_spot_trade_summary(result, rates)
        self.assertEqual(result, expected)
        BinanceClient.revalue_spot_trade_summary(result, rates)
        self.assertEqual(result, expected)
        self.assertEqual(result["commissionByAsset"], native_before)

    def test_zero_amounts_do_not_require_valuation(self):
        result = _TradeHistoryClient([[_fill(1, "0")]]).spot_trade_volume_summary(
            "XPLUSDC", start_time_ms=_HOUR, end_time_ms=_END,
        )
        self.assertEqual(result["unvaluedHourlyAmounts"], {})
        self.assertEqual(result["unvaluedQuoteVolumeByAsset"], {})

    def test_duplicate_page_boundary_does_not_duplicate_volume_or_fee_buckets(self):
        first = [_fill(i, "1", fee="0.001", order_id=i // 2) for i in range(1000)]
        client = _TradeHistoryClient([first, [first[-1], _fill(1000, "2", fee="0.002")]])
        result = client.spot_trade_volume_summary("XPLUSDC", start_time_ms=_HOUR, end_time_ms=_END)
        rates = {"USDC": {_HOUR: Decimal("1")}, "BNB": {_HOUR: Decimal("300")}}
        BinanceClient.revalue_spot_trade_summary(result, rates)
        self.assertEqual(result["quoteVolume"], Decimal("1002"))
        self.assertEqual(result["usdtQuoteVolume"], Decimal("1002"))
        self.assertEqual(result["usdtCommission"], Decimal("300.6"))
        self.assertEqual(result["tradeCount"], 1001)
        self.assertEqual(result["orderCount"], 501)

    def test_invalid_historical_prices_remain_unvalued(self):
        client = _PublicMarketClient(
            [{"symbol": "BNBUSDT", "baseAsset": "BNB", "quoteAsset": "USDT"}],
            {"BNBUSDT": [[0, "0", "0", "0", "NaN"], [3_600_000, "0", "0", "0", "Infinity"]]},
        )
        self.assertEqual(client.get_historical_usdt_hourly_rates(
            {"BNB"}, start_time_ms=0, end_time_ms=4_000_000,
            spot_symbols=client.get_spot_symbol_catalog(),
        ), {})

    def test_normal_controller_output_and_formula_are_unchanged(self):
        app = _MonthlyApp()
        app.pages[("0", "XPLUSDT")] = [[
            _fill(1, "4000", fee="3", fee_asset="USDT", order_id=100),
            _fill(2, "6000", fee="5", fee_asset="USDT", order_id=100),
            _fill(3, "9990", buy=False, fee="2", fee_asset="USDT", order_id=101),
        ]]
        app.query()
        self.assertEqual(app.statuses["0"], "✔XPL 2对｜≈19990 USDT｜净≈-10｜费≈10｜总盈亏≈-20")
        self.assertEqual(app.final_status, "状态：XPL月成交 2对｜≈19990 USDT｜净≈-10｜费≈10｜1/1（详情见日志）")
        self.assertIn("订单/成交明细：2单/3笔", app.logs)
        self.assertIn("全部账号成交汇总（成功1/1）", app.logs)
        self.assertEqual(app.repair_requests, [])
        self.assertEqual(len(app.trade_requests), 2)
        self.assertEqual(len(app.closed), 2)

    def test_monthly_query_has_no_per_request_delay(self):
        app = _MonthlyApp(account_count=2, threads=2)
        app.initial_rates = {}
        app.repair_rates = {"BNB": {_HOUR: Decimal("300")}}
        for acc in app.accounts:
            app.pages[(acc["api_key"], "XPLUSDT")] = [[_fill(1, "100", fee="0.01")]]
        app.query()
        self.assertEqual(len(app.created_gates), 4)  # 初始化、两个账号、集中补价。
        gate = app.created_gates[0]
        self.assertTrue(all(item is gate for item in app.created_gates))
        self.assertEqual(gate.wait_seconds, 10)
        with patch.object(app.stop_event, "wait", side_effect=AssertionError("unexpected request delay")), \
                patch("exchange_binance_client.time.sleep", side_effect=AssertionError("unexpected request delay")):
            for _ in range(5):
                self.assertTrue(gate.wait())
        self.assertIn("≈200 USDT｜净≈-200｜费≈6｜2/2", app.final_status)

    def test_five_workers_start_immediately_and_only_finished_worker_delays_next_account(self):
        app = _MonthlyApp(account_count=6, threads=5)
        app.min_delay_var.set("1200")
        app.max_delay_var.set("2600")
        first_wave = threading.Barrier(5)
        sixth_started = threading.Event()
        worker_ids, delays = {}, []

        def first_page(key):
            def run():
                worker_ids[key] = threading.get_ident()
                first_wave.wait(timeout=5)
                if key != "0":
                    self.assertTrue(sixth_started.wait(timeout=5))
                return []
            return run

        for i in range(5):
            app.pages[(str(i), "XPLUSDT")] = [first_page(str(i))]

        def sixth_page():
            worker_ids["5"] = threading.get_ident()
            sixth_started.set()
            return []

        app.pages[("5", "XPLUSDT")] = [sixth_page]

        def wait_between_accounts(stop_event, low, high, **kwargs):
            self.assertTrue(app.statuses["0"].startswith("✔"))
            self.assertEqual([symbol for key, symbol, _ in app.trade_requests if key == "0"], ["XPLUSDT", "XPLUSDC"])
            delays.append((threading.get_ident(), low, high, kwargs["log_prefix"]))
            return True

        with patch.object(app, "_wait_batch_random_delay", side_effect=wait_between_accounts):
            app.query()
        self.assertEqual(len(delays), 1)
        self.assertEqual(delays[0][:3], (worker_ids["0"], 1200, 2600))
        self.assertIn("切换账号 #6", delays[0][3])
        self.assertEqual(worker_ids["0"], worker_ids["5"])
        self.assertTrue(all(status.startswith("✔") for status in app.statuses.values()))
        self.assertIn("6/6", app.final_status)

    def test_account_switch_uses_ui_range_and_existing_delay_defaults(self):
        for low, high, expected in (
            ("1200", "2600", (1200, 2600)),
            ("2600", "1200", (1200, 2600)),
            ("最小", "最大", (1000, 3000)),
        ):
            with self.subTest(low=low, high=high):
                app = _MonthlyApp(account_count=3)
                app.min_delay_var.set(low)
                app.max_delay_var.set(high)
                with patch("exchange_app_batch.random.randint", return_value=1800) as randint, \
                        patch("exchange_app_batch.CombinedStopEvent.wait", return_value=False) as wait:
                    app.query()
                self.assertEqual([call.args for call in randint.call_args_list], [expected, expected])
                self.assertEqual([call.args for call in wait.call_args_list], [(1.8,), (1.8,)])
                self.assertIn("3/3", app.final_status)

    def test_single_account_pagination_and_price_repair_do_not_add_random_wait(self):
        app = _MonthlyApp()
        app.min_delay_var.set("1000")
        app.max_delay_var.set("3000")
        app.initial_rates.pop("BNB")
        app.repair_rates = {"BNB": {_HOUR: Decimal("300")}}
        app.pages[("0", "XPLUSDT")] = [
            [_fill(i, "1", fee="0.001") for i in range(1000)],
            [_fill(1000, "1")],
        ]
        with patch.object(app, "_wait_batch_random_delay", return_value=True) as wait:
            app.query()
        wait.assert_not_called()
        self.assertEqual(len(app.trade_requests), 3)
        self.assertEqual(len(app.repair_requests), 1)
        self.assertIn("≈1001 USDT｜净≈-1001｜费≈300", app.final_status)

    def test_zero_config_does_not_wait_between_accounts(self):
        app = _MonthlyApp(account_count=3)
        with patch("exchange_app_batch.random.randint") as randint, \
                patch("exchange_app_batch.CombinedStopEvent.wait") as wait:
            app.query()
        randint.assert_not_called()
        wait.assert_not_called()
        self.assertIn("3/3", app.final_status)

    def test_stop_during_account_switch_keeps_first_result_and_skips_later_requests(self):
        app = _MonthlyApp(account_count=3)
        app.min_delay_var.set("1000")
        app.max_delay_var.set("3000")
        app.pages[("0", "XPLUSDT")] = [[_fill(1, "100")]]

        def stop(seconds):
            app.stop_event.set()
            return True

        with patch("exchange_app_batch.CombinedStopEvent.wait", side_effect=stop) as wait:
            app.query()
        self.assertEqual(wait.call_count, 1)
        self.assertEqual([key for key, _, _ in app.trade_requests], ["0", "0"])
        self.assertTrue(app.statuses["0"].startswith("✔"))
        self.assertEqual(app.statuses["1"], "已停止")
        self.assertEqual(app.statuses["2"], "已停止")
        self.assertIn("已停止，部分汇总｜≈100", app.final_status)
        self.assertFalse(any(app.active.values()))

    def test_invalid_random_delay_is_rejected_before_starting_queries(self):
        app = _MonthlyApp()
        app.min_delay_var.set("not a number")
        with patch("exchange_app_batch.messagebox.showerror") as error:
            app.query()
        error.assert_called_once()
        self.assertEqual(app.created_gates, [])
        self.assertEqual(app.trade_requests, [])

    def test_pair_failure_preserves_other_pair_in_either_order(self):
        for failed, successful in (("XPLUSDT", "XPLUSDC"), ("XPLUSDC", "XPLUSDT")):
            with self.subTest(failed=failed):
                app = _MonthlyApp()
                app.pages[("0", failed)] = [TimeoutError("synthetic timeout")]
                app.pages[("0", successful)] = [[_fill(1, "100")]]
                app.query()
                self.assertIn("部分1/2｜≈100 USDT", app.statuses["0"])
                self.assertFalse(app.statuses["0"].startswith("✔"))
                self.assertIn("部分汇总｜≈100 USDT", app.final_status)
                self.assertIn("synthetic timeout", app.logs)
                self.assertIn("仅含已查部分", app.logs)
                self.assertEqual(len(app.trade_requests), 2)

    def test_mid_pagination_failure_does_not_count_unfinished_symbol(self):
        app = _MonthlyApp()
        app.pages[("0", "XPLUSDT")] = [[_fill(i, "1") for i in range(1000)], TimeoutError("page 2")]
        app.pages[("0", "XPLUSDC")] = [[_fill(1, "20")]]
        app.query()
        self.assertIn("部分1/2｜≈20 USDT", app.statuses["0"])
        self.assertEqual(app.trade_requests[1][2]["fromId"], 1000)

    def test_all_failed_is_distinct_from_successful_zero_trades(self):
        app = _MonthlyApp()
        app.pages = {("0", symbol): [TimeoutError("offline")] for symbol in ("XPLUSDT", "XPLUSDC")}
        app.query()
        self.assertIn("查询失败", app.statuses["0"])
        self.assertIn("查询失败", app.final_status)
        self.assertNotIn("≈0", app.final_status)
        self.assertNotIn("总盈亏≈0", app.logs)
        empty = _MonthlyApp()
        empty.query()
        self.assertEqual(empty.statuses["0"], "✔XPL 2对｜≈0 USDT｜净≈0｜费≈0｜总盈亏≈0")
        self.assertIn("成功1/1", empty.logs)

    def test_partial_account_and_complete_account_both_enter_global_sum(self):
        app = _MonthlyApp(account_count=2, threads=2)
        app.pages[("0", "XPLUSDT")] = [[_fill(1, "10")]]
        app.pages[("0", "XPLUSDC")] = [TimeoutError("offline")]
        app.pages[("1", "XPLUSDT")] = [[_fill(1, "20")]]
        app.query()
        self.assertIn("部分汇总｜≈30 USDT", app.final_status)
        self.assertIn("完整账号1/2", app.logs)
        self.assertTrue(app.statuses["1"].startswith("✔"))

    def test_shared_repair_revalues_all_accounts_without_refetching_trades(self):
        app = _MonthlyApp(account_count=3, threads=2)
        app.initial_rates = {}
        app.repair_rates = {"BNB": {_HOUR: Decimal("300")}, "USDC": {_HOUR: Decimal("0.999")}}
        for acc in app.accounts:
            app.pages[(acc["api_key"], "XPLUSDC")] = [[_fill(1, "100", fee="0.01")]]
        app.query()
        self.assertEqual(len(app.repair_requests), 2)
        self.assertEqual(len(app.trade_requests), 6)
        for params, kwargs in app.repair_requests:
            self.assertEqual(params["startTime"], _HOUR)
            self.assertEqual(params["endTime"], _END)
            self.assertEqual(kwargs["max_retries"], 1)
        for status in app.statuses.values():
            self.assertEqual(status, "✔XPL 2对｜≈99.9 USDT｜净≈-99.9｜费≈3｜总盈亏≈-102.9")
        self.assertIn("≈299.7 USDT｜净≈-299.7｜费≈9｜3/3", app.final_status)
        self.assertIn("总盈亏≈-308.7 USDT", app.logs)
        self.assertNotIn("待折算", app.logs)
        self.assertFalse(any(app.active.values()))

    def test_repair_only_fills_missing_hours_without_changing_existing_rates(self):
        app = _MonthlyApp()
        old_hour = _HOUR - 3_600_000
        app.pages[("0", "XPLUSDC")] = [[_fill(1, "1"), _fill(2, "2", timestamp=old_hour + 1000)]]
        app.repair_rates = {"USDC": {old_hour: Decimal("1.1"), _HOUR: Decimal("9")}}
        app.query()
        self.assertIn("≈3.2 USDT", app.final_status)
        self.assertEqual(len(app.repair_requests), 1)
        params = app.repair_requests[0][0]
        self.assertEqual(params["startTime"], old_hour)
        self.assertEqual(params["endTime"], _HOUR - 1)

    def test_missing_fee_price_never_displays_complete_net_result(self):
        app = _MonthlyApp()
        app.initial_rates.pop("BNB")
        app.repair_rates["BNB"] = TimeoutError("price offline")
        app.pages[("0", "XPLUSDT")] = [[_fill(1, "100", fee="0.01")]]
        app.query()
        self.assertEqual(app.statuses["0"], "XPL 2对｜≈100 USDT｜净≈-100｜费待折算｜总盈亏待折算")
        self.assertIn("手续费：BNB 0.01", app.logs)
        self.assertIn("总盈亏：待折算", app.logs)
        self.assertNotIn("总盈亏≈", app.logs)
        self.assertEqual(len(app.repair_requests), 1)

    def test_missing_quote_price_never_displays_complete_pnl(self):
        app = _MonthlyApp()
        app.initial_rates.pop("USDC")
        app.pages[("0", "XPLUSDT")] = [[_fill(1, "100", buy=False)]]
        app.pages[("0", "XPLUSDC")] = [[_fill(1, "101")]]
        app.query()
        self.assertIn("≥100 USDT｜净待折算｜费≈0｜总盈亏待折算", app.statuses["0"])
        self.assertIn("交易净额：待折算", app.logs)
        self.assertNotIn("总盈亏≈", app.logs)
        self.assertIn("未折算成交额：USDC 101", app.logs)

    def test_failed_price_asset_does_not_block_other_price_repair(self):
        app = _MonthlyApp()
        app.initial_rates = {}
        app.repair_rates = {"BNB": TimeoutError("offline"), "USDC": {_HOUR: Decimal("0.999")}}
        app.pages[("0", "XPLUSDC")] = [[_fill(1, "100", fee="0.01")]]
        app.query()
        self.assertEqual(len(app.repair_requests), 2)
        self.assertIn("≈99.9 USDT｜净≈-99.9｜费待折算｜总盈亏待折算", app.statuses["0"])
        self.assertIn("折合成交额≈99.9 USDT", app.logs)
        self.assertNotIn("未折算成交额", app.logs)

    def test_failure_restores_ui_and_next_query_starts_cleanly(self):
        app = _MonthlyApp()
        app.pages = {("0", symbol): [TimeoutError("offline")] for symbol in ("XPLUSDT", "XPLUSDC")}
        app.query()
        self.assertFalse(app._batch_task_active)
        self.assertEqual(app.btn_start["state"], "normal")
        self.assertEqual(app.btn_stop["state"], "disabled")
        app.pages = {("0", "XPLUSDT"): [[_fill(1, "20")]]}
        app.query()
        self.assertEqual(app.statuses["0"], "✔XPL 2对｜≈20 USDT｜净≈-20｜费≈0｜总盈亏≈-20")
        self.assertEqual(app.progress["value"], 0)
        self.assertFalse(any(app.active.values()))

    def test_stop_preserves_completed_pair_and_clears_queued_accounts(self):
        app = _MonthlyApp(account_count=2)
        app.pages[("0", "XPLUSDT")] = [[_fill(1, "100")]]

        def stop():
            app.stop_event.set()
            raise RuntimeError("检测到停止信号")

        app.pages[("0", "XPLUSDC")] = [stop]
        app.query()
        self.assertIn("已停止 部分1/2｜≈100", app.statuses["0"])
        self.assertEqual(app.statuses["1"], "已停止")
        self.assertIn("已停止，部分汇总｜≈100", app.final_status)
        self.assertNotIn("查询失败", app.logs)
        self.assertFalse(any(app.active.values()))
        self.assertEqual(app.repair_requests, [])

    def test_account_stop_does_not_prevent_other_account_query(self):
        app = _MonthlyApp(account_count=2)

        def stop():
            app.account_stops["0"].set()
            raise RuntimeError("account stopped")

        app.pages[("0", "XPLUSDT")] = [stop]
        app.pages[("1", "XPLUSDT")] = [[_fill(1, "20")]]
        app.query()
        self.assertEqual(app.statuses["0"], "已停止")
        self.assertTrue(app.statuses["1"].startswith("✔"))
        self.assertIn("≈20 USDT", app.final_status)

    def test_stop_during_repair_does_not_publish_success_or_start_next_asset(self):
        app = _MonthlyApp()
        app.initial_rates = {}
        app.pages[("0", "XPLUSDC")] = [[_fill(1, "100", fee="0.01")]]
        app.repair_rates = {"BNB": {_HOUR: Decimal("300")}}
        app.repair_hook = lambda: app.stop_event.set()
        app.query()
        self.assertIn("已停止", app.statuses["0"])
        self.assertIn("总盈亏待折算", app.statuses["0"])
        self.assertIn("已停止", app.final_status)
        self.assertEqual(len(app.repair_requests), 1)
        self.assertEqual(len(app.closed), 3)

    def test_stop_during_initial_price_query_marks_all_accounts_stopped(self):
        app = _MonthlyApp(account_count=2)

        def stop(client, *args, **kwargs):
            app.stop_event.set()
            raise RuntimeError("检测到停止信号")

        with patch.object(_MonthlyClient, "public_get", stop):
            app.query()
        self.assertTrue(all("已停止" in status for status in app.statuses.values()))
        self.assertIn("已停止", app.final_status)
        self.assertNotIn("查询失败", app.logs)
        self.assertFalse(any(app.active.values()))
        self.assertEqual(app.trade_requests, [])
        self.assertEqual(app.repair_requests, [])

    def test_global_sum_uses_unrounded_values(self):
        app = _MonthlyApp(account_count=2)
        for acc in app.accounts:
            app.pages[(acc["api_key"], "XPLUSDT")] = [[_fill(1, "0.6")]]
        app.query()
        self.assertTrue(all("≈0.6 USDT" in status for status in app.statuses.values()))
        self.assertIn("≈1.2 USDT｜净≈-1.2", app.final_status)

    def test_monthly_statistics_round_to_five_decimal_places(self):
        app = _MonthlyApp()
        app.pages[("0", "XPLUSDT")] = [[
            _fill(1, "1.2345678", fee="0.000006", fee_asset="USDT")
        ]]
        app.query()
        self.assertEqual(
            app.statuses["0"],
            "✔XPL 2对｜≈1.23457 USDT｜净≈-1.23457｜费≈0.00001｜总盈亏≈-1.23457",
        )
        self.assertIn("折合成交额≈1.23457 USDT", app.logs)


if __name__ == "__main__":
    unittest.main()
