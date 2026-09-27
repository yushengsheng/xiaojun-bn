#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
import time
from decimal import Decimal

from exchange_binance_client import BinanceAPIError, BinanceClient
from exchange_constants import (
    LADDER_LAYOUT_BUY1_SELL1,
    LADDER_LAYOUT_BUY1_SELL2,
    LADDER_LAYOUT_BUY2_SELL1,
    LADDER_LAYOUT_BUY2_SELL2,
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_MODE_LADDER,
)
from exchange_strategy import Strategy


class _PriceRulesClient:
    @staticmethod
    def get_symbol_tick_size(_symbol: str) -> Decimal:
        return Decimal("0.001")

    @staticmethod
    def get_symbol_trade_rules(_symbol: str) -> dict[str, Decimal]:
        return {"tickSize": Decimal("0.001"), "minNotional": Decimal("5")}

    @staticmethod
    def adjust_price_to_valid_tick(_symbol: str, price: Decimal, *, round_up: bool = False) -> Decimal:
        step = Decimal("0.001")
        units = Decimal(str(price)) / step
        integral = units.to_integral_value(rounding="ROUND_CEILING" if round_up else "ROUND_FLOOR")
        return integral * step

    @staticmethod
    def normalize_price_delta(_symbol: str, delta: Decimal, *, min_one_tick: bool = True) -> Decimal:
        value = Decimal(str(delta))
        if min_one_tick and value <= 0:
            return Decimal("0.001")
        return value


class _StopAfterOrders:
    def __init__(self):
        self.stopped = False

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, _seconds: float) -> bool:
        return self.stopped


class _StopAfterTwoLoops(_StopAfterOrders):
    def __init__(self):
        super().__init__()
        self.wait_count = 0

    def wait(self, _seconds: float) -> bool:
        self.wait_count += 1
        if self.wait_count >= 2:
            self.stopped = True
        return self.stopped


class _StopAfterThreeLoops(_StopAfterOrders):
    def __init__(self):
        super().__init__()
        self.wait_count = 0

    def wait(self, _seconds: float) -> bool:
        self.wait_count += 1
        if self.wait_count >= 3:
            self.stopped = True
        return self.stopped


class _RunningLadderClient(_PriceRulesClient):
    def __init__(self, stop_event: _StopAfterOrders, *, stop_after_placements: int | None = 2):
        self.stop_event = stop_event
        self.stop_after_placements = stop_after_placements
        self.quote_free = Decimal("100")
        self.base_free = Decimal("0")
        self.orders: dict[str, dict[str, object]] = {}
        self.placements: list[dict[str, object]] = []
        self.next_order_id = 1

    def _stop_if_ready(self) -> None:
        if self.stop_after_placements is not None and len(self.placements) >= self.stop_after_placements:
            self.stop_event.stopped = True

    @staticmethod
    def get_spot_base_asset(_symbol: str) -> str:
        return "BTC"

    @staticmethod
    def get_spot_quote_asset(_symbol: str) -> str:
        return "USDT"

    @staticmethod
    def collect_asset_to_spot(_asset: str, *, context: str = "交易前") -> int:
        return 1

    @staticmethod
    def get_book_ticker(_symbol: str) -> dict[str, Decimal]:
        return {"bidPrice": Decimal("10.111"), "askPrice": Decimal("10.112")}

    @staticmethod
    def get_last_spot_trade_time_ms(_symbol: str) -> int:
        return int(time.time() * 1000)

    def spot_balance_snapshot(self, _assets) -> dict[str, dict[str, Decimal]]:
        return {
            "USDT": {"free": self.quote_free, "locked": Decimal("0"), "total": self.quote_free},
            "BTC": {"free": self.base_free, "locked": Decimal("0"), "total": self.base_free},
            "BNB": {"free": Decimal("1"), "locked": Decimal("0"), "total": Decimal("1")},
        }

    def spot_limit_buy_quote_amount(
        self,
        _symbol: str,
        price: Decimal,
        quote_amount: Decimal,
        *,
        maker_only: bool = False,
    ) -> dict[str, object]:
        amount = Decimal(str(quote_amount))
        self.quote_free -= amount
        order_id = str(self.next_order_id)
        self.next_order_id += 1
        order = {
            "orderId": order_id,
            "side": "BUY",
            "status": "NEW",
            "price": str(price),
            "origQty": str(amount / Decimal(str(price))),
            "executedQty": "0",
        }
        self.orders[order_id] = order
        self.placements.append(
            {"side": "BUY", "price": Decimal(str(price)), "amount": amount, "maker_only": maker_only}
        )
        self._stop_if_ready()
        return dict(order)

    def spot_limit_sell_quantity(
        self,
        _symbol: str,
        price: Decimal,
        quantity: Decimal,
        *,
        maker_only: bool = False,
    ) -> dict[str, object]:
        amount = Decimal(str(quantity))
        self.base_free -= amount
        order_id = str(self.next_order_id)
        self.next_order_id += 1
        order = {
            "orderId": order_id,
            "side": "SELL",
            "status": "NEW",
            "price": str(price),
            "origQty": str(amount),
            "executedQty": "0",
        }
        self.orders[order_id] = order
        self.placements.append(
            {"side": "SELL", "price": Decimal(str(price)), "amount": amount, "maker_only": maker_only}
        )
        self._stop_if_ready()
        return dict(order)

    def get_order(self, _symbol: str, order_id) -> dict[str, object]:
        return dict(self.orders[str(order_id)])

    def cancel_order(self, _symbol: str, order_id) -> dict[str, object]:
        self.orders[str(order_id)]["status"] = "CANCELED"
        return dict(self.orders[str(order_id)])


class _RoundTripLadderClient(_RunningLadderClient):
    def __init__(self, stop_event: _StopAfterOrders):
        super().__init__(stop_event, stop_after_placements=6)
        self._credited_orders: set[str] = set()

    def get_order(self, _symbol: str, order_id) -> dict[str, object]:
        order_key = str(order_id)
        order = self.orders[order_key]
        side = str(order.get("side") or "").upper()
        if order_key not in self._credited_orders and side == "BUY" and order_key in {"1", "2"}:
            executed_qty = Decimal(str(order["origQty"])) / Decimal("2")
            order["executedQty"] = str(executed_qty)
            order["status"] = "PARTIALLY_FILLED"
            self.base_free += executed_qty
            self._credited_orders.add(order_key)
        elif order_key not in self._credited_orders and side == "SELL":
            executed_qty = Decimal(str(order["origQty"]))
            order["executedQty"] = str(executed_qty)
            order["status"] = "FILLED"
            self.quote_free += executed_qty * Decimal(str(order["price"]))
            self._credited_orders.add(order_key)
        return dict(order)


class _StopCleanupLadderClient(_RunningLadderClient):
    def __init__(self, stop_event: _StopAfterOrders):
        super().__init__(stop_event, stop_after_placements=4)
        self.base_free = Decimal("10")
        self.cleanup_cancel_calls: list[str] = []
        self.liquidated_base = Decimal("0")

    def get_order(self, _symbol: str, _order_id):
        raise RuntimeError("普通请求已被停止信号拦截")

    def cancel_order(self, _symbol: str, _order_id):
        raise RuntimeError("普通请求已被停止信号拦截")

    def get_order_for_cleanup(self, symbol: str, order_id) -> dict[str, object]:
        return super().get_order(symbol, order_id)

    def cancel_order_for_cleanup(self, symbol: str, order_id) -> dict[str, object]:
        self.cleanup_cancel_calls.append(str(order_id))
        order = super().cancel_order(symbol, order_id)
        remaining_qty = Decimal(str(order.get("origQty", "0"))) - Decimal(str(order.get("executedQty", "0")))
        if str(order.get("side") or "").upper() == "BUY":
            self.quote_free += remaining_qty * Decimal(str(order.get("price", "0")))
        else:
            self.base_free += remaining_qty
        return order

    def spot_sell_all_base(self, _symbol: str):
        self.liquidated_base = self.base_free
        self.base_free = Decimal("0")
        return self.liquidated_base > 0

    def spot_market_sellable_base_quantity(self, _symbol: str) -> Decimal:
        return self.base_free


class _BnbStopLadderClient(_RunningLadderClient):
    def __init__(self, stop_event: _StopAfterOrders):
        super().__init__(stop_event, stop_after_placements=None)
        self.base_free = Decimal("10")
        self.snapshot_count = 0
        self.events: list[str] = []
        self.liquidated_base = Decimal("0")

    def spot_balance_snapshot(self, assets) -> dict[str, dict[str, Decimal]]:
        snapshot = super().spot_balance_snapshot(assets)
        self.snapshot_count += 1
        bnb_free = Decimal("1") if self.snapshot_count == 1 else Decimal("0.001")
        snapshot["BNB"] = {"free": bnb_free, "locked": Decimal("0"), "total": bnb_free}
        return snapshot

    def cancel_order(self, symbol: str, order_id) -> dict[str, object]:
        order = super().cancel_order(symbol, order_id)
        side = str(order.get("side") or "").upper()
        remaining_qty = Decimal(str(order.get("origQty", "0"))) - Decimal(str(order.get("executedQty", "0")))
        if side == "BUY":
            self.quote_free += remaining_qty * Decimal(str(order.get("price", "0")))
        elif side == "SELL":
            self.base_free += remaining_qty
        self.events.append("cancel-%s" % order_id)
        return order

    def spot_sell_all_base(self, symbol: str):
        self.events.append("market-sell-%s" % str(symbol or "").upper())
        self.liquidated_base = self.base_free
        self.base_free = Decimal("0")
        return self.liquidated_base > 0

    def spot_market_sellable_base_quantity(self, _symbol: str) -> Decimal:
        return self.base_free


class _UnconfirmedCleanupLadderClient(_StopCleanupLadderClient):
    @staticmethod
    def spot_market_sellable_base_quantity(_symbol: str) -> Decimal:
        return Decimal("1")


class _RecordingBinanceClient(BinanceClient):
    def __init__(self):
        self.spot = "spot"
        self.requests: list[tuple[str, str, str, dict[str, object]]] = []

    @staticmethod
    def get_symbol_trade_rules(_symbol: str) -> dict[str, object]:
        return {
            "status": "TRADING",
            "stepSize": Decimal("0.001"),
            "minQty": Decimal("0.001"),
            "maxQty": Decimal("999999"),
            "tickSize": Decimal("0.001"),
            "minPrice": Decimal("0.001"),
            "maxPrice": Decimal("999999"),
            "minNotional": Decimal("5"),
        }

    def request(self, base, method, path, params=None, **_kwargs):
        self.requests.append((base, method, path, dict(params or {})))
        if path == "/api/v3/account":
            return {
                "balances": [
                    {"asset": "USDT", "free": "12.5", "locked": "7.5"},
                    {"asset": "BTC", "free": "0.25", "locked": "0"},
                ]
            }
        return {"orderId": 7, "price": str((params or {}).get("price", "0"))}


class _CancelClient(_PriceRulesClient):
    def __init__(self):
        self.orders = {
            "1": {"orderId": "1", "status": "NEW", "price": "10.111", "origQty": "50", "executedQty": "0"},
            "2": {"orderId": "2", "status": "NEW", "price": "10.110", "origQty": "50", "executedQty": "0"},
        }
        self.cancelled: list[str] = []

    def get_order(self, _symbol: str, order_id) -> dict[str, object]:
        return dict(self.orders[str(order_id)])

    def cancel_order(self, _symbol: str, order_id) -> dict[str, object]:
        order_key = str(order_id)
        self.cancelled.append(order_key)
        self.orders[order_key]["status"] = "CANCELED"
        return dict(self.orders[order_key])


def _strategy(
    client,
    *,
    layout=LADDER_LAYOUT_BUY2_SELL2,
    min_pool=Decimal("50"),
    sell_premium_enabled: bool = False,
    sell_premium: Decimal = Decimal("0"),
) -> Strategy:
    return Strategy(
        client=client,
        spot_rounds=1,
        withdraw_coin="USDT",
        withdraw_address="",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=lambda: None,
        enable_withdraw=False,
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_LADDER,
        ladder_layout=layout,
        ladder_min_pool_notional=min_pool,
        ladder_sell_premium_enabled=sell_premium_enabled,
        ladder_sell_premium=sell_premium,
        bnb_fee_stop_value=Decimal("0"),
        reprice_threshold_amount=Decimal("0.001"),
    )


class ExchangeLadderStrategyTests(unittest.TestCase):
    def test_ladder_poll_interval_is_one_and_half_seconds(self) -> None:
        self.assertEqual(Strategy.LADDER_POLL_INTERVAL_SECONDS, 1.5)

    def test_liquidation_confirmation_counts_locked_base_balance(self) -> None:
        client = object.__new__(BinanceClient)
        client.get_spot_base_asset = lambda _symbol: "ALLO"
        client.spot_balance_snapshot = lambda _assets: {
            "ALLO": {"free": Decimal("0"), "locked": Decimal("10"), "total": Decimal("10")}
        }
        client.get_symbol_trade_rules = lambda _symbol: {
            "status": "TRADING",
            "stepSize": Decimal("0.1"),
            "minQty": Decimal("0.1"),
            "maxQty": Decimal("1000000"),
            "minNotional": Decimal("5"),
        }
        client.get_symbol_price = lambda _symbol: Decimal("1")

        self.assertEqual(client.spot_market_sellable_base_quantity("ALLOUSDT"), Decimal("10"))

    def test_four_layouts_map_to_expected_side_levels(self) -> None:
        expected = {
            LADDER_LAYOUT_BUY2_SELL2: (2, 2),
            LADDER_LAYOUT_BUY1_SELL2: (1, 2),
            LADDER_LAYOUT_BUY2_SELL1: (2, 1),
            LADDER_LAYOUT_BUY1_SELL1: (1, 1),
        }
        for layout, counts in expected.items():
            with self.subTest(layout=layout):
                self.assertEqual(Strategy._ladder_layout_level_counts(layout), counts)

    def test_targets_use_best_price_and_one_exchange_tick(self) -> None:
        strategy = _strategy(_PriceRulesClient())
        targets = strategy._ladder_target_prices(Decimal("10.111"), Decimal("10.112"))
        self.assertEqual(targets["BUY"], [Decimal("10.111"), Decimal("10.110")])
        self.assertEqual(targets["SELL"], [Decimal("10.112"), Decimal("10.113")])

    def test_enabled_sell_premium_uses_current_bid_plus_absolute_value(self) -> None:
        strategy = _strategy(
            _PriceRulesClient(),
            sell_premium_enabled=True,
            sell_premium=Decimal("0.1"),
        )
        targets = strategy._ladder_target_prices(
            Decimal("10.111"),
            Decimal("10.500"),
            sell_bid_anchor=Decimal("10.111"),
        )
        self.assertEqual(targets["BUY"], [Decimal("10.111"), Decimal("10.110")])
        self.assertEqual(targets["SELL"], [Decimal("10.211"), Decimal("10.212")])

    def test_one_level_uses_all_pool_and_two_levels_split_pool(self) -> None:
        one_level = _strategy(_PriceRulesClient(), layout=LADDER_LAYOUT_BUY1_SELL1)
        one_pending: list[dict[str, object]] = []
        one_level._ladder_enqueue_allocations(one_pending, "BUY", Decimal("100"))
        self.assertEqual([item["amount"] for item in one_pending], [Decimal("100")])

        two_level = _strategy(_PriceRulesClient(), layout=LADDER_LAYOUT_BUY2_SELL2)
        two_pending: list[dict[str, object]] = []
        two_level._ladder_enqueue_allocations(two_pending, "SELL", Decimal("3"))
        self.assertEqual([item["amount"] for item in two_pending], [Decimal("1.5"), Decimal("1.5")])

    def test_initial_balance_pool_places_two_maker_buy_children(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event)
        strategy = _strategy(client)

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(
            client.placements,
            [
                {"side": "BUY", "price": Decimal("10.111"), "amount": Decimal("50"), "maker_only": True},
                {"side": "BUY", "price": Decimal("10.110"), "amount": Decimal("50"), "maker_only": True},
            ],
        )
        self.assertTrue(all(order["status"] == "CANCELED" for order in client.orders.values()))

    def test_enabled_sell_premium_keeps_initial_base_at_current_ask(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event)
        client.quote_free = Decimal("0")
        client.base_free = Decimal("10")
        strategy = _strategy(
            client,
            sell_premium_enabled=True,
            sell_premium=Decimal("0.1"),
        )

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(
            [item["price"] for item in client.placements],
            [Decimal("10.112"), Decimal("10.113")],
        )

    def test_manual_stop_uses_cleanup_path_to_cancel_all_ladder_orders(self) -> None:
        stop_event = _StopAfterOrders()
        client = _StopCleanupLadderClient(stop_event)
        strategy = _strategy(client)

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(client.cleanup_cancel_calls, ["1", "2", "3", "4"])
        self.assertTrue(all(order["status"] == "CANCELED" for order in client.orders.values()))
        self.assertEqual(client.liquidated_base, Decimal("10"))
        self.assertEqual(client.base_free, Decimal("0"))
        self.assertTrue(strategy.ladder_cleanup_confirmed)
        self.assertEqual(strategy.ladder_cleanup_error, "")

    def test_bnb_stop_cancels_orders_then_market_sells_all_base(self) -> None:
        stop_event = _StopAfterOrders()
        client = _BnbStopLadderClient(stop_event)
        strategy = _strategy(client)
        strategy.bnb_fee_stop_value = Decimal("0.01")
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(len(client.orders), 4)
        self.assertTrue(all(order["status"] == "CANCELED" for order in client.orders.values()))
        self.assertEqual(client.liquidated_base, Decimal("10"))
        self.assertEqual(client.base_free, Decimal("0"))
        self.assertEqual(client.events[-1], "market-sell-BTCUSDT")
        self.assertTrue(strategy.ladder_cleanup_confirmed)
        self.assertEqual(strategy.ladder_cleanup_error, "")

    def test_manual_stop_is_failure_until_liquidation_is_confirmed(self) -> None:
        stop_event = _StopAfterOrders()
        client = _UnconfirmedCleanupLadderClient(stop_event)
        strategy = _strategy(client)
        strategy.LADDER_CLEANUP_CONFIRM_TIMEOUT_SECONDS = 0

        strategy._run_ladder_mode(stop_event)

        self.assertFalse(strategy.ladder_cleanup_confirmed)
        self.assertIn("确认超时", strategy.ladder_cleanup_error)
        self.assertEqual(strategy.trade_error_stage, "分仓挂单结束清理")

    def test_bnb_stop_before_first_order_market_sells_existing_base(self) -> None:
        stop_event = _StopAfterOrders()
        client = _BnbStopLadderClient(stop_event)
        client.snapshot_count = 1
        strategy = _strategy(client)
        strategy.bnb_fee_stop_value = Decimal("0.01")

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(client.orders, {})
        self.assertEqual(client.liquidated_base, Decimal("10"))
        self.assertEqual(client.base_free, Decimal("0"))
        self.assertEqual(client.events, ["market-sell-BTCUSDT"])

    def test_target_cleanup_only_runs_after_reprice_trigger(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event)
        strategy = _strategy(client)
        cleanup_calls = []
        strategy._ladder_cancel_orders_outside_targets = lambda *_args, **_kwargs: cleanup_calls.append(1)

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(cleanup_calls, [])

    def test_reprice_keeps_overlap_and_directly_refills_missing_level(self) -> None:
        client = _CancelClient()
        strategy = _strategy(client)
        active_orders = {
            "1": {"order_id": "1", "side": "BUY", "price": Decimal("10.111"), "level_index": 0},
            "2": {"order_id": "2", "side": "BUY", "price": Decimal("10.110"), "level_index": 1},
        }
        pending_allocations: list[dict[str, object]] = []

        strategy._ladder_cancel_orders_outside_targets(
            active_orders,
            pending_allocations,
            "BUY",
            [Decimal("10.112"), Decimal("10.111")],
            reason="reprice",
        )

        self.assertEqual(list(active_orders), ["1"])
        self.assertEqual(client.cancelled, ["2"])
        self.assertEqual(pending_allocations, [{
            "side": "BUY",
            "level_index": 0,
            "amount": Decimal("505.500"),
            "source": "reprice",
        }])

    def test_reprice_keeps_order_when_cancel_status_cannot_be_confirmed(self) -> None:
        class UnclearCancelClient(_CancelClient):
            reads = 0

            def get_order(self, symbol, order_id):
                self.reads += 1
                if self.reads > 1:
                    raise ConnectionError("查询撤单状态失败")
                return super().get_order(symbol, order_id)

            def cancel_order(self, _symbol, order_id):
                return {"orderId": order_id}  # 回复未提供终态

        client = UnclearCancelClient()
        strategy = _strategy(client)
        active_orders = {
            "2": {"order_id": "2", "side": "BUY", "price": Decimal("10.110"), "level_index": 1},
        }
        pending_allocations = []

        with self.assertRaisesRegex(RuntimeError, "撤销后状态未确认"):
            strategy._ladder_cancel_orders_outside_targets(
                active_orders, pending_allocations, "BUY",
                [Decimal("10.112"), Decimal("10.111")], reason="reprice",
            )
        self.assertIn("2", active_orders)
        self.assertEqual(pending_allocations, [])

    def test_small_cancelled_remainder_returns_to_balance_pool(self) -> None:
        client = _CancelClient()
        client.orders["2"]["origQty"] = "1"
        strategy = _strategy(client, min_pool=Decimal("50"))
        active_orders = {
            "1": {"order_id": "1", "side": "BUY", "price": Decimal("10.111"), "level_index": 0},
            "2": {"order_id": "2", "side": "BUY", "price": Decimal("10.110"), "level_index": 1},
        }
        pending_allocations: list[dict[str, object]] = []

        strategy._ladder_cancel_orders_outside_targets(
            active_orders,
            pending_allocations,
            "BUY",
            [Decimal("10.112"), Decimal("10.111")],
            reason="reprice",
        )

        self.assertEqual(pending_allocations, [])
        self.assertEqual(client.cancelled, ["2"])

    def test_sell_reprice_directly_refills_missing_level_with_base_quantity(self) -> None:
        client = _CancelClient()
        client.orders = {
            "1": {"orderId": "1", "status": "NEW", "price": "10.112", "origQty": "5", "executedQty": "0"},
            "2": {"orderId": "2", "status": "NEW", "price": "10.113", "origQty": "5", "executedQty": "0"},
        }
        strategy = _strategy(client, min_pool=Decimal("50"))
        active_orders = {
            "1": {"order_id": "1", "side": "SELL", "price": Decimal("10.112"), "level_index": 0},
            "2": {"order_id": "2", "side": "SELL", "price": Decimal("10.113"), "level_index": 1},
        }
        pending_allocations: list[dict[str, object]] = []

        strategy._ladder_cancel_orders_outside_targets(
            active_orders,
            pending_allocations,
            "SELL",
            [Decimal("10.111"), Decimal("10.112")],
            reason="reprice",
        )

        self.assertEqual(list(active_orders), ["1"])
        self.assertEqual(pending_allocations, [{
            "side": "SELL",
            "level_index": 0,
            "amount": Decimal("5"),
            "source": "reprice",
        }])

    def test_active_child_order_limit_is_enforced_per_side(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event)
        strategy = _strategy(client)
        target_prices = {
            "BUY": [Decimal("10.111"), Decimal("10.110")],
            "SELL": [Decimal("10.112"), Decimal("10.113")],
        }
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                active_orders = {
                    str(index): {
                        "order_id": str(index),
                        "side": side,
                        "price": target_prices[side][0],
                        "level_index": 0,
                    }
                    for index in range(strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE)
                }
                pending_allocations = [{"side": side, "level_index": 0, "amount": Decimal("50"), "source": "pool"}]
                before_count = len(client.placements)

                strategy._ladder_submit_pending_allocations(
                    pending_allocations,
                    active_orders,
                    target_prices,
                )

                self.assertEqual(len(client.placements), before_count)
                self.assertEqual(len(active_orders), strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE)
                self.assertEqual(len(pending_allocations), 1)

    def test_pending_allocations_use_latest_book_prices(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        client.base_free = Decimal("10")
        client.get_book_ticker = lambda _symbol: {
            "bidPrice": Decimal("10.113"),
            "askPrice": Decimal("10.114"),
        }
        strategy = _strategy(client)
        pending_allocations = [
            {"side": "BUY", "level_index": 0, "amount": Decimal("50"), "source": "pool"},
            {"side": "SELL", "level_index": 1, "amount": Decimal("5"), "source": "pool"},
        ]

        submitted_anchors = strategy._ladder_submit_pending_allocations(
            pending_allocations,
            {},
            {
                "BUY": [Decimal("10.111"), Decimal("10.110")],
                "SELL": [Decimal("10.112"), Decimal("10.113")],
            },
        )

        self.assertEqual([item["price"] for item in client.placements], [
            Decimal("10.113"),
            Decimal("10.115"),
        ])
        self.assertEqual(submitted_anchors, {
            "BUY": Decimal("10.113"),
            "SELL": Decimal("10.114"),
        })

    def test_enabled_sell_premium_reprice_refill_uses_live_bid(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        client.base_free = Decimal("5")
        strategy = _strategy(
            client,
            sell_premium_enabled=True,
            sell_premium=Decimal("0.1"),
        )
        pending_allocations = [
            {"side": "SELL", "level_index": 0, "amount": Decimal("5"), "source": "reprice"},
        ]

        strategy._ladder_submit_pending_allocations(
            pending_allocations,
            {},
            {"BUY": [Decimal("10.111"), Decimal("10.110")], "SELL": [Decimal("10.500")]},
            sell_from_bid=True,
        )

        self.assertEqual(client.placements[-1]["price"], Decimal("10.211"))

    def test_submission_price_drift_updates_anchor_and_reprices_on_rebound(self) -> None:
        stop_event = _StopAfterTwoLoops()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        book_prices = iter([
            (Decimal("10.112"), Decimal("10.113")),
            (Decimal("10.111"), Decimal("10.112")),
            (Decimal("10.112"), Decimal("10.113")),
            (Decimal("10.112"), Decimal("10.113")),
        ])

        def next_book(_symbol: str) -> dict[str, Decimal]:
            bid, ask = next(book_prices)
            return {"bidPrice": bid, "askPrice": ask}

        client.get_book_ticker = next_book
        strategy = _strategy(client)

        strategy._run_ladder_mode(stop_event)

        self.assertEqual([item["price"] for item in client.placements], [
            Decimal("10.111"),
            Decimal("10.110"),
            Decimal("10.112"),
        ])

    def test_reprice_cleanup_retries_after_transient_failure_without_new_price_move(self) -> None:
        stop_event = _StopAfterThreeLoops()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        book_prices = iter([
            (Decimal("10.111"), Decimal("10.112")),
            (Decimal("10.111"), Decimal("10.112")),
            (Decimal("10.112"), Decimal("10.113")),
            (Decimal("10.112"), Decimal("10.113")),
            (Decimal("10.112"), Decimal("10.113")),
            (Decimal("10.112"), Decimal("10.113")),
        ])

        def next_book(_symbol: str) -> dict[str, Decimal]:
            bid, ask = next(book_prices)
            return {"bidPrice": bid, "askPrice": ask}

        client.get_book_ticker = next_book
        strategy = _strategy(client)
        original_cleanup = strategy._ladder_cancel_orders_outside_targets
        cleanup_calls = 0

        def flaky_cleanup(*args, **kwargs):
            nonlocal cleanup_calls
            cleanup_calls += 1
            if cleanup_calls == 1:
                raise ConnectionError("temporary cancellation failure")
            return original_cleanup(*args, **kwargs)

        strategy._ladder_cancel_orders_outside_targets = flaky_cleanup
        strategy._run_ladder_mode(stop_event)

        self.assertEqual(cleanup_calls, 2)
        self.assertEqual(client.orders["2"]["status"], "CANCELED")
        self.assertEqual(client.placements[-1]["price"], Decimal("10.112"))

    def test_maker_cross_rejection_keeps_pending_allocation(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        strategy = _strategy(client)
        pending_allocations = [
            {"side": "BUY", "level_index": 0, "amount": Decimal("50"), "source": "pool"},
        ]
        original_submit = client.spot_limit_buy_quote_amount

        def reject_maker_cross(*_args, **_kwargs):
            raise BinanceAPIError(-2010, "Order would immediately match and take.")

        client.spot_limit_buy_quote_amount = reject_maker_cross
        strategy._ladder_submit_pending_allocations(
            pending_allocations,
            {},
            {"BUY": [Decimal("10.111"), Decimal("10.110")], "SELL": []},
        )
        self.assertEqual(len(pending_allocations), 1)

        client.spot_limit_buy_quote_amount = original_submit
        active_orders: dict[str, dict[str, object]] = {}
        strategy._ladder_submit_pending_allocations(
            pending_allocations,
            active_orders,
            {"BUY": [Decimal("10.111"), Decimal("10.110")], "SELL": []},
        )
        self.assertEqual(pending_allocations, [])
        self.assertEqual(len(active_orders), 1)

    def test_non_maker_cross_rejection_is_not_swallowed(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        strategy = _strategy(client)

        def reject_insufficient_balance(*_args, **_kwargs):
            raise BinanceAPIError(-2010, "Account has insufficient balance for requested action.")

        client.spot_limit_buy_quote_amount = reject_insufficient_balance

        with self.assertRaises(BinanceAPIError):
            strategy._ladder_submit_pending_allocations(
                [{"side": "BUY", "level_index": 0, "amount": Decimal("50"), "source": "pool"}],
                {},
                {"BUY": [Decimal("10.111"), Decimal("10.110")], "SELL": []},
            )

    def test_reprice_refill_has_priority_when_limit_slot_opens(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RunningLadderClient(stop_event, stop_after_placements=None)
        client.get_book_ticker = lambda _symbol: {
            "bidPrice": Decimal("10.112"),
            "askPrice": Decimal("10.113"),
        }
        strategy = _strategy(client)
        active_orders: dict[str, dict[str, object]] = {}
        for index in range(1, strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE + 1):
            order_id = str(index)
            order_price = Decimal("10.110") if index == strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE else Decimal("10.111")
            client.orders[order_id] = {
                "orderId": order_id,
                "side": "BUY",
                "status": "NEW",
                "price": str(order_price),
                "origQty": "50",
                "executedQty": "0",
            }
            active_orders[order_id] = {
                "order_id": order_id,
                "side": "BUY",
                "price": order_price,
                "level_index": 1,
            }
        client.next_order_id = strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE + 1
        pending_allocations = [
            {"side": "BUY", "level_index": 1, "amount": Decimal("50"), "source": "pool"}
        ]
        target_prices = {
            "BUY": [Decimal("10.112"), Decimal("10.111")],
            "SELL": [Decimal("10.112"), Decimal("10.113")],
        }

        strategy._ladder_cancel_orders_outside_targets(
            active_orders,
            pending_allocations,
            "BUY",
            target_prices["BUY"],
            reason="reprice",
        )
        strategy._ladder_submit_pending_allocations(
            pending_allocations,
            active_orders,
            target_prices,
        )

        self.assertEqual(len(active_orders), strategy.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE)
        self.assertEqual(client.orders["20"]["status"], "CANCELED")
        self.assertEqual(client.placements[-1]["price"], Decimal("10.112"))
        self.assertEqual(client.placements[-1]["amount"], Decimal("505.500"))
        self.assertEqual(pending_allocations, [
            {"side": "BUY", "level_index": 1, "amount": Decimal("50"), "source": "pool"}
        ])

    def test_partial_buy_then_sell_then_buy_cycle(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RoundTripLadderClient(stop_event)
        strategy = _strategy(client)
        strategy.LADDER_ORDER_RECONCILE_INTERVAL_SECONDS = 0
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0

        strategy._run_ladder_mode(stop_event)

        self.assertEqual([item["side"] for item in client.placements], [
            "BUY", "BUY", "SELL", "SELL", "BUY", "BUY",
        ])
        self.assertTrue(all(item["maker_only"] for item in client.placements))
        self.assertEqual(
            [item["price"] for item in client.placements[2:4]],
            [Decimal("10.112"), Decimal("10.113")],
        )
        self.assertGreaterEqual(
            sum((item["amount"] * item["price"] for item in client.placements[2:4]), Decimal("0")),
            Decimal("50"),
        )

    def test_enabled_sell_premium_prices_later_sell_pool_from_live_bid(self) -> None:
        stop_event = _StopAfterOrders()
        client = _RoundTripLadderClient(stop_event)
        strategy = _strategy(
            client,
            sell_premium_enabled=True,
            sell_premium=Decimal("0.1"),
        )
        strategy.LADDER_ORDER_RECONCILE_INTERVAL_SECONDS = 0
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0

        strategy._run_ladder_mode(stop_event)

        self.assertEqual(
            [item["price"] for item in client.placements[2:4]],
            [Decimal("10.211"), Decimal("10.212")],
        )

    def test_maker_limit_order_uses_limit_maker_without_time_in_force(self) -> None:
        client = _RecordingBinanceClient()
        client.place_limit_order(
            "BTCUSDT",
            "BUY",
            Decimal("1"),
            Decimal("10.111"),
            maker_only=True,
        )
        params = client.requests[-1][3]
        self.assertEqual(params["type"], "LIMIT_MAKER")
        self.assertNotIn("timeInForce", params)

    def test_balance_snapshot_returns_free_locked_and_missing_assets(self) -> None:
        client = _RecordingBinanceClient()
        snapshot = client.spot_balance_snapshot({"USDT", "BNB"})
        self.assertEqual(snapshot["USDT"], {
            "free": Decimal("12.5"),
            "locked": Decimal("7.5"),
            "total": Decimal("20.0"),
        })
        self.assertEqual(snapshot["BNB"]["total"], Decimal("0"))


if __name__ == "__main__":
    unittest.main()
