#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
from decimal import Decimal

from exchange_binance_client import BinanceClient
from exchange_constants import (
    FUTURES_LEVERAGE_DEFAULT,
    FUTURES_MARGIN_TYPE_DEFAULT,
    FUTURES_MARGIN_TYPE_LABEL_TO_VALUE,
    FUTURES_MARGIN_TYPE_OPTIONS,
    FUTURES_ROUNDS_DEFAULT,
    FUTURES_SIDE_DEFAULT,
    FUTURES_SIDE_LONG,
    FUTURES_SIDE_OPTIONS,
    FUTURES_SYMBOL_DEFAULT,
    PREMIUM_APPEND_THRESHOLD_DEFAULT,
    PREMIUM_ORDER_COUNT_DEFAULT,
    REPRICE_THRESHOLD_DEFAULT,
    TRADE_ACCOUNT_TYPE_DEFAULT,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_ACCOUNT_TYPE_OPTIONS,
    TRADE_MODE_CONVERT,
    TRADE_MODE_DEFAULT,
    TRADE_MODE_LIMIT,
    TRADE_MODE_MARKET,
    TRADE_MODE_OPTIONS,
    TRADE_MODE_PREMIUM,
)
from exchange_logging import logger


def _normalize_futures_margin_type(value) -> str:
    text = str(value or '').strip()
    if not text:
        return FUTURES_MARGIN_TYPE_DEFAULT
    upper_text = text.upper()
    if upper_text in FUTURES_MARGIN_TYPE_OPTIONS:
        return upper_text
    return FUTURES_MARGIN_TYPE_LABEL_TO_VALUE.get(text, FUTURES_MARGIN_TYPE_DEFAULT)


class Strategy:
    def __init__(
        self,
        client,
        spot_rounds,
        withdraw_coin,
        withdraw_address,
        withdraw_network,
        withdraw_fee_buffer,
        spot_symbol,
        sleep_fn,
        enable_withdraw,
        withdraw_callback=None,
        trade_account_type: str = TRADE_ACCOUNT_TYPE_DEFAULT,
        trade_mode: str = TRADE_MODE_DEFAULT,
        premium_delta: Decimal | None = None,
        premium_order_count: int = PREMIUM_ORDER_COUNT_DEFAULT,
        premium_append_threshold: Decimal | None = None,
        bnb_fee_stop_value: Decimal | None = None,
        bnb_topup_amount: Decimal | None = None,
        reprice_threshold_amount: Decimal | None = None,
        futures_symbol: str = FUTURES_SYMBOL_DEFAULT,
        futures_rounds: int = FUTURES_ROUNDS_DEFAULT,
        futures_amount: Decimal | None = None,
        futures_leverage: int = FUTURES_LEVERAGE_DEFAULT,
        futures_margin_type: str = FUTURES_MARGIN_TYPE_DEFAULT,
        futures_side: str = FUTURES_SIDE_DEFAULT,
    ):
        self.c = client
        self.spot_rounds = spot_rounds
        self.withdraw_coin = withdraw_coin
        self.withdraw_address = withdraw_address
        self.withdraw_network = withdraw_network
        self.withdraw_fee_buffer = withdraw_fee_buffer
        self.spot_symbol = spot_symbol
        self.sleep_fn = sleep_fn
        self.enable_withdraw = enable_withdraw
        self.withdraw_callback = withdraw_callback
        self.trade_account_type = str(trade_account_type or TRADE_ACCOUNT_TYPE_DEFAULT)
        self.trade_mode = str(trade_mode or TRADE_MODE_DEFAULT)
        self.premium_delta = Decimal(str(premium_delta if premium_delta is not None else "0"))
        self.premium_order_count = max(0, int(premium_order_count or 0))
        self.premium_append_threshold = Decimal(
            str(premium_append_threshold if premium_append_threshold is not None else PREMIUM_APPEND_THRESHOLD_DEFAULT)
        )
        self.bnb_fee_stop_value = Decimal(str(bnb_fee_stop_value if bnb_fee_stop_value is not None else "0"))
        self.bnb_topup_amount = Decimal(str(bnb_topup_amount if bnb_topup_amount is not None else "0"))
        self.reprice_threshold_amount = Decimal(
            str(reprice_threshold_amount if reprice_threshold_amount is not None else REPRICE_THRESHOLD_DEFAULT)
        )
        self.futures_symbol = str(futures_symbol or FUTURES_SYMBOL_DEFAULT).strip().upper()
        self.futures_rounds = max(1, int(futures_rounds or FUTURES_ROUNDS_DEFAULT))
        self.futures_amount = Decimal(str(futures_amount if futures_amount is not None else "0"))
        self.futures_leverage = max(1, int(futures_leverage or FUTURES_LEVERAGE_DEFAULT))
        self.futures_margin_type = _normalize_futures_margin_type(futures_margin_type)
        self.futures_side = str(futures_side or FUTURES_SIDE_DEFAULT).strip()
        if self.futures_margin_type not in FUTURES_MARGIN_TYPE_OPTIONS:
            self.futures_margin_type = FUTURES_MARGIN_TYPE_DEFAULT
        if self.futures_side not in FUTURES_SIDE_OPTIONS:
            self.futures_side = FUTURES_SIDE_DEFAULT

    def ensure_base_sold(self):
        try:
            if self._is_convert_mode():
                sold = self.c.convert_base_to_quote_all(self.spot_symbol)
            else:
                base_asset = self.c.get_spot_base_asset(self.spot_symbol)
                small_qty_log_text = "残留BNB数量过小，跳过补救卖出" if base_asset == "BNB" else None
                sold = self.c.spot_sell_all_base(self.spot_symbol, small_qty_log_text=small_qty_log_text)
            if sold:
                logger.info("【补救措施】检测到残留基础币，已执行补充卖出。")
        except Exception as e:
            logger.warning(f"补救卖出时发生错误（可忽略）: {e}")

    def _trade_account_type_name(self) -> str:
        mode = str(self.trade_account_type or TRADE_ACCOUNT_TYPE_DEFAULT)
        return mode if mode in TRADE_ACCOUNT_TYPE_OPTIONS else TRADE_ACCOUNT_TYPE_DEFAULT

    def _is_futures_mode(self) -> bool:
        return self._trade_account_type_name() == TRADE_ACCOUNT_TYPE_FUTURES

    def _futures_side_order(self) -> str:
        return "BUY" if self.futures_side == FUTURES_SIDE_LONG else "SELL"

    def _futures_margin_asset(self) -> str:
        return self.c.get_um_futures_margin_asset(self.futures_symbol)

    def ensure_futures_position_closed(self):
        try:
            close_orders = self.c.close_all_um_futures_positions_market(self.futures_symbol)
            if close_orders:
                logger.info("【补救措施】检测到残留合约持仓，已执行 %d 笔市价平仓。", len(close_orders))
                deadline = time.time() + 8
                while time.time() < deadline:
                    remaining_positions = [
                        position
                        for position in self.c.get_um_futures_positions(self.futures_symbol)
                        if Decimal(str(position.get("positionAmt", "0"))) != 0
                    ]
                    if not remaining_positions:
                        return
                    time.sleep(0.3)
                logger.warning("残留合约持仓平仓后仍未完全归零，后续设置可能被交易所拒绝")
        except Exception as e:
            logger.warning(f"补救平仓时发生错误（可忽略）: {e}")

    def _mode_name(self) -> str:
        mode = str(self.trade_mode or TRADE_MODE_DEFAULT)
        return mode if mode in TRADE_MODE_OPTIONS else TRADE_MODE_DEFAULT

    def _is_convert_mode(self) -> bool:
        return (not self._is_futures_mode()) and self._mode_name() == TRADE_MODE_CONVERT

    def _limit_like_mode(self) -> bool:
        return self._mode_name() in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}

    def _premium_split_order_count(self) -> int:
        return max(0, int(self.premium_order_count or 0))

    def _premium_append_threshold_value(self) -> Decimal:
        if self._premium_split_order_count() <= 1:
            return Decimal("0")
        threshold = Decimal(str(self.premium_append_threshold or "0"))
        if threshold <= 0:
            return Decimal("0")
        return self.c.normalize_price_delta(self.spot_symbol, threshold, min_one_tick=True)

    def _premium_append_mode_enabled(self) -> bool:
        return self._mode_name() == TRADE_MODE_PREMIUM and self._premium_append_threshold_value() > 0

    def _should_stop_for_bnb_fee(self) -> bool:
        if not self._limit_like_mode():
            return False
        threshold = Decimal(str(self.bnb_fee_stop_value or "0"))
        if threshold < 0:
            threshold = Decimal("0")
        current_bnb = Decimal(str(self.c.spot_balance("BNB")))
        if current_bnb < threshold:
            logger.info(
                "%s模式停止：当前 BNB 手续费余额 %s 低于停止值 %s",
                self._mode_name(),
                BinanceClient._format_decimal(current_bnb),
                BinanceClient._format_decimal(threshold),
            )
            return True
        return False

    def _premium_sell_price(self, buy_price: Decimal) -> Decimal:
        premium_delta = self.c.normalize_price_delta(
            self.spot_symbol,
            Decimal(str(self.premium_delta or "0")),
            min_one_tick=False,
        )
        desired_price = Decimal(str(buy_price)) + premium_delta
        return self.c.adjust_price_to_valid_tick(self.spot_symbol, desired_price, round_up=True)

    def _reprice_threshold_value(self) -> Decimal:
        if self._premium_append_mode_enabled():
            return Decimal("0")
        threshold = Decimal(str(self.reprice_threshold_amount or "0"))
        if threshold <= 0:
            return Decimal("0")
        return self.c.normalize_price_delta(self.spot_symbol, threshold, min_one_tick=True)

    def _reprice_threshold_log_text(self) -> str:
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        threshold = self._reprice_threshold_value()
        return f"{BinanceClient._format_decimal(threshold)} {quote_asset}"

    def _run_bnb_topup_if_needed(self):
        if self._is_futures_mode() or self._is_convert_mode():
            return False
        topup_amount = Decimal(str(self.bnb_topup_amount or "0"))
        if topup_amount <= 0:
            return False
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        logger.info(
            "开始闪兑预买 BNB：使用 %s 金额 %s",
            quote_asset,
            BinanceClient._format_decimal(topup_amount),
        )
        spot_bnb_before = self.c.spot_asset_balance_decimal("BNB")
        bought_order = self.c.buy_bnb_with_quote_amount(quote_asset, topup_amount, return_order=True)
        if bought_order:
            bought_bnb = self.c._decimal_from_str(bought_order.get("toAmount", "0"), "0")
            target_spot_bnb = spot_bnb_before + bought_bnb if bought_bnb > 0 else max(
                spot_bnb_before,
                Decimal(str(self.bnb_fee_stop_value or "0")),
            )
            spot_bnb = self.c.ensure_bnb_fee_ready_in_spot(
                min_spot_balance=target_spot_bnb,
                max_transfer_amount=bought_bnb if bought_bnb > 0 else None,
                timeout_seconds=12.0,
            )
            logger.info(
                "闪兑预买 BNB 完成，交易前已确认现货 BNB 手续费余额=%s",
                BinanceClient._format_decimal(spot_bnb),
            )
        else:
            logger.info("闪兑预买 BNB 未执行")
        return bool(bought_order)

    @staticmethod
    def _pause_with_stop(stop_event, seconds: float) -> bool:
        delay = max(0.0, float(seconds))
        if delay <= 0:
            return bool(stop_event and stop_event.is_set())
        if stop_event is not None:
            try:
                return bool(stop_event.wait(delay))
            except Exception:
                pass
        time.sleep(delay)
        try:
            return bool(stop_event and stop_event.is_set())
        except Exception:
            return False

    @staticmethod
    def _order_price_decimal(order_data: dict, fallback_price: Decimal) -> Decimal:
        try:
            price = Decimal(str(order_data.get("price", "")))
            if price > 0:
                return price
        except Exception:
            pass
        return Decimal(str(fallback_price))

    def _should_reprice_open_order(self, side: str, order_price: Decimal, book_ticker: dict[str, Decimal]) -> tuple[bool, Decimal]:
        side_u = str(side or "").strip().upper()
        price = Decimal(str(order_price))
        threshold_amount = self._reprice_threshold_value()
        if side_u == "BUY":
            current_ref = Decimal(str(book_ticker["bidPrice"]))
            if threshold_amount <= 0:
                return False, current_ref
            trigger_price = price + threshold_amount
            return current_ref >= trigger_price, current_ref
        current_ref = Decimal(str(book_ticker["askPrice"]))
        if threshold_amount <= 0:
            return False, current_ref
        trigger_price = price - threshold_amount
        return current_ref <= trigger_price, current_ref

    def _wait_order_filled_or_reprice(
        self,
        order_id: int | str,
        side: str,
        order_price: Decimal,
        stop_event,
        mode_name: str,
        poll_interval: float = 1.0,
    ) -> tuple[str, dict | None]:
        symbol_u = self.spot_symbol.upper()
        side_u = str(side or "").strip().upper()

        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.c.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return "filled", order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")

            if status == "NEW":
                book_ticker = self.c.get_book_ticker(symbol_u)
                should_reprice, current_ref = self._should_reprice_open_order(side_u, order_price, book_ticker)
                if should_reprice:
                    try:
                        self.c.cancel_order(symbol_u, order_id)
                        logger.info(
                            "%s模式%s单价格偏离超过 %s，已撤单重挂：旧价=%s，当前参考价=%s",
                            mode_name,
                            "买" if side_u == "BUY" else "卖",
                            self._reprice_threshold_log_text(),
                            BinanceClient._format_decimal(order_price),
                            BinanceClient._format_decimal(current_ref),
                        )
                        return "reprice", book_ticker
                    except Exception as cancel_exc:
                        logger.warning("撤单重挂时撤单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                        latest_order = self.c.get_order(symbol_u, order_id)
                        latest_status = str(latest_order.get("status") or "").upper()
                        if latest_status == "FILLED":
                            return "filled", latest_order
                        raise

            if stop_event and stop_event.wait(max(0.2, float(poll_interval))):
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception:
                    pass
                raise RuntimeError("收到停止信号，已停止等待挂单成交")
            if not stop_event:
                time.sleep(max(0.2, float(poll_interval)))

    def _place_buy_order_with_reprice(self, stop_event, mode_name: str):
        while True:
            book_ticker = self.c.get_book_ticker(self.spot_symbol)
            buy_price = Decimal(str(book_ticker["bidPrice"]))
            ask_price = Decimal(str(book_ticker["askPrice"]))
            buy_order = self.c.spot_limit_buy_all_usdt(symbol=self.spot_symbol, price=buy_price)
            if not buy_order:
                return None, ask_price

            buy_order_id = buy_order.get("orderId")
            if not buy_order_id:
                raise RuntimeError("买单返回缺少 orderId")
            order_price = self._order_price_decimal(buy_order, buy_price)
            action, payload = self._wait_order_filled_or_reprice(
                buy_order_id,
                "BUY",
                order_price,
                stop_event,
                mode_name,
            )
            if action == "filled":
                return payload, ask_price

    def _place_sell_order_with_reprice(self, stop_event, mode_name: str, buy_fill_price: Decimal):
        premium_reprice_by_market = False
        while True:
            book_ticker = self.c.get_book_ticker(self.spot_symbol)
            ask_price = Decimal(str(book_ticker["askPrice"]))
            if mode_name == TRADE_MODE_PREMIUM:
                reference_price = ask_price if premium_reprice_by_market else buy_fill_price
                sell_price = self._premium_sell_price(reference_price)
            else:
                sell_price = ask_price

            sell_order = self.c.spot_limit_sell_all_base(symbol=self.spot_symbol, price=sell_price)
            if not sell_order:
                return None

            sell_order_id = sell_order.get("orderId")
            if not sell_order_id:
                raise RuntimeError("卖单返回缺少 orderId")
            order_price = self._order_price_decimal(sell_order, sell_price)
            action, payload = self._wait_order_filled_or_reprice(
                sell_order_id,
                "SELL",
                order_price,
                stop_event,
                mode_name,
            )
            if action == "filled":
                return payload
            premium_reprice_by_market = True

    def _cancel_limit_order_with_fill_guard(self, order_id: int | str, side: str) -> dict:
        symbol_u = self.spot_symbol.upper()
        latest_order = None
        try:
            latest_order = self.c.get_order(symbol_u, order_id)
        except Exception:
            latest_order = None

        if latest_order is not None:
            latest_status = str(latest_order.get("status") or "").upper()
            if latest_status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                return latest_order

        try:
            self.c.cancel_order(symbol_u, order_id)
        except Exception as cancel_exc:
            try:
                latest_order = self.c.get_order(symbol_u, order_id)
            except Exception:
                latest_order = None
            if latest_order is not None and str(latest_order.get("status") or "").upper() == "FILLED":
                return latest_order
            raise cancel_exc

        try:
            latest_order = self.c.get_order(symbol_u, order_id)
        except Exception:
            if latest_order is None:
                latest_order = {
                    "orderId": order_id,
                    "side": side,
                    "status": "CANCELED",
                }
            else:
                latest_order = dict(latest_order)
                latest_order["status"] = str(latest_order.get("status") or "CANCELED").upper()
        return latest_order

    def _limit_order_plan(self, book_ticker: dict[str, Decimal]) -> dict[str, Decimal | str]:
        symbol_u = self.spot_symbol.upper()
        quote_asset = self.c.get_spot_quote_asset(symbol_u)
        base_asset = self.c.get_spot_base_asset(symbol_u)
        self.c.collect_funding_asset_to_spot(quote_asset)

        bid_price = Decimal(str(book_ticker["bidPrice"]))
        ask_price = Decimal(str(book_ticker["askPrice"]))
        mid_price = (bid_price + ask_price) / Decimal("2")
        quote_balance = self.c.spot_asset_balance_decimal(quote_asset)
        base_balance = self.c.spot_asset_balance_decimal(base_asset)
        base_notional = base_balance * mid_price

        return {
            "quote_asset": quote_asset,
            "base_asset": base_asset,
            "quote_balance": quote_balance,
            "base_balance": base_balance,
            "base_notional": base_notional,
            "bid_price": bid_price,
            "ask_price": ask_price,
        }

    def _place_limit_orders(self, mode_name: str, book_ticker: dict[str, Decimal]) -> tuple[dict | None, dict | None]:
        plan = self._limit_order_plan(book_ticker)
        quote_asset = str(plan["quote_asset"])
        base_asset = str(plan["base_asset"])
        quote_balance = Decimal(str(plan["quote_balance"]))
        base_balance = Decimal(str(plan["base_balance"]))
        base_notional = Decimal(str(plan["base_notional"]))
        bid_price = Decimal(str(plan["bid_price"]))
        ask_price = Decimal(str(plan["ask_price"]))

        if quote_balance <= 0 and base_balance <= 0:
            logger.info("%s模式当前现货 %s 和 %s 余额均为 0，结束本次运行", mode_name, quote_asset, base_asset)
            return None, None

        buy_order = None
        sell_order = None
        if quote_balance > 0:
            buy_order = self.c.spot_limit_buy_quote_amount(self.spot_symbol, bid_price, quote_balance)
        if base_balance > 0:
            sell_order = self.c.spot_limit_sell_quantity(self.spot_symbol, ask_price, base_balance)

        buy_text = "未挂出"
        if buy_order:
            buy_qty = Decimal(str(buy_order.get("origQty", buy_order.get("executedQty", "0")) or "0"))
            buy_price = self._order_price_decimal(buy_order, bid_price)
            buy_text = f"{BinanceClient._format_decimal(buy_qty)} @ {BinanceClient._format_decimal(buy_price)}"

        sell_text = "未挂出"
        if sell_order:
            sell_qty = Decimal(str(sell_order.get("origQty", sell_order.get("executedQty", "0")) or "0"))
            sell_price = self._order_price_decimal(sell_order, ask_price)
            sell_text = f"{BinanceClient._format_decimal(sell_qty)} @ {BinanceClient._format_decimal(sell_price)}"

        logger.info(
            "%s模式双边挂单：%s=%s | %s=%s(约 %s %s) | 买1=%s | 卖1=%s",
            mode_name,
            quote_asset,
            BinanceClient._format_decimal(quote_balance),
            base_asset,
            BinanceClient._format_decimal(base_balance),
            BinanceClient._format_decimal(base_notional),
            quote_asset,
            buy_text,
            sell_text,
        )
        return buy_order, sell_order

    def _monitor_limit_orders(
        self,
        stop_event,
        mode_name: str,
        *,
        buy_order: dict | None,
        sell_order: dict | None,
        poll_interval: float = 1.0,
    ) -> tuple[str, dict[str, object]]:
        symbol_u = self.spot_symbol.upper()
        active_orders: dict[str, dict[str, object]] = {}
        if buy_order and buy_order.get("orderId"):
            active_orders["BUY"] = {
                "order_id": buy_order.get("orderId"),
                "price": self._order_price_decimal(buy_order, Decimal("0")),
            }
        if sell_order and sell_order.get("orderId"):
            active_orders["SELL"] = {
                "order_id": sell_order.get("orderId"),
                "price": self._order_price_decimal(sell_order, Decimal("0")),
            }

        if not active_orders:
            return "idle", {"filled_orders": []}

        while True:
            if stop_event and stop_event.is_set():
                for side_u, state in list(active_orders.items()):
                    try:
                        self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    except Exception:
                        pass
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            book_ticker = self.c.get_book_ticker(symbol_u)
            filled_orders: list[tuple[str, dict]] = []
            reprice_triggered = False

            for side_u, state in list(active_orders.items()):
                order = self.c.get_order(symbol_u, state["order_id"])
                status = str(order.get("status") or "").upper()
                if status == "FILLED":
                    filled_orders.append((side_u, order))
                    active_orders.pop(side_u, None)
                    continue
                if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                    active_orders.pop(side_u, None)
                    continue

                order_price = self._order_price_decimal(order, Decimal(str(state["price"])))
                state["price"] = order_price
                should_reprice, current_ref = self._should_reprice_open_order(side_u, order_price, book_ticker)
                if not should_reprice:
                    continue

                latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                latest_status = str(latest_order.get("status") or "").upper()
                if latest_status == "FILLED":
                    filled_orders.append((side_u, latest_order))
                else:
                    logger.info(
                        "%s模式%s单价格偏离超过 %s，已撤单准备按最新余额重挂：旧价=%s，当前参考价=%s",
                        mode_name,
                        "买" if side_u == "BUY" else "卖",
                        self._reprice_threshold_log_text(),
                        BinanceClient._format_decimal(order_price),
                        BinanceClient._format_decimal(current_ref),
                    )
                active_orders.pop(side_u, None)
                reprice_triggered = True

            if filled_orders:
                for side_u, state in list(active_orders.items()):
                    latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    if str(latest_order.get("status") or "").upper() == "FILLED":
                        filled_orders.append((side_u, latest_order))
                    active_orders.pop(side_u, None)
                return "filled", {"filled_orders": filled_orders}

            if reprice_triggered:
                for side_u, state in list(active_orders.items()):
                    latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    if str(latest_order.get("status") or "").upper() == "FILLED":
                        filled_orders.append((side_u, latest_order))
                    active_orders.pop(side_u, None)
                if filled_orders:
                    return "filled", {"filled_orders": filled_orders}
                return "reprice", {"book_ticker": book_ticker}

            if not active_orders:
                return "idle", {"filled_orders": []}

            if stop_event and stop_event.wait(max(0.2, float(poll_interval))):
                continue
            if not stop_event:
                time.sleep(max(0.2, float(poll_interval)))

    def _limit_fill_summary(self, filled_orders: list[tuple[str, dict]]) -> str:
        if not filled_orders:
            return "本轮无成交"

        parts: list[str] = []
        for side_u, order in filled_orders:
            side_text = "买单" if side_u == "BUY" else "卖单"
            qty = Decimal(str(order.get("executedQty", order.get("origQty", "0")) or "0"))
            avg_price = self.c.get_order_average_price(order) or self._order_price_decimal(order, Decimal("0"))
            text = f"{side_text}成交 数量={BinanceClient._format_decimal(qty)}"
            if avg_price > 0:
                text += f" 均价={BinanceClient._format_decimal(avg_price)}"
            parts.append(text)
        return "；".join(parts)

    @staticmethod
    def _order_executed_qty_decimal(order_data: dict) -> Decimal:
        try:
            return Decimal(str(order_data.get("executedQty", order_data.get("origQty", "0")) or "0"))
        except Exception:
            return Decimal("0")

    def _place_premium_split_buy_order(self, quote_amount: Decimal, price: Decimal, slot_index: int) -> dict | None:
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        available_quote = self.c.spot_asset_balance_decimal(quote_asset)
        amount = Decimal(str(quote_amount))
        if amount <= 0:
            logger.info("溢价单分笔买单金额 <= 0，跳过")
            return None
        if available_quote < amount:
            logger.info(
                "溢价单模式第 %d 笔追加买单跳过：现货 %s 可用余额 %s < 单笔金额 %s",
                slot_index,
                quote_asset,
                BinanceClient._format_decimal(available_quote),
                BinanceClient._format_decimal(amount),
            )
            return None

        buy_order = self.c.spot_limit_buy_quote_amount(self.spot_symbol, price, amount)
        if not buy_order:
            return None

        order_id = buy_order.get("orderId")
        if not order_id:
            raise RuntimeError("溢价单分笔买单返回缺少 orderId")
        order_price = self._order_price_decimal(buy_order, price)
        logger.info(
            "溢价单模式挂出第 %d 笔买单：金额=%s %s 价格=%s",
            slot_index,
            BinanceClient._format_decimal(amount),
            quote_asset,
            BinanceClient._format_decimal(order_price),
        )
        return {
            "order_id": order_id,
            "price": order_price,
            "quote_amount": amount,
        }

    def _place_premium_split_sell_order(self, quantity: Decimal, buy_fill_price: Decimal) -> dict | None:
        qty = Decimal(str(quantity))
        if qty <= 0:
            logger.info("溢价单模式卖单数量 <= 0，跳过")
            return None

        sell_price = self._premium_sell_price(buy_fill_price)
        sell_order = self.c.spot_limit_sell_quantity(self.spot_symbol, sell_price, qty)
        if not sell_order:
            return None

        order_id = sell_order.get("orderId")
        if not order_id:
            raise RuntimeError("溢价单分笔卖单返回缺少 orderId")
        order_price = self._order_price_decimal(sell_order, sell_price)
        logger.info(
            "溢价单模式挂出对应卖单：数量=%s 买入均价=%s 卖价=%s",
            BinanceClient._format_decimal(qty),
            BinanceClient._format_decimal(buy_fill_price),
            BinanceClient._format_decimal(order_price),
        )
        return {
            "order_id": order_id,
            "price": order_price,
            "quantity": qty,
            "buy_fill_price": Decimal(str(buy_fill_price)),
        }

    def _cancel_premium_active_orders(
        self,
        active_buy_orders: dict[str, dict[str, object]],
        active_sell_orders: dict[str, dict[str, object]],
    ) -> None:
        for side_u, orders in (("BUY", active_buy_orders), ("SELL", active_sell_orders)):
            for order_key, state in list(orders.items()):
                order_id = state.get("order_id")
                if not order_id:
                    orders.pop(order_key, None)
                    continue
                try:
                    self._cancel_limit_order_with_fill_guard(order_id, side_u)
                except Exception as exc:
                    logger.warning("停止时撤销溢价单模式未完成订单失败 %s #%s: %s", side_u, order_id, exc)
                orders.pop(order_key, None)

    def _run_premium_append_mode(self, stop_event, progress_cb=None):
        mode_name = self._mode_name()
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        append_threshold = self._premium_append_threshold_value()
        order_count = self._premium_split_order_count()
        if order_count <= 1 or append_threshold <= 0:
            self._run_premium_mode(stop_event, progress_cb=progress_cb)
            return

        step = 0
        active_buy_orders: dict[str, dict[str, object]] = {}
        active_sell_orders: dict[str, dict[str, object]] = {}
        last_append_price: Decimal | None = None
        per_order_quote_amount = Decimal("0")
        submitted_buy_count = 0
        stop_new_entries = False
        stop_reason_logged = False
        mode_logged = False

        while True:
            if stop_event and stop_event.is_set():
                self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                logger.info("检测到停止信号，已撤销溢价单模式未完成订单")
                return

            if (not stop_new_entries) and self._should_stop_for_bnb_fee():
                stop_new_entries = True
                if not stop_reason_logged:
                    logger.info("溢价单模式已触发 BNB 手续费停止条件，停止追加新单，继续等待当前订单完成")
                    stop_reason_logged = True

            try:
                if not active_buy_orders and not active_sell_orders:
                    if stop_new_entries:
                        logger.info("溢价单模式当前无未完成订单，结束本次运行")
                        return

                    self.c.collect_funding_asset_to_spot(quote_asset)
                    cycle_quote_amount = self.c.spot_asset_balance_decimal(quote_asset)
                    if cycle_quote_amount <= 0:
                        logger.info("溢价单模式现货 %s 可用余额为 0，结束本次运行", quote_asset)
                        return

                    per_order_quote_amount = cycle_quote_amount / Decimal(str(order_count))
                    submitted_buy_count = 0
                    if not mode_logged:
                        logger.info(
                            "启用溢价单分笔追加挂单：笔数=%d，单笔金额=%s %s，追加挂单=%s %s，重挂阈值已自动关闭",
                            order_count,
                            BinanceClient._format_decimal(per_order_quote_amount),
                            quote_asset,
                            BinanceClient._format_decimal(append_threshold),
                            quote_asset,
                        )
                        mode_logged = True
                    else:
                        logger.info(
                            "溢价单模式开始新一轮分笔挂单：总金额=%s %s，单笔=%s %s",
                            BinanceClient._format_decimal(cycle_quote_amount),
                            quote_asset,
                            BinanceClient._format_decimal(per_order_quote_amount),
                            quote_asset,
                        )

                    book_ticker = self.c.get_book_ticker(self.spot_symbol)
                    current_bid = Decimal(str(book_ticker["bidPrice"]))
                    initial_order = self._place_premium_split_buy_order(per_order_quote_amount, current_bid, 1)
                    if not initial_order:
                        logger.info("溢价单模式首笔买单未执行（可能余额不足或不满足最小下单额），结束本次运行")
                        return
                    active_buy_orders[str(initial_order["order_id"])] = initial_order
                    submitted_buy_count = 1
                    last_append_price = Decimal(str(initial_order["price"]))

                for order_key, state in list(active_buy_orders.items()):
                    order = self.c.get_order(self.spot_symbol, state["order_id"])
                    status = str(order.get("status") or "").upper()
                    executed_qty = self._order_executed_qty_decimal(order)

                    if status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                        active_buy_orders.pop(order_key, None)

                    if status == "FILLED" or (status in {"CANCELED", "EXPIRED", "EXPIRED_IN_MATCH"} and executed_qty > 0):
                        buy_fill_price = self.c.get_order_average_price(order) or Decimal(str(state.get("price", "0") or "0"))
                        sell_state = self._place_premium_split_sell_order(executed_qty, buy_fill_price)
                        if sell_state:
                            active_sell_orders[str(sell_state["order_id"])] = sell_state
                        else:
                            logger.warning(
                                "溢价单模式买单已成交，但对应卖单未挂出：数量=%s 买入均价=%s",
                                BinanceClient._format_decimal(executed_qty),
                                BinanceClient._format_decimal(buy_fill_price),
                            )

                for order_key, state in list(active_sell_orders.items()):
                    order = self.c.get_order(self.spot_symbol, state["order_id"])
                    status = str(order.get("status") or "").upper()
                    if status == "FILLED":
                        active_sell_orders.pop(order_key, None)
                        step += 1
                        qty = self._order_executed_qty_decimal(order)
                        avg_price = self.c.get_order_average_price(order) or Decimal(str(state.get("price", "0") or "0"))
                        logger.info(
                            "--- %s分笔 %d 完成：卖单成交 数量=%s 均价=%s ---",
                            mode_name,
                            step,
                            BinanceClient._format_decimal(qty),
                            BinanceClient._format_decimal(avg_price),
                        )
                        if progress_cb:
                            progress_cb(step, max(step, 1), f"{mode_name}分笔 {step}")
                    elif status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                        active_sell_orders.pop(order_key, None)
                        logger.warning(
                            "溢价单模式卖单结束，状态=%s，原挂价=%s",
                            status,
                            BinanceClient._format_decimal(Decimal(str(state.get("price", "0") or "0"))),
                        )

                if (not stop_new_entries) and per_order_quote_amount > 0:
                    if submitted_buy_count < order_count and last_append_price is not None:
                        book_ticker = self.c.get_book_ticker(self.spot_symbol)
                        current_bid = Decimal(str(book_ticker["bidPrice"]))
                        price_diff = abs(current_bid - last_append_price)
                        if price_diff >= append_threshold:
                            next_slot_index = submitted_buy_count + 1
                            appended_order = self._place_premium_split_buy_order(
                                per_order_quote_amount,
                                current_bid,
                                next_slot_index,
                            )
                            if appended_order:
                                active_buy_orders[str(appended_order["order_id"])] = appended_order
                                submitted_buy_count += 1
                                logger.info(
                                    "溢价单模式价格偏移达到追加挂单阈值 %s %s，已追加第 %d 笔买单：上一锚点=%s 当前买1=%s",
                                    BinanceClient._format_decimal(append_threshold),
                                    quote_asset,
                                    next_slot_index,
                                    BinanceClient._format_decimal(last_append_price),
                                    BinanceClient._format_decimal(Decimal(str(appended_order["price"]))),
                                )
                                last_append_price = Decimal(str(appended_order["price"]))

                if stop_event and stop_event.wait(1.0):
                    continue
                if not stop_event:
                    time.sleep(1.0)
            except Exception as e:
                if stop_event and stop_event.is_set():
                    self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                    logger.info("检测到停止信号，已停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s分笔模式执行异常: %s", mode_name, e)
                if self._pause_with_stop(stop_event, 3):
                    self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                    return

    def _run_market_mode(self, stop_event, progress_cb=None):
        total_steps = self.spot_rounds if self.spot_rounds > 0 else 1
        step = 0

        self.ensure_base_sold()

        for i in range(self.spot_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（现货阶段）")
                return

            self.ensure_base_sold()
            logger.info("--- 现货轮 %d/%d 开始 ---", i + 1, self.spot_rounds)

            try:
                buy_success = self.c.spot_buy_all_usdt(symbol=self.spot_symbol)
                if buy_success:
                    self.sleep_fn()
                    self.c.spot_sell_all_base(symbol=self.spot_symbol)
                else:
                    logger.info("买入未执行（可能余额不足），跳过本轮卖出")

                logger.info("--- 现货轮 %d 完成 ---", i + 1)

            except Exception as e:
                logger.error(f"现货轮 %d 执行异常: {e}", i + 1)
                if self._pause_with_stop(stop_event, 3):
                    return

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "现货轮 %d/%d" % (i + 1, self.spot_rounds))
            self.sleep_fn()

        self.ensure_base_sold()

    def _run_convert_mode(self, stop_event, progress_cb=None):
        total_steps = self.spot_rounds if self.spot_rounds > 0 else 1
        step = 0

        self.ensure_base_sold()

        for i in range(self.spot_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（闪兑阶段）")
                return

            self.ensure_base_sold()
            logger.info("--- 闪兑轮 %d/%d 开始 ---", i + 1, self.spot_rounds)

            try:
                buy_result = self.c.convert_quote_to_base_all(symbol=self.spot_symbol)
                if buy_result:
                    self.sleep_fn()
                    sell_result = self.c.convert_base_to_quote_all(symbol=self.spot_symbol)
                    if not sell_result:
                        logger.info("闪兑卖出未执行（可能余额不足或不满足最小闪兑金额），结束本次运行")
                        return
                else:
                    logger.info("闪兑换入未执行（可能余额不足或不满足最小闪兑金额），结束本次运行")
                    return

                logger.info("--- 闪兑轮 %d 完成 ---", i + 1)
            except Exception as e:
                logger.error(f"闪兑轮 %d 执行异常: {e}", i + 1)
                if self._pause_with_stop(stop_event, 3):
                    return

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "闪兑轮 %d/%d" % (i + 1, self.spot_rounds))
            self.sleep_fn()

        self.ensure_base_sold()

    def _run_limit_mode(self, stop_event, progress_cb=None):
        step = 0
        mode_name = self._mode_name()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                return
            if self._should_stop_for_bnb_fee():
                return

            try:
                book_ticker = self.c.get_book_ticker(self.spot_symbol)
                buy_order, sell_order = self._place_limit_orders(mode_name, book_ticker)
                if not buy_order and not sell_order:
                    logger.info("%s模式当前无可挂出的买卖单（余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                action, payload = self._monitor_limit_orders(
                    stop_event,
                    mode_name,
                    buy_order=buy_order,
                    sell_order=sell_order,
                )
                if action == "filled":
                    step += 1
                    filled_summary = self._limit_fill_summary(list(payload.get("filled_orders") or []))
                    logger.info("--- %s轮 %d 完成：%s ---", mode_name, step, filled_summary)
                    if progress_cb:
                        progress_cb(step, max(step, 1), f"{mode_name}轮 {step}")
                    self.sleep_fn()
                    continue
                if action == "reprice":
                    continue

                logger.info("%s模式当前无活动挂单，结束本次运行", mode_name)
                return
            except Exception as e:
                if stop_event and stop_event.is_set():
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)
                if self._pause_with_stop(stop_event, 3):
                    return

    def _run_premium_mode(self, stop_event, progress_cb=None):
        if self._premium_append_mode_enabled():
            self._run_premium_append_mode(stop_event, progress_cb=progress_cb)
            return

        step = 0
        mode_name = self._mode_name()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                return
            if self._should_stop_for_bnb_fee():
                return

            step += 1
            logger.info("--- %s轮 %d 开始 ---", mode_name, step)
            try:
                buy_result, _ask_price = self._place_buy_order_with_reprice(stop_event, mode_name)
                buy_order = buy_result
                if not buy_order:
                    logger.info("%s模式买入未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                buy_fill_price = self.c.get_order_average_price(buy_order) or Decimal("0")
                if buy_fill_price <= 0:
                    buy_fill_price = Decimal(str(self.c.get_book_ticker(self.spot_symbol)["bidPrice"]))

                self.sleep_fn()

                sell_order = self._place_sell_order_with_reprice(stop_event, mode_name, buy_fill_price)
                if not sell_order:
                    logger.info("%s模式卖出未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                logger.info("--- %s轮 %d 完成 ---", mode_name, step)
            except Exception as e:
                if stop_event and stop_event.is_set():
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)
                if self._pause_with_stop(stop_event, 3):
                    return

            if progress_cb:
                progress_cb(step, max(step, 1), f"{mode_name}轮 {step}")
            self.sleep_fn()

    def _run_limit_like_mode(self, stop_event, progress_cb=None):
        if self._mode_name() == TRADE_MODE_LIMIT:
            self._run_limit_mode(stop_event, progress_cb=progress_cb)
            return
        self._run_premium_mode(stop_event, progress_cb=progress_cb)

    def _log_futures_symbol_settings(self):
        current_dual_mode = self.c.get_um_futures_position_mode()
        logger.info("U本位合约当前持仓模式：%s", "双向仓" if current_dual_mode else "单向仓")

        symbol_config = self.c.get_um_futures_symbol_config(self.futures_symbol) or {}
        logger.info(
            "U本位合约 %s 当前配置：杠杆=%s，保证金模式=%s",
            self.futures_symbol,
            symbol_config.get("leverage", "--"),
            symbol_config.get("marginType", "--"),
        )

    def _prepare_futures_symbol_settings(self):
        self.c.ensure_um_futures_one_way_mode()
        self.c.ensure_um_futures_margin_type(self.futures_symbol, self.futures_margin_type)
        final_leverage = self.c.ensure_um_futures_leverage(self.futures_symbol, self.futures_leverage)
        self.futures_leverage = int(final_leverage)

    def _ensure_futures_available_balance(self):
        margin_asset = self._futures_margin_asset()
        balance = self.c.um_futures_asset_balance(margin_asset)
        available_balance = Decimal(str(balance.get("availableBalance", "0")))
        required_margin = Decimal("0")
        if self.futures_leverage > 0:
            required_margin = self.futures_amount / Decimal(str(self.futures_leverage))
        if required_margin > 0 and available_balance < required_margin:
            missing_margin = required_margin - available_balance
            moved_amount = self.c.transfer_spot_asset_to_um_futures(margin_asset, missing_margin)
            if moved_amount > 0:
                balance = self.c.um_futures_asset_balance(margin_asset)
                available_balance = Decimal(str(balance.get("availableBalance", "0")))
                logger.info(
                    "U本位合约 %s 已自动补划保证金 %s=%s，当前可用=%s",
                    self.futures_symbol,
                    margin_asset,
                    BinanceClient._format_decimal(moved_amount),
                    BinanceClient._format_decimal(available_balance),
                )
        logger.info(
            "U本位合约 %s 可用保证金 %s=%s，预计占用保证金=%s，下单金额=%s，杠杆=%s",
            self.futures_symbol,
            margin_asset,
            BinanceClient._format_decimal(available_balance),
            BinanceClient._format_decimal(required_margin),
            BinanceClient._format_decimal(self.futures_amount),
            self.futures_leverage,
        )
        if required_margin > 0 and available_balance < required_margin:
            raise RuntimeError(
                f"U本位合约 {margin_asset} 可用保证金不足：需要至少 {BinanceClient._format_decimal(required_margin)}，当前 {BinanceClient._format_decimal(available_balance)}"
            )
        return margin_asset, available_balance

    def _run_futures_mode(self, stop_event, progress_cb=None):
        total_steps = self.futures_rounds if self.futures_rounds > 0 else 1
        step = 0
        open_side = self._futures_side_order()
        side_text = self.futures_side if self.futures_side in FUTURES_SIDE_OPTIONS else FUTURES_SIDE_DEFAULT

        self.ensure_futures_position_closed()
        self._log_futures_symbol_settings()
        preview_qty = self.c.calculate_um_futures_order_quantity(
            self.futures_symbol,
            self.futures_amount,
            open_side,
        )
        self._ensure_futures_available_balance()
        logger.info(
            "【合约预检通过】%s %s，单轮预计下单数量=%s，下单金额=%s",
            self.futures_symbol,
            side_text,
            BinanceClient._format_decimal(preview_qty),
            BinanceClient._format_decimal(self.futures_amount),
        )
        self._prepare_futures_symbol_settings()

        for i in range(self.futures_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（合约阶段）")
                return

            self.ensure_futures_position_closed()
            logger.info("--- 合约轮 %d/%d 开始：%s %s ---", i + 1, self.futures_rounds, self.futures_symbol, side_text)

            try:
                stage_label = "开仓前预检"
                qty = self.c.calculate_um_futures_order_quantity(
                    self.futures_symbol,
                    self.futures_amount,
                    open_side,
                )
                stage_label = "开仓下单"
                open_order = self.c.place_um_futures_market_order(
                    self.futures_symbol,
                    open_side,
                    qty,
                )
                try:
                    avg_price = Decimal(str(open_order.get("avgPrice", "0")))
                except Exception:
                    avg_price = Decimal("0")
                logger.info(
                    "【开仓成功】%s %s，数量=%s，均价=%s",
                    self.futures_symbol,
                    side_text,
                    BinanceClient._format_decimal(qty),
                    BinanceClient._format_decimal(avg_price),
                )

                self.sleep_fn()
                stage_label = "平仓下单"
                close_order = self.c.close_um_futures_position_market(self.futures_symbol)
                if not close_order:
                    raise RuntimeError("合约平仓未执行：未检测到可平持仓")
                try:
                    close_price = Decimal(str(close_order.get("avgPrice", "0")))
                except Exception:
                    close_price = Decimal("0")
                logger.info(
                    "【平仓成功】%s %s，均价=%s",
                    self.futures_symbol,
                    side_text,
                    BinanceClient._format_decimal(close_price),
                )
            except Exception as e:
                logger.error("【%s失败】合约轮 %d：%s", stage_label, i + 1, e)
                if self._pause_with_stop(stop_event, 3):
                    return
            finally:
                self.ensure_futures_position_closed()

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "合约轮 %d/%d" % (i + 1, self.futures_rounds))
            self.sleep_fn()

        self.ensure_futures_position_closed()

    def run(self, stop_event, progress_cb=None):
        withdraw_amount = 0.0
        withdraw_error = ""
        withdraw_attempted = False

        self._run_bnb_topup_if_needed()

        if self._is_futures_mode():
            self._run_futures_mode(stop_event, progress_cb=progress_cb)
        elif self._is_convert_mode():
            self._run_convert_mode(stop_event, progress_cb=progress_cb)
        elif self._mode_name() == TRADE_MODE_MARKET:
            self._run_market_mode(stop_event, progress_cb=progress_cb)
        else:
            self._run_limit_like_mode(stop_event, progress_cb=progress_cb)

        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过最终提现")
            return {
                "withdraw_amount": withdraw_amount,
                "withdraw_error": withdraw_error,
                "withdraw_attempted": withdraw_attempted,
            }

        logger.info(f"开始最终提现所有 {self.withdraw_coin}")
        withdraw_attempted = True
        try:
            withdraw_amount = self.c.withdraw_all_coin(
                coin=self.withdraw_coin,
                address=self.withdraw_address,
                network=self.withdraw_network,
                fee_buffer=self.withdraw_fee_buffer,
                enable_withdraw=self.enable_withdraw,
                auto_collect_to_spot=self._is_futures_mode(),
            )
            if self.withdraw_callback:
                self.withdraw_callback(withdraw_amount)
        except Exception as e:
            withdraw_error = str(e)
            logger.error(f"提现阶段异常: {e}")

        logger.info("策略执行完毕")
        return {
            "withdraw_amount": withdraw_amount,
            "withdraw_error": withdraw_error,
            "withdraw_attempted": withdraw_attempted,
        }

class CombinedStopEvent:
    def __init__(self, *events):
        self._events = [event for event in events if event is not None]

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)

    def wait(self, timeout=None) -> bool:
        if self.is_set():
            return True
        if timeout is None:
            while not self.is_set():
                time.sleep(0.1)
            return True

        end_time = time.time() + max(0.0, float(timeout))
        while True:
            if self.is_set():
                return True
            remaining = end_time - time.time()
            if remaining <= 0:
                return self.is_set()
            step = min(0.2, remaining)
            if self._events:
                self._events[0].wait(step)
            else:
                time.sleep(step)
