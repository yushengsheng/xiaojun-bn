#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
from contextlib import nullcontext
from decimal import Decimal
from typing import Callable

from exchange_auto_ladder import AutoLadderSettingsStore, price_risk_reason, trade_is_recent
from exchange_auto_ladder_observer import AutoLadderMarketObserver
from exchange_binance_client import BinanceAPIError, BinanceClient, UnconfirmedOrderError
from retry_utils import RetryAborted, RetryPolicy, run_with_retries
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
    LADDER_LAYOUT_BUY1_SELL1,
    LADDER_LAYOUT_BUY1_SELL2,
    LADDER_LAYOUT_BUY2_SELL1,
    LADDER_LAYOUT_BUY2_SELL2,
    LADDER_LAYOUT_DEFAULT,
    LADDER_LAYOUT_OPTIONS,
    LADDER_MIN_POOL_NOTIONAL_DEFAULT,
    LADDER_SELL_PREMIUM_DEFAULT,
    LADDER_SELL_PREMIUM_ENABLED_DEFAULT,
    MARKET_STAGGER_ENABLED_DEFAULT,
    MARKET_STOP_MODE_BNB,
    MARKET_STOP_MODE_DEFAULT,
    MARKET_STOP_MODE_OPTIONS,
    PREMIUM_APPEND_THRESHOLD_DEFAULT,
    PREMIUM_ORDER_COUNT_DEFAULT,
    PREMIUM_PARTIAL_FILL_DEFAULT,
    REPRICE_THRESHOLD_DEFAULT,
    TRADE_ACCOUNT_TYPE_DEFAULT,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_ACCOUNT_TYPE_OPTIONS,
    TRADE_MODE_CONVERT,
    TRADE_MODE_DEFAULT,
    TRADE_MODE_LIMIT,
    TRADE_MODE_LADDER,
    TRADE_MODE_AUTO_LADDER,
    TRADE_MODE_MARKET,
    TRADE_MODE_OPTIONS,
    TRADE_MODE_PREMIUM,
)
from exchange_logging import logger


class _AutoLadderMarketPause(RuntimeError):
    """The newest book is outside the configured price safety band."""


def _normalize_futures_margin_type(value) -> str:
    text = str(value or '').strip()
    if not text:
        return FUTURES_MARGIN_TYPE_DEFAULT
    upper_text = text.upper()
    if upper_text in FUTURES_MARGIN_TYPE_OPTIONS:
        return upper_text
    return FUTURES_MARGIN_TYPE_LABEL_TO_VALUE.get(text, FUTURES_MARGIN_TYPE_DEFAULT)


class MarketStaggerCoordinator:
    """Keep market-order worker lanes in two strictly alternating phase groups."""

    def __init__(self, lane_ids):
        self._condition = threading.Condition()
        self._known_lanes = {max(1, int(lane_id)) for lane_id in lane_ids}
        self._active_lanes = set(self._known_lanes)
        self._phase = 0
        self._phase_open = False
        self._phase_tokens: dict[int, dict[str, object]] = {}
        self._pending: dict[int, dict[str, object]] = {}

    @staticmethod
    def _side_for_phase(lane_id: int, phase: int) -> str | None:
        odd_lane = int(lane_id) % 2 == 1
        if phase == 0:
            return "BUY" if odd_lane else None
        if phase % 2 == 1:
            return "SELL" if odd_lane else "BUY"
        return "BUY" if odd_lane else "SELL"

    def _expected_lanes_locked(self) -> dict[int, str]:
        while self._known_lanes:
            for lane_id, token in tuple(self._pending.items()):
                if (
                    lane_id in self._known_lanes
                    and lane_id not in self._active_lanes
                    and self._side_for_phase(lane_id, self._phase) == token["side"]
                ):
                    self._active_lanes.add(lane_id)
            expected = {
                lane_id: side
                for lane_id in self._active_lanes
                if (side := self._side_for_phase(lane_id, self._phase)) is not None
            }
            if expected:
                return expected
            if not self._active_lanes and not any(
                lane_id in self._known_lanes for lane_id in self._pending
            ):
                return {}
            self._phase += 1
        return {}

    def _try_open_phase_locked(self) -> None:
        if self._phase_open:
            return
        expected = self._expected_lanes_locked()
        if not expected:
            self._condition.notify_all()
            return
        for lane_id, side in expected.items():
            token = self._pending.get(lane_id)
            if token is None or token["side"] != side:
                return
        self._phase_open = True
        self._phase_tokens = {lane_id: self._pending[lane_id] for lane_id in expected}
        for token in self._phase_tokens.values():
            token["released"] = True
        self._condition.notify_all()

    def _try_finish_phase_locked(self) -> None:
        if not self._phase_open:
            self._try_open_phase_locked()
            return
        if any(not bool(token["finished"]) for token in self._phase_tokens.values()):
            return
        completed_tokens = tuple(self._phase_tokens.items())
        for lane_id, token in completed_tokens:
            if self._pending.get(lane_id) is token:
                self._pending.pop(lane_id, None)
            token["phase_done"] = True
        self._phase_tokens = {}
        self._phase_open = False
        self._phase += 1
        self._condition.notify_all()
        self._try_open_phase_locked()

    def _suspend_locked(self, lane: int) -> None:
        self._active_lanes.discard(lane)
        pending = self._pending.pop(lane, None)
        if pending is not None:
            pending["cancelled"] = True
            pending["phase_done"] = True
        phase_token = self._phase_tokens.pop(lane, None)
        if phase_token is not None:
            phase_token["cancelled"] = True
            phase_token["finished"] = True
            phase_token["phase_done"] = True
        self._try_finish_phase_locked()
        self._try_open_phase_locked()
        self._condition.notify_all()

    def suspend(self, lane_id: int) -> None:
        lane = max(1, int(lane_id))
        with self._condition:
            if lane in self._known_lanes:
                self._suspend_locked(lane)

    def unregister(self, lane_id: int) -> None:
        lane = max(1, int(lane_id))
        with self._condition:
            self._known_lanes.discard(lane)
            self._suspend_locked(lane)

    def execute(self, lane_id: int, side: str, action, stop_event=None):
        lane = max(1, int(lane_id))
        side_u = str(side or "").strip().upper()
        if side_u not in {"BUY", "SELL"}:
            raise ValueError(f"不支持的市价交错方向：{side}")

        token: dict[str, object] = {
            "side": side_u,
            "released": False,
            "finished": False,
            "phase_done": False,
            "cancelled": False,
        }
        bypass_coordination = False
        with self._condition:
            if lane not in self._known_lanes:
                bypass_coordination = True
            else:
                if lane in self._pending:
                    raise RuntimeError(f"市价交错线程 {lane} 存在未完成动作")
                self._pending[lane] = token
                self._try_open_phase_locked()
            while not bypass_coordination and not bool(token["released"]):
                if bool(token["cancelled"]):
                    raise RetryAborted("市价交错等待已取消")
                if stop_event is not None and stop_event.is_set():
                    self._suspend_locked(lane)
                    raise RetryAborted("检测到停止信号")
                self._condition.wait(timeout=0.1)

        if bypass_coordination:
            return action()

        result = None
        action_error = None
        try:
            result = action()
        except BaseException as exc:
            action_error = exc
        finally:
            with self._condition:
                token["finished"] = True
                self._try_finish_phase_locked()
                while not bool(token["phase_done"]):
                    if stop_event is not None and stop_event.is_set():
                        self._suspend_locked(lane)
                        break
                    self._condition.wait(timeout=0.1)

        if action_error is not None:
            raise action_error
        return result


class Strategy:
    TRADE_OPERATION_RETRY_POLICY = RetryPolicy(attempts=10, delay_seconds=1)
    LADDER_POLL_INTERVAL_SECONDS = 1.5
    LADDER_ORDER_RECONCILE_INTERVAL_SECONDS = 5.0
    LADDER_CLEANUP_CONFIRM_TIMEOUT_SECONDS = 12.0
    LADDER_CLEANUP_CONFIRM_POLL_INTERVAL_SECONDS = 0.5
    LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE = 20

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
        market_stop_mode: str = MARKET_STOP_MODE_DEFAULT,
        market_initial_stagger: bool = MARKET_STAGGER_ENABLED_DEFAULT,
        market_stagger_coordinator: MarketStaggerCoordinator | None = None,
        market_stagger_lane_id: int = 0,
        premium_delta: Decimal | None = None,
        premium_order_count: int = PREMIUM_ORDER_COUNT_DEFAULT,
        premium_append_threshold: Decimal | None = None,
        premium_partial_fill: bool = PREMIUM_PARTIAL_FILL_DEFAULT,
        bnb_fee_stop_value: Decimal | None = None,
        bnb_topup_amount: Decimal | None = None,
        reprice_threshold_amount: Decimal | None = None,
        ladder_layout: str = LADDER_LAYOUT_DEFAULT,
        ladder_min_pool_notional: Decimal | None = None,
        ladder_sell_premium_enabled: bool = LADDER_SELL_PREMIUM_ENABLED_DEFAULT,
        ladder_sell_premium: Decimal | None = None,
        auto_ladder_settings: AutoLadderSettingsStore | None = None,
        auto_ladder_observer: AutoLadderMarketObserver | None = None,
        futures_symbol: str = FUTURES_SYMBOL_DEFAULT,
        futures_rounds: int = FUTURES_ROUNDS_DEFAULT,
        futures_amount: Decimal | None = None,
        futures_leverage: int = FUTURES_LEVERAGE_DEFAULT,
        futures_margin_type: str = FUTURES_MARGIN_TYPE_DEFAULT,
        futures_side: str = FUTURES_SIDE_DEFAULT,
        operation_journal=None,
        operation_scope: str = "",
    ):
        self.c = client
        self.spot_rounds = spot_rounds
        self.withdraw_coin = withdraw_coin
        self.withdraw_address = withdraw_address
        self.withdraw_network = withdraw_network
        self.withdraw_fee_buffer = withdraw_fee_buffer
        self.spot_symbol = BinanceClient.normalize_trade_symbol(spot_symbol)
        self.sleep_fn = sleep_fn
        self.enable_withdraw = enable_withdraw
        self.withdraw_callback = withdraw_callback
        self.trade_account_type = str(trade_account_type or TRADE_ACCOUNT_TYPE_DEFAULT)
        self.trade_mode = str(trade_mode or TRADE_MODE_DEFAULT)
        self.market_stop_mode = str(market_stop_mode or MARKET_STOP_MODE_DEFAULT)
        self.market_initial_stagger = bool(market_initial_stagger)
        self.market_stagger_coordinator = market_stagger_coordinator
        self.market_stagger_lane_id = max(0, int(market_stagger_lane_id or 0))
        self.premium_delta = Decimal(str(premium_delta if premium_delta is not None else "0"))
        self.premium_order_count = max(0, int(premium_order_count or 0))
        self.premium_append_threshold = Decimal(
            str(premium_append_threshold if premium_append_threshold is not None else PREMIUM_APPEND_THRESHOLD_DEFAULT)
        )
        self.premium_partial_fill = bool(premium_partial_fill)
        self.bnb_fee_stop_value = Decimal(str(bnb_fee_stop_value if bnb_fee_stop_value is not None else "0"))
        self.bnb_topup_amount = Decimal(str(bnb_topup_amount if bnb_topup_amount is not None else "0"))
        self.reprice_threshold_amount = Decimal(
            str(reprice_threshold_amount if reprice_threshold_amount is not None else REPRICE_THRESHOLD_DEFAULT)
        )
        self.ladder_layout = str(ladder_layout or LADDER_LAYOUT_DEFAULT).strip()
        if self.ladder_layout not in LADDER_LAYOUT_OPTIONS:
            self.ladder_layout = LADDER_LAYOUT_DEFAULT
        self.ladder_min_pool_notional = Decimal(
            str(
                ladder_min_pool_notional
                if ladder_min_pool_notional is not None
                else LADDER_MIN_POOL_NOTIONAL_DEFAULT
            )
        )
        self.ladder_sell_premium_enabled = bool(ladder_sell_premium_enabled)
        self.ladder_sell_premium = Decimal(
            str(ladder_sell_premium if ladder_sell_premium is not None else LADDER_SELL_PREMIUM_DEFAULT)
        )
        self.auto_ladder_settings = auto_ladder_settings
        self.auto_ladder_observer = auto_ladder_observer
        self.futures_symbol = BinanceClient.normalize_trade_symbol(futures_symbol or FUTURES_SYMBOL_DEFAULT)
        self.futures_rounds = max(1, int(futures_rounds or FUTURES_ROUNDS_DEFAULT))
        self.futures_amount = Decimal(str(futures_amount if futures_amount is not None else "0"))
        self.futures_leverage = max(1, int(futures_leverage or FUTURES_LEVERAGE_DEFAULT))
        self.futures_margin_type = _normalize_futures_margin_type(futures_margin_type)
        self.futures_side = str(futures_side or FUTURES_SIDE_DEFAULT).strip()
        self.operation_journal = operation_journal
        self.operation_scope = str(operation_scope or "").strip()
        self._journal_order_entries: dict[str, str] = {}
        if self.futures_margin_type not in FUTURES_MARGIN_TYPE_OPTIONS:
            self.futures_margin_type = FUTURES_MARGIN_TYPE_DEFAULT
        if self.futures_side not in FUTURES_SIDE_OPTIONS:
            self.futures_side = FUTURES_SIDE_DEFAULT
        self.trade_error = ""
        self.trade_error_stage = ""
        self.market_round_total = 0
        self.market_round_completed = 0
        self.market_round_failed_rounds: list[int] = []
        self.market_round_failure_details: list[dict[str, object]] = []
        self.ladder_cleanup_confirmed = False
        self.ladder_cleanup_error = ""

    def _reset_market_round_tracking(self) -> None:
        self.market_round_total = 0 if self._is_market_bnb_stop_mode() else max(0, int(self.spot_rounds or 0))
        self.market_round_completed = 0
        self.market_round_failed_rounds = []
        self.market_round_failure_details = []

    def _record_market_round_failure(self, round_number: int, reason: object) -> None:
        try:
            round_index = max(1, int(round_number))
        except Exception:
            round_index = 0
        reason_text = str(reason or "").strip() or "未知原因"
        if round_index not in self.market_round_failed_rounds:
            self.market_round_failed_rounds.append(round_index)
        self.market_round_failure_details.append({"round": round_index, "reason": reason_text})

    def _journal_open_order(self, order_id: int | str, side: str) -> str:
        key = str(order_id or "").strip()
        if not key or self.operation_journal is None:
            return ""
        entry_id = self._journal_order_entries.get(key)
        if entry_id:
            return entry_id
        entry_id = self.operation_journal.begin(
            "exchange-order",
            self.operation_scope,
            {"symbol": self.spot_symbol.upper(), "order_id": key, "side": str(side or "").upper()},
        )
        self.operation_journal.update(entry_id, "open")
        self._journal_order_entries[key] = entry_id
        return entry_id

    def _journal_order_state(self, order_id: int | str, side: str, state: str, **changes: object) -> None:
        entry_id = self._journal_open_order(order_id, side)
        if entry_id:
            self.operation_journal.update(entry_id, state, **changes)

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

    def _is_zero_round_spot_conversion(self) -> bool:
        try:
            rounds = int(self.spot_rounds)
        except Exception:
            return False
        return (not self._is_futures_mode()) and (not self._is_market_bnb_stop_mode()) and rounds == 0

    @staticmethod
    def _emit_progress(progress_cb, step: int, total: int, text: str) -> None:
        if progress_cb:
            progress_cb(step, total, text)

    def _spot_pair_assets(self) -> tuple[str, str]:
        symbol_u = str(self.spot_symbol or "").strip().upper()
        return self.c.get_spot_base_asset(symbol_u), self.c.get_spot_quote_asset(symbol_u)

    def _run_zero_round_spot_conversion(self, stop_event, progress_cb=None):
        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过 0 轮现货兑换")
            return

        market_mode = self._mode_name() == TRADE_MODE_MARKET
        if market_mode:
            route = self.c.resolve_spot_market_conversion(self.spot_symbol)
            base_asset = str(route.get("fromAsset") or "").strip().upper()
            quote_asset = str(route.get("toAsset") or "").strip().upper()
        else:
            base_asset, quote_asset = self._spot_pair_assets()
        self._emit_progress(progress_cb, 0, 1, "0轮兑换 %s->%s" % (base_asset, quote_asset))
        logger.info(
            "现货轮次为 0，执行前置币种兑换为后置币种：%s -> %s，交易对=%s",
            base_asset,
            quote_asset,
            self.spot_symbol,
        )
        self.sleep_fn()
        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过 0 轮现货兑换")
            return
        try:
            trade_action = (
                (lambda: self.c.spot_market_convert_base_to_quote_all(symbol=self.spot_symbol))
                if market_mode
                else (lambda: self.c.convert_base_to_quote_all(symbol=self.spot_symbol))
            )
            result = self._run_trade_action_with_retry(
                stop_event,
                "0轮兑换 %s->%s" % (base_asset, quote_asset),
                trade_action,
            )
            if result:
                logger.info("0 轮现货兑换完成：%s -> %s", base_asset, quote_asset)
            else:
                minimum_text = "市价最小下单额" if market_mode else "最小闪兑金额"
                logger.info("0 轮现货兑换未执行：可能 %s 余额不足或不满足%s", base_asset, minimum_text)
        except Exception as e:
            if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                logger.info("检测到停止信号，跳过 0 轮现货兑换")
                return
            self.trade_error = str(e)
            self.trade_error_stage = "0轮现货兑换"
            logger.error("0 轮现货兑换异常: %s", e)
            return
        self._emit_progress(progress_cb, 1, 1, "0轮兑换 %s->%s 完成" % (base_asset, quote_asset))

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

    def _market_stop_mode_name(self) -> str:
        mode = str(self.market_stop_mode or MARKET_STOP_MODE_DEFAULT)
        return mode if mode in MARKET_STOP_MODE_OPTIONS else MARKET_STOP_MODE_DEFAULT

    def _is_market_bnb_stop_mode(self) -> bool:
        return self._mode_name() == TRADE_MODE_MARKET and self._market_stop_mode_name() == MARKET_STOP_MODE_BNB

    def _is_convert_mode(self) -> bool:
        return (not self._is_futures_mode()) and self._mode_name() == TRADE_MODE_CONVERT

    def _limit_like_mode(self) -> bool:
        return self._mode_name() in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM, TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}

    def _premium_split_order_count(self) -> int:
        return max(0, int(self.premium_order_count or 0))

    def _premium_partial_fill_mode_enabled(self) -> bool:
        return self._mode_name() == TRADE_MODE_PREMIUM and bool(self.premium_partial_fill)

    def _premium_append_threshold_value(self) -> Decimal:
        if self._premium_partial_fill_mode_enabled():
            return Decimal("0")
        if self._premium_split_order_count() <= 1:
            return Decimal("0")
        threshold = Decimal(str(self.premium_append_threshold or "0"))
        if threshold <= 0:
            return Decimal("0")
        return self.c.normalize_price_delta(self.spot_symbol, threshold, min_one_tick=True)

    def _premium_append_mode_enabled(self) -> bool:
        return self._mode_name() == TRADE_MODE_PREMIUM and self._premium_append_threshold_value() > 0

    def _should_stop_for_bnb_fee(self) -> bool:
        if not self._limit_like_mode() and not self._is_market_bnb_stop_mode():
            return False
        threshold = Decimal(str(self.bnb_fee_stop_value or "0"))
        if threshold < 0:
            threshold = Decimal("0")
        if self._is_market_bnb_stop_mode() and threshold <= 0:
            raise RuntimeError("市价剩余BNB停止值必须大于 0")
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

    def _run_bnb_topup_if_needed(self, stop_event=None):
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
        spot_bnb_before = self._run_trade_action_with_retry(
            stop_event,
            "预买BNB 查询BNB余额",
            lambda: self.c.spot_asset_balance_decimal("BNB"),
        )
        bought_order = self._run_trade_action_with_retry(
            stop_event,
            "预买BNB 买入",
            lambda: self.c.buy_bnb_with_quote_amount(quote_asset, topup_amount, return_order=True),
        )
        if bought_order:
            bought_bnb = self.c._decimal_from_str(bought_order.get("toAmount", "0"), "0")
            target_spot_bnb = spot_bnb_before + bought_bnb if bought_bnb > 0 else max(
                spot_bnb_before,
                Decimal(str(self.bnb_fee_stop_value or "0")),
            )
            spot_bnb = self._run_trade_action_with_retry(
                stop_event,
                "预买BNB 确认现货余额",
                lambda: self.c.ensure_bnb_fee_ready_in_spot(
                    min_spot_balance=target_spot_bnb,
                    max_transfer_amount=bought_bnb if bought_bnb > 0 else None,
                    timeout_seconds=12.0,
                ),
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

    def _run_trade_action_with_retry(self, stop_event, context: str, action):
        context_text = str(context or "交易动作").strip() or "交易动作"
        policy = self.TRADE_OPERATION_RETRY_POLICY

        def should_retry(_exc: Exception) -> bool:
            return not bool(stop_event and stop_event.is_set())

        def on_retry(failed_attempt: int, total: int, exc: Exception) -> None:
            logger.warning(
                "%s执行异常（%d/%d）: %s，将在 %g 秒后重试",
                context_text,
                failed_attempt,
                total,
                exc,
                policy.delay_seconds,
            )

        def on_give_up(total: int, exc: Exception) -> None:
            logger.error("%s执行异常，重试 %d 次后仍失败: %s", context_text, total, exc)

        def retry_sleep(seconds: float) -> None:
            if self._pause_with_stop(stop_event, seconds):
                raise RetryAborted("检测到停止信号")

        return run_with_retries(
            action,
            policy=policy,
            should_retry=should_retry,
            on_retry=on_retry,
            on_give_up=on_give_up,
            sleep_fn=retry_sleep,
        )

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
        self._journal_open_order(order_id, side_u)

        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    self._journal_order_state(order_id, side_u, "cancelled", reason="stop")
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.c.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                self._journal_order_state(order_id, side_u, "completed", exchange_status=status)
                return "filled", order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                self._journal_order_state(order_id, side_u, "cancelled", exchange_status=status)
                raise RuntimeError(f"订单未成交，状态={status}")

            if status == "NEW":
                book_ticker = self.c.get_book_ticker(symbol_u)
                should_reprice, current_ref = self._should_reprice_open_order(side_u, order_price, book_ticker)
                if should_reprice:
                    try:
                        self.c.cancel_order(symbol_u, order_id)
                        self._journal_order_state(order_id, side_u, "cancelled", reason="reprice")
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
                            self._journal_order_state(order_id, side_u, "completed", exchange_status=latest_status)
                            return "filled", latest_order
                        raise

            if stop_event and stop_event.wait(max(0.2, float(poll_interval))):
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    self._journal_order_state(order_id, side_u, "cancelled", reason="stop")
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

    def _cancel_limit_order_with_fill_guard(
        self,
        order_id: int | str,
        side: str,
        *,
        cleanup_after_stop: bool = False,
    ) -> dict:
        symbol_u = self.spot_symbol.upper()
        get_order = self.c.get_order
        cancel_order = self.c.cancel_order
        if cleanup_after_stop:
            get_order = getattr(self.c, "get_order_for_cleanup", get_order)
            cancel_order = getattr(self.c, "cancel_order_for_cleanup", cancel_order)
        latest_order = None
        try:
            latest_order = get_order(symbol_u, order_id)
        except Exception:
            latest_order = None

        if latest_order is not None:
            latest_status = str(latest_order.get("status") or "").upper()
            if latest_status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                return latest_order

        try:
            cancel_result = cancel_order(symbol_u, order_id)
        except Exception as cancel_exc:
            try:
                latest_order = get_order(symbol_u, order_id)
            except Exception:
                latest_order = None
            if latest_order is not None and str(latest_order.get("status") or "").upper() == "FILLED":
                return latest_order
            raise cancel_exc

        try:
            latest_order = get_order(symbol_u, order_id)
        except Exception:
            cancel_status = (
                str(cancel_result.get("status") or "").upper()
                if isinstance(cancel_result, dict)
                else ""
            )
            if cancel_status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                return dict(cancel_result)
            if cleanup_after_stop:
                raise RuntimeError(f"订单 #{order_id} 撤销后状态未确认")
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
        self.c.collect_asset_to_spot(quote_asset, context="交易前")

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

    def _place_limit_orders(
        self,
        mode_name: str,
        book_ticker: dict[str, Decimal],
        *,
        progress_cb=None,
        step: int = 0,
    ) -> tuple[dict | None, dict | None]:
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
        action_parts: list[str] = []
        if quote_balance > 0:
            action_parts.append(f"挂买{base_asset}")
        if base_balance > 0:
            action_parts.append(f"挂卖{base_asset}")
        if action_parts:
            self._emit_progress(
                progress_cb,
                max(0, int(step or 0)),
                max(int(step or 0) + 1, 1),
                f"{mode_name}轮 {int(step or 0) + 1} {'/'.join(action_parts)}",
            )
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
            self._journal_open_order(buy_order.get("orderId"), "BUY")
            active_orders["BUY"] = {
                "order_id": buy_order.get("orderId"),
                "price": self._order_price_decimal(buy_order, Decimal("0")),
            }
        if sell_order and sell_order.get("orderId"):
            self._journal_open_order(sell_order.get("orderId"), "SELL")
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

    @staticmethod
    def _order_cumulative_quote_decimal(order_data: dict) -> Decimal:
        if not isinstance(order_data, dict):
            return Decimal("0")
        raw = order_data.get("cummulativeQuoteQty", order_data.get("cumulativeQuoteQty", "0"))
        try:
            value = Decimal(str(raw or "0"))
        except Exception:
            return Decimal("0")
        return value if value > 0 else Decimal("0")

    @staticmethod
    def _order_terminal_status(status: str) -> bool:
        return str(status or "").strip().upper() in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}

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

    def _run_premium_partial_fill_mode(self, stop_event, progress_cb=None):
        mode_name = self._mode_name()
        base_asset, quote_asset = self._spot_pair_assets()
        active_buy_orders: dict[str, dict[str, object]] = {}
        active_sell_orders: dict[str, dict[str, object]] = {}
        pending_base_qty = Decimal("0")
        pending_base_quote = Decimal("0")
        pending_quote = Decimal("0")
        step = 0
        stop_new_entries = False
        stop_reason_logged = False
        mode_logged = False

        def retry(context: str, action):
            return self._run_trade_action_with_retry(stop_event, context, action)

        def _add_active_order(
            active_orders: dict[str, dict[str, object]],
            order: dict,
            fallback_price: Decimal,
            *,
            side_text: str,
            extra: dict[str, object] | None = None,
        ) -> dict[str, object]:
            order_id = order.get("orderId")
            if not order_id:
                raise RuntimeError(f"溢价单部分成交循环{side_text}单返回缺少 orderId")
            self._journal_open_order(order_id, "BUY" if side_text == "买" else "SELL")
            order_price = self._order_price_decimal(order, fallback_price)
            state: dict[str, object] = {
                "order_id": order_id,
                "price": order_price,
                "processed_qty": Decimal("0"),
                "processed_quote": Decimal("0"),
            }
            if extra:
                state.update(extra)
            active_orders[str(order_id)] = state
            return state

        def _processed_decimal(state: dict[str, object], key: str) -> Decimal:
            try:
                return Decimal(str(state.get(key, "0") or "0"))
            except Exception:
                return Decimal("0")

        def _order_delta(order: dict, state: dict[str, object]) -> tuple[Decimal, Decimal, Decimal]:
            order_price = self._order_price_decimal(order, Decimal(str(state.get("price", "0") or "0")))
            state["price"] = order_price
            executed_qty = self._order_executed_qty_decimal(order)
            cumulative_quote = self._order_cumulative_quote_decimal(order)
            processed_qty = _processed_decimal(state, "processed_qty")
            processed_quote = _processed_decimal(state, "processed_quote")
            delta_qty = executed_qty - processed_qty
            delta_quote = cumulative_quote - processed_quote
            if delta_qty <= 0:
                return Decimal("0"), Decimal("0"), order_price
            if delta_quote <= 0 and order_price > 0:
                delta_quote = delta_qty * order_price
            if delta_quote < 0:
                delta_quote = Decimal("0")
            state["processed_qty"] = executed_qty
            state["processed_quote"] = processed_quote + delta_quote
            return delta_qty, delta_quote, order_price

        def _buy_order_quote_amount(order: dict, order_price: Decimal, fallback_amount: Decimal) -> Decimal:
            try:
                orig_qty = Decimal(str(order.get("origQty", order.get("executedQty", "0")) or "0"))
            except Exception:
                orig_qty = Decimal("0")
            if orig_qty > 0 and order_price > 0:
                return orig_qty * order_price
            try:
                fallback = Decimal(str(fallback_amount or "0"))
            except Exception:
                fallback = Decimal("0")
            return fallback if fallback > 0 else Decimal("0")

        def _remaining_buy_quote(state: dict[str, object]) -> Decimal:
            quote_amount = _processed_decimal(state, "quote_amount")
            processed_quote = _processed_decimal(state, "processed_quote")
            remaining = quote_amount - processed_quote
            return remaining if remaining > 0 else Decimal("0")

        def _record_buy_delta(order: dict, state: dict[str, object]) -> tuple[Decimal, Decimal, Decimal]:
            nonlocal pending_base_qty, pending_base_quote
            delta_qty, delta_quote, order_price = _order_delta(order, state)
            if delta_qty > 0:
                pending_base_qty += delta_qty
                pending_base_quote += delta_quote
                logger.info(
                    "溢价单部分成交循环买单新增成交：订单#%s 数量=%s 均价=%s，准备挂卖",
                    state["order_id"],
                    BinanceClient._format_decimal(delta_qty),
                    BinanceClient._format_decimal(delta_quote / delta_qty if delta_qty > 0 and delta_quote > 0 else order_price),
                )
            return delta_qty, delta_quote, order_price

        def _try_reprice_buy_order(order_key: str, state: dict[str, object], order_price: Decimal) -> bool:
            if stop_new_entries or self._reprice_threshold_value() <= 0:
                return False
            book_ticker = retry(
                f"{mode_name}部分成交 读取买单重挂盘口",
                lambda: self.c.get_book_ticker(self.spot_symbol),
            )
            should_reprice, current_ref = self._should_reprice_open_order("BUY", order_price, book_ticker)
            if not should_reprice:
                return False

            latest_order = retry(
                f"{mode_name}部分成交 撤销买单#{state['order_id']}",
                lambda: self._cancel_limit_order_with_fill_guard(state["order_id"], "BUY"),
            )
            latest_status = str(latest_order.get("status") or "").upper()
            _record_buy_delta(latest_order, state)
            active_buy_orders.pop(order_key, None)

            remaining_quote = _remaining_buy_quote(state)
            if latest_status == "FILLED" or remaining_quote <= 0:
                logger.info(
                    "溢价单部分成交循环买单价格偏离超过 %s，撤单前已全部成交或无剩余金额：旧单#%s 旧价=%s 当前买1=%s",
                    self._reprice_threshold_log_text(),
                    state["order_id"],
                    BinanceClient._format_decimal(order_price),
                    BinanceClient._format_decimal(current_ref),
                )
                return True

            available_quote = retry(
                f"{mode_name}部分成交 查询{quote_asset}余额",
                lambda: self.c.spot_asset_balance_decimal(quote_asset),
            )
            reprice_amount = min(remaining_quote, available_quote)
            if reprice_amount <= 0:
                logger.info(
                    "溢价单部分成交循环买单价格偏离超过 %s，已撤销旧单#%s，但 %s 可用余额不足，剩余金额=%s %s 暂不重挂",
                    self._reprice_threshold_log_text(),
                    state["order_id"],
                    quote_asset,
                    BinanceClient._format_decimal(remaining_quote),
                    quote_asset,
                )
                return True

            buy_price = Decimal(str(book_ticker["bidPrice"]))
            self._emit_progress(progress_cb, step, max(step + 1, 1), f"{mode_name}部分成交 重挂买入 {base_asset}")
            buy_order = retry(
                f"{mode_name}部分成交 重挂买单",
                lambda: self.c.spot_limit_buy_quote_amount(self.spot_symbol, buy_price, reprice_amount),
            )
            if not buy_order:
                logger.info(
                    "溢价单部分成交循环买单价格偏离超过 %s，旧单#%s 已撤销，剩余金额=%s %s 不满足重挂条件",
                    self._reprice_threshold_log_text(),
                    state["order_id"],
                    BinanceClient._format_decimal(reprice_amount),
                    quote_asset,
                )
                return True

            new_state = _add_active_order(active_buy_orders, buy_order, buy_price, side_text="重挂买")
            new_price = Decimal(str(new_state["price"]))
            new_quote_amount = _buy_order_quote_amount(buy_order, new_price, reprice_amount)
            new_state["quote_amount"] = new_quote_amount
            logger.info(
                "溢价单部分成交循环买单价格偏离超过 %s，已撤单重挂：旧单#%s 旧价=%s 当前买1=%s 重挂金额=%s %s 新单#%s 新价=%s",
                self._reprice_threshold_log_text(),
                state["order_id"],
                BinanceClient._format_decimal(order_price),
                BinanceClient._format_decimal(current_ref),
                BinanceClient._format_decimal(new_quote_amount),
                quote_asset,
                new_state["order_id"],
                BinanceClient._format_decimal(new_price),
            )
            return True

        def _place_initial_buy() -> bool:
            nonlocal mode_logged
            available_quote = retry(
                f"{mode_name}部分成交 查询{quote_asset}余额",
                lambda: self.c.spot_asset_balance_decimal(quote_asset),
            )
            if available_quote <= 0:
                logger.info("溢价单部分成交循环现货 %s 可用余额为 0，结束本次运行", quote_asset)
                return False
            if not mode_logged:
                reprice_threshold = self._reprice_threshold_value()
                if reprice_threshold > 0:
                    logger.info(
                        "启用溢价单部分成交循环：买单部分成交立即挂卖，卖单部分成交立即挂买；追加挂单已自动关闭，买单重挂阈值=%s %s，卖单不自动重挂",
                        BinanceClient._format_decimal(reprice_threshold),
                        quote_asset,
                    )
                else:
                    logger.info(
                        "启用溢价单部分成交循环：买单部分成交立即挂卖，卖单部分成交立即挂买；追加挂单已自动关闭，买单重挂未启用，卖单不自动重挂"
                    )
                mode_logged = True
            book_ticker = retry(
                f"{mode_name}部分成交 读取首笔盘口",
                lambda: self.c.get_book_ticker(self.spot_symbol),
            )
            buy_price = Decimal(str(book_ticker["bidPrice"]))
            self._emit_progress(progress_cb, step, max(step + 1, 1), f"{mode_name}部分成交 买入 {base_asset}")
            buy_order = retry(
                f"{mode_name}部分成交 首笔买单",
                lambda: self.c.spot_limit_buy_all_usdt(symbol=self.spot_symbol, price=buy_price),
            )
            if not buy_order:
                return False
            state = _add_active_order(active_buy_orders, buy_order, buy_price, side_text="买")
            quote_amount = _buy_order_quote_amount(buy_order, Decimal(str(state["price"])), available_quote)
            state["quote_amount"] = quote_amount
            logger.info(
                "溢价单部分成交循环挂出买单：金额约=%s %s 价格=%s",
                BinanceClient._format_decimal(quote_amount if quote_amount > 0 else available_quote),
                quote_asset,
                BinanceClient._format_decimal(Decimal(str(state["price"]))),
            )
            return True

        def _try_place_pending_sell() -> bool:
            nonlocal pending_base_qty, pending_base_quote
            if pending_base_qty <= 0:
                return False
            available_base = retry(
                f"{mode_name}部分成交 查询{base_asset}余额",
                lambda: self.c.spot_asset_balance_decimal(base_asset),
            )
            sell_qty = min(pending_base_qty, available_base)
            if sell_qty <= 0:
                return False
            if pending_base_quote > 0 and pending_base_qty > 0:
                buy_avg_price = pending_base_quote / pending_base_qty
            else:
                buy_avg_price = Decimal(str(retry(
                    f"{mode_name}部分成交 读取卖单兜底盘口",
                    lambda: self.c.get_book_ticker(self.spot_symbol),
                )["bidPrice"]))
            sell_price = self._premium_sell_price(buy_avg_price)
            self._emit_progress(progress_cb, step, max(step + 1, 1), f"{mode_name}部分成交 卖出 {base_asset}")
            sell_order = retry(
                f"{mode_name}部分成交 挂卖单",
                lambda: self.c.spot_limit_sell_quantity(self.spot_symbol, sell_price, sell_qty),
            )
            if not sell_order:
                return False

            order_qty = Decimal(str(sell_order.get("origQty", sell_order.get("executedQty", sell_qty)) or sell_qty))
            if order_qty <= 0 or order_qty > sell_qty:
                order_qty = sell_qty
            base_before = pending_base_qty
            quote_before = pending_base_quote
            quote_for_order = Decimal("0")
            if base_before > 0 and quote_before > 0:
                quote_for_order = quote_before * order_qty / base_before
            pending_base_qty = max(Decimal("0"), base_before - order_qty)
            pending_base_quote = max(Decimal("0"), quote_before - quote_for_order)

            state = _add_active_order(
                active_sell_orders,
                sell_order,
                sell_price,
                side_text="卖",
                extra={
                    "quantity": order_qty,
                    "buy_fill_price": buy_avg_price,
                },
            )
            logger.info(
                "溢价单部分成交循环已将买单新增成交挂卖：数量=%s 买入均价=%s 卖价=%s",
                BinanceClient._format_decimal(order_qty),
                BinanceClient._format_decimal(buy_avg_price),
                BinanceClient._format_decimal(Decimal(str(state["price"]))),
            )
            return True

        def _try_place_pending_buy() -> bool:
            nonlocal pending_quote
            if stop_new_entries or pending_quote <= 0:
                return False
            available_quote = retry(
                f"{mode_name}部分成交 查询{quote_asset}余额",
                lambda: self.c.spot_asset_balance_decimal(quote_asset),
            )
            buy_amount = min(pending_quote, available_quote)
            if buy_amount <= 0:
                return False
            book_ticker = retry(
                f"{mode_name}部分成交 读取回买盘口",
                lambda: self.c.get_book_ticker(self.spot_symbol),
            )
            buy_price = Decimal(str(book_ticker["bidPrice"]))
            self._emit_progress(progress_cb, step, max(step + 1, 1), f"{mode_name}部分成交 买入 {base_asset}")
            buy_order = retry(
                f"{mode_name}部分成交 挂回买单",
                lambda: self.c.spot_limit_buy_quote_amount(self.spot_symbol, buy_price, buy_amount),
            )
            if not buy_order:
                return False

            state = _add_active_order(active_buy_orders, buy_order, buy_price, side_text="买")
            order_price = Decimal(str(state["price"]))
            try:
                order_qty = Decimal(str(buy_order.get("origQty", buy_order.get("executedQty", "0")) or "0"))
            except Exception:
                order_qty = Decimal("0")
            locked_quote = order_qty * order_price if order_qty > 0 and order_price > 0 else buy_amount
            if locked_quote <= 0 or locked_quote > buy_amount:
                locked_quote = buy_amount
            state["quote_amount"] = locked_quote
            pending_quote = max(Decimal("0"), pending_quote - locked_quote)
            logger.info(
                "溢价单部分成交循环已将卖单新增成交挂买：金额=%s %s 价格=%s",
                BinanceClient._format_decimal(locked_quote),
                quote_asset,
                BinanceClient._format_decimal(order_price),
            )
            return True

        while True:
            if stop_event and stop_event.is_set():
                self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                logger.info("检测到停止信号，已撤销溢价单部分成交循环未完成订单")
                return

            if (not stop_new_entries) and self._should_stop_for_bnb_fee():
                stop_new_entries = True
                if not stop_reason_logged:
                    logger.info("溢价单部分成交循环已触发 BNB 手续费停止条件，停止新增买单，继续处理已成交基础币卖出")
                    stop_reason_logged = True

            try:
                if (
                    not active_buy_orders
                    and not active_sell_orders
                    and pending_base_qty <= 0
                    and pending_quote <= 0
                ):
                    if stop_new_entries:
                        logger.info("溢价单部分成交循环当前无未完成订单，结束本次运行")
                        return
                    if not _place_initial_buy():
                        logger.info("溢价单部分成交循环首笔买单未执行（可能余额不足或不满足最小下单额），结束本次运行")
                        return

                for order_key, state in list(active_buy_orders.items()):
                    order = retry(
                        f"{mode_name}部分成交 查询买单#{state['order_id']}",
                        lambda order_id=state["order_id"]: self.c.get_order(self.spot_symbol, order_id),
                    )
                    status = str(order.get("status") or "").upper()
                    _delta_qty, _delta_quote, order_price = _record_buy_delta(order, state)
                    if self._order_terminal_status(status):
                        active_buy_orders.pop(order_key, None)
                        continue
                    _try_reprice_buy_order(order_key, state, order_price)

                _try_place_pending_sell()

                for order_key, state in list(active_sell_orders.items()):
                    order = retry(
                        f"{mode_name}部分成交 查询卖单#{state['order_id']}",
                        lambda order_id=state["order_id"]: self.c.get_order(self.spot_symbol, order_id),
                    )
                    status = str(order.get("status") or "").upper()
                    delta_qty, delta_quote, order_price = _order_delta(order, state)
                    if delta_qty > 0:
                        pending_quote += delta_quote
                        logger.info(
                            "溢价单部分成交循环卖单新增成交：订单#%s 数量=%s 均价=%s，准备挂买",
                            state["order_id"],
                            BinanceClient._format_decimal(delta_qty),
                            BinanceClient._format_decimal(delta_quote / delta_qty if delta_qty > 0 and delta_quote > 0 else order_price),
                        )
                    if status == "FILLED":
                        active_sell_orders.pop(order_key, None)
                        step += 1
                        self._emit_progress(progress_cb, step, max(step, 1), f"{mode_name}部分成交 {step} 完成")
                    elif self._order_terminal_status(status):
                        active_sell_orders.pop(order_key, None)

                _try_place_pending_buy()

                if not active_buy_orders and not active_sell_orders:
                    if pending_base_qty > 0 and not _try_place_pending_sell():
                        logger.info(
                            "溢价单部分成交循环剩余 %s 数量 %s 暂无法挂卖，结束本次运行",
                            base_asset,
                            BinanceClient._format_decimal(pending_base_qty),
                        )
                        return
                    if pending_quote > 0:
                        if stop_new_entries:
                            logger.info(
                                "溢价单部分成交循环已触发停止条件，卖出所得 %s %s 不再挂买，结束本次运行",
                                BinanceClient._format_decimal(pending_quote),
                                quote_asset,
                            )
                            return
                        if not _try_place_pending_buy():
                            logger.info(
                                "溢价单部分成交循环剩余 %s 金额 %s 暂无法挂买，结束本次运行",
                                quote_asset,
                                BinanceClient._format_decimal(pending_quote),
                            )
                            return

                if stop_event and stop_event.wait(1.0):
                    continue
                if not stop_event:
                    time.sleep(1.0)
            except Exception as e:
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                    logger.info("检测到停止信号，已停止后续执行（%s部分成交循环）", mode_name)
                    return
                logger.error("%s部分成交循环执行异常: %s", mode_name, e)

    def _run_premium_append_mode(self, stop_event, progress_cb=None):
        mode_name = self._mode_name()
        append_threshold = self._premium_append_threshold_value()
        order_count = self._premium_split_order_count()
        if order_count <= 1 or append_threshold <= 0:
            self._run_premium_mode(stop_event, progress_cb=progress_cb)
            return
        base_asset, quote_asset = self._spot_pair_assets()

        step = 0
        active_buy_orders: dict[str, dict[str, object]] = {}
        active_sell_orders: dict[str, dict[str, object]] = {}
        last_append_price: Decimal | None = None
        per_order_quote_amount = Decimal("0")
        submitted_buy_count = 0
        stop_new_entries = False
        stop_reason_logged = False
        mode_logged = False

        def retry(context: str, action):
            return self._run_trade_action_with_retry(stop_event, context, action)

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

                    retry(
                        f"{mode_name}分笔 交易前归集{quote_asset}",
                        lambda: self.c.collect_asset_to_spot(quote_asset, context="交易前"),
                    )
                    cycle_quote_amount = retry(
                        f"{mode_name}分笔 查询{quote_asset}余额",
                        lambda: self.c.spot_asset_balance_decimal(quote_asset),
                    )
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

                    book_ticker = retry(
                        f"{mode_name}分笔 读取首笔盘口",
                        lambda: self.c.get_book_ticker(self.spot_symbol),
                    )
                    current_bid = Decimal(str(book_ticker["bidPrice"]))
                    self._emit_progress(
                        progress_cb,
                        step,
                        max(step + 1, 1),
                        f"{mode_name}分笔 挂买 {base_asset} 第1笔",
                    )
                    initial_order = retry(
                        f"{mode_name}分笔 挂买第1笔",
                        lambda: self._place_premium_split_buy_order(per_order_quote_amount, current_bid, 1),
                    )
                    if not initial_order:
                        logger.info("溢价单模式首笔买单未执行（可能余额不足或不满足最小下单额），结束本次运行")
                        return
                    active_buy_orders[str(initial_order["order_id"])] = initial_order
                    submitted_buy_count = 1
                    last_append_price = Decimal(str(initial_order["price"]))

                for order_key, state in list(active_buy_orders.items()):
                    order = retry(
                        f"{mode_name}分笔 查询买单#{state['order_id']}",
                        lambda order_id=state["order_id"]: self.c.get_order(self.spot_symbol, order_id),
                    )
                    status = str(order.get("status") or "").upper()
                    executed_qty = self._order_executed_qty_decimal(order)

                    if status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                        active_buy_orders.pop(order_key, None)

                    if status == "FILLED" or (status in {"CANCELED", "EXPIRED", "EXPIRED_IN_MATCH"} and executed_qty > 0):
                        buy_fill_price = retry(
                            f"{mode_name}分笔 读取买单均价#{state['order_id']}",
                            lambda order_data=order: self.c.get_order_average_price(order_data),
                        ) or Decimal(str(state.get("price", "0") or "0"))
                        self._emit_progress(
                            progress_cb,
                            step,
                            max(step + 1, 1),
                            f"{mode_name}分笔 挂卖 {base_asset}",
                        )
                        sell_state = retry(
                            f"{mode_name}分笔 挂卖{base_asset}",
                            lambda: self._place_premium_split_sell_order(executed_qty, buy_fill_price),
                        )
                        if sell_state:
                            active_sell_orders[str(sell_state["order_id"])] = sell_state
                        else:
                            logger.warning(
                                "溢价单模式买单已成交，但对应卖单未挂出：数量=%s 买入均价=%s",
                                BinanceClient._format_decimal(executed_qty),
                                BinanceClient._format_decimal(buy_fill_price),
                            )

                for order_key, state in list(active_sell_orders.items()):
                    order = retry(
                        f"{mode_name}分笔 查询卖单#{state['order_id']}",
                        lambda order_id=state["order_id"]: self.c.get_order(self.spot_symbol, order_id),
                    )
                    status = str(order.get("status") or "").upper()
                    if status == "FILLED":
                        active_sell_orders.pop(order_key, None)
                        step += 1
                        qty = self._order_executed_qty_decimal(order)
                        avg_price = retry(
                            f"{mode_name}分笔 读取卖单均价#{state['order_id']}",
                            lambda order_data=order: self.c.get_order_average_price(order_data),
                        ) or Decimal(str(state.get("price", "0") or "0"))
                        logger.info(
                            "--- %s分笔 %d 完成：卖单成交 数量=%s 均价=%s ---",
                            mode_name,
                            step,
                            BinanceClient._format_decimal(qty),
                            BinanceClient._format_decimal(avg_price),
                        )
                        self._emit_progress(progress_cb, step, max(step, 1), f"{mode_name}分笔 {step} 完成")
                    elif status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                        active_sell_orders.pop(order_key, None)
                        logger.warning(
                            "溢价单模式卖单结束，状态=%s，原挂价=%s",
                            status,
                            BinanceClient._format_decimal(Decimal(str(state.get("price", "0") or "0"))),
                        )

                if (not stop_new_entries) and per_order_quote_amount > 0:
                    if submitted_buy_count < order_count and last_append_price is not None:
                        book_ticker = retry(
                            f"{mode_name}分笔 读取追加盘口",
                            lambda: self.c.get_book_ticker(self.spot_symbol),
                        )
                        current_bid = Decimal(str(book_ticker["bidPrice"]))
                        price_diff = abs(current_bid - last_append_price)
                        if price_diff >= append_threshold:
                            next_slot_index = submitted_buy_count + 1
                            self._emit_progress(
                                progress_cb,
                                step,
                                max(step + 1, 1),
                                f"{mode_name}分笔 挂买 {base_asset} 第{next_slot_index}笔",
                            )
                            appended_order = retry(
                                f"{mode_name}分笔 追加买单第{next_slot_index}笔",
                                lambda: self._place_premium_split_buy_order(
                                    per_order_quote_amount,
                                    current_bid,
                                    next_slot_index,
                                ),
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
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    self._cancel_premium_active_orders(active_buy_orders, active_sell_orders)
                    logger.info("检测到停止信号，已停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s分笔模式执行异常: %s", mode_name, e)

    @staticmethod
    def _ladder_layout_level_counts(layout: str) -> tuple[int, int]:
        normalized = str(layout or LADDER_LAYOUT_DEFAULT).strip()
        mapping = {
            LADDER_LAYOUT_BUY2_SELL2: (2, 2),
            LADDER_LAYOUT_BUY1_SELL2: (1, 2),
            LADDER_LAYOUT_BUY2_SELL1: (2, 1),
            LADDER_LAYOUT_BUY1_SELL1: (1, 1),
        }
        return mapping.get(normalized, mapping[LADDER_LAYOUT_DEFAULT])

    def _ladder_min_pool_value(self) -> Decimal:
        value = Decimal(str(self.ladder_min_pool_notional or "0"))
        return value if value > 0 else Decimal("0")

    def _ladder_sell_premium_value(self) -> Decimal:
        value = Decimal(str(self.ladder_sell_premium or "0"))
        if value <= 0:
            return Decimal("0")
        return self.c.normalize_price_delta(self.spot_symbol, value, min_one_tick=True)

    def _ladder_target_prices(
        self,
        bid_anchor: Decimal,
        ask_anchor: Decimal,
        *,
        sell_bid_anchor: Decimal | None = None,
    ) -> dict[str, list[Decimal]]:
        tick_size = Decimal(str(self.c.get_symbol_tick_size(self.spot_symbol) or "0"))
        if tick_size <= 0:
            raise RuntimeError(f"无法读取 {self.spot_symbol} 的最小价格精度 tickSize")
        buy_levels, sell_levels = self._ladder_layout_level_counts(self.ladder_layout)
        bid_price = self.c.adjust_price_to_valid_tick(self.spot_symbol, Decimal(str(bid_anchor)))
        if self.ladder_sell_premium_enabled and sell_bid_anchor is not None:
            sell_price = Decimal(str(sell_bid_anchor)) + self._ladder_sell_premium_value()
        else:
            sell_price = Decimal(str(ask_anchor))
        ask_price = self.c.adjust_price_to_valid_tick(self.spot_symbol, sell_price, round_up=True)
        buy_prices = [bid_price]
        sell_prices = [ask_price]
        if buy_levels > 1:
            buy_prices.append(self.c.adjust_price_to_valid_tick(self.spot_symbol, bid_price - tick_size))
        if sell_levels > 1:
            sell_prices.append(
                self.c.adjust_price_to_valid_tick(
                    self.spot_symbol,
                    ask_price + tick_size,
                    round_up=True,
                )
            )
        if any(price <= 0 for price in buy_prices + sell_prices):
            raise RuntimeError(f"{self.spot_symbol} 档位价格无效")
        return {"BUY": buy_prices, "SELL": sell_prices}

    @staticmethod
    def _ladder_split_amount(amount: Decimal, level_count: int) -> list[Decimal]:
        total = Decimal(str(amount))
        count = max(1, int(level_count or 1))
        if total <= 0:
            return []
        if count == 1:
            return [total]
        first = total / Decimal(str(count))
        result = [first for _ in range(count - 1)]
        result.append(total - sum(result, Decimal("0")))
        return result

    def _ladder_validate_pool_threshold(self) -> None:
        threshold = self._ladder_min_pool_value()
        if threshold <= 0:
            raise RuntimeError("最小分仓成交额必须大于 0")
        if self.ladder_sell_premium_enabled and self._ladder_sell_premium_value() <= 0:
            raise RuntimeError("勾选溢价挂卖时，卖出溢价必须大于 0")
        rules = self.c.get_symbol_trade_rules(self.spot_symbol) or {}
        min_notional = Decimal(str(rules.get("minNotional", "0") or "0"))
        buy_levels, sell_levels = self._ladder_layout_level_counts(self.ladder_layout)
        required_threshold = min_notional * Decimal(str(max(buy_levels, sell_levels)))
        if min_notional > 0 and threshold < required_threshold:
            quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
            raise RuntimeError(
                "最小分仓成交额过小：当前档位模式至少需要 "
                f"{BinanceClient._format_decimal(required_threshold)} {quote_asset}"
            )

    @staticmethod
    def _ladder_pending_amount(pending_allocations: list[dict[str, object]], side: str) -> Decimal:
        side_u = str(side or "").strip().upper()
        return sum(
            (
                Decimal(str(item.get("amount", "0") or "0"))
                for item in pending_allocations
                if str(item.get("side") or "").strip().upper() == side_u
            ),
            Decimal("0"),
        )

    def _ladder_enqueue_allocations(
        self,
        pending_allocations: list[dict[str, object]],
        side: str,
        amount: Decimal,
    ) -> None:
        side_u = str(side or "").strip().upper()
        buy_levels, sell_levels = self._ladder_layout_level_counts(self.ladder_layout)
        level_count = buy_levels if side_u == "BUY" else sell_levels
        for level_index, split_amount in enumerate(self._ladder_split_amount(amount, level_count)):
            if split_amount <= 0:
                continue
            pending_allocations.append(
                {
                    "side": side_u,
                    "level_index": level_index,
                    "amount": split_amount,
                    "source": "pool",
                }
            )

    def _ladder_submit_pending_allocations(
        self,
        pending_allocations: list[dict[str, object]],
        active_orders: dict[str, dict[str, object]],
        target_prices: dict[str, list[Decimal]],
        *,
        sell_from_bid: bool = False,
        market_guard: Callable[[Decimal, Decimal], bool] | None = None,
    ) -> dict[str, Decimal]:
        if not pending_allocations:
            return {}
        active_counts = {
            side: sum(
                1
                for state in active_orders.values()
                if str(state.get("side") or "").strip().upper() == side
            )
            for side in ("BUY", "SELL")
        }
        pending_sides = {
            str(item.get("side") or "").strip().upper()
            for item in pending_allocations
        }
        if pending_sides and all(
            active_counts.get(side, 0) >= self.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE
            for side in pending_sides
        ):
            return {}
        latest_book = self.c.get_book_ticker(self.spot_symbol)
        latest_bid = Decimal(str(latest_book["bidPrice"]))
        latest_ask = Decimal(str(latest_book["askPrice"]))
        if market_guard is not None and not market_guard(latest_bid, latest_ask):
            raise _AutoLadderMarketPause("提交前盘口价格超出安全范围")
        live_target_prices = self._ladder_target_prices(
            latest_bid,
            latest_ask,
            sell_bid_anchor=(latest_bid if sell_from_bid else None),
        )
        ordered_allocations = sorted(
            pending_allocations,
            key=lambda item: 0 if item.get("source") == "reprice" else 1,
        )
        submitted_anchors: dict[str, Decimal] = {}
        order_kwargs = {"maker_only": True}
        if market_guard is not None and isinstance(self.c, BinanceClient):
            def retry_guard() -> bool:
                retry_book = self.c.get_book_ticker(self.spot_symbol)
                return market_guard(
                    Decimal(str(retry_book["bidPrice"])), Decimal(str(retry_book["askPrice"])),
                )
            order_kwargs.update(safe_retry=True, retry_guard=retry_guard)
        for index, allocation in enumerate(ordered_allocations):
            if market_guard is not None:
                if index:
                    latest_book = self.c.get_book_ticker(self.spot_symbol)
                    latest_bid = Decimal(str(latest_book["bidPrice"]))
                    latest_ask = Decimal(str(latest_book["askPrice"]))
                    live_target_prices = self._ladder_target_prices(
                        latest_bid, latest_ask,
                        sell_bid_anchor=(latest_bid if sell_from_bid else None),
                    )
                if not market_guard(latest_bid, latest_ask):
                    raise _AutoLadderMarketPause("提交子订单前公共行情已变化")
            side_u = str(allocation.get("side") or "").strip().upper()
            amount = Decimal(str(allocation.get("amount", "0") or "0"))
            prices = list(live_target_prices.get(side_u) or target_prices.get(side_u) or [])
            if amount <= 0 or not prices:
                pending_allocations.remove(allocation)
                continue
            if active_counts.get(side_u, 0) >= self.LADDER_MAX_ACTIVE_CHILD_ORDERS_PER_SIDE:
                continue
            level_index = min(max(0, int(allocation.get("level_index", 0) or 0)), len(prices) - 1)
            order_price = Decimal(str(prices[level_index]))
            try:
                if side_u == "BUY":
                    order = self.c.spot_limit_buy_quote_amount(
                        self.spot_symbol,
                        order_price,
                        amount,
                        **order_kwargs,
                    )
                else:
                    order = self.c.spot_limit_sell_quantity(
                        self.spot_symbol,
                        order_price,
                        amount,
                        **order_kwargs,
                    )
            except BinanceAPIError as exc:
                if exc.code == -2010 and "immediately match and take" in str(exc.msg or "").lower():
                    logger.warning(
                        "分仓挂单%s%d提交时盘口已变化，保留额度等待下一轮：金额/数量=%s，价格=%s",
                        "买" if side_u == "BUY" else "卖",
                        level_index + 1,
                        BinanceClient._format_decimal(amount),
                        BinanceClient._format_decimal(order_price),
                    )
                    break
                raise
            if not order:
                logger.warning(
                    "分仓挂单%s%d未挂出，将保留可用余额等待后续处理：金额/数量=%s，价格=%s",
                    "买" if side_u == "BUY" else "卖",
                    level_index + 1,
                    BinanceClient._format_decimal(amount),
                    BinanceClient._format_decimal(order_price),
                )
                pending_allocations.remove(allocation)
                continue
            order_id = order.get("orderId")
            if not order_id:
                raise RuntimeError("分仓挂单返回缺少 orderId")
            actual_price = self._order_price_decimal(order, order_price)
            active_orders[str(order_id)] = {
                "order_id": order_id,
                "side": side_u,
                "price": actual_price,
                "level_index": level_index,
            }
            active_counts[side_u] = active_counts.get(side_u, 0) + 1
            submitted_anchors[side_u] = (
                Decimal(str(latest_book["bidPrice"]))
                if side_u == "BUY"
                else Decimal(str(latest_book["askPrice"]))
            )
            self._journal_open_order(order_id, side_u)
            pending_allocations.remove(allocation)
            logger.info(
                "分仓挂单%s%s%d子订单：金额/数量=%s，价格=%s，订单#%s",
                "重挂补档" if allocation.get("source") == "reprice" else "追加",
                "买" if side_u == "BUY" else "卖",
                level_index + 1,
                BinanceClient._format_decimal(amount),
                BinanceClient._format_decimal(actual_price),
                order_id,
            )
        return submitted_anchors

    def _ladder_reconcile_order_states(self, active_orders: dict[str, dict[str, object]]) -> None:
        for order_key, state in list(active_orders.items()):
            side_u = str(state.get("side") or "").strip().upper()
            order_id = state.get("order_id")
            order = self.c.get_order(self.spot_symbol, order_id)
            status = str(order.get("status") or "").strip().upper()
            state["price"] = self._order_price_decimal(order, Decimal(str(state.get("price", "0") or "0")))
            if status == "FILLED":
                self._journal_order_state(order_id, side_u, "completed", exchange_status=status)
                active_orders.pop(order_key, None)
                logger.info("分仓挂单子订单已成交：%s #%s", side_u, order_id)
            elif self._order_terminal_status(status):
                self._journal_order_state(order_id, side_u, "cancelled", exchange_status=status)
                active_orders.pop(order_key, None)
                logger.info("分仓挂单子订单已结束：%s #%s 状态=%s", side_u, order_id, status)

    def _ladder_cancel_order_state(
        self,
        state: dict[str, object],
        *,
        reason: str,
    ) -> tuple[dict, str]:
        side_u = str(state.get("side") or "").strip().upper()
        order_id = state.get("order_id")
        latest_order = self._cancel_limit_order_with_fill_guard(
            order_id,
            side_u,
            cleanup_after_stop=reason == "stop",
        )
        latest_status = str(latest_order.get("status") or "").strip().upper()
        if not self._order_terminal_status(latest_status):
            raise RuntimeError(f"订单 #{order_id} 撤销后状态未确认：{latest_status or '--'}")
        if latest_status == "FILLED":
            self._journal_order_state(order_id, side_u, "completed", exchange_status=latest_status)
        else:
            self._journal_order_state(
                order_id,
                side_u,
                "cancelled",
                reason=reason,
                exchange_status=latest_status,
            )
        return latest_order, latest_status

    def _ladder_cancel_orders_outside_targets(
        self,
        active_orders: dict[str, dict[str, object]],
        pending_allocations: list[dict[str, object]],
        side: str,
        allowed_prices: list[Decimal],
        *,
        reason: str,
    ) -> None:
        side_u = str(side or "").strip().upper()
        target_prices = [Decimal(str(price)) for price in allowed_prices]
        allowed = set(target_prices)
        occupied_levels = {
            level_index
            for level_index, target_price in enumerate(target_prices)
            if any(
                str(state.get("side") or "").strip().upper() == side_u
                and Decimal(str(state.get("price", "0") or "0")) == target_price
                for state in active_orders.values()
            )
            or any(
                str(item.get("side") or "").strip().upper() == side_u
                and int(item.get("level_index", -1) or 0) == level_index
                for item in pending_allocations
            )
        }
        for order_key, state in list(active_orders.items()):
            if str(state.get("side") or "").strip().upper() != side_u:
                continue
            order_price = Decimal(str(state.get("price", "0") or "0"))
            if order_price in allowed:
                continue
            order_id = state.get("order_id")
            latest_order, latest_status = self._ladder_cancel_order_state(state, reason=reason)
            active_orders.pop(order_key, None)

            missing_level = next(
                (level_index for level_index in range(len(target_prices)) if level_index not in occupied_levels),
                None,
            )
            if latest_status != "FILLED" and missing_level is not None:
                remaining_qty = max(
                    Decimal("0"),
                    Decimal(str(latest_order.get("origQty", "0") or "0"))
                    - Decimal(str(latest_order.get("executedQty", "0") or "0")),
                )
                released_amount = (
                    remaining_qty * self._order_price_decimal(latest_order, order_price)
                    if side_u == "BUY"
                    else remaining_qty
                )
                released_notional = (
                    released_amount
                    if side_u == "BUY"
                    else released_amount * target_prices[missing_level]
                )
                if released_amount > 0 and released_notional >= self._ladder_min_pool_value():
                    pending_allocations.append(
                        {
                            "side": side_u,
                            "level_index": missing_level,
                            "amount": released_amount,
                            "source": "reprice",
                        }
                    )
                    occupied_levels.add(missing_level)
            logger.info(
                "分仓挂单撤销移出目标档位的%s单：旧价=%s，订单#%s，状态=%s",
                "买" if side_u == "BUY" else "卖",
                BinanceClient._format_decimal(order_price),
                order_id,
                latest_status or "--",
            )

    def _ladder_cancel_all_orders(self, active_orders: dict[str, dict[str, object]], *, reason: str) -> bool:
        all_cancelled = True
        for order_key, state in list(active_orders.items()):
            side_u = str(state.get("side") or "").strip().upper()
            order_id = state.get("order_id")
            try:
                self._ladder_cancel_order_state(state, reason=reason)
                active_orders.pop(order_key, None)
            except Exception as exc:
                all_cancelled = False
                logger.warning("分仓挂单停止时撤销订单失败 %s #%s: %s", side_u, order_id, exc)
        return all_cancelled

    def _ladder_cleanup_after_manual_stop(
        self,
        active_orders: dict[str, dict[str, object]],
        base_asset: str,
    ) -> None:
        cleanup_context = getattr(self.c, "cleanup_requests_after_stop", None)
        context = cleanup_context() if callable(cleanup_context) else nullcontext()
        with context:
            all_cancelled = self._ladder_cancel_all_orders(active_orders, reason="stop")
            try:
                self._ladder_sell_all_base_and_confirm(base_asset, reason="手动停止分仓挂单后")
            except Exception as exc:
                self._set_ladder_cleanup_failure(f"市价清仓未确认：{exc}")
        if not all_cancelled:
            self._set_ladder_cleanup_failure("部分分仓挂单撤销未确认")
        elif not self.ladder_cleanup_error:
            self.ladder_cleanup_confirmed = True
            logger.info("检测到停止信号，已确认撤销全部分仓挂单并完成清仓")

    def _set_ladder_cleanup_failure(self, reason: object) -> None:
        reason_text = str(reason or "").strip() or "未知原因"
        self.ladder_cleanup_confirmed = False
        self.ladder_cleanup_error = reason_text
        self.trade_error = reason_text
        self.trade_error_stage = "分仓挂单结束清理"
        logger.error("分仓挂单结束清理失败：%s", reason_text)

    def _ladder_sell_all_base_and_confirm(self, base_asset: str, *, reason: str) -> None:
        sold = self.c.spot_sell_all_base(self.spot_symbol)
        sellable_quantity = getattr(self.c, "spot_market_sellable_base_quantity", None)
        if not callable(sellable_quantity):
            if sold:
                logger.info("%s已市价卖出全部 %s", reason, base_asset)
                return
            logger.info("%s无可市价卖出的 %s", reason, base_asset)
            return

        deadline = time.monotonic() + max(0.0, float(self.LADDER_CLEANUP_CONFIRM_TIMEOUT_SECONDS))
        while True:
            remaining = Decimal(str(sellable_quantity(self.spot_symbol)))
            if remaining <= 0:
                logger.info("%s已确认 %s 无可交易余额", reason, base_asset)
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "确认超时，仍有可交易 %s 数量 %s"
                    % (base_asset, BinanceClient._format_decimal(remaining))
                )
            time.sleep(max(0.0, float(self.LADDER_CLEANUP_CONFIRM_POLL_INTERVAL_SECONDS)))

    def _auto_ladder_cancel_orders(self, active_orders: dict[str, dict[str, object]], *, reason: str) -> bool:
        """Unlike legacy cleanup, never assume a cancel succeeded without a final query."""
        confirmed = True
        for key, state in list(active_orders.items()):
            order_id = state["order_id"]
            side = str(state["side"])
            try:
                self._cancel_limit_order_with_fill_guard(order_id, side)
                final = self.c.get_order(self.spot_symbol, order_id)
                status = str(final.get("status") or "").upper()
                if not self._order_terminal_status(status):
                    raise RuntimeError(f"订单#{order_id}撤销未确认：{status}")
                self._journal_order_state(
                    order_id, side, "completed" if status == "FILLED" else "cancelled",
                    reason=reason, exchange_status=status,
                )
                active_orders.pop(key, None)
            except Exception as exc:
                confirmed = False
                logger.error("自动分仓撤单未确认 %s #%s：%s", side, order_id, exc)
        return confirmed

    def _run_ladder_mode(self, stop_event, progress_cb=None):
        mode_name = self._mode_name()
        base_asset, quote_asset = self._spot_pair_assets()
        self._ladder_validate_pool_threshold()
        min_pool = self._ladder_min_pool_value()
        buy_levels, sell_levels = self._ladder_layout_level_counts(self.ladder_layout)
        active_orders: dict[str, dict[str, object]] = {}
        pending_allocations: list[dict[str, object]] = []
        bid_anchor: Decimal | None = None
        ask_anchor: Decimal | None = None
        buy_reprice_pending = False
        sell_reprice_pending = False
        last_reconcile_at = time.monotonic()
        loop_count = 0
        automatic = mode_name == TRADE_MODE_AUTO_LADDER
        auto_store = (self.auto_ladder_settings or AutoLadderSettingsStore()) if automatic else None
        observer = (
            self.auto_ladder_observer or AutoLadderMarketObserver(self.c, self.spot_symbol, auto_store)
        ) if automatic else None
        local_observer = automatic and self.auto_ladder_observer is None
        auto_state = "OBSERVE" if automatic else "ACTIVE"
        paused_reason = ""
        stable_since: float | None = None

        self.c.collect_asset_to_spot(quote_asset, context="交易前")
        sell_price_text = "卖1"
        if self.ladder_sell_premium_enabled:
            sell_price_text = "后续买1+%s（初始持币仍按卖1）" % BinanceClient._format_decimal(
                self._ladder_sell_premium_value()
            )
        logger.info(
            "启用分仓挂单：档位模式=%s，买档=%d，卖档=%d，最小分仓成交额=%s %s，卖价=%s，轮询=%g秒，对账=%g秒",
            self.ladder_layout,
            buy_levels,
            sell_levels,
            BinanceClient._format_decimal(min_pool),
            quote_asset,
            sell_price_text,
            self.LADDER_POLL_INTERVAL_SECONDS,
            self.LADDER_ORDER_RECONCILE_INTERVAL_SECONDS,
        )

        while True:
            if stop_event and stop_event.is_set():
                self._ladder_cleanup_after_manual_stop(active_orders, base_asset)
                return

            try:
                loop_count += 1
                if automatic:
                    if local_observer:
                        observer.poll_once()
                    market = observer.snapshot()
                    settings, version = auto_store.snapshot()
                    market_available = market is not None and market.settings_version == version
                    current_bid = market.bid if market_available else Decimal("0")
                    current_ask = market.ask if market_available else Decimal("0")
                else:
                    book_ticker = self.c.get_book_ticker(self.spot_symbol)
                    current_bid = Decimal(str(book_ticker["bidPrice"]))
                    current_ask = Decimal(str(book_ticker["askPrice"]))
                now = time.monotonic()
                if automatic:
                    market_ready = market_available and market.market_ready
                    market_dry = market_available and market.market_dry
                    price_safe = market_available and market.price_safe
                    choppy = market_available and market.choppy
                    no_trade = market_available and not trade_is_recent(
                        market.last_trade_ms, int(time.time() * 1000), settings.no_trade_seconds,
                    )
                    turns = market.turns if market_available else 0
                    if auto_state == "ACTIVE" and (not market_available or no_trade or market_dry or choppy or not price_safe):
                        paused_reason = (
                            "行情不可用" if not market_available else
                            "超过无成交暂停时间" if no_trade else
                            "快速反复波动" if choppy else
                            "价格超出安全范围" if not price_safe else "成交额缩量"
                        )
                        auto_state = "PAUSED"
                        stable_since = None
                        pending_allocations.clear()
                        bid_anchor = ask_anchor = None
                        buy_reprice_pending = sell_reprice_pending = False
                        logger.warning("自动分仓暂停挂单：%s（折返=%d）", paused_reason, turns)
                    if auto_state == "PAUSED" and active_orders:
                        if not self._auto_ladder_cancel_orders(active_orders, reason="auto-pause"):
                            self._pause_with_stop(stop_event, self.LADDER_POLL_INTERVAL_SECONDS)
                            continue
                    if auto_state != "ACTIVE":
                        eligible = market_available and market_ready and price_safe and not choppy and not no_trade
                        if eligible:
                            if stable_since is None:
                                stable_since = now
                            if (
                                (auto_state == "OBSERVE" and market.entry_ready)
                                or (auto_state == "PAUSED" and now - stable_since >= settings.stable_seconds)
                            ):
                                initial_entry = auto_state == "OBSERVE"
                                auto_state = "ACTIVE"
                                bid_anchor = current_bid
                                ask_anchor = current_ask
                                if initial_entry:
                                    loop_count = 0  # 首次入场仍按旧分仓的初始持仓卖价
                                logger.info("自动分仓行情达标，开始按当前盘口挂单")
                        else:
                            stable_since = None
                        if auto_state != "ACTIVE":
                            if progress_cb and loop_count % 7 == 0:
                                progress_cb(0, 1, f"自动分仓观察中：{paused_reason or '等待行情达标'}")
                            self._pause_with_stop(stop_event, self.LADDER_POLL_INTERVAL_SECONDS)
                            continue

                if (now - last_reconcile_at) >= self.LADDER_ORDER_RECONCILE_INTERVAL_SECONDS:
                    self._ladder_reconcile_order_states(active_orders)
                    last_reconcile_at = now

                has_active_buy = any(str(state.get("side") or "").upper() == "BUY" for state in active_orders.values())
                has_active_sell = any(str(state.get("side") or "").upper() == "SELL" for state in active_orders.values())
                if bid_anchor is None or not has_active_buy:
                    bid_anchor = current_bid
                if ask_anchor is None or not has_active_sell:
                    ask_anchor = current_ask

                reprice_threshold = self._reprice_threshold_value()
                buy_reprice_from: Decimal | None = None
                sell_reprice_from: Decimal | None = None
                if has_active_buy and reprice_threshold > 0 and current_bid >= bid_anchor + reprice_threshold:
                    buy_reprice_from = bid_anchor
                    bid_anchor = current_bid
                    buy_reprice_pending = True

                if has_active_sell and reprice_threshold > 0 and current_ask <= ask_anchor - reprice_threshold:
                    sell_reprice_from = ask_anchor
                    ask_anchor = current_ask
                    sell_reprice_pending = True

                premium_sell_pricing = bool(self.ladder_sell_premium_enabled and loop_count > 1)
                target_prices = self._ladder_target_prices(
                    bid_anchor,
                    ask_anchor,
                    sell_bid_anchor=(current_bid if premium_sell_pricing else None),
                )
                if buy_reprice_from is not None:
                    logger.info(
                        "分仓挂单买侧触发重挂：旧买1=%s，新买1=%s，目标=%s",
                        BinanceClient._format_decimal(buy_reprice_from),
                        BinanceClient._format_decimal(bid_anchor),
                        "/".join(BinanceClient._format_decimal(price) for price in target_prices["BUY"]),
                    )
                if sell_reprice_from is not None:
                    logger.info(
                        "分仓挂单卖侧触发重挂：旧卖1=%s，新卖1=%s，目标=%s",
                        BinanceClient._format_decimal(sell_reprice_from),
                        BinanceClient._format_decimal(ask_anchor),
                        "/".join(BinanceClient._format_decimal(price) for price in target_prices["SELL"]),
                    )
                if buy_reprice_pending:
                    self._ladder_cancel_orders_outside_targets(
                        active_orders,
                        pending_allocations,
                        "BUY",
                        target_prices["BUY"],
                        reason="target-reconcile",
                    )
                    buy_reprice_pending = False
                if sell_reprice_pending:
                    self._ladder_cancel_orders_outside_targets(
                        active_orders,
                        pending_allocations,
                        "SELL",
                        target_prices["SELL"],
                        reason="target-reconcile",
                    )
                    sell_reprice_pending = False
                snapshot = self.c.spot_balance_snapshot({base_asset, quote_asset, "BNB"})
                current_bnb = Decimal(str((snapshot.get("BNB") or {}).get("free", "0") or "0"))
                bnb_stop = max(Decimal("0"), Decimal(str(self.bnb_fee_stop_value or "0")))
                if current_bnb < bnb_stop:
                    logger.info(
                        "%s模式停止：当前 BNB 手续费余额 %s 低于停止值 %s",
                        mode_name,
                        BinanceClient._format_decimal(current_bnb),
                        BinanceClient._format_decimal(bnb_stop),
                    )
                    all_cancelled = self._ladder_cancel_all_orders(active_orders, reason="bnb-fee-stop")
                    try:
                        self._ladder_sell_all_base_and_confirm(
                            base_asset,
                            reason="分仓挂单BNB手续费不足停止后",
                        )
                    except Exception as exc:
                        self._set_ladder_cleanup_failure(f"市价清仓未确认：{exc}")
                    if not all_cancelled:
                        self._set_ladder_cleanup_failure("部分分仓挂单撤销未确认")
                    elif not self.ladder_cleanup_error:
                        self.ladder_cleanup_confirmed = True
                        logger.info("分仓挂单BNB手续费不足停止后，已确认撤销全部挂单并完成清仓")
                    return

                quote_free = Decimal(str((snapshot.get(quote_asset) or {}).get("free", "0") or "0"))
                base_free = Decimal(str((snapshot.get(base_asset) or {}).get("free", "0") or "0"))
                if base_asset == "BNB" and bnb_stop > 0:
                    base_free = max(Decimal("0"), base_free - bnb_stop)
                reserved_quote = self._ladder_pending_amount(pending_allocations, "BUY")
                reserved_base = self._ladder_pending_amount(pending_allocations, "SELL")
                available_quote = max(Decimal("0"), quote_free - reserved_quote)
                available_base = max(Decimal("0"), base_free - reserved_base)

                if available_quote >= min_pool:
                    self._ladder_enqueue_allocations(pending_allocations, "BUY", available_quote)
                    logger.info(
                        "待买池达到阈值：可用=%s %s，按%d档分配",
                        BinanceClient._format_decimal(available_quote),
                        quote_asset,
                        buy_levels,
                    )
                available_base_notional = available_base * current_ask
                if available_base > 0 and available_base_notional >= min_pool:
                    self._ladder_enqueue_allocations(pending_allocations, "SELL", available_base)
                    logger.info(
                        "待卖池达到阈值：可用=%s %s（约%s %s），按%d档分配",
                        BinanceClient._format_decimal(available_base),
                        base_asset,
                        BinanceClient._format_decimal(available_base_notional),
                        quote_asset,
                        sell_levels,
                    )

                def auto_market_guard(bid: Decimal, ask: Decimal) -> bool:
                    latest = observer.snapshot()
                    if latest is None:
                        raise _AutoLadderMarketPause("提交前公共行情已过期")
                    if latest.settings_version != version or auto_store.snapshot()[1] != version:
                        raise _AutoLadderMarketPause("提交前设置已更新，等待重新观察")
                    if latest.choppy:
                        raise _AutoLadderMarketPause("提交前价格反复折返")
                    if latest.market_dry:
                        raise _AutoLadderMarketPause("提交前成交额已缩量")
                    if not trade_is_recent(latest.last_trade_ms, int(time.time() * 1000), settings.no_trade_seconds):
                        raise _AutoLadderMarketPause("提交前已达到无成交暂停时间")
                    if not latest.price_safe:
                        raise _AutoLadderMarketPause("提交前公共行情价格尚未恢复稳定")
                    reason = price_risk_reason(latest.rows, settings, latest.tick, bid, ask)
                    if reason:
                        raise _AutoLadderMarketPause(f"提交前盘口价格超出安全范围：{reason}")
                    return True

                submitted_anchors = self._ladder_submit_pending_allocations(
                    pending_allocations,
                    active_orders,
                    target_prices,
                    sell_from_bid=premium_sell_pricing,
                    market_guard=auto_market_guard if automatic else None,
                )
                submitted_bid = submitted_anchors.get("BUY")
                if submitted_bid is not None:
                    bid_anchor = min(bid_anchor, submitted_bid)
                submitted_ask = submitted_anchors.get("SELL")
                if submitted_ask is not None:
                    ask_anchor = max(ask_anchor, submitted_ask)

                if progress_cb:
                    progress_cb(
                        loop_count,
                        max(loop_count, 1),
                        f"分仓挂单 活动订单{len(active_orders)} 待提交{len(pending_allocations)}",
                    )

                if not active_orders and not pending_allocations and not automatic:
                    logger.info(
                        "分仓挂单无活动订单，且可用余额均未达到最小分仓成交额 %s %s，结束本次运行",
                        BinanceClient._format_decimal(min_pool),
                        quote_asset,
                    )
                    return
            except Exception as exc:
                if isinstance(exc, UnconfirmedOrderError):
                    if self.operation_journal is not None:
                        entry_id = self.operation_journal.begin(
                            "exchange-order", self.operation_scope,
                            {"symbol": exc.symbol, "client_order_id": exc.client_order_id},
                        )
                        self.operation_journal.update(entry_id, "open")
                    self.trade_error = str(exc)
                    self.trade_error_stage = "自动分仓订单状态"
                    logger.error("自动分仓停止：%s", exc)
                    self._auto_ladder_cancel_orders(active_orders, reason="unconfirmed-order")
                    return
                if isinstance(exc, RetryAborted) or (stop_event and stop_event.is_set()):
                    self._ladder_cleanup_after_manual_stop(active_orders, base_asset)
                    logger.info("检测到停止信号，已停止后续执行（%s模式）", mode_name)
                    return
                if isinstance(exc, _AutoLadderMarketPause):
                    logger.warning("自动分仓暂停挂单：%s", exc)
                else:
                    logger.error("分仓挂单轮询异常: %s", exc)
                if automatic:
                    auto_state = "PAUSED"
                    paused_reason = str(exc) if isinstance(exc, _AutoLadderMarketPause) else "轮询异常"
                    stable_since = None
                    pending_allocations.clear()
                    if not self._auto_ladder_cancel_orders(active_orders, reason="auto-error"):
                        logger.error("自动分仓仍有未确认订单，保持暂停并重试撤单")

            if self._pause_with_stop(stop_event, self.LADDER_POLL_INTERVAL_SECONDS):
                continue

    def _run_market_mode(self, stop_event, progress_cb=None):
        self._reset_market_round_tracking()
        bnb_stop_mode = self._is_market_bnb_stop_mode()
        total_steps = 1 if bnb_stop_mode else (self.spot_rounds if self.spot_rounds > 0 else 1)
        step = 0
        base_asset, quote_asset = self._spot_pair_assets()
        stagger_coordinator = self.market_stagger_coordinator
        stagger_lane_id = self.market_stagger_lane_id

        def run_market_action(side: str, context: str, action):
            run_with_retry = lambda: self._run_trade_action_with_retry(stop_event, context, action)
            if stagger_coordinator is None or stagger_lane_id <= 0:
                return run_with_retry()
            return stagger_coordinator.execute(
                stagger_lane_id,
                side,
                run_with_retry,
                stop_event=stop_event,
            )

        def complete_stagger_side_without_order(side: str) -> None:
            if stagger_coordinator is None or stagger_lane_id <= 0:
                return
            stagger_coordinator.execute(
                stagger_lane_id,
                side,
                lambda: False,
                stop_event=stop_event,
            )

        self.ensure_base_sold()
        try:
            self._run_trade_action_with_retry(
                stop_event,
                "现货轮开始前归集 %s 到现货" % quote_asset,
                lambda: self.c.collect_asset_to_spot(quote_asset, context="交易前"),
            )
        except Exception as e:
            if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                logger.info("检测到停止信号，停止后续执行（现货阶段）")
                return
            logger.error("现货轮开始前归集 %s 到现货失败：%s；后续轮次将仅使用现货账户已有余额", quote_asset, e)

        if self.market_initial_stagger and stagger_coordinator is None:
            logger.info("市价交错执行：首次买入前等待一个随机延迟周期")
            self._emit_progress(progress_cb, 0, total_steps, "市价交错等待首次买入")
            self.sleep_fn()
            if stop_event and stop_event.is_set():
                logger.info("市价交错等待被停止，停止后续执行（现货阶段）")
                return

        i = 0
        while bnb_stop_mode or i < self.spot_rounds:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（现货阶段）")
                return

            self.ensure_base_sold()
            if bnb_stop_mode and self._should_stop_for_bnb_fee():
                break

            round_number = i + 1
            round_label = str(round_number) if bnb_stop_mode else "%d/%d" % (round_number, self.spot_rounds)
            total_steps = round_number if bnb_stop_mode else total_steps
            stop_after_round = False
            round_stage = "BUY"
            logger.info("--- 现货轮 %s 开始 ---", round_label)

            try:
                self._emit_progress(
                    progress_cb,
                    step,
                    total_steps,
                    "现货轮 %s 买入 %s" % (round_label, base_asset),
                )
                buy_success = run_market_action(
                    "BUY",
                    "现货轮 %s 买入 %s" % (round_label, base_asset),
                    lambda: self.c.spot_buy_all_usdt(symbol=self.spot_symbol, assume_spot_funds_ready=True),
                )
                if buy_success:
                    self._emit_progress(
                        progress_cb,
                        step,
                        total_steps,
                        "现货轮 %s 等待卖出 %s" % (round_label, base_asset),
                    )
                    self.sleep_fn()
                    self._emit_progress(
                        progress_cb,
                        step,
                        total_steps,
                        "现货轮 %s 卖出 %s" % (round_label, base_asset),
                    )
                    round_stage = "SELL"
                    run_market_action(
                        "SELL",
                        "现货轮 %s 卖出 %s" % (round_label, base_asset),
                        lambda: self.c.spot_sell_all_base(symbol=self.spot_symbol),
                    )
                else:
                    logger.info("买入未执行（可能余额不足），跳过本轮卖出")
                    complete_stagger_side_without_order("SELL")
                    stop_after_round = bnb_stop_mode

                logger.info("--- 现货轮 %d 完成 ---", round_number)

            except Exception as e:
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    logger.info("检测到停止信号，停止后续执行（现货阶段）")
                    return
                self._record_market_round_failure(round_number, e)
                logger.error(f"现货轮 %d 执行异常: {e}", round_number)
                if round_stage == "BUY":
                    try:
                        complete_stagger_side_without_order("SELL")
                    except Exception as stagger_exc:
                        if isinstance(stagger_exc, RetryAborted) or (stop_event and stop_event.is_set()):
                            logger.info("检测到停止信号，停止后续执行（现货阶段）")
                            return
                        logger.error("现货轮 %d 交错卖出占位异常: %s", round_number, stagger_exc)
                stop_after_round = bnb_stop_mode

            step += 1
            self.market_round_completed = step
            if bnb_stop_mode:
                self.market_round_total = step
            failure_suffix = ""
            if self.market_round_failed_rounds:
                failure_suffix = "，失败%d轮" % len(self.market_round_failed_rounds)
            progress_text = (
                "现货轮%s%d完成%s" % (base_asset.upper(), round_number, failure_suffix)
                if bnb_stop_mode
                else "现货轮%s%d/%d完成%s"
                % (base_asset.upper(), round_number, self.spot_rounds, failure_suffix)
            )
            self._emit_progress(
                progress_cb,
                step,
                step if bnb_stop_mode else total_steps,
                progress_text,
            )
            i += 1
            if stop_after_round:
                break
            self.sleep_fn()

        self.ensure_base_sold()

    def _run_convert_mode(self, stop_event, progress_cb=None):
        total_steps = self.spot_rounds if self.spot_rounds > 0 else 1
        step = 0
        base_asset, quote_asset = self._spot_pair_assets()

        self.ensure_base_sold()

        for i in range(self.spot_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（闪兑阶段）")
                return

            self.ensure_base_sold()
            logger.info("--- 闪兑轮 %d/%d 开始 ---", i + 1, self.spot_rounds)

            try:
                self._emit_progress(
                    progress_cb,
                    step,
                    total_steps,
                    "闪兑轮 %d/%d %s->%s" % (i + 1, self.spot_rounds, quote_asset, base_asset),
                )
                buy_result = self._run_trade_action_with_retry(
                    stop_event,
                    "闪兑轮 %d/%d %s->%s" % (i + 1, self.spot_rounds, quote_asset, base_asset),
                    lambda: self.c.convert_quote_to_base_all(symbol=self.spot_symbol),
                )
                if buy_result:
                    self._emit_progress(
                        progress_cb,
                        step,
                        total_steps,
                        "闪兑轮 %d/%d 等待 %s->%s" % (i + 1, self.spot_rounds, base_asset, quote_asset),
                    )
                    self.sleep_fn()
                    self._emit_progress(
                        progress_cb,
                        step,
                        total_steps,
                        "闪兑轮 %d/%d %s->%s" % (i + 1, self.spot_rounds, base_asset, quote_asset),
                    )
                    sell_result = self._run_trade_action_with_retry(
                        stop_event,
                        "闪兑轮 %d/%d %s->%s" % (i + 1, self.spot_rounds, base_asset, quote_asset),
                        lambda: self.c.convert_base_to_quote_all(symbol=self.spot_symbol),
                    )
                    if not sell_result:
                        logger.info("闪兑卖出未执行（可能余额不足或不满足最小闪兑金额），结束本次运行")
                        return
                else:
                    logger.info("闪兑换入未执行（可能余额不足或不满足最小闪兑金额），结束本次运行")
                    return

                logger.info("--- 闪兑轮 %d 完成 ---", i + 1)
            except Exception as e:
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    logger.info("检测到停止信号，停止后续执行（闪兑阶段）")
                    return
                logger.error(f"闪兑轮 %d 执行异常: {e}", i + 1)

            step += 1
            self._emit_progress(progress_cb, step, total_steps, "闪兑轮 %d/%d 完成" % (i + 1, self.spot_rounds))
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
                book_ticker = self._run_trade_action_with_retry(
                    stop_event,
                    f"{mode_name}轮 {step + 1} 读取盘口",
                    lambda: self.c.get_book_ticker(self.spot_symbol),
                )
                buy_order, sell_order = self._run_trade_action_with_retry(
                    stop_event,
                    f"{mode_name}轮 {step + 1} 挂单",
                    lambda: self._place_limit_orders(
                        mode_name,
                        book_ticker,
                        progress_cb=progress_cb,
                        step=step,
                    ),
                )
                if not buy_order and not sell_order:
                    logger.info("%s模式当前无可挂出的买卖单（余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                action, payload = self._run_trade_action_with_retry(
                    stop_event,
                    f"{mode_name}轮 {step + 1} 监控订单",
                    lambda: self._monitor_limit_orders(
                        stop_event,
                        mode_name,
                        buy_order=buy_order,
                        sell_order=sell_order,
                    ),
                )
                if action == "filled":
                    step += 1
                    filled_summary = self._limit_fill_summary(list(payload.get("filled_orders") or []))
                    logger.info("--- %s轮 %d 完成：%s ---", mode_name, step, filled_summary)
                    self._emit_progress(progress_cb, step, max(step, 1), f"{mode_name}轮 {step} 完成")
                    self.sleep_fn()
                    continue
                if action == "reprice":
                    continue

                logger.info("%s模式当前无活动挂单，结束本次运行", mode_name)
                return
            except Exception as e:
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)

    def _run_premium_mode(self, stop_event, progress_cb=None):
        if self._premium_partial_fill_mode_enabled():
            self._run_premium_partial_fill_mode(stop_event, progress_cb=progress_cb)
            return
        if self._premium_append_mode_enabled():
            self._run_premium_append_mode(stop_event, progress_cb=progress_cb)
            return

        step = 0
        mode_name = self._mode_name()
        base_asset, _quote_asset = self._spot_pair_assets()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                return
            if self._should_stop_for_bnb_fee():
                return

            step += 1
            logger.info("--- %s轮 %d 开始 ---", mode_name, step)
            try:
                self._emit_progress(
                    progress_cb,
                    max(step - 1, 0),
                    max(step, 1),
                    f"{mode_name}轮 {step} 买入 {base_asset}",
                )
                buy_result, _ask_price = self._run_trade_action_with_retry(
                    stop_event,
                    f"{mode_name}轮 {step} 买入 {base_asset}",
                    lambda: self._place_buy_order_with_reprice(stop_event, mode_name),
                )
                buy_order = buy_result
                if not buy_order:
                    logger.info("%s模式买入未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                buy_fill_price = self.c.get_order_average_price(buy_order) or Decimal("0")
                if buy_fill_price <= 0:
                    buy_fill_price = Decimal(str(self._run_trade_action_with_retry(
                        stop_event,
                        f"{mode_name}轮 {step} 读取买入均价兜底盘口",
                        lambda: self.c.get_book_ticker(self.spot_symbol),
                    )["bidPrice"]))

                self._emit_progress(
                    progress_cb,
                    max(step - 1, 0),
                    max(step, 1),
                    f"{mode_name}轮 {step} 等待卖出 {base_asset}",
                )
                self.sleep_fn()

                self._emit_progress(
                    progress_cb,
                    max(step - 1, 0),
                    max(step, 1),
                    f"{mode_name}轮 {step} 卖出 {base_asset}",
                )
                sell_order = self._run_trade_action_with_retry(
                    stop_event,
                    f"{mode_name}轮 {step} 卖出 {base_asset}",
                    lambda: self._place_sell_order_with_reprice(stop_event, mode_name, buy_fill_price),
                )
                if not sell_order:
                    logger.info("%s模式卖出未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                logger.info("--- %s轮 %d 完成 ---", mode_name, step)
            except Exception as e:
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)

            self._emit_progress(progress_cb, step, max(step, 1), f"{mode_name}轮 {step} 完成")
            self.sleep_fn()

    def _run_limit_like_mode(self, stop_event, progress_cb=None):
        if self._mode_name() == TRADE_MODE_LIMIT:
            self._run_limit_mode(stop_event, progress_cb=progress_cb)
            return
        if self._mode_name() in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
            self._run_ladder_mode(stop_event, progress_cb=progress_cb)
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
        clear_fn = getattr(self.c, "ensure_um_futures_asset_clear_before_trade", None)
        if callable(clear_fn):
            clear_fn(margin_asset)
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
        try:
            self._ensure_futures_available_balance()
        except Exception as e:
            if "合约有问题" in str(e):
                self.trade_error = str(e)
                self.trade_error_stage = "合约有问题"
                logger.error("【合约有问题】%s", e)
                if progress_cb:
                    progress_cb(1, total_steps, "合约有问题")
                return
            raise
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
                qty = self._run_trade_action_with_retry(
                    stop_event,
                    "合约轮 %d/%d 开仓前预检" % (i + 1, self.futures_rounds),
                    lambda: self.c.calculate_um_futures_order_quantity(
                        self.futures_symbol,
                        self.futures_amount,
                        open_side,
                    ),
                )
                stage_label = "开仓下单"
                open_order = self._run_trade_action_with_retry(
                    stop_event,
                    "合约轮 %d/%d 开仓下单" % (i + 1, self.futures_rounds),
                    lambda: self.c.place_um_futures_market_order(
                        self.futures_symbol,
                        open_side,
                        qty,
                    ),
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
                close_order = self._run_trade_action_with_retry(
                    stop_event,
                    "合约轮 %d/%d 平仓下单" % (i + 1, self.futures_rounds),
                    lambda: self.c.close_um_futures_position_market(self.futures_symbol),
                )
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
                if isinstance(e, RetryAborted) or (stop_event and stop_event.is_set()):
                    logger.info("检测到停止信号，停止后续执行（合约阶段）")
                    return
                self.trade_error = str(e)
                self.trade_error_stage = stage_label
                logger.error("【%s失败】合约轮 %d：%s", stage_label, i + 1, e)
                if progress_cb:
                    if stage_label == "开仓下单":
                        progress_cb(i + 1, total_steps, "开仓失败")
                    elif stage_label == "平仓下单":
                        progress_cb(i + 1, total_steps, "平仓失败")
                    elif "预检" in stage_label:
                        progress_cb(i + 1, total_steps, "合约预检失败")
                    else:
                        progress_cb(i + 1, total_steps, "合约失败")
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
        self.trade_error = ""
        self.trade_error_stage = ""
        self.ladder_cleanup_confirmed = False
        self.ladder_cleanup_error = ""

        def result_payload():
            return {
                "withdraw_amount": withdraw_amount,
                "withdraw_error": withdraw_error,
                "withdraw_attempted": withdraw_attempted,
                "trade_error": self.trade_error,
                "trade_error_stage": self.trade_error_stage,
                "trade_failed": bool(self.trade_error),
                "market_round_total": self.market_round_total,
                "market_round_completed": self.market_round_completed,
                "market_round_failed_count": len(self.market_round_failed_rounds),
                "market_round_failed_rounds": list(self.market_round_failed_rounds),
                "market_round_failure_details": list(self.market_round_failure_details),
                "ladder_cleanup_confirmed": self.ladder_cleanup_confirmed,
                "ladder_cleanup_error": self.ladder_cleanup_error,
            }

        zero_round_spot_conversion = self._is_zero_round_spot_conversion()

        if zero_round_spot_conversion:
            self._run_zero_round_spot_conversion(stop_event, progress_cb=progress_cb)
            if self.trade_error:
                logger.error("0 轮现货兑换失败，已中止后续提现: %s", self.trade_error)
                return result_payload()
        else:
            self._run_bnb_topup_if_needed(stop_event)

        if not zero_round_spot_conversion:
            if self._is_futures_mode():
                self._run_futures_mode(stop_event, progress_cb=progress_cb)
            elif self._is_convert_mode():
                self._run_convert_mode(stop_event, progress_cb=progress_cb)
            elif self._mode_name() == TRADE_MODE_MARKET:
                try:
                    self._run_market_mode(stop_event, progress_cb=progress_cb)
                finally:
                    if self.market_stagger_coordinator is not None and self.market_stagger_lane_id > 0:
                        self.market_stagger_coordinator.suspend(self.market_stagger_lane_id)
            else:
                self._run_limit_like_mode(stop_event, progress_cb=progress_cb)

        if self._mode_name() == TRADE_MODE_AUTO_LADDER and self.trade_error:
            logger.error("自动分仓策略失败，跳过最终提现：%s", self.trade_error)
            return result_payload()

        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过最终提现")
            return result_payload()

        if not str(self.withdraw_address or "").strip():
            logger.info("未填写提现地址，跳过最终提现")
            return result_payload()

        if self.enable_withdraw:
            logger.info("最终提现前执行随机延迟")
            self.sleep_fn()
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，跳过最终提现")
                return result_payload()

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
        return result_payload()

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

        end_time = time.monotonic() + max(0.0, float(timeout))
        while True:
            if self.is_set():
                return True
            remaining = end_time - time.monotonic()
            if remaining <= 0:
                return self.is_set()
            step = min(0.2, remaining)
            if self._events:
                self._events[0].wait(step)
            else:
                time.sleep(step)
