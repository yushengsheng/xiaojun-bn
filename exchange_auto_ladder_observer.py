#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One public-market observer per automatic ladder run; account orders remain separate."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from decimal import Decimal

from exchange_auto_ladder import (
    AutoLadderSettingsStore, price_risk_reason, reversal_count, trade_is_recent,
    volume_and_price_signal,
)
from exchange_logging import logger


@dataclass(frozen=True)
class AutoLadderObservation:
    observed_at: float
    settings_version: int
    bid: Decimal
    ask: Decimal
    tick: Decimal
    rows: tuple[dict, ...]
    last_trade_ms: int
    market_ready: bool
    market_dry: bool
    price_safe: bool
    choppy: bool
    no_trade: bool
    entry_ready: bool
    turns: int


class AutoLadderMarketObserver:
    POLL_SECONDS = 1.5
    KLINE_REFRESH_SECONDS = 1.5
    MAX_SNAPSHOT_AGE_SECONDS = 5.0
    REPORT_SECONDS = 15.0
    ERROR_REPORT_SECONDS = 30.0

    def __init__(
        self, client, symbol: str, settings_store: AutoLadderSettingsStore,
        *, close_client_on_exit: bool = False,
    ):
        self.client = client
        self.symbol = symbol
        self.settings_store = settings_store
        self._close_client_on_exit = close_client_on_exit
        self._lock = threading.Lock()
        self._snapshot: AutoLadderObservation | None = None
        self._rows: list[dict] = []
        self._entry_rows: list[dict] = []
        self._tick: Decimal | None = None
        self._settings_version = -1
        self._last_kline_at = -float("inf")
        self._eligible_since: float | None = None
        self._last_success_at: float | None = None
        self._failed_since: float | None = None
        self._mid_samples: list[tuple[float, Decimal]] = []
        self._has_live_candle = False
        self._last_report_at = -float("inf")
        self._last_report_key: tuple[str, tuple[str, ...]] | None = None
        self._last_error: str | None = None
        self._last_error_report_at = -float("inf")
        self._shutdown = threading.Event()
        self._external_stop = None
        self._thread: threading.Thread | None = None

    def poll_once(self) -> None:
        """Update one shared snapshot; also used synchronously for a single account."""
        poll_started = time.monotonic()
        try:
            settings, version = self.settings_store.snapshot()
            if self._tick is None:
                self._tick = Decimal(str(self.client.get_symbol_tick_size(self.symbol) or "0"))
                if not self._tick.is_finite() or self._tick <= 0:
                    raise RuntimeError("自动分仓无法读取交易对价格精度")
            if version != self._settings_version:
                self._eligible_since = None
                self._settings_version = version
                self._last_kline_at = -float("inf")
            now = time.monotonic()
            if now - self._last_kline_at >= self.KLINE_REFRESH_SECONDS:
                live_reader = getattr(self.client, "get_minute_klines_with_live", None)
                if callable(live_reader):
                    self._rows, live = live_reader(self.symbol, settings.required_minutes)
                    self._entry_rows = [*self._rows[1:], live]
                    self._has_live_candle = True
                else:
                    self._rows = self.client.get_closed_minute_klines(self.symbol, settings.required_minutes)
                    self._entry_rows = self._rows
                    self._has_live_candle = False
                self._last_kline_at = time.monotonic()
            last_trade_ms = self.client.get_last_spot_trade_time_ms(self.symbol)
            fast_book = getattr(self.client, "get_observation_book_ticker", None)
            book = fast_book(self.symbol) if callable(fast_book) else self.client.get_book_ticker(self.symbol)
            bid = Decimal(str(book["bidPrice"]))
            ask = Decimal(str(book["askPrice"]))
            now = time.monotonic()
            if self._last_success_at is None:
                self._eligible_since = None
            elif self._eligible_since is not None:
                pause_since = self._failed_since
                if now - self._last_success_at > self.MAX_SNAPSHOT_AGE_SECONDS:
                    pause_since = min(pause_since, self._last_success_at) if pause_since is not None else self._last_success_at
                if pause_since is not None:
                    # Failed or stale observations pause confirmation without erasing progress.
                    self._eligible_since += now - pause_since
            now_ms = int(time.time() * 1000)
            if self._has_live_candle and int(self._entry_rows[-1]["open_time"]) != (now_ms // 60000) * 60000:
                self._last_kline_at = -float("inf")
                raise RuntimeError("当前分钟已切换，等待刷新分钟K线")
            if self.settings_store.snapshot()[1] != version:
                self._eligible_since = None
                with self._lock:
                    self._snapshot = None
                return
            market_ready, _ = volume_and_price_signal(self._entry_rows, settings, self._tick)
            _, historical_dry = volume_and_price_signal(self._rows, settings, self._tick)
            # Current-minute volume meeting the entry threshold is fresh evidence
            # that overrides an otherwise dry closed-minute window.
            market_dry = historical_dry and not market_ready
            price_reason = price_risk_reason(self._entry_rows, settings, self._tick, bid, ask)
            price_safe = not price_reason
            no_trade = not trade_is_recent(last_trade_ms, now_ms, settings.no_trade_seconds)
            self._mid_samples.append((now, (bid + ask) / Decimal("2")))
            self._mid_samples = [sample for sample in self._mid_samples if now - sample[0] <= 600]
            turns = reversal_count(
                self._mid_samples, now, settings.turn_seconds, self._tick * settings.turn_ticks,
            )
            choppy = turns >= settings.turn_count
            eligible = market_ready and price_safe and not choppy and not no_trade
            if eligible:
                if self._eligible_since is None:
                    self._eligible_since = now
            else:
                self._eligible_since = None
            observation = AutoLadderObservation(
                observed_at=now,
                settings_version=version,
                bid=bid,
                ask=ask,
                tick=self._tick,
                rows=tuple(self._entry_rows),
                last_trade_ms=last_trade_ms,
                market_ready=market_ready,
                market_dry=market_dry,
                price_safe=price_safe,
                choppy=choppy,
                no_trade=no_trade,
                entry_ready=eligible and now - self._eligible_since >= settings.entry_confirm_seconds,
                turns=turns,
            )
            with self._lock:
                self._snapshot = observation
            self._last_success_at = now
            self._failed_since = None
            if self._last_error is not None:
                logger.info("自动分仓公共行情读取恢复正常：%s", self.symbol)
                self._last_error = None
                self._last_report_at = -float("inf")
            self._report_observation(observation, settings, now_ms, price_reason)
        except Exception as exc:
            with self._lock:
                self._snapshot = None
            now = time.monotonic()
            if self._failed_since is None:
                self._failed_since = poll_started
            message = str(exc)
            if message != self._last_error or now - self._last_error_report_at >= self.ERROR_REPORT_SECONDS:
                logger.warning("自动分仓公共行情读取失败，等待重试并暂停新挂单：%s", message)
                self._last_error_report_at = now
            self._last_error = message

    def _report_observation(self, observation, settings, now_ms: int, price_reason: str) -> None:
        """Report shared entry evidence at a bounded rate, without five account copies."""
        rows = observation.rows
        if len(rows) < max(settings.volume_minutes, settings.price_minutes, settings.recent_minutes):
            logger.warning("自动分仓公共行情[%s] 分钟K线数量不足，等待重试", self.symbol)
            return
        volume_rows = rows[-settings.volume_minutes:]
        price_rows = rows[-settings.price_minutes:]
        total_volume = sum((row["quote_volume"] for row in volume_rows), Decimal(0))
        historical_volume = sum((row["quote_volume"] for row in volume_rows[:-1]), Decimal(0))
        last_volume = volume_rows[-1]["quote_volume"]
        recent_volume = sum((row["quote_volume"] for row in rows[-settings.recent_minutes:]), Decimal(0))
        price_range = max(row["high"] for row in price_rows) - min(row["low"] for row in price_rows)
        price_limit = observation.tick * settings.price_ticks
        last_trade_age = (now_ms - observation.last_trade_ms) / 1000
        reasons = []
        if total_volume < settings.start_volume:
            reasons.append("成交额不足")
        if recent_volume <= 0:
            reasons.append("近期无成交")
        if price_range > price_limit:
            reasons.append("入场价格振幅过大")
        if price_reason:
            reasons.append("盘口价格风险")
        if observation.no_trade:
            reasons.append("距最近成交过久")
        if observation.choppy:
            reasons.append("价格反复折返")
        if not reasons:
            stage = "入场条件已确认" if observation.entry_ready else "连续确认中"
        else:
            stage = "等待行情达标"
        key = stage, tuple(reasons)
        now = observation.observed_at
        if (key != self._last_report_key and now - self._last_report_at >= 5) or now - self._last_report_at >= self.REPORT_SECONDS:
            confirmed = max(0, now - self._eligible_since) if self._eligible_since is not None else 0
            last_label = "当前分钟" if self._has_live_candle else "最近一根已收盘分钟"
            logger.info(
                "自动分仓公共行情[%s] %s：近%d分钟成交额=%s/%s（此前%d分钟=%s，%s=%s）；"
                "近%d分钟成交额=%s（需>0）；近%d分钟价格振幅=%s/%s（%s/%s tick）；"
                "盘口=%s；最近成交距今=%.1f/%d秒；价格折返=%d/%d次；"
                "入场连续确认=%.1f/%d秒；缩量暂停信号=%s；未满足=%s",
                self.symbol, stage, settings.volume_minutes, total_volume, settings.start_volume,
                settings.volume_minutes - 1, historical_volume, last_label, last_volume,
                settings.recent_minutes, recent_volume, settings.price_minutes, price_range, price_limit,
                price_range / observation.tick, settings.price_ticks,
                price_reason or "安全", last_trade_age, settings.no_trade_seconds,
                observation.turns, settings.turn_count, confirmed, settings.entry_confirm_seconds,
                "是" if observation.market_dry else "否", "、".join(reasons) or "无",
            )
            self._last_report_at = now
            self._last_report_key = key

    def snapshot(self) -> AutoLadderObservation | None:
        with self._lock:
            observation = self._snapshot
        if observation is None or time.monotonic() - observation.observed_at > self.MAX_SNAPSHOT_AGE_SECONDS:
            return None
        return observation

    def start(self, stop_event=None) -> None:
        if self._thread is not None:
            raise RuntimeError("自动分仓行情观察线程已经启动")
        self._external_stop = stop_event
        self._thread = threading.Thread(
            target=self._run, name="auto-ladder-market-observer", daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._shutdown.is_set() and not (
                self._external_stop is not None and self._external_stop.is_set()
            ):
                self.poll_once()
                self._shutdown.wait(self.POLL_SECONDS)
        finally:
            if self._close_client_on_exit:
                self.client.close()

    def close(self) -> bool:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=25)
            return not self._thread.is_alive()
        return True
