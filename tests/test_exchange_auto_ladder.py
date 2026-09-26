from __future__ import annotations

import unittest
import threading
import time
from decimal import Decimal
from unittest.mock import patch
import requests

from exchange_binance_client import BinanceAPIError, BinanceClient, UnconfirmedOrderError
from exchange_auto_ladder import (
    AutoLadderSettings, AutoLadderSettingsStore, price_risk_reason, price_risk_safe, reversal_count,
    trade_is_recent, volume_and_price_signal,
)
from exchange_auto_ladder_observer import AutoLadderMarketObserver
from exchange_constants import TRADE_MODE_AUTO_LADDER
from tests.test_exchange_ladder_strategy import _BnbStopLadderClient, _StopAfterOrders, _strategy


def candles(volumes, *, low="10.110", high="10.112"):
    return [
        {"open_time": i * 60000, "quote_volume": Decimal(str(volume)),
         "low": Decimal(low), "high": Decimal(high)}
        for i, volume in enumerate(volumes)
    ]


def recent_candles_with_live(closed_volumes, live_volume):
    live_open = int(time.time() // 60) * 60000
    closed = candles(closed_volumes)
    offset = live_open - len(closed) * 60000
    for row in closed:
        row["open_time"] += offset
    live = candles([live_volume])[0]
    live["open_time"] = live_open
    return closed, live


class _ObserverClient:
    def __init__(self):
        self.last_trade_ms = None
        self.calls: list[str] = []
        self.book_seen = threading.Event()
        self.closed = False

    def get_symbol_tick_size(self, _symbol):
        self.calls.append(threading.current_thread().name)
        return Decimal("0.001")

    def get_closed_minute_klines(self, _symbol, _count):
        self.calls.append(threading.current_thread().name)
        return candles([100000] * 6)

    def get_last_spot_trade_time_ms(self, _symbol):
        self.calls.append(threading.current_thread().name)
        return self.last_trade_ms if self.last_trade_ms is not None else int(time.time() * 1000)

    def get_book_ticker(self, _symbol):
        self.calls.append(threading.current_thread().name)
        self.book_seen.set()
        return {"bidPrice": Decimal("10.111"), "askPrice": Decimal("10.112")}

    def close(self):
        self.closed = True


class AutoLadderTests(unittest.TestCase):
    def test_settings_validate_and_replace_atomically(self):
        store = AutoLadderSettingsStore()
        original, version = store.snapshot()
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"dry_volume": "100000"})
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"turn_count": "2.5"})
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"pause_price_ticks": "4"})
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"no_trade_seconds": "0"})
        self.assertEqual(AutoLadderSettings.parse({"entry_confirm_seconds": "0"}).entry_confirm_seconds, 0)
        self.assertEqual(
            AutoLadderSettings.parse({"price_ticks": "20"}).pause_price_ticks, 20,
        )  # 旧配置没有新增字段时仍能载入
        self.assertEqual(store.snapshot(), (original, version))
        update = AutoLadderSettings.parse({"start_volume": "200000"})
        store.replace(update)
        self.assertEqual(store.snapshot(), (update, version + 1))

    def test_five_minute_average_not_each_minute(self):
        settings = AutoLadderSettings()
        data = candles([0, 100000, 0, 0, 0, 1])
        self.assertEqual(volume_and_price_signal(data, settings, Decimal("0.001")), (True, False))
        data[-1]["quote_volume"] = Decimal(0)
        self.assertEqual(volume_and_price_signal(data, settings, Decimal("0.001")), (False, False))
        data[-1]["quote_volume"] = Decimal(1)
        data[1]["high"] = Decimal("10.116")
        self.assertFalse(volume_and_price_signal(data, settings, Decimal("0.001"))[0])

    def test_dry_confirmation_and_missing_minutes(self):
        settings = AutoLadderSettings.parse({"dry_minutes": "2", "dry_volume": "50", "dry_confirm_minutes": "2"})
        self.assertTrue(volume_and_price_signal(candles([1, 1, 1, 1, 1]), settings, Decimal("0.001"))[1])
        self.assertFalse(volume_and_price_signal(candles([1, 1, 100, 1, 1]), settings, Decimal("0.001"))[1])
        missing = candles([1, 1, 1, 1, 1])
        missing[-2]["open_time"] += 60000
        self.assertEqual(volume_and_price_signal(missing, settings, Decimal("0.001")), (False, False))

    def test_reversals_ignore_one_way_spike_and_one_tick_noise(self):
        tick = Decimal("0.001")
        def score(values):
            return reversal_count([(i * 1.5, Decimal(str(v))) for i, v in enumerate(values)],
                                  (len(values)-1) * 1.5, 45, tick * 2)
        self.assertEqual(score([10, 10.01, 10.01, 10.01]), 0)
        self.assertEqual(score([10, 10.01, 10, 10]), 1)
        self.assertEqual(score([10, 10.001, 10, 10.001, 10]), 0)
        self.assertEqual(score([10, 10.002, 10, 10.002, 10]), 3)
        self.assertEqual(reversal_count([(0, Decimal(10)), (16, Decimal(10)), (60, Decimal(11)),
                                         (61, Decimal(10))], 61, 45, Decimal(1)), 1)

    def test_price_risk_catches_one_way_jump_and_wide_spread(self):
        settings = AutoLadderSettings()
        rows = candles([100000] * 5)
        tick = Decimal("0.001")
        self.assertTrue(price_risk_safe(rows, settings, tick, Decimal("10.111"), Decimal("10.112")))
        self.assertFalse(price_risk_safe(rows, settings, tick, Decimal("10.130"), Decimal("10.131")))
        self.assertFalse(price_risk_safe(rows, settings, tick, Decimal("10.105"), Decimal("10.116")))
        self.assertFalse(price_risk_safe(rows, settings, tick, Decimal("10.113"), Decimal("10.112")))
        rows[-1]["high"] = Decimal("10.125")
        self.assertFalse(price_risk_safe(rows, settings, tick, Decimal("10.111"), Decimal("10.112")))
        self.assertIn("价格振幅", price_risk_reason(rows, settings, tick, Decimal("10.111"), Decimal("10.112")))

    def test_observer_enters_from_four_closed_minutes_and_current_live_minute(self):
        class LiveClient(_ObserverClient):
            live_volume = Decimal("1")
            def get_minute_klines_with_live(self, _symbol, _count):
                return recent_candles_with_live([0, 0, 100000, 0, 0, 0], self.live_volume)
            def get_closed_minute_klines(self, *_args):
                raise AssertionError("实盘观察应读取当前分钟")
        client = LiveClient()
        settings = AutoLadderSettingsStore(AutoLadderSettings.parse({"entry_confirm_seconds": "0"}))
        observer = AutoLadderMarketObserver(client, "BTCUSDT", settings)
        observer.KLINE_REFRESH_SECONDS = 0
        observer.poll_once()
        self.assertTrue(observer.snapshot().market_ready)
        self.assertTrue(observer.snapshot().entry_ready)
        self.assertFalse(observer.snapshot().market_dry)
        self.assertEqual(observer.snapshot().rows[-1]["quote_volume"], Decimal("1"))
        client.live_volume = Decimal("0")
        observer.poll_once()
        self.assertFalse(observer.snapshot().market_ready)

    def test_current_minute_recovery_overrides_dry_closed_history(self):
        class LiveClient(_ObserverClient):
            def get_minute_klines_with_live(self, _symbol, _count):
                return recent_candles_with_live([0] * 6, 150000)

        observer = AutoLadderMarketObserver(
            LiveClient(), "BTCUSDT",
            AutoLadderSettingsStore(AutoLadderSettings.parse({"entry_confirm_seconds": "0"})),
        )
        observer.poll_once()
        signal = observer.snapshot()
        self.assertTrue(signal.market_ready)
        self.assertFalse(signal.market_dry)
        self.assertTrue(signal.entry_ready)

    def test_minute_switch_during_market_fetch_discards_stale_live_candle(self):
        clock = [360.0]

        class LiveClient(_ObserverClient):
            kline_calls = 0

            def get_minute_klines_with_live(self, _symbol, _count):
                self.kline_calls += 1
                return recent_candles_with_live([100000] * 6, 1)

            def get_last_spot_trade_time_ms(self, _symbol):
                clock[0] = 420.0
                return 420000

        client = LiveClient()
        observer = AutoLadderMarketObserver(client, "BTCUSDT", AutoLadderSettingsStore())
        with patch("exchange_auto_ladder_observer.time.time", side_effect=lambda: clock[0]):
            observer.poll_once()
            self.assertIsNone(observer.snapshot())
            observer.poll_once()
            self.assertIsNotNone(observer.snapshot())
        self.assertEqual(client.kline_calls, 2)

    def test_auto_mode_can_submit_during_current_minute_volume_recovery(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = 1
        closed, live = recent_candles_with_live([0] * 6, 150000)
        client.get_minute_klines_with_live = lambda *_args: (closed, live)
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = AutoLadderSettingsStore(
            AutoLadderSettings.parse({"entry_confirm_seconds": "0"})
        )
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0.001
        strategy._run_ladder_mode(stop)
        self.assertTrue(client.placements)

    def test_observer_reports_actual_entry_evidence_without_five_account_log_spam(self):
        class LiveClient(_ObserverClient):
            def get_minute_klines_with_live(self, _symbol, _count):
                return recent_candles_with_live([10000] * 6, 1)

        observer = AutoLadderMarketObserver(
            LiveClient(), "BTCUSDT", AutoLadderSettingsStore(),
        )
        with patch("exchange_auto_ladder_observer.logger.info") as info:
            observer.poll_once()
            observer.poll_once()
        self.assertIsNotNone(observer.snapshot())
        self.assertEqual(info.call_count, 1)
        report = info.call_args.args[0] % info.call_args.args[1:]
        self.assertIn("等待行情达标", report)
        self.assertIn("此前4分钟=40000", report)
        self.assertIn("当前分钟=1", report)
        self.assertIn("成交额不足", report)
        self.assertIn("入场连续确认=0.0/30秒", report)

    def test_recent_trade_boundary_and_future_timestamp(self):
        self.assertTrue(trade_is_recent(55001, 100000, 45))
        self.assertFalse(trade_is_recent(55000, 100000, 45))
        self.assertFalse(trade_is_recent(0, 100000, 45))
        self.assertFalse(trade_is_recent(106000, 100000, 45))

    def test_entry_confirmation_resets_after_silence_and_setting_change(self):
        clock = [100.0]
        client = _ObserverClient()
        settings = AutoLadderSettingsStore()
        observer = AutoLadderMarketObserver(client, "BTCUSDT", settings)
        with patch("exchange_auto_ladder_observer.time.monotonic", side_effect=lambda: clock[0]), \
             patch("exchange_auto_ladder_observer.time.time", side_effect=lambda: clock[0]):
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            for second in range(104, 129, 4):
                clock[0] = float(second)
                observer.poll_once()
            clock[0] = 129.0
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            clock[0] = 130.0
            observer.poll_once()
            self.assertTrue(observer.snapshot().entry_ready)
            client.last_trade_ms = 85000
            clock[0] = 131.0
            observer.poll_once()
            self.assertTrue(observer.snapshot().no_trade)
            self.assertFalse(observer.snapshot().entry_ready)
            client.last_trade_ms = None
            clock[0] = 132.0
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            for second in range(136, 161, 4):
                clock[0] = float(second)
                observer.poll_once()
            clock[0] = 162.0
            observer.poll_once()
            self.assertTrue(observer.snapshot().entry_ready)
            settings.replace(AutoLadderSettings.parse({"entry_confirm_seconds": "30"}))
            clock[0] = 163.0
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            clock[0] = 169.0
            self.assertIsNone(observer.snapshot())  # 失联的旧快照不能继续触发入场

    def test_five_readers_share_one_market_fetch_thread(self):
        client = _ObserverClient()
        observer = AutoLadderMarketObserver(
            client, "BTCUSDT", AutoLadderSettingsStore(), close_client_on_exit=True,
        )
        observer.POLL_SECONDS = 10
        observer.start()
        try:
            self.assertTrue(client.book_seen.wait(2))
            snapshots = []
            readers = [threading.Thread(target=lambda: snapshots.append(observer.snapshot())) for _ in range(5)]
            for reader in readers:
                reader.start()
            for reader in readers:
                reader.join()
            self.assertEqual(len(snapshots), 5)
            self.assertTrue(all(snapshot is snapshots[0] for snapshot in snapshots))
            self.assertEqual(set(client.calls), {"auto-ladder-market-observer"})
        finally:
            self.assertTrue(observer.close())
        self.assertTrue(client.closed)

    def test_entry_confirmation_pauses_during_observer_gap(self):
        clock = [100.0]
        observer = AutoLadderMarketObserver(
            _ObserverClient(), "BTCUSDT", AutoLadderSettingsStore(),
        )
        with patch("exchange_auto_ladder_observer.time.monotonic", side_effect=lambda: clock[0]), \
             patch("exchange_auto_ladder_observer.time.time", side_effect=lambda: clock[0]):
            observer.poll_once()
            clock[0] = 120.0  # 不把失联时间计入确认时长
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            for second in range(124, 149, 4):
                clock[0] = float(second)
                observer.poll_once()
            clock[0] = 149.0
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            clock[0] = 150.0
            observer.poll_once()
            self.assertTrue(observer.snapshot().entry_ready)

    def test_failed_market_fetch_keeps_confirmed_progress_and_fails_closed(self):
        clock = [100.0]
        client = _ObserverClient()
        observer = AutoLadderMarketObserver(client, "BTCUSDT", AutoLadderSettingsStore())
        original_book = client.get_book_ticker
        with patch("exchange_auto_ladder_observer.time.monotonic", side_effect=lambda: clock[0]), \
             patch("exchange_auto_ladder_observer.time.time", side_effect=lambda: clock[0]):
            for second in (100, 104, 108, 110):
                clock[0] = float(second)
                observer.poll_once()
            client.get_book_ticker = lambda _symbol: (_ for _ in ()).throw(ConnectionError("断线"))
            clock[0] = 112.0
            observer.poll_once()
            self.assertIsNone(observer.snapshot())
            client.get_book_ticker = original_book
            clock[0] = 120.0
            observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            for second in (124, 128, 132, 136, 140):
                clock[0] = float(second)
                observer.poll_once()
            self.assertTrue(observer.snapshot().entry_ready)

    def test_short_failed_poll_pauses_entry_confirmation(self):
        clock = [100.0]
        client = _ObserverClient()
        observer = AutoLadderMarketObserver(client, "BTCUSDT", AutoLadderSettingsStore())
        original_book = client.get_book_ticker
        with patch("exchange_auto_ladder_observer.time.monotonic", side_effect=lambda: clock[0]), \
             patch("exchange_auto_ladder_observer.time.time", side_effect=lambda: clock[0]):
            for second in (100, 104, 108, 110):
                clock[0] = float(second)
                observer.poll_once()
            client.get_book_ticker = lambda _symbol: (_ for _ in ()).throw(ConnectionError("短暂断线"))
            clock[0] = 112.0
            observer.poll_once()
            client.get_book_ticker = original_book
            clock[0] = 115.0
            observer.poll_once()
            for second in (119, 123, 127, 129):
                clock[0] = float(second)
                observer.poll_once()
            self.assertFalse(observer.snapshot().entry_ready)
            clock[0] = 133.0
            observer.poll_once()
            self.assertTrue(observer.snapshot().entry_ready)

    def test_five_account_workers_do_not_fetch_public_data_while_observing(self):
        observer_client = _ObserverClient()
        settings = AutoLadderSettingsStore()
        observer = AutoLadderMarketObserver(observer_client, "BTCUSDT", settings)
        observer.poll_once()
        self.assertFalse(observer.snapshot().entry_ready)
        failures = []
        clients = []
        def run_account():
            stop = _StopAfterOrders()
            client = _BnbStopLadderClient(stop)
            client.stop_after_placements = None
            def forbidden(*_args, **_kwargs):
                raise AssertionError("账号线程不应重复请求公共行情")
            client.get_book_ticker = forbidden
            client.get_closed_minute_klines = forbidden
            client.get_last_spot_trade_time_ms = forbidden
            stop.wait = lambda _seconds: setattr(stop, "stopped", True) or True
            strategy = _strategy(client)
            strategy.trade_mode = TRADE_MODE_AUTO_LADDER
            strategy.auto_ladder_settings = settings
            strategy.auto_ladder_observer = observer
            try:
                strategy._run_ladder_mode(stop)
            except Exception as exc:
                failures.append(exc)
            clients.append(client)
        workers = [threading.Thread(target=run_account) for _ in range(5)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=2)
        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(failures, [])
        self.assertEqual(len(clients), 5)
        self.assertTrue(all(not client.placements for client in clients))

    def test_auto_mode_enters_then_cancels_on_dry_without_market_sell(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = None
        store = AutoLadderSettingsStore(AutoLadderSettings.parse({
            "volume_minutes": "1", "start_volume": "100", "recent_minutes": "1",
            "price_minutes": "1", "dry_minutes": "1", "dry_volume": "50",
            "dry_confirm_minutes": "1", "stable_seconds": "1", "entry_confirm_seconds": "0",
        }))
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = store
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0.001
        samples = [candles([100]), candles([0])]
        def get_market(_symbol, _count):
            return samples.pop(0) if samples else candles([0])
        client.get_closed_minute_klines = get_market
        seen_pause = []
        seen_on_save = []
        def wait(_seconds):
            stop.wait_count += 1
            if stop.wait_count == 1:
                store.replace(store.snapshot()[0])  # 触发下一轮行情读取
                seen_on_save.extend(client.events)
            if stop.wait_count == 2:
                seen_pause.extend(client.events)
                stop.stopped = True
            return stop.stopped
        stop.wait_count = 0
        stop.wait = wait
        strategy._run_ladder_mode(stop)
        self.assertTrue(client.placements)
        self.assertFalse(any(event.startswith("cancel-") for event in seen_on_save))
        self.assertTrue(any(event.startswith("cancel-") for event in seen_pause))
        self.assertFalse(any(event.startswith("market-sell-") for event in seen_pause))
        self.assertEqual(client.events[-1], "market-sell-BTCUSDT")  # 人工停止仍沿用原逻辑

    def test_auto_mode_pauses_on_one_way_jump_before_next_candle(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = None
        book_reads = 0
        def get_book(_symbol):
            nonlocal book_reads
            book_reads += 1
            if book_reads <= 2:
                return {"bidPrice": Decimal("10.111"), "askPrice": Decimal("10.112")}
            return {"bidPrice": Decimal("10.130"), "askPrice": Decimal("10.131")}
        client.get_book_ticker = get_book
        client.get_closed_minute_klines = lambda *_args: candles([100000] * 6)
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = AutoLadderSettingsStore(
            AutoLadderSettings.parse({"entry_confirm_seconds": "0"})
        )
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0.001
        events_on_pause = []
        def wait(_seconds):
            stop.wait_count += 1
            if stop.wait_count == 2:
                events_on_pause.extend(client.events)
                stop.stopped = True
            return stop.stopped
        stop.wait_count = 0
        stop.wait = wait
        strategy._run_ladder_mode(stop)
        self.assertTrue(client.placements)
        self.assertTrue(any(event.startswith("cancel-") for event in events_on_pause))
        self.assertFalse(any(event.startswith("market-sell-") for event in events_on_pause))

    def test_auto_mode_pauses_when_no_new_trades(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = None
        client.get_closed_minute_klines = lambda *_args: candles([100000] * 6)
        trade_checks = 0
        def last_trade(_symbol):
            nonlocal trade_checks
            trade_checks += 1
            return int(time.time() * 1000) - (46000 if trade_checks > 1 else 0)
        client.get_last_spot_trade_time_ms = last_trade
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = AutoLadderSettingsStore(
            AutoLadderSettings.parse({"entry_confirm_seconds": "0"})
        )
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0.001
        events_on_pause = []
        def wait(_seconds):
            stop.wait_count += 1
            if stop.wait_count == 2:
                events_on_pause.extend(client.events)
                stop.stopped = True
            return stop.stopped
        stop.wait_count = 0
        stop.wait = wait
        strategy._run_ladder_mode(stop)
        self.assertTrue(client.placements)
        self.assertTrue(any(event.startswith("cancel-") for event in events_on_pause))
        self.assertFalse(any(event.startswith("market-sell-") for event in events_on_pause))

    def test_auto_mode_rechecks_book_before_submitting_orders(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = None
        book_reads = 0
        def get_book(_symbol):
            nonlocal book_reads
            book_reads += 1
            if book_reads == 1:
                return {"bidPrice": Decimal("10.111"), "askPrice": Decimal("10.112")}
            return {"bidPrice": Decimal("10.130"), "askPrice": Decimal("10.131")}
        client.get_book_ticker = get_book
        client.get_closed_minute_klines = lambda *_args: candles([100000] * 6)
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = AutoLadderSettingsStore(
            AutoLadderSettings.parse({"entry_confirm_seconds": "0"})
        )
        strategy.LADDER_POLL_INTERVAL_SECONDS = 0.001
        def wait(_seconds):
            stop.stopped = True
            return True
        stop.wait = wait
        strategy._run_ladder_mode(stop)
        self.assertEqual(book_reads, 2)
        self.assertEqual(client.placements, [])

    def test_auto_mode_rechecks_book_between_child_orders(self):
        client = _BnbStopLadderClient(_StopAfterOrders())
        strategy = _strategy(client)
        book_reads = 0
        def get_book(_symbol):
            nonlocal book_reads
            book_reads += 1
            bid = Decimal("10.111") if book_reads == 1 else Decimal("10.130")
            return {"bidPrice": bid, "askPrice": bid + Decimal("0.001")}
        client.get_book_ticker = get_book
        pending = [
            {"side": "BUY", "level_index": index, "amount": Decimal("25"), "source": "pool"}
            for index in range(2)
        ]
        active = {}
        target_prices = strategy._ladder_target_prices(Decimal("10.111"), Decimal("10.112"))
        with self.assertRaisesRegex(RuntimeError, "提交子订单前公共行情已变化"):
            strategy._ladder_submit_pending_allocations(
                pending, active, target_prices,
                market_guard=lambda bid, _ask: bid < Decimal("10.120"),
            )
        self.assertEqual(book_reads, 2)
        self.assertEqual(len(client.placements), 1)
        self.assertEqual(len(active), 1)
        self.assertEqual(len(pending), 1)

    def test_real_client_order_path_enables_identified_retry_and_market_guard(self):
        class RecordingClient(_BnbStopLadderClient, BinanceClient):
            def spot_limit_buy_quote_amount(self, symbol, price, amount, **kwargs):
                self.safety_kwargs = kwargs
                return super().spot_limit_buy_quote_amount(
                    symbol, price, amount, maker_only=kwargs["maker_only"],
                )
        client = RecordingClient(_StopAfterOrders())
        strategy = _strategy(client)
        pending = [{"side": "BUY", "level_index": 0, "amount": Decimal("25"), "source": "pool"}]
        target_prices = strategy._ladder_target_prices(Decimal("10.111"), Decimal("10.112"))
        strategy._ladder_submit_pending_allocations(
            pending, {}, target_prices, market_guard=lambda _bid, _ask: True,
        )
        self.assertTrue(client.safety_kwargs["safe_retry"])
        self.assertTrue(client.safety_kwargs["retry_guard"]())

    def test_unconfirmed_order_stops_account_and_records_recovery_blocker(self):
        class UnconfirmedClient(_BnbStopLadderClient):
            def spot_limit_buy_quote_amount(self, *_args, **_kwargs):
                raise UnconfirmedOrderError("BTCUSDT", "xja-unknown")
        class Journal:
            def __init__(self):
                self.payload = None
                self.state = None
            def begin(self, _kind, _scope, payload):
                self.payload = payload
                return "entry-1"
            def update(self, _entry_id, state):
                self.state = state
        stop = _StopAfterOrders()
        client = UnconfirmedClient(stop)
        client.get_closed_minute_klines = lambda *_args: candles([100000] * 6)
        strategy = _strategy(client)
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = AutoLadderSettingsStore(
            AutoLadderSettings.parse({"entry_confirm_seconds": "0"})
        )
        journal = Journal()
        strategy.operation_journal = journal
        strategy.operation_scope = "account:test"
        strategy._run_ladder_mode(stop)
        self.assertIn("xja-unknown", strategy.trade_error)
        self.assertEqual(journal.payload["client_order_id"], "xja-unknown")
        self.assertEqual(journal.state, "open")

    def test_market_client_uses_closed_quote_volume_not_taker_volume(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        def row(minute, quote):
            return [minute * 60000, "10", "10.112", "10.110", "10", "0",
                    minute * 60000 + 59999, quote, "0", "0", "99999999", "0"]
        rows = [row(120, "3"), row(121, "11"), row(122, "22"), row(123, "999")]
        client.public_get_query = lambda *_args, **_kwargs: rows
        with patch("exchange_binance_client.time.time", return_value=123 * 60 + 30):
            result = client.get_closed_minute_klines("BABYUSDC", 2)
            self.assertEqual([item["quote_volume"] for item in result], [Decimal(11), Decimal(22)])
            rows[2][0] += 60000
            with self.assertRaisesRegex(RuntimeError, "时间格式无效"):
                client.get_closed_minute_klines("BABYUSDC", 2)

    def test_market_client_reads_current_and_historical_minutes_together(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        def row(minute, quote):
            return [minute * 60000, "10", "10.112", "10.110", "10", "0",
                    minute * 60000 + 59999, quote, "0", "0", "99999999", "0"]
        rows = [row(minute, str(minute)) for minute in range(120, 126)]
        client.public_get_query = lambda *_args, **_kwargs: rows
        with patch("exchange_binance_client.time.time", return_value=125 * 60 + 30):
            closed, live = client.get_minute_klines_with_live("BABYUSDT", 5)
            self.assertEqual([item["open_time"] for item in closed], [minute * 60000 for minute in range(120, 125)])
            self.assertEqual(live["quote_volume"], Decimal("125"))
            rows.pop()
            with self.assertRaisesRegex(RuntimeError, "当前分钟K线不可用"):
                client.get_minute_klines_with_live("BABYUSDT", 5)

    def test_market_client_reads_last_trade_time_from_avg_price(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        calls = []
        def public_get_query(_base, path, params, **kwargs):
            calls.append((path, params, kwargs))
            return {"mins": 5, "price": "10", "closeTime": 123456789}
        client.public_get_query = public_get_query
        self.assertEqual(client.get_last_spot_trade_time_ms("BTCUSDT"), 123456789)
        self.assertEqual(calls[0][0], "/api/v3/avgPrice")
        self.assertEqual(calls[0][1], {"symbol": "BTCUSDT"})
        self.assertEqual(calls[0][2]["max_retries"], client.DEFAULT_REQUEST_RETRY_COUNT)
        client.public_get_query = lambda *_args, **_kwargs: {"price": "10"}
        with self.assertRaisesRegex(RuntimeError, "最近成交时间无效"):
            client.get_last_spot_trade_time_ms("BTCUSDT")

    def test_ambiguous_order_reply_is_recovered_without_second_post(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        posts = []
        def request_query(_base, method, _path, params, **_kwargs):
            if method == "POST":
                posts.append(params["newClientOrderId"])
                raise requests.Timeout("response lost")
            self.assertEqual(params["origClientOrderId"], posts[0])
            return {"orderId": 42, "status": "NEW"}
        client.request_query = request_query
        order = client._place_limit_order_with_recovery("BTCUSDT", {"symbol": "BTCUSDT"}, lambda: True)
        self.assertEqual(order["orderId"], 42)
        self.assertEqual(len(posts), 1)

    def test_missing_order_after_lost_reply_never_reposts_even_if_first_filled(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        posts = []
        def request_query(_base, method, _path, params, **_kwargs):
            if method == "POST":
                posts.append(params["newClientOrderId"])
                raise requests.Timeout("response lost after first order filled")
            raise BinanceAPIError(-2013, "Order does not exist")
        client.request_query = request_query
        with patch("exchange_binance_client.time.sleep"):
            with self.assertRaises(UnconfirmedOrderError):
                client._place_limit_order_with_recovery("BTCUSDT", {"symbol": "BTCUSDT"}, lambda: True)
        self.assertEqual(len(posts), 1)

    def test_known_rejected_limit_order_does_not_create_recovery_blocker(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        calls = []
        def request_query(_base, method, _path, _params, **_kwargs):
            calls.append(method)
            raise BinanceAPIError(-2010, "Order would immediately match and take")
        client.request_query = request_query
        with self.assertRaises(BinanceAPIError):
            client._place_limit_order_with_recovery("BTCUSDT", {"symbol": "BTCUSDT"}, lambda: True)
        self.assertEqual(calls, ["POST"])

    def test_order_status_query_failure_stops_without_duplicate_post(self):
        client = BinanceClient.__new__(BinanceClient)
        client.spot = "https://unused.example"
        posts = []
        def request_query(_base, method, _path, params, **_kwargs):
            if method == "POST":
                posts.append(params["newClientOrderId"])
                raise requests.Timeout("response lost")
            raise ConnectionError("order status unavailable")
        client.request_query = request_query
        with self.assertRaises(UnconfirmedOrderError) as captured:
            client._place_limit_order_with_recovery("BTCUSDT", {"symbol": "BTCUSDT"}, lambda: True)
        self.assertEqual(len(posts), 1)
        self.assertEqual(captured.exception.client_order_id, posts[0])

    def test_running_settings_store_is_same_instance(self):
        store = AutoLadderSettingsStore()
        strategy = _strategy(object())
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = store
        store.replace(AutoLadderSettings.parse({"turn_count": "5"}))
        self.assertEqual(strategy.auto_ladder_settings.snapshot()[0].turn_count, 5)


if __name__ == "__main__":
    unittest.main()
