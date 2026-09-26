from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

from exchange_binance_client import BinanceClient
from exchange_auto_ladder import (
    AutoLadderSettings, AutoLadderSettingsStore, reversal_count, volume_and_price_signal,
)
from exchange_constants import TRADE_MODE_AUTO_LADDER
from tests.test_exchange_ladder_strategy import _BnbStopLadderClient, _StopAfterOrders, _strategy


def candles(volumes, *, low="10.110", high="10.112"):
    return [
        {"open_time": i * 60000, "quote_volume": Decimal(str(volume)),
         "low": Decimal(low), "high": Decimal(high)}
        for i, volume in enumerate(volumes)
    ]


class AutoLadderTests(unittest.TestCase):
    def test_settings_validate_and_replace_atomically(self):
        store = AutoLadderSettingsStore()
        original, version = store.snapshot()
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"dry_volume": "100000"})
        with self.assertRaises(ValueError):
            AutoLadderSettings.parse({"turn_count": "2.5"})
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

    def test_auto_mode_enters_then_cancels_on_dry_without_market_sell(self):
        stop = _StopAfterOrders()
        client = _BnbStopLadderClient(stop)
        client.stop_after_placements = None
        store = AutoLadderSettingsStore(AutoLadderSettings.parse({
            "volume_minutes": "1", "start_volume": "100", "recent_minutes": "1",
            "price_minutes": "1", "dry_minutes": "1", "dry_volume": "50",
            "dry_confirm_minutes": "1", "stable_seconds": "1",
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

    def test_running_settings_store_is_same_instance(self):
        store = AutoLadderSettingsStore()
        strategy = _strategy(object())
        strategy.trade_mode = TRADE_MODE_AUTO_LADDER
        strategy.auto_ladder_settings = store
        store.replace(AutoLadderSettings.parse({"turn_count": "5"}))
        self.assertEqual(strategy.auto_ladder_settings.snapshot()[0].turn_count, 5)


if __name__ == "__main__":
    unittest.main()
