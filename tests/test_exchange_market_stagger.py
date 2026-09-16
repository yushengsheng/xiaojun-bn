#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
import unittest

from exchange_strategy import MarketStaggerCoordinator


class MarketStaggerCoordinatorTests(unittest.TestCase):
    @staticmethod
    def _join_threads(threads: list[threading.Thread]) -> None:
        for thread in threads:
            thread.join(timeout=2)
        stuck = [thread.name for thread in threads if thread.is_alive()]
        if stuck:
            raise AssertionError(f"交错线程未结束：{stuck}")

    def test_slow_trade_blocks_the_next_phase(self):
        coordinator = MarketStaggerCoordinator([1, 2])
        events: list[str] = []
        events_lock = threading.Lock()
        slow_sell_started = threading.Event()
        release_slow_sell = threading.Event()

        def action(name: str, *, wait_event: threading.Event | None = None):
            def run():
                with events_lock:
                    events.append(f"{name}:start")
                if name == "lane1-sell1":
                    slow_sell_started.set()
                if wait_event is not None:
                    self.assertTrue(wait_event.wait(timeout=2))
                with events_lock:
                    events.append(f"{name}:end")
                return True

            return run

        def lane1():
            coordinator.execute(1, "BUY", action("lane1-buy1"))
            coordinator.execute(1, "SELL", action("lane1-sell1", wait_event=release_slow_sell))
            coordinator.execute(1, "BUY", action("lane1-buy2"))

        def lane2():
            coordinator.execute(2, "BUY", action("lane2-buy1"))
            coordinator.execute(2, "SELL", action("lane2-sell1"))

        threads = [
            threading.Thread(target=lane1, name="lane1"),
            threading.Thread(target=lane2, name="lane2"),
        ]
        for thread in threads:
            thread.start()

        self.assertTrue(slow_sell_started.wait(timeout=2))
        time.sleep(0.05)
        with events_lock:
            snapshot = list(events)
        self.assertIn("lane2-buy1:start", snapshot)
        self.assertNotIn("lane1-buy2:start", snapshot)
        self.assertNotIn("lane2-sell1:start", snapshot)

        release_slow_sell.set()
        self._join_threads(threads)
        with events_lock:
            self.assertIn("lane1-buy2:start", events)
            self.assertIn("lane2-sell1:start", events)

    def test_three_lanes_use_two_synchronized_odd_lanes_and_one_opposite_lane(self):
        coordinator = MarketStaggerCoordinator([1, 2, 3])
        events: list[str] = []
        events_lock = threading.Lock()
        odd_buy_started = threading.Event()
        release_odd_buy = threading.Event()

        def record(name: str, *, block: bool = False):
            def run():
                with events_lock:
                    events.append(name)
                if block:
                    odd_buy_started.set()
                    self.assertTrue(release_odd_buy.wait(timeout=2))
                return True

            return run

        def odd_lane(lane_id: int):
            coordinator.execute(
                lane_id,
                "BUY",
                record(f"lane{lane_id}-buy", block=(lane_id == 3)),
            )
            coordinator.execute(lane_id, "SELL", record(f"lane{lane_id}-sell"))

        def even_lane():
            coordinator.execute(2, "BUY", record("lane2-buy"))

        threads = [
            threading.Thread(target=lambda: odd_lane(1), name="lane1"),
            threading.Thread(target=even_lane, name="lane2"),
            threading.Thread(target=lambda: odd_lane(3), name="lane3"),
        ]
        for thread in threads:
            thread.start()

        self.assertTrue(odd_buy_started.wait(timeout=2))
        time.sleep(0.05)
        with events_lock:
            self.assertCountEqual(events, ["lane1-buy", "lane3-buy"])

        release_odd_buy.set()
        self._join_threads(threads)
        with events_lock:
            first_opposite_phase = {
                events.index("lane1-sell"),
                events.index("lane2-buy"),
                events.index("lane3-sell"),
            }
            last_initial_phase = max(events.index("lane1-buy"), events.index("lane3-buy"))
        self.assertTrue(all(index > last_initial_phase for index in first_opposite_phase))

    def test_unregistering_a_lane_releases_remaining_workers(self):
        coordinator = MarketStaggerCoordinator([1, 2])
        action_started = threading.Event()

        worker = threading.Thread(
            target=lambda: coordinator.execute(2, "BUY", lambda: action_started.set() or True),
            name="lane2",
        )
        worker.start()
        time.sleep(0.05)
        self.assertFalse(action_started.is_set())

        coordinator.unregister(1)
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertTrue(action_started.is_set())

    def test_suspended_lane_can_rejoin_with_a_new_account(self):
        coordinator = MarketStaggerCoordinator([1, 2])
        events: list[str] = []

        def lane1_first_account():
            coordinator.execute(1, "BUY", lambda: events.append("lane1-buy1") or True)
            coordinator.execute(1, "SELL", lambda: events.append("lane1-sell1") or True)
            coordinator.suspend(1)

        def lane2_first_account():
            coordinator.execute(2, "BUY", lambda: events.append("lane2-buy1") or True)
            coordinator.execute(2, "SELL", lambda: events.append("lane2-sell1") or True)
            coordinator.suspend(2)

        first_wave = [
            threading.Thread(target=lane1_first_account, name="lane1-first"),
            threading.Thread(target=lane2_first_account, name="lane2-first"),
        ]
        for thread in first_wave:
            thread.start()
        self._join_threads(first_wave)

        second_account = threading.Thread(
            target=lambda: coordinator.execute(
                1,
                "BUY",
                lambda: events.append("lane1-buy2") or True,
            ),
            name="lane1-second",
        )
        second_account.start()
        second_account.join(timeout=2)

        self.assertFalse(second_account.is_alive())
        self.assertIn("lane1-buy2", events)


if __name__ == "__main__":
    unittest.main()
