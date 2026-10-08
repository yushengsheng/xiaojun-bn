#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
import unittest

from exchange_strategy import MarketStaggerCoordinator


class MarketStaggerCoordinatorTests(unittest.TestCase):
    def _run_accounts(self, coordinator, rounds):
        events, errors = [], []
        stop = threading.Event()
        def worker(lane, count):
            try:
                for _ in range(count):
                    for side in ("BUY", "SELL"):
                        def action(side=side):
                            with coordinator._condition:
                                events.append((coordinator._phase, lane, side))
                            time.sleep(0.001)
                        coordinator.execute(lane, side, action, stop_event=stop)
            except BaseException as exc:
                errors.append(exc)
            finally:
                coordinator.suspend(lane)
                coordinator.unregister(lane)
        threads = [threading.Thread(target=worker, args=(lane, count), daemon=True)
                   for lane, count in rounds.items()]
        for thread in threads:
            thread.start()
        try:
            self._join_threads(threads)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=1)
        self.assertEqual(errors, [])
        for lane, count in rounds.items():
            self.assertEqual([side for _, ident, side in events if ident == lane], ["BUY", "SELL"] * count)
        return events

    def _assert_opposite_pair_phases(self, events, pair):
        phases = {}
        for phase, lane, side in events:
            phases.setdefault(phase, {})[lane] = side
        paired = [actions for actions in phases.values() if set(actions) == set(pair)]
        self.assertGreaterEqual(len(paired), 4)
        for actions in paired:
            self.assertCountEqual(actions.values(), ["BUY", "SELL"])

    def test_remaining_same_parity_lanes_are_rebalanced(self):
        for pair in ((1, 3), (2, 6)):
            with self.subTest(pair=pair):
                coordinator = MarketStaggerCoordinator(range(1, 7))
                for lane in set(range(1, 7)) - set(pair):
                    coordinator.unregister(lane)
                events = self._run_accounts(coordinator, dict.fromkeys(pair, 4))
                self._assert_opposite_pair_phases(events, pair)

    def test_accounts_finish_one_by_one_until_two_then_one_remain(self):
        coordinator = MarketStaggerCoordinator(range(1, 7))
        events = self._run_accounts(coordinator, {1: 1, 2: 15, 3: 3, 4: 5, 5: 7, 6: 12})
        self._assert_opposite_pair_phases(events, (2, 6))

    def test_rebalanced_lane_wait_can_be_stopped_without_submitting_a_trade(self):
        coordinator = MarketStaggerCoordinator([1, 3])
        stop = threading.Event()
        actions, errors = [], []
        def worker():
            try:
                coordinator.execute(3, "BUY", lambda: actions.append("cancelled-buy"), stop_event=stop)
            except Exception as exc:
                errors.append(str(exc))
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        with coordinator._condition:
            coordinator._condition.wait_for(lambda: 3 in coordinator._pending, timeout=1)
        stop.set()
        self._join_threads([thread])
        self.assertEqual(actions, [])
        self.assertEqual(errors, ["检测到停止信号"])
        coordinator.execute(1, "BUY", lambda: actions.append("buy"))
        coordinator.execute(1, "SELL", lambda: actions.append("sell"))
        self.assertEqual(actions, ["buy", "sell"])

    def test_rebalance_waits_for_released_sells_before_delaying_a_buy(self):
        coordinator = MarketStaggerCoordinator([1, 2, 3])
        events = []
        release_sell = threading.Event()
        sell_started = threading.Event()
        def lane(lane_id):
            for side in ("BUY", "SELL") * 5:
                def action(side=side):
                    with coordinator._condition:
                        events.append((coordinator._phase, lane_id, side))
                    if side == "SELL" and not release_sell.is_set():
                        if sum(item[2] == "SELL" for item in events) == 2:
                            sell_started.set()
                        release_sell.wait(timeout=2)
                coordinator.execute(lane_id, side, action)
            coordinator.unregister(lane_id)
        threads = [threading.Thread(target=lane, args=(ident,), daemon=True) for ident in (1, 3)]
        threads.append(threading.Thread(target=lambda: coordinator.execute(2, "BUY", lambda: True), daemon=True))
        for thread in threads:
            thread.start()
        self.assertTrue(sell_started.wait(timeout=2))
        coordinator.unregister(2)
        with coordinator._condition:
            self.assertEqual([side for _, _, side in events].count("SELL"), 2)
        release_sell.set()
        self._join_threads(threads)
        # The sells already released finish together; all subsequent paired
        # phases have opposite sides without adding or skipping any trade.
        first_sell_phase = next(phase for phase, _, side in events if side == "SELL")
        self._assert_opposite_pair_phases([event for event in events if event[0] > first_sell_phase], (1, 3))

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
