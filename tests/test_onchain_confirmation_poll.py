#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from onchain_relay_runner import _relay_poll_interval_seconds, _wait_for_transaction_success


class _StopRequested:
    def __init__(self):
        self.wait_calls: list[float] = []

    @staticmethod
    def is_set() -> bool:
        return False

    def wait(self, seconds: float) -> bool:
        self.wait_calls.append(seconds)
        return True


class _ConfirmationOwner:
    def __init__(self, poll_interval: object):
        self.poll_interval = poll_interval
        self.stop_requested = _StopRequested()
        self.client = type("Client", (), {"get_transaction_receipt": staticmethod(lambda _network, _txid: None)})()

    def _runtime_confirmation_poll_interval_seconds(self):
        return self.poll_interval


class OnchainConfirmationPollTests(unittest.TestCase):
    def test_relay_confirmation_uses_configured_runtime_poll_interval(self) -> None:
        owner = _ConfirmationOwner(0.5)

        with self.assertRaisesRegex(RuntimeError, "任务已停止"):
            _wait_for_transaction_success(owner, "BSC", "0xabc", label="确认", timeout_seconds=10)

        self.assertEqual(owner.stop_requested.wait_calls, [0.5])

    def test_invalid_runtime_poll_interval_falls_back_to_default(self) -> None:
        owner = _ConfirmationOwner("nan")

        self.assertEqual(_relay_poll_interval_seconds(owner), 2.0)


if __name__ == "__main__":
    unittest.main()
