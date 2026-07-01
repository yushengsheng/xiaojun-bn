#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from exchange_binance_client import BinanceAPIError, BinanceClient
from retry_utils import RetryPolicy, run_with_retries


class _RequestRetryProbe(BinanceClient):
    def __init__(self):
        self.reset_count = 0

    def _reset_session_after_transport_error(self) -> None:
        self.reset_count += 1


class ExchangeRetryPolicyTests(unittest.TestCase):
    def test_generic_retry_helper_retries_exact_attempt_count(self):
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            if calls < 4:
                raise RuntimeError("temporary")
            return "ok"

        result = run_with_retries(
            action,
            policy=RetryPolicy(attempts=10, delay_seconds=0),
            should_retry=lambda _exc: True,
        )

        self.assertEqual(result, "ok")
        self.assertEqual(calls, 4)

    def test_binance_permission_error_retries_with_request_policy(self):
        probe = _RequestRetryProbe()
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            if calls < 10:
                raise BinanceAPIError(-2015, "Invalid API-key, IP, or permissions for action.")
            return {"ok": True}

        result = probe._run_request_with_retry(action, max_retries=10, delay=0)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls, 10)

    def test_non_retryable_binance_error_still_fails_immediately(self):
        probe = _RequestRetryProbe()
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            raise BinanceAPIError(-2010, "Account has insufficient balance")

        with self.assertRaises(BinanceAPIError):
            probe._run_request_with_retry(action, max_retries=10, delay=0)

        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
