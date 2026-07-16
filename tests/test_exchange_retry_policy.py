#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
import unittest

import requests

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceAPIError, BinanceClient, SharedRateLimitGate
from exchange_constants import (
    TRADE_ACCOUNT_TYPE_EARN,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_MODE_CONVERT,
    TRADE_MODE_LIMIT,
    TRADE_MODE_MARKET,
    TRADE_MODE_PREMIUM,
)
from retry_utils import RetryPolicy, run_with_retries


class _RequestRetryProbe(BinanceClient):
    def __init__(self, request_gate=None, request_stop_event=None):
        self.reset_count = 0
        self._request_gate = request_gate
        self._request_stop_event = request_stop_event
        self.spot = "https://api.binance.com"

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

    def test_shared_gate_retries_same_request_without_consuming_regular_attempt(self):
        probe = _RequestRetryProbe(SharedRateLimitGate(0.02))
        calls = 0
        started_at = time.monotonic()

        def action():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise BinanceAPIError(-1003, "Too many requests queued.")
            return {"ok": True}

        result = probe._run_request_with_retry(
            action,
            max_retries=1,
            delay=0,
            base=probe.spot,
            path="/api/v3/order",
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls, 2)
        self.assertGreaterEqual(time.monotonic() - started_at, 0.015)

    def test_shared_gate_honors_longer_retry_after_header(self):
        probe = _RequestRetryProbe(SharedRateLimitGate(0.01))
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = "0.04"
        error = requests.exceptions.HTTPError("429 Too Many Requests", response=response)
        calls = 0
        started_at = time.monotonic()

        def action():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise error
            return "ok"

        self.assertEqual(
            probe._run_request_with_retry(
                action,
                max_retries=1,
                delay=0,
                base=probe.spot,
                path="/api/v3/account",
            ),
            "ok",
        )
        self.assertEqual(calls, 2)
        self.assertGreaterEqual(time.monotonic() - started_at, 0.03)

    def test_shared_gate_blocks_all_clients_in_same_batch(self):
        gate = SharedRateLimitGate(0.04)
        probes = [_RequestRetryProbe(gate), _RequestRetryProbe(gate)]
        completed: list[int] = []
        gate.pause()
        started_at = time.monotonic()

        def run_probe(index: int) -> None:
            probes[index]._run_request_with_retry(
                lambda: completed.append(index),
                max_retries=1,
                delay=0,
            )

        threads = [threading.Thread(target=run_probe, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=1)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(sorted(completed), [0, 1])
        self.assertGreaterEqual(time.monotonic() - started_at, 0.03)

    def test_convert_busy_code_is_scoped_to_convert_endpoint(self):
        convert_probe = _RequestRetryProbe(SharedRateLimitGate(0.01))
        convert_calls = 0

        def convert_action():
            nonlocal convert_calls
            convert_calls += 1
            if convert_calls == 1:
                raise BinanceAPIError(-1004, "Server is busy")
            return "ok"

        self.assertEqual(
            convert_probe._run_request_with_retry(
                convert_action,
                max_retries=1,
                delay=0,
                base=convert_probe.spot,
                path="/sapi/v1/convert/getQuote",
            ),
            "ok",
        )
        self.assertEqual(convert_calls, 2)

        futures_probe = _RequestRetryProbe(SharedRateLimitGate(0.01))
        with self.assertRaises(BinanceAPIError):
            futures_probe._run_request_with_retry(
                lambda: (_ for _ in ()).throw(BinanceAPIError(-1004, "Duplicate IP")),
                max_retries=1,
                delay=0,
                base="https://fapi.binance.com",
                path="/fapi/v1/order",
            )

    def test_shared_gate_wait_is_interruptible_by_batch_stop(self):
        stop_event = threading.Event()
        gate = SharedRateLimitGate(1, stop_event=stop_event)
        probe = _RequestRetryProbe(gate)
        gate.pause()
        stop_event.set()
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            return "unexpected"

        with self.assertRaisesRegex(RuntimeError, "停止信号"):
            probe._run_request_with_retry(action, max_retries=1, delay=0)
        self.assertEqual(calls, 0)

    def test_shared_gate_wait_is_interruptible_by_account_stop(self):
        batch_stop = threading.Event()
        account_stop = threading.Event()
        gate = SharedRateLimitGate(1, stop_event=batch_stop)
        probe = _RequestRetryProbe(gate, request_stop_event=account_stop)
        gate.pause()
        account_stop.set()

        with self.assertRaisesRegex(RuntimeError, "停止信号"):
            probe._run_request_with_retry(lambda: "unexpected", max_retries=1, delay=0)

    def test_shared_gate_checks_stop_after_cooldown_has_elapsed(self):
        stop_event = threading.Event()
        gate = SharedRateLimitGate(0, stop_event=stop_event)
        probe = _RequestRetryProbe(gate)
        stop_event.set()
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            return "unexpected"

        with self.assertRaisesRegex(RuntimeError, "停止信号"):
            probe._run_request_with_retry(action, max_retries=1, delay=0)
        self.assertEqual(calls, 0)

    def test_shared_gate_only_applies_to_requested_three_trade_options(self):
        uses_gate = ExchangeAppBatchMixin._uses_shared_rate_limit_gate
        self.assertTrue(uses_gate(TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET))
        self.assertTrue(uses_gate(TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_CONVERT))
        self.assertTrue(uses_gate(TRADE_ACCOUNT_TYPE_FUTURES, TRADE_MODE_MARKET))
        self.assertFalse(uses_gate(TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_LIMIT))
        self.assertFalse(uses_gate(TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_PREMIUM))
        self.assertFalse(uses_gate(TRADE_ACCOUNT_TYPE_EARN, TRADE_MODE_MARKET))


if __name__ == "__main__":
    unittest.main()
