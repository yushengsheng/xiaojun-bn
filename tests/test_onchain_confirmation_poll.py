#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from app_defaults import ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT
from onchain_relay_runner import (
    RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS,
    _relay_is_transient_rpc_error,
    _relay_poll_interval_seconds,
    _wait_for_transaction_success,
)


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


class _RecordingReceiptClient:
    def __init__(self):
        self.receipt_kwargs: list[dict[str, object]] = []

    def get_transaction_receipt(self, _network: str, _txid: str, **kwargs):
        self.receipt_kwargs.append(dict(kwargs))
        return None

    @staticmethod
    def _int_from_hex(value) -> int:
        return int(str(value), 16)


class OnchainConfirmationPollTests(unittest.TestCase):
    def test_relay_confirmation_uses_configured_runtime_poll_interval(self) -> None:
        owner = _ConfirmationOwner(0.5)

        with self.assertRaisesRegex(RuntimeError, "任务已停止"):
            _wait_for_transaction_success(owner, "BSC", "0xabc", label="确认", timeout_seconds=10)

        self.assertEqual(owner.stop_requested.wait_calls, [0.5])

    def test_invalid_runtime_poll_interval_falls_back_to_default(self) -> None:
        owner = _ConfirmationOwner("nan")

        self.assertEqual(_relay_poll_interval_seconds(owner), ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT)

    def test_relay_confirmation_receipt_poll_uses_bounded_rpc_timeout(self) -> None:
        owner = _ConfirmationOwner(0.5)
        owner.client = _RecordingReceiptClient()

        with self.assertRaisesRegex(RuntimeError, "任务已停止"):
            _wait_for_transaction_success(owner, "BSC", "0xabc", label="确认", timeout_seconds=10)

        self.assertEqual(len(owner.client.receipt_kwargs), 1)
        kwargs = owner.client.receipt_kwargs[0]
        self.assertEqual(kwargs.get("max_attempts"), 1)
        self.assertLessEqual(float(kwargs.get("read_timeout", 99)), 3.0)
        self.assertLessEqual(float(kwargs.get("connect_timeout", 99)), float(kwargs.get("read_timeout", 0)))

    def test_relay_recovery_treats_rpc_transport_errors_as_retryable(self) -> None:
        self.assertTrue(_relay_is_transient_rpc_error(RuntimeError("BSC RPC 请求失败：Read timed out")))
        self.assertTrue(_relay_is_transient_rpc_error(RuntimeError("SSLEOFError EOF occurred in violation of protocol")))
        self.assertFalse(_relay_is_transient_rpc_error(RuntimeError("insufficient funds for gas * price + value")))
        self.assertFalse(_relay_is_transient_rpc_error(RuntimeError("nonce too low")))

    def test_relay_record_retry_attempts_default_to_ten(self) -> None:
        self.assertEqual(RELAY_RECOVERY_RECORD_RETRY_ATTEMPTS, 10)


if __name__ == "__main__":
    unittest.main()
