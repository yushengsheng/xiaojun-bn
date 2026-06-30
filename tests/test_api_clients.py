#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from api_clients import EvmClient, call_with_transient_rpc_retries, is_transient_rpc_error


class _RecoveryTimeoutClient(EvmClient):
    def __init__(self):
        super().__init__()
        self.receipt_kwargs: list[dict[str, object]] = []
        self.tx_kwargs: list[dict[str, object]] = []
        self.nonce_kwargs: list[dict[str, object]] = []

    def get_transaction_receipt(self, _network: str, _tx_hash: str, **kwargs):
        self.receipt_kwargs.append(dict(kwargs))
        return None

    def get_transaction_by_hash(self, _network: str, _tx_hash: str, **kwargs):
        self.tx_kwargs.append(dict(kwargs))
        return None

    def get_nonce(self, _network: str, _address: str, **kwargs) -> int:
        self.nonce_kwargs.append(dict(kwargs))
        return 8


class EvmClientDependencyCommandTests(unittest.TestCase):
    def test_dependency_command_requires_hash_locked_file(self) -> None:
        command = EvmClient._dependency_install_command(["eth-account", "eth-utils"])
        self.assertIn("requirements.lock", command)
        self.assertIn("--require-hashes", command)
        self.assertNotIn("requirements.txt\"", command)

    def test_missing_lock_does_not_offer_unpinned_install(self) -> None:
        with patch("api_clients.os.path.exists", return_value=False):
            command = EvmClient._dependency_install_command(["eth-account"])
        self.assertIn("依赖锁文件", command)
        self.assertNotIn("pip install", command)

    def test_submission_recovery_rpc_checks_are_bounded(self) -> None:
        client = _RecoveryTimeoutClient()

        recovered, reason = client._recover_submitted_transaction(
            "BSC",
            "0x" + "a" * 64,
            source_address="0x00000000000000000000000000000000000000aa",
            nonce=7,
            recovery_timeout_seconds=5.0,
        )

        self.assertTrue(recovered)
        self.assertEqual(reason, "source-nonce")
        for kwargs in (client.receipt_kwargs[0], client.tx_kwargs[0], client.nonce_kwargs[0]):
            self.assertEqual(kwargs.get("max_attempts"), 1)
            self.assertLessEqual(float(kwargs.get("read_timeout", 99)), 3.0)
            self.assertLessEqual(float(kwargs.get("connect_timeout", 99)), float(kwargs.get("read_timeout", 0)))

    def test_default_rpc_calls_use_short_single_attempt_timeout(self) -> None:
        client = EvmClient()
        timeouts: list[tuple[float, float]] = []

        class _Session:
            def post(self, _url, **kwargs):
                timeouts.append(kwargs["timeout"])
                raise requests.exceptions.ReadTimeout("slow rpc")

        client._network_info = lambda _network: {"rpc_urls": ["https://rpc.example"]}  # type: ignore[method-assign]
        client._rpc_session = lambda _target_url="": (_Session(), "")  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "RPC 请求失败"):
            client.get_balance_wei("BSC", "0x00000000000000000000000000000000000000aa")

        self.assertEqual(timeouts, [(3, 3)])

    def test_rpc_order_prefers_better_recent_endpoint(self) -> None:
        client = EvmClient()
        urls = ["https://slow.example", "https://fast.example", "https://cold.example"]

        self.assertEqual(client._ordered_rpc_urls("BSC", urls), urls)

        client._remember_preferred_rpc_url("BSC", "https://slow.example", latency_seconds=2.0)
        client._remember_preferred_rpc_url("BSC", "https://fast.example", latency_seconds=0.1)

        ordered = client._ordered_rpc_urls("BSC", urls)
        self.assertEqual(ordered[0], "https://fast.example")

        client._record_rpc_failure("BSC", "https://fast.example")
        client._record_rpc_failure("BSC", "https://fast.example")

        ordered_after_failures = client._ordered_rpc_urls("BSC", urls)
        self.assertEqual(ordered_after_failures[0], "https://slow.example")

    def test_transient_rpc_retry_helper_retries_then_succeeds(self) -> None:
        calls = 0
        retries: list[tuple[int, int, str]] = []

        def action():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("BSC RPC 请求失败：Read timed out")
            return "ok"

        result = call_with_transient_rpc_retries(
            action,
            attempts=10,
            retry_delay_seconds=0,
            on_retry=lambda next_attempt, total, exc: retries.append((next_attempt, total, str(exc))),
        )

        self.assertEqual(result, "ok")
        self.assertEqual(calls, 3)
        self.assertEqual([item[0] for item in retries], [2, 3])
        self.assertTrue(all(item[1] == 10 for item in retries))

    def test_transient_rpc_retry_helper_does_not_retry_business_errors(self) -> None:
        calls = 0

        def action():
            nonlocal calls
            calls += 1
            raise RuntimeError("insufficient funds for gas * price + value")

        with self.assertRaisesRegex(RuntimeError, "insufficient funds"):
            call_with_transient_rpc_retries(action, attempts=10, retry_delay_seconds=0)

        self.assertEqual(calls, 1)

    def test_transient_rpc_classifier_covers_transport_errors(self) -> None:
        retryable_errors = [
            "BSC RPC 请求失败：Read timed out",
            "SSLEOFError EOF occurred in violation of protocol",
            "HTTPSConnectionPool Max retries exceeded",
            "Remote end closed connection without response",
            "RPC(https://rpc.example) HTTP 503 Service Unavailable",
            "RPC(https://rpc.example) HTTP 429 Too Many Requests",
        ]

        for message in retryable_errors:
            with self.subTest(message=message):
                self.assertTrue(is_transient_rpc_error(RuntimeError(message)))

    def test_transient_rpc_classifier_rejects_deterministic_business_errors(self) -> None:
        business_errors = [
            "insufficient funds for gas * price + value",
            "nonce too low",
            "replacement transaction underpriced",
            "execution reverted: transfer amount exceeds balance",
            "invalid sender",
            "intrinsic gas too low",
            "invalid params",
            "RPC(https://rpc.example) code=-32000 msg=insufficient funds for gas",
        ]

        for message in business_errors:
            with self.subTest(message=message):
                self.assertFalse(is_transient_rpc_error(RuntimeError(message)))


if __name__ == "__main__":
    unittest.main()
