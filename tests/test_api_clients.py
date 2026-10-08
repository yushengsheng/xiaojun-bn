#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import requests
from api_clients import EvmClient
from core_models import GeneratedWalletEntry


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


class EvmClientWalletGenerationTests(unittest.TestCase):
    def test_accepts_9999_wallets(self) -> None:
        client = EvmClient()
        wallet = GeneratedWalletEntry(address="0x" + "1" * 40, private_key="2" * 64)

        with patch.object(client, "create_wallet", return_value=wallet) as create_wallet:
            wallets = client.create_wallets(9999, worker_threads=1)

        self.assertEqual(len(wallets), 9999)
        self.assertEqual(create_wallet.call_count, 9999)

    def test_rejects_10000_wallets(self) -> None:
        client = EvmClient()

        with self.assertRaisesRegex(RuntimeError, "单次最多创建 9999 个钱包"):
            client.create_wallets(10000, worker_threads=1)


class EvmClientSessionLifecycleTests(unittest.TestCase):
    class _Session:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def test_short_lived_workers_release_every_rpc_session(self) -> None:
        client = EvmClient(allow_system_proxy=False)
        sessions = []
        sessions_lock = threading.Lock()

        def build_session(*_args, **_kwargs):
            session = self._Session()
            with sessions_lock:
                sessions.append(session)
            return session

        def worker():
            try:
                client._rpc_session("https://rpc.example")
            finally:
                client.release_current_thread_rpc_session()

        with patch.object(client, "_build_rpc_session", side_effect=build_session):
            for _round in range(20):
                threads = [threading.Thread(target=worker) for _ in range(20)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

        self.assertEqual(len(sessions), 400)
        self.assertTrue(all(session.closed for session in sessions))
        self.assertEqual(client._rpc_sessions, {})

    def test_next_rpc_prunes_sessions_left_by_dead_threads(self) -> None:
        client = EvmClient(allow_system_proxy=False)
        sessions = []

        def build_session(*_args, **_kwargs):
            session = self._Session()
            sessions.append(session)
            return session

        with patch.object(client, "_build_rpc_session", side_effect=build_session):
            worker = threading.Thread(target=lambda: client._rpc_session("https://old.example"))
            worker.start()
            worker.join()
            self.assertEqual(len(client._rpc_sessions), 1)
            client._rpc_session("https://new.example")

        self.assertTrue(sessions[0].closed)
        client.release_current_thread_rpc_session()
        self.assertTrue(sessions[1].closed)
        self.assertEqual(client._rpc_sessions, {})


class EvmClientGasPriceTests(unittest.TestCase):
    @staticmethod
    def _history(tips: list[int], next_base: int = 0) -> dict:
        return {
            "reward": [[hex(tip)] for tip in tips],
            "baseFeePerGas": ["0x0"] * len(tips) + [hex(next_base)],
        }

    def test_dynamic_quote_covers_congestion_base_fee_and_outliers(self) -> None:
        large_tip = 2**60 + 1
        cases = [
            ("BSC", 50_000_000, [100_000_000] * 20, 0, 110_000_000),
            ("ETH", 2_000_000_000, [1_000_000_000] * 20, 3_000_000_000, 4_400_000_000),
            ("BSC", 200_000_000, [100_000_000] * 20, 0, 200_000_000),
            ("BSC", 1, [100] * 19 + [10**18], 0, 110),
            ("BSC", 1, [0, 100, 100, 100, 0], 0, 110),
            ("BSC", 1, [1, 2, 3, 4], 0, 4),
            ("ETH", 1, [large_tip] * 3, 1, ((large_tip + 1) * 110 + 99) // 100),
        ]
        for network, node_price, tips, base, expected in cases:
            with self.subTest(network=network, node_price=node_price, tips=tips):
                client = EvmClient()
                with patch.object(client, "_rpc_call", side_effect=[hex(node_price), self._history(tips, base)]) as rpc:
                    self.assertEqual(client.get_gas_price_wei(network), expected)
                self.assertEqual(rpc.call_count, 2)
                self.assertEqual(rpc.call_args.args, (network, "eth_feeHistory", ["0x14", "latest", [50]]))
                self.assertEqual(rpc.call_args.kwargs, {"timeout_seconds": 8.0})

    def test_invalid_or_unavailable_history_falls_back_to_positive_node_quote(self) -> None:
        malformed = [
            None, "0x1", {}, self._history([]), self._history([1, 2]),
            self._history([0] * 20), self._history([1] * 21),
            {"reward": [["0x1"]] * 3, "baseFeePerGas": ["0x0"] * 3},
            {"reward": [["0x1", "0x2"]] * 3, "baseFeePerGas": ["0x0"] * 4},
            {"reward": [[None]] * 3, "baseFeePerGas": ["0x0"] * 4},
            {"reward": [["-1"]] * 3, "baseFeePerGas": ["0x0"] * 4},
            {"reward": [["0x1"]] * 3, "baseFeePerGas": ["0x0"] * 3 + [None]},
            RuntimeError("method not supported"), requests.exceptions.ReadTimeout("slow RPC"),
        ]
        for history in malformed:
            with self.subTest(history=history):
                client = EvmClient()
                with (
                    patch.object(client, "_rpc_call", side_effect=["0x2faf080", history]),
                    self.assertLogs("bot", level="WARNING") as logs,
                ):
                    self.assertEqual(client.get_gas_price_wei("BSC"), 50_000_000)
                self.assertIn("回退节点建议价", logs.output[0])

    def test_nonpositive_node_quote_is_not_sent_or_cached(self) -> None:
        for result in (None, "0x0", "-1"):
            with self.subTest(result=result):
                client = EvmClient()
                with patch.object(client, "_rpc_call", return_value=result), self.assertRaisesRegex(RuntimeError, "gasPrice"):
                    client.get_gas_price_wei("BSC")
                self.assertEqual(client._gas_price_cache, {})

    def test_quote_cache_expires_and_is_separate_for_each_network(self) -> None:
        client = EvmClient()
        now = [10.0]
        history = self._history([100] * 20)
        with (
            patch("api_clients.time.monotonic", side_effect=lambda: now[0]),
            patch.object(client, "_rpc_call", side_effect=["0x1", history, "0x1", history, "0xc8", history]) as rpc,
        ):
            self.assertEqual(client.get_gas_price_wei("bsc"), 110)
            now[0] = 12.9
            self.assertEqual(client.get_gas_price_wei("BSC"), 110)
            self.assertEqual(rpc.call_count, 2)
            self.assertEqual(client.get_gas_price_wei("ETH"), 110)
            self.assertEqual(rpc.call_count, 4)
            now[0] = 13.0
            self.assertEqual(client.get_gas_price_wei("BSC"), 200)
            self.assertEqual(rpc.call_count, 6)

    def test_expired_quote_is_not_reused_if_refresh_fails(self) -> None:
        client = EvmClient()
        client._gas_price_cache["BSC"] = (1.0, 100)
        with (
            patch("api_clients.time.monotonic", return_value=10.0),
            patch.object(client, "_rpc_call", side_effect=RuntimeError("RPC unavailable")),
            self.assertRaisesRegex(RuntimeError, "RPC unavailable"),
        ):
            client.get_gas_price_wei("BSC")

    def test_concurrent_wallets_share_one_quote_refresh(self) -> None:
        client = EvmClient()
        started = threading.Event()
        release = threading.Event()

        def rpc(_network, method, _params, **_kwargs):
            if method == "eth_gasPrice":
                started.set()
                if not release.wait(timeout=3):
                    raise AssertionError("quote release timed out")
                return "0x1"
            return self._history([100] * 20)

        with patch.object(client, "_rpc_call", side_effect=rpc) as mocked_rpc, ThreadPoolExecutor(max_workers=8) as executor:
            jobs = [executor.submit(client.get_gas_price_wei, "BSC") for _ in range(8)]
            try:
                self.assertTrue(started.wait(timeout=3))
            finally:
                release.set()
            self.assertEqual([job.result(timeout=3) for job in jobs], [110] * 8)
        self.assertEqual(mocked_rpc.call_count, 2)

    def test_fee_history_rpc_error_switches_to_backup_node(self) -> None:
        client = EvmClient()
        urls = ["https://first.example", "https://backup.example"]
        session = Mock()

        def post(url, **kwargs):
            method = json.loads(kwargs["data"])["method"]
            if method == "eth_gasPrice":
                result = {"result": "0x2faf080"}
            elif url == urls[0]:
                result = {"error": {"code": -32601, "message": "method not supported"}}
            else:
                result = {"result": self._history([100_000_000] * 20)}
            return Mock(status_code=200, text=json.dumps(result))

        session.post.side_effect = post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.sleep") as sleep,
        ):
            self.assertEqual(client.get_gas_price_wei("BSC"), 110_000_000)
        self.assertEqual([call.args[0] for call in session.post.call_args_list], [urls[0], *urls])
        self.assertEqual(client._preferred_rpc_urls["BSC"], urls[1])
        sleep.assert_not_called()

    def test_confirmed_nonce_uses_latest_block(self) -> None:
        client = EvmClient()
        with (
            patch.object(client, "is_address", return_value=True),
            patch.object(client, "_rpc_call", return_value="0x8") as rpc_call,
        ):
            nonce = client.get_confirmed_nonce("BSC", "0x" + "1" * 40)

        self.assertEqual(nonce, 8)
        rpc_call.assert_called_once_with(
            "BSC",
            "eth_getTransactionCount",
            ["0x" + "1" * 40, "latest"],
        )

    def test_invalid_preferred_gas_price_falls_back_to_next_rpc(self) -> None:
        client = EvmClient()
        bad_url = "https://bad.example"
        good_url = "https://good.example"
        client._preferred_rpc_urls["BSC"] = bad_url
        session = Mock()

        def post(url, **_kwargs):
            response = Mock(status_code=200)
            response.text = '{"jsonrpc":"2.0","id":1,"result":"0x0"}' if url == bad_url else (
                '{"jsonrpc":"2.0","id":1,"result":"0x2faf080"}'
            )
            return response

        session.post.side_effect = post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": [bad_url, good_url]}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.sleep"),
            self.assertLogs("bot", level="WARNING"),
        ):
            gas_price = client.get_gas_price_wei("BSC")

        self.assertEqual(gas_price, 50_000_000)
        self.assertEqual([call.args[0] for call in session.post.call_args_list], [bad_url, good_url, good_url])
        self.assertEqual(client._preferred_rpc_urls["BSC"], good_url)


class EvmClientReceiptFallbackTests(unittest.TestCase):
    def test_slow_node_leaves_time_for_backup_within_shared_timeout(self) -> None:
        client = EvmClient()
        urls = ["https://slow.example", "https://synced.example"]
        now = [0.0]
        session = Mock()

        def post(url, **kwargs):
            if url == urls[0]:
                self.assertEqual(kwargs["timeout"].total, 5)
                now[0] += kwargs["timeout"].total
                raise requests.exceptions.ReadTimeout("slow node")
            self.assertEqual(kwargs["timeout"].total, 5)
            return Mock(status_code=200, text=json.dumps({"result":{"status":"0x1", "transactionHash":"0x"+"a"*64}}))

        session.post.side_effect = post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.monotonic", side_effect=lambda: now[0]),
            patch("api_clients.time.sleep") as sleep,
        ):
            self.assertEqual(client.get_transaction_receipt("BSC", "0x" + "a" * 64, timeout_seconds=10),
                             {"status": "0x1", "transactionHash": "0x" + "a" * 64})
        self.assertEqual([call.args[0] for call in session.post.call_args_list], urls)
        sleep.assert_not_called()

        # 其他 RPC 查询改写通用首选节点后，回执查询仍直达已验证有效的节点。
        client._preferred_rpc_urls["BSC"] = urls[0]
        session.post.reset_mock()
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.monotonic", side_effect=lambda: now[0]),
        ):
            self.assertEqual(client.get_transaction_receipt("BSC", "0x" + "a" * 64, timeout_seconds=10),
                             {"status": "0x1", "transactionHash": "0x" + "a" * 64})
        self.assertEqual([call.args[0] for call in session.post.call_args_list], [urls[1]])

    def test_recently_failed_receipt_node_is_deprioritized_until_cooldown_expires(self) -> None:
        client = EvmClient()
        urls = ["https://slow.example", "https://pending.example"]
        client._preferred_rpc_urls["BSC"] = urls[0]
        now = [0.0]
        session = Mock()

        def first_post(url, **_kwargs):
            if url == urls[0]:
                raise requests.exceptions.ReadTimeout("slow node")
            return Mock(status_code=200, text='{"result":null}')

        session.post.side_effect = first_post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.monotonic", side_effect=lambda: now[0]),
        ):
            self.assertIsNone(client.get_transaction_receipt("BSC", "0x" + "a" * 64))
            session.post.reset_mock()
            session.post.side_effect = None
            session.post.return_value = Mock(status_code=200, text='{"result":null}')
            self.assertIsNone(client.get_transaction_receipt("BSC", "0x" + "a" * 64))
            self.assertEqual([call.args[0] for call in session.post.call_args_list], list(reversed(urls)))
            session.post.reset_mock()
            now[0] = 31.0
            self.assertIsNone(client.get_transaction_receipt("BSC", "0x" + "a" * 64))
            self.assertEqual([call.args[0] for call in session.post.call_args_list], urls)

    def test_no_additional_node_request_after_deadline(self) -> None:
        client = EvmClient()
        now = [0.0]
        session = Mock()

        def post(*_args, **_kwargs):
            now[0] = 10
            raise requests.exceptions.ReadTimeout("timeout")

        session.post.side_effect = post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": ["https://first.example", "https://second.example"]}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.monotonic", side_effect=lambda: now[0]),
            self.assertRaisesRegex(RuntimeError, "RPC 查询超时"),
        ):
            client.get_transaction_receipt("BSC", "0x" + "a" * 64, timeout_seconds=10)
        session.post.assert_called_once()

    def test_stop_during_node_request_prevents_backup_requests(self) -> None:
        client = EvmClient()
        stop_event = threading.Event()
        session = Mock()

        def post(*_args, **_kwargs):
            stop_event.set()
            return Mock(status_code=200, text='{"result":null}')

        session.post.side_effect = post
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": ["https://first.example", "https://second.example"]}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            self.assertRaisesRegex(RuntimeError, "任务已停止"),
        ):
            client.get_transaction_receipt("BSC", "0x" + "a" * 64, timeout_seconds=10, stop_event=stop_event)
        session.post.assert_called_once()

    def test_empty_receipt_tries_other_node_without_retrying_stale_node(self) -> None:
        client = EvmClient()
        urls = ["https://stale.example", "https://synced.example"]
        client._preferred_rpc_urls["BSC"] = urls[0]
        receipt = {"status": "0x1", "transactionHash": "0x" + "a" * 64}
        session = Mock()
        session.post.side_effect = [
            Mock(status_code=200, text=json.dumps({"result": None})),
            Mock(status_code=200, text=json.dumps({"result": receipt})),
        ]
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
            patch("api_clients.time.sleep") as sleep,
        ):
            result = client.get_transaction_receipt("BSC", receipt["transactionHash"])
        self.assertEqual(result, receipt)
        self.assertEqual([call.args[0] for call in session.post.call_args_list], urls)
        self.assertEqual(client._preferred_rpc_urls["BSC"], urls[1])
        sleep.assert_not_called()

    def test_all_nodes_pending_returns_none_and_keeps_preferred_node(self) -> None:
        client = EvmClient()
        urls = ["https://first.example", "https://second.example"]
        client._preferred_rpc_urls["BSC"] = urls[0]
        session = Mock()
        session.post.return_value = Mock(status_code=200, text='{"result":null}')
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": urls}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
        ):
            self.assertIsNone(client.get_transaction_receipt("BSC", "0x" + "a" * 64))
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(client._preferred_rpc_urls["BSC"], urls[0])

    def test_receipt_status_zero_is_returned_without_fallback(self) -> None:
        client = EvmClient()
        session = Mock()
        session.post.return_value = Mock(status_code=200, text=json.dumps({"result":{"status":"0x0", "transactionHash":"0x"+"a"*64}}))
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": ["https://first.example", "https://second.example"]}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
        ):
            self.assertEqual(client.get_transaction_receipt("BSC", "0x" + "a" * 64), {"status": "0x0", "transactionHash": "0x" + "a" * 64})
        self.assertEqual(session.post.call_count, 1)

    def test_other_rpc_methods_keep_existing_null_behavior(self) -> None:
        client = EvmClient()
        session = Mock()
        session.post.return_value = Mock(status_code=200, text='{"result":null}')
        with (
            patch.object(client, "_network_info", return_value={"rpc_urls": ["https://first.example", "https://second.example"]}),
            patch.object(client, "_rpc_session", return_value=(session, "")),
        ):
            self.assertIsNone(client.get_transaction_by_hash("BSC", "0x" + "a" * 64))
        self.assertEqual(session.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
