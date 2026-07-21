#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

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


class EvmClientGasPriceTests(unittest.TestCase):
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
        ):
            gas_price = client.get_gas_price_wei("BSC")

        self.assertEqual(gas_price, 50_000_000)
        self.assertEqual([call.args[0] for call in session.post.call_args_list], [bad_url, bad_url, good_url])
        self.assertEqual(client._preferred_rpc_urls["BSC"], good_url)


if __name__ == "__main__":
    unittest.main()
