#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
