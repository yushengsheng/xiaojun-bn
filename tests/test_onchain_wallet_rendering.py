#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import Mock

from core_models import GeneratedWalletEntry
from onchain_wallets import OnchainWalletMixin


class _FakeTree:
    def __init__(self) -> None:
        self.rows: dict[str, tuple[object, ...]] = {}
        self.pending: dict[str, object] = {}
        self.cancelled: list[str] = []
        self._next_token = 1

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows)

    def delete(self, *item_ids: str) -> None:
        for item_id in item_ids:
            self.rows.pop(item_id, None)

    def insert(self, _parent: str, _position: str, *, iid: str, values: tuple[object, ...]) -> None:
        self.rows[iid] = values

    def after_idle(self, callback) -> str:
        token = f"after-{self._next_token}"
        self._next_token += 1
        self.pending[token] = callback
        return token

    def after_cancel(self, token: str) -> None:
        self.cancelled.append(token)
        self.pending.pop(token, None)

    def run_pending(self) -> None:
        while self.pending:
            token = next(iter(self.pending))
            callback = self.pending.pop(token)
            callback()


class _WalletRenderOwner(OnchainWalletMixin):
    def __init__(self, wallets: list[GeneratedWalletEntry]) -> None:
        self.generated_wallets = wallets
        self.wallet_generator_tree = _FakeTree()
        self.wallet_generator_window = None
        self.wallet_generator_generate_btn = None
        self.logs: list[str] = []
        self.client = Mock()

    def log(self, message: str) -> None:
        self.logs.append(message)

    def _dispatch_ui(self, callback) -> None:
        callback()


def _wallet(index: int, prefix: str = "a") -> GeneratedWalletEntry:
    return GeneratedWalletEntry(address=f"0x{prefix}{index:04d}", private_key=f"key-{prefix}-{index:04d}")


class WalletTreeRenderingTests(unittest.TestCase):
    def test_wallet_tree_renders_in_order_across_idle_batches(self) -> None:
        batch_size = _WalletRenderOwner.WALLET_RENDER_BATCH_SIZE
        wallets = [_wallet(i) for i in range(9999)]
        owner = _WalletRenderOwner(wallets)

        owner._refresh_generated_wallet_tree()

        self.assertEqual(len(owner.wallet_generator_tree.rows), batch_size)
        self.assertEqual(len(owner.wallet_generator_tree.pending), 1)
        owner.wallet_generator_tree.run_pending()
        self.assertEqual(len(owner.wallet_generator_tree.rows), len(wallets))
        self.assertEqual(owner.wallet_generator_tree.rows["wallet_1"], (1, wallets[0].address, wallets[0].private_key))
        self.assertEqual(
            owner.wallet_generator_tree.rows[f"wallet_{len(wallets)}"],
            (len(wallets), wallets[-1].address, wallets[-1].private_key),
        )
        self.assertTrue(any(f"数量={len(wallets)}" in message and "耗时=" in message for message in owner.logs))
        self.assertFalse(any(wallets[0].private_key in message for message in owner.logs))

    def test_new_refresh_cancels_stale_wallet_rows(self) -> None:
        wallet_count = _WalletRenderOwner.WALLET_RENDER_BATCH_SIZE + 1
        owner = _WalletRenderOwner([_wallet(i) for i in range(wallet_count)])
        owner._refresh_generated_wallet_tree()
        self.assertEqual(len(owner.wallet_generator_tree.rows), wallet_count - 1)

        replacement = [_wallet(1, "b"), _wallet(2, "b")]
        owner.generated_wallets = replacement
        owner._refresh_generated_wallet_tree()
        owner.wallet_generator_tree.run_pending()

        self.assertEqual(tuple(owner.wallet_generator_tree.rows), ("wallet_1", "wallet_2"))
        self.assertEqual(owner.wallet_generator_tree.rows["wallet_1"][1], replacement[0].address)
        self.assertEqual(len(owner.wallet_generator_tree.cancelled), 1)

    def test_generation_timing_does_not_change_generated_wallet_data(self) -> None:
        wallets = [_wallet(1), _wallet(2)]
        owner = _WalletRenderOwner([])
        owner.client.create_wallets.return_value = wallets

        owner._run_generate_wallets(2, 4)

        self.assertIs(owner.generated_wallets, wallets)
        owner.client.create_wallets.assert_called_once_with(2, worker_threads=4)
        self.assertTrue(any("生成耗时=" in message for message in owner.logs))


if __name__ == "__main__":
    unittest.main()
