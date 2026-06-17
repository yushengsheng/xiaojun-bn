#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core_models import EvmToken
from onchain_relay_wallets import RelayWalletFileStore, RelayWalletRecord


class _FakeCleanupClient:
    def get_default_tokens(self, network: str) -> list[EvmToken]:
        return [
            EvmToken(symbol="BNB", contract="", decimals=18, is_native=True),
            EvmToken(symbol="USDT", contract="0x55d398326f99059ff775485246999027b3197955", decimals=18, is_native=False),
        ]

    def get_balance_wei(self, network: str, address: str) -> int:
        return 1 if address.endswith("3") else 0

    def get_erc20_balance(self, network: str, contract: str, address: str) -> int:
        return 0


def _expired_record(index: int) -> RelayWalletRecord:
    address = f"0x{index:040x}"
    return RelayWalletRecord(
        created_at="2000-01-01T00:00:00Z",
        expires_at="2000-01-04T00:00:00Z",
        network="BSC",
        source="0x0000000000000000000000000000000000000100",
        target="0x0000000000000000000000000000000000000200",
        relay_address=address,
        private_key="0" * 64,
        status="completed",
        batch_id="relay-cleanup-progress-test",
        token_symbol="BNB",
        token_is_native=True,
    )


class RelayWalletCleanupTests(unittest.TestCase):
    def test_cleanup_logs_balance_query_progress(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-cleanup-test-") as tmpdir:
            store = RelayWalletFileStore(Path(tmpdir) / "relay.txt")
            store.append_records([_expired_record(1), _expired_record(2), _expired_record(3)])
            logs: list[str] = []

            checked, removed, kept = store.cleanup_expired_empty_records(
                _FakeCleanupClient(),
                log=logs.append,
                worker_threads=2,
            )

            self.assertEqual((checked, removed, kept), (3, 2, 1))
            self.assertTrue(any("中转钱包过期清理开始：发现 3 条" in line for line in logs))
            self.assertTrue(any("中转钱包过期清理进度：余额查询 1/3" in line for line in logs))
            self.assertTrue(any("中转钱包过期清理进度：余额查询 3/3" in line for line in logs))


if __name__ == "__main__":
    unittest.main()
