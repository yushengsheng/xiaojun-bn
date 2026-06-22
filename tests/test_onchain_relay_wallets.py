#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core_models import EvmToken
import onchain_relay_wallets as relay_wallets_module
from onchain_relay_wallets import RelayWalletFileStore, RelayWalletRecord
from secret_box import SecretBox


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
    def setUp(self) -> None:
        self._secret_tmpdir = tempfile.TemporaryDirectory(prefix="xiaojun-relay-secret-test-")
        self._secret_box_patch = patch.object(
            relay_wallets_module,
            "SECRET_BOX",
            SecretBox(Path(self._secret_tmpdir.name) / ".secret.key"),
        )
        self._secret_box_patch.start()

    def tearDown(self) -> None:
        self._secret_box_patch.stop()
        self._secret_tmpdir.cleanup()

    def test_relay_wallet_file_encrypts_private_key_at_rest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            store = RelayWalletFileStore(Path(tmpdir) / "relay.txt")
            private_key = "1" * 64
            record = _expired_record(1)
            record.private_key = private_key

            store.append_records([record])

            raw_text = store.file_path.read_text(encoding="utf-8")
            self.assertNotIn(private_key, raw_text)
            self.assertIn("enc::", raw_text)
            loaded = store.load_records()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].private_key, private_key)

    def test_relay_wallet_file_rewrites_legacy_plain_private_key(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            path = Path(tmpdir) / "relay.txt"
            private_key = "2" * 64
            record = _expired_record(2)
            record.private_key = private_key
            path.write_text(json.dumps(record.__dict__, ensure_ascii=False) + "\n", encoding="utf-8")
            store = RelayWalletFileStore(path)

            rewritten = store.rewrite_encrypted()

            self.assertEqual(rewritten, 1)
            raw_text = path.read_text(encoding="utf-8")
            self.assertNotIn(private_key, raw_text)
            self.assertIn("enc::", raw_text)
            self.assertEqual(store.load_records()[0].private_key, private_key)

    def test_relay_wallet_file_recovers_from_authenticated_backup(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            path = Path(tmpdir) / "relay.txt"
            store = RelayWalletFileStore(path)
            record = _expired_record(4)
            record.private_key = "4" * 64
            store.append_records([record])
            backup_path = path.with_name(f"{path.name}.bak")
            self.assertTrue(backup_path.exists())
            path.write_text("tampered", encoding="utf-8")

            loaded = store.load_records()

            self.assertEqual(loaded[0].private_key, "4" * 64)
            self.assertEqual(path.read_text(encoding="utf-8"), backup_path.read_text(encoding="utf-8"))

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
