#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import onchain_relay_wallets as relay_wallets_module
from onchain_relay_wallets import RelayWalletFileStore, RelayWalletRecord
from secret_box import SecretBox


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

    def test_relay_wallet_file_skips_rewrite_when_already_authenticated(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            path = Path(tmpdir) / "relay.txt"
            store = RelayWalletFileStore(path)
            record = _expired_record(5)
            record.private_key = "5" * 64
            store.append_records([record])
            before = path.read_text(encoding="utf-8")

            rewritten = store.rewrite_encrypted_if_needed()

            self.assertEqual(rewritten, 0)
            self.assertEqual(path.read_text(encoding="utf-8"), before)

    def test_deferred_updates_flush_once_after_stage(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            path = Path(tmpdir) / "relay.txt"
            store = RelayWalletFileStore(path)
            record = _expired_record(6)
            record.private_key = "6" * 64
            store.append_records([record])
            before = path.read_text(encoding="utf-8")

            with store.defer_writes():
                updated = store.update_record(record.relay_address, batch_id=record.batch_id, status="submitted")
                self.assertEqual(updated.status, "submitted")
                self.assertEqual(path.read_text(encoding="utf-8"), before)

            self.assertEqual(store.load_records()[0].status, "submitted")
            self.assertNotEqual(path.read_text(encoding="utf-8"), before)

    def test_remove_records_only_removes_confirmed_keys(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-relay-wallet-secret-test-") as tmpdir:
            path = Path(tmpdir) / "relay.txt"
            store = RelayWalletFileStore(path)
            remove_record = _expired_record(7)
            keep_same_batch = _expired_record(8)
            keep_same_address = _expired_record(7)
            keep_same_address.batch_id = "relay-cleanup-progress-test-other"
            store.append_records([remove_record, keep_same_batch, keep_same_address])

            removed = store.remove_records({(remove_record.batch_id, remove_record.relay_address)})

            self.assertEqual(removed, 1)
            remaining = store.load_records()
            remaining_keys = {(item.batch_id, item.relay_address) for item in remaining}
            self.assertEqual(len(remaining), 2)
            self.assertNotIn((remove_record.batch_id, remove_record.relay_address), remaining_keys)
            self.assertIn((keep_same_batch.batch_id, keep_same_batch.relay_address), remaining_keys)
            self.assertIn((keep_same_address.batch_id, keep_same_address.relay_address), remaining_keys)

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

if __name__ == "__main__":
    unittest.main()
