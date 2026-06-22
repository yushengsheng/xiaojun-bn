#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app_defaults import (
    EXCHANGE_WORKER_THREADS_DEFAULT,
    ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
    ONCHAIN_DELAY_SECONDS_DEFAULT,
    ONCHAIN_WORKER_THREADS_DEFAULT,
)
from core_models import OnchainSettings
from secret_box import SecretBox
import stores as stores_module
from stores import AccountStore, OnchainStore, _load_authenticated_config


class StoreDefaultTests(unittest.TestCase):
    def setUp(self) -> None:
        self._secret_tmpdir = tempfile.TemporaryDirectory(prefix="xiaojun-store-secret-test-")
        self.secret_box = SecretBox(Path(self._secret_tmpdir.name) / ".secret.key")
        self._secret_box_patch = patch.object(stores_module, "SECRET_BOX", self.secret_box)
        self._secret_box_patch.start()

    def tearDown(self) -> None:
        self._secret_box_patch.stop()
        self._secret_tmpdir.cleanup()

    def test_account_store_uses_central_defaults_when_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = AccountStore(Path(tmpdir) / "accounts.json")
            store.load()
            self.assertEqual(store.settings.worker_threads, EXCHANGE_WORKER_THREADS_DEFAULT)
            self.assertEqual(store.settings.delay_seconds, ONCHAIN_DELAY_SECONDS_DEFAULT)

    def test_account_store_recovers_when_only_backup_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "accounts.json"
            source = AccountStore(path)
            source.accounts = [
                stores_module.AccountEntry(
                    api_key="account-key",
                    api_secret="account-secret",
                    address="account-address",
                    network="BSC",
                )
            ]
            source.save()
            path.unlink()

            recovered = AccountStore(path)
            recovered.load()

            self.assertTrue(path.exists())
            self.assertEqual(len(recovered.accounts), 1)
            self.assertEqual(recovered.accounts[0].api_key, "account-key")

    def test_onchain_store_uses_central_defaults_when_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = OnchainStore(Path(tmpdir) / "onchain.json")
            store.load()
            self.assertEqual(store.settings.worker_threads, ONCHAIN_WORKER_THREADS_DEFAULT)
            self.assertEqual(store.settings.delay_seconds, ONCHAIN_DELAY_SECONDS_DEFAULT)
            self.assertEqual(store.settings.confirm_timeout_seconds, ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT)

    def test_onchain_store_recovers_when_only_backup_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "onchain.json"
            source = OnchainStore(path)
            source.many_to_one_sources = ["source-private-key"]
            source.settings = OnchainSettings(many_to_one_target="0x0000000000000000000000000000000000000001")
            source.save()
            path.unlink()

            recovered = OnchainStore(path)
            recovered.load()

            self.assertTrue(path.exists())
            self.assertEqual(recovered.many_to_one_sources, ["source-private-key"])
            self.assertEqual(recovered.settings.many_to_one_target, "0x0000000000000000000000000000000000000001")

    def test_onchain_store_repairs_invalid_runtime_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "onchain.json"
            path.write_text(
                '{"settings":{"worker_threads":"bad","delay_seconds":-5,"confirm_timeout_seconds":0}}',
                encoding="utf-8",
            )
            store = OnchainStore(path)
            store.load()
            self.assertEqual(store.settings.worker_threads, ONCHAIN_WORKER_THREADS_DEFAULT)
            self.assertEqual(store.settings.delay_seconds, 0.0)
            self.assertEqual(store.settings.confirm_timeout_seconds, ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")).get("format"), "xiaojun-authenticated-config-v1")

    def test_onchain_store_does_not_persist_ephemeral_one_to_many_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "onchain.json"
            store = OnchainStore(path)
            store.settings = OnchainSettings(
                one_to_many_source="private-key-for-this-run",
                one_to_many_source_ephemeral=True,
            )
            store.one_to_many_addresses = ["0x0000000000000000000000000000000000000001"]

            store.save()

            raw_text = path.read_text(encoding="utf-8")
            raw = json.loads(raw_text)
            self.assertNotIn("private-key-for-this-run", raw_text)
            self.assertEqual(raw.get("format"), "xiaojun-authenticated-config-v1")
            payload, _recovered = _load_authenticated_config(path)
            self.assertEqual(payload["settings"]["one_to_many_source"], "")
            self.assertTrue(payload["settings"]["one_to_many_source_ephemeral"])
            self.assertNotIn("source", payload["one_to_many"])

            loaded = OnchainStore(path)
            loaded.load()
            self.assertTrue(loaded.settings.one_to_many_source_ephemeral)
            self.assertEqual(loaded.settings.one_to_many_source, "")

    def test_onchain_store_ignores_legacy_one_to_many_source_when_ephemeral(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "onchain.json"
            path.write_text(
                json.dumps(
                    {
                        "settings": {
                            "one_to_many_source": self.secret_box.encrypt("settings-private-key"),
                            "one_to_many_source_ephemeral": True,
                        },
                        "one_to_many": {
                            "source": self.secret_box.encrypt("legacy-private-key"),
                            "addresses": [],
                        },
                    }
                ),
                encoding="utf-8",
            )

            store = OnchainStore(path)
            store.load()

            self.assertTrue(store.settings.one_to_many_source_ephemeral)
            self.assertEqual(store.settings.one_to_many_source, "")

    def test_onchain_store_save_settings_only_replaces_legacy_one_to_many_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "onchain.json"
            path.write_text(
                json.dumps(
                    {
                        "settings": {},
                        "one_to_many": {
                            "source": self.secret_box.encrypt("old-private-key"),
                            "addresses": [],
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = OnchainStore(path)
            store.settings = OnchainSettings(one_to_many_source="new-private-key")

            store.save_settings_only()

            loaded = OnchainStore(path)
            loaded.load()
            self.assertEqual(loaded.settings.one_to_many_source, "new-private-key")
            self.assertFalse(loaded.settings.one_to_many_source_ephemeral)

    def test_authenticated_config_rejects_tampering_and_file_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            onchain_path = root / "onchain.json"
            replay_path = root / "accounts.json"
            store = OnchainStore(onchain_path)
            store.settings = OnchainSettings(many_to_one_target="0x0000000000000000000000000000000000000001")
            store.many_to_one_sources = ["private-key-for-test"]
            store.save()

            encrypted = json.loads(onchain_path.read_text(encoding="utf-8"))
            replay_path.write_text(json.dumps(encrypted), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                AccountStore(replay_path).load()

            ciphertext = str(encrypted["ciphertext"])
            index = len(ciphertext) // 2
            encrypted["ciphertext"] = (
                ciphertext[:index] + ("A" if ciphertext[index] != "A" else "B") + ciphertext[index + 1 :]
            )
            onchain_path.write_text(json.dumps(encrypted), encoding="utf-8")
            onchain_path.with_name(f"{onchain_path.name}.bak").write_text(json.dumps(encrypted), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                OnchainStore(onchain_path).load()


if __name__ == "__main__":
    unittest.main()
