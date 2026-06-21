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
from stores import AccountStore, OnchainStore


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

    def test_onchain_store_uses_central_defaults_when_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = OnchainStore(Path(tmpdir) / "onchain.json")
            store.load()
            self.assertEqual(store.settings.worker_threads, ONCHAIN_WORKER_THREADS_DEFAULT)
            self.assertEqual(store.settings.delay_seconds, ONCHAIN_DELAY_SECONDS_DEFAULT)
            self.assertEqual(store.settings.confirm_timeout_seconds, ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT)

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
            self.assertEqual(raw["settings"]["one_to_many_source"], "")
            self.assertTrue(raw["settings"]["one_to_many_source_ephemeral"])
            self.assertNotIn("source", raw["one_to_many"])

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


if __name__ == "__main__":
    unittest.main()
