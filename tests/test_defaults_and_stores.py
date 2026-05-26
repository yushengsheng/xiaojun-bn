#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app_defaults import (
    EXCHANGE_WORKER_THREADS_DEFAULT,
    ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
    ONCHAIN_DELAY_SECONDS_DEFAULT,
    ONCHAIN_WORKER_THREADS_DEFAULT,
)
from stores import AccountStore, OnchainStore


class StoreDefaultTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
