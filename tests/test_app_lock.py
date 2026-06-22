#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app_lock import AppLockManager
from secret_box import SecretBox


class AppLockManagerTests(unittest.TestCase):
    def test_password_wraps_secret_box_key(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-app-lock-test-") as tmpdir:
            root = Path(tmpdir)
            box = SecretBox(root / ".secret.key")
            manager = AppLockManager(root / "app_lock.json", box)
            cipher = box.encrypt("sensitive-value")

            manager.setup_password("strong-password")

            self.assertTrue(manager.is_configured())
            self.assertTrue((root / ".secret.key").read_text(encoding="utf-8").startswith(SecretBox.LOCKED_MARKER))
            box.clear_cached_key()
            with self.assertRaises(RuntimeError):
                box.decrypt(cipher)

            with self.assertRaises(RuntimeError):
                manager.unlock("wrong-password")
            manager.unlock("strong-password")
            self.assertEqual(box.decrypt(cipher), "sensitive-value")

    def test_change_password_rewraps_existing_key(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-app-lock-test-") as tmpdir:
            root = Path(tmpdir)
            box = SecretBox(root / ".secret.key")
            manager = AppLockManager(root / "app_lock.json", box, scrypt_n=2**12)
            cipher = box.encrypt("sensitive-value")
            manager.setup_password("old-password")

            manager.change_password("old-password", "new-password")

            box.clear_cached_key()
            with self.assertRaises(RuntimeError):
                manager.unlock("old-password")
            manager.unlock("new-password")
            self.assertEqual(box.decrypt(cipher), "sensitive-value")

    def test_unlock_recovers_from_password_config_backup(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-app-lock-test-") as tmpdir:
            root = Path(tmpdir)
            key_file = root / ".secret.key"
            lock_file = root / "app_lock.json"
            first = AppLockManager(lock_file, SecretBox(key_file), scrypt_n=2**10)
            first.setup_password("password-123")
            expected_key = first.secret_box.export_raw_key()
            backup_path = lock_file.with_name(f"{lock_file.name}.bak")
            self.assertTrue(backup_path.exists())
            lock_file.unlink()

            recovered = AppLockManager(lock_file, SecretBox(key_file), scrypt_n=2**10)
            recovered.unlock("password-123")

            self.assertTrue(lock_file.exists())
            self.assertEqual(recovered.secret_box.export_raw_key(), expected_key)

    def test_env_key_cannot_bypass_configured_password(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-app-lock-test-") as tmpdir:
            root = Path(tmpdir)
            lock_file = root / "app_lock.json"
            encoded_key = base64.urlsafe_b64encode(b"k" * 32).decode("ascii")
            with patch("secret_box.APP_LOCK_FILE", lock_file), patch(
                "secret_box.APP_LOCK_BACKUP_FILE", lock_file.with_name(f"{lock_file.name}.bak")
            ), patch.dict(
                os.environ,
                {SecretBox.ENV_KEY: encoded_key},
                clear=False,
            ):
                box = SecretBox(root / ".secret.key")
                manager = AppLockManager(lock_file, box, scrypt_n=2**12)
                cipher = box.encrypt("sensitive-value")
                manager.setup_password("strong-password")

                box.clear_cached_key()
                with self.assertRaises(RuntimeError):
                    box.decrypt(cipher)
                manager.unlock("strong-password")
                self.assertEqual(box.decrypt(cipher), "sensitive-value")


if __name__ == "__main__":
    unittest.main()
