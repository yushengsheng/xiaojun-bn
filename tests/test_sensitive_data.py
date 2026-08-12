#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

from secret_box import atomic_write_private_text
from sensitive_data import redact_sensitive_text


class SensitiveDataTests(unittest.TestCase):
    def test_redacts_bound_v3_ciphertext(self) -> None:
        cipher = "enc::v3::abcdefghijklmnop.abcdefghijklmnopqrstuvwxyz"

        redacted = redact_sensitive_text(f"payload={cipher}")

        self.assertNotIn(cipher, redacted)

    @unittest.skipIf(os.name == "nt", "Windows chmod does not provide POSIX owner-only mode")
    def test_private_text_write_uses_owner_only_permissions(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-private-export-test-") as tmpdir:
            path = Path(tmpdir) / "wallets.txt"

            atomic_write_private_text(path, "private-value\n")

            self.assertEqual(path.read_text(encoding="utf-8"), "private-value\n")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
