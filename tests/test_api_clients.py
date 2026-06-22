#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

from api_clients import EvmClient


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


if __name__ == "__main__":
    unittest.main()
