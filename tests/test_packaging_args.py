#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from tools.pyinstaller_common_args import HIDDEN_IMPORTS, common_pyinstaller_args


class PyInstallerArgsTests(unittest.TestCase):
    def test_hidden_imports_include_split_selftest_modules(self) -> None:
        required = {
            "app_defaults",
            "input_validation",
            "secure_config",
            "operation_journal",
            "exchange_order_recovery",
            "exchange_account_destruction",
            "secure_delete",
            "selftest_basic_checks",
            "selftest_gui_checks",
            "selftest_online_checks",
        }
        self.assertTrue(required.issubset(set(HIDDEN_IMPORTS)))

    def test_common_args_are_flat_and_stable(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-packaging-test-") as tmpdir:
            version_file = Path(tmpdir) / "version.txt"
            version_file.write_text("v-test", encoding="utf-8")
            args = common_pyinstaller_args(version_file=version_file)
        self.assertEqual(len(args), len([item for item in args if isinstance(item, str)]))
        self.assertEqual(args.count("--hidden-import"), len(HIDDEN_IMPORTS))
        self.assertEqual(len(HIDDEN_IMPORTS), len(set(HIDDEN_IMPORTS)))
        self.assertIn(f"{version_file}{os.pathsep}.", args)

    def test_common_args_skip_missing_optional_version_file(self) -> None:
        args = common_pyinstaller_args(version_file=Path("/missing/version.txt"))
        self.assertNotIn("--add-data", args)


if __name__ == "__main__":
    unittest.main()
