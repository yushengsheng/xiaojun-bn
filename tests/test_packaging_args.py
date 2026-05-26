#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from tools.pyinstaller_common_args import HIDDEN_IMPORTS, common_pyinstaller_args


class PyInstallerArgsTests(unittest.TestCase):
    def test_hidden_imports_include_split_selftest_modules(self) -> None:
        required = {
            "app_defaults",
            "selftest_basic_checks",
            "selftest_gui_checks",
            "selftest_online_checks",
        }
        self.assertTrue(required.issubset(set(HIDDEN_IMPORTS)))

    def test_common_args_are_flat_and_stable(self) -> None:
        args = common_pyinstaller_args()
        self.assertEqual(len(args), len([item for item in args if isinstance(item, str)]))
        self.assertEqual(args.count("--hidden-import"), len(HIDDEN_IMPORTS))
        self.assertEqual(len(HIDDEN_IMPORTS), len(set(HIDDEN_IMPORTS)))
        self.assertIn("version.txt;.", args)


if __name__ == "__main__":
    unittest.main()
