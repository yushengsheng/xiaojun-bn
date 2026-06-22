#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from tools.run_regression import _display_command


class RegressionRunnerTests(unittest.TestCase):
    def test_command_display_is_safe_for_legacy_ascii_consoles(self) -> None:
        rendered = _display_command(["python", "小军bn.py", "--selftest"])

        self.assertEqual(rendered, "python \\u5c0f\\u519bbn.py --selftest")
        self.assertTrue(rendered.isascii())


if __name__ == "__main__":
    unittest.main()
