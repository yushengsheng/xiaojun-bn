#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from selftest_basic_checks import run_basic_selftest_checks


class SelftestModuleTests(unittest.TestCase):
    def test_basic_selftest_module_reports_expected_markers(self) -> None:
        checks: list[str] = []
        run_basic_selftest_checks(checks)
        self.assertIn("sensitive-redaction=ok", checks)
        self.assertIn("exchange-log-account-context=ok", checks)
        self.assertIn("exchange-log-display-time=ok", checks)
        self.assertIn("exchange-log-amount-highlight=ok", checks)
        self.assertTrue(any(item.startswith("secret-box=") for item in checks))


if __name__ == "__main__":
    unittest.main()
