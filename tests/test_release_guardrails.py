#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from tools.check_release_guardrails import check_release_guardrails


class ReleaseGuardrailTests(unittest.TestCase):
    def test_release_guardrails_are_enforced(self) -> None:
        check_release_guardrails()


if __name__ == "__main__":
    unittest.main()
