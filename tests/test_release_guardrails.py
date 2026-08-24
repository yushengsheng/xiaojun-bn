#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from tools.check_release_guardrails import _proxy_runtime_pins, check_release_guardrails


class ReleaseGuardrailTests(unittest.TestCase):
    def test_release_guardrails_are_enforced(self) -> None:
        check_release_guardrails()

    def test_proxy_runtime_pins_are_parsed_exactly(self) -> None:
        pins = _proxy_runtime_pins(
            """
            XRAY_TAG: v1
            XRAY_ASSET: 'xray.zip'
            XRAY_SHA256: \"abc123\"
            """
        )

        self.assertEqual(
            pins,
            {"XRAY_TAG": "v1", "XRAY_ASSET": "xray.zip", "XRAY_SHA256": "abc123"},
        )


if __name__ == "__main__":
    unittest.main()
