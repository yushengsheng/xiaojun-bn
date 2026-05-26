#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from shared_utils import _shift_text_view_state_after_trim, log_amount_value_spans


class LogAmountHighlightTests(unittest.TestCase):
    def _highlighted_values(self, text: str) -> list[str]:
        return [text[start:end] for start, end in log_amount_value_spans(text)]

    def test_highlights_labeled_and_coin_amounts(self) -> None:
        text = "金额=USDT 12.34，手续费 0.0002 BNB，重挂阈值=0.1 USDT"
        self.assertEqual(self._highlighted_values(text), ["12.34", "0.0002", "0.1"])

    def test_does_not_highlight_operational_numbers(self) -> None:
        text = "账号 #8 serverTime=1710000000000，delay=3，threads=2，确认 0 笔失败"
        self.assertEqual(self._highlighted_values(text), [])


class TextViewStateTests(unittest.TestCase):
    def test_shift_text_view_state_after_trim(self) -> None:
        self.assertEqual(_shift_text_view_state_after_trim(("text", "12.4"), 5), ("text", "7.4"))
        self.assertEqual(_shift_text_view_state_after_trim(("text", "2.0"), 10), ("text", "1.0"))
        self.assertEqual(_shift_text_view_state_after_trim(("fraction", 0.5), 5), ("fraction", 0.5))


if __name__ == "__main__":
    unittest.main()
