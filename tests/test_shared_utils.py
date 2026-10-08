#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from decimal import Decimal

from shared_utils import _shift_text_view_state_after_trim, decimal_to_stat_text, decimal_to_text, log_amount_value_spans


class StatisticDecimalFormatTests(unittest.TestCase):
    def test_rounds_statistics_to_five_decimal_places(self) -> None:
        self.assertEqual(decimal_to_stat_text(Decimal("12.345675")), "12.34568")
        self.assertEqual(decimal_to_stat_text(Decimal("-0.000006")), "-0.00001")
        self.assertEqual(decimal_to_stat_text(Decimal("0.000004")), "0")

    def test_supports_fixed_five_decimal_result_files(self) -> None:
        self.assertEqual(decimal_to_stat_text(Decimal("12.3"), fixed=True), "12.30000")
        self.assertEqual(decimal_to_stat_text(Decimal("1E+100")), "1" + ("0" * 100))

    def test_full_precision_formatter_is_unchanged_for_transaction_values(self) -> None:
        self.assertEqual(decimal_to_text(Decimal("12.34567891")), "12.34567891")


class LogAmountHighlightTests(unittest.TestCase):
    def _highlighted_values(self, text: str) -> list[str]:
        return [text[start:end] for start, end in log_amount_value_spans(text)]

    def test_highlights_labeled_and_coin_amounts(self) -> None:
        text = "金额=USDT 12.34，手续费 0.0002 BNB，重挂阈值=0.1 USDT"
        self.assertEqual(self._highlighted_values(text), ["12.34", "0.0002", "0.1"])

    def test_does_not_highlight_operational_numbers(self) -> None:
        text = "账号 #8 serverTime=1710000000000，delay=3，threads=2，确认 0 笔失败"
        self.assertEqual(self._highlighted_values(text), [])

    def test_highlights_trade_results_and_fee_stop_values(self) -> None:
        text = "✔市价59轮结束｜交易量≈67087.24099 USDT｜买卖收支差≈+0.17009 USDT"
        self.assertEqual(self._highlighted_values(text), ["67087.24099", "+0.17009"])
        text = "闪兑完成：支出=50 到账=0.06414189，停止值=0.0005 BNB"
        self.assertEqual(self._highlighted_values(text), ["50", "0.06414189", "0.0005"])


class TextViewStateTests(unittest.TestCase):
    def test_shift_text_view_state_after_trim(self) -> None:
        self.assertEqual(_shift_text_view_state_after_trim(("text", "12.4"), 5), ("text", "7.4"))
        self.assertEqual(_shift_text_view_state_after_trim(("text", "2.0"), 10), ("text", "1.0"))
        self.assertEqual(_shift_text_view_state_after_trim(("fraction", 0.5), 5), ("fraction", 0.5))


if __name__ == "__main__":
    unittest.main()
