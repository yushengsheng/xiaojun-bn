#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import unittest

from exchange_logging import TkLogEntry, is_transient_retry_log_message


class RetryLogRoutingTests(unittest.TestCase):
    def test_transient_retry_warning_stays_out_of_error_log(self) -> None:
        entry = TkLogEntry(
            "网络请求失败 (1/10): Read timed out，将在 1 秒后重试...",
            levelno=logging.WARNING,
            levelname="WARNING",
        )

        self.assertTrue(is_transient_retry_log_message(entry.message))
        self.assertFalse(entry.is_problem)

    def test_final_retry_failure_still_goes_to_error_log(self) -> None:
        entry = TkLogEntry(
            "重试 10 次后仍然失败。",
            levelno=logging.ERROR,
            levelname="ERROR",
        )

        self.assertFalse(is_transient_retry_log_message(entry.message))
        self.assertTrue(entry.is_problem)


if __name__ == "__main__":
    unittest.main()
