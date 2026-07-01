#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

import task_progress


class _Var:
    def __init__(self) -> None:
        self.value = ""

    def set(self, value: str) -> None:
        self.value = str(value)


class _Owner:
    def __init__(self) -> None:
        self.progress_var = _Var()
        self.row_status = {}

    def _progress_store(self, _kind: str):
        return self.row_status


class TaskProgressTests(unittest.TestCase):
    def test_recovery_metric_is_optional(self) -> None:
        owner = _Owner()

        task_progress.begin(owner, "transfer", ["row-1"], amount_label="转账总额")
        task_progress.set_metrics(owner, balance_text="1 BNB", amount_text="2 USDT", gas_text="预估 0.1 BNB")
        owner.row_status["row-1"] = "success"
        task_progress.finish(owner, "transfer", 1, 0)

        self.assertIn("余额总额=1 BNB", owner.progress_var.value)
        self.assertIn("转账总额=2 USDT", owner.progress_var.value)
        self.assertNotIn("回收总额", owner.progress_var.value)
        self.assertIn("gas总额=预估 0.1 BNB", owner.progress_var.value)

    def test_recovery_metric_stays_visible_after_finish(self) -> None:
        owner = _Owner()

        task_progress.begin(owner, "transfer", ["row-1"], amount_label="转账总额")
        task_progress.set_metrics(
            owner,
            balance_text="2 BNB；20 USDT",
            amount_text="20 USDT",
            recovery_text="2 BNB",
            gas_text="预估 0.1 BNB",
        )
        owner.row_status["row-1"] = "success"
        task_progress.finish(owner, "transfer", 1, 0)

        self.assertIn("余额总额=2 BNB；20 USDT", owner.progress_var.value)
        self.assertIn("转账总额=20 USDT", owner.progress_var.value)
        self.assertIn("回收总额=2 BNB", owner.progress_var.value)
        self.assertIn("gas总额=预估 0.1 BNB", owner.progress_var.value)


if __name__ == "__main__":
    unittest.main()
