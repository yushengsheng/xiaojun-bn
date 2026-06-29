#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from onchain_rpc_runtime import format_onchain_failure_status


class OnchainFailureStatusTextTests(unittest.TestCase):
    def test_balance_errors_are_grouped_for_status_column(self) -> None:
        self.assertEqual(format_onchain_failure_status("代币余额不足"), "❌代币余额不足")
        self.assertEqual(format_onchain_failure_status("原生币余额不足，无法支付 gas"), "❌原生币不足(gas)")
        self.assertEqual(format_onchain_failure_status("源钱包原生币余额不足，无法完成当前中转续跑阶段"), "❌源钱包原生币不足")

    def test_network_and_rpc_errors_keep_actionable_reason(self) -> None:
        self.assertEqual(
            format_onchain_failure_status(RuntimeError("BSC RPC 请求失败：Read timed out")),
            "❌网络异常：Read timed out",
        )
        self.assertEqual(
            format_onchain_failure_status("确认中超过 3 秒，最后错误：BSC RPC 请求失败：Read timed out"),
            "❌确认超时(RPC无响应)",
        )

    def test_prefixes_are_removed_before_classification(self) -> None:
        self.assertEqual(
            format_onchain_failure_status("[序号#3][3/9] 转账失败：insufficient funds for gas * price + value"),
            "❌原生币不足(gas)",
        )


if __name__ == "__main__":
    unittest.main()
