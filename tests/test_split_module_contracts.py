#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceClient
from page_onchain_base import OnchainTransferPageBase


class SplitModuleContractTests(unittest.TestCase):
    def test_onchain_callback_methods_remain_instance_methods(self) -> None:
        for name in ("_display_credential", "_show_result_summary_dialog", "_on_mode_changed"):
            raw = OnchainTransferPageBase.__dict__.get(name)
            self.assertIsNotNone(raw, name)
            self.assertFalse(isinstance(raw, staticmethod), name)
            self.assertFalse(isinstance(raw, classmethod), name)

    def test_split_mixins_preserve_public_method_lookup(self) -> None:
        for name in ("_runtime_worker_threads", "_start_managed_thread", "_apply_setting_layout", "test_onchain_proxy"):
            self.assertTrue(hasattr(OnchainTransferPageBase, name), name)
        for name in ("_build_batch_summary_text", "run_selected_accounts", "batch_manual_withdraw"):
            self.assertTrue(hasattr(ExchangeAppBatchMixin, name), name)

    def test_binance_client_split_mixins_preserve_public_method_lookup(self) -> None:
        expected_modules = {
            "request": ("request", "exchange_binance_request"),
            "account": ("query_total_wallet_balance", "exchange_binance_account"),
            "earn": ("get_simple_earn_flexible_positions", "exchange_binance_earn"),
            "convert": ("convert_with_from_amount", "exchange_binance_convert"),
            "spot": ("place_limit_order", "exchange_binance_spot"),
            "futures": ("place_um_futures_market_order", "exchange_binance_futures"),
            "funding": ("collect_all_to_spot", "exchange_binance_funding"),
        }
        for label, (method_name, module_name) in expected_modules.items():
            method = getattr(BinanceClient, method_name, None)
            self.assertIsNotNone(method, label)
            self.assertEqual(getattr(method, "__module__", ""), module_name, label)
        self.assertEqual(BinanceClient.split_spot_symbol("XLM/USDT"), ("XLM", "USDT"))


if __name__ == "__main__":
    unittest.main()
