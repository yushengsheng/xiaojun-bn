#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from exchange_app_batch import ExchangeAppBatchMixin
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


if __name__ == "__main__":
    unittest.main()
