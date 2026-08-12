#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import inspect
import unittest
from decimal import Decimal

from exchange_app_batch import ExchangeAppBatchMixin


class CollectBnbFlowOrderTests(unittest.TestCase):
    def test_dust_conversion_runs_immediately_after_collection_stage(self) -> None:
        source = inspect.getsource(ExchangeAppBatchMixin.run_selected_accounts)
        stage_markers = (
            "client.collect_all_to_spot()",
            "client.convert_spot_dust_to_bnb()",
            "self._collect_bnb_simple_earn_assets(client)",
            "client.convert_large_spot_assets_to_bnb(",
            'client.buy_bnb_with_quote_amount(',
            "client.ensure_bnb_fee_ready_in_spot()",
            "self._collect_bnb_remaining_spot_assets(client)",
            "client.withdraw_all_coin(",
        )
        positions = [source.index(marker) for marker in stage_markers]
        self.assertEqual(positions, sorted(positions))

    def test_small_usdt_residual_is_below_ignore_threshold(self) -> None:
        class Client:
            @staticmethod
            def spot_positive_assets():
                return [
                    {"asset": "BNB", "free": Decimal("0.05")},
                    {"asset": "USDT", "free": Decimal("0.099")},
                ]

            @staticmethod
            def get_symbol_price(_symbol):
                return Decimal("600")

        residual = ExchangeAppBatchMixin._collect_bnb_spot_residual_usdt(
            Client(),
            ["USDT"],
        )

        self.assertEqual(residual, Decimal("0.099"))
        self.assertLess(residual, ExchangeAppBatchMixin.COLLECT_BNB_RESIDUAL_IGNORE_USDT)

    def test_residual_equal_to_threshold_is_not_ignored(self) -> None:
        class Client:
            @staticmethod
            def spot_positive_assets():
                return [{"asset": "USDT", "free": Decimal("0.1")}]

            @staticmethod
            def get_symbol_price(_symbol):
                return Decimal("600")

        residual = ExchangeAppBatchMixin._collect_bnb_spot_residual_usdt(
            Client(),
            ["USDT"],
        )

        self.assertEqual(residual, ExchangeAppBatchMixin.COLLECT_BNB_RESIDUAL_IGNORE_USDT)


if __name__ == "__main__":
    unittest.main()
