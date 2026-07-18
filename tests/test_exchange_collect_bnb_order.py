#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import inspect
import unittest

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


if __name__ == "__main__":
    unittest.main()
