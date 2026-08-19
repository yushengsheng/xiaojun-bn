#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from decimal import Decimal

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_constants import TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_MARKET, TRADE_MODE_OPTIONS


class _SpotPairBalanceClient:
    def __init__(self, balances: dict[str, Decimal]):
        self.balances = {str(asset).upper(): Decimal(str(amount)) for asset, amount in balances.items()}
        self.collected_assets: list[str] = []

    def collect_asset_to_spot(self, asset: str, *, context: str = "交易前") -> int:
        self.collected_assets.append(str(asset or "").strip().upper())
        return 0

    def spot_asset_balance_decimal(self, asset: str) -> Decimal:
        return self.balances.get(str(asset or "").strip().upper(), Decimal("0"))


class _SpotPairBalanceProbe(ExchangeAppBatchMixin):
    @staticmethod
    def _normalize_trade_account_type(value):
        return str(value or "").strip()

    @staticmethod
    def _normalize_trade_mode(value):
        return str(value or "").strip()

    @staticmethod
    def _current_random_delay_seconds() -> float:
        return 0.0


class ExchangeSpotPairBalanceWaitTests(unittest.TestCase):
    def test_all_spot_modes_start_with_either_side_of_pair(self) -> None:
        probe = _SpotPairBalanceProbe()
        for mode in TRADE_MODE_OPTIONS:
            for balances in ({"ALLO": Decimal("12")}, {"USDT": Decimal("20")}):
                with self.subTest(mode=mode, balances=balances):
                    client = _SpotPairBalanceClient(balances)
                    ready = probe.wait_for_usdt(
                        1,
                        None,
                        client=client,
                        symbol="ALLOUSDT",
                        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
                        trade_mode=mode,
                    )
                    self.assertTrue(ready)
                    self.assertEqual(client.collected_assets, ["USDT", "ALLO"])

    def test_zero_round_wait_also_starts_with_either_side_of_pair(self) -> None:
        probe = _SpotPairBalanceProbe()
        for balances in ({"ALLO": Decimal("12")}, {"USDT": Decimal("20")}):
            with self.subTest(balances=balances):
                client = _SpotPairBalanceClient(balances)
                ready = probe.wait_for_usdt(
                    1,
                    None,
                    client=client,
                    symbol="ALLOUSDT",
                    trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
                    trade_mode=TRADE_MODE_MARKET,
                    spot_base_to_quote_only=True,
                )
                self.assertTrue(ready)
                self.assertEqual(client.collected_assets, ["USDT", "ALLO"])


if __name__ == "__main__":
    unittest.main()
