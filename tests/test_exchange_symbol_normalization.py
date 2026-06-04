#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from exchange_binance_client import BinanceClient


class ExchangeSymbolNormalizationTests(unittest.TestCase):
    def test_normalizes_slash_pair_format(self) -> None:
        self.assertEqual(BinanceClient.normalize_trade_symbol("XLM/USDT"), "XLMUSDT")
        self.assertEqual(BinanceClient.normalize_trade_symbol("xlm / usdt"), "XLMUSDT")

    def test_keeps_native_binance_symbol_format(self) -> None:
        self.assertEqual(BinanceClient.normalize_trade_symbol("XLMUSDT"), "XLMUSDT")

    def test_spot_symbol_parts_accept_slash_pair_format(self) -> None:
        self.assertEqual(BinanceClient.split_spot_symbol("XLM/USDT"), ("XLM", "USDT"))
        self.assertEqual(BinanceClient.get_spot_base_asset("XLM/USDT"), "XLM")
        self.assertEqual(BinanceClient.get_spot_quote_asset("XLM/USDT"), "USDT")


if __name__ == "__main__":
    unittest.main()
