#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from api_clients import EvmClient


def run_online_selftest_checks(client: EvmClient, checks: list[str]) -> None:
    zero_address = "0x0000000000000000000000000000000000000000"
    eth_balance = client.get_balance_wei("ETH", zero_address)
    bsc_balance = client.get_balance_wei("BSC", zero_address)
    checks.append(f"eth-rpc={eth_balance}")
    checks.append(f"bsc-rpc={bsc_balance}")
