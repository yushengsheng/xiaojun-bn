#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class AccountEntry:
    api_key: str
    api_secret: str
    address: str


@dataclass
class GlobalSettings:
    coin: str = "USDT"
    network: str = ""
    amount: str = ""
    delay_seconds: float = 1.0
    worker_threads: int = 5
    mode: str = "M2M"
    random_amount_enabled: bool = False
    random_amount_min: str = ""
    random_amount_max: str = ""
    dry_run: bool = True


@dataclass(frozen=True)
class WithdrawRuntimeParams:
    coin: str
    amount: str
    network: str
    delay: float
    threads: int
    random_enabled: bool = False
    random_min: Decimal | None = None
    random_max: Decimal | None = None
    token_contract: str = ""
    token_decimals: int = 18
    token_is_native: bool = True


@dataclass
class BgOneToManySettings:
    coin: str = "USDT"
    network: str = ""
    amount_mode: str = "固定数量"
    amount: str = ""
    random_min: str = ""
    random_max: str = ""
    delay_seconds: float = 1.0
    worker_threads: int = 5
    dry_run: bool = True
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""


@dataclass
class OnchainPairEntry:
    source: str
    target: str


@dataclass(frozen=True)
class EvmToken:
    symbol: str
    contract: str = ""
    decimals: int = 18
    is_native: bool = True


@dataclass
class OnchainSettings:
    mode: str = "多对多"
    network: str = "ETH"
    token_symbol: str = ""
    token_contract: str = ""
    amount_mode: str = "固定数量"
    amount: str = ""
    random_min: str = ""
    random_max: str = ""
    delay_seconds: float = 1.0
    worker_threads: int = 10
    dry_run: bool = True
    one_to_many_source: str = ""
    many_to_one_target: str = ""
