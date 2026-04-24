#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class AccountEntry:
    api_key: str
    api_secret: str
    address: str
    network: str = ""


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
    confirm_timeout_seconds: float = 180.0
    random_enabled: bool = False
    random_min: Decimal | None = None
    random_max: Decimal | None = None
    token_contract: str = ""
    token_decimals: int = 18
    token_is_native: bool = True
    relay_enabled: bool = False
    relay_fee_reserve: Decimal | None = None
    relay_sweep_enabled: bool = False
    relay_sweep_target: str = ""


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


@dataclass(frozen=True)
class GeneratedWalletEntry:
    address: str
    private_key: str


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
    mode_amounts: dict[str, dict[str, str]] = field(default_factory=dict)
    delay_seconds: float = 1.0
    worker_threads: int = 10
    confirm_timeout_seconds: float = 180.0
    dry_run: bool = True
    use_config_proxy: bool = False
    proxy_url: str = ""
    one_to_many_source: str = ""
    many_to_one_target: str = ""
    relay_enabled: bool = False
    relay_fee_reserve: str = ""
    mode_relay_configs: dict[str, dict[str, object]] = field(default_factory=dict)
    relay_sweep_enabled: bool = True
    relay_sweep_target: str = ""
