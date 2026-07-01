#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app_defaults import (
    EXCHANGE_WORKER_THREADS_DEFAULT,
    ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT,
    ONCHAIN_DELAY_SECONDS_DEFAULT,
    ONCHAIN_NONCE_WINDOW_DEFAULT,
    ONCHAIN_ONE_TO_MANY_SOURCE_THREADS_DEFAULT,
    ONCHAIN_RECEIPT_POLL_INTERVAL_SECONDS_DEFAULT,
    ONCHAIN_RECEIPT_RETRY_COUNT_DEFAULT,
    ONCHAIN_WORKER_THREADS_DEFAULT,
)


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
    delay_seconds: float = ONCHAIN_DELAY_SECONDS_DEFAULT
    worker_threads: int = EXCHANGE_WORKER_THREADS_DEFAULT
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
    one_to_many_source_threads: int = ONCHAIN_ONE_TO_MANY_SOURCE_THREADS_DEFAULT
    confirm_timeout_seconds: float = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
    receipt_poll_interval_seconds: float = ONCHAIN_RECEIPT_POLL_INTERVAL_SECONDS_DEFAULT
    receipt_retry_count: int = ONCHAIN_RECEIPT_RETRY_COUNT_DEFAULT
    nonce_window: int = ONCHAIN_NONCE_WINDOW_DEFAULT
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
    delay_seconds: float = ONCHAIN_DELAY_SECONDS_DEFAULT
    one_to_many_source_threads: int = ONCHAIN_ONE_TO_MANY_SOURCE_THREADS_DEFAULT
    worker_threads: int = ONCHAIN_WORKER_THREADS_DEFAULT
    confirm_timeout_seconds: float = ONCHAIN_CONFIRM_TIMEOUT_SECONDS_DEFAULT
    receipt_poll_interval_seconds: float = ONCHAIN_RECEIPT_POLL_INTERVAL_SECONDS_DEFAULT
    receipt_retry_count: int = ONCHAIN_RECEIPT_RETRY_COUNT_DEFAULT
    nonce_window: int = ONCHAIN_NONCE_WINDOW_DEFAULT
    dry_run: bool = True
    use_config_proxy: bool = False
    proxy_url: str = ""
    one_to_many_source: str = ""
    one_to_many_source_ephemeral: bool = False
    many_to_one_target: str = ""
    relay_enabled: bool = False
    relay_fee_reserve: str = ""
    mode_relay_configs: dict[str, dict[str, object]] = field(default_factory=dict)
    relay_sweep_enabled: bool = True
    relay_sweep_target: str = ""
