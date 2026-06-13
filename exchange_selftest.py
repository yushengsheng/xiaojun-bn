#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import tempfile
import threading
from decimal import Decimal
from pathlib import Path

import requests

from api_clients import EvmClient
from app_paths import LOG_FILE_PATH
from core_models import EvmToken, OnchainPairEntry, OnchainSettings, WithdrawRuntimeParams
from exchange_app_base import ExchangeAppBase
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_app_config import ExchangeAppConfigMixin
from exchange_binance_client import BinanceAPIError, BinanceClient
from exchange_constants import TRADE_ACCOUNT_TYPE_FUTURES, TRADE_ACCOUNT_TYPE_SPOT, TRADE_MODE_CONVERT, TRADE_MODE_LIMIT, TRADE_MODE_MARKET, TRADE_MODE_PREMIUM
from exchange_logging import logger, runtime_log_path
from onchain_relay_wallets import RelayWalletFileStore
from onchain_relay_runner import (
    _record_matches_job,
    _relay_is_terminal_dust_state,
    _relay_native_dust_below_threshold,
    _relay_positive_tracked_tokens,
    _relay_token_dust_below_threshold,
)
from exchange_proxy_runtime import ExchangeProxyRuntime
from exchange_strategy import Strategy
from selftest_basic_checks import run_basic_selftest_checks
from selftest_gui_checks import run_gui_selftest_checks
from selftest_online_checks import run_online_selftest_checks
from shared_utils import decimal_to_text, random_decimal_between
from stores import OnchainStore


def _load_onchain_page_class():
    try:
        from page_onchain import OnchainTransferPage
    except Exception as exc:
        return None, exc
    return OnchainTransferPage, None


def _run_offline_business_selftest_checks(checks: list[str]) -> None:
    run_basic_selftest_checks(checks)

    with tempfile.TemporaryDirectory(prefix="xiaojun-selftest-") as tmpdir:
        store_path = Path(tmpdir) / "onchain.json"
        store = OnchainStore(store_path)
        store.multi_to_multi_pairs = [
            OnchainPairEntry(
                source="0x1111111111111111111111111111111111111111111111111111111111111111",
                target="0x0000000000000000000000000000000000000001",
            )
        ]
        store.multi_to_multi_drafts = [
            {
                "source": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "target": "",
            },
            {
                "source": "",
                "target": "0x0000000000000000000000000000000000000002",
            },
        ]
        store.save_transfer_lists_only()

        loaded_store = OnchainStore(store_path)
        loaded_store.load()
        if len(loaded_store.multi_to_multi_pairs) != 1:
            raise RuntimeError("链上导入列表自检失败：完整多对多记录未恢复")
        if loaded_store.multi_to_multi_drafts != store.multi_to_multi_drafts:
            raise RuntimeError("链上导入列表自检失败：待补齐草稿未恢复")
        store.settings = OnchainSettings(
            mode="多对1",
            network="BSC",
            token_symbol="BNB",
            amount_mode="全部",
            amount="全部",
            mode_amounts={
                "multi_to_multi": {
                    "amount_mode": "全部",
                    "amount": "全部",
                    "random_min": "",
                    "random_max": "",
                },
                "one_to_many": {
                    "amount_mode": "固定数量",
                    "amount": "0.01",
                    "random_min": "",
                    "random_max": "",
                },
                "many_to_one": {
                    "amount_mode": "全部",
                    "amount": "全部",
                    "random_min": "",
                    "random_max": "",
                },
            },
            mode_relay_configs={
                "one_to_many": {
                    "relay_enabled": True,
                    "relay_fee_reserve": "0.0001",
                },
                "many_to_one": {
                    "relay_enabled": False,
                    "relay_fee_reserve": "0",
                },
            },
        )
        store.save_settings_only()
        loaded_store = OnchainStore(store_path)
        loaded_store.load()
        if loaded_store.settings.mode_amounts.get("many_to_one", {}).get("amount_mode") != "全部":
            raise RuntimeError("链上配置自检失败：多对1数量模式未独立恢复")
        if loaded_store.settings.mode_amounts.get("one_to_many", {}).get("amount") != "0.01":
            raise RuntimeError("链上配置自检失败：1对多固定数量未独立恢复")
        if loaded_store.settings.mode_relay_configs.get("many_to_one", {}).get("relay_fee_reserve") != "0":
            raise RuntimeError("链上配置自检失败：多对1手续费预留未独立恢复")
        if loaded_store.settings.mode_relay_configs.get("one_to_many", {}).get("relay_fee_reserve") != "0.0001":
            raise RuntimeError("链上配置自检失败：1对多手续费预留未独立恢复")
        if loaded_store.settings.mode_relay_configs.get("one_to_many", {}).get("relay_enabled") is not True:
            raise RuntimeError("链上配置自检失败：1对多中转开关未独立恢复")
        if loaded_store.settings.mode_relay_configs.get("many_to_one", {}).get("relay_enabled") is not False:
            raise RuntimeError("链上配置自检失败：多对1中转开关未独立恢复")

        import page_onchain_base

        class _Var:
            def __init__(self, value: str = ""):
                self._value = value

            def get(self):
                return self._value

            def set(self, value):
                self._value = value

        class _AmountProbe(page_onchain_base.OnchainTransferPageBase):
            def __init__(self):
                self.amount_mode_var = _Var(self.AMOUNT_MODE_FIXED)
                self.amount_var = _Var("")
                self.random_min_var = _Var("")
                self.random_max_var = _Var("")
                self.relay_enabled_var = _Var(False)
                self.relay_fee_reserve_var = _Var("")
                self._mode_amount_configs = {}
                self._last_mode_for_amounts = self.MODE_M2M
                self._mode_relay_configs = {}
                self._last_mode_for_relay = self.MODE_M2M

            def _mode(self):
                return self._last_mode_for_amounts

        probe = _AmountProbe()
        probe._load_mode_amount_configs_from_settings(OnchainSettings())
        if probe._mode_amount_configs.get(probe.MODE_M2M, {}).get("amount_mode") != probe.AMOUNT_MODE_ALL:
            raise RuntimeError("链上配置自检失败：多对多默认数量模式不是“全部”")
        if probe._mode_amount_configs.get(probe.MODE_1M, {}).get("amount_mode") != probe.AMOUNT_MODE_FIXED:
            raise RuntimeError("链上配置自检失败：1对多默认数量模式不是“固定数量”")
        if probe._mode_amount_configs.get(probe.MODE_M1, {}).get("amount_mode") != probe.AMOUNT_MODE_ALL:
            raise RuntimeError("链上配置自检失败：多对1默认数量模式不是“全部”")

        probe._load_mode_amount_configs_from_settings(
            OnchainSettings(mode="1对多", amount_mode="固定数量", amount="0.02")
        )
        if probe._mode_amount_configs.get(probe.MODE_1M, {}).get("amount") != "0.02":
            raise RuntimeError("链上配置自检失败：旧版1对多数量配置未迁移")
        if probe._mode_amount_configs.get(probe.MODE_M2M, {}).get("amount_mode") != probe.AMOUNT_MODE_ALL:
            raise RuntimeError("链上配置自检失败：旧版迁移污染了多对多默认值")

        probe._store_mode_amount_config(
            probe.MODE_1M,
            {"amount_mode": probe.AMOUNT_MODE_FIXED, "amount": "0.03", "random_min": "", "random_max": ""},
        )
        probe._apply_mode_amount_config(probe.MODE_1M)
        if probe.amount_var.get() != "0.03":
            raise RuntimeError("链上配置自检失败：1对多独立数量未正确回填")
        probe._apply_mode_amount_config(probe.MODE_M1)
        if probe._amount_mode() != probe.AMOUNT_MODE_ALL:
            raise RuntimeError("链上配置自检失败：切换到多对1后默认数量模式异常")
        probe._apply_mode_amount_config(probe.MODE_1M)
        if probe.amount_var.get() != "0.03":
            raise RuntimeError("链上配置自检失败：模式切换后1对多数量被污染")

        probe._store_mode_amount_config(
            probe.MODE_1M,
            {"amount_mode": probe.AMOUNT_MODE_RANDOM, "amount": "", "random_min": "abc", "random_max": "1"},
        )
        payload = probe._mode_amounts_payload(
            existing_mode_amounts={
                "one_to_many": {
                    "amount_mode": probe.AMOUNT_MODE_FIXED,
                    "amount": "0.02",
                    "random_min": "",
                    "random_max": "",
                }
            },
            current_mode=probe.MODE_M2M,
            current_config={"amount_mode": probe.AMOUNT_MODE_ALL, "amount": probe.AMOUNT_ALL_LABEL, "random_min": "", "random_max": ""},
        )
        if payload.get("one_to_many", {}).get("amount") != "0.02":
            raise RuntimeError("链上配置自检失败：隐藏模式的无效数量配置错误覆盖了已保存值")

        probe._load_mode_relay_configs_from_settings(OnchainSettings())
        if probe._mode_relay_configs.get(probe.MODE_1M, {}).get("relay_fee_reserve") != "":
            raise RuntimeError("链上配置自检失败：1对多默认手续费预留不是空值")
        if probe._mode_relay_configs.get(probe.MODE_M1, {}).get("relay_fee_reserve") != "":
            raise RuntimeError("链上配置自检失败：多对1默认手续费预留不是空值")
        if probe._mode_relay_configs.get(probe.MODE_1M, {}).get("relay_enabled") is not False:
            raise RuntimeError("链上配置自检失败：1对多默认中转开关不是关闭")
        if probe._mode_relay_configs.get(probe.MODE_M1, {}).get("relay_enabled") is not False:
            raise RuntimeError("链上配置自检失败：多对1默认中转开关不是关闭")

        probe._load_mode_relay_configs_from_settings(
            OnchainSettings(mode="1对多", relay_enabled=True, relay_fee_reserve="0.0001")
        )
        if probe._mode_relay_configs.get(probe.MODE_1M, {}).get("relay_fee_reserve") != "0.0001":
            raise RuntimeError("链上配置自检失败：旧版1对多手续费预留未迁移")
        if probe._mode_relay_configs.get(probe.MODE_M1, {}).get("relay_fee_reserve") != "":
            raise RuntimeError("链上配置自检失败：旧版手续费预留迁移污染了多对1默认值")
        if probe._mode_relay_configs.get(probe.MODE_1M, {}).get("relay_enabled") is not True:
            raise RuntimeError("链上配置自检失败：旧版1对多中转开关未迁移")
        if probe._mode_relay_configs.get(probe.MODE_M1, {}).get("relay_enabled") is not False:
            raise RuntimeError("链上配置自检失败：旧版中转开关迁移污染了多对1默认值")

        probe._store_mode_relay_config(
            probe.MODE_1M,
            {"relay_enabled": True, "relay_fee_reserve": "0.0001"},
        )
        probe._apply_mode_relay_config(probe.MODE_1M)
        if probe.relay_fee_reserve_var.get() != "0.0001":
            raise RuntimeError("链上配置自检失败：1对多手续费预留未正确回填")
        if probe.relay_enabled_var.get() is not True:
            raise RuntimeError("链上配置自检失败：1对多中转开关未正确回填")
        probe._apply_mode_relay_config(probe.MODE_M1)
        if probe.relay_fee_reserve_var.get() != "":
            raise RuntimeError("链上配置自检失败：切换到多对1后默认手续费预留异常")
        if probe.relay_enabled_var.get() is not False:
            raise RuntimeError("链上配置自检失败：切换到多对1后默认中转开关异常")
        probe._apply_mode_relay_config(probe.MODE_1M)
        if probe.relay_fee_reserve_var.get() != "0.0001":
            raise RuntimeError("链上配置自检失败：模式切换后1对多手续费预留被污染")
        if probe.relay_enabled_var.get() is not True:
            raise RuntimeError("链上配置自检失败：模式切换后1对多中转开关被污染")

        probe._store_mode_relay_config(
            probe.MODE_1M,
            {"relay_enabled": False, "relay_fee_reserve": "abc"},
        )
        relay_payload = probe._mode_relay_configs_payload(
            existing_mode_relay_configs={
                "one_to_many": {
                    "relay_enabled": True,
                    "relay_fee_reserve": "0.0001",
                }
            },
            current_mode=probe.MODE_M1,
            current_config={"relay_enabled": False, "relay_fee_reserve": "0"},
        )
        if relay_payload.get("one_to_many", {}).get("relay_fee_reserve") != "0.0001":
            raise RuntimeError("链上配置自检失败：隐藏模式的无效手续费预留错误覆盖了已保存值")
        if relay_payload.get("one_to_many", {}).get("relay_enabled") is not True:
            raise RuntimeError("链上配置自检失败：隐藏模式的无效中转配置错误覆盖了已保存值")

        class _RowStore:
            pass

        probe.store = _RowStore()
        probe.store.multi_to_multi_pairs = [
            OnchainPairEntry(source="s1", target="t1"),
            OnchainPairEntry(source="s2", target="t2"),
            OnchainPairEntry(source="s3", target="t3"),
        ]
        probe.store.one_to_many_addresses = ["a1", "a2", "a3"]
        probe.store.many_to_one_sources = ["m1", "m2", "m3"]
        probe._last_mode_for_amounts = probe.MODE_M2M
        m2m_numbers = probe._row_number_by_key_snapshot()
        m2m_key_2 = probe._m2m_key(probe.store.multi_to_multi_pairs[1])
        m2m_key_3 = probe._m2m_key(probe.store.multi_to_multi_pairs[2])
        if probe._row_log_prefix(m2m_key_2, 1, 2, m2m_numbers) != "[序号#2][1/2]":
            raise RuntimeError("链上日志序号自检失败：多对多序号前缀异常")
        if probe._row_group_log_prefix([m2m_key_2, m2m_key_3], 1, 1, m2m_numbers) != "[序号#2,3][1/1]":
            raise RuntimeError("链上日志序号自检失败：多行序号前缀异常")
        probe._last_mode_for_amounts = probe.MODE_1M
        one_to_many_numbers = probe._row_number_by_key_snapshot()
        if probe._row_log_prefix(probe._one_to_many_key("a3"), 2, 3, one_to_many_numbers) != "[序号#3][2/3]":
            raise RuntimeError("链上日志序号自检失败：1对多序号前缀异常")
        probe._last_mode_for_amounts = probe.MODE_M1
        many_to_one_numbers = probe._row_number_by_key_snapshot()
        if probe._row_log_prefix(probe._many_to_one_key("m1"), 1, 3, many_to_one_numbers) != "[序号#1][1/3]":
            raise RuntimeError("链上日志序号自检失败：多对1序号前缀异常")
    checks.append("onchain-log-sequence=ok")
    checks.append("onchain-drafts=ok")

    with tempfile.TemporaryDirectory(prefix="xiaojun-relay-selftest-") as tmpdir:
        relay_root = Path(tmpdir)
        relay_file = relay_root / "\u4e2d\u8f6c\u94b1\u5305.txt"
        relay_store = RelayWalletFileStore(relay_file)
        relay_client = EvmClient()
        try:
            relay_wallet = relay_client.create_wallets(1)[0]
            relay_token = EvmToken(
                symbol="USDT",
                contract="0x55d398326f99059ff775485246999027b3197955",
                decimals=18,
                is_native=False,
            )
            relay_record = relay_store.build_record(
                batch_id="relay-selftest",
                network="BSC",
                source_address="0x0000000000000000000000000000000000000001",
                target_address="0x0000000000000000000000000000000000000002",
                relay_wallet=relay_wallet,
                token=relay_token,
                relay_fee_reserve=Decimal("0.0012"),
                sweep_enabled=True,
                sweep_target="0x0000000000000000000000000000000000000003",
            )
            relay_store.append_records([relay_record])
            loaded_relay_records = relay_store.load_records()
            if len(loaded_relay_records) != 1:
                raise RuntimeError("中转钱包自检失败：明文 TXT 记录数量异常")
            updated_relay_record = relay_store.update_record(
                relay_wallet.address,
                batch_id="relay-selftest",
                status="forwarded",
                transfer_amount="1.23",
                transfer_units="1230000000000000000",
                token_forward_txid="0x" + "1" * 64,
            )
            if updated_relay_record.status != "forwarded":
                raise RuntimeError("中转钱包自检失败：状态更新未生效")
            if updated_relay_record.transfer_amount != "1.23":
                raise RuntimeError("中转钱包自检失败：金额更新未生效")
            completed_relay_record = relay_store.update_record(
                relay_wallet.address,
                batch_id="relay-selftest",
                status="completed",
                sweep_resolution="manual_empty",
            )
            if completed_relay_record.sweep_resolution != "manual_empty":
                raise RuntimeError("中转钱包自检失败：手动清空终态未恢复")
            if not _relay_is_terminal_dust_state(completed_relay_record, {}):
                raise RuntimeError("中转钱包自检失败：终态 dust 记录未被识别")
            if _relay_is_terminal_dust_state(completed_relay_record, {"USDT": (relay_token, 1)}):
                raise RuntimeError("中转钱包自检失败：仍有代币余额时不应忽略终态记录")
            custom_relay_token = EvmToken(
                symbol="ABC",
                contract="0x00000000000000000000000000000000000000ab",
                decimals=18,
                is_native=False,
            )
            native_relay_token = EvmToken(symbol="BNB", contract="", decimals=18, is_native=True)
            recoverable_tokens = _relay_positive_tracked_tokens({
                "ABC": (custom_relay_token, 1),
                "BNB": (native_relay_token, 1),
            })
            if "ABC" not in recoverable_tokens or "BNB" in recoverable_tokens:
                raise RuntimeError("中转钱包自检失败：自定义中转代币识别异常")

            class _DustThresholdOwner:
                def _network_fee_symbol(self, network):
                    return "BNB"

                def _units_to_amount(self, units, decimals):
                    return Decimal(int(units)) / (Decimal(10) ** int(decimals))

                def _decimal_to_text(self, value):
                    return format(Decimal(value).normalize(), "f")

                def _token_amount_text(self, symbol, amount):
                    return f"{self._decimal_to_text(amount)} {symbol}"

                def _gas_fee_amount_text(self, network, amount_units):
                    return f"{self._decimal_to_text(self._units_to_amount(amount_units, 18))} BNB"

            dust_owner = _DustThresholdOwner()
            dust_low, dust_detail = _relay_token_dust_below_threshold(
                dust_owner,
                {"USDT": (relay_token, 1_390_000_000_000)},
            )
            if not dust_low:
                raise RuntimeError("中转钱包自检失败：低于固定阈值的代币 dust 未被识别")
            if "dust阈值" not in dust_detail:
                raise RuntimeError("中转钱包自检失败：代币 dust 说明文案缺少固定阈值")
            dust_equal, _dust_detail = _relay_token_dust_below_threshold(
                dust_owner,
                {"USDT": (relay_token, 100_000_000_000_000)},
            )
            if dust_equal:
                raise RuntimeError("中转钱包自检失败：等于固定阈值的代币余额被误判为 dust")
            native_low, native_detail = _relay_native_dust_below_threshold(
                dust_owner,
                "BSC",
                9_999_999_999_999,
            )
            if not native_low or "dust阈值" not in native_detail:
                raise RuntimeError("中转钱包自检失败：低于固定阈值的 BNB dust 未被识别")
            native_equal, _native_detail = _relay_native_dust_below_threshold(
                dust_owner,
                "BSC",
                10_000_000_000_000,
            )
            if native_equal:
                raise RuntimeError("中转钱包自检失败：等于固定阈值的 BNB 余额被误判为 dust")

            relay_store_path = relay_root / "onchain-relay.json"
            relay_onchain_store = OnchainStore(relay_store_path)
            relay_onchain_store.settings = OnchainSettings(
                mode="1对多",
                network="BSC",
                token_symbol="USDT",
                token_contract=relay_token.contract,
                amount_mode="固定数量",
                amount="1",
                confirm_timeout_seconds=240.0,
                relay_enabled=True,
                relay_fee_reserve="0.0012",
                relay_sweep_enabled=True,
                relay_sweep_target="0x0000000000000000000000000000000000000003",
            )
            relay_onchain_store.save_settings_only()
            relay_loaded_store = OnchainStore(relay_store_path)
            relay_loaded_store.load()
            if not relay_loaded_store.settings.relay_enabled:
                raise RuntimeError("中转钱包自检失败：配置中的中转开关未恢复")
            if relay_loaded_store.settings.relay_fee_reserve != "0.0012":
                raise RuntimeError("中转钱包自检失败：配置中的手续费预留未恢复")
            if abs(float(relay_loaded_store.settings.confirm_timeout_seconds) - 240.0) > 0.0001:
                raise RuntimeError("中转钱包自检失败：配置中的确认超时未恢复")
            relay_onchain_store.settings.mode = "多对1"
            relay_onchain_store.settings.relay_enabled = True
            relay_onchain_store.save_settings_only()
            relay_loaded_store = OnchainStore(relay_store_path)
            relay_loaded_store.load()
            if relay_loaded_store.settings.mode != "多对1":
                raise RuntimeError("中转钱包自检失败：多对1模式未恢复")
            if not relay_loaded_store.settings.relay_enabled:
                raise RuntimeError("中转钱包自检失败：多对1模式下中转开关未恢复")
            many_to_one_resume_record = relay_store.build_record(
                batch_id="relay-selftest-m1",
                network="BSC",
                source_address="0x0000000000000000000000000000000000000011",
                target_address="0x0000000000000000000000000000000000000022",
                relay_wallet=relay_wallet,
                token=relay_token,
                relay_fee_reserve=Decimal("0.0001"),
                sweep_enabled=False,
                sweep_target="0x0000000000000000000000000000000000000022",
            )
            many_to_one_resume_params = WithdrawRuntimeParams(
                coin="USDT",
                amount="1",
                network="BSC",
                delay=0.0,
                threads=1,
                token_contract=relay_token.contract,
                token_decimals=relay_token.decimals,
                token_is_native=False,
                relay_enabled=True,
                relay_fee_reserve=Decimal("0"),
            )
            if _record_matches_job(
                many_to_one_resume_record,
                many_to_one_resume_params,
                many_to_one_resume_record.source,
                many_to_one_resume_record.target,
            ):
                raise RuntimeError("中转钱包自检失败：多对1更改手续费预留后仍错误续跑旧记录")
        finally:
            relay_client.close()
    checks.append("relay-wallets=ok")

    import exchange_app_log_view

    class _LogViewProbe(exchange_app_log_view.ExchangeAppLogViewMixin):
        def __init__(self, *, use_config_proxy: bool, raw_proxy: str, direct_ip: str | Exception, proxy_result=None, system_proxy=None):
            self._snapshot = {
                "use_config_proxy": use_config_proxy,
                "raw_proxy": raw_proxy,
            }
            self._direct_ip = direct_ip
            self._proxy_result = proxy_result
            self._system_proxy = dict(system_proxy or {})

        def _exchange_proxy_state_snapshot(self):
            return dict(self._snapshot)

        def _system_proxy_map(self):
            return dict(self._system_proxy)

        def _fetch_public_ip(self, *, use_exchange_proxy: bool, allow_system_proxy: bool = True):
            if use_exchange_proxy:
                return "9.9.9.9"
            if isinstance(self._direct_ip, Exception):
                raise self._direct_ip
            return str(self._direct_ip)

        def _test_exchange_proxy_once(self, *, include_exit_ip: bool = True, state=None):
            if isinstance(self._proxy_result, Exception):
                raise self._proxy_result
            return self._proxy_result

    ip_probe = _LogViewProbe(
        use_config_proxy=True,
        raw_proxy="ss://example",
        direct_ip="1.2.3.4",
        proxy_result=RuntimeError("proxy down"),
    )
    ip_text, proxy_status, proxy_exit_ip = ip_probe._resolve_exchange_ip_refresh_state()
    if ip_text != "1.2.3.4":
        raise RuntimeError("状态栏自检失败：代理失败时直连 IP 被错误覆盖")
    if proxy_status != "连接失败":
        raise RuntimeError("状态栏自检失败：代理失败时状态未标记为连接失败")
    if proxy_exit_ip != "--":
        raise RuntimeError("状态栏自检失败：代理失败时出口 IP 未保持空值")

    direct_fail_probe = _LogViewProbe(
        use_config_proxy=False,
        raw_proxy="",
        direct_ip=RuntimeError("network down"),
        proxy_result=None,
    )
    ip_text, proxy_status, proxy_exit_ip = direct_fail_probe._resolve_exchange_ip_refresh_state()
    if "获取失败: network down" != ip_text:
        raise RuntimeError("状态栏自检失败：直连失败提示异常")
    if proxy_status != "直连异常":
        raise RuntimeError("状态栏自检失败：直连失败状态异常")
    if proxy_exit_ip != "--":
        raise RuntimeError("状态栏自检失败：直连失败时出口 IP 未保持空值")
    checks.append("status-bar=ok")

    merged_backend_error = ExchangeProxyRuntime._merge_backend_errors([
        "xray 启动失败: 权限不足",
        "sing-box 启动失败: 拒绝访问",
    ])
    merged_text = str(merged_backend_error)
    if "xray 启动失败: 权限不足" not in merged_text or "sing-box 启动失败: 拒绝访问" not in merged_text:
        raise RuntimeError("代理诊断自检失败：多后端失败信息未完整保留")
    checks.append("proxy-errors=ok")

    random_value = random_decimal_between(Decimal("0.00001"), Decimal("0.00003"), Decimal("0.00001"))
    if random_value < Decimal("0.00001") or random_value > Decimal("0.00003"):
        raise RuntimeError("随机金额精度自检失败：结果超出范围")
    if decimal_to_text(random_value) not in {"0.00001", "0.00002", "0.00003"}:
        raise RuntimeError("随机金额精度自检失败：结果未按最小单位取值")
    checks.append("random-unit=ok")

    class _SelftestSymbolClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")

        def get_exchange_info(self, symbol: str):
            symbol_u = str(symbol or "").strip().upper()
            if symbol_u == "XAUTUSDT":
                return {
                    "symbol": symbol_u,
                    "status": "TRADING",
                    "baseAsset": "XAUT",
                    "quoteAsset": "USDT",
                }
            if symbol_u == "BTCU":
                return {
                    "symbol": symbol_u,
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "U",
                }
            return None

        def get_convert_pair_info(self, from_asset: str, to_asset: str):
            key = (str(from_asset or "").strip().upper(), str(to_asset or "").strip().upper())
            if key in {("USDT", "XAUT"), ("XAUT", "USDT"), ("USDT", "U"), ("U", "USDT")}:
                return {
                    "fromAsset": key[0],
                    "toAsset": key[1],
                }
            return None

        def get_um_futures_exchange_info(self, symbol: str):
            symbol_u = str(symbol or "").strip().upper()
            if symbol_u == "BTCUSDT":
                return {
                    "symbol": symbol_u,
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                }
            return None

    symbol_client = _SelftestSymbolClient()
    try:
        base_asset, quote_asset = symbol_client.ensure_spot_symbol_supported("XAUTUSDT")
        if (base_asset, quote_asset) != ("XAUT", "USDT"):
            raise RuntimeError("交易对校验自检失败：现货资产解析异常")
        symbol_client.ensure_convert_symbol_supported("XAUTUSDT")
        if BinanceClient.split_spot_symbol("USDTU") != ("USDT", "U"):
            raise RuntimeError("U后置币种解析自检失败：USDTU 未解析为 USDT/U")
        if BinanceClient.split_spot_symbol("UUSDT") != ("U", "USDT"):
            raise RuntimeError("U后置币种解析自检失败：UUSDT 未解析为 U/USDT")
        base_asset, quote_asset = symbol_client.ensure_spot_symbol_supported("BTCU")
        if (base_asset, quote_asset) != ("BTC", "U"):
            raise RuntimeError("U后置现货交易对自检失败：BTCU 解析异常")
        base_asset, quote_asset = symbol_client.ensure_convert_symbol_supported("USDTU")
        if (base_asset, quote_asset) != ("USDT", "U"):
            raise RuntimeError("USDT->U闪兑资产对自检失败：USDTU 未被识别为闪兑资产对")
        try:
            symbol_client.ensure_spot_symbol_supported("USDTU")
        except RuntimeError:
            pass
        else:
            raise RuntimeError("USDTU现货校验自检失败：不应把USDTU误判为现货交易对")
        quote_asset_for_zero_round = ExchangeAppConfigMixin._ensure_trade_symbol_supported(
            symbol_client,
            TRADE_ACCOUNT_TYPE_SPOT,
            TRADE_MODE_MARKET,
            "USDTU",
            "BTCUSDT",
            0,
        )
        if quote_asset_for_zero_round != "U":
            raise RuntimeError("0轮USDTU校验自检失败：未按闪兑资产对放行")
        try:
            ExchangeAppConfigMixin._ensure_trade_symbol_supported(
                symbol_client,
                TRADE_ACCOUNT_TYPE_SPOT,
                TRADE_MODE_MARKET,
                "USDTU",
                "BTCUSDT",
                1,
            )
        except RuntimeError:
            pass
        else:
            raise RuntimeError("非0轮USDTU校验自检失败：普通市价不应放行非现货交易对")
        quote_asset_for_convert = ExchangeAppConfigMixin._ensure_trade_symbol_supported(
            symbol_client,
            TRADE_ACCOUNT_TYPE_SPOT,
            TRADE_MODE_CONVERT,
            "USDTU",
            "BTCUSDT",
            1,
        )
        if quote_asset_for_convert != "U":
            raise RuntimeError("闪兑模式USDTU校验自检失败：未按闪兑资产对放行")
        if symbol_client.get_um_futures_margin_asset("BTCUSDT") != "USDT":
            raise RuntimeError("交易对校验自检失败：合约保证金币种解析异常")
        try:
            symbol_client.ensure_spot_symbol_supported("BADPAIR")
        except RuntimeError:
            pass
        else:
            raise RuntimeError("交易对校验自检失败：现货无效交易对未拦截")
        try:
            symbol_client.get_um_futures_margin_asset("BADPAIR")
        except RuntimeError:
            pass
        else:
            raise RuntimeError("交易对校验自检失败：合约无效交易对未拦截")
    finally:
        symbol_client.close()
    checks.append("symbol-validate=ok")

    class _SelftestSellClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.last_order_params: dict[str, str] | None = None

        def request(self, base, method, path, params=None):
            if method == "GET" and path == "/api/v3/account":
                return {
                    "balances": [
                        {
                            "asset": "XAUT",
                            "free": "1.23456789",
                            "locked": "0",
                        }
                    ]
                }
            if method == "POST" and path == "/api/v3/order":
                self.last_order_params = dict(params or {})
                return {
                    "symbol": "XAUTUSDT",
                    "status": "FILLED",
                }
            raise RuntimeError(f"现货卖出自检遇到未知请求：{method} {path}")

        def get_symbol_trade_rules(self, symbol: str):
            return {
                "status": "TRADING",
                "stepSize": Decimal("0.01"),
                "minQty": Decimal("0.01"),
                "maxQty": Decimal("999999"),
                "minNotional": Decimal("10"),
                "quoteAsset": "USDT",
            }

        def get_symbol_price(self, symbol: str):
            return Decimal("3200")

    sell_client = _SelftestSellClient()
    try:
        if not sell_client.spot_sell_all_base("XAUTUSDT"):
            raise RuntimeError("现货卖出自检失败：卖出未执行")
        order_params = sell_client.last_order_params or {}
        if str(order_params.get("quantity") or "") != "1.23":
            raise RuntimeError(f"现货卖出自检失败：数量取整异常（{order_params.get('quantity')}）")
    finally:
        sell_client.close()
    checks.append("spot-sell=ok")

    class _SelftestBuyClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.last_order_params: dict[str, str] | None = None

        def request(self, base, method, path, params=None):
            if method == "GET" and path == "/api/v3/account":
                return {
                    "balances": [
                        {
                            "asset": "USDT",
                            "free": "10.998375",
                            "locked": "0",
                        }
                    ]
                }
            if method == "POST" and path == "/api/v3/order":
                self.last_order_params = dict(params or {})
                return {
                    "symbol": "XAUTUSDT",
                    "status": "FILLED",
                }
            raise RuntimeError(f"现货买入自检遇到未知请求：{method} {path}")

        def get_exchange_info(self, symbol: str):
            symbol_u = str(symbol or "").strip().upper()
            if symbol_u != "XAUTUSDT":
                return None
            return {
                "symbol": symbol_u,
                "status": "TRADING",
                "baseAsset": "XAUT",
                "quoteAsset": "USDT",
                "quoteAssetPrecision": 8,
                "quotePrecision": 8,
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001", "maxQty": "999999"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01", "minPrice": "0.01", "maxPrice": "999999"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                ],
            }

    buy_client = _SelftestBuyClient()
    try:
        if not buy_client.spot_buy_quote_amount("XAUTUSDT", Decimal("10.987376625")):
            raise RuntimeError("现货买入自检失败：买入未执行")
        order_params = buy_client.last_order_params or {}
        if str(order_params.get("quoteOrderQty") or "") != "10.98737662":
            raise RuntimeError(
                f"现货买入自检失败：quoteOrderQty 精度异常（{order_params.get('quoteOrderQty')}）"
            )
    finally:
        buy_client.close()
    checks.append("spot-buy-precision=ok")

    class _SelftestZeroRoundClient:
        def __init__(self, events=None):
            self.calls: list[tuple[str, str]] = []
            self.events = events

        def _record_event(self, name: str) -> None:
            if self.events is not None:
                self.events.append(name)

        def get_spot_base_asset(self, symbol: str) -> str:
            self.calls.append(("base", str(symbol or "").strip().upper()))
            return "BTC"

        def get_spot_quote_asset(self, symbol: str) -> str:
            self.calls.append(("quote", str(symbol or "").strip().upper()))
            return "USDT"

        def convert_base_to_quote_all(self, symbol: str):
            self.calls.append(("convert-base-to-quote", str(symbol or "").strip().upper()))
            self._record_event("convert")
            return {"status": "ok"}

        def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
            self.calls.append(("withdraw", str(coin or "").strip().upper()))
            self._record_event("withdraw")
            return 0

        def spot_buy_all_usdt(self, symbol: str):
            raise RuntimeError("0轮现货兑换不应执行市价买入")

        def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
            raise RuntimeError("0轮现货兑换不应执行市价卖出")

        def buy_bnb_with_quote_amount(self, symbol: str, quote_amount):
            raise RuntimeError("0轮现货兑换不应执行预买BNB")

    zero_round_events: list[str] = []
    zero_round_client = _SelftestZeroRoundClient(zero_round_events)
    zero_round_progress: list[tuple[int, int, str]] = []
    zero_round_sleep_calls = {"count": 0}

    def zero_round_sleep_fn():
        zero_round_sleep_calls["count"] += 1
        zero_round_events.append("sleep")

    zero_round_strategy = Strategy(
        client=zero_round_client,
        spot_rounds=0,
        withdraw_coin="USDT",
        withdraw_address="0x0000000000000000000000000000000000000001",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=zero_round_sleep_fn,
        enable_withdraw=True,
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
        bnb_topup_amount=Decimal("10"),
    )
    zero_round_result = zero_round_strategy.run(
        stop_event=None,
        progress_cb=lambda step, total, text: zero_round_progress.append((step, total, text)),
    )
    zero_round_convert_calls = [
        call for call in zero_round_client.calls if call == ("convert-base-to-quote", "BTCUSDT")
    ]
    if len(zero_round_convert_calls) != 1:
        raise RuntimeError(f"0轮现货兑换自检失败：兑换调用次数异常（{zero_round_client.calls}）")
    expected_zero_round_progress = [
        (0, 1, "0轮兑换 BTC->USDT"),
        (1, 1, "0轮兑换 BTC->USDT 完成"),
    ]
    if zero_round_progress != expected_zero_round_progress:
        raise RuntimeError(f"0轮现货兑换自检失败：进度回调异常（{zero_round_progress}）")
    if zero_round_sleep_calls["count"] != 2:
        raise RuntimeError(f"0轮现货兑换自检失败：随机延迟未在兑换前和最终提现前执行（{zero_round_sleep_calls}）")
    if zero_round_events != ["sleep", "convert", "sleep", "withdraw"]:
        raise RuntimeError(f"0轮现货兑换自检失败：随机延迟和提现顺序异常（{zero_round_events}）")
    if not zero_round_result or zero_round_result.get("withdraw_attempted") is not True:
        raise RuntimeError("0轮现货兑换自检失败：未保持既有最终提现流程")
    checks.append("spot-zero-round-convert=ok")

    stop_withdraw_events: list[str] = []
    stop_withdraw_client = _SelftestZeroRoundClient(stop_withdraw_events)
    stop_withdraw_event = threading.Event()
    stop_withdraw_sleep_calls = {"count": 0}

    def stop_withdraw_sleep_fn():
        stop_withdraw_sleep_calls["count"] += 1
        stop_withdraw_events.append("sleep")
        if stop_withdraw_sleep_calls["count"] == 2:
            stop_withdraw_event.set()

    stop_withdraw_strategy = Strategy(
        client=stop_withdraw_client,
        spot_rounds=0,
        withdraw_coin="USDT",
        withdraw_address="0x0000000000000000000000000000000000000001",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=stop_withdraw_sleep_fn,
        enable_withdraw=True,
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
        bnb_topup_amount=Decimal("10"),
    )
    stop_withdraw_result = stop_withdraw_strategy.run(stop_event=stop_withdraw_event)
    if ("withdraw", "USDT") in stop_withdraw_client.calls:
        raise RuntimeError(f"最终提现停止自检失败：停止后仍执行提现（{stop_withdraw_client.calls}）")
    if stop_withdraw_result.get("withdraw_attempted") is not False:
        raise RuntimeError(f"最终提现停止自检失败：停止后提现状态异常（{stop_withdraw_result}）")
    if stop_withdraw_events != ["sleep", "convert", "sleep"]:
        raise RuntimeError(f"最终提现停止自检失败：随机延迟和停止顺序异常（{stop_withdraw_events}）")
    checks.append("final-withdraw-delay-stop=ok")

    class _SelftestStableUZeroRoundClient:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def get_spot_base_asset(self, symbol: str) -> str:
            return BinanceClient.get_spot_base_asset(symbol)

        def get_spot_quote_asset(self, symbol: str) -> str:
            return BinanceClient.get_spot_quote_asset(symbol)

        def convert_base_to_quote_all(self, symbol: str):
            symbol_u = str(symbol or "").strip().upper()
            base_asset = BinanceClient.get_spot_base_asset(symbol_u)
            quote_asset = BinanceClient.get_spot_quote_asset(symbol_u)
            self.calls.append(("convert", f"{base_asset}->{quote_asset}"))
            return {"status": "ok"}

        def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
            self.calls.append(("withdraw", str(coin or "").strip().upper()))
            return 0

        def spot_buy_all_usdt(self, symbol: str):
            raise RuntimeError("0轮U闪兑不应执行市价买入")

        def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
            raise RuntimeError("0轮U闪兑不应执行市价卖出")

        def buy_bnb_with_quote_amount(self, symbol: str, quote_amount):
            raise RuntimeError("0轮U闪兑不应执行预买BNB")

    for symbol, expected_direction in (("USDTU", "USDT->U"), ("UUSDT", "U->USDT")):
        stable_u_client = _SelftestStableUZeroRoundClient()
        stable_u_strategy = Strategy(
            client=stable_u_client,
            spot_rounds=0,
            withdraw_coin="USDT",
            withdraw_address="",
            withdraw_network="BSC",
            withdraw_fee_buffer=0,
            spot_symbol=symbol,
            sleep_fn=lambda: None,
            enable_withdraw=False,
            trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
            trade_mode=TRADE_MODE_MARKET,
            bnb_topup_amount=Decimal("10"),
        )
        stable_u_strategy.run(stop_event=None)
        if ("convert", expected_direction) not in stable_u_client.calls:
            raise RuntimeError(f"0轮U闪兑方向自检失败：{symbol} 未执行 {expected_direction}（{stable_u_client.calls}）")
    checks.append("spot-zero-round-u-convert=ok")

    class _SelftestZeroRoundWaitClient:
        def __init__(self):
            self.collected_assets: list[str] = []

        def collect_funding_asset_to_spot(self, asset: str):
            self.collected_assets.append(str(asset or "").strip().upper())
            return Decimal("0")

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            asset_u = str(asset or "").strip().upper()
            return Decimal("0.01") if asset_u == "BTC" else Decimal("0")

    class _SelftestZeroRoundWaitProbe(ExchangeAppBatchMixin):
        def __init__(self, client):
            self.client = client

        def _normalize_trade_account_type(self, value):
            return str(value or "").strip()

        def _normalize_trade_mode(self, value):
            return str(value or "").strip()

        def _current_random_delay_seconds(self) -> float:
            return 0.0

    zero_round_wait_client = _SelftestZeroRoundWaitClient()
    zero_round_wait_probe = _SelftestZeroRoundWaitProbe(zero_round_wait_client)
    if not zero_round_wait_probe.wait_for_usdt(
        1,
        None,
        client=zero_round_wait_client,
        symbol="BTCUSDT",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
        spot_base_to_quote_only=True,
    ):
        raise RuntimeError("0轮现货兑换到账检测自检失败：未检测前置币种余额")
    if zero_round_wait_client.collected_assets != ["BTC"]:
        raise RuntimeError(
            f"0轮现货兑换到账检测自检失败：错误检测了后置币种（{zero_round_wait_client.collected_assets}）"
        )
    checks.append("spot-zero-round-wait=ok")

    class _SelftestAccountAssetBalanceClient:
        def __init__(self):
            self.calls: list[tuple[str, bool]] = []

        def query_asset_balances_breakdown(self, *, fast: bool = False) -> dict[str, Decimal]:
            self.calls.append(("asset-breakdown", bool(fast)))
            return {"USDT": Decimal("12.5"), "U": Decimal("2.5")}

        def spot_all_balances(self, *, fast: bool = False):
            raise RuntimeError("余额刷新不应走现货交易对余额路径")

    asset_balance_client = _SelftestAccountAssetBalanceClient()
    asset_balance_text = ExchangeAppBatchMixin._query_account_asset_balances_text(asset_balance_client, fast=True)
    if asset_balance_text != "USDT 12.5 | U 2.5":
        raise RuntimeError(f"账户资产余额查询自检失败：显示结果异常（{asset_balance_text}）")
    if asset_balance_client.calls != [("asset-breakdown", True)]:
        raise RuntimeError(f"账户资产余额查询自检失败：查询路径异常（{asset_balance_client.calls}）")
    checks.append("account-asset-balance-query=ok")

    class _SelftestMarketRoundClient:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def get_spot_base_asset(self, symbol: str) -> str:
            return "BTC"

        def get_spot_quote_asset(self, symbol: str) -> str:
            return "USDT"

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            self.calls.append(("balance", str(asset or "").strip().upper()))
            return Decimal("0.1") if str(asset or "").strip().upper() == "BNB" else Decimal("0")

        def buy_bnb_with_quote_amount(self, quote_asset: str, quote_amount, return_order: bool = False):
            self.calls.append(("bnb-topup", str(quote_asset or "").strip().upper()))
            return {"toAmount": "0.001"} if return_order else True

        def _decimal_from_str(self, value, default="0") -> Decimal:
            try:
                return Decimal(str(value))
            except Exception:
                return Decimal(str(default))

        def ensure_bnb_fee_ready_in_spot(self, *, min_spot_balance, max_transfer_amount=None, timeout_seconds=12.0):
            self.calls.append(("bnb-ready", str(min_spot_balance)))
            return Decimal(str(min_spot_balance))

        def spot_buy_all_usdt(self, symbol: str):
            self.calls.append(("buy", str(symbol or "").strip().upper()))
            return True

        def spot_sell_all_base(self, symbol: str, small_qty_log_text=None):
            self.calls.append(("sell", str(symbol or "").strip().upper()))
            return True

        def convert_base_to_quote_all(self, symbol: str):
            raise RuntimeError("普通市价轮次不应走0轮闪兑分支")

        def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
            self.calls.append(("withdraw", str(coin or "").strip().upper()))
            return 0

    market_round_client = _SelftestMarketRoundClient()
    market_round_progress: list[tuple[int, int, str]] = []
    market_round_strategy = Strategy(
        client=market_round_client,
        spot_rounds=2,
        withdraw_coin="USDT",
        withdraw_address="",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=lambda: None,
        enable_withdraw=False,
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
        bnb_topup_amount=Decimal("5"),
    )
    market_round_result = market_round_strategy.run(
        stop_event=None,
        progress_cb=lambda step, total, text: market_round_progress.append((step, total, text)),
    )
    market_buy_count = sum(1 for call in market_round_client.calls if call == ("buy", "BTCUSDT"))
    market_sell_count = sum(1 for call in market_round_client.calls if call == ("sell", "BTCUSDT"))
    market_topup_count = sum(1 for call in market_round_client.calls if call == ("bnb-topup", "USDT"))
    if market_buy_count != 2 or market_sell_count != 6 or market_topup_count != 1:
        raise RuntimeError(f"普通市价轮次回归自检失败：执行路径异常（{market_round_client.calls}）")
    expected_market_round_progress = [
        (0, 2, "现货轮 1/2 买入 BTC"),
        (0, 2, "现货轮 1/2 等待卖出 BTC"),
        (0, 2, "现货轮 1/2 卖出 BTC"),
        (1, 2, "现货轮BTC1/2完成"),
        (1, 2, "现货轮 2/2 买入 BTC"),
        (1, 2, "现货轮 2/2 等待卖出 BTC"),
        (1, 2, "现货轮 2/2 卖出 BTC"),
        (2, 2, "现货轮BTC2/2完成"),
    ]
    if market_round_progress != expected_market_round_progress:
        raise RuntimeError(f"普通市价轮次状态栏自检失败：进度回调异常（{market_round_progress}）")
    if not market_round_result or market_round_result.get("withdraw_attempted") is not True:
        raise RuntimeError("普通市价轮次回归自检失败：未保持既有最终提现流程")
    no_withdraw_status, no_withdraw_success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
        0.0,
        "USDT",
        enable_withdraw=False,
    )
    if no_withdraw_status is not None or no_withdraw_success is not True:
        raise RuntimeError("批量策略状态栏自检失败：关闭自动提现时不应覆盖最后轮次状态")
    withdraw_status, withdraw_success = ExchangeAppBatchMixin._format_batch_strategy_completion_status(
        0.0,
        "USDT",
        enable_withdraw=True,
    )
    if withdraw_success is not False or "提现失败" not in str(withdraw_status):
        raise RuntimeError("批量策略状态栏自检失败：开启自动提现时未保持提现结果状态")
    checks.append("spot-market-rounds=ok")

    class _SelftestPremiumPartialFillClient:
        def __init__(self):
            self.quote_free = Decimal("100")
            self.base_free = Decimal("0")
            self.actions: list[tuple[str, Decimal | str]] = []
            self.timeline: list[tuple[str, object, str | None]] = []
            self.read_counts: dict[int, int] = {}

        def get_spot_base_asset(self, symbol: str) -> str:
            return "BTC"

        def get_spot_quote_asset(self, symbol: str) -> str:
            return "USDT"

        def spot_balance(self, asset: str) -> Decimal:
            return Decimal("10") if str(asset or "").strip().upper() == "BNB" else self.spot_asset_balance_decimal(asset)

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            asset_u = str(asset or "").strip().upper()
            if asset_u == "BTC":
                return self.base_free
            if asset_u == "USDT":
                return self.quote_free
            if asset_u == "BNB":
                return Decimal("10")
            return Decimal("0")

        def get_book_ticker(self, symbol: str):
            return {"bidPrice": Decimal("10"), "askPrice": Decimal("10.5")}

        def normalize_price_delta(self, symbol: str, delta, *, min_one_tick: bool = True) -> Decimal:
            return Decimal(str(delta or "0"))

        def adjust_price_to_valid_tick(self, symbol: str, desired_price, round_up: bool = False) -> Decimal:
            return Decimal(str(desired_price))

        def spot_limit_buy_all_usdt(self, symbol: str, price):
            price_dec = Decimal(str(price))
            self.actions.append(("initial-buy", price_dec))
            self.timeline.append(("place-buy", 1, None))
            self.quote_free = Decimal("0")
            return {"orderId": 1, "price": str(price_dec), "origQty": "10", "executedQty": "0"}

        def spot_limit_sell_quantity(self, symbol: str, price, quantity):
            price_dec = Decimal(str(price))
            qty_dec = Decimal(str(quantity))
            self.actions.append(("sell", qty_dec, price_dec))
            self.timeline.append(("place-sell", 2, None))
            self.base_free = max(Decimal("0"), self.base_free - qty_dec)
            return {"orderId": 2, "price": str(price_dec), "origQty": str(qty_dec), "executedQty": "0"}

        def spot_limit_buy_quote_amount(self, symbol: str, price, quote_amount):
            price_dec = Decimal(str(price))
            amount_dec = Decimal(str(quote_amount))
            self.actions.append(("rebuy", amount_dec, price_dec))
            self.timeline.append(("place-buy", 3, None))
            self.quote_free = max(Decimal("0"), self.quote_free - amount_dec)
            return {
                "orderId": 3,
                "price": str(price_dec),
                "origQty": str(amount_dec / price_dec),
                "executedQty": "0",
            }

        def get_order(self, symbol: str, order_id):
            order_id_int = int(order_id)
            read_count = self.read_counts.get(order_id_int, 0) + 1
            self.read_counts[order_id_int] = read_count
            if order_id_int == 1 and read_count == 1:
                self.base_free += Decimal("4")
                status = "PARTIALLY_FILLED"
                order = {
                    "orderId": 1,
                    "status": status,
                    "price": "10",
                    "origQty": "10",
                    "executedQty": "4",
                    "cummulativeQuoteQty": "40",
                }
            elif order_id_int == 1:
                status = "CANCELED"
                order = {
                    "orderId": 1,
                    "status": status,
                    "price": "10",
                    "origQty": "10",
                    "executedQty": "4",
                    "cummulativeQuoteQty": "40",
                }
            elif order_id_int == 2 and read_count == 1:
                self.quote_free += Decimal("22")
                status = "PARTIALLY_FILLED"
                order = {
                    "orderId": 2,
                    "status": status,
                    "price": "11",
                    "origQty": "4",
                    "executedQty": "2",
                    "cummulativeQuoteQty": "22",
                }
            elif order_id_int == 2:
                status = "CANCELED"
                order = {
                    "orderId": 2,
                    "status": status,
                    "price": "11",
                    "origQty": "4",
                    "executedQty": "2",
                    "cummulativeQuoteQty": "22",
                }
            else:
                status = "CANCELED"
                order = {
                    "orderId": order_id_int,
                    "status": status,
                    "price": "10",
                    "origQty": "2.2",
                    "executedQty": "0",
                    "cummulativeQuoteQty": "0",
                }
            self.timeline.append(("read", order_id_int, status))
            return order

        def get_order_average_price(self, order_data: dict) -> Decimal | None:
            executed_qty = Decimal(str(order_data.get("executedQty", "0") or "0"))
            quote_qty = Decimal(str(order_data.get("cummulativeQuoteQty", "0") or "0"))
            if executed_qty > 0 and quote_qty > 0:
                return quote_qty / executed_qty
            return Decimal(str(order_data.get("price", "0") or "0"))

        def cancel_order(self, symbol: str, order_id):
            self.timeline.append(("cancel", int(order_id), None))
            return {"orderId": int(order_id), "status": "CANCELED"}

        def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
            self.actions.append(("withdraw", str(coin or "").strip().upper()))
            return 0

    class _SelftestImmediateWait:
        def is_set(self):
            return False

        def wait(self, _seconds=None):
            return False

    premium_partial_client = _SelftestPremiumPartialFillClient()
    premium_partial_strategy = Strategy(
        client=premium_partial_client,
        spot_rounds=1,
        withdraw_coin="USDT",
        withdraw_address="",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=lambda: None,
        enable_withdraw=False,
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_PREMIUM,
        premium_delta=Decimal("1"),
        premium_partial_fill=True,
        bnb_fee_stop_value=Decimal("0"),
    )
    premium_partial_strategy.run(stop_event=_SelftestImmediateWait())
    action_names = [item[0] for item in premium_partial_client.actions]
    if action_names[:3] != ["initial-buy", "sell", "rebuy"]:
        raise RuntimeError(f"溢价单部分成交循环自检失败：下单顺序异常（{premium_partial_client.actions}）")
    sell_action = premium_partial_client.actions[1]
    rebuy_action = premium_partial_client.actions[2]
    if sell_action != ("sell", Decimal("4"), Decimal("11")):
        raise RuntimeError(f"溢价单部分成交循环自检失败：买单部分成交挂卖异常（{sell_action}）")
    if rebuy_action != ("rebuy", Decimal("22"), Decimal("10")):
        raise RuntimeError(f"溢价单部分成交循环自检失败：卖单部分成交挂买异常（{rebuy_action}）")
    try:
        sell_place_index = premium_partial_client.timeline.index(("place-sell", 2, None))
        buy_terminal_index = premium_partial_client.timeline.index(("read", 1, "CANCELED"))
        rebuy_place_index = premium_partial_client.timeline.index(("place-buy", 3, None))
        sell_terminal_index = premium_partial_client.timeline.index(("read", 2, "CANCELED"))
    except ValueError as exc:
        raise RuntimeError(f"溢价单部分成交循环自检失败：关键时序缺失（{premium_partial_client.timeline}）") from exc
    if sell_place_index >= buy_terminal_index:
        raise RuntimeError(f"溢价单部分成交循环自检失败：卖单等待了买单终态（{premium_partial_client.timeline}）")
    if rebuy_place_index >= sell_terminal_index:
        raise RuntimeError(f"溢价单部分成交循环自检失败：回买等待了卖单终态（{premium_partial_client.timeline}）")
    checks.append("premium-partial-fill-loop=ok")

    class _SelftestFuturesOpenFailureClient:
        def __init__(self):
            self.calls: list[str] = []

        def close_all_um_futures_positions_market(self, symbol: str):
            self.calls.append("ensure-close")
            return []

        def get_um_futures_positions(self, symbol: str):
            return []

        def get_um_futures_position_mode(self):
            return False

        def get_um_futures_symbol_config(self, symbol: str):
            return {"leverage": "20", "marginType": "CROSSED"}

        def get_um_futures_margin_asset(self, symbol: str):
            return "USDT"

        def calculate_um_futures_order_quantity(self, symbol: str, amount, side: str):
            return Decimal("0.057")

        def um_futures_asset_balance(self, asset: str):
            return {"availableBalance": "50"}

        def ensure_um_futures_one_way_mode(self):
            self.calls.append("one-way")

        def ensure_um_futures_margin_type(self, symbol: str, margin_type: str):
            self.calls.append("margin")

        def ensure_um_futures_leverage(self, symbol: str, leverage: int):
            self.calls.append("leverage")
            return leverage

        def place_um_futures_market_order(self, symbol: str, side: str, quantity):
            self.calls.append("open")
            raise BinanceAPIError(-1135, "Invalid account status.")

        def close_um_futures_position_market(self, symbol: str):
            raise RuntimeError("开仓失败后不应进入正常平仓")

        def withdraw_all_coin(self, *, coin, address, network, fee_buffer, enable_withdraw, auto_collect_to_spot=False):
            self.calls.append("withdraw")
            return 0

    class _SelftestNoSleepStop:
        def is_set(self):
            return False

        def wait(self, _seconds=None):
            return False

    futures_failure_client = _SelftestFuturesOpenFailureClient()
    futures_failure_progress: list[tuple[int, int, str]] = []
    futures_failure_strategy = Strategy(
        client=futures_failure_client,
        spot_rounds=1,
        withdraw_coin="USDT",
        withdraw_address="",
        withdraw_network="BSC",
        withdraw_fee_buffer=0,
        spot_symbol="BTCUSDT",
        sleep_fn=lambda: None,
        enable_withdraw=False,
        trade_account_type=TRADE_ACCOUNT_TYPE_FUTURES,
        trade_mode=TRADE_MODE_MARKET,
        futures_symbol="XAUUSDT",
        futures_rounds=1,
        futures_amount=Decimal("260"),
        futures_leverage=20,
        futures_margin_type="CROSSED",
    )
    futures_failure_result = futures_failure_strategy.run(
        stop_event=_SelftestNoSleepStop(),
        progress_cb=lambda step, total, text: futures_failure_progress.append((step, total, text)),
    )
    if not futures_failure_result.get("trade_failed"):
        raise RuntimeError(f"合约开仓失败自检失败：未返回交易失败状态（{futures_failure_result}）")
    if futures_failure_result.get("trade_error_stage") != "开仓下单":
        raise RuntimeError(f"合约开仓失败自检失败：失败阶段异常（{futures_failure_result}）")
    if "开仓失败" not in [item[2] for item in futures_failure_progress]:
        raise RuntimeError(f"合约开仓失败自检失败：未回调开仓失败状态（{futures_failure_progress}）")
    if ExchangeAppBatchMixin._format_trade_failure_status("开仓下单", futures_failure_result.get("trade_error"))[:5] != "×开仓失败":
        raise RuntimeError("合约开仓失败自检失败：批量状态栏未显示开仓失败")
    if "withdraw" not in futures_failure_client.calls:
        raise RuntimeError(f"合约开仓失败自检失败：失败后未保持最终归集提现流程（{futures_failure_client.calls}）")
    checks.append("futures-open-failure-status=ok")

    class _SelftestQuoteWaitClient:
        def __init__(self):
            self.collected_assets: list[str] = []

        def collect_funding_asset_to_spot(self, asset: str):
            self.collected_assets.append(str(asset or "").strip().upper())
            return Decimal("0")

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            asset_u = str(asset or "").strip().upper()
            return Decimal("20") if asset_u == "USDT" else Decimal("0")

    quote_wait_client = _SelftestQuoteWaitClient()
    quote_wait_probe = _SelftestZeroRoundWaitProbe(quote_wait_client)
    if not quote_wait_probe.wait_for_usdt(
        1,
        None,
        client=quote_wait_client,
        symbol="BTCUSDT",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
    ):
        raise RuntimeError("普通市价到账检测自检失败：未检测后置币种余额")
    if quote_wait_client.collected_assets != ["USDT"]:
        raise RuntimeError(
            f"普通市价到账检测自检失败：错误检测了前置币种（{quote_wait_client.collected_assets}）"
        )
    checks.append("spot-quote-wait=ok")

    class _SelftestSpotPretradeCollectClient:
        def __init__(self):
            self.collect_calls: list[tuple[str, str]] = []
            self.balances: dict[str, Decimal] = {}

        def collect_asset_to_spot(self, asset: str, *, context: str = "提现前"):
            asset_u = str(asset or "").strip().upper()
            self.collect_calls.append((asset_u, str(context or "")))
            if asset_u == "USDT":
                self.balances[asset_u] = Decimal("20")
            elif asset_u == "BTC":
                self.balances[asset_u] = Decimal("0.01")
            return 1

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            return self.balances.get(str(asset or "").strip().upper(), Decimal("0"))

    pretrade_quote_client = _SelftestSpotPretradeCollectClient()
    pretrade_quote_probe = _SelftestZeroRoundWaitProbe(pretrade_quote_client)
    if not pretrade_quote_probe.wait_for_usdt(
        1,
        None,
        client=pretrade_quote_client,
        symbol="BTCUSDT",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
    ):
        raise RuntimeError("现货交易前归集自检失败：后置币种归集后未通过到账检测")
    if pretrade_quote_client.collect_calls != [("USDT", "交易前")]:
        raise RuntimeError(f"现货交易前归集自检失败：后置币种归集路径异常（{pretrade_quote_client.collect_calls}）")

    pretrade_base_client = _SelftestSpotPretradeCollectClient()
    pretrade_base_probe = _SelftestZeroRoundWaitProbe(pretrade_base_client)
    if not pretrade_base_probe.wait_for_usdt(
        1,
        None,
        client=pretrade_base_client,
        symbol="BTCUSDT",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
        spot_base_to_quote_only=True,
    ):
        raise RuntimeError("现货交易前归集自检失败：前置币种归集后未通过0轮到账检测")
    if pretrade_base_client.collect_calls != [("BTC", "交易前")]:
        raise RuntimeError(f"现货交易前归集自检失败：前置币种归集路径异常（{pretrade_base_client.collect_calls}）")

    class _SelftestSpotPretradeCollectRetryClient:
        def __init__(self):
            self.collect_calls: list[tuple[str, str]] = []
            self.funding_calls: list[str] = []
            self.balance = Decimal("0")

        def collect_asset_to_spot(self, asset: str, *, context: str = "提现前"):
            self.collect_calls.append((str(asset or "").strip().upper(), str(context or "")))
            return 0

        def collect_funding_asset_to_spot(self, asset: str):
            asset_u = str(asset or "").strip().upper()
            self.funding_calls.append(asset_u)
            if asset_u == "USDT":
                self.balance = Decimal("20")
            return self.balance

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            return self.balance if str(asset or "").strip().upper() == "USDT" else Decimal("0")

    class _SelftestPretradeDelayProbe(_SelftestZeroRoundWaitProbe):
        def _current_random_delay_seconds(self) -> float:
            return 0.01

    pretrade_retry_client = _SelftestSpotPretradeCollectRetryClient()
    pretrade_retry_probe = _SelftestPretradeDelayProbe(pretrade_retry_client)
    if not pretrade_retry_probe.wait_for_usdt(
        1,
        None,
        client=pretrade_retry_client,
        symbol="BTCUSDT",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
    ):
        raise RuntimeError("现货交易前归集自检失败：资金账户后续归集未保持重试")
    if pretrade_retry_client.collect_calls != [("USDT", "交易前")]:
        raise RuntimeError(f"现货交易前归集自检失败：完整归集重复执行（{pretrade_retry_client.collect_calls}）")
    if pretrade_retry_client.funding_calls != ["USDT"]:
        raise RuntimeError(f"现货交易前归集自检失败：资金账户后续归集异常（{pretrade_retry_client.funding_calls}）")
    checks.append("spot-pretrade-collect=ok")

    class _SelftestUsdcQuoteWaitClient:
        def __init__(self):
            self.collected_assets: list[str] = []

        def collect_funding_asset_to_spot(self, asset: str):
            self.collected_assets.append(str(asset or "").strip().upper())
            return Decimal("0")

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            asset_u = str(asset or "").strip().upper()
            return Decimal("20") if asset_u == "USDC" else Decimal("0")

    usdc_quote_wait_client = _SelftestUsdcQuoteWaitClient()
    usdc_quote_wait_probe = _SelftestZeroRoundWaitProbe(usdc_quote_wait_client)
    if not usdc_quote_wait_probe.wait_for_usdt(
        1,
        None,
        client=usdc_quote_wait_client,
        symbol="BTCUSDC",
        trade_account_type=TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode=TRADE_MODE_MARKET,
    ):
        raise RuntimeError("USDC后置币种到账检测自检失败：未检测USDC余额")
    if usdc_quote_wait_client.collected_assets != ["USDC"]:
        raise RuntimeError(
            f"USDC后置币种到账检测自检失败：错误检测了其他币种（{usdc_quote_wait_client.collected_assets}）"
        )
    checks.append("spot-usdc-quote-wait=ok")

    class _SimpleVar:
        def __init__(self, value=""):
            self._value = value

        def get(self):
            return self._value

        def set(self, value):
            self._value = value

    class _SelftestTradeSettingsProbe(ExchangeAppBase):
        def __init__(self, *, mode=TRADE_MODE_MARKET, spot_rounds=0):
            self.trade_account_type_var = _SimpleVar(TRADE_ACCOUNT_TYPE_SPOT)
            self.trade_mode_var = _SimpleVar(mode)
            self.spot_symbol_var = _SimpleVar("BTCUSDT")
            self.spot_rounds_var = _SimpleVar(spot_rounds)
            self.futures_rounds_var = _SimpleVar(20)
            self.premium_delta_var = _SimpleVar("")
            self.premium_order_count_var = _SimpleVar(0)
            self.premium_append_threshold_var = _SimpleVar("")
            self.premium_partial_fill_var = _SimpleVar(False)
            self.bnb_fee_stop_var = _SimpleVar("")
            self.bnb_topup_amount_var = _SimpleVar("0")
            self.reprice_threshold_var = _SimpleVar("0")
            self.futures_symbol_var = _SimpleVar("BTCUSDT")
            self.futures_amount_var = _SimpleVar("100")
            self.futures_margin_type_var = _SimpleVar("CROSSED")
            self.futures_side_var = _SimpleVar("")
            self.futures_leverage_var = _SimpleVar(10)

    market_zero_settings = _SelftestTradeSettingsProbe(
        mode=TRADE_MODE_MARKET,
        spot_rounds=0,
    )._collect_trade_mode_settings()
    if market_zero_settings.get("spot_rounds") != 0:
        raise RuntimeError(f"0轮市价参数自检失败：轮次未保留为0（{market_zero_settings}）")
    limit_zero_settings = _SelftestTradeSettingsProbe(
        mode=TRADE_MODE_LIMIT,
        spot_rounds=0,
    )._collect_trade_mode_settings()
    if limit_zero_settings.get("spot_rounds") != 0 or limit_zero_settings.get("bnb_fee_stop_value") is not None:
        raise RuntimeError(f"0轮挂单参数自检失败：0轮不应强制挂单专属参数（{limit_zero_settings}）")
    premium_zero_settings = _SelftestTradeSettingsProbe(
        mode=TRADE_MODE_PREMIUM,
        spot_rounds=0,
    )._collect_trade_mode_settings()
    if premium_zero_settings.get("spot_rounds") != 0 or premium_zero_settings.get("premium_delta_value") is not None:
        raise RuntimeError(f"0轮溢价参数自检失败：0轮不应强制溢价专属参数（{premium_zero_settings}）")
    try:
        _SelftestTradeSettingsProbe(mode=TRADE_MODE_MARKET, spot_rounds=-1)._collect_trade_mode_settings()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("现货负轮次参数自检失败：负数轮次未被拒绝")
    checks.append("spot-zero-round-settings=ok")

    class _SelftestCollectClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.transfers: list[tuple[str, str, Decimal]] = []

        def um_futures_transferable_assets(self, *, fast: bool = False):
            return [{"asset": "USDT", "amount": Decimal("0.123456789123")}]

        def cm_futures_transferable_assets(self, *, fast: bool = False):
            return [{"asset": "USDT", "amount": Decimal("0.987654321987")}]

        def funding_positive_assets(self, *, fast: bool = False):
            return [{"asset": "USDT", "free": Decimal("0.000000001234")}]

        def universal_transfer(self, transfer_type: str, asset: str, amount):
            self.transfers.append((transfer_type, str(asset or "").strip().upper(), Decimal(str(amount))))
            return True

    collect_client = _SelftestCollectClient()
    try:
        collected = collect_client.collect_asset_to_spot("USDT")
        expected_transfers = [
            ("UMFUTURE_MAIN", "USDT", Decimal("0.123456789123")),
            ("CMFUTURE_MAIN", "USDT", Decimal("0.987654321987")),
            ("FUNDING_MAIN", "USDT", Decimal("0.000000001234")),
        ]
        if collected != 3:
            raise RuntimeError(f"归集精度自检失败：处理数量异常（{collected}）")
        if collect_client.transfers != expected_transfers:
            raise RuntimeError(f"归集精度自检失败：划转金额异常（{collect_client.transfers}）")
    finally:
        collect_client.close()
    checks.append("collect-precision=ok")

    class _SelftestCollectRetryClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.transfer_attempts = 0

        def um_futures_transferable_assets(self, *, fast: bool = False):
            return [{"asset": "USDT", "amount": Decimal("1.23")}]

        def cm_futures_transferable_assets(self, *, fast: bool = False):
            return []

        def funding_positive_assets(self, *, fast: bool = False):
            return []

        def universal_transfer(self, transfer_type: str, asset: str, amount):
            self.transfer_attempts += 1
            if self.transfer_attempts <= 3:
                raise BinanceAPIError(32020, "We have temporarily suspended Futures transactions. Please try again later.")
            return True

    collect_retry_client = _SelftestCollectRetryClient()
    try:
        collected = collect_retry_client.collect_asset_to_spot("USDT")
        if collected != 1:
            raise RuntimeError(f"归集划转重试自检失败：成功数量异常（{collected}）")
        if collect_retry_client.transfer_attempts != 4:
            raise RuntimeError(f"归集划转重试自检失败：未执行失败后3次重试（{collect_retry_client.transfer_attempts}）")
    finally:
        collect_retry_client.close()
    checks.append("collect-transfer-retry=ok")

    class _SelftestLargeAssetBnbClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.actions: list[tuple[str, str]] = []

        def spot_all_balances(self, *, fast: bool = False):
            return [
                {"asset": "AAA", "free": "5", "locked": "0", "total": "5"},
                {"asset": "BBB", "free": "7", "locked": "0", "total": "7"},
                {"asset": "CCC", "free": "9", "locked": "0", "total": "9"},
                {"asset": "USDT", "free": "11", "locked": "0", "total": "11"},
                {"asset": "BNB", "free": "1", "locked": "0", "total": "1"},
            ]

        def get_convert_pair_info(self, from_asset: str, to_asset: str) -> dict[str, object] | None:
            from_asset_u = str(from_asset or "").strip().upper()
            to_asset_u = str(to_asset or "").strip().upper()
            if (from_asset_u, to_asset_u) == ("AAA", "BNB"):
                return {"fromAsset": "AAA", "toAsset": "BNB"}
            return None

        def convert_with_from_amount(
            self,
            from_asset: str,
            to_asset: str,
            from_amount,
            *,
            wallet_type: str = "SPOT",
            valid_time: str = "10s",
        ):
            self.actions.append(("convert", f"{str(from_asset).upper()}->{str(to_asset).upper()}"))
            return {"fromAmount": str(from_amount), "toAmount": "1"}

        def find_usdt_symbol_for_asset(self, asset: str):
            asset_u = str(asset or "").strip().upper()
            if asset_u == "BBB":
                return "BBBUSDT"
            return None

        def sell_asset_market(
            self,
            symbol: str,
            free_balance,
            reserve_ratio=Decimal("0.999"),
            *,
            small_qty_log_text: str | None = None,
        ) -> bool:
            self.actions.append(("sell", str(symbol or "").upper()))
            return True

    large_asset_client = _SelftestLargeAssetBnbClient()
    try:
        summary = large_asset_client.convert_large_spot_assets_to_bnb()
        if summary.get("direct_bnb") != ["AAA"]:
            raise RuntimeError(f"大额资产直兑BNB自检失败：直兑结果异常（{summary}）")
        if summary.get("fallback_usdt") != ["BBB"]:
            raise RuntimeError(f"大额资产直兑BNB自检失败：回退结果异常（{summary}）")
        if summary.get("residual_assets") != ["CCC"]:
            raise RuntimeError(f"大额资产直兑BNB自检失败：残留资产结果异常（{summary}）")
        if large_asset_client.actions != [("convert", "AAA->BNB"), ("sell", "BBBUSDT")]:
            raise RuntimeError(f"大额资产直兑BNB自检失败：执行路径异常（{large_asset_client.actions}）")
    finally:
        large_asset_client.close()
    checks.append("large-asset-bnb=ok")

    class _SelftestBnbFeeReadyClient(BinanceClient):
        def __init__(self, *, spot_bnb: Decimal, funding_bnb: Decimal):
            super().__init__("selftest-key", "selftest-secret")
            self._spot_balances = {"BNB": Decimal(str(spot_bnb))}
            self._funding_balances = {"BNB": Decimal(str(funding_bnb))}
            self.transfers: list[tuple[str, str, Decimal]] = []

        def spot_asset_balance_decimal(self, asset: str) -> Decimal:
            return Decimal(str(self._spot_balances.get(str(asset or "").strip().upper(), Decimal("0"))))

        def funding_asset_balance(self, asset: str) -> Decimal:
            return Decimal(str(self._funding_balances.get(str(asset or "").strip().upper(), Decimal("0"))))

        def collect_funding_asset_to_spot(
            self,
            asset: str,
            amount: Decimal | str | float | int | None = None,
        ) -> Decimal:
            asset_u = str(asset or "").strip().upper()
            available_amount = Decimal(str(self._funding_balances.get(asset_u, Decimal("0"))))
            if available_amount <= 0:
                return Decimal("0")
            transfer_amount = available_amount
            if amount is not None:
                transfer_amount = min(available_amount, Decimal(str(amount)))
            if transfer_amount <= 0:
                return Decimal("0")
            self.transfers.append(("FUNDING_MAIN", asset_u, transfer_amount))
            self._funding_balances[asset_u] = available_amount - transfer_amount
            self._spot_balances[asset_u] = Decimal(str(self._spot_balances.get(asset_u, Decimal("0")))) + transfer_amount
            return transfer_amount

    bnb_ready_client = _SelftestBnbFeeReadyClient(spot_bnb=Decimal("0.010"), funding_bnb=Decimal("0.500"))
    try:
        spot_bnb = bnb_ready_client.ensure_bnb_fee_ready_in_spot(
            min_spot_balance=Decimal("0.025"),
            max_transfer_amount=Decimal("0.015"),
            timeout_seconds=1.0,
        )
        if spot_bnb != Decimal("0.025"):
            raise RuntimeError(f"BNB手续费现货确认自检失败：现货BNB异常（{spot_bnb}）")
        if bnb_ready_client.funding_asset_balance("BNB") != Decimal("0.485"):
            raise RuntimeError("BNB手续费现货确认自检失败：资金账户BNB剩余异常")
        if bnb_ready_client.transfers != [("FUNDING_MAIN", "BNB", Decimal("0.015"))]:
            raise RuntimeError(f"BNB手续费现货确认自检失败：划转记录异常（{bnb_ready_client.transfers}）")
    finally:
        bnb_ready_client.close()
    checks.append("bnb-fee-ready=ok")

    bnb_ready_skip_client = _SelftestBnbFeeReadyClient(spot_bnb=Decimal("0.030"), funding_bnb=Decimal("0.500"))
    try:
        spot_bnb = bnb_ready_skip_client.ensure_bnb_fee_ready_in_spot(
            min_spot_balance=Decimal("0.025"),
            max_transfer_amount=Decimal("0.015"),
            timeout_seconds=1.0,
        )
        if spot_bnb != Decimal("0.030"):
            raise RuntimeError(f"BNB手续费现货确认自检失败：免划转现货BNB异常（{spot_bnb}）")
        if bnb_ready_skip_client.funding_asset_balance("BNB") != Decimal("0.500"):
            raise RuntimeError("BNB手续费现货确认自检失败：免划转资金账户BNB异常")
        if bnb_ready_skip_client.transfers:
            raise RuntimeError(f"BNB手续费现货确认自检失败：本不应划转却发生了划转（{bnb_ready_skip_client.transfers}）")
    finally:
        bnb_ready_skip_client.close()
    checks.append("bnb-fee-ready-skip=ok")

    collect_coin, collect_enable = ExchangeAppBatchMixin._collect_bnb_withdraw_runtime(
        force_bnb_withdraw=True,
        enable_withdraw=False,
        withdraw_coin="USDT",
    )
    if collect_coin != "BNB" or collect_enable is not True:
        raise RuntimeError("归集BNB并提现自检失败：强制提现参数未固定为 BNB/开启")
    checks.append("collect-bnb-withdraw=ok")

    class _SelftestSimpleEarnClient(BinanceClient):
        def __init__(self):
            super().__init__("selftest-key", "selftest-secret")
            self.requests_made: list[tuple[str, str, str, dict[str, object]]] = []

        def request(self, base, method, path, params=None):
            params_dict = dict(params or {})
            self.requests_made.append((str(base), str(method), str(path), params_dict))
            if method == "GET" and path == "/sapi/v1/simple-earn/flexible/position":
                current = int(params_dict.get("current", 1) or 1)
                if current == 1:
                    return {
                        "total": 3,
                        "rows": [
                            {
                                "productId": "P-USDT",
                                "asset": "usdt",
                                "totalAmount": "12.34",
                                "redeemingAmount": "0.1",
                                "canRedeem": True,
                                "autoSubscribe": False,
                            },
                            {
                                "productId": "P-BNB",
                                "asset": "bnb",
                                "totalAmount": "0.5",
                                "redeemingAmount": "0",
                                "canRedeem": "true",
                                "autoSubscribe": "1",
                            },
                        ],
                    }
                if current == 2:
                    return {
                        "total": 3,
                        "rows": [
                            {
                                "productId": "P-USDC",
                                "asset": "USDC",
                                "totalAmount": "8.88",
                                "redeemingAmount": "0",
                                "canRedeem": "false",
                                "autoSubscribe": "0",
                            }
                        ],
                    }
                return {"total": 3, "rows": []}
            if method == "POST" and path == "/sapi/v1/simple-earn/flexible/redeem":
                return {"success": True, "redeemId": "R-123"}
            if method == "POST" and path == "/sapi/v1/simple-earn/flexible/setAutoSubscribe":
                return {"success": True}
            raise RuntimeError(f"活期理财自检遇到未知请求：{method} {path}")

    simple_earn_client = _SelftestSimpleEarnClient()
    try:
        positions = simple_earn_client.get_simple_earn_flexible_positions()
        if len(positions) != 3:
            raise RuntimeError(f"活期理财持仓自检失败：分页数量异常（{positions}）")
        if positions[0].get("asset") != "USDT" or positions[0].get("totalAmount") != Decimal("12.34"):
            raise RuntimeError(f"活期理财持仓自检失败：USDT 记录解析异常（{positions[0]}）")
        if positions[1].get("canRedeem") is not True or positions[1].get("autoSubscribe") is not True:
            raise RuntimeError(f"活期理财持仓自检失败：布尔字段解析异常（{positions[1]}）")
        if positions[2].get("canRedeem") is not False or positions[2].get("asset") != "USDC":
            raise RuntimeError(f"活期理财持仓自检失败：USDC 记录解析异常（{positions[2]}）")

        filtered_positions = simple_earn_client.get_simple_earn_flexible_positions(asset="USDT")
        if len(filtered_positions) != 1 or filtered_positions[0].get("asset") != "USDT":
            raise RuntimeError(f"活期理财持仓自检失败：按资产过滤异常（{filtered_positions}）")

        redeem_result = simple_earn_client.redeem_simple_earn_flexible_product("P-USDT", redeem_all=True, dest_account="SPOT")
        if str(redeem_result.get("redeemId") or "") != "R-123":
            raise RuntimeError(f"活期理财赎回自检失败：返回结果异常（{redeem_result}）")
        redeem_requests = [
            req for req in simple_earn_client.requests_made
            if req[1] == "POST" and req[2] == "/sapi/v1/simple-earn/flexible/redeem"
        ]
        if not redeem_requests:
            raise RuntimeError("活期理财赎回自检失败：未发出赎回请求")
        redeem_params = redeem_requests[-1][3]
        if redeem_params != {"productId": "P-USDT", "redeemAll": "true", "destAccount": "SPOT"}:
            raise RuntimeError(f"活期理财赎回自检失败：请求参数异常（{redeem_params}）")

        auto_subscribe_result = simple_earn_client.set_simple_earn_flexible_auto_subscribe("P-BNB", False)
        if auto_subscribe_result != {"success": True}:
            raise RuntimeError(f"活期理财自动申购开关自检失败：返回结果异常（{auto_subscribe_result}）")
        auto_subscribe_requests = [
            req for req in simple_earn_client.requests_made
            if req[1] == "POST" and req[2] == "/sapi/v1/simple-earn/flexible/setAutoSubscribe"
        ]
        if not auto_subscribe_requests:
            raise RuntimeError("活期理财自动申购开关自检失败：未发出关闭请求")
        auto_subscribe_params = auto_subscribe_requests[-1][3]
        if auto_subscribe_params != {"productId": "P-BNB", "autoSubscribe": "false"}:
            raise RuntimeError(f"活期理财自动申购开关自检失败：请求参数异常（{auto_subscribe_params}）")
    finally:
        simple_earn_client.close()
    checks.append("simple-earn-flexible=ok")

    class _SelftestBatchSummaryProbe(ExchangeAppBatchMixin):
        pass

    summary_probe = _SelftestBatchSummaryProbe()
    redeem_summary_text = summary_probe._build_batch_summary_text(
        {
            "runner": "batch_redeem_flexible_earn",
            "results": {"a": True, "b": False, "c": None},
            "redeemed_assets_by_account": {"a": 2, "b": 1},
            "skipped_assets_by_account": {"a": 0, "b": 3},
            "auto_disabled_by_account": {"a": 2, "b": 1},
        },
        pending_as_failed=True,
    )
    expected_summary_text = "结果汇总：成功1 | 失败2 | 已赎回资产3 | 跳过资产3 | 关闭自动申购3"
    if redeem_summary_text != expected_summary_text:
        raise RuntimeError(f"理财赎回汇总自检失败：{redeem_summary_text}")
    checks.append("simple-earn-summary=ok")

def _emit_selftest_console(message: str, *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    try:
        print(message, file=stream, flush=True)
    except Exception:
        pass


def run_selftest(*, include_online_checks: bool = False, include_gui_checks: bool = False) -> int:
    client = None
    try:
        checks: list[str] = []

        OnchainTransferPage, onchain_import_error = _load_onchain_page_class()
        if OnchainTransferPage is None:
            raise RuntimeError(f"链上模块导入失败: {onchain_import_error}")
        checks.append("onchain-import")

        EvmClient.ensure_dependencies(require_signing=True)
        client = EvmClient()
        checks.append("evm-deps")

        wallet = client.create_wallet()
        if not wallet.address or not wallet.private_key:
            raise RuntimeError("钱包生成结果为空")
        derived_address = client.address_from_private_key(wallet.private_key)
        if client.normalize_address(wallet.address) != client.normalize_address(derived_address):
            raise RuntimeError("钱包生成校验失败：私钥反推地址不一致")
        zero_address = client.validate_evm_address("0x0000000000000000000000000000000000000000", "零地址")
        checks.append("wallet-gen=ok")
        checks.append(f"zero={zero_address}")
        checks.append(f"eth-chain={client.get_chain_id('ETH')}")
        checks.append(f"bsc-chain={client.get_chain_id('BSC')}")
        checks.append(f"eth-tokens={len(client.get_default_tokens('ETH'))}")
        checks.append(f"bsc-tokens={len(client.get_default_tokens('BSC'))}")
        _run_offline_business_selftest_checks(checks)
        if include_gui_checks:
            run_gui_selftest_checks(checks)
            checks.append("gui-check=enabled")
        else:
            checks.append("gui-check=skipped")
        if include_online_checks:
            run_online_selftest_checks(client, checks)
            checks.append("online-check=enabled")
        else:
            checks.append("online-check=skipped")

        session = requests.Session()
        session.trust_env = False
        session.proxies = {
            "http": "socks5://127.0.0.1:9",
            "https": "socks5://127.0.0.1:9",
        }
        try:
            session.get("http://example.com", timeout=1)
        except Exception as exc:
            if "Missing dependencies for SOCKS support" in str(exc):
                raise RuntimeError("requests 缺少 SOCKS 支持（PySocks 未打包）") from exc
        finally:
            session.close()
        checks.append("socks-support")

        try:
            xray_path = ExchangeProxyRuntime.find_xray_executable()
        except Exception as exc:
            if os.name == "nt" and getattr(sys, "frozen", False):
                raise
            logger.warning("SELFTEST: xray 未找到，将继续使用 sing-box 作为内置 SS 代理主后端: %s", exc)
            checks.append("xray=optional-missing")
        else:
            checks.append(f"xray={xray_path.name}")
        sing_box_path = ExchangeProxyRuntime.find_sing_box_executable()
        checks.append(f"sing-box={sing_box_path.name}")
        with tempfile.TemporaryDirectory(prefix="xiaojun-proxy-runtime-") as tmpdir:
            proxy_runtime = ExchangeProxyRuntime(Path(tmpdir), runtime_name="selftest")
            def _probe_or_skip(backend_name: str) -> None:
                try:
                    proxy_runtime.probe_backend_launch(backend_name)
                except Exception as exc:
                    if os.name == "nt" and getattr(sys, "frozen", False):
                        raise
                    lower_text = str(exc or "").lower()
                    if "operation not permitted" in lower_text or "permission denied" in lower_text:
                        logger.warning("SELFTEST: 跳过 %s 运行时拉起检查（当前环境禁止本地监听端口）: %s", backend_name, exc)
                        checks.append(f"{backend_name}-launch=skipped-permission")
                        return
                    raise
                checks.append(f"{backend_name}-launch=ok")

            if "xray_path" in locals():
                _probe_or_skip("xray")
            _probe_or_skip("sing-box")

        summary = ", ".join(checks)
        logger.info("SELFTEST OK: %s", summary)
        _emit_selftest_console(f"SELFTEST OK: {summary}")
        return 0
    except Exception as exc:
        logger.exception("SELFTEST FAILED: %s", exc)
        _emit_selftest_console(f"SELFTEST FAILED: {exc}", error=True)
        if runtime_log_path is not None:
            _emit_selftest_console(f"Runtime log: {runtime_log_path}", error=True)
        _emit_selftest_console(f"Compat log: {LOG_FILE_PATH}", error=True)
        return 1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
