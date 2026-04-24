#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import contextlib
import os
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import requests

from api_clients import EvmClient
from app_paths import LOG_FILE_PATH
from core_models import EvmToken, OnchainPairEntry, OnchainSettings, WithdrawRuntimeParams
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_binance_client import BinanceClient
from exchange_logging import logger, runtime_log_path
from onchain_relay_wallets import RelayWalletFileStore
from onchain_relay_runner import _record_matches_job
from exchange_proxy_runtime import ExchangeProxyRuntime
from secret_box import SECRET_BOX
from shared_utils import decimal_to_text, random_decimal_between
from stores import OnchainStore


def _load_onchain_page_class():
    try:
        from page_onchain import OnchainTransferPage
    except Exception as exc:
        return None, exc
    return OnchainTransferPage, None


def _run_online_selftest_checks(client: EvmClient, checks: list[str]) -> None:
    zero_address = "0x0000000000000000000000000000000000000000"
    eth_balance = client.get_balance_wei("ETH", zero_address)
    bsc_balance = client.get_balance_wei("BSC", zero_address)
    checks.append(f"eth-rpc={eth_balance}")
    checks.append(f"bsc-rpc={bsc_balance}")

def _run_offline_business_selftest_checks(checks: list[str]) -> None:
    secret_backend = SECRET_BOX.modern_encryption_backend_name()
    if secret_backend == "missing":
        raise RuntimeError("安全存储新版密文后端不可用：缺少 cryptography / pycryptodome")
    probe_text = "selftest-secret-box"
    v2_cipher = SECRET_BOX.encrypt(probe_text)
    if not str(v2_cipher).startswith(SECRET_BOX.PREFIX_V2):
        raise RuntimeError("安全存储自检失败：未生成新版密文")
    if SECRET_BOX.decrypt(v2_cipher) != probe_text:
        raise RuntimeError("安全存储自检失败：新版密文回读不一致")
    v1_cipher = SECRET_BOX._encrypt_v1(probe_text)
    if SECRET_BOX.decrypt(v1_cipher) != probe_text:
        raise RuntimeError("安全存储自检失败：旧版密文兼容回读失败")
    checks.append(f"secret-box={secret_backend}")

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
            return None

        def get_convert_pair_info(self, from_asset: str, to_asset: str):
            key = (str(from_asset or "").strip().upper(), str(to_asset or "").strip().upper())
            if key in {("USDT", "XAUT"), ("XAUT", "USDT")}:
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

def _emit_selftest_console(message: str, *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    try:
        print(message, file=stream, flush=True)
    except Exception:
        pass


@contextlib.contextmanager
def _temporary_attr_overrides(patches: list[tuple[object, str, object]]):
    sentinel = object()
    previous: list[tuple[object, str, object]] = []
    try:
        for owner, attr_name, value in patches:
            before = getattr(owner, attr_name, sentinel)
            previous.append((owner, attr_name, before))
            setattr(owner, attr_name, value)
        yield
    finally:
        for owner, attr_name, before in reversed(previous):
            if before is sentinel:
                try:
                    delattr(owner, attr_name)
                except Exception:
                    pass
            else:
                setattr(owner, attr_name, before)


def _pump_tk_events(app, *, timeout: float = 20.0, predicate=None) -> None:
    deadline = time.monotonic() + max(0.1, float(timeout))
    while time.monotonic() < deadline:
        app.update_idletasks()
        app.update()
        if predicate is None or predicate():
            return
        time.sleep(0.05)
    if predicate is not None and not predicate():
        raise RuntimeError("GUI 自检超时")


def _run_gui_selftest_checks(checks: list[str]) -> None:
    from tkinter import messagebox

    import exchange_app_base
    import exchange_app_config
    import onchain_imports
    import page_onchain_base
    from exchange_app import App

    dialogs: list[tuple[str, str, str]] = []

    def _dialog_stub(kind: str):
        def _inner(title, message, **_kwargs):
            dialogs.append((kind, str(title), str(message)))
            return True
        return _inner

    with tempfile.TemporaryDirectory(prefix="xiaojun-selftest-gui-") as tmpdir:
        tmp_root = Path(tmpdir)
        strategy_path = tmp_root / "exchange_strategy_settings.json"
        proxy_path = tmp_root / "exchange_proxy_settings.json"
        onchain_path = tmp_root / "onchain.json"
        patches = [
            (exchange_app_base.ExchangeAppBase, "update_ip", lambda self, schedule_next=True: None),
            (exchange_app_base, "STRATEGY_CONFIG_FILE", strategy_path),
            (exchange_app_base, "EXCHANGE_PROXY_CONFIG_FILE", proxy_path),
            (exchange_app_config, "STRATEGY_CONFIG_FILE", strategy_path),
            (exchange_app_config, "EXCHANGE_PROXY_CONFIG_FILE", proxy_path),
            (page_onchain_base, "ONCHAIN_DATA_FILE", onchain_path),
            (onchain_imports, "ONCHAIN_DATA_FILE", onchain_path),
            (messagebox, "showinfo", _dialog_stub("info")),
            (messagebox, "showwarning", _dialog_stub("warning")),
            (messagebox, "showerror", _dialog_stub("error")),
            (messagebox, "askyesno", lambda *_args, **_kwargs: True),
        ]
        with _temporary_attr_overrides(patches):
            app = None
            try:
                app = App()
                app.withdraw()
                _pump_tk_events(app, timeout=5.0)

                app.save_strategy_config()
                if not strategy_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成交易所策略配置文件")
                if not proxy_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成交易所代理配置文件")

                app._show_main_page("onchain")
                _pump_tk_events(app, timeout=5.0, predicate=lambda: getattr(app, "onchain_page", None) is not None)
                page = app.onchain_page
                if page is None:
                    raise RuntimeError("GUI 自检失败：链上页面未成功加载")

                if page._amount_mode() != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对多默认数量模式不是“全部”")
                page.mode_var.set(page.MODE_1M)
                _pump_tk_events(app, timeout=5.0)
                if page._amount_mode() != page.AMOUNT_MODE_FIXED or page.amount_var.get().strip():
                    raise RuntimeError("GUI 自检失败：1对多默认数量模式异常")
                page.amount_var.set("0.01")
                page.mode_var.set(page.MODE_M1)
                _pump_tk_events(app, timeout=5.0)
                if page._amount_mode() != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对1默认数量模式不是“全部”")
                page.mode_var.set(page.MODE_1M)
                _pump_tk_events(app, timeout=5.0)
                if page.amount_var.get().strip() != "0.01":
                    raise RuntimeError("GUI 自检失败：模式切换后1对多数量未保持独立")

                page.network_var.set("BSC")
                page.mode_var.set(page.MODE_M1)
                _pump_tk_events(app, timeout=5.0)
                page.save_all()
                if not onchain_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成链上配置文件")
                saved_store = OnchainStore(onchain_path)
                saved_store.load()
                if saved_store.settings.mode_amounts.get("multi_to_multi", {}).get("amount_mode") != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对多数量配置未保存")
                if saved_store.settings.mode_amounts.get("one_to_many", {}).get("amount") != "0.01":
                    raise RuntimeError("GUI 自检失败：1对多独立数量配置未保存")
                if saved_store.settings.mode_amounts.get("many_to_one", {}).get("amount_mode") != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对1独立数量配置未保存")

                page.open_wallet_generator()
                _pump_tk_events(app, timeout=5.0)
                page.wallet_generate_count_var.set("2")
                page.generate_wallets()
                _pump_tk_events(
                    app,
                    timeout=20.0,
                    predicate=lambda: len(getattr(page, "generated_wallets", [])) >= 2
                    and not any(t.is_alive() for t in page._managed_threads_snapshot()),
                )
                if len(page.generated_wallets) != 2:
                    raise RuntimeError("GUI 自检失败：钱包生成数量异常")
                error_dialogs = [item for item in dialogs if item[0] == "error"]
                if error_dialogs:
                    raise RuntimeError(f"GUI 自检失败：出现错误弹窗 {error_dialogs[0][2]}")
            finally:
                if app is not None:
                    try:
                        app.destroy()
                    except Exception:
                        pass

    checks.append("gui-smoke=ok")


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
            _run_gui_selftest_checks(checks)
            checks.append("gui-check=enabled")
        else:
            checks.append("gui-check=skipped")
        if include_online_checks:
            _run_online_selftest_checks(client, checks)
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
