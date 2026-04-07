#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

import requests

from api_clients import EvmClient
from app_paths import LOG_FILE_PATH
from core_models import OnchainPairEntry
from exchange_binance_client import BinanceClient
from exchange_logging import logger, runtime_log_path
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
    checks.append("onchain-drafts=ok")

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

def _emit_selftest_console(message: str, *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    try:
        print(message, file=stream, flush=True)
    except Exception:
        pass

def run_selftest(*, include_online_checks: bool = False) -> int:
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
