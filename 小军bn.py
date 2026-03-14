import time
import hmac
import hashlib
import logging
import ipaddress
from typing import Dict, Any, Optional
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import threading
import queue
import random
import os
import functools
import sys
import csv
import re

import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

try:
    from page_onchain import OnchainTransferPage
    _ONCHAIN_IMPORT_ERROR = None
except Exception as e:
    OnchainTransferPage = None
    _ONCHAIN_IMPORT_ERROR = e

# ====================== 默认配置 ======================
API_KEY_DEFAULT = ""
API_SECRET_DEFAULT = ""

SPOT_SYMBOL_DEFAULT = "BNBUSDT"
SPOT_ROUNDS_DEFAULT = 20

WITHDRAW_ADDRESS_DEFAULT = ""
WITHDRAW_NETWORK_DEFAULT = "BSC"
WITHDRAW_COIN_DEFAULT = "USDT"
WITHDRAW_FEE_BUFFER_DEFAULT = 0
WITHDRAW_NETWORK_OPTIONS = (
    "BSC",
    "ETH",
    "TRX",
    "ARB",
    "OP",
    "MATIC",
    "AVAXC",
    "SOL",
    "BASE",
    "LINEA",
    "ZKSYNCERA",
)
WITHDRAW_COIN_OPTIONS = (
    "BNB",
    "USDT",
    "USDC",
)

SPOT_PRECISION_DEFAULT = 3
MAX_THREADS_DEFAULT = 5

# ====================== 日志 & 队列 ======================
log_queue = queue.Queue()

_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)


class TkLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.put(msg)
        except Exception:
            pass


_tk_handler = TkLogHandler()
_tk_handler.setFormatter(_formatter)
logger.addHandler(_tk_handler)

try:
    _file_handler = logging.FileHandler("bot_log.txt", encoding="utf-8")
    _file_handler.setFormatter(_formatter)
    logger.addHandler(_file_handler)
except Exception as e:
    print(f"无法创建日志文件: {e}")


# ====================== 自定义错误 & 装饰器 ======================
class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance API error {code}: {msg}")


def retry_request(max_retries=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    logger.warning(f"网络请求失败 ({i + 1}/{max_retries}): {e}，将在 {delay} 秒后重试...")
                    time.sleep(delay)
                except BinanceAPIError as e:
                    last_exception = e
                    if e.code in [-1001, -1003]:
                        logger.warning(f"Binance 系统繁忙 ({i + 1}/{max_retries}): {e}，重试中...")
                        time.sleep(delay)
                    else:
                        raise e
            logger.error(f"重试 {max_retries} 次后仍然失败。")
            if last_exception:
                raise last_exception
            raise RuntimeError("未知错误：重试失败")
        return wrapper
    return decorator


# ====================== Binance 客户端 ======================
class BinanceClient:
    def __init__(self, key: str, secret: str):
        if not key or not secret:
            raise ValueError("API KEY / SECRET 不能为空")

        self.key = key
        self.secret = secret.encode()
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": key})

        self.spot = "https://api.binance.com"
        self.um_futures = "https://fapi.binance.com"
        self.cm_futures = "https://dapi.binance.com"

        self._exchange_info_cache = {}
        self._price_cache = {}

    def sign(self, params: Dict[str, Any]):
        return hmac.new(
            self.secret, urlencode(params, True).encode(), hashlib.sha256
        ).hexdigest()

    @retry_request(max_retries=3, delay=1)
    def request(self, base, method, path, params=None):
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self.sign(params)

        url = base + path

        if method == "GET":
            r = self.session.get(url, params=params, timeout=15)
        else:
            r = self.session.request(method, url, data=params, timeout=15)

        try:
            data = r.json()
        except Exception:
            r.raise_for_status()

        if r.status_code != 200:
            raise BinanceAPIError(data.get("code", -1), data.get("msg", "Unknown"))
        return data

    @retry_request(max_retries=3, delay=1)
    def public_get(self, base, path, params=None):
        params = params or {}
        url = base + path
        r = self.session.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # -------- 余额 --------
    def spot_balance(self, asset: str) -> float:
        data = self.request(self.spot, "GET", "/api/v3/account")
        for b in data.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", 0))
        return 0.0

    def spot_all_balances(self):
        data = self.request(self.spot, "GET", "/api/v3/account")
        result = []
        for b in data.get("balances", []):
            free_amt = float(b.get("free", 0) or 0)
            locked_amt = float(b.get("locked", 0) or 0)
            total = free_amt + locked_amt
            if total > 0:
                result.append({
                    "asset": b.get("asset"),
                    "free": free_amt,
                    "locked": locked_amt,
                    "total": total,
                })
        return result

    def query_total_wallet_balance(self, quote_asset="USDT"):
        data = self.request(
            self.spot,
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": quote_asset},
        )
        total = Decimal("0")
        rows = []
        for item in data:
            bal = Decimal(str(item.get("balance", "0")))
            wallet_name = item.get("walletName", "Unknown")
            active = bool(item.get("activate", False))
            rows.append({
                "walletName": wallet_name,
                "balance": bal,
                "activate": active,
            })
            total += bal
        return total, rows

    # -------- 工具：从 symbol 推断现货基础币种 --------
    @staticmethod
    def get_spot_base_asset(symbol: str) -> str:
        symbol = symbol.upper()
        suffixes = ["USDT", "BUSD", "USDC", "FDUSD", "BTC", "ETH", "BNB", "TRY", "EUR"]
        for s in suffixes:
            if symbol.endswith(s):
                return symbol[:-len(s)]
        return "BTC"

    # -------- 公共行情 / 交易规则 --------
    def get_exchange_info(self, symbol: str):
        symbol = symbol.upper()
        if symbol in self._exchange_info_cache:
            return self._exchange_info_cache[symbol]
        data = self.public_get(self.spot, "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = symbols[0]
        self._exchange_info_cache[symbol] = info
        return info

    def get_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol = symbol.upper()
        try:
            data = self.public_get(self.spot, "/api/v3/ticker/price", {"symbol": symbol})
            price = Decimal(str(data.get("price")))
            self._price_cache[symbol] = price
            return price
        except Exception:
            return None

    @staticmethod
    def _extract_filter(symbol_info: dict, filter_type: str):
        for f in symbol_info.get("filters", []):
            if f.get("filterType") == filter_type:
                return f
        return None

    @staticmethod
    def _decimal_from_str(v, default="0"):
        try:
            return Decimal(str(v))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(default)

    @staticmethod
    def _floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return qty
        return (qty // step) * step

    def get_symbol_trade_rules(self, symbol: str):
        info = self.get_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")

        step_size = self._decimal_from_str((lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((lot or {}).get("maxQty", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("minNotional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "minNotional": min_notional_val,
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
        }

    def sell_asset_market(self, symbol: str, free_balance: float, reserve_ratio=Decimal("0.999")) -> bool:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            logger.info("找不到交易对规则，跳过卖出 %s", symbol)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过", symbol)
            return False

        qty = Decimal(str(free_balance)) * reserve_ratio
        qty = self._floor_to_step(qty, rules["stepSize"])

        if qty <= 0 or qty < rules["minQty"]:
            logger.info("交易对 %s 数量过小，跳过卖出", symbol)
            return False

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol)
        if price:
            notional = qty * price
            if rules["minNotional"] > 0 and notional < rules["minNotional"]:
                logger.info("交易对 %s 名义价值 %.8f 小于最小下单额 %.8f，跳过",
                            symbol, float(notional), float(rules["minNotional"]))
                return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty.normalize(), "f"),
            },
        )
        logger.info("现货市价卖出 %s，数量 %s", symbol, format(qty.normalize(), "f"))
        return True

    def find_usdt_symbol_for_asset(self, asset: str) -> Optional[str]:
        asset = asset.upper()
        if asset in ("USDT", "BNB"):
            return None
        symbol = f"{asset}USDT"
        info = self.get_exchange_info(symbol)
        if info and info.get("status") == "TRADING":
            return symbol
        return None

    def sell_large_spot_assets_to_usdt(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        sold_assets = []
        balances = self.spot_all_balances()
        for item in balances:
            asset = (item.get("asset") or "").upper()
            free_amt = float(item.get("free", 0) or 0)

            if asset in skip_assets or free_amt <= 0:
                continue

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                logger.info("未找到 %sUSDT 交易对，跳过 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    sold_assets.append(asset)
                    time.sleep(0.3)
            except Exception as e:
                logger.warning("卖出大额币 %s 失败: %s", asset, e)

        logger.info("大额币卖出为 USDT 完成，成功处理 %d 个币种", len(sold_assets))
        return sold_assets

    # -------- 现货买入（全部 USDT） --------
    def spot_buy_all_usdt(self, buffer=0.2, symbol="BTCUSDT"):
        usdt = self.spot_balance("USDT")
        if usdt <= buffer:
            logger.info("现货 USDT %.8f <= buffer %.8f，跳过买入", usdt, buffer)
            return False

        amount = (usdt - buffer) * 0.999
        if amount <= 0:
            logger.info("可用 USDT 金额太小，跳过买入")
            return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{amount:.8f}",
            },
        )
        logger.info("现货市价买入 %s，使用 USDT 金额 %.8f", symbol, amount)
        return True

    # -------- 现货卖出（全部基础币，精度由参数决定） --------
    def spot_sell_all_base(self, symbol: str, precision: int):
        base = self.get_spot_base_asset(symbol)
        balance = self.spot_balance(base)

        if balance <= 0:
            return False

        qty = Decimal(str(balance)) * Decimal("0.999")
        if precision < 0:
            precision = 0
        step = Decimal("1").scaleb(-precision)
        qty = qty.quantize(step, rounding=ROUND_DOWN)

        if qty <= step:
            return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": str(qty),
            },
        )
        logger.info("现货市价卖出 %s 数量 %s（基础币 %s）", symbol, qty, base)
        return True

    # -------- 提现 --------
    def withdraw_all_coin(
        self,
        coin: str,
        address: str,
        network: str,
        fee_buffer: float = WITHDRAW_FEE_BUFFER_DEFAULT,
        enable_withdraw: bool = True,
    ) -> float:
        balance = self.spot_balance(coin)
        amount = balance - fee_buffer
        if amount <= 0:
            logger.info("%s 余额 %.8f 不足以提现（需 > buffer %.8f）", coin, balance, fee_buffer)
            return 0.0

        params = {
            "coin": coin,
            "address": address,
            "network": network,
            "amount": f"{amount:.8f}",
        }

        if enable_withdraw:
            self.request(
                self.spot,
                "POST",
                "/sapi/v1/capital/withdraw/apply",
                params,
            )
            logger.info("已提交提现 %.8f %s → %s (%s)", amount, coin, address, network)
            return amount
        else:
            logger.info("提现开关关闭，仅打印参数: %s", params)
            return 0.0

    # -------- 现货正余额资产 --------
    def spot_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v3/asset/getUserAsset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- 资金账户正余额资产 --------
    def funding_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- U本位合约可转出余额 --------
    def um_futures_transferable_assets(self):
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v2/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("maxWithdrawAmount", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 币本位合约可转出余额 --------
    def cm_futures_transferable_assets(self):
        data = self.request(
            self.cm_futures,
            "GET",
            "/dapi/v1/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("withdrawAvailable", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 通用划转 --------
    def universal_transfer(self, transfer_type: str, asset: str, amount: float):
        if amount <= 0:
            return False

        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return False

        amt_str = format(amount_dec.normalize(), "f")
        if amt_str == "0":
            return False

        self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/transfer",
            {
                "type": transfer_type,
                "asset": asset,
                "amount": amt_str,
            },
        )
        logger.info("划转成功: %s %s %s", transfer_type, asset, amt_str)
        return True

    # -------- 归集合约/资金到现货 --------
    def collect_all_to_spot(self):
        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            if items:
                logger.info("检测到 U本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["amount"]
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("U本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 U本位合约余额失败: %s", e)

        try:
            items = self.cm_futures_transferable_assets()
            if items:
                logger.info("检测到 币本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["amount"]
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("币本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 币本位合约余额失败: %s", e)

        try:
            items = self.funding_positive_assets()
            if items:
                logger.info("检测到 资金账户可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["free"]
                try:
                    self.universal_transfer("FUNDING_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("资金账户划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询资金账户余额失败: %s", e)

        logger.info("归集到现货完成，共处理 %d 项资产", total_count)
        return total_count

    # -------- 查询现货可小额兑换资产 --------
    def get_spot_dust_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust-btc",
            {"accountType": "SPOT"},
        )

        assets = []
        for item in data.get("details", []):
            asset = item.get("asset")
            amount_free = float(item.get("amountFree", 0) or 0)
            if asset and amount_free > 0 and asset != "BNB":
                assets.append(asset)
        return assets

    # -------- 小额兑换：现货可兑换资产 -> BNB --------
    def convert_spot_dust_to_bnb(self):
        assets = self.get_spot_dust_assets()
        if not assets:
            logger.info("没有可进行小额兑换的现货资产")
            return []

        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust",
            {
                "asset": assets,
                "accountType": "SPOT",
            },
        )
        result = data.get("transferResult", [])
        logger.info("小额兑换完成，兑换资产数: %d", len(result))
        return result


# ====================== 策略 ======================
class Strategy:
    def __init__(
        self,
        client,
        spot_rounds,
        withdraw_coin,
        withdraw_address,
        withdraw_network,
        withdraw_fee_buffer,
        spot_symbol,
        spot_precision,
        sleep_fn,
        enable_withdraw,
        withdraw_callback=None,
    ):
        self.c = client
        self.spot_rounds = spot_rounds
        self.withdraw_coin = withdraw_coin
        self.withdraw_address = withdraw_address
        self.withdraw_network = withdraw_network
        self.withdraw_fee_buffer = withdraw_fee_buffer
        self.spot_symbol = spot_symbol
        self.spot_precision = spot_precision
        self.sleep_fn = sleep_fn
        self.enable_withdraw = enable_withdraw
        self.withdraw_callback = withdraw_callback

    def ensure_base_sold(self):
        try:
            sold = self.c.spot_sell_all_base(self.spot_symbol, self.spot_precision)
            if sold:
                logger.info("【补救措施】检测到残留基础币，已执行补充卖出。")
        except Exception as e:
            logger.warning(f"补救卖出时发生错误（可忽略）: {e}")

    def run(self, stop_event, progress_cb=None):
        total_steps = self.spot_rounds if self.spot_rounds > 0 else 1
        step = 0
        withdraw_amount = 0.0
        withdraw_error = ""
        withdraw_attempted = False

        self.ensure_base_sold()

        for i in range(self.spot_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（现货阶段）")
                return {
                    "withdraw_amount": withdraw_amount,
                    "withdraw_error": withdraw_error,
                    "withdraw_attempted": withdraw_attempted,
                }

            self.ensure_base_sold()
            logger.info("--- 现货轮 %d/%d 开始 ---", i + 1, self.spot_rounds)

            try:
                buy_success = self.c.spot_buy_all_usdt(symbol=self.spot_symbol)
                if buy_success:
                    self.sleep_fn()
                    self.c.spot_sell_all_base(symbol=self.spot_symbol, precision=self.spot_precision)
                else:
                    logger.info("买入未执行（可能余额不足），跳过本轮卖出")

                logger.info("--- 现货轮 %d 完成 ---", i + 1)

            except Exception as e:
                logger.error(f"现货轮 %d 执行异常: {e}", i + 1)
                time.sleep(3)

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "现货轮 %d/%d" % (i + 1, self.spot_rounds))
            self.sleep_fn()

        self.ensure_base_sold()

        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过最终提现")
            return {
                "withdraw_amount": withdraw_amount,
                "withdraw_error": withdraw_error,
                "withdraw_attempted": withdraw_attempted,
            }

        logger.info(f"开始最终提现所有 {self.withdraw_coin}")
        withdraw_attempted = True
        try:
            withdraw_amount = self.c.withdraw_all_coin(
                coin=self.withdraw_coin,
                address=self.withdraw_address,
                network=self.withdraw_network,
                fee_buffer=self.withdraw_fee_buffer,
                enable_withdraw=self.enable_withdraw,
            )
            if self.withdraw_callback:
                self.withdraw_callback(withdraw_amount)
        except Exception as e:
            withdraw_error = str(e)
            logger.error(f"提现阶段异常: {e}")

        logger.info("策略执行完毕")
        return {
            "withdraw_amount": withdraw_amount,
            "withdraw_error": withdraw_error,
            "withdraw_attempted": withdraw_attempted,
        }


# ====================== GUI 应用 ======================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Binance 自动交易机器人（增强版 GUI）")
        self.geometry("1320x920")

        self.client = None
        self.worker_thread = None
        self.stop_event = None

        self.accounts = []
        self.total_asset_results = {}

        self._build_ui()
        self.after(100, self._poll_log_queue)
        self.update_ip()

    def _build_ui(self):
        self.main_tabs = ttk.Notebook(self)
        self.main_tabs.pack(fill="both", expand=True, padx=8, pady=8)

        self.exchange_tab = ttk.Frame(self.main_tabs)
        self.onchain_tab = ttk.Frame(self.main_tabs)
        self.main_tabs.add(self.exchange_tab, text="交易所批量")
        self.main_tabs.add(self.onchain_tab, text="链上")

        frame_ip = ttk.Frame(self.exchange_tab)
        frame_ip.pack(fill="x", padx=10, pady=3)
        ttk.Label(frame_ip, text="本机公网 IP：").pack(side="left")
        self.ip_var = tk.StringVar(value="获取中...")
        ttk.Label(frame_ip, textvariable=self.ip_var).pack(side="left")

        frame_top = ttk.LabelFrame(self.exchange_tab, text="策略配置（单账号 & 批量共享）")
        frame_top.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_top, text="API KEY:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(frame_top, text="API SECRET:").grid(row=1, column=0, sticky="w", padx=5, pady=2)

        self.api_key_var = tk.StringVar(value=API_KEY_DEFAULT)
        self.api_secret_var = tk.StringVar(value=API_SECRET_DEFAULT)

        ttk.Entry(frame_top, textvariable=self.api_key_var, width=50).grid(row=0, column=1, sticky="w", padx=5)
        ttk.Entry(frame_top, textvariable=self.api_secret_var, width=50, show="*").grid(row=1, column=1, sticky="w", padx=5)

        ttk.Label(frame_top, text="现货轮数:").grid(row=0, column=2, sticky="e", padx=5)
        self.spot_rounds_var = tk.IntVar(value=SPOT_ROUNDS_DEFAULT)
        ttk.Spinbox(frame_top, from_=0, to=100, textvariable=self.spot_rounds_var, width=6).grid(row=0, column=3, sticky="w")

        ttk.Label(frame_top, text="现货交易对:").grid(row=1, column=2, sticky="e", padx=5)
        self.spot_symbol_var = tk.StringVar(value=SPOT_SYMBOL_DEFAULT)
        ttk.Entry(frame_top, textvariable=self.spot_symbol_var, width=12).grid(row=1, column=3, sticky="w", padx=5)

        ttk.Label(frame_top, text="现货数量精度(小数位):").grid(row=1, column=4, sticky="w", padx=5)
        self.spot_precision_var = tk.IntVar(value=SPOT_PRECISION_DEFAULT)
        ttk.Entry(frame_top, textvariable=self.spot_precision_var, width=6).grid(row=1, column=5, sticky="w", padx=5)

        ttk.Label(frame_top, text="提现地址(单账号默认):").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ttk.Label(frame_top, text="网络:").grid(row=2, column=2, sticky="e", padx=5)
        ttk.Label(frame_top, text="提现币种:").grid(row=2, column=4, sticky="e", padx=5)

        self.withdraw_addr_var = tk.StringVar(value=WITHDRAW_ADDRESS_DEFAULT)
        self.withdraw_net_var = tk.StringVar(value=WITHDRAW_NETWORK_DEFAULT)
        self.withdraw_coin_var = tk.StringVar(value=WITHDRAW_COIN_DEFAULT)
        self.withdraw_buffer_var = tk.DoubleVar(value=WITHDRAW_FEE_BUFFER_DEFAULT)
        self.enable_withdraw_var = tk.BooleanVar(value=True)

        ttk.Entry(frame_top, textvariable=self.withdraw_addr_var, width=40).grid(row=2, column=1, sticky="w", padx=5)
        self.withdraw_net_combo = ttk.Combobox(
            frame_top,
            textvariable=self.withdraw_net_var,
            values=WITHDRAW_NETWORK_OPTIONS,
            width=10,
            state="readonly",
        )
        self.withdraw_net_combo.grid(row=2, column=3, sticky="w", padx=5)
        self.withdraw_coin_combo = ttk.Combobox(
            frame_top,
            textvariable=self.withdraw_coin_var,
            values=WITHDRAW_COIN_OPTIONS,
            width=8,
            state="readonly",
        )
        self.withdraw_coin_combo.grid(row=2, column=5, sticky="w", padx=5)

        ttk.Label(frame_top, text="手续费预留:").grid(row=2, column=6, sticky="e", padx=5)
        ttk.Entry(frame_top, textvariable=self.withdraw_buffer_var, width=6).grid(row=2, column=7, sticky="w", padx=5)

        ttk.Checkbutton(frame_top, text="自动提现", variable=self.enable_withdraw_var).grid(row=2, column=8, sticky="w", padx=5)

        ttk.Label(frame_top, text="随机延迟(毫秒) 最小:").grid(row=3, column=0, sticky="w", padx=5)
        ttk.Label(frame_top, text="最大:").grid(row=3, column=2, sticky="w", padx=5)

        self.min_delay_var = tk.IntVar(value=1000)
        self.max_delay_var = tk.IntVar(value=3000)

        ttk.Entry(frame_top, textvariable=self.min_delay_var, width=10).grid(row=3, column=1, padx=5)
        ttk.Entry(frame_top, textvariable=self.max_delay_var, width=10).grid(row=3, column=3, padx=5)

        ttk.Label(frame_top, text="USDT 到账超时(秒):").grid(row=3, column=4, sticky="e", padx=5)
        self.usdt_timeout_var = tk.IntVar(value=30)
        ttk.Entry(frame_top, textvariable=self.usdt_timeout_var, width=8).grid(row=3, column=5, padx=5)

        frame_mid = ttk.LabelFrame(self.exchange_tab, text="单账号控制 & 状态")
        frame_mid.pack(fill="x", padx=10, pady=5)

        self.btn_start = ttk.Button(frame_mid, text="开始运行（当前 API）", command=self.start_bot)
        self.btn_stop = ttk.Button(frame_mid, text="停止运行", command=self.stop_bot, state="disabled")
        self.btn_refresh = ttk.Button(frame_mid, text="刷新余额（当前 API）", command=self.refresh_balances)
        self.btn_withdraw = ttk.Button(frame_mid, text="手动提现", command=self.manual_withdraw)

        self.btn_start.grid(row=0, column=0, padx=5, pady=5)
        self.btn_stop.grid(row=0, column=1, padx=5, pady=5)
        self.btn_refresh.grid(row=0, column=2, padx=5, pady=5)
        self.btn_withdraw.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(frame_mid, text="现货 USDT:").grid(row=1, column=0, sticky="e", padx=5)
        ttk.Label(frame_mid, text="现货 基础币:").grid(row=1, column=2, sticky="e", padx=5)

        self.spot_usdt_var = tk.StringVar(value="--")
        self.spot_base_var = tk.StringVar(value="--")

        ttk.Label(frame_mid, textvariable=self.spot_usdt_var).grid(row=1, column=1, sticky="w")
        ttk.Label(frame_mid, textvariable=self.spot_base_var).grid(row=1, column=3, sticky="w")

        self.progress = ttk.Progressbar(frame_mid, orient="horizontal", mode="determinate")
        self.progress.grid(row=2, column=0, columnspan=6, sticky="ew", padx=5, pady=5)
        self.status_var = tk.StringVar(value="状态：空闲")
        ttk.Label(frame_mid, textvariable=self.status_var).grid(row=3, column=0, columnspan=6, sticky="w", padx=5, pady=2)

        frame_acc = ttk.LabelFrame(self.exchange_tab, text="账号列表管理（批量 API + 提现地址）")
        frame_acc.pack(fill="both", expand=True, padx=10, pady=5)

        self.acc_api_key_var = tk.StringVar()
        self.acc_api_secret_var = tk.StringVar()
        self.acc_withdraw_addr_var = tk.StringVar()
        self.acc_network_var = self.withdraw_net_var

        frame_acc_input = ttk.Frame(frame_acc)
        frame_acc_input.pack(fill="x", padx=5, pady=5)

        ttk.Label(frame_acc_input, text="API KEY:").pack(side="left")
        ttk.Entry(frame_acc_input, textvariable=self.acc_api_key_var, width=20).pack(side="left", padx=2)
        ttk.Label(frame_acc_input, text="SECRET:").pack(side="left")
        ttk.Entry(frame_acc_input, textvariable=self.acc_api_secret_var, width=20, show="*").pack(side="left", padx=2)
        ttk.Label(frame_acc_input, text="地址:").pack(side="left")
        ttk.Entry(frame_acc_input, textvariable=self.acc_withdraw_addr_var, width=25).pack(side="left", padx=2)
        ttk.Label(frame_acc_input, text="网络:").pack(side="left")
        self.acc_network_combo = ttk.Combobox(
            frame_acc_input,
            textvariable=self.acc_network_var,
            values=WITHDRAW_NETWORK_OPTIONS,
            width=8,
            state="readonly",
        )
        self.acc_network_combo.pack(side="left", padx=2)

        self.withdraw_net_var.trace_add("write", self._on_global_network_changed)

        self.btn_add_account = ttk.Button(frame_acc_input, text="添加", command=self.add_account_to_list)
        self.btn_add_account.pack(side="left", padx=5)

        header = ttk.Frame(frame_acc)
        header.pack(fill="x", padx=5)
        ttk.Label(header, text="No.", width=4).pack(side="left", padx=2)
        ttk.Label(header, text="选", width=4).pack(side="left", padx=2)
        ttk.Label(header, text="API KEY", width=25).pack(side="left", padx=2)
        ttk.Label(header, text="提现地址", width=35).pack(side="left", padx=2)
        ttk.Label(header, text="网络", width=8).pack(side="left", padx=2)
        ttk.Label(header, text="状态", width=30).pack(side="left", padx=2)

        self.frame_list_canvas = ttk.Frame(frame_acc)
        self.frame_list_canvas.pack(fill="both", expand=True, padx=5, pady=2)

        self.canvas = tk.Canvas(self.frame_list_canvas, height=190, bg="#f0f0f0")
        self.scrollbar = ttk.Scrollbar(self.frame_list_canvas, orient="vertical", command=self.canvas.yview)

        self.accounts_container = ttk.Frame(self.canvas)

        self.accounts_container.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.accounts_container, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        self._setup_account_list_mousewheel_bindings()

        frame_batch_ctrl = ttk.Frame(frame_acc)
        frame_batch_ctrl.pack(fill="x", padx=5, pady=5)

        ttk.Button(frame_batch_ctrl, text="全选", width=6, command=self.select_all_accounts).pack(side="left", padx=2)
        ttk.Button(frame_batch_ctrl, text="全不选", width=6, command=self.deselect_all_accounts).pack(side="left", padx=2)

        ttk.Separator(frame_batch_ctrl, orient="vertical").pack(side="left", padx=5, fill="y")

        self.btn_run_accounts = ttk.Button(frame_batch_ctrl, text="批量执行", command=self.run_selected_accounts)
        self.btn_run_accounts.pack(side="left", padx=5)

        ttk.Label(frame_batch_ctrl, text="线程数:").pack(side="left", padx=2)
        self.max_threads_var = tk.IntVar(value=MAX_THREADS_DEFAULT)
        ttk.Spinbox(frame_batch_ctrl, from_=1, to=50, textvariable=self.max_threads_var, width=3).pack(side="left", padx=2)

        self.btn_del_accounts = ttk.Button(frame_batch_ctrl, text="删除选中", command=self.delete_selected_accounts)
        self.btn_del_accounts.pack(side="left", padx=5)

        self.btn_export_accounts = ttk.Button(frame_batch_ctrl, text="导出", command=self.export_accounts)
        self.btn_export_accounts.pack(side="left", padx=5)

        self.btn_import_accounts = ttk.Button(frame_batch_ctrl, text="导入", command=self.import_accounts)
        self.btn_import_accounts.pack(side="left", padx=5)

        self.btn_paste_accounts = ttk.Button(frame_batch_ctrl, text="粘贴导入", command=self.import_accounts_from_clipboard)
        self.btn_paste_accounts.pack(side="left", padx=5)

        self.btn_export_asset_csv = ttk.Button(frame_batch_ctrl, text="导出总资产CSV", command=self.export_total_asset_csv)
        self.btn_export_asset_csv.pack(side="left", padx=5)

        self.btn_batch_withdraw = ttk.Button(frame_batch_ctrl, text="批量提现", command=self.batch_manual_withdraw)
        self.btn_batch_withdraw.pack(side="left", padx=5)

        self.batch_total_asset_only_var = tk.BooleanVar(value=False)
        self.batch_collect_bnb_mode_var = tk.BooleanVar(value=False)
        self.skip_usdt_wait_in_batch_var = tk.BooleanVar(value=False)
        self.batch_sell_large_spot_to_bnb_var = tk.BooleanVar(value=False)

        frame_batch_opts = ttk.Frame(frame_acc)
        frame_batch_opts.pack(fill="x", padx=5, pady=(0, 5))

        ttk.Checkbutton(
            frame_batch_opts,
            text="批量查询总资产（只运行这个功能）",
            variable=self.batch_total_asset_only_var
        ).pack(side="left", padx=(0, 12))

        ttk.Checkbutton(
            frame_batch_opts,
            text="批量归集BNB模式",
            variable=self.batch_collect_bnb_mode_var
        ).pack(side="left", padx=(0, 12))

        ttk.Checkbutton(
            frame_batch_opts,
            text="批量策略跳过USDT检测",
            variable=self.skip_usdt_wait_in_batch_var
        ).pack(side="left", padx=(0, 12))

        ttk.Checkbutton(
            frame_batch_opts,
            text="归集BNB模式下：卖大额币买BNB",
            variable=self.batch_sell_large_spot_to_bnb_var
        ).pack(side="left", padx=(0, 12))

        frame_log = ttk.LabelFrame(self.exchange_tab, text="运行日志")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.text_log = tk.Text(frame_log, wrap="word", height=10, state="disabled")
        self.text_log.pack(fill="both", expand=True, side="left")

        scrollbar_log = ttk.Scrollbar(frame_log, command=self.text_log.yview)
        scrollbar_log.pack(side="right", fill="y")
        self.text_log["yscrollcommand"] = scrollbar_log.set

        onchain_shell = ttk.Frame(self.onchain_tab)
        onchain_shell.pack(fill="both", expand=True)
        onchain_intro = ttk.LabelFrame(onchain_shell, text="链上模块")
        onchain_intro.pack(fill="x", padx=10, pady=(8, 6))
        ttk.Label(onchain_intro, text="该页面为独立链上批量转账模块，与交易所批量页面配置互不影响。", foreground="#666").pack(
            anchor="w", padx=8, pady=(6, 6)
        )

        onchain_body = ttk.Frame(onchain_shell)
        onchain_body.pack(fill="both", expand=True, padx=2, pady=(0, 2))

        if OnchainTransferPage is not None:
            try:
                self.onchain_page = OnchainTransferPage(onchain_body)
            except Exception as exc:
                self.onchain_page = None
                logger.exception("链上页面初始化失败: %s", exc)
                fail_box = ttk.LabelFrame(onchain_body, text="链上模块加载失败")
                fail_box.pack(fill="both", expand=True, padx=12, pady=12)
                ttk.Label(fail_box, text=f"链上页面加载失败：{exc}").pack(anchor="w", padx=8, pady=(8, 4))
                ttk.Label(fail_box, text="请检查运行依赖：eth-account、eth-utils").pack(anchor="w", padx=8, pady=(0, 8))
        else:
            self.onchain_page = None
            fail_box = ttk.LabelFrame(onchain_body, text="链上模块加载失败")
            fail_box.pack(fill="both", expand=True, padx=12, pady=12)
            ttk.Label(fail_box, text=f"链上页面导入失败：{_ONCHAIN_IMPORT_ERROR}").pack(anchor="w", padx=8, pady=(8, 4))
            ttk.Label(fail_box, text="请检查运行依赖：eth-account、eth-utils").pack(anchor="w", padx=8, pady=(0, 8))

        # 快捷键：Ctrl+V / Cmd+V 直接触发“从剪贴板导入账号”
        self.bind_all("<Control-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Control-V>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-V>", self._on_paste_shortcut, add="+")

    def _setup_account_list_mousewheel_bindings(self):
        # 账号列表区域内，鼠标滚轮可直接滚动，无需命中右侧滚动条
        self.bind_all("<MouseWheel>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-4>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-5>", self._on_account_list_mousewheel, add="+")

    def _pointer_in_account_list(self):
        frame = getattr(self, "frame_list_canvas", None)
        if frame is None or not frame.winfo_exists():
            return False
        try:
            x = self.winfo_pointerx()
            y = self.winfo_pointery()
            left = frame.winfo_rootx()
            top = frame.winfo_rooty()
            right = left + frame.winfo_width()
            bottom = top + frame.winfo_height()
            return left <= x <= right and top <= y <= bottom
        except Exception:
            return False

    def _on_account_list_mousewheel(self, event=None):
        if not self._pointer_in_account_list():
            return None
        if not hasattr(self, "canvas"):
            return None

        units = 0
        num = getattr(event, "num", None)
        if num == 4:
            units = -1
        elif num == 5:
            units = 1
        else:
            delta = int(getattr(event, "delta", 0) or 0)
            if delta != 0:
                if sys.platform == "darwin":
                    units = -1 if delta > 0 else 1
                else:
                    units = -int(delta / 120) if abs(delta) >= 120 else (-1 if delta > 0 else 1)

        if units == 0:
            return None

        try:
            self.canvas.yview_scroll(units, "units")
            return "break"
        except Exception:
            return None

    def random_sleep(self, min_ms, max_ms):
        if max_ms < min_ms:
            min_ms, max_ms = max_ms, min_ms
        delay_ms = random.randint(min_ms, max_ms)
        time.sleep(delay_ms / 1000.0)

    def _mask_key(self, key, prefix=6, suffix=4):
        if len(key) <= prefix + suffix:
            return key
        return key[:prefix] + "..." + key[-suffix:]

    def _mask_addr(self, addr, prefix=6, suffix=4):
        if len(addr) <= prefix + suffix:
            return addr
        return addr[:prefix] + "..." + addr[-suffix:]

    def _on_paste_shortcut(self, event=None):
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            return None

        parsed, err_msg = self._parse_accounts_from_text(raw)
        if parsed:
            self._import_accounts_from_text(raw, "剪贴板")
            return "break"

        focus = self.focus_get()
        if focus is not None:
            widget_class = str(focus.winfo_class())
            if widget_class in {"Entry", "TEntry", "Text", "TCombobox", "Spinbox", "TSpinbox"}:
                return None

        if err_msg:
            messagebox.showerror("错误", f"剪贴板导入失败：{err_msg}")
            return "break"
        return None

    def _parse_accounts_from_text(self, raw_text):
        if raw_text is None:
            return [], "内容为空"

        normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
        tokens = []

        for raw_line in normalized.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    tokens.extend(parts[:3])
                    continue

            # 支持“行尾备注/中文说明”，只提取长 token（API KEY/SECRET/地址）
            found = re.findall(r"0x[a-fA-F0-9]{40}|[A-Za-z0-9]{24,}", line)
            if found:
                tokens.extend(found)

        if not tokens:
            return [], "没有识别到账号数据"

        if len(tokens) % 3 != 0:
            return [], f"识别到 {len(tokens)} 条有效字段，必须按 3 条一组：APIKEY / APISECRET / 提现地址"

        accounts = []
        for i in range(0, len(tokens), 3):
            key, secret, addr = tokens[i], tokens[i + 1], tokens[i + 2]
            accounts.append((key, secret, addr))
        return accounts, ""

    def _import_accounts_from_text(self, raw_text, source_name):
        parsed, err_msg = self._parse_accounts_from_text(raw_text)
        if err_msg:
            messagebox.showerror("错误", f"{source_name}导入失败：{err_msg}")
            return 0

        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for key, secret, addr in parsed:
            self._append_account_row(key, secret, addr, net)

        self._reindex_accounts()
        logger.info("从%s导入账号数量：%d", source_name, len(parsed))
        messagebox.showinfo("成功", f"从{source_name}导入账号数量：{len(parsed)}")
        return len(parsed)

    def record_withdraw(self, index, api_key, address, amount):
        line = f"{index}+{api_key}+{address}+{amount:.8f}\n"
        try:
            fname = "withdraw_success.txt"
            with open(fname, "a", encoding="utf-8") as f:
                f.write(line)
            logger.info("已记录提现到 %s：%s", fname, line.strip())
        except Exception as e:
            logger.error("写入提现记录文件失败: %s", e)

    def record_total_asset(self, index, api_key, address, network, total_usdt):
        total_dec = Decimal(str(total_usdt))
        line = f"{index}+{api_key}+{total_dec:.8f}\n"

        try:
            fname = "total_asset_result.txt"
            with open(fname, "a", encoding="utf-8") as f:
                f.write(line)
            logger.info("已记录总资产到 %s：%s", fname, line.strip())
        except Exception as e:
            logger.error("写入总资产记录文件失败: %s", e)

        self.total_asset_results[index] = {
            "index": index,
            "api_key": api_key,
            "address": address,
            "network": network,
            "total_usdt": total_dec,
        }

    def export_total_asset_csv(self):
        if not self.total_asset_results:
            messagebox.showinfo("提示", "当前没有可导出的总资产结果，请先运行一次“批量查询总资产”")
            return

        path = filedialog.asksaveasfilename(
            title="导出总资产 CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            rows = sorted(self.total_asset_results.values(), key=lambda x: x["index"])

            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "API_KEY", "提现地址", "网络", "总资产(USDT)"])
                for row in rows:
                    writer.writerow([
                        row["index"],
                        row["api_key"],
                        row["address"],
                        row["network"],
                        f'{row["total_usdt"]:.8f}',
                    ])

            logger.info("总资产 CSV 已导出到：%s", path)
            messagebox.showinfo("成功", f"总资产 CSV 已导出到：\n{path}")
        except Exception as e:
            logger.error("导出总资产 CSV 失败: %s", e)
            messagebox.showerror("错误", "导出总资产 CSV 失败: %s" % e)

    @staticmethod
    def _fetch_public_ip() -> str:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://ipinfo.io/ip",
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        for url in urls:
            try:
                r = requests.get(url, headers=headers, timeout=6)
                r.raise_for_status()
                ip = (r.text or "").strip()
                ipaddress.ip_address(ip)
                return ip
            except Exception:
                continue
        raise RuntimeError("网络不可达或 IP 服务异常")

    def update_ip(self):
        def worker():
            try:
                ip = self._fetch_public_ip()
            except Exception as e:
                ip = "获取失败: %s" % str(e)

            def _update():
                self.ip_var.set(ip)
            self.after(0, _update)

        threading.Thread(target=worker, daemon=True).start()
        self.after(60000, self.update_ip)

    def _poll_log_queue(self):
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                self._append_log(msg)
        self.after(100, self._poll_log_queue)

    def _append_log(self, msg: str):
        self.text_log.configure(state="normal")
        self.text_log.insert("end", msg + "\n")
        self.text_log.see("end")
        self.text_log.configure(state="disabled")

    def _set_account_manage_buttons_state(self, state):
        for name in (
            "btn_add_account",
            "btn_del_accounts",
            "btn_export_accounts",
            "btn_import_accounts",
            "btn_paste_accounts",
        ):
            btn = getattr(self, name, None)
            if btn is not None:
                try:
                    btn.config(state=state)
                except Exception:
                    pass

    def _set_combo_states_for_run(self, is_running):
        state = "disabled" if is_running else "readonly"
        for name in (
            "withdraw_net_combo",
            "acc_network_combo",
            "withdraw_coin_combo",
        ):
            combo = getattr(self, name, None)
            if combo is not None:
                try:
                    combo.config(state=state)
                except Exception:
                    pass

    def wait_for_usdt(self, timeout_sec, stop_event, client=None):
        start = time.time()
        c = client or self.client
        if c is None:
            logger.error("wait_for_usdt 调用时没有可用的 BinanceClient")
            return False

        while time.time() - start < timeout_sec:
            if stop_event and stop_event.is_set():
                logger.info("检测 USDT 时收到停止信号，结束检测")
                return False
            try:
                usdt = c.spot_balance("USDT")
                logger.info("USDT 到账检测中，当前现货 USDT = %.8f", usdt)
            except Exception as e:
                logger.error("检测 USDT 余额失败: %s", e)
                usdt = 0.0

            if usdt > 0:
                logger.info("检测到 USDT 已到账，开始执行后续策略")
                return True

            time.sleep(1)

        logger.error("在 %d 秒内未检测到 USDT 到账，终止任务", timeout_sec)
        return False

    def start_bot(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "任务已经在运行中")
            return

        key = self.api_key_var.get().strip()
        secret = self.api_secret_var.get().strip()
        if not key or not secret:
            messagebox.showerror("错误", "请先填写 API KEY 和 SECRET")
            return

        try:
            spot_rounds = int(self.spot_rounds_var.get())
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = int(self.min_delay_var.get())
            max_delay = int(self.max_delay_var.get())
            usdt_timeout = int(self.usdt_timeout_var.get())
            spot_precision = int(self.spot_precision_var.get())
        except ValueError:
            messagebox.showerror("错误", "轮数 / 数量 / 精度 / 延迟 / 超时时间 格式不正确")
            return

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_address = self.withdraw_addr_var.get().strip()
        withdraw_network = self.withdraw_net_var.get().strip()
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        if enable_withdraw and (not withdraw_address or not withdraw_network or not withdraw_coin):
            messagebox.showerror("错误", "开启自动提现时，请填写 提现地址 / 网络 / 币种")
            return

        try:
            self.client = BinanceClient(key, secret)
        except Exception as e:
            messagebox.showerror("错误", "创建 BinanceClient 失败: %s" % e)
            return

        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：单账号运行中...")
        self.progress["value"] = 0
        total_steps = max(spot_rounds, 1)
        self.progress["maximum"] = total_steps

        def sleep_fn():
            if self.stop_event and self.stop_event.is_set():
                return
            self.random_sleep(min_delay, max_delay)

        def withdraw_callback(amount, idx=1, api_key=key, address=withdraw_address):
            if amount > 0:
                self.record_withdraw(idx, api_key, address, amount)

        strategy = Strategy(
            client=self.client,
            spot_rounds=spot_rounds,
            withdraw_coin=withdraw_coin,
            withdraw_address=withdraw_address,
            withdraw_network=withdraw_network,
            withdraw_fee_buffer=withdraw_buffer,
            spot_symbol=spot_symbol,
            spot_precision=spot_precision,
            sleep_fn=sleep_fn,
            enable_withdraw=enable_withdraw,
            withdraw_callback=withdraw_callback,
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self.after(0, _update)

        def worker():
            try:
                if not self.wait_for_usdt(usdt_timeout, self.stop_event):
                    logger.info("USDT 检测未通过，任务结束")
                    return

                logger.info("开始执行策略：现货 %d 轮", spot_rounds)
                strategy.run(self.stop_event, progress_cb=progress_cb)
            except Exception as e:
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def _on_worker_finished(self):
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.btn_run_accounts.config(state="normal")
        self.btn_batch_withdraw.config(state="normal")
        self.btn_refresh.config(state="normal")
        self.btn_withdraw.config(state="normal")
        self._set_account_manage_buttons_state("normal")
        self._set_combo_states_for_run(False)
        self.status_var.set("状态：已完成/已停止")
        self.progress["value"] = 0

        if self.api_key_var.get().strip() and self.api_secret_var.get().strip():
            self.refresh_balances(silent=True)

    def stop_bot(self):
        if self.stop_event:
            self.stop_event.set()
            logger.info("已请求停止任务")

    def refresh_balances(self, silent=False):
        key = self.api_key_var.get().strip()
        secret = self.api_secret_var.get().strip()
        if not key or not secret:
            if not silent:
                messagebox.showerror("错误", "请先填写 API KEY 和 SECRET")
            return

        def worker():
            try:
                client = BinanceClient(key, secret)
                spot_usdt = client.spot_balance("USDT")
                spot_symbol = self.spot_symbol_var.get().strip().upper()
                base = BinanceClient.get_spot_base_asset(spot_symbol)
                spot_base = client.spot_balance(base)

                def _update():
                    self.client = client
                    self.spot_usdt_var.set(f"{spot_usdt:.8f}")
                    self.spot_base_var.set(f"{spot_base:.8f} ({base})")
                    logger.info("余额刷新完成")
                self.after(0, _update)
            except Exception as e:
                logger.error("刷新余额失败: %s", e)

        threading.Thread(target=worker, daemon=True).start()

    def manual_withdraw(self):
        if not self.client:
            messagebox.showerror("错误", "请先使用当前 API 创建连接（点击一次开始或刷新余额）")
            return

        enable_withdraw = bool(self.enable_withdraw_var.get())
        address = self.withdraw_addr_var.get().strip()
        network = self.withdraw_net_var.get().strip()
        coin = self.withdraw_coin_var.get().strip().upper()
        try:
            buffer_val = float(self.withdraw_buffer_var.get())
        except ValueError:
            messagebox.showerror("错误", "手续费预留格式不正确")
            return

        if enable_withdraw and (not address or not network or not coin):
            messagebox.showerror("错误", "请填写 提现地址 / 网络 / 币种")
            return

        def worker():
            try:
                logger.info(f"手动触发提现 {coin}")
                amount = self.client.withdraw_all_coin(
                    coin=coin,
                    address=address,
                    network=network,
                    fee_buffer=buffer_val,
                    enable_withdraw=enable_withdraw,
                )
                if amount > 0:
                    self.record_withdraw(1, self.client.key, address, amount)
            except Exception as e:
                logger.error("手动提现失败: %s", e)

        threading.Thread(target=worker, daemon=True).start()

    def _reindex_accounts(self):
        for i, acc in enumerate(self.accounts, start=1):
            acc["index_var"].set(str(i))

    def _on_global_network_changed(self, *_):
        self.apply_network_to_all_accounts()

    def apply_network_to_all_accounts(self):
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for acc in self.accounts:
            acc["network"] = net
            if "network_var" in acc:
                acc["network_var"].set(net)

    @staticmethod
    def _account_row_color_by_status(status_text: str) -> str:
        s = str(status_text or "").strip()
        if not s or s == "就绪":
            return "#f2f2f2"
        if "未到账" in s:
            return "#ffe3b8"
        if any(k in s for k in ("失败", "异常")):
            return "#f8c7c7"
        if any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "#cfeecf"
        return "#cfe3ff"

    def _apply_account_row_style(self, acc: dict):
        bg = self._account_row_color_by_status(acc.get("status_var").get())
        row_frame = acc.get("frame")
        if row_frame is not None:
            try:
                row_frame.configure(bg=bg)
            except Exception:
                pass
        for widget in acc.get("row_widgets", []):
            try:
                widget.configure(bg=bg)
            except Exception:
                pass
            try:
                widget.configure(activebackground=bg)
            except Exception:
                pass

    def _set_account_status(self, acc: dict, text: str):
        acc["status_var"].set(str(text))
        self._apply_account_row_style(acc)

    @staticmethod
    def _format_amount(value: float, precision: int = 8) -> str:
        text = f"{float(value):.{precision}f}" if value is not None else "0"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text else "0"

    @classmethod
    def _format_withdraw_amount_status(cls, amount: float, coin: str, *, enable_withdraw: bool) -> str:
        coin_u = str(coin or "").strip().upper()
        if amount > 0:
            return f"提现额度 {cls._format_amount(amount)} {coin_u}"
        if not enable_withdraw:
            return f"提现额度 0 {coin_u}（自动提现已关闭）"
        return f"提现额度 0 {coin_u}（可提余额不足/预留过高）"

    @staticmethod
    def _compact_error_text(err_text: str, max_len: int = 28) -> str:
        s = str(err_text or "").strip().replace("\n", " ")
        if not s:
            return "未知错误"
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")

    def _append_account_row(self, key, secret, addr, net, selected=True):
        net = (net or "").strip() or WITHDRAW_NETWORK_DEFAULT

        row_frame = tk.Frame(self.accounts_container, bg="#f7f7f7")
        row_frame.pack(fill="x", padx=2, pady=1)

        index_var = tk.StringVar()
        selected_var = tk.BooleanVar(value=selected)
        network_var = tk.StringVar(value=net)
        status_var = tk.StringVar(value="就绪")

        lbl_index = tk.Label(row_frame, textvariable=index_var, width=4, anchor="w", bg="#f7f7f7")
        chk_selected = tk.Checkbutton(
            row_frame,
            variable=selected_var,
            bg="#f7f7f7",
            activebackground="#f7f7f7",
            bd=0,
            highlightthickness=0,
            relief="flat",
        )
        lbl_key = tk.Label(row_frame, text=self._mask_key(key), width=25, anchor="w", bg="#f7f7f7")
        lbl_addr = tk.Label(row_frame, text=self._mask_addr(addr), width=35, anchor="w", bg="#f7f7f7")
        lbl_net = tk.Label(row_frame, textvariable=network_var, width=8, anchor="w", bg="#f7f7f7")
        lbl_status = tk.Label(row_frame, textvariable=status_var, width=30, anchor="w", bg="#f7f7f7")

        for w in (lbl_index, chk_selected, lbl_key, lbl_addr, lbl_net, lbl_status):
            w.pack(side="left", padx=2)

        acc = {
            "frame": row_frame,
            "row_widgets": [lbl_index, chk_selected, lbl_key, lbl_addr, lbl_net, lbl_status],
            "index_var": index_var,
            "selected_var": selected_var,
            "network_var": network_var,
            "status_var": status_var,
            "api_key": key,
            "api_secret": secret,
            "address": addr,
            "network": net,
        }
        self._apply_account_row_style(acc)
        self.accounts.append(acc)
        return acc

    def add_account_to_list(self):
        key = self.acc_api_key_var.get().strip()
        secret = self.acc_api_secret_var.get().strip()
        addr = self.acc_withdraw_addr_var.get().strip()
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT

        if not key or not secret or not addr:
            messagebox.showerror("错误", "请完整填写：API KEY / SECRET / 提现地址")
            return

        self._append_account_row(key, secret, addr, net)
        self._reindex_accounts()

        self.acc_api_key_var.set("")
        self.acc_api_secret_var.set("")
        self.acc_withdraw_addr_var.set("")

    def delete_selected_accounts(self):
        keep = []
        for acc in self.accounts:
            if acc["selected_var"].get():
                acc["frame"].destroy()
            else:
                keep.append(acc)
        self.accounts = keep
        self._reindex_accounts()

    def select_all_accounts(self):
        for acc in self.accounts:
            acc["selected_var"].set(True)

    def deselect_all_accounts(self):
        for acc in self.accounts:
            acc["selected_var"].set(False)

    def export_accounts(self):
        if not self.accounts:
            messagebox.showinfo("提示", "当前账号列表为空，无需导出")
            return

        path = filedialog.asksaveasfilename(
            title="导出账号列表到文件",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            with open(path, "w", encoding="utf-8") as f:
                for acc in self.accounts:
                    line = "|".join([
                        acc["api_key"],
                        acc["api_secret"],
                        acc["address"],
                        acc["network"],
                    ])
                    f.write(line + "\n")
            logger.info("账号列表已导出到文件：%s", path)
            messagebox.showinfo("成功", f"账号列表已导出到：\n{path}")
        except Exception as e:
            logger.error("导出账号列表失败: %s", e)
            messagebox.showerror("错误", "导出账号列表失败: %s" % e)

    def import_accounts(self):
        path = filedialog.askopenfilename(
            title="导入账号列表文件",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            try:
                with open(path, "r", encoding="utf-8-sig") as f:
                    raw = f.read()
            except UnicodeDecodeError:
                with open(path, "r", encoding="gb18030", errors="ignore") as f:
                    raw = f.read()

            self._import_accounts_from_text(raw, "文件")
        except Exception as e:
            logger.error("导入账号列表失败: %s", e)
            messagebox.showerror("错误", "导入账号列表失败: %s" % e)

    def import_accounts_from_clipboard(self):
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            messagebox.showerror("错误", "剪贴板内容不可用或为空")
            return

        self._import_accounts_from_text(raw, "剪贴板")

    def run_selected_accounts(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = [acc for acc in self.accounts if acc["selected_var"].get()]
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return

        try:
            spot_rounds = int(self.spot_rounds_var.get())
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = int(self.min_delay_var.get())
            max_delay = int(self.max_delay_var.get())
            usdt_timeout = int(self.usdt_timeout_var.get())
            spot_precision = int(self.spot_precision_var.get())
            max_threads = int(self.max_threads_var.get())
        except ValueError:
            messagebox.showerror("错误", "参数格式不正确 (请检查轮数/线程数/延迟等)")
            return

        if max_threads < 1:
            max_threads = 1

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        batch_total_asset_only = bool(self.batch_total_asset_only_var.get())
        batch_collect_bnb_mode = bool(self.batch_collect_bnb_mode_var.get())
        skip_usdt_wait_in_batch = bool(self.skip_usdt_wait_in_batch_var.get())
        batch_sell_large_spot_to_bnb = bool(self.batch_sell_large_spot_to_bnb_var.get())

        if batch_total_asset_only:
            self.total_asset_results = {}

        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)

        if batch_total_asset_only:
            self.status_var.set(f"状态：批量查询总资产中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode:
            self.status_var.set(f"状态：批量归集BNB模式运行中 (并发 {max_threads} 线程)...")
        else:
            self.status_var.set(f"状态：批量执行中 (并发 {max_threads} 线程)...")

        self.progress["value"] = 0
        total_accounts = len(selected)
        self.progress["maximum"] = total_accounts

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

        completed_count = 0
        count_lock = threading.Lock()

        def finish_one():
            nonlocal completed_count
            with count_lock:
                completed_count += 1
                curr = completed_count

            def _u():
                self.progress["value"] = curr
                self.status_var.set(f"状态：已完成 {curr}/{total_accounts}")
            self.after(0, _u)

        def worker_loop(thread_id):
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    def _u():
                        self._set_account_status(acc_ref, text)
                    self.after(0, _u)

                def progress_cb(step, total, text, acc_obj=acc):
                    def _u():
                        self._set_account_status(acc_obj, text)
                    self.after(0, _u)

                logger.info(f"[线程 {thread_id}] 开始处理账号 #{idx}")

                should_finish_in_finally = True

                try:
                    client = BinanceClient(acc["api_key"], acc["api_secret"])

                    # 1) 只查询总资产
                    if batch_total_asset_only:
                        set_status("查询总资产...")
                        total_usdt, rows = client.query_total_wallet_balance("USDT")

                        self.record_total_asset(
                            idx,
                            acc["api_key"],
                            acc["address"],
                            acc["network"],
                            total_usdt
                        )

                        detail_text = " | ".join(
                            [f'{r["walletName"]}:{r["balance"]:.4f}' for r in rows if r["balance"] > 0]
                        )
                        logger.info(
                            "账号 #%d 总资产约 %s USDT；%s",
                            idx,
                            f"{total_usdt:.8f}",
                            detail_text if detail_text else "无非零钱包余额"
                        )

                        set_status(f"总资产 {Decimal(str(total_usdt)):.4f} USDT")

                    # 2) 批量归集BNB模式
                    elif batch_collect_bnb_mode:
                        logger.info(f"账号 #{idx} 开始执行【批量归集BNB模式】")
                        set_status("归集合约/资金...")
                        client.collect_all_to_spot()

                        if self.stop_event.is_set():
                            set_status("已停止")
                            should_finish_in_finally = False
                            finish_one()
                            task_queue.task_done()
                            logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                            return

                        time.sleep(1)

                        set_status("小额兑换为BNB...")
                        try:
                            convert_result = client.convert_spot_dust_to_bnb()
                            logger.info(f"账号 #{idx} 小额兑换完成，数量: {len(convert_result)}")
                        except Exception as e:
                            logger.warning(f"账号 #{idx} 小额兑换失败: {e}")

                        if self.stop_event.is_set():
                            set_status("已停止")
                            should_finish_in_finally = False
                            finish_one()
                            task_queue.task_done()
                            logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                            return

                        time.sleep(1)

                        if batch_sell_large_spot_to_bnb:
                            set_status("大额币卖USDT...")
                            try:
                                sold_assets = client.sell_large_spot_assets_to_usdt()
                                logger.info(f"账号 #{idx} 大额币卖出完成: {sold_assets}")
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 大额币卖出失败: {e}")

                            if self.stop_event.is_set():
                                set_status("已停止")
                                should_finish_in_finally = False
                                finish_one()
                                task_queue.task_done()
                                logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                                return

                            time.sleep(1)

                            set_status("USDT买入BNB...")
                            try:
                                buy_ok = client.spot_buy_all_usdt(buffer=0.0, symbol="BNBUSDT")
                                if buy_ok:
                                    logger.info("账号 #%d 已将现货 USDT 买入为 BNB", idx)
                                else:
                                    logger.info("账号 #%d 无可用 USDT 买入 BNB", idx)
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 用 USDT 买 BNB 失败: {e}")

                            time.sleep(1)

                        set_status("查询BNB余额...")
                        bnb_balance = client.spot_balance("BNB")
                        logger.info("账号 #%d 当前现货 BNB = %.8f", idx, bnb_balance)

                        set_status("提现BNB...")
                        try:
                            amount = client.withdraw_all_coin(
                                coin="BNB",
                                address=acc["address"],
                                network=(acc["network"] or WITHDRAW_NETWORK_DEFAULT),
                                fee_buffer=withdraw_buffer,
                                enable_withdraw=enable_withdraw,
                            )
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            set_status(self._format_withdraw_amount_status(amount, "BNB", enable_withdraw=enable_withdraw))
                        except Exception as e:
                            logger.error(f"账号 #{idx} BNB提现失败: {e}")
                            set_status("BNB提现失败")

                    # 3) 原批量现货策略
                    else:
                        need_wait_usdt = not skip_usdt_wait_in_batch

                        if need_wait_usdt:
                            set_status("检测 USDT 到账...")
                            if not self.wait_for_usdt(usdt_timeout, self.stop_event, client=client):
                                logger.info(f"账号 #{idx} USDT 检测超时，跳过")
                                set_status("USDT 未到账")
                                should_finish_in_finally = False
                                finish_one()
                                task_queue.task_done()
                                logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                                continue
                        else:
                            logger.info(f"账号 #{idx} 已开启“批量策略跳过USDT检测”")
                            set_status("跳过USDT检测")

                        set_status("策略执行中...")

                        def sleep_fn():
                            if self.stop_event.is_set():
                                return
                            self.random_sleep(min_delay, max_delay)

                        withdraw_state = {"amount": None}

                        def withdraw_callback(
                            amount,
                            idx_local=idx,
                            api_key=acc["api_key"],
                            address=acc["address"],
                        ):
                            try:
                                amt = float(amount)
                            except Exception:
                                amt = 0.0
                            withdraw_state["amount"] = amt
                            if amt > 0:
                                self.record_withdraw(idx_local, api_key, address, amt)

                        strategy = Strategy(
                            client=client,
                            spot_rounds=spot_rounds,
                            withdraw_coin=withdraw_coin,
                            withdraw_address=acc["address"],
                            withdraw_network=acc["network"],
                            withdraw_fee_buffer=withdraw_buffer,
                            spot_symbol=spot_symbol,
                            spot_precision=spot_precision,
                            sleep_fn=sleep_fn,
                            enable_withdraw=enable_withdraw,
                            withdraw_callback=withdraw_callback,
                        )

                        strategy_result = strategy.run(self.stop_event, progress_cb=progress_cb)

                        if not self.stop_event.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount

                            if isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                set_status(f"提现失败 {self._compact_error_text(strategy_result.get('withdraw_error', ''))}")
                            else:
                                set_status(
                                    self._format_withdraw_amount_status(
                                        float(final_amount or 0.0),
                                        withdraw_coin,
                                        enable_withdraw=enable_withdraw,
                                    )
                                )

                except Exception as e:
                    logger.error(f"账号 #{idx} 执行异常: {e}")
                    set_status("异常")
                finally:
                    if should_finish_in_finally:
                        finish_one()
                        task_queue.task_done()
                        logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")

        def controller():
            threads = []
            logger.info(f"启动 {max_threads} 个工作线程处理 {total_accounts} 个账号")
            for i in range(max_threads):
                t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            logger.info("批量任务全部结束")
            self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=controller, daemon=True)
        self.worker_thread.start()

    def batch_manual_withdraw(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = [acc for acc in self.accounts if acc["selected_var"].get()]
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return

        try:
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            max_threads = int(self.max_threads_var.get())
        except ValueError:
            messagebox.showerror("错误", "参数格式不正确")
            return

        if max_threads < 1:
            max_threads = 1

        enable_withdraw = bool(self.enable_withdraw_var.get())
        coin = self.withdraw_coin_var.get().strip().upper()

        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：批量提现中...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

        total_accounts = len(selected)
        completed_count = 0
        count_lock = threading.Lock()

        def withdraw_worker(thread_id):
            nonlocal completed_count
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    self.after(0, lambda: self._set_account_status(acc_ref, text))

                set_status(f"提现 {coin}...")
                try:
                    client = BinanceClient(acc["api_key"], acc["api_secret"])
                    amount = client.withdraw_all_coin(
                        coin=coin,
                        address=acc["address"],
                        network=acc["network"],
                        fee_buffer=withdraw_buffer,
                        enable_withdraw=enable_withdraw,
                    )
                    if amount > 0:
                        self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                    set_status(self._format_withdraw_amount_status(amount, coin, enable_withdraw=enable_withdraw))
                except Exception as e:
                    logger.error(f"账号 #{idx} 提现失败: {e}")
                    set_status("提现失败")
                finally:
                    task_queue.task_done()
                    with count_lock:
                        completed_count += 1
                        curr = completed_count

                    def _u():
                        self.progress["value"] = curr
                        self.status_var.set(f"批量提现进度: {curr}/{total_accounts}")
                    self.after(0, _u)

        def controller():
            threads = []
            for i in range(max_threads):
                t = threading.Thread(target=withdraw_worker, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=controller, daemon=True)
        self.worker_thread.start()


# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))

        os.chdir(application_path)
    except Exception as e:
        print(f"路径设置失败: {e}")

    app = App()
    app.mainloop()
