#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import Callable

import requests

from core_models import AccountEntry, EvmToken
from exchange_binance_client import BinanceClient


class SubmissionUncertainError(RuntimeError):
    def __init__(self, message: str, *, reference: str = ""):
        super().__init__(message)
        self.reference = str(reference or "")


class EvmClient:
    _DEPENDENCY_LOCK = threading.Lock()
    _ONCHAIN_DEPENDENCY_MODULES = {
        "eth-account": "eth_account",
        "eth-utils": "eth_utils",
    }
    NATIVE_GAS_LIMIT = 21000
    ERC20_DEFAULT_GAS_LIMIT = 70000
    NETWORKS = {
        "ETH": {
            "chain_id": 1,
            "symbol": "ETH",
            "rpc_urls": [
                "https://gateway.tenderly.co/public/mainnet",
                "https://rpc.flashbots.net",
                "https://mainnet.gateway.tenderly.co",
                "https://1rpc.io/eth",
                "https://ethereum-rpc.publicnode.com",
                "https://eth.llamarpc.com",
                "https://cloudflare-eth.com",
            ],
        },
        "BSC": {
            "chain_id": 56,
            "symbol": "BNB",
            "rpc_urls": [
                "https://bsc-dataseed.bnbchain.org",
                "https://bsc-dataseed1.bnbchain.org",
                "https://bsc-dataseed.binance.org",
                "https://bsc-rpc.publicnode.com",
            ],
        },
    }
    PRESET_TOKENS = {
        "ETH": [
            EvmToken(symbol="ETH", contract="", decimals=18, is_native=True),
            EvmToken(symbol="USDT", contract="0xdac17f958d2ee523a2206206994597c13d831ec7", decimals=6, is_native=False),
            EvmToken(symbol="USDC", contract="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", decimals=6, is_native=False),
        ],
        "BSC": [
            EvmToken(symbol="BNB", contract="", decimals=18, is_native=True),
            EvmToken(symbol="USDT", contract="0x55d398326f99059ff775485246999027b3197955", decimals=18, is_native=False),
            EvmToken(symbol="USDC", contract="0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", decimals=18, is_native=False),
        ],
    }

    def __init__(self, proxy_provider: Callable[[], str] | None = None, *, allow_system_proxy: bool = True):
        self._proxy_provider = proxy_provider
        self._allow_system_proxy = bool(allow_system_proxy)

    def _current_proxy_url(self) -> str:
        if self._proxy_provider is None:
            return ""
        try:
            value = self._proxy_provider()
        except Exception as exc:
            raise RuntimeError(f"RPC 代理初始化失败：{exc}") from exc
        return str(value or "").strip()

    def _new_rpc_session(self) -> tuple[requests.Session, str]:
        proxy_url = self._current_proxy_url()
        session = requests.Session()
        session.trust_env = self._allow_system_proxy and not bool(proxy_url)
        if proxy_url:
            session.proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
        return session, proxy_url

    def _network_info(self, network: str) -> dict:
        net = network.strip().upper()
        info = self.NETWORKS.get(net)
        if not info:
            raise RuntimeError(f"暂不支持网络：{network}")
        cfg = dict(info)
        urls = list(cfg.get("rpc_urls", []))
        env_key = f"EVM_RPC_{net}"
        env_val = os.environ.get(env_key, "").strip()
        if env_val:
            custom_urls = [u.strip() for u in re.split(r"[,\n;]+", env_val) if u.strip()]
            if custom_urls:
                merged = custom_urls + [u for u in urls if u not in custom_urls]
                cfg["rpc_urls"] = merged
                return cfg
        cfg["rpc_urls"] = urls
        return cfg

    def get_default_tokens(self, network: str) -> list[EvmToken]:
        net = network.strip().upper()
        items = self.PRESET_TOKENS.get(net, [])
        return [EvmToken(symbol=t.symbol, contract=t.contract, decimals=t.decimals, is_native=t.is_native) for t in items]

    @classmethod
    def _import_module(cls, module_name: str):
        try:
            return importlib.import_module(module_name)
        except Exception:
            return None

    @classmethod
    def _install_missing_dependencies(cls, package_names: list[str]) -> None:
        if not package_names:
            return
        if getattr(sys, "frozen", False):
            pkg_text = ", ".join(package_names)
            raise RuntimeError(f"链上依赖缺失：{pkg_text}。当前为打包版，请在构建环境先安装后重新打包。")

        cmd = [sys.executable, "-m", "pip", "install", "--user", *package_names]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except Exception as exc:
            cmd_text = subprocess.list2cmdline(cmd)
            raise RuntimeError(f"链上依赖自动安装失败，请手动执行：{cmd_text}") from exc

    @classmethod
    def ensure_dependencies(cls, *, require_signing: bool = True):
        required = ["eth-utils"]
        if require_signing:
            required.append("eth-account")

        with cls._DEPENDENCY_LOCK:
            missing_packages: list[str] = []
            for package_name in required:
                module_name = cls._ONCHAIN_DEPENDENCY_MODULES[package_name]
                if cls._import_module(module_name) is None:
                    missing_packages.append(package_name)

            if missing_packages:
                cls._install_missing_dependencies(missing_packages)

            missing_after_install: list[str] = []
            modules: dict[str, object] = {}
            for package_name in required:
                module_name = cls._ONCHAIN_DEPENDENCY_MODULES[package_name]
                module = cls._import_module(module_name)
                if module is None:
                    missing_after_install.append(package_name)
                else:
                    modules[module_name] = module

            if missing_after_install:
                pkg_text = ", ".join(missing_after_install)
                raise RuntimeError(f"链上依赖仍不可用：{pkg_text}")

            return modules

    @classmethod
    def _ensure_eth_account(cls):
        cls.ensure_dependencies(require_signing=True)
        try:
            from eth_account import Account  # type: ignore
        except Exception as exc:
            cmd = f'"{sys.executable}" -m pip install --user eth-account eth-utils'
            raise RuntimeError(f"链上签名依赖不可用，请执行：{cmd}") from exc
        return Account

    @classmethod
    def _ensure_eth_utils(cls):
        cls.ensure_dependencies(require_signing=False)
        try:
            import eth_utils  # type: ignore
        except Exception as exc:
            cmd = f'"{sys.executable}" -m pip install --user eth-utils'
            raise RuntimeError(f"链上地址校验依赖不可用，请执行：{cmd}") from exc
        return eth_utils

    @staticmethod
    def _ensure_hex_prefixed(value: str) -> str:
        s = value.strip()
        if s.startswith("0x") or s.startswith("0X"):
            return "0x" + s[2:]
        return "0x" + s

    @classmethod
    def normalize_address(cls, value: str) -> str:
        s = cls._ensure_hex_prefixed(value)
        return "0x" + s[2:].lower()

    @classmethod
    def to_checksum_address(cls, value: str) -> str:
        normalized = cls.normalize_address(value)
        try:
            eth_utils = cls._ensure_eth_utils()
            return str(eth_utils.to_checksum_address(normalized))
        except Exception:
            return normalized

    @staticmethod
    def is_address(value: str) -> bool:
        return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value.strip()))

    @classmethod
    def validate_evm_address(cls, value: str, field_label: str = "地址") -> str:
        raw = str(value or "").strip()
        if not cls.is_address(raw):
            raise RuntimeError(f"{field_label}格式错误：{raw}")

        body = cls._strip_0x(raw)
        has_lower = any(ch.isalpha() and ch.islower() for ch in body)
        has_upper = any(ch.isalpha() and ch.isupper() for ch in body)
        if has_lower and has_upper:
            try:
                eth_utils = cls._ensure_eth_utils()
            except Exception as exc:
                raise RuntimeError(f"{field_label}校验失败：缺少 checksum 校验依赖，无法验证大小写混合地址") from exc
            if not bool(eth_utils.is_checksum_address(cls._ensure_hex_prefixed(raw))):
                raise RuntimeError(f"{field_label}校验失败：大小写混合地址不符合 EVM checksum 规范")

        return cls.to_checksum_address(raw)

    def credential_to_private_key(self, credential: str) -> str:
        s = credential.strip()
        if not s:
            raise RuntimeError("转出凭证不能为空")

        key_match = re.fullmatch(r"(0x)?[a-fA-F0-9]{64}", s)
        if key_match:
            return self._ensure_hex_prefixed(s).lower()

        words = [x for x in s.split() if x]
        if len(words) in {12, 15, 18, 21, 24}:
            Account = self._ensure_eth_account()
            try:
                Account.enable_unaudited_hdwallet_features()
            except Exception:
                pass
            try:
                acct = Account.from_mnemonic(" ".join(words))
                return acct.key.hex()
            except Exception as exc:
                raise RuntimeError(f"助记词解析失败：{exc}") from exc

        raise RuntimeError("转出凭证格式错误：仅支持 64位私钥 或 12/15/18/21/24 助记词")

    def address_from_private_key(self, private_key: str) -> str:
        Account = self._ensure_eth_account()
        try:
            acct = Account.from_key(private_key)
            return str(acct.address)
        except Exception as exc:
            raise RuntimeError(f"私钥解析失败：{exc}") from exc

    def _rpc_call(self, network: str, method: str, params: list) -> object:
        info = self._network_info(network)
        max_attempts = 2
        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method,
            "params": params,
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        last_err = ""
        for url in info["rpc_urls"]:
            for attempt in range(1, max_attempts + 1):
                session = None
                proxy_url = ""
                try:
                    session, proxy_url = self._new_rpc_session()
                    resp = session.post(
                        url,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        timeout=20,
                    )
                    text = resp.text
                    if resp.status_code >= 400:
                        preview = (text or "").strip()
                        if len(preview) > 240:
                            preview = preview[:240] + "..."
                        raise RuntimeError(f"RPC({url}) HTTP {resp.status_code} {preview}".strip())
                    j = json.loads(text) if text else {}
                    if "error" in j:
                        err = j.get("error") or {}
                        code = err.get("code")
                        msg = err.get("message")
                        raise RuntimeError(f"RPC({url}) code={code} msg={msg}")
                    return j.get("result")
                except Exception as exc:
                    err_text = str(exc)
                    if proxy_url and "Missing dependencies for SOCKS support" in err_text:
                        err_text = f"当前代理为 SOCKS，但运行环境缺少 PySocks：{proxy_url}"
                    last_err = err_text
                    if attempt < max_attempts:
                        time.sleep(0.4 * attempt)
                        continue
                    break
                finally:
                    if session is not None:
                        session.close()
        raise RuntimeError(f"{network} RPC 请求失败：{last_err}")

    @staticmethod
    def _int_from_hex(result: object) -> int:
        if result is None:
            return 0
        text = str(result).strip()
        if not text:
            return 0
        if text.startswith("0x") or text.startswith("0X"):
            return int(text, 16)
        return int(text)

    @staticmethod
    def _hex_quantity(value: int) -> str:
        if value < 0:
            raise RuntimeError("数值不能为负数")
        return hex(int(value))

    @staticmethod
    def _strip_0x(value: str) -> str:
        s = str(value or "").strip()
        if s.startswith("0x") or s.startswith("0X"):
            return s[2:]
        return s

    @classmethod
    def _pad_32_hex(cls, hex_without_0x: str) -> str:
        h = cls._strip_0x(hex_without_0x)
        if len(h) > 64:
            raise RuntimeError("ABI 编码超出 32 字节")
        return h.rjust(64, "0")

    @classmethod
    def _abi_encode_address(cls, address: str) -> str:
        if not cls.is_address(address):
            raise RuntimeError(f"地址格式错误：{address}")
        return cls._pad_32_hex(cls._strip_0x(address).lower())

    @classmethod
    def _abi_encode_uint(cls, value: int) -> str:
        if value < 0:
            raise RuntimeError("uint 不能为负数")
        return cls._pad_32_hex(hex(value))

    def _erc20_call(self, network: str, contract: str, data: str) -> str:
        contract_n = self.normalize_address(contract)
        if not self.is_address(contract_n):
            raise RuntimeError(f"代币合约地址格式错误：{contract}")
        payload = {
            "to": contract_n,
            "data": self._ensure_hex_prefixed(data),
        }
        result = self._rpc_call(network, "eth_call", [payload, "latest"])
        if result is None:
            raise RuntimeError("代币合约调用返回为空")
        return self._ensure_hex_prefixed(str(result))

    @classmethod
    def _decode_symbol_result(cls, result_hex: str) -> str:
        raw_hex = cls._strip_0x(result_hex)
        if not raw_hex:
            return ""
        try:
            raw = bytes.fromhex(raw_hex)
        except Exception:
            return ""

        if len(raw) >= 64:
            try:
                offset = int.from_bytes(raw[0:32], "big")
                if offset + 32 <= len(raw):
                    ln = int.from_bytes(raw[offset : offset + 32], "big")
                    start = offset + 32
                    end = start + ln
                    if 0 <= ln <= 256 and end <= len(raw):
                        text = raw[start:end].decode("utf-8", errors="ignore").strip().strip("\x00")
                        if text:
                            return text
            except Exception:
                pass

        text = raw[:32].rstrip(b"\x00").decode("utf-8", errors="ignore").strip()
        return text

    def get_symbol(self, network: str) -> str:
        return str(self._network_info(network).get("symbol", ""))

    def get_chain_id(self, network: str) -> int:
        return int(self._network_info(network).get("chain_id"))

    def get_balance_wei(self, network: str, address: str) -> int:
        if not self.is_address(address):
            raise RuntimeError(f"地址格式错误：{address}")
        result = self._rpc_call(network, "eth_getBalance", [address, "latest"])
        return self._int_from_hex(result)

    def get_erc20_decimals(self, network: str, contract: str) -> int:
        result = self._erc20_call(network, contract, "0x313ce567")
        value = self._int_from_hex(result)
        if value < 0 or value > 36:
            raise RuntimeError(f"代币 decimals 异常：{value}")
        return value

    def get_erc20_symbol(self, network: str, contract: str) -> str:
        result = self._erc20_call(network, contract, "0x95d89b41")
        symbol = self._decode_symbol_result(result).upper().strip()
        return symbol or "TOKEN"

    def get_erc20_balance(self, network: str, contract: str, address: str) -> int:
        data = "0x70a08231" + self._abi_encode_address(address)
        result = self._erc20_call(network, contract, data)
        return self._int_from_hex(result)

    def get_erc20_token_info(self, network: str, contract: str) -> EvmToken:
        contract_n = self.normalize_address(contract)
        symbol = self.get_erc20_symbol(network, contract_n)
        decimals = self.get_erc20_decimals(network, contract_n)
        return EvmToken(symbol=symbol, contract=contract_n, decimals=decimals, is_native=False)

    def get_nonce(self, network: str, address: str) -> int:
        if not self.is_address(address):
            raise RuntimeError(f"地址格式错误：{address}")
        result = self._rpc_call(network, "eth_getTransactionCount", [address, "pending"])
        return self._int_from_hex(result)

    def get_gas_price_wei(self, network: str) -> int:
        result = self._rpc_call(network, "eth_gasPrice", [])
        val = self._int_from_hex(result)
        if val <= 0:
            raise RuntimeError("读取 gasPrice 失败")
        return val

    def estimate_erc20_transfer_gas(self, network: str, from_address: str, contract: str, to_address: str, amount_units: int) -> int:
        data = "0xa9059cbb" + self._abi_encode_address(to_address) + self._abi_encode_uint(amount_units)
        payload = {
            "from": self.normalize_address(from_address),
            "to": self.normalize_address(contract),
            "data": data,
            "value": "0x0",
        }
        try:
            result = self._rpc_call(network, "eth_estimateGas", [payload])
            val = self._int_from_hex(result)
            if val > 0:
                padded = int(val * 1.2)
                return max(self.ERC20_DEFAULT_GAS_LIMIT, padded)
        except Exception:
            pass
        return self.ERC20_DEFAULT_GAS_LIMIT

    @staticmethod
    def _signed_raw_transaction_hex(signed_tx) -> str:
        raw = getattr(signed_tx, "raw_transaction", None)
        if raw is None:
            raw = getattr(signed_tx, "rawTransaction", None)
        if raw is None:
            raise RuntimeError("签名结果缺少 raw transaction")
        if hasattr(raw, "hex"):
            try:
                return EvmClient._ensure_hex_prefixed(raw.hex())
            except Exception:
                pass
        if isinstance(raw, (bytes, bytearray)):
            return EvmClient._ensure_hex_prefixed(bytes(raw).hex())
        return EvmClient._ensure_hex_prefixed(str(raw))

    def _sign_transaction_hex(self, tx: dict, private_key: str, error_prefix: str) -> str:
        Account = self._ensure_eth_account()
        try:
            signed = Account.sign_transaction(tx, private_key=private_key)
        except Exception as exc:
            raise RuntimeError(f"{error_prefix}：{exc}") from exc
        try:
            return self._signed_raw_transaction_hex(signed)
        except Exception as exc:
            raise RuntimeError(f"{error_prefix}结果解析失败：{exc}") from exc

    def send_native_transfer(
        self,
        network: str,
        private_key: str,
        to_address: str,
        value_wei: int,
        nonce: int,
        gas_price_wei: int,
        gas_limit: int = 21000,
    ) -> str:
        if value_wei <= 0:
            raise RuntimeError("转账金额必须大于 0")
        safe_to_address = self.validate_evm_address(to_address, "接收地址")

        tx = {
            "nonce": int(nonce),
            "to": safe_to_address,
            "value": int(value_wei),
            "gas": int(gas_limit),
            "gasPrice": int(gas_price_wei),
            "chainId": self.get_chain_id(network),
        }
        raw = self._sign_transaction_hex(tx, private_key, "交易签名失败")
        tx_hash = self._rpc_call(network, "eth_sendRawTransaction", [raw])
        return str(tx_hash)

    def send_erc20_transfer(
        self,
        network: str,
        private_key: str,
        token_contract: str,
        to_address: str,
        amount_units: int,
        nonce: int,
        gas_price_wei: int,
        gas_limit: int,
    ) -> str:
        if amount_units <= 0:
            raise RuntimeError("代币转账数量必须大于 0")
        safe_to_address = self.validate_evm_address(to_address, "接收地址")

        data = "0xa9059cbb" + self._abi_encode_address(safe_to_address) + self._abi_encode_uint(amount_units)
        tx = {
            "nonce": int(nonce),
            "to": self.to_checksum_address(token_contract),
            "value": 0,
            "data": self._ensure_hex_prefixed(data),
            "gas": int(gas_limit),
            "gasPrice": int(gas_price_wei),
            "chainId": self.get_chain_id(network),
        }
        raw = self._sign_transaction_hex(tx, private_key, "代币交易签名失败")
        tx_hash = self._rpc_call(network, "eth_sendRawTransaction", [raw])
        return str(tx_hash)


class BitgetClient:
    def __init__(self, base_url: str = "https://api.bitget.com"):
        self.base_url = base_url.rstrip("/")

    @staticmethod
    def _sign(secret: str, prehash: str) -> str:
        digest = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        body: dict | None = None,
        auth: bool = False,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        non_idempotent: bool = False,
    ) -> dict:
        method_u = method.upper()
        query = urllib.parse.urlencode(params or {})
        request_path = path + (f"?{query}" if query else "")
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""
        data = body_text.encode("utf-8") if method_u != "GET" else None
        url = f"{self.base_url}{request_path}"

        max_attempts = 1 if non_idempotent else 3
        for attempt in range(1, max_attempts + 1):
            headers = {"Content-Type": "application/json", "locale": "zh-CN"}
            if auth:
                ts = str(int(time.time() * 1000))
                prehash = f"{ts}{method_u}{request_path}{body_text}"
                sign = self._sign(api_secret, prehash)
                headers.update(
                    {
                        "ACCESS-KEY": api_key,
                        "ACCESS-SIGN": sign,
                        "ACCESS-TIMESTAMP": ts,
                        "ACCESS-PASSPHRASE": passphrase,
                    }
                )

            req = urllib.request.Request(url=url, data=data, method=method_u, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = resp.read().decode("utf-8")
                    try:
                        j = json.loads(payload) if payload else {}
                    except json.JSONDecodeError as exc:
                        if non_idempotent:
                            raise SubmissionUncertainError(
                                "提现请求结果不确定：响应无法解析，系统将自动继续确认"
                            ) from exc
                        raise RuntimeError("响应解析失败")
            except urllib.error.HTTPError as exc:
                payload = exc.read().decode("utf-8", errors="ignore")
                retryable = exc.code in {429, 500, 502, 503, 504}
                if non_idempotent and retryable:
                    raise SubmissionUncertainError(
                        f"提现请求结果不确定：HTTP {exc.code}: {payload}，系统将自动继续确认"
                    )
                if retryable and attempt < max_attempts:
                    time.sleep(0.5 * attempt)
                    continue
                raise RuntimeError(f"HTTP {exc.code}: {payload}")
            except urllib.error.URLError as exc:
                if non_idempotent:
                    raise SubmissionUncertainError(
                        f"提现请求结果不确定：网络错误：{exc}，系统将自动继续确认"
                    )
                if attempt < max_attempts:
                    time.sleep(0.5 * attempt)
                    continue
                raise RuntimeError(f"网络错误：{exc}")

            code = str(j.get("code", ""))
            if code and code != "00000":
                msg = j.get("msg") or j.get("message") or ""
                raise RuntimeError(f"code={code} msg={msg}")
            return j

        raise RuntimeError("请求失败：已达到最大重试次数")

    def get_public_coins(self, coin: str = "") -> list[dict]:
        params = {"coin": coin.strip().upper()} if coin else {}
        j = self._request("GET", "/api/v2/spot/public/coins", params=params, auth=False)
        data = j.get("data", []) or []
        return data if isinstance(data, list) else []

    def get_coin_networks(self, coin: str) -> list[str]:
        coin_u = coin.strip().upper()
        if not coin_u:
            return []
        data = self.get_public_coins(coin_u)
        target = None
        for item in data:
            name = str(item.get("coin", "")).strip().upper()
            if not name:
                name = str(item.get("coinName", "")).strip().upper()
            if name == coin_u:
                target = item
                break
        if target is None and data:
            target = data[0]
        if target is None:
            return []

        arr: list[str] = []
        seen: set[str] = set()
        for c in target.get("chains", []) or []:
            chain = str(c.get("chain", "")).strip()
            if not chain or chain in seen:
                continue
            withdrawable = str(c.get("withdrawable", "true")).strip().lower() in {"true", "1"}
            if withdrawable:
                seen.add(chain)
                arr.append(chain)
        return arr

    def get_withdraw_fee(self, coin: str, chain: str) -> Decimal | None:
        coin_u = coin.strip().upper()
        chain_u = chain.strip().upper()
        if not coin_u:
            return None
        data = self.get_public_coins(coin_u)
        target = None
        for item in data:
            name = str(item.get("coin", "")).strip().upper()
            if not name:
                name = str(item.get("coinName", "")).strip().upper()
            if name == coin_u:
                target = item
                break
        if target is None and data:
            target = data[0]
        if target is None:
            return None

        fallback_fee: Decimal | None = None
        for c in target.get("chains", []) or []:
            code = str(c.get("chain", "")).strip().upper()
            fee_raw = c.get("withdrawFee")
            if fee_raw in {None, ""}:
                fee = None
            else:
                try:
                    fee = Decimal(str(fee_raw))
                except Exception:
                    fee = None
            if not chain_u:
                if fallback_fee is None:
                    fallback_fee = fee
                continue
            if code == chain_u:
                return fee
        return fallback_fee

    def get_account_assets(self, api_key: str, api_secret: str, passphrase: str) -> dict[str, Decimal]:
        j = self._request(
            "GET",
            "/api/v2/spot/account/assets",
            params={},
            auth=True,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
        )
        data = j.get("data", []) or []
        totals: dict[str, Decimal] = {}
        for item in data:
            coin = str(item.get("coin", "")).strip().upper()
            if not coin:
                continue
            available = Decimal(str(item.get("available", "0")))
            frozen = Decimal(str(item.get("frozen", "0")))
            locked = Decimal(str(item.get("locked", "0")))
            total = available + frozen + locked
            if total <= 0:
                continue
            totals[coin] = total
        return totals

    def get_available_balance(self, api_key: str, api_secret: str, passphrase: str, coin: str) -> Decimal:
        coin_u = coin.strip().upper()
        j = self._request(
            "GET",
            "/api/v2/spot/account/assets",
            params={"coin": coin_u},
            auth=True,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
        )
        data = j.get("data", []) or []
        for item in data:
            c = str(item.get("coin", "")).strip().upper()
            if c == coin_u:
                return Decimal(str(item.get("available", "0")))
        return Decimal("0")

    def withdraw(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        coin: str,
        address: str,
        amount: str,
        chain: str,
        *,
        client_oid: str = "",
    ) -> dict:
        safe_client_oid = client_oid.strip() or f"codex_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        body = {
            "coin": coin.strip().upper(),
            "transferType": "on_chain",
            "address": address.strip(),
            "chain": chain.strip(),
            "size": amount,
            "clientOid": safe_client_oid,
        }
        j = self._request(
            "POST",
            "/api/v2/spot/wallet/withdrawal",
            body=body,
            auth=True,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            non_idempotent=True,
        )
        data = j.get("data", {}) or {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def new_client_oid() -> str:
        return f"codex_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
