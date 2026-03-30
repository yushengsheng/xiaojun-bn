#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import importlib
import json
import os
import re
import sys
import threading
import time
from decimal import Decimal
from typing import Callable

import requests

from core_models import EvmToken, GeneratedWalletEntry


class EvmClient:
    _DEPENDENCY_LOCK = threading.Lock()
    _DEPENDENCY_MODULE_CACHE: dict[str, object] = {}
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

    def __init__(
        self,
        proxy_provider: Callable[[], str] | None = None,
        *,
        allow_system_proxy: bool = True,
        allow_system_proxy_provider: Callable[[], bool] | None = None,
    ):
        self._proxy_provider = proxy_provider
        self._allow_system_proxy = bool(allow_system_proxy)
        self._allow_system_proxy_provider = allow_system_proxy_provider

    def _allow_system_proxy_now(self) -> bool:
        if self._allow_system_proxy_provider is None:
            return self._allow_system_proxy
        try:
            return bool(self._allow_system_proxy_provider())
        except Exception as exc:
            raise RuntimeError(f"系统代理开关读取失败：{exc}") from exc

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
        session.trust_env = self._allow_system_proxy_now() and not bool(proxy_url)
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
    def _dependency_install_command(cls, package_names: list[str]) -> str:
        pkg_list = " ".join(str(name).strip() for name in package_names if str(name).strip())
        return f'"{sys.executable}" -m pip install --user {pkg_list}'.strip()

    @classmethod
    def _missing_dependency_error(cls, package_names: list[str]) -> RuntimeError:
        pkg_text = ", ".join(package_names)
        if getattr(sys, "frozen", False):
            return RuntimeError(f"链上依赖缺失：{pkg_text}。当前为打包版，请在构建环境先安装后重新打包。")
        return RuntimeError(f"链上依赖缺失：{pkg_text}。请先执行：{cls._dependency_install_command(package_names)}")

    @classmethod
    def ensure_dependencies(cls, *, require_signing: bool = True):
        required = ["eth-utils"]
        if require_signing:
            required.append("eth-account")

        with cls._DEPENDENCY_LOCK:
            modules: dict[str, object] = {}
            missing_packages: list[str] = []
            for package_name in required:
                module_name = cls._ONCHAIN_DEPENDENCY_MODULES[package_name]
                module = cls._DEPENDENCY_MODULE_CACHE.get(module_name)
                if module is None:
                    module = cls._import_module(module_name)
                if module is None:
                    missing_packages.append(package_name)
                    continue
                cls._DEPENDENCY_MODULE_CACHE[module_name] = module
                modules[module_name] = module

            if missing_packages:
                raise cls._missing_dependency_error(missing_packages)

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

    def create_wallet(self) -> GeneratedWalletEntry:
        Account = self._ensure_eth_account()
        try:
            acct = Account.create(os.urandom(32))
            private_key = acct.key.hex()
            if not re.fullmatch(r"[a-f0-9]{64}", private_key):
                raise RuntimeError("生成的私钥格式异常")
            derived = Account.from_key(private_key)
            address = str(acct.address)
            derived_address = str(derived.address)
            if self.normalize_address(address) != self.normalize_address(derived_address):
                raise RuntimeError("生成结果校验失败：私钥与地址不匹配")
            return GeneratedWalletEntry(address=derived_address, private_key=private_key)
        except Exception as exc:
            raise RuntimeError(f"创建钱包失败：{exc}") from exc

    def create_wallets(self, count: int, worker_threads: int = 1) -> list[GeneratedWalletEntry]:
        try:
            total = int(count)
        except Exception as exc:
            raise RuntimeError("创建数量格式错误") from exc
        if total <= 0:
            raise RuntimeError("创建数量必须大于 0")
        if total > 2000:
            raise RuntimeError("单次最多创建 2000 个钱包")
        try:
            workers = max(1, int(worker_threads))
        except Exception as exc:
            raise RuntimeError("执行线程数格式错误") from exc
        if workers == 1 or total == 1:
            return [self.create_wallet() for _ in range(total)]
        with ThreadPoolExecutor(max_workers=min(workers, total)) as executor:
            return list(executor.map(lambda _idx: self.create_wallet(), range(total)))

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
