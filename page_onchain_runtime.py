#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ipaddress
import math
import re
from decimal import ROUND_FLOOR, Decimal

import requests

from api_clients import EvmClient
from app_defaults import (
    ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
    ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_MIN,
    ONCHAIN_WORKER_THREADS_DEFAULT,
)
from shared_utils import decimal_to_text, mask_text, parse_worker_threads
from sensitive_data import redact_sensitive_text
from system_proxy import configure_requests_session_for_proxy, refresh_system_proxy_cache, system_proxy_map

ONCHAIN_MAX_TOKEN_DECIMALS = 36


class OnchainRuntimeMixin(object):
    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)
    @classmethod
    def _decimal_to_text(cls, v: Decimal) -> str:
        return decimal_to_text(v)
    @staticmethod
    def _factor_by_decimals(decimals: int) -> Decimal:
        if decimals < 0 or decimals > ONCHAIN_MAX_TOKEN_DECIMALS:
            raise RuntimeError(f"代币精度超出范围：{decimals}")
        return Decimal(10) ** decimals
    @classmethod
    def _units_to_amount(cls, units: int, decimals: int) -> Decimal:
        return Decimal(units) / cls._factor_by_decimals(decimals)
    @classmethod
    def _amount_to_units(cls, amount: Decimal, decimals: int) -> int:
        val = (amount * cls._factor_by_decimals(decimals)).to_integral_value(rounding=ROUND_FLOOR)
        return int(val)
    def _random_amount_unit(cls, decimals: int) -> Decimal:
        if decimals < 0 or decimals > cls.MAX_TOKEN_DECIMALS:
            raise RuntimeError(f"代币精度超出范围：{decimals}")
        return Decimal("1").scaleb(-int(decimals))
    def _random_amount_range_text(cls, low: Decimal, high: Decimal) -> str:
        return f"随机 {cls._decimal_to_text(low)} ~ {cls._decimal_to_text(high)}"
    def _network_fee_symbol(self, network: str) -> str:
        try:
            symbol = self.client.get_symbol(network)
        except Exception:
            symbol = ""
        return symbol or network.strip().upper() or "-"
    def _gas_fee_amount_text(self, network: str, fee_wei: int) -> str:
        fee_text = self._decimal_to_text(self._units_to_amount(int(fee_wei), 18))
        symbol = self._network_fee_symbol(network)
        return f"{fee_text} {symbol}"
    def _estimated_gas_fee_text(self, network: str, fee_wei: int) -> str:
        return f"预估 {self._gas_fee_amount_text(network, fee_wei)}"
    def _token_amount_text(self, symbol: str, amount: Decimal) -> str:
        return f"{self._decimal_to_text(amount)} {symbol.strip().upper()}"
    def _runtime_worker_threads(self) -> int:
        raw = self._runtime_state_snapshot().get("threads_raw", ONCHAIN_WORKER_THREADS_DEFAULT)
        return parse_worker_threads(raw, default=ONCHAIN_WORKER_THREADS_DEFAULT)
    @staticmethod
    def _parse_confirm_poll_interval_seconds(value: object) -> float:
        try:
            interval = float(str(value).strip())
        except Exception as exc:
            raise ValueError("确认轮询间隔格式错误") from exc
        if not math.isfinite(interval) or interval < ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_MIN:
            raise ValueError(f"确认轮询间隔必须是不小于 {ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_MIN:g} 秒的有限数字")
        return interval
    def _runtime_confirmation_poll_interval_seconds(self) -> float:
        raw = self._runtime_state_snapshot().get(
            "confirm_poll_interval_raw",
            ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT,
        )
        try:
            return self._parse_confirm_poll_interval_seconds(raw)
        except ValueError:
            return ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT
    def _sync_runtime_state(self) -> None:
        threads_var = getattr(self, "threads_var", None)
        threads_raw = str(threads_var.get() if threads_var is not None else ONCHAIN_WORKER_THREADS_DEFAULT)
        poll_interval_var = getattr(self, "confirm_poll_interval_var", None)
        poll_interval_raw = str(
            poll_interval_var.get() if poll_interval_var is not None else ONCHAIN_CONFIRM_POLL_INTERVAL_SECONDS_DEFAULT
        )
        with self._runtime_state_lock:
            self._runtime_state = {
                "threads_raw": threads_raw,
                "confirm_poll_interval_raw": poll_interval_raw,
            }
    def _runtime_state_snapshot(self) -> dict[str, object]:
        with self._runtime_state_lock:
            return dict(self._runtime_state)
    def _on_runtime_settings_changed(self, *_args) -> None:
        self._sync_runtime_state()
    @staticmethod
    def _http_get_via_proxy(
        url: str,
        *,
        proxies: dict[str, str] | None = None,
        timeout: int = 10,
        allow_system_proxy: bool = True,
    ):
        session = requests.Session()
        try:
            if proxies:
                session.trust_env = False
                session.proxies.update(proxies)
            else:
                configure_requests_session_for_proxy(
                    session,
                    target_url=url,
                    allow_system_proxy=allow_system_proxy,
                )
            return session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        finally:
            session.close()
    def _allow_onchain_system_proxy(self) -> bool:
        state = self._onchain_proxy_state_snapshot()
        return not bool(state.get("use_config_proxy") and str(state.get("raw_proxy") or "").strip())
    def _sync_onchain_proxy_state(self) -> None:
        use_var = getattr(self, "use_config_proxy_var", None)
        proxy_var = getattr(self, "onchain_proxy_var", None)
        use_proxy = bool(use_var.get()) if use_var is not None else False
        raw_proxy = str(proxy_var.get() or "").strip() if proxy_var is not None else ""
        with self._onchain_proxy_state_lock:
            self._onchain_proxy_state = {"use_config_proxy": use_proxy, "raw_proxy": raw_proxy}
    def _onchain_proxy_state_snapshot(self) -> dict[str, object]:
        with self._onchain_proxy_state_lock:
            return dict(self._onchain_proxy_state)
    def _normalize_onchain_proxy(self, proxy_text: str) -> str:
        text = str(proxy_text or "").strip()
        if not text:
            return ""
        normalizer = getattr(self, "_proxy_text_normalizer", None)
        if callable(normalizer):
            return str(normalizer(text))
        lower = text.lower()
        if "://" not in text:
            text = f"http://{text}"
            lower = text.lower()
        if not lower.startswith(("http://", "https://", "socks5://", "socks5h://", "ss://")):
            raise RuntimeError("代理地址格式不支持，请使用 http://、https://、socks5://、socks5h:// 或 ss://")
        return text
    def _onchain_proxy_map(self, state: dict[str, object] | None = None) -> dict[str, str]:
        snapshot = dict(state or self._onchain_proxy_state_snapshot())
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        if not use_config_proxy:
            return {}
        proxy_text = self._normalize_onchain_proxy(snapshot.get("raw_proxy") or "")
        if not proxy_text:
            return {}
        if callable(self._rpc_proxy_getter):
            try:
                proxy_url = self._rpc_proxy_getter(proxy_text=proxy_text, use_config_proxy=use_config_proxy)
            except TypeError:
                proxy_url = self._rpc_proxy_getter()
        else:
            proxy_url = proxy_text
        proxy_url = str(proxy_url or "").strip()
        if not proxy_url:
            return {}
        return {"http": proxy_url, "https": proxy_url}
    @staticmethod
    def _onchain_system_proxy_targets() -> tuple[str, ...]:
        urls = ["https://gateway.tenderly.co/public/mainnet"]
        for network in EvmClient.NETWORKS.values():
            urls.extend(str(url or "") for url in network.get("rpc_urls", []))
        return tuple(dict.fromkeys(url for url in urls if url))
    @classmethod
    def _refresh_onchain_system_proxy_cache(cls, *, background: bool = False) -> dict[str, str]:
        return refresh_system_proxy_cache(cls._onchain_system_proxy_targets(), background=background)
    @staticmethod
    def _onchain_system_proxy_map(*, refresh: bool = False) -> dict[str, str]:
        return system_proxy_map("https://gateway.tenderly.co/public/mainnet", refresh=refresh)
    def _schedule_onchain_system_proxy_cache_refresh(self) -> None:
        root = getattr(self, "root", None) or self

        def _refresh() -> None:
            setattr(self, "_onchain_system_proxy_refresh_after_token", None)
            self._refresh_onchain_system_proxy_cache(background=True)

        token = getattr(self, "_onchain_system_proxy_refresh_after_token", None)
        if token is not None:
            try:
                root.after_cancel(token)
            except Exception:
                pass
        try:
            setattr(self, "_onchain_system_proxy_refresh_after_token", root.after(300, _refresh))
        except Exception:
            self._refresh_onchain_system_proxy_cache(background=True)
    def _onchain_proxy_route_text(self, state: dict[str, object] | None = None) -> str:
        snapshot = dict(state or self._onchain_proxy_state_snapshot())
        raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        if not use_config_proxy:
            system_proxy = self._onchain_system_proxy_map()
            if system_proxy:
                return redact_sensitive_text(f"system-proxy -> {system_proxy.get('https') or system_proxy.get('http')}")
            return "direct"
        if not raw_proxy:
            system_proxy = self._onchain_system_proxy_map()
            if system_proxy:
                return redact_sensitive_text(f"system-proxy -> {system_proxy.get('https') or system_proxy.get('http')}")
            return "direct"
        if raw_proxy.lower().startswith("ss://"):
            return "builtin-ss"
        return redact_sensitive_text(f"manual-proxy -> {self._normalize_onchain_proxy(raw_proxy)}")
    def _fetch_onchain_public_ip(
        self,
        *,
        use_config_proxy: bool,
        allow_system_proxy: bool = True,
        state: dict[str, object] | None = None,
    ) -> str:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://ipinfo.io/ip",
        ]
        proxies = self._onchain_proxy_map(state=state) if use_config_proxy else None
        for url in urls:
            try:
                resp = self._http_get_via_proxy(
                    url,
                    proxies=proxies or None,
                    timeout=6,
                    allow_system_proxy=allow_system_proxy,
                )
                resp.raise_for_status()
                ip = (resp.text or "").strip()
                ipaddress.ip_address(ip)
                return ip
            except Exception:
                continue
        raise RuntimeError("网络不可达或 IP 服务异常")
    def _proxy_test_networks(self, selected_network: str = "") -> list[str]:
        selected_network = str(selected_network or self.network_var.get() or "").strip().upper()
        if selected_network in EvmClient.NETWORKS:
            return [selected_network]
        return list(EvmClient.NETWORKS.keys())
    def _test_onchain_target_connectivity(self, selected_network: str = "") -> str:
        networks = self._proxy_test_networks(selected_network)
        details: list[str] = []
        errors: list[str] = []
        success_count = 0
        for network in networks:
            try:
                chain_id = self.client.get_rpc_chain_id(network)
                details.append(f"{network} RPC chainId={chain_id}")
                success_count += 1
            except Exception as exc:
                errors.append(f"{network}: {exc}")
                if len(networks) == 1:
                    raise
        if success_count <= 0:
            raise RuntimeError("；".join(errors) if errors else "RPC 连通性测试失败")
        if errors:
            details.append("部分失败=" + " | ".join(errors))
        return "；".join(details)
    def _test_onchain_proxy_once(
        self,
        *,
        include_exit_ip: bool = True,
        state: dict[str, object] | None = None,
        selected_network: str = "",
    ) -> tuple[str, str, str]:
        snapshot = dict(state or self._onchain_proxy_state_snapshot())
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
        has_config_proxy = bool(use_config_proxy and raw_proxy)
        system_proxy = self._onchain_system_proxy_map() if not has_config_proxy else {}
        status = "跟随系统代理" if system_proxy else "未启用"
        exit_ip = "--"
        if has_config_proxy:
            status = "SS代理连接中..." if raw_proxy.lower().startswith("ss://") else "代理连接中..."
        if has_config_proxy:
            # 触发代理规范化/内置 SS runtime 初始化
            self._onchain_proxy_map(state=snapshot)
        target = self._test_onchain_target_connectivity(selected_network)
        if has_config_proxy:
            status = "SS代理已连接" if raw_proxy.lower().startswith("ss://") else "代理已连接"
        elif system_proxy:
            status = "系统代理已连接"
        else:
            status = "直连可用"
        if has_config_proxy:
            if include_exit_ip:
                exit_ip = self._fetch_onchain_public_ip(use_config_proxy=True, allow_system_proxy=False, state=snapshot)
        elif system_proxy:
            if include_exit_ip:
                exit_ip = self._fetch_onchain_public_ip(use_config_proxy=False, allow_system_proxy=True)
        else:
            if include_exit_ip:
                exit_ip = self._fetch_onchain_public_ip(use_config_proxy=False, allow_system_proxy=False)
        return status, exit_ip, target
    def _refresh_onchain_proxy_summary(self) -> None:
        raw_proxy = str(self.onchain_proxy_var.get() or "").strip()
        has_config_proxy = bool(self.use_config_proxy_var.get() and raw_proxy)
        if has_config_proxy:
            status = "待测试"
        else:
            status = "跟随系统代理" if self._onchain_system_proxy_map() else "未启用"
        self.onchain_proxy_status_var.set(status)
        self.onchain_proxy_exit_ip_var.set("--")
    def _mask_credential(self, credential: str) -> str:
        s = credential.strip()
        if re.fullmatch(r"(0x)?[a-fA-F0-9]{64}", s):
            return self._mask(s)
        words = [x for x in s.split() if x]
        if len(words) >= 2:
            return f"{words[0]} {words[1]} ... ({len(words)}词)"
        return self._mask(s, head=4, tail=4)


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
