#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ipaddress
import platform
import socket
import subprocess
import threading
import time
import urllib.request
from urllib.parse import urlsplit

import requests


PROXY_ROUTE_CONFIGURED = "configured"
PROXY_ROUTE_SYSTEM = "system"
PROXY_ROUTE_DIRECT = "direct"
SYSTEM_PROXY_CACHE_TTL_SECONDS = 60.0
_CACHE_LOCK = threading.RLock()
_CACHE_VALUES: dict[str, tuple[float, dict[str, str]]] = {}


def _proxy_map(proxy_url: str) -> dict[str, str]:
    proxy = str(proxy_url or "").strip()
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}


def _clean_proxy_map(proxies: dict | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in ("http", "https"):
        value = str((proxies or {}).get(key) or "").strip()
        if value:
            result[key] = value
    return result


def _target_host(target_url: str) -> str:
    try:
        return str(urlsplit(str(target_url or "")).hostname or "").strip().lower()
    except Exception:
        return ""


def _target_in_exceptions(target_url: str, exceptions: list[str]) -> bool:
    host = _target_host(target_url)
    if not host:
        return False
    for item in exceptions:
        rule = str(item or "").strip().lower()
        if not rule:
            continue
        if rule == host or (rule.startswith(".") and host.endswith(rule)):
            return True
        if "/" in rule:
            try:
                if ipaddress.ip_address(host) in ipaddress.ip_network(rule, strict=False):
                    return True
            except Exception:
                pass
    return False


def parse_macos_scutil_proxy_output(text: str, *, target_url: str = "") -> dict[str, str]:
    values: dict[str, str] = {}
    exceptions: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key.isdigit():
            exceptions.append(value)
        else:
            values[key] = value

    if target_url and _target_in_exceptions(target_url, exceptions):
        return {}

    result: dict[str, str] = {}
    if values.get("HTTPEnable") == "1" and values.get("HTTPProxy") and values.get("HTTPPort"):
        proxy = f"http://{values['HTTPProxy']}:{values['HTTPPort']}"
        result["http"] = proxy
        result.setdefault("https", proxy)
    if values.get("HTTPSEnable") == "1" and values.get("HTTPSProxy") and values.get("HTTPSPort"):
        result["https"] = f"http://{values['HTTPSProxy']}:{values['HTTPSPort']}"
    if not result and values.get("SOCKSEnable") == "1" and values.get("SOCKSProxy") and values.get("SOCKSPort"):
        proxy = f"socks5h://{values['SOCKSProxy']}:{values['SOCKSPort']}"
        result["http"] = proxy
        result["https"] = proxy
    return result


def macos_system_proxy_map(target_url: str = "") -> dict[str, str]:
    if platform.system().lower() != "darwin":
        return {}
    try:
        result = subprocess.run(
            ["/usr/sbin/scutil", "--proxy"],
            check=False,
            capture_output=True,
            text=True,
            timeout=0.8,
        )
    except Exception:
        return {}
    if result.returncode != 0:
        return {}
    return parse_macos_scutil_proxy_output(result.stdout, target_url=target_url)


def _local_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.05):
            return True
    except Exception:
        return False


def detect_local_system_proxy_map() -> dict[str, str]:
    # Common loopback ports used by Shadowrocket/Clash/V2Ray-style clients.
    for port in (1082, 7890, 7897, 1087, 1080, 8080, 8888, 6152):
        if _local_port_open(port):
            proxy = f"http://127.0.0.1:{port}"
            return {"http": proxy, "https": proxy}
    return {}


def _detect_system_proxy_map(target_url: str = "", *, include_local_probe: bool = False) -> dict[str, str]:
    try:
        proxies = _clean_proxy_map(requests.utils.get_environ_proxies(target_url or "https://api.binance.com"))
        if proxies:
            return proxies
    except Exception:
        pass
    try:
        proxies = _clean_proxy_map(urllib.request.getproxies())
        if proxies:
            return proxies
    except Exception:
        pass
    proxies = macos_system_proxy_map(target_url)
    if proxies:
        return proxies
    if include_local_probe:
        return detect_local_system_proxy_map()
    return {}


def clear_system_proxy_cache() -> None:
    with _CACHE_LOCK:
        _CACHE_VALUES.clear()


def system_proxy_map(target_url: str = "", *, refresh: bool = False, include_local_probe: bool = False) -> dict[str, str]:
    target = str(target_url or "").strip()
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE_VALUES.get(target)
        if not refresh and cached is not None:
            cached_time, cached_value = cached
            if (now - cached_time) < SYSTEM_PROXY_CACHE_TTL_SECONDS:
                return dict(cached_value)
    proxies = _detect_system_proxy_map(target, include_local_probe=include_local_probe)
    with _CACHE_LOCK:
        _CACHE_VALUES[target] = (time.monotonic(), dict(proxies))
    return dict(proxies)


def refresh_system_proxy_cache(target_url: str = "", *, include_local_probe: bool = False) -> dict[str, str]:
    return system_proxy_map(target_url, refresh=True, include_local_probe=include_local_probe)


def resolve_proxy_route(
    *,
    explicit_proxy: str = "",
    target_url: str = "",
    allow_system_proxy: bool = True,
) -> tuple[str, dict[str, str]]:
    explicit = str(explicit_proxy or "").strip()
    if explicit:
        return PROXY_ROUTE_CONFIGURED, _proxy_map(explicit)
    if allow_system_proxy:
        proxies = system_proxy_map(target_url)
        if proxies:
            return PROXY_ROUTE_SYSTEM, proxies
    return PROXY_ROUTE_DIRECT, {}


def configure_requests_session_for_proxy(
    session: requests.Session,
    *,
    explicit_proxy: str = "",
    target_url: str = "",
    allow_system_proxy: bool = True,
) -> dict[str, str]:
    _route, proxies = resolve_proxy_route(
        explicit_proxy=explicit_proxy,
        target_url=target_url,
        allow_system_proxy=allow_system_proxy,
    )
    session.trust_env = False
    session.proxies.clear()
    if proxies:
        session.proxies.update(proxies)
    return proxies
