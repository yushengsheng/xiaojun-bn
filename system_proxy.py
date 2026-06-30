#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ipaddress
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable

import requests


_CACHE_SECONDS = 60.0
_CACHE_LOCK = threading.Lock()
_MACOS_SCUTIL_CACHE: tuple[float, str] = (0.0, "")
_LOCAL_PROXY_CACHE: tuple[float, dict[str, str] | None] = (0.0, None)
_SYSTEM_PROXY_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
_SYSTEM_PROXY_REFRESHING: set[str] = set()
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_COMMON_LOCAL_PROXY_PORTS = (1082, 7890, 7897, 1087, 1080, 8080, 8888, 6152)
PROXY_ROUTE_CONFIGURED = "configured"
PROXY_ROUTE_SYSTEM = "system"
PROXY_ROUTE_DIRECT = "direct"


def _target_host(target_url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(str(target_url or ""))
    except Exception:
        return ""
    return str(parsed.hostname or "").strip().lower()


def _target_url(target_url: str = "") -> str:
    return str(target_url or "https://example.com").strip() or "https://example.com"


def _cache_key(target_url: str = "") -> str:
    url = _target_url(target_url)
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return url
    scheme = (parsed.scheme or "https").lower()
    hostname = (parsed.hostname or "").strip().lower()
    port = f":{parsed.port}" if parsed.port else ""
    if hostname:
        return f"{scheme}://{hostname}{port}"
    return url


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _proxy_url(host: object, port: object, *, scheme: str = "http") -> str:
    host_text = str(host or "").strip()
    port_text = str(port or "").strip()
    if not host_text or not port_text:
        return ""
    if ":" in host_text and not (host_text.startswith("[") and host_text.endswith("]")):
        host_text = f"[{host_text}]"
    return f"{scheme}://{host_text}:{port_text}"


def _normalize_proxy_map(proxies: dict[str, object] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in ("http", "https"):
        value = str((proxies or {}).get(key) or "").strip()
        if value:
            result[key] = value
    if not result:
        all_proxy = str((proxies or {}).get("all") or "").strip()
        if all_proxy:
            result = {"http": all_proxy, "https": all_proxy}
    return result


def _host_matches_exception(host: str, patterns: list[str]) -> bool:
    host_l = str(host or "").strip().lower().strip("[]")
    if not host_l:
        return False
    if host_l in _LOCAL_HOSTS:
        return True
    for pattern in patterns:
        p = str(pattern or "").strip().lower()
        if not p:
            continue
        try:
            if "/" in p:
                network = ipaddress.ip_network(p, strict=False)
                address = ipaddress.ip_address(host_l)
                if address in network:
                    return True
                continue
        except Exception:
            pass
        if p.startswith("*.") and host_l.endswith(p[1:]):
            return True
        if p == host_l:
            return True
    return False


def parse_macos_scutil_proxy_output(output: str, *, target_url: str = "") -> dict[str, str]:
    values: dict[str, str] = {}
    exceptions: list[str] = []
    in_exceptions = False

    for raw_line in str(output or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("ExceptionsList"):
            in_exceptions = True
            continue
        if in_exceptions:
            if line.startswith("}"):
                in_exceptions = False
                continue
            match = re.match(r"\d+\s*:\s*(.+)$", line)
            if match:
                exceptions.append(match.group(1).strip())
            continue
        match = re.match(r"([A-Za-z0-9]+)\s*:\s*(.+)$", line)
        if match:
            values[match.group(1)] = match.group(2).strip()

    host = _target_host(target_url)
    if _host_matches_exception(host, exceptions):
        return {}

    result: dict[str, str] = {}
    if _enabled(values.get("HTTPEnable")):
        http_url = _proxy_url(values.get("HTTPProxy"), values.get("HTTPPort"))
        if http_url:
            result["http"] = http_url
    if _enabled(values.get("HTTPSEnable")):
        https_url = _proxy_url(values.get("HTTPSProxy"), values.get("HTTPSPort"))
        if https_url:
            result["https"] = https_url
    if _enabled(values.get("SOCKSEnable")):
        socks_url = _proxy_url(values.get("SOCKSProxy"), values.get("SOCKSPort"), scheme="socks5h")
        if socks_url:
            result.setdefault("http", socks_url)
            result.setdefault("https", socks_url)
    if result and "https" not in result and "http" in result:
        result["https"] = result["http"]
    if result and "http" not in result and "https" in result:
        result["http"] = result["https"]
    return result


def macos_system_proxy_map(target_url: str = "", *, refresh: bool = False) -> dict[str, str]:
    global _MACOS_SCUTIL_CACHE
    if sys.platform != "darwin":
        return {}
    now = time.monotonic()
    with _CACHE_LOCK:
        cached_at, cached_output = _MACOS_SCUTIL_CACHE
        if (not refresh) and cached_output and (now - cached_at) < _CACHE_SECONDS:
            return parse_macos_scutil_proxy_output(cached_output, target_url=target_url)
    try:
        completed = subprocess.run(
            ["/usr/sbin/scutil", "--proxy"],
            capture_output=True,
            text=True,
            timeout=1.0,
            check=False,
        )
    except FileNotFoundError:
        try:
            completed = subprocess.run(
                ["scutil", "--proxy"],
                capture_output=True,
                text=True,
                timeout=1.0,
                check=False,
            )
        except Exception:
            return {}
    except Exception:
        return {}
    if completed.returncode != 0:
        return {}
    proxies = parse_macos_scutil_proxy_output(completed.stdout, target_url=target_url)
    with _CACHE_LOCK:
        _MACOS_SCUTIL_CACHE = (time.monotonic(), str(completed.stdout or ""))
    return proxies


def _http_proxy_probe(port: int, target_url: str) -> bool:
    target_host = _target_host(target_url) or "example.com"
    request = (
        f"CONNECT {target_host}:443 HTTP/1.1\r\n"
        f"Host: {target_host}:443\r\n"
        "Proxy-Connection: close\r\n"
        "\r\n"
    ).encode("ascii", errors="ignore")
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.25) as sock:
            sock.settimeout(0.35)
            sock.sendall(request)
            data = sock.recv(32)
        return data.startswith(b"HTTP/")
    except Exception:
        return False


def _socks5_proxy_probe(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.25) as sock:
            sock.settimeout(0.35)
            sock.sendall(b"\x05\x01\x00")
            data = sock.recv(2)
        return len(data) >= 2 and data[0] == 5
    except Exception:
        return False


def detect_local_system_proxy_map(target_url: str = "", *, refresh: bool = False) -> dict[str, str]:
    global _LOCAL_PROXY_CACHE
    now = time.monotonic()
    with _CACHE_LOCK:
        cached_at, cached = _LOCAL_PROXY_CACHE
        if (not refresh) and cached is not None and (now - cached_at) < _CACHE_SECONDS:
            return dict(cached)
    ports = list(_COMMON_LOCAL_PROXY_PORTS)
    result: dict[str, str] = {}
    for port in ports:
        if _http_proxy_probe(port, target_url):
            proxy = f"http://127.0.0.1:{port}"
            result = {"http": proxy, "https": proxy}
            break
    if not result:
        for port in ports:
            if _socks5_proxy_probe(port):
                proxy = f"socks5h://127.0.0.1:{port}"
                result = {"http": proxy, "https": proxy}
                break
    with _CACHE_LOCK:
        _LOCAL_PROXY_CACHE = (time.monotonic(), dict(result))
    return result


def _detect_system_proxy_map(target_url: str = "", *, refresh: bool = False) -> dict[str, str]:
    url = _target_url(target_url)
    try:
        proxies = _normalize_proxy_map(requests.utils.get_environ_proxies(url) or {})
        if proxies:
            return proxies
    except Exception:
        pass
    try:
        proxies = _normalize_proxy_map(urllib.request.getproxies() or {})
        if proxies:
            return proxies
    except Exception:
        pass
    proxies = macos_system_proxy_map(url, refresh=refresh)
    if proxies:
        return proxies
    return detect_local_system_proxy_map(url, refresh=refresh)


def _store_system_proxy_cache(key: str, proxies: dict[str, str]) -> dict[str, str]:
    with _CACHE_LOCK:
        _SYSTEM_PROXY_CACHE[key] = (time.monotonic(), dict(proxies))
    return dict(proxies)


def _refresh_system_proxy_cache_sync(target_url: str = "", *, refresh: bool = True) -> dict[str, str]:
    url = _target_url(target_url)
    key = _cache_key(url)
    proxies = _detect_system_proxy_map(url, refresh=refresh)
    return _store_system_proxy_cache(key, proxies)


def _refresh_system_proxy_cache_worker(urls: list[str], keys: list[str]) -> None:
    try:
        for index, url in enumerate(urls):
            _refresh_system_proxy_cache_sync(url, refresh=(index == 0))
    finally:
        with _CACHE_LOCK:
            for key in keys:
                _SYSTEM_PROXY_REFRESHING.discard(key)


def _schedule_async_system_proxy_refresh(target_urls: str | Iterable[str] = "") -> None:
    if isinstance(target_urls, str):
        urls = [_target_url(target_urls)]
    else:
        urls = [_target_url(url) for url in target_urls]
    pending_urls: list[str] = []
    pending_keys: list[str] = []
    with _CACHE_LOCK:
        for url in urls:
            key = _cache_key(url)
            if key in _SYSTEM_PROXY_REFRESHING:
                continue
            _SYSTEM_PROXY_REFRESHING.add(key)
            pending_urls.append(url)
            pending_keys.append(key)
    if not pending_urls:
        return
    thread = threading.Thread(
        target=_refresh_system_proxy_cache_worker,
        args=(pending_urls, pending_keys),
        name="system-proxy-refresh",
        daemon=True,
    )
    thread.start()


def cached_system_proxy_map(target_url: str = "") -> dict[str, str]:
    key = _cache_key(target_url)
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _SYSTEM_PROXY_CACHE.get(key)
    if cached is None:
        return {}
    cached_at, proxies = cached
    if now - cached_at >= _CACHE_SECONDS:
        _schedule_async_system_proxy_refresh(target_url)
    return dict(proxies)


def refresh_system_proxy_cache(
    target_urls: str | Iterable[str] = "",
    *,
    background: bool = False,
) -> dict[str, str]:
    if isinstance(target_urls, str):
        urls = [_target_url(target_urls)]
    else:
        urls = [_target_url(url) for url in target_urls]
    last: dict[str, str] = {}
    if background:
        _schedule_async_system_proxy_refresh(urls)
        for url in urls:
            last = cached_system_proxy_map(url)
        return last
    for index, url in enumerate(urls):
        last = _refresh_system_proxy_cache_sync(url, refresh=(index == 0))
    return last


def clear_system_proxy_cache() -> None:
    global _MACOS_SCUTIL_CACHE, _LOCAL_PROXY_CACHE
    with _CACHE_LOCK:
        _MACOS_SCUTIL_CACHE = (0.0, "")
        _LOCAL_PROXY_CACHE = (0.0, None)
        _SYSTEM_PROXY_CACHE.clear()
        _SYSTEM_PROXY_REFRESHING.clear()


def system_proxy_map(target_url: str = "", *, refresh: bool = False) -> dict[str, str]:
    if refresh:
        return _refresh_system_proxy_cache_sync(target_url, refresh=True)

    key = _cache_key(target_url)
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _SYSTEM_PROXY_CACHE.get(key)
    if cached is None:
        return _refresh_system_proxy_cache_sync(target_url, refresh=False)

    cached_at, proxies = cached
    if now - cached_at >= _CACHE_SECONDS:
        _schedule_async_system_proxy_refresh(target_url)
    return dict(proxies)


def resolve_proxy_route(
    *,
    explicit_proxy: str = "",
    target_url: str = "",
    allow_system_proxy: bool = True,
) -> tuple[str, dict[str, str]]:
    """Permanent routing rule: configured proxy, then system proxy, then direct."""

    proxy_text = str(explicit_proxy or "").strip()
    if proxy_text:
        return PROXY_ROUTE_CONFIGURED, {"http": proxy_text, "https": proxy_text}
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
    if proxies:
        session.proxies.update(proxies)
    return proxies


__all__ = [
    "cached_system_proxy_map",
    "clear_system_proxy_cache",
    "configure_requests_session_for_proxy",
    "detect_local_system_proxy_map",
    "macos_system_proxy_map",
    "parse_macos_scutil_proxy_output",
    "PROXY_ROUTE_CONFIGURED",
    "PROXY_ROUTE_DIRECT",
    "PROXY_ROUTE_SYSTEM",
    "refresh_system_proxy_cache",
    "resolve_proxy_route",
    "system_proxy_map",
]
