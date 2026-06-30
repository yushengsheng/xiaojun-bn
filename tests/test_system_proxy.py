#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from system_proxy import (
    PROXY_ROUTE_CONFIGURED,
    PROXY_ROUTE_DIRECT,
    PROXY_ROUTE_SYSTEM,
    clear_system_proxy_cache,
    configure_requests_session_for_proxy,
    parse_macos_scutil_proxy_output,
    refresh_system_proxy_cache,
    resolve_proxy_route,
    system_proxy_map,
)


SCUTIL_PROXY_OUTPUT = """<dictionary> {
  ExceptionsList : <array> {
    0 : 198.18.0.0/15
    1 : localhost
  }
  HTTPEnable : 1
  HTTPPort : 1082
  HTTPProxy : 127.0.0.1
  HTTPSEnable : 1
  HTTPSPort : 1082
  HTTPSProxy : 127.0.0.1
  SOCKSEnable : 0
}
"""
LOCAL_PROXY = {"http": "http://127.0.0.1:1082", "https": "http://127.0.0.1:1082"}


class SystemProxyRuleTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_system_proxy_cache()

    def tearDown(self) -> None:
        clear_system_proxy_cache()

    def test_parse_macos_scutil_http_https_proxy(self) -> None:
        proxies = parse_macos_scutil_proxy_output(SCUTIL_PROXY_OUTPUT, target_url="https://api.binance.com")
        self.assertEqual(proxies["http"], "http://127.0.0.1:1082")
        self.assertEqual(proxies["https"], "http://127.0.0.1:1082")

    def test_parse_macos_scutil_respects_exception_network(self) -> None:
        proxies = parse_macos_scutil_proxy_output(SCUTIL_PROXY_OUTPUT, target_url="https://198.18.0.13")
        self.assertEqual(proxies, {})

    def test_proxy_route_priority_is_configured_then_system_then_direct(self) -> None:
        with patch("system_proxy.system_proxy_map", return_value={"http": "http://127.0.0.1:1082", "https": "http://127.0.0.1:1082"}):
            route, proxies = resolve_proxy_route(explicit_proxy="socks5h://127.0.0.1:9999", target_url="https://api.binance.com")
            self.assertEqual(route, PROXY_ROUTE_CONFIGURED)
            self.assertEqual(proxies["https"], "socks5h://127.0.0.1:9999")

            route, proxies = resolve_proxy_route(explicit_proxy="", target_url="https://api.binance.com")
            self.assertEqual(route, PROXY_ROUTE_SYSTEM)
            self.assertEqual(proxies["https"], "http://127.0.0.1:1082")

        with patch("system_proxy.system_proxy_map", return_value={}):
            route, proxies = resolve_proxy_route(explicit_proxy="", target_url="https://api.binance.com")
            self.assertEqual(route, PROXY_ROUTE_DIRECT)
            self.assertEqual(proxies, {})

    def test_system_proxy_falls_back_to_detected_local_proxy(self) -> None:
        with patch("system_proxy.requests.utils.get_environ_proxies", return_value={}), \
            patch("system_proxy.urllib.request.getproxies", return_value={}), \
            patch("system_proxy.macos_system_proxy_map", return_value={}), \
            patch("system_proxy.detect_local_system_proxy_map", return_value=LOCAL_PROXY):
            route, proxies = resolve_proxy_route(explicit_proxy="", target_url="https://api.binance.com")
        self.assertEqual(route, PROXY_ROUTE_SYSTEM)
        self.assertEqual(proxies["https"], "http://127.0.0.1:1082")

    def test_configure_requests_session_freezes_resolved_system_proxy(self) -> None:
        session = requests.Session()
        try:
            with patch("system_proxy.system_proxy_map", return_value={"http": "http://127.0.0.1:1082", "https": "http://127.0.0.1:1082"}):
                proxies = configure_requests_session_for_proxy(
                    session,
                    target_url="https://api.binance.com",
                    allow_system_proxy=True,
                )
            self.assertFalse(session.trust_env)
            self.assertEqual(proxies["https"], "http://127.0.0.1:1082")
            self.assertEqual(session.proxies["https"], "http://127.0.0.1:1082")
        finally:
            session.close()

    def test_system_proxy_map_uses_cache_for_repeated_reads(self) -> None:
        calls = {"count": 0}

        def local_proxy(*_args, **_kwargs):
            calls["count"] += 1
            return dict(LOCAL_PROXY)

        with patch("system_proxy.requests.utils.get_environ_proxies", return_value={}), \
            patch("system_proxy.urllib.request.getproxies", return_value={}), \
            patch("system_proxy.macos_system_proxy_map", return_value={}), \
            patch("system_proxy.detect_local_system_proxy_map", side_effect=local_proxy):
            self.assertEqual(system_proxy_map("https://api.binance.com/api/v3/time", refresh=True), LOCAL_PROXY)
            self.assertEqual(system_proxy_map("https://api.binance.com/api/v3/time"), LOCAL_PROXY)
            self.assertEqual(system_proxy_map("https://api.binance.com/api/v3/time"), LOCAL_PROXY)

        self.assertEqual(calls["count"], 1)

    def test_refresh_system_proxy_cache_forces_redetection(self) -> None:
        responses = [{}, dict(LOCAL_PROXY)]

        def local_proxy(*_args, **_kwargs):
            return responses.pop(0)

        with patch("system_proxy.requests.utils.get_environ_proxies", return_value={}), \
            patch("system_proxy.urllib.request.getproxies", return_value={}), \
            patch("system_proxy.macos_system_proxy_map", return_value={}), \
            patch("system_proxy.detect_local_system_proxy_map", side_effect=local_proxy):
            self.assertEqual(refresh_system_proxy_cache("https://api.binance.com/api/v3/time"), {})
            self.assertEqual(system_proxy_map("https://api.binance.com/api/v3/time"), {})
            self.assertEqual(refresh_system_proxy_cache("https://api.binance.com/api/v3/time"), LOCAL_PROXY)
            self.assertEqual(system_proxy_map("https://api.binance.com/api/v3/time"), LOCAL_PROXY)


if __name__ == "__main__":
    unittest.main()
