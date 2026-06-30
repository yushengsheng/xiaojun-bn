#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

from api_clients import EvmClient
from exchange_binance_client import BinanceClient


SYSTEM_PROXY = {"http": "http://127.0.0.1:1082", "https": "http://127.0.0.1:1082"}


class NetworkProxyRuleIntegrationTests(unittest.TestCase):
    def test_exchange_client_uses_configured_proxy_before_system_proxy(self) -> None:
        with patch("system_proxy.system_proxy_map", return_value=SYSTEM_PROXY):
            client = BinanceClient("key", "secret", proxy_url="socks5h://127.0.0.1:9999")
        try:
            self.assertFalse(client.session.trust_env)
            self.assertEqual(client.session.proxies["https"], "socks5h://127.0.0.1:9999")
        finally:
            client.close()

    def test_exchange_client_falls_back_to_system_proxy_when_unconfigured(self) -> None:
        with patch("system_proxy.system_proxy_map", return_value=SYSTEM_PROXY):
            client = BinanceClient("key", "secret")
        try:
            self.assertFalse(client.session.trust_env)
            self.assertEqual(client.session.proxies["https"], "http://127.0.0.1:1082")
        finally:
            client.close()

    def test_onchain_client_uses_independent_configured_proxy_before_system_proxy(self) -> None:
        with patch("system_proxy.system_proxy_map", return_value=SYSTEM_PROXY):
            client = EvmClient(proxy_provider=lambda: "socks5h://127.0.0.1:7777")
            session, proxy = client._rpc_session("https://bsc-dataseed.bnbchain.org")
        try:
            self.assertFalse(session.trust_env)
            self.assertEqual(session.proxies["https"], "socks5h://127.0.0.1:7777")
            self.assertEqual(proxy, "socks5h://127.0.0.1:7777")
        finally:
            client.close()

    def test_onchain_client_falls_back_to_system_proxy_when_unconfigured(self) -> None:
        with patch("system_proxy.system_proxy_map", return_value=SYSTEM_PROXY):
            client = EvmClient(proxy_provider=lambda: "")
            session, proxy = client._rpc_session("https://bsc-dataseed.bnbchain.org")
        try:
            self.assertFalse(session.trust_env)
            self.assertEqual(session.proxies["https"], "http://127.0.0.1:1082")
            self.assertEqual(proxy, "http://127.0.0.1:1082")
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
