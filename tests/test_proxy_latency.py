#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from exchange_app_config import ExchangeAppConfigMixin
from exchange_app_log_view import ExchangeAppLogViewMixin
from page_onchain_base import OnchainTransferPageBase


class _Var:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class ProxyLatencyTests(unittest.TestCase):
    def test_exchange_latency_measures_fresh_business_request(self) -> None:
        response = Mock()
        response.json.return_value = {"serverTime": 123456789}
        latency: list[int] = []
        with patch("exchange_app_log_view.http_get_via_proxy", return_value=response), patch(
            "exchange_app_log_view.time.perf_counter", side_effect=[10.0, 10.123]
        ):
            detail = ExchangeAppLogViewMixin._test_exchange_target_connectivity(
                proxies={"https": "http://127.0.0.1:1234"},
                allow_system_proxy=False,
                latency_out=latency,
            )
        self.assertEqual(latency, [123])
        self.assertIn("serverTime=123456789", detail)
        response.raise_for_status.assert_called_once_with()

    def test_onchain_latency_uses_first_successful_rpc(self) -> None:
        page = OnchainTransferPageBase.__new__(OnchainTransferPageBase)
        page._proxy_test_networks = lambda _selected="": ["ETH", "BSC"]
        page.client = Mock()
        page.client.get_rpc_chain_id.side_effect = [RuntimeError("ETH unavailable"), 56]
        latency: list[int] = []
        with patch("page_onchain_base.time.perf_counter", side_effect=[1.0, 2.0, 2.08]):
            detail = page._test_onchain_target_connectivity(latency_out=latency)
        self.assertEqual(detail, "BSC RPC chainId=56")
        self.assertEqual(latency, [80])
        self.assertEqual(page.client.get_rpc_chain_id.call_count, 2)

    def test_proxy_normalizer_accepts_both_hysteria2_schemes(self) -> None:
        pin = "%3A".join(["AA"] * 32)
        for scheme in ("hy2", "hysteria2"):
            uri = f"{scheme}://auth@example.com:443/?pinSHA256={pin}"
            self.assertEqual(ExchangeAppConfigMixin._normalize_proxy_text(uri), uri)

    def test_exchange_button_runs_a_fresh_test_on_every_click(self) -> None:
        class Probe(ExchangeAppLogViewMixin):
            def __init__(self):
                self._exchange_proxy_test_inflight = False
                self.exchange_proxy_latency_var = _Var("延迟 --")
                self.exchange_proxy_status_var = _Var()
                self.exchange_proxy_exit_ip_var = _Var()
                self.calls = 0

            def _exchange_proxy_state_snapshot(self):
                return {"use_config_proxy": True, "raw_proxy": "http://127.0.0.1:8080"}

            def _exchange_proxy_route_text_from_state(self, _state):
                return "manual-proxy"

            def _test_exchange_proxy_once(self, *, state, latency_out):
                self.calls += 1
                latency_out.append(20 + self.calls)
                return "代理已连接", "1.2.3.4", "target OK"

            def _save_exchange_proxy_config_only(self, *, state):
                return None

            def _append_log(self, *_args, **_kwargs):
                return None

            def _start_managed_thread(self, target, **_kwargs):
                target()

            def _dispatch_ui(self, callback):
                callback()

        probe = Probe()
        probe.test_exchange_proxy()
        self.assertEqual(probe.exchange_proxy_latency_var.get(), "延迟 21 ms")
        probe.test_exchange_proxy()
        self.assertEqual(probe.exchange_proxy_latency_var.get(), "延迟 22 ms")
        self.assertEqual(probe.calls, 2)

    def test_onchain_button_displays_failure(self) -> None:
        class Probe(OnchainTransferPageBase):
            def __init__(self):
                self._proxy_test_inflight = False
                self.onchain_proxy_latency_var = _Var("延迟 --")
                self.onchain_proxy_status_var = _Var()
                self.onchain_proxy_exit_ip_var = _Var()
                self.network_var = _Var("ETH")

            def _onchain_proxy_state_snapshot(self):
                return {"use_config_proxy": True, "raw_proxy": "hysteria2://auth@example.com/"}

            def _onchain_proxy_route_text(self, *, state):
                return "builtin-hysteria2/hysteria"

            def _test_onchain_proxy_once(self, **_kwargs):
                raise RuntimeError("connect failed")

            def _start_managed_thread(self, target, **_kwargs):
                target()

            def _dispatch_ui(self, callback):
                callback()

            def log(self, _text):
                return None

        probe = Probe()
        probe.test_onchain_proxy()
        self.assertEqual(probe.onchain_proxy_latency_var.get(), "失败")
        self.assertEqual(probe.onchain_proxy_status_var.get(), "连接失败")


if __name__ == "__main__":
    unittest.main()
