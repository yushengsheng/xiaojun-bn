#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import time

from exchange_app_base import (
    EXCHANGE_LOG_MAX_ROWS,
    http_get_via_proxy,
    ipaddress,
    log_queue,
    queue,
)
from sensitive_data import redact_sensitive_text
from shared_utils import append_text_log_messages


PROBLEM_LOG_LEVELS = ("[WARNING]", "[ERROR]", "[CRITICAL]")
PROBLEM_LOG_KEYWORDS = (
    "失败",
    "异常",
    "错误",
    "报错",
    "Traceback",
    "Exception",
    "Client Error",
    "API error",
    "APIError",
)


class ExchangeAppLogViewMixin(object):
    def _fetch_public_ip(
        self,
        *,
        use_exchange_proxy: bool,
        allow_system_proxy: bool = True,
        state: dict[str, object] | None = None,
    ) -> str:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://ipinfo.io/ip",
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        proxies = self._requests_proxy_map_from_state(state) if use_exchange_proxy and state is not None else None
        if use_exchange_proxy and state is None:
            proxies = self._requests_proxy_map()
        for url in urls:
            try:
                r = http_get_via_proxy(
                    url,
                    headers=headers,
                    timeout=6,
                    proxies=proxies or None,
                    allow_system_proxy=allow_system_proxy,
                )
                r.raise_for_status()
                ip = (r.text or "").strip()
                ipaddress.ip_address(ip)
                return ip
            except Exception:
                continue
        raise RuntimeError("网络不可达或 IP 服务异常")
    @staticmethod
    def _test_exchange_target_connectivity(
        *,
        proxies: dict[str, str] | None,
        allow_system_proxy: bool,
        latency_out: list[int] | None = None,
    ) -> str:
        started = time.perf_counter()
        test_resp = http_get_via_proxy(
            "https://api.binance.com/api/v3/time",
            proxies=proxies or None,
            timeout=10,
            allow_system_proxy=allow_system_proxy,
        )
        test_resp.raise_for_status()
        data = test_resp.json()
        try:
            server_time = int(data.get("serverTime"))
        except Exception as exc:
            raise RuntimeError(f"Binance 时间接口返回异常：{data}") from exc
        if latency_out is not None:
            latency_out.append(max(0, round((time.perf_counter() - started) * 1000)))
        return f"Binance /api/v3/time OK (serverTime={server_time})"
    def _test_exchange_proxy_once(
        self,
        *,
        include_exit_ip: bool = True,
        state: dict[str, object] | None = None,
        latency_out: list[int] | None = None,
    ) -> tuple[str, str, str]:
        snapshot = dict(state or self._exchange_proxy_state_snapshot())
        proxies = self._requests_proxy_map_from_state(snapshot)
        proxy_text = str(snapshot.get("raw_proxy") or "").strip()
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        system_proxy = self._system_proxy_map() if not use_config_proxy else {}
        proxy_status = "跟随系统代理" if system_proxy else "未启用"
        proxy_exit_ip = "--"
        if use_config_proxy and proxy_text:
            proxy_status = "SS代理连接中..." if proxy_text.lower().startswith("ss://") else "代理连接中..."
        allow_system_proxy = bool(system_proxy) and not use_config_proxy
        target = self._test_exchange_target_connectivity(
            proxies=proxies or None,
            allow_system_proxy=allow_system_proxy,
            latency_out=latency_out,
        )
        if use_config_proxy and proxy_text:
            proxy_status = "SS代理已连接" if proxy_text.lower().startswith("ss://") else "代理已连接"
            if include_exit_ip:
                try:
                    proxy_exit_ip = self._fetch_public_ip(
                        use_exchange_proxy=True,
                        allow_system_proxy=False,
                        state=snapshot,
                    )
                except Exception:
                    proxy_exit_ip = "获取失败"
        elif system_proxy:
            proxy_status = "系统代理已连接"
            if include_exit_ip:
                try:
                    proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=True)
                except Exception:
                    proxy_exit_ip = "获取失败"
        else:
            proxy_status = "直连可用"
            if include_exit_ip:
                try:
                    proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
                except Exception:
                    proxy_exit_ip = "获取失败"
        return proxy_status, proxy_exit_ip, target
    def _resolve_exchange_ip_refresh_state(self, snapshot: dict[str, object] | None = None) -> tuple[str, str, str]:
        current = dict(snapshot or self._exchange_proxy_state_snapshot())
        use_config_proxy = bool(current.get("use_config_proxy"))
        system_proxy = self._system_proxy_map() if not use_config_proxy else {}
        proxy_status = "跟随系统代理" if system_proxy else "未启用"
        proxy_exit_ip = "--"

        try:
            ip_text = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
        except Exception as exc:
            ip_text = f"获取失败: {exc}"
            if use_config_proxy:
                proxy_status = "连接失败"
            elif system_proxy:
                proxy_status = "系统代理异常"
            else:
                proxy_status = "直连异常"
            return ip_text, proxy_status, proxy_exit_ip

        if use_config_proxy or system_proxy:
            try:
                proxy_status, proxy_exit_ip, _target = self._test_exchange_proxy_once(include_exit_ip=True, state=current)
            except Exception:
                if use_config_proxy:
                    proxy_status = "连接失败"
                elif system_proxy:
                    proxy_status = "系统代理异常"
                else:
                    proxy_status = "直连异常"
            return ip_text, proxy_status, proxy_exit_ip

        proxy_exit_ip = ip_text
        return ip_text, proxy_status, proxy_exit_ip
    def test_exchange_proxy(self):
        if self._exchange_proxy_test_inflight:
            return
        self._exchange_proxy_test_inflight = True
        self.exchange_proxy_latency_var.set("测试中…")
        snapshot = self._exchange_proxy_state_snapshot()

        def worker():
            test_ok = False
            save_err = ""
            latency_ms = None
            try:
                if not bool(snapshot.get("use_config_proxy")):
                    self._system_proxy_map(refresh=True, include_local_probe=True)
                route_text = self._exchange_proxy_route_text_from_state(snapshot)
                latency_values: list[int] = []
                status, exit_ip, target = self._test_exchange_proxy_once(
                    state=snapshot,
                    latency_out=latency_values,
                )
                latency_ms = latency_values[0]
                if self._exchange_proxy_state_snapshot() == snapshot:
                    try:
                        self._save_exchange_proxy_config_only(state=snapshot)
                    except Exception as e:
                        save_err = str(e)
                else:
                    save_err = "测试期间代理配置已变化，未保存旧配置"
                test_ok = True
                log_text = f"交易所代理测试成功：status={status}，exit_ip={exit_ip}，target={target}，route={route_text}"
                if save_err:
                    log_text = f"{log_text}，但保存配置失败：{save_err}"
                else:
                    log_text = f"{log_text}，已自动保存配置"
            except Exception as e:
                route_text = self._exchange_proxy_route_text_from_state(snapshot)
                use_config_proxy = bool(snapshot.get("use_config_proxy"))
                raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
                status = "连接失败" if (use_config_proxy and raw_proxy) else "未启用"
                if (not use_config_proxy) and self._system_proxy_map():
                    status = "系统代理异常"
                elif not use_config_proxy:
                    status = "直连异常"
                exit_ip = "--"
                log_text = f"交易所代理测试失败：{e}，route={route_text}"

            def _update():
                self._exchange_proxy_test_inflight = False
                is_current = self._exchange_proxy_state_snapshot() == snapshot
                if is_current:
                    self.exchange_proxy_status_var.set(status)
                    self.exchange_proxy_exit_ip_var.set(exit_ip)
                    self.exchange_proxy_latency_var.set(f"延迟 {latency_ms} ms" if test_ok else "失败")
                self._append_log(log_text, problem=(not test_ok) or bool(save_err))

            self._dispatch_ui(_update)

        self._start_managed_thread(worker, name="exchange-proxy-test")
    def _try_begin_ip_refresh(self) -> bool:
        with self._ip_refresh_lock:
            if self._closing or self._ip_refresh_inflight:
                return False
            self._ip_refresh_inflight = True
            return True
    def _finish_ip_refresh(self) -> None:
        with self._ip_refresh_lock:
            self._ip_refresh_inflight = False
    def update_ip(self, schedule_next: bool = True):
        if schedule_next and not self._closing:
            self._cancel_after_token("_update_ip_after_token")
            try:
                self._update_ip_after_token = self.after(60000, self.update_ip)
            except Exception:
                self._update_ip_after_token = None
        if not self._try_begin_ip_refresh():
            return

        snapshot = self._exchange_proxy_state_snapshot()

        def worker():
            try:
                ip, proxy_status, proxy_exit_ip = self._resolve_exchange_ip_refresh_state(snapshot)

                def _update():
                    self.ip_var.set(ip)
                    self.exchange_proxy_status_var.set(proxy_status)
                    self.exchange_proxy_exit_ip_var.set(proxy_exit_ip)
                self._dispatch_ui(_update)
            finally:
                self._finish_ip_refresh()

        self._start_managed_thread(worker, name="exchange-ip-refresh")
    def _poll_log_queue(self):
        if self._closing:
            self._log_poll_after_token = None
            return
        pending_logs: list[object] = []
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                pending_logs.append(msg)
        if pending_logs:
            self._append_log_batch(pending_logs)
        try:
            self._log_poll_after_token = self.after(100, self._poll_log_queue)
        except Exception:
            self._log_poll_after_token = None
    @staticmethod
    def _log_entry_text(entry: object) -> str:
        return str(getattr(entry, "message", entry) or "")

    @staticmethod
    def _is_problem_log_entry(entry: object) -> bool:
        explicit = getattr(entry, "is_problem", None)
        if isinstance(explicit, bool):
            return explicit
        if callable(explicit):
            try:
                return bool(explicit())
            except Exception:
                pass
        try:
            if int(getattr(entry, "levelno")) >= 30:
                return True
        except Exception:
            pass
        text = str(getattr(entry, "message", entry) or "")
        return any(marker in text for marker in PROBLEM_LOG_LEVELS) or any(keyword in text for keyword in PROBLEM_LOG_KEYWORDS)

    def _append_log(self, msg: str, *, problem: bool | None = None):
        if problem is None:
            self._append_log_batch([msg])
        else:
            self._append_log_batch([_ManualLogEntry(msg, problem=problem)])

    def _append_log_batch(self, messages: list[object]):
        if not messages:
            return
        run_messages = [redact_sensitive_text(self._log_entry_text(msg)) for msg in messages]
        problem_messages = [
            redact_sensitive_text(self._log_entry_text(msg))
            for msg in messages
            if self._is_problem_log_entry(msg)
        ]
        self._append_text_log_batch(self.text_log, run_messages)
        if problem_messages and hasattr(self, "text_error_log"):
            self._append_text_log_batch(self.text_error_log, problem_messages)

    def _append_text_log_batch(self, text_widget, messages: list[str]):
        if not messages:
            return
        append_text_log_messages(text_widget, messages, max_rows=EXCHANGE_LOG_MAX_ROWS, prepend_time=False)


class _ManualLogEntry:
    __slots__ = ("message", "is_problem")

    def __init__(self, message: str, *, problem: bool):
        self.message = str(message or "")
        self.is_problem = bool(problem)

    def __str__(self) -> str:
        return self.message
