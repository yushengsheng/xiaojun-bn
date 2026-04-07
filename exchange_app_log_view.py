#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import *  # noqa: F401,F403
from exchange_app_base import _shift_text_view_state_after_trim


class ExchangeAppLogViewMixin(object):
    def _fetch_public_ip(self, *, use_exchange_proxy: bool, allow_system_proxy: bool = True) -> str:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://ipinfo.io/ip",
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        proxies = self._requests_proxy_map() if use_exchange_proxy else None
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
    def _test_exchange_target_connectivity(*, proxies: dict[str, str] | None, allow_system_proxy: bool) -> str:
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
        return f"Binance /api/v3/time OK (serverTime={server_time})"
    def _test_exchange_proxy_once(
        self,
        *,
        include_exit_ip: bool = True,
        state: dict[str, object] | None = None,
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
        )
        if use_config_proxy and proxy_text:
            proxy_status = "SS代理已连接" if proxy_text.lower().startswith("ss://") else "代理已连接"
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=True, allow_system_proxy=False)
        elif system_proxy:
            proxy_status = "系统代理已连接"
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=True)
        else:
            proxy_status = "直连可用"
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
        return proxy_status, proxy_exit_ip, target
    def test_exchange_proxy(self):
        snapshot = self._exchange_proxy_state_snapshot()
        route_text = self._exchange_proxy_route_text_from_state(snapshot)

        def worker():
            test_ok = False
            save_err = ""
            try:
                status, exit_ip, target = self._test_exchange_proxy_once(state=snapshot)
                try:
                    self._save_exchange_proxy_config_only(state=snapshot)
                except Exception as e:
                    save_err = str(e)
                test_ok = True
                log_text = f"交易所代理测试成功：status={status}，exit_ip={exit_ip}，target={target}，route={route_text}"
                if save_err:
                    log_text = f"{log_text}，但保存配置失败：{save_err}"
                else:
                    log_text = f"{log_text}，已自动保存配置"
            except Exception as e:
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
                self.exchange_proxy_status_var.set(status)
                self.exchange_proxy_exit_ip_var.set(exit_ip)
                self._append_log(log_text)
                if not test_ok:
                    messagebox.showerror("代理测试失败", log_text)
                elif save_err:
                    messagebox.showwarning("代理测试成功", log_text)
                else:
                    messagebox.showinfo("代理测试成功", log_text)

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
                use_config_proxy = bool(snapshot.get("use_config_proxy"))
                proxy_status = "跟随系统代理" if self._system_proxy_map() and not use_config_proxy else "未启用"
                proxy_exit_ip = "--"
                try:
                    ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
                    if use_config_proxy:
                        proxy_status, proxy_exit_ip, _target = self._test_exchange_proxy_once(include_exit_ip=True, state=snapshot)
                    elif self._system_proxy_map():
                        proxy_status, proxy_exit_ip, _target = self._test_exchange_proxy_once(include_exit_ip=True, state=snapshot)
                    else:
                        proxy_exit_ip = ip
                except Exception as e:
                    ip = "获取失败: %s" % str(e)
                    if use_config_proxy:
                        proxy_status = "连接失败"
                    elif self._system_proxy_map():
                        proxy_status = "系统代理异常"
                    else:
                        proxy_status = "直连异常"

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
        pending_logs: list[str] = []
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                pending_logs.append(str(msg))
        if pending_logs:
            self._append_log_batch(pending_logs)
        try:
            self._log_poll_after_token = self.after(100, self._poll_log_queue)
        except Exception:
            self._log_poll_after_token = None
    def _append_log(self, msg: str):
        self._append_log_batch([msg])
    def _append_log_batch(self, messages: list[str]):
        if not messages:
            return
        follow_tail, view_state = capture_vertical_view_state(self.text_log)
        self.text_log.configure(state="normal")
        self.text_log.insert("end", "\n".join(str(msg) for msg in messages) + "\n")
        trimmed_lines = 0
        try:
            total_lines = max(0, int(str(self.text_log.index("end-1c")).split(".", 1)[0]) - 1)
        except Exception:
            total_lines = 0
        if total_lines > EXCHANGE_LOG_MAX_ROWS:
            trimmed_lines = total_lines - EXCHANGE_LOG_MAX_ROWS
            try:
                self.text_log.delete("1.0", f"{trimmed_lines + 1}.0")
            except Exception:
                trimmed_lines = 0
        if follow_tail:
            self.text_log.see("end")
        else:
            restore_vertical_view_state(self.text_log, _shift_text_view_state_after_trim(view_state, trimmed_lines))
        self.text_log.configure(state="disabled")
