#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import re
from pathlib import Path
from tkinter import messagebox

from app_paths import RELAY_FAILED_EXPORT_FILE, RELAY_MANUAL_EXPORT_FILE
from onchain_diagnostics import append_onchain_runtime_log
from shared_utils import LOG_MAX_ROWS, append_log_row, queue_log_row, queue_ui_render

ONCHAIN_PROBLEM_LOG_KEYWORDS = (
    "失败",
    "异常",
    "错误",
    "报错",
    "不足",
    "超时",
    "拒绝",
    "无效",
    "未确认",
    "Traceback",
    "Exception",
    "Client Error",
    "RPC",
    "revert",
)


class OnchainLoggingMixin(object):
    @staticmethod
    def _short_contract(contract: str) -> str:
        s = contract.strip()
        if len(s) <= 12:
            return s
        return f"{s[:8]}...{s[-6:]}"
    def _open_export_file(self, path: Path, label: str) -> None:
        target = Path(path)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.touch()
            if hasattr(os, "startfile"):
                os.startfile(str(target))
                self.log(f"{label}：已打开 {target.name}")
                return
            messagebox.showinfo("文件路径", f"{label}：{target}")
        except Exception as exc:
            messagebox.showerror("打开失败", f"{label} 打开失败：{exc}")
    def open_relay_manual_export_file(self) -> None:
        self._open_export_file(RELAY_MANUAL_EXPORT_FILE, "待人工处理文件")
    def open_relay_failed_export_file(self) -> None:
        self._open_export_file(RELAY_FAILED_EXPORT_FILE, "失败账号文件")
    @staticmethod
    def _is_problem_log_text(text: str) -> bool:
        message = str(text or "")
        if any(keyword in message for keyword in ONCHAIN_PROBLEM_LOG_KEYWORDS if keyword != "失败"):
            return True
        if re.search(r"失败\s*[1-9]\d*", message):
            return True
        return any(marker in message for marker in ("失败：", "失败:", "=失败", "失败，"))
    def _append_error_log_row(self, text: str) -> None:
        error_log_text = getattr(self, "error_log_text", None) or getattr(self, "error_log_tree", None)
        if error_log_text is None:
            return
        append_log_row(error_log_text, text, max_rows=LOG_MAX_ROWS)
    def log(self, text: str):
        append_onchain_runtime_log(text)
        queue_log_row(self, self.log_text, text, root=getattr(self, "root", None), max_rows=LOG_MAX_ROWS)
        if self._is_problem_log_text(text):
            queue_ui_render(
                self,
                lambda t=str(text or ""): self._append_error_log_row(t),
                root=getattr(self, "root", None),
            )
    def test_onchain_proxy(self):
        snapshot = self._onchain_proxy_state_snapshot()
        selected_network = str(self.network_var.get() or "").strip().upper()

        def worker():
            test_ok = False
            route_text = ""
            try:
                self._refresh_onchain_system_proxy_cache(background=False)
                route_text = self._onchain_proxy_route_text(state=snapshot)
                status, exit_ip, target = self._test_onchain_proxy_once(state=snapshot, selected_network=selected_network)
                test_ok = True
                log_text = f"链上代理测试成功：status={status}，exit_ip={exit_ip}，target={target}，route={route_text}"
            except Exception as exc:
                use_config_proxy = bool(snapshot.get("use_config_proxy"))
                raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
                has_config_proxy = bool(use_config_proxy and raw_proxy)
                if has_config_proxy:
                    status = "连接失败"
                elif (not has_config_proxy) and self._onchain_system_proxy_map():
                    status = "系统代理异常"
                else:
                    status = "直连异常"
                exit_ip = "--"
                if not route_text:
                    route_text = self._onchain_proxy_route_text(state=snapshot)
                log_text = f"链上代理测试失败：{exc}，route={route_text}"

            def _update():
                self.onchain_proxy_status_var.set(status)
                self.onchain_proxy_exit_ip_var.set(exit_ip)
                self.log(log_text)
                if test_ok:
                    messagebox.showinfo("链上代理测试成功", log_text)
                else:
                    messagebox.showerror("链上代理测试失败", log_text)

            self._dispatch_ui(_update)

        self._start_managed_thread(worker, name="onchain-proxy-test")


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
