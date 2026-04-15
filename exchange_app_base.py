#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import importlib
import ipaddress
import json
import os
import platform
import queue
import random
import re
import socket
import sys
import threading
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from api_clients import EvmClient
from app_paths import (
    CONFIG_BACKUP_SUFFIX,
    DATA_FILE,
    EXCHANGE_PROXY_CONFIG_FILE,
    STRATEGY_CONFIG_FILE,
    TOTAL_ASSET_RESULT_FILE,
    WITHDRAW_SUCCESS_FILE,
)
from core_models import AccountEntry
from exchange_binance_client import BinanceClient, summarize_exchange_exception
from exchange_constants import *
from exchange_logging import EXCHANGE_LOG_MAX_ROWS, log_queue, logger
from exchange_proxy_runtime import ExchangeProxyRuntime, http_get_via_proxy
from exchange_strategy import CombinedStopEvent, Strategy
from secret_box import SECRET_BOX
from shared_utils import (
    SolidButton,
    capture_vertical_view_state,
    dispatch_ui_callback,
    make_scrollbar,
    restore_vertical_view_state,
    schedule_ui_callback,
    start_ui_bridge,
    stop_ui_bridge,
)
from stores import AccountStore, _atomic_write_text, _atomic_write_text_with_backup, _load_json_with_backup

_ONCHAIN_IMPORT_ERROR = None
_ONCHAIN_PAGE_CLASS = None


def _load_onchain_page_class():
    global _ONCHAIN_PAGE_CLASS, _ONCHAIN_IMPORT_ERROR
    if _ONCHAIN_PAGE_CLASS is not None:
        return _ONCHAIN_PAGE_CLASS
    try:
        module = importlib.import_module('page_onchain')
        _ONCHAIN_PAGE_CLASS = getattr(module, 'OnchainTransferPage')
        _ONCHAIN_IMPORT_ERROR = None
    except Exception as exc:
        _ONCHAIN_IMPORT_ERROR = exc
        _ONCHAIN_PAGE_CLASS = None
    return _ONCHAIN_PAGE_CLASS


def _json_dump_text(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)
def _read_text_snapshot(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")
def _restore_text_snapshot(path: Path, snapshot: str | None) -> None:
    if snapshot is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        try:
            path.with_name(f"{path.name}{CONFIG_BACKUP_SUFFIX}").unlink()
        except FileNotFoundError:
            pass
        return
    _atomic_write_text_with_backup(path, snapshot, encoding="utf-8")
def _atomic_write_json(path: Path, payload: object) -> None:
    _atomic_write_text(path, _json_dump_text(payload), encoding="utf-8")
def _atomic_write_config_json(path: Path, payload: object) -> None:
    _atomic_write_text_with_backup(path, _json_dump_text(payload), encoding="utf-8")
def _require_dict_payload(raw: object) -> None:
    if isinstance(raw, dict):
        return
    raise RuntimeError("配置文件结构无效")
def _shift_text_view_state_after_trim(state: tuple[str, object] | None, trimmed_lines: int) -> tuple[str, object] | None:
    if trimmed_lines <= 0 or state is None:
        return state
    kind, payload = state
    if kind != "text":
        return state
    index_text = str(payload or "").strip()
    if not index_text:
        return state
    line_text, dot, col_text = index_text.partition(".")
    try:
        new_line = max(1, int(line_text) - int(trimmed_lines))
    except Exception:
        return state
    return kind, f"{new_line}{dot or '.'}{col_text or '0'}"

class ExchangeAppBase(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Binance 自动交易机器人（增强版 GUI）")
        self.geometry("1320x920")

        self.client = None
        self.worker_thread = None
        self.stop_event = None
        self._batch_task_active = False
        self._closing = False
        self._close_finalized = False
        self._close_deadline_monotonic = 0.0
        self._close_wait_after_token = None
        self._log_poll_after_token = None
        self._accounts_save_after_token = None
        self._update_ip_after_token = None
        self._ip_refresh_inflight = False
        self._ip_refresh_lock = threading.Lock()
        self._result_file_lock = threading.Lock()
        self._managed_threads_lock = threading.Lock()
        self._managed_threads: set[threading.Thread] = set()
        self._exchange_proxy_state_lock = threading.Lock()
        self._exchange_proxy_state = {
            "use_config_proxy": bool(EXCHANGE_USE_CONFIG_PROXY_DEFAULT),
            "raw_proxy": str(EXCHANGE_PROXY_DEFAULT or "").strip(),
        }
        self._loading_accounts = False
        self.exchange_proxy_runtime = ExchangeProxyRuntime(STRATEGY_CONFIG_FILE.parent, runtime_name="exchange")
        self.onchain_proxy_runtime = ExchangeProxyRuntime(STRATEGY_CONFIG_FILE.parent, runtime_name="onchain")
        self.account_store = AccountStore(DATA_FILE)

        self.accounts = []
        self.total_asset_results = {}

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build_ui()
        self._sync_exchange_proxy_state()
        start_ui_bridge(self, root=self)
        self._load_strategy_config()
        self._load_exchange_proxy_config()
        self._load_accounts()
        self._log_poll_after_token = self.after(100, self._poll_log_queue)
        self.update_ip()
    def _build_ui(self):
        self.api_key_var = tk.StringVar(value=API_KEY_DEFAULT)
        self.api_secret_var = tk.StringVar(value=API_SECRET_DEFAULT)
        self.exchange_proxy_var = tk.StringVar(value=EXCHANGE_PROXY_DEFAULT)
        self.use_exchange_config_proxy_var = tk.BooleanVar(value=EXCHANGE_USE_CONFIG_PROXY_DEFAULT)
        self.exchange_proxy_var.trace_add("write", self._on_exchange_proxy_config_changed)
        self.use_exchange_config_proxy_var.trace_add("write", self._on_exchange_proxy_config_changed)
        self.trade_account_type_var = tk.StringVar(value=TRADE_ACCOUNT_TYPE_DEFAULT)
        self.spot_rounds_var = tk.IntVar(value=SPOT_ROUNDS_DEFAULT)
        self.trade_mode_var = tk.StringVar(value=TRADE_MODE_DEFAULT)
        self.premium_delta_var = tk.StringVar(value=PREMIUM_DELTA_DEFAULT)
        self.premium_order_count_var = tk.IntVar(value=PREMIUM_ORDER_COUNT_DEFAULT)
        self.premium_append_threshold_var = tk.StringVar(value=PREMIUM_APPEND_THRESHOLD_DEFAULT)
        self.bnb_fee_stop_var = tk.StringVar(value=BNB_FEE_STOP_DEFAULT)
        self.bnb_topup_amount_var = tk.StringVar(value=BNB_TOPUP_AMOUNT_DEFAULT)
        self.reprice_threshold_var = tk.StringVar(value=REPRICE_THRESHOLD_DEFAULT)
        self.spot_symbol_var = tk.StringVar(value=SPOT_SYMBOL_DEFAULT)
        self.futures_symbol_var = tk.StringVar(value=FUTURES_SYMBOL_DEFAULT)
        self.futures_rounds_var = tk.IntVar(value=FUTURES_ROUNDS_DEFAULT)
        self.futures_amount_var = tk.StringVar(value=FUTURES_AMOUNT_DEFAULT)
        self.futures_leverage_var = tk.IntVar(value=FUTURES_LEVERAGE_DEFAULT)
        self.futures_margin_type_var = tk.StringVar(value=FUTURES_MARGIN_TYPE_LABEL_CROSSED)
        self.futures_side_var = tk.StringVar(value=FUTURES_SIDE_DEFAULT)

        self.withdraw_addr_var = tk.StringVar(value=WITHDRAW_ADDRESS_DEFAULT)
        self.withdraw_net_var = tk.StringVar(value=WITHDRAW_NETWORK_DEFAULT)
        self.withdraw_coin_var = tk.StringVar(value=WITHDRAW_COIN_DEFAULT)
        self.withdraw_buffer_var = tk.DoubleVar(value=WITHDRAW_FEE_BUFFER_DEFAULT)
        self.enable_withdraw_var = tk.BooleanVar(value=True)

        self.min_delay_var = tk.StringVar(value="")
        self.max_delay_var = tk.StringVar(value="")
        self.usdt_timeout_var = tk.IntVar(value=30)
        self.ip_var = tk.StringVar(value="获取中...")
        self.exchange_proxy_status_var = tk.StringVar(value="未启用")
        self.exchange_proxy_exit_ip_var = tk.StringVar(value="--")
        self.top_proxy_name_var = tk.StringVar(value="交易所代理:")
        self.top_proxy_test_btn_text_var = tk.StringVar(value="测试交易所代理")
        self._current_main_page = "exchange"

        self.main_tabs = None

        top_bar = ttk.Frame(self)
        top_bar.pack(fill="x", padx=8, pady=(8, 0))
        top_bar.columnconfigure(1, weight=1)

        tab_bar = ttk.Frame(top_bar)
        tab_bar.grid(row=0, column=0, sticky="w")
        self.btn_exchange_tab = tk.Button(
            tab_bar,
            text="交易所批量",
            command=lambda: self._show_main_page("exchange"),
            bd=1,
            relief="sunken",
            padx=14,
            pady=5,
            highlightthickness=0,
            cursor="hand2",
        )
        self.btn_exchange_tab.pack(side="left")
        self.btn_onchain_tab = tk.Button(
            tab_bar,
            text="链上",
            command=lambda: self._show_main_page("onchain"),
            bd=1,
            relief="raised",
            padx=14,
            pady=5,
            highlightthickness=0,
            cursor="hand2",
        )
        self.btn_onchain_tab.pack(side="left", padx=(6, 0))

        proxy_bar = ttk.Frame(top_bar)
        proxy_bar.grid(row=0, column=1, sticky="e")
        ttk.Label(proxy_bar, text="本机直连 IP:").grid(row=0, column=0, sticky="e")
        ttk.Label(proxy_bar, textvariable=self.ip_var).grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.lbl_top_proxy_name = ttk.Label(proxy_bar, textvariable=self.top_proxy_name_var)
        self.lbl_top_proxy_name.grid(row=0, column=2, sticky="e")
        self.ent_top_proxy = ttk.Entry(proxy_bar, textvariable=self.exchange_proxy_var, width=24)
        self.ent_top_proxy.grid(row=0, column=3, sticky="w", padx=(4, 6))
        self.btn_top_proxy_test = ttk.Button(proxy_bar, textvariable=self.top_proxy_test_btn_text_var, command=self.test_exchange_proxy)
        self.btn_top_proxy_test.grid(row=0, column=4, sticky="w", padx=(0, 12))
        ttk.Label(proxy_bar, text="状态:").grid(row=0, column=5, sticky="e")
        self.lbl_top_proxy_status = ttk.Label(proxy_bar, textvariable=self.exchange_proxy_status_var)
        self.lbl_top_proxy_status.grid(row=0, column=6, sticky="w", padx=(4, 12))
        ttk.Label(proxy_bar, text="出口 IP:").grid(row=0, column=7, sticky="e")
        self.lbl_top_proxy_exit_ip = ttk.Label(proxy_bar, textvariable=self.exchange_proxy_exit_ip_var)
        self.lbl_top_proxy_exit_ip.grid(row=0, column=8, sticky="w", padx=(4, 0))

        self.main_content = ttk.Frame(self)
        self.main_content.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.exchange_tab = ttk.Frame(self.main_content)
        self.onchain_tab = ttk.Frame(self.main_content)
        self._refresh_main_page_tab_buttons()
        self._show_main_page(self._current_main_page)

        frame_top = ttk.LabelFrame(self.exchange_tab, text="策略配置（单账号 & 批量共享）")
        frame_top.pack(fill="x", padx=10, pady=5)

        frame_mid = ttk.LabelFrame(self.exchange_tab, text="状态控制")
        frame_mid.pack(fill="x", padx=10, pady=5)
        self.status_var = tk.StringVar(value="状态：空闲")
        self.single_account_balances_var = tk.StringVar(value="--")
        self.exchange_strategy_frame = frame_top
        self._rebuild_exchange_panels(frame_top, frame_mid)

        frame_acc = ttk.LabelFrame(self.exchange_tab, text="账号列表管理（批量 API + 提现地址）")
        frame_acc.pack(fill="both", expand=True, padx=10, pady=5)

        self.acc_api_key_var = tk.StringVar()
        self.acc_api_secret_var = tk.StringVar()
        self.acc_withdraw_addr_var = tk.StringVar()
        self.acc_network_var = self.withdraw_net_var

        self.withdraw_net_var.trace_add("write", self._on_global_network_changed)
        self.max_threads_var = tk.IntVar(value=MAX_THREADS_DEFAULT)

        self.frame_list_canvas = ttk.Frame(frame_acc)
        self.frame_list_canvas.pack(fill="both", expand=True, padx=5, pady=2)
        self.frame_list_canvas.columnconfigure(0, weight=1)
        self.frame_list_canvas.rowconfigure(0, weight=1)

        tree_cols = ("checked", "idx", "api_key", "address", "network", "status")
        try:
            account_tree_style = ttk.Style(self)
            account_tree_style.map(
                "ExchangeAccounts.Treeview",
                background=[("selected", "#0A64AD")],
                foreground=[("selected", "#FFFFFF")],
            )
        except Exception:
            pass
        self.account_tree = ttk.Treeview(
            self.frame_list_canvas,
            columns=tree_cols,
            show="headings",
            selectmode="browse",
            height=9,
            style="ExchangeAccounts.Treeview",
        )
        self._account_tree_row_to_account = {}
        self.account_tree.heading("checked", text="勾选")
        self.account_tree.heading("idx", text="编号")
        self.account_tree.heading("api_key", text="API KEY")
        self.account_tree.heading("address", text="提现地址")
        self.account_tree.heading("network", text="网络")
        self.account_tree.heading("status", text="状态")
        self.account_tree.column("checked", width=52, minwidth=52, stretch=False, anchor="center")
        self.account_tree.column("idx", width=52, minwidth=52, stretch=False, anchor="center")
        self.account_tree.column("api_key", width=220, minwidth=180, anchor="w")
        self.account_tree.column("address", width=330, minwidth=260, anchor="w")
        self.account_tree.column("network", width=76, minwidth=68, stretch=False, anchor="center")
        self.account_tree.column("status", width=500, minwidth=240, anchor="w")
        self.account_tree.tag_configure("acc_ready", foreground="#111111", background="#F2F2F2")
        self.account_tree.tag_configure("acc_running", foreground="#111111", background="#CFE3FF")
        self.account_tree.tag_configure("acc_warn", foreground="#111111", background="#FFE3B8")
        self.account_tree.tag_configure("acc_failed", foreground="#C62828", background="#FDECEC")
        self.account_tree.tag_configure("acc_success", foreground="#1E8449", background="#E8F5E9")
        self.account_tree.tag_configure("acc_context", foreground="#FFFFFF", background="#0A64AD")

        self.account_tree_ybar = make_scrollbar(self.frame_list_canvas, orient="vertical", command=self.account_tree.yview)
        self.account_tree_xbar = make_scrollbar(self.frame_list_canvas, orient="horizontal", command=self.account_tree.xview)
        self.account_tree.configure(yscrollcommand=self.account_tree_ybar.set, xscrollcommand=self.account_tree_xbar.set)
        self.account_tree.grid(row=0, column=0, sticky="nsew")
        self.account_tree_ybar.grid(row=0, column=1, sticky="ns")
        self.account_tree_xbar.grid(row=1, column=0, sticky="ew")
        self.account_list_hint = ttk.Label(
            self.frame_list_canvas,
            text="账号列表为空。点击此区域后可直接 Ctrl+V / Cmd+V 粘贴导入账号。\n导入格式：每 3 段一组，依次为 API KEY / SECRET / 提现地址。",
            foreground="#666",
            justify="center",
            anchor="center",
        )
        self.account_tree.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.account_tree.bind("<<TreeviewSelect>>", self._on_account_tree_selection_changed, add="+")
        self.account_tree.bind("<Double-Button-1>", self._on_account_tree_double_click, add="+")
        self.account_tree.bind("<Button-2>", self._on_account_tree_right_click, add="+")
        self.account_tree.bind("<Button-3>", self._on_account_tree_right_click, add="+")
        self.account_tree.bind("<Control-Button-1>", self._on_account_tree_right_click, add="+")
        self.frame_list_canvas.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.account_list_hint.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self._refresh_account_list_hint()
        self.account_row_menu = tk.Menu(self, tearoff=0)
        self.account_row_menu.add_command(label="查询", command=self.run_context_account_query)
        self.account_row_menu.add_command(label="执行", command=self.run_context_account_execute)
        self.account_row_menu.add_command(label="停止", command=self.run_context_account_stop)
        self.account_row_menu.add_command(label="提现", command=self.run_context_account_withdraw)
        self.account_row_menu.add_command(label="归集BNB", command=self.run_context_account_collect_bnb)
        self._context_account = None
        self._setup_account_list_mousewheel_bindings()

        frame_batch_ctrl = ttk.Frame(frame_acc)
        frame_batch_ctrl.pack(fill="x", padx=5, pady=5)

        self.btn_toggle_select_accounts = ttk.Button(frame_batch_ctrl, text="全选", width=8, command=self.toggle_select_all_accounts)
        self.btn_toggle_select_accounts.pack(side="left", padx=(0, 5))
        self.btn_run_accounts = SolidButton(
            frame_batch_ctrl,
            text="批量执行",
            command=self.run_selected_accounts,
            bg="#1E8449",
            fg="#FFFFFF",
            activebackground="#186A3B",
            activeforeground="#FFFFFF",
            disabledforeground="#E8F5E9",
            relief="flat",
            padx=12,
            pady=2,
        )
        self.btn_run_accounts.pack(side="left", padx=5)
        self.btn_query_all_assets = ttk.Button(frame_batch_ctrl, text="查询全部总资产", command=self.run_query_total_assets_for_all_accounts)
        self.btn_query_all_assets.pack(side="left", padx=5)
        self.btn_batch_withdraw = ttk.Button(frame_batch_ctrl, text="批量提现", command=self.batch_manual_withdraw)
        self.btn_batch_withdraw.pack(side="left", padx=5)
        self.btn_collect_bnb_combo = ttk.Button(frame_batch_ctrl, text="归集并买BNB", command=self.run_batch_collect_bnb_with_confirm)
        self.btn_collect_bnb_combo.pack(side="left", padx=5)

        self.btn_del_accounts = ttk.Button(frame_batch_ctrl, text="删除选中", command=self.delete_selected_accounts)
        self.btn_del_accounts.pack(side="left", padx=5)

        self.btn_paste_accounts = ttk.Button(frame_batch_ctrl, text="粘贴导入", command=self.import_accounts_from_clipboard)
        self.btn_paste_accounts.pack(side="left", padx=5)
        ttk.Label(frame_batch_ctrl, text="绾跨▼鏁?").pack(side="left", padx=(10, 2))
        ttk.Spinbox(frame_batch_ctrl, from_=1, to=50, textvariable=self.max_threads_var, width=3).pack(side="left", padx=2)
        try:
            frame_batch_ctrl.winfo_children()[-2].configure(text="\u7ebf\u7a0b\u6570:")
        except Exception:
            pass

        self.skip_usdt_wait_in_batch_var = tk.BooleanVar(value=False)
        self._current_batch_summary = None
        self._last_batch_retry = None
        self._batch_summary_lock = threading.Lock()
        self.exchange_batch_summary_var = tk.StringVar(value="结果汇总：成功0 | 失败0 | 提现总额=- | 余额总额=-")

        frame_batch_opts = ttk.Frame(frame_acc)
        frame_batch_opts.pack(fill="x", padx=5, pady=(0, 5))

        self.btn_export_accounts = ttk.Button(frame_batch_opts, text="导出", command=self.export_accounts)
        self.btn_export_accounts.pack(side="left", padx=(0, 5))

        self.btn_import_accounts = ttk.Button(frame_batch_opts, text="导入", command=self.import_accounts)
        self.btn_import_accounts.pack(side="left", padx=5)

        self.btn_export_asset_csv = ttk.Button(frame_batch_opts, text="导出总资产CSV", command=self.export_total_asset_csv)
        self.btn_export_asset_csv.pack(side="left", padx=5)

        ttk.Checkbutton(
            frame_batch_opts,
            text="批量策略跳过USDT检测",
            variable=self.skip_usdt_wait_in_batch_var
        ).pack(side="left", padx=(12, 0))
        self.btn_retry_failed_accounts = ttk.Button(
            frame_batch_opts,
            text="失败重试",
            command=self.retry_last_failed_batch_operation,
            state="disabled",
        )
        self.btn_retry_failed_accounts.pack(side="left", padx=(8, 0))
        ttk.Label(frame_batch_opts, textvariable=self.exchange_batch_summary_var, foreground="#666666").pack(side="left", padx=(12, 0))

        frame_log = ttk.LabelFrame(self.exchange_tab, text="运行日志")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.text_log = tk.Text(frame_log, wrap="word", height=10, state="disabled")
        self.text_log.pack(fill="both", expand=True, side="left")

        scrollbar_log = ttk.Scrollbar(frame_log, command=self.text_log.yview)
        scrollbar_log.pack(side="right", fill="y")
        self.text_log["yscrollcommand"] = scrollbar_log.set

        onchain_shell = ttk.Frame(self.onchain_tab)
        onchain_shell.pack(fill="both", expand=True)
        onchain_intro = ttk.LabelFrame(onchain_shell, text="链上模块")
        onchain_intro.pack(fill="x", padx=10, pady=(8, 6))
        ttk.Label(onchain_intro, text="该页面为独立链上批量转账模块，与交易所批量页面配置互不影响。", foreground="#666").pack(
            anchor="w", padx=8, pady=(6, 6)
        )

        onchain_intro.pack_forget()
        self.onchain_page = None
        self._onchain_body = ttk.Frame(onchain_shell)
        self._onchain_body.pack(fill="both", expand=True, padx=2, pady=2)
        self._show_onchain_loading_hint()
        self._refresh_top_proxy_binding()

        # 快捷键：Ctrl+V / Cmd+V 直接触发“从剪贴板导入账号”
        self.bind_all("<Control-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Control-V>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-V>", self._on_paste_shortcut, add="+")
    def _refresh_main_page_tab_buttons(self):
        tabs = (
            (self.btn_exchange_tab, "exchange"),
            (self.btn_onchain_tab, "onchain"),
        )
        for btn, page_name in tabs:
            is_active = self._current_main_page == page_name
            btn.configure(
                relief="sunken" if is_active else "raised",
                bg="#ffffff" if is_active else "#e9e9e9",
                fg="#111111" if is_active else "#555555",
                activebackground="#ffffff" if is_active else "#f1f1f1",
                activeforeground="#111111",
            )
    def _refresh_top_proxy_binding(self):
        page = self._current_main_page
        if page == "onchain" and getattr(self, "onchain_page", None) is not None:
            self.top_proxy_name_var.set("链上RPC代理:")
            self.top_proxy_test_btn_text_var.set("测试链上代理")
            self.ent_top_proxy.configure(textvariable=self.onchain_page.onchain_proxy_var)
            self.btn_top_proxy_test.configure(command=self.onchain_page.test_onchain_proxy, state="normal")
            self.lbl_top_proxy_status.configure(textvariable=self.onchain_page.onchain_proxy_status_var)
            self.lbl_top_proxy_exit_ip.configure(textvariable=self.onchain_page.onchain_proxy_exit_ip_var)
            return
        self.top_proxy_name_var.set("交易所代理:")
        self.top_proxy_test_btn_text_var.set("测试交易所代理")
        self.ent_top_proxy.configure(textvariable=self.exchange_proxy_var)
        self.btn_top_proxy_test.configure(command=self.test_exchange_proxy, state="normal")
        self.lbl_top_proxy_status.configure(textvariable=self.exchange_proxy_status_var)
        self.lbl_top_proxy_exit_ip.configure(textvariable=self.exchange_proxy_exit_ip_var)
    def _show_onchain_loading_hint(self) -> None:
        body = getattr(self, "_onchain_body", None)
        if body is None:
            return
        self._clear_container_children(body)
        hint_box = ttk.LabelFrame(body, text="链上模块")
        hint_box.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(hint_box, text="链上页面将在切换到该页签时加载。", foreground="#666").pack(anchor="w", padx=8, pady=(10, 4))
        ttk.Label(hint_box, text="首次进入时会按需导入链上依赖和页面组件。", foreground="#666").pack(anchor="w", padx=8, pady=(0, 10))
    def _show_onchain_load_failure(self, detail: object) -> None:
        body = getattr(self, "_onchain_body", None)
        if body is None:
            return
        self._clear_container_children(body)
        fail_box = ttk.LabelFrame(body, text="链上模块加载失败")
        fail_box.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(fail_box, text=f"链上页面加载失败：{detail}").pack(anchor="w", padx=(8, 8), pady=(8, 4))
        ttk.Label(fail_box, text="请检查运行依赖：eth-account、eth-utils").pack(anchor="w", padx=(8, 8), pady=(0, 8))
    def _ensure_onchain_page_loaded(self):
        page = getattr(self, "onchain_page", None)
        if page is not None:
            return page
        body = getattr(self, "_onchain_body", None)
        if body is None:
            return None
        onchain_page_cls = _load_onchain_page_class()
        if onchain_page_cls is None:
            self._show_onchain_load_failure(_ONCHAIN_IMPORT_ERROR)
            return None
        self._clear_container_children(body)
        try:
            page = onchain_page_cls(
                body,
                rpc_proxy_getter=self._get_onchain_proxy_url,
                proxy_text_normalizer=self._normalize_proxy_text,
            )
        except Exception as exc:
            logger.exception("链上页面初始化失败: %s", exc)
            self.onchain_page = None
            self._show_onchain_load_failure(exc)
            return None
        self.onchain_page = page
        return page
    def _show_main_page(self, page_name: str):
        target = self.exchange_tab if page_name == "exchange" else self.onchain_tab
        if self._current_main_page != page_name:
            self._current_main_page = page_name
        if page_name == "onchain":
            self._ensure_onchain_page_loaded()
        try:
            self.exchange_tab.pack_forget()
        except Exception:
            pass
        try:
            self.onchain_tab.pack_forget()
        except Exception:
            pass
        target.pack(fill="both", expand=True)
        self._refresh_main_page_tab_buttons()
        self._refresh_top_proxy_binding()
    @staticmethod
    def _clear_container_children(container):
        for child in list(container.winfo_children()):
            try:
                child.destroy()
            except Exception:
                pass
    def _rebuild_exchange_panels(self, frame_top, frame_mid):
        self._rebuild_exchange_strategy_panel(frame_top)
        self._rebuild_exchange_single_panel(frame_mid)
    def _rebuild_exchange_single_panel(self, frame_mid):
        self._clear_container_children(frame_mid)
        try:
            frame_mid.configure(text="状态控制")
        except Exception:
            pass

        frame_mid.columnconfigure(0, weight=0)
        frame_mid.columnconfigure(1, weight=0)

        left = ttk.Frame(frame_mid)
        left.grid(row=0, column=0, sticky="nw", padx=(0, 0), pady=4)
        left.columnconfigure(0, weight=0)
        left.columnconfigure(1, weight=0)

        right = ttk.Frame(frame_mid)
        right.grid(row=0, column=1, sticky="nw", padx=(16, 0), pady=4)
        self._single_panel_right = right
        right.columnconfigure(1, weight=1)
        right.columnconfigure(2, weight=0)

        self.btn_start = ttk.Button(right, text="开始运行（当前 API）", command=self.start_bot)
        self.btn_stop = ttk.Button(left, text="停止运行", command=self.stop_bot, state="disabled")
        self.btn_refresh = ttk.Button(right, text="刷新余额（当前 API）", command=self.refresh_balances)
        self.btn_withdraw = ttk.Button(right, text="手动提现", command=self.manual_withdraw)

        self.btn_stop.grid(row=0, column=0, padx=5, pady=5, sticky="w")

        self.progress = ttk.Progressbar(left, orient="horizontal", mode="determinate")
        self.progress.grid(row=1, column=0, columnspan=2, sticky="w", padx=5, pady=5)
        ttk.Label(left, textvariable=self.status_var).grid(row=2, column=0, columnspan=2, sticky="w", padx=5, pady=2)

        ttk.Label(right, text="API KEY:").grid(row=0, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.api_key_var, width=36).grid(row=0, column=1, sticky="ew", padx=5, pady=3)
        self.btn_start.grid(row=0, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(right, text="API SECRET:").grid(row=1, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.api_secret_var, width=36, show="*").grid(row=1, column=1, sticky="ew", padx=5, pady=3)
        self.btn_refresh.grid(row=1, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(right, text="提现地址:").grid(row=2, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.withdraw_addr_var, width=36).grid(row=2, column=1, sticky="ew", padx=5, pady=3)
        self.btn_withdraw.grid(row=2, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(
            right,
            textvariable=self.single_account_balances_var,
            justify="left",
            anchor="w",
            wraplength=760,
        ).grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=(4, 2))

        self.after_idle(self._align_single_status_panel)
    @staticmethod
    def _normalized_entry_text(value, *placeholders):
        text = str(value or "").strip()
        return "" if text in set(placeholders) else text
    def _delay_var_value(self, variable, default: int, placeholder: str) -> int:
        text = self._normalized_entry_text(variable.get(), placeholder)
        if not text:
            return int(default)
        return int(text)
    @staticmethod
    def _normalize_trade_account_type(value) -> str:
        text = str(value or "").strip()
        return text if text in TRADE_ACCOUNT_TYPE_OPTIONS else TRADE_ACCOUNT_TYPE_DEFAULT
    @staticmethod
    def _normalize_trade_mode(value) -> str:
        text = str(value or "").strip()
        return text if text in TRADE_MODE_OPTIONS else TRADE_MODE_DEFAULT
    @staticmethod
    def _normalize_futures_margin_type(value) -> str:
        text = str(value or "").strip()
        if not text:
            return FUTURES_MARGIN_TYPE_DEFAULT
        upper_text = text.upper()
        if upper_text in FUTURES_MARGIN_TYPE_OPTIONS:
            return upper_text
        return FUTURES_MARGIN_TYPE_LABEL_TO_VALUE.get(text, FUTURES_MARGIN_TYPE_DEFAULT)
    @staticmethod
    def _futures_margin_type_label(value) -> str:
        normalized = ExchangeAppBase._normalize_futures_margin_type(value)
        return FUTURES_MARGIN_TYPE_VALUE_TO_LABEL.get(normalized, FUTURES_MARGIN_TYPE_LABEL_CROSSED)
    def _refresh_strategy_panel_layout(self):
        frame_top = getattr(self, "exchange_strategy_frame", None)
        if frame_top is not None:
            self._rebuild_exchange_strategy_panel(frame_top)
    def _align_trade_mode_sections(self):
        row2 = getattr(self, "_strategy_row2", None)
        row3 = getattr(self, "_strategy_row3", None)
        row2_left = getattr(self, "_strategy_row2_left", None)
        row3_left = getattr(self, "_strategy_row3_left", None)
        row3_right = getattr(self, "_strategy_row3_right", None)
        btn = getattr(self, "btn_save_strategy_config", None)
        if row2 is None or row3 is None or row2_left is None or row3_left is None or btn is None:
            return
        try:
            btn.update_idletasks()
            row2.update_idletasks()
            row3.update_idletasks()
            if row3_right is not None:
                row3_right.update_idletasks()
            row2_target = max(
                row2_left.winfo_reqwidth(),
                (btn.winfo_rootx() + btn.winfo_width()) - row2.winfo_rootx() + 12,
            )
            btn_end = btn.winfo_rootx() + btn.winfo_width()
            row3_right_width = row3_right.winfo_reqwidth() if row3_right is not None else 0
            row3_target = max(
                row3_left.winfo_reqwidth(),
                btn_end - row3.winfo_rootx() - row3_right_width,
            )
            row2.grid_columnconfigure(0, minsize=int(row2_target), weight=0)
            row3.grid_columnconfigure(0, minsize=int(row3_target), weight=0)
        except Exception:
            pass
    def _align_single_status_panel(self):
        stop_btn = getattr(self, "btn_stop", None)
        progress = getattr(self, "progress", None)
        right_frame = getattr(self, "_single_panel_right", None)
        if stop_btn is None or progress is None or right_frame is None:
            return
        try:
            stop_btn.update_idletasks()
            btn_width = max(1, stop_btn.winfo_width())
            progress.configure(length=btn_width * 3)
            right_frame.grid_configure(padx=(btn_width, 0))
        except Exception:
            pass
    def _on_trade_mode_changed(self, _event=None):
        self._refresh_strategy_panel_layout()
    @staticmethod
    def _reprice_threshold_label_text() -> str:
        return "重挂阈值:"
    @staticmethod
    def _decimal_field_value(raw_value, field_label: str, *, min_value: Decimal | str | int | float = Decimal("0")) -> Decimal:
        text = str(raw_value or "").strip()
        if not text:
            raise RuntimeError(f"{field_label}不能为空")
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"{field_label}格式不正确") from exc
        if value < Decimal(str(min_value)):
            raise RuntimeError(f"{field_label}不能小于 {min_value}")
        return value
    def _collect_trade_mode_settings(self) -> dict[str, object]:
        trade_account_type = self._normalize_trade_account_type(self.trade_account_type_var.get())
        mode = self._normalize_trade_mode(self.trade_mode_var.get())
        spot_symbol = str(self.spot_symbol_var.get() or "").strip().upper()
        try:
            stored_rounds = int(self.spot_rounds_var.get())
        except Exception:
            stored_rounds = SPOT_ROUNDS_DEFAULT
        try:
            stored_futures_rounds = int(self.futures_rounds_var.get())
        except Exception:
            stored_futures_rounds = FUTURES_ROUNDS_DEFAULT

        premium_text = str(self.premium_delta_var.get() or "").strip()
        premium_order_count_text = str(self.premium_order_count_var.get() or PREMIUM_ORDER_COUNT_DEFAULT).strip()
        premium_append_threshold_text = str(self.premium_append_threshold_var.get() or "").strip()
        fee_stop_text = str(self.bnb_fee_stop_var.get() or "").strip()
        bnb_topup_text = str(self.bnb_topup_amount_var.get() or "").strip()
        reprice_threshold_text = str(self.reprice_threshold_var.get() or "").strip()
        futures_symbol = str(self.futures_symbol_var.get() or "").strip().upper()
        futures_amount_text = str(self.futures_amount_var.get() or "").strip()
        futures_margin_type = self._normalize_futures_margin_type(self.futures_margin_type_var.get())
        futures_side = str(self.futures_side_var.get() or FUTURES_SIDE_DEFAULT).strip()
        premium_value: Decimal | None = None
        premium_order_count = PREMIUM_ORDER_COUNT_DEFAULT
        premium_append_threshold_value = Decimal(PREMIUM_APPEND_THRESHOLD_DEFAULT)
        fee_stop_value: Decimal | None = None
        bnb_topup_value = Decimal("0")
        reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
        futures_rounds = stored_futures_rounds if stored_futures_rounds > 0 else FUTURES_ROUNDS_DEFAULT
        futures_amount_value: Decimal | None = None
        try:
            futures_leverage = int(self.futures_leverage_var.get())
        except Exception:
            futures_leverage = FUTURES_LEVERAGE_DEFAULT

        if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            if not spot_symbol:
                raise RuntimeError("现货交易对不能为空")
            if not bnb_topup_text:
                bnb_topup_text = "0"
            if not reprice_threshold_text:
                reprice_threshold_text = REPRICE_THRESHOLD_DEFAULT
            bnb_topup_value = self._decimal_field_value(bnb_topup_text, "预买BNB金额", min_value=0)
            reprice_threshold_value = self._decimal_field_value(reprice_threshold_text, "重挂阈值", min_value=0)

            if mode in {TRADE_MODE_MARKET, TRADE_MODE_CONVERT}:
                try:
                    runtime_rounds = int(self.spot_rounds_var.get())
                except Exception as exc:
                    raise RuntimeError(f"{mode}模式下必须填写现货轮次") from exc
                if runtime_rounds < 1:
                    raise RuntimeError(f"{mode}模式下现货轮次必须大于等于 1")
            else:
                runtime_rounds = stored_rounds if stored_rounds > 0 else SPOT_ROUNDS_DEFAULT
                fee_stop_value = self._decimal_field_value(fee_stop_text, "剩余bnb手续费", min_value=0)
                if mode == TRADE_MODE_PREMIUM:
                    premium_value = self._decimal_field_value(premium_text, "溢价", min_value=0)
                    if not premium_append_threshold_text:
                        premium_append_threshold_text = PREMIUM_APPEND_THRESHOLD_DEFAULT
                    try:
                        premium_order_count = int(premium_order_count_text)
                    except Exception as exc:
                        raise RuntimeError("笔数格式不正确") from exc
                    if premium_order_count < 0:
                        raise RuntimeError("笔数不能小于 0")
                    premium_append_threshold_value = self._decimal_field_value(
                        premium_append_threshold_text,
                        "追加挂单",
                        min_value=0,
                    )
        else:
            runtime_rounds = stored_rounds if stored_rounds > 0 else SPOT_ROUNDS_DEFAULT
            try:
                futures_rounds = int(self.futures_rounds_var.get())
            except Exception as exc:
                raise RuntimeError("合约模式下必须填写轮次") from exc
            if futures_rounds < 1:
                raise RuntimeError("合约轮次必须大于等于 1")
            if not futures_symbol:
                raise RuntimeError("合约交易对不能为空")
            futures_amount_value = self._decimal_field_value(futures_amount_text, "合约下单金额", min_value=0)
            if futures_amount_value <= 0:
                raise RuntimeError("合约下单金额必须大于 0")
            try:
                futures_leverage = int(self.futures_leverage_var.get())
            except Exception as exc:
                raise RuntimeError("合约杠杆格式不正确") from exc
            if futures_leverage < 1 or futures_leverage > 125:
                raise RuntimeError("合约杠杆必须在 1-125 之间")
            futures_margin_type = self._normalize_futures_margin_type(futures_margin_type)
            if futures_side not in FUTURES_SIDE_OPTIONS:
                futures_side = FUTURES_SIDE_DEFAULT

        return {
            "trade_account_type": trade_account_type,
            "trade_mode": mode,
            "spot_rounds": runtime_rounds,
            "stored_spot_rounds": stored_rounds,
            "premium_delta": premium_text,
            "premium_delta_value": premium_value,
            "premium_order_count": premium_order_count,
            "premium_append_threshold": premium_append_threshold_text,
            "premium_append_threshold_value": premium_append_threshold_value,
            "bnb_fee_stop": fee_stop_text,
            "bnb_fee_stop_value": fee_stop_value,
            "bnb_topup_amount": bnb_topup_text,
            "bnb_topup_amount_value": bnb_topup_value,
            "reprice_threshold": reprice_threshold_text,
            "reprice_threshold_value": reprice_threshold_value,
            "futures_symbol": futures_symbol,
            "futures_rounds": futures_rounds,
            "stored_futures_rounds": stored_futures_rounds,
            "futures_amount": futures_amount_text,
            "futures_amount_value": futures_amount_value,
            "futures_leverage": futures_leverage,
            "futures_margin_type": futures_margin_type,
            "futures_side": futures_side,
        }
    def _install_entry_placeholder(self, entry, variable, placeholder: str):
        placeholder_color = "#8a8a8a"
        normal_color = entry.cget("fg")

        def set_normal():
            try:
                entry.configure(fg=normal_color)
            except Exception:
                pass

        def set_placeholder():
            if self.focus_get() is entry:
                return
            text = str(variable.get() or "").strip()
            if text and text != placeholder:
                set_normal()
                return
            variable.set(placeholder)
            try:
                entry.configure(fg=placeholder_color)
            except Exception:
                pass

        def clear_placeholder(_event=None):
            if str(variable.get() or "") == placeholder:
                variable.set("")
            set_normal()

        def on_focus_out(_event=None):
            if not str(variable.get() or "").strip():
                set_placeholder()
            else:
                set_normal()

        def on_var_change(*_args):
            text = str(variable.get() or "")
            if text == placeholder:
                try:
                    entry.configure(fg=placeholder_color)
                except Exception:
                    pass
            elif text.strip():
                set_normal()
            elif self.focus_get() is not entry:
                self.after_idle(set_placeholder)

        variable.trace_add("write", on_var_change)
        entry.bind("<FocusIn>", clear_placeholder, add="+")
        entry.bind("<FocusOut>", on_focus_out, add="+")
        if str(variable.get() or "").strip():
            on_var_change()
        else:
            set_placeholder()
    def _rebuild_exchange_strategy_panel(self, frame_top):
        self.exchange_strategy_frame = frame_top
        self._clear_container_children(frame_top)
        try:
            frame_top.configure(text="\u5171\u4eab\u7b56\u7565\u914d\u7f6e")
        except Exception:
            pass
        try:
            ttk.Style(self).configure("ExchangeAccent.TLabel", foreground="#7A3FF2")
        except Exception:
            pass
        try:
            style = ttk.Style(self)
            style.configure("AutoWithdraw.TCheckbutton", foreground="#C62828")
            style.map("AutoWithdraw.TCheckbutton", foreground=[("disabled", "#C62828")])
        except Exception:
            pass

        row1 = ttk.Frame(frame_top)
        row1.pack(fill="x", padx=5, pady=(2, 3))

        ttk.Label(row1, text="\u968f\u673a\u5ef6\u8fdf(\u6beb\u79d2):").grid(row=0, column=0, sticky="e")
        delay_wrap = ttk.Frame(row1)
        delay_wrap.grid(row=0, column=1, sticky="w", padx=(4, 8))
        self.min_delay_entry = tk.Entry(delay_wrap, textvariable=self.min_delay_var, width=8)
        self.min_delay_entry.pack(side="left")
        self.max_delay_entry = tk.Entry(delay_wrap, textvariable=self.max_delay_var, width=8)
        self.max_delay_entry.pack(side="left", padx=(6, 0))
        self._install_entry_placeholder(self.min_delay_entry, self.min_delay_var, "\u6700\u5c0f")
        self._install_entry_placeholder(self.max_delay_entry, self.max_delay_var, "\u6700\u5927")

        ttk.Label(row1, text="\u63d0\u73b0\u5e01\u79cd:").grid(row=0, column=2, sticky="e", padx=(12, 0))
        self.withdraw_coin_combo = ttk.Combobox(
            row1,
            textvariable=self.withdraw_coin_var,
            values=WITHDRAW_COIN_OPTIONS,
            width=8,
            state="readonly",
        )
        self.withdraw_coin_combo.grid(row=0, column=3, sticky="w", padx=(4, 12))

        ttk.Label(row1, text="\u7f51\u7edc:").grid(row=0, column=4, sticky="e")
        self.withdraw_net_combo = ttk.Combobox(
            row1,
            textvariable=self.withdraw_net_var,
            values=WITHDRAW_NETWORK_OPTIONS,
            width=10,
            state="readonly",
        )
        self.withdraw_net_combo.grid(row=0, column=5, sticky="w", padx=(4, 12))

        ttk.Label(row1, text="\u5230\u8d26\u8d85\u65f6(\u79d2):").grid(row=0, column=6, sticky="e")
        ttk.Entry(row1, textvariable=self.usdt_timeout_var, width=8).grid(row=0, column=7, sticky="w", padx=(4, 12))

        ttk.Checkbutton(row1, text="使用配置代理", variable=self.use_exchange_config_proxy_var).grid(row=0, column=8, sticky="w", padx=(0, 12))

        self.btn_save_strategy_config = ttk.Button(row1, text="\u4fdd\u5b58\u914d\u7f6e", command=self.save_strategy_config)
        self.btn_save_strategy_config.grid(row=0, column=9, sticky="w")

        row2 = ttk.Frame(frame_top)
        row2.pack(fill="x", padx=5, pady=3)
        self._strategy_row2 = row2
        current_trade_account_type = self._normalize_trade_account_type(self.trade_account_type_var.get())
        current_trade_mode = self._normalize_trade_mode(self.trade_mode_var.get())
        row2_left = ttk.Frame(row2)
        row2_left.grid(row=0, column=0, sticky="w")
        self._strategy_row2_left = row2_left
        row2_right = ttk.Frame(row2)
        row2_right.grid(row=0, column=1, sticky="w")
        row2.grid_columnconfigure(2, weight=1)
        self.trade_mode_combo = None
        self.trade_account_type_combo = None
        self.futures_margin_type_combo = None
        self.futures_side_combo = None

        ttk.Label(row2_left, text="交易类型:").pack(side="left")
        self.trade_account_type_combo = ttk.Combobox(
            row2_left,
            textvariable=self.trade_account_type_var,
            values=TRADE_ACCOUNT_TYPE_OPTIONS,
            width=8,
            state="readonly",
        )
        self.trade_account_type_combo.pack(side="left", padx=(4, 12))
        self.trade_account_type_combo.bind("<<ComboboxSelected>>", self._on_trade_mode_changed, add="+")

        if current_trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            ttk.Label(row2_left, text="交易模式:").pack(side="left")
            self.trade_mode_combo = ttk.Combobox(
                row2_left,
                textvariable=self.trade_mode_var,
                values=TRADE_MODE_OPTIONS,
                width=8,
                state="readonly",
            )
            self.trade_mode_combo.pack(side="left", padx=(4, 12))
            self.trade_mode_combo.bind("<<ComboboxSelected>>", self._on_trade_mode_changed, add="+")

            ttk.Label(row2_left, text="现货交易对:", style="ExchangeAccent.TLabel").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.spot_symbol_var, width=14).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="现货轮次:").pack(side="left")
            ttk.Spinbox(row2_left, from_=1, to=100, textvariable=self.spot_rounds_var, width=6).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="预购买BNB金额:").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.bnb_topup_amount_var, width=10).pack(side="left", padx=(4, 12))
        else:
            ttk.Label(row2_left, text="合约交易对:", style="ExchangeAccent.TLabel").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.futures_symbol_var, width=14).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="合约轮次:").pack(side="left")
            ttk.Spinbox(row2_left, from_=1, to=100, textvariable=self.futures_rounds_var, width=6).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="开仓方向:").pack(side="left")
            self.futures_side_combo = ttk.Combobox(
                row2_left,
                textvariable=self.futures_side_var,
                values=FUTURES_SIDE_OPTIONS,
                width=8,
                state="readonly",
            )
            self.futures_side_combo.pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="下单金额:").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.futures_amount_var, width=10).pack(side="left", padx=(4, 12))

        row3 = ttk.Frame(frame_top)
        row3.pack(fill="x", padx=5, pady=(3, 2))
        self._strategy_row3 = row3
        row3_left = ttk.Frame(row3)
        row3_left.grid(row=0, column=0, sticky="w")
        self._strategy_row3_left = row3_left
        row3_right = ttk.Frame(row3)
        row3_right.grid(row=0, column=1, sticky="w")
        self._strategy_row3_right = row3_right
        row3.grid_columnconfigure(2, weight=1)
        row3_entry_width = 8
        row3_compact_entry_width = 7
        row3_spin_width = 4
        row3_withdraw_width = 6
        row3_item_pad = (4, 8)
        row3_withdraw_pad = (4, 4)

        if current_trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            if current_trade_mode == TRADE_MODE_LIMIT:
                ttk.Label(row3_left, text="剩余bnb手续费:").pack(side="left")
                ttk.Entry(row3_left, textvariable=self.bnb_fee_stop_var, width=row3_entry_width).pack(side="left", padx=row3_item_pad)
                ttk.Label(row3_left, text=self._reprice_threshold_label_text()).pack(side="left")
                ttk.Entry(row3_left, textvariable=self.reprice_threshold_var, width=row3_entry_width).pack(side="left", padx=row3_item_pad)
            elif current_trade_mode == TRADE_MODE_PREMIUM:
                ttk.Label(row3_left, text="剩余bnb手续费:").pack(side="left")
                ttk.Entry(row3_left, textvariable=self.bnb_fee_stop_var, width=row3_compact_entry_width).pack(side="left", padx=row3_item_pad)
                ttk.Label(row3_left, text="溢价:").pack(side="left")
                ttk.Entry(row3_left, textvariable=self.premium_delta_var, width=row3_compact_entry_width).pack(side="left", padx=row3_item_pad)
                ttk.Label(row3_left, text="笔数:").pack(side="left")
                ttk.Spinbox(row3_left, from_=0, to=20, textvariable=self.premium_order_count_var, width=row3_spin_width).pack(side="left", padx=row3_item_pad)
                ttk.Label(row3_left, text="追加挂单:").pack(side="left")
                ttk.Entry(row3_left, textvariable=self.premium_append_threshold_var, width=row3_compact_entry_width).pack(side="left", padx=row3_item_pad)
                ttk.Label(row3_left, text=self._reprice_threshold_label_text()).pack(side="left")
                ttk.Entry(row3_left, textvariable=self.reprice_threshold_var, width=row3_compact_entry_width).pack(side="left", padx=row3_item_pad)

            ttk.Label(row3_right, text="提现预留:").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.withdraw_buffer_var, width=row3_withdraw_width).pack(side="left", padx=row3_withdraw_pad)
            ttk.Checkbutton(row3_right, text="\u81ea\u52a8\u63d0\u73b0", variable=self.enable_withdraw_var, style="AutoWithdraw.TCheckbutton").pack(side="left")
        else:
            ttk.Label(row3_left, text="保证金模式:").pack(side="left")
            self.futures_margin_type_combo = ttk.Combobox(
                row3_left,
                textvariable=self.futures_margin_type_var,
                values=FUTURES_MARGIN_TYPE_LABEL_OPTIONS,
                width=10,
                state="readonly",
            )
            self.futures_margin_type_combo.pack(side="left", padx=row3_item_pad)
            ttk.Label(row3_left, text="杠杆:").pack(side="left")
            ttk.Spinbox(row3_left, from_=1, to=125, textvariable=self.futures_leverage_var, width=5).pack(side="left", padx=row3_item_pad)
            ttk.Label(row3_right, text="提现预留:").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.withdraw_buffer_var, width=row3_withdraw_width).pack(side="left", padx=row3_withdraw_pad)
            ttk.Checkbutton(row3_right, text="\u81ea\u52a8\u63d0\u73b0", variable=self.enable_withdraw_var, style="AutoWithdraw.TCheckbutton").pack(side="left")

        self.after_idle(self._align_trade_mode_sections)
    def _on_close(self):
        if self._closing:
            return
        self._closing = True
        self._close_deadline_monotonic = time.monotonic() + 2.5
        logger.info("收到窗口关闭请求，开始优雅停止后台任务")
        self._cancel_after_token("_accounts_save_after_token")
        self._save_accounts_silently()
        self._cancel_after_token("_update_ip_after_token")
        self._cancel_after_token("_log_poll_after_token")
        try:
            self.stop_bot()
        except Exception:
            pass
        page = getattr(self, "onchain_page", None)
        if page is not None:
            try:
                page.shutdown()
            except Exception:
                pass
        self._complete_close_when_idle()
    def _cancel_after_token(self, attr_name: str) -> None:
        token = getattr(self, attr_name, None)
        if token is None:
            return
        setattr(self, attr_name, None)
        try:
            self.after_cancel(token)
        except Exception:
            pass
    def _start_managed_thread(self, target, *, args=(), kwargs=None, name: str = "app-bg", daemon: bool = True) -> threading.Thread:
        call_kwargs = dict(kwargs or {})

        def runner():
            try:
                target(*args, **call_kwargs)
            finally:
                current = threading.current_thread()
                with self._managed_threads_lock:
                    self._managed_threads.discard(current)

        thread = threading.Thread(target=runner, daemon=daemon, name=name)
        with self._managed_threads_lock:
            self._managed_threads.add(thread)
        thread.start()
        return thread
    def _managed_threads_snapshot(self) -> list[threading.Thread]:
        current = threading.current_thread()
        with self._managed_threads_lock:
            return [t for t in self._managed_threads if t is not current and t.is_alive()]
    def _join_managed_threads(self, timeout_total: float = 1.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_total))
        while True:
            alive_threads = self._managed_threads_snapshot()
            if not alive_threads:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            per_thread = max(0.05, remaining / max(1, len(alive_threads)))
            for thread in alive_threads:
                thread.join(per_thread)
    def _background_shutdown_pending(self) -> bool:
        if self.worker_thread and self.worker_thread.is_alive():
            return True
        if self._managed_threads_snapshot():
            return True
        page = getattr(self, "onchain_page", None)
        if page is not None and bool(getattr(page, "is_running", False)):
            return True
        return False
    def _complete_close_when_idle(self):
        self._close_wait_after_token = None
        if self._background_shutdown_pending() and time.monotonic() < self._close_deadline_monotonic:
            try:
                self._close_wait_after_token = self.after(100, self._complete_close_when_idle)
                return
            except Exception:
                self._close_wait_after_token = None
        self._finalize_close()
    def _finalize_close(self):
        if self._close_finalized:
            return
        self._close_finalized = True
        self._cancel_after_token("_accounts_save_after_token")
        self._cancel_after_token("_close_wait_after_token")
        self._cancel_after_token("_update_ip_after_token")
        self._cancel_after_token("_log_poll_after_token")
        page = getattr(self, "onchain_page", None)
        if page is not None:
            try:
                page.shutdown()
            except Exception:
                pass
        try:
            stop_ui_bridge(self)
        except Exception:
            pass
        try:
            self.exchange_proxy_runtime.stop()
        except Exception:
            pass
        try:
            self.onchain_proxy_runtime.stop()
        except Exception:
            pass
        self._join_managed_threads(timeout_total=1.0)
        self._clear_current_binance_client()
        self.destroy()
    def _dispatch_ui(self, callback) -> None:
        if self._closing:
            return
        dispatch_ui_callback(self, callback, root=self)


_STAR_EXPORT_NAMES = {
    "_atomic_write_config_json",
    "_atomic_write_json",
    "_atomic_write_text",
    "_atomic_write_text_with_backup",
    "_load_json_with_backup",
    "_read_text_snapshot",
    "_require_dict_payload",
    "_restore_text_snapshot",
    "_shift_text_view_state_after_trim",
}


def _build_module_star_exports() -> list[str]:
    exports: list[str] = []
    for name in globals():
        if name.startswith("__"):
            continue
        if name.startswith("_") and name not in _STAR_EXPORT_NAMES:
            continue
        exports.append(name)
    return exports


__all__ = _build_module_star_exports()
