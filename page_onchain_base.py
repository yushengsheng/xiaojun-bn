#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ipaddress
import os
import queue
import re
import threading
import time
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
import tkinter as tk
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, BooleanVar, DoubleVar, Frame as TkFrame, Menu, StringVar
from tkinter import messagebox, ttk

import requests

from api_clients import EvmClient
from app_paths import (
    ONCHAIN_DATA_FILE,
    RELAY_FAILED_EXPORT_FILE,
    RELAY_MANUAL_EXPORT_FILE,
    RELAY_WALLET_FILE,
)
from core_models import EvmToken, GeneratedWalletEntry, OnchainPairEntry, OnchainSettings, WithdrawRuntimeParams
from onchain_relay_wallets import RelayWalletFileStore
from shared_utils import (
    LOG_MAX_ROWS,
    SolidButton,
    bind_paste_shortcuts,
    clear_ui_batch_size,
    decimal_to_text,
    dispatch_ui_callback,
    flush_queued_log_rows,
    flush_queued_ui_renders,
    make_scrollbar,
    mask_text,
    parse_worker_threads,
    queue_log_row,
    queue_ui_render,
    random_decimal_between,
    schedule_ui_callback,
    start_ui_bridge,
    stop_ui_bridge,
    set_ui_batch_size,
)
from stores import OnchainStore
from table_import_utils import (
    IMPORT_TARGET_PURPLE,
    column_name_from_identifier,
    heading_text,
    merge_column_values,
    parse_single_value_lines,
    update_import_target_bar,
)
import task_progress

SUBMITTED_TIMEOUT_SECONDS = 180.0

class OnchainTransferPageBase:
    MODE_M2M = "多对多"
    MODE_1M = "1对多"
    MODE_M1 = "多对1"
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数"
    AMOUNT_MODE_ALL = "全部"
    AMOUNT_ALL_LABEL = "全部"
    MODE_AMOUNT_STORAGE_KEYS = {
        MODE_M2M: "multi_to_multi",
        MODE_1M: "one_to_many",
        MODE_M1: "many_to_one",
    }
    MODE_RELAY_STORAGE_KEYS = {
        MODE_1M: "one_to_many",
        MODE_M1: "many_to_one",
    }
    MODE_AMOUNT_DEFAULTS = {
        MODE_M2M: AMOUNT_MODE_ALL,
        MODE_1M: AMOUNT_MODE_FIXED,
        MODE_M1: AMOUNT_MODE_ALL,
    }
    NETWORK_OPTIONS = ["", "ETH", "BSC"]
    MAX_TOKEN_DECIMALS = 36
    TREE_COL_MIN_WIDTHS = {
        "checked": 42,
        "idx": 42,
        "source": 330,
        "target": 420,
        "status": 110,
        "recovery": 96,
        "balance": 80,
    }
    TREE_COL_WEIGHTS = {
        "checked": 1,
        "idx": 1,
        "source": 5,
        "target": 7,
        "status": 2,
        "recovery": 2,
        "balance": 1,
    }

    def __init__(self, parent, rpc_proxy_getter=None, proxy_text_normalizer=None):
        self.parent = parent
        self.root = parent.winfo_toplevel()
        self.store = OnchainStore(ONCHAIN_DATA_FILE)
        self._rpc_proxy_getter = rpc_proxy_getter
        self._proxy_text_normalizer = proxy_text_normalizer
        self.client = EvmClient(
            proxy_provider=rpc_proxy_getter,
            allow_system_proxy_provider=self._allow_onchain_system_proxy,
        )
        self.relay_wallet_store = RelayWalletFileStore(RELAY_WALLET_FILE)
        self._closing = False
        self.is_running = False
        self.stop_requested = threading.Event()
        self._managed_threads_lock = threading.Lock()
        self._managed_threads: set[threading.Thread] = set()
        self._layout_mode: str | None = None

        self.row_index_map: dict[str, int] = {}
        self.row_key_by_row_id: dict[str, str] = {}
        self.row_id_by_key: dict[str, str] = {}
        self.checked_row_keys: set[str] = set()
        self.row_status: dict[str, str] = {}
        self.row_status_text_map: dict[str, str] = {}
        self.row_status_context: dict[str, str] = {}
        self.row_recovery_status: dict[str, str] = {}
        self.row_recovery_text_map: dict[str, str] = {}
        self.row_recovery_context: dict[str, str] = {}
        self.query_row_status: dict[str, str] = {}
        self.query_row_status_context: dict[str, str] = {}
        self.source_balance_cache: dict[str, Decimal] = {}
        self.target_balance_cache: dict[str, Decimal] = {}
        self.source_address_cache: dict[str, str] = {}
        self.source_private_key_cache: dict[str, str] = {}
        self.current_tokens: dict[str, EvmToken] = {}
        self.custom_tokens_by_network: dict[str, dict[str, EvmToken]] = {"ETH": {}, "BSC": {}}
        self._query_row_keys_by_source: dict[str, list[str]] = {}
        self.wallet_cache_lock = threading.Lock()
        self._onchain_proxy_state_lock = threading.Lock()
        self._onchain_proxy_state = {"use_config_proxy": False, "raw_proxy": ""}
        self._runtime_state_lock = threading.Lock()
        self._runtime_state = {"threads_raw": "10"}

        self.mode_var = StringVar(value=self.MODE_M2M)
        self.network_var = StringVar(value="")
        self.coin_var = StringVar(value="")
        self.symbol_var = StringVar(value="-")
        self.contract_search_var = StringVar(value="")
        self.amount_mode_var = StringVar(value=self.AMOUNT_MODE_FIXED)
        self.amount_var = StringVar(value="")
        self.random_min_var = StringVar(value="")
        self.random_max_var = StringVar(value="")
        self.delay_var = DoubleVar(value=1.0)
        self.threads_var = StringVar(value="10")
        self.confirm_timeout_var = StringVar(value="180")
        self.dry_run_var = BooleanVar(value=False)
        self.use_config_proxy_var = BooleanVar(value=False)
        self.onchain_proxy_var = StringVar(value="")
        self.onchain_proxy_status_var = StringVar(value="未启用")
        self.onchain_proxy_exit_ip_var = StringVar(value="--")
        self.source_credential_var = StringVar(value="")
        self.target_address_var = StringVar(value="")
        self.relay_enabled_var = BooleanVar(value=False)
        self.relay_fee_reserve_var = StringVar(value="")
        self.source_balance_var = StringVar(value="-")
        self.target_balance_var = StringVar(value="-")
        self.progress_var = StringVar(value=task_progress.idle_text("转账总额"))
        self._active_progress_kind = ""
        self._active_progress_keys: list[str] = []
        self._progress_amount_label = "转账总额"
        self._summary_balance_text = "-"
        self._summary_amount_text = "-"
        self._summary_gas_text = "-"
        self._import_target = "full"
        self._mode_amount_configs: dict[str, dict[str, str]] = {}
        self._mode_amount_config_ready = False
        self._last_mode_for_amounts = self.mode_var.get().strip()
        self._mode_relay_configs: dict[str, dict[str, object]] = {}
        self._mode_relay_config_ready = False
        self._last_mode_for_relay = self.mode_var.get().strip()
        self.m2m_import_drafts: list[dict[str, str]] = []
        self.checked_m2m_draft_rows: set[int] = set()
        self.generated_wallets: list[GeneratedWalletEntry] = []
        self.wallet_generator_window = None
        self.wallet_generator_generate_btn = None
        self.wallet_generate_count_var = StringVar(value="10")
        self.wallet_export_format_var = StringVar(value="地址 + 私钥")
        self._sync_onchain_proxy_state()
        self._sync_runtime_state()

        self._build_ui()
        start_ui_bridge(self, root=self.root)
        self._load_data()
    def _build_ui(self):
        try:
            style = ttk.Style(self.root)
            style.configure("WalletAction.TButton", foreground="#C62828")
            style.map(
                "WalletAction.TButton",
                foreground=[
                    ("disabled", "#9E9E9E"),
                    ("pressed", "#B71C1C"),
                    ("active", "#B71C1C"),
                    ("!disabled", "#C62828"),
                ],
            )
        except Exception:
            pass

        main = ttk.Frame(self.parent, padding=12)
        main.pack(fill=BOTH, expand=True)

        setting = ttk.LabelFrame(main, text="链上批量转账配置（EVM）", padding=14)
        setting.pack(fill="x", pady=(0, 10))
        self.setting_frame = setting

        self.lbl_mode = ttk.Label(setting, text="转账模式*")
        self.mode_box = ttk.Combobox(
            setting,
            textvariable=self.mode_var,
            values=[self.MODE_M2M, self.MODE_1M, self.MODE_M1],
            width=10,
            state="readonly",
        )
        self.lbl_network = ttk.Label(setting, text="网络*")
        self.network_box = ttk.Combobox(
            setting,
            textvariable=self.network_var,
            values=self.NETWORK_OPTIONS,
            width=8,
            state="readonly",
        )
        self.lbl_coin = ttk.Label(setting, text="币种*")
        self.coin_box = ttk.Combobox(setting, textvariable=self.coin_var, width=14, state="readonly")
        self.lbl_contract_search = ttk.Label(setting, text="合约搜索")
        self.ent_contract_search = ttk.Entry(setting, textvariable=self.contract_search_var, width=28)
        bind_paste_shortcuts(self.ent_contract_search)
        self.btn_contract_search = ttk.Button(setting, text="搜索并选择", command=self.search_contract_token)

        self.lbl_amount = ttk.Label(setting, text="转账数量*")
        self.amount_ctrl = ttk.Frame(setting)
        self.amount_mode_box = ttk.Combobox(
            self.amount_ctrl,
            textvariable=self.amount_mode_var,
            values=[self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL],
            width=7,
            state="readonly",
        )
        self.ent_amount = ttk.Entry(self.amount_ctrl, textvariable=self.amount_var, width=7)
        self.ent_random_min = ttk.Entry(self.amount_ctrl, textvariable=self.random_min_var, width=6)
        self.lbl_random_sep = ttk.Label(self.amount_ctrl, text="~")
        self.ent_random_max = ttk.Entry(self.amount_ctrl, textvariable=self.random_max_var, width=6)
        self.lbl_amount_all_hint = ttk.Label(self.amount_ctrl, text="按钱包可用余额", style="Subtle.TLabel")
        self._apply_amount_layout()

        self.chk_dry_run = ttk.Checkbutton(setting, text="模拟执行", variable=self.dry_run_var)
        self.chk_use_config_proxy = ttk.Checkbutton(setting, text="使用配置代理", variable=self.use_config_proxy_var)
        self.btn_save_all = ttk.Button(setting, text="保存配置", command=self.save_all)
        self.lbl_delay = ttk.Label(setting, text="执行间隔(秒)")
        self.ent_delay = ttk.Entry(setting, textvariable=self.delay_var, width=7)
        self.lbl_threads = ttk.Label(setting, text="执行线程数")
        self.spin_threads = ttk.Spinbox(setting, from_=1, to=64, textvariable=self.threads_var, width=6)
        self.lbl_confirm_timeout = ttk.Label(setting, text="确认超时(秒)")
        self.ent_confirm_timeout = ttk.Entry(setting, textvariable=self.confirm_timeout_var, width=7)

        self.lbl_source_credential = ttk.Label(setting, text="转出钱包私钥/助记词*")
        self.ent_source_credential = ttk.Entry(setting, textvariable=self.source_credential_var, width=34)
        self.btn_query_source_balance = ttk.Button(setting, text="查询", command=self.query_current_source_balance)
        self.lbl_source_balance_title = ttk.Label(setting, text="转出钱包余额")
        self.lbl_source_balance_val = ttk.Label(setting, textvariable=self.source_balance_var, style="Value.TLabel")
        self.chk_relay_enabled = ttk.Checkbutton(setting, text="启用中转", variable=self.relay_enabled_var)
        self.lbl_relay_fee_reserve = ttk.Label(setting, text="预留原生币手续费")
        self.ent_relay_fee_reserve = ttk.Entry(setting, textvariable=self.relay_fee_reserve_var, width=12)
        self.lbl_relay_wallet_file = ttk.Label(
            setting,
            text=f"中转钱包明文保存在：{RELAY_WALLET_FILE.name}；仅清理超过 72 小时且余额已清空的钱包。",
            style="Subtle.TLabel",
            justify="left",
            wraplength=920,
        )
        self.lbl_target_address = ttk.Label(setting, text="收款地址*")
        self.ent_target_address = ttk.Entry(setting, textvariable=self.target_address_var, width=34)
        self.btn_query_target_balance = ttk.Button(setting, text="查询", command=self.query_current_target_balance)
        bind_paste_shortcuts(self.ent_source_credential)
        bind_paste_shortcuts(self.ent_target_address)
        self.lbl_target_balance_title = ttk.Label(setting, text="收款地址余额")
        self.lbl_target_balance_val = ttk.Label(setting, textvariable=self.target_balance_var, style="Value.TLabel")
        self._apply_setting_layout("wide")
        self._refresh_relay_controls()

        self.table_wrap = ttk.Frame(main)
        self.table_wrap.pack(fill=BOTH, expand=True)
        self.table_wrap.columnconfigure(0, weight=1)
        self.table_wrap.rowconfigure(0, weight=1)

        cols = ("checked", "idx", "source", "target", "status", "recovery", "balance")
        self.tree = ttk.Treeview(self.table_wrap, columns=cols, show="headings", selectmode="extended", height=16)
        self._tree_column_ids = cols
        self._tree_heading_base_texts = {
            "checked": "勾选",
            "idx": "编号",
            "source": "转出凭证",
            "target": "接收地址",
            "status": "执行状态",
            "recovery": "回收状态",
            "balance": "余额",
        }
        for column, text in self._tree_heading_base_texts.items():
            self.tree.heading(column, text=text)

        self.tree.column("checked", width=42, anchor="center")
        self.tree.column("idx", width=42, anchor="center")
        self.tree.column("source", width=360, anchor="w")
        self.tree.column("target", width=420, anchor="w")
        self.tree.column("status", width=110, anchor="center")
        self.tree.column("recovery", width=96, anchor="center")
        self.tree.column("balance", width=100, anchor="w")
        self.tree.tag_configure("st_waiting", foreground="#8a6d3b", background="#fff7e0")
        self.tree.tag_configure("st_running", foreground="#1d5fbf", background="#eaf2ff")
        self.tree.tag_configure("st_success", foreground="#1b7f3b", background="#eaf8ef")
        self.tree.tag_configure("st_failed", foreground="#b02a37", background="#fdecef")
        self.tree.tag_configure("st_submitted", foreground="#6d28d9", background="#f3e8ff")
        self.tree.tag_configure("st_incomplete", foreground="#8a6d3b", background="#fff7e0")

        self.tree_ybar = self._make_scrollbar(self.table_wrap, orient=VERTICAL, command=self.tree.yview)
        self.tree_xbar = self._make_scrollbar(self.table_wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.tree_ybar.set, xscrollcommand=self.tree_xbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree_ybar.grid(row=0, column=1, sticky="ns")
        self.tree_xbar.grid(row=1, column=0, sticky="ew")
        self.import_target_bar = TkFrame(self.table_wrap, bg=IMPORT_TARGET_PURPLE, bd=0, highlightthickness=0)
        self.empty_hint_label = ttk.Label(self.table_wrap, style="Subtle.TLabel", justify="center", anchor="center")

        self.tree.bind("<Button-1>", self._on_tree_pointer_down, add="+")
        self.tree.bind("<Double-Button-1>", self._on_tree_click, add="+")
        self.tree.bind("<Button-2>", self._on_tree_right_click, add="+")
        self.tree.bind("<Button-3>", self._on_tree_right_click, add="+")
        self.tree.bind("<Control-Button-1>", self._on_tree_right_click, add="+")
        self.tree.bind("<Command-v>", self._on_tree_paste)
        self.tree.bind("<Control-v>", self._on_tree_paste)
        self.empty_hint_label.bind("<Button-1>", lambda _event: self._set_import_target("full", log_change=True))
        self.row_menu = Menu(self.root, tearoff=0)
        self.row_menu.add_command(label="查询余额（当前行）", command=self.start_query_balance_current_row)
        self.row_menu.add_command(label="执行转账（当前行）", command=self.start_transfer_current_row)
        self.row_menu.add_separator()
        self.row_menu.add_command(label="删除（当前行）", command=self.delete_current_row)

        action1 = ttk.Frame(main)
        action1.pack(fill="x", pady=10)
        ttk.Button(action1, text="粘贴导入", command=self.import_from_paste).pack(side=LEFT)
        ttk.Button(action1, text="导入 TXT", command=self.import_txt).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="导出 TXT", command=self.export_txt).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="全选/取消全选", command=self.toggle_check_all).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="删除选中", command=self.delete_selected).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="创建钱包", style="WalletAction.TButton", command=self.open_wallet_generator).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="打开待人工", command=self.open_relay_manual_export_file).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="打开失败账号", command=self.open_relay_failed_export_file).pack(side=LEFT, padx=(8, 0))
        ttk.Label(action1, text="链上为独立模块，与交易所互不影响。", style="Subtle.TLabel").pack(side=LEFT, padx=(12, 0))

        action2 = ttk.Frame(main)
        action2.pack(fill="x", pady=(0, 10))
        ttk.Button(action2, text="查询余额", command=self.start_query_balance).pack(side=LEFT)
        self.btn_stop_tasks = SolidButton(
            action2,
            text="停止",
            command=self.stop_current_tasks,
            bg="#C62828",
            fg="#FFFFFF",
            activebackground="#B71C1C",
            activeforeground="#FFFFFF",
            relief="flat",
            padx=12,
        )
        self.btn_stop_tasks.pack(side=LEFT, padx=(8, 0))
        self.lbl_progress = ttk.Label(action2, textvariable=self.progress_var, style="Subtle.TLabel", anchor="w", justify="left")
        self.lbl_progress.pack(side=LEFT, fill="x", expand=True, padx=(10, 0))
        self.btn_batch_transfer = SolidButton(
            action2,
            text="执行批量转账",
            command=self.start_batch_transfer,
            bg="#1E8449",
            fg="#FFFFFF",
            activebackground="#186A3B",
            activeforeground="#FFFFFF",
            disabledforeground="#E8F5E9",
            relief="flat",
            padx=12,
            pady=2,
        )
        self.btn_batch_transfer.pack(side=RIGHT)
        ttk.Button(action2, text="中转手续费回收", style="Action.TButton", command=self.start_relay_fee_recovery).pack(side=RIGHT, padx=(8, 0))
        ttk.Button(action2, text="失败重试", style="Action.TButton", command=self.start_retry_failed).pack(side=RIGHT, padx=(8, 0))

        self.log_box = ttk.LabelFrame(main, text="执行日志", padding=8)
        self.log_box.pack(fill=BOTH, expand=False)
        self.log_box.columnconfigure(0, weight=1)
        self.log_box.rowconfigure(0, weight=1)
        self.log_tree = ttk.Treeview(self.log_box, columns=("time", "msg"), show="headings", height=9)
        self.log_tree.heading("time", text="时间")
        self.log_tree.heading("msg", text="日志")
        self.log_tree.column("time", width=170, anchor="center")
        self.log_tree.column("msg", width=950, anchor="w")

        self.log_ybar = self._make_scrollbar(self.log_box, orient=VERTICAL, command=self.log_tree.yview)
        self.log_xbar = self._make_scrollbar(self.log_box, orient="horizontal", command=self.log_tree.xview)
        self.log_tree.configure(yscrollcommand=self.log_ybar.set, xscrollcommand=self.log_xbar.set)
        self.log_tree.grid(row=0, column=0, sticky="nsew")
        self.log_ybar.grid(row=0, column=1, sticky="ns")
        self.log_xbar.grid(row=1, column=0, sticky="ew")

        self.mode_var.trace_add("write", self._on_mode_changed)
        self.network_var.trace_add("write", self._on_network_changed)
        self.coin_var.trace_add("write", self._on_coin_changed)
        self.amount_mode_var.trace_add("write", self._on_amount_mode_changed)
        self.threads_var.trace_add("write", self._on_runtime_settings_changed)
        self.use_config_proxy_var.trace_add("write", self._on_proxy_config_changed)
        self.onchain_proxy_var.trace_add("write", self._on_proxy_config_changed)
        self.source_credential_var.trace_add("write", self._on_source_or_target_changed)
        self.target_address_var.trace_add("write", self._on_source_or_target_changed)
        self.relay_enabled_var.trace_add("write", self._on_relay_settings_changed)

        self.table_wrap.bind("<Configure>", self._on_table_resize)
        self.log_box.bind("<Configure>", self._on_log_resize)
        self.root.bind("<Configure>", self._on_root_resize, add="+")
        self.root.after_idle(self._on_mode_changed)
        self.root.after_idle(self._resize_tree_columns)
        self.root.after_idle(self._on_log_resize)
        self.root.after_idle(self._on_root_resize)
        self.root.after_idle(self._apply_import_target_view)
        self.root.after_idle(self._update_empty_hint)
        self.root.after_idle(self._refresh_onchain_proxy_summary)
    @staticmethod
    def _make_scrollbar(parent, orient, command):
        return make_scrollbar(parent, orient, command)
    def _mode(self) -> str:
        m = self.mode_var.get().strip()
        if m not in {self.MODE_M2M, self.MODE_1M, self.MODE_M1}:
            m = self.MODE_M2M
            self.mode_var.set(m)
        return m
    def _is_mode_m2m(self) -> bool:
        return self._mode() == self.MODE_M2M
    def _is_mode_1m(self) -> bool:
        return self._mode() == self.MODE_1M
    def _is_mode_m1(self) -> bool:
        return self._mode() == self.MODE_M1
    @classmethod
    def _amount_storage_key(cls, mode: str) -> str:
        return cls.MODE_AMOUNT_STORAGE_KEYS.get(str(mode or "").strip(), "")
    @classmethod
    def _default_mode_amount_config(cls, mode: str) -> dict[str, str]:
        amount_mode = cls.MODE_AMOUNT_DEFAULTS.get(str(mode or "").strip(), cls.AMOUNT_MODE_FIXED)
        return {
            "amount_mode": amount_mode,
            "amount": cls.AMOUNT_ALL_LABEL if amount_mode == cls.AMOUNT_MODE_ALL else "",
            "random_min": "",
            "random_max": "",
        }
    @classmethod
    def _normalize_mode_amount_config(cls, mode: str, config: dict[str, object] | None) -> dict[str, str]:
        raw = dict(config or {})
        normalized = cls._default_mode_amount_config(mode)
        amount_mode = str(raw.get("amount_mode", normalized["amount_mode"]) or normalized["amount_mode"]).strip()
        if amount_mode not in {cls.AMOUNT_MODE_FIXED, cls.AMOUNT_MODE_RANDOM, cls.AMOUNT_MODE_ALL}:
            amount_mode = normalized["amount_mode"]
        amount = str(raw.get("amount", normalized["amount"]) or "").strip()
        if amount_mode == cls.AMOUNT_MODE_ALL:
            amount = cls.AMOUNT_ALL_LABEL
        random_min = str(raw.get("random_min", normalized["random_min"]) or "").strip()
        random_max = str(raw.get("random_max", normalized["random_max"]) or "").strip()
        return {
            "amount_mode": amount_mode,
            "amount": amount,
            "random_min": random_min,
            "random_max": random_max,
        }
    @classmethod
    def _persistable_mode_amount_config(cls, mode: str, config: dict[str, object] | None) -> dict[str, str] | None:
        normalized = cls._normalize_mode_amount_config(mode, config)
        default_config = cls._default_mode_amount_config(mode)
        if normalized == default_config:
            return dict(default_config)
        amount_mode = normalized["amount_mode"]
        if amount_mode == cls.AMOUNT_MODE_ALL:
            return dict(default_config if default_config["amount_mode"] == cls.AMOUNT_MODE_ALL else {
                "amount_mode": cls.AMOUNT_MODE_ALL,
                "amount": cls.AMOUNT_ALL_LABEL,
                "random_min": "",
                "random_max": "",
            })
        if amount_mode == cls.AMOUNT_MODE_FIXED:
            amount_raw = normalized["amount"].strip()
            if not amount_raw:
                return None
            try:
                amount_value = Decimal(amount_raw)
            except Exception:
                return None
            if amount_value <= 0:
                return None
            return {
                "amount_mode": cls.AMOUNT_MODE_FIXED,
                "amount": decimal_to_text(amount_value),
                "random_min": "",
                "random_max": "",
            }
        if amount_mode == cls.AMOUNT_MODE_RANDOM:
            min_raw = normalized["random_min"].strip()
            max_raw = normalized["random_max"].strip()
            if not min_raw or not max_raw:
                return None
            try:
                min_value = Decimal(min_raw)
                max_value = Decimal(max_raw)
            except Exception:
                return None
            if min_value <= 0 or max_value <= 0 or max_value < min_value:
                return None
            return {
                "amount_mode": cls.AMOUNT_MODE_RANDOM,
                "amount": "",
                "random_min": decimal_to_text(min_value),
                "random_max": decimal_to_text(max_value),
            }
        return None
    def _load_mode_amount_configs_from_settings(self, settings: OnchainSettings) -> None:
        raw_mode_amounts = getattr(settings, "mode_amounts", {}) or {}
        if not isinstance(raw_mode_amounts, dict):
            raw_mode_amounts = {}
        active_mode = str(getattr(settings, "mode", "") or "").strip()
        legacy_config = {
            "amount_mode": str(getattr(settings, "amount_mode", "") or "").strip(),
            "amount": str(getattr(settings, "amount", "") or "").strip(),
            "random_min": str(getattr(settings, "random_min", "") or "").strip(),
            "random_max": str(getattr(settings, "random_max", "") or "").strip(),
        }
        legacy_has_explicit_value = (
            legacy_config["amount_mode"] in {self.AMOUNT_MODE_ALL, self.AMOUNT_MODE_RANDOM}
            or bool(legacy_config["amount"])
            or bool(legacy_config["random_min"])
            or bool(legacy_config["random_max"])
        )
        configs: dict[str, dict[str, str]] = {}
        for mode in (self.MODE_M2M, self.MODE_1M, self.MODE_M1):
            storage_key = self._amount_storage_key(mode)
            stored = raw_mode_amounts.get(storage_key) if storage_key else None
            if isinstance(stored, dict):
                configs[mode] = self._normalize_mode_amount_config(mode, stored)
                continue
            if mode == active_mode and legacy_has_explicit_value:
                configs[mode] = self._normalize_mode_amount_config(mode, legacy_config)
                continue
            configs[mode] = self._default_mode_amount_config(mode)
        self._mode_amount_configs = configs
    def _store_mode_amount_config(self, mode: str, config: dict[str, object]) -> None:
        mode_key = str(mode or "").strip()
        if mode_key not in self.MODE_AMOUNT_STORAGE_KEYS:
            return
        self._mode_amount_configs[mode_key] = self._normalize_mode_amount_config(mode_key, config)
    def _capture_mode_amount_config(self, mode: str | None = None) -> None:
        mode_key = str(mode or getattr(self, "_last_mode_for_amounts", "") or self._mode()).strip()
        if mode_key not in self.MODE_AMOUNT_STORAGE_KEYS:
            return
        current = {
            "amount_mode": self._amount_mode(),
            "amount": self.amount_var.get().strip(),
            "random_min": self.random_min_var.get().strip(),
            "random_max": self.random_max_var.get().strip(),
        }
        if current["amount_mode"] == self.AMOUNT_MODE_ALL:
            current["amount"] = self.AMOUNT_ALL_LABEL
        self._store_mode_amount_config(mode_key, current)
    def _apply_mode_amount_config(self, mode: str) -> None:
        config = self._normalize_mode_amount_config(mode, self._mode_amount_configs.get(str(mode or "").strip()))
        self.amount_mode_var.set(config["amount_mode"])
        self.amount_var.set("" if config["amount_mode"] == self.AMOUNT_MODE_ALL else config["amount"])
        self.random_min_var.set(config["random_min"])
        self.random_max_var.set(config["random_max"])
    def _mode_amounts_payload(
        self,
        *,
        existing_mode_amounts: dict[str, dict[str, object]] | None = None,
        current_mode: str | None = None,
        current_config: dict[str, object] | None = None,
    ) -> dict[str, dict[str, str]]:
        payload: dict[str, dict[str, str]] = {}
        existing_raw = existing_mode_amounts if isinstance(existing_mode_amounts, dict) else {}
        for mode, storage_key in self.MODE_AMOUNT_STORAGE_KEYS.items():
            candidate = current_config if str(current_mode or "").strip() == mode and current_config is not None else self._mode_amount_configs.get(mode)
            persisted = self._persistable_mode_amount_config(mode, candidate)
            if persisted is None:
                persisted = self._persistable_mode_amount_config(mode, existing_raw.get(storage_key))
            if persisted is None:
                persisted = self._default_mode_amount_config(mode)
            payload[storage_key] = dict(persisted)
        return payload
    @classmethod
    def _relay_storage_key(cls, mode: str) -> str:
        return cls.MODE_RELAY_STORAGE_KEYS.get(str(mode or "").strip(), "")
    @classmethod
    def _default_mode_relay_config(cls, mode: str) -> dict[str, object]:
        return {"relay_enabled": False, "relay_fee_reserve": ""}
    @classmethod
    def _normalize_mode_relay_config(cls, mode: str, config: dict[str, object] | None) -> dict[str, object]:
        raw = dict(config or {})
        raw_enabled = raw.get("relay_enabled", False)
        if isinstance(raw_enabled, str):
            relay_enabled = raw_enabled.strip().lower() in {"1", "true", "yes", "on"}
        else:
            relay_enabled = bool(raw_enabled)
        return {
            "relay_enabled": relay_enabled,
            "relay_fee_reserve": str(raw.get("relay_fee_reserve", "") or "").strip(),
        }
    @classmethod
    def _persistable_mode_relay_config(cls, mode: str, config: dict[str, object] | None) -> dict[str, object] | None:
        normalized = cls._normalize_mode_relay_config(mode, config)
        reserve_raw = normalized["relay_fee_reserve"]
        relay_enabled = bool(normalized.get("relay_enabled"))
        if not reserve_raw:
            return {
                "relay_enabled": relay_enabled,
                "relay_fee_reserve": "",
            }
        try:
            reserve_value = Decimal(reserve_raw)
        except Exception:
            return None
        if reserve_value < 0:
            return None
        return {
            "relay_enabled": relay_enabled,
            "relay_fee_reserve": decimal_to_text(reserve_value),
        }
    def _load_mode_relay_configs_from_settings(self, settings: OnchainSettings) -> None:
        raw_mode_relay_configs = getattr(settings, "mode_relay_configs", {}) or {}
        if not isinstance(raw_mode_relay_configs, dict):
            raw_mode_relay_configs = {}
        active_mode = str(getattr(settings, "mode", "") or "").strip()
        legacy_enabled = bool(getattr(settings, "relay_enabled", False))
        legacy_reserve = str(getattr(settings, "relay_fee_reserve", "") or "").strip()
        legacy_has_explicit_value = active_mode in self.MODE_RELAY_STORAGE_KEYS and (legacy_enabled or bool(legacy_reserve))
        configs: dict[str, dict[str, object]] = {}
        for mode in (self.MODE_1M, self.MODE_M1):
            storage_key = self._relay_storage_key(mode)
            stored = raw_mode_relay_configs.get(storage_key) if storage_key else None
            if isinstance(stored, dict):
                configs[mode] = self._normalize_mode_relay_config(mode, stored)
                continue
            if mode == active_mode and legacy_has_explicit_value:
                configs[mode] = self._normalize_mode_relay_config(
                    mode,
                    {"relay_enabled": legacy_enabled, "relay_fee_reserve": legacy_reserve},
                )
                continue
            configs[mode] = self._default_mode_relay_config(mode)
        self._mode_relay_configs = configs
    def _store_mode_relay_config(self, mode: str, config: dict[str, object]) -> None:
        mode_key = str(mode or "").strip()
        if mode_key not in self.MODE_RELAY_STORAGE_KEYS:
            return
        self._mode_relay_configs[mode_key] = self._normalize_mode_relay_config(mode_key, config)
    def _capture_mode_relay_config(self, mode: str | None = None) -> None:
        mode_key = str(mode or getattr(self, "_last_mode_for_relay", "") or self._mode()).strip()
        if mode_key not in self.MODE_RELAY_STORAGE_KEYS:
            return
        self._store_mode_relay_config(
            mode_key,
            {
                "relay_enabled": bool(self.relay_enabled_var.get()),
                "relay_fee_reserve": self.relay_fee_reserve_var.get().strip(),
            },
        )
    def _apply_mode_relay_config(self, mode: str) -> None:
        if str(mode or "").strip() not in self.MODE_RELAY_STORAGE_KEYS:
            self.relay_enabled_var.set(False)
            self.relay_fee_reserve_var.set("")
            return
        config = self._normalize_mode_relay_config(mode, self._mode_relay_configs.get(str(mode or "").strip()))
        self.relay_enabled_var.set(bool(config.get("relay_enabled")))
        self.relay_fee_reserve_var.set(config["relay_fee_reserve"])
    def _mode_relay_configs_payload(
        self,
        *,
        existing_mode_relay_configs: dict[str, dict[str, object]] | None = None,
        current_mode: str | None = None,
        current_config: dict[str, object] | None = None,
    ) -> dict[str, dict[str, object]]:
        payload: dict[str, dict[str, object]] = {}
        existing_raw = existing_mode_relay_configs if isinstance(existing_mode_relay_configs, dict) else {}
        for mode, storage_key in self.MODE_RELAY_STORAGE_KEYS.items():
            candidate = current_config if str(current_mode or "").strip() == mode and current_config is not None else self._mode_relay_configs.get(mode)
            persisted = self._persistable_mode_relay_config(mode, candidate)
            if persisted is None:
                persisted = self._persistable_mode_relay_config(mode, existing_raw.get(storage_key))
            if persisted is None:
                persisted = self._default_mode_relay_config(mode)
            payload[storage_key] = dict(persisted)
        return payload
    def _relay_controls_visible(self) -> bool:
        return self._is_mode_1m() or self._is_mode_m1()
    def _relay_enabled(self) -> bool:
        return self._relay_controls_visible() and bool(self.relay_enabled_var.get())
    def _refresh_relay_controls(self) -> None:
        enabled = self._relay_enabled()
        try:
            self.ent_relay_fee_reserve.configure(state="normal" if enabled else "disabled")
        except Exception:
            pass
    def _on_relay_settings_changed(self, *_args) -> None:
        self._refresh_relay_controls()
        width = self.root.winfo_width() if hasattr(self, "root") and hasattr(self.root, "winfo_width") else 1500
        self._apply_setting_layout(self._layout_mode_for_width(width))
    def _relay_fee_reserve_label_text(self) -> str:
        if self._is_mode_m1():
            return "源钱包手续费预留"
        if self._is_mode_1m():
            return "中转手续费预留"
        return "预留原生币手续费"
    def _refresh_relay_fee_reserve_label(self) -> None:
        try:
            self.lbl_relay_fee_reserve.configure(text=self._relay_fee_reserve_label_text())
        except Exception:
            pass
    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)
    @classmethod
    def _decimal_to_text(cls, v: Decimal) -> str:
        return decimal_to_text(v)
    @staticmethod
    def _factor_by_decimals(decimals: int) -> Decimal:
        if decimals < 0 or decimals > OnchainTransferPageBase.MAX_TOKEN_DECIMALS:
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
        raw = self._runtime_state_snapshot().get("threads_raw", 10)
        return parse_worker_threads(raw, default=10)
    def _sync_runtime_state(self) -> None:
        threads_var = getattr(self, "threads_var", None)
        threads_raw = str(threads_var.get() if threads_var is not None else "10")
        with self._runtime_state_lock:
            self._runtime_state = {"threads_raw": threads_raw}
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
            session.trust_env = bool(allow_system_proxy) and not bool(proxies)
            if proxies:
                session.proxies.update(proxies)
            return session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        finally:
            session.close()
    def _allow_onchain_system_proxy(self) -> bool:
        state = self._onchain_proxy_state_snapshot()
        return not bool(state.get("use_config_proxy"))
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
    def _onchain_system_proxy_map() -> dict[str, str]:
        try:
            proxies = requests.utils.get_environ_proxies("https://gateway.tenderly.co/public/mainnet") or {}
        except Exception:
            return {}
        result = {}
        for key in ("http", "https"):
            value = str(proxies.get(key) or "").strip()
            if value:
                result[key] = value
        return result
    def _onchain_proxy_route_text(self, state: dict[str, object] | None = None) -> str:
        snapshot = dict(state or self._onchain_proxy_state_snapshot())
        raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        if not use_config_proxy:
            system_proxy = self._onchain_system_proxy_map()
            if system_proxy:
                return f"system-proxy -> {system_proxy.get('https') or system_proxy.get('http')}"
            return "direct"
        if not raw_proxy:
            return "direct"
        if raw_proxy.lower().startswith("ss://"):
            return "builtin-ss"
        return f"manual-proxy -> {self._normalize_onchain_proxy(raw_proxy)}"
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
        system_proxy = self._onchain_system_proxy_map() if not use_config_proxy else {}
        status = "跟随系统代理" if system_proxy else "未启用"
        exit_ip = "--"
        if use_config_proxy and raw_proxy:
            status = "SS代理连接中..." if raw_proxy.lower().startswith("ss://") else "代理连接中..."
        if use_config_proxy:
            # 触发代理规范化/内置 SS runtime 初始化
            self._onchain_proxy_map(state=snapshot)
        target = self._test_onchain_target_connectivity(selected_network)
        if use_config_proxy and raw_proxy:
            status = "SS代理已连接" if raw_proxy.lower().startswith("ss://") else "代理已连接"
        elif system_proxy:
            status = "系统代理已连接"
        else:
            status = "直连可用"
        if use_config_proxy:
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
        if self.use_config_proxy_var.get():
            raw_proxy = str(self.onchain_proxy_var.get() or "").strip()
            status = "待测试" if raw_proxy else "未启用"
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
    @staticmethod
    def _display_credential(credential: str) -> str:
        s = credential.strip()
        return s if s else "-"
    @staticmethod
    def _status_text(status: str) -> str:
        if status == "waiting":
            return "等待中"
        if status == "running":
            return "进行中"
        if status == "success":
            return "✅"
        if status == "failed":
            return "❌"
        if status == "submitted":
            return "确认中"
        if status == "incomplete":
            return "待处理"
        return "-"
    @staticmethod
    def _status_tag(status: str) -> str:
        if status == "waiting":
            return "st_waiting"
        if status == "running":
            return "st_running"
        if status == "success":
            return "st_success"
        if status == "failed":
            return "st_failed"
        if status == "submitted":
            return "st_submitted"
        if status == "incomplete":
            return "st_incomplete"
        return ""
    @staticmethod
    def _success_status_text(coin: str, amount_text: str) -> str:
        amount = str(amount_text or "").strip()
        coin_text = str(coin or "").strip().lower()
        if not amount:
            return "✅"
        if coin_text:
            return f"✅{amount}{coin_text}"
        return f"✅{amount}"
    def _status_text_for_row(self, row_key: str, status: str) -> str:
        custom = str(getattr(self, "row_status_text_map", {}).get(row_key, "") or "").strip()
        if custom and self._context_matches(row_key, getattr(self, "row_status_context", {})):
            return custom
        return self._status_text(status)
    def _recovery_status_text_for_row(self, row_key: str, status: str) -> str:
        custom = str(getattr(self, "row_recovery_text_map", {}).get(row_key, "") or "").strip()
        if custom and self._context_matches(row_key, getattr(self, "row_recovery_context", {})):
            return custom
        return self._status_text(status)
    @staticmethod
    def _m2m_key(item: OnchainPairEntry) -> str:
        return f"m2m:{item.source}|{item.target}"
    @staticmethod
    def _one_to_many_key(address: str) -> str:
        return f"1m:{address}"
    @staticmethod
    def _many_to_one_key(source: str) -> str:
        return f"m1:{source}"
    @staticmethod
    def _token_identity(token: EvmToken) -> tuple[str, str]:
        return token.symbol.strip().upper(), token.contract.strip().lower()
    def _token_display(self, token: EvmToken) -> str:
        if token.is_native:
            return f"{token.symbol}(原生)"
        return token.symbol
    def _balance_text_for_source(self, source: str) -> str:
        v = self.source_balance_cache.get(source)
        if v is None:
            return "-"
        symbol = self.symbol_var.get().strip() or "-"
        return f"{symbol}:{self._decimal_to_text(v)}"
    def _balance_text_for_target(self, target: str) -> str:
        v = self.target_balance_cache.get(target)
        if v is None:
            return "-"
        symbol = self.symbol_var.get().strip() or "-"
        return f"{symbol}:{self._decimal_to_text(v)}"
    def _source_addr_text(self, source: str) -> str:
        addr = self.source_address_cache.get(source, "")
        return addr if addr else "-"
    def _ensure_query_status_store(self) -> dict[str, str]:
        if not hasattr(self, "query_row_status"):
            self.query_row_status = {}
        return self.query_row_status
    def _display_status(self, row_key: str) -> str:
        query_status = self._ensure_query_status_store()
        if row_key in query_status and self._context_matches(row_key, getattr(self, "query_row_status_context", {})):
            return query_status.get(row_key, "")
        if self._context_matches(row_key, getattr(self, "row_status_context", {})):
            return getattr(self, "row_status", {}).get(row_key, "")
        return ""
    def _display_recovery_status(self, row_key: str) -> str:
        if self._context_matches(row_key, getattr(self, "row_recovery_context", {})):
            return getattr(self, "row_recovery_status", {}).get(row_key, "")
        return ""
    def _available_import_targets(self) -> dict[str, str]:
        mode = self._mode()
        if mode == self.MODE_1M:
            return {
                "full": "整行导入",
                "target": "接收地址列",
            }
        if mode == self.MODE_M1:
            return {
                "full": "整行导入",
                "source": "转出凭证列",
            }
        return {
            "full": "整行导入",
            "source": "转出凭证列",
            "target": "接收地址列",
        }
    def _current_import_target(self) -> str:
        target = str(getattr(self, "_import_target", "full") or "full")
        if target not in self._available_import_targets():
            target = "full"
            self._import_target = target
        return target
    def _set_import_target(self, target: str, *, log_change: bool = False):
        targets = self._available_import_targets()
        prev = self._current_import_target()
        if target not in targets:
            target = "full"
        self._import_target = target
        self._apply_import_target_view()
        try:
            self.tree.focus_set()
        except Exception:
            pass
        if log_change and prev != target:
            self.log(f"已切换粘贴目标：{targets[target]}")
    def _apply_import_target_view(self):
        target = self._current_import_target()
        importable = set(self._available_import_targets()) - {"full"}
        for column, text in self._tree_heading_base_texts.items():
            self.tree.heading(column, text=heading_text(text, active=(column == target and column in importable)))
        update_import_target_bar(self.import_target_bar, self.tree, self._tree_column_ids, target)
    def _on_tree_pointer_down(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region == "separator":
            return None
        if region == "heading":
            column_id = self.tree.identify_column(event.x)
            column = column_name_from_identifier(self._tree_column_ids, column_id)
            self._set_import_target(column if column in self._available_import_targets() else "full", log_change=True)
            return None
        self._set_import_target("full", log_change=True)
        return None
    def _selected_draft_index(self) -> int | None:
        tree = getattr(self, "tree", None)
        if tree is None or not hasattr(tree, "selection"):
            return None
        selected = tuple(tree.selection())
        if not selected:
            return None
        return getattr(self, "draft_row_index_map", {}).get(selected[0])
    def _ensure_checked_m2m_draft_rows(self) -> set[int]:
        checked = getattr(self, "checked_m2m_draft_rows", None)
        if checked is None:
            checked = set()
            self.checked_m2m_draft_rows = checked
        return checked
    def _normalize_context_target(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            return self._validate_recipient_address(raw, "收款地址")
        except Exception:
            return raw
    def _current_row_context(self, row_key: str) -> str:
        key = str(row_key or "").strip()
        if key.startswith("1m:"):
            if not hasattr(self, "source_credential_var"):
                return ""
            return self.source_credential_var.get().strip()
        if key.startswith("m1:"):
            if not hasattr(self, "target_address_var"):
                return ""
            return self._normalize_context_target(self.target_address_var.get().strip())
        return ""
    def _row_context_for_values(self, row_key: str, source: str = "", target: str = "") -> str:
        key = str(row_key or "").strip()
        if key.startswith("1m:"):
            return str(source or "").strip()
        if key.startswith("m1:"):
            return self._normalize_context_target(target)
        return ""
    def _context_matches(self, row_key: str, context_map: dict[str, str]) -> bool:
        key = str(row_key or "").strip()
        if key not in context_map:
            return True
        return context_map.get(key, "") == self._current_row_context(key)
    def _mark_row_status_context(self, row_key: str, context_sig: str) -> None:
        key = str(row_key or "").strip()
        if not hasattr(self, "row_status_context"):
            self.row_status_context = {}
        if key.startswith("m2m:"):
            self.row_status_context.pop(key, None)
            return
        self.row_status_context[key] = str(context_sig or "")
    def _mark_recovery_status_context(self, row_key: str, context_sig: str) -> None:
        key = str(row_key or "").strip()
        if not hasattr(self, "row_recovery_context"):
            self.row_recovery_context = {}
        if key.startswith("m2m:"):
            self.row_recovery_context.pop(key, None)
            return
        self.row_recovery_context[key] = str(context_sig or "")
    def _mark_query_status_context(self, row_key: str, context_sig: str) -> None:
        key = str(row_key or "").strip()
        if not hasattr(self, "query_row_status_context"):
            self.query_row_status_context = {}
        if key.startswith("m2m:"):
            self.query_row_status_context.pop(key, None)
            return
        self.query_row_status_context[key] = str(context_sig or "")
    def _mark_query_status_contexts(self, row_keys: list[str], context_sig: str) -> None:
        for row_key in row_keys:
            self._mark_query_status_context(row_key, context_sig)
    def _validate_recipient_address(self, value: str, field_label: str = "收款地址") -> str:
        validator = getattr(self.client, "validate_evm_address", None)
        if callable(validator):
            return str(validator(value, field_label))
        raw = str(value or "").strip()
        if not getattr(self.client, "is_address", lambda _v: False)(raw):
            raise RuntimeError(f"{field_label}格式错误：{raw}")
        return raw
    def _try_validate_recipient_address(self, value: str, field_label: str = "收款地址") -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        return self._validate_recipient_address(raw, field_label)
    def _warn_if_draft_selected(self) -> bool:
        if self._selected_draft_index() is None:
            return False
        messagebox.showwarning("提示", "当前行为待补齐数据，请先补齐转出凭证和接收地址")
        return True
    def _promote_complete_m2m_drafts(self) -> tuple[int, int]:
        ready: list[OnchainPairEntry] = []
        remaining: list[dict[str, str]] = []
        for row in self.m2m_import_drafts:
            source = str(row.get("source", "") or "").strip()
            target = str(row.get("target", "") or "").strip()
            if source and target:
                ready.append(OnchainPairEntry(source=source, target=target))
            else:
                remaining.append(
                    {
                        "source": source,
                        "target": target,
                    }
                )
        self.m2m_import_drafts = remaining
        if not ready:
            return 0, 0
        created = self.store.upsert_multi_to_multi(ready)
        return len(ready), created
    def _import_m2m_column(self, field: str, lines: list[str], source_name: str):
        if field == "target":
            values = [
                self._validate_recipient_address(value, f"第 {i} 行接收地址")
                for i, value in enumerate(parse_single_value_lines(lines, "接收地址"), start=1)
            ]
        else:
            values = parse_single_value_lines(lines, "转出凭证", allow_inner_whitespace=True)
        if not values:
            messagebox.showwarning("提示", "没有可导入的数据")
            return
        merge_column_values(self.m2m_import_drafts, ("source", "target"), field, values)
        completed, created = self._promote_complete_m2m_drafts()
        if completed:
            self.checked_row_keys = set(self._active_row_keys())
        self._refresh_tree()
        self._persist_import_rows()
        parts = [f"{source_name}导入完成：写入 {len(values)} 条{self._available_import_targets()[field]}"]
        if completed:
            parts.append(f"补齐 {completed} 条")
        if created:
            parts.append(f"新增 {created} 条")
        if self.m2m_import_drafts:
            parts.append(f"待补齐 {len(self.m2m_import_drafts)} 行")
        self.log("，".join(parts))
    def _empty_hint_text(self) -> str:
        mode = self._mode()
        if mode == self.MODE_1M:
            return (
                "导入格式\n"
                "每行一个接收地址\n\n"
                "导入方式\n"
                "先在上方填写转出钱包私钥或助记词\n"
                "点击中间空白处后按 Cmd+V / Ctrl+V 可按原格式导入\n"
                "点击“接收地址”表头后可按列粘贴，也可继续使用“粘贴导入”/“导入 TXT”"
            )
        if mode == self.MODE_M1:
            return (
                "导入格式\n"
                "每行一个转出钱包私钥或助记词\n\n"
                "导入方式\n"
                "先在上方填写收款地址\n"
                "点击中间空白处后按 Cmd+V / Ctrl+V 可按原格式导入\n"
                "点击“转出凭证”表头后可按列粘贴，也可继续使用“粘贴导入”/“导入 TXT”"
            )
        return (
            "导入格式\n"
            "每行：转出钱包私钥或助记词 接收地址\n\n"
            "导入方式\n"
            "点击中间空白处后按 Cmd+V / Ctrl+V 可按原格式导入\n"
            "点击“转出凭证”或“接收地址”表头后可按列粘贴\n"
            "也可继续使用“粘贴导入”或“导入 TXT”"
        )
    def _update_empty_hint(self):
        label = getattr(self, "empty_hint_label", None)
        if label is None:
            return
        width = 0
        if hasattr(self, "table_wrap") and hasattr(self.table_wrap, "winfo_width"):
            try:
                width = int(self.table_wrap.winfo_width())
            except Exception:
                width = 0
        wraplength = max(340, width - 120) if width > 0 else 620
        try:
            label.configure(text=self._empty_hint_text(), wraplength=wraplength)
        except Exception:
            pass

        if self._is_mode_m2m():
            has_rows = bool(self.store.multi_to_multi_pairs) or bool(getattr(self, "m2m_import_drafts", []))
        elif self._is_mode_1m():
            has_rows = bool(self.store.one_to_many_addresses)
        else:
            has_rows = bool(self.store.many_to_one_sources)
        try:
            if has_rows:
                label.place_forget()
            else:
                label.place(relx=0.5, rely=0.45, anchor="center")
                if hasattr(label, "lift"):
                    label.lift()
        except Exception:
            pass
    def _update_row_view(self, row_key: str):
        if not hasattr(self, "tree"):
            return
        row_id = self.row_id_by_key.get(row_key)
        if not row_id or row_id not in self.row_index_map:
            return

        idx = self.row_index_map[row_id]
        checked = "✓" if row_key in self.checked_row_keys else ""
        status = self._display_status(row_key)
        recovery_status = self._display_recovery_status(row_key)
        mode = self._mode()

        if mode == self.MODE_M2M:
            if not (0 <= idx < len(self.store.multi_to_multi_pairs)):
                return
            item = self.store.multi_to_multi_pairs[idx]
            values = (
                checked,
                idx + 1,
                self._display_credential(item.source),
                item.target,
                self._status_text_for_row(row_key, status),
                self._recovery_status_text_for_row(row_key, recovery_status),
                self._balance_text_for_source(item.source),
            )
        elif mode == self.MODE_1M:
            if not (0 <= idx < len(self.store.one_to_many_addresses)):
                return
            source = self.source_credential_var.get().strip()
            target = self.store.one_to_many_addresses[idx]
            values = (
                checked,
                idx + 1,
                self._display_credential(source),
                target,
                self._status_text_for_row(row_key, status),
                self._recovery_status_text_for_row(row_key, recovery_status),
                self._balance_text_for_target(target),
            )
        else:
            if not (0 <= idx < len(self.store.many_to_one_sources)):
                return
            source = self.store.many_to_one_sources[idx]
            target = self.target_address_var.get().strip()
            values = (
                checked,
                idx + 1,
                self._display_credential(source),
                target if target else "-",
                self._status_text_for_row(row_key, status),
                self._recovery_status_text_for_row(row_key, recovery_status),
                self._balance_text_for_source(source),
            )

        self.tree.item(row_id, values=values)
        tag = self._status_tag(status)
        self.tree.item(row_id, tags=((tag,) if tag else ()))
    def _update_rows_view(self, row_keys: list[str]):
        for row_key in row_keys:
            self._update_row_view(row_key)
    @staticmethod
    def _unique_row_keys(row_keys: list[str]) -> list[str]:
        return task_progress.unique_keys(row_keys)
    def _progress_store(self, kind: str) -> dict[str, str]:
        if kind == "query":
            return self._ensure_query_status_store()
        if kind == "recovery":
            return self.row_recovery_status
        return self.row_status
    def _begin_progress(self, kind: str, row_keys: list[str]):
        task_progress.begin(self, kind, row_keys, amount_label="转账总额")
    def _progress_counts(self, kind: str, row_keys: list[str]) -> tuple[int, int, int, int, int]:
        return task_progress.progress_counts(self, kind, row_keys)
    def _refresh_progress_display(self):
        task_progress.refresh_display(self)
    def _refresh_progress_if_active(self, kind: str, row_key: str):
        task_progress.refresh_if_active(self, kind, row_key)
    def _dispatch_ui(self, callback) -> None:
        if self._closing or bool(getattr(self.root, "_closing", False)):
            return
        dispatch_ui_callback(self, callback)
    def _start_managed_thread(self, target, *, args=(), kwargs=None, name: str = "onchain-bg", daemon: bool = True) -> threading.Thread:
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
    def _schedule_tree_refresh(self) -> None:
        schedule_ui_callback(self, "tree_refresh", self._refresh_tree, root=getattr(self, "root", None))
    def shutdown(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.stop_requested.set()
        try:
            self._close_wallet_generator()
        except Exception:
            pass
        self._join_managed_threads(timeout_total=1.0)
        try:
            self.client.close()
        except Exception:
            pass
        stop_ui_bridge(self)
    def _finish_progress(self, kind: str, success: int, failed: int):
        flush_queued_ui_renders(self)
        log_tree = getattr(self, "log_tree", None)
        if log_tree is not None:
            flush_queued_log_rows(self, log_tree, max_rows=LOG_MAX_ROWS)
        clear_ui_batch_size(self)
        task_progress.finish(self, kind, success, failed)
    def _show_result_summary_dialog(
        self,
        *,
        title: str,
        summary_title: str,
        success: int,
        failed: int,
        detail_text: str = "",
        action_buttons: list[tuple[str, object, bool]] | None = None,
        success_label: str = "成功",
        failed_label: str = "失败",
    ) -> None:
        parent = getattr(self, "root", None) or self.parent
        dialog = tk.Toplevel(parent)
        dialog.title(str(title or "执行完成"))
        dialog.transient(parent)
        dialog.resizable(False, False)
        dialog.grab_set()

        body = ttk.Frame(dialog, padding=16)
        body.pack(fill=BOTH, expand=True)

        ttk.Label(body, text=str(summary_title or "执行完成"), font=("", 11, "bold")).pack(anchor="w")

        counts = ttk.Frame(body)
        counts.pack(anchor="w", pady=(10, 0))
        ttk.Label(counts, text=f"{str(success_label or '成功')}：").pack(side=LEFT)
        tk.Label(counts, text=str(int(success)), fg="#1E8449").pack(side=LEFT)
        ttk.Label(counts, text=f"   {str(failed_label or '失败')}：").pack(side=LEFT)
        tk.Label(counts, text=str(int(failed)), fg="#C62828").pack(side=LEFT)

        detail = str(detail_text or "").strip()
        if detail:
            ttk.Label(body, text=detail, foreground="#666666", wraplength=420, justify="left").pack(anchor="w", pady=(10, 0))

        btn_row = ttk.Frame(body)
        btn_row.pack(fill="x", pady=(14, 0))
        for label, callback, enabled in list(action_buttons or []):
            def _run_action(cb=callback):
                try:
                    dialog.destroy()
                except Exception:
                    pass
                if callable(cb):
                    cb()

            ttk.Button(
                btn_row,
                text=str(label or "操作"),
                command=_run_action,
                state=("normal" if enabled else "disabled"),
            ).pack(side=RIGHT, padx=(8, 0))
        ttk.Button(btn_row, text="确定", command=dialog.destroy).pack(side=RIGHT)

        dialog.update_idletasks()
        try:
            parent_x = parent.winfo_rootx()
            parent_y = parent.winfo_rooty()
            parent_w = parent.winfo_width()
            parent_h = parent.winfo_height()
            width = dialog.winfo_width()
            height = dialog.winfo_height()
            x = parent_x + max(0, (parent_w - width) // 2)
            y = parent_y + max(0, (parent_h - height) // 2)
            dialog.geometry(f"+{x}+{y}")
        except Exception:
            pass
        dialog.focus_set()
    def _set_progress_metrics(
        self,
        *,
        balance_text: str | None = None,
        amount_text: str | None = None,
        gas_text: str | None = None,
    ):
        task_progress.set_metrics(self, balance_text=balance_text, amount_text=amount_text, gas_text=gas_text)
    def _set_status(self, row_key: str, status: str, status_text: str = ""):
        self._ensure_query_status_store().pop(row_key, None)
        self.row_status[row_key] = status
        text = str(status_text or "").strip()
        if text:
            self.row_status_text_map[row_key] = text
        else:
            self.row_status_text_map.pop(row_key, None)
        queue_ui_render(self, lambda k=row_key: self._update_row_view(k), root=getattr(self, "root", None))
        self._refresh_progress_if_active("transfer", row_key)
    def _set_recovery_status(self, row_key: str, status: str, status_text: str = ""):
        self.row_recovery_status[row_key] = status
        text = str(status_text or "").strip()
        if text:
            self.row_recovery_text_map[row_key] = text
        else:
            self.row_recovery_text_map.pop(row_key, None)
        queue_ui_render(self, lambda k=row_key: self._update_row_view(k), root=getattr(self, "root", None))
        self._refresh_progress_if_active("recovery", row_key)
    def _set_query_status(self, row_key: str, status: str):
        self._ensure_query_status_store()[row_key] = status
        queue_ui_render(self, lambda k=row_key: self._update_row_view(k), root=getattr(self, "root", None))
        self._refresh_progress_if_active("query", row_key)
    def _set_query_statuses(self, row_keys: list[str], status: str):
        for row_key in row_keys:
            self._set_query_status(row_key, status)
    def _apply_source_balance_summary(self, context_sig: str, text: str) -> None:
        if context_sig == self.source_credential_var.get().strip():
            self.source_balance_var.set(text)
    def _clear_source_balance_summary(self, context_sig: str) -> None:
        if context_sig == self.source_credential_var.get().strip():
            self.source_balance_var.set("-")
    def _apply_target_balance_summary(self, context_sig: str, text: str) -> None:
        if context_sig == self._normalize_context_target(self.target_address_var.get().strip()):
            self.target_balance_var.set(text)
    def _clear_target_balance_summary(self, context_sig: str) -> None:
        if context_sig == self._normalize_context_target(self.target_address_var.get().strip()):
            self.target_balance_var.set("-")
    def _active_row_keys(self) -> list[str]:
        if self._is_mode_m2m():
            return [self._m2m_key(x) for x in self.store.multi_to_multi_pairs]
        if self._is_mode_1m():
            return [self._one_to_many_key(x) for x in self.store.one_to_many_addresses]
        return [self._many_to_one_key(x) for x in self.store.many_to_one_sources]
    def _refresh_tree(self):
        self.tree.delete(*self.tree.get_children())
        self.row_index_map = {}
        self.row_key_by_row_id = {}
        self.row_id_by_key = {}
        self.draft_row_index_map = {}

        active = set(self._active_row_keys())
        self.checked_row_keys.intersection_update(active)
        self._ensure_checked_m2m_draft_rows().intersection_update(set(range(len(self.m2m_import_drafts))))
        self.row_status = {k: v for k, v in self.row_status.items() if k in active}
        self.row_status_text_map = {k: v for k, v in self.row_status_text_map.items() if k in active}
        self.row_status_context = {k: v for k, v in self.row_status_context.items() if k in active}
        self.row_recovery_status = {k: v for k, v in self.row_recovery_status.items() if k in active}
        self.row_recovery_text_map = {k: v for k, v in self.row_recovery_text_map.items() if k in active}
        self.row_recovery_context = {k: v for k, v in self.row_recovery_context.items() if k in active}
        self.query_row_status = {k: v for k, v in self._ensure_query_status_store().items() if k in active}
        self.query_row_status_context = {k: v for k, v in self.query_row_status_context.items() if k in active}

        mode = self._mode()
        if mode == self.MODE_M2M:
            for i, item in enumerate(self.store.multi_to_multi_pairs, start=1):
                key = self._m2m_key(item)
                row_id = f"onchain_row_{i}"
                self.row_index_map[row_id] = i - 1
                self.row_key_by_row_id[row_id] = key
                self.row_id_by_key[key] = row_id
                checked = "✓" if key in self.checked_row_keys else ""
                st = self._display_status(key)
                tag = self._status_tag(st)
                self.tree.insert(
                    "",
                    END,
                    iid=row_id,
                    values=(
                        checked,
                        i,
                        self._display_credential(item.source),
                        item.target,
                        self._status_text_for_row(key, st),
                        self._recovery_status_text_for_row(key, self._display_recovery_status(key)),
                        self._balance_text_for_source(item.source),
                    ),
                    tags=((tag,) if tag else ()),
                )
            start_index = len(self.store.multi_to_multi_pairs) + 1
            for i, row in enumerate(self.m2m_import_drafts, start=start_index):
                row_id = f"onchain_draft_{i}"
                self.draft_row_index_map[row_id] = i - start_index
                draft_idx = i - start_index
                self.tree.insert(
                    "",
                    END,
                    iid=row_id,
                    values=(
                        "✓" if draft_idx in self._ensure_checked_m2m_draft_rows() else "",
                        i,
                        self._display_credential(row.get("source", "")),
                        row.get("target", "") or "-",
                        "待补齐",
                        "-",
                        "-",
                    ),
                    tags=("st_incomplete",),
                )
            self._apply_import_target_view()
            self._update_empty_hint()
            return

        if mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            for i, target in enumerate(self.store.one_to_many_addresses, start=1):
                key = self._one_to_many_key(target)
                row_id = f"onchain_row_{i}"
                self.row_index_map[row_id] = i - 1
                self.row_key_by_row_id[row_id] = key
                self.row_id_by_key[key] = row_id
                checked = "✓" if key in self.checked_row_keys else ""
                st = self._display_status(key)
                tag = self._status_tag(st)
                self.tree.insert(
                    "",
                    END,
                    iid=row_id,
                    values=(
                        checked,
                        i,
                        self._display_credential(source),
                        target,
                        self._status_text_for_row(key, st),
                        self._recovery_status_text_for_row(key, self._display_recovery_status(key)),
                        self._balance_text_for_target(target),
                    ),
                    tags=((tag,) if tag else ()),
                )
            self._apply_import_target_view()
            self._update_empty_hint()
            return

        target = self.target_address_var.get().strip()
        for i, source in enumerate(self.store.many_to_one_sources, start=1):
            key = self._many_to_one_key(source)
            row_id = f"onchain_row_{i}"
            self.row_index_map[row_id] = i - 1
            self.row_key_by_row_id[row_id] = key
            self.row_id_by_key[key] = row_id
            checked = "✓" if key in self.checked_row_keys else ""
            st = self._display_status(key)
            tag = self._status_tag(st)
            self.tree.insert(
                "",
                END,
                iid=row_id,
                values=(
                    checked,
                    i,
                    self._display_credential(source),
                    target if target else "-",
                    self._status_text_for_row(key, st),
                    self._recovery_status_text_for_row(key, self._display_recovery_status(key)),
                    self._balance_text_for_source(source),
                ),
                tags=((tag,) if tag else ()),
            )
        self._apply_import_target_view()
        self._update_empty_hint()
    def _selected_indices(self) -> list[int]:
        idxs: list[int] = []
        for row_id in self.tree.selection():
            if row_id in self.row_index_map:
                idxs.append(self.row_index_map[row_id])
        return sorted(set(idxs))
    def _on_tree_click(self, event):
        if self.tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return None
        draft_idx = getattr(self, "draft_row_index_map", {}).get(row_id)
        if draft_idx is not None and self._is_mode_m2m():
            checked_rows = self._ensure_checked_m2m_draft_rows()
            checked = draft_idx not in checked_rows
            if checked:
                checked_rows.add(draft_idx)
            else:
                checked_rows.discard(draft_idx)
            values = list(self.tree.item(row_id, "values"))
            if values:
                values[0] = "✓" if checked else ""
                self.tree.item(row_id, values=values)
            return None
        key = self.row_key_by_row_id.get(row_id, "")
        if not key:
            return "break"
        checked = key not in self.checked_row_keys
        if checked:
            self.checked_row_keys.add(key)
        else:
            self.checked_row_keys.discard(key)
        values = list(self.tree.item(row_id, "values"))
        if values:
            values[0] = "✓" if checked else ""
            self.tree.item(row_id, values=values)
        return None
    def _on_tree_right_click(self, event):
        if self.tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return None
        self.tree.selection_set(row_id)
        self.tree.focus(row_id)
        try:
            self.row_menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                self.row_menu.grab_release()
            except Exception:
                pass
        return "break"
    def _single_selected_index(self) -> int | None:
        idxs = self._selected_indices()
        if not idxs:
            return None
        return idxs[0]
    def _single_row_job(self) -> tuple[str, str, str] | None:
        idx = self._single_selected_index()
        if idx is None:
            return None
        mode = self._mode()
        if mode == self.MODE_M2M:
            if not (0 <= idx < len(self.store.multi_to_multi_pairs)):
                return None
            item = self.store.multi_to_multi_pairs[idx]
            return self._m2m_key(item), item.source, item.target
        if mode == self.MODE_1M:
            if not (0 <= idx < len(self.store.one_to_many_addresses)):
                return None
            target = self.store.one_to_many_addresses[idx]
            source = self.source_credential_var.get().strip()
            return self._one_to_many_key(target), source, target
        if not (0 <= idx < len(self.store.many_to_one_sources)):
            return None
        source = self.store.many_to_one_sources[idx]
        target = self.target_address_var.get().strip()
        return self._many_to_one_key(source), source, target
    def _on_tree_paste(self, _event=None):
        self.import_from_clipboard()
        return "break"
    def _amount_mode(self) -> str:
        m = self.amount_mode_var.get().strip()
        if m not in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL}:
            m = self.AMOUNT_MODE_FIXED
            self.amount_mode_var.set(m)
        return m
    def _apply_amount_layout(self):
        for w in (self.amount_mode_box, self.ent_amount, self.ent_random_min, self.lbl_random_sep, self.ent_random_max, self.lbl_amount_all_hint):
            w.pack_forget()
        self.amount_mode_box.pack(side=LEFT)
        m = self._amount_mode()
        if m == self.AMOUNT_MODE_FIXED:
            self.ent_amount.pack(side=LEFT, padx=(4, 0))
        elif m == self.AMOUNT_MODE_RANDOM:
            self.ent_random_min.pack(side=LEFT, padx=(4, 0))
            self.lbl_random_sep.pack(side=LEFT, padx=(2, 2))
            self.ent_random_max.pack(side=LEFT)
        else:
            self.lbl_amount_all_hint.pack(side=LEFT, padx=(4, 0))
    def _on_amount_mode_changed(self, *_args):
        self._apply_amount_layout()
    @staticmethod
    def _layout_mode_for_width(width: int) -> str:
        if width < 920:
            return "narrow"
        if width < 1360:
            return "medium"
        return "wide"
    def _apply_setting_layout(self, layout_mode: str):
        if layout_mode not in {"wide", "medium", "narrow"}:
            layout_mode = "wide"
        self._layout_mode = layout_mode
        if layout_mode == "wide":
            self.mode_box.configure(width=8)
            self.network_box.configure(width=6)
            self.coin_box.configure(width=12)
            self.amount_mode_box.configure(width=7)
            self.ent_amount.configure(width=7)
            self.ent_random_min.configure(width=6)
            self.ent_random_max.configure(width=6)
        elif layout_mode == "medium":
            self.mode_box.configure(width=7)
            self.network_box.configure(width=5)
            self.coin_box.configure(width=11)
            self.amount_mode_box.configure(width=6)
            self.ent_amount.configure(width=6)
            self.ent_random_min.configure(width=5)
            self.ent_random_max.configure(width=5)
        else:
            self.mode_box.configure(width=10)
            self.network_box.configure(width=8)
            self.coin_box.configure(width=13)
            self.amount_mode_box.configure(width=7)
            self.ent_amount.configure(width=7)
            self.ent_random_min.configure(width=6)
            self.ent_random_max.configure(width=6)
        widgets = [
            self.lbl_mode,
            self.mode_box,
            self.lbl_network,
            self.network_box,
            self.lbl_coin,
            self.coin_box,
            self.lbl_contract_search,
            self.ent_contract_search,
            self.btn_contract_search,
            self.lbl_amount,
            self.amount_ctrl,
            self.chk_dry_run,
            self.chk_use_config_proxy,
            self.btn_save_all,
            self.lbl_delay,
            self.ent_delay,
            self.lbl_threads,
            self.spin_threads,
            self.lbl_confirm_timeout,
            self.ent_confirm_timeout,
            self.lbl_source_credential,
            self.ent_source_credential,
            self.btn_query_source_balance,
            self.lbl_source_balance_title,
            self.lbl_source_balance_val,
            self.chk_relay_enabled,
            self.lbl_relay_fee_reserve,
            self.ent_relay_fee_reserve,
            self.lbl_relay_wallet_file,
            self.lbl_target_address,
            self.ent_target_address,
            self.btn_query_target_balance,
            self.lbl_target_balance_title,
            self.lbl_target_balance_val,
        ]
        for w in widgets:
            w.grid_forget()

        for c in range(14):
            self.setting_frame.columnconfigure(c, weight=0)

        show_source = self._is_mode_1m()
        show_target = self._is_mode_m1()
        show_relay = self._relay_controls_visible()

        if layout_mode == "wide":
            self.lbl_mode.grid(row=0, column=0, sticky=W)
            self.mode_box.grid(row=0, column=1, sticky=W, padx=(4, 10))
            self.lbl_network.grid(row=0, column=2, sticky=W)
            self.network_box.grid(row=0, column=3, sticky=W, padx=(4, 10))
            self.lbl_coin.grid(row=0, column=4, sticky=W)
            self.coin_box.grid(row=0, column=5, sticky=W, padx=(4, 10))
            self.lbl_amount.grid(row=0, column=6, sticky=W)
            self.amount_ctrl.grid(row=0, column=7, sticky=W, padx=(4, 10))
            self.chk_dry_run.grid(row=0, column=8, sticky=W, padx=(4, 0))
            self.chk_use_config_proxy.grid(row=0, column=9, sticky=W, padx=(10, 0))
            self.btn_save_all.grid(row=0, column=10, sticky=W, padx=(8, 0))

            self.lbl_contract_search.grid(row=1, column=0, sticky=W, pady=(8, 0))
            self.ent_contract_search.grid(row=1, column=1, columnspan=6, sticky="ew", padx=(4, 10), pady=(8, 0))
            self.btn_contract_search.grid(row=1, column=7, sticky=W, pady=(8, 0))
            self.lbl_delay.grid(row=1, column=8, sticky=W, pady=(8, 0))
            self.ent_delay.grid(row=1, column=9, sticky=W, padx=(4, 10), pady=(8, 0))
            self.lbl_threads.grid(row=1, column=10, sticky=W, pady=(8, 0))
            self.spin_threads.grid(row=1, column=11, sticky=W, padx=(4, 0), pady=(8, 0))
            if not show_relay:
                self.lbl_confirm_timeout.grid(row=1, column=12, sticky=W, pady=(8, 0))
                self.ent_confirm_timeout.grid(row=1, column=13, sticky=W, padx=(4, 0), pady=(8, 0))
            self.setting_frame.columnconfigure(1, weight=1)
            self.setting_frame.columnconfigure(3, weight=1)
            self.setting_frame.columnconfigure(5, weight=1)
            self.setting_frame.columnconfigure(6, weight=1)
            self.setting_frame.columnconfigure(11, weight=1)
            if not show_relay:
                self.setting_frame.columnconfigure(13, weight=1)

            row = 2
            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=4, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=5, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=6, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=7, columnspan=3, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1

            if show_relay:
                self.chk_relay_enabled.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.lbl_relay_fee_reserve.grid(row=row, column=1, sticky=W, pady=(8, 0))
                self.ent_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(4, 10), pady=(8, 0))
                self.lbl_confirm_timeout.grid(row=row, column=3, sticky=W, pady=(8, 0))
                self.ent_confirm_timeout.grid(row=row, column=4, sticky=W, padx=(4, 10), pady=(8, 0))
                row += 1
                self.lbl_relay_wallet_file.grid(row=row, column=0, columnspan=11, sticky=W, pady=(6, 0))
                row += 1

            if show_target:
                self.lbl_target_address.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_target_address.grid(row=row, column=1, columnspan=4, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_target_balance.grid(row=row, column=5, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_target_balance_title.grid(row=row, column=6, sticky=W, pady=(8, 0))
                self.lbl_target_balance_val.grid(row=row, column=7, columnspan=3, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1
            return

        if layout_mode == "medium":
            self.lbl_mode.grid(row=0, column=0, sticky=W)
            self.mode_box.grid(row=0, column=1, sticky=W, padx=(4, 10))
            self.lbl_network.grid(row=0, column=2, sticky=W)
            self.network_box.grid(row=0, column=3, sticky=W, padx=(4, 10))
            self.lbl_coin.grid(row=0, column=4, sticky=W)
            self.coin_box.grid(row=0, column=5, sticky=W, padx=(4, 10))
            self.lbl_amount.grid(row=0, column=6, sticky=W)
            self.amount_ctrl.grid(row=0, column=7, sticky=W, padx=(4, 0))
            self.chk_dry_run.grid(row=0, column=8, sticky=W, padx=(10, 0))
            self.chk_use_config_proxy.grid(row=0, column=9, sticky=W, padx=(10, 0))
            self.btn_save_all.grid(row=0, column=10, sticky=W, padx=(8, 0))

            self.lbl_contract_search.grid(row=1, column=0, sticky=W, pady=(8, 0))
            self.ent_contract_search.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(4, 8), pady=(8, 0))
            self.btn_contract_search.grid(row=1, column=6, sticky=W, pady=(8, 0))
            self.lbl_delay.grid(row=1, column=7, sticky=W, padx=(10, 0), pady=(8, 0))
            self.ent_delay.grid(row=1, column=8, sticky=W, padx=(4, 10), pady=(8, 0))
            self.lbl_threads.grid(row=1, column=9, sticky=W, pady=(8, 0))
            self.spin_threads.grid(row=1, column=10, sticky=W, padx=(4, 0), pady=(8, 0))
            row = 2
            if not show_relay:
                self.lbl_confirm_timeout.grid(row=row, column=7, sticky=W, padx=(10, 0), pady=(8, 0))
                self.ent_confirm_timeout.grid(row=row, column=8, sticky=W, padx=(4, 10), pady=(8, 0))
                row += 1

            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=3, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=4, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=5, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=6, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1

            if show_relay:
                self.chk_relay_enabled.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.lbl_relay_fee_reserve.grid(row=row, column=1, sticky=W, pady=(8, 0))
                self.ent_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(4, 8), pady=(8, 0))
                self.lbl_confirm_timeout.grid(row=row, column=3, sticky=W, pady=(8, 0))
                self.ent_confirm_timeout.grid(row=row, column=4, sticky=W, padx=(4, 8), pady=(8, 0))
                row += 1
                self.lbl_relay_wallet_file.grid(row=row, column=0, columnspan=10, sticky=W, pady=(6, 0))
                row += 1

            if show_target:
                self.lbl_target_address.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_target_address.grid(row=row, column=1, columnspan=3, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_target_balance.grid(row=row, column=4, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_target_balance_title.grid(row=row, column=5, sticky=W, pady=(8, 0))
                self.lbl_target_balance_val.grid(row=row, column=6, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))

            self.setting_frame.columnconfigure(1, weight=1)
            self.setting_frame.columnconfigure(3, weight=1)
            self.setting_frame.columnconfigure(5, weight=1)
            return

        row = 0
        self.lbl_mode.grid(row=row, column=0, sticky=W)
        self.mode_box.grid(row=row, column=1, sticky="ew", padx=(4, 0))
        row += 1

        self.lbl_network.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.network_box.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
        row += 1

        self.lbl_coin.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.coin_box.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
        row += 1

        self.lbl_amount.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.amount_ctrl.grid(row=row, column=1, sticky=W, padx=(4, 0), pady=(8, 0))
        row += 1

        self.lbl_contract_search.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.ent_contract_search.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
        row += 1
        self.btn_contract_search.grid(row=row, column=1, sticky=W, pady=(6, 0))
        row += 1

        self.lbl_delay.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.ent_delay.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
        row += 1

        self.lbl_threads.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.spin_threads.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
        row += 1
        if not show_relay:
            self.lbl_confirm_timeout.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_confirm_timeout.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1

        self.chk_dry_run.grid(row=row, column=0, columnspan=2, sticky=W, pady=(8, 0))
        row += 1
        self.chk_use_config_proxy.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.btn_save_all.grid(row=row, column=1, sticky=W, padx=(8, 0), pady=(8, 0))
        row += 1

        if show_source:
            self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_source_credential.grid(row=row, column=1, sticky="ew", padx=(4, 6), pady=(8, 0))
            self.btn_query_source_balance.grid(row=row, column=2, sticky=W, padx=(0, 6), pady=(8, 0))
            self.lbl_source_balance_title.grid(row=row, column=3, sticky=W, pady=(8, 0))
            self.lbl_source_balance_val.grid(row=row, column=4, sticky=W, padx=(4, 0), pady=(8, 0))
            row += 1

        if show_relay:
            self.chk_relay_enabled.grid(row=row, column=0, columnspan=2, sticky=W, pady=(8, 0))
            self.lbl_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(10, 0), pady=(8, 0))
            self.ent_relay_fee_reserve.grid(row=row, column=3, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1
            self.lbl_confirm_timeout.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_confirm_timeout.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1
            self.lbl_relay_wallet_file.grid(row=row, column=0, columnspan=5, sticky=W, pady=(6, 0))
            row += 1

        if show_target:
            self.lbl_target_address.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_target_address.grid(row=row, column=1, sticky="ew", padx=(4, 6), pady=(8, 0))
            self.btn_query_target_balance.grid(row=row, column=2, sticky=W, padx=(0, 6), pady=(8, 0))
            self.lbl_target_balance_title.grid(row=row, column=3, sticky=W, pady=(8, 0))
            self.lbl_target_balance_val.grid(row=row, column=4, sticky=W, padx=(4, 0), pady=(8, 0))

        self.setting_frame.columnconfigure(0, weight=0)
        self.setting_frame.columnconfigure(1, weight=1)
        self.setting_frame.columnconfigure(2, weight=0)
        self.setting_frame.columnconfigure(3, weight=0)
        self.setting_frame.columnconfigure(4, weight=0)
    def _on_root_resize(self, _event=None):
        width = self.root.winfo_width()
        layout_mode = self._layout_mode_for_width(width)
        if layout_mode != self._layout_mode:
            self._apply_setting_layout(layout_mode)
        self._resize_tree_columns()
        self._on_log_resize()
    def _on_table_resize(self, _event=None):
        self._resize_tree_columns()
        self._apply_import_target_view()
        self._update_empty_hint()
    def _resize_tree_columns(self):
        if not hasattr(self, "tree"):
            return
        cols = ("checked", "idx", "source", "target", "status", "recovery", "balance")
        width = self.tree.winfo_width()
        if width <= 1 and hasattr(self, "table_wrap"):
            width = self.table_wrap.winfo_width() - 20
        if width <= 0:
            return

        min_widths = dict(self.TREE_COL_MIN_WIDTHS)
        weights = dict(self.TREE_COL_WEIGHTS)
        if width < 820:
            min_widths.update({"source": 150, "target": 220, "status": 92, "recovery": 84, "balance": 68})
            weights.update({"source": 3, "target": 6, "status": 2, "recovery": 2, "balance": 1})
        elif width < 980:
            min_widths.update({"source": 170, "target": 250, "status": 96, "recovery": 86, "balance": 72})
            weights.update({"source": 3, "target": 6, "status": 2, "recovery": 2, "balance": 1})
        elif width < 1220:
            min_widths.update({"source": 210, "target": 300, "status": 100, "recovery": 90, "balance": 76})
            weights.update({"source": 4, "target": 6, "status": 2, "recovery": 2, "balance": 1})

        total_min = sum(min_widths[c] for c in cols)

        if width <= total_min:
            for c in cols:
                self.tree.column(c, width=min_widths[c], stretch=False)
            return

        extra = width - total_min
        total_weight = sum(weights[c] for c in cols)
        used = 0
        for i, c in enumerate(cols):
            add = extra - used if i == len(cols) - 1 else int(extra * weights[c] / total_weight)
            used += add
            self.tree.column(c, width=min_widths[c] + max(0, add), stretch=True)
    def _on_log_resize(self, _event=None):
        if not hasattr(self, "log_tree"):
            return
        width = self.log_tree.winfo_width()
        if width <= 1 and hasattr(self, "log_box"):
            width = self.log_box.winfo_width() - 20
        if width <= 0:
            return
        msg_width = max(300, width - 190)
        self.log_tree.column("time", width=170, stretch=False)
        self.log_tree.column("msg", width=msg_width, stretch=True)
    def _on_mode_changed(self, *_args):
        mode = self._mode()
        previous_mode = str(getattr(self, "_last_mode_for_amounts", "") or "").strip()
        if self._mode_amount_config_ready:
            if previous_mode and previous_mode != mode:
                self._capture_mode_amount_config(previous_mode)
            self._apply_mode_amount_config(mode)
        self._last_mode_for_amounts = mode
        previous_relay_mode = str(getattr(self, "_last_mode_for_relay", "") or "").strip()
        if self._mode_relay_config_ready:
            if previous_relay_mode and previous_relay_mode != mode:
                self._capture_mode_relay_config(previous_relay_mode)
            self._apply_mode_relay_config(mode)
        self._last_mode_for_relay = mode
        self._set_import_target("full")
        self._refresh_relay_fee_reserve_label()
        self._refresh_relay_controls()
        if mode == self.MODE_M2M:
            self.source_balance_var.set("-")
            self.target_balance_var.set("-")
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            self.source_balance_var.set(self._balance_text_for_source(source) if source else "-")
            self.target_balance_var.set("-")
        else:
            self.source_balance_var.set("-")
            target = self.target_address_var.get().strip()
            self.target_balance_var.set(self._balance_text_for_target(target) if target else "-")
        width = self.root.winfo_width() if hasattr(self, "root") and hasattr(self.root, "winfo_width") else 1500
        self._apply_setting_layout(self._layout_mode_for_width(width))
        self._refresh_tree()
        self._resize_tree_columns()
        self._update_empty_hint()
        self._update_wallet_generator_import_button()
    def _on_network_changed(self, *_args):
        network = self.network_var.get().strip().upper()
        if network in {"ETH", "BSC"}:
            self._build_token_options(network=network, prefer_symbol=self.symbol_var.get().strip(), prefer_contract="")
        else:
            self.current_tokens = {}
            self.coin_box.configure(values=[])
            self.coin_var.set("")
            self.symbol_var.set("-")
        self.source_balance_cache.clear()
        self.target_balance_cache.clear()
        self.source_balance_var.set("-")
        self.target_balance_var.set("-")
        self._update_balance_heading()
        self._refresh_tree()
    def _on_coin_changed(self, *_args):
        token = self._selected_token(with_message=False)
        if token:
            self.symbol_var.set(token.symbol)
        else:
            self.symbol_var.set("-")
        self.source_balance_cache.clear()
        self.target_balance_cache.clear()
        self.source_balance_var.set("-")
        self.target_balance_var.set("-")
        self._update_balance_heading()
        self._refresh_tree()
    def _on_proxy_config_changed(self, *_args):
        self._sync_onchain_proxy_state()
        self._refresh_onchain_proxy_summary()
    def _on_source_or_target_changed(self, *_args):
        if self._is_mode_1m():
            source = self.source_credential_var.get().strip()
            bal = self._balance_text_for_source(source) if source else "-"
            self.source_balance_var.set(bal)
            self.target_balance_var.set("-")
            if not self.is_running:
                task_progress.reset_metrics(self, amount_label="转账总额")
            self._schedule_tree_refresh()
            return
        self.source_balance_var.set("-")
        if self._is_mode_m1():
            target = self.target_address_var.get().strip()
            self.target_balance_var.set(self._balance_text_for_target(target) if target else "-")
            if not self.is_running:
                task_progress.reset_metrics(self, amount_label="转账总额")
            self._schedule_tree_refresh()
            return
        self.target_balance_var.set("-")
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
    def log(self, text: str):
        queue_log_row(self, self.log_tree, text, root=getattr(self, "root", None), max_rows=LOG_MAX_ROWS)
    def test_onchain_proxy(self):
        snapshot = self._onchain_proxy_state_snapshot()
        route_text = self._onchain_proxy_route_text(state=snapshot)
        selected_network = str(self.network_var.get() or "").strip().upper()

        def worker():
            test_ok = False
            try:
                status, exit_ip, target = self._test_onchain_proxy_once(state=snapshot, selected_network=selected_network)
                test_ok = True
                log_text = f"链上代理测试成功：status={status}，exit_ip={exit_ip}，target={target}，route={route_text}"
            except Exception as exc:
                use_config_proxy = bool(snapshot.get("use_config_proxy"))
                raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
                if use_config_proxy and raw_proxy:
                    status = "连接失败"
                elif (not use_config_proxy) and self._onchain_system_proxy_map():
                    status = "系统代理异常"
                else:
                    status = "直连异常"
                exit_ip = "--"
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
