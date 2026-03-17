#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import queue
import re
import threading
import time
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
import tkinter as tk
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, BooleanVar, DoubleVar, Frame as TkFrame, Menu, StringVar
from tkinter import messagebox, ttk

from api_clients import EvmClient
from app_paths import ONCHAIN_DATA_FILE
from core_models import EvmToken, OnchainPairEntry, WithdrawRuntimeParams
from shared_utils import (
    LOG_MAX_ROWS,
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

SUBMITTED_TIMEOUT_SECONDS = 10.0

class OnchainTransferPage:
    MODE_M2M = "多对多"
    MODE_1M = "1对多"
    MODE_M1 = "多对1"
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数"
    AMOUNT_MODE_ALL = "全部"
    AMOUNT_ALL_LABEL = "全部"
    NETWORK_OPTIONS = ["", "ETH", "BSC"]
    MAX_TOKEN_DECIMALS = 36
    TREE_COL_MIN_WIDTHS = {
        "checked": 42,
        "idx": 42,
        "source": 330,
        "target": 420,
        "status": 96,
        "balance": 80,
    }
    TREE_COL_WEIGHTS = {
        "checked": 1,
        "idx": 1,
        "source": 5,
        "target": 7,
        "status": 2,
        "balance": 1,
    }

    def __init__(self, parent, rpc_proxy_getter=None):
        self.parent = parent
        self.root = parent.winfo_toplevel()
        self.store = OnchainStore(ONCHAIN_DATA_FILE)
        self.client = EvmClient(proxy_provider=rpc_proxy_getter)
        self.is_running = False
        self.stop_requested = threading.Event()
        self._layout_mode: str | None = None

        self.row_index_map: dict[str, int] = {}
        self.row_key_by_row_id: dict[str, str] = {}
        self.row_id_by_key: dict[str, str] = {}
        self.checked_row_keys: set[str] = set()
        self.row_status: dict[str, str] = {}
        self.row_status_text_map: dict[str, str] = {}
        self.row_status_context: dict[str, str] = {}
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
        self.dry_run_var = BooleanVar(value=False)
        self.source_credential_var = StringVar(value="")
        self.target_address_var = StringVar(value="")
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
        self.m2m_import_drafts: list[dict[str, str]] = []
        self.checked_m2m_draft_rows: set[int] = set()

        self._build_ui()
        self._load_data()

    def _build_ui(self):
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
            width=8,
            state="readonly",
        )
        self.ent_amount = ttk.Entry(self.amount_ctrl, textvariable=self.amount_var, width=8)
        self.ent_random_min = ttk.Entry(self.amount_ctrl, textvariable=self.random_min_var, width=7)
        self.lbl_random_sep = ttk.Label(self.amount_ctrl, text="~")
        self.ent_random_max = ttk.Entry(self.amount_ctrl, textvariable=self.random_max_var, width=7)
        self.lbl_amount_all_hint = ttk.Label(self.amount_ctrl, text="按钱包可用余额", style="Subtle.TLabel")
        self._apply_amount_layout()

        self.chk_dry_run = ttk.Checkbutton(setting, text="模拟执行", variable=self.dry_run_var)
        self.lbl_delay = ttk.Label(setting, text="执行间隔(秒)")
        self.ent_delay = ttk.Entry(setting, textvariable=self.delay_var, width=7)
        self.lbl_threads = ttk.Label(setting, text="执行线程数")
        self.spin_threads = ttk.Spinbox(setting, from_=1, to=64, textvariable=self.threads_var, width=6)

        self.lbl_source_credential = ttk.Label(setting, text="转出钱包私钥/助记词*")
        self.ent_source_credential = ttk.Entry(setting, textvariable=self.source_credential_var, width=34)
        self.btn_query_source_balance = ttk.Button(setting, text="查询", command=self.query_current_source_balance)
        self.lbl_source_balance_title = ttk.Label(setting, text="转出钱包余额")
        self.lbl_source_balance_val = ttk.Label(setting, textvariable=self.source_balance_var, style="Value.TLabel")
        self.lbl_target_address = ttk.Label(setting, text="收款地址*")
        self.ent_target_address = ttk.Entry(setting, textvariable=self.target_address_var, width=34)
        self.btn_query_target_balance = ttk.Button(setting, text="查询", command=self.query_current_target_balance)
        bind_paste_shortcuts(self.ent_source_credential)
        bind_paste_shortcuts(self.ent_target_address)
        self.lbl_target_balance_title = ttk.Label(setting, text="收款地址余额")
        self.lbl_target_balance_val = ttk.Label(setting, textvariable=self.target_balance_var, style="Value.TLabel")
        self._apply_setting_layout("wide")

        self.table_wrap = ttk.Frame(main)
        self.table_wrap.pack(fill=BOTH, expand=True)
        self.table_wrap.columnconfigure(0, weight=1)
        self.table_wrap.rowconfigure(0, weight=1)

        cols = ("checked", "idx", "source", "target", "status", "balance")
        self.tree = ttk.Treeview(self.table_wrap, columns=cols, show="headings", selectmode="extended", height=16)
        self._tree_column_ids = cols
        self._tree_heading_base_texts = {
            "checked": "勾选",
            "idx": "编号",
            "source": "转出凭证",
            "target": "接收地址",
            "status": "执行状态",
            "balance": "余额",
        }
        for column, text in self._tree_heading_base_texts.items():
            self.tree.heading(column, text=text)

        self.tree.column("checked", width=42, anchor="center")
        self.tree.column("idx", width=42, anchor="center")
        self.tree.column("source", width=360, anchor="w")
        self.tree.column("target", width=420, anchor="w")
        self.tree.column("status", width=110, anchor="center")
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
        ttk.Button(action1, text="保存配置", command=self.save_all).pack(side=LEFT, padx=(8, 0))
        ttk.Label(action1, text="链上为独立模块，与交易所互不影响。", style="Subtle.TLabel").pack(side=LEFT, padx=(12, 0))

        action2 = ttk.Frame(main)
        action2.pack(fill="x", pady=(0, 10))
        ttk.Button(action2, text="查询余额", command=self.start_query_balance).pack(side=LEFT)
        self.btn_stop_tasks = tk.Button(
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
        ttk.Button(action2, text="执行批量转账", style="Action.TButton", command=self.start_batch_transfer).pack(side=RIGHT)
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
        self.source_credential_var.trace_add("write", self._on_source_or_target_changed)
        self.target_address_var.trace_add("write", self._on_source_or_target_changed)

        self.table_wrap.bind("<Configure>", self._on_table_resize)
        self.log_box.bind("<Configure>", self._on_log_resize)
        self.root.bind("<Configure>", self._on_root_resize, add="+")
        self.root.after_idle(self._on_mode_changed)
        self.root.after_idle(self._resize_tree_columns)
        self.root.after_idle(self._on_log_resize)
        self.root.after_idle(self._on_root_resize)
        self.root.after_idle(self._apply_import_target_view)
        self.root.after_idle(self._update_empty_hint)

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

    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)

    @classmethod
    def _decimal_to_text(cls, v: Decimal) -> str:
        return decimal_to_text(v)

    @staticmethod
    def _factor_by_decimals(decimals: int) -> Decimal:
        if decimals < 0 or decimals > OnchainTransferPage.MAX_TOKEN_DECIMALS:
            raise RuntimeError(f"代币精度超出范围：{decimals}")
        return Decimal(10) ** decimals

    @classmethod
    def _units_to_amount(cls, units: int, decimals: int) -> Decimal:
        return Decimal(units) / cls._factor_by_decimals(decimals)

    @classmethod
    def _amount_to_units(cls, amount: Decimal, decimals: int) -> int:
        val = (amount * cls._factor_by_decimals(decimals)).to_integral_value(rounding=ROUND_FLOOR)
        return int(val)

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
        raw = self.threads_var.get() if hasattr(self, "threads_var") else 10
        return parse_worker_threads(raw, default=10)

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
            return "完成"
        if status == "failed":
            return "失败"
        if status == "submitted":
            return "确认中"
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
        return ""

    @staticmethod
    def _success_status_text(coin: str, amount_text: str) -> str:
        amount = str(amount_text or "").strip()
        coin_u = str(coin or "").strip().upper()
        if not amount:
            return "完成"
        if coin_u:
            return f"已转 {amount} {coin_u}"
        return f"已转 {amount}"

    def _status_text_for_row(self, row_key: str, status: str) -> str:
        if status == "success":
            custom = str(getattr(self, "row_status_text_map", {}).get(row_key, "") or "").strip()
            if custom and self._context_matches(row_key, getattr(self, "row_status_context", {})):
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
                self._balance_text_for_source(source),
            )

        self.tree.item(row_id, values=values)
        tag = self._status_tag(status)
        self.tree.item(row_id, tags=(tag,) if tag else ())

    def _update_rows_view(self, row_keys: list[str]):
        for row_key in row_keys:
            self._update_row_view(row_key)

    @staticmethod
    def _unique_row_keys(row_keys: list[str]) -> list[str]:
        return task_progress.unique_keys(row_keys)

    def _progress_store(self, kind: str) -> dict[str, str]:
        if kind == "query":
            return self._ensure_query_status_store()
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
        dispatch_ui_callback(self, callback)

    def _schedule_tree_refresh(self) -> None:
        schedule_ui_callback(self, "tree_refresh", self._refresh_tree, root=getattr(self, "root", None))

    def _finish_progress(self, kind: str, success: int, failed: int):
        flush_queued_ui_renders(self)
        log_tree = getattr(self, "log_tree", None)
        if log_tree is not None:
            flush_queued_log_rows(self, log_tree, max_rows=LOG_MAX_ROWS)
        clear_ui_batch_size(self)
        task_progress.finish(self, kind, success, failed)

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
        if status == "success":
            text = str(status_text or "").strip()
            if text:
                self.row_status_text_map[row_key] = text
            else:
                self.row_status_text_map.pop(row_key, None)
        else:
            self.row_status_text_map.pop(row_key, None)
        queue_ui_render(self, lambda k=row_key: self._update_row_view(k), root=getattr(self, "root", None))
        self._refresh_progress_if_active("transfer", row_key)

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
                        self._balance_text_for_source(item.source),
                    ),
                    tags=(tag,) if tag else (),
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
                        self._balance_text_for_target(target),
                ),
                tags=(tag,) if tag else (),
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
                    self._balance_text_for_source(source),
                ),
                tags=(tag,) if tag else (),
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
            self.lbl_delay,
            self.ent_delay,
            self.lbl_threads,
            self.spin_threads,
            self.lbl_source_credential,
            self.ent_source_credential,
            self.btn_query_source_balance,
            self.lbl_source_balance_title,
            self.lbl_source_balance_val,
            self.lbl_target_address,
            self.ent_target_address,
            self.btn_query_target_balance,
            self.lbl_target_balance_title,
            self.lbl_target_balance_val,
        ]
        for w in widgets:
            w.grid_forget()

        for c in range(12):
            self.setting_frame.columnconfigure(c, weight=0)

        show_source = self._is_mode_1m()
        show_target = self._is_mode_m1()

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

            self.lbl_contract_search.grid(row=1, column=0, sticky=W, pady=(8, 0))
            self.ent_contract_search.grid(row=1, column=1, columnspan=6, sticky="ew", padx=(4, 10), pady=(8, 0))
            self.btn_contract_search.grid(row=1, column=7, sticky=W, pady=(8, 0))
            self.lbl_delay.grid(row=1, column=8, sticky=W, pady=(8, 0))
            self.ent_delay.grid(row=1, column=9, sticky=W, padx=(4, 10), pady=(8, 0))
            self.lbl_threads.grid(row=1, column=10, sticky=W, pady=(8, 0))
            self.spin_threads.grid(row=1, column=11, sticky=W, padx=(4, 0), pady=(8, 0))
            self.setting_frame.columnconfigure(1, weight=1)
            self.setting_frame.columnconfigure(3, weight=1)
            self.setting_frame.columnconfigure(5, weight=1)
            self.setting_frame.columnconfigure(6, weight=1)

            row = 2
            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=4, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=5, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=6, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=7, columnspan=3, sticky=W, padx=(4, 0), pady=(8, 0))
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

            self.lbl_contract_search.grid(row=1, column=0, sticky=W, pady=(8, 0))
            self.ent_contract_search.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(4, 8), pady=(8, 0))
            self.btn_contract_search.grid(row=1, column=6, sticky=W, pady=(8, 0))
            self.lbl_delay.grid(row=1, column=7, sticky=W, padx=(10, 0), pady=(8, 0))
            self.ent_delay.grid(row=1, column=8, sticky=W, padx=(4, 10), pady=(8, 0))
            self.lbl_threads.grid(row=1, column=9, sticky=W, pady=(8, 0))
            self.spin_threads.grid(row=1, column=10, sticky=W, padx=(4, 0), pady=(8, 0))

            row = 2

            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=3, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=4, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=5, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=6, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))
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

        self.chk_dry_run.grid(row=row, column=0, columnspan=2, sticky=W, pady=(8, 0))
        row += 1

        if show_source:
            self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_source_credential.grid(row=row, column=1, sticky="ew", padx=(4, 6), pady=(8, 0))
            self.btn_query_source_balance.grid(row=row, column=2, sticky=W, padx=(0, 6), pady=(8, 0))
            self.lbl_source_balance_title.grid(row=row, column=3, sticky=W, pady=(8, 0))
            self.lbl_source_balance_val.grid(row=row, column=4, sticky=W, padx=(4, 0), pady=(8, 0))
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
        cols = ("checked", "idx", "source", "target", "status", "balance")
        width = self.tree.winfo_width()
        if width <= 1 and hasattr(self, "table_wrap"):
            width = self.table_wrap.winfo_width() - 20
        if width <= 0:
            return

        min_widths = dict(self.TREE_COL_MIN_WIDTHS)
        weights = dict(self.TREE_COL_WEIGHTS)
        if width < 820:
            min_widths.update({"source": 150, "target": 220, "status": 84, "balance": 68})
            weights.update({"source": 3, "target": 6, "status": 2, "balance": 1})
        elif width < 980:
            min_widths.update({"source": 170, "target": 250, "status": 86, "balance": 72})
            weights.update({"source": 3, "target": 6, "status": 2, "balance": 1})
        elif width < 1220:
            min_widths.update({"source": 210, "target": 300, "status": 90, "balance": 76})
            weights.update({"source": 4, "target": 6, "status": 2, "balance": 1})

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
        self._set_import_target("full")
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

    def _prepare_async_task(self) -> bool:
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return False
        self.stop_requested.clear()
        self.is_running = True
        return True

    def stop_current_tasks(self):
        if not self.is_running:
            self.log("当前没有运行中的链上任务")
            return
        self.stop_requested.set()
        self.log("已收到停止请求，正在停止当前链上任务...")

    def query_current_source_balance(self):
        try:
            if not self._is_mode_1m():
                return
            source = self.source_credential_var.get().strip()
            if not source:
                messagebox.showerror("参数错误", "请先填写转出钱包私钥/助记词")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            if not self._prepare_async_task():
                return
            threading.Thread(
                target=self._run_query_balance_one_to_many,
                args=(network, token, source, []),
                daemon=True,
            ).start()
        except Exception as exc:
            self.log(f"转出钱包余额查询启动失败：{exc}")
            self.is_running = False
            messagebox.showerror("执行异常", str(exc))

    def query_current_target_balance(self):
        try:
            if not self._is_mode_m1():
                return
            target = self.target_address_var.get().strip()
            if not target:
                messagebox.showerror("参数错误", "请先填写收款地址")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            target = self._validate_recipient_address(target, "收款地址")
            self.target_address_var.set(target)
            if not self._prepare_async_task():
                return
            threading.Thread(
                target=self._run_query_balance_many_to_one,
                args=(network, token, target, []),
                daemon=True,
            ).start()
        except Exception as exc:
            self.log(f"收款地址余额查询启动失败：{exc}")
            self.is_running = False
            messagebox.showerror("执行异常", str(exc))

    def _update_balance_heading(self):
        symbol = self.symbol_var.get().strip() or "-"
        text = f"余额({symbol})"
        self._tree_heading_base_texts["balance"] = text
        self.tree.heading("balance", text=text)
        self._apply_import_target_view()

    @staticmethod
    def _short_contract(contract: str) -> str:
        s = contract.strip()
        if len(s) <= 12:
            return s
        return f"{s[:8]}...{s[-6:]}"

    def _build_token_options(self, network: str, prefer_symbol: str = "", prefer_contract: str = ""):
        net = network.strip().upper()
        defaults = self.client.get_default_tokens(net)
        custom = list((self.custom_tokens_by_network.get(net) or {}).values())
        all_tokens = defaults + custom

        mapping: dict[str, EvmToken] = {}
        used: set[str] = set()
        for token in all_tokens:
            label = self._token_display(token)
            if label in used:
                if token.contract:
                    label = f"{token.symbol}({self._short_contract(token.contract)})"
                else:
                    label = f"{token.symbol}(原生)"
            idx = 2
            while label in used:
                label = f"{label}#{idx}"
                idx += 1
            used.add(label)
            mapping[label] = token

        self.current_tokens = mapping
        values = list(mapping.keys())
        self.coin_box.configure(values=values)
        if not values:
            self.coin_var.set("")
            self.symbol_var.set("-")
            return

        current_label = self.coin_var.get().strip()
        selected = ""
        prefer_contract_l = prefer_contract.strip().lower()
        prefer_symbol_u = prefer_symbol.strip().upper()

        if prefer_contract_l:
            for label, token in mapping.items():
                if token.contract.strip().lower() == prefer_contract_l:
                    selected = label
                    break
        if not selected and prefer_symbol_u:
            for label, token in mapping.items():
                if token.symbol.strip().upper() == prefer_symbol_u:
                    selected = label
                    break
        if not selected and current_label in mapping:
            selected = current_label
        if not selected:
            selected = values[0]
        self.coin_var.set(selected)

    def _selected_token(self, with_message: bool = False) -> EvmToken | None:
        label = self.coin_var.get().strip() if hasattr(self, "coin_var") else ""
        token_map = self.current_tokens if hasattr(self, "current_tokens") else {}
        token = token_map.get(label)
        if token is not None:
            return token

        if token_map:
            first_label = next(iter(token_map))
            if hasattr(self, "coin_var"):
                self.coin_var.set(first_label)
            return token_map[first_label]

        network = self.network_var.get().strip().upper()
        if network in {"ETH", "BSC"}:
            symbol = self.client.get_symbol(network)
            if symbol:
                return EvmToken(symbol=symbol, contract="", decimals=18, is_native=True)

        if with_message:
            messagebox.showerror("参数错误", "币种未设置，无法执行")
        return None

    def _token_desc(self, token: EvmToken) -> str:
        if token.is_native:
            return token.symbol
        return f"{token.symbol}({self._short_contract(token.contract)})"

    def _token_desc_from_params(self, params: WithdrawRuntimeParams) -> str:
        if params.token_is_native:
            return params.coin
        return f"{params.coin}({self._short_contract(params.token_contract)})"

    def search_contract_token(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        network = self.network_var.get().strip().upper()
        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "请先设置网络后再搜索合约")
            return
        contract = self.contract_search_var.get().strip()
        if not self.client.is_address(contract):
            messagebox.showerror("参数错误", "合约地址格式错误")
            return
        self.log(f"开始搜索代币合约：network={network}, contract={contract}")
        threading.Thread(target=self._run_search_contract_token, args=(network, contract), daemon=True).start()

    def _run_search_contract_token(self, network: str, contract: str):
        try:
            token = self.client.get_erc20_token_info(network, contract)
            self.custom_tokens_by_network.setdefault(network, {})[token.contract.lower()] = token
            self._dispatch_ui(lambda n=network, t=token: self._apply_found_token(n, t))
        except Exception as exc:
            err_text = str(exc)
            self._dispatch_ui(lambda m=f"合约搜索失败：{err_text}": self.log(m))
            self._dispatch_ui(lambda e=err_text: messagebox.showerror("合约搜索失败", e))

    def _apply_found_token(self, network: str, token: EvmToken):
        self.log(
            f"合约识别成功：network={network}, symbol={token.symbol}, decimals={token.decimals}, contract={token.contract}"
        )
        if self.network_var.get().strip().upper() == network:
            self._build_token_options(network=network, prefer_symbol=token.symbol, prefer_contract=token.contract)

    def log(self, text: str):
        queue_log_row(self, self.log_tree, text, root=getattr(self, "root", None), max_rows=LOG_MAX_ROWS)

    def _parse_m2m_lines(self, lines: list[str]) -> list[OnchainPairEntry]:
        result: list[OnchainPairEntry] = []
        seen: set[tuple[str, str]] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if any(x in s for x in ("\t", ",", ";")):
                arr = [x.strip() for x in re.split(r"[\t,;]+", s) if x.strip()]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            else:
                arr = [x for x in s.split() if x]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            if not source:
                raise RuntimeError(f"第 {i} 行格式错误：私钥/助记词不能为空")
            target = self._validate_recipient_address(target, f"第 {i} 行接收地址")
            key = (source, target)
            if key in seen:
                continue
            seen.add(key)
            result.append(OnchainPairEntry(source=source, target=target))
        return result

    def _parse_address_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            s = self._validate_recipient_address(s, f"第 {i} 行接收地址")
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _parse_source_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _import_rows(self, rows: list[OnchainPairEntry] | list[str], source_name: str):
        if not rows:
            messagebox.showwarning("提示", "没有可导入的数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M:
            created = self.store.upsert_multi_to_multi(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self.log(f"{source_name}导入完成：新增 {created} 条，已自动全选 {len(self.store.multi_to_multi_pairs)} 条")
            return
        if mode == self.MODE_1M:
            created = self.store.upsert_one_to_many_addresses(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self.log(f"{source_name}导入完成：新增 {created} 条地址，已自动全选 {len(self.store.one_to_many_addresses)} 条")
            return
        created = self.store.upsert_many_to_one_sources(rows)  # type: ignore[arg-type]
        self.checked_row_keys = set(self._active_row_keys())
        self._refresh_tree()
        self.log(f"{source_name}导入完成：新增 {created} 条钱包，已自动全选 {len(self.store.many_to_one_sources)} 条")

    def import_from_paste(self):
        self.import_from_clipboard()

    def import_from_clipboard(self):
        try:
            text = self.root.clipboard_get()
        except Exception:
            messagebox.showwarning("提示", "剪贴板为空或不可读取")
            return
        try:
            lines = text.splitlines()
            mode = self._mode()
            target = self._current_import_target()
            if mode == self.MODE_M2M and target != "full":
                self._import_m2m_column(target, lines, "剪贴板")
                return
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "剪贴板")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    def import_txt(self):
        from tkinter import filedialog

        path = filedialog.askopenfilename(title="选择 TXT", filetypes=[("Text", "*.txt"), ("All Files", "*.*")])
        if not path:
            return
        try:
            content = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = Path(path).read_text(encoding="utf-8-sig")
        try:
            lines = content.splitlines()
            mode = self._mode()
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "TXT")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    def export_txt(self):
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(title="导出 TXT", defaultextension=".txt", filetypes=[("Text", "*.txt")])
        if not path:
            return
        try:
            mode = self._mode()
            if mode == self.MODE_M2M:
                lines = [f"{x.source} {x.target}" for x in self.store.multi_to_multi_pairs]
            elif mode == self.MODE_1M:
                lines = list(self.store.one_to_many_addresses)
            else:
                lines = list(self.store.many_to_one_sources)
            Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            self.log(f"TXT 导出完成：{path}")
            messagebox.showinfo("导出完成", f"已导出 {len(lines)} 条")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc))

    def toggle_check_all(self):
        keys = set(self._active_row_keys())
        draft_rows = set(range(len(self.m2m_import_drafts))) if self._is_mode_m2m() else set()
        checked_draft_rows = self._ensure_checked_m2m_draft_rows() if self._is_mode_m2m() else set()
        if not keys and not draft_rows:
            messagebox.showwarning("提示", "当前模式暂无可操作数据")
            return
        if self.checked_row_keys == keys and checked_draft_rows == draft_rows:
            self.checked_row_keys.clear()
            checked_draft_rows.clear()
            self.log("已取消全选")
        else:
            self.checked_row_keys = set(keys)
            checked_draft_rows.clear()
            checked_draft_rows.update(draft_rows)
            self.log(f"已全选 {len(keys) + len(draft_rows)} 条")
        self._refresh_tree()

    def delete_selected(self):
        mode = self._mode()
        if mode == self.MODE_M2M:
            idxs = [i for i, item in enumerate(self.store.multi_to_multi_pairs) if self._m2m_key(item) in self.checked_row_keys]
            draft_idxs = sorted(self._ensure_checked_m2m_draft_rows())
        elif mode == self.MODE_1M:
            idxs = [i for i, target in enumerate(self.store.one_to_many_addresses) if self._one_to_many_key(target) in self.checked_row_keys]
            draft_idxs = []
        else:
            idxs = [i for i, source in enumerate(self.store.many_to_one_sources) if self._many_to_one_key(source) in self.checked_row_keys]
            draft_idxs = []
        if not idxs and not draft_idxs:
            messagebox.showwarning("提示", "请先勾选要删除的数据")
            return
        total = len(idxs) + len(draft_idxs)
        if not messagebox.askyesno("确认删除", f"确认删除 {total} 条数据吗？"):
            return

        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices(idxs)
            for draft_idx in sorted(draft_idxs, reverse=True):
                if 0 <= draft_idx < len(self.m2m_import_drafts):
                    self.m2m_import_drafts.pop(draft_idx)
            self._ensure_checked_m2m_draft_rows().clear()
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices(idxs)
        else:
            self.store.delete_many_to_one_by_indices(idxs)
        self._refresh_tree()
        self.log(f"已删除 {total} 条")

    def delete_current_row(self):
        draft_idx = self._selected_draft_index()
        if draft_idx is not None and self._is_mode_m2m():
            if not (0 <= draft_idx < len(self.m2m_import_drafts)):
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            if not messagebox.askyesno("确认删除", "确认删除当前待补齐数据吗？"):
                return
            self.m2m_import_drafts.pop(draft_idx)
            self._ensure_checked_m2m_draft_rows().clear()
            self._refresh_tree()
            self.log("已删除当前待补齐数据")
            return
        idx = self._single_selected_index()
        if idx is None:
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M and not (0 <= idx < len(self.store.multi_to_multi_pairs)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_1M and not (0 <= idx < len(self.store.one_to_many_addresses)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_M1 and not (0 <= idx < len(self.store.many_to_one_sources)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if not messagebox.askyesno("确认删除", "确认删除当前数据吗？"):
            return
        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices([idx])
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices([idx])
        else:
            self.store.delete_many_to_one_by_indices([idx])
        self._refresh_tree()
        self.log("已删除当前数据")

    def _apply_settings_to_store(self) -> bool:
        mode = self._mode()
        network = self.network_var.get().strip().upper()
        token = self._selected_token(with_message=False)
        amount_mode = self._amount_mode()
        amount = self.amount_var.get().strip()
        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        elif amount_mode == self.AMOUNT_MODE_FIXED:
            if not amount:
                messagebox.showerror("参数错误", "转账数量不能为空")
                return False
            try:
                amount_value = Decimal(amount)
                if amount_value <= 0:
                    raise InvalidOperation
                amount = self._decimal_to_text(amount_value)
            except Exception:
                messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
                return False
        random_min = self.random_min_var.get().strip()
        random_max = self.random_max_var.get().strip()
        if amount_mode == self.AMOUNT_MODE_RANDOM:
            try:
                random_min_value = Decimal(random_min)
                random_max_value = Decimal(random_max)
            except Exception:
                messagebox.showerror("参数错误", "随机金额最小值/最大值格式错误")
                return False
            if random_min_value <= 0 or random_max_value <= 0:
                messagebox.showerror("参数错误", "随机金额最小值和最大值必须大于 0")
                return False
            if random_max_value < random_min_value:
                messagebox.showerror("参数错误", "随机金额最大值必须大于或等于最小值")
                return False
            random_min = self._decimal_to_text(random_min_value)
            random_max = self._decimal_to_text(random_max_value)
        try:
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return False
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
        except Exception:
            messagebox.showerror("参数错误", "执行线程数格式错误")
            return False

        self.store.settings = OnchainSettings(
            mode=mode,
            network=network,
            token_symbol=(token.symbol if token else ""),
            token_contract=(token.contract if token else ""),
            amount_mode=amount_mode,
            amount=amount,
            random_min=random_min,
            random_max=random_max,
            delay_seconds=delay,
            worker_threads=threads,
            dry_run=bool(self.dry_run_var.get()),
            one_to_many_source=self.source_credential_var.get().strip(),
            many_to_one_target="",
        )
        target = self.target_address_var.get().strip()
        if target:
            try:
                safe_target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return False
            self.target_address_var.set(safe_target)
            self.store.settings.many_to_one_target = safe_target
        return True

    def save_all(self):
        draft_count = len(self.m2m_import_drafts)
        if draft_count and not messagebox.askyesno(
            "未补齐提示",
            f"当前有 {draft_count} 行待补齐数据，保存时只会写入已补齐数据，确认继续？",
        ):
            return
        if not self._apply_settings_to_store():
            return
        try:
            self.store.save()
            self.log("链上配置已保存")
            messagebox.showinfo("成功", f"配置已保存到：{ONCHAIN_DATA_FILE}")
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))

    def _load_data(self):
        try:
            self.store.load()
            st = self.store.settings
            loaded_mode = st.mode if st.mode in {self.MODE_M2M, self.MODE_1M, self.MODE_M1} else self.MODE_M2M
            self.mode_var.set(loaded_mode)
            net = st.network if st.network in {"ETH", "BSC"} else ""
            self.network_var.set(net)
            if net:
                self._build_token_options(network=net, prefer_symbol=st.token_symbol, prefer_contract=st.token_contract)
            else:
                self.current_tokens = {}
                self.coin_box.configure(values=[])
                self.coin_var.set("")
            self.amount_mode_var.set(st.amount_mode if st.amount_mode in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL} else self.AMOUNT_MODE_FIXED)
            self.amount_var.set("" if self.amount_mode_var.get() == self.AMOUNT_MODE_ALL else (st.amount or ""))
            self.random_min_var.set(st.random_min or "")
            self.random_max_var.set(st.random_max or "")
            self.delay_var.set(st.delay_seconds)
            self.threads_var.set(str(max(1, int(st.worker_threads or 1))))
            self.dry_run_var.set(st.dry_run)
            self.source_credential_var.set(st.one_to_many_source or "")
            loaded_target = st.many_to_one_target or ""
            try:
                loaded_target = self._try_validate_recipient_address(loaded_target, "收款地址") or loaded_target
            except Exception:
                pass
            self.target_address_var.set(loaded_target)
            self.contract_search_var.set(st.token_contract or "")
            self.source_balance_var.set("-")
            self.target_balance_var.set("-")
            self.checked_row_keys = set(self._active_row_keys())
            self._on_coin_changed()
            self._on_mode_changed()
            self.log("链上配置加载完成")
        except Exception as exc:
            messagebox.showerror("加载失败", str(exc))

    def _collect_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if key in self.checked_row_keys:
                    try:
                        target = self._validate_recipient_address(item.target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, item.source, target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if key in self.checked_row_keys:
                    try:
                        safe_target = self._validate_recipient_address(target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, source, safe_target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            try:
                target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                if with_message:
                    messagebox.showerror("参数错误", str(exc))
                return []
            self.target_address_var.set(target)
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if key in self.checked_row_keys:
                    jobs.append((key, source, target))

        if with_message and not jobs:
            messagebox.showwarning("提示", "请先勾选至少一条数据")
        return jobs

    def _collect_failed_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if self._display_status(key) == "failed":
                    try:
                        target = self._validate_recipient_address(item.target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, item.source, target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if self._display_status(key) == "failed":
                    try:
                        safe_target = self._validate_recipient_address(target, "接收地址")
                    except Exception as exc:
                        if with_message:
                            messagebox.showerror("参数错误", str(exc))
                        return []
                    jobs.append((key, source, safe_target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            try:
                target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                if with_message:
                    messagebox.showerror("参数错误", str(exc))
                return []
            self.target_address_var.set(target)
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if self._display_status(key) == "failed":
                    jobs.append((key, source, target))

        if with_message and not jobs:
            tip = "当前没有失败记录可重试"
            if self._has_submitted_jobs():
                tip = "当前没有失败记录可重试，存在确认中的记录，请等待自动确认完成"
            messagebox.showwarning("提示", tip)
        return jobs

    def _has_submitted_jobs(self) -> bool:
        return any(self._display_status(row_key) == "submitted" for row_key in self._active_row_keys())

    @classmethod
    def _random_decimal_between(cls, low: Decimal, high: Decimal) -> Decimal:
        return random_decimal_between(low, high)

    def _validate_transfer_params(self) -> WithdrawRuntimeParams | None:
        network = self.network_var.get().strip().upper()
        amount_mode = self._amount_mode()
        amount_raw = self.amount_var.get().strip()
        random_enabled = amount_mode == self.AMOUNT_MODE_RANDOM

        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "未设置网络，无法转账")
            return None

        token = self._selected_token(with_message=True)
        if not token:
            return None
        if (not token.is_native) and (not self.client.is_address(token.contract)):
            messagebox.showerror("参数错误", "代币合约地址格式错误")
            return None
        decimals = int(token.decimals)
        if decimals < 0 or decimals > self.MAX_TOKEN_DECIMALS:
            messagebox.showerror("参数错误", f"代币精度无效：{decimals}")
            return None
        coin = token.symbol.strip().upper()
        if not coin:
            messagebox.showerror("参数错误", "币种无效")
            return None

        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        else:
            if amount_mode == self.AMOUNT_MODE_FIXED and not amount_raw:
                messagebox.showerror("参数错误", "转账数量不能为空")
                return None
            try:
                if amount_mode == self.AMOUNT_MODE_FIXED:
                    v = Decimal(amount_raw)
                    if v <= 0:
                        raise InvalidOperation
                    amount = self._decimal_to_text(v)
                else:
                    amount = amount_raw or "0"
            except Exception:
                messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
                return None

        random_min: Decimal | None = None
        random_max: Decimal | None = None
        if random_enabled:
            min_raw = self.random_min_var.get().strip()
            max_raw = self.random_max_var.get().strip()
            try:
                random_min = Decimal(min_raw)
                random_max = Decimal(max_raw)
            except Exception:
                messagebox.showerror("参数错误", "随机金额最小值/最大值格式错误")
                return None
            if random_min <= 0 or random_max <= 0:
                messagebox.showerror("参数错误", "随机金额最小值和最大值必须大于 0")
                return None
            if random_max < random_min:
                messagebox.showerror("参数错误", "随机金额最大值必须大于或等于最小值")
                return None

        try:
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return None
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
        except Exception:
            messagebox.showerror("参数错误", "执行线程数格式错误")
            return None

        if hasattr(self, "mode_var") and self._is_mode_m1():
            target = self.target_address_var.get().strip()
            if not target:
                messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return None
            try:
                safe_target = self._validate_recipient_address(target, "收款地址")
            except Exception as exc:
                messagebox.showerror("参数错误", str(exc))
                return None
            self.target_address_var.set(safe_target)

        return WithdrawRuntimeParams(
            coin=coin,
            amount=amount,
            network=network,
            delay=delay,
            threads=threads,
            random_enabled=random_enabled,
            random_min=random_min,
            random_max=random_max,
            token_contract=token.contract if not token.is_native else "",
            token_decimals=decimals,
            token_is_native=bool(token.is_native),
        )

    def _resolve_wallet(self, source: str) -> tuple[str, str]:
        source_key = source.strip()
        if not source_key:
            raise RuntimeError("转出钱包私钥/助记词不能为空")
        if self.client.is_address(source_key):
            raise RuntimeError("当前数据仅包含钱包地址，无法签名转账。真实转账请提供私钥或助记词。")
        with self.wallet_cache_lock:
            cached_pk = self.source_private_key_cache.get(source_key)
            cached_addr = self.source_address_cache.get(source_key)
            if cached_pk and cached_addr:
                return cached_pk, cached_addr
        private_key = self.client.credential_to_private_key(source_key)
        address = self.client.address_from_private_key(private_key)
        with self.wallet_cache_lock:
            self.source_private_key_cache[source_key] = private_key
            self.source_address_cache[source_key] = address
        return private_key, address

    def _resolve_source_address(self, source: str) -> str:
        source_key = source.strip()
        if not source_key:
            raise RuntimeError("转出钱包不能为空")
        with self.wallet_cache_lock:
            cached_addr = self.source_address_cache.get(source_key)
            if cached_addr:
                return cached_addr
        if self.client.is_address(source_key):
            address = self.client.validate_evm_address(source_key, "转出钱包地址")
            with self.wallet_cache_lock:
                self.source_address_cache[source_key] = address
            return address
        _private_key, address = self._resolve_wallet(source_key)
        return address

    def _ensure_signing_sources(self, sources: list[str]) -> None:
        address_only = [s.strip() for s in sources if self.client.is_address(s.strip())]
        if not address_only:
            return
        count = len(address_only)
        sample = address_only[0]
        raise RuntimeError(
            f"当前选择的数据里有 {count} 条仅包含钱包地址，无法真实转账。请把左列改为私钥或助记词后再执行。示例：{sample}"
        )

    def start_query_balance(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            mode = self._mode()

            if mode == self.MODE_1M:
                source = self.source_credential_var.get().strip()
                selected_targets = [t for t in self.store.one_to_many_addresses if self._one_to_many_key(t) in self.checked_row_keys]
                targets: list[str] = []
                seen_t: set[str] = set()
                for target in selected_targets:
                    t = target.strip()
                    if not t or t in seen_t:
                        continue
                    seen_t.add(t)
                    targets.append(t)
                if not targets:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                self._mark_query_status_contexts([self._one_to_many_key(t) for t in targets], source)
                self._set_query_statuses([self._one_to_many_key(t) for t in targets], "waiting")
                self.stop_requested.clear()
                self.is_running = True
                threading.Thread(
                    target=self._run_query_balance_one_to_many,
                    args=(network, token, source, targets),
                    daemon=True,
                ).start()
                return

            if mode == self.MODE_M1:
                target = self.target_address_var.get().strip()
                if target:
                    try:
                        target = self._validate_recipient_address(target, "收款地址")
                        self.target_address_var.set(target)
                    except Exception as exc:
                        self.log(f"{exc}，已跳过收款地址余额查询")
                        target = ""
                selected_sources = [s for s in self.store.many_to_one_sources if self._many_to_one_key(s) in self.checked_row_keys]
                sources: list[str] = []
                seen_s: set[str] = set()
                for source in selected_sources:
                    s = source.strip()
                    if not s or s in seen_s:
                        continue
                    seen_s.add(s)
                    sources.append(s)
                if not sources:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                target_context = self._normalize_context_target(target) if target else ""
                self._mark_query_status_contexts([self._many_to_one_key(s) for s in sources], target_context)
                self._set_query_statuses([self._many_to_one_key(s) for s in sources], "waiting")
                self.stop_requested.clear()
                self.is_running = True
                threading.Thread(
                    target=self._run_query_balance_many_to_one,
                    args=(network, token, target, sources),
                    daemon=True,
                ).start()
                return

            selected_pairs = [x for x in self.store.multi_to_multi_pairs if self._m2m_key(x) in self.checked_row_keys]
            sources: list[str] = []
            seen: set[str] = set()
            row_keys_by_source: dict[str, list[str]] = {}
            for item in selected_pairs:
                s = item.source.strip()
                if not s or s in seen:
                    if s:
                        row_keys_by_source.setdefault(s, []).append(self._m2m_key(item))
                    continue
                seen.add(s)
                sources.append(s)
                row_keys_by_source[s] = [self._m2m_key(item)]
            if not sources:
                messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                return

            self._query_row_keys_by_source = {k: list(v) for k, v in row_keys_by_source.items()}
            self._set_query_statuses([key for keys in row_keys_by_source.values() for key in keys], "waiting")
            self.stop_requested.clear()
            self.is_running = True
            threading.Thread(target=self._run_query_balance_for_sources, args=(network, token, sources), daemon=True).start()
        except Exception as exc:
            self.log(f"余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_query_balance_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            if self._warn_if_draft_selected():
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            _row_key, source, target = job
            mode = self._mode()
            if mode == self.MODE_1M:
                try:
                    target = self._validate_recipient_address(target, "接收地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
                self.stop_requested.clear()
                self.is_running = True
                self._mark_query_status_context(self._one_to_many_key(target), source)
                self._set_query_status(self._one_to_many_key(target), "waiting")
                threading.Thread(
                    target=self._run_query_balance_one_to_many,
                    args=(network, token, source, [target]),
                    daemon=True,
                ).start()
                return
            if mode == self.MODE_M1:
                target_addr = target
                if target_addr:
                    try:
                        target_addr = self._validate_recipient_address(target_addr, "收款地址")
                        self.target_address_var.set(target_addr)
                    except Exception as exc:
                        self.log(f"{exc}，已跳过收款地址余额查询")
                        target_addr = ""
                self.stop_requested.clear()
                self.is_running = True
                self._mark_query_status_context(self._many_to_one_key(source), self._normalize_context_target(target_addr))
                self._set_query_status(self._many_to_one_key(source), "waiting")
                threading.Thread(
                    target=self._run_query_balance_many_to_one,
                    args=(network, token, target_addr, [source]),
                    daemon=True,
                ).start()
                return
            self._query_row_keys_by_source = {source: [_row_key]}
            self._set_query_status(_row_key, "waiting")
            self.stop_requested.clear()
            self.is_running = True
            threading.Thread(
                target=self._run_query_balance_for_sources,
                args=(network, token, [source]),
                daemon=True,
            ).start()
        except Exception as exc:
            self.log(f"当前行余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_transfer_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            if self._warn_if_draft_selected():
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            row_key, source, target = job
            mode = self._mode()
            if mode == self.MODE_1M and not source:
                messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return
            if mode in {self.MODE_M2M, self.MODE_1M}:
                try:
                    target = self._validate_recipient_address(target, "接收地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            if mode == self.MODE_M1:
                if not target:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                    return
                try:
                    target = self._validate_recipient_address(target, "收款地址")
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
                self.target_address_var.set(target)

            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
                try:
                    self._ensure_signing_sources([source])
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将执行链上当前行真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：1\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了当前行真实链上转账")
                    return

            self.log(f"已启动当前行转账任务：mode={self._mode()}，币种={token_desc}")
            self.stop_requested.clear()
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=([(row_key, source, target)], params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"当前行转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def _run_query_balance_for_sources(self, network: str, token: EvmToken, sources: list[str]):
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            dispatch_ui = self._dispatch_ui
            symbol = token.symbol
            token_desc = symbol if token.is_native else f"{symbol}({self._short_contract(token.contract)})"
            row_keys_by_source = {k: list(v) for k, v in getattr(self, "_query_row_keys_by_source", {}).items()}
            if not row_keys_by_source:
                row_keys_by_source = {}
                for item in getattr(self.store, "multi_to_multi_pairs", []):
                    src = item.source.strip()
                    if src in sources:
                        row_keys_by_source.setdefault(src, []).append(self._m2m_key(item))
            progress_keys = self._unique_row_keys([key for src in sources for key in row_keys_by_source.get(src, [])])
            dispatch_ui(lambda keys=progress_keys: self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=[key for src in sources for key in row_keys_by_source.get(src, [])]: self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：网络={network}，币种={token_desc}，钱包数={len(sources)}"))
            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, source in enumerate(sources, start=1):
                jobs.put((i, source))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, source = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_keys = list(row_keys_by_source.get(source, []))
                    prefix = f"[{i}/{len(sources)}]"
                    dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "running"))
                    try:
                        addr = self._resolve_source_address(source)
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.source_balance_cache[source] = amount
                            total += amount
                            ok += 1
                            done_row_keys.update(row_keys)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            done_row_keys.update(row_keys)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        dispatch_ui(lambda keys=row_keys: self._set_query_statuses(keys, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = max(1, min(self._runtime_worker_threads(), len(sources)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in progress_keys if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))
            summary = f"余额查询结束：成功 {ok}，失败 {failed}，{symbol} 合计={self._decimal_to_text(total)}"
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self._query_row_keys_by_source = {}
            self.is_running = False

    def _run_query_balance_one_to_many(self, network: str, token: EvmToken, source: str, targets: list[str]):
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            dispatch_ui = self._dispatch_ui
            symbol = token.symbol
            token_desc = self._token_desc(token)
            src_count = 1 if source else 0
            context_sig = source.strip()
            row_keys_by_target = {target: self._one_to_many_key(target) for target in targets}
            self._mark_query_status_contexts(list(row_keys_by_target.values()), context_sig)
            dispatch_ui(lambda keys=self._unique_row_keys(list(row_keys_by_target.values())): self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=list(row_keys_by_target.values()): self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：模式=1对多，网络={network}，币种={token_desc}，源钱包数={src_count}，目标地址数={len(targets)}"))

            # 先查询转出钱包余额，展示在私钥输入框旁
            source_addr = ""
            source_query_state = "skip"
            if source:
                try:
                    source_addr = self._resolve_source_address(source)
                    if token.is_native:
                        src_units = self.client.get_balance_wei(network, source_addr)
                    else:
                        src_units = self.client.get_erc20_balance(network, token.contract, source_addr)
                    src_amount = self._units_to_amount(src_units, token.decimals)
                    self.source_balance_cache[source] = src_amount
                    src_text = f"{symbol}:{self._decimal_to_text(src_amount)}"
                    dispatch_ui(lambda c=context_sig, t=src_text: self._apply_source_balance_summary(c, t))
                    dispatch_ui(lambda m=f"[SRC][{self._mask(source_addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(src_amount)}": self.log(m))
                    source_query_state = "ok"
                except Exception as exc:
                    dispatch_ui(lambda c=context_sig: self._clear_source_balance_summary(c))
                    dispatch_ui(lambda m=f"[SRC] 查询失败：{exc}": self.log(m))
                    source_query_state = "failed"
            else:
                dispatch_ui(lambda: self.source_balance_var.set("-"))
                dispatch_ui(lambda: self.log("[SRC] 未提供转出钱包，已跳过源钱包余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, addr in enumerate(targets, start=1):
                jobs.put((i, addr))
            if not targets:
                dispatch_ui(lambda: self.log("[DST] 未提供目标地址，已跳过目标地址余额查询"))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, addr = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_key = row_keys_by_target.get(addr, "")
                    prefix = f"[{i}/{len(targets)}]"
                    if row_key:
                        self._mark_query_status_context(row_key, context_sig)
                        dispatch_ui(lambda k=row_key: self._set_query_status(k, "running"))
                    try:
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.target_balance_cache[addr] = amount
                            total += amount
                            ok += 1
                            if row_key:
                                done_row_keys.add(row_key)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            if row_key:
                                done_row_keys.add(row_key)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = min(self._runtime_worker_threads(), len(targets))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in self._unique_row_keys(list(row_keys_by_target.values())) if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))

            src_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(source_query_state, source_query_state)
            summary = (
                f"余额查询结束：源钱包查询={src_status_text}，目标地址成功 {ok}，失败 {failed}，"
                f"目标地址{symbol}合计={self._decimal_to_text(total)}"
            )
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self.is_running = False

    def _run_query_balance_many_to_one(self, network: str, token: EvmToken, target: str, sources: list[str]):
        try:
            set_ui_batch_size(self, self._runtime_worker_threads())
            dispatch_ui = self._dispatch_ui
            symbol = token.symbol
            token_desc = self._token_desc(token)
            target_count = 1 if target else 0
            context_sig = self._normalize_context_target(target)
            row_keys_by_source = {source: self._many_to_one_key(source) for source in sources}
            self._mark_query_status_contexts(list(row_keys_by_source.values()), context_sig)
            dispatch_ui(lambda keys=self._unique_row_keys(list(row_keys_by_source.values())): self._begin_progress("query", keys))
            dispatch_ui(lambda a=self._token_amount_text(symbol, Decimal("0")): self._set_progress_metrics(balance_text=a))
            dispatch_ui(lambda keys=list(row_keys_by_source.values()): self._set_query_statuses(keys, "waiting"))
            dispatch_ui(lambda: self.log(f"开始查询余额：模式=多对1，网络={network}，币种={token_desc}，收款地址数={target_count}，源钱包数={len(sources)}"))

            target_query_state = "skip"
            if target:
                try:
                    if token.is_native:
                        target_units = self.client.get_balance_wei(network, target)
                    else:
                        target_units = self.client.get_erc20_balance(network, token.contract, target)
                    target_amount = self._units_to_amount(target_units, token.decimals)
                    self.target_balance_cache[target] = target_amount
                    target_text = f"{symbol}:{self._decimal_to_text(target_amount)}"
                    dispatch_ui(lambda c=context_sig, t=target_text: self._apply_target_balance_summary(c, t))
                    dispatch_ui(lambda m=f"[DST][{self._mask(target, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(target_amount)}": self.log(m))
                    target_query_state = "ok"
                except Exception as exc:
                    dispatch_ui(lambda c=context_sig: self._clear_target_balance_summary(c))
                    dispatch_ui(lambda m=f"[DST] 查询失败：{exc}": self.log(m))
                    target_query_state = "failed"
            else:
                dispatch_ui(lambda: self.target_balance_var.set("-"))
                dispatch_ui(lambda: self.log("[DST] 未提供收款地址，已跳过收款地址余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            done_row_keys: set[str] = set()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, source in enumerate(sources, start=1):
                jobs.put((i, source))
            if not sources:
                dispatch_ui(lambda: self.log("[SRC] 未提供源钱包，已跳过源钱包余额查询"))

            def worker():
                nonlocal ok, failed, total
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, source = jobs.get_nowait()
                    except queue.Empty:
                        return
                    row_key = row_keys_by_source.get(source, "")
                    prefix = f"[{i}/{len(sources)}]"
                    if row_key:
                        self._mark_query_status_context(row_key, context_sig)
                        dispatch_ui(lambda k=row_key: self._set_query_status(k, "running"))
                    try:
                        addr = self._resolve_source_address(source)
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.source_balance_cache[source] = amount
                            total += amount
                            ok += 1
                            if row_key:
                                done_row_keys.add(row_key)
                            balance_total_text = self._token_amount_text(symbol, total)
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        dispatch_ui(lambda m=msg: self.log(m))
                        dispatch_ui(lambda a=balance_total_text: self._set_progress_metrics(balance_text=a))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "success"))
                    except Exception as exc:
                        with lock:
                            failed += 1
                            if row_key:
                                done_row_keys.add(row_key)
                        dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                        if row_key:
                            self._mark_query_status_context(row_key, context_sig)
                            dispatch_ui(lambda k=row_key: self._set_query_status(k, "failed"))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = min(self._runtime_worker_threads(), len(sources))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                pending_keys = [key for key in self._unique_row_keys(list(row_keys_by_source.values())) if key not in done_row_keys]
                failed += len(pending_keys)
                dispatch_ui(lambda keys=pending_keys: self._set_query_statuses(keys, "failed"))
                dispatch_ui(lambda: self.log("余额查询已停止"))

            dst_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(target_query_state, target_query_state)
            summary = (
                f"余额查询结束：收款地址查询={dst_status_text}，源钱包成功 {ok}，失败 {failed}，"
                f"源钱包{symbol}合计={self._decimal_to_text(total)}"
            )
            dispatch_ui(lambda: self._refresh_tree())
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda s=ok, f=failed: self._finish_progress("query", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("query", s, f))
        finally:
            self.is_running = False

    def _resolve_amount_and_gas(self, params: WithdrawRuntimeParams, source_addr: str, target_addr: str) -> tuple[int, int, int, str]:
        target_addr = self._validate_recipient_address(target_addr, "接收地址")
        gas_price = self.client.get_gas_price_wei(params.network)

        if params.token_is_native:
            gas_limit = self.client.NATIVE_GAS_LIMIT
            gas_cost = gas_price * gas_limit

            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_dec = self._random_decimal_between(params.random_min, params.random_max)
                if amount_dec <= 0:
                    raise RuntimeError("随机金额生成失败：结果必须大于 0")
                value_units = self._amount_to_units(amount_dec, params.token_decimals)
                amount_text = f"{amount_dec:.2f}"
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                if balance_units < value_units + gas_cost:
                    raise RuntimeError("余额不足（随机金额 + gas）")
                return value_units, gas_price, gas_limit, amount_text

            if params.amount == self.AMOUNT_ALL_LABEL:
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                value_units = balance_units - gas_cost
                if value_units <= 0:
                    raise RuntimeError("余额不足以覆盖 gas")
                amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
                return value_units, gas_price, gas_limit, amount_text

            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            if value_units <= 0:
                raise RuntimeError("转账数量过小")
            balance_units = self.client.get_balance_wei(params.network, source_addr)
            if balance_units < value_units + gas_cost:
                raise RuntimeError("余额不足（固定金额 + gas）")
            return value_units, gas_price, gas_limit, self._decimal_to_text(amount_dec)

        token_contract = params.token_contract.strip()
        if not token_contract:
            raise RuntimeError("代币合约未设置")
        token_balance_units = self.client.get_erc20_balance(params.network, token_contract, source_addr)
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_dec = self._random_decimal_between(params.random_min, params.random_max)
            if amount_dec <= 0:
                raise RuntimeError("随机金额生成失败：结果必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = f"{amount_dec:.2f}"
        elif params.amount == self.AMOUNT_ALL_LABEL:
            value_units = token_balance_units
            amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
        else:
            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = self._decimal_to_text(amount_dec)
        if value_units <= 0:
            raise RuntimeError("代币余额为 0 或转账数量过小")
        if token_balance_units < value_units:
            raise RuntimeError("代币余额不足")

        gas_limit = self.client.estimate_erc20_transfer_gas(
            params.network,
            source_addr,
            token_contract,
            target_addr,
            value_units,
        )
        gas_cost = gas_price * gas_limit
        native_balance_units = self.client.get_balance_wei(params.network, source_addr)
        if native_balance_units < gas_cost:
            raise RuntimeError("原生币余额不足，无法支付 gas")
        return value_units, gas_price, gas_limit, amount_text

    def start_batch_transfer(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
                try:
                    self._ensure_signing_sources([source for _row_key, source, _target in jobs])
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            if params.amount == self.AMOUNT_ALL_LABEL:
                source_counter: dict[str, int] = {}
                for _k, source, _target in jobs:
                    source_counter[source] = source_counter.get(source, 0) + 1
                duplicated = [s for s, c in source_counter.items() if c > 1]
                if duplicated:
                    messagebox.showerror("参数错误", "数量为“全部”时，同一个转出钱包只能执行 1 条任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将执行链上真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了真实链上转账")
                    return

            self.log(f"已启动批量转账任务准备：任务={len(jobs)}，币种={token_desc}")
            self.stop_requested.clear()
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=(jobs, params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"批量转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_retry_failed(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_failed_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
                try:
                    self._ensure_signing_sources([source for _row_key, source, _target in jobs])
                except Exception as exc:
                    messagebox.showerror("参数错误", str(exc))
                    return
            if params.amount == self.AMOUNT_ALL_LABEL:
                source_counter: dict[str, int] = {}
                for _k, source, _target in jobs:
                    source_counter[source] = source_counter.get(source, 0) + 1
                duplicated = [s for s, c in source_counter.items() if c > 1]
                if duplicated:
                    messagebox.showerror("参数错误", "重试模式下，数量为“全部”时同一个转出钱包只能处理 1 条失败任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将重试链上失败转账：\n"
                    f"模式：{self._mode()}\n"
                    f"失败任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("重试确认", text):
                    self.log("用户取消了失败重试")
                    return

            self.log(f"开始重试失败任务：{len(jobs)}")
            self.stop_requested.clear()
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=(jobs, params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"失败重试启动异常：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def _run_batch_transfer(self, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
        try:
            set_ui_batch_size(self, params.threads)
            dispatch_ui = self._dispatch_ui
            def job_prefix(index: int) -> str:
                return f"[{index}/{len(jobs_data)}]"

            amount_view = params.amount
            token_desc = self._token_desc_from_params(params)
            progress_keys = self._unique_row_keys([row_key for row_key, _source, _target in jobs_data])
            track_amount_total = not (dry_run and params.amount == self.AMOUNT_ALL_LABEL)
            context_by_row_key = {
                row_key: self._row_context_for_values(row_key, source, target)
                for row_key, source, target in jobs_data
            }
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
            dispatch_ui(lambda keys=progress_keys: self._begin_progress("transfer", keys))
            dispatch_ui(
                lambda a=self._token_amount_text(params.coin, Decimal("0")), g=("-" if dry_run else self._estimated_gas_fee_text(params.network, 0)), amt_known=track_amount_total: self._set_progress_metrics(
                    amount_text=(a if amt_known else "-"),
                    gas_text=g,
                ),
            )
            dispatch_ui(
                lambda: self.log(
                    f"开始批量链上转账：mode={self._mode()}，任务={len(jobs_data)}，network={params.network}，"
                    f"coin={token_desc}，amount={amount_view}，delay={params.delay}，threads={params.threads}，dry_run={dry_run}"
                ),
            )
            fallback_prefixes: dict[str, str] = {}
            for row_key, _source, _target in jobs_data:
                fallback_prefixes[row_key] = job_prefix(len(fallback_prefixes) + 1)
                self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                dispatch_ui(lambda k=row_key: self._set_status(k, "waiting"))

            success = 0
            failed = 0
            resolved = 0
            total_amount = Decimal("0")
            total_gas_fee_wei = 0
            lock = threading.Lock()
            resolved_event = threading.Event()
            resolved_row_keys: set[str] = set()
            nonce_lock_map: dict[str, threading.Lock] = {}
            nonce_next_map: dict[str, int] = {}
            nonce_guard = threading.Lock()

            jobs_q: queue.Queue[tuple[int, str, str, str]] = queue.Queue()
            for i, item in enumerate(jobs_data, start=1):
                row_key, source, target = item
                jobs_q.put((i, row_key, source, target))

            def alloc_nonce(source_addr: str) -> int:
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    nonce = nonce_next_map.get(source_addr)
                    if nonce is None:
                        nonce = self.client.get_nonce(params.network, source_addr)
                    nonce_next_map[source_addr] = nonce + 1
                    return nonce

            def rollback_nonce(source_addr: str, used_nonce: int):
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    cached_next = nonce_next_map.get(source_addr)
                    # 仅在当前缓存恰好对应本次分配的 nonce 时回滚，避免覆盖其他线程进度。
                    if cached_next == used_nonce + 1:
                        nonce_next_map.pop(source_addr, None)

            def finalize_job(row_key: str, result_status: str, msg: str, *, amount_text: str = "", gas_fee_wei: int = 0):
                nonlocal success, failed, resolved, total_amount, total_gas_fee_wei
                context_sig = context_by_row_key.get(row_key, "")
                row_status_text = ""
                with lock:
                    if row_key in resolved_row_keys:
                        return
                    resolved_row_keys.add(row_key)
                    if result_status == "success":
                        success += 1
                        row_status_text = self._success_status_text(params.coin, amount_text)
                        if track_amount_total:
                            try:
                                total_amount += Decimal(amount_text)
                            except Exception:
                                pass
                    else:
                        failed += 1
                    total_gas_fee_wei += gas_fee_wei
                    amount_total_text = self._token_amount_text(params.coin, total_amount) if track_amount_total else "-"
                    gas_total_text = "-" if dry_run else self._estimated_gas_fee_text(params.network, total_gas_fee_wei)
                    resolved += 1
                    done = resolved >= len(jobs_data)
                if done:
                    resolved_event.set()
                dispatch_ui(lambda m=msg: self.log(m))
                self._mark_row_status_context(row_key, context_sig)
                dispatch_ui(lambda k=row_key, s=result_status, t=row_status_text: self._set_status(k, s, t))
                dispatch_ui(lambda a=amount_total_text, g=gas_total_text: self._set_progress_metrics(amount_text=a, gas_text=g))

            def schedule_submitted_timeout(row_key: str, prefix: str, submitted_timeout_seconds: float):
                timeout_msg = f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，自动判定失败"

                def timeout_worker():
                    if submitted_timeout_seconds > 0:
                        time.sleep(submitted_timeout_seconds)
                    finalize_job(row_key, "failed", timeout_msg)

                if submitted_timeout_seconds > 0:
                    threading.Thread(target=timeout_worker, daemon=True).start()
                else:
                    timeout_worker()

            def worker():
                total = len(jobs_data)
                while True:
                    if self.stop_requested.is_set():
                        return
                    try:
                        i, row_key, source, target = jobs_q.get_nowait()
                    except queue.Empty:
                        return

                    result_status = "failed"
                    msg = ""
                    amount_text = ""
                    used_nonce: int | None = None
                    source_addr = ""
                    gas_fee_wei = 0
                    tx_sent = False
                    txid = ""
                    self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                    dispatch_ui(lambda k=row_key: self._set_status(k, "running"))
                    submitted_timeout_seconds = max(0.0, float(getattr(self, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS)))
                    prefix = job_prefix(i)
                    try:
                        if dry_run:
                            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                                amount_text = f"{self._random_decimal_between(params.random_min, params.random_max):.2f}"
                            elif params.amount == self.AMOUNT_ALL_LABEL:
                                amount_text = f"{self.AMOUNT_ALL_LABEL}(模拟)"
                            else:
                                amount_text = params.amount
                            prefix = f"[{i}/{total}][{self._mask_credential(source)}]"
                            msg = (
                                f"{prefix} 模拟成功 -> {params.coin} {amount_text} "
                                f"到 {self._mask(target, head=8, tail=6)}"
                            )
                            result_status = "success"
                        else:
                            private_key, source_addr = self._resolve_wallet(source)
                            value_units, gas_price_wei, gas_limit, amount_text = self._resolve_amount_and_gas(
                                params, source_addr, target
                            )
                            nonce = alloc_nonce(source_addr)
                            used_nonce = nonce
                            if params.token_is_native:
                                txid = self.client.send_native_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    to_address=target,
                                    value_wei=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            else:
                                txid = self.client.send_erc20_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    token_contract=params.token_contract,
                                    to_address=target,
                                    amount_units=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            tx_sent = True
                            if txid:
                                gas_fee_wei = max(0, int(gas_limit)) * max(0, int(gas_price_wei))
                                gas_text = self._gas_fee_amount_text(params.network, gas_fee_wei)
                                msg = (
                                    f"{prefix} 金额={params.coin} {amount_text}，预估gas={gas_text}，"
                                    f"from={source_addr}，to={target}，txid={txid}"
                                )
                                result_status = "success"
                            else:
                                msg = (
                                    f"{prefix} 转账确认中：未拿到 txid，"
                                    f"from={source_addr}，to={target}"
                                )
                                result_status = "submitted"
                    except Exception as exc:
                        if used_nonce is not None and source_addr and not tx_sent:
                            rollback_nonce(source_addr, used_nonce)
                        msg = f"[{i}/{total}] 转账失败：{exc}"
                    finally:
                        jobs_q.task_done()

                    if result_status == "submitted":
                        dispatch_ui(lambda m=msg: self.log(m))
                        self._mark_row_status_context(row_key, context_by_row_key.get(row_key, ""))
                        dispatch_ui(lambda k=row_key: self._set_status(k, "submitted"))
                        schedule_submitted_timeout(row_key, prefix, submitted_timeout_seconds)
                    else:
                        finalize_job(row_key, result_status, msg, amount_text=amount_text, gas_fee_wei=gas_fee_wei)
                    if params.delay > 0:
                        if self.stop_requested.wait(params.delay):
                            return

            workers: list[threading.Thread] = []
            worker_count = max(1, min(params.threads, len(jobs_data)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            if self.stop_requested.is_set():
                with lock:
                    pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
                for row_key in pending_row_keys:
                    prefix = fallback_prefixes.get(row_key, "")
                    stop_msg = f"{prefix} 已停止" if prefix else "任务已停止"
                    finalize_job(row_key, "failed", stop_msg)
                resolved_event.set()
                dispatch_ui(lambda: self.log("链上转账任务已停止"))
            batch_finalize_timeout_seconds = max(
                0.2,
                max(0.0, float(getattr(self, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS))) + 1.0,
            )
            if len(jobs_data) == 0:
                resolved_event.set()
            if not resolved_event.wait(batch_finalize_timeout_seconds):
                with lock:
                    pending_row_keys = [row_key for row_key in progress_keys if row_key not in resolved_row_keys]
                for row_key in pending_row_keys:
                    prefix = fallback_prefixes.get(row_key, "")
                    timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                    finalize_job(row_key, "failed", timeout_msg)

            summary = f"链上转账任务结束：成功 {success}，失败 {failed}"
            if not dry_run:
                summary = (
                    f"{summary}，转账总额={self._token_amount_text(params.coin, total_amount) if track_amount_total else '-'}，"
                    f"预估gas合计={self._gas_fee_amount_text(params.network, total_gas_fee_wei)}"
                )
            else:
                summary = f"{summary}，转账总额={self._token_amount_text(params.coin, total_amount) if track_amount_total else '-'}，预估gas合计=-"
            dispatch_ui(lambda: self.log(summary))
            dispatch_ui(lambda: messagebox.showinfo("执行完成", summary))
            dispatch_ui(lambda s=success, f=failed: self._finish_progress("transfer", s, f))
        except Exception as exc:
            err_text = str(exc)
            dispatch_ui(lambda m=f"链上转账任务异常终止：{err_text}": self.log(m))
            dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
            dispatch_ui(
                lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: self._finish_progress("transfer", s, f),
            )
        finally:
            self.is_running = False
