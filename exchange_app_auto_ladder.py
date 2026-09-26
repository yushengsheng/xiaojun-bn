#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""自动分仓独立设置窗；只修改自动判断参数，不接触正在运行的订单参数。"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from app_paths import STRATEGY_CONFIG_FILE
from exchange_app_base import _atomic_write_config_json, _load_authenticated_config, _require_dict_payload
from exchange_auto_ladder import AutoLadderSettings
from exchange_logging import logger
from stores import config_or_backup_exists


AUTO_LADDER_CONFIG_FILE = STRATEGY_CONFIG_FILE.with_name("auto_ladder_settings.json")
FIELDS = (
    ("volume_minutes", "成交额窗口（分钟）"),
    ("start_volume", "开始挂单成交额（计价币）"),
    ("recent_minutes", "近期有成交窗口（分钟）"),
    ("price_minutes", "入场价格窗口（分钟）"),
    ("price_ticks", "入场最大振幅（tick）"),
    ("dry_minutes", "缩量观察窗口（分钟）"),
    ("dry_volume", "缩量暂停成交额（计价币）"),
    ("dry_confirm_minutes", "缩量连续确认（分钟）"),
    ("turn_seconds", "反向折返窗口（秒）"),
    ("turn_ticks", "最小反向幅度（tick）"),
    ("turn_count", "暂停折返次数"),
    ("stable_seconds", "恢复平稳时长（秒）"),
)


class ExchangeAppAutoLadderMixin:
    def _load_auto_ladder_settings(self) -> None:
        if not config_or_backup_exists(AUTO_LADDER_CONFIG_FILE):
            return
        try:
            raw, recovered = _load_authenticated_config(AUTO_LADDER_CONFIG_FILE, validator=_require_dict_payload)
            self.auto_ladder_settings_store.replace(AutoLadderSettings.parse(raw))
            if recovered:
                logger.warning("自动分仓设置已从备份恢复：%s", AUTO_LADDER_CONFIG_FILE)
        except Exception as exc:
            logger.error("自动分仓设置读取失败：%s", exc)
            messagebox.showwarning("自动分仓设置读取失败", str(exc), parent=self)

    def open_auto_ladder_settings(self) -> None:
        existing = getattr(self, "auto_ladder_window", None)
        if existing is not None and existing.winfo_exists():
            existing.lift()
            existing.focus_force()
            return
        settings, _ = self.auto_ladder_settings_store.snapshot()
        window = tk.Toplevel(self)
        window.title("自动分仓策略设置")
        window.transient(self)
        window.geometry("520x590")
        window.minsize(470, 530)
        self.auto_ladder_window = window
        window.protocol("WM_DELETE_WINDOW", window.destroy)
        main = ttk.Frame(window, padding=16)
        main.pack(fill="both", expand=True)
        ttk.Label(main, text="仅调整自动入场、暂停和恢复条件；已挂订单不会因保存立即撤销。",
                  wraplength=465).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 12))
        variables: dict[str, tk.StringVar] = {}
        for row, (name, label) in enumerate(FIELDS, start=1):
            ttk.Label(main, text=label).grid(row=row, column=0, sticky="e", padx=(0, 12), pady=4)
            variable = tk.StringVar(window, value=str(getattr(settings, name)))
            variables[name] = variable
            ttk.Entry(main, textvariable=variable, width=20).grid(row=row, column=1, sticky="w", pady=4)

        def save_and_run() -> None:
            try:
                updated = AutoLadderSettings.parse({name: variable.get() for name, variable in variables.items()})
                _atomic_write_config_json(AUTO_LADDER_CONFIG_FILE, updated.as_dict())
                self.auto_ladder_settings_store.replace(updated)
            except Exception as exc:
                logger.error("保存自动分仓设置失败：%s", exc)
                messagebox.showerror("设置失败", str(exc), parent=window)
                return
            logger.info("自动分仓判断参数已保存并生效；当前账号下次行情判断使用新版设置")
            window.destroy()

        ttk.Label(main, text="运行中保存立即更新判断参数；已有下单档位、金额、溢价和重挂设置不变。",
                  wraplength=465).grid(row=len(FIELDS)+1, column=0, columnspan=2, sticky="w", pady=(10, 6))
        ttk.Button(main, text="保存并运行", command=save_and_run).grid(
            row=len(FIELDS)+2, column=0, columnspan=2, pady=(6, 0)
        )
