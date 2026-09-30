#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""自动分仓独立设置窗；只修改自动判断参数，不接触正在运行的订单参数。"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from app_paths import STRATEGY_CONFIG_FILE
from exchange_app_base import _atomic_write_config_json, _load_authenticated_config, _require_dict_payload
from exchange_auto_ladder import AutoLadderSettings, export_auto_ladder_settings, import_auto_ladder_settings
from exchange_logging import logger
from stores import config_or_backup_exists


AUTO_LADDER_CONFIG_FILE = STRATEGY_CONFIG_FILE.with_name("auto_ladder_settings.json")
FIELDS = (
    ("volume_minutes", "成交额窗口（分钟）"),
    ("start_volume", "开始挂单成交额（计价币）"),
    ("recent_minutes", "近期有成交窗口（分钟）"),
    ("entry_confirm_seconds", "入场连续确认（秒）"),
    ("no_trade_seconds", "无成交暂停（秒）"),
    ("price_minutes", "入场价格窗口（分钟）"),
    ("price_ticks", "入场最大振幅（tick）"),
    ("pause_price_ticks", "运行中价格暂停幅度（tick）"),
    ("dry_minutes", "缩量观察窗口（分钟）"),
    ("dry_volume", "缩量暂停成交额（计价币）"),
    ("dry_confirm_minutes", "缩量连续确认（分钟）"),
    ("turn_seconds", "反向折返窗口（秒）"),
    ("turn_ticks", "最小反向幅度（tick）"),
    ("turn_count", "暂停折返次数"),
    ("stable_seconds", "恢复平稳时长（秒）"),
)
FIELD_HELP = {
    "volume_minutes": "入场看当前正在走的 1 分钟，加上前面已收盘的分钟，合计这么多分钟的成交额。例：填 5，就是当前分钟已发生的成交额＋前 4 个完整分钟；不必等当前分钟收盘。",
    "start_volume": "只有上述时间段的总成交额达到这个数，才允许首次入场或暂停后恢复。单位是交易对右边的计价币；例如 BTCUSDT 填 100000，就是至少成交 10 万 USDT。",
    "recent_minutes": "最近这几分钟里至少要有一笔成交，包含当前正在走的分钟。填 1 就要求当前分钟已有成交；这里只检查成交额大于 0，不要求达到启动门槛。",
    "entry_confirm_seconds": "首次入场前，成交额、价格、盘口与最近成交都必须连续达标这么久，才开始挂单。有一项不达标就重新计时；短暂获取失败会先重试，重试期间暂停计时。填 0 可关闭首次入场等待；暂停后恢复仍由“恢复平稳时长”控制。",
    "no_trade_seconds": "交易所公布的最近一笔成交距现在达到这么久，就暂时观察并尝试撤销挂单，不等几分钟的缩量窗口。再次出现成交后，还要满足其他条件才能入场或恢复。",
    "price_minutes": "入场看当前正在走的分钟和前面的完整分钟，取这段时间的最高价减最低价，与“入场最大振幅”比较。填 5 就是当前分钟＋前 4 分钟。",
    "price_ticks": "入场时允许的价格上下波动上限。tick 是该交易对最小的报价变化单位；如 1 tick=0.001，填 5 就是最近价格最高与最低最多相差 0.005。数值越小越难入场。",
    "pause_price_ticks": "运行中用这条较宽的线防突发价格风险：最近价格振幅、当前买卖价差，或实时盘口偏离近期价格区间超过这个幅度时，暂停新挂单并尝试撤单。价格重新稳定后仍需满足成交额、近期成交与恢复平稳时长才能再挂单。应不小于入场最大振幅。",
    "dry_minutes": "运行中用最近这些已经收盘的完整分钟的总成交额判断是否缩量，避免刚进入新分钟时因当前分钟成交尚少而误判。",
    "dry_volume": "滚动窗口的总成交额低于此值，算一次“缩量”。低于启动门槛但还没低于这里时，已启动的挂单不会仅因成交额下降而暂停。单位同样是计价币。",
    "dry_confirm_minutes": "需要连续多少次、每次相隔 1 分钟的缩量窗口都低于门槛，才暂停挂单。例：观察 5 分钟、确认 2 分钟，要连续两个重叠的 5 分钟窗口都缩量。",
    "turn_seconds": "观察最近多少秒的盘口中间价；中间价是买一价与卖一价的平均值。用于识别短时间来回折返。",
    "turn_ticks": "价格先朝一个方向走，再反向走至少多少 tick，才记作一次折返。小于这个幅度的抖动不计数；单向上涨或下跌也不算折返。",
    "turn_count": "在上述秒数内，折返次数达到此值就暂停，并尝试撤销当前挂单。数值越小越容易因来回波动暂停。",
    "stable_seconds": "暂停后，成交额和价格条件持续达标、且没有达到折返次数，保持这么久才重新挂单。短暂行情读取失败会冻结计时，恢复后接着算；连续失联达到 30 秒或条件不达标才重新计时。首次入场由“入场连续确认”控制。",
}


class _HoverHelp:
    """A small, non-interactive help bubble shared by one field's label and entry."""

    def __init__(self, parent: tk.Toplevel, widgets: tuple[tk.Widget, ...], message: str):
        self.parent = parent
        self.message = message
        self._after_id: str | None = None
        self._bubble: tk.Toplevel | None = None
        for widget in widgets:
            widget.bind("<Enter>", self._schedule, add="+")
            widget.bind("<Leave>", self._hide, add="+")
            widget.bind("<Button-1>", self._hide, add="+")
        parent.bind("<Destroy>", self._on_destroy, add="+")

    def _schedule(self, _event=None) -> None:
        self._hide()
        self._after_id = self.parent.after(350, self._show)

    def _show(self) -> None:
        self._after_id = None
        if not self.parent.winfo_exists():
            return
        bubble = tk.Toplevel(self.parent)
        bubble.withdraw()
        bubble.overrideredirect(True)
        label = tk.Label(
            bubble, text=self.message, justify="left", anchor="w", wraplength=390,
            background="#FFF9DB", foreground="#202020", relief="solid", borderwidth=1,
            padx=10, pady=8,
        )
        label.pack()
        bubble.update_idletasks()
        x = min(self.parent.winfo_pointerx() + 12, bubble.winfo_screenwidth() - bubble.winfo_reqwidth() - 8)
        y = min(self.parent.winfo_pointery() + 18, bubble.winfo_screenheight() - bubble.winfo_reqheight() - 8)
        bubble.geometry(f"+{max(0, x)}+{max(0, y)}")
        bubble.deiconify()
        self._bubble = bubble

    def _hide(self, _event=None) -> None:
        if self._after_id is not None:
            self.parent.after_cancel(self._after_id)
            self._after_id = None
        if self._bubble is not None:
            self._bubble.destroy()
            self._bubble = None

    def _on_destroy(self, event) -> None:
        if event.widget is self.parent:
            self._hide()


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
        window.geometry("550x710")
        window.minsize(500, 660)
        self.auto_ladder_window = window
        window.protocol("WM_DELETE_WINDOW", window.destroy)
        main = ttk.Frame(window, padding=16)
        main.pack(fill="both", expand=True)
        ttk.Label(main, text="鼠标停在参数名称、输入框或 ⓘ 上可查看具体作用。暂停会尝试撤单，但不会自动卖出已持有的币；保存设置不会立即撤单。",
                  wraplength=500).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 12))
        variables: dict[str, tk.StringVar] = {}
        help_bubbles: list[_HoverHelp] = []
        for row, (name, label) in enumerate(FIELDS, start=1):
            field_label = ttk.Label(main, text=label)
            field_label.grid(row=row, column=0, sticky="e", padx=(0, 12), pady=4)
            variable = tk.StringVar(window, value=str(getattr(settings, name)))
            variables[name] = variable
            entry = ttk.Entry(main, textvariable=variable, width=20)
            entry.grid(row=row, column=1, sticky="w", pady=4)
            help_label = ttk.Label(main, text="ⓘ", foreground="#1664A3", cursor="question_arrow")
            help_label.grid(row=row, column=2, sticky="w", padx=(8, 0), pady=4)
            help_bubbles.append(_HoverHelp(window, (field_label, entry, help_label), FIELD_HELP[name]))
        window._auto_ladder_help_bubbles = help_bubbles

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

        share_status = tk.StringVar(window)

        def share_settings() -> None:
            try:
                current = AutoLadderSettings.parse({name: variable.get() for name, variable in variables.items()})
                self.clipboard_clear()
                self.clipboard_append(export_auto_ladder_settings(current))
            except (ValueError, tk.TclError) as exc:
                messagebox.showerror("分享失败", str(exc), parent=window)
                return
            share_status.set("已复制当前表单参数，可发送到另一台机器。")

        def import_settings() -> None:
            try:
                imported = import_auto_ladder_settings(self.clipboard_get())
            except (ValueError, tk.TclError) as exc:
                messagebox.showerror("导入失败", str(exc), parent=window)
                return
            for name, variable in variables.items():
                variable.set(str(getattr(imported, name)))
            share_status.set("已填入导入参数；检查后点击“保存并运行”生效。")

        ttk.Label(main, text="运行中保存立即更新判断参数；已有下单档位、金额、溢价和重挂设置不变。",
                  wraplength=500).grid(row=len(FIELDS)+1, column=0, columnspan=3, sticky="w", pady=(10, 6))
        button_row = len(FIELDS) + 2
        ttk.Button(main, text="分享策略", command=share_settings).grid(row=button_row, column=0, sticky="e", pady=(6, 0))
        ttk.Button(main, text="导入策略", command=import_settings).grid(row=button_row, column=1, sticky="w", pady=(6, 0))
        ttk.Button(main, text="保存并运行", command=save_and_run).grid(row=button_row, column=2, sticky="w", pady=(6, 0))
        ttk.Label(main, textvariable=share_status, wraplength=500).grid(
            row=button_row+1, column=0, columnspan=3, sticky="w", pady=(6, 0)
        )
