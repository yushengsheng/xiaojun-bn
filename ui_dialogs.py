#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, Y, Scrollbar, Text, Toplevel
from tkinter import messagebox, ttk

from core_models import AccountEntry


class PasteImportDialog:
    def __init__(self, parent, one_to_many: bool = False):
        self.top = Toplevel(parent)
        self.top.title("粘贴批量账号")
        self.top.geometry("860x520")
        self.one_to_many = one_to_many
        self.result: list[AccountEntry] | list[str] | None = None

        frame = ttk.Frame(self.top, padding=12)
        frame.pack(fill=BOTH, expand=True)

        if self.one_to_many:
            hint = "每行一个提现地址。空行和 # 开头注释会自动忽略。"
        else:
            hint = (
                "每行一个账号，格式：api_key api_secret 提现地址\n"
                "支持空格、Tab、逗号、分号分隔。空行和 # 开头注释会自动忽略。"
            )
        ttk.Label(frame, text=hint, foreground="#666").pack(anchor=W, pady=(0, 8))

        text_wrap = ttk.Frame(frame)
        text_wrap.pack(fill=BOTH, expand=True)
        self.text = Text(text_wrap, height=22)
        text_bar = Scrollbar(text_wrap, orient=VERTICAL, command=self.text.yview, width=14)
        for k, v in {
            "bg": "#bcbcbc",
            "activebackground": "#8f8f8f",
            "troughcolor": "#ececec",
            "relief": "raised",
            "bd": 1,
        }.items():
            try:
                text_bar.configure(**{k: v})
            except Exception:
                pass
        self.text.configure(yscrollcommand=text_bar.set)
        self.text.pack(side=LEFT, fill=BOTH, expand=True)
        text_bar.pack(side=RIGHT, fill=Y)

        btn = ttk.Frame(frame)
        btn.pack(fill="x", pady=(10, 0))
        ttk.Button(btn, text="取消", command=self.top.destroy).pack(side=RIGHT)
        ttk.Button(btn, text="导入", command=self._confirm).pack(side=RIGHT, padx=(0, 8))

        self.top.grab_set()
        self.top.focus_set()

    def _parse_line(self, line: str) -> AccountEntry | str:
        if self.one_to_many:
            s = line.strip()
            if not s:
                raise ValueError("地址不能为空")
            return s
        arr = [x for x in re.split(r"[\s,;]+", line.strip()) if x]
        if len(arr) < 3:
            raise ValueError("字段不足，至少需要 3 列")
        return AccountEntry(api_key=arr[0], api_secret=arr[1], address=arr[2])

    def _confirm(self):
        lines = self.text.get("1.0", END).splitlines()
        result: list[AccountEntry] | list[str] = []
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            try:
                result.append(self._parse_line(s))
            except Exception as exc:
                messagebox.showerror("格式错误", f"第 {i} 行解析失败：{exc}\n内容：{line}")
                return

        if not result:
            messagebox.showwarning("提示", "没有可导入的数据")
            return

        self.result = result
        self.top.destroy()

