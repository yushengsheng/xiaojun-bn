#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from page_onchain_base import *  # noqa: F401,F403


class OnchainWalletMixin(object):
    def _wallet_export_lines(self, format_name: str) -> list[str]:
        wallets = list(self.generated_wallets)
        if format_name == "仅地址":
            return [item.address for item in wallets]
        if format_name == "仅私钥":
            return [item.private_key for item in wallets]
        return [f"{item.address} {item.private_key}" for item in wallets]
    def _wallet_import_button_text(self) -> str:
        mode = self._mode()
        if mode == self.MODE_1M:
            return "导入地址到接收列表"
        if mode == self.MODE_M1:
            return "导入私钥到转出列表"
        return "导入私钥到转出列"
    def _update_wallet_generator_import_button(self) -> None:
        btn = getattr(self, "wallet_generator_import_btn", None)
        if btn is None:
            return
        try:
            btn.configure(text=self._wallet_import_button_text())
        except Exception:
            pass
    def _close_wallet_generator(self) -> None:
        window = getattr(self, "wallet_generator_window", None)
        if window is not None:
            try:
                window.destroy()
            except Exception:
                pass
        self.wallet_generator_window = None
        self.wallet_generator_tree = None
        self.wallet_generator_import_btn = None
        self.wallet_generator_generate_btn = None
    def open_wallet_generator(self):
        window = getattr(self, "wallet_generator_window", None)
        if window is not None:
            try:
                if window.winfo_exists():
                    window.deiconify()
                    window.lift()
                    window.focus_force()
                    self._update_wallet_generator_import_button()
                    self._refresh_generated_wallet_tree()
                    return
            except Exception:
                self.wallet_generator_window = None

        window = tk.Toplevel(self.root)
        window.title("批量创建 EVM 钱包")
        window.transient(self.root)
        window.geometry("980x560")
        window.minsize(860, 420)
        window.protocol("WM_DELETE_WINDOW", self._close_wallet_generator)
        self.wallet_generator_window = window

        main = ttk.Frame(window, padding=12)
        main.pack(fill=BOTH, expand=True)
        main.columnconfigure(0, weight=1)
        main.rowconfigure(2, weight=1)

        tips = ttk.LabelFrame(main, text="说明", padding=10)
        tips.grid(row=0, column=0, sticky="ew")
        ttk.Label(
            tips,
            text="本地离线批量创建 EVM 钱包，默认复用当前页面“执行线程数”；导入主表后的下方地址/钱包数据会自动保存，上方批量转账配置仍需单独点击“保存配置”。",
            style="Subtle.TLabel",
            wraplength=900,
            justify="left",
        ).pack(anchor="w")

        ctrl = ttk.Frame(main)
        ctrl.grid(row=1, column=0, sticky="ew", pady=(10, 10))
        ttk.Label(ctrl, text="创建数量").pack(side=LEFT)
        ent_count = ttk.Entry(ctrl, textvariable=self.wallet_generate_count_var, width=8)
        ent_count.pack(side=LEFT, padx=(6, 10))
        bind_paste_shortcuts(ent_count)
        self.wallet_generator_generate_btn = ttk.Button(ctrl, text="立即创建", command=self.generate_wallets)
        self.wallet_generator_generate_btn.pack(side=LEFT)
        ttk.Label(ctrl, text="导出格式").pack(side=LEFT, padx=(16, 6))
        fmt_box = ttk.Combobox(
            ctrl,
            textvariable=self.wallet_export_format_var,
            values=["地址 + 私钥", "仅地址", "仅私钥"],
            width=14,
            state="readonly",
        )
        fmt_box.pack(side=LEFT)
        ttk.Button(ctrl, text="复制到剪贴板", command=self.copy_generated_wallets).pack(side=LEFT, padx=(10, 0))
        ttk.Button(ctrl, text="导出 TXT", command=self.export_generated_wallets).pack(side=LEFT, padx=(8, 0))
        self.wallet_generator_import_btn = ttk.Button(ctrl, text=self._wallet_import_button_text(), command=self.import_generated_wallets)
        self.wallet_generator_import_btn.pack(side=LEFT, padx=(8, 0))
        ttk.Button(ctrl, text="关闭", command=self._close_wallet_generator).pack(side=RIGHT)

        table_box = ttk.Frame(main)
        table_box.grid(row=2, column=0, sticky="nsew")
        table_box.columnconfigure(0, weight=1)
        table_box.rowconfigure(0, weight=1)

        tree = ttk.Treeview(table_box, columns=("idx", "address", "private_key"), show="headings", height=16)
        tree.heading("idx", text="编号")
        tree.heading("address", text="地址")
        tree.heading("private_key", text="私钥")
        tree.column("idx", width=56, anchor="center", stretch=False)
        tree.column("address", width=330, anchor="w", stretch=True)
        tree.column("private_key", width=520, anchor="w", stretch=True)
        ybar = self._make_scrollbar(table_box, orient=VERTICAL, command=tree.yview)
        xbar = self._make_scrollbar(table_box, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        self.wallet_generator_tree = tree

        self._update_wallet_generator_import_button()
        self._refresh_generated_wallet_tree()
    def _set_wallet_generator_busy(self, busy: bool) -> None:
        btn = getattr(self, "wallet_generator_generate_btn", None)
        if btn is None:
            return
        try:
            btn.configure(state="disabled" if busy else "normal")
        except Exception:
            pass
    def _refresh_generated_wallet_tree(self) -> None:
        tree = getattr(self, "wallet_generator_tree", None)
        if tree is None:
            return
        tree.delete(*tree.get_children())
        for i, item in enumerate(self.generated_wallets, start=1):
            tree.insert("", END, iid=f"wallet_{i}", values=(i, item.address, item.private_key))
    def generate_wallets(self):
        try:
            count = int(str(self.wallet_generate_count_var.get()).strip())
        except Exception:
            messagebox.showerror("创建失败", "创建数量格式错误", parent=getattr(self, "wallet_generator_window", None) or self.root)
            return
        workers = self._runtime_worker_threads()
        self._set_wallet_generator_busy(True)
        self.log(f"钱包生成器开始创建：数量={count}，执行线程数={workers}")
        self._start_managed_thread(
            self._run_generate_wallets,
            args=(count, workers),
            name="onchain-generate-wallets",
        )
    def _run_generate_wallets(self, count: int, workers: int) -> None:
        try:
            wallets = self.client.create_wallets(count, worker_threads=workers)
        except Exception as exc:
            self._dispatch_ui(
                lambda err=str(exc): (
                    self._set_wallet_generator_busy(False),
                    messagebox.showerror("创建失败", err, parent=getattr(self, "wallet_generator_window", None) or self.root),
                )
            )
            return

        def apply_result() -> None:
            self.generated_wallets = wallets
            self._refresh_generated_wallet_tree()
            self._set_wallet_generator_busy(False)
            self.log(f"钱包生成器创建完成：新增 {len(wallets)} 个 EVM 钱包，执行线程数={workers}")

        self._dispatch_ui(apply_result)
    def copy_generated_wallets(self):
        if not self.generated_wallets:
            messagebox.showwarning("提示", "请先创建钱包", parent=getattr(self, "wallet_generator_window", None) or self.root)
            return
        lines = self._wallet_export_lines(self.wallet_export_format_var.get().strip())
        text = "\n".join(lines)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.root.update_idletasks()
        self.log(f"钱包生成器已复制 {len(lines)} 条到剪贴板")
    def export_generated_wallets(self):
        from tkinter import filedialog

        if not self.generated_wallets:
            messagebox.showwarning("提示", "请先创建钱包", parent=getattr(self, "wallet_generator_window", None) or self.root)
            return
        path = filedialog.asksaveasfilename(
            title="导出钱包 TXT",
            defaultextension=".txt",
            filetypes=[("Text", "*.txt")],
            parent=getattr(self, "wallet_generator_window", None) or self.root,
        )
        if not path:
            return
        try:
            lines = self._wallet_export_lines(self.wallet_export_format_var.get().strip())
            Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            self.log(f"钱包生成器 TXT 导出完成：{path}")
            messagebox.showinfo("导出完成", f"已导出 {len(lines)} 条", parent=getattr(self, "wallet_generator_window", None) or self.root)
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc), parent=getattr(self, "wallet_generator_window", None) or self.root)
    def import_generated_wallets(self):
        if not self.generated_wallets:
            messagebox.showwarning("提示", "请先创建钱包", parent=getattr(self, "wallet_generator_window", None) or self.root)
            return

        mode = self._mode()
        if mode == self.MODE_1M:
            rows = [item.address for item in self.generated_wallets]
            created = self.store.upsert_one_to_many_addresses(rows)
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self._persist_import_rows()
            self.log(f"钱包生成器导入完成：新增 {created} 条接收地址，已自动全选 {len(self.store.one_to_many_addresses)} 条")
            return

        if mode == self.MODE_M1:
            rows = [item.private_key for item in self.generated_wallets]
            created = self.store.upsert_many_to_one_sources(rows)
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self._persist_import_rows()
            self.log(f"钱包生成器导入完成：新增 {created} 条转出钱包，已自动全选 {len(self.store.many_to_one_sources)} 条")
            return

        values = [item.private_key for item in self.generated_wallets]
        merge_column_values(self.m2m_import_drafts, ("source", "target"), "source", values)
        completed, created = self._promote_complete_m2m_drafts()
        if completed:
            self.checked_row_keys = set(self._active_row_keys())
        self._refresh_tree()
        self._persist_import_rows()
        parts = [f"钱包生成器导入完成：写入 {len(values)} 条转出凭证列"]
        if completed:
            parts.append(f"补齐 {completed} 条")
        if created:
            parts.append(f"新增 {created} 条")
        if self.m2m_import_drafts:
            parts.append(f"待补齐 {len(self.m2m_import_drafts)} 行")
        self.log("，".join(parts))
