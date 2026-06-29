#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tkinter.font as tkfont
from tkinter import LEFT, W


class OnchainLayoutMixin(object):
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
        self._refresh_source_credential_security_controls()
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
            self.lbl_confirm_poll_interval,
            self.ent_confirm_poll_interval,
            self.lbl_source_credential,
            self.ent_source_credential,
            self.btn_toggle_source_credential,
            self.chk_one_to_many_source_ephemeral,
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

        for c in range(16):
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
                self.lbl_confirm_poll_interval.grid(row=1, column=14, sticky=W, padx=(10, 0), pady=(8, 0))
                self.ent_confirm_poll_interval.grid(row=1, column=15, sticky=W, padx=(4, 0), pady=(8, 0))
            self.setting_frame.columnconfigure(1, weight=1)
            self.setting_frame.columnconfigure(3, weight=1)
            self.setting_frame.columnconfigure(5, weight=1)
            self.setting_frame.columnconfigure(6, weight=1)
            self.setting_frame.columnconfigure(11, weight=1)
            if not show_relay:
                self.setting_frame.columnconfigure(13, weight=1)
                self.setting_frame.columnconfigure(15, weight=1)

            row = 2
            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=4, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_toggle_source_credential.grid(row=row, column=5, sticky=W, padx=(0, 6), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=6, sticky=W, padx=(0, 8), pady=(8, 0))
                self.chk_one_to_many_source_ephemeral.grid(row=row, column=7, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=8, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=9, columnspan=3, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1

            if show_relay:
                self.chk_relay_enabled.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.lbl_relay_fee_reserve.grid(row=row, column=1, sticky=W, pady=(8, 0))
                self.ent_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(4, 10), pady=(8, 0))
                self.lbl_confirm_timeout.grid(row=row, column=3, sticky=W, pady=(8, 0))
                self.ent_confirm_timeout.grid(row=row, column=4, sticky=W, padx=(4, 10), pady=(8, 0))
                self.lbl_confirm_poll_interval.grid(row=row, column=5, sticky=W, pady=(8, 0))
                self.ent_confirm_poll_interval.grid(row=row, column=6, sticky=W, padx=(4, 10), pady=(8, 0))
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
                self.lbl_confirm_poll_interval.grid(row=row, column=9, sticky=W, pady=(8, 0))
                self.ent_confirm_poll_interval.grid(row=row, column=10, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1

            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=3, sticky="ew", padx=(4, 8), pady=(8, 0))
                self.btn_toggle_source_credential.grid(row=row, column=4, sticky=W, padx=(0, 6), pady=(8, 0))
                self.btn_query_source_balance.grid(row=row, column=5, sticky=W, padx=(0, 8), pady=(8, 0))
                self.chk_one_to_many_source_ephemeral.grid(row=row, column=6, sticky=W, padx=(0, 8), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=7, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=8, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))
                row += 1

            if show_relay:
                self.chk_relay_enabled.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.lbl_relay_fee_reserve.grid(row=row, column=1, sticky=W, pady=(8, 0))
                self.ent_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(4, 8), pady=(8, 0))
                self.lbl_confirm_timeout.grid(row=row, column=3, sticky=W, pady=(8, 0))
                self.ent_confirm_timeout.grid(row=row, column=4, sticky=W, padx=(4, 8), pady=(8, 0))
                self.lbl_confirm_poll_interval.grid(row=row, column=5, sticky=W, pady=(8, 0))
                self.ent_confirm_poll_interval.grid(row=row, column=6, sticky=W, padx=(4, 8), pady=(8, 0))
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
            self.lbl_confirm_poll_interval.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_confirm_poll_interval.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1

        self.chk_dry_run.grid(row=row, column=0, columnspan=2, sticky=W, pady=(8, 0))
        row += 1
        self.chk_use_config_proxy.grid(row=row, column=0, sticky=W, pady=(8, 0))
        self.btn_save_all.grid(row=row, column=1, sticky=W, padx=(8, 0), pady=(8, 0))
        row += 1

        if show_source:
            self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_source_credential.grid(row=row, column=1, sticky="ew", padx=(4, 6), pady=(8, 0))
            self.btn_toggle_source_credential.grid(row=row, column=2, sticky=W, padx=(0, 6), pady=(8, 0))
            self.btn_query_source_balance.grid(row=row, column=3, sticky=W, padx=(0, 6), pady=(8, 0))
            row += 1
            self.chk_one_to_many_source_ephemeral.grid(row=row, column=1, sticky=W, padx=(4, 6), pady=(4, 0))
            self.lbl_source_balance_title.grid(row=row, column=2, sticky=W, pady=(4, 0))
            self.lbl_source_balance_val.grid(row=row, column=3, columnspan=2, sticky=W, padx=(4, 0), pady=(4, 0))
            row += 1

        if show_relay:
            self.chk_relay_enabled.grid(row=row, column=0, columnspan=2, sticky=W, pady=(8, 0))
            self.lbl_relay_fee_reserve.grid(row=row, column=2, sticky=W, padx=(10, 0), pady=(8, 0))
            self.ent_relay_fee_reserve.grid(row=row, column=3, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1
            self.lbl_confirm_timeout.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_confirm_timeout.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
            row += 1
            self.lbl_confirm_poll_interval.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_confirm_poll_interval.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=(8, 0))
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
    def _schedule_tree_column_resize(self) -> None:
        try:
            from shared_utils import schedule_ui_callback

            schedule_ui_callback(
                self,
                "onchain_tree_columns",
                self._resize_tree_columns,
                root=getattr(self, "root", None),
            )
        except Exception:
            try:
                self._resize_tree_columns()
            except Exception:
                pass
    @staticmethod
    def _fallback_text_pixel_width(text: str) -> int:
        width = 0
        for ch in str(text or ""):
            width += 13 if ord(ch) > 127 else 7
        return width
    def _tree_text_pixel_width(self, text: str, *, heading: bool = False) -> int:
        value = str(text or "")
        if not value:
            return 0
        try:
            font_name = "TkHeadingFont" if heading else "TkDefaultFont"
            return int(tkfont.nametofont(font_name).measure(value))
        except Exception:
            return self._fallback_text_pixel_width(value)
    def _tree_column_content_widths(self, cols: tuple[str, ...]) -> dict[str, int]:
        headings = getattr(self, "_tree_heading_base_texts", {}) or {}
        widths: dict[str, int] = {}
        for column in cols:
            heading_text = str(headings.get(column, column) or "")
            widths[column] = self._tree_text_pixel_width(heading_text, heading=True) + 22

        tree = getattr(self, "tree", None)
        if tree is None:
            return widths
        try:
            row_ids = list(tree.get_children(""))
        except Exception:
            row_ids = []
        for row_id in row_ids:
            try:
                values = list(tree.item(row_id, "values") or [])
            except Exception:
                continue
            for index, column in enumerate(cols):
                text = values[index] if index < len(values) else ""
                padding = 26 if column in {"source", "target", "status", "recovery", "balance"} else 18
                widths[column] = max(widths[column], self._tree_text_pixel_width(str(text), heading=False) + padding)
        return widths
    @staticmethod
    def _shrink_tree_widths_to_fit(
        widths: dict[str, int],
        min_widths: dict[str, int],
        cols: tuple[str, ...],
        available_width: int,
    ) -> dict[str, int]:
        fitted = dict(widths)
        overflow = sum(fitted[c] for c in cols) - max(0, int(available_width))
        if overflow <= 0:
            return fitted
        shrinkable = sum(max(0, fitted[c] - min_widths[c]) for c in cols)
        if shrinkable <= 0:
            return {c: min_widths[c] for c in cols}
        remaining = overflow
        for column in sorted(cols, key=lambda c: fitted[c] - min_widths[c], reverse=True):
            room = max(0, fitted[column] - min_widths[column])
            if room <= 0:
                continue
            share = min(room, max(1, int(overflow * room / shrinkable)))
            fitted[column] -= share
            remaining -= share
            if remaining <= 0:
                break
        if remaining > 0:
            for column in sorted(cols, key=lambda c: fitted[c] - min_widths[c], reverse=True):
                room = max(0, fitted[column] - min_widths[column])
                if room <= 0:
                    continue
                cut = min(room, remaining)
                fitted[column] -= cut
                remaining -= cut
                if remaining <= 0:
                    break
        return {c: max(min_widths[c], fitted[c]) for c in cols}
    def _resize_tree_columns(self):
        if not hasattr(self, "tree"):
            return
        cols = tuple(getattr(self, "_tree_column_ids", ()) or ("checked", "idx", "source", "target", "status", "recovery", "balance"))
        width = self.tree.winfo_width()
        if width <= 1 and hasattr(self, "table_wrap"):
            width = self.table_wrap.winfo_width() - 20
        if width <= 0:
            return

        min_widths = dict(self.TREE_COL_MIN_WIDTHS)
        max_widths = dict(getattr(self, "TREE_COL_MAX_WIDTHS", {}))
        content_widths = self._tree_column_content_widths(cols)
        desired_widths: dict[str, int] = {}
        for column in cols:
            min_w = max(24, int(min_widths.get(column, 80)))
            max_w = max(min_w, int(max_widths.get(column, max(min_w, content_widths.get(column, min_w)))))
            desired_widths[column] = min(max_w, max(min_w, int(content_widths.get(column, min_w))))

        available_width = max(0, int(width) - 4)
        if sum(desired_widths[c] for c in cols) > available_width:
            desired_widths = self._shrink_tree_widths_to_fit(desired_widths, min_widths, cols, available_width)

        for column in cols:
            self.tree.column(column, width=max(1, int(desired_widths[column])), stretch=False)
    def _on_log_resize(self, _event=None):
        if not hasattr(self, "log_text"):
            return
        for tree, box in (
            (getattr(self, "log_text", None), getattr(self, "run_log_box", None)),
            (getattr(self, "error_log_text", None), getattr(self, "error_log_box", None)),
        ):
            if tree is None:
                continue
            widget_class_getter = getattr(tree, "winfo_class", None)
            if not callable(widget_class_getter) or str(widget_class_getter()) != "Treeview":
                continue
            width = tree.winfo_width()
            if width <= 1 and box is not None:
                width = box.winfo_width() - 20
            if width <= 0:
                continue
            msg_width = max(220, width - 190)
            tree.column("time", width=170, stretch=False)
            tree.column("msg", width=msg_width, stretch=True)


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
