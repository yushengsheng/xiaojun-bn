#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import (
    BinanceClient,
    CombinedStopEvent,
    Decimal,
    FUTURES_AMOUNT_DEFAULT,
    FUTURES_LEVERAGE_DEFAULT,
    FUTURES_ROUNDS_DEFAULT,
    FUTURES_SIDE_DEFAULT,
    PREMIUM_APPEND_THRESHOLD_DEFAULT,
    PREMIUM_ORDER_COUNT_DEFAULT,
    REPRICE_THRESHOLD_DEFAULT,
    SPOT_ROUNDS_DEFAULT,
    Strategy,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_MODE_CONVERT,
    TRADE_MODE_DEFAULT,
    TRADE_MODE_LIMIT,
    TRADE_MODE_MARKET,
    TRADE_MODE_PREMIUM,
    WITHDRAW_COIN_DEFAULT,
    WITHDRAW_NETWORK_DEFAULT,
    clear_exchange_account_log_index,
    logger,
    messagebox,
    queue,
    random,
    schedule_ui_callback,
    set_exchange_account_log_index,
    summarize_exchange_exception,
    threading,
    time,
    tk,
    ttk,
)


class ExchangeBatchSummaryMixin(object):
    def _schedule_batch_summary_text(self, text: str) -> None:
        if self._closing:
            return
        schedule_ui_callback(
            self,
            "exchange-batch-summary",
            lambda value=str(text): self._set_batch_summary_text(value),
            root=self,
        )
    def _get_account_stop_event(self, acc: dict):
        stop_event = acc.get("stop_event")
        if stop_event is None:
            stop_event = threading.Event()
            acc["stop_event"] = stop_event
        return stop_event
    def _clear_account_stop_request(self, acc: dict):
        self._get_account_stop_event(acc).clear()
    def _set_account_batch_active(self, acc: dict, active: bool):
        acc["batch_active"] = bool(active)
    def _prepare_accounts_for_batch(self, selected_accounts):
        selected_ids = {id(acc) for acc in selected_accounts}
        for acc in self.accounts:
            self._clear_account_stop_request(acc)
            self._set_account_batch_active(acc, id(acc) in selected_ids)
    def _clear_account_batch_runtime(self):
        for acc in self.accounts:
            self._clear_account_stop_request(acc)
            self._set_account_batch_active(acc, False)
    @staticmethod
    def _account_display_index(acc: dict, fallback: int) -> int:
        try:
            text = str(acc.get("index_var").get() or "").strip()
            if text:
                return int(text)
        except Exception:
            pass
        return int(fallback)
    def _set_context_account(self, acc: dict | None):
        previous = getattr(self, "_context_account", None)
        self._context_account = acc
        if previous is not None and previous is not acc:
            self._apply_account_row_style(previous)
        if acc is not None:
            self._apply_account_row_style(acc)
    def _on_account_tree_selection_changed(self, _event=None):
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return None
        current_context = getattr(self, "_context_account", None)
        try:
            selected = tree.selection()
        except Exception:
            selected = ()
        if not selected:
            self._set_context_account(None)
        elif current_context is not None:
            selected_acc = self._account_from_tree_row_id(selected[0])
            if selected_acc is not current_context:
                self._set_context_account(None)
        elif getattr(self, "_context_account", None) is not None:
            self._set_context_account(None)
        return None
    def _on_account_tree_double_click(self, event):
        tree = getattr(self, "account_tree", None)
        if tree is None or tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = tree.identify_row(event.y)
        acc = self._account_from_tree_row_id(row_id)
        if acc is None:
            return None
        try:
            tree.focus(row_id)
        except Exception:
            pass
        selected_var = acc.get("selected_var")
        if selected_var is None:
            return "break"
        selected_var.set(not bool(selected_var.get()))
        return "break"
    def _on_account_tree_right_click(self, event):
        tree = getattr(self, "account_tree", None)
        if tree is None or tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = tree.identify_row(event.y)
        acc = self._account_from_tree_row_id(row_id)
        if acc is None:
            return None
        try:
            tree.selection_set(row_id)
            tree.focus(row_id)
        except Exception:
            pass
        return self._show_account_row_menu(event, acc)
    def _show_account_row_menu(self, event, acc: dict):
        self._set_context_account(acc)
        self._focus_account_list_for_paste()
        try:
            self.account_row_menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                self.account_row_menu.grab_release()
            except Exception:
                pass
        return "break"
    def _get_context_account_or_warn(self):
        acc = getattr(self, "_context_account", None)
        if acc is None or acc not in self.accounts:
            messagebox.showinfo("提示", "请先右键选择一个账号")
            return None
        return acc
    def run_context_account_execute(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_selected_accounts(accounts_to_run=[acc], require_confirm=True)
    def copy_context_account_api_key(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self._copy_account_field(acc, "api_key", "API KEY")
    def copy_context_account_withdraw_address(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self._copy_account_field(acc, "address", "提现地址")
    def run_context_account_delete(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        if self._batch_task_active or (self.worker_thread and self.worker_thread.is_alive()):
            messagebox.showinfo("提示", "当前有任务在运行中，请先停止后再删除账号")
            return
        try:
            idx_text = str(acc.get("index_var").get() or "").strip()
        except Exception:
            idx_text = ""
        api_key_preview = self._mask_key(str(acc.get("api_key") or "").strip())
        removed_count = self._delete_accounts(
            [acc],
            require_confirm=True,
            confirm_text=f"确认删除账号 #{idx_text or '?'}（{api_key_preview}）吗？",
        )
        if removed_count > 0:
            logger.info("已通过右键菜单删除账号 #%s", idx_text or "?")
    def run_context_account_query(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_selected_accounts(
            accounts_to_run=[acc],
            batch_total_asset_only=True,
            require_confirm=False,
        )
    def run_context_account_stop(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        if not self._batch_task_active or not (self.worker_thread and self.worker_thread.is_alive()):
            messagebox.showinfo("提示", "当前没有运行中的批量任务")
            return
        if not bool(acc.get("batch_active")):
            messagebox.showinfo("提示", "该账号当前不在运行队列中")
            return

        account_stop_event = self._get_account_stop_event(acc)
        if account_stop_event.is_set():
            messagebox.showinfo("提示", "该账号已请求停止")
            return

        account_stop_event.set()
        self._set_account_status(acc, "已请求停止")
        try:
            idx_text = acc["index_var"].get()
        except Exception:
            idx_text = "?"
        logger.info("已请求停止账号 #%s", idx_text)
    def run_context_account_withdraw(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.batch_manual_withdraw(accounts_to_run=[acc], require_confirm=True)
    def run_context_account_redeem_earn(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.batch_redeem_flexible_earn(accounts_to_run=[acc], require_confirm=True)
    def run_context_account_collect_bnb(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_batch_collect_bnb_with_confirm(accounts_to_run=[acc])
    @staticmethod
    def _format_amount(value: float, precision: int = 8) -> str:
        text = f"{float(value):.{precision}f}" if value is not None else "0"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text else "0"
    @classmethod
    def _format_spot_balances_text(cls, balances: list[dict]) -> str:
        if not balances:
            return "--"
        parts = []
        for item in balances:
            asset = str(item.get("asset") or "").strip().upper()
            total = float(item.get("total", 0) or 0)
            if not asset or total <= 0:
                continue
            parts.append(f"{asset} {cls._format_amount(total)}")
        return " | ".join(parts) if parts else "--"
    @classmethod
    def _query_account_asset_balances_text(cls, client, *, fast: bool = True) -> str:
        asset_breakdown = client.query_asset_balances_breakdown(fast=fast)
        return cls._format_asset_breakdown_text(asset_breakdown)
    @classmethod
    def _format_asset_breakdown_text(cls, balances: dict[str, Decimal]) -> str:
        if not balances:
            return "--"
        items = []
        for asset, amount in balances.items():
            amount_dec = Decimal(str(amount or "0"))
            if amount_dec <= 0:
                continue
            items.append((asset, amount_dec))
        if not items:
            return "--"
        items.sort(key=lambda item: (-item[1], item[0]))
        return " | ".join(f"{asset} {cls._format_amount(float(amount_dec))}" for asset, amount_dec in items)
    @staticmethod
    def _exchange_success_status_text(text: str, *, fallback: str = "成功") -> str:
        body = str(text or "").strip() or str(fallback or "成功").strip() or "成功"
        return body if body.startswith("✔") else f"✔{body}"
    @staticmethod
    def _exchange_failed_status_text(text: str, *, fallback: str = "失败") -> str:
        body = str(text or "").strip() or str(fallback or "失败").strip() or "失败"
        return body if body.startswith("×") else f"×{body}"
    @staticmethod
    def _plain_exchange_status_text(text: str, *, fallback: str = "") -> str:
        body = str(text or "").strip()
        while body[:1] in {"✔", "×"}:
            body = body[1:].strip()
        return body or str(fallback or "").strip()
    @classmethod
    def _format_withdraw_amount_status(cls, amount: float, coin: str, *, enable_withdraw: bool) -> tuple[str, bool]:
        coin_u = str(coin or "").strip().upper()
        if amount > 0:
            return cls._exchange_success_status_text(f"提现{cls._format_amount(amount)} {coin_u}"), True
        if not enable_withdraw:
            return cls._exchange_success_status_text("自动提现已关闭"), True
        return cls._exchange_failed_status_text("提现失败：可提余额不足/预留过高"), False
    @classmethod
    def _format_batch_strategy_completion_status(
        cls,
        amount: float,
        coin: str,
        *,
        enable_withdraw: bool,
    ) -> tuple[str | None, bool]:
        if not enable_withdraw:
            return None, True
        return cls._format_withdraw_amount_status(amount, coin, enable_withdraw=enable_withdraw)
    @classmethod
    def _format_trade_failure_status(cls, stage: str, err: object) -> str:
        stage_text = str(stage or "").strip()
        if stage_text == "开仓下单":
            prefix = "开仓失败"
        elif stage_text == "平仓下单":
            prefix = "平仓失败"
        elif "预检" in stage_text:
            prefix = "合约预检失败"
        else:
            prefix = "交易失败"
        return cls._exchange_error_status_text(prefix, err, fallback=prefix)
    @staticmethod
    def _collect_bnb_withdraw_runtime(*, force_bnb_withdraw: bool, enable_withdraw: bool, withdraw_coin: str) -> tuple[str, bool]:
        if force_bnb_withdraw:
            return "BNB", True
        return "BNB", bool(enable_withdraw)
    @staticmethod
    def _collect_bnb_remaining_spot_assets(client) -> list[str]:
        remaining_assets: list[str] = []
        for item in client.spot_positive_assets():
            asset = str((item or {}).get("asset") or "").strip().upper()
            if not asset or asset == "BNB":
                continue
            if asset not in remaining_assets:
                remaining_assets.append(asset)
        return remaining_assets
    @staticmethod
    def _asset_list_preview(assets: list[str], *, limit: int = 3) -> str:
        unique_assets: list[str] = []
        for asset in assets:
            asset_u = str(asset or "").strip().upper()
            if asset_u and asset_u not in unique_assets:
                unique_assets.append(asset_u)
        if not unique_assets:
            return "--"
        preview = ",".join(unique_assets[:limit])
        if len(unique_assets) > limit:
            preview += f"等{len(unique_assets)}个"
        return preview
    @staticmethod
    def _detail_list_preview(items: list[str], *, limit: int = 3) -> str:
        unique_items: list[str] = []
        for item in items:
            text = str(item or "").strip()
            if text and text not in unique_items:
                unique_items.append(text)
        if not unique_items:
            return "--"
        preview = "，".join(unique_items[:limit])
        if len(unique_items) > limit:
            preview += f"等{len(unique_items)}项"
        return preview
    @staticmethod
    def _wait_batch_random_delay(
        stop_event,
        min_delay_ms: int,
        max_delay_ms: int,
        *,
        minimum_seconds: float = 0.0,
        log_prefix: str = "",
    ) -> bool:
        if stop_event is not None and stop_event.is_set():
            return False
        low_ms = max(0, int(min(min_delay_ms, max_delay_ms)))
        high_ms = max(0, int(max(min_delay_ms, max_delay_ms)))
        delay_seconds = random.randint(low_ms, high_ms) / 1000.0 if high_ms > 0 else 0.0
        delay_seconds = max(float(minimum_seconds or 0.0), delay_seconds)
        if delay_seconds <= 0:
            return not (stop_event is not None and stop_event.is_set())
        prefix = str(log_prefix or "").strip()
        if prefix:
            logger.info("%s随机延迟 %.3f 秒", prefix, delay_seconds)
        if stop_event is None:
            time.sleep(delay_seconds)
            return True
        return not stop_event.wait(delay_seconds)
    @staticmethod
    def _compact_error_text(err_text: str, max_len: int = 28) -> str:
        s = str(err_text or "").strip().replace("\n", " ")
        if not s:
            return "未知错误"
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")
    @staticmethod
    def _exchange_error_status_text(prefix: str, err: object, *, fallback: str = "异常") -> str:
        prefix_text = str(prefix or "").strip() or str(fallback or "异常").strip() or "异常"
        reason_text = summarize_exchange_exception(err, fallback=fallback, max_len=22)
        if not reason_text:
            return ExchangeBatchSummaryMixin._exchange_failed_status_text(prefix_text, fallback=fallback)
        return ExchangeBatchSummaryMixin._exchange_failed_status_text(f"{prefix_text}: {reason_text}", fallback=fallback)
    @staticmethod
    def _account_batch_key(acc: dict) -> str:
        key = str((acc or {}).get("api_key") or "").strip()
        return key or f"account:{id(acc)}"
    def _set_retry_failed_button_state(self) -> None:
        btn = getattr(self, "btn_retry_failed_accounts", None)
        if btn is None:
            return
        can_retry = bool(self._last_batch_retry and self._last_batch_retry.get("failed_account_keys"))
        if self.worker_thread and self.worker_thread.is_alive():
            can_retry = False
        try:
            btn.config(state="normal" if can_retry else "disabled")
        except Exception:
            pass
    @classmethod
    def _format_batch_metric_totals(cls, metrics_by_account: dict[str, tuple[Decimal, str]] | None) -> str:
        if not metrics_by_account:
            return "-"
        totals: dict[str, Decimal] = {}
        for value in metrics_by_account.values():
            if not isinstance(value, tuple) or len(value) != 2:
                continue
            amount_raw, asset_raw = value
            try:
                amount = Decimal(str(amount_raw or "0"))
            except Exception:
                amount = Decimal("0")
            asset = str(asset_raw or "").strip().upper()
            totals[asset] = totals.get(asset, Decimal("0")) + amount
        if not totals:
            return "-"
        parts = []
        for asset in sorted(totals.keys()):
            amount = totals.get(asset, Decimal("0"))
            amount_text = cls._format_amount(float(amount))
            parts.append(f"{amount_text} {asset}" if asset else amount_text)
        return " / ".join(parts)
    def _build_batch_summary_text(self, summary: dict | None = None, *, pending_as_failed: bool = False) -> str:
        data = summary if isinstance(summary, dict) else None
        if not data:
            return "结果汇总：成功0 | 失败0 | 提现总额=- | 余额总额=-"
        results = dict(data.get("results") or {})
        success_count = sum(1 for value in results.values() if value is True)
        failed_count = sum(
            1
            for value in results.values()
            if value is False or (pending_as_failed and value is not True)
        )
        runner = str(data.get("runner") or "").strip()
        if runner == "batch_redeem_flexible_earn":
            redeemed_count = self._sum_batch_count_metrics(data.get("redeemed_assets_by_account") or {})
            skipped_count = self._sum_batch_count_metrics(data.get("skipped_assets_by_account") or {})
            auto_disabled_count = self._sum_batch_count_metrics(data.get("auto_disabled_by_account") or {})
            return (
                f"结果汇总：成功{success_count} | 失败{failed_count} | "
                f"已赎回资产{redeemed_count} | 跳过资产{skipped_count} | 关闭自动申购{auto_disabled_count}"
            )
        withdraw_text = self._format_batch_metric_totals(data.get("withdraw_by_account") or {})
        balance_text = self._format_batch_metric_totals(data.get("balance_by_account") or {})
        return f"结果汇总：成功{success_count} | 失败{failed_count} | 提现总额={withdraw_text} | 余额总额={balance_text}"
    def _set_batch_summary_text(self, text: str) -> None:
        var = getattr(self, "exchange_batch_summary_var", None)
        if var is not None:
            var.set(str(text or ""))
    def _refresh_batch_summary_text(self) -> None:
        with self._batch_summary_lock:
            text = self._build_batch_summary_text(self._current_batch_summary)
        self._dispatch_ui(lambda t=text: self._set_batch_summary_text(t))
    def _record_batch_withdraw_metric(self, acc: dict, amount, asset: str) -> None:
        summary_text = ""
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            if not summary:
                return
            key = self._account_batch_key(acc)
            try:
                amount_dec = Decimal(str(amount or "0"))
            except Exception:
                amount_dec = Decimal("0")
            summary.setdefault("withdraw_by_account", {})[key] = (amount_dec, str(asset or "").strip().upper())
            summary_text = self._build_batch_summary_text(summary)
        self._schedule_batch_summary_text(summary_text)
    def _record_batch_balance_metric(self, acc: dict, amount, asset: str) -> None:
        summary_text = ""
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            if not summary:
                return
            key = self._account_batch_key(acc)
            try:
                amount_dec = Decimal(str(amount or "0"))
            except Exception:
                amount_dec = Decimal("0")
            summary.setdefault("balance_by_account", {})[key] = (amount_dec, str(asset or "").strip().upper())
            summary_text = self._build_batch_summary_text(summary)
        self._schedule_batch_summary_text(summary_text)
    @staticmethod
    def _sum_batch_count_metrics(metrics_by_account: dict[str, int] | None) -> int:
        total = 0
        for value in dict(metrics_by_account or {}).values():
            try:
                total += max(0, int(value or 0))
            except Exception:
                continue
        return total
    def _record_batch_count_metric(self, acc: dict, field_name: str, value: int) -> None:
        summary_text = ""
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            if not summary:
                return
            key = self._account_batch_key(acc)
            try:
                count_value = max(0, int(value or 0))
            except Exception:
                count_value = 0
            summary.setdefault(str(field_name or "").strip(), {})[key] = count_value
            summary_text = self._build_batch_summary_text(summary)
        self._schedule_batch_summary_text(summary_text)
    def _begin_batch_summary_tracking(
        self,
        *,
        action_label: str,
        runner: str,
        retry_kwargs: dict | None,
        selected_accounts: list[dict],
    ) -> None:
        accounts_by_key = {
            self._account_batch_key(acc): acc
            for acc in list(selected_accounts or [])
        }
        with self._batch_summary_lock:
            self._current_batch_summary = {
                "action_label": action_label,
                "runner": runner,
                "retry_kwargs": dict(retry_kwargs or {}),
                "accounts_by_key": accounts_by_key,
                "results": {key: None for key in accounts_by_key},
                "withdraw_by_account": {},
                "balance_by_account": {},
            }
            summary_text = self._build_batch_summary_text(self._current_batch_summary)
        self._last_batch_retry = None
        self._set_retry_failed_button_state()
        self._schedule_batch_summary_text(summary_text)
    def _mark_batch_account_result(self, acc: dict, success: bool) -> None:
        summary_text = ""
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            if not summary:
                return
            key = self._account_batch_key(acc)
            results = summary.get("results") or {}
            if key not in results:
                return
            results[key] = bool(success)
            summary_text = self._build_batch_summary_text(summary)
        self._schedule_batch_summary_text(summary_text)
    def _finish_batch_summary_tracking(self) -> None:
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            self._current_batch_summary = None
        if not summary:
            self._set_retry_failed_button_state()
            return

        results = dict(summary.get("results") or {})
        accounts_by_key = dict(summary.get("accounts_by_key") or {})
        total_count = len(results)
        success_count = sum(1 for value in results.values() if value is True)
        failed_account_keys = [key for key, value in results.items() if value is not True]
        failed_count = len(failed_account_keys)

        if failed_account_keys:
            self._last_batch_retry = {
                "action_label": str(summary.get("action_label") or "批量任务"),
                "runner": str(summary.get("runner") or ""),
                "retry_kwargs": dict(summary.get("retry_kwargs") or {}),
                "failed_account_keys": list(failed_account_keys),
                "accounts_by_key": accounts_by_key,
            }
        else:
            self._last_batch_retry = None

        self._set_retry_failed_button_state()
        self._set_batch_summary_text(self._build_batch_summary_text(summary, pending_as_failed=True))
        self._show_batch_result_dialog(
            title=f"{summary.get('action_label', '批量任务')}完成",
            action_label=str(summary.get("action_label") or "批量任务"),
            success_count=success_count,
            failed_count=failed_count,
            total_count=total_count,
        )
    def _show_batch_result_dialog(
        self,
        *,
        title: str,
        action_label: str,
        success_count: int,
        failed_count: int,
        total_count: int,
    ) -> None:
        dialog = tk.Toplevel(self)
        dialog.title(str(title or "执行完成"))
        dialog.transient(self)
        dialog.resizable(False, False)
        dialog.grab_set()

        body = ttk.Frame(dialog, padding=16)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text=str(action_label or "批量任务"), font=("", 11, "bold")).pack(anchor="w")
        ttk.Label(body, text=f"总数：{int(total_count)}", foreground="#555555").pack(anchor="w", pady=(6, 0))

        counts = ttk.Frame(body)
        counts.pack(anchor="w", pady=(8, 0))
        ttk.Label(counts, text="成功：").pack(side="left")
        tk.Label(counts, text=str(int(success_count)), fg="#1E8449").pack(side="left")
        ttk.Label(counts, text="   失败：").pack(side="left")
        tk.Label(counts, text=str(int(failed_count)), fg="#C62828").pack(side="left")

        hint_text = "失败账号可点击“失败重试”继续执行上一次批量操作。" if failed_count > 0 else "本次批量操作没有失败账号。"
        ttk.Label(body, text=hint_text, foreground="#666666", wraplength=320, justify="left").pack(anchor="w", pady=(10, 0))

        btn_row = ttk.Frame(body)
        btn_row.pack(fill="x", pady=(14, 0))
        ttk.Button(btn_row, text="确定", command=dialog.destroy).pack(side="right")

        dialog.update_idletasks()
        try:
            parent_x = self.winfo_rootx()
            parent_y = self.winfo_rooty()
            parent_w = self.winfo_width()
            parent_h = self.winfo_height()
            width = dialog.winfo_width()
            height = dialog.winfo_height()
            x = parent_x + max(0, (parent_w - width) // 2)
            y = parent_y + max(0, (parent_h - height) // 2)
            dialog.geometry(f"+{x}+{y}")
        except Exception:
            pass
        dialog.focus_set()
    def _build_batch_execute_summary_lines(
        self,
        *,
        account_count: int,
        trade_account_type: str,
        trade_mode: str,
        spot_symbol: str,
        spot_rounds: int,
        futures_symbol: str,
        futures_rounds: int,
        min_delay: int,
        max_delay: int,
        withdraw_coin: str,
        withdraw_network: str,
        enable_withdraw: bool,
        bnb_topup_amount: Decimal | None = None,
    ) -> list[str]:
        lines = [f"批量账号数：{int(account_count)}"]
        lines.append(f"交易类型：{trade_account_type}")
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
            lines.append(f"交易对：{str(futures_symbol or '').strip().upper() or '-'}")
            lines.append(f"轮次：{int(futures_rounds)}")
        else:
            lines.append(f"交易模式：{trade_mode}")
            lines.append(f"交易对：{str(spot_symbol or '').strip().upper() or '-'}")
            lines.append(f"轮次：{int(spot_rounds)}")
            if trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}:
                lines.append(
                    "预购买BNB金额：%s" % BinanceClient._format_decimal(Decimal(str(bnb_topup_amount or "0")))
                )
        lines.append(f"随机延迟：{int(min_delay)} - {int(max_delay)} 毫秒")
        lines.append(f"提现币种：{str(withdraw_coin or '').strip().upper() or WITHDRAW_COIN_DEFAULT}")
        lines.append(f"提现网络：{str(withdraw_network or '').strip().upper() or WITHDRAW_NETWORK_DEFAULT}")
        lines.append(f"自动提现：{'开启' if enable_withdraw else '关闭'}")
        return lines
    def _show_batch_execute_confirm_dialog(
        self,
        *,
        account_count: int,
        trade_account_type: str,
        trade_mode: str,
        spot_symbol: str,
        spot_rounds: int,
        futures_symbol: str,
        futures_rounds: int,
        min_delay: int,
        max_delay: int,
        withdraw_coin: str,
        withdraw_network: str,
        enable_withdraw: bool,
        bnb_topup_amount: Decimal | None = None,
        enable_bnb_topup: bool = False,
        show_bnb_topup_option: bool = False,
    ) -> tuple[bool, bool, bool]:
        dialog = tk.Toplevel(self)
        dialog.title("确认批量执行")
        dialog.transient(self)
        dialog.resizable(False, False)
        dialog.grab_set()

        result = {
            "confirmed": False,
            "enable_withdraw": bool(enable_withdraw),
            "enable_bnb_topup": bool(enable_bnb_topup),
        }
        enable_var = tk.BooleanVar(value=bool(enable_withdraw))
        enable_bnb_topup_var = tk.BooleanVar(value=bool(enable_bnb_topup))

        body = ttk.Frame(dialog, padding=16)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="批量执行配置确认", font=("", 11, "bold")).pack(anchor="w")
        summary = ttk.Frame(body)
        summary.pack(anchor="w", pady=(10, 0))

        for line in self._build_batch_execute_summary_lines(
            account_count=account_count,
            trade_account_type=trade_account_type,
            trade_mode=trade_mode,
            spot_symbol=spot_symbol,
            spot_rounds=spot_rounds,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            min_delay=min_delay,
            max_delay=max_delay,
            withdraw_coin=withdraw_coin,
            withdraw_network=withdraw_network,
            enable_withdraw=enable_withdraw,
            bnb_topup_amount=bnb_topup_amount,
        ):
            ttk.Label(summary, text=line, foreground="#444444").pack(anchor="w", pady=(0, 2))

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=(12, 10))

        if show_bnb_topup_option:
            tk.Checkbutton(
                body,
                text="本次批量执行启用预购买BNB",
                variable=enable_bnb_topup_var,
                fg="#C62828",
                activeforeground="#C62828",
                selectcolor="#FFFFFF",
                anchor="w",
            ).pack(anchor="w")
            ttk.Label(
                body,
                text="使用当前配置的预购买BNB金额，仅对本次批量执行生效。",
                foreground="#666666",
            ).pack(anchor="w", pady=(6, 0))

        tk.Checkbutton(
            body,
            text="本次批量执行启用自动提现",
            variable=enable_var,
            fg="#C62828",
            activeforeground="#C62828",
            selectcolor="#FFFFFF",
            anchor="w",
        ).pack(anchor="w")
        ttk.Label(body, text="仅对本次批量执行生效，不修改当前界面配置。", foreground="#666666").pack(anchor="w", pady=(6, 0))

        btn_row = ttk.Frame(body)
        btn_row.pack(fill="x", pady=(14, 0))

        def on_confirm():
            result["confirmed"] = True
            result["enable_withdraw"] = bool(enable_var.get())
            result["enable_bnb_topup"] = bool(enable_bnb_topup_var.get())
            dialog.destroy()

        def on_cancel():
            dialog.destroy()

        ttk.Button(btn_row, text="取消", command=on_cancel).pack(side="right")
        ttk.Button(btn_row, text="开始执行", command=on_confirm).pack(side="right", padx=(0, 8))
        dialog.bind("<Escape>", lambda _event: on_cancel())
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)

        dialog.update_idletasks()
        try:
            parent_x = self.winfo_rootx()
            parent_y = self.winfo_rooty()
            parent_w = self.winfo_width()
            parent_h = self.winfo_height()
            width = dialog.winfo_width()
            height = dialog.winfo_height()
            x = parent_x + max(0, (parent_w - width) // 2)
            y = parent_y + max(0, (parent_h - height) // 2)
            dialog.geometry(f"+{x}+{y}")
        except Exception:
            pass
        dialog.focus_set()
        self.wait_window(dialog)
        return (
            bool(result["confirmed"]),
            bool(result["enable_withdraw"]),
            bool(result["enable_bnb_topup"]),
        )


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
