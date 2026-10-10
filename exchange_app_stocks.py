"""Small stock-only UI and execution entrypoints."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tkinter import messagebox, ttk
import tkinter as tk

from exchange_binance_client import SharedRateLimitGate
from exchange_logging import logger, set_exchange_account_log_index, clear_exchange_account_log_index
from exchange_stocks import STOCK_FIELDS, STOCK_TYPE, StockSettings, StockStrategy
from input_validation import bounded_int


def stock_payload(owner):
    variables = vars(owner).get("stock_vars", {})
    return {key: variables[key].get() if key in variables else default for key, default in STOCK_FIELDS.items()}


def init_stock_variables(owner):
    owner.stock_vars = {key: tk.StringVar(owner, value=value) for key, value in STOCK_FIELDS.items()}


def build_stock_row(owner, parent):
    variables = owner.stock_vars
    for label, key, width in (("股票代码:", "symbol", 10), ("轮次:", "rounds", 5), ("每轮金额(留空全部):", "amount", 10)):
        ttk.Label(parent, text=label).pack(side="left")
        ttk.Entry(parent, textvariable=variables[key], width=width).pack(side="left", padx=(4, 10))
    ttk.Label(parent, text="支付：USDC").pack(side="left", padx=(0, 10))
    ttk.Button(parent, text="交易设置", command=owner.open_stock_settings).pack(side="left")


class ExchangeAppStocksMixin:
    def open_stock_settings(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "请在任务结束后修改股票设置")
            return
        window = tk.Toplevel(self)
        window.title("股票交易设置")
        window.transient(self)
        pending = {}
        labels = (("tick", "价格步长（以币安股票参数为准）"), ("poll", "成交检查间隔（秒）"),
                  ("reprice", "报价复核/改价间隔（秒）"), ("timeout", "每个买卖阶段等待上限（秒）"),
                  ("deviation", "最大价格偏离（%）"), ("spread", "新买单最大盘口价差（%）"), ("interval", "轮次间隔（秒）"))
        for index, (key, label) in enumerate(labels):
            ttk.Label(window, text=label).grid(row=index, column=0, padx=12, pady=5, sticky="w")
            pending[key] = tk.StringVar(window, value=self.stock_vars[key].get())
            ttk.Entry(window, textvariable=pending[key], width=14).grid(row=index, column=1, padx=12, pady=5)
        def save():
            raw = {**stock_payload(self), **{key: var.get() for key, var in pending.items()}}
            try:
                StockSettings.parse(raw)
            except (ValueError, RuntimeError) as exc:
                messagebox.showerror("错误", str(exc), parent=window)
                return
            for key, var in pending.items():
                self.stock_vars[key].set(var.get())
            window.destroy()
        ttk.Button(window, text="确定", command=save).grid(row=len(labels), column=1, padx=12, pady=10, sticky="e")

    def start_stock_tasks(self, accounts=None, *, raw=None, require_confirm=True):
        from exchange_strategy import CombinedStopEvent
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中")
            return
        if self._pending_cleanup_snapshot():
            messagebox.showinfo("提示", "存在待清理任务，请先点击停止完成清理")
            return
        batch = accounts is not None
        selected = list(accounts) if batch else [{"api_key": self.api_key_var.get().strip(), "api_secret": self.api_secret_var.get().strip()}]
        try:
            settings = StockSettings.parse(raw or stock_payload(self))
            threads = bounded_int(self.max_threads_var.get(), "线程数") if batch else 1
            if not selected or any(not acc.get("api_key") or not acc.get("api_secret") for acc in selected):
                raise ValueError("请先填写账号API KEY和SECRET")
            if len({acc["api_key"] for acc in selected}) != len(selected):
                raise ValueError("同一API账号不能同时执行多个股票任务")
            rate_wait = self._rate_limit_wait_seconds_value()
        except (ValueError, RuntimeError) as exc:
            messagebox.showerror("错误", str(exc))
            return
        if batch and require_confirm and not messagebox.askyesno("执行股票交易", f"对{len(selected)}个账号执行 {settings.symbol} 自动限价 {settings.rounds}轮。\n买入=卖一+1步长；卖出=买一-1步长。\n开始前将现货可用USDC全部划转到资金账户，所有轮次使用资金账户。\n每轮金额：{settings.amount or '全部任务可用资金'}；不兑换USDT，不自动提现。"):
            return
        self.stop_event = threading.Event()
        gate = SharedRateLimitGate(rate_wait, stop_event=self.stop_event)
        self._batch_task_active = True
        if batch:
            self._begin_batch_summary_tracking(action_label="股票交易", runner="run_selected_accounts",
                retry_kwargs={"trade_settings_override": {"trade_account_type": STOCK_TYPE, "stock_settings": settings.payload()}}, selected_accounts=selected)
            self._prepare_accounts_for_batch(selected)
        for name in ("btn_start", "btn_run_accounts", "btn_query_all_assets", "btn_collect_bnb_combo", "btn_batch_withdraw", "btn_batch_redeem_earn", "btn_refresh", "btn_withdraw"):
            getattr(self, name).config(state="disabled")
        self.btn_stop.config(state="normal")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self._set_retry_failed_button_state()
        self.status_var.set("状态：股票交易预检中...")
        self.progress["maximum"], self.progress["value"] = len(selected) if batch else settings.rounds, 0

        def run_account(index, acc):
            set_exchange_account_log_index(self._account_display_index(acc, index) if batch else index)
            client = None
            result = {"success": False, "error": "任务已停止"}
            stop = CombinedStopEvent(self.stop_event, self._get_account_stop_event(acc)) if batch else self.stop_event
            def progress(step, total, text):
                if batch:
                    self._schedule_account_status(acc, text)
                else:
                    self._dispatch_ui(lambda: (self.status_var.set("状态：" + text), self.progress.configure(value=step)))
            try:
                if not stop.is_set():
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"], request_gate=gate, request_stop_event=stop)
                    result = StockStrategy(client, settings, cleanup_callback=self._record_strategy_cleanup).run(stop, progress)
                status = f"股票{settings.symbol} {result.get('completed', 0)}/{settings.rounds}轮，费用{result.get('fees', '0')} USDC"
                status += f"；买/卖额{result.get('buy_total', '0')}/{result.get('sell_total', '0')} USDC"
                if not result["success"]:
                    status += "；" + (result.get("cleanup_error") or result["error"])
                    status += f"；本任务残余{result.get('inventory', '0')}股"
                logger.info("股票交易结果：%s", status)
                result["status"] = status
                if batch:
                    self._schedule_account_status(acc, status)
            except Exception as exc:
                result = {"success": False, "error": str(exc), "status": "股票交易失败：" + str(exc)}
                logger.exception("股票交易失败")
                if batch:
                    self._schedule_account_status(acc, result["status"])
            finally:
                self._close_binance_client_instance(client)
                if batch:
                    self._mark_batch_account_result(acc, result["success"])
                    self._set_account_batch_active(acc, False)
                clear_exchange_account_log_index()
            return result

        def worker():
            results = []
            try:
                with ThreadPoolExecutor(max_workers=min(threads, len(selected)), thread_name_prefix="stocks") as pool:
                    futures = [pool.submit(run_account, index, acc) for index, acc in enumerate(selected, 1)]
                    for index, future in enumerate(as_completed(futures), 1):
                        results.append(future.result())
                        if batch:
                            self._dispatch_ui(lambda value=index: self.progress.configure(value=value))
            finally:
                status = "状态：股票任务结束"
                if not batch and results:
                    status = "状态：" + results[0]["status"]
                self._dispatch_ui(lambda: self._restore_batch_ui_to_idle(status_text=status, finish_summary=batch))
        self.worker_thread = self._start_managed_thread(worker, name="stock-trading")
