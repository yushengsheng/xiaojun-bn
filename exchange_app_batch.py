#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import *  # noqa: F401,F403


class ExchangeAppBatchMixin(object):
    def _set_account_manage_buttons_state(self, state):
        for name in (
            "btn_add_account",
            "btn_toggle_select_accounts",
            "btn_del_accounts",
            "btn_export_accounts",
            "btn_import_accounts",
            "btn_paste_accounts",
        ):
            btn = getattr(self, name, None)
            if btn is not None:
                try:
                    btn.config(state=state)
                except Exception:
                    pass
    def _set_combo_states_for_run(self, is_running):
        state = "disabled" if is_running else "readonly"
        for name in (
            "trade_account_type_combo",
            "trade_mode_combo",
            "futures_margin_type_combo",
            "futures_side_combo",
            "withdraw_net_combo",
            "acc_network_combo",
            "withdraw_coin_combo",
        ):
            combo = getattr(self, name, None)
            if combo is not None:
                try:
                    combo.config(state=state)
                except Exception:
                    pass
    def wait_for_usdt(
        self,
        timeout_sec,
        stop_event,
        client=None,
        symbol: str = "",
        trade_account_type: str = TRADE_ACCOUNT_TYPE_SPOT,
        trade_mode: str = TRADE_MODE_DEFAULT,
        required_quote_amount: Decimal | None = None,
    ):
        start = time.time()
        c = client or self.client
        if c is None:
            logger.error("wait_for_usdt 调用时没有可用的 BinanceClient")
            return False
        trade_type = self._normalize_trade_account_type(trade_account_type)
        mode_name = self._normalize_trade_mode(trade_mode)
        try:
            if trade_type == TRADE_ACCOUNT_TYPE_FUTURES:
                quote_asset = c.get_um_futures_margin_asset(symbol)
                base_asset = ""
            else:
                quote_asset = BinanceClient.get_spot_quote_asset(symbol)
                base_asset = BinanceClient.get_spot_base_asset(symbol)
        except Exception as exc:
            logger.error("到账检测前解析交易对失败: %s", exc)
            return False
        required_amount = Decimal(str(required_quote_amount)) if required_quote_amount is not None else Decimal("0")
        progress_log_times: dict[str, float] = {}
        error_log_times: dict[str, float] = {}
        progress_log_interval = 5.0
        error_log_interval = 5.0

        def log_progress_throttled(key: str, message: str, *args) -> None:
            now = time.monotonic()
            last_time = progress_log_times.get(key)
            if last_time is not None and (now - last_time) < progress_log_interval:
                return
            progress_log_times[key] = now
            logger.info(message, *args)

        def log_error_throttled(key: str, message: str, *args) -> None:
            now = time.monotonic()
            last_time = error_log_times.get(key)
            if last_time is not None and (now - last_time) < error_log_interval:
                return
            error_log_times[key] = now
            logger.error(message, *args)

        while time.time() - start < timeout_sec:
            if stop_event and stop_event.is_set():
                logger.info("检测 %s 时收到停止信号，结束检测", quote_asset)
                return False
            try:
                base_balance_dec = Decimal("0")
                if trade_type == TRADE_ACCOUNT_TYPE_FUTURES:
                    quote_balance_dec = Decimal(str(c.um_futures_asset_balance(quote_asset).get("availableBalance", Decimal("0"))))
                    if required_amount > 0 and quote_balance_dec < required_amount:
                        moved_amount = c.transfer_spot_asset_to_um_futures(quote_asset, required_amount - quote_balance_dec)
                        if moved_amount > 0:
                            quote_balance_dec = Decimal(
                                str(c.um_futures_asset_balance(quote_asset).get("availableBalance", Decimal("0")))
                            )
                    quote_balance = float(quote_balance_dec)
                    if required_amount > 0:
                        log_progress_throttled(
                            "futures-balance-required",
                            "%s 到账检测中，当前 U本位可用 %s = %.8f，目标至少 %.8f",
                            quote_asset,
                            quote_asset,
                            quote_balance,
                            float(required_amount),
                        )
                    else:
                        log_progress_throttled(
                            "futures-balance",
                            "%s 到账检测中，当前 U本位可用 %s = %.8f",
                            quote_asset,
                            quote_asset,
                            quote_balance,
                        )
                else:
                    c.collect_funding_asset_to_spot(quote_asset)
                    quote_balance_dec = c.spot_asset_balance_decimal(quote_asset)
                    if mode_name == TRADE_MODE_CONVERT and base_asset:
                        c.collect_funding_asset_to_spot(base_asset)
                        base_balance_dec = c.spot_asset_balance_decimal(base_asset)
                    quote_balance = float(quote_balance_dec)
                    if mode_name == TRADE_MODE_LIMIT:
                        base_balance_dec = c.spot_asset_balance_decimal(base_asset)
                        log_progress_throttled(
                            "spot-balance-limit",
                            "%s模式余额检测中，当前现货 %s = %.8f，%s = %.8f",
                            mode_name,
                            quote_asset,
                            quote_balance,
                            base_asset,
                            float(base_balance_dec),
                        )
                    elif mode_name == TRADE_MODE_CONVERT:
                        log_progress_throttled(
                            "spot-balance-convert",
                            "%s模式余额检测中，当前现货 %s = %.8f，%s = %.8f",
                            mode_name,
                            quote_asset,
                            quote_balance,
                            base_asset,
                            float(base_balance_dec),
                        )
                    else:
                        log_progress_throttled(
                            "spot-balance",
                            "%s 到账检测中，当前现货 %s = %.8f",
                            quote_asset,
                            quote_asset,
                            quote_balance,
                        )
            except Exception as e:
                log_error_throttled(
                    f"balance-error:{type(e).__name__}:{e}",
                    "检测 %s 余额失败: %s",
                    quote_asset,
                    e,
                )
                quote_balance = 0.0
                base_balance_dec = Decimal("0")

            balance_ready = quote_balance > 0
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            if trade_type == TRADE_ACCOUNT_TYPE_FUTURES and required_amount > 0:
                balance_ready = Decimal(str(quote_balance)) >= required_amount

            if balance_ready:
                if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                    logger.info("检测到可挂单余额，开始执行后续策略")
                elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                    logger.info("检测到可闪兑余额，开始执行后续策略")
                else:
                    logger.info("检测到 %s 已到账，开始执行后续策略", quote_asset)
                return True

            delay_seconds = min(self._current_random_delay_seconds(), max(0.0, timeout_sec - (time.time() - start)))
            if delay_seconds <= 0:
                continue
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                log_progress_throttled("retry-limit", "未检测到可挂单余额，%.3f 秒后重试", delay_seconds)
            elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                log_progress_throttled("retry-convert", "未检测到可闪兑余额，%.3f 秒后重试", delay_seconds)
            else:
                log_progress_throttled("retry-balance", "%s 未到账，%.3f 秒后重试", quote_asset, delay_seconds)
            if stop_event:
                if stop_event.wait(delay_seconds):
                    logger.info("检测 %s 等待期间收到停止信号，结束检测", quote_asset)
                    return False
            else:
                time.sleep(delay_seconds)

        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
            logger.error("在 %d 秒内未检测到可挂单余额，终止任务", timeout_sec)
            return False
        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
            logger.error("在 %d 秒内未检测到可闪兑余额，终止任务", timeout_sec)
            return False
        logger.error("在 %d 秒内未检测到 %s 到账，终止任务", timeout_sec, quote_asset)
        return False
    def start_bot(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "任务已经在运行中")
            return

        key = self.api_key_var.get().strip()
        secret = self.api_secret_var.get().strip()
        if not key or not secret:
            messagebox.showerror("错误", "请先填写 API KEY 和 SECRET")
            return

        try:
            trade_settings = self._collect_trade_mode_settings()
            trade_account_type = str(trade_settings["trade_account_type"])
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_delta_value = trade_settings["premium_delta_value"]
            premium_order_count = int(trade_settings["premium_order_count"])
            premium_append_threshold_value = trade_settings["premium_append_threshold_value"]
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_value = trade_settings["reprice_threshold_value"]
            futures_symbol = str(trade_settings["futures_symbol"])
            futures_rounds = int(trade_settings["futures_rounds"])
            futures_amount_value = trade_settings["futures_amount_value"]
            futures_leverage = int(trade_settings["futures_leverage"])
            futures_margin_type = str(trade_settings["futures_margin_type"])
            futures_side = str(trade_settings["futures_side"])
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = int(self.usdt_timeout_var.get())
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_address = self.withdraw_addr_var.get().strip()
        withdraw_network = self.withdraw_net_var.get().strip()
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        if enable_withdraw and (not withdraw_address or not withdraw_network or not withdraw_coin):
            messagebox.showerror("错误", "开启自动提现时，请填写 提现地址 / 网络 / 币种")
            return

        client = None
        try:
            client = self._create_binance_client(key, secret)
            quote_asset = self._ensure_trade_symbol_supported(
                client,
                trade_account_type,
                trade_mode,
                spot_symbol,
                futures_symbol,
            )
            effective_reprice_threshold = None
            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}:
                premium_append_mode_enabled = (
                    trade_mode == TRADE_MODE_PREMIUM
                    and premium_order_count > 1
                    and Decimal(str(premium_append_threshold_value or "0")) > 0
                )
                if premium_append_mode_enabled:
                    effective_reprice_threshold = Decimal("0")
                elif reprice_threshold_value > 0:
                    effective_reprice_threshold = client.normalize_price_delta(
                        spot_symbol,
                        reprice_threshold_value,
                        min_one_tick=True,
                    )
                else:
                    effective_reprice_threshold = Decimal("0")
        except Exception as e:
            self._close_binance_client_instance(client)
            messagebox.showerror("错误", "Binance 连接初始化失败: %s" % e)
            return
        required_quote_amount = None
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
            required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))
        self._replace_current_binance_client(client)

        logger.info("交易所单账号链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：单账号运行中...")
        self.progress["value"] = 0
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
            total_steps = max(futures_rounds, 1)
        else:
            total_steps = max(spot_rounds, 1) if trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_CONVERT} else 1
        self.progress["maximum"] = total_steps

        def sleep_fn():
            if self.stop_event and self.stop_event.is_set():
                return
            self.random_sleep(min_delay, max_delay)

        def withdraw_callback(amount, idx=1, api_key=key, address=withdraw_address):
            if amount > 0:
                self.record_withdraw(idx, api_key, address, amount)

        strategy = Strategy(
            client=client,
            spot_rounds=spot_rounds,
            withdraw_coin=withdraw_coin,
            withdraw_address=withdraw_address,
            withdraw_network=withdraw_network,
            withdraw_fee_buffer=withdraw_buffer,
            spot_symbol=spot_symbol,
            sleep_fn=sleep_fn,
            enable_withdraw=enable_withdraw,
            withdraw_callback=withdraw_callback,
            trade_account_type=trade_account_type,
            trade_mode=trade_mode,
            premium_delta=premium_delta_value,
            premium_order_count=premium_order_count,
            premium_append_threshold=premium_append_threshold_value,
            bnb_fee_stop_value=bnb_fee_stop_value,
            bnb_topup_amount=bnb_topup_amount_value,
            reprice_threshold_amount=reprice_threshold_value,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            futures_amount=futures_amount_value,
            futures_leverage=futures_leverage,
            futures_margin_type=futures_margin_type,
            futures_side=futures_side,
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self._dispatch_ui(_update)

        def worker():
            try:
                if not self.wait_for_usdt(
                    usdt_timeout,
                    self.stop_event,
                    client=client,
                    symbol=trade_symbol,
                    trade_account_type=trade_account_type,
                    trade_mode=trade_mode,
                    required_quote_amount=required_quote_amount,
                ):
                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                        logger.info("挂单余额检测未通过，任务结束")
                    else:
                        logger.info("%s 检测未通过，任务结束", quote_asset)
                    return

                if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
                    logger.info(
                        "开始执行合约策略：%s，方向=%s，轮次=%d，下单金额=%s，杠杆=%s，保证金模式=%s",
                        futures_symbol,
                        futures_side,
                        futures_rounds,
                        futures_amount_value,
                        futures_leverage,
                        futures_margin_type,
                    )
                elif trade_mode == TRADE_MODE_MARKET:
                    logger.info("开始执行策略：市价 %d 轮，预买BNB金额=%s", spot_rounds, bnb_topup_amount_value)
                elif trade_mode == TRADE_MODE_CONVERT:
                    logger.info("开始执行策略：闪兑 %d 轮，交易对=%s", spot_rounds, spot_symbol)
                elif trade_mode == TRADE_MODE_LIMIT:
                    logger.info(
                        "开始执行策略：挂单模式，预买BNB金额=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                        bnb_topup_amount_value,
                        bnb_fee_stop_value,
                        BinanceClient._format_decimal(effective_reprice_threshold or Decimal("0")),
                        quote_asset,
                    )
                else:
                    premium_append_mode_enabled = (
                        premium_order_count > 1 and Decimal(str(premium_append_threshold_value or "0")) > 0
                    )
                    premium_append_log_text = Decimal("0")
                    if premium_append_mode_enabled:
                        premium_append_log_text = client.normalize_price_delta(
                            spot_symbol,
                            premium_append_threshold_value,
                            min_one_tick=True,
                        )
                    logger.info(
                        "开始执行策略：溢价单模式，预买BNB金额=%s，溢价=%s %s，笔数=%s，追加挂单=%s %s，剩余bnb手续费=%s，重挂阈值=%s %s",
                        bnb_topup_amount_value,
                        premium_delta_value,
                        quote_asset,
                        premium_order_count,
                        BinanceClient._format_decimal(premium_append_log_text),
                        quote_asset,
                        bnb_fee_stop_value,
                        BinanceClient._format_decimal(effective_reprice_threshold or Decimal("0")),
                        quote_asset,
                    )
                strategy.run(self.stop_event, progress_cb=progress_cb)
            except Exception as e:
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self._dispatch_ui(self._on_worker_finished)

        self.worker_thread = self._start_managed_thread(worker, name="exchange-single-run")
    def _on_worker_finished(self):
        self._batch_task_active = False
        self._clear_account_batch_runtime()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.btn_run_accounts.config(state="normal")
        self.btn_query_all_assets.config(state="normal")
        self.btn_collect_bnb_combo.config(state="normal")
        self.btn_batch_withdraw.config(state="normal")
        self.btn_refresh.config(state="normal")
        self.btn_withdraw.config(state="normal")
        self._finish_batch_summary_tracking()
        self._set_account_manage_buttons_state("normal")
        self._set_combo_states_for_run(False)
        self.status_var.set("状态：已完成/已停止")
        self.progress["value"] = 0

        if self.api_key_var.get().strip() and self.api_secret_var.get().strip():
            self.refresh_balances(silent=True)
    def stop_bot(self):
        if self.stop_event:
            self.stop_event.set()
            logger.info("已请求停止任务")
    def refresh_balances(self, silent=False):
        key = self.api_key_var.get().strip()
        secret = self.api_secret_var.get().strip()
        if not key or not secret:
            if not silent:
                messagebox.showerror("错误", "请先填写 API KEY 和 SECRET")
            return

        def worker():
            client = None
            assigned_to_app = False
            try:
                logger.info("交易所刷新余额链路：%s", self._exchange_proxy_route_text())
                client = self._create_binance_client(key, secret)
                spot_balances = client.spot_all_balances()
                balances_text = self._format_spot_balances_text(spot_balances)
                if self._closing:
                    return

                def _update(c=client):
                    self._replace_current_binance_client(c)
                    self.single_account_balances_var.set(balances_text)
                    logger.info("余额刷新完成")
                self._dispatch_ui(_update)
                assigned_to_app = True
            except Exception as e:
                logger.error("刷新余额失败: %s", e)
            finally:
                if client is not None and not assigned_to_app:
                    self._close_binance_client_instance(client)

        self._start_managed_thread(worker, name="exchange-refresh-balances")
    def manual_withdraw(self):
        client = self.client
        if not client:
            messagebox.showerror("错误", "请先使用当前 API 创建连接（点击一次开始或刷新余额）")
            return

        address = self.withdraw_addr_var.get().strip()
        network = self.withdraw_net_var.get().strip()
        coin = self.withdraw_coin_var.get().strip().upper()
        try:
            buffer_val = float(self.withdraw_buffer_var.get())
        except ValueError:
            messagebox.showerror("错误", "提现预留格式不正确")
            return

        if not address or not network or not coin:
            messagebox.showerror("错误", "请填写 提现地址 / 网络 / 币种")
            return

        def worker(client_ref=client):
            try:
                logger.info(f"手动触发提现 {coin}")
                amount = client_ref.withdraw_all_coin(
                    coin=coin,
                    address=address,
                    network=network,
                    fee_buffer=buffer_val,
                    enable_withdraw=True,
                    auto_collect_to_spot=True,
                )
                if amount > 0:
                    self.record_withdraw(1, client_ref.key, address, amount)
            except Exception as e:
                logger.error("手动提现失败: %s", e)

        self._start_managed_thread(worker, name="exchange-manual-withdraw")
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
    def _set_context_account(self, acc: dict | None):
        previous = getattr(self, "_context_account", None)
        self._context_account = acc
        if previous is not None and previous is not acc:
            self._apply_account_row_style(previous)
        if acc is not None:
            self._apply_account_row_style(acc)
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
    @classmethod
    def _format_withdraw_amount_status(cls, amount: float, coin: str, *, enable_withdraw: bool) -> str:
        coin_u = str(coin or "").strip().upper()
        if amount > 0:
            return f"提现额度 {cls._format_amount(amount)} {coin_u}"
        if not enable_withdraw:
            return f"提现额度 0 {coin_u}（自动提现已关闭）"
        return f"提现额度 0 {coin_u}（可提余额不足/预留过高）"
    @staticmethod
    def _compact_error_text(err_text: str, max_len: int = 28) -> str:
        s = str(err_text or "").strip().replace("\n", " ")
        if not s:
            return "未知错误"
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")
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
    ) -> tuple[bool, bool]:
        dialog = tk.Toplevel(self)
        dialog.title("确认批量执行")
        dialog.transient(self)
        dialog.resizable(False, False)
        dialog.grab_set()

        result = {"confirmed": False, "enable_withdraw": bool(enable_withdraw)}
        enable_var = tk.BooleanVar(value=bool(enable_withdraw))

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
        ):
            ttk.Label(summary, text=line, foreground="#444444").pack(anchor="w", pady=(0, 2))

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=(12, 10))

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
        return bool(result["confirmed"]), bool(result["enable_withdraw"])
    def retry_last_failed_batch_operation(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return
        retry_info = self._last_batch_retry or {}
        failed_keys = list(retry_info.get("failed_account_keys") or [])
        if not failed_keys:
            messagebox.showinfo("提示", "当前没有可重试的失败账号")
            return

        current_accounts_by_key = {self._account_batch_key(acc): acc for acc in self.accounts}
        stored_accounts_by_key = dict(retry_info.get("accounts_by_key") or {})
        accounts_to_retry = []
        for key in failed_keys:
            if key in current_accounts_by_key:
                accounts_to_retry.append(current_accounts_by_key[key])
            elif key in stored_accounts_by_key:
                accounts_to_retry.append(stored_accounts_by_key[key])
        if not accounts_to_retry:
            messagebox.showinfo("提示", "失败账号已不存在，无法重试")
            self._last_batch_retry = None
            self._set_retry_failed_button_state()
            return

        action_label = str(retry_info.get("action_label") or "批量任务")
        if not messagebox.askyesno("确认重试", f"确认对 {len(accounts_to_retry)} 个失败账号重试“{action_label}”吗？"):
            return

        runner = str(retry_info.get("runner") or "")
        retry_kwargs = dict(retry_info.get("retry_kwargs") or {})
        if runner == "batch_manual_withdraw":
            self.batch_manual_withdraw(accounts_to_run=accounts_to_retry, require_confirm=False)
            return
        self.run_selected_accounts(accounts_to_run=accounts_to_retry, require_confirm=False, **retry_kwargs)
    def run_query_total_assets_for_all_accounts(self):
        all_accounts = list(self.accounts)
        if not all_accounts:
            messagebox.showinfo("提示", "当前没有可查询的账号")
            return
        self.run_selected_accounts(accounts_to_run=all_accounts, batch_total_asset_only=True, require_confirm=False)
    def run_batch_collect_bnb_with_confirm(self, accounts_to_run=None):
        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        text = (
            f"即将对 {len(selected)} 个账号执行“归集并买BNB”。\n"
            "流程包含：归集合约/资金到现货、小额币兑换BNB、卖出大额币换USDT、再买入BNB。\n\n"
            "确认继续吗？"
        )
        if not messagebox.askyesno("确认执行", text):
            return
        self.run_selected_accounts(
            accounts_to_run=selected,
            batch_collect_bnb_mode=True,
            batch_sell_large_spot_to_bnb=True,
            require_confirm=False,
        )
    def run_selected_accounts(
        self,
        accounts_to_run=None,
        *,
        batch_total_asset_only: bool = False,
        batch_collect_bnb_mode: bool = False,
        batch_sell_large_spot_to_bnb: bool = False,
        batch_enable_withdraw_override: bool | None = None,
        require_confirm: bool = True,
    ):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "当前没有可执行的账号")
            return

        trade_settings = None
        try:
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = int(self.usdt_timeout_var.get())
            max_threads = int(self.max_threads_var.get())
            if (not batch_total_asset_only) and (not batch_collect_bnb_mode):
                trade_settings = self._collect_trade_mode_settings()
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确 (请检查轮数/线程数/延迟等)")
            return

        if max_threads < 1:
            max_threads = 1

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        enable_withdraw = (
            bool(batch_enable_withdraw_override)
            if batch_enable_withdraw_override is not None
            else bool(self.enable_withdraw_var.get())
        )
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()
        if trade_settings:
            trade_account_type = str(trade_settings["trade_account_type"])
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_delta_value = trade_settings["premium_delta_value"]
            premium_order_count = int(trade_settings["premium_order_count"])
            premium_append_threshold_value = trade_settings["premium_append_threshold_value"]
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_value = trade_settings["reprice_threshold_value"]
            futures_symbol = str(trade_settings["futures_symbol"])
            futures_rounds = int(trade_settings["futures_rounds"])
            futures_amount_value = trade_settings["futures_amount_value"]
            futures_leverage = int(trade_settings["futures_leverage"])
            futures_margin_type = str(trade_settings["futures_margin_type"])
            futures_side = str(trade_settings["futures_side"])
        else:
            trade_account_type = TRADE_ACCOUNT_TYPE_SPOT
            spot_rounds = max(1, int(self.spot_rounds_var.get() or SPOT_ROUNDS_DEFAULT))
            trade_mode = TRADE_MODE_MARKET
            premium_delta_value = None
            premium_order_count = PREMIUM_ORDER_COUNT_DEFAULT
            premium_append_threshold_value = Decimal(PREMIUM_APPEND_THRESHOLD_DEFAULT)
            bnb_fee_stop_value = None
            bnb_topup_amount_value = Decimal("0")
            reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
            futures_symbol = self.futures_symbol_var.get().strip().upper()
            try:
                futures_rounds = max(1, int(self.futures_rounds_var.get() or FUTURES_ROUNDS_DEFAULT))
            except Exception:
                futures_rounds = FUTURES_ROUNDS_DEFAULT
            try:
                futures_amount_value = Decimal(str(self.futures_amount_var.get() or FUTURES_AMOUNT_DEFAULT))
            except Exception:
                futures_amount_value = Decimal(FUTURES_AMOUNT_DEFAULT)
            try:
                futures_leverage = int(self.futures_leverage_var.get() or FUTURES_LEVERAGE_DEFAULT)
            except Exception:
                futures_leverage = FUTURES_LEVERAGE_DEFAULT
            futures_margin_type = self._normalize_futures_margin_type(self.futures_margin_type_var.get())
            futures_side = str(self.futures_side_var.get() or FUTURES_SIDE_DEFAULT).strip()

        if require_confirm and not batch_total_asset_only and not batch_collect_bnb_mode:
            confirmed, batch_enable_withdraw = self._show_batch_execute_confirm_dialog(
                account_count=len(selected),
                trade_account_type=trade_account_type,
                trade_mode=trade_mode,
                spot_symbol=spot_symbol,
                spot_rounds=spot_rounds,
                futures_symbol=futures_symbol,
                futures_rounds=futures_rounds,
                min_delay=min_delay,
                max_delay=max_delay,
                withdraw_coin=withdraw_coin,
                withdraw_network=self.withdraw_net_var.get().strip(),
                enable_withdraw=enable_withdraw,
            )
            if not confirmed:
                return
            enable_withdraw = bool(batch_enable_withdraw)
            batch_enable_withdraw_override = enable_withdraw

        if trade_settings:
            validate_client = None
            try:
                sample_acc = selected[0]
                validate_client = self._create_binance_client(sample_acc["api_key"], sample_acc["api_secret"])
                self._ensure_trade_symbol_supported(
                    validate_client,
                    trade_account_type,
                    trade_mode,
                    spot_symbol,
                    futures_symbol,
                )
            except Exception as e:
                self._close_binance_client_instance(validate_client)
                messagebox.showerror("错误", "Binance 连接初始化失败: %s" % e)
                return
            self._close_binance_client_instance(validate_client)

        skip_usdt_wait_in_batch = bool(self.skip_usdt_wait_in_batch_var.get())

        if batch_total_asset_only:
            self.total_asset_results = {}

        if batch_total_asset_only:
            batch_action_label = "查询全部总资产"
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            batch_action_label = "归集并买BNB"
        elif batch_collect_bnb_mode:
            batch_action_label = "归集BNB"
        else:
            batch_action_label = "批量执行"

        logger.info("交易所批量链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self._batch_task_active = True
        self._begin_batch_summary_tracking(
            action_label=batch_action_label,
            runner="run_selected_accounts",
            retry_kwargs={
                "batch_total_asset_only": bool(batch_total_asset_only),
                "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                "batch_sell_large_spot_to_bnb": bool(batch_sell_large_spot_to_bnb),
                "batch_enable_withdraw_override": enable_withdraw if (not batch_total_asset_only and not batch_collect_bnb_mode) else None,
            },
            selected_accounts=selected,
        )
        self._prepare_accounts_for_batch(selected)
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self._set_retry_failed_button_state()
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)

        if batch_total_asset_only:
            self.status_var.set(f"状态：查询全部总资产中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            self.status_var.set(f"状态：归集并买BNB中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode:
            self.status_var.set(f"状态：批量归集BNB模式运行中 (并发 {max_threads} 线程)...")
        else:
            self.status_var.set(f"状态：批量执行中 (并发 {max_threads} 线程)...")

        self.progress["value"] = 0
        total_accounts = len(selected)
        self.progress["maximum"] = total_accounts

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

        completed_count = 0
        count_lock = threading.Lock()

        def finish_one():
            nonlocal completed_count
            with count_lock:
                completed_count += 1
                curr = completed_count

            def _u():
                self.progress["value"] = curr
                self.status_var.set(f"状态：已完成 {curr}/{total_accounts}")
            self._dispatch_ui(_u)

        def worker_loop(thread_id):
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    self._schedule_account_status(acc_ref, text)

                def progress_cb(step, total, text, acc_obj=acc):
                    self._schedule_account_status(acc_obj, text)

                combined_stop = CombinedStopEvent(self.stop_event, self._get_account_stop_event(acc))
                logger.info(f"[线程 {thread_id}] 开始处理账号 #{idx}")

                should_finish_in_finally = True
                op_success = False

                def finish_current_now(final_status: str | None = None, *, success: bool = False):
                    nonlocal should_finish_in_finally
                    if final_status is not None:
                        set_status(final_status)
                    self._set_account_batch_active(acc, False)
                    self._mark_batch_account_result(acc, success)
                    should_finish_in_finally = False
                    finish_one()
                    task_queue.task_done()
                    logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")

                if combined_stop.is_set():
                    finish_current_now("已停止", success=False)
                    continue

                try:
                    client = None
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                    trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
                    quote_asset = self._ensure_trade_symbol_supported(
                        client,
                        trade_account_type,
                        trade_mode,
                        spot_symbol,
                        futures_symbol,
                    )
                    required_quote_amount = None
                    if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
                        required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))

                    # 1) 只查询总资产
                    if batch_total_asset_only:
                        set_status("查询总资产...")
                        total_usdt, rows = client.query_total_wallet_balance("USDT")
                        asset_breakdown = client.query_asset_balances_breakdown()
                        asset_text = self._format_asset_breakdown_text(asset_breakdown)
                        self._record_batch_balance_metric(acc, total_usdt, "USDT")

                        self.record_total_asset(
                            idx,
                            acc["api_key"],
                            acc["address"],
                            acc["network"],
                            total_usdt
                        )

                        detail_text = " | ".join(
                            [f'{r["walletName"]}:{r["balance"]:.4f}' for r in rows if r["balance"] > 0]
                        )
                        logger.info(
                            "账号 #%d 总资产约 %s USDT；钱包=%s；币种=%s",
                            idx,
                            f"{total_usdt:.8f}",
                            detail_text if detail_text else "无非零钱包余额",
                            asset_text if asset_text != "--" else "无币种资产",
                        )

                        set_status(asset_text if asset_text != "--" else f"总资产 {Decimal(str(total_usdt)):.4f} USDT")
                        op_success = True

                    # 2) 批量归集BNB模式
                    elif batch_collect_bnb_mode:
                        logger.info(f"账号 #{idx} 开始执行【批量归集BNB模式】")
                        set_status("归集合约/资金...")
                        client.collect_all_to_spot()

                        if combined_stop.is_set():
                            finish_current_now("已停止")
                            continue

                        time.sleep(1)

                        set_status("小额兑换为BNB...")
                        try:
                            convert_result = client.convert_spot_dust_to_bnb()
                            logger.info(f"账号 #{idx} 小额兑换完成，数量: {len(convert_result)}")
                        except Exception as e:
                            logger.warning(f"账号 #{idx} 小额兑换失败: {e}")

                        if combined_stop.is_set():
                            finish_current_now("已停止")
                            continue

                        time.sleep(1)

                        if batch_sell_large_spot_to_bnb:
                            set_status("大额币卖USDT...")
                            try:
                                sold_assets = client.sell_large_spot_assets_to_usdt()
                                logger.info(f"账号 #{idx} 大额币卖出完成: {sold_assets}")
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 大额币卖出失败: {e}")

                            if combined_stop.is_set():
                                finish_current_now("已停止")
                                continue

                            time.sleep(1)

                            set_status("USDT买入BNB...")
                            try:
                                buy_ok = client.spot_buy_all_usdt(buffer=0.0, symbol="BNBUSDT")
                                if buy_ok:
                                    logger.info("账号 #%d 已将现货 USDT 买入为 BNB", idx)
                                else:
                                    logger.info("账号 #%d 无可用 USDT 买入 BNB", idx)
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 用 USDT 买 BNB 失败: {e}")

                            time.sleep(1)

                        set_status("查询BNB余额...")
                        bnb_balance = client.spot_balance("BNB")
                        self._record_batch_balance_metric(acc, Decimal(str(bnb_balance)), "BNB")
                        logger.info("账号 #%d 当前现货 BNB = %.8f", idx, bnb_balance)

                        set_status("提现BNB...")
                        try:
                            amount = client.withdraw_all_coin(
                                coin="BNB",
                                address=acc["address"],
                                network=(acc["network"] or WITHDRAW_NETWORK_DEFAULT),
                                fee_buffer=withdraw_buffer,
                                enable_withdraw=enable_withdraw,
                            )
                            self._record_batch_withdraw_metric(acc, amount, "BNB")
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            set_status(self._format_withdraw_amount_status(amount, "BNB", enable_withdraw=enable_withdraw))
                            op_success = True
                        except Exception as e:
                            logger.error(f"账号 #{idx} BNB提现失败: {e}")
                            set_status("BNB提现失败")
                            op_success = False

                    # 3) 原批量现货策略
                    else:
                        need_wait_usdt = not skip_usdt_wait_in_batch

                        if need_wait_usdt:
                            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                                set_status("检测挂单余额...")
                            else:
                                set_status(f"检测 {quote_asset} 到账...")
                            if not self.wait_for_usdt(
                                usdt_timeout,
                                combined_stop,
                                client=client,
                                symbol=trade_symbol,
                                trade_account_type=trade_account_type,
                                trade_mode=trade_mode,
                                required_quote_amount=required_quote_amount,
                            ):
                                if combined_stop.is_set():
                                    logger.info(f"账号 #{idx} 已请求停止")
                                    finish_current_now("已停止", success=False)
                                else:
                                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                                        logger.info(f"账号 #{idx} 挂单余额检测超时，跳过")
                                        set_status("无可挂单余额")
                                    else:
                                        logger.info(f"账号 #{idx} {quote_asset} 检测超时，跳过")
                                        set_status(f"{quote_asset} 未到账")
                                    finish_current_now(success=False)
                                continue
                        else:
                            logger.info(f"账号 #{idx} 已开启“批量策略跳过{quote_asset}检测”")
                            set_status(f"跳过{quote_asset}检测")

                        set_status("策略执行中...")
                        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
                            logger.info(
                                "账号 #%d 开始执行合约策略：%s，方向=%s，轮次=%d，下单金额=%s，杠杆=%s，保证金模式=%s",
                                idx,
                                futures_symbol,
                                futures_side,
                                futures_rounds,
                                futures_amount_value,
                                futures_leverage,
                                futures_margin_type,
                            )
                        elif trade_mode == TRADE_MODE_MARKET:
                            logger.info("账号 #%d 开始执行市价策略：%d 轮，预买BNB金额=%s", idx, spot_rounds, bnb_topup_amount_value)
                        elif trade_mode == TRADE_MODE_CONVERT:
                            logger.info("账号 #%d 开始执行闪兑策略：%d 轮，交易对=%s", idx, spot_rounds, spot_symbol)
                        elif trade_mode == TRADE_MODE_LIMIT:
                            if reprice_threshold_value > 0:
                                effective_reprice_threshold = client.normalize_price_delta(
                                    spot_symbol,
                                    reprice_threshold_value,
                                    min_one_tick=True,
                                )
                            else:
                                effective_reprice_threshold = Decimal("0")
                            logger.info(
                                "账号 #%d 开始执行挂单策略：预买BNB金额=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                                idx,
                                bnb_topup_amount_value,
                                bnb_fee_stop_value,
                                BinanceClient._format_decimal(effective_reprice_threshold),
                                quote_asset,
                            )
                        else:
                            premium_append_mode_enabled = (
                                premium_order_count > 1 and Decimal(str(premium_append_threshold_value or "0")) > 0
                            )
                            if premium_append_mode_enabled:
                                effective_reprice_threshold = Decimal("0")
                            elif reprice_threshold_value > 0:
                                effective_reprice_threshold = client.normalize_price_delta(
                                    spot_symbol,
                                    reprice_threshold_value,
                                    min_one_tick=True,
                                )
                            else:
                                effective_reprice_threshold = Decimal("0")
                            premium_append_log_text = Decimal("0")
                            if premium_append_mode_enabled:
                                premium_append_log_text = client.normalize_price_delta(
                                    spot_symbol,
                                    premium_append_threshold_value,
                                    min_one_tick=True,
                                )
                            logger.info(
                                "账号 #%d 开始执行溢价单策略：预买BNB金额=%s，溢价=%s %s，笔数=%s，追加挂单=%s %s，剩余bnb手续费=%s，重挂阈值=%s %s",
                                idx,
                                bnb_topup_amount_value,
                                premium_delta_value,
                                quote_asset,
                                premium_order_count,
                                BinanceClient._format_decimal(premium_append_log_text),
                                quote_asset,
                                bnb_fee_stop_value,
                                BinanceClient._format_decimal(effective_reprice_threshold),
                                quote_asset,
                            )

                        def sleep_fn():
                            if combined_stop.is_set():
                                return
                            if max_delay < min_delay:
                                low_ms, high_ms = max_delay, min_delay
                            else:
                                low_ms, high_ms = min_delay, max_delay
                            combined_stop.wait(random.randint(low_ms, high_ms) / 1000.0)

                        withdraw_state = {"amount": None}

                        def withdraw_callback(
                            amount,
                            idx_local=idx,
                            api_key=acc["api_key"],
                            address=acc["address"],
                        ):
                            try:
                                amt = float(amount)
                            except Exception:
                                amt = 0.0
                            withdraw_state["amount"] = amt
                            if amt > 0:
                                self.record_withdraw(idx_local, api_key, address, amt)

                        strategy = Strategy(
                            client=client,
                            spot_rounds=spot_rounds,
                            withdraw_coin=withdraw_coin,
                            withdraw_address=acc["address"],
                            withdraw_network=acc["network"],
                            withdraw_fee_buffer=withdraw_buffer,
                            spot_symbol=spot_symbol,
                            sleep_fn=sleep_fn,
                            enable_withdraw=enable_withdraw,
                            withdraw_callback=withdraw_callback,
                            trade_account_type=trade_account_type,
                            trade_mode=trade_mode,
                            premium_delta=premium_delta_value,
                            premium_order_count=premium_order_count,
                            premium_append_threshold=premium_append_threshold_value,
                            bnb_fee_stop_value=bnb_fee_stop_value,
                            bnb_topup_amount=bnb_topup_amount_value,
                            reprice_threshold_amount=reprice_threshold_value,
                            futures_symbol=futures_symbol,
                            futures_rounds=futures_rounds,
                            futures_amount=futures_amount_value,
                            futures_leverage=futures_leverage,
                            futures_margin_type=futures_margin_type,
                            futures_side=futures_side,
                        )

                        strategy_result = strategy.run(combined_stop, progress_cb=progress_cb)

                        if not combined_stop.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount

                            if isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(f"提现失败 {self._compact_error_text(strategy_result.get('withdraw_error', ''))}")
                                op_success = False
                            else:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(
                                    self._format_withdraw_amount_status(
                                        float(final_amount or 0.0),
                                        withdraw_coin,
                                        enable_withdraw=enable_withdraw,
                                    )
                                )
                                op_success = True
                        else:
                            set_status("已停止")
                            op_success = False

                except Exception as e:
                    logger.error(f"账号 #{idx} 执行异常: {e}")
                    set_status("异常")
                    op_success = False
                finally:
                    self._close_binance_client_instance(client)
                    if should_finish_in_finally:
                        self._set_account_batch_active(acc, False)
                        self._mark_batch_account_result(acc, op_success)
                        finish_one()
                        task_queue.task_done()
                        logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")

        def controller():
            threads = []
            logger.info(f"启动 {max_threads} 个工作线程处理 {total_accounts} 个账号")
            for i in range(max_threads):
                t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            logger.info("批量任务全部结束")
            self._dispatch_ui(self._on_worker_finished)

        self.worker_thread = self._start_managed_thread(controller, name="exchange-batch-run")
    def batch_manual_withdraw(self, accounts_to_run=None, *, require_confirm: bool = True):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else [acc for acc in self.accounts if acc["selected_var"].get()]
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        if require_confirm and not messagebox.askyesno("确认执行", f"确认对 {len(selected)} 个账号执行批量提现吗？"):
            return

        try:
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            max_threads = int(self.max_threads_var.get())
        except ValueError:
            messagebox.showerror("错误", "参数格式不正确")
            return

        if max_threads < 1:
            max_threads = 1

        coin = self.withdraw_coin_var.get().strip().upper()
        if not coin:
            messagebox.showerror("错误", "请先填写提现币种")
            return

        logger.info("交易所批量提现链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self._batch_task_active = True
        self._begin_batch_summary_tracking(
            action_label="批量提现",
            runner="batch_manual_withdraw",
            retry_kwargs={},
            selected_accounts=selected,
        )
        self._prepare_accounts_for_batch(selected)
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self._set_retry_failed_button_state()
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：批量提现中...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

        total_accounts = len(selected)
        completed_count = 0
        count_lock = threading.Lock()

        def withdraw_worker(thread_id):
            nonlocal completed_count
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    self._schedule_account_status(acc_ref, text)

                combined_stop = CombinedStopEvent(self.stop_event, self._get_account_stop_event(acc))
                op_success = False
                try:
                    client = None
                    if combined_stop.is_set():
                        set_status("已停止")
                        op_success = False
                    else:
                        set_status(f"提现 {coin}...")
                        client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                        if combined_stop.is_set():
                            set_status("已停止")
                            op_success = False
                        else:
                            amount = client.withdraw_all_coin(
                                coin=coin,
                                address=acc["address"],
                                network=acc["network"],
                                fee_buffer=withdraw_buffer,
                                enable_withdraw=True,
                                auto_collect_to_spot=True,
                            )
                            self._record_batch_withdraw_metric(acc, amount, coin)
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            set_status(self._format_withdraw_amount_status(amount, coin, enable_withdraw=True))
                            op_success = True
                except Exception as e:
                    logger.error(f"账号 #{idx} 提现失败: {e}")
                    set_status("提现失败")
                    op_success = False
                finally:
                    self._close_binance_client_instance(client)
                    self._set_account_batch_active(acc, False)
                    self._mark_batch_account_result(acc, op_success)
                    task_queue.task_done()
                    with count_lock:
                        completed_count += 1
                        curr = completed_count

                    def _u():
                        self.progress["value"] = curr
                        self.status_var.set(f"批量提现进度: {curr}/{total_accounts}")
                    self._dispatch_ui(_u)

        def controller():
            threads = []
            for i in range(max_threads):
                t = threading.Thread(target=withdraw_worker, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self._dispatch_ui(self._on_worker_finished)

        self.worker_thread = self._start_managed_thread(controller, name="exchange-batch-withdraw")
