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
from exchange_order_recovery import exchange_operation_scope, reconcile_exchange_orders
from exchange_app_batch_summary import ExchangeBatchSummaryMixin
from exchange_app_batch_flows import ExchangeBatchFlowMixin


class ExchangeAppBatchMixin(ExchangeBatchSummaryMixin, ExchangeBatchFlowMixin):
    @staticmethod
    def _exchange_operation_scope(api_key: str) -> str:
        return exchange_operation_scope(api_key)

    def _reconcile_exchange_orders(self, client, api_key: str) -> None:
        reconcile_exchange_orders(getattr(self, "exchange_operation_journal", None), client, api_key)

    def _set_account_manage_buttons_state(self, state):
        for name in (
            "btn_add_account",
            "btn_toggle_select_accounts",
            "btn_del_accounts",
            "btn_export_accounts",
            "btn_import_accounts",
            "btn_paste_accounts",
            "btn_destroy_all_accounts",
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
        spot_base_to_quote_only: bool = False,
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
        spot_base_to_quote_only = bool(spot_base_to_quote_only and trade_type == TRADE_ACCOUNT_TYPE_SPOT)
        wait_asset = base_asset if spot_base_to_quote_only else quote_asset
        required_amount = Decimal(str(required_quote_amount)) if required_quote_amount is not None else Decimal("0")
        progress_log_times: dict[str, float] = {}
        error_log_times: dict[str, float] = {}
        full_collect_trade_assets: set[str] = set()
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

        def collect_trade_asset_to_spot(asset: str) -> None:
            asset_u = str(asset or "").strip().upper()
            if not asset_u:
                return
            if asset_u not in full_collect_trade_assets and hasattr(c, "collect_asset_to_spot"):
                c.collect_asset_to_spot(asset_u, context="交易前")
                full_collect_trade_assets.add(asset_u)
                return
            if hasattr(c, "collect_funding_asset_to_spot"):
                c.collect_funding_asset_to_spot(asset_u)
            elif hasattr(c, "collect_asset_to_spot"):
                c.collect_asset_to_spot(asset_u, context="交易前")

        while time.time() - start < timeout_sec:
            if stop_event and stop_event.is_set():
                logger.info("检测 %s 时收到停止信号，结束检测", wait_asset)
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
                    if spot_base_to_quote_only:
                        quote_balance_dec = Decimal("0")
                    else:
                        collect_trade_asset_to_spot(quote_asset)
                        quote_balance_dec = c.spot_asset_balance_decimal(quote_asset)
                    if (spot_base_to_quote_only or mode_name == TRADE_MODE_CONVERT) and base_asset:
                        collect_trade_asset_to_spot(base_asset)
                        base_balance_dec = c.spot_asset_balance_decimal(base_asset)
                    quote_balance = float(quote_balance_dec)
                    if spot_base_to_quote_only:
                        log_progress_throttled(
                            "spot-balance-zero-round",
                            "0轮兑换到账检测中，当前现货 %s = %.8f",
                            base_asset,
                            float(base_balance_dec),
                        )
                    elif mode_name == TRADE_MODE_LIMIT:
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
                    wait_asset,
                    e,
                )
                quote_balance = 0.0
                base_balance_dec = Decimal("0")

            balance_ready = quote_balance > 0
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and spot_base_to_quote_only:
                balance_ready = base_balance_dec > 0
            elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            elif trade_type == TRADE_ACCOUNT_TYPE_FUTURES and required_amount > 0:
                balance_ready = Decimal(str(quote_balance)) >= required_amount

            if balance_ready:
                if trade_type == TRADE_ACCOUNT_TYPE_SPOT and spot_base_to_quote_only:
                    logger.info("检测到可兑换 %s 余额，开始执行后续策略", base_asset)
                elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                    logger.info("检测到可挂单余额，开始执行后续策略")
                elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                    logger.info("检测到可闪兑余额，开始执行后续策略")
                else:
                    logger.info("检测到 %s 已到账，开始执行后续策略", quote_asset)
                return True

            delay_seconds = min(self._current_random_delay_seconds(), max(0.0, timeout_sec - (time.time() - start)))
            if delay_seconds <= 0:
                continue
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and spot_base_to_quote_only:
                log_progress_throttled("retry-zero-round", "未检测到可兑换 %s 余额，%.3f 秒后重试", base_asset, delay_seconds)
            elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                log_progress_throttled("retry-limit", "未检测到可挂单余额，%.3f 秒后重试", delay_seconds)
            elif trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
                log_progress_throttled("retry-convert", "未检测到可闪兑余额，%.3f 秒后重试", delay_seconds)
            else:
                log_progress_throttled("retry-balance", "%s 未到账，%.3f 秒后重试", quote_asset, delay_seconds)
            if stop_event:
                if stop_event.wait(delay_seconds):
                    logger.info("检测 %s 等待期间收到停止信号，结束检测", wait_asset)
                    return False
            else:
                time.sleep(delay_seconds)

        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and spot_base_to_quote_only:
            logger.error("在 %d 秒内未检测到可兑换 %s 余额，终止任务", timeout_sec, base_asset)
            return False
        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
            logger.error("在 %d 秒内未检测到可挂单余额，终止任务", timeout_sec)
            return False
        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_CONVERT:
            logger.error("在 %d 秒内未检测到可闪兑余额，终止任务", timeout_sec)
            return False
        logger.error("在 %d 秒内未检测到 %s 到账，终止任务", timeout_sec, wait_asset)
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
            premium_partial_fill = bool(trade_settings["premium_partial_fill"])
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

        spot_symbol = BinanceClient.normalize_trade_symbol(self.spot_symbol_var.get())
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
                spot_rounds,
            )
            effective_reprice_threshold = None
            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}:
                premium_append_mode_enabled = (
                    trade_mode == TRADE_MODE_PREMIUM
                    and premium_order_count > 1
                    and Decimal(str(premium_append_threshold_value or "0")) > 0
                )
                if (not premium_partial_fill) and premium_append_mode_enabled:
                    effective_reprice_threshold = Decimal("0")
                elif reprice_threshold_value > 0:
                    effective_reprice_threshold = client.normalize_price_delta(
                        spot_symbol,
                        reprice_threshold_value,
                        min_one_tick=True,
                    )
                else:
                    effective_reprice_threshold = Decimal("0")
            self._reconcile_exchange_orders(client, key)
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
        self.btn_batch_redeem_earn.config(state="disabled")
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
            premium_partial_fill=premium_partial_fill,
            bnb_fee_stop_value=bnb_fee_stop_value,
            bnb_topup_amount=bnb_topup_amount_value,
            reprice_threshold_amount=reprice_threshold_value,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            futures_amount=futures_amount_value,
            futures_leverage=futures_leverage,
            futures_margin_type=futures_margin_type,
            futures_side=futures_side,
            operation_journal=self.exchange_operation_journal,
            operation_scope=self._exchange_operation_scope(key),
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self._dispatch_ui(_update)

        def worker():
            failure_text = ""
            try:
                zero_round_spot_conversion = (
                    trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and spot_rounds == 0
                )
                wait_asset = (
                    BinanceClient.get_spot_base_asset(spot_symbol)
                    if zero_round_spot_conversion
                    else quote_asset
                )
                if not self.wait_for_usdt(
                    usdt_timeout,
                    self.stop_event,
                    client=client,
                    symbol=trade_symbol,
                    trade_account_type=trade_account_type,
                    trade_mode=trade_mode,
                    required_quote_amount=required_quote_amount,
                    spot_base_to_quote_only=zero_round_spot_conversion,
                ):
                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                        logger.info("挂单余额检测未通过，任务结束")
                    else:
                        logger.info("%s 检测未通过，任务结束", wait_asset)
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
                elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and spot_rounds == 0:
                    logger.info("开始执行策略：0轮现货兑换，交易对=%s", spot_symbol)
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
                    if (not premium_partial_fill) and premium_append_mode_enabled:
                        premium_append_log_text = client.normalize_price_delta(
                            spot_symbol,
                            premium_append_threshold_value,
                            min_one_tick=True,
                        )
                    logger.info(
                        "开始执行策略：溢价单模式，预买BNB金额=%s，溢价=%s %s，笔数=%s，追加挂单=%s %s，部分成交循环=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                        bnb_topup_amount_value,
                        premium_delta_value,
                        quote_asset,
                        premium_order_count,
                        BinanceClient._format_decimal(premium_append_log_text),
                        quote_asset,
                        "开" if premium_partial_fill else "关",
                        bnb_fee_stop_value,
                        BinanceClient._format_decimal(effective_reprice_threshold or Decimal("0")),
                        quote_asset,
                    )
                strategy.run(self.stop_event, progress_cb=progress_cb)
            except Exception as e:
                failure_text = str(e)
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self._dispatch_ui(lambda error=failure_text: self._on_worker_finished(error))

        self.worker_thread = self._start_managed_thread(worker, name="exchange-single-run")
    def _restore_batch_ui_to_idle(self, *, status_text: str, finish_summary: bool) -> None:
        self._batch_task_active = False
        self._clear_account_batch_runtime()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.btn_run_accounts.config(state="normal")
        self.btn_query_all_assets.config(state="normal")
        self.btn_collect_bnb_combo.config(state="normal")
        self.btn_batch_withdraw.config(state="normal")
        self.btn_batch_redeem_earn.config(state="normal")
        self.btn_refresh.config(state="normal")
        self.btn_withdraw.config(state="normal")
        self._set_account_manage_buttons_state("normal")
        self._set_combo_states_for_run(False)
        if finish_summary:
            self._finish_batch_summary_tracking()
        else:
            self.after(80, self._set_retry_failed_button_state)
        self.status_var.set(str(status_text or "状态：已完成/已停止"))
        self.progress["value"] = 0
    def _on_worker_finished(self, failure_text: str = ""):
        status_text = "状态：已完成/已停止" if not failure_text else f"状态：失败：{failure_text[:80]}"
        self._restore_batch_ui_to_idle(status_text=status_text, finish_summary=True)
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
                balances_text = self._query_account_asset_balances_text(client, fast=True)
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
