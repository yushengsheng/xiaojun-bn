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


class ExchangeBatchFlowMixin(object):
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
        if runner == "batch_redeem_flexible_earn":
            self.batch_redeem_flexible_earn(accounts_to_run=accounts_to_retry, require_confirm=False)
            return
        self.run_selected_accounts(accounts_to_run=accounts_to_retry, require_confirm=False, **retry_kwargs)
    def run_query_total_assets_for_all_accounts(self):
        selected_accounts = self._get_selected_accounts()
        if not selected_accounts:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        self.run_selected_accounts(accounts_to_run=selected_accounts, batch_total_asset_only=True, require_confirm=False)
    def batch_redeem_flexible_earn(self, accounts_to_run=None, *, require_confirm: bool = True):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return

        try:
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "最小")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "最大")
            max_threads = int(self.max_threads_var.get())
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

        if max_threads < 1:
            max_threads = 1

        if require_confirm:
            text = (
                f"即将对 {len(selected)} 个账号执行“批量赎回理财”。\n"
                "流程包含：查询当前账号所有活期理财持仓，发现可赎回资产后逐个赎回到现货。\n"
                "该模式不限制币种，只要活期理财里有可赎回资产就会处理。\n"
                "赎回流程会复用共享策略中的随机延迟，并遵守单账号连续赎回的接口节流。\n\n"
                "确认继续吗？"
            )
            if not messagebox.askyesno("确认执行", text):
                return

        logger.info("交易所批量赎回理财链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self._batch_task_active = True
        self._begin_batch_summary_tracking(
            action_label="批量赎回理财",
            runner="batch_redeem_flexible_earn",
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
        self.btn_batch_redeem_earn.config(state="disabled")
        self._set_retry_failed_button_state()
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set(f"状态：批量赎回理财中 (并发 {max_threads} 线程)...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for batch_pos, acc in enumerate(selected, start=1):
            task_queue.put((self._account_display_index(acc, batch_pos), acc))

        total_accounts = len(selected)
        completed_count = 0
        count_lock = threading.Lock()
        # Simple Earn 持仓查询单次权重较高，跨账号并发时做全局串行节流，避免瞬时把同一出口 IP 打爆。
        position_query_gap_seconds = 1.7
        position_query_lock = threading.Lock()
        last_position_query_started_at = {"value": 0.0}

        def finish_one():
            nonlocal completed_count
            with count_lock:
                completed_count += 1
                curr = completed_count

            def _u():
                self.progress["value"] = curr
                self.status_var.set(f"批量赎回理财进度: {curr}/{total_accounts}")
            self._dispatch_ui(_u)

        def wait_for_position_query_slot(stop_event) -> bool:
            while True:
                if stop_event is not None and stop_event.is_set():
                    return False
                with position_query_lock:
                    now = time.monotonic()
                    last_started_at = float(last_position_query_started_at.get("value") or 0.0)
                    wait_seconds = 0.0 if last_started_at <= 0 else max(
                        0.0,
                        position_query_gap_seconds - (now - last_started_at),
                    )
                    if wait_seconds <= 0:
                        last_position_query_started_at["value"] = now
                        return True
                if stop_event is None:
                    time.sleep(min(0.2, wait_seconds) if wait_seconds > 0 else 0.0)
                elif stop_event.wait(min(0.2, wait_seconds) if wait_seconds > 0 else 0.0):
                    return False

        def redeem_worker(thread_id):
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    clear_exchange_account_log_index()
                    return
                set_exchange_account_log_index(idx)

                final_status_text = ""

                def set_status(text, acc_ref=acc):
                    self._schedule_account_status(acc_ref, text)
                def set_final_status(text, acc_ref=acc):
                    nonlocal final_status_text
                    final_status_text = str(text or "").strip()
                    self._schedule_account_status(acc_ref, final_status_text)

                combined_stop = CombinedStopEvent(self.stop_event, self._get_account_stop_event(acc))
                op_success = False
                client = None
                try:
                    if combined_stop.is_set():
                        set_final_status("已停止")
                    else:
                        client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                        set_status("查询活期理财...")
                        if not wait_for_position_query_slot(combined_stop):
                            set_final_status("已停止")
                            op_success = False
                            continue
                        positions = client.get_simple_earn_flexible_positions()

                        redeemable_positions: list[dict] = []
                        skipped_entries: list[str] = []
                        for item in positions:
                            asset = str((item or {}).get("asset") or "").strip().upper()
                            product_id = str((item or {}).get("productId") or "").strip()
                            total_amount = Decimal(str((item or {}).get("totalAmount") or "0"))
                            can_redeem = bool((item or {}).get("canRedeem"))
                            auto_subscribe = bool((item or {}).get("autoSubscribe"))
                            if total_amount <= 0:
                                continue
                            if not asset:
                                continue
                            if not product_id:
                                skipped_entries.append(f"{asset}(缺少产品ID)")
                                continue
                            if not can_redeem:
                                skipped_entries.append(f"{asset}(当前不可赎回)")
                                continue
                            redeemable_positions.append(
                                {
                                    "asset": asset,
                                    "productId": product_id,
                                    "totalAmount": total_amount,
                                    "autoSubscribe": auto_subscribe,
                                }
                            )

                        if not redeemable_positions:
                            self._record_batch_count_metric(acc, "redeemed_assets_by_account", 0)
                            self._record_batch_count_metric(acc, "skipped_assets_by_account", len(skipped_entries))
                            self._record_batch_count_metric(acc, "auto_disabled_by_account", 0)
                            if skipped_entries:
                                set_final_status(
                                    self._exchange_success_status_text(
                                        f"无资产可赎回；跳过：{self._detail_list_preview(skipped_entries)}"
                                    )
                                )
                            else:
                                set_final_status(self._exchange_success_status_text("无资产可赎回"))
                            op_success = True
                        else:
                            redeemed_assets: list[str] = []
                            auto_disabled_assets: list[str] = []
                            failed_entries: list[str] = []
                            stopped_early = False

                            for pos_idx, item in enumerate(redeemable_positions, start=1):
                                asset = str(item.get("asset") or "").strip().upper()
                                amount = Decimal(str(item.get("totalAmount") or "0"))
                                auto_subscribe_enabled = bool(item.get("autoSubscribe"))
                                if combined_stop.is_set():
                                    stopped_early = True
                                    set_final_status("已停止")
                                    break

                                min_gap = 0.0 if pos_idx == 1 else 3.0
                                delay_ok = self._wait_batch_random_delay(
                                    combined_stop,
                                    min_delay,
                                    max_delay,
                                    minimum_seconds=min_gap,
                                    log_prefix=f"账号 #{idx} 活期{asset}赎回前 ",
                                )
                                if not delay_ok:
                                    stopped_early = True
                                    set_final_status("已停止")
                                    break

                                if auto_subscribe_enabled:
                                    try:
                                        client.set_simple_earn_flexible_auto_subscribe(
                                            str(item.get("productId") or "").strip(),
                                            False,
                                        )
                                        auto_disabled_assets.append(asset)
                                        logger.info("账号 #%d 已关闭活期理财自动申购：%s", idx, asset)
                                    except Exception as e:
                                        logger.error("账号 #%d 关闭活期理财自动申购失败 %s: %s", idx, asset, e)
                                        failed_entries.append(
                                            f"{asset}(关闭自动申购失败:{summarize_exchange_exception(e, fallback='关闭失败', max_len=10)})"
                                        )
                                        continue

                                set_status(f"赎回 {asset} ({pos_idx}/{len(redeemable_positions)})...")
                                try:
                                    result = client.redeem_simple_earn_flexible_product(
                                        str(item.get("productId") or "").strip(),
                                        redeem_all=True,
                                        dest_account="SPOT",
                                    )
                                    redeem_id = str((result or {}).get("redeemId") or "").strip()
                                    logger.info(
                                        "账号 #%d 活期理财赎回已提交：资产=%s，金额=%s，redeemId=%s",
                                        idx,
                                        asset,
                                        BinanceClient._format_decimal(amount),
                                        redeem_id or "--",
                                    )
                                    redeemed_assets.append(asset)
                                except Exception as e:
                                    logger.error("账号 #%d 活期理财赎回失败 %s: %s", idx, asset, e)
                                    failed_entries.append(
                                        f"{asset}({summarize_exchange_exception(e, fallback='赎回失败', max_len=12)})"
                                    )

                            self._record_batch_count_metric(acc, "redeemed_assets_by_account", len(redeemed_assets))
                            self._record_batch_count_metric(acc, "skipped_assets_by_account", len(skipped_entries))
                            self._record_batch_count_metric(acc, "auto_disabled_by_account", len(auto_disabled_assets))

                            if stopped_early:
                                op_success = False
                            elif failed_entries:
                                parts: list[str] = []
                                if redeemed_assets:
                                    parts.append(f"已赎回：{self._asset_list_preview(redeemed_assets, limit=5)}")
                                if auto_disabled_assets:
                                    parts.append(f"已关自动申购：{self._asset_list_preview(auto_disabled_assets, limit=5)}")
                                if skipped_entries:
                                    parts.append(f"跳过：{self._detail_list_preview(skipped_entries)}")
                                parts.append(f"失败：{self._detail_list_preview(failed_entries)}")
                                set_final_status(self._exchange_failed_status_text("；".join(parts), fallback="部分赎回失败"))
                                op_success = False
                            else:
                                parts = [f"已赎回：{self._asset_list_preview(redeemed_assets, limit=6)}"]
                                if auto_disabled_assets:
                                    parts.append(f"已关自动申购：{self._asset_list_preview(auto_disabled_assets, limit=6)}")
                                if skipped_entries:
                                    parts.append(f"跳过：{self._detail_list_preview(skipped_entries)}")
                                set_final_status(self._exchange_success_status_text("；".join(parts)))
                                op_success = True
                except Exception as e:
                    logger.error("账号 #%d 批量赎回理财失败: %s", idx, e)
                    set_final_status(self._exchange_error_status_text("赎回理财失败", e, fallback="赎回理财失败"))
                    op_success = False
                finally:
                    self._close_binance_client_instance(client)
                    self._set_account_batch_active(acc, False)
                    self._mark_batch_account_result(acc, op_success)
                    task_queue.task_done()
                    finish_one()
                    result_text = self._plain_exchange_status_text(
                        final_status_text,
                        fallback=("成功" if op_success else "已停止" if combined_stop.is_set() else "失败"),
                    )
                    logger.info("[线程 %d] 账号 #%d 理财赎回处理完毕：%s", thread_id, idx, result_text)
                    clear_exchange_account_log_index()

        def controller():
            threads = []
            for i in range(max_threads):
                t = threading.Thread(target=redeem_worker, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self._dispatch_ui(self._on_worker_finished)

        self.worker_thread = self._start_managed_thread(controller, name="exchange-batch-redeem-earn")
    def run_batch_collect_bnb_with_confirm(self, accounts_to_run=None):
        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        text = (
            f"即将对 {len(selected)} 个账号执行“归集BNB并提现”。\n"
            "流程包含：归集合约/资金到现货、小额币兑换BNB、大额资产优先直接闪兑BNB，失败时回退卖USDT再换BNB、最后自动提现BNB。\n"
            "该模式固定提取 BNB，不受“自动提现”勾选和提现币种设置影响。\n\n"
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
        batch_enable_bnb_topup_override: bool | None = None,
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

        spot_symbol = BinanceClient.normalize_trade_symbol(self.spot_symbol_var.get())
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
        else:
            trade_account_type = TRADE_ACCOUNT_TYPE_SPOT
            spot_rounds = max(1, int(self.spot_rounds_var.get() or SPOT_ROUNDS_DEFAULT))
            trade_mode = TRADE_MODE_MARKET
            premium_delta_value = None
            premium_order_count = PREMIUM_ORDER_COUNT_DEFAULT
            premium_append_threshold_value = Decimal(PREMIUM_APPEND_THRESHOLD_DEFAULT)
            premium_partial_fill = False
            bnb_fee_stop_value = None
            bnb_topup_amount_value = Decimal("0")
            reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
            futures_symbol = BinanceClient.normalize_trade_symbol(self.futures_symbol_var.get())
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

        configured_bnb_topup_amount_value = Decimal(str(bnb_topup_amount_value or "0"))
        show_bnb_topup_option = (
            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and spot_rounds != 0
            and trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}
        )
        enable_bnb_topup = (
            bool(batch_enable_bnb_topup_override)
            if batch_enable_bnb_topup_override is not None
            else bool(configured_bnb_topup_amount_value > 0)
        )

        if require_confirm and not batch_total_asset_only and not batch_collect_bnb_mode:
            confirmed, batch_enable_withdraw, batch_enable_bnb_topup = self._show_batch_execute_confirm_dialog(
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
                bnb_topup_amount=configured_bnb_topup_amount_value,
                enable_bnb_topup=enable_bnb_topup,
                show_bnb_topup_option=show_bnb_topup_option,
            )
            if not confirmed:
                return
            enable_withdraw = bool(batch_enable_withdraw)
            batch_enable_withdraw_override = enable_withdraw
            enable_bnb_topup = bool(batch_enable_bnb_topup)
            batch_enable_bnb_topup_override = enable_bnb_topup

        if not (show_bnb_topup_option and enable_bnb_topup):
            bnb_topup_amount_value = Decimal("0")
        else:
            bnb_topup_amount_value = configured_bnb_topup_amount_value

        if batch_total_asset_only:
            self.total_asset_results = {}

        if batch_total_asset_only:
            batch_action_label = "查询全部总资产"
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            batch_action_label = "归集BNB并提现"
        elif batch_collect_bnb_mode:
            batch_action_label = "归集BNB"
        else:
            batch_action_label = "批量执行"
        preflight_required = bool(trade_settings)
        validated_quote_asset = None

        logger.info("交易所批量链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self._batch_task_active = True
        if not preflight_required:
            self._begin_batch_summary_tracking(
                action_label=batch_action_label,
                runner="run_selected_accounts",
                retry_kwargs={
                    "batch_total_asset_only": bool(batch_total_asset_only),
                    "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                    "batch_sell_large_spot_to_bnb": bool(batch_sell_large_spot_to_bnb),
                    "batch_enable_withdraw_override": enable_withdraw if (not batch_total_asset_only and not batch_collect_bnb_mode) else None,
                    "batch_enable_bnb_topup_override": (
                        bool(enable_bnb_topup)
                        if (not batch_total_asset_only and not batch_collect_bnb_mode and show_bnb_topup_option)
                        else None
                    ),
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
        self.btn_batch_redeem_earn.config(state="disabled")
        self._set_retry_failed_button_state()
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)

        if preflight_required:
            self.status_var.set("状态：初始化连接/校验交易对中...")
        elif batch_total_asset_only:
            self.status_var.set(f"状态：查询全部总资产中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            self.status_var.set(f"状态：归集BNB并提现中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode:
            self.status_var.set(f"状态：批量归集BNB模式运行中 (并发 {max_threads} 线程)...")
        else:
            self.status_var.set(f"状态：批量执行中 (并发 {max_threads} 线程)...")

        self.progress["value"] = 0
        total_accounts = len(selected)
        self.progress["maximum"] = total_accounts

        task_queue = queue.Queue()
        for batch_pos, acc in enumerate(selected, start=1):
            task_queue.put((self._account_display_index(acc, batch_pos), acc))

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
                    clear_exchange_account_log_index()
                    return
                set_exchange_account_log_index(idx)

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
                    clear_exchange_account_log_index()

                if combined_stop.is_set():
                    finish_current_now("已停止", success=False)
                    continue

                try:
                    client = None
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                    trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
                    quote_asset = str(validated_quote_asset or "").strip().upper()
                    if (not batch_total_asset_only) and (not quote_asset):
                        quote_asset = self._ensure_trade_symbol_supported(
                            client,
                            trade_account_type,
                            trade_mode,
                            spot_symbol,
                            futures_symbol,
                            spot_rounds,
                        )
                    required_quote_amount = None
                    if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
                        required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))

                    # 1) 只查询总资产
                    if batch_total_asset_only:
                        set_status("查询总资产...")
                        total_usdt, rows = client.query_total_wallet_balance("USDT", fast=True)
                        asset_breakdown = client.query_asset_balances_breakdown(fast=True)
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

                        set_status(
                            self._exchange_success_status_text(
                                asset_text if asset_text != "--" else f"总资产 {Decimal(str(total_usdt)):.4f} USDT"
                            )
                        )
                        op_success = True

                    # 2) 批量归集BNB模式
                    elif batch_collect_bnb_mode:
                        collect_bnb_force_withdraw = bool(batch_sell_large_spot_to_bnb)
                        collect_withdraw_coin, collect_withdraw_enabled = self._collect_bnb_withdraw_runtime(
                            force_bnb_withdraw=collect_bnb_force_withdraw,
                            enable_withdraw=enable_withdraw,
                            withdraw_coin=withdraw_coin,
                        )
                        if batch_sell_large_spot_to_bnb:
                            logger.info(f"账号 #{idx} 开始执行【归集BNB并提现模式】")
                        else:
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
                            set_status("大额资产兑BNB...")
                            convert_summary = {"direct_bnb": [], "fallback_usdt": [], "residual_assets": []}
                            try:
                                convert_summary = client.convert_large_spot_assets_to_bnb()
                                logger.info(
                                    "账号 #%d 大额资产处理完成：直兑BNB=%s，回退USDT=%s",
                                    idx,
                                    convert_summary.get("direct_bnb", []),
                                    convert_summary.get("fallback_usdt", []),
                                )
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 大额资产兑BNB失败: {e}")

                            if combined_stop.is_set():
                                finish_current_now("已停止")
                                continue

                            time.sleep(1)

                            set_status("USDT补换BNB...")
                            try:
                                usdt_balance = client.spot_asset_balance_decimal("USDT")
                                if usdt_balance > 0:
                                    buy_ok = client.buy_bnb_with_quote_amount(
                                        "USDT",
                                        usdt_balance,
                                        assume_spot_funds_ready=True,
                                        spot_balance_snapshot=usdt_balance,
                                    )
                                else:
                                    buy_ok = False
                                if buy_ok:
                                    logger.info("账号 #%d 已将回退得到的现货 USDT 闪兑为 BNB", idx)
                                else:
                                    logger.info("账号 #%d 无需再用 USDT 补换 BNB", idx)
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 用 USDT 补换 BNB 失败: {e}")

                            time.sleep(1)

                        if batch_sell_large_spot_to_bnb:
                            set_status("确认BNB在现货...")
                            client.ensure_bnb_fee_ready_in_spot()

                        residual_assets: list[str] = []
                        if batch_sell_large_spot_to_bnb:
                            for asset in convert_summary.get("residual_assets", []):
                                asset_u = str(asset or "").strip().upper()
                                if asset_u and asset_u not in residual_assets:
                                    residual_assets.append(asset_u)
                            try:
                                for asset in self._collect_bnb_remaining_spot_assets(client):
                                    if asset not in residual_assets:
                                        residual_assets.append(asset)
                            except Exception as e:
                                logger.error(f"账号 #{idx} 归集完成后校验残留资产失败: {e}")
                                set_status(self._exchange_error_status_text("归集校验失败", e, fallback="归集校验失败"))
                                op_success = False
                                continue
                            if residual_assets:
                                asset_preview = self._asset_list_preview(residual_assets)
                                logger.warning("账号 #%d 仍有残留现货资产，已停止 BNB 提现: %s", idx, residual_assets)
                                set_status(self._exchange_failed_status_text(f"残留资产：{asset_preview}", fallback="残留资产"))
                                op_success = False
                                continue

                        set_status("查询BNB余额...")
                        bnb_balance = client.spot_asset_balance_decimal(collect_withdraw_coin)
                        self._record_batch_balance_metric(acc, bnb_balance, collect_withdraw_coin)
                        logger.info(
                            "账号 #%d 当前现货 %s = %s",
                            idx,
                            collect_withdraw_coin,
                            BinanceClient._format_decimal(bnb_balance),
                        )

                        set_status(f"提现{collect_withdraw_coin}...")
                        try:
                            amount = client.withdraw_all_coin(
                                coin=collect_withdraw_coin,
                                address=acc["address"],
                                network=(acc["network"] or WITHDRAW_NETWORK_DEFAULT),
                                fee_buffer=withdraw_buffer,
                                enable_withdraw=collect_withdraw_enabled,
                            )
                            self._record_batch_withdraw_metric(acc, amount, collect_withdraw_coin)
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            status_text, withdraw_success = self._format_withdraw_amount_status(
                                amount,
                                collect_withdraw_coin,
                                enable_withdraw=collect_withdraw_enabled,
                            )
                            set_status(status_text)
                            op_success = withdraw_success
                        except Exception as e:
                            logger.error(f"账号 #{idx} {collect_withdraw_coin}提现失败: {e}")
                            set_status(self._exchange_error_status_text(f"{collect_withdraw_coin}提现失败", e, fallback=f"{collect_withdraw_coin}提现失败"))
                            op_success = False

                    # 3) 原批量现货策略
                    else:
                        zero_round_spot_conversion = (
                            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and spot_rounds == 0
                        )
                        base_asset = (
                            BinanceClient.get_spot_base_asset(spot_symbol)
                            if zero_round_spot_conversion
                            else ""
                        )
                        wait_asset = base_asset if zero_round_spot_conversion else quote_asset
                        if zero_round_spot_conversion:
                            set_status(f"检测{base_asset} 到账...")
                        elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
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
                            spot_base_to_quote_only=zero_round_spot_conversion,
                        ):
                            if combined_stop.is_set():
                                logger.info(f"账号 #{idx} 已请求停止")
                                finish_current_now("已停止", success=False)
                            else:
                                if zero_round_spot_conversion:
                                    logger.info(f"账号 #{idx} {base_asset} 检测超时，跳过")
                                    set_status(self._exchange_failed_status_text(f"{base_asset} 未到账"))
                                elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                                    logger.info(f"账号 #{idx} 挂单余额检测超时，跳过")
                                    set_status(self._exchange_failed_status_text("无可挂单余额"))
                                else:
                                    logger.info(f"账号 #{idx} {wait_asset} 检测超时，跳过")
                                    set_status(self._exchange_failed_status_text(f"{wait_asset} 未到账"))
                                finish_current_now(success=False)
                            continue

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
                        elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and spot_rounds == 0:
                            logger.info("账号 #%d 开始执行0轮现货兑换，交易对=%s", idx, spot_symbol)
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
                            premium_append_log_text = Decimal("0")
                            if (not premium_partial_fill) and premium_append_mode_enabled:
                                premium_append_log_text = client.normalize_price_delta(
                                    spot_symbol,
                                    premium_append_threshold_value,
                                    min_one_tick=True,
                                )
                            logger.info(
                                "账号 #%d 开始执行溢价单策略：预买BNB金额=%s，溢价=%s %s，笔数=%s，追加挂单=%s %s，部分成交循环=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                                idx,
                                bnb_topup_amount_value,
                                premium_delta_value,
                                quote_asset,
                                premium_order_count,
                                BinanceClient._format_decimal(premium_append_log_text),
                                quote_asset,
                                "开" if premium_partial_fill else "关",
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
                            delay_seconds = random.randint(low_ms, high_ms) / 1000.0
                            if zero_round_spot_conversion:
                                logger.info(
                                    "账号 #%d 0轮随机延迟开始：计划 %.3f 秒（配置 %d-%d ms）",
                                    idx,
                                    delay_seconds,
                                    low_ms,
                                    high_ms,
                                )
                            delay_started_at = time.monotonic()
                            interrupted = combined_stop.wait(delay_seconds)
                            actual_delay = time.monotonic() - delay_started_at
                            if zero_round_spot_conversion:
                                if interrupted:
                                    logger.info(
                                        "账号 #%d 0轮随机延迟被停止信号中断：实际 %.3f / 计划 %.3f 秒",
                                        idx,
                                        actual_delay,
                                        delay_seconds,
                                    )
                                elif actual_delay + 0.05 < delay_seconds:
                                    logger.warning(
                                        "账号 #%d 0轮随机延迟提前返回：实际 %.3f / 计划 %.3f 秒",
                                        idx,
                                        actual_delay,
                                        delay_seconds,
                                    )
                                else:
                                    logger.info(
                                        "账号 #%d 0轮随机延迟结束：实际 %.3f / 计划 %.3f 秒",
                                        idx,
                                        actual_delay,
                                        delay_seconds,
                                    )

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

                        self._reconcile_exchange_orders(client, acc["api_key"])
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
                            operation_scope=self._exchange_operation_scope(acc["api_key"]),
                        )

                        strategy_result = strategy.run(combined_stop, progress_cb=progress_cb)

                        if not combined_stop.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount

                            trade_error = strategy_result.get("trade_error") if isinstance(strategy_result, dict) else ""
                            if trade_error:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(
                                    self._format_trade_failure_status(
                                        strategy_result.get("trade_error_stage", ""),
                                        trade_error,
                                    )
                                )
                                op_success = False
                            elif isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(
                                    self._exchange_error_status_text(
                                        "提现失败",
                                        strategy_result.get("withdraw_error", ""),
                                        fallback="提现失败",
                                    )
                                )
                                op_success = False
                            else:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                status_text, withdraw_success = self._format_batch_strategy_completion_status(
                                    float(final_amount or 0.0),
                                    withdraw_coin,
                                    enable_withdraw=enable_withdraw,
                                )
                                if status_text is not None:
                                    set_status(status_text)
                                op_success = withdraw_success
                        else:
                            set_status("已停止")
                            op_success = False

                except Exception as e:
                    logger.error(f"账号 #{idx} 执行异常: {e}")
                    set_status(self._exchange_error_status_text("异常", e))
                    op_success = False
                finally:
                    self._close_binance_client_instance(client)
                    if should_finish_in_finally:
                        self._set_account_batch_active(acc, False)
                        self._mark_batch_account_result(acc, op_success)
                        finish_one()
                        task_queue.task_done()
                        logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                    clear_exchange_account_log_index()

        def controller():
            nonlocal validated_quote_asset
            if preflight_required:
                validate_client = None
                try:
                    sample_acc = selected[0]
                    validate_client = self._create_binance_client(sample_acc["api_key"], sample_acc["api_secret"])
                    validated_quote_asset = self._ensure_trade_symbol_supported(
                        validate_client,
                        trade_account_type,
                        trade_mode,
                        spot_symbol,
                        futures_symbol,
                        spot_rounds,
                    )
                    self._begin_batch_summary_tracking(
                        action_label=batch_action_label,
                        runner="run_selected_accounts",
                        retry_kwargs={
                            "batch_total_asset_only": bool(batch_total_asset_only),
                            "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                            "batch_sell_large_spot_to_bnb": bool(batch_sell_large_spot_to_bnb),
                            "batch_enable_withdraw_override": enable_withdraw if (not batch_total_asset_only and not batch_collect_bnb_mode) else None,
                            "batch_enable_bnb_topup_override": (
                                bool(enable_bnb_topup)
                                if (not batch_total_asset_only and not batch_collect_bnb_mode and show_bnb_topup_option)
                                else None
                            ),
                        },
                        selected_accounts=selected,
                    )
                    self._prepare_accounts_for_batch(selected)

                    def _set_running_status():
                        self._set_retry_failed_button_state()
                        if batch_total_asset_only:
                            self.status_var.set(f"状态：查询全部总资产中 (并发 {max_threads} 线程)...")
                        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
                            self.status_var.set(f"状态：归集BNB并提现中 (并发 {max_threads} 线程)...")
                        elif batch_collect_bnb_mode:
                            self.status_var.set(f"状态：批量归集BNB模式运行中 (并发 {max_threads} 线程)...")
                        else:
                            self.status_var.set(f"状态：批量执行中 (并发 {max_threads} 线程)...")

                    self._dispatch_ui(_set_running_status)
                except Exception as e:
                    logger.error("批量执行启动校验失败: %s", e)

                    def _handle_start_failure(err=e):
                        messagebox.showerror("错误", "Binance 连接初始化失败: %s" % err)
                        self._restore_batch_ui_to_idle(status_text="状态：启动失败", finish_summary=False)

                    self._dispatch_ui(_handle_start_failure)
                    return
                finally:
                    self._close_binance_client_instance(validate_client)
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
        self.btn_batch_redeem_earn.config(state="disabled")
        self._set_retry_failed_button_state()
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：批量提现中...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for batch_pos, acc in enumerate(selected, start=1):
            task_queue.put((self._account_display_index(acc, batch_pos), acc))

        total_accounts = len(selected)
        completed_count = 0
        count_lock = threading.Lock()

        def withdraw_worker(thread_id):
            nonlocal completed_count
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    clear_exchange_account_log_index()
                    return
                set_exchange_account_log_index(idx)

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
                            status_text, withdraw_success = self._format_withdraw_amount_status(
                                amount,
                                coin,
                                enable_withdraw=True,
                            )
                            set_status(status_text)
                            op_success = withdraw_success
                except Exception as e:
                    logger.error(f"账号 #{idx} 提现失败: {e}")
                    set_status(self._exchange_error_status_text("提现失败", e, fallback="提现失败"))
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
                    clear_exchange_account_log_index()

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


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
