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
    LADDER_LAYOUT_DEFAULT,
    LADDER_MIN_POOL_NOTIONAL_DEFAULT,
    LADDER_SELL_PREMIUM_DEFAULT,
    LADDER_SELL_PREMIUM_ENABLED_DEFAULT,
    MARKET_STAGGER_ENABLED_DEFAULT,
    MarketStaggerCoordinator,
    MARKET_STOP_MODE_BNB,
    MARKET_STOP_MODE_DEFAULT,
    PREMIUM_APPEND_THRESHOLD_DEFAULT,
    PREMIUM_ORDER_COUNT_DEFAULT,
    REPRICE_THRESHOLD_DEFAULT,
    SPOT_ROUNDS_DEFAULT,
    SharedRateLimitGate,
    Strategy,
    TRADE_ACCOUNT_TYPE_EARN,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_MODE_CONVERT,
    TRADE_MODE_DEFAULT,
    TRADE_MODE_LADDER,
    TRADE_MODE_AUTO_LADDER,
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
from input_validation import bounded_int, finite_float
from exchange_order_recovery import exchange_operation_scope
from exchange_auto_ladder_observer import AutoLadderMarketObserver
from shared_utils import decimal_to_stat_text, trade_volume_time_range, normalize_trade_volume_date
from stores import _load_authenticated_config, _save_authenticated_config, config_or_backup_exists
from exchange_stocks import STOCK_TYPE


class ExchangeAppBatchMixin(object):

    COLLECT_BNB_RESIDUAL_IGNORE_USDT = Decimal("0.1")

    @classmethod
    def _batch_account_bnb_topup_amount(
        cls,
        acc: dict,
        configured_amount,
        enabled_account_keys: set[str],
    ) -> Decimal:
        if cls._account_batch_key(acc) not in enabled_account_keys:
            return Decimal("0")
        return Decimal(str(configured_amount or "0"))

    @staticmethod
    def _batch_supports_bnb_topup(
        trade_account_type: str,
        trade_mode: str,
        spot_rounds: int,
        market_stop_mode: str,
    ) -> bool:
        return bool(
            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and (
                (trade_mode == TRADE_MODE_MARKET and market_stop_mode == MARKET_STOP_MODE_BNB)
                or int(spot_rounds) != 0
            )
            and trade_mode in {
                TRADE_MODE_MARKET,
                TRADE_MODE_LIMIT,
                TRADE_MODE_PREMIUM,
                TRADE_MODE_LADDER,
                TRADE_MODE_AUTO_LADDER,
            }
        )

    @staticmethod
    def _batch_market_stagger_enabled(
        *,
        enabled: bool,
        account_count: int,
        max_threads: int,
        trade_account_type: str,
        trade_mode: str,
        spot_rounds: int,
        market_stop_mode: str,
    ) -> bool:
        market_has_rounds = int(spot_rounds) > 0 or market_stop_mode == MARKET_STOP_MODE_BNB
        return bool(
            enabled
            and int(account_count) > 1
            and int(max_threads) > 1
            and trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and trade_mode == TRADE_MODE_MARKET
            and market_has_rounds
        )

    @staticmethod
    def _uses_shared_rate_limit_gate(trade_account_type: str, trade_mode: str) -> bool:
        return trade_account_type in {
            TRADE_ACCOUNT_TYPE_SPOT,
            TRADE_ACCOUNT_TYPE_FUTURES,
            TRADE_ACCOUNT_TYPE_EARN,
        }

    @staticmethod
    def _exchange_operation_scope(api_key: str) -> str:
        return exchange_operation_scope(api_key)

    def _assert_account_cleanup_finished(self, client, api_key: str) -> None:
        pending_reader = getattr(self, "_pending_cleanup_snapshot", None)
        if callable(pending_reader) and any(getattr(strategy.c, "key", "") == api_key for strategy in pending_reader()):
            raise RuntimeError("该账号停止清理尚未完成，请等待清理确认后再启动")

    def _resolve_earn_subscribe_amount(self, client, asset: str, configured_amount, earn_amount_all: bool) -> Decimal:
        asset_u = str(asset or "").strip().upper()
        if not asset_u:
            raise RuntimeError("理财币种不能为空")
        if earn_amount_all:
            amount_dec = client.spot_asset_balance_decimal(asset_u)
            if amount_dec <= 0:
                raise RuntimeError(f"现货 {asset_u} 可用余额为 0，无法全部存入理财")
            logger.info(
                "理财全部存入：现货 %s 可用余额=%s",
                asset_u,
                self._format_amount(amount_dec),
            )
            return amount_dec
        amount_dec = Decimal(str(configured_amount or "0"))
        if amount_dec <= 0:
            raise RuntimeError("理财存入数量必须大于 0")
        return amount_dec

    def _set_account_manage_buttons_state(self, state):
        for name in (
            "btn_add_account",
            "btn_toggle_select_accounts",
            "btn_del_accounts",
            "btn_export_accounts",
            "btn_import_accounts",
            "btn_paste_accounts",
            "btn_query_monthly_volume",
            "btn_volume_settings",
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
            "market_stop_mode_combo",
            "futures_margin_type_combo",
            "futures_side_combo",
            "ladder_layout_combo",
            "stock_wallet_combo",
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
            elif (
                spot_base_to_quote_only
                and mode_name == TRADE_MODE_MARKET
                and callable(getattr(c, "resolve_spot_market_conversion", None))
            ):
                route = c.resolve_spot_market_conversion(symbol)
                base_asset = str(route.get("fromAsset") or "").strip().upper()
                quote_asset = str(route.get("toAsset") or "").strip().upper()
            else:
                quote_asset = BinanceClient.get_spot_quote_asset(symbol)
                base_asset = BinanceClient.get_spot_base_asset(symbol)
        except Exception as exc:
            logger.error("到账检测前解析交易对失败: %s", exc)
            return False
        spot_base_to_quote_only = bool(spot_base_to_quote_only and trade_type == TRADE_ACCOUNT_TYPE_SPOT)
        spot_pair_balance_wait = trade_type == TRADE_ACCOUNT_TYPE_SPOT
        wait_asset = "%s/%s" % (base_asset, quote_asset) if spot_pair_balance_wait else quote_asset
        required_amount = Decimal(str(required_quote_amount)) if required_quote_amount is not None else Decimal("0")
        if trade_type == TRADE_ACCOUNT_TYPE_FUTURES:
            clear_fn = getattr(c, "ensure_um_futures_asset_clear_before_trade", None)
            if callable(clear_fn):
                clear_fn(quote_asset)
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
                            "%s 到账检测中，当前 U本位可用 %s = %s，目标至少 %s",
                            quote_asset,
                            quote_asset,
                            self._format_amount(quote_balance_dec),
                            self._format_amount(required_amount),
                        )
                    else:
                        log_progress_throttled(
                            "futures-balance",
                            "%s 到账检测中，当前 U本位可用 %s = %s",
                            quote_asset,
                            quote_asset,
                            self._format_amount(quote_balance_dec),
                        )
                else:
                    collect_trade_asset_to_spot(quote_asset)
                    quote_balance_dec = c.spot_asset_balance_decimal(quote_asset)
                    if spot_pair_balance_wait and base_asset:
                        collect_trade_asset_to_spot(base_asset)
                        base_balance_dec = c.spot_asset_balance_decimal(base_asset)
                    quote_balance = float(quote_balance_dec)
                    if spot_base_to_quote_only:
                        log_progress_throttled(
                            "spot-balance-zero-round",
                            "0轮兑换余额检测中，当前现货 %s = %s，%s = %s",
                            quote_asset,
                            self._format_amount(quote_balance_dec),
                            base_asset,
                            self._format_amount(base_balance_dec),
                        )
                    elif spot_pair_balance_wait:
                        log_progress_throttled(
                            "spot-pair-balance",
                            "%s模式余额检测中，当前现货 %s = %s，%s = %s",
                            mode_name,
                            quote_asset,
                            self._format_amount(quote_balance_dec),
                            base_asset,
                            self._format_amount(base_balance_dec),
                        )
                    else:
                        log_progress_throttled(
                            "spot-balance",
                            "%s 到账检测中，当前现货 %s = %s",
                            quote_asset,
                            quote_asset,
                            self._format_amount(quote_balance_dec),
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
            if spot_pair_balance_wait:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            elif trade_type == TRADE_ACCOUNT_TYPE_FUTURES and required_amount > 0:
                balance_ready = Decimal(str(quote_balance)) >= required_amount

            if balance_ready:
                if spot_pair_balance_wait:
                    logger.info("检测到交易对任一方可用余额，开始执行后续策略")
                else:
                    logger.info("检测到 %s 已到账，开始执行后续策略", quote_asset)
                return True

            delay_seconds = min(self._current_random_delay_seconds(), max(0.0, timeout_sec - (time.time() - start)))
            if delay_seconds <= 0:
                continue
            if spot_pair_balance_wait:
                log_progress_throttled("retry-spot-pair", "未检测到交易对任一方可用余额，%.3f 秒后重试", delay_seconds)
            else:
                log_progress_throttled("retry-balance", "%s 未到账，%.3f 秒后重试", quote_asset, delay_seconds)
            if stop_event:
                if stop_event.wait(delay_seconds):
                    logger.info("检测 %s 等待期间收到停止信号，结束检测", wait_asset)
                    return False
            else:
                time.sleep(delay_seconds)

        if spot_pair_balance_wait:
            logger.error("在 %d 秒内未检测到交易对任一方可用余额，终止任务", timeout_sec)
            return False
        logger.error("在 %d 秒内未检测到 %s 到账，终止任务", timeout_sec, wait_asset)
        return False
    def start_bot(self):
        if self.trade_account_type_var.get() == STOCK_TYPE:
            return self.start_stock_tasks()
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
            market_stop_mode = str(trade_settings.get("market_stop_mode", MARKET_STOP_MODE_DEFAULT))
            premium_delta_value = trade_settings["premium_delta_value"]
            premium_order_count = int(trade_settings["premium_order_count"])
            premium_append_threshold_value = trade_settings["premium_append_threshold_value"]
            premium_partial_fill = bool(trade_settings["premium_partial_fill"])
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_value = trade_settings["reprice_threshold_value"]
            ladder_layout = str(trade_settings["ladder_layout"])
            ladder_min_pool_notional_value = trade_settings["ladder_min_pool_notional_value"]
            ladder_sell_premium_enabled = bool(trade_settings["ladder_sell_premium_enabled"])
            ladder_sell_premium_value = trade_settings["ladder_sell_premium_value"]
            futures_symbol = str(trade_settings["futures_symbol"])
            futures_rounds = int(trade_settings["futures_rounds"])
            futures_amount_value = trade_settings["futures_amount_value"]
            futures_leverage = int(trade_settings["futures_leverage"])
            futures_margin_type = str(trade_settings["futures_margin_type"])
            futures_side = str(trade_settings["futures_side"])
            earn_asset = str(trade_settings["earn_asset"])
            earn_amount_value = trade_settings["earn_amount_value"]
            earn_amount_all = bool(trade_settings.get("earn_amount_all"))
            earn_auto_subscribe = bool(trade_settings.get("earn_auto_subscribe"))
            earn_auto_subscribe_only = bool(trade_settings.get("earn_auto_subscribe_only"))
            withdraw_buffer = finite_float(self.withdraw_buffer_var.get(), "提现预留", maximum=None)
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = bounded_int(self.usdt_timeout_var.get(), "到账超时", minimum=0, maximum=86400)
            rate_limit_wait_seconds = self._rate_limit_wait_seconds_value()
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

        spot_symbol = BinanceClient.normalize_trade_symbol(self.spot_symbol_var.get())
        market_bnb_stop_mode = (
            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and trade_mode == TRADE_MODE_MARKET
            and market_stop_mode == MARKET_STOP_MODE_BNB
        )
        trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_address = self.withdraw_addr_var.get().strip()
        withdraw_network = self.withdraw_net_var.get().strip()
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        if trade_account_type != TRADE_ACCOUNT_TYPE_EARN and enable_withdraw and (not withdraw_address or not withdraw_network or not withdraw_coin):
            messagebox.showerror("错误", "开启自动提现时，请填写 提现地址 / 网络 / 币种")
            return

        client = None
        quote_asset = ""
        validated_earn_product_id = ""
        effective_reprice_threshold = None
        runtime_stop_event = threading.Event()
        request_gate = SharedRateLimitGate(rate_limit_wait_seconds, stop_event=runtime_stop_event)
        try:
            client = self._create_binance_client(
                key,
                secret,
                request_gate=request_gate,
                request_stop_event=runtime_stop_event,
            )
            if trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
                earn_product = client.find_simple_earn_flexible_product(
                    earn_asset,
                    None if earn_amount_all or earn_auto_subscribe_only else earn_amount_value,
                )
                validated_earn_product_id = str(earn_product.get("productId") or "").strip()
            else:
                quote_asset = self._ensure_trade_symbol_supported(
                    client,
                    trade_account_type,
                    trade_mode,
                    spot_symbol,
                    futures_symbol,
                    spot_rounds,
                )
            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode in {
                TRADE_MODE_LIMIT,
                TRADE_MODE_PREMIUM,
                TRADE_MODE_LADDER,
                TRADE_MODE_AUTO_LADDER,
            }:
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
            if trade_account_type != TRADE_ACCOUNT_TYPE_EARN:
                self._assert_account_cleanup_finished(client, key)
        except Exception as e:
            self._close_binance_client_instance(client)
            messagebox.showerror("错误", "Binance 连接初始化失败: %s" % e)
            return
        required_quote_amount = None
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
            required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))
        self._replace_current_binance_client(client)

        logger.info("交易所单账号链路：%s", self._exchange_proxy_route_text())
        logger.info("单账号 API 限流等待已启用：%d 秒", rate_limit_wait_seconds)
        self.stop_event = runtime_stop_event
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
        elif trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
            total_steps = 1
        else:
            total_steps = (
                1
                if market_bnb_stop_mode
                else (max(spot_rounds, 1) if trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_CONVERT} else 1)
            )
        self.progress["maximum"] = total_steps

        def sleep_fn():
            if self.stop_event and self.stop_event.is_set():
                return
            self.random_sleep(min_delay, max_delay)

        def withdraw_callback(amount, idx=1, api_key=key, address=withdraw_address):
            if amount > 0:
                self.record_withdraw(idx, api_key, address, amount)

        if trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
            def earn_worker():
                failure_text = ""
                try:
                    asset_u = str(earn_asset or "").strip().upper()
                    action_text = "开启自动申购" if earn_auto_subscribe_only else "理财存入"
                    if not self._wait_batch_random_delay(
                        self.stop_event,
                        min_delay,
                        max_delay,
                        log_prefix=f"单账号{action_text}前",
                    ):
                        return
                    product_id = validated_earn_product_id
                    if earn_auto_subscribe_only:
                        self._dispatch_ui(lambda: self.status_var.set(f"状态：开启 {asset_u} 自动申购..."))
                        client.set_simple_earn_flexible_auto_subscribe(product_id, True)
                        logger.info("单账号已开启活期理财自动申购：资产=%s，productId=%s", asset_u, product_id)
                        done_text = f"已开启 {asset_u} 自动申购"
                    else:
                        self._dispatch_ui(lambda: self.status_var.set(f"状态：存入理财 {asset_u}..."))
                        amount_dec = self._resolve_earn_subscribe_amount(
                            client,
                            asset_u,
                            earn_amount_value,
                            earn_amount_all,
                        )
                        if earn_amount_all or not product_id:
                            earn_product = client.find_simple_earn_flexible_product(asset_u, amount_dec)
                            product_id = str(earn_product.get("productId") or "").strip()
                        result = client.subscribe_simple_earn_flexible_product(
                            product_id,
                            amount_dec,
                            source_account="SPOT",
                            auto_subscribe=earn_auto_subscribe,
                        )
                        amount_text = self._format_amount(amount_dec)
                        purchase_id = str((result or {}).get("purchaseId") or "").strip() if isinstance(result, dict) else ""
                        logger.info(
                            "单账号活期理财存入已提交：资产=%s，数量=%s，自动申购=%s，productId=%s，purchaseId=%s",
                            asset_u,
                            amount_text,
                            "开启" if earn_auto_subscribe else "关闭",
                            product_id,
                            purchase_id or "--",
                        )
                        done_text = f"理财存入 {amount_text} {asset_u}"

                    def _done():
                        self.progress["maximum"] = 1
                        self.progress["value"] = 1
                        self.status_var.set(f"状态：{done_text}")

                    self._dispatch_ui(_done)
                except Exception as e:
                    failure_text = str(e)
                    logger.error("理财存入过程中出现异常: %s", e)
                finally:
                    self._dispatch_ui(lambda error=failure_text: self._on_worker_finished(error))

            self.worker_thread = self._start_managed_thread(earn_worker, name="exchange-single-earn-subscribe")
            return

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
            market_stop_mode=market_stop_mode,
            premium_delta=premium_delta_value,
            premium_order_count=premium_order_count,
            premium_append_threshold=premium_append_threshold_value,
            premium_partial_fill=premium_partial_fill,
            bnb_fee_stop_value=bnb_fee_stop_value,
            bnb_topup_amount=bnb_topup_amount_value,
            reprice_threshold_amount=reprice_threshold_value,
            ladder_layout=ladder_layout,
            ladder_min_pool_notional=ladder_min_pool_notional_value,
            ladder_sell_premium_enabled=ladder_sell_premium_enabled,
            ladder_sell_premium=ladder_sell_premium_value,
            auto_ladder_settings=self.auto_ladder_settings_store if trade_mode == TRADE_MODE_AUTO_LADDER else None,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            futures_amount=futures_amount_value,
            futures_leverage=futures_leverage,
            futures_margin_type=futures_margin_type,
            futures_side=futures_side,
            operation_journal=self.exchange_operation_journal,
            operation_scope=self._exchange_operation_scope(key),
            cleanup_callback=getattr(self, "_record_strategy_cleanup", None),
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self._dispatch_ui(_update)

        def worker():
            failure_text = ""
            completion_status = ""
            try:
                zero_round_spot_conversion = (
                    trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
                    and not market_bnb_stop_mode
                    and spot_rounds == 0
                )
                if zero_round_spot_conversion and trade_mode == TRADE_MODE_MARKET:
                    wait_asset = client.resolve_spot_market_conversion(spot_symbol)["fromAsset"]
                elif zero_round_spot_conversion:
                    wait_asset = BinanceClient.get_spot_base_asset(spot_symbol)
                else:
                    wait_asset = quote_asset
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
                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
                        logger.info("交易对任一方余额检测未通过，任务结束")
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
                elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and not market_bnb_stop_mode and spot_rounds == 0:
                    logger.info("开始执行策略：0轮现货兑换，交易对=%s", spot_symbol)
                elif trade_mode == TRADE_MODE_MARKET:
                    if market_bnb_stop_mode:
                        logger.info(
                            "开始执行策略：市价按剩余BNB停止，停止值=%s BNB，预买BNB金额=%s",
                            BinanceClient._format_decimal(bnb_fee_stop_value),
                            bnb_topup_amount_value,
                        )
                    else:
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
                elif trade_mode in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
                    logger.info(
                        "开始执行策略：分仓挂单，档位模式=%s，最小分仓成交额=%s %s，预买BNB金额=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                        ladder_layout,
                        BinanceClient._format_decimal(ladder_min_pool_notional_value),
                        quote_asset,
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
                strategy_result, trade_stats_text = self._run_strategy_with_spot_statistics(
                    strategy, client, self.stop_event, progress_cb,
                    trade_account_type=trade_account_type,
                    trade_mode=trade_mode,
                    zero_round_spot_conversion=zero_round_spot_conversion,
                    spot_symbol=spot_symbol,
                    quote_asset=quote_asset,
                )
                result = strategy_result if isinstance(strategy_result, dict) else {}
                failure_text = self._strategy_failure_text(result, stopped=self.stop_event.is_set(), trade_stats_text=trade_stats_text)
                if not failure_text and trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and not self.stop_event.is_set():
                    completion_status, _ = self._format_batch_strategy_completion_status(
                        float(result.get("withdraw_amount") or 0), withdraw_coin,
                        enable_withdraw=enable_withdraw, strategy_result=result,
                        completion_text=self._spot_strategy_completion_text(trade_mode, spot_rounds, result),
                        trade_stats_text=trade_stats_text,
                    )
            except Exception as e:
                failure_text = str(e)
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self._dispatch_ui(lambda error=failure_text, status=completion_status: self._on_worker_finished(error, status))

        self.worker_thread = self._start_managed_thread(worker, name="exchange-single-run")
    def _restore_batch_ui_to_idle(self, *, status_text: str, finish_summary: bool) -> None:
        self._batch_task_active = False
        self._clear_account_batch_runtime()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="normal" if getattr(self, "_pending_cleanup_snapshot", lambda: [])() else "disabled")
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
    @classmethod
    def _strategy_failure_text(cls, result: dict, *, stopped: bool = False, trade_stats_text: str = "") -> str:
        cleanup_error = str(result.get("cleanup_error") or result.get("ladder_cleanup_error") or "").strip()
        if cleanup_error:
            return f"停止清理失败：{cleanup_error}"
        if stopped:
            return ""
        if result.get("trade_error"):
            return cls._format_trade_failure_status(result.get("trade_error_stage", ""), result["trade_error"])
        if result.get("withdraw_error") and not result.get("withdraw_amount"):
            text = f"提现失败：{result['withdraw_error']}"
            return f"{text}｜{trade_stats_text}" if trade_stats_text else text
        return ""

    def _on_worker_finished(self, failure_text: str = "", completion_status: str = ""):
        status_text = f"状态：{completion_status}" if completion_status else "状态：已完成/已停止"
        if failure_text:
            status_text = f"状态：失败：{failure_text[:80]}"
        self._restore_batch_ui_to_idle(status_text=status_text, finish_summary=True)
        if self.api_key_var.get().strip() and self.api_secret_var.get().strip():
            self.refresh_balances(silent=True)
    def stop_bot(self):
        if self.stop_event:
            self.stop_event.set()
            logger.info("已请求停止任务")
        retry_cleanup = getattr(self, "_retry_pending_cleanups", None)
        if callable(retry_cleanup):
            for strategy in self._pending_cleanup_snapshot():
                strategy.stop_cleanup_requested = True
            retry_cleanup()
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
            buffer_val = finite_float(self.withdraw_buffer_var.get(), "提现预留", maximum=None)
        except (ValueError, RuntimeError) as exc:
            messagebox.showerror("错误", str(exc) or "提现预留格式不正确")
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
        if tree.identify_column(event.x) == "#6":
            return "break"
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
    def run_context_account_monthly_trade_volume(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_query_monthly_trade_volume(accounts_to_run=[acc])
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
    def run_context_account_enable_earn_auto_subscribe(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        try:
            trade_settings = self._collect_trade_mode_settings(
                trade_account_type_override=TRADE_ACCOUNT_TYPE_EARN,
                earn_amount_override="0",
                earn_amount_all_override=False,
                earn_auto_subscribe_override=True,
            )
        except RuntimeError as e:
            messagebox.showerror("错误", str(e))
            return
        self.run_selected_accounts(
            accounts_to_run=[acc],
            trade_settings_override=trade_settings,
            require_confirm=True,
        )
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
    def _format_amount(value, precision: int = 5) -> str:
        return decimal_to_stat_text(value, precision=precision)
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
        return " | ".join(f"{asset} {cls._format_amount(amount_dec)}" for asset, amount_dec in items)

    @staticmethod
    def _merge_monthly_trade_volume_summaries(summaries: list[dict[str, object]]) -> dict[str, object]:
        quote_volumes: dict[str, Decimal] = {}
        commissions: dict[str, Decimal] = {}
        unvalued_quote_volumes: dict[str, Decimal] = {}
        unvalued_commissions: dict[str, Decimal] = {}
        result: dict[str, object] = {
            "quoteVolumes": quote_volumes,
            "commissionByAsset": commissions,
            "unvaluedQuoteVolumeByAsset": unvalued_quote_volumes,
            "unvaluedCommissionByAsset": unvalued_commissions,
            "usdtQuoteVolume": Decimal("0"),
            "usdtBuyQuoteVolume": Decimal("0"),
            "usdtSellQuoteVolume": Decimal("0"),
            "usdtCommission": Decimal("0"),
            "tradeCount": 0,
            "orderCount": 0,
            "symbolCount": len(summaries),
        }

        def merge_breakdown(target: dict[str, Decimal], source) -> None:
            for asset, amount in dict(source or {}).items():
                asset_u = str(asset or "").strip().upper()
                if asset_u:
                    target[asset_u] = target.get(asset_u, Decimal("0")) + Decimal(str(amount or "0"))

        for summary in summaries:
            quote_asset = str(summary.get("quoteAsset") or "").strip().upper()
            if quote_asset:
                quote_volumes[quote_asset] = quote_volumes.get(quote_asset, Decimal("0")) + Decimal(
                    str(summary.get("quoteVolume") or "0")
                )
            merge_breakdown(commissions, summary.get("commissionByAsset"))
            merge_breakdown(unvalued_quote_volumes, summary.get("unvaluedQuoteVolumeByAsset"))
            merge_breakdown(unvalued_commissions, summary.get("unvaluedCommissionByAsset"))
            for key in (
                "usdtQuoteVolume",
                "usdtBuyQuoteVolume",
                "usdtSellQuoteVolume",
                "usdtCommission",
            ):
                result[key] = Decimal(str(result[key])) + Decimal(str(summary.get(key) or "0"))
            result["tradeCount"] = int(result["tradeCount"]) + int(summary.get("tradeCount") or 0)
            result["orderCount"] = int(result["orderCount"]) + int(summary.get("orderCount") or 0)

        result["usdtOrderPnl"] = Decimal(str(result["usdtSellQuoteVolume"])) - Decimal(
            str(result["usdtBuyQuoteVolume"])
        )
        result["usdtNetResult"] = Decimal(str(result["usdtOrderPnl"])) - Decimal(
            str(result["usdtCommission"])
        )
        return result
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
    def _format_spot_run_trade_statistics(
        cls,
        symbol: str,
        summary: dict[str, object] | None,
        *,
        unavailable: bool = False,
    ) -> str:
        symbol_u = BinanceClient.normalize_trade_symbol(symbol)
        if not isinstance(summary, dict):
            return f"{symbol_u} 交易统计暂不可用" if unavailable and symbol_u else ""
        quote_asset = str(summary.get("quoteAsset") or "").strip().upper()
        try:
            volume = Decimal(str(summary["quoteVolume"]))
            pnl = Decimal(str(summary["orderPnl"]))
            if not volume.is_finite() or not pnl.is_finite() or volume < 0:
                raise ValueError("无效的交易统计")
        except Exception:
            return f"{symbol_u} 交易统计暂不可用"
        pnl_text = cls._format_amount(pnl)
        if pnl > 0:
            pnl_text = f"+{pnl_text}"
        asset_suffix = f" {quote_asset}" if quote_asset else ""
        symbol_prefix = f"{symbol_u} " if symbol_u else ""
        return (
            f"{symbol_prefix}交易量≈{cls._format_amount(volume)}{asset_suffix}｜"
            f"买卖收支差≈{pnl_text}{asset_suffix}"
        )
    def _run_strategy_with_spot_statistics(
        self, strategy, client, stop_event, progress_cb, *,
        trade_account_type: str, trade_mode: str,
        zero_round_spot_conversion: bool, spot_symbol: str, quote_asset: str,
    ):
        collect_stats = (
            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and not zero_round_spot_conversion
            and trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM, TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}
        )
        started_at_ms = client.current_server_time_ms() if collect_stats else None
        result = strategy.run(stop_event, progress_cb=progress_cb)
        if (
            not collect_stats
            or (isinstance(result, dict) and result.get("trade_error"))
            or stop_event.is_set()
        ):
            return result, ""
        try:
            summary = client.spot_trade_volume_summary(
                spot_symbol,
                start_time_ms=started_at_ms,
                end_time_ms=client.current_server_time_ms(),
                stop_event=stop_event,
                quote_asset=quote_asset,
            )
            stats = self._format_spot_run_trade_statistics(spot_symbol, summary, unavailable=True)
            logger.info("本次现货交易统计：%s", stats)
        except Exception as exc:
            stats = self._format_spot_run_trade_statistics(spot_symbol, None, unavailable=True)
            logger.warning("本次现货交易统计失败，不影响交易结果：%s", exc)
        return result, stats
    @staticmethod
    def _spot_strategy_completion_text(
        trade_mode: str,
        spot_rounds: int,
        strategy_result: dict | None = None,
    ) -> str:
        mode = str(trade_mode or "").strip()
        if mode in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
            return "分仓挂单结束"
        if mode == TRADE_MODE_LIMIT:
            return "挂单结束"
        if mode == TRADE_MODE_PREMIUM:
            return "溢价挂单结束"
        if mode == TRADE_MODE_CONVERT:
            return f"闪兑{max(0, int(spot_rounds or 0))}轮结束"
        if mode == TRADE_MODE_MARKET:
            completed = 0
            if isinstance(strategy_result, dict):
                try:
                    completed = max(0, int(strategy_result.get("market_round_completed") or 0))
                except Exception:
                    completed = 0
            completed = completed or max(0, int(spot_rounds or 0))
            return f"市价{completed}轮结束" if completed > 0 else "市价交易结束"
        return "现货交易结束"
    @classmethod
    def _format_market_round_failure_status(cls, strategy_result: dict | None) -> str:
        if not isinstance(strategy_result, dict):
            return ""
        try:
            failed_count = max(0, int(strategy_result.get("market_round_failed_count") or 0))
        except Exception:
            failed_count = 0
        if failed_count <= 0:
            return ""
        try:
            completed = max(0, int(strategy_result.get("market_round_completed") or 0))
        except Exception:
            completed = 0
        try:
            total = max(0, int(strategy_result.get("market_round_total") or 0))
        except Exception:
            total = 0
        finished = total or completed
        if finished > 0:
            return f"{finished}轮完成，失败{failed_count}轮"
        return f"失败{failed_count}轮"
    @classmethod
    def _format_batch_strategy_completion_status(
        cls,
        amount: float,
        coin: str,
        *,
        enable_withdraw: bool,
        strategy_result: dict | None = None,
        completion_text: str = "",
        trade_stats_text: str = "",
    ) -> tuple[str | None, bool]:
        round_status = cls._format_market_round_failure_status(strategy_result)
        completion = str(completion_text or "").strip()
        stats = str(trade_stats_text or "").strip()
        if not enable_withdraw:
            body = completion or round_status
            if completion and round_status:
                try:
                    failed_count = max(0, int((strategy_result or {}).get("market_round_failed_count") or 0))
                except Exception:
                    failed_count = 0
                if failed_count > 0:
                    body = f"{completion}（失败{failed_count}轮）"
            if stats:
                body = f"{body}｜{stats}" if body else stats
            if body:
                return cls._exchange_success_status_text(body), True
            return None, True
        withdraw_status, success = cls._format_withdraw_amount_status(amount, coin, enable_withdraw=enable_withdraw)
        body = cls._plain_exchange_status_text(withdraw_status)
        if stats:
            body = f"{body}｜{stats}"
        if round_status:
            body = f"{body}｜{round_status}"
        prefix = cls._exchange_success_status_text if success else cls._exchange_failed_status_text
        return prefix(body), success
    @classmethod
    def _format_missing_withdraw_address_status(
        cls,
        strategy_result: dict | None = None,
        trade_stats_text: str = "",
    ) -> str:
        skip_text = "未填写提现地址，已跳过提现"
        round_status = cls._format_market_round_failure_status(strategy_result)
        parts = [skip_text]
        if str(trade_stats_text or "").strip():
            parts.append(str(trade_stats_text).strip())
        if round_status:
            parts.append(round_status)
        skip_text = "｜".join(parts)
        return cls._exchange_success_status_text(skip_text)
    @classmethod
    def _format_trade_failure_status(cls, stage: str, err: object) -> str:
        stage_text = str(stage or "").strip()
        if stage_text == "开仓下单":
            prefix = "开仓失败"
        elif stage_text == "平仓下单":
            prefix = "平仓失败"
        elif stage_text == "合约有问题":
            prefix = "合约有问题"
        elif stage_text == "分仓挂单结束清理":
            prefix = "结束清理失败"
        elif "预检" in stage_text:
            prefix = "合约预检失败"
        else:
            prefix = "交易失败"
        return cls._exchange_error_status_text(prefix, err, fallback=prefix)
    @staticmethod
    def _collect_bnb_withdraw_runtime() -> tuple[str, bool]:
        return "BNB", True
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
    def _collect_bnb_spot_residual_usdt(client, residual_assets: list[str]) -> Decimal:
        balances = {
            str((item or {}).get("asset") or "").strip().upper(): Decimal(
                str((item or {}).get("free") or "0")
            )
            for item in client.spot_positive_assets()
        }
        total_usdt = Decimal("0")
        for asset in residual_assets:
            asset_u = str(asset or "").strip().upper()
            amount = balances.get(asset_u, Decimal("0"))
            if amount <= 0:
                continue
            if asset_u == "USDT":
                total_usdt += amount
                continue
            price = client.get_symbol_price(f"{asset_u}USDT")
            if price is None:
                raise RuntimeError(f"无法获取 {asset_u}/USDT 价格")
            total_usdt += amount * Decimal(str(price))
        return total_usdt
    @staticmethod
    def _collect_bnb_simple_earn_assets(client) -> list[str]:
        if not hasattr(client, "get_simple_earn_flexible_positions"):
            return []
        assets: list[str] = []
        for item in client.get_simple_earn_flexible_positions():
            item_asset = str((item or {}).get("asset") or "").strip().upper()
            if not item_asset:
                continue
            has_amount = False
            for field_name in ("totalAmount", "redeemingAmount"):
                try:
                    amount = Decimal(str((item or {}).get(field_name) or "0"))
                except Exception:
                    amount = Decimal("0")
                if amount > 0:
                    has_amount = True
                    break
            if has_amount and item_asset not in assets:
                assets.append(item_asset)
        return assets
    @staticmethod
    def _collect_bnb_simple_earn_balance_assets(earn_assets: list[str]) -> list[str]:
        balance_assets: list[str] = []
        for asset in earn_assets:
            asset_u = str(asset or "").strip().upper()
            balance_asset = f"LD{asset_u}" if asset_u else ""
            if balance_asset and balance_asset not in balance_assets:
                balance_assets.append(balance_asset)
        return balance_assets
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
    @classmethod
    def _format_collect_bnb_earn_status(
        cls,
        amount: float,
        coin: str,
        earn_assets: list[str] | None = None,
    ) -> tuple[str, bool]:
        status_text, withdraw_success = cls._format_withdraw_amount_status(
            amount,
            coin,
            enable_withdraw=True,
        )
        if not earn_assets:
            return status_text, withdraw_success
        if withdraw_success:
            return cls._exchange_success_status_text(
                f"{cls._plain_exchange_status_text(status_text)}；跳过理财"
            ), True
        # 理财已明确排除时，显示提现失败，但不把账号放回失败重试队列。
        return cls._exchange_failed_status_text("跳过理财；提现失败：提现额度不足"), True
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
    def _exchange_error_status_text(prefix: str, err: object, *, fallback: str = "异常") -> str:
        prefix_text = str(prefix or "").strip() or str(fallback or "异常").strip() or "异常"
        reason_text = summarize_exchange_exception(err, fallback=fallback, max_len=72)
        if not reason_text:
            return ExchangeAppBatchMixin._exchange_failed_status_text(prefix_text, fallback=fallback)
        return ExchangeAppBatchMixin._exchange_failed_status_text(f"{prefix_text}: {reason_text}", fallback=fallback)
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
            amount_text = cls._format_amount(amount)
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
        if runner == "batch_subscribe_flexible_earn":
            earn_subscribe_text = self._format_batch_metric_totals(data.get("earn_subscribe_by_account") or {})
            return (
                f"结果汇总：成功{success_count} | 失败{failed_count} | "
                f"理财存入总额={earn_subscribe_text}"
            )
        if runner == "batch_enable_flexible_earn_auto_subscribe":
            auto_enabled_count = self._sum_batch_count_metrics(data.get("auto_enabled_by_account") or {})
            return f"结果汇总：成功{success_count} | 失败{failed_count} | 开启自动申购{auto_enabled_count}"
        withdraw_text = self._format_batch_metric_totals(data.get("withdraw_by_account") or {})
        balance_text = self._format_batch_metric_totals(data.get("balance_by_account") or {})
        text = f"结果汇总：成功{success_count} | 失败{failed_count} | 提现总额={withdraw_text} | 余额总额={balance_text}"
        market_failed = self._sum_batch_count_metrics(data.get("market_failed_rounds_by_account") or {})
        if market_failed > 0:
            affected_accounts = 0
            for value in dict(data.get("market_failed_rounds_by_account") or {}).values():
                try:
                    if int(value or 0) > 0:
                        affected_accounts += 1
                except Exception:
                    continue
            text += f" | 现货市价失败轮次={market_failed}(账号{affected_accounts})"
        return text
    def _build_batch_result_extra_lines(self, summary: dict | None = None) -> list[str]:
        data = summary if isinstance(summary, dict) else {}
        market_failed = self._sum_batch_count_metrics(data.get("market_failed_rounds_by_account") or {})
        if market_failed <= 0:
            return []
        affected_accounts = 0
        for value in dict(data.get("market_failed_rounds_by_account") or {}).values():
            try:
                if int(value or 0) > 0:
                    affected_accounts += 1
            except Exception:
                continue
        return [f"现货市价失败轮次：{market_failed}（涉及账号 {affected_accounts} 个）"]
    def _set_batch_summary_text(self, text: str) -> None:
        var = getattr(self, "exchange_batch_summary_var", None)
        if var is not None:
            var.set(str(text or ""))
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
    def _record_batch_amount_metric(self, acc: dict, field_name: str, amount, asset: str) -> None:
        summary_text = ""
        field = str(field_name or "").strip()
        if not field:
            return
        with self._batch_summary_lock:
            summary = self._current_batch_summary
            if not summary:
                return
            key = self._account_batch_key(acc)
            try:
                amount_dec = Decimal(str(amount or "0"))
            except Exception:
                amount_dec = Decimal("0")
            summary.setdefault(field, {})[key] = (amount_dec, str(asset or "").strip().upper())
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
        checkpoint = getattr(self, "batch_checkpoint", None)
        if callable(checkpoint):
            checkpoint(acc, success)
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
        market_stop_mode: str = MARKET_STOP_MODE_DEFAULT,
        market_stagger_enabled: bool = MARKET_STAGGER_ENABLED_DEFAULT,
        bnb_fee_stop_value: Decimal | str | None = None,
        futures_symbol: str,
        futures_rounds: int,
        min_delay: int,
        max_delay: int,
        withdraw_coin: str,
        withdraw_network: str,
        enable_withdraw: bool,
        bnb_topup_amount: Decimal | None = None,
        ladder_layout: str = LADDER_LAYOUT_DEFAULT,
        ladder_min_pool_notional: Decimal | str | None = None,
        earn_asset: str = "",
        earn_amount: Decimal | str | None = None,
        earn_amount_all: bool = False,
        earn_auto_subscribe: bool = False,
        earn_auto_subscribe_only: bool = False,
    ) -> list[str]:
        lines = [f"批量账号数：{int(account_count)}"]
        lines.append(f"交易类型：{trade_account_type}")
        if trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
            lines.append(f"理财币种：{str(earn_asset or '').strip().upper() or '-'}")
            if earn_auto_subscribe_only:
                lines.append("执行内容：仅开启自动申购")
            else:
                amount_text = "全部" if earn_amount_all else BinanceClient._format_decimal(Decimal(str(earn_amount or "0")))
                lines.append(f"存入数量：{amount_text}")
            lines.append(f"自动申购：{'开启' if earn_auto_subscribe else '关闭'}")
        elif trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
            lines.append(f"交易对：{str(futures_symbol or '').strip().upper() or '-'}")
            lines.append(f"轮次：{int(futures_rounds)}")
        else:
            lines.append(f"交易模式：{trade_mode}")
            lines.append(f"交易对：{str(spot_symbol or '').strip().upper() or '-'}")
            if trade_mode == TRADE_MODE_MARKET and market_stop_mode == MARKET_STOP_MODE_BNB:
                lines.append(
                    "停止方式：剩余BNB低于 %s"
                    % BinanceClient._format_decimal(Decimal(str(bnb_fee_stop_value or "0")))
                )
            else:
                lines.append(f"轮次：{int(spot_rounds)}")
            if trade_mode == TRADE_MODE_MARKET:
                lines.append(f"市价交错执行：{'开启' if market_stagger_enabled else '关闭'}")
            if trade_mode in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
                lines.append(f"档位模式：{ladder_layout}")
                lines.append(
                    "最小分仓成交额：%s"
                    % BinanceClient._format_decimal(Decimal(str(ladder_min_pool_notional or "0")))
                )
            if trade_mode in {TRADE_MODE_MARKET, TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM, TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
                lines.append(
                    "预购买BNB金额：%s" % BinanceClient._format_decimal(Decimal(str(bnb_topup_amount or "0")))
                )
        lines.append(f"随机延迟：{int(min_delay)} - {int(max_delay)} 毫秒")
        if trade_account_type != TRADE_ACCOUNT_TYPE_EARN:
            lines.append(f"提现币种：{str(withdraw_coin or '').strip().upper() or WITHDRAW_COIN_DEFAULT}")
            lines.append(f"提现网络：{str(withdraw_network or '').strip().upper() or WITHDRAW_NETWORK_DEFAULT}")
            lines.append(f"自动提现：{'开启' if enable_withdraw else '关闭'}")
        return lines
    @staticmethod
    def _format_bnb_topup_account_count_text(account_count: int, topup_count: int) -> str:
        total = max(0, int(account_count))
        enabled = max(0, min(total, int(topup_count)))
        return f"预购：{enabled} 个账号；不预购：{total - enabled} 个账号。"
    def _show_batch_execute_confirm_dialog(
        self,
        *,
        account_count: int,
        trade_account_type: str,
        trade_mode: str,
        spot_symbol: str,
        spot_rounds: int,
        market_stop_mode: str = MARKET_STOP_MODE_DEFAULT,
        market_stagger_enabled: bool = MARKET_STAGGER_ENABLED_DEFAULT,
        bnb_fee_stop_value: Decimal | str | None = None,
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
        bnb_topup_account_count: int = 0,
        ladder_layout: str = LADDER_LAYOUT_DEFAULT,
        ladder_min_pool_notional: Decimal | str | None = None,
        earn_asset: str = "",
        earn_amount: Decimal | str | None = None,
        earn_amount_all: bool = False,
        earn_auto_subscribe: bool = False,
        earn_auto_subscribe_only: bool = False,
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
            market_stop_mode=market_stop_mode,
            market_stagger_enabled=market_stagger_enabled,
            bnb_fee_stop_value=bnb_fee_stop_value,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            min_delay=min_delay,
            max_delay=max_delay,
            withdraw_coin=withdraw_coin,
            withdraw_network=withdraw_network,
            enable_withdraw=enable_withdraw,
            bnb_topup_amount=bnb_topup_amount,
            ladder_layout=ladder_layout,
            ladder_min_pool_notional=ladder_min_pool_notional,
            earn_asset=earn_asset,
            earn_amount=earn_amount,
            earn_amount_all=earn_amount_all,
            earn_auto_subscribe=earn_auto_subscribe,
            earn_auto_subscribe_only=earn_auto_subscribe_only,
        ):
            ttk.Label(summary, text=line, foreground="#444444").pack(anchor="w", pady=(0, 2))

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=(12, 10))

        if show_bnb_topup_option and trade_account_type != TRADE_ACCOUNT_TYPE_EARN:
            tk.Checkbutton(
                body,
                text="对账号列表中已勾选的账号预购买BNB",
                variable=enable_bnb_topup_var,
                fg="#C62828",
                activeforeground="#C62828",
                selectcolor="#FFFFFF",
                anchor="w",
            ).pack(anchor="w")
            ttk.Label(
                body,
                text=self._format_bnb_topup_account_count_text(account_count, bnb_topup_account_count),
                foreground="#666666",
            ).pack(anchor="w", pady=(6, 0))

        if trade_account_type != TRADE_ACCOUNT_TYPE_EARN:
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
    @staticmethod
    def _trade_volume_query_settings_path():
        import exchange_app_base as base
        return base.DATA_FILE.parent / "trade_volume_query.json"

    def _load_trade_volume_query_settings(self):
        self.trade_volume_query_dates = {"start_date": "", "end_date": ""}
        self.trade_volume_query_error = ""
        path = self._trade_volume_query_settings_path()
        if not config_or_backup_exists(path):
            return
        try:
            raw, _ = _load_authenticated_config(path, validator=trade_volume_time_range)
            self.trade_volume_query_dates = {name: normalize_trade_volume_date(raw.get(name, ""))
                                            for name in ("start_date", "end_date")}
        except Exception as exc:
            self.trade_volume_query_error = "查询条件读取失败，请打开成交额设置重新保存"
            logger.warning("%s：%s", self.trade_volume_query_error, exc)

    def save_trade_volume_query_settings(self, start_date, end_date):
        dates = {"start_date": normalize_trade_volume_date(start_date),
                 "end_date": normalize_trade_volume_date(end_date)}
        trade_volume_time_range(dates)
        _save_authenticated_config(self._trade_volume_query_settings_path(), dates)
        self.trade_volume_query_dates = dates
        self.trade_volume_query_error = ""
        return dict(dates)

    def show_trade_volume_query_settings(self):
        dialog = tk.Toplevel(self)
        dialog.title("成交额查询设置")
        dialog.transient(self)
        dialog.resizable(False, False)
        body = ttk.Frame(dialog, padding=16)
        body.pack(fill="both", expand=True)
        saved = getattr(self, "trade_volume_query_dates", {})
        dates = [tk.StringVar(master=dialog, value=saved.get(name, ""))
                 for name in ("start_date", "end_date")]

        def format_date(var, complete=False):
            text = var.get().strip()
            if complete or (len(text) == 8 and text.isdigit()):
                try:
                    var.set(normalize_trade_volume_date(text))
                except RuntimeError:
                    pass

        for row, (text, var) in enumerate(zip(("开始日期", "结束日期"), dates)):
            ttk.Label(body, text=text).grid(row=row, column=0, sticky="w", pady=5)
            entry = ttk.Entry(body, textvariable=var, width=26)
            entry.grid(row=row, column=1, sticky="ew", padx=(10, 0))
            var.trace_add("write", lambda *_args, var=var: format_date(var))
            entry.bind("<FocusOut>", lambda _event, var=var: format_date(var, True))
        ttk.Label(body, text="均留空：近30天；只填一个：当天；填写两个：起止日期。\n"
                  "北京时间；可输入 20260902 或 2026-9-2，自动补零。",
                  foreground="#666666").grid(row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))

        def submit():
            try:
                self.save_trade_volume_query_settings(dates[0].get(), dates[1].get())
            except Exception as exc:
                messagebox.showerror("错误", str(exc), parent=dialog)
                return
            dialog.destroy()

        actions = ttk.Frame(body)
        actions.grid(row=3, column=0, columnspan=2, sticky="e", pady=(14, 0))
        ttk.Button(actions, text="保存", command=submit).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="取消", command=dialog.destroy).pack(side="left")
        dialog.bind("<Return>", lambda _event: submit())
        dialog.bind("<Escape>", lambda _event: dialog.destroy())
        dialog.grab_set()
        self.wait_window(dialog)

    def run_query_monthly_trade_volume(self, accounts_to_run=None, *, query_options=None):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return

        symbol = BinanceClient.normalize_trade_symbol(self.spot_symbol_var.get())
        if not symbol:
            messagebox.showerror("错误", "现货交易对不能为空")
            return
        try:
            if query_options is None:
                if getattr(self, "trade_volume_query_error", ""):
                    raise RuntimeError(self.trade_volume_query_error)
                query_options = dict(getattr(self, "trade_volume_query_dates", {}))
            start_time_ms, end_time_ms, period_label = trade_volume_time_range(query_options)
            base_asset = BinanceClient.get_spot_base_asset(symbol)
            max_threads = bounded_int(self.max_threads_var.get(), "线程数")
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "最小")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "最大")
        except Exception as exc:
            messagebox.showerror("错误", str(exc) or "交易对、线程数或随机延迟格式不正确")
            return

        query_label = "月成交额" if period_label == "近30天" else "成交额"
        summary_label = "月成交" if period_label == "近30天" else "成交"
        self.stop_event = threading.Event()
        request_gate = SharedRateLimitGate(10, stop_event=self.stop_event)
        self._batch_task_active = True
        self._prepare_accounts_for_batch(selected)
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
        self.status_var.set(f"状态：查询 {base_asset} 全部现货交易对{period_label}成交额...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for batch_pos, acc in enumerate(selected, start=1):
            task_queue.put((self._account_display_index(acc, batch_pos), acc))

        result_lock = threading.Lock()
        completed_count = 0
        account_results = []
        spot_symbol_infos: list[dict[str, str]] = []
        hourly_usdt_rates: dict[str, dict[int, Decimal]] = {}

        def format_pnl(value: Decimal) -> str:
            text = self._format_amount(value)
            return f"+{text}" if value > 0 else text

        def finish_one():
            nonlocal completed_count
            with result_lock:
                completed_count += 1
                current = completed_count
            self._dispatch_ui(lambda: self.progress.config(value=current))

        def valuation_price_lookup(asset: str, timestamp_ms: int) -> Decimal | None:
            return BinanceClient.historical_usdt_price_lookup(hourly_usdt_rates, asset, timestamp_ms)

        def needs_valuation(summary) -> bool:
            return bool(summary["unvaluedQuoteVolumeByAsset"] or summary["unvaluedCommissionByAsset"])

        def compact_summary_text(summary, *, include_total: bool = True) -> str:
            def amount(key, *, signed=False):
                value = Decimal(str(summary[key]))
                return format_pnl(value) if signed else self._format_amount(value)

            missing_volume = bool(summary["unvaluedQuoteVolumeByAsset"])
            missing_fee = bool(summary["unvaluedCommissionByAsset"])
            volume_mark = "≥" if missing_volume else "≈"
            fields = [
                f"{volume_mark}{amount('usdtQuoteVolume')} USDT",
                "买卖差待折算" if missing_volume else f"买卖差≈{amount('usdtOrderPnl', signed=True)}",
                "费待折算" if missing_fee else f"费≈{amount('usdtCommission')}",
            ]
            if include_total:
                fields.append(
                    "含费收支差待折算" if missing_volume or missing_fee
                    else f"含费收支差≈{amount('usdtNetResult', signed=True)}"
                )
            return "｜".join(fields)

        def detailed_summary_text(
            *,
            title: str,
            summary: dict[str, object],
            symbols_text: str,
        ) -> str:
            volume_text = self._format_amount(Decimal(str(summary["usdtQuoteVolume"])))
            buy_text = self._format_amount(Decimal(str(summary["usdtBuyQuoteVolume"])))
            sell_text = self._format_amount(Decimal(str(summary["usdtSellQuoteVolume"])))
            pnl_text = format_pnl(Decimal(str(summary["usdtOrderPnl"])))
            fee_text = self._format_amount(Decimal(str(summary["usdtCommission"])))
            net_text = format_pnl(Decimal(str(summary["usdtNetResult"])))
            native_volume_text = self._format_asset_breakdown_text(dict(summary["quoteVolumes"]))
            native_fee_text = self._format_asset_breakdown_text(dict(summary["commissionByAsset"]))
            missing_volume = bool(summary["unvaluedQuoteVolumeByAsset"])
            missing_fee = bool(summary["unvaluedCommissionByAsset"])
            volume_label = "已折算部分成交额" if missing_volume else "折合成交额"
            fee_label = "已折算部分" if missing_fee else "折合"
            lines = [
                title,
                f"交易对：{symbols_text}",
                f"原币种成交额：{native_volume_text}",
                f"{volume_label}≈{volume_text} USDT（买入≈{buy_text} / 卖出≈{sell_text}）",
                "买卖收支差：待折算" if missing_volume else f"买卖收支差≈{pnl_text} USDT",
                f"手续费：{native_fee_text}（{fee_label}≈{fee_text} USDT）",
                "含费收支差：待折算" if missing_volume or missing_fee else f"含费收支差≈{net_text} USDT",
                f"说明：这里是{period_label}卖出收入减买入支出及手续费，未计入期初/期末持币价值和闪兑，不等于实际盈亏。",
                f"订单/成交明细：{int(summary['orderCount'])}单/{int(summary['tradeCount'])}笔",
            ]
            unvalued_volume = self._format_asset_breakdown_text(dict(summary["unvaluedQuoteVolumeByAsset"]))
            unvalued_fee = self._format_asset_breakdown_text(dict(summary["unvaluedCommissionByAsset"]))
            if unvalued_volume != "--":
                lines.append(f"未折算成交额：{unvalued_volume}")
            if unvalued_fee != "--":
                lines.append(f"未折算手续费：{unvalued_fee}")
            return "\n".join(lines)

        def publish_account_result(result):
            idx, acc = result["idx"], result["account"]
            summaries = result["summaries"]
            stopped = result["stopped"]
            if not summaries:
                status = "已停止" if stopped else f"{query_label}查询失败（无完整交易对数据）"
                self._schedule_account_status(acc, status if stopped else self._exchange_failed_status_text(status))
                logger.info("账号 #%d %s：%s", idx, base_asset, status)
            else:
                summary = self._merge_monthly_trade_volume_summaries(summaries)
                partial = len(summaries) < len(spot_symbol_infos) or stopped
                label = f"部分{len(summaries)}/{len(spot_symbol_infos)}" if partial else f"{len(spot_symbol_infos)}对"
                if stopped:
                    label = "已停止 " + label
                body = f"{base_asset} {label}｜{compact_summary_text(summary)}"
                self._schedule_account_status(
                    acc, body if partial or needs_valuation(summary) else self._exchange_success_status_text(body),
                )
                title = f"账号 #{idx} {base_asset} {period_label}成交汇总"
                if partial:
                    title += f"（{label}，仅含已查部分）"
                logger.info("%s", detailed_summary_text(
                    title=title, summary=summary,
                    symbols_text="、".join(item["symbol"] for item in summaries),
                ))
            result["reported"] = True
            self._set_account_batch_active(acc, False)

        def query_worker(thread_id):
            first_account = True
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    clear_exchange_account_log_index()
                    return
                set_exchange_account_log_index(idx)
                client = None
                combined_stop = CombinedStopEvent(self.stop_event, self._get_account_stop_event(acc))
                result = {"idx": idx, "account": acc, "summaries": [], "stopped": False, "reported": False}
                try:
                    if not first_account:
                        self._schedule_account_status(acc, f"{base_asset} 查询前随机等待...")
                        if not self._wait_batch_random_delay(
                            combined_stop, min_delay, max_delay,
                            log_prefix=f"线程 {thread_id} 切换账号 #{idx} {query_label}查询前 ",
                        ):
                            raise InterruptedError("成交额查询已停止")
                    self._schedule_account_status(acc, f"查询 {base_asset} 全部交易对...")
                    client = self._create_binance_client(
                        acc["api_key"],
                        acc["api_secret"],
                        request_gate=request_gate,
                        request_stop_event=combined_stop,
                    )
                    for symbol_pos, info in enumerate(spot_symbol_infos, start=1):
                        if combined_stop.is_set():
                            raise InterruptedError("成交额查询已停止")
                        query_symbol = info["symbol"]
                        self._schedule_account_status(
                            acc,
                            f"查询 {base_asset} {symbol_pos}/{len(spot_symbol_infos)}...",
                        )
                        try:
                            result["summaries"].append(
                                client.spot_trade_volume_summary(
                                    query_symbol,
                                    start_time_ms=start_time_ms,
                                    end_time_ms=end_time_ms,
                                    stop_event=combined_stop,
                                    valuation_price_lookup=valuation_price_lookup,
                                    quote_asset=info["quoteAsset"],
                                )
                            )
                        except InterruptedError:
                            raise
                        except Exception as exc:
                            if combined_stop.is_set():
                                raise InterruptedError("成交额查询已停止") from exc
                            logger.error("账号 #%d 查询 %s 近30天成交额失败（其他交易对继续）: %s", idx, query_symbol, exc)
                except InterruptedError:
                    result["stopped"] = True
                except Exception as exc:
                    result["stopped"] = combined_stop.is_set()
                    if not result["stopped"]:
                        logger.error("账号 #%d 查询 %s 近30天成交额失败: %s", idx, symbol, exc)
                finally:
                    first_account = False
                    result["stopped"] = result["stopped"] or combined_stop.is_set()
                    with result_lock:
                        account_results.append(result)
                    if not result["stopped"] and any(needs_valuation(item) for item in result["summaries"]):
                        self._schedule_account_status(acc, f"{base_asset} 成交已查，等待补价...")
                    else:
                        publish_account_result(result)
                    self._close_binance_client_instance(client)
                    self._set_account_batch_active(acc, not result["reported"])
                    task_queue.task_done()
                    finish_one()
                    clear_exchange_account_log_index()

        def controller():
            nonlocal base_asset, spot_symbol_infos, hourly_usdt_rates
            bootstrap_client = None
            try:
                first_account = selected[0]
                bootstrap_client = self._create_binance_client(
                    first_account["api_key"],
                    first_account["api_secret"],
                    request_gate=request_gate,
                    request_stop_event=self.stop_event,
                )
                catalog = bootstrap_client.get_spot_symbol_catalog()
                base_asset, spot_symbol_infos = bootstrap_client.get_spot_symbols_for_base_asset(
                    symbol,
                    spot_symbols=catalog,
                )
                valuation_assets = {base_asset, "BNB"}
                valuation_assets.update(item["quoteAsset"] for item in spot_symbol_infos)
                hourly_usdt_rates = bootstrap_client.get_historical_usdt_hourly_rates(
                    valuation_assets,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    spot_symbols=catalog,
                    stop_event=self.stop_event,
                )
                logger.info(
                    "%s 成交额查询范围：%s，%d个现货交易对：%s",
                    base_asset,
                    period_label,
                    len(spot_symbol_infos),
                    "、".join(item["symbol"] for item in spot_symbol_infos),
                )
            except InterruptedError:
                for acc in selected:
                    self._schedule_account_status(acc, "已停止")
                    self._set_account_batch_active(acc, False)
                self._dispatch_ui(
                    lambda: self._restore_batch_ui_to_idle(status_text=f"状态：{query_label}查询已停止", finish_summary=False)
                )
                return
            except Exception as exc:
                if not self.stop_event.is_set():
                    logger.error("%s查询初始化失败: %s", query_label, exc)
                error_text = (
                    f"{query_label}查询已停止" if self.stop_event.is_set()
                    else self._exchange_error_status_text(f"{query_label}查询失败", exc, fallback="查询失败")
                )
                for acc in selected:
                    self._schedule_account_status(acc, error_text)
                    self._set_account_batch_active(acc, False)
                self._dispatch_ui(
                    lambda text=error_text: self._restore_batch_ui_to_idle(
                        status_text=f"状态：{text}",
                        finish_summary=False,
                    )
                )
                return
            finally:
                self._close_binance_client_instance(bootstrap_client)

            threads = []
            for thread_id in range(min(max_threads, len(selected))):
                thread = threading.Thread(target=query_worker, args=(thread_id + 1,), daemon=True)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()

            # 只补查实际成交缺失的小时价格；同币种所有账号合并为一次区间请求。
            missing_hours: dict[str, set[int]] = {}
            for result in account_results:
                if result["reported"]:
                    continue
                result["stopped"] = result["stopped"] or self._get_account_stop_event(result["account"]).is_set()
                if result["stopped"]:
                    continue
                for item in result["summaries"]:
                    for asset, hour in item.get("unvaluedHourlyAmounts", {}):
                        missing_hours.setdefault(asset, set()).add(hour)
            repair_client = None
            if missing_hours and not self.stop_event.is_set():
                self._dispatch_ui(lambda: self.status_var.set("状态：成交查询结束，补查缺失历史价格..."))
                try:
                    repair_client = self._create_binance_client(
                        selected[0]["api_key"], selected[0]["api_secret"],
                        request_gate=request_gate, request_stop_event=self.stop_event,
                    )
                    for asset, hours in sorted(missing_hours.items()):
                        if self.stop_event.is_set():
                            break
                        repaired = repair_client.get_historical_usdt_hourly_rates(
                            {asset}, start_time_ms=min(hours),
                            end_time_ms=min(end_time_ms, max(hours) + 3_600_000 - 1),
                            spot_symbols=catalog, stop_event=self.stop_event, max_retries=1,
                        )
                        for hour, price in repaired.get(asset, {}).items():
                            if hour in hours:
                                hourly_usdt_rates.setdefault(asset, {}).setdefault(hour, price)
                except InterruptedError:
                    pass
                except Exception as exc:
                    if not self.stop_event.is_set():
                        logger.warning("补查历史价格失败，保留已查询成交数据: %s", exc)
                finally:
                    self._close_binance_client_instance(repair_client)

            completed_summaries = []
            success = 0
            for result in account_results:
                if not result["reported"]:
                    for item in result["summaries"]:
                        BinanceClient.revalue_spot_trade_summary(item, hourly_usdt_rates)
                    result["stopped"] = (
                        result["stopped"] or self.stop_event.is_set()
                        or self._get_account_stop_event(result["account"]).is_set()
                    )
                    publish_account_result(result)
                complete = (
                    not result["stopped"] and len(result["summaries"]) == len(spot_symbol_infos)
                    and not any(needs_valuation(item) for item in result["summaries"])
                )
                success += int(complete)
                completed_summaries.extend(result["summaries"])

            # 尚未开始的账号也要结束运行状态，不能在停止后一直显示“等待”。
            while not task_queue.empty():
                _idx, acc = task_queue.get_nowait()
                self._schedule_account_status(acc, "已停止")
                self._set_account_batch_active(acc, False)
                task_queue.task_done()

            if not completed_summaries:
                status_text = f"{query_label}查询已停止" if self.stop_event.is_set() or any(
                    result["stopped"] for result in account_results
                ) else f"{query_label}查询失败（无完整交易对数据）"
                logger.info("%s %s", base_asset, status_text)
                self._dispatch_ui(lambda: self._restore_batch_ui_to_idle(
                    status_text=f"状态：{status_text}", finish_summary=False,
                ))
                return

            summary = self._merge_monthly_trade_volume_summaries(completed_summaries)
            partial = success < len(selected) or self.stop_event.is_set()
            label = "已停止，部分汇总" if self.stop_event.is_set() else "部分汇总"
            title = (
                f"{base_asset} {period_label}{label}（完整账号{success}/{len(selected)}，仅含已查部分）" if partial
                else f"{base_asset} {period_label}全部账号成交汇总（成功{success}/{len(selected)}）"
            )
            logger.info(
                "%s",
                detailed_summary_text(
                    title=title,
                    summary=summary,
                    symbols_text="、".join(dict.fromkeys(item["symbol"] for item in completed_summaries)),
                ),
            )
            status_text = (
                f"状态：{base_asset}{summary_label} {label if partial else str(len(spot_symbol_infos)) + '对'}｜"
                f"{compact_summary_text(summary, include_total=False)}｜"
                f"{success}/{len(selected)}（详情见日志）"
            )
            self._dispatch_ui(
                lambda: self._restore_batch_ui_to_idle(status_text=status_text, finish_summary=False)
            )

        self.worker_thread = self._start_managed_thread(controller, name="exchange-monthly-trade-volume")
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
            max_threads = bounded_int(self.max_threads_var.get(), "线程数")
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

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
                                        self._format_amount(amount),
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
            require_confirm=False,
        )
    def run_selected_accounts(
        self,
        accounts_to_run=None,
        *,
        batch_total_asset_only: bool = False,
        batch_collect_bnb_mode: bool = False,
        batch_enable_withdraw_override: bool | None = None,
        batch_enable_bnb_topup_override: bool | None = None,
        batch_bnb_topup_account_keys_override: list[str] | tuple[str, ...] | set[str] | None = None,
        trade_settings_override: dict[str, object] | None = None,
        require_confirm: bool = True,
    ):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "当前没有可执行的账号")
            return

        account_type = (trade_settings_override or {}).get("trade_account_type", self.trade_account_type_var.get())
        if not batch_total_asset_only and not batch_collect_bnb_mode and account_type == STOCK_TYPE:
            return self.start_stock_tasks(selected, raw=(trade_settings_override or {}).get("stock_settings"), require_confirm=require_confirm)

        trade_settings = None
        try:
            withdraw_buffer = finite_float(self.withdraw_buffer_var.get(), "提现预留", maximum=None)
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = bounded_int(self.usdt_timeout_var.get(), "到账超时", minimum=0, maximum=86400)
            max_threads = bounded_int(self.max_threads_var.get(), "线程数")
            if (not batch_total_asset_only) and (not batch_collect_bnb_mode):
                trade_settings = (
                    dict(trade_settings_override)
                    if trade_settings_override is not None
                    else self._collect_trade_mode_settings()
                )
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确 (请检查轮数/线程数/延迟等)")
            return

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
            market_stop_mode = str(trade_settings.get("market_stop_mode", MARKET_STOP_MODE_DEFAULT))
            market_stagger_enabled = bool(trade_settings.get("market_stagger_enabled", MARKET_STAGGER_ENABLED_DEFAULT))
            premium_delta_value = trade_settings["premium_delta_value"]
            premium_order_count = int(trade_settings["premium_order_count"])
            premium_append_threshold_value = trade_settings["premium_append_threshold_value"]
            premium_partial_fill = bool(trade_settings["premium_partial_fill"])
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_value = trade_settings["reprice_threshold_value"]
            ladder_layout = str(trade_settings["ladder_layout"])
            ladder_min_pool_notional_value = trade_settings["ladder_min_pool_notional_value"]
            ladder_sell_premium_enabled = bool(trade_settings["ladder_sell_premium_enabled"])
            ladder_sell_premium_value = trade_settings["ladder_sell_premium_value"]
            futures_symbol = str(trade_settings["futures_symbol"])
            futures_rounds = int(trade_settings["futures_rounds"])
            futures_amount_value = trade_settings["futures_amount_value"]
            futures_leverage = int(trade_settings["futures_leverage"])
            futures_margin_type = str(trade_settings["futures_margin_type"])
            futures_side = str(trade_settings["futures_side"])
            earn_asset = str(trade_settings["earn_asset"])
            earn_amount_value = trade_settings["earn_amount_value"]
            earn_amount_all = bool(trade_settings.get("earn_amount_all"))
            earn_auto_subscribe = bool(trade_settings.get("earn_auto_subscribe"))
            earn_auto_subscribe_only = bool(trade_settings.get("earn_auto_subscribe_only"))
        else:
            trade_account_type = TRADE_ACCOUNT_TYPE_SPOT
            spot_rounds = max(1, int(self.spot_rounds_var.get() or SPOT_ROUNDS_DEFAULT))
            trade_mode = TRADE_MODE_MARKET
            market_stop_mode = MARKET_STOP_MODE_DEFAULT
            market_stagger_enabled = MARKET_STAGGER_ENABLED_DEFAULT
            premium_delta_value = None
            premium_order_count = PREMIUM_ORDER_COUNT_DEFAULT
            premium_append_threshold_value = Decimal(PREMIUM_APPEND_THRESHOLD_DEFAULT)
            premium_partial_fill = False
            bnb_fee_stop_value = None
            bnb_topup_amount_value = Decimal("0")
            reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
            ladder_layout = LADDER_LAYOUT_DEFAULT
            ladder_min_pool_notional_value = Decimal(LADDER_MIN_POOL_NOTIONAL_DEFAULT)
            ladder_sell_premium_enabled = LADDER_SELL_PREMIUM_ENABLED_DEFAULT
            ladder_sell_premium_value = Decimal(LADDER_SELL_PREMIUM_DEFAULT)
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
            earn_asset = ""
            earn_amount_value = None
            earn_amount_all = False
            earn_auto_subscribe = False
            earn_auto_subscribe_only = False

        market_bnb_stop_mode = (
            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
            and trade_mode == TRADE_MODE_MARKET
            and market_stop_mode == MARKET_STOP_MODE_BNB
        )

        use_shared_rate_limit_gate = bool(
            trade_settings
            and self._uses_shared_rate_limit_gate(trade_account_type, trade_mode)
        )
        try:
            rate_limit_wait_seconds = (
                self._rate_limit_wait_seconds_value() if use_shared_rate_limit_gate else 0
            )
        except RuntimeError as e:
            messagebox.showerror("错误", str(e))
            return

        configured_bnb_topup_amount_value = Decimal(str(bnb_topup_amount_value or "0"))
        show_bnb_topup_option = self._batch_supports_bnb_topup(
            trade_account_type,
            trade_mode,
            spot_rounds,
            market_stop_mode,
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
                market_stop_mode=market_stop_mode,
                market_stagger_enabled=market_stagger_enabled,
                bnb_fee_stop_value=bnb_fee_stop_value,
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
                bnb_topup_account_count=sum(
                    1 for acc in selected if self._account_bnb_topup_enabled(acc)
                ),
                ladder_layout=ladder_layout,
                ladder_min_pool_notional=ladder_min_pool_notional_value,
                earn_asset=earn_asset,
                earn_amount=earn_amount_value,
                earn_amount_all=earn_amount_all,
                earn_auto_subscribe=earn_auto_subscribe,
                earn_auto_subscribe_only=earn_auto_subscribe_only,
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

        if batch_bnb_topup_account_keys_override is None:
            bnb_topup_account_keys = {
                self._account_batch_key(acc)
                for acc in selected
                if self._account_bnb_topup_enabled(acc)
            }
        else:
            bnb_topup_account_keys = {
                str(account_key)
                for account_key in batch_bnb_topup_account_keys_override
                if str(account_key)
            }

        if batch_total_asset_only:
            self.total_asset_results = {}

        if batch_total_asset_only:
            batch_action_label = "查询全部总资产"
        elif batch_collect_bnb_mode:
            batch_action_label = "归集BNB并提现"
        elif trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
            batch_action_label = "开启自动申购" if earn_auto_subscribe_only else "理财存入"
        else:
            batch_action_label = "批量执行"
        if trade_account_type == TRADE_ACCOUNT_TYPE_EARN and not batch_total_asset_only and not batch_collect_bnb_mode:
            batch_summary_runner = (
                "batch_enable_flexible_earn_auto_subscribe"
                if earn_auto_subscribe_only
                else "batch_subscribe_flexible_earn"
            )
        else:
            batch_summary_runner = "run_selected_accounts"
        preflight_required = bool(trade_settings)
        validated_quote_asset = None
        validated_earn_product_id = None

        logger.info("交易所批量链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        batch_request_gate = (
            SharedRateLimitGate(rate_limit_wait_seconds, stop_event=self.stop_event)
            if use_shared_rate_limit_gate
            else None
        )
        if batch_request_gate is not None:
            logger.info("批次 API 限流共享等待已启用：%d 秒", rate_limit_wait_seconds)
        self._batch_task_active = True
        if not preflight_required:
            self._begin_batch_summary_tracking(
                action_label=batch_action_label,
                runner=batch_summary_runner,
                retry_kwargs={
                    "batch_total_asset_only": bool(batch_total_asset_only),
                    "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                    "batch_enable_withdraw_override": (
                        enable_withdraw
                        if (
                            not batch_total_asset_only
                            and not batch_collect_bnb_mode
                            and trade_account_type != TRADE_ACCOUNT_TYPE_EARN
                        )
                        else None
                    ),
                    "batch_enable_bnb_topup_override": (
                        bool(enable_bnb_topup)
                        if (
                            not batch_total_asset_only
                            and not batch_collect_bnb_mode
                            and trade_account_type != TRADE_ACCOUNT_TYPE_EARN
                            and show_bnb_topup_option
                        )
                        else None
                    ),
                    "batch_bnb_topup_account_keys_override": (
                        sorted(bnb_topup_account_keys)
                        if show_bnb_topup_option and enable_bnb_topup
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
        total_accounts = len(selected)
        worker_count = min(max_threads, total_accounts)

        if preflight_required:
            if trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
                self.status_var.set("状态：初始化连接/校验理财产品中...")
            else:
                self.status_var.set("状态：初始化连接/校验交易对中...")
        elif batch_total_asset_only:
            self.status_var.set(f"状态：查询全部总资产中 (并发 {worker_count} 线程)...")
        elif batch_collect_bnb_mode:
            self.status_var.set(f"状态：归集BNB并提现中 (并发 {worker_count} 线程)...")
        elif trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
            action_text = "开启自动申购" if earn_auto_subscribe_only else "理财存入"
            self.status_var.set(f"状态：{action_text}中 (并发 {worker_count} 线程)...")
        else:
            self.status_var.set(f"状态：批量执行中 (并发 {worker_count} 线程)...")

        self.progress["value"] = 0
        self.progress["maximum"] = total_accounts

        strict_market_stagger = self._batch_market_stagger_enabled(
            enabled=market_stagger_enabled,
            account_count=total_accounts,
            max_threads=worker_count,
            trade_account_type=trade_account_type,
            trade_mode=trade_mode,
            spot_rounds=spot_rounds,
            market_stop_mode=market_stop_mode,
        )
        market_stagger_coordinator = (
            MarketStaggerCoordinator(range(1, worker_count + 1))
            if strict_market_stagger
            else None
        )
        if market_stagger_coordinator is not None:
            logger.info(
                "市价严格交错已启用：%d 个工作线程，随账号完成自适应平衡两组买卖方向",
                worker_count,
            )
        auto_ladder_observer = None
        auto_ladder_observer_client = None

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
                    if market_stagger_coordinator is not None:
                        market_stagger_coordinator.unregister(thread_id)
                    clear_exchange_account_log_index()
                    return
                set_exchange_account_log_index(idx)
                account_bnb_topup_amount_value = self._batch_account_bnb_topup_amount(
                    acc,
                    bnb_topup_amount_value,
                    bnb_topup_account_keys,
                )

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
                    client = self._create_binance_client(
                        acc["api_key"],
                        acc["api_secret"],
                        request_gate=batch_request_gate,
                        request_stop_event=combined_stop,
                    )
                    trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
                    quote_asset = str(validated_quote_asset or "").strip().upper()
                    if (not batch_total_asset_only) and trade_account_type != TRADE_ACCOUNT_TYPE_EARN and (not quote_asset):
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
                            [f'{r["walletName"]}:{self._format_amount(r["balance"])}' for r in rows if r["balance"] > 0]
                        )
                        logger.info(
                            "账号 #%d 总资产约 %s USDT；钱包=%s；币种=%s",
                            idx,
                            self._format_amount(total_usdt),
                            detail_text if detail_text else "无非零钱包余额",
                            asset_text if asset_text != "--" else "无币种资产",
                        )

                        set_status(
                            self._exchange_success_status_text(
                                asset_text if asset_text != "--" else f"总资产 {self._format_amount(total_usdt)} USDT"
                            )
                        )
                        op_success = True

                    # 2) 归集BNB并提现
                    elif batch_collect_bnb_mode:
                        collect_withdraw_coin, collect_withdraw_enabled = self._collect_bnb_withdraw_runtime()
                        logger.info(f"账号 #{idx} 开始执行【归集BNB并提现模式】")
                        set_status("归集合约/资金...")
                        client.collect_all_to_spot()

                        if combined_stop.is_set():
                            finish_current_now("已停止")
                            continue

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

                        earn_assets: list[str] = []
                        earn_balance_assets: list[str] = []
                        try:
                            earn_assets = self._collect_bnb_simple_earn_assets(client)
                            earn_balance_assets = self._collect_bnb_simple_earn_balance_assets(earn_assets)
                            if earn_balance_assets:
                                logger.info(
                                    "账号 #%d 跳过理财余额映射资产：%s",
                                    idx,
                                    earn_balance_assets,
                                )
                        except Exception as e:
                            logger.warning("账号 #%d 查询理财持仓失败，继续按现货资产归集: %s", idx, e)

                        time.sleep(1)

                        set_status("大额资产兑BNB...")
                        convert_summary = {"direct_bnb": [], "fallback_usdt": [], "residual_assets": []}
                        try:
                            convert_summary = client.convert_large_spot_assets_to_bnb(
                                skip_assets=earn_balance_assets,
                            )
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

                        set_status("确认BNB在现货...")
                        client.ensure_bnb_fee_ready_in_spot()

                        residual_assets: list[str] = []
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
                            try:
                                residual_total_usdt = self._collect_bnb_spot_residual_usdt(
                                    client,
                                    residual_assets,
                                )
                                if residual_total_usdt < self.COLLECT_BNB_RESIDUAL_IGNORE_USDT:
                                    logger.info(
                                        "账号 #%d 残留现货资产总额约 %s USDT，小于 0.1 USDT，忽略残留：%s",
                                        idx,
                                        self._format_amount(residual_total_usdt),
                                        residual_assets,
                                    )
                                    residual_assets.clear()
                            except Exception as e:
                                logger.warning("账号 #%d 残留资产估值失败，继续按残留处理: %s", idx, e)
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
                            self._format_amount(bnb_balance),
                        )
                        if not str(acc.get("address") or "").strip():
                            logger.info("账号 #%d 未填写提现地址，跳过 %s 提现", idx, collect_withdraw_coin)
                            set_status(self._format_missing_withdraw_address_status())
                            op_success = True
                            continue
                        if earn_assets and bnb_balance - Decimal(str(withdraw_buffer or "0")) <= 0:
                            logger.info(
                                "账号 #%d 检测到活期理财资产并跳过，现货提现额度不足：理财资产=%s，现货%s=%s，预留=%s",
                                idx,
                                earn_assets,
                                collect_withdraw_coin,
                                self._format_amount(bnb_balance),
                                self._format_amount(Decimal(str(withdraw_buffer or "0"))),
                            )
                            status_text, batch_complete = self._format_collect_bnb_earn_status(
                                0.0,
                                collect_withdraw_coin,
                                earn_assets,
                            )
                            set_status(status_text)
                            op_success = batch_complete
                            continue

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
                            status_text, batch_complete = self._format_collect_bnb_earn_status(
                                amount,
                                collect_withdraw_coin,
                                earn_assets,
                            )
                            set_status(status_text)
                            op_success = batch_complete
                        except Exception as e:
                            logger.error(f"账号 #{idx} {collect_withdraw_coin}提现失败: {e}")
                            set_status(self._exchange_error_status_text(f"{collect_withdraw_coin}提现失败", e, fallback=f"{collect_withdraw_coin}提现失败"))
                            op_success = False

                    # 3) 活期理财存入 / 自动申购开关
                    elif trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
                        asset_u = str(earn_asset or "").strip().upper()
                        product_id = str(validated_earn_product_id or "").strip()
                        if not product_id:
                            set_status(f"查询{asset_u}理财...")
                            product = client.find_simple_earn_flexible_product(
                                asset_u,
                                None if earn_auto_subscribe_only else earn_amount_value,
                            )
                            product_id = str(product.get("productId") or "").strip()
                        if combined_stop.is_set():
                            finish_current_now("已停止", success=False)
                            continue

                        delay_ok = self._wait_batch_random_delay(
                            combined_stop,
                            min_delay,
                            max_delay,
                            log_prefix=(
                                f"账号 #{idx} 开启自动申购前"
                                if earn_auto_subscribe_only
                                else f"账号 #{idx} 理财存入前"
                            ),
                        )
                        if not delay_ok:
                            finish_current_now("已停止", success=False)
                            continue

                        if earn_auto_subscribe_only:
                            set_status(f"开启 {asset_u} 自动申购...")
                            client.set_simple_earn_flexible_auto_subscribe(product_id, True)
                            self._record_batch_count_metric(acc, "auto_enabled_by_account", 1)
                            logger.info(
                                "账号 #%d 已开启活期理财自动申购：资产=%s，productId=%s",
                                idx,
                                asset_u,
                                product_id,
                            )
                            set_status(self._exchange_success_status_text(f"已开启 {asset_u} 自动申购"))
                        else:
                            set_status(f"存入理财 {asset_u}...")
                            amount_dec = self._resolve_earn_subscribe_amount(
                                client,
                                asset_u,
                                earn_amount_value,
                                earn_amount_all,
                            )
                            if earn_amount_all:
                                set_status(f"查询{asset_u}理财...")
                                product = client.find_simple_earn_flexible_product(asset_u, amount_dec)
                                product_id = str(product.get("productId") or "").strip()
                                set_status(f"存入理财 {asset_u}...")
                            result = client.subscribe_simple_earn_flexible_product(
                                product_id,
                                amount_dec,
                                source_account="SPOT",
                                auto_subscribe=earn_auto_subscribe,
                            )
                            amount_text = self._format_amount(amount_dec)
                            purchase_id = str((result or {}).get("purchaseId") or "").strip() if isinstance(result, dict) else ""
                            self._record_batch_amount_metric(acc, "earn_subscribe_by_account", amount_dec, asset_u)
                            logger.info(
                                "账号 #%d 活期理财存入已提交：资产=%s，数量=%s，自动申购=%s，productId=%s，purchaseId=%s",
                                idx,
                                asset_u,
                                amount_text,
                                "开启" if earn_auto_subscribe else "关闭",
                                product_id,
                                purchase_id or "--",
                            )
                            set_status(self._exchange_success_status_text(f"理财存入 {amount_text} {asset_u}"))
                        op_success = True

                    # 4) 原批量现货/合约策略
                    else:
                        zero_round_spot_conversion = (
                            trade_account_type == TRADE_ACCOUNT_TYPE_SPOT
                            and not market_bnb_stop_mode
                            and spot_rounds == 0
                        )
                        if zero_round_spot_conversion and trade_mode == TRADE_MODE_MARKET:
                            base_asset = client.resolve_spot_market_conversion(spot_symbol)["fromAsset"]
                        elif zero_round_spot_conversion:
                            base_asset = BinanceClient.get_spot_base_asset(spot_symbol)
                        else:
                            base_asset = ""
                        wait_asset = base_asset if zero_round_spot_conversion else quote_asset
                        if zero_round_spot_conversion:
                            set_status(f"检测{base_asset}/{quote_asset} 任一方余额...")
                        elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
                            set_status("检测交易对任一方余额...")
                        else:
                            set_status(f"检测 {quote_asset} 到账...")
                        try:
                            balance_ready = self.wait_for_usdt(
                                usdt_timeout,
                                combined_stop,
                                client=client,
                                symbol=trade_symbol,
                                trade_account_type=trade_account_type,
                                trade_mode=trade_mode,
                                required_quote_amount=required_quote_amount,
                                spot_base_to_quote_only=zero_round_spot_conversion,
                            )
                        except Exception as e:
                            if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and "合约有问题" in str(e):
                                logger.error("账号 #%d 合约交易前检查失败: %s", idx, e)
                                set_status(self._exchange_error_status_text("合约有问题", e, fallback="合约有问题"))
                                finish_current_now(success=False)
                                continue
                            raise
                        if not balance_ready:
                            if combined_stop.is_set():
                                logger.info(f"账号 #{idx} 已请求停止")
                                finish_current_now("已停止", success=False)
                            else:
                                if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
                                    logger.info(f"账号 #{idx} 交易对任一方余额检测超时，跳过")
                                    set_status(self._exchange_failed_status_text("无交易对可用余额"))
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
                        elif trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and not market_bnb_stop_mode and spot_rounds == 0:
                            logger.info("账号 #%d 开始执行0轮现货兑换，交易对=%s", idx, spot_symbol)
                        elif trade_mode == TRADE_MODE_MARKET:
                            if market_bnb_stop_mode:
                                logger.info(
                                    "账号 #%d 开始执行市价策略：按剩余BNB停止，停止值=%s BNB，预买BNB金额=%s",
                                    idx,
                                    BinanceClient._format_decimal(bnb_fee_stop_value),
                                    account_bnb_topup_amount_value,
                                )
                            else:
                                logger.info("账号 #%d 开始执行市价策略：%d 轮，预买BNB金额=%s", idx, spot_rounds, account_bnb_topup_amount_value)
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
                                account_bnb_topup_amount_value,
                                bnb_fee_stop_value,
                                BinanceClient._format_decimal(effective_reprice_threshold),
                                quote_asset,
                            )
                        elif trade_mode in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER}:
                            if reprice_threshold_value > 0:
                                effective_reprice_threshold = client.normalize_price_delta(
                                    spot_symbol,
                                    reprice_threshold_value,
                                    min_one_tick=True,
                                )
                            else:
                                effective_reprice_threshold = Decimal("0")
                            logger.info(
                                "账号 #%d 开始执行分仓挂单策略：档位模式=%s，最小分仓成交额=%s %s，预买BNB金额=%s，剩余bnb手续费=%s，重挂阈值=%s %s",
                                idx,
                                ladder_layout,
                                BinanceClient._format_decimal(ladder_min_pool_notional_value),
                                quote_asset,
                                account_bnb_topup_amount_value,
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
                                account_bnb_topup_amount_value,
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

                        self._assert_account_cleanup_finished(client, acc["api_key"])
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
                            market_stop_mode=market_stop_mode,
                            market_stagger_coordinator=market_stagger_coordinator,
                            market_stagger_lane_id=thread_id,
                            premium_delta=premium_delta_value,
                            premium_order_count=premium_order_count,
                            premium_append_threshold=premium_append_threshold_value,
                            premium_partial_fill=premium_partial_fill,
                            bnb_fee_stop_value=bnb_fee_stop_value,
                            bnb_topup_amount=account_bnb_topup_amount_value,
                            reprice_threshold_amount=reprice_threshold_value,
                            ladder_layout=ladder_layout,
                            ladder_min_pool_notional=ladder_min_pool_notional_value,
                            ladder_sell_premium_enabled=ladder_sell_premium_enabled,
                            ladder_sell_premium=ladder_sell_premium_value,
                            auto_ladder_settings=self.auto_ladder_settings_store if trade_mode == TRADE_MODE_AUTO_LADDER else None,
                            auto_ladder_observer=auto_ladder_observer if trade_mode == TRADE_MODE_AUTO_LADDER else None,
                            futures_symbol=futures_symbol,
                            futures_rounds=futures_rounds,
                            futures_amount=futures_amount_value,
                            futures_leverage=futures_leverage,
                            futures_margin_type=futures_margin_type,
                            futures_side=futures_side,
                            operation_journal=self.exchange_operation_journal,
                            operation_scope=self._exchange_operation_scope(acc["api_key"]),
                            cleanup_callback=getattr(self, "_record_strategy_cleanup", None),
                        )

                        strategy_result, trade_stats_text = self._run_strategy_with_spot_statistics(
                            strategy, client, combined_stop, progress_cb,
                            trade_account_type=trade_account_type,
                            trade_mode=trade_mode,
                            zero_round_spot_conversion=zero_round_spot_conversion,
                            spot_symbol=spot_symbol,
                            quote_asset=quote_asset,
                        )
                        trade_error = strategy_result.get("trade_error") if isinstance(strategy_result, dict) else ""

                        if not combined_stop.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount
                            if isinstance(strategy_result, dict):
                                self._record_batch_count_metric(
                                    acc,
                                    "market_failed_rounds_by_account",
                                    strategy_result.get("market_round_failed_count", 0),
                                )

                            if trade_error:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(
                                    self._format_trade_failure_status(
                                        strategy_result.get("trade_error_stage", ""),
                                        trade_error,
                                    )
                                )
                                op_success = False
                            elif enable_withdraw and not str(acc.get("address") or "").strip():
                                set_status(
                                    self._format_missing_withdraw_address_status(
                                        strategy_result if isinstance(strategy_result, dict) else None,
                                        trade_stats_text,
                                    )
                                )
                                op_success = True
                            elif isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                withdraw_failure_status = self._exchange_error_status_text(
                                    "提现失败",
                                    strategy_result.get("withdraw_error", ""),
                                    fallback="提现失败",
                                )
                                round_status = self._format_market_round_failure_status(strategy_result)
                                if round_status:
                                    set_status(
                                        self._exchange_failed_status_text(
                                            "｜".join(
                                                part for part in (
                                                    self._plain_exchange_status_text(withdraw_failure_status),
                                                    trade_stats_text,
                                                    round_status,
                                                ) if part
                                            ),
                                            fallback="提现失败",
                                        )
                                    )
                                else:
                                    failure_body = self._plain_exchange_status_text(withdraw_failure_status)
                                    if trade_stats_text:
                                        failure_body = f"{failure_body}｜{trade_stats_text}"
                                    set_status(self._exchange_failed_status_text(failure_body, fallback="提现失败"))
                                op_success = False
                            else:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                status_text, withdraw_success = self._format_batch_strategy_completion_status(
                                    float(final_amount or 0.0),
                                    withdraw_coin,
                                    enable_withdraw=enable_withdraw,
                                    strategy_result=strategy_result if isinstance(strategy_result, dict) else None,
                                    completion_text=(self._spot_strategy_completion_text(
                                        trade_mode,
                                        spot_rounds,
                                        strategy_result if isinstance(strategy_result, dict) else None,
                                    ) if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT else ""),
                                    trade_stats_text=trade_stats_text,
                                )
                                if status_text is not None:
                                    set_status(status_text)
                                op_success = withdraw_success
                        else:
                            cleanup_error = (
                                str(strategy_result.get("ladder_cleanup_error") or "").strip()
                                if isinstance(strategy_result, dict)
                                else ""
                            )
                            cleanup_confirmed = bool(
                                isinstance(strategy_result, dict)
                                and strategy_result.get("ladder_cleanup_confirmed")
                            )
                            if cleanup_error:
                                set_status(self._exchange_failed_status_text(f"停止清理失败：{cleanup_error}"))
                            elif trade_mode in {TRADE_MODE_LADDER, TRADE_MODE_AUTO_LADDER} and cleanup_confirmed:
                                set_status("已停止（已确认撤单清仓）")
                            else:
                                set_status("已停止")
                            op_success = False

                except Exception as e:
                    if combined_stop.is_set():
                        logger.info("账号 #%d 已停止", idx)
                        set_status("已停止")
                    else:
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

            if market_stagger_coordinator is not None:
                market_stagger_coordinator.unregister(thread_id)

        def controller():
            nonlocal validated_quote_asset, validated_earn_product_id, auto_ladder_observer, auto_ladder_observer_client
            if preflight_required:
                validate_client = None
                try:
                    sample_acc = selected[0]
                    validate_client = self._create_binance_client(
                        sample_acc["api_key"],
                        sample_acc["api_secret"],
                        request_gate=batch_request_gate,
                    )
                    if trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
                        earn_product = validate_client.find_simple_earn_flexible_product(
                            earn_asset,
                            None if earn_amount_all or earn_auto_subscribe_only else earn_amount_value,
                        )
                        validated_earn_product_id = str(earn_product.get("productId") or "").strip()
                    else:
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
                        runner=batch_summary_runner,
                        retry_kwargs={
                            "batch_total_asset_only": bool(batch_total_asset_only),
                            "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                            "batch_enable_withdraw_override": (
                                enable_withdraw
                                if (
                                    not batch_total_asset_only
                                    and not batch_collect_bnb_mode
                                    and trade_account_type != TRADE_ACCOUNT_TYPE_EARN
                                )
                                else None
                            ),
                            "batch_enable_bnb_topup_override": (
                                bool(enable_bnb_topup)
                                if (
                                    not batch_total_asset_only
                                    and not batch_collect_bnb_mode
                                    and trade_account_type != TRADE_ACCOUNT_TYPE_EARN
                                    and show_bnb_topup_option
                                )
                                else None
                            ),
                            "batch_bnb_topup_account_keys_override": (
                                sorted(bnb_topup_account_keys)
                                if show_bnb_topup_option and enable_bnb_topup
                                else None
                            ),
                            "trade_settings_override": (
                                dict(trade_settings_override)
                                if trade_settings_override is not None
                                else None
                            ),
                        },
                        selected_accounts=selected,
                    )
                    self._prepare_accounts_for_batch(selected)

                    def _set_running_status():
                        self._set_retry_failed_button_state()
                        if batch_total_asset_only:
                            self.status_var.set(f"状态：查询全部总资产中 (并发 {worker_count} 线程)...")
                        elif batch_collect_bnb_mode:
                            self.status_var.set(f"状态：归集BNB并提现中 (并发 {worker_count} 线程)...")
                        elif trade_account_type == TRADE_ACCOUNT_TYPE_EARN:
                            action_text = "开启自动申购" if earn_auto_subscribe_only else "理财存入"
                            self.status_var.set(f"状态：{action_text}中 (并发 {worker_count} 线程)...")
                        else:
                            self.status_var.set(f"状态：批量执行中 (并发 {worker_count} 线程)...")

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
            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_AUTO_LADDER:
                try:
                    sample_acc = selected[0]
                    auto_ladder_observer_client = self._create_binance_client(
                        sample_acc["api_key"], sample_acc["api_secret"],
                        request_gate=batch_request_gate, request_stop_event=self.stop_event,
                    )
                    auto_ladder_observer = AutoLadderMarketObserver(
                        auto_ladder_observer_client, spot_symbol, self.auto_ladder_settings_store,
                        close_client_on_exit=True,
                    )
                    auto_ladder_observer.start(self.stop_event)
                    logger.info("自动分仓公共行情由单独观察线程读取，%d 个账号共享信号", worker_count)
                except Exception as exc:
                    logger.error("自动分仓公共行情观察启动失败：%s", exc)
                    self._close_binance_client_instance(auto_ladder_observer_client)
                    self._dispatch_ui(lambda err=exc: self._restore_batch_ui_to_idle(
                        status_text=f"状态：公共行情观察启动失败：{err}", finish_summary=False,
                    ))
                    return
            try:
                threads = []
                logger.info(f"启动 {worker_count} 个工作线程处理 {total_accounts} 个账号")
                for i in range(worker_count):
                    t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join()
            finally:
                if auto_ladder_observer is not None:
                    if not auto_ladder_observer.close():
                        logger.warning("自动分仓公共行情观察线程尚未退出；连接将在后台请求结束后释放")

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
            withdraw_buffer = finite_float(self.withdraw_buffer_var.get(), "提现预留", maximum=None)
            max_threads = bounded_int(self.max_threads_var.get(), "线程数")
        except (ValueError, RuntimeError) as exc:
            messagebox.showerror("错误", str(exc) or "参数格式不正确")
            return

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
                    elif not str(acc.get("address") or "").strip():
                        logger.info("账号 #%d 未填写提现地址，跳过批量提现", idx)
                        set_status(self._format_missing_withdraw_address_status())
                        op_success = True
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
