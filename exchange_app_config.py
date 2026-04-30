#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from exchange_app_base import (
    BNB_FEE_STOP_DEFAULT,
    BNB_TOPUP_AMOUNT_DEFAULT,
    BinanceClient,
    EXCHANGE_PROXY_CONFIG_FILE,
    EXCHANGE_PROXY_DEFAULT,
    EXCHANGE_USE_CONFIG_PROXY_DEFAULT,
    ExchangeProxyRuntime,
    FUTURES_AMOUNT_DEFAULT,
    FUTURES_LEVERAGE_DEFAULT,
    FUTURES_MARGIN_TYPE_DEFAULT,
    FUTURES_ROUNDS_DEFAULT,
    FUTURES_SIDE_DEFAULT,
    FUTURES_SIDE_OPTIONS,
    FUTURES_SYMBOL_DEFAULT,
    PREMIUM_APPEND_THRESHOLD_DEFAULT,
    PREMIUM_DELTA_DEFAULT,
    PREMIUM_ORDER_COUNT_DEFAULT,
    REPRICE_THRESHOLD_DEFAULT,
    SECRET_BOX,
    SPOT_ROUNDS_DEFAULT,
    SPOT_SYMBOL_DEFAULT,
    STRATEGY_CONFIG_FILE,
    TRADE_ACCOUNT_TYPE_DEFAULT,
    TRADE_ACCOUNT_TYPE_FUTURES,
    TRADE_MODE_CONVERT,
    TRADE_MODE_DEFAULT,
    WITHDRAW_ADDRESS_DEFAULT,
    WITHDRAW_COIN_DEFAULT,
    WITHDRAW_FEE_BUFFER_DEFAULT,
    WITHDRAW_NETWORK_DEFAULT,
    _atomic_write_config_json,
    _load_json_with_backup,
    _read_text_snapshot,
    _require_dict_payload,
    _restore_text_snapshot,
    logger,
    messagebox,
    requests,
)


class ExchangeAppConfigMixin(object):
    @staticmethod
    def _normalize_proxy_text(proxy_text: str) -> str:
        proxy = str(proxy_text or "").strip()
        if not proxy:
            return ""
        lower = proxy.lower()
        if lower.startswith("ss://"):
            ExchangeProxyRuntime.parse_ss_uri(proxy)
            return proxy
        if "://" not in proxy:
            proxy = f"http://{proxy}"
            lower = proxy.lower()
        if not lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
            raise RuntimeError("代理地址格式不支持，请使用 http://、https://、socks5://、socks5h:// 或 ss://")
        return proxy
    def _normalize_exchange_proxy(self, proxy_text: str) -> str:
        return self._normalize_proxy_text(proxy_text)
    def _on_exchange_proxy_config_changed(self, *_args) -> None:
        self._sync_exchange_proxy_state()
    def _sync_exchange_proxy_state(self) -> None:
        use_var = getattr(self, "use_exchange_config_proxy_var", None)
        proxy_var = getattr(self, "exchange_proxy_var", None)
        use_proxy = bool(use_var.get()) if use_var is not None else False
        raw_proxy = str(proxy_var.get() or "").strip() if proxy_var is not None else ""
        lock = getattr(self, "_exchange_proxy_state_lock", None)
        if lock is None:
            self._exchange_proxy_state = {"use_config_proxy": use_proxy, "raw_proxy": raw_proxy}
            return
        with lock:
            self._exchange_proxy_state = {"use_config_proxy": use_proxy, "raw_proxy": raw_proxy}
    def _exchange_proxy_state_snapshot(self) -> dict[str, object]:
        lock = getattr(self, "_exchange_proxy_state_lock", None)
        if lock is None:
            return dict(getattr(self, "_exchange_proxy_state", {}) or {})
        with lock:
            return dict(getattr(self, "_exchange_proxy_state", {}) or {})
    def _get_exchange_proxy(self) -> str:
        state = self._exchange_proxy_state_snapshot()
        return self._normalize_exchange_proxy(state.get("raw_proxy") or "")
    def _exchange_proxy_config_payload(self, state: dict[str, object] | None = None) -> dict[str, object]:
        snapshot = dict(state or self._exchange_proxy_state_snapshot())
        use_proxy = bool(snapshot.get("use_config_proxy"))
        raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
        if use_proxy:
            proxy_text = self._normalize_exchange_proxy(raw_proxy)
            if state is None:
                self.exchange_proxy_var.set(proxy_text)
                self._sync_exchange_proxy_state()
        else:
            proxy_text = raw_proxy
        return {
            "exchange_proxy_enc": self._encrypt_optional_text(proxy_text),
            "use_exchange_config_proxy": use_proxy,
        }
    def _use_exchange_config_proxy(self) -> bool:
        state = self._exchange_proxy_state_snapshot()
        return bool(state.get("use_config_proxy"))
    def _get_exchange_proxy_url(self, *, proxy_text: str | None = None, use_config_proxy: bool | None = None) -> str:
        if proxy_text is None or use_config_proxy is None:
            state = self._exchange_proxy_state_snapshot()
            if proxy_text is None:
                proxy_text = str(state.get("raw_proxy") or "")
            if use_config_proxy is None:
                use_config_proxy = bool(state.get("use_config_proxy"))
        if not use_config_proxy:
            self.exchange_proxy_runtime.stop()
            return ""
        proxy = self._normalize_exchange_proxy(proxy_text)
        if not proxy:
            self.exchange_proxy_runtime.stop()
            return ""
        if proxy.lower().startswith("ss://"):
            return self.exchange_proxy_runtime.ensure_proxy(proxy)
        return proxy
    def _get_onchain_proxy_page(self):
        page = getattr(self, "onchain_page", None)
        if page is None:
            return None
        return page
    def _get_onchain_proxy(self) -> str:
        state = self._get_onchain_proxy_state()
        return self._normalize_proxy_text(state.get("raw_proxy") or "")
    def _get_onchain_proxy_state(self) -> dict[str, object]:
        page = self._get_onchain_proxy_page()
        if page is None:
            return {"use_config_proxy": False, "raw_proxy": ""}
        snapshotter = getattr(page, "_onchain_proxy_state_snapshot", None)
        if callable(snapshotter):
            try:
                state = dict(snapshotter() or {})
            except Exception:
                state = {}
            return {
                "use_config_proxy": bool(state.get("use_config_proxy")),
                "raw_proxy": str(state.get("raw_proxy") or ""),
            }
        raw = getattr(page, "onchain_proxy_var", None)
        use_var = getattr(page, "use_config_proxy_var", None)
        return {
            "use_config_proxy": bool(use_var.get()) if use_var is not None else False,
            "raw_proxy": str(raw.get() or "") if raw is not None else "",
        }
    def _use_onchain_config_proxy(self) -> bool:
        state = self._get_onchain_proxy_state()
        return bool(state.get("use_config_proxy"))
    def _get_onchain_proxy_url(self, *, proxy_text: str | None = None, use_config_proxy: bool | None = None) -> str:
        if proxy_text is None or use_config_proxy is None:
            state = self._get_onchain_proxy_state()
            if proxy_text is None:
                proxy_text = str(state.get("raw_proxy") or "")
            if use_config_proxy is None:
                use_config_proxy = bool(state.get("use_config_proxy"))
        if not use_config_proxy:
            self.onchain_proxy_runtime.stop()
            return ""
        proxy = self._normalize_proxy_text(proxy_text)
        if not proxy:
            self.onchain_proxy_runtime.stop()
            return ""
        if proxy.lower().startswith("ss://"):
            return self.onchain_proxy_runtime.ensure_proxy(proxy)
        return proxy
    def _requests_proxy_map_from_state(self, state: dict[str, object] | None = None) -> dict[str, str]:
        snapshot = dict(state or self._exchange_proxy_state_snapshot())
        proxy = self._get_exchange_proxy_url(
            proxy_text=str(snapshot.get("raw_proxy") or ""),
            use_config_proxy=bool(snapshot.get("use_config_proxy")),
        )
        if not proxy:
            return {}
        return {"http": proxy, "https": proxy}
    def _requests_proxy_map(self) -> dict[str, str]:
        return self._requests_proxy_map_from_state()
    @staticmethod
    def _system_proxy_map() -> dict[str, str]:
        try:
            proxies = requests.utils.get_environ_proxies("https://api.binance.com/api/v3/time") or {}
        except Exception:
            return {}
        result = {}
        for key in ("http", "https"):
            value = str(proxies.get(key) or "").strip()
            if value:
                result[key] = value
        return result
    def _exchange_proxy_route_text_from_state(self, state: dict[str, object] | None = None) -> str:
        snapshot = dict(state or self._exchange_proxy_state_snapshot())
        raw_proxy = str(snapshot.get("raw_proxy") or "").strip()
        use_config_proxy = bool(snapshot.get("use_config_proxy"))
        if not use_config_proxy:
            system_proxy = self._system_proxy_map()
            if system_proxy:
                return f"system-proxy -> {system_proxy.get('https') or system_proxy.get('http')}"
            return "direct"
        if not raw_proxy:
            return "direct"
        if raw_proxy.lower().startswith("ss://"):
            snap = self.exchange_proxy_runtime.snapshot()
            backend = snap.get("backend") or "unknown"
            local_proxy = snap.get("local_proxy_url") or "pending"
            return f"builtin-ss/{backend} -> {local_proxy}"
        return f"manual-proxy -> {self._normalize_exchange_proxy(raw_proxy)}"
    def _exchange_proxy_route_text(self) -> str:
        return self._exchange_proxy_route_text_from_state()
    @staticmethod
    def _close_binance_client_instance(client: BinanceClient | None) -> None:
        if client is None:
            return
        try:
            client.close()
        except Exception:
            pass
    def _replace_current_binance_client(self, client: BinanceClient | None) -> None:
        previous = self.client
        self.client = client
        if previous is not None and previous is not client:
            self._close_binance_client_instance(previous)
    def _clear_current_binance_client(self) -> None:
        previous = self.client
        self.client = None
        self._close_binance_client_instance(previous)
    def _create_binance_client(self, key: str, secret: str) -> BinanceClient:
        return BinanceClient(key, secret, proxy_url=self._get_exchange_proxy_url())
    @staticmethod
    def _ensure_trade_symbol_supported(
        client: BinanceClient,
        trade_account_type: str,
        trade_mode: str,
        spot_symbol: str,
        futures_symbol: str,
    ) -> str:
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
            return client.get_um_futures_margin_asset(futures_symbol)
        _base_asset, quote_asset = client.ensure_spot_symbol_supported(spot_symbol)
        if trade_mode == TRADE_MODE_CONVERT:
            client.ensure_convert_symbol_supported(spot_symbol)
        return quote_asset
    @staticmethod
    def _encrypt_optional_text(value: str) -> str:
        text = str(value or "").strip()
        return SECRET_BOX.encrypt(text) if text else ""
    @staticmethod
    def _decrypt_optional_text(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return SECRET_BOX.decrypt(text).strip()
    def _strategy_config_payload(self):
        trade_settings = self._collect_trade_mode_settings()
        return {
            "api_key": SECRET_BOX.encrypt(self.api_key_var.get().strip()),
            "api_secret": SECRET_BOX.encrypt(self.api_secret_var.get().strip()),
            "trade_account_type": trade_settings["trade_account_type"],
            "spot_rounds": int(trade_settings["stored_spot_rounds"]),
            "trade_mode": trade_settings["trade_mode"],
            "premium_delta": trade_settings["premium_delta"],
            "premium_order_count": int(trade_settings["premium_order_count"]),
            "premium_append_threshold": trade_settings["premium_append_threshold"],
            "bnb_fee_stop": trade_settings["bnb_fee_stop"],
            "bnb_topup_amount": trade_settings["bnb_topup_amount"],
            "reprice_threshold": trade_settings["reprice_threshold"],
            "spot_symbol": self.spot_symbol_var.get().strip().upper(),
            "futures_symbol": str(trade_settings["futures_symbol"] or "").strip().upper(),
            "futures_rounds": int(trade_settings["stored_futures_rounds"]),
            "futures_amount": str(trade_settings["futures_amount"] or "").strip(),
            "futures_leverage": int(trade_settings["futures_leverage"]),
            "futures_margin_type": str(trade_settings["futures_margin_type"] or FUTURES_MARGIN_TYPE_DEFAULT).strip().upper(),
            "futures_side": str(trade_settings["futures_side"] or FUTURES_SIDE_DEFAULT).strip(),
            "withdraw_address": self.withdraw_addr_var.get().strip(),
            "withdraw_network": self.withdraw_net_var.get().strip(),
            "withdraw_coin": self.withdraw_coin_var.get().strip().upper(),
            "withdraw_buffer": float(self.withdraw_buffer_var.get()),
            "enable_withdraw": bool(self.enable_withdraw_var.get()),
            "min_delay_ms": self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f"),
            "max_delay_ms": self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927"),
            "usdt_timeout_sec": int(self.usdt_timeout_var.get()),
        }
    def save_strategy_config(self):
        try:
            proxy_payload = self._exchange_proxy_config_payload()
            payload = self._strategy_config_payload()
            strategy_snapshot = _read_text_snapshot(STRATEGY_CONFIG_FILE)
            proxy_snapshot = _read_text_snapshot(EXCHANGE_PROXY_CONFIG_FILE)
            try:
                _atomic_write_config_json(STRATEGY_CONFIG_FILE, payload)
                _atomic_write_config_json(EXCHANGE_PROXY_CONFIG_FILE, proxy_payload)
            except Exception:
                try:
                    _restore_text_snapshot(STRATEGY_CONFIG_FILE, strategy_snapshot)
                    _restore_text_snapshot(EXCHANGE_PROXY_CONFIG_FILE, proxy_snapshot)
                except Exception as rollback_exc:
                    logger.error("保存配置回滚失败: %s", rollback_exc)
                raise
            logger.info("策略配置已保存到：%s", STRATEGY_CONFIG_FILE)
            messagebox.showinfo("成功", f"策略配置已保存到：\n{STRATEGY_CONFIG_FILE}")
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "配置格式不正确，请检查交易模式、轮次、溢价、笔数、追加挂单、剩余bnb手续费和超时时间")
        except Exception as e:
            logger.error("保存策略配置失败: %s", e)
            messagebox.showerror("错误", "保存策略配置失败: %s" % e)
    def _save_exchange_proxy_config_only(
        self,
        payload: dict[str, object] | None = None,
        *,
        state: dict[str, object] | None = None,
    ) -> None:
        if payload is None:
            payload = self._exchange_proxy_config_payload(state=state)
        _atomic_write_config_json(EXCHANGE_PROXY_CONFIG_FILE, payload)
    def _proxy_config_from_payload(self, raw: object) -> tuple[str, bool]:
        if not isinstance(raw, dict):
            return "", False
        proxy_enc = str(raw.get("exchange_proxy_enc", "") or "").strip()
        legacy_proxy = str(raw.get("exchange_proxy", EXCHANGE_PROXY_DEFAULT) or EXCHANGE_PROXY_DEFAULT).strip()
        try:
            proxy_text = self._decrypt_optional_text(proxy_enc) if proxy_enc else legacy_proxy
        except Exception:
            proxy_text = legacy_proxy
        if "use_exchange_config_proxy" in raw:
            use_proxy = bool(raw.get("use_exchange_config_proxy"))
        else:
            use_proxy = bool(proxy_text)
        return proxy_text, use_proxy
    def _load_exchange_proxy_config(self):
        proxy_text = ""
        use_proxy = EXCHANGE_USE_CONFIG_PROXY_DEFAULT
        if EXCHANGE_PROXY_CONFIG_FILE.exists():
            try:
                raw, recovered = _load_json_with_backup(EXCHANGE_PROXY_CONFIG_FILE, validator=_require_dict_payload)
                proxy_text, use_proxy = self._proxy_config_from_payload(raw)
                if recovered:
                    logger.warning("交易所代理配置损坏，已自动从备份恢复：%s", EXCHANGE_PROXY_CONFIG_FILE)
                else:
                    logger.info("已加载代理配置：%s", EXCHANGE_PROXY_CONFIG_FILE)
            except Exception as e:
                logger.error("加载代理配置失败: %s", e)
        elif STRATEGY_CONFIG_FILE.exists():
            try:
                raw, recovered = _load_json_with_backup(STRATEGY_CONFIG_FILE, validator=_require_dict_payload)
                proxy_text, use_proxy = self._proxy_config_from_payload(raw)
                if recovered:
                    logger.warning("旧版策略配置损坏，已自动从备份恢复：%s", STRATEGY_CONFIG_FILE)
                if proxy_text:
                    try:
                        self.exchange_proxy_var.set(proxy_text)
                        self.use_exchange_config_proxy_var.set(use_proxy)
                        self._save_exchange_proxy_config_only()
                        logger.info("已迁移旧版代理配置到：%s", EXCHANGE_PROXY_CONFIG_FILE)
                    except Exception as save_exc:
                        logger.error("迁移旧版代理配置失败: %s", save_exc)
            except Exception as e:
                logger.error("读取旧版代理配置失败: %s", e)
        self.exchange_proxy_var.set(proxy_text)
        self.use_exchange_config_proxy_var.set(use_proxy)
    def _load_strategy_config(self):
        if not STRATEGY_CONFIG_FILE.exists():
            return
        try:
            raw, recovered = _load_json_with_backup(STRATEGY_CONFIG_FILE, validator=_require_dict_payload)
            if recovered:
                logger.warning("交易所策略配置损坏，已自动从备份恢复：%s", STRATEGY_CONFIG_FILE)

            self.api_key_var.set(SECRET_BOX.decrypt(str(raw.get("api_key", "") or "").strip()).strip())
            self.api_secret_var.set(SECRET_BOX.decrypt(str(raw.get("api_secret", "") or "").strip()).strip())
            self.trade_account_type_var.set(
                self._normalize_trade_account_type(raw.get("trade_account_type", TRADE_ACCOUNT_TYPE_DEFAULT))
            )
            self.spot_rounds_var.set(int(raw.get("spot_rounds", SPOT_ROUNDS_DEFAULT)))
            self.trade_mode_var.set(self._normalize_trade_mode(raw.get("trade_mode", TRADE_MODE_DEFAULT)))
            premium_delta_text = str(raw.get("premium_delta", PREMIUM_DELTA_DEFAULT) or PREMIUM_DELTA_DEFAULT).strip()
            legacy_premium_percent_text = str(raw.get("premium_percent", "") or "").strip()
            if premium_delta_text:
                self.premium_delta_var.set(premium_delta_text)
            else:
                self.premium_delta_var.set(PREMIUM_DELTA_DEFAULT)
                if legacy_premium_percent_text:
                    logger.warning(
                        "检测到旧版“溢价百分比”配置 %s；新版已改为按后置币设置固定溢价，请手动重新填写“溢价”",
                        legacy_premium_percent_text,
                    )
            self.premium_order_count_var.set(int(raw.get("premium_order_count", PREMIUM_ORDER_COUNT_DEFAULT) or PREMIUM_ORDER_COUNT_DEFAULT))
            self.premium_append_threshold_var.set(
                str(
                    raw.get("premium_append_threshold", PREMIUM_APPEND_THRESHOLD_DEFAULT)
                    or PREMIUM_APPEND_THRESHOLD_DEFAULT
                ).strip()
            )
            self.bnb_fee_stop_var.set(str(raw.get("bnb_fee_stop", BNB_FEE_STOP_DEFAULT) or BNB_FEE_STOP_DEFAULT).strip())
            self.bnb_topup_amount_var.set(str(raw.get("bnb_topup_amount", BNB_TOPUP_AMOUNT_DEFAULT) or BNB_TOPUP_AMOUNT_DEFAULT).strip())
            self.reprice_threshold_var.set(
                str(raw.get("reprice_threshold", raw.get("reprice_threshold_percent", REPRICE_THRESHOLD_DEFAULT)) or REPRICE_THRESHOLD_DEFAULT).strip()
            )
            self.spot_symbol_var.set(str(raw.get("spot_symbol", SPOT_SYMBOL_DEFAULT) or SPOT_SYMBOL_DEFAULT).strip().upper())
            self.futures_symbol_var.set(
                str(raw.get("futures_symbol", FUTURES_SYMBOL_DEFAULT) or FUTURES_SYMBOL_DEFAULT).strip().upper()
            )
            self.futures_rounds_var.set(int(raw.get("futures_rounds", FUTURES_ROUNDS_DEFAULT)))
            self.futures_amount_var.set(str(raw.get("futures_amount", FUTURES_AMOUNT_DEFAULT) or FUTURES_AMOUNT_DEFAULT).strip())
            self.futures_leverage_var.set(int(raw.get("futures_leverage", FUTURES_LEVERAGE_DEFAULT)))
            self.futures_margin_type_var.set(
                self._futures_margin_type_label(raw.get("futures_margin_type", FUTURES_MARGIN_TYPE_DEFAULT))
            )
            self.futures_side_var.set(str(raw.get("futures_side", FUTURES_SIDE_DEFAULT) or FUTURES_SIDE_DEFAULT).strip())
            if str(self.futures_side_var.get() or "").strip() not in FUTURES_SIDE_OPTIONS:
                self.futures_side_var.set(FUTURES_SIDE_DEFAULT)
            self.withdraw_addr_var.set(str(raw.get("withdraw_address", WITHDRAW_ADDRESS_DEFAULT) or WITHDRAW_ADDRESS_DEFAULT).strip())
            self.withdraw_net_var.set(str(raw.get("withdraw_network", WITHDRAW_NETWORK_DEFAULT) or WITHDRAW_NETWORK_DEFAULT).strip())
            self.withdraw_coin_var.set(str(raw.get("withdraw_coin", WITHDRAW_COIN_DEFAULT) or WITHDRAW_COIN_DEFAULT).strip().upper())
            self.withdraw_buffer_var.set(float(raw.get("withdraw_buffer", WITHDRAW_FEE_BUFFER_DEFAULT)))
            self.enable_withdraw_var.set(bool(raw.get("enable_withdraw", True)))
            self.min_delay_var.set(int(raw.get("min_delay_ms", 1000)))
            self.max_delay_var.set(int(raw.get("max_delay_ms", 3000)))
            self.usdt_timeout_var.set(int(raw.get("usdt_timeout_sec", 30)))
            self._refresh_strategy_panel_layout()
            logger.info("已加载策略配置：%s", STRATEGY_CONFIG_FILE)
        except Exception as e:
            logger.error("加载策略配置失败: %s", e)
            messagebox.showwarning("提示", f"策略配置加载失败：{e}")
