"""Headless view adapter for the existing exchange controller.

No Tk interpreter, window, desktop data, or onchain page is created. Business
methods remain the same methods used by the desktop application.
"""
from __future__ import annotations

import hashlib
import heapq
import itertools
import threading
import time
from collections import deque
from pathlib import Path

import exchange_app_base as base
from exchange_app_accounts import ExchangeAppAccountsMixin
from exchange_app_batch import ExchangeAppBatchMixin
from exchange_app_config import ExchangeAppConfigMixin
from exchange_app_stocks import ExchangeAppStocksMixin, stock_payload
from exchange_auto_ladder import AutoLadderSettings, AutoLadderSettingsStore
from exchange_stocks import STOCK_FIELDS, STOCK_TYPE, StockSettings
from shared_utils import start_ui_bridge, stop_ui_bridge
from stores import _save_authenticated_config, _load_authenticated_config


FIELD_SPECS = [
    ("trade_account_type", "交易类型", "TRADE_ACCOUNT_TYPE_DEFAULT", "TRADE_ACCOUNT_TYPE_OPTIONS"),
    ("trade_mode", "现货交易模式", "TRADE_MODE_DEFAULT", "TRADE_MODE_OPTIONS"),
    ("spot_symbol", "现货交易对", "SPOT_SYMBOL_DEFAULT", None),
    ("spot_rounds", "现货轮次", "SPOT_ROUNDS_DEFAULT", None),
    ("market_stop_mode", "市价停止方式", "MARKET_STOP_MODE_DEFAULT", "MARKET_STOP_MODE_OPTIONS"),
    ("market_stagger_enabled", "市价交错执行", "MARKET_STAGGER_ENABLED_DEFAULT", None),
    ("premium_delta", "溢价差额", "PREMIUM_DELTA_DEFAULT", None),
    ("premium_order_count", "溢价挂单数量", "PREMIUM_ORDER_COUNT_DEFAULT", None),
    ("premium_append_threshold", "补单阈值", "PREMIUM_APPEND_THRESHOLD_DEFAULT", None),
    ("premium_partial_fill", "允许部分成交", "PREMIUM_PARTIAL_FILL_DEFAULT", None),
    ("bnb_fee_stop", "剩余 BNB 停止阈值", "BNB_FEE_STOP_DEFAULT", None),
    ("bnb_topup_amount", "预购 BNB 金额", "BNB_TOPUP_AMOUNT_DEFAULT", None),
    ("reprice_threshold", "改价阈值", "REPRICE_THRESHOLD_DEFAULT", None),
    ("ladder_layout", "分仓布局", "LADDER_LAYOUT_DEFAULT", "LADDER_LAYOUT_OPTIONS"),
    ("ladder_min_pool_notional", "最小资金池", "LADDER_MIN_POOL_NOTIONAL_DEFAULT", None),
    ("ladder_sell_premium_enabled", "分仓卖出溢价", "LADDER_SELL_PREMIUM_ENABLED_DEFAULT", None),
    ("ladder_sell_premium", "分仓卖出溢价金额", "LADDER_SELL_PREMIUM_DEFAULT", None),
    ("futures_symbol", "合约交易对", "FUTURES_SYMBOL_DEFAULT", None),
    ("futures_rounds", "合约轮次", "FUTURES_ROUNDS_DEFAULT", None),
    ("futures_amount", "合约金额", "FUTURES_AMOUNT_DEFAULT", None),
    ("futures_leverage", "合约杠杆", "FUTURES_LEVERAGE_DEFAULT", None),
    ("futures_margin_type", "保证金模式", "FUTURES_MARGIN_TYPE_DEFAULT", "FUTURES_MARGIN_TYPE_OPTIONS"),
    ("futures_side", "合约方向", "FUTURES_SIDE_DEFAULT", "FUTURES_SIDE_OPTIONS"),
    ("earn_asset", "理财币种", "EARN_ASSET_DEFAULT", None),
    ("earn_amount", "理财金额", "EARN_AMOUNT_DEFAULT", None),
    ("earn_amount_all", "理财使用全部余额", "EARN_AMOUNT_ALL_DEFAULT", None),
    ("earn_auto_subscribe", "理财自动申购", "EARN_AUTO_SUBSCRIBE_DEFAULT", None),
    ("withdraw_coin", "提现币种", "WITHDRAW_COIN_DEFAULT", "WITHDRAW_COIN_OPTIONS"),
    ("withdraw_buffer", "提现预留", "WITHDRAW_FEE_BUFFER_DEFAULT", None),
    ("enable_withdraw", "交易后自动提现", None, None),
    ("max_threads", "账号并发数", None, None),
    ("min_delay", "最小随机延迟（毫秒）", None, None),
    ("max_delay", "最大随机延迟（毫秒）", None, None),
    ("usdt_timeout", "到账等待超时（秒）", "EXCHANGE_USDT_TIMEOUT_SECONDS_DEFAULT", None),
    ("rate_limit_wait", "API 限流等待（秒）", "EXCHANGE_RATE_LIMIT_WAIT_SECONDS_DEFAULT", None),
    ("acc_network", "默认提现网络", "WITHDRAW_NETWORK_DEFAULT", "WITHDRAW_NETWORK_OPTIONS"),
    ("exchange_proxy", "代理地址或分享链接", "EXCHANGE_PROXY_DEFAULT", None),
    ("use_exchange_config_proxy", "使用指定代理", "EXCHANGE_USE_CONFIG_PROXY_DEFAULT", None),
]
EXTRA_DEFAULTS = {"enable_withdraw": False, "max_threads": 4, "min_delay": "", "max_delay": ""}
STOCK_LABELS = dict(symbol="股票代码", rounds="股票轮次", asset="股票支付币种", amount="股票每轮金额（留空全部）",
                    tick="股票价格步长", poll="成交检查间隔", reprice="股票改价间隔",
                    timeout="股票阶段等待上限", deviation="最大价格偏离 %",
                    spread="最大盘口价差 %", interval="股票轮次间隔")


def field_schema():
    result = []
    for name, label, constant, options in FIELD_SPECS:
        default = getattr(base, constant) if constant else EXTRA_DEFAULTS[name]
        result.append(dict(name=name, label=label, default=default,
                           kind="boolean" if isinstance(default, bool) else "text",
                           options=list(getattr(base, options)) if options else None))
    for name, value in STOCK_FIELDS.items():
        result.append(dict(name="stock_" + name, label=STOCK_LABELS.get(name, name), default=value,
                           kind="text", options=None))
    return result


def account_id(key):
    return hashlib.sha256(str(key).encode()).hexdigest()[:24]


class Value:
    def __init__(self, value=""):
        self.value = value
        self.lock = threading.RLock()

    def get(self):
        with self.lock:
            return self.value

    def set(self, value):
        with self.lock:
            self.value = value

    def trace_add(self, *_args):
        return ""


class ViewState(dict):
    """Only presentation state is retained; no widget or interpreter exists."""
    def config(self, **kwargs):
        self.update(kwargs)

    configure = config

    def winfo_exists(self):
        return True


class HeadlessExchange(ExchangeAppStocksMixin, ExchangeAppBatchMixin,
                       ExchangeAppAccountsMixin, ExchangeAppConfigMixin):
    def __init__(self):
        self.client = None
        self.worker_thread = None
        self.stop_event = None
        self._batch_task_active = self._closing = self._close_finalized = False
        self._loading_accounts = False
        self._accounts_save_after_token = None
        self._cleanup_state_lock = threading.RLock()
        self._pending_strategy_cleanups = {}
        self._cleanup_retry_running = False
        self._cleanup_retry_after = 0.0
        self._managed_threads_lock = threading.RLock()
        self._managed_threads = set()
        self._exchange_proxy_state_lock = threading.RLock()
        self._result_file_lock = threading.RLock()
        self._batch_summary_lock = threading.RLock()
        self._current_batch_summary = None
        self._last_batch_retry = None
        self._context_account = None
        self._account_tree_row_to_account = {}
        self._timers = []
        self._timer_ids = itertools.count(1)
        self._timer_lock = threading.RLock()
        self._cancelled_timers = set()
        self.notices = deque(maxlen=40)
        self.accounts = []
        self.total_asset_results = {}
        self.onchain_page = None
        self.progress = ViewState(value=0, maximum=1)
        for name in ("btn_start", "btn_stop", "btn_run_accounts", "btn_query_all_assets",
                     "btn_collect_bnb_combo", "btn_batch_withdraw", "btn_batch_redeem_earn",
                     "btn_refresh", "btn_withdraw", "btn_retry_failed_accounts"):
            setattr(self, name, ViewState(state="normal"))
        for field in field_schema():
            name = field["name"]
            if not name.startswith("stock_"):
                setattr(self, name + "_var", Value(field["default"]))
        self.stock_vars = {name: Value(value) for name, value in STOCK_FIELDS.items()}
        for name in ("api_key", "api_secret", "withdraw_addr", "acc_api_key", "acc_api_secret",
                     "acc_withdraw_addr", "account_search", "account_search_result", "withdraw_net",
                     "single_account_balances", "exchange_proxy_exit_ip", "exchange_proxy_latency",
                     "exchange_proxy_status", "ip", "top_proxy_name"):
            setattr(self, name + "_var", Value(""))
        self.status_var = Value("状态：等待任务")
        self.exchange_batch_summary_var = Value("")
        self.exchange_proxy_runtime = base.ExchangeProxyRuntime(base.STRATEGY_CONFIG_FILE.parent, "exchange")
        self.exchange_proxy_runtime.cleanup_stale_runtime()
        self.account_store = base.AccountStore(base.DATA_FILE)
        self.exchange_operation_journal = base.OperationJournal(base.EXCHANGE_OPERATION_JOURNAL_FILE, fail_unconfirmed=True)
        self.auto_ladder_settings_store = AutoLadderSettingsStore()
        self.settings_file = base.DATA_FILE.parent / "linux_settings.json"
        self.config_update_file = base.DATA_FILE.parent / "linux_account_update.json"
        from config_transaction import recover
        recover(self.config_update_file, [base.DATA_FILE, self.settings_file])
        self.settings = {f["name"]: f["default"] for f in field_schema()}
        self.account_meta = {}
        if base.config_or_backup_exists(self.settings_file):
            raw, _ = _load_authenticated_config(self.settings_file)
            self.settings.update(raw.get("settings", {}))
            self.account_meta = dict(raw.get("accounts", {}))
            self.auto_ladder_settings_store.replace(AutoLadderSettings.parse(raw.get("auto_ladder", {})))
        self.apply_settings()
        self._load_trade_volume_query_settings()
        # Loading failure must block control, rather than silently showing an
        # empty list which could overwrite recoverable encrypted accounts.
        self.account_store.load()
        for a in self.account_store.accounts:
            self._append_account_row(a.api_key, a.api_secret, a.address, a.network)
        start_ui_bridge(self, root=self)

    def apply_settings(self):
        for field in field_schema():
            name = field["name"]
            if name.startswith("stock_"):
                self.stock_vars[name[6:]].set(self.settings[name])
            else:
                getattr(self, name + "_var").set(self.settings[name])
        self._sync_exchange_proxy_state()

    def save_settings(self):
        auto, _ = self.auto_ladder_settings_store.snapshot()
        _save_authenticated_config(self.settings_file, {
            "settings": self.settings, "accounts": self.account_meta, "auto_ladder": auto.as_dict(),
        })

    def update_settings(self, changes, auto=None):
        if not isinstance(changes, dict) or set(changes) - set(self.settings):
            raise ValueError("存在不支持的设置字段")
        proposed = {**self.settings, **changes}
        for field in field_schema():
            value = proposed[field["name"]]
            if field["kind"] == "boolean":
                if type(value) is not bool:
                    raise ValueError(field["label"] + "必须为开关")
            elif not isinstance(value, (str, int, float)) or len(str(value)) > 8192:
                raise ValueError(field["label"] + "格式无效")
            if field["options"] and value not in field["options"]:
                raise ValueError(field["label"] + "选项无效")
        # The validator owns independent values even while strategy threads are
        # reading the current view. It cannot start clients or a proxy runtime.
        validator = object.__new__(HeadlessExchange)
        validator.stock_vars = {n: Value(proposed["stock_" + n]) for n in STOCK_FIELDS}
        for f in field_schema():
            if not f["name"].startswith("stock_"):
                setattr(validator, f["name"] + "_var", Value(proposed[f["name"]]))
        validator._collect_trade_mode_settings()
        from input_validation import bounded_int, finite_float
        bounded_int(proposed["max_threads"], "线程数")
        bounded_int(proposed["usdt_timeout"], "到账超时", minimum=0, maximum=86400)
        finite_float(proposed["withdraw_buffer"], "提现预留", maximum=None)
        minimum = validator._delay_var_value(validator.min_delay_var, 1000, "最小")
        maximum = validator._delay_var_value(validator.max_delay_var, 3000, "最大")
        if minimum > maximum:
            raise ValueError("最小延迟不能大于最大延迟")
        validator._rate_limit_wait_seconds_value()
        validator._normalize_exchange_proxy(str(proposed["exchange_proxy"]))
        parsed_auto = AutoLadderSettings.parse(auto) if auto is not None else None
        old = self.settings
        old_auto, _ = self.auto_ladder_settings_store.snapshot()
        self.settings = proposed
        try:
            # Persist first; hot settings only change after durable success.
            _save_authenticated_config(self.settings_file, {
                "settings": proposed, "accounts": self.account_meta,
                "auto_ladder": (parsed_auto or old_auto).as_dict(),
            })
        except Exception:
            self.settings = old
            raise
        if parsed_auto:
            self.auto_ladder_settings_store.replace(parsed_auto)
        if not self.busy:
            self.apply_settings()

    @property
    def busy(self):
        return bool(self._batch_task_active or (self.worker_thread and self.worker_thread.is_alive())
                    or self._pending_cleanup_snapshot())

    def _append_account_row(self, key, secret, addr, net, selected=True):
        self.accounts_revision = getattr(self, "accounts_revision", 0) + 1
        if any(a["api_key"] == key for a in self.accounts):
            raise ValueError("账号 API Key 重复")
        ident = account_id(key)
        meta = self.account_meta.get(ident, {})
        account = dict(api_key=key, api_secret=secret, address=addr, network=net, id=ident,
                       index_var=Value(str(len(self.accounts) + 1)),
                       selected_var=Value(bool(meta.get("selected", selected))),
                       bnb_topup_var=Value(bool(meta.get("bnb_topup", True))),
                       network_var=Value(net), status_var=Value("就绪"),
                       stop_event=threading.Event(), batch_active=False, tree_id=ident)
        self.accounts.append(account)
        return account

    def account_records(self):
        return [dict(id=a["id"], index=i, api_key=self._mask_key(a["api_key"]),
                     address=a["address"], network=self._account_network_value(a),
                     selected=bool(a["selected_var"].get()), bnb_topup=bool(a["bnb_topup_var"].get()),
                     status=str(a["status_var"].get()), active=bool(a["batch_active"]))
                for i, a in enumerate(self.accounts, 1)]

    def replace_accounts(self, rows):
        if self.busy:
            raise ValueError("任务运行或清理期间不能修改账号凭据")
        if not isinstance(rows, list) or len(rows) > 2000:
            raise ValueError("账号列表无效或超过 2000 个")
        previous = {a["id"]: a for a in self.accounts}
        normalized = []
        seen = set()
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("账号格式无效")
            existing = previous.get(row.get("id"))
            key = str(row.get("api_key") or (existing or {}).get("api_key") or "").strip()
            secret = str(row.get("api_secret") or (existing or {}).get("api_secret") or "").strip()
            if not key or not secret or len(key) > 512 or len(secret) > 1024 or key in seen:
                raise ValueError("API Key/Secret 为空、过长或重复")
            seen.add(key)
            if any(type(row.get(k, True)) is not bool for k in ["selected", "bnb_topup"]):
                raise ValueError("账号开关必须为布尔值")
            addr = str(row.get("address", (existing or {}).get("address", ""))).strip()
            net = self._normalize_account_network_text(row.get("network", ""))
            if len(addr) > 512 or len(net) > 64:
                raise ValueError("提现地址或网络过长")
            normalized.append((key, secret, addr, net, row.get("selected", True), row.get("bnb_topup", True)))
        # All validation precedes any mutation.
        from core_models import AccountEntry
        old_entries = self.account_store.accounts
        old_accounts, old_meta = self.accounts, self.account_meta
        from config_transaction import update_group
        try:
            with update_group(self.config_update_file, [self.account_store.file_path, self.settings_file]):
                self.account_store.accounts = [AccountEntry(key, secret, addr, net)
                                              for key, secret, addr, net, _, _ in normalized]
                self.account_store.save()
                self.accounts = []
                self.account_meta = {}
                for key, secret, addr, net, selected, topup in normalized:
                    a = self._append_account_row(key, secret, addr, net, selected)
                    a["bnb_topup_var"].set(topup)
                    old = previous.get(a["id"])
                    if old and (old["api_secret"], old["address"], self._account_network_value(old)) == (secret, addr, net):
                        a["status_var"].set(old["status_var"].get())
                    self.account_meta[a["id"]] = dict(selected=selected, bnb_topup=topup)
                self.save_settings()
        except Exception:
            self.account_store.accounts = old_entries
            self.accounts, self.account_meta = old_accounts, old_meta
            raise

    def selected_accounts(self, identifiers=None):
        if identifiers is None:
            result = self._get_selected_accounts()
        else:
            if not isinstance(identifiers, list) or len(set(identifiers)) != len(identifiers):
                raise ValueError("参与账号标识无效")
            by_id = {a["id"]: a for a in self.accounts}
            if any(i not in by_id for i in identifiers):
                raise ValueError("参与账号不存在")
            result = [by_id[i] for i in identifiers]
        if not result:
            raise ValueError("请至少选择一个账号")
        return result

    def execute(self, action, identifiers=None, *, options=None):
        if self.busy:
            raise ValueError("已有任务运行或清理中")
        self.apply_settings()
        accounts = self.selected_accounts(identifiers) if action != "retry_failed" else []
        if action == "run":
            options = options or {}
            self.run_selected_accounts(accounts, require_confirm=False,
                batch_enable_withdraw_override=options.get("enable_withdraw"),
                batch_enable_bnb_topup_override=options.get("enable_bnb_topup"))
        elif action == "query_assets":
            self.run_selected_accounts(accounts, batch_total_asset_only=True, require_confirm=False)
        elif action == "query_volume":
            self.run_query_monthly_trade_volume(accounts, query_options=options)
        elif action == "collect_bnb":
            self.run_selected_accounts(accounts, batch_collect_bnb_mode=True, require_confirm=False)
        elif action == "withdraw":
            self.batch_manual_withdraw(accounts, require_confirm=False)
        elif action == "redeem_earn":
            self.batch_redeem_flexible_earn(accounts, require_confirm=False)
        elif action == "earn_auto_subscribe":
            values = self._collect_trade_mode_settings(
                trade_account_type_override=base.TRADE_ACCOUNT_TYPE_EARN,
                earn_amount_override="0", earn_amount_all_override=False,
                earn_auto_subscribe_override=True)
            self.run_selected_accounts(accounts, trade_settings_override=values, require_confirm=False)
        elif action == "retry_failed":
            self.retry_last_failed_batch_operation()
        else:
            raise ValueError("不支持的任务")

    def stop_accounts(self, identifiers=None):
        if identifiers is None:
            self.stop_bot()
        else:
            for a in self.selected_accounts(identifiers):
                self._get_account_stop_event(a).set()
                self._set_account_status(a, "已请求停止")
        self.status_var.set("状态：已请求停止，等待撤单及清理确认" if self.busy else "状态：任务已停止，清理完成")

    def after(self, delay, callback, *args):
        with self._timer_lock:
            token = next(self._timer_ids)
            heapq.heappush(self._timers, (time.monotonic() + max(1, int(delay)) / 1000,
                                        token, callback, args))
            return token

    def after_cancel(self, token):
        with self._timer_lock:
            self._cancelled_timers.add(token)

    def pump(self):
        ready = []
        with self._timer_lock:
            now = time.monotonic()
            while self._timers and self._timers[0][0] <= now:
                _, token, callback, args = heapq.heappop(self._timers)
                if token in self._cancelled_timers:
                    self._cancelled_timers.discard(token)
                else:
                    ready.append((callback, args))
        for callback, args in ready:
            try:
                callback(*args)
            except Exception:
                base.logger.exception("Linux 状态回调失败")

    def winfo_exists(self):
        return True

    def _reindex_accounts(self):
        for i, a in enumerate(self.accounts, 1):
            a["index_var"].set(str(i))

    def _show_batch_result_dialog(self, **values):
        self.notices.append(values)

    def _clear_exchange_account_ui_state(self):
        self.accounts.clear()
        self.account_meta.clear()
        self.account_store.accounts = []
        self.account_store.one_to_many_addresses = []
        self.account_store.one_to_many_source_api_key = self.account_store.one_to_many_source_api_secret = ""
        self.total_asset_results.clear()
        self._current_batch_summary = self._last_batch_retry = None
        self.exchange_batch_summary_var.set("")
        self.save_settings()

    def _clear_exchange_log_widgets(self):
        self.notices.clear()

    def _rotate_result_file(self, path):
        # Completed human-readable results are bounded independently of the
        # encrypted order journal. Retain current + 3 most recent 5 MiB files.
        if not path.exists() or path.stat().st_size < 5*1024*1024:
            return
        path.with_name(path.name + ".3").unlink(missing_ok=True)
        for index in (2, 1):
            previous = path.with_name(path.name + f".{index}")
            if previous.exists():
                previous.replace(path.with_name(path.name + f".{index+1}"))
        path.replace(path.with_name(path.name + ".1"))

    def record_withdraw(self, *args):
        with self._result_file_lock:
            self._rotate_result_file(base.WITHDRAW_SUCCESS_FILE)
            return ExchangeAppAccountsMixin.record_withdraw(self, *args)

    def record_total_asset(self, *args):
        with self._result_file_lock:
            self._rotate_result_file(base.TOTAL_ASSET_RESULT_FILE)
            return ExchangeAppAccountsMixin.record_total_asset(self, *args)

    def close_idle(self):
        if self.busy or self._managed_threads_snapshot():
            raise ValueError("后台任务或清理尚未完成")
        stop_ui_bridge(self)
        self.exchange_proxy_runtime.stop()
        self._clear_current_binance_client()

    def _view_noop(self, *_args, **_kwargs):
        return None


# Only controller methods are borrowed. Tk itself is never a base class.
for _name, _value in base.ExchangeAppBase.__dict__.items():
    if not _name.startswith("__") and (callable(_value) or isinstance(_value, (staticmethod, classmethod))):
        if not hasattr(HeadlessExchange, _name):
            setattr(HeadlessExchange, _name, _value)
for _name in ("_set_account_manage_buttons_state", "_set_combo_states_for_run",
              "_refresh_account_list_hint", "_refresh_account_tree_row", "_insert_account_tree_row",
              "_apply_account_row_style", "_update_toggle_select_button_text",
              "_focus_account_list_for_paste", "_refresh_exchange_proxy_summary"):
    setattr(HeadlessExchange, _name, HeadlessExchange._view_noop)
