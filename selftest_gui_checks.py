#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import contextlib
import os
import tempfile
import time
import threading
from types import SimpleNamespace
from pathlib import Path

from core_models import OnchainPairEntry
from exchange_logging import logger
from selftest_basic_checks import drain_exchange_log_queue
from secure_delete import SecureDeleteReport
from stores import OnchainStore


@contextlib.contextmanager
def temporary_attr_overrides(patches: list[tuple[object, str, object]]):
    sentinel = object()
    previous: list[tuple[object, str, object]] = []
    try:
        for owner, attr_name, value in patches:
            before = getattr(owner, attr_name, sentinel)
            previous.append((owner, attr_name, before))
            setattr(owner, attr_name, value)
        yield
    finally:
        for owner, attr_name, before in reversed(previous):
            if before is sentinel:
                try:
                    delattr(owner, attr_name)
                except Exception:
                    pass
            else:
                setattr(owner, attr_name, before)


def pump_tk_events(app, *, timeout: float = 20.0, predicate=None) -> None:
    deadline = time.monotonic() + max(0.1, float(timeout))
    while time.monotonic() < deadline:
        app.update_idletasks()
        app.update()
        if predicate is None or predicate():
            return
        time.sleep(0.05)
    if predicate is not None and not predicate():
        raise RuntimeError("GUI 自检超时")


def run_gui_selftest_checks(checks: list[str]) -> None:
    previous_lock_bypass = os.environ.get("XIAOJUN_APP_LOCK_BYPASS")
    os.environ["XIAOJUN_APP_LOCK_BYPASS"] = "1"
    try:
        _run_gui_selftest_checks_unlocked(checks)
    finally:
        if previous_lock_bypass is None:
            os.environ.pop("XIAOJUN_APP_LOCK_BYPASS", None)
        else:
            os.environ["XIAOJUN_APP_LOCK_BYPASS"] = previous_lock_bypass


def _run_gui_selftest_checks_unlocked(checks: list[str]) -> None:
    from tkinter import messagebox

    import exchange_app_accounts
    import exchange_app_auto_ladder
    import exchange_app_base
    import exchange_app_config
    import onchain_imports
    import page_onchain_base
    from exchange_app import App

    dialogs: list[tuple[str, str, str]] = []

    def _dialog_stub(kind: str):
        def _inner(title, message, **_kwargs):
            dialogs.append((kind, str(title), str(message)))
            return True
        return _inner

    with tempfile.TemporaryDirectory(prefix="xiaojun-selftest-gui-") as tmpdir:
        tmp_root = Path(tmpdir)
        accounts_path = tmp_root / "accounts.json"
        exchange_journal_path = tmp_root / "exchange_operations.json"
        strategy_path = tmp_root / "exchange_strategy_settings.json"
        auto_ladder_path = tmp_root / "auto_ladder_settings.json"
        proxy_path = tmp_root / "exchange_proxy_settings.json"
        onchain_path = tmp_root / "onchain.json"
        withdraw_result_path = tmp_root / "withdraw_success.txt"
        total_asset_result_path = tmp_root / "total_asset_result.txt"
        patches = [
            (exchange_app_base.ExchangeAppBase, "update_ip", lambda self, schedule_next=True: None),
            (exchange_app_base, "DATA_FILE", accounts_path),
            (exchange_app_base, "EXCHANGE_OPERATION_JOURNAL_FILE", exchange_journal_path),
            (exchange_app_base, "STRATEGY_CONFIG_FILE", strategy_path),
            (exchange_app_auto_ladder, "AUTO_LADDER_CONFIG_FILE", auto_ladder_path),
            (exchange_app_base, "EXCHANGE_PROXY_CONFIG_FILE", proxy_path),
            (exchange_app_accounts, "DATA_FILE", accounts_path),
            (exchange_app_accounts, "EXCHANGE_OPERATION_JOURNAL_FILE", exchange_journal_path),
            (exchange_app_accounts, "STRATEGY_CONFIG_FILE", strategy_path),
            (exchange_app_accounts, "WITHDRAW_SUCCESS_FILE", withdraw_result_path),
            (exchange_app_accounts, "TOTAL_ASSET_RESULT_FILE", total_asset_result_path),
            (exchange_app_accounts, "purge_exchange_file_logs", lambda: SecureDeleteReport()),
            (exchange_app_accounts, "clear_exchange_log_queue", lambda: None),
            (exchange_app_config, "STRATEGY_CONFIG_FILE", strategy_path),
            (exchange_app_config, "EXCHANGE_PROXY_CONFIG_FILE", proxy_path),
            (page_onchain_base, "ONCHAIN_DATA_FILE", onchain_path),
            (onchain_imports, "ONCHAIN_DATA_FILE", onchain_path),
            (messagebox, "showinfo", _dialog_stub("info")),
            (messagebox, "showwarning", _dialog_stub("warning")),
            (messagebox, "showerror", _dialog_stub("error")),
            (messagebox, "askyesno", lambda *_args, **_kwargs: True),
        ]
        with temporary_attr_overrides(patches):
            app = None
            try:
                app = App()
                app.withdraw()
                pump_tk_events(app, timeout=5.0)
                if int(app.account_tree.cget("height")) != 10:
                    raise RuntimeError("GUI 自检失败：交易所账号列表高度不是10行")
                account_columns = tuple(app.account_tree.cget("columns"))
                if account_columns[-3:] != ("network", "bnb_topup", "status"):
                    raise RuntimeError(f"GUI 自检失败：预买BNB列位置异常（{account_columns}）")
                if str(app.account_tree.heading("bnb_topup", "text")) != "BNB预买全选":
                    raise RuntimeError("GUI 自检失败：预买BNB全选表头缺失")
                topup_probe = app._append_account_row("A" * 32, "b" * 32, "", "BSC")
                if not bool(topup_probe["bnb_topup_var"].get()):
                    raise RuntimeError("GUI 自检失败：新账号未默认勾选预购买BNB")
                topup_probe["bnb_topup_var"].set(False)
                if app._account_tree_values(topup_probe)[5] != "":
                    raise RuntimeError("GUI 自检失败：单账号预购买BNB取消未刷新")
                app.select_all_bnb_topup_accounts()
                if app._account_tree_values(topup_probe)[5] != "✓":
                    raise RuntimeError("GUI 自检失败：预购买BNB全选未生效")
                app._delete_accounts([topup_probe], require_confirm=False)
                checks.append("exchange-account-bnb-topup-control-gui=ok")
                if str(app.btn_destroy_all_accounts.cget("text")) != "销毁所有":
                    raise RuntimeError("GUI 自检失败：交易所账号销毁按钮缺失")
                if str(app.btn_destroy_all_accounts.cget("fg")).lower() != "#c62828":
                    raise RuntimeError("GUI 自检失败：交易所账号销毁按钮未使用红色字体")
                batch_controls = list(app.btn_paste_accounts.master.pack_slaves())
                paste_index = batch_controls.index(app.btn_paste_accounts)
                monthly_volume_index = batch_controls.index(app.btn_query_monthly_volume)
                if str(app.btn_query_monthly_volume.cget("text")) != "查询月成交额":
                    raise RuntimeError("GUI 自检失败：月成交额查询按钮缺失")
                if monthly_volume_index != paste_index + 1:
                    raise RuntimeError("GUI 自检失败：月成交额查询按钮未紧随粘贴导入")
                checks.append("exchange-monthly-volume-control-gui=ok")
                context_menu_labels = [
                    str(app.account_row_menu.entrycget(index, "label") or "")
                    for index in range(int(app.account_row_menu.index("end") or 0) + 1)
                    if str(app.account_row_menu.type(index)) == "command"
                ]
                if "自动申购" not in context_menu_labels:
                    raise RuntimeError("GUI 自检失败：账号右键自动申购菜单缺失")
                if "查询月成交额" not in context_menu_labels:
                    raise RuntimeError("GUI 自检失败：账号右键月成交额查询菜单缺失")
                header_children = list(app.btn_clear_account_search.master.pack_slaves())
                if header_children.index(app.btn_destroy_all_accounts) != header_children.index(app.btn_clear_account_search) + 1:
                    raise RuntimeError("GUI 自检失败：交易所账号销毁按钮未紧随清空按钮")
                checks.append("exchange-destroy-control-gui=ok")

                def _strategy_row3_labels() -> list[str]:
                    labels: list[str] = []
                    for widget in app._strategy_row3_right.pack_slaves():
                        try:
                            text = str(widget.cget("text") or "")
                        except Exception:
                            text = ""
                        if text:
                            labels.append(text)
                    return labels

                if str(app.rate_limit_wait_var.get()) != "10":
                    raise RuntimeError("GUI 自检失败：限流等待默认值不是10秒")
                expected_gate_modes = [
                    ("现货", "市价", True),
                    ("现货", "闪兑", True),
                    ("现货", "挂单", True),
                    ("现货", "溢价单", True),
                    ("现货", "分仓挂单", True),
                    ("合约", "市价", True),
                    ("理财", "市价", True),
                ]
                for account_type, trade_mode, expected_visible in expected_gate_modes:
                    app.trade_account_type_var.set(account_type)
                    app.trade_mode_var.set(trade_mode)
                    app._refresh_strategy_panel_layout()
                    pump_tk_events(app, timeout=2.0)
                    labels = _strategy_row3_labels()
                    visible = "限流等待(秒):" in labels
                    if visible != expected_visible:
                        raise RuntimeError(
                            f"GUI 自检失败：{account_type}/{trade_mode} 限流等待显示状态异常（{labels}）"
                        )
                    if visible and "提现预留:" in labels and labels.index("限流等待(秒):") > labels.index("提现预留:"):
                        raise RuntimeError("GUI 自检失败：限流等待未放在提现预留前")
                app.trade_account_type_var.set("现货")
                app.trade_mode_var.set("市价")
                app.market_stop_mode_var.set("剩余BNB")
                app.bnb_fee_stop_var.set("0.01")
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                if tuple(app.market_stop_mode_combo.cget("values")) != ("现货轮次", "剩余BNB"):
                    raise RuntimeError("GUI 自检失败：市价停止方式不是固定二选一")
                market_bnb_settings = app._collect_trade_mode_settings()
                if (
                    market_bnb_settings.get("market_stop_mode") != "剩余BNB"
                    or str(market_bnb_settings.get("bnb_fee_stop_value")) != "0.01"
                ):
                    raise RuntimeError(f"GUI 自检失败：市价剩余BNB停止参数未生效（{market_bnb_settings}）")
                app.market_stop_mode_var.set("现货轮次")
                app.market_stagger_enabled_var.set(True)
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                market_controls = [
                    str(widget.cget("text") or "")
                    for widget in app._strategy_row3_left.pack_slaves()
                    if "text" in widget.keys()
                ]
                if "市价交错执行" not in market_controls:
                    raise RuntimeError(f"GUI 自检失败：市价交错执行控件缺失（{market_controls}）")
                if app._collect_trade_mode_settings().get("market_stagger_enabled") is not True:
                    raise RuntimeError("GUI 自检失败：市价交错执行参数未生效")
                app.market_stagger_enabled_var.set(False)
                checks.append("exchange-market-stop-mode-gui=ok")
                app.trade_account_type_var.set("现货")
                app.trade_mode_var.set("分仓挂单")
                app.spot_rounds_var.set(1)
                app.bnb_fee_stop_var.set("0")
                app.ladder_layout_var.set("买2卖1")
                app.ladder_min_pool_notional_var.set("50")
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                ladder_labels = [
                    str(widget.cget("text") or "")
                    for widget in app._strategy_row3_left.pack_slaves()
                    if "text" in widget.keys()
                ]
                if "档位模式:" not in ladder_labels or "最小分仓成交额:" not in ladder_labels:
                    raise RuntimeError(f"GUI 自检失败：分仓挂单控件缺失（{ladder_labels}）")
                ladder_settings = app._collect_trade_mode_settings()
                if (
                    ladder_settings.get("ladder_layout") != "买2卖1"
                    or str(ladder_settings.get("ladder_min_pool_notional_value")) != "50"
                ):
                    raise RuntimeError(f"GUI 自检失败：分仓挂单参数未生效（{ladder_settings}）")
                checks.append("exchange-ladder-control-gui=ok")
                app.trade_mode_var.set("自动分仓挂单")
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                row2_widgets = list(app._strategy_row2_left.pack_slaves())
                if app.btn_auto_ladder_settings not in row2_widgets or str(app.btn_auto_ladder_settings.cget("state")) != "normal":
                    raise RuntimeError("GUI 自检失败：自动分仓弹窗按钮缺失或不可用")
                auto_settings = app._collect_trade_mode_settings()
                if auto_settings.get("ladder_layout") != "买2卖1":
                    raise RuntimeError("GUI 自检失败：自动分仓未复用主界面的档位")
                app.open_auto_ladder_settings()
                window = app.auto_ladder_window
                if not window.winfo_exists():
                    raise RuntimeError("GUI 自检失败：自动分仓设置弹窗未显示")
                field_names = {name for name, _label in exchange_app_auto_ladder.FIELDS}
                if set(exchange_app_auto_ladder.FIELD_HELP) != field_names:
                    raise RuntimeError("GUI 自检失败：自动分仓参数说明不完整")
                help_bubbles = window._auto_ladder_help_bubbles
                if len(help_bubbles) != len(field_names):
                    raise RuntimeError("GUI 自检失败：自动分仓悬浮说明未绑定到全部参数")
                help_bubbles[0]._show()
                if help_bubbles[0]._bubble is None or not help_bubbles[0]._bubble.winfo_exists():
                    raise RuntimeError("GUI 自检失败：自动分仓悬浮说明未显示")
                help_bubbles[0]._hide()
                controls = window.winfo_children()[0].winfo_children()
                buttons = {widget.cget("text"): widget for widget in controls if widget.winfo_class() == "TButton"}
                if not {"分享策略", "导入策略", "保存并运行"}.issubset(buttons):
                    raise RuntimeError("GUI 自检失败：自动分仓分享、导入或保存按钮缺失")
                volume_entry = next(widget for widget in controls
                                    if widget.winfo_class() == "TEntry" and int(widget.grid_info()["row"]) == 2)
                clipboard = {"text": ""}
                with temporary_attr_overrides([
                    (app, "clipboard_clear", lambda: clipboard.update(text="")),
                    (app, "clipboard_append", lambda value: clipboard.update(text=value)),
                    (app, "clipboard_get", lambda: clipboard["text"]),
                ]):
                    volume_entry.delete(0, "end")
                    volume_entry.insert(0, "250000")
                    buttons["分享策略"].invoke()
                    shared = clipboard["text"]
                    if exchange_app_auto_ladder.import_auto_ladder_settings(shared).start_volume != 250000:
                        raise RuntimeError("GUI 自检失败：分享未复制当前表单参数")
                    if app.auto_ladder_settings_store.snapshot()[1] != 0:
                        raise RuntimeError("GUI 自检失败：分享时意外更新了运行参数")
                    volume_entry.delete(0, "end")
                    volume_entry.insert(0, "300000")
                    clipboard["text"] = "无效策略"
                    buttons["导入策略"].invoke()
                    if volume_entry.get() != "300000" or app.auto_ladder_settings_store.snapshot()[1] != 0:
                        raise RuntimeError("GUI 自检失败：无效策略导入改动了现有参数")
                    if not dialogs or dialogs[-1][:2] != ("error", "导入失败"):
                        raise RuntimeError("GUI 自检失败：无效策略导入未提示错误")
                    dialogs.pop()
                    clipboard["text"] = shared
                    buttons["导入策略"].invoke()
                    if volume_entry.get() != "250000" or app.auto_ladder_settings_store.snapshot()[1] != 0:
                        raise RuntimeError("GUI 自检失败：导入策略未正确回填或提前生效")
                buttons["保存并运行"].invoke()
                saved_settings, saved_version = app.auto_ladder_settings_store.snapshot()
                if not auto_ladder_path.exists() or saved_version != 1 or saved_settings.start_volume != 250000:
                    raise RuntimeError("GUI 自检失败：自动分仓设置未原子保存并更新")
                app._set_combo_states_for_run(True)
                try:
                    if str(app.trade_mode_combo.cget("state")) != "disabled":
                        raise RuntimeError("GUI 自检失败：运行中主界面模式未锁定")
                    if str(app.btn_auto_ladder_settings.cget("state")) != "normal":
                        raise RuntimeError("GUI 自检失败：运行中自动分仓设置按钮不可用")
                    app.btn_auto_ladder_settings.invoke()
                    if not app.auto_ladder_window.winfo_exists():
                        raise RuntimeError("GUI 自检失败：运行中无法打开自动分仓设置")
                    app.auto_ladder_window.destroy()
                finally:
                    app._set_combo_states_for_run(False)
                checks.append("exchange-auto-ladder-control-gui=ok")
                app.trade_account_type_var.set("理财")
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                earn_controls = [
                    str(widget.cget("text") or "")
                    for widget in app._strategy_row2_left.pack_slaves()
                    if "text" in widget.keys()
                ]
                if "全部" not in earn_controls or "自动申购" not in earn_controls:
                    raise RuntimeError(f"GUI 自检失败：理财自动申购控件缺失（{earn_controls}）")
                if earn_controls.index("自动申购") != earn_controls.index("全部") + 1:
                    raise RuntimeError("GUI 自检失败：自动申购未紧随全部选项")
                app.earn_amount_var.set("")
                app.earn_auto_subscribe_var.set(True)
                earn_settings = app._collect_trade_mode_settings()
                if (
                    earn_settings.get("earn_auto_subscribe") is not True
                    or earn_settings.get("earn_auto_subscribe_only") is not True
                ):
                    raise RuntimeError("GUI 自检失败：理财仅开启自动申购设置未生效")
                checks.append("earn-auto-subscribe-control-gui=ok")
                app.trade_account_type_var.set("现货")
                app.trade_mode_var.set("市价")
                app.rate_limit_wait_var.set("17")
                app._refresh_strategy_panel_layout()
                pump_tk_events(app, timeout=2.0)
                checks.append("exchange-rate-limit-control-gui=ok")

                drain_exchange_log_queue()
                logger.info("GUI日志拆分普通日志自检")
                logger.error("GUI日志拆分报错日志自检")
                pump_tk_events(
                    app,
                    timeout=5.0,
                    predicate=lambda: "GUI日志拆分报错日志自检" in app.text_error_log.get("1.0", "end"),
                )
                run_log_text = app.text_log.get("1.0", "end")
                error_log_text = app.text_error_log.get("1.0", "end")
                if "GUI日志拆分普通日志自检" not in run_log_text or "GUI日志拆分报错日志自检" not in run_log_text:
                    raise RuntimeError("GUI 自检失败：运行日志未显示完整日志")
                if "GUI日志拆分报错日志自检" not in error_log_text:
                    raise RuntimeError("GUI 自检失败：报错日志未显示错误日志")
                if "GUI日志拆分普通日志自检" in error_log_text:
                    raise RuntimeError("GUI 自检失败：普通日志错误进入报错日志")
                checks.append("exchange-log-split-gui=ok")

                app.api_key_var.set("selftest-api-key")
                app.api_secret_var.set("selftest-api-secret")
                app.withdraw_addr_var.set("selftest-withdraw-address")
                app.spot_symbol_var.set("BNBUSDT")
                app.save_strategy_config()
                if not strategy_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成交易所策略配置文件")
                if not proxy_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成交易所代理配置文件")
                app._append_account_row(
                    "selftest-account-key",
                    "selftest-account-secret",
                    "selftest-account-address",
                    "BSC",
                )
                app._save_accounts_silently()
                app.exchange_operation_journal.begin("exchange-order", "selftest", {"order_id": "selftest-order"})
                withdraw_result_path.write_text("selftest-withdraw", encoding="utf-8")
                total_asset_result_path.write_text("selftest-asset", encoding="utf-8")
                (tmp_root / "exchange_ss_proxy_1_selftest.json").write_text("selftest-proxy", encoding="utf-8")
                (tmp_root / "exchange_proxy_runtime_1.log").write_text("selftest-runtime", encoding="utf-8")
                confirm_destroy = messagebox.askyesno
                messagebox.askyesno = lambda *_args, **_kwargs: False
                app.destroy_all_exchange_accounts()
                if not accounts_path.exists() or not exchange_journal_path.exists():
                    raise RuntimeError("GUI 自检失败：确认框取消后仍执行了交易所账号销毁")
                messagebox.askyesno = confirm_destroy
                app.destroy_all_exchange_accounts()
                if app.accounts or accounts_path.exists() or exchange_journal_path.exists():
                    raise RuntimeError("GUI 自检失败：交易所账号或恢复账本未销毁")
                if withdraw_result_path.exists() or total_asset_result_path.exists():
                    raise RuntimeError("GUI 自检失败：交易所结果文件未销毁")
                if (tmp_root / "exchange_ss_proxy_1_selftest.json").exists() or (tmp_root / "exchange_proxy_runtime_1.log").exists():
                    raise RuntimeError("GUI 自检失败：交易所代理运行时文件未销毁")
                strategy_payload, _recovered = exchange_app_config._load_authenticated_config(strategy_path)
                if any(strategy_payload.get(field) for field in ("api_key", "api_secret", "withdraw_address")):
                    raise RuntimeError("GUI 自检失败：共享策略仍保留交易所账号凭据")
                if strategy_payload.get("spot_symbol") != "BNBUSDT" or not proxy_path.exists():
                    raise RuntimeError("GUI 自检失败：销毁误删共享策略参数或代理配置")
                if strategy_payload.get("rate_limit_wait_sec") != 17:
                    raise RuntimeError("GUI 自检失败：限流等待配置未保存")
                if strategy_payload.get("market_stop_mode") != "现货轮次":
                    raise RuntimeError("GUI 自检失败：市价停止方式配置未保存")
                if strategy_payload.get("earn_auto_subscribe") is not True:
                    raise RuntimeError("GUI 自检失败：理财自动申购配置未保存")
                checks.append("exchange-destroy-execution-gui=ok")

                app._show_main_page("onchain")
                pump_tk_events(app, timeout=5.0, predicate=lambda: getattr(app, "onchain_page", None) is not None)
                page = app.onchain_page
                if page is None:
                    raise RuntimeError("GUI 自检失败：链上页面未成功加载")
                if int(page.tree.cget("height")) != 10:
                    raise RuntimeError("GUI 自检失败：链上账号列表高度不是10行")
                if str(page.log_tree.winfo_class()) != "Text" or str(page.error_log_tree.winfo_class()) != "Text":
                    raise RuntimeError("GUI 自检失败：链上日志未使用 Text 控件")
                if page._confirmation_worker_limit() < 1:
                    raise RuntimeError("GUI 自检失败：链上确认线程池限制异常")
                confirm_probe: list[str] = []
                confirm_future = page._submit_confirmation_task(lambda: confirm_probe.append("ok"), name="selftest-confirm")
                if confirm_future is None:
                    raise RuntimeError("GUI 自检失败：链上确认任务提交失败")
                confirm_future.result(timeout=3.0)
                if confirm_probe != ["ok"]:
                    raise RuntimeError("GUI 自检失败：链上确认任务未执行")
                checks.append("onchain-confirm-executor-gui=ok")

                def _text_log_lines(widget) -> list[str]:
                    return [
                        line
                        for line in str(widget.get("1.0", "end") or "").splitlines()
                        if line.strip()
                    ]

                page.log("链上日志拆分普通日志自检")
                page.log("链上日志拆分失败 0 普通汇总自检")
                page.log("链上日志拆分失败：报错日志自检")
                page.log("链上金额高亮自检：金额=USDT 12.34")
                pump_tk_events(
                    app,
                    timeout=5.0,
                    predicate=lambda: any(
                        "链上日志拆分失败：报错日志自检" in item
                        for item in _text_log_lines(page.error_log_tree)
                    ),
                )

                onchain_run_logs = _text_log_lines(page.log_tree)
                onchain_error_logs = _text_log_lines(page.error_log_tree)
                onchain_time_values = [line.split(" ", 1)[0] for line in onchain_run_logs]
                if any(time_text[:4].isdigit() and time_text[4:5] == "-" for time_text in onchain_time_values):
                    raise RuntimeError("GUI 自检失败：链上日志时间仍显示年月日")
                if any(
                    time_text and (len(time_text) != 8 or time_text[2] != ":" or time_text[5] != ":")
                    for time_text in onchain_time_values
                ):
                    raise RuntimeError("GUI 自检失败：链上日志时间未使用时分秒")
                if not any("链上日志拆分普通日志自检" in item for item in onchain_run_logs):
                    raise RuntimeError("GUI 自检失败：链上运行日志未显示普通日志")
                if not any("链上日志拆分失败：报错日志自检" in item for item in onchain_run_logs):
                    raise RuntimeError("GUI 自检失败：链上运行日志未显示失败日志")
                if not any("链上日志拆分失败：报错日志自检" in item for item in onchain_error_logs):
                    raise RuntimeError("GUI 自检失败：链上报错日志未显示失败日志")
                if any("链上日志拆分普通日志自检" in item for item in onchain_error_logs):
                    raise RuntimeError("GUI 自检失败：链上普通日志错误进入报错日志")
                if any("链上日志拆分失败 0 普通汇总自检" in item for item in onchain_error_logs):
                    raise RuntimeError("GUI 自检失败：链上失败0日志错误进入报错日志")
                if not page.log_tree.tag_ranges("log_amount_value"):
                    raise RuntimeError("GUI 自检失败：链上 Text 日志金额未高亮")
                checks.append("onchain-log-split-gui=ok")

                if page._amount_mode() != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对多默认数量模式不是“全部”")
                page.mode_var.set(page.MODE_1M)
                pump_tk_events(app, timeout=5.0)
                if page._amount_mode() != page.AMOUNT_MODE_FIXED or page.amount_var.get().strip():
                    raise RuntimeError("GUI 自检失败：1对多默认数量模式异常")
                page.amount_var.set("0.01")
                page.mode_var.set(page.MODE_M1)
                pump_tk_events(app, timeout=5.0)
                if page._amount_mode() != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对1默认数量模式不是“全部”")
                page.mode_var.set(page.MODE_1M)
                pump_tk_events(app, timeout=5.0)
                if page.amount_var.get().strip() != "0.01":
                    raise RuntimeError("GUI 自检失败：模式切换后1对多数量未保持独立")

                page.network_var.set("BSC")
                page.mode_var.set(page.MODE_M1)
                pump_tk_events(app, timeout=5.0)
                page.save_all()
                if not onchain_path.exists():
                    raise RuntimeError("GUI 自检失败：未生成链上配置文件")
                saved_store = OnchainStore(onchain_path)
                saved_store.load()
                if saved_store.settings.mode_amounts.get("multi_to_multi", {}).get("amount_mode") != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对多数量配置未保存")
                if saved_store.settings.mode_amounts.get("one_to_many", {}).get("amount") != "0.01":
                    raise RuntimeError("GUI 自检失败：1对多独立数量配置未保存")
                if saved_store.settings.mode_amounts.get("many_to_one", {}).get("amount_mode") != page.AMOUNT_MODE_ALL:
                    raise RuntimeError("GUI 自检失败：多对1独立数量配置未保存")

                def _addr(n: int) -> str:
                    return f"0x{n:040x}"

                def _assert_onchain_search(query: str, expected_row_id: str, label: str) -> None:
                    page.onchain_search_var.set(query)
                    page.search_onchain_table()
                    pump_tk_events(app, timeout=5.0)
                    selected = tuple(page.tree.selection())
                    if not selected or selected[0] != expected_row_id:
                        raise RuntimeError(
                            f"GUI 自检失败：链上搜索未定位到{label}，期望 {expected_row_id}，实际 {selected}"
                        )

                source_a = "0x" + "1" * 64
                source_b = "2" * 64
                source_c = "0x" + "3" * 64
                source_d = "0x" + "4" * 64
                source_e = "5" * 64
                source_a_addr = page.client.address_from_private_key(source_a)
                source_e_addr = page.client.address_from_private_key("0x" + source_e)

                page.store.multi_to_multi_pairs = [
                    OnchainPairEntry(source=source_a, target=_addr(101)),
                    OnchainPairEntry(source=source_b, target=_addr(102)),
                ]
                page.m2m_import_drafts = [{"source": source_c, "target": _addr(103)}]
                page.mode_var.set(page.MODE_M2M)
                pump_tk_events(app, timeout=5.0)
                _assert_onchain_search(_addr(102), "onchain_row_2", "多对多接收地址")
                _assert_onchain_search(source_a_addr, "onchain_row_1", "多对多源钱包地址")
                _assert_onchain_search(source_c, "onchain_draft_3", "多对多待补齐私钥")

                page.store.one_to_many_addresses = [_addr(201), _addr(202)]
                page.source_credential_var.set(source_a)
                page.mode_var.set(page.MODE_1M)
                pump_tk_events(app, timeout=5.0)
                _assert_onchain_search(_addr(202), "onchain_row_2", "1对多收款地址")
                _assert_onchain_search(source_a_addr, "onchain_row_1", "1对多源钱包地址")

                page.store.many_to_one_sources = [source_d, source_e]
                page.target_address_var.set(_addr(301))
                page.mode_var.set(page.MODE_M1)
                pump_tk_events(app, timeout=5.0)
                _assert_onchain_search(source_e_addr, "onchain_row_2", "多对1源钱包地址")
                _assert_onchain_search("0x" + source_e, "onchain_row_2", "多对1源钱包私钥")
                checks.append("onchain-search-gui=ok")

                page.open_wallet_generator()
                pump_tk_events(app, timeout=5.0)
                page.wallet_generate_count_var.set("2")
                page.generate_wallets()
                pump_tk_events(
                    app,
                    timeout=20.0,
                    predicate=lambda: len(getattr(page, "generated_wallets", [])) >= 2
                    and not any(t.is_alive() for t in page._managed_threads_snapshot()),
                )
                if len(page.generated_wallets) != 2:
                    raise RuntimeError("GUI 自检失败：钱包生成数量异常")
                error_dialogs = [item for item in dialogs if item[0] == "error"]
                if error_dialogs:
                    raise RuntimeError(f"GUI 自检失败：出现错误弹窗 {error_dialogs[0][2]}")

                # Keep a real Tk window alive past the former 2.5 second exit deadline.
                worker_release = threading.Event()
                cleanup_done = threading.Event()
                cleanup = SimpleNamespace(cleanup_confirmed=False, cleanup_error="自检待清理", stop_cleanup_requested=False,
                                          c=SimpleNamespace(close=lambda: None))
                def complete_cleanup():
                    if app._close_finalized:
                        raise RuntimeError("GUI 自检失败：清理前已经最终退出")
                    cleanup.cleanup_confirmed = True
                    cleanup_done.set()
                    app._record_strategy_cleanup(cleanup)
                    return True
                cleanup.retry_stop_cleanup = complete_cleanup
                app._record_strategy_cleanup(cleanup)
                app._start_managed_thread(lambda: worker_release.wait(8), name="selftest-close-worker")
                close_started = time.monotonic()
                try:
                    app._on_close()
                    pump_tk_events(app, timeout=5, predicate=lambda: time.monotonic() - close_started >= 2.8)
                    if app._close_finalized or not app.winfo_exists() or cleanup_done.is_set():
                        raise RuntimeError("GUI 自检失败：后台任务结束前退出或开始清理")
                finally:
                    worker_release.set()
                pump_tk_events(app, timeout=8, predicate=lambda: app._close_finalized)
                if not cleanup_done.is_set():
                    raise RuntimeError("GUI 自检失败：未完成状态清理便退出")
                checks.append("close-after-cleanup-gui=ok")
            finally:
                if app is not None:
                    try:
                        app.destroy()
                    except Exception:
                        pass

    checks.append("gui-smoke=ok")
