"""Standalone exchange execution process, independent of browser sessions."""
from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import io
import math
import json
import os
from pathlib import Path
import queue
import secrets
import signal
import socketserver
import threading
import time
import uuid
from collections import deque

from .bootstrap import configure_data_directory
from .ipc import receive, MAX_MESSAGE, RemoteError

TASKS = {"run", "query_assets", "query_volume", "collect_bnb", "withdraw",
         "redeem_earn", "earn_auto_subscribe", "retry_failed"}
READ_METHODS = {"state", "session"}


class Service:
    def __init__(self, directory, setup_token_file):
        self.directory = configure_data_directory(directory)
        # Fail closed: local developer bypasses are never valid on a server.
        os.environ.pop("XIAOJUN_APP_LOCK_BYPASS", None)
        os.environ.pop("XIAOJUN_APP_PASSWORD", None)
        from app_lock import APP_LOCK
        self.lock = APP_LOCK
        self.lock.lock()
        self.setup_token_file = Path(setup_token_file)
        self.runtime = None
        self.sessions = {}
        self.login_attempts = {}
        self.commands = {}
        self.command_file = self.directory / "linux_commands.json"
        self.run_file = self.directory / "linux_run.json"
        self.logs = deque(maxlen=600)
        self.log_lock = threading.RLock()
        self.log_seq = 0
        self.boot = uuid.uuid4().hex
        self.active_run = None
        self._checkpoint_lock = threading.RLock()
        self._checkpoint_retry_at = 0
        self.interrupted = None
        self.dialog_context = threading.local()
        self.maintenance_at = 0
        self.maintenance_result = {}
        self.storage_check_at = 0
        self._redact_runtime = None
        self._redact_accounts = None
        self._redact_pattern = None
        self._install_dialogs()

    def _install_dialogs(self):
        # Executed only inside the dedicated worker, never at module import.
        from tkinter import messagebox
        def notice(_title, message, **_kwargs):
            self.add_log(message)
            return True
        def error(_title, message, **_kwargs):
            if getattr(self.dialog_context, "control", False):
                raise ValueError(str(message))
            self.add_log(message)
            return False
        def warning(title, message, **kwargs):
            if getattr(self.dialog_context, "control", False):
                return error(title, message, **kwargs)
            return notice(title, message, **kwargs)
        messagebox.showinfo = notice
        messagebox.showwarning = warning
        messagebox.showerror = error
        messagebox.askyesno = lambda *_a, **_k: bool(getattr(self.dialog_context, "confirmed", False))

    def redact(self, text):
        from sensitive_data import redact_sensitive_text
        raw = str(text)
        if self.runtime:
            version = (id(self.runtime.accounts), getattr(self.runtime, "accounts_revision", len(self.runtime.accounts)))
            if self._redact_runtime is not self.runtime or self._redact_accounts != version:
                import re
                secrets_to_mask = {a[name] for a in self.runtime.accounts for name in ("api_key", "api_secret") if a[name]}
                self._redact_pattern = re.compile("|".join(re.escape(s) for s in sorted(secrets_to_mask, key=len, reverse=True))) if secrets_to_mask else None
                self._redact_runtime, self._redact_accounts = self.runtime, version
            if self._redact_pattern is not None:
                raw = self._redact_pattern.sub("***", raw)
        return redact_sensitive_text(raw)[:1200]

    def add_log(self, text, *, persist=True):
        from shared_utils import log_amount_value_spans
        message = self.redact(text)
        if persist:
            from exchange_logging import persist_exchange_message
            persist_exchange_message(message)
        with self.log_lock:
            self.log_seq += 1
            self.logs.append(dict(seq=self.log_seq, text=message,
                                  amount_spans=log_amount_value_spans(message)))

    def _password(self, value):
        if not isinstance(value, str) or not value or len(value) > 1024:
            raise RemoteError("解锁密码格式无效", 400)
        return value

    def _throttle(self, peer):
        # Bound cardinality, and aggregate attacks spanning different addresses.
        now = time.monotonic()
        self.login_attempts = {k: [t for t in v if now-t < 300]
                               for k, v in self.login_attempts.items() if any(now-t < 300 for t in v)}
        for key, maximum in ((str(peer)[:128], 8), ("*", 40)):
            times = self.login_attempts.setdefault(key, [])
            if len(times) >= maximum:
                raise RemoteError("密码尝试过多，请 5 分钟后重试", 429)
        for key in (str(peer)[:128], "*"):
            self.login_attempts[key].append(now)

    def verify_password(self, password, peer):
        self._throttle(peer)
        try:
            self.lock.unlock(self._password(password))
        except Exception as exc:
            raise RemoteError("解锁失败，请检查密码或服务端配置", 401) from exc

    def _load_runtime(self):
        if self.runtime:
            return
        from .runtime import HeadlessExchange
        import exchange_logging
        import logging
        # A stalled management view must never block a trading thread or grow
        # its in-memory UI log queue without a limit.
        class BoundedLogHandler(logging.Handler):
            def emit(self, record):
                entry = exchange_logging.TkLogEntry(self.format(record), levelno=record.levelno, levelname=record.levelname)
                try:
                    exchange_logging.log_queue.put_nowait(entry)
                except queue.Full:
                    try:
                        exchange_logging.log_queue.get_nowait()
                        exchange_logging.log_queue.put_nowait(entry)
                    except (queue.Empty, queue.Full):
                        pass
        with exchange_logging.log_queue.mutex:
            exchange_logging.log_queue.maxsize = 2000
        for handler in list(exchange_logging.logger.handlers):
            if isinstance(handler, exchange_logging.TkLogHandler) or getattr(handler, "linux_ui_log", False):
                bounded = BoundedLogHandler()
                bounded.setFormatter(handler.formatter)
                bounded.linux_ui_log = True
                exchange_logging.logger.removeHandler(handler)
                exchange_logging.logger.addHandler(bounded)
        from stores import _load_authenticated_config, config_or_backup_exists
        runtime = HeadlessExchange()
        try:
            if config_or_backup_exists(self.command_file):
                self.commands = _load_authenticated_config(self.command_file)[0]
            if config_or_backup_exists(self.run_file):
                saved = _load_authenticated_config(self.run_file)[0]
                if saved.get("active"):
                    self.interrupted = saved
                    runtime.status_var.set("状态：上次服务中断，请确认交易所订单后重新执行；不会自动重下单")
        except Exception:
            runtime.close_idle()
            raise
        self.runtime = runtime
        runtime.batch_checkpoint = self._checkpoint_account

    def _checkpoint_account(self, account, success):
        from stores import _save_authenticated_config
        from .runtime import account_id
        with self._checkpoint_lock:
            if not self.active_run:
                return
            self.active_run.setdefault("results", {})[account_id(account["api_key"])] = bool(success)
            try:
                _save_authenticated_config(self.run_file, self.active_run, strict_backup=True)
            except OSError:
                self.add_log("任务检查点保存失败，已请求停止；请检查磁盘和文件权限后核对交易所记录")
                self.runtime.stop_accounts()

    def prepare_shutdown(self, signum):
        """Record external interruption before graceful cancellation finishes."""
        import logging
        from contextlib import closing
        from daily_logging import BeijingFormatter, DailyFileHandler
        name = signal.Signals(signum).name
        active = bool(self.runtime and self.runtime.busy)
        message = f"服务收到 {name} 停止信号；任务运行中={active}，进程={os.getpid()}，线程数={threading.active_count()}"
        # Also available while locked, before exchange_logging is imported.
        try:
            with closing(DailyFileHandler(self.directory / "logs" / "service_runtime.log", prefix="service_runtime")) as handler:
                handler.setFormatter(BeijingFormatter("%(asctime)s [%(levelname)s] %(message)s"))
                handler.handle(logging.LogRecord("service.runtime", logging.WARNING, __file__, 0, message, (), None))
        except OSError:
            print(message, flush=True)  # systemd journal remains available.
        with self._checkpoint_lock:
            if self.active_run and active:
                from stores import _save_authenticated_config
                saved = {**self.active_run, "interrupted_at": time.time(), "reason": name}
                self.interrupted = saved
                # pump() must not turn a cancelled partial batch into a completed run.
                self.active_run = None
                try:
                    _save_authenticated_config(self.run_file, saved, strict_backup=True)
                except OSError:
                    # The already persisted active=True checkpoint is still valid;
                    # failing diagnostics must not prevent a graceful task stop.
                    print("保存服务中断原因失败，保留原任务检查点", flush=True)
        if self.runtime:
            self.add_log(message)
            self.runtime.stop_accounts()

    def _login(self, payload):
        password = self._password(payload.get("password"))
        peer = payload.get("peer", "unknown")
        if not self.lock.is_configured():
            self._throttle(peer)
            expected = self.setup_token_file.read_text().strip() if self.setup_token_file.exists() else ""
            supplied = payload.get("setup_token", "")
            if not expected or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
                raise RemoteError("首次设置需要服务器初始化码", 401)
            self.lock.validate_new_password(password)
            self.lock.setup_password(password)
            self.setup_token_file.unlink(missing_ok=True)
        else:
            self.verify_password(password, peer)
        try:
            self._load_runtime()
        except Exception as exc:
            if not self.runtime:
                self.lock.lock()
            raise RemoteError("服务端加密配置读取失败，请检查服务器日志", 503) from exc
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        now = time.monotonic()
        self.sessions = {k: v for k, v in self.sessions.items() if v["expires"] > now}
        if len(self.sessions) >= 32:
            raise RemoteError("管理会话过多，请稍后重试", 429)
        self.sessions[hashlib.sha256(token.encode()).hexdigest()] = dict(csrf=csrf, expires=now+43200)
        return dict(token=token, csrf=csrf)

    def authenticate(self, request):
        token = request.get("token", "")
        if not isinstance(token, str) or len(token) > 256:
            raise RemoteError("请先解锁登录", 401)
        key = hashlib.sha256(token.encode()).hexdigest()
        session = self.sessions.get(key)
        if not self.runtime or not session or session["expires"] <= time.monotonic():
            self.sessions.pop(key, None)
            raise RemoteError("会话已过期，请重新输入密码", 401)
        if request.get("method") not in READ_METHODS:
            csrf = request.get("csrf", "")
            if not isinstance(csrf, str) or not hmac.compare_digest(session["csrf"], csrf):
                raise RemoteError("请求校验失败", 403)
        return key, session

    def state(self, since=0):
        r = self.runtime
        settings = dict(r.settings)
        proxy_configured = bool(settings.pop("exchange_proxy", ""))
        auto, revision = r.auto_ladder_settings_store.snapshot()
        since = max(0, int(since))
        with self.log_lock:
            logs = [dict(x) for x in self.logs if x["seq"] > since]
            sequence = self.log_seq
        with r._result_file_lock:
            assets = {str(k): dict(v) for k, v in r.total_asset_results.items()}
        return dict(boot=self.boot, busy=r.busy, status=str(r.status_var.get()),
                    summary=str(r.exchange_batch_summary_var.get()),
                    accounts=r.account_records(), settings=settings, proxy_configured=proxy_configured,
                    auto_ladder=auto.as_dict(), auto_revision=revision,
                    volume_query=dict(r.trade_volume_query_dates), volume_query_error=r.trade_volume_query_error,
                    progress=dict(r.progress), interrupted=({k: v for k, v in self.interrupted.items()
                        if k in {"active", "action", "command_id", "started", "reason", "interrupted_at", "accounts", "results"}} if self.interrupted else None),
                    can_retry=bool(r._last_batch_retry), seq=sequence, logs=logs,
                    assets=assets, maintenance=self.maintenance_result,
                    server_time=time.time())

    def _save_commands(self):
        from stores import _save_authenticated_config
        # Capacity fails closed: never evict IDs and silently execute a replay.
        _save_authenticated_config(self.command_file, self.commands)

    def _command(self, payload):
        ident = payload.get("command_id", "")
        try:
            uuid.UUID(ident)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("操作编号无效") from exc
        action = payload.get("action")
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        previous = self.commands.get(ident)
        if previous:
            if previous["digest"] != digest:
                raise RemoteError("同一操作编号不能对应不同指令", 409)
            return dict(**previous["result"], replay=True)
        issued = payload.get("issued_at")
        if not isinstance(issued, (int, float)) or not math.isfinite(issued) or not -30 <= time.time()-issued <= 300:
            raise ValueError("操作时间已过期，请核对状态后重新操作")
        if payload.get("confirmed") is not True:
            raise ValueError("执行操作需要界面确认")
        if action not in TASKS | {"stop"}:
            raise ValueError("不支持的任务")
        options = payload.get("options", {})
        if action == "query_volume":
            from shared_utils import trade_volume_time_range
            trade_volume_time_range(options)
        elif (not isinstance(options, dict) or set(options) - {"enable_withdraw", "enable_bnb_topup"}
                or any(type(value) is not bool for value in options.values())
                or (options and action != "run")):
            raise ValueError("本次执行选项无效")
        from .maintenance import storage_status
        if action != "stop" and not storage_status(self.directory)["ready"]:
            raise ValueError("服务器磁盘剩余空间不足，已阻止新任务；停止及清理仍可执行")
        self.commands = {k: v for k, v in self.commands.items() if time.time()-v.get("created", time.time()) < 7*86400}
        if len(self.commands) >= 20000:
            raise ValueError("操作记录达到容量，请维护后继续；停止操作仍可使用紧急停止接口")
        record = dict(digest=digest, created=time.time(), result=dict(accepted=True, action=action, command_id=ident,
                                                state="submitted"))
        self.commands[ident] = record
        try:
            self._save_commands()
        except Exception:
            self.commands.pop(ident, None)
            raise
        r = self.runtime
        try:
            if action == "stop":
                r.stop_accounts(payload.get("accounts"))
            else:
                if r.busy:
                    raise ValueError("已有任务运行或清理中")
                # Durable intent precedes financial execution. After a crash it
                # is surfaced for reconciliation, never automatically replayed.
                from stores import _save_authenticated_config
                if action == "retry_failed":
                    failed_keys = set((r._last_batch_retry or {}).get("failed_account_keys", []))
                    account_ids = [a["id"] for a in r.accounts if r._account_batch_key(a) in failed_keys]
                else:
                    account_ids = [a["id"] for a in r.selected_accounts(payload.get("accounts"))]
                with self._checkpoint_lock:
                    self.active_run = dict(active=True, action=action, command_id=ident,
                                           started=time.time(), accounts=account_ids,
                                           settings=dict(r.settings), options=options)
                    self._checkpoint_retry_at = 0
                    _save_authenticated_config(self.run_file, self.active_run, strict_backup=True)
                if options:
                    r.execute(action, payload.get("accounts"), options=options)
                else:
                    r.execute(action, payload.get("accounts"))
                self.interrupted = None
            record["result"]["state"] = "started"
        except Exception as exc:
            record["result"] = dict(accepted=False, action=action, command_id=ident,
                                    error=self.redact(exc), state="rejected")
            if action != "stop" and not r.busy:
                from stores import _save_authenticated_config
                with self._checkpoint_lock:
                    self.active_run = None
                    _save_authenticated_config(self.run_file, {**(self.interrupted or {"active": False}), "last_rejected": ident}, strict_backup=True)
        self._save_commands()
        return record["result"]

    def dispatch(self, request):
        method, payload = request.get("method"), request.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("请求格式无效")
        if method == "health":
            return dict(configured=self.lock.is_configured(), unlocked=self.runtime is not None,
                        busy=bool(self.runtime and (self.runtime.busy or self.runtime._managed_threads_snapshot())))
        if method == "login":
            return self._login(payload)
        key, session = self.authenticate(request)
        r = self.runtime
        self.dialog_context.control = True
        self.dialog_context.confirmed = payload.get("confirmed") is True
        try:
            if method == "session":
                from .runtime import field_schema
                return dict(csrf=session["csrf"], schema=field_schema())
            if method == "state":
                return self.state(payload.get("since", 0))
            if method == "command":
                return self._command(payload)
            if method == "emergency_stop":
                r.stop_accounts()
                return dict(stopping=True)
            if method == "settings":
                r.update_settings(payload.get("settings", {}), payload.get("auto_ladder"))
                return dict(staged=r.busy)
            if method == "volume_settings":
                return dict(saved=True, dates=r.save_trade_volume_query_settings(
                    payload.get("start_date", ""), payload.get("end_date", "")))
            if method == "accounts":
                r.replace_accounts(payload.get("accounts"))
                return dict(count=len(r.accounts))
            if method == "account_key":
                account = r.selected_accounts([payload.get("id")])[0]
                return dict(text=account["api_key"])
            if method == "search_accounts":
                query = payload.get("query", "")
                if not isinstance(query, str) or len(query) > 512:
                    raise ValueError("搜索内容无效")
                query = query.strip().lower()
                return dict(ids=[a["id"] for a in r.accounts if query and any(query in value.lower()
                            for value in (a["api_key"], a["address"], r._mask_key(a["api_key"]),
                                          r._mask_addr(a["address"])))])
            if method == "import_accounts":
                if r.busy:
                    raise ValueError("请先停止任务，再导入账号")
                text = payload.get("text", "")
                if not isinstance(text, str) or len(text) > 1000000:
                    raise ValueError("账号导入内容无效")
                parsed, error = r._parse_accounts_from_text(text)
                if error:
                    raise ValueError(error)
                rows = r.account_records()
                seen = {a["api_key"] for a in r.accounts}
                added = skipped = 0
                for apikey, secret, address, network in parsed:
                    if apikey in seen:
                        skipped += 1
                        continue
                    seen.add(apikey)
                    row = dict(api_key=apikey, api_secret=secret, address=address,
                               network=network or r.settings["acc_network"])
                    rows.append(row)
                    added += 1
                # Blank keys preserve existing credentials without ever sending
                # them back to the web process.
                for row in rows:
                    if row.get("id"):
                        row["api_key"] = ""
                if added:
                    r.replace_accounts(rows)
                return dict(count=len(r.accounts), added=added, skipped=skipped)
            if method == "selection":
                rows = payload.get("accounts", [])
                previous = dict(r.account_meta)
                changes = {}
                for row in rows:
                    ident = row.get("id")
                    r.selected_accounts([ident])
                    if type(row.get("selected")) is not bool or type(row.get("bnb_topup")) is not bool:
                        raise ValueError("账号开关格式无效")
                    changes[ident] = dict(selected=row["selected"], bnb_topup=row["bnb_topup"])
                r.account_meta.update(changes)
                try:
                    r.save_settings()
                except Exception:
                    r.account_meta = previous
                    raise
                for a in r.accounts:
                    if a["id"] in changes:
                        a["selected_var"].set(changes[a["id"]]["selected"])
                        a["bnb_topup_var"].set(changes[a["id"]]["bnb_topup"])
                return dict(saved=True, next_run=r.busy)
            if method == "proxy_test":
                def test():
                    from exchange_proxy_runtime import http_get_via_proxy
                    try:
                        start = time.monotonic()
                        proxy = r._get_exchange_proxy_url()
                        response = http_get_via_proxy("https://api.ipify.org",
                            proxies={"http": proxy, "https": proxy} if proxy else {}, timeout=10,
                            allow_system_proxy=False)
                        response.raise_for_status()
                        self.add_log(f"连接出口 {response.text.strip()}，耗时 {int((time.monotonic()-start)*1000)} ms")
                    except Exception as exc:
                        self.add_log(f"连接测试失败：{exc}")
                r._start_managed_thread(test, name="linux-proxy-test")
                return dict(started=True)
            if method == "asset_export":
                return dict(text=self._asset_csv(), filename="exchange-assets.csv")
            if method in {"export_accounts", "destroy_accounts", "lock", "change_password"}:
                if payload.get("confirmed") is not True:
                    raise ValueError("该操作需要确认")
                self.verify_password(payload.get("password"), payload.get("peer", "unknown"))
                if method == "export_accounts":
                    return dict(text="\n".join("|".join((a["api_key"], a["api_secret"], a["address"],
                                   r._account_network_value(a))) for a in r.accounts), filename="exchange-accounts.txt")
                if r.busy or r._managed_threads_snapshot():
                    raise ValueError("运行或清理期间不能执行此操作")
                if method == "destroy_accounts":
                    r.destroy_all_exchange_accounts()
                    from secure_delete import secure_unlink_all
                    extras = set(self.directory.glob("exchange_proxy_runtime_*.log.tail"))
                    for name in ("withdraw_success.txt", "total_asset_result.txt"):
                        extras.update(self.directory / f"{name}.{i}" for i in (1, 2, 3))
                    report = secure_unlink_all(extras)
                    if not report.ok:
                        raise ValueError("部分历史结果未能清理，请检查服务器文件权限")
                    with self.log_lock:
                        self.logs.clear()
                    self.interrupted = None
                    return dict(destroyed=True)
                if method == "change_password":
                    self.lock.change_password(payload["password"], self._password(payload.get("new_password")))
                    self.sessions.clear()
                    return dict(changed=True)
                r.close_idle()
                self.runtime = None
                self.sessions.clear()
                self.lock.lock()
                return dict(locked=True)
            if method == "logout":
                self.sessions.pop(key, None)
                return dict(logged_out=True)
            raise RemoteError("接口不存在", 404)
        finally:
            self.dialog_context.control = self.dialog_context.confirmed = False

    def _asset_csv(self):
        data = io.StringIO()
        writer = csv.writer(data)
        writer.writerow(["No", "API_KEY", "提现地址", "网络", "总资产(USDT)"])
        with self.runtime._result_file_lock:
            values = sorted(self.runtime.total_asset_results.values(), key=lambda x: x["index"])
        for value in values:
            cells = [value["index"], value["api_key"], value["address"], value["network"], value["total_usdt"]]
            writer.writerow([("'" + str(v)) if str(v).startswith(("=", "+", "-", "@")) else v for v in cells])
        return data.getvalue()

    def pump(self):
        if time.monotonic()-self.maintenance_at > 3600:
            from .maintenance import clean
            try:
                self.maintenance_result = clean(self.directory)
            except OSError:
                self.maintenance_result = dict(error="缓存清理失败，请检查服务端文件权限")
            self.maintenance_at = time.monotonic()
        if not self.runtime:
            return
        r = self.runtime
        r.pump()
        if time.monotonic()-self.storage_check_at > 60:
            from .maintenance import storage_status
            self.maintenance_result.update(storage_status(self.directory))
            self.storage_check_at = time.monotonic()
        from exchange_logging import log_queue
        for _ in range(200):
            try:
                entry = log_queue.get_nowait()
            except queue.Empty:
                break
            self.add_log(getattr(entry, "message", entry), persist=False)
        with self._checkpoint_lock:
            if self.active_run and not r.busy and time.monotonic() >= self._checkpoint_retry_at:
                from stores import _save_authenticated_config
                saved = {**self.active_run, "active": False, "ended": time.time()}
                try:
                    _save_authenticated_config(self.run_file, saved, strict_backup=True)
                except OSError:
                    self._checkpoint_retry_at = time.monotonic() + 60
                    self.add_log("任务结束记录保存失败，保留原检查点；请检查磁盘和文件权限")
                else:
                    self.active_run = None


class RequestServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    request_queue_size = 16


def serve(args):
    import fcntl
    os.umask(0o077)
    data = configure_data_directory(args.data)
    lease = (data / ".worker.lock").open("a")
    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    service = Service(args.data, args.setup_token_file)
    jobs = queue.Queue(maxsize=64)
    stopping = threading.Event()
    shutdown_signal = signal.SIGTERM
    def shutdown(signum, _frame):
        nonlocal shutdown_signal
        shutdown_signal = signum
        stopping.set()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.request.settimeout(45)
            try:
                request = receive(self.rfile)
                event, box = threading.Event(), []
                jobs.put_nowait((request, event, box))
                if not event.wait(40):
                    raise RemoteError("执行服务忙，请刷新状态后确认操作结果", 503)
                result = box[0]
            except Exception as exc:
                result = dict(ok=False, error=service.redact(exc), code=getattr(exc, "code", 400))
            data = json.dumps(result, ensure_ascii=False, default=str).encode() + b"\n"
            if len(data) > MAX_MESSAGE:
                data = b'{"ok":false,"error":"response too large","code":413}\n'
            self.wfile.write(data)
    path = Path(args.socket)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.unlink(missing_ok=True)
    with RequestServer(str(path), Handler) as server:
        path.chmod(0o600)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stop_requested = False
        try:
            while True:
                if stopping.is_set():
                    if not stop_requested:
                        service.prepare_shutdown(shutdown_signal)
                    stop_requested = True
                    if not service.runtime or (not service.runtime.busy and not service.runtime._managed_threads_snapshot()):
                        break
                try:
                    request, event, box = jobs.get(timeout=0.03)
                except queue.Empty:
                    pass
                else:
                    try:
                        if stop_requested:
                            raise RemoteError("执行服务正在停止", 503)
                        box.append(dict(ok=True, result=service.dispatch(request)))
                    except Exception as exc:
                        box.append(dict(ok=False, error=service.redact(exc), code=getattr(exc, "code", 400)))
                    finally:
                        event.set()
                service.pump()
        finally:
            server.shutdown()
            path.unlink(missing_ok=True)
            if service.runtime and not service.runtime.busy:
                service.runtime.close_idle()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--setup-token-file", required=True)
    serve(parser.parse_args())


if __name__ == "__main__":
    main()
