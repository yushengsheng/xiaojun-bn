"""Isolated integration fixtures; exchange writes use fake clients only."""
from pathlib import Path
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
TEMP = Path(os.environ.get("XIAOJUN_TEST_ROOT", ROOT / ".linux-tests"))
TEMP.mkdir(exist_ok=True)
WORK = Path(tempfile.mkdtemp(dir=TEMP))
from linux_exchange.worker import Service
from linux_exchange.ipc import RemoteError


def assert_error(fn, code=None):
    try:
        fn()
    except (ValueError, RuntimeError) as e:
        if code:
            assert getattr(e, "code", None) == code, str(e)
        return
    raise AssertionError("expected rejection")


def fixture():
    token = WORK/"setup"
    token.write_text("test-initialization-code")
    s = Service(WORK/"data", token)
    login = s.dispatch(dict(method="login", payload=dict(password="test-password-strong",
                                                       setup_token=token.read_text())))
    def call(method, payload=None):
        return s.dispatch(dict(method=method, payload=payload or {}, **login))
    call("accounts", {"accounts": [dict(api_key="example-key-123456789012345678901234",
                                       api_secret="example-secret-123456789012345678901234", selected=True),
                                     dict(api_key="example-key-234567890123456789012345", api_secret="example-secret-2", selected=False)]})
    return s, call, login


def command(action="run", **kw):
    return dict(action=action, command_id=str(uuid.uuid4()), confirmed=True, issued_at=time.time(), **kw)


def auth():
    s, call, login = fixture()
    assert_error(lambda: s.dispatch(dict(method="state")), 401)
    assert_error(lambda: s.dispatch(dict(method="settings", token=login["token"], csrf="wrong")), 403)
    assert_error(lambda: call("export_accounts", dict(password="wrong", confirmed=True)), 401)
    assert_error(lambda: call("export_accounts", dict(password="test-password-strong")))
    state = call("state")
    serialized = json.dumps(state, default=str)
    assert "example-secret" not in serialized
    assert "example-key-123456789012345678901234" not in serialized
    call("settings", {"settings": {"exchange_proxy": "http://proxy-user:proxy-password@127.0.0.1:10080"}})
    assert "proxy-password" not in json.dumps(call("state"), default=str)
    message = "✔ [账号 #1] 市价买入 BTCUSDT，金额 12.3456，API Secret=" + s.runtime.accounts[0]["api_secret"]
    s.add_log(message)
    entry = call("state")["logs"][-1]
    assert "example-secret" not in entry["text"]
    assert [entry["text"][start:end] for start, end in entry["amount_spans"]] == ["12.3456"]
    export = call("export_accounts", dict(password="test-password-strong", confirmed=True))
    assert "example-secret" in export["text"]
    archive=WORK/"data/withdraw_success.txt.1"
    archive.write_text("old-result")
    call("destroy_accounts",dict(password="test-password-strong",confirmed=True))
    assert not archive.exists() and call("state")["accounts"]==[]
    call("logout")
    assert s.runtime and s.lock.is_unlocked()
    assert_error(lambda: call("state"), 401)
    s.runtime.close_idle()


def persistence():
    s, call, login = fixture()
    request = command()
    with patch.object(s.runtime, "execute") as execute:
        first = call("command", request)
        assert first["accepted"]
        assert call("command", request)["replay"]
        assert execute.call_count == 1
        assert_error(lambda: call("command", {**request, "action": "withdraw"}), 409)
    for path in (WORK/"data").glob("*.json*"):
        raw = path.read_text()
        assert "example-secret" not in raw and "example-key-" not in raw
    with patch.object(type(s.runtime), 'busy', new_callable=lambda: property(lambda _r: True)), patch.object(s.runtime, 'stop_accounts') as stop:
        import signal
        s.prepare_shutdown(signal.SIGTERM)
        stop.assert_called_once()
    # Graceful cancellation has finished, but the encrypted run must remain
    # interrupted instead of being marked completed by pump().
    s.pump()
    s.runtime.close_idle()
    restarted = Service(WORK/"data", WORK/"no-setup")
    assert restarted.runtime is None and not restarted.lock.is_unlocked()
    session = restarted.dispatch(dict(method="login", payload={"password": "test-password-strong"}))
    assert restarted.interrupted and restarted.interrupted["command_id"] == request["command_id"]
    assert restarted.interrupted['reason'] == 'SIGTERM'
    assert 'SIGTERM' in next((WORK/'data/logs').glob('service_runtime_*.log')).read_text()
    assert len(restarted.runtime.accounts) == 2
    with patch.object(restarted.runtime, "execute") as execute:
        result = restarted.dispatch(dict(method="command", payload=request, **session))
        assert result["replay"] and execute.call_count == 0
        expired = command();expired["issued_at"] -= 1000
        assert_error(lambda: restarted.dispatch(dict(method="command", payload=expired, **session)))
        assert execute.call_count == 0
    restarted.runtime.close_idle()


def checkpoint():
    import signal
    import threading
    s, call, _ = fixture()
    import stores
    s.active_run = dict(active=True, action="run", command_id="checkpoint-race", started=time.time())
    entered, release, shutdown_started = threading.Event(), threading.Event(), threading.Event()
    errors = []
    original = stores._save_authenticated_config
    def save(path, data, **kwargs):
        if threading.current_thread().name == "checkpoint-test":
            entered.set()
            assert release.wait(5), "checkpoint release timed out"
        original(path, data, **kwargs)
    def run_checkpoint():
        try:
            s._checkpoint_account(s.runtime.accounts[0], True)
        except Exception as exc:
            errors.append(exc)
    def shutdown():
        shutdown_started.set()
        try:
            s.prepare_shutdown(signal.SIGTERM)
        except Exception as exc:
            errors.append(exc)
    with patch.object(type(s.runtime), "busy", new_callable=lambda: property(lambda _r: True)), patch.object(s.runtime, "stop_accounts") as stop, patch.object(stores, "_save_authenticated_config", side_effect=save):
        writer = threading.Thread(target=run_checkpoint, name="checkpoint-test")
        writer.start()
        assert entered.wait(5)
        stopper = threading.Thread(target=shutdown)
        stopper.start()
        assert shutdown_started.wait(5)
        time.sleep(0.1)
        assert not stop.called, "shutdown must wait for the in-flight checkpoint"
        release.set()
        writer.join(5); stopper.join(5)
        assert not writer.is_alive() and not stopper.is_alive() and not errors, errors
    saved = stores._load_authenticated_config(s.run_file)[0]
    assert saved["reason"] == "SIGTERM" and saved["active"] is True
    assert list(saved["results"].values()) == [True]
    assert s.active_run is None
    # A disk failure after completion keeps the worker alive and the prior
    # intent intact, without repeatedly hammering a failing disk.
    s.active_run = dict(active=True, action="run", command_id="disk-failure")
    with patch.object(stores, "_save_authenticated_config", side_effect=OSError("disk full")) as save:
        s.pump(); s.pump()
        assert save.call_count == 1 and s.active_run["active"] is True
    s.runtime.close_idle()


def dispatch():
    s, call, _ = fixture()
    r = s.runtime
    from exchange_constants import TRADE_MODE_OPTIONS
    for mode in TRADE_MODE_OPTIONS:
        changes={"trade_mode": mode, "premium_delta": "0.01", "bnb_fee_stop": "0.001"}
        call("settings", {"settings": changes})
        with patch.object(r, "run_selected_accounts") as run:
            r.execute("run")
            assert len(run.call_args.args[0]) == 1
            assert r.trade_mode_var.get() == mode
    for action, name in [("query_assets","run_selected_accounts"), ("collect_bnb","run_selected_accounts"),
                         ("query_volume","run_query_monthly_trade_volume"), ("withdraw","batch_manual_withdraw"),
                         ("redeem_earn","batch_redeem_flexible_earn"), ("earn_auto_subscribe","run_selected_accounts"),
                         ("retry_failed","retry_last_failed_batch_operation")]:
        with patch.object(r, name) as target:
            r.execute(action)
            assert target.call_count == 1, action
            if action=="earn_auto_subscribe":
                assert target.call_args.kwargs["trade_settings_override"]["earn_auto_subscribe_only"]
    ids=[a["id"] for a in r.accounts]
    call("selection", {"accounts":[dict(id=ids[1], selected=True, bnb_topup=False)]})
    assert len(r.selected_accounts())==2 and not r.accounts[1]["bnb_topup_var"].get()
    r.accounts[1]["batch_active"]=True
    r.stop_accounts([ids[1]])
    assert r.accounts[1]["stop_event"].is_set() and not r.accounts[0]["stop_event"].is_set()
    call("import_accounts", {"text":"new-example-key|new-example-secret||BSC"})
    assert len(r.accounts)==3
    r.close_idle()


def settings():
    s, call, _ = fixture()
    r = s.runtime
    old = r.spot_symbol_var.get()
    r._batch_task_active=True
    call("settings", {"settings":{"spot_symbol":"BTCUSDT","max_threads":3}, "auto_ladder":{"price_ticks":"8","pause_price_ticks":"10"}})
    assert r.spot_symbol_var.get()==old and r.settings["spot_symbol"]=="BTCUSDT"
    assert r.auto_ladder_settings_store.snapshot()[0].price_ticks==8
    assert_error(lambda:call("settings",{"settings":{"max_threads":"nan"}}))
    assert_error(lambda:call("settings",{"settings":{"futures_leverage":"invalid", "trade_account_type":"合约"}}))
    assert r.settings["max_threads"]==3
    assert_error(lambda:call("accounts",{"accounts":[]}))
    assert_error(lambda:call("lock",dict(password="test-password-strong",confirmed=True)))
    assert s.runtime is r and s.lock.is_unlocked()
    r._batch_task_active=False
    r.apply_settings()
    assert r.spot_symbol_var.get()=="BTCUSDT"
    r.close_idle()


def wait_idle(service):
    deadline=time.monotonic()+15
    while service.runtime.busy or service.runtime._managed_threads_snapshot():
        service.pump()
        assert time.monotonic()<deadline, service.runtime.status_var.get()
        time.sleep(.03)
    service.pump()
    for _ in range(8):
        time.sleep(.04)
        service.pump()


def execution():
    s, call, _=fixture()
    r=s.runtime
    from decimal import Decimal
    client=Mock()
    client.query_total_wallet_balance.return_value=(Decimal("123.45"), [{"walletName":"现货","balance":Decimal("123.45")}])
    client.query_asset_balances_breakdown.return_value={"USDT":Decimal("123.45")}
    with patch.object(r,"_create_binance_client",return_value=client):
        result=call("command",command("query_assets"))
        assert result["accepted"]
        wait_idle(s)
    assert r.total_asset_results[1]["total_usdt"]==Decimal("123.45"), r.accounts[0]["status_var"].get()
    assert "成功" in r.exchange_batch_summary_var.get(), r.exchange_batch_summary_var.get()
    # Existing stock strategy fixture simulates signed Binance endpoints,
    # durable order intent, fills and fund transfers; no live HTTP is used.
    from test_exchange_stocks import Client, NOW
    from exchange_stocks import StockStrategy
    stock=Client()
    r.update_settings({"trade_account_type":"股票"})
    with patch.object(r,"_create_binance_client",return_value=stock), patch(
        "exchange_app_stocks.StockStrategy",
        side_effect=lambda *args, **kwargs: StockStrategy(*args, now=lambda: NOW, **kwargs),
    ):
        result=call("command",command())
        assert result["accepted"]
        wait_idle(s)
    assert stock.posts and len(stock.posts)==2, r.accounts[0]["status_var"].get()
    assert "1/1轮" in r.accounts[0]["status_var"].get(), r.accounts[0]["status_var"].get()
    assert not any(a["batch_active"] for a in r.accounts)
    r.close_idle()


def failed_result_next_account():
    from decimal import Decimal
    import requests
    from exchange_binance_client import BinanceClient
    s, call, _ = fixture()
    r = s.runtime
    r.update_settings({"max_threads": 1})
    for account in r.accounts:
        account["address"] = "test-withdraw-target"
        account["network"] = "BSC"
    created, posts = [], []
    def make_client(key, secret, **kwargs):
        with patch.object(BinanceClient, '_build_session', return_value=Mock()):
            client = BinanceClient(key, secret, operation_journal=r.exchange_operation_journal)
        index = len(created)
        created.append(client)
        client.DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 0
        client.collect_asset_to_spot = Mock(return_value=0)
        client.spot_asset_balance_decimal = Mock(return_value=Decimal(10))
        def network(base, method, path, params, **kw):
            if method == 'GET' and path == '/sapi/v1/capital/config/getall':
                return [{'coin': 'USDT', 'networkList': [{'network': 'BSC', 'withdrawEnable': True}]}]
            if method == 'POST' and path == '/sapi/v1/capital/withdraw/apply':
                posts.append((index, dict(params)))
                if index == 0:
                    raise requests.ReadTimeout('missing withdrawal result')
                return {'id': 'second-account-receipt'}
            raise AssertionError('Unexpected endpoint')
        client._request_impl = network
        return client
    with patch.object(r, '_create_binance_client', side_effect=make_client):
        r.batch_manual_withdraw(r.accounts, require_confirm=False)
        wait_idle(s)
    assert [index for index, _ in posts] == [0, 0, 1], posts
    assert posts[0][1]['withdrawOrderId'] != posts[1][1]['withdrawOrderId']
    assert not r.stop_event.is_set()
    summary = r.exchange_batch_summary_var.get()
    assert '成功1' in summary and '失败1' in summary, summary
    assert '失败' in r.accounts[0]['status_var'].get()
    assert '提现' in r.accounts[1]['status_var'].get()
    assert not r.exchange_operation_journal.active_entries(kind='exchange-funds')
    assert not r._pending_cleanup_snapshot()
    assert_error(lambda: call('operation_review', {}))
    assert_error(lambda: call('operation_resolve', {}))
    r.close_idle()


def web():
    s, _, _=fixture()
    from fastapi.testclient import TestClient
    from linux_exchange.web import create_app
    def rpc(method,payload,token="",csrf=""):
        return s.dispatch(dict(method=method,payload=payload,token=token,csrf=csrf))
    origin="https://testserver:26483"
    client=TestClient(create_app(origin=origin,rpc=rpc),base_url=origin)
    assert client.get("/api/state").status_code==401
    assert client.post("/api/login",json={"password":"test-password-strong"}).status_code==403
    assert client.post("/api/login",headers={"Origin":"https://evil.example"},json={"password":"test-password-strong"}).status_code==403
    login=client.post("/api/login",headers={"Origin":origin},json={"password":"test-password-strong"})
    assert login.status_code==200, login.text
    cookie=login.headers["set-cookie"].lower()
    assert all(x in cookie for x in ("secure","httponly","samesite=strict"))
    assert client.post("/api/settings",headers={"Origin":origin},json={"settings":{}}).status_code==403
    headers={"Origin":origin,"X-CSRF-Token":login.json()["csrf"]}
    assert client.post("/api/settings",headers=headers,json={"settings":{"max_threads":2}}).status_code==200
    assert client.post("/api/volume_settings",headers={"Origin":origin},json={}).status_code==403
    saved=client.post("/api/volume_settings",headers=headers,json={"start_date":"202692","end_date":""})
    assert saved.status_code==200 and saved.json()["dates"]==dict(start_date="2026-09-02",end_date="")
    assert client.get("/api/state").json()["volume_query"]==saved.json()["dates"]
    response=client.get("/api/state")
    assert response.status_code==200 and "example-secret" not in response.text
    assert response.headers["cache-control"]=="no-store"
    assert client.get("/api/state",headers={"Host":"evil.example"}).status_code==400
    assert client.get("/").status_code==200 and client.get("/app.js").status_code==200
    assert client.post("/api/logout",headers=headers,json={}).status_code==200
    assert s.runtime is not None and s.lock.is_unlocked()
    s.runtime.close_idle()


def web_shutdown():
    import requests
    import shlex
    import signal as signals
    import socket as network_socket
    import threading
    # Run the real service command with a fake RPC and an open real SSE stream.
    # An unbounded graceful shutdown would hang until the fixture is killed.
    with network_socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    factory = WORK/'slow_web.py'
    marker = WORK/'rpc-entered'
    factory.write_text("from linux_exchange.web import create_app\nfrom pathlib import Path\nimport time\n"
                       "def rpc(method, *a, **k):\n"
                       " if method == 'health':\n"
                       f"  Path({str(marker)!r}).write_text('entered')\n"
                       "  time.sleep(60)\n"
                       " return {'seq': 0}\n"
                       "def app_factory():\n"
                       f" return create_app(origin='https://127.0.0.1:{port}', rpc=rpc)\n")
    unit = (ROOT/'linux_exchange/deploy/xiaojun-exchange-web.service').read_text()
    args = shlex.split(next(line.split('=', 1)[1] for line in unit.splitlines() if line.startswith('ExecStart=')))
    args[0] = sys.executable
    args[args.index('linux_exchange.web:app_factory')] = 'slow_web:app_factory'
    args[args.index('--port')+1] = str(port)
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join((str(WORK), str(ROOT)))}
    process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stream = None
    try:
        deadline = time.monotonic()+10
        while True:
            assert process.poll() is None
            try:
                ready = requests.get(f'http://127.0.0.1:{port}/', timeout=.5)
                if ready.status_code == 200: break
            except requests.RequestException: pass
            assert time.monotonic() < deadline
            time.sleep(.05)
        stream = requests.get(f'http://127.0.0.1:{port}/api/events', stream=True, timeout=15)
        assert stream.status_code == 200
        assert next(stream.iter_lines(chunk_size=1)) == b'event: state'
        def slow_request():
            try: requests.get(f'http://127.0.0.1:{port}/api/health', timeout=20)
            except requests.RequestException: pass
        pending = threading.Thread(target=slow_request, daemon=True)
        pending.start()
        deadline = time.monotonic()+5
        while not marker.exists():
            assert time.monotonic() < deadline
            time.sleep(.05)
        process.terminate()
        output = process.communicate(timeout=15)
        # Uvicorn restores/re-raises SIGTERM after orderly application shutdown.
        assert process.returncode in (0, -signals.SIGTERM), output
        assert 'Application shutdown complete' in output[1], output
    finally:
        if stream is not None: stream.close()
        if process.poll() is None:
            process.kill();process.communicate(timeout=5)


def maintenance():
    from linux_exchange.maintenance import clean
    data=WORK/"data";data.mkdir();logs=data/"logs";logs.mkdir()
    old=logs/"exchange_runtime_old_99999999.log";old.write_bytes(b"x"*100)
    live=logs/f"exchange_runtime_now_{os.getpid()}.log";live.write_text("protected-live-log")
    recent = logs/'exchange_runtime_recent_99999997.log'
    with recent.open('wb') as f: f.truncate(25*1024*1024)
    os.utime(old,(time.time()-9*86400,)*2)
    protected=["accounts.json","accounts.json.bak",".secret.key","app_lock.json","exchange_operations.json","withdraw_success.txt"]
    for name in protected:(data/name).write_text("protected-"+name)
    temp=data/".accounts.json.crashed.tmp";temp.write_text("discard");os.utime(temp,(time.time()-2*86400,)*2)
    link=logs/"exchange_runtime_bad_99999998.log";link.symlink_to(data/"exchange_operations.json")
    proxy=data/f"exchange_proxy_runtime_{os.getpid()}.log"
    with proxy.open("wb") as f:
        f.truncate(6*1024*1024)
    result=clean(data)
    assert result["removed"]==2 and not old.exists() and not temp.exists()
    assert live.exists() and link.exists()
    assert recent.stat().st_size == 25*1024*1024
    assert proxy.stat().st_size==6*1024*1024 and not proxy.with_name(proxy.name+".tail").exists()
    assert all((data/name).read_text()=="protected-"+name for name in protected)


def socket():
    from linux_exchange.ipc import call
    token=WORK/"setup";token.write_text("socket-setup-code")
    path=WORK/"worker.sock"
    args=[sys.executable,"-m","linux_exchange.worker","--data",str(WORK/"data"),"--socket",str(path),"--setup-token-file",str(token)]
    process=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        deadline=time.monotonic()+10
        while not path.exists():
            assert process.poll() is None
            assert time.monotonic()<deadline
            time.sleep(.05)
        assert path.stat().st_mode & 0o777==0o600
        assert not call("health",socket_path=str(path))["unlocked"]
        second=subprocess.run(args,capture_output=True,text=True,timeout=5)
        assert second.returncode!=0 and path.exists()
        login=call("login",dict(password="socket-password",setup_token="socket-setup-code"),socket_path=str(path))
        assert call("state",token=login["token"],socket_path=str(path))["accounts"]==[]
        assert_error(lambda: call("settings",{},token=login["token"],socket_path=str(path)),403)
    finally:
        process.terminate()
        output=process.communicate(timeout=15)
        assert process.returncode==0, output


def desktop():
    s, call, login = fixture()
    r = s.runtime
    original = r.accounts[0]
    original["status_var"].set("总资产 123.45 USDT")
    key, secret = original["api_key"], original["api_secret"]
    text = f"{key}|replacement-secret|replacement-address|ETH\nnew-desktop-key-123456789012345|new-secret|new-address|BSC\nnew-desktop-key-123456789012345|different-secret"
    imported = call("import_accounts", {"text": text})
    assert imported == dict(count=3, added=1, skipped=2), imported
    assert r.accounts[0]["api_secret"] == secret and r.accounts[0]["address"] != "replacement-address"
    assert r.accounts[0]["status_var"].get() == "总资产 123.45 USDT"
    with patch.object(r, "replace_accounts") as replace:
        duplicate = call("import_accounts", {"text": f"{key}|ignored-secret"})
        assert duplicate == dict(count=3, added=0, skipped=1), duplicate
        replace.assert_not_called()
    assert call("search_accounts", {"query": key})["ids"] == [original["id"]]
    assert call("search_accounts", {"query": "new-address"})["ids"] == [r.accounts[2]["id"]]
    assert call("search_accounts", {"query": ""})["ids"] == []
    assert call("account_key", {"id": original["id"]}) == dict(text=key)
    assert_error(lambda: call("account_key", {"id": "unknown"}))
    assert_error(lambda: s.dispatch(dict(method="account_key", payload={"id": original["id"]}, token=login["token"])), 403)
    old_settings = dict(r.settings)
    with patch.object(r, "run_selected_accounts") as run:
        request = command(options=dict(enable_withdraw=False, enable_bnb_topup=True))
        assert call("command", request)["accepted"]
        assert run.call_args.kwargs["batch_enable_withdraw_override"] is False
        assert run.call_args.kwargs["batch_enable_bnb_topup_override"] is True
        assert call("command", request)["replay"] and run.call_count == 1
        assert r.settings == old_settings
        for invalid in (dict(enable_withdraw="false"), dict(unrecognized=True), []):
            assert_error(lambda invalid=invalid: call("command", command(options=invalid)))
        assert_error(lambda: call("command", command("withdraw", options=dict(enable_withdraw=True))))
    with patch.object(r, "run_query_monthly_trade_volume") as query:
        options = dict(period="custom", end_date="2026-10-01")
        request = command("query_volume", options=options)
        assert call("command", request)["accepted"]
        assert query.call_args.kwargs["query_options"] == options
        assert call("command", request)["replay"] and query.call_count == 1
        assert r.settings == old_settings
        for invalid in (dict(period="custom", start_date="bad"),
                        dict(period="custom", start_date="2026-10-03", end_date="2026-10-01"),
                        dict(period="custom", start_date="2999-10-01"), dict(enable_withdraw=True)):
            assert_error(lambda invalid=invalid: call("command", command("query_volume", options=invalid)))
        assert query.call_count == 1
    saved=call("volume_settings",dict(start_date="202692",end_date="2026/9/3"))["dates"]
    assert saved==dict(start_date="2026-09-02",end_date="2026-09-03")
    assert call("state")["volume_query"]==saved
    assert r.settings==old_settings
    with patch.object(r,"_start_managed_thread") as start:
        assert_error(lambda: call("volume_settings",dict(start_date="2026-09-04",end_date="2026-09-03")))
        start.assert_not_called()
    assert r.trade_volume_query_dates==saved
    r.close_idle()
    # Both service restart and a new login reload durable query conditions.
    restarted=Service(s.directory,WORK/"no-setup")
    session=restarted.dispatch(dict(method="login",payload={"password":"test-password-strong"}))
    assert restarted.dispatch(dict(method="state",payload={},**session))["volume_query"]==saved
    restarted.runtime.close_idle()


if __name__=="__main__":
    try:
        globals()[sys.argv[1]]()
        print("LINUX CASE OK",sys.argv[1])
    finally:
        shutil.rmtree(WORK)
