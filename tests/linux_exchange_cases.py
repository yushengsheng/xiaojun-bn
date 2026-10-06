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
    s.runtime.close_idle()
    restarted = Service(WORK/"data", WORK/"no-setup")
    assert restarted.runtime is None and not restarted.lock.is_unlocked()
    session = restarted.dispatch(dict(method="login", payload={"password": "test-password-strong"}))
    assert restarted.interrupted and restarted.interrupted["command_id"] == request["command_id"]
    assert len(restarted.runtime.accounts) == 2
    with patch.object(restarted.runtime, "execute") as execute:
        result = restarted.dispatch(dict(method="command", payload=request, **session))
        assert result["replay"] and execute.call_count == 0
        expired = command();expired["issued_at"] -= 1000
        assert_error(lambda: restarted.dispatch(dict(method="command", payload=expired, **session)))
        assert execute.call_count == 0
    restarted.runtime.close_idle()


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
    from test_exchange_stocks import Client
    stock=Client()
    r.update_settings({"trade_account_type":"股票"})
    with patch.object(r,"_create_binance_client",return_value=stock), patch("exchange_app_stocks.reconcile_exchange_orders"):
        result=call("command",command())
        assert result["accepted"]
        wait_idle(s)
    assert stock.posts and len(stock.posts)==2, r.accounts[0]["status_var"].get()
    assert "1/1轮" in r.accounts[0]["status_var"].get(), r.accounts[0]["status_var"].get()
    assert not any(a["batch_active"] for a in r.accounts)
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
    response=client.get("/api/state")
    assert response.status_code==200 and "example-secret" not in response.text
    assert response.headers["cache-control"]=="no-store"
    assert client.get("/api/state",headers={"Host":"evil.example"}).status_code==400
    assert client.get("/").status_code==200 and client.get("/app.js").status_code==200
    assert client.post("/api/logout",headers=headers,json={}).status_code==200
    assert s.runtime is not None and s.lock.is_unlocked()
    s.runtime.close_idle()


def maintenance():
    from linux_exchange.maintenance import clean
    data=WORK/"data";data.mkdir();logs=data/"logs";logs.mkdir()
    old=logs/"exchange_runtime_old_99999999.log";old.write_bytes(b"x"*100)
    live=logs/f"exchange_runtime_now_{os.getpid()}.log";live.write_text("protected-live-log")
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
    assert proxy.stat().st_size==0 and proxy.with_name(proxy.name+".tail").stat().st_size==1024*1024
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
    r.close_idle()


if __name__=="__main__":
    try:
        globals()[sys.argv[1]]()
        print("LINUX CASE OK",sys.argv[1])
    finally:
        shutil.rmtree(WORK)
