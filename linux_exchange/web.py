"""Small HTTPS-facing management API. No credentials or keys are stored here."""
import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from .ipc import call, RemoteError, MAX_MESSAGE
MAX_BODY = 2 * 1024 * 1024

STATIC = Path(__file__).with_name("static")
MUTATIONS = {"command", "settings", "accounts", "import_accounts", "selection", "proxy_test",
             "export_accounts", "destroy_accounts", "asset_export", "lock", "change_password",
             "logout", "emergency_stop", "account_key", "search_accounts"}


def create_app(*, origin=None, rpc=call):
    expected = origin or os.environ["XIAOJUN_PUBLIC_ORIGIN"]
    parsed = urlsplit(expected)
    if parsed.scheme != "https" or parsed.path not in ("", "/") or not parsed.netloc:
        raise ValueError("管理面板必须配置 HTTPS 地址")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def security(request, next_handler):
        if request.headers.get("host") != parsed.netloc:
            response = JSONResponse({"error": "访问地址无效"}, status_code=400)
        elif request.method not in {"GET", "HEAD"} and request.headers.get("origin") != expected.rstrip("/"):
            response = JSONResponse({"error": "请求来源校验失败"}, status_code=403)
        elif request.headers.get("content-length", "0").isdigit() and int(request.headers.get("content-length", "0")) > MAX_BODY:
            response = JSONResponse({"error": "请求过长"}, status_code=413)
        else:
            response = await next_handler(request)
        response.headers.update({
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY",
            "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        })
        return response

    async def invoke(request, method, payload=None, csrf=True):
        try:
            return await run_in_threadpool(rpc, method, payload or {},
                token=request.cookies.get("exchange_session", ""),
                csrf=request.headers.get("x-csrf-token", "") if csrf else "")
        except RemoteError as exc:
            raise exc
        except (OSError, TimeoutError) as exc:
            raise RemoteError("执行服务暂时不可用，请稍后刷新确认运行状态", 503) from exc

    @app.exception_handler(RemoteError)
    async def remote_error(_request, exc):
        return JSONResponse({"error": str(exc)}, status_code=exc.code)

    async def body(request):
        raw = await request.body()
        if len(raw) > MAX_BODY:
            raise RemoteError("请求过长", 413)
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise RemoteError("请求内容无效", 400) from exc
        if not isinstance(result, dict):
            raise RemoteError("请求内容无效", 400)
        # Peer information is injected by the server, never trusted from JSON.
        result["peer"] = request.client.host if request.client else "unknown"
        return result

    @app.get("/")
    async def page():
        return FileResponse(STATIC / "index.html")

    @app.get("/app.js")
    async def script():
        return FileResponse(STATIC / "app.js", media_type="application/javascript")

    @app.get("/style.css")
    async def style():
        return FileResponse(STATIC / "style.css", media_type="text/css")

    @app.get("/api/health")
    async def health(request: Request):
        return await invoke(request, "health", csrf=False)

    @app.post("/api/login")
    async def login(request: Request):
        result = await invoke(request, "login", await body(request), csrf=False)
        response = JSONResponse({"csrf": result["csrf"]})
        response.set_cookie("exchange_session", result["token"], max_age=43200,
                            secure=True, httponly=True, samesite="strict", path="/")
        return response

    @app.get("/api/session")
    async def session(request: Request):
        return await invoke(request, "session", csrf=False)

    @app.get("/api/state")
    async def state(request: Request, since: int = 0):
        return await invoke(request, "state", {"since": since}, csrf=False)

    @app.get("/api/events")
    async def events(request: Request, since: int = 0):
        first = await invoke(request, "state", {"since": since}, csrf=False)
        async def stream():
            nonlocal first, since
            while not await request.is_disconnected():
                try:
                    current = first or await invoke(request, "state", {"since": since}, csrf=False)
                    first = None
                    since = current["seq"]
                    yield "event: state\ndata: " + json.dumps(current, ensure_ascii=False, default=str) + "\n\n"
                except RemoteError as exc:
                    yield "event: problem\ndata: " + json.dumps({"error": str(exc), "code": exc.code}) + "\n\n"
                    return
                await asyncio.sleep(1)
        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"X-Accel-Buffering": "no"})

    @app.post("/api/{method}")
    async def mutation(method: str, request: Request):
        if method not in MUTATIONS:
            raise RemoteError("接口不存在", 404)
        result = await invoke(request, method, await body(request))
        response = JSONResponse(result)
        if method in {"logout", "lock", "change_password"}:
            response.delete_cookie("exchange_session", secure=True, httponly=True, samesite="strict")
        return response

    return app


def app_factory():
    return create_app()
