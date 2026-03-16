import time
import hmac
import hashlib
import logging
from logging.handlers import RotatingFileHandler
import ipaddress
import base64
from typing import Dict, Any, Optional
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import threading
import queue
import random
import os
import functools
import sys
import csv
import json
import re
import shutil
import socket
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from app_paths import APP_DIR, BUNDLE_DIR, STRATEGY_CONFIG_FILE
from secret_box import SECRET_BOX

try:
    from page_onchain import OnchainTransferPage
    _ONCHAIN_IMPORT_ERROR = None
except Exception as e:
    OnchainTransferPage = None
    _ONCHAIN_IMPORT_ERROR = e

# ====================== 默认配置 ======================
API_KEY_DEFAULT = ""
API_SECRET_DEFAULT = ""
EXCHANGE_PROXY_DEFAULT = ""

SPOT_SYMBOL_DEFAULT = "BNBUSDT"
SPOT_ROUNDS_DEFAULT = 20
TRADE_MODE_MARKET = "市价"
TRADE_MODE_LIMIT = "挂单"
TRADE_MODE_PREMIUM = "溢价单"
TRADE_MODE_OPTIONS = (
    TRADE_MODE_MARKET,
    TRADE_MODE_LIMIT,
    TRADE_MODE_PREMIUM,
)
TRADE_MODE_DEFAULT = TRADE_MODE_MARKET
PREMIUM_PERCENT_DEFAULT = ""
BNB_FEE_STOP_DEFAULT = ""
BNB_TOPUP_AMOUNT_DEFAULT = "0"
REPRICE_THRESHOLD_PERCENT_DEFAULT = "1"
SUPPORTED_QUOTE_ASSET_SUFFIXES = (
    "FDUSD",
    "USDT",
    "USDC",
    "USD1",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "TRY",
    "EUR",
)

WITHDRAW_ADDRESS_DEFAULT = ""
WITHDRAW_NETWORK_DEFAULT = "BSC"
WITHDRAW_COIN_DEFAULT = "USDT"
WITHDRAW_FEE_BUFFER_DEFAULT = 0
WITHDRAW_NETWORK_OPTIONS = (
    "BSC",
    "ETH",
    "TRX",
    "ARB",
    "OP",
    "MATIC",
    "AVAXC",
    "SOL",
    "BASE",
    "LINEA",
    "ZKSYNCERA",
)
WITHDRAW_COIN_OPTIONS = (
    "BNB",
    "USDT",
    "USDC",
)

SPOT_PRECISION_DEFAULT = 0
MAX_THREADS_DEFAULT = 5


class ExchangeProxyRuntime:
    def __init__(self, work_dir: Path):
        self.work_dir = Path(work_dir)
        self._lock = threading.RLock()
        self._proc = None
        self._raw_source = ""
        self._local_proxy_url = ""
        self._config_path: Path | None = None
        self._log_path: Path | None = None
        self._log_handle = None
        self._backend = ""

    @staticmethod
    def _decode_ss_userinfo(userinfo: str) -> tuple[str, str]:
        padded = str(userinfo or "") + "=" * (-len(str(userinfo or "")) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded.encode()).decode("utf-8")
        except Exception as exc:
            raise RuntimeError("SS 链接解析失败：用户信息不是有效的 Base64") from exc
        if ":" not in decoded:
            raise RuntimeError("SS 链接解析失败：缺少 method:password")
        method, password = decoded.split(":", 1)
        method = method.strip()
        password = password.strip()
        if not method or not password:
            raise RuntimeError("SS 链接解析失败：method 或 password 为空")
        return method, password

    @classmethod
    def parse_ss_uri(cls, ss_uri: str) -> dict[str, object]:
        text = str(ss_uri or "").strip()
        parsed = urlsplit(text)
        if parsed.scheme.lower() != "ss":
            raise RuntimeError("不是有效的 ss:// 链接")
        if "@" not in parsed.netloc:
            raise RuntimeError("SS 链接解析失败：缺少服务器地址")
        userinfo, hostport = parsed.netloc.split("@", 1)
        method, password = cls._decode_ss_userinfo(userinfo)
        if ":" not in hostport:
            raise RuntimeError("SS 链接解析失败：缺少端口")
        host, port_text = hostport.rsplit(":", 1)
        host = host.strip()
        if not host:
            raise RuntimeError("SS 链接解析失败：服务器地址为空")
        try:
            port = int(port_text)
        except Exception as exc:
            raise RuntimeError("SS 链接解析失败：端口无效") from exc
        if not (1 <= port <= 65535):
            raise RuntimeError("SS 链接解析失败：端口超出范围")
        query = parse_qs(parsed.query)
        network = str((query.get("type") or ["tcp"])[0] or "tcp").strip().lower()
        if network not in {"tcp", "udp"}:
            network = "tcp"
        return {
            "server": host,
            "server_port": port,
            "method": method,
            "password": password,
            "network": network,
        }

    @staticmethod
    def _allocate_local_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _candidate_sing_box_paths() -> list[Path]:
        home = Path.home()
        candidates = [
            BUNDLE_DIR / "bin" / "sing-box" / "sing-box.exe",
            BUNDLE_DIR / "bin" / "sing_box" / "sing-box.exe",
            APP_DIR / "bin" / "sing-box" / "sing-box.exe",
            APP_DIR / "bin" / "sing_box" / "sing-box.exe",
            home / "Desktop" / "v2rayN-windows-64" / "bin" / "sing_box" / "sing-box.exe",
            home / "Desktop" / "v2rayN-windows-64" / "bin" / "sing-box.exe",
        ]
        unique: list[Path] = []
        seen: set[str] = set()
        for path in candidates:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    @staticmethod
    def _candidate_xray_paths() -> list[Path]:
        home = Path.home()
        candidates = [
            BUNDLE_DIR / "bin" / "xray" / "xray.exe",
            APP_DIR / "bin" / "xray" / "xray.exe",
            home / "Desktop" / "v2rayN-windows-64" / "bin" / "xray" / "xray.exe",
            home / "Desktop" / "v2rayN-windows-64" / "bin" / "xray.exe",
        ]
        unique: list[Path] = []
        seen: set[str] = set()
        for path in candidates:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    @classmethod
    def find_sing_box_executable(cls) -> Path:
        env_path = os.environ.get("SING_BOX_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return p
        which_path = shutil.which("sing-box")
        if which_path:
            return Path(which_path)
        for p in cls._candidate_sing_box_paths():
            if p.exists():
                return p
        raise RuntimeError("未找到 sing-box.exe，请先安装 sing-box 或 v2rayN/sing-box 内核")

    @classmethod
    def find_xray_executable(cls) -> Path:
        env_path = os.environ.get("XRAY_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return p
        which_path = shutil.which("xray")
        if which_path:
            return Path(which_path)
        for p in cls._candidate_xray_paths():
            if p.exists():
                return p
        raise RuntimeError("未找到 xray.exe，请先安装 xray 或 v2rayN/xray 内核")

    def _stop_locked(self) -> None:
        proc = self._proc
        config_path = self._config_path
        log_handle = self._log_handle
        self._proc = None
        self._raw_source = ""
        self._local_proxy_url = ""
        self._config_path = None
        self._log_handle = None
        self._backend = ""
        if proc is None:
            if log_handle is not None:
                try:
                    log_handle.close()
                except Exception:
                    pass
            if config_path is not None:
                try:
                    Path(config_path).unlink(missing_ok=True)
                except Exception:
                    pass
            return
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        finally:
            if log_handle is not None:
                try:
                    log_handle.close()
                except Exception:
                    pass
            if config_path is not None:
                try:
                    Path(config_path).unlink(missing_ok=True)
                except Exception:
                    pass

    def stop(self) -> None:
        with self._lock:
            self._stop_locked()

    @staticmethod
    def _is_process_alive(proc) -> bool:
        return proc is not None and getattr(proc, "poll", lambda: 1)() is None

    def _runtime_file_path(self, prefix: str, suffix: str) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        token = uuid.uuid4().hex[:10]
        return self.work_dir / f"{prefix}_{os.getpid()}_{token}{suffix}"

    def _runtime_log_path(self) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        return self.work_dir / f"exchange_proxy_runtime_{os.getpid()}.log"

    def _read_log_tail(self, max_chars: int = 1200) -> str:
        log_path = self._log_path
        if not log_path or not Path(log_path).exists():
            return ""
        try:
            text = Path(log_path).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""
        text = text.strip()
        if not text:
            return ""
        return text[-max_chars:].strip()

    def _open_runtime_log(self):
        log_path = self._runtime_log_path()
        handle = open(log_path, "ab")
        self._log_path = log_path
        return handle

    @staticmethod
    def _hidden_process_kwargs() -> dict[str, object]:
        kwargs: dict[str, object] = {}
        if os.name != "nt":
            return kwargs
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        if creationflags:
            kwargs["creationflags"] = creationflags
        startupinfo_factory = getattr(subprocess, "STARTUPINFO", None)
        if startupinfo_factory is None:
            return kwargs
        startupinfo = startupinfo_factory()
        startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
        startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        kwargs["startupinfo"] = startupinfo
        return kwargs

    def _format_backend_error(self, backend: str, exc: Exception) -> RuntimeError:
        tail = self._read_log_tail()
        msg = f"{backend} 启动失败: {exc}"
        if tail:
            msg = f"{msg} | runtime_log={self._log_path} | tail={tail}"
        return RuntimeError(msg)

    def _write_sing_box_config(self, ss_info: dict[str, object], listen_port: int) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        config_path = self._runtime_file_path("exchange_ss_proxy", ".json")
        cfg = {
            "log": {"level": "warn"},
            "inbounds": [
                {
                    "type": "mixed",
                    "tag": "mixed-in",
                    "listen": "127.0.0.1",
                    "listen_port": listen_port,
                }
            ],
            "outbounds": [
                {
                    "type": "shadowsocks",
                    "tag": "ss-out",
                    "server": ss_info["server"],
                    "server_port": ss_info["server_port"],
                    "method": ss_info["method"],
                    "password": ss_info["password"],
                    "network": ss_info["network"],
                },
                {"type": "direct", "tag": "direct"},
            ],
            "route": {"final": "ss-out"},
        }
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return config_path

    def _write_xray_config(self, ss_info: dict[str, object], listen_port: int) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        config_path = self._runtime_file_path("exchange_ss_proxy_xray", ".json")
        cfg = {
            "log": {"loglevel": "warning"},
            "inbounds": [
                {
                    "listen": "127.0.0.1",
                    "port": listen_port,
                    "protocol": "socks",
                    "settings": {"udp": True},
                }
            ],
            "outbounds": [
                {
                    "protocol": "shadowsocks",
                    "settings": {
                        "servers": [
                            {
                                "address": ss_info["server"],
                                "port": ss_info["server_port"],
                                "method": ss_info["method"],
                                "password": ss_info["password"],
                            }
                        ]
                    },
                    "streamSettings": {"network": ss_info["network"]},
                    "tag": "ss-out",
                }
            ],
        }
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return config_path

    def _wait_ready(self, local_proxy_url: str, timeout_sec: float = 12.0) -> None:
        deadline = time.time() + timeout_sec
        proxies = {"http": local_proxy_url, "https": local_proxy_url}
        last_err = ""
        while time.time() < deadline:
            try:
                resp = http_get_via_proxy("https://api.ipify.org", proxies=proxies, timeout=4)
                resp.raise_for_status()
                return
            except Exception as exc:
                last_err = str(exc)
                time.sleep(0.4)
        raise RuntimeError(f"SS 代理启动失败：{last_err or '本地代理未就绪'}")

    def ensure_proxy(self, ss_uri: str) -> str:
        raw = str(ss_uri or "").strip()
        if not raw:
            return ""
        with self._lock:
            if raw == self._raw_source and self._is_process_alive(self._proc) and self._local_proxy_url:
                return self._local_proxy_url
            self._stop_locked()
            ss_info = self.parse_ss_uri(raw)
            listen_port = self._allocate_local_port()
            last_exc = None
            proc = None
            config_path = None
            local_proxy_url = ""
            runtime_log_handle = None
            for backend in ("xray", "sing-box"):
                try:
                    runtime_log_handle = self._open_runtime_log()
                    if backend == "xray":
                        exe = self.find_xray_executable()
                        config_path = self._write_xray_config(ss_info, listen_port)
                        proc = subprocess.Popen(
                            [str(exe), "run", "-c", str(config_path)],
                            stdout=runtime_log_handle,
                            stderr=runtime_log_handle,
                            **self._hidden_process_kwargs(),
                        )
                        local_proxy_url = f"socks5h://127.0.0.1:{listen_port}"
                    else:
                        exe = self.find_sing_box_executable()
                        config_path = self._write_sing_box_config(ss_info, listen_port)
                        proc = subprocess.Popen(
                            [str(exe), "run", "-c", str(config_path)],
                            stdout=runtime_log_handle,
                            stderr=runtime_log_handle,
                            **self._hidden_process_kwargs(),
                        )
                        local_proxy_url = f"http://127.0.0.1:{listen_port}"
                    self._wait_ready(local_proxy_url)
                    break
                except Exception as exc:
                    last_exc = self._format_backend_error(backend, exc)
                    if proc is not None:
                        try:
                            proc.terminate()
                            proc.wait(timeout=5)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                    if runtime_log_handle is not None:
                        try:
                            runtime_log_handle.close()
                        except Exception:
                            pass
                        runtime_log_handle = None
                    if config_path is not None:
                        try:
                            Path(config_path).unlink(missing_ok=True)
                        except Exception:
                            pass
                    proc = None
                    config_path = None
                    local_proxy_url = ""
            if proc is None or not local_proxy_url:
                raise last_exc or RuntimeError("SS 代理启动失败")
            self._proc = proc
            self._raw_source = raw
            self._local_proxy_url = local_proxy_url
            self._config_path = config_path
            self._log_handle = runtime_log_handle
            self._backend = backend
            return local_proxy_url

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return {
                "backend": self._backend,
                "raw_source": self._raw_source,
                "local_proxy_url": self._local_proxy_url,
            }


def http_get_via_proxy(
    url: str,
    *,
    proxies: dict[str, str] | None = None,
    timeout: int = 10,
    headers: dict[str, str] | None = None,
    allow_system_proxy: bool = True,
):
    session = requests.Session()
    session.trust_env = bool(allow_system_proxy) and not bool(proxies)
    if proxies:
        session.proxies.update(proxies)
    return session.get(url, timeout=timeout, headers=headers or {})

# ====================== 日志 & 队列 ======================
log_queue = queue.Queue()
LOG_FILE_PATH = "bot_log.txt"
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 2

_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
logger.propagate = False


class TkLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.put(msg)
        except Exception:
            pass


class SafeRotatingFileHandler(RotatingFileHandler):
    def doRollover(self):
        try:
            super().doRollover()
        except PermissionError:
            # Windows 下另一个进程占用日志文件时，跳过轮转但继续写当前文件。
            return


_tk_handler = TkLogHandler()
_tk_handler.setFormatter(_formatter)
logger.addHandler(_tk_handler)

try:
    _file_handler = SafeRotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=LOG_FILE_MAX_BYTES,
        backupCount=LOG_FILE_BACKUP_COUNT,
        encoding="utf-8",
    )
    _file_handler.setFormatter(_formatter)
    logger.addHandler(_file_handler)
except Exception as e:
    print(f"无法创建日志文件: {e}")


# ====================== 自定义错误 & 装饰器 ======================
class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance API error {code}: {msg}")


def retry_request(max_retries=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    logger.warning(f"网络请求失败 ({i + 1}/{max_retries}): {e}，将在 {delay} 秒后重试...")
                    time.sleep(delay)
                except BinanceAPIError as e:
                    last_exception = e
                    if e.code in [-1001, -1003]:
                        logger.warning(f"Binance 系统繁忙 ({i + 1}/{max_retries}): {e}，重试中...")
                        time.sleep(delay)
                    else:
                        raise e
            logger.error(f"重试 {max_retries} 次后仍然失败。")
            if last_exception:
                raise last_exception
            raise RuntimeError("未知错误：重试失败")
        return wrapper
    return decorator


# ====================== Binance 客户端 ======================
class BinanceClient:
    SERVER_TIME_CACHE_TTL_SECONDS = 30.0
    DEFAULT_RECV_WINDOW_MS = 10000

    def __init__(self, key: str, secret: str, proxy_url: str = ""):
        if not key or not secret:
            raise ValueError("API KEY / SECRET 不能为空")

        self.key = key
        self.secret = secret.encode()
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": key})
        try:
            self.recv_window_ms = max(1000, int(self.DEFAULT_RECV_WINDOW_MS))
        except Exception:
            self.recv_window_ms = self.DEFAULT_RECV_WINDOW_MS
        proxy = str(proxy_url or "").strip()
        self.session.trust_env = not bool(proxy)
        if proxy:
            self.session.proxies.update({
                "http": proxy,
                "https": proxy,
            })

        self.spot = "https://api.binance.com"
        self.um_futures = "https://fapi.binance.com"
        self.cm_futures = "https://dapi.binance.com"

        self._exchange_info_cache = {}
        self._price_cache = {}
        self._server_time_offset_ms = {}
        self._server_time_synced_at = {}
        self._server_time_lock = threading.Lock()

    @staticmethod
    def _normalize_base_url(base: str) -> str:
        return str(base or "").rstrip("/")

    def _server_time_path(self, base: str) -> str:
        base_n = self._normalize_base_url(base)
        if base_n == self._normalize_base_url(self.um_futures):
            return "/fapi/v1/time"
        if base_n == self._normalize_base_url(self.cm_futures):
            return "/dapi/v1/time"
        return "/api/v3/time"

    @staticmethod
    def _local_timestamp_ms() -> int:
        return int(time.time() * 1000)

    def _fetch_server_time_ms(self, base: str) -> int:
        base_n = self._normalize_base_url(base)
        url = base_n + self._server_time_path(base_n)
        start_ms = self._local_timestamp_ms()
        r = self.session.get(url, timeout=10)
        end_ms = self._local_timestamp_ms()
        r.raise_for_status()
        data = r.json()
        try:
            server_time_ms = int(data.get("serverTime"))
        except Exception as exc:
            raise RuntimeError(f"服务器时间接口返回异常：{data}") from exc
        midpoint_ms = (start_ms + end_ms) // 2
        return int(server_time_ms - midpoint_ms)

    def _sync_server_time_offset(self, base: str, *, force: bool = False) -> int:
        base_n = self._normalize_base_url(base)
        now_mono = time.monotonic()
        with self._server_time_lock:
            cached_offset = self._server_time_offset_ms.get(base_n)
            synced_at = self._server_time_synced_at.get(base_n, 0.0)
            if (not force) and cached_offset is not None and (now_mono - synced_at) < self.SERVER_TIME_CACHE_TTL_SECONDS:
                return int(cached_offset)

        offset_ms = self._fetch_server_time_ms(base_n)
        with self._server_time_lock:
            self._server_time_offset_ms[base_n] = int(offset_ms)
            self._server_time_synced_at[base_n] = time.monotonic()
        return int(offset_ms)

    def _signed_params(self, base: str, params=None, *, force_time_sync: bool = False) -> dict[str, Any]:
        signed_params = dict(params or {})
        offset_ms = 0
        try:
            offset_ms = self._sync_server_time_offset(base, force=force_time_sync)
        except Exception:
            if force_time_sync:
                raise
            base_n = self._normalize_base_url(base)
            with self._server_time_lock:
                cached_offset = self._server_time_offset_ms.get(base_n)
            if cached_offset is not None:
                offset_ms = int(cached_offset)

        signed_params["timestamp"] = self._local_timestamp_ms() + int(offset_ms)
        signed_params["recvWindow"] = self.recv_window_ms
        signed_params["signature"] = self.sign(signed_params)
        return signed_params

    def sign(self, params: Dict[str, Any]):
        return hmac.new(
            self.secret, urlencode(params, True).encode(), hashlib.sha256
        ).hexdigest()

    @retry_request(max_retries=3, delay=1)
    def request(self, base, method, path, params=None):
        url = base + path
        last_error = None

        for attempt in range(2):
            signed_params = self._signed_params(base, params, force_time_sync=(attempt > 0))

            if method == "GET":
                r = self.session.get(url, params=signed_params, timeout=15)
            else:
                r = self.session.request(method, url, data=signed_params, timeout=15)

            try:
                data = r.json()
            except Exception:
                r.raise_for_status()

            if r.status_code == 200:
                return data

            err = BinanceAPIError(data.get("code", -1), data.get("msg", "Unknown"))
            last_error = err
            if err.code == -1021 and attempt == 0:
                try:
                    self._sync_server_time_offset(base, force=True)
                    logger.warning("Binance 返回 -1021，已重新同步服务器时间并重试一次")
                    continue
                except Exception as sync_exc:
                    logger.warning("Binance 返回 -1021，但服务器时间同步失败: %s", sync_exc)
            raise err

        if last_error:
            raise last_error
        raise RuntimeError("签名请求失败")

    @retry_request(max_retries=3, delay=1)
    def public_get(self, base, path, params=None):
        params = params or {}
        url = base + path
        r = self.session.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # -------- 余额 --------
    def spot_balance(self, asset: str) -> float:
        data = self.request(self.spot, "GET", "/api/v3/account")
        for b in data.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", 0))
        return 0.0

    def spot_all_balances(self):
        data = self.request(self.spot, "GET", "/api/v3/account")
        result = []
        for b in data.get("balances", []):
            free_amt = float(b.get("free", 0) or 0)
            locked_amt = float(b.get("locked", 0) or 0)
            total = free_amt + locked_amt
            if total > 0:
                result.append({
                    "asset": b.get("asset"),
                    "free": free_amt,
                    "locked": locked_amt,
                    "total": total,
                })
        return result

    def query_total_wallet_balance(self, quote_asset="USDT"):
        data = self.request(
            self.spot,
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": quote_asset},
        )
        total = Decimal("0")
        rows = []
        for item in data:
            bal = Decimal(str(item.get("balance", "0")))
            wallet_name = item.get("walletName", "Unknown")
            active = bool(item.get("activate", False))
            rows.append({
                "walletName": wallet_name,
                "balance": bal,
                "activate": active,
            })
            total += bal
        return total, rows

    # -------- 工具：从 symbol 推断现货基础币种 --------
    @staticmethod
    def split_spot_symbol(symbol: str) -> tuple[str, str]:
        symbol_u = str(symbol or "").strip().upper()
        for suffix in SUPPORTED_QUOTE_ASSET_SUFFIXES:
            if symbol_u.endswith(suffix) and len(symbol_u) > len(suffix):
                return symbol_u[:-len(suffix)], suffix
        return "BTC", "USDT"

    @classmethod
    def get_spot_base_asset(cls, symbol: str) -> str:
        base_asset, _quote_asset = cls.split_spot_symbol(symbol)
        return base_asset

    @classmethod
    def get_spot_quote_asset(cls, symbol: str) -> str:
        _base_asset, quote_asset = cls.split_spot_symbol(symbol)
        return quote_asset

    # -------- 公共行情 / 交易规则 --------
    def get_exchange_info(self, symbol: str):
        symbol = symbol.upper()
        if symbol in self._exchange_info_cache:
            return self._exchange_info_cache[symbol]
        data = self.public_get(self.spot, "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = data.get("symbols", [])
        if not symbols:
            return None
        info = symbols[0]
        self._exchange_info_cache[symbol] = info
        return info

    def get_symbol_price(self, symbol: str) -> Optional[Decimal]:
        symbol = symbol.upper()
        try:
            data = self.public_get(self.spot, "/api/v3/ticker/price", {"symbol": symbol})
            price = Decimal(str(data.get("price")))
            self._price_cache[symbol] = price
            return price
        except Exception:
            return None

    @staticmethod
    def _extract_filter(symbol_info: dict, filter_type: str):
        for f in symbol_info.get("filters", []):
            if f.get("filterType") == filter_type:
                return f
        return None

    @staticmethod
    def _decimal_from_str(v, default="0"):
        try:
            return Decimal(str(v))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(default)

    @staticmethod
    def _floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return qty
        return (qty // step) * step

    @staticmethod
    def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        if value <= 0:
            return Decimal("0")
        units = (value / step).to_integral_value(rounding=ROUND_UP)
        return units * step

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        text = format(Decimal(str(value)).normalize(), "f")
        if text in {"-0", "-0.0"}:
            return "0"
        return text

    def get_symbol_trade_rules(self, symbol: str):
        info = self.get_exchange_info(symbol)
        if not info:
            return None

        lot = self._extract_filter(info, "LOT_SIZE")
        min_notional = self._extract_filter(info, "MIN_NOTIONAL")
        notional = self._extract_filter(info, "NOTIONAL")
        price_filter = self._extract_filter(info, "PRICE_FILTER")

        step_size = self._decimal_from_str((lot or {}).get("stepSize", "0.00000001"), "0.00000001")
        min_qty = self._decimal_from_str((lot or {}).get("minQty", "0"), "0")
        max_qty = self._decimal_from_str((lot or {}).get("maxQty", "999999999"), "999999999")
        tick_size = self._decimal_from_str((price_filter or {}).get("tickSize", "0.00000001"), "0.00000001")
        min_price = self._decimal_from_str((price_filter or {}).get("minPrice", "0"), "0")
        max_price = self._decimal_from_str((price_filter or {}).get("maxPrice", "999999999"), "999999999")

        min_notional_val = Decimal("0")
        if min_notional:
            min_notional_val = self._decimal_from_str(min_notional.get("minNotional", "0"), "0")
        elif notional:
            min_notional_val = self._decimal_from_str(notional.get("minNotional", "0"), "0")

        return {
            "stepSize": step_size,
            "minQty": min_qty,
            "maxQty": max_qty,
            "tickSize": tick_size,
            "minPrice": min_price,
            "maxPrice": max_price,
            "minNotional": min_notional_val,
            "status": info.get("status"),
            "quoteAsset": info.get("quoteAsset"),
            "baseAsset": info.get("baseAsset"),
        }

    def get_book_ticker(self, symbol: str) -> dict[str, Decimal]:
        symbol_u = symbol.upper()
        data = self.public_get(self.spot, "/api/v3/ticker/bookTicker", {"symbol": symbol_u})
        bid_price = self._decimal_from_str(data.get("bidPrice"), "0")
        ask_price = self._decimal_from_str(data.get("askPrice"), "0")
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"读取盘口失败：{symbol_u} bid/ask 无效")
        return {
            "bidPrice": bid_price,
            "askPrice": ask_price,
        }

    def place_limit_order(self, symbol: str, side: str, quantity: Decimal, price: Decimal):
        symbol_u = symbol.upper()
        side_u = side.upper()
        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过挂单 %s", symbol_u)
            return None
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过挂单", symbol_u)
            return None

        qty = self._floor_to_step(Decimal(str(quantity)), rules["stepSize"])
        order_price = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0 and self._floor_to_step(order_price, tick_size) != order_price:
            raise RuntimeError(f"挂单价格不符合最小价格精度：{order_price}")

        if qty <= 0 or qty < rules["minQty"]:
            logger.info("交易对 %s 挂单数量过小，跳过", symbol_u)
            return None

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        if rules["minNotional"] > 0 and (qty * order_price) < rules["minNotional"]:
            logger.info(
                "交易对 %s 挂单金额 %s 小于最小下单额 %s，跳过",
                symbol_u,
                self._format_decimal(qty * order_price),
                self._format_decimal(rules["minNotional"]),
            )
            return None

        params = {
            "symbol": symbol_u,
            "side": side_u,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self._format_decimal(qty),
            "price": self._format_decimal(order_price),
        }
        data = self.request(self.spot, "POST", "/api/v3/order", params)
        logger.info(
            "限价挂单 %s %s：数量=%s 价格=%s",
            symbol_u,
            side_u,
            params["quantity"],
            params["price"],
        )
        return data

    def get_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "GET",
            "/api/v3/order",
            {
                "symbol": symbol.upper(),
                "orderId": int(order_id),
            },
        )

    def cancel_order(self, symbol: str, order_id: int | str):
        return self.request(
            self.spot,
            "DELETE",
            "/api/v3/order",
            {
                "symbol": symbol.upper(),
                "orderId": int(order_id),
            },
        )

    def wait_order_filled(self, symbol: str, order_id: int | str, stop_event=None, poll_interval: float = 1.0):
        symbol_u = symbol.upper()
        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")
            time.sleep(max(0.2, float(poll_interval)))

    def get_order_average_price(self, order_data: dict) -> Decimal | None:
        if not isinstance(order_data, dict):
            return None
        try:
            executed_qty = Decimal(str(order_data.get("executedQty", "0")))
        except Exception:
            executed_qty = Decimal("0")
        try:
            quote_qty = Decimal(str(order_data.get("cummulativeQuoteQty", "0")))
        except Exception:
            quote_qty = Decimal("0")
        if executed_qty > 0 and quote_qty > 0:
            try:
                return quote_qty / executed_qty
            except Exception:
                pass
        try:
            price = Decimal(str(order_data.get("price", "0")))
        except Exception:
            price = Decimal("0")
        return price if price > 0 else None

    def spot_limit_buy_all_usdt(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        quote_asset = self.get_spot_quote_asset(symbol)
        quote_balance = Decimal(str(self.spot_balance(quote_asset)))
        if quote_balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单买入", quote_asset)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单买入价格无效，跳过")
            return None

        amount_quote = quote_balance * Decimal(str(reserve_ratio))
        qty = self._floor_to_step(amount_quote / price_dec, rules["stepSize"])
        return self.place_limit_order(symbol, "BUY", qty, price_dec)

    def spot_buy_quote_amount(self, symbol: str, quote_amount: Decimal | str | float | int):
        symbol_u = str(symbol or "").strip().upper()
        amount = Decimal(str(quote_amount))
        if amount <= 0:
            logger.info("买入金额 <= 0，跳过买入 %s", symbol_u)
            return False

        rules = self.get_symbol_trade_rules(symbol_u)
        if not rules:
            logger.info("找不到交易对规则，跳过买入 %s", symbol_u)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过买入", symbol_u)
            return False

        quote_asset = self.get_spot_quote_asset(symbol_u)
        quote_balance = Decimal(str(self.spot_balance(quote_asset)))
        if quote_balance < amount:
            raise RuntimeError(
                f"现货 {quote_asset} 余额不足：需要 {self._format_decimal(amount)}，当前 {self._format_decimal(quote_balance)}"
            )

        if rules["minNotional"] > 0 and amount < rules["minNotional"]:
            raise RuntimeError(
                f"{symbol_u} 买入金额过小：{self._format_decimal(amount)} < 最小下单额 {self._format_decimal(rules['minNotional'])}"
            )

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol_u,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": self._format_decimal(amount),
            },
        )
        logger.info("现货市价买入 %s，使用 %s 金额 %s", symbol_u, quote_asset, self._format_decimal(amount))
        return True

    def buy_bnb_with_quote_amount(self, quote_asset: str, amount: Decimal | str | float | int):
        quote_asset_u = str(quote_asset or "").strip().upper()
        if not quote_asset_u:
            raise RuntimeError("后置币种为空，无法预买 BNB")
        if quote_asset_u == "BNB":
            logger.info("后置币种为 BNB，无需预买 BNB")
            return False
        symbol = f"BNB{quote_asset_u}"
        return self.spot_buy_quote_amount(symbol, amount)

    def spot_limit_sell_all_base(self, symbol: str, price: Decimal, reserve_ratio: Decimal = Decimal("1")):
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return None

        base = self.get_spot_base_asset(symbol)
        balance = Decimal(str(self.spot_balance(base)))
        if balance <= 0:
            logger.info("现货 %s 余额不足，跳过挂单卖出", base)
            return None

        price_dec = Decimal(str(price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size > 0:
            price_dec = self._floor_to_step(price_dec, tick_size)
        if price_dec <= 0:
            logger.info("挂单卖出价格无效，跳过")
            return None

        qty = self._floor_to_step(balance * Decimal(str(reserve_ratio)), rules["stepSize"])
        return self.place_limit_order(symbol, "SELL", qty, price_dec)

    def adjust_price_to_valid_tick(self, symbol: str, target_price: Decimal, *, round_up: bool = False) -> Decimal:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            return Decimal(str(target_price))
        price = Decimal(str(target_price))
        tick_size = Decimal(str(rules.get("tickSize", "0") or "0"))
        if tick_size <= 0:
            return price
        normalized = self._floor_to_step(price, tick_size)
        if normalized == price:
            return price
        if round_up:
            return self._ceil_to_step(price, tick_size)
        return normalized

    def sell_asset_market(self, symbol: str, free_balance: float, reserve_ratio=Decimal("0.999")) -> bool:
        rules = self.get_symbol_trade_rules(symbol)
        if not rules:
            logger.info("找不到交易对规则，跳过卖出 %s", symbol)
            return False
        if rules["status"] != "TRADING":
            logger.info("交易对 %s 非 TRADING，跳过", symbol)
            return False

        qty = Decimal(str(free_balance)) * reserve_ratio
        qty = self._floor_to_step(qty, rules["stepSize"])

        if qty <= 0 or qty < rules["minQty"]:
            logger.info("交易对 %s 数量过小，跳过卖出", symbol)
            return False

        if qty > rules["maxQty"]:
            qty = rules["maxQty"]

        price = self.get_symbol_price(symbol)
        if price:
            notional = qty * price
            if rules["minNotional"] > 0 and notional < rules["minNotional"]:
                logger.info("交易对 %s 名义价值 %.8f 小于最小下单额 %.8f，跳过",
                            symbol, float(notional), float(rules["minNotional"]))
                return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": format(qty.normalize(), "f"),
            },
        )
        logger.info("现货市价卖出 %s，数量 %s", symbol, format(qty.normalize(), "f"))
        return True

    def find_usdt_symbol_for_asset(self, asset: str) -> Optional[str]:
        asset = asset.upper()
        if asset in ("USDT", "BNB"):
            return None
        symbol = f"{asset}USDT"
        info = self.get_exchange_info(symbol)
        if info and info.get("status") == "TRADING":
            return symbol
        return None

    def sell_large_spot_assets_to_usdt(self, skip_assets=None):
        skip_assets = set(a.upper() for a in (skip_assets or []))
        skip_assets.update({"USDT", "BNB"})

        sold_assets = []
        balances = self.spot_all_balances()
        for item in balances:
            asset = (item.get("asset") or "").upper()
            free_amt = float(item.get("free", 0) or 0)

            if asset in skip_assets or free_amt <= 0:
                continue

            symbol = self.find_usdt_symbol_for_asset(asset)
            if not symbol:
                logger.info("未找到 %sUSDT 交易对，跳过 %s", asset, asset)
                continue

            try:
                ok = self.sell_asset_market(symbol, free_amt)
                if ok:
                    sold_assets.append(asset)
                    time.sleep(0.3)
            except Exception as e:
                logger.warning("卖出大额币 %s 失败: %s", asset, e)

        logger.info("大额币卖出为 USDT 完成，成功处理 %d 个币种", len(sold_assets))
        return sold_assets

    # -------- 现货买入（全部 USDT） --------
    def spot_buy_all_usdt(self, buffer=0.2, symbol="BTCUSDT"):
        quote_asset = self.get_spot_quote_asset(symbol)
        quote_balance = self.spot_balance(quote_asset)
        if quote_balance <= buffer:
            logger.info("现货 %s %.8f <= buffer %.8f，跳过买入", quote_asset, quote_balance, buffer)
            return False

        amount = (quote_balance - buffer) * 0.999
        if amount <= 0:
            logger.info("可用 %s 金额太小，跳过买入", quote_asset)
            return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{amount:.8f}",
            },
        )
        logger.info("现货市价买入 %s，使用 %s 金额 %.8f", symbol, quote_asset, amount)
        return True

    # -------- 现货卖出（全部基础币，精度由参数决定） --------
    def spot_sell_all_base(self, symbol: str, precision: int):
        base = self.get_spot_base_asset(symbol)
        balance = self.spot_balance(base)

        if balance <= 0:
            return False

        qty = Decimal(str(balance)) * Decimal("0.999")
        if precision < 0:
            precision = 0
        step = Decimal("1").scaleb(-precision)
        qty = qty.quantize(step, rounding=ROUND_DOWN)

        if qty <= step:
            return False

        self.request(
            self.spot,
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": str(qty),
            },
        )
        logger.info("现货市价卖出 %s 数量 %s（基础币 %s）", symbol, qty, base)
        return True

    # -------- 提现 --------
    def withdraw_all_coin(
        self,
        coin: str,
        address: str,
        network: str,
        fee_buffer: float = WITHDRAW_FEE_BUFFER_DEFAULT,
        enable_withdraw: bool = True,
    ) -> float:
        balance = self.spot_balance(coin)
        amount = balance - fee_buffer
        if amount <= 0:
            logger.info("%s 余额 %.8f 不足以提现（需 > buffer %.8f）", coin, balance, fee_buffer)
            return 0.0

        params = {
            "coin": coin,
            "address": address,
            "network": network,
            "amount": f"{amount:.8f}",
        }

        if enable_withdraw:
            self.request(
                self.spot,
                "POST",
                "/sapi/v1/capital/withdraw/apply",
                params,
            )
            logger.info("已提交提现 %.8f %s → %s (%s)", amount, coin, address, network)
            return amount
        else:
            logger.info("提现开关关闭，仅打印参数: %s", params)
            return 0.0

    # -------- 现货正余额资产 --------
    def spot_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v3/asset/getUserAsset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- 资金账户正余额资产 --------
    def funding_positive_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
        )
        result = []
        for item in data:
            free_amt = float(item.get("free", 0) or 0)
            if free_amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "free": free_amt,
                })
        return result

    # -------- U本位合约可转出余额 --------
    def um_futures_transferable_assets(self):
        data = self.request(
            self.um_futures,
            "GET",
            "/fapi/v2/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("maxWithdrawAmount", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 币本位合约可转出余额 --------
    def cm_futures_transferable_assets(self):
        data = self.request(
            self.cm_futures,
            "GET",
            "/dapi/v1/balance",
            {},
        )
        result = []
        for item in data:
            amt = float(item.get("withdrawAvailable", 0) or 0)
            if amt > 0:
                result.append({
                    "asset": item.get("asset"),
                    "amount": amt,
                })
        return result

    # -------- 通用划转 --------
    def universal_transfer(self, transfer_type: str, asset: str, amount: float):
        if amount <= 0:
            return False

        amount_dec = Decimal(str(amount))
        if amount_dec <= 0:
            return False

        amt_str = format(amount_dec.normalize(), "f")
        if amt_str == "0":
            return False

        self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/transfer",
            {
                "type": transfer_type,
                "asset": asset,
                "amount": amt_str,
            },
        )
        logger.info("划转成功: %s %s %s", transfer_type, asset, amt_str)
        return True

    # -------- 归集合约/资金到现货 --------
    def collect_all_to_spot(self):
        total_count = 0

        try:
            items = self.um_futures_transferable_assets()
            if items:
                logger.info("检测到 U本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["amount"]
                try:
                    self.universal_transfer("UMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("U本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 U本位合约余额失败: %s", e)

        try:
            items = self.cm_futures_transferable_assets()
            if items:
                logger.info("检测到 币本位可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["amount"]
                try:
                    self.universal_transfer("CMFUTURE_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("币本位划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询 币本位合约余额失败: %s", e)

        try:
            items = self.funding_positive_assets()
            if items:
                logger.info("检测到 资金账户可划转资产 %d 项", len(items))
            for item in items:
                asset = item["asset"]
                amount = item["free"]
                try:
                    self.universal_transfer("FUNDING_MAIN", asset, amount)
                    total_count += 1
                except Exception as e:
                    logger.warning("资金账户划转失败 %s %.8f: %s", asset, amount, e)
        except Exception as e:
            logger.warning("查询资金账户余额失败: %s", e)

        logger.info("归集到现货完成，共处理 %d 项资产", total_count)
        return total_count

    # -------- 查询现货可小额兑换资产 --------
    def get_spot_dust_assets(self):
        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust-btc",
            {"accountType": "SPOT"},
        )

        assets = []
        for item in data.get("details", []):
            asset = item.get("asset")
            amount_free = float(item.get("amountFree", 0) or 0)
            if asset and amount_free > 0 and asset != "BNB":
                assets.append(asset)
        return assets

    # -------- 小额兑换：现货可兑换资产 -> BNB --------
    def convert_spot_dust_to_bnb(self):
        assets = self.get_spot_dust_assets()
        if not assets:
            logger.info("没有可进行小额兑换的现货资产")
            return []

        data = self.request(
            self.spot,
            "POST",
            "/sapi/v1/asset/dust",
            {
                "asset": assets,
                "accountType": "SPOT",
            },
        )
        result = data.get("transferResult", [])
        logger.info("小额兑换完成，兑换资产数: %d", len(result))
        return result


# ====================== 策略 ======================
class Strategy:
    def __init__(
        self,
        client,
        spot_rounds,
        withdraw_coin,
        withdraw_address,
        withdraw_network,
        withdraw_fee_buffer,
        spot_symbol,
        spot_precision,
        sleep_fn,
        enable_withdraw,
        withdraw_callback=None,
        trade_mode: str = TRADE_MODE_DEFAULT,
        premium_percent: Decimal | None = None,
        bnb_fee_stop_value: Decimal | None = None,
        bnb_topup_amount: Decimal | None = None,
        reprice_threshold_percent: Decimal | None = None,
    ):
        self.c = client
        self.spot_rounds = spot_rounds
        self.withdraw_coin = withdraw_coin
        self.withdraw_address = withdraw_address
        self.withdraw_network = withdraw_network
        self.withdraw_fee_buffer = withdraw_fee_buffer
        self.spot_symbol = spot_symbol
        self.spot_precision = spot_precision
        self.sleep_fn = sleep_fn
        self.enable_withdraw = enable_withdraw
        self.withdraw_callback = withdraw_callback
        self.trade_mode = str(trade_mode or TRADE_MODE_DEFAULT)
        self.premium_percent = Decimal(str(premium_percent if premium_percent is not None else "0"))
        self.bnb_fee_stop_value = Decimal(str(bnb_fee_stop_value if bnb_fee_stop_value is not None else "0"))
        self.bnb_topup_amount = Decimal(str(bnb_topup_amount if bnb_topup_amount is not None else "0"))
        self.reprice_threshold_percent = Decimal(
            str(reprice_threshold_percent if reprice_threshold_percent is not None else REPRICE_THRESHOLD_PERCENT_DEFAULT)
        )

    def ensure_base_sold(self):
        try:
            sold = self.c.spot_sell_all_base(self.spot_symbol, self.spot_precision)
            if sold:
                logger.info("【补救措施】检测到残留基础币，已执行补充卖出。")
        except Exception as e:
            logger.warning(f"补救卖出时发生错误（可忽略）: {e}")

    def _mode_name(self) -> str:
        mode = str(self.trade_mode or TRADE_MODE_DEFAULT)
        return mode if mode in TRADE_MODE_OPTIONS else TRADE_MODE_DEFAULT

    def _limit_like_mode(self) -> bool:
        return self._mode_name() in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}

    def _should_stop_for_bnb_fee(self) -> bool:
        if not self._limit_like_mode():
            return False
        threshold = Decimal(str(self.bnb_fee_stop_value or "0"))
        if threshold < 0:
            threshold = Decimal("0")
        current_bnb = Decimal(str(self.c.spot_balance("BNB")))
        if current_bnb < threshold:
            logger.info(
                "%s模式停止：当前 BNB 手续费余额 %s 低于停止值 %s",
                self._mode_name(),
                BinanceClient._format_decimal(current_bnb),
                BinanceClient._format_decimal(threshold),
            )
            return True
        return False

    def _premium_sell_price(self, buy_price: Decimal) -> Decimal:
        premium_ratio = Decimal("1") + (Decimal(str(self.premium_percent or "0")) / Decimal("100"))
        desired_price = Decimal(str(buy_price)) * premium_ratio
        return self.c.adjust_price_to_valid_tick(self.spot_symbol, desired_price, round_up=True)

    def _reprice_threshold_ratio(self) -> Decimal:
        percent = Decimal(str(self.reprice_threshold_percent or "0"))
        if percent < 0:
            percent = Decimal("0")
        return percent / Decimal("100")

    def _run_bnb_topup_if_needed(self):
        topup_amount = Decimal(str(self.bnb_topup_amount or "0"))
        if topup_amount <= 0:
            return False
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        logger.info(
            "开始预买 BNB：使用 %s 金额 %s",
            quote_asset,
            BinanceClient._format_decimal(topup_amount),
        )
        bought = self.c.buy_bnb_with_quote_amount(quote_asset, topup_amount)
        if bought:
            logger.info("预买 BNB 完成")
        else:
            logger.info("预买 BNB 未执行")
        return bool(bought)

    def _prepare_quote_by_selling_base(self, stop_event, ask_price: Decimal, mode_name: str) -> bool:
        sell_order = self.c.spot_limit_sell_all_base(symbol=self.spot_symbol, price=ask_price)
        if not sell_order:
            return False

        sell_order_id = sell_order.get("orderId")
        if not sell_order_id:
            raise RuntimeError("预处理卖单返回缺少 orderId")
        self.c.wait_order_filled(self.spot_symbol, sell_order_id, stop_event=stop_event)
        logger.info(
            "%s模式检测到后置币种余额不足，已先将前置币种按卖一价格卖出，下一轮继续",
            mode_name,
        )
        return True

    @staticmethod
    def _order_price_decimal(order_data: dict, fallback_price: Decimal) -> Decimal:
        try:
            price = Decimal(str(order_data.get("price", "")))
            if price > 0:
                return price
        except Exception:
            pass
        return Decimal(str(fallback_price))

    def _should_reprice_open_order(self, side: str, order_price: Decimal, book_ticker: dict[str, Decimal]) -> tuple[bool, Decimal]:
        side_u = str(side or "").strip().upper()
        price = Decimal(str(order_price))
        threshold_ratio = self._reprice_threshold_ratio()
        if side_u == "BUY":
            current_ref = Decimal(str(book_ticker["bidPrice"]))
            trigger_price = price * (Decimal("1") + threshold_ratio)
            return current_ref > trigger_price, current_ref
        current_ref = Decimal(str(book_ticker["askPrice"]))
        trigger_price = price * (Decimal("1") - threshold_ratio)
        return current_ref < trigger_price, current_ref

    def _wait_order_filled_or_reprice(
        self,
        order_id: int | str,
        side: str,
        order_price: Decimal,
        stop_event,
        mode_name: str,
        poll_interval: float = 1.0,
    ) -> tuple[str, dict | None]:
        symbol_u = self.spot_symbol.upper()
        side_u = str(side or "").strip().upper()

        while True:
            if stop_event and stop_event.is_set():
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception as cancel_exc:
                    logger.warning("停止时撤销订单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            order = self.c.get_order(symbol_u, order_id)
            status = str(order.get("status") or "").upper()
            if status == "FILLED":
                return "filled", order
            if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                raise RuntimeError(f"订单未成交，状态={status}")

            if status == "NEW":
                book_ticker = self.c.get_book_ticker(symbol_u)
                should_reprice, current_ref = self._should_reprice_open_order(side_u, order_price, book_ticker)
                if should_reprice:
                    try:
                        self.c.cancel_order(symbol_u, order_id)
                        logger.info(
                            "%s模式%s单价格偏离超过 %s%%，已撤单重挂：旧价=%s，当前参考价=%s",
                            mode_name,
                            "买" if side_u == "BUY" else "卖",
                            BinanceClient._format_decimal(self.reprice_threshold_percent),
                            BinanceClient._format_decimal(order_price),
                            BinanceClient._format_decimal(current_ref),
                        )
                        return "reprice", book_ticker
                    except Exception as cancel_exc:
                        logger.warning("撤单重挂时撤单失败 %s #%s: %s", symbol_u, order_id, cancel_exc)
                        latest_order = self.c.get_order(symbol_u, order_id)
                        latest_status = str(latest_order.get("status") or "").upper()
                        if latest_status == "FILLED":
                            return "filled", latest_order
                        raise

            if stop_event and stop_event.wait(max(0.2, float(poll_interval))):
                try:
                    self.c.cancel_order(symbol_u, order_id)
                    logger.info("停止时已尝试撤销未完成订单 %s #%s", symbol_u, order_id)
                except Exception:
                    pass
                raise RuntimeError("收到停止信号，已停止等待挂单成交")
            if not stop_event:
                time.sleep(max(0.2, float(poll_interval)))

    def _place_buy_order_with_reprice(self, stop_event, mode_name: str):
        while True:
            book_ticker = self.c.get_book_ticker(self.spot_symbol)
            buy_price = Decimal(str(book_ticker["bidPrice"]))
            ask_price = Decimal(str(book_ticker["askPrice"]))
            buy_order = self.c.spot_limit_buy_all_usdt(symbol=self.spot_symbol, price=buy_price)
            if not buy_order:
                return None, ask_price

            buy_order_id = buy_order.get("orderId")
            if not buy_order_id:
                raise RuntimeError("买单返回缺少 orderId")
            order_price = self._order_price_decimal(buy_order, buy_price)
            action, payload = self._wait_order_filled_or_reprice(
                buy_order_id,
                "BUY",
                order_price,
                stop_event,
                mode_name,
            )
            if action == "filled":
                return payload, ask_price

    def _place_sell_order_with_reprice(self, stop_event, mode_name: str, buy_fill_price: Decimal):
        premium_reprice_by_market = False
        while True:
            book_ticker = self.c.get_book_ticker(self.spot_symbol)
            ask_price = Decimal(str(book_ticker["askPrice"]))
            if mode_name == TRADE_MODE_PREMIUM:
                reference_price = ask_price if premium_reprice_by_market else buy_fill_price
                sell_price = self._premium_sell_price(reference_price)
            else:
                sell_price = ask_price

            sell_order = self.c.spot_limit_sell_all_base(symbol=self.spot_symbol, price=sell_price)
            if not sell_order:
                return None

            sell_order_id = sell_order.get("orderId")
            if not sell_order_id:
                raise RuntimeError("卖单返回缺少 orderId")
            order_price = self._order_price_decimal(sell_order, sell_price)
            action, payload = self._wait_order_filled_or_reprice(
                sell_order_id,
                "SELL",
                order_price,
                stop_event,
                mode_name,
            )
            if action == "filled":
                return payload
            premium_reprice_by_market = True

    def _run_market_mode(self, stop_event, progress_cb=None):
        total_steps = self.spot_rounds if self.spot_rounds > 0 else 1
        step = 0

        self.ensure_base_sold()

        for i in range(self.spot_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（现货阶段）")
                return

            self.ensure_base_sold()
            logger.info("--- 现货轮 %d/%d 开始 ---", i + 1, self.spot_rounds)

            try:
                buy_success = self.c.spot_buy_all_usdt(symbol=self.spot_symbol)
                if buy_success:
                    self.sleep_fn()
                    self.c.spot_sell_all_base(symbol=self.spot_symbol, precision=self.spot_precision)
                else:
                    logger.info("买入未执行（可能余额不足），跳过本轮卖出")

                logger.info("--- 现货轮 %d 完成 ---", i + 1)

            except Exception as e:
                logger.error(f"现货轮 %d 执行异常: {e}", i + 1)
                time.sleep(3)

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "现货轮 %d/%d" % (i + 1, self.spot_rounds))
            self.sleep_fn()

        self.ensure_base_sold()

    def _run_limit_like_mode(self, stop_event, progress_cb=None):
        step = 0
        mode_name = self._mode_name()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                return
            if self._should_stop_for_bnb_fee():
                return

            step += 1
            logger.info("--- %s轮 %d 开始 ---", mode_name, step)
            try:
                buy_result, ask_price = self._place_buy_order_with_reprice(stop_event, mode_name)
                buy_order = buy_result
                if not buy_order:
                    if self._prepare_quote_by_selling_base(stop_event, ask_price, mode_name):
                        self.sleep_fn()
                        continue
                    logger.info("%s模式买入未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                buy_fill_price = self.c.get_order_average_price(buy_order) or Decimal("0")
                if buy_fill_price <= 0:
                    buy_fill_price = Decimal(str(self.c.get_book_ticker(self.spot_symbol)["bidPrice"]))

                self.sleep_fn()

                sell_order = self._place_sell_order_with_reprice(stop_event, mode_name, buy_fill_price)
                if not sell_order:
                    logger.info("%s模式卖出未执行（可能余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                logger.info("--- %s轮 %d 完成 ---", mode_name, step)
            except Exception as e:
                if stop_event and stop_event.is_set():
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)
                time.sleep(3)

            if progress_cb:
                progress_cb(step, max(step, 1), f"{mode_name}轮 {step}")
            self.sleep_fn()

    def run(self, stop_event, progress_cb=None):
        withdraw_amount = 0.0
        withdraw_error = ""
        withdraw_attempted = False

        self._run_bnb_topup_if_needed()

        if self._mode_name() == TRADE_MODE_MARKET:
            self._run_market_mode(stop_event, progress_cb=progress_cb)
        else:
            self._run_limit_like_mode(stop_event, progress_cb=progress_cb)

        if stop_event and stop_event.is_set():
            logger.info("检测到停止信号，跳过最终提现")
            return {
                "withdraw_amount": withdraw_amount,
                "withdraw_error": withdraw_error,
                "withdraw_attempted": withdraw_attempted,
            }

        logger.info(f"开始最终提现所有 {self.withdraw_coin}")
        withdraw_attempted = True
        try:
            withdraw_amount = self.c.withdraw_all_coin(
                coin=self.withdraw_coin,
                address=self.withdraw_address,
                network=self.withdraw_network,
                fee_buffer=self.withdraw_fee_buffer,
                enable_withdraw=self.enable_withdraw,
            )
            if self.withdraw_callback:
                self.withdraw_callback(withdraw_amount)
        except Exception as e:
            withdraw_error = str(e)
            logger.error(f"提现阶段异常: {e}")

        logger.info("策略执行完毕")
        return {
            "withdraw_amount": withdraw_amount,
            "withdraw_error": withdraw_error,
            "withdraw_attempted": withdraw_attempted,
        }


# ====================== GUI 应用 ======================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Binance 自动交易机器人（增强版 GUI）")
        self.geometry("1320x920")

        self.client = None
        self.worker_thread = None
        self.stop_event = None
        self.exchange_proxy_runtime = ExchangeProxyRuntime(STRATEGY_CONFIG_FILE.parent)

        self.accounts = []
        self.total_asset_results = {}

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build_ui()
        self._load_strategy_config()
        self.after(100, self._poll_log_queue)
        self.update_ip()

    def _build_ui(self):
        self.main_tabs = ttk.Notebook(self)
        self.main_tabs.pack(fill="both", expand=True, padx=8, pady=8)

        self.exchange_tab = ttk.Frame(self.main_tabs)
        self.onchain_tab = ttk.Frame(self.main_tabs)
        self.main_tabs.add(self.exchange_tab, text="交易所批量")
        self.main_tabs.add(self.onchain_tab, text="链上")

        frame_ip = ttk.Frame(self.exchange_tab)
        frame_ip.pack(fill="x", padx=10, pady=3)
        ttk.Label(frame_ip, text="本机直连 IP：").pack(side="left")
        self.ip_var = tk.StringVar(value="获取中...")
        ttk.Label(frame_ip, textvariable=self.ip_var).pack(side="left")
        ttk.Label(frame_ip, text="   交易所代理：").pack(side="left", padx=(16, 0))
        self.exchange_proxy_status_var = tk.StringVar(value="未启用")
        ttk.Label(frame_ip, textvariable=self.exchange_proxy_status_var).pack(side="left")
        ttk.Label(frame_ip, text="   交易所出口 IP：").pack(side="left", padx=(16, 0))
        self.exchange_proxy_exit_ip_var = tk.StringVar(value="--")
        ttk.Label(frame_ip, textvariable=self.exchange_proxy_exit_ip_var).pack(side="left")

        frame_top = ttk.LabelFrame(self.exchange_tab, text="策略配置（单账号 & 批量共享）")
        frame_top.pack(fill="x", padx=10, pady=5)

        self.api_key_var = tk.StringVar(value=API_KEY_DEFAULT)
        self.api_secret_var = tk.StringVar(value=API_SECRET_DEFAULT)
        self.exchange_proxy_var = tk.StringVar(value=EXCHANGE_PROXY_DEFAULT)
        self.spot_rounds_var = tk.IntVar(value=SPOT_ROUNDS_DEFAULT)
        self.trade_mode_var = tk.StringVar(value=TRADE_MODE_DEFAULT)
        self.premium_percent_var = tk.StringVar(value=PREMIUM_PERCENT_DEFAULT)
        self.bnb_fee_stop_var = tk.StringVar(value=BNB_FEE_STOP_DEFAULT)
        self.bnb_topup_amount_var = tk.StringVar(value=BNB_TOPUP_AMOUNT_DEFAULT)
        self.reprice_threshold_percent_var = tk.StringVar(value=REPRICE_THRESHOLD_PERCENT_DEFAULT)
        self.spot_symbol_var = tk.StringVar(value=SPOT_SYMBOL_DEFAULT)
        self.spot_precision_var = tk.IntVar(value=SPOT_PRECISION_DEFAULT)

        self.withdraw_addr_var = tk.StringVar(value=WITHDRAW_ADDRESS_DEFAULT)
        self.withdraw_net_var = tk.StringVar(value=WITHDRAW_NETWORK_DEFAULT)
        self.withdraw_coin_var = tk.StringVar(value=WITHDRAW_COIN_DEFAULT)
        self.withdraw_buffer_var = tk.DoubleVar(value=WITHDRAW_FEE_BUFFER_DEFAULT)
        self.enable_withdraw_var = tk.BooleanVar(value=True)

        self.min_delay_var = tk.StringVar(value="")
        self.max_delay_var = tk.StringVar(value="")
        self.usdt_timeout_var = tk.IntVar(value=30)

        frame_mid = ttk.LabelFrame(self.exchange_tab, text="状态控制")
        frame_mid.pack(fill="x", padx=10, pady=5)
        self.status_var = tk.StringVar(value="状态：空闲")
        self.single_account_balances_var = tk.StringVar(value="--")
        self.exchange_strategy_frame = frame_top
        self._rebuild_exchange_panels(frame_top, frame_mid)

        frame_acc = ttk.LabelFrame(self.exchange_tab, text="账号列表管理（批量 API + 提现地址）")
        frame_acc.pack(fill="both", expand=True, padx=10, pady=5)

        self.acc_api_key_var = tk.StringVar()
        self.acc_api_secret_var = tk.StringVar()
        self.acc_withdraw_addr_var = tk.StringVar()
        self.acc_network_var = self.withdraw_net_var

        self.withdraw_net_var.trace_add("write", self._on_global_network_changed)
        self.max_threads_var = tk.IntVar(value=MAX_THREADS_DEFAULT)

        header = ttk.Frame(frame_acc)
        header.pack(fill="x", padx=5)
        ttk.Label(header, text="No.", width=4).pack(side="left", padx=2)
        ttk.Label(header, text="选", width=4).pack(side="left", padx=2)
        ttk.Label(header, text="API KEY", width=25).pack(side="left", padx=2)
        ttk.Label(header, text="提现地址", width=35).pack(side="left", padx=2)
        ttk.Label(header, text="网络", width=8).pack(side="left", padx=2)
        ttk.Label(header, text="状态", width=30).pack(side="left", padx=2)

        self.frame_list_canvas = ttk.Frame(frame_acc)
        self.frame_list_canvas.pack(fill="both", expand=True, padx=5, pady=2)

        self.canvas = tk.Canvas(self.frame_list_canvas, height=190, bg="#f0f0f0")
        self.canvas.configure(takefocus=1, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.frame_list_canvas, orient="vertical", command=self.canvas.yview)

        self.accounts_container = ttk.Frame(self.canvas)

        self.accounts_container.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.accounts_container, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        self.account_list_hint = ttk.Label(
            self.frame_list_canvas,
            text="账号列表为空。点击此区域后可直接 Ctrl+V / Cmd+V 粘贴导入账号。\n导入格式：每 3 段一组，依次为 API KEY / SECRET / 提现地址。",
            foreground="#666",
            justify="center",
            anchor="center",
        )
        self.canvas.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.accounts_container.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.frame_list_canvas.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.account_list_hint.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self._refresh_account_list_hint()
        self.account_row_menu = tk.Menu(self, tearoff=0)
        self.account_row_menu.add_command(label="查询", command=self.run_context_account_query)
        self.account_row_menu.add_command(label="执行", command=self.run_context_account_execute)
        self.account_row_menu.add_command(label="提现", command=self.run_context_account_withdraw)
        self.account_row_menu.add_command(label="归集BNB", command=self.run_context_account_collect_bnb)
        self._context_account = None
        self._setup_account_list_mousewheel_bindings()

        frame_batch_ctrl = ttk.Frame(frame_acc)
        frame_batch_ctrl.pack(fill="x", padx=5, pady=5)

        self.btn_toggle_select_accounts = ttk.Button(frame_batch_ctrl, text="全选", width=8, command=self.toggle_select_all_accounts)
        self.btn_toggle_select_accounts.pack(side="left", padx=(0, 5))
        self.btn_run_accounts = ttk.Button(frame_batch_ctrl, text="批量执行", command=self.run_selected_accounts)
        self.btn_run_accounts.pack(side="left", padx=5)
        self.btn_query_all_assets = ttk.Button(frame_batch_ctrl, text="查询全部总资产", command=self.run_query_total_assets_for_all_accounts)
        self.btn_query_all_assets.pack(side="left", padx=5)
        self.btn_batch_withdraw = ttk.Button(frame_batch_ctrl, text="批量提现", command=self.batch_manual_withdraw)
        self.btn_batch_withdraw.pack(side="left", padx=5)
        self.btn_collect_bnb_combo = ttk.Button(frame_batch_ctrl, text="归集并买BNB", command=self.run_batch_collect_bnb_with_confirm)
        self.btn_collect_bnb_combo.pack(side="left", padx=5)

        self.btn_del_accounts = ttk.Button(frame_batch_ctrl, text="删除选中", command=self.delete_selected_accounts)
        self.btn_del_accounts.pack(side="left", padx=5)

        self.btn_paste_accounts = ttk.Button(frame_batch_ctrl, text="粘贴导入", command=self.import_accounts_from_clipboard)
        self.btn_paste_accounts.pack(side="left", padx=5)
        ttk.Label(frame_batch_ctrl, text="绾跨▼鏁?").pack(side="left", padx=(10, 2))
        ttk.Spinbox(frame_batch_ctrl, from_=1, to=50, textvariable=self.max_threads_var, width=3).pack(side="left", padx=2)
        try:
            frame_batch_ctrl.winfo_children()[-2].configure(text="\u7ebf\u7a0b\u6570:")
        except Exception:
            pass

        self.skip_usdt_wait_in_batch_var = tk.BooleanVar(value=False)

        frame_batch_opts = ttk.Frame(frame_acc)
        frame_batch_opts.pack(fill="x", padx=5, pady=(0, 5))

        self.btn_export_accounts = ttk.Button(frame_batch_opts, text="导出", command=self.export_accounts)
        self.btn_export_accounts.pack(side="left", padx=(0, 5))

        self.btn_import_accounts = ttk.Button(frame_batch_opts, text="导入", command=self.import_accounts)
        self.btn_import_accounts.pack(side="left", padx=5)

        self.btn_export_asset_csv = ttk.Button(frame_batch_opts, text="导出总资产CSV", command=self.export_total_asset_csv)
        self.btn_export_asset_csv.pack(side="left", padx=5)

        ttk.Checkbutton(
            frame_batch_opts,
            text="批量策略跳过USDT检测",
            variable=self.skip_usdt_wait_in_batch_var
        ).pack(side="left", padx=(12, 0))

        frame_log = ttk.LabelFrame(self.exchange_tab, text="运行日志")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.text_log = tk.Text(frame_log, wrap="word", height=10, state="disabled")
        self.text_log.pack(fill="both", expand=True, side="left")

        scrollbar_log = ttk.Scrollbar(frame_log, command=self.text_log.yview)
        scrollbar_log.pack(side="right", fill="y")
        self.text_log["yscrollcommand"] = scrollbar_log.set

        onchain_shell = ttk.Frame(self.onchain_tab)
        onchain_shell.pack(fill="both", expand=True)
        onchain_intro = ttk.LabelFrame(onchain_shell, text="链上模块")
        onchain_intro.pack(fill="x", padx=10, pady=(8, 6))
        ttk.Label(onchain_intro, text="该页面为独立链上批量转账模块，与交易所批量页面配置互不影响。", foreground="#666").pack(
            anchor="w", padx=8, pady=(6, 6)
        )

        onchain_intro.pack_forget()
        onchain_body = ttk.Frame(onchain_shell)
        onchain_body.pack(fill="both", expand=True, padx=2, pady=2)

        if OnchainTransferPage is not None:
            try:
                self.onchain_page = OnchainTransferPage(onchain_body)
            except Exception as exc:
                self.onchain_page = None
                logger.exception("链上页面初始化失败: %s", exc)
                fail_box = ttk.LabelFrame(onchain_body, text="链上模块加载失败")
                fail_box.pack(fill="both", expand=True, padx=12, pady=12)
                ttk.Label(fail_box, text=f"链上页面加载失败：{exc}").pack(anchor="w", padx=8, pady=(8, 4))
                ttk.Label(fail_box, text="请检查运行依赖：eth-account、eth-utils").pack(anchor="w", padx=8, pady=(0, 8))
        else:
            self.onchain_page = None
            fail_box = ttk.LabelFrame(onchain_body, text="链上模块加载失败")
            fail_box.pack(fill="both", expand=True, padx=12, pady=12)
            ttk.Label(fail_box, text=f"链上页面导入失败：{_ONCHAIN_IMPORT_ERROR}").pack(anchor="w", padx=8, pady=(8, 4))
            ttk.Label(fail_box, text="请检查运行依赖：eth-account、eth-utils").pack(anchor="w", padx=8, pady=(0, 8))

        # 快捷键：Ctrl+V / Cmd+V 直接触发“从剪贴板导入账号”
        self.bind_all("<Control-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Control-V>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-V>", self._on_paste_shortcut, add="+")

    @staticmethod
    def _clear_container_children(container):
        for child in list(container.winfo_children()):
            try:
                child.destroy()
            except Exception:
                pass

    def _rebuild_exchange_panels(self, frame_top, frame_mid):
        self._rebuild_exchange_strategy_panel(frame_top)
        self._rebuild_exchange_single_panel(frame_mid)

    def _rebuild_exchange_single_panel(self, frame_mid):
        self._clear_container_children(frame_mid)
        try:
            frame_mid.configure(text="状态控制")
        except Exception:
            pass

        frame_mid.columnconfigure(0, weight=0)
        frame_mid.columnconfigure(1, weight=0)

        left = ttk.Frame(frame_mid)
        left.grid(row=0, column=0, sticky="nw", padx=(0, 0), pady=4)
        left.columnconfigure(0, weight=0)
        left.columnconfigure(1, weight=0)

        right = ttk.Frame(frame_mid)
        right.grid(row=0, column=1, sticky="nw", padx=(16, 0), pady=4)
        self._single_panel_right = right
        right.columnconfigure(1, weight=1)
        right.columnconfigure(2, weight=0)

        self.btn_start = ttk.Button(right, text="开始运行（当前 API）", command=self.start_bot)
        self.btn_stop = ttk.Button(left, text="停止运行", command=self.stop_bot, state="disabled")
        self.btn_refresh = ttk.Button(right, text="刷新余额（当前 API）", command=self.refresh_balances)
        self.btn_withdraw = ttk.Button(right, text="手动提现", command=self.manual_withdraw)

        self.btn_stop.grid(row=0, column=0, padx=5, pady=5, sticky="w")

        self.progress = ttk.Progressbar(left, orient="horizontal", mode="determinate")
        self.progress.grid(row=1, column=0, columnspan=2, sticky="w", padx=5, pady=5)
        ttk.Label(left, textvariable=self.status_var).grid(row=2, column=0, columnspan=2, sticky="w", padx=5, pady=2)

        ttk.Label(right, text="API KEY:").grid(row=0, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.api_key_var, width=36).grid(row=0, column=1, sticky="ew", padx=5, pady=3)
        self.btn_start.grid(row=0, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(right, text="API SECRET:").grid(row=1, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.api_secret_var, width=36, show="*").grid(row=1, column=1, sticky="ew", padx=5, pady=3)
        self.btn_refresh.grid(row=1, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(right, text="提现地址:").grid(row=2, column=0, sticky="e", padx=5, pady=3)
        ttk.Entry(right, textvariable=self.withdraw_addr_var, width=36).grid(row=2, column=1, sticky="ew", padx=5, pady=3)
        self.btn_withdraw.grid(row=2, column=2, padx=(5, 0), pady=3, sticky="w")
        ttk.Label(
            right,
            textvariable=self.single_account_balances_var,
            justify="left",
            anchor="w",
            wraplength=760,
        ).grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=(4, 2))

        self.after_idle(self._align_single_status_panel)

    @staticmethod
    def _normalized_entry_text(value, *placeholders):
        text = str(value or "").strip()
        return "" if text in set(placeholders) else text

    def _delay_var_value(self, variable, default: int, placeholder: str) -> int:
        text = self._normalized_entry_text(variable.get(), placeholder)
        if not text:
            return int(default)
        return int(text)

    @staticmethod
    def _normalize_trade_mode(value) -> str:
        text = str(value or "").strip()
        return text if text in TRADE_MODE_OPTIONS else TRADE_MODE_DEFAULT

    def _refresh_strategy_panel_layout(self):
        frame_top = getattr(self, "exchange_strategy_frame", None)
        if frame_top is not None:
            self._rebuild_exchange_strategy_panel(frame_top)

    def _align_trade_mode_sections(self):
        row2 = getattr(self, "_strategy_row2", None)
        row3 = getattr(self, "_strategy_row3", None)
        row2_left = getattr(self, "_strategy_row2_left", None)
        row3_left = getattr(self, "_strategy_row3_left", None)
        btn = getattr(self, "btn_save_strategy_config", None)
        if row2 is None or row3 is None or row2_left is None or row3_left is None or btn is None:
            return
        try:
            btn.update_idletasks()
            row2.update_idletasks()
            row3.update_idletasks()
            row2_target = max(
                row2_left.winfo_reqwidth(),
                (btn.winfo_rootx() + btn.winfo_width()) - row2.winfo_rootx() + 12,
            )
            row3_target = max(
                row3_left.winfo_reqwidth(),
                (btn.winfo_rootx() + btn.winfo_width()) - row3.winfo_rootx() + 12,
            )
            row2.grid_columnconfigure(0, minsize=int(row2_target), weight=0)
            row3.grid_columnconfigure(0, minsize=int(row3_target), weight=0)
        except Exception:
            pass

    def _align_single_status_panel(self):
        stop_btn = getattr(self, "btn_stop", None)
        progress = getattr(self, "progress", None)
        right_frame = getattr(self, "_single_panel_right", None)
        if stop_btn is None or progress is None or right_frame is None:
            return
        try:
            stop_btn.update_idletasks()
            btn_width = max(1, stop_btn.winfo_width())
            progress.configure(length=btn_width * 3)
            right_frame.grid_configure(padx=(btn_width, 0))
        except Exception:
            pass

    def _on_trade_mode_changed(self, _event=None):
        self._refresh_strategy_panel_layout()

    @staticmethod
    def _decimal_field_value(raw_value, field_label: str, *, min_value: Decimal | str | int | float = Decimal("0")) -> Decimal:
        text = str(raw_value or "").strip()
        if not text:
            raise RuntimeError(f"{field_label}不能为空")
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"{field_label}格式不正确") from exc
        if value < Decimal(str(min_value)):
            raise RuntimeError(f"{field_label}不能小于 {min_value}")
        return value

    def _collect_trade_mode_settings(self) -> dict[str, object]:
        mode = self._normalize_trade_mode(self.trade_mode_var.get())
        try:
            stored_rounds = int(self.spot_rounds_var.get())
        except Exception:
            stored_rounds = SPOT_ROUNDS_DEFAULT

        premium_text = str(self.premium_percent_var.get() or "").strip()
        fee_stop_text = str(self.bnb_fee_stop_var.get() or "").strip()
        bnb_topup_text = str(self.bnb_topup_amount_var.get() or "").strip()
        reprice_threshold_text = str(self.reprice_threshold_percent_var.get() or "").strip()
        premium_value: Decimal | None = None
        fee_stop_value: Decimal | None = None
        if not bnb_topup_text:
            bnb_topup_text = "0"
        if not reprice_threshold_text:
            reprice_threshold_text = REPRICE_THRESHOLD_PERCENT_DEFAULT
        bnb_topup_value = self._decimal_field_value(bnb_topup_text, "预买BNB金额", min_value=0)
        reprice_threshold_value = self._decimal_field_value(reprice_threshold_text, "重新挂单阈值百分比", min_value=0)

        if mode == TRADE_MODE_MARKET:
            try:
                runtime_rounds = int(self.spot_rounds_var.get())
            except Exception as exc:
                raise RuntimeError("市价模式下必须填写现货轮次") from exc
            if runtime_rounds < 1:
                raise RuntimeError("市价模式下现货轮次必须大于等于 1")
        else:
            runtime_rounds = stored_rounds if stored_rounds > 0 else SPOT_ROUNDS_DEFAULT
            fee_stop_value = self._decimal_field_value(fee_stop_text, "剩余 BNB 手续费停止值", min_value=0)
            if mode == TRADE_MODE_PREMIUM:
                premium_value = self._decimal_field_value(premium_text, "溢价百分比", min_value=0)

        return {
            "trade_mode": mode,
            "spot_rounds": runtime_rounds,
            "stored_spot_rounds": stored_rounds,
            "premium_percent": premium_text,
            "premium_percent_value": premium_value,
            "bnb_fee_stop": fee_stop_text,
            "bnb_fee_stop_value": fee_stop_value,
            "bnb_topup_amount": bnb_topup_text,
            "bnb_topup_amount_value": bnb_topup_value,
            "reprice_threshold_percent": reprice_threshold_text,
            "reprice_threshold_percent_value": reprice_threshold_value,
        }

    def _install_entry_placeholder(self, entry, variable, placeholder: str):
        placeholder_color = "#8a8a8a"
        normal_color = entry.cget("fg")

        def set_normal():
            try:
                entry.configure(fg=normal_color)
            except Exception:
                pass

        def set_placeholder():
            if self.focus_get() is entry:
                return
            text = str(variable.get() or "").strip()
            if text and text != placeholder:
                set_normal()
                return
            variable.set(placeholder)
            try:
                entry.configure(fg=placeholder_color)
            except Exception:
                pass

        def clear_placeholder(_event=None):
            if str(variable.get() or "") == placeholder:
                variable.set("")
            set_normal()

        def on_focus_out(_event=None):
            if not str(variable.get() or "").strip():
                set_placeholder()
            else:
                set_normal()

        def on_var_change(*_args):
            text = str(variable.get() or "")
            if text == placeholder:
                try:
                    entry.configure(fg=placeholder_color)
                except Exception:
                    pass
            elif text.strip():
                set_normal()
            elif self.focus_get() is not entry:
                self.after_idle(set_placeholder)

        variable.trace_add("write", on_var_change)
        entry.bind("<FocusIn>", clear_placeholder, add="+")
        entry.bind("<FocusOut>", on_focus_out, add="+")
        if str(variable.get() or "").strip():
            on_var_change()
        else:
            set_placeholder()

    def _rebuild_exchange_strategy_panel(self, frame_top):
        self.exchange_strategy_frame = frame_top
        self._clear_container_children(frame_top)
        try:
            frame_top.configure(text="\u5171\u4eab\u7b56\u7565\u914d\u7f6e")
        except Exception:
            pass
        try:
            ttk.Style(self).configure("ExchangeAccent.TLabel", foreground="#7A3FF2")
        except Exception:
            pass

        row1 = ttk.Frame(frame_top)
        row1.pack(fill="x", padx=5, pady=(2, 3))

        ttk.Label(row1, text="\u4ee3\u7406:").grid(row=0, column=0, sticky="w")
        proxy_wrap = ttk.Frame(row1)
        proxy_wrap.grid(row=0, column=1, sticky="w", padx=(4, 8))
        ttk.Entry(proxy_wrap, textvariable=self.exchange_proxy_var, width=24).grid(row=0, column=0, sticky="w")
        self.btn_test_exchange_proxy = ttk.Button(proxy_wrap, text="\u4ee3\u7406\u6d4b\u8bd5", command=self.test_exchange_proxy)
        self.btn_test_exchange_proxy.grid(row=0, column=1, padx=(6, 0))

        ttk.Label(row1, text="\u968f\u673a\u5ef6\u8fdf(\u6beb\u79d2):").grid(row=0, column=2, sticky="e")
        delay_wrap = ttk.Frame(row1)
        delay_wrap.grid(row=0, column=3, sticky="w", padx=(4, 8))
        self.min_delay_entry = tk.Entry(delay_wrap, textvariable=self.min_delay_var, width=8)
        self.min_delay_entry.pack(side="left")
        self.max_delay_entry = tk.Entry(delay_wrap, textvariable=self.max_delay_var, width=8)
        self.max_delay_entry.pack(side="left", padx=(6, 0))
        self._install_entry_placeholder(self.min_delay_entry, self.min_delay_var, "\u6700\u5c0f")
        self._install_entry_placeholder(self.max_delay_entry, self.max_delay_var, "\u6700\u5927")

        self.btn_save_strategy_config = ttk.Button(row1, text="\u4fdd\u5b58\u914d\u7f6e", command=self.save_strategy_config)
        self.btn_save_strategy_config.grid(row=0, column=4, sticky="w")

        row2 = ttk.Frame(frame_top)
        row2.pack(fill="x", padx=5, pady=3)
        self._strategy_row2 = row2
        current_trade_mode = self._normalize_trade_mode(self.trade_mode_var.get())
        row2_left = ttk.Frame(row2)
        row2_left.grid(row=0, column=0, sticky="w")
        self._strategy_row2_left = row2_left
        row2_right = ttk.Frame(row2)
        row2_right.grid(row=0, column=1, sticky="w")
        row2.grid_columnconfigure(2, weight=1)

        ttk.Label(row2_left, text="\u73b0\u8d27\u4ea4\u6613\u5bf9:", style="ExchangeAccent.TLabel").pack(side="left")
        ttk.Entry(row2_left, textvariable=self.spot_symbol_var, width=14).pack(side="left", padx=(4, 12))
        ttk.Label(row2_left, text="\u73b0\u8d27\u6570\u91cf\u7cbe\u5ea6:").pack(side="left")
        ttk.Entry(row2_left, textvariable=self.spot_precision_var, width=6).pack(side="left", padx=(4, 12))
        ttk.Label(row2_left, text="\u624b\u7eed\u8d39\u9884\u7559:").pack(side="left")
        ttk.Entry(row2_left, textvariable=self.withdraw_buffer_var, width=8).pack(side="left", padx=(4, 12))

        ttk.Label(row2_right, text="交易模式:").pack(side="left")
        self.trade_mode_combo = ttk.Combobox(
            row2_right,
            textvariable=self.trade_mode_var,
            values=TRADE_MODE_OPTIONS,
            width=8,
            state="readonly",
        )
        self.trade_mode_combo.pack(side="left", padx=(4, 12))
        self.trade_mode_combo.bind("<<ComboboxSelected>>", self._on_trade_mode_changed, add="+")
        ttk.Label(row2_right, text="预买BNB金额:").pack(side="left")
        ttk.Entry(row2_right, textvariable=self.bnb_topup_amount_var, width=10).pack(side="left", padx=(4, 12))

        row3 = ttk.Frame(frame_top)
        row3.pack(fill="x", padx=5, pady=(3, 2))
        self._strategy_row3 = row3
        row3_left = ttk.Frame(row3)
        row3_left.grid(row=0, column=0, sticky="w")
        self._strategy_row3_left = row3_left
        row3_right = ttk.Frame(row3)
        row3_right.grid(row=0, column=1, sticky="w")
        row3.grid_columnconfigure(2, weight=1)

        ttk.Label(row3_left, text="\u63d0\u73b0\u5e01\u79cd:").pack(side="left")
        self.withdraw_coin_combo = ttk.Combobox(
            row3_left,
            textvariable=self.withdraw_coin_var,
            values=WITHDRAW_COIN_OPTIONS,
            width=8,
            state="readonly",
        )
        self.withdraw_coin_combo.pack(side="left", padx=(4, 12))
        ttk.Label(row3_left, text="\u7f51\u7edc:").pack(side="left")
        self.withdraw_net_combo = ttk.Combobox(
            row3_left,
            textvariable=self.withdraw_net_var,
            values=WITHDRAW_NETWORK_OPTIONS,
            width=10,
            state="readonly",
        )
        self.withdraw_net_combo.pack(side="left", padx=(4, 12))
        ttk.Label(row3_left, text="USDT \u5230\u8d26\u8d85\u65f6(\u79d2):").pack(side="left")
        ttk.Entry(row3_left, textvariable=self.usdt_timeout_var, width=8).pack(side="left", padx=(4, 12))
        ttk.Checkbutton(row3_left, text="\u81ea\u52a8\u63d0\u73b0", variable=self.enable_withdraw_var).pack(side="left")

        if current_trade_mode == TRADE_MODE_MARKET:
            ttk.Label(row3_right, text="现货轮次:").pack(side="left")
            ttk.Spinbox(row3_right, from_=1, to=100, textvariable=self.spot_rounds_var, width=6).pack(side="left", padx=(4, 12))
        elif current_trade_mode == TRADE_MODE_LIMIT:
            ttk.Label(row3_right, text="剩余 BNB 手续费停止值:").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.bnb_fee_stop_var, width=10).pack(side="left", padx=(4, 12))
            ttk.Label(row3_right, text="重新挂单阈值(%):").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.reprice_threshold_percent_var, width=8).pack(side="left", padx=(4, 12))
        else:
            ttk.Label(row3_right, text="溢价百分比(%):").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.premium_percent_var, width=10).pack(side="left", padx=(4, 12))
            ttk.Label(row3_right, text="剩余 BNB 手续费停止值:").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.bnb_fee_stop_var, width=10).pack(side="left", padx=(4, 12))
            ttk.Label(row3_right, text="重新挂单阈值(%):").pack(side="left")
            ttk.Entry(row3_right, textvariable=self.reprice_threshold_percent_var, width=8).pack(side="left", padx=(4, 12))

        self.after_idle(self._align_trade_mode_sections)

    def _on_close(self):
        try:
            self.exchange_proxy_runtime.stop()
        except Exception:
            pass
        self.destroy()

    def _setup_account_list_mousewheel_bindings(self):
        # 账号列表区域内，鼠标滚轮可直接滚动，无需命中右侧滚动条
        self.bind_all("<MouseWheel>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-4>", self._on_account_list_mousewheel, add="+")
        self.bind_all("<Button-5>", self._on_account_list_mousewheel, add="+")

    def _pointer_in_account_list(self):
        frame = getattr(self, "frame_list_canvas", None)
        if frame is None or not frame.winfo_exists():
            return False
        try:
            x = self.winfo_pointerx()
            y = self.winfo_pointery()
            left = frame.winfo_rootx()
            top = frame.winfo_rooty()
            right = left + frame.winfo_width()
            bottom = top + frame.winfo_height()
            return left <= x <= right and top <= y <= bottom
        except Exception:
            return False

    def _on_account_list_mousewheel(self, event=None):
        if not self._pointer_in_account_list():
            return None
        if not hasattr(self, "canvas"):
            return None

        units = 0
        num = getattr(event, "num", None)
        if num == 4:
            units = -1
        elif num == 5:
            units = 1
        else:
            delta = int(getattr(event, "delta", 0) or 0)
            if delta != 0:
                if sys.platform == "darwin":
                    units = -1 if delta > 0 else 1
                else:
                    units = -int(delta / 120) if abs(delta) >= 120 else (-1 if delta > 0 else 1)

        if units == 0:
            return None

        try:
            self.canvas.yview_scroll(units, "units")
            return "break"
        except Exception:
            return None

    def _focus_account_list_for_paste(self, _event=None):
        canvas = getattr(self, "canvas", None)
        if canvas is None:
            return None
        try:
            canvas.focus_set()
        except Exception:
            return None
        return None

    def _refresh_account_list_hint(self):
        hint = getattr(self, "account_list_hint", None)
        if hint is None:
            return
        if self.accounts:
            hint.place_forget()
            return
        hint.place(relx=0.5, rely=0.5, anchor="center")

    def random_sleep(self, min_ms, max_ms):
        if max_ms < min_ms:
            min_ms, max_ms = max_ms, min_ms
        delay_ms = random.randint(min_ms, max_ms)
        time.sleep(delay_ms / 1000.0)

    def _current_random_delay_seconds(self) -> float:
        try:
            min_ms = int(self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f"))
            max_ms = int(self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927"))
        except Exception:
            min_ms = 1000
            max_ms = 3000
        if max_ms < min_ms:
            min_ms, max_ms = max_ms, min_ms
        return max(0.0, random.randint(min_ms, max_ms) / 1000.0)

    def _mask_key(self, key, prefix=6, suffix=4):
        if len(key) <= prefix + suffix:
            return key
        return key[:prefix] + "..." + key[-suffix:]

    def _mask_addr(self, addr, prefix=6, suffix=4):
        if len(addr) <= prefix + suffix:
            return addr
        return addr[:prefix] + "..." + addr[-suffix:]

    def _on_paste_shortcut(self, event=None):
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            return None

        parsed, err_msg = self._parse_accounts_from_text(raw)
        if parsed:
            self._import_accounts_from_text(raw, "剪贴板")
            return "break"

        focus = self.focus_get()
        if focus is not None:
            widget_class = str(focus.winfo_class())
            if widget_class in {"Entry", "TEntry", "Text", "TCombobox", "Spinbox", "TSpinbox"}:
                return None

        if err_msg:
            messagebox.showerror("错误", f"剪贴板导入失败：{err_msg}")
            return "break"
        return None

    def _parse_accounts_from_text(self, raw_text):
        if raw_text is None:
            return [], "内容为空"

        normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
        tokens = []

        for raw_line in normalized.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 3:
                    tokens.extend(parts[:3])
                    continue

            # 支持“行尾备注/中文说明”，只提取长 token（API KEY/SECRET/地址）
            found = re.findall(r"0x[a-fA-F0-9]{40}|[A-Za-z0-9]{24,}", line)
            if found:
                tokens.extend(found)

        if not tokens:
            return [], "没有识别到账号数据"

        if len(tokens) % 3 != 0:
            return [], f"识别到 {len(tokens)} 条有效字段，必须按 3 条一组：APIKEY / APISECRET / 提现地址"

        accounts = []
        for i in range(0, len(tokens), 3):
            key, secret, addr = tokens[i], tokens[i + 1], tokens[i + 2]
            accounts.append((key, secret, addr))
        return accounts, ""

    def _import_accounts_from_text(self, raw_text, source_name):
        parsed, err_msg = self._parse_accounts_from_text(raw_text)
        if err_msg:
            messagebox.showerror("错误", f"{source_name}导入失败：{err_msg}")
            return 0

        existing_api_keys = {
            str(acc.get("api_key", "") or "").strip()
            for acc in self.accounts
            if str(acc.get("api_key", "") or "").strip()
        }
        seen_api_keys = set(existing_api_keys)
        deduped_accounts = []
        duplicate_count = 0
        for key, secret, addr in parsed:
            api_key = str(key or "").strip()
            if api_key in seen_api_keys:
                duplicate_count += 1
                continue
            seen_api_keys.add(api_key)
            deduped_accounts.append((key, secret, addr))

        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for key, secret, addr in deduped_accounts:
            self._append_account_row(key, secret, addr, net)

        self._reindex_accounts()
        self._focus_account_list_for_paste()
        logger.info("从%s导入账号数量：%d，重复 API 数量：%d", source_name, len(deduped_accounts), duplicate_count)
        if deduped_accounts:
            msg = f"从{source_name}导入账号数量：{len(deduped_accounts)}"
            if duplicate_count:
                msg += f"\n已跳过重复 API：{duplicate_count}"
            messagebox.showinfo("成功", msg)
        else:
            messagebox.showinfo("提示", f"{source_name}没有新增账号，全部为重复 API（重复 {duplicate_count} 个）")
        return len(deduped_accounts)

    def record_withdraw(self, index, api_key, address, amount):
        line = f"{index}+{api_key}+{address}+{amount:.8f}\n"
        try:
            fname = "withdraw_success.txt"
            with open(fname, "a", encoding="utf-8") as f:
                f.write(line)
            logger.info("已记录提现到 %s：%s", fname, line.strip())
        except Exception as e:
            logger.error("写入提现记录文件失败: %s", e)

    def record_total_asset(self, index, api_key, address, network, total_usdt):
        total_dec = Decimal(str(total_usdt))
        line = f"{index}+{api_key}+{total_dec:.8f}\n"

        try:
            fname = "total_asset_result.txt"
            with open(fname, "a", encoding="utf-8") as f:
                f.write(line)
            logger.info("已记录总资产到 %s：%s", fname, line.strip())
        except Exception as e:
            logger.error("写入总资产记录文件失败: %s", e)

        self.total_asset_results[index] = {
            "index": index,
            "api_key": api_key,
            "address": address,
            "network": network,
            "total_usdt": total_dec,
        }

    def export_total_asset_csv(self):
        if not self.total_asset_results:
            messagebox.showinfo("提示", "当前没有可导出的总资产结果，请先运行一次“批量查询总资产”")
            return

        path = filedialog.asksaveasfilename(
            title="导出总资产 CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            rows = sorted(self.total_asset_results.values(), key=lambda x: x["index"])

            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "API_KEY", "提现地址", "网络", "总资产(USDT)"])
                for row in rows:
                    writer.writerow([
                        row["index"],
                        row["api_key"],
                        row["address"],
                        row["network"],
                        f'{row["total_usdt"]:.8f}',
                    ])

            logger.info("总资产 CSV 已导出到：%s", path)
            messagebox.showinfo("成功", f"总资产 CSV 已导出到：\n{path}")
        except Exception as e:
            logger.error("导出总资产 CSV 失败: %s", e)
            messagebox.showerror("错误", "导出总资产 CSV 失败: %s" % e)

    @staticmethod
    def _normalize_exchange_proxy(proxy_text: str) -> str:
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
            raise RuntimeError("代理地址格式不支持，请使用 http://、https://、socks5:// 或 socks5h://")
        return proxy

    def _get_exchange_proxy(self) -> str:
        return self._normalize_exchange_proxy(self.exchange_proxy_var.get())

    def _get_exchange_proxy_url(self) -> str:
        proxy = self._get_exchange_proxy()
        if not proxy:
            self.exchange_proxy_runtime.stop()
            return ""
        if proxy.lower().startswith("ss://"):
            return self.exchange_proxy_runtime.ensure_proxy(proxy)
        return proxy

    def _requests_proxy_map(self) -> dict[str, str]:
        proxy = self._get_exchange_proxy_url()
        if not proxy:
            return {}
        return {"http": proxy, "https": proxy}

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

    def _exchange_proxy_route_text(self) -> str:
        raw_proxy = str(self.exchange_proxy_var.get() or "").strip()
        if not raw_proxy:
            system_proxy = self._system_proxy_map()
            if system_proxy:
                return f"system-proxy -> {system_proxy.get('https') or system_proxy.get('http')}"
            return "direct"
        if raw_proxy.lower().startswith("ss://"):
            snap = self.exchange_proxy_runtime.snapshot()
            backend = snap.get("backend") or "unknown"
            local_proxy = snap.get("local_proxy_url") or "pending"
            return f"builtin-ss/{backend} -> {local_proxy}"
        return f"manual-proxy -> {self._normalize_exchange_proxy(raw_proxy)}"

    def _create_binance_client(self, key: str, secret: str) -> BinanceClient:
        return BinanceClient(key, secret, proxy_url=self._get_exchange_proxy_url())

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
            "exchange_proxy_enc": self._encrypt_optional_text(self._get_exchange_proxy()),
            "spot_rounds": int(trade_settings["stored_spot_rounds"]),
            "trade_mode": trade_settings["trade_mode"],
            "premium_percent": trade_settings["premium_percent"],
            "bnb_fee_stop": trade_settings["bnb_fee_stop"],
            "bnb_topup_amount": trade_settings["bnb_topup_amount"],
            "reprice_threshold_percent": trade_settings["reprice_threshold_percent"],
            "spot_symbol": self.spot_symbol_var.get().strip().upper(),
            "spot_precision": int(self.spot_precision_var.get()),
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
            payload = self._strategy_config_payload()
            STRATEGY_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
            STRATEGY_CONFIG_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("策略配置已保存到：%s", STRATEGY_CONFIG_FILE)
            self.update_ip(schedule_next=False)
            messagebox.showinfo("成功", f"策略配置已保存到：\n{STRATEGY_CONFIG_FILE}")
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "配置格式不正确，请检查交易模式、轮次、溢价比例、手续费停止值和超时时间")
        except Exception as e:
            logger.error("保存策略配置失败: %s", e)
            messagebox.showerror("错误", "保存策略配置失败: %s" % e)

    def _load_strategy_config(self):
        if not STRATEGY_CONFIG_FILE.exists():
            return
        try:
            raw = json.loads(STRATEGY_CONFIG_FILE.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise RuntimeError("配置文件结构无效")

            self.api_key_var.set(SECRET_BOX.decrypt(str(raw.get("api_key", "") or "").strip()).strip())
            self.api_secret_var.set(SECRET_BOX.decrypt(str(raw.get("api_secret", "") or "").strip()).strip())
            proxy_enc = str(raw.get("exchange_proxy_enc", "") or "").strip()
            legacy_proxy = str(raw.get("exchange_proxy", EXCHANGE_PROXY_DEFAULT) or EXCHANGE_PROXY_DEFAULT).strip()
            try:
                proxy_text = self._decrypt_optional_text(proxy_enc) if proxy_enc else legacy_proxy
            except Exception:
                proxy_text = legacy_proxy
            self.exchange_proxy_var.set(proxy_text)
            self.spot_rounds_var.set(int(raw.get("spot_rounds", SPOT_ROUNDS_DEFAULT)))
            self.trade_mode_var.set(self._normalize_trade_mode(raw.get("trade_mode", TRADE_MODE_DEFAULT)))
            self.premium_percent_var.set(str(raw.get("premium_percent", PREMIUM_PERCENT_DEFAULT) or PREMIUM_PERCENT_DEFAULT).strip())
            self.bnb_fee_stop_var.set(str(raw.get("bnb_fee_stop", BNB_FEE_STOP_DEFAULT) or BNB_FEE_STOP_DEFAULT).strip())
            self.bnb_topup_amount_var.set(str(raw.get("bnb_topup_amount", BNB_TOPUP_AMOUNT_DEFAULT) or BNB_TOPUP_AMOUNT_DEFAULT).strip())
            self.reprice_threshold_percent_var.set(
                str(raw.get("reprice_threshold_percent", REPRICE_THRESHOLD_PERCENT_DEFAULT) or REPRICE_THRESHOLD_PERCENT_DEFAULT).strip()
            )
            self.spot_symbol_var.set(str(raw.get("spot_symbol", SPOT_SYMBOL_DEFAULT) or SPOT_SYMBOL_DEFAULT).strip().upper())
            self.spot_precision_var.set(int(raw.get("spot_precision", SPOT_PRECISION_DEFAULT)))
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

    def _fetch_public_ip(self, *, use_exchange_proxy: bool, allow_system_proxy: bool = True) -> str:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://ipinfo.io/ip",
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        proxies = self._requests_proxy_map() if use_exchange_proxy else None
        for url in urls:
            try:
                r = http_get_via_proxy(
                    url,
                    headers=headers,
                    timeout=6,
                    proxies=proxies or None,
                    allow_system_proxy=allow_system_proxy,
                )
                r.raise_for_status()
                ip = (r.text or "").strip()
                ipaddress.ip_address(ip)
                return ip
            except Exception:
                continue
        raise RuntimeError("网络不可达或 IP 服务异常")

    def _test_exchange_proxy_once(self, *, include_exit_ip: bool = True) -> tuple[str, str]:
        proxies = self._requests_proxy_map()
        proxy_text = self.exchange_proxy_var.get().strip()
        system_proxy = self._system_proxy_map() if not proxy_text else {}
        proxy_status = "跟随系统代理" if system_proxy else "未启用"
        proxy_exit_ip = "--"
        if proxy_text:
            proxy_status = "SS代理连接中..." if proxy_text.lower().startswith("ss://") else "代理连接中..."
        if proxy_text:
            test_resp = http_get_via_proxy(
                "https://api.binance.com/api/v3/time",
                proxies=proxies or None,
                timeout=10,
                allow_system_proxy=False,
            )
            test_resp.raise_for_status()
            proxy_status = "SS代理已连接" if proxy_text.lower().startswith("ss://") else "代理已连接"
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=True, allow_system_proxy=False)
        elif system_proxy:
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=True)
        else:
            if include_exit_ip:
                proxy_exit_ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
        return proxy_status, proxy_exit_ip

    def test_exchange_proxy(self):
        def worker():
            try:
                status, exit_ip = self._test_exchange_proxy_once()
                route_text = self._exchange_proxy_route_text()
                log_text = f"交易所代理测试成功：status={status}，exit_ip={exit_ip}，route={route_text}"
            except Exception as e:
                status = "连接失败" if self.exchange_proxy_var.get().strip() else "未启用"
                exit_ip = "--"
                route_text = self._exchange_proxy_route_text()
                log_text = f"交易所代理测试失败：{e}，route={route_text}"

            def _update():
                self.exchange_proxy_status_var.set(status)
                self.exchange_proxy_exit_ip_var.set(exit_ip)
                self._append_log(log_text)
                if "失败" in log_text:
                    messagebox.showerror("代理测试失败", log_text)
                else:
                    messagebox.showinfo("代理测试成功", log_text)

            self.after(0, _update)

        threading.Thread(target=worker, daemon=True).start()

    def update_ip(self, schedule_next: bool = True):
        def worker():
            proxy_status = "跟随系统代理" if self._system_proxy_map() and not self.exchange_proxy_var.get().strip() else "未启用"
            proxy_exit_ip = "--"
            try:
                ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
                if self.exchange_proxy_var.get().strip():
                    proxy_status, proxy_exit_ip = self._test_exchange_proxy_once(include_exit_ip=True)
                elif self._system_proxy_map():
                    proxy_status, proxy_exit_ip = self._test_exchange_proxy_once(include_exit_ip=True)
                else:
                    proxy_exit_ip = ip
            except Exception as e:
                ip = "获取失败: %s" % str(e)
                if self.exchange_proxy_var.get().strip():
                    proxy_status = "连接失败"
                elif self._system_proxy_map():
                    proxy_status = "系统代理异常"

            def _update():
                self.ip_var.set(ip)
                self.exchange_proxy_status_var.set(proxy_status)
                self.exchange_proxy_exit_ip_var.set(proxy_exit_ip)
            self.after(0, _update)

        threading.Thread(target=worker, daemon=True).start()
        if schedule_next:
            self.after(60000, self.update_ip)

    def _poll_log_queue(self):
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                self._append_log(msg)
        self.after(100, self._poll_log_queue)

    def _append_log(self, msg: str):
        self.text_log.configure(state="normal")
        self.text_log.insert("end", msg + "\n")
        self.text_log.see("end")
        self.text_log.configure(state="disabled")

    def _set_account_manage_buttons_state(self, state):
        for name in (
            "btn_add_account",
            "btn_toggle_select_accounts",
            "btn_del_accounts",
            "btn_export_accounts",
            "btn_import_accounts",
            "btn_paste_accounts",
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
            "trade_mode_combo",
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

    def wait_for_usdt(self, timeout_sec, stop_event, client=None, symbol: str = ""):
        start = time.time()
        c = client or self.client
        if c is None:
            logger.error("wait_for_usdt 调用时没有可用的 BinanceClient")
            return False
        quote_asset = BinanceClient.get_spot_quote_asset(symbol) if symbol else "USDT"

        while time.time() - start < timeout_sec:
            if stop_event and stop_event.is_set():
                logger.info("检测 %s 时收到停止信号，结束检测", quote_asset)
                return False
            try:
                quote_balance = c.spot_balance(quote_asset)
                logger.info("%s 到账检测中，当前现货 %s = %.8f", quote_asset, quote_asset, quote_balance)
            except Exception as e:
                logger.error("检测 %s 余额失败: %s", quote_asset, e)
                quote_balance = 0.0

            if quote_balance > 0:
                logger.info("检测到 %s 已到账，开始执行后续策略", quote_asset)
                return True

            delay_seconds = min(self._current_random_delay_seconds(), max(0.0, timeout_sec - (time.time() - start)))
            if delay_seconds <= 0:
                continue
            logger.info("%s 未到账，%.3f 秒后重试", quote_asset, delay_seconds)
            if stop_event:
                if stop_event.wait(delay_seconds):
                    logger.info("检测 %s 等待期间收到停止信号，结束检测", quote_asset)
                    return False
            else:
                time.sleep(delay_seconds)

        logger.error("在 %d 秒内未检测到 %s 到账，终止任务", timeout_sec, quote_asset)
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
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_percent_value = trade_settings["premium_percent_value"]
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_percent_value = trade_settings["reprice_threshold_percent_value"]
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = int(self.usdt_timeout_var.get())
            spot_precision = int(self.spot_precision_var.get())
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        quote_asset = BinanceClient.get_spot_quote_asset(spot_symbol)
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_address = self.withdraw_addr_var.get().strip()
        withdraw_network = self.withdraw_net_var.get().strip()
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        if enable_withdraw and (not withdraw_address or not withdraw_network or not withdraw_coin):
            messagebox.showerror("错误", "开启自动提现时，请填写 提现地址 / 网络 / 币种")
            return

        try:
            self.client = self._create_binance_client(key, secret)
        except Exception as e:
            messagebox.showerror("错误", "创建 BinanceClient 失败: %s" % e)
            return

        logger.info("交易所单账号链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：单账号运行中...")
        self.progress["value"] = 0
        total_steps = max(spot_rounds, 1) if trade_mode == TRADE_MODE_MARKET else 1
        self.progress["maximum"] = total_steps

        def sleep_fn():
            if self.stop_event and self.stop_event.is_set():
                return
            self.random_sleep(min_delay, max_delay)

        def withdraw_callback(amount, idx=1, api_key=key, address=withdraw_address):
            if amount > 0:
                self.record_withdraw(idx, api_key, address, amount)

        strategy = Strategy(
            client=self.client,
            spot_rounds=spot_rounds,
            withdraw_coin=withdraw_coin,
            withdraw_address=withdraw_address,
            withdraw_network=withdraw_network,
            withdraw_fee_buffer=withdraw_buffer,
            spot_symbol=spot_symbol,
            spot_precision=spot_precision,
            sleep_fn=sleep_fn,
            enable_withdraw=enable_withdraw,
            withdraw_callback=withdraw_callback,
            trade_mode=trade_mode,
            premium_percent=premium_percent_value,
            bnb_fee_stop_value=bnb_fee_stop_value,
            bnb_topup_amount=bnb_topup_amount_value,
            reprice_threshold_percent=reprice_threshold_percent_value,
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self.after(0, _update)

        def worker():
            try:
                if not self.wait_for_usdt(usdt_timeout, self.stop_event, symbol=spot_symbol):
                    logger.info("%s 检测未通过，任务结束", quote_asset)
                    return

                if trade_mode == TRADE_MODE_MARKET:
                    logger.info("开始执行策略：市价 %d 轮，预买BNB金额=%s", spot_rounds, bnb_topup_amount_value)
                elif trade_mode == TRADE_MODE_LIMIT:
                    logger.info(
                        "开始执行策略：挂单模式，预买BNB金额=%s，BNB 手续费停止值=%s，重新挂单阈值=%s%%",
                        bnb_topup_amount_value,
                        bnb_fee_stop_value,
                        reprice_threshold_percent_value,
                    )
                else:
                    logger.info(
                        "开始执行策略：溢价单模式，预买BNB金额=%s，溢价百分比=%s，BNB 手续费停止值=%s，重新挂单阈值=%s%%",
                        bnb_topup_amount_value,
                        premium_percent_value,
                        bnb_fee_stop_value,
                        reprice_threshold_percent_value,
                    )
                strategy.run(self.stop_event, progress_cb=progress_cb)
            except Exception as e:
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def _on_worker_finished(self):
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.btn_run_accounts.config(state="normal")
        self.btn_query_all_assets.config(state="normal")
        self.btn_collect_bnb_combo.config(state="normal")
        self.btn_batch_withdraw.config(state="normal")
        self.btn_refresh.config(state="normal")
        self.btn_withdraw.config(state="normal")
        self._set_account_manage_buttons_state("normal")
        self._set_combo_states_for_run(False)
        self.status_var.set("状态：已完成/已停止")
        self.progress["value"] = 0

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
            try:
                logger.info("交易所刷新余额链路：%s", self._exchange_proxy_route_text())
                client = self._create_binance_client(key, secret)
                spot_balances = client.spot_all_balances()
                balances_text = self._format_spot_balances_text(spot_balances)

                def _update():
                    self.client = client
                    self.single_account_balances_var.set(balances_text)
                    logger.info("余额刷新完成")
                self.after(0, _update)
            except Exception as e:
                logger.error("刷新余额失败: %s", e)

        threading.Thread(target=worker, daemon=True).start()

    def manual_withdraw(self):
        if not self.client:
            messagebox.showerror("错误", "请先使用当前 API 创建连接（点击一次开始或刷新余额）")
            return

        enable_withdraw = bool(self.enable_withdraw_var.get())
        address = self.withdraw_addr_var.get().strip()
        network = self.withdraw_net_var.get().strip()
        coin = self.withdraw_coin_var.get().strip().upper()
        try:
            buffer_val = float(self.withdraw_buffer_var.get())
        except ValueError:
            messagebox.showerror("错误", "手续费预留格式不正确")
            return

        if enable_withdraw and (not address or not network or not coin):
            messagebox.showerror("错误", "请填写 提现地址 / 网络 / 币种")
            return

        def worker():
            try:
                logger.info(f"手动触发提现 {coin}")
                amount = self.client.withdraw_all_coin(
                    coin=coin,
                    address=address,
                    network=network,
                    fee_buffer=buffer_val,
                    enable_withdraw=enable_withdraw,
                )
                if amount > 0:
                    self.record_withdraw(1, self.client.key, address, amount)
            except Exception as e:
                logger.error("手动提现失败: %s", e)

        threading.Thread(target=worker, daemon=True).start()

    def _reindex_accounts(self):
        for i, acc in enumerate(self.accounts, start=1):
            acc["index_var"].set(str(i))
        self._refresh_account_list_hint()

    def _on_global_network_changed(self, *_):
        self.apply_network_to_all_accounts()

    def apply_network_to_all_accounts(self):
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for acc in self.accounts:
            acc["network"] = net
            if "network_var" in acc:
                acc["network_var"].set(net)

    @staticmethod
    def _account_row_color_by_status(status_text: str) -> str:
        s = str(status_text or "").strip()
        if not s or s == "就绪":
            return "#f2f2f2"
        if "未到账" in s:
            return "#ffe3b8"
        if any(k in s for k in ("失败", "异常")):
            return "#f8c7c7"
        if any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "#cfeecf"
        return "#cfe3ff"

    def _is_context_account(self, acc: dict) -> bool:
        return bool(acc is not None and acc is getattr(self, "_context_account", None))

    def _apply_account_row_style(self, acc: dict):
        if self._is_context_account(acc):
            bg = "#7A3FF2"
            fg = "#FFFFFF"
        else:
            bg = self._account_row_color_by_status(acc.get("status_var").get())
            fg = "#111111"
        row_frame = acc.get("frame")
        if row_frame is not None:
            try:
                row_frame.configure(bg=bg)
            except Exception:
                pass
        for widget in acc.get("row_widgets", []):
            try:
                widget.configure(bg=bg)
            except Exception:
                pass
            try:
                widget.configure(fg=fg)
            except Exception:
                pass
            try:
                widget.configure(activebackground=bg)
            except Exception:
                pass
            try:
                widget.configure(activeforeground=fg)
            except Exception:
                pass
            try:
                widget.configure(selectcolor=bg)
            except Exception:
                pass

    def _set_account_status(self, acc: dict, text: str):
        acc["status_var"].set(str(text))
        self._apply_account_row_style(acc)

    def _set_context_account(self, acc: dict | None):
        previous = getattr(self, "_context_account", None)
        self._context_account = acc
        if previous is not None and previous is not acc:
            self._apply_account_row_style(previous)
        if acc is not None:
            self._apply_account_row_style(acc)

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

    def run_context_account_query(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_selected_accounts(
            accounts_to_run=[acc],
            batch_total_asset_only=True,
            require_confirm=False,
        )

    def run_context_account_withdraw(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.batch_manual_withdraw(accounts_to_run=[acc], require_confirm=True)

    def run_context_account_collect_bnb(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_batch_collect_bnb_with_confirm(accounts_to_run=[acc])

    @staticmethod
    def _format_amount(value: float, precision: int = 8) -> str:
        text = f"{float(value):.{precision}f}" if value is not None else "0"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text else "0"

    @classmethod
    def _format_spot_balances_text(cls, balances: list[dict]) -> str:
        if not balances:
            return "--"
        parts = []
        for item in balances:
            asset = str(item.get("asset") or "").strip().upper()
            total = float(item.get("total", 0) or 0)
            if not asset or total <= 0:
                continue
            parts.append(f"{asset} {cls._format_amount(total)}")
        return " | ".join(parts) if parts else "--"

    @classmethod
    def _format_withdraw_amount_status(cls, amount: float, coin: str, *, enable_withdraw: bool) -> str:
        coin_u = str(coin or "").strip().upper()
        if amount > 0:
            return f"提现额度 {cls._format_amount(amount)} {coin_u}"
        if not enable_withdraw:
            return f"提现额度 0 {coin_u}（自动提现已关闭）"
        return f"提现额度 0 {coin_u}（可提余额不足/预留过高）"

    @staticmethod
    def _compact_error_text(err_text: str, max_len: int = 28) -> str:
        s = str(err_text or "").strip().replace("\n", " ")
        if not s:
            return "未知错误"
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")

    def _append_account_row(self, key, secret, addr, net, selected=True):
        net = (net or "").strip() or WITHDRAW_NETWORK_DEFAULT

        row_frame = tk.Frame(self.accounts_container, bg="#f7f7f7")
        row_frame.pack(fill="x", padx=2, pady=1)

        index_var = tk.StringVar()
        selected_var = tk.BooleanVar(value=selected)
        selected_var.trace_add("write", lambda *_args: self._update_toggle_select_button_text())
        network_var = tk.StringVar(value=net)
        status_var = tk.StringVar(value="就绪")

        lbl_index = tk.Label(row_frame, textvariable=index_var, width=4, anchor="w", bg="#f7f7f7")
        chk_selected = tk.Checkbutton(
            row_frame,
            variable=selected_var,
            bg="#f7f7f7",
            activebackground="#f7f7f7",
            bd=0,
            highlightthickness=0,
            relief="flat",
        )
        lbl_key = tk.Label(row_frame, text=self._mask_key(key), width=25, anchor="w", bg="#f7f7f7")
        lbl_addr = tk.Label(row_frame, text=self._mask_addr(addr), width=35, anchor="w", bg="#f7f7f7")
        lbl_net = tk.Label(row_frame, textvariable=network_var, width=8, anchor="w", bg="#f7f7f7")
        lbl_status = tk.Label(row_frame, textvariable=status_var, width=30, anchor="w", bg="#f7f7f7")

        for w in (lbl_index, chk_selected, lbl_key, lbl_addr, lbl_net, lbl_status):
            w.pack(side="left", padx=2)

        acc = {
            "frame": row_frame,
            "row_widgets": [lbl_index, chk_selected, lbl_key, lbl_addr, lbl_net, lbl_status],
            "index_var": index_var,
            "selected_var": selected_var,
            "network_var": network_var,
            "status_var": status_var,
            "api_key": key,
            "api_secret": secret,
            "address": addr,
            "network": net,
        }
        self._apply_account_row_style(acc)
        for widget in (row_frame, *acc["row_widgets"]):
            widget.bind("<Button-3>", lambda event, a=acc: self._show_account_row_menu(event, a), add="+")
            widget.bind("<Button-2>", lambda event, a=acc: self._show_account_row_menu(event, a), add="+")
            widget.bind("<Control-Button-1>", lambda event, a=acc: self._show_account_row_menu(event, a), add="+")
        self.accounts.append(acc)
        self._update_toggle_select_button_text()
        self._refresh_account_list_hint()
        return acc

    def add_account_to_list(self):
        key = self.acc_api_key_var.get().strip()
        secret = self.acc_api_secret_var.get().strip()
        addr = self.acc_withdraw_addr_var.get().strip()
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT

        if not key or not secret or not addr:
            messagebox.showerror("错误", "请完整填写：API KEY / SECRET / 提现地址")
            return

        self._append_account_row(key, secret, addr, net)
        self._reindex_accounts()
        self._update_toggle_select_button_text()

        self.acc_api_key_var.set("")
        self.acc_api_secret_var.set("")
        self.acc_withdraw_addr_var.set("")

    def delete_selected_accounts(self):
        selected_count = sum(1 for acc in self.accounts if acc["selected_var"].get())
        if selected_count <= 0:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        if not messagebox.askyesno("确认删除", f"确认删除已勾选的 {selected_count} 个账号吗？"):
            return
        current_context = getattr(self, "_context_account", None)
        keep = []
        for acc in self.accounts:
            if acc["selected_var"].get():
                acc["frame"].destroy()
            else:
                keep.append(acc)
        self.accounts = keep
        if current_context is not None and current_context not in keep:
            self._set_context_account(None)
        self._reindex_accounts()
        self._update_toggle_select_button_text()

    def select_all_accounts(self):
        for acc in self.accounts:
            acc["selected_var"].set(True)
        self._update_toggle_select_button_text()

    def deselect_all_accounts(self):
        for acc in self.accounts:
            acc["selected_var"].set(False)
        self._update_toggle_select_button_text()

    def toggle_select_all_accounts(self):
        if self.accounts and all(acc["selected_var"].get() for acc in self.accounts):
            self.deselect_all_accounts()
        else:
            self.select_all_accounts()

    def _update_toggle_select_button_text(self):
        btn = getattr(self, "btn_toggle_select_accounts", None)
        if btn is None:
            return
        all_selected = bool(self.accounts) and all(acc["selected_var"].get() for acc in self.accounts)
        btn.config(text="取消全选" if all_selected else "全选")

    def export_accounts(self):
        if not self.accounts:
            messagebox.showinfo("提示", "当前账号列表为空，无需导出")
            return

        path = filedialog.asksaveasfilename(
            title="导出账号列表到文件",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            with open(path, "w", encoding="utf-8") as f:
                for acc in self.accounts:
                    line = "|".join([
                        acc["api_key"],
                        acc["api_secret"],
                        acc["address"],
                        acc["network"],
                    ])
                    f.write(line + "\n")
            logger.info("账号列表已导出到文件：%s", path)
            messagebox.showinfo("成功", f"账号列表已导出到：\n{path}")
        except Exception as e:
            logger.error("导出账号列表失败: %s", e)
            messagebox.showerror("错误", "导出账号列表失败: %s" % e)

    def import_accounts(self):
        path = filedialog.askopenfilename(
            title="导入账号列表文件",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not path:
            return

        try:
            try:
                with open(path, "r", encoding="utf-8-sig") as f:
                    raw = f.read()
            except UnicodeDecodeError:
                with open(path, "r", encoding="gb18030", errors="ignore") as f:
                    raw = f.read()

            self._import_accounts_from_text(raw, "文件")
        except Exception as e:
            logger.error("导入账号列表失败: %s", e)
            messagebox.showerror("错误", "导入账号列表失败: %s" % e)

    def import_accounts_from_clipboard(self):
        try:
            raw = self.clipboard_get()
        except tk.TclError:
            messagebox.showerror("错误", "剪贴板内容不可用或为空")
            return

        self._import_accounts_from_text(raw, "剪贴板")

    def _get_selected_accounts(self):
        return [acc for acc in self.accounts if acc["selected_var"].get()]

    def run_query_total_assets_for_all_accounts(self):
        all_accounts = list(self.accounts)
        if not all_accounts:
            messagebox.showinfo("提示", "当前没有可查询的账号")
            return
        self.run_selected_accounts(accounts_to_run=all_accounts, batch_total_asset_only=True, require_confirm=False)

    def run_batch_collect_bnb_with_confirm(self, accounts_to_run=None):
        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "请至少勾选一个账号")
            return
        text = (
            f"即将对 {len(selected)} 个账号执行“归集并买BNB”。\n"
            "流程包含：归集合约/资金到现货、小额币兑换BNB、卖出大额币换USDT、再买入BNB。\n\n"
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
        require_confirm: bool = True,
    ):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "已有任务在运行中，请先停止当前任务")
            return

        selected = list(accounts_to_run) if accounts_to_run is not None else self._get_selected_accounts()
        if not selected:
            messagebox.showinfo("提示", "当前没有可执行的账号")
            return
        if require_confirm and not batch_total_asset_only and not batch_collect_bnb_mode:
            if not messagebox.askyesno("确认执行", f"确认对 {len(selected)} 个勾选账号执行批量策略吗？"):
                return

        trade_settings = None
        try:
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = int(self.usdt_timeout_var.get())
            spot_precision = int(self.spot_precision_var.get())
            max_threads = int(self.max_threads_var.get())
            if (not batch_total_asset_only) and (not batch_collect_bnb_mode):
                trade_settings = self._collect_trade_mode_settings()
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确 (请检查轮数/线程数/延迟等)")
            return

        if max_threads < 1:
            max_threads = 1

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        quote_asset = BinanceClient.get_spot_quote_asset(spot_symbol)
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()
        if trade_settings:
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_percent_value = trade_settings["premium_percent_value"]
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_percent_value = trade_settings["reprice_threshold_percent_value"]
        else:
            spot_rounds = max(1, int(self.spot_rounds_var.get() or SPOT_ROUNDS_DEFAULT))
            trade_mode = TRADE_MODE_MARKET
            premium_percent_value = None
            bnb_fee_stop_value = None
            bnb_topup_amount_value = Decimal("0")
            reprice_threshold_percent_value = Decimal(REPRICE_THRESHOLD_PERCENT_DEFAULT)

        skip_usdt_wait_in_batch = bool(self.skip_usdt_wait_in_batch_var.get())

        if batch_total_asset_only:
            self.total_asset_results = {}

        logger.info("交易所批量链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)

        if batch_total_asset_only:
            self.status_var.set(f"状态：查询全部总资产中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            self.status_var.set(f"状态：归集并买BNB中 (并发 {max_threads} 线程)...")
        elif batch_collect_bnb_mode:
            self.status_var.set(f"状态：批量归集BNB模式运行中 (并发 {max_threads} 线程)...")
        else:
            self.status_var.set(f"状态：批量执行中 (并发 {max_threads} 线程)...")

        self.progress["value"] = 0
        total_accounts = len(selected)
        self.progress["maximum"] = total_accounts

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

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
            self.after(0, _u)

        def worker_loop(thread_id):
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    def _u():
                        self._set_account_status(acc_ref, text)
                    self.after(0, _u)

                def progress_cb(step, total, text, acc_obj=acc):
                    def _u():
                        self._set_account_status(acc_obj, text)
                    self.after(0, _u)

                logger.info(f"[线程 {thread_id}] 开始处理账号 #{idx}")

                should_finish_in_finally = True

                try:
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"])

                    # 1) 只查询总资产
                    if batch_total_asset_only:
                        set_status("查询总资产...")
                        total_usdt, rows = client.query_total_wallet_balance("USDT")

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
                            "账号 #%d 总资产约 %s USDT；%s",
                            idx,
                            f"{total_usdt:.8f}",
                            detail_text if detail_text else "无非零钱包余额"
                        )

                        set_status(f"总资产 {Decimal(str(total_usdt)):.4f} USDT")

                    # 2) 批量归集BNB模式
                    elif batch_collect_bnb_mode:
                        logger.info(f"账号 #{idx} 开始执行【批量归集BNB模式】")
                        set_status("归集合约/资金...")
                        client.collect_all_to_spot()

                        if self.stop_event.is_set():
                            set_status("已停止")
                            should_finish_in_finally = False
                            finish_one()
                            task_queue.task_done()
                            logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                            return

                        time.sleep(1)

                        set_status("小额兑换为BNB...")
                        try:
                            convert_result = client.convert_spot_dust_to_bnb()
                            logger.info(f"账号 #{idx} 小额兑换完成，数量: {len(convert_result)}")
                        except Exception as e:
                            logger.warning(f"账号 #{idx} 小额兑换失败: {e}")

                        if self.stop_event.is_set():
                            set_status("已停止")
                            should_finish_in_finally = False
                            finish_one()
                            task_queue.task_done()
                            logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                            return

                        time.sleep(1)

                        if batch_sell_large_spot_to_bnb:
                            set_status("大额币卖USDT...")
                            try:
                                sold_assets = client.sell_large_spot_assets_to_usdt()
                                logger.info(f"账号 #{idx} 大额币卖出完成: {sold_assets}")
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 大额币卖出失败: {e}")

                            if self.stop_event.is_set():
                                set_status("已停止")
                                should_finish_in_finally = False
                                finish_one()
                                task_queue.task_done()
                                logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                                return

                            time.sleep(1)

                            set_status("USDT买入BNB...")
                            try:
                                buy_ok = client.spot_buy_all_usdt(buffer=0.0, symbol="BNBUSDT")
                                if buy_ok:
                                    logger.info("账号 #%d 已将现货 USDT 买入为 BNB", idx)
                                else:
                                    logger.info("账号 #%d 无可用 USDT 买入 BNB", idx)
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 用 USDT 买 BNB 失败: {e}")

                            time.sleep(1)

                        set_status("查询BNB余额...")
                        bnb_balance = client.spot_balance("BNB")
                        logger.info("账号 #%d 当前现货 BNB = %.8f", idx, bnb_balance)

                        set_status("提现BNB...")
                        try:
                            amount = client.withdraw_all_coin(
                                coin="BNB",
                                address=acc["address"],
                                network=(acc["network"] or WITHDRAW_NETWORK_DEFAULT),
                                fee_buffer=withdraw_buffer,
                                enable_withdraw=enable_withdraw,
                            )
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            set_status(self._format_withdraw_amount_status(amount, "BNB", enable_withdraw=enable_withdraw))
                        except Exception as e:
                            logger.error(f"账号 #{idx} BNB提现失败: {e}")
                            set_status("BNB提现失败")

                    # 3) 原批量现货策略
                    else:
                        need_wait_usdt = not skip_usdt_wait_in_batch

                        if need_wait_usdt:
                            set_status(f"检测 {quote_asset} 到账...")
                            if not self.wait_for_usdt(usdt_timeout, self.stop_event, client=client, symbol=spot_symbol):
                                logger.info(f"账号 #{idx} {quote_asset} 检测超时，跳过")
                                set_status(f"{quote_asset} 未到账")
                                should_finish_in_finally = False
                                finish_one()
                                task_queue.task_done()
                                logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")
                                continue
                        else:
                            logger.info(f"账号 #{idx} 已开启“批量策略跳过{quote_asset}检测”")
                            set_status(f"跳过{quote_asset}检测")

                        set_status("策略执行中...")
                        if trade_mode == TRADE_MODE_MARKET:
                            logger.info("账号 #%d 开始执行市价策略：%d 轮，预买BNB金额=%s", idx, spot_rounds, bnb_topup_amount_value)
                        elif trade_mode == TRADE_MODE_LIMIT:
                            logger.info(
                                "账号 #%d 开始执行挂单策略：预买BNB金额=%s，BNB 手续费停止值=%s，重新挂单阈值=%s%%",
                                idx,
                                bnb_topup_amount_value,
                                bnb_fee_stop_value,
                                reprice_threshold_percent_value,
                            )
                        else:
                            logger.info(
                                "账号 #%d 开始执行溢价单策略：预买BNB金额=%s，溢价百分比=%s，BNB 手续费停止值=%s，重新挂单阈值=%s%%",
                                idx,
                                bnb_topup_amount_value,
                                premium_percent_value,
                                bnb_fee_stop_value,
                                reprice_threshold_percent_value,
                            )

                        def sleep_fn():
                            if self.stop_event.is_set():
                                return
                            self.random_sleep(min_delay, max_delay)

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

                        strategy = Strategy(
                            client=client,
                            spot_rounds=spot_rounds,
                            withdraw_coin=withdraw_coin,
                            withdraw_address=acc["address"],
                            withdraw_network=acc["network"],
                            withdraw_fee_buffer=withdraw_buffer,
                            spot_symbol=spot_symbol,
                            spot_precision=spot_precision,
                            sleep_fn=sleep_fn,
                            enable_withdraw=enable_withdraw,
                            withdraw_callback=withdraw_callback,
                            trade_mode=trade_mode,
                            premium_percent=premium_percent_value,
                            bnb_fee_stop_value=bnb_fee_stop_value,
                            bnb_topup_amount=bnb_topup_amount_value,
                            reprice_threshold_percent=reprice_threshold_percent_value,
                        )

                        strategy_result = strategy.run(self.stop_event, progress_cb=progress_cb)

                        if not self.stop_event.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount

                            if isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                set_status(f"提现失败 {self._compact_error_text(strategy_result.get('withdraw_error', ''))}")
                            else:
                                set_status(
                                    self._format_withdraw_amount_status(
                                        float(final_amount or 0.0),
                                        withdraw_coin,
                                        enable_withdraw=enable_withdraw,
                                    )
                                )

                except Exception as e:
                    logger.error(f"账号 #{idx} 执行异常: {e}")
                    set_status("异常")
                finally:
                    if should_finish_in_finally:
                        finish_one()
                        task_queue.task_done()
                        logger.info(f"[线程 {thread_id}] 账号 #{idx} 处理完毕")

        def controller():
            threads = []
            logger.info(f"启动 {max_threads} 个工作线程处理 {total_accounts} 个账号")
            for i in range(max_threads):
                t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            logger.info("批量任务全部结束")
            self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=controller, daemon=True)
        self.worker_thread.start()

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

        enable_withdraw = bool(self.enable_withdraw_var.get())
        coin = self.withdraw_coin_var.get().strip().upper()

        logger.info("交易所批量提现链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.btn_run_accounts.config(state="disabled")
        self.btn_query_all_assets.config(state="disabled")
        self.btn_collect_bnb_combo.config(state="disabled")
        self.btn_batch_withdraw.config(state="disabled")
        self.btn_refresh.config(state="disabled")
        self.btn_withdraw.config(state="disabled")
        self._set_account_manage_buttons_state("disabled")
        self._set_combo_states_for_run(True)
        self.status_var.set("状态：批量提现中...")
        self.progress["value"] = 0
        self.progress["maximum"] = len(selected)

        task_queue = queue.Queue()
        for idx, acc in enumerate(selected, start=1):
            task_queue.put((idx, acc))

        total_accounts = len(selected)
        completed_count = 0
        count_lock = threading.Lock()

        def withdraw_worker(thread_id):
            nonlocal completed_count
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    self.after(0, lambda: self._set_account_status(acc_ref, text))

                set_status(f"提现 {coin}...")
                try:
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                    amount = client.withdraw_all_coin(
                        coin=coin,
                        address=acc["address"],
                        network=acc["network"],
                        fee_buffer=withdraw_buffer,
                        enable_withdraw=enable_withdraw,
                    )
                    if amount > 0:
                        self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                    set_status(self._format_withdraw_amount_status(amount, coin, enable_withdraw=enable_withdraw))
                except Exception as e:
                    logger.error(f"账号 #{idx} 提现失败: {e}")
                    set_status("提现失败")
                finally:
                    task_queue.task_done()
                    with count_lock:
                        completed_count += 1
                        curr = completed_count

                    def _u():
                        self.progress["value"] = curr
                        self.status_var.set(f"批量提现进度: {curr}/{total_accounts}")
                    self.after(0, _u)

        def controller():
            threads = []
            for i in range(max_threads):
                t = threading.Thread(target=withdraw_worker, args=(i + 1,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self.after(0, self._on_worker_finished)

        self.worker_thread = threading.Thread(target=controller, daemon=True)
        self.worker_thread.start()


# ====================== 入口 ======================
if __name__ == "__main__":
    try:
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))

        os.chdir(application_path)
    except Exception as e:
        print(f"路径设置失败: {e}")

    app = App()
    app.mainloop()
