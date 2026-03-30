import time
import logging
import ipaddress
import base64
import hashlib
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import threading
import queue
import random
import os
import platform
import sys
import csv
import json
import re
import shutil
import socket
import subprocess
import tarfile
import tempfile
import uuid
import zipfile
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from api_clients import EvmClient
from app_paths import (
    APP_DIR,
    BUNDLE_DIR,
    CONFIG_BACKUP_SUFFIX,
    DATA_DIR,
    EXCHANGE_PROXY_CONFIG_FILE,
    LOG_DIR,
    LOG_FILE_PATH,
    STRATEGY_CONFIG_FILE,
    TOTAL_ASSET_RESULT_FILE,
    WITHDRAW_SUCCESS_FILE,
)
from exchange_binance_client import BinanceClient
from secret_box import SECRET_BOX
from shared_utils import SolidButton, dispatch_ui_callback, make_scrollbar, start_ui_bridge, stop_ui_bridge
from stores import _atomic_write_text, _atomic_write_text_with_backup, _load_json_with_backup

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
EXCHANGE_USE_CONFIG_PROXY_DEFAULT = False

SPOT_SYMBOL_DEFAULT = "BNBUSDT"
SPOT_ROUNDS_DEFAULT = 20
TRADE_ACCOUNT_TYPE_SPOT = "现货"
TRADE_ACCOUNT_TYPE_FUTURES = "合约"
TRADE_ACCOUNT_TYPE_OPTIONS = (
    TRADE_ACCOUNT_TYPE_SPOT,
    TRADE_ACCOUNT_TYPE_FUTURES,
)
TRADE_ACCOUNT_TYPE_DEFAULT = TRADE_ACCOUNT_TYPE_SPOT
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
REPRICE_THRESHOLD_DEFAULT = "0"
FUTURES_SYMBOL_DEFAULT = "BTCUSDT"
FUTURES_ROUNDS_DEFAULT = 20
FUTURES_AMOUNT_DEFAULT = "100"
FUTURES_LEVERAGE_DEFAULT = 10
FUTURES_MARGIN_TYPE_CROSSED = "CROSSED"
FUTURES_MARGIN_TYPE_ISOLATED = "ISOLATED"
FUTURES_MARGIN_TYPE_LABEL_CROSSED = "全仓"
FUTURES_MARGIN_TYPE_LABEL_ISOLATED = "逐仓"
FUTURES_MARGIN_TYPE_OPTIONS = (
    FUTURES_MARGIN_TYPE_CROSSED,
    FUTURES_MARGIN_TYPE_ISOLATED,
)
FUTURES_MARGIN_TYPE_LABEL_OPTIONS = (
    FUTURES_MARGIN_TYPE_LABEL_CROSSED,
    FUTURES_MARGIN_TYPE_LABEL_ISOLATED,
)
FUTURES_MARGIN_TYPE_DEFAULT = FUTURES_MARGIN_TYPE_CROSSED
FUTURES_MARGIN_TYPE_VALUE_TO_LABEL = {
    FUTURES_MARGIN_TYPE_CROSSED: FUTURES_MARGIN_TYPE_LABEL_CROSSED,
    FUTURES_MARGIN_TYPE_ISOLATED: FUTURES_MARGIN_TYPE_LABEL_ISOLATED,
}
FUTURES_MARGIN_TYPE_LABEL_TO_VALUE = {
    FUTURES_MARGIN_TYPE_LABEL_CROSSED: FUTURES_MARGIN_TYPE_CROSSED,
    FUTURES_MARGIN_TYPE_LABEL_ISOLATED: FUTURES_MARGIN_TYPE_ISOLATED,
}
FUTURES_SIDE_LONG = "做多"
FUTURES_SIDE_SHORT = "做空"
FUTURES_SIDE_OPTIONS = (
    FUTURES_SIDE_LONG,
    FUTURES_SIDE_SHORT,
)
FUTURES_SIDE_DEFAULT = FUTURES_SIDE_LONG

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
    _RUNTIME_PREPARE_LOCK = threading.Lock()
    _RUNTIME_HTTP_HEADERS = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "xiaojun-bn/1.0",
    }
    _SING_BOX_RELEASE_API = "https://api.github.com/repos/SagerNet/sing-box/releases/latest"

    def __init__(self, work_dir: Path, runtime_name: str = "exchange"):
        self.work_dir = Path(work_dir)
        self.runtime_name = str(runtime_name or "exchange").strip() or "exchange"
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
    def _collect_executable_candidates(base_dirs: list[Path], exe_names: str | list[str]) -> list[Path]:
        names = [str(exe_names)] if isinstance(exe_names, str) else [str(name) for name in exe_names if str(name).strip()]
        unique: list[Path] = []
        seen: set[str] = set()
        for base_dir in base_dirs:
            candidates: list[Path] = []
            for exe_name in names:
                candidates.append(base_dir / exe_name)
            if base_dir.exists():
                try:
                    for exe_name in names:
                        candidates.extend(sorted(base_dir.rglob(exe_name)))
                except Exception:
                    pass
            for path in candidates:
                key = str(path)
                if key in seen:
                    continue
                seen.add(key)
                unique.append(path)
        return unique

    @staticmethod
    def _runtime_cache_dir(backend: str) -> Path:
        return DATA_DIR / "proxy_runtimes" / backend

    @staticmethod
    def _runtime_platform_tag() -> str:
        if os.name == "nt":
            return "windows"
        if sys.platform == "darwin":
            return "darwin"
        if sys.platform.startswith("linux"):
            return "linux"
        raise RuntimeError(f"当前系统暂不支持内置 SS 代理自动准备：{sys.platform}")

    @staticmethod
    def _runtime_arch_tag() -> str:
        machine = platform.machine().lower()
        mapping = {
            "x86_64": "amd64",
            "amd64": "amd64",
            "arm64": "arm64",
            "aarch64": "arm64",
            "i386": "386",
            "i686": "386",
        }
        return mapping.get(machine, machine)

    @classmethod
    def _executable_name(cls, base_name: str) -> str:
        return f"{base_name}.exe" if os.name == "nt" else base_name

    @classmethod
    def _candidate_executable_names(cls, base_name: str) -> list[str]:
        preferred = cls._executable_name(base_name)
        names = [preferred]
        alt = base_name if preferred.endswith(".exe") else f"{base_name}.exe"
        if alt not in names:
            names.append(alt)
        return names

    @staticmethod
    def _candidate_sing_box_paths() -> list[Path]:
        home = Path.home()
        bundled_candidates = ExchangeProxyRuntime._collect_executable_candidates([
            BUNDLE_DIR / "bin",
            BUNDLE_DIR / "bin" / "sing-box",
            BUNDLE_DIR / "bin" / "sing_box",
            APP_DIR / "bin",
            APP_DIR / "bin" / "sing-box",
            APP_DIR / "bin" / "sing_box",
            ExchangeProxyRuntime._runtime_cache_dir("sing-box"),
        ], ExchangeProxyRuntime._candidate_executable_names("sing-box"))
        candidates = bundled_candidates + [
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
        bundled_candidates = ExchangeProxyRuntime._collect_executable_candidates([
            BUNDLE_DIR / "bin",
            BUNDLE_DIR / "bin" / "xray",
            APP_DIR / "bin",
            APP_DIR / "bin" / "xray",
            ExchangeProxyRuntime._runtime_cache_dir("xray"),
        ], ExchangeProxyRuntime._candidate_executable_names("xray"))
        candidates = bundled_candidates + [
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

    @staticmethod
    def _download_release_json(url: str) -> dict[str, object]:
        resp = requests.get(url, headers=ExchangeProxyRuntime._RUNTIME_HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError("上游发布信息格式异常")
        return data

    @classmethod
    def _select_sing_box_asset(cls, assets: object) -> dict[str, object]:
        if not isinstance(assets, list):
            raise RuntimeError("sing-box 发布资产列表无效")
        platform_tag = cls._runtime_platform_tag()
        arch_tag = cls._runtime_arch_tag()
        archive_suffix = ".zip" if platform_tag == "windows" else ".tar.gz"
        matches: list[dict[str, object]] = []
        legacy_matches: list[dict[str, object]] = []
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            name = str(asset.get("name") or "").strip()
            if not name.startswith("sing-box-") or not name.endswith(archive_suffix):
                continue
            if f"-{platform_tag}-{arch_tag}" not in name:
                continue
            if "legacy-" in name:
                legacy_matches.append(asset)
                continue
            matches.append(asset)
        candidates = matches or legacy_matches
        if not candidates:
            raise RuntimeError(f"官方发布中未找到适配当前系统的 sing-box 资产（platform={platform_tag}, arch={arch_tag}）")
        candidates.sort(key=lambda item: (len(str(item.get("name") or "")), str(item.get("name") or "")))
        return candidates[0]

    @staticmethod
    def _download_asset(url: str, destination: Path, *, digest: str = "") -> None:
        expected_sha256 = ""
        digest_text = str(digest or "").strip()
        if digest_text.startswith("sha256:"):
            expected_sha256 = digest_text.split(":", 1)[1].strip().lower()
        hasher = hashlib.sha256() if expected_sha256 else None
        with requests.get(
            url,
            headers=ExchangeProxyRuntime._RUNTIME_HTTP_HEADERS,
            timeout=(15, 120),
            stream=True,
        ) as resp:
            resp.raise_for_status()
            with open(destination, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=262144):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    if hasher is not None:
                        hasher.update(chunk)
        if hasher is not None and hasher.hexdigest().lower() != expected_sha256:
            raise RuntimeError("下载的 sing-box 运行时校验失败")

    @staticmethod
    def _extract_archive(archive_path: Path, destination: Path) -> None:
        if str(archive_path).lower().endswith(".zip"):
            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(destination)
            return
        with tarfile.open(archive_path, "r:gz") as archive:
            base_dir = destination.resolve()
            for member in archive.getmembers():
                member_path = (destination / member.name).resolve()
                if os.path.commonpath([str(base_dir), str(member_path)]) != str(base_dir):
                    raise RuntimeError(f"压缩包包含非法路径：{member.name}")
            archive.extractall(destination)

    @classmethod
    def _find_extracted_executable(cls, root_dir: Path, base_name: str) -> Path | None:
        for name in cls._candidate_executable_names(base_name):
            try:
                for path in sorted(root_dir.rglob(name)):
                    if path.is_file():
                        return path
            except Exception:
                continue
        return None

    @classmethod
    def _ensure_sing_box_runtime(cls) -> Path:
        with cls._RUNTIME_PREPARE_LOCK:
            for path in cls._candidate_sing_box_paths():
                if path.exists():
                    return path

            release = cls._download_release_json(cls._SING_BOX_RELEASE_API)
            asset = cls._select_sing_box_asset(release.get("assets"))
            asset_name = str(asset.get("name") or "").strip()
            download_url = str(asset.get("browser_download_url") or "").strip()
            if not asset_name or not download_url:
                raise RuntimeError("sing-box 发布资产缺少下载地址")

            tag = str(release.get("tag_name") or "latest").strip() or "latest"
            install_dir = cls._runtime_cache_dir("sing-box") / tag
            install_path = install_dir / cls._executable_name("sing-box")
            if install_path.exists():
                return install_path

            logger.info("未找到 sing-box，开始下载项目内运行时：%s", asset_name)
            install_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="sing-box_", dir=str(cls._runtime_cache_dir("sing-box"))) as tmp_dir:
                tmp_root = Path(tmp_dir)
                archive_path = tmp_root / asset_name
                extract_dir = tmp_root / "extract"
                extract_dir.mkdir(parents=True, exist_ok=True)
                cls._download_asset(download_url, archive_path, digest=str(asset.get("digest") or ""))
                cls._extract_archive(archive_path, extract_dir)
                extracted = cls._find_extracted_executable(extract_dir, "sing-box")
                if extracted is None:
                    raise RuntimeError("sing-box 压缩包已下载，但未找到可执行文件")
                shutil.copy2(extracted, install_path)
            if os.name != "nt":
                install_path.chmod(install_path.stat().st_mode | 0o755)
            logger.info("sing-box 已缓存到项目目录：%s", install_path)
            return install_path

    @classmethod
    def find_sing_box_executable(cls) -> Path:
        env_path = os.environ.get("SING_BOX_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return p
        for exe_name in cls._candidate_executable_names("sing-box"):
            which_path = shutil.which(exe_name)
            if which_path:
                return Path(which_path)
        for p in cls._candidate_sing_box_paths():
            if p.exists():
                return p
        try:
            return cls._ensure_sing_box_runtime()
        except Exception as exc:
            raise RuntimeError(f"未找到 sing-box 可执行文件，且项目内自动准备失败：{exc}") from exc

    @classmethod
    def find_xray_executable(cls) -> Path:
        env_path = os.environ.get("XRAY_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return p
        for exe_name in cls._candidate_executable_names("xray"):
            which_path = shutil.which(exe_name)
            if which_path:
                return Path(which_path)
        for p in cls._candidate_xray_paths():
            if p.exists():
                return p
        raise RuntimeError("未找到 xray 可执行文件，请先安装 xray 或放到项目 bin/ 目录")

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
        return self.work_dir / f"{self.runtime_name}_{prefix}_{os.getpid()}_{token}{suffix}"

    def _runtime_log_path(self) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        return self.work_dir / f"{self.runtime_name}_proxy_runtime_{os.getpid()}.log"

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
        config_path = self._runtime_file_path("ss_proxy", ".json")
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
        _atomic_write_json(config_path, cfg)
        return config_path

    def _write_xray_config(self, ss_info: dict[str, object], listen_port: int) -> Path:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        config_path = self._runtime_file_path("ss_proxy_xray", ".json")
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
        _atomic_write_json(config_path, cfg)
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
            backends = ("xray", "sing-box") if os.name == "nt" else ("sing-box", "xray")
            for backend in backends:
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
    try:
        session.trust_env = bool(allow_system_proxy) and not bool(proxies)
        if proxies:
            session.proxies.update(proxies)
        return session.get(url, timeout=timeout, headers=headers or {})
    finally:
        session.close()

# ====================== 日志 & 队列 ======================
log_queue = queue.Queue()
LOG_FILE_RUNTIME_PREFIX = "exchange_runtime"
LOG_FILE_RETENTION_COUNT = 20
LOG_FILE_TOTAL_SIZE_LIMIT_BYTES = 200 * 1024 * 1024

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


def _create_runtime_log_path() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{LOG_FILE_RUNTIME_PREFIX}_{timestamp}_{os.getpid()}.log"


def _prune_runtime_logs(current_path: Path | None = None) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return

    entries = []
    for path in LOG_DIR.glob(f"{LOG_FILE_RUNTIME_PREFIX}_*.log"):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append((path, stat.st_mtime, stat.st_size))

    entries.sort(key=lambda item: item[1], reverse=True)
    kept_count = 0
    kept_size = 0
    for path, _, size in entries:
        if current_path is not None and path == current_path:
            kept_count += 1
            kept_size += size
            continue
        if kept_count < LOG_FILE_RETENTION_COUNT and (kept_size + size) <= LOG_FILE_TOTAL_SIZE_LIMIT_BYTES:
            kept_count += 1
            kept_size += size
            continue
        try:
            path.unlink()
        except OSError:
            pass


_tk_handler = TkLogHandler()
_tk_handler.setFormatter(_formatter)
logger.addHandler(_tk_handler)


def _json_dump_text(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _read_text_snapshot(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _restore_text_snapshot(path: Path, snapshot: str | None) -> None:
    if snapshot is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        try:
            path.with_name(f"{path.name}{CONFIG_BACKUP_SUFFIX}").unlink()
        except FileNotFoundError:
            pass
        return
    _atomic_write_text_with_backup(path, snapshot, encoding="utf-8")


def _atomic_write_json(path: Path, payload: object) -> None:
    _atomic_write_text(path, _json_dump_text(payload), encoding="utf-8")


def _atomic_write_config_json(path: Path, payload: object) -> None:
    _atomic_write_text_with_backup(path, _json_dump_text(payload), encoding="utf-8")


def _require_dict_payload(raw: object) -> None:
    if isinstance(raw, dict):
        return
    raise RuntimeError("配置文件结构无效")


_runtime_log_path = None
try:
    _prune_runtime_logs()
    _runtime_log_path = _create_runtime_log_path()
    _runtime_file_handler = logging.FileHandler(_runtime_log_path, encoding="utf-8")
    _runtime_file_handler.setFormatter(_formatter)
    logger.addHandler(_runtime_file_handler)
    _prune_runtime_logs(_runtime_log_path)
except Exception as e:
    print(f"无法创建日志文件: {e}")

try:
    _compat_file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
    _compat_file_handler.setFormatter(_formatter)
    logger.addHandler(_compat_file_handler)
except Exception as e:
    print(f"无法创建兼容日志文件: {e}")

if _runtime_log_path is not None:
    logger.info("当前运行日志文件：%s", _runtime_log_path)


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
        trade_account_type: str = TRADE_ACCOUNT_TYPE_DEFAULT,
        trade_mode: str = TRADE_MODE_DEFAULT,
        premium_percent: Decimal | None = None,
        bnb_fee_stop_value: Decimal | None = None,
        bnb_topup_amount: Decimal | None = None,
        reprice_threshold_amount: Decimal | None = None,
        futures_symbol: str = FUTURES_SYMBOL_DEFAULT,
        futures_rounds: int = FUTURES_ROUNDS_DEFAULT,
        futures_amount: Decimal | None = None,
        futures_leverage: int = FUTURES_LEVERAGE_DEFAULT,
        futures_margin_type: str = FUTURES_MARGIN_TYPE_DEFAULT,
        futures_side: str = FUTURES_SIDE_DEFAULT,
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
        self.trade_account_type = str(trade_account_type or TRADE_ACCOUNT_TYPE_DEFAULT)
        self.trade_mode = str(trade_mode or TRADE_MODE_DEFAULT)
        self.premium_percent = Decimal(str(premium_percent if premium_percent is not None else "0"))
        self.bnb_fee_stop_value = Decimal(str(bnb_fee_stop_value if bnb_fee_stop_value is not None else "0"))
        self.bnb_topup_amount = Decimal(str(bnb_topup_amount if bnb_topup_amount is not None else "0"))
        self.reprice_threshold_amount = Decimal(
            str(reprice_threshold_amount if reprice_threshold_amount is not None else REPRICE_THRESHOLD_DEFAULT)
        )
        self.futures_symbol = str(futures_symbol or FUTURES_SYMBOL_DEFAULT).strip().upper()
        self.futures_rounds = max(1, int(futures_rounds or FUTURES_ROUNDS_DEFAULT))
        self.futures_amount = Decimal(str(futures_amount if futures_amount is not None else "0"))
        self.futures_leverage = max(1, int(futures_leverage or FUTURES_LEVERAGE_DEFAULT))
        self.futures_margin_type = App._normalize_futures_margin_type(futures_margin_type)
        self.futures_side = str(futures_side or FUTURES_SIDE_DEFAULT).strip()
        if self.futures_margin_type not in FUTURES_MARGIN_TYPE_OPTIONS:
            self.futures_margin_type = FUTURES_MARGIN_TYPE_DEFAULT
        if self.futures_side not in FUTURES_SIDE_OPTIONS:
            self.futures_side = FUTURES_SIDE_DEFAULT

    def ensure_base_sold(self):
        try:
            sold = self.c.spot_sell_all_base(self.spot_symbol, self.spot_precision)
            if sold:
                logger.info("【补救措施】检测到残留基础币，已执行补充卖出。")
        except Exception as e:
            logger.warning(f"补救卖出时发生错误（可忽略）: {e}")

    def _trade_account_type_name(self) -> str:
        mode = str(self.trade_account_type or TRADE_ACCOUNT_TYPE_DEFAULT)
        return mode if mode in TRADE_ACCOUNT_TYPE_OPTIONS else TRADE_ACCOUNT_TYPE_DEFAULT

    def _is_futures_mode(self) -> bool:
        return self._trade_account_type_name() == TRADE_ACCOUNT_TYPE_FUTURES

    def _futures_side_order(self) -> str:
        return "BUY" if self.futures_side == FUTURES_SIDE_LONG else "SELL"

    def _futures_margin_asset(self) -> str:
        return self.c.get_um_futures_margin_asset(self.futures_symbol)

    def ensure_futures_position_closed(self):
        try:
            close_orders = self.c.close_all_um_futures_positions_market(self.futures_symbol)
            if close_orders:
                logger.info("【补救措施】检测到残留合约持仓，已执行 %d 笔市价平仓。", len(close_orders))
                deadline = time.time() + 8
                while time.time() < deadline:
                    remaining_positions = [
                        position
                        for position in self.c.get_um_futures_positions(self.futures_symbol)
                        if Decimal(str(position.get("positionAmt", "0"))) != 0
                    ]
                    if not remaining_positions:
                        return
                    time.sleep(0.3)
                logger.warning("残留合约持仓平仓后仍未完全归零，后续设置可能被交易所拒绝")
        except Exception as e:
            logger.warning(f"补救平仓时发生错误（可忽略）: {e}")

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

    def _reprice_threshold_value(self) -> Decimal:
        threshold = Decimal(str(self.reprice_threshold_amount or "0"))
        return self.c.normalize_price_delta(self.spot_symbol, threshold, min_one_tick=True)

    def _reprice_threshold_log_text(self) -> str:
        quote_asset = self.c.get_spot_quote_asset(self.spot_symbol)
        threshold = self._reprice_threshold_value()
        return f"{BinanceClient._format_decimal(threshold)} {quote_asset}"

    def _run_bnb_topup_if_needed(self):
        if self._is_futures_mode():
            return False
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
        threshold_amount = self._reprice_threshold_value()
        if side_u == "BUY":
            current_ref = Decimal(str(book_ticker["bidPrice"]))
            trigger_price = price + threshold_amount
            return current_ref >= trigger_price, current_ref
        current_ref = Decimal(str(book_ticker["askPrice"]))
        trigger_price = price - threshold_amount
        return current_ref <= trigger_price, current_ref

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
                            "%s模式%s单价格偏离超过 %s，已撤单重挂：旧价=%s，当前参考价=%s",
                            mode_name,
                            "买" if side_u == "BUY" else "卖",
                            self._reprice_threshold_log_text(),
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

    def _cancel_limit_order_with_fill_guard(self, order_id: int | str, side: str) -> dict:
        symbol_u = self.spot_symbol.upper()
        latest_order = None
        try:
            latest_order = self.c.get_order(symbol_u, order_id)
        except Exception:
            latest_order = None

        if latest_order is not None:
            latest_status = str(latest_order.get("status") or "").upper()
            if latest_status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                return latest_order

        try:
            self.c.cancel_order(symbol_u, order_id)
        except Exception as cancel_exc:
            try:
                latest_order = self.c.get_order(symbol_u, order_id)
            except Exception:
                latest_order = None
            if latest_order is not None and str(latest_order.get("status") or "").upper() == "FILLED":
                return latest_order
            raise cancel_exc

        try:
            latest_order = self.c.get_order(symbol_u, order_id)
        except Exception:
            if latest_order is None:
                latest_order = {
                    "orderId": order_id,
                    "side": side,
                    "status": "CANCELED",
                }
            else:
                latest_order = dict(latest_order)
                latest_order["status"] = str(latest_order.get("status") or "CANCELED").upper()
        return latest_order

    def _limit_order_plan(self, book_ticker: dict[str, Decimal]) -> dict[str, Decimal | str]:
        symbol_u = self.spot_symbol.upper()
        quote_asset = self.c.get_spot_quote_asset(symbol_u)
        base_asset = self.c.get_spot_base_asset(symbol_u)
        self.c.collect_funding_asset_to_spot(quote_asset)

        bid_price = Decimal(str(book_ticker["bidPrice"]))
        ask_price = Decimal(str(book_ticker["askPrice"]))
        mid_price = (bid_price + ask_price) / Decimal("2")
        quote_balance = self.c.spot_asset_balance_decimal(quote_asset)
        base_balance = self.c.spot_asset_balance_decimal(base_asset)
        base_notional = base_balance * mid_price

        return {
            "quote_asset": quote_asset,
            "base_asset": base_asset,
            "quote_balance": quote_balance,
            "base_balance": base_balance,
            "base_notional": base_notional,
            "bid_price": bid_price,
            "ask_price": ask_price,
        }

    def _place_limit_orders(self, mode_name: str, book_ticker: dict[str, Decimal]) -> tuple[dict | None, dict | None]:
        plan = self._limit_order_plan(book_ticker)
        quote_asset = str(plan["quote_asset"])
        base_asset = str(plan["base_asset"])
        quote_balance = Decimal(str(plan["quote_balance"]))
        base_balance = Decimal(str(plan["base_balance"]))
        base_notional = Decimal(str(plan["base_notional"]))
        bid_price = Decimal(str(plan["bid_price"]))
        ask_price = Decimal(str(plan["ask_price"]))

        if quote_balance <= 0 and base_balance <= 0:
            logger.info("%s模式当前现货 %s 和 %s 余额均为 0，结束本次运行", mode_name, quote_asset, base_asset)
            return None, None

        buy_order = None
        sell_order = None
        if quote_balance > 0:
            buy_order = self.c.spot_limit_buy_quote_amount(self.spot_symbol, bid_price, quote_balance)
        if base_balance > 0:
            sell_order = self.c.spot_limit_sell_quantity(self.spot_symbol, ask_price, base_balance)

        buy_text = "未挂出"
        if buy_order:
            buy_qty = Decimal(str(buy_order.get("origQty", buy_order.get("executedQty", "0")) or "0"))
            buy_price = self._order_price_decimal(buy_order, bid_price)
            buy_text = f"{BinanceClient._format_decimal(buy_qty)} @ {BinanceClient._format_decimal(buy_price)}"

        sell_text = "未挂出"
        if sell_order:
            sell_qty = Decimal(str(sell_order.get("origQty", sell_order.get("executedQty", "0")) or "0"))
            sell_price = self._order_price_decimal(sell_order, ask_price)
            sell_text = f"{BinanceClient._format_decimal(sell_qty)} @ {BinanceClient._format_decimal(sell_price)}"

        logger.info(
            "%s模式双边挂单：%s=%s | %s=%s(约 %s %s) | 买1=%s | 卖1=%s",
            mode_name,
            quote_asset,
            BinanceClient._format_decimal(quote_balance),
            base_asset,
            BinanceClient._format_decimal(base_balance),
            BinanceClient._format_decimal(base_notional),
            quote_asset,
            buy_text,
            sell_text,
        )
        return buy_order, sell_order

    def _monitor_limit_orders(
        self,
        stop_event,
        mode_name: str,
        *,
        buy_order: dict | None,
        sell_order: dict | None,
        poll_interval: float = 1.0,
    ) -> tuple[str, dict[str, object]]:
        symbol_u = self.spot_symbol.upper()
        active_orders: dict[str, dict[str, object]] = {}
        if buy_order and buy_order.get("orderId"):
            active_orders["BUY"] = {
                "order_id": buy_order.get("orderId"),
                "price": self._order_price_decimal(buy_order, Decimal("0")),
            }
        if sell_order and sell_order.get("orderId"):
            active_orders["SELL"] = {
                "order_id": sell_order.get("orderId"),
                "price": self._order_price_decimal(sell_order, Decimal("0")),
            }

        if not active_orders:
            return "idle", {"filled_orders": []}

        while True:
            if stop_event and stop_event.is_set():
                for side_u, state in list(active_orders.items()):
                    try:
                        self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    except Exception:
                        pass
                raise RuntimeError("收到停止信号，已停止等待挂单成交")

            book_ticker = self.c.get_book_ticker(symbol_u)
            filled_orders: list[tuple[str, dict]] = []
            reprice_triggered = False

            for side_u, state in list(active_orders.items()):
                order = self.c.get_order(symbol_u, state["order_id"])
                status = str(order.get("status") or "").upper()
                if status == "FILLED":
                    filled_orders.append((side_u, order))
                    active_orders.pop(side_u, None)
                    continue
                if status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                    active_orders.pop(side_u, None)
                    continue

                order_price = self._order_price_decimal(order, Decimal(str(state["price"])))
                state["price"] = order_price
                should_reprice, current_ref = self._should_reprice_open_order(side_u, order_price, book_ticker)
                if not should_reprice:
                    continue

                latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                latest_status = str(latest_order.get("status") or "").upper()
                if latest_status == "FILLED":
                    filled_orders.append((side_u, latest_order))
                else:
                    logger.info(
                        "%s模式%s单价格偏离超过 %s，已撤单准备按最新余额重挂：旧价=%s，当前参考价=%s",
                        mode_name,
                        "买" if side_u == "BUY" else "卖",
                        self._reprice_threshold_log_text(),
                        BinanceClient._format_decimal(order_price),
                        BinanceClient._format_decimal(current_ref),
                    )
                active_orders.pop(side_u, None)
                reprice_triggered = True

            if filled_orders:
                for side_u, state in list(active_orders.items()):
                    latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    if str(latest_order.get("status") or "").upper() == "FILLED":
                        filled_orders.append((side_u, latest_order))
                    active_orders.pop(side_u, None)
                return "filled", {"filled_orders": filled_orders}

            if reprice_triggered:
                for side_u, state in list(active_orders.items()):
                    latest_order = self._cancel_limit_order_with_fill_guard(state["order_id"], side_u)
                    if str(latest_order.get("status") or "").upper() == "FILLED":
                        filled_orders.append((side_u, latest_order))
                    active_orders.pop(side_u, None)
                if filled_orders:
                    return "filled", {"filled_orders": filled_orders}
                return "reprice", {"book_ticker": book_ticker}

            if not active_orders:
                return "idle", {"filled_orders": []}

            if stop_event and stop_event.wait(max(0.2, float(poll_interval))):
                continue
            if not stop_event:
                time.sleep(max(0.2, float(poll_interval)))

    def _limit_fill_summary(self, filled_orders: list[tuple[str, dict]]) -> str:
        if not filled_orders:
            return "本轮无成交"

        parts: list[str] = []
        for side_u, order in filled_orders:
            side_text = "买单" if side_u == "BUY" else "卖单"
            qty = Decimal(str(order.get("executedQty", order.get("origQty", "0")) or "0"))
            avg_price = self.c.get_order_average_price(order) or self._order_price_decimal(order, Decimal("0"))
            text = f"{side_text}成交 数量={BinanceClient._format_decimal(qty)}"
            if avg_price > 0:
                text += f" 均价={BinanceClient._format_decimal(avg_price)}"
            parts.append(text)
        return "；".join(parts)

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

    def _run_limit_mode(self, stop_event, progress_cb=None):
        step = 0
        mode_name = self._mode_name()

        while True:
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                return
            if self._should_stop_for_bnb_fee():
                return

            try:
                book_ticker = self.c.get_book_ticker(self.spot_symbol)
                buy_order, sell_order = self._place_limit_orders(mode_name, book_ticker)
                if not buy_order and not sell_order:
                    logger.info("%s模式当前无可挂出的买卖单（余额不足或不满足最小下单额），结束本次运行", mode_name)
                    return

                action, payload = self._monitor_limit_orders(
                    stop_event,
                    mode_name,
                    buy_order=buy_order,
                    sell_order=sell_order,
                )
                if action == "filled":
                    step += 1
                    filled_summary = self._limit_fill_summary(list(payload.get("filled_orders") or []))
                    logger.info("--- %s轮 %d 完成：%s ---", mode_name, step, filled_summary)
                    if progress_cb:
                        progress_cb(step, max(step, 1), f"{mode_name}轮 {step}")
                    self.sleep_fn()
                    continue
                if action == "reprice":
                    continue

                logger.info("%s模式当前无活动挂单，结束本次运行", mode_name)
                return
            except Exception as e:
                if stop_event and stop_event.is_set():
                    logger.info("检测到停止信号，停止后续执行（%s模式）", mode_name)
                    return
                logger.error("%s轮 %d 执行异常: %s", mode_name, step, e)
                time.sleep(3)

    def _run_premium_mode(self, stop_event, progress_cb=None):
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
                buy_result, _ask_price = self._place_buy_order_with_reprice(stop_event, mode_name)
                buy_order = buy_result
                if not buy_order:
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

    def _run_limit_like_mode(self, stop_event, progress_cb=None):
        if self._mode_name() == TRADE_MODE_LIMIT:
            self._run_limit_mode(stop_event, progress_cb=progress_cb)
            return
        self._run_premium_mode(stop_event, progress_cb=progress_cb)

    def _log_futures_symbol_settings(self):
        current_dual_mode = self.c.get_um_futures_position_mode()
        logger.info("U本位合约当前持仓模式：%s", "双向仓" if current_dual_mode else "单向仓")

        symbol_config = self.c.get_um_futures_symbol_config(self.futures_symbol) or {}
        logger.info(
            "U本位合约 %s 当前配置：杠杆=%s，保证金模式=%s",
            self.futures_symbol,
            symbol_config.get("leverage", "--"),
            symbol_config.get("marginType", "--"),
        )

    def _prepare_futures_symbol_settings(self):
        self.c.ensure_um_futures_one_way_mode()
        self.c.ensure_um_futures_margin_type(self.futures_symbol, self.futures_margin_type)
        final_leverage = self.c.ensure_um_futures_leverage(self.futures_symbol, self.futures_leverage)
        self.futures_leverage = int(final_leverage)

    def _ensure_futures_available_balance(self):
        margin_asset = self._futures_margin_asset()
        balance = self.c.um_futures_asset_balance(margin_asset)
        available_balance = Decimal(str(balance.get("availableBalance", "0")))
        required_margin = Decimal("0")
        if self.futures_leverage > 0:
            required_margin = self.futures_amount / Decimal(str(self.futures_leverage))
        if required_margin > 0 and available_balance < required_margin:
            missing_margin = required_margin - available_balance
            moved_amount = self.c.transfer_spot_asset_to_um_futures(margin_asset, missing_margin)
            if moved_amount > 0:
                balance = self.c.um_futures_asset_balance(margin_asset)
                available_balance = Decimal(str(balance.get("availableBalance", "0")))
                logger.info(
                    "U本位合约 %s 已自动补划保证金 %s=%s，当前可用=%s",
                    self.futures_symbol,
                    margin_asset,
                    BinanceClient._format_decimal(moved_amount),
                    BinanceClient._format_decimal(available_balance),
                )
        logger.info(
            "U本位合约 %s 可用保证金 %s=%s，预计占用保证金=%s，下单金额=%s，杠杆=%s",
            self.futures_symbol,
            margin_asset,
            BinanceClient._format_decimal(available_balance),
            BinanceClient._format_decimal(required_margin),
            BinanceClient._format_decimal(self.futures_amount),
            self.futures_leverage,
        )
        if required_margin > 0 and available_balance < required_margin:
            raise RuntimeError(
                f"U本位合约 {margin_asset} 可用保证金不足：需要至少 {BinanceClient._format_decimal(required_margin)}，当前 {BinanceClient._format_decimal(available_balance)}"
            )
        return margin_asset, available_balance

    def _run_futures_mode(self, stop_event, progress_cb=None):
        total_steps = self.futures_rounds if self.futures_rounds > 0 else 1
        step = 0
        open_side = self._futures_side_order()
        side_text = self.futures_side if self.futures_side in FUTURES_SIDE_OPTIONS else FUTURES_SIDE_DEFAULT

        self.ensure_futures_position_closed()
        self._log_futures_symbol_settings()
        preview_qty = self.c.calculate_um_futures_order_quantity(
            self.futures_symbol,
            self.futures_amount,
            open_side,
        )
        self._ensure_futures_available_balance()
        logger.info(
            "【合约预检通过】%s %s，单轮预计下单数量=%s，下单金额=%s",
            self.futures_symbol,
            side_text,
            BinanceClient._format_decimal(preview_qty),
            BinanceClient._format_decimal(self.futures_amount),
        )
        self._prepare_futures_symbol_settings()

        for i in range(self.futures_rounds):
            if stop_event and stop_event.is_set():
                logger.info("检测到停止信号，停止后续执行（合约阶段）")
                return

            self.ensure_futures_position_closed()
            logger.info("--- 合约轮 %d/%d 开始：%s %s ---", i + 1, self.futures_rounds, self.futures_symbol, side_text)

            try:
                stage_label = "开仓前预检"
                qty = self.c.calculate_um_futures_order_quantity(
                    self.futures_symbol,
                    self.futures_amount,
                    open_side,
                )
                stage_label = "开仓下单"
                open_order = self.c.place_um_futures_market_order(
                    self.futures_symbol,
                    open_side,
                    qty,
                )
                try:
                    avg_price = Decimal(str(open_order.get("avgPrice", "0")))
                except Exception:
                    avg_price = Decimal("0")
                logger.info(
                    "【开仓成功】%s %s，数量=%s，均价=%s",
                    self.futures_symbol,
                    side_text,
                    BinanceClient._format_decimal(qty),
                    BinanceClient._format_decimal(avg_price),
                )

                self.sleep_fn()
                stage_label = "平仓下单"
                close_order = self.c.close_um_futures_position_market(self.futures_symbol)
                if not close_order:
                    raise RuntimeError("合约平仓未执行：未检测到可平持仓")
                try:
                    close_price = Decimal(str(close_order.get("avgPrice", "0")))
                except Exception:
                    close_price = Decimal("0")
                logger.info(
                    "【平仓成功】%s %s，均价=%s",
                    self.futures_symbol,
                    side_text,
                    BinanceClient._format_decimal(close_price),
                )
            except Exception as e:
                logger.error("【%s失败】合约轮 %d：%s", stage_label, i + 1, e)
                time.sleep(3)
            finally:
                self.ensure_futures_position_closed()

            step += 1
            if progress_cb:
                progress_cb(step, total_steps, "合约轮 %d/%d" % (i + 1, self.futures_rounds))
            self.sleep_fn()

        self.ensure_futures_position_closed()

    def run(self, stop_event, progress_cb=None):
        withdraw_amount = 0.0
        withdraw_error = ""
        withdraw_attempted = False

        self._run_bnb_topup_if_needed()

        if self._is_futures_mode():
            self._run_futures_mode(stop_event, progress_cb=progress_cb)
        elif self._mode_name() == TRADE_MODE_MARKET:
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
                auto_collect_to_spot=self._is_futures_mode(),
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
class CombinedStopEvent:
    def __init__(self, *events):
        self._events = [event for event in events if event is not None]

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)

    def wait(self, timeout=None) -> bool:
        if self.is_set():
            return True
        if timeout is None:
            while not self.is_set():
                time.sleep(0.1)
            return True

        end_time = time.time() + max(0.0, float(timeout))
        while True:
            if self.is_set():
                return True
            remaining = end_time - time.time()
            if remaining <= 0:
                return self.is_set()
            step = min(0.2, remaining)
            if self._events:
                self._events[0].wait(step)
            else:
                time.sleep(step)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Binance 自动交易机器人（增强版 GUI）")
        self.geometry("1320x920")

        self.client = None
        self.worker_thread = None
        self.stop_event = None
        self._batch_task_active = False
        self._closing = False
        self._close_finalized = False
        self._close_deadline_monotonic = 0.0
        self._close_wait_after_token = None
        self._log_poll_after_token = None
        self._update_ip_after_token = None
        self._ip_refresh_inflight = False
        self._ip_refresh_lock = threading.Lock()
        self._result_file_lock = threading.Lock()
        self._managed_threads_lock = threading.Lock()
        self._managed_threads: set[threading.Thread] = set()
        self.exchange_proxy_runtime = ExchangeProxyRuntime(STRATEGY_CONFIG_FILE.parent, runtime_name="exchange")
        self.onchain_proxy_runtime = ExchangeProxyRuntime(STRATEGY_CONFIG_FILE.parent, runtime_name="onchain")

        self.accounts = []
        self.total_asset_results = {}

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build_ui()
        start_ui_bridge(self, root=self)
        self._load_strategy_config()
        self._load_exchange_proxy_config()
        self._log_poll_after_token = self.after(100, self._poll_log_queue)
        self.update_ip()

    def _build_ui(self):
        self.api_key_var = tk.StringVar(value=API_KEY_DEFAULT)
        self.api_secret_var = tk.StringVar(value=API_SECRET_DEFAULT)
        self.exchange_proxy_var = tk.StringVar(value=EXCHANGE_PROXY_DEFAULT)
        self.use_exchange_config_proxy_var = tk.BooleanVar(value=EXCHANGE_USE_CONFIG_PROXY_DEFAULT)
        self.trade_account_type_var = tk.StringVar(value=TRADE_ACCOUNT_TYPE_DEFAULT)
        self.spot_rounds_var = tk.IntVar(value=SPOT_ROUNDS_DEFAULT)
        self.trade_mode_var = tk.StringVar(value=TRADE_MODE_DEFAULT)
        self.premium_percent_var = tk.StringVar(value=PREMIUM_PERCENT_DEFAULT)
        self.bnb_fee_stop_var = tk.StringVar(value=BNB_FEE_STOP_DEFAULT)
        self.bnb_topup_amount_var = tk.StringVar(value=BNB_TOPUP_AMOUNT_DEFAULT)
        self.reprice_threshold_var = tk.StringVar(value=REPRICE_THRESHOLD_DEFAULT)
        self.spot_symbol_var = tk.StringVar(value=SPOT_SYMBOL_DEFAULT)
        self.spot_precision_var = tk.IntVar(value=SPOT_PRECISION_DEFAULT)
        self.futures_symbol_var = tk.StringVar(value=FUTURES_SYMBOL_DEFAULT)
        self.futures_rounds_var = tk.IntVar(value=FUTURES_ROUNDS_DEFAULT)
        self.futures_amount_var = tk.StringVar(value=FUTURES_AMOUNT_DEFAULT)
        self.futures_leverage_var = tk.IntVar(value=FUTURES_LEVERAGE_DEFAULT)
        self.futures_margin_type_var = tk.StringVar(value=FUTURES_MARGIN_TYPE_LABEL_CROSSED)
        self.futures_side_var = tk.StringVar(value=FUTURES_SIDE_DEFAULT)

        self.withdraw_addr_var = tk.StringVar(value=WITHDRAW_ADDRESS_DEFAULT)
        self.withdraw_net_var = tk.StringVar(value=WITHDRAW_NETWORK_DEFAULT)
        self.withdraw_coin_var = tk.StringVar(value=WITHDRAW_COIN_DEFAULT)
        self.withdraw_buffer_var = tk.DoubleVar(value=WITHDRAW_FEE_BUFFER_DEFAULT)
        self.enable_withdraw_var = tk.BooleanVar(value=True)

        self.min_delay_var = tk.StringVar(value="")
        self.max_delay_var = tk.StringVar(value="")
        self.usdt_timeout_var = tk.IntVar(value=30)
        self.ip_var = tk.StringVar(value="获取中...")
        self.exchange_proxy_status_var = tk.StringVar(value="未启用")
        self.exchange_proxy_exit_ip_var = tk.StringVar(value="--")
        self.top_proxy_name_var = tk.StringVar(value="交易所代理:")
        self.top_proxy_test_btn_text_var = tk.StringVar(value="测试交易所代理")
        self._current_main_page = "exchange"

        self.main_tabs = None

        top_bar = ttk.Frame(self)
        top_bar.pack(fill="x", padx=8, pady=(8, 0))
        top_bar.columnconfigure(1, weight=1)

        tab_bar = ttk.Frame(top_bar)
        tab_bar.grid(row=0, column=0, sticky="w")
        self.btn_exchange_tab = tk.Button(
            tab_bar,
            text="交易所批量",
            command=lambda: self._show_main_page("exchange"),
            bd=1,
            relief="sunken",
            padx=14,
            pady=5,
            highlightthickness=0,
            cursor="hand2",
        )
        self.btn_exchange_tab.pack(side="left")
        self.btn_onchain_tab = tk.Button(
            tab_bar,
            text="链上",
            command=lambda: self._show_main_page("onchain"),
            bd=1,
            relief="raised",
            padx=14,
            pady=5,
            highlightthickness=0,
            cursor="hand2",
        )
        self.btn_onchain_tab.pack(side="left", padx=(6, 0))

        proxy_bar = ttk.Frame(top_bar)
        proxy_bar.grid(row=0, column=1, sticky="e")
        ttk.Label(proxy_bar, text="本机直连 IP:").grid(row=0, column=0, sticky="e")
        ttk.Label(proxy_bar, textvariable=self.ip_var).grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.lbl_top_proxy_name = ttk.Label(proxy_bar, textvariable=self.top_proxy_name_var)
        self.lbl_top_proxy_name.grid(row=0, column=2, sticky="e")
        self.ent_top_proxy = ttk.Entry(proxy_bar, textvariable=self.exchange_proxy_var, width=24)
        self.ent_top_proxy.grid(row=0, column=3, sticky="w", padx=(4, 6))
        self.btn_top_proxy_test = ttk.Button(proxy_bar, textvariable=self.top_proxy_test_btn_text_var, command=self.test_exchange_proxy)
        self.btn_top_proxy_test.grid(row=0, column=4, sticky="w", padx=(0, 12))
        ttk.Label(proxy_bar, text="状态:").grid(row=0, column=5, sticky="e")
        self.lbl_top_proxy_status = ttk.Label(proxy_bar, textvariable=self.exchange_proxy_status_var)
        self.lbl_top_proxy_status.grid(row=0, column=6, sticky="w", padx=(4, 12))
        ttk.Label(proxy_bar, text="出口 IP:").grid(row=0, column=7, sticky="e")
        self.lbl_top_proxy_exit_ip = ttk.Label(proxy_bar, textvariable=self.exchange_proxy_exit_ip_var)
        self.lbl_top_proxy_exit_ip.grid(row=0, column=8, sticky="w", padx=(4, 0))

        self.main_content = ttk.Frame(self)
        self.main_content.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.exchange_tab = ttk.Frame(self.main_content)
        self.onchain_tab = ttk.Frame(self.main_content)
        self._refresh_main_page_tab_buttons()
        self._show_main_page(self._current_main_page)

        frame_top = ttk.LabelFrame(self.exchange_tab, text="策略配置（单账号 & 批量共享）")
        frame_top.pack(fill="x", padx=10, pady=5)

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

        self.frame_list_canvas = ttk.Frame(frame_acc)
        self.frame_list_canvas.pack(fill="both", expand=True, padx=5, pady=2)
        self.frame_list_canvas.columnconfigure(0, weight=1)
        self.frame_list_canvas.rowconfigure(0, weight=1)

        tree_cols = ("checked", "idx", "api_key", "address", "network", "status")
        self.account_tree = ttk.Treeview(
            self.frame_list_canvas,
            columns=tree_cols,
            show="headings",
            selectmode="browse",
            height=9,
        )
        self._account_tree_row_to_account = {}
        self.account_tree.heading("checked", text="勾选")
        self.account_tree.heading("idx", text="编号")
        self.account_tree.heading("api_key", text="API KEY")
        self.account_tree.heading("address", text="提现地址")
        self.account_tree.heading("network", text="网络")
        self.account_tree.heading("status", text="状态")
        self.account_tree.column("checked", width=52, minwidth=52, stretch=False, anchor="center")
        self.account_tree.column("idx", width=52, minwidth=52, stretch=False, anchor="center")
        self.account_tree.column("api_key", width=220, minwidth=180, anchor="w")
        self.account_tree.column("address", width=330, minwidth=260, anchor="w")
        self.account_tree.column("network", width=76, minwidth=68, stretch=False, anchor="center")
        self.account_tree.column("status", width=500, minwidth=240, anchor="w")
        self.account_tree.tag_configure("acc_ready", foreground="#111111", background="#F2F2F2")
        self.account_tree.tag_configure("acc_running", foreground="#111111", background="#CFE3FF")
        self.account_tree.tag_configure("acc_warn", foreground="#111111", background="#FFE3B8")
        self.account_tree.tag_configure("acc_failed", foreground="#111111", background="#F8C7C7")
        self.account_tree.tag_configure("acc_success", foreground="#111111", background="#CFEECF")
        self.account_tree.tag_configure("acc_context", foreground="#FFFFFF", background="#7A3FF2")

        self.account_tree_ybar = make_scrollbar(self.frame_list_canvas, orient="vertical", command=self.account_tree.yview)
        self.account_tree_xbar = make_scrollbar(self.frame_list_canvas, orient="horizontal", command=self.account_tree.xview)
        self.account_tree.configure(yscrollcommand=self.account_tree_ybar.set, xscrollcommand=self.account_tree_xbar.set)
        self.account_tree.grid(row=0, column=0, sticky="nsew")
        self.account_tree_ybar.grid(row=0, column=1, sticky="ns")
        self.account_tree_xbar.grid(row=1, column=0, sticky="ew")
        self.account_list_hint = ttk.Label(
            self.frame_list_canvas,
            text="账号列表为空。点击此区域后可直接 Ctrl+V / Cmd+V 粘贴导入账号。\n导入格式：每 3 段一组，依次为 API KEY / SECRET / 提现地址。",
            foreground="#666",
            justify="center",
            anchor="center",
        )
        self.account_tree.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.account_tree.bind("<Double-Button-1>", self._on_account_tree_double_click, add="+")
        self.account_tree.bind("<Button-2>", self._on_account_tree_right_click, add="+")
        self.account_tree.bind("<Button-3>", self._on_account_tree_right_click, add="+")
        self.account_tree.bind("<Control-Button-1>", self._on_account_tree_right_click, add="+")
        self.frame_list_canvas.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self.account_list_hint.bind("<Button-1>", self._focus_account_list_for_paste, add="+")
        self._refresh_account_list_hint()
        self.account_row_menu = tk.Menu(self, tearoff=0)
        self.account_row_menu.add_command(label="查询", command=self.run_context_account_query)
        self.account_row_menu.add_command(label="执行", command=self.run_context_account_execute)
        self.account_row_menu.add_command(label="停止", command=self.run_context_account_stop)
        self.account_row_menu.add_command(label="提现", command=self.run_context_account_withdraw)
        self.account_row_menu.add_command(label="归集BNB", command=self.run_context_account_collect_bnb)
        self._context_account = None
        self._setup_account_list_mousewheel_bindings()

        frame_batch_ctrl = ttk.Frame(frame_acc)
        frame_batch_ctrl.pack(fill="x", padx=5, pady=5)

        self.btn_toggle_select_accounts = ttk.Button(frame_batch_ctrl, text="全选", width=8, command=self.toggle_select_all_accounts)
        self.btn_toggle_select_accounts.pack(side="left", padx=(0, 5))
        self.btn_run_accounts = SolidButton(
            frame_batch_ctrl,
            text="批量执行",
            command=self.run_selected_accounts,
            bg="#1E8449",
            fg="#FFFFFF",
            activebackground="#186A3B",
            activeforeground="#FFFFFF",
            disabledforeground="#E8F5E9",
            relief="flat",
            padx=12,
            pady=2,
        )
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
        self._current_batch_summary = None
        self._last_batch_retry = None
        self._batch_summary_lock = threading.Lock()
        self.exchange_batch_summary_var = tk.StringVar(value="结果汇总：成功0 | 失败0 | 提现总额=- | 余额总额=-")

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
        self.btn_retry_failed_accounts = ttk.Button(
            frame_batch_opts,
            text="失败重试",
            command=self.retry_last_failed_batch_operation,
            state="disabled",
        )
        self.btn_retry_failed_accounts.pack(side="left", padx=(8, 0))
        ttk.Label(frame_batch_opts, textvariable=self.exchange_batch_summary_var, foreground="#666666").pack(side="left", padx=(12, 0))

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
                self.onchain_page = OnchainTransferPage(
                    onchain_body,
                    rpc_proxy_getter=self._get_onchain_proxy_url,
                    proxy_text_normalizer=self._normalize_proxy_text,
                )
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
        self._refresh_top_proxy_binding()

        # 快捷键：Ctrl+V / Cmd+V 直接触发“从剪贴板导入账号”
        self.bind_all("<Control-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Control-V>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-v>", self._on_paste_shortcut, add="+")
        self.bind_all("<Command-V>", self._on_paste_shortcut, add="+")

    def _refresh_main_page_tab_buttons(self):
        tabs = (
            (self.btn_exchange_tab, "exchange"),
            (self.btn_onchain_tab, "onchain"),
        )
        for btn, page_name in tabs:
            is_active = self._current_main_page == page_name
            btn.configure(
                relief="sunken" if is_active else "raised",
                bg="#ffffff" if is_active else "#e9e9e9",
                fg="#111111" if is_active else "#555555",
                activebackground="#ffffff" if is_active else "#f1f1f1",
                activeforeground="#111111",
            )

    def _refresh_top_proxy_binding(self):
        page = self._current_main_page
        if page == "onchain" and getattr(self, "onchain_page", None) is not None:
            self.top_proxy_name_var.set("链上RPC代理:")
            self.top_proxy_test_btn_text_var.set("测试链上代理")
            self.ent_top_proxy.configure(textvariable=self.onchain_page.onchain_proxy_var)
            self.btn_top_proxy_test.configure(command=self.onchain_page.test_onchain_proxy, state="normal")
            self.lbl_top_proxy_status.configure(textvariable=self.onchain_page.onchain_proxy_status_var)
            self.lbl_top_proxy_exit_ip.configure(textvariable=self.onchain_page.onchain_proxy_exit_ip_var)
            return
        self.top_proxy_name_var.set("交易所代理:")
        self.top_proxy_test_btn_text_var.set("测试交易所代理")
        self.ent_top_proxy.configure(textvariable=self.exchange_proxy_var)
        self.btn_top_proxy_test.configure(command=self.test_exchange_proxy, state="normal")
        self.lbl_top_proxy_status.configure(textvariable=self.exchange_proxy_status_var)
        self.lbl_top_proxy_exit_ip.configure(textvariable=self.exchange_proxy_exit_ip_var)

    def _show_main_page(self, page_name: str):
        target = self.exchange_tab if page_name == "exchange" else self.onchain_tab
        if self._current_main_page != page_name:
            self._current_main_page = page_name
        try:
            self.exchange_tab.pack_forget()
        except Exception:
            pass
        try:
            self.onchain_tab.pack_forget()
        except Exception:
            pass
        target.pack(fill="both", expand=True)
        self._refresh_main_page_tab_buttons()
        self._refresh_top_proxy_binding()

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
    def _normalize_trade_account_type(value) -> str:
        text = str(value or "").strip()
        return text if text in TRADE_ACCOUNT_TYPE_OPTIONS else TRADE_ACCOUNT_TYPE_DEFAULT

    @staticmethod
    def _normalize_trade_mode(value) -> str:
        text = str(value or "").strip()
        return text if text in TRADE_MODE_OPTIONS else TRADE_MODE_DEFAULT

    @staticmethod
    def _normalize_futures_margin_type(value) -> str:
        text = str(value or "").strip()
        if not text:
            return FUTURES_MARGIN_TYPE_DEFAULT
        upper_text = text.upper()
        if upper_text in FUTURES_MARGIN_TYPE_OPTIONS:
            return upper_text
        return FUTURES_MARGIN_TYPE_LABEL_TO_VALUE.get(text, FUTURES_MARGIN_TYPE_DEFAULT)

    @staticmethod
    def _futures_margin_type_label(value) -> str:
        normalized = App._normalize_futures_margin_type(value)
        return FUTURES_MARGIN_TYPE_VALUE_TO_LABEL.get(normalized, FUTURES_MARGIN_TYPE_LABEL_CROSSED)

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
    def _reprice_threshold_label_text() -> str:
        return "重新挂单阈值(后置币):"

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
        trade_account_type = self._normalize_trade_account_type(self.trade_account_type_var.get())
        mode = self._normalize_trade_mode(self.trade_mode_var.get())
        try:
            stored_rounds = int(self.spot_rounds_var.get())
        except Exception:
            stored_rounds = SPOT_ROUNDS_DEFAULT
        try:
            stored_futures_rounds = int(self.futures_rounds_var.get())
        except Exception:
            stored_futures_rounds = FUTURES_ROUNDS_DEFAULT

        premium_text = str(self.premium_percent_var.get() or "").strip()
        fee_stop_text = str(self.bnb_fee_stop_var.get() or "").strip()
        bnb_topup_text = str(self.bnb_topup_amount_var.get() or "").strip()
        reprice_threshold_text = str(self.reprice_threshold_var.get() or "").strip()
        futures_symbol = str(self.futures_symbol_var.get() or "").strip().upper()
        futures_amount_text = str(self.futures_amount_var.get() or "").strip()
        futures_margin_type = self._normalize_futures_margin_type(self.futures_margin_type_var.get())
        futures_side = str(self.futures_side_var.get() or FUTURES_SIDE_DEFAULT).strip()
        premium_value: Decimal | None = None
        fee_stop_value: Decimal | None = None
        bnb_topup_value = Decimal("0")
        reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
        futures_rounds = stored_futures_rounds if stored_futures_rounds > 0 else FUTURES_ROUNDS_DEFAULT
        futures_amount_value: Decimal | None = None
        try:
            futures_leverage = int(self.futures_leverage_var.get())
        except Exception:
            futures_leverage = FUTURES_LEVERAGE_DEFAULT

        if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            if not bnb_topup_text:
                bnb_topup_text = "0"
            if not reprice_threshold_text:
                reprice_threshold_text = REPRICE_THRESHOLD_DEFAULT
            bnb_topup_value = self._decimal_field_value(bnb_topup_text, "预买BNB金额", min_value=0)
            reprice_threshold_value = self._decimal_field_value(reprice_threshold_text, "重新挂单阈值", min_value=0)

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
        else:
            runtime_rounds = stored_rounds if stored_rounds > 0 else SPOT_ROUNDS_DEFAULT
            try:
                futures_rounds = int(self.futures_rounds_var.get())
            except Exception as exc:
                raise RuntimeError("合约模式下必须填写轮次") from exc
            if futures_rounds < 1:
                raise RuntimeError("合约轮次必须大于等于 1")
            if not futures_symbol:
                raise RuntimeError("合约交易对不能为空")
            futures_amount_value = self._decimal_field_value(futures_amount_text, "合约下单金额", min_value=0)
            if futures_amount_value <= 0:
                raise RuntimeError("合约下单金额必须大于 0")
            try:
                futures_leverage = int(self.futures_leverage_var.get())
            except Exception as exc:
                raise RuntimeError("合约杠杆格式不正确") from exc
            if futures_leverage < 1 or futures_leverage > 125:
                raise RuntimeError("合约杠杆必须在 1-125 之间")
            futures_margin_type = self._normalize_futures_margin_type(futures_margin_type)
            if futures_side not in FUTURES_SIDE_OPTIONS:
                futures_side = FUTURES_SIDE_DEFAULT

        return {
            "trade_account_type": trade_account_type,
            "trade_mode": mode,
            "spot_rounds": runtime_rounds,
            "stored_spot_rounds": stored_rounds,
            "premium_percent": premium_text,
            "premium_percent_value": premium_value,
            "bnb_fee_stop": fee_stop_text,
            "bnb_fee_stop_value": fee_stop_value,
            "bnb_topup_amount": bnb_topup_text,
            "bnb_topup_amount_value": bnb_topup_value,
            "reprice_threshold": reprice_threshold_text,
            "reprice_threshold_value": reprice_threshold_value,
            "futures_symbol": futures_symbol,
            "futures_rounds": futures_rounds,
            "stored_futures_rounds": stored_futures_rounds,
            "futures_amount": futures_amount_text,
            "futures_amount_value": futures_amount_value,
            "futures_leverage": futures_leverage,
            "futures_margin_type": futures_margin_type,
            "futures_side": futures_side,
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

        ttk.Label(row1, text="\u968f\u673a\u5ef6\u8fdf(\u6beb\u79d2):").grid(row=0, column=0, sticky="e")
        delay_wrap = ttk.Frame(row1)
        delay_wrap.grid(row=0, column=1, sticky="w", padx=(4, 8))
        self.min_delay_entry = tk.Entry(delay_wrap, textvariable=self.min_delay_var, width=8)
        self.min_delay_entry.pack(side="left")
        self.max_delay_entry = tk.Entry(delay_wrap, textvariable=self.max_delay_var, width=8)
        self.max_delay_entry.pack(side="left", padx=(6, 0))
        self._install_entry_placeholder(self.min_delay_entry, self.min_delay_var, "\u6700\u5c0f")
        self._install_entry_placeholder(self.max_delay_entry, self.max_delay_var, "\u6700\u5927")

        ttk.Label(row1, text="USDT \u5230\u8d26\u8d85\u65f6(\u79d2):").grid(row=0, column=2, sticky="e", padx=(12, 0))
        ttk.Entry(row1, textvariable=self.usdt_timeout_var, width=8).grid(row=0, column=3, sticky="w", padx=(4, 12))
        ttk.Checkbutton(row1, text="使用配置的代理", variable=self.use_exchange_config_proxy_var).grid(row=0, column=4, sticky="w", padx=(0, 12))

        self.btn_save_strategy_config = ttk.Button(row1, text="\u4fdd\u5b58\u914d\u7f6e", command=self.save_strategy_config)
        self.btn_save_strategy_config.grid(row=0, column=5, sticky="w")

        row2 = ttk.Frame(frame_top)
        row2.pack(fill="x", padx=5, pady=3)
        self._strategy_row2 = row2
        current_trade_account_type = self._normalize_trade_account_type(self.trade_account_type_var.get())
        current_trade_mode = self._normalize_trade_mode(self.trade_mode_var.get())
        row2_left = ttk.Frame(row2)
        row2_left.grid(row=0, column=0, sticky="w")
        self._strategy_row2_left = row2_left
        row2_right = ttk.Frame(row2)
        row2_right.grid(row=0, column=1, sticky="w")
        row2.grid_columnconfigure(2, weight=1)
        self.trade_mode_combo = None
        self.trade_account_type_combo = None
        self.futures_margin_type_combo = None
        self.futures_side_combo = None

        ttk.Label(row2_right, text="交易类型:").pack(side="left")
        self.trade_account_type_combo = ttk.Combobox(
            row2_right,
            textvariable=self.trade_account_type_var,
            values=TRADE_ACCOUNT_TYPE_OPTIONS,
            width=8,
            state="readonly",
        )
        self.trade_account_type_combo.pack(side="left", padx=(4, 12))
        self.trade_account_type_combo.bind("<<ComboboxSelected>>", self._on_trade_mode_changed, add="+")

        if current_trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            ttk.Label(row2_left, text="现货交易对:", style="ExchangeAccent.TLabel").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.spot_symbol_var, width=14).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="现货数量精度:").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.spot_precision_var, width=6).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="手续费预留:").pack(side="left")
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
        else:
            ttk.Label(row2_left, text="合约交易对:", style="ExchangeAccent.TLabel").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.futures_symbol_var, width=14).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="保证金模式:").pack(side="left")
            self.futures_margin_type_combo = ttk.Combobox(
                row2_left,
                textvariable=self.futures_margin_type_var,
                values=FUTURES_MARGIN_TYPE_LABEL_OPTIONS,
                width=10,
                state="readonly",
            )
            self.futures_margin_type_combo.pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="杠杆:").pack(side="left")
            ttk.Spinbox(row2_left, from_=1, to=125, textvariable=self.futures_leverage_var, width=6).pack(side="left", padx=(4, 12))
            ttk.Label(row2_left, text="手续费预留:").pack(side="left")
            ttk.Entry(row2_left, textvariable=self.withdraw_buffer_var, width=8).pack(side="left", padx=(4, 12))

            ttk.Label(row2_right, text="开仓方向:").pack(side="left")
            self.futures_side_combo = ttk.Combobox(
                row2_right,
                textvariable=self.futures_side_var,
                values=FUTURES_SIDE_OPTIONS,
                width=8,
                state="readonly",
            )
            self.futures_side_combo.pack(side="left", padx=(4, 12))
            ttk.Label(row2_right, text="下单金额:").pack(side="left")
            ttk.Entry(row2_right, textvariable=self.futures_amount_var, width=10).pack(side="left", padx=(4, 12))

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
        ttk.Checkbutton(row3_left, text="\u81ea\u52a8\u63d0\u73b0", variable=self.enable_withdraw_var).pack(side="left")

        if current_trade_account_type == TRADE_ACCOUNT_TYPE_SPOT:
            if current_trade_mode == TRADE_MODE_MARKET:
                ttk.Label(row3_right, text="现货轮次:").pack(side="left")
                ttk.Spinbox(row3_right, from_=1, to=100, textvariable=self.spot_rounds_var, width=6).pack(side="left", padx=(4, 12))
            elif current_trade_mode == TRADE_MODE_LIMIT:
                ttk.Label(row3_right, text="剩余 BNB 手续费停止值:").pack(side="left")
                ttk.Entry(row3_right, textvariable=self.bnb_fee_stop_var, width=10).pack(side="left", padx=(4, 12))
                ttk.Label(row3_right, text=self._reprice_threshold_label_text()).pack(side="left")
                ttk.Entry(row3_right, textvariable=self.reprice_threshold_var, width=10).pack(side="left", padx=(4, 12))
            else:
                ttk.Label(row3_right, text="溢价百分比(%):").pack(side="left")
                ttk.Entry(row3_right, textvariable=self.premium_percent_var, width=10).pack(side="left", padx=(4, 12))
                ttk.Label(row3_right, text="剩余 BNB 手续费停止值:").pack(side="left")
                ttk.Entry(row3_right, textvariable=self.bnb_fee_stop_var, width=10).pack(side="left", padx=(4, 12))
                ttk.Label(row3_right, text=self._reprice_threshold_label_text()).pack(side="left")
                ttk.Entry(row3_right, textvariable=self.reprice_threshold_var, width=10).pack(side="left", padx=(4, 12))
        else:
            ttk.Label(row3_right, text="合约轮次:").pack(side="left")
            ttk.Spinbox(row3_right, from_=1, to=100, textvariable=self.futures_rounds_var, width=6).pack(side="left", padx=(4, 12))

        self.after_idle(self._align_trade_mode_sections)

    def _on_close(self):
        if self._closing:
            return
        self._closing = True
        self._close_deadline_monotonic = time.monotonic() + 2.5
        logger.info("收到窗口关闭请求，开始优雅停止后台任务")
        self._cancel_after_token("_update_ip_after_token")
        self._cancel_after_token("_log_poll_after_token")
        try:
            self.stop_bot()
        except Exception:
            pass
        page = getattr(self, "onchain_page", None)
        if page is not None:
            try:
                page.shutdown()
            except Exception:
                pass
        self._complete_close_when_idle()

    def _cancel_after_token(self, attr_name: str) -> None:
        token = getattr(self, attr_name, None)
        if token is None:
            return
        setattr(self, attr_name, None)
        try:
            self.after_cancel(token)
        except Exception:
            pass

    def _start_managed_thread(self, target, *, args=(), kwargs=None, name: str = "app-bg", daemon: bool = True) -> threading.Thread:
        call_kwargs = dict(kwargs or {})

        def runner():
            try:
                target(*args, **call_kwargs)
            finally:
                current = threading.current_thread()
                with self._managed_threads_lock:
                    self._managed_threads.discard(current)

        thread = threading.Thread(target=runner, daemon=daemon, name=name)
        with self._managed_threads_lock:
            self._managed_threads.add(thread)
        thread.start()
        return thread

    def _managed_threads_snapshot(self) -> list[threading.Thread]:
        current = threading.current_thread()
        with self._managed_threads_lock:
            return [t for t in self._managed_threads if t is not current and t.is_alive()]

    def _join_managed_threads(self, timeout_total: float = 1.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_total))
        while True:
            alive_threads = self._managed_threads_snapshot()
            if not alive_threads:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            per_thread = max(0.05, remaining / max(1, len(alive_threads)))
            for thread in alive_threads:
                thread.join(per_thread)

    def _background_shutdown_pending(self) -> bool:
        if self.worker_thread and self.worker_thread.is_alive():
            return True
        if self._managed_threads_snapshot():
            return True
        page = getattr(self, "onchain_page", None)
        if page is not None and bool(getattr(page, "is_running", False)):
            return True
        return False

    def _complete_close_when_idle(self):
        self._close_wait_after_token = None
        if self._background_shutdown_pending() and time.monotonic() < self._close_deadline_monotonic:
            try:
                self._close_wait_after_token = self.after(100, self._complete_close_when_idle)
                return
            except Exception:
                self._close_wait_after_token = None
        self._finalize_close()

    def _finalize_close(self):
        if self._close_finalized:
            return
        self._close_finalized = True
        self._cancel_after_token("_close_wait_after_token")
        self._cancel_after_token("_update_ip_after_token")
        self._cancel_after_token("_log_poll_after_token")
        page = getattr(self, "onchain_page", None)
        if page is not None:
            try:
                page.shutdown()
            except Exception:
                pass
        try:
            stop_ui_bridge(self)
        except Exception:
            pass
        try:
            self.exchange_proxy_runtime.stop()
        except Exception:
            pass
        try:
            self.onchain_proxy_runtime.stop()
        except Exception:
            pass
        self._join_managed_threads(timeout_total=1.0)
        self._clear_current_binance_client()
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
        tree = getattr(self, "account_tree", None)
        if tree is None:
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
            tree.yview_scroll(units, "units")
            return "break"
        except Exception:
            return None

    def _focus_account_list_for_paste(self, _event=None):
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return None
        try:
            tree.focus_set()
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
            WITHDRAW_SUCCESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._result_file_lock:
                with open(WITHDRAW_SUCCESS_FILE, "a", encoding="utf-8") as f:
                    f.write(line)
            logger.info("已记录提现到 %s：%s", WITHDRAW_SUCCESS_FILE, line.strip())
        except Exception as e:
            logger.error("写入提现记录文件失败: %s", e)

    def record_total_asset(self, index, api_key, address, network, total_usdt):
        total_dec = Decimal(str(total_usdt))
        line = f"{index}+{api_key}+{total_dec:.8f}\n"

        try:
            TOTAL_ASSET_RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._result_file_lock:
                with open(TOTAL_ASSET_RESULT_FILE, "a", encoding="utf-8") as f:
                    f.write(line)
            logger.info("已记录总资产到 %s：%s", TOTAL_ASSET_RESULT_FILE, line.strip())
        except Exception as e:
            logger.error("写入总资产记录文件失败: %s", e)
        with self._result_file_lock:
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

    def _get_exchange_proxy(self) -> str:
        return self._normalize_exchange_proxy(self.exchange_proxy_var.get())

    def _exchange_proxy_config_payload(self) -> dict[str, object]:
        use_proxy = bool(self.use_exchange_config_proxy_var.get())
        raw_proxy = str(self.exchange_proxy_var.get() or "").strip()
        if use_proxy:
            proxy_text = self._normalize_exchange_proxy(raw_proxy)
            self.exchange_proxy_var.set(proxy_text)
        else:
            proxy_text = raw_proxy
        return {
            "exchange_proxy_enc": self._encrypt_optional_text(proxy_text),
            "use_exchange_config_proxy": use_proxy,
        }

    def _use_exchange_config_proxy(self) -> bool:
        return bool(self.use_exchange_config_proxy_var.get())

    def _get_exchange_proxy_url(self) -> str:
        if not self._use_exchange_config_proxy():
            self.exchange_proxy_runtime.stop()
            return ""
        proxy = self._get_exchange_proxy()
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
        page = self._get_onchain_proxy_page()
        if page is None:
            return ""
        raw = getattr(page, "onchain_proxy_var", None)
        if raw is None:
            return ""
        return self._normalize_proxy_text(raw.get())

    def _use_onchain_config_proxy(self) -> bool:
        page = self._get_onchain_proxy_page()
        if page is None:
            return False
        var = getattr(page, "use_config_proxy_var", None)
        return bool(var.get()) if var is not None else False

    def _get_onchain_proxy_url(self) -> str:
        if not self._use_onchain_config_proxy():
            self.onchain_proxy_runtime.stop()
            return ""
        proxy = self._get_onchain_proxy()
        if not proxy:
            self.onchain_proxy_runtime.stop()
            return ""
        if proxy.lower().startswith("ss://"):
            return self.onchain_proxy_runtime.ensure_proxy(proxy)
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
        if not self._use_exchange_config_proxy():
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
        try:
            spot_precision = int(self.spot_precision_var.get())
        except Exception:
            spot_precision = SPOT_PRECISION_DEFAULT
        return {
            "api_key": SECRET_BOX.encrypt(self.api_key_var.get().strip()),
            "api_secret": SECRET_BOX.encrypt(self.api_secret_var.get().strip()),
            "trade_account_type": trade_settings["trade_account_type"],
            "spot_rounds": int(trade_settings["stored_spot_rounds"]),
            "trade_mode": trade_settings["trade_mode"],
            "premium_percent": trade_settings["premium_percent"],
            "bnb_fee_stop": trade_settings["bnb_fee_stop"],
            "bnb_topup_amount": trade_settings["bnb_topup_amount"],
            "reprice_threshold": trade_settings["reprice_threshold"],
            "spot_symbol": self.spot_symbol_var.get().strip().upper(),
            "spot_precision": spot_precision,
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
            messagebox.showerror("错误", str(e) or "配置格式不正确，请检查交易模式、轮次、溢价比例、手续费停止值和超时时间")
        except Exception as e:
            logger.error("保存策略配置失败: %s", e)
            messagebox.showerror("错误", "保存策略配置失败: %s" % e)

    def _save_exchange_proxy_config_only(self, payload: dict[str, object] | None = None) -> None:
        if payload is None:
            payload = self._exchange_proxy_config_payload()
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
            self.premium_percent_var.set(str(raw.get("premium_percent", PREMIUM_PERCENT_DEFAULT) or PREMIUM_PERCENT_DEFAULT).strip())
            self.bnb_fee_stop_var.set(str(raw.get("bnb_fee_stop", BNB_FEE_STOP_DEFAULT) or BNB_FEE_STOP_DEFAULT).strip())
            self.bnb_topup_amount_var.set(str(raw.get("bnb_topup_amount", BNB_TOPUP_AMOUNT_DEFAULT) or BNB_TOPUP_AMOUNT_DEFAULT).strip())
            self.reprice_threshold_var.set(
                str(raw.get("reprice_threshold", raw.get("reprice_threshold_percent", REPRICE_THRESHOLD_DEFAULT)) or REPRICE_THRESHOLD_DEFAULT).strip()
            )
            self.spot_symbol_var.set(str(raw.get("spot_symbol", SPOT_SYMBOL_DEFAULT) or SPOT_SYMBOL_DEFAULT).strip().upper())
            self.spot_precision_var.set(int(raw.get("spot_precision", SPOT_PRECISION_DEFAULT)))
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
        use_config_proxy = self._use_exchange_config_proxy()
        system_proxy = self._system_proxy_map() if not use_config_proxy else {}
        proxy_status = "跟随系统代理" if system_proxy else "未启用"
        proxy_exit_ip = "--"
        if use_config_proxy and proxy_text:
            proxy_status = "SS代理连接中..." if proxy_text.lower().startswith("ss://") else "代理连接中..."
        if use_config_proxy and proxy_text:
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
            test_ok = False
            save_err = ""
            try:
                status, exit_ip = self._test_exchange_proxy_once()
                route_text = self._exchange_proxy_route_text()
                try:
                    self._save_exchange_proxy_config_only()
                except Exception as e:
                    save_err = str(e)
                test_ok = True
                log_text = f"交易所代理测试成功：status={status}，exit_ip={exit_ip}，route={route_text}"
                if save_err:
                    log_text = f"{log_text}，但保存配置失败：{save_err}"
                else:
                    log_text = f"{log_text}，已自动保存配置"
            except Exception as e:
                status = "连接失败" if (self._use_exchange_config_proxy() and self.exchange_proxy_var.get().strip()) else "未启用"
                if (not self._use_exchange_config_proxy()) and self._system_proxy_map():
                    status = "系统代理异常"
                exit_ip = "--"
                route_text = self._exchange_proxy_route_text()
                log_text = f"交易所代理测试失败：{e}，route={route_text}"

            def _update():
                self.exchange_proxy_status_var.set(status)
                self.exchange_proxy_exit_ip_var.set(exit_ip)
                self._append_log(log_text)
                if not test_ok:
                    messagebox.showerror("代理测试失败", log_text)
                elif save_err:
                    messagebox.showwarning("代理测试成功", log_text)
                else:
                    messagebox.showinfo("代理测试成功", log_text)

            self._dispatch_ui(_update)

        self._start_managed_thread(worker, name="exchange-proxy-test")

    def _try_begin_ip_refresh(self) -> bool:
        with self._ip_refresh_lock:
            if self._closing or self._ip_refresh_inflight:
                return False
            self._ip_refresh_inflight = True
            return True

    def _finish_ip_refresh(self) -> None:
        with self._ip_refresh_lock:
            self._ip_refresh_inflight = False

    def update_ip(self, schedule_next: bool = True):
        if schedule_next and not self._closing:
            self._cancel_after_token("_update_ip_after_token")
            try:
                self._update_ip_after_token = self.after(60000, self.update_ip)
            except Exception:
                self._update_ip_after_token = None
        if not self._try_begin_ip_refresh():
            return

        def worker():
            try:
                proxy_status = "跟随系统代理" if self._system_proxy_map() and not self._use_exchange_config_proxy() else "未启用"
                proxy_exit_ip = "--"
                try:
                    ip = self._fetch_public_ip(use_exchange_proxy=False, allow_system_proxy=False)
                    if self._use_exchange_config_proxy():
                        proxy_status, proxy_exit_ip = self._test_exchange_proxy_once(include_exit_ip=True)
                    elif self._system_proxy_map():
                        proxy_status, proxy_exit_ip = self._test_exchange_proxy_once(include_exit_ip=True)
                    else:
                        proxy_exit_ip = ip
                except Exception as e:
                    ip = "获取失败: %s" % str(e)
                    if self._use_exchange_config_proxy():
                        proxy_status = "连接失败"
                    elif self._system_proxy_map():
                        proxy_status = "系统代理异常"

                def _update():
                    self.ip_var.set(ip)
                    self.exchange_proxy_status_var.set(proxy_status)
                    self.exchange_proxy_exit_ip_var.set(proxy_exit_ip)
                self._dispatch_ui(_update)
            finally:
                self._finish_ip_refresh()

        self._start_managed_thread(worker, name="exchange-ip-refresh")

    def _poll_log_queue(self):
        if self._closing:
            self._log_poll_after_token = None
            return
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            else:
                self._append_log(msg)
        try:
            self._log_poll_after_token = self.after(100, self._poll_log_queue)
        except Exception:
            self._log_poll_after_token = None

    def _append_log(self, msg: str):
        self.text_log.configure(state="normal")
        self.text_log.insert("end", msg + "\n")
        self.text_log.see("end")
        self.text_log.configure(state="disabled")

    def _dispatch_ui(self, callback) -> None:
        if self._closing:
            return
        dispatch_ui_callback(self, callback, root=self)

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
            "trade_account_type_combo",
            "trade_mode_combo",
            "futures_margin_type_combo",
            "futures_side_combo",
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
    ):
        start = time.time()
        c = client or self.client
        if c is None:
            logger.error("wait_for_usdt 调用时没有可用的 BinanceClient")
            return False
        trade_type = self._normalize_trade_account_type(trade_account_type)
        mode_name = self._normalize_trade_mode(trade_mode)
        if trade_type == TRADE_ACCOUNT_TYPE_FUTURES:
            quote_asset = c.get_um_futures_margin_asset(symbol) if symbol else "USDT"
            base_asset = ""
        else:
            quote_asset = BinanceClient.get_spot_quote_asset(symbol) if symbol else "USDT"
            base_asset = BinanceClient.get_spot_base_asset(symbol) if symbol else ""
        required_amount = Decimal(str(required_quote_amount)) if required_quote_amount is not None else Decimal("0")

        while time.time() - start < timeout_sec:
            if stop_event and stop_event.is_set():
                logger.info("检测 %s 时收到停止信号，结束检测", quote_asset)
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
                        logger.info(
                            "%s 到账检测中，当前 U本位可用 %s = %.8f，目标至少 %.8f",
                            quote_asset,
                            quote_asset,
                            quote_balance,
                            float(required_amount),
                        )
                    else:
                        logger.info("%s 到账检测中，当前 U本位可用 %s = %.8f", quote_asset, quote_asset, quote_balance)
                else:
                    c.collect_funding_asset_to_spot(quote_asset)
                    quote_balance_dec = c.spot_asset_balance_decimal(quote_asset)
                    quote_balance = float(quote_balance_dec)
                    if mode_name == TRADE_MODE_LIMIT:
                        base_balance_dec = c.spot_asset_balance_decimal(base_asset)
                        logger.info(
                            "%s模式余额检测中，当前现货 %s = %.8f，%s = %.8f",
                            mode_name,
                            quote_asset,
                            quote_balance,
                            base_asset,
                            float(base_balance_dec),
                        )
                    else:
                        logger.info("%s 到账检测中，当前现货 %s = %.8f", quote_asset, quote_asset, quote_balance)
            except Exception as e:
                logger.error("检测 %s 余额失败: %s", quote_asset, e)
                quote_balance = 0.0
                base_balance_dec = Decimal("0")

            balance_ready = quote_balance > 0
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                balance_ready = Decimal(str(quote_balance)) > 0 or base_balance_dec > 0
            if trade_type == TRADE_ACCOUNT_TYPE_FUTURES and required_amount > 0:
                balance_ready = Decimal(str(quote_balance)) >= required_amount

            if balance_ready:
                if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                    logger.info("检测到可挂单余额，开始执行后续策略")
                else:
                    logger.info("检测到 %s 已到账，开始执行后续策略", quote_asset)
                return True

            delay_seconds = min(self._current_random_delay_seconds(), max(0.0, timeout_sec - (time.time() - start)))
            if delay_seconds <= 0:
                continue
            if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
                logger.info("未检测到可挂单余额，%.3f 秒后重试", delay_seconds)
            else:
                logger.info("%s 未到账，%.3f 秒后重试", quote_asset, delay_seconds)
            if stop_event:
                if stop_event.wait(delay_seconds):
                    logger.info("检测 %s 等待期间收到停止信号，结束检测", quote_asset)
                    return False
            else:
                time.sleep(delay_seconds)

        if trade_type == TRADE_ACCOUNT_TYPE_SPOT and mode_name == TRADE_MODE_LIMIT:
            logger.error("在 %d 秒内未检测到可挂单余额，终止任务", timeout_sec)
            return False
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
            trade_account_type = str(trade_settings["trade_account_type"])
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_percent_value = trade_settings["premium_percent_value"]
            bnb_fee_stop_value = trade_settings["bnb_fee_stop_value"]
            bnb_topup_amount_value = trade_settings["bnb_topup_amount_value"]
            reprice_threshold_value = trade_settings["reprice_threshold_value"]
            futures_symbol = str(trade_settings["futures_symbol"])
            futures_rounds = int(trade_settings["futures_rounds"])
            futures_amount_value = trade_settings["futures_amount_value"]
            futures_leverage = int(trade_settings["futures_leverage"])
            futures_margin_type = str(trade_settings["futures_margin_type"])
            futures_side = str(trade_settings["futures_side"])
            withdraw_buffer = float(self.withdraw_buffer_var.get())
            min_delay = self._delay_var_value(self.min_delay_var, 1000, "\u6700\u5c0f")
            max_delay = self._delay_var_value(self.max_delay_var, 3000, "\u6700\u5927")
            usdt_timeout = int(self.usdt_timeout_var.get())
            spot_precision = int(self.spot_precision_var.get()) if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT else SPOT_PRECISION_DEFAULT
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确")
            return

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_address = self.withdraw_addr_var.get().strip()
        withdraw_network = self.withdraw_net_var.get().strip()
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()

        if enable_withdraw and (not withdraw_address or not withdraw_network or not withdraw_coin):
            messagebox.showerror("错误", "开启自动提现时，请填写 提现地址 / 网络 / 币种")
            return

        client = None
        try:
            client = self._create_binance_client(key, secret)
            quote_asset = (
                client.get_um_futures_margin_asset(trade_symbol)
                if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES
                else BinanceClient.get_spot_quote_asset(trade_symbol)
            )
            effective_reprice_threshold = None
            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode in {TRADE_MODE_LIMIT, TRADE_MODE_PREMIUM}:
                effective_reprice_threshold = client.normalize_price_delta(spot_symbol, reprice_threshold_value, min_one_tick=True)
        except Exception as e:
            self._close_binance_client_instance(client)
            messagebox.showerror("错误", "Binance 连接初始化失败: %s" % e)
            return
        required_quote_amount = None
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
            required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))
        self._replace_current_binance_client(client)

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
        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES:
            total_steps = max(futures_rounds, 1)
        else:
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
            client=client,
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
            trade_account_type=trade_account_type,
            trade_mode=trade_mode,
            premium_percent=premium_percent_value,
            bnb_fee_stop_value=bnb_fee_stop_value,
            bnb_topup_amount=bnb_topup_amount_value,
            reprice_threshold_amount=reprice_threshold_value,
            futures_symbol=futures_symbol,
            futures_rounds=futures_rounds,
            futures_amount=futures_amount_value,
            futures_leverage=futures_leverage,
            futures_margin_type=futures_margin_type,
            futures_side=futures_side,
        )

        def progress_cb(step, total, text):
            def _update():
                self.progress["maximum"] = total
                self.progress["value"] = step
                self.status_var.set("状态：%s (%d/%d)" % (text, step, total))
            self._dispatch_ui(_update)

        def worker():
            try:
                if not self.wait_for_usdt(
                    usdt_timeout,
                    self.stop_event,
                    client=client,
                    symbol=trade_symbol,
                    trade_account_type=trade_account_type,
                    trade_mode=trade_mode,
                    required_quote_amount=required_quote_amount,
                ):
                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                        logger.info("挂单余额检测未通过，任务结束")
                    else:
                        logger.info("%s 检测未通过，任务结束", quote_asset)
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
                elif trade_mode == TRADE_MODE_MARKET:
                    logger.info("开始执行策略：市价 %d 轮，预买BNB金额=%s", spot_rounds, bnb_topup_amount_value)
                elif trade_mode == TRADE_MODE_LIMIT:
                    logger.info(
                        "开始执行策略：挂单模式，预买BNB金额=%s，BNB 手续费停止值=%s，重新挂单阈值=%s %s",
                        bnb_topup_amount_value,
                        bnb_fee_stop_value,
                        BinanceClient._format_decimal(effective_reprice_threshold or Decimal("0")),
                        quote_asset,
                    )
                else:
                    logger.info(
                        "开始执行策略：溢价单模式，预买BNB金额=%s，溢价百分比=%s，BNB 手续费停止值=%s，重新挂单阈值=%s %s",
                        bnb_topup_amount_value,
                        premium_percent_value,
                        bnb_fee_stop_value,
                        BinanceClient._format_decimal(effective_reprice_threshold or Decimal("0")),
                        quote_asset,
                    )
                strategy.run(self.stop_event, progress_cb=progress_cb)
            except Exception as e:
                logger.error("运行过程中出现异常: %s", e)
            finally:
                self._dispatch_ui(self._on_worker_finished)

        self.worker_thread = self._start_managed_thread(worker, name="exchange-single-run")

    def _on_worker_finished(self):
        self._batch_task_active = False
        self._clear_account_batch_runtime()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.btn_run_accounts.config(state="normal")
        self.btn_query_all_assets.config(state="normal")
        self.btn_collect_bnb_combo.config(state="normal")
        self.btn_batch_withdraw.config(state="normal")
        self.btn_refresh.config(state="normal")
        self.btn_withdraw.config(state="normal")
        self._finish_batch_summary_tracking()
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
            client = None
            assigned_to_app = False
            try:
                logger.info("交易所刷新余额链路：%s", self._exchange_proxy_route_text())
                client = self._create_binance_client(key, secret)
                spot_balances = client.spot_all_balances()
                balances_text = self._format_spot_balances_text(spot_balances)
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
            buffer_val = float(self.withdraw_buffer_var.get())
        except ValueError:
            messagebox.showerror("错误", "手续费预留格式不正确")
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

    def _reindex_accounts(self):
        for i, acc in enumerate(self.accounts, start=1):
            acc["index_var"].set(str(i))
            self._refresh_account_tree_row(acc)
        self._refresh_account_list_hint()

    def _on_global_network_changed(self, *_):
        self.apply_network_to_all_accounts()

    def apply_network_to_all_accounts(self):
        net = self.acc_network_var.get().strip() or WITHDRAW_NETWORK_DEFAULT
        for acc in self.accounts:
            acc["network"] = net
            if "network_var" in acc:
                acc["network_var"].set(net)
            self._refresh_account_tree_row(acc)

    @staticmethod
    def _account_row_color_by_status(status_text: str) -> str:
        s = str(status_text or "").strip()
        if not s or s == "就绪":
            return "#f2f2f2"
        if "未到账" in s:
            return "#ffe3b8"
        if any(k in s for k in ("已停止", "已请求停止")):
            return "#ffe3b8"
        if any(k in s for k in ("失败", "异常")):
            return "#f8c7c7"
        if any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "#cfeecf"
        return "#cfe3ff"

    def _is_context_account(self, acc: dict) -> bool:
        return bool(acc is not None and acc is getattr(self, "_context_account", None))

    def _account_row_style_tag(self, acc: dict) -> str:
        if self._is_context_account(acc):
            return "acc_context"
        s = str(acc.get("status_var").get() if acc.get("status_var") is not None else "").strip()
        if not s or s == "就绪":
            return "acc_ready"
        if "未到账" in s or any(k in s for k in ("已停止", "已请求停止")):
            return "acc_warn"
        if any(k in s for k in ("失败", "异常")):
            return "acc_failed"
        if any(k in s for k in ("成功", "完成", "总资产", "无可提", "提现额度")):
            return "acc_success"
        return "acc_running"

    def _account_tree_values(self, acc: dict) -> tuple[str, str, str, str, str, str]:
        checked = "✓" if bool(acc.get("selected_var").get()) else ""
        index_text = str(acc.get("index_var").get() or "")
        api_key = self._mask_key(str(acc.get("api_key") or ""))
        address = self._mask_addr(str(acc.get("address") or ""))
        network = str(acc.get("network_var").get() or acc.get("network") or "")
        status = str(acc.get("status_var").get() or "")
        return checked, index_text, api_key, address, network, status

    def _refresh_account_tree_row(self, acc: dict) -> None:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return
        tree_id = str(acc.get("tree_id") or "").strip()
        if not tree_id:
            return
        try:
            tree.item(tree_id, values=self._account_tree_values(acc), tags=(self._account_row_style_tag(acc),))
        except Exception:
            pass

    def _insert_account_tree_row(self, acc: dict) -> None:
        tree = getattr(self, "account_tree", None)
        if tree is None:
            return
        tree_id = tree.insert("", "end", values=self._account_tree_values(acc), tags=(self._account_row_style_tag(acc),))
        acc["tree_id"] = tree_id
        self._account_tree_row_to_account[tree_id] = acc

    def _account_from_tree_row_id(self, row_id: str) -> dict | None:
        return self._account_tree_row_to_account.get(str(row_id or "").strip())

    def _apply_account_row_style(self, acc: dict):
        self._refresh_account_tree_row(acc)

    def _set_account_status(self, acc: dict, text: str):
        acc["status_var"].set(str(text))
        self._apply_account_row_style(acc)

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

    def _set_context_account(self, acc: dict | None):
        previous = getattr(self, "_context_account", None)
        self._context_account = acc
        if previous is not None and previous is not acc:
            self._apply_account_row_style(previous)
        if acc is not None:
            self._apply_account_row_style(acc)

    def _on_account_tree_double_click(self, event):
        tree = getattr(self, "account_tree", None)
        if tree is None or tree.identify("region", event.x, event.y) != "cell":
            return None
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

    def run_context_account_query(self):
        acc = self._get_context_account_or_warn()
        if acc is None:
            return
        self.run_selected_accounts(
            accounts_to_run=[acc],
            batch_total_asset_only=True,
            require_confirm=False,
        )

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
        return " | ".join(f"{asset} {cls._format_amount(float(amount_dec))}" for asset, amount_dec in items)

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
            amount_text = cls._format_amount(float(amount))
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
        withdraw_text = self._format_batch_metric_totals(data.get("withdraw_by_account") or {})
        balance_text = self._format_batch_metric_totals(data.get("balance_by_account") or {})
        return f"结果汇总：成功{success_count} | 失败{failed_count} | 提现总额={withdraw_text} | 余额总额={balance_text}"

    def _set_batch_summary_text(self, text: str) -> None:
        var = getattr(self, "exchange_batch_summary_var", None)
        if var is not None:
            var.set(str(text or ""))

    def _refresh_batch_summary_text(self) -> None:
        with self._batch_summary_lock:
            text = self._build_batch_summary_text(self._current_batch_summary)
        self._dispatch_ui(lambda t=text: self._set_batch_summary_text(t))

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
        self._dispatch_ui(lambda t=summary_text: self._set_batch_summary_text(t))

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
        self._dispatch_ui(lambda t=summary_text: self._set_batch_summary_text(t))

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
        self._dispatch_ui(lambda t=summary_text: self._set_batch_summary_text(t))

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
        self._dispatch_ui(lambda t=summary_text: self._set_batch_summary_text(t))

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
        self.run_selected_accounts(accounts_to_run=accounts_to_retry, require_confirm=False, **retry_kwargs)

    def _append_account_row(self, key, secret, addr, net, selected=True):
        net = (net or "").strip() or WITHDRAW_NETWORK_DEFAULT
        index_var = tk.StringVar(value=str(len(self.accounts) + 1))
        selected_var = tk.BooleanVar(value=selected)
        network_var = tk.StringVar(value=net)
        status_var = tk.StringVar(value="就绪")
        acc = {
            "index_var": index_var,
            "selected_var": selected_var,
            "network_var": network_var,
            "status_var": status_var,
            "stop_event": threading.Event(),
            "batch_active": False,
            "api_key": key,
            "api_secret": secret,
            "address": addr,
            "network": net,
            "tree_id": "",
        }
        selected_var.trace_add("write", lambda *_args, a=acc: (self._update_toggle_select_button_text(), self._refresh_account_tree_row(a)))
        self.accounts.append(acc)
        self._insert_account_tree_row(acc)
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
                tree_id = str(acc.get("tree_id") or "").strip()
                if tree_id:
                    self._account_tree_row_to_account.pop(tree_id, None)
                    try:
                        self.account_tree.delete(tree_id)
                    except Exception:
                        pass
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
            spot_precision = SPOT_PRECISION_DEFAULT
            max_threads = int(self.max_threads_var.get())
            if (not batch_total_asset_only) and (not batch_collect_bnb_mode):
                trade_settings = self._collect_trade_mode_settings()
                if str(trade_settings.get("trade_account_type")) == TRADE_ACCOUNT_TYPE_SPOT:
                    spot_precision = int(self.spot_precision_var.get())
        except (ValueError, RuntimeError) as e:
            messagebox.showerror("错误", str(e) or "参数格式不正确 (请检查轮数/线程数/延迟等)")
            return

        if max_threads < 1:
            max_threads = 1

        spot_symbol = self.spot_symbol_var.get().strip().upper()
        enable_withdraw = bool(self.enable_withdraw_var.get())
        withdraw_coin = self.withdraw_coin_var.get().strip().upper()
        if trade_settings:
            trade_account_type = str(trade_settings["trade_account_type"])
            spot_rounds = int(trade_settings["spot_rounds"])
            trade_mode = str(trade_settings["trade_mode"])
            premium_percent_value = trade_settings["premium_percent_value"]
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
            premium_percent_value = None
            bnb_fee_stop_value = None
            bnb_topup_amount_value = Decimal("0")
            reprice_threshold_value = Decimal(REPRICE_THRESHOLD_DEFAULT)
            futures_symbol = self.futures_symbol_var.get().strip().upper()
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

        skip_usdt_wait_in_batch = bool(self.skip_usdt_wait_in_batch_var.get())

        if batch_total_asset_only:
            self.total_asset_results = {}

        if batch_total_asset_only:
            batch_action_label = "查询全部总资产"
        elif batch_collect_bnb_mode and batch_sell_large_spot_to_bnb:
            batch_action_label = "归集并买BNB"
        elif batch_collect_bnb_mode:
            batch_action_label = "归集BNB"
        else:
            batch_action_label = "批量执行"

        logger.info("交易所批量链路：%s", self._exchange_proxy_route_text())
        self.stop_event = threading.Event()
        self._batch_task_active = True
        self._begin_batch_summary_tracking(
            action_label=batch_action_label,
            runner="run_selected_accounts",
            retry_kwargs={
                "batch_total_asset_only": bool(batch_total_asset_only),
                "batch_collect_bnb_mode": bool(batch_collect_bnb_mode),
                "batch_sell_large_spot_to_bnb": bool(batch_sell_large_spot_to_bnb),
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
        self._set_retry_failed_button_state()
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
            self._dispatch_ui(_u)

        def worker_loop(thread_id):
            while not self.stop_event.is_set():
                try:
                    idx, acc = task_queue.get(timeout=1)
                except queue.Empty:
                    return

                def set_status(text, acc_ref=acc):
                    def _u():
                        self._set_account_status(acc_ref, text)
                    self._dispatch_ui(_u)

                def progress_cb(step, total, text, acc_obj=acc):
                    def _u():
                        self._set_account_status(acc_obj, text)
                    self._dispatch_ui(_u)

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

                if combined_stop.is_set():
                    finish_current_now("已停止", success=False)
                    continue

                try:
                    client = None
                    client = self._create_binance_client(acc["api_key"], acc["api_secret"])
                    trade_symbol = futures_symbol if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES else spot_symbol
                    quote_asset = (
                        client.get_um_futures_margin_asset(trade_symbol)
                        if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES
                        else BinanceClient.get_spot_quote_asset(trade_symbol)
                    )
                    required_quote_amount = None
                    if trade_account_type == TRADE_ACCOUNT_TYPE_FUTURES and futures_amount_value is not None and futures_leverage > 0:
                        required_quote_amount = futures_amount_value / Decimal(str(futures_leverage))

                    # 1) 只查询总资产
                    if batch_total_asset_only:
                        set_status("查询总资产...")
                        total_usdt, rows = client.query_total_wallet_balance("USDT")
                        asset_breakdown = client.query_asset_balances_breakdown()
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

                        set_status(asset_text if asset_text != "--" else f"总资产 {Decimal(str(total_usdt)):.4f} USDT")
                        op_success = True

                    # 2) 批量归集BNB模式
                    elif batch_collect_bnb_mode:
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
                            set_status("大额币卖USDT...")
                            try:
                                sold_assets = client.sell_large_spot_assets_to_usdt()
                                logger.info(f"账号 #{idx} 大额币卖出完成: {sold_assets}")
                            except Exception as e:
                                logger.warning(f"账号 #{idx} 大额币卖出失败: {e}")

                            if combined_stop.is_set():
                                finish_current_now("已停止")
                                continue

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
                        self._record_batch_balance_metric(acc, Decimal(str(bnb_balance)), "BNB")
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
                            self._record_batch_withdraw_metric(acc, amount, "BNB")
                            if amount > 0:
                                self.record_withdraw(idx, acc["api_key"], acc["address"], amount)
                            set_status(self._format_withdraw_amount_status(amount, "BNB", enable_withdraw=enable_withdraw))
                            op_success = True
                        except Exception as e:
                            logger.error(f"账号 #{idx} BNB提现失败: {e}")
                            set_status("BNB提现失败")
                            op_success = False

                    # 3) 原批量现货策略
                    else:
                        need_wait_usdt = not skip_usdt_wait_in_batch

                        if need_wait_usdt:
                            if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
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
                            ):
                                if combined_stop.is_set():
                                    logger.info(f"账号 #{idx} 已请求停止")
                                    finish_current_now("已停止", success=False)
                                else:
                                    if trade_account_type == TRADE_ACCOUNT_TYPE_SPOT and trade_mode == TRADE_MODE_LIMIT:
                                        logger.info(f"账号 #{idx} 挂单余额检测超时，跳过")
                                        set_status("无可挂单余额")
                                    else:
                                        logger.info(f"账号 #{idx} {quote_asset} 检测超时，跳过")
                                        set_status(f"{quote_asset} 未到账")
                                    finish_current_now(success=False)
                                continue
                        else:
                            logger.info(f"账号 #{idx} 已开启“批量策略跳过{quote_asset}检测”")
                            set_status(f"跳过{quote_asset}检测")

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
                        elif trade_mode == TRADE_MODE_MARKET:
                            logger.info("账号 #%d 开始执行市价策略：%d 轮，预买BNB金额=%s", idx, spot_rounds, bnb_topup_amount_value)
                        elif trade_mode == TRADE_MODE_LIMIT:
                            effective_reprice_threshold = client.normalize_price_delta(
                                spot_symbol,
                                reprice_threshold_value,
                                min_one_tick=True,
                            )
                            logger.info(
                                "账号 #%d 开始执行挂单策略：预买BNB金额=%s，BNB 手续费停止值=%s，重新挂单阈值=%s %s",
                                idx,
                                bnb_topup_amount_value,
                                bnb_fee_stop_value,
                                BinanceClient._format_decimal(effective_reprice_threshold),
                                quote_asset,
                            )
                        else:
                            effective_reprice_threshold = client.normalize_price_delta(
                                spot_symbol,
                                reprice_threshold_value,
                                min_one_tick=True,
                            )
                            logger.info(
                                "账号 #%d 开始执行溢价单策略：预买BNB金额=%s，溢价百分比=%s，BNB 手续费停止值=%s，重新挂单阈值=%s %s",
                                idx,
                                bnb_topup_amount_value,
                                premium_percent_value,
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
                            combined_stop.wait(random.randint(low_ms, high_ms) / 1000.0)

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
                            trade_account_type=trade_account_type,
                            trade_mode=trade_mode,
                            premium_percent=premium_percent_value,
                            bnb_fee_stop_value=bnb_fee_stop_value,
                            bnb_topup_amount=bnb_topup_amount_value,
                            reprice_threshold_amount=reprice_threshold_value,
                            futures_symbol=futures_symbol,
                            futures_rounds=futures_rounds,
                            futures_amount=futures_amount_value,
                            futures_leverage=futures_leverage,
                            futures_margin_type=futures_margin_type,
                            futures_side=futures_side,
                        )

                        strategy_result = strategy.run(combined_stop, progress_cb=progress_cb)

                        if not combined_stop.is_set():
                            callback_amount = withdraw_state.get("amount")
                            result_amount = float((strategy_result or {}).get("withdraw_amount", 0.0)) if isinstance(strategy_result, dict) else 0.0
                            final_amount = callback_amount if callback_amount is not None else result_amount

                            if isinstance(strategy_result, dict) and strategy_result.get("withdraw_error") and (final_amount or 0) <= 0:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(f"提现失败 {self._compact_error_text(strategy_result.get('withdraw_error', ''))}")
                                op_success = False
                            else:
                                self._record_batch_withdraw_metric(acc, final_amount or 0.0, withdraw_coin)
                                set_status(
                                    self._format_withdraw_amount_status(
                                        float(final_amount or 0.0),
                                        withdraw_coin,
                                        enable_withdraw=enable_withdraw,
                                    )
                                )
                                op_success = True
                        else:
                            set_status("已停止")
                            op_success = False

                except Exception as e:
                    logger.error(f"账号 #{idx} 执行异常: {e}")
                    set_status("异常")
                    op_success = False
                finally:
                    self._close_binance_client_instance(client)
                    if should_finish_in_finally:
                        self._set_account_batch_active(acc, False)
                        self._mark_batch_account_result(acc, op_success)
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
        self._set_retry_failed_button_state()
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
                    self._dispatch_ui(lambda: self._set_account_status(acc_ref, text))

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
                            set_status(self._format_withdraw_amount_status(amount, coin, enable_withdraw=True))
                            op_success = True
                except Exception as e:
                    logger.error(f"账号 #{idx} 提现失败: {e}")
                    set_status("提现失败")
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

def run_selftest() -> int:
    try:
        checks: list[str] = []

        if OnchainTransferPage is None:
            raise RuntimeError(f"链上模块导入失败: {_ONCHAIN_IMPORT_ERROR}")
        checks.append("onchain-import")

        EvmClient.ensure_dependencies(require_signing=True)
        client = EvmClient()
        checks.append("evm-deps")

        zero_address = "0x0000000000000000000000000000000000000000"
        eth_balance = client.get_balance_wei("ETH", zero_address)
        bsc_balance = client.get_balance_wei("BSC", zero_address)
        checks.append(f"eth-rpc={eth_balance}")
        checks.append(f"bsc-rpc={bsc_balance}")

        session = requests.Session()
        session.trust_env = False
        session.proxies = {
            "http": "socks5://127.0.0.1:9",
            "https": "socks5://127.0.0.1:9",
        }
        try:
            session.get("http://example.com", timeout=1)
        except Exception as exc:
            if "Missing dependencies for SOCKS support" in str(exc):
                raise RuntimeError("requests 缺少 SOCKS 支持（PySocks 未打包）") from exc
        finally:
            session.close()
        checks.append("socks-support")

        try:
            xray_path = ExchangeProxyRuntime.find_xray_executable()
        except Exception as exc:
            if os.name == "nt" and getattr(sys, "frozen", False):
                raise
            logger.warning("SELFTEST: xray 未找到，将继续使用 sing-box 作为内置 SS 代理主后端: %s", exc)
            checks.append("xray=optional-missing")
        else:
            checks.append(f"xray={xray_path.name}")
        sing_box_path = ExchangeProxyRuntime.find_sing_box_executable()
        checks.append(f"sing-box={sing_box_path.name}")

        logger.info("SELFTEST OK: %s", ", ".join(checks))
        return 0
    except Exception as exc:
        logger.exception("SELFTEST FAILED: %s", exc)
        return 1


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
    if "--selftest" in sys.argv:
        raise SystemExit(run_selftest())

    app = App()
    app.mainloop()
