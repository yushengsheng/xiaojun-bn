#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

from app_paths import APP_DIR, BUNDLE_DIR, DATA_DIR
from exchange_logging import logger
from stores import _atomic_write_text


def _json_dump_text(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _atomic_write_json(path: Path, payload: object) -> None:
    _atomic_write_text(path, _json_dump_text(payload), encoding="utf-8")

class ExchangeProxyRuntime:
    _RUNTIME_PREPARE_LOCK = threading.Lock()
    _RUNTIME_HTTP_HEADERS = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "xiaojun-bn/1.0",
    }
    _RUNTIME_READY_HEADERS = {
        "User-Agent": "xiaojun-bn/1.0",
    }
    _READY_CHECK_URLS = (
        "https://api.binance.com/api/v3/time",
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://ipinfo.io/ip",
    )
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
                base_dir = destination.resolve()
                for member in archive.infolist():
                    member_name = str(member.filename or "")
                    if not member_name:
                        continue
                    member_path = (destination / member_name).resolve()
                    if os.path.commonpath([str(base_dir), str(member_path)]) != str(base_dir):
                        raise RuntimeError(f"压缩包包含非法路径：{member_name}")
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
            for url in self._READY_CHECK_URLS:
                try:
                    resp = http_get_via_proxy(
                        url,
                        headers=self._RUNTIME_READY_HEADERS,
                        proxies=proxies,
                        timeout=4,
                    )
                    resp.raise_for_status()
                    return
                except Exception as exc:
                    last_err = f"{url}: {exc}"
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
