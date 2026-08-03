#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
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
from urllib.parse import parse_qs, unquote, urlsplit

import requests

from app_paths import APP_DIR, BUNDLE_DIR, DATA_DIR
from exchange_logging import logger
from secret_box import SECRET_BOX
from sensitive_data import redact_sensitive_text
from stores import _atomic_write_json_with_backup, _atomic_write_text, _load_json_with_backup
from system_proxy import configure_requests_session_for_proxy


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
    _SING_BOX_TAG = "v1.13.5"
    _SING_BOX_ASSET_SHA256 = {
        "sing-box-1.13.5-darwin-amd64.tar.gz": "ca37fa11955950c3484bd3365e6d7ed479b8fdd2e41c26a6b4d60faa2283fd3f",
        "sing-box-1.13.5-darwin-arm64.tar.gz": "7700717680c2aa6eb9b034d7114fd16d6f205039255ea406fdd6eb7e3b93d991",
        "sing-box-1.13.5-linux-386.tar.gz": "ddceeb0d4014e5a968a1607dd50b14467621e4c1094277dd24dae55e392fa055",
        "sing-box-1.13.5-linux-amd64.tar.gz": "645d2f30bc984177dc48c749d2e856ccad915df412dc364ca0b312385b3ef255",
        "sing-box-1.13.5-linux-arm64.tar.gz": "d6ca578ed8de5c4dacbd8931fa0bd5dfd940a5767dd6992504f121dbb664bbc9",
        "sing-box-1.13.5-windows-386.zip": "c1644083fc9439b442a063534cd0fc7a1243cce6e9aff2f83b4585342ade01b9",
        "sing-box-1.13.5-windows-amd64.zip": "841a46ab1fef823fefd4248229db5220f09535c883b4cfc6d4fbe5903bc3f1d1",
        "sing-box-1.13.5-windows-arm64.zip": "b9c7eda0ce0f511cf8433181319f983a864f2316c4c1c3a949f5abe447316d71",
    }
    _HYSTERIA_TAG = "app/v2.9.2"
    _HYSTERIA_ASSET_SHA256 = {
        "hysteria-darwin-amd64": "226363d1ad1f44c978f4ee052e45196589fc6eefa21c9646b3c27ef89122d172",
        "hysteria-darwin-arm64": "a08b087f4a7544d07e6dd94280bc1baa23f6ab67d314e4578aa717209917b3eb",
        "hysteria-linux-386": "cc6f15d81185bd009625d98bcee16bb3be058f23b050e467056c26b3a053194c",
        "hysteria-linux-amd64": "86fef8e2f1b2bf41318ac96724eee6c3b449e4e510022cc89658b63a6713922a",
        "hysteria-linux-arm64": "9ec8f49f4ea554b1cac04e6f3690cea76ff835082e943e54196d7f323fcfba71",
        "hysteria-windows-386.exe": "586bb34ef13aeef36bde97229c9e3d0d5a2a09c96fb9cd25cf4d4ea186d77581",
        "hysteria-windows-amd64.exe": "103c7258384d0ebbf0d19ca4bec50d6cb9b0b7be33f5b76f004fed0a8805ff9f",
        "hysteria-windows-arm64.exe": "668d8d36650df0bd0c26b606ed6cf45e9dfb22e63b276e72e3d33fc345c9635d",
    }
    _RUNTIME_MANIFEST_VERSION = 1

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
    def is_hysteria2_uri(value: object) -> bool:
        return urlsplit(str(value or "").strip()).scheme.lower() in {"hy2", "hysteria2"}

    @classmethod
    def validate_hysteria2_uri(cls, value: object) -> str:
        text = str(value or "").strip()
        parsed = urlsplit(text)
        if parsed.scheme.lower() not in {"hy2", "hysteria2"}:
            raise RuntimeError("不是有效的 Hysteria2 链接")
        if not parsed.hostname:
            raise RuntimeError("Hysteria2 链接解析失败：缺少服务器地址")
        userinfo = parsed.netloc.rsplit("@", 1)[0] if "@" in parsed.netloc else ""
        if not unquote(userinfo).strip():
            raise RuntimeError("Hysteria2 链接解析失败：缺少认证信息")
        query = {str(key or "").lower(): values for key, values in parse_qs(parsed.query).items()}
        pin_values = query.get("pinsha256") or []
        if pin_values:
            fingerprint = str(pin_values[0] or "").replace(":", "").strip()
            if len(fingerprint) != 64 or any(char not in "0123456789abcdefABCDEF" for char in fingerprint):
                raise RuntimeError("Hysteria2 链接解析失败：pinSHA256 证书指纹无效")
        return text

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
    def _file_sha256(path: Path) -> str:
        hasher = hashlib.sha256()
        with Path(path).open("rb") as handle:
            while True:
                chunk = handle.read(262144)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    @classmethod
    def _sing_box_manifest_path(cls, executable: Path) -> Path:
        candidate = Path(executable)
        return candidate.with_name(f".{candidate.name}.manifest.json")

    @classmethod
    def _sing_box_manifest_context(cls, executable: Path) -> str:
        candidate = Path(executable)
        return f"proxy-runtime:sing-box:{cls._SING_BOX_TAG}:{candidate.parent.name}:{candidate.name}"

    @classmethod
    def _is_cached_sing_box_path(cls, executable: Path) -> bool:
        return cls._path_is_within(Path(executable), cls._runtime_cache_dir("sing-box"))

    @classmethod
    def _validate_sing_box_manifest(cls, payload: object, executable: Path) -> None:
        if not isinstance(payload, dict):
            raise RuntimeError("sing-box 运行时清单格式无效")
        if int(payload.get("version", 0) or 0) != cls._RUNTIME_MANIFEST_VERSION:
            raise RuntimeError("sing-box 运行时清单版本不支持")
        if str(payload.get("backend") or "") != "sing-box":
            raise RuntimeError("sing-box 运行时清单后端不匹配")
        if str(payload.get("runtime_tag") or "") != cls._SING_BOX_TAG:
            raise RuntimeError("sing-box 运行时版本不匹配")
        if str(payload.get("executable") or "") != Path(executable).name:
            raise RuntimeError("sing-box 运行时文件名不匹配")
        digest = str(payload.get("sha256") or "").strip().lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise RuntimeError("sing-box 运行时清单哈希无效")

    @classmethod
    def _write_sing_box_manifest(cls, executable: Path) -> None:
        candidate = Path(executable)
        payload = {
            "version": cls._RUNTIME_MANIFEST_VERSION,
            "backend": "sing-box",
            "runtime_tag": cls._SING_BOX_TAG,
            "executable": candidate.name,
            "sha256": cls._file_sha256(candidate),
        }
        manifest_path = cls._sing_box_manifest_path(candidate)
        _atomic_write_json_with_backup(
            manifest_path,
            payload,
            secret_box=SECRET_BOX,
            config_context=cls._sing_box_manifest_context(candidate),
        )

    @staticmethod
    def _prepare_darwin_runtime(executable: Path, backend: str = "sing-box") -> None:
        """Apply a local ad-hoc signature after the downloaded archive hash is verified.

        macOS can leave a downloaded command-line Mach-O blocked before it reaches
        its listener, even when the release archive itself passed SHA-256 checks.
        Re-signing locally changes only the code-signature envelope and preserves
        the verified executable content.
        """
        if sys.platform != "darwin":
            return
        codesign = Path("/usr/bin/codesign")
        if not codesign.is_file():
            raise RuntimeError(f"macOS 缺少 codesign，无法准备已验证的 {backend} 运行时")
        try:
            result = subprocess.run(
                [str(codesign), "--force", "--sign", "-", str(executable)],
                capture_output=True,
                text=True,
                timeout=45,
                check=False,
            )
        except Exception as exc:
            raise RuntimeError(f"macOS {backend} 本机签名失败：{exc}") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "未知错误").strip()
            raise RuntimeError(f"macOS {backend} 本机签名失败：{detail}")

    @classmethod
    def _hysteria_manifest_path(cls, executable: Path) -> Path:
        candidate = Path(executable)
        return candidate.with_name(f".{candidate.name}.manifest.json")

    @classmethod
    def _hysteria_manifest_context(cls, executable: Path) -> str:
        candidate = Path(executable)
        return f"proxy-runtime:hysteria:{cls._HYSTERIA_TAG}:{candidate.parent.name}:{candidate.name}"

    @classmethod
    def _is_cached_hysteria_path(cls, executable: Path) -> bool:
        return cls._path_is_within(Path(executable), cls._runtime_cache_dir("hysteria"))

    @classmethod
    def _validate_hysteria_manifest(cls, payload: object, executable: Path) -> None:
        if not isinstance(payload, dict):
            raise RuntimeError("Hysteria 运行时清单格式无效")
        if int(payload.get("version", 0) or 0) != cls._RUNTIME_MANIFEST_VERSION:
            raise RuntimeError("Hysteria 运行时清单版本不支持")
        if str(payload.get("backend") or "") != "hysteria":
            raise RuntimeError("Hysteria 运行时清单后端不匹配")
        if str(payload.get("runtime_tag") or "") != cls._HYSTERIA_TAG:
            raise RuntimeError("Hysteria 运行时版本不匹配")
        if str(payload.get("executable") or "") != Path(executable).name:
            raise RuntimeError("Hysteria 运行时文件名不匹配")
        digest = str(payload.get("sha256") or "").strip().lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise RuntimeError("Hysteria 运行时清单哈希无效")

    @classmethod
    def _write_hysteria_manifest(cls, executable: Path) -> None:
        candidate = Path(executable)
        payload = {
            "version": cls._RUNTIME_MANIFEST_VERSION,
            "backend": "hysteria",
            "runtime_tag": cls._HYSTERIA_TAG,
            "executable": candidate.name,
            "sha256": cls._file_sha256(candidate),
        }
        _atomic_write_json_with_backup(
            cls._hysteria_manifest_path(candidate),
            payload,
            secret_box=SECRET_BOX,
            config_context=cls._hysteria_manifest_context(candidate),
        )

    @classmethod
    def _verify_cached_hysteria(cls, executable: Path) -> bool:
        candidate = Path(executable)
        if not candidate.is_file():
            return False
        try:
            payload, _recovered = _load_json_with_backup(
                cls._hysteria_manifest_path(candidate),
                validator=lambda value: cls._validate_hysteria_manifest(value, candidate),
                secret_box=SECRET_BOX,
                config_context=cls._hysteria_manifest_context(candidate),
            )
            return cls._file_sha256(candidate).lower() == str(payload["sha256"]).strip().lower()
        except Exception as exc:
            logger.warning("已忽略未通过认证的 Hysteria 缓存运行时 %s：%s", candidate, exc)
            return False

    @classmethod
    def _trusted_hysteria_candidate(cls, executable: Path) -> Path | None:
        candidate = Path(executable)
        if cls._is_cached_hysteria_path(candidate) and not cls._verify_cached_hysteria(candidate):
            return None
        return candidate

    @classmethod
    def _verify_external_hysteria_override(cls, executable: Path) -> None:
        expected = str(os.environ.get("HYSTERIA_EXECUTABLE_SHA256", "") or "").strip().lower()
        if expected.startswith("sha256:"):
            expected = expected.split(":", 1)[1].strip()
        if not expected:
            return
        if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
            raise RuntimeError("HYSTERIA_EXECUTABLE_SHA256 格式无效，应为 64 位 SHA-256")
        if cls._file_sha256(Path(executable)).lower() != expected:
            raise RuntimeError("显式 Hysteria 运行时哈希校验失败")

    @classmethod
    def _verify_cached_sing_box(cls, executable: Path) -> bool:
        candidate = Path(executable)
        if not candidate.is_file():
            return False
        manifest_path = cls._sing_box_manifest_path(candidate)
        try:
            payload, _recovered = _load_json_with_backup(
                manifest_path,
                validator=lambda value: cls._validate_sing_box_manifest(value, candidate),
                secret_box=SECRET_BOX,
                config_context=cls._sing_box_manifest_context(candidate),
            )
            expected_digest = str(payload["sha256"]).strip().lower()
            return cls._file_sha256(candidate).lower() == expected_digest
        except Exception as exc:
            logger.warning("已忽略未通过认证的 sing-box 缓存运行时 %s：%s", candidate, exc)
            return False

    @classmethod
    def _trusted_sing_box_candidate(cls, executable: Path) -> Path | None:
        candidate = Path(executable)
        if cls._is_cached_sing_box_path(candidate) and not cls._verify_cached_sing_box(candidate):
            return None
        return candidate

    @classmethod
    def _verify_external_sing_box_override(cls, executable: Path) -> None:
        """Optionally bind an explicit external override to a caller-supplied hash."""
        expected = str(os.environ.get("SING_BOX_EXECUTABLE_SHA256", "") or "").strip().lower()
        if expected.startswith("sha256:"):
            expected = expected.split(":", 1)[1].strip()
        if not expected:
            return
        if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
            raise RuntimeError("SING_BOX_EXECUTABLE_SHA256 格式无效，应为 64 位 SHA-256")
        actual = cls._file_sha256(Path(executable)).lower()
        if actual != expected:
            raise RuntimeError("显式 sing-box 运行时哈希校验失败")

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
    def _candidate_hysteria_paths() -> list[Path]:
        return ExchangeProxyRuntime._collect_executable_candidates([
            BUNDLE_DIR / "bin",
            BUNDLE_DIR / "bin" / "hysteria",
            APP_DIR / "bin",
            APP_DIR / "bin" / "hysteria",
            ExchangeProxyRuntime._runtime_cache_dir("hysteria"),
        ], ExchangeProxyRuntime._candidate_executable_names("hysteria"))

    @staticmethod
    def _path_is_within(path: Path, root: Path) -> bool:
        try:
            Path(path).resolve().relative_to(Path(root).resolve())
            return True
        except Exception:
            return False

    @staticmethod
    def _bundle_backend_roots(backend: str) -> list[Path]:
        backend_name = str(backend or "").strip()
        if not backend_name:
            return []
        roots = [BUNDLE_DIR / "bin" / backend_name]
        alt = backend_name.replace("-", "_")
        if alt != backend_name:
            roots.append(BUNDLE_DIR / "bin" / alt)
        return roots

    @classmethod
    def _stage_bundled_runtime_if_needed(cls, executable: Path, backend: str) -> Path:
        candidate = Path(executable)
        if not (os.name == "nt" and getattr(sys, "frozen", False)):
            return candidate
        if not candidate.exists():
            return candidate
        if not cls._path_is_within(candidate, BUNDLE_DIR):
            return candidate

        staged_root = cls._runtime_cache_dir(backend) / "bundled"
        staged_root.mkdir(parents=True, exist_ok=True)

        with cls._RUNTIME_PREPARE_LOCK:
            for source_root in cls._bundle_backend_roots(backend):
                if source_root.exists() and cls._path_is_within(candidate, source_root):
                    shutil.copytree(source_root, staged_root, dirs_exist_ok=True)
                    staged_executable = cls._find_extracted_executable(staged_root, backend)
                    if staged_executable is not None and staged_executable.exists():
                        return staged_executable
                    break

            staged_executable = staged_root / candidate.name
            if (not staged_executable.exists()) or staged_executable.stat().st_size != candidate.stat().st_size:
                shutil.copy2(candidate, staged_executable)
            return staged_executable

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

    @classmethod
    def _pinned_sing_box_asset(cls) -> tuple[str, str, str]:
        platform_tag = cls._runtime_platform_tag()
        arch_tag = cls._runtime_arch_tag()
        archive_suffix = ".zip" if platform_tag == "windows" else ".tar.gz"
        version = cls._SING_BOX_TAG.removeprefix("v")
        asset_name = f"sing-box-{version}-{platform_tag}-{arch_tag}{archive_suffix}"
        digest = cls._SING_BOX_ASSET_SHA256.get(asset_name)
        if not digest:
            raise RuntimeError(
                f"当前系统没有受支持的固定 sing-box 运行时（platform={platform_tag}, arch={arch_tag}）。"
            )
        url = f"https://github.com/SagerNet/sing-box/releases/download/{cls._SING_BOX_TAG}/{asset_name}"
        return asset_name, url, f"sha256:{digest}"

    @classmethod
    def _pinned_hysteria_asset(cls) -> tuple[str, str, str]:
        platform_tag = cls._runtime_platform_tag()
        arch_tag = cls._runtime_arch_tag()
        suffix = ".exe" if platform_tag == "windows" else ""
        asset_name = f"hysteria-{platform_tag}-{arch_tag}{suffix}"
        digest = cls._HYSTERIA_ASSET_SHA256.get(asset_name)
        if not digest:
            raise RuntimeError(
                f"当前系统没有受支持的固定 Hysteria 运行时（platform={platform_tag}, arch={arch_tag}）。"
            )
        url = f"https://github.com/apernet/hysteria/releases/download/{cls._HYSTERIA_TAG}/{asset_name}"
        return asset_name, url, f"sha256:{digest}"

    @staticmethod
    def _download_asset(url: str, destination: Path, *, digest: str = "", runtime_name: str = "sing-box") -> None:
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
            raise RuntimeError(f"下载的 {runtime_name} 运行时校验失败")

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
                if member.issym() or member.islnk() or member.isdev():
                    raise RuntimeError(f"压缩包包含不安全链接或设备文件：{member.name}")
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
                    trusted = cls._trusted_sing_box_candidate(path)
                    if trusted is not None:
                        return trusted

            asset_name, download_url, digest = cls._pinned_sing_box_asset()
            install_dir = cls._runtime_cache_dir("sing-box") / cls._SING_BOX_TAG
            install_path = install_dir / cls._executable_name("sing-box")
            if install_path.exists() and cls._verify_cached_sing_box(install_path):
                return install_path

            logger.info("未找到 sing-box，开始下载项目内运行时：%s", asset_name)
            install_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="sing-box_", dir=str(cls._runtime_cache_dir("sing-box"))) as tmp_dir:
                tmp_root = Path(tmp_dir)
                archive_path = tmp_root / asset_name
                extract_dir = tmp_root / "extract"
                extract_dir.mkdir(parents=True, exist_ok=True)
                cls._download_asset(download_url, archive_path, digest=digest)
                cls._extract_archive(archive_path, extract_dir)
                extracted = cls._find_extracted_executable(extract_dir, "sing-box")
                if extracted is None:
                    raise RuntimeError("sing-box 压缩包已下载，但未找到可执行文件")
                shutil.copy2(extracted, install_path)
            if os.name != "nt":
                install_path.chmod(install_path.stat().st_mode | 0o755)
            cls._prepare_darwin_runtime(install_path)
            cls._write_sing_box_manifest(install_path)
            logger.info("sing-box 已缓存到项目目录：%s", install_path)
            return install_path

    @classmethod
    def _ensure_hysteria_runtime(cls) -> Path:
        with cls._RUNTIME_PREPARE_LOCK:
            for path in cls._candidate_hysteria_paths():
                if path.exists():
                    trusted = cls._trusted_hysteria_candidate(path)
                    if trusted is not None:
                        return trusted

            asset_name, download_url, digest = cls._pinned_hysteria_asset()
            install_dir = cls._runtime_cache_dir("hysteria") / cls._HYSTERIA_TAG.replace("/", "_")
            install_path = install_dir / cls._executable_name("hysteria")
            if install_path.exists() and cls._verify_cached_hysteria(install_path):
                return install_path

            logger.info("未找到 Hysteria，开始下载项目内运行时：%s", asset_name)
            install_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="hysteria_", dir=str(cls._runtime_cache_dir("hysteria"))) as tmp_dir:
                downloaded = Path(tmp_dir) / asset_name
                cls._download_asset(download_url, downloaded, digest=digest, runtime_name="Hysteria")
                shutil.copy2(downloaded, install_path)
            if os.name != "nt":
                install_path.chmod(install_path.stat().st_mode | 0o755)
            cls._prepare_darwin_runtime(install_path, "Hysteria")
            cls._write_hysteria_manifest(install_path)
            logger.info("Hysteria 已缓存到项目目录：%s", install_path)
            return install_path

    @classmethod
    def find_sing_box_executable(cls) -> Path:
        env_path = os.environ.get("SING_BOX_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                cls._verify_external_sing_box_override(p)
                trusted = cls._trusted_sing_box_candidate(p)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "sing-box")
        for p in cls._candidate_sing_box_paths():
            if p.exists():
                trusted = cls._trusted_sing_box_candidate(p)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "sing-box")
        for exe_name in cls._candidate_executable_names("sing-box"):
            which_path = shutil.which(exe_name)
            if which_path:
                candidate = Path(which_path)
                cls._verify_external_sing_box_override(candidate)
                trusted = cls._trusted_sing_box_candidate(candidate)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "sing-box")
        try:
            return cls._stage_bundled_runtime_if_needed(cls._ensure_sing_box_runtime(), "sing-box")
        except Exception as exc:
            raise RuntimeError(f"未找到 sing-box 可执行文件，且项目内自动准备失败：{exc}") from exc

    @classmethod
    def find_hysteria_executable(cls) -> Path:
        env_path = os.environ.get("HYSTERIA_PATH", "").strip()
        if env_path:
            candidate = Path(env_path)
            if candidate.exists():
                cls._verify_external_hysteria_override(candidate)
                trusted = cls._trusted_hysteria_candidate(candidate)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "hysteria")
        for candidate in cls._candidate_hysteria_paths():
            if candidate.exists():
                trusted = cls._trusted_hysteria_candidate(candidate)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "hysteria")
        for exe_name in cls._candidate_executable_names("hysteria"):
            which_path = shutil.which(exe_name)
            if which_path:
                candidate = Path(which_path)
                cls._verify_external_hysteria_override(candidate)
                trusted = cls._trusted_hysteria_candidate(candidate)
                if trusted is not None:
                    return cls._stage_bundled_runtime_if_needed(trusted, "hysteria")
        try:
            return cls._stage_bundled_runtime_if_needed(cls._ensure_hysteria_runtime(), "hysteria")
        except Exception as exc:
            raise RuntimeError(f"未找到 Hysteria 可执行文件，且项目内自动准备失败：{exc}") from exc

    @classmethod
    def find_xray_executable(cls) -> Path:
        env_path = os.environ.get("XRAY_PATH", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return cls._stage_bundled_runtime_if_needed(p, "xray")
        for exe_name in cls._candidate_executable_names("xray"):
            which_path = shutil.which(exe_name)
            if which_path:
                return cls._stage_bundled_runtime_if_needed(Path(which_path), "xray")
        for p in cls._candidate_xray_paths():
            if p.exists():
                return cls._stage_bundled_runtime_if_needed(p, "xray")
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
        try:
            os.chmod(log_path, 0o600)
        except Exception:
            pass
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
        return RuntimeError(redact_sensitive_text(msg))

    @staticmethod
    def _merge_backend_errors(errors: list[object]) -> RuntimeError:
        parts = [str(item or "").strip() for item in errors if str(item or "").strip()]
        if not parts:
            return RuntimeError("SS 代理启动失败")
        if len(parts) == 1:
            return RuntimeError(parts[0])
        return RuntimeError("SS 代理启动失败；" + "；".join(parts))

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

    def _write_hysteria_config(self, hysteria2_uri: str, listen_port: int, *, lazy: bool = False) -> Path:
        config_path = self._runtime_file_path("hysteria2_proxy", ".json")
        cfg = {
            "server": self.validate_hysteria2_uri(hysteria2_uri),
            "http": {"listen": f"127.0.0.1:{listen_port}"},
            "fastOpen": True,
        }
        if lazy:
            cfg["lazy"] = True
        _atomic_write_json(config_path, cfg)
        return config_path

    def _wait_ready(self, local_proxy_url: str, timeout_sec: float = 12.0, *, proxy_name: str = "SS") -> None:
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
        raise RuntimeError(f"{proxy_name} 代理启动失败：{last_err or '本地代理未就绪'}")

    @staticmethod
    def _wait_local_port_ready(port: int, timeout_sec: float = 6.0) -> None:
        deadline = time.time() + timeout_sec
        last_err = ""
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", int(port)), timeout=1.0):
                    return
            except Exception as exc:
                last_err = str(exc)
                time.sleep(0.2)
        raise RuntimeError(f"本地代理监听端口未就绪: 127.0.0.1:{port} ({last_err or 'timeout'})")

    def probe_backend_launch(self, backend: str) -> str:
        backend_name = str(backend or "").strip().lower()
        if backend_name not in {"xray", "sing-box", "hysteria"}:
            raise RuntimeError(f"不支持的代理后端: {backend}")

        ss_info = {
            "server": "127.0.0.1",
            "server_port": 9,
            "method": "aes-128-gcm",
            "password": "xiaojun-proxy-selftest",
            "network": "tcp",
        }
        proc = None
        config_path = None
        log_handle = None
        self._log_path = None
        try:
            listen_port = self._allocate_local_port()
            log_handle = self._open_runtime_log()
            if backend_name == "hysteria":
                exe = self.find_hysteria_executable()
                probe_uri = (
                    "hysteria2://xiaojun-proxy-selftest@127.0.0.1:9/"
                    "?insecure=1&pinSHA256=" + ("00:" * 31) + "00"
                )
                config_path = self._write_hysteria_config(probe_uri, listen_port, lazy=True)
                cmd = [str(exe), "-c", str(config_path)]
            elif backend_name == "xray":
                exe = self.find_xray_executable()
                config_path = self._write_xray_config(ss_info, listen_port)
                cmd = [str(exe), "run", "-c", str(config_path)]
            else:
                exe = self.find_sing_box_executable()
                config_path = self._write_sing_box_config(ss_info, listen_port)
                cmd = [str(exe), "run", "-c", str(config_path)]
            proc = subprocess.Popen(
                cmd,
                stdout=log_handle,
                stderr=log_handle,
                **self._hidden_process_kwargs(),
            )
            self._wait_local_port_ready(listen_port)
            return Path(exe).name
        except Exception as exc:
            raise self._format_backend_error(backend_name, exc) from exc
        finally:
            if proc is not None:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
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
            self._log_path = None

    def _ensure_hysteria2_proxy_locked(self, raw: str) -> str:
        hysteria2_uri = self.validate_hysteria2_uri(raw)
        listen_port = self._allocate_local_port()
        proc = None
        config_path = None
        runtime_log_handle = None
        try:
            runtime_log_handle = self._open_runtime_log()
            executable = self.find_hysteria_executable()
            config_path = self._write_hysteria_config(hysteria2_uri, listen_port)
            proc = subprocess.Popen(
                [str(executable), "-c", str(config_path)],
                stdout=runtime_log_handle,
                stderr=runtime_log_handle,
                **self._hidden_process_kwargs(),
            )
            local_proxy_url = f"http://127.0.0.1:{listen_port}"
            self._wait_ready(local_proxy_url, proxy_name="Hysteria2")
        except Exception as exc:
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
            if config_path is not None:
                try:
                    Path(config_path).unlink(missing_ok=True)
                except Exception:
                    pass
            raise self._format_backend_error("Hysteria2", exc) from exc

        self._proc = proc
        self._raw_source = raw
        self._local_proxy_url = local_proxy_url
        self._config_path = config_path
        self._log_handle = runtime_log_handle
        self._backend = "hysteria"
        return local_proxy_url

    def ensure_proxy(self, proxy_uri: str) -> str:
        raw = str(proxy_uri or "").strip()
        if not raw:
            return ""
        with self._lock:
            if raw == self._raw_source and self._is_process_alive(self._proc) and self._local_proxy_url:
                return self._local_proxy_url
            self._stop_locked()
            if self.is_hysteria2_uri(raw):
                return self._ensure_hysteria2_proxy_locked(raw)
            ss_info = self.parse_ss_uri(raw)
            listen_port = self._allocate_local_port()
            last_exc = None
            backend_errors: list[str] = []
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
                    backend_errors.append(str(last_exc))
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
                raise self._merge_backend_errors(backend_errors) from last_exc
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
                "source_configured": bool(self._raw_source),
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
        explicit_proxy = ""
        if proxies:
            explicit_proxy = str(proxies.get("https") or proxies.get("http") or "").strip()
        configure_requests_session_for_proxy(
            session,
            explicit_proxy=explicit_proxy,
            target_url=url,
            allow_system_proxy=bool(allow_system_proxy) and not bool(explicit_proxy),
        )
        return session.get(url, timeout=timeout, headers=headers or {})
    finally:
        session.close()
