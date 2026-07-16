#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV_DIR = ROOT / ".venv-regression"
LOCK_FILE = ROOT / "requirements-build.lock"
LOCK_MARKER = ENV_DIR / ".requirements-build.sha256"
BASE_PYTHON_ENV = "XIAOJUN_REGRESSION_BASE_PYTHON"
REQUIRED_PYTHON = (3, 11)
BUNDLED_SING_BOX = ROOT / "data" / "proxy_runtimes" / "sing-box" / "v1.13.5" / "sing-box"
BUNDLED_SING_BOX_SHA256 = "44c9311a332c66a9785ffecc318fc2bddda9dbd61be23b462b526cbc4ad4f207"


def _environment_python(env_dir: Path = ENV_DIR) -> Path:
    if os.name == "nt":
        return env_dir / "Scripts" / "python.exe"
    return env_dir / "bin" / "python"


def _lock_digest(lock_file: Path = LOCK_FILE) -> str:
    digest = hashlib.sha256()
    with lock_file.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _locked_versions(lock_file: Path = LOCK_FILE) -> dict[str, str]:
    versions: dict[str, str] = {}
    for raw_line in lock_file.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_.-]+)==([^\s\\]+)", raw_line)
        if match:
            name = re.sub(r"[-_.]+", "-", match.group(1)).lower()
            versions[name] = match.group(2)
    return versions


def _run_capture(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def _probe_python(command: list[str]) -> dict[str, object] | None:
    probe = (
        "import json,sys; "
        "print(json.dumps({'version': list(sys.version_info[:2]), "
        "'prefix': sys.prefix, 'base_prefix': sys.base_prefix}))"
    )
    try:
        result = _run_capture([*command, "-c", probe])
        if result.returncode != 0:
            return None
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (OSError, ValueError, IndexError, json.JSONDecodeError):
        return None


def _installed_versions(python: Path) -> dict[str, str] | None:
    probe = (
        "import importlib.metadata as m,json,re; "
        "print(json.dumps({re.sub(r'[-_.]+', '-', d.metadata['Name']).lower(): d.version "
        "for d in m.distributions() if d.metadata.get('Name')}))"
    )
    try:
        result = _run_capture([str(python), "-c", probe])
        if result.returncode != 0:
            return None
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (OSError, ValueError, IndexError, json.JSONDecodeError):
        return None


def _environment_status(
    env_dir: Path = ENV_DIR,
    lock_file: Path = LOCK_FILE,
    marker_file: Path = LOCK_MARKER,
) -> tuple[bool, str]:
    python = _environment_python(env_dir)
    if not (env_dir / "pyvenv.cfg").is_file() or not python.is_file():
        return False, "虚拟环境文件不完整"

    probe = _probe_python([str(python)])
    if probe is None:
        return False, "虚拟环境 Python 无法启动"
    if tuple(probe.get("version", ())) != REQUIRED_PYTHON:
        return False, "虚拟环境不是 Python 3.11"
    if probe.get("prefix") == probe.get("base_prefix"):
        return False, "Python 实际指向系统环境"
    try:
        if Path(str(probe.get("prefix", ""))).resolve() != env_dir.resolve():
            return False, "Python 指向了其他虚拟环境"
    except OSError:
        return False, "虚拟环境路径无效"

    try:
        expected_digest = _lock_digest(lock_file)
        installed_digest = marker_file.read_text(encoding="ascii").strip()
    except OSError:
        return False, "缺少依赖锁指纹"
    if installed_digest != expected_digest:
        return False, "依赖锁已变化"

    installed_versions = _installed_versions(python)
    if installed_versions is None:
        return False, "无法读取已安装依赖版本"
    locked_versions = _locked_versions(lock_file)
    if any(installed_versions.get(name) != version for name, version in locked_versions.items()):
        return False, "已安装依赖与锁文件不一致"

    dependency_probe = _run_capture(
        [
            str(python),
            "-c",
            "import PyInstaller, cryptography, eth_account, pip_audit, requests, socks",
        ]
    )
    if dependency_probe.returncode != 0:
        return False, "回归依赖不完整"
    pip_check = _run_capture([str(python), "-m", "pip", "check"])
    if pip_check.returncode != 0:
        return False, "回归依赖存在冲突"
    return True, "环境有效"


def _base_python_candidates() -> list[list[str]]:
    candidates: list[list[str]] = []
    configured = os.environ.get(BASE_PYTHON_ENV, "").strip()
    if configured:
        candidates.append([configured])
    if sys.version_info[:2] == REQUIRED_PYTHON:
        candidates.append([sys.executable])
    for name in ("python3.11", "python3"):
        path = shutil.which(name)
        if path:
            candidates.append([path])
    if sys.platform == "darwin":
        candidates.extend(
            [[path] for path in (
                "/opt/homebrew/bin/python3.11",
                "/opt/homebrew/opt/python@3.11/bin/python3.11",
                "/usr/local/bin/python3.11",
            ) if Path(path).is_file()]
        )
    if os.name == "nt":
        launcher = shutil.which("py")
        if launcher:
            candidates.append([launcher, "-3.11"])

    unique: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for candidate in candidates:
        try:
            candidate_path = Path(candidate[0]).absolute()
            if candidate_path == ENV_DIR or ENV_DIR in candidate_path.parents:
                continue
        except OSError:
            continue
        key = tuple(candidate)
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def _select_base_python() -> list[str]:
    for candidate in _base_python_candidates():
        probe = _probe_python(candidate)
        if probe is not None and tuple(probe.get("version", ())) == REQUIRED_PYTHON:
            return candidate
    raise RuntimeError(
        "未找到 Python 3.11；请安装 Python 3.11，或设置 "
        f"{BASE_PYTHON_ENV} 指向 Python 3.11"
    )


def _remove_managed_environment(env_dir: Path = ENV_DIR) -> None:
    if env_dir.resolve().parent != ROOT.resolve() or env_dir.name != ".venv-regression":
        raise RuntimeError(f"拒绝清理非托管目录: {env_dir}")
    if env_dir.is_symlink() or env_dir.is_file():
        env_dir.unlink()
    elif env_dir.exists():
        shutil.rmtree(env_dir)


def _rebuild_environment() -> Path:
    base_python = _select_base_python()
    _remove_managed_environment()
    print(f"重建固定回归环境: {ENV_DIR}", flush=True)
    subprocess.run([*base_python, "-m", "venv", str(ENV_DIR)], cwd=ROOT, check=True)

    python = _environment_python()
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--require-hashes",
            "-r",
            str(LOCK_FILE),
        ],
        cwd=ROOT,
        check=True,
    )
    subprocess.run([str(python), "-m", "pip", "check"], cwd=ROOT, check=True)
    LOCK_MARKER.write_text(_lock_digest() + "\n", encoding="ascii")
    return python


def ensure_regression_environment() -> Path:
    valid, reason = _environment_status()
    if valid:
        print(f"复用固定回归环境: {ENV_DIR}", flush=True)
        return _environment_python()
    print(f"固定回归环境需要重建: {reason}", flush=True)
    python = _rebuild_environment()
    valid, reason = _environment_status()
    if not valid:
        raise RuntimeError(f"固定回归环境重建后校验失败: {reason}")
    return python


def main() -> int:
    try:
        python = ensure_regression_environment()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"回归环境准备失败: {exc}", file=sys.stderr)
        return 1

    env = os.environ.copy()
    env["XIAOJUN_GUI_PYTHON"] = str(python)
    if BUNDLED_SING_BOX.is_file():
        env.setdefault("SING_BOX_PATH", str(BUNDLED_SING_BOX))
        env.setdefault("SING_BOX_EXECUTABLE_SHA256", BUNDLED_SING_BOX_SHA256)
    command = [str(python), "tools/run_local_gui_check.py", "--regression"]
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
