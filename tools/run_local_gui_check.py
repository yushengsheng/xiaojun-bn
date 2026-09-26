#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import MutableMapping
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHILD_ENV_KEY = "XIAOJUN_GUI_CHECK_CHILD"
PERSISTENT_REGRESSION_PYTHON = ROOT / ".venv-regression" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
VERIFIED_SING_BOX_PATH = ROOT / "data" / "proxy_runtimes" / "sing-box" / "v1.13.5" / "sing-box"
VERIFIED_SING_BOX_SHA256 = "44c9311a332c66a9785ffecc318fc2bddda9dbd61be23b462b526cbc4ad4f207"
VERIFIED_HYSTERIA_PATH = ROOT / "data" / "proxy_runtimes" / "hysteria" / "app_v2.9.2" / "hysteria"
VERIFIED_HYSTERIA_SHA256 = "16a978d3c1db6658cb2fb086f580a63ebbca57110be62cc06ff7258c204ec24f"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _candidate_pythons() -> list[Path]:
    candidates: list[Path] = []
    env_python = os.environ.get("XIAOJUN_GUI_PYTHON", "").strip()
    if env_python:
        candidates.append(Path(env_python))
    candidates.extend(
        [
            PERSISTENT_REGRESSION_PYTHON,
            ROOT / ".venv" / "bin" / "python",
            ROOT / ".venv" / "Scripts" / "python.exe",
            Path(sys.executable),
        ]
    )
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.is_file() and os.access(candidate, os.X_OK):
            unique.append(candidate)
    return unique


def _select_python() -> Path:
    candidates = _candidate_pythons()
    if not candidates:
        raise RuntimeError("未找到可用 Python；请设置 XIAOJUN_GUI_PYTHON 指向已验证的 GUI 回归环境")
    return candidates[0]


def _apply_verified_runtime_defaults(env: MutableMapping[str, str]) -> None:
    existing_path = str(env.get("SING_BOX_PATH", "") or "").strip()
    if not existing_path and VERIFIED_SING_BOX_PATH.exists():
        env["SING_BOX_PATH"] = str(VERIFIED_SING_BOX_PATH)
        existing_path = str(VERIFIED_SING_BOX_PATH)

    existing_sha = str(env.get("SING_BOX_EXECUTABLE_SHA256", "") or "").strip()
    if not existing_sha and existing_path == str(VERIFIED_SING_BOX_PATH):
        env["SING_BOX_EXECUTABLE_SHA256"] = VERIFIED_SING_BOX_SHA256

    if not str(env.get("HYSTERIA_PATH", "") or "").strip() and VERIFIED_HYSTERIA_PATH.exists():
        env["HYSTERIA_PATH"] = str(VERIFIED_HYSTERIA_PATH)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(262144), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stage_verified_proxy_runtimes(env: MutableMapping[str, str], stage_dir: Path) -> None:
    """Run the local selftest from verified copies, never touching app cache manifests."""
    for backend, source, expected in (
        ("SING_BOX", VERIFIED_SING_BOX_PATH, VERIFIED_SING_BOX_SHA256),
        ("HYSTERIA", VERIFIED_HYSTERIA_PATH, VERIFIED_HYSTERIA_SHA256),
    ):
        if not source.is_file():
            raise RuntimeError(f"本地 GUI 自检缺少已验证的 {backend} 运行时：{source}")
        actual = _sha256_file(source)
        if actual != expected:
            raise RuntimeError(f"本地 GUI 自检的 {backend} 运行时哈希不匹配：{source}")
        staged = stage_dir / backend.lower() / source.name
        staged.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, staged)
        if _sha256_file(staged) != expected:
            raise RuntimeError(f"本地 GUI 自检的 {backend} 运行时复制校验失败")
        env[f"{backend}_PATH"] = str(staged)
        env[f"{backend}_EXECUTABLE_SHA256"] = expected


def _run_gui_smoke() -> int:
    from tkinter import messagebox

    import exchange_app_accounts
    import exchange_app_base
    import exchange_app_config
    import exchange_app_log_view
    from exchange_app import App
    from secure_delete import SecureDeleteReport
    from selftest_gui_checks import pump_tk_events, temporary_attr_overrides

    with tempfile.TemporaryDirectory(prefix="xiaojun-local-gui-smoke-") as tmpdir:
        tmp_root = Path(tmpdir)
        accounts_path = tmp_root / "accounts.json"
        exchange_journal_path = tmp_root / "exchange_operations.json"
        strategy_path = tmp_root / "exchange_strategy_settings.json"
        proxy_path = tmp_root / "exchange_proxy_settings.json"
        withdraw_result_path = tmp_root / "withdraw_success.txt"
        total_asset_result_path = tmp_root / "total_asset_result.txt"
        patches = [
            (exchange_app_log_view.ExchangeAppLogViewMixin, "update_ip", lambda self, schedule_next=True: None),
            (exchange_app_base, "DATA_FILE", accounts_path),
            (exchange_app_base, "EXCHANGE_OPERATION_JOURNAL_FILE", exchange_journal_path),
            (exchange_app_base, "STRATEGY_CONFIG_FILE", strategy_path),
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
            (messagebox, "showinfo", lambda *_args, **_kwargs: True),
            (messagebox, "showwarning", lambda *_args, **_kwargs: True),
            (messagebox, "showerror", lambda *_args, **_kwargs: True),
            (messagebox, "askyesno", lambda *_args, **_kwargs: True),
        ]
        with temporary_attr_overrides(patches):
            app = None
            try:
                app = App()
                app.withdraw()
                pump_tk_events(app, timeout=5.0)
                print("GUI CHECK OK", flush=True)
                return 0
            finally:
                if app is not None:
                    try:
                        app.destroy()
                    except Exception:
                        pass


def _run_child(args: argparse.Namespace) -> int:
    env = os.environ.copy()
    env.setdefault("XIAOJUN_APP_LOCK_BYPASS", "1")
    _apply_verified_runtime_defaults(env)
    with tempfile.TemporaryDirectory(prefix=".regression-proxy-", dir=ROOT) as tmpdir:
        if sys.platform == "darwin":
            _stage_verified_proxy_runtimes(env, Path(tmpdir))
        os.environ.update(env)
        if args.full_selftest:
            cmd = [sys.executable, "小军bn.py", "--selftest", "--selftest-gui"]
        elif args.regression:
            cmd = [sys.executable, "tools/run_regression.py", "--gui", "--strict-pip-check"]
        else:
            return _run_gui_smoke()
        return subprocess.run(cmd, cwd=ROOT, env=env, check=False).returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run local GUI checks with the verified GUI Python environment.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full-selftest", action="store_true", help="Run 小军bn.py --selftest --selftest-gui.")
    mode.add_argument("--regression", action="store_true", help="Run tools/run_regression.py --gui --strict-pip-check.")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.child or os.environ.get(CHILD_ENV_KEY) == "1":
        return _run_child(args)

    python = _select_python()
    env = os.environ.copy()
    env[CHILD_ENV_KEY] = "1"
    env.setdefault("XIAOJUN_APP_LOCK_BYPASS", "1")
    _apply_verified_runtime_defaults(env)
    cmd = [str(python), str(Path(__file__).resolve()), "--child"]
    if args.full_selftest:
        cmd.append("--full-selftest")
    if args.regression:
        cmd.append("--regression")
    print(f"Using GUI Python: {python}", flush=True)
    result = subprocess.run(cmd, cwd=ROOT, env=env, check=False)
    if result.returncode == 134:
        print(
            "GUI CHECK FAILED: Python/Tk 以 134 退出。请确认这是脱离沙箱运行，并设置 "
            "XIAOJUN_GUI_PYTHON 指向可打开 Tk 的本地回归环境。",
            file=sys.stderr,
        )
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
