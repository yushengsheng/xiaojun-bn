#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _display_command(command: list[str]) -> str:
    """Return an ASCII-safe command representation for legacy Windows consoles."""
    return " ".join(item.encode("ascii", errors="backslashreplace").decode("ascii") for item in command)


def _run(label: str, command: list[str]) -> None:
    print(f"\n==> {label}", flush=True)
    print(_display_command(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _git_available() -> bool:
    return (ROOT / ".git").exists()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run xiaojun-bn regression checks.")
    parser.add_argument("--gui", action="store_true", help="Include GUI selftest checks.")
    parser.add_argument("--online", action="store_true", help="Include ETH/BSC public RPC checks.")
    parser.add_argument("--skip-app-selftest", action="store_true", help="Skip 小军bn.py --selftest.")
    parser.add_argument("--skip-pip-check", action="store_true", help="Skip python -m pip check in environment check.")
    parser.add_argument("--skip-vulnerability-audit", action="store_true", help="Skip locked dependency vulnerability audit.")
    parser.add_argument("--strict-pip-check", action="store_true", help="Fail on pip check dependency conflicts.")
    args = parser.parse_args(argv)

    python = sys.executable
    env_args = [python, "tools/check_python_environment.py"]
    if args.skip_pip_check:
        env_args.append("--skip-pip-check")
    if args.strict_pip_check:
        env_args.append("--strict-pip-check")

    _run("environment", env_args)
    _run("compileall", [python, "-m", "compileall", "-q", "."])
    _run("unit tests", [python, "-m", "unittest", "discover", "-s", "tests"])
    if _git_available():
        _run("diff whitespace", ["git", "diff", "--check"])
    _run("sensitive artifact check", [python, "tools/check_sensitive_artifacts.py", "--git-only"])
    _run("pyinstaller args", [python, "tools/pyinstaller_common_args.py"])
    if not args.skip_vulnerability_audit:
        _run(
            "runtime dependency vulnerability audit",
            [python, "-m", "pip_audit", "--strict", "--disable-pip", "-r", "requirements.lock"],
        )
        _run(
            "build dependency vulnerability audit",
            [python, "-m", "pip_audit", "--strict", "--disable-pip", "-r", "requirements-build.lock"],
        )

    if not args.skip_app_selftest:
        selftest_args = [python, "小军bn.py", "--selftest"]
        if args.gui:
            selftest_args.append("--selftest-gui")
        if args.online:
            selftest_args.append("--selftest-online")
        _run("application selftest", selftest_args)
    print("\nREGRESSION OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
