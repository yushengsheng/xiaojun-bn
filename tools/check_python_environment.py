#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _is_virtual_environment() -> bool:
    return bool(getattr(sys, "base_prefix", sys.prefix) != sys.prefix or getattr(sys, "real_prefix", None))


def _requirements_are_pinned(path: Path) -> tuple[bool, list[str]]:
    issues: list[str] = []
    if not path.exists():
        return False, [f"{path.name} is missing"]
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            continue
        if "==" not in line:
            issues.append(f"{path.name}:{line_no}: dependency is not pinned: {line}")
    return not issues, issues


def _run_pip_check() -> tuple[int, str]:
    proc = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check the local Python environment used by xiaojun-bn.")
    parser.add_argument("--skip-pip-check", action="store_true", help="Do not run python -m pip check.")
    parser.add_argument("--strict-pip-check", action="store_true", help="Fail if python -m pip check reports conflicts.")
    args = parser.parse_args(argv)

    print(f"python={sys.executable}")
    print(f"virtualenv={'yes' if _is_virtual_environment() else 'no'}")
    if not _is_virtual_environment():
        print("warning=not running inside a virtual environment")

    ok = True
    for requirement_file in (ROOT / "requirements.txt", ROOT / "requirements-build.txt"):
        pinned, issues = _requirements_are_pinned(requirement_file)
        print(f"{requirement_file.name}-pinned={'yes' if pinned else 'no'}")
        for issue in issues:
            print(f"warning={issue}")
        ok = ok and pinned

    if not args.skip_pip_check:
        code, output = _run_pip_check()
        print(f"pip-check={'ok' if code == 0 else 'failed'}")
        if output:
            print(output)
        if code != 0 and args.strict_pip_check:
            ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
