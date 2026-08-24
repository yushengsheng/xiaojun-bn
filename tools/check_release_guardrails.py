#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    file_path = ROOT / path
    if not file_path.exists():
        raise AssertionError(f"missing required file: {path}")
    return file_path.read_text(encoding="utf-8")


def _require(text: str, needle: str, *, source: str) -> None:
    if needle not in text:
        raise AssertionError(f"{source} must contain {needle!r}")


def _proxy_runtime_pins(workflow: str) -> dict[str, str]:
    names = {
        "XRAY_TAG",
        "XRAY_ASSET",
        "XRAY_SHA256",
        "SING_BOX_TAG",
        "SING_BOX_ASSET",
        "SING_BOX_SHA256",
        "HYSTERIA_TAG",
        "HYSTERIA_ASSET",
        "HYSTERIA_SHA256",
    }
    pins: dict[str, str] = {}
    for match in re.finditer(r"^\s*([A-Z0-9_]+):\s*([^\s#]+)\s*$", workflow, flags=re.MULTILINE):
        if match.group(1) in names:
            pins[match.group(1)] = match.group(2).strip("'\"")
    return pins


def check_release_guardrails() -> None:
    guardrails = _read("RELEASE_GUARDRAILS.md")
    release_doc = _read("RELEASE.md")
    release_workflow = _read(".github/workflows/release-windows-exe.yml")
    ci_workflow = _read(".github/workflows/ci.yml")
    regression = _read("tools/run_regression.py")
    full_regression = _read("tools/run_full_regression.py")
    local_gui_check = _read("tools/run_local_gui_check.py")

    for needle in (
        "Python 意外退出",
        "exit 134",
        "tools/run_local_gui_check.py",
        "XIAOJUN_GUI_PYTHON",
        "--selftest --selftest-gui",
        "GitHub Actions",
        "SING_BOX_SHA256",
        "HYSTERIA_SHA256",
        "不允许发布",
        "tools/check_release_guardrails.py",
    ):
        _require(guardrails, needle, source="RELEASE_GUARDRAILS.md")

    for needle in (
        "RELEASE_GUARDRAILS.md",
        "GitHub Actions",
        "--selftest --selftest-gui",
        "requirements-build.lock",
        "SING_BOX_SHA256",
        "HYSTERIA_SHA256",
        "check_sensitive_artifacts.py",
    ):
        _require(release_doc, needle, source="RELEASE.md")

    for needle in (
        "runs-on: windows-latest",
        "python -m pip install --require-hashes -r requirements-build.lock",
        "XRAY_SHA256",
        "SING_BOX_SHA256",
        "HYSTERIA_SHA256",
        "python tools/run_regression.py --skip-pip-check --skip-app-selftest",
        "Invoke-Selftest",
        "'--selftest', '--selftest-gui'",
        '@("geoip.dat", "geosite.dat")',
        "python tools/check_sensitive_artifacts.py --root $portableStage --include-direct-children",
        "refusing to overwrite published assets",
    ):
        _require(release_workflow, needle, source=".github/workflows/release-windows-exe.yml")
    if "--clobber" in release_workflow:
        raise AssertionError(".github/workflows/release-windows-exe.yml must not overwrite published assets")

    for needle in (
        "windows-latest",
        "python tools\\run_regression.py --gui --strict-pip-check",
        "XRAY_SHA256",
        "SING_BOX_SHA256",
        "HYSTERIA_SHA256",
        '@("geoip.dat", "geosite.dat")',
    ):
        _require(ci_workflow, needle, source=".github/workflows/ci.yml")
    release_pins = _proxy_runtime_pins(release_workflow)
    ci_pins = _proxy_runtime_pins(ci_workflow)
    if len(release_pins) != 9 or ci_pins != release_pins:
        raise AssertionError("CI and release proxy runtime pins must match exactly")

    _require(regression, "tools/check_release_guardrails.py", source="tools/run_regression.py")
    for needle in (
        ".venv-regression",
        "requirements-build.lock",
        "--require-hashes",
        "tools/run_local_gui_check.py",
        "--regression",
    ):
        _require(full_regression, needle, source="tools/run_full_regression.py")
    for needle in (
        "XIAOJUN_GUI_PYTHON",
        ".venv-regression",
        "proxy_runtimes",
        "SING_BOX_EXECUTABLE_SHA256",
        "HYSTERIA_PATH",
        "--full-selftest",
        "--regression",
        "GUI CHECK FAILED",
    ):
        _require(local_gui_check, needle, source="tools/run_local_gui_check.py")


def main() -> int:
    try:
        check_release_guardrails()
    except AssertionError as exc:
        print(f"RELEASE GUARDRAIL FAILED: {exc}", file=sys.stderr)
        return 1
    print("Release guardrails OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
