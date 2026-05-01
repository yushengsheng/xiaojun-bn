#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from app_paths import APP_DIR, BUNDLE_DIR


APP_TITLE_BASE = "Binance 自动交易机器人"
VERSION_FILE_NAME = "version.txt"
VERSION_ENV_VAR = "XIAOJUN_APP_VERSION"


def _normalize_version_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text


def _read_version_file(path: Path) -> str:
    try:
        return _normalize_version_text(path.read_text(encoding="utf-8"))
    except Exception:
        return ""


def _version_from_git(repo_dir: Path) -> str:
    commands = [
        ["git", "-C", str(repo_dir), "describe", "--tags", "--exact-match", "HEAD"],
        ["git", "-C", str(repo_dir), "describe", "--tags", "--abbrev=0"],
    ]
    for cmd in commands:
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=2,
            )
        except Exception:
            continue
        version_text = _normalize_version_text(result.stdout)
        if version_text:
            return version_text
    return ""


def get_app_version() -> str:
    env_version = _normalize_version_text(os.environ.get(VERSION_ENV_VAR, ""))
    if env_version:
        return env_version

    for base_dir in (APP_DIR, BUNDLE_DIR):
        version_text = _read_version_file(Path(base_dir) / VERSION_FILE_NAME)
        if version_text:
            return version_text

    return _version_from_git(APP_DIR)


def get_app_window_title() -> str:
    version_text = get_app_version()
    if not version_text:
        return APP_TITLE_BASE
    return f"{APP_TITLE_BASE}({version_text})"
