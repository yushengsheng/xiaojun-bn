#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from app_paths import LOG_DIR
from sensitive_data import redact_sensitive_text


_LOG_RETENTION_COUNT = 10
_LOG_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
_LOG_LOCK = threading.RLock()
_RUNTIME_LOG_PATH: Path | None = None
_ACTIVITY_LOG_PATH: Path | None = None


def _prune_logs(prefix: str, current_path: Path | None) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return
    entries: list[tuple[Path, float]] = []
    for path in LOG_DIR.glob(f"{prefix}_*.log*"):
        try:
            entries.append((path, path.stat().st_mtime))
        except OSError:
            continue
    now = time.time()
    entries.sort(key=lambda item: item[1], reverse=True)
    kept = 0
    for path, mtime in entries:
        if current_path is not None and path == current_path:
            kept += 1
            continue
        if (now - mtime) > _LOG_MAX_AGE_SECONDS or kept >= _LOG_RETENTION_COUNT:
            try:
                path.unlink()
            except OSError:
                pass
            continue
        kept += 1


def _runtime_path(prefix: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{prefix}_{timestamp}_{os.getpid()}.log"


def _get_runtime_log_path() -> Path:
    global _RUNTIME_LOG_PATH
    if _RUNTIME_LOG_PATH is None:
        _RUNTIME_LOG_PATH = _runtime_path("onchain_runtime")
        _prune_logs("onchain_runtime", _RUNTIME_LOG_PATH)
    return _RUNTIME_LOG_PATH


def _get_activity_log_path() -> Path:
    global _ACTIVITY_LOG_PATH
    if _ACTIVITY_LOG_PATH is None:
        _ACTIVITY_LOG_PATH = _runtime_path("onchain_activity")
        _prune_logs("onchain_activity", _ACTIVITY_LOG_PATH)
    return _ACTIVITY_LOG_PATH


def _append_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
        fh.write("\n")


def append_onchain_runtime_log(text: object) -> None:
    message = redact_sensitive_text(str(text or "").strip())
    if not message:
        return
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [INFO] {message}"
    try:
        with _LOG_LOCK:
            _append_line(_get_runtime_log_path(), line)
    except Exception:
        pass


def append_onchain_activity(event: str, **fields: object) -> None:
    payload: dict[str, object] = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "event": str(event or "").strip() or "event",
    }
    payload.update(fields)
    try:
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        line = redact_sensitive_text(line)
        with _LOG_LOCK:
            _append_line(_get_activity_log_path(), line)
    except Exception:
        pass


__all__ = [
    "append_onchain_activity",
    "append_onchain_runtime_log",
]
