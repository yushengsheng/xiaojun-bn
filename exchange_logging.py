#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import os
import queue
import time
from pathlib import Path

from app_paths import LOG_DIR, LOG_FILE_PATH
from shared_utils import LOG_MAX_ROWS


log_queue: "queue.Queue[str]" = queue.Queue()
LOG_FILE_RUNTIME_PREFIX = "exchange_runtime"
LOG_FILE_RETENTION_COUNT = 20
LOG_FILE_TOTAL_SIZE_LIMIT_BYTES = 200 * 1024 * 1024
EXCHANGE_LOG_MAX_ROWS = LOG_MAX_ROWS

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

    entries: list[tuple[Path, float, int]] = []
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

runtime_log_path: Path | None = None
try:
    _prune_runtime_logs()
    runtime_log_path = _create_runtime_log_path()
    _runtime_file_handler = logging.FileHandler(runtime_log_path, encoding="utf-8")
    _runtime_file_handler.setFormatter(_formatter)
    logger.addHandler(_runtime_file_handler)
    _prune_runtime_logs(runtime_log_path)
except Exception as exc:
    print(f"无法创建日志文件: {exc}")

try:
    _compat_file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
    _compat_file_handler.setFormatter(_formatter)
    logger.addHandler(_compat_file_handler)
except Exception as exc:
    print(f"无法创建兼容日志文件: {exc}")

if runtime_log_path is not None:
    logger.info("当前运行日志文件：%s", runtime_log_path)
