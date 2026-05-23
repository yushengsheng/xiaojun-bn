#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import os
import queue
import re
import threading
import time
from pathlib import Path

from app_paths import LOG_DIR, LOG_FILE_PATH
from sensitive_data import redact_sensitive_text
from shared_utils import LOG_MAX_ROWS


log_queue: "queue.Queue[str]" = queue.Queue()
LOG_FILE_RUNTIME_PREFIX = "exchange_runtime"
LOG_FILE_RETENTION_COUNT = 10
LOG_FILE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_TOTAL_SIZE_LIMIT_BYTES = 50 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 3
COMPAT_LOG_MAX_BYTES = 5 * 1024 * 1024
COMPAT_LOG_BACKUP_COUNT = 3
EXCHANGE_LOG_MAX_ROWS = LOG_MAX_ROWS
_ACCOUNT_LOG_CONTEXT = threading.local()
_ACCOUNT_TEXT_RE = re.compile(r"账号\s*[#＃]?\s*\d+")


def set_exchange_account_log_index(account_index: object) -> None:
    text = str(account_index or "").strip()
    if text:
        _ACCOUNT_LOG_CONTEXT.account_index = text
    else:
        clear_exchange_account_log_index()


def clear_exchange_account_log_index() -> None:
    try:
        del _ACCOUNT_LOG_CONTEXT.account_index
    except AttributeError:
        pass


def current_exchange_account_log_index() -> str:
    return str(getattr(_ACCOUNT_LOG_CONTEXT, "account_index", "") or "").strip()


class SensitiveFormatter(logging.Formatter):
    def format(self, record):
        account_index = current_exchange_account_log_index()
        message = str(record.getMessage() or "")
        if account_index and not _ACCOUNT_TEXT_RE.search(message):
            record.account_prefix = f"[账号 #{account_index}] "
        else:
            record.account_prefix = ""
        return redact_sensitive_text(super().format(record))


_formatter = SensitiveFormatter("%(asctime)s [%(levelname)s] %(account_prefix)s%(message)s")
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
    for path in LOG_DIR.glob(f"{LOG_FILE_RUNTIME_PREFIX}_*.log*"):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append((path, stat.st_mtime, stat.st_size))

    now = time.time()
    entries.sort(key=lambda item: item[1], reverse=True)
    kept_count = 0
    kept_size = 0
    for path, mtime, size in entries:
        if current_path is not None and path == current_path:
            kept_count += 1
            kept_size += size
            continue
        if (now - mtime) > LOG_FILE_MAX_AGE_SECONDS:
            try:
                path.unlink()
            except OSError:
                pass
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
    _runtime_file_handler = RotatingFileHandler(
        runtime_log_path,
        maxBytes=LOG_FILE_MAX_BYTES,
        backupCount=LOG_FILE_BACKUP_COUNT,
        encoding="utf-8",
    )
    _runtime_file_handler.setFormatter(_formatter)
    logger.addHandler(_runtime_file_handler)
    _prune_runtime_logs(runtime_log_path)
except Exception as exc:
    print(f"无法创建日志文件: {exc}")

try:
    _compat_file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        mode="w",
        maxBytes=COMPAT_LOG_MAX_BYTES,
        backupCount=COMPAT_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    _compat_file_handler.setFormatter(_formatter)
    logger.addHandler(_compat_file_handler)
except Exception as exc:
    print(f"无法创建兼容日志文件: {exc}")

if runtime_log_path is not None:
    logger.info("当前运行日志文件：%s", runtime_log_path)
