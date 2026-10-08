#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import queue
import re
import threading
from pathlib import Path

from app_paths import LOG_DIR, LOG_FILE_PATH
from daily_logging import BeijingFormatter, DailyFileHandler, log_day, prune_logs
from secure_delete import SecureDeleteReport, secure_unlink_all
from sensitive_data import redact_sensitive_text
from shared_utils import LOG_MAX_ROWS


log_queue: "queue.Queue[object]" = queue.Queue(maxsize=2000)
LOG_FILE_RUNTIME_PREFIX = "exchange_runtime"
EXCHANGE_LOG_MAX_ROWS = LOG_MAX_ROWS
_ACCOUNT_LOG_CONTEXT = threading.local()
_ACCOUNT_TEXT_RE = re.compile(r"账号\s*[#＃]?\s*\d+")
_TRANSIENT_RETRY_MARKERS = (
    "将在",
    "重试中",
    "准备重试",
    "临时RPC错误",
    "临时 RPC 错误",
    "网络请求失败",
)


def is_transient_retry_log_message(message: object) -> bool:
    text = str(message or "")
    if not text:
        return False
    if "重试" not in text and "临时" not in text:
        return False
    if "重试" in text and ("仍然失败" in text or "最终失败" in text):
        return False
    return any(marker in text for marker in _TRANSIENT_RETRY_MARKERS)


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


class SensitiveFormatter(BeijingFormatter):
    def format(self, record):
        account_index = current_exchange_account_log_index()
        message = str(record.getMessage() or "")
        if account_index and not _ACCOUNT_TEXT_RE.search(message):
            record.account_prefix = f"[账号 #{account_index}] "
        else:
            record.account_prefix = ""
        return redact_sensitive_text(super().format(record))


class TkLogEntry:
    __slots__ = ("message", "levelno", "levelname")

    def __init__(self, message: str, *, levelno: int = logging.INFO, levelname: str = "INFO"):
        self.message = str(message or "")
        try:
            self.levelno = int(levelno)
        except Exception:
            self.levelno = logging.INFO
        self.levelname = str(levelname or "")

    @property
    def is_problem(self) -> bool:
        if is_transient_retry_log_message(self.message):
            return False
        return self.levelno >= logging.WARNING

    def __str__(self) -> str:
        return self.message


_LOG_RECORD_FORMAT = "%(asctime)s [%(levelname)s] %(account_prefix)s%(message)s"
_formatter = SensitiveFormatter(_LOG_RECORD_FORMAT)
_tk_formatter = SensitiveFormatter(_LOG_RECORD_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
logger.propagate = False
_file_handler_lock = threading.RLock()


class TkLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            entry = TkLogEntry(msg, levelno=record.levelno, levelname=record.levelname)
            try:
                log_queue.put_nowait(entry)
            except queue.Full:
                try:
                    log_queue.get_nowait()
                    log_queue.put_nowait(entry)
                except (queue.Empty, queue.Full):
                    pass
        except Exception:
            pass


def _create_runtime_log_path() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR / f"{LOG_FILE_RUNTIME_PREFIX}_{log_day():%Y%m%d}.log"


def _prune_runtime_logs(current_path: Path | None = None) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return

    prune_logs(LOG_DIR.glob(f"{LOG_FILE_RUNTIME_PREFIX}_*.log*"), protected=(current_path,))


_tk_handler = TkLogHandler()
_tk_handler.setFormatter(_tk_formatter)
logger.addHandler(_tk_handler)

runtime_log_path: Path | None = None
_runtime_file_handler: DailyFileHandler | None = None
_compat_file_handler: DailyFileHandler | None = None


def _close_file_handler(handler: logging.Handler | None) -> None:
    if handler is None:
        return
    try:
        logger.removeHandler(handler)
    except Exception:
        pass
    try:
        handler.close()
    except Exception:
        pass


def _configure_file_handlers(*, announce_runtime_path: bool) -> None:
    global runtime_log_path, _runtime_file_handler, _compat_file_handler
    runtime_log_path = None
    _runtime_file_handler = None
    _compat_file_handler = None
    try:
        _prune_runtime_logs()
        runtime_log_path = _create_runtime_log_path()
        _runtime_file_handler = DailyFileHandler(runtime_log_path, prefix=LOG_FILE_RUNTIME_PREFIX)
        _runtime_file_handler.setFormatter(_formatter)
        logger.addHandler(_runtime_file_handler)
        _prune_runtime_logs(runtime_log_path)
    except Exception as exc:
        print(f"无法创建日志文件: {exc}")

    try:
        _compat_file_handler = DailyFileHandler(LOG_FILE_PATH)
        _compat_file_handler.setFormatter(_formatter)
        logger.addHandler(_compat_file_handler)
    except Exception as exc:
        print(f"无法创建兼容日志文件: {exc}")

    if announce_runtime_path and runtime_log_path is not None:
        logger.info("当前运行日志文件：%s", runtime_log_path)


def _exchange_log_artifact_paths() -> set[Path]:
    paths: set[Path] = {LOG_FILE_PATH}
    paths.update(LOG_FILE_PATH.parent.glob(f"{LOG_FILE_PATH.name}.*"))
    if LOG_DIR.exists():
        paths.update(LOG_DIR.glob(f"{LOG_FILE_RUNTIME_PREFIX}_*.log*"))
    return paths


def clear_exchange_log_queue() -> None:
    while True:
        try:
            log_queue.get_nowait()
        except queue.Empty:
            return


def purge_exchange_file_logs() -> SecureDeleteReport:
    """Close active exchange log handles, directly erase old logs, then start fresh files."""

    global runtime_log_path, _runtime_file_handler, _compat_file_handler
    with _file_handler_lock:
        _close_file_handler(_runtime_file_handler)
        _close_file_handler(_compat_file_handler)
        _runtime_file_handler = None
        _compat_file_handler = None
        runtime_log_path = None
        report = secure_unlink_all(_exchange_log_artifact_paths())
        _configure_file_handlers(announce_runtime_path=False)
        return report


_configure_file_handlers(announce_runtime_path=True)


_onchain_logger = logging.getLogger("onchain.runtime")
_onchain_logger.setLevel(logging.INFO)
_onchain_logger.propagate = False


def log_onchain_message(text: object) -> None:
    # The UI keeps only 500 rows; this independent sink saves every ordinary
    # chain message without feeding it back into the exchange UI queue.
    with _file_handler_lock:
        if not _onchain_logger.handlers:
            try:
                handler = DailyFileHandler(LOG_DIR / "onchain_runtime.log", prefix="onchain_runtime")
            except OSError as exc:
                logger.error("无法创建链上运行日志：%s", exc)
                return
            handler.setFormatter(SensitiveFormatter("%(asctime)s %(message)s"))
            _onchain_logger.addHandler(handler)
    _onchain_logger.info("%s", text)


def persist_exchange_message(text: object) -> None:
    """Save service notices without putting the same notice back in its queue."""
    record = logger.makeRecord(logger.name, logging.INFO, __file__, 0, "%s", (text,), None)
    with _file_handler_lock:
        for handler in (_runtime_file_handler, _compat_file_handler):
            if handler is not None:
                handler.handle(record)
