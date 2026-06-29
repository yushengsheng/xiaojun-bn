#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

from app_defaults import ONCHAIN_CONFIRM_WORKER_LIMIT_MAX
from exchange_logging import logger
from shared_utils import (
    LOG_MAX_ROWS,
    clear_ui_batch_size,
    dispatch_ui_callback,
    flush_queued_log_rows,
    flush_queued_ui_renders,
    schedule_ui_callback,
    stop_ui_bridge,
)
import task_progress


class OnchainThreadingMixin(object):
    @staticmethod
    def _unique_row_keys(row_keys: list[str]) -> list[str]:
        return task_progress.unique_keys(row_keys)
    def _progress_store(self, kind: str) -> dict[str, str]:
        if kind == "query":
            return self._ensure_query_status_store()
        if kind == "recovery":
            return self.row_recovery_status
        return self.row_status
    def _begin_progress(self, kind: str, row_keys: list[str]):
        task_progress.begin(self, kind, row_keys, amount_label="转账总额")
    def _progress_counts(self, kind: str, row_keys: list[str]) -> tuple[int, int, int, int, int]:
        return task_progress.progress_counts(self, kind, row_keys)
    def _refresh_progress_display(self):
        task_progress.refresh_display(self)
    def _refresh_progress_if_active(self, kind: str, row_key: str):
        task_progress.refresh_if_active(self, kind, row_key)
    def _dispatch_ui(self, callback) -> None:
        if self._closing or bool(getattr(self.root, "_closing", False)):
            return
        dispatch_ui_callback(self, callback)
    def _start_managed_thread(self, target, *, args=(), kwargs=None, name: str = "onchain-bg", daemon: bool = True) -> threading.Thread:
        call_kwargs = dict(kwargs or {})

        def runner():
            try:
                target(*args, **call_kwargs)
            except Exception:
                logger.exception("链上后台线程异常：%s", name)
            finally:
                current = threading.current_thread()
                with self._managed_threads_lock:
                    self._managed_threads.discard(current)

        thread = threading.Thread(target=runner, daemon=daemon, name=name)
        with self._managed_threads_lock:
            self._managed_threads.add(thread)
        thread.start()
        return thread
    def _confirmation_worker_limit(self) -> int:
        return min(max(1, self._runtime_worker_threads()), ONCHAIN_CONFIRM_WORKER_LIMIT_MAX)
    def _confirmation_executor_for_current_settings(self) -> ThreadPoolExecutor:
        with self._confirmation_executor_lock:
            if self._confirmation_executor is not None:
                return self._confirmation_executor
            max_workers = self._confirmation_worker_limit()
            self._confirmation_executor = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="onchain-confirm",
            )
            self._confirmation_executor_max_workers = max_workers
            return self._confirmation_executor
    def _finish_confirmation_future(self, future: Future, task_name: str) -> None:
        with self._confirmation_executor_lock:
            self._confirmation_futures.discard(future)
        if future.cancelled():
            return
        try:
            exc = future.exception()
        except Exception:
            logger.exception("链上确认任务状态读取失败：%s", task_name)
            return
        if exc is not None:
            logger.error("链上确认任务异常：%s", task_name, exc_info=(type(exc), exc, exc.__traceback__))
    def _submit_confirmation_task(self, target, *, name: str = "onchain-confirm") -> Future | None:
        if self._closing or bool(getattr(self.root, "_closing", False)):
            return None
        try:
            executor = self._confirmation_executor_for_current_settings()
            future = executor.submit(target)
        except RuntimeError:
            return None
        with self._confirmation_executor_lock:
            self._confirmation_futures.add(future)
        future.add_done_callback(lambda f, task_name=name: self._finish_confirmation_future(f, task_name))
        return future
    def _shutdown_confirmation_executor(self) -> None:
        with self._confirmation_executor_lock:
            executor = self._confirmation_executor
            self._confirmation_executor = None
            self._confirmation_executor_max_workers = 0
            futures = list(self._confirmation_futures)
            self._confirmation_futures.clear()
        for future in futures:
            future.cancel()
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)
    def _managed_threads_snapshot(self) -> list[threading.Thread]:
        current = threading.current_thread()
        with self._managed_threads_lock:
            return [t for t in self._managed_threads if t is not current and t.is_alive()]
    def _join_managed_threads(self, timeout_total: float = 1.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_total))
        while True:
            alive_threads = self._managed_threads_snapshot()
            if not alive_threads:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            per_thread = max(0.05, remaining / max(1, len(alive_threads)))
            for thread in alive_threads:
                thread.join(per_thread)
    def _schedule_tree_refresh(self) -> None:
        schedule_ui_callback(self, "tree_refresh", self._refresh_tree, root=getattr(self, "root", None))
    def shutdown(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.stop_requested.set()
        try:
            self._close_wallet_generator()
        except Exception:
            pass
        self._clear_source_private_key_cache()
        self._shutdown_confirmation_executor()
        self._join_managed_threads(timeout_total=1.0)
        try:
            self.client.close()
        except Exception:
            pass
        stop_ui_bridge(self)
    def _finish_progress(self, kind: str, success: int, failed: int):
        flush_queued_ui_renders(self)
        log_text = getattr(self, "log_text", None) or getattr(self, "log_tree", None)
        if log_text is not None:
            flush_queued_log_rows(self, log_text, max_rows=LOG_MAX_ROWS)
        clear_ui_batch_size(self)
        task_progress.finish(self, kind, success, failed)


def _build_module_star_exports() -> list[str]:
    return [name for name in globals() if not name.startswith("_") and not name.startswith("__")]


__all__ = _build_module_star_exports()
