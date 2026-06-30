#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, TypeVar


T = TypeVar("T")


class RetryAborted(RuntimeError):
    pass


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 10
    delay_seconds: float = 1.0


def run_with_retries(
    action: Callable[[], T],
    *,
    policy: RetryPolicy,
    should_retry: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[int, int, Exception], None] | None = None,
    on_give_up: Callable[[int, Exception], None] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> T:
    attempts = max(1, int(policy.attempts or 1))
    delay_seconds = max(0.0, float(policy.delay_seconds or 0.0))
    sleeper = sleep_fn or time.sleep

    for attempt in range(1, attempts + 1):
        try:
            return action()
        except RetryAborted:
            raise
        except Exception as exc:
            if should_retry is not None and not should_retry(exc):
                raise
            if attempt >= attempts:
                if on_give_up is not None:
                    on_give_up(attempts, exc)
                raise
            if on_retry is not None:
                on_retry(attempt, attempts, exc)
            if delay_seconds > 0:
                sleeper(delay_seconds)

    raise RuntimeError("未知错误：重试失败")
