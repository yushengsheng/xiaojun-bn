"""Small, shared checks for user supplied numbers."""

from decimal import Decimal, InvalidOperation
import math


MAX_WORKER_THREADS = 128
MAX_RETRY_COUNT = 1000
MAX_WAIT_SECONDS = 86400


def finite_decimal(raw, label: str, *, minimum=0) -> Decimal:
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise RuntimeError(f"{label}格式不正确") from exc
    if not value.is_finite():
        raise RuntimeError(f"{label}必须是有限数字")
    if value < Decimal(str(minimum)):
        raise RuntimeError(f"{label}不能小于 {minimum}")
    return value


def finite_float(raw, label: str, *, positive: bool = False, maximum=MAX_WAIT_SECONDS) -> float:
    try:
        value = float(raw)
    except (ValueError, TypeError, OverflowError) as exc:
        raise RuntimeError(f"{label}格式不正确") from exc
    if not math.isfinite(value):
        raise RuntimeError(f"{label}必须是有限数字")
    if value < 0 or (positive and value == 0) or (maximum is not None and value > maximum):
        raise RuntimeError(f"{label}必须{'大于' if positive else '大于等于'} 0" + (f"，且不超过 {maximum}" if maximum is not None else ""))
    return value


def bounded_int(raw, label: str, *, minimum=1, maximum=MAX_WORKER_THREADS) -> int:
    try:
        value = int(str(raw).strip())
    except (ValueError, TypeError, OverflowError) as exc:
        raise RuntimeError(f"{label}必须是整数") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{label}必须在 {minimum}-{maximum} 之间")
    return value
