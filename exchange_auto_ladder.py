#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""自动分仓的可热更新判断参数及纯行情信号；不持有订单或账号状态。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class AutoLadderSettings:
    volume_minutes: int = 5
    start_volume: Decimal = Decimal("100000")
    recent_minutes: int = 1
    price_minutes: int = 5
    price_ticks: int = 5
    dry_minutes: int = 5
    dry_volume: Decimal = Decimal("50000")
    dry_confirm_minutes: int = 2
    turn_seconds: int = 45
    turn_ticks: int = 2
    turn_count: int = 3
    stable_seconds: int = 30

    @classmethod
    def parse(cls, raw: dict[str, object]) -> "AutoLadderSettings":
        defaults = cls()
        values = {}
        for name in cls.__dataclass_fields__:
            text = str(raw.get(name, getattr(defaults, name))).strip()
            try:
                if name in {"start_volume", "dry_volume"}:
                    value = Decimal(text)
                    if not value.is_finite() or value <= 0 or value > Decimal("1000000000000"):
                        raise ValueError()
                else:
                    value = int(text)
                    if str(value) != text or value < 1:
                        raise ValueError()
            except (ValueError, InvalidOperation):
                raise ValueError(f"自动分仓参数 {name} 必须为正数（分钟/秒/tick/次数必须为整数）") from None
            values[name] = value
        if values["volume_minutes"] > 60 or values["price_minutes"] > 60 or values["dry_minutes"] > 60:
            raise ValueError("行情观察窗口最多 60 分钟")
        if values["recent_minutes"] > values["volume_minutes"]:
            raise ValueError("近期成交窗口不能超过成交额观察窗口")
        if values["dry_confirm_minutes"] > 10:
            raise ValueError("缩量确认最多 10 分钟")
        if values["price_ticks"] > 1000 or values["turn_ticks"] > 1000 or values["turn_count"] > 100:
            raise ValueError("价格 tick 最多 1000，折返次数最多 100")
        if values["turn_seconds"] > 600 or values["stable_seconds"] > 600:
            raise ValueError("价格观察与恢复时间最多 600 秒")
        if values["dry_volume"] >= values["start_volume"]:
            raise ValueError("缩量暂停门槛必须低于启动成交额门槛")
        return cls(**values)

    def as_dict(self) -> dict[str, str]:
        return {name: str(getattr(self, name)) for name in self.__dataclass_fields__}

    @property
    def required_minutes(self) -> int:
        return max(self.volume_minutes, self.price_minutes,
                   self.dry_minutes + self.dry_confirm_minutes - 1)


class AutoLadderSettingsStore:
    def __init__(self, settings: AutoLadderSettings | None = None):
        self._lock = threading.Lock()
        self._settings = settings or AutoLadderSettings()
        self._version = 0

    def snapshot(self) -> tuple[AutoLadderSettings, int]:
        with self._lock:
            return self._settings, self._version

    def replace(self, settings: AutoLadderSettings) -> None:
        if not isinstance(settings, AutoLadderSettings):
            raise TypeError("自动分仓设置类型无效")
        with self._lock:
            self._settings = settings
            self._version += 1


def volume_and_price_signal(rows: list[dict], settings: AutoLadderSettings, tick: Decimal) -> tuple[bool, bool]:
    """rows 为按分钟递增的已收盘 K 线；(入场允许, 缩量暂停)。"""
    needed = settings.required_minutes
    if tick <= 0 or len(rows) < needed:
        return False, False
    tail = rows[-needed:]
    if any(tail[i]["open_time"] - tail[i-1]["open_time"] != 60000 for i in range(1, len(tail))):
        return False, False
    entry = tail[-settings.volume_minutes:]
    prices = tail[-settings.price_minutes:]
    recent = tail[-settings.recent_minutes:]
    ready = (
        sum((row["quote_volume"] for row in entry), Decimal(0)) >= settings.start_volume
        and sum((row["quote_volume"] for row in recent), Decimal(0)) > 0
        and max(row["high"] for row in prices) - min(row["low"] for row in prices) <= tick * settings.price_ticks
    )
    dry = all(
        sum((row["quote_volume"] for row in tail[-settings.dry_minutes - i:len(tail)-i if i else None]), Decimal(0)) < settings.dry_volume
        for i in range(settings.dry_confirm_minutes)
    )
    return ready, dry


def reversal_count(samples: list[tuple[float, Decimal]], now: float, seconds: int, swing: Decimal) -> int:
    """峰谷回撤计数：单边跳价为零次；小于 swing 的盘口跳动不计。"""
    prices = [price for timestamp, price in samples if now - seconds <= timestamp <= now]
    if not prices or swing <= 0:
        return 0
    extreme = prices[0]
    direction = 0
    turns = 0
    for price in prices[1:]:
        if direction == 0:
            if price >= extreme + swing:
                direction, extreme = 1, price
            elif price <= extreme - swing:
                direction, extreme = -1, price
            # 尚未形成第一段时保留起点，避免逐 tick 滑动抹去真实波动。
        elif direction > 0:
            if price > extreme:
                extreme = price
            elif price <= extreme - swing:
                turns += 1
                direction, extreme = -1, price
        elif price < extreme:
            extreme = price
        elif price >= extreme + swing:
            turns += 1
            direction, extreme = 1, price
    return turns
