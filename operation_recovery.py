#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pure planning helpers for encrypted on-chain operation recovery."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


def _digest(value: object) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class OnchainRecoveryTask:
    row_key: str
    source: str
    target: str
    source_address: str
    task_key: str


@dataclass(frozen=True)
class OnchainRecoveryPlan:
    fingerprint: str
    tasks: tuple[OnchainRecoveryTask, ...]


def build_onchain_recovery_plan(
    *,
    network: str,
    mode: str,
    coin: str,
    amount: str,
    random_enabled: bool,
    random_min: str,
    random_max: str,
    token_contract: str,
    token_decimals: int,
    token_is_native: bool,
    jobs: list[tuple[str, str, str, str]],
) -> OnchainRecoveryPlan:
    """Create stable batch/task identifiers without serialising private keys.

    ``jobs`` contains ``(row_key, source_credential, target, source_address)``.
    The credential intentionally does not enter the fingerprint or task key.
    """
    occurrences: dict[tuple[str, str], int] = {}
    tasks: list[OnchainRecoveryTask] = []
    for row_key, source, target, source_address in jobs:
        source_text = str(source_address or "").strip().lower()
        target_text = str(target or "").strip().lower()
        pair = (source_text, target_text)
        occurrence = occurrences.get(pair, 0)
        occurrences[pair] = occurrence + 1
        task_key = _digest({"source": source_text, "target": target_text, "occurrence": occurrence})
        tasks.append(
            OnchainRecoveryTask(
                row_key=str(row_key or ""),
                source=str(source or ""),
                target=str(target or ""),
                source_address=source_text,
                task_key=task_key,
            )
        )

    fingerprint = _digest(
        {
            "network": str(network or "").strip().upper(),
            "mode": str(mode or "").strip(),
            "coin": str(coin or "").strip().upper(),
            "amount": str(amount or "").strip(),
            "random_enabled": bool(random_enabled),
            "random_min": str(random_min or "").strip(),
            "random_max": str(random_max or "").strip(),
            "token_contract": str(token_contract or "").strip().lower(),
            "token_decimals": int(token_decimals),
            "token_is_native": bool(token_is_native),
            "task_keys": [task.task_key for task in tasks],
        }
    )
    return OnchainRecoveryPlan(fingerprint=fingerprint, tasks=tuple(tasks))
