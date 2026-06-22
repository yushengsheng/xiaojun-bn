#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path


FORMAT = "xiaojun-authenticated-config-v1"


def config_context(path: Path | str) -> str:
    """Stable context prevents ciphertext from being replayed into another config file."""
    return f"config:{Path(path).name}"


def seal_config_payload(payload: object, *, secret_box, context: str) -> dict[str, object]:
    plain = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return {"format": FORMAT, "ciphertext": secret_box.encrypt_bound(plain, context)}


def unseal_config_payload(raw: object, *, secret_box, context: str) -> tuple[object, bool]:
    if not isinstance(raw, dict) or "format" not in raw:
        return raw, False
    if raw.get("format") != FORMAT:
        raise RuntimeError("认证配置文件版本不支持")
    cipher = str(raw.get("ciphertext") or "").strip()
    if not cipher:
        raise RuntimeError("认证配置密文为空")
    try:
        payload = json.loads(secret_box.decrypt_bound(cipher, context))
    except Exception as exc:
        raise RuntimeError("认证配置校验失败，文件可能被篡改或密码不匹配") from exc
    return payload, True
