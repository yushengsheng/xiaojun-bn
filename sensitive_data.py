#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re


REDACTED = "***"

_SS_URI_RE = re.compile(r"\bss://[^\s,，;；]+", re.IGNORECASE)
_HYSTERIA2_URI_RE = re.compile(r"\b(?:hy2|hysteria2)://[^\s,，;；]+", re.IGNORECASE)
_URL_USERINFO_RE = re.compile(r"\b((?:https?|socks5h?|socks4)://)([^/\s:@]+):([^/\s@]+)@", re.IGNORECASE)
_QUERY_SECRET_RE = re.compile(
    r"((?:^|[?&])(?:signature|api[_-]?key|apikey|api[_-]?secret|private[_-]?key|privatekey|"
    r"mnemonic|password|secret|proxy|ss_uri)=)([^&#\s]+)",
    re.IGNORECASE,
)
_JSON_SECRET_RE = re.compile(
    r"([\"'](?:api_key|apikey|api_secret|private_key|privatekey|mnemonic|password|"
    r"secret|proxy_url|exchange_proxy|exchange_proxy_enc)[\"']\s*:\s*[\"'])([^\"']+)([\"'])",
    re.IGNORECASE,
)
_LABEL_SECRET_RE = re.compile(
    r"\b(api[_ -]?key|apikey|api[_ -]?secret|private[_ -]?key|privatekey|mnemonic|"
    r"password|secret|proxy_url|proxy|ss_uri|signature)\b(\s*[:=]\s*)([^\s,，;；&]+)",
    re.IGNORECASE,
)
_CN_LABEL_SECRET_RE = re.compile(
    r"(私钥|助记词|代理链接|代理配置|代理|密钥|密码|签名)(\s*[：:=]\s*)([^\s,，;；&]+)"
)
_ENCRYPTED_VALUE_RE = re.compile(r"\benc::v[12]::[A-Za-z0-9_.=-]+")
_PLUS_RESULT_API_KEY_RE = re.compile(r"((?:^|[^\d])\d+\+)([A-Za-z0-9]{24,})(?=\+)")
_RAW_HEX_SECRET_RE = re.compile(r"(?<!0x)(?<!0X)(?<![A-Fa-f0-9])([A-Fa-f0-9]{64})(?![A-Fa-f0-9])")


def mask_middle(value: object, *, prefix: int = 6, suffix: int = 4, marker: str = "...") -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= prefix + suffix:
        return REDACTED
    return f"{text[:prefix]}{marker}{text[-suffix:]}"


def mask_api_key(value: object) -> str:
    return mask_middle(value, prefix=6, suffix=4)


def mask_secret_value(value: object) -> str:
    return mask_middle(value, prefix=4, suffix=4)


def redact_sensitive_text(text: object) -> str:
    raw = str(text or "")
    if not raw:
        return raw

    redacted = _SS_URI_RE.sub("ss://***", raw)
    redacted = _HYSTERIA2_URI_RE.sub("hysteria2://***", redacted)
    redacted = _URL_USERINFO_RE.sub(lambda m: f"{m.group(1)}***:***@", redacted)
    redacted = _QUERY_SECRET_RE.sub(lambda m: f"{m.group(1)}{mask_secret_value(m.group(2))}", redacted)
    redacted = _JSON_SECRET_RE.sub(lambda m: f"{m.group(1)}{mask_secret_value(m.group(2))}{m.group(3)}", redacted)
    redacted = _LABEL_SECRET_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{mask_secret_value(m.group(3))}", redacted)
    redacted = _CN_LABEL_SECRET_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{mask_secret_value(m.group(3))}", redacted)
    redacted = _ENCRYPTED_VALUE_RE.sub(lambda m: mask_secret_value(m.group(0)), redacted)
    redacted = _PLUS_RESULT_API_KEY_RE.sub(lambda m: f"{m.group(1)}{mask_api_key(m.group(2))}", redacted)
    redacted = _RAW_HEX_SECRET_RE.sub(lambda m: mask_secret_value(m.group(1)), redacted)
    return redacted
