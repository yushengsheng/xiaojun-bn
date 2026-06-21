#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import queue
import tempfile
from pathlib import Path

from exchange_app_batch import ExchangeAppBatchMixin
from exchange_logging import (
    clear_exchange_account_log_index,
    log_queue,
    logger,
    set_exchange_account_log_index,
)
from secret_box import SecretBox
from sensitive_data import mask_api_key, redact_sensitive_text
from shared_utils import log_amount_value_spans


def drain_exchange_log_queue() -> list[str]:
    messages: list[str] = []
    while True:
        try:
            messages.append(str(log_queue.get_nowait()))
        except queue.Empty:
            return messages


def _run_secret_box_selftest_checks(checks: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="xiaojun-secret-box-selftest-") as tmpdir:
        secret_box = SecretBox(Path(tmpdir) / ".secret.key")
        secret_backend = secret_box.modern_encryption_backend_name()
        if secret_backend == "missing":
            raise RuntimeError("安全存储新版密文后端不可用：缺少 cryptography / pycryptodome")
        probe_text = "selftest-secret-box"
        v2_cipher = secret_box.encrypt(probe_text)
        if not str(v2_cipher).startswith(secret_box.PREFIX_V2):
            raise RuntimeError("安全存储自检失败：未生成新版密文")
        if secret_box.decrypt(v2_cipher) != probe_text:
            raise RuntimeError("安全存储自检失败：新版密文回读不一致")
        v1_cipher = secret_box._encrypt_v1(probe_text)
        if secret_box.decrypt(v1_cipher) != probe_text:
            raise RuntimeError("安全存储自检失败：旧版密文兼容回读失败")
        checks.append(f"secret-box={secret_backend}")


def _run_sensitive_redaction_selftest_checks(checks: list[str]) -> None:
    sample_api_key = "AK" + "1" * 62
    sample_signature = "f" * 64
    sample_private_key = "a" * 64
    sample_proxy = "ss://example-sensitive-proxy"
    redacted = redact_sensitive_text(
        "api_key=%s&signature=%s 私钥：%s proxy=%s 1+%s+0x0000000000000000000000000000000000000001+1.0"
        % (sample_api_key, sample_signature, sample_private_key, sample_proxy, sample_api_key)
    )
    for secret_text in (sample_api_key, sample_signature, sample_private_key, sample_proxy):
        if secret_text in redacted:
            raise RuntimeError("敏感信息脱敏自检失败：仍包含原始敏感字段")
    if mask_api_key(sample_api_key) not in redacted:
        raise RuntimeError("敏感信息脱敏自检失败：结果文件 API KEY 脱敏格式异常")
    if redact_sensitive_text("secret-box=cryptography") != "secret-box=cryptography":
        raise RuntimeError("敏感信息脱敏自检失败：误伤普通 selftest 文本")
    checks.append("sensitive-redaction=ok")


def _run_exchange_log_selftest_checks(checks: list[str]) -> None:
    class _SelftestVar:
        def __init__(self, value: str = ""):
            self._value = value

        def get(self):
            return self._value

    if ExchangeAppBatchMixin._account_display_index({"index_var": _SelftestVar("8")}, 1) != 8:
        raise RuntimeError("交易所日志账号编号自检失败：未使用账号列表显示编号")
    if ExchangeAppBatchMixin._account_display_index({"index_var": _SelftestVar("")}, 3) != 3:
        raise RuntimeError("交易所日志账号编号自检失败：空编号未回退到批次编号")

    drain_exchange_log_queue()
    set_exchange_account_log_index(8)
    try:
        logger.info("策略内部日志自检")
        logger.info("账号 #8 已有账号编号自检")
    finally:
        clear_exchange_account_log_index()
    exchange_log_messages = drain_exchange_log_queue()
    internal_log = next((msg for msg in exchange_log_messages if "策略内部日志自检" in msg), "")
    explicit_log = next((msg for msg in exchange_log_messages if "已有账号编号自检" in msg), "")
    if "[账号 #8]" not in internal_log:
        raise RuntimeError("交易所日志账号编号自检失败：策略内部日志未自动带账号编号")
    if explicit_log.count("账号 #8") != 1:
        raise RuntimeError("交易所日志账号编号自检失败：已有账号编号的日志被重复加前缀")
    checks.append("exchange-log-account-context=ok")

    display_time = internal_log.split(" ", 1)[0]
    if len(display_time) != 8 or display_time[2] != ":" or display_time[5] != ":":
        raise RuntimeError("交易所日志时间显示自检失败：UI 日志未使用时分秒")
    if internal_log[:4].isdigit() and internal_log[4:5] == "-":
        raise RuntimeError("交易所日志时间显示自检失败：UI 日志仍显示年月日")
    checks.append("exchange-log-display-time=ok")


def _run_log_amount_highlight_selftest_checks(checks: list[str]) -> None:
    def highlighted_values(text: str) -> list[str]:
        return [text[start:end] for start, end in log_amount_value_spans(text)]

    amount_log = (
        "2026-05-25 12:34:56 [INFO] [账号 #8] 开始执行挂单策略："
        "预买BNB金额=0.0100，剩余bnb手续费=0.0002，重挂阈值=0.1 USDT，线程=2"
    )
    if highlighted_values(amount_log) != ["0.0100", "0.0002", "0.1"]:
        raise RuntimeError("交易所日志金额高亮自检失败：金额数值识别异常")
    non_amount_log = "2026-05-25 12:34:56 [INFO] 账号 #8 serverTime=1710000000000，delay=3，threads=2"
    if highlighted_values(non_amount_log):
        raise RuntimeError("交易所日志金额高亮自检失败：误识别普通数字")
    checks.append("exchange-log-amount-highlight=ok")


def run_basic_selftest_checks(checks: list[str]) -> None:
    _run_secret_box_selftest_checks(checks)
    _run_sensitive_redaction_selftest_checks(checks)
    _run_exchange_log_selftest_checks(checks)
    _run_log_amount_highlight_selftest_checks(checks)
