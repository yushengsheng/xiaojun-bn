#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path


# PyInstaller 冻结运行时使用 exe 所在目录，保证 data 持久化在可见目录
if getattr(sys, "frozen", False):
    APP_DIR = Path(sys.executable).resolve().parent
else:
    APP_DIR = Path(__file__).resolve().parent
if getattr(sys, "frozen", False):
    BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR)).resolve()
else:
    BUNDLE_DIR = APP_DIR
DATA_DIR = APP_DIR / "data"
CONFIG_BACKUP_SUFFIX = ".bak"
DATA_FILE = DATA_DIR / "accounts.json"
STRATEGY_CONFIG_FILE = DATA_DIR / "exchange_strategy_settings.json"
EXCHANGE_PROXY_CONFIG_FILE = DATA_DIR / "exchange_proxy_settings.json"
ONCHAIN_DATA_FILE = DATA_DIR / "onchain.json"
SECRET_KEY_FILE = DATA_DIR / ".secret.key"
LOG_DIR = DATA_DIR / "logs"
LOG_FILE_PATH = APP_DIR / "bot_log.txt"
WITHDRAW_SUCCESS_FILE = DATA_DIR / "withdraw_success.txt"
TOTAL_ASSET_RESULT_FILE = DATA_DIR / "total_asset_result.txt"
