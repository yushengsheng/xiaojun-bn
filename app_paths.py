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
DATA_DIR = APP_DIR / "data"
DATA_FILE = DATA_DIR / "accounts.json"
BG_DATA_FILE = DATA_DIR / "bg_one_to_many.json"
ONCHAIN_DATA_FILE = DATA_DIR / "onchain.json"
SECRET_KEY_FILE = DATA_DIR / ".secret.key"
