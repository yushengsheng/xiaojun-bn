#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/startup.log"
mkdir -p "$LOG_DIR"

# 优先使用 Homebrew Python 3.11（本机 Tkinter 更稳定）
PYTHON_BIN="/opt/homebrew/bin/python3.11"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

if [ -z "${PYTHON_BIN:-}" ]; then
  osascript -e 'display alert "启动失败" message "未找到 Python 3，请先安装 Python 3.11。" as critical'
  exit 1
fi

cd "$PROJECT_DIR"

# 自动补齐运行依赖（按需安装）
if ! "$PYTHON_BIN" -c 'import requests, eth_account, eth_utils' >/dev/null 2>&1; then
  "$PYTHON_BIN" -m pip install --user requests eth-account eth-utils >>"$LOG_FILE" 2>&1 || true
fi

# 启动 GUI（不弹终端）
exec "$PYTHON_BIN" "$PROJECT_DIR/小军bn.py" >>"$LOG_FILE" 2>&1
