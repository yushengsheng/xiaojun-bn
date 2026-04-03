#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/data/logs"
LOG_FILE="$LOG_DIR/startup.log"
mkdir -p "$LOG_DIR"
: > "$LOG_FILE"

# 优先使用 Homebrew Python 3.11（本机 Tkinter 更稳定）
PYTHON_BIN="/opt/homebrew/bin/python3.11"
if [ ! -x "$PYTHON_BIN" ]; then
  if PYTHON_BIN="$(command -v python3 2>/dev/null)"; then
    :
  else
    PYTHON_BIN=""
  fi
fi

if [ -z "${PYTHON_BIN:-}" ]; then
  osascript -e 'display alert "启动失败" message "未找到 Python 3，请先安装 Python 3.11。" as critical'
  exit 1
fi

cd "$PROJECT_DIR"

# 自动补齐运行依赖（按需安装）
if ! "$PYTHON_BIN" -c 'import requests, eth_account, eth_utils, socks, cryptography' >/dev/null 2>&1; then
  if ! "$PYTHON_BIN" -m pip install --user requests PySocks cryptography eth-account eth-utils >>"$LOG_FILE" 2>&1; then
    osascript -e "display alert \"启动失败\" message \"自动安装运行依赖失败，请查看日志：$LOG_FILE\" as critical"
    exit 1
  fi
  if ! "$PYTHON_BIN" -c 'import requests, eth_account, eth_utils, socks, cryptography' >/dev/null 2>&1; then
    osascript -e "display alert \"启动失败\" message \"运行依赖校验失败，请查看日志：$LOG_FILE\" as critical"
    exit 1
  fi
fi

# 启动 GUI（不弹终端）
exec "$PYTHON_BIN" "$PROJECT_DIR/小军bn.py" >>"$LOG_FILE" 2>&1
