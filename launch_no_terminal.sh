#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/data/logs"
LOG_FILE="$LOG_DIR/startup.log"
mkdir -p "$LOG_DIR"
: > "$LOG_FILE"

fail() {
  printf '%s\n' "$1" >>"$LOG_FILE"
  osascript - "$1" "$LOG_FILE" <<'APPLESCRIPT'
on run argv
  display alert "启动失败" message ((item 1 of argv) & return & "日志：" & (item 2 of argv)) as critical
end run
APPLESCRIPT
  exit 1
}

# 始终使用项目隔离环境，避免误用系统 Python 3.9 或污染全局依赖。
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
UV_BIN="$(command -v uv 2>/dev/null || true)"
if [ -z "$UV_BIN" ] && [ -x "$HOME/.local/bin/uv" ]; then
  UV_BIN="$HOME/.local/bin/uv"
fi
if [ ! -x "$PYTHON_BIN" ]; then
  [ -n "$UV_BIN" ] || fail "缺少项目 Python 环境及 uv，无法自动恢复。"
  "$UV_BIN" venv --python 3.11 "$PROJECT_DIR/.venv" >>"$LOG_FILE" 2>&1 || fail "创建 Python 3.11 环境失败。"
fi
"$PYTHON_BIN" -c 'import sys; assert sys.version_info[:2] == (3, 11)' >>"$LOG_FILE" 2>&1 || fail "项目 Python 必须为 3.11。"

cd "$PROJECT_DIR"
if ! "$PYTHON_BIN" -c 'import tkinter, requests, eth_account, eth_utils, socks, cryptography' >>"$LOG_FILE" 2>&1; then
  [ -n "$UV_BIN" ] || fail "运行依赖缺失，且未找到 uv。"
  "$UV_BIN" pip install --python "$PYTHON_BIN" --require-hashes -r "$PROJECT_DIR/requirements.lock" >>"$LOG_FILE" 2>&1 || fail "安装运行依赖失败。"
  "$PYTHON_BIN" -c 'import tkinter, requests, eth_account, eth_utils, socks, cryptography' >>"$LOG_FILE" 2>&1 || fail "运行依赖校验失败。"
fi

# .app 内由 AppleScript 后台调用，不会打开终端窗口。
"$PYTHON_BIN" "$PROJECT_DIR/小军bn.py" >>"$LOG_FILE" 2>&1 || fail "程序异常退出。"
