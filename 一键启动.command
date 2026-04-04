#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCH_SCRIPT="$PROJECT_DIR/launch_no_terminal.sh"

if [ ! -f "$LAUNCH_SCRIPT" ]; then
  osascript -e 'display alert "启动失败" message "未找到 launch_no_terminal.sh。" as critical'
  exit 1
fi

exec /bin/bash "$LAUNCH_SCRIPT"
