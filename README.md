# xiaojun-bn

Binance 自动化工具（Tkinter GUI）。

## 当前页面结构

- **交易所批量**：原有功能页（保持兼容）
- **链上**：新增子页面，迁入独立链上批量模块（EVM）

> 两个页面配置与数据隔离，不互相覆盖。

## 快速使用（Windows）

1. 从 Releases 下载 `xiaojun-bn-vX.Y.Z-windows-x64.exe`，或下载 `xiaojun-bn-vX.Y.Z-windows-x64-portable.zip`
2. 双击运行，无需本机安装 Python、交易所模块依赖、代理模块依赖、链上模块依赖、UI 运行依赖
3. 首次运行会在程序同目录自动创建 `data/`，用于保存本地配置与链上页面数据

> Windows Release 已内置主程序运行环境，以及交易所、代理、链上、UI 所需运行依赖；同时内置 `xray` 与 `sing-box`，页面内置 SS 代理在其他电脑上也可直接使用。

## 源码运行

### Windows

```powershell
py -3 -m pip install requests PySocks eth-account eth-utils
py -3 小军bn.py
```

### macOS（本地一键启动，无终端窗口）

- 双击：`小军bn一键启动.app`
- 启动脚本：`launch_no_terminal.sh`
- 启动日志：`data/logs/startup.log`

## 依赖

- 基础：`requests`
- 链上模块：`eth-account`、`eth-utils`

- 源码运行前请先安装依赖
- macOS 一键启动脚本会在启动前按需自动安装依赖

## 发布包说明

- `windows-x64.exe`：单文件版，适合直接下载即用
- `windows-x64-portable.zip`：便携目录版，适合长期放在固定目录运行
- GitHub Release 构建会一并打包交易所、代理、链上、UI 所需运行依赖
- Release 会附带页面内置 SS 代理所需的 `xray` 与 `sing-box` 运行文件，无需目标电脑额外安装 v2rayN 或单独拷贝代理内核

## 账号导入格式（交易所批量页）

- 支持 3 行一组：`APIKEY` / `APISECRET` / `提现地址`
- 支持行尾备注文本（会自动忽略）
- 支持剪贴板粘贴导入（按钮或 `Ctrl+V`）
