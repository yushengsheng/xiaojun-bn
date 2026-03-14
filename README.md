# xiaojun-bn

Binance 自动化工具（Tkinter GUI）。

## 当前页面结构

- **交易所批量**：原有功能页（保持兼容）
- **链上**：新增子页面，迁入独立链上批量模块（EVM）

> 两个页面配置与数据隔离，不互相覆盖。

## 快速使用（Windows）

1. 从 Releases 下载 `xiaojun-bn.exe`
2. 双击运行，无需本机安装 Python 依赖

## 源码运行

### Windows

```powershell
py -3 小军bn.py
```

### macOS（本地一键启动，无终端窗口）

- 双击：`小军bn一键启动.app`
- 启动脚本：`launch_no_terminal.sh`

## 依赖

- 基础：`requests`
- 链上模块：`eth-account`、`eth-utils`

一键启动脚本会按需自动安装依赖。

## 账号导入格式（交易所批量页）

- 支持 3 行一组：`APIKEY` / `APISECRET` / `提现地址`
- 支持行尾备注文本（会自动忽略）
- 支持剪贴板粘贴导入（按钮或 `Ctrl+V`）
