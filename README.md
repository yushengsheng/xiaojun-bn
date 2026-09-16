# xiaojun-bn

Binance 自动化工具（Tkinter GUI）。

## 当前页面结构

- **交易所批量**：原有功能页（保持兼容）
- **链上**：新增子页面，迁入独立链上批量模块（EVM）

> 两个页面配置与数据隔离，不互相覆盖。

## 交易所批量页使用说明

### 账号选择与预购买 BNB

- 账号列表最左侧的勾选决定账号是否参与本次批量任务。
- “预购 BNB”列是独立的单账号开关：勾选才会为该账号执行预购，不勾选不会预购。表头复选框可对当前账号批量全选或全不选。
- 预购只在本次批量确认中启用、配置金额大于 `0`，且该账号的预购框已勾选时执行。重试失败账号时会按稳定账号标识恢复原选择，不依赖内存对象 ID。
- 预购使用交易对的计价币。例如 `XPLUSDC` 使用 `USDC` 购买 BNB；下单后会再确认现货 BNB 可用余额。
- 预购覆盖现货市价、挂单、溢价挂单和分仓挂单。“剩余 BNB 停止”的市价模式即使页面轮次为 `0` 也支持预购。合约、理财、闪兑和普通现货 `0` 轮单次兑换不执行预购。

### 现货模式与停止方式

- 市价模式的停止方式为二选一：
  - **现货轮次**：保持原有逻辑，配置多少轮就执行多少轮。
  - **剩余 BNB**：不使用现货轮次作为上限，每轮检查 BNB 余额，低于设置阈值时停止。
- 分仓挂单行情轮询间隔为 `1.5` 秒。手动点击停止，或触发剩余 BNB 阈值时，都会先撤销该任务挂单，再将可交易的基础币市价卖成计价币。程序会复查未完成挂单和可交易仓位，确认清理成功后才报告结束；失败则显示明确的清理异常。
- 所有现货模式的到账检测都允许“交易对任意一方有仓位”后开始。例如账号只有 `ALLO` 而没有 `USDT`，`ALLOUSDT` 分仓挂单可以直接从卖单开始。
- 批次共享的 API 限流等待同时适用于现货市价、挂单、溢价挂单、分仓挂单、合约和理财请求，等待期间仍可停止任务。

### `0` 轮兑换与交易对输入

- 交易对可输入 Binance 原生格式，也可加 `/`；程序会移除分隔符并转为大写。例如 `btc/eth` 会解析为 `BTCETH`。
- **现货 + 市价 + `0` 轮**是一次真实现货市价兑换，不走闪兑。程序会从 Binance 现货交易对中解析正向或反向路由：例如输入 `USDT/USDC`，如果交易所仅有 `USDCUSDT`，则使用该交易对买入 USDC；输入 `BTC/ETH` 时也会按交易所实际存在的正向或反向市场执行。
- 双向解析只扩展市价 `0` 轮单次兑换，不改变多轮市价和其他挂单模式的交易对方向。
- `0` 轮下单异常会标记为“0轮现货兑换”失败并立即中止后续提现，不会把未完成的兑换误报为完成。

### 错误重试与运行安全

- 网络瞬时失败和 Binance 暂时性服务错误保持原有重试策略。
- Binance `-2015` 表示 API Key / Secret、账号权限或 IP 白名单与当前请求不匹配。该类明确权限错误每个请求最多尝试 `3` 次，以减少无效请求和重复日志；连续失败时应检查 API 权限、IP 白名单和代理出口 IP，而不是反复重启任务。
- 账号数据、代理配置和任务状态保存在本地 `data/`。不要将该目录、API Secret、提现凭据或完整代理链接提交到 Git。
- 启动代理前会清理本程序留下的过期运行时。Windows 通过进程 ID 存活检查和完整命令行匹配确认残留，只结束精确引用本程序过期配置的 `sing-box` / `xray` / `hysteria` 进程；正在运行的所有者和无法确认的进程会保留。
- 归集 BNB 时，账号总残留估值小于 `0.1 USDT` 视为小额尘埃，不报残留异常。

## 快速使用（Windows）

1. 从 Releases 下载 `xiaojun-bn-vX.Y.Z-windows-x64.exe`，或下载 `xiaojun-bn-vX.Y.Z-windows-x64-portable.zip`
2. 双击运行，无需本机安装 Python、交易所模块依赖、代理模块依赖、链上模块依赖、UI 运行依赖
3. 首次运行会在程序同目录自动创建 `data/`，用于保存本地配置与链上页面数据

> Windows Release 已内置主程序运行环境，以及交易所、代理、链上、UI 所需运行依赖；同时内置 `xray`、`sing-box` 与官方 `Hysteria` 客户端，SS 和 Hysteria2 代理在其他电脑上也可直接使用。

## 源码运行

### Windows

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python -m pip install --require-hashes -r requirements.lock
.\.venv\Scripts\python 小军bn.py
```

可选自检：

```powershell
.\.venv\Scripts\python 小军bn.py --selftest
.\.venv\Scripts\python 小军bn.py --selftest-online
```

- `--selftest`：离线完整性自检，适合本地排查或发布冒烟验证
- `--selftest-online`：额外验证 ETH/BSC 公网 RPC 可达性，适合手动联机排查

### macOS（本地启动）

- 无终端窗口：`小军bn一键启动.app`
- 备用脚本：`一键启动.command`
- 启动脚本：`launch_no_terminal.sh`
- 启动日志：`data/logs/startup.log`

## 依赖

- 基础：`requests`
- 安全存储：`cryptography`
- 链上模块：`eth-account`、`eth-utils`

- 源码运行前请使用 `requirements.lock` 安装带哈希校验的依赖
- macOS 一键启动脚本会在启动前按需自动安装依赖
- 建议源码运行使用项目内 `.venv`，避免全局 Python 依赖冲突影响交易所、链上或打包自检
- 发布回归会审计运行锁和构建锁中的已知漏洞；本地完整回归请按 `requirements-build.lock` 安装依赖
- 若通过 `SING_BOX_PATH` 指定外部 `sing-box`，可同时设置 `SING_BOX_EXECUTABLE_SHA256` 为该可执行文件的 SHA-256；项目内已认证缓存和随包运行时会优先于普通 `PATH` 程序
- 交易所与链上代理框支持完整 `hy2://` / `hysteria2://` 分享链接；`pinSHA256` 证书指纹会原样交给官方客户端校验，并启用 Hysteria2 Fast Open 降低新连接延迟
- 代理框旁的按钮每次点击都会重新请求对应业务接口，并显示 `延迟 N ms`；连接失败时显示 `失败`

## 链上动态估费

- ETH / BSC 转账使用最近 20 个区块的费用历史：取各区块按 gas 用量加权的 P50 优先费，再取跨区块中位数，加上下一块基础费并留 10% 余量；最终报价不低于节点建议价。
- 同网络并发任务共享最多 3 秒的报价；中转第一跳发送前也会刷新报价并重新检查余额。历史查询最多等待 8 秒，节点请求失败会尝试备用节点；历史数据不可用时回退节点建议价并记录警告。
- 仅影响新签名的交易，不会自动加价替换已提交交易；拥堵时不能保证立即确认。转账金额、收款地址和中转流程保持不变。

## 发布包说明

- `windows-x64.exe`：单文件版，适合直接下载即用
- `windows-x64-portable.zip`：便携目录版，适合长期放在固定目录运行
- GitHub Release 构建会一并打包交易所、代理、链上、UI 所需运行依赖
- Release 会附带 SS 所需的 `xray` / `sing-box` 和 Hysteria2 所需的官方 `Hysteria` 客户端，无需目标电脑额外安装代理内核
- Release 冒烟默认使用离线 `--selftest`；联机链上连通性需要时再额外执行 `--selftest-online`

## 回归检查

macOS 源码改动后使用固定入口运行完整回归：

```bash
python3 tools/run_full_regression.py
```

该入口使用项目内 `.venv-regression`。首次运行或依赖锁变化时会按
`requirements-build.lock` 自动重建，后续直接复用，不依赖会被系统清理的临时目录。

Windows 可在已安装构建锁依赖的项目虚拟环境中运行：

```powershell
.\.venv\Scripts\python tools\run_regression.py --gui
```

需要额外验证公网 RPC 时再加 `--online`。如果只是确认当前 Python 环境是否隔离，可运行：

```powershell
.\.venv\Scripts\python tools\check_python_environment.py
```

## 账号导入格式（交易所批量页）

- 支持 2 段一组：`APIKEY` / `APISECRET`，未提供提现地址时自动跳过该账号的所有提现步骤
- 继续支持 3 段一组：`APIKEY` / `APISECRET` / `提现地址`
- 支持同一行使用空格、Tab 或 `|` 分隔
- 支持行尾备注文本（会自动忽略）
- 支持剪贴板粘贴导入（按钮或 `Ctrl+V`）
