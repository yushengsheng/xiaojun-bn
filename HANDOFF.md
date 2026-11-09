# HANDOFF

> 本次会话：2026-11-09 · v1.1.90 GitHub 发布

## 本次完成

- (uncommitted)，改动及验证记录见 `CHANGELOG.md`。

## 进行中 / 未完成

- 用户已授权上传 GitHub、干净环境打包并发布新版本；待办见 `TODO.md` 的发布项。目标仓库 yushengsheng/xiaojun-bn，main 起点为 e06d022，待发布版本 v1.1.90，已确认本地与远端未使用。
- 发布必须遵循已完整读取的 `RELEASE.md`、`RELEASE_GUARDRAILS.md`：最终门禁、提交与推送 main、推送同一提交的标签、等待唯一工作流、验证六项资产。不手动发布或覆盖已有 Release。
- 最终本地门禁已通过，日志 `.release-v1.1.90-local-gate.log`，含 617 项测试、全部审计及 GUI 自检，已输出 `REGRESSION OK`；下一步提交并推送 main 和同一提交的 v1.1.90 标签。
- 首次提交 dee43b5 已推送 main 和标签，工作流 42587845078 的 Linux 通过，但 Windows BOM 测试夹具默认 cp1252 编码写入失败。已确认 v1.1.90 Release 不存在，当前只补充 `tests/test_onchain_recovery.py` 该写入的 UTF-8 编码。须重新完成完整门禁、推送修复，然后仅删除失败标签并在修复提交重建 v1.1.90，等待新运行，禁止使用新版本号或手动发布。
- 修复后的 44 项专项测试及完整本地门禁已通过，日志 `.release-v1.1.90-recovery-gate.log`，已输出 `REGRESSION OK`。下一步提交并推送此单行测试修复及记录，再核对 Release 仍不存在后重建失败标签。
- 修复提交 40f916c 已完成首次同版本恢复。工作流 42589152175 的 Linux、Windows 源码回归/审计、两个包构建均通过，但 onefile 自检中 Xray 因 UDP 端口绑定被 Windows 拒绝而停止；Release 仍不存在。当前修复仅让 Xray 及其探测要求同一 TCP/UDP 端口可用，不关闭 UDP 或跳过检查；Sing-box/Hysteria 原有分配及后端回退保留，五项新增回归及 22 项代理专项通过。
- 最终完整门禁已通过，日志 `.release-v1.1.90-final-622-gate.log`，622 项测试、全部审计及 GUI 自检成功且输出 `REGRESSION OK`。仅在该门禁子进程通过 NO_PROXY 显式直连官方 PyPI，未改系统/业务代理设置。网页测试清理仅回退本次 Popen 父进程；日志中的旧两个测试 PID 已通过只读检查确认退出，无外部批量清理。
- 下一步推送已验证修复，仅在确认无 Release 且旧标签仍指向 40f916c 后按既有流程重建 v1.1.90，再完成 Windows/Linux 干净环境及六项资产验收。

## 待决定

无。

## 关键上下文

- 用户限定只调整合约结束后的归集及完成文案，特别要求保留合约交易前的划转能力检测。因此仅对结束提现试算的归集开关增加自动提现条件，手动提现继续沿用原有按币种归集入口。
- 用户随后明确授权在保留现有功能的前提下修复依赖漏洞；本轮仅更新五个受影响依赖及全部对应锁文件，保留其他版本及业务代码。
- 后续授权已覆盖本次 GitHub 上传与正式发布，并要求提供 Linux 升级命令；不连接用户服务器实际执行升级。
- 初始工作区已有 `AGENTS.md`、`README.md`、链上执行与 GUI/恢复测试的未提交修改；这些不是本次合约业务改动，不得覆盖或回退。本次仅在 `README.md` 中增补合约资金保留说明。
- 已安装客户端及旧安装包尚未更新；无实盘操作，Windows/Linux 未进行原生系统实机验收。

## 备注

- 门禁环境位于项目 `.venv-regression`，不是 `/private/tmp`。最新完整门禁日志 `.dependency-security-full-regression.log` 已输出 `REGRESSION OK` 且退出码为 0；之前 `.futures-*` 中的失败审计属于修复前历史，不代表当前结果。
- 本轮五份锁文件审计日志为 `.dependency-security-audit-0.log` 至 `.dependency-security-audit-4.log`，均未检出已知漏洞；官方版本元数据及哈希保存在 `.dependency-security-metadata.log`，变更清单为 `.dependency-security-changes.log`。
- Linux 隔离测试须先调用 `fixture()` 重定向数据路径，再导入策略/桌面测试辅助模块，避免提前绑定本机密码文件。新增 `futures_completion` 用例已遵守该顺序。

## 下一步

- 继续当前已授权发布流程，完成 GitHub 工作流与六项资产验收后，提供固定 v1.1.90、保留原密码/配置的命令。
