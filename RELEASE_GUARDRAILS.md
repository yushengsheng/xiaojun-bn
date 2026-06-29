# 发版与运行时行为准则

本文件是项目的硬性行为准则，用来杜绝反复出现的同类问题：Python/Tk 崩溃、代理运行时缺失或认证失败、网络下载中断、打包后自检失败、GitHub Release 发布失败。

## 一票否决规则

出现下列任一情况时，必须立即停止打包、推送 tag、上传 GitHub Release，不允许用“可能是环境问题”继续发版：

- macOS 或 Windows 弹出 `Python 意外退出`、进程退出码 `134`、GUI smoke 直接 abort。
- `--selftest` 或 `--selftest --selftest-gui` 失败。
- 打包后的 onefile 或 portable 任一包未通过 `--selftest --selftest-gui`。
- 代理运行时缺失、manifest 认证失败、`sing-box` 或 `xray` 无法启动、端口未监听。
- 发版验证过程中需要临时联网下载运行时，且下载失败或哈希未验证。
- GitHub Actions 发版 workflow 失败、卡住、或 release asset 不完整。
- 工作区存在未解释的脏改动、测试失败、敏感文件检查失败、依赖漏洞审计失败。

## 固定流程

本项目只允许用 GitHub Actions 的 Windows clean runner 作为正式发版入口，本地机器只能做预检和问题定位。

正式发版必须满足：

- 目标 commit 已在本地完成回归：`python tools/run_regression.py --gui --strict-pip-check`。
- 若本地 GUI/Tk 环境异常，必须明确标记为阻断项，并在 GitHub Actions 上完成等价 GUI 自检后才能发版。
- CI 使用 `pip install --require-hashes -r requirements-build.lock` 安装构建依赖。
- release workflow 下载 `xray` 和 `sing-box` 时必须校验 `XRAY_SHA256` 和 `SING_BOX_SHA256`。
- 打包前必须运行源码回归。
- 打包后必须分别对 onefile 和 portable 执行 `--selftest --selftest-gui`。
- portable 打包必须从干净 staging 目录生成，不能把测试产生的 `data/`、日志、查询结果、钱包文件带入 release zip。
- 发布前必须运行敏感文件检查，确认 release 包不包含本地账号、私钥、日志、运行时状态。

## 环境问题处理规则

遇到 `Python 意外退出`、`exit 134`、Tk abort、GUI smoke 无输出退出时，不能直接重试发版。必须先完成以下判断：

- 本地 GUI 检查必须使用 `tools/run_local_gui_check.py`，不得临时手写 `python3 -c ...` 或直接使用已知会 abort 的普通 macOS Python。
- 在 Codex/沙箱环境里启动 Tk GUI 时，必须脱离沙箱申请 GUI 运行权限；普通沙箱里的 `exit 134` 不能被当作应用本身失败。
- `tools/run_local_gui_check.py` 会优先使用 `XIAOJUN_GUI_PYTHON`，其次使用 `/private/tmp/xiaojun-bn-regression-verify/bin/python`；如本地路径变化，先设置 `XIAOJUN_GUI_PYTHON`，再运行 GUI 检查。
- `tools/run_local_gui_check.py` 会在未显式指定 `SING_BOX_PATH` 时使用本机已验证的 `/private/tmp/xiaojun-bn-sing-box-verify` 和 `SING_BOX_EXECUTABLE_SHA256`；不要让完整 GUI 自检临时联网下载代理运行时。
- 确认崩溃发生在源码 GUI、打包 GUI、代理运行时启动、还是网络下载阶段。
- 查最近的 runtime log 和 `bot_log.txt`，记录最后一个通过的自检步骤。
- 如果是 GUI/Tk 层 abort，必须用同一目标平台的 packaged app 自检作为最终依据。
- 如果是 `sing-box` 或 `xray` 问题，必须先固定运行时来源、哈希和 manifest；不能在发版过程中依赖临时下载补救。
- 如果是网络下载中断，只能判定为环境阻断，不允许发布。

## GitHub 更新规则

更新 GitHub 或发版前必须满足：

- `git status --short` 已检查，所有改动都可解释。
- 代码、测试、文档、workflow 相关改动已一起提交，不能只提交局部修复。
- 本地回归通过；如果 GUI 自检因本机环境无法执行，必须在最终说明里明确，并等待 GitHub Actions GUI 自检通过。
- tag 推送前必须确认远端没有同名 tag，release workflow 不会覆盖错误版本。
- GitHub Actions 失败时禁止继续追加 tag 或重复上传，应先修复根因。

## 以后修改时必须遵守

- 不得删除 `tools/check_release_guardrails.py` 或从 `tools/run_regression.py` 移除该检查。
- 不得删除 `tools/run_local_gui_check.py`，本地 GUI 验证必须通过该脚本或 GitHub Actions 的 packaged GUI selftest 完成。
- 不得降低 release workflow 中 packaged `--selftest --selftest-gui` 的要求。
- 不得移除 proxy runtime 哈希校验。
- 不得把本地运行状态、日志、账号、私钥、中转钱包文件打进 release 包。
- 修改 GUI、代理、启动入口、PyInstaller 参数、运行时下载、发版 workflow 时，必须同步更新测试或 guardrail 检查。
