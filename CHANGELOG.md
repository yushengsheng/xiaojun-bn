# CHANGELOG

## 2026-11-09 合约及依赖修复
- v1.1.90 发布前最终门禁通过：`python3 tools/run_full_regression.py` 退出码 0，617 项单元测试、四项漏洞审计、应用及 GUI 自检通过并输出 `REGRESSION OK`；日志 `.release-v1.1.90-local-gate.log`。发布内容已按当前源码同步至 `RELEASE_NOTES.md`。
- 开始准备已授权的 GitHub v1.1.90 发布：本地 main 与 origin/main 一致，版本标签本地及远端均未使用；同步本轮与工作区既有链上修复的发布说明，按仓库固定流程重新完成最终工作树门禁后提交和发版。
- 完成依赖修复后的 `python3 tools/run_full_regression.py`：固定环境按新锁文件重建及哈希安装成功，617 项单元测试、四项漏洞审计、应用及 GUI 自检均通过，退出码 0 并输出 `REGRESSION OK`。额外审计 Linux 测试锁文件也未检出已知漏洞；构建/TLS 锁文件完整依赖解析通过，五个升级包均提供 Python 3.11 的 Windows/macOS/Linux 发行轮包。交易所三个公开 ping 接口使用新证书库均返回 HTTP 200，未带账户凭据或发起资金操作。
- 复核本轮仅修改依赖版本、对应官方哈希及任务记录，未变更现有业务逻辑。Windows/Linux 未进行原生系统实机验证；未生成新安装包、上传、发版或升级已安装客户端，亦未执行 Linux 安装、升级、部署。
- 同步 3 份直接依赖文件及 5 份带哈希锁文件，仅调整五个有漏洞依赖的版本及官方发布 SHA-256；保留其他依赖和锁文件注释，覆盖桌面、Linux 服务/测试及独立 TLS 依赖集合。
- 按后续授权核对现有依赖漏洞及官方修复版本，选择 certifi 2026.10.18、filelock 4.2.1、msgpack 1.2.3、opentelemetry-api 1.45.1、setuptools 84.1.0；均支持 Python 3.11，官方 PyPI 元数据未标记撤回或已知漏洞，准备同步全部对应锁文件及发布文件哈希。
- 验证两次 `python3 tools/run_full_regression.py`，617 项单元测试均通过，环境、编译、差异格式、敏感产物、发布规则和打包参数检查通过；另行运行 `python3 tools/run_local_gui_check.py --full-selftest`，输出 `SELFTEST OK`，包含 GUI 自检。首轮门禁遇到 PyPI 代理错误，复跑及单独审计确认现有运行/构建/Linux/TLS 锁文件的依赖漏洞，未输出 `REGRESSION OK`；保留现有依赖锁文件。
- 完成源码复核，保留用户已有链上及文档修改；Windows/macOS 桌面与 Linux 使用同一策略实现，Linux 控制器在本机隔离测试通过，Windows/Linux 未进行原生系统实机验证。本次未提交、上传、打包、发版或升级已安装客户端，亦未执行 Linux 安装、升级或部署。
- 增加真实签名及加密账本回归，覆盖连续轮次、复用客户端切换保证金币种与金额、重启后按资产提现、更换地址、多账号隔离、平仓回复丢失、部分划转失败、账本写入失败及停止/失败状态；9 项专项测试通过，Linux 共用控制器的实际策略与手动提现链路通过。
- 补全合约轮完成状态；关闭自动提现时不触发结束归集，保留交易前划转检测及按提现币种归集，并更新使用说明。
- 核查桌面与 Linux 共用策略、合约交易前划转检测、自动及手动提现入口；定位到关闭自动提现时的试算仍触发归集。
- 评估 Binance 官方 Python Connector：Python 技术栈及 MIT 许可证兼容，提供划转和提现接口，仓库仍维护；本次复用项目已有接口和恢复保护，无需引入 SDK 或更换业务底座。参考：https://github.com/binance/binance-connector-python 。
