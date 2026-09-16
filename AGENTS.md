# 发布规则

- 更新 GitHub、创建或修复版本发布前，必须完整读取 `RELEASE.md` 和 `RELEASE_GUARDRAILS.md`，并以仓库当前文件为准。
- 唯一完整本地门禁是 `python3 tools/run_full_regression.py`；必须成功结束并输出 `REGRESSION OK`。
- 不得使用 `/private/tmp` 回归环境，也不得把 Memory 中的旧发版命令作为依据。
