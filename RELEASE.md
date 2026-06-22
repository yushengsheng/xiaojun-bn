# Release Process

This project must be released from GitHub Actions on a clean `windows-latest` runner.

Rules:
- Do not build release packages only from a local machine.
- Tag the target commit with `v*` and push the tag to trigger `.github/workflows/release-windows-exe.yml`.
- Keep the workflow as the single source of truth for release packaging.
- The release workflow must continue bundling:
  - Python runtime dependencies collected by PyInstaller
  - pinned proxy runtimes downloaded during CI (`xray`, `sing-box`)
  - both onefile and portable onedir packages
- Keep runtime dependency sources in `requirements.txt` and build dependency sources in `requirements-build.txt`; regenerate and commit `requirements.lock` and `requirements-build.lock` with SHA-256 hashes whenever either source file changes.
- Install CI and release dependencies only with `pip install --require-hashes -r requirements-build.lock`.
- Run `python tools/run_regression.py --skip-pip-check --skip-app-selftest` before building release packages on CI. The command audits both locked dependency sets and must fail on known vulnerabilities.
- Smoke test both packaged apps with `--selftest --selftest-gui` before publishing release assets.
  - `--selftest` should stay offline-friendly and focus on packaged integrity.
  - `--selftest-gui` should cover GUI startup, page loading, config save, and wallet generation.
  - If you need to verify outbound EVM RPC reachability manually, use `--selftest-online`.
- Do not package the portable directory in place after smoke tests.
  - Smoke tests intentionally create runtime files such as `data/`, `bot_log.txt`, proxy runtime logs, and temporary proxy config files.
  - On GitHub Windows runners, deleting those files in place can fail because of transient file locks or permission timing.
  - Always copy the tested portable directory into a clean release staging directory while excluding runtime artifacts, then zip the staging directory.
  - Run `tools/check_sensitive_artifacts.py` against the staged portable directory before creating the portable zip.
- When bumping bundled proxy runtimes, update workflow env vars `XRAY_TAG` / `XRAY_ASSET` / `XRAY_SHA256` / `SING_BOX_TAG` / `SING_BOX_ASSET` / `SING_BOX_SHA256` intentionally in the same commit.
- Every Windows release must remain a complete bundle for a clean target machine:
  - include all Python runtime dependencies required by exchange, proxy, onchain, crypto, and GUI paths
  - include hidden imports for newly added internal modules when PyInstaller cannot discover them automatically
  - include pinned proxy runtime binaries and any required supporting files
  - ensure both onefile and portable packages pass smoke tests before publishing
  - ensure release zips do not contain local runtime state (`data/`, logs, query results, withdraw results, or relay wallet files)

Current release asset naming:
- `xiaojun-bn-<tag>-windows-x64.exe`
- `xiaojun-bn-<tag>-windows-x64.zip`
- `xiaojun-bn-<tag>-windows-x64-portable.zip`

If new internal modules are added, update the workflow hidden imports when needed so packaged builds remain complete. If selftest modules are split, keep `tools/pyinstaller_common_args.py` and `tests/test_packaging_args.py` in sync.
