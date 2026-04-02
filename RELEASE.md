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
- Smoke test both packaged apps with `--selftest` before publishing release assets.
  - `--selftest` should stay offline-friendly and focus on packaged integrity.
  - If you need to verify outbound EVM RPC reachability manually, use `--selftest-online`.
- When bumping bundled proxy runtimes, update workflow env vars `XRAY_TAG` / `XRAY_ASSET` / `SING_BOX_TAG` / `SING_BOX_ASSET` intentionally in the same commit.

Current release asset naming:
- `xiaojun-bn-<tag>-windows-x64.exe`
- `xiaojun-bn-<tag>-windows-x64.zip`
- `xiaojun-bn-<tag>-windows-x64-portable.zip`

If new internal modules are added, update the workflow hidden imports when needed so packaged builds remain complete.
