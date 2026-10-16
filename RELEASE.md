# Release Process

This project must be released from GitHub Actions on a clean `windows-latest` runner.

## Mandatory release runbook

Before every GitHub update or version release, read this file and
`RELEASE_GUARDRAILS.md` in full. Use the following sequence exactly; do not
switch environments, create a release manually, or try alternate publishing
methods after a failure.

1. Confirm the intended changes and repository state:
   - `git status --short`
   - `git branch --show-current` must be `main`.
   - `git fetch origin --tags`
   - `git rev-parse origin/main`
2. Select the next sequential `v*` tag and prove it is unused both locally and
   remotely:
   - `git tag --list <tag>` must print nothing.
   - `git ls-remote --exit-code --refs origin refs/tags/<tag>` must report no
     matching ref (exit code 2).
3. Use the repository's fixed regression entrypoint. It validates and reuses
   `.venv-regression`, or rebuilds it from `requirements-build.lock` when the
   environment is missing, damaged, or stale. Do not use `/private/tmp` or
   switch Python environments manually.
4. Run the complete local gate once on the final worktree:
   - `python3 tools/run_full_regression.py`
   - In Codex, run the Tk/GUI gate outside the sandbox as required by
     `RELEASE_GUARDRAILS.md`.
   - Continue only after the command ends with `REGRESSION OK`.
5. Commit all intended code, tests, and release-document changes together,
   then push the commit:
   - `git add <intended-files>`
   - `git commit -m "<message>"`
   - `git push origin main`
6. Tag that exact pushed commit and trigger the release workflow:
   - `git tag -a <tag> -m "<tag>"`
   - `git push origin <tag>`
7. Locate the single run of `release-windows-exe.yml` for that tag and wait for
   it instead of triggering another run or creating another tag:
   - `gh run list --workflow release-windows-exe.yml --limit 10`
   - `gh run watch <run-id> --exit-status`
8. Verify the published release and all six expected assets:
   - `gh release view <tag>`
   - `xiaojun-bn-<tag>-windows-x64.exe`
   - `xiaojun-bn-<tag>-windows-x64.zip`
   - `xiaojun-bn-<tag>-windows-x64-portable.zip`
   - `xiaojun-bn-<tag>-linux-amd64.tar.gz`
   - `install-linux.sh`
   - `SHA256SUMS` (Linux archive and installer hashes)
9. Report success only after the workflow and release asset checks both pass.
   On a genuine failure, stop at that step and diagnose it; never hide the
   failure by changing the tag or publishing path.

If a tag-triggered workflow fails before publishing a GitHub Release, use this
single recovery path: confirm that the release does not exist, fix the root
cause on `main`, rerun the complete local gate, push the fix, delete only the
failed local/remote tag, recreate the same version tag on the verified commit,
and wait for its new workflow run. Do not skip to another version number and do
not publish assets manually.

Never overwrite assets in an existing GitHub Release. An existing release is a
hard stop that requires diagnosis rather than `--clobber`.

`RELEASE_GUARDRAILS.md` is mandatory. Any crash such as `Python 意外退出`, exit code `134`, failed GUI selftest, proxy runtime authentication failure, or failed GitHub Actions release is a blocking release issue, not a warning.

Rules:
- Do not build release packages only from a local machine.
- Tag the target commit with `v*` and push the tag to trigger `.github/workflows/release-windows-exe.yml`.
- Keep the workflow as the single source of truth for release packaging.
- The release workflow must continue bundling:
  - Python runtime dependencies collected by PyInstaller
  - pinned proxy runtimes downloaded during CI (`xray`, `sing-box`, `hysteria`)
  - both onefile and portable onedir packages
- Keep the CI and release proxy runtime tags, asset names, and SHA-256 values identical.
- The generated Xray Shadowsocks configuration does not use geo routing. Exclude
  `geoip.dat` and `geosite.dat` from Windows packages while retaining and smoke
  testing `xray.exe`; revisit this rule if Xray routing rules are introduced.
- Keep runtime dependency sources in `requirements.txt` and build dependency sources in `requirements-build.txt`; regenerate and commit `requirements.lock` and `requirements-build.lock` with SHA-256 hashes whenever either source file changes.
- Install CI and release dependencies only with `python -m pip install --require-hashes -r requirements-build.lock`.
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
- When bumping bundled proxy runtimes, update workflow env vars `XRAY_TAG` / `XRAY_ASSET` / `XRAY_SHA256`, `SING_BOX_TAG` / `SING_BOX_ASSET` / `SING_BOX_SHA256`, and `HYSTERIA_TAG` / `HYSTERIA_ASSET` / `HYSTERIA_SHA256` intentionally in the same commit.
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
- `xiaojun-bn-<tag>-linux-amd64.tar.gz`
- `install-linux.sh`
- `SHA256SUMS`

Linux assets are built by the same release workflow on a clean `ubuntu-24.04`
runner. The Linux job installs hash-locked test dependencies, tests the exchange
worker/API and installation safeguards, verifies both archive and executable
hashes for all three proxy runtimes, and tests a fresh systemd/HTTPS installation
with a disposable certificate explicitly trusted by the test client. Its source
bundle uses an allowlist and a per-file manifest; no local data, credentials,
logs, virtual environments or server-specific deployment scripts are included.
The Windows publication job requires this Linux job to succeed, downloads its
verified assets, then publishes all six assets together only after the existing
source audit and both packaged GUI selftests pass. No second publishing path is
permitted. `RELEASE_NOTES.md` describes the version's user-visible changes.

The installer supports Ubuntu 24.04 and Debian 12/13 x86_64 hosts with Python
3.11+ and systemd. The same `install-linux.sh` resolves the latest published
Release on every run (or accepts `--version vX.Y.Z`), verifies its archive, and
dispatches to `linux_exchange/deploy.py`. Fresh hosts use the existing isolated
installer; identical releases are a no-op. Upgrades stage code and locked venvs
before stopping the web entrypoint and rechecking worker idleness. Active or
unknown trading state, custom units/drop-ins, modified payloads, incomplete
transactions and downgrades are refused. Health-check failure restores the
previous code, runtimes and service configuration; existing encrypted data,
credentials, TLS and ports are preserved. Retain previous releases and private
deployment backups; a power loss leaves a journal and blocks further upgrades
until manual recovery. The clean Linux runner tests fresh HTTPS installation,
automatic upgrade, settings/certificate preservation, no-op and downgrade
refusal. Automatic IP/domain TLS uses the pinned Certbot lock and TCP 80;
supplied certificates retain renewal responsibility with the supplying tool.
No deployment starts trading or resumes interrupted commands automatically.
The simple command accepts `IP PORT 'PASSWORD'`. Fresh installation initializes
that password through the existing local worker API and returns locked. For
upgrades an empty password retains the existing password; an explicit new
password prompts for the old one before touching deployment state, then rewraps
the same master key through the existing password-change API. Passwords are
passed to the Python installer over stdin, never subprocess arguments or logs.
Private snapshots preserve password wrappers, ownership and setup state for
rollback; interrupted initialization retains a new key rather than orphaning
data. The Linux acceptance gate tests explicit initialization, changed/retained
passwords and encrypted settings. Isolated service tests cover lost password
write replies with real encryption and accounts/recovery data. See
`LINUX_DEPLOY.md` for the copy-and-fill instructions.

If new internal modules are added, update the workflow hidden imports when needed so packaged builds remain complete. If selftest modules are split, keep `tools/pyinstaller_common_args.py` and `tests/test_packaging_args.py` in sync.
