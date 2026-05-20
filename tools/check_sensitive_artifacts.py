#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


SENSITIVE_EXACT_NAMES = {
    ".secret.key",
    "bot_log.txt",
    "total_asset_result.txt",
    "withdraw_success.txt",
    "中转钱包.txt",
    "中转钱包_待人工处理.txt",
    "中转钱包_失败账号.txt",
}
SENSITIVE_ROOT_RELS = (
    Path("data"),
    Path("logs"),
    Path("bot_log.txt"),
    Path("total_asset_result.txt"),
    Path("withdraw_success.txt"),
    Path("中转钱包.txt"),
    Path("中转钱包_待人工处理.txt"),
    Path("中转钱包_失败账号.txt"),
)
RUNTIME_FILE_PATTERNS = (
    "bot_log.txt.*",
    "*.log",
    "*.log.*",
    "exchange_*_proxy_runtime_*.log",
    "onchain_*_proxy_runtime_*.log",
    "exchange_ss_proxy_*.json",
    "onchain_ss_proxy_*.json",
)


def _norm_rel(path: str) -> str:
    return str(path or "").replace("\\", "/").strip("/")


def _tracked_path_is_sensitive(path: str) -> bool:
    rel = _norm_rel(path)
    if not rel:
        return False
    name = rel.rsplit("/", 1)[-1]
    parts = rel.split("/")
    if parts[0] in {"data", "logs"}:
        return True
    if name in SENSITIVE_EXACT_NAMES:
        return True
    if name.startswith("中转钱包") and name.endswith(".txt"):
        return True
    if name == "bot_log.txt" or name.startswith("bot_log.txt."):
        return True
    if name.endswith(".log") or ".log." in name:
        return True
    return False


def _git_tracked_sensitive_paths(repo_root: Path) -> list[str]:
    try:
        output = subprocess.check_output(
            ["git", "ls-files"],
            cwd=str(repo_root),
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
    except Exception:
        return []
    return sorted(rel for rel in output.splitlines() if _tracked_path_is_sensitive(rel))


def _candidate_roots(root: Path, include_direct_children: bool) -> list[Path]:
    roots = [root]
    if include_direct_children and root.exists():
        for child in root.iterdir():
            if child.is_dir():
                roots.append(child)
    return roots


def _runtime_matches(scan_root: Path, include_direct_children: bool) -> list[Path]:
    found: dict[Path, None] = {}
    for root in _candidate_roots(scan_root, include_direct_children):
        for rel in SENSITIVE_ROOT_RELS:
            candidate = root / rel
            if candidate.exists():
                found[candidate.resolve()] = None
        for pattern in RUNTIME_FILE_PATTERNS:
            for candidate in root.glob(pattern):
                if candidate.exists():
                    found[candidate.resolve()] = None
    return sorted(found, key=lambda p: str(p))


def _remove_paths(paths: list[Path]) -> list[Path]:
    removed: list[Path] = []
    for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except FileNotFoundError:
            continue
        removed.append(path)
    return removed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check release/package roots for local sensitive runtime artifacts.")
    parser.add_argument("--root", action="append", default=[], help="Root directory to scan. Can be passed more than once.")
    parser.add_argument("--git-only", action="store_true", help="Only check tracked Git files, not local runtime files.")
    parser.add_argument("--include-direct-children", action="store_true", help="Also scan each direct child directory of --root.")
    parser.add_argument("--clean-runtime", action="store_true", help="Remove detected local runtime artifacts before checking.")
    args = parser.parse_args(argv)

    repo_root = Path.cwd().resolve()
    tracked = _git_tracked_sensitive_paths(repo_root)
    if tracked:
        print("ERROR: sensitive runtime paths are tracked by Git:", file=sys.stderr)
        for rel in tracked:
            print(f"  - {rel}", file=sys.stderr)
        return 1

    if args.git_only:
        print("Sensitive artifact check OK: no sensitive paths are tracked by Git.")
        return 0

    scan_roots = [Path(item).resolve() for item in (args.root or ["."])]
    matches: list[Path] = []
    for root in scan_roots:
        matches.extend(_runtime_matches(root, args.include_direct_children))
    matches = sorted(dict.fromkeys(matches), key=lambda p: str(p))

    if args.clean_runtime and matches:
        removed = _remove_paths(matches)
        for path in removed:
            print(f"Removed runtime artifact: {path}")
        matches = []
        for root in scan_roots:
            matches.extend(_runtime_matches(root, args.include_direct_children))
        matches = sorted(dict.fromkeys(matches), key=lambda p: str(p))

    if matches:
        print("ERROR: sensitive runtime artifacts detected:", file=sys.stderr)
        for path in matches:
            print(f"  - {path}", file=sys.stderr)
        print("Run with --clean-runtime for disposable build/package directories only.", file=sys.stderr)
        return 1

    print("Sensitive artifact check OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
