"""Bound disposable server files without touching credentials or order recovery."""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import re
import shutil
import time
from daily_logging import expired_log

MIB = 1024 * 1024
MIN_FREE_BYTES = 512 * MIB


def storage_status(directory):
    usage = shutil.disk_usage(directory)
    return dict(free_bytes=usage.free, total_bytes=usage.total,
                ready=usage.free >= MIN_FREE_BYTES and usage.free / usage.total >= .02)


def _pid_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except (PermissionError, ValueError):
        return True


def clean(directory, *, now=None):
    root = Path(directory).resolve()
    now = time.time() if now is None else now
    removed, reclaimed = 0, 0
    def delete(path):
        nonlocal removed, reclaimed
        if path.is_symlink() or not path.is_file():
            return
        size = path.stat().st_size
        path.unlink()
        removed += 1
        reclaimed += size
    # Retain all records in the latest three Beijing calendar days. Size and
    # process restart count must never delete recent transaction diagnostics.
    logs = root / "logs"
    if logs.is_dir() and not logs.is_symlink():
        for p in logs.iterdir():
            if p.is_symlink() or not p.is_file():
                continue
            if not re.fullmatch(r"(?:exchange|onchain|service)_runtime_.*\.log(?:\..*)?", p.name):
                continue
            match = re.search(r"_(\d+)\.log(?:\.\d+)?$", p.name)
            # Date-only filenames are closed on midnight rollover. Legacy PID
            # files still belonging to a live old client remain protected.
            dated = bool(re.fullmatch(r"(?:exchange|onchain|service)_runtime_\d{8}\.log", p.name))
            if not dated and match and _pid_alive(match[1]):
                continue
            if expired_log(p, now=now):
                delete(p)
    for p in root.glob("bot_log.txt.*"):
        if expired_log(p, now=now):
            delete(p)
    # Interrupted atomic writes are safe to delete after 24 h. Final files,
    # .bak backups, journals, account files, and lock data never match this rule.
    for p in root.glob(".*.tmp"):
        if p.is_file() and not p.is_symlink() and now-p.stat().st_mtime > 86400:
            delete(p)
    # Keep complete recent diagnostics; size truncation used to discard logs
    # from the same day. Live child descriptors must not be replaced.
    for p in root.glob("exchange_proxy_runtime_*.log"):
        if p.is_symlink() or not p.is_file():
            continue
        match = re.fullmatch(r"exchange_proxy_runtime_(\d+)\.log", p.name)
        if not match:
            continue
        if not _pid_alive(match[1]) and expired_log(p, now=now):
            delete(p)
    for p in root.glob("exchange_proxy_runtime_*.log.tail"):
        if not p.is_symlink() and p.is_file() and expired_log(p, now=now):
            delete(p)
    # Proxy downloads use disposable, named extraction directories. Never prune
    # version directories, manifests, configured binaries, or live process files.
    cache = root / "proxy_runtimes"
    if cache.exists() and not cache.is_symlink():
        for backend in ("sing-box", "hysteria", "xray"):
            base = cache / backend
            if not base.is_dir() or base.is_symlink():
                continue
            for p in base.iterdir():
                if p.is_symlink() or not p.is_dir() or not p.name.startswith(("sing-box_", "hysteria_", "xray_")):
                    continue
                if now-p.stat().st_mtime > 7*86400:
                    reclaimed += sum(f.stat().st_size for f in p.rglob("*") if f.is_file() and not f.is_symlink())
                    shutil.rmtree(p)
                    removed += 1
    # Large plain result archives are protected user records, not caches. Report
    # their size so the operator can export them, rather than deleting history.
    result_bytes = sum(p.stat().st_size for p in (root/"withdraw_success.txt", root/"total_asset_result.txt") if p.exists())
    return dict(removed=removed, reclaimed_bytes=reclaimed, result_bytes=result_bytes,
                **storage_status(root), checked_at=now)


def main():
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    args = parser.parse_args()
    print(json.dumps(clean(args.data)))


if __name__ == "__main__":
    main()
