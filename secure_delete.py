#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path


_CHUNK_SIZE = 1024 * 1024


@dataclass
class SecureDeleteReport:
    """Best-effort direct deletion result; paths are never sent to a trash folder."""

    destroyed: list[Path] = field(default_factory=list)
    missing: list[Path] = field(default_factory=list)
    failed: dict[Path, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failed

    def merge(self, other: "SecureDeleteReport") -> "SecureDeleteReport":
        self.destroyed.extend(other.destroyed)
        self.missing.extend(other.missing)
        self.failed.update(other.failed)
        return self


def _fsync_parent(path: Path) -> None:
    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def secure_unlink(path: Path | str) -> None:
    """Overwrite a regular file once, then unlink it without using a trash folder.

    Filesystems with copy-on-write, SSD wear levelling, snapshots, backups, or an
    external copy cannot promise physical-media erasure. This helper therefore
    guarantees only the application-level operation: no trash move and no
    recoverable path left in the active application data tree.
    """

    candidate = Path(path)
    try:
        info = candidate.lstat()
    except FileNotFoundError:
        return

    if stat.S_ISDIR(info.st_mode):
        raise RuntimeError("refusing to delete directory")

    if stat.S_ISREG(info.st_mode):
        try:
            os.chmod(candidate, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
        try:
            with candidate.open("r+b", buffering=0) as handle:
                remaining = int(info.st_size)
                while remaining > 0:
                    size = min(_CHUNK_SIZE, remaining)
                    handle.write(os.urandom(size))
                    remaining -= size
                handle.flush()
                os.fsync(handle.fileno())
        except FileNotFoundError:
            return

    candidate.unlink()
    _fsync_parent(candidate)


def secure_unlink_all(paths: list[Path] | tuple[Path, ...] | set[Path]) -> SecureDeleteReport:
    report = SecureDeleteReport()
    for raw_path in sorted({Path(path) for path in paths}, key=lambda item: str(item)):
        try:
            if not raw_path.exists() and not raw_path.is_symlink():
                report.missing.append(raw_path)
                continue
            secure_unlink(raw_path)
            report.destroyed.append(raw_path)
        except Exception as exc:
            report.failed[raw_path] = str(exc)
    return report
