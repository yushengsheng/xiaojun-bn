"""Append-only daily logs, retaining today and the previous two Beijing dates."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import shutil
import time

LOG_RETENTION_DAYS = 3
LOG_TIMEZONE = timezone(timedelta(hours=8))


class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        value = datetime.fromtimestamp(record.created, LOG_TIMEZONE)
        return value.strftime(datefmt) if datefmt else value.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"


def log_day(timestamp=None):
    return datetime.fromtimestamp(time.time() if timestamp is None else timestamp, LOG_TIMEZONE).date()


def log_cutoff(timestamp=None):
    day = log_day(timestamp) - timedelta(days=LOG_RETENTION_DAYS - 1)
    return datetime.combine(day, datetime.min.time(), LOG_TIMEZONE).timestamp()


def expired_log(path, *, now=None):
    """Use last-write time, so legacy files and logs spanning midnight are safe."""
    return not path.is_symlink() and path.is_file() and path.stat().st_mtime < log_cutoff(now)


def prune_logs(paths, *, now=None, protected=()):
    protected = set(protected)
    for path in paths:
        try:
            if path not in protected and expired_log(path, now=now):
                path.unlink()
        except OSError:
            pass


class DailyFileHandler(logging.FileHandler):
    """Rotate at Beijing midnight; never discard recent logs by size or count.

    With a prefix, write prefix_YYYYMMDD.log. Otherwise keep the compatibility
    filename and archive it as filename.YYYYMMDD. Restarting always appends.
    """

    def __init__(self, path, *, prefix=None, clock=time.time):
        self.path = Path(path)
        self.prefix = prefix
        self.clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.day = log_day(clock())
        if prefix is None and self.path.exists():
            previous = log_day(self.path.stat().st_mtime)
            if previous < self.day:
                self._archive(previous)
        super().__init__(self._current_path(), mode="a", encoding="utf-8", delay=True)
        self.prune()

    def _current_path(self):
        if self.prefix:
            return self.path.parent / f"{self.prefix}_{self.day:%Y%m%d}.log"
        return self.path

    def _archive(self, day):
        if not self.path.exists() or self.path.is_symlink():
            return
        archive = self.path.with_name(f"{self.path.name}.{day:%Y%m%d}")
        if archive.is_symlink():
            raise OSError("日志归档路径不能是符号链接")
        if archive.exists():
            # Migration from older compatibility logs must preserve both files.
            mtime = max(archive.stat().st_mtime, self.path.stat().st_mtime)
            with archive.open("ab") as target, self.path.open("rb") as source:
                shutil.copyfileobj(source, target)
            import os
            os.utime(archive, (mtime, mtime))
            self.path.unlink()
        else:
            self.path.rename(archive)

    def prune(self):
        pattern = f"{self.prefix}_*.log*" if self.prefix else f"{self.path.name}.*"
        prune_logs(self.path.parent.glob(pattern), now=self.clock(), protected=(Path(self.baseFilename),))

    def emit(self, record):
        try:
            day = log_day(self.clock())
            if day != self.day:
                if self.stream:
                    self.stream.close()
                    self.stream = None
                if self.prefix is None:
                    self._archive(self.day)
                self.day = day
                self.baseFilename = str(self._current_path().absolute())
                self.prune()
            super().emit(record)
        except Exception:
            self.handleError(record)
