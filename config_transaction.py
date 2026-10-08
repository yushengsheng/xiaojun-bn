"""Recoverable two-file configuration updates; runtime credentials stay encrypted."""
from contextlib import contextmanager
import base64
from pathlib import Path

from stores import _load_authenticated_config, _save_authenticated_config, _atomic_write_text, _fsync_parent_dir


def _restore(files, paths):
    allowed = {p.name: p for path in paths for p in (Path(path), Path(str(path) + ".bak"))}
    for name, value in files.items():
        if name not in allowed:
            raise RuntimeError("配置恢复路径无效")
        path = allowed[name]
        if value is None:
            path.unlink(missing_ok=True)
        else:
            _atomic_write_text(path, base64.b64decode(value, validate=True).decode("utf-8"))


def _clear(marker):
    # Removing the primary last is the commit point. An interrupted removal
    # leaves a valid intent that restores the old group on the next startup.
    Path(str(marker) + ".bak").unlink(missing_ok=True)
    _fsync_parent_dir(marker)
    marker.unlink(missing_ok=True)
    _fsync_parent_dir(marker)


def recover(marker, paths):
    marker = Path(marker)
    if not marker.exists() and not Path(str(marker) + ".bak").exists():
        return
    data, _ = _load_authenticated_config(marker)
    _restore(data["files"], paths)
    _clear(marker)


@contextmanager
def update_group(marker, paths):
    marker = Path(marker)
    paths = [Path(p) for p in paths]
    recover(marker, paths)
    files = {p.name: base64.b64encode(p.read_bytes()).decode("ascii") if p.exists() else None
             for path in paths for p in (path, Path(str(path) + ".bak"))}
    _save_authenticated_config(marker, {"files": files}, strict_backup=True)
    try:
        yield
        # All primary and backup files must agree before committing the group.
        for path in paths:
            backup = Path(str(path) + ".bak")
            if not backup.exists() or path.read_bytes() != backup.read_bytes():
                raise OSError("配置备份未完整保存")
        _clear(marker)
    except Exception:
        _restore(files, paths)
        _clear(marker)
        raise
