#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _is_virtual_environment() -> bool:
    return bool(getattr(sys, "base_prefix", sys.prefix) != sys.prefix or getattr(sys, "real_prefix", None))


def _requirements_are_pinned(path: Path) -> tuple[bool, list[str]]:
    issues: list[str] = []
    if not path.exists():
        return False, [f"{path.name} is missing"]
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            continue
        if "==" not in line:
            issues.append(f"{path.name}:{line_no}: dependency is not pinned: {line}")
    return not issues, issues


def _lock_has_hashes(path: Path) -> tuple[bool, list[str]]:
    """Validate the generated pip lock without requiring network access."""
    if not path.exists():
        return False, [f"{path.name} is missing"]
    issues: list[str] = []
    current_requirement = ""
    current_has_hash = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not raw_line.startswith((" ", "\t", "#")) and "==" in line:
            if current_requirement and not current_has_hash:
                issues.append(f"{path.name}: missing hash for {current_requirement}")
            current_requirement = line.split("==", 1)[0].strip()
            current_has_hash = False
        elif current_requirement and "--hash=sha256:" in line:
            current_has_hash = True
    if current_requirement and not current_has_hash:
        issues.append(f"{path.name}: missing hash for {current_requirement}")
    return not issues, issues


def _pinned_requirements(path: Path, *, visited: set[Path] | None = None) -> dict[str, str]:
    visited = visited or set()
    target = path.resolve()
    if target in visited:
        return {}
    visited.add(target)
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            result.update(_pinned_requirements(path.parent / line[3:].strip(), visited=visited))
            continue
        if "==" not in line:
            continue
        name, version = line.split("==", 1)
        normalized_name = name.split("[", 1)[0].strip().lower().replace("_", "-")
        result[normalized_name] = version.strip()
    return result


def _lock_versions(path: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or raw_line.startswith((" ", "\t", "#")) or "==" not in line:
            continue
        name, version = line.split("==", 1)
        versions[name.strip().lower().replace("_", "-")] = version.split("\\", 1)[0].strip()
    return versions


def _lock_matches_sources(lock_path: Path, sources: tuple[Path, ...]) -> tuple[bool, list[str]]:
    if not lock_path.exists():
        return False, [f"{lock_path.name} is missing"]
    expected: dict[str, str] = {}
    for source in sources:
        expected.update(_pinned_requirements(source))
    actual = _lock_versions(lock_path)
    issues = [
        f"{lock_path.name}: {name}=={version} does not match lock"
        for name, version in expected.items()
        if actual.get(name) != version
    ]
    return not issues, issues


def _run_pip_check() -> tuple[int, str]:
    proc = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return proc.returncode, proc.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check the local Python environment used by xiaojun-bn.")
    parser.add_argument("--skip-pip-check", action="store_true", help="Do not run python -m pip check.")
    parser.add_argument("--strict-pip-check", action="store_true", help="Fail if python -m pip check reports conflicts.")
    args = parser.parse_args(argv)

    print(f"python={sys.executable}")
    print(f"virtualenv={'yes' if _is_virtual_environment() else 'no'}")
    if not _is_virtual_environment():
        print("warning=not running inside a virtual environment")

    ok = True
    for requirement_file in (ROOT / "requirements.txt", ROOT / "requirements-build.txt"):
        pinned, issues = _requirements_are_pinned(requirement_file)
        print(f"{requirement_file.name}-pinned={'yes' if pinned else 'no'}")
        for issue in issues:
            print(f"warning={issue}")
        ok = ok and pinned

    for lock_file in (ROOT / "requirements.lock", ROOT / "requirements-build.lock"):
        hashed, issues = _lock_has_hashes(lock_file)
        print(f"{lock_file.name}-hashes={'yes' if hashed else 'no'}")
        for issue in issues:
            print(f"warning={issue}")
        ok = ok and hashed

    lock_pairs = (
        (ROOT / "requirements.lock", (ROOT / "requirements.txt",)),
        (ROOT / "requirements-build.lock", (ROOT / "requirements-build.txt",)),
    )
    for lock_file, sources in lock_pairs:
        matches, issues = _lock_matches_sources(lock_file, sources)
        print(f"{lock_file.name}-sources={'yes' if matches else 'no'}")
        for issue in issues:
            print(f"warning={issue}")
        ok = ok and matches

    if not args.skip_pip_check:
        code, output = _run_pip_check()
        print(f"pip-check={'ok' if code == 0 else 'failed'}")
        if output:
            print(output)
        if code != 0 and args.strict_pip_check:
            ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
