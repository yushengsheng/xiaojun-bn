#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path

from app_paths import CONFIG_BACKUP_SUFFIX
from secure_config import config_context as secure_config_context
from secret_box import SECRET_BOX
from secure_delete import SecureDeleteReport, secure_unlink_all
from stores import _atomic_write_json_with_backup, _load_json_with_backup, config_or_backup_exists


_STRATEGY_ACCOUNT_FIELDS = ("api_key", "api_secret", "withdraw_address")


def scrub_exchange_strategy_account_credentials(strategy_path: Path, *, secret_box=None) -> bool:
    """Keep shared trading settings while removing all persisted account credentials."""

    candidate = Path(strategy_path)
    if not config_or_backup_exists(candidate):
        return False
    active_box = secret_box or SECRET_BOX
    raw, _recovered = _load_json_with_backup(
        candidate,
        validator=lambda value: _require_dict(value),
        secret_box=active_box,
        config_context=secure_config_context(candidate),
        migrate_legacy=True,
    )
    cleaned = dict(raw)
    changed = False
    for field_name in _STRATEGY_ACCOUNT_FIELDS:
        if str(cleaned.get(field_name, "") or ""):
            changed = True
        cleaned[field_name] = ""
    _atomic_write_json_with_backup(
        candidate,
        cleaned,
        secret_box=active_box,
        config_context=secure_config_context(candidate),
    )
    return changed


def _require_dict(value: object) -> None:
    if not isinstance(value, dict):
        raise RuntimeError("strategy config structure is invalid")


def _file_and_backup_paths(path: Path) -> set[Path]:
    candidate = Path(path)
    return {
        candidate,
        candidate.with_name(f"{candidate.name}{CONFIG_BACKUP_SUFFIX}"),
    }


def exchange_account_artifact_paths(
    *,
    accounts_file: Path,
    operation_journal_file: Path,
    withdraw_result_file: Path,
    total_asset_result_file: Path,
    runtime_dir: Path,
) -> set[Path]:
    """Return only exchange-account artifacts; shared strategy/proxy data is excluded."""

    paths: set[Path] = set()
    for target in (accounts_file, operation_journal_file):
        paths.update(_file_and_backup_paths(target))
        paths.update(target.parent.glob(f".{target.name}.*.tmp"))
    for target in (withdraw_result_file, total_asset_result_file):
        candidate = Path(target)
        paths.add(candidate)
        paths.update(candidate.parent.glob(f".{candidate.name}.*.tmp"))

    runtime_root = Path(runtime_dir)
    for pattern in (
        "exchange_ss_proxy_*.json",
        "exchange_ss_proxy_xray_*.json",
        "exchange_hysteria2_proxy_*.json",
        "exchange_proxy_runtime_*.log",
    ):
        paths.update(runtime_root.glob(pattern))
    return paths


def destroy_exchange_account_artifacts(
    *,
    accounts_file: Path,
    operation_journal_file: Path,
    withdraw_result_file: Path,
    total_asset_result_file: Path,
    runtime_dir: Path,
) -> SecureDeleteReport:
    """Directly erase persisted exchange-account artifacts without using a trash folder."""

    return secure_unlink_all(
        exchange_account_artifact_paths(
            accounts_file=accounts_file,
            operation_journal_file=operation_journal_file,
            withdraw_result_file=withdraw_result_file,
            total_asset_result_file=total_asset_result_file,
            runtime_dir=runtime_dir,
        )
    )
