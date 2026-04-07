#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from app_paths import CONFIG_BACKUP_SUFFIX
from core_models import (
    AccountEntry,
    GlobalSettings,
    OnchainPairEntry,
    OnchainSettings,
)
from secret_box import SECRET_BOX

def _fsync_parent_dir(path: Path) -> None:
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
    except Exception:
        return
    try:
        os.fsync(dir_fd)
    except Exception:
        pass
    finally:
        try:
            os.close(dir_fd)
        except Exception:
            pass


def _atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding=encoding) as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_name, path)
        _fsync_parent_dir(path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _config_backup_path(path: Path) -> Path:
    return path.with_name(f"{path.name}{CONFIG_BACKUP_SUFFIX}")


def _safe_write_backup_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    try:
        _atomic_write_text(_config_backup_path(path), content, encoding=encoding)
    except Exception:
        pass


def _atomic_write_text_with_backup(path: Path, content: str, encoding: str = "utf-8") -> None:
    _atomic_write_text(path, content, encoding=encoding)
    _safe_write_backup_text(path, content, encoding=encoding)


def _atomic_write_json_with_backup(path: Path, payload: object, encoding: str = "utf-8") -> None:
    _atomic_write_text_with_backup(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding=encoding)


def _load_json_with_backup(
    path: Path,
    *,
    encoding: str = "utf-8",
    validator: Callable[[object], None] | None = None,
) -> tuple[object, bool]:
    backup_path = _config_backup_path(path)
    candidates: list[tuple[Path, bool]] = []
    if path.exists():
        candidates.append((path, False))
    if backup_path.exists():
        candidates.append((backup_path, True))
    if not candidates:
        raise FileNotFoundError(path)

    errors: list[str] = []
    for candidate_path, is_backup in candidates:
        try:
            text = candidate_path.read_text(encoding=encoding)
            raw = json.loads(text)
            if validator is not None:
                validator(raw)
            if is_backup:
                try:
                    _atomic_write_text(path, text, encoding=encoding)
                except Exception:
                    pass
            elif not backup_path.exists():
                _safe_write_backup_text(path, text, encoding=encoding)
            return raw, is_backup
        except Exception as exc:
            source_name = "备份文件" if is_backup else "主文件"
            errors.append(f"{source_name} {candidate_path.name}: {exc}")
    raise RuntimeError("配置文件读取失败；" + "；".join(errors))


def _allow_list_or_dict(raw: object) -> None:
    if isinstance(raw, (list, dict)):
        return
    raise RuntimeError("配置文件结构无效")


def _require_dict(raw: object) -> None:
    if isinstance(raw, dict):
        return
    raise RuntimeError("配置文件结构无效")


class AccountStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.accounts: list[AccountEntry] = []
        self.one_to_many_addresses: list[str] = []
        self.one_to_many_source_api_key: str = ""
        self.one_to_many_source_api_secret: str = ""
        self.settings = GlobalSettings()
        self.last_load_notice = ""

    def load(self) -> None:
        self.last_load_notice = ""
        if not self.file_path.exists():
            self.accounts = []
            self.one_to_many_addresses = []
            self.one_to_many_source_api_key = ""
            self.one_to_many_source_api_secret = ""
            self.settings = GlobalSettings()
            return

        raw, recovered = _load_json_with_backup(self.file_path, validator=_allow_list_or_dict)
        if recovered:
            self.last_load_notice = f"检测到 {self.file_path.name} 损坏，已自动从备份恢复。"

        # 兼容旧格式：直接是账号列表
        if isinstance(raw, list):
            accounts_raw = raw
            settings_raw = {}
            one_to_many_raw = {}
        elif isinstance(raw, dict):
            accounts_raw = raw.get("accounts", [])
            settings_raw = raw.get("settings", {})
            one_to_many_raw = raw.get("one_to_many", {}) or {}
        else:
            raise RuntimeError("配置文件结构无效")

        loaded: list[AccountEntry] = []
        for item in accounts_raw:
            api_key = SECRET_BOX.decrypt(str(item.get("api_key") or "").strip()).strip()
            api_secret = SECRET_BOX.decrypt(str(item.get("api_secret") or "").strip()).strip()
            address = (item.get("address") or "").strip()
            network = str(item.get("network") or settings_raw.get("network") or "").strip().upper()
            if api_key and api_secret and address:
                loaded.append(AccountEntry(api_key=api_key, api_secret=api_secret, address=address, network=network))

        self.accounts = loaded
        addrs: list[str] = []
        seen_addr: set[str] = set()
        for x in one_to_many_raw.get("addresses", []) or []:
            s = str(x or "").strip()
            if not s or s in seen_addr:
                continue
            seen_addr.add(s)
            addrs.append(s)
        self.one_to_many_addresses = addrs
        self.one_to_many_source_api_key = SECRET_BOX.decrypt(str(one_to_many_raw.get("api_key", "") or "").strip()).strip()
        self.one_to_many_source_api_secret = SECRET_BOX.decrypt(str(one_to_many_raw.get("api_secret", "") or "").strip()).strip()
        try:
            worker_threads = max(1, int(settings_raw.get("worker_threads", 5)))
        except Exception:
            worker_threads = 5
        try:
            delay_seconds = float(settings_raw.get("delay_seconds", 1.0))
            if delay_seconds < 0:
                delay_seconds = 0.0
        except Exception:
            delay_seconds = 1.0

        self.settings = GlobalSettings(
            coin=(settings_raw.get("coin") or "USDT").strip().upper(),
            network=(settings_raw.get("network") or "").strip().upper(),
            amount=(settings_raw.get("amount") or "").strip(),
            delay_seconds=delay_seconds,
            worker_threads=worker_threads,
            mode=(settings_raw.get("mode") or "M2M").strip().upper(),
            random_amount_enabled=bool(settings_raw.get("random_amount_enabled", False)),
            random_amount_min=(settings_raw.get("random_amount_min") or "").strip(),
            random_amount_max=(settings_raw.get("random_amount_max") or "").strip(),
            dry_run=bool(settings_raw.get("dry_run", True)),
        )

    def save(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "accounts": [
                {
                    "api_key": SECRET_BOX.encrypt(a.api_key),
                    "api_secret": SECRET_BOX.encrypt(a.api_secret),
                    "address": a.address,
                    "network": str(a.network or "").strip().upper(),
                }
                for a in self.accounts
            ],
            "settings": asdict(self.settings),
            "one_to_many": {
                "api_key": SECRET_BOX.encrypt(self.one_to_many_source_api_key),
                "api_secret": SECRET_BOX.encrypt(self.one_to_many_source_api_secret),
                "addresses": self.one_to_many_addresses,
            },
        }
        _atomic_write_json_with_backup(self.file_path, payload, encoding="utf-8")

    def upsert_many(self, items: list[AccountEntry]) -> tuple[int, int]:
        """返回 (新增数, 更新数)"""
        index = {a.api_key: i for i, a in enumerate(self.accounts)}
        created = 0
        updated = 0

        for item in items:
            if item.api_key in index:
                self.accounts[index[item.api_key]] = item
                updated += 1
            else:
                self.accounts.append(item)
                index[item.api_key] = len(self.accounts) - 1
                created += 1

        return created, updated

    def delete_by_indices(self, indices: list[int]) -> None:
        s = set(indices)
        self.accounts = [a for i, a in enumerate(self.accounts) if i not in s]

    def delete_addresses_by_indices(self, indices: list[int]) -> None:
        s = set(indices)
        self.one_to_many_addresses = [a for i, a in enumerate(self.one_to_many_addresses) if i not in s]


class OnchainStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.multi_to_multi_pairs: list[OnchainPairEntry] = []
        self.multi_to_multi_drafts: list[dict[str, str]] = []
        self.one_to_many_addresses: list[str] = []
        self.many_to_one_sources: list[str] = []
        self.settings = OnchainSettings()
        self.last_load_notice = ""

    def _build_settings_payload(self) -> dict:
        settings_payload = asdict(self.settings)
        settings_payload["one_to_many_source"] = SECRET_BOX.encrypt(self.settings.one_to_many_source)
        settings_payload["proxy_url"] = SECRET_BOX.encrypt(self.settings.proxy_url)
        return settings_payload

    def _build_transfer_rows_payload(self) -> dict:
        return {
            "multi_to_multi": [
                {
                    "source": SECRET_BOX.encrypt(x.source),
                    "target": x.target,
                }
                for x in self.multi_to_multi_pairs
            ],
            "multi_to_multi_drafts": [
                {
                    "source": SECRET_BOX.encrypt(str(item.get("source", "") or "").strip()),
                    "target": str(item.get("target", "") or "").strip(),
                }
                for item in self.multi_to_multi_drafts
                if str(item.get("source", "") or "").strip() or str(item.get("target", "") or "").strip()
            ],
            "one_to_many": {
                "addresses": list(self.one_to_many_addresses),
            },
            "many_to_one": {
                "sources": [SECRET_BOX.encrypt(x) for x in self.many_to_one_sources],
            },
        }

    def load(self) -> None:
        self.last_load_notice = ""
        if not self.file_path.exists():
            self.multi_to_multi_pairs = []
            self.multi_to_multi_drafts = []
            self.one_to_many_addresses = []
            self.many_to_one_sources = []
            self.settings = OnchainSettings()
            return

        raw, recovered = _load_json_with_backup(self.file_path, validator=_require_dict)
        if recovered:
            self.last_load_notice = f"检测到 {self.file_path.name} 损坏，已自动从备份恢复。"

        settings_raw = raw.get("settings", {}) or {}
        pairs_raw = raw.get("multi_to_multi", []) or []
        drafts_raw = raw.get("multi_to_multi_drafts", []) or []
        one_many_raw = raw.get("one_to_many", {}) or {}
        many_one_raw = raw.get("many_to_one", {}) or {}

        pairs: list[OnchainPairEntry] = []
        pair_seen: set[tuple[str, str]] = set()
        for item in pairs_raw:
            source = SECRET_BOX.decrypt(str(item.get("source", "") or "").strip()).strip()
            target = str(item.get("target", "") or "").strip()
            if not source or not target:
                continue
            key = (source, target)
            if key in pair_seen:
                continue
            pair_seen.add(key)
            pairs.append(OnchainPairEntry(source=source, target=target))

        drafts: list[dict[str, str]] = []
        for item in drafts_raw:
            if not isinstance(item, dict):
                continue
            source = SECRET_BOX.decrypt(str(item.get("source", "") or "").strip()).strip()
            target = str(item.get("target", "") or "").strip()
            if not source and not target:
                continue
            drafts.append({
                "source": source,
                "target": target,
            })

        one_many_list: list[str] = []
        one_many_seen: set[str] = set()
        for x in one_many_raw.get("addresses", []) or []:
            s = str(x or "").strip()
            if not s or s in one_many_seen:
                continue
            one_many_seen.add(s)
            one_many_list.append(s)

        many_one_list: list[str] = []
        many_one_seen: set[str] = set()
        for x in many_one_raw.get("sources", []) or []:
            s = SECRET_BOX.decrypt(str(x or "").strip()).strip()
            if not s or s in many_one_seen:
                continue
            many_one_seen.add(s)
            many_one_list.append(s)

        try:
            delay = float(settings_raw.get("delay_seconds", 1.0))
            if delay < 0:
                delay = 0.0
        except Exception:
            delay = 1.0
        try:
            threads = max(1, int(settings_raw.get("worker_threads", 10)))
        except Exception:
            threads = 10

        self.multi_to_multi_pairs = pairs
        self.multi_to_multi_drafts = drafts
        self.one_to_many_addresses = one_many_list
        self.many_to_one_sources = many_one_list
        source_raw = one_many_raw.get("source", "")
        if not source_raw:
            source_raw = settings_raw.get("one_to_many_source", "")
        target_raw = many_one_raw.get("target", "")
        if not target_raw:
            target_raw = settings_raw.get("many_to_one_target", "")

        self.settings = OnchainSettings(
            mode=str(settings_raw.get("mode", "多对多") or "多对多").strip(),
            network=str(settings_raw.get("network", "ETH") or "ETH").strip().upper(),
            token_symbol=str(settings_raw.get("token_symbol", "") or "").strip().upper(),
            token_contract=str(settings_raw.get("token_contract", "") or "").strip(),
            amount_mode=str(settings_raw.get("amount_mode", "固定数量") or "固定数量").strip(),
            amount=str(settings_raw.get("amount", "") or "").strip(),
            random_min=str(settings_raw.get("random_min", "") or "").strip(),
            random_max=str(settings_raw.get("random_max", "") or "").strip(),
            delay_seconds=delay,
            worker_threads=threads,
            dry_run=bool(settings_raw.get("dry_run", True)),
            use_config_proxy=bool(settings_raw.get("use_config_proxy", False)),
            proxy_url=SECRET_BOX.decrypt(str(settings_raw.get("proxy_url", "") or "").strip()).strip(),
            one_to_many_source=SECRET_BOX.decrypt(str(source_raw or "").strip()).strip(),
            many_to_one_target=str(target_raw or "").strip(),
        )

    def save(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "settings": self._build_settings_payload(),
        }
        payload.update(self._build_transfer_rows_payload())
        payload["one_to_many"]["source"] = SECRET_BOX.encrypt(self.settings.one_to_many_source)
        payload["many_to_one"]["target"] = self.settings.many_to_one_target
        _atomic_write_json_with_backup(self.file_path, payload, encoding="utf-8")

    def save_settings_only(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw, _recovered = _load_json_with_backup(self.file_path, validator=_require_dict)
            payload = dict(raw) if isinstance(raw, dict) else {}
        except FileNotFoundError:
            payload = {}

        payload["settings"] = self._build_settings_payload()
        payload.setdefault("multi_to_multi", [])
        payload.setdefault("one_to_many", {})
        payload.setdefault("many_to_one", {})
        _atomic_write_json_with_backup(self.file_path, payload, encoding="utf-8")

    def save_transfer_lists_only(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw, _recovered = _load_json_with_backup(self.file_path, validator=_require_dict)
            payload = dict(raw) if isinstance(raw, dict) else {}
        except FileNotFoundError:
            payload = {"settings": self._build_settings_payload()}

        rows_payload = self._build_transfer_rows_payload()
        payload.setdefault("settings", self._build_settings_payload())
        payload["multi_to_multi"] = rows_payload["multi_to_multi"]
        payload["multi_to_multi_drafts"] = rows_payload["multi_to_multi_drafts"]
        payload["one_to_many"] = dict(payload.get("one_to_many") or {})
        payload["one_to_many"]["addresses"] = rows_payload["one_to_many"]["addresses"]
        payload["many_to_one"] = dict(payload.get("many_to_one") or {})
        payload["many_to_one"]["sources"] = rows_payload["many_to_one"]["sources"]
        _atomic_write_json_with_backup(self.file_path, payload, encoding="utf-8")

    def upsert_multi_to_multi(self, rows: list[OnchainPairEntry]) -> int:
        existing = {(x.source, x.target) for x in self.multi_to_multi_pairs}
        created = 0
        for row in rows:
            key = (row.source, row.target)
            if key in existing:
                continue
            existing.add(key)
            self.multi_to_multi_pairs.append(row)
            created += 1
        return created

    def upsert_one_to_many_addresses(self, rows: list[str]) -> int:
        existing = set(self.one_to_many_addresses)
        created = 0
        for row in rows:
            s = str(row or "").strip()
            if not s or s in existing:
                continue
            existing.add(s)
            self.one_to_many_addresses.append(s)
            created += 1
        return created

    def upsert_many_to_one_sources(self, rows: list[str]) -> int:
        existing = set(self.many_to_one_sources)
        created = 0
        for row in rows:
            s = str(row or "").strip()
            if not s or s in existing:
                continue
            existing.add(s)
            self.many_to_one_sources.append(s)
            created += 1
        return created

    def delete_multi_to_multi_by_indices(self, indices: list[int]) -> None:
        s = set(indices)
        self.multi_to_multi_pairs = [x for i, x in enumerate(self.multi_to_multi_pairs) if i not in s]

    def delete_one_to_many_by_indices(self, indices: list[int]) -> None:
        s = set(indices)
        self.one_to_many_addresses = [x for i, x in enumerate(self.one_to_many_addresses) if i not in s]

    def delete_many_to_one_by_indices(self, indices: list[int]) -> None:
        s = set(indices)
        self.many_to_one_sources = [x for i, x in enumerate(self.many_to_one_sources) if i not in s]
