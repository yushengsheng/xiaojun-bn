#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app_paths import CONFIG_BACKUP_SUFFIX, RELAY_WALLET_FILE
from core_models import EvmToken, GeneratedWalletEntry
from secure_config import FORMAT as SECURE_CONFIG_FORMAT
from secure_config import config_context as secure_config_context, seal_config_payload, unseal_config_payload
from secret_box import SECRET_BOX
from stores import _atomic_write_text_with_backup

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime | None = None) -> str:
    current = value or _utc_now()
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decimal_text(value: Decimal | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value != value.to_integral() else format(value, "f")
    return str(value or "").strip()


def _atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".relay-wallet.", suffix=".tmp", dir=str(path.parent))
    try:
        try:
            os.chmod(tmp_name, 0o600)
        except Exception:
            pass
        with os.fdopen(fd, "w", encoding=encoding) as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_name, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


@dataclass
class RelayWalletRecord:
    created_at: str
    expires_at: str
    network: str
    source: str
    target: str
    relay_address: str
    private_key: str
    status: str
    batch_id: str
    reuse_key: str = ""
    token_symbol: str = ""
    token_contract: str = ""
    token_decimals: int = 18
    token_is_native: bool = True
    relay_fee_reserve: str = ""
    transfer_amount: str = ""
    transfer_units: str = ""
    sweep_enabled: bool = False
    sweep_target: str = ""
    fee_funded_txid: str = ""
    fee_funded_nonce: str = ""
    token_funded_txid: str = ""
    token_funded_nonce: str = ""
    token_forward_txid: str = ""
    token_forward_nonce: str = ""
    gas_sweep_txid: str = ""
    gas_sweep_nonce: str = ""
    sweep_resolution: str = ""
    last_error: str = ""
    completed_at: str = ""

    @classmethod
    def from_raw(cls, raw: dict[str, object]) -> RelayWalletRecord:
        return cls(
            created_at=str(raw.get("created_at") or "").strip(),
            expires_at=str(raw.get("expires_at") or "").strip(),
            network=str(raw.get("network") or "").strip().upper(),
            source=str(raw.get("source") or "").strip(),
            target=str(raw.get("target") or "").strip(),
            relay_address=str(raw.get("relay_address") or "").strip(),
            private_key=str(raw.get("private_key") or "").strip(),
            status=str(raw.get("status") or "").strip(),
            batch_id=str(raw.get("batch_id") or "").strip(),
            reuse_key=str(raw.get("reuse_key") or "").strip(),
            token_symbol=str(raw.get("token_symbol") or "").strip().upper(),
            token_contract=str(raw.get("token_contract") or "").strip(),
            token_decimals=max(0, int(raw.get("token_decimals", 18) or 18)),
            token_is_native=bool(raw.get("token_is_native", True)),
            relay_fee_reserve=str(raw.get("relay_fee_reserve") or "").strip(),
            transfer_amount=str(raw.get("transfer_amount") or "").strip(),
            transfer_units=str(raw.get("transfer_units") or "").strip(),
            sweep_enabled=bool(raw.get("sweep_enabled", False)),
            sweep_target=str(raw.get("sweep_target") or "").strip(),
            fee_funded_txid=str(raw.get("fee_funded_txid") or "").strip(),
            fee_funded_nonce=str(raw.get("fee_funded_nonce") or "").strip(),
            token_funded_txid=str(raw.get("token_funded_txid") or "").strip(),
            token_funded_nonce=str(raw.get("token_funded_nonce") or "").strip(),
            token_forward_txid=str(raw.get("token_forward_txid") or "").strip(),
            token_forward_nonce=str(raw.get("token_forward_nonce") or "").strip(),
            gas_sweep_txid=str(raw.get("gas_sweep_txid") or "").strip(),
            gas_sweep_nonce=str(raw.get("gas_sweep_nonce") or "").strip(),
            sweep_resolution=str(raw.get("sweep_resolution") or "").strip(),
            last_error=str(raw.get("last_error") or "").strip(),
            completed_at=str(raw.get("completed_at") or "").strip(),
        )


class RelayWalletFileStore:
    def __init__(self, file_path: Path | None = None, *, operation_journal=None):
        self.file_path = Path(file_path or RELAY_WALLET_FILE)
        self.operation_journal = operation_journal
        self._lock = threading.RLock()
        self._records_cache: list[RelayWalletRecord] | None = None
        self._cache_signature: tuple[int, int] | None = None
        self._dirty = False
        self._defer_writes = 0
        self.ensure_file()

    def ensure_file(self) -> None:
        with self._lock:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path = self.file_path.with_name(f"{self.file_path.name}{CONFIG_BACKUP_SUFFIX}")
            if self.file_path.exists() or backup_path.exists():
                return
            _atomic_write_text(self.file_path, "", encoding="utf-8")

    def _load_records_locked(self) -> list[RelayWalletRecord]:
        if (
            self._records_cache is not None
            and self._cache_signature is not None
            and self._cache_signature == self._file_signature(self.file_path)
        ):
            return list(self._records_cache)
        self.ensure_file()
        backup_path = self.file_path.with_name(f"{self.file_path.name}{CONFIG_BACKUP_SUFFIX}")
        errors: list[str] = []
        for candidate in (self.file_path, backup_path):
            if not candidate.exists():
                continue
            try:
                candidate_text = candidate.read_text(encoding="utf-8")
                records = self._parse_records_text(candidate_text)
                if candidate == backup_path:
                    if self.operation_journal is not None:
                        self.operation_journal.require_recovery("中转钱包文件从备份恢复，须核对中转地址及链上交易")
                    _atomic_write_text_with_backup(self.file_path, candidate_text, encoding="utf-8", strict_backup=True)
                self._records_cache = list(records)
                self._cache_signature = self._file_signature(self.file_path)
                self._dirty = False
                return records
            except Exception as exc:
                errors.append(f"{candidate.name}: {exc}")
        raise RuntimeError("中转钱包文件读取失败：" + "；".join(errors))

    def _is_authenticated_envelope_file_locked(self, path: Path) -> bool:
        if not path.exists():
            return False
        try:
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return False
            raw = json.loads(text)
        except Exception:
            return False
        return (
            isinstance(raw, dict)
            and raw.get("format") == SECURE_CONFIG_FORMAT
            and str(raw.get("ciphertext") or "").strip()
        )

    def _file_signature(self, path: Path) -> tuple[int, int] | None:
        try:
            stat = path.stat()
        except OSError:
            return None
        return (int(stat.st_mtime_ns), int(stat.st_size))

    def _parse_records_text(self, text: str) -> list[RelayWalletRecord]:
        whole_text = text.strip()
        if whole_text:
            try:
                envelope = json.loads(whole_text)
                payload, protected = unseal_config_payload(
                    envelope,
                    secret_box=SECRET_BOX,
                    context=secure_config_context(self.file_path),
                )
                if protected:
                    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
                        raise RuntimeError("中转钱包认证配置结构无效")
                    raw_rows = payload["records"]
                else:
                    raw_rows = None
            except json.JSONDecodeError:
                raw_rows = None
        else:
            raw_rows = []
        records: list[RelayWalletRecord] = []
        rows = raw_rows if raw_rows is not None else text.splitlines()
        for line_no, row in enumerate(rows, start=1):
            if isinstance(row, dict):
                raw = row
            else:
                raw_line = str(row or "").strip()
                if not raw_line or raw_line.startswith("#"):
                    continue
                try:
                    raw = json.loads(raw_line)
                except Exception as exc:
                    raise RuntimeError(f"中转钱包.txt 第 {line_no} 行 JSON 解析失败：{exc}") from exc
            if not isinstance(raw, dict):
                raise RuntimeError(f"中转钱包.txt 第 {line_no} 行格式无效")
            record = RelayWalletRecord.from_raw(raw)
            record.private_key = SECRET_BOX.decrypt(record.private_key).strip()
            if not record.relay_address or not record.private_key:
                raise RuntimeError(f"中转钱包.txt 第 {line_no} 行缺少中转钱包地址或私钥")
            records.append(record)
        return records

    def _save_records_locked(self, records: list[RelayWalletRecord]) -> None:
        rows: list[dict[str, object]] = []
        for record in records:
            payload = asdict(record)
            private_key = str(payload.get("private_key") or "").strip()
            payload["private_key"] = SECRET_BOX.encrypt(private_key) if private_key else ""
            rows.append(payload)
        envelope = seal_config_payload(
            {"records": rows},
            secret_box=SECRET_BOX,
            context=secure_config_context(self.file_path),
        )
        content = json.dumps(envelope, ensure_ascii=False, indent=2) + "\n"
        _atomic_write_text_with_backup(self.file_path, content, encoding="utf-8", strict_backup=True)
        self._records_cache = list(records)
        self._cache_signature = self._file_signature(self.file_path)
        self._dirty = False

    def load_records(self) -> list[RelayWalletRecord]:
        with self._lock:
            return self._load_records_locked()

    def rewrite_encrypted(self) -> int:
        with self._lock:
            records = self._load_records_locked()
            self._save_records_locked(records)
            return len(records)

    def rewrite_encrypted_if_needed(self) -> int:
        """Migrate legacy relay files once; skip already authenticated files without decrypting all rows."""
        with self._lock:
            if self._is_authenticated_envelope_file_locked(self.file_path):
                return 0
            records = self._load_records_locked()
            self._save_records_locked(records)
            return len(records)

    def flush(self) -> None:
        with self._lock:
            if self._dirty and self._records_cache is not None:
                self._save_records_locked(self._records_cache)

    def defer_writes(self):
        return _RelayWalletDeferredWrites(self)

    def append_records(self, records: list[RelayWalletRecord]) -> None:
        if not records:
            return
        with self._lock:
            current = self._load_records_locked()
            current.extend(records)
            self._save_records_locked(current)

    def remove_records(self, record_keys: set[tuple[str, str]]) -> int:
        """Remove records after confirming all tracked balances are zero/below recovery margins."""
        normalized_keys = {
            (str(batch_id or "").strip(), str(relay_address or "").strip())
            for batch_id, relay_address in (record_keys or set())
            if str(relay_address or "").strip()
        }
        if not normalized_keys:
            return 0
        with self._lock:
            records = self._load_records_locked()
            remaining: list[RelayWalletRecord] = []
            removed = 0
            for record in records:
                record_key = (str(record.batch_id or "").strip(), str(record.relay_address or "").strip())
                if record_key in normalized_keys:
                    removed += 1
                    continue
                remaining.append(record)
            if removed > 0:
                self._save_records_locked(remaining)
            return removed

    def update_record(self, relay_address: str, *, batch_id: str = "", new_batch_id: str = "", **changes: object) -> RelayWalletRecord:
        relay_key = str(relay_address or "").strip()
        batch_key = str(batch_id or "").strip()
        if not relay_key:
            raise RuntimeError("中转钱包地址不能为空")
        with self._lock:
            records = self._load_records_locked()
            for idx, record in enumerate(records):
                if record.relay_address != relay_key:
                    continue
                if batch_key and record.batch_id != batch_key:
                    continue
                updated_raw = asdict(record)
                updated_raw.update(changes)
                if new_batch_id:
                    updated_raw["batch_id"] = str(new_batch_id).strip()
                updated = RelayWalletRecord.from_raw(updated_raw)
                records[idx] = updated
                self._records_cache = list(records)
                self._cache_signature = self._file_signature(self.file_path)
                self._dirty = True
                if self._defer_writes <= 0:
                    self._save_records_locked(records)
                return updated
        raise RuntimeError(f"未找到中转钱包记录：{relay_key}")

    def build_record(
        self,
        *,
        batch_id: str,
        network: str,
        source_address: str,
        target_address: str,
        relay_wallet: GeneratedWalletEntry,
        token: EvmToken,
        relay_fee_reserve: Decimal | str | None,
        sweep_enabled: bool,
        sweep_target: str,
        reuse_key: str = "",
    ) -> RelayWalletRecord:
        created_at = _utc_now()
        return RelayWalletRecord(
            created_at=_utc_text(created_at),
            expires_at="",
            network=str(network or "").strip().upper(),
            source=str(source_address or "").strip(),
            target=str(target_address or "").strip(),
            relay_address=str(relay_wallet.address or "").strip(),
            private_key=str(relay_wallet.private_key or "").strip(),
            status="created",
            batch_id=str(batch_id or "").strip(),
            reuse_key=str(reuse_key or "").strip(),
            token_symbol=str(token.symbol or "").strip().upper(),
            token_contract=str(token.contract or "").strip(),
            token_decimals=max(0, int(token.decimals)),
            token_is_native=bool(token.is_native),
            relay_fee_reserve=_decimal_text(relay_fee_reserve),
            sweep_enabled=bool(sweep_enabled),
            sweep_target=str(sweep_target or "").strip(),
        )

class _RelayWalletDeferredWrites:
    def __init__(self, store: RelayWalletFileStore):
        self.store = store

    def __enter__(self):
        with self.store._lock:
            self.store._defer_writes += 1
        return self.store

    def __exit__(self, exc_type, exc, tb) -> bool:
        with self.store._lock:
            self.store._defer_writes = max(0, self.store._defer_writes - 1)
            should_flush = self.store._defer_writes == 0 and self.store._dirty
        if should_flush:
            self.store.flush()
        return False
