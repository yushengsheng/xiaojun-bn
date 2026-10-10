#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import uuid
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

from secure_config import config_context as secure_config_context
from secret_box import SECRET_BOX
from stores import _atomic_write_json_with_backup, _load_json_with_backup, config_or_backup_exists


TERMINAL_STATES = {"completed", "cancelled", "failed", "reconciled"}
BATCH_RECOVERABLE_STATES = {"running", "interrupted", "partial_failed"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class OperationJournal:
    """Encrypted, append-preserving operation state for crash-safe reconciliation."""

    VERSION = 1
    HISTORY_ENTRY_LIMIT = 500
    _path_locks: dict[str, threading.RLock] = {}
    _operation_locks: dict[tuple[str, str, str], threading.RLock] = {}
    _path_locks_guard = threading.Lock()

    def __init__(self, file_path: Path, *, secret_box=None, fail_unconfirmed=False):
        self.file_path = Path(file_path)
        self.secret_box = secret_box or SECRET_BOX
        self.fail_unconfirmed = bool(fail_unconfirmed)
        with self._path_locks_guard:
            self._lock = self._path_locks.setdefault(str(self.file_path.resolve()), threading.RLock())
        if self.fail_unconfirmed:
            with self._lock:
                data = self._load_locked(allow_recovery=True)
                if self._fail_old_exchange_results(data):
                    self._save_locked(data)

    @staticmethod
    def _fail_old_exchange_results(data):
        changed = bool(data.pop("recovery_required", False))
        for entry in data.get("entries", []):
            if not isinstance(entry, dict):
                continue
            if entry.get("state") in TERMINAL_STATES:
                continue
            payload = entry.get("payload", {})
            if entry.get("kind") in {"exchange-funds", "exchange-order"}:
                entry.update(state="failed", updated_at=_utc_now())
                payload["reason"] = "missing-result-on-restart"
                changed = True
            elif entry.get("kind") == "stock-task" and (
                payload.get("conversion") or payload.get("funding_transfer")
                or payload.get("active") and not payload["active"].get("orderId")
            ):
                payload.update(conversion=None, funding_transfer=None,
                               reason="missing-result-on-restart")
                if payload.get("active") and not payload["active"].get("orderId"):
                    payload["active"] = None
                if not payload.get("active") and Decimal(str(payload.get("inventory", "0"))) == 0:
                    entry.update(state="failed", updated_at=_utc_now())
                changed = True
        return changed

    def operation_lock(self, kind: str, scope: str):
        """Serialize check + intent + submission across clients for one account."""
        key = (str(self.file_path.resolve()), str(kind), str(scope))
        with self._path_locks_guard:
            return self._operation_locks.setdefault(key, threading.RLock())

    def _load_locked(self, *, allow_recovery: bool = False) -> dict[str, object]:
        if not config_or_backup_exists(self.file_path):
            return {"version": self.VERSION, "entries": []}
        raw, _recovered = _load_json_with_backup(
            self.file_path,
            validator=lambda value: self._validate_payload(value),
            secret_box=self.secret_box,
            config_context=secure_config_context(self.file_path),
            migrate_legacy=True,
            recovery_marker_key="recovery_required",
        )
        if raw.get("recovery_required") and self.fail_unconfirmed:
            self._fail_old_exchange_results(raw)
            self._save_locked(raw)
        if raw.get("recovery_required") and not allow_recovery:
            raise RuntimeError("操作账本已从备份恢复，须核对交易所及链上记录后解除恢复保护；禁止直接重试资金操作")
        return dict(raw)

    def recovery_snapshot(self) -> dict[str, object]:
        with self._lock:
            return self._load_locked(allow_recovery=True)

    def acknowledge_recovery(self, confirmation: str) -> None:
        if confirmation != "已核对全部操作":
            raise ValueError("须确认已核对全部操作")
        with self._lock:
            data = self._load_locked(allow_recovery=True)
            data.update(recovery_required=False, recovery_reviewed_at=_utc_now())
            self._save_locked(data)

    def require_recovery(self, reason: str) -> None:
        with self._lock:
            data = self._load_locked(allow_recovery=True)
            data.update(recovery_required=True, recovery_reason=str(reason))
            self._save_locked(data)

    def _save_locked(self, payload: dict[str, object]) -> None:
        _atomic_write_json_with_backup(
            self.file_path,
            payload,
            secret_box=self.secret_box,
            config_context=secure_config_context(self.file_path),
            strict_backup=True,
        )

    @staticmethod
    def _entry_payload(entry: object) -> dict[str, object]:
        if not isinstance(entry, dict):
            return {}
        payload = entry.get("payload")
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def _recoverable_batch_ids(cls, entries: list[object]) -> set[str]:
        """Return unfinished batch headers that must never be compacted away."""
        result: set[str] = set()
        for raw in entries:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("state") or "") not in BATCH_RECOVERABLE_STATES:
                continue
            payload = cls._entry_payload(raw)
            if not str(payload.get("fingerprint") or "").strip():
                continue
            if not isinstance(payload.get("task_keys"), list):
                continue
            entry_id = str(raw.get("id") or "").strip()
            if entry_id:
                result.add(entry_id)
        return result

    @classmethod
    def _compact_entries(cls, entries: list[object]) -> list[object]:
        """Keep recovery data intact while bounding only disposable terminal history.

        A transfer batch can contain more tasks than the history limit.  Its header
        and every linked task state therefore remain until the batch is terminal;
        otherwise a restart could mistake completed transfers for new work.
        """
        active_batches = cls._recoverable_batch_ids(entries)
        protected_indexes: set[int] = set()
        history_indexes: list[int] = []

        for index, raw in enumerate(entries):
            if not isinstance(raw, dict):
                continue
            entry_id = str(raw.get("id") or "").strip()
            payload = cls._entry_payload(raw)
            linked_batch_id = str(payload.get("batch_id") or "").strip()
            state = str(raw.get("state") or "")
            if entry_id in active_batches or linked_batch_id in active_batches or state not in TERMINAL_STATES:
                protected_indexes.add(index)
            else:
                history_indexes.append(index)

        retained_indexes = protected_indexes | set(history_indexes[-cls.HISTORY_ENTRY_LIMIT :])
        return [raw for index, raw in enumerate(entries) if index in retained_indexes]

    @classmethod
    def _validate_payload(cls, value: object) -> None:
        if not isinstance(value, dict):
            raise RuntimeError("操作恢复账本结构无效")
        if int(value.get("version", 0) or 0) != cls.VERSION:
            raise RuntimeError("操作恢复账本版本不支持")
        if not isinstance(value.get("entries", []), list):
            raise RuntimeError("操作恢复账本记录无效")

    def begin(self, kind: str, scope: str, payload: dict[str, object]) -> str:
        entry_id = uuid.uuid4().hex
        now = _utc_now()
        entry = {
            "id": entry_id,
            "kind": str(kind or "").strip(),
            "scope": str(scope or "").strip(),
            "state": "prepared",
            "created_at": now,
            "updated_at": now,
            "payload": dict(payload or {}),
        }
        with self._lock:
            data = self._load_locked()
            entries = list(data.get("entries", []))
            entries.append(entry)
            data["entries"] = self._compact_entries(entries)
            self._save_locked(data)
        return entry_id

    def update(self, entry_id: str, state: str, **changes: object) -> None:
        target = str(entry_id or "").strip()
        if not target:
            return
        with self._lock:
            data = self._load_locked()
            entries = list(data.get("entries", []))
            for raw in entries:
                if not isinstance(raw, dict) or str(raw.get("id") or "") != target:
                    continue
                raw["state"] = str(state or "").strip() or str(raw.get("state") or "prepared")
                raw["updated_at"] = _utc_now()
                current_payload = raw.get("payload")
                if not isinstance(current_payload, dict):
                    current_payload = {}
                current_payload.update(changes)
                raw["payload"] = current_payload
                break
            else:
                return
            data["entries"] = self._compact_entries(entries)
            self._save_locked(data)

    def active_entries(self, *, kind: str = "", scope: str = "") -> list[dict[str, object]]:
        with self._lock:
            data = self._load_locked()
        result: list[dict[str, object]] = []
        for raw in data.get("entries", []):
            if not isinstance(raw, dict):
                continue
            if kind and str(raw.get("kind") or "") != kind:
                continue
            if scope and str(raw.get("scope") or "") != scope:
                continue
            if str(raw.get("state") or "") in TERMINAL_STATES:
                continue
            result.append(dict(raw))
        return result

    def resume_or_begin_batch(
        self,
        kind: str,
        scope: str,
        *,
        fingerprint: str,
        task_keys: list[str],
        allow_subset: bool = False,
    ) -> tuple[str, bool]:
        """Return a compatible unfinished batch, or create a new batch.

        A batch is compatible only when both its immutable request fingerprint and
        ordered task keys match. This prevents a changed transfer list from being
        mistaken for a safe continuation of an older request.
        """
        clean_kind = str(kind or "").strip()
        clean_scope = str(scope or "").strip()
        clean_fingerprint = str(fingerprint or "").strip()
        clean_task_keys = [str(value or "").strip() for value in task_keys]
        if not clean_kind or not clean_scope or not clean_fingerprint or not all(clean_task_keys):
            raise RuntimeError("操作恢复批次参数无效")

        with self._lock:
            data = self._load_locked()
            entries = list(data.get("entries", []))
            for raw in reversed(entries):
                if not isinstance(raw, dict):
                    continue
                if str(raw.get("kind") or "") != clean_kind or str(raw.get("scope") or "") != clean_scope:
                    continue
                if str(raw.get("state") or "") not in BATCH_RECOVERABLE_STATES:
                    continue
                payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
                if str(payload.get("fingerprint") or "") != clean_fingerprint:
                    continue
                saved_keys = list(payload.get("task_keys") or [])
                if saved_keys != clean_task_keys and not (allow_subset and set(clean_task_keys) <= set(saved_keys)):
                    continue
                return str(raw.get("id") or ""), True

            entry_id = uuid.uuid4().hex
            now = _utc_now()
            entries.append(
                {
                    "id": entry_id,
                    "kind": clean_kind,
                    "scope": clean_scope,
                    "state": "running",
                    "created_at": now,
                    "updated_at": now,
                    "payload": {
                        "fingerprint": clean_fingerprint,
                        "task_keys": clean_task_keys,
                    },
                }
            )
            data["entries"] = self._compact_entries(entries)
            self._save_locked(data)
            return entry_id, False

    def batch_task_states(self, batch_id: str, *, kind: str) -> dict[str, dict[str, object]]:
        """Return the newest operation entry for each task in one batch."""
        clean_batch_id = str(batch_id or "").strip()
        clean_kind = str(kind or "").strip()
        if not clean_batch_id or not clean_kind:
            return {}
        with self._lock:
            data = self._load_locked()
        result: dict[str, dict[str, object]] = {}
        for raw in data.get("entries", []):
            if not isinstance(raw, dict) or str(raw.get("kind") or "") != clean_kind:
                continue
            payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
            if str(payload.get("batch_id") or "") != clean_batch_id:
                continue
            task_key = str(payload.get("task_key") or "").strip()
            if task_key:
                result[task_key] = dict(raw)
        return result

    def finish_batch(self, batch_id: str, *, success: int, failed: int, interrupted: bool = False, task_kind: str = "") -> None:
        """Persist a resumable terminal/partial result for an operation batch."""
        if task_kind:
            with self._lock:
                data = self._load_locked()
            header = next((e for e in data["entries"] if e.get("id") == batch_id), {})
            keys = header.get("payload", {}).get("task_keys", [])
            states = self.batch_task_states(batch_id, kind=task_kind)
            success = sum(states.get(k, {}).get("state") == "completed" for k in keys)
            failed = len(keys) - success
        if interrupted:
            state = "interrupted"
        elif failed > 0:
            state = "partial_failed"
        else:
            state = "completed"
        self.update(
            batch_id,
            state,
            success=max(0, int(success)),
            failed=max(0, int(failed)),
        )
