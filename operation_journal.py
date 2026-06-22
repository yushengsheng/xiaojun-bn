#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import uuid
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

    def __init__(self, file_path: Path, *, secret_box=None):
        self.file_path = Path(file_path)
        self.secret_box = secret_box or SECRET_BOX
        self._lock = threading.RLock()

    def _load_locked(self) -> dict[str, object]:
        if not config_or_backup_exists(self.file_path):
            return {"version": self.VERSION, "entries": []}
        raw, _recovered = _load_json_with_backup(
            self.file_path,
            validator=lambda value: self._validate_payload(value),
            secret_box=self.secret_box,
            config_context=secure_config_context(self.file_path),
            migrate_legacy=True,
        )
        return dict(raw)

    def _save_locked(self, payload: dict[str, object]) -> None:
        _atomic_write_json_with_backup(
            self.file_path,
            payload,
            secret_box=self.secret_box,
            config_context=secure_config_context(self.file_path),
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
                if list(payload.get("task_keys") or []) != clean_task_keys:
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

    def finish_batch(self, batch_id: str, *, success: int, failed: int, interrupted: bool = False) -> None:
        """Persist a resumable terminal/partial result for an operation batch."""
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
