#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from operation_journal import OperationJournal
from secret_box import SecretBox


class OperationJournalTests(unittest.TestCase):
    def test_journal_is_encrypted_and_detects_tampering(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            box = SecretBox(root / ".secret.key")
            path = root / "operations.json"
            journal = OperationJournal(path, secret_box=box)
            entry_id = journal.begin("onchain-transfer", "ETH:0xsource", {"nonce": 8, "target": "0xtarget"})
            journal.update(entry_id, "submitted", txid="0xabc")

            raw_text = path.read_text(encoding="utf-8")
            self.assertNotIn("0xtarget", raw_text)
            self.assertEqual(len(journal.active_entries(kind="onchain-transfer")), 1)

            raw = json.loads(raw_text)
            cipher = str(raw["ciphertext"])
            index = len(cipher) // 2
            raw["ciphertext"] = cipher[:index] + ("A" if cipher[index] != "A" else "B") + cipher[index + 1 :]
            path.write_text(json.dumps(raw), encoding="utf-8")
            path.with_name(f"{path.name}.bak").write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                journal.active_entries()

    def test_terminal_entries_are_not_returned_as_active(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "operations.json", secret_box=SecretBox(root / ".secret.key"))
            entry_id = journal.begin("exchange-order", "account", {"order_id": "1"})
            journal.update(entry_id, "completed")
            self.assertEqual(journal.active_entries(), [])

    def test_journal_recovers_when_only_backup_exists(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            path = root / "operations.json"
            journal = OperationJournal(path, secret_box=SecretBox(root / ".secret.key"))
            entry_id = journal.begin("exchange-order", "account", {"order_id": "1"})
            path.unlink()

            active = journal.active_entries(kind="exchange-order")

            self.assertTrue(path.exists())
            self.assertEqual(active[0]["id"], entry_id)

    def test_unfinished_batch_resumes_and_finished_batch_does_not(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "operations.json", secret_box=SecretBox(root / ".secret.key"))
            batch_id, resumed = journal.resume_or_begin_batch(
                "onchain-transfer-batch",
                "BSC",
                fingerprint="request-v1",
                task_keys=["task-a", "task-b"],
            )
            self.assertFalse(resumed)
            task_id = journal.begin(
                "onchain-transfer",
                "BSC:0xsource",
                {"batch_id": batch_id, "task_key": "task-a", "txid": "0xabc"},
            )
            journal.update(task_id, "completed")
            journal.finish_batch(batch_id, success=1, failed=0, interrupted=True)

            resumed_batch_id, resumed = journal.resume_or_begin_batch(
                "onchain-transfer-batch",
                "BSC",
                fingerprint="request-v1",
                task_keys=["task-a", "task-b"],
            )
            self.assertTrue(resumed)
            self.assertEqual(resumed_batch_id, batch_id)
            self.assertEqual(journal.batch_task_states(batch_id, kind="onchain-transfer")["task-a"]["state"], "completed")

            journal.finish_batch(batch_id, success=2, failed=0)
            new_batch_id, resumed = journal.resume_or_begin_batch(
                "onchain-transfer-batch",
                "BSC",
                fingerprint="request-v1",
                task_keys=["task-a", "task-b"],
            )
            self.assertFalse(resumed)
            self.assertNotEqual(new_batch_id, batch_id)

    def test_large_unfinished_batch_retains_header_and_all_task_states(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "operations.json", secret_box=SecretBox(root / ".secret.key"))
            task_keys = [f"task-{index}" for index in range(journal.HISTORY_ENTRY_LIMIT)]
            batch_id, resumed = journal.resume_or_begin_batch(
                "onchain-transfer-batch",
                "BSC",
                fingerprint="large-request-v1",
                task_keys=task_keys,
            )
            self.assertFalse(resumed)

            for task_key in task_keys:
                task_id = journal.begin(
                    "onchain-transfer",
                    "BSC:0xsource",
                    {"batch_id": batch_id, "task_key": task_key},
                )
                journal.update(task_id, "completed")

            resumed_batch_id, resumed = journal.resume_or_begin_batch(
                "onchain-transfer-batch",
                "BSC",
                fingerprint="large-request-v1",
                task_keys=task_keys,
            )
            self.assertTrue(resumed)
            self.assertEqual(resumed_batch_id, batch_id)
            self.assertEqual(len(journal.batch_task_states(batch_id, kind="onchain-transfer")), len(task_keys))

    def test_terminal_history_is_bounded_without_discarding_active_entries(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-operation-journal-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "operations.json", secret_box=SecretBox(root / ".secret.key"))
            for index in range(journal.HISTORY_ENTRY_LIMIT + 20):
                entry_id = journal.begin("exchange-order", "account", {"order_id": str(index)})
                journal.update(entry_id, "completed")

            self.assertEqual(len(journal._load_locked()["entries"]), journal.HISTORY_ENTRY_LIMIT)


if __name__ == "__main__":
    unittest.main()
