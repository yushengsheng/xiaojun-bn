#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace

from operation_journal import OperationJournal
from operation_recovery import build_onchain_recovery_plan
from onchain_transfer_runner import OnchainTransferRunnerMixin
from secret_box import SecretBox


class OnchainRecoveryPlanTests(unittest.TestCase):
    def _plan(self, source: str = "private-key-must-not-be-persisted"):
        return build_onchain_recovery_plan(
            network="bsc",
            mode="多对多",
            coin="USDT",
            amount="1.5",
            random_enabled=False,
            random_min="",
            random_max="",
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
            jobs=[
                ("row-a", source, "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa"),
                ("row-b", source, "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa"),
            ],
        )

    def test_fingerprint_uses_public_identity_not_source_credential(self) -> None:
        first = self._plan("private-key-one")
        second = self._plan("private-key-two")
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertNotIn("private-key-one", first.fingerprint)
        self.assertNotEqual(first.tasks[0].task_key, first.tasks[1].task_key)

    def test_changed_transfer_parameters_start_a_different_plan(self) -> None:
        first = self._plan()
        changed = build_onchain_recovery_plan(
            network="bsc",
            mode="多对多",
            coin="USDT",
            amount="2",
            random_enabled=False,
            random_min="",
            random_max="",
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
            jobs=[("row-a", "secret", "0x0000000000000000000000000000000000000022", "0x00000000000000000000000000000000000000aa")],
        )
        self.assertNotEqual(first.fingerprint, changed.fingerprint)


class _RecoveryRunner(OnchainTransferRunnerMixin):
    def __init__(self, journal: OperationJournal, *, receipt=None, nonce: int = 0):
        self.operation_journal = journal
        self.client = SimpleNamespace(
            get_transaction_receipt=lambda _network, _txid: receipt,
            get_nonce=lambda _network, _source: nonce,
            _int_from_hex=lambda value: int(str(value), 16),
        )
        self.logs: list[str] = []

    def _resolve_wallet(self, source: str):
        return "derived-private-key", "0x00000000000000000000000000000000000000aa"

    @staticmethod
    def _validate_recipient_address(value: str, _label: str) -> str:
        return str(value).lower()

    @staticmethod
    def _decimal_to_text(value) -> str:
        return str(value)

    @staticmethod
    def _mode() -> str:
        return "多对多"

    def log(self, message: str) -> None:
        self.logs.append(message)

    @staticmethod
    def _dispatch_ui(_callback) -> None:
        return None


class OnchainRecoveryIntegrationTests(unittest.TestCase):
    @staticmethod
    def _params():
        return SimpleNamespace(
            network="BSC",
            coin="USDT",
            amount="1.5",
            random_enabled=False,
            random_min=None,
            random_max=None,
            token_contract="0x0000000000000000000000000000000000000011",
            token_decimals=18,
            token_is_native=False,
        )

    def test_completed_task_is_returned_when_same_batch_resumes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _RecoveryRunner(journal)
            jobs = [("row-1", "private-credential", "0x0000000000000000000000000000000000000022")]
            batch_id, plan, completed, resumed = runner._prepare_onchain_recovery_batch(self._params(), jobs)
            self.assertFalse(resumed)
            self.assertEqual(completed, {})

            entry_id = journal.begin(
                "onchain-transfer",
                "BSC:0x00000000000000000000000000000000000000aa",
                {"batch_id": batch_id, "task_key": plan.tasks[0].task_key, "amount": "1.5"},
            )
            journal.update(entry_id, "completed")
            journal.finish_batch(batch_id, success=1, failed=0, interrupted=True)

            resumed_id, resumed_plan, resumed_completed, resumed = runner._prepare_onchain_recovery_batch(
                self._params(), jobs
            )
            self.assertTrue(resumed)
            self.assertEqual(resumed_id, batch_id)
            self.assertEqual(resumed_plan.tasks[0].task_key, plan.tasks[0].task_key)
            self.assertIn(plan.tasks[0].task_key, resumed_completed)

    def test_unconfirmed_nonce_blocks_duplicate_broadcast(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-recovery-runner-test-") as tmpdir:
            root = Path(tmpdir)
            journal = OperationJournal(root / "journal.json", secret_box=SecretBox(root / ".secret.key"))
            runner = _RecoveryRunner(journal, receipt=None, nonce=7)
            journal.begin(
                "onchain-transfer",
                "BSC:0x00000000000000000000000000000000000000aa",
                {
                    "network": "BSC",
                    "source_address": "0x00000000000000000000000000000000000000aa",
                    "nonce": 7,
                    "txid": "0x" + "a" * 64,
                },
            )
            jobs = [("row-1", "private-credential", "0x0000000000000000000000000000000000000022")]

            self.assertFalse(runner._reconcile_pending_onchain_operations("BSC", jobs))
            self.assertTrue(any("已阻止重复转账" in line for line in runner.logs))


if __name__ == "__main__":
    unittest.main()
