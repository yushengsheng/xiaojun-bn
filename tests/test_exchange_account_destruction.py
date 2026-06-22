#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from exchange_account_destruction import (
    destroy_exchange_account_artifacts,
    scrub_exchange_strategy_account_credentials,
)
from secure_config import config_context
from secret_box import SecretBox
from stores import _atomic_write_json_with_backup, _load_json_with_backup


class ExchangeAccountDestructionTests(unittest.TestCase):
    def test_scrub_preserves_strategy_parameters_and_removes_credentials(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-destroy-strategy-test-") as tmpdir:
            root = Path(tmpdir)
            path = root / "exchange_strategy_settings.json"
            box = SecretBox(root / ".secret.key")
            _atomic_write_json_with_backup(
                path,
                {
                    "api_key": box.encrypt("api-key-value"),
                    "api_secret": box.encrypt("api-secret-value"),
                    "withdraw_address": "0x1234567890",
                    "spot_symbol": "BNBUSDT",
                    "premium_delta": "0.01",
                },
                secret_box=box,
                config_context=config_context(path),
            )

            self.assertTrue(scrub_exchange_strategy_account_credentials(path, secret_box=box))
            payload, _recovered = _load_json_with_backup(
                path,
                secret_box=box,
                config_context=config_context(path),
            )
            self.assertEqual(payload["api_key"], "")
            self.assertEqual(payload["api_secret"], "")
            self.assertEqual(payload["withdraw_address"], "")
            self.assertEqual(payload["spot_symbol"], "BNBUSDT")
            self.assertEqual(payload["premium_delta"], "0.01")

            path.unlink()
            backup_payload, recovered = _load_json_with_backup(
                path,
                secret_box=box,
                config_context=config_context(path),
            )
            self.assertTrue(recovered)
            self.assertEqual(backup_payload["api_secret"], "")

    def test_destroy_removes_only_exchange_account_artifacts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-destroy-artifacts-test-") as tmpdir:
            root = Path(tmpdir)
            data = root / "data"
            logs = data / "logs"
            data.mkdir()
            logs.mkdir()
            accounts = data / "accounts.json"
            journal = data / "exchange_operations.json"
            withdraw = data / "withdraw_success.txt"
            total = data / "total_asset_result.txt"
            exchange_targets = (
                accounts,
                accounts.with_name("accounts.json.bak"),
                journal,
                journal.with_name("exchange_operations.json.bak"),
                withdraw,
                total,
                data / ".accounts.json.interrupted.tmp",
                data / ".exchange_operations.json.interrupted.tmp",
                data / ".withdraw_success.txt.interrupted.tmp",
                data / ".total_asset_result.txt.interrupted.tmp",
                data / "exchange_ss_proxy_123_token.json",
                data / "exchange_ss_proxy_xray_123_token.json",
                data / "exchange_proxy_runtime_123.log",
            )
            for index, target in enumerate(exchange_targets):
                target.write_text(f"account-data-{index}", encoding="utf-8")

            preserved = (
                data / "exchange_strategy_settings.json",
                data / "exchange_proxy_settings.json",
                data / "onchain.json",
                data / "onchain_operations.json",
                data / "onchain_ss_proxy_123_token.json",
                data / "onchain_proxy_runtime_123.log",
                logs / "relay_sweep.jsonl",
            )
            for target in preserved:
                target.write_text("must-remain", encoding="utf-8")

            report = destroy_exchange_account_artifacts(
                accounts_file=accounts,
                operation_journal_file=journal,
                withdraw_result_file=withdraw,
                total_asset_result_file=total,
                runtime_dir=data,
            )

            self.assertTrue(report.ok)
            self.assertTrue(report.destroyed)
            self.assertTrue(all(not target.exists() for target in exchange_targets))
            self.assertTrue(all(target.read_text(encoding="utf-8") == "must-remain" for target in preserved))


if __name__ == "__main__":
    unittest.main()
