#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


class ExchangeLogPurgeTests(unittest.TestCase):
    def test_purge_closes_handlers_and_restarts_clean_log_files(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-log-purge-test-") as tmpdir:
            root = Path(tmpdir)
            script = textwrap.dedent(
                """
                from pathlib import Path
                import app_paths

                root = Path(r'__ROOT__')
                app_paths.LOG_DIR = root / 'data' / 'logs'
                app_paths.LOG_FILE_PATH = root / 'bot_log.txt'

                import exchange_logging

                exchange_logging.logger.info('pre-destroy-account-log-token')
                exchange_logging._runtime_file_handler.flush()
                exchange_logging._compat_file_handler.flush()
                report = exchange_logging.purge_exchange_file_logs()
                if not report.ok:
                    raise SystemExit('purge failed: ' + str(report.failed))
                exchange_logging.logger.info('post-destroy-log-token')
                exchange_logging._runtime_file_handler.flush()
                exchange_logging._compat_file_handler.flush()
                all_logs = [app_paths.LOG_FILE_PATH]
                all_logs.extend(app_paths.LOG_DIR.glob('exchange_runtime_*.log*'))
                content = '\\n'.join(path.read_text(encoding='utf-8') for path in all_logs if path.exists())
                if 'pre-destroy-account-log-token' in content:
                    raise SystemExit('old log content remained')
                if 'post-destroy-log-token' not in content:
                    raise SystemExit('fresh log handler unavailable')
                """
            ).replace("__ROOT__", str(root).replace("\\", "\\\\"))
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
