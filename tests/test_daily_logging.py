"""Time retention, restart continuity and actual chain-page persistence."""
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from daily_logging import DailyFileHandler, LOG_TIMEZONE, log_day


class DailyLoggingTests(unittest.TestCase):
    def fixture(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name)

    def emit(self, handler, text, timestamp):
        handler.handle(logging.LogRecord('test', logging.INFO, __file__, 0, text, (), None))
        handler.flush()
        os.utime(handler.baseFilename, (timestamp, timestamp))

    def test_midnight_rotation_and_restart_append_without_size_or_count_limit(self):
        root = self.fixture()
        current = [datetime(2026, 10, 8, 23, 59, 59, tzinfo=LOG_TIMEZONE).timestamp()]
        path = root / 'bot_log.txt'
        handler = DailyFileHandler(path, clock=lambda: current[0])
        self.emit(handler, 'first task', current[0])
        handler.close()
        handler = DailyFileHandler(path, clock=lambda: current[0])
        self.emit(handler, 'second task', current[0])
        self.assertEqual(path.read_text().splitlines(), ['first task', 'second task'])
        # Recent legacy backups survive migration, regardless of count or size.
        for index in range(15):
            recent = root / f'bot_log.txt.{index}'
            with recent.open('wb') as f: f.truncate(5 * 1024 * 1024)
            os.utime(recent, (current[0], current[0]))
        current[0] += 2
        self.emit(handler, 'new day', current[0])
        self.assertEqual((root/'bot_log.txt.20261008').read_text().splitlines(), ['first task', 'second task'])
        self.assertEqual(path.read_text().strip(), 'new day')
        self.assertEqual(len(list(root.glob('bot_log.txt.*'))), 16)
        current[0] += 3 * 86400
        self.emit(handler, 'after gap', current[0])
        self.assertEqual(path.read_text().strip(), 'after gap')
        self.assertEqual(list(root.glob('bot_log.txt.*')), [])
        handler.close()

    def test_dated_logs_keep_latest_three_days_and_protect_symlinks(self):
        root = self.fixture()
        today = datetime(2026, 10, 8, 0, 0, tzinfo=LOG_TIMEZONE)
        times = [(today-timedelta(days=i)).timestamp() for i in range(4)]
        current = [times[-1]]
        handler = DailyFileHandler(root/'exchange_runtime.log', prefix='exchange_runtime', clock=lambda: current[0])
        for day in reversed(times):
            current[0] = day
            self.emit(handler, str(log_day(day)), day)
        self.assertFalse((root/'exchange_runtime_20261005.log').exists())
        for date in ('20261006', '20261007', '20261008'):
            self.assertTrue((root/f'exchange_runtime_{date}.log').exists())
        protected = root/'orders.json'; protected.write_text('order history')
        os.utime(protected, (times[-1], times[-1]))
        if os.name == 'posix':
            link = root/'exchange_runtime_old.log'; link.symlink_to(protected)
            handler.prune()
            self.assertTrue(link.is_symlink())
            self.assertEqual(protected.read_text(), 'order history')
        handler.close()

    def test_chain_page_saves_all_rows_and_redacts_private_keys(self):
        root = self.fixture()
        script = textwrap.dedent('''
            from pathlib import Path
            from types import SimpleNamespace
            from unittest.mock import patch
            import app_paths
            root = Path(__ROOT__)
            app_paths.LOG_DIR = root/'logs'
            app_paths.LOG_FILE_PATH = root/'bot_log.txt'
            import page_onchain_base as page
            import exchange_logging
            owner = SimpleNamespace(log_text=None, root=None, _is_problem_log_text=lambda text: False)
            key = 'a'*64
            with patch.object(page, 'queue_log_row'):
                for index in range(1500):
                    page.OnchainTransferPageBase.log(owner, f'账号 #{index} 收到 USDT 1.25 交易 0x123')
                page.OnchainTransferPageBase.log(owner, 'private_key=' + key)
            logs = list((root/'logs').glob('onchain_runtime_*.log'))
            assert len(logs) == 1
            content = logs[0].read_text()
            assert content.count('收到 USDT 1.25') == 1500
            assert '账号 #0' in content and '账号 #1499' in content
            assert key not in content and '0x123' in content
            exchange_logging.logger.info('exchange row')
            assert 'exchange row' in root.joinpath('bot_log.txt').read_text()
            assert '收到 USDT' not in root.joinpath('bot_log.txt').read_text()
        ''').replace('__ROOT__', repr(str(root)))
        result = subprocess.run([sys.executable, '-c', script], cwd=Path(__file__).resolve().parents[1],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)


if __name__ == '__main__':
    unittest.main()
