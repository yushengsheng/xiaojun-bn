"""A transient Windows lock must not lose the old or the new configuration."""
import errno
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import stores


def windows_lock(code=32):
    error = PermissionError(errno.EACCES, 'temporarily locked')
    error.winerror = code
    return error


class AtomicWriteRetryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / 'config.json'
        self.path.write_text('previous complete config', encoding='utf-8')

    def test_transient_windows_locks_retry_same_complete_file(self):
        replace = stores.os.replace
        calls = []
        def temporarily_locked(source, destination):
            calls.append(source)
            self.assertEqual(Path(source).read_text(encoding='utf-8'), 'new complete config')
            self.assertEqual(self.path.read_text(encoding='utf-8'), 'previous complete config')
            if len(calls) <= 3:
                raise windows_lock((5, 32, 33)[len(calls) - 1])
            replace(source, destination)
        with patch.object(stores.os, 'replace', side_effect=temporarily_locked), patch.object(stores.time, 'sleep') as sleep:
            stores._atomic_write_text(self.path, 'new complete config')
        self.assertEqual(self.path.read_text(encoding='utf-8'), 'new complete config')
        self.assertEqual(len(set(calls)), 1)
        self.assertEqual(sleep.call_count, 3)
        self.assertFalse(list(self.root.glob('*.tmp')))

    def test_permanent_lock_exhausts_bounded_retries_preserves_old_and_cleans_temp(self):
        with patch.object(stores.os, 'replace', side_effect=windows_lock()) as replace, patch.object(stores.time, 'sleep') as sleep, self.assertRaises(PermissionError):
            stores._atomic_write_text(self.path, 'new complete config')
        self.assertEqual(replace.call_count, 5)
        self.assertEqual(sleep.call_count, 4)
        self.assertLessEqual(sum(c.args[0] for c in sleep.call_args_list), 0.51)
        self.assertEqual(self.path.read_text(encoding='utf-8'), 'previous complete config')
        self.assertFalse(list(self.root.glob('*.tmp')))

    def test_other_filesystem_errors_fail_without_retry_and_preserve_old(self):
        for error in (PermissionError('real permission failure'), OSError(errno.ENOSPC, 'disk full'), windows_lock(19)):
            with self.subTest(error=error), patch.object(stores.os, 'replace', side_effect=error) as replace, patch.object(stores.time, 'sleep') as sleep, self.assertRaises(OSError):
                stores._atomic_write_text(self.path, 'new complete config')
            replace.assert_called_once(); sleep.assert_not_called()
            self.assertEqual(self.path.read_text(encoding='utf-8'), 'previous complete config')
            self.assertFalse(list(self.root.glob('*.tmp')))


if __name__ == '__main__':
    unittest.main()
