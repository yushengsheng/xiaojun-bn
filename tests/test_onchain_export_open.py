"""Export buttons open the document itself through the platform boundary."""
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from page_onchain_base import OnchainTransferPageBase


class ExportOpenTests(unittest.TestCase):
    def test_export_buttons_open_documents_on_macos_and_windows(self):
        for platform in ('darwin', 'win32'):
            with self.subTest(platform=platform), TemporaryDirectory() as directory:
                page=object.__new__(OnchainTransferPageBase)
                page.log=Mock()
                manual=Path(directory)/'待人工 文档.txt'
                failed=Path(directory)/'失败账号.txt'
                with patch('page_onchain_base.RELAY_MANUAL_EXPORT_FILE',manual), \
                     patch('page_onchain_base.RELAY_FAILED_EXPORT_FILE',failed), \
                     patch('page_onchain_base.sys.platform',platform), \
                     patch('page_onchain_base.subprocess.run') as run, \
                     patch('page_onchain_base.messagebox.showinfo') as info, \
                     patch('page_onchain_base.os.startfile',create=True) as start:
                    if platform=='darwin':
                        page.open_relay_manual_export_file()
                        page.open_relay_failed_export_file()
                        self.assertEqual([call.args[0] for call in run.call_args_list],
                                         [['open','-t',str(manual)],['open','-t',str(failed)]])
                        start.assert_not_called()
                    else:
                        page.open_relay_manual_export_file()
                        page.open_relay_failed_export_file()
                        self.assertEqual([call.args[0] for call in start.call_args_list],[str(manual),str(failed)])
                        run.assert_not_called()
                    info.assert_not_called()
                    self.assertTrue(manual.is_file())
                    self.assertTrue(failed.is_file())

    def test_open_failure_is_reported_without_claiming_success(self):
        with TemporaryDirectory() as directory:
            page=object.__new__(OnchainTransferPageBase)
            page.log=Mock()
            with patch('page_onchain_base.sys.platform','darwin'), \
                 patch('page_onchain_base.subprocess.run',side_effect=subprocess.CalledProcessError(1,['open'])), \
                 patch('page_onchain_base.messagebox.showerror') as error:
                page._open_export_file(Path(directory)/'failed.txt','失败账号文件')
            error.assert_called_once()
            page.log.assert_not_called()
