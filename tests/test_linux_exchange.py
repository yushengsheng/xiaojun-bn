"""Server tests execute in separate processes; desktop globals stay untouched."""
import os
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.name == 'posix', 'Unix socket server is validated on the Ubuntu release runner')
class LinuxServerTests(unittest.TestCase):
    def run_script(self, script, *args):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        result = subprocess.run([sys.executable, str(ROOT/"tests"/script), *args],
                                cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def run_case(self, case):
        self.run_script("linux_exchange_cases.py", case)

    def test_actual_exchange_controllers_without_network(self):
        self.run_script("linux_controller_acceptance.py", str(ROOT))

    def test_desktop_import_search_copy_and_per_run_options(self):
        self.run_case("desktop")

    def test_credentials_authentication_and_sessions(self):
        self.run_case("auth")

    def test_encrypted_persistence_and_interrupted_commands(self):
        self.run_case("persistence")

    def test_shutdown_serializes_checkpoints_and_disk_failure_does_not_crash_worker(self):
        self.run_case("checkpoint")

    def test_all_exchange_task_dispatch_and_selection(self):
        self.run_case("dispatch")

    def test_running_settings_are_staged_and_validated(self):
        self.run_case("settings")

    def test_headless_asset_query_and_stock_trading(self):
        self.run_case("execution")

    def test_failed_result_retries_five_times_and_same_worker_continues_next_account(self):
        self.run_case("failed_result_next_account")

    def test_cleanup_retries_five_times_and_same_worker_continues_rounds_and_accounts(self):
        self.run_case("cleanup_next_account")

    def test_web_authentication_csrf_and_credentials(self):
        self.run_case("web")

    def test_web_stops_with_an_open_event_stream(self):
        self.run_case("web_shutdown")

    def test_automatic_cleanup_preserves_recovery(self):
        self.run_case("maintenance")

    def test_worker_socket_and_single_instance(self):
        self.run_case("socket")
