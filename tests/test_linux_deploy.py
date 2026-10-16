"""Deployment transactions use real files; service/process boundaries are simulated."""
import argparse
import io
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from linux_exchange import deploy, install

REPO = Path(__file__).resolve().parents[1]


class LinuxDeploymentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.root = self.base / 'app'
        self.config = self.base / 'config'
        self.data = self.base / 'data'
        self.units = self.base / 'units'
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for key, value in (('ROOT', self.root), ('CONFIG', self.config), ('DATA', self.data), ('UNITS', self.units)):
            self.stack.enter_context(patch.object(install, key, value))
        self.args = argparse.Namespace(host=None, port=None, backend_port=None, tls_cert=None, tls_key=None, check=False)
        self.source = self.bundle(self.base / 'source', 'v1.2.4')

    def bundle(self, directory, version):
        directory.mkdir(parents=True)
        (directory / 'version.txt').write_text(version)
        (directory / 'requirements-certbot.lock').write_text('unchanged')
        (directory / 'requirements-linux.lock').write_text('locked')
        (directory / 'linux_exchange/deploy').mkdir(parents=True)
        for unit in (REPO / 'linux_exchange/deploy').glob('*'):
            if unit.suffix in {'.service', '.timer'}:
                shutil.copyfile(unit, directory / 'linux_exchange/deploy' / unit.name)
        pins = {name: {'executable_sha256': hashlib.sha256(name.encode()).hexdigest()} for name in ('xray', 'sing-box', 'hysteria')}
        (directory / 'linux_exchange/deploy/proxy-runtimes.json').write_text(json.dumps(pins))
        (directory / 'bin').mkdir()
        for name in pins:
            (directory / 'bin' / name).write_text(name)
        files = {p.relative_to(directory).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in directory.rglob('*') if p.is_file()}
        (directory / 'linux-release-manifest.json').write_text(json.dumps(files))
        return directory

    def setup_installation(self):
        self.old = self.bundle(self.root / 'releases/v1.2.3', 'v1.2.3')
        (self.root / 'current').symlink_to(self.old, target_is_directory=True)
        for path in (self.config, self.data, self.units, self.root / 'venv', self.root / 'proxy-bin'):
            path.mkdir()
        (self.root / 'venv/original-runtime').write_text('old venv')
        (self.root / 'proxy-bin/original-proxy').write_text('old proxy')
        (self.data / 'encrypted-accounts').write_bytes(b'protected credentials')
        (self.data / 'recovery-journal').write_bytes(b'protected recovery')
        (self.config / 'private-key').write_bytes(b'protected TLS key')
        origin = 'XIAOJUN_PUBLIC_ORIGIN=https://panel.example.com:27483\nXIAOJUN_WORKER_SOCKET=/run/xiaojun-exchange/worker.sock\n'
        for name, prefix in (('xray', 'XRAY'), ('sing-box', 'SING_BOX'), ('hysteria', 'HYSTERIA')):
            origin += f'{prefix}_PATH={self.root}/proxy-bin/{name}\n{prefix}_EXECUTABLE_SHA256={hashlib.sha256(name.encode()).hexdigest()}\n'
        (self.config / 'server.env').write_text(origin)
        for path in (self.old / 'linux_exchange/deploy').glob('*'):
            if path.suffix in {'.service', '.timer'}:
                (self.units / path.name).write_text(path.read_text().replace('--port 18880', '--port 19880'))
        self.systemctl = self.stack.enter_context(patch.object(deploy, 'systemctl', return_value=''))
        self.stack.enter_context(patch.object(deploy, 'active', side_effect=lambda unit: unit in (deploy.WORKER, deploy.WEB, deploy.MAINTENANCE)))
        self.health = self.stack.enter_context(patch.object(deploy, 'health', return_value={'busy': False, 'unlocked': True}))
        self.wait = self.stack.enter_context(patch.object(deploy, 'wait_healthy'))
        self.guard = self.stack.enter_context(patch.object(install, 'install_restart_guard'))
        self.stage = self.stack.enter_context(patch.object(deploy, 'staged_runtime', side_effect=self.fake_stage))
        self.stack.enter_context(patch.object(deploy.shutil, 'disk_usage', return_value=argparse.Namespace(free=10 * 1024**3)))
        self.protected = {p: p.read_bytes() for directory in (self.config, self.data) for p in directory.iterdir() if p.name != 'server.env'}

    def fake_stage(self, source, target, old):
        shutil.copytree(source, target)
        (target / '.venv').mkdir()
        (target / '.venv/new-runtime').write_text('new venv')
        pins = json.loads((target / 'linux_exchange/deploy/proxy-runtimes.json').read_text())
        return target / '.venv', None, pins

    def assert_protected(self):
        for path, data in self.protected.items():
            self.assertEqual(path.read_bytes(), data, str(path))

    def test_health_must_explicitly_report_busy_boolean(self):
        for value in (None, {}, {'busy': 0}, {'busy': 'false'}):
            with self.subTest(value=value), patch.object(deploy, 'call', return_value=value), self.assertRaises(ValueError):
                deploy.health()

    @unittest.skipUnless(os.name == 'posix', 'Real service password transactions run in an isolated Unix process')
    def test_password_changes_preserve_real_encrypted_accounts_and_recovery(self):
        result = subprocess.run([sys.executable, str(REPO/'tests/linux_deploy_password_cases.py')], cwd=REPO,
                                env={**os.environ, 'PYTHONPATH':str(REPO)}, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('LINUX PASSWORD TRANSACTION OK', result.stdout)

    def test_empty_fresh_password_rejected_before_install(self):
        with patch.object(install.platform, 'system', return_value='Linux'), patch.object(install.platform, 'machine', return_value='x86_64'), patch.object(deploy.os, 'geteuid', return_value=0, create=True), patch.object(deploy.sys, 'stdin', argparse.Namespace(buffer=io.BytesIO(b'\0'))), patch.object(install, 'main') as fresh:
            self.assertEqual(deploy.main(['--source', str(self.source), '--host', 'panel.example.com', '--credentials-stdin']), 1)
            fresh.assert_not_called()

    def test_fresh_install_dispatch_keeps_existing_installer(self):
        with patch.object(install.platform, 'system', return_value='Linux'), patch.object(install.platform, 'machine', return_value='x86_64'), patch.object(deploy.os, 'geteuid', return_value=0, create=True), patch.object(install, 'main', return_value=0) as fresh:
            self.assertEqual(deploy.main(['--source', str(self.source), '--host', 'panel.example.com']), 0)
            self.assertIn('panel.example.com', fresh.call_args.args[0])
            self.assertIn('26483', fresh.call_args.args[0])

    def test_stage_preexisting_directory_never_removed(self):
        old = self.bundle(self.base / 'old', 'v1.2.3')
        existing = self.base / 'existing'; existing.mkdir()
        sentinel = existing / 'keep'; sentinel.write_text('preserve')
        with self.assertRaises(ValueError):
            deploy.staged_runtime(self.source, existing, old)
        self.assertEqual(sentinel.read_text(), 'preserve')

    def test_pip_failure_has_bounded_runtime(self):
        old = self.bundle(self.base / 'old', 'v1.2.3')
        with patch.object(install, 'run', side_effect=[None, RuntimeError('network down')]) as command, self.assertRaises(RuntimeError):
            deploy.staged_runtime(self.source, self.base / 'new', old)
        self.assertEqual(command.call_args.kwargs['timeout'], 900)
        self.assertEqual(command.call_args.kwargs['env']['PIP_RETRIES'], '3')

    @unittest.skipUnless(os.name == 'posix', 'POSIX symlinks and service layout are checked on Ubuntu')
    def test_upgrade_and_repeat_preserve_all_runtime_data_and_ports(self):
        self.setup_installation()
        deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.assertEqual((self.root / 'current').resolve().name, 'v1.2.4')
        self.assertEqual((self.root / 'venv/new-runtime').read_text(), 'new venv')
        self.assertIn('--port 19880', (self.units / deploy.WEB).read_text())
        self.assert_protected()
        self.assertFalse((self.root / '.upgrade-incomplete.json').exists())
        self.systemctl.reset_mock(); self.health.reset_mock(); self.stage.reset_mock()
        deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.systemctl.assert_not_called(); self.health.assert_not_called(); self.stage.assert_not_called()

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_busy_worker_is_never_stopped(self):
        self.setup_installation(); self.health.return_value = {'busy': True}
        with self.assertRaisesRegex(ValueError, '仍在运行'):
            deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.stage.assert_not_called(); self.systemctl.assert_not_called(); self.assert_protected()

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_task_started_during_preparation_aborts_without_stopping_worker(self):
        self.setup_installation()
        self.health.side_effect = [{'busy': False}, {'busy': True}]
        with self.assertRaisesRegex(ValueError, '已恢复原版本'):
            deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.assertNotIn(('stop', deploy.WORKER), [c.args for c in self.systemctl.call_args_list])
        self.assertIn(('start', deploy.WEB), [c.args for c in self.systemctl.call_args_list])
        self.assertEqual((self.root / 'current').resolve(), self.old)
        self.assertFalse((self.root / 'releases/v1.2.4').exists()); self.assert_protected()

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_failed_startup_restores_code_venv_proxy_units_and_environment(self):
        self.setup_installation()
        before = {p: p.read_bytes() for p in [self.config / 'server.env', *self.units.iterdir()]}
        self.wait.side_effect = [ValueError('new service failed'), None]
        with self.assertRaisesRegex(ValueError, '已恢复原版本'):
            deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.assertEqual((self.root / 'current').resolve(), self.old)
        self.assertFalse((self.root / 'venv').is_symlink())
        self.assertEqual((self.root / 'venv/original-runtime').read_text(), 'old venv')
        self.assertEqual((self.root / 'proxy-bin/original-proxy').read_text(), 'old proxy')
        for path, content in before.items():
            self.assertEqual(path.read_bytes(), content)
        self.assert_protected(); self.assertFalse((self.root / '.upgrade-incomplete.json').exists())

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_failed_rollback_retains_recovery_journal_and_backup(self):
        self.setup_installation(); self.wait.side_effect = ValueError('services failed')
        with self.assertRaisesRegex(ValueError, '自动回退未完成'):
            deploy.upgrade(self.source, 'v1.2.4', self.args)
        marker = json.loads((self.root / '.upgrade-incomplete.json').read_text())
        self.assertTrue(Path(marker['backup']).is_dir()); self.assert_protected()
        with self.assertRaisesRegex(ValueError, '未完成'):
            deploy.installed_release()

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_custom_unit_and_downgrade_refused_before_staging(self):
        self.setup_installation()
        with self.assertRaisesRegex(ValueError, '降级'):
            deploy.upgrade(self.source, 'v1.2.2', self.args)
        with (self.units / deploy.WEB).open('a') as stream:
            stream.write('\n# administrator settings\n')
        with self.assertRaisesRegex(ValueError, '管理员修改'):
            deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.stage.assert_not_called(); self.assert_protected()

    @unittest.skipUnless(os.name == 'posix', 'POSIX installation layout')
    def test_preflight_does_not_prepare_or_stop_services(self):
        self.setup_installation(); self.args.check = True
        deploy.upgrade(self.source, 'v1.2.4', self.args)
        self.stage.assert_not_called()
        self.assertFalse(any(c.args[0] in ('stop', 'start') for c in self.systemctl.call_args_list))
        self.assert_protected()


if __name__ == '__main__':
    unittest.main()
