"""Installation safety and release integrity without modifying host services."""
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from linux_exchange import install
from tools import build_linux_release as packaging


class LinuxInstallTests(unittest.TestCase):
    def fixture(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name)

    def test_host_injection_and_private_acme_addresses_rejected(self):
        for host in ('127.0.0.1', '10.0.0.1', '8.8.8.8;evil', 'a.example\n}', '../a.example', '::1'):
            with self.subTest(host=host), self.assertRaises(ValueError):
                install.validate_host(host)
        self.assertEqual(install.validate_host('panel.example.com'), 'panel.example.com')
        self.assertEqual(install.validate_host('127.0.0.1', True), '127.0.0.1')

    def test_installed_dependencies_are_not_upgraded_or_restarted(self):
        with patch.object(install.subprocess, 'run', return_value=argparse.Namespace(returncode=0, stdout='installed')), patch.object(install, 'run') as run:
            install.dependencies(False)
            run.assert_not_called()

    def test_only_missing_dependencies_are_installed(self):
        def status(command, **kwargs):
            return argparse.Namespace(returncode=0, stdout='installed' if command[-1] != 'python3-tk' else 'not-installed')
        with patch.object(install.subprocess, 'run', side_effect=status), patch.object(install, 'run') as run:
            install.dependencies(False)
            commands = [call.args for call in run.call_args_list]
            self.assertEqual(commands, [('apt-get','update'), ('apt-get','install','-y','--no-install-recommends','python3-tk')])
            self.assertEqual(run.call_args.kwargs['env']['NEEDRESTART_MODE'], 'l')

    def test_existing_install_refused_before_dependency_changes(self):
        root = self.fixture(); (root / 'existing').mkdir()
        args = argparse.Namespace(host='8.8.8.8', port=26483, backend_port=18880, tls_cert=None, tls_key=None)
        with ExitStack() as stack:
            for name, value in (('ROOT', root/'existing'), ('CONFIG', root/'config'), ('DATA', root/'data'), ('UNITS', root/'units')):
                stack.enter_context(patch.object(install, name, value))
            stack.enter_context(patch.object(install.platform, 'system', return_value='Linux'))
            stack.enter_context(patch.object(install.platform, 'machine', return_value='x86_64'))
            stack.enter_context(patch.object(install.platform, 'freedesktop_os_release', return_value={'ID':'ubuntu','VERSION_ID':'24.04'}, create=True))
            stack.enter_context(patch.object(install.os, 'geteuid', return_value=0, create=True))
            stack.enter_context(patch.object(install.Path, 'is_dir', return_value=True))
            dependencies = stack.enter_context(patch.object(install, 'dependencies'))
            with self.assertRaisesRegex(ValueError, '已有安装'):
                install.preflight(args)
            dependencies.assert_not_called()

    @unittest.skipUnless(os.name == 'posix', 'Linux bind semantics are validated on the Ubuntu release runner')
    def test_conflicting_ports_rejected_without_killing_listener(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); sock.listen()
            port = sock.getsockname()[1]
            other = 26483 if port != 26483 else 26484
            with self.assertRaises(ValueError):
                install.validate_ports(port, other, False)
            self.assertEqual(sock.getsockname()[1], port)
        with self.assertRaises(ValueError):
            install.validate_ports(18880, 18880, False)

    def test_manifest_tampering_and_traversal_rejected(self):
        root = self.fixture(); (root/'version.txt').write_text('v1.2.3')
        expected = hashlib.sha256((root/'version.txt').read_bytes()).hexdigest()
        manifest = root/'linux-release-manifest.json'
        manifest.write_text(json.dumps({'version.txt': expected}))
        self.assertEqual(install.verify_payload(root)[0], 'v1.2.3')
        (root/'version.txt').write_text('v1.2.4')
        with self.assertRaises(ValueError): install.verify_payload(root)
        manifest.write_text(json.dumps({'../outside': expected}))
        with self.assertRaises(ValueError): install.verify_payload(root)

    def test_nginx_is_isolated_and_custom_tls_does_not_bind_80(self):
        custom = install.nginx_config('panel.example.com', 27483, 19880, '/etc/app/cert.pem', '/etc/app/key.pem', False)
        self.assertIn('listen 27483 ssl', custom)
        self.assertIn('127.0.0.1:19880', custom)
        self.assertNotIn('listen 80;', custom)
        self.assertIn('/run/xiaojun-exchange-nginx/nginx.pid', custom)
        challenge = install.nginx_config('8.8.8.8', 26483, 18880)
        self.assertIn('listen 80;', challenge)
        self.assertNotIn('listen 26483', challenge)
        self.assertNotIn('PANEL_SERVER', custom)

    def test_release_contains_only_source_and_verified_binaries(self):
        output = self.fixture()
        with patch.object(packaging, 'proxy_binary', return_value=b'fake-test-executable'):
            asset = packaging.build('v1.2.3', output)
        with tarfile.open(asset) as archive:
            members = archive.getmembers()
            self.assertTrue(all(m.isfile() and not (m.mode & 0o7000) for m in members))
            names = {m.name for m in members}
            self.assertIn('linux_exchange/install.py', names)
            self.assertIn('bin/sing-box', names)
            self.assertFalse(any(p.startswith(('data/', '.linux-tests/', '.venv/', '.git/')) for p in names))
            manifest = json.load(archive.extractfile('linux-release-manifest.json'))
            for name, expected in manifest.items():
                self.assertEqual(hashlib.sha256(archive.extractfile(name).read()).hexdigest(), expected)
        sums = (output/'SHA256SUMS').read_text()
        self.assertIn(hashlib.sha256(asset.read_bytes()).hexdigest(), sums)
        self.assertNotIn('__RELEASE_VERSION__', (output/'install-linux.sh').read_text(encoding='utf-8'))

    def test_runtime_digest_mismatch_blocks_packaging(self):
        root = self.fixture(); (root/'xray').write_bytes(b'wrong')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            packaging.proxy_binary('xray', {'executable_sha256':'0'*64}, root)


if __name__ == '__main__':
    unittest.main()
