"""Run only on the clean Linux release runner. Uses no real exchange accounts."""
from pathlib import Path
import argparse
import hashlib
import http.cookiejar
import json
import os
import secrets
import shutil
from datetime import datetime
import socket
import ssl
import subprocess
import tarfile
import time
import urllib.error
import urllib.request


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def check_log_retention(root):
    # Exercise the installed policy on disposable paths. Repeated hourly checks
    # must not truncate a large log written today or disturb trading services.
    directory = root / 'log-retention'; directory.mkdir()
    unit = run('systemctl', 'cat', 'xiaojun-exchange-logrotate.service')
    assert 'ExecStart=/usr/sbin/logrotate /etc/logrotate.d/xiaojun-exchange' in unit
    assert '--state' not in unit, 'System and project schedulers must share the default rotation state'
    log, cert_log = directory / 'error.log', directory / 'cert.log'
    policy = Path('/etc/logrotate.d/xiaojun-exchange').read_text()
    policy = policy.replace('/var/log/xiaojun-exchange-nginx/error.log', str(log))
    policy = policy.replace('/var/log/xiaojun-exchange-acme/*.log', str(cert_log))
    config, state = directory / 'config', directory / 'state'
    config.write_text(policy)
    today = datetime.now().strftime('%Y-%m-%d')
    state.write_text(f'logrotate state -- version 2\n"{log}" {today}-0:0:0\n')
    with log.open('wb') as target:
        target.write(b'first recent diagnostic\n')
        target.truncate(6 * 1024 * 1024)
    for index in range(2):
        with log.open('ab') as target:
            target.write(f'recent check {index}\n'.encode())
        run('/usr/sbin/logrotate', '--state', str(state), str(config))
    content = log.read_bytes()
    assert b'first recent diagnostic' in content and b'recent check 0' in content and b'recent check 1' in content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('bundle')
    args = parser.parse_args()
    assert os.geteuid() == 0 and not Path('/opt/xiaojun-exchange').exists(), 'Fresh disposable runner required'
    root = Path('build/linux-install-acceptance').resolve(); root.mkdir(parents=True)
    with tarfile.open(args.bundle) as archive:
        for entry in archive.getmembers():
            assert entry.isfile() and not Path(entry.name).is_absolute() and '..' not in Path(entry.name).parts
            path = root/'source'/entry.name; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(archive.extractfile(entry).read()); path.chmod(entry.mode)
    cert, key = root/'cert.pem', root/'key.pem'
    run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
        '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', str(key), '-out', str(cert))
    sentinel = socket.socket(); sentinel.bind(('127.0.0.1', 35123)); sentinel.listen()
    try:
        command = ['python3', str(root/'source/linux_exchange/deploy.py'), '--source', str(root/'source'),
                   '--host', '127.0.0.1', '--tls-cert', str(cert), '--tls-key', str(key)]
        subprocess.run(command, check=True)
        origin = 'https://127.0.0.1:26483'
        cookies = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(cert))),
                                             urllib.request.HTTPCookieProcessor(cookies))
        def request(path, payload=None, headers=None):
            body = None if payload is None else json.dumps(payload).encode()
            req = urllib.request.Request(origin+path, data=body, headers={'Content-Type':'application/json', **(headers or {})})
            with opener.open(req, timeout=15) as response:
                return response, response.read()
        _, body = request('/api/health'); assert json.loads(body)['configured'] is False
        try:
            request('/api/state'); raise AssertionError('Unauthenticated state exposed')
        except urllib.error.HTTPError as exc: assert exc.code == 401
        setup = Path('/var/lib/xiaojun-exchange/setup-token').read_text().strip()
        password = secrets.token_urlsafe(24)
        response, body = request('/api/login', {'password':password, 'setup_token':setup}, {'Origin':origin})
        assert all(x in response.headers['Set-Cookie'].lower() for x in ('secure', 'httponly', 'samesite=strict'))
        csrf = json.loads(body)['csrf']; headers = {'Origin':origin,'X-CSRF-Token':csrf}
        _, body = request('/api/state'); assert json.loads(body)['accounts'] == []
        try:
            request('/api/settings', {'settings':{'max_threads':2}}, {'Origin':origin})
            raise AssertionError('Missing CSRF accepted')
        except urllib.error.HTTPError as exc: assert exc.code == 403
        request('/api/settings', {'settings':{'max_threads':2}}, headers)
        # Verify served assets match the release bundle whose DOM/interaction contract
        # is checked by test_linux_frontend.py, not merely HTTP 200 responses.
        source = root / 'source' / 'linux_exchange' / 'static'
        for path, name in (('/', 'index.html'), ('/app.js', 'app.js'), ('/style.css', 'style.css')):
            response, content = request(path)
            assert response.status == 200 and content == (source / name).read_bytes(), f'Published {path} differs from tested source'
            assert response.headers.get('Cache-Control') == 'no-store', f'{path} must not serve a stale UI'
        pid = run('systemctl', 'show', 'xiaojun-exchange-worker.service', '--property=MainPID', '--value')
        request('/api/logout', {}, headers)
        run('systemctl', 'restart', 'xiaojun-exchange-web.service')
        # Type=simple becomes active before uvicorn binds. Require a successful
        # HTTPS request after restart before checking service isolation/listeners.
        for attempt in range(20):
            try:
                _, body = request('/api/health')
                assert json.loads(body)['configured'] is True
                break
            except (urllib.error.URLError, TimeoutError):
                if attempt == 19:
                    raise
                time.sleep(0.25)
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        data = Path('/var/lib/xiaojun-exchange')
        snapshot = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in data.iterdir() if p.is_file()}
        retry = subprocess.run(['python3', str(root/'source/linux_exchange/install.py'), *command[2:]], capture_output=True)
        assert retry.returncode != 0, 'Existing installation overwritten'
        assert all(hashlib.sha256((data/name).read_bytes()).hexdigest()==value for name,value in snapshot.items())
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        for unit in ('worker.service','web.service','nginx.service','maintenance.timer','logrotate.timer'):
            assert run('systemctl','is-active',f'xiaojun-exchange-{unit}') == 'active'
        run('systemctl','start','xiaojun-exchange-maintenance.service','xiaojun-exchange-logrotate.service')
        check_log_retention(root)
        assert run('systemctl','show','xiaojun-exchange-worker.service','--property=NRestarts','--value') == '0'
        assert data.stat().st_mode & 0o777 == 0o700
        assert Path('/run/xiaojun-exchange/worker.sock').stat().st_mode & 0o777 == 0o600
        assert sentinel.getsockname()[1] == 35123
        assert '127.0.0.1:18880' in run('ss','-lnt')
        for name, command_name in (('sing-box','version'),('hysteria','version'),('xray','version')):
            run(f'/opt/xiaojun-exchange/proxy-bin/{name}',command_name)
        # The automatic entrypoint must be a no-op for an identical release.
        deployment = ['python3', str(root/'source/linux_exchange/deploy.py'), '--source', str(root/'source')]
        run(*deployment)
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        # Upgrade a disposable copy of this verified release to a distinct version.
        # Only service/payload boundaries are real; no exchange account is present.
        updated = root / 'upgrade-source'; shutil.copytree(root/'source', updated)
        numbers = list(map(int, (updated/'version.txt').read_text().strip()[1:].split('.')))
        numbers[2] += 1
        upgraded_version = 'v' + '.'.join(map(str, numbers))
        (updated/'version.txt').write_text(upgraded_version)
        manifest = json.loads((updated/'linux-release-manifest.json').read_text())
        manifest['version.txt'] = hashlib.sha256((updated/'version.txt').read_bytes()).hexdigest()
        (updated/'linux-release-manifest.json').write_text(json.dumps(manifest))
        protected = {p: p.read_bytes() for p in data.glob('*.json*')}
        tls_key = Path('/etc/xiaojun-exchange/tls/privkey.pem').read_bytes()
        upgrade = ['python3', str(updated/'linux_exchange/deploy.py'), '--source', str(updated)]
        run(*upgrade, '--check')
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        run(*upgrade)
        assert (Path('/opt/xiaojun-exchange/current')/'version.txt').read_text() == upgraded_version
        upgraded_pid = run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        assert pid != upgraded_pid
        assert all(p.read_bytes() == content for p, content in protected.items()), 'Upgrade changed encrypted data'
        assert Path('/etc/xiaojun-exchange/tls/privkey.pem').read_bytes() == tls_key
        _, body = request('/api/health'); assert json.loads(body) == dict(configured=True, unlocked=False, busy=False)
        response, body = request('/api/login', {'password':password}, {'Origin':origin})
        _, body = request('/api/state'); assert json.loads(body)['settings']['max_threads'] == 2
        run(*upgrade)
        assert upgraded_pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        downgrade = subprocess.run(deployment, capture_output=True)
        assert downgrade.returncode != 0
        assert upgraded_pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        assert sentinel.getsockname()[1] == 35123
        print('LINUX INSTALL ACCEPTANCE OK: HTTPS, initialization, CSRF, services, isolation, cleanup, automatic install/upgrade, no-op, encrypted data preservation and downgrade refusal')
    finally:
        sentinel.close()


if __name__ == '__main__':
    main()
