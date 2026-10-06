"""Run only on the clean Linux release runner. Uses no real exchange accounts."""
from pathlib import Path
import argparse
import hashlib
import http.cookiejar
import json
import os
import secrets
import socket
import ssl
import subprocess
import tarfile
import urllib.error
import urllib.request


def run(*args):
    return subprocess.check_output(args, text=True).strip()


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
        command = ['python3', str(root/'source/linux_exchange/install.py'), '--source', str(root/'source'),
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
        for file in ('/', '/app.js', '/style.css'): request(file)
        pid = run('systemctl', 'show', 'xiaojun-exchange-worker.service', '--property=MainPID', '--value')
        request('/api/logout', {}, headers)
        run('systemctl', 'restart', 'xiaojun-exchange-web.service')
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        data = Path('/var/lib/xiaojun-exchange')
        snapshot = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in data.iterdir() if p.is_file()}
        retry = subprocess.run(command, capture_output=True)
        assert retry.returncode != 0, 'Existing installation overwritten'
        assert all(hashlib.sha256((data/name).read_bytes()).hexdigest()==value for name,value in snapshot.items())
        assert pid == run('systemctl','show','xiaojun-exchange-worker.service','--property=MainPID','--value')
        for unit in ('worker.service','web.service','nginx.service','maintenance.timer','logrotate.timer'):
            assert run('systemctl','is-active',f'xiaojun-exchange-{unit}') == 'active'
        run('systemctl','start','xiaojun-exchange-maintenance.service','xiaojun-exchange-logrotate.service')
        assert run('systemctl','show','xiaojun-exchange-worker.service','--property=NRestarts','--value') == '0'
        assert data.stat().st_mode & 0o777 == 0o700
        assert Path('/run/xiaojun-exchange/worker.sock').stat().st_mode & 0o777 == 0o600
        assert sentinel.getsockname()[1] == 35123
        assert '127.0.0.1:18880' in run('ss','-lnt')
        for name, command_name in (('sing-box','version'),('hysteria','version'),('xray','version')):
            run(f'/opt/xiaojun-exchange/proxy-bin/{name}',command_name)
        print('LINUX INSTALL ACCEPTANCE OK: HTTPS, initialization, CSRF, services, isolation, cleanup and overwrite refusal')
    finally:
        sentinel.close()


if __name__ == '__main__':
    main()
