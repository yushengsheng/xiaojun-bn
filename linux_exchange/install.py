"""Fresh Linux installation only; never replace a running financial worker."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import time
import urllib.request

ROOT = Path('/opt/xiaojun-exchange')
CONFIG = Path('/etc/xiaojun-exchange')
DATA = Path('/var/lib/xiaojun-exchange')
UNITS = Path('/etc/systemd/system')
NGINX_UNIT = 'xiaojun-exchange-nginx.service'
TIMERS = ('xiaojun-exchange-maintenance.timer', 'xiaojun-exchange-logrotate.timer')


def run(*args, **kwargs):
    kwargs.setdefault('timeout', 900)
    return subprocess.run(list(args), check=True, **kwargs)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_host(value, custom_tls=False):
    value = value.strip().lower()
    try:
        address = ipaddress.IPv4Address(value)
        if not custom_tls and not address.is_global:
            raise ValueError('自动证书需要公网 IPv4 地址')
        return str(address)
    except ipaddress.AddressValueError:
        if len(value) > 253 or '.' not in value or not all(
                re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label)
                for label in value.split('.')):
            raise ValueError('请输入公网 IPv4 或完整域名；目前不支持 IPv6')
        return value


def validate_ports(panel, backend, acme):
    if any(type(p) is not int or not 1024 <= p <= 65535 for p in (panel, backend)) or panel == backend:
        raise ValueError('面板和内部端口须不同，并位于 1024–65535')
    for port in (panel, backend, *([80] if acme else [])):
        for family, address in ((socket.AF_INET, '0.0.0.0'), (socket.AF_INET6, '::')):
            if family == socket.AF_INET6 and not socket.has_ipv6:
                continue
            try:
                with socket.socket(family, socket.SOCK_STREAM) as sock:
                    if family == socket.AF_INET6:
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                    sock.bind((address, port))
            except OSError as exc:
                raise ValueError(f'TCP {port} 已被占用或无法监听；请保留现有服务并选用空闲端口') from exc


def verify_payload(source):
    source = Path(source).resolve()
    manifest = json.loads((source / 'linux-release-manifest.json').read_text())
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError('发行包清单无效')
    for name, expected in manifest.items():
        relative = Path(name)
        path = source / relative
        if relative.is_absolute() or '..' in relative.parts or not path.resolve().is_relative_to(source) or path.is_symlink():
            raise ValueError('发行包包含不安全路径')
        if not path.is_file() or digest(path) != expected:
            raise ValueError(f'发行包校验失败：{name}')
    version = (source / 'version.txt').read_text().strip()
    if not re.fullmatch(r'v\d+\.\d+\.\d+', version):
        raise ValueError('发行版本号无效')
    return version, manifest


def preflight(args):
    if platform.system() != 'Linux' or platform.machine() not in ('x86_64', 'amd64'):
        raise ValueError('安装器仅支持 Linux x86_64')
    if os.geteuid() != 0:
        raise ValueError('请使用 sudo 运行安装器')
    release = platform.freedesktop_os_release()
    if (release.get('ID'), release.get('VERSION_ID')) not in {('ubuntu', '24.04'), ('debian', '12'), ('debian', '13')}:
        raise ValueError('支持 Ubuntu 24.04、Debian 12/13（Python 3.11 或更高）')
    if sys.version_info < (3, 11) or not Path('/run/systemd/system').is_dir():
        raise ValueError('需要 Python 3.11+ 和正在运行的 systemd')
    if any(p.exists() for p in (ROOT, CONFIG, DATA)) or any(UNITS.glob('xiaojun-exchange-*')):
        raise ValueError('检测到已有安装或配置，拒绝覆盖。现有交易和数据保持原样；升级须在维护时进行')
    custom = bool(args.tls_cert or args.tls_key)
    if bool(args.tls_cert) != bool(args.tls_key):
        raise ValueError('--tls-cert 与 --tls-key 必须同时提供')
    args.host = validate_host(args.host, custom)
    if custom:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.tls_cert, args.tls_key)
    validate_ports(args.port, args.backend_port, not custom)
    if shutil.disk_usage('/').free < 1024**3:
        raise ValueError('安装至少需要 1 GiB 可用磁盘空间')


def nginx_config(host, panel, backend, cert=None, key=None, acme=True):
    template = Path(__file__).with_name('deploy') / 'nginx.conf'
    text = template.read_text()
    challenge = f'''server {{
        listen 80; listen [::]:80; server_name {host};
        access_log off;
        location ^~ /.well-known/acme-challenge/ {{
            root /var/lib/xiaojun-exchange-acme-webroot;
            default_type text/plain; try_files $uri =404;
        }}
        location / {{ return 404; }}
    }}'''
    secure = '' if not cert else (Path(__file__).with_name('deploy') / 'nginx-panel.conf').read_text()
    values = {'SERVER_HOST': host, 'PANEL_PORT': str(panel), 'BACKEND_PORT': str(backend),
              'TLS_CERT': str(cert or ''), 'TLS_KEY': str(key or ''),
              'CHALLENGE_SERVER': challenge if acme else '', 'PANEL_SERVER': secure}
    for name in ('CHALLENGE_SERVER', 'PANEL_SERVER', 'SERVER_HOST', 'PANEL_PORT', 'BACKEND_PORT', 'TLS_CERT', 'TLS_KEY'):
        text = text.replace(name, values[name])
    return text


def dependencies(custom):
    # Mask only a previously absent default nginx unit during package installation.
    # The app runs its own nginx; existing system nginx is never restarted.
    packages = ('python3-venv', 'python3-tk', 'nginx', 'logrotate', 'ca-certificates')
    missing = []
    for package in packages:
        status = subprocess.run(['dpkg-query', '-W', '-f=${db:Status-Status}', package],
                                capture_output=True, text=True, check=False)
        if status.returncode != 0 or status.stdout.strip() != 'installed':
            missing.append(package)
    if not missing:
        return
    masked = 'nginx' in missing
    unit = Path('/etc/systemd/system/nginx.service')
    if masked and (unit.exists() or unit.is_symlink() or shutil.which('nginx')):
        raise ValueError('检测到非系统包管理的 nginx 配置，请保留原服务并先安装所需系统依赖')
    if masked:
        run('systemctl', 'mask', 'nginx.service')
    try:
        run('apt-get', 'update')
        run('apt-get', 'install', '-y', '--no-install-recommends', *missing,
            env={**os.environ, 'DEBIAN_FRONTEND': 'noninteractive', 'NEEDRESTART_MODE': 'l'})
    finally:
        if masked:
            run('systemctl', 'unmask', 'nginx.service')


def install_restart_guard(directory=Path('/etc/needrestart/conf.d')):
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / 'xiaojun-exchange.conf'
    source = Path(__file__).parent / 'deploy' / 'needrestart.conf'
    target.write_text(source.read_text(), encoding='utf-8')
    target.chmod(0o644)


def retry_incomplete(args):
    """Discard a proven first-install workspace before any unit is created."""
    if platform.system() != 'Linux' or os.geteuid() != 0:
        raise ValueError('修复未完成安装须在 Linux 使用 sudo')
    marker = ROOT / '.install-incomplete.json'
    if ROOT.is_symlink() or marker.is_symlink() or not marker.is_file():
        raise ValueError('没有可安全重试的首次安装标记；拒绝覆盖已有安装')
    saved = json.loads(marker.read_text())
    version, _ = verify_payload(args.source)
    if saved != dict(version=version, host=args.host, port=args.port, backend_port=args.backend_port, phase='bootstrap'):
        raise ValueError('安装已进入服务配置阶段或参数变化，请保留现场；禁止覆盖交易配置')
    if any(UNITS.glob('xiaojun-exchange-*')) or any(p.is_symlink() for p in (ROOT, CONFIG, DATA)):
        raise ValueError('检测到服务或异常路径，拒绝清理')
    if DATA.exists() and any(p.name != 'setup-token' for p in DATA.iterdir()):
        raise ValueError('检测到运行数据，拒绝清理')
    for path in (DATA, CONFIG, ROOT):
        if path.exists():
            shutil.rmtree(path)


def install(args, version, manifest):
    import pwd
    source = Path(args.source).resolve()
    custom = bool(args.tls_cert)
    dependencies(custom)
    install_restart_guard()
    ROOT.mkdir(mode=0o755)
    marker = ROOT / '.install-incomplete.json'
    marker.write_text(json.dumps(dict(version=version, host=args.host, port=args.port,
                                     backend_port=args.backend_port, phase='bootstrap')))
    target = ROOT / 'releases' / version
    target.mkdir(parents=True)
    for name in manifest:
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, destination)
        destination.chmod(0o755 if name.startswith('bin/') or name.endswith('.sh') else 0o644)
    shutil.copyfile(source / 'linux-release-manifest.json', target / 'linux-release-manifest.json')
    verify_payload(target)
    CONFIG.mkdir(mode=0o700)
    DATA.mkdir(mode=0o700)
    try:
        user = pwd.getpwnam('xiaojun-exchange')
        if user.pw_uid == 0 or user.pw_dir != str(DATA) or user.pw_shell not in ('/usr/sbin/nologin', '/sbin/nologin'):
            raise ValueError('已有同名系统用户不符合专用服务账号要求')
    except KeyError:
        run('useradd', '--system', '--home-dir', str(DATA), '--shell', '/usr/sbin/nologin', 'xiaojun-exchange')
        user = pwd.getpwnam('xiaojun-exchange')
    os.chown(DATA, user.pw_uid, user.pw_gid)
    token = DATA / 'setup-token'
    token.write_text(secrets.token_urlsafe(32)); token.chmod(0o600); os.chown(token, user.pw_uid, user.pw_gid)
    run('python3', '-m', 'venv', str(ROOT / 'venv'))
    python = str(ROOT / 'venv/bin/python')
    run(python, '-m', 'pip', 'install', '--no-cache-dir', '--require-hashes', '-r', str(target / 'requirements-linux.lock'))
    run(python, '-m', 'pip', 'check')
    pins = json.loads((target / 'linux_exchange/deploy/proxy-runtimes.json').read_text())
    proxy = ROOT / 'proxy-bin'; proxy.mkdir()
    env = [f'XIAOJUN_PUBLIC_ORIGIN=https://{args.host}:{args.port}', 'XIAOJUN_WORKER_SOCKET=/run/xiaojun-exchange/worker.sock']
    for name, pin in pins.items():
        binary = proxy / name
        shutil.copyfile(target / 'bin' / name, binary); binary.chmod(0o755)
        if digest(binary) != pin['executable_sha256']:
            raise ValueError(f'{name} 运行时校验失败')
        variable = {'xray': 'XRAY', 'sing-box': 'SING_BOX', 'hysteria': 'HYSTERIA'}[name]
        env.extend((f'{variable}_PATH={binary}', f'{variable}_EXECUTABLE_SHA256={pin["executable_sha256"]}'))
    (CONFIG / 'server.env').write_text('\n'.join(env) + '\n'); (CONFIG / 'server.env').chmod(0o600)
    (CONFIG / 'proxy-headers.conf').write_text((target / 'linux_exchange/deploy/proxy-headers.conf').read_text())
    (CONFIG / 'proxy-headers.conf').chmod(0o644)
    for path in (Path('/var/log/xiaojun-exchange-nginx'), Path('/var/lib/xiaojun-exchange-acme'), Path('/var/log/xiaojun-exchange-acme'),
                 Path('/var/lib/xiaojun-exchange-acme-webroot/.well-known/acme-challenge')):
        path.mkdir(parents=True, exist_ok=True)
    (ROOT / 'current').symlink_to(target)
    deploy = target / 'linux_exchange/deploy'
    marker.write_text(json.dumps(dict(version=version, host=args.host, port=args.port,
                                     backend_port=args.backend_port, phase='services')))
    for pattern in ('*.service', '*.timer'):
        for unit in deploy.glob(pattern):
            content = unit.read_text().replace('--port 18880', f'--port {args.backend_port}')
            (UNITS / unit.name).write_text(content)
    hook = CONFIG / 'reload-nginx.sh'
    hook.write_text('#!/bin/sh\nset -eu\n/usr/sbin/nginx -t -c /etc/xiaojun-exchange/nginx.conf\n/bin/systemctl reload xiaojun-exchange-nginx.service\n')
    hook.chmod(0o700)
    Path('/etc/logrotate.d/xiaojun-exchange').write_text('''/var/log/xiaojun-exchange-nginx/error.log /var/log/xiaojun-exchange-acme/*.log {
    daily
    rotate 2
    maxage 2
    dateext
    dateyesterday
    compress
    missingok
    notifempty
    copytruncate
    su root root
}
''')
    run('systemctl', 'daemon-reload')
    configuration = CONFIG / 'nginx.conf'
    if custom:
        tls = CONFIG / 'tls'; tls.mkdir(mode=0o700)
        cert, key = tls / 'fullchain.pem', tls / 'privkey.pem'
        shutil.copyfile(args.tls_cert, cert); shutil.copyfile(args.tls_key, key)
        cert.chmod(0o600); key.chmod(0o600)
    else:
        configuration.write_text(nginx_config(args.host, args.port, args.backend_port))
        run('systemctl', 'start', NGINX_UNIT)
        run('python3', '-m', 'venv', str(ROOT / 'certbot-venv'))
        run(str(ROOT / 'certbot-venv/bin/python'), '-m', 'pip', 'install', '--no-cache-dir', '--require-hashes', '-r', str(target / 'requirements-certbot.lock'))
        certbot = str(ROOT / 'certbot-venv/bin/certbot')
        command = [certbot, 'certonly', '--webroot', '-w', '/var/lib/xiaojun-exchange-acme-webroot', '--non-interactive', '--agree-tos',
                   '--register-unsafely-without-email', '--cert-name', 'exchange-ip', '--config-dir', str(CONFIG / 'letsencrypt'),
                   '--work-dir', '/var/lib/xiaojun-exchange-acme', '--logs-dir', '/var/log/xiaojun-exchange-acme', '--deploy-hook', str(hook)]
        try:
            ipaddress.IPv4Address(args.host)
            command.extend(('--ip-address', args.host, '--preferred-profile', 'shortlived'))
        except ipaddress.AddressValueError:
            command.extend(('-d', args.host))
        run(*command)
        cert, key = CONFIG / 'letsencrypt/live/exchange-ip/fullchain.pem', CONFIG / 'letsencrypt/live/exchange-ip/privkey.pem'
    configuration.write_text(nginx_config(args.host, args.port, args.backend_port, cert, key, not custom))
    configuration.chmod(0o600)
    run('systemctl', 'enable', '--now', 'xiaojun-exchange-worker.service', 'xiaojun-exchange-web.service', *TIMERS)
    run('systemctl', 'enable', NGINX_UNIT)
    run('systemctl', 'restart', NGINX_UNIT)
    if not custom:
        run('systemctl', 'enable', '--now', 'xiaojun-exchange-cert-renew.timer')
    for _ in range(20):
        try:
            request = urllib.request.Request(f'http://127.0.0.1:{args.backend_port}/api/health', headers={'Host': f'{args.host}:{args.port}'})
            with urllib.request.urlopen(request, timeout=2) as response:
                if json.load(response).get('configured') is False:
                    break
        except (OSError, ValueError):
            time.sleep(1)
    else:
        raise ValueError('执行服务检查未通过，请检查本项目服务日志')
    marker.unlink(missing_ok=True)
    print(f'安装成功：{version}\n网页：https://{args.host}:{args.port}/')
    if not getattr(args, 'defer_password_setup', False):
        print(f'读取首次初始化码：sudo cat {token}\n在网页录入初始化码并设置程序密码。关闭网页不停止服务器任务。')
    if custom:
        print('使用自备证书：请由原证书管理工具负责续期。')


def main(argv=None):
    parser = argparse.ArgumentParser(description='小军 BN Linux 交易所首次安装（不会覆盖已有安装）')
    parser.add_argument('--host', required=True, help='公网 IPv4 或域名')
    parser.add_argument('--port', type=int, default=26483)
    parser.add_argument('--backend-port', type=int, default=18880)
    parser.add_argument('--source', default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument('--tls-cert'); parser.add_argument('--tls-key')
    parser.add_argument('--check', action='store_true', help='只检查，不安装或更改服务')
    parser.add_argument('--retry-incomplete', action='store_true', help='重试尚未创建服务的首次安装；拒绝清理运行数据')
    parser.add_argument('--defer-password-setup', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.retry_incomplete:
            if args.check:
                raise ValueError('--check 不能与 --retry-incomplete 同用')
            retry_incomplete(args)
        preflight(args)
        version, manifest = verify_payload(args.source)
        if args.check:
            print(f'安装预检通过：{version}；面板 TCP {args.port}，内部 TCP {args.backend_port}' + ('，证书验证 TCP 80' if not args.tls_cert else ''))
            return 0
        install(args, version, manifest)
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f'安装失败：{exc}\n保留配置和数据。若尚未创建服务，可用相同参数加 --retry-incomplete 重试；其他情况请检查服务日志，勿覆盖交易数据。', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
