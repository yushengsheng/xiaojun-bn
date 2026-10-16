"""Install or upgrade verified releases, without interrupting active trading."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import tempfile
import urllib.request
import urllib.parse

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from linux_exchange import install
from linux_exchange.ipc import call

WORKER = 'xiaojun-exchange-worker.service'
WEB = 'xiaojun-exchange-web.service'
MAINTENANCE = 'xiaojun-exchange-maintenance.timer'


def systemctl(*args):
    return subprocess.run(['systemctl', *args], check=True, capture_output=True, text=True, timeout=330).stdout.strip()


def active(unit):
    result = subprocess.run(['systemctl', 'is-active', unit], capture_output=True, text=True, timeout=15)
    return result.stdout.strip() == 'active'


def health():
    result = call('health', socket_path='/run/xiaojun-exchange/worker.sock')
    if not isinstance(result, dict) or type(result.get('busy')) is not bool:
        raise ValueError('无法可靠确认交易服务是否空闲，拒绝升级')
    return result


def require_idle():
    if health()['busy']:
        raise ValueError('交易或清理任务仍在运行，本次不升级；请等待任务空闲后重新运行同一命令')


def installed_release():
    root = install.ROOT
    if (root / '.install-incomplete.json').exists() or (root / '.upgrade-incomplete.json').exists():
        raise ValueError('检测到未完成的安装/升级，请保留现场及备份并检查服务；拒绝覆盖')
    current = root / 'current'
    if root.is_symlink() or not current.is_symlink():
        raise ValueError('已有目录不属于可识别的安装，拒绝覆盖')
    target = current.resolve(strict=True)
    if target.parent != (root / 'releases').resolve() or target.is_symlink():
        raise ValueError('现有源码路径异常，拒绝覆盖')
    version, manifest = install.verify_payload(target)
    if any(path.is_symlink() for path in (install.CONFIG, install.DATA, root / 'releases')):
        raise ValueError('配置、数据或版本目录路径异常，拒绝覆盖')
    for path in (install.CONFIG, install.DATA, root / 'venv', root / 'proxy-bin'):
        if not path.is_dir() or (path.is_symlink() and not path.resolve().is_relative_to(root.resolve())):
            raise ValueError(f'现有安装路径异常：{path}')
    return target, version, manifest


def version_tuple(version):
    if not re.fullmatch(r'v\d+\.\d+\.\d+', version):
        raise ValueError('版本号必须为 vX.Y.Z')
    return tuple(map(int, version[1:].split('.')))


def backend_port():
    text = (install.UNITS / WEB).read_text(encoding='utf-8')
    match = re.search(r'--port (\d+)\b', text)
    if not match or not 1024 <= int(match[1]) <= 65535:
        raise ValueError('无法识别现有内部端口，拒绝改写服务')
    return int(match[1])


def planned_units(old, source, port):
    result = {}
    for candidate in sorted((source / 'linux_exchange/deploy').glob('*')):
        if candidate.suffix not in {'.service', '.timer'}:
            continue
        target = install.UNITS / candidate.name
        before = old / 'linux_exchange/deploy' / candidate.name
        if not target.is_file() or target.is_symlink() or not before.is_file():
            raise ValueError(f'服务配置缺失或布局变化，需人工维护：{target.name}')
        expected = before.read_text(encoding='utf-8').replace('--port 18880', f'--port {port}')
        if target.read_text(encoding='utf-8') != expected:
            raise ValueError(f'服务配置已由管理员修改，拒绝覆盖：{target.name}')
        if systemctl('show', candidate.name, '--property=DropInPaths', '--value'):
            raise ValueError(f'服务存在自定义覆盖配置，拒绝自动升级：{target.name}')
        result[target] = candidate.read_text(encoding='utf-8').replace('--port 18880', f'--port {port}').encode()
    return result


def staged_runtime(source, target, old):
    """Prepare immutable code and venvs before stopping any existing service."""
    if target.exists() or target.is_symlink():
        raise ValueError(f'目标版本目录已存在，请保留现场检查后处理：{target}')
    _, manifest = install.verify_payload(source)
    target.mkdir(parents=True)
    target.chmod(0o755)
    for name in manifest:
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, destination)
        destination.chmod(0o755 if name.startswith('bin/') or name.endswith('.sh') else 0o644)
    shutil.copyfile(source / 'linux-release-manifest.json', target / 'linux-release-manifest.json')
    install.verify_payload(target)
    for path in (target / 'bin').iterdir():
        path.chmod(0o755)
    environment = target / '.venv'
    install.run('python3', '-m', 'venv', str(environment), timeout=120)
    python = str(environment / 'bin/python')
    pip_env = {**os.environ, 'PIP_DEFAULT_TIMEOUT': '60', 'PIP_RETRIES': '3', 'PIP_DISABLE_PIP_VERSION_CHECK': '1'}
    install.run(python, '-m', 'pip', 'install', '--no-cache-dir', '--require-hashes', '-r',
                str(target / 'requirements-linux.lock'), env=pip_env, timeout=900)
    install.run(python, '-m', 'pip', 'check', timeout=60)
    install.run(python, '-c', 'import linux_exchange.worker, linux_exchange.web, exchange_strategy',
                cwd=target, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}, timeout=60)
    pins = json.loads((target / 'linux_exchange/deploy/proxy-runtimes.json').read_text())
    if set(pins) != {'xray', 'sing-box', 'hysteria'}:
        raise ValueError('代理运行时清单不完整')
    for name, pin in pins.items():
        binary = target / 'bin' / name
        if install.digest(binary) != pin['executable_sha256']:
            raise ValueError(f'{name} 运行时校验失败')
        install.run(str(binary), 'version', stdout=subprocess.DEVNULL, timeout=20)
    certbot = None
    if (install.ROOT / 'certbot-venv').exists():
        if (old / 'requirements-certbot.lock').read_bytes() != (target / 'requirements-certbot.lock').read_bytes():
            certbot = target / '.certbot-venv'
            install.run('python3', '-m', 'venv', str(certbot), timeout=120)
            install.run(str(certbot / 'bin/python'), '-m', 'pip', 'install', '--no-cache-dir', '--require-hashes',
                        '-r', str(target / 'requirements-certbot.lock'), env=pip_env, timeout=900)
            install.run(str(certbot / 'bin/python'), '-m', 'pip', 'check', timeout=60)
    return environment, certbot, pins


def replace_link(path, target):
    temporary = path.with_name(path.name + '.next')
    if temporary.exists() or temporary.is_symlink():
        raise ValueError(f'检测到未完成切换：{temporary}')
    temporary.symlink_to(target, target_is_directory=True)
    os.replace(temporary, path)
    sync_directory(path.parent)


def sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def replace_file(path, content):
    mode = path.stat().st_mode & 0o777
    descriptor, temporary = tempfile.mkstemp(prefix=f'.{path.name}.', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def wait_healthy(port, origin):
    for _ in range(30):
        try:
            state = health()
            if state['busy'] or state.get('unlocked') is not False:
                raise ValueError('新执行服务必须保持锁定且空闲')
            request = urllib.request.Request(f'http://127.0.0.1:{port}/api/health', headers={'Host': origin.split('://', 1)[1]})
            with urllib.request.urlopen(request, timeout=2) as response:
                if json.load(response).get('configured') == state.get('configured'):
                    return
        except (OSError, ValueError, RuntimeError):
            time.sleep(1)
    raise ValueError('升级后的执行/网页服务检查失败')


@contextmanager
def catch_termination():
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt('部署被取消')
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def upgrade(source, version, args):
    version_tuple(version)
    target = install.ROOT / 'releases' / version
    existed = target.exists() or target.is_symlink()
    try:
        return _upgrade(source, version, args)
    except BaseException:
        # Remove only a new, inactive staging release after a clean failure.
        # An interrupted transaction keeps every artifact for manual recovery.
        if (not existed and not (install.ROOT / '.upgrade-incomplete.json').exists()
                and target.is_dir() and not target.is_symlink()
                and (install.ROOT / 'current').resolve() != target.resolve()):
            shutil.rmtree(target)
        raise


def _upgrade(source, version, args):
    old, old_version, old_manifest = installed_release()
    if version_tuple(version) < version_tuple(old_version):
        raise ValueError(f'拒绝自动降级：{old_version} → {version}')
    if version == old_version:
        if install.verify_payload(source)[1] != old_manifest:
            raise ValueError('同版本文件清单不同，拒绝替换已安装版本')
        print(f'已是最新版 {version}，无需升级，现有服务保持运行。')
        return
    if not active(WORKER) or not active(WEB):
        raise ValueError('现有执行/网页服务不正常，无法安全判断状态；请先检查服务')
    require_idle()
    port = backend_port()
    files = planned_units(old, source, port)
    env_path = install.CONFIG / 'server.env'
    if env_path.is_symlink():
        raise ValueError('环境配置路径异常')
    original_env = env_path.read_text(encoding='utf-8')
    old_pins = json.loads((old / 'linux_exchange/deploy/proxy-runtimes.json').read_text())
    for name, prefix in (('xray', 'XRAY'), ('sing-box', 'SING_BOX'), ('hysteria', 'HYSTERIA')):
        for key, expected in ((f'{prefix}_PATH', str(install.ROOT / 'proxy-bin' / name)),
                              (f'{prefix}_EXECUTABLE_SHA256', old_pins[name]['executable_sha256'])):
            if re.findall(rf'^{key}=(.*)$', original_env, re.M) != [expected]:
                raise ValueError(f'现有代理环境配置被修改，拒绝覆盖：{key}')
    origins = re.findall(r'^XIAOJUN_PUBLIC_ORIGIN=(https://[^\s]+)$', original_env, re.M)
    if len(origins) != 1:
        raise ValueError('无法识别已安装的 HTTPS 地址')
    address = urllib.parse.urlsplit(origins[0])
    if ((args.host and args.host.lower() != address.hostname) or
            (args.port is not None and args.port != address.port) or
            (args.backend_port is not None and args.backend_port != port) or args.tls_cert or args.tls_key):
        raise ValueError('升级沿用原地址、端口和证书；拒绝更改这些首次安装参数')
    if shutil.disk_usage(install.ROOT).free < 1024**3:
        raise ValueError('升级至少需要 1 GiB 可用空间，旧版本和数据不会自动删除')
    if args.check:
        print(f'升级预检通过：{old_version} → {version}；沿用 {origins[0]}；正式切换前再次确认空闲。')
        return
    target = install.ROOT / 'releases' / version
    if target.exists() or target.is_symlink():
        raise ValueError(f'目标版本目录已存在，请保留现场检查后处理：{target}')
    environment, certbot, pins = staged_runtime(source, target, old)
    replacements = {install.ROOT / 'current': target, install.ROOT / 'venv': environment,
                    install.ROOT / 'proxy-bin': target / 'bin'}
    if certbot:
        replacements[install.ROOT / 'certbot-venv'] = certbot
    for name, prefix in (('xray', 'XRAY'), ('sing-box', 'SING_BOX'), ('hysteria', 'HYSTERIA')):
        for key, value in ((f'{prefix}_PATH', str(install.ROOT / 'proxy-bin' / name)),
                           (f'{prefix}_EXECUTABLE_SHA256', pins[name]['executable_sha256'])):
            if len(re.findall(rf'^{key}=.*$', original_env, re.M)) != 1:
                raise ValueError(f'现有环境配置缺少唯一的 {key}')
            original_env = re.sub(rf'^{key}=.*$', f'{key}={value}', original_env, flags=re.M)
    files[env_path] = original_env.encode()
    backup = install.ROOT / 'upgrade-backups' / f'{old_version}-to-{version}-{time.time_ns()}'
    backup.mkdir(parents=True, mode=0o700)
    saved = {}
    for index, path in enumerate(files):
        copy = backup / f'file-{index}'
        shutil.copy2(path, copy)
        with copy.open('rb') as stream:
            os.fsync(stream.fileno())
        saved[path] = copy
    sync_directory(backup)
    sync_directory(backup.parent)
    links = {path: dict(link=os.readlink(path)) if path.is_symlink() else dict(directory=str(backup / path.name))
             for path in replacements}
    marker = install.ROOT / '.upgrade-incomplete.json'
    paused = [unit for unit in (MAINTENANCE, 'xiaojun-exchange-cert-renew.timer') if active(unit)]
    with marker.open('x', encoding='utf-8') as stream:
        marker.chmod(0o600)
        json.dump(dict(old=str(old), new=str(target), backup=str(backup),
                       files={str(k): str(v) for k, v in saved.items()},
                       paths={str(k): v for k, v in links.items()}, paused=paused), stream)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(marker.parent)
    changed = []
    worker_stopped = False
    try:
        systemctl('stop', WEB)
        # The web process is gone before this second check: no new browser command
        # can race the idle check. Busy workers are never sent a stop signal.
        require_idle()
        for unit in paused:
            systemctl('stop', unit)
        for unit in ('xiaojun-exchange-maintenance.service', 'xiaojun-exchange-cert-renew.service'):
            state = systemctl('show', unit, '--property=ActiveState', '--value')
            if state in {'activating', 'active', 'deactivating'}:
                raise ValueError('后台维护/证书任务正在运行，请稍后重试')
        worker_stopped = True
        systemctl('stop', WORKER)
        for path, destination in replacements.items():
            if 'directory' in links[path]:
                path.rename(links[path]['directory'])
                sync_directory(path.parent)
                sync_directory(backup)
                changed.append(path)
                replace_link(path, destination)
            else:
                changed.append(path)
                replace_link(path, destination)
        for path, content in files.items():
            replace_file(path, content)
        install.install_restart_guard()
        systemctl('daemon-reload')
        systemctl('start', WORKER, WEB)
        wait_healthy(port, origins[0])
        for unit in paused:
            systemctl('start', unit)
        marker.unlink()
        sync_directory(marker.parent)
    except BaseException as exc:
        try:
            if worker_stopped:
                systemctl('stop', WEB, WORKER)
                for path in reversed(changed):
                    state = links[path]
                    if 'link' in state:
                        replace_link(path, state['link'])
                    else:
                        if path.is_symlink():
                            path.unlink()
                        Path(state['directory']).rename(path)
                for path, copy in saved.items():
                    replace_file(path, copy.read_bytes())
                systemctl('daemon-reload')
                systemctl('start', WORKER, WEB)
                wait_healthy(port, origins[0])
            else:
                systemctl('start', WEB)
            for unit in paused:
                systemctl('start', unit)
            marker.unlink()
            sync_directory(marker.parent)
        except BaseException as rollback_error:
            raise ValueError(f'升级失败且自动回退未完成；保留 {marker} 和 {backup}，请人工检查服务') from rollback_error
        raise ValueError(f'升级未完成，已恢复原版本 {old_version}；配置和数据保留。原因：{exc}') from exc
    print(f'升级成功：{old_version} → {version}\n网页：{origins[0]}/\n请重新解锁；不会自动启动交易。旧版本及部署备份保留在 {backup}。')


def main(argv=None):
    parser = argparse.ArgumentParser(description='自动识别新装/升级；忙碌时拒绝升级，保留现有配置与数据')
    parser.add_argument('--source', default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument('--host')
    parser.add_argument('--port', type=int)
    parser.add_argument('--backend-port', type=int)
    parser.add_argument('--tls-cert'); parser.add_argument('--tls-key')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--retry-incomplete', action='store_true')
    args = parser.parse_args(argv)
    try:
        if install.platform.system() != 'Linux' or install.platform.machine() not in ('x86_64', 'amd64') or os.geteuid() != 0:
            raise ValueError('须在 Linux x86_64 使用 sudo 运行')
        version, _ = install.verify_payload(args.source)
        existing = any(p.exists() or p.is_symlink() for p in (install.ROOT, install.CONFIG, install.DATA)) or any(install.UNITS.glob('xiaojun-exchange-*'))
        if existing and not args.retry_incomplete:
            with catch_termination():
                upgrade(Path(args.source).resolve(), version, args)
            return 0
        if not args.host:
            if not sys.stdin.isatty():
                raise ValueError('首次安装请加 --host 公网IPv4或域名，或在交互终端运行后输入地址')
            args.host = input('首次安装，请输入服务器公网 IPv4 或完整域名：').strip()
        forwarded = ['--source', args.source, '--host', args.host, '--port', str(args.port if args.port is not None else 26483),
                     '--backend-port', str(args.backend_port if args.backend_port is not None else 18880)]
        for name in ('tls_cert', 'tls_key'):
            if getattr(args, name):
                forwarded.extend(('--' + name.replace('_', '-'), getattr(args, name)))
        for name in ('check', 'retry_incomplete'):
            if getattr(args, name):
                forwarded.append('--' + name.replace('_', '-'))
        return install.main(forwarded)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as exc:
        print(f'部署未完成：{exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
