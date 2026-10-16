"""Build a clean, hash-verified Linux release; never recurse into local data."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def source_files(root):
    root = Path(root)
    files = list(root.glob('*.py'))
    files += [root / name for name in (
        'README.md', 'install-linux.sh', 'requirements-linux.txt', 'requirements-linux.lock',
        'requirements-certbot.txt', 'requirements-certbot.lock')]
    suffixes = {'.py', '.md', '.conf', '.service', '.timer', '.html', '.css', '.js'}
    files += [p for p in (root / 'linux_exchange').rglob('*') if p.suffix in suffixes]
    files.append(root / 'linux_exchange/deploy/proxy-runtimes.json')
    result = {}
    for path in sorted(set(files)):
        relative = path.relative_to(root)
        if any(part.startswith('.') or part in {'data', 'logs', '__pycache__', 'bin'} for part in relative.parts):
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'Unsafe or missing source: {relative}')
        result[relative.as_posix()] = path.read_bytes()
    return result


def proxy_binary(name, pin, runtime_dir=None):
    if runtime_dir:
        binary = (Path(runtime_dir) / name).read_bytes()
    else:
        request = urllib.request.Request(pin['url'], headers={'User-Agent': 'xiaojun-bn-release'})
        with urllib.request.urlopen(request, timeout=120) as response:
            archive = response.read()
        if sha256(archive) != pin['archive_sha256']:
            raise ValueError(f'{name} archive digest mismatch')
        if pin['url'].endswith('.tar.gz'):
            with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
                entries = [m for m in bundle.getmembers() if m.isfile() and Path(m.name).name == name]
                if len(entries) != 1:
                    raise ValueError(f'{name} archive member mismatch')
                binary = bundle.extractfile(entries[0]).read()
        elif pin['url'].endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                entries = [m for m in bundle.namelist() if Path(m).name == name]
                if len(entries) != 1:
                    raise ValueError(f'{name} archive member mismatch')
                binary = bundle.read(entries[0])
        else:
            binary = archive
    if sha256(binary) != pin['executable_sha256']:
        raise ValueError(f'{name} executable digest mismatch')
    return binary


def build(version, output, runtime_dir=None, root=ROOT):
    if not re.fullmatch(r'v\d+\.\d+\.\d+', version):
        raise ValueError('Version must be an exact vX.Y.Z tag')
    payload = source_files(root)
    payload['version.txt'] = version.encode()
    pins = json.loads(payload['linux_exchange/deploy/proxy-runtimes.json'])
    for name, pin in pins.items():
        payload[f'bin/{name}'] = proxy_binary(name, pin, runtime_dir)
    manifest = {name: sha256(data) for name, data in sorted(payload.items())}
    payload['linux-release-manifest.json'] = json.dumps(manifest, sort_keys=True, indent=2).encode() + b'\n'
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    asset = output / f'xiaojun-bn-{version}-linux-amd64.tar.gz'
    with tarfile.open(asset, 'w:gz', format=tarfile.PAX_FORMAT) as archive:
        for name, data in sorted(payload.items()):
            info = tarfile.TarInfo(name); info.size = len(data)
            info.mode = 0o755 if name.startswith('bin/') or name.endswith('.sh') else 0o644
            archive.addfile(info, io.BytesIO(data))
    installer = output / 'install-linux.sh'
    installer.write_bytes(payload['install-linux.sh']); installer.chmod(0o755)
    sums = output / 'SHA256SUMS'
    sums.write_text(''.join(f'{sha256(p.read_bytes())}  {p.name}\n' for p in (asset, installer)))
    print(f'Linux release ready: {version}; {len(manifest)} verified files')
    return asset


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', default='release')
    parser.add_argument('--runtime-dir', help='Optional already verified local binaries for packaging preflight')
    args = parser.parse_args()
    build(args.version, args.output, args.runtime_dir)


if __name__ == '__main__':
    main()
