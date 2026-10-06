#!/usr/bin/env bash
set -Eeuo pipefail

# Release packaging replaces this marker with the exact tested tag.
version="__RELEASE_VERSION__"
repo="yushengsheng/xiaojun-bn"
if [[ "$version" != v[0-9]*.[0-9]*.[0-9]* ]]; then
    printf '%s\n' '请从 GitHub Releases 下载 install-linux.sh，或使用已解压发行包内的安装器。' >&2
    exit 1
fi
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    printf '%s\n' "用法: sudo bash install-linux.sh --host 公网IP [--port 26483] [--backend-port 18880]" \
        "可选: --tls-cert 完整证书 --tls-key 私钥（由原工具续期，不需要80端口）" \
        "支持 Ubuntu 24.04、Debian 12/13 x86_64；自动证书需开放 TCP 80 和面板端口。" \
        "已有安装和端口冲突会拒绝安装；--check 只执行预检。版本: $version"
    exit 0
fi
if [[ "$EUID" -ne 0 ]]; then
    printf '%s\n' '请使用 sudo bash install-linux.sh --host 公网IP' >&2
    exit 1
fi
if [[ -e /opt/xiaojun-exchange || -e /etc/xiaojun-exchange || -e /var/lib/xiaojun-exchange ]] || compgen -G '/etc/systemd/system/xiaojun-exchange-*' >/dev/null; then
    printf '%s\n' '检测到已有安装，拒绝覆盖；请保留现有交易和数据。' >&2
    exit 1
fi
missing=()
command -v python3 >/dev/null || missing+=(python3)
command -v curl >/dev/null || missing+=(curl)
[[ -f /etc/ssl/certs/ca-certificates.crt ]] || missing+=(ca-certificates)
if [[ ${#missing[@]} -gt 0 ]]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l apt-get install -y --no-install-recommends "${missing[@]}"
fi
stage="$(mktemp -d -t xiaojun-install.XXXXXXXX)"
trap 'rm -rf -- "$stage"' EXIT
asset="xiaojun-bn-$version-linux-amd64.tar.gz"
url="https://github.com/$repo/releases/download/$version"
curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --output "$stage/$asset" "$url/$asset"
curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --output "$stage/SHA256SUMS" "$url/SHA256SUMS"
python3 - "$stage" "$asset" <<'PY'
from pathlib import Path
import hashlib,sys,tarfile
stage=Path(sys.argv[1]);asset=sys.argv[2]
rows=[line.split() for line in (stage/'SHA256SUMS').read_text().splitlines()]
matches=[row[0] for row in rows if len(row)==2 and row[1]==asset]
with (stage/asset).open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
if len(matches)!=1 or digest!=matches[0]:raise SystemExit('发行包 SHA-256 校验失败，停止安装')
with tarfile.open(stage/asset) as archive:
    for entry in archive.getmembers():
        path=Path(entry.name)
        if not entry.isfile() or path.is_absolute() or '..' in path.parts or entry.mode & 0o7000:
            raise SystemExit('发行包包含不安全路径')
        destination=stage/'source'/path
        destination.parent.mkdir(parents=True,exist_ok=True)
        with archive.extractfile(entry) as source, destination.open('wb') as target:
            target.write(source.read())
        destination.chmod(0o755 if entry.name.startswith('bin/') else 0o644)
PY
python3 "$stage/source/linux_exchange/install.py" --source "$stage/source" "$@"
