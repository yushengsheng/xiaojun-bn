#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

repo="yushengsheng/xiaojun-bn"
version=""
args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            [[ $# -ge 2 ]] || { printf '%s\n' '--version 缺少 vX.Y.Z 版本号' >&2; exit 1; }
            version="$2"; shift 2 ;;
        --help|-h)
            printf '%s\n' '用法: sudo bash install-linux.sh [--host 公网IPv4或域名] [--check] [--version vX.Y.Z]' \
                '自动识别：未安装则新装；已有新版本且交易空闲则升级；同版本跳过，拒绝降级。' \
                '首次安装可输入地址；可选 --port 26483 --backend-port 18880。' \
                '自备证书：--tls-cert 完整证书 --tls-key 私钥；默认自动证书需开放 TCP 80。' \
                '支持 Ubuntu 24.04、Debian 12/13 x86_64 和 systemd；升级保留原地址、证书和数据。'
            exit 0 ;;
        *) args+=("$1"); shift ;;
    esac
done
if [[ "$EUID" -ne 0 ]]; then
    printf '%s\n' '请使用 sudo bash install-linux.sh' >&2; exit 1
fi
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 || ! -d /run/systemd/system ]]; then
    printf '%s\n' '需要 Linux x86_64 和正在运行的 systemd' >&2; exit 1
fi
source /etc/os-release
case "${ID:-}:${VERSION_ID:-}" in
    ubuntu:24.04|debian:12|debian:13) ;;
    *) printf '%s\n' '仅支持 Ubuntu 24.04、Debian 12/13' >&2; exit 1 ;;
esac
exec 9>/run/xiaojun-exchange-deploy.lock
flock -n 9 || { printf '%s\n' '另一个安装/升级正在进行，请稍后重试' >&2; exit 1; }
missing=()
command -v python3 >/dev/null || missing+=(python3)
command -v curl >/dev/null || missing+=(curl)
[[ -f /etc/ssl/certs/ca-certificates.crt ]] || missing+=(ca-certificates)
if [[ ${#missing[@]} -gt 0 ]]; then
    if [[ " ${args[*]} " == *" --check "* ]]; then
        printf '%s\n' '预检需要先安装 python3、curl 和 ca-certificates' >&2; exit 1
    fi
    timeout 300 apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
    DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l timeout 600 apt-get install -y --no-install-recommends "${missing[@]}"
fi
python3 -c 'import sys; assert sys.version_info >= (3,11), "需要 Python 3.11+"'
stage="$(mktemp -d -t xiaojun-install.XXXXXXXX)"
trap 'rm -rf -- "$stage"' EXIT
download() {
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 --fail --location \
        --connect-timeout 15 --max-time 600 --retry 3 --retry-max-time 900 \
        --max-filesize 268435456 --output "$1" "$2"
}
if [[ -z "$version" ]]; then
    download "$stage/latest.json" "https://api.github.com/repos/$repo/releases/latest"
    version="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tag_name"])' "$stage/latest.json")"
fi
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s\n' '无法确认正式发行版本号，停止部署' >&2; exit 1
fi
asset="xiaojun-bn-$version-linux-amd64.tar.gz"
url="https://github.com/$repo/releases/download/$version"
printf '检查正式发行版：%s\n' "$version"
download "$stage/$asset" "$url/$asset"
download "$stage/SHA256SUMS" "$url/SHA256SUMS"
python3 - "$stage" "$asset" <<'PY'
from pathlib import Path
import hashlib,shutil,sys,tarfile
stage=Path(sys.argv[1]);asset=sys.argv[2]
rows=[line.split() for line in (stage/'SHA256SUMS').read_text().splitlines()]
matches=[row[0] for row in rows if len(row)==2 and row[1]==asset]
with (stage/asset).open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
if len(matches)!=1 or digest!=matches[0]:raise SystemExit('发行包 SHA-256 校验失败，停止部署')
with tarfile.open(stage/asset) as archive:
    entries=archive.getmembers()
    if len(entries)>4096 or sum(e.size for e in entries)>768*1024*1024:
        raise SystemExit('发行包超出资源上限')
    names=set()
    for entry in entries:
        path=Path(entry.name)
        if not entry.isfile() or path.is_absolute() or '..' in path.parts or entry.mode & 0o7000 or path.as_posix() in names:
            raise SystemExit('发行包包含不安全路径或重复文件')
        names.add(path.as_posix())
        destination=stage/'source'/path
        destination.parent.mkdir(parents=True,exist_ok=True)
        with archive.extractfile(entry) as source, destination.open('xb') as target:
            shutil.copyfileobj(source,target,1024*1024)
        destination.chmod(0o755 if entry.name.startswith('bin/') else 0o644)
PY
if [[ ! -f "$stage/source/linux_exchange/deploy.py" ]]; then
    printf '%s\n' '此版本尚不支持自动升级；请选择包含 linux_exchange/deploy.py 的新版正式发行包。' >&2
    exit 1
fi
python3 "$stage/source/linux_exchange/deploy.py" --source "$stage/source" "${args[@]}"
