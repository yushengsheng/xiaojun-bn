#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:
    AESGCM = None
try:
    from Crypto.Cipher import AES as CRYPTO_AES
except Exception:
    CRYPTO_AES = None

from app_paths import APP_LOCK_BACKUP_FILE, APP_LOCK_FILE
from secret_box import SECRET_BOX, SecretBox


APP_LOCK_BYPASS_ENV = "XIAOJUN_APP_LOCK_BYPASS"
APP_LOCK_PASSWORD_ENV = "XIAOJUN_APP_PASSWORD"
APP_LOCK_MIN_PASSWORD_LENGTH = 8


def _b64_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64_decode(text: str) -> bytes:
    s = str(text or "").strip()
    if not s:
        raise RuntimeError("密文为空")
    s += "=" * (-len(s) % 4)
    try:
        return base64.urlsafe_b64decode(s.encode("ascii"))
    except Exception as exc:
        raise RuntimeError("Base64 解码失败") from exc


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        try:
            os.chmod(tmp_name, 0o600)
        except Exception:
            pass
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_name, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    _atomic_write_json_file(path, payload)
    backup_path = APP_LOCK_BACKUP_FILE if path == APP_LOCK_FILE else path.with_name(f"{path.name}.bak")
    try:
        _atomic_write_json_file(backup_path, payload)
    except Exception:
        pass


class AppLockManager:
    VERSION = 1
    KDF_NAME = "scrypt"
    DEFAULT_N = 2**15
    DEFAULT_R = 8
    DEFAULT_P = 1
    DEFAULT_DKLEN = 32
    DEFAULT_MAXMEM = 64 * 1024 * 1024
    AAD = b"xiaojun-bn-app-lock-v1"

    def __init__(
        self,
        lock_file: Path | None = None,
        secret_box: SecretBox | None = None,
        *,
        scrypt_n: int | None = None,
        scrypt_r: int | None = None,
        scrypt_p: int | None = None,
    ):
        self.lock_file = Path(lock_file or APP_LOCK_FILE)
        self.secret_box = secret_box or SECRET_BOX
        self.scrypt_n = int(scrypt_n or self.DEFAULT_N)
        self.scrypt_r = int(scrypt_r or self.DEFAULT_R)
        self.scrypt_p = int(scrypt_p or self.DEFAULT_P)

    @staticmethod
    def bypass_enabled() -> bool:
        return str(os.environ.get(APP_LOCK_BYPASS_ENV, "") or "").strip() == "1"

    @staticmethod
    def env_password() -> str:
        return str(os.environ.get(APP_LOCK_PASSWORD_ENV, "") or "")

    def is_configured(self) -> bool:
        return self.lock_file.exists() or self._backup_path().exists()

    def _backup_path(self) -> Path:
        return APP_LOCK_BACKUP_FILE if self.lock_file == APP_LOCK_FILE else self.lock_file.with_name(f"{self.lock_file.name}.bak")

    def is_unlocked(self) -> bool:
        return self.secret_box.has_cached_key()

    @staticmethod
    def validate_new_password(password: str) -> None:
        text = str(password or "")
        if len(text) < APP_LOCK_MIN_PASSWORD_LENGTH:
            raise RuntimeError(f"程序密码至少需要 {APP_LOCK_MIN_PASSWORD_LENGTH} 位")
        if not text.strip():
            raise RuntimeError("程序密码不能为空")

    def _derive_key(self, password: str, *, salt: bytes, n: int, r: int, p: int, dklen: int) -> bytes:
        raw_password = str(password or "").encode("utf-8")
        if not raw_password:
            raise RuntimeError("程序密码不能为空")
        try:
            return hashlib.scrypt(
                raw_password,
                salt=salt,
                n=int(n),
                r=int(r),
                p=int(p),
                dklen=int(dklen),
                maxmem=self.DEFAULT_MAXMEM,
            )
        except ValueError as exc:
            if "memory limit exceeded" in str(exc).lower():
                raise RuntimeError("当前运行环境无法分配程序密码所需内存") from exc
            raise RuntimeError("程序密码参数无效") from exc

    @staticmethod
    def _encrypt_with_key(key: bytes, plain: bytes) -> tuple[bytes, bytes]:
        nonce = os.urandom(12)
        if AESGCM is not None:
            return nonce, AESGCM(key).encrypt(nonce, plain, AppLockManager.AAD)
        if CRYPTO_AES is None:
            raise RuntimeError('当前环境缺少 "cryptography" 或 "pycryptodome"，无法启用程序密码')
        aes = CRYPTO_AES.new(key, CRYPTO_AES.MODE_GCM, nonce=nonce)
        aes.update(AppLockManager.AAD)
        encrypted, tag = aes.encrypt_and_digest(plain)
        return nonce, encrypted + tag

    @staticmethod
    def _decrypt_with_key(key: bytes, nonce: bytes, cipher: bytes) -> bytes:
        try:
            if AESGCM is not None:
                return AESGCM(key).decrypt(nonce, cipher, AppLockManager.AAD)
            if CRYPTO_AES is None:
                raise RuntimeError('当前环境缺少 "cryptography" 或 "pycryptodome"，无法解锁程序密码')
            if len(cipher) < 16:
                raise RuntimeError("程序密码密文格式错误")
            encrypted = cipher[:-16]
            tag = cipher[-16:]
            aes = CRYPTO_AES.new(key, CRYPTO_AES.MODE_GCM, nonce=nonce)
            aes.update(AppLockManager.AAD)
            return aes.decrypt_and_verify(encrypted, tag)
        except Exception as exc:
            raise RuntimeError("程序密码错误，或密码文件已损坏") from exc

    def _load_config(self) -> dict[str, object]:
        candidates = (self.lock_file, self._backup_path())
        if not any(path.exists() for path in candidates):
            raise RuntimeError("尚未设置程序密码")
        errors: list[str] = []
        for path in candidates:
            if not path.exists():
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raise RuntimeError("程序密码文件格式无效")
                if int(raw.get("version", 0) or 0) != self.VERSION:
                    raise RuntimeError("程序密码文件版本不支持")
                if str(raw.get("kdf", "") or "") != self.KDF_NAME:
                    raise RuntimeError("程序密码 KDF 不支持")
                if path != self.lock_file:
                    _atomic_write_json(self.lock_file, raw)
                return raw
            except Exception as exc:
                errors.append(f"{path.name}: {exc}")
        raise RuntimeError("程序密码文件读取失败：" + "；".join(errors))

    def _build_config(self, password: str, raw_secret_key: bytes) -> dict[str, object]:
        self.validate_new_password(password)
        raw_key = bytes(raw_secret_key or b"")
        if len(raw_key) != 32:
            raise RuntimeError("本地主密钥长度无效")
        salt = os.urandom(16)
        derived = self._derive_key(
            password,
            salt=salt,
            n=self.scrypt_n,
            r=self.scrypt_r,
            p=self.scrypt_p,
            dklen=self.DEFAULT_DKLEN,
        )
        nonce, cipher = self._encrypt_with_key(derived, raw_key)
        return {
            "version": self.VERSION,
            "kdf": self.KDF_NAME,
            "n": self.scrypt_n,
            "r": self.scrypt_r,
            "p": self.scrypt_p,
            "dklen": self.DEFAULT_DKLEN,
            "salt": _b64_encode(salt),
            "nonce": _b64_encode(nonce),
            "wrapped_key": _b64_encode(cipher),
            "created_at": _utc_text(),
            "updated_at": _utc_text(),
        }

    def setup_password(self, password: str) -> None:
        if self.is_configured():
            raise RuntimeError("程序密码已设置，请使用修改密码")
        raw_secret_key = self.secret_box.export_raw_key()
        payload = self._build_config(password, raw_secret_key)
        _atomic_write_json(self.lock_file, payload)
        self.secret_box.set_unlocked_key(raw_secret_key)
        self.secret_box.write_locked_marker()

    def unlock(self, password: str) -> None:
        raw = self._load_config()
        derived = self._derive_key(
            password,
            salt=_b64_decode(str(raw.get("salt", "") or "")),
            n=int(raw.get("n", self.DEFAULT_N) or self.DEFAULT_N),
            r=int(raw.get("r", self.DEFAULT_R) or self.DEFAULT_R),
            p=int(raw.get("p", self.DEFAULT_P) or self.DEFAULT_P),
            dklen=int(raw.get("dklen", self.DEFAULT_DKLEN) or self.DEFAULT_DKLEN),
        )
        raw_secret_key = self._decrypt_with_key(
            derived,
            _b64_decode(str(raw.get("nonce", "") or "")),
            _b64_decode(str(raw.get("wrapped_key", "") or "")),
        )
        self.secret_box.set_unlocked_key(raw_secret_key)
        self.secret_box.write_locked_marker()

    def change_password(self, old_password: str, new_password: str) -> None:
        if not self.is_configured():
            raise RuntimeError("尚未设置程序密码")
        self.unlock(old_password)
        raw_secret_key = self.secret_box.export_raw_key()
        payload = self._build_config(new_password, raw_secret_key)
        _atomic_write_json(self.lock_file, payload)
        self.secret_box.set_unlocked_key(raw_secret_key)
        self.secret_box.write_locked_marker()

    def lock(self) -> None:
        self.secret_box.clear_cached_key()


APP_LOCK = AppLockManager()
