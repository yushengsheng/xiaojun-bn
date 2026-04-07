#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import tempfile
import threading
from pathlib import Path

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:
    AESGCM = None
try:
    from Crypto.Cipher import AES as CRYPTO_AES
except Exception:
    CRYPTO_AES = None

from app_paths import SECRET_KEY_FILE


def _atomic_write_secret_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        try:
            os.chmod(tmp_name, 0o600)
        except Exception:
            pass
        with os.fdopen(fd, "w", encoding=encoding) as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_name, path)
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
        except Exception:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class SecretBox:
    """
    轻量本地密钥封装：敏感字段落盘加密，内存中保持明文。
    支持兼容旧明文（未加密值会直接返回）。
    """

    PREFIX_V1 = "enc::v1::"
    PREFIX_V2 = "enc::v2::"
    ENV_KEY = "WITHDRAW_SECRET_KEY"
    KEY_FILE = SECRET_KEY_FILE

    def __init__(self, key_file: Path | None = None):
        self.key_file = key_file or self.KEY_FILE
        self._key_cache: bytes | None = None
        self._lock = threading.Lock()

    @staticmethod
    def _b64_encode(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def _b64_decode(text: str) -> bytes:
        s = str(text or "").strip()
        if not s:
            raise RuntimeError("密钥/密文为空")
        s += "=" * (-len(s) % 4)
        try:
            return base64.urlsafe_b64decode(s.encode("ascii"))
        except Exception as exc:
            raise RuntimeError("Base64 解码失败") from exc

    @classmethod
    def is_encrypted(cls, text: str) -> bool:
        raw = str(text or "")
        return raw.startswith(cls.PREFIX_V1) or raw.startswith(cls.PREFIX_V2)

    @staticmethod
    def _xor_bytes(a: bytes, b: bytes) -> bytes:
        return bytes(x ^ y for x, y in zip(a, b))

    @staticmethod
    def _build_keystream(key: bytes, nonce: bytes, size: int) -> bytes:
        out = bytearray()
        counter = 0
        while len(out) < size:
            block = hmac.new(key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest()
            out.extend(block)
            counter += 1
        return bytes(out[:size])

    def _decode_key(self, raw: str) -> bytes:
        key = self._b64_decode(raw)
        if len(key) != 32:
            raise RuntimeError("密钥长度无效，必须是 32 字节")
        return key

    def _load_or_create_key(self) -> bytes:
        env_key = os.environ.get(self.ENV_KEY, "").strip()
        if env_key:
            return self._decode_key(env_key)

        if self.key_file.exists():
            return self._decode_key(self.key_file.read_text(encoding="utf-8").strip())

        self.key_file.parent.mkdir(parents=True, exist_ok=True)
        key = os.urandom(32)
        _atomic_write_secret_text(self.key_file, self._b64_encode(key), encoding="utf-8")
        try:
            os.chmod(self.key_file, 0o600)
        except Exception:
            pass
        return key

    def _key(self) -> bytes:
        with self._lock:
            if self._key_cache is None:
                self._key_cache = self._load_or_create_key()
            return self._key_cache

    @staticmethod
    def modern_encryption_available() -> bool:
        return SecretBox.modern_encryption_backend_name() != "missing"

    @staticmethod
    def modern_encryption_backend_name() -> str:
        if AESGCM is not None:
            return "cryptography"
        if CRYPTO_AES is not None:
            return "pycryptodome"
        return "missing"

    @staticmethod
    def _require_modern_cipher():
        backend = SecretBox.modern_encryption_backend_name()
        if backend == "cryptography":
            return backend, AESGCM
        if backend == "pycryptodome":
            return backend, CRYPTO_AES
        raise RuntimeError('检测到新版密文，但当前环境缺少 "cryptography" 或 "pycryptodome" 依赖')

    def _encrypt_v1(self, text: str) -> str:
        key = self._key()
        nonce = os.urandom(16)
        plain = text.encode("utf-8")
        stream = self._build_keystream(key, nonce, len(plain))
        cipher = self._xor_bytes(plain, stream)
        mac = hmac.new(key, b"auth-v1|" + nonce + cipher, hashlib.sha256).digest()
        return f"{self.PREFIX_V1}{self._b64_encode(nonce)}.{self._b64_encode(cipher)}.{self._b64_encode(mac)}"

    def _decrypt_v1(self, text: str) -> str:
        key = self._key()
        body = text[len(self.PREFIX_V1) :]
        parts = body.split(".")
        if len(parts) != 3:
            raise RuntimeError("密文格式错误")
        nonce = self._b64_decode(parts[0])
        cipher = self._b64_decode(parts[1])
        mac = self._b64_decode(parts[2])
        expected = hmac.new(key, b"auth-v1|" + nonce + cipher, hashlib.sha256).digest()
        if not hmac.compare_digest(mac, expected):
            raise RuntimeError("密文校验失败（密钥不匹配或文件损坏）")
        stream = self._build_keystream(key, nonce, len(cipher))
        plain = self._xor_bytes(cipher, stream)
        try:
            return plain.decode("utf-8")
        except Exception as exc:
            raise RuntimeError("密文解码失败") from exc

    def _encrypt_v2(self, text: str) -> str:
        backend, cipher_impl = self._require_modern_cipher()
        key = self._key()
        nonce = os.urandom(12)
        plain = text.encode("utf-8")
        if backend == "cryptography":
            cipher = cipher_impl(key).encrypt(nonce, plain, b"secret-box-v2")
        else:
            aes = cipher_impl.new(key, cipher_impl.MODE_GCM, nonce=nonce)
            aes.update(b"secret-box-v2")
            encrypted, tag = aes.encrypt_and_digest(plain)
            cipher = encrypted + tag
        return f"{self.PREFIX_V2}{self._b64_encode(nonce)}.{self._b64_encode(cipher)}"

    def _decrypt_v2(self, text: str) -> str:
        backend, cipher_impl = self._require_modern_cipher()
        key = self._key()
        body = text[len(self.PREFIX_V2) :]
        parts = body.split(".")
        if len(parts) != 2:
            raise RuntimeError("密文格式错误")
        nonce = self._b64_decode(parts[0])
        cipher = self._b64_decode(parts[1])
        try:
            if backend == "cryptography":
                plain = cipher_impl(key).decrypt(nonce, cipher, b"secret-box-v2")
            else:
                if len(cipher) < 16:
                    raise RuntimeError("密文格式错误")
                encrypted = cipher[:-16]
                tag = cipher[-16:]
                aes = cipher_impl.new(key, cipher_impl.MODE_GCM, nonce=nonce)
                aes.update(b"secret-box-v2")
                plain = aes.decrypt_and_verify(encrypted, tag)
        except Exception as exc:
            raise RuntimeError("密文校验失败（密钥不匹配或文件损坏）") from exc
        try:
            return plain.decode("utf-8")
        except Exception as exc:
            raise RuntimeError("密文解码失败") from exc

    def encrypt(self, plain_text: str) -> str:
        text = str(plain_text or "")
        if not text:
            return ""
        if self.is_encrypted(text):
            return text
        if self.modern_encryption_available():
            return self._encrypt_v2(text)
        return self._encrypt_v1(text)

    def decrypt(self, maybe_cipher_text: str) -> str:
        text = str(maybe_cipher_text or "")
        if not text:
            return ""
        if not self.is_encrypted(text):
            return text
        if text.startswith(self.PREFIX_V2):
            return self._decrypt_v2(text)
        if text.startswith(self.PREFIX_V1):
            return self._decrypt_v1(text)
        raise RuntimeError("不支持的密文版本")


SECRET_BOX = SecretBox()
