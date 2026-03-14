#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import threading
from pathlib import Path

from app_paths import SECRET_KEY_FILE


class SecretBox:
    """
    轻量本地密钥封装：敏感字段落盘加密，内存中保持明文。
    支持兼容旧明文（未加密值会直接返回）。
    """

    PREFIX = "enc::v1::"
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
        return str(text or "").startswith(cls.PREFIX)

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
        self.key_file.write_text(self._b64_encode(key), encoding="utf-8")
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

    def encrypt(self, plain_text: str) -> str:
        text = str(plain_text or "")
        if not text:
            return ""
        if self.is_encrypted(text):
            return text

        key = self._key()
        nonce = os.urandom(16)
        plain = text.encode("utf-8")
        stream = self._build_keystream(key, nonce, len(plain))
        cipher = self._xor_bytes(plain, stream)
        mac = hmac.new(key, b"auth-v1|" + nonce + cipher, hashlib.sha256).digest()
        return f"{self.PREFIX}{self._b64_encode(nonce)}.{self._b64_encode(cipher)}.{self._b64_encode(mac)}"

    def decrypt(self, maybe_cipher_text: str) -> str:
        text = str(maybe_cipher_text or "")
        if not text:
            return ""
        if not self.is_encrypted(text):
            return text

        key = self._key()
        body = text[len(self.PREFIX) :]
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


SECRET_BOX = SecretBox()
