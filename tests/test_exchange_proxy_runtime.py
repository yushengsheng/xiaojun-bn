#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
import tempfile
import json
from pathlib import Path
import threading
from unittest.mock import patch

from exchange_proxy_runtime import ExchangeProxyRuntime
from secret_box import SecretBox
from sensitive_data import redact_sensitive_text


class PinnedProxyRuntimeTests(unittest.TestCase):
    HYSTERIA2_URI = (
        "hysteria2://secret-auth@example.com:443/?insecure=1&"
        "pinSHA256=EB%3A70%3A12%3ABF%3A67%3AA3%3A93%3A89%3A95%3ACF%3A84%3A70%3A25%3A20%3A25%3A24%3A"
        "68%3ACF%3ADC%3A0A%3AC3%3A11%3A22%3A33%3A44%3A55%3A66%3A77%3A88%3A99%3AAA%3ABB&"
        "sni=ucloud-hy2.local#UCloud-HY2"
    )

    def test_pinned_asset_uses_fixed_url_and_sha256(self) -> None:
        with patch.object(ExchangeProxyRuntime, "_runtime_platform_tag", return_value="darwin"), patch.object(
            ExchangeProxyRuntime, "_runtime_arch_tag", return_value="arm64"
        ):
            name, url, digest = ExchangeProxyRuntime._pinned_sing_box_asset()
        self.assertEqual(name, "sing-box-1.13.5-darwin-arm64.tar.gz")
        self.assertIn("/releases/download/v1.13.5/", url)
        self.assertEqual(digest, "sha256:7700717680c2aa6eb9b034d7114fd16d6f205039255ea406fdd6eb7e3b93d991")

    def test_unknown_architecture_is_not_downloaded_from_latest(self) -> None:
        with patch.object(ExchangeProxyRuntime, "_runtime_platform_tag", return_value="linux"), patch.object(
            ExchangeProxyRuntime, "_runtime_arch_tag", return_value="mips64"
        ):
            with self.assertRaisesRegex(RuntimeError, "固定 sing-box"):
                ExchangeProxyRuntime._pinned_sing_box_asset()

    def test_hysteria_asset_uses_fixed_official_release_and_sha256(self) -> None:
        with patch.object(ExchangeProxyRuntime, "_runtime_platform_tag", return_value="windows"), patch.object(
            ExchangeProxyRuntime, "_runtime_arch_tag", return_value="amd64"
        ):
            name, url, digest = ExchangeProxyRuntime._pinned_hysteria_asset()
        self.assertEqual(name, "hysteria-windows-amd64.exe")
        self.assertIn("apernet/hysteria/releases/download/app/v2.9.2", url)
        self.assertEqual(digest, "sha256:103c7258384d0ebbf0d19ca4bec50d6cb9b0b7be33f5b76f004fed0a8805ff9f")

    def test_hysteria2_uri_and_full_certificate_pin_are_preserved(self) -> None:
        self.assertEqual(ExchangeProxyRuntime.validate_hysteria2_uri(self.HYSTERIA2_URI), self.HYSTERIA2_URI)
        with tempfile.TemporaryDirectory(prefix="xiaojun-hysteria-config-test-") as tmpdir:
            runtime = ExchangeProxyRuntime(Path(tmpdir))
            config_path = runtime._write_hysteria_config(self.HYSTERIA2_URI, 12345)
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["server"], self.HYSTERIA2_URI)
        self.assertEqual(payload["http"], {"listen": "127.0.0.1:12345"})
        self.assertIs(payload["fastOpen"], True)
        self.assertNotIn("tls", payload)

    def test_hysteria2_rejects_invalid_certificate_pin(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "证书指纹无效"):
            ExchangeProxyRuntime.validate_hysteria2_uri("hy2://auth@example.com/?pinSHA256=EB%3A70")

    def test_hysteria2_accepts_official_multi_port_format(self) -> None:
        uri = "hysteria2://auth@example.com:123,5000-6000/?insecure=1"
        self.assertEqual(ExchangeProxyRuntime.validate_hysteria2_uri(uri), uri)

    def test_hysteria2_uri_is_redacted_as_one_secret(self) -> None:
        redacted = redact_sensitive_text(f"proxy={self.HYSTERIA2_URI}")
        self.assertNotIn("secret-auth", redacted)
        self.assertNotIn("pinSHA256", redacted)
        self.assertNotIn("ucloud-hy2.local", redacted)

    def test_runtime_snapshot_does_not_return_ss_source(self) -> None:
        runtime = ExchangeProxyRuntime.__new__(ExchangeProxyRuntime)
        runtime._lock = threading.RLock()
        runtime._backend = "sing-box"
        runtime._raw_source = "ss://sensitive-proxy-source"
        runtime._local_proxy_url = "http://127.0.0.1:12345"
        snapshot = runtime.snapshot()
        self.assertNotIn("raw_source", snapshot)
        self.assertTrue(snapshot["source_configured"])

    def test_cached_runtime_requires_authenticated_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-proxy-runtime-test-") as tmpdir:
            executable = Path(tmpdir) / "sing-box"
            executable.write_bytes(b"trusted-runtime")
            local_box = SecretBox(Path(tmpdir) / ".secret.key")
            with patch("exchange_proxy_runtime.SECRET_BOX", local_box):
                ExchangeProxyRuntime._write_sing_box_manifest(executable)
                self.assertTrue(ExchangeProxyRuntime._verify_cached_sing_box(executable))
                self.assertNotIn("trusted-runtime", ExchangeProxyRuntime._sing_box_manifest_path(executable).read_text(encoding="utf-8"))

                executable.write_bytes(b"modified-runtime")
                self.assertFalse(ExchangeProxyRuntime._verify_cached_sing_box(executable))

    def test_cached_hysteria_runtime_requires_authenticated_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-hysteria-runtime-test-") as tmpdir:
            executable = Path(tmpdir) / "hysteria"
            executable.write_bytes(b"trusted-hysteria-runtime")
            local_box = SecretBox(Path(tmpdir) / ".secret.key")
            with patch("exchange_proxy_runtime.SECRET_BOX", local_box):
                ExchangeProxyRuntime._write_hysteria_manifest(executable)
                self.assertTrue(ExchangeProxyRuntime._verify_cached_hysteria(executable))
                executable.write_bytes(b"modified-runtime")
                self.assertFalse(ExchangeProxyRuntime._verify_cached_hysteria(executable))

    def test_darwin_runtime_is_adhoc_signed_after_verified_download(self) -> None:
        executable = Path("/tmp/sing-box")
        completed = type("Completed", (), {"returncode": 0, "stderr": "", "stdout": ""})()
        with patch("exchange_proxy_runtime.sys.platform", "darwin"), patch.object(Path, "is_file", return_value=True), patch(
            "exchange_proxy_runtime.subprocess.run", return_value=completed
        ) as run:
            ExchangeProxyRuntime._prepare_darwin_runtime(executable)
        run.assert_called_once_with(
            [str(Path("/usr/bin/codesign")), "--force", "--sign", "-", str(executable)],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )

    def test_explicit_runtime_hash_override_is_optional_but_enforced_when_set(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-proxy-runtime-test-") as tmpdir:
            executable = Path(tmpdir) / "sing-box"
            executable.write_bytes(b"trusted-runtime")
            digest = ExchangeProxyRuntime._file_sha256(executable)
            with patch.dict("exchange_proxy_runtime.os.environ", {"SING_BOX_EXECUTABLE_SHA256": digest}, clear=False):
                ExchangeProxyRuntime._verify_external_sing_box_override(executable)
            with patch.dict("exchange_proxy_runtime.os.environ", {"SING_BOX_EXECUTABLE_SHA256": "0" * 64}, clear=False):
                with self.assertRaisesRegex(RuntimeError, "哈希校验失败"):
                    ExchangeProxyRuntime._verify_external_sing_box_override(executable)


if __name__ == "__main__":
    unittest.main()
