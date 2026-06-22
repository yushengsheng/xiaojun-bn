#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
import threading
from unittest.mock import patch

from exchange_proxy_runtime import ExchangeProxyRuntime
from secret_box import SecretBox


class PinnedProxyRuntimeTests(unittest.TestCase):
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

    def test_darwin_runtime_is_adhoc_signed_after_verified_download(self) -> None:
        executable = Path("/tmp/sing-box")
        completed = type("Completed", (), {"returncode": 0, "stderr": "", "stdout": ""})()
        with patch("exchange_proxy_runtime.sys.platform", "darwin"), patch.object(Path, "is_file", return_value=True), patch(
            "exchange_proxy_runtime.subprocess.run", return_value=completed
        ) as run:
            ExchangeProxyRuntime._prepare_darwin_runtime(executable)
        run.assert_called_once_with(
            ["/usr/bin/codesign", "--force", "--sign", "-", str(executable)],
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
