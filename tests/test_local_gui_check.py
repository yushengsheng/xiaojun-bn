#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import argparse
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from tools import run_local_gui_check


class LocalGuiCheckTests(unittest.TestCase):
    def test_prefers_explicit_gui_python(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-gui-python-test-") as tmpdir:
            gui_python = Path(tmpdir) / "python"
            gui_python.write_text("", encoding="utf-8")
            gui_python.chmod(0o755)
            with patch.dict(run_local_gui_check.os.environ, {"XIAOJUN_GUI_PYTHON": str(gui_python)}):
                candidates = run_local_gui_check._candidate_pythons()

        self.assertEqual(candidates[0], gui_python)

    def test_guarded_child_env_name_is_stable(self) -> None:
        self.assertEqual(run_local_gui_check.CHILD_ENV_KEY, "XIAOJUN_GUI_CHECK_CHILD")

    def test_prefers_persistent_regression_environment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-gui-python-test-") as tmpdir:
            regression_python = Path(tmpdir) / "python"
            regression_python.write_text("", encoding="utf-8")
            regression_python.chmod(0o755)
            with (
                patch.dict(run_local_gui_check.os.environ, {}, clear=True),
                patch.object(run_local_gui_check, "PERSISTENT_REGRESSION_PYTHON", regression_python),
            ):
                candidates = run_local_gui_check._candidate_pythons()

        self.assertEqual(candidates[0], regression_python)

    def test_applies_verified_sing_box_defaults_when_available(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-sing-box-test-") as tmpdir:
            sing_box = Path(tmpdir) / "sing-box"
            sing_box.write_text("stub", encoding="utf-8")
            env: dict[str, str] = {}
            with patch.object(run_local_gui_check, "VERIFIED_SING_BOX_PATH", sing_box):
                run_local_gui_check._apply_verified_runtime_defaults(env)

        self.assertEqual(env["SING_BOX_PATH"], str(sing_box))
        self.assertEqual(env["SING_BOX_EXECUTABLE_SHA256"], run_local_gui_check.VERIFIED_SING_BOX_SHA256)

    def test_preserves_explicit_sing_box_defaults(self) -> None:
        env = {
            "SING_BOX_PATH": "/custom/sing-box",
            "SING_BOX_EXECUTABLE_SHA256": "0" * 64,
        }
        run_local_gui_check._apply_verified_runtime_defaults(env)

        self.assertEqual(env["SING_BOX_PATH"], "/custom/sing-box")
        self.assertEqual(env["SING_BOX_EXECUTABLE_SHA256"], "0" * 64)

    def test_regression_uses_verified_copies_without_changing_live_runtime(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_box = root / "live" / "sing-box"
            source_hysteria = root / "live" / "hysteria"
            source_box.parent.mkdir()
            source_box.write_bytes(b"verified sing-box")
            source_hysteria.write_bytes(b"verified hysteria")
            box_hash = hashlib.sha256(source_box.read_bytes()).hexdigest()
            hysteria_hash = hashlib.sha256(source_hysteria.read_bytes()).hexdigest()
            env = {"SING_BOX_PATH": str(source_box), "HYSTERIA_PATH": str(source_hysteria)}
            with patch.object(run_local_gui_check, "VERIFIED_SING_BOX_PATH", source_box), \
                 patch.object(run_local_gui_check, "VERIFIED_HYSTERIA_PATH", source_hysteria), \
                 patch.object(run_local_gui_check, "VERIFIED_SING_BOX_SHA256", box_hash), \
                 patch.object(run_local_gui_check, "VERIFIED_HYSTERIA_SHA256", hysteria_hash):
                run_local_gui_check._stage_verified_proxy_runtimes(env, root / "stage")
                self.assertEqual(Path(env["SING_BOX_PATH"]).read_bytes(), source_box.read_bytes())
                self.assertEqual(Path(env["HYSTERIA_PATH"]).read_bytes(), source_hysteria.read_bytes())
                self.assertNotEqual(Path(env["SING_BOX_PATH"]), source_box)
                self.assertNotEqual(Path(env["HYSTERIA_PATH"]), source_hysteria)
                self.assertEqual(env["SING_BOX_EXECUTABLE_SHA256"], box_hash)
                self.assertEqual(env["HYSTERIA_EXECUTABLE_SHA256"], hysteria_hash)

            with patch.object(run_local_gui_check, "VERIFIED_SING_BOX_PATH", source_box), \
                 patch.object(run_local_gui_check, "VERIFIED_SING_BOX_SHA256", "0" * 64):
                with self.assertRaisesRegex(RuntimeError, "哈希不匹配"):
                    run_local_gui_check._stage_verified_proxy_runtimes({}, root / "bad-stage")

    def test_regression_child_stages_proxy_before_selftest(self):
        args = argparse.Namespace(full_selftest=False, regression=True)
        with patch.dict(run_local_gui_check.os.environ, {}, clear=False), \
             patch.object(run_local_gui_check.sys, "platform", "darwin"), \
             patch.object(run_local_gui_check, "_stage_verified_proxy_runtimes") as stage, \
             patch.object(run_local_gui_check.subprocess, "run") as run:
            run.return_value.returncode = 0
            self.assertEqual(run_local_gui_check._run_child(args), 0)
        stage.assert_called_once()
        self.assertEqual(run.call_args.args[0][1:], ["tools/run_regression.py", "--gui", "--strict-pip-check"])


if __name__ == "__main__":
    unittest.main()
