#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
