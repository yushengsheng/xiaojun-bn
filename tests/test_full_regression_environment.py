#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import run_full_regression


class FullRegressionEnvironmentTests(unittest.TestCase):
    def test_rejects_environment_without_pyvenv_config(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-regression-env-") as tmpdir:
            env_dir = Path(tmpdir) / ".venv-regression"
            python = run_full_regression._environment_python(env_dir)
            python.parent.mkdir(parents=True)
            python.write_text("", encoding="utf-8")

            valid, reason = run_full_regression._environment_status(
                env_dir,
                Path(tmpdir) / "requirements-build.lock",
                env_dir / ".requirements-build.sha256",
            )

        self.assertFalse(valid)
        self.assertIn("incomplete", reason)

    def test_accepts_matching_healthy_environment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-regression-env-") as tmpdir:
            root = Path(tmpdir)
            env_dir = root / ".venv-regression"
            python = run_full_regression._environment_python(env_dir)
            python.parent.mkdir(parents=True)
            python.write_text("", encoding="utf-8")
            (env_dir / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
            lock_file = root / "requirements-build.lock"
            lock_file.write_text("requests==1\n", encoding="utf-8")
            marker = env_dir / ".requirements-build.sha256"
            marker.write_text(run_full_regression._lock_digest(lock_file), encoding="ascii")
            probe = {
                "version": [3, 11],
                "prefix": str(env_dir),
                "base_prefix": str(root / "python-base"),
            }
            healthy = subprocess.CompletedProcess([], 0, "")
            installed_versions = {"requests": "1"}

            with (
                patch.object(run_full_regression, "_probe_python", return_value=probe),
                patch.object(run_full_regression, "_installed_versions", return_value=installed_versions),
                patch.object(run_full_regression, "_run_capture", return_value=healthy),
            ):
                valid, reason = run_full_regression._environment_status(
                    env_dir,
                    lock_file,
                    marker,
                )

        self.assertTrue(valid)
        self.assertEqual(reason, "environment is valid")

    def test_rejects_environment_when_lock_changes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-regression-env-") as tmpdir:
            root = Path(tmpdir)
            env_dir = root / ".venv-regression"
            python = run_full_regression._environment_python(env_dir)
            python.parent.mkdir(parents=True)
            python.write_text("", encoding="utf-8")
            (env_dir / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
            lock_file = root / "requirements-build.lock"
            lock_file.write_text("new-lock\n", encoding="utf-8")
            marker = env_dir / ".requirements-build.sha256"
            marker.write_text("old-lock", encoding="ascii")
            probe = {
                "version": [3, 11],
                "prefix": str(env_dir),
                "base_prefix": str(root / "python-base"),
            }

            with patch.object(run_full_regression, "_probe_python", return_value=probe):
                valid, reason = run_full_regression._environment_status(
                    env_dir,
                    lock_file,
                    marker,
                )

        self.assertFalse(valid)
        self.assertIn("lock has changed", reason)

    def test_rejects_installed_version_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="xiaojun-regression-env-") as tmpdir:
            root = Path(tmpdir)
            env_dir = root / ".venv-regression"
            python = run_full_regression._environment_python(env_dir)
            python.parent.mkdir(parents=True)
            python.write_text("", encoding="utf-8")
            (env_dir / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
            lock_file = root / "requirements-build.lock"
            lock_file.write_text("requests==1\n", encoding="utf-8")
            marker = env_dir / ".requirements-build.sha256"
            marker.write_text(run_full_regression._lock_digest(lock_file), encoding="ascii")
            probe = {
                "version": [3, 11],
                "prefix": str(env_dir),
                "base_prefix": str(root / "python-base"),
            }

            with (
                patch.object(run_full_regression, "_probe_python", return_value=probe),
                patch.object(run_full_regression, "_installed_versions", return_value={"requests": "2"}),
            ):
                valid, reason = run_full_regression._environment_status(
                    env_dir,
                    lock_file,
                    marker,
                )

        self.assertFalse(valid)
        self.assertIn("do not match", reason)

    def test_invalid_environment_is_rebuilt_and_rechecked(self) -> None:
        rebuilt_python = Path("/managed/.venv-regression/bin/python")
        with (
            patch.object(
                run_full_regression,
                "_environment_status",
                side_effect=[
                    (False, "virtual environment files are incomplete"),
                    (True, "environment is valid"),
                ],
            ) as status,
            patch.object(
                run_full_regression,
                "_rebuild_environment",
                return_value=rebuilt_python,
            ) as rebuild,
        ):
            result = run_full_regression.ensure_regression_environment()

        self.assertEqual(result, rebuilt_python)
        self.assertEqual(status.call_count, 2)
        rebuild.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
