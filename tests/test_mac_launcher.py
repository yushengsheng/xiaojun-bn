from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(sys.platform == "darwin", "macOS launcher only")
class MacLauncherTests(unittest.TestCase):
    def test_launcher_uses_project_python_and_starts_app(self):
        source = ROOT / "launch_no_terminal.sh"
        subprocess.run(["/bin/bash", "-n", str(source)], check=True)
        with tempfile.TemporaryDirectory(prefix="xiaojun-launcher-check-") as tmpdir:
            root = Path(tmpdir)
            shutil.copy2(source, root / source.name)
            shutil.copy2(ROOT / "requirements.lock", root / "requirements.lock")
            python = root / ".venv" / "bin" / "python"
            python.parent.mkdir(parents=True)
            python.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} \"$@\"\n", encoding="utf-8")
            python.chmod(0o755)
            (root / "小军bn.py").write_text("print('LAUNCHER_SMOKE_OK')\n", encoding="utf-8")
            result = subprocess.run(["/bin/bash", str(root / source.name)], cwd=root, env=os.environ.copy(),
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("LAUNCHER_SMOKE_OK", (root / "data" / "logs" / "startup.log").read_text())

    def test_launcher_updates_stale_importable_dependency_from_hashed_lock(self):
        source = ROOT / "launch_no_terminal.sh"
        with tempfile.TemporaryDirectory(prefix="xiaojun-launcher-lock-check-") as tmpdir:
            root = Path(tmpdir)
            shutil.copy2(source, root / source.name)
            shutil.copy2(ROOT / "requirements.lock", root / "requirements.lock")
            stale, installed = root / "stale", root / "install_args.txt"
            stale.touch()
            probe = (
                "import importlib.metadata as m,sys; original=m.version; "
                "m.version=lambda name: '0.0.0' if name=='urllib3' else original(name); "
                "exec(sys.stdin.read())"
            )
            python = root / ".venv" / "bin" / "python"
            python.parent.mkdir(parents=True)
            python.write_text(
                "#!/bin/bash\n"
                f"if [ \"$1\" = '-' ] && [ -f {shlex.quote(str(stale))} ]; then\n"
                f"  exec {shlex.quote(sys.executable)} -c {shlex.quote(probe)} \"$2\"\n"
                "fi\n"
                "if [ \"$1\" = '-m' ] && [ \"$2\" = 'pip' ] && [ \"$3\" = 'install' ]; then\n"
                f"  printf '%s\\n' \"$@\" > {shlex.quote(str(installed))}\n"
                f"  rm {shlex.quote(str(stale))}\n"
                "  exit 0\n"
                "fi\n"
                f"exec {shlex.quote(sys.executable)} \"$@\"\n", encoding="utf-8")
            python.chmod(0o755)
            (root / "小军bn.py").write_text("print('UPDATED_LAUNCHER_OK')\n", encoding="utf-8")
            env = os.environ.copy()
            # The fake interpreter records the install; no network or real dependency mutation.
            fake_uv = root / "bin" / "uv"
            fake_uv.parent.mkdir()
            fake_uv.write_text(
                "#!/bin/bash\n"
                "[ \"$1\" = 'pip' ] && [ \"$2\" = 'install' ] || exit 1\n"
                "shift 2\n"
                "[ \"$1\" = '--python' ] || exit 1\n"
                "shift\n"
                "launcher_test_python=\"$1\"\n"
                "shift\n"
                "exec \"$launcher_test_python\" -m pip install \"$@\"\n", encoding="utf-8")
            fake_uv.chmod(0o755)
            env["PATH"] = str(fake_uv.parent) + ":/usr/bin:/bin"
            result = subprocess.run(["/bin/bash", str(root / source.name)], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            arguments = installed.read_text().splitlines()
            self.assertIn("--require-hashes", arguments)
            self.assertIn(str(root / "requirements.lock"), arguments)
            log = (root / "data" / "logs" / "startup.log").read_text()
            self.assertIn("urllib3 版本不一致", log)
            self.assertIn("UPDATED_LAUNCHER_OK", log)


if __name__ == "__main__":
    unittest.main()
