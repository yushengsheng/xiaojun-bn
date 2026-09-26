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
            python = root / ".venv" / "bin" / "python"
            python.parent.mkdir(parents=True)
            python.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} \"$@\"\n", encoding="utf-8")
            python.chmod(0o755)
            (root / "小军bn.py").write_text("print('LAUNCHER_SMOKE_OK')\n", encoding="utf-8")
            result = subprocess.run(["/bin/bash", str(root / source.name)], cwd=root, env=os.environ.copy(),
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("LAUNCHER_SMOKE_OK", (root / "data" / "logs" / "startup.log").read_text())


if __name__ == "__main__":
    unittest.main()
