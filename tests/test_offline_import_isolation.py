from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class OfflineImportIsolationTests(unittest.TestCase):
    def test_pure_normalizers_import_without_selenium_driver_or_network(self) -> None:
        script = """
import builtins
import socket
import sys

real_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == 'selenium' or name.startswith('selenium.'):
        raise AssertionError(f'pure import attempted Selenium: {name}')
    return real_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
socket.create_connection = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('network attempted'))
from app.services import act_normalizer, act_process_affinity, pt_normalizer, ted_normalizer
assert not any(name == 'selenium' or name.startswith('selenium.') for name in sys.modules)
"""
        environment = os.environ.copy()
        environment.update({"OFFLINE_ONLY": "true", "DEBUG": "false"})
        python_path = [str(REPO_ROOT / "backend"), str(REPO_ROOT)]
        if environment.get("PYTHONPATH"):
            python_path.append(environment["PYTHONPATH"])
        environment["PYTHONPATH"] = os.pathsep.join(python_path)

        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=REPO_ROOT / "tests" / "fixtures",
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
