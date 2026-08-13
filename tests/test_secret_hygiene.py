from __future__ import annotations

import sys
import unittest
import zipfile
from io import BytesIO
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.check_secret_hygiene import (  # noqa: E402
    packaged_real_env_files,
    tracked_real_env_files,
    unsafe_example_lines,
)


class SecretHygieneTests(unittest.TestCase):
    def test_repository_tracks_no_real_env_file(self) -> None:
        self.assertEqual(tracked_real_env_files(), [])

    def test_root_example_uses_placeholders_for_sensitive_fields(self) -> None:
        self.assertEqual(unsafe_example_lines(REPO_ROOT / ".env.example"), [])

    def test_zip_guard_rejects_real_env_member(self) -> None:
        archive_bytes = BytesIO()
        with zipfile.ZipFile(archive_bytes, "w") as archive:
            archive.writestr("backend/.env", "EXAMPLE_ONLY=placeholder\n")
            archive.writestr("backend/.env.example", "EXAMPLE_ONLY=placeholder\n")
        archive_bytes.seek(0)

        self.assertEqual(packaged_real_env_files(archive_bytes), ["backend/.env"])

    def test_zip_guard_allows_example_member(self) -> None:
        archive_bytes = BytesIO()
        with zipfile.ZipFile(archive_bytes, "w") as archive:
            archive.writestr("backend/.env.example", "EXAMPLE_ONLY=placeholder\n")
        archive_bytes.seek(0)

        self.assertEqual(packaged_real_env_files(archive_bytes), [])


if __name__ == "__main__":
    unittest.main()
