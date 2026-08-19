from __future__ import annotations

import sys
import unittest
import zipfile
from io import BytesIO
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.build_release_package import (  # noqa: E402
    ALLOWED_ROOT_FILES,
    ALLOWED_TREES,
    REQUIRED_MEMBERS,
    ReleasePackageError,
    build_release_bytes,
    is_allowlisted,
    prohibited_reason,
    validate_release_package,
)


def archive_with(extra: dict[str, str] | None = None) -> BytesIO:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in sorted(REQUIRED_MEMBERS):
            archive.writestr(member, "placeholder\n")
        for name, content in (extra or {}).items():
            archive.writestr(name, content)
    buffer.seek(0)
    return buffer


class ReleasePackageTests(unittest.TestCase):
    def test_real_release_is_deterministic_and_allowlisted(self) -> None:
        first = build_release_bytes()
        second = build_release_bytes()
        self.assertEqual(first, second)
        with zipfile.ZipFile(BytesIO(first)) as archive:
            names = archive.namelist()
        self.assertTrue(REQUIRED_MEMBERS.issubset(names))
        self.assertTrue(all(is_allowlisted(name) for name in names))
        self.assertTrue(all(prohibited_reason(name) is None for name in names))

    def test_allowlist_is_explicit(self) -> None:
        self.assertEqual(
            ALLOWED_ROOT_FILES,
            {".env.example", "README.md", "constraints.txt", "dashboard_streamlit.py", "requirements.txt"},
        )
        self.assertEqual(ALLOWED_TREES, {"assets", "backend", "dashboard", "scripts"})
        self.assertFalse(is_allowlisted("tests/test_release_package.py"))
        self.assertFalse(is_allowlisted("arbitrary.txt"))

    def test_rejects_environment_database_temp_cache_and_output(self) -> None:
        prohibited = [
            "backend/.env",
            "backend/data/auth.db",
            "backend/_tmp_auth/session.json",
            "backend/__pycache__/module.pyc",
            "backend/output/result.csv",
            "dashboard/.pytest_cache/state.txt",
        ]
        for name in prohibited:
            with self.subTest(name=name), self.assertRaises(ReleasePackageError):
                validate_release_package(archive_with({name: "placeholder\n"}))

    def test_rejects_member_outside_allowlist_and_unsafe_path(self) -> None:
        for name in ["tests/test_example.py", "../escape.txt", "/absolute.txt"]:
            with self.subTest(name=name), self.assertRaises(ReleasePackageError):
                validate_release_package(archive_with({name: "placeholder\n"}))

    def test_rejects_high_confidence_secret_content(self) -> None:
        with self.assertRaises(ReleasePackageError):
            validate_release_package(archive_with({"backend/settings.ini": "SERVICE_TOKEN=real-value\n"}))

    def test_rejects_missing_required_member(self) -> None:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("README.md", "placeholder\n")
        buffer.seek(0)
        with self.assertRaises(ReleasePackageError):
            validate_release_package(buffer)


if __name__ == "__main__":
    unittest.main()
