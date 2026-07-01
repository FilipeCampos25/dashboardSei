from __future__ import annotations

import sqlite3
import shutil
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.auth import authenticate_user, count_users, create_user, init_auth_db


class DashboardAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path.cwd() / "_tmp_dashboard_auth" / uuid.uuid4().hex
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmpdir / "dashboard_auth.db"
        init_auth_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_and_authenticate_user_with_hash(self) -> None:
        created, _ = create_user("admin", "secret123", db_path=self.db_path, is_admin=True)

        self.assertTrue(created)
        self.assertEqual(count_users(self.db_path), 1)

        user = authenticate_user("admin", "secret123", db_path=self.db_path)
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "admin")
        self.assertTrue(user["is_admin"])

        self.assertIsNone(authenticate_user("admin", "wrongpass", db_path=self.db_path))

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT password_hash, password_salt FROM dashboard_users WHERE username = ?",
                ("admin",),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertNotEqual(row["password_hash"], "secret123")
        self.assertNotEqual(row["password_salt"], "secret123")
        self.assertEqual(len(row["password_salt"]), 32)

    def test_duplicate_user_is_rejected(self) -> None:
        first, _ = create_user("admin", "secret123", db_path=self.db_path, is_admin=True)
        second, _ = create_user("admin", "another123", db_path=self.db_path, is_admin=False)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(count_users(self.db_path), 1)


if __name__ == "__main__":
    unittest.main()
