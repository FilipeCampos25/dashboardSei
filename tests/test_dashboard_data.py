from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.dashboard_data import load_dashboard_bundle, read_csv_safe


class DashboardDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root_dir = (Path.cwd() / "_tmp_dashboard_data" / uuid.uuid4().hex).resolve()
        shutil.rmtree(self.root_dir, ignore_errors=True)
        (self.root_dir / "backend" / "output").mkdir(parents=True, exist_ok=True)
        (self.root_dir / "output").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root_dir, ignore_errors=True)

    def test_read_csv_safe_handles_absent_empty_and_malformed_files(self) -> None:
        absent = self.root_dir / "backend" / "output" / "absent.csv"
        self.assertTrue(read_csv_safe(absent, ["processo"]).empty)

        empty = self.root_dir / "backend" / "output" / "empty.csv"
        empty.write_text("", encoding="utf-8")
        self.assertTrue(read_csv_safe(empty, ["processo"]).empty)

        malformed = self.root_dir / "backend" / "output" / "malformed.csv"
        malformed.write_text('processo,objeto\n"60090.000001/2026-01,sem fechamento', encoding="utf-8")
        self.assertTrue(read_csv_safe(malformed, ["processo", "objeto"]).empty)

    def test_collection_date_uses_latest_log_timestamp(self) -> None:
        backend_output = self.root_dir / "backend" / "output"
        with (backend_output / "dashboard_ready_latest.csv").open("w", encoding="utf-8-sig", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=["processo"])
            writer.writeheader()
            writer.writerow({"processo": "60090.000001/2026-01"})

        with (self.root_dir / "output" / "execution_log_latest.json").open("w", encoding="utf-8") as file_obj:
            file_obj.write(json.dumps({"timestamp": "2026-06-16T13:04:47", "level": "INFO"}) + "\n")
            file_obj.write(json.dumps({"timestamp": "2026-06-16T13:20:39", "level": "INFO"}) + "\n")

        bundle = load_dashboard_bundle(self.root_dir)

        self.assertEqual(bundle["collection_meta"]["data_ultima_coleta"], "2026-06-16T13:20:39")
        self.assertEqual(bundle["collection_meta"]["fonte_data_ultima_coleta"], "execution_log")


if __name__ == "__main__":
    unittest.main()
