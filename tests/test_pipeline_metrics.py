from __future__ import annotations

import csv
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.measure_pipeline_metrics import measure_pipeline_metrics, serialize_report


REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_OUTPUT = REPO_ROOT / "backend" / "output"
GOLDEN_PATH = REPO_ROOT / "tests" / "fixtures" / "characterization" / "legacy_metrics.json"


class PipelineMetricsTests(unittest.TestCase):
    def _write_csv(self, root: Path, filename: str, rows: list[dict[str, str]]) -> None:
        path = root / filename
        fieldnames = list(rows[0]) if rows else ["processo"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _synthetic_baseline(self, root: Path) -> None:
        status_rows = [
            {
                "processo": "P1",
                "found": "True",
                "text_chars": "10",
                "extraction_error": "",
                "validation_status": "valid_for_requested_type",
                "publication_status": "published_gold",
            },
            {
                "processo": "P1",
                "found": "True",
                "text_chars": "0",
                "extraction_error": "",
                "validation_status": "related_but_not_requested",
                "publication_status": "retained_silver",
            },
            {
                "processo": "P2",
                "found": "False",
                "text_chars": "0",
                "extraction_error": "timeout",
                "validation_status": "not_found",
                "publication_status": "retained_silver",
            },
        ]
        final_rows = [{"processo": "P1", "objeto": "present", "numero_acordo": "", "numero_ted": "1"}]
        written: set[str] = set()
        from scripts.measure_pipeline_metrics import FAMILY_SOURCES

        for config in FAMILY_SOURCES.values():
            for key in ("status", "candidates"):
                name = str(config[key])
                if name not in written:
                    self._write_csv(root, name, status_rows)
                    written.add(name)
            name = str(config["final"])
            if name not in written:
                self._write_csv(root, name, final_rows)
                written.add(name)

    def test_six_dimensions_and_four_families_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tests") as temporary_directory:
            root = Path(temporary_directory)
            self._synthetic_baseline(root)
            report = measure_pipeline_metrics(root)
        self.assertEqual(
            report["dimensions"],
            ["discovery", "access", "extraction", "classification", "canonicity", "fields"],
        )
        self.assertEqual(sorted(report["families"]), ["act", "administrative", "pt", "ted"])
        self.assertEqual(report["global"]["discovery"]["counts"], {"found": 8, "not_found": 4})
        self.assertEqual(report["global"]["fields"]["counts"]["business_fields"], 40)

    def test_unknown_access_is_not_invented_and_empty_is_only_extraction_state(self) -> None:
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tests") as temporary_directory:
            root = Path(temporary_directory)
            self._synthetic_baseline(root)
            family = measure_pipeline_metrics(root)["families"]["act"]
        self.assertEqual(family["access"]["counts"], {"not_observable_in_legacy": 3})
        self.assertEqual(family["access"]["measurement_kind"], "unavailable")
        self.assertEqual(family["extraction"]["counts"]["empty_content"], 1)
        self.assertNotIn("inaccessible", family["extraction"]["counts"])

    def test_candidate_is_not_automatically_counted_as_winner(self) -> None:
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tests") as temporary_directory:
            root = Path(temporary_directory)
            self._synthetic_baseline(root)
            canonicity = measure_pipeline_metrics(root)["families"]["act"]["canonicity"]
        self.assertEqual(canonicity["counts"]["candidate_rows"], 3)
        self.assertEqual(canonicity["counts"]["explicit_gold_rows"], 1)
        self.assertIn("processes_without_explicit_gold", canonicity["derived_metrics"])

    def test_field_presence_and_absence_are_derived_separately(self) -> None:
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tests") as temporary_directory:
            root = Path(temporary_directory)
            self._synthetic_baseline(root)
            fields = measure_pipeline_metrics(root)["families"]["act"]["fields"]
        self.assertEqual(fields["measurement_kind"], "derived")
        self.assertEqual(fields["counts"]["objeto"], {"absent_or_empty": 0, "evaluable": 1, "present": 1})
        self.assertEqual(fields["counts"]["numero_acordo"], {"absent_or_empty": 1, "evaluable": 1, "present": 0})

    def test_serialization_is_deterministic_and_contains_no_absolute_path(self) -> None:
        first = serialize_report(measure_pipeline_metrics(REAL_OUTPUT))
        second = serialize_report(measure_pipeline_metrics(REAL_OUTPUT))
        self.assertEqual(first, second)
        self.assertNotIn(str(REPO_ROOT), first)

    def test_real_round_shared_counts_match_characterization(self) -> None:
        report = measure_pipeline_metrics(REAL_OUTPUT)
        golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            report["families"]["pt"]["canonicity"]["counts"]["candidate_rows"],
            golden["round"]["universe"]["pt_candidates"],
        )
        self.assertEqual(
            report["families"]["act"]["canonicity"]["counts"]["candidate_rows"],
            golden["round"]["universe"]["act_candidates"],
        )
        self.assertEqual(
            report["families"]["pt"]["canonicity"]["counts"]["processes_with_multiple_gold"],
            golden["round"]["known_legacy_bugs"]["pt_processes_with_multiple_gold_markings"],
        )

    def test_real_baseline_is_read_only_and_network_is_not_used(self) -> None:
        paths = sorted(path for path in REAL_OUTPUT.rglob("*") if path.is_file())
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        with patch.dict(os.environ, {"OFFLINE_ONLY": "true"}), patch.object(
            socket, "create_connection", side_effect=AssertionError("network attempted")
        ):
            measure_pipeline_metrics(REAL_OUTPUT)
        after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        self.assertEqual(before, after)

    def test_cli_is_independent_of_cwd_and_does_not_import_selenium(self) -> None:
        script = REPO_ROOT / "scripts" / "measure_pipeline_metrics.py"
        environment = os.environ.copy()
        environment.update({"OFFLINE_ONLY": "true", "DEBUG": "false"})
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tests") as temporary_directory:
            result = subprocess.run(
                [sys.executable, str(script), "--baseline-output", str(REAL_OUTPUT)],
                cwd=temporary_directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["report_scope"], "legacy_offline_measurement")
        self.assertNotIn("selenium", result.stdout.lower())


if __name__ == "__main__":
    unittest.main()
