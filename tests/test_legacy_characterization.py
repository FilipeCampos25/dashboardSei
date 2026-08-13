from __future__ import annotations

import csv
import hashlib
import json
import os
import socket
import unittest
from collections import Counter
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from tests.fixture_loader import FIXTURE_ROOT, load_all_fixtures


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "backend" / "output"
GOLDEN_PATH = REPO_ROOT / "tests" / "fixtures" / "characterization" / "legacy_metrics.json"


def _read_csv(name: str) -> list[dict[str, str]]:
    with (OUTPUT_DIR / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _count(rows: list[dict[str, str]], field: str, value: str) -> int:
    return sum(row.get(field) == value for row in rows)


def _nonempty(rows: list[dict[str, str]], field: str) -> int:
    return sum(bool((row.get(field) or "").strip()) for row in rows)


def _distinct(rows: list[dict[str, str]], field: str) -> int:
    return len({row[field] for row in rows if row.get(field)})


def _multiple_per_process(rows: list[dict[str, str]]) -> int:
    return sum(count > 1 for count in Counter(row["processo"] for row in rows).values())


def collect_legacy_metrics() -> dict[str, object]:
    fixtures = load_all_fixtures()
    manifest = json.loads((FIXTURE_ROOT / "manifest.json").read_text(encoding="utf-8"))
    fixture_paths = [item["path"] for item in manifest["fixtures"]]
    fixture_families = Counter(item["metadata"]["family"] for item in fixtures)
    technical_states = Counter(item["metadata"]["technical_state"] for item in fixtures)

    pt = _read_csv("pt_auditoria_latest.csv")
    pt_final = _read_csv("pt_normalizado_latest.csv")
    act = _read_csv("act_classificacao_latest.csv")
    act_final = _read_csv("act_normalizado_latest.csv")
    ted = _read_csv("ted_status_execucao_latest.csv")
    ted_final = _read_csv("ted_normalizado_latest.csv")
    administrative = _read_csv("documento_administrativo_status_execucao_latest.csv")
    administrative_final = _read_csv("documento_administrativo_normalizado_latest.csv")
    dashboard = _read_csv("dashboard_ready_latest.csv")

    pt_gold = [row for row in pt if row["publication_status"] == "published_gold"]
    return {
        "schema_version": "1.0",
        "expectation_policy": {
            "update_mode": "explicit_review_only",
            "normative_categories": ["correct_invariant"],
            "non_normative_categories": ["known_legacy_bug", "legacy_characterization"],
        },
        "fixtures": {
            "family_counts": dict(sorted(fixture_families.items())),
            "technical_state_counts": dict(sorted(technical_states.items())),
            "correct_invariants": {
                "all_origins_are_synthetic": all(
                    item["metadata"]["origin"] == "synthetic" for item in fixtures
                ),
                "all_manifest_paths_are_relative": all(
                    not PurePosixPath(path).is_absolute() and "\\" not in path
                    for path in fixture_paths
                ),
                "families_represented": sorted(fixture_families),
            },
        },
        "round": {
            "universe": {
                "dashboard_processes": len(dashboard),
                "pt_candidates": len(pt),
                "pt_candidate_processes": _distinct(pt, "processo"),
                "pt_final_rows": len(pt_final),
                "act_candidates": len(act),
                "act_candidate_processes": _distinct(act, "processo"),
                "act_final_rows": len(act_final),
                "ted_final_rows": len(ted_final),
                "administrative_final_rows": len(administrative_final),
            },
            "publication": {
                "pt_gold_rows": len(pt_gold),
                "pt_silver_rows": _count(pt, "publication_status", "retained_silver"),
                "pt_gold_processes": _distinct(pt_gold, "processo"),
                "act_gold_rows": _count(act, "publication_status", "published_gold"),
                "act_silver_rows": _count(act, "publication_status", "retained_silver"),
                "ted_gold_rows": _count(ted, "publication_status", "published_gold"),
                "administrative_gold_rows": _count(
                    administrative, "publication_status", "published_gold"
                ),
            },
            "known_legacy_bugs": {
                "ted_empty_published_gold": sum(
                    row["publication_status"] == "published_gold"
                    and int(row.get("text_chars") or 0) == 0
                    for row in ted
                ),
                "administrative_empty_published_gold": sum(
                    row["publication_status"] == "published_gold"
                    and int(row.get("text_chars") or 0) == 0
                    for row in administrative
                ),
                "pt_processes_with_multiple_gold_markings": _multiple_per_process(pt_gold),
            },
            "legacy_characterization": {
                "act_multi_candidate_processes": _multiple_per_process(act),
                "act_candidates_discarded_by_relative_score": _count(
                    act, "normalization_status", "descartado_por_desempate"
                ),
                "pt_rows_with_preview_number": _nonempty(pt, "preview_numero_act"),
                "dashboard_rows_with_preview_number": _nonempty(
                    dashboard, "preview_numero_act"
                ),
                "dashboard_quality_counts": dict(
                    sorted(Counter(row["quality_status"] for row in dashboard).items())
                ),
            },
        },
        "excluded_unstable_values": [
            "absolute_paths",
            "captured_at",
            "file_mtime",
            "raw_document_content",
            "temporary_directory_names",
        ],
    }


def assert_matches_golden(actual: dict[str, object], expected: dict[str, object]) -> None:
    if actual != expected:
        raise AssertionError(
            "Legacy metrics changed. Review the behavioral diff and update "
            "legacy_metrics.json explicitly only when the change is intentional."
        )


class LegacyCharacterizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

    def test_legacy_metrics_match_explicit_golden_master_offline(self) -> None:
        source_files = sorted(
            [GOLDEN_PATH, FIXTURE_ROOT / "manifest.json"]
            + list(FIXTURE_ROOT.glob("*.json"))
            + list(OUTPUT_DIR.rglob("*latest*"))
        )
        before = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in source_files
            if path.is_file()
        }
        with patch.dict(os.environ, {"OFFLINE_ONLY": "true"}), patch.object(
            socket, "create_connection", side_effect=AssertionError("network attempted")
        ):
            actual = collect_legacy_metrics()
        assert_matches_golden(actual, self.expected)
        after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}
        self.assertEqual(before, after, "characterization must not modify fixtures or outputs")

    def test_unexpected_metric_change_fails_without_updating_expectations(self) -> None:
        original = GOLDEN_PATH.read_bytes()
        changed = json.loads(json.dumps(self.expected))
        changed["round"]["universe"]["dashboard_processes"] += 1
        with self.assertRaisesRegex(AssertionError, "update legacy_metrics.json explicitly"):
            assert_matches_golden(changed, self.expected)
        self.assertEqual(GOLDEN_PATH.read_bytes(), original)

    def test_golden_master_contains_only_relative_policy_and_labeled_legacy_bugs(self) -> None:
        serialized = GOLDEN_PATH.read_text(encoding="utf-8")
        self.assertEqual(
            self.expected["expectation_policy"]["update_mode"], "explicit_review_only"
        )
        self.assertIn("known_legacy_bugs", self.expected["round"])
        self.assertIn("correct_invariants", self.expected["fixtures"])
        self.assertNotIn(str(REPO_ROOT), serialized)
        self.assertNotIn("C:/Users/", serialized.replace("\\", "/"))

    def test_legacy_ted_empty_gold_characterization(self) -> None:
        self.assertEqual(self.expected["round"]["known_legacy_bugs"]["ted_empty_published_gold"], 9)

    def test_legacy_admin_empty_gold_characterization(self) -> None:
        self.assertEqual(
            self.expected["round"]["known_legacy_bugs"]["administrative_empty_published_gold"],
            12,
        )

    def test_legacy_pt_multiple_gold_and_act_relative_score_characterization(self) -> None:
        self.assertEqual(
            self.expected["round"]["known_legacy_bugs"]["pt_processes_with_multiple_gold_markings"],
            1,
        )
        self.assertEqual(
            self.expected["round"]["legacy_characterization"]["act_candidates_discarded_by_relative_score"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
