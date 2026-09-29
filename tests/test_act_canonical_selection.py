from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.config import Settings
from app.services.act_normalizer import export_normalized_csv


class ACTCanonicalSelectionTests(unittest.TestCase):
    output_dir = ROOT / "tests" / "_tmp_act_canonical_selection"
    process_id = "60090.000001/2026-00"

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def test_runtime_threshold_and_margin_are_explicit(self) -> None:
        settings = Settings(
            ACT_CANONICAL_MINIMUM_SCORE=50,
            ACT_CANONICAL_MINIMUM_MARGIN=5,
        )
        self.assertEqual(50, settings.act_canonical_minimum_score)
        self.assertEqual(5, settings.act_canonical_minimum_margin)

    def test_threshold_margin_tie_and_single_candidate_matrix(self) -> None:
        cases = (
            ("negative_pair", {"A": -20, "B": -40}, 0, 5, None, "UNRESOLVED", "canonical.unresolved.below_threshold"),
            ("single_below", {"A": -20}, 0, 5, None, "UNRESOLVED", "canonical.unresolved.below_threshold"),
            ("single_above", {"A": 80}, 50, 5, "A", "SELECTED", "canonical.selected"),
            ("small_margin", {"A": 80, "B": 78}, 50, 5, None, "UNRESOLVED", "canonical.unresolved.insufficient_margin"),
            ("tie", {"A": 80, "B": 80}, 50, 5, None, "TIE", "canonical.unresolved.tie"),
            ("clear", {"A": 80, "B": 60}, 50, 5, "A", "SELECTED", "canonical.selected"),
        )
        for name, scores, threshold, margin, winner, state, reason in cases:
            with self.subTest(name=name):
                rows, normalized = self._run(scores, threshold=threshold, margin=margin)
                selected = [row for row in rows if row["publication_status"] == "published_gold"]
                self.assertEqual([winner] if winner else [], [row["canonical_candidate_id"] for row in normalized])
                self.assertEqual(1 if winner else 0, len(selected))
                eligible_rows = [row for row in rows if row["canonical_candidate_id"] in scores]
                expected_states = {state} if not winner else {"SELECTED", *("UNRESOLVED" for _ in scores if len(scores) > 1)}
                self.assertEqual(expected_states, {row["canonical_state"] for row in eligible_rows})
                self.assertTrue(all(row["canonical_reason"] == reason for row in eligible_rows))
                self.assertEqual(len(scores), len(eligible_rows))

    def test_ineligible_function_and_affinity_never_reenter_competition(self) -> None:
        payloads = {
            "HOST": self._payload("HOST"),
            "EXTERNAL": self._payload("EXTERNAL", document_process="60090.000002/2026-00"),
            "REPORT": self._payload("REPORT", report=True),
        }
        rows, normalized = self._run_payloads(
            payloads,
            {"HOST": 80, "EXTERNAL": 500, "REPORT": 600},
            threshold=50,
            margin=5,
        )
        self.assertEqual(["HOST"], [row["canonical_candidate_id"] for row in normalized])
        by_id = {row["canonical_candidate_id"]: row for row in rows}
        self.assertEqual("published_gold", by_id["HOST"]["publication_status"])
        self.assertEqual("retained_silver", by_id["EXTERNAL"]["publication_status"])
        self.assertEqual("retained_silver", by_id["REPORT"]["publication_status"])
        self.assertEqual("INELIGIBLE", by_id["EXTERNAL"]["canonical_state"])
        self.assertEqual("INELIGIBLE", by_id["REPORT"]["canonical_state"])

    def test_no_eligible_candidate_abstains_and_preserves_records(self) -> None:
        payloads = {
            "EXTERNAL": self._payload("EXTERNAL", document_process="60090.000002/2026-00"),
            "REPORT": self._payload("REPORT", report=True),
        }
        rows, normalized = self._run_payloads(payloads, {"EXTERNAL": 500, "REPORT": 600}, threshold=0, margin=1)
        self.assertEqual([], normalized)
        self.assertEqual(2, len(rows))
        self.assertTrue(all(row["canonical_state"] == "INELIGIBLE" for row in rows))
        self.assertTrue(
            all(row["canonical_selection_reason"] == "canonical.unresolved.no_candidate" for row in rows)
        )
        self.assertTrue(all(row["publication_status"] == "retained_silver" for row in rows))

    def _run(self, scores: dict[str, int], *, threshold: float, margin: float):
        payloads = {candidate_id: self._payload(candidate_id) for candidate_id in scores}
        return self._run_payloads(payloads, scores, threshold=threshold, margin=margin)

    def _run_payloads(self, payloads, scores, *, threshold: float, margin: float):
        shutil.rmtree(self.output_dir, ignore_errors=True)
        self.output_dir.mkdir(parents=True)
        for candidate_id, payload in payloads.items():
            path = self.output_dir / f"acordo_cooperacao_tecnica_{candidate_id}.json"
            path.write_text(json.dumps(payload), encoding="utf-8")

        def score(payload, _record):
            return scores[payload["collection"]["candidate_id"]]

        settings = SimpleNamespace(
            act_canonical_minimum_score=threshold,
            act_canonical_minimum_margin=margin,
            v2_dual_write=False,
        )
        with patch("app.services.act_normalizer._canonical_score", side_effect=score), patch(
            "app.services.act_normalizer.get_settings", return_value=settings
        ):
            export_normalized_csv(self.output_dir)
        with (self.output_dir / "act_classificacao_latest.csv").open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        normalized_path = self.output_dir / "act_normalizado_latest.csv"
        with normalized_path.open(encoding="utf-8-sig", newline="") as stream:
            normalized = list(csv.DictReader(stream))
        return rows, normalized

    def _payload(self, candidate_id: str, *, document_process: str | None = None, report: bool = False):
        process = document_process or self.process_id
        title = "Relatorio de encerramento de ACT" if report else f"Acordo de Cooperacao Tecnica {candidate_id}"
        body = (
            "RELATORIO FINAL DE ENCERRAMENTO DO ACORDO DE COOPERACAO TECNICA. "
            if report
            else "ACORDO DE COOPERACAO TECNICA QUE ENTRE SI CELEBRAM O CENSIPAM E O PARCEIRO. "
            "CLAUSULA PRIMEIRA - DO OBJETO. O objeto do presente acordo e a cooperacao."
        )
        return {
            "processo": self.process_id,
            "requested_type": "act",
            "snapshot": {"title": title, "text": f"PROCESSO No {process}. {body}"},
            "collection": {"candidate_id": candidate_id, "chosen_documento": title},
        }


if __name__ == "__main__":
    unittest.main()
