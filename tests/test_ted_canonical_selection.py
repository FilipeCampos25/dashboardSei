from __future__ import annotations

import json
import copy
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pipeline_states import AccessState, DiscoveryState, ExtractionState, OpeningState
from app.services.ted_normalizer import apply_ted_canonical_selection, export_normalized_csv
from app.config import Settings


class TEDCanonicalSelectionCharacterizationTests(unittest.TestCase):
    def test_equal_eligible_candidates_abstain_instead_of_remaining_multiple_gold(self) -> None:
        output_dir = Path(__file__).resolve().parent / "_tmp_ted_canonical_characterization"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            records = []
            for candidate_id in ("candidate-a", "candidate-b"):
                source = output_dir / f"{candidate_id}.json"
                source.write_text(json.dumps(self._instrument_payload(candidate_id)), encoding="utf-8")
                records.append({"publication_status": "published_gold", "json_path": str(source)})

            with patch(
                "app.services.ted_normalizer.get_settings",
                return_value=SimpleNamespace(v2_dual_write=True),
            ):
                result = export_normalized_csv(output_dir, records=records)

            sidecar = json.loads(result["v2_path"].read_text(encoding="utf-8"))
            self.assertEqual(2, result["records"])
            self.assertEqual(
                ["TIE", "TIE"],
                [item["semantic_state"]["canonical"] for item in sidecar["records"]],
            )
            self.assertTrue(all(
                item["document_gold_decision"]["semantic_state"]["publication"] == "BLOCKED"
                for item in sidecar["records"]
            ))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_v2_considers_silver_candidate_without_adding_it_to_legacy_csv(self) -> None:
        output_dir = Path(__file__).resolve().parent / "_tmp_ted_canonical_silver"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            source = output_dir / "silver.json"
            source.write_text(json.dumps(self._instrument_payload("silver-candidate")), encoding="utf-8")
            records = [{"publication_status": "retained_silver", "json_path": str(source)}]
            with patch(
                "app.services.ted_normalizer.get_settings",
                return_value=SimpleNamespace(
                    v2_dual_write=True,
                    ted_canonical_minimum_score=2,
                    ted_canonical_minimum_margin=1,
                ),
            ):
                result = export_normalized_csv(output_dir, records=records)

            sidecar = json.loads(result["v2_path"].read_text(encoding="utf-8"))
            self.assertEqual(0, result["records"])
            self.assertEqual(1, len(sidecar["records"]))
            self.assertEqual("retained_silver", sidecar["records"][0]["legacy_publication_status"])
            self.assertEqual("SELECTED", sidecar["records"][0]["semantic_state"]["canonical"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    @staticmethod
    def _instrument_payload(candidate_id: str) -> dict:
        return {
            "processo": "test-only:process-1",
            "documento": f"document-{candidate_id}",
            "collection": {
                "found": True,
                "candidate_id": candidate_id,
                "document_id": f"document-{candidate_id}",
                "acquisition_state": {
                    "discovery": DiscoveryState.FOUND.value,
                    "opening": OpeningState.OPENED.value,
                    "access": AccessState.ACCESSIBLE.value,
                    "extraction": ExtractionState.EXTRACTED.value,
                },
            },
            "snapshot": {
                "title": "TED 01/2026",
                "text": "Termo de Execucao Descentralizada. Objeto: cooperacao. Valor global.",
                "tables": [],
            },
            "analysis": {"publication_status": "published_gold"},
        }


class TEDCanonicalSelectionMatrixTests(unittest.TestCase):
    def test_threshold_and_margin_are_explicit_runtime_settings(self) -> None:
        settings = Settings(
            TED_CANONICAL_MINIMUM_SCORE=2.5,
            TED_CANONICAL_MINIMUM_MARGIN=0.5,
        )
        self.assertEqual(2.5, settings.ted_canonical_minimum_score)
        self.assertEqual(0.5, settings.ted_canonical_minimum_margin)

    def test_zero_candidates_and_all_ineligible_have_zero_winners(self) -> None:
        self.assertEqual([], apply_ted_canonical_selection([]))
        selected = apply_ted_canonical_selection([
            self.record("P1", "related", quality="high", eligible=False),
            self.record("P1", "meeting", quality="high", eligible=False),
        ])
        self.assertEqual(0, self.winner_count(selected, "P1"))
        self.assertTrue(all(item["semantic_state"]["canonical"] == "INELIGIBLE" for item in selected))

    def test_one_clear_candidate_wins_but_one_below_floor_abstains(self) -> None:
        winner = apply_ted_canonical_selection([self.record("P1", "A", quality="medium")])
        below = apply_ted_canonical_selection([self.record("P1", "A", quality="low")])
        self.assertEqual(1, self.winner_count(winner, "P1"))
        self.assertEqual("canonical.selected", winner[0]["canonical_decision"]["reason"])
        self.assertEqual(0, self.winner_count(below, "P1"))
        self.assertEqual("canonical.unresolved.below_threshold", below[0]["canonical_decision"]["reason"])

    def test_clear_winner_margin_insufficient_and_tie(self) -> None:
        candidates = [self.record("P1", "A", quality="high"), self.record("P1", "B", quality="medium")]
        clear = apply_ted_canonical_selection(copy.deepcopy(candidates), threshold=2, min_margin=1)
        close = apply_ted_canonical_selection(copy.deepcopy(candidates), threshold=2, min_margin=2)
        tied = apply_ted_canonical_selection([
            self.record("P1", "A", quality="high"), self.record("P1", "B", quality="high")
        ])
        self.assertEqual(1, self.winner_count(clear, "P1"))
        self.assertEqual(0, self.winner_count(close, "P1"))
        self.assertEqual("canonical.unresolved.insufficient_margin", close[0]["canonical_decision"]["reason"])
        self.assertEqual(0, self.winner_count(tied, "P1"))
        self.assertTrue(all(item["semantic_state"]["canonical"] == "TIE" for item in tied))

    def test_high_scoring_ineligible_candidate_never_competes(self) -> None:
        selected = apply_ted_canonical_selection([
            self.record("P1", "related", quality="high", eligible=False),
            self.record("P1", "instrument", quality="medium"),
        ])
        self.assertEqual("INELIGIBLE", selected[0]["semantic_state"]["canonical"])
        self.assertEqual("SELECTED", selected[1]["semantic_state"]["canonical"])

    def test_processes_are_independent_and_missing_process_does_not_collapse(self) -> None:
        selected = apply_ted_canonical_selection([
            self.record("P1", "A", quality="medium"),
            self.record("P2", "B", quality="high"),
            self.record("", "C", quality="high"),
            self.record("", "D", quality="high"),
            self.record("P3", "", quality="high"),
        ])
        self.assertEqual(1, self.winner_count(selected, "P1"))
        self.assertEqual(1, self.winner_count(selected, "P2"))
        self.assertTrue(all(item["semantic_state"]["canonical"] == "UNRESOLVED" for item in selected[2:]))

    def test_cardinality_and_order_independence(self) -> None:
        candidates = [
            self.record("P1", "A", quality="high"),
            self.record("P1", "B", quality="medium"),
            self.record("P2", "C", quality="high"),
            self.record("P2", "D", quality="high"),
        ]
        forward = apply_ted_canonical_selection(copy.deepcopy(candidates))
        reverse = apply_ted_canonical_selection(list(reversed(copy.deepcopy(candidates))))
        for process_id in ("P1", "P2"):
            self.assertIn(self.winner_count(forward, process_id), (0, 1))
            self.assertLessEqual(self.winner_count(forward, process_id), 1)
        forward_outcomes = sorted(
            (item["identity"]["candidate_id"], item["semantic_state"]["canonical"]) for item in forward
        )
        reverse_outcomes = sorted(
            (item["identity"]["candidate_id"], item["semantic_state"]["canonical"]) for item in reverse
        )
        self.assertEqual(forward_outcomes, reverse_outcomes)

    @staticmethod
    def winner_count(records: list[dict], process_id: str) -> int:
        return sum(
            item["identity"]["process_id"] == process_id
            and item["semantic_state"]["canonical"] == "SELECTED"
            for item in records
        )

    @staticmethod
    def record(process_id: str, candidate_id: str, *, quality: str, eligible: bool = True) -> dict:
        function = "INSTRUMENT" if eligible else "INELIGIBLE"
        resolved = "ted.instrument" if eligible else "ted.related"
        canonical = "NOT_EVALUATED" if eligible else "INELIGIBLE"
        publication = "PUBLISHED" if eligible else "BLOCKED"
        semantic = {
            "classification": "CONFIRMED" if eligible else "RELATED",
            "resolved_class": "ted",
            "function": function,
            "resolved_function": resolved,
            "affinity": "NOT_EVALUATED",
            "canonical": canonical,
            "publication": publication,
        }
        identity = {
            "process_id": process_id,
            "document_id": f"D-{candidate_id}",
            "candidate_id": candidate_id,
            "source_url": None,
        }
        return {
            "identity": identity,
            "semantic_state": dict(semantic),
            "document_gold_decision": {
                "identity": identity,
                "semantic_state": dict(semantic),
                "reason_codes": ["document_gold.eligible.verifiable_content"] if eligible else ["document_gold.ineligible.function"],
            },
            "quality_status_v2": quality,
        }


if __name__ == "__main__":
    unittest.main()
