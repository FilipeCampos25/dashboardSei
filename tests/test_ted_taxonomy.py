from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from app.services.ted_classifier import classify_ted_snapshot
from app.services.ted_normalizer import build_normalized_record, build_ted_v2_record
from tests.fixture_loader import load_fixture


class TedTaxonomyTests(unittest.TestCase):
    def test_document_functions_prefer_document_content_over_ted_title(self) -> None:
        fixture = load_fixture("ted_taxonomy_cases.json")
        for case in fixture["payload"]["cases"]:
            with self.subTest(name=case["id"]):
                result = classify_ted_snapshot({"title": case["title"], "text": case["text"], "tables": []})
                self.assertEqual(case["expected_function"], result.resolved_function)

    def test_title_only_is_candidate_without_confirmed_instrument_function(self) -> None:
        result = classify_ted_snapshot(
            {"title": "Termo de Execucao Descentralizada TED 01/2026", "text": "", "tables": []}
        )

        self.assertEqual("CANDIDATE", result.classification)
        self.assertIsNone(result.resolved_function)

    def test_unknown_content_abstains_and_classification_is_deterministic(self) -> None:
        snapshot = {"title": "Documento", "text": "Conteudo sem marcadores documentais.", "tables": []}

        first = classify_ted_snapshot(snapshot)
        second = classify_ted_snapshot(snapshot)

        self.assertEqual("AMBIGUOUS", first.classification)
        self.assertIsNone(first.resolved_function)
        self.assertEqual(first, second)

    def test_ted_mention_without_document_function_abstains(self) -> None:
        result = classify_ted_snapshot(
            {"title": "Documento", "text": "Documento que cita o TED 01/2026 sem identificar sua propria funcao.", "tables": []}
        )

        self.assertEqual("AMBIGUOUS", result.function)
        self.assertIsNone(result.resolved_function)
        self.assertEqual("ted.content.insufficient_instrument_evidence", result.reason)

    def test_empty_snapshot_abstains(self) -> None:
        result = classify_ted_snapshot({"title": "", "text": "", "tables": []})

        self.assertEqual("AMBIGUOUS", result.function)
        self.assertIsNone(result.resolved_function)

    def test_tables_are_verifiable_content(self) -> None:
        result = classify_ted_snapshot(
            {"title": "TED 01/2026", "text": "", "tables": [{"rows": [["Cabecalho", "PLANO DE TRABALHO do TED"]]}]}
        )

        self.assertEqual("ted.work_plan", result.resolved_function)
        self.assertEqual("snapshot.tables", result.evidence_source)

    def test_chosen_documento_is_not_treated_as_document_content(self) -> None:
        result = classify_ted_snapshot(
            {"title": "", "text": "", "tables": [], "chosen_documento": "Termo de Execucao Descentralizada"}
        )

        self.assertIsNone(result.resolved_function)

    def test_v2_sidecar_records_function_without_changing_legacy_publication(self) -> None:
        payload = {
            "processo": "test-only:ted-taxonomy",
            "snapshot": {
                "title": "TED 01/2026",
                "text": "PLANO DE TRABALHO do Termo de Execucao Descentralizada TED 01/2026.",
                "tables": [],
            },
            "collection": {"found": True},
            "analysis": {"publication_status": "published_gold"},
        }
        record, diagnostics = build_normalized_record(payload, "taxonomy.json")
        v2 = build_ted_v2_record(record, payload, diagnostics)

        self.assertEqual("published_gold", record["publication_status"])
        self.assertEqual("RELATED", v2["semantic_state"]["classification"])
        self.assertEqual("RELATED", v2["semantic_state"]["function"])
        self.assertEqual("ted.work_plan", v2["semantic_state"]["resolved_function"])


if __name__ == "__main__":
    unittest.main()
