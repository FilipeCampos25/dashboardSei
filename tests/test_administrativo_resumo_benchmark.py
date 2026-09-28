from __future__ import annotations

import json
import statistics
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.documento_administrativo_normalizer import (
    build_normalized_record,
    _extract_resumo_candidate,
    _extract_resumo_legacy,
)


BENCHMARK_PATH = Path(__file__).parent / "benchmarks" / "administrativo_resumo_v1.json"


class AdministrativeSummaryBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.benchmark = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))

    def test_dataset_is_small_sanitized_and_covers_administrative_classes(self) -> None:
        examples = self.benchmark["examples"]
        self.assertGreaterEqual(len(examples), 8)
        self.assertLessEqual(len(examples), 20)
        self.assertEqual(
            {example["class"] for example in examples},
            {"nota_tecnica", "memorando", "despacho", "oficio"},
        )
        serialized = json.dumps(examples, ensure_ascii=False)
        self.assertNotIn("60090.", serialized)
        self.assertNotIn("60093.", serialized)

    def test_human_scores_meet_predeclared_activation_rule(self) -> None:
        scores = self.benchmark["human_scores"]
        legacy_totals = [sum(score["legacy"]) for score in scores.values()]
        candidate_totals = [sum(score["candidate"]) for score in scores.values()]
        deltas = [candidate - legacy for legacy, candidate in zip(legacy_totals, candidate_totals)]
        rule = self.benchmark["evaluation"]["activation_rule"]

        self.assertGreaterEqual(
            statistics.mean(candidate_totals) - statistics.mean(legacy_totals),
            rule["minimum_mean_total_gain"],
        )
        self.assertGreaterEqual(sum(delta > 0 for delta in deltas), rule["minimum_improved_examples"])
        self.assertLessEqual(sum(delta < 0 for delta in deltas), rule["maximum_regressed_examples"])
        legacy_fidelity = statistics.mean(score["legacy"][3] for score in scores.values())
        candidate_fidelity = statistics.mean(score["candidate"][3] for score in scores.values())
        self.assertGreaterEqual(candidate_fidelity - legacy_fidelity, rule["minimum_fidelity_delta"])
        self.assertFalse(any(delta <= -3 for delta in deltas))

    def test_candidate_is_deterministic_and_uses_only_literal_sentences(self) -> None:
        for example in self.benchmark["examples"]:
            with self.subTest(example=example["id"]):
                first = _extract_resumo_candidate(example["text"])
                second = _extract_resumo_candidate(example["text"])
                self.assertEqual(first, second)
                if first:
                    for sentence in first.split(". "):
                        self.assertIn(sentence.rstrip("."), example["text"])

    def test_candidate_handles_required_edge_cases(self) -> None:
        outputs = {
            example["id"]: _extract_resumo_candidate(example["text"])
            for example in self.benchmark["examples"]
        }
        self.assertNotIn("MINISTERIO EXEMPLO", outputs["nota_header_first"])
        self.assertNotIn("Processo administrativo de referencia", outputs["nota_protocol_first"])
        self.assertIn("Conclui-se", outputs["nota_history_then_conclusion"])
        self.assertEqual(outputs["memorando_short"], "Encaminho o relatorio revisado para conhecimento.")
        self.assertEqual(outputs["empty_unavailable"], "")

    def test_legacy_baseline_remains_reproducible(self) -> None:
        outputs = {
            example["id"]: _extract_resumo_legacy(example["text"])
            for example in self.benchmark["examples"]
        }
        self.assertIn("MINISTERIO EXEMPLO", outputs["nota_header_first"])
        self.assertIn("Processo administrativo de referencia", outputs["nota_protocol_first"])
        self.assertNotIn("Conclui-se", outputs["nota_history_then_conclusion"])
        self.assertEqual(outputs["empty_unavailable"], "")

    def test_production_uses_candidate_and_preserves_other_legacy_fields(self) -> None:
        example = next(
            item for item in self.benchmark["examples"] if item["id"] == "nota_header_first"
        )
        payload = {
            "processo": "PROCESSO-EXEMPLO-RESUMO",
            "documento": "DOCUMENTO-EXEMPLO-RESUMO",
            "snapshot": {
                "title": "Nota Tecnica DOCUMENTO-EXEMPLO-RESUMO",
                "text": example["text"],
                "tables": [],
                "extraction_mode": "html",
            },
            "collection": {"found": True},
        }
        candidate = build_normalized_record(payload, Path("synthetic.json"))
        with patch(
            "app.services.documento_administrativo_normalizer._extract_resumo",
            _extract_resumo_legacy,
        ):
            legacy = build_normalized_record(payload, Path("synthetic.json"))

        self.assertEqual(candidate["resumo"], _extract_resumo_candidate(example["text"]))
        self.assertNotEqual(candidate["resumo"], legacy["resumo"])
        self.assertEqual(
            {key: value for key, value in candidate.items() if key != "resumo"},
            {key: value for key, value in legacy.items() if key != "resumo"},
        )


if __name__ == "__main__":
    unittest.main()
