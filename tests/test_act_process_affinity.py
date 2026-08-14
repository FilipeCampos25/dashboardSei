from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.act_normalizer import build_normalized_record
from app.services.act_process_affinity import AFFINITY_RULE_VERSION, assess_act_process_affinity
from tests.fixture_loader import load_fixture


class ActProcessAffinityTests(unittest.TestCase):
    def _fixture_result(self, fixture_name: str) -> tuple[dict, dict]:
        payload = load_fixture(fixture_name)["payload"]
        result = assess_act_process_affinity(
            payload["snapshot"],
            current_process=payload["processo"],
            collection=payload.get("collection", {}),
        )
        return payload, result

    def test_target_mb_act_is_related_with_probable_external_origin(self) -> None:
        payload, result = self._fixture_result("act_affinity_related.json")

        self.assertFalse(result["current_process_explicit"]["found"])
        self.assertEqual(result["affinity_status"], "related_document")
        self.assertEqual(result["affinity_confidence"], "medium")
        self.assertEqual(result["document_origin_process"]["process"], "61074.007769/2025-46")
        self.assertEqual(result["document_origin_process"]["source"], "header.institutional_process_header")
        self.assertTrue(result["shadow_only"])
        self.assertEqual(result["affinity_rule_version"], AFFINITY_RULE_VERSION)
        self.assertEqual(payload["processo"], "60090.001292/2025-24")

    def test_target_inpe_act_uses_reference_footer_as_origin(self) -> None:
        text = (
            "PROCESSO INPE 01340.003873/2025-42. ACORDO DE COOPERACAO TECNICA. "
            "CLAUSULA PRIMEIRA - DO OBJETO. " + ("conteudo sintetico " * 80)
            + "Autenticidade: Referencia: Processo 01340.003873/2025-42 SEI n 1. "
            "Processo relacionado 01340.009269/2023-68."
        )
        result = assess_act_process_affinity(
            {"title": "ACT", "text": text},
            current_process="60090.000702/2025-10",
            collection={"related_to_current_process": True},
        )

        self.assertFalse(result["current_process_explicit"]["found"])
        self.assertEqual(result["affinity_status"], "related_document")
        self.assertEqual(result["affinity_confidence"], "high")
        self.assertEqual(result["document_origin_process"]["process"], "01340.003873/2025-42")
        external = {item["process"]: item for item in result["external_processes_found"]}
        self.assertEqual(external["01340.003873/2025-42"]["role"], "origin")
        self.assertIn("01340.009269/2023-68", external)
        zones = {
            occurrence["zone"]
            for occurrence in external["01340.003873/2025-42"]["occurrences"]
        }
        self.assertIn("authentication_footer", zones)

    def test_multi_process_gold_baselines_remain_strong_matches(self) -> None:
        for current_process, external_process in (
            ("08650.063489/2021-11", "60090.000001/2021-00"),
            ("60090.000269/2020-16", "08650.000001/2020-00"),
        ):
            result = assess_act_process_affinity(
                {"title": "ACT", "text": f"Processo {current_process}. Processo relacionado {external_process}. CLAUSULA PRIMEIRA - DO OBJETO."},
                current_process=current_process,
                collection={},
            )
            with self.subTest(processo=current_process):
                self.assertTrue(result["current_process_explicit"]["found"])
                self.assertTrue(result["external_processes_found"])
                self.assertEqual(result["affinity_status"], "strong_match")
                self.assertEqual(result["affinity_confidence"], "high")

    def test_body_citation_of_current_process_does_not_promote(self) -> None:
        snapshot = {
            "title": "Acordo de Cooperacao Tecnica",
            "text": (
                "ACORDO DE COOPERACAO TECNICA QUE ENTRE SI CELEBRAM AS PARTES. "
                "CLAUSULA PRIMEIRA - DO OBJETO. O objeto e apoiar atividades. "
                "Como antecedente, consulte-se o processo 60090.000001/2026-00."
            ),
        }
        result = assess_act_process_affinity(
            snapshot,
            current_process="60090.000001/2026-00",
            collection={"found_in": "filter"},
        )
        self.assertEqual(result["affinity_status"], "ambiguous")
        self.assertEqual(result["current_process_explicit"]["occurrences"][0]["zone"], "body")

    def test_external_footer_without_current_link_is_probable_external(self) -> None:
        text = (
            "ACORDO DE COOPERACAO TECNICA QUE ENTRE SI CELEBRAM AS PARTES.\n"
            "CLAUSULA PRIMEIRA - DO OBJETO. Execucao de atividades conjuntas.\n"
            + ("conteudo contratual " * 80)
            + "A autenticidade pode ser conferida no SEI externo. "
            "Referencia: Processo n 01340.000001/2026-00 SEI n 1234."
        )
        result = assess_act_process_affinity(
            {"title": "ACT externo", "text": text, "url": "https://sei.exemplo/documento"},
            current_process="60090.000001/2026-00",
            collection={},
        )
        self.assertEqual(result["affinity_status"], "probable_external_document")
        self.assertEqual(result["document_origin_process"]["process"], "01340.000001/2026-00")

    def test_independent_origin_metadata_can_confirm_current_process(self) -> None:
        result = assess_act_process_affinity(
            {"title": "ACT", "text": "ACORDO DE COOPERACAO TECNICA QUE ENTRE SI CELEBRAM AS PARTES."},
            current_process="60090.000001/2026-00",
            collection={"processo_origem": "60090.000001/2026-00"},
        )
        self.assertEqual(result["affinity_status"], "strong_match")
        self.assertEqual(result["document_origin_process"]["source"], "metadata.processo_origem")

    def test_shadow_fields_do_not_promote_related_document_publication(self) -> None:
        payload = load_fixture("act_affinity_related.json")["payload"]
        record = build_normalized_record(payload, Path("act_affinity_related.json"))

        self.assertEqual(record["publication_status"], "retained_silver")
        self.assertEqual(record["affinity_status"], "related_document")
        self.assertEqual(record["affinity_rule_version"], AFFINITY_RULE_VERSION)
        self.assertIn("current_metadata_link=true", record["affinity_evidence"])
        self.assertIsInstance(record["canonical_score"], int)


if __name__ == "__main__":
    unittest.main()
