from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.act_normalizer import build_normalized_record, classify_act_snapshot
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
        self.assertFalse(result["shadow_only"])
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

    def test_host_origin_and_mentions_are_independent(self) -> None:
        host = "60090.000001/2026-00"
        origin = "60090.000002/2026-00"
        mentioned = "60090.000003/2026-00"
        result = assess_act_process_affinity(
            {
                "title": "Acordo de Cooperacao Tecnica",
                "text": (
                    f"PROCESSO MD {origin}. ACORDO DE COOPERACAO TECNICA. "
                    "CLAUSULA PRIMEIRA - DO OBJETO. "
                    f"Como antecedente, consulte-se o processo {mentioned}."
                ),
            },
            current_process=host,
            collection={"related_to_current_process": True},
        )

        self.assertEqual(result["host_process"], host)
        self.assertEqual(result["origin_process"]["process"], origin)
        self.assertEqual(
            [item["process"] for item in result["mentioned_processes"]],
            [mentioned],
        )
        self.assertEqual(result["affinity_status"], "related_document")
        self.assertFalse(result["canonical_eligible"])
        self.assertEqual(result["reason_code"], "act.affinity.related")

    def test_body_mention_is_not_origin_evidence(self) -> None:
        mentioned = "60090.000003/2026-00"
        result = assess_act_process_affinity(
            {
                "title": "Acordo de Cooperacao Tecnica",
                "text": (
                    "ACORDO DE COOPERACAO TECNICA. CLAUSULA PRIMEIRA - DO OBJETO. "
                    f"O processo {mentioned} e citado apenas como antecedente."
                ),
            },
            current_process="60090.000001/2026-00",
            collection={},
        )

        self.assertEqual(result["origin_process"]["process"], "")
        self.assertEqual(result["mentioned_processes"][0]["process"], mentioned)
        self.assertEqual(result["affinity_status"], "unknown")
        self.assertFalse(result["canonical_eligible"])

    def test_multiple_body_mentions_do_not_choose_an_origin(self) -> None:
        mentioned = ("60090.000003/2026-00", "60090.000004/2026-00")
        result = assess_act_process_affinity(
            {
                "title": "Acordo de Cooperacao Tecnica",
                "text": (
                    "ACORDO DE COOPERACAO TECNICA. CLAUSULA PRIMEIRA - DO OBJETO. "
                    f"Antecedentes nos processos {mentioned[0]} e {mentioned[1]}."
                ),
            },
            current_process="60090.000001/2026-00",
            collection={},
        )

        self.assertEqual(result["origin_process"]["process"], "")
        self.assertEqual(
            {item["process"] for item in result["mentioned_processes"]}, set(mentioned)
        )
        self.assertEqual(result["affinity_status"], "unknown")

    def test_conflicting_origin_evidence_is_ambiguous(self) -> None:
        result = assess_act_process_affinity(
            {
                "title": "ACT",
                "text": "PROCESSO MD 60090.000002/2026-00. ACORDO DE COOPERACAO TECNICA.",
            },
            current_process="60090.000001/2026-00",
            collection={"processo_origem": "60090.000004/2026-00"},
        )

        self.assertEqual(result["affinity_status"], "ambiguous")
        self.assertFalse(result["canonical_eligible"])
        self.assertEqual(result["reason_code"], "act.affinity.ambiguous")

    def test_affinity_controls_act_candidate_eligibility(self) -> None:
        host = "60090.000001/2026-00"
        base_text = (
            "ACORDO DE COOPERACAO TECNICA QUE ENTRE SI CELEBRAM A UNIAO, "
            "REPRESENTADA PELO CENSIPAM, E A PARTE SINTETICA. "
            "CLAUSULA PRIMEIRA - DO OBJETO. Executar atividade conjunta."
        )
        same = classify_act_snapshot(
            {"title": "ACT", "text": f"PROCESSO No {host}. {base_text}"},
            processo=host,
        )
        external = classify_act_snapshot(
            {"title": "ACT", "text": f"PROCESSO MD 60090.000002/2026-00. {base_text}"},
            collection_context={"related_to_current_process": True},
            processo=host,
        )
        unknown = classify_act_snapshot(
            {"title": "ACT", "text": base_text},
            processo=host,
        )

        self.assertTrue(same["is_canonical_candidate"])
        self.assertEqual(same["publication_status"], "published_gold")
        for result in (external, unknown):
            self.assertFalse(result["is_canonical_candidate"])
            self.assertEqual(result["publication_status"], "retained_silver")
            self.assertTrue(result["affinity_reason_code"].startswith("act.affinity."))


if __name__ == "__main__":
    unittest.main()
