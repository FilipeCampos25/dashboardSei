from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.documento_administrativo_normalizer import build_administrativo_v2_record, build_normalized_record
from app.services.field_states import FieldResult, FieldState


class AdministrativoContextFieldsTests(unittest.TestCase):
    def resolve(self, text: str, title: str = "Memorando 1/2026"):
        payload = {
            "processo": "60090.000001/2026-00",
            "documento": "4455667",
            "snapshot": {"title": title, "text": text, "tables": []},
            "collection": {
                "document_id": "4455667",
                "found": True,
                "acquisition_state": {
                    "discovery": "FOUND", "opening": "OPENED",
                    "access": "ACCESSIBLE", "extraction": "EXTRACTED",
                },
            },
        }
        legacy = build_normalized_record(payload, Path("admin.json"))
        v2 = build_administrativo_v2_record(legacy, payload, source_path="admin.json")
        fields = {item["field_name"]: FieldResult.from_dict(item) for item in v2["fields"]}
        return legacy, v2, fields

    def test_explicit_subject_uses_document_line(self):
        _, _, fields = self.resolve("Assunto: Solicitação de informações\nSolicita-se resposta.")
        subject = fields["assunto"]
        self.assertEqual("Solicitação de informações", subject.value)
        self.assertEqual("administrativo.assunto.explicit_line", subject.evidences[0].rule_id)
        self.assertEqual("body", subject.evidences[0].location.section)

    def test_title_subject_is_an_auditable_fallback(self):
        legacy, _, fields = self.resolve("Conteúdo técnico sem linha de assunto.", "Nota Técnica - Análise de cooperação")
        self.assertEqual("Nota Técnica - Análise de cooperação", legacy["assunto"])
        subject = fields["assunto"]
        self.assertEqual(legacy["assunto"], subject.value)
        self.assertEqual("administrativo.assunto.title_fallback", subject.evidences[0].rule_id)
        self.assertEqual("title", subject.evidences[0].location.section)

    def test_current_action_is_present_with_context(self):
        _, _, fields = self.resolve("Encaminhamento\nSolicita-se à unidade o envio da documentação.")
        action = fields["acao_solicitada"]
        self.assertIs(FieldState.PRESENT, action.state)
        self.assertIn("Solicita-se", action.value)
        self.assertEqual("current_request", action.evidences[0].location.section)

    def test_historical_action_is_not_promoted(self):
        legacy, _, fields = self.resolve("Em 2020, solicitamos o envio dos documentos. Atualmente, o processo encontra-se em análise.")
        self.assertTrue(legacy["acao_solicitada"])
        self.assertIs(FieldState.UNRESOLVED, fields["acao_solicitada"].state)
        self.assertIsNone(fields["acao_solicitada"].value)
        self.assertEqual("administrativo.acao.historical_context", fields["acao_solicitada"].evidences[0].rule_id)

    def test_deadline_linked_to_current_action_is_present(self):
        _, _, fields = self.resolve("Encaminhar a resposta no prazo de 10 dias.")
        self.assertIs(FieldState.PRESENT, fields["acao_solicitada"].state)
        self.assertIs(FieldState.PRESENT, fields["prazo"].state)
        self.assertIn("prazo de 10 dias", fields["prazo"].value)
        self.assertEqual("current_action_context", fields["prazo"].evidences[0].location.section)

    def test_historical_deadline_is_not_promoted(self):
        legacy, _, fields = self.resolve("Foi concedido prazo de 10 dias no procedimento anterior.")
        self.assertTrue(legacy["prazo"])
        self.assertIs(FieldState.UNRESOLVED, fields["prazo"].state)
        self.assertIsNone(fields["prazo"].value)

    def test_action_and_deadline_from_different_contexts_are_not_combined(self):
        _, _, fields = self.resolve("Em 2019 foi concedido prazo de 5 dias no procedimento anterior.\nSolicita-se nova análise.")
        self.assertIs(FieldState.PRESENT, fields["acao_solicitada"].state)
        self.assertIs(FieldState.UNRESOLVED, fields["prazo"].state)

    def test_multiple_current_requests_abstain(self):
        _, _, fields = self.resolve("Solicita-se o envio do relatório.\nEncaminhar também a planilha revisada.")
        self.assertIs(FieldState.UNRESOLVED, fields["acao_solicitada"].state)
        self.assertIsNone(fields["acao_solicitada"].value)
        self.assertEqual(2, len(fields["acao_solicitada"].evidences))

    def test_title_only_remains_non_gold(self):
        _, v2, _ = self.resolve("", "Ofício 7/2024")
        self.assertEqual("BLOCKED", v2["document_gold_decision"]["semantic_state"]["publication"])


if __name__ == "__main__":
    unittest.main()
