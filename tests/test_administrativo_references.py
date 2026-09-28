from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.documento_administrativo_normalizer import (
    _extract_documentos_mencionados,
    build_administrativo_v2_record,
    build_normalized_record,
)


class AdministrativoMentionedReferencesTests(unittest.TestCase):
    def _resolve(self, text: str):
        payload = {
            "processo": "60090.000001/2026-00",
            "documento": "4455667",
            "snapshot": {"title": "Memorando 1/2026", "text": text, "tables": []},
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
        return legacy, build_administrativo_v2_record(legacy, payload, source_path="admin.json")

    def test_generic_and_non_sei_numbers_are_rejected(self):
        cases = {
            "cpf": "CPF 123.456.789-00.",
            "cnpj": "CNPJ 12.345.678/0001-90.",
            "monetary": "Valor R$ 1.234.567,89.",
            "generic": "Identificador 123456789.",
            "year_sequence": "Exercício 2024, sequência 123456.",
            "normative": "Portaria 123456/2024.",
        }
        for name, text in cases.items():
            with self.subTest(case=name):
                legacy, v2 = self._resolve(text)
                self.assertEqual("", legacy["documentos_mencionados"])
                self.assertEqual([], v2["mentioned_references"])

    def test_explicit_document_and_process_are_typed_and_deduplicated(self):
        text = (
            "Documento SEI nº 7788990 e documento SEI 7788990. "
            "Processo nº 60090.000002/2020-00."
        )
        legacy, v2 = self._resolve(text)
        self.assertEqual("60090.000002/2020-00 | 7788990", legacy["documentos_mencionados"])
        references = v2["mentioned_references"]
        self.assertEqual(["process", "document"], [item["kind"] for item in references])
        self.assertEqual("60090.000002/2020-00", references[0]["identity"]["process_id"])
        self.assertIsNone(references[0]["identity"]["document_id"])
        self.assertEqual("7788990", references[1]["identity"]["document_id"])
        self.assertEqual("", references[1]["identity"]["process_id"])
        self.assertTrue(all(item["evidence"]["raw_evidence"] for item in references))

    def test_url_document_and_candidate_parameters_keep_distinct_semantics(self):
        _, v2 = self._resolve(
            "Consulte https://sei.example/controlador.php?id_documento=7788990 "
            "e https://sei.example/controlador.php?id_anexo=1139528."
        )
        references = v2["mentioned_references"]
        self.assertEqual(["document", "unresolved"], [item["kind"] for item in references])
        self.assertEqual("7788990", references[0]["identity"]["document_id"])
        self.assertEqual("1139528", references[1]["identity"]["candidate_id"])

    def test_deduplication_does_not_collapse_distinct_types(self):
        _, v2 = self._resolve(
            "Documento SEI 7788990 e https://sei.example/controlador.php?id_anexo=7788990."
        )
        self.assertEqual({"document", "unresolved"}, {item["kind"] for item in v2["mentioned_references"]})

    def test_ambiguous_number_does_not_fabricate_a_type(self):
        legacy, v2 = self._resolve("Referência 99887766 sem marcador SEI.")
        self.assertEqual("", legacy["documentos_mencionados"])
        self.assertEqual([], v2["mentioned_references"])

    def test_current_identity_is_not_emitted_as_a_mention(self):
        legacy, v2 = self._resolve(
            "Processo 60090.000001/2026-00, documento SEI 4455667."
        )
        self.assertEqual("", legacy["documentos_mencionados"])
        self.assertEqual([], v2["mentioned_references"])

    def test_legacy_extractor_rejects_an_isolated_long_number(self):
        self.assertEqual("", _extract_documentos_mencionados("Código 123456789", "", ""))


if __name__ == "__main__":
    unittest.main()
