from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.documento_administrativo_normalizer import build_administrativo_v2_record, build_normalized_record
from app.services.field_states import FieldResult, FieldState
from app.services.provenance_validation import validate_field_provenance


class AdministrativoDateTests(unittest.TestCase):
    def resolve(self, text, title="Nota Técnica 1/2020"):
        payload = {
            "processo": "60090.000001/2020-00",
            "snapshot": {"title": title, "text": text, "tables": []},
            "collection": {"document_id": "1234567", "captured_at": "2026-09-18",
                           "acquisition_state": {"discovery": "FOUND", "opening": "OPENED",
                                                 "access": "ACCESSIBLE", "extraction": "EXTRACTED"}},
        }
        legacy = build_normalized_record(payload, Path("admin.json"))
        v2 = build_administrativo_v2_record(legacy, payload, source_path="admin.json")
        fields = {f["field_name"]: FieldResult.from_dict(f) for f in v2["fields"]}
        return legacy, v2, fields

    def test_old_law_first_cannot_win_by_position(self):
        text = "Lei nº 1, de 15 de março de 1989.\nBrasília, 20 de agosto de 2020."
        legacy, _, fields = self.resolve(text)
        self.assertEqual("15 de março de 1989", legacy["data"])
        result = fields["data"]
        self.assertEqual("2020-08-20", result.value)
        self.assertIs(FieldState.PRESENT, result.state)
        evidence = result.evidences[0]
        self.assertEqual("20 de agosto de 2020", evidence.raw_evidence)
        self.assertEqual(text.index("20 de agosto"), evidence.location.position)
        self.assertEqual("admin.json", evidence.location.source_path)
        self.assertEqual("1234567", evidence.source_document.document_id)
        self.assertTrue(validate_field_provenance(result).is_valid)

    def test_historical_portaria_before_document_date(self):
        _, _, fields = self.resolve("Portaria nº 1, de 12/04/2011.\nPorto Velho, 03/11/2021.")
        self.assertEqual("2021-11-03", fields["data"].value)

    def test_explicit_header_across_existing_classes(self):
        for title in ("Nota Técnica", "Memorando", "Despacho", "Ofício"):
            with self.subTest(title=title):
                _, _, fields = self.resolve("Brasília, 3 de novembro de 2020.\nEncaminho para ciência.", title)
                self.assertEqual("2020-11-03", fields["data"].value)

    def test_signature_is_kept_separate(self):
        legacy, _, fields = self.resolve("Data: 03/11/2020\nDocumento assinado eletronicamente em 04/11/2020.")
        self.assertEqual("2020-11-03", fields["data"].value)
        self.assertEqual("2020-11-04", fields["data_assinatura"].value)
        self.assertEqual(legacy["data_assinatura"], fields["data_assinatura"].value)

    def test_equally_plausible_dates_conflict_without_winner(self):
        for text in ("Data: 03/11/2020\nBrasília, 04/11/2020.",
                     "Brasília, 04/11/2020.\nData: 03/11/2020"):
            _, _, fields = self.resolve(text)
            self.assertIs(FieldState.CONFLICT, fields["data"].state)
            self.assertIsNone(fields["data"].value)
            self.assertEqual(2, len(fields["data"].evidences))

    def test_no_date_does_not_use_process_capture_filename_or_title(self):
        _, _, fields = self.resolve("Encaminho para ciência.", "Memorando de 03/11/2020")
        self.assertIs(FieldState.NOT_EVALUATED, fields["data"].state)
        self.assertIsNone(fields["data"].value)
        self.assertEqual((), fields["data"].evidences)

    def test_empty_and_title_only_remain_non_gold(self):
        for text in ("", " \n\t"):
            _, v2, fields = self.resolve(text, "Ofício de 03/11/2020")
            self.assertEqual("BLOCKED", v2["document_gold_decision"]["semantic_state"]["publication"])
            self.assertIsNone(fields["data"].value)

    def test_cited_only_or_signature_only_abstains(self):
        for text in ("Lei nº 1, de 15/03/1989.", "Portaria nº 1, de 12/04/2011.",
                     "Documento assinado eletronicamente em 04/11/2020.",
                     "Prazo até 03/11/2020.", "Histórico: documento de 03/11/2020."):
            with self.subTest(text=text):
                _, _, fields = self.resolve(text)
                self.assertIs(FieldState.UNRESOLVED, fields["data"].state)
                self.assertIsNone(fields["data"].value)
                self.assertTrue(fields["data"].evidences)

    def test_uncontextualized_dates_abstain(self):
        _, _, fields = self.resolve("03/11/2020\n04/11/2020")
        self.assertIs(FieldState.UNRESOLVED, fields["data"].state)
        self.assertEqual(2, len(fields["data"].evidences))

    def test_repeated_same_date_different_formats_is_not_conflict(self):
        _, _, fields = self.resolve("Data: 03/11/2020\nBrasília, 3 de novembro de 2020.")
        self.assertEqual("2020-11-03", fields["data"].value)
        self.assertEqual(2, len(fields["data"].evidences))

    def test_invalid_calendar_date_is_not_present(self):
        _, _, fields = self.resolve("Data: 31/02/2020")
        self.assertIs(FieldState.UNRESOLVED, fields["data"].state)
        self.assertIsNone(fields["data"].value)

    def test_inline_or_quoted_city_date_is_not_document_header(self):
        for text in ('O ofício citado diz: Brasília, 03/11/2020.', '> Brasília, 03/11/2020.'):
            _, _, fields = self.resolve(text)
            self.assertIs(FieldState.UNRESOLVED, fields["data"].state)


if __name__ == "__main__":
    unittest.main()
