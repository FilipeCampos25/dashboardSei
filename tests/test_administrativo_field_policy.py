from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.administrativo_field_policy import (
    ADMINISTRATIVE_FIELD_PROFILES,
    AdministrativeFieldPolicy,
)
from app.services.documento_administrativo_normalizer import build_administrativo_v2_record, build_normalized_record
from app.services.field_states import FieldResult, FieldState
from app.services.normalization_review import _required_field_issues
from app.services.pipeline_states import AccessState, DiscoveryState, ExtractionState, OpeningState


class AdministrativoFieldPolicyTests(unittest.TestCase):
    def test_supported_classes_have_complete_explicit_profiles(self) -> None:
        self.assertEqual({"nota_tecnica", "memorando", "despacho", "oficio"}, set(ADMINISTRATIVE_FIELD_PROFILES))
        expected = set(next(iter(ADMINISTRATIVE_FIELD_PROFILES.values())))
        for profile in ADMINISTRATIVE_FIELD_PROFILES.values():
            self.assertEqual(expected, set(profile))
        self.assertIs(AdministrativeFieldPolicy.NOT_APPLICABLE, ADMINISTRATIVE_FIELD_PROFILES["nota_tecnica"]["origem"])
        self.assertIs(AdministrativeFieldPolicy.EXPECTED_ELSEWHERE, ADMINISTRATIVE_FIELD_PROFILES["memorando"]["origem"])

    def test_policy_states_and_quality_are_class_specific(self) -> None:
        for title, expected_origin in (
            ("Nota Tecnica 1/2026", FieldState.NOT_APPLICABLE),
            ("Memorando 1/2026", FieldState.EXPECTED_ELSEWHERE),
            ("Despacho 1/2026", FieldState.NOT_APPLICABLE),
            ("Oficio 1/2026", FieldState.EXPECTED_ELSEWHERE),
        ):
            with self.subTest(title=title):
                v2 = self._v2(title)
                fields = {item["field_name"]: FieldResult.from_dict(item) for item in v2["fields"]}
                self.assertIs(expected_origin, fields["origem"].state)
                self.assertEqual("high", v2["quality_status_v2"])

    def test_required_absence_degrades_quality_without_fabricating_evidence(self) -> None:
        record = build_normalized_record(self._payload("Memorando 1/2026"), Path("admin.json"))
        record["funcao_administrativa"] = ""
        v2 = build_administrativo_v2_record(record, self._payload("Memorando 1/2026"))
        field = next(item for item in v2["fields"] if item["field_name"] == "funcao_administrativa")
        self.assertEqual("ABSENT", field["state"])
        self.assertEqual([], field["evidences"])
        self.assertEqual("medium", v2["quality_status_v2"])

    def test_extraction_failure_is_technical_not_semantic_absence(self) -> None:
        payload = self._payload("Oficio 1/2026", extraction=ExtractionState.EXTRACTION_FAILED)
        record = build_normalized_record(payload, Path("admin.json"))
        v2 = build_administrativo_v2_record(record, payload)
        field = next(item for item in v2["fields"] if item["field_name"] == "origem")
        self.assertEqual("EXTRACTION_FAILED", field["state"])
        self.assertEqual("low", v2["quality_status_v2"])

    def test_present_provenance_is_unchanged_by_policy(self) -> None:
        payload = self._payload("Memorando 1/2026", text="De: Unidade A\nMemorando 1/2026\nEncaminho para ciencia.")
        v2 = build_administrativo_v2_record(build_normalized_record(payload, Path("admin.json")), payload)
        field = next(item for item in v2["fields"] if item["field_name"] == "origem")
        self.assertEqual("PRESENT", field["state"])
        self.assertEqual("document", field["evidences"][0]["source_kind"])
        self.assertEqual("administrativo.line_value.origem", field["evidences"][0]["rule_id"])

    def test_review_uses_v2_absent_states_instead_of_global_missing_fields(self) -> None:
        row = {
            "publication_status": "published_gold",
            "assunto": "",
            "resumo": "",
            "_field_results_v2": [
                {"field_name": "origem", "state": "EXPECTED_ELSEWHERE"},
                {"field_name": "assunto", "state": "NOT_EVALUATED"},
                {"field_name": "funcao_administrativa", "state": "ABSENT"},
            ],
        }
        issues = _required_field_issues("documento_administrativo", row)
        self.assertEqual(["funcao_administrativa"], [item["field"] for item in issues])

    def _v2(self, title: str) -> dict:
        payload = self._payload(title)
        return build_administrativo_v2_record(build_normalized_record(payload, Path("admin.json")), payload)

    @staticmethod
    def _payload(title: str, *, text: str | None = None, extraction: ExtractionState = ExtractionState.EXTRACTED) -> dict:
        return {
            "processo": "60090.000001/2020-00",
            "documento": "4455667",
            "snapshot": {"title": title, "text": text if text is not None else f"{title}\nEncaminho para ciencia.", "tables": []},
            "collection": {"found": True, "acquisition_state": {
                "discovery": DiscoveryState.FOUND.value,
                "opening": OpeningState.OPENED.value,
                "access": AccessState.ACCESSIBLE.value,
                "extraction": extraction.value,
            }},
        }


if __name__ == "__main__":
    unittest.main()
