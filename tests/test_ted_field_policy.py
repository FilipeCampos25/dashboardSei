from __future__ import annotations

import sys
import json
import shutil
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldState
from app.services.ted_classifier import (
    TED_FUNCTION_AMENDMENT,
    TED_FUNCTION_EXTRACT,
    TED_FUNCTION_INSTRUMENT,
    TED_FUNCTION_MEETING_MINUTES,
    TED_FUNCTION_RELATED,
    TED_FUNCTION_WORK_PLAN,
)
from app.services.ted_field_policy import (
    RequirementPolicy,
    field_policy_for_function,
    field_state_for_policy,
    may_complement_instrument,
)
from app.output import csv_writer
from app.services.normalization_review import _ted_issues, collect_review_issues
from app.services.ted_normalizer import build_normalized_record, build_ted_v2_record


class TEDFieldPolicyTests(unittest.TestCase):
    def _v2(self, text: str) -> dict:
        payload = {"processo": "test-only:policy", "snapshot": {"title": "TED", "text": text, "tables": []}}
        row, diagnostics = build_normalized_record(payload, "policy.json")
        return build_ted_v2_record(row, payload, diagnostics)

    def test_instrument_keeps_core_required_and_expects_plan_sections_elsewhere(self) -> None:
        self.assertIs(
            RequirementPolicy.REQUIRED,
            field_policy_for_function(TED_FUNCTION_INSTRUMENT, "objeto"),
        )
        self.assertIs(
            RequirementPolicy.EXPECTED_ELSEWHERE,
            field_policy_for_function(TED_FUNCTION_INSTRUMENT, "metas"),
        )
        self.assertIs(
            RequirementPolicy.OPTIONAL,
            field_policy_for_function(TED_FUNCTION_INSTRUMENT, "data_assinatura"),
        )

    def test_work_plan_requires_only_sections_supported_as_its_own_content(self) -> None:
        self.assertIs(
            RequirementPolicy.REQUIRED,
            field_policy_for_function(TED_FUNCTION_WORK_PLAN, "metas"),
        )
        self.assertIs(
            RequirementPolicy.EXPECTED_ELSEWHERE,
            field_policy_for_function(TED_FUNCTION_WORK_PLAN, "valor_global"),
        )

    def test_non_instrument_profiles_do_not_default_to_instrument(self) -> None:
        self.assertIs(
            RequirementPolicy.NOT_APPLICABLE,
            field_policy_for_function(TED_FUNCTION_MEETING_MINUTES, "objeto"),
        )
        for function in (TED_FUNCTION_AMENDMENT, TED_FUNCTION_EXTRACT, TED_FUNCTION_RELATED, None):
            with self.subTest(function=function):
                self.assertIs(
                    RequirementPolicy.NOT_EVALUATED,
                    field_policy_for_function(function, "objeto"),
                )

    def test_only_work_plan_sections_are_authorized_for_automatic_consolidation(self) -> None:
        for field_name in ("plano_aplicacao", "cronograma_desembolso", "metas"):
            self.assertTrue(may_complement_instrument(TED_FUNCTION_WORK_PLAN, field_name))
        for function in (TED_FUNCTION_AMENDMENT, TED_FUNCTION_EXTRACT, TED_FUNCTION_RELATED, TED_FUNCTION_MEETING_MINUTES):
            self.assertFalse(may_complement_instrument(function, "metas"))
        self.assertFalse(may_complement_instrument(TED_FUNCTION_WORK_PLAN, "objeto"))

    def test_policy_maps_to_existing_field_states_without_fabricating_value(self) -> None:
        cases = (
            (RequirementPolicy.REQUIRED, False, FieldState.ABSENT),
            (RequirementPolicy.EXPECTED_ELSEWHERE, False, FieldState.EXPECTED_ELSEWHERE),
            (RequirementPolicy.NOT_APPLICABLE, False, FieldState.NOT_APPLICABLE),
            (RequirementPolicy.NOT_EVALUATED, False, FieldState.NOT_EVALUATED),
            (RequirementPolicy.OPTIONAL, False, FieldState.NOT_EVALUATED),
            (RequirementPolicy.NOT_APPLICABLE, True, FieldState.PRESENT),
        )
        for policy, present, expected in cases:
            with self.subTest(policy=policy, present=present):
                self.assertIs(expected, field_state_for_policy(policy, value_present=present))

    def test_v2_states_cover_each_audited_function_without_changing_publication_contract(self) -> None:
        cases = (
            ("TERMO DE EXECUCAO DESCENTRALIZADA. OBJETO: entrega. UNIDADE DESCENTRALIZADORA: A.", "ted.instrument", "objeto", "ABSENT"),
            ("PLANO DE TRABALHO do TED 1/2026.", "ted.work_plan", "valor_global", "EXPECTED_ELSEWHERE"),
            ("ATA DE REUNIAO sobre o TED 1/2026.", "ted.meeting_minutes", "objeto", "NOT_APPLICABLE"),
            ("TERMO ADITIVO ao TED 1/2026.", "ted.amendment", "objeto", "NOT_EVALUATED"),
            ("EXTRATO do TED 1/2026.", "ted.extract", "objeto", "NOT_EVALUATED"),
            ("OFICIO referente ao TED 1/2026.", "ted.related", "objeto", "NOT_EVALUATED"),
            ("Documento que cita TED 1/2026.", None, "objeto", "NOT_EVALUATED"),
        )
        for text, function, field_name, expected_state in cases:
            with self.subTest(function=function):
                v2 = self._v2(text)
                fields = {item["field_name"]: item for item in v2["fields"]}
                self.assertEqual(function, v2["semantic_state"]["resolved_function"])
                self.assertEqual(expected_state, fields[field_name]["state"])
                if expected_state != "PRESENT":
                    self.assertIsNone(fields[field_name]["value"])
                    self.assertEqual([], fields[field_name]["evidences"])

    def test_review_only_reports_absent_required_fields(self) -> None:
        base = {"processo": "P", "documento": "D", "publication_status": "retained_silver"}
        meeting = self._v2("ATA DE REUNIAO sobre o TED 1/2026.")
        meeting_issues = _ted_issues([{**base, "_field_results_v2": meeting["fields"]}], [])
        self.assertFalse(any(issue["code"] == "required_field_missing" for issue in meeting_issues))
        self.assertFalse(any(issue["code"] == "ted_missing_financial_value" for issue in meeting_issues))
        self.assertFalse(any(issue["code"] == "ted_without_application_plan" for issue in meeting_issues))

        instrument = self._v2("TERMO DE EXECUCAO DESCENTRALIZADA. OBJETO: entrega. UNIDADE DESCENTRALIZADORA: A.")
        instrument_issues = _ted_issues([{**base, "_field_results_v2": instrument["fields"]}], [])
        missing = {(issue["code"], issue["field"]) for issue in instrument_issues}
        self.assertIn(("required_field_missing", "valor_global"), missing)
        self.assertNotIn(("required_field_missing", "plano_aplicacao"), missing)
        self.assertNotIn(("ted_without_application_plan", "plano_aplicacao"), missing)

    def test_review_queue_consumes_v2_sidecar_without_changing_legacy_csv(self) -> None:
        root = Path(__file__).resolve().parent / "_tmp_ted_field_policy"
        shutil.rmtree(root, ignore_errors=True)
        (root / "v2").mkdir(parents=True)
        try:
            row = {"processo": "P", "documento": "Ata", "publication_status": "retained_silver", "json_path": "ata.json"}
            csv_writer.write_csv([row], root / "ted_normalizado_latest.csv")
            legacy_bytes = (root / "ted_normalizado_latest.csv").read_bytes()
            meeting = self._v2("ATA DE REUNIAO sobre o TED 1/2026.")
            meeting["identity"]["process_id"] = "P"
            meeting["legacy_json_name"] = "ata.json"
            (root / "v2" / "ted_normalizado_latest.v2.json").write_text(
                json.dumps({"records": [meeting]}), encoding="utf-8"
            )

            issues = collect_review_issues(root)

            self.assertFalse(any(issue["code"] in {"required_field_missing", "ted_missing_financial_value", "ted_without_application_plan"} for issue in issues))
            self.assertEqual(legacy_bytes, (root / "ted_normalizado_latest.csv").read_bytes())
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
