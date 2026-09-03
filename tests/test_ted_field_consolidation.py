from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.ted_field_consolidation import consolidate_ted_fields


class TEDFieldConsolidationTests(unittest.TestCase):
    def test_only_instrument_preserves_value(self) -> None:
        result = consolidate_ted_fields([self.record("I", "ted.instrument", "SELECTED", "metas", "A")])
        self.assertEqual("A", self.field(result[0], "metas")["value"])

    def test_authorized_work_plan_resolves_expected_elsewhere_with_source_identity(self) -> None:
        result = consolidate_ted_fields([
            self.record("I", "ted.instrument", "SELECTED", "metas", None, "EXPECTED_ELSEWHERE"),
            self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A"),
        ])
        field = self.field(result[0], "metas")
        self.assertEqual(("PRESENT", "A"), (field["state"], field["value"]))
        self.assertEqual("D-P", field["evidences"][0]["source_document"]["document_id"])
        self.assertEqual("document", field["evidences"][0]["source_kind"])
        self.assertEqual("ted.same_process_complement:ted.work_plan", field["evidences"][0]["relation"])
        self.assertEqual("ted.extract", field["evidences"][0]["rule_id"])
        self.assertEqual("ted.field_consolidation.authorized_complement", result[0]["ted_field_consolidation"]["metas"]["rule_id"])
        self.assertEqual("I", result[0]["identity"]["candidate_id"])
        self.assertEqual("SELECTED", result[0]["semantic_state"]["canonical"])
        self.assertEqual("INELIGIBLE", result[1]["semantic_state"]["canonical"])

    def test_unauthorized_related_document_does_not_fill_field(self) -> None:
        result = consolidate_ted_fields([
            self.record("I", "ted.instrument", "SELECTED", "metas", None, "EXPECTED_ELSEWHERE"),
            self.record("R", "ted.related", "INELIGIBLE", "metas", "A"),
        ])
        self.assertEqual("EXPECTED_ELSEWHERE", self.field(result[0], "metas")["state"])

    def test_equal_sources_accumulate_evidence_without_conflict(self) -> None:
        result = consolidate_ted_fields([
            self.record("I", "ted.instrument", "SELECTED", "metas", "A"),
            self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A"),
        ])
        field = self.field(result[0], "metas")
        self.assertEqual(("PRESENT", "A", 2), (field["state"], field["value"], len(field["evidences"])))

    def test_divergent_instrument_and_complement_are_conflict(self) -> None:
        result = consolidate_ted_fields([
            self.record("I", "ted.instrument", "SELECTED", "metas", "A"),
            self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "B"),
        ])
        field = self.field(result[0], "metas")
        self.assertEqual(("CONFLICT", None, 2), (field["state"], field["value"], len(field["evidences"])))
        self.assertEqual(["A", "B"], result[0]["ted_field_consolidation"]["metas"]["values"])

    def test_complete_complement_and_zero_winner_never_change_canonicity(self) -> None:
        instrument = self.record("I", "ted.instrument", "SELECTED", "metas", None, "EXPECTED_ELSEWHERE")
        complement = self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A")
        complement["fields"].append(self.field_value("objeto", "extra"))
        selected = consolidate_ted_fields([instrument, complement])
        self.assertEqual(["SELECTED", "INELIGIBLE"], [item["semantic_state"]["canonical"] for item in selected])

        no_winner = consolidate_ted_fields([self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A")])
        self.assertEqual("INELIGIBLE", no_winner[0]["semantic_state"]["canonical"])
        self.assertNotIn("ted_field_consolidation", no_winner[0])

    def test_two_complements_agree_or_conflict_deterministically(self) -> None:
        base = self.record("I", "ted.instrument", "SELECTED", "metas", None, "EXPECTED_ELSEWHERE")
        agree = [base, self.record("P2", "ted.work_plan", "INELIGIBLE", "metas", "A"), self.record("P1", "ted.work_plan", "INELIGIBLE", "metas", "A")]
        forward = consolidate_ted_fields(copy.deepcopy(agree))
        reverse = consolidate_ted_fields(list(reversed(copy.deepcopy(agree))))
        self.assertEqual(self.field(forward[0], "metas"), self.field(reverse[-1], "metas"))
        self.assertEqual(2, len(self.field(forward[0], "metas")["evidences"]))

        disagree = copy.deepcopy(agree)
        self.field(disagree[1], "metas")["value"] = "B"
        resolved = consolidate_ted_fields(disagree)
        self.assertEqual("CONFLICT", self.field(resolved[0], "metas")["state"])

    def test_not_applicable_is_never_filled(self) -> None:
        instrument = self.record("I", "ted.instrument", "SELECTED", "metas", None, "NOT_APPLICABLE")
        result = consolidate_ted_fields([instrument, self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A")])
        self.assertEqual("NOT_APPLICABLE", self.field(result[0], "metas")["state"])

    def test_process_match_without_distinct_document_identity_is_not_a_relation(self) -> None:
        instrument = self.record("I", "ted.instrument", "SELECTED", "metas", None, "EXPECTED_ELSEWHERE")
        complement = self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A")
        complement["identity"].update({"document_id": None, "candidate_id": None})
        complement["fields"][0]["evidences"][0]["source_document"] = dict(complement["identity"])
        result = consolidate_ted_fields([instrument, complement])
        self.assertEqual("EXPECTED_ELSEWHERE", self.field(result[0], "metas")["state"])

    def test_preexisting_conflict_is_not_converted_to_present(self) -> None:
        instrument = self.record("I", "ted.instrument", "SELECTED", "metas", None, "CONFLICT")
        result = consolidate_ted_fields([instrument, self.record("P", "ted.work_plan", "INELIGIBLE", "metas", "A")])
        self.assertEqual("CONFLICT", self.field(result[0], "metas")["state"])

    @staticmethod
    def field(record: dict, name: str) -> dict:
        return next(item for item in record["fields"] if item["field_name"] == name)

    @staticmethod
    def field_value(name: str, value: str | None, state: str = "PRESENT") -> dict:
        evidence = [] if value is None else [{
            "field_name": name,
            "source_kind": "document",
            "source_document": None,
            "relation": None,
            "rule_id": "ted.extract",
            "location": None,
            "raw_evidence": value,
            "external_reference": None,
        }]
        return {"field_name": name, "state": state, "value": value, "evidences": evidence}

    @classmethod
    def record(cls, candidate: str, function: str, canonical: str, field: str, value: str | None, state: str = "PRESENT") -> dict:
        identity = {"process_id": "PROC", "document_id": f"D-{candidate}", "candidate_id": candidate, "source_url": f"https://example/{candidate}"}
        item = cls.field_value(field, value, state)
        if item["evidences"]:
            item["evidences"][0]["source_document"] = dict(identity)
        semantic = {"classification": "CONFIRMED" if function == "ted.instrument" else "RELATED", "resolved_class": "ted", "function": "INSTRUMENT" if function == "ted.instrument" else "RELATED", "resolved_function": function, "affinity": "NOT_EVALUATED", "canonical": canonical, "publication": "PUBLISHED" if canonical == "SELECTED" else "BLOCKED"}
        return {"identity": identity, "semantic_state": semantic, "document_gold_decision": {"identity": identity, "semantic_state": dict(semantic), "reason_codes": []}, "fields": [item]}


if __name__ == "__main__":
    unittest.main()
