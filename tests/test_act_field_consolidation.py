from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.act_field_consolidation import consolidate_act_fields


class ActFieldConsolidationTests(unittest.TestCase):
    def test_authorized_related_extract_supplies_missing_publication_date(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", "2026-03-04")

        result = consolidate_act_fields([primary, related])
        field = self.field(result[0], "data_publicacao")

        self.assertEqual("PRESENT", field["state"])
        self.assertEqual("2026-03-04", field["value"])
        self.assertEqual("related_document", field["evidences"][0]["source_kind"])
        self.assertEqual("D-E", field["evidences"][0]["source_document"]["document_id"])
        self.assertEqual("act.same_process_complement:act.extract", field["evidences"][0]["relation"])
        self.assertEqual("INELIGIBLE", result[1]["semantic_state"]["canonical"])
        self.assertEqual("BLOCKED", result[1]["semantic_state"]["publication"])

    def test_primary_value_is_not_silently_overwritten_and_conflict_is_explicit(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", "2026-03-04")
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", "2026-03-05")

        field = self.field(consolidate_act_fields([primary, related])[0], "data_publicacao")

        self.assertEqual("CONFLICT", field["state"])
        self.assertIsNone(field["value"])
        self.assertEqual(2, len(field["evidences"]))

    def test_matching_related_value_preserves_primary_and_adds_real_source(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", "2026-03-04")
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", "2026-03-04")

        field = self.field(consolidate_act_fields([primary, related])[0], "data_publicacao")

        self.assertEqual("PRESENT", field["state"])
        self.assertEqual("2026-03-04", field["value"])
        self.assertEqual(2, len(field["evidences"]))

    def test_unauthorized_related_function_does_not_supply_field(self) -> None:
        for function in ("act.amendment", "act.report", "act.related"):
            with self.subTest(function=function):
                primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
                related = self.record("R", function, "INELIGIBLE", "data_publicacao", "2026-03-04")

                field = self.field(consolidate_act_fields([primary, related])[0], "data_publicacao")

                self.assertEqual("NOT_EVALUATED", field["state"])
                self.assertIsNone(field["value"])

    def test_absent_evidence_remains_unresolved(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", None)

        field = self.field(consolidate_act_fields([primary, related])[0], "data_publicacao")

        self.assertEqual("NOT_EVALUATED", field["state"])
        self.assertIsNone(field["value"])

    def test_distinct_same_process_identity_is_required(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", "2026-03-04")
        related["identity"].update({"document_id": None, "candidate_id": None})
        related["fields"][0]["evidences"][0]["source_document"] = copy.deepcopy(related["identity"])

        field = self.field(consolidate_act_fields([primary, related])[0], "data_publicacao")

        self.assertEqual("NOT_EVALUATED", field["state"])

    @staticmethod
    def field(record: dict, name: str) -> dict:
        return next(item for item in record["fields"] if item["field_name"] == name)

    @classmethod
    def record(
        cls,
        candidate: str,
        function: str,
        canonical: str,
        field_name: str,
        value: str | None,
    ) -> dict:
        identity = {
            "process_id": "PROC",
            "document_id": f"D-{candidate}",
            "candidate_id": candidate,
            "source_url": f"https://example/{candidate}",
        }
        evidence = [] if value is None else [
            {
                "field_name": field_name,
                "source_kind": "document",
                "source_document": copy.deepcopy(identity),
                "relation": None,
                "rule_id": "act.data_publicacao.publicacao",
                "location": None,
                "raw_evidence": value,
                "external_reference": None,
            }
        ]
        field = {
            "field_name": field_name,
            "state": "PRESENT" if value is not None else "NOT_EVALUATED",
            "value": value,
            "evidences": evidence,
        }
        is_instrument = function == "act.instrument"
        semantic = {
            "classification": "CONFIRMED" if is_instrument else "RELATED",
            "resolved_class": "act_final" if is_instrument else "act_related",
            "function": "INSTRUMENT" if is_instrument else "RELATED",
            "resolved_function": function,
            "affinity": "MATCHED",
            "canonical": canonical,
            "publication": "PUBLISHED" if canonical == "SELECTED" else "BLOCKED",
        }
        return {
            "identity": identity,
            "semantic_state": semantic,
            "document_gold_decision": {
                "identity": copy.deepcopy(identity),
                "semantic_state": copy.deepcopy(semantic),
                "reason_codes": [],
            },
            "fields": [field],
        }


if __name__ == "__main__":
    unittest.main()
