from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.act_field_consolidation import consolidate_act_fields
from app.services.act_normalizer import resolve_act_vigencia


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

    def test_title_only_extract_without_publication_evidence_remains_unresolved(self) -> None:
        primary = self.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
        related = self.record("E", "act.extract", "INELIGIBLE", "data_publicacao", None)
        related["title"] = "Extrato de publicacao"

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

    def test_publication_vigencia_uses_authorized_related_evidence(self) -> None:
        primary, related = self.publication_vigencia_records("2021-04-10")
        primary["fields"].append(
            self.present_field("objeto", "cooperacao tecnica", primary["identity"])
        )
        primary_semantic = copy.deepcopy(primary["semantic_state"])
        primary_decision = copy.deepcopy(primary["document_gold_decision"])
        related_semantic = copy.deepcopy(related["semantic_state"])
        related_decision = copy.deepcopy(related["document_gold_decision"])

        result = consolidate_act_fields(
            [primary, related], vigencia_resolver=resolve_act_vigencia
        )
        start = self.field(result[0], "vigencia_inicio")
        end = self.field(result[0], "vigencia_fim")

        self.assertEqual(("PRESENT", "2021-04-10"), (start["state"], start["value"]))
        self.assertEqual(("PRESENT", "2026-04-09"), (end["state"], end["value"]))
        related_evidence = next(
            item for item in start["evidences"] if item["source_kind"] == "related_document"
        )
        self.assertEqual("D-E", related_evidence["source_document"]["document_id"])
        self.assertEqual(
            "resolved_from_related_publication",
            result[0]["act_vigencia_resolution"]["reason"],
        )
        self.assertEqual("cooperacao tecnica", self.field(result[0], "objeto")["value"])
        self.assertEqual(primary_semantic, result[0]["semantic_state"])
        self.assertEqual(primary_decision, result[0]["document_gold_decision"])
        self.assertEqual(related_semantic, result[1]["semantic_state"])
        self.assertEqual(related_decision, result[1]["document_gold_decision"])

    def test_publication_vigencia_without_publication_stays_unresolved(self) -> None:
        primary, related = self.publication_vigencia_records(None)

        result = consolidate_act_fields(
            [primary, related], vigencia_resolver=resolve_act_vigencia
        )

        self.assertEqual("UNRESOLVED", self.field(result[0], "vigencia_inicio")["state"])
        self.assertEqual(
            "publication_missing",
            result[0]["act_vigencia_resolution"]["reason"],
        )

    def test_conflicting_publications_leave_vigencia_unresolved(self) -> None:
        primary, first = self.publication_vigencia_records("2021-04-10")
        second = self.record(
            "E2", "act.extract", "INELIGIBLE", "data_publicacao", "2021-04-11"
        )

        result = consolidate_act_fields(
            [primary, first, second], vigencia_resolver=resolve_act_vigencia
        )

        self.assertEqual("CONFLICT", self.field(result[0], "data_publicacao")["state"])
        self.assertEqual("UNRESOLVED", self.field(result[0], "vigencia_fim")["state"])
        self.assertEqual(
            "publication_conflict",
            result[0]["act_vigencia_resolution"]["reason"],
        )

    def test_signature_vigencia_is_not_changed_by_related_publication(self) -> None:
        primary, related = self.publication_vigencia_records("2021-04-10")
        clause = self.field(primary, "vigencia_raw")
        clause["value"] = "5 anos a partir da ultima assinatura"
        primary["fields"].append(
            self.present_field("data_assinatura", "2021-04-01", primary["identity"])
        )
        original = self.field(primary, "vigencia_inicio")
        original.update(self.present_field("vigencia_inicio", "2021-04-01", primary["identity"]))

        result = consolidate_act_fields(
            [primary, related], vigencia_resolver=resolve_act_vigencia
        )

        self.assertEqual("2021-04-01", self.field(result[0], "vigencia_inicio")["value"])
        self.assertNotIn("act_vigencia_resolution", result[0])

    @classmethod
    def publication_vigencia_records(cls, publication: str | None) -> tuple[dict, dict]:
        primary = cls.record("I", "act.instrument", "SELECTED", "data_publicacao", None)
        primary["fields"].append(
            cls.present_field(
                "vigencia_raw",
                "vigencia de 5 anos a partir da publicacao no DOU",
                primary["identity"],
            )
        )
        primary["fields"].extend(
            {"field_name": name, "state": "NOT_EVALUATED", "value": None, "evidences": []}
            for name in (
                "vigencia_inicio",
                "vigencia_fim",
                "data_inicio_vigencia",
                "data_fim_vigencia",
            )
        )
        related = cls.record("E", "act.extract", "INELIGIBLE", "data_publicacao", publication)
        return primary, related

    @staticmethod
    def present_field(name: str, value: str, identity: dict) -> dict:
        return {
            "field_name": name,
            "state": "PRESENT",
            "value": value,
            "evidences": [
                {
                    "field_name": name,
                    "source_kind": "document",
                    "source_document": copy.deepcopy(identity),
                    "relation": None,
                    "rule_id": f"act.{name}",
                    "location": None,
                    "raw_evidence": value,
                    "external_reference": None,
                }
            ],
        }

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
