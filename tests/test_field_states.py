from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.pipeline_states import AccessState, AcquisitionState, DiscoveryState, ExtractionState, OpeningState


class FieldStateTests(unittest.TestCase):
    def test_all_states_have_distinct_deterministic_values(self) -> None:
        expected = {
            "NOT_EVALUATED",
            "PRESENT",
            "ABSENT",
            "NOT_APPLICABLE",
            "EXPECTED_ELSEWHERE",
            "CONFLICT",
            "INACCESSIBLE",
            "EXTRACTION_FAILED",
            "UNRESOLVED",
        }
        self.assertEqual(expected, {state.value for state in FieldState})
        self.assertEqual(len(expected), len(FieldState))

    def test_unknown_state_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            FieldState("MISSING")
        with self.assertRaises(ValueError):
            FieldResult.from_dict({"field_name": "parceiro", "state": "MISSING"})

    def test_present_round_trip_preserves_document_and_preview_evidence(self) -> None:
        identity = DocumentIdentity(process_id="P-1", document_id="D-1")
        for source_kind in (SourceKind.DOCUMENT, SourceKind.PREVIEW):
            with self.subTest(source_kind=source_kind):
                result = FieldResult(
                    field_name="parceiro",
                    state=FieldState.PRESENT,
                    value="Órgão X",
                    evidences=(FieldEvidence("parceiro", identity, source_kind),),
                )
                payload = result.to_dict()
                self.assertEqual("PRESENT", payload["state"])
                self.assertEqual(source_kind.value, payload["evidences"][0]["source_kind"])
                self.assertEqual(result, FieldResult.from_dict(json.loads(json.dumps(payload))))

    def test_absence_states_do_not_fabricate_value_or_evidence(self) -> None:
        for state in (
            FieldState.NOT_EVALUATED,
            FieldState.ABSENT,
            FieldState.NOT_APPLICABLE,
            FieldState.EXPECTED_ELSEWHERE,
            FieldState.INACCESSIBLE,
            FieldState.EXTRACTION_FAILED,
            FieldState.UNRESOLVED,
        ):
            with self.subTest(state=state):
                result = FieldResult("parceiro", state)
                self.assertIsNone(result.value)
                self.assertEqual((), result.evidences)
                self.assertEqual(result, FieldResult.from_dict(result.to_dict()))

    def test_conflict_preserves_all_evidence_without_winner(self) -> None:
        evidences = (
            FieldEvidence("vigencia", source_kind=SourceKind.DOCUMENT, raw_evidence="2025"),
            FieldEvidence("vigencia", source_kind=SourceKind.RELATED_DOCUMENT, raw_evidence="2026"),
        )
        result = FieldResult("vigencia", FieldState.CONFLICT, evidences=evidences)
        self.assertIsNone(result.value)
        self.assertEqual(evidences, result.evidences)
        self.assertEqual(result, FieldResult.from_dict(result.to_dict()))

    def test_non_present_state_rejects_a_resolved_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot carry"):
            FieldResult("parceiro", FieldState.ABSENT, value="X")

    def test_empty_string_is_not_inferred_as_absent(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-empty"):
            FieldResult("parceiro", FieldState.PRESENT, value="")
        with self.assertRaises(TypeError):
            FieldResult("parceiro", "", value="")

    def test_evidence_must_belong_to_the_same_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "must match"):
            FieldResult(
                "parceiro",
                FieldState.PRESENT,
                value="X",
                evidences=(FieldEvidence("objeto", source_kind=SourceKind.PREVIEW),),
            )

    def test_acquisition_and_field_states_are_independent(self) -> None:
        acquisition = AcquisitionState(
            discovery=DiscoveryState.FOUND,
            opening=OpeningState.OPENED,
            access=AccessState.ACCESS_RESTRICTED,
            extraction=ExtractionState.NOT_ATTEMPTED,
        )
        field = FieldResult("parceiro", FieldState.NOT_EVALUATED)
        self.assertEqual(AccessState.ACCESS_RESTRICTED, acquisition.access)
        self.assertEqual(FieldState.NOT_EVALUATED, field.state)
        self.assertNotEqual(acquisition.access.value, field.state.value)


if __name__ == "__main__":
    unittest.main()
