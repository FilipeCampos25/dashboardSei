from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import EvidenceLocation, FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.provenance_validation import (
    ProvenanceViolationCode,
    ProvenanceViolationKind,
    validate_field_provenance,
)


class ProvenanceValidationTests(unittest.TestCase):
    def test_present_document_with_auditable_reference_is_valid(self) -> None:
        self._assert_valid(SourceKind.DOCUMENT, source_document=self._document())

    def test_present_preview_is_identified_by_its_explicit_source_kind(self) -> None:
        self._assert_valid(SourceKind.PREVIEW)

    def test_present_related_document_requires_reference_and_relation(self) -> None:
        self._assert_valid(
            SourceKind.RELATED_DOCUMENT,
            source_document=self._document("related-1"),
            relation="supplementary",
        )

    def test_present_external_requires_external_reference(self) -> None:
        self._assert_valid(SourceKind.EXTERNAL, external_reference="external:item:7")

    def test_present_derived_requires_derivation_rule(self) -> None:
        self._assert_valid(SourceKind.DERIVED, rule_id="period.calculate_end")

    def test_present_without_evidence_has_explicit_violation(self) -> None:
        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, value="X"))

        self.assertEqual((ProvenanceViolationCode.MISSING_EVIDENCE,), report.codes)
        self.assertFalse(report.is_valid)

    def test_unknown_source_kind_is_rejected_by_the_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown source_kind"):
            FieldEvidence("objeto", source_kind="unknown")

    def test_legacy_payload_without_source_kind_cannot_pass_without_document_reference(self) -> None:
        evidence = FieldEvidence.from_dict({"field_name": "objeto"})

        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "X", (evidence,)))
        self.assertIs(evidence.source_kind, SourceKind.DOCUMENT)
        self.assertEqual((ProvenanceViolationCode.MISSING_DOCUMENT_REFERENCE,), report.codes)
        self.assertTrue(all(item.kind is ProvenanceViolationKind.INCOMPLETE for item in report.violations))

    def test_required_references_are_reported_when_missing(self) -> None:
        cases = (
            (SourceKind.DOCUMENT, ProvenanceViolationCode.MISSING_DOCUMENT_REFERENCE),
            (SourceKind.RELATED_DOCUMENT, ProvenanceViolationCode.MISSING_DOCUMENT_REFERENCE),
            (SourceKind.EXTERNAL, ProvenanceViolationCode.MISSING_EXTERNAL_REFERENCE),
            (SourceKind.DERIVED, ProvenanceViolationCode.MISSING_DERIVATION_RULE),
        )
        for source_kind, expected in cases:
            with self.subTest(source_kind=source_kind):
                report = self._validate(source_kind)
                self.assertIn(expected, report.codes)

    def test_related_document_without_relation_is_reported(self) -> None:
        report = self._validate(SourceKind.RELATED_DOCUMENT, source_document=self._document("related-1"))

        self.assertIn(ProvenanceViolationCode.MISSING_RELATION, report.codes)

    def test_optional_location_can_be_honestly_absent(self) -> None:
        evidence = FieldEvidence("objeto", source_document=self._document(), location=None)

        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "X", (evidence,)))
        self.assertTrue(report.is_valid)
        self.assertIsNone(evidence.location)

    def test_empty_location_does_not_fabricate_coordinates(self) -> None:
        evidence = FieldEvidence("objeto", source_document=self._document(), location=EvidenceLocation())

        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "X", (evidence,)))
        self.assertTrue(report.is_valid)
        self.assertTrue(all(value is None for value in evidence.location.to_dict().values()))

    def test_non_published_states_are_outside_the_published_field_invariant(self) -> None:
        for state in (
            FieldState.NOT_EVALUATED,
            FieldState.ABSENT,
            FieldState.NOT_APPLICABLE,
            FieldState.EXPECTED_ELSEWHERE,
            FieldState.CONFLICT,
            FieldState.INACCESSIBLE,
            FieldState.EXTRACTION_FAILED,
            FieldState.UNRESOLVED,
        ):
            with self.subTest(state=state):
                report = validate_field_provenance(FieldResult("objeto", state))
                self.assertFalse(report.is_published)
                self.assertTrue(report.is_valid)
                self.assertEqual((), report.violations)

    def test_multiple_valid_evidences_are_supported(self) -> None:
        evidences = (
            FieldEvidence("objeto", source_document=self._document("doc-1")),
            FieldEvidence("objeto", source_kind=SourceKind.PREVIEW),
        )
        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "X", evidences))

        self.assertTrue(report.is_valid)
        self.assertEqual((), report.violations)

    def _assert_valid(self, source_kind: SourceKind, **kwargs: object) -> None:
        report = self._validate(source_kind, **kwargs)
        self.assertTrue(report.is_published)
        self.assertTrue(report.is_valid)
        self.assertEqual((), report.violations)

    def _validate(self, source_kind: SourceKind, **kwargs: object):
        evidence = FieldEvidence("objeto", source_kind=source_kind, **kwargs)
        return validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "X", (evidence,)))

    @staticmethod
    def _document(document_id: str = "doc-1") -> DocumentIdentity:
        return DocumentIdentity(process_id="process-1", document_id=document_id)


if __name__ == "__main__":
    unittest.main()
