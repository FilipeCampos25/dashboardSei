from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.gold_contracts import EvidenceLocation, FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity


class SourceKindTests(unittest.TestCase):
    def test_universal_values_are_stable_and_round_trip(self) -> None:
        expected = {
            "document",
            "preview",
            "related_document",
            "external",
            "derived",
        }

        self.assertEqual({item.value for item in SourceKind}, expected)
        for item in SourceKind:
            self.assertIs(SourceKind(item.value), item)

    def test_unknown_value_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            FieldEvidence(field_name="objeto", source_kind="act")


class EvidenceLocationTests(unittest.TestCase):
    def test_known_and_partial_locations_round_trip(self) -> None:
        locations = (
            EvidenceLocation(
                source_path="snapshot.tables[1]",
                page=2,
                table_index=1,
                row_index=3,
                column_index=2,
                section="Vigência",
                position=140,
            ),
            EvidenceLocation(table_index=1, row_index=2),
        )

        for location in locations:
            self.assertEqual(EvidenceLocation.from_dict(location.to_dict()), location)

    def test_unknown_location_fabricates_no_coordinates(self) -> None:
        location = EvidenceLocation()

        self.assertEqual(
            location.to_dict(),
            {
                "source_path": None,
                "page": None,
                "table_index": None,
                "row_index": None,
                "column_index": None,
                "section": None,
                "position": None,
            },
        )

    def test_invalid_coordinate_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            EvidenceLocation(page=-1)


class FieldEvidenceTests(unittest.TestCase):
    def test_document_preserves_identity_without_process_fallback(self) -> None:
        identity = DocumentIdentity(process_id="process-1", document_id=None)
        evidence = FieldEvidence("numero", identity)

        restored = FieldEvidence.from_dict(evidence.to_dict())
        self.assertEqual(restored.source_document, identity)
        self.assertIsNone(restored.source_document.document_id)
        self.assertIs(restored.source_kind, SourceKind.DOCUMENT)

    def test_preview_without_document_or_location_is_valid(self) -> None:
        evidence = FieldEvidence(
            field_name="parceiro",
            source_kind=SourceKind.PREVIEW,
            rule_id="pt.preview.partner_fallback",
        )

        serialized = evidence.to_dict()
        self.assertIsNone(serialized["source_document"])
        self.assertIsNone(serialized["location"])
        self.assertIsNone(serialized["raw_evidence"])
        self.assertEqual(FieldEvidence.from_dict(serialized), evidence)

    def test_related_document_preserves_identity_and_relation_without_gold_promotion(self) -> None:
        identity = DocumentIdentity(process_id="process-1", document_id="related-7")
        evidence = FieldEvidence(
            field_name="data_publicacao",
            source_document=identity,
            source_kind=SourceKind.RELATED_DOCUMENT,
            relation="supplementary",
        )

        restored = FieldEvidence.from_dict(evidence.to_dict())
        self.assertEqual(restored.source_document, identity)
        self.assertEqual(restored.relation, "supplementary")
        self.assertIsInstance(restored.source_document, DocumentIdentity)

    def test_external_source_does_not_require_sei_identity_or_imply_authority(self) -> None:
        evidence = FieldEvidence(
            field_name="situacao",
            source_kind=SourceKind.EXTERNAL,
            external_reference="https://dados.example/item/7",
        )

        serialized = evidence.to_dict()
        self.assertIsNone(serialized["source_document"])
        self.assertNotIn("authoritative", serialized)
        self.assertEqual(FieldEvidence.from_dict(serialized), evidence)

    def test_derived_source_supports_optional_rule_without_document_location(self) -> None:
        with_rule = FieldEvidence(
            field_name="vigencia_fim",
            source_kind=SourceKind.DERIVED,
            rule_id="act.vigencia.calculate_end",
            raw_evidence="60 meses",
        )
        without_rule = FieldEvidence(field_name="indicador", source_kind=SourceKind.DERIVED)

        for evidence in (with_rule, without_rule):
            self.assertIsNone(evidence.source_document)
            self.assertIsNone(evidence.location)
            self.assertEqual(FieldEvidence.from_dict(evidence.to_dict()), evidence)

    def test_document_location_and_raw_evidence_round_trip(self) -> None:
        evidence = FieldEvidence(
            field_name="unidade_descentralizada",
            source_document=DocumentIdentity(process_id="process-1", document_id="ted-1"),
            location=EvidenceLocation(
                source_path="snapshot.tables[1]",
                table_index=1,
                row_index=2,
                column_index=2,
            ),
            rule_id="ted.unit.ug_name.horizontal",
            raw_evidence="Unidade Gestora 123",
        )

        self.assertEqual(FieldEvidence.from_dict(evidence.to_dict()), evidence)

    def test_legacy_payload_and_constructor_remain_supported(self) -> None:
        identity = DocumentIdentity(process_id="process-1", document_id="doc-1")
        payload = {"field_name": "objeto", "source_document": identity.to_dict()}

        from_payload = FieldEvidence.from_dict(payload)
        from_constructor = FieldEvidence("objeto", identity)
        self.assertEqual(from_payload, from_constructor)
        self.assertIs(from_payload.source_kind, SourceKind.DOCUMENT)

    def test_blank_optional_metadata_becomes_unknown_not_placeholder(self) -> None:
        evidence = FieldEvidence(
            field_name="objeto",
            source_kind=SourceKind.PREVIEW,
            relation=" ",
            rule_id="",
            raw_evidence="\n",
            external_reference="",
        )

        self.assertIsNone(evidence.relation)
        self.assertIsNone(evidence.rule_id)
        self.assertIsNone(evidence.raw_evidence)
        self.assertIsNone(evidence.external_reference)


if __name__ == "__main__":
    unittest.main()
