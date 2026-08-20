from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.gold_contracts import DocumentGoldDecision, FieldEvidence, RecordGold
from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


class GoldContractsTests(unittest.TestCase):
    def test_document_gold_decision_preserves_identity_and_semantic_state(self) -> None:
        decision = self._document_gold()

        self.assertEqual(decision.identity.document_id, "doc-main")
        self.assertIs(decision.semantic_state.publication, PublicationState.PUBLISHED)
        self.assertEqual(decision.reason_codes, ("document.selected",))

    def test_document_gold_round_trip_is_deterministic(self) -> None:
        decision = self._document_gold()

        self.assertEqual(DocumentGoldDecision.from_dict(decision.to_dict()), decision)
        self.assertEqual(DocumentGoldDecision.from_dict(decision.to_dict()).to_dict(), decision.to_dict())

    def test_document_gold_does_not_assert_or_create_record_completeness(self) -> None:
        decision = self._document_gold()

        self.assertNotIsInstance(decision, RecordGold)
        self.assertNotIn("record_complete", decision.to_dict())
        self.assertNotIn("field_evidence", decision.to_dict())

    def test_field_evidence_from_related_source_does_not_promote_source(self) -> None:
        source = DocumentIdentity(process_id="process-1", document_id="doc-preview")
        evidence = FieldEvidence(field_name="parceiro", source_document=source)

        self.assertEqual(evidence.source_document, source)
        self.assertNotIsInstance(evidence.source_document, DocumentGoldDecision)
        self.assertEqual(set(evidence.to_dict()), {"field_name", "source_document"})

    def test_field_evidence_round_trip_does_not_fabricate_rich_provenance(self) -> None:
        evidence = FieldEvidence(
            field_name="data_publicacao",
            source_document=DocumentIdentity(process_id="process-1", document_id="doc-related"),
        )

        serialized = evidence.to_dict()
        self.assertEqual(FieldEvidence.from_dict(serialized), evidence)
        for deferred_field in ("page", "excerpt", "xpath", "table", "confidence", "warning", "relation"):
            self.assertNotIn(deferred_field, serialized)

    def test_record_gold_combines_primary_document_and_multiple_evidence(self) -> None:
        primary = self._document_gold()
        evidence = (
            FieldEvidence("parceiro", DocumentIdentity(process_id="process-1", document_id="doc-preview")),
            FieldEvidence("data_publicacao", DocumentIdentity(process_id="process-1", document_id="doc-related")),
        )

        record = RecordGold(process_id="process-1", primary_document=primary, field_evidence=evidence)

        self.assertEqual(record.primary_document, primary)
        self.assertEqual(len(record.field_evidence), 2)
        self.assertEqual({item.source_document.document_id for item in record.field_evidence}, {"doc-preview", "doc-related"})

    def test_record_gold_round_trip_is_deterministic(self) -> None:
        record = RecordGold(
            process_id="process-1",
            primary_document=self._document_gold(),
            field_evidence=(
                FieldEvidence("parceiro", DocumentIdentity(process_id="process-1", document_id="doc-preview")),
            ),
        )

        self.assertEqual(RecordGold.from_dict(record.to_dict()), record)
        self.assertEqual(RecordGold.from_dict(record.to_dict()).to_dict(), record.to_dict())

    def test_contracts_are_distinct_and_creation_has_no_cross_contract_side_effect(self) -> None:
        decision = self._document_gold()
        evidence = FieldEvidence("parceiro", DocumentIdentity(process_id="process-1", document_id="doc-preview"))

        self.assertIsNot(type(decision), type(evidence))
        self.assertIsNot(type(evidence), RecordGold)
        self.assertEqual(decision.identity.document_id, "doc-main")
        self.assertEqual(evidence.source_document.document_id, "doc-preview")
        self.assertNotIn("is_gold", decision.to_dict())
        self.assertNotIn("is_gold", evidence.to_dict())

    @staticmethod
    def _document_gold() -> DocumentGoldDecision:
        return DocumentGoldDecision(
            identity=DocumentIdentity(process_id="process-1", document_id="doc-main"),
            semantic_state=SemanticState(
                classification=ClassificationState.CONFIRMED,
                function=DocumentFunctionState.INSTRUMENT,
                affinity=AffinityState.MATCHED,
                canonical=CanonicalState.SELECTED,
                publication=PublicationState.PUBLISHED,
            ),
            reason_codes=("document.selected",),
        )


if __name__ == "__main__":
    unittest.main()
