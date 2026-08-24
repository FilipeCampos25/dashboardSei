from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.gold_contracts import SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.pipeline_states import AccessState, AcquisitionState, DiscoveryState, ExtractionState, OpeningState
from app.services.publication_policy import ExternalAuthority, PublicationReasonCode, evaluate_document_gold
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


class PublicationPolicyTests(unittest.TestCase):
    def test_verifiable_content_is_eligible_without_mutating_input(self) -> None:
        semantic = self._semantic()
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EXTRACTED),
            semantic=semantic,
            has_verifiable_content=True,
        )

        self.assertIs(decision.semantic_state.publication, PublicationState.PUBLISHED)
        self.assertEqual(decision.reason_codes, (PublicationReasonCode.ELIGIBLE_VERIFIABLE_CONTENT.value,))
        self.assertIs(semantic.publication, PublicationState.NOT_EVALUATED)

    def test_empty_content_fails_closed(self) -> None:
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EMPTY_CONTENT),
            semantic=self._semantic(),
            has_verifiable_content=False,
        )

        self.assertIs(decision.semantic_state.publication, PublicationState.BLOCKED)
        self.assertEqual(decision.reason_codes, (PublicationReasonCode.EMPTY_CONTENT.value,))

    def test_title_only_cannot_satisfy_the_gate(self) -> None:
        title = "Termo de Execucao Descentralizada 123/2026"
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EXTRACTED),
            semantic=self._semantic(),
            has_verifiable_content=False,
        )

        self.assertTrue(title)
        self.assertIs(decision.semantic_state.publication, PublicationState.BLOCKED)
        self.assertEqual(decision.reason_codes, (PublicationReasonCode.NO_VERIFIABLE_CONTENT.value,))

    def test_acquisition_failures_keep_their_technical_reason(self) -> None:
        cases = (
            (
                AcquisitionState(DiscoveryState.FOUND, OpeningState.TIMEOUT, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED),
                PublicationReasonCode.OPENING_TIMEOUT,
            ),
            (
                AcquisitionState(DiscoveryState.FOUND, OpeningState.OPENED, AccessState.IFRAME_UNAVAILABLE, ExtractionState.NOT_ATTEMPTED),
                PublicationReasonCode.IFRAME_UNAVAILABLE,
            ),
            (
                AcquisitionState(DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESS_RESTRICTED, ExtractionState.NOT_ATTEMPTED),
                PublicationReasonCode.ACCESS_RESTRICTED,
            ),
            (
                self._acquisition(ExtractionState.EXTRACTION_FAILED),
                PublicationReasonCode.EXTRACTION_FAILED,
            ),
        )
        for acquisition, reason in cases:
            with self.subTest(reason=reason):
                decision = evaluate_document_gold(
                    identity=self._identity(), acquisition=acquisition, semantic=self._semantic(), has_verifiable_content=False
                )
                self.assertEqual(decision.reason_codes, (reason.value,))
                self.assertNotEqual(decision.reason_codes, (PublicationReasonCode.EMPTY_CONTENT.value,))

    def test_explicit_external_authority_can_satisfy_empty_document(self) -> None:
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EMPTY_CONTENT),
            semantic=self._semantic(),
            has_verifiable_content=False,
            external_authority=ExternalAuthority(SourceKind.EXTERNAL, authority_confirmed=True),
        )

        self.assertIs(decision.semantic_state.publication, PublicationState.PUBLISHED)
        self.assertEqual(decision.reason_codes, (PublicationReasonCode.ELIGIBLE_EXTERNAL_AUTHORITY.value,))

    def test_external_without_explicit_authority_fails_closed(self) -> None:
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EXTRACTED),
            semantic=self._semantic(),
            has_verifiable_content=False,
            external_authority=ExternalAuthority(SourceKind.EXTERNAL, authority_confirmed=False),
        )

        self.assertEqual(decision.reason_codes, (PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED.value,))

    def test_preview_and_related_document_do_not_promote_document_gold(self) -> None:
        for source_kind in (SourceKind.PREVIEW, SourceKind.RELATED_DOCUMENT):
            with self.subTest(source_kind=source_kind):
                decision = evaluate_document_gold(
                    identity=self._identity(),
                    acquisition=self._acquisition(ExtractionState.EXTRACTED),
                    semantic=self._semantic(),
                    has_verifiable_content=False,
                    external_authority=ExternalAuthority(source_kind, authority_confirmed=True),
                )
                self.assertIs(decision.semantic_state.publication, PublicationState.BLOCKED)
                self.assertEqual(decision.reason_codes, (PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED.value,))

    def test_ineligible_function_fails_closed_without_reclassification(self) -> None:
        semantic = self._semantic(function=DocumentFunctionState.INELIGIBLE, resolved_function="supplementary")
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EXTRACTED),
            semantic=semantic,
            has_verifiable_content=True,
        )

        self.assertEqual(decision.reason_codes, (PublicationReasonCode.FUNCTION_INELIGIBLE.value,))
        self.assertIs(decision.semantic_state.function, DocumentFunctionState.INELIGIBLE)
        self.assertEqual(decision.semantic_state.resolved_function, "supplementary")

    def test_decision_round_trip_preserves_stable_reason_code(self) -> None:
        decision = evaluate_document_gold(
            identity=self._identity(),
            acquisition=self._acquisition(ExtractionState.EXTRACTED),
            semantic=self._semantic(),
            has_verifiable_content=True,
        )

        restored = type(decision).from_dict(decision.to_dict())
        self.assertEqual(restored, decision)

    def test_multiple_blockers_have_stable_primary_reason_precedence(self) -> None:
        technical_failure = AcquisitionState(
            DiscoveryState.FOUND,
            OpeningState.TIMEOUT,
            AccessState.UNKNOWN,
            ExtractionState.NOT_ATTEMPTED,
        )
        cases = (
            (
                technical_failure,
                self._semantic(),
                None,
                PublicationReasonCode.OPENING_TIMEOUT,
            ),
            (
                self._acquisition(ExtractionState.EXTRACTED),
                self._semantic(function=DocumentFunctionState.INELIGIBLE),
                None,
                PublicationReasonCode.FUNCTION_INELIGIBLE,
            ),
            (
                self._acquisition(ExtractionState.EXTRACTED),
                self._semantic(),
                ExternalAuthority(SourceKind.EXTERNAL, authority_confirmed=False),
                PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED,
            ),
            (
                technical_failure,
                self._semantic(function=DocumentFunctionState.INELIGIBLE),
                None,
                PublicationReasonCode.OPENING_TIMEOUT,
            ),
        )

        for acquisition, semantic, authority, expected in cases:
            with self.subTest(expected=expected):
                decisions = tuple(
                    evaluate_document_gold(
                        identity=self._identity(),
                        acquisition=acquisition,
                        semantic=semantic,
                        has_verifiable_content=False,
                        external_authority=authority,
                    )
                    for _ in range(5)
                )
                self.assertEqual({decision.reason_codes for decision in decisions}, {(expected.value,)})
                self.assertEqual({decision.to_dict()["semantic_state"]["publication"] for decision in decisions}, {"BLOCKED"})

    @staticmethod
    def _identity() -> DocumentIdentity:
        return DocumentIdentity(process_id="P1", document_id="D1", candidate_id="C1")

    @staticmethod
    def _acquisition(extraction: ExtractionState) -> AcquisitionState:
        return AcquisitionState(DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, extraction)

    @staticmethod
    def _semantic(**overrides: object) -> SemanticState:
        values = {
            "classification": ClassificationState.CONFIRMED,
            "function": DocumentFunctionState.INSTRUMENT,
            "affinity": AffinityState.NOT_EVALUATED,
            "canonical": CanonicalState.NOT_EVALUATED,
            "publication": PublicationState.NOT_EVALUATED,
            **overrides,
        }
        return SemanticState(**values)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
