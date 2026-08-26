from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.services.gold_contracts import SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.pipeline_states import (
    AccessState,
    AcquisitionState,
    DiscoveryState,
    ExtractionState,
    OpeningState,
)
from app.services.publication_comparison import (
    CandidateComparisonInput,
    ComparisonReasonCode,
    ComparisonState,
    build_comparison_report,
)
from app.services.publication_policy import (
    ExternalAuthority,
    PublicationReasonCode,
    evaluate_document_gold,
)
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


FAMILIES = ("act", "pt", "ted", "administrative")
OUTPUT_DIR = REPO_ROOT / "backend" / "output"
HISTORICAL_SOURCES = {
    "ted": ("ted_status_execucao_latest.csv", 9),
    "administrative": ("documento_administrativo_status_execucao_latest.csv", 12),
}


class DocumentGoldInvariantTests(unittest.TestCase):
    def test_universal_content_invariant_and_positive_boundary_for_every_family(self) -> None:
        for families in (FAMILIES, tuple(reversed(FAMILIES))):
            decisions = {}
            for family in families:
                with self.subTest(order=families, family=family, case="verifiable"):
                    positive = self._evaluate(
                        family,
                        acquisition=self._successful(ExtractionState.EXTRACTED),
                        has_verifiable_content=True,
                    )
                    self.assertIs(positive.semantic_state.publication, PublicationState.PUBLISHED)
                    self.assertEqual(
                        positive.reason_codes,
                        (PublicationReasonCode.ELIGIBLE_VERIFIABLE_CONTENT.value,),
                    )

                with self.subTest(order=families, family=family, case="empty"):
                    decisions[family] = self._assert_non_verifiable_candidate_cannot_be_document_gold(
                        family,
                        acquisition=self._successful(ExtractionState.EMPTY_CONTENT),
                        expected_reason=PublicationReasonCode.EMPTY_CONTENT,
                    )

            self.assertEqual(set(decisions), set(FAMILIES))

    def test_title_and_metadata_only_never_satisfy_gate_for_any_family(self) -> None:
        titles = {
            "act": "Acordo de Cooperação Técnica perfeito",
            "pt": "Plano de Trabalho completo",
            "ted": "Termo de Execução Descentralizada",
            "administrative": "Memorando, Ofício e Nota Técnica",
        }
        for family, title in titles.items():
            with self.subTest(family=family, title=title):
                self.assertTrue(title)  # Metadata exists but is deliberately not document content.
                self._assert_non_verifiable_candidate_cannot_be_document_gold(
                    family,
                    acquisition=self._successful(ExtractionState.EXTRACTED),
                    expected_reason=PublicationReasonCode.NO_VERIFIABLE_CONTENT,
                )

    def test_technical_failures_never_publish_and_preserve_non_empty_reason(self) -> None:
        cases = (
            (
                AcquisitionState(
                    DiscoveryState.FOUND,
                    OpeningState.OPENED,
                    AccessState.IFRAME_UNAVAILABLE,
                    ExtractionState.NOT_ATTEMPTED,
                ),
                PublicationReasonCode.IFRAME_UNAVAILABLE,
            ),
            (
                AcquisitionState(
                    DiscoveryState.FOUND,
                    OpeningState.OPENED,
                    AccessState.ACCESS_RESTRICTED,
                    ExtractionState.NOT_ATTEMPTED,
                ),
                PublicationReasonCode.ACCESS_RESTRICTED,
            ),
            (
                AcquisitionState(
                    DiscoveryState.FOUND,
                    OpeningState.TIMEOUT,
                    AccessState.UNKNOWN,
                    ExtractionState.NOT_ATTEMPTED,
                ),
                PublicationReasonCode.OPENING_TIMEOUT,
            ),
            (
                AcquisitionState(
                    DiscoveryState.FOUND,
                    OpeningState.OPEN_FAILED,
                    AccessState.UNKNOWN,
                    ExtractionState.NOT_ATTEMPTED,
                ),
                PublicationReasonCode.OPENING_FAILED,
            ),
            (self._successful(ExtractionState.EXTRACTION_FAILED), PublicationReasonCode.EXTRACTION_FAILED),
        )
        for family in FAMILIES:
            for acquisition, reason in cases:
                with self.subTest(family=family, reason=reason):
                    decision = self._assert_non_verifiable_candidate_cannot_be_document_gold(
                        family,
                        acquisition=acquisition,
                        expected_reason=reason,
                    )
                    self.assertNotEqual(
                        decision.reason_codes,
                        (PublicationReasonCode.EMPTY_CONTENT.value,),
                    )

    def test_external_evidence_boundary_for_every_family(self) -> None:
        for family in FAMILIES:
            with self.subTest(family=family, source="external-unconfirmed"):
                self._assert_non_verifiable_candidate_cannot_be_document_gold(
                    family,
                    acquisition=self._successful(ExtractionState.EXTRACTED),
                    expected_reason=PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED,
                    external_authority=ExternalAuthority(SourceKind.EXTERNAL, authority_confirmed=False),
                )
            for source_kind in (SourceKind.PREVIEW, SourceKind.RELATED_DOCUMENT):
                with self.subTest(family=family, source=source_kind):
                    self._assert_non_verifiable_candidate_cannot_be_document_gold(
                        family,
                        acquisition=self._successful(ExtractionState.EXTRACTED),
                        expected_reason=PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED,
                        external_authority=ExternalAuthority(source_kind, authority_confirmed=True),
                    )
            with self.subTest(family=family, source="external-confirmed"):
                decision = self._evaluate(
                    family,
                    acquisition=self._successful(ExtractionState.EMPTY_CONTENT),
                    has_verifiable_content=False,
                    external_authority=ExternalAuthority(SourceKind.EXTERNAL, authority_confirmed=True),
                )
                self.assertIs(decision.semantic_state.publication, PublicationState.PUBLISHED)
                self.assertEqual(
                    decision.reason_codes,
                    (PublicationReasonCode.ELIGIBLE_EXTERNAL_AUTHORITY.value,),
                )

    def test_frozen_legacy_empty_gold_sentinels_are_blocked_in_shadow_v2(self) -> None:
        comparison_inputs = []
        for family, (filename, expected_count) in HISTORICAL_SOURCES.items():
            rows = self._legacy_empty_gold_rows(filename)
            self.assertEqual(len(rows), expected_count)
            for index, row in enumerate(rows, start=1):
                identity = self._synthetic_historical_identity(family, row["processo"], index)
                # The legacy CSV proves zero stored characters, but not an EMPTY_CONTENT
                # V2 acquisition fact. EXTRACTED + explicit false content is conservative.
                decision = evaluate_document_gold(
                    identity=identity,
                    acquisition=self._successful(ExtractionState.EXTRACTED),
                    semantic=self._semantic(),
                    has_verifiable_content=False,
                )
                self.assertIs(decision.semantic_state.publication, PublicationState.BLOCKED)
                self.assertEqual(
                    decision.reason_codes,
                    (PublicationReasonCode.NO_VERIFIABLE_CONTENT.value,),
                )
                comparison_inputs.append(
                    CandidateComparisonInput(
                        family=family,
                        identity=identity,
                        legacy_document_gold=True,
                        legacy_selected=None,
                        gate_decision=decision,
                    )
                )

        report = build_comparison_report(reversed(comparison_inputs))
        self.assertEqual(len(report["candidates"]), 21)
        self.assertTrue(
            all(row["comparison_state"] == ComparisonState.DIVERGENCE.value for row in report["candidates"])
        )
        self.assertTrue(
            all(row["comparison_reason"] == ComparisonReasonCode.LEGACY_ONLY.value for row in report["candidates"])
        )

    def _assert_non_verifiable_candidate_cannot_be_document_gold(
        self,
        family: str,
        *,
        acquisition: AcquisitionState,
        expected_reason: PublicationReasonCode,
        external_authority: ExternalAuthority | None = None,
    ):
        decision = self._evaluate(
            family,
            acquisition=acquisition,
            has_verifiable_content=False,
            external_authority=external_authority,
        )
        self.assertIs(decision.semantic_state.publication, PublicationState.BLOCKED)
        self.assertEqual(decision.reason_codes, (expected_reason.value,))
        return decision

    def _evaluate(
        self,
        family: str,
        *,
        acquisition: AcquisitionState,
        has_verifiable_content: bool,
        external_authority: ExternalAuthority | None = None,
    ):
        return evaluate_document_gold(
            identity=DocumentIdentity(
                process_id=f"test-only:{family}:process",
                document_id=f"test-only:{family}:document",
                candidate_id=f"test-only:{family}:candidate",
            ),
            acquisition=acquisition,
            semantic=self._semantic(),
            has_verifiable_content=has_verifiable_content,
            external_authority=external_authority,
        )

    @staticmethod
    def _successful(extraction: ExtractionState) -> AcquisitionState:
        return AcquisitionState(
            DiscoveryState.FOUND,
            OpeningState.OPENED,
            AccessState.ACCESSIBLE,
            extraction,
        )

    @staticmethod
    def _semantic() -> SemanticState:
        return SemanticState(
            ClassificationState.CONFIRMED,
            DocumentFunctionState.INSTRUMENT,
            AffinityState.NOT_EVALUATED,
            CanonicalState.NOT_EVALUATED,
            PublicationState.NOT_EVALUATED,
        )

    @staticmethod
    def _legacy_empty_gold_rows(filename: str) -> list[dict[str, str]]:
        with (OUTPUT_DIR / filename).open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        return [
            row
            for row in rows
            if row.get("publication_status") == "published_gold"
            and int(row.get("text_chars") or 0) == 0
        ]

    @staticmethod
    def _synthetic_historical_identity(family: str, process_id: str, index: int) -> DocumentIdentity:
        return DocumentIdentity(
            process_id=process_id,
            candidate_id=f"test-only:historical-{family}-empty-{index:02d}",
        )


if __name__ == "__main__":
    unittest.main()
