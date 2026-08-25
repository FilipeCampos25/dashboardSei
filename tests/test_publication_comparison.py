from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.canonical_selection import CanonicalCandidate, select_canonical
from app.services.normalization_contract import DocumentIdentity
from app.services.publication_comparison import (
    CandidateComparisonInput,
    ComparisonReasonCode,
    build_comparison_report,
    serialize_comparison_report,
)
from app.services.publication_policy import PublicationReasonCode, evaluate_document_gold
from app.services.pipeline_states import AccessState, AcquisitionState, DiscoveryState, ExtractionState, OpeningState
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


FIXTURE = Path(__file__).parent / "fixtures" / "publication_comparison_snapshot.json"


class PublicationComparisonTests(unittest.TestCase):
    def test_snapshot_covers_candidate_states_family_summaries_and_stable_reasons(self) -> None:
        inputs = self._scenario()
        report = build_comparison_report(inputs)

        self.assertEqual(json.loads(serialize_comparison_report(report)), json.loads(FIXTURE.read_text(encoding="utf-8")))
        blocked = next(row for row in report["candidates"] if row["candidate_id"] == "C-BLOCKED")
        self.assertEqual(blocked["comparison_reason"], ComparisonReasonCode.LEGACY_ONLY.value)
        self.assertEqual(blocked["v2_gate_reason"], PublicationReasonCode.NO_VERIFIABLE_CONTENT.value)

    def test_order_and_serialization_are_deterministic(self) -> None:
        inputs = self._scenario()
        first = serialize_comparison_report(build_comparison_report(inputs))
        second = serialize_comparison_report(build_comparison_report(list(reversed(inputs))))
        self.assertEqual(first, second)
        self.assertEqual(first, serialize_comparison_report(build_comparison_report(inputs)))

    def test_duplicate_candidate_identity_is_rejected(self) -> None:
        item = self._scenario()[0]
        with self.assertRaises(ValueError):
            build_comparison_report([item, item])

    def test_same_candidate_id_is_allowed_in_different_processes_and_families(self) -> None:
        report = build_comparison_report([
            self._input("act", "P-A", "C-SHARED", False, False, False, None),
            self._input("act", "P-B", "C-SHARED", False, False, False, None),
            self._input("pt", "P-A", "C-SHARED", False, False, False, None),
        ])
        self.assertEqual(len(report["candidates"]), 3)

    def test_gate_blocked_is_distinct_from_canonical_parameters_unavailable(self) -> None:
        blocked, eligible = build_comparison_report([
            self._input("act", "P-BLOCKED", "C-BLOCKED", True, False, False, None),
            self._input("act", "P-ELIGIBLE", "C-ELIGIBLE", True, None, True, None),
        ])["candidates"]

        self.assertEqual(blocked["v2_gate_state"], PublicationState.BLOCKED.value)
        self.assertEqual(blocked["v2_canonical_state"], CanonicalState.INELIGIBLE.value)
        self.assertEqual(blocked["v2_canonical_reason"], ComparisonReasonCode.GATE_INELIGIBLE.value)
        self.assertFalse(blocked["v2_selected"])
        self.assertEqual(blocked["comparison_reason"], ComparisonReasonCode.LEGACY_ONLY.value)

        self.assertEqual(eligible["v2_gate_state"], PublicationState.PUBLISHED.value)
        self.assertEqual(eligible["v2_canonical_state"], CanonicalState.NOT_EVALUATED.value)
        self.assertEqual(eligible["v2_canonical_reason"], ComparisonReasonCode.PARAMETERS_UNAVAILABLE.value)
        self.assertIsNone(eligible["v2_selected"])
        self.assertNotEqual(blocked["v2_canonical_reason"], eligible["v2_canonical_reason"])

    def test_winner_changed_count_is_candidate_row_count_and_summary_is_derived(self) -> None:
        report = build_comparison_report(self._scenario())
        changed = [
            row for row in report["candidates"]
            if row["process_id"] == "P2" and row["comparison_reason"] == ComparisonReasonCode.WINNER_CHANGED.value
        ]
        self.assertEqual(
            [(row["candidate_id"], row["legacy_selected"], row["v2_selected"]) for row in changed],
            [("C-NEW", False, True), ("C-OLD", True, False)],
        )
        act_summary = next(summary for summary in report["families"] if summary["family"] == "act")
        act_rows = [row for row in report["candidates"] if row["family"] == "act"]
        self.assertEqual(act_summary["candidate_count"], len(act_rows))
        self.assertEqual(act_summary["winner_changed_count"], len(changed))
        self.assertEqual(
            act_summary["divergence_count"],
            sum(row["comparison_state"] == "DIVERGENCE" for row in act_rows),
        )

    def test_unknown_legacy_selected_is_not_false(self) -> None:
        decision = select_canonical(
            [CanonicalCandidate(self._identity("act", "P-UNKNOWN", "C-UNKNOWN-WINNER"), 10)],
            threshold=5,
            min_margin=1,
        )
        unknown = self._input("act", "P-UNKNOWN", "C-UNKNOWN-WINNER", True, None, True, decision)
        row = build_comparison_report([unknown])["candidates"][0]
        self.assertIsNone(row["legacy_selected"])
        self.assertEqual(row["comparison_state"], "NOT_COMPARABLE")
        self.assertEqual(row["comparison_reason"], ComparisonReasonCode.INSUFFICIENT_INFORMATION.value)

    def test_negative_gate_agreement_and_missing_document_id_remain_explicit(self) -> None:
        item = self._input("ted", "P6", "C-NO", False, False, False, None)
        item = CandidateComparisonInput(
            family=item.family,
            identity=DocumentIdentity(process_id="P6", candidate_id="C-NO"),
            legacy_document_gold=False,
            legacy_selected=False,
            gate_decision=evaluate_document_gold(
                identity=DocumentIdentity(process_id="P6", candidate_id="C-NO"),
                acquisition=AcquisitionState(DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EXTRACTED),
                semantic=item.gate_decision.semantic_state,
                has_verifiable_content=False,
            ),
        )
        row = build_comparison_report([item])["candidates"][0]
        self.assertIsNone(row["document_id"])
        self.assertEqual(row["comparison_reason"], ComparisonReasonCode.GATE_AGREEMENT.value)

    def test_zero_candidates_has_explicit_empty_report(self) -> None:
        self.assertEqual(build_comparison_report([]), {"schema_version": "1.0", "candidates": [], "families": []})

    @classmethod
    def _scenario(cls) -> list[CandidateComparisonInput]:
        selected_same = select_canonical([CanonicalCandidate(cls._identity("act", "P1", "C-SAME"), 10)], threshold=5, min_margin=2)
        changed = select_canonical(
            [
                CanonicalCandidate(cls._identity("act", "P2", "C-NEW"), 10),
                CanonicalCandidate(cls._identity("act", "P2", "C-OLD"), 5),
            ],
            threshold=5,
            min_margin=2,
        )
        unresolved = select_canonical([CanonicalCandidate(cls._identity("pt", "P3", "C-LOW"), 4)], threshold=5, min_margin=1)
        tie = select_canonical(
            [CanonicalCandidate(cls._identity("pt", "P4", "C-TIE-A"), 8), CanonicalCandidate(cls._identity("pt", "P4", "C-TIE-B"), 8)],
            threshold=5,
            min_margin=1,
        )
        return [
            cls._input("act", "P1", "C-SAME", True, True, True, selected_same),
            cls._input("act", "P1", "C-V2", False, False, True, selected_same),
            cls._input("act", "P1", "C-BLOCKED", True, False, False, None),
            cls._input("act", "P2", "C-OLD", True, True, True, changed),
            cls._input("act", "P2", "C-NEW", True, False, True, changed),
            cls._input("pt", "P3", "C-LOW", True, True, True, unresolved),
            cls._input("pt", "P4", "C-TIE-A", True, True, True, tie),
            cls._input("pt", "P4", "C-TIE-B", True, False, True, tie),
            cls._input("pt", "P5", "C-UNKNOWN", None, None, True, None),
        ]

    @classmethod
    def _input(cls, family, process, candidate, legacy_gold, legacy_selected, eligible, canonical):
        identity = cls._identity(family, process, candidate)
        extraction = ExtractionState.EXTRACTED
        return CandidateComparisonInput(
            family=family,
            identity=identity,
            legacy_document_gold=legacy_gold,
            legacy_selected=legacy_selected,
            gate_decision=evaluate_document_gold(
                identity=identity,
                acquisition=AcquisitionState(DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, extraction),
                semantic=SemanticState(
                    ClassificationState.CONFIRMED,
                    DocumentFunctionState.INSTRUMENT,
                    AffinityState.NOT_EVALUATED,
                    CanonicalState.NOT_EVALUATED,
                    PublicationState.NOT_EVALUATED,
                ),
                has_verifiable_content=eligible,
            ),
            canonical_decision=canonical,
        )

    @staticmethod
    def _identity(family: str, process: str, candidate: str) -> DocumentIdentity:
        return DocumentIdentity(process_id=process, document_id=f"D-{candidate}", candidate_id=candidate)


if __name__ == "__main__":
    unittest.main()
