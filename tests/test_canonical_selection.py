from __future__ import annotations

import math
import sys
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.canonical_selection import (
    CanonicalCandidate,
    CanonicalDecision,
    CanonicalReasonCode,
    select_canonical,
)
from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import CanonicalState


class CanonicalSelectionTests(unittest.TestCase):
    def test_no_candidate_abstains(self) -> None:
        decision = select_canonical([], threshold=5, min_margin=2)
        self.assert_decision(decision, CanonicalState.UNRESOLVED, CanonicalReasonCode.NO_CANDIDATE)

    def test_single_candidate_below_threshold_does_not_win(self) -> None:
        decision = select_canonical([self.candidate("A", 4.9)], threshold=5, min_margin=2)
        self.assert_decision(decision, CanonicalState.UNRESOLVED, CanonicalReasonCode.BELOW_THRESHOLD)

    def test_threshold_is_inclusive_and_single_candidate_needs_no_margin(self) -> None:
        decision = select_canonical([self.candidate("A", 5)], threshold=5, min_margin=2)
        self.assert_decision(decision, CanonicalState.SELECTED, CanonicalReasonCode.SELECTED)
        self.assertEqual(decision.winner_candidate_id, "A")
        self.assertEqual(decision.winner_score, 5.0)
        self.assertIsNone(decision.runner_up_score)
        self.assertIsNone(decision.observed_margin)

    def test_multiple_candidates_all_below_threshold_do_not_choose_relative_best(self) -> None:
        decision = select_canonical(
            [self.candidate("A", 4), self.candidate("B", 3)], threshold=5, min_margin=1
        )
        self.assert_decision(decision, CanonicalState.UNRESOLVED, CanonicalReasonCode.BELOW_THRESHOLD)

    def test_clear_winner_is_selected_and_identity_is_preserved(self) -> None:
        winner = self.candidate("A", 20)
        decision = select_canonical([winner, self.candidate("B", 10)], threshold=5, min_margin=5)
        self.assert_decision(decision, CanonicalState.SELECTED, CanonicalReasonCode.SELECTED)
        self.assertIs(decision.winner_identity, winner.identity)
        self.assertEqual(decision.runner_up_score, 10.0)
        self.assertEqual(decision.observed_margin, 10.0)

    def test_insufficient_margin_abstains(self) -> None:
        decision = select_canonical(
            [self.candidate("A", 20), self.candidate("B", 19)], threshold=5, min_margin=2
        )
        self.assert_decision(decision, CanonicalState.UNRESOLVED, CanonicalReasonCode.INSUFFICIENT_MARGIN)

    def test_top_score_tie_abstains_without_id_tiebreak(self) -> None:
        decision = select_canonical(
            [self.candidate("Z", 10), self.candidate("A", 10)], threshold=5, min_margin=0
        )
        self.assert_decision(decision, CanonicalState.TIE, CanonicalReasonCode.TIE)

    def test_margin_is_inclusive_at_boundary(self) -> None:
        decision = select_canonical(
            [self.candidate("A", 20), self.candidate("B", 18)], threshold=5, min_margin=2
        )
        self.assert_decision(decision, CanonicalState.SELECTED, CanonicalReasonCode.SELECTED)

    def test_runner_up_below_threshold_still_participates_in_margin(self) -> None:
        decision = select_canonical(
            [self.candidate("A", 10), self.candidate("B", 4)], threshold=5, min_margin=7
        )
        self.assert_decision(decision, CanonicalState.UNRESOLVED, CanonicalReasonCode.INSUFFICIENT_MARGIN)
        self.assertEqual(decision.runner_up_score, 4.0)
        self.assertEqual(decision.observed_margin, 6.0)

    def test_reason_codes_have_stable_canonical_state_mapping(self) -> None:
        cases = (
            (select_canonical([], threshold=5, min_margin=1), CanonicalState.UNRESOLVED, CanonicalReasonCode.NO_CANDIDATE),
            (select_canonical([self.candidate("A", 4)], threshold=5, min_margin=1), CanonicalState.UNRESOLVED, CanonicalReasonCode.BELOW_THRESHOLD),
            (select_canonical([self.candidate("A", 10), self.candidate("B", 10)], threshold=5, min_margin=1), CanonicalState.TIE, CanonicalReasonCode.TIE),
            (select_canonical([self.candidate("A", 10), self.candidate("B", 9.5)], threshold=5, min_margin=1), CanonicalState.UNRESOLVED, CanonicalReasonCode.INSUFFICIENT_MARGIN),
            (select_canonical([self.candidate("A", 10)], threshold=5, min_margin=1), CanonicalState.SELECTED, CanonicalReasonCode.SELECTED),
        )
        for decision, state, reason in cases:
            with self.subTest(reason=reason):
                self.assert_decision(decision, state, reason)

    def test_three_way_input_keeps_top_tie_across_order_and_id(self) -> None:
        candidates = [self.candidate("Z", 10), self.candidate("A", 10), self.candidate("C", 5)]
        first = select_canonical(candidates, threshold=5, min_margin=1)
        second = select_canonical(list(reversed(candidates)), threshold=5, min_margin=1)
        self.assertEqual(first, second)
        self.assert_decision(first, CanonicalState.TIE, CanonicalReasonCode.TIE)

    def test_input_order_does_not_change_decision(self) -> None:
        candidates = [self.candidate("A", 20), self.candidate("B", 10), self.candidate("C", 8)]
        first = select_canonical(candidates, threshold=5, min_margin=5)
        second = select_canonical([candidates[2], candidates[0], candidates[1]], threshold=5, min_margin=5)
        self.assertEqual(first, second)

    def test_decision_round_trip_is_stable_and_serializable(self) -> None:
        decision = select_canonical(
            [self.candidate("A", 20), self.candidate("B", 10)], threshold=5, min_margin=5
        )
        self.assertEqual(CanonicalDecision.from_dict(decision.to_dict()), decision)

    def test_inputs_and_decision_are_immutable(self) -> None:
        candidate = self.candidate("A", 20)
        select_canonical([candidate], threshold=5, min_margin=2)
        self.assertEqual(candidate.score, 20.0)
        with self.assertRaises(FrozenInstanceError):
            candidate.score = 1  # type: ignore[misc]

    def test_invalid_scores_and_parameters_are_rejected(self) -> None:
        invalid_scores = (None, "10", True, math.nan, math.inf, -math.inf)
        for score in invalid_scores:
            with self.subTest(score=score), self.assertRaises((TypeError, ValueError)):
                CanonicalCandidate(identity=self.identity("A"), score=score)  # type: ignore[arg-type]
        for name, kwargs in (
            ("threshold_type", {"threshold": "1", "min_margin": 1}),
            ("threshold", {"threshold": math.nan, "min_margin": 1}),
            ("threshold_infinite", {"threshold": math.inf, "min_margin": 1}),
            ("min_margin", {"threshold": 1, "min_margin": -1}),
            ("min_margin_type", {"threshold": 1, "min_margin": "1"}),
            ("min_margin_infinite", {"threshold": 1, "min_margin": math.inf}),
        ):
            with self.subTest(name=name), self.assertRaises((TypeError, ValueError)):
                select_canonical([], **kwargs)

    def test_negative_threshold_is_valid_for_negative_score_scales(self) -> None:
        decision = select_canonical([self.candidate("A", -2)], threshold=-2, min_margin=1)
        self.assert_decision(decision, CanonicalState.SELECTED, CanonicalReasonCode.SELECTED)

    def test_distinct_candidate_ids_may_share_document_id(self) -> None:
        identity_a = DocumentIdentity(process_id="P1", document_id="D1", candidate_id="A")
        identity_b = DocumentIdentity(process_id="P1", document_id="D1", candidate_id="B")
        decision = select_canonical(
            [CanonicalCandidate(identity_a, 10), CanonicalCandidate(identity_b, 10)],
            threshold=5,
            min_margin=1,
        )
        self.assert_decision(decision, CanonicalState.TIE, CanonicalReasonCode.TIE)

    def test_missing_and_duplicate_candidate_ids_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            CanonicalCandidate(identity=DocumentIdentity(process_id="P1", document_id="D1"), score=10)
        duplicate = [self.candidate("A", 10), self.candidate("A", 9)]
        with self.assertRaises(ValueError):
            select_canonical(duplicate, threshold=5, min_margin=1)

    @staticmethod
    def identity(candidate_id: str) -> DocumentIdentity:
        return DocumentIdentity(process_id="P1", document_id=f"D-{candidate_id}", candidate_id=candidate_id)

    @classmethod
    def candidate(cls, candidate_id: str, score: float) -> CanonicalCandidate:
        return CanonicalCandidate(identity=cls.identity(candidate_id), score=score)

    def assert_decision(
        self, decision: CanonicalDecision, state: CanonicalState, reason: CanonicalReasonCode
    ) -> None:
        self.assertIs(decision.canonical_state, state)
        self.assertIs(decision.reason, reason)
        if state is not CanonicalState.SELECTED:
            self.assertIsNone(decision.winner_identity)
            self.assertIsNone(decision.winner_candidate_id)


if __name__ == "__main__":
    unittest.main()
