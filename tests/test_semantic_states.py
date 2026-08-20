from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pipeline_states import AccessState, AcquisitionState, DiscoveryState, ExtractionState, OpeningState
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


class SemanticStatesTests(unittest.TestCase):
    def test_enum_values_are_stable_and_complete(self) -> None:
        expected = {
            ClassificationState: ["NOT_CLASSIFIED", "CANDIDATE", "CONFIRMED", "RELATED", "REJECTED", "AMBIGUOUS"],
            DocumentFunctionState: ["NOT_EVALUATED", "INSTRUMENT", "RELATED", "AMBIGUOUS", "INELIGIBLE"],
            AffinityState: ["NOT_EVALUATED", "MATCHED", "MISMATCHED", "AMBIGUOUS"],
            CanonicalState: ["NOT_EVALUATED", "SELECTED", "UNRESOLVED", "TIE", "INELIGIBLE"],
            PublicationState: ["NOT_EVALUATED", "RETAINED", "PUBLISHED", "BLOCKED"],
        }
        for enum_type, values in expected.items():
            with self.subTest(enum_type=enum_type.__name__):
                self.assertEqual([state.value for state in enum_type], values)

    def test_round_trip_is_deterministic_and_preserves_family_labels(self) -> None:
        expected = {
            "classification": "CONFIRMED",
            "resolved_class": "act_final",
            "function": "INSTRUMENT",
            "resolved_function": "family_specific_main_instrument",
            "affinity": "MATCHED",
            "canonical": "SELECTED",
            "publication": "PUBLISHED",
        }
        state = SemanticState.from_dict(expected)
        self.assertEqual(state.to_dict(), expected)
        self.assertEqual(SemanticState.from_dict(state.to_dict()), state)

    def test_unknown_enum_value_is_rejected(self) -> None:
        payload = self._payload()
        payload["canonical"] = "WINNER"
        with self.assertRaises(ValueError):
            SemanticState.from_dict(payload)

    def test_decisions_are_independent(self) -> None:
        not_evaluated = self._state(classification=ClassificationState.CONFIRMED)
        unresolved_retained = self._state(
            classification=ClassificationState.CONFIRMED,
            canonical=CanonicalState.UNRESOLVED,
            publication=PublicationState.RETAINED,
        )
        self.assertEqual(not_evaluated.canonical, CanonicalState.NOT_EVALUATED)
        self.assertEqual(not_evaluated.publication, PublicationState.NOT_EVALUATED)
        self.assertEqual(unresolved_retained.canonical, CanonicalState.UNRESOLVED)
        self.assertEqual(unresolved_retained.publication, PublicationState.RETAINED)

    def test_important_semantic_cases_are_representable(self) -> None:
        cases = (
            self._state(classification=ClassificationState.CANDIDATE),
            self._state(classification=ClassificationState.CONFIRMED),
            self._state(classification=ClassificationState.RELATED, function=DocumentFunctionState.RELATED, publication=PublicationState.RETAINED),
            self._state(affinity=AffinityState.AMBIGUOUS),
            self._state(canonical=CanonicalState.INELIGIBLE),
            self._state(canonical=CanonicalState.TIE),
            self._state(canonical=CanonicalState.UNRESOLVED),
            self._state(classification=ClassificationState.CONFIRMED, canonical=CanonicalState.SELECTED),
            self._state(publication=PublicationState.BLOCKED),
            self._state(publication=PublicationState.NOT_EVALUATED),
        )
        self.assertEqual(len(cases), 10)

    def test_rejected_classification_cannot_be_selected(self) -> None:
        with self.assertRaises(ValueError):
            self._state(classification=ClassificationState.REJECTED, canonical=CanonicalState.SELECTED)

    def test_ineligible_candidate_cannot_be_published(self) -> None:
        with self.assertRaises(ValueError):
            self._state(canonical=CanonicalState.INELIGIBLE, publication=PublicationState.PUBLISHED)

    def test_constructor_requires_enum_instances_and_non_blank_family_labels(self) -> None:
        with self.assertRaises(TypeError):
            self._state(classification="CONFIRMED")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self._state(resolved_class="  ")

    def test_successful_acquisition_does_not_imply_semantic_decisions(self) -> None:
        acquisition = AcquisitionState(
            discovery=DiscoveryState.FOUND,
            opening=OpeningState.OPENED,
            access=AccessState.ACCESSIBLE,
            extraction=ExtractionState.EXTRACTED,
        )
        semantic = self._state()
        self.assertEqual(acquisition.extraction, ExtractionState.EXTRACTED)
        self.assertEqual(semantic.classification, ClassificationState.NOT_CLASSIFIED)
        self.assertEqual(semantic.canonical, CanonicalState.NOT_EVALUATED)
        self.assertEqual(semantic.publication, PublicationState.NOT_EVALUATED)

    @staticmethod
    def _state(**overrides: object) -> SemanticState:
        values = {
            "classification": ClassificationState.NOT_CLASSIFIED,
            "function": DocumentFunctionState.NOT_EVALUATED,
            "affinity": AffinityState.NOT_EVALUATED,
            "canonical": CanonicalState.NOT_EVALUATED,
            "publication": PublicationState.NOT_EVALUATED,
            **overrides,
        }
        return SemanticState(**values)  # type: ignore[arg-type]

    @staticmethod
    def _payload() -> dict[str, object]:
        return SemanticStatesTests._state().to_dict()


if __name__ == "__main__":
    unittest.main()
