from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pipeline_states import (
    AccessState,
    AcquisitionState,
    DiscoveryState,
    ExtractionState,
    OpeningState,
)


class PipelineStatesTests(unittest.TestCase):
    def test_enum_values_are_stable_and_complete(self) -> None:
        self.assertEqual([state.value for state in DiscoveryState], ["NOT_SEARCHED", "NOT_FOUND", "FOUND"])
        self.assertEqual(
            [state.value for state in OpeningState],
            ["NOT_ATTEMPTED", "OPENED", "OPEN_FAILED", "TIMEOUT"],
        )
        self.assertEqual(
            [state.value for state in AccessState],
            ["UNKNOWN", "ACCESSIBLE", "ACCESS_RESTRICTED", "IFRAME_UNAVAILABLE"],
        )
        self.assertEqual(
            [state.value for state in ExtractionState],
            ["NOT_ATTEMPTED", "EXTRACTED", "CONTENT_PARTIAL", "EMPTY_CONTENT", "EXTRACTION_FAILED"],
        )

    def test_round_trip_is_deterministic(self) -> None:
        expected = {
            "discovery": "FOUND",
            "opening": "OPENED",
            "access": "ACCESSIBLE",
            "extraction": "CONTENT_PARTIAL",
        }
        state = AcquisitionState.from_dict(expected)
        self.assertEqual(state.to_dict(), expected)
        self.assertEqual(AcquisitionState.from_dict(state.to_dict()), state)

    def test_unknown_enum_value_is_rejected(self) -> None:
        payload = self._payload()
        payload["access"] = "UNRECOGNIZED"
        with self.assertRaises(ValueError):
            AcquisitionState.from_dict(payload)

    def test_valid_states(self) -> None:
        valid_states = (
            (DiscoveryState.NOT_SEARCHED, OpeningState.NOT_ATTEMPTED, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.NOT_FOUND, OpeningState.NOT_ATTEMPTED, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.NOT_ATTEMPTED, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EXTRACTION_FAILED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESS_RESTRICTED, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.IFRAME_UNAVAILABLE, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EMPTY_CONTENT),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.CONTENT_PARTIAL),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EXTRACTED),
        )
        for values in valid_states:
            with self.subTest(values=values):
                AcquisitionState(*values)

    def test_invalid_states(self) -> None:
        invalid_states = (
            (DiscoveryState.NOT_FOUND, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EXTRACTED),
            (DiscoveryState.NOT_SEARCHED, OpeningState.OPENED, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPEN_FAILED, AccessState.ACCESSIBLE, ExtractionState.NOT_ATTEMPTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.ACCESS_RESTRICTED, ExtractionState.EXTRACTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.IFRAME_UNAVAILABLE, ExtractionState.EXTRACTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.UNKNOWN, ExtractionState.EXTRACTED),
            (DiscoveryState.FOUND, OpeningState.OPENED, AccessState.IFRAME_UNAVAILABLE, ExtractionState.EMPTY_CONTENT),
            (DiscoveryState.NOT_SEARCHED, OpeningState.NOT_ATTEMPTED, AccessState.UNKNOWN, ExtractionState.EXTRACTION_FAILED),
        )
        for values in invalid_states:
            with self.subTest(values=values), self.assertRaises(ValueError):
                AcquisitionState(*values)

    def test_found_does_not_infer_later_states(self) -> None:
        state = AcquisitionState(
            discovery=DiscoveryState.FOUND,
            opening=OpeningState.NOT_ATTEMPTED,
            access=AccessState.UNKNOWN,
            extraction=ExtractionState.NOT_ATTEMPTED,
        )
        self.assertEqual(state.opening, OpeningState.NOT_ATTEMPTED)
        self.assertEqual(state.access, AccessState.UNKNOWN)
        self.assertEqual(state.extraction, ExtractionState.NOT_ATTEMPTED)

    def test_constructor_requires_enum_instances(self) -> None:
        with self.assertRaises(TypeError):
            AcquisitionState(
                discovery="FOUND",  # type: ignore[arg-type]
                opening=OpeningState.NOT_ATTEMPTED,
                access=AccessState.UNKNOWN,
                extraction=ExtractionState.NOT_ATTEMPTED,
            )

    @staticmethod
    def _payload() -> dict[str, str]:
        return {
            "discovery": "FOUND",
            "opening": "OPENED",
            "access": "ACCESSIBLE",
            "extraction": "EXTRACTED",
        }


if __name__ == "__main__":
    unittest.main()
