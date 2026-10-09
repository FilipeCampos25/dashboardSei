from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.temporal_duration import add_inclusive_duration


class InclusiveDurationTests(unittest.TestCase):
    def test_years_are_calendar_based_and_inclusive(self) -> None:
        self.assertEqual(add_inclusive_duration("2025-12-20", 2, "anos"), "2027-12-19")
        self.assertEqual(add_inclusive_duration("2024-02-29", 1, "ano"), "2025-02-27")

    def test_months_are_not_converted_to_thirty_days(self) -> None:
        self.assertEqual(add_inclusive_duration("2024-08-31", 6, "meses"), "2025-02-27")
        self.assertEqual(add_inclusive_duration("2023-08-31", 6, "meses"), "2024-02-28")

    def test_days_preserve_inclusive_semantics(self) -> None:
        self.assertEqual(add_inclusive_duration("2025-12-20", 10, "dias"), "2025-12-29")

    def test_invalid_input_does_not_fabricate_a_date(self) -> None:
        self.assertEqual(add_inclusive_duration("", 12, "meses"), "")
        self.assertEqual(add_inclusive_duration("2025-01-01", 0, "meses"), "")
        self.assertEqual(add_inclusive_duration("not-a-date", 12, "meses"), "")


if __name__ == "__main__":
    unittest.main()
