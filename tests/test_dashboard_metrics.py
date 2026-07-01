from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.dashboard_metrics import classify_deadline, coverage_rows, days_remaining, parse_money


class DashboardMetricsTests(unittest.TestCase):
    def test_deadline_boundaries_are_project_contract(self) -> None:
        today = date(2026, 1, 1)

        cases = [
            ("", "sem_data"),
            ("nao e data", "sem_data"),
            ("2025-12-31", "vermelho"),
            ("2026-01-01", "vermelho"),
            ("2026-06-30", "vermelho"),
            ("2026-07-01", "amarelo"),
            ("2027-01-01", "amarelo"),
            ("2027-01-02", "verde"),
        ]

        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(classify_deadline(value, today=today), expected)

        self.assertEqual(days_remaining("2026-07-01", today=today), 181)
        self.assertEqual(days_remaining("2027-01-01", today=today), 365)
        self.assertEqual(days_remaining("2027-01-02", today=today), 366)

    def test_parse_money_handles_brazilian_and_numeric_formats(self) -> None:
        self.assertEqual(parse_money("R$ 1.234.567,89"), 1234567.89)
        self.assertEqual(parse_money("1500000.00"), 1500000.00)
        self.assertIsNone(parse_money(""))

    def test_coverage_rows_counts_boolean_true_values(self) -> None:
        coverage = coverage_rows(
            pd.DataFrame(
                [
                    {"parceiro": "A", "possui_pt": True},
                    {"parceiro": "", "possui_pt": False},
                    {"parceiro": "C", "possui_pt": True},
                ]
            ),
            ["parceiro", "possui_pt"],
        )
        by_field = {row["campo"]: row for row in coverage.to_dict(orient="records")}

        self.assertEqual(by_field["parceiro"]["preenchidos"], 2)
        self.assertEqual(by_field["possui_pt"]["preenchidos"], 2)


if __name__ == "__main__":
    unittest.main()
