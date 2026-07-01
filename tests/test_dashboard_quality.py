from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.dashboard_data import empty_dataframe
from backend.app.services.dashboard_portfolio import build_ted_dataframe
from backend.app.services.dashboard_quality import field_coverage, is_chartable_dimension


def _bundle(**overrides: Any) -> dict[str, Any]:
    base = {
        "ted_normalized": empty_dataframe([]),
    }
    base.update(overrides)
    return base


class DashboardQualityTests(unittest.TestCase):
    def test_field_coverage_counts_absence_without_inventing_values(self) -> None:
        df = pd.DataFrame(
            [
                {"parceiro": "Parceiro A", "possui_pt": True},
                {"parceiro": "", "possui_pt": False},
                {"parceiro": "Parceiro C", "possui_pt": True},
            ]
        )

        coverage = field_coverage(df, ["parceiro", "possui_pt", "objeto_completo"])
        by_field = {row["campo"]: row for row in coverage.to_dict(orient="records")}

        self.assertEqual(by_field["parceiro"]["preenchidos"], 2)
        self.assertEqual(by_field["possui_pt"]["preenchidos"], 2)
        self.assertEqual(by_field["objeto_completo"]["preenchidos"], 0)

    def test_dirty_long_or_low_coverage_dimensions_are_blocked(self) -> None:
        low_coverage = pd.DataFrame({"unidade": ["Unidade A", "", ""]})
        self.assertFalse(is_chartable_dimension(low_coverage, "unidade")["allowed"])

        long_label = pd.DataFrame({"unidade": ["Unidade com nome extremamente longo que nao deve virar rotulo de grafico"]})
        long_check = is_chartable_dimension(long_label, "unidade")
        self.assertFalse(long_check["allowed"])
        self.assertIn("rotulos", long_check["reason"])

        raw_text = pd.DataFrame({"unidade": ["Documento assinado pela autoridade responsavel pelo acompanhamento"]})
        raw_check = is_chartable_dimension(raw_text, "unidade")
        self.assertFalse(raw_check["allowed"])
        self.assertIn("conteudo documental bruto", raw_check["reason"])

    def test_ted_missing_value_deadline_or_unit_is_quality_issue(self) -> None:
        ted_df = pd.DataFrame(
            [
                {
                    "processo": "60090.000001/2026-01",
                    "numero_ted": "1",
                    "ano_ted": "2026",
                    "objeto": "Objeto TED",
                    "valor_global": "",
                    "vigencia_inicio": "2026-01-01",
                    "vigencia_fim": "",
                    "unidade_descentralizadora": "",
                    "unidade_descentralizada": "Unidade B",
                }
            ]
        )

        result = build_ted_dataframe(_bundle(ted_normalized=ted_df), today=date(2026, 1, 1))

        self.assertEqual(result.iloc[0]["indicador_vigencia"], "sem_data")
        self.assertIn("valor_global", result.iloc[0]["campos_ausentes"])
        self.assertIn("vigencia_fim", result.iloc[0]["campos_ausentes"])
        self.assertIn("unidade_descentralizadora", result.iloc[0]["campos_ausentes"])


if __name__ == "__main__":
    unittest.main()
