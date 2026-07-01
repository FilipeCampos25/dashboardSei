from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.dashboard_data import empty_dataframe
from backend.app.services.dashboard_portfolio import (
    SITUACAO_ATIVA,
    SITUACAO_REVISAR,
    build_dashboard_model,
    build_pt_dataframe,
)


def _bundle(**overrides: Any) -> dict[str, Any]:
    base = {
        "overview": empty_dataframe([]),
        "ted_normalized": empty_dataframe([]),
        "pt_audit": empty_dataframe([]),
        "memorando_normalized": empty_dataframe([]),
        "admin_normalized": empty_dataframe([]),
        "parcerias_descontinuadas": empty_dataframe([]),
        "divergence": empty_dataframe([]),
        "collection_meta": {"data_ultima_coleta": "2026-06-16T13:20:39"},
    }
    base.update(overrides)
    return base


def _overview_row(processo: str, fim: str, *, numero: str = "ACT 1/2026") -> dict[str, Any]:
    return {
        "processo": processo,
        "best_numero_acordo": numero,
        "best_parceiro": "Parceiro A",
        "best_objeto": "Objeto de cooperacao tecnica com detalhamento suficiente",
        "best_vigencia_inicio": "2026-01-01",
        "best_vigencia_fim": fim,
        "act_quality": "gold",
        "quality_status": "high",
        "pt_gold": False,
        "ted_gold": False,
        "memorando_gold": False,
        "has_process_mismatch": False,
    }


class DashboardPortfolioTests(unittest.TestCase):
    def test_history_does_not_contaminate_active_deadline_alerts(self) -> None:
        today = date(2026, 1, 1)
        active_ok = _overview_row("60090.000001/2026-01", "2027-01-02")
        active_conflict = _overview_row("60090.000002/2026-02", "2026-01-01", numero="ACT 2/2026")
        history = {
            "processo": "60090.000002/2026-02",
            "tipo": "ACT",
            "numero_act": "ACT 2/2026",
            "parceiro": "Parceiro A",
            "objeto": "Historico encerrado",
            "data_vencimento": "2026-01-01",
            "status_normalizado": "Encerrado",
            "status_categoria": "encerrado",
        }

        model = build_dashboard_model(
            _bundle(
                overview=pd.DataFrame([active_ok, active_conflict]),
                parcerias_descontinuadas=pd.DataFrame([history]),
            ),
            today=today,
        )

        self.assertEqual(len(model["active"]), 1)
        self.assertEqual(model["active"].iloc[0]["processo"], "60090.000001/2026-01")
        self.assertEqual(model["active"].iloc[0]["indicador_vigencia"], "verde")
        self.assertNotIn("vermelho", model["priorities"]["indicador_vigencia"].tolist())

        conflict_row = model["portfolio"][model["portfolio"]["processo"] == "60090.000002/2026-02"].iloc[0]
        self.assertEqual(conflict_row["situacao_carteira"], SITUACAO_REVISAR)
        self.assertIn("processo_tambem_no_historico", conflict_row["conflitos"])

    def test_duplicate_canonical_key_is_marked_for_review(self) -> None:
        row_a = _overview_row("60090.000010/2026-10", "2027-01-02", numero="ACT 10/2026")
        row_b = _overview_row("60090.000010/2026-10", "2027-01-02", numero="ACT 10/2026")

        model = build_dashboard_model(_bundle(overview=pd.DataFrame([row_a, row_b])), today=date(2026, 1, 1))

        self.assertEqual(len(model["portfolio"]), 1)
        self.assertEqual(model["portfolio"].iloc[0]["situacao_carteira"], SITUACAO_REVISAR)
        self.assertIn("duplicidade_canonica=2", model["portfolio"].iloc[0]["conflitos"])
        self.assertEqual(len(model["active"]), 0)

    def test_pt_without_metas_acoes_or_prazo_is_explicit(self) -> None:
        pt_df = pd.DataFrame(
            [
                {
                    "processo": "60090.000003/2026-03",
                    "documento": "PT",
                    "parceiro": "Parceiro PT",
                    "objeto": "Plano de trabalho sem estrutura",
                    "vigencia_fim": "2027-01-02",
                    "metas_raw": "",
                    "acoes_raw": "",
                    "prazo_fim": "",
                }
            ]
        )

        result = build_pt_dataframe(_bundle(pt_audit=pt_df), today=date(2026, 1, 1))

        self.assertEqual(len(result), 1)
        self.assertFalse(bool(result.iloc[0]["possui_metas"]))
        self.assertFalse(bool(result.iloc[0]["possui_acoes"]))
        self.assertFalse(bool(result.iloc[0]["possui_prazo"]))
        self.assertEqual(result.iloc[0]["indicador_prazo_pt"], "sem_data")

    def test_active_record_without_history_remains_active(self) -> None:
        model = build_dashboard_model(
            _bundle(overview=pd.DataFrame([_overview_row("60090.000004/2026-04", "2027-01-02")])),
            today=date(2026, 1, 1),
        )

        self.assertEqual(model["portfolio"].iloc[0]["situacao_carteira"], SITUACAO_ATIVA)
        self.assertEqual(len(model["active"]), 1)

    def test_missing_fields_use_canonical_names(self) -> None:
        row = _overview_row("60090.000005/2026-05", "", numero="")
        row["best_parceiro"] = ""
        row["best_objeto"] = ""

        model = build_dashboard_model(_bundle(overview=pd.DataFrame([row])), today=date(2026, 1, 1))

        missing = model["portfolio"].iloc[0]["campos_ausentes"]
        self.assertIn("parceiro", missing)
        self.assertIn("objeto", missing)
        self.assertIn("vigencia_fim", missing)
        self.assertIn("documento_principal_numero", missing)
        self.assertNotIn("objeto_completo", missing)


if __name__ == "__main__":
    unittest.main()
