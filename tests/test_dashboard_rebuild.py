from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.data_sources import empty_dataframe, load_dashboard_bundle, read_csv_safe, read_json_safe
from dashboard.historical_partnerships import build_historical_partnerships, display_table as history_display_table, history_metrics
from dashboard.partnerships_active import active_metrics, build_active_partnerships, filter_active_partnerships
from dashboard.ted_metrics import build_teds, chartable_dimension, filter_teds, ted_metrics
from dashboard.vigencia_rules import classify_vigencia, days_remaining


def _bundle(**overrides: Any) -> dict[str, Any]:
    base = {
        "parcerias_vigentes": empty_dataframe([]),
        "dashboard_ready": empty_dataframe([]),
        "pt_normalized": empty_dataframe([]),
        "pt_audit": empty_dataframe([]),
        "act_normalized": empty_dataframe([]),
        "memorando_normalized": empty_dataframe([]),
        "admin_normalized": empty_dataframe([]),
        "ted_normalized": empty_dataframe([]),
        "history_normalized": empty_dataframe([]),
        "history_raw": empty_dataframe([]),
        "collection_meta": {"data_ultima_coleta": "2026-06-16T13:20:39"},
    }
    base.update(overrides)
    return base


class VigenciaRulesTests(unittest.TestCase):
    def test_boundaries_are_centralized_contract(self) -> None:
        today = date(2026, 1, 1)
        cases = [
            ("", "sem_data", None),
            ("not a date", "sem_data", None),
            ("2025-12-31", "vermelho", -1),
            ("2026-01-01", "vermelho", 0),
            ("2026-06-30", "vermelho", 180),
            ("2026-07-01", "amarelo", 181),
            ("2027-01-01", "amarelo", 365),
            ("2027-01-02", "verde", 366),
        ]

        for value, expected_status, expected_days in cases:
            with self.subTest(value=value):
                self.assertEqual(classify_vigencia(value, today=today), expected_status)
                self.assertEqual(days_remaining(value, today=today), expected_days)


class DashboardRebuildModelTests(unittest.TestCase):
    def test_active_partnerships_ignore_ted_only_rows_and_keep_history_conflict_as_detail(self) -> None:
        bundle = _bundle(
            parcerias_vigentes=pd.DataFrame(
                [
                    {
                        "interno_descricao": "PARCERIAS VIGENTES",
                        "seq": "1",
                        "processo": "60090.000001/2026-01",
                        "parceiro": "Parceiro preview",
                        "vigencia": "60 meses",
                        "numero_act": "1/2026",
                        "objeto": "Objeto preview",
                    }
                ]
            ),
            dashboard_ready=pd.DataFrame(
                [
                    {
                        "processo": "60090.000001/2026-01",
                        "best_numero_acordo": "1/2026",
                        "best_parceiro": "Parceiro consolidado",
                        "best_objeto": "Objeto consolidado",
                        "best_vigencia_inicio": "2026-01-01",
                        "best_vigencia_fim": "2027-01-02",
                        "best_vigencia_source": "act_gold",
                    },
                    {
                        "processo": "60090.000002/2026-02",
                        "best_numero_acordo": "",
                        "best_objeto": "TED fora de parcerias vigentes",
                    },
                ]
            ),
            ted_normalized=pd.DataFrame(
                [
                    {
                        "processo": "60090.000002/2026-02",
                        "documento": "TED",
                        "numero_ted": "2",
                        "ano_ted": "2026",
                        "valor_global": "1000.00",
                    }
                ]
            ),
            history_normalized=pd.DataFrame(
                [
                    {
                        "processo": "60090.000001/2026-01",
                        "tipo": "ACT",
                        "status_categoria": "encerrado",
                        "status_normalizado": "Encerrado",
                    }
                ]
            ),
        )

        active_df = build_active_partnerships(bundle, today=date(2026, 1, 1))

        self.assertEqual(active_metrics(active_df)["total"], 1)
        self.assertEqual(active_df.iloc[0]["processo"], "60090.000001/2026-01")
        self.assertNotIn("60090.000002/2026-02", active_df["processo"].tolist())
        self.assertIn("processo_tambem_no_historico", active_df.iloc[0]["conflitos"])

    def test_ted_values_are_summed_only_when_valid_and_coverage_is_explicit(self) -> None:
        ted_df = build_teds(
            _bundle(
                ted_normalized=pd.DataFrame(
                    [
                        {"processo": "60090.000001/2026-01", "documento": "A", "valor_global": "100.00", "vigencia_fim": "2027-01-02"},
                        {"processo": "60090.000002/2026-02", "documento": "B", "valor_global": "", "vigencia_fim": ""},
                        {"processo": "60090.000003/2026-03", "documento": "C", "valor_global": "abc", "vigencia_fim": ""},
                        {"processo": "60090.000004/2026-04", "documento": "D", "valor_global": "0.00", "vigencia_fim": ""},
                    ]
                )
            ),
            today=date(2026, 1, 1),
        )

        metrics = ted_metrics(ted_df)

        self.assertEqual(metrics["total"], 4)
        self.assertEqual(metrics["valor_validos"], 1)
        self.assertEqual(metrics["valor_total"], 100.0)

    def test_historical_records_do_not_have_deadline_alert_columns(self) -> None:
        hist_df = build_historical_partnerships(
            _bundle(
                history_normalized=pd.DataFrame(
                    [
                        {
                            "processo": "60090.000010/2020-10",
                            "tipo": "ACT",
                            "numero_act": "1/2020",
                            "parceiro": "Parceiro",
                            "objeto": "Objeto historico",
                            "status_categoria": "vigente_em_descontinuadas",
                            "status_normalizado": "Vigente",
                            "data_vencimento": "2025-01-01",
                        }
                    ]
                )
            )
        )

        self.assertNotIn("dias_restantes", hist_df.columns)
        self.assertNotIn("situacao_vigencia", hist_df.columns)
        self.assertNotIn("Dias Restantes", history_display_table(hist_df).columns)
        self.assertEqual(history_metrics(hist_df)["inconsistentes"], 1)

    def test_missing_empty_and_invalid_sources_are_tolerated(self) -> None:
        root = Path.cwd() / "_tmp_dashboard_rebuild" / uuid.uuid4().hex
        try:
            backend_output = root / "backend" / "output"
            backend_output.mkdir(parents=True)
            (root / "output").mkdir(parents=True)
            malformed = backend_output / "bad.csv"
            malformed.write_text('processo,objeto\n"60090.000001/2026-01,sem fechamento', encoding="utf-8")
            invalid_json = backend_output / "bad.json"
            invalid_json.write_text("{not-json", encoding="utf-8")
            (root / "output" / "execution_log_latest.json").write_text(
                json.dumps({"timestamp": "2026-06-16T13:20:39"}) + "\n",
                encoding="utf-8",
            )

            self.assertTrue(read_csv_safe(malformed, ["processo", "objeto"]).empty)
            self.assertEqual(read_json_safe(invalid_json), {})
            bundle = load_dashboard_bundle(root)
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertTrue(bundle["parcerias_vigentes"].empty)
        self.assertEqual(bundle["collection_meta"]["data_ultima_coleta"], "2026-06-16T13:20:39")
        self.assertEqual(bundle["collection_meta"]["data_ultima_coleta_display"], "16/06/2026 13:20")

    def test_deduplication_is_scoped_to_each_category(self) -> None:
        bundle = _bundle(
            parcerias_vigentes=pd.DataFrame(
                [
                    {"seq": "1", "processo": "60090.000001/2026-01", "numero_act": "1/2026", "parceiro": "", "objeto": ""},
                    {"seq": "2", "processo": "60090.000001/2026-01", "numero_act": "1/2026", "parceiro": "Parceiro", "objeto": "Objeto"},
                ]
            ),
            ted_normalized=pd.DataFrame(
                [
                    {"processo": "60090.000001/2026-01", "documento": "TED", "numero_ted": "1", "ano_ted": "2026", "valor_global": ""},
                    {"processo": "60090.000001/2026-01", "documento": "TED", "numero_ted": "1", "ano_ted": "2026", "valor_global": "100.00"},
                ]
            ),
            history_normalized=pd.DataFrame(
                [
                    {"processo": "60090.000001/2026-01", "tipo": "ACT", "numero_act": "1/2026", "status_categoria": "encerrado"},
                    {"processo": "60090.000001/2026-01", "tipo": "ACT", "numero_act": "1/2026", "status_categoria": "encerrado", "objeto": "Objeto"},
                ]
            ),
        )

        active_df = build_active_partnerships(bundle)
        ted_df = build_teds(bundle)
        hist_df = build_historical_partnerships(bundle)

        self.assertEqual(len(active_df), 1)
        self.assertEqual(len(ted_df), 1)
        self.assertEqual(len(hist_df), 1)
        self.assertIn("duplicidade_interna=2", active_df.iloc[0]["conflitos"])
        self.assertIn("duplicidade_interna=2", ted_df.iloc[0]["conflitos"])
        self.assertIn("duplicidade_interna=2", hist_df.iloc[0]["conflitos"])

    def test_dirty_dimension_is_blocked_for_ted_chart(self) -> None:
        low_coverage = pd.DataFrame({"unidade_descentralizadora": ["Unidade A", "", ""]})
        self.assertFalse(chartable_dimension(low_coverage, "unidade_descentralizadora")["allowed"])

        raw_text = pd.DataFrame({"unidade_descentralizadora": ["Nome da Autoridade Competente: Fulano Responsável pelo Acompanhamento"]})
        check = chartable_dimension(raw_text, "unidade_descentralizadora")
        self.assertFalse(check["allowed"])
        self.assertIn("conteúdo documental bruto", check["reason"])

    def test_long_objects_are_summarized_without_losing_detail(self) -> None:
        long_object = ("Objeto " + ("muito detalhado " * 30)).strip()
        active_df = build_active_partnerships(
            _bundle(
                parcerias_vigentes=pd.DataFrame(
                    [
                        {
                            "seq": "1",
                            "processo": "60090.000001/2026-01",
                            "numero_act": "1/2026",
                            "parceiro": "Parceiro",
                            "objeto": long_object,
                        }
                    ]
                )
            )
        )

        self.assertLess(len(active_df.iloc[0]["objeto_resumo"]), len(long_object))
        self.assertEqual(active_df.iloc[0]["objeto_completo"], long_object)

    def test_filters_do_not_mix_categories(self) -> None:
        active_df = build_active_partnerships(
            _bundle(
                parcerias_vigentes=pd.DataFrame(
                    [{"seq": "1", "processo": "60090.000001/2026-01", "parceiro": "Parceiro A", "objeto": "Objeto A"}]
                )
            )
        )
        ted_df = build_teds(
            _bundle(
                ted_normalized=pd.DataFrame(
                    [{"processo": "60090.000002/2026-02", "documento": "TED", "numero_ted": "2", "ano_ted": "2026", "objeto": "Objeto B"}]
                )
            )
        )

        self.assertTrue(filter_active_partnerships(active_df, query="Objeto B").empty)
        self.assertTrue(filter_teds(ted_df, query="Objeto A").empty)


if __name__ == "__main__":
    unittest.main()
