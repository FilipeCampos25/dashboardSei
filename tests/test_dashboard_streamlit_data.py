from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.dashboard_streamlit_data import (
    explode_pt_acoes,
    explode_pt_metas,
    load_dashboard_bundle,
    memorando_detail_dataframe,
    parse_act_rejection_summary,
    pt_process_metrics,
    runtime_for_processes,
    summarize_log_entries,
    ted_detail_dataframe,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_obj:
        for row in rows:
            file_obj.write(json.dumps(row, ensure_ascii=False) + "\n")


class DashboardStreamlitDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root_dir = (Path(__file__).resolve().parent / "_tmp_dashboard_streamlit_data").resolve()
        shutil.rmtree(self.root_dir, ignore_errors=True)
        (self.root_dir / "backend" / "output").mkdir(parents=True, exist_ok=True)
        (self.root_dir / "output").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root_dir, ignore_errors=True)

    def test_load_dashboard_bundle_reads_real_artifacts(self) -> None:
        backend_output = self.root_dir / "backend" / "output"
        _write_csv(
            backend_output / "dashboard_ready_latest.csv",
            [
                "processo",
                "preview_parceiro",
                "preview_numero_act",
                "preview_objeto",
                "preview_vigencia",
                "pt_gold",
                "pt_json_path",
                "pt_vigencia_inicio",
                "pt_vigencia_fim",
                "pt_quality",
                "act_gold",
                "act_json_path",
                "act_numero_acordo",
                "act_data_inicio_vigencia",
                "act_data_fim_vigencia",
                "act_orgao_convenente",
                "act_objeto",
                "act_quality",
                "has_process_mismatch",
                "source_act_objeto",
                "source_act_parceiro",
                "memorando_gold",
                "memorando_json_path",
                "ted_quality",
                "ted_gold",
                "ted_json_path",
                "ted_objeto",
                "ted_valor_global",
                "ted_situacao",
                "ted_uf",
                "quality_status",
                "quality_notes",
                "act_attempts_count",
                "act_rejection_summary",
            ],
            [
                {
                    "processo": "60090.000001/2026-01",
                    "preview_parceiro": "Parceiro A",
                    "preview_numero_act": "1/2026",
                    "preview_objeto": "Objeto A",
                    "preview_vigencia": "60 meses",
                    "pt_gold": "True",
                    "pt_json_path": "x.json",
                    "pt_vigencia_inicio": "2026-01-01",
                    "pt_vigencia_fim": "2026-12-31",
                    "pt_quality": "gold",
                    "act_gold": "False",
                    "act_json_path": "",
                    "act_numero_acordo": "",
                    "act_data_inicio_vigencia": "",
                    "act_data_fim_vigencia": "",
                    "act_orgao_convenente": "Parceiro A",
                    "act_objeto": "Objeto A",
                    "act_quality": "silver_only",
                    "has_process_mismatch": "False",
                    "source_act_objeto": "preview_fallback",
                    "source_act_parceiro": "preview_fallback",
                    "memorando_gold": "False",
                    "memorando_json_path": "",
                    "ted_quality": "not_found",
                    "ted_gold": "False",
                    "ted_json_path": "",
                    "ted_objeto": "",
                    "ted_valor_global": "",
                    "ted_situacao": "",
                    "ted_uf": "",
                    "quality_status": "medium",
                    "quality_notes": "act=silver_only",
                    "act_attempts_count": "2",
                    "act_rejection_summary": "extrato:cabecalho_extrato(1)",
                }
            ],
        )
        _write_json(
            backend_output / "performance_analysis.json",
            {
                "total_execution_time": 120.0,
                "spans": {
                    "processo:60090.000001/2026-01": {"total_seconds": 30.0},
                    "processo:60090.000002/2026-02": {"total_seconds": 45.0},
                },
            },
        )
        _write_jsonl(
            self.root_dir / "output" / "execution_log_latest.json",
            [
                {"timestamp": "2026-04-15T10:00:00", "level": "INFO", "message": "inicio"},
                {"timestamp": "2026-04-15T10:00:01", "level": "WARNING", "message": "aviso"},
                {"timestamp": "2026-04-15T10:00:02", "level": "ERROR", "message": "erro"},
            ],
        )

        bundle = load_dashboard_bundle(self.root_dir)
        overview_df = bundle["overview"]

        self.assertEqual(len(overview_df), 1)
        self.assertTrue(bool(overview_df.iloc[0]["pt_gold"]))
        self.assertEqual(int(overview_df.iloc[0]["act_attempts_count"]), 2)

        log_summary = summarize_log_entries(bundle["log_entries"])
        self.assertEqual(log_summary["info"], 1)
        self.assertEqual(log_summary["warning"], 1)
        self.assertEqual(log_summary["error"], 1)

        runtime = runtime_for_processes(bundle["performance"], ["60090.000001/2026-01"])
        self.assertEqual(runtime["total_seconds"], 30.0)
        self.assertEqual(runtime["avg_seconds"], 30.0)

    def test_load_dashboard_bundle_tolerates_missing_files(self) -> None:
        bundle = load_dashboard_bundle(self.root_dir)

        self.assertTrue(bundle["overview"].empty)
        self.assertTrue(bundle["pt_status"].empty)
        self.assertTrue(bundle["act_status"].empty)
        self.assertEqual(bundle["log_entries"], [])
        self.assertEqual(bundle["performance"], {})

    def test_pt_explosions_break_out_metas_and_acoes(self) -> None:
        pt_df = pd.DataFrame(
            [
                {
                    "processo": "60090.000572/2024-34",
                    "parceiro": "UFAM",
                    "metas_raw": "1 | Meta A || 2 | Meta B",
                    "acoes_raw": (
                        "1 | Acao A | Entrega A | Equipe A | janeiro/2025 - fevereiro/2025 || "
                        "Acao B | Entrega B | Time B | marco/2025 - abril/2025"
                    ),
                }
            ]
        )

        metas_df = explode_pt_metas(pt_df)
        acoes_df = explode_pt_acoes(pt_df)
        metrics_df = pt_process_metrics(pt_df)

        self.assertEqual(len(metas_df), 2)
        self.assertEqual(metas_df.iloc[0]["meta_ref"], "1")
        self.assertEqual(len(acoes_df), 2)
        self.assertEqual(acoes_df.iloc[0]["acao_ref"], "1")
        self.assertEqual(acoes_df.iloc[0]["responsavel"], "Equipe A")
        self.assertIn("janeiro/2025", acoes_df.iloc[0]["periodo_raw"])
        self.assertEqual(int(metrics_df.iloc[0]["metas_count"]), 2)
        self.assertEqual(int(metrics_df.iloc[0]["acoes_count"]), 2)

    def test_parse_act_rejection_summary_aggregates_entries(self) -> None:
        overview_df = pd.DataFrame(
            [
                {
                    "processo": "60090.001393/2025-03",
                    "act_rejection_summary": "not_found_after_filter_and_tree:not_found(1) | extrato:cabecalho_extrato(2)",
                }
            ]
        )

        rejections_df = parse_act_rejection_summary(overview_df)

        self.assertEqual(len(rejections_df), 2)
        self.assertEqual(int(rejections_df["count"].sum()), 3)
        self.assertIn("extrato:cabecalho_extrato", rejections_df["rejection"].tolist())

    def test_memorando_and_ted_detail_dataframes_read_json_payloads(self) -> None:
        backend_output = self.root_dir / "backend" / "output"
        memorando_json = backend_output / "memorando_entendimentos_60091.000060_2023-87.json"
        ted_json = backend_output / "termo_execucao_descentralizada_60090.000004_2026-04.json"

        _write_csv(
            backend_output / "memorando_normalizado_latest.csv",
            ["processo", "documento", "snapshot_mode", "json_path"],
            [
                {
                    "processo": "60091.000060/2023-87",
                    "documento": "60091.000060/2023-87",
                    "snapshot_mode": "html_dom",
                    "json_path": str(memorando_json),
                }
            ],
        )
        _write_json(
            memorando_json,
            {"snapshot": {"text": "Memorando de Entendimentos com objetivo de cooperacao tecnica detalhada."}},
        )

        _write_csv(
            backend_output / "ted_normalizado_latest.csv",
            ["processo", "json_path"],
            [{"processo": "60090.000004/2026-04", "json_path": str(ted_json)}],
        )
        _write_json(
            ted_json,
            {
                "snapshot": {
                    "api_payload": {
                        "objeto": "Execucao descentralizada de atividades",
                        "valor_global": "1500000.00",
                        "situacao": "Em execucao",
                        "uf": "DF",
                    }
                }
            },
        )

        bundle = load_dashboard_bundle(self.root_dir)
        memorando_df = memorando_detail_dataframe(bundle)
        ted_df = ted_detail_dataframe(bundle)

        self.assertEqual(len(memorando_df), 1)
        self.assertIn("Memorando de Entendimentos", memorando_df.iloc[0]["excerpt"])
        self.assertEqual(len(ted_df), 1)
        self.assertEqual(ted_df.iloc[0]["objeto"], "Execucao descentralizada de atividades")
        self.assertEqual(ted_df.iloc[0]["situacao"], "Em execucao")
        self.assertEqual(ted_df.iloc[0]["uf"], "DF")


if __name__ == "__main__":
    unittest.main()
