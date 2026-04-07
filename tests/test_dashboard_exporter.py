from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.dashboard_exporter import export_dashboard_ready_csv


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class DashboardExporterTests(unittest.TestCase):
    def test_export_dashboard_ready_uses_preview_as_base_and_only_gold_for_enrichment(self) -> None:
        output_dir = Path.cwd() / "tests" / "_tmp_dashboard_exporter"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            preview_rows = []
            for index in range(1, 23):
                processo = f"60090.0000{index:02d}/2026-{index:02d}"
                preview_rows.append(
                    {
                        "interno_descricao": "PARCERIAS VIGENTES",
                        "seq": str(index),
                        "processo": processo,
                        "parceiro": f"PARCEIRO {index}",
                        "vigencia": "60 meses",
                        "numero_act": f"{index}/2026",
                        "objeto": f"Objeto preview {index}",
                    }
                )
            _write_csv(
                output_dir / "parcerias_vigentes_latest.csv",
                ["interno_descricao", "seq", "processo", "parceiro", "vigencia", "numero_act", "objeto"],
                preview_rows,
            )

            _write_csv(
                output_dir / "pt_auditoria_latest.csv",
                [
                    "processo",
                    "publication_status",
                    "captured_focus_fields",
                    "vigencia_inicio",
                    "vigencia_fim",
                    "objeto",
                    "json_path",
                ],
                [
                    {
                        "processo": "60090.000001/2026-01",
                        "publication_status": "published_gold",
                        "captured_focus_fields": "6",
                        "vigencia_inicio": "2026-01-01",
                        "vigencia_fim": "2026-12-31",
                        "objeto": "Objeto PT gold",
                        "json_path": "output/plano_trabalho_60090.000001_2026-01.json",
                    },
                    {
                        "processo": "60090.000002/2026-02",
                        "publication_status": "retained_silver",
                        "captured_focus_fields": "3",
                        "vigencia_inicio": "",
                        "vigencia_fim": "",
                        "objeto": "",
                        "json_path": "output/plano_trabalho_60090.000002_2026-02.json",
                    },
                ],
            )

            _write_csv(
                output_dir / "act_classificacao_latest.csv",
                [
                    "processo",
                    "publication_status",
                    "canonical_score",
                    "numero_acordo",
                    "data_inicio_vigencia",
                    "data_fim_vigencia",
                    "orgao_convenente",
                    "objeto",
                    "validation_warning",
                    "json_path",
                ],
                [
                    {
                        "processo": "60090.000001/2026-01",
                        "publication_status": "published_gold",
                        "canonical_score": "180",
                        "numero_acordo": "1/2026",
                        "data_inicio_vigencia": "",
                        "data_fim_vigencia": "",
                        "orgao_convenente": "",
                        "objeto": "",
                        "validation_warning": "",
                        "json_path": "output/acordo_cooperacao_tecnica_60090.000001_2026-01.json",
                    },
                    {
                        "processo": "60090.000002/2026-02",
                        "publication_status": "retained_silver",
                        "canonical_score": "120",
                        "numero_acordo": "",
                        "data_inicio_vigencia": "",
                        "data_fim_vigencia": "",
                        "orgao_convenente": "",
                        "objeto": "",
                        "validation_warning": "processo_divergente_documento=99999.999999/2026-99",
                        "json_path": "output/acordo_cooperacao_tecnica_60090.000002_2026-02.json",
                    },
                ],
            )

            _write_csv(
                output_dir / "memorando_normalizado_latest.csv",
                [
                    "captured_at",
                    "requested_type",
                    "processo",
                    "documento",
                    "resolved_document_type",
                    "selection_reason",
                    "classification_reason",
                    "validation_status",
                    "publication_status",
                    "snapshot_mode",
                    "json_path",
                ],
                [
                    {
                        "captured_at": "2026-03-30T10:00:00",
                        "requested_type": "memorando",
                        "processo": "60090.000003/2026-03",
                        "documento": "123",
                        "resolved_document_type": "memorando_entendimentos",
                        "selection_reason": "primeiro_resultado_mais_recente",
                        "classification_reason": "cabecalho_memorando",
                        "validation_status": "valid_for_requested_type",
                        "publication_status": "published_gold",
                        "snapshot_mode": "html_dom",
                        "json_path": "output/memorando_entendimentos_60090.000003_2026-03.json",
                    }
                ],
            )

            ted_json_path = output_dir / "termo_execucao_descentralizada_60090.000004_2026-04.json"
            _write_json(
                ted_json_path,
                {
                    "captured_at": "2026-03-30T10:05:00",
                    "document_type": "ted",
                    "processo": "60090.000004/2026-04",
                    "documento": "4",
                    "snapshot": {
                        "extraction_mode": "api",
                        "api_payload": {
                            "numero_processo": "60090000004202604",
                            "objeto": "Execucao descentralizada de atividades",
                            "valor_global": "1500000.00",
                            "situacao": "Em execucao",
                            "uf": "DF",
                            "itens": [],
                        },
                    },
                },
            )
            _write_csv(
                output_dir / "ted_normalizado_latest.csv",
                [
                    "captured_at",
                    "requested_type",
                    "processo",
                    "documento",
                    "resolved_document_type",
                    "selection_reason",
                    "classification_reason",
                    "validation_status",
                    "publication_status",
                    "snapshot_mode",
                    "json_path",
                ],
                [
                    {
                        "captured_at": "2026-03-30T10:05:00",
                        "requested_type": "ted",
                        "processo": "60090.000004/2026-04",
                        "documento": "4",
                        "resolved_document_type": "termo_execucao_descentralizada",
                        "selection_reason": "api_result",
                        "classification_reason": "api_transferegov",
                        "validation_status": "valid_for_requested_type",
                        "publication_status": "published_gold",
                        "snapshot_mode": "api",
                        "json_path": str(ted_json_path),
                    }
                ],
            )

            # Duplicidades operacionais que nao podem contaminar o dataset final.
            _write_csv(
                output_dir / "act_status_execucao_latest.csv",
                ["processo", "found"],
                [
                    {"processo": "60090.000001/2026-01", "found": True},
                    {"processo": "60090.000001/2026-01", "found": True},
                ],
            )
            _write_csv(
                output_dir / "pt_status_execucao_latest.csv",
                ["processo", "found"],
                [
                    {"processo": "60090.000001/2026-01", "found": True},
                    {"processo": "60090.000001/2026-01", "found": True},
                ],
            )

            result = export_dashboard_ready_csv(output_dir)
            self.assertEqual(result["records"], 22)

            dashboard_path = output_dir / "dashboard_ready_latest.csv"
            self.assertTrue(dashboard_path.exists())
            with dashboard_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))

            self.assertEqual(len(rows), 22)

            row_1 = next(row for row in rows if row["processo"] == "60090.000001/2026-01")
            self.assertEqual(row_1["pt_gold"], "True")
            self.assertEqual(row_1["act_gold"], "True")
            self.assertEqual(row_1["act_numero_acordo"], "1/2026")
            self.assertEqual(row_1["act_data_inicio_vigencia"], "")
            self.assertEqual(row_1["act_orgao_convenente"], "PARCEIRO 1")
            self.assertEqual(row_1["source_act_parceiro"], "preview_fallback")
            self.assertEqual(row_1["act_objeto"], "Objeto preview 1")
            self.assertEqual(row_1["source_act_objeto"], "preview_fallback")
            self.assertEqual(row_1["act_quality"], "gold_partial")

            row_2 = next(row for row in rows if row["processo"] == "60090.000002/2026-02")
            self.assertEqual(row_2["act_gold"], "False")
            self.assertEqual(row_2["act_numero_acordo"], "")
            self.assertEqual(row_2["act_quality"], "silver_only")
            self.assertEqual(row_2["quality_status"], "low")

            row_3 = next(row for row in rows if row["processo"] == "60090.000003/2026-03")
            self.assertEqual(row_3["memorando_gold"], "True")
            self.assertEqual(row_3["memorando_json_path"], "output/memorando_entendimentos_60090.000003_2026-03.json")

            row_4 = next(row for row in rows if row["processo"] == "60090.000004/2026-04")
            self.assertEqual(row_4["ted_gold"], "True")
            self.assertEqual(row_4["ted_json_path"], str(ted_json_path))
            self.assertEqual(row_4["ted_objeto"], "Execucao descentralizada de atividades")
            self.assertEqual(row_4["ted_valor_global"], "1500000.00")
            self.assertEqual(row_4["ted_situacao"], "Em execucao")
            self.assertEqual(row_4["ted_uf"], "DF")
            self.assertEqual(row_4["quality_status"], "medium")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
