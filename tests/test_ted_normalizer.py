from __future__ import annotations

import csv
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.dashboard_exporter import export_dashboard_ready_csv
from app.services.ted_normalizer import RICH_COLUMNS, export_normalized_csv, parse_brl_money


class TEDNormalizerTests(unittest.TestCase):
    def test_parse_brl_money_converts_brazilian_values_to_decimal(self) -> None:
        self.assertEqual(parse_brl_money("R$ 1.255.800,00"), "1255800.00")
        self.assertEqual(parse_brl_money("valor total de R$ 2.500.000,00"), "2500000.00")
        self.assertEqual(parse_brl_money("130.000,00"), "130000.00")

    def test_export_uses_three_real_ted_jsons_and_writes_diagnostics(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        source_dir = repo_root / "backend" / "output"
        source_files = [
            source_dir / "termo_execucao_descentralizada_60090.000165_2024-27.json",
            source_dir / "termo_execucao_descentralizada_60092.000266_2020-54.json",
            source_dir / "termo_execucao_descentralizada_60090.000840_2025-07.json",
        ]
        for source_file in source_files:
            self.assertTrue(source_file.exists(), f"Fixture real ausente: {source_file}")

        output_dir = Path.cwd() / "tests" / "_tmp_ted_normalizer"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            records = []
            for source_file in source_files:
                target = output_dir / source_file.name
                shutil.copyfile(source_file, target)
                records.append({"publication_status": "published_gold", "json_path": str(target)})

            result = export_normalized_csv(output_dir, records=records)
            self.assertEqual(result["records"], 3)

            with (output_dir / "ted_normalizado_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 3)
            for column in RICH_COLUMNS:
                self.assertIn(column, rows[0])

            row_2020 = next(row for row in rows if row["processo"] == "60092.000266/2020-54")
            self.assertEqual(row_2020["numero_ted"], "12")
            self.assertEqual(row_2020["ano_ted"], "2020")
            self.assertEqual(row_2020["valor_global"], "1255800.00")
            self.assertEqual(row_2020["vigencia_inicio"], "2020-12-01")
            self.assertEqual(row_2020["vigencia_fim"], "2022-12-31")
            self.assertIn("UNIVERSIDADE FEDERAL", row_2020["unidade_descentralizada"].upper())
            self.assertIn("Natureza da Despesa", row_2020["cronograma_desembolso"])

            row_2025 = next(row for row in rows if row["processo"] == "60090.000840/2025-07")
            self.assertEqual(row_2025["numero_ted"], "04")
            self.assertEqual(row_2025["ano_ted"], "2025")
            self.assertEqual(row_2025["valor_global"], "2500000.00")
            self.assertIn("META 1", row_2025["metas"])

            with (output_dir / "ted_field_diagnostics_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                diagnostics = list(csv.DictReader(file_obj))
            self.assertGreaterEqual(len(diagnostics), 36)
            self.assertTrue(any(row["field_name"] == "valor_global" and row["status"] == "extracted" for row in diagnostics))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_dashboard_ready_includes_ted_process_without_preview(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        source_file = repo_root / "backend" / "output" / "termo_execucao_descentralizada_60092.000266_2020-54.json"
        output_dir = Path.cwd() / "tests" / "_tmp_ted_dashboard"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            target = output_dir / source_file.name
            shutil.copyfile(source_file, target)
            export_normalized_csv(output_dir, records=[{"publication_status": "published_gold", "json_path": str(target)}])

            result = export_dashboard_ready_csv(output_dir)
            self.assertEqual(result["records"], 1)
            with (output_dir / "dashboard_ready_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(rows[0]["processo"], "60092.000266/2020-54")
            self.assertEqual(rows[0]["ted_gold"], "True")
            self.assertEqual(rows[0]["ted_valor_global"], "1255800.00")
            self.assertIn("Sistema SipamHidro", rows[0]["ted_objeto"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
