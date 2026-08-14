from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.dashboard_exporter import export_dashboard_ready_csv
from app.services.ted_normalizer import RICH_COLUMNS, build_normalized_record, export_normalized_csv, parse_brl_money
from tests.fixture_loader import load_fixture


class TEDNormalizerTests(unittest.TestCase):
    def _normalize_tables(self, tables: list[dict], *, text: str = "") -> tuple[dict, list[dict]]:
        payload = {
            "processo": "00000.000000/2026-00",
            "snapshot": {"text": text, "tables": tables},
            "analysis": {"validation_status": "valid_for_requested_type", "publication_status": "published_gold"},
        }
        return build_normalized_record(payload, "fixture_ted.json")

    def test_parse_brl_money_converts_brazilian_values_to_decimal(self) -> None:
        self.assertEqual(parse_brl_money("R$ 1.255.800,00"), "1255800.00")
        self.assertEqual(parse_brl_money("valor total de R$ 2.500.000,00"), "2500000.00")
        self.assertEqual(parse_brl_money("130.000,00"), "130000.00")

    def test_versioned_rich_ted_extracts_both_units_with_table_provenance(self) -> None:
        payload = load_fixture("ted_normalizer_rich.json")["payload"]
        row, diagnostics = build_normalized_record(payload, "ted_normalizer_rich.json")
        self.assertEqual(row["unidade_descentralizadora"], "CENSIPAM")
        self.assertEqual(row["unidade_descentralizada"], "UNIVERSIDADE FEDERAL SINTETICA")
        unit_diagnostics = {item["field_name"]: item for item in diagnostics if item["field_name"].startswith("unidade_")}
        self.assertEqual(unit_diagnostics["unidade_descentralizadora"]["table_index"], 1)
        self.assertEqual(unit_diagnostics["unidade_descentralizadora"]["row_index"], 2)
        self.assertEqual(unit_diagnostics["unidade_descentralizadora"]["column_index"], 2)
        self.assertEqual(unit_diagnostics["unidade_descentralizadora"]["confidence"], "high")
        self.assertIn("descentralizador", unit_diagnostics["unidade_descentralizadora"]["matched_key"].lower())
        self.assertEqual(unit_diagnostics["unidade_descentralizada"]["row_index"], 2)
        self.assertEqual(unit_diagnostics["unidade_descentralizada"]["confidence"], "high")
        self.assertNotIn("DANIEL DIAS PEREIRA", row["unidade_descentralizadora"])
        self.assertNotIn("MÁRCIA ABRAHÃO", row["unidade_descentralizada"])

        self.assertEqual(row["datas_assinatura"], "2020-12-01")
        self.assertEqual(row["data_assinatura"], "2020-12-01")
        self.assertEqual(row["vigencia_prazo_quantidade"], "25")
        self.assertEqual(row["vigencia_prazo_unidade"], "meses")
        self.assertEqual(row["vigencia_regra_inicio"], "assinatura")
        self.assertEqual(row["vigencia_inicio"], "2020-12-01")
        self.assertEqual(row["vigencia_fim"], "2022-12-31")
        self.assertEqual(row["vigencia_inicio_origem"], "calculada")
        self.assertEqual(row["vigencia_fim_origem"], "calculada")

    def test_relative_duration_without_anchor_is_preserved_without_dates(self) -> None:
        row, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"],
            ["O prazo de vigência será de 24 (vinte e quatro) meses."],
        ]}])
        self.assertEqual(row["vigencia_prazo_quantidade"], "24")
        self.assertEqual(row["vigencia_prazo_unidade"], "meses")
        self.assertEqual(row["vigencia_regra_inicio"], "indeterminada")
        self.assertEqual(row["vigencia_inicio"], "")
        self.assertEqual(row["vigencia_fim"], "")
        self.assertEqual(row["vigencia_warning"], "vigencia_prazo_sem_data_base")

    def test_signature_anchor_uses_latest_unique_signature_and_handles_end_of_month(self) -> None:
        text = "\n".join([
            "Documento assinado eletronicamente por Pessoa B, em 31/01/2024, às 10:00.",
            "Documento assinado eletronicamente por Pessoa A, em 30/01/2024, às 09:00.",
            "Documento assinado eletronicamente por Pessoa B, em 31/01/2024, às 10:00.",
        ])
        row, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 1 mês a partir da assinatura do TED."],
        ]}], text=text)
        self.assertEqual(row["datas_assinatura"], "2024-01-30;2024-01-31")
        self.assertEqual(row["vigencia_inicio"], "2024-01-31")
        self.assertEqual(row["vigencia_fim"], "2024-02-28")

    def test_explicit_date_and_publication_anchors_are_resolved_only_from_clause(self) -> None:
        explicit, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 2 anos a partir de 01/09/2023."],
        ]}])
        self.assertEqual(explicit["vigencia_regra_inicio"], "data_explicita")
        self.assertEqual(explicit["vigencia_inicio"], "2023-09-01")
        self.assertEqual(explicit["vigencia_fim"], "2025-08-31")

        publication, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 12 meses a partir da publicação, ocorrida em 15/09/2023."],
        ]}], text="Portaria de nomeação publicada em 20/11/2020.")
        self.assertEqual(publication["vigencia_regra_inicio"], "publicacao")
        self.assertEqual(publication["vigencia_inicio"], "2023-09-15")
        self.assertEqual(publication["vigencia_fim"], "2024-09-14")

        missing_publication, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 12 meses a partir da publicação."],
        ]}], text="Portaria de recondução publicada em 20/11/2020.")
        self.assertEqual(missing_publication["vigencia_inicio"], "")
        self.assertEqual(missing_publication["vigencia_warning"], "vigencia_dependente_publicacao_sem_data")

    def test_explicit_period_wins_and_reports_relative_calculation_conflict(self) -> None:
        row, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"],
            ["Prazo de 12 meses a partir de 01/01/2024. Início: 02/01/2024 Fim: 31/12/2024"],
        ]}])
        self.assertEqual(row["vigencia_inicio"], "2024-01-02")
        self.assertEqual(row["vigencia_fim"], "2024-12-31")
        self.assertEqual(row["vigencia_inicio_origem"], "explicita")
        self.assertIn("vigencia_inicio_divergente", row["vigencia_warning"])

    def test_year_duration_handles_leap_day_inclusively(self) -> None:
        row, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 1 ano a partir de 29/02/2024."],
        ]}])
        self.assertEqual(row["vigencia_inicio"], "2024-02-29")
        self.assertEqual(row["vigencia_fim"], "2025-02-27")

    def test_invalid_duration_and_ambiguous_anchor_do_not_calculate(self) -> None:
        invalid, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 0 meses a partir de 01/01/2024."],
        ]}])
        self.assertEqual(invalid["vigencia_fim"], "")
        self.assertEqual(invalid["vigencia_warning"], "vigencia_prazo_invalido")

        ambiguous, _ = self._normalize_tables([{"rows": [
            ["5. VIGÊNCIA"], ["Vigência de 12 meses, com início a partir da assinatura ou da publicação."],
        ]}], text="Documento assinado eletronicamente por Pessoa, em 01/01/2024, às 10:00.")
        self.assertEqual(ambiguous["vigencia_regra_inicio"], "indeterminada")
        self.assertEqual(ambiguous["vigencia_inicio"], "")
        self.assertEqual(ambiguous["vigencia_fim"], "")
        self.assertEqual(ambiguous["vigencia_warning"], "vigencia_data_base_ambigua")

    def test_act_and_pt_payload_dates_are_not_ted_signature_sources(self) -> None:
        payload = {
            "snapshot": {"text": "", "tables": [{"rows": [
                ["5. VIGÊNCIA"], ["Vigência de 24 meses a partir da assinatura do TED."],
            ]}]},
            "act": {"data_assinatura": "2024-01-01"},
            "pt": {"data_assinatura": "2024-02-01"},
        }
        row, _ = build_normalized_record(payload, "negative_ted.json")
        self.assertEqual(row["data_assinatura"], "")
        self.assertEqual(row["vigencia_inicio"], "")
        self.assertEqual(row["vigencia_fim"], "")

    def test_units_support_horizontal_vertical_and_irregular_rows(self) -> None:
        row, diagnostics = self._normalize_tables([{"rows": [
            ["1. DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADORA"],
            ["Órgão descentralizador", "Ministério Alfa"],
            ["2. DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADA"],
            ["Nome do órgão ou entidade descentralizada"],
            ["Universidade Beta - UB", ""],
        ]}])
        self.assertEqual(row["unidade_descentralizadora"], "Ministério Alfa")
        self.assertEqual(row["unidade_descentralizada"], "Universidade Beta - UB")
        unit_diagnostics = {item["field_name"]: item for item in diagnostics if item["field_name"].startswith("unidade_")}
        self.assertEqual(unit_diagnostics["unidade_descentralizadora"]["rule_id"], "ted.unit.entity_name.horizontal")
        self.assertEqual(unit_diagnostics["unidade_descentralizada"]["rule_id"], "ted.unit.entity_name.vertical")

    def test_concatenated_units_and_ug_aliases_stop_before_people_and_admin_units(self) -> None:
        row, _ = self._normalize_tables([{"rows": [
            ["DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADORA"],
            ["UNIDADE DESCENTRALIZADORA Nome da Autoridade Competente: Pessoa Um UG/SIAFI Número e Nome da Unidade Gestora - UG que descentralizará o crédito: 110511/0001 - Órgão Alfa"],
            ["DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADA"],
            ["UNIDADE DESCENTRALIZADA Nome do órgão ou entidade descentralizada: Instituto Beta Nome da autoridade competente: Pessoa Dois Nome da Secretaria/Departamento/Unidade Responsável: Unidade Administrativa"],
        ]}])
        self.assertEqual(row["unidade_descentralizadora"], "Órgão Alfa")
        self.assertEqual(row["unidade_descentralizada"], "Instituto Beta")

    def test_conflicting_same_confidence_candidates_return_partial_with_warning(self) -> None:
        row, diagnostics = self._normalize_tables([{"rows": [
            ["DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADORA"],
            ["Órgão descentralizador: Órgão Alfa"],
            ["Nome do órgão ou entidade descentralizador(a): Órgão Beta"],
            ["DADOS CADASTRAIS DA UNIDADE DESCENTRALIZADA"],
            ["Nome do órgão ou entidade descentralizada: Universidade Gama"],
        ]}])
        self.assertEqual(row["unidade_descentralizadora"], "")
        self.assertEqual(row["unidade_descentralizada"], "Universidade Gama")
        self.assertIn("unidade_descentralizadora:ambiguous_unit_candidates", row["quality_notes"])
        diagnostic = next(item for item in diagnostics if item["field_name"] == "unidade_descentralizadora")
        self.assertEqual(diagnostic["warning"], "ambiguous_unit_candidates")

    def test_signature_schedule_censipam_and_act_pt_payload_are_not_unit_sources(self) -> None:
        payload = {
            "snapshot": {"text": "CENSIPAM", "tables": [
                {"rows": [["CRONOGRAMA"], ["Órgão executor", "Órgão do Cronograma"]]},
                {"rows": [["14. ASSINATURA"], ["Unidade Descentralizadora"], ["DANIEL DIAS PEREIRA"]]},
            ]},
            "act": {"unidade_descentralizadora": "ACT Alfa"},
            "pt": {"unidade_descentralizada": "PT Beta"},
        }
        row, _ = build_normalized_record(payload, "negative_ted.json")
        self.assertEqual(row["unidade_descentralizadora"], "")
        self.assertEqual(row["unidade_descentralizada"], "")

    def test_export_uses_versioned_ted_fixture_and_writes_diagnostics(self) -> None:
        fixture = load_fixture("ted_normalizer_rich.json")
        output_dir = Path(__file__).resolve().parent / "_tmp_ted_normalizer"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            target = output_dir / "ted_normalizer_rich.json"
            target.write_text(json.dumps(fixture["payload"]), encoding="utf-8")
            records = [{"publication_status": "published_gold", "json_path": str(target)}]
            result = export_normalized_csv(output_dir, records=records)
            self.assertEqual(result["records"], 1)

            with (output_dir / "ted_normalizado_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            for column in RICH_COLUMNS:
                self.assertIn(column, rows[0])

            row_2020 = rows[0]
            self.assertEqual(row_2020["numero_ted"], "12")
            self.assertEqual(row_2020["ano_ted"], "2020")
            self.assertEqual(row_2020["valor_global"], "1255800.00")
            self.assertEqual(row_2020["vigencia_inicio"], "2020-12-01")
            self.assertEqual(row_2020["vigencia_fim"], "2022-12-31")
            self.assertIn("UNIVERSIDADE FEDERAL", row_2020["unidade_descentralizada"].upper())
            self.assertIn("Natureza da Despesa", row_2020["cronograma_desembolso"])

            self.assertIn("META 1", row_2020["metas"])

            with (output_dir / "ted_field_diagnostics_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                diagnostics = list(csv.DictReader(file_obj))
            self.assertGreaterEqual(len(diagnostics), 12)
            self.assertTrue(any(row["field_name"] == "valor_global" and row["status"] == "extracted" for row in diagnostics))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_dashboard_ready_includes_ted_process_without_preview(self) -> None:
        fixture = load_fixture("ted_normalizer_rich.json")
        output_dir = Path(__file__).resolve().parent / "_tmp_ted_dashboard"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            target = output_dir / "ted_normalizer_rich.json"
            target.write_text(json.dumps(fixture["payload"]), encoding="utf-8")
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
