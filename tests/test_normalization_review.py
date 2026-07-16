from __future__ import annotations

import shutil
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.output import csv_writer
from app.services.normalization_review import collect_review_issues, export_review_queue


class NormalizationReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = Path(__file__).resolve().parent / "_tmp_normalization_review"
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_collects_problematic_normalization_rules(self) -> None:
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000001/2026-01",
                    "documento": "PT 1",
                    "parceiro": "Parceiro",
                    "vigencia_raw": "60 meses da assinatura",
                    "vigencia_inicio": "",
                    "vigencia_fim": "",
                    "objeto": "apoio",
                    "period_source": "unresolved_relative",
                    "period_warning": "",
                    "classification_reason": "",
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "published_gold",
                    "normalization_status": "parcial_padronizado",
                    "json_path": "pt.json",
                }
            ],
            self.tmp_dir / "pt_normalizado_latest.csv",
        )
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000002/2026-02",
                    "documento": "ACT minuta",
                    "numero_acordo": "",
                    "data_inicio_vigencia": "",
                    "data_fim_vigencia": "",
                    "orgao_convenente": "",
                    "objeto": "Acordo de cooperacao",
                    "doc_class": "minuta",
                    "validation_status": "related_but_not_requested",
                    "publication_status": "retained_silver",
                    "normalization_status": "descartado_nao_canonico",
                    "process_alignment_status": "aligned",
                    "json_path": "act-minuta.json",
                },
                {
                    "processo": "60090.000003/2026-03",
                    "documento": "Termo Aditivo 1",
                    "numero_acordo": "1/2026",
                    "data_inicio_vigencia": "2026-01-01",
                    "data_fim_vigencia": "2027-01-01",
                    "orgao_convenente": "Orgao",
                    "objeto": "Ajuste ao acordo principal para prorrogacao de atividades.",
                    "doc_class": "termo_aditivo",
                    "validation_status": "related_but_not_requested",
                    "publication_status": "retained_silver",
                    "normalization_status": "descartado_nao_canonico",
                    "process_alignment_status": "processo_divergente_documento",
                    "json_path": "act-aditivo.json",
                },
                {
                    "processo": "60090.000006/2026-06",
                    "documento": "ACT externo relacionado",
                    "numero_acordo": "6/2026",
                    "data_inicio_vigencia": "2026-01-01",
                    "data_fim_vigencia": "2027-01-01",
                    "orgao_convenente": "Orgao parceiro",
                    "objeto": "Execucao conjunta de atividades operacionais e compartilhamento de dados.",
                    "doc_class": "act_final",
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "published_gold",
                    "normalization_status": "publicado_canonico",
                    "process_alignment_status": "external_reference",
                    "affinity_status": "related_document",
                    "json_path": "act-related.json",
                },
            ],
            self.tmp_dir / "act_classificacao_latest.csv",
        )
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000004/2026-04",
                    "documento": "TED 1",
                    "numero_ted": "1",
                    "ano_ted": "2026",
                    "objeto": "Execucao de servicos especializados para apoio operacional.",
                    "unidade_descentralizadora": "A",
                    "unidade_descentralizada": "B",
                    "valor_global": "",
                    "vigencia_inicio": "2026-01-01",
                    "vigencia_fim": "2027-01-01",
                    "plano_aplicacao": "",
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "published_gold",
                    "normalization_status": "parcial_padronizado",
                    "json_path": "ted.json",
                }
            ],
            self.tmp_dir / "ted_normalizado_latest.csv",
        )
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000005/2026-05",
                    "documento": "",
                    "validation_status": "not_found",
                    "publication_status": "retained_silver",
                    "normalization_status": "not_found",
                    "json_path": "",
                }
            ],
            self.tmp_dir / "memorando_status_execucao_latest.csv",
        )

        issues = collect_review_issues(self.tmp_dir)
        codes = {issue["code"] for issue in issues}

        self.assertIn("required_field_missing", codes)
        self.assertIn("validity_without_base_date", codes)
        self.assertIn("unresolved_relative_deadline", codes)
        self.assertIn("draft_document", codes)
        self.assertIn("amendment_confused_with_main", codes)
        self.assertIn("related_not_canonical", codes)
        self.assertIn("process_mismatch", codes)
        self.assertIn("act_affinity_shadow_review", codes)
        self.assertIn("object_too_short_or_generic", codes)
        self.assertIn("ted_missing_financial_value", codes)
        self.assertIn("ted_without_application_plan", codes)
        self.assertIn("administrative_document_not_found", codes)

    def test_export_orders_high_gold_missing_before_not_found(self) -> None:
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000010/2026-10",
                    "documento": "TED 10",
                    "numero_ted": "10",
                    "ano_ted": "2026",
                    "objeto": "Execucao de servicos especializados para apoio operacional.",
                    "unidade_descentralizadora": "A",
                    "unidade_descentralizada": "B",
                    "valor_global": "",
                    "vigencia_inicio": "2026-01-01",
                    "vigencia_fim": "2027-01-01",
                    "plano_aplicacao": "",
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "published_gold",
                    "normalization_status": "parcial_padronizado",
                    "json_path": "ted.json",
                }
            ],
            self.tmp_dir / "ted_normalizado_latest.csv",
        )
        csv_writer.write_csv(
            [
                {
                    "processo": "60090.000011/2026-11",
                    "documento": "",
                    "validation_status": "not_found",
                    "publication_status": "retained_silver",
                    "normalization_status": "not_found",
                    "json_path": "",
                }
            ],
            self.tmp_dir / "memorando_status_execucao_latest.csv",
        )

        result = export_review_queue(self.tmp_dir)
        rows = list(pd.read_csv(result["latest_path"], dtype=str).fillna("").to_dict(orient="records"))

        self.assertEqual(rows[0]["severity"], "high")
        self.assertEqual(rows[0]["is_gold_missing"], "True")
        self.assertEqual(rows[-1]["code"], "administrative_document_not_found")
        self.assertEqual(rows[-1]["is_not_found"], "True")


if __name__ == "__main__":
    unittest.main()
