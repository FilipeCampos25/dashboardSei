from __future__ import annotations

import sys
import json
import shutil
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.cooperation_common import CooperationDocumentHandler
from app.documents.types import DocumentTypeSpec
from app.services.act_normalizer import build_normalized_record as build_act_record
from app.services.normalization_contract import (
    SOURCE_DOCUMENT_TEXT,
    SOURCE_FALLBACK,
    SOURCE_MISSING,
    SOURCE_PREVIEW,
    build_document_contract,
    make_field,
    make_missing_field,
)
from app.services.pt_normalizer import build_normalized_record as build_pt_record


class NormalizationContractTests(unittest.TestCase):
    def test_contract_preserves_field_source_metadata(self) -> None:
        contract = build_document_contract(
            processo="60090.000001/2026-01",
            requested_type="administrativo",
            resolved_document_type="despacho",
            documento="Despacho 123",
            found=True,
            is_canonical_candidate=False,
            validation_status="related_but_not_requested",
            publication_status="retained_silver",
            normalization_status="descartado_semantico",
            fields={
                "numero": make_field(
                    value="123",
                    raw_value="No 123",
                    source_type=SOURCE_DOCUMENT_TEXT,
                    confidence="high",
                    rule_id="administrativo.numero.regex",
                ),
                "objeto": make_field(
                    value="Objeto herdado",
                    raw_value="Objeto herdado da tela",
                    source_type=SOURCE_FALLBACK,
                    confidence="low",
                    rule_id="administrativo.objeto.fallback",
                    warning="fallback_sem_confirmacao_documental",
                ),
                "vigencia": make_missing_field(rule_id="administrativo.vigencia.missing"),
            },
        )

        self.assertEqual(
            set(contract),
            {
                "processo",
                "requested_type",
                "resolved_document_type",
                "documento",
                "found",
                "is_canonical_candidate",
                "validation_status",
                "publication_status",
                "normalization_status",
                "fields",
                "field_sources",
                "quality",
            },
        )
        self.assertEqual(contract["fields"]["objeto"]["source_type"], SOURCE_FALLBACK)
        self.assertEqual(contract["fields"]["objeto"]["confidence"], "low")
        self.assertEqual(contract["field_sources"]["objeto"]["warning"], "fallback_sem_confirmacao_documental")
        self.assertEqual(contract["fields"]["vigencia"]["source_type"], SOURCE_MISSING)
        self.assertIn("objeto:fallback", contract["quality"]["issues"])
        self.assertIn("not_canonical_candidate", contract["quality"]["issues"])
        self.assertIn(contract["quality"]["status"], {"low", "medium"})

    def test_pt_contract_marks_preview_values_as_preview_not_high_confidence(self) -> None:
        payload = {
            "captured_at": "2026-03-13T12:00:00",
            "processo": "60090.001292/2025-24",
            "documento": "Plano de Trabalho",
            "requested_type": "pt",
            "resolved_document_type": "plano_trabalho",
            "collection": {"selection_reason": "highest_tree_match_score"},
            "snapshot": {
                "text": """
                    PLANO DE TRABALHO
                    2.2. Periodo de Execucao 5 de novembro de 2025 a 5 de novembro de 2030.
                    5. METODOLOGIA E INTERVENCAO
                    Meta 1 - Deteccao de embarcacoes.
                    Acao 1 - Reunioes tecnicas.
                """,
                "tables": [],
                "extraction_mode": "html_dom",
            },
            "prazos": {},
            "analysis": {},
        }
        preview = {
            "parceiro": "MB-EMA",
            "vigencia": "60 meses",
            "objeto": "execucao colaborativa de atividades",
        }

        record = build_pt_record(payload, preview, Path("plano_trabalho_60090.001292_2025-24.json"))
        contract = record["normalization_contract"]

        self.assertEqual(contract["requested_type"], "pt")
        self.assertEqual(contract["fields"]["parceiro"]["source_type"], SOURCE_PREVIEW)
        self.assertEqual(contract["fields"]["parceiro"]["confidence"], "medium")
        self.assertEqual(contract["fields"]["objeto"]["source_type"], SOURCE_PREVIEW)
        self.assertNotEqual(contract["fields"]["objeto"]["confidence"], "high")
        self.assertEqual(contract["fields"]["vigencia_inicio"]["source_type"], SOURCE_DOCUMENT_TEXT)
        self.assertNotIn("snapshot", contract["fields"])

    def test_act_contract_exposes_normalized_fields_and_quality(self) -> None:
        payload = {
            "processo": "60090.000269/2020-16",
            "documento": "Acordo de Cooperacao Tecnica 1/2021",
            "snapshot": {
                "title": "SEI - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    Acordo de Cooperacao Tecnica No 1/2021
                    PROCESSO No 60090.000269/2020-16
                    ACORDO DE COOPERACAO TECNICA No 1/2021 QUE ENTRE SI CELEBRAM A UNIAO,
                    REPRESENTADA PELO MINISTERIO DA DEFESA, POR INTERMEDIO DO CENTRO GESTOR E
                    OPERACIONAL DO SISTEMA DE PROTECAO DA AMAZONIA - CENSIPAM E A EMBRAPA.

                    CLAUSULA PRIMEIRA - DO OBJETO
                    Integracao de esforcos para uso da Inteligencia Territorial.

                    CLAUSULA NONA - DO PRAZO E VIGENCIA
                    Vigencia de 5 anos a partir da data da ultima assinatura.

                    Documento assinado eletronicamente por A, em 14/12/2021.
                    Documento assinado eletronicamente por B, em 20/12/2021.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 1/2021"},
        }

        record = build_act_record(payload, Path("acordo_cooperacao_tecnica_60090.000269_2020-16.json"))
        contract = record["normalization_contract"]

        self.assertEqual(contract["requested_type"], "act")
        self.assertTrue(contract["found"])
        self.assertIn("numero_acordo", contract["fields"])
        self.assertEqual(contract["fields"]["numero_acordo"]["source_type"], SOURCE_DOCUMENT_TEXT)
        self.assertIn(contract["quality"]["status"], {"high", "medium", "low"})
        self.assertIsInstance(contract["quality"]["score"], (int, float))

    def test_manifest_contract_supports_administrative_documents(self) -> None:
        handler = CooperationDocumentHandler(status_filename="administrativo_status_execucao_latest.csv")
        spec = DocumentTypeSpec(
            key="administrativo",
            display_name="Documento Administrativo",
            search_terms=("Despacho",),
            tree_match_terms=("despacho",),
            snapshot_prefix="documento_administrativo",
            log_label="ADMIN",
            cleanup_patterns=(),
            handler=handler,
        )
        row = handler._build_published_manifest_row(
            spec=spec,
            record={
                "captured_at": "2026-05-20T10:00:00",
                "requested_type": "administrativo",
                "processo": "60090.000001/2026-01",
                "documento": "Despacho 123",
                "resolved_document_type": "despacho",
                "selection_reason": "highest_tree_match_score",
                "classification_reason": "manifesto_administrativo",
                "validation_status": "valid_for_requested_type",
                "publication_status": "published_gold",
                "snapshot_mode": "html_dom",
                "json_path": "backend/output/documento_administrativo.json",
            },
        )

        contract = row["normalization_contract"]
        self.assertEqual(contract["requested_type"], "administrativo")
        self.assertIn("documento_administrativo", contract["fields"])
        self.assertNotEqual(contract["fields"]["documento_administrativo"]["source_type"], SOURCE_MISSING)

    def test_ted_manifest_contract_reads_api_payload_without_marking_as_text(self) -> None:
        handler = CooperationDocumentHandler(status_filename="ted_status_execucao_latest.csv")
        spec = DocumentTypeSpec(
            key="ted",
            display_name="TED",
            search_terms=("TED",),
            tree_match_terms=("termo de execucao descentralizada",),
            snapshot_prefix="termo_execucao_descentralizada",
            log_label="TED",
            cleanup_patterns=(),
            handler=handler,
        )
        tmp_dir = Path(__file__).resolve().parent / "_tmp_normalization_contract"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True)
        try:
            json_path = tmp_dir / "termo_execucao_descentralizada.json"
            json_path.write_text(
                json.dumps(
                    {
                        "snapshot": {
                            "api_payload": {
                                "objeto": "Apoio logistico",
                                "valor_global": "1000.00",
                                "situacao": "Publicado",
                                "uf": "DF",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            row = handler._build_published_manifest_row(
                spec=spec,
                record={
                    "captured_at": "2026-05-20T10:00:00",
                    "requested_type": "ted",
                    "processo": "60090.000001/2026-01",
                    "documento": "TED 123",
                    "resolved_document_type": "termo_execucao_descentralizada",
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "published_gold",
                    "json_path": str(json_path),
                },
            )

            contract = row["normalization_contract"]
            self.assertEqual(contract["fields"]["objeto"]["value"], "Apoio logistico")
            self.assertEqual(contract["fields"]["objeto"]["source_type"], "derived")
            self.assertNotEqual(contract["fields"]["objeto"]["source_type"], SOURCE_DOCUMENT_TEXT)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
