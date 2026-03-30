from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.act_normalizer import (
    DOC_CLASS_ACT_FINAL,
    DOC_CLASS_EXTRATO,
    DOC_CLASS_MEMORANDO,
    DOC_CLASS_TERMO_ADESAO,
    DOC_CLASS_TERMO_ADITIVO,
    PUBLICATION_STATUS_GOLD,
    PUBLICATION_STATUS_SILVER,
    RESOLVED_TYPE_ACT,
    RESOLVED_TYPE_MEMORANDO,
    VALIDATION_STATUS_RELATED,
    VALIDATION_STATUS_VALID,
    build_normalized_record,
    classify_act_snapshot,
    classify_cooperation_snapshot,
    export_normalized_csv,
)


class ACTNormalizerTests(unittest.TestCase):
    def test_classify_act_snapshot_identifies_known_classes(self) -> None:
        cases = [
            (
                {
                    "title": "SEI - Acordo de Cooperacao Tecnica",
                    "text": "Acordo de Cooperacao Tecnica no 1/2021 que entre si celebram a Uniao e a EMBRAPA.",
                },
                DOC_CLASS_ACT_FINAL,
            ),
            (
                {
                    "title": "SEI - Memorando de Entendimentos",
                    "text": "Memorando de Entendimentos no 1 que entre si celebram a Uniao e o Estado de Roraima.",
                },
                DOC_CLASS_MEMORANDO,
            ),
            (
                {
                    "title": "SEI",
                    "text": "TERMO DE ADESAO AO ACORDO DE COOPERACAO TECNICA No 109/2022.",
                },
                DOC_CLASS_TERMO_ADESAO,
            ),
            (
                {
                    "title": "SEI",
                    "text": "PRIMEIRO TERMO ADITIVO AO ACORDO DE COOPERACAO TECNICA No 2/2023.",
                },
                DOC_CLASS_TERMO_ADITIVO,
            ),
            (
                {
                    "title": "SEI",
                    "text": "EXTRATO ACORDO DE COOPERACAO TECNICA no 2/2023 firmado entre Censipam e PRF.",
                },
                DOC_CLASS_EXTRATO,
            ),
        ]

        for snapshot, expected in cases:
            with self.subTest(expected=expected):
                result = classify_act_snapshot(snapshot, {})
                self.assertEqual(result["doc_class"], expected)
                if expected == DOC_CLASS_ACT_FINAL:
                    self.assertEqual(result["resolved_document_type"], RESOLVED_TYPE_ACT)
                if expected == DOC_CLASS_MEMORANDO:
                    self.assertEqual(result["resolved_document_type"], RESOLVED_TYPE_MEMORANDO)

    def test_classify_cooperation_snapshot_respects_requested_family(self) -> None:
        snapshot = {
            "title": "SEI - Memorando de Entendimentos",
            "text": "Memorando de Entendimentos no 1 que entre si celebram a Uniao e o Estado de Roraima.",
        }

        result = classify_cooperation_snapshot(snapshot, requested_type="memorando", collection_context={})
        self.assertEqual(result["doc_class"], DOC_CLASS_MEMORANDO)
        self.assertTrue(result["is_canonical_candidate"])
        self.assertEqual(result["validation_status"], VALIDATION_STATUS_VALID)
        self.assertEqual(result["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_classify_act_snapshot_rejeita_documentos_relacionados_no_cabecalho(self) -> None:
        cases = [
            (
                {
                    "title": "SEI - Portaria",
                    "text": "PORTARIA DE DESIGNACAO DE GESTORES DO ACORDO DE COOPERACAO TECNICA No 4/2020.",
                },
                "cabecalho_portaria",
            ),
            (
                {
                    "title": "SEI - Anexo Reuniao",
                    "text": "REUNIAO DE ACOMPANHAMENTO DO ACORDO DE COOPERACAO TECNICA No 1/2021.",
                },
                "cabecalho_reuniao",
            ),
            (
                {
                    "title": "SEI - Plano de Trabalho",
                    "text": "PLANO DE TRABALHO - PT No 1/2023 referente ao Acordo de Cooperacao Tecnica.",
                },
                "cabecalho_plano_trabalho",
            ),
        ]

        for snapshot, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                result = classify_act_snapshot(snapshot, {})
                self.assertNotEqual(result["doc_class"], DOC_CLASS_ACT_FINAL)
                self.assertEqual(result["classification_reason"], expected_reason)

    def test_build_normalized_record_extracts_contract_fields_from_act_final(self) -> None:
        payload = {
            "processo": "60090.000269/2020-16",
            "snapshot": {
                "title": "SEI - 4433322 - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    Acordo de Cooperacao Tecnica No 1/2021
                    PROCESSO No 60090.000269/2020-16
                    ACORDO DE COOPERACAO TECNICA No 1/2021 QUE ENTRE SI CELEBRAM, A UNIAO,
                    REPRESENTADA PELO MINISTERIO DA DEFESA, POR INTERMEDIO DO CENTRO GESTOR E
                    OPERACIONAL DO SISTEMA DE PROTECAO DA AMAZONIA - CENSIPAM E A EMPRESA
                    BRASILEIRA DE PESQUISA AGROPECUARIA - EMBRAPA, PARA OS FINS QUE ESPECIFICA.

                    CLAUSULA PRIMEIRA - DO OBJETO
                    O objeto do presente Acordo de Cooperacao Tecnica e a integracao de esforcos,
                    em regime de colaboracao, para ampliacao do uso da Inteligencia Territorial Estrategica.

                    CLAUSULA NONA - DO PRAZO E VIGENCIA
                    O prazo de vigencia deste Acordo de Cooperacao e de 5 (cinco) anos a partir da data da ultima assinatura.

                    CLAUSULA DECIMA QUINTA - DA AFERICAO DE RESULTADOS
                    Os Participes deverao aferir os beneficios e alcance do interesse publico obtidos
                    em decorrencia do ajuste, mediante a elaboracao de relatorio conjunto de execucao de atividades relativas a parceria,
                    no prazo de ate 30 (trinta) dias apos o encerramento.

                    Documento assinado eletronicamente por Sergio Nathan Marinho Goldstein, em 14/12/2021.
                    Documento assinado eletronicamente por Evaristo Eduardo de Miranda, em 20/12/2021.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 1/2021 (4433322)"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.000269_2020-16.json"))
        self.assertEqual(record["doc_class"], DOC_CLASS_ACT_FINAL)
        self.assertEqual(record["numero_acordo"], "1/2021")
        self.assertEqual(record["processo"], "60090.000269/2020-16")
        self.assertEqual(record["data_inicio_vigencia"], "2021-12-20")
        self.assertEqual(record["data_fim_vigencia"], "2026-12-19")
        self.assertIn("EMBRAPA", record["orgao_convenente"])
        self.assertIn("integracao de esforcos", record["objeto"].lower())
        self.assertEqual(record["gestor_titular"], "")
        self.assertEqual(record["gestor_substituto"], "")
        self.assertEqual(record["unidade_responsavel"], "")
        self.assertEqual(record["classificacao"], DOC_CLASS_ACT_FINAL)
        self.assertTrue(record["relatorio_encerramento"])
        self.assertEqual(record["field_source_numero_acordo"], "cabecalho_act_tecnica")
        self.assertEqual(record["field_source_objeto"], "clausula_objeto")
        self.assertEqual(record["field_source_vigencia"], "clausula_vigencia_ultima_assinatura")
        self.assertEqual(record["validation_warning"], "")

    def test_build_normalized_record_handles_mojibake_and_external_process_reference(self) -> None:
        title_mojibake = b"SEI - Acordo de Coopera\xc3\xa7\xc3\xa3o T\xc3\xa9cnica".decode("latin1")
        body_mojibake = (
            b"Acordo de Coopera\xc3\xa7\xc3\xa3o T\xc3\xa9cnica (PROCESSO INPE N\xc2\xba - 01340.009269/2023-68)\n"
            b"ACORDO DE COOPERA\xc3\x87\xc3\x83O T\xc3\x89CNICA QUE ENTRE SI CELEBRAM A UNI\xc3\x83O, POR INTERM\xc3\x89DIO DO\n"
            b"INSTITUTO NACIONAL DE PESQUISAS ESPACIAIS - INPE E O MINIST\xc3\x89RIO DA DEFESA POR INTERM\xc3\x89DIO\n"
            b"DO CENTRO GESTOR E OPERACIONAL DO SISTEMA DE PROTE\xc3\x87\xc3\x83O DA AMAZ\xc3\x94NIA - CENSIPAM,\n"
            b"PARA OS FINS QUE ESPECIFICA.\n\n"
            b"RESOLVEM celebrar o presente Acordo de Coopera\xc3\xa7\xc3\xa3o T\xc3\xa9cnica tendo em vista o que consta\n"
            b"do Processo n. 01340.003873/2025-42.\n"
        ).decode("latin1")
        payload = {
            "processo": "60090.000702/2025-10",
            "snapshot": {
                "title": title_mojibake,
                "extraction_mode": "html_dom",
                "text": f"""
                    Instituto Nacional de Pesquisas Espaciais
                    {body_mojibake}

                    1. CLÁUSULA PRIMEIRA – DO OBJETO
                    O objeto do presente Acordo de Cooperação Técnica é a execução de atividades conjuntas
                    de CT&I relativamente ao projeto Rede INPE-CENSIPAM.

                    9. CLÁUSULA NONA – DO PRAZO E VIGÊNCIA
                    O prazo de vigência deste Acordo de Cooperação Técnica é de 5 anos a partir da data da última assinatura.

                    Documento assinado eletronicamente por Antonio Miguel Vieira Monteiro, em 10/11/2025.
                    Documento assinado eletronicamente por Richard Fernandez Nunes, em 05/01/2026.
                """,
            },
            "collection": {"chosen_documento": b"Acordo de Coopera\xc3\xa7\xc3\xa3o T\xc3\xa9cnica INPE-CENSIPAM".decode("latin1")},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.000702_2025-10.json"))
        self.assertEqual(record["validation_status"], VALIDATION_STATUS_VALID)
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)
        self.assertEqual(record["data_inicio_vigencia"], "2026-01-05")
        self.assertEqual(record["data_fim_vigencia"], "2031-01-04")
        self.assertIn("instituto nacional de pesquisas espaciais", record["orgao_convenente"].lower())
        self.assertIn("rede inpe-censipam", record["objeto"].lower())
        self.assertIn("processo_referencia_externa_documento=01340.009269/2023-68", record["validation_warning"])

    def test_build_normalized_record_extracts_orgao_when_censipam_is_second_party(self) -> None:
        payload = {
            "processo": "60090.001292/2025-24",
            "snapshot": {
                "title": "SEI - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    ACORDO DE COOPERAÇÃO TÉCNICA Nº 1-2025/MB/DIGER
                    ACORDO DE COOPERAÇÃO TÉCNICA QUE ENTRE SI CELEBRAM A MARINHA DO BRASIL,
                    POR INTERMÉDIO DO ESTADO-MAIOR DA ARMADA, E O CENTRO GESTOR E OPERACIONAL
                    DO SISTEMA DE PROTEÇÃO DA AMAZÔNIA PARA OS FINS QUE ESPECIFICA.

                    A Marinha do Brasil (MB), com sede em Brasília, DF, inscrita no CNPJ/MF sob o nº 00.394.502/0074-08.
                    O Centro Gestor e Operacional do Sistema de Proteção da Amazônia (CENSIPAM),
                    com sede em Brasília, DF, inscrito no CNPJ/MF sob o nº 07.129.796/0001-26.

                    CLÁUSULA PRIMEIRA – DO OBJETO
                    O objeto do presente Acordo de Cooperação Técnica é a execução colaborativa de atividades
                    voltadas à geração de conhecimento operacional.

                    CLÁUSULA NONA – DO PRAZO E VIGÊNCIA
                    O prazo de vigência deste Acordo de Cooperação Técnica será de 5 anos a partir da assinatura.

                    Brasília, DF, em 5 de novembro de 2025.
                """,
            },
            "collection": {"chosen_documento": "ACT MB DIGER"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.001292_2025-24.json"))
        self.assertEqual(record["numero_acordo"], "1-2025/mb/diger")
        self.assertEqual(record["data_inicio_vigencia"], "2025-11-05")
        self.assertEqual(record["data_fim_vigencia"], "2030-11-04")
        self.assertIn("marinha do brasil", record["orgao_convenente"].lower())
        self.assertEqual(record["field_source_vigencia"], "clausula_vigencia_assinatura")

    def test_build_normalized_record_extracts_objeto_outside_first_clause(self) -> None:
        payload = {
            "processo": "60090.001333/2026-44",
            "snapshot": {
                "title": "SEI - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    ACORDO DE COOPERACAO TECNICA Nº 7/2026 QUE ENTRE SI CELEBRAM A UNIÃO,
                    POR INTERMÉDIO DO CENSIPAM E A UNIVERSIDADE FEDERAL DE TESTE.

                    CLÁUSULA PRIMEIRA – DAS DISPOSIÇÕES INICIAIS
                    As partes definem as premissas institucionais.

                    CLÁUSULA SEGUNDA – DO OBJETO
                    O objeto do presente Acordo de Cooperação Técnica é a implantação conjunta
                    de laboratório de monitoramento remoto.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 7/2026"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.001333_2026-44.json"))
        self.assertIn("monitoramento remoto", record["objeto"].lower())
        self.assertEqual(record["field_source_objeto"], "clausula_objeto")

    def test_build_normalized_record_relegates_materially_divergent_act_to_silver(self) -> None:
        payload = {
            "processo": "60090.000615/2022-10",
            "snapshot": {
                "title": "SEI - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    MINISTÉRIO DA ECONOMIA
                    ACORDO DE COOPERAÇÃO TÉCNICA Nº 109/2022
                    TERMO DE COOPERAÇÃO TÉCNICA QUE, ENTRE SI, CELEBRAM A UNIÃO,
                    POR INTERMÉDIO DA CENTRAL DE COMPRAS E O BANCO DO BRASIL S.A.
                    Processo nº 14022.172688/2022-07.

                    CLÁUSULA PRIMEIRA – DO OBJETO
                    O objeto do presente acordo é a operacionalização de conta-depósito vinculada.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 109/2022"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.000615_2022-10.json"))
        self.assertEqual(record["validation_status"], VALIDATION_STATUS_RELATED)
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)
        self.assertEqual(record["classification_reason"], "act_sem_marcador_interno")
        self.assertIn("processo_divergente_documento=14022.172688/2022-07", record["validation_warning"])

    def test_build_normalized_record_leaves_missing_fields_blank_when_only_publication_rule_exists(self) -> None:
        payload = {
            "processo": "60090.000445/2023-54",
            "snapshot": {
                "title": "SEI - 6467241 - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    Acordo de Cooperacao Tecnica no 3/2023
                    PROCESSO No 60090.000445/2023-54
                    Acordo de Cooperacao Tecnica que entre si celebram, a Uniao, representada pelo Ministerio da Defesa,
                    por intermedio do Centro Gestor e Operacional do Sistema de Protecao da Amazonia - Censipam
                    e a VISIONA TECNOLOGIA ESPACIAL S/A para os fins que especifica.

                    CLAUSULA PRIMEIRA - DO OBJETO
                    O objeto do presente Acordo de Cooperacao Tecnica e a execucao da cooperacao tecnica e operacional entre as participes.

                    CLAUSULA SETIMA - DO PRAZO E VIGENCIA
                    O prazo de vigencia deste Acordo de Cooperacao Tecnica sera de 03 anos a partir da publicacao no Diario Oficial da Uniao.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 3 (6467241)"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.000445_2023-54.json"))
        self.assertEqual(record["doc_class"], DOC_CLASS_ACT_FINAL)
        self.assertEqual(record["data_inicio_vigencia"], "")
        self.assertEqual(record["data_fim_vigencia"], "")
        self.assertEqual(record["gestor_titular"], "")
        self.assertEqual(record["gestor_substituto"], "")
        self.assertEqual(record["unidade_responsavel"], "")
        self.assertFalse(record["relatorio_encerramento"])
        self.assertIn("vigencia_dependente_publicacao_sem_data", record["validation_warning"])

    def test_relatorio_encerramento_ignora_relatorio_periodico_sem_fecho(self) -> None:
        payload = {
            "processo": "60090.001000/2026-01",
            "snapshot": {
                "title": "SEI - Acordo de Cooperacao Tecnica",
                "extraction_mode": "html_dom",
                "text": """
                    ACORDO DE COOPERACAO TECNICA No 4/2026 QUE ENTRE SI CELEBRAM A UNIAO E A VISIONA.
                    CLAUSULA PRIMEIRA - DO OBJETO
                    O objeto do presente Acordo de Cooperacao Tecnica e a cooperacao institucional.
                    CLAUSULA NONA - DA VIGENCIA
                    A vigencia sera de 12 meses a partir da ultima assinatura.
                    As partes apresentarao relatorio semestral de acompanhamento da execucao.
                    Documento assinado eletronicamente por Fulano, em 10/01/2026.
                    Documento assinado eletronicamente por Beltrano, em 12/01/2026.
                """,
            },
            "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 4/2026"},
        }

        record = build_normalized_record(payload, Path("acordo_cooperacao_tecnica_60090.001000_2026-01.json"))
        self.assertFalse(record["relatorio_encerramento"])

    def test_export_normalized_csv_keeps_only_act_final(self) -> None:
        output_dir = Path.cwd() / "tests" / "_tmp_act_normalizer"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            act_payload = {
                "processo": "08650.063489/2021-11",
                "snapshot": {
                    "title": "SEI - Acordo de Cooperacao Tecnica",
                    "extraction_mode": "zip_docx",
                    "text": """
                        Acordo de Cooperacao Tecnica no 2/2023
                        PROCESSO No 08650.063489/2021-11
                        ACORDO DE COOPERACAO TECNICA No 2/2023 QUE ENTRE SI CELEBRAM A UNIAO,
                        REPRESENTADA PELO MINISTERIO DA DEFESA, POR INTERMEDIO DO CENSIPAM
                        E A POLICIA RODOVIARIA FEDERAL.
                        CLAUSULA PRIMEIRA - DO OBJETO
                        O objeto do presente Acordo de Cooperacao Tecnica e a realizacao da analise integrada de informacoes.
                    """,
                },
                "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica 2 PRF (6451163)"},
            }
            extrato_payload = {
                "processo": "08650.063489/2021-11",
                "snapshot": {
                    "title": "SEI",
                    "extraction_mode": "zip_docx",
                    "text": "EXTRATO ACORDO DE COOPERACAO TECNICA no 2/2023 firmado entre Censipam e PRF.",
                },
                "collection": {"chosen_documento": "Acordo de Cooperacao Tecnica no 2/2023 DOU no 169 (6541115)"},
            }
            memorando_payload = {
                "processo": "60091.000060/2023-87",
                "requested_type": "memorando",
                "snapshot": {
                    "title": "SEI - Memorando de Entendimentos",
                    "extraction_mode": "html_dom",
                    "text": "Memorando de Entendimentos no 1 que entre si celebram a Uniao e o Estado de Roraima.",
                },
                "analysis": {
                    "doc_class": DOC_CLASS_MEMORANDO,
                    "resolved_document_type": RESOLVED_TYPE_MEMORANDO,
                    "snapshot_prefix": "memorando_entendimentos",
                    "classification_reason": "cabecalho_memorando",
                    "classification_priority": 80,
                    "requested_type": "memorando",
                    "accepted_doc_classes": (DOC_CLASS_MEMORANDO,),
                    "is_canonical_candidate": True,
                    "validation_status": VALIDATION_STATUS_VALID,
                    "publication_status": PUBLICATION_STATUS_GOLD,
                    "normalization_status": "publicado_canonico",
                    "discard_reason": "",
                    "requested_snapshot_prefix": "memorando_entendimentos",
                },
                "collection": {"chosen_documento": "Memorando de Entendimentos no 1 (6256843)"},
            }
            (output_dir / "acordo_cooperacao_tecnica_canonico.json").write_text(
                json.dumps(act_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            (output_dir / "acordo_cooperacao_tecnica_extrato.json").write_text(
                json.dumps(extrato_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            (output_dir / "memorando_entendimentos_60091.000060_2023-87.json").write_text(
                json.dumps(memorando_payload, ensure_ascii=False),
                encoding="utf-8",
            )

            export_result = export_normalized_csv(output_dir)
            self.assertEqual(export_result["records"], 1)

            normalized_path = output_dir / "act_normalizado_latest.csv"
            audit_path = output_dir / "act_classificacao_latest.csv"
            self.assertTrue(normalized_path.exists())
            self.assertTrue(audit_path.exists())

            with normalized_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["processo"], "08650.063489/2021-11")
            self.assertEqual(rows[0]["classificacao"], DOC_CLASS_ACT_FINAL)

            with audit_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                audit_rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(audit_rows), 2)
            classes = {row["doc_class"] for row in audit_rows}
            self.assertIn(DOC_CLASS_ACT_FINAL, classes)
            self.assertIn(DOC_CLASS_EXTRATO, classes)
            self.assertNotIn(DOC_CLASS_MEMORANDO, classes)
            self.assertIn("canon_rejection_reason", audit_rows[0])
            self.assertIn("field_source_vigencia", audit_rows[0])
            self.assertIn("validation_warning", audit_rows[0])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
