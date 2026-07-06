from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pt_normalizer import (
    CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
    PERIOD_CLASS_CONTAMINATED_TEXT,
    PERIOD_CLASS_EXPLICIT_DATE,
    PERIOD_CLASS_NARRATIVE_NO_BASE,
    PERIOD_CLASS_RELATIVE_APPROVAL,
    PERIOD_CLASS_RELATIVE_PUBLICATION,
    PERIOD_CLASS_RELATIVE_SIGNATURE,
    PERIOD_SOURCE_DIRECT,
    PERIOD_SOURCE_NOISE,
    PERIOD_SOURCE_SIGNATURE,
    PUBLICATION_STATUS_GOLD,
    PUBLICATION_STATUS_SILVER,
    VALIDATION_STATUS_VALID,
    VALIDATION_STATUS_NON_CANONICAL,
    build_normalized_record,
    export_normalized_csv,
    normalize_pt_period,
)
from app.rpa.sei.document_text_extractor import parse_prazos


def _payload(
    processo: str,
    text: str,
    *,
    extraction_mode: str = "html_dom",
    tables: list[dict] | None = None,
    prazos: dict | None = None,
    collection: dict | None = None,
    analysis: dict | None = None,
) -> dict:
    return {
        "captured_at": "2026-03-13T12:00:00",
        "processo": processo,
        "documento": processo,
        "requested_type": "pt",
        "resolved_document_type": "plano_trabalho",
        "collection": collection or {"selection_reason": "primeiro_resultado_mais_recente"},
        "snapshot": {
            "text": text,
            "tables": tables or [],
            "extraction_mode": extraction_mode,
        },
        "prazos": prazos or {},
        "analysis": analysis or {},
    }


class PTNormalizerTests(unittest.TestCase):
    def test_pdf_native_periodo_explicito_e_metas(self) -> None:
        payload = _payload(
            "60090.001292/2025-24",
            """
            2.2. Periodo de Execucao 5 de novembro de 2025 a 5 de novembro de 2030.
            2.3. Identificacao do Objeto O objeto do presente Plano de Trabalho e a execucao colaborativa de atividades.
            5. METODOLOGIA E INTERVENCAO
            5.1. Meta 1 - Deteccao de embarcacoes nao colaborativas.
            5.1.1. Acao 1 - Reunioes tecnicas.
            5.1.2. Acao 2 - Compartilhamento de dados.
            6. UNIDADE RESPONSAVEL
            """,
            extraction_mode="pdf_native",
        )
        preview = {"parceiro": "MB-EMA", "vigencia": "60 meses", "objeto": "execucao colaborativa de atividades"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.001292_2025-24.json"))
        self.assertEqual(record["vigencia_inicio"], "2025-11-05")
        self.assertEqual(record["vigencia_fim"], "2030-11-05")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_DIRECT)
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_pdf_ocr_duracao_relativa_com_assinatura(self) -> None:
        payload = _payload(
            "60090.000692/2021-99",
            """
            PLANO DE TRABALHO ENTRE CENSIPAM E IFB.
            4. ETAPAS, EXECUCAO E CRONOGRAMA:
            Meta 1 - Realizacao do estagio curricular obrigatorio.
            Acao: apresentacao do Plano de Atividades semestral.
            Produto: Plano de Atividades de estagio.
            Meta 2 - Consolidacao do conhecimento teorico e pratico.
            Acao: Organizacao do relatorio e acervo documental.
            Produto: Relatorio de Estagio.
            8. PREVISAO DE INICIO E TERMINO:
            O presente plano de trabalho vigorara pelo prazo de 60 (sessenta) meses,
            a partir da data de sua assinatura.
            Brasilia, 5 de fevereiro de 2022.
            """,
            extraction_mode="pdf_ocr",
        )
        preview = {
            "parceiro": "IFB Instituto Federal de Brasilia",
            "vigencia": "60 meses",
            "objeto": "concessao de estagio obrigatorio",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000692_2021-99.json"))
        self.assertEqual(record["vigencia_inicio"], "2022-02-05")
        self.assertEqual(record["vigencia_fim"], "2027-02-05")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_SIGNATURE)
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_pdf_ocr_assinatura_com_dia_degradado_por_ocr(self) -> None:
        payload = _payload(
            "60090.000692/2021-99",
            """
            PLANO DE TRABALHO ENTRE CENSIPAM E IFB.
            8. PREVISAO DE INICIO E TERMINO:
            O presente plano de trabalho vigorara pelo prazo de 60 (sessenta) meses,
            a partir da data de sua assinatura.
            Brasilia, Ł5 de fevereiro de 2022.
            7. METAS A SEREM ATINGIDAS:
            Meta 1 - Supervisao do estagio curricular.
            Acao: Organizacao do ensino aplicado aos discentes.
            """,
            extraction_mode="pdf_ocr",
        )
        preview = {
            "parceiro": "IFB Instituto Federal de Brasilia",
            "vigencia": "60 meses",
            "objeto": "concessao de estagio obrigatorio",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000692_2021-99.json"))
        self.assertEqual(record["vigencia_inicio"], "2022-02-25")
        self.assertEqual(record["vigencia_fim"], "2027-02-25")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_SIGNATURE)
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_html_dom_assinatura_eletronica(self) -> None:
        payload = _payload(
            "60092.000220/2021-16",
            """
            PLANO DE TRABALHO ENTRE CENSIPAM E IFPA.
            4. ETAPAS E EXECUCAO E CRONOGRAMA:
            Meta 1 - Realizacao do estagio curricular supervisionado obrigatorio.
            Acao: apresentacao do Plano de Atividades.
            Produto: Plano de Atividades de estagio.
            Meta 2 - Consolidacao do conhecimento teorico e pratico.
            Acao: organizacao do relatorio e acervo documental.
            Produto: Relatorio de Estagio.
            8. PREVISAO DE INICIO E TERMINO:
            O presente plano de trabalho vigorara pelo prazo de 60 (sessenta) meses,
            a partir da data de sua assinatura.
            Documento assinado eletronicamente por Rafael Pinto Costa, Diretor-Geral, em 04/08/2022.
            """,
        )
        preview = {
            "parceiro": "IFPA INSTITUTO FEDERAL DE EDUCACAO, CIENCIA E TECNOLOGIA DO PARA",
            "vigencia": "60 meses",
            "objeto": "oportunidade de vivencia interdisciplinar",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60092.000220_2021-16.json"))
        self.assertEqual(record["data_assinatura"], "2022-08-04")
        self.assertEqual(record["datas_assinatura"], "2022-08-04")
        self.assertEqual(record["vigencia_inicio"], "2022-08-04")
        self.assertEqual(record["vigencia_fim"], "2027-08-04")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_SIGNATURE)
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")

    def test_parceiro_executor_e_acoes_por_tabela(self) -> None:
        payload = _payload(
            "61074.007095/2020-75",
            """
            b. Outros Participes - Executor Orgao / Entidade Estado-Maior da Armada - EMA CNPJ 00.394.502/0074-08.
            2. DESCRICAO DO PROJETO.
            Periodo de Execucao OUT2020 a OUT2025.
            """,
            tables=[
                {
                    "rows": [
                        ["METAS", "ACAO", "RESPONSAVEL", "PERIODO", "SITUACAO"],
                        ["1", "Nivelamento de Procedimentos", "1 - Reuniao tecnica de alinhamento.", "DGMM/Censipam", "OUT2020"],
                        ["2 - Construcao de agenda de capacitacao.", "DGMM/Censipam", "NOV2020", ""],
                    ]
                }
            ],
        )
        preview = {
            "vigencia": "out2020 a out2025",
            "objeto": "Construcao de agenda de capacitacao nos temas objeto deste Plano de Trabalho",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_61074.007095_2020-75.json"))
        self.assertIn("Estado-Maior da Armada", record["parceiro"])
        self.assertEqual(record["vigencia_inicio"], "2020-10-01")
        self.assertEqual(record["vigencia_fim"], "2025-10-31")
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")

    def test_tabela_execucao_com_titulo_antes_do_cabecalho(self) -> None:
        payload = _payload(
            "60090.000445/2023-54",
            """
            PLANO DE TRABALHO - PT.
            Periodo de Execucao AGO/2023 a AGO/2026.
            """,
            tables=[
                {
                    "rows": [
                        ["10. PLANO DE ACAO E CRONOGRAMA DE EXECUCAO"],
                        ["METAS", "ACAO", "RESPONSAVEL", "2023", "2024-2027", "SITUACAO"],
                        ["1", "Alinhamento entre as equipes do Censipam e Visiona.", "Reuniao", "CENSIPAM/Visiona", "", "X", "", "X", "Ja realizada"],
                        ["2", "Desenvolvimento de metodologia para recebimento de imagens.", "-", "CENSIPAM/Visiona", "", "X", "", "", ""],
                        ["6", "Difusao de conhecimentos e geracao conjunta de produtos.", "-", "CENSIPAM/Visiona", "", "X", "X", "X", ""],
                    ]
                }
            ],
            prazos={"inicio_raw": "01/08/2023", "termino_raw": "31/08/2026"},
        )
        preview = {
            "parceiro": "Visiona Tecnologia Espacial S/A",
            "vigencia": "60 meses.",
            "objeto": "execucao da cooperacao tecnica e operacional entre as participes",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000445_2023-54.json"))
        self.assertIn("1 | Alinhamento entre as equipes", record["metas_raw"])
        self.assertIn("6 | Difusao de conhecimentos", record["metas_raw"])
        self.assertIn("Reuniao | CENSIPAM/Visiona", record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_tabela_execucao_com_cabecalho_dividido_em_duas_linhas(self) -> None:
        payload = _payload(
            "60093.000183/2021-36",
            """
            PLANO DE TRABALHO.
            Periodo de Execucao MAR/2023 a MAR/2028.
            """,
            tables=[
                {
                    "rows": [
                        ["ACAO E CRONOGRAMA", "INDICADOR FISICO", "CRONOGRAMA DE EXECUCAO", "RESPONSAVEL"],
                        ["METAS", "ETAPA", "ESPECIFICACAO", "UNID", "QUANT", "Inicio", "Termino"],
                        ["1 NIVELAMENTO DE PROCEDIMENTOS", "1.1", "Reuniao tecnica para definicao dos cenarios.", "Reuniao", "2", "MAR/23", "MAR/27", "COHIDRO/CPRM"],
                        ["1.2", "Construcao de agenda de capacitacao.", "Agenda", "1", "ABR/23", "ABR/26", "COHIDRO/CPRM"],
                        ["2 PRODUTOS HIDROMETEOROLOGICOS", "2.1", "Analise espaco temporal dos niveis.", "Relatorio", "1", "MAR/23", "ABR/26", "COHIDRO/CPRM"],
                    ]
                }
            ],
            prazos={"inicio_raw": "01/03/2023", "termino_raw": "31/03/2028"},
        )
        preview = {
            "parceiro": "CPRM",
            "vigencia": "60 meses.",
            "objeto": "intercambio de informacoes para monitoramento hidrometeorologico",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60093.000183_2021-36.json"))
        self.assertIn("1 NIVELAMENTO DE PROCEDIMENTOS", record["metas_raw"])
        self.assertIn("2 PRODUTOS HIDROMETEOROLOGICOS", record["metas_raw"])
        self.assertIn("1.2 | Construcao de agenda de capacitacao", record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_tabela_generica_nao_vira_execucao_sem_cabecalho_forte(self) -> None:
        payload = _payload(
            "60090.000033/2021-52",
            """
            PLANO DE TRABALHO.
            Periodo de Execucao OUT/2021 a OUT/2025.
            """,
            tables=[
                {
                    "rows": [
                        ["Responsavel", "Acoes previstas"],
                        ["CENSIPAM", "Apoiar as atividades administrativas do acordo."],
                    ]
                }
            ],
        )
        preview = {
            "parceiro": "COMAE",
            "vigencia": "48 meses",
            "objeto": "cooperacao entre Censipam e COMAE",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000033_2021-52.json"))
        self.assertEqual(record["metas_raw"], "")
        self.assertEqual(record["acoes_raw"], "")
        self.assertNotEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)

    def test_pdf_native_sem_vigencia_global_permanece_parcial(self) -> None:
        payload = _payload(
            "60090.000702/2025-10",
            """
            Fase A: Assinatura do Acordo de Parceria. Meta1: Assinatura do Acordo.
            Atividade A.1. Definir as missoes que serao rastreadas.
            Fase B: Definicao da Interface - Meta 2: Documento de interface INPE-CENSIPAM.
            Atividade B.1. Definir protocolo de comunicacao.
            Fase F: Rotina - Meta 6: Operacao de Rotina.
            Atividade F.1. Criar um plano de voo definindo quais satelites serao rastreados.
            Atividade F.4. Gerar relatorio mensal da disponibilidade do sistema.
            """,
            extraction_mode="pdf_native",
        )
        preview = {
            "parceiro": "INSTITUTO NACIONAL DE PESQUISAS ESPACIAIS (INPE)",
            "vigencia": "60 meses.",
            "objeto": "execucao de atividades entre o INPE e CENSIPAM",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000702_2025-10.json"))
        self.assertEqual(record["vigencia_inicio"], "")
        self.assertEqual(record["vigencia_fim"], "")
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "parcial_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)

    def test_pdf_native_inicio_termino_inline_ignora_cabecalho_de_impressao(self) -> None:
        text = """
            PLANO DE TRABALHO
            2. IDENTIFICACAO DO OBJETO
            INICIO(MES/ANO) JUNHO/2025
            TERMINO(MES/ANO) JUNHO/2030
            11/11/2025, 09:59 SEI/MCTI - 13091424 - Anexo
            https://sei.mcti.gov.br/sei/controlador.php?acao=documento_imprimir_web&id_documento=14176805 1/9
            OBJETO: execucao de atividades entre o INPE e CENSIPAM.
            10. PLANO DE ACAO E CRONOGRAMA FISICO DE EXECUCAO
            Meta 1 - Desenvolver novos procedimentos.
            Atividade F.4. Gerar relatorio mensal da disponibilidade do sistema.
        """
        prazos = parse_prazos(text)
        self.assertEqual(prazos["inicio_data"], "2025-06-01")
        self.assertEqual(prazos["termino_data"], "2030-06-30")

        payload = _payload(
            "60090.000702/2025-10",
            text,
            extraction_mode="pdf_native",
            prazos=prazos,
        )
        preview = {
            "parceiro": "INSTITUTO NACIONAL DE PESQUISAS ESPACIAIS (INPE)",
            "vigencia": "60 meses.",
            "objeto": "execucao de atividades entre o INPE e CENSIPAM",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000702_2025-10.json"))
        self.assertEqual(record["vigencia_inicio"], "2025-06-01")
        self.assertEqual(record["vigencia_fim"], "2030-06-30")
        self.assertEqual(record["prazo_inicio_raw"], "junho/2025")
        self.assertEqual(record["prazo_fim_raw"], "junho/2030")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_DIRECT)
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_GOLD)

    def test_placeholder_nao_conta_como_periodo_valido(self) -> None:
        payload = _payload(
            "60090.000033/2021-52",
            """
            Plano de Trabalho.
            Acao: operacao das antenas.
            8. PREVISAO DE INICIO E TERMINO: inserir previsao de inicio e termino.
            """,
            extraction_mode="zip_docx",
        )
        preview = {"parceiro": "COMAE", "vigencia": "48 meses", "objeto": "cooperacao entre Censipam e COMAE"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000033_2021-52.json"))
        self.assertNotEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)

    def test_periodo_incompleto_rebaixa_falso_positivo(self) -> None:
        payload = _payload(
            "60093.000125/2020-21",
            """
            ACAO E CRONOGRAMA.
            Meta 1 - Apoio tecnico para monitoramento.
            O presente plano de trabalho tem por objetivo apoiar acoes do monitoramento estadual.
            """,
            extraction_mode="zip_docx",
            prazos={"termino_raw": "o presente plano de trabalho tem por"},
        )
        preview = {"parceiro": "SEDAM/RO", "vigencia": "60 meses.", "objeto": "apoio tecnico no monitoramento"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60093.000125_2020-21.json"))
        self.assertEqual(record["prazo_fim"], "")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_NOISE)
        self.assertNotEqual(record["normalization_status"], "completo_padronizado")
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)

    def test_pt_minuta_documentacao_fica_na_silver_mesmo_com_conteudo_extraivel(self) -> None:
        payload = _payload(
            "60093.000125/2020-21",
            """
            MINUTA DE PLANO DE TRABALHO PARA ACORDO DE COOPERACAO TECNICA
            PLANO DE TRABALHO
            IDENTIFICACAO DO OBJETO
            Inicio: JUN/2020
            Termino: JUN/2025
            Objeto: apoio tecnico entre o Censipam e a SEDAM.
            8 - PLANO DE ACAO E CRONOGRAMA DE EXECUCAO
            Meta 1 - Nivelamento de procedimentos.
            Acao 1: Reuniao tecnica.
            """,
            extraction_mode="zip_docx",
            prazos={"inicio_raw": "JUN/2020", "termino_raw": "JUN/2025"},
            collection={
                "selection_reason": "highest_tree_match_score",
                "chosen_documento": "Documentação - Minutas ACT e Plano de Trabalho (2358804)",
            },
            analysis={
                "doc_class": "pt_minuta_documentacao",
                "resolved_document_type": "plano_trabalho",
                "is_canonical_candidate": False,
                "validation_status": VALIDATION_STATUS_NON_CANONICAL,
                "classification_reason": CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
            },
        )
        preview = {"parceiro": "SEDAM/RO", "vigencia": "60 meses.", "objeto": "apoio tecnico no monitoramento"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60093.000125_2020-21.json"))
        self.assertEqual(record["validation_status"], VALIDATION_STATUS_NON_CANONICAL)
        self.assertEqual(record["classification_reason"], CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO)
        self.assertEqual(record["publication_status"], PUBLICATION_STATUS_SILVER)

    def test_periodo_relativo_usa_assinatura_como_ancora(self) -> None:
        payload = _payload(
            "60090.000269/2020-16",
            """
            PLANO DE TRABALHO - PT 2
            Inicio: imediatamente apos a assinatura.
            Termino: cinco anos apos a assinatura.
            Documento assinado eletronicamente por Sergio Nathan Marinho Goldstein, em 14/12/2021.
            Documento assinado eletronicamente por Evaristo Eduardo de Miranda, em 20/12/2021.
            Documento assinado eletronicamente por Jose Gilberto Jardine, em 20/12/2021.
            """,
            prazos={
                "inicio_raw": "imediatamente apos a assinatura",
                "termino_raw": "cinco anos apos a assinatura",
            },
        )
        preview = {
            "parceiro": "EMBRAPA",
            "vigencia": "5 anos",
            "objeto": "integracao de esforcos",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000269_2020-16.json"))
        self.assertEqual(record["data_assinatura"], "2021-12-20")
        self.assertEqual(record["datas_assinatura"], "2021-12-14 | 2021-12-20")
        self.assertEqual(record["prazo_inicio"], "2021-12-14")
        self.assertEqual(record["prazo_fim"], "2026-12-14")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_SIGNATURE)
        self.assertEqual(record["period_class"], PERIOD_CLASS_RELATIVE_SIGNATURE)
        self.assertEqual(record["rule_amount"], "5")
        self.assertEqual(record["rule_unit"], "anos")
        self.assertEqual(record["rule_anchor"], "assinatura")

    def test_export_normalized_csv_publica_apenas_melhor_pt_por_processo(self) -> None:
        processo = "60090.000100/2026-00"
        base_text = """
            PLANO DE TRABALHO
            OBJETO: cooperacao tecnica para pesquisa aplicada.
            Periodo de Execucao 1 de janeiro de 2026 a 31 de dezembro de 2026.
            Meta 1 - Implantar rotina de acompanhamento.
            Acao 1 - Realizar reunioes tecnicas.
        """
        stronger_text = base_text + """
            Meta 2 - Consolidar indicadores de desempenho.
            Acao 2 - Publicar relatorio tecnico de acompanhamento.
            8. PREVISAO DE INICIO E TERMINO.
        """
        output_dir = Path.cwd() / "tests" / "_tmp_pt_normalizer_export"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            preview_path = output_dir / "parcerias_vigentes_latest.csv"
            with preview_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
                writer = csv.DictWriter(
                    file_obj,
                    fieldnames=["processo", "parceiro", "vigencia", "objeto", "numero_act"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "processo": processo,
                        "parceiro": "UNIVERSIDADE FEDERAL DE TESTE",
                        "vigencia": "12 meses",
                        "objeto": "cooperacao tecnica para pesquisa aplicada",
                        "numero_act": "01/2026",
                    }
                )

            weak_payload = _payload(
                processo,
                base_text,
                analysis={
                    "validation_status": VALIDATION_STATUS_VALID,
                    "is_canonical_candidate": True,
                    "internal_content_score": 3,
                },
            )
            weak_payload["documento"] = "PT-FRACO"
            strong_payload = _payload(
                processo,
                stronger_text,
                analysis={
                    "validation_status": VALIDATION_STATUS_VALID,
                    "is_canonical_candidate": True,
                    "internal_content_score": 7,
                },
            )
            strong_payload["documento"] = "PT-FORTE"

            (output_dir / "plano_trabalho_60090.000100_2026-00_tree_rank_001.json").write_text(
                json.dumps(weak_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            (output_dir / "plano_trabalho_60090.000100_2026-00_tree_rank_002.json").write_text(
                json.dumps(strong_payload, ensure_ascii=False),
                encoding="utf-8",
            )

            result = export_normalized_csv(output_dir)

            self.assertEqual(result["records"], 1)
            with (output_dir / "pt_auditoria_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                audit_rows = list(csv.DictReader(file_obj))
            with (output_dir / "pt_normalizado_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                published_rows = list(csv.DictReader(file_obj))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(len(audit_rows), 2)
        self.assertEqual(len(published_rows), 1)
        self.assertEqual(published_rows[0]["documento"], "PT-FORTE")
        self.assertGreater(
            int(published_rows[0]["canonical_score"]),
            int(next(row["canonical_score"] for row in audit_rows if row["documento"] == "PT-FRACO")),
        )

    def test_normalize_pt_period_classifica_60_meses_sem_data_base(self) -> None:
        period = normalize_pt_period("", "60 meses", {})
        self.assertEqual(period.period_class, PERIOD_CLASS_NARRATIVE_NO_BASE)
        self.assertEqual(period.rule_amount, "60")
        self.assertEqual(period.rule_unit, "meses")
        self.assertEqual(period.missing_base_date, "true")
        self.assertEqual(period.prazo_fim, "")

    def test_normalize_pt_period_calcula_cinco_anos_apos_assinatura(self) -> None:
        period = normalize_pt_period(
            "imediatamente apos a assinatura",
            "cinco anos apos a assinatura",
            {"signature_date": "2021-12-14"},
        )
        self.assertEqual(period.period_class, PERIOD_CLASS_RELATIVE_SIGNATURE)
        self.assertEqual(period.prazo_inicio, "2021-12-14")
        self.assertEqual(period.prazo_fim, "2026-12-14")
        self.assertEqual(period.rule_amount, "5")
        self.assertEqual(period.rule_anchor, "assinatura")

    def test_normalize_pt_period_classifica_imediatamente_apos_publicacao(self) -> None:
        period = normalize_pt_period("imediatamente apos a publicacao", "", {"publication_date": "2024-06-10"})
        self.assertEqual(period.period_class, PERIOD_CLASS_RELATIVE_PUBLICATION)
        self.assertEqual(period.prazo_inicio, "2024-06-10")
        self.assertEqual(period.rule_anchor, "publicacao")
        self.assertEqual(period.period_warning, "periodo_relativo_sem_regra_completa")

    def test_normalize_pt_period_classifica_relativo_aprovacao_sem_base(self) -> None:
        period = normalize_pt_period("apos aprovacao", "12 meses apos aprovacao", {})
        self.assertEqual(period.period_class, PERIOD_CLASS_RELATIVE_APPROVAL)
        self.assertEqual(period.rule_anchor, "aprovacao")
        self.assertEqual(period.rule_amount, "12")
        self.assertEqual(period.missing_base_date, "true")

    def test_normalize_pt_period_data_explicita(self) -> None:
        period = normalize_pt_period("01/06/2025", "30/06/2030", {})
        self.assertEqual(period.period_class, PERIOD_CLASS_EXPLICIT_DATE)
        self.assertEqual(period.prazo_inicio, "2025-06-01")
        self.assertEqual(period.prazo_fim, "2030-06-30")
        self.assertEqual(period.period_source, PERIOD_SOURCE_DIRECT)

    def test_normalize_pt_period_narrativo_impossivel_de_calcular(self) -> None:
        period = normalize_pt_period("", "ate a conclusao das atividades pactuadas", {})
        self.assertEqual(period.period_class, PERIOD_CLASS_NARRATIVE_NO_BASE)
        self.assertEqual(period.prazo_fim, "")
        self.assertEqual(period.period_warning, "periodo_narrativo_sem_data_base")

    def test_normalize_pt_period_texto_contaminado(self) -> None:
        period = normalize_pt_period("", "codigo verificador 123456 documento assinado eletronicamente", {})
        self.assertEqual(period.period_class, PERIOD_CLASS_CONTAMINATED_TEXT)
        self.assertEqual(period.period_warning, "periodo_bruto_contaminado_ou_narrativo")


if __name__ == "__main__":
    unittest.main()
