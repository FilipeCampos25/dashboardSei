from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pt_normalizer import (
    CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
    PERIOD_SOURCE_DIRECT,
    PERIOD_SOURCE_NOISE,
    PERIOD_SOURCE_SIGNATURE,
    PUBLICATION_STATUS_GOLD,
    PUBLICATION_STATUS_SILVER,
    VALIDATION_STATUS_NON_CANONICAL,
    build_normalized_record,
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
        self.assertEqual(record["prazo_inicio"], "2021-12-14")
        self.assertEqual(record["prazo_fim"], "2026-12-14")
        self.assertEqual(record["period_source"], PERIOD_SOURCE_SIGNATURE)


if __name__ == "__main__":
    unittest.main()
