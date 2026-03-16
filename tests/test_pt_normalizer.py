from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pt_normalizer import build_normalized_record


def _payload(
    processo: str,
    text: str,
    *,
    extraction_mode: str = "html_dom",
    tables: list[dict] | None = None,
    prazos: dict | None = None,
) -> dict:
    return {
        "captured_at": "2026-03-13T12:00:00",
        "processo": processo,
        "documento": processo,
        "snapshot": {
            "text": text,
            "tables": tables or [],
            "extraction_mode": extraction_mode,
        },
        "prazos": prazos or {},
    }


class PTNormalizerTests(unittest.TestCase):
    def test_pdf_native_periodo_explicito_e_metas(self) -> None:
        payload = _payload(
            "60090.001292/2025-24",
            """
            2.2. Período de Execução 5 de novembro de 2025 a 5 de novembro de 2030.
            2.3. Identificação do Objeto O objeto do presente Plano de Trabalho é a execução colaborativa de atividades.
            5. METODOLOGIA E INTERVENÇÃO
            5.1. Meta 1 - Detecção de embarcações não colaborativas.
            5.1.1. Ação 1 - Reuniões técnicas.
            5.1.2. Ação 2 - Compartilhamento de dados.
            6. UNIDADE RESPONSÁVEL
            """,
            extraction_mode="pdf_native",
        )
        preview = {"parceiro": "MB-EMA", "vigencia": "60 meses", "objeto": "execução colaborativa de atividades"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.001292_2025-24.json"))
        self.assertEqual(record["vigencia_inicio"], "2025-11-05")
        self.assertEqual(record["vigencia_fim"], "2030-11-05")
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")

    def test_pdf_ocr_duracao_relativa_com_assinatura(self) -> None:
        payload = _payload(
            "60090.000692/2021-99",
            """
            PLANO DE TRABALHO ENTRE CENSIPAM E IFB.
            4. ETAPAS, EXECUCAO E CRONOGRAMA:
            Meta 1 - Realização do estágio curricular obrigatório.
            Ação: apresentação do Plano de Atividades semestral.
            Produto: Plano de Atividades de estágio.
            Meta 2 - Consolidação do conhecimento teórico e prático.
            Ação: Organização do relatório e acervo documental.
            Produto: Relatório de Estágio.
            8. PREVISAO DE INICIO E TERMINO:
            O presente plano de trabalho vigorará pelo prazo de 60 (sessenta) meses,
            a partir da data de sua assinatura.
            Brasília, 5 de fevereiro de 2022.
            """,
            extraction_mode="pdf_ocr",
        )
        preview = {
            "parceiro": "IFB Instituto Federal de Brasília",
            "vigencia": "60 meses",
            "objeto": "concessão de estágio obrigatório",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000692_2021-99.json"))
        self.assertEqual(record["vigencia_inicio"], "2022-02-05")
        self.assertEqual(record["vigencia_fim"], "2027-02-05")
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")

    def test_html_dom_assinatura_eletronica(self) -> None:
        payload = _payload(
            "60092.000220/2021-16",
            """
            PLANO DE TRABALHO ENTRE CENSIPAM E IFPA.
            4. ETAPAS E EXECUÇÃO E CRONOGRAMA:
            Meta 1 - Realização do estágio curricular supervisionado obrigatório.
            Ação: apresentação do Plano de Atividades.
            Produto: Plano de Atividades de estágio.
            Meta 2 - Consolidação do conhecimento teórico e prático.
            Ação: organização do relatório e acervo documental.
            Produto: Relatório de Estágio.
            8. PREVISÃO DE INÍCIO E TÉRMINO:
            O presente plano de trabalho vigorará pelo prazo de 60 (sessenta) meses,
            a partir da data de sua assinatura.
            Documento assinado eletronicamente por Rafael Pinto Costa, Diretor-Geral, em 04/08/2022.
            """,
        )
        preview = {
            "parceiro": "IFPA INSTITUTO FEDERAL DE EDUCAÇÃO, CIÊNCIA E TECNOLOGIA DO PARÁ",
            "vigencia": "60 meses",
            "objeto": "oportunidade de vivência interdisciplinar",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60092.000220_2021-16.json"))
        self.assertEqual(record["vigencia_inicio"], "2022-08-04")
        self.assertEqual(record["vigencia_fim"], "2027-08-04")
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "completo_padronizado")

    def test_parceiro_executor_e_acoes_por_tabela(self) -> None:
        payload = _payload(
            "61074.007095/2020-75",
            """
            b. Outros Partícipes - Executor Órgão / Entidade Estado-Maior da Armada - EMA CNPJ 00.394.502/0074-08.
            2. DESCRIÇÃO DO PROJETO.
            Período de Execução OUT2020 a OUT2025.
            """,
            tables=[
                {
                    "rows": [
                        ["METAS", "AÇÃO", "RESPONSÁVEL", "PERÍODO", "SITUAÇÃO"],
                        ["1", "Nivelamento de Procedimentos", "1 - Reunião técnica de alinhamento.", "DGMM/Censipam", "OUT2020"],
                        ["2 - Construção de agenda de capacitação.", "DGMM/Censipam", "NOV2020", ""],
                    ]
                }
            ],
        )
        preview = {
            "vigencia": "out2020 a out2025",
            "objeto": "Construção de agenda de capacitação nos temas objeto deste Plano de Trabalho",
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
            Atividade A.1. Definir as missões que serão rastreadas.
            Fase B: Definição da Interface - Meta 2: Documento de interface INPE-CENSIPAM.
            Atividade B.1. Definir protocolo de comunicação.
            Fase F: Rotina - Meta 6: Operação de Rotina.
            Atividade F.1. Criar um plano de voo definindo quais satélites serão rastreados.
            Atividade F.4. Gerar relatório mensal da disponibilidade do sistema.
            """,
            extraction_mode="pdf_native",
        )
        preview = {
            "parceiro": "INSTITUTO NACIONAL DE PESQUISAS ESPACIAIS (INPE)",
            "vigencia": "60 meses.",
            "objeto": "execução de atividades entre o INPE e CENSIPAM",
        }
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000702_2025-10.json"))
        self.assertEqual(record["vigencia_inicio"], "")
        self.assertEqual(record["vigencia_fim"], "")
        self.assertTrue(record["metas_raw"])
        self.assertTrue(record["acoes_raw"])
        self.assertEqual(record["normalization_status"], "parcial_padronizado")

    def test_placeholder_nao_conta_como_periodo_valido(self) -> None:
        payload = _payload(
            "60090.000033/2021-52",
            """
            Plano de Trabalho.
            Ação: operação das antenas.
            8. PREVISÃO DE INÍCIO E TÉRMINO: inserir previsão de início e término.
            """,
            extraction_mode="zip_docx",
        )
        preview = {"parceiro": "COMAE", "vigencia": "48 meses", "objeto": "cooperação entre Censipam e COMAE"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60090.000033_2021-52.json"))
        self.assertNotEqual(record["normalization_status"], "completo_padronizado")

    def test_periodo_incompleto_rebaixa_falso_positivo(self) -> None:
        payload = _payload(
            "60093.000125/2020-21",
            """
            AÇÃO E CRONOGRAMA.
            Meta 1 - Apoio técnico para monitoramento.
            O presente plano de trabalho tem por objetivo apoiar ações do monitoramento estadual.
            """,
            extraction_mode="zip_docx",
        )
        preview = {"parceiro": "SEDAM/RO", "vigencia": "60 meses.", "objeto": "apoio técnico no monitoramento"}
        record = build_normalized_record(payload, preview, Path("plano_trabalho_60093.000125_2020-21.json"))
        self.assertNotEqual(record["normalization_status"], "completo_padronizado")


if __name__ == "__main__":
    unittest.main()
