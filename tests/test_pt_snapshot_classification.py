from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from types import MethodType

os.environ["DEBUG"] = "false"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.rpa.scraping import SEIScraper


def _classifier(*, internal_score: int = 6, penalties: str = "") -> SEIScraper:
    scraper = object.__new__(SEIScraper)

    def content_score(self, snapshot, collection_context=None):
        return {
            "internal_content_score": internal_score,
            "internal_content_signals": "marcador_plano_trabalho|objeto|acoes_cronograma|prazo_periodo",
            "internal_content_penalties": penalties,
        }

    scraper._pt_internal_content_score = MethodType(content_score, scraper)
    return scraper


def _snapshot(text: str, *, title: str = "SEI/MD - PLANO DE TRABALHO - PT", tables=None) -> dict:
    return {
        "text": text,
        "title": title,
        "url": "https://sei.exemplo/documento",
        "tables": tables or [],
        "extraction_mode": "html_dom",
    }


class PTSnapshotClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = _classifier()

    def assert_explicit_minuta(self, text: str, *, title: str = "SEI/MD - PLANO DE TRABALHO - PT") -> None:
        analysis = self.scraper._classify_pt_snapshot(_snapshot(text, title=title))
        self.assertEqual(analysis["doc_class"], "pt_minuta_documentacao")
        self.assertEqual(analysis["validation_status"], "related_but_not_canonical")
        self.assertEqual(analysis["publication_status"], "retained_silver")
        self.assertEqual(analysis["discard_reason"], "minuta_documentacao")
        self.assertFalse(analysis["is_canonical_candidate"])

    def test_regressao_61074_minuta_rica_e_assinada_permanece_silver(self) -> None:
        text = """
        MINUTA
        MINISTERIO DA DEFESA
        PLANO DE TRABALHO - PT N 1/2020 - CGINT
        1. DADOS CADASTRAIS
        Parceiros: CENSIPAM e Estado-Maior da Armada.
        2. IDENTIFICACAO DO OBJETO
        Cooperacao e apoio tecnico para implementacao do SisGAAz.
        Periodo de execucao: OUT2020 a OUT2025.
        8. PLANO DE ACAO E CRONOGRAMA DE EXECUCAO
        Meta 1 - Nivelamento de procedimentos.
        9. APROVACAO DOS DIRIGENTES
        Pelo Censipam: RAFAEL PINTO COSTA, CPF 920.322.490-49.
        Pela MB: CLAUDIO PORTUGAL DE VIVEIROS, CPF 504.430.977-04.
        Documento assinado eletronicamente por Raimundo Lopes Camargos Filho,
        Coordenador-Geral, em 02/10/2020.
        """ + (" conteudo detalhado" * 2000)

        analysis = self.scraper._classify_pt_snapshot(
            _snapshot(text, tables=[["meta", "acao", "periodo"]] * 13),
            {"chosen_documento": "PLANO DE TRABALHO - PT 1 (2735510)"},
        )

        self.assertEqual(analysis["doc_class"], "pt_minuta_documentacao")
        self.assertEqual(analysis["publication_status"], "retained_silver")
        self.assertEqual(analysis["internal_content_score"], 6)

    def test_minuta_com_placeholders_e_sem_assinaturas_permanece_silver(self) -> None:
        self.assert_explicit_minuta(
            "MINUTA DE PLANO DE TRABALHO\nParceiro: XXXXX\nObjeto: A DEFINIR\nInicio: ___"
        )

    def test_minuta_assinada_apenas_pelo_elaborador_permanece_silver(self) -> None:
        self.assert_explicit_minuta(
            "MINUTA\nPLANO DE TRABALHO\nObjeto: cooperacao tecnica.\n"
            "Documento assinado eletronicamente por Servidor Elaborador, Coordenador-Geral."
        )

    def test_minuta_com_nomes_digitados_na_aprovacao_permanece_silver(self) -> None:
        self.assert_explicit_minuta(
            "MINUTA\nPLANO DE TRABALHO\n9. APROVACAO\n"
            "Pelo Censipam: NOME DO DIRIGENTE\nPelo parceiro: NOME DO REPRESENTANTE"
        )

    def test_documentacao_de_minutas_no_titulo_permanece_silver(self) -> None:
        self.assert_explicit_minuta(
            "PLANO DE TRABALHO\nObjeto: cooperacao tecnica.\nMeta 1: execucao conjunta.",
            title="Documentacao - Minutas ACT e Plano de Trabalho",
        )

    def test_pt_definitivo_assinado_sem_rotulo_de_minuta_e_canonico(self) -> None:
        analysis = self.scraper._classify_pt_snapshot(
            _snapshot(
                "PLANO DE TRABALHO - PT 2/2024\n"
                "Objeto: cooperacao tecnica entre os participes.\n"
                "Periodo: JAN2024 a DEZ2025.\n"
                "Meta 1: executar atividades conjuntas.\n"
                "Documento assinado eletronicamente por Representante Um.\n"
                "Documento assinado eletronicamente por Representante Dois."
            )
        )

        self.assertEqual(analysis["doc_class"], "plano_trabalho")
        self.assertEqual(analysis["validation_status"], "valid_for_requested_type")
        self.assertTrue(analysis["is_canonical_candidate"])
        self.assertEqual(analysis["publication_status"], "")

    def test_referencia_historica_fora_do_cabecalho_nao_rebaixa_pt_final(self) -> None:
        header = (
            "PLANO DE TRABALHO - PT 2/2024\n"
            "Objeto: cooperacao tecnica entre os participes para pesquisa aplicada.\n"
            + ("Escopo definitivo aprovado pelas instituicoes. " * 12)
        )
        self.assertGreater(len(header), 400)
        text = header + "Historico: a minuta foi aprovada e substituida por este documento final."

        analysis = self.scraper._classify_pt_snapshot(_snapshot(text))

        self.assertEqual(analysis["doc_class"], "plano_trabalho")
        self.assertEqual(analysis["validation_status"], "valid_for_requested_type")


if __name__ == "__main__":
    unittest.main()
