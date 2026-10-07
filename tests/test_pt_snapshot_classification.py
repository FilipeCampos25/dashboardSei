from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from types import MethodType

os.environ["DEBUG"] = "false"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.rpa.scraping import SEIScraper
from app.services.pt_classifier import classify_pt_snapshot, pt_internal_content_score


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


def _acquisition_context(*, opening: str, access: str, extraction: str, diagnostic: str = "") -> dict:
    stage = {
        "TIMEOUT": "opening",
        "OPEN_FAILED": "opening",
        "IFRAME_UNAVAILABLE": "access",
        "ACCESS_RESTRICTED": "access",
        "EXTRACTION_FAILED": "extraction",
    }.get(diagnostic, "")
    return {
        "acquisition_state": {
            "discovery": "FOUND",
            "opening": opening,
            "access": access,
            "extraction": extraction,
        },
        "acquisition_diagnostic_code": diagnostic,
        "acquisition_diagnostic_stage": stage,
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
            ),
            _acquisition_context(opening="OPENED", access="ACCESSIBLE", extraction="EXTRACTED"),
        )

        self.assertEqual(analysis["doc_class"], "plano_trabalho")
        self.assertEqual(analysis["validation_status"], "valid_for_requested_type")
        self.assertTrue(analysis["is_canonical_candidate"])
        self.assertEqual(analysis["publication_status"], "")

    def test_fixture_iframe_indisponivel_preserva_causa_tecnica(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "documents" / "pt_iframe_unavailable.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        technical = fixture["payload"]["technical"]
        context = _acquisition_context(
            opening="OPENED",
            access=technical["access_state"],
            extraction=technical["extraction_state"],
            diagnostic=fixture["metadata"]["technical_state"],
        )

        analysis = _classifier(internal_score=0)._classify_pt_snapshot(fixture["payload"]["snapshot"], context)

        self.assertEqual(analysis["classification_reason"], "IFRAME_UNAVAILABLE")
        self.assertFalse(analysis["semantic_evaluation_eligible"])

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

    def test_falhas_tecnicas_nao_viram_conteudo_interno_insuficiente(self) -> None:
        cases = (
            ("IFRAME_UNAVAILABLE", "OPENED", "IFRAME_UNAVAILABLE", "NOT_ATTEMPTED"),
            ("TIMEOUT", "TIMEOUT", "UNKNOWN", "NOT_ATTEMPTED"),
            ("ACCESS_RESTRICTED", "OPENED", "ACCESS_RESTRICTED", "NOT_ATTEMPTED"),
            ("EXTRACTION_FAILED", "OPENED", "ACCESSIBLE", "EXTRACTION_FAILED"),
            ("OPEN_FAILED", "OPEN_FAILED", "UNKNOWN", "NOT_ATTEMPTED"),
        )
        for reason, opening, access, extraction in cases:
            with self.subTest(reason=reason):
                analysis = _classifier(internal_score=0)._classify_pt_snapshot(
                    _snapshot(""),
                    _acquisition_context(
                        opening=opening,
                        access=access,
                        extraction=extraction,
                        diagnostic=reason if reason != "OPEN_FAILED" else "",
                    ),
                )

                self.assertEqual(analysis["classification_reason"], reason)
                self.assertNotEqual(analysis["doc_class"], "pt_conteudo_interno_insuficiente")
                self.assertFalse(analysis["semantic_evaluation_eligible"])

    def test_vazio_real_preserva_estado_sem_decisao_semantica(self) -> None:
        analysis = _classifier(internal_score=0)._classify_pt_snapshot(
            _snapshot(""),
            _acquisition_context(opening="OPENED", access="ACCESSIBLE", extraction="EMPTY_CONTENT"),
        )

        self.assertEqual(analysis["classification_reason"], "EMPTY_CONTENT")
        self.assertNotEqual(analysis["doc_class"], "pt_conteudo_interno_insuficiente")
        self.assertFalse(analysis["semantic_evaluation_eligible"])

    def test_conteudo_acessivel_mas_insuficiente_permanece_semantico(self) -> None:
        analysis = _classifier(internal_score=1)._classify_pt_snapshot(
            _snapshot("PLANO DE TRABALHO sem estrutura interna verificavel."),
            _acquisition_context(opening="OPENED", access="ACCESSIBLE", extraction="EXTRACTED"),
        )

        self.assertEqual(analysis["doc_class"], "pt_conteudo_interno_insuficiente")
        self.assertTrue(analysis["semantic_evaluation_eligible"])


class PurePTClassifierTests(unittest.TestCase):
    def test_score_limite_combina_texto_e_tabela_sem_mudar_pesos(self) -> None:
        score = pt_internal_content_score(
            _snapshot("Objeto: apoio. Meta 1.", title="Documento", tables=[["acao"]])
        )

        self.assertEqual(score["internal_content_score"], 3)
        self.assertEqual(score["internal_content_signals"], "objeto|metas|tabelas")
        self.assertEqual(score["internal_content_penalties"], "")

    def test_titulo_forte_sem_conteudo_suficiente_permanece_rejeitado(self) -> None:
        analysis = classify_pt_snapshot(_snapshot("", title="PLANO DE TRABALHO"))

        self.assertEqual(analysis["internal_content_score"], 1)
        self.assertEqual(analysis["doc_class"], "pt_conteudo_interno_insuficiente")
        self.assertEqual(analysis["classification_reason"], "pt_conteudo_interno_insuficiente")

    def test_penalidades_existentes_rejeitam_mesmo_com_score_base_suficiente(self) -> None:
        text = (
            "PLANO DE TRABALHO\nObjeto: cooperacao.\nMeta 1.\n"
            "Atividades e cronograma.\nPeriodo de execucao.\nXX/20XX"
        )
        analysis = classify_pt_snapshot(_snapshot(text, tables=[["meta"]]))

        self.assertEqual(analysis["doc_class"], "pt_conteudo_interno_insuficiente")
        self.assertEqual(analysis["internal_content_penalties"], "placeholder")

    def test_wrapper_legado_e_funcao_pura_sao_equivalentes(self) -> None:
        cases = (
            (_snapshot("PLANO DE TRABALHO\nObjeto: X\nMeta 1\nAtividade\nInicio: 01/01/2026"), None),
            (_snapshot("PLANO DE TRABALHO"), None),
            (_snapshot("MINUTA DE PLANO DE TRABALHO\nObjeto: X\nMeta 1"), None),
            (
                _snapshot(""),
                _acquisition_context(
                    opening="OPENED",
                    access="IFRAME_UNAVAILABLE",
                    extraction="NOT_ATTEMPTED",
                    diagnostic="IFRAME_UNAVAILABLE",
                ),
            ),
        )
        scraper = object.__new__(SEIScraper)
        for snapshot, context in cases:
            with self.subTest(text=snapshot["text"], context=context):
                self.assertEqual(
                    scraper._classify_pt_snapshot(snapshot, context),
                    classify_pt_snapshot(snapshot, context),
                )


if __name__ == "__main__":
    unittest.main()
