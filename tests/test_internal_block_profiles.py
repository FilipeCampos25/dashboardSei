from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


os.environ["DEBUG"] = "false"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.rpa import scraping
from app.documents.types import DocumentTypeSpec


class DummyLogger:
    def debug(self, *args, **kwargs) -> None:
        return

    def info(self, *args, **kwargs) -> None:
        return

    def warning(self, *args, **kwargs) -> None:
        return

    def error(self, *args, **kwargs) -> None:
        return


class DummyHandler:
    def reset_run(self) -> None:
        return

    def process_snapshot(self, **kwargs) -> None:
        return None

    def finalize_run(self, **kwargs) -> None:
        return


def make_document_type(key: str, display_name: str) -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key=key,
        display_name=display_name,
        search_terms=(display_name,),
        tree_match_terms=(display_name,),
        snapshot_prefix=key,
        log_label=display_name,
        cleanup_patterns=(),
        handler=DummyHandler(),
    )


class InternalBlockProfileResolutionTests(unittest.TestCase):
    def _make_scraper(self) -> scraping.SEIScraper:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.descricao_match_mode = "contains"
        return scraper

    def test_parcerias_vigentes_resolves_profile(self) -> None:
        scraper = self._make_scraper()

        profile = scraper._resolve_internal_block_profile("PARCERIAS VIGENTES")

        self.assertIsNotNone(profile)
        self.assertEqual(profile.key, "parcerias_vigentes")

    def test_ted_resolves_profile_with_accent(self) -> None:
        scraper = self._make_scraper()

        profile = scraper._resolve_internal_block_profile("Termo de Execução Descentralizada")

        self.assertIsNotNone(profile)
        self.assertEqual(profile.key, "ted")

    def test_ted_resolves_profile_without_accent(self) -> None:
        scraper = self._make_scraper()

        profile = scraper._resolve_internal_block_profile("Termo de Execucao Descentralizada")

        self.assertIsNotNone(profile)
        self.assertEqual(profile.key, "ted")

    def test_unknown_description_returns_none(self) -> None:
        scraper = self._make_scraper()

        profile = scraper._resolve_internal_block_profile("BLOCO INTERNO DESCONHECIDO")

        self.assertIsNone(profile)


class InternalBlockProfileDocumentTypesTests(unittest.TestCase):
    def _make_scraper(self) -> scraping.SEIScraper:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.document_types_by_key = {
            "pt": make_document_type("pt", "Plano de Trabalho"),
            "act": make_document_type("act", "Acordo de Cooperacao Tecnica"),
            "memorando": make_document_type("memorando", "Memorando"),
            "ted": make_document_type("ted", "TED - Termo de Execucao Descentralizada"),
        }
        scraper._get_document_type = scraping.SEIScraper._get_document_type.__get__(scraper, scraping.SEIScraper)
        return scraper

    def test_parcerias_vigentes_profile_returns_expected_keys(self) -> None:
        scraper = self._make_scraper()
        profile = next(item for item in scraping.INTERNAL_BLOCK_PROFILES if item.key == "parcerias_vigentes")

        document_types = scraper._get_document_types_for_profile(profile)

        self.assertEqual([spec.key for spec in document_types], ["pt", "act", "memorando"])

    def test_ted_profile_returns_expected_key(self) -> None:
        scraper = self._make_scraper()
        profile = next(item for item in scraping.INTERNAL_BLOCK_PROFILES if item.key == "ted")

        document_types = scraper._get_document_types_for_profile(profile)

        self.assertEqual([spec.key for spec in document_types], ["ted"])

    def test_ted_does_not_appear_in_parcerias_vigentes_profile(self) -> None:
        scraper = self._make_scraper()
        profile = next(item for item in scraping.INTERNAL_BLOCK_PROFILES if item.key == "parcerias_vigentes")

        document_types = scraper._get_document_types_for_profile(profile)

        self.assertNotIn("ted", [spec.key for spec in document_types])

    def test_pt_act_memorando_do_not_appear_in_ted_profile(self) -> None:
        scraper = self._make_scraper()
        profile = next(item for item in scraping.INTERNAL_BLOCK_PROFILES if item.key == "ted")

        document_types = scraper._get_document_types_for_profile(profile)

        self.assertEqual([spec.key for spec in document_types], ["ted"])
        self.assertNotIn("pt", [spec.key for spec in document_types])
        self.assertNotIn("act", [spec.key for spec in document_types])
        self.assertNotIn("memorando", [spec.key for spec in document_types])


class DummyProfiler:
    def start_span(self, name: str) -> None:
        return

    def end_span(self, name: str) -> None:
        return


class InternalBlockProfileMainLoopTests(unittest.TestCase):
    def _make_scraper(self) -> scraping.SEIScraper:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper.base_url = "https://sei.example.local"
        scraper.driver = Mock()
        scraper.found = set()
        scraper.document_types_by_key = {
            "pt": make_document_type("pt", "Plano de Trabalho"),
            "act": make_document_type("act", "Acordo de Cooperacao Tecnica"),
            "memorando": make_document_type("memorando", "Memorando"),
            "ted": make_document_type("ted", "TED - Termo de Execucao Descentralizada"),
        }
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._resolve_internal_block_profile = scraping.SEIScraper._resolve_internal_block_profile.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._get_document_type = scraping.SEIScraper._get_document_type.__get__(scraper, scraping.SEIScraper)
        scraper._get_document_types_for_profile = scraping.SEIScraper._get_document_types_for_profile.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._get_document_types_for_process = Mock(side_effect=AssertionError("should not be used in main loop"))
        scraper._reset_candidate_screening_stats = Mock()
        scraper._prepare_output_dir_for_run = Mock()
        scraper._wait_for_manual_login = Mock()
        scraper._login_if_possible = Mock()
        scraper._remember_main_window_handle = Mock()
        scraper._close_popup_if_exists = Mock()
        scraper._open_interno_menu = Mock()
        scraper._click_selected_interno = Mock(return_value=True)
        scraper._collect_preview_if_parcerias_vigencias = Mock()
        scraper._list_processos = Mock(return_value=["60093.000015/2020-60"])
        scraper._clear_process_filter_state = Mock()
        scraper._switch_to_main_window_context = Mock()
        scraper._open_processo = Mock()
        scraper._wait_page_ready_in_processo = Mock()
        scraper._click_abrir_todas_as_pastas = Mock()
        scraper._run_document_search_for_process = Mock()
        scraper._close_current_tab_and_back = Mock()
        scraper._finalize_document_runs = Mock()
        scraper._log_candidate_screening_summary = Mock()
        scraper._export_performance_analysis = Mock()
        return scraper

    def _run_flow(self, scraper: scraping.SEIScraper) -> list[str]:
        with patch("app.rpa.scraping.PerformanceProfiler", new=DummyProfiler), patch(
            "app.rpa.scraping.set_active_profiler"
        ), patch("builtins.print"):
            return scraper.run_full_flow()

    def test_main_loop_uses_parcerias_profile_document_types(self) -> None:
        scraper = self._make_scraper()
        scraper._select_guided_internos_by_descricao = Mock(
            return_value=[
                (
                    scraping.InternoRow(
                        numero_interno="I-001",
                        descricao="PARCERIAS VIGENTES",
                        descricao_normalizada="PARCERIAS VIGENTES",
                        link=None,
                        page=1,
                        row_index=1,
                    ),
                    "PARCERIAS VIGENTES",
                    "https://sei.example.local/lista",
                )
            ]
        )

        self._run_flow(scraper)

        scraper._collect_preview_if_parcerias_vigencias.assert_called_once()
        self.assertEqual(
            [call.args[1].key for call in scraper._run_document_search_for_process.call_args_list],
            ["pt", "act", "memorando"],
        )

    def test_main_loop_uses_ted_profile_document_types(self) -> None:
        scraper = self._make_scraper()
        scraper._select_guided_internos_by_descricao = Mock(
            return_value=[
                (
                    scraping.InternoRow(
                        numero_interno="I-002",
                        descricao="Termo de Execucao Descentralizada",
                        descricao_normalizada="TERMO DE EXECUCAO DESCENTRALIZADA",
                        link=None,
                        page=1,
                        row_index=1,
                    ),
                    "TERMO DE EXECUCAO DESCENTRALIZADA",
                    "https://sei.example.local/lista",
                )
            ]
        )

        self._run_flow(scraper)

        scraper._collect_preview_if_parcerias_vigencias.assert_not_called()
        scraper._list_processos.assert_called_once()
        self.assertEqual(
            [call.args[1].key for call in scraper._run_document_search_for_process.call_args_list],
            ["ted"],
        )

    def test_main_loop_skips_unknown_profile_without_searching_documents(self) -> None:
        scraper = self._make_scraper()
        scraper._select_guided_internos_by_descricao = Mock(
            return_value=[
                (
                    scraping.InternoRow(
                        numero_interno="I-003",
                        descricao="BLOCO INTERNO DESCONHECIDO",
                        descricao_normalizada="BLOCO INTERNO DESCONHECIDO",
                        link=None,
                        page=1,
                        row_index=1,
                    ),
                    "BLOCO INTERNO DESCONHECIDO",
                    "https://sei.example.local/lista",
                )
            ]
        )

        self._run_flow(scraper)

        scraper._click_selected_interno.assert_not_called()
        scraper._run_document_search_for_process.assert_not_called()


if __name__ == "__main__":
    unittest.main()
