from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from types import SimpleNamespace

from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By


os.environ["DEBUG"] = "false"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.types import DocumentTypeSpec
from app.rpa import scraping
from app.rpa.sei import document_search
from app.rpa.sei import document_text_extractor


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

    def process_snapshot(self, **kwargs: Any) -> None:
        return None

    def finalize_run(self, **kwargs: Any) -> None:
        return


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += float(seconds)


class PopupElement:
    def __init__(self, *, fail_clicks: int = 0) -> None:
        self.fail_clicks = fail_clicks
        self.clicks = 0

    def is_displayed(self) -> bool:
        return True

    def is_enabled(self) -> bool:
        return True

    def click(self) -> None:
        self.clicks += 1
        if self.clicks <= self.fail_clicks:
            raise WebDriverException("popup not ready")


class PopupDriver:
    def __init__(self, elements: list[PopupElement]) -> None:
        self.elements = elements
        self.find_calls = 0

    def find_elements(self, by: Any, value: str) -> list[PopupElement]:
        self.find_calls += 1
        if by == By.XPATH:
            return self.elements
        return []


class FakeRow:
    def __init__(self, text: str) -> None:
        self.text = text


class FakePaginationButton:
    def __init__(self, *, stale_after: float | None = None, clock: FakeClock | None = None) -> None:
        self.stale_after = stale_after
        self.clock = clock
        self.clicked = False

    def get_attribute(self, name: str) -> str:
        return ""

    def click(self) -> None:
        self.clicked = True

    def is_enabled(self) -> bool:
        if self.stale_after is not None and self.clock is not None and self.clock.time() >= self.stale_after:
            raise StaleElementReferenceException()
        return True


class FakeFrame:
    def __init__(self, frame_id: str, *, frame_name: str | None = None, src: str = "") -> None:
        self.frame_id = frame_id
        self.frame_name = frame_name or frame_id
        self.src = src

    def get_attribute(self, name: str) -> str:
        if name in {"id", "name"}:
            return self.frame_id if name == "id" else self.frame_name
        if name == "src":
            return self.src
        return ""


class FakeSwitchTo:
    def __init__(self, driver: "FakeSearchDriver") -> None:
        self.driver = driver

    def default_content(self) -> None:
        self.driver.context = ()

    def frame(self, frame: FakeFrame) -> None:
        self.driver.context = self.driver.context + (frame.frame_id,)


class FakeSearchElement:
    pass


class FakeSearchDriver:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.context: tuple[str, ...] = ()
        self.switch_to = FakeSwitchTo(self)
        self.root_frame = FakeFrame("root", src="https://sei.defesa.gov.br/root")
        self.inner_frame = FakeFrame("inner", src="https://sei.defesa.gov.br/inner")
        self.target = FakeSearchElement()
        self.current_url = "https://sei.defesa.gov.br/processo"
        self.title = "SEI - Processo"

    def find_elements(self, by: Any, value: str) -> list[Any]:
        if by == By.TAG_NAME and value == "iframe":
            if self.context == ():
                return [self.root_frame]
            if self.context == ("root",):
                return [self.inner_frame]
            return []

        if by == By.XPATH and value == "//target":
            if self.context == ("root", "inner") and self.clock.time() >= 0.2:
                return [self.target]
            return []

        return []


class StagnantSearchDriver:
    def __init__(self) -> None:
        self.context: tuple[str, ...] = ()
        self.switch_to = FakeSwitchTo(self)
        self.root_frames = [
            FakeFrame("ifrArvore", src="https://sei.defesa.gov.br/arvore"),
            FakeFrame(
                "ifrConteudoVisualizacao",
                src="https://sei.defesa.gov.br/visualizacao",
            ),
        ]
        self.tree_inner = [FakeFrame("ifrPasta", src="https://sei.defesa.gov.br/arvore_inner")]
        self.content_inner = [
            FakeFrame(
                "ifrVisualizacao",
                src="https://sei.defesa.gov.br/visualizacao_inner",
            )
        ]
        self.current_url = "https://sei.defesa.gov.br/processo"
        self.title = "SEI - Processo"

    def find_elements(self, by: Any, value: str) -> list[Any]:
        if by == By.TAG_NAME and value == "iframe":
            if self.context == ():
                return self.root_frames
            if self.context == ("ifrArvore",):
                return self.tree_inner
            if self.context == ("ifrConteudoVisualizacao",):
                return self.content_inner
            return []
        return []


class ChangingSearchDriver(StagnantSearchDriver):
    def __init__(self, clock: FakeClock) -> None:
        super().__init__()
        self.clock = clock
        self.target = FakeSearchElement()

    def find_elements(self, by: Any, value: str) -> list[Any]:
        if by == By.TAG_NAME and value == "iframe":
            if self.context == ():
                return self.root_frames
            if self.context == ("ifrArvore",):
                return self.tree_inner
            if self.context == ("ifrConteudoVisualizacao",):
                if self.clock.time() >= 0.4:
                    return [
                        FakeFrame(
                            "ifrVisualizacao",
                            src="https://sei.defesa.gov.br/procedimento_pesquisar?id=2",
                        )
                    ]
                return self.content_inner
            return []

        if by == By.XPATH and value == "//target":
            if (
                self.context == ("ifrConteudoVisualizacao", "ifrVisualizacao")
                and self.clock.time() >= 0.4
            ):
                return [self.target]
            return []

        return []


class ActiveSearchPageDriver(StagnantSearchDriver):
    def __init__(self) -> None:
        super().__init__()
        self.root_frames[1] = FakeFrame(
            "ifrConteudoVisualizacao",
            src="https://sei.defesa.gov.br/controlador.php?acao=procedimento_pesquisar",
        )


class ResultElement:
    def __init__(self, text: str) -> None:
        self.text = text

    def find_elements(self, by: Any, value: str) -> list[Any]:
        if by == By.XPATH and ".//a" in value:
            return [self]
        return []

    def get_attribute(self, name: str) -> str:
        return ""


class ResultFallbackDriver:
    def find_elements(self, by: Any, value: str) -> list[Any]:
        if by != By.XPATH:
            return []
        if "pesquisaTituloRegistro" in value:
            return []
        if "pesquisaResultado" in value and "//a" in value:
            return [ResultElement("PT fallback")]
        return []


class FakeDriverSwitchOnly:
    def __init__(self) -> None:
        self.default_content_calls = 0
        self.switch_to = self
        self.current_url = "https://sei.defesa.gov.br/processo"
        self.title = "SEI - Processo"

    def default_content(self) -> None:
        self.default_content_calls += 1


class FakeScraperDriver:
    def __init__(self) -> None:
        self.current_url = "https://sei.defesa.gov.br/processo"
        self.title = "SEI - Processo"
        self.window_handles = ["main"]
        self.current_window_handle = "main"
        self.switch_to = SimpleNamespace(default_content=Mock())


class SimpleSelectors:
    def __init__(self, mapping: dict[str, Any]) -> None:
        self.mapping = mapping

    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.mapping
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node


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


class WaitOptimizationTests(unittest.TestCase):
    def test_click_optional_popup_returns_immediately_when_absent(self) -> None:
        clock = FakeClock()
        driver = PopupDriver(elements=[])
        with patch("app.rpa.scraping.time.time", side_effect=clock.time), patch(
            "app.rpa.scraping.time.sleep", side_effect=clock.sleep
        ) as sleep_mock:
            result = scraping._click_optional_popup(driver, "//popup")

        self.assertFalse(result)
        self.assertEqual(driver.find_calls, 1)
        sleep_mock.assert_not_called()

    def test_click_optional_popup_retries_when_present_but_not_ready(self) -> None:
        clock = FakeClock()
        element = PopupElement(fail_clicks=1)
        driver = PopupDriver(elements=[element])
        with patch("app.rpa.scraping.time.time", side_effect=clock.time), patch(
            "app.rpa.scraping.time.sleep", side_effect=clock.sleep
        ):
            result = scraping._click_optional_popup(driver, "//popup")

        self.assertTrue(result)
        self.assertEqual(element.clicks, 2)
        self.assertGreater(clock.time(), 0.0)

    def test_wait_for_page_signature_change_returns_early_when_rows_change(self) -> None:
        clock = FakeClock()
        previous_signature = scraping._build_rows_signature([FakeRow("linha atual")])
        button = FakePaginationButton(clock=clock)

        def read_rows() -> tuple[str, int] | None:
            if clock.time() < 0.1:
                return scraping._build_rows_signature([FakeRow("linha atual")])
            return scraping._build_rows_signature([FakeRow("linha seguinte")])

        with patch("app.rpa.scraping.time.time", side_effect=clock.time), patch(
            "app.rpa.scraping.time.sleep", side_effect=clock.sleep
        ):
            changed = scraping._wait_for_page_signature_change(
                read_rows,
                previous_signature,
                button,
                timeout_seconds=0.5,
                poll_interval_seconds=0.1,
            )

        self.assertTrue(changed)

    def test_click_next_page_keeps_sleep_fallback_when_no_signal_is_detectable(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.selectors = {"interno": {"paginacao_proxima": "//next", "tabela_blocos_rows": "//rows"}}
        scraper.logger = DummyLogger()
        button = FakePaginationButton()

        def fake_find_elements_any_context(xpath: str) -> list[Any]:
            if xpath == "//next":
                return [button]
            if xpath == "//rows":
                return [FakeRow("linha atual")]
            return []

        scraper._find_elements_any_context = fake_find_elements_any_context  # type: ignore[attr-defined]

        with patch("app.rpa.scraping._wait_for_page_signature_change", return_value=False), patch(
            "app.rpa.scraping.time.sleep"
        ) as sleep_mock:
            result = scraping.SEIScraper._click_next_page_if_available(scraper, page=1)

        self.assertTrue(result)
        sleep_mock.assert_called_once_with(scraping.PAGINATION_SETTLE_SLEEP_SECONDS)

    def test_find_first_in_pesquisa_context_uses_outer_deadline_without_inner_polling(self) -> None:
        clock = FakeClock()
        driver = FakeSearchDriver(clock)
        logger = DummyLogger()

        with patch(
            "app.rpa.sei.document_search._find_elements_in_current_context"
        ) as waited_lookup_mock, patch(
            "app.rpa.sei.document_search.time.time", side_effect=clock.time
        ), patch(
            "app.rpa.sei.document_search.time.sleep", side_effect=clock.sleep
        ):
            result = document_search._find_first_in_pesquisa_context(
                driver=driver,
                logger=logger,
                timeout_seconds=1,
                search_xpaths=["//target"],
                element_name="target",
            )

        self.assertIs(result, driver.target)
        waited_lookup_mock.assert_not_called()
        self.assertEqual(clock.time(), 0.2)

    def test_find_first_in_pesquisa_context_stops_early_when_context_stagnates(self) -> None:
        clock = FakeClock()
        driver = StagnantSearchDriver()
        logger = DummyLogger()

        with patch("app.rpa.sei.document_search.time.time", side_effect=clock.time), patch(
            "app.rpa.sei.document_search.time.sleep", side_effect=clock.sleep
        ):
            with self.assertRaises(TimeoutException) as ctx:
                document_search._find_first_in_pesquisa_context(
                    driver=driver,
                    logger=logger,
                    timeout_seconds=20,
                    search_xpaths=["//target"],
                    element_name="anchor do filtro",
                )

        self.assertIn("motivo=estagnacao_do_contexto", str(ctx.exception))
        self.assertLess(clock.time(), 5.0)

    def test_find_first_in_pesquisa_context_keeps_working_when_context_changes(self) -> None:
        clock = FakeClock()
        driver = ChangingSearchDriver(clock)
        logger = DummyLogger()

        with patch("app.rpa.sei.document_search.time.time", side_effect=clock.time), patch(
            "app.rpa.sei.document_search.time.sleep", side_effect=clock.sleep
        ):
            result = document_search._find_first_in_pesquisa_context(
                driver=driver,
                logger=logger,
                timeout_seconds=20,
                search_xpaths=["//target"],
                element_name="anchor do filtro",
            )

        self.assertIs(result, driver.target)
        self.assertLess(clock.time(), 2.0)

    def test_find_first_in_pesquisa_context_does_not_stagnate_on_active_search_page(self) -> None:
        clock = FakeClock()
        driver = ActiveSearchPageDriver()
        logger = DummyLogger()

        with patch("app.rpa.sei.document_search.time.time", side_effect=clock.time), patch(
            "app.rpa.sei.document_search.time.sleep", side_effect=clock.sleep
        ):
            with self.assertRaises(TimeoutException) as ctx:
                document_search._find_first_in_pesquisa_context(
                    driver=driver,
                    logger=logger,
                    timeout_seconds=4,
                    search_xpaths=["//target"],
                    element_name="anchor do filtro",
                )

        self.assertIn("motivo=timeout", str(ctx.exception))
        self.assertIn("estado=search_results", str(ctx.exception))
        self.assertGreaterEqual(clock.time(), 4.0)

    def test_get_primeiro_resultado_uses_fallback_xpath_when_primary_row_class_varies(self) -> None:
        driver = ResultFallbackDriver()
        selectors = SimpleSelectors({"pesquisar_processos": {}})

        result = document_search._get_primeiro_resultado(
            driver=driver,
            selectors=selectors,
            timeout_seconds=1,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.text, "PT fallback")

    def test_extract_document_snapshot_short_circuits_to_file_fallback_when_html_stagnates(self) -> None:
        clock = FakeClock()
        driver = FakeDriverSwitchOnly()
        logger = DummyLogger()
        placeholder_state = {
            "url": "https://sei.defesa.gov.br/visualizacao",
            "title": "Documento",
            "text": "Clique aqui para visualizar o conteudo deste documento em uma nova janela.",
        }

        with patch(
            "app.rpa.sei.document_text_extractor._switch_to_visualizacao_iframe",
            return_value=True,
        ), patch(
            "app.rpa.sei.document_text_extractor._read_visualizacao_state",
            return_value=placeholder_state,
        ), patch(
            "app.rpa.sei.document_text_extractor._extract_pdf_text_via_anchor_fallback",
            return_value={
                "text": "conteudo pdf extraido",
                "mode": "pdf_native",
                "source_url": "https://sei.defesa.gov.br/documento.pdf",
            },
        ) as fallback_mock, patch(
            "app.rpa.sei.document_text_extractor._extract_tables_in_current_context",
            return_value=[],
        ), patch(
            "app.rpa.sei.document_text_extractor.time.time",
            side_effect=clock.time,
        ), patch(
            "app.rpa.sei.document_text_extractor.time.sleep",
            side_effect=clock.sleep,
        ):
            snapshot = document_text_extractor.extract_document_snapshot(driver, logger=logger)

        self.assertEqual(snapshot["extraction_mode"], "pdf_native")
        self.assertEqual(snapshot["text"], "conteudo pdf extraido")
        fallback_mock.assert_called_once()
        self.assertLess(clock.time(), 2.0)

    def test_extract_document_snapshot_keeps_html_path_when_content_progresses(self) -> None:
        clock = FakeClock()
        driver = FakeDriverSwitchOnly()
        logger = DummyLogger()
        states = [
            {
                "url": "https://sei.defesa.gov.br/visualizacao",
                "title": "Documento",
                "text": "Clique aqui para visualizar o conteudo deste documento em uma nova janela.",
            },
            {
                "url": "https://sei.defesa.gov.br/visualizacao",
                "title": "Documento",
                "text": "Conteudo final do documento pronto para leitura.",
            },
        ]

        with patch(
            "app.rpa.sei.document_text_extractor._switch_to_visualizacao_iframe",
            return_value=True,
        ), patch(
            "app.rpa.sei.document_text_extractor._read_visualizacao_state",
            side_effect=states,
        ), patch(
            "app.rpa.sei.document_text_extractor._extract_pdf_text_via_anchor_fallback",
            return_value={},
        ) as fallback_mock, patch(
            "app.rpa.sei.document_text_extractor._extract_tables_in_current_context",
            return_value=[],
        ), patch(
            "app.rpa.sei.document_text_extractor.time.time",
            side_effect=clock.time,
        ), patch(
            "app.rpa.sei.document_text_extractor.time.sleep",
            side_effect=clock.sleep,
        ):
            snapshot = document_text_extractor.extract_document_snapshot(driver, logger=logger)

        self.assertEqual(snapshot["extraction_mode"], "html_dom")
        self.assertIn("Conteudo final", snapshot["text"])
        fallback_mock.assert_not_called()
        self.assertLess(clock.time(), 2.0)

    def test_ensure_document_search_open_restores_before_reusing_degraded_filter(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper.selectors = {}
        scraper._process_filter_degraded = {"60093.000015/2020-60": True}
        scraper._process_filter_recovery_attempts = {}
        document_type = make_document_type("act", "Acordo de Cooperacao Tecnica")
        call_order: list[str] = []

        with patch.object(
            scraping.SEIScraper,
            "_restore_process_base_context",
            autospec=True,
            side_effect=lambda self, *args, **kwargs: call_order.append("restore"),
        ), patch.object(
            scraping.SEIScraper,
            "_click_pesquisar_no_processo",
            autospec=True,
            side_effect=lambda self: call_order.append("click"),
        ), patch("app.rpa.scraping.toolbar_actions.wait_pesquisa_anchor", return_value=None):
            scraping.SEIScraper._ensure_document_search_open(
                scraper,
                "60093.000015/2020-60",
                document_type,
            )

        self.assertEqual(call_order[:2], ["restore", "click"])
        self.assertNotIn("60093.000015/2020-60", scraper._process_filter_degraded)

    def test_ensure_document_search_open_does_not_loop_after_second_stagnation(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper.selectors = {}
        scraper._process_filter_degraded = {"60093.000015/2020-60": True}
        scraper._process_filter_recovery_attempts = {}
        document_type = make_document_type("act", "Acordo de Cooperacao Tecnica")
        stagnation_exc = TimeoutException(
            "Timeout aguardando elemento no contexto de pesquisa: elemento=anchor do filtro timeout=20s contextos=[] motivo=estagnacao_do_contexto"
        )
        restore_calls: list[str] = []

        with patch.object(
            scraping.SEIScraper,
            "_restore_process_base_context",
            autospec=True,
            side_effect=lambda self, *args, **kwargs: restore_calls.append("restore"),
        ), patch.object(
            scraping.SEIScraper,
            "_click_pesquisar_no_processo",
            autospec=True,
            side_effect=stagnation_exc,
        ):
            with self.assertRaises(TimeoutException):
                scraping.SEIScraper._ensure_document_search_open(
                    scraper,
                    "60093.000015/2020-60",
                    document_type,
                )

        self.assertEqual(restore_calls, ["restore"])

    def test_reset_search_context_light_reuses_filter_without_reload(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper.timeout_seconds = 20
        search_input = Mock()

        def get_attribute(name: str) -> str:
            return "valor anterior" if name == "value" else ""

        search_input.get_attribute.side_effect = get_attribute
        search_input.text = ""
        search_input.clear = Mock()
        scraper.driver = Mock(find_elements=Mock(return_value=[search_input]))
        scraper.driver.switch_to = SimpleNamespace(default_content=Mock())
        scraper.selectors = {}
        scraper._click_pesquisar_no_processo = Mock()

        with patch("app.rpa.scraping.document_search._switch_to_pesquisa_context", return_value=None) as switch_mock:
            scraping.SEIScraper.reset_search_context_light(
                scraper,
                "60093.000015/2020-60",
                reason="alias alternativo",
            )

        switch_mock.assert_called_once()
        scraper._click_pesquisar_no_processo.assert_not_called()
        search_input.clear.assert_called_once()
        self.assertTrue(
            any("reset_context_light usado" in str(call.args[0]) for call in scraper.logger.info.call_args_list)
        )

    def test_reset_search_context_with_fallback_usa_reload_quando_contexto_perdido(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper.selectors = {}
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper.reset_search_context_light = Mock(side_effect=StaleElementReferenceException("iframe stale"))
        scraper._restore_process_base_context = Mock()
        scraper._ensure_document_search_open = Mock()
        document_type = make_document_type("act", "Acordo de Cooperacao Tecnica")

        scraping.SEIScraper._reset_search_context_with_fallback(
            scraper,
            "60093.000015/2020-60",
            document_type,
            process_url="https://sei.defesa.gov.br/processo",
            reason="alias alternativo",
        )

        scraper._restore_process_base_context.assert_called_once()
        scraper._ensure_document_search_open.assert_called_once_with(
            "60093.000015/2020-60",
            document_type,
        )
        self.assertTrue(
            any(
                "reload completo usado (fallback)" in str(call.args[0])
                for call in scraper.logger.warning.call_args_list
            )
        )

    def test_busca_pt_reabre_filtro_em_sessao_limpa_apos_zero_resultado(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper.selectors = {}
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_search_terms = scraping.SEIScraper._iter_unique_search_terms.__get__(scraper, scraping.SEIScraper)
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._search_pt_document_in_filter = scraping.SEIScraper._search_pt_document_in_filter.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._collect_pesquisa_diagnostics = lambda: {
            "state": "search_results",
            "current_url": "https://sei.defesa.gov.br/processo",
            "current_title": "SEI - Processo",
            "ifrConteudoVisualizacao_src": "conteudo",
            "ifrVisualizacao_src": "visualizacao",
            "primary_result_count": 0,
            "fallback_result_count": 0,
        }
        scraper._format_pesquisa_diagnostics = scraping.SEIScraper._format_pesquisa_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_pt_filter_diagnostics = scraping.SEIScraper._log_pt_filter_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        document_type = DocumentTypeSpec(
            key="pt",
            display_name="Plano de Trabalho",
            search_terms=(
                "PLANO DE TRABALHO - PT",
                "Plano de Trabalho",
            ),
            tree_match_terms=("PLANO DE TRABALHO",),
            snapshot_prefix="pt",
            log_label="PT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )

        with patch.object(
            scraper,
            "_search_document_in_filter",
            side_effect=[
                (
                    [],
                    scraper._build_collection_context(
                        found=False,
                        found_in="filter",
                        search_term="PLANO DE TRABALHO - PT",
                        results_count=0,
                        selection_reason="no_results_in_filter",
                        selection_detail="sem resultados para o alias inicial",
                    ),
                ),
                (
                    [document_search.SearchHit(protocolo="123", total_resultados=1)],
                    scraper._build_collection_context(
                        found=True,
                        found_in="filter",
                        search_term="Plano de Trabalho",
                        results_count=1,
                        chosen_documento="123",
                        selection_reason="primeiro_resultado_mais_recente",
                        selection_detail="position=1 total=1",
                    ),
                ),
            ],
        ), patch.object(
            scraper,
            "_reset_search_context_with_fallback",
        ) as reset_mock:
            candidate_groups, collection_context = scraping.SEIScraper._search_pt_document_in_filter(
                scraper,
                "60090.000269/2020-16",
                document_type,
            )

        self.assertEqual(len(candidate_groups), 1)
        self.assertEqual(candidate_groups[0][0], "Plano de Trabalho")
        self.assertEqual(candidate_groups[0][1][0].protocolo, "123")
        self.assertTrue(collection_context["found"])
        self.assertEqual(collection_context["search_term"], "Plano de Trabalho")
        reset_mock.assert_called_once()

    def test_busca_pt_cai_para_arvore_apos_estagnacao_no_filtro(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_search_terms = scraping.SEIScraper._iter_unique_search_terms.__get__(scraper, scraping.SEIScraper)
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._collect_pesquisa_diagnostics = lambda: {
            "state": "inactive",
            "current_url": "https://sei.defesa.gov.br/processo",
            "current_title": "SEI - Processo",
            "ifrConteudoVisualizacao_src": "",
            "ifrVisualizacao_src": "",
            "primary_result_count": 0,
            "fallback_result_count": 0,
        }
        scraper._format_pesquisa_diagnostics = scraping.SEIScraper._format_pesquisa_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_pt_filter_diagnostics = scraping.SEIScraper._log_pt_filter_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._record_document_search_outcome = Mock()
        scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
        scraper._open_document_via_tree = Mock(return_value=True)
        scraper.buscar_documento_mais_recente_no_filtro = lambda *args, **kwargs: (_ for _ in ()).throw(
            TimeoutException(
                "Timeout aguardando elemento no contexto de pesquisa: elemento=anchor do filtro timeout=20s contextos=[] motivo=estagnacao_do_contexto"
            )
        )
        scraper.abrir_documento_mais_recente_no_filtro = lambda *args, **kwargs: None
        scraper._switch_to_newly_opened_window = lambda *args, **kwargs: None
        scraper._extract_and_process_document_snapshot = lambda *args, **kwargs: None
        document_type = make_document_type("pt", "Plano de Trabalho")

        def stale_search(*args: Any, **kwargs: Any) -> tuple[list[document_search.SearchHit], dict[str, Any]]:
            scraper._process_filter_degraded["60093.000015/2020-60"] = True
            return (
                [],
                scraper._build_collection_context(
                    found=False,
                    found_in="filter",
                    search_term="Plano de Trabalho",
                    results_count=0,
                    selection_reason="search_context_stagnation",
                    selection_detail="timeout no contexto de pesquisa",
                    extraction_error="motivo=estagnacao_do_contexto",
                ),
            )

        scraper._search_document_in_filter = Mock(side_effect=stale_search)

        scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
            scraper,
            "60093.000015/2020-60",
            document_type,
        )

        self.assertTrue(scraper._process_filter_degraded.get("60093.000015/2020-60"))
        scraper._open_document_via_tree.assert_called_once()
        scraper._record_document_search_outcome.assert_not_called()

    def test_busca_pt_registra_not_found_so_apos_filtro_e_arvore_falharem(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_search_terms = scraping.SEIScraper._iter_unique_search_terms.__get__(scraper, scraping.SEIScraper)
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._collect_pesquisa_diagnostics = lambda: {
            "state": "search_results",
            "current_url": "https://sei.defesa.gov.br/processo",
            "current_title": "SEI - Processo",
            "ifrConteudoVisualizacao_src": "conteudo",
            "ifrVisualizacao_src": "visualizacao",
            "primary_result_count": 0,
            "fallback_result_count": 0,
        }
        scraper._format_pesquisa_diagnostics = scraping.SEIScraper._format_pesquisa_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_pt_filter_diagnostics = scraping.SEIScraper._log_pt_filter_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._record_document_search_outcome = Mock()
        scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
        scraper._open_document_via_tree = Mock(return_value=False)
        scraper._search_document_in_filter = Mock(
            side_effect=[
                (
                    [],
                    scraper._build_collection_context(
                        found=False,
                        found_in="filter",
                        search_term="PLANO DE TRABALHO - PT",
                        results_count=0,
                        selection_reason="no_results_in_filter",
                        selection_detail="sem resultados para o alias inicial",
                    ),
                ),
                (
                    [],
                    scraper._build_collection_context(
                        found=False,
                        found_in="filter",
                        search_term="Plano de Trabalho - PT",
                        results_count=0,
                        selection_reason="no_results_in_filter",
                        selection_detail="sem resultados para o alias alternativo",
                    ),
                ),
            ]
        )
        scraper.abrir_documento_mais_recente_no_filtro = lambda *args, **kwargs: None
        scraper._switch_to_newly_opened_window = lambda *args, **kwargs: None
        scraper._extract_and_process_document_snapshot = lambda *args, **kwargs: None
        scraper._reset_search_context_with_fallback = lambda *args, **kwargs: None
        document_type = DocumentTypeSpec(
            key="pt",
            display_name="Plano de Trabalho",
            search_terms=(
                "PLANO DE TRABALHO - PT",
                "Plano de Trabalho - PT",
            ),
            tree_match_terms=("PLANO DE TRABALHO",),
            snapshot_prefix="pt",
            log_label="PT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )

        scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
            scraper,
            "60093.000015/2020-60",
            document_type,
        )

        scraper._open_document_via_tree.assert_called_once()
        scraper._record_document_search_outcome.assert_called_once()
        outcome = scraper._record_document_search_outcome.call_args.args[2]
        self.assertEqual(outcome["selection_reason"], "not_found_after_filter_and_tree")

    def test_busca_tipos_baixa_relevancia_pula_fallback_da_arvore_apos_zero_resultado(self) -> None:
        cases = (
            (
                "ted",
                "Termo de Execucao Descentralizada",
                "TED",
                ("TED - Termo de Execucao Descentralizada",),
                ("termo de execucao descentralizada",),
            ),
            (
                "memorando",
                "Memorando de Entendimentos",
                "MEMORANDO",
                ("Memorando de Entendimentos",),
                ("memorando de entendimentos",),
            ),
        )

        for key, display_name, log_label, search_terms, tree_match_terms in cases:
            with self.subTest(document_type=key):
                scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
                scraper.logger = Mock()
                scraper.timeout_seconds = 20
                scraper.driver = FakeScraperDriver()
                scraper._process_filter_degraded = {}
                scraper._process_filter_recovery_attempts = {}
                scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
                scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(
                    scraper,
                    scraping.SEIScraper,
                )
                scraper._record_document_search_outcome = Mock()
                scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
                scraper._open_document_via_tree = Mock(return_value=True)
                scraper._search_document_in_filter = Mock(
                    return_value=(
                        [],
                        scraper._build_collection_context(
                            found=False,
                            found_in="filter",
                            search_term=search_terms[0],
                            results_count=0,
                            selection_reason="no_results_in_filter",
                            selection_detail="sem resultados no filtro",
                        ),
                    )
                )
                document_type = DocumentTypeSpec(
                    key=key,
                    display_name=display_name,
                    search_terms=search_terms,
                    tree_match_terms=tree_match_terms,
                    snapshot_prefix=key,
                    log_label=log_label,
                    cleanup_patterns=(),
                    handler=DummyHandler(),
                )

                result = scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
                    scraper,
                    "60093.000015/2020-60",
                    document_type,
                )

                self.assertFalse(result)
                scraper._open_document_via_tree.assert_not_called()
                scraper._record_document_search_outcome.assert_called_once()
                outcome = scraper._record_document_search_outcome.call_args.args[2]
                self.assertEqual(outcome["selection_reason"], "not_found_after_filter")
                self.assertEqual(outcome["selection_detail"], "nao encontrado no filtro")
                self.assertTrue(
                    any(
                        "fallback skip: baixa relevância do tipo." in str(call.args[0])
                        for call in scraper.logger.info.call_args_list
                    )
                )

    def test_busca_act_reabre_filtro_em_sessao_limpa_apos_zero_resultado(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper.selectors = {}
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_search_terms = scraping.SEIScraper._iter_unique_search_terms.__get__(scraper, scraping.SEIScraper)
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._search_document_in_filter = scraping.SEIScraper._search_document_in_filter.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._collect_pesquisa_diagnostics = lambda: {
            "state": "search_results",
            "current_url": "https://sei.defesa.gov.br/processo",
            "current_title": "SEI - Processo",
            "ifrConteudoVisualizacao_src": "conteudo",
            "ifrVisualizacao_src": "visualizacao",
            "primary_result_count": 0,
            "fallback_result_count": 0,
        }
        scraper._format_pesquisa_diagnostics = scraping.SEIScraper._format_pesquisa_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_filter_diagnostics = scraping.SEIScraper._log_filter_diagnostics.__get__(
            scraper,
            scraping.SEIScraper,
        )
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=(
                "ACORDO DE COOPERACAO TECNICA - ACT",
                "Memorando de Entendimentos",
            ),
            tree_match_terms=("memorando de entendimentos", "act"),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )

        with patch.object(
            scraper,
            "_search_document_in_filter",
            side_effect=[
                (
                    [],
                    scraper._build_collection_context(
                        found=False,
                        found_in="filter",
                        search_term="ACORDO DE COOPERACAO TECNICA - ACT",
                        results_count=0,
                        selection_reason="no_results_in_filter",
                        selection_detail="sem resultados para o alias inicial",
                    ),
                ),
                (
                    [document_search.SearchHit(protocolo="MEMO-1", total_resultados=1)],
                    scraper._build_collection_context(
                        found=True,
                        found_in="filter",
                        search_term="Memorando de Entendimentos",
                        results_count=1,
                        chosen_documento="MEMO-1",
                        selection_reason="primeiro_resultado_mais_recente",
                        selection_detail="position=1 total=1",
                    ),
                ),
            ],
        ), patch.object(
            scraper,
            "_reset_search_context_with_fallback",
        ) as reset_mock:
            candidate_groups, collection_context = scraping.SEIScraper._search_document_candidates_in_filter(
                scraper,
                "60091.000060/2023-87",
                document_type,
            )

        self.assertEqual(len(candidate_groups), 1)
        self.assertEqual(candidate_groups[0][0], "Memorando de Entendimentos")
        self.assertEqual(candidate_groups[0][1][0].protocolo, "MEMO-1")
        self.assertTrue(collection_context["found"])
        self.assertEqual(collection_context["search_term"], "Memorando de Entendimentos")
        reset_mock.assert_called_once()

    def test_open_document_via_tree_tenta_proximo_candidato_apos_snapshot_invalido(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = Mock(
            current_url="https://sei.defesa.gov.br/processo",
            title="SEI - Processo",
            window_handles=["main"],
            current_window_handle="main",
        )
        scraper.driver.execute_script = Mock()
        scraper.driver.switch_to = SimpleNamespace(default_content=Mock())
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._find_document_candidates_in_tree = Mock(
            return_value=[
                {"text": "E-mail Confirmacao de recebimento-ACT", "score": 40, "matched_terms": ["act"]},
                {"text": "Memorando de Entendimentos 1", "score": 80, "matched_terms": ["memorando de entendimentos"]},
            ]
        )
        first_link = Mock()
        second_link = Mock()
        scraper._locate_tree_link_by_text = Mock(side_effect=[first_link, second_link])
        scraper._extract_and_process_document_snapshot = Mock(side_effect=[False, True])
        scraper._restore_process_base_context = Mock()
        scraper._should_skip_candidate_pre_open = lambda *args, **kwargs: False
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=("Memorando de Entendimentos",),
            tree_match_terms=("memorando de entendimentos", "act"),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )

        result = scraping.SEIScraper._open_document_via_tree(
            scraper,
            "60091.000060/2023-87",
            document_type,
            process_url="https://sei.defesa.gov.br/processo",
        )

        self.assertTrue(result)
        self.assertEqual(scraper._locate_tree_link_by_text.call_count, 2)
        self.assertEqual(scraper._extract_and_process_document_snapshot.call_count, 2)
        self.assertEqual(scraper._restore_process_base_context.call_count, 2)

    def test_busca_act_cai_para_arvore_quando_snapshot_do_filtro_e_invalido(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = DummyLogger()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_search_terms = scraping.SEIScraper._iter_unique_search_terms.__get__(scraper, scraping.SEIScraper)
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._search_document_in_filter = Mock(
            return_value=(
                [document_search.SearchHit(protocolo="MEMO-1", total_resultados=1)],
                scraper._build_collection_context(
                    found=True,
                    found_in="filter",
                    search_term="Memorando de Entendimentos",
                    results_count=1,
                    chosen_documento="MEMO-1",
                    selection_reason="primeiro_resultado_mais_recente",
                    selection_detail="position=1 total=1",
                ),
            )
        )
        scraper._record_document_search_outcome = Mock()
        scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
        scraper._open_document_via_tree = Mock(return_value=True)
        scraper._extract_and_process_document_snapshot = Mock(return_value=False)
        scraper._restore_process_base_context = Mock()
        scraper._ensure_document_search_open = Mock()
        scraper._should_skip_candidate_pre_open = lambda *args, **kwargs: False
        scraper.abrir_documento_no_filtro = lambda *args, **kwargs: None
        scraper._switch_to_newly_opened_window = lambda *args, **kwargs: None
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=("Memorando de Entendimentos",),
            tree_match_terms=("memorando de entendimentos", "act"),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )

        scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
            scraper,
            "60091.000060/2023-87",
            document_type,
        )

        scraper._open_document_via_tree.assert_called_once()
        scraper._record_document_search_outcome.assert_not_called()

    def test_busca_act_limita_abertura_do_filtro_ao_top_2(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_filter_terms = scraping.SEIScraper._iter_unique_filter_terms.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._get_ordered_filter_hits_for_opening = scraping.SEIScraper._get_ordered_filter_hits_for_opening.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_valid_candidate_early_stop = scraping.SEIScraper._log_valid_candidate_early_stop.__get__(
            scraper,
            scraping.SEIScraper,
        )
        collection_context = scraper._build_collection_context(
            found=True,
            found_in="filter",
            search_term="Acordo de Cooperação Técnica",
            results_count=3,
            chosen_documento="ACT-1",
            selection_reason="resultado_ranqueado_por_data",
            selection_detail="position=1 total=3",
        )
        scraper._search_document_in_filter = Mock(
            side_effect=[
                (
                    [
                        document_search.SearchHit(protocolo="ACT-3", total_resultados=3, selected_position=3),
                        document_search.SearchHit(protocolo="ACT-1", total_resultados=3, selected_position=1),
                        document_search.SearchHit(protocolo="ACT-2", total_resultados=3, selected_position=2),
                    ],
                    collection_context,
                ),
                (
                    [
                        document_search.SearchHit(protocolo="ACT-1", total_resultados=3, selected_position=1),
                        document_search.SearchHit(protocolo="ACT-2", total_resultados=3, selected_position=2),
                        document_search.SearchHit(protocolo="ACT-3", total_resultados=3, selected_position=3),
                    ],
                    collection_context,
                ),
            ]
        )
        scraper._record_document_search_outcome = Mock()
        scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
        scraper._open_document_via_tree = Mock(return_value=False)
        scraper._extract_and_process_document_snapshot = Mock(return_value=False)
        scraper._reset_search_context_with_fallback = Mock()
        scraper._restore_process_base_context = Mock()
        scraper._ensure_document_search_open = Mock()
        scraper._should_skip_candidate_pre_open = lambda *args, **kwargs: False
        scraper.abrir_documento_no_filtro = Mock()
        scraper._switch_to_newly_opened_window = lambda *args, **kwargs: None
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=("Acordo de Cooperação Técnica",),
            tree_match_terms=("act",),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
            max_filter_candidates=2,
        )

        scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
            scraper,
            "60091.000060/2023-87",
            document_type,
        )

        self.assertEqual(scraper.abrir_documento_no_filtro.call_count, 2)
        opened_positions = [call.kwargs["position"] for call in scraper.abrir_documento_no_filtro.call_args_list]
        self.assertEqual(opened_positions, [1, 2])
        scraper.logger.info.assert_any_call(
            "Processo %s: %s limitando abertura do filtro aos top %d candidatos (mais recente primeiro) para alias '%s'.",
            "60091.000060/2023-87",
            "ACT",
            2,
            "Acordo de Cooperação Técnica",
        )

    def test_busca_act_registra_early_stop_apos_primeiro_snapshot_valido(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper.timeout_seconds = 20
        scraper.driver = FakeScraperDriver()
        scraper._process_filter_degraded = {}
        scraper._process_filter_recovery_attempts = {}
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_filter_terms = scraping.SEIScraper._iter_unique_filter_terms.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._get_ordered_filter_hits_for_opening = scraping.SEIScraper._get_ordered_filter_hits_for_opening.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._log_valid_candidate_early_stop = scraping.SEIScraper._log_valid_candidate_early_stop.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._search_document_in_filter = Mock(
            return_value=(
                [
                    document_search.SearchHit(protocolo="ACT-1", total_resultados=3, selected_position=1),
                    document_search.SearchHit(protocolo="ACT-2", total_resultados=3, selected_position=2),
                    document_search.SearchHit(protocolo="ACT-3", total_resultados=3, selected_position=3),
                ],
                scraper._build_collection_context(
                    found=True,
                    found_in="filter",
                    search_term="Acordo de Cooperação Técnica",
                    results_count=3,
                    chosen_documento="ACT-1",
                    selection_reason="resultado_ranqueado_por_data",
                    selection_detail="position=1 total=3",
                ),
            )
        )
        scraper._record_document_search_outcome = Mock()
        scraper._close_opened_doc_tabs = lambda *args, **kwargs: None
        scraper._open_document_via_tree = Mock(return_value=False)
        scraper._extract_and_process_document_snapshot = Mock(side_effect=[False, True])
        scraper._reset_search_context_with_fallback = Mock()
        scraper._restore_process_base_context = Mock()
        scraper._ensure_document_search_open = Mock()
        scraper._should_skip_candidate_pre_open = lambda *args, **kwargs: False
        scraper.abrir_documento_no_filtro = Mock()
        scraper._switch_to_newly_opened_window = lambda *args, **kwargs: None
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=("Acordo de Cooperação Técnica",),
            tree_match_terms=("act",),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
            max_filter_candidates=2,
        )

        result = scraping.SEIScraper._buscar_e_abrir_documento_mais_recente(
            scraper,
            "60091.000060/2023-87",
            document_type,
        )

        self.assertTrue(result)
        self.assertEqual(scraper.abrir_documento_no_filtro.call_count, 2)
        scraper.logger.info.assert_any_call(
            "Processo %s: ACT early stop após candidato válido",
            "60091.000060/2023-87",
        )

    def test_get_document_types_for_process_reorders_ted_after_act(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.document_types = [
            make_document_type("ted", "TED"),
            make_document_type("act", "ACT"),
            make_document_type("pt", "PT"),
        ]

        ordered = scraping.SEIScraper._get_document_types_for_process(scraper)

        self.assertEqual([spec.key for spec in ordered], ["act", "ted", "pt"])

    def test_run_document_search_for_process_skips_ted_without_prior_act(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper._build_collection_context = scraping.SEIScraper._build_collection_context.__get__(scraper, scraping.SEIScraper)
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._dedupe_terms = scraping.SEIScraper._dedupe_terms.__get__(scraper, scraping.SEIScraper)
        scraper._iter_unique_filter_terms = scraping.SEIScraper._iter_unique_filter_terms.__get__(
            scraper,
            scraping.SEIScraper,
        )
        scraper._record_document_search_outcome = Mock()
        scraper._ensure_document_search_open = Mock()
        scraper._buscar_e_abrir_documento_mais_recente = Mock(return_value=False)
        document_type = make_document_type("ted", "TED - Termo de Execucao Descentralizada")

        result = scraping.SEIScraper._run_document_search_for_process(
            scraper,
            "60093.000015/2020-60",
            document_type,
        )

        self.assertFalse(result)
        scraper._ensure_document_search_open.assert_not_called()
        scraper._buscar_e_abrir_documento_mais_recente.assert_not_called()
        scraper._record_document_search_outcome.assert_called_once()
        outcome = scraper._record_document_search_outcome.call_args.args[2]
        self.assertEqual(outcome["selection_reason"], "skipped_without_prior_act")
        self.assertEqual(outcome["selection_detail"], "TED skip: sem ACT prévio")
        scraper.logger.info.assert_any_call("Processo %s: TED skip: sem ACT prévio", "60093.000015/2020-60")

    def test_run_document_search_for_process_executes_ted_after_act_found(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.logger = Mock()
        scraper._ensure_document_search_open = Mock()
        scraper._buscar_e_abrir_documento_mais_recente = Mock(side_effect=[True, True])
        act_document_type = make_document_type("act", "ACT")
        ted_document_type = make_document_type("ted", "TED - Termo de Execucao Descentralizada")

        act_result = scraping.SEIScraper._run_document_search_for_process(
            scraper,
            "60093.000015/2020-60",
            act_document_type,
        )
        ted_result = scraping.SEIScraper._run_document_search_for_process(
            scraper,
            "60093.000015/2020-60",
            ted_document_type,
        )

        self.assertTrue(act_result)
        self.assertTrue(ted_result)
        self.assertTrue(scraping.SEIScraper._has_prior_act_for_process(scraper, "60093.000015/2020-60"))
        self.assertEqual(scraper._ensure_document_search_open.call_count, 2)
        self.assertEqual(scraper._buscar_e_abrir_documento_mais_recente.call_count, 2)
        scraper._ensure_document_search_open.assert_any_call("60093.000015/2020-60", ted_document_type)
        scraper.logger.info.assert_any_call("Processo %s: TED executado: ACT presente", "60093.000015/2020-60")

    def test_validate_snapshot_rejeita_email_para_act(self) -> None:
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        document_type = DocumentTypeSpec(
            key="act",
            display_name="Acordo de Cooperacao Tecnica",
            search_terms=("Memorando de Entendimentos",),
            tree_match_terms=("memorando de entendimentos", "act"),
            snapshot_prefix="act",
            log_label="ACT",
            cleanup_patterns=(),
            handler=DummyHandler(),
        )
        snapshot = {
            "title": "E-mail Confirmacao",
            "url": "https://sei.defesa.gov.br/email",
            "text": "De: teste@defesa.gov.br Para: equipe@defesa.gov.br Assunto: Memorando de Entendimentos",
        }

        is_valid, reason, analysis = scraping.SEIScraper._validate_snapshot_for_document_type(
            scraper,
            "60093.000015/2020-60",
            document_type,
            snapshot,
            {"chosen_documento": "E-mail Confirmacao de recebimento-ACT"},
        )

        self.assertTrue(is_valid)
        self.assertEqual(reason, "email_outro")
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis["doc_class"], "email_outro")
        self.assertEqual(analysis["validation_status"], "rejected_snapshot")


if __name__ == "__main__":
    unittest.main()
