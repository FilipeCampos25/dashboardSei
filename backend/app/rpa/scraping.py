from __future__ import annotations

import os
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    UnexpectedAlertPresentException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from app.config import get_settings
from app.core.driver_factory import create_chrome_driver
from app.core.logging_config import setup_logger
from app.documents import resolve_document_types
from app.documents.common import sanitize_snapshot
from app.documents.document_utils import should_skip_candidate
from app.documents.types import DocumentTypeSpec
from app.output import csv_writer
from app.rpa.sei import process_navigation
from app.rpa.sei import toolbar_actions
from app.rpa.sei import document_search
from app.rpa.sei import document_text_extractor
from app.services.act_normalizer import (
    DOC_CLASS_ACT_FINAL,
    DOC_CLASS_EMAIL_OUTRO,
    DOC_CLASS_MEMORANDO,
    DOC_CLASS_STUB,
    DOC_CLASS_TED,
    TREE_PENALTY_MARKERS,
    VALIDATION_STATUS_VALID,
    classify_cooperation_snapshot,
)
from app.services.pt_normalizer import (
    CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
    REQUESTED_TYPE_PT,
    RESOLVED_TYPE_PT,
    VALIDATION_STATUS_NON_CANONICAL,
)
from app.rpa.selenium_utils import (
    get_iframes_info,
    wait_for_clickable as selenium_wait_for_clickable,
    wait_for_document_ready as selenium_wait_for_document_ready,
    wait_for_elements as selenium_wait_for_elements,
)
from app.rpa.selectors import load_xpath_selectors

POST_LOGIN_REFRESH_SLEEP_SECONDS = 1.0
UI_SETTLE_SLEEP_SECONDS = 0.35
PAGINATION_SETTLE_SLEEP_SECONDS = 0.25


@dataclass
class FoundItem:
    text: str


@dataclass
class InternoRow:
    numero_interno: str
    descricao: str
    descricao_normalizada: str
    link: Any
    page: int
    row_index: int


def _compact_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def _click_optional_popup(
    driver: Any,
    xpath: str,
    *,
    probe_timeout_seconds: float = 0.5,
    poll_interval_seconds: float = 0.1,
) -> bool:
    try:
        candidates = driver.find_elements(By.XPATH, xpath)
    except WebDriverException:
        return False

    if not candidates:
        return False

    deadline = time.time() + max(0.0, float(probe_timeout_seconds))
    while True:
        for candidate in candidates:
            try:
                if hasattr(candidate, "is_displayed") and not candidate.is_displayed():
                    continue
            except WebDriverException:
                continue

            try:
                if hasattr(candidate, "is_enabled") and not candidate.is_enabled():
                    continue
            except WebDriverException:
                continue

            try:
                candidate.click()
                return True
            except WebDriverException:
                continue

        remaining = deadline - time.time()
        if remaining <= 0:
            return False

        time.sleep(min(poll_interval_seconds, remaining))
        try:
            candidates = driver.find_elements(By.XPATH, xpath)
        except WebDriverException:
            return False

    return False


def _is_overlay_displayed(driver: Any, xpath: str) -> bool:
    try:
        overlays = driver.find_elements(By.XPATH, xpath)
    except WebDriverException:
        return False

    for overlay in overlays:
        try:
            if overlay.is_displayed():
                return True
        except WebDriverException:
            continue
    return False


def _build_rows_signature(rows: List[Any]) -> tuple[str, int] | None:
    if not rows:
        return None

    first_non_empty = ""
    for row in rows[:5]:
        try:
            text = _compact_text(getattr(row, "text", "") or "")
        except WebDriverException:
            continue
        if text:
            first_non_empty = text
            break

    return (first_non_empty, len(rows))


def _clicked_element_became_stale(element: Any) -> bool:
    try:
        if hasattr(element, "is_enabled"):
            element.is_enabled()
    except StaleElementReferenceException:
        return True
    except WebDriverException:
        return False
    return False


def _wait_for_page_signature_change(
    read_rows: Callable[[], tuple[str, int] | None],
    previous_signature: tuple[str, int] | None,
    clicked_element: Any,
    *,
    timeout_seconds: float = 1.0,
    poll_interval_seconds: float = 0.1,
) -> bool:
    deadline = time.time() + max(0.1, float(timeout_seconds))
    while time.time() < deadline:
        if _clicked_element_became_stale(clicked_element):
            return True

        current_signature = read_rows()
        if (
            previous_signature is not None
            and current_signature is not None
            and current_signature != previous_signature
        ):
            return True

        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(poll_interval_seconds, remaining))

    return False


class SEIScraper:

    # Setup / lifecycle
    def __init__(self) -> None:
        self.logger = setup_logger()

        cfg = get_settings()
        self.settings = cfg
        self.logger.info("DEBUG CFG document_types raw: %s", cfg.document_types)
        self.logger.info("DEBUG ENV DOCUMENT_TYPES: %s", os.getenv("DOCUMENT_TYPES"))
        self.base_url = (
            cfg.sei_url
            or os.getenv("URL")
            or os.getenv("url_sei")
            or os.getenv("SEI_URL")
            or os.getenv("URL_SEI")
        )
        self.username = (
            cfg.username
            or os.getenv("USERNAME")
            or os.getenv("username")
            or os.getenv("USER")
            or os.getenv("SEI_USERNAME")
        )
        self.password = (
            cfg.password
            or os.getenv("PASSWORD")
            or os.getenv("password")
            or os.getenv("PASS")
            or os.getenv("SEI_PASSWORD")
        )

        self.driver = create_chrome_driver(headless=cfg.headless)
        self.wait = WebDriverWait(self.driver, cfg.timeout_seconds)
        self.timeout_seconds = cfg.timeout_seconds
        self.selectors = load_xpath_selectors()
        self.main_window_handle: Optional[str] = None
        self.found: Set[str] = set()
        self.document_types = resolve_document_types(cfg.document_types, logger=self.logger)
        self.document_types_by_key = {spec.key: spec for spec in self.document_types}
        self._process_filter_degraded: Dict[str, bool] = {}
        self._process_filter_recovery_attempts: Dict[Tuple[str, str], int] = {}
        self._process_act_found: Dict[str, bool] = {}
        self.total_candidatos_avaliados = 0
        self.candidatos_descartados_pre_abertura = 0
        self.logger.info(
            "Tipos documentais ativos: %s",
            ", ".join(spec.key for spec in self.document_types),
        )
        self.descricoes_busca = self._parse_descricoes_busca(cfg.descricoes_busca)
        self.descricao_match_mode = (cfg.descricoes_match_mode or "contains").strip().lower()
        if self.descricao_match_mode == "exact":
            self.descricao_match_mode = "equals"
        if self.descricao_match_mode not in {"contains", "equals"}:
            self.logger.warning(
                "DESCRICOES_MATCH_MODE invalido (%s). Usando 'contains'.",
                self.descricao_match_mode,
            )
            self.descricao_match_mode = "contains"

    def _prepare_output_dir_for_run(self) -> None:
        output_dir = self._resolve_preview_output_dir()
        csv_writer.ensure_output_dir(output_dir)

        cleanup_patterns = {"parcerias_vigentes_latest.csv", "dashboard_ready_latest.csv"}
        for document_type in self.document_types:
            document_type.handler.reset_run()
            cleanup_patterns.update(document_type.cleanup_patterns)
        removed = 0
        for pattern in sorted(cleanup_patterns):
            for path in output_dir.glob(pattern):
                try:
                    path.unlink()
                    removed += 1
                except FileNotFoundError:
                    continue
                except Exception as exc:
                    self.logger.warning("Falha ao remover artefato antigo %s (%s).", path, exc)
        self.logger.info("Output preparado para nova rodada em %s; artefatos removidos=%d", output_dir, removed)

    # Fluxo principal
    def run_full_flow(
        self,
        manual_login: bool = True,
        max_internos: Optional[int] = None,
        max_processos_por_interno: Optional[int] = None,
        stop_at_filter: bool = True,
    ) -> List[str]:
        if not self.base_url:
            raise RuntimeError("Config ausente: sei_url / URL / SEI_URL")

        self._reset_candidate_screening_stats()
        self._prepare_output_dir_for_run()

        self.logger.info("Abrindo SEI em: %s", self.base_url)
        self.driver.get(self.base_url)

        if manual_login:
            self._wait_for_manual_login()
        else:
            self._login_if_possible()
        self._remember_main_window_handle(context="pos_login")
        self.logger.info("=== START SCRAPING === %s", datetime.now().isoformat())

        self._close_popup_if_exists()
        self._open_interno_menu()
        selecionados = self._select_guided_internos_by_descricao()
        if not selecionados:
            self.logger.warning(
                "Modo guiado: nenhum interno selecionado pelas descricoes configuradas."
            )
            self._finalize_document_runs()
            self._log_candidate_screening_summary()
            result = sorted(self.found)
            self.logger.info("Itens unicos encontrados: %d", len(result))
            print(result)
            return result

        if max_internos is not None:
            self.logger.info("Limitando internos para %d", max_internos)
            selecionados = selecionados[:max_internos]

        if max_processos_por_interno is not None:
            self.logger.info("Limitando processos por interno para %d", max_processos_por_interno)

        for selecionado, selected_target, list_url in selecionados:
            if not self._click_selected_interno(selecionado, selected_target, list_url):
                continue

            self._collect_preview_if_parcerias_vigencias()
            processos = self._list_processos()
            if max_processos_por_interno is not None:
                processos = processos[:max_processos_por_interno]

            for proc in processos:
                self._clear_process_filter_state(proc)
                self._switch_to_main_window_context()
                self.logger.info("Abrindo processo %s", proc)
                self._open_processo(proc)
                self.logger.info("Processo %s: aguardando pagina pronta", proc)
                self._wait_page_ready_in_processo()
                self.logger.info("Processo %s: clicando Abrir todas as Pastas", proc)
                self._click_abrir_todas_as_pastas()
                for document_type in self._get_document_types_for_process():
                    self._run_document_search_for_process(proc, document_type)

                if stop_at_filter:
                    self.logger.info("Processo %s: fechando aba e voltando", proc)
                    self._close_current_tab_and_back()
                else:
                    self.logger.info(
                        "Processo %s: mantendo aba aberta no filtro (--no-stop-at-filter); interrompendo loop.",
                        proc,
                    )
                    self._finalize_document_runs()
                    self._log_candidate_screening_summary()
                    result = sorted(self.found)
                    self.logger.info("Itens unicos encontrados: %d", len(result))
                    print(result)
                    return result

        self._finalize_document_runs()
        self._log_candidate_screening_summary()
        result = sorted(self.found)
        self.logger.info("Itens unicos encontrados: %d", len(result))
        self.logger.info("=== END SCRAPING === %s", datetime.now().isoformat())
        print(result)
        return result

    # Utils wrappers (selenium_utils)
    def _wait_for_document_ready(self, timeout: int, tag: str) -> None:
        selenium_wait_for_document_ready(self.driver, timeout, tag, self.logger)


    def wait_for_elements(
        self,
        xpath: str,
        tag: str,
        timeout: int | None = None,
        restore_context: bool = True,
    ) -> List[Any]:
        effective_timeout = timeout or self.timeout_seconds
        return selenium_wait_for_elements(
            self.driver,
            self.logger,
            xpath,
            tag,
            timeout_seconds=effective_timeout,
            restore_context=restore_context,
        )

    def wait_for_clickable(self, xpath: str, tag: str, timeout: int | None = None) -> Any:
        effective_timeout = timeout or self.timeout_seconds
        return selenium_wait_for_clickable(
            self.driver,
            self.logger,
            xpath,
            tag,
            timeout_seconds=effective_timeout,
        )

    # Login / tela inicial
    def _wait_for_manual_login(self) -> None:
        cfg = self.settings
        self.logger.info("Aguardando conclusao do login/autenticacao no SEI (modo manual).")

        wait_seconds = max(5, cfg.manual_login_wait_seconds)
        self._wait_for_post_login_ready(wait_seconds)

    def _consume_login_alert_if_present(self, exc: Exception | None = None) -> str:
        alert_text = ""
        if isinstance(exc, UnexpectedAlertPresentException):
            alert_text = (getattr(exc, "alert_text", "") or "").strip()

        try:
            alert = self.driver.switch_to.alert
            if not alert_text:
                alert_text = (alert.text or "").strip()
            try:
                alert.accept()
            except WebDriverException:
                try:
                    alert.dismiss()
                except WebDriverException:
                    pass
        except WebDriverException:
            pass

        return " ".join(alert_text.split()).strip()

    def _handle_login_alert(self, exc: Exception | None = None) -> bool:
        alert_text = self._consume_login_alert_if_present(exc)
        if not alert_text:
            return False

        alert_norm = self._normalize_text(alert_text)
        self.logger.warning("Alerta detectado durante login: %s", alert_text)

        if "USUARIO OU SENHA INVALIDA" in alert_norm:
            raise RuntimeError(
                "O SEI retornou 'Usuário ou Senha Inválida.' durante o login. "
                "Verifique as credenciais usadas antes de prosseguir para o validador."
            )

        return True

    def _wait_for_post_login_ready(self, wait_seconds: int) -> None:
        sel = self.selectors.get("tela_inicio", {})
        x_bloco = sel.get("bloco")

        self.logger.info("Validando se a tela principal do SEI ficou pronta.")
        deadline = time.time() + wait_seconds
        gateway_timeout_retry_limit = 2
        gateway_timeout_hits = 0
        last_gateway_timeout_state = ""
        login_url_markers = ("/sip/login.php", "sigla_sistema=sei")
        post_login_url_markers = (
            "/sei/controlador.php",
            "acao=procedimento_controlar",
            "acao_origem=principal",
        )

        while time.time() < deadline:
            if self._handle_login_alert():
                time.sleep(UI_SETTLE_SLEEP_SECONDS)
                continue

            try:
                current_url = (self.driver.current_url or "").lower()
            except UnexpectedAlertPresentException as exc:
                if self._handle_login_alert(exc):
                    time.sleep(UI_SETTLE_SLEEP_SECONDS)
                    continue
                raise

            # Sinal 1: URL de destino conhecida apos login.
            if all(marker in current_url for marker in post_login_url_markers):
                self.logger.info("Login confirmado por mudanca de URL: %s", self.driver.current_url)
                return

            # Sinal 2: URL nao parece mais ser a tela de login.
            if current_url and not all(marker in current_url for marker in login_url_markers):
                if x_bloco and self.driver.find_elements(By.XPATH, x_bloco):
                    self.logger.info("Login confirmado: menu principal encontrado apos mudanca de URL.")
                    return
                if "/sei/" in current_url:
                    self.logger.info("Login confirmado por URL no contexto /sei/: %s", self.driver.current_url)
                    return

            # Sinal 3: fallback por elemento do menu principal.
            if x_bloco and self.driver.find_elements(By.XPATH, x_bloco):
                self.logger.info("Login confirmado: menu principal encontrado.")
                return

            if self._is_gateway_timeout_page():
                gateway_timeout_hits += 1
                last_gateway_timeout_state = self._describe_current_page_state()
                if gateway_timeout_hits <= gateway_timeout_retry_limit:
                    self.logger.warning(
                        "Tela 504 detectada apos login (%d/%d). "
                        "Tentando recarregar a pagina. estado=%s",
                        gateway_timeout_hits,
                        gateway_timeout_retry_limit,
                        last_gateway_timeout_state,
                    )
                    try:
                        self.driver.refresh()
                    except WebDriverException as exc:
                        self.logger.warning("Falha ao recarregar pagina apos 504: %s", exc)
                    time.sleep(POST_LOGIN_REFRESH_SLEEP_SECONDS)
                    continue

                raise RuntimeError(
                    "Tela de erro 504 detectada apos login em tentativas consecutivas. "
                    "Interrompendo execucao para evitar loop."
                )

            time.sleep(UI_SETTLE_SLEEP_SECONDS)

        final_state = self._describe_current_page_state()
        if gateway_timeout_hits:
            raise RuntimeError(
                "Login/autenticacao nao confirmado porque o SEI retornou erro 504 apos o login. "
                f"tentativas_504={gateway_timeout_hits} ultimo_estado={last_gateway_timeout_state or final_state}"
            )

        raise RuntimeError(
            "Login/autenticacao nao confirmado dentro do tempo limite. "
            f"Confirme se o processo foi concluido no navegador. estado_final={final_state}"
        )

    def _is_gateway_timeout_page(self) -> bool:
        try:
            title = (self.driver.title or "").lower()
            url = (self.driver.current_url or "").lower()
            html = (self.driver.page_source or "")[:12000].lower()
        except WebDriverException:
            return False

        combined = f"{title} {url} {html}"
        normalized = re.sub(r"[\s\-_]+", " ", combined)
        markers = (
            "gateway timeout",
            "gateway time out",
            "504 gateway timeout",
            "504 gateway time out",
            "erro 504",
            "error 504",
            "http error 504",
            "http 504",
            "the server didn't respond in time",
            "server didn't respond in time",
        )
        if any(marker in normalized for marker in markers):
            return True

        if "504" in normalized and ("gateway" in normalized or "time out" in normalized or "timeout" in normalized):
            return True

        return False

    def _describe_current_page_state(self) -> str:
        try:
            title = _compact_text(self.driver.title or "")
        except WebDriverException:
            title = ""

        try:
            url = _compact_text(self.driver.current_url or "")
        except WebDriverException:
            url = ""

        try:
            html_preview = _compact_text((self.driver.page_source or "")[:400])
        except WebDriverException:
            html_preview = ""

        preview = html_preview[:220]
        return f"url={url or '<vazia>'} title={title or '<vazio>'} html_preview={preview or '<vazio>'}"


    def _login_if_possible(self) -> None:
        if not self.username or not self.password:
            self.logger.info("Sem credenciais no env; pulando login automatizado.")
            return

        login_sel = self.selectors.get("login", {})
        x_user = login_sel.get("username")
        x_pass = login_sel.get("password")
        x_btn = login_sel.get("acessar")
        if not (x_user and x_pass and x_btn):
            self.logger.info("Seletores de login nao encontrados; pulando login automatizado.")
            return

        try:
            user_elem = self.wait_for_elements(x_user, tag="login_username")[0]
            pass_elem = self.wait_for_elements(x_pass, tag="login_password")[0]
            user_elem.send_keys(self.username)
            pass_elem.send_keys(self.password)
            self.wait_for_clickable(x_btn, tag="login_submit").click()
            self.logger.info("Login automatizado enviado.")
        except Exception as exc:
            self.logger.exception("Falha no login automatizado: %s", exc)

    def _close_popup_if_exists(self) -> None:
        sel = self.selectors.get("tela_inicio", {})
        x = sel.get("remove_pup_pop")
        if not x:
            return
        try:
            if _click_optional_popup(self.driver, x):
                self.logger.info("Pop-up fechado.")
        except Exception:
            return

    def _wait_for_overlay_to_clear(
        self,
        *,
        timeout_seconds: float = 6.0,
        overlay_xpath: str = "//*[contains(@class,'sparkling-modal-overlay') or contains(@id,'InfraSparklingModalOverlay')]",
    ) -> bool:
        deadline = time.time() + max(0.5, float(timeout_seconds))
        while time.time() < deadline:
            if not _is_overlay_displayed(self.driver, overlay_xpath):
                return True
            time.sleep(0.2)
        return not _is_overlay_displayed(self.driver, overlay_xpath)

    # Helpers de seletores / clique
    def _click_first_clickable(self, xpaths: List[str], label: str) -> None:
        checked: List[str] = []
        candidate_timeout = max(3, min(8, self.timeout_seconds))
        for idx, xpath in enumerate(xpaths, start=1):
            if not xpath or xpath in checked:
                continue
            checked.append(xpath)
            try:
                elem = self.wait_for_clickable(
                    xpath,
                    tag=f"{label}_candidate_{idx}",
                    timeout=candidate_timeout,
                )
                try:
                    elem.click()
                except ElementClickInterceptedException as exc:
                    self.logger.info(
                        "Clique interceptado em '%s' (xpath #%d). Aguardando overlay/modal do SEI desaparecer.",
                        label,
                        idx,
                    )
                    if not self._wait_for_overlay_to_clear(timeout_seconds=min(6.0, float(self.timeout_seconds))):
                        self.logger.warning(
                            "Overlay/modal do SEI permaneceu visivel ao clicar em '%s' (%s). Tentando fallback JS.",
                            label,
                            exc,
                        )
                    try:
                        elem = self.wait_for_clickable(
                            xpath,
                            tag=f"{label}_candidate_{idx}_retry",
                            timeout=candidate_timeout,
                        )
                        elem.click()
                    except (ElementClickInterceptedException, WebDriverException):
                        self.driver.execute_script("arguments[0].click();", elem)
                return
            except TimeoutException:
                continue

        raise TimeoutException(
            f"Nao localizei elemento clicavel para '{label}'. XPaths tentados: {checked}"
        )

    # Toolbar do processo (ate abrir filtro)
    def _wait_page_ready_in_processo(self) -> None:
        toolbar_actions.wait_page_ready_in_processo(
            self.driver,
            self.logger,
            timeout=self.timeout_seconds,
        )

    def _click_abrir_todas_as_pastas(self) -> None:
        toolbar_actions.click_abrir_todas_as_pastas(
            self.driver,
            self.selectors,
            self.logger,
            raise_on_fail=False,
        )

    def _click_pesquisar_no_processo(self) -> None:
        toolbar_actions.click_pesquisar_no_processo(
            self.driver,
            self.selectors,
            self.logger,
        )
        toolbar_actions.wait_pesquisa_anchor(
            self.driver,
            self.selectors,
            self.logger,
            timeout=self.timeout_seconds,
        )

    def _restore_process_base_context(
        self,
        processo: str,
        *,
        process_url: Optional[str] = None,
        reason: str = "",
    ) -> None:
        target_url = (process_url or self.driver.current_url or "").strip()
        if not target_url:
            self.logger.warning(
                "Processo %s: nao foi possivel restaurar contexto base%s porque a URL do processo esta vazia.",
                processo,
                f" ({reason})" if reason else "",
            )
            return

        try:
            self.driver.switch_to.default_content()
        except WebDriverException:
            pass

        self.logger.info(
            "Processo %s: restaurando contexto base do processo%s via reload da URL.",
            processo,
            f" ({reason})" if reason else "",
        )
        self.driver.get(target_url)
        self._wait_page_ready_in_processo()
        self._click_abrir_todas_as_pastas()

    def _clear_search_input_if_present(self) -> None:
        search_input_xpath = (
            self.selectors.get("pesquisar_processos.caixa_de_texto")
            or "//input[@id='txtPesquisa']"
        )
        try:
            elems = self.driver.find_elements(By.XPATH, search_input_xpath)
        except WebDriverException:
            return
        if not elems:
            return

        search_input = elems[0]
        try:
            current_value = (
                search_input.get_attribute("value")
                or search_input.get_attribute("title")
                or search_input.text
                or ""
            ).strip()
        except WebDriverException:
            current_value = ""
        if not current_value:
            return

        try:
            search_input.clear()
        except WebDriverException:
            try:
                self.driver.execute_script("arguments[0].value = '';", search_input)
            except WebDriverException:
                return

        self.logger.info("Pesquisar no Processo: campo de busca limpo para reutilizar o filtro.")

    def reset_search_context_light(self, processo: str, *, reason: str = "") -> None:
        timeout = min(5, max(1, self.timeout_seconds))
        try:
            self.driver.switch_to.default_content()
        except WebDriverException:
            pass

        try:
            document_search._switch_to_pesquisa_context(
                self.driver,
                self.selectors,
                self.logger,
                timeout_seconds=timeout,
            )
        except (TimeoutException, RuntimeError, NoSuchElementException, StaleElementReferenceException, WebDriverException):
            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass
            self._click_pesquisar_no_processo()

        self._clear_search_input_if_present()
        self.logger.info(
            "Processo %s: reset_context_light usado%s.",
            processo,
            f" ({reason})" if reason else "",
        )

    def _should_fallback_to_full_reload(self, exc: Exception) -> bool:
        if self._is_search_context_stagnation_timeout(exc):
            return True
        if isinstance(exc, (NoSuchElementException, StaleElementReferenceException)):
            return True
        diagnostics = self._collect_pesquisa_diagnostics()
        return diagnostics.get("state") == document_search.PESQUISA_STATE_INACTIVE

    def _reset_search_context_with_fallback(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        *,
        process_url: Optional[str] = None,
        reason: str = "",
    ) -> None:
        if self._process_filter_degraded.get(processo):
            self.logger.info(
                "Processo %s: reload completo usado (fallback)%s.",
                processo,
                f" ({reason}; filtro degradado)" if reason else " (filtro degradado)",
            )
            self._restore_process_base_context(
                processo,
                process_url=process_url,
                reason=reason,
            )
            self._ensure_document_search_open(processo, document_type)
            return

        try:
            self.reset_search_context_light(processo, reason=reason)
            return
        except (TimeoutException, RuntimeError, NoSuchElementException, StaleElementReferenceException, WebDriverException) as exc:
            if self._is_search_context_stagnation_timeout(exc):
                self._process_filter_degraded[processo] = True
            if not self._should_fallback_to_full_reload(exc):
                raise
            self.logger.warning(
                "Processo %s: reload completo usado (fallback)%s. erro=%s",
                processo,
                f" ({reason})" if reason else "",
                exc,
            )
            self._restore_process_base_context(
                processo,
                process_url=process_url,
                reason=reason,
            )
            self._ensure_document_search_open(processo, document_type)

    def _clear_process_filter_state(self, processo: str) -> None:
        self._process_filter_degraded.pop(processo, None)
        stale_keys = [
            key for key in self._process_filter_recovery_attempts if key[0] == processo
        ]
        for key in stale_keys:
            self._process_filter_recovery_attempts.pop(key, None)
        act_state = getattr(self, "_process_act_found", None)
        if isinstance(act_state, dict):
            act_state.pop(processo, None)

    def _get_document_types_for_process(self) -> List[DocumentTypeSpec]:
        ordered = list(self.document_types)
        ted_index = next((idx for idx, spec in enumerate(ordered) if spec.key == "ted"), None)
        act_index = next((idx for idx, spec in enumerate(ordered) if spec.key == "act"), None)
        if ted_index is None or act_index is None or act_index < ted_index:
            return ordered

        ted_spec = ordered.pop(ted_index)
        act_index = next(idx for idx, spec in enumerate(ordered) if spec.key == "act")
        ordered.insert(act_index + 1, ted_spec)
        return ordered

    def _has_prior_act_for_process(self, processo: str) -> bool:
        return bool(getattr(self, "_process_act_found", {}).get(processo, False))

    def _should_use_tree_fallback(self, document_type: DocumentTypeSpec) -> bool:
        return document_type.key in {"pt", "act"}

    def _set_process_act_found(self, processo: str, found: bool) -> None:
        act_state = getattr(self, "_process_act_found", None)
        if not isinstance(act_state, dict):
            act_state = {}
            self._process_act_found = act_state
        act_state[processo] = bool(found)

    def _should_run_document_search(self, processo: str, document_type: DocumentTypeSpec) -> bool:
        if document_type.key != "ted":
            return True
        if not self._has_prior_act_for_process(processo):
            self.logger.info("Processo %s: TED skip: sem ACT prévio", processo)
            collection_context = self._build_collection_context(
                found=False,
                found_in="skipped",
                search_term="|".join(self._iter_unique_filter_terms(document_type)),
                selection_reason="skipped_without_prior_act",
                selection_detail="TED skip: sem ACT prévio",
            )
            self._record_document_search_outcome(processo, document_type, collection_context)
            return False
        self.logger.info("Processo %s: TED executado: ACT presente", processo)
        return True

    def _run_document_search_for_process(self, processo: str, document_type: DocumentTypeSpec) -> bool:
        if not self._should_run_document_search(processo, document_type):
            return False
        self._ensure_document_search_open(processo, document_type)
        found = self._buscar_e_abrir_documento_mais_recente(processo, document_type)
        if document_type.key == "act":
            self._set_process_act_found(processo, found)
        return found

    def _is_search_context_stagnation_timeout(self, exc: Exception) -> bool:
        return isinstance(exc, TimeoutException) and "motivo=estagnacao_do_contexto" in str(exc)

    def _can_retry_filter_recovery(self, processo: str, document_key: str) -> bool:
        return self._process_filter_recovery_attempts.get((processo, document_key), 0) < 1

    def _consume_filter_recovery_attempt(self, processo: str, document_key: str) -> bool:
        if not self._can_retry_filter_recovery(processo, document_key):
            return False
        key = (processo, document_key)
        self._process_filter_recovery_attempts[key] = self._process_filter_recovery_attempts.get(key, 0) + 1
        return True

    def _ensure_document_search_open(self, processo: str, document_type: DocumentTypeSpec) -> None:
        anchor_timeout = min(3, max(1, self.timeout_seconds))
        force_restore = bool(self._process_filter_degraded.get(processo))
        if force_restore:
            self.logger.info(
                "Processo %s: filtro marcado como degradado; restaurando contexto antes de %s.",
                processo,
                document_type.display_name,
            )
        else:
            try:
                toolbar_actions.wait_pesquisa_anchor(
                    self.driver,
                    self.selectors,
                    self.logger,
                    timeout=anchor_timeout,
                )
                self.logger.info(
                    "Processo %s: filtro ja estava aberto para %s.",
                    processo,
                    document_type.display_name,
                )
                return
            except (TimeoutException, RuntimeError):
                pass

        last_exc: Optional[Exception] = None
        while True:
            if force_restore:
                if not self._consume_filter_recovery_attempt(processo, document_type.key):
                    if last_exc is not None:
                        raise last_exc
                    break
                try:
                    self._restore_process_base_context(
                        processo,
                        reason=f"recovery abrir filtro para {document_type.log_label}",
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Processo %s: falha ao restaurar contexto antes da reabertura do filtro para %s (%s).",
                        processo,
                        document_type.display_name,
                        exc,
                    )
                    last_exc = exc
                force_restore = False

            self.logger.info(
                "Processo %s: clicando Pesquisar no Processo para %s.",
                processo,
                document_type.display_name,
            )
            try:
                self._click_pesquisar_no_processo()
                self._process_filter_degraded.pop(processo, None)
                self.logger.info(
                    "Processo %s: filtro aberto (anchor ok) para %s.",
                    processo,
                    document_type.display_name,
                )
                return
            except (TimeoutException, RuntimeError) as exc:
                last_exc = exc
                if self._is_search_context_stagnation_timeout(exc):
                    self._process_filter_degraded[processo] = True
                    self.logger.warning(
                        "Processo %s: filtro entrou em estagnacao para %s; contexto sera restaurado antes da proxima tentativa.",
                        processo,
                        document_type.display_name,
                    )
                if self._can_retry_filter_recovery(processo, document_type.key):
                    self.logger.warning(
                        "Processo %s: falha ao abrir filtro para %s; tentando uma restauracao controlada da tela base.",
                        processo,
                        document_type.display_name,
                    )
                    force_restore = True
                    continue
                raise

        if last_exc is not None:
            raise last_exc

    def _get_document_type(self, key: str) -> Optional[DocumentTypeSpec]:
        return self.document_types_by_key.get(key)

    def _iter_unique_search_terms(self, document_type: DocumentTypeSpec) -> List[str]:
        return self._dedupe_terms(document_type.search_terms)

    def _iter_unique_filter_terms(self, document_type: DocumentTypeSpec) -> List[str]:
        terms = document_type.filter_type_aliases or document_type.search_terms
        return self._dedupe_terms(terms)

    def _get_ordered_filter_hits_for_opening(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        termo: str,
        hits: List[document_search.SearchHit],
    ) -> List[document_search.SearchHit]:
        ordered_hits = sorted(
            hits,
            key=lambda hit: max(1, int(getattr(hit, "selected_position", 1) or 1)),
        )
        max_candidates = max(0, int(document_type.max_filter_candidates or 0))
        if max_candidates <= 0:
            return ordered_hits

        limited_hits = ordered_hits[:max_candidates]
        if len(ordered_hits) > max_candidates:
            self.logger.info(
                "Processo %s: %s limitando abertura do filtro aos top %d candidatos (mais recente primeiro) para alias '%s'.",
                processo,
                document_type.log_label,
                max_candidates,
                termo,
            )
        return limited_hits

    def _log_valid_candidate_early_stop(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
    ) -> None:
        if document_type.key == "act":
            self.logger.info("Processo %s: ACT early stop após candidato válido", processo)

    def _dedupe_terms(self, terms: Tuple[str, ...] | List[str]) -> List[str]:
        unique_terms: List[str] = []
        seen: Set[str] = set()
        for term in terms:
            normalized = self._normalize_text(term)
            collapsed = re.sub(r"\s+", " ", normalized).strip()
            if not collapsed or collapsed in seen:
                continue
            seen.add(collapsed)
            unique_terms.append(term)
        return unique_terms

    def _build_collection_context(
        self,
        *,
        found: bool,
        found_in: str,
        search_term: str = "",
        results_count: int = 0,
        chosen_documento: str = "",
        selection_reason: str = "",
        selection_detail: str = "",
        extraction_error: str = "",
    ) -> Dict[str, Any]:
        return {
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "found": found,
            "found_in": found_in,
            "search_term": search_term,
            "results_count": int(results_count or 0),
            "chosen_documento": chosen_documento,
            "selection_reason": selection_reason,
            "selection_detail": selection_detail,
            "extraction_error": extraction_error,
        }

    def _collect_pesquisa_diagnostics(self) -> Dict[str, Any]:
        try:
            return document_search.describe_pesquisa_context(self.driver, self.selectors)
        except Exception as exc:
            self.logger.warning("Falha ao coletar diagnostico do filtro de pesquisa (%s).", exc)
            return {
                "state": "inactive",
                "current_url": "",
                "current_title": "",
                "ifrConteudoVisualizacao_src": "",
                "ifrVisualizacao_src": "",
                "primary_result_count": 0,
                "fallback_result_count": 0,
            }

    def _format_pesquisa_diagnostics(self, diagnostics: Dict[str, Any]) -> str:
        return (
            "state={state} url={url} title={title} "
            "ifrConteudoVisualizacao_src={conteudo_src} "
            "ifrVisualizacao_src={visualizacao_src} "
            "primary_result_count={primary_count} "
            "fallback_result_count={fallback_count}"
        ).format(
            state=diagnostics.get("state", "inactive") or "inactive",
            url=diagnostics.get("current_url", "") or "-",
            title=diagnostics.get("current_title", "") or "-",
            conteudo_src=diagnostics.get("ifrConteudoVisualizacao_src", "") or "-",
            visualizacao_src=diagnostics.get("ifrVisualizacao_src", "") or "-",
            primary_count=int(diagnostics.get("primary_result_count", 0) or 0),
            fallback_count=int(diagnostics.get("fallback_result_count", 0) or 0),
        )

    def _log_filter_diagnostics(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        termo: str,
        *,
        outcome: str,
        exc: Exception | None = None,
    ) -> str:
        diagnostics = self._collect_pesquisa_diagnostics()
        detail = self._format_pesquisa_diagnostics(diagnostics)
        log_label = document_type.log_label
        if outcome == "zero_results":
            self.logger.info(
                "Processo %s: %s termo '%s' sem resultado no filtro. %s",
                processo,
                log_label,
                termo,
                detail,
            )
        elif outcome == "stagnation":
            self.logger.warning(
                "Processo %s: %s termo '%s' entrou em estagnacao no filtro. %s erro=%s",
                processo,
                log_label,
                termo,
                detail,
                exc,
            )
        else:
            self.logger.warning(
                "Processo %s: %s termo '%s' falhou no filtro (%s). %s erro=%s",
                processo,
                log_label,
                termo,
                outcome,
                detail,
                exc,
            )
        return f"outcome={outcome}; {detail}"

    def _log_pt_filter_diagnostics(
        self,
        processo: str,
        termo: str,
        *,
        outcome: str,
        exc: Exception | None = None,
    ) -> str:
        document_type = self._get_document_type("pt")
        if document_type is None:
            diagnostics = self._collect_pesquisa_diagnostics()
            return f"outcome={outcome}; {self._format_pesquisa_diagnostics(diagnostics)}"
        return self._log_filter_diagnostics(
            processo,
            document_type,
            termo,
            outcome=outcome,
            exc=exc,
        )

    def _record_document_search_outcome(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        collection_context: Dict[str, Any],
    ) -> None:
        recorder = getattr(document_type.handler, "record_search_outcome", None)
        if callable(recorder):
            recorder(
                spec=document_type,
                processo=processo,
                collection_context=collection_context,
            )

    def _record_document_extraction_failure(
        self,
        processo: str,
        protocolo_documento: str,
        document_type: DocumentTypeSpec,
        collection_context: Dict[str, Any],
    ) -> None:
        recorder = getattr(document_type.handler, "record_extraction_failure", None)
        if callable(recorder):
            recorder(
                spec=document_type,
                processo=processo,
                protocolo_documento=protocolo_documento,
                collection_context=collection_context,
            )

    def _search_document_in_filter(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        termo: str,
    ) -> Tuple[List[document_search.SearchHit], Dict[str, Any]]:
        try:
            hits = self.buscar_documentos_no_filtro(
                termo=termo,
                timeout_seconds=self.timeout_seconds,
            )
        except (TimeoutException, NoSuchElementException) as exc:
            stagnated = self._is_search_context_stagnation_timeout(exc)
            if stagnated:
                self._process_filter_degraded[processo] = True
            detail = self._log_filter_diagnostics(
                processo,
                document_type,
                termo,
                outcome="stagnation" if stagnated else "filter_error",
                exc=exc,
            )
            return (
                [],
                self._build_collection_context(
                    found=False,
                    found_in="filter",
                    search_term=termo,
                    results_count=0,
                    selection_reason="search_context_stagnation" if stagnated else "search_open_error",
                    selection_detail=detail,
                    extraction_error=str(exc),
                ),
            )

        if not hits:
            detail = self._log_filter_diagnostics(
                processo,
                document_type,
                termo,
                outcome="zero_results",
            )
            return (
                [],
                self._build_collection_context(
                    found=False,
                    found_in="filter",
                    search_term=termo,
                    results_count=0,
                    selection_reason="no_results_in_filter",
                    selection_detail=detail,
                ),
            )

        first_hit = hits[0]
        return (
            hits,
            self._build_collection_context(
                found=True,
                found_in="filter",
                search_term=termo,
                results_count=first_hit.total_resultados,
                chosen_documento=first_hit.protocolo,
                selection_reason=first_hit.selection_reason,
                selection_detail=f"position={first_hit.selected_position} total={first_hit.total_resultados}",
            ),
        )

    def _search_document_candidates_in_filter(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
    ) -> Tuple[List[Tuple[str, List[document_search.SearchHit]]], Dict[str, Any]]:
        process_url = (self.driver.current_url or "").strip()
        search_terms = self._iter_unique_filter_terms(document_type)
        candidate_groups: List[Tuple[str, List[document_search.SearchHit]]] = []
        collection_context = self._build_collection_context(
            found=False,
            found_in="filter",
            search_term="|".join(search_terms),
            results_count=0,
            selection_reason="no_results_in_filter",
            selection_detail=f"nenhum termo de {document_type.log_label} retornou resultado no filtro",
        )

        for index, termo in enumerate(search_terms):
            if index > 0:
                self.logger.info(
                    "Processo %s: resetando contexto leve do filtro para %s com termo '%s'.",
                    processo,
                    document_type.display_name,
                    termo,
                )
                try:
                    self._reset_search_context_with_fallback(
                        processo,
                        document_type,
                        process_url=process_url,
                        reason=f"nova busca no filtro {document_type.log_label} ({termo})",
                    )
                except (TimeoutException, RuntimeError, NoSuchElementException, WebDriverException) as exc:
                    stagnated = self._is_search_context_stagnation_timeout(exc)
                    if stagnated:
                        self._process_filter_degraded[processo] = True
                    detail = self._log_filter_diagnostics(
                        processo,
                        document_type,
                        termo,
                        outcome="stagnation" if stagnated else "filter_reopen_error",
                        exc=exc,
                    )
                    collection_context = self._build_collection_context(
                        found=False,
                        found_in="filter",
                        search_term=termo,
                        results_count=0,
                        selection_reason=(
                            "search_context_stagnation" if stagnated else "filter_reopen_error"
                        ),
                        selection_detail=detail,
                        extraction_error=str(exc),
                    )
                    if stagnated:
                        return (candidate_groups, collection_context)
                    continue

            hits, term_context = self._search_document_in_filter(
                processo=processo,
                document_type=document_type,
                termo=termo,
            )
            collection_context = term_context
            if not hits:
                if term_context.get("selection_reason") == "search_context_stagnation":
                    return (candidate_groups, collection_context)
                continue

            candidate_groups.append((termo, hits))
            return (candidate_groups, collection_context)

        return (candidate_groups, collection_context)

    def _search_pt_document_in_filter(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
    ) -> Tuple[List[Tuple[str, List[document_search.SearchHit]]], Dict[str, Any]]:
        return self._search_document_candidates_in_filter(processo, document_type)

    def _score_tree_candidate(
        self,
        document_type: DocumentTypeSpec,
        raw_text: str,
    ) -> Tuple[int, List[str]]:
        normalized = self._normalize_text(raw_text)
        if not normalized:
            return (0, [])

        matched_terms: List[str] = []
        score = 0
        for idx, term in enumerate(document_type.tree_match_terms):
            normalized_term = self._normalize_text(term)
            if not normalized_term or normalized_term not in normalized:
                continue
            matched_terms.append(term)
            score += max(5, len(normalized_term)) + max(0, 40 - idx)

        if not matched_terms:
            return (0, [])

        rejection_penalties = {
            "e-mail": 220,
            "email": 220,
            "correio eletronico": 220,
            "planilha": 220,
            "xls": 220,
            "xlsx": 220,
            "csv": 220,
        }
        if any(marker in normalized for marker in rejection_penalties):
            score -= 200

        if document_type.key == "act":
            for marker in TREE_PENALTY_MARKERS:
                if marker not in normalized:
                    continue
                score -= 40 if marker == "anexo" else 120
            for marker in (
                "portaria",
                "publicacao",
                "reuniao",
                "termo aditivo",
                "termo de adesao",
                "plano de trabalho",
                "memorando",
                "termo de execucao descentralizada",
            ):
                if marker in normalized:
                    score -= 180
        elif document_type.key == "pt":
            if "DOCUMENTACAO" in normalized:
                score -= 20
            if "MINUTA" in normalized or "MINUTAS" in normalized:
                score -= 25

        return (score, matched_terms)

    def _find_document_candidates_in_tree(
        self,
        document_type: DocumentTypeSpec,
    ) -> List[Dict[str, Any]]:
        xpaths = self.selectors.get_many("processo.documentos_do_processo_links")

        try:
            self.driver.switch_to.default_content()
            iframe = self.driver.find_element(By.XPATH, "//iframe[@id='ifrArvore' or @name='ifrArvore']")
            self.driver.switch_to.frame(iframe)
        except WebDriverException as exc:
            self.logger.info(
                "Fallback arvore %s: nao foi possivel entrar no ifrArvore (%s).",
                document_type.log_label,
                exc,
            )
            return []

        candidates_by_text: Dict[str, Dict[str, Any]] = {}
        for xpath in xpaths:
            try:
                elems = self.driver.find_elements(By.XPATH, xpath)
            except WebDriverException:
                continue
            for elem in elems:
                try:
                    raw_text = (elem.text or "").strip()
                except WebDriverException:
                    continue
                normalized = self._normalize_text(raw_text)
                score, matched_terms = self._score_tree_candidate(document_type, raw_text)
                if score <= 0:
                    continue
                detail = {
                    "text": raw_text,
                    "normalized_text": normalized,
                    "score": score,
                    "matched_terms": matched_terms,
                }
                current = candidates_by_text.get(normalized)
                if current is None or int(current.get("score", 0)) < score:
                    candidates_by_text[normalized] = detail
            if candidates_by_text:
                break

        try:
            self.driver.switch_to.default_content()
        except WebDriverException:
            pass

        candidates = sorted(
            candidates_by_text.values(),
            key=lambda item: (-int(item.get("score", 0)), str(item.get("text", ""))),
        )
        if not candidates:
            self.logger.info(
                "Fallback arvore %s: nenhum documento correspondente encontrado na arvore.",
                document_type.log_label,
            )
            return []

        self.logger.info(
            "Fallback arvore %s: %d candidato(s) ranqueado(s); melhor candidato texto='%s' score=%d termos=%s.",
            document_type.log_label,
            len(candidates),
            candidates[0]["text"],
            candidates[0]["score"],
            "|".join(candidates[0]["matched_terms"]),
        )
        return candidates

    def _find_document_link_in_tree(self, document_type: DocumentTypeSpec) -> Optional[Tuple[Any, Dict[str, Any]]]:
        candidates = self._find_document_candidates_in_tree(document_type)
        if not candidates:
            return None
        link = self._locate_tree_link_by_text(candidates[0]["text"])
        if link is None:
            return None
        context = self._build_collection_context(
            found=True,
            found_in="tree",
            search_term="|".join(document_type.tree_match_terms),
            results_count=len(candidates),
            chosen_documento=candidates[0]["text"],
            selection_reason="highest_tree_match_score",
            selection_detail=(
                f"score={candidates[0]['score']} termos={'|'.join(candidates[0]['matched_terms'])}"
            ),
        )
        return (link, context)

    def _locate_tree_link_by_text(self, target_text: str) -> Optional[Any]:
        target_normalized = self._normalize_text(target_text)
        if not target_normalized:
            return None

        xpaths = self.selectors.get_many("processo.documentos_do_processo_links")
        try:
            self.driver.switch_to.default_content()
            iframe = self.driver.find_element(By.XPATH, "//iframe[@id='ifrArvore' or @name='ifrArvore']")
            self.driver.switch_to.frame(iframe)
        except WebDriverException:
            return None

        for xpath in xpaths:
            try:
                elems = self.driver.find_elements(By.XPATH, xpath)
            except WebDriverException:
                continue
            for elem in elems:
                try:
                    raw_text = (elem.text or "").strip()
                except WebDriverException:
                    continue
                if self._normalize_text(raw_text) == target_normalized:
                    return elem

        try:
            self.driver.switch_to.default_content()
        except WebDriverException:
            pass
        return None

    def _open_document_via_tree(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        *,
        process_url: Optional[str] = None,
    ) -> bool:
        candidates = self._find_document_candidates_in_tree(document_type)
        if not candidates:
            return False

        base_process_url = (process_url or self.driver.current_url or "").strip()
        for attempt_index, candidate in enumerate(candidates, start=1):
            candidate_text = str(candidate.get("text", "") or "")
            if self._should_skip_candidate_pre_open(candidate_text):
                continue

            if attempt_index > 1:
                try:
                    self._restore_process_base_context(
                        processo,
                        process_url=base_process_url,
                        reason=f"novo candidato da arvore para {document_type.log_label}",
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Processo %s: falha ao restaurar contexto antes do candidato #%d da arvore para %s (%s).",
                        processo,
                        attempt_index,
                        document_type.log_label,
                        exc,
                    )
                    return False

            link = self._locate_tree_link_by_text(str(candidate.get("text", "")))
            if link is None:
                self.logger.warning(
                    "Processo %s: candidato #%d da arvore para %s nao foi reencontrado (texto='%s').",
                    processo,
                    attempt_index,
                    document_type.log_label,
                    candidate.get("text", ""),
                )
                continue

            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            except WebDriverException:
                pass

            try:
                link.click()
            except WebDriverException:
                try:
                    self.driver.execute_script("arguments[0].click();", link)
                except WebDriverException as exc:
                    self.logger.warning(
                        "Processo %s: falha ao clicar no candidato #%d da arvore para %s (%s).",
                        processo,
                        attempt_index,
                        document_type.log_label,
                        exc,
                    )
                    continue

            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass

            self.logger.info(
                "Processo %s: documento aberto via arvore do processo. candidato=%d/%d texto='%s' score=%s",
                processo,
                attempt_index,
                len(candidates),
                candidate.get("text", ""),
                candidate.get("score", 0),
            )
            collection_context = self._build_collection_context(
                found=True,
                found_in="tree",
                search_term="|".join(document_type.tree_match_terms),
                results_count=len(candidates),
                chosen_documento=str(candidate.get("text", "") or ""),
                selection_reason="highest_tree_match_score",
                selection_detail=(
                    f"rank={attempt_index}/{len(candidates)} "
                    f"score={candidate.get('score', 0)} "
                    f"termos={'|'.join(candidate.get('matched_terms', []))}"
                ),
            )
            snapshot_saved = self._extract_and_process_document_snapshot(
                processo=processo,
                protocolo_documento=processo,
                document_type=document_type,
                collection_context=collection_context,
            )
            if snapshot_saved:
                self._log_valid_candidate_early_stop(processo, document_type)
                try:
                    self._restore_process_base_context(
                        processo,
                        process_url=base_process_url,
                        reason=f"apos fallback pela arvore ({document_type.log_label})",
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Processo %s: falha ao restaurar contexto apos fallback pela arvore de %s (%s).",
                        processo,
                        document_type.display_name,
                        exc,
                    )
                return True

            self.logger.warning(
                "Processo %s: candidato da arvore rejeitado apos validar snapshot para %s. texto='%s'",
                processo,
                document_type.log_label,
                candidate.get("text", ""),
            )

        try:
            self._restore_process_base_context(
                processo,
                process_url=base_process_url,
                reason=f"apos esgotar candidatos da arvore ({document_type.log_label})",
            )
        except Exception as exc:
            self.logger.warning(
                "Processo %s: falha ao restaurar contexto apos esgotar candidatos da arvore de %s (%s).",
                processo,
                document_type.display_name,
                exc,
            )
        return False

    def _buscar_e_abrir_documento_mais_recente(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
    ) -> bool:
        collection_context = self._build_collection_context(found=False, found_in="filter")
        processo_handle = ""
        process_url = (self.driver.current_url or "").strip()
        try:
            self.logger.info(
                "Processo %s: iniciando busca do documento '%s'. contexto_pre_busca url=%s title=%s handles=%d",
                processo,
                document_type.display_name,
                self.driver.current_url,
                self.driver.title,
                len(self.driver.window_handles),
            )
            processo_handle = self.driver.current_window_handle
            filter_terms = self._iter_unique_filter_terms(document_type)
            if len(filter_terms) != len(document_type.filter_type_aliases or document_type.search_terms):
                self.logger.info(
                    "Processo %s: aliases de filtro %s deduplicados de %d para %d.",
                    processo,
                    document_type.log_label,
                    len(document_type.filter_type_aliases or document_type.search_terms),
                    len(filter_terms),
                )
            found_any_filter_candidate = False
            saw_invalid_filter_candidate = False

            for term_index, termo in enumerate(filter_terms):
                if term_index > 0:
                    self.logger.info(
                        "Processo %s: resetando contexto leve do filtro para %s com alias '%s'.",
                        processo,
                        document_type.display_name,
                        termo,
                    )
                    try:
                        self._reset_search_context_with_fallback(
                            processo,
                            document_type,
                            process_url=process_url,
                            reason=f"nova busca no filtro {document_type.log_label} ({termo})",
                        )
                    except (TimeoutException, RuntimeError, NoSuchElementException, WebDriverException) as exc:
                        stagnated = self._is_search_context_stagnation_timeout(exc)
                        if stagnated:
                            self._process_filter_degraded[processo] = True
                        detail = self._log_filter_diagnostics(
                            processo,
                            document_type,
                            termo,
                            outcome="stagnation" if stagnated else "filter_reopen_error",
                            exc=exc,
                        )
                        collection_context = self._build_collection_context(
                            found=False,
                            found_in="filter",
                            search_term=termo,
                            results_count=0,
                            selection_reason="search_context_stagnation" if stagnated else "filter_reopen_error",
                            selection_detail=detail,
                            extraction_error=str(exc),
                        )
                        if stagnated:
                            break
                        continue

                hits, term_context = self._search_document_in_filter(
                    processo=processo,
                    document_type=document_type,
                    termo=termo,
                )
                collection_context = term_context
                if not hits:
                    if term_context.get("selection_reason") == "search_context_stagnation":
                        break
                    continue

                found_any_filter_candidate = True
                self.logger.info(
                    "Processo %s: %s retornou %d candidato(s) no filtro para alias '%s'.",
                    processo,
                    document_type.log_label,
                    len(hits),
                    termo,
                )

                hits_to_open = self._get_ordered_filter_hits_for_opening(
                    processo,
                    document_type,
                    termo,
                    hits,
                )
                for hit in hits_to_open:
                    candidate_text = str(hit.protocolo or "")
                    if self._should_skip_candidate_pre_open(candidate_text):
                        continue

                    attempt_opened_doc_handles: Set[str] = set()
                    attempt_context = self._build_collection_context(
                        found=True,
                        found_in="filter",
                        search_term=termo,
                        results_count=hit.total_resultados,
                        chosen_documento=hit.protocolo,
                        selection_reason=hit.selection_reason,
                        selection_detail=f"position={hit.selected_position} total={hit.total_resultados}",
                    )
                    try:
                        if term_index > 0 or hit.selected_position > 1:
                            self._reset_search_context_with_fallback(
                                processo,
                                document_type,
                                process_url=process_url,
                                reason=(
                                    f"novo candidato do filtro {document_type.log_label} "
                                    f"rank={hit.selected_position}"
                                ),
                            )
                            refreshed_hits, refreshed_context = self._search_document_in_filter(
                                processo=processo,
                                document_type=document_type,
                                termo=termo,
                            )
                            if len(refreshed_hits) < hit.selected_position:
                                self.logger.warning(
                                    "Processo %s: candidato #%d do filtro para %s nao reapareceu apos reabrir os resultados.",
                                    processo,
                                    hit.selected_position,
                                    document_type.log_label,
                                )
                                collection_context = refreshed_context
                                continue
                            hit = refreshed_hits[hit.selected_position - 1]
                            candidate_text = str(hit.protocolo or "")
                            if self._should_skip_candidate_pre_open(candidate_text):
                                continue
                            attempt_context = self._build_collection_context(
                                found=True,
                                found_in="filter",
                                search_term=termo,
                                results_count=hit.total_resultados,
                                chosen_documento=hit.protocolo,
                                selection_reason=hit.selection_reason,
                                selection_detail=f"position={hit.selected_position} total={hit.total_resultados}",
                            )

                        self.logger.info(
                            "Processo %s: abrindo candidato do filtro %d/%d para %s (%s).",
                            processo,
                            hit.selected_position,
                            hit.total_resultados,
                            document_type.log_label,
                            hit.protocolo,
                        )
                        handles_before_click = set(self.driver.window_handles)
                        self.abrir_documento_no_filtro(
                            position=hit.selected_position,
                            timeout_seconds=self.timeout_seconds,
                        )
                        handles_after_click = set(self.driver.window_handles)
                        attempt_opened_doc_handles = handles_after_click - handles_before_click
                        switched_handle = self._switch_to_newly_opened_window(
                            handles_before=handles_before_click,
                            reason=f"{document_type.log_label} {processo} posicao={hit.selected_position}",
                        )
                        if switched_handle:
                            attempt_opened_doc_handles.add(switched_handle)
                        snapshot_saved = self._extract_and_process_document_snapshot(
                            processo=processo,
                            protocolo_documento=hit.protocolo,
                            document_type=document_type,
                            collection_context=attempt_context,
                        )
                        if snapshot_saved:
                            self._log_valid_candidate_early_stop(processo, document_type)
                            return True
                        saw_invalid_filter_candidate = True
                        collection_context = self._build_collection_context(
                            found=True,
                            found_in="filter",
                            search_term=termo,
                            results_count=hit.total_resultados,
                            chosen_documento=hit.protocolo,
                            selection_reason="candidate_retained_only_in_silver",
                            selection_detail=f"position={hit.selected_position} total={hit.total_resultados}",
                        )
                    finally:
                        self._close_opened_doc_tabs(
                            processo=processo,
                            opened_doc_handles=attempt_opened_doc_handles,
                            preferred_return_handle=processo_handle,
                        )

            attempted_tree_fallback = False
            if self._should_use_tree_fallback(document_type):
                self.logger.info(
                    "Processo %s: nenhum candidato canonico de %s consolidado no filtro; tentando fallback pela arvore.",
                    processo,
                    document_type.log_label,
                )
                attempted_tree_fallback = True
                if self._open_document_via_tree(processo, document_type, process_url=process_url):
                    self.logger.info(
                        "Processo %s: %s recuperado via arvore apos filtro.",
                        processo,
                        document_type.log_label,
                    )
                    return True
            else:
                self.logger.info(
                    "Processo %s: %s fallback skip: baixa relevância do tipo.",
                    processo,
                    document_type.log_label,
                )

            selection_reason = "not_found_after_filter"
            selection_detail = "nao encontrado no filtro"
            if attempted_tree_fallback:
                selection_reason = "not_found_after_filter_and_tree"
                selection_detail = (
                    "nao encontrado no filtro nem na arvore"
                    if not found_any_filter_candidate
                    else "candidatos do filtro/arvore ficaram apenas na silver; nenhum canonico publicado"
                )
            if saw_invalid_filter_candidate:
                selection_reason = (
                    "no_canonical_candidate_after_filter_and_tree"
                    if attempted_tree_fallback
                    else "no_canonical_candidate_after_filter"
                )
                selection_detail = "candidatos do filtro ficaram apenas na silver; nenhum canonico publicado"
            collection_context = self._build_collection_context(
                found=False,
                found_in="none",
                search_term="|".join(filter_terms),
                results_count=0,
                chosen_documento=collection_context.get("chosen_documento", ""),
                selection_reason=selection_reason,
                selection_detail=selection_detail,
                extraction_error=collection_context.get("extraction_error", ""),
            )
            self._record_document_search_outcome(processo, document_type, collection_context)
            self.logger.info(
                "Processo %s: nenhum %s canonico localizado; seguindo.",
                processo,
                document_type.log_label,
            )
            return False
        except (TimeoutException, NoSuchElementException) as exc:
            if self._is_search_context_stagnation_timeout(exc):
                self._process_filter_degraded[processo] = True
                self.logger.warning(
                    "Processo %s: busca do filtro para %s entrou em estagnacao; o contexto do processo foi marcado como degradado.",
                    processo,
                    document_type.display_name,
                )
            collection_context = self._build_collection_context(
                found=bool(collection_context.get("found")),
                found_in=collection_context.get("found_in", "filter"),
                search_term=collection_context.get("search_term", document_type.display_name),
                results_count=collection_context.get("results_count", 0),
                chosen_documento=collection_context.get("chosen_documento", ""),
                selection_reason=(
                    "search_context_stagnation"
                    if self._is_search_context_stagnation_timeout(exc)
                    else collection_context.get("selection_reason", "search_open_error")
                ),
                selection_detail=collection_context.get("selection_detail", ""),
                extraction_error=str(exc),
            )
            self._record_document_search_outcome(processo, document_type, collection_context)
            self.logger.warning(
                "Processo %s: falha resiliente ao buscar/abrir '%s' (%s); seguindo.",
                processo,
                collection_context.get("search_term", document_type.display_name) or document_type.display_name,
                exc,
            )
            return False

    def _find_plano_trabalho_link_in_tree(self) -> Optional[Any]:
        document_type = self._get_document_type("pt")
        if document_type is None:
            return None
        tree_result = self._find_document_link_in_tree(document_type)
        if tree_result is None:
            return None
        return tree_result[0]

    def _abrir_plano_trabalho_pela_arvore(self, processo: str) -> bool:
        document_type = self._get_document_type("pt")
        if document_type is None:
            return False
        return self._open_document_via_tree(processo, document_type)

    def _buscar_e_abrir_plano_de_trabalho_mais_recente(self, processo: str) -> None:
        document_type = self._get_document_type("pt")
        if document_type is None:
            return
        self._buscar_e_abrir_documento_mais_recente(processo, document_type)

    def _switch_to_newly_opened_window(
        self,
        handles_before: Set[str],
        reason: str,
        timeout_seconds: int = 6,
    ) -> Optional[str]:
        if not handles_before:
            return None

        deadline = time.time() + max(1, timeout_seconds)
        while time.time() < deadline:
            try:
                handles_now = set(self.driver.window_handles)
            except WebDriverException as exc:
                self.logger.warning("Switch nova janela (%s): falha ao ler handles (%s).", reason, exc)
                return None

            new_handles = [h for h in handles_now if h not in handles_before]
            if not new_handles:
                time.sleep(0.15)
                continue

            target = new_handles[-1]
            try:
                self.driver.switch_to.window(target)
                self.logger.info(
                    "Switch nova janela (%s): sucesso. handle=%s url=%s title=%s",
                    reason,
                    target,
                    self.driver.current_url,
                    self.driver.title,
                )
                return target
            except WebDriverException as exc:
                self.logger.warning(
                    "Switch nova janela (%s): falha no switch para handle=%s (%s).",
                    reason,
                    target,
                    exc,
                )
                return None

        self.logger.info(
            "Switch nova janela (%s): nenhum novo handle detectado apos %ss.",
            reason,
            timeout_seconds,
        )
        return None

    def _close_opened_doc_tabs(
        self,
        processo: str,
        opened_doc_handles: Set[str],
        preferred_return_handle: str,
    ) -> None:
        if not opened_doc_handles:
            return

        closed = 0
        failed: List[str] = []
        for handle in list(opened_doc_handles):
            try:
                handles_now = set(self.driver.window_handles)
                if handle not in handles_now:
                    continue
                self.driver.switch_to.window(handle)
                self.logger.info("Processo %s: fechando aba PT handle=%s", processo, handle)
                self.driver.close()
                closed += 1
            except WebDriverException as exc:
                failed.append(handle)
                self.logger.warning(
                    "Processo %s: falha ao fechar aba PT handle=%s (%s).",
                    processo,
                    handle,
                    exc,
                )

        try:
            remaining = list(self.driver.window_handles)
            if preferred_return_handle and preferred_return_handle in remaining:
                self.driver.switch_to.window(preferred_return_handle)
            elif self.main_window_handle and self.main_window_handle in remaining:
                self.driver.switch_to.window(self.main_window_handle)
            elif remaining:
                self.driver.switch_to.window(remaining[0])
            self.logger.info(
                "Processo %s: fechamento de abas PT concluido. fechadas=%d falhas=%d handles_restantes=%d atual=%s",
                processo,
                closed,
                len(failed),
                len(remaining),
                self.driver.current_window_handle if remaining else "-",
            )
        except WebDriverException as exc:
            self.logger.warning("Processo %s: falha ao restaurar contexto apos fechar abas PT (%s).", processo, exc)
        except WebDriverException as exc:
            self.logger.warning(
                "Processo %s: erro WebDriver ao buscar/abrir '%s' (%s); seguindo.",
                processo,
                termo,
                exc,
            )

    def buscar_documento_mais_recente_no_filtro(
        self, termo: str, timeout_seconds: int = 20
    ) -> Optional[document_search.SearchHit]:
        """Na tela 'Pesquisar no Processo', busca e seleciona o documento mais recente (topo)."""
        self.logger.info(
            "Busca filtro: termo='%s' timeout=%ss url=%s title=%s",
            termo,
            timeout_seconds,
            self.driver.current_url,
            self.driver.title,
        )
        return document_search.buscar_documento_mais_recente(
            driver=self.driver,
            selectors=self.selectors,
            logger=self.logger,
            termo=termo,
            timeout_seconds=timeout_seconds,
        )

    def buscar_documentos_no_filtro(
        self, termo: str, timeout_seconds: int = 20
    ) -> List[document_search.SearchHit]:
        self.logger.info(
            "Busca filtro: listando resultados termo='%s' timeout=%ss url=%s title=%s",
            termo,
            timeout_seconds,
            self.driver.current_url,
            self.driver.title,
        )
        return document_search.listar_resultados_pesquisa(
            driver=self.driver,
            selectors=self.selectors,
            logger=self.logger,
            termo=termo,
            timeout_seconds=timeout_seconds,
        )

    def abrir_documento_mais_recente_no_filtro(self, timeout_seconds: int = 20) -> None:
        """Na tela de resultados, abre o documento mais recente (primeiro resultado)."""
        self.abrir_documento_no_filtro(position=1, timeout_seconds=timeout_seconds)

    def abrir_documento_no_filtro(self, position: int, timeout_seconds: int = 20) -> None:
        """Na tela de resultados, abre o documento na posicao informada."""
        self.logger.info(
            "Abertura filtro: tentando abrir resultado posicao=%d timeout=%ss url=%s title=%s",
            position,
            timeout_seconds,
            self.driver.current_url,
            self.driver.title,
        )
        document_search.abrir_resultado_pesquisa_por_posicao(
            driver=self.driver,
            selectors=self.selectors,
            logger=self.logger,
            position=position,
            timeout_seconds=timeout_seconds,
        )
    # Internos (menu, selecao guiada, paginacao)
    def _open_interno_menu(self) -> None:
        sel = self.selectors.get("tela_inicio", {})
        x_bloco = sel.get("bloco")
        x_interno = sel.get("interno")
        if not (x_bloco and x_interno):
            raise RuntimeError("Seletores de menu bloco/interno ausentes em xpath_selector.json")

        bloco_candidates = [
            x_bloco,
            "//a[contains(normalize-space(.), 'Bloco')]",
            "//span[contains(normalize-space(.), 'Bloco')]/ancestor::a[1]",
        ]
        interno_candidates = [
            x_interno,
            "//a[contains(normalize-space(.), 'Interno')]",
            "//span[contains(normalize-space(.), 'Interno')]/ancestor::a[1]",
        ]

        try:
            self._click_first_clickable(bloco_candidates, "menu_bloco")
            self._click_first_clickable(interno_candidates, "submenu_interno")
        except TimeoutException as exc:
            raise RuntimeError("Nao foi possivel abrir o menu Bloco > Interno.") from exc

    def _normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        fixed = value
        if any(marker in fixed for marker in ("\u00C3", "\u00C2", "\uFFFD")):
            try:
                fixed = fixed.encode("latin1").decode("utf-8")
            except UnicodeError:
                fixed = value

        collapsed = " ".join(fixed.split()).strip().upper()
        deaccented = unicodedata.normalize("NFKD", collapsed)
        return "".join(ch for ch in deaccented if not unicodedata.combining(ch))

    def _parse_descricoes_busca(self, raw_value: str | None) -> List[str]:
        if not raw_value:
            return []
        return [part for part in (self._normalize_text(x) for x in raw_value.split("|")) if part]

    def _descricao_match(self, descricao_normalizada: str, alvo_normalizado: str) -> bool:
        if self.descricao_match_mode == "equals":
            return descricao_normalizada == alvo_normalizado
        return alvo_normalizado in descricao_normalizada

    def _is_valid_descricao_candidate(self, text: str, numero_normalizado: str) -> bool:
        normalized = self._normalize_text(text)
        if not normalized:
            return False
        if normalized == numero_normalizado:
            return False
        if normalized in {"GERADO", "RECEBIDO", "CONCLUIDO"}:
            return False
        if len(normalized) < 4:
            return False
        return True

    def _extract_descricao_from_row(
        self,
        row: Any,
        numero_normalizado: str,
        x_desc_rel: str,
    ) -> str:
        try:
            desc_cells = row.find_elements(By.XPATH, x_desc_rel)
        except WebDriverException:
            desc_cells = []

        for cell in desc_cells:
            text = (cell.text or "").strip()
            if self._is_valid_descricao_candidate(text, numero_normalizado):
                return text

        for td in row.find_elements(By.XPATH, ".//td[not(contains(@class,'d-none'))]"):
            try:
                if td.find_elements(By.XPATH, ".//a|.//img|.//input|.//button"):
                    continue
            except WebDriverException:
                continue

            text = (td.text or "").strip()
            if self._is_valid_descricao_candidate(text, numero_normalizado):
                return text

        return ""

    def _find_elements_any_context(self, xpath: str) -> List[Any]:
        try:
            self.driver.switch_to.default_content()
            elems = self.driver.find_elements(By.XPATH, xpath)
            if elems:
                return elems

            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            for idx in range(len(iframes)):
                try:
                    self.driver.switch_to.default_content()
                    current_iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                    if idx >= len(current_iframes):
                        continue

                    self.driver.switch_to.frame(current_iframes[idx])
                    frame_elems = self.driver.find_elements(By.XPATH, xpath)
                    if frame_elems:
                        return frame_elems
                except WebDriverException:
                    continue
        except WebDriverException:
            return []
        finally:
            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass
        return []

    def _get_current_interno_page_signature(self) -> tuple[str, int] | None:
        sel = self.selectors.get("interno", {})
        x_rows = sel.get("tabela_blocos_rows") or "//tr[td]"
        rows = self._find_elements_any_context(x_rows)
        return _build_rows_signature(rows)

    def _collect_interno_rows_current_page(self, page: int) -> List[InternoRow]:
        sel = self.selectors.get("interno", {})
        x_rows = sel.get("tabela_blocos_rows") or "//tr[td]"
        x_link_rel = sel.get("numero_interno_link") or ".//a[contains(@class,'ancoraBlocoAberto')]"
        x_desc_rel = (
            sel.get("descricao_cell")
            or ".//a[contains(@class,'ancoraBlocoAberto')]/ancestor::td/following-sibling::td[not(contains(@class,'d-none')) and not(descendant::a) and normalize-space(.)!='']"
        )

        rows: List[Any] = []
        try:
            rows = self.wait_for_elements(
                x_rows,
                tag=f"tabela_blocos_rows_page_{page}",
                timeout=max(4, min(8, self.timeout_seconds)),
                restore_context=False,
            )
        except TimeoutException:
            self.logger.info("Pagina %d: nenhuma linha de tabela encontrada.", page)
            return []

        internos: List[InternoRow] = []
        descricoes_vazias = 0
        for idx, row in enumerate(rows, start=1):
            try:
                links = row.find_elements(By.XPATH, x_link_rel)
                if not links:
                    continue

                link = links[0]
                numero = self._normalize_text(link.text)
                if not numero:
                    continue

                descricao = self._extract_descricao_from_row(
                    row=row,
                    numero_normalizado=numero,
                    x_desc_rel=x_desc_rel,
                )
                if not descricao:
                    descricoes_vazias += 1

                descricao_norm = self._normalize_text(descricao)
                internos.append(
                    InternoRow(
                        numero_interno=numero,
                        descricao=descricao.strip(),
                        descricao_normalizada=descricao_norm,
                        link=link,
                        page=page,
                        row_index=idx,
                    )
                )
            except StaleElementReferenceException:
                continue

        self.logger.info(
            "Pagina %d: linhas=%d links_numero_interno=%d",
            page,
            len(rows),
            len(internos),
        )
        self.logger.info("Pagina %d: descricoes_vazias=%d", page, descricoes_vazias)
        for sample in internos[:5]:
            self.logger.info(
                "Amostra interno: numero=%s descricao='%s'",
                sample.numero_interno,
                sample.descricao,
            )
        return internos

    def _click_next_page_if_available(self, page: int) -> bool:
        sel = self.selectors.get("interno", {})
        previous_signature = self._get_current_interno_page_signature()
        candidates = [
            sel.get("paginacao_proxima"),
            "//a[@title='Proxima' or @title='Próxima']",
            "//a[contains(@aria-label,'Proxima') or contains(@aria-label,'Próxima')]",
            "//img[contains(@title,'Proxima') or contains(@title,'Próxima')]/ancestor::a[1]",
        ]
        checked: List[str] = []
        for xpath in candidates:
            if not xpath or xpath in checked:
                continue
            checked.append(xpath)
            elems = self._find_elements_any_context(xpath)
            if not elems:
                continue

            for elem in elems:
                try:
                    classes = (elem.get_attribute("class") or "").lower()
                    aria_disabled = (elem.get_attribute("aria-disabled") or "").lower()
                    if "disabled" in classes or aria_disabled == "true":
                        continue
                    elem.click()
                    self.logger.info("Paginacao: avancando para pagina %d", page + 1)
                    if _wait_for_page_signature_change(
                        self._get_current_interno_page_signature,
                        previous_signature,
                        elem,
                    ):
                        return True
                    time.sleep(PAGINATION_SETTLE_SLEEP_SECONDS)
                    return True
                except (StaleElementReferenceException, WebDriverException):
                    continue

        self.logger.info("Paginacao: nenhuma proxima pagina detectada apos pagina %d.", page)
        return False
    def _collect_interno_rows_with_pagination(self, max_pages: int = 10) -> List[InternoRow]:
        seen: Set[tuple[str, str]] = set()
        collected: List[InternoRow] = []
        page = 1
        while page <= max_pages:
            try:
                page_rows = self._collect_interno_rows_current_page(page=page)
                for item in page_rows:
                    signature = (item.numero_interno, item.descricao_normalizada)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    collected.append(item)
            finally:
                try:
                    self.driver.switch_to.default_content()
                except WebDriverException:
                    pass

            if page >= max_pages:
                self.logger.warning("Paginacao: limite de seguranca atingido (%d paginas).", max_pages)
                break
            if not self._click_next_page_if_available(page=page):
                break
            page += 1

        self.logger.info("Total coletado na tabela de internos: %d", len(collected))
        return collected

    def _select_guided_internos_by_descricao(self) -> List[Tuple[InternoRow, str, str]]:
        if not self.descricoes_busca:
            self.logger.warning(
                "DESCRICOES_BUSCA vazio. Defina no .env (ex.: DESCRICOES_BUSCA=\"A|B|C\")."
            )
            return []

        list_url = self.driver.current_url
        internos = self._collect_interno_rows_with_pagination(max_pages=10)
        if not internos:
            self.logger.warning("Nenhum numero interno elegivel foi encontrado na tabela.")
            return []

        matches_by_target: Dict[str, List[InternoRow]] = {target: [] for target in self.descricoes_busca}
        for item in internos:
            for target in self.descricoes_busca:
                if self._descricao_match(item.descricao_normalizada, target):
                    matches_by_target[target].append(item)

        total_matches = 0
        for target in self.descricoes_busca:
            count = len(matches_by_target[target])
            total_matches += count
            self.logger.info("Matches para descricao '%s': %d", target, count)
        self.logger.info("Total de matches por descricao: %d", total_matches)

        if total_matches == 0:
            self.logger.warning("Nenhuma descricao da lista teve match na tabela.")
            return []

        selected_items: List[Tuple[InternoRow, str, str]] = []
        seen: Set[Tuple[str, str]] = set()
        for target in self.descricoes_busca:
            for item in matches_by_target[target]:
                signature = (item.numero_interno, item.descricao_normalizada)
                if signature in seen:
                    continue
                seen.add(signature)
                selected_items.append((item, target, list_url))

        if not selected_items:
            self.logger.warning("Houve match contabilizado, mas nenhum item selecionavel.")
            return []

        return selected_items

    def _click_selected_interno(
        self,
        selected: InternoRow,
        selected_target: str,
        list_url: str,
    ) -> bool:
        try:
            self.driver.get(list_url)
            self._wait_for_document_ready(self.timeout_seconds, "reload_lista_internos")
        except WebDriverException as exc:
            self.logger.error("Falha ao recarregar lista de internos para clique guiado: %s", exc)
            return False

        current_page = 1
        while current_page < selected.page:
            if not self._click_next_page_if_available(page=current_page):
                self.logger.error(
                    "Nao consegui navegar ate a pagina do item selecionado. alvo_page=%d pagina_atual=%d",
                    selected.page,
                    current_page,
                )
                return False
            current_page += 1

        try:
            page_rows = self._collect_interno_rows_current_page(page=selected.page)
            for item in page_rows:
                if (
                    item.numero_interno == selected.numero_interno
                    and item.descricao_normalizada == selected.descricao_normalizada
                ):
                    item.link.click()
                    self.logger.info(
                        "Numero interno clicado: %s | descricao='%s' | criterio='%s' | pagina=%d linha=%d",
                        selected.numero_interno,
                        selected.descricao,
                        selected_target,
                        selected.page,
                        selected.row_index,
                    )
                    return True
        except (StaleElementReferenceException, WebDriverException) as exc:
            self.logger.error("Falha ao executar clique guiado no numero interno: %s", exc)
            return False
        finally:
            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass

        self.logger.error(
            "Item selecionado nao foi encontrado para clique. numero=%s descricao='%s' pagina=%d",
            selected.numero_interno,
            selected.descricao,
            selected.page,
        )
        return False

    def _get_current_interno_descricao_value(self) -> str:
        contexts_to_try: List[tuple[str, Any]] = [("default", None)]
        try:
            self.driver.switch_to.default_content()
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            for frame in iframes:
                contexts_to_try.append(("iframe", frame))
        except WebDriverException:
            pass

        for kind, frame in contexts_to_try:
            try:
                self.driver.switch_to.default_content()
                if kind == "iframe" and frame is not None:
                    self.driver.switch_to.frame(frame)

                elem = self.driver.find_element(
                    By.XPATH,
                    "//input[@id='txtDescricao' or @name='txtDescricao']",
                )
                value = (elem.get_attribute("value") or elem.get_attribute("title") or elem.text or "").strip()
                if value:
                    return value
            except WebDriverException:
                continue
            finally:
                try:
                    self.driver.switch_to.default_content()
                except WebDriverException:
                    pass

        return ""

    # Preview (PARCERIAS VIGENTES)
    def _should_collect_preview_for_current_descricao(self, descricao_atual: str) -> bool:
        descricao_norm = self._normalize_text(descricao_atual)
        return descricao_norm == "PARCERIAS VIGENTES"

    def _clean_text_value(self, value: str) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()

    def _clean_numero_act(self, value: str) -> str:
        cleaned = self._clean_text_value(value)
        if not cleaned:
            return ""
        cleaned = re.sub(
            r"^(?:N[º°oO]\s*|N\.\s*|NO\s+|NRO\.?\s*|NUM\.?\s*)",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        return cleaned.rstrip(" .;,:")

    def _normalize_label(self, label: str) -> str:
        return re.sub(r"\s+", " ", self._normalize_text(label)).strip()

    def _looks_like_metadata_label(self, label_norm: str) -> bool:
        if not label_norm:
            return False
        explicit_prefixes = (
            "GESTOR",
            "PORTARIA",
            "DOU",
            "DATA",
            "STATUS",
            "DESIGNACAO",
            "VENCIMENTO",
            "TERMO ADITIVO",
            "PARCEIRO",
            "PARCEIROS",
            "VIGENCIA",
            "NUMERO",
            "NUMERO ACT",
            "OBJETO",
            "VALOR",
            "TIPO",
            "PROCESSO",
        )
        if any(label_norm.startswith(prefix) for prefix in explicit_prefixes):
            return True

        words = [w for w in label_norm.split(" ") if w]
        if not words or len(words) > 4:
            return False
        if not re.fullmatch(r"[A-Z0-9 /().-]+", label_norm):
            return False
        return True

    def _extract_anotacao_prefixed_value(self, line: str) -> tuple[str, str] | None:
        compact = (line or "").strip()
        if ":" not in compact:
            return None

        label, value = compact.split(":", 1)
        label_norm = self._normalize_label(label)
        value_clean = self._clean_text_value(value)

        if label_norm in {"PARCEIRO", "PARCEIROS"}:
            return ("parceiro", value_clean)
        if label_norm == "VIGENCIA":
            return ("vigencia", value_clean)
        if label_norm in {"NUMERO ACT", "NUMEROACT", "NUMERO"}:
            return ("numero_act", self._clean_numero_act(value_clean))
        if label_norm == "OBJETO":
            return ("objeto", value)
        return None

    def parse_anotacoes(self, anotacoes_raw: str) -> Dict[str, str]:
        parsed = {
            "parceiro": "",
            "vigencia": "",
            "numero_act": "",
            "objeto": "",
        }

        objeto_lines: List[str] = []
        collecting_objeto = False

        for raw_line in (anotacoes_raw or "").replace("\r", "\n").splitlines():
            line = raw_line.strip()
            if not line:
                continue

            label_match = re.match(r"^\s*([^:\n]{2,80})\s*:\s*(.*)$", line)
            if collecting_objeto and label_match:
                candidate_label_norm = self._normalize_label(label_match.group(1))
                if self._looks_like_metadata_label(candidate_label_norm) and candidate_label_norm != "OBJETO":
                    collecting_objeto = False

            prefixed = self._extract_anotacao_prefixed_value(line)
            if prefixed and not collecting_objeto:
                key, value = prefixed
                if key == "objeto":
                    collecting_objeto = True
                    objeto_lines = []
                    initial_obj = self._clean_text_value(value)
                    if initial_obj:
                        objeto_lines.append(initial_obj)
                elif not parsed[key]:
                    parsed[key] = value
                continue

            if collecting_objeto:
                # Reprocessa a linha como novo rotulo quando o bloco de OBJETO termina.
                if label_match:
                    candidate_label_norm = self._normalize_label(label_match.group(1))
                    if self._looks_like_metadata_label(candidate_label_norm) and candidate_label_norm != "OBJETO":
                        prefixed_after_obj = self._extract_anotacao_prefixed_value(line)
                        if prefixed_after_obj:
                            key, value = prefixed_after_obj
                            if key != "objeto" and not parsed[key]:
                                parsed[key] = value
                        continue

                cleaned_line = self._clean_text_value(line)
                if cleaned_line:
                    objeto_lines.append(cleaned_line)

        if objeto_lines:
            parsed["objeto"] = self._clean_text_value(" ".join(objeto_lines))

        return parsed

    def _parse_preview_anotacoes(self, anotacoes_raw: str) -> Dict[str, str]:
        # Compatibilidade com chamadas legadas internas.
        return self.parse_anotacoes(anotacoes_raw)

    def _infer_seq_coluna(self, cell_texts: List[str], processo: str) -> str:
        for text in cell_texts[:3]:
            if not text:
                continue
            if processo and text == processo:
                continue
            if re.fullmatch(r"\d{1,6}", text):
                return text

        for text in cell_texts[:3]:
            if not text:
                continue
            if processo and text == processo:
                continue
            return text

        return ""

    def _cell_looks_like_anotacoes(self, text: str) -> bool:
        if not text:
            return False
        normalized = self._normalize_text(text)
        markers = ("PARCEIRO:", "PARCEIROS:", "VIGENCIA:", "OBJETO:", "NUMERO ACT:", "NUMERO:")
        normalized_markers = [self._normalize_text(m) for m in markers]
        return any(marker in normalized for marker in normalized_markers)

    def _find_anotacoes_cell_index(self, tds: List[Any], cell_texts: List[str]) -> int:
        for idx, text in enumerate(cell_texts):
            if self._cell_looks_like_anotacoes(text):
                return idx

        best_idx = -1
        best_score = -1
        for idx, td in enumerate(tds):
            text = cell_texts[idx] if idx < len(cell_texts) else ""
            if not text:
                continue
            try:
                html = (td.get_attribute("innerHTML") or "").upper()
            except WebDriverException:
                html = ""

            score = 0
            score += text.count(":") * 3
            score += text.count("\n") * 2
            if "<BR" in html:
                score += 6
            if len(text) > 80:
                score += 2

            if score > best_score:
                best_score = score
                best_idx = idx

        return best_idx

    def _find_processo_cell_index(self, tds: List[Any]) -> int:
        for idx, td in enumerate(tds):
            try:
                links = td.find_elements(By.CSS_SELECTOR, "a.protocoloFechado, a[class*='protocoloFechado']")
            except WebDriverException:
                links = []
            if links:
                return idx
        return -1


    def _extract_preview_record_from_row(self, row: Any, interno_descricao: str) -> Optional[Dict[str, str]]:
        try:
            tds = row.find_elements(By.CSS_SELECTOR, "td")
        except WebDriverException:
            return None
        if not tds:
            return None

        cell_texts = [(td.text or "").strip() for td in tds]

        processo = ""
        for selector in ("a.protocoloFechado", "a[class*='protocoloFechado']", "a"):
            try:
                links = row.find_elements(By.CSS_SELECTOR, selector)
            except WebDriverException:
                links = []
            for link in links:
                text = (link.text or "").strip()
                if text:
                    processo = text
                    break
            if processo:
                break

        if not processo:
            return None

        processo_idx = self._find_processo_cell_index(tds)
        anotacoes_idx = self._find_anotacoes_cell_index(tds, cell_texts)
        anotacoes_raw = ""
        if anotacoes_idx >= 0 and anotacoes_idx < len(tds):
            try:
                anotacoes_raw = (tds[anotacoes_idx].get_attribute("innerText") or "").strip()
            except WebDriverException:
                anotacoes_raw = (tds[anotacoes_idx].text or "").strip()
        anotacoes_parsed = self._parse_preview_anotacoes(anotacoes_raw)
        cell_texts_compact = [(" ".join(text.split()).strip()) for text in cell_texts]

        return {
            "interno_descricao": "PARCERIAS VIGENTES",
            "seq": self._infer_seq_coluna(cell_texts_compact, processo),
            "processo": processo,
            "parceiro": anotacoes_parsed["parceiro"],
            "vigencia": anotacoes_parsed["vigencia"],
            "objeto": anotacoes_parsed["objeto"],
            "numero_act": anotacoes_parsed["numero_act"],
        }

    def _collect_preview_records_from_current_page(self, interno_descricao: str) -> List[Dict[str, str]]:
        try:
            rows = self.wait_for_elements(
                "//table[@id='tblProtocolosBlocos']//tbody/tr[td]",
                tag="preview_tblProtocolosBlocos_rows",
                timeout=max(5, min(10, self.timeout_seconds)),
                restore_context=False,
            )
        except TimeoutException:
            self.logger.warning(
                "Coleta preview pulada: tabela tblProtocolosBlocos nao encontrada para descricao '%s'.",
                interno_descricao,
            )
            return []

        records: List[Dict[str, str]] = []
        try:
            for row in rows:
                record = self._extract_preview_record_from_row(row, interno_descricao)
                if record:
                    records.append(record)
        finally:
            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass

        return records

    def _collect_preview_records_from_current_list(self, interno_descricao: str) -> List[Dict[str, str]]:
        all_records: List[Dict[str, str]] = []
        seen_rows: Set[Tuple[str, str, str]] = set()
        seen_pages: Set[Tuple[str, int]] = set()
        page = 1
        max_pages = 100

        while page <= max_pages:
            page_records = self._collect_preview_records_from_current_page(interno_descricao)
            page_signature = (
                page_records[0]["processo"] if page_records else "",
                len(page_records),
            )
            if page_signature in seen_pages and page_records:
                self.logger.warning(
                    "Coleta PARCERIAS VIGENTES: pagina repetida detectada na pagina %d; encerrando paginacao.",
                    page,
                )
                break
            seen_pages.add(page_signature)

            for record in page_records:
                row_key = (record.get("seq", ""), record.get("processo", ""), record.get("numero_act", ""))
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)
                all_records.append(record)

            if not self._click_next_page_if_available(page=page):
                break
            page += 1

        if page > max_pages:
            self.logger.warning(
                "Coleta PARCERIAS VIGENTES: limite de seguranca de paginacao atingido (%d paginas).",
                max_pages,
            )

        return all_records

    def _resolve_preview_output_dir(self) -> Path:
        configured = (self.settings.output_dir or "output").strip()
        backend_root = Path(__file__).resolve().parents[2]
        output_dir = Path(configured)
        if not output_dir.is_absolute():
            output_dir = backend_root / output_dir
        return output_dir

    def _finalize_document_runs(self) -> None:
        output_dir = self._resolve_preview_output_dir()
        for document_type in self.document_types:
            document_type.handler.finalize_run(
                spec=document_type,
                output_dir=output_dir,
                logger=self.logger,
                settings=self.settings,
            )

    def _reset_candidate_screening_stats(self) -> None:
        self.total_candidatos_avaliados = 0
        self.candidatos_descartados_pre_abertura = 0

    def _should_skip_candidate_pre_open(self, candidate_text: str) -> bool:
        self.total_candidatos_avaliados += 1
        if not should_skip_candidate(candidate_text):
            return False
        self.candidatos_descartados_pre_abertura += 1
        self.logger.info("candidato_descartado_pre_abertura: %s", candidate_text)
        return True

    def _log_candidate_screening_summary(self) -> None:
        total = self.total_candidatos_avaliados
        discarded = self.candidatos_descartados_pre_abertura
        discard_percentage = (discarded / total * 100.0) if total else 0.0
        self.logger.info(
            "Triagem de candidatos: total_candidatos=%d descartados_pre_abertura=%d percentual_descarte=%.2f%%",
            total,
            discarded,
            discard_percentage,
        )

    def _save_pt_tracking_reports(self) -> None:
        document_type = self._get_document_type("pt")
        if document_type is None:
            return
        document_type.handler.finalize_run(
            spec=document_type,
            output_dir=self._resolve_preview_output_dir(),
            logger=self.logger,
            settings=self.settings,
        )

    def _snapshot_contains_any_marker(self, value: str, markers: Tuple[str, ...]) -> bool:
        normalized = self._normalize_text(value)
        return any(self._normalize_text(marker) in normalized for marker in markers if marker)

    def _snapshot_text_blob(
        self,
        snapshot: Dict[str, Any],
        collection_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        parts = [
            str((collection_context or {}).get("chosen_documento", "") or ""),
            str(snapshot.get("title", "") or ""),
            str(snapshot.get("url", "") or ""),
            str(snapshot.get("text", "") or "")[:8000],
        ]
        return self._normalize_text(" ".join(part for part in parts if part))

    def _looks_like_email_snapshot(
        self,
        snapshot: Dict[str, Any],
        collection_context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        blob = self._snapshot_text_blob(snapshot, collection_context)
        selected_doc = self._normalize_text(str((collection_context or {}).get("chosen_documento", "") or ""))
        title_blob = self._normalize_text(str(snapshot.get("title", "") or ""))
        if any(marker in selected_doc or marker in title_blob for marker in ("E-MAIL", "EMAIL")):
            return True

        email_markers = ("ASSUNTO:", "PARA:", "DE:", "ENVIADO:", "ENVIADA:", "CC:", "CCO:")
        hits = sum(1 for marker in email_markers if marker in blob)
        return hits >= 3

    def _classify_pt_snapshot(
        self,
        snapshot: Dict[str, Any],
        collection_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        chosen_documento = self._normalize_text(str((collection_context or {}).get("chosen_documento", "") or ""))
        title_blob = self._normalize_text(str(snapshot.get("title", "") or ""))
        text_head = self._normalize_text(str(snapshot.get("text", "") or "")[:1600])
        title_context = " ".join(part for part in (chosen_documento, title_blob) if part)

        has_plano_marker = "PLANO DE TRABALHO" in text_head or "PLANO DE TRABALHO" in title_context
        has_documentacao_marker = "DOCUMENTACAO" in title_context
        has_minuta_marker = "MINUTA" in title_context or "MINUTAS" in title_context
        has_minuta_text = bool(
            re.search(r"\bMINUTA(?:\s+DE)?\s+PLANO\s+DE\s+TRABALHO\b", text_head, flags=re.IGNORECASE)
        )

        is_non_canonical = has_plano_marker and (has_minuta_text or (has_documentacao_marker and has_minuta_marker))
        if is_non_canonical:
            return {
                "doc_class": "pt_minuta_documentacao",
                "requested_type": REQUESTED_TYPE_PT,
                "resolved_document_type": RESOLVED_TYPE_PT,
                "is_canonical_candidate": False,
                "validation_status": VALIDATION_STATUS_NON_CANONICAL,
                "publication_status": "retained_silver",
                "discard_reason": "minuta_documentacao",
                "classification_reason": CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
            }

        return {
            "doc_class": RESOLVED_TYPE_PT,
            "requested_type": REQUESTED_TYPE_PT,
            "resolved_document_type": RESOLVED_TYPE_PT,
            "is_canonical_candidate": True,
            "validation_status": VALIDATION_STATUS_VALID,
            "publication_status": "",
            "discard_reason": "",
            "classification_reason": "",
        }

    def _validate_snapshot_for_document_type(
        self,
        processo: str,
        document_type: DocumentTypeSpec,
        snapshot: Dict[str, Any],
        collection_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        blob = self._snapshot_text_blob(snapshot, collection_context)
        if not blob:
            return (False, "snapshot_vazio", None)

        invalid_search_markers = (
            "PESQUISAR NO PROCESSO",
            "TIPOS DE DOCUMENTOS DISPONIVEIS NESTE PROCESSO",
            "VER CRITERIOS DE PESQUISA",
        )
        if any(marker in blob for marker in invalid_search_markers):
            return (False, "pagina_de_pesquisa", None)

        if document_type.key in {"act", "memorando", "ted"}:
            analysis = classify_cooperation_snapshot(snapshot, document_type.key, collection_context, processo=processo)
            return (True, str(analysis.get("doc_class", "") or "ok"), analysis)

        if document_type.key == "pt":
            if "PLANO DE TRABALHO" not in blob:
                return (False, "conteudo_nao_compativel_com_pt", None)
            analysis = self._classify_pt_snapshot(snapshot, collection_context)
            return (True, str(analysis.get("doc_class", "") or "ok"), analysis)

        return (True, "ok", None)

    def _extract_and_process_document_snapshot(
        self,
        processo: str,
        protocolo_documento: str,
        document_type: DocumentTypeSpec,
        collection_context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        try:
            iframe_info = get_iframes_info(self.driver)
            self.logger.info(
                "Processo %s: iniciando snapshot %s. contexto_pre_snapshot url=%s title=%s iframes=%d",
                processo,
                document_type.log_label,
                self.driver.current_url,
                self.driver.title,
                len(iframe_info),
            )
            if iframe_info:
                self.logger.info("Processo %s: iframes pre_snapshot=%s", processo, iframe_info)
            snapshot = sanitize_snapshot(
                document_text_extractor.extract_document_snapshot(
                    self.driver,
                    logger=self.logger,
                )
            )
            is_valid, validation_reason, analysis = self._validate_snapshot_for_document_type(
                processo,
                document_type,
                snapshot,
                collection_context,
            )
            if not is_valid:
                self.logger.warning(
                    "Processo %s: snapshot rejeitado para %s. motivo=%s titulo=%s url=%s texto_preview=%s",
                    processo,
                    document_type.log_label,
                    validation_reason,
                    snapshot.get("title", ""),
                    snapshot.get("url", ""),
                    _compact_text(str(snapshot.get("text", "") or ""))[:280],
                )
                return False
            output_path = document_type.handler.process_snapshot(
                spec=document_type,
                processo=processo,
                protocolo_documento=protocolo_documento,
                snapshot=snapshot,
                collection_context=collection_context,
                analysis=analysis,
                output_dir=self._resolve_preview_output_dir(),
                logger=self.logger,
                settings=self.settings,
            )
            if output_path:
                self.logger.info(
                    "Processo %s: JSON do documento %s salvo em %s",
                    processo,
                    document_type.log_label,
                    output_path,
                )
            if document_type.key in {"act", "memorando", "ted"}:
                is_canonical = bool((analysis or {}).get("validation_status") == VALIDATION_STATUS_VALID)
                if not is_canonical:
                    self.logger.warning(
                        "Processo %s: snapshot de %s retido apenas na silver. doc_class=%s motivo=%s",
                        processo,
                        document_type.log_label,
                        (analysis or {}).get("doc_class", ""),
                        (analysis or {}).get("classification_reason", validation_reason),
                    )
                return is_canonical
            if document_type.key == "pt":
                is_canonical = bool((analysis or {}).get("is_canonical_candidate", True))
                if not is_canonical:
                    self.logger.warning(
                        "Processo %s: snapshot de %s retido apenas na silver. doc_class=%s motivo=%s",
                        processo,
                        document_type.log_label,
                        (analysis or {}).get("doc_class", ""),
                        (analysis or {}).get("classification_reason", validation_reason),
                    )
                return is_canonical
            return True
        except Exception as exc:
            failure_context = dict(collection_context or {})
            failure_context["captured_at"] = datetime.now().isoformat(timespec="seconds")
            failure_context["extraction_error"] = str(exc)
            self._record_document_extraction_failure(
                processo=processo,
                protocolo_documento=protocolo_documento,
                document_type=document_type,
                collection_context=failure_context,
            )
            self.logger.warning(
                "Processo %s: falha resiliente na extracao do documento aberto (%s).",
                processo,
                exc,
            )
            return False

    def _extract_and_save_plano_trabalho_snapshot(
        self,
        processo: str,
        protocolo_documento: str,
    ) -> None:
        document_type = self._get_document_type("pt")
        if document_type is None:
            return
        self._extract_and_process_document_snapshot(
            processo=processo,
            protocolo_documento=protocolo_documento,
            document_type=document_type,
        )

    def _save_preview_records_csv(self, records: List[Dict[str, str]]) -> Optional[Path]:
        if not records:
            return None

        output_dir = self._resolve_preview_output_dir()
        csv_writer.ensure_output_dir(output_dir)
        csv_path = output_dir / "parcerias_vigentes_latest.csv"
        ordered_columns = [
            "interno_descricao",
            "seq",
            "processo",
            "parceiro",
            "vigencia",
            "numero_act",
            "objeto",
        ]
        sanitized_records: List[Dict[str, str]] = []
        for record in records:
            sanitized_records.append(
                {col: self._clean_text_value(str(record.get(col, "") or "")) for col in ordered_columns}
            )
        csv_writer.write_csv(sanitized_records, csv_path, columns=ordered_columns)
        return csv_path

    def _collect_preview_if_parcerias_vigencias(self) -> None:
        try:
            descricao_atual = self._get_current_interno_descricao_value()
            if not descricao_atual:
                self.logger.warning(
                    "Coleta preview pulada: nao foi possivel ler txtDescricao (descricao atual do interno)."
                )
                return

            if not self._should_collect_preview_for_current_descricao(descricao_atual):
                self.logger.info(
                    "Coleta PARCERIAS VIGENTES pulada: interno atual '%s' nao e 'PARCERIAS VIGENTES'.",
                    descricao_atual,
                )
                return

            self.logger.info("Entrou no interno '%s'. Iniciando coleta direcional de PARCERIAS VIGENTES.", descricao_atual)
            records = self._collect_preview_records_from_current_list(descricao_atual)
            csv_path = self._save_preview_records_csv(records)
            self.logger.info("Coleta PARCERIAS VIGENTES: %d registro(s) coletado(s).", len(records))
            if csv_path:
                self.logger.info("CSV PARCERIAS VIGENTES gerado em: %s", csv_path)
            else:
                self.logger.info("CSV PARCERIAS VIGENTES nao gerado: nenhuma linha util foi extraida.")
        except Exception as exc:
            self.logger.exception("Falha na coleta preview direcional: %s", exc)

    # Processos (listagem / abertura)
    def _list_processos(self) -> List[str]:
        sel = self.selectors.get("interno", {})
        x = sel.get("processo")
        if not x:
            raise RuntimeError("Seletor interno.processo ausente em xpath_selector.json")

        elems = self.wait_for_elements(x, tag="list_processos")
        out: List[str] = []
        for elem in elems:
            text = elem.text.strip() if elem.text else ""
            if text:
                out.append(text)
        return out

    def _open_processo(self, processo_text: str) -> None:
        self._switch_to_main_window_context()
        setattr(self.driver, "_sei_timeout_seconds", self.timeout_seconds)
        process_navigation.open_processo(self.driver, processo_text, self.selectors, self.logger)

    # Navegacao de abas / retorno
    def _close_current_tab_and_back(self) -> None:
        returned_handle = process_navigation.close_current_tab_and_back(
            self.driver,
            self.logger,
            preferred_handle=self.main_window_handle,
        )
        if returned_handle:
            self.main_window_handle = returned_handle

    def _remember_main_window_handle(self, context: str) -> None:
        try:
            self.main_window_handle = self.driver.current_window_handle
            self.logger.info(
                "Janela principal registrada (%s): handle=%s total_handles=%d",
                context,
                self.main_window_handle,
                len(self.driver.window_handles),
            )
        except WebDriverException as exc:
            self.logger.warning(
                "Falha ao registrar janela principal (%s): %s",
                context,
                exc,
            )

    def _switch_to_main_window_context(self) -> None:
        if not self.main_window_handle:
            self._remember_main_window_handle(context="auto_descoberta")
            return

        try:
            handles = list(self.driver.window_handles)
            if self.main_window_handle in handles:
                self.driver.switch_to.window(self.main_window_handle)
                return

            if handles:
                self.main_window_handle = handles[0]
                self.driver.switch_to.window(self.main_window_handle)
                self.logger.warning(
                    "Janela principal anterior indisponivel; novo handle principal=%s",
                    self.main_window_handle,
                )
        except WebDriverException as exc:
            self.logger.warning("Falha ao alternar para janela principal: %s", exc)
