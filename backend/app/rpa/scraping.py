from __future__ import annotations

import os
import json
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from app.config import get_settings
from app.core.driver_factory import create_chrome_driver
from app.core.logging_config import setup_logger
from app.core.raw_date_field_collector import collect_raw_fields, export_raw_fields_csv
from app.output import csv_writer
from app.rpa.sei import process_navigation
from app.rpa.sei import toolbar_actions
from app.rpa.sei import document_search
from app.rpa.sei import document_text_extractor
from app.services.pt_normalizer import export_normalized_csv
from app.rpa.selenium_utils import (
    get_iframes_info,
    wait_for_clickable as selenium_wait_for_clickable,
    wait_for_document_ready as selenium_wait_for_document_ready,
    wait_for_elements as selenium_wait_for_elements,
)
from app.rpa.selectors import load_xpath_selectors


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


class SEIScraper:

    # Setup / lifecycle
    def __init__(self) -> None:
        self.logger = setup_logger()

        cfg = get_settings()
        self.settings = cfg
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
        self._pt_tracking_records: List[Dict[str, Any]] = []

    def _prepare_output_dir_for_run(self) -> None:
        output_dir = self._resolve_preview_output_dir()
        csv_writer.ensure_output_dir(output_dir)

        cleanup_patterns = [
            "plano_trabalho_*.json",
            "pt_fields_raw.csv",
            "pt_status_execucao_latest.csv",
            "pt_sem_prazo_latest.csv",
            "pt_normalizado_latest.csv",
            "pt_normalizado_completo_latest.csv",
            "parcerias_vigentes_latest.csv",
        ]
        removed = 0
        for pattern in cleanup_patterns:
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

        self._prepare_output_dir_for_run()

        self.logger.info("Abrindo SEI em: %s", self.base_url)
        self.driver.get(self.base_url)

        if manual_login:
            self._wait_for_manual_login()
        else:
            self._login_if_possible()
        self._remember_main_window_handle(context="pos_login")

        self._close_popup_if_exists()
        self._open_interno_menu()
        selecionados = self._select_guided_internos_by_descricao()
        if not selecionados:
            self.logger.warning(
                "Modo guiado: nenhum interno selecionado pelas descricoes configuradas."
            )
            self._save_pt_tracking_reports()
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
                self._switch_to_main_window_context()
                self.logger.info("Abrindo processo %s", proc)
                self._open_processo(proc)
                self.logger.info("Processo %s: aguardando pagina pronta", proc)
                self._wait_page_ready_in_processo()
                self.logger.info("Processo %s: clicando Abrir todas as Pastas", proc)
                self._click_abrir_todas_as_pastas()
                self.logger.info("Processo %s: clicando Pesquisar no Processo", proc)
                self._click_pesquisar_no_processo()
                self.logger.info("Processo %s: filtro aberto (anchor ok)", proc)
                self._buscar_e_abrir_plano_de_trabalho_mais_recente(proc)

                if stop_at_filter:
                    self.logger.info("Processo %s: fechando aba e voltando", proc)
                    self._close_current_tab_and_back()
                else:
                    self.logger.info(
                        "Processo %s: mantendo aba aberta no filtro (--no-stop-at-filter); interrompendo loop.",
                        proc,
                    )
                    self._save_pt_tracking_reports()
                    result = sorted(self.found)
                    self.logger.info("Itens unicos encontrados: %d", len(result))
                    print(result)
                    return result

            self._back_to_interno_list()

        self._save_pt_tracking_reports()
        result = sorted(self.found)
        self.logger.info("Itens unicos encontrados: %d", len(result))
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
        self.logger.info("Aguardando conclusao do login/autenticacao no SEI (modo automatico).")

        wait_seconds = max(5, cfg.manual_login_wait_seconds)
        self._wait_for_post_login_ready(wait_seconds)

    def _wait_for_post_login_ready(self, wait_seconds: int) -> None:
        sel = self.selectors.get("tela_inicio", {})
        x_bloco = sel.get("bloco")

        self.logger.info("Validando se a tela principal do SEI ficou pronta.")
        deadline = time.time() + wait_seconds
        gateway_timeout_retry_limit = 2
        gateway_timeout_hits = 0
        login_url_markers = ("/sip/login.php", "sigla_sistema=sei")
        post_login_url_markers = (
            "/sei/controlador.php",
            "acao=procedimento_controlar",
            "acao_origem=principal",
        )

        while time.time() < deadline:
            current_url = (self.driver.current_url or "").lower()

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
                if gateway_timeout_hits <= gateway_timeout_retry_limit:
                    self.logger.warning(
                        "Tela 504 detectada apos login (%d/%d). "
                        "Tentando recarregar a pagina.",
                        gateway_timeout_hits,
                        gateway_timeout_retry_limit,
                    )
                    try:
                        self.driver.refresh()
                    except WebDriverException as exc:
                        self.logger.warning("Falha ao recarregar pagina apos 504: %s", exc)
                    time.sleep(3)
                    continue

                raise RuntimeError(
                    "Tela de erro 504 detectada apos login em tentativas consecutivas. "
                    "Interrompendo execucao para evitar loop."
                )

            time.sleep(1)

        raise RuntimeError(
            "Login/autenticacao nao confirmado dentro do tempo limite. "
            "Confirme se o processo foi concluido no navegador."
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
            time.sleep(1)
            self.driver.find_element(By.XPATH, x).click()
            self.logger.info("Pop-up fechado.")
        except Exception:
            return

    # Helpers de seletores / clique
    def _click_first_clickable(self, xpaths: List[str], label: str) -> None:
        checked: List[str] = []
        candidate_timeout = max(3, min(8, self.timeout_seconds))
        for idx, xpath in enumerate(xpaths, start=1):
            if not xpath or xpath in checked:
                continue
            checked.append(xpath)
            try:
                self.wait_for_clickable(
                    xpath,
                    tag=f"{label}_candidate_{idx}",
                    timeout=candidate_timeout,
                ).click()
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

    def _find_plano_trabalho_link_in_tree(self) -> Optional[Any]:
        xpaths = self.selectors.get_many("processo.documentos_do_processo_links")
        preferred_terms = (
            "PLANO DE TRABALHO - PT",
            "PLANO DE TRABALHO PT",
            "PLANO DE TRABALHO",
        )

        try:
            self.driver.switch_to.default_content()
            iframe = self.driver.find_element(By.XPATH, "//iframe[@id='ifrArvore' or @name='ifrArvore']")
            self.driver.switch_to.frame(iframe)
        except WebDriverException as exc:
            self.logger.info("Fallback arvore PT: nao foi possivel entrar no ifrArvore (%s).", exc)
            return None

        candidates: List[Tuple[int, Any, str]] = []
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
                if not normalized:
                    continue
                score = -1
                for idx, term in enumerate(preferred_terms):
                    if term in normalized:
                        score = len(preferred_terms) - idx
                        break
                if score <= 0:
                    continue
                candidates.append((score, elem, raw_text))
            if candidates:
                break

        if not candidates:
            self.logger.info("Fallback arvore PT: nenhum documento com 'PLANO DE TRABALHO' encontrado na arvore.")
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        chosen = candidates[0]
        self.logger.info(
            "Fallback arvore PT: candidato selecionado texto='%s' score=%d.",
            chosen[2],
            chosen[0],
        )
        return chosen[1]

    def _abrir_plano_trabalho_pela_arvore(self, processo: str) -> bool:
        link = self._find_plano_trabalho_link_in_tree()
        if link is None:
            return False

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
                self.logger.warning("Processo %s: falha ao clicar no PT pela arvore (%s).", processo, exc)
                return False

        try:
            self.driver.switch_to.default_content()
        except WebDriverException:
            pass

        time.sleep(1.2)
        self.logger.info("Processo %s: documento aberto via arvore do processo.", processo)
        self._extract_and_save_plano_trabalho_snapshot(
            processo=processo,
            protocolo_documento=processo,
        )
        return True

    def _buscar_e_abrir_plano_de_trabalho_mais_recente(self, processo: str) -> None:
        termo = "PLANO DE TRABALHO - PT"
        handles_before_click: Set[str] = set()
        processo_handle = ""
        opened_doc_handles: Set[str] = set()
        try:
            self.logger.info(
                "Processo %s: iniciando busca do documento '%s'. contexto_pre_busca url=%s title=%s handles=%d",
                processo,
                termo,
                self.driver.current_url,
                self.driver.title,
                len(self.driver.window_handles),
            )
            processo_handle = self.driver.current_window_handle
            hit = self.buscar_documento_mais_recente_no_filtro(
                termo=termo,
                timeout_seconds=self.timeout_seconds,
            )
            if hit is None:
                self.logger.info("Processo %s: %s nao encontrado no filtro; tentando fallback pela arvore.", processo, termo)
                if self._abrir_plano_trabalho_pela_arvore(processo):
                    return
                self.logger.info("Processo %s: nenhum PT localizado nem no filtro nem na arvore; seguindo.", processo)
                return
            self.logger.info(
                "Processo %s: documento encontrado para '%s' (%s).",
                processo,
                termo,
                hit.protocolo,
            )
            handles_before_click = set(self.driver.window_handles)
            self.abrir_documento_mais_recente_no_filtro(timeout_seconds=self.timeout_seconds)
            handles_after_click = set(self.driver.window_handles)
            opened_doc_handles = handles_after_click - handles_before_click
            self.logger.info(
                "Processo %s: pos_click_resultado handles_antes=%d handles_depois=%d novos_handles=%s",
                processo,
                len(handles_before_click),
                len(handles_after_click),
                list(opened_doc_handles),
            )
            switched_handle = self._switch_to_newly_opened_window(
                handles_before=handles_before_click,
                reason=f"PT {processo}",
            )
            if switched_handle:
                opened_doc_handles.add(switched_handle)
            self.logger.info(
                "Processo %s: clique no primeiro resultado efetuado. contexto_pos_click url=%s title=%s handles=%d",
                processo,
                self.driver.current_url,
                self.driver.title,
                len(self.driver.window_handles),
            )
            self.logger.info(
                "Processo %s: documento mais recente de '%s' aberto (%s).",
                processo,
                termo,
                hit.protocolo,
            )
            self._extract_and_save_plano_trabalho_snapshot(
                processo=processo,
                protocolo_documento=hit.protocolo,
            )
        except (TimeoutException, NoSuchElementException) as exc:
            self.logger.warning(
                "Processo %s: falha resiliente ao buscar/abrir '%s' (%s); seguindo.",
                processo,
                termo,
                exc,
            )
        finally:
            self._close_opened_doc_tabs(
                processo=processo,
                opened_doc_handles=opened_doc_handles,
                preferred_return_handle=processo_handle,
            )

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

    def abrir_documento_mais_recente_no_filtro(self, timeout_seconds: int = 20) -> None:
        """Na tela de resultados, abre o documento mais recente (primeiro resultado)."""
        self.logger.info(
            "Abertura filtro: tentando abrir primeiro resultado timeout=%ss url=%s title=%s",
            timeout_seconds,
            self.driver.current_url,
            self.driver.title,
        )
        document_search.abrir_documento_mais_recente(
            driver=self.driver,
            selectors=self.selectors,
            logger=self.logger,
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
                    time.sleep(0.8)
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

    def _sanitize_filename_part(self, value: str, fallback: str = "sem_id") -> str:
        cleaned = re.sub(r"[^\w.-]+", "_", (value or "").strip())
        cleaned = cleaned.strip("_")
        if not cleaned:
            return fallback
        return cleaned[:80]

    def _save_document_snapshot_json(
        self,
        processo: str,
        protocolo_documento: str,
        snapshot: Dict[str, Any],
        prazos: Dict[str, str],
    ) -> Optional[Path]:
        output_dir = self._resolve_preview_output_dir()
        csv_writer.ensure_output_dir(output_dir)

        processo_id = self._sanitize_filename_part(processo, fallback="sem_processo")
        filename = f"plano_trabalho_{processo_id}.json"
        filepath = output_dir / filename

        payload: Dict[str, Any] = {
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "processo": processo,
            "documento": protocolo_documento,
            "snapshot": snapshot,
            "prazos": prazos,
        }

        try:
            filepath.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return filepath
        except Exception as exc:
            self.logger.warning(
                "Processo %s: falha ao salvar snapshot do documento (%s).",
                processo,
                exc,
            )
            return None

    def _register_pt_tracking_record(
        self,
        processo: str,
        protocolo_documento: str,
        snapshot: Dict[str, Any],
        prazos: Dict[str, str],
        output_path: Optional[Path],
    ) -> None:
        inicio_found = bool(prazos.get("inicio_data") or prazos.get("inicio_raw"))
        termino_found = bool(prazos.get("termino_data") or prazos.get("termino_raw"))
        sem_prazo = not (inicio_found and termino_found)
        record: Dict[str, Any] = {
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "processo": processo,
            "documento": protocolo_documento,
            "snapshot_mode": (snapshot.get("extraction_mode", "") or ""),
            "text_chars": len(snapshot.get("text", "") or ""),
            "tables_count": len(snapshot.get("tables", []) or []),
            "prazos_status": (prazos.get("status", "") or ""),
            "inicio_data": (prazos.get("inicio_data", "") or ""),
            "inicio_raw": (prazos.get("inicio_raw", "") or ""),
            "termino_data": (prazos.get("termino_data", "") or ""),
            "termino_raw": (prazos.get("termino_raw", "") or ""),
            "tem_inicio": inicio_found,
            "tem_termino": termino_found,
            "sem_prazo": sem_prazo,
            "json_path": str(output_path) if output_path else "",
        }
        self._pt_tracking_records.append(record)

    def _save_pt_tracking_reports(self) -> None:
        if not self._pt_tracking_records:
            return

        output_dir = self._resolve_preview_output_dir()
        csv_writer.ensure_output_dir(output_dir)

        all_columns = [
            "captured_at",
            "processo",
            "documento",
            "snapshot_mode",
            "text_chars",
            "tables_count",
            "prazos_status",
            "inicio_data",
            "inicio_raw",
            "termino_data",
            "termino_raw",
            "tem_inicio",
            "tem_termino",
            "sem_prazo",
            "json_path",
        ]
        all_path = output_dir / "pt_status_execucao_latest.csv"
        csv_writer.write_csv(self._pt_tracking_records, all_path, columns=all_columns)

        sem_records = [r for r in self._pt_tracking_records if bool(r.get("sem_prazo"))]
        sem_path = output_dir / "pt_sem_prazo_latest.csv"
        csv_writer.write_csv(sem_records, sem_path, columns=all_columns)

        self.logger.info(
            "Relatorio PT gerado: total=%d sem_prazo=%d arquivo=%s",
            len(self._pt_tracking_records),
            len(sem_records),
            sem_path,
        )
        try:
            export_result = export_normalized_csv(output_dir, logger=self.logger)
            if export_result.get("latest_path"):
                self.logger.info(
                    "Relatorio PT normalizado gerado: registros=%d latest=%s",
                    int(export_result.get("records", 0) or 0),
                    export_result["latest_path"],
                )
        except Exception as exc:
            self.logger.warning("Falha ao gerar CSV PT normalizado (%s).", exc)

    def _extract_and_save_plano_trabalho_snapshot(
        self,
        processo: str,
        protocolo_documento: str,
    ) -> None:
        try:
            iframe_info = get_iframes_info(self.driver)
            self.logger.info(
                "Processo %s: iniciando snapshot PT. contexto_pre_snapshot url=%s title=%s iframes=%d",
                processo,
                self.driver.current_url,
                self.driver.title,
                len(iframe_info),
            )
            if iframe_info:
                self.logger.info("Processo %s: iframes pre_snapshot=%s", processo, iframe_info)
            snapshot = document_text_extractor.extract_document_snapshot(
                self.driver,
                logger=self.logger,
            )
            prazos = document_text_extractor.parse_prazos(snapshot.get("text", "") or "", logger=self.logger)
            tables = snapshot.get("tables", [])
            text = snapshot.get("text", "") or ""

            inicio_found = bool(prazos.get("inicio_data") or prazos.get("inicio_raw"))
            termino_found = bool(prazos.get("termino_data") or prazos.get("termino_raw"))
            self.logger.info(
                "Processo %s: snapshot extraido (texto_chars=%d, tabelas=%d, inicio=%s, termino=%s).",
                processo,
                len(text),
                len(tables) if isinstance(tables, list) else 0,
                "sim" if inicio_found else "nao",
                "sim" if termino_found else "nao",
            )

            if self.settings.export_raw_fields_csv:
                try:
                    output_dir = self._resolve_preview_output_dir()
                    csv_writer.ensure_output_dir(output_dir)
                    raw_fields = collect_raw_fields(text, tables if isinstance(tables, list) else [])
                    raw_csv_path = output_dir / "pt_fields_raw.csv"
                    export_raw_fields_csv(
                        out_csv_path=str(raw_csv_path),
                        processo_sei=processo,
                        doc_title=(snapshot.get("title", "") or ""),
                        doc_url=(snapshot.get("url", "") or ""),
                        raw_fields=raw_fields,
                        captured_at=datetime.now().isoformat(timespec="seconds"),
                    )
                    self.logger.info(
                        "Processo %s: CSV raw atualizado em %s (+%d linha(s)).",
                        processo,
                        raw_csv_path,
                        len(raw_fields),
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Processo %s: falha ao exportar campos raw para CSV (%s).",
                        processo,
                        exc,
                    )

            output_path = self._save_document_snapshot_json(
                processo=processo,
                protocolo_documento=protocolo_documento,
                snapshot=snapshot,
                prazos=prazos,
            )
            self._register_pt_tracking_record(
                processo=processo,
                protocolo_documento=protocolo_documento,
                snapshot=snapshot,
                prazos=prazos,
                output_path=output_path,
            )
            if output_path:
                self.logger.info("Processo %s: JSON do plano de trabalho salvo em %s", processo, output_path)
        except Exception as exc:
            self.logger.warning(
                "Processo %s: falha resiliente na extracao do documento aberto (%s).",
                processo,
                exc,
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

    def _back_to_interno_list(self) -> None:
        try:
            self.driver.back()
            time.sleep(1)
        except Exception:
            return
