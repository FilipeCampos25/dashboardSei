from __future__ import annotations

import os
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Set

from selenium.common.exceptions import (
    NoSuchFrameException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from app.config import get_settings
from app.core.driver_factory import create_chrome_driver
from app.core.logging_config import setup_logger
from app.services.selectors import load_selectors


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
    XPATH_PASTAS_FECHADAS = "//img[starts-with(@id,'joinPASTA') and contains(@title,'Abrir')]"

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
        self.selectors = load_selectors()
        self.found: Set[str] = set()
        self.descricoes_busca = self._parse_descricoes_busca(cfg.descricoes_busca)
        self.descricao_match_mode = (cfg.descricoes_match_mode or "contains").strip().lower()
        if self.descricao_match_mode not in {"contains", "exact"}:
            self.logger.warning(
                "DESCRICOES_MATCH_MODE invalido (%s). Usando 'contains'.",
                self.descricao_match_mode,
            )
            self.descricao_match_mode = "contains"

    def run_full_flow(
        self,
        manual_login: bool = True,
        max_internos: int = 3,
        max_processos_por_interno: int = 5,
    ) -> List[str]:
        if not self.base_url:
            raise RuntimeError("Config ausente: sei_url / URL / SEI_URL")

        self.logger.info("Abrindo SEI em: %s", self.base_url)
        self.driver.get(self.base_url)

        if manual_login:
            self._wait_for_manual_login()
        else:
            self._login_if_possible()

        self._close_popup_if_exists()
        self._open_interno_menu()
        selecionado = self._open_guided_interno_by_descricao()
        if not selecionado:
            self.logger.warning(
                "Modo guiado: nenhum interno selecionado pelas descricoes configuradas."
            )
            result = sorted(self.found)
            self.logger.info("Itens unicos encontrados: %d", len(result))
            print(result)
            return result

        processos = self._list_processos()[:max_processos_por_interno]
        for proc in processos:
            self._open_processo(proc)
            self._collect_documentos()
            self._close_current_tab_and_back()

        self._back_to_interno_list()

        result = sorted(self.found)
        self.logger.info("Itens unicos encontrados: %d", len(result))
        print(result)
        return result

    def _get_iframes_info(self) -> List[Dict[str, Any]]:
        frames: List[Dict[str, Any]] = []
        try:
            elems = self.driver.find_elements(By.TAG_NAME, "iframe")
        except WebDriverException:
            return frames

        for idx, frame in enumerate(elems):
            try:
                frames.append(
                    {
                        "index": idx,
                        "id": frame.get_attribute("id"),
                        "name": frame.get_attribute("name"),
                        "src": frame.get_attribute("src"),
                    }
                )
            except WebDriverException:
                frames.append(
                    {
                        "index": idx,
                        "id": None,
                        "name": None,
                        "src": None,
                    }
                )
        return frames

    def _get_ready_state(self) -> str:
        try:
            value = self.driver.execute_script("return document.readyState")
        except WebDriverException:
            return "unavailable"
        return str(value) if value else "unknown"

    def _wait_for_document_ready(self, timeout: int, tag: str) -> None:
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException as exc:
            self.logger.error("Timeout aguardando readyState=complete (%s)", tag)
            raise exc

    def _log_iframe_hint(self, context: str) -> List[Dict[str, Any]]:
        frames = self._get_iframes_info()
        if frames:
            self.logger.error(
                "%s: encontrados %d iframe(s). O XPath pode estar em outro contexto.",
                context,
                len(frames),
            )
        return frames

    def wait_for_elements(
        self,
        xpath: str,
        tag: str,
        timeout: int | None = None,
        restore_context: bool = True,
    ) -> List[Any]:
        effective_timeout = timeout or self.timeout_seconds
        self._wait_for_document_ready(effective_timeout, tag)
        deadline = time.time() + effective_timeout
        iframe_count_logged = False

        try:
            while time.time() < deadline:
                self.driver.switch_to.default_content()
                elems = self.driver.find_elements(By.XPATH, xpath)
                if elems:
                    return elems

                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                if iframes and not iframe_count_logged:
                    self.logger.info(
                        "wait_for_elements(%s): fallback em %d iframe(s)",
                        tag,
                        len(iframes),
                    )
                    iframe_count_logged = True

                for idx in range(len(iframes)):
                    if time.time() >= deadline:
                        break

                    try:
                        self.driver.switch_to.default_content()
                        current_iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                        if idx >= len(current_iframes):
                            continue

                        frame = current_iframes[idx]
                        frame_id = frame.get_attribute("id")
                        frame_name = frame.get_attribute("name")
                        frame_src = frame.get_attribute("src")

                        self.logger.debug(
                            "wait_for_elements(%s): tentando iframe[%d] id=%s name=%s src=%s",
                            tag,
                            idx,
                            frame_id,
                            frame_name,
                            frame_src,
                        )

                        self.driver.switch_to.frame(frame)
                        elems = self.driver.find_elements(By.XPATH, xpath)
                        if elems:
                            return elems
                    except (StaleElementReferenceException, NoSuchFrameException, WebDriverException) as frame_exc:
                        self.logger.debug(
                            "wait_for_elements(%s): iframe[%d] indisponivel/stale (%s)",
                            tag,
                            idx,
                            frame_exc,
                        )
                        continue

                time.sleep(min(0.5, max(0.0, deadline - time.time())))

            frames = self._log_iframe_hint(f"wait_for_elements falhou ({tag})")
            self.logger.error(
                "Timeout aguardando elementos: tag=%s xpath=%s timeout=%ss",
                tag,
                xpath,
                effective_timeout,
            )
            self.logger.error("Contexto: iframe_count=%d url=%s", len(frames), self.driver.current_url)
            raise TimeoutException(
                f"Timeout aguardando elementos: tag={tag} xpath={xpath} timeout={effective_timeout}s"
            )
        finally:
            if restore_context:
                try:
                    self.driver.switch_to.default_content()
                except WebDriverException:
                    pass

    def wait_for_clickable(self, xpath: str, tag: str, timeout: int | None = None) -> Any:
        effective_timeout = timeout or self.timeout_seconds
        self._wait_for_document_ready(effective_timeout, tag)
        try:
            return WebDriverWait(self.driver, effective_timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
        except TimeoutException as exc:
            frames = self._log_iframe_hint(f"wait_for_clickable falhou ({tag})")
            self.logger.error(
                "Timeout aguardando clique: tag=%s xpath=%s timeout=%ss",
                tag,
                xpath,
                effective_timeout,
            )
            self.logger.error("Contexto: iframe_count=%d url=%s", len(frames), self.driver.current_url)
            raise exc

    def _wait_for_manual_login(self) -> None:
        cfg = self.settings
        self.logger.info("Aguardando login manual no SEI.")

        if sys.stdin and sys.stdin.isatty():
            try:
                input("Quando terminar o login manual, pressione ENTER para continuar...")
            except EOFError:
                self.logger.warning("STDIN sem entrada disponivel; aplicando espera controlada.")
        else:
            self.logger.warning("Entrada nao interativa detectada; aplicando espera controlada.")

        wait_seconds = max(5, cfg.manual_login_wait_seconds)
        self._wait_for_post_login_ready(wait_seconds)

    def _wait_for_post_login_ready(self, wait_seconds: int) -> None:
        sel = self.selectors.get("tela_inicio", {})
        x_bloco = sel.get("bloco")

        if not x_bloco:
            self.logger.warning("Seletor tela_inicio.bloco ausente; seguindo sem validacao de login.")
            return

        self.logger.info("Validando se a tela principal do SEI ficou pronta.")
        deadline = time.time() + wait_seconds

        while time.time() < deadline:
            if self.driver.find_elements(By.XPATH, x_bloco):
                self.logger.info("Login manual confirmado: menu principal encontrado.")
                return

            if self._is_gateway_timeout_page():
                raise RuntimeError(
                    "Tela de erro 504 detectada apos login. "
                    "Interrompendo execucao para evitar loop."
                )

            time.sleep(1)

        raise RuntimeError(
            "Login manual nao confirmado dentro do tempo limite. "
            "Confirme se o login terminou no navegador antes de pressionar ENTER."
        )

    def _is_gateway_timeout_page(self) -> bool:
        try:
            title = (self.driver.title or "").lower()
            url = (self.driver.current_url or "").lower()
            html = (self.driver.page_source or "")[:4000].lower()
        except WebDriverException:
            return False

        combined = f"{title} {url} {html}"
        return any(
            marker in combined
            for marker in ("gateway timeout", "erro 504", "http error 504")
        )

    def _safe_refresh(self) -> None:
        try:
            self.driver.refresh()
        except WebDriverException as exc:
            self.logger.debug("Falha ao atualizar pagina durante espera de login: %s", exc)

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
        if self.descricao_match_mode == "exact":
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

    def _open_guided_interno_by_descricao(self) -> InternoRow | None:
        if not self.descricoes_busca:
            self.logger.warning(
                "DESCRICOES_BUSCA vazio. Defina no .env (ex.: DESCRICOES_BUSCA=\"A|B|C\")."
            )
            return None

        list_url = self.driver.current_url
        internos = self._collect_interno_rows_with_pagination(max_pages=10)
        if not internos:
            self.logger.warning("Nenhum numero interno elegivel foi encontrado na tabela.")
            return None

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
            return None

        selected: InternoRow | None = None
        selected_target = ""
        for target in self.descricoes_busca:
            if matches_by_target[target]:
                selected = matches_by_target[target][0]
                selected_target = target
                break

        if not selected:
            self.logger.warning("Houve match contabilizado, mas nenhum item selecionavel.")
            return None

        if self._click_selected_interno(selected, selected_target, list_url):
            return selected
        return None

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

    def _list_internos(self) -> List[str]:
        sel = self.selectors.get("interno", {})
        x = sel.get("numero_interno")
        if not x:
            raise RuntimeError("Seletor interno.numero_interno ausente em xpath_selector.json")

        elems = self.wait_for_elements(x, tag="list_internos")
        return [e.text.strip() for e in elems if e.text and e.text.strip()]

    def _open_interno(self, interno_text: str) -> None:
        sel = self.selectors.get("interno", {})
        x = sel.get("numero_interno")
        if not x:
            raise RuntimeError("Seletor interno.numero_interno ausente em xpath_selector.json")

        elems = self.wait_for_elements(x, tag="open_interno_list")
        for elem in elems:
            if elem.text.strip() == interno_text:
                elem.click()
                return

        frames = self._log_iframe_hint("Nao consegui encontrar o interno na lista")
        self.logger.error("Contexto: iframe_count=%d url=%s", len(frames), self.driver.current_url)
        raise RuntimeError(f"Nao consegui abrir o interno: {interno_text}")

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
        sel = self.selectors.get("interno", {})
        x = sel.get("processo")
        if not x:
            raise RuntimeError("Seletor interno.processo ausente em xpath_selector.json")

        elems = self.wait_for_elements(x, tag="open_processo_list")
        for elem in elems:
            if (elem.text or "").strip() == processo_text:
                handles_before = set(self.driver.window_handles)
                elem.click()

                try:
                    WebDriverWait(self.driver, self.timeout_seconds).until(
                        lambda d: len(set(d.window_handles) - handles_before) > 0
                    )
                except TimeoutException as exc:
                    frames = self._log_iframe_hint("Timeout aguardando nova janela do processo")
                    self.logger.error("Contexto: iframe_count=%d url=%s", len(frames), self.driver.current_url)
                    raise RuntimeError(
                        f"Nao abriu nova janela para o processo: {processo_text}"
                    ) from exc

                handles_after = set(self.driver.window_handles)
                new_handles = list(handles_after - handles_before)
                if not new_handles:
                    raise RuntimeError(
                        f"Nao identifiquei o handle da nova janela do processo: {processo_text}"
                    )

                new_handle = new_handles[0]
                self.driver.switch_to.window(new_handle)
                self.logger.info(
                    "Nova janela do processo aberta. handle=%s url=%s title=%s",
                    new_handle,
                    self.driver.current_url,
                    self.driver.title,
                )
                return

        frames = self._log_iframe_hint("Nao consegui localizar o processo para abrir")
        self.logger.error("Contexto: iframe_count=%d url=%s", len(frames), self.driver.current_url)
        raise RuntimeError(f"Nao consegui abrir o processo: {processo_text}")

    def _collect_documentos(self) -> None:
        sel = self.selectors.get("interno", {})
        x_docs = sel.get("documentos_do_processo")
        if not x_docs:
            self.logger.info("Seletor de documentos nao encontrado; pulando coleta.")
            return

        try:
            self._expand_all_pastas(max_cycles=15)
            elems = self.wait_for_elements(
                x_docs,
                tag="collect_documentos",
                restore_context=False,
            )
            for elem in elems:
                text = (elem.text or "").strip()
                if text:
                    if text not in self.found:
                        self.logger.info("[ACHEI] %s", text)
                    self.found.add(text)
        except Exception as exc:
            self.logger.exception("Falha ao coletar documentos: %s", exc)
        finally:
            try:
                self.driver.switch_to.default_content()
            except WebDriverException:
                pass

    def _switch_to_ifr_arvore(self) -> bool:
        try:
            self.driver.switch_to.default_content()
            iframe = self.driver.find_element(By.ID, "ifrArvore")
            self.driver.switch_to.frame(iframe)
            return True
        except WebDriverException:
            pass

        try:
            self.driver.switch_to.default_content()
            iframe = self.driver.find_element(By.NAME, "ifrArvore")
            self.driver.switch_to.frame(iframe)
            return True
        except WebDriverException:
            self.logger.debug("Nao foi possivel acessar iframe ifrArvore por id/name.")
            return False

    def _expand_all_pastas(self, max_cycles: int = 15) -> None:
        total_clicked = 0
        previous_doc_count = -1

        for cycle in range(1, max_cycles + 1):
            if not self._switch_to_ifr_arvore():
                self.logger.debug("Expandir pastas: ifrArvore nao encontrado; seguindo sem expansao.")
                return

            try:
                doc_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@class,'infraArvoreNo')]/span"))
            except WebDriverException:
                doc_count = -1

            try:
                closed = self.driver.find_elements(By.XPATH, self.XPATH_PASTAS_FECHADAS)
            except WebDriverException as exc:
                self.logger.debug("Expandir pastas: falha ao buscar pastas fechadas (%s)", exc)
                break

            if not closed:
                self.logger.info(
                    "Expandir pastas: nenhuma pasta fechada encontrada (ciclo=%d, total_cliques=%d).",
                    cycle,
                    total_clicked,
                )
                break

            self.logger.info("Expandir pastas: pastas fechadas encontradas no ciclo %d: %d", cycle, len(closed))
            clicked_this_cycle = 0

            for idx in range(len(closed)):
                if not self._switch_to_ifr_arvore():
                    break

                try:
                    current_closed = self.driver.find_elements(By.XPATH, self.XPATH_PASTAS_FECHADAS)
                    if idx >= len(current_closed):
                        continue

                    icon = current_closed[idx]
                    pasta_id = icon.get_attribute("id") or f"idx_{idx}"
                    self.logger.info("Abrindo pasta %s", pasta_id)
                    self.driver.execute_script("arguments[0].click();", icon)
                    clicked_this_cycle += 1
                    total_clicked += 1
                    time.sleep(0.2)
                except (StaleElementReferenceException, WebDriverException) as exc:
                    self.logger.debug("Expandir pastas: falha ao clicar pasta idx=%d (%s)", idx, exc)
                    continue

            if clicked_this_cycle == 0:
                self.logger.info("Expandir pastas: nenhum clique efetivo no ciclo %d; encerrando.", cycle)
                break

            time.sleep(0.3)

            if not self._switch_to_ifr_arvore():
                break
            try:
                current_doc_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@class,'infraArvoreNo')]/span"))
            except WebDriverException:
                current_doc_count = -1

            if current_doc_count == previous_doc_count == doc_count:
                self.logger.info(
                    "Expandir pastas: sem alteracao na arvore (doc_count=%d) no ciclo %d; encerrando.",
                    current_doc_count,
                    cycle,
                )
                break

            previous_doc_count = current_doc_count

    def _close_current_tab_and_back(self) -> None:
        try:
            if len(self.driver.window_handles) > 1:
                self.driver.close()
                remaining = list(self.driver.window_handles)
                self.driver.switch_to.window(remaining[-1])
        except Exception as exc:
            self.logger.exception("Falha ao fechar aba/voltar: %s", exc)

    def _back_to_interno_list(self) -> None:
        try:
            self.driver.back()
            time.sleep(1)
        except Exception:
            return

