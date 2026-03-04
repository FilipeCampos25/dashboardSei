from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Optional

from selenium.common.exceptions import (
    NoSuchFrameException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

from app.rpa.selectors import XPathSelectors


@dataclass(frozen=True)
class SearchHit:
    """Resultado de uma pesquisa no 'Pesquisar no Processo'."""

    # Texto visivel do link (ex.: "60093.000015/2020-60")
    protocolo: str


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _xpath_text_literal(value: str) -> str:
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    return "concat(" + ", \"'\", ".join(f"'{part}'" for part in parts) + ")"


def _find_elements_in_current_context(
    driver: Any,
    xpath: str,
    timeout_seconds: int,
) -> list[Any]:
    deadline = time.time() + max(1, int(timeout_seconds))
    while time.time() < deadline:
        try:
            elems = driver.find_elements(By.XPATH, xpath)
        except WebDriverException:
            elems = []
        if elems:
            return elems
        time.sleep(0.2)
    return []


def _find_first_in_pesquisa_context(
    driver: Any,
    logger: Any,
    timeout_seconds: int,
    search_xpaths: list[str],
    element_name: str,
) -> Any:
    deadline = time.time() + max(1, int(timeout_seconds))
    tried_contexts: list[str] = []
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

        try:
            root_iframes = driver.find_elements(By.TAG_NAME, "iframe")
        except WebDriverException as exc:
            root_iframes = []
            last_error = exc

        if not root_iframes:
            tried_contexts.append("default->iframe(nao encontrado)")
            time.sleep(0.2)
            continue

        for root_idx in range(len(root_iframes)):
            try:
                driver.switch_to.default_content()
                root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                if root_idx >= len(root_iframes_now):
                    tried_contexts.append(f"default->root[{root_idx}](stale)")
                    continue

                root_frame = root_iframes_now[root_idx]
                root_id = (root_frame.get_attribute("id") or "").strip() or "-"
                root_name = (root_frame.get_attribute("name") or "").strip() or "-"
                logger.info(
                    "Pesquisar no Processo: testando iframe raiz #%d (id=%s, name=%s).",
                    root_idx,
                    root_id,
                    root_name,
                )

                driver.switch_to.frame(root_frame)
                tried_contexts.append(f"default->root[{root_idx}]")
            except (NoSuchFrameException, StaleElementReferenceException, WebDriverException) as exc:
                tried_contexts.append(f"default->root[{root_idx}](erro switch)")
                last_error = exc
                continue

            for xp in search_xpaths:
                elems = _find_elements_in_current_context(driver, xp, timeout_seconds=1)
                if elems:
                    logger.info(
                        "Pesquisar no Processo: %s encontrado no iframe raiz #%d (id=%s, name=%s).",
                        element_name,
                        root_idx,
                        root_id,
                        root_name,
                    )
                    return elems[0]

            try:
                inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
            except WebDriverException:
                inner_iframes = []

            for inner_idx in range(len(inner_iframes)):
                try:
                    driver.switch_to.default_content()
                    root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                    if root_idx >= len(root_iframes_now):
                        tried_contexts.append(f"default->root[{root_idx}](stale apos inner)")
                        break

                    root_frame = root_iframes_now[root_idx]
                    root_id = (root_frame.get_attribute("id") or "").strip() or "-"
                    root_name = (root_frame.get_attribute("name") or "").strip() or "-"
                    driver.switch_to.frame(root_frame)

                    inner_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                    if inner_idx >= len(inner_iframes_now):
                        tried_contexts.append(f"root[{root_idx}]->inner[{inner_idx}](stale)")
                        continue

                    inner_frame = inner_iframes_now[inner_idx]
                    inner_id = (inner_frame.get_attribute("id") or "").strip() or "-"
                    inner_name = (inner_frame.get_attribute("name") or "").strip() or "-"
                    logger.info(
                        "Pesquisar no Processo: testando inner iframe #%d do root #%d (id=%s, name=%s).",
                        inner_idx,
                        root_idx,
                        inner_id,
                        inner_name,
                    )

                    driver.switch_to.frame(inner_frame)
                    tried_contexts.append(f"root[{root_idx}]->inner[{inner_idx}]")
                except (NoSuchFrameException, StaleElementReferenceException, WebDriverException) as exc:
                    tried_contexts.append(f"root[{root_idx}]->inner[{inner_idx}](erro switch)")
                    last_error = exc
                    continue

                for xp in search_xpaths:
                    elems = _find_elements_in_current_context(driver, xp, timeout_seconds=1)
                    if elems:
                        logger.info(
                            "Pesquisar no Processo: %s encontrado no inner iframe #%d do root #%d (root id=%s, root name=%s, inner id=%s, inner name=%s).",
                            element_name,
                            inner_idx,
                            root_idx,
                            root_id,
                            root_name,
                            inner_id,
                            inner_name,
                        )
                        return elems[0]

                try:
                    driver.switch_to.default_content()
                except WebDriverException:
                    pass

        time.sleep(0.2)

    logger.error(
        "Pesquisar no Processo: elemento '%s' nao localizado. Contextos tentados=%s",
        element_name,
        tried_contexts,
    )
    raise TimeoutException(
        "Timeout aguardando elemento no contexto de pesquisa: "
        f"elemento={element_name} timeout={timeout_seconds}s contextos={tried_contexts}"
    ) from last_error


def _switch_to_pesquisa_context(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    timeout_seconds: int,
) -> None:
    anchor_xpaths: list[str] = []
    for key in (
        "pesquisar_processos.dropdown_tipos",
        "pesquisar_processos.botao_pesquisar_submit",
        "pesquisar_processos.botao_pesquisar",
        "pesquisar_processos.caixa_de_texto",
    ):
        xp = selectors.get(key)
        if xp:
            anchor_xpaths.append(xp)
    anchor_xpaths.extend(
        [
            "//button[contains(@class,'ms-choice')]",
            "//input[@type='submit' and @id='sbmPesquisar']",
            "//input[@id='txtPesquisa']",
        ]
    )
    deduped = list(dict.fromkeys(anchor_xpaths))
    _find_first_in_pesquisa_context(
        driver=driver,
        logger=logger,
        timeout_seconds=timeout_seconds,
        search_xpaths=deduped,
        element_name="anchor do filtro",
    )


def _get_first_by_xpath(driver: Any, xpath: str, timeout_seconds: int) -> Optional[Any]:
    elems = _find_elements_in_current_context(driver, xpath, timeout_seconds=timeout_seconds)
    if not elems:
        return None
    return elems[0]


def _build_tipo_xpath(template: str, tipo_exato: str) -> str:
    placeholder = "{TIPO_EXATO}"
    if placeholder not in template:
        return template
    return template.replace(placeholder, _xpath_text_literal(tipo_exato))


def _find_tipo_option_case_insensitive(
    driver: Any,
    selectors: XPathSelectors,
    tipo_exato: str,
) -> tuple[Optional[Any], Optional[Any]]:
    dropdown_visivel_xpath = (
        selectors.get("pesquisar_processos.dropdown_lista_visivel")
        or "//div[contains(@class,'ms-drop') and contains(@class,'bottom') and not(contains(@style,'display: none'))]"
    )
    containers = driver.find_elements(By.XPATH, dropdown_visivel_xpath)
    if not containers:
        return (None, None)

    alvo = _norm(tipo_exato).casefold()
    for container in containers:
        spans = container.find_elements(By.XPATH, ".//li//span[normalize-space(.)!='']")
        for span in spans:
            texto = _norm(span.text).casefold()
            if texto != alvo:
                continue

            li_nodes = span.find_elements(By.XPATH, "./ancestor::li[1]")
            if not li_nodes:
                continue
            li = li_nodes[0]

            labels = li.find_elements(By.XPATH, ".//label")
            checkboxes = li.find_elements(By.XPATH, ".//input[@type='checkbox']")
            label = labels[0] if labels else None
            checkbox = checkboxes[0] if checkboxes else None
            return (checkbox, label)

    return (None, None)


def _click_element(driver: Any, elem: Any) -> None:
    try:
        elem.click()
    except Exception:
        driver.execute_script("arguments[0].click();", elem)


def _open_dropdown_tipos(
    driver: Any,
    selectors: XPathSelectors,
    timeout_seconds: int,
) -> bool:
    dropdown_xpath = (
        selectors.get("pesquisar_processos.dropdown_tipos")
        or "//button[contains(@class,'ms-choice')]"
    )
    dropdown_visivel_xpath = (
        selectors.get("pesquisar_processos.dropdown_lista_visivel")
        or "//div[contains(@class,'ms-drop') and contains(@class,'bottom') and not(contains(@style,'display: none'))]"
    )

    for _ in range(2):
        btn = _get_first_by_xpath(driver, dropdown_xpath, timeout_seconds=2)
        if btn is None:
            return False
        _click_element(driver, btn)
        if _get_first_by_xpath(driver, dropdown_visivel_xpath, timeout_seconds=2) is not None:
            return True
    return False


def _clear_tipo_selections(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
) -> None:
    checked_xpath = (
        selectors.get("pesquisar_processos.checkboxes_marcados")
        or "//div[contains(@class,'ms-drop') and contains(@class,'bottom') and not(contains(@style,'display: none'))]//input[@type='checkbox' and @checked]"
    )

    checked_boxes = driver.find_elements(By.XPATH, checked_xpath)
    if not checked_boxes:
        return

    for box in checked_boxes:
        try:
            li_nodes = box.find_elements(By.XPATH, "./ancestor::li[1]")
            label = None
            if li_nodes:
                labels = li_nodes[0].find_elements(By.XPATH, ".//label")
                if labels:
                    label = labels[0]
            if label is not None:
                _click_element(driver, label)
            else:
                _click_element(driver, box)
        except WebDriverException:
            continue

    logger.info("Pesquisar no Processo: filtros de tipo anteriores foram limpos.")


def _executar_pesquisa_por_tipo_exato(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    tipo_exato: str,
    timeout_seconds: int,
) -> bool:
    tipo_exato = _norm(tipo_exato)
    if not tipo_exato:
        logger.warning("Pesquisar no Processo: tipo_exato vazio.")
        return False

    _switch_to_pesquisa_context(
        driver=driver,
        selectors=selectors,
        logger=logger,
        timeout_seconds=timeout_seconds,
    )

    if not _open_dropdown_tipos(driver=driver, selectors=selectors, timeout_seconds=timeout_seconds):
        logger.warning("Pesquisar no Processo: nao foi possivel abrir o dropdown de tipos.")
        return False

    _clear_tipo_selections(driver=driver, selectors=selectors, logger=logger)

    checkbox_template = (
        selectors.get("pesquisar_processos.item_tipo_checkbox_template")
        or "//div[contains(@class,'ms-drop') and contains(@class,'bottom') and not(contains(@style,'display: none'))]//li[.//span[normalize-space(.)='{TIPO_EXATO}']]//input[@type='checkbox']"
    )
    label_template = (
        selectors.get("pesquisar_processos.item_tipo_label_template")
        or "//div[contains(@class,'ms-drop') and contains(@class,'bottom') and not(contains(@style,'display: none'))]//li[.//span[normalize-space(.)='{TIPO_EXATO}']]//label"
    )
    checkbox_xpath = _build_tipo_xpath(checkbox_template, tipo_exato)
    label_xpath = _build_tipo_xpath(label_template, tipo_exato)

    checkbox = _get_first_by_xpath(driver, checkbox_xpath, timeout_seconds=2)
    label = _get_first_by_xpath(driver, label_xpath, timeout_seconds=2)
    if checkbox is None and label is None:
        checkbox, label = _find_tipo_option_case_insensitive(
            driver=driver,
            selectors=selectors,
            tipo_exato=tipo_exato,
        )
    if checkbox is None and label is None:
        logger.info("Tipo n\u00e3o dispon\u00edvel no processo: %s", tipo_exato)
        return False
    logger.info("Tipo encontrado no dropdown: %s", tipo_exato)

    selected = False
    if checkbox is not None:
        selected = bool(checkbox.get_attribute("checked")) or checkbox.is_selected()

    if not selected:
        if label is not None:
            _click_element(driver, label)
            logger.info("Tipo clicado no dropdown (label): %s", tipo_exato)
        elif checkbox is not None:
            _click_element(driver, checkbox)
            logger.info("Tipo clicado no dropdown (checkbox): %s", tipo_exato)
    else:
        logger.info("Tipo ja estava selecionado no dropdown: %s", tipo_exato)

    btn_submit_xpath = (
        selectors.get("pesquisar_processos.botao_pesquisar_submit")
        or "//input[@type='submit' and @id='sbmPesquisar']"
    )
    btn_submit_fallback_xpath = (
        selectors.get("pesquisar_processos.botao_pesquisar_submit_fallback")
        or "//input[@type='submit' and (contains(@value,'Pesquisar') or contains(@value,'PESQUISAR'))]"
    )

    btn = _get_first_by_xpath(driver, btn_submit_xpath, timeout_seconds=3)
    if btn is None:
        btn = _get_first_by_xpath(driver, btn_submit_fallback_xpath, timeout_seconds=3)
    if btn is None:
        logger.warning("Pesquisar no Processo: botao de submit nao encontrado.")
        return False

    _click_element(driver, btn)
    logger.info("Tipo selecionado e pesquisa executada: %s", tipo_exato)
    return True


def _get_primeiro_resultado(
    driver: Any,
    selectors: XPathSelectors,
    timeout_seconds: int,
) -> Optional[Any]:
    primeiro_xpath = (
        selectors.get("pesquisar_processos.primeiro_resultado_mais_recente")
        or "(//table[contains(@class,'pesquisaResultado')]//tr[contains(@class,'pesquisaTituloRegistro')]//a)[1]"
    )
    return _get_first_by_xpath(driver, primeiro_xpath, timeout_seconds=timeout_seconds)


def buscar_por_tipo_e_abrir_mais_recente(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    tipo_exato: str,
    timeout_seconds: int = 20,
) -> bool:
    """
    Busca por tipo exato no filtro "Pesquisar no Processo" e abre o resultado mais recente.

    Retorna:
      - True quando abriu o primeiro resultado
      - False quando tipo/resultado nao existir ou em falha resiliente
    """
    try:
        pesquisou = _executar_pesquisa_por_tipo_exato(
            driver=driver,
            selectors=selectors,
            logger=logger,
            tipo_exato=tipo_exato,
            timeout_seconds=timeout_seconds,
        )
        if not pesquisou:
            return False

        first_link = _get_primeiro_resultado(
            driver=driver,
            selectors=selectors,
            timeout_seconds=timeout_seconds,
        )
        if first_link is None:
            logger.info("Nenhum resultado encontrado para tipo: %s", tipo_exato)
            return False

        _click_element(driver, first_link)
        logger.info("Documento mais recente aberto para tipo exato: %s", tipo_exato)
        return True
    except (TimeoutException, WebDriverException) as exc:
        logger.warning(
            "Pesquisar no Processo: falha resiliente na busca por tipo exato '%s' (%s).",
            tipo_exato,
            exc,
        )
        return False


def buscar_documento_mais_recente(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    termo: str,
    timeout_seconds: int = 20,
) -> Optional[SearchHit]:
    """
    Executa a busca no 'Pesquisar no Processo' por tipo exato e retorna
    o documento MAIS RECENTE (topo), sem abrir o link.

    Retorno:
      - SearchHit se houver resultados
      - None se nao houver nenhum resultado ou se o tipo nao existir
    """
    pesquisou = _executar_pesquisa_por_tipo_exato(
        driver=driver,
        selectors=selectors,
        logger=logger,
        tipo_exato=termo,
        timeout_seconds=timeout_seconds,
    )
    if not pesquisou:
        return None

    first_link = _get_primeiro_resultado(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
    )
    if first_link is None:
        logger.info("Nenhum resultado encontrado para tipo: %s", termo)
        return None

    protocolo = _norm(first_link.text)
    if not protocolo:
        try:
            row = first_link.find_element(By.XPATH, "./ancestor::tr[1]")
            protocolo = _norm(row.text)
        except WebDriverException:
            protocolo = ""

    logger.info("Busca por tipo '%s': selecionado mais recente (topo): %s", termo, protocolo)
    return SearchHit(protocolo=protocolo)


def abrir_documento_mais_recente(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    timeout_seconds: int = 20,
) -> None:
    """
    Clica no link do documento mais recente (primeiro resultado) na tela atual.

    Util quando voce ja chamou buscar_documento_mais_recente() e quer abrir imediatamente.
    """
    first_link = _get_primeiro_resultado(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
    )
    if first_link is None:
        raise TimeoutException("Nenhum resultado disponivel para abrir")

    _click_element(driver, first_link)
    logger.info("Documento mais recente aberto (click no primeiro resultado).")

