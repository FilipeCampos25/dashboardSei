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

PESQUISA_CONTEXT_STAGNATION_PASSES = 6
PESQUISA_CONTEXT_STAGNATION_MIN_SECONDS = 2.0
PESQUISA_STATE_SEARCH_FORM = "search_form"
PESQUISA_STATE_SEARCH_RESULTS = "search_results"
PESQUISA_STATE_INACTIVE = "inactive"


@dataclass(frozen=True)
class SearchHit:
    """Resultado de uma pesquisa no 'Pesquisar no Processo'."""

    # Texto visivel do link (ex.: "60093.000015/2020-60")
    protocolo: str
    total_resultados: int = 0
    selected_position: int = 1
    selection_reason: str = "primeiro_resultado_mais_recente"


def _selector_get(selectors: Any, path: str, default: Any = None) -> Any:
    getter = getattr(selectors, "get", None)
    if callable(getter):
        try:
            return getter(path, default)
        except TypeError:
            pass

    if not isinstance(selectors, dict):
        return default

    node: Any = selectors
    for part in path.split("."):
        if not isinstance(node, dict):
            return default
        node = node.get(part)
        if node is None:
            return default
    return node


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


def _find_elements_immediate_in_current_context(driver: Any, xpath: str) -> list[Any]:
    try:
        return driver.find_elements(By.XPATH, xpath)
    except WebDriverException:
        return []


def _append_unique_context(contexts: list[str], value: str) -> None:
    if value and value not in contexts:
        contexts.append(value)


def _safe_get_attribute(elem: Any, name: str) -> str:
    try:
        return (elem.get_attribute(name) or "").strip()
    except WebDriverException:
        return ""


def _safe_driver_value(driver: Any, attr_name: str) -> str:
    try:
        return getattr(driver, attr_name, "") or ""
    except WebDriverException:
        return ""


def _state_rank(state: str) -> int:
    return {
        PESQUISA_STATE_INACTIVE: 0,
        PESQUISA_STATE_SEARCH_RESULTS: 1,
        PESQUISA_STATE_SEARCH_FORM: 2,
    }.get(state, 0)


def _merge_search_state(current: str, candidate: str) -> str:
    if _state_rank(candidate) > _state_rank(current):
        return candidate
    return current


def _dedupe_non_empty(values: list[str]) -> list[str]:
    return list(dict.fromkeys([value for value in values if value]))


def _get_anchor_xpaths(selectors: Any) -> list[str]:
    anchor_xpaths: list[str] = []
    for key in (
        "pesquisar_processos.dropdown_tipos",
        "pesquisar_processos.botao_pesquisar_submit",
        "pesquisar_processos.botao_pesquisar",
        "pesquisar_processos.caixa_de_texto",
    ):
        xp = _selector_get(selectors, key)
        if isinstance(xp, str) and xp.strip():
            anchor_xpaths.append(xp)
    anchor_xpaths.extend(
        [
            "//button[contains(@class,'ms-choice')]",
            "//input[@type='submit' and @id='sbmPesquisar']",
            "//input[@id='txtPesquisa']",
        ]
    )
    return _dedupe_non_empty(anchor_xpaths)


def _get_primary_result_xpath(selectors: Any) -> str:
    return (
        _selector_get(selectors, "pesquisar_processos.primeiro_resultado_mais_recente")
        or "(//table[contains(@class,'pesquisaResultado')]//tr[contains(@class,'pesquisaTituloRegistro')]//a)[1]"
    )


def _get_fallback_result_xpaths(selectors: Any) -> list[str]:
    return _dedupe_non_empty(
        [
            _selector_get(selectors, "pesquisar_processos.primeiro_resultado_fallback"),
            "(//table[contains(@class,'pesquisaResultado')]//tr[.//a[normalize-space(.)!='']]//a[normalize-space(.)!=''])[1]",
            "(//table[contains(@class,'pesquisaResultado')]//a[normalize-space(.)!=''])[1]",
        ]
    )


def _get_primary_result_rows_xpath(selectors: Any) -> str:
    return (
        _selector_get(selectors, "pesquisar_processos.linha_de_resultado")
        or "//table[contains(@class,'pesquisaResultado')]//tr[contains(@class,'pesquisaTituloRegistro')]"
    )


def _get_fallback_result_rows_xpaths(selectors: Any) -> list[str]:
    return _dedupe_non_empty(
        [
            _selector_get(selectors, "pesquisar_processos.linha_de_resultado_fallback"),
            "//table[contains(@class,'pesquisaResultado')]//tr[.//a[normalize-space(.)!='']]",
            "//table[contains(@class,'pesquisaResultado')]//a[normalize-space(.)!='']/ancestor::tr[1]",
        ]
    )


def _count_elements_immediate_in_current_context(driver: Any, xpath: str) -> int:
    return len(_find_elements_immediate_in_current_context(driver, xpath))


def _detect_pesquisa_state_in_current_context(
    driver: Any,
    *,
    search_xpaths: list[str],
    selectors: Any | None = None,
    frame_src: str = "",
) -> tuple[str, int, int]:
    if any(_find_elements_immediate_in_current_context(driver, xp) for xp in search_xpaths):
        return (PESQUISA_STATE_SEARCH_FORM, 0, 0)

    primary_rows = _count_elements_immediate_in_current_context(
        driver,
        _get_primary_result_rows_xpath(selectors),
    )
    fallback_rows = 0
    for xp in _get_fallback_result_rows_xpaths(selectors):
        fallback_rows = max(
            fallback_rows,
            _count_elements_immediate_in_current_context(driver, xp),
        )

    has_results_markup = bool(
        _find_elements_immediate_in_current_context(driver, "//table[contains(@class,'pesquisaResultado')]")
    ) or bool(
        _find_elements_immediate_in_current_context(
            driver,
            "//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'nenhum resultado')]",
        )
    )
    page_marker = (
        f"{_safe_driver_value(driver, 'current_url')} {frame_src}".strip().lower()
    )
    if primary_rows > 0 or fallback_rows > 0 or has_results_markup or "procedimento_pesquisar" in page_marker:
        return (PESQUISA_STATE_SEARCH_RESULTS, primary_rows, fallback_rows)
    return (PESQUISA_STATE_INACTIVE, primary_rows, fallback_rows)


def describe_pesquisa_context(driver: Any, selectors: Any) -> dict[str, Any]:
    search_xpaths = _get_anchor_xpaths(selectors)
    context_state = PESQUISA_STATE_INACTIVE
    primary_result_count = 0
    fallback_result_count = 0
    ifr_conteudo_src = ""
    ifr_visualizacao_src = ""

    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    top_url = _safe_driver_value(driver, "current_url")
    top_title = _safe_driver_value(driver, "title")

    state_here, primary_here, fallback_here = _detect_pesquisa_state_in_current_context(
        driver,
        search_xpaths=search_xpaths,
        selectors=selectors,
    )
    context_state = _merge_search_state(context_state, state_here)
    primary_result_count = max(primary_result_count, primary_here)
    fallback_result_count = max(fallback_result_count, fallback_here)

    try:
        root_iframes = driver.find_elements(By.TAG_NAME, "iframe")
    except WebDriverException:
        root_iframes = []

    for root_idx in range(len(root_iframes)):
        try:
            driver.switch_to.default_content()
            root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
            if root_idx >= len(root_iframes_now):
                continue
            root_frame = root_iframes_now[root_idx]
            root_id = _safe_get_attribute(root_frame, "id")
            root_name = _safe_get_attribute(root_frame, "name")
            root_src = _safe_get_attribute(root_frame, "src")
            if root_id == "ifrConteudoVisualizacao" or root_name == "ifrConteudoVisualizacao":
                ifr_conteudo_src = root_src

            driver.switch_to.frame(root_frame)
            state_here, primary_here, fallback_here = _detect_pesquisa_state_in_current_context(
                driver,
                search_xpaths=search_xpaths,
                selectors=selectors,
                frame_src=root_src,
            )
            context_state = _merge_search_state(context_state, state_here)
            primary_result_count = max(primary_result_count, primary_here)
            fallback_result_count = max(fallback_result_count, fallback_here)

            try:
                inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
            except WebDriverException:
                inner_iframes = []

            for inner_idx in range(len(inner_iframes)):
                try:
                    driver.switch_to.default_content()
                    root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                    if root_idx >= len(root_iframes_now):
                        break

                    root_frame = root_iframes_now[root_idx]
                    root_id = _safe_get_attribute(root_frame, "id")
                    root_name = _safe_get_attribute(root_frame, "name")
                    root_src = _safe_get_attribute(root_frame, "src")
                    if root_id == "ifrConteudoVisualizacao" or root_name == "ifrConteudoVisualizacao":
                        ifr_conteudo_src = root_src

                    driver.switch_to.frame(root_frame)
                    inner_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                    if inner_idx >= len(inner_iframes_now):
                        continue

                    inner_frame = inner_iframes_now[inner_idx]
                    inner_id = _safe_get_attribute(inner_frame, "id")
                    inner_name = _safe_get_attribute(inner_frame, "name")
                    inner_src = _safe_get_attribute(inner_frame, "src")
                    if inner_id == "ifrVisualizacao" or inner_name == "ifrVisualizacao":
                        ifr_visualizacao_src = inner_src

                    driver.switch_to.frame(inner_frame)
                    state_here, primary_here, fallback_here = _detect_pesquisa_state_in_current_context(
                        driver,
                        search_xpaths=search_xpaths,
                        selectors=selectors,
                        frame_src=inner_src,
                    )
                    context_state = _merge_search_state(context_state, state_here)
                    primary_result_count = max(primary_result_count, primary_here)
                    fallback_result_count = max(fallback_result_count, fallback_here)
                except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
                    continue
        except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
            continue
        finally:
            try:
                driver.switch_to.default_content()
            except WebDriverException:
                pass

    return {
        "state": context_state,
        "current_url": top_url,
        "current_title": top_title,
        "ifrConteudoVisualizacao_src": ifr_conteudo_src,
        "ifrVisualizacao_src": ifr_visualizacao_src,
        "primary_result_count": primary_result_count,
        "fallback_result_count": fallback_result_count,
    }


def log_debug_pesquisa_state(
    driver: Any,
    selectors: Any,
    logger: Any,
    *,
    processo: str = "",
    ponto: str = "",
) -> None:
    processo_value = _norm(processo) or "-"
    ponto_suffix = f" ponto={ponto}" if ponto else ""
    try:
        diagnostics = describe_pesquisa_context(driver, selectors)
        logger.info(
            "DEBUG STATE: estado=%s url=%s processo=%s%s",
            (diagnostics.get("state") or PESQUISA_STATE_INACTIVE).upper(),
            diagnostics.get("current_url") or "-",
            processo_value,
            ponto_suffix,
        )
    except Exception as exc:
        logger.info(
            "DEBUG STATE: estado=UNKNOWN url=%s processo=%s%s erro=%s",
            _safe_driver_value(driver, "current_url") or "-",
            processo_value,
            ponto_suffix,
            exc,
        )


def _build_context_signature(
    *,
    root_descriptions: list[str],
    inner_descriptions: list[str],
    matched_xpath: bool,
    context_state: str,
    current_url: str,
    current_title: str,
) -> tuple[str, ...]:
    parts: list[str] = []
    if not root_descriptions:
        parts.append("roots:none")
    else:
        parts.extend(root_descriptions)

    if inner_descriptions:
        parts.extend(inner_descriptions)

    parts.append(f"context_state:{context_state}")
    parts.append(f"current_url:{_norm(current_url)}")
    parts.append(f"current_title:{_norm(current_title)}")
    parts.append(f"matched_xpath:{int(bool(matched_xpath))}")
    return tuple(parts)


def _find_first_in_pesquisa_context(
    driver: Any,
    logger: Any,
    timeout_seconds: int,
    search_xpaths: list[str],
    element_name: str,
    selectors: Any | None = None,
) -> Any:
    deadline = time.time() + max(1, int(timeout_seconds))
    tried_contexts: list[str] = []
    last_error: Exception | None = None
    last_signature: tuple[str, ...] | None = None
    signature_started_at: float | None = None
    same_signature_passes = 0
    pass_number = 0
    stagnated = False

    while time.time() < deadline:
        pass_number += 1
        matched_xpath = False
        context_state = PESQUISA_STATE_INACTIVE
        root_descriptions: list[str] = []
        inner_descriptions: list[str] = []
        pending_logs: list[tuple[str, int, str, str, int | None]] = []
        current_url = ""
        current_title = ""

        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

        current_url = _safe_driver_value(driver, "current_url")
        current_title = _safe_driver_value(driver, "title")

        for xp in search_xpaths:
            elems = _find_elements_immediate_in_current_context(driver, xp)
            if elems:
                matched_xpath = True
                logger.info(
                    "Pesquisar no Processo: %s encontrado no contexto principal.",
                    element_name,
                )
                return elems[0]

        state_here, _, _ = _detect_pesquisa_state_in_current_context(
            driver,
            search_xpaths=search_xpaths,
            selectors=selectors,
        )
        context_state = _merge_search_state(context_state, state_here)

        try:
            root_iframes = driver.find_elements(By.TAG_NAME, "iframe")
        except WebDriverException as exc:
            root_iframes = []
            last_error = exc

        if not root_iframes:
            _append_unique_context(tried_contexts, "default->iframe(nao encontrado)")
        else:
            for root_idx in range(len(root_iframes)):
                try:
                    driver.switch_to.default_content()
                    root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                    if root_idx >= len(root_iframes_now):
                        _append_unique_context(tried_contexts, f"default->root[{root_idx}](stale)")
                        continue

                    root_frame = root_iframes_now[root_idx]
                    root_id = _safe_get_attribute(root_frame, "id") or "-"
                    root_name = _safe_get_attribute(root_frame, "name") or "-"
                    root_src = _safe_get_attribute(root_frame, "src") or "-"
                    root_descriptions.append(f"root[{root_idx}]({root_id},{root_name},{root_src})")
                    pending_logs.append(("root", root_idx, root_id, root_name, None))

                    driver.switch_to.frame(root_frame)
                    _append_unique_context(tried_contexts, f"default->root[{root_idx}]")
                except (NoSuchFrameException, StaleElementReferenceException, WebDriverException) as exc:
                    _append_unique_context(tried_contexts, f"default->root[{root_idx}](erro switch)")
                    last_error = exc
                    continue

                for xp in search_xpaths:
                    elems = _find_elements_immediate_in_current_context(driver, xp)
                    if elems:
                        matched_xpath = True
                        logger.info(
                            "Pesquisar no Processo: %s encontrado no iframe raiz #%d (id=%s, name=%s).",
                            element_name,
                            root_idx,
                            root_id,
                            root_name,
                        )
                        return elems[0]

                state_here, _, _ = _detect_pesquisa_state_in_current_context(
                    driver,
                    search_xpaths=search_xpaths,
                    selectors=selectors,
                    frame_src=root_src,
                )
                context_state = _merge_search_state(context_state, state_here)

                try:
                    inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                except WebDriverException:
                    inner_iframes = []

                for inner_idx in range(len(inner_iframes)):
                    try:
                        driver.switch_to.default_content()
                        root_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                        if root_idx >= len(root_iframes_now):
                            _append_unique_context(
                                tried_contexts,
                                f"default->root[{root_idx}](stale apos inner)",
                            )
                            break

                        root_frame = root_iframes_now[root_idx]
                        root_id = _safe_get_attribute(root_frame, "id") or "-"
                        root_name = _safe_get_attribute(root_frame, "name") or "-"
                        driver.switch_to.frame(root_frame)

                        inner_iframes_now = driver.find_elements(By.TAG_NAME, "iframe")
                        if inner_idx >= len(inner_iframes_now):
                            _append_unique_context(tried_contexts, f"root[{root_idx}]->inner[{inner_idx}](stale)")
                            continue

                        inner_frame = inner_iframes_now[inner_idx]
                        inner_id = _safe_get_attribute(inner_frame, "id") or "-"
                        inner_name = _safe_get_attribute(inner_frame, "name") or "-"
                        inner_src = _safe_get_attribute(inner_frame, "src") or "-"
                        inner_descriptions.append(
                            f"root[{root_idx}]->inner[{inner_idx}]({inner_id},{inner_name},{inner_src})"
                        )
                        pending_logs.append(("inner", inner_idx, inner_id, inner_name, root_idx))

                        driver.switch_to.frame(inner_frame)
                        _append_unique_context(tried_contexts, f"root[{root_idx}]->inner[{inner_idx}]")
                    except (NoSuchFrameException, StaleElementReferenceException, WebDriverException) as exc:
                        _append_unique_context(tried_contexts, f"root[{root_idx}]->inner[{inner_idx}](erro switch)")
                        last_error = exc
                        continue

                    for xp in search_xpaths:
                        elems = _find_elements_immediate_in_current_context(driver, xp)
                        if elems:
                            matched_xpath = True
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

                    state_here, _, _ = _detect_pesquisa_state_in_current_context(
                        driver,
                        search_xpaths=search_xpaths,
                        selectors=selectors,
                        frame_src=inner_src,
                    )
                    context_state = _merge_search_state(context_state, state_here)

                    try:
                        driver.switch_to.default_content()
                    except WebDriverException:
                        pass

        signature = _build_context_signature(
            root_descriptions=root_descriptions,
            inner_descriptions=inner_descriptions,
            matched_xpath=matched_xpath,
            context_state=context_state,
            current_url=current_url,
            current_title=current_title,
        )
        should_log_pass = pass_number == 1 or signature != last_signature
        if should_log_pass:
            for entry_type, index, frame_id, frame_name, root_idx in pending_logs:
                if entry_type == "root":
                    logger.info(
                        "Pesquisar no Processo: testando iframe raiz #%d (id=%s, name=%s).",
                        index,
                        frame_id,
                        frame_name,
                    )
                else:
                    logger.info(
                        "Pesquisar no Processo: testando inner iframe #%d do root #%d (id=%s, name=%s).",
                        index,
                        int(root_idx or 0),
                        frame_id,
                        frame_name,
                    )

        now = time.time()
        if signature == last_signature:
            same_signature_passes += 1
        else:
            last_signature = signature
            same_signature_passes = 1
            signature_started_at = now

        if (
            context_state == PESQUISA_STATE_INACTIVE
            and
            same_signature_passes >= PESQUISA_CONTEXT_STAGNATION_PASSES
            and signature_started_at is not None
            and (now - signature_started_at) >= PESQUISA_CONTEXT_STAGNATION_MIN_SECONDS
        ):
            stagnated = True
            break

        time.sleep(0.2)

    failure_reason = "estagnacao_do_contexto" if stagnated else "timeout"
    if stagnated:
        logger.warning(
            "Pesquisar no Processo: varredura interrompida por estagnacao do contexto apos %d passada(s). estado=%s contextos_unicos=%s",
            same_signature_passes,
            context_state,
            tried_contexts,
        )
    logger.error(
        "Pesquisar no Processo: elemento '%s' nao localizado. estado=%s contextos_unicos=%s motivo=%s",
        element_name,
        context_state,
        tried_contexts,
        failure_reason,
    )
    raise TimeoutException(
        "Timeout aguardando elemento no contexto de pesquisa: "
        f"elemento={element_name} timeout={timeout_seconds}s contextos={tried_contexts} estado={context_state} motivo={failure_reason}"
    ) from last_error


def _switch_to_pesquisa_context(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    timeout_seconds: int,
) -> None:
    deduped = _get_anchor_xpaths(selectors)
    current_state, _, _ = _detect_pesquisa_state_in_current_context(
        driver,
        search_xpaths=deduped,
        selectors=selectors,
    )
    if current_state == PESQUISA_STATE_SEARCH_FORM:
        logger.info("STATE SKIP: já em SEARCH_FORM → reutilizando contexto atual")
        return
    log_debug_pesquisa_state(
        driver,
        selectors,
        logger,
        ponto="_switch_to_pesquisa_context:start",
    )
    if current_state == PESQUISA_STATE_SEARCH_RESULTS:
        search_button_xpaths = _dedupe_non_empty(
            [
                _selector_get(selectors, "pesquisar_processos.botao_pesquisar"),
                "//input[@name='sbmPesquisar']",
                "//button[@name='sbmPesquisar']",
                "//input[@type='submit' and normalize-space(@value)='Pesquisar']",
                "//button[normalize-space(.)='Pesquisar']",
            ]
        )
        search_button = None
        for xp in search_button_xpaths:
            elems = _find_elements_immediate_in_current_context(driver, xp)
            if elems:
                search_button = elems[0]
                break
        if search_button is not None:
            logger.info("STATE FIX: SEARCH_RESULTS detectado → clicando em Pesquisar para voltar ao formulário")
            _click_element(driver, search_button)
            form_deadline = time.time() + max(1, int(timeout_seconds))
            while time.time() < form_deadline:
                state_after_click, _, _ = _detect_pesquisa_state_in_current_context(
                    driver,
                    search_xpaths=deduped,
                    selectors=selectors,
                )
                if state_after_click == PESQUISA_STATE_SEARCH_FORM:
                    break
                time.sleep(0.2)
    log_debug_pesquisa_state(
        driver,
        selectors,
        logger,
        ponto="_switch_to_pesquisa_context:before_find_first",
    )
    _find_first_in_pesquisa_context(
        driver=driver,
        logger=logger,
        timeout_seconds=timeout_seconds,
        search_xpaths=deduped,
        element_name="anchor do filtro",
        selectors=selectors,
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
    return _get_resultado_por_posicao(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
        position=1,
    )


def _dedupe_links(links: list[Any]) -> list[Any]:
    deduped: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        try:
            text = _norm(link.text)
        except WebDriverException:
            text = ""
        href = _safe_get_attribute(link, "href")
        key = (text, href)
        if not text and not href:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


def _collect_result_links(
    driver: Any,
    selectors: XPathSelectors,
    timeout_seconds: int,
) -> list[Any]:
    deadline = time.time() + max(1, int(timeout_seconds))
    primary_rows_xpath = _get_primary_result_rows_xpath(selectors)
    fallback_row_xpaths = _get_fallback_result_rows_xpaths(selectors)
    generic_link_xpaths = [
        "//table[contains(@class,'pesquisaResultado')]//tr[.//a[normalize-space(.)!='']]//a[normalize-space(.)!=''][1]",
        "//table[contains(@class,'pesquisaResultado')]//a[normalize-space(.)!='']",
    ]

    while time.time() < deadline:
        rows = _find_elements_immediate_in_current_context(driver, primary_rows_xpath)
        if not rows:
            for xpath in fallback_row_xpaths:
                rows = _find_elements_immediate_in_current_context(driver, xpath)
                if rows:
                    break
        if rows:
            row_links: list[Any] = []
            for row in rows:
                try:
                    links = row.find_elements(By.XPATH, ".//a[normalize-space(.)!='']")
                except WebDriverException:
                    links = []
                if links:
                    row_links.append(links[0])
            deduped = _dedupe_links(row_links)
            if deduped:
                return deduped

        for xpath in generic_link_xpaths:
            deduped = _dedupe_links(_find_elements_immediate_in_current_context(driver, xpath))
            if deduped:
                return deduped

        time.sleep(0.2)
    return []


def _get_resultado_por_posicao(
    driver: Any,
    selectors: XPathSelectors,
    timeout_seconds: int,
    position: int,
) -> Optional[Any]:
    if position < 1:
        return None
    links = _collect_result_links(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
    )
    if position > len(links):
        return None
    return links[position - 1]


def _build_search_hit(link: Any, position: int, total_resultados: int) -> SearchHit:
    protocolo = _norm(getattr(link, "text", "") or "")
    if not protocolo:
        try:
            row = link.find_element(By.XPATH, "./ancestor::tr[1]")
            protocolo = _norm(row.text)
        except WebDriverException:
            protocolo = ""
    return SearchHit(
        protocolo=protocolo,
        total_resultados=total_resultados,
        selected_position=position,
        selection_reason="resultado_ranqueado_por_data",
    )


def _contar_resultados(driver: Any, selectors: XPathSelectors) -> int:
    primary_count = _count_elements_immediate_in_current_context(
        driver,
        _get_primary_result_rows_xpath(selectors),
    )
    fallback_count = 0
    for xp in _get_fallback_result_rows_xpaths(selectors):
        fallback_count = max(fallback_count, _count_elements_immediate_in_current_context(driver, xp))
    return max(primary_count, fallback_count)


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

        first_link = _get_primeiro_resultado(driver=driver, selectors=selectors, timeout_seconds=timeout_seconds)
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

    hits = listar_resultados_pesquisa(
        driver=driver,
        selectors=selectors,
        logger=logger,
        termo=termo,
        timeout_seconds=timeout_seconds,
    )
    if not hits:
        logger.info("Nenhum resultado encontrado para tipo: %s", termo)
        return None
    return hits[0]


def listar_resultados_pesquisa(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    termo: str,
    timeout_seconds: int = 20,
) -> list[SearchHit]:
    pesquisou = _executar_pesquisa_por_tipo_exato(
        driver=driver,
        selectors=selectors,
        logger=logger,
        tipo_exato=termo,
        timeout_seconds=timeout_seconds,
    )
    if not pesquisou:
        return []

    links = _collect_result_links(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
    )
    if not links:
        logger.info("Nenhum resultado encontrado para tipo: %s", termo)
        return []

    total_resultados = max(_contar_resultados(driver, selectors), len(links))
    hits = [_build_search_hit(link, index, total_resultados) for index, link in enumerate(links, start=1)]
    logger.info(
        "Busca por tipo '%s': total_resultados=%d candidatos=%d primeiro=%s",
        termo,
        total_resultados,
        len(hits),
        hits[0].protocolo,
    )
    return hits


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
    abrir_resultado_pesquisa_por_posicao(
        driver=driver,
        selectors=selectors,
        logger=logger,
        position=1,
        timeout_seconds=timeout_seconds,
    )


def abrir_resultado_pesquisa_por_posicao(
    driver: Any,
    selectors: XPathSelectors,
    logger: Any,
    position: int,
    timeout_seconds: int = 20,
) -> None:
    target_link = _get_resultado_por_posicao(
        driver=driver,
        selectors=selectors,
        timeout_seconds=timeout_seconds,
        position=position,
    )
    if target_link is None:
        raise TimeoutException("Nenhum resultado disponivel para abrir")
    link_preview = _norm(target_link.text)
    logger.info("Abrir documento do filtro: posicao=%d link_identificado='%s'", position, link_preview)
    _click_element(driver, target_link)
    logger.info("Documento do filtro aberto (click na posicao %d).", position)

