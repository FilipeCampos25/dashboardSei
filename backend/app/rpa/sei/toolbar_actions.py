from __future__ import annotations

import time
from typing import Any, List

from selenium.common.exceptions import (
    NoSuchFrameException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from app.rpa.selenium_utils import click_xpath_with_retry


def _resolve_timeout(driver: Any, timeout: int | float | None) -> float:
    if timeout is not None:
        return float(timeout)
    return max(10.0, float(getattr(driver, "_sei_timeout_seconds", 10)))


def _get_selector_candidates(selectors: Any, path: str) -> List[str]:
    candidates = selectors.get_many(path)
    return [str(xpath) for xpath in candidates if str(xpath).strip()]


def _switch_to_ifr_arvore_if_present(driver: Any) -> bool:
    for by, value in ((By.ID, "ifrArvore"), (By.NAME, "ifrArvore")):
        try:
            driver.switch_to.default_content()
            iframe = driver.find_element(by, value)
            driver.switch_to.frame(iframe)
            return True
        except WebDriverException:
            continue
    return False


def wait_page_ready_in_processo(driver: Any, logger: Any, timeout: int | float | None = None) -> None:
    timeout_seconds = max(10.0, min(20.0, _resolve_timeout(driver, timeout)))

    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    WebDriverWait(driver, timeout_seconds).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

    def _toolbar_or_frame_ready(current_driver: Any) -> bool:
        try:
            current_driver.switch_to.default_content()
        except WebDriverException:
            return False

        for by, value in ((By.ID, "ifrArvore"), (By.NAME, "ifrArvore")):
            try:
                if current_driver.find_elements(by, value):
                    return True
            except WebDriverException:
                continue

        try:
            if current_driver.find_elements(
                By.XPATH,
                "//a[.//img[@title='Pesquisar no Processo']]",
            ):
                return True
        except WebDriverException:
            return False

        return False

    WebDriverWait(driver, timeout_seconds).until(_toolbar_or_frame_ready)


def click_abrir_todas_as_pastas(driver: Any, selectors: Any, logger: Any) -> None:
    """
    O SEI varia onde renderiza o botão/ícone "Abrir todas as Pastas" (às vezes no TOP, às vezes em iframe).
    Além disso, o id costuma ser dinâmico (iconAP<ID_PROCEDIMENTO>), então não dá pra depender de um id fixo.
    """
    base_xpaths = _get_selector_candidates(selectors, "processo.abrir_todas_as_pastas")

    # Fallbacks robustos (não altera arquivo de selectors; só reforça aqui)
    extra_xpaths = [
        # id dinâmico (iconAP<numero>) + title
        "//img[starts-with(@id,'iconAP') and contains(@title,'Abrir todas as Pastas')]",
        # só pelo alt/title (caso mude o id)
        "//img[contains(@alt,'Abrir todas as Pastas') or contains(@title,'Abrir todas as Pastas')]",
    ]

    xpaths = list(dict.fromkeys(base_xpaths + extra_xpaths))  # preserva ordem e remove duplicados

    def _safe_default() -> None:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

    def _safe_switch_frame_by_id_or_name(frame_id: str) -> bool:
        for by, value in ((By.ID, frame_id), (By.NAME, frame_id)):
            try:
                _safe_default()
                iframe = driver.find_element(by, value)
                driver.switch_to.frame(iframe)
                return True
            except WebDriverException:
                continue
        return False

    def _attempt_click_in_current_context(context_label: str, timeout_seconds: float) -> bool:
        try:
            clicked_xpath = click_xpath_with_retry(
                driver,
                xpaths,
                "processo.abrir_todas_as_pastas",
                default_timeout_seconds=int(max(1, _resolve_timeout(driver, None))),
                timeout_seconds=timeout_seconds,
            )
            logger.info("Abrir todas as Pastas: clique realizado (%s) [contexto=%s]", clicked_xpath, context_label)
            return True
        except TimeoutException:
            return False

    tried_contexts: List[str] = []
    last_timeout: TimeoutException | None = None

    # 1) TOP
    _safe_default()
    try:
        if _attempt_click_in_current_context("TOP", timeout_seconds=2.5):
            return
        tried_contexts.append("TOP")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("TOP")

    # 2) ifrArvore (muito comum para botões ligados à árvore/pastas)
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrArvore"):
            if _attempt_click_in_current_context("ifrArvore", timeout_seconds=3.5):
                _safe_default()
                return
            tried_contexts.append("ifrArvore")
        else:
            tried_contexts.append("ifrArvore(não encontrado)")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("ifrArvore(timeout)")
    finally:
        _safe_default()

    # 3) ifrConteudoVisualizacao (algumas variações colocam o botão no conteúdo)
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
            if _attempt_click_in_current_context("ifrConteudoVisualizacao", timeout_seconds=3.5):
                _safe_default()
                return
            tried_contexts.append("ifrConteudoVisualizacao")
        else:
            tried_contexts.append("ifrConteudoVisualizacao(não encontrado)")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("ifrConteudoVisualizacao(timeout)")
    finally:
        _safe_default()

    # 4) Iframes internos dentro de ifrConteudoVisualizacao (1 nível)
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
            inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for idx in range(min(len(inner_iframes), 10)):
                try:
                    _safe_default()
                    if not _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
                        break
                    inner_iframes2 = driver.find_elements(By.TAG_NAME, "iframe")
                    if idx >= len(inner_iframes2):
                        break
                    driver.switch_to.frame(inner_iframes2[idx])

                    if _attempt_click_in_current_context(f"ifrConteudoVisualizacao->inner[{idx}]", timeout_seconds=2.5):
                        _safe_default()
                        return
                    tried_contexts.append(f"ifrConteudoVisualizacao->inner[{idx}]")
                except (StaleElementReferenceException, WebDriverException):
                    tried_contexts.append(f"ifrConteudoVisualizacao->inner[{idx}](stale/erro)")
                    continue
    except TimeoutException as e:
        last_timeout = e
    finally:
        _safe_default()

    msg = (
        f"Timeout ao localizar 'processo.abrir_todas_as_pastas' em nenhum contexto. "
        f"Contextos tentados={tried_contexts}. XPaths={xpaths}"
    )
    raise TimeoutException(msg) from last_timeout


def click_pesquisar_no_processo(driver: Any, selectors: Any, logger: Any) -> None:
    """
    Em algumas variações do SEI, o botão/link "Pesquisar no Processo" não está no TOP.
    Esta função tenta clicar em múltiplos contextos (TOP, ifrConteudoVisualizacao,
    iframes internos e, por fim, ifrArvore) antes de falhar.
    """
    xpaths = _get_selector_candidates(selectors, "processo.pesquisar_no_processo")

    def _safe_default() -> None:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

    def _safe_switch_frame_by_id_or_name(frame_id: str) -> bool:
        for by, value in ((By.ID, frame_id), (By.NAME, frame_id)):
            try:
                _safe_default()
                iframe = driver.find_element(by, value)
                driver.switch_to.frame(iframe)
                return True
            except WebDriverException:
                continue
        return False

    def _attempt_click_in_current_context(context_label: str, timeout_seconds: float) -> bool:
        """
        Tenta clicar no contexto atual. Retorna True se clicou, False se não achou.
        """
        try:
            clicked_xpath = click_xpath_with_retry(
                driver,
                xpaths,
                "processo.pesquisar_no_processo",
                default_timeout_seconds=int(max(1, _resolve_timeout(driver, None))),
                timeout_seconds=timeout_seconds,
            )
            logger.info(
                "Pesquisar no Processo: clique realizado (%s) [contexto=%s]",
                clicked_xpath,
                context_label,
            )
            return True
        except TimeoutException:
            return False

    tried_contexts: List[str] = []
    last_timeout: TimeoutException | None = None

    # 1) TOP
    _safe_default()
    try:
        if _attempt_click_in_current_context("TOP", timeout_seconds=2.5):
            return
        tried_contexts.append("TOP")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("TOP")

    # 2) Iframe principal de conteúdo: ifrConteudoVisualizacao
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
            if _attempt_click_in_current_context("ifrConteudoVisualizacao", timeout_seconds=3.5):
                _safe_default()
                return
            tried_contexts.append("ifrConteudoVisualizacao")
        else:
            tried_contexts.append("ifrConteudoVisualizacao(não encontrado)")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("ifrConteudoVisualizacao(timeout)")

    # 3) Iframes internos dentro de ifrConteudoVisualizacao (variações de layout do SEI)
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
            inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
            # limita a varredura pra não virar algo pesado/infinito
            for idx in range(min(len(inner_iframes), 10)):
                try:
                    _safe_default()
                    if not _safe_switch_frame_by_id_or_name("ifrConteudoVisualizacao"):
                        break
                    inner_iframes2 = driver.find_elements(By.TAG_NAME, "iframe")
                    if idx >= len(inner_iframes2):
                        break

                    driver.switch_to.frame(inner_iframes2[idx])
                    if _attempt_click_in_current_context(f"ifrConteudoVisualizacao->inner[{idx}]", timeout_seconds=2.5):
                        _safe_default()
                        return
                    tried_contexts.append(f"ifrConteudoVisualizacao->inner[{idx}]")
                except (StaleElementReferenceException, WebDriverException):
                    tried_contexts.append(f"ifrConteudoVisualizacao->inner[{idx}](stale/erro)")
                    continue
    except TimeoutException as e:
        last_timeout = e

    # 4) Fallback: ifrArvore (menos comum para toolbar, mas pode variar por perfil/layout)
    _safe_default()
    try:
        if _safe_switch_frame_by_id_or_name("ifrArvore"):
            if _attempt_click_in_current_context("ifrArvore", timeout_seconds=2.5):
                _safe_default()
                return
            tried_contexts.append("ifrArvore")
        else:
            tried_contexts.append("ifrArvore(não encontrado)")
    except TimeoutException as e:
        last_timeout = e
        tried_contexts.append("ifrArvore(timeout)")
    finally:
        _safe_default()

    # Se chegou aqui, não encontrou em nenhum contexto.
    # Mantém a mensagem original (XPaths tentados) e adiciona contextos.
    msg = (
        f"Timeout ao localizar 'processo.pesquisar_no_processo' em nenhum contexto. "
        f"Contextos tentados={tried_contexts}. XPaths={xpaths}"
    )
    raise TimeoutException(msg) from last_timeout


def wait_pesquisa_anchor(
    driver: Any,
    selectors: Any,
    logger: Any,
    timeout: int | float | None = None,
) -> None:
    x_iframe_visualizacao = selectors.get("processo.iframe_visualizacao")
    x_pesquisa_anchor = str(selectors.require("processo.pesquisa_anchor"))
    timeout_seconds = max(10.0, min(20.0, _resolve_timeout(driver, timeout)))

    def _safe_default() -> None:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        _safe_default()

        # 1) Se por algum motivo abriu fora de iframe (raro), a URL confirma
        try:
            if "procedimento_pesquisar" in (driver.current_url or ""):
                logger.info("filtro aberto (anchor ok: url)")
                return
        except WebDriverException:
            pass

        # 2) Se o anchor existir no TOP (algumas variações), confirma
        try:
            if driver.find_elements(By.XPATH, x_pesquisa_anchor):
                logger.info("filtro aberto (anchor ok: top)")
                return
        except WebDriverException:
            pass

        # 3) Monta lista de iframes candidatos (iframe específico + todos)
        iframe_candidates: List[Any] = []
        if x_iframe_visualizacao:
            try:
                iframe_candidates.extend(driver.find_elements(By.XPATH, str(x_iframe_visualizacao)))
            except WebDriverException:
                pass

        try:
            iframe_candidates.extend(driver.find_elements(By.TAG_NAME, "iframe"))
        except WebDriverException:
            pass

        # 4) Varre iframes top-level e 1 nível interno
        for iframe_elem in iframe_candidates:
            try:
                _safe_default()
                # Checa src no top-level
                try:
                    iframe_src = (iframe_elem.get_attribute("src") or "").lower()
                except WebDriverException:
                    iframe_src = ""

                if "procedimento_pesquisar" in iframe_src:
                    logger.info("filtro aberto (anchor ok: iframe src)")
                    return

                # Entra no iframe e procura anchor
                try:
                    driver.switch_to.frame(iframe_elem)
                except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
                    continue

                try:
                    if driver.find_elements(By.XPATH, x_pesquisa_anchor):
                        logger.info("filtro aberto (anchor ok: dentro do iframe)")
                        return
                except WebDriverException:
                    pass

                # 1 nível interno: iframes dentro deste iframe
                try:
                    inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                except WebDriverException:
                    inner_iframes = []

                for inner in inner_iframes[:10]:
                    try:
                        try:
                            inner_src = (inner.get_attribute("src") or "").lower()
                        except WebDriverException:
                            inner_src = ""

                        if "procedimento_pesquisar" in inner_src:
                            logger.info("filtro aberto (anchor ok: inner iframe src)")
                            return

                        try:
                            driver.switch_to.frame(inner)
                        except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
                            # volta pro iframe pai e continua
                            try:
                                driver.switch_to.parent_frame()
                            except WebDriverException:
                                pass
                            continue

                        try:
                            if driver.find_elements(By.XPATH, x_pesquisa_anchor):
                                logger.info("filtro aberto (anchor ok: dentro do inner iframe)")
                                return
                        except WebDriverException:
                            pass
                        finally:
                            # volta pro iframe pai
                            try:
                                driver.switch_to.parent_frame()
                            except WebDriverException:
                                pass
                    except WebDriverException:
                        continue

            finally:
                _safe_default()

        time.sleep(0.2)

    raise RuntimeError("Tela de filtro/pesquisa nao confirmou abertura (anchor/url)")
