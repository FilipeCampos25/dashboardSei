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
    xpaths = _get_selector_candidates(selectors, "processo.abrir_todas_as_pastas")

    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    clicked_xpath: str | None = None

    try:
        if _switch_to_ifr_arvore_if_present(driver):
            clicked_xpath = click_xpath_with_retry(
                driver,
                xpaths,
                "processo.abrir_todas_as_pastas",
                default_timeout_seconds=int(max(1, _resolve_timeout(driver, None))),
                timeout_seconds=10.0,
            )
        else:
            driver.switch_to.default_content()
            clicked_xpath = click_xpath_with_retry(
                driver,
                xpaths,
                "processo.abrir_todas_as_pastas",
                default_timeout_seconds=int(max(1, _resolve_timeout(driver, None))),
                timeout_seconds=10.0,
            )
    finally:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

    logger.info("Abrir todas as Pastas: clique realizado (%s)", clicked_xpath or "xpath_desconhecido")


def click_pesquisar_no_processo(driver: Any, selectors: Any, logger: Any) -> None:
    xpaths = _get_selector_candidates(selectors, "processo.pesquisar_no_processo")

    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    clicked_xpath = click_xpath_with_retry(
        driver,
        xpaths,
        "processo.pesquisar_no_processo",
        default_timeout_seconds=int(max(1, _resolve_timeout(driver, None))),
        timeout_seconds=10.0,
    )
    logger.info("Pesquisar no Processo: clique realizado (%s)", clicked_xpath)


def wait_pesquisa_anchor(
    driver: Any,
    selectors: Any,
    logger: Any,
    timeout: int | float | None = None,
) -> None:
    x_iframe_visualizacao = selectors.get("processo.iframe_visualizacao")
    x_pesquisa_anchor = str(selectors.require("processo.pesquisa_anchor"))
    timeout_seconds = max(10.0, min(20.0, _resolve_timeout(driver, timeout)))

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass

        try:
            if "procedimento_pesquisar" in (driver.current_url or ""):
                logger.info("filtro aberto (anchor ok)")
                return
        except WebDriverException:
            pass

        try:
            if driver.find_elements(By.XPATH, x_pesquisa_anchor):
                logger.info("filtro aberto (anchor ok)")
                return
        except WebDriverException:
            pass

        iframe_elems: List[Any] = []
        if x_iframe_visualizacao:
            try:
                iframe_elems = driver.find_elements(By.XPATH, str(x_iframe_visualizacao))
            except WebDriverException:
                iframe_elems = []

        if iframe_elems:
            iframe_elem = iframe_elems[0]
            try:
                iframe_src = (iframe_elem.get_attribute("src") or "").lower()
            except WebDriverException:
                iframe_src = ""

            if "procedimento_pesquisar" in iframe_src:
                logger.info("filtro aberto (anchor ok)")
                return

            try:
                driver.switch_to.default_content()
                driver.switch_to.frame(iframe_elem)
                if driver.find_elements(By.XPATH, x_pesquisa_anchor):
                    logger.info("filtro aberto (anchor ok)")
                    return
            except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
                pass
            finally:
                try:
                    driver.switch_to.default_content()
                except WebDriverException:
                    pass

        time.sleep(0.2)

    raise RuntimeError("Tela de filtro/pesquisa nao confirmou abertura (anchor/url)")
