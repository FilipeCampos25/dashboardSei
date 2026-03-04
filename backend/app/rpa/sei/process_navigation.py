from __future__ import annotations

from typing import Any, Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from app.rpa.selenium_utils import log_iframe_hint as selenium_log_iframe_hint
from app.rpa.selenium_utils import wait_for_elements as selenium_wait_for_elements


def open_processo(driver: Any, processo_text: str, selectors: Any, logger: Any) -> tuple[str, str, str]:
    sel = selectors.get("interno", {})
    x = sel.get("processo")
    if not x:
        raise RuntimeError("Seletor interno.processo ausente em xpath_selector.json")

    timeout_seconds = getattr(driver, "_sei_timeout_seconds", 10)
    elems = selenium_wait_for_elements(
        driver,
        logger,
        x,
        "open_processo_list",
        timeout_seconds=timeout_seconds,
        restore_context=True,
    )
    for elem in elems:
        if (elem.text or "").strip() == processo_text:
            handles_before = set(driver.window_handles)
            elem.click()

            try:
                WebDriverWait(driver, timeout_seconds).until(
                    lambda d: len(set(d.window_handles) - handles_before) > 0
                )
            except TimeoutException as exc:
                frames = selenium_log_iframe_hint(driver, logger, "Timeout aguardando nova janela do processo")
                logger.error("Contexto: iframe_count=%d url=%s", len(frames), driver.current_url)
                raise RuntimeError(
                    f"Nao abriu nova janela para o processo: {processo_text}"
                ) from exc

            handles_after = set(driver.window_handles)
            new_handles = list(handles_after - handles_before)
            if not new_handles:
                raise RuntimeError(
                    f"Nao identifiquei o handle da nova janela do processo: {processo_text}"
                )

            new_handle = new_handles[0]
            driver.switch_to.window(new_handle)
            url = driver.current_url
            title = driver.title
            logger.info(
                "Nova janela do processo aberta. handle=%s url=%s title=%s",
                new_handle,
                url,
                title,
            )
            return new_handle, url, title

    frames = selenium_log_iframe_hint(driver, logger, "Nao consegui localizar o processo para abrir")
    logger.error("Contexto: iframe_count=%d url=%s", len(frames), driver.current_url)
    raise RuntimeError(f"Nao consegui abrir o processo: {processo_text}")


def close_current_tab_and_back(
    driver: Any,
    logger: Any,
    preferred_handle: Optional[str] = None,
) -> Optional[str]:
    try:
        before_handles = list(driver.window_handles)
        current_handle = driver.current_window_handle
        logger.info(
            "Fechar aba: handles antes=%d atual=%s preferido=%s",
            len(before_handles),
            current_handle,
            preferred_handle,
        )

        if len(before_handles) > 1:
            driver.close()
            remaining = list(driver.window_handles)
            target_handle: Optional[str] = None

            if preferred_handle and preferred_handle in remaining:
                target_handle = preferred_handle
            elif remaining:
                target_handle = remaining[0]

            if target_handle:
                driver.switch_to.window(target_handle)
                logger.info(
                    "Fechar aba: retornou para handle=%s (restantes=%d)",
                    target_handle,
                    len(remaining),
                )
                return target_handle

            return None

        if preferred_handle and current_handle != preferred_handle and preferred_handle in before_handles:
            driver.switch_to.window(preferred_handle)
            logger.info(
                "Fechar aba: contexto corrigido para handle preferido=%s.",
                preferred_handle,
            )
            return preferred_handle

        return current_handle
    except Exception as exc:
        logger.exception("Falha ao fechar aba/voltar: %s", exc)
        return None
