from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Dict, List, Optional

from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchFrameException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from app.rpa.performance_profiler import profiler_sleep

ELEMENT_LOOKUP_POLL_SECONDS = 0.25
CLICK_RETRY_POLL_SECONDS = 0.15


@dataclass(frozen=True)
class UIContextHint:
    context_label: str
    window_handle: str = ""
    root_frame_id: str = ""
    root_frame_name: str = ""
    root_frame_index: int | None = None
    inner_frame_id: str = ""
    inner_frame_name: str = ""
    inner_frame_index: int | None = None


def _safe_current_window_handle(driver: Any) -> str:
    try:
        return str(getattr(driver, "current_window_handle", "") or "")
    except WebDriverException:
        return ""


def _ui_context_store(driver: Any) -> Dict[str, UIContextHint]:
    store = getattr(driver, "_sei_ui_context_hints", None)
    if isinstance(store, dict):
        return store
    store = {}
    setattr(driver, "_sei_ui_context_hints", store)
    return store


def get_ui_context_hint(driver: Any, key: str) -> UIContextHint | None:
    hint = _ui_context_store(driver).get(key)
    if not isinstance(hint, UIContextHint):
        return None
    current_handle = _safe_current_window_handle(driver)
    if hint.window_handle and current_handle and hint.window_handle != current_handle:
        return None
    return hint


def remember_ui_context_hint(driver: Any, key: str, hint: UIContextHint) -> None:
    _ui_context_store(driver)[key] = hint


def clear_ui_context_hint(driver: Any, key: str) -> None:
    _ui_context_store(driver).pop(key, None)


def _find_frame_in_current_context(
    driver: Any,
    *,
    frame_id: str = "",
    frame_name: str = "",
    frame_index: int | None = None,
) -> Any | None:
    for by, value in ((By.ID, frame_id), (By.NAME, frame_name)):
        if not value:
            continue
        try:
            return driver.find_element(by, value)
        except WebDriverException:
            continue

    if frame_index is None or frame_index < 0:
        return None

    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
    except WebDriverException:
        return None
    if frame_index >= len(frames):
        return None
    return frames[frame_index]


def switch_to_ui_context_hint(driver: Any, hint: UIContextHint) -> bool:
    try:
        driver.switch_to.default_content()
    except WebDriverException:
        return False

    if hint.context_label == "TOP":
        return True

    root_frame = _find_frame_in_current_context(
        driver,
        frame_id=hint.root_frame_id,
        frame_name=hint.root_frame_name,
        frame_index=hint.root_frame_index,
    )
    if root_frame is None:
        return False

    try:
        driver.switch_to.frame(root_frame)
    except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
        return False

    if not (hint.inner_frame_id or hint.inner_frame_name or hint.inner_frame_index is not None):
        return True

    inner_frame = _find_frame_in_current_context(
        driver,
        frame_id=hint.inner_frame_id,
        frame_name=hint.inner_frame_name,
        frame_index=hint.inner_frame_index,
    )
    if inner_frame is None:
        return False

    try:
        driver.switch_to.frame(inner_frame)
    except (NoSuchFrameException, StaleElementReferenceException, WebDriverException):
        return False
    return True


def get_iframes_info(driver: Any) -> List[Dict[str, Any]]:
    frames: List[Dict[str, Any]] = []
    try:
        elems = driver.find_elements(By.TAG_NAME, "iframe")
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


def get_ready_state(driver: Any) -> str:
    try:
        value = driver.execute_script("return document.readyState")
    except WebDriverException:
        return "unavailable"
    return str(value) if value else "unknown"


def wait_for_document_ready(driver: Any, timeout: int, tag: str, logger: Any) -> None:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException as exc:
        logger.error("Timeout aguardando readyState=complete (%s)", tag)
        raise exc


def log_iframe_hint(driver: Any, logger: Any, context: str) -> List[Dict[str, Any]]:
    frames = get_iframes_info(driver)
    if frames:
        logger.error(
            "%s: encontrados %d iframe(s). O XPath pode estar em outro contexto.",
            context,
            len(frames),
        )
    return frames


def wait_for_elements(
    driver: Any,
    logger: Any,
    xpath: str,
    tag: str,
    timeout_seconds: int,
    restore_context: bool = True,
) -> List[Any]:
    wait_for_document_ready(driver, timeout_seconds, tag, logger)
    deadline = time.time() + timeout_seconds
    iframe_count_logged = False

    try:
        while time.time() < deadline:
            driver.switch_to.default_content()
            elems = driver.find_elements(By.XPATH, xpath)
            if elems:
                return elems

            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes and not iframe_count_logged:
                logger.info(
                    "wait_for_elements(%s): fallback em %d iframe(s)",
                    tag,
                    len(iframes),
                )
                iframe_count_logged = True

            for idx in range(len(iframes)):
                if time.time() >= deadline:
                    break

                try:
                    driver.switch_to.default_content()
                    current_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if idx >= len(current_iframes):
                        continue

                    frame = current_iframes[idx]
                    frame_id = frame.get_attribute("id")
                    frame_name = frame.get_attribute("name")
                    frame_src = frame.get_attribute("src")

                    logger.debug(
                        "wait_for_elements(%s): tentando iframe[%d] id=%s name=%s src=%s",
                        tag,
                        idx,
                        frame_id,
                        frame_name,
                        frame_src,
                    )

                    driver.switch_to.frame(frame)
                    elems = driver.find_elements(By.XPATH, xpath)
                    if elems:
                        return elems
                except (StaleElementReferenceException, NoSuchFrameException, WebDriverException) as frame_exc:
                    logger.debug(
                        "wait_for_elements(%s): iframe[%d] indisponivel/stale (%s)",
                        tag,
                        idx,
                        frame_exc,
                    )
                    continue

            # Reduce idle time between iframe scans without changing the overall timeout.
            profiler_sleep(min(ELEMENT_LOOKUP_POLL_SECONDS, max(0.0, deadline - time.time())))

        frames = log_iframe_hint(driver, logger, f"wait_for_elements falhou ({tag})")
        logger.error(
            "Timeout aguardando elementos: tag=%s xpath=%s timeout=%ss",
            tag,
            xpath,
            timeout_seconds,
        )
        logger.error("Contexto: iframe_count=%d url=%s", len(frames), driver.current_url)
        raise TimeoutException(
            f"Timeout aguardando elementos: tag={tag} xpath={xpath} timeout={timeout_seconds}s"
        )
    finally:
        if restore_context:
            try:
                driver.switch_to.default_content()
            except WebDriverException:
                pass


def wait_for_clickable(
    driver: Any,
    logger: Any,
    xpath: str,
    tag: str,
    timeout_seconds: int,
) -> Any:
    wait_for_document_ready(driver, timeout_seconds, tag, logger)
    try:
        return WebDriverWait(driver, timeout_seconds).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
    except TimeoutException as exc:
        frames = log_iframe_hint(driver, logger, f"wait_for_clickable falhou ({tag})")
        logger.error(
            "Timeout aguardando clique: tag=%s xpath=%s timeout=%ss",
            tag,
            xpath,
            timeout_seconds,
        )
        logger.error("Contexto: iframe_count=%d url=%s", len(frames), driver.current_url)
        raise exc


def click_xpath_with_retry(
    driver: Any,
    xpaths: List[str],
    label: str,
    default_timeout_seconds: int,
    timeout_seconds: Optional[float] = None,
) -> str:
    deadline = time.time() + (timeout_seconds or max(8.0, min(15.0, float(default_timeout_seconds))))
    last_error: Optional[BaseException] = None
    tried: List[str] = []

    while time.time() < deadline:
        for xpath in xpaths:
            if xpath not in tried:
                tried.append(xpath)
            try:
                elems = driver.find_elements(By.XPATH, xpath)
                if not elems:
                    continue
                elem = elems[0]
                try:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
                        elem,
                    )
                except WebDriverException:
                    pass
                try:
                    elem.click()
                except (ElementClickInterceptedException, WebDriverException):
                    driver.execute_script("arguments[0].click();", elem)
                return xpath
            except (StaleElementReferenceException, WebDriverException) as exc:
                last_error = exc
                continue
        # Shorter click retry polling keeps the same deadline while reducing fixed wait cost.
        profiler_sleep(CLICK_RETRY_POLL_SECONDS)

    if last_error:
        raise RuntimeError(
            f"Falha ao clicar '{label}' apos tentativas. XPaths={tried} erro={last_error}"
        ) from last_error
    raise TimeoutException(f"Timeout ao localizar '{label}'. XPaths={tried}")
