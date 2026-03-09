from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Dict, List

from selenium.common.exceptions import NoSuchFrameException, WebDriverException
from selenium.webdriver.common.by import By


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    if logger is None:
        return
    try:
        log_fn = getattr(logger, level, None)
        if callable(log_fn):
            log_fn(msg, *args)
    except Exception:
        return


def _truncate(value: str, limit: int = 240) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _describe_iframe(frame: Any, idx: int) -> str:
    try:
        frame_id = (frame.get_attribute("id") or "").strip() or "-"
        frame_name = (frame.get_attribute("name") or "").strip() or "-"
        frame_src = (frame.get_attribute("src") or "").strip() or "-"
    except WebDriverException:
        frame_id = "-"
        frame_name = "-"
        frame_src = "-"

    if len(frame_src) > 180:
        frame_src = f"{frame_src[:180]}..."
    return f"#{idx}(id={frame_id}, name={frame_name}, src={frame_src})"


def _log_iframe_inventory(driver: Any, logger: Any, context: str) -> None:
    try:
        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
    except WebDriverException as exc:
        _log(logger, "warning", "%s: falha ao listar iframes (%s).", context, exc)
        return

    if not iframes:
        _log(logger, "info", "%s: nenhum iframe encontrado no contexto atual.", context)
        return

    descriptions: List[str] = []
    for idx, frame in enumerate(iframes):
        descriptions.append(_describe_iframe(frame, idx))
    _log(logger, "info", "%s: iframes detectados=%d %s", context, len(descriptions), descriptions)


def get_visualizacao_iframe(driver: Any, logger: Any = None) -> Any:
    """Retorna o iframe de visualizacao do documento (ifrVisualizacao)."""
    tried: List[str] = []
    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    try:
        direct_xpath = "//iframe[@id='ifrVisualizacao' or @name='ifrVisualizacao']"
        direct_matches = driver.find_elements(By.XPATH, direct_xpath)
        if direct_matches:
            _log(
                logger,
                "info",
                "Visualizacao: iframe encontrado por id/name (%s).",
                _describe_iframe(direct_matches[0], 0),
            )
            return direct_matches[0]

        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if len(iframes) > 1:
            _log(
                logger,
                "info",
                "Visualizacao: %d iframes detectados; iniciando tentativa por src.",
                len(iframes),
            )

        for idx, frame in enumerate(iframes):
            description = _describe_iframe(frame, idx)
            tried.append(description)
            src = (frame.get_attribute("src") or "").lower()
            if "acao=documento_visualizar" in src or "documento_visualizar" in src:
                _log(logger, "info", "Visualizacao: iframe encontrado por src (%s).", description)
                return frame
    except WebDriverException as exc:
        _log(logger, "warning", "Visualizacao: falha ao localizar iframe (%s).", exc)
        return None

    _log_iframe_inventory(driver, logger, "Visualizacao: inventario de iframes antes de falha")
    _log(logger, "warning", "Visualizacao: iframe nao encontrado. Tentativas=%s", tried)
    return None


def _switch_to_visualizacao_iframe_once(driver: Any, logger: Any = None) -> bool:
    iframe = get_visualizacao_iframe(driver, logger=logger)
    if iframe is None:
        _log(logger, "info", "Visualizacao: iniciando fallback de busca em iframes aninhados.")
        try:
            driver.switch_to.default_content()
            parent_candidates = driver.find_elements(
                By.XPATH,
                "//iframe[@id='ifrConteudoVisualizacao' or @name='ifrConteudoVisualizacao']",
            )
            if not parent_candidates:
                parent_candidates = driver.find_elements(By.TAG_NAME, "iframe")
        except WebDriverException as exc:
            _log(logger, "warning", "Visualizacao: falha ao listar iframes de fallback (%s).", exc)
            return False

        for pidx in range(len(parent_candidates)):
            try:
                driver.switch_to.default_content()
                parents_now = driver.find_elements(
                    By.XPATH,
                    "//iframe[@id='ifrConteudoVisualizacao' or @name='ifrConteudoVisualizacao']",
                )
                if not parents_now:
                    parents_now = driver.find_elements(By.TAG_NAME, "iframe")
                if pidx >= len(parents_now):
                    continue

                parent = parents_now[pidx]
                _log(
                    logger,
                    "info",
                    "Visualizacao fallback: testando parent iframe %s",
                    _describe_iframe(parent, pidx),
                )
                driver.switch_to.frame(parent)
                deadline_inner = time.time() + 2.5
                inner_candidates: List[Any] = []
                while time.time() < deadline_inner:
                    inner_candidates = driver.find_elements(By.XPATH, "//iframe[@id='ifrVisualizacao' or @name='ifrVisualizacao']")
                    if not inner_candidates:
                        inner_candidates = driver.find_elements(
                            By.XPATH,
                            "//iframe[contains(@src,'documento_visualizar') or contains(@src,'acao=documento_visualizar')]",
                        )
                    if inner_candidates:
                        break
                    time.sleep(0.15)
                if inner_candidates:
                    driver.switch_to.frame(inner_candidates[0])
                    _log(
                        logger,
                        "info",
                        "Visualizacao fallback: switch OK para iframe interno %s",
                        _describe_iframe(inner_candidates[0], 0),
                    )
                    return True
            except (NoSuchFrameException, WebDriverException) as exc:
                _log(logger, "debug", "Visualizacao fallback: erro no parent idx=%d (%s).", pidx, exc)
                continue

        return False
    try:
        driver.switch_to.default_content()
        driver.switch_to.frame(iframe)
        _log(logger, "info", "Visualizacao: contexto alterado para iframe de visualizacao com sucesso.")
        return True
    except (NoSuchFrameException, WebDriverException) as exc:
        _log(logger, "warning", "Visualizacao: erro no switch_to.frame (%s).", exc)
        return False


def _switch_to_visualizacao_iframe(
    driver: Any,
    logger: Any = None,
    timeout_seconds: float = 10.0,
) -> bool:
    deadline = time.time() + max(1.0, timeout_seconds)
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        if _switch_to_visualizacao_iframe_once(driver, logger=logger):
            if attempt > 1:
                _log(logger, "info", "Visualizacao: switch concluido apos %d tentativa(s).", attempt)
            return True
        time.sleep(0.25)

    _log(logger, "warning", "Visualizacao: nao foi possivel entrar no iframe de visualizacao em %.1fs.", timeout_seconds)
    return False


def extract_body_text_from_visualizacao(driver: Any, logger: Any = None) -> str:
    text = ""
    try:
        if not _switch_to_visualizacao_iframe(driver, logger=logger):
            return text
        body_text = driver.execute_script(
            "return (document && document.body) ? (document.body.innerText || '') : '';"
        )
        if isinstance(body_text, str):
            text = body_text
    except WebDriverException as exc:
        _log(logger, "warning", "Visualizacao: falha ao extrair body.innerText (%s).", exc)
    finally:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass
    return text


def extract_tables_from_visualizacao(
    driver: Any,
    logger: Any = None,
) -> List[Dict[str, List[List[str]]]]:
    tables: List[Dict[str, List[List[str]]]] = []
    try:
        if not _switch_to_visualizacao_iframe(driver, logger=logger):
            return tables
        raw_tables = driver.execute_script(
            """
            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            return Array.from(document.querySelectorAll('table')).map((table) => ({
              rows: Array.from(table.querySelectorAll('tr')).map((tr) =>
                Array.from(tr.querySelectorAll('th,td')).map((cell) => normalize(cell.innerText))
              )
            }));
            """
        )
        if isinstance(raw_tables, list):
            for table in raw_tables:
                rows: List[List[str]] = []
                raw_rows = table.get("rows", []) if isinstance(table, dict) else []
                if isinstance(raw_rows, list):
                    for row in raw_rows:
                        if isinstance(row, list):
                            rows.append([str(cell) if cell is not None else "" for cell in row])
                tables.append({"rows": rows})
    except WebDriverException as exc:
        _log(logger, "warning", "Visualizacao: falha ao extrair tabelas (%s).", exc)
    finally:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass
    return tables


def _extract_tables_in_current_context(driver: Any) -> List[Dict[str, List[List[str]]]]:
    tables: List[Dict[str, List[List[str]]]] = []
    raw_tables = driver.execute_script(
        """
        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
        return Array.from(document.querySelectorAll('table')).map((table) => ({
          rows: Array.from(table.querySelectorAll('tr')).map((tr) =>
            Array.from(tr.querySelectorAll('th,td')).map((cell) => normalize(cell.innerText))
          )
        }));
        """
    )
    if isinstance(raw_tables, list):
        for table in raw_tables:
            rows: List[List[str]] = []
            raw_rows = table.get("rows", []) if isinstance(table, dict) else []
            if isinstance(raw_rows, list):
                for row in raw_rows:
                    if isinstance(row, list):
                        rows.append([str(cell) if cell is not None else "" for cell in row])
            tables.append({"rows": rows})
    return tables


def _looks_like_placeholder_text(text: str) -> bool:
    normalized = " ".join((text or "").split()).casefold()
    markers = (
        "clique aqui para visualizar o conteudo deste documento em uma nova janela",
        "clique aqui para visualizar o conteúdo deste documento em uma nova janela",
    )
    return any(marker in normalized for marker in markers)


def extract_document_snapshot(driver: Any, logger: Any = None) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "text": "",
        "tables": [],
        "url": "",
        "title": "",
    }
    try:
        _log(
            logger,
            "info",
            "Snapshot PT: iniciando extracao. window_url=%s title=%s",
            getattr(driver, "current_url", ""),
            getattr(driver, "title", ""),
        )
        if not _switch_to_visualizacao_iframe(driver, logger=logger):
            _log(logger, "warning", "Snapshot PT: extracao abortada; iframe de visualizacao indisponivel.")
            return snapshot

        snapshot["url"] = str(driver.execute_script("return window.location.href || '';") or "")
        snapshot["title"] = str(driver.execute_script("return document.title || '';") or "")
        snapshot["text"] = str(
            driver.execute_script(
                "return (document && document.body) ? (document.body.innerText || '') : '';"
            )
            or ""
        )
        if not snapshot["text"].strip() or _looks_like_placeholder_text(snapshot["text"]):
            _log(
                logger,
                "info",
                "Snapshot PT: conteudo inicial vazio/intermediario (chars=%d). Aguardando renderizacao final.",
                len(snapshot["text"]),
            )
            refresh_deadline = time.time() + 8.0
            while time.time() < refresh_deadline:
                time.sleep(0.4)
                if not _switch_to_visualizacao_iframe(driver, logger=logger, timeout_seconds=2.5):
                    continue
                snapshot["url"] = str(driver.execute_script("return window.location.href || '';") or "")
                snapshot["title"] = str(driver.execute_script("return document.title || '';") or "")
                snapshot["text"] = str(
                    driver.execute_script(
                        "return (document && document.body) ? (document.body.innerText || '') : '';"
                    )
                    or ""
                )
                if snapshot["text"].strip() and not _looks_like_placeholder_text(snapshot["text"]):
                    _log(
                        logger,
                        "info",
                        "Snapshot PT: renderizacao final detectada apos espera (chars=%d).",
                        len(snapshot["text"]),
                    )
                    break

        snapshot["tables"] = _extract_tables_in_current_context(driver)
        _log(
            logger,
            "info",
            "Snapshot PT: concluido. iframe_url=%s iframe_title=%s text_chars=%d tables=%d text_preview=%s",
            snapshot["url"],
            snapshot["title"],
            len(snapshot["text"]),
            len(snapshot["tables"]) if isinstance(snapshot["tables"], list) else 0,
            _truncate(snapshot["text"], 280),
        )
    except WebDriverException as exc:
        _log(logger, "warning", "Visualizacao: falha ao montar snapshot (%s).", exc)
    finally:
        try:
            driver.switch_to.default_content()
        except WebDriverException:
            pass
    return snapshot


def _extract_label_value(text: str, label_regex: str) -> str:
    if not text:
        return ""

    patterns = [
        rf"(?is)\b(?:{label_regex})\s*\(?(?:m[eê]s\s*/\s*ano)?\)?\s*[:\-]\s*(.+?)(?=\b(?:in[ií]cio|t[eé]rmino)\b\s*\(?(?:m[eê]s\s*/\s*ano)?\)?\s*[:\-]|$)",
        rf"(?im)\b(?:{label_regex})\s*\(?(?:m[eê]s\s*/\s*ano)?\)?\s*[:\-]\s*(.+)$",
    ]
    candidates = [text]
    fixed = _maybe_fix_mojibake(text)
    if fixed != text:
        candidates.append(fixed)

    for candidate in candidates:
        for pattern in patterns:
            match = re.search(pattern, candidate)
            if not match:
                continue
            value = " ".join((match.group(1) or "").replace("\r", "\n").split()).strip(" ;,.")
            if value:
                return value
    return ""


def _parse_possible_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"\s+", "", raw).replace(".", "/").replace("-", "/")

    dmy_match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if dmy_match:
        day = int(dmy_match.group(1))
        month = int(dmy_match.group(2))
        year = int(dmy_match.group(3))
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return ""

    my_match = re.fullmatch(r"(\d{1,2})/(\d{4})", raw)
    if my_match:
        month = int(my_match.group(1))
        year = int(my_match.group(2))
        try:
            return datetime(year, month, 1).date().isoformat()
        except ValueError:
            return ""

    return ""


def _extract_first_date_token(value: str) -> str:
    raw = " ".join((value or "").replace("\r", "\n").split()).strip()
    if not raw:
        return ""

    candidates = [raw]
    fixed = _maybe_fix_mojibake(raw)
    if fixed != raw:
        candidates.append(fixed)

    patterns = (
        r"\b\d{1,2}\s*[\/\-.]\s*\d{1,2}\s*[\/\-.]\s*\d{4}\b",
        r"\b\d{1,2}\s*[\/\-.]\s*\d{4}\b",
    )
    for candidate in candidates:
        for pattern in patterns:
            match = re.search(pattern, candidate)
            if match:
                return " ".join(match.group(0).split())
    return ""


def _maybe_fix_mojibake(value: str) -> str:
    text = value or ""
    if not text:
        return text
    if not any(marker in text for marker in ("Ã", "Â", "\ufffd")):
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except UnicodeError:
        return text


def parse_prazos(text: str, logger: Any = None) -> Dict[str, str]:
    result = {
        "status": "nao_encontrado",
        "inicio_data": "",
        "inicio_raw": "",
        "termino_data": "",
        "termino_raw": "",
    }
    try:
        base_text = text or ""
        _log(logger, "info", "Parse prazos: iniciando com text_chars=%d", len(base_text))
        inicio_value = _extract_label_value(base_text, r"in[ií]cio")
        termino_value = _extract_label_value(base_text, r"t[eé]rmino")
        _log(
            logger,
            "info",
            "Parse prazos: valores brutos extraidos inicio='%s' termino='%s'",
            _truncate(inicio_value, 120),
            _truncate(termino_value, 120),
        )
        if not inicio_value and not termino_value:
            _log(logger, "warning", "Parse prazos: nenhum marcador de inicio/termino encontrado.")
            return result

        if inicio_value:
            inicio_token = _extract_first_date_token(inicio_value)
            inicio_iso = _parse_possible_date(inicio_token or inicio_value)
            if inicio_iso:
                result["inicio_data"] = inicio_iso
            else:
                result["inicio_raw"] = inicio_value

        if termino_value:
            termino_token = _extract_first_date_token(termino_value)
            termino_iso = _parse_possible_date(termino_token or termino_value)
            if termino_iso:
                result["termino_data"] = termino_iso
            else:
                result["termino_raw"] = termino_value

        result["status"] = "encontrado" if (inicio_value and termino_value) else "parcial"
        _log(logger, "info", "Parse prazos: resultado final=%s", result)
    except Exception:
        _log(logger, "exception", "Parse prazos: erro inesperado durante parse.")
        return result
    return result
