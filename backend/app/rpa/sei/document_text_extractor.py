from __future__ import annotations

import calendar
import io
import os
import re
import tempfile
import time
import unicodedata
import zipfile
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urljoin

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


def _safe_import_requests() -> Any:
    try:
        import requests  # type: ignore

        return requests
    except Exception:
        return None


def _safe_import_pypdf_reader() -> Any:
    try:
        from pypdf import PdfReader  # type: ignore

        return PdfReader
    except Exception:
        pass
    try:
        from PyPDF2 import PdfReader  # type: ignore

        return PdfReader
    except Exception:
        return None


def _safe_import_pdf2image_convert() -> Any:
    try:
        from pdf2image import convert_from_bytes  # type: ignore

        return convert_from_bytes
    except Exception:
        return None


def _safe_import_pytesseract() -> Any:
    try:
        import pytesseract  # type: ignore

        return pytesseract
    except Exception:
        return None


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
        "clique aqui para visualizar o conteãºdo deste documento em uma nova janela",
        "clique aqui para visualizar o conteudo deste documento (",
        "clique aqui para visualizar o conteúdo deste documento (",
        "clique aqui para visualizar o conteãºdo deste documento (",
    )
    return any(marker in normalized for marker in markers)


def _find_download_anchor_url(driver: Any, logger: Any = None) -> str:
    try:
        href = driver.execute_script(
            """
            const byClass = document.querySelector("a.ancoraVisualizacaoArvore[href]");
            if (byClass) return byClass.href || "";
            const byPattern = Array.from(document.querySelectorAll("a[href]"))
              .find((a) => /acao=documento_download_anexo/i.test(a.getAttribute("href") || ""));
            return byPattern ? (byPattern.href || "") : "";
            """
        )
        if isinstance(href, str) and href.strip():
            return href.strip()
    except WebDriverException as exc:
        _log(logger, "warning", "Fallback PDF: falha ao localizar link 'aqui' (%s).", exc)
    return ""


def _write_bytes_temp_pdf(content: bytes) -> str:
    fd, temp_path = tempfile.mkstemp(prefix="sei_pt_", suffix=".pdf")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        raise
    return temp_path


def _download_pdf_with_session(driver: Any, url: str, logger: Any = None) -> Dict[str, Any]:
    requests = _safe_import_requests()
    if requests is None:
        _log(logger, "warning", "Fallback PDF: biblioteca 'requests' indisponivel.")
        return {}

    session = requests.Session()
    try:
        user_agent = ""
        try:
            user_agent = str(driver.execute_script("return navigator.userAgent || '';") or "")
        except Exception:
            user_agent = ""

        if user_agent:
            session.headers.update({"User-Agent": user_agent})
        try:
            referer = str(getattr(driver, "current_url", "") or "")
            if referer:
                session.headers.update({"Referer": referer})
        except Exception:
            pass

        for cookie in driver.get_cookies() or []:
            name = cookie.get("name")
            value = cookie.get("value")
            if not name:
                continue
            session.cookies.set(
                name=name,
                value=value or "",
                domain=cookie.get("domain"),
                path=cookie.get("path") or "/",
            )

        resp = session.get(url, timeout=60, allow_redirects=True)
        content = resp.content or b""
        content_type = (resp.headers.get("Content-Type") or "").lower()
        return {
            "content": content,
            "url": str(getattr(resp, "url", "") or url),
            "content_type": content_type,
            "status_code": int(getattr(resp, "status_code", 0) or 0),
        }
    except Exception as exc:
        _log(logger, "warning", "Fallback PDF: erro no download do anexo (%s).", exc)
        return {}
    finally:
        try:
            session.close()
        except Exception:
            pass


def _extract_pdf_bytes_from_zip(zip_bytes: bytes, logger: Any = None) -> Dict[str, Any]:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                return {}

            pdf_candidates = [n for n in names if n.lower().endswith(".pdf")]
            ordered = pdf_candidates or names
            # Prefere maior arquivo para reduzir risco de pegar metadado pequeno.
            ordered = sorted(
                ordered,
                key=lambda n: (zf.getinfo(n).file_size if n in zf.namelist() else 0),
                reverse=True,
            )
            for name in ordered:
                try:
                    data = zf.read(name)
                except Exception:
                    continue
                if not data:
                    continue
                if data.startswith(b"%PDF"):
                    return {"pdf_bytes": data, "zip_member": name}
            return {}
    except Exception as exc:
        _log(logger, "warning", "Fallback ZIP: falha ao abrir zip (%s).", exc)
        return {}


def _extract_text_from_docx_bytes(docx_bytes: bytes, logger: Any = None) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
            if "word/document.xml" not in zf.namelist():
                return ""
            xml_bytes = zf.read("word/document.xml")
    except Exception as exc:
        _log(logger, "warning", "Fallback DOCX: falha ao abrir DOCX (%s).", exc)
        return ""

    try:
        xml_text = xml_bytes.decode("utf-8", errors="ignore")
        # Remove tags e preserva quebras de parágrafo simples.
        xml_text = re.sub(r"</w:p>", "\n", xml_text, flags=re.IGNORECASE)
        xml_text = re.sub(r"<[^>]+>", "", xml_text)
        xml_text = re.sub(r"\n{3,}", "\n\n", xml_text)
        return xml_text.strip()
    except Exception as exc:
        _log(logger, "warning", "Fallback DOCX: falha ao parsear XML (%s).", exc)
        return ""


def _extract_docx_bytes_from_zip(zip_bytes: bytes, logger: Any = None) -> Dict[str, Any]:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                return {}
            docx_candidates = [n for n in names if n.lower().endswith(".docx")]
            for name in docx_candidates:
                try:
                    data = zf.read(name)
                except Exception:
                    continue
                if not data:
                    continue
                text = _extract_text_from_docx_bytes(data, logger=logger)
                if text:
                    return {"docx_text": text, "zip_member": name}
            return {}
    except Exception as exc:
        _log(logger, "warning", "Fallback ZIP: falha ao inspecionar DOCX no zip (%s).", exc)
        return {}


def _extract_text_from_pdf_native(pdf_content: bytes, logger: Any = None) -> str:
    PdfReader = _safe_import_pypdf_reader()
    if PdfReader is None:
        _log(logger, "info", "Fallback PDF: leitor PDF nativo indisponivel (pypdf/PyPDF2).")
        return ""

    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        chunks: List[str] = []
        for page in getattr(reader, "pages", []) or []:
            try:
                chunks.append((page.extract_text() or "").strip())
            except Exception:
                continue
        return "\n\n".join([c for c in chunks if c])
    except Exception as exc:
        _log(logger, "warning", "Fallback PDF: falha na extracao nativa (%s).", exc)
        return ""


def _extract_text_from_pdf_ocr(pdf_content: bytes, logger: Any = None, ocr_dpi: int = 200) -> str:
    convert_from_bytes = _safe_import_pdf2image_convert()
    pytesseract = _safe_import_pytesseract()
    if convert_from_bytes is None or pytesseract is None:
        _log(
            logger,
            "info",
            "Fallback PDF OCR: dependencias indisponiveis (pdf2image/pytesseract).",
        )
        return ""

    tesseract_cmd = (os.getenv("TESSERACT_CMD") or "").strip()
    poppler_path = (os.getenv("POPPLER_PATH") or "").strip()
    if tesseract_cmd:
        try:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        except Exception:
            pass

    convert_kwargs: Dict[str, Any] = {"dpi": max(72, int(ocr_dpi))}
    if poppler_path:
        convert_kwargs["poppler_path"] = poppler_path

    try:
        images = convert_from_bytes(pdf_content, **convert_kwargs)
    except Exception as exc:
        _log(logger, "warning", "Fallback PDF OCR: falha ao renderizar PDF (%s).", exc)
        return ""

    chunks: List[str] = []
    for image in images:
        text = ""
        for lang in ("por+eng", "por", "eng"):
            try:
                text = (pytesseract.image_to_string(image, lang=lang) or "").strip()
                if text:
                    break
            except Exception:
                continue
        if text:
            chunks.append(text)
    return "\n\n".join(chunks).strip()


def _extract_pdf_text_via_anchor_fallback(
    driver: Any,
    logger: Any = None,
    ocr_dpi: int = 200,
) -> Dict[str, Any]:
    href = _find_download_anchor_url(driver, logger=logger)
    if not href:
        _log(logger, "info", "Fallback PDF: link 'aqui' nao encontrado no DOM atual.")
        return {}

    resolved_url = urljoin(str(getattr(driver, "current_url", "") or ""), href)
    _log(logger, "info", "Fallback PDF: tentando download via link do anexo.")

    downloaded = _download_pdf_with_session(driver, resolved_url, logger=logger)
    if not downloaded:
        return {}

    downloaded_content = downloaded.get("content") or b""
    if not isinstance(downloaded_content, (bytes, bytearray)) or not downloaded_content:
        return {}
    content_type = str(downloaded.get("content_type") or "").lower()

    pdf_content = bytes(downloaded_content)
    source_hint = "response"
    is_pdf = "application/pdf" in content_type or pdf_content.startswith(b"%PDF")
    is_zip = (
        "application/zip" in content_type
        or "application/x-zip-compressed" in content_type
        or pdf_content.startswith(b"PK\x03\x04")
    )

    if not is_pdf and is_zip:
        docx_result = _extract_docx_bytes_from_zip(pdf_content, logger=logger)
        if docx_result:
            text_docx = str(docx_result.get("docx_text") or "").strip()
            source_hint = f"zip:{docx_result.get('zip_member') or '-'}"
            _log(
                logger,
                "info",
                "Fallback ZIP: DOCX interno extraido com sucesso (%s chars=%d).",
                source_hint,
                len(text_docx),
            )
            return {
                "text": text_docx,
                "mode": "zip_docx",
                "source_url": str(downloaded.get("url") or resolved_url),
                "source_hint": source_hint,
            }

        zip_result = _extract_pdf_bytes_from_zip(pdf_content, logger=logger)
        if not zip_result:
            _log(
                logger,
                "warning",
                "Fallback ZIP: nenhum PDF encontrado no zip (bytes=%d).",
                len(pdf_content),
            )
            return {}
        pdf_content = bytes(zip_result.get("pdf_bytes") or b"")
        source_hint = f"zip:{zip_result.get('zip_member') or '-'}"
        _log(logger, "info", "Fallback ZIP: PDF interno selecionado (%s).", source_hint)
        is_pdf = bool(pdf_content.startswith(b"%PDF"))

    if not is_pdf:
        _log(
            logger,
            "warning",
            "Fallback PDF: resposta nao suportada (status=%s content_type=%s bytes=%d).",
            downloaded.get("status_code", "?"),
            content_type,
            len(downloaded_content),
        )
        return {}

    temp_pdf_path = ""
    try:
        # Mantemos um arquivo temporario para facilitar diagnostico/localidade de processamento.
        temp_pdf_path = _write_bytes_temp_pdf(bytes(pdf_content))
    except Exception as exc:
        _log(logger, "warning", "Fallback PDF: falha ao materializar arquivo temporario (%s).", exc)

    text_native = _extract_text_from_pdf_native(bytes(pdf_content), logger=logger).strip()
    if len(text_native) >= 120:
        _log(
            logger,
            "info",
            "Fallback PDF: extracao nativa bem-sucedida (chars=%d).",
            len(text_native),
        )
        if temp_pdf_path:
            try:
                os.remove(temp_pdf_path)
            except Exception:
                pass
        return {
            "text": text_native,
            "mode": "pdf_native",
            "source_url": str(downloaded.get("url") or resolved_url),
            "source_hint": source_hint,
        }

    text_ocr = _extract_text_from_pdf_ocr(bytes(pdf_content), logger=logger, ocr_dpi=ocr_dpi).strip()
    if temp_pdf_path:
        try:
            os.remove(temp_pdf_path)
        except Exception:
            pass

    if text_ocr:
        _log(
            logger,
            "info",
            "Fallback PDF: extracao OCR bem-sucedida (dpi=%d chars=%d).",
            ocr_dpi,
            len(text_ocr),
        )
        return {
            "text": text_ocr,
            "mode": "pdf_ocr",
            "source_url": str(downloaded.get("url") or resolved_url),
            "source_hint": source_hint,
        }

    _log(logger, "warning", "Fallback PDF: nao foi possivel extrair texto do anexo.")
    return {}


def extract_document_snapshot(driver: Any, logger: Any = None) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "text": "",
        "tables": [],
        "url": "",
        "title": "",
        "extraction_mode": "html_dom",
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

        if not snapshot["text"].strip() or _looks_like_placeholder_text(snapshot["text"]):
            fallback = _extract_pdf_text_via_anchor_fallback(driver, logger=logger, ocr_dpi=200)
            if fallback:
                snapshot["text"] = str(fallback.get("text") or "")
                snapshot["tables"] = []
                snapshot["extraction_mode"] = str(fallback.get("mode") or "pdf_fallback")
                if fallback.get("source_url"):
                    snapshot["url"] = str(fallback.get("source_url"))
                _log(
                    logger,
                    "info",
                    "Snapshot PT: fallback aplicado com sucesso (mode=%s chars=%d).",
                    snapshot["extraction_mode"],
                    len(snapshot["text"]),
                )
                return snapshot

        snapshot["tables"] = _extract_tables_in_current_context(driver)
        _log(
            logger,
            "info",
            "Snapshot PT: concluido. mode=%s iframe_url=%s iframe_title=%s text_chars=%d tables=%d text_preview=%s",
            snapshot["extraction_mode"],
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
        rf"(?is)\b(?:{label_regex})\s*\(?(?:m(?:e|ê|Ãª)s\s*/\s*ano)?\)?\s*[:\-]\s*(.+?)(?=\b(?:in(?:i|í|Ã­)cio|t(?:e|é|Ã©)rmino)\b\s*\(?(?:m(?:e|ê|Ãª)s\s*/\s*ano)?\)?\s*[:\-]|$)",
        rf"(?im)\b(?:{label_regex})\s*\(?(?:m(?:e|ê|Ãª)s\s*/\s*ano)?\)?\s*[:\-]\s*(.+)$",
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


def _normalize_date_text(value: str) -> str:
    text = " ".join((value or "").replace("\r", "\n").split()).strip()
    if not text:
        return ""
    text = _maybe_fix_mojibake(text).replace("º", "").replace("°", "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    return re.sub(r"\s+", " ", text).strip()


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _coerce_year(year_raw: str) -> int:
    year = int(year_raw)
    if year < 100:
        return 2000 + year
    return year


def _month_from_name(month_raw: str) -> int:
    month_key = _normalize_date_text(month_raw).replace(".", "")
    months = {
        "jan": 1,
        "janeiro": 1,
        "fev": 2,
        "fevereiro": 2,
        "mar": 3,
        "marco": 3,
        "abr": 4,
        "abril": 4,
        "mai": 5,
        "maio": 5,
        "jun": 6,
        "junho": 6,
        "jul": 7,
        "julho": 7,
        "ago": 8,
        "agosto": 8,
        "set": 9,
        "setembro": 9,
        "out": 10,
        "outubro": 10,
        "nov": 11,
        "novembro": 11,
        "dez": 12,
        "dezembro": 12,
    }
    return months.get(month_key, 0)


def _parse_possible_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    normalized = _normalize_date_text(raw)
    compact = re.sub(r"\s+", "", normalized).replace(".", "/").replace("-", "/")

    dmy_match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", compact)
    if dmy_match:
        day = int(dmy_match.group(1))
        month = int(dmy_match.group(2))
        year = int(dmy_match.group(3))
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return ""

    my_match = re.fullmatch(r"(\d{1,2})/(\d{4})", compact)
    if my_match:
        month = int(my_match.group(1))
        year = int(my_match.group(2))
        try:
            return datetime(year, month, 1).date().isoformat()
        except ValueError:
            return ""

    month_year_match = re.fullmatch(r"([a-z]+)\s*[\/ ]\s*(\d{2,4})", normalized)
    if month_year_match:
        month = _month_from_name(month_year_match.group(1))
        year = _coerce_year(month_year_match.group(2))
        if month:
            try:
                return datetime(year, month, 1).date().isoformat()
            except ValueError:
                return ""

    textual_match = re.fullmatch(r"(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})", normalized)
    if textual_match:
        day = int(textual_match.group(1))
        month = _month_from_name(textual_match.group(2))
        year = int(textual_match.group(3))
        if month:
            try:
                return datetime(year, month, day).date().isoformat()
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
        r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*[\/ ]\s*\d{2,4}\b",
        r"\b\d{1,2}\s*(?:o|º|°)?\s+de\s+(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s+de\s+\d{4}\b",
    )
    for candidate in candidates:
        normalized_candidate = _normalize_date_text(candidate)
        for pattern in patterns:
            match = re.search(pattern, normalized_candidate)
            if match:
                return " ".join(match.group(0).split())
    return ""


def _normalize_boundary_date(value: str, is_end: bool) -> str:
    token = _extract_first_date_token(value) or value
    normalized = _normalize_date_text(token)
    if not normalized:
        return ""

    mm_yyyy_match = re.fullmatch(r"(\d{1,2})/(\d{4})", re.sub(r"\s+", "", normalized))
    if mm_yyyy_match:
        month = int(mm_yyyy_match.group(1))
        year = int(mm_yyyy_match.group(2))
        day = _last_day_of_month(year, month) if is_end else 1
        return datetime(year, month, day).date().isoformat()

    month_year_match = re.fullmatch(r"([a-z]+)\s*[\/ ]\s*(\d{2,4})", normalized)
    if month_year_match:
        month = _month_from_name(month_year_match.group(1))
        year = _coerce_year(month_year_match.group(2))
        if month:
            day = _last_day_of_month(year, month) if is_end else 1
            return datetime(year, month, day).date().isoformat()

    parsed = _parse_possible_date(token)
    if parsed:
        return parsed
    return ""


def _maybe_fix_mojibake(value: str) -> str:
    text = value or ""
    if not text:
        return text
    if not any(marker in text for marker in ("Ã", "Â", "Ãƒ", "Ã‚", "\ufffd")):
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except UnicodeError:
        return text


def _slice_period_value(text: str, label: str) -> str:
    normalized = _normalize_date_text(text)
    if not normalized:
        return ""

    start_match = re.search(rf"\b{label}\b[^:]*:\s*", normalized)
    if not start_match:
        return ""

    tail = normalized[start_match.end():]
    stop_match = re.search(
        r"\b(?:inicio|termino|objeto|diagnostico|abrangencia|justificativa|objetivo|metodologia|unidade responsavel|resultados esperados|plano de acao)\b",
        tail,
    )
    if stop_match:
        tail = tail[:stop_match.start()]

    return _clean_spaces(tail).strip(" ;,.")


def _extract_period_values(text: str) -> Dict[str, str]:
    normalized = _normalize_date_text(text or "")
    if not normalized:
        return {"inicio_raw": "", "termino_raw": ""}

    inicio_raw = _slice_period_value(normalized, "inicio")
    termino_raw = _slice_period_value(normalized, "termino")

    date_candidates: List[str] = []
    patterns = (
        r"\b\d{1,2}\s*[\/\-.]\s*\d{1,2}\s*[\/\-.]\s*\d{4}\b",
        r"\b\d{1,2}\s*[\/\-.]\s*\d{4}\b",
        r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*[\/ ]\s*\d{2,4}\b",
        r"\b\d{1,2}\s+de\s+(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s+de\s+\d{4}\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            token = _clean_spaces(match.group(0))
            if token and token not in date_candidates:
                date_candidates.append(token)

    if not inicio_raw and date_candidates:
        inicio_raw = date_candidates[0]
    if not termino_raw and len(date_candidates) >= 2:
        termino_raw = date_candidates[1]

    return {"inicio_raw": inicio_raw, "termino_raw": termino_raw}


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
        inicio_value = _extract_label_value(base_text, r"in(?:i|í|Ã­)cio")
        termino_value = _extract_label_value(base_text, r"t(?:e|é|Ã©)rmino")
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
            inicio_iso = _normalize_boundary_date(inicio_value, is_end=False)
            if inicio_iso:
                result["inicio_data"] = inicio_iso
            else:
                result["inicio_raw"] = inicio_value

        if termino_value:
            termino_iso = _normalize_boundary_date(termino_value, is_end=True)
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


def _extract_period_values_v2(text: str) -> Dict[str, str]:
    normalized = _normalize_date_text(text or "")
    if not normalized:
        return {"inicio_raw": "", "termino_raw": ""}

    def _compact(value: str) -> str:
        return " ".join((value or "").split()).strip()

    def _slice(label: str) -> str:
        match = re.search(rf"\b{label}\b[^:]*:\s*", normalized)
        if not match:
            return ""
        tail = normalized[match.end():]
        stop = re.search(
            r"\b(?:inicio|termino|objeto|diagnostico|abrangencia|justificativa|objetivo|metodologia|unidade responsavel|resultados esperados|plano de acao)\b",
            tail,
        )
        if stop:
            tail = tail[:stop.start()]
        return _compact(tail).strip(" ;,.")

    inicio_raw = _slice("inicio")
    termino_raw = _slice("termino")

    if not inicio_raw or not termino_raw:
        candidates: List[str] = []
        for pattern in (
            r"\b\d{1,2}\s*[\/\-.]\s*\d{1,2}\s*[\/\-.]\s*\d{4}\b",
            r"\b\d{1,2}\s*[\/\-.]\s*\d{4}\b",
            r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*[\/ ]\s*\d{2,4}\b",
            r"\b\d{1,2}\s+de\s+(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s+de\s+\d{4}\b",
        ):
            for found in re.finditer(pattern, normalized, flags=re.IGNORECASE):
                token = _compact(found.group(0))
                if token and token not in candidates:
                    candidates.append(token)
        if not inicio_raw and candidates:
            inicio_raw = candidates[0]
        if not termino_raw and len(candidates) >= 2:
            termino_raw = candidates[1]

    return {"inicio_raw": inicio_raw, "termino_raw": termino_raw}


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
        extracted = _extract_period_values_v2(base_text)
        inicio_value = extracted.get("inicio_raw", "")
        termino_value = extracted.get("termino_raw", "")
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
            inicio_iso = _normalize_boundary_date(inicio_value, is_end=False)
            if inicio_iso:
                result["inicio_data"] = inicio_iso
            else:
                result["inicio_raw"] = inicio_value

        if termino_value:
            termino_iso = _normalize_boundary_date(termino_value, is_end=True)
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

