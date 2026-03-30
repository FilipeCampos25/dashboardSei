from __future__ import annotations

import calendar
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.output import csv_writer

ATTRIBUICOES_COLUMN = "atribuições_raw"
REQUESTED_TYPE_PT = "pt"
RESOLVED_TYPE_PT = "plano_trabalho"
VALIDATION_STATUS_VALID = "valid_for_requested_type"
VALIDATION_STATUS_NON_CANONICAL = "related_but_not_canonical"
PUBLICATION_STATUS_GOLD = "published_gold"
PUBLICATION_STATUS_SILVER = "retained_silver"
PERIOD_SOURCE_DIRECT = "direct_label"
PERIOD_SOURCE_SIGNATURE = "derived_from_signature"
PERIOD_SOURCE_RELATIVE = "unresolved_relative"
PERIOD_SOURCE_NOISE = "unresolved_noise"
PERIOD_SOURCE_MISSING = "missing_period"
CLASSIFICATION_REASON_PT = "plano_trabalho_validado_por_conteudo"
CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO = "pt_minuta_documentacao"

MONTHS = {
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

NUMBER_WORDS = {
    "um": 1,
    "uma": 1,
    "dois": 2,
    "duas": 2,
    "tres": 3,
    "quatro": 4,
    "cinco": 5,
    "seis": 6,
    "sete": 7,
    "oito": 8,
    "nove": 9,
    "dez": 10,
    "sessenta": 60,
}

OCR_DIGIT_MAP = {
    "o": "0",
    "q": "0",
    "d": "0",
    "i": "1",
    "l": "1",
    "|": "1",
    "!": "1",
    "z": "2",
    "ł": "2",
    "£": "2",
    "€": "2",
    "s": "5",
    "$": "5",
    "b": "6",
    "g": "9",
}

INVALID_MARKERS = (
    "inserir previsao",
    "na data de assinatura",
    "a autenticidade do documento pode ser conferida",
    "codigo verificador",
    "codigo crc",
    "documento assinado eletronicamente",
    "criado por ",
    "testemunhas",
)

WEAK_PERIOD_MARKERS = (
    "o presente plano de trabalho tem por",
    "o presente plano de trabalho vigorara",
)

DATE_TOKEN = (
    r"(?:\d{1,2}\s*(?:o|º|°)?\s+de\s+[a-zc]+\s+de\s+\d{4}"
    r"|\d{1,2}\s*[\/.-]\s*\d{1,2}\s*[\/.-]\s*\d{4}"
    r"|\d{1,2}\s*[\/.-]\s*\d{4}"
    r"|(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zc]*\s*[\/ ]?\s*\d{2,4}"
    r"|(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zc]*\d{4})"
)

TOP_STOP = (
    r"(?:\bprevisao\s+de\s+inicio\b|\bunidade\s+responsavel\b|\bobservacoes\b"
    r"|\bcronograma\s+de\s+desembolso\b|(?:^|\n)\s*\d+\.\s*[A-Z])"
)


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    if logger is None:
        return
    try:
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(msg, *args)
    except Exception:
        return


def _clean_spaces(value: str) -> str:
    return " ".join((value or "").replace("\r", "\n").split()).strip()


def _maybe_fix_mojibake(value: str) -> str:
    text = value or ""
    if not text or not any(marker in text for marker in ("Ã", "Â", "\ufffd")):
        return text
    repaired = text
    for _ in range(2):
        candidate = repaired
        for source_encoding in ("latin1", "cp1252"):
            try:
                candidate = repaired.encode(source_encoding).decode("utf-8")
                break
            except UnicodeError:
                candidate = repaired
        if candidate == repaired:
            break
        repaired = candidate
        if not any(marker in repaired for marker in ("Ã", "Â", "â", "\ufffd")):
            break
    return repaired


def _prepare_text(value: str) -> str:
    text = _maybe_fix_mojibake(value or "")
    if not text:
        return ""
    replacements = {
        "\u00a0": " ",
        "\ufb01": "fi",
        "\ufb02": "fl",
        "\ufb00": "ff",
        "\ufb03": "ffi",
        "\ufb04": "ffl",
        "â€“": "-",
        "â€”": "-",
        "Ã¢â‚¬â€œ": "-",
        "Ã¢â‚¬â€": "-",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<=\bMeta)(?=\d)", " ", text)
    text = re.sub(r"(?<=\bFase)(?=[A-Z])", " ", text)
    text = re.sub(r"(?<=\bAtividade)(?=[A-Z0-9])", " ", text)
    text = re.sub(r"(?<=\bAcao)(?=\d)", " ", text)
    text = re.sub(r"(?<=\bAção)(?=\d)", " ", text)
    text = re.sub(r"(?<=\bAte)(?=(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez))", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=[a-zà-ÿ])(?=[A-ZÀ-Ý])", " ", text)
    text = re.sub(r"(?<=[A-Za-zÀ-ÿ])(?=\d{4}\b)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_text(value: str) -> str:
    text = _clean_spaces(_prepare_text(value))
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text.lower().replace("º", "").replace("°", "")).strip()


def _find_latest_preview_csv(output_dir: Path) -> Optional[Path]:
    path = output_dir / "parcerias_vigentes_latest.csv"
    return path if path.exists() else None


def _load_preview_map(output_dir: Path, logger: Any = None) -> Dict[str, Dict[str, str]]:
    csv_path = _find_latest_preview_csv(output_dir)
    if csv_path is None:
        _log(logger, "info", "Normalizador PT: nenhum CSV de previa encontrado em %s.", output_dir)
        return {}
    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
    except Exception as exc:
        _log(logger, "warning", "Normalizador PT: falha ao ler previa %s (%s).", csv_path, exc)
        return {}
    preview: Dict[str, Dict[str, str]] = {}
    for row in df.to_dict(orient="records"):
        processo = _clean_spaces(str(row.get("processo", "") or ""))
        if processo and processo not in preview:
            preview[processo] = {key: _clean_spaces(str(value or "")) for key, value in row.items()}
    return preview


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_placeholder(value: str) -> bool:
    normalized = _normalize_text(value)
    return not normalized or any(marker in normalized for marker in INVALID_MARKERS)


def _trim_noise(value: str) -> str:
    prepared = _prepare_text(value)
    if not prepared:
        return ""
    for pattern in (
        r"documento assinado eletronicamente",
        r"a autenticidade do documento",
        r"codigo verificador",
        r"codigo crc",
        r"criado por ",
        r"acao=documento_conferir",
        r"controlador_externo\.php",
    ):
        match = re.search(pattern, prepared, flags=re.IGNORECASE)
        if match:
            prepared = prepared[: match.start()]
    return _clean_spaces(prepared)


def _has_content(value: str, min_alpha: int = 8) -> bool:
    cleaned = _trim_noise(value)
    return not _is_placeholder(cleaned) and len(re.findall(r"[A-Za-zÀ-ÿ]", cleaned)) >= min_alpha


def _norm_month(month_raw: str) -> str:
    return _normalize_text(month_raw).replace(".", "")


def _last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _coerce_year(year_raw: str) -> int:
    year = int(_coerce_numeric_token(year_raw, max_len=4) or year_raw)
    return 2000 + year if year < 100 else year


def _coerce_numeric_token(value: str, *, max_len: Optional[int] = None) -> str:
    raw = _clean_spaces(value)
    if not raw:
        return ""
    normalized = _normalize_text(raw).replace(" ", "")
    chars: List[str] = []
    for char in normalized:
        if char.isdigit():
            chars.append(char)
            continue
        mapped = OCR_DIGIT_MAP.get(char)
        if mapped is not None:
            chars.append(mapped)
    token = "".join(chars)
    if max_len is not None and len(token) > max_len:
        token = token[:max_len]
    return token


def _add_months(base_date: datetime, months: int) -> datetime:
    year = base_date.year + (base_date.month - 1 + months) // 12
    month = (base_date.month - 1 + months) % 12 + 1
    return datetime(year, month, min(base_date.day, _last_day(year, month)))


def _normalize_date_token(token: str, end_of_month: bool = False) -> str:
    normalized = _normalize_text(token)
    if not normalized:
        return ""

    patterns = (
        (
            r"([0-9a-zł£€|!$]{1,3})\s*[\/.-]\s*([0-9a-zł£€|!$]{1,3})\s*[\/.-]\s*([0-9a-zł£€|!$]{2,4})",
            lambda match: datetime(
                _coerce_year(match.group(3)),
                int(_coerce_numeric_token(match.group(2), max_len=2) or "0"),
                int(_coerce_numeric_token(match.group(1), max_len=2) or "0"),
            ).date().isoformat(),
        ),
        (
            r"([0-9a-zł£€|!$]{1,3})\s*[\/.-]\s*([0-9a-zł£€|!$]{2,4})",
            lambda match: datetime(
                _coerce_year(match.group(2)),
                int(_coerce_numeric_token(match.group(1), max_len=2) or "0"),
                _last_day(_coerce_year(match.group(2)), int(_coerce_numeric_token(match.group(1), max_len=2) or "0")) if end_of_month else 1,
            ).date().isoformat(),
        ),
        (
            r"([a-zc]+)\s*[\/ ]\s*(\d{2,4})",
            lambda match: datetime(
                _coerce_year(match.group(2)),
                MONTHS.get(_norm_month(match.group(1)), 0),
                _last_day(_coerce_year(match.group(2)), MONTHS.get(_norm_month(match.group(1)), 0)) if end_of_month else 1,
            ).date().isoformat(),
        ),
        (
            r"([a-zc]+)(\d{4})",
            lambda match: datetime(
                int(match.group(2)),
                MONTHS.get(_norm_month(match.group(1)), 0),
                _last_day(int(match.group(2)), MONTHS.get(_norm_month(match.group(1)), 0)) if end_of_month else 1,
            ).date().isoformat(),
        ),
        (
            r"([0-9a-zł£€|!$]{1,3})\s+de\s+([a-zc]+)\s+de\s+([0-9a-zł£€|!$]{2,4})",
            lambda match: datetime(
                _coerce_year(match.group(3)),
                MONTHS.get(_norm_month(match.group(2)), 0),
                int(_coerce_numeric_token(match.group(1), max_len=2) or "0"),
            ).date().isoformat(),
        ),
    )
    for pattern, handler in patterns:
        match = re.fullmatch(pattern, normalized)
        if not match:
            continue
        try:
            return handler(match)
        except Exception:
            return ""
    return ""


def _signature_dates(text: str) -> List[str]:
    normalized = _normalize_text(text or "")
    tail = normalized[-3500:]
    dates: List[str] = []
    for pattern in (
        r"assinad[oa].{0,180}?\bem\s+(\d{1,2}/\d{1,2}/\d{4})",
        r"brasilia,\s*(\d{1,2}\s+de\s+[a-zc]+\s+de\s+\d{4})",
        r"brasilia,\s*(\d{1,2}/\d{1,2}/\d{4})",
        r"brasilia,\s*([0-9a-zł£€|!$]{1,3}\s+de\s+[a-zc]+\s+de\s+[0-9a-zł£€|!$]{2,4})",
        r"assinad[oa].{0,180}?\bem\s+([0-9a-zł£€|!$]{1,3}/[0-9a-zł£€|!$]{1,3}/[0-9a-zł£€|!$]{2,4})",
    ):
        for match in re.finditer(pattern, tail, flags=re.IGNORECASE | re.DOTALL):
            iso = _normalize_date_token(match.group(1), end_of_month=False)
            if iso and iso not in dates:
                dates.append(iso)
    return dates


def _looks_like_relative_signature_reference(value: str) -> bool:
    normalized = _normalize_text(value)
    return bool(
        normalized
        and re.search(
            r"(a partir da assinatura|apos a assinatura|imediatamente apos a assinatura|na data de sua assinatura|na data da assinatura)",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _period_value_is_noise(value: str) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return False
    if any(marker in normalized for marker in INVALID_MARKERS + WEAK_PERIOD_MARKERS):
        return True
    if len(normalized.split()) >= 12 and not re.search(DATE_TOKEN, normalized, flags=re.IGNORECASE):
        return True
    return False


def _empty_period(*, source: str = PERIOD_SOURCE_MISSING, warning: str = "") -> Dict[str, str]:
    return {
        "prazo_inicio_raw": "",
        "prazo_inicio": "",
        "prazo_fim_raw": "",
        "prazo_fim": "",
        "period_source": source,
        "period_warning": warning,
    }


def _extract_period_from_snapshot(snapshot: Dict[str, Any], prazos: Dict[str, Any]) -> Dict[str, str]:
    text = _prepare_text(str(snapshot.get("text", "") or ""))
    normalized = _normalize_text(text)
    empty = _empty_period()
    if not normalized:
        return empty

    inline_label_pattern = (
        r"\binicio(?:\s*\(\s*mes\s*/\s*ano\s*\))?\s*(?::|-)?\s*"
        rf"({DATE_TOKEN})\s+"
        r"termino(?:\s*\(\s*mes\s*/\s*ano\s*\))?\s*(?::|-)?\s*"
        rf"({DATE_TOKEN})"
    )
    for pattern in (
        rf"(?:periodo\s+de\s+execucao|previsao\s+de\s+inicio\s+e\s+termino)[^a-z0-9]+({DATE_TOKEN})\s+(?:a|ate|até|-)\s+({DATE_TOKEN})",
        inline_label_pattern,
    ):
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        start_raw = _clean_spaces(match.group(1))
        end_raw = _clean_spaces(match.group(2))
        start_iso = _normalize_date_token(start_raw, end_of_month=False)
        end_iso = _normalize_date_token(end_raw, end_of_month=True)
        if start_iso and end_iso and end_iso >= start_iso:
            return {
                "prazo_inicio_raw": start_raw,
                "prazo_inicio": start_iso,
                "prazo_fim_raw": end_raw,
                "prazo_fim": end_iso,
                "period_source": PERIOD_SOURCE_DIRECT,
                "period_warning": "",
            }

    signature_dates = _signature_dates(text)
    signature_iso = signature_dates[0] if signature_dates else _clean_spaces(str(prazos.get("inicio_data", "") or ""))
    duration = re.search(
        r"prazo\s+de\s+(\d+|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|sessenta)"
        r"(?:\s*\([^)]+\))?\s+(mes(?:es)?|anos?)"
        r".{0,120}?(?:a\s+partir\s+da\s+data\s+de\s+sua\s+assinatura|a\s+partir\s+da\s+assinatura|apos\s+a\s+assinatura)",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if duration and signature_iso:
        raw_amount, unit = duration.groups()
        amount = int(raw_amount) if raw_amount.isdigit() else NUMBER_WORDS.get(raw_amount.replace(" ", "_"), 0)
        if amount > 0:
            base = datetime.fromisoformat(signature_iso)
            end_dt = base.replace(year=base.year + amount) if "ano" in unit else _add_months(base, amount)
            return {
                "prazo_inicio_raw": "a partir da assinatura",
                "prazo_inicio": signature_iso,
                "prazo_fim_raw": duration.group(0),
                "prazo_fim": end_dt.date().isoformat(),
                "period_source": PERIOD_SOURCE_SIGNATURE,
                "period_warning": "",
            }

    if signature_iso:
        start_iso = ""
        end_iso = ""
        if re.search(r"imediatamente\s+apos\s+a\s+assinatura|a\s+partir\s+da\s+assinatura", normalized):
            start_iso = signature_iso
        relative = re.search(
            r"(\d+|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|sessenta)\s+(mes(?:es)?|anos?)\s+apos\s+a\s+assinatura",
            normalized,
            flags=re.IGNORECASE,
        )
        if relative:
            raw_amount, unit = relative.groups()
            amount = int(raw_amount) if raw_amount.isdigit() else NUMBER_WORDS.get(raw_amount.replace(" ", "_"), 0)
            if amount > 0:
                base = datetime.fromisoformat(signature_iso)
                end_dt = base.replace(year=base.year + amount) if "ano" in unit else _add_months(base, amount)
                end_iso = end_dt.date().isoformat()
        if start_iso and end_iso and end_iso >= start_iso:
            return {
                "prazo_inicio_raw": "a partir da assinatura",
                "prazo_inicio": start_iso,
                "prazo_fim_raw": relative.group(0) if relative else "",
                "prazo_fim": end_iso,
                "period_source": PERIOD_SOURCE_SIGNATURE,
                "period_warning": "",
            }

    start_raw = _clean_spaces(str(prazos.get("inicio_raw", "") or ""))
    end_raw = _clean_spaces(str(prazos.get("termino_raw", "") or ""))
    start_iso = _clean_spaces(str(prazos.get("inicio_data", "") or ""))
    end_iso = _clean_spaces(str(prazos.get("termino_data", "") or ""))
    if start_raw and not _period_value_is_noise(start_raw):
        start_iso = start_iso or _normalize_date_token(start_raw, end_of_month=False)
    if end_raw and not _period_value_is_noise(end_raw):
        end_iso = end_iso or _normalize_date_token(end_raw, end_of_month=True)
    if start_iso and end_iso and end_iso >= start_iso:
        return {
            "prazo_inicio_raw": start_raw or start_iso,
            "prazo_inicio": start_iso,
            "prazo_fim_raw": end_raw or end_iso,
            "prazo_fim": end_iso,
            "period_source": PERIOD_SOURCE_DIRECT,
            "period_warning": "",
        }

    raw_blob = " ".join(part for part in (start_raw, end_raw) if _clean_spaces(part))
    if raw_blob:
        if _period_value_is_noise(start_raw) or _period_value_is_noise(end_raw):
            return _empty_period(source=PERIOD_SOURCE_NOISE, warning="periodo_bruto_contaminado_ou_narrativo")
        if _looks_like_relative_signature_reference(raw_blob):
            if signature_iso:
                relative = re.search(
                    r"(\d+|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|sessenta)\s+(mes(?:es)?|anos?)",
                    _normalize_text(raw_blob),
                    flags=re.IGNORECASE,
                )
                if relative:
                    raw_amount, unit = relative.groups()
                    amount = int(raw_amount) if raw_amount.isdigit() else NUMBER_WORDS.get(raw_amount.replace(" ", "_"), 0)
                    if amount > 0:
                        base = datetime.fromisoformat(signature_iso)
                        end_dt = base.replace(year=base.year + amount) if "ano" in unit else _add_months(base, amount)
                        return {
                            "prazo_inicio_raw": start_raw or "a partir da assinatura",
                            "prazo_inicio": signature_iso,
                            "prazo_fim_raw": end_raw,
                            "prazo_fim": end_dt.date().isoformat(),
                            "period_source": PERIOD_SOURCE_SIGNATURE,
                            "period_warning": "",
                        }
            return _empty_period(source=PERIOD_SOURCE_RELATIVE, warning="periodo_relativo_sem_assinatura_inequivoca")

    if re.search(r"(a partir da assinatura|apos a assinatura|imediatamente apos a assinatura)", normalized, flags=re.IGNORECASE):
        return _empty_period(source=PERIOD_SOURCE_RELATIVE, warning="periodo_relativo_sem_data_derivada")

    return empty


def _extract_section(text: str, headings: Iterable[str]) -> str:
    prepared = _prepare_text(text)
    for heading in headings:
        match = re.search(heading, prepared, flags=re.IGNORECASE)
        if not match:
            continue
        tail = prepared[match.start():]
        stop = re.search(TOP_STOP, tail[1:], flags=re.IGNORECASE | re.MULTILINE)
        return tail[: stop.start() + 1].strip() if stop else tail.strip()
    return ""


def _extract_pattern_fragments(text: str, patterns: Iterable[str], max_len: int = 900) -> List[str]:
    prepared = _prepare_text(text)
    out: List[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, prepared, flags=re.IGNORECASE | re.DOTALL):
            snippet = _trim_noise(match.group(0))
            if snippet and snippet not in out:
                out.append(snippet[:max_len].rstrip(" ,;") + ("..." if len(snippet) > max_len else ""))
    return out


def _execution_from_tables(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    metas: List[str] = []
    acoes: List[str] = []
    for table in snapshot.get("tables", []) or []:
        rows = table.get("rows", []) if isinstance(table, dict) else table
        rows = rows or []
        if not rows:
            continue
        header = " | ".join(_normalize_text(cell) for cell in rows[0] if _clean_spaces(str(cell or "")))
        relevant = (
            "meta" in header and ("acao" in header or "descricao" in header) and ("periodo" in header or "cronograma" in header or "responsavel" in header)
        ) or ("etapa" in header and "descricao" in header and "cronograma" in header)
        if not relevant:
            continue
        for row in rows[1:]:
            cells = [_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or ""))]
            if not cells:
                continue
            row_text = " | ".join(cells)
            first = _normalize_text(cells[0])
            if re.fullmatch(r"\d+", first) and len(cells) >= 2:
                metas.append(f"{cells[0]} | {cells[1]}")
                if len(cells) >= 3:
                    acoes.append(" | ".join(cells[1:]))
            elif first.startswith(("meta", "fase", "etapa")):
                metas.append(row_text)
                if len(cells) > 1:
                    acoes.append(" | ".join(cells[1:]))
            else:
                acoes.append(row_text)
    return (" || ".join(dict.fromkeys(metas)), " || ".join(dict.fromkeys(acoes)))


def _extract_execution_section(text: str) -> str:
    section = _extract_section(
        text,
        [
            r"5\.\s*metodologia\s+e\s+interven[cç][aã]o",
            r"metas?\s+de\s+execu[cç][aã]o",
            r"acao\s+e\s+cronograma",
            r"4\.\s*etapas?\s*,?\s*execu[cç][aã]o\s+e\s+cronograma",
            r"4\.\s*etapas?\s+e\s+execu[cç][aã]o\s+e\s+cronograma",
        ],
    )
    if section:
        return section
    prepared = _prepare_text(text)
    starts = [
        match.start()
        for match in (
            re.search(r"\bmeta\s*\d+\b", prepared, re.I),
            re.search(r"\bfase\s*[a-z]\b", prepared, re.I),
        )
        if match
    ]
    if not starts:
        return ""
    tail = prepared[min(starts):]
    stop = re.search(TOP_STOP, tail[1:], flags=re.IGNORECASE | re.MULTILINE)
    return tail[: stop.start() + 1].strip() if stop else tail.strip()


def _extract_objeto(snapshot: Dict[str, Any], preview: Dict[str, str]) -> str:
    preview_obj = _clean_spaces(str(preview.get("objeto", "") or ""))
    if _has_content(preview_obj):
        return preview_obj
    text = _prepare_text(str(snapshot.get("text", "") or ""))
    match = re.search(
        r"identificacao\s+do\s+objeto\s+(.*?)(?=\b(?:diagnostico|objetivo|metodologia|meta\s*\d+|previsao\s+de\s+inicio|unidade\s+responsavel)\b|$)",
        _normalize_text(text),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match and _has_content(match.group(1)):
        return _clean_spaces(match.group(1))
    for line in text.replace("\r", "\n").splitlines():
        cleaned = _clean_spaces(line)
        if "objeto" in _normalize_text(cleaned) and ":" in cleaned:
            value = _clean_spaces(cleaned.split(":", 1)[1])
            if _has_content(value):
                return value
    return ""


def _extract_partner(snapshot: Dict[str, Any], preview: Dict[str, str]) -> str:
    preview_partner = _clean_spaces(str(preview.get("parceiro", "") or ""))
    if _has_content(preview_partner, min_alpha=4):
        return preview_partner
    text = _prepare_text(str(snapshot.get("text", "") or ""))
    for pattern in (
        r"part[ií]cipe\s*2\s*:\s*(.+?)(?=\s+CNPJ\b|\s+DDD/Telefone\b|\s+Respons[aá]vel\b|$)",
        r"outros\s+part[ií]cipes?\s*-\s*executor\s+[^\n]*?[óo]rg[aã]o\s*/\s*entidade\s+(.+?)(?=\s+CNPJ\b|\s+Endere[cç]o\b|$)",
        r"executor\s+[^\n]*?[óo]rg[aã]o\s*/\s*entidade\s+(.+?)(?=\s+CNPJ\b|\s+Endere[cç]o\b|$)",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            candidate = _clean_spaces(match.group(1))
            if _has_content(candidate, min_alpha=4) and "censipam" not in _normalize_text(candidate):
                return candidate
    match = re.search(r"estado-maior\s+da\s+armada\s*-\s*ema", text, flags=re.IGNORECASE)
    return _clean_spaces(match.group(0)) if match else ""


def _extract_atribuicoes(snapshot: Dict[str, Any]) -> str:
    return _clean_spaces(
        _extract_section(
            str(snapshot.get("text", "") or ""),
            [
                r"responsabilidades\s+dos\s+participes",
                r"objetivo\s+geral\s+e\s+objetivos\s+especificos",
                r"compromissos\s+e\s+responsabilidades",
            ],
        )
    )


def _extract_metas(snapshot: Dict[str, Any]) -> str:
    metas_from_tables, _ = _execution_from_tables(snapshot)
    if _has_content(metas_from_tables):
        return metas_from_tables
    base = _extract_execution_section(str(snapshot.get("text", "") or "")) or str(snapshot.get("text", "") or "")
    fragments = _extract_pattern_fragments(
        base,
        [
            r"\bmeta\s*\d+\b.{0,700}?(?=\bmeta\s*\d+\b|\bfase\s*[a-z]\b|\bacao\b|\batividade\s*[a-z]?\.\d+(?:\.\d+)?\b|" + TOP_STOP + r"|$)",
            r"\bfase\s*[a-z]\b.{0,700}?(?=\bfase\s*[a-z]\b|\bmeta\s*\d+\b|" + TOP_STOP + r"|$)",
            r"\betapa(?:s)?\b.{0,700}?(?=" + TOP_STOP + r"|$)",
        ],
    )
    return " || ".join(fragments) if fragments else _clean_spaces(_extract_execution_section(base))


def _extract_acoes(snapshot: Dict[str, Any]) -> str:
    _, acoes_from_tables = _execution_from_tables(snapshot)
    if _has_content(acoes_from_tables):
        return acoes_from_tables
    base = _extract_execution_section(str(snapshot.get("text", "") or "")) or str(snapshot.get("text", "") or "")
    fragments = _extract_pattern_fragments(
        base,
        [
            r"\bacao(?:\s*\d+)?\b.{0,600}?(?=\bacao(?:\s*\d+)?\b|\bproduto\b|\bmeta\s*\d+\b|\batividade\s*[a-z]?\.\d+(?:\.\d+)?\b|" + TOP_STOP + r"|$)",
            r"\batividade\s*[a-z]?\.\d+(?:\.\d+)?\b.{0,600}?(?=\batividade\s*[a-z]?\.\d+(?:\.\d+)?\b|\bfase\s*[a-z]\b|\bmeta\s*\d+\b|" + TOP_STOP + r"|$)",
            r"\bproduto\b.{0,300}?(?=\bproduto\b|\bmeta\s*\d+\b|\bacao\b|" + TOP_STOP + r"|$)",
        ],
    )
    return " || ".join(fragments) if fragments else _clean_spaces(_extract_execution_section(base))


def _classify_record(record: Dict[str, str]) -> Tuple[str, int]:
    has_partner = _has_content(record.get("parceiro", ""), min_alpha=4)
    has_objeto = _has_content(record.get("objeto", ""))
    has_metas = _has_content(record.get("metas_raw", ""))
    has_acoes = _has_content(record.get("acoes_raw", ""))
    has_period = bool(record.get("prazo_inicio") and record.get("prazo_fim") and record["prazo_fim"] >= record["prazo_inicio"])
    captured = sum(
        [
            1 if has_partner else 0,
            1 if has_objeto else 0,
            1 if record.get("prazo_inicio") else 0,
            1 if record.get("prazo_fim") else 0,
            1 if has_metas else 0,
            1 if has_acoes else 0,
        ]
    )
    if has_partner and has_objeto and has_period and (has_metas or has_acoes):
        return ("completo_padronizado", captured)
    if has_objeto and sum([1 if has_partner else 0, 1 if has_period else 0, 1 if (has_metas or has_acoes) else 0]) >= 2:
        return ("parcial_padronizado", captured)
    return ("extraido_sem_padrao", captured)


def build_normalized_record(payload: Dict[str, Any], preview: Dict[str, str], json_path: Path) -> Dict[str, str]:
    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    analysis = payload.get("analysis", {}) or {}
    period = _extract_period_from_snapshot(snapshot, payload.get("prazos", {}) or {})
    vigencia_raw = _clean_spaces(str(preview.get("vigencia", "") or ""))
    if not _has_content(vigencia_raw, min_alpha=2):
        vigencia_raw = " a ".join(part for part in (period["prazo_inicio_raw"], period["prazo_fim_raw"]) if _clean_spaces(part))
    validation_status = _clean_spaces(str(analysis.get("validation_status", "") or "")) or VALIDATION_STATUS_VALID
    is_canonical_candidate = bool(analysis.get("is_canonical_candidate", validation_status == VALIDATION_STATUS_VALID))
    classification_reason = (
        _clean_spaces(str(analysis.get("classification_reason", "") or ""))
        or period["period_warning"]
        or CLASSIFICATION_REASON_PT
    )
    record = {
        "captured_at": _clean_spaces(str(payload.get("captured_at", "") or "")),
        "requested_type": _clean_spaces(str(payload.get("requested_type", "") or "")) or REQUESTED_TYPE_PT,
        "resolved_document_type": _clean_spaces(str(analysis.get("resolved_document_type", "") or "")) or _clean_spaces(str(payload.get("resolved_document_type", "") or "")) or RESOLVED_TYPE_PT,
        "processo": _clean_spaces(str(payload.get("processo", "") or "")),
        "documento": _clean_spaces(str(payload.get("documento", "") or "")),
        "parceiro": _extract_partner(snapshot, preview),
        "vigencia_raw": vigencia_raw,
        "vigencia_inicio": period["prazo_inicio"],
        "vigencia_fim": period["prazo_fim"],
        "objeto": _extract_objeto(snapshot, preview),
        ATTRIBUICOES_COLUMN: _extract_atribuicoes(snapshot),
        "metas_raw": _extract_metas(snapshot),
        "acoes_raw": _extract_acoes(snapshot),
        "prazo_inicio_raw": period["prazo_inicio_raw"],
        "prazo_inicio": period["prazo_inicio"],
        "prazo_fim_raw": period["prazo_fim_raw"],
        "prazo_fim": period["prazo_fim"],
        "period_source": period["period_source"],
        "period_warning": period["period_warning"],
        "selection_reason": _clean_spaces(str(collection.get("selection_reason", "") or "")),
        "classification_reason": classification_reason,
        "validation_status": validation_status,
        "snapshot_mode": _clean_spaces(str(snapshot.get("extraction_mode", "") or "")),
        "preview_numero_act": _clean_spaces(str(preview.get("numero_act", "") or "")),
        "json_path": str(json_path),
    }
    status, captured = _classify_record(record)
    record["normalization_status"] = status
    record["captured_focus_fields"] = str(captured)
    record["publication_status"] = (
        PUBLICATION_STATUS_GOLD
        if is_canonical_candidate and status == "completo_padronizado"
        else PUBLICATION_STATUS_SILVER
    )
    return record


def export_normalized_csv(output_dir: Path, logger: Any = None) -> Dict[str, Any]:
    csv_writer.ensure_output_dir(output_dir)
    preview_map = _load_preview_map(output_dir, logger=logger)
    json_paths = sorted(output_dir.glob("plano_trabalho_*.json"))
    if not json_paths:
        _log(logger, "info", "Normalizador PT: nenhum JSON de plano de trabalho encontrado em %s.", output_dir)
        return {"records": 0, "csv_path": None, "latest_path": None}

    records: List[Dict[str, str]] = []
    for json_path in json_paths:
        try:
            payload = _read_json(json_path)
            processo = _clean_spaces(str(payload.get("processo", "") or ""))
            records.append(build_normalized_record(payload, preview_map.get(processo, {}), json_path))
        except Exception as exc:
            _log(logger, "warning", "Normalizador PT: falha ao processar %s (%s).", json_path, exc)

    columns = [
        "captured_at",
        "requested_type",
        "resolved_document_type",
        "processo",
        "documento",
        "parceiro",
        "vigencia_raw",
        "vigencia_inicio",
        "vigencia_fim",
        "objeto",
        ATTRIBUICOES_COLUMN,
        "metas_raw",
        "acoes_raw",
        "prazo_inicio_raw",
        "prazo_inicio",
        "prazo_fim_raw",
        "prazo_fim",
        "period_source",
        "period_warning",
        "selection_reason",
        "classification_reason",
        "validation_status",
        "publication_status",
        "snapshot_mode",
        "preview_numero_act",
        "normalization_status",
        "captured_focus_fields",
        "json_path",
    ]

    audit_path = output_dir / "pt_auditoria_latest.csv"
    csv_writer.write_csv(records, audit_path, columns=columns)

    published_rows = [record for record in records if record.get("publication_status") == PUBLICATION_STATUS_GOLD]
    csv_path = output_dir / "pt_normalizado_latest.csv"
    complete_path = output_dir / "pt_normalizado_completo_latest.csv"
    # Ambos os arquivos publicam apenas o subconjunto gold; a dashboard deve consumir o export consolidado.
    csv_writer.write_csv(published_rows, csv_path, columns=columns)
    csv_writer.write_csv(published_rows, complete_path, columns=columns)

    _log(
        logger,
        "info",
        "Normalizador PT: auditoria=%d publicados_gold=%d arquivo=%s.",
        len(records),
        len(published_rows),
        csv_path,
    )
    return {
        "records": len(published_rows),
        "audit_records": len(records),
        "csv_path": csv_path,
        "latest_path": csv_path,
        "audit_path": audit_path,
        "complete_path": complete_path,
        "complete_latest_path": complete_path,
    }
