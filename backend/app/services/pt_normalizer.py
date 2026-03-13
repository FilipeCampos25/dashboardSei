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


MONTHS = {
    "jan": 1,
    "janeiro": 1,
    "fev": 2,
    "fevereiro": 2,
    "mar": 3,
    "marco": 3,
    "março": 3,
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


def _normalize_text(value: str) -> str:
    text = _clean_spaces(value)
    if not text:
        return ""
    text = _maybe_fix_mojibake(text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


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


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_latest_preview_csv(output_dir: Path) -> Optional[Path]:
    latest = output_dir / "parcerias_vigentes_latest.csv"
    if latest.exists():
        return latest
    return None


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

    preview_map: Dict[str, Dict[str, str]] = {}
    for row in df.to_dict(orient="records"):
        processo = _clean_spaces(str(row.get("processo", "") or ""))
        if processo and processo not in preview_map:
            preview_map[processo] = {k: _clean_spaces(str(v or "")) for k, v in row.items()}
    _log(logger, "info", "Normalizador PT: previa carregada de %s (%d processos).", csv_path, len(preview_map))
    return preview_map


def _extract_heading_block(text: str, heading_pattern: str) -> str:
    base = _maybe_fix_mojibake(text or "")
    if not base:
        return ""

    lines = [line.strip() for line in base.replace("\r", "\n").splitlines()]
    content: List[str] = []
    capture = False

    for line in lines:
        cleaned = _clean_spaces(line)
        if not cleaned:
            if capture and content:
                content.append("")
            continue

        normalized = _normalize_text(cleaned)
        if re.search(heading_pattern, normalized):
            capture = True
            continue

        if capture and re.match(r"^(?:[ivxlcdm]+|\d+(?:\.\d+)*)\s*[-.)]?\s*[a-z]", normalized):
            break

        if capture:
            content.append(cleaned)

    return "\n".join(content).strip()


def _extract_label_value(text: str, label_patterns: Iterable[str]) -> str:
    base = _maybe_fix_mojibake(text or "")
    if not base:
        return ""

    for raw_line in base.replace("\r", "\n").splitlines():
        line = _clean_spaces(raw_line)
        if not line:
            continue
        normalized = _normalize_text(line)
        for label in label_patterns:
            if re.search(rf"\b{label}\b", normalized):
                parts = re.split(r":", line, maxsplit=1)
                if len(parts) == 2:
                    return _clean_spaces(parts[1])
    return ""


def _extract_date_like_candidates(value: str) -> List[str]:
    raw_text = _clean_spaces(_maybe_fix_mojibake(value or ""))
    if not raw_text:
        return []

    text = _normalize_text(raw_text)
    patterns = [
        r"\b\d{1,2}\s*[\/.-]\s*\d{1,2}\s*[\/.-]\s*\d{4}\b",
        r"\b\d{1,2}\s*(?:o|º|°)?\s+de\s+[a-zç]+\s+de\s+\d{4}\b",
        r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zç]*\s*[\/ ]\s*\d{2,4}\b",
        r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zç]*\s*\d{4}\b",
        r"\b\d{1,2}\s*[\/.-]\s*\d{4}\b",
    ]

    matches: List[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            cleaned = _clean_spaces(match.group(0))
            if cleaned and cleaned not in matches:
                matches.append(cleaned)
    if not matches and raw_text:
        matches.append(raw_text)
    return matches


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _coerce_year(year_raw: str) -> int:
    year = int(year_raw)
    if year < 100:
        return 2000 + year
    return year


def _add_months(base_date: datetime, months: int) -> datetime:
    year = base_date.year + (base_date.month - 1 + months) // 12
    month = (base_date.month - 1 + months) % 12 + 1
    day = min(base_date.day, _last_day_of_month(year, month))
    return datetime(year, month, day)


def _extract_signature_dates(text: str) -> List[str]:
    normalized = _normalize_text(_maybe_fix_mojibake(text or ""))
    if not normalized:
        return []

    dates: List[str] = []
    for match in re.finditer(
        r"assinad[oa].{0,180}?\bem\s+(\d{1,2}/\d{1,2}/\d{4})",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        iso = _normalize_date_token(match.group(1), end_of_month=False)
        if iso and iso not in dates:
            dates.append(iso)
    return dates


def _extract_reference_signature_date(text: str) -> str:
    dates = _extract_signature_dates(text)
    return dates[0] if dates else ""


def _parse_relative_period_value(value: str, text: str, is_end: bool) -> str:
    normalized = _normalize_text(_maybe_fix_mojibake(value or ""))
    if not normalized:
        return ""

    signature_iso = _extract_reference_signature_date(text)
    if not signature_iso:
        return ""

    signature_dt = datetime.fromisoformat(signature_iso)
    number_words = {
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

    years_match = re.search(
        r"\b(\d+|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+anos?\s+apos\s+a\s+assinatura",
        normalized,
    )
    if years_match:
        raw_years = years_match.group(1)
        years = int(raw_years) if raw_years.isdigit() else number_words.get(raw_years, 0)
        if years > 0:
            try:
                return signature_dt.replace(year=signature_dt.year + years).date().isoformat()
            except ValueError:
                return signature_dt.replace(year=signature_dt.year + years, day=28).date().isoformat()

    months_match = re.search(
        r"\b(?:prazo\s+de\s+)?(\d+|sessenta)(?:\s*\(\s*[a-z]+\s*\))?\s+mes(?:es)?\b.*?\b(?:a\s+partir\s+da\s+data\s+de\s+sua\s+assinatura|a\s+partir\s+da\s+data\s+de\s+assinatura|apos\s+a\s+assinatura|a\s+partir\s+da\s+assinatura)\b",
        normalized,
    )
    if months_match:
        raw_months = months_match.group(1)
        months = int(raw_months) if raw_months.isdigit() else number_words.get(raw_months, 0)
        if months > 0:
            return _add_months(signature_dt, months if is_end else 0).date().isoformat()

    if any(
        expr in normalized
        for expr in (
            "imediatamente apos a assinatura",
            "imediatamente apos a data de assinatura",
            "a partir da data de sua assinatura",
            "a partir da data de assinatura",
            "na data de assinatura",
        )
    ):
        return signature_iso

    return ""


def _extract_compact_period_pair(text: str) -> Tuple[str, str]:
    normalized = _normalize_text(_maybe_fix_mojibake(text or ""))
    if not normalized:
        return ("", "")

    match = re.search(
        r"\b((?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zç]*\s*\d{4})\s+a\s+((?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-zç]*\s*\d{4})\b",
        normalized,
        flags=re.IGNORECASE,
    )
    if not match:
        return ("", "")
    return (_clean_spaces(match.group(1)), _clean_spaces(match.group(2)))


def _normalize_date_token(token: str, end_of_month: bool = False) -> str:
    raw = _clean_spaces(_maybe_fix_mojibake(token or ""))
    if not raw:
        return ""

    normalized = _normalize_text(raw)

    m = re.fullmatch(r"(\d{1,2})\s*[\/.-]\s*(\d{1,2})\s*[\/.-]\s*(\d{4})", normalized)
    if m:
        day, month, year = map(int, m.groups())
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return ""

    m = re.fullmatch(r"(\d{1,2})\s*[\/.-]\s*(\d{4})", normalized)
    if m:
        month, year = map(int, m.groups())
        day = _last_day_of_month(year, month) if end_of_month else 1
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return ""

    m = re.fullmatch(r"([a-zç]+)\s*[\/ ]\s*(\d{2,4})", normalized)
    if m:
        month_name, year_raw = m.groups()
        month = MONTHS.get(month_name)
        if month:
            year = _coerce_year(year_raw)
            day = _last_day_of_month(year, month) if end_of_month else 1
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                return ""

    m = re.fullmatch(r"([a-zç]+)\s*(\d{4})", normalized)
    if m:
        month_name, year_raw = m.groups()
        month = MONTHS.get(month_name)
        if month:
            year = _coerce_year(year_raw)
            day = _last_day_of_month(year, month) if end_of_month else 1
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                return ""

    m = re.fullmatch(r"(\d{1,2})\s*(?:o|º|°)?\s+de\s+([a-zç]+)\s+de\s+(\d{4})", normalized)
    if m:
        day, month_name, year = m.groups()
        month = MONTHS.get(month_name)
        if month:
            try:
                return datetime(int(year), month, int(day)).date().isoformat()
            except ValueError:
                return ""

    return ""


def _extract_period_from_text(text: str, prazos: Dict[str, Any]) -> Dict[str, str]:
    inicio_raw = _clean_spaces(str(prazos.get("inicio_raw", "") or ""))
    termino_raw = _clean_spaces(str(prazos.get("termino_raw", "") or ""))
    inicio_norm = ""
    termino_norm = ""

    period_block = _extract_heading_block(text, r"periodo\s+de\s+execucao|inicio\s*\(mes/ano\)|termino\s*\(mes/ano\)")
    base = period_block or _maybe_fix_mojibake(text or "")

    if not inicio_raw:
        inicio_raw = _extract_label_value(base, [r"inicio", r"inicio\s*\(mes/ano\)"])
    if not termino_raw:
        termino_raw = _extract_label_value(base, [r"termino", r"termino\s*\(mes/ano\)"])

    if not inicio_raw or not termino_raw:
        lines = [_clean_spaces(line) for line in base.replace("\r", "\n").splitlines() if _clean_spaces(line)]
        joined = " ".join(lines)
        pair_match = re.search(
            r"in[ií]cio[^:]*:\s*(.+?)\s+t[eé]rmino[^:]*:\s*(.+?)(?:\s+objeto\s*:|$)",
            joined,
            flags=re.IGNORECASE,
        )
        if pair_match:
            if not inicio_raw:
                inicio_raw = _clean_spaces(pair_match.group(1))
            if not termino_raw:
                termino_raw = _clean_spaces(pair_match.group(2))

        if not inicio_raw:
            match = re.search(r"in[ií]cio[^:]*:\s*(.+?)(?:t[eé]rmino[^:]*:|$)", joined, flags=re.IGNORECASE)
            if match:
                inicio_raw = _clean_spaces(match.group(1))
        if not termino_raw:
            match = re.search(r"t[eé]rmino[^:]*:\s*(.+?)(?:objeto\s*:|$)", joined, flags=re.IGNORECASE)
            if match:
                termino_raw = _clean_spaces(match.group(1))

    joined = ""
    if not inicio_raw or not termino_raw:
        lines = [_clean_spaces(line) for line in base.replace("\r", "\n").splitlines() if _clean_spaces(line)]
        joined = " ".join(lines)
    else:
        lines = []

    if not inicio_raw or not termino_raw:
        pair_match = re.search(
            r"in[iÃ­]cio[^:]*:\s*(.+?)\s+t[eÃ©]rmino[^:]*:\s*(.+?)(?:\s+objeto\s*:|$)",
            joined,
            flags=re.IGNORECASE,
        )
        if pair_match:
            if not inicio_raw:
                inicio_raw = _clean_spaces(pair_match.group(1))
            if not termino_raw:
                termino_raw = _clean_spaces(pair_match.group(2))

        if not inicio_raw:
            match = re.search(r"in[iÃ­]cio[^:]*:\s*(.+?)(?:t[eÃ©]rmino[^:]*:|$)", joined, flags=re.IGNORECASE)
            if match:
                inicio_raw = _clean_spaces(match.group(1))
        if not termino_raw:
            match = re.search(r"t[eÃ©]rmino[^:]*:\s*(.+?)(?:objeto\s*:|$)", joined, flags=re.IGNORECASE)
            if match:
                termino_raw = _clean_spaces(match.group(1))

    # Fallback mais resiliente: usa os dois primeiros candidatos do bloco de período.
    period_candidates = _extract_date_like_candidates(base)
    if period_candidates:
        if not inicio_raw:
            inicio_raw = period_candidates[0]
        if len(period_candidates) >= 2 and not termino_raw:
            termino_raw = period_candidates[1]

    normalized_joined = _normalize_text(base)
    normalized_pair = re.search(
        r"inicio[^:]*:\s*(.+?)\s+termino[^:]*:\s*(.+?)(?:\s+objeto|$)",
        normalized_joined,
        flags=re.IGNORECASE,
    )
    if normalized_pair:
        normalized_inicio_raw = _clean_spaces(normalized_pair.group(1))
        normalized_termino_raw = _clean_spaces(normalized_pair.group(2))
        if (not inicio_raw) or (inicio_norm and termino_norm and termino_norm < inicio_norm):
            inicio_raw = normalized_inicio_raw
        if (not termino_raw) or (inicio_norm and termino_norm and termino_norm < inicio_norm):
            termino_raw = normalized_termino_raw

    if inicio_raw:
        for token in _extract_date_like_candidates(inicio_raw):
            inicio_norm = _normalize_date_token(token, end_of_month=False)
            if inicio_norm:
                break

    if termino_raw:
        for token in _extract_date_like_candidates(termino_raw):
            termino_norm = _normalize_date_token(token, end_of_month=True)
            if termino_norm:
                break

    if not inicio_norm:
        inicio_norm = _clean_spaces(str(prazos.get("inicio_data", "") or ""))

    if not termino_norm:
        termino_norm = _clean_spaces(str(prazos.get("termino_data", "") or ""))

    # Revalida contra o bloco de período quando o extrator original trouxe datas incoerentes.
    if period_candidates:
        candidate_inicio = _normalize_date_token(period_candidates[0], end_of_month=False)
        candidate_fim = _normalize_date_token(period_candidates[1], end_of_month=True) if len(period_candidates) >= 2 else ""
        if candidate_inicio and candidate_fim and (not termino_norm or termino_norm < inicio_norm or candidate_fim < candidate_inicio):
            inicio_norm = candidate_inicio
            termino_norm = candidate_fim
            inicio_raw = period_candidates[0]
            termino_raw = period_candidates[1]
        elif candidate_inicio and candidate_fim and (not inicio_norm or not termino_norm or termino_norm < inicio_norm):
            inicio_norm = candidate_inicio
            termino_norm = candidate_fim
            inicio_raw = period_candidates[0]
            termino_raw = period_candidates[1]

    return {
        "prazo_inicio_raw": inicio_raw,
        "prazo_inicio": inicio_norm,
        "prazo_fim_raw": termino_raw,
        "prazo_fim": termino_norm,
    }


def _extract_period_from_snapshot(snapshot: Dict[str, Any], prazos: Dict[str, Any]) -> Dict[str, str]:
    text = str(snapshot.get("text", "") or "")
    period = _extract_period_from_text(text, prazos)

    tables = snapshot.get("tables", []) or []
    table_inicio_raw = ""
    table_fim_raw = ""
    for table in tables:
        table_rows = table.get("rows", []) or []
        table_text = " ".join(
            _clean_spaces(str(cell or ""))
            for row in table_rows
            for cell in (row or [])
            if _clean_spaces(str(cell or ""))
        )
        normalized_table_text = _normalize_text(table_text)
        if "periodo de execucao" not in normalized_table_text:
            continue
        if "inicio" not in normalized_table_text or "termino" not in normalized_table_text:
            continue
        for row in table_rows:
            cells = [_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or ""))]
            if not cells:
                continue
            for idx, cell in enumerate(cells):
                normalized = _normalize_text(cell)
                next_cell = cells[idx + 1] if idx + 1 < len(cells) else ""
                if not table_inicio_raw and normalized.startswith("inicio"):
                    table_inicio_raw = next_cell or (cell.split(":", 1)[1].strip() if ":" in cell else cell)
                if not table_fim_raw and (normalized.startswith("termino") or normalized.startswith("término")):
                    table_fim_raw = next_cell or (cell.split(":", 1)[1].strip() if ":" in cell else cell)
            if table_inicio_raw and table_fim_raw:
                break
        if table_inicio_raw and table_fim_raw:
            break

    table_inicio_raw = _clean_spaces(table_inicio_raw)
    table_fim_raw = _clean_spaces(table_fim_raw)
    table_inicio = _normalize_date_token(table_inicio_raw, end_of_month=False) if table_inicio_raw else ""
    table_fim = _normalize_date_token(table_fim_raw, end_of_month=True) if table_fim_raw else ""

    text_period_complete = bool(period.get("prazo_inicio") and period.get("prazo_fim"))
    text_period_consistent = text_period_complete and period["prazo_fim"] >= period["prazo_inicio"]

    if table_inicio and table_fim and not text_period_consistent:
        return {
            "prazo_inicio_raw": table_inicio_raw,
            "prazo_inicio": table_inicio,
            "prazo_fim_raw": table_fim_raw,
            "prazo_fim": table_fim,
        }

    return period


def _extract_objeto(snapshot: Dict[str, Any], preview: Dict[str, str]) -> str:
    if preview.get("objeto"):
        return preview["objeto"]

    text = _maybe_fix_mojibake(str(snapshot.get("text", "") or ""))
    value = _extract_label_value(text, [r"objeto"])
    if value:
        return value

    tables = snapshot.get("tables", []) or []
    for table in tables:
        for row in table.get("rows", []) or []:
            joined = " ".join(_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or ""))).strip()
            if not joined:
                continue
            normalized = _normalize_text(joined)
            if normalized.startswith("objeto:"):
                parts = joined.split(":", 1)
                if len(parts) == 2:
                    return _clean_spaces(parts[1])
    return ""


def _extract_partner(snapshot: Dict[str, Any], preview: Dict[str, str]) -> str:
    if preview.get("parceiro"):
        return preview["parceiro"]

    tables = snapshot.get("tables", []) or []
    participant2_seen = False
    for table in tables:
        for row in table.get("rows", []) or []:
            cells = [_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or ""))]
            if not cells:
                continue
            joined = " ".join(cells)
            normalized = _normalize_text(joined)
            if "participe 2" in normalized or "participe 2 -" in normalized:
                participant2_seen = True
                continue
            if participant2_seen and len(cells) >= 2 and _normalize_text(cells[0]).startswith("unidade"):
                return cells[1]

    text = _maybe_fix_mojibake(str(snapshot.get("text", "") or ""))
    lines = [_clean_spaces(line) for line in text.replace("\r", "\n").splitlines() if _clean_spaces(line)]
    participant2_seen = False
    for line in lines:
        normalized = _normalize_text(line)
        if "participe 2" in normalized:
            participant2_seen = True
            continue
        if participant2_seen and normalized.startswith("unidade:"):
            parts = line.split(":", 1)
            if len(parts) == 2:
                return _clean_spaces(parts[1])
    return ""


def _extract_atribuicoes(snapshot: Dict[str, Any]) -> str:
    text = str(snapshot.get("text", "") or "")
    blocks = [
        _extract_heading_block(text, r"responsabilidades\s+dos\s+participes"),
        _extract_heading_block(text, r"objetivo\s+geral\s+e\s+objetivos\s+especificos"),
    ]
    for block in blocks:
        if block:
            return _clean_spaces(block)
    return ""


def _extract_metas(snapshot: Dict[str, Any]) -> str:
    text = str(snapshot.get("text", "") or "")
    block = _extract_heading_block(text, r"plano\s+de\s+acao|cronograma\s+de\s+execucao")
    if block:
        return _clean_spaces(block)

    tables = snapshot.get("tables", []) or []
    for table in tables:
        rows = table.get("rows", []) or []
        if not rows:
            continue
        header = " ".join(_clean_spaces(str(cell or "")) for cell in rows[0] if _clean_spaces(str(cell or "")))
        if "METAS" in header.upper():
            fragments = []
            for row in rows:
                joined = " | ".join(_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or "")))
                if joined:
                    fragments.append(joined)
            return " || ".join(fragments)
    return ""


def _extract_acoes(snapshot: Dict[str, Any]) -> str:
    text = str(snapshot.get("text", "") or "")
    block = _extract_heading_block(text, r"metodologia\s+de\s+intervencao|plano\s+de\s+acao|cronograma\s+de\s+execucao")
    if block:
        return _clean_spaces(block)
    return ""


def _classify_record(record: Dict[str, str]) -> Tuple[str, int]:
    focus_fields = [
        record.get("parceiro", ""),
        record.get("vigencia_raw", "") or record.get("prazo_inicio", "") or record.get("prazo_fim", ""),
        record.get("objeto", ""),
        record.get("prazo_inicio", "") or record.get("prazo_inicio_raw", ""),
        record.get("prazo_fim", "") or record.get("prazo_fim_raw", ""),
        record.get("metas_raw", ""),
        record.get("acoes_raw", ""),
    ]
    captured = sum(1 for value in focus_fields if _clean_spaces(value))
    if captured >= 6:
        return ("completo_padronizado", captured)
    if captured >= 4:
        return ("parcial_padronizado", captured)
    return ("extraido_sem_padrao", captured)


def build_normalized_record(
    payload: Dict[str, Any],
    preview: Dict[str, str],
    json_path: Path,
) -> Dict[str, str]:
    snapshot = payload.get("snapshot", {}) or {}
    prazos = payload.get("prazos", {}) or {}
    period = _extract_period_from_snapshot(snapshot, prazos)

    vigencia_raw = preview.get("vigencia", "")
    if not vigencia_raw and (period["prazo_inicio_raw"] or period["prazo_fim_raw"]):
        vigencia_raw = " a ".join(part for part in (period["prazo_inicio_raw"], period["prazo_fim_raw"]) if part)

    record = {
        "captured_at": _clean_spaces(str(payload.get("captured_at", "") or "")),
        "processo": _clean_spaces(str(payload.get("processo", "") or "")),
        "documento": _clean_spaces(str(payload.get("documento", "") or "")),
        "parceiro": _extract_partner(snapshot, preview),
        "vigencia_raw": _clean_spaces(vigencia_raw),
        "vigencia_inicio": period["prazo_inicio"],
        "vigencia_fim": period["prazo_fim"],
        "objeto": _extract_objeto(snapshot, preview),
        "atribuições_raw": _extract_atribuicoes(snapshot),
        "metas_raw": _extract_metas(snapshot),
        "acoes_raw": _extract_acoes(snapshot),
        "prazo_inicio_raw": period["prazo_inicio_raw"],
        "prazo_inicio": period["prazo_inicio"],
        "prazo_fim_raw": period["prazo_fim_raw"],
        "prazo_fim": period["prazo_fim"],
        "snapshot_mode": _clean_spaces(str(snapshot.get("extraction_mode", "") or "")),
        "preview_numero_act": _clean_spaces(str(preview.get("numero_act", "") or "")),
        "json_path": str(json_path),
    }
    status, captured = _classify_record(record)
    record["normalization_status"] = status
    record["captured_focus_fields"] = str(captured)
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
            preview = preview_map.get(processo, {})
            records.append(build_normalized_record(payload, preview, json_path))
        except Exception as exc:
            _log(logger, "warning", "Normalizador PT: falha ao processar %s (%s).", json_path, exc)

    csv_path = output_dir / "pt_normalizado_latest.csv"
    latest_path = csv_path
    columns = [
        "captured_at",
        "processo",
        "documento",
        "parceiro",
        "vigencia_raw",
        "vigencia_inicio",
        "vigencia_fim",
        "objeto",
        "atribuições_raw",
        "metas_raw",
        "acoes_raw",
        "prazo_inicio_raw",
        "prazo_inicio",
        "prazo_fim_raw",
        "prazo_fim",
        "snapshot_mode",
        "preview_numero_act",
        "normalization_status",
        "captured_focus_fields",
        "json_path",
    ]
    csv_writer.write_csv(records, csv_path, columns=columns)

    complete_records = [record for record in records if record.get("normalization_status") == "completo_padronizado"]
    complete_path = output_dir / "pt_normalizado_completo_latest.csv"
    complete_latest = complete_path
    csv_writer.write_csv(complete_records, complete_path, columns=columns)

    _log(
        logger,
        "info",
        "Normalizador PT: CSV consolidado gerado com %d registro(s), completos=%d.",
        len(records),
        len(complete_records),
    )
    return {
        "records": len(records),
        "csv_path": csv_path,
        "latest_path": latest_path,
        "complete_path": complete_path,
        "complete_latest_path": complete_latest,
    }


def _extract_period_from_text(text: str, prazos: Dict[str, Any]) -> Dict[str, str]:
    inicio_raw = _clean_spaces(str(prazos.get("inicio_raw", "") or ""))
    termino_raw = _clean_spaces(str(prazos.get("termino_raw", "") or ""))
    inicio_norm = ""
    termino_norm = ""

    normalized_text = _normalize_text(_maybe_fix_mojibake(text or ""))

    def _slice(label: str) -> str:
        match = re.search(rf"\b{label}\b[^:]*:\s*", normalized_text)
        if not match:
            return ""
        tail = normalized_text[match.end():]
        stop = re.search(
            r"\b(?:inicio|termino|objeto|diagnostico|abrangencia|justificativa|objetivo|metodologia|unidade responsavel|resultados esperados|plano de acao)\b",
            tail,
        )
        if stop:
            tail = tail[:stop.start()]
        return _clean_spaces(tail).strip(" ;,.")

    if not inicio_raw:
        inicio_raw = _slice("inicio")
    if not termino_raw:
        termino_raw = _slice("termino")

    compact_inicio, compact_fim = _extract_compact_period_pair(text)
    if not inicio_raw and compact_inicio:
        inicio_raw = compact_inicio
    if not termino_raw and compact_fim:
        termino_raw = compact_fim

    if inicio_raw:
        inicio_norm = _parse_relative_period_value(inicio_raw, text, is_end=False)
        if not inicio_norm:
            inicio_candidates = [inicio_raw, *_extract_date_like_candidates(inicio_raw)]
            for token in dict.fromkeys(inicio_candidates):
                inicio_norm = _normalize_date_token(token, end_of_month=False)
                if inicio_norm:
                    break
    if termino_raw:
        termino_norm = _parse_relative_period_value(termino_raw, text, is_end=True)
        if not termino_norm:
            termino_candidates = [termino_raw, *_extract_date_like_candidates(termino_raw)]
            for token in dict.fromkeys(termino_candidates):
                termino_norm = _normalize_date_token(token, end_of_month=True)
                if termino_norm:
                    break

    if not inicio_norm:
        inicio_norm = _clean_spaces(str(prazos.get("inicio_data", "") or ""))
    if not termino_norm:
        termino_norm = _clean_spaces(str(prazos.get("termino_data", "") or ""))

    if inicio_norm and not termino_norm and termino_raw:
        termino_norm = _parse_relative_period_value(termino_raw, text, is_end=True)
    if termino_norm and not inicio_norm and inicio_raw:
        inicio_norm = _parse_relative_period_value(inicio_raw, text, is_end=False)

    return {
        "prazo_inicio_raw": inicio_raw,
        "prazo_inicio": inicio_norm,
        "prazo_fim_raw": termino_raw,
        "prazo_fim": termino_norm,
    }


def _extract_period_from_snapshot(snapshot: Dict[str, Any], prazos: Dict[str, Any]) -> Dict[str, str]:
    text = str(snapshot.get("text", "") or "")

    tables = snapshot.get("tables", []) or []
    for table in tables:
        rows = table.get("rows", []) or []
        flattened = " ".join(
            _clean_spaces(str(cell or ""))
            for row in rows
            for cell in (row or [])
            if _clean_spaces(str(cell or ""))
        )
        normalized_flattened = _normalize_text(flattened)
        if "inicio" not in normalized_flattened or "termino" not in normalized_flattened:
            continue

        table_inicio_raw = ""
        table_fim_raw = ""
        for row in rows:
            cells = [_clean_spaces(str(cell or "")) for cell in row if _clean_spaces(str(cell or ""))]
            for idx, cell in enumerate(cells):
                normalized_cell = _normalize_text(cell)
                next_cell = cells[idx + 1] if idx + 1 < len(cells) else ""
                if not table_inicio_raw and normalized_cell.startswith("inicio"):
                    table_inicio_raw = next_cell or _clean_spaces(cell.split(":", 1)[1] if ":" in cell else "")
                if not table_fim_raw and normalized_cell.startswith("termino"):
                    table_fim_raw = next_cell or _clean_spaces(cell.split(":", 1)[1] if ":" in cell else "")
            if table_inicio_raw and table_fim_raw:
                break

        if not (table_inicio_raw and table_fim_raw):
            line_text = re.sub(r"\s+", " ", flattened).strip()
            pair_match = re.search(
                r"in[ií]cio[^:]*[: ]\s*(.+?)\s+t[ée]rmino[^:]*[: ]\s*(.+?)(?:\s{2,}|$)",
                line_text,
                flags=re.IGNORECASE,
            )
            if pair_match:
                table_inicio_raw = _clean_spaces(pair_match.group(1))
                table_fim_raw = _clean_spaces(pair_match.group(2))

        if not table_inicio_raw or not table_fim_raw:
            compact_inicio, compact_fim = _extract_compact_period_pair(flattened)
            if compact_inicio and compact_fim:
                table_inicio_raw = table_inicio_raw or compact_inicio
                table_fim_raw = table_fim_raw or compact_fim

        if table_inicio_raw and table_fim_raw:
            table_inicio = _normalize_date_token(table_inicio_raw, end_of_month=False)
            table_fim = _normalize_date_token(table_fim_raw, end_of_month=True)
            if not table_inicio:
                table_inicio = _parse_relative_period_value(table_inicio_raw, text, is_end=False)
            if not table_fim:
                table_fim = _parse_relative_period_value(table_fim_raw, text, is_end=True)
            if table_inicio and table_fim and table_fim >= table_inicio:
                return {
                    "prazo_inicio_raw": table_inicio_raw,
                    "prazo_inicio": table_inicio,
                    "prazo_fim_raw": table_fim_raw,
                    "prazo_fim": table_fim,
                }

    return _extract_period_from_text(text, prazos)
