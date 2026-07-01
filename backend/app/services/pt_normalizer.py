from __future__ import annotations

import calendar
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.output import csv_writer
from app.services.normalization_contract import (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    SOURCE_DERIVED,
    SOURCE_DOCUMENT_TEXT,
    SOURCE_MISSING,
    SOURCE_PREVIEW,
    SOURCE_TABLE,
    build_document_contract,
    make_field,
    make_missing_field,
)

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
PERIOD_CLASS_EXPLICIT_DATE = "data_explicita"
PERIOD_CLASS_RELATIVE_SIGNATURE = "prazo_relativo_assinatura"
PERIOD_CLASS_RELATIVE_PUBLICATION = "prazo_relativo_publicacao"
PERIOD_CLASS_RELATIVE_APPROVAL = "prazo_relativo_aprovacao"
PERIOD_CLASS_NARRATIVE_NO_BASE = "prazo_narrativo_sem_data_base"
PERIOD_CLASS_CONTAMINATED_TEXT = "texto_contaminado"
PERIOD_CLASS_MISSING = "ausente"
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


@dataclass(frozen=True)
class NormalizedPeriod:
    prazo_inicio_raw: str = ""
    prazo_inicio: str = ""
    prazo_fim_raw: str = ""
    prazo_fim: str = ""
    period_source: str = PERIOD_SOURCE_MISSING
    period_warning: str = ""
    period_class: str = PERIOD_CLASS_MISSING
    rule_amount: str = ""
    rule_unit: str = ""
    rule_anchor: str = ""
    missing_base_date: str = ""

    def to_record(self) -> Dict[str, str]:
        return asdict(self)


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


def _signature_dates_value(dates: List[str]) -> str:
    return " | ".join(_clean_spaces(date_value) for date_value in dates if _clean_spaces(date_value))


def _signature_date_value(dates: List[str]) -> str:
    return max(dates) if dates else ""


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


def _anchor_from_text(value: str) -> str:
    normalized = _normalize_text(value)
    if re.search(r"\b(publicacao|publicado|diario oficial|dou)\b", normalized):
        return "publicacao"
    if re.search(r"\b(aprovacao|aprovado|aprovada)\b", normalized):
        return "aprovacao"
    if re.search(r"\b(assinatura|assinado|assinada)\b", normalized):
        return "assinatura"
    return ""


def _period_class_for_anchor(anchor: str) -> str:
    return {
        "assinatura": PERIOD_CLASS_RELATIVE_SIGNATURE,
        "publicacao": PERIOD_CLASS_RELATIVE_PUBLICATION,
        "aprovacao": PERIOD_CLASS_RELATIVE_APPROVAL,
    }.get(anchor, PERIOD_CLASS_NARRATIVE_NO_BASE)


def _period_source_for_anchor(anchor: str, has_base_date: bool) -> str:
    if anchor == "assinatura" and has_base_date:
        return PERIOD_SOURCE_SIGNATURE
    if anchor:
        return PERIOD_SOURCE_RELATIVE
    return PERIOD_SOURCE_NOISE


def _base_date_from_context(anchor: str, context: Dict[str, Any]) -> str:
    keys_by_anchor = {
        "assinatura": ("signature_date", "assinatura_data", "data_assinatura", "base_signature_date"),
        "publicacao": ("publication_date", "publicacao_data", "data_publicacao", "base_publication_date"),
        "aprovacao": ("approval_date", "aprovacao_data", "data_aprovacao", "base_approval_date"),
    }
    for key in keys_by_anchor.get(anchor, ()):
        value = _clean_spaces(str(context.get(key, "") or ""))
        if value:
            return value
    return ""


def _parse_amount(raw_amount: str) -> int:
    normalized = _normalize_text(raw_amount).replace(" ", "_")
    return int(raw_amount) if str(raw_amount).isdigit() else NUMBER_WORDS.get(normalized, 0)


def _normalize_rule_unit(unit: str) -> str:
    normalized = _normalize_text(unit)
    if "ano" in normalized:
        return "anos"
    if "mes" in normalized:
        return "meses"
    return normalized


def _duration_match(value: str) -> Optional[re.Match[str]]:
    normalized = _normalize_text(value)
    return re.search(
        r"\b(\d+|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|sessenta)"
        r"(?:\s*\([^)]+\))?\s+(mes(?:es)?|anos?)\b",
        normalized,
        flags=re.IGNORECASE,
    )


def _add_rule_duration(base_iso: str, amount: int, unit: str) -> str:
    try:
        base = datetime.fromisoformat(base_iso)
        if "ano" in unit:
            return base.replace(year=base.year + amount).date().isoformat()
        return _add_months(base, amount).date().isoformat()
    except Exception:
        return ""


def _period_value_is_noise(value: str) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return False
    if any(marker in normalized for marker in INVALID_MARKERS + WEAK_PERIOD_MARKERS):
        return True
    if len(normalized.split()) >= 12 and not re.search(DATE_TOKEN, normalized, flags=re.IGNORECASE):
        return True
    return False


def _empty_period(*, source: str = PERIOD_SOURCE_MISSING, warning: str = "") -> NormalizedPeriod:
    period_class = PERIOD_CLASS_CONTAMINATED_TEXT if source == PERIOD_SOURCE_NOISE else PERIOD_CLASS_MISSING
    return NormalizedPeriod(period_source=source, period_warning=warning, period_class=period_class)


def normalize_pt_period(raw_inicio: str, raw_fim: str, context: Dict[str, Any]) -> NormalizedPeriod:
    start_raw = _clean_spaces(str(raw_inicio or ""))
    end_raw = _clean_spaces(str(raw_fim or ""))
    context = context or {}
    start_iso = _clean_spaces(str(context.get("inicio_data", "") or "")) or _normalize_date_token(start_raw, end_of_month=False)
    end_iso = _clean_spaces(str(context.get("termino_data", "") or "")) or _normalize_date_token(end_raw, end_of_month=True)
    raw_blob = _clean_spaces(" ".join(part for part in (start_raw, end_raw) if part))
    raw_blob_normalized = _normalize_text(raw_blob)
    if raw_blob_normalized and any(marker in raw_blob_normalized for marker in INVALID_MARKERS):
        return NormalizedPeriod(
            prazo_inicio_raw=start_raw,
            prazo_fim_raw=end_raw,
            period_source=PERIOD_SOURCE_NOISE,
            period_warning="periodo_bruto_contaminado_ou_narrativo",
            period_class=PERIOD_CLASS_CONTAMINATED_TEXT,
        )
    anchor = _anchor_from_text(raw_blob)
    duration = _duration_match(raw_blob)

    if not (anchor or duration) and (start_raw and _period_value_is_noise(start_raw) or end_raw and _period_value_is_noise(end_raw)):
        return NormalizedPeriod(
            prazo_inicio_raw=start_raw,
            prazo_fim_raw=end_raw,
            period_source=PERIOD_SOURCE_NOISE,
            period_warning="periodo_bruto_contaminado_ou_narrativo",
            period_class=PERIOD_CLASS_CONTAMINATED_TEXT,
        )

    if start_iso and end_iso and end_iso >= start_iso:
        return NormalizedPeriod(
            prazo_inicio_raw=start_raw or start_iso,
            prazo_inicio=start_iso,
            prazo_fim_raw=end_raw or end_iso,
            prazo_fim=end_iso,
            period_source=PERIOD_SOURCE_DIRECT,
            period_class=PERIOD_CLASS_EXPLICIT_DATE,
        )

    if not raw_blob:
        return _empty_period()

    has_immediate_start = bool(re.search(r"\b(imediatamente|a partir|na data)\b", _normalize_text(start_raw or raw_blob)))
    amount = 0
    unit = ""
    if duration:
        amount = _parse_amount(duration.group(1))
        unit = _normalize_rule_unit(duration.group(2))

    if anchor:
        base_date = _base_date_from_context(anchor, context)
        period_class = _period_class_for_anchor(anchor)
        if not base_date:
            return NormalizedPeriod(
                prazo_inicio_raw=start_raw,
                prazo_fim_raw=end_raw,
                period_source=PERIOD_SOURCE_RELATIVE,
                period_warning=f"periodo_relativo_{anchor}_sem_data_base",
                period_class=period_class,
                rule_amount=str(amount) if amount else "",
                rule_unit=unit,
                rule_anchor=anchor,
                missing_base_date="true",
            )
        start_value = base_date if (has_immediate_start or anchor in _normalize_text(start_raw)) else ""
        end_value = _add_rule_duration(base_date, amount, unit) if amount and unit else ""
        warning = "" if start_value and end_value else "periodo_relativo_sem_regra_completa"
        return NormalizedPeriod(
            prazo_inicio_raw=start_raw or f"a partir da {anchor}",
            prazo_inicio=start_value,
            prazo_fim_raw=end_raw,
            prazo_fim=end_value,
            period_source=_period_source_for_anchor(anchor, bool(base_date)),
            period_warning=warning,
            period_class=period_class,
            rule_amount=str(amount) if amount else "",
            rule_unit=unit,
            rule_anchor=anchor,
            missing_base_date="",
        )

    if duration or re.search(
        r"\b(prazo|vigorar|vigencia|meses|anos|imediatamente|apos|a partir|ate a conclusao|conclusao das atividades)\b",
        _normalize_text(raw_blob),
    ):
        return NormalizedPeriod(
            prazo_inicio_raw=start_raw,
            prazo_fim_raw=end_raw,
            period_source=PERIOD_SOURCE_RELATIVE,
            period_warning="periodo_narrativo_sem_data_base",
            period_class=PERIOD_CLASS_NARRATIVE_NO_BASE,
            rule_amount=str(amount) if amount else "",
            rule_unit=unit,
            missing_base_date="true",
        )

    return NormalizedPeriod(
        prazo_inicio_raw=start_raw,
        prazo_fim_raw=end_raw,
        period_source=PERIOD_SOURCE_NOISE,
        period_warning="periodo_bruto_contaminado_ou_narrativo",
        period_class=PERIOD_CLASS_CONTAMINATED_TEXT,
    )


def _extract_period_from_snapshot(snapshot: Dict[str, Any], prazos: Dict[str, Any]) -> Dict[str, str]:
    text = _prepare_text(str(snapshot.get("text", "") or ""))
    normalized = _normalize_text(text)
    empty = _empty_period().to_record()
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
        period = normalize_pt_period(start_raw, end_raw, {})
        if period.prazo_inicio and period.prazo_fim:
            return period.to_record()

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
        period = normalize_pt_period(
            "a partir da assinatura",
            duration.group(0),
            {"signature_date": signature_iso},
        )
        if period.prazo_inicio and period.prazo_fim:
            return period.to_record()

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
            period = normalize_pt_period(
                "a partir da assinatura",
                relative.group(0) if relative else "",
                {"signature_date": signature_iso},
            )
            if period.prazo_inicio and period.prazo_fim:
                return period.to_record()

    start_raw = _clean_spaces(str(prazos.get("inicio_raw", "") or ""))
    end_raw = _clean_spaces(str(prazos.get("termino_raw", "") or ""))
    start_iso = _clean_spaces(str(prazos.get("inicio_data", "") or ""))
    end_iso = _clean_spaces(str(prazos.get("termino_data", "") or ""))
    context = {
        "inicio_data": start_iso,
        "termino_data": end_iso,
        "signature_date": signature_iso,
    }
    period = normalize_pt_period(start_raw, end_raw, context)
    if period.period_class != PERIOD_CLASS_MISSING and (
        period.prazo_inicio
        or period.prazo_fim
        or period.period_source in {PERIOD_SOURCE_NOISE, PERIOD_SOURCE_RELATIVE}
    ):
        return period.to_record()

    raw_blob = " ".join(part for part in (start_raw, end_raw) if _clean_spaces(part))
    if raw_blob:
        if _looks_like_relative_signature_reference(raw_blob):
            return normalize_pt_period(start_raw, end_raw, context).to_record()

    if re.search(r"(a partir da assinatura|apos a assinatura|imediatamente apos a assinatura)", normalized, flags=re.IGNORECASE):
        return normalize_pt_period("a partir da assinatura", "", context).to_record()

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


def _field_or_missing(
    *,
    value: str,
    raw_value: str = "",
    source_type: str,
    confidence: str,
    rule_id: str,
    warning: str = "",
) -> Dict[str, Any]:
    if not _clean_spaces(str(value or "")):
        return make_missing_field(rule_id=rule_id, warning=warning or "missing")
    return make_field(
        value=value,
        raw_value=raw_value or value,
        source_type=source_type,
        confidence=confidence,
        rule_id=rule_id,
        warning=warning,
    )


def _period_source_type(period_source: str) -> str:
    if period_source == PERIOD_SOURCE_DIRECT:
        return SOURCE_DOCUMENT_TEXT
    if period_source == PERIOD_SOURCE_SIGNATURE:
        return SOURCE_DERIVED
    return SOURCE_MISSING


def _build_contract_fields(
    *,
    record: Dict[str, str],
    preview: Dict[str, str],
    snapshot: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    preview_partner = _clean_spaces(str(preview.get("parceiro", "") or ""))
    preview_objeto = _clean_spaces(str(preview.get("objeto", "") or ""))
    tables = snapshot.get("tables", []) or []
    metas_source = SOURCE_TABLE if tables and _clean_spaces(record.get("metas_raw", "")) else SOURCE_DOCUMENT_TEXT
    acoes_source = SOURCE_TABLE if tables and _clean_spaces(record.get("acoes_raw", "")) else SOURCE_DOCUMENT_TEXT
    period_source = _clean_spaces(record.get("period_source", ""))
    period_source_type = _period_source_type(period_source)
    period_confidence = CONFIDENCE_HIGH if period_source == PERIOD_SOURCE_DIRECT else CONFIDENCE_MEDIUM
    data_assinatura = _clean_spaces(record.get("data_assinatura", ""))

    return {
        "parceiro": _field_or_missing(
            value=record.get("parceiro", ""),
            raw_value=preview_partner if preview_partner and record.get("parceiro", "") == preview_partner else record.get("parceiro", ""),
            source_type=SOURCE_PREVIEW if preview_partner and record.get("parceiro", "") == preview_partner else SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM if preview_partner and record.get("parceiro", "") == preview_partner else CONFIDENCE_HIGH,
            rule_id="pt.parceiro.preview_or_document_text",
            warning="preview_fallback" if preview_partner and record.get("parceiro", "") == preview_partner else "",
        ),
        "vigencia_inicio": _field_or_missing(
            value=record.get("vigencia_inicio", ""),
            raw_value=record.get("prazo_inicio_raw", ""),
            source_type=period_source_type,
            confidence=period_confidence,
            rule_id=f"pt.vigencia.{period_source or PERIOD_SOURCE_MISSING}",
            warning=record.get("period_warning", ""),
        ),
        "vigencia_fim": _field_or_missing(
            value=record.get("vigencia_fim", ""),
            raw_value=record.get("prazo_fim_raw", ""),
            source_type=period_source_type,
            confidence=period_confidence,
            rule_id=f"pt.vigencia.{period_source or PERIOD_SOURCE_MISSING}",
            warning=record.get("period_warning", ""),
        ),
        "data_assinatura": _field_or_missing(
            value=data_assinatura,
            raw_value=record.get("datas_assinatura", "") or data_assinatura,
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="pt.data_assinatura.assinaturas_eletronicas_ou_fecho",
        ),
        "objeto": _field_or_missing(
            value=record.get("objeto", ""),
            raw_value=preview_objeto if preview_objeto and record.get("objeto", "") == preview_objeto else record.get("objeto", ""),
            source_type=SOURCE_PREVIEW if preview_objeto and record.get("objeto", "") == preview_objeto else SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM if preview_objeto and record.get("objeto", "") == preview_objeto else CONFIDENCE_HIGH,
            rule_id="pt.objeto.preview_or_document_text",
            warning="preview_fallback" if preview_objeto and record.get("objeto", "") == preview_objeto else "",
        ),
        "metas_raw": _field_or_missing(
            value=record.get("metas_raw", ""),
            source_type=metas_source,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="pt.execucao.metas",
        ),
        "acoes_raw": _field_or_missing(
            value=record.get("acoes_raw", ""),
            source_type=acoes_source,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="pt.execucao.acoes",
        ),
    }


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
    parceiro = _extract_partner(snapshot, preview)
    objeto = _extract_objeto(snapshot, preview)
    atribuicoes = _extract_atribuicoes(snapshot)
    metas = _extract_metas(snapshot)
    acoes = _extract_acoes(snapshot)
    signature_dates = _signature_dates(str(snapshot.get("text", "") or ""))
    record = {
        "captured_at": _clean_spaces(str(payload.get("captured_at", "") or "")),
        "requested_type": _clean_spaces(str(payload.get("requested_type", "") or "")) or REQUESTED_TYPE_PT,
        "resolved_document_type": _clean_spaces(str(analysis.get("resolved_document_type", "") or "")) or _clean_spaces(str(payload.get("resolved_document_type", "") or "")) or RESOLVED_TYPE_PT,
        "processo": _clean_spaces(str(payload.get("processo", "") or "")),
        "documento": _clean_spaces(str(payload.get("documento", "") or "")),
        "parceiro": parceiro,
        "data_assinatura": _signature_date_value(signature_dates),
        "datas_assinatura": _signature_dates_value(signature_dates),
        "vigencia_raw": vigencia_raw,
        "vigencia_inicio": period["prazo_inicio"],
        "vigencia_fim": period["prazo_fim"],
        "objeto": objeto,
        ATTRIBUICOES_COLUMN: atribuicoes,
        "metas_raw": metas,
        "acoes_raw": acoes,
        "prazo_inicio_raw": period["prazo_inicio_raw"],
        "prazo_inicio": period["prazo_inicio"],
        "prazo_fim_raw": period["prazo_fim_raw"],
        "prazo_fim": period["prazo_fim"],
        "period_source": period["period_source"],
        "period_warning": period["period_warning"],
        "period_class": period.get("period_class", ""),
        "rule_amount": period.get("rule_amount", ""),
        "rule_unit": period.get("rule_unit", ""),
        "rule_anchor": period.get("rule_anchor", ""),
        "missing_base_date": period.get("missing_base_date", ""),
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
    contract_fields = _build_contract_fields(record=record, preview=preview, snapshot=snapshot)
    record["normalization_contract"] = build_document_contract(
        processo=record["processo"],
        requested_type=record["requested_type"],
        resolved_document_type=record["resolved_document_type"],
        documento=record["documento"] or None,
        found=True,
        is_canonical_candidate=is_canonical_candidate,
        validation_status=record["validation_status"],
        publication_status=record["publication_status"],
        normalization_status=record["normalization_status"],
        fields=contract_fields,
        extra_issues=[record.get("period_warning", "")],
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
        "data_assinatura",
        "datas_assinatura",
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
        "period_class",
        "rule_amount",
        "rule_unit",
        "rule_anchor",
        "missing_base_date",
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

    diagnostic_columns = [
        "processo",
        "documento",
        "prazo_inicio_raw",
        "prazo_inicio",
        "prazo_fim_raw",
        "prazo_fim",
        "period_class",
        "period_source",
        "rule_amount",
        "rule_unit",
        "rule_anchor",
        "missing_base_date",
        "period_warning",
        "normalization_status",
        "publication_status",
        "json_path",
    ]
    diagnostics_path = output_dir / "pt_period_diagnostics_latest.csv"
    csv_writer.write_csv(records, diagnostics_path, columns=diagnostic_columns)

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
        "diagnostics_path": diagnostics_path,
        "complete_path": complete_path,
        "complete_latest_path": complete_path,
    }
