from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

DATEY_LABEL_HINTS = (
    "inicio",
    "termino",
    "assinatura",
    "vigencia",
    "prazo",
    "periodo",
    "data",
    "duracao",
    "execucao",
)

MONTH_HINTS = (
    "jan",
    "fev",
    "mar",
    "abr",
    "mai",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dez",
    "janeiro",
    "fevereiro",
    "marco",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)

NOISE_MARKERS = (
    "http://",
    "https://",
    "documento assinado eletronicamente",
    "a autenticidade do documento pode ser conferida",
    "codigo verificador",
    "codigo crc",
    "controlador_externo.php",
    "criado por ",
    "super.gov.br",
)

RE_LABEL_COLON = re.compile(
    r"(?P<label>[^:\n]{2,80})\s*:\s*(?P<value>[^:\n]{0,300})",
    re.IGNORECASE,
)
RE_DATE_LIKE = re.compile(
    r"(\b\d{1,2}\s*[\/\-\.]\s*\d{1,2}\s*[\/\-\.]\s*\d{2,4}\b)|"
    r"(\b\d{1,2}\s*[\/\-\.]\s*\d{4}\b)|"
    r"(\b[A-Za-zA-ZÀ-ÿ]{3,12}\s*[\/\-\.]\s*\d{2,4}\b)|"
    r"(\b\d{1,2}\s+de\s+[A-Za-zA-ZÀ-ÿ]{3,12}\s+de\s+\d{4}\b)|"
    r"(\b(ap[oó]s|antes|imediatamente|sem data|na data de assinatura|a partir da assinatura)\b)",
    re.IGNORECASE,
)


def _fold_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def _norm(value: str) -> str:
    value = (value or "").replace("\u00A0", " ").replace("\r", "\n")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n+", "\n", value)
    return value.strip()


def _is_noise_text(value: str) -> bool:
    normalized = _fold_text(_norm(value))
    if not normalized:
        return True
    return any(marker in normalized for marker in NOISE_MARKERS)


def _looks_like_label(value: str) -> bool:
    normalized = _norm(value)
    folded = _fold_text(normalized)
    if not normalized or _is_noise_text(normalized):
        return False
    if len(normalized) < 3 or len(normalized) > 60:
        return False
    if re.search(r"https?://", normalized, flags=re.IGNORECASE):
        return False
    if re.search(r"[|@]", normalized):
        return False
    words = [word for word in re.split(r"\s+", folded) if word]
    if not words or len(words) > 6:
        return False
    if len(re.findall(r"[a-z]", folded)) < 4:
        return False
    return any(hint in folded for hint in DATEY_LABEL_HINTS)


def _classify_field_key(label_raw: str) -> str:
    label = _fold_text(_norm(label_raw))
    if "inicio" in label:
        return "inicio"
    if "termino" in label or "fim" in label:
        return "termino"
    if "assinatura" in label:
        return "assinatura"
    if "vigencia" in label:
        return "vigencia"
    if "prazo" in label:
        return "prazo"
    if "periodo" in label:
        return "periodo"
    if "data" in label:
        return "data"
    return "outro"


def _value_has_date_like(value: str) -> int:
    normalized = _norm(value)
    folded = _fold_text(normalized)
    if not normalized or _is_noise_text(normalized):
        return 0
    if RE_DATE_LIKE.search(normalized):
        return 1
    if any(month in folded for month in MONTH_HINTS):
        return 1
    return 0


def _is_meaningful_value(value: str) -> bool:
    normalized = _norm(value)
    if not normalized or _is_noise_text(normalized):
        return False
    if len(normalized) <= 2:
        return False
    return True


@dataclass
class RawField:
    field_key: str
    label_raw: str
    value_raw: str
    origin: str
    origin_ref: str
    section_hint: str
    evidence_snippet: str


def collect_raw_fields(
    snapshot_text: str,
    snapshot_tables: Optional[List[List[List[str]]]] = None,
) -> List[RawField]:
    snapshot_tables = snapshot_tables or []
    text = _norm(snapshot_text or "")
    fields: List[RawField] = []
    seen: set[tuple[str, str, str, str]] = set()

    def append_field(field: RawField) -> None:
        key = (
            field.field_key,
            _norm(field.label_raw),
            _norm(field.value_raw),
            field.origin_ref,
        )
        if key in seen:
            return
        seen.add(key)
        fields.append(field)

    for table_index, table in enumerate(snapshot_tables):
        rows = table.get("rows", []) if isinstance(table, dict) else table
        for row_index, row in enumerate(rows):
            if not row:
                continue
            label = _norm(row[0] if len(row) > 0 else "")
            value = " | ".join(
                cell for cell in (_norm(col) for col in row[1:]) if cell
            )
            if not _looks_like_label(label):
                continue
            if not _is_meaningful_value(value):
                continue
            if not _value_has_date_like(value) and _classify_field_key(label) == "outro":
                continue
            append_field(
                RawField(
                    field_key=_classify_field_key(label),
                    label_raw=label,
                    value_raw=value,
                    origin="table",
                    origin_ref=f"table#{table_index} row#{row_index}",
                    section_hint="",
                    evidence_snippet=(f"{label}: {value}")[:180],
                )
            )

    lines = text.split("\n") if text else []
    for line_index, line in enumerate(lines):
        compact = _norm(line)
        if not compact or _is_noise_text(compact):
            continue

        for match in RE_LABEL_COLON.finditer(compact):
            label = _norm(match.group("label"))
            value = _norm(match.group("value"))
            if not _looks_like_label(label):
                continue
            if not _is_meaningful_value(value):
                continue
            if not _value_has_date_like(value):
                continue
            append_field(
                RawField(
                    field_key=_classify_field_key(label),
                    label_raw=label,
                    value_raw=value,
                    origin="text",
                    origin_ref=f"line#{line_index}",
                    section_hint="",
                    evidence_snippet=(f"{label}: {value}")[:180],
                )
            )

    for line_index, line in enumerate(lines):
        compact = _norm(line)
        if not compact or _is_noise_text(compact):
            continue
        if RE_LABEL_COLON.search(compact):
            continue
        if not RE_DATE_LIKE.search(compact):
            continue
        if len(compact) > 160:
            continue
        append_field(
            RawField(
                field_key="data",
                label_raw="",
                value_raw=compact,
                origin="text",
                origin_ref=f"line#{line_index}",
                section_hint="",
                evidence_snippet=compact[:180],
            )
        )

    return fields


def export_raw_fields_csv(
    out_csv_path: str,
    processo_sei: str,
    doc_title: str,
    doc_url: str,
    raw_fields: List[RawField],
    captured_at: Optional[str] = None,
) -> None:
    captured_at = captured_at or datetime.utcnow().isoformat()

    header = [
        "captured_at",
        "processo_sei",
        "doc_title",
        "doc_url",
        "field_key",
        "label_raw",
        "value_raw",
        "value_is_empty",
        "value_has_date_like",
        "origin",
        "origin_ref",
        "section_hint",
        "evidence_snippet",
    ]

    write_header = False
    try:
        with open(out_csv_path, "r", encoding="utf-8", newline="") as _:
            pass
    except FileNotFoundError:
        write_header = True

    with open(out_csv_path, "a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=header)
        if write_header:
            writer.writeheader()

        for raw_field in raw_fields:
            value_raw = raw_field.value_raw or ""
            writer.writerow(
                {
                    "captured_at": captured_at,
                    "processo_sei": processo_sei,
                    "doc_title": doc_title,
                    "doc_url": doc_url,
                    "field_key": raw_field.field_key,
                    "label_raw": raw_field.label_raw,
                    "value_raw": value_raw,
                    "value_is_empty": 1 if not value_raw.strip() else 0,
                    "value_has_date_like": _value_has_date_like(value_raw),
                    "origin": raw_field.origin,
                    "origin_ref": raw_field.origin_ref,
                    "section_hint": raw_field.section_hint,
                    "evidence_snippet": raw_field.evidence_snippet,
                }
            )
