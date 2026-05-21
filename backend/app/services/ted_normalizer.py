from __future__ import annotations

import calendar
import json
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from app.output import csv_writer
from app.services.act_normalizer import PUBLICATION_STATUS_GOLD


RICH_COLUMNS = [
    "processo",
    "documento",
    "numero_ted",
    "ano_ted",
    "objeto",
    "unidade_descentralizadora",
    "unidade_descentralizada",
    "valor_global",
    "vigencia_inicio",
    "vigencia_fim",
    "plano_aplicacao",
    "cronograma_desembolso",
    "metas",
    "prestacao_contas",
    "validation_status",
    "publication_status",
    "normalization_status",
    "quality_status",
    "quality_notes",
    "json_path",
]

RAW_SOURCE_COLUMNS = [
    "numero_ted_raw",
    "numero_ted_source",
    "ano_ted_raw",
    "ano_ted_source",
    "objeto_raw",
    "objeto_source",
    "unidade_descentralizadora_raw",
    "unidade_descentralizadora_source",
    "unidade_descentralizada_raw",
    "unidade_descentralizada_source",
    "valor_global_raw",
    "valor_global_source",
    "vigencia_inicio_raw",
    "vigencia_inicio_source",
    "vigencia_fim_raw",
    "vigencia_fim_source",
    "plano_aplicacao_raw",
    "plano_aplicacao_source",
    "cronograma_desembolso_raw",
    "cronograma_desembolso_source",
    "metas_raw",
    "metas_source",
    "prestacao_contas_raw",
    "prestacao_contas_source",
]

DIAGNOSTIC_COLUMNS = [
    "processo",
    "documento",
    "field_name",
    "value",
    "raw_value",
    "source",
    "status",
    "rule_id",
    "json_path",
]

MONTHS = {
    "janeiro": 1,
    "jan": 1,
    "fevereiro": 2,
    "fev": 2,
    "marco": 3,
    "mar": 3,
    "abril": 4,
    "abr": 4,
    "maio": 5,
    "mai": 5,
    "junho": 6,
    "jun": 6,
    "julho": 7,
    "jul": 7,
    "agosto": 8,
    "ago": 8,
    "setembro": 9,
    "set": 9,
    "outubro": 10,
    "out": 10,
    "novembro": 11,
    "nov": 11,
    "dezembro": 12,
    "dez": 12,
}


@dataclass
class ExtractedField:
    value: str = ""
    raw_value: str = ""
    source: str = "missing"
    rule_id: str = ""

    @property
    def status(self) -> str:
        return "extracted" if self.value else "missing"


def _clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _norm(value: str) -> str:
    return _strip_accents(_clean_spaces(value)).lower()


def _table_rows(table: Any) -> List[List[str]]:
    if not isinstance(table, dict):
        return []
    rows = table.get("rows", [])
    if not isinstance(rows, list):
        return []
    normalized: List[List[str]] = []
    for row in rows:
        if isinstance(row, list):
            normalized.append([_clean_spaces(cell) for cell in row])
        else:
            normalized.append([_clean_spaces(row)])
    return normalized


def _tables(snapshot: Dict[str, Any]) -> List[List[List[str]]]:
    return [_table_rows(table) for table in snapshot.get("tables", []) or [] if _table_rows(table)]


def _rows_text(rows: Sequence[Sequence[str]]) -> str:
    return "\n".join(" | ".join(cell for cell in row if cell) for row in rows if any(row)).strip()


def _section_from_tables(
    snapshot: Dict[str, Any],
    include: Sequence[str],
    exclude: Sequence[str] = (),
    *,
    heading_only: bool = False,
) -> ExtractedField:
    include_norm = [_norm(term) for term in include]
    exclude_norm = [_norm(term) for term in exclude]
    for index, rows in enumerate(_tables(snapshot), start=1):
        text = _rows_text(rows)
        heading = " ".join(rows[0]) if rows else ""
        ntext = _norm(text)
        nheading = _norm(heading)
        haystack = nheading if heading_only else ntext
        if all(term in haystack for term in include_norm) and not any(term in ntext for term in exclude_norm):
            body = rows[1:] if len(rows) > 1 else rows
            value = _rows_text(body) or text
            return ExtractedField(value=_clean_spaces(value), raw_value=text, source=f"snapshot.tables[{index}]", rule_id="ted.table.section")
    return ExtractedField(rule_id="ted.table.section")


def _section_from_text(text: str, headings: Sequence[str], stop_headings: Sequence[str]) -> ExtractedField:
    if not text:
        return ExtractedField(rule_id="ted.text.section")
    lines = [line.strip() for line in text.replace("\r", "\n").split("\n")]
    normalized_headings = [_norm(heading) for heading in headings]
    normalized_stops = [_norm(heading) for heading in stop_headings]
    start = -1
    for idx, line in enumerate(lines):
        nline = _norm(line)
        for heading in normalized_headings:
            if heading not in nline:
                continue
            if nline.startswith(heading) or re.match(rf"^\d+(?:\.\d+)*\.?\s*{re.escape(heading)}\b", nline):
                start = idx
                break
        if start >= 0:
            break
    if start < 0:
        return ExtractedField(rule_id="ted.text.section")

    end = len(lines)
    for idx in range(start + 1, len(lines)):
        nline = _norm(lines[idx])
        if any(stop in nline for stop in normalized_stops):
            end = idx
            break
    raw = "\n".join(line for line in lines[start:end] if line).strip()
    body = "\n".join(line for line in lines[start + 1 : end] if line).strip() or raw
    return ExtractedField(value=_clean_spaces(body), raw_value=raw, source="snapshot.text", rule_id="ted.text.section")


def parse_brl_money(value: Any) -> str:
    raw = _clean_spaces(value)
    if not raw:
        return ""
    match = re.search(r"R\$\s*(\d{1,3}(?:\.\d{3})*,\s*\d{2}|\d+,\s*\d{2}|\d+(?:\.\d{2})?)", raw, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\b(\d{1,3}(?:\.\d{3})+,\s*\d{2}|\d+,\s*\d{2})\b", raw)
    if not match:
        return ""
    number = match.group(1).replace(" ", "")
    if "," in number:
        number = number.replace(".", "").replace(",", ".")
    try:
        return f"{Decimal(number):.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _extract_money(snapshot: Dict[str, Any], text: str) -> ExtractedField:
    table_field = _section_from_tables(snapshot, ["valor", "ted"], heading_only=True)
    for preferred in (
        r"valor\s+total\s+(?:atribu[ií]do\s+a\s+este\s+TED\s+[ée]\s+de|de)\s+(R\$\s*[\d\.\s]+,\s*\d{2})",
        r"valor\s+global\s+(?:do\s+TED\s+)?(?:[ée]\s+de|de)?\s*(R\$\s*[\d\.\s]+,\s*\d{2})",
    ):
        match = re.search(preferred, text, flags=re.IGNORECASE)
        if match:
            value = parse_brl_money(match.group(1))
            if value:
                return ExtractedField(value=value, raw_value=_clean_spaces(match.group(0)), source="snapshot.text", rule_id="ted.money.total_text")
    for candidate in (table_field.raw_value, text):
        value = parse_brl_money(candidate)
        if value:
            source = table_field.source if candidate == table_field.raw_value and table_field.raw_value else "snapshot.text"
            return ExtractedField(value=value, raw_value=_clean_spaces(candidate), source=source, rule_id="ted.money.brl")
    return ExtractedField(rule_id="ted.money.brl")


def _date_from_month_year(raw: str, *, end_of_month: bool = False) -> str:
    match = re.search(r"([A-Za-zÀ-ÿ]+)\s*/\s*(20\d{2}|19\d{2})", raw or "", flags=re.IGNORECASE)
    if not match:
        return ""
    month = MONTHS.get(_norm(match.group(1)))
    if not month:
        return ""
    year = int(match.group(2))
    day = calendar.monthrange(year, month)[1] if end_of_month else 1
    return f"{year:04d}-{month:02d}-{day:02d}"


def _date_from_numeric(raw: str) -> str:
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2}|19\d{2})\b", raw or "")
    if not match:
        return ""
    day, month, year = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    if not (1 <= month <= 12 and 1 <= day <= calendar.monthrange(year, month)[1]):
        return ""
    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_vigencia(snapshot: Dict[str, Any], text: str) -> tuple[ExtractedField, ExtractedField]:
    field = _section_from_tables(snapshot, ["vigencia"], heading_only=True)
    if not field.value:
        field = _section_from_text(text, ["vigência", "vigencia"], ["valor do ted", "classificação", "classificacao", "bens remanescentes", "alterações", "alteracoes"])
    raw = field.raw_value or field.value
    start_raw = ""
    end_raw = ""
    match = re.search(r"in[ií]cio\s*:\s*(.*?)\bfim\s*:\s*([^.;\n]+)", raw, flags=re.IGNORECASE | re.DOTALL)
    if match:
        start_raw = _clean_spaces(match.group(1))
        end_raw = _clean_spaces(match.group(2))
    else:
        dates = re.findall(r"\b\d{1,2}/\d{1,2}/(?:20\d{2}|19\d{2})\b", raw)
        if len(dates) >= 2:
            start_raw, end_raw = dates[0], dates[1]
    start_value = _date_from_numeric(start_raw) or _date_from_month_year(start_raw)
    end_value = _date_from_numeric(end_raw) or _date_from_month_year(end_raw, end_of_month=True)
    source = field.source if field.source != "missing" else "missing"
    return (
        ExtractedField(value=start_value, raw_value=start_raw or raw, source=source, rule_id="ted.vigencia.inicio"),
        ExtractedField(value=end_value, raw_value=end_raw or raw, source=source, rule_id="ted.vigencia.fim"),
    )


def _extract_numero_ano(snapshot: Dict[str, Any], text: str) -> tuple[ExtractedField, ExtractedField]:
    title = _clean_spaces(snapshot.get("title", ""))
    haystack = "\n".join([title, text])
    normalized_haystack = _norm(haystack)
    patterns = [
        r"termo\s+de\s+execucao\s+descentralizada\s*(?:n\S{0,4}\s*)?(\d+)(?:\s*/\s*[a-z]+)?\s*/\s*(20\d{2}|19\d{2})",
        r"ted\s*(?:n\S{0,4}\s*)?(\d+)(?:\s*/\s*[a-z]+)?\s*/\s*(20\d{2}|19\d{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized_haystack, flags=re.IGNORECASE)
        if match:
            raw = _clean_spaces(match.group(0))
            return (
                ExtractedField(value=match.group(1), raw_value=raw, source="snapshot.text", rule_id="ted.numero"),
                ExtractedField(value=match.group(2), raw_value=raw, source="snapshot.text", rule_id="ted.ano"),
            )
    return ExtractedField(rule_id="ted.numero"), ExtractedField(rule_id="ted.ano")


def _extract_unit(snapshot: Dict[str, Any], text: str, *, decentralized: bool) -> ExtractedField:
    label = "unidade descentralizada" if decentralized else "unidade descentralizadora"
    table = _section_from_tables(snapshot, ["dados cadastrais", label])
    raw = table.raw_value or text
    if decentralized:
        pattern = r"Nome do [oó]rg[aã]o ou entidade descentralizada:\s*([^\n]+)"
    else:
        pattern = r"Nome do [oó]rg[aã]o ou entidade descentralizador\(a\):\s*([^\n]+)"
    match = re.search(pattern, raw, flags=re.IGNORECASE)
    if match:
        value = _clean_spaces(match.group(1))
        return ExtractedField(value=value, raw_value=_clean_spaces(match.group(0)), source=table.source or "snapshot.text", rule_id=f"ted.{label}")
    fallback_pattern = r"UNIDADE DESCENTRALIZAD[AO]RA? E RESPONS[ÁA]VEL\s+(.+?)(?:Nome da|Cargo:|Ato de|b\)\s*UG|2\.)"
    if decentralized:
        fallback_pattern = r"UNIDADE DESCENTRALIZADA E RESPONS[ÁA]VEL\s+(.+?)(?:Nome da|Cargo:|Ato de|b\)\s*UG|3\.)"
    match = re.search(fallback_pattern, raw, flags=re.IGNORECASE | re.DOTALL)
    if match:
        value = _clean_spaces(match.group(1))
        return ExtractedField(value=value, raw_value=_clean_spaces(match.group(0)), source=table.source or "snapshot.text", rule_id=f"ted.{label}.fallback")
    return ExtractedField(rule_id=f"ted.{label}")


def _extract_objeto(snapshot: Dict[str, Any], text: str) -> ExtractedField:
    field = _section_from_tables(snapshot, ["objeto"], ["bens remanescentes"], heading_only=True)
    if field.value:
        return field
    return _section_from_text(
        text,
        ["objeto do termo de execução descentralizada", "objeto:"],
        ["obrigações", "obrigacoes", "descrição das ações", "descricao das acoes", "justificativa", "vigência", "vigencia"],
    )


def _extract_prestacao(snapshot: Dict[str, Any], text: str) -> ExtractedField:
    field = _section_from_tables(snapshot, ["prestação de contas"], heading_only=True)
    if field.value:
        return field
    return _section_from_text(
        text,
        ["prestação de contas", "prestacao de contas", "avaliação dos resultados", "avaliacao dos resultados"],
        ["bens remanescentes", "denúncia", "denuncia", "rescisão", "rescisao", "solução de conflito", "solucao de conflito"],
    )


def _extract_table_like(snapshot: Dict[str, Any], text: str, *, kind: str) -> ExtractedField:
    if kind == "plano_aplicacao":
        table = _section_from_tables(snapshot, ["plano", "aplic"], heading_only=True)
        if not table.value:
            for index, rows in enumerate(_tables(snapshot), start=1):
                heading = _norm(" ".join(rows[0]) if rows else "")
                if "natureza da despesa" in heading and "valor previsto" in heading:
                    raw = _rows_text(rows)
                    table = ExtractedField(value=_clean_spaces(raw), raw_value=raw, source=f"snapshot.tables[{index}]", rule_id="ted.plano_aplicacao.structured_table")
                    break
        headings = ["plano de aplicação", "plano de aplicacao"]
        stops = ["aprovação", "aprovacao", "prestação de contas", "prestacao de contas", "cronograma"]
    elif kind == "cronograma_desembolso":
        table = _section_from_tables(snapshot, ["cronograma", "desembolso"], heading_only=True)
        if not table.value:
            for index, rows in enumerate(_tables(snapshot), start=1):
                heading = _norm(" ".join(rows[0]) if rows else "")
                if "data" in heading and "natureza da despesa" in heading and "valor" in heading:
                    raw = _rows_text(rows)
                    table = ExtractedField(value=_clean_spaces(raw), raw_value=raw, source=f"snapshot.tables[{index}]", rule_id="ted.cronograma_desembolso.structured_table")
                    break
        headings = ["cronograma de desembolso"]
        stops = ["plano de aplicação", "plano de aplicacao", "prestação de contas", "prestacao de contas", "aprovação", "aprovacao"]
    else:
        table = _section_from_tables(snapshot, ["meta"], heading_only=True)
        headings = ["metas", "meta 1", "descrição das ações e metas", "descricao das acoes e metas", "cronograma físico-financeiro", "cronograma fisico-financeiro"]
        stops = ["justificativa", "cronograma de desembolso", "plano de aplicação", "plano de aplicacao"]
    if table.value:
        table.rule_id = f"ted.{kind}.table"
        return table
    field = _section_from_text(text, headings, stops)
    field.rule_id = f"ted.{kind}.text"
    return field


def _payload_from_path(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _resolve_path(raw_path: Any, output_dir: Path) -> Path | None:
    cleaned = _clean_spaces(raw_path)
    if not cleaned:
        return None
    path = Path(cleaned)
    if path.exists():
        return path
    fallback = output_dir / path.name
    if fallback.exists():
        return fallback
    return None


def build_normalized_record(payload: Dict[str, Any], json_path: Path | str) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    snapshot = payload.get("snapshot", {}) if isinstance(payload, dict) else {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    text = str(snapshot.get("text", "") or "")
    analysis = payload.get("analysis", {}) if isinstance(payload.get("analysis", {}), dict) else {}
    collection = payload.get("collection", {}) if isinstance(payload.get("collection", {}), dict) else {}

    fields: Dict[str, ExtractedField] = {}
    fields["numero_ted"], fields["ano_ted"] = _extract_numero_ano(snapshot, text)
    fields["objeto"] = _extract_objeto(snapshot, text)
    fields["unidade_descentralizadora"] = _extract_unit(snapshot, text, decentralized=False)
    fields["unidade_descentralizada"] = _extract_unit(snapshot, text, decentralized=True)
    fields["valor_global"] = _extract_money(snapshot, text)
    fields["vigencia_inicio"], fields["vigencia_fim"] = _extract_vigencia(snapshot, text)
    fields["plano_aplicacao"] = _extract_table_like(snapshot, text, kind="plano_aplicacao")
    fields["cronograma_desembolso"] = _extract_table_like(snapshot, text, kind="cronograma_desembolso")
    fields["metas"] = _extract_table_like(snapshot, text, kind="metas")
    fields["prestacao_contas"] = _extract_prestacao(snapshot, text)

    required_business = [
        "numero_ted",
        "ano_ted",
        "objeto",
        "unidade_descentralizadora",
        "unidade_descentralizada",
        "valor_global",
        "vigencia_inicio",
        "vigencia_fim",
        "plano_aplicacao",
        "cronograma_desembolso",
        "metas",
        "prestacao_contas",
    ]
    missing = [name for name in required_business if not fields[name].value]
    if not missing:
        normalization_status = "completo_padronizado"
        quality_status = "high"
    elif fields["objeto"].value and fields["valor_global"].value and fields["vigencia_inicio"].value and fields["vigencia_fim"].value:
        normalization_status = "parcial_padronizado"
        quality_status = "medium"
    elif any(fields[name].value for name in required_business):
        normalization_status = "extraido_sem_padrao"
        quality_status = "low"
    else:
        normalization_status = "sem_campos_de_negocio"
        quality_status = "low"

    notes = []
    if missing:
        notes.append(f"missing={','.join(missing)}")
    if not _tables(snapshot):
        notes.append("snapshot_sem_tabelas")
    if _norm(text).startswith("1 identificacgao ata de reuniao") or "ata de reuniao" in _norm(text[:500]):
        notes.append("possivel_ata_ou_documento_relacionado")

    row: Dict[str, Any] = {
        "processo": _clean_spaces(payload.get("processo", "")),
        "documento": _clean_spaces(payload.get("documento", "")) or _clean_spaces(collection.get("chosen_documento", "")),
        "validation_status": _clean_spaces(analysis.get("validation_status", "")),
        "publication_status": _clean_spaces(analysis.get("publication_status", "")) or PUBLICATION_STATUS_GOLD,
        "normalization_status": normalization_status,
        "quality_status": quality_status,
        "quality_notes": "; ".join(notes),
        "json_path": str(json_path),
    }
    for name, field in fields.items():
        row[name] = field.value
        row[f"{name}_raw"] = field.raw_value
        row[f"{name}_source"] = field.source

    diagnostics = [
        {
            "processo": row["processo"],
            "documento": row["documento"],
            "field_name": name,
            "value": field.value,
            "raw_value": field.raw_value,
            "source": field.source,
            "status": field.status,
            "rule_id": field.rule_id,
            "json_path": str(json_path),
        }
        for name, field in fields.items()
    ]
    return row, diagnostics


def _iter_gold_records(records: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for record in records:
        if record.get("publication_status") == PUBLICATION_STATUS_GOLD and record.get("json_path"):
            yield record


def export_normalized_csv(
    output_dir: Path,
    *,
    records: Sequence[Dict[str, Any]] | None = None,
    logger: Any = None,
) -> Dict[str, Any]:
    csv_writer.ensure_output_dir(output_dir)
    rows: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, Any]] = []

    if records is None:
        json_paths = sorted(output_dir.glob("termo_execucao_descentralizada_*.json"))
        source_records = [{"json_path": str(path), "publication_status": PUBLICATION_STATUS_GOLD} for path in json_paths]
    else:
        source_records = list(_iter_gold_records(records))

    for record in source_records:
        json_path = _resolve_path(record.get("json_path", ""), output_dir)
        if json_path is None:
            continue
        payload = _payload_from_path(json_path)
        if not payload:
            continue
        row, field_diagnostics = build_normalized_record(payload, json_path)
        if not row.get("validation_status"):
            row["validation_status"] = _clean_spaces(record.get("validation_status", ""))
        if not row.get("publication_status"):
            row["publication_status"] = _clean_spaces(record.get("publication_status", ""))
        rows.append(row)
        diagnostics.extend(field_diagnostics)

    csv_path = output_dir / "ted_normalizado_latest.csv"
    diagnostics_path = output_dir / "ted_field_diagnostics_latest.csv"
    csv_writer.write_csv(rows, csv_path, columns=RICH_COLUMNS + RAW_SOURCE_COLUMNS)
    csv_writer.write_csv(diagnostics, diagnostics_path, columns=DIAGNOSTIC_COLUMNS)
    if logger is not None:
        try:
            logger.info("Relatorio TED normalizado gerado: registros=%d latest=%s diagnosticos=%s", len(rows), csv_path, diagnostics_path)
        except Exception:
            pass
    return {"records": len(rows), "latest_path": csv_path, "diagnostics_path": diagnostics_path}
