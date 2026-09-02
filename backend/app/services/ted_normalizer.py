from __future__ import annotations

import calendar
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

from app.config import get_settings
from app.output import csv_writer
from app.services.act_normalizer import PUBLICATION_STATUS_GOLD, PUBLICATION_STATUS_SILVER
from app.services.contract_adapters import (
    V2_SCHEMA_VERSION,
    adapt_legacy_record,
    v2_sidecar_path,
    write_v2_sidecar,
)
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import EvidenceLocation, FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.pipeline_states import AcquisitionState
from app.services.publication_policy import evaluate_document_gold
from app.services.semantic_states import ClassificationState, DocumentFunctionState, SemanticState
from app.services.ted_classifier import classify_ted_snapshot


RICH_COLUMNS = [
    "processo",
    "documento",
    "numero_ted",
    "ano_ted",
    "objeto",
    "unidade_descentralizadora",
    "unidade_descentralizada",
    "valor_global",
    "data_assinatura",
    "datas_assinatura",
    "vigencia_inicio",
    "vigencia_fim",
    "vigencia_prazo_quantidade",
    "vigencia_prazo_unidade",
    "vigencia_regra_inicio",
    "vigencia_regra_evidencia",
    "vigencia_inicio_origem",
    "vigencia_fim_origem",
    "vigencia_warning",
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
    "data_assinatura_raw",
    "data_assinatura_source",
    "datas_assinatura_raw",
    "datas_assinatura_source",
    "vigencia_inicio_raw",
    "vigencia_inicio_source",
    "vigencia_fim_raw",
    "vigencia_fim_source",
    "vigencia_prazo_quantidade_raw",
    "vigencia_prazo_quantidade_source",
    "vigencia_prazo_unidade_raw",
    "vigencia_prazo_unidade_source",
    "vigencia_regra_inicio_raw",
    "vigencia_regra_inicio_source",
    "vigencia_regra_evidencia_raw",
    "vigencia_regra_evidencia_source",
    "vigencia_inicio_origem_raw",
    "vigencia_inicio_origem_source",
    "vigencia_fim_origem_raw",
    "vigencia_fim_origem_source",
    "vigencia_warning_raw",
    "vigencia_warning_source",
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
    "matched_key",
    "table_index",
    "row_index",
    "column_index",
    "confidence",
    "warning",
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
    matched_key: str = ""
    table_index: int | str = ""
    row_index: int | str = ""
    column_index: int | str = ""
    confidence: str = "low"
    warning: str = ""

    @property
    def status(self) -> str:
        return "extracted" if self.value else "missing"


@dataclass(frozen=True)
class UnitAlias:
    text: str
    confidence: str
    rule: str


@dataclass
class UnitCandidate:
    field_name: str
    value: str
    raw_value: str
    matched_key: str
    table_index: int | str
    row_index: int | str
    column_index: int | str
    confidence: str
    rule_id: str
    source: str


UNIT_ALIASES = {
    "unidade_descentralizadora": (
        UnitAlias("nome do orgao ou entidade descentralizador(a)", "high", "entity_name"),
        UnitAlias("nome do orgao ou entidade descentralizadora", "high", "entity_name"),
        UnitAlias("orgao descentralizador", "high", "entity_name"),
        UnitAlias("unidade descentralizadora", "high", "block_name"),
        UnitAlias("numero e nome da unidade gestora - ug que descentralizara o credito", "medium", "ug_name"),
        UnitAlias("numero e nome da unidade gestora que descentralizara o credito", "medium", "ug_name"),
        UnitAlias("ug que descentralizara o credito", "medium", "ug_name"),
        UnitAlias("ug descentralizadora", "medium", "ug_name"),
    ),
    "unidade_descentralizada": (
        UnitAlias("nome do orgao ou entidade descentralizada", "high", "entity_name"),
        UnitAlias("orgao executor", "high", "entity_name"),
        UnitAlias("unidade descentralizada", "high", "block_name"),
        UnitAlias("numero e nome da unidade gestora - ug que recebera o credito", "medium", "ug_name"),
        UnitAlias("numero e nome da unidade gestora que recebera o credito", "medium", "ug_name"),
        UnitAlias("ug que recebera o credito", "medium", "ug_name"),
        UnitAlias("ug recebedora", "medium", "ug_name"),
    ),
}

UNIT_STOP_LABELS = (
    "nome da autoridade", "nome do responsavel", "autoridade competente", "responsavel pelo acompanhamento",
    "nome da secretaria", "nome do departamento", "unidade responsavel", "coordenacao", "coordenador",
    "cargo", "cpf", "cnpj", "ato de nomeacao", "ato de reconducao", "identificacao do ato",
    "endereco", "assinatura", "ug siafi", "ug/siafi", "numero e nome da unidade gestora",
)

UNIT_EXCLUDED_SECTIONS = (
    "obrigacoes", "competencias dos participes", "cronograma", "avaliacao dos resultados",
    "publicacao", "assinatura", "solucao de conflito", "denuncia", "rescisao",
)


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


def _extract_signature_dates(text: str) -> List[str]:
    dates: List[str] = []
    pattern = re.compile(
        r"documento\s+assinado\s+eletronicamente\s+por\b.{0,300}?\bem\s+"
        r"(\d{1,2}/\d{1,2}/(?:20\d{2}|19\d{2}))",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text or ""):
        normalized = _date_from_numeric(match.group(1))
        if normalized and normalized not in dates:
            dates.append(normalized)
    return sorted(dates)


def _add_inclusive_duration(start_iso: str, amount: int, unit: str) -> str:
    base = date.fromisoformat(start_iso)
    if unit == "anos":
        target_year = base.year + amount
        target = date(target_year, base.month, min(base.day, calendar.monthrange(target_year, base.month)[1]))
    elif unit == "meses":
        month_index = base.month - 1 + amount
        target_year = base.year + month_index // 12
        target_month = month_index % 12 + 1
        target = date(target_year, target_month, min(base.day, calendar.monthrange(target_year, target_month)[1]))
    else:
        return ""
    return (target - timedelta(days=1)).isoformat()


def _vigencia_field(value: str, raw: str, source: str, rule_id: str, warning: str = "") -> ExtractedField:
    return ExtractedField(
        value=value,
        raw_value=raw,
        source=source if value or raw else "missing",
        rule_id=rule_id,
        confidence="high" if value else "low",
        warning=warning,
    )


def _extract_vigencia(snapshot: Dict[str, Any], text: str) -> Dict[str, ExtractedField]:
    field = _section_from_tables(snapshot, ["vigencia"], heading_only=True)
    if not field.value:
        field = _section_from_text(text, ["vigência", "vigencia"], ["valor do ted", "classificação", "classificacao", "bens remanescentes", "alterações", "alteracoes"])
    raw = field.raw_value or field.value
    source = field.source if field.source != "missing" else "missing"
    evidence = _clean_spaces(raw)
    signatures = _extract_signature_dates(text)
    signature_raw = ";".join(signatures)
    result = {
        "data_assinatura": _vigencia_field(signatures[-1] if signatures else "", signature_raw, "snapshot.text" if signatures else "missing", "ted.signature.latest"),
        "datas_assinatura": _vigencia_field(signature_raw, signature_raw, "snapshot.text" if signatures else "missing", "ted.signature.all"),
        "vigencia_inicio": _vigencia_field("", raw, source, "ted.vigencia.inicio"),
        "vigencia_fim": _vigencia_field("", raw, source, "ted.vigencia.fim"),
        "vigencia_prazo_quantidade": _vigencia_field("", raw, source, "ted.vigencia.prazo.quantidade"),
        "vigencia_prazo_unidade": _vigencia_field("", raw, source, "ted.vigencia.prazo.unidade"),
        "vigencia_regra_inicio": _vigencia_field("", raw, source, "ted.vigencia.regra_inicio"),
        "vigencia_regra_evidencia": _vigencia_field(evidence, raw, source, "ted.vigencia.evidencia"),
        "vigencia_inicio_origem": _vigencia_field("", raw, source, "ted.vigencia.inicio_origem"),
        "vigencia_fim_origem": _vigencia_field("", raw, source, "ted.vigencia.fim_origem"),
        "vigencia_warning": _vigencia_field("", raw, source, "ted.vigencia.warning"),
    }
    if not raw:
        return result

    normalized = _norm(raw)
    duration_match = re.search(r"\b(\d{1,3})\s*(?:\([^)]+\))?\s*(mes(?:es)?|anos?)\b", normalized)
    amount = int(duration_match.group(1)) if duration_match else 0
    unit = "anos" if duration_match and duration_match.group(2).startswith("ano") else "meses" if duration_match else ""
    if duration_match and amount > 0:
        result["vigencia_prazo_quantidade"] = _vigencia_field(str(amount), duration_match.group(0), source, "ted.vigencia.prazo.quantidade")
        result["vigencia_prazo_unidade"] = _vigencia_field(unit, duration_match.group(0), source, "ted.vigencia.prazo.unidade")

    start_raw = ""
    end_raw = ""
    labeled = re.search(r"in[ií]cio\s*:\s*(.*?)\bfim\s*:\s*([^.;\n]+)", raw, flags=re.IGNORECASE | re.DOTALL)
    if labeled:
        start_raw = _clean_spaces(labeled.group(1))
        end_raw = _clean_spaces(labeled.group(2))
    start_value = _date_from_numeric(start_raw) or _date_from_month_year(start_raw)
    end_value = _date_from_numeric(end_raw) or _date_from_month_year(end_raw, end_of_month=True)
    numeric_dates = [_date_from_numeric(token) for token in re.findall(r"\b\d{1,2}/\d{1,2}/(?:20\d{2}|19\d{2})\b", raw)]
    numeric_dates = [value for value in numeric_dates if value]
    if not start_value and not end_value and len(numeric_dates) >= 2:
        start_value, end_value = numeric_dates[0], numeric_dates[1]

    anchor = "indeterminada"
    base_date = ""
    signature_anchor = bool("assinatura" in normalized and re.search(r"(?:inicio|a partir|a contar|contad[oa]s?)\b.{0,100}\bassinatura", normalized))
    publication_anchor = bool(re.search(r"(?:inicio|a partir|a contar|contad[oa]s?)\b.{0,100}\bpublicacao", normalized))
    ambiguous_anchor = signature_anchor and publication_anchor
    if ambiguous_anchor:
        anchor = "indeterminada"
    elif signature_anchor:
        anchor = "assinatura"
        base_date = signatures[-1] if signatures else ""
    elif publication_anchor:
        anchor = "publicacao"
        publication = re.search(
            r"publica(?:cao|do|da)\b.{0,80}?\b(?:ocorrida\s+)?em\s+(\d{1,2}/\d{1,2}/(?:20\d{2}|19\d{2}))",
            normalized,
        )
        base_date = _date_from_numeric(publication.group(1)) if publication else ""
    else:
        relative_date = re.search(
            r"(?:a partir|a contar|contad[oa]s?)\s+(?:de\s+)?(\d{1,2}/\d{1,2}/(?:20\d{2}|19\d{2}))",
            normalized,
        )
        if start_value or relative_date:
            anchor = "data_explicita"
            base_date = _date_from_numeric(relative_date.group(1)) if relative_date else start_value
    result["vigencia_regra_inicio"] = _vigencia_field(anchor, raw, source, "ted.vigencia.regra_inicio")

    calculated_end = _add_inclusive_duration(base_date, amount, unit) if base_date and amount > 0 and unit else ""
    warnings: List[str] = []
    if ambiguous_anchor:
        warnings.append("vigencia_data_base_ambigua")
    elif duration_match and amount <= 0:
        warnings.append("vigencia_prazo_invalido")
    elif anchor == "assinatura" and not base_date:
        warnings.append("vigencia_dependente_assinatura_sem_data")
    elif anchor == "publicacao" and not base_date:
        warnings.append("vigencia_dependente_publicacao_sem_data")
    elif anchor == "indeterminada" and amount > 0:
        warnings.append("vigencia_prazo_sem_data_base")
    elif not duration_match and anchor != "indeterminada" and not end_value:
        warnings.append("vigencia_regra_sem_prazo")

    if start_value:
        result["vigencia_inicio"] = _vigencia_field(start_value, start_raw or raw, source, "ted.vigencia.inicio.explicit")
        result["vigencia_inicio_origem"] = _vigencia_field("explicita", start_raw or raw, source, "ted.vigencia.inicio_origem")
        if base_date and start_value != base_date:
            warnings.append(f"vigencia_inicio_divergente:explicita={start_value},calculada={base_date}")
    elif base_date:
        result["vigencia_inicio"] = _vigencia_field(base_date, raw, source, "ted.vigencia.inicio.calculated")
        result["vigencia_inicio_origem"] = _vigencia_field("calculada", raw, source, "ted.vigencia.inicio_origem")

    if end_value:
        result["vigencia_fim"] = _vigencia_field(end_value, end_raw or raw, source, "ted.vigencia.fim.explicit")
        result["vigencia_fim_origem"] = _vigencia_field("explicita", end_raw or raw, source, "ted.vigencia.fim_origem")
        if calculated_end and end_value != calculated_end:
            warnings.append(f"vigencia_fim_divergente:explicita={end_value},calculada={calculated_end}")
    elif calculated_end:
        result["vigencia_fim"] = _vigencia_field(calculated_end, raw, source, "ted.vigencia.fim.calculated")
        result["vigencia_fim_origem"] = _vigencia_field("calculada", raw, source, "ted.vigencia.fim_origem")

    warning = ";".join(warnings)
    result["vigencia_warning"] = _vigencia_field(warning, raw, source, "ted.vigencia.warning", warning)
    return result


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


def _extract_unit_legacy(snapshot: Dict[str, Any], text: str, *, decentralized: bool) -> ExtractedField:
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


def _unit_field_from_context(value: str) -> str:
    normalized = _norm(value)
    if "descentralizadora" in normalized:
        return "unidade_descentralizadora"
    if "descentralizada" in normalized:
        return "unidade_descentralizada"
    return ""


def _alias_pattern(alias: str) -> re.Pattern[str]:
    escaped = re.escape(_norm(alias)).replace(r"\ ", r"\s+")
    escaped = escaped.replace(r"\-", r"\s*-?\s*")
    escaped = escaped.replace(r"\(a\)", r"\s*\(?a\)?")
    # Some historical snapshots already contain U+FFFD in accented labels.
    escaped = escaped.replace("orgao", r".?rg.?o")
    return re.compile(escaped, flags=re.IGNORECASE)


def _next_unit_label_start(normalized: str, start: int) -> int:
    positions = []
    labels = list(UNIT_STOP_LABELS)
    for aliases in UNIT_ALIASES.values():
        labels.extend(alias.text for alias in aliases)
    for label in labels:
        match = _alias_pattern(label).search(normalized, start)
        if match:
            positions.append(match.start())
    return min(positions) if positions else len(normalized)


def _normalize_unit_value(value: str, *, from_ug: bool) -> str:
    cleaned = _clean_spaces(value).strip(" :-–—.;|")
    cleaned = re.sub(r"^e\s+respons.vel\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^nome\s+do\s+.?rg.?o\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    if from_ug:
        match = re.match(
            r"^(?:UG\s*)?[\d.]+(?:\s*/\s*\d+)?(?:\s+Gest[aã]o\s*:?\s*\d+)?\s*[-–—]\s*(.+)$",
            cleaned,
            flags=re.IGNORECASE,
        )
        if match:
            cleaned = _clean_spaces(match.group(1)).strip(" :-–—.;|")
    return cleaned


def _candidate_from_cell(
    *,
    field_name: str,
    alias: UnitAlias,
    cell: str,
    table_index: int | str,
    row_index: int | str,
    column_index: int | str,
    source: str,
) -> UnitCandidate | None:
    normalized = _norm(cell)
    match = _alias_pattern(alias.text).search(normalized)
    if not match:
        return None
    value_start = match.end()
    while value_start < len(cell) and cell[value_start] in " :;-–—|)":
        value_start += 1
    value_end = _next_unit_label_start(normalized, value_start)
    raw_value = _clean_spaces(cell[value_start:value_end])
    value = _normalize_unit_value(raw_value, from_ug=alias.rule == "ug_name")
    if not value or _norm(value) in {"e responsavel", "responsavel"} or re.fullmatch(r"[a-z]\)?", _norm(value)):
        return None
    if any(_norm(value).startswith(label) for label in UNIT_STOP_LABELS):
        return None
    return UnitCandidate(
        field_name=field_name,
        value=value,
        raw_value=raw_value,
        matched_key=_clean_spaces(cell[match.start():match.end()]),
        table_index=table_index,
        row_index=row_index,
        column_index=column_index,
        confidence=alias.confidence,
        rule_id=f"ted.unit.{alias.rule}.inline",
        source=source,
    )


def _collect_unit_candidates(snapshot: Dict[str, Any], text: str) -> Dict[str, List[UnitCandidate]]:
    candidates: Dict[str, List[UnitCandidate]] = {name: [] for name in UNIT_ALIASES}
    for table_index, rows in enumerate(_tables(snapshot), start=1):
        table_text = _norm(_rows_text(rows))
        known_specific_alias = any(
            _norm(alias.text) in table_text
            for aliases in UNIT_ALIASES.values()
            for alias in aliases
            if alias.rule in {"entity_name", "ug_name"}
        )
        identification_table = "dados cadastrais" in table_text or known_specific_alias
        heading = _norm(" ".join(rows[0]) if rows else "")
        if not identification_table or any(section in heading for section in UNIT_EXCLUDED_SECTIONS):
            continue
        context = ""
        pending: Dict[str, tuple[UnitAlias, str, int, int]] = {}
        for row_index, row in enumerate(rows, start=1):
            joined_row = " ".join(row)
            row_context = _unit_field_from_context(joined_row)
            if "dados cadastrais" in _norm(joined_row) and row_context:
                context = row_context
            for column_index, cell in enumerate(row, start=1):
                if not cell:
                    continue
                cell_norm = _norm(cell)
                contains_known_label = any(
                    _alias_pattern(alias.text).search(cell_norm)
                    for aliases in UNIT_ALIASES.values()
                    for alias in aliases
                ) or any(_alias_pattern(label).search(cell_norm) for label in UNIT_STOP_LABELS)
                for pending_field, (pending_alias, pending_key, _, _) in list(pending.items()):
                    if (not context or pending_field == context) and not contains_known_label:
                        value = _normalize_unit_value(cell, from_ug=pending_alias.rule == "ug_name")
                        if value:
                            candidates[pending_field].append(UnitCandidate(
                                field_name=pending_field,
                                value=value,
                                raw_value=_clean_spaces(cell),
                                matched_key=pending_key,
                                table_index=table_index,
                                row_index=row_index,
                                column_index=column_index,
                                confidence=pending_alias.confidence,
                                rule_id=f"ted.unit.{pending_alias.rule}.vertical",
                                source=f"snapshot.tables[{table_index}]",
                            ))
                            del pending[pending_field]
                direct_context = _unit_field_from_context(cell)
                if direct_context and (
                    "dados cadastrais" in cell_norm
                    or cell_norm.startswith(("a) unidade", "a - unidade", "unidade"))
                ):
                    context = direct_context
                for field_name, aliases in UNIT_ALIASES.items():
                    if context and field_name != context:
                        continue
                    for alias in aliases:
                        candidate = _candidate_from_cell(
                            field_name=field_name,
                            alias=alias,
                            cell=cell,
                            table_index=table_index,
                            row_index=row_index,
                            column_index=column_index,
                            source=f"snapshot.tables[{table_index}]",
                        )
                        if candidate:
                            candidates[field_name].append(candidate)
                    key_norm = cell_norm.strip(" :")
                    for alias in aliases:
                        if _alias_pattern(alias.text).fullmatch(key_norm):
                            pending[field_name] = (alias, cell, row_index, column_index)
                    if column_index < len(row):
                        for alias in aliases:
                            if _alias_pattern(alias.text).fullmatch(key_norm):
                                raw_value = _clean_spaces(row[column_index])
                                value = _normalize_unit_value(raw_value, from_ug=alias.rule == "ug_name")
                                if value:
                                    candidates[field_name].append(UnitCandidate(
                                        field_name=field_name,
                                        value=value,
                                        raw_value=raw_value,
                                        matched_key=cell,
                                        table_index=table_index,
                                        row_index=row_index,
                                        column_index=column_index + 1,
                                        confidence=alias.confidence,
                                        rule_id=f"ted.unit.{alias.rule}.horizontal",
                                        source=f"snapshot.tables[{table_index}]",
                                    ))

    # Only the current TED snapshot text is eligible for fallback; ACT/PT payload data is ignored.
    for field_name in UNIT_ALIASES:
        if candidates[field_name]:
            continue
        cleaned_text = _clean_spaces(text)
        normalized_text = _norm(cleaned_text)
        block_label = "dados cadastrais da " + field_name.replace("_", " ")
        block_start = normalized_text.find(block_label)
        if block_start < 0:
            continue
        opposite = "unidade descentralizada" if field_name == "unidade_descentralizadora" else "unidade descentralizadora"
        block_end = normalized_text.find("dados cadastrais da " + opposite, block_start + len(block_label))
        block = cleaned_text[block_start:block_end if block_end >= 0 else len(cleaned_text)]
        for alias in UNIT_ALIASES[field_name]:
            candidate = _candidate_from_cell(
                field_name=field_name,
                alias=alias,
                cell=block,
                table_index="",
                row_index="",
                column_index="",
                source="snapshot.text",
            )
            if candidate:
                candidate.rule_id = f"ted.unit.{alias.rule}.text_fallback"
                candidates[field_name].append(candidate)
    return candidates


def _select_unit_candidate(field_name: str, candidates: Sequence[UnitCandidate]) -> ExtractedField:
    if not candidates:
        return ExtractedField(rule_id=f"ted.{field_name}", warning="missing")
    rank = {"high": 2, "medium": 1, "low": 0}
    best_rank = max(rank.get(candidate.confidence, 0) for candidate in candidates)
    best = [candidate for candidate in candidates if rank.get(candidate.confidence, 0) == best_rank]
    values = {_norm(candidate.value) for candidate in best}
    if len(values) > 1:
        return ExtractedField(
            rule_id=f"ted.{field_name}.ambiguous",
            confidence=best[0].confidence,
            warning="ambiguous_unit_candidates",
        )
    chosen = best[0]
    return ExtractedField(
        value=chosen.value,
        raw_value=chosen.raw_value,
        source=chosen.source,
        rule_id=chosen.rule_id,
        matched_key=chosen.matched_key,
        table_index=chosen.table_index,
        row_index=chosen.row_index,
        column_index=chosen.column_index,
        confidence=chosen.confidence,
    )


def _preserve_legacy_unit(structured: ExtractedField, legacy: ExtractedField) -> ExtractedField:
    """Keep the previous extractor only when structured extraction found no safe candidate."""
    if structured.value or structured.warning != "missing" or not legacy.value:
        return structured
    legacy.confidence = "low"
    legacy.warning = "preserved_legacy_value"
    return legacy


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
    unit_candidates = _collect_unit_candidates(snapshot, text)
    structured_decentralizer = _select_unit_candidate(
        "unidade_descentralizadora", unit_candidates["unidade_descentralizadora"]
    )
    structured_decentralized = _select_unit_candidate(
        "unidade_descentralizada", unit_candidates["unidade_descentralizada"]
    )
    fields["unidade_descentralizadora"] = _preserve_legacy_unit(
        structured_decentralizer, _extract_unit_legacy(snapshot, text, decentralized=False)
    )
    fields["unidade_descentralizada"] = _preserve_legacy_unit(
        structured_decentralized, _extract_unit_legacy(snapshot, text, decentralized=True)
    )
    fields["valor_global"] = _extract_money(snapshot, text)
    fields.update(_extract_vigencia(snapshot, text))
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
    informative_business = required_business + ["vigencia_prazo_quantidade", "vigencia_prazo_unidade"]
    missing = [name for name in required_business if not fields[name].value]
    if not missing:
        normalization_status = "completo_padronizado"
        quality_status = "high"
    elif fields["objeto"].value and fields["valor_global"].value and fields["vigencia_inicio"].value and fields["vigencia_fim"].value:
        normalization_status = "parcial_padronizado"
        quality_status = "medium"
    elif any(fields[name].value for name in informative_business):
        normalization_status = "extraido_sem_padrao"
        quality_status = "low"
    else:
        normalization_status = "sem_campos_de_negocio"
        quality_status = "low"

    notes = []
    if missing:
        notes.append(f"missing={','.join(missing)}")
    if fields["vigencia_warning"].value:
        notes.append(fields["vigencia_warning"].value)
    if not _tables(snapshot):
        notes.append("snapshot_sem_tabelas")
    if _norm(text).startswith("1 identificacgao ata de reuniao") or "ata de reuniao" in _norm(text[:500]):
        notes.append("possivel_ata_ou_documento_relacionado")
    for name in ("unidade_descentralizadora", "unidade_descentralizada"):
        if fields[name].warning and fields[name].warning != "missing":
            notes.append(f"{name}:{fields[name].warning}")

    row: Dict[str, Any] = {
        "processo": _clean_spaces(payload.get("processo", "")),
        "documento": _clean_spaces(payload.get("documento", "")) or _clean_spaces(collection.get("chosen_documento", "")),
        "validation_status": _clean_spaces(analysis.get("validation_status", "")),
        "publication_status": _clean_spaces(analysis.get("publication_status", "")) or PUBLICATION_STATUS_SILVER,
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
            "matched_key": field.matched_key,
            "table_index": field.table_index,
            "row_index": field.row_index,
            "column_index": field.column_index,
            "confidence": field.confidence,
            "warning": field.warning,
            "json_path": str(json_path),
        }
        for name, field in fields.items()
    ]
    return row, diagnostics


_TED_DERIVED_FIELDS = {
    "vigencia_regra_inicio",
    "vigencia_inicio_origem",
    "vigencia_fim_origem",
    "vigencia_warning",
}


def _ted_document_identity(record: Dict[str, Any], payload: Dict[str, Any]) -> DocumentIdentity:
    collection = payload.get("collection", {}) if isinstance(payload.get("collection", {}), dict) else {}
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot", {}), dict) else {}
    source_url = _clean_spaces(collection.get("source_url") or snapshot.get("url") or "") or None
    query = parse_qs(urlparse(source_url).query) if source_url else {}
    document_values = query.get("id_documento", ())
    candidate_values = query.get("id_anexo", ())
    document_id = _clean_spaces(collection.get("document_id", "")) or None
    candidate_id = _clean_spaces(collection.get("candidate_id", "")) or None
    if document_id is None and len(document_values) == 1 and document_values[0].strip().isdigit():
        document_id = document_values[0].strip()
    if candidate_id is None and len(candidate_values) == 1 and candidate_values[0].strip().isdigit():
        candidate_id = candidate_values[0].strip()
    if document_id == _clean_spaces(record.get("processo", "")):
        document_id = None
    return DocumentIdentity(
        process_id=record.get("processo", ""),
        document_id=document_id,
        candidate_id=candidate_id,
        source_url=source_url,
    )


def _diagnostic_index(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _ted_source_kind(field_name: str, diagnostic: Dict[str, Any]) -> SourceKind | None:
    if diagnostic.get("source") == "missing":
        return None
    rule_id = _clean_spaces(diagnostic.get("rule_id", ""))
    if field_name in _TED_DERIVED_FIELDS or rule_id.endswith(".calculated"):
        return SourceKind.DERIVED
    source = _clean_spaces(diagnostic.get("source", ""))
    if source == "snapshot.text" or source.startswith("snapshot.tables["):
        return SourceKind.DOCUMENT
    return None


def _ted_field_state(diagnostic: Dict[str, Any]) -> FieldState:
    if diagnostic.get("value") is not None and _clean_spaces(diagnostic.get("value", "")):
        return FieldState.PRESENT
    if _clean_spaces(diagnostic.get("warning", "")) == "ambiguous_unit_candidates":
        return FieldState.CONFLICT
    return FieldState.NOT_EVALUATED


def build_ted_v2_record(
    record: Dict[str, Any],
    payload: Dict[str, Any],
    diagnostics: Sequence[Dict[str, Any]],
    *,
    source_path: str | Path | None = None,
) -> Dict[str, Any]:
    """Adapt existing TED field diagnostics without changing legacy resolution."""

    identity = _ted_document_identity(record, payload)
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot", {}), dict) else {}
    collection = payload.get("collection", {}) if isinstance(payload.get("collection", {}), dict) else {}
    text = str(snapshot.get("text", "") or "")
    field_results: List[FieldResult] = []
    for diagnostic in diagnostics:
        field_name = _clean_spaces(diagnostic.get("field_name", ""))
        if not field_name:
            continue
        state = _ted_field_state(diagnostic)
        evidences: Tuple[FieldEvidence, ...] = ()
        if state is FieldState.PRESENT:
            source_kind = _ted_source_kind(field_name, diagnostic)
            if source_kind is not None:
                table_index = _diagnostic_index(diagnostic.get("table_index"))
                row_index = _diagnostic_index(diagnostic.get("row_index"))
                column_index = _diagnostic_index(diagnostic.get("column_index"))
                has_structural_location = any(value is not None for value in (table_index, row_index, column_index))
                location = (
                    EvidenceLocation(
                        source_path=str(source_path) if source_path else None,
                        table_index=table_index,
                        row_index=row_index,
                        column_index=column_index,
                    )
                    if has_structural_location
                    else None
                )
                evidences = (
                    FieldEvidence(
                        field_name=field_name,
                        source_kind=source_kind,
                        source_document=identity if source_kind is SourceKind.DOCUMENT else None,
                        rule_id=_clean_spaces(diagnostic.get("rule_id", "")) or None,
                        location=location,
                        raw_evidence=_clean_spaces(diagnostic.get("raw_value", "")) or None,
                    ),
                )
        field_results.append(
            FieldResult(
                field_name=field_name,
                state=state,
                value=diagnostic.get("value") if state is FieldState.PRESENT else None,
                evidences=evidences,
            )
        )

    adapted = adapt_legacy_record(
        {
            **record,
            "process_id": identity.process_id,
            "document_id": identity.document_id,
            "candidate_id": identity.candidate_id,
            "source_url": identity.source_url,
            "found": collection.get("found"),
            "acquisition_state": collection.get("acquisition_state") or payload.get("acquisition_state"),
        }
    )
    adapted["fields"] = [field.to_dict() for field in field_results]
    adapted["ted_field_diagnostics"] = [dict(item) for item in diagnostics]
    adapted["legacy_publication_status"] = record.get("publication_status")
    ted_classification = classify_ted_snapshot(snapshot)
    previous_semantic = SemanticState.from_dict(adapted["semantic_state"])
    adapted["semantic_state"] = SemanticState(
        classification=ClassificationState(ted_classification.classification),
        function=DocumentFunctionState(ted_classification.function),
        affinity=previous_semantic.affinity,
        canonical=previous_semantic.canonical,
        publication=previous_semantic.publication,
        resolved_class=ted_classification.resolved_class,
        resolved_function=ted_classification.resolved_function,
    ).to_dict()
    adapted["ted_classification"] = {
        "reason": ted_classification.reason,
        "evidence_source": ted_classification.evidence_source,
    }
    acquisition = AcquisitionState.from_dict(adapted["acquisition_state"])
    decision = evaluate_document_gold(
        identity=identity,
        acquisition=acquisition,
        semantic=SemanticState.from_dict(adapted["semantic_state"]),
        has_verifiable_content=bool(text.strip()) or bool(snapshot.get("tables", []) or []),
    )
    adapted["document_gold_decision"] = decision.to_dict()
    return adapted


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
    v2_inputs: List[tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Path]] = []

    if records is None:
        source_records = []
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
        v2_inputs.append((row, payload, field_diagnostics, json_path))

    csv_path = output_dir / "ted_normalizado_latest.csv"
    diagnostics_path = output_dir / "ted_field_diagnostics_latest.csv"
    csv_writer.write_csv(rows, csv_path, columns=RICH_COLUMNS + RAW_SOURCE_COLUMNS)
    csv_writer.write_csv(diagnostics, diagnostics_path, columns=DIAGNOSTIC_COLUMNS)
    v2_path = None
    if get_settings().v2_dual_write:
        envelope = {
            "schema_version": V2_SCHEMA_VERSION,
            "legacy_artifact": csv_path.name,
            "records": [
                build_ted_v2_record(row, payload, field_diagnostics, source_path=json_path.name)
                for row, payload, field_diagnostics, json_path in v2_inputs
            ],
        }
        v2_path = write_v2_sidecar(v2_sidecar_path(csv_path), envelope, family="TED")
    if logger is not None:
        try:
            logger.info("Relatorio TED normalizado gerado: registros=%d latest=%s diagnosticos=%s", len(rows), csv_path, diagnostics_path)
        except Exception:
            pass
    return {"records": len(rows), "latest_path": csv_path, "diagnostics_path": diagnostics_path, "v2_path": v2_path}
