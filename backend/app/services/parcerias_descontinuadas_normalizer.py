from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo

import pandas as pd

from app.config import get_settings
from app.output import csv_writer
from app.services.contract_adapters import V2_SCHEMA_VERSION, adapt_legacy_record, v2_sidecar_path, write_v2_sidecar
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import EvidenceLocation, FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity

SOURCE_FILENAME = "parcerias_descontinuadas_latest.csv"
OUTPUT_FILENAME = "parcerias_descontinuadas_normalizado_latest.csv"

NORMALIZED_COLUMNS = [
    "processo",
    "tipo",
    "numero_act",
    "numero_termo_encerramento",
    "parceiro",
    "vigencia",
    "objeto",
    "gestor_titular",
    "gestor_substituto",
    "portaria_designacao",
    "data_assinatura",
    "data_vencimento",
    "termo_encerramento_raw",
    "status_raw",
    "status_normalizado",
    "status_calculado",
    "status_categoria",
    "status_evidencia",
    "status_data_referencia",
    "normalization_status",
    "missing_fields",
    "raw_anotacoes",
]

_LABEL_TO_FIELD = {
    "TIPO": "tipo",
    "NUMERO ACT": "numero_act",
    "N ACT": "numero_act",
    "NO ACT": "numero_act",
    "NRO ACT": "numero_act",
    "NUM ACT": "numero_act",
    "NUMERO TERMO ENCERRAMENTO": "numero_termo_encerramento",
    "N TERMO ENCERRAMENTO": "numero_termo_encerramento",
    "NO TERMO ENCERRAMENTO": "numero_termo_encerramento",
    "PARCEIRO": "parceiro",
    "PARCEIROS": "parceiro",
    "VIGENCIA": "vigencia",
    "OBJETO": "objeto",
    "GESTOR TITULAR": "gestor_titular",
    "GESTOR SUBSTITUTO": "gestor_substituto",
    "PORTARIA DE DESIGNACAO": "portaria_designacao",
    "PORTARIA": "portaria_designacao",
    "DATA DE ASSINATURA": "data_assinatura",
    "DATA DE VENCIMENTO": "data_vencimento",
    "VENCIMENTO": "data_vencimento",
    "STATUS": "status_raw",
    "TERMO DE ENCERRAMENTO": "termo_encerramento_raw",
}

_LABEL_PREFIXES = sorted(_LABEL_TO_FIELD.keys(), key=len, reverse=True)
_MULTILINE_FIELDS = {"parceiro", "objeto", "termo_encerramento_raw"}
_REQUIRED_STRUCTURED_FIELDS = ("tipo", "parceiro", "objeto", "status_raw")


def _clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _clean_multiline(value: Any) -> str:
    lines: List[str] = []
    for raw_line in str(value or "").replace("\r", "\n").splitlines():
        cleaned = _clean_spaces(raw_line)
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _normalize_label(value: Any) -> str:
    text = str(value or "").replace("º", "O").replace("°", "O")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9]+", " ", text).strip().upper()
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_status_key(value: Any) -> str:
    normalized = _normalize_label(value)
    return normalized.strip(" .;:")


def _split_labeled_line(line: str) -> tuple[str, str] | None:
    cleaned = _clean_spaces(line)
    if not cleaned:
        return None

    normalized = _normalize_label(cleaned)
    for prefix in _LABEL_PREFIXES:
        if normalized == prefix:
            return (_LABEL_TO_FIELD[prefix], "")
        if not normalized.startswith(prefix + " "):
            continue

        raw_match = re.match(
            rf"^\s*(?:{_label_regex(prefix)})(?:\s*[:\-]\s*|\s+)(.*)$",
            cleaned,
            flags=re.IGNORECASE,
        )
        if raw_match:
            return (_LABEL_TO_FIELD[prefix], _clean_spaces(raw_match.group(1)))

        remainder = normalized[len(prefix) :].strip(" :-")
        return (_LABEL_TO_FIELD[prefix], _clean_spaces(remainder))
    return None


def _label_regex(normalized_label: str) -> str:
    if normalized_label in {"N ACT", "NO ACT"}:
        return r"(?:N[º°oO]?|NO)\s*ACT"
    if normalized_label == "NUMERO ACT":
        return r"N[úu]mero\s+ACT|Numero\s+ACT"
    if normalized_label in {"N TERMO ENCERRAMENTO", "NO TERMO ENCERRAMENTO"}:
        return r"(?:N[º°oO]?|NO)\s+TERMO\s+ENCERRAMENTO"
    if normalized_label == "NUMERO TERMO ENCERRAMENTO":
        return r"N[úu]mero\s+TERMO\s+ENCERRAMENTO|Numero\s+TERMO\s+ENCERRAMENTO"

    parts = normalized_label.split()
    aliases = []
    for part in parts:
        if part == "VIGENCIA":
            aliases.append(r"Vig[êe]ncia")
        elif part == "DESIGNACAO":
            aliases.append(r"Designa[çc][ãa]o")
        else:
            aliases.append(re.escape(part))
    return r"\s+".join(aliases)


def _append_field(record: Dict[str, str], field: str, value: str) -> None:
    cleaned = _clean_spaces(value)
    if not cleaned:
        return
    if field in _MULTILINE_FIELDS and record.get(field):
        record[field] = _clean_spaces(f"{record[field]} {cleaned}")
        return
    if not record.get(field):
        record[field] = cleaned


def _extract_fields_from_anotacoes(anotacoes: str) -> Dict[str, str]:
    record = {column: "" for column in NORMALIZED_COLUMNS}
    current_multiline_field = ""

    for raw_line in str(anotacoes or "").replace("\r", "\n").splitlines():
        line = _clean_spaces(raw_line)
        if not line:
            continue

        labeled = _split_labeled_line(line)
        if labeled:
            field, value = labeled
            current_multiline_field = field if field in _MULTILINE_FIELDS else ""
            _append_field(record, field, value)
            continue

        if current_multiline_field:
            _append_field(record, current_multiline_field, line)

    return record


def _annotation_evidence(anotacoes: str) -> Dict[str, Dict[str, Any]]:
    """Capture the label and zero-based line position without changing parsing."""
    evidence: Dict[str, Dict[str, Any]] = {}
    current_multiline_field = ""
    for position, raw_line in enumerate(str(anotacoes or "").replace("\r", "\n").splitlines()):
        line = _clean_spaces(raw_line)
        if not line:
            continue
        labeled = _split_labeled_line(line)
        if labeled:
            field, value = labeled
            current_multiline_field = field if field in _MULTILINE_FIELDS else ""
            label = line[: max(0, len(line) - len(value))].strip(" :-") if value else line.strip(" :-")
            if value and field not in evidence:
                evidence[field] = {"label": label, "position": position, "raw": value}
            continue
        if current_multiline_field and current_multiline_field in evidence:
            previous = evidence[current_multiline_field]["raw"]
            evidence[current_multiline_field]["raw"] = _clean_spaces(f"{previous} {line}")
    return evidence


def _extract_termo_number(termo_raw: str) -> str:
    text = _clean_spaces(termo_raw)
    match = re.search(r"(?:N[º°oO]?\s*)?([0-9]+(?:/[0-9]{2,4})?)", text, flags=re.IGNORECASE)
    return _clean_spaces(match.group(1)) if match else ""


def _normalize_reference_date(value: date | datetime | pd.Timestamp | str | None) -> date:
    if value is None:
        return datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(ZoneInfo("America/Sao_Paulo"))
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        raise ValueError(f"Data de referencia invalida: {value!r}")
    return parsed.date()


def _has_identified_termination_term(record: Dict[str, str]) -> bool:
    term = _clean_spaces(record.get("termo_encerramento_raw"))
    if not term:
        return False
    return bool(
        re.search(r"\b\d{5,}\b", term)
        or re.search(r"\b\d+\s*/\s*\d{2,4}\b", term)
        or re.search(r"\bpublicad[oa]\b", _normalize_status_key(term), flags=re.IGNORECASE)
    )


def _is_not_realized_status(key: str) -> bool:
    return any(
        marker in key
        for marker in (
            "NAO REALIZADO",
            "NAO FORMALIZADO",
            "NAO ASSINADO",
            "NAO AUTORIZADO",
            "NAO HOUVE CONTINUIDADE",
        )
    )


def _normalized_raw_status(raw: str) -> str:
    key = _normalize_status_key(raw)
    if not key:
        return ""
    if key in {"ENCERRADO", "ENCERRADA"} or key.startswith("ENCERRADO PELA"):
        return "Encerrado"
    if key.startswith("VIGENTE"):
        return "Vigente"
    if _is_not_realized_status(key):
        return "Nao Realizado"
    return raw


def _status_from_record(
    record: Dict[str, str], reference_date: date
) -> tuple[str, str, str, str, str]:
    raw = _clean_spaces(record.get("status_raw"))
    key = _normalize_status_key(raw)
    normalized = _normalized_raw_status(raw)
    evidence: List[str] = []

    has_term = _has_identified_termination_term(record)
    if has_term:
        evidence.append("termo_encerramento_identificado")

    parsed_end = pd.to_datetime(_clean_spaces(record.get("data_vencimento")), errors="coerce", dayfirst=True)
    if not pd.isna(parsed_end):
        if parsed_end.date() < reference_date:
            evidence.append("data_final_anterior_referencia")
        else:
            evidence.append("data_final_igual_ou_posterior_referencia")

    if key:
        if key.startswith("VIGENTE"):
            evidence.append("status_raw_vigente")
        elif _is_not_realized_status(key):
            evidence.append("status_raw_nao_realizado")
        elif key in {"ENCERRADO", "ENCERRADA"} or key.startswith("ENCERRADO PELA"):
            evidence.append("status_raw_encerrado")
        else:
            evidence.append("status_raw_nao_mapeado")

    if has_term:
        calculated, category = "Encerrado", "encerrado"
    elif _is_not_realized_status(key):
        calculated, category = "Nao Realizado", "nao_realizado"
    elif not pd.isna(parsed_end) and parsed_end.date() < reference_date:
        calculated, category = "Vencido", "vencido"
    elif not pd.isna(parsed_end):
        calculated, category = "Vigente", "vigente"
    else:
        calculated, category = "Indeterminado", "indeterminado"

    return raw, normalized, calculated, category, ";".join(evidence)


def build_normalized_record(
    row: Dict[str, Any], *, reference_date: date | datetime | pd.Timestamp | str | None = None
) -> Dict[str, str]:
    resolved_reference = _normalize_reference_date(reference_date)
    raw_anotacoes = _clean_multiline(row.get("anotacoes", ""))
    record = _extract_fields_from_anotacoes(raw_anotacoes)
    record["processo"] = _clean_spaces(row.get("processo", ""))
    record["raw_anotacoes"] = raw_anotacoes

    if not record.get("numero_termo_encerramento") and record.get("termo_encerramento_raw"):
        record["numero_termo_encerramento"] = _extract_termo_number(record["termo_encerramento_raw"])

    status_raw, status_normalizado, status_calculado, status_categoria, status_evidencia = _status_from_record(
        record, resolved_reference
    )
    record["status_raw"] = status_raw
    record["status_normalizado"] = status_normalizado
    record["status_calculado"] = status_calculado
    record["status_categoria"] = status_categoria
    record["status_evidencia"] = status_evidencia
    record["status_data_referencia"] = resolved_reference.isoformat()

    missing = [field for field in _REQUIRED_STRUCTURED_FIELDS if not _clean_spaces(record.get(field))]
    if len(missing) == 0:
        normalization_status = "completo"
    elif len(missing) >= len(_REQUIRED_STRUCTURED_FIELDS):
        normalization_status = "sem_campos_estruturados"
    else:
        normalization_status = "parcial"

    record["normalization_status"] = normalization_status
    record["missing_fields"] = ",".join(missing)
    return {column: record.get(column, "") for column in NORMALIZED_COLUMNS}


def build_descontinuada_v2_record(row: Dict[str, Any], record: Dict[str, str]) -> Dict[str, Any]:
    annotations = _clean_multiline(row.get("anotacoes", ""))
    annotation_fields = _annotation_evidence(annotations)
    identity = DocumentIdentity(process_id=_clean_spaces(record.get("processo")))
    fields: List[FieldResult] = []
    derived_rules = {
        "numero_termo_encerramento": "descontinuadas.termo.number",
        "status_normalizado": "descontinuadas.status.normalize_legacy",
        "status_calculado": "descontinuadas.status.calculate_legacy",
        "status_categoria": "descontinuadas.status.category_legacy",
        "status_evidencia": "descontinuadas.status.evidence_codes",
        "status_data_referencia": "descontinuadas.status.reference_date",
        "normalization_status": "descontinuadas.completeness.legacy",
        "missing_fields": "descontinuadas.missing_fields.legacy",
    }
    status_inputs = " | ".join(
        annotation_fields[name]["raw"]
        for name in ("status_raw", "data_vencimento", "termo_encerramento_raw")
        if name in annotation_fields
    )
    for field_name in NORMALIZED_COLUMNS:
        value = record.get(field_name)
        present = value is not None and (not isinstance(value, str) or bool(value.strip()))
        evidences = ()
        if present:
            annotation = annotation_fields.get(field_name)
            derived = field_name in derived_rules
            if field_name == "numero_termo_encerramento" and "termo_encerramento_raw" in annotation_fields:
                raw = annotation_fields["termo_encerramento_raw"]["raw"]
            elif field_name in {"status_normalizado", "status_calculado", "status_categoria", "status_evidencia"}:
                raw = status_inputs
            else:
                raw = annotation["raw"] if annotation else _clean_spaces(value)
            evidences = (FieldEvidence(
                field_name=field_name,
                source_kind=SourceKind.DERIVED if derived else SourceKind.PREVIEW,
                source_document=None,
                rule_id=derived_rules.get(field_name) or "descontinuadas.annotation.parse",
                location=(EvidenceLocation(section=annotation["label"], position=annotation["position"])
                          if annotation and not derived else None),
                raw_evidence=raw or None,
            ),)
        fields.append(FieldResult(
            field_name=field_name,
            state=FieldState.PRESENT if present else FieldState.NOT_EVALUATED,
            value=value if present else None,
            evidences=evidences,
        ))
    adapted = adapt_legacy_record({**record, "process_id": identity.process_id})
    adapted["fields"] = [field.to_dict() for field in fields]
    return adapted


def normalize_rows(
    rows: Iterable[Dict[str, Any]], *, reference_date: date | datetime | pd.Timestamp | str | None = None
) -> List[Dict[str, str]]:
    resolved_reference = _normalize_reference_date(reference_date)
    return [build_normalized_record(row, reference_date=resolved_reference) for row in rows]


def export_normalized_csv(
    output_dir: Path | str,
    records: List[Dict[str, Any]] | None = None,
    logger: Any = None,
    *,
    reference_date: date | datetime | pd.Timestamp | str | None = None,
) -> Dict[str, Any]:
    output_path = csv_writer.ensure_output_dir(output_dir)
    if records is None:
        source_path = output_path / SOURCE_FILENAME
        if not source_path.exists():
            rows: List[Dict[str, Any]] = []
        else:
            df = pd.read_csv(source_path, dtype=str).fillna("")
            rows = [{key: value for key, value in row.items()} for row in df.to_dict(orient="records")]
    else:
        rows = records

    normalized_rows = normalize_rows(rows, reference_date=reference_date)
    csv_path = output_path / OUTPUT_FILENAME
    csv_writer.write_csv(normalized_rows, csv_path, columns=NORMALIZED_COLUMNS)
    v2_path = None
    if get_settings().v2_dual_write:
        envelope = {
            "schema_version": V2_SCHEMA_VERSION,
            "legacy_artifact": csv_path.name,
            "records": [build_descontinuada_v2_record(row, record) for row, record in zip(rows, normalized_rows)],
        }
        v2_path = write_v2_sidecar(
            v2_sidecar_path(csv_path), envelope, family="Descontinuadas"
        )

    if logger is not None:
        logger.info(
            "Relatorio PARCERIAS DESCONTINUADAS normalizado gerado: registros=%d latest=%s",
            len(normalized_rows),
            csv_path,
        )
    return {"records": len(normalized_rows), "latest_path": csv_path, "v2_path": v2_path}
