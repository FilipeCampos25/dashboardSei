from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from app.output import csv_writer

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
    "status_categoria",
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
    "VIGENCIA": "vigencia",
    "OBJETO": "objeto",
    "GESTOR TITULAR": "gestor_titular",
    "GESTOR SUBSTITUTO": "gestor_substituto",
    "PORTARIA DE DESIGNACAO": "portaria_designacao",
    "PORTARIA": "portaria_designacao",
    "DATA DE ASSINATURA": "data_assinatura",
    "DATA DE VENCIMENTO": "data_vencimento",
    "STATUS": "status_raw",
    "TERMO DE ENCERRAMENTO": "termo_encerramento_raw",
}

_LABEL_PREFIXES = sorted(_LABEL_TO_FIELD.keys(), key=len, reverse=True)
_MULTILINE_FIELDS = {"objeto", "termo_encerramento_raw"}
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


def _extract_termo_number(termo_raw: str) -> str:
    text = _clean_spaces(termo_raw)
    match = re.search(r"(?:N[º°oO]?\s*)?([0-9]+(?:/[0-9]{2,4})?)", text, flags=re.IGNORECASE)
    return _clean_spaces(match.group(1)) if match else ""


def _status_from_record(record: Dict[str, str]) -> tuple[str, str, str]:
    raw = _clean_spaces(record.get("status_raw"))
    termo_raw = _clean_spaces(record.get("termo_encerramento_raw"))
    if not raw and termo_raw:
        raw = "Encerrado"

    key = _normalize_status_key(raw)
    if not key:
        return ("", "", "sem_status")
    if key in {"ENCERRADO", "ENCERRADA"} or key.startswith("ENCERRADO PELA"):
        return (raw, "Encerrado", "encerrado")
    if key.startswith("VIGENTE"):
        return (raw, "Vigente", "vigente_em_descontinuadas")
    if (
        "NAO REALIZADO" in key
        or "NAO FORMALIZADO" in key
        or "NAO ASSINADO" in key
        or "NAO AUTORIZADO" in key
        or "NAO HOUVE CONTINUIDADE" in key
    ):
        return (raw, "Nao Realizado", "nao_realizado")
    return (raw, raw, "nao_realizado")


def build_normalized_record(row: Dict[str, Any]) -> Dict[str, str]:
    raw_anotacoes = _clean_multiline(row.get("anotacoes", ""))
    record = _extract_fields_from_anotacoes(raw_anotacoes)
    record["processo"] = _clean_spaces(row.get("processo", ""))
    record["raw_anotacoes"] = raw_anotacoes

    if not record.get("numero_termo_encerramento") and record.get("termo_encerramento_raw"):
        record["numero_termo_encerramento"] = _extract_termo_number(record["termo_encerramento_raw"])

    status_raw, status_normalizado, status_categoria = _status_from_record(record)
    record["status_raw"] = status_raw
    record["status_normalizado"] = status_normalizado
    record["status_categoria"] = status_categoria

    missing = [field for field in _REQUIRED_STRUCTURED_FIELDS if not _clean_spaces(record.get(field))]
    if status_categoria == "sem_status" and "status_raw" not in missing:
        missing.append("status_raw")
    if len(missing) == 0:
        normalization_status = "completo"
    elif len(missing) >= len(_REQUIRED_STRUCTURED_FIELDS):
        normalization_status = "sem_campos_estruturados"
    else:
        normalization_status = "parcial"

    record["normalization_status"] = normalization_status
    record["missing_fields"] = ",".join(missing)
    return {column: record.get(column, "") for column in NORMALIZED_COLUMNS}


def normalize_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    return [build_normalized_record(row) for row in rows]


def export_normalized_csv(output_dir: Path | str, records: List[Dict[str, Any]] | None = None, logger: Any = None) -> Dict[str, Any]:
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

    normalized_rows = normalize_rows(rows)
    csv_path = output_path / OUTPUT_FILENAME
    csv_writer.write_csv(normalized_rows, csv_path, columns=NORMALIZED_COLUMNS)

    if logger is not None:
        logger.info(
            "Relatorio PARCERIAS DESCONTINUADAS normalizado gerado: registros=%d latest=%s",
            len(normalized_rows),
            csv_path,
        )
    return {"records": len(normalized_rows), "latest_path": csv_path}
