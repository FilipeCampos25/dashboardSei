from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.output import csv_writer
from app.services.act_normalizer import (
    DOC_CLASS_MEMORANDO,
    PUBLICATION_STATUS_GOLD,
    classify_cooperation_snapshot,
)

DATE_PATTERN = r"\d{1,2}(?:[./-]\d{1,2}[./-]\d{4}|\s+de\s+[A-Za-z\u00c0-\u00ff]+\s+de\s+\d{4})"
PROCESS_PATTERN = r"[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"

NORMALIZED_COLUMNS = [
    "captured_at",
    "requested_type",
    "processo",
    "documento",
    "resolved_document_type",
    "funcao_administrativa",
    "origem",
    "destino",
    "data",
    "assunto",
    "resumo",
    "acao_solicitada",
    "prazo",
    "documentos_mencionados",
    "selection_reason",
    "classification_reason",
    "validation_status",
    "publication_status",
    "snapshot_mode",
    "json_path",
]


def _clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _snapshot_text(snapshot: Dict[str, Any]) -> str:
    return str(snapshot.get("text", "") or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _line_value(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        pattern = rf"(?im)^\s*{label}\s*[:\-]\s*(.+?)\s*$"
        match = re.search(pattern, text)
        if match:
            return _clean_spaces(match.group(1))
    return ""


def _extract_data(text: str) -> str:
    match = re.search(DATE_PATTERN, text, flags=re.IGNORECASE)
    return _clean_spaces(match.group(0)) if match else ""


def _extract_assunto(snapshot: Dict[str, Any], text: str) -> str:
    assunto = _line_value(text, ("Assunto", "Interessado", "Referencia", "Referência"))
    if assunto:
        return assunto
    return _clean_spaces(snapshot.get("title", ""))


def _extract_resumo(text: str) -> str:
    if not text:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    useful = [sentence for sentence in sentences if len(sentence) >= 30]
    return _clean_spaces(" ".join(useful[:2]))[:600]


def _extract_acao_solicitada(text: str) -> str:
    for pattern in (
        r"(?i)\bsolicit(?:o|a|amos|ar)\b(.{0,280})",
        r"(?i)\bencaminh(?:o|a|amos|ar)\b(.{0,280})",
        r"(?i)\bsubmet(?:o|e|emos|er)\b(.{0,280})",
        r"(?i)\brecomend(?:o|a|amos|ar)\b(.{0,280})",
    ):
        match = re.search(pattern, text)
        if match:
            return _clean_spaces(match.group(0))[:350]
    return ""


def _extract_prazo(text: str) -> str:
    for pattern in (
        r"(?i)\bprazo\s+(?:de|para)?\s*.{0,80}",
        r"(?i)\bat[eé]\s+" + DATE_PATTERN,
        r"(?i)\bem\s+at[eé]\s+\d+\s+dias?",
    ):
        match = re.search(pattern, text)
        if match:
            return _clean_spaces(match.group(0))[:160]
    return ""


def _extract_documentos_mencionados(text: str, processo: str, documento: str) -> str:
    values: List[str] = []
    for item in re.findall(PROCESS_PATTERN, text):
        if item != processo and item not in values:
            values.append(item)
    for item in re.findall(r"\b(?:SEI\s*)?\d{6,}\b", text, flags=re.IGNORECASE):
        cleaned = _clean_spaces(item)
        if cleaned != documento and cleaned not in values:
            values.append(cleaned)
    return " | ".join(values[:20])


def _classify_funcao(text: str) -> str:
    lowered = text.lower()
    checks = (
        ("encaminhamento", ("encaminh", "remeto", "submeto")),
        ("solicitação", ("solicit", "requisit", "requeiro")),
        ("resposta", ("em resposta", "respon", "manifestacao sobre")),
        ("aprovação", ("aprovo", "aprovacao", "de acordo", "autorizo")),
        ("ciência", ("ciencia", "para conhecimento", "tomo conhecimento")),
        ("correção", ("corrig", "retific", "ajust")),
        ("juntada", ("juntada", "juntar", "anexo", "anexamos")),
    )
    for label, markers in checks:
        if any(marker in lowered for marker in markers):
            return label
    return "outro"


def build_normalized_record(payload: Dict[str, Any], json_path: Path, fallback_record: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    fallback_record = fallback_record or {}
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    collection = payload.get("collection", {}) if isinstance(payload.get("collection"), dict) else {}
    processo = _clean_spaces(payload.get("processo") or fallback_record.get("processo"))
    documento = _clean_spaces(payload.get("documento") or fallback_record.get("documento"))
    analysis = classify_cooperation_snapshot(snapshot, "memorando", collection_context=collection, processo=processo)
    text = _snapshot_text(snapshot)
    origem = _line_value(text, ("De", "Origem", "Remetente"))
    destino = _line_value(text, ("Para", "Ao", "A", "Destino", "Destinatario", "Destinatário"))

    return {
        "captured_at": _clean_spaces(collection.get("captured_at") or fallback_record.get("captured_at")),
        "requested_type": "memorando",
        "processo": processo,
        "documento": documento,
        "resolved_document_type": _clean_spaces(analysis.get("resolved_document_type")),
        "funcao_administrativa": _classify_funcao(text),
        "origem": origem,
        "destino": destino,
        "data": _extract_data(text),
        "assunto": _extract_assunto(snapshot, text),
        "resumo": _extract_resumo(text),
        "acao_solicitada": _extract_acao_solicitada(text),
        "prazo": _extract_prazo(text),
        "documentos_mencionados": _extract_documentos_mencionados(text, processo, documento),
        "selection_reason": _clean_spaces(collection.get("selection_reason") or fallback_record.get("selection_reason")),
        "classification_reason": _clean_spaces(analysis.get("classification_reason")),
        "validation_status": _clean_spaces(analysis.get("validation_status")),
        "publication_status": _clean_spaces(analysis.get("publication_status")),
        "snapshot_mode": _clean_spaces(snapshot.get("extraction_mode") or fallback_record.get("snapshot_mode")),
        "json_path": str(json_path),
        "doc_class": _clean_spaces(analysis.get("doc_class")),
    }


def export_normalized_csv(output_dir: Path, records: List[Dict[str, Any]], logger: Any = None) -> Dict[str, Any]:
    csv_writer.ensure_output_dir(output_dir)
    normalized_rows: List[Dict[str, Any]] = []
    for record in records:
        if record.get("publication_status") != PUBLICATION_STATUS_GOLD or not record.get("json_path"):
            continue
        json_path = Path(str(record.get("json_path", "")))
        payload = _read_json(json_path)
        if not payload:
            continue
        normalized_rows.append(build_normalized_record(payload, json_path, fallback_record=record))

    public_rows = [{column: row.get(column, "") for column in NORMALIZED_COLUMNS} for row in normalized_rows]
    admin_path = output_dir / "documento_administrativo_normalizado_latest.csv"
    csv_writer.write_csv(public_rows, admin_path, columns=NORMALIZED_COLUMNS)

    memorando_rows = [
        {column: row.get(column, "") for column in NORMALIZED_COLUMNS}
        for row in normalized_rows
        if row.get("doc_class") == DOC_CLASS_MEMORANDO
    ]
    memorando_path = output_dir / "memorando_normalizado_latest.csv"
    csv_writer.write_csv(memorando_rows, memorando_path, columns=NORMALIZED_COLUMNS)

    if logger is not None:
        logger.info(
            "Relatorio Documento Administrativo normalizado gerado: registros=%d memorandos=%d latest=%s",
            len(public_rows),
            len(memorando_rows),
            admin_path,
        )
    return {
        "records": len(public_rows),
        "memorando_records": len(memorando_rows),
        "latest_path": admin_path,
        "memorando_path": memorando_path,
    }
