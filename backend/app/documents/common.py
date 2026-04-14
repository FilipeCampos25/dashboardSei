from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from app.documents.types import DocumentTypeSpec


MOJIBAKE_MARKERS = ("Ã", "Â", "â", "\ufffd")


def sanitize_filename_part(value: str, fallback: str = "sem_id") -> str:
    cleaned = re.sub(r"[^\w.-]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_")
    if not cleaned:
        return fallback
    return cleaned[:80]


def maybe_fix_mojibake(value: str) -> str:
    text = value or ""
    if not text or not any(marker in text for marker in MOJIBAKE_MARKERS):
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
        if not any(marker in repaired for marker in MOJIBAKE_MARKERS):
            break
    return repaired


def sanitize_text_payload(value: Any) -> Any:
    if isinstance(value, str):
        return maybe_fix_mojibake(value)
    if isinstance(value, list):
        return [sanitize_text_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_text_payload(item) for item in value)
    if isinstance(value, dict):
        return {key: sanitize_text_payload(item) for key, item in value.items()}
    return value


def sanitize_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    sanitized = sanitize_text_payload(snapshot)
    return sanitized if isinstance(sanitized, dict) else {}


def derive_search_outcome_status(collection_context: Optional[dict[str, Any]] = None) -> dict[str, str]:
    context = collection_context or {}
    explicit_status = str(context.get("validation_status", "") or "").strip()
    selection_reason = str(context.get("selection_reason", "") or "").strip()
    extraction_error = str(context.get("extraction_error", "") or "").strip()

    status = explicit_status or "not_found"
    if not explicit_status:
        if selection_reason == "search_context_stagnation":
            status = "search_context_stagnation"
        elif selection_reason in {"search_open_error", "filter_error", "filter_reopen_error"}:
            status = "filter_error"
        elif extraction_error and selection_reason != "not_found":
            status = "filter_error"

    return {
        "validation_status": status,
        "normalization_status": status,
        "discard_reason": status,
    }


def save_snapshot_json(
    *,
    spec: DocumentTypeSpec,
    processo: str,
    protocolo_documento: str,
    snapshot: dict[str, Any],
    output_dir: Path,
    logger: Any,
    extra_payload: Optional[dict[str, Any]] = None,
    snapshot_prefix_override: Optional[str] = None,
    filename_suffix: Optional[str] = None,
) -> Optional[Path]:
    processo_id = sanitize_filename_part(processo, fallback="sem_processo")
    snapshot_prefix = snapshot_prefix_override or spec.snapshot_prefix
    suffix = sanitize_filename_part(filename_suffix, fallback="").strip("_") if filename_suffix else ""
    filename = f"{snapshot_prefix}_{processo_id}{'_' + suffix if suffix else ''}.json"
    filepath = output_dir / filename
    sanitized_snapshot = sanitize_snapshot(snapshot)
    payload: Dict[str, Any] = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        "snapshot": sanitized_snapshot,
    }
    if extra_payload:
        sanitized_extra_payload = sanitize_text_payload(extra_payload)
        if isinstance(sanitized_extra_payload, dict):
            payload.update(sanitized_extra_payload)
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return filepath
    except Exception as exc:
        logger.warning(
            "Processo %s: falha ao salvar snapshot do documento (%s).",
            processo,
            exc,
        )
        return None

def build_basic_tracking_record(
    *,
    spec: DocumentTypeSpec,
    processo: str,
    protocolo_documento: str,
    snapshot: dict[str, Any],
    output_path: Optional[Path],
    collection_context: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    sanitized_snapshot = sanitize_snapshot(snapshot)
    record = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        "snapshot_mode": (sanitized_snapshot.get("extraction_mode", "") or ""),
        "text_chars": len(sanitized_snapshot.get("text", "") or ""),
        "tables_count": len(sanitized_snapshot.get("tables", []) or []),
        "json_path": str(output_path) if output_path else "",
    }
    if collection_context:
        sanitized_context = sanitize_text_payload(collection_context)
        if isinstance(sanitized_context, dict):
            record.update(sanitized_context)
    return record
