from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from app.documents.types import DocumentTypeSpec


def sanitize_filename_part(value: str, fallback: str = "sem_id") -> str:
    cleaned = re.sub(r"[^\w.-]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_")
    if not cleaned:
        return fallback
    return cleaned[:80]


def save_snapshot_json(
    *,
    spec: DocumentTypeSpec,
    processo: str,
    protocolo_documento: str,
    snapshot: dict[str, Any],
    output_dir: Path,
    logger: Any,
    extra_payload: Optional[dict[str, Any]] = None,
) -> Optional[Path]:
    processo_id = sanitize_filename_part(processo, fallback="sem_processo")
    filename = f"{spec.snapshot_prefix}_{processo_id}.json"
    filepath = output_dir / filename
    payload: Dict[str, Any] = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        "snapshot": snapshot,
    }
    if extra_payload:
        payload.update(extra_payload)
    try:
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
    record = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        "snapshot_mode": (snapshot.get("extraction_mode", "") or ""),
        "text_chars": len(snapshot.get("text", "") or ""),
        "tables_count": len(snapshot.get("tables", []) or []),
        "json_path": str(output_path) if output_path else "",
    }
    if collection_context:
        record.update(collection_context)
    return record
