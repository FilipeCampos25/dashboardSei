from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from app.documents.types import DocumentTypeSpec
from app.services.pipeline_states import (
    AccessState,
    AcquisitionState,
    DiscoveryState,
    ExtractionState,
    OpeningState,
)


def identity_from_source_url(source_url: Any) -> dict[str, Optional[str]]:
    """Extract only supported SEI identity parameters from an observed URL."""
    url = str(source_url or "").strip()
    identity: dict[str, Optional[str]] = {
        "document_id": None,
        "candidate_id": None,
        "source_url": url or None,
    }
    if not url:
        return identity
    try:
        query = parse_qs(urlparse(url).query, keep_blank_values=False)
    except ValueError:
        return identity
    document_values = query.get("id_documento", [])
    candidate_values = query.get("id_anexo", [])
    if len(document_values) == 1 and document_values[0].strip().isdigit():
        identity["document_id"] = document_values[0].strip()
    if len(candidate_values) == 1 and candidate_values[0].strip().isdigit():
        identity["candidate_id"] = candidate_values[0].strip()
    return identity


def build_document_identity(
    processo: Any,
    collection_context: Optional[dict[str, Any]] = None,
    snapshot: Optional[dict[str, Any]] = None,
) -> dict[str, Optional[str]]:
    context = collection_context or {}
    source_url = context.get("source_url") or (snapshot or {}).get("url")
    observed = identity_from_source_url(source_url)
    return {
        "process_id": str(processo or "").strip(),
        "document_id": str(context.get("document_id") or observed["document_id"] or "").strip() or None,
        "candidate_id": str(context.get("candidate_id") or observed["candidate_id"] or "").strip() or None,
        "source_url": str(source_url or "").strip() or None,
    }


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


def build_acquisition_state(
    collection_context: Optional[dict[str, Any]] = None,
    snapshot: Optional[dict[str, Any]] = None,
) -> AcquisitionState:
    """Build V2 acquisition state only from explicit technical observations."""
    context = collection_context or {}
    explicit = context.get("acquisition_state")
    if isinstance(explicit, dict):
        state = AcquisitionState.from_dict(explicit)
    else:
        found = context.get("found")
        discovery = (
            DiscoveryState.FOUND
            if found is True
            else DiscoveryState.NOT_FOUND
            if found is False
            else DiscoveryState.NOT_SEARCHED
        )
        state = AcquisitionState(
            discovery=discovery,
            opening=OpeningState.NOT_ATTEMPTED,
            access=AccessState.UNKNOWN,
            extraction=ExtractionState.NOT_ATTEMPTED,
        )

    observation = (snapshot or {}).get("acquisition_observation")
    if not isinstance(observation, dict):
        return state

    opening = state.opening
    if observation.get("opening_attempted") is True:
        opening = OpeningState.OPENED if observation.get("opened") is True else OpeningState.OPEN_FAILED

    access = state.access
    if observation.get("access_observed") is True:
        access = AccessState.ACCESSIBLE

    extraction = state.extraction
    if observation.get("extraction_attempted") is True:
        if observation.get("extraction_error"):
            extraction = ExtractionState.EXTRACTION_FAILED
        elif observation.get("extraction_partial") is True:
            extraction = ExtractionState.CONTENT_PARTIAL
        elif observation.get("extraction_complete") is True:
            has_content = bool(str((snapshot or {}).get("text", "") or "").strip()) or bool(
                (snapshot or {}).get("tables", []) or []
            )
            extraction = ExtractionState.EXTRACTED if has_content else ExtractionState.EMPTY_CONTENT

    return AcquisitionState(state.discovery, opening, access, extraction)


def acquisition_state_payload(
    collection_context: Optional[dict[str, Any]] = None,
    snapshot: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    return build_acquisition_state(collection_context, snapshot).to_dict()


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
    collection = extra_payload.get("collection") if isinstance(extra_payload, dict) else None
    identity = build_document_identity(
        processo,
        collection if isinstance(collection, dict) else None,
        sanitized_snapshot,
    )
    acquisition_state = acquisition_state_payload(collection, sanitized_snapshot)
    payload: Dict[str, Any] = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        "process_id": identity["process_id"],
        "document_id": identity["document_id"],
        "candidate_id": identity["candidate_id"],
        "source_url": identity["source_url"],
        "identity": identity,
        "acquisition_state": acquisition_state,
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
    identity = build_document_identity(processo, collection_context, sanitized_snapshot)
    acquisition_state = acquisition_state_payload(collection_context, sanitized_snapshot)
    record = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "document_type": spec.key,
        "processo": processo,
        "documento": protocolo_documento,
        **identity,
        "acquisition_state": acquisition_state,
        "acquisition_state_v2": json.dumps(acquisition_state, ensure_ascii=False, sort_keys=True),
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
