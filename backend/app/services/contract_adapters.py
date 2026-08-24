"""Conservative legacy-to-V2 adapters and opt-in parallel output."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from app.config import get_settings
from app.output import csv_writer
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.portable_paths import PortableArtifactRef, PortablePathError
from app.services.pipeline_states import (
    AccessState,
    AcquisitionState,
    DiscoveryState,
    ExtractionState,
    OpeningState,
)
from app.services.semantic_states import (
    AffinityState,
    CanonicalState,
    ClassificationState,
    DocumentFunctionState,
    PublicationState,
    SemanticState,
)


V2_SCHEMA_VERSION = "2.0"

# These mappings describe origin only. They do not imply publication, winner,
# document identity, technical access, or field applicability.
_EXPLICIT_SOURCE_KINDS = {
    "document_text": SourceKind.DOCUMENT,
    "document_title": SourceKind.DOCUMENT,
    "document_metadata": SourceKind.DOCUMENT,
    "table": SourceKind.DOCUMENT,
    "preview": SourceKind.PREVIEW,
    "derived": SourceKind.DERIVED,
}


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _adapt_acquisition_diagnostic(payload: Mapping[str, Any]) -> dict[str, str]:
    code = _optional_text(payload.get("acquisition_diagnostic_code")) or ""
    stage = _optional_text(payload.get("acquisition_diagnostic_stage")) or ""
    allowed = {
        "IFRAME_UNAVAILABLE": "access",
        "ACCESS_RESTRICTED": "access",
        "TIMEOUT": "opening",
        "EMPTY_CONTENT": "extraction",
        "EXTRACTION_FAILED": "extraction",
    }
    if code not in allowed or stage != allowed[code]:
        return {"code": "", "stage": ""}
    return {"code": code, "stage": stage}


def adapt_legacy_record(
    payload: Mapping[str, Any],
    *,
    field_names: Sequence[str] = (),
    artifact_root: str | Path | None = None,
    artifact_path_fields: Sequence[str] = ("candidate_json_path", "json_path"),
) -> dict[str, Any]:
    """Translate only facts that the generic legacy shape proves.

    Family-specific callers must opt fields in. The ambiguous legacy
    ``documento`` field is deliberately never promoted to ``document_id``.
    """

    process_id = _optional_text(payload.get("process_id")) or _optional_text(payload.get("processo")) or ""
    identity = DocumentIdentity(
        process_id=process_id,
        document_id=_optional_text(payload.get("document_id")),
        candidate_id=_optional_text(payload.get("candidate_id")),
        source_url=_optional_text(payload.get("source_url")),
    )

    found = payload.get("found")
    discovery = (
        DiscoveryState.FOUND
        if found is True
        else DiscoveryState.NOT_FOUND
        if found is False
        else DiscoveryState.NOT_SEARCHED
    )
    explicit_acquisition = payload.get("acquisition_state")
    acquisition = (
        AcquisitionState.from_dict(explicit_acquisition)
        if isinstance(explicit_acquisition, Mapping)
        else AcquisitionState(
            discovery=discovery,
            opening=OpeningState.NOT_ATTEMPTED,
            access=AccessState.UNKNOWN,
            extraction=ExtractionState.NOT_ATTEMPTED,
        )
    )
    semantic = SemanticState(
        classification=ClassificationState.NOT_CLASSIFIED,
        function=DocumentFunctionState.NOT_EVALUATED,
        affinity=AffinityState.NOT_EVALUATED,
        canonical=CanonicalState.NOT_EVALUATED,
        publication=PublicationState.NOT_EVALUATED,
    )

    fields = [_adapt_field(name, payload.get(name), identity) for name in field_names]
    diagnostics = []
    if payload.get("documento") not in (None, "") and identity.document_id is None:
        diagnostics.append("legacy_documento_not_promoted")
    if payload.get("publication_status") not in (None, ""):
        diagnostics.append("legacy_publication_status_not_expanded")

    artifact_ref = None
    if artifact_root is not None:
        for field_name in artifact_path_fields:
            raw_path = _optional_text(payload.get(field_name))
            if raw_path is None:
                continue
            try:
                artifact_ref = PortableArtifactRef.from_path(raw_path, root=artifact_root).to_dict()
            except PortablePathError:
                diagnostics.append(f"{field_name}_not_portable")
            break

    adapted = {
        "identity": identity.to_dict(),
        "acquisition_state": acquisition.to_dict(),
        "acquisition_diagnostic": _adapt_acquisition_diagnostic(payload),
        "semantic_state": semantic.to_dict(),
        "fields": [field.to_dict() for field in fields],
        "diagnostics": diagnostics,
    }
    if artifact_ref is not None:
        adapted["artifact_ref"] = artifact_ref
    return adapted


def _adapt_field(name: str, legacy_value: Any, identity: DocumentIdentity) -> FieldResult:
    value = legacy_value
    source_type = None
    rule_id = None
    if isinstance(legacy_value, Mapping):
        value = legacy_value.get("value")
        source_type = _optional_text(legacy_value.get("source_type"))
        rule_id = _optional_text(legacy_value.get("rule_id"))

    evidences = ()
    source_kind = _EXPLICIT_SOURCE_KINDS.get(source_type or "")
    if source_kind is not None:
        has_document_reference = any((identity.document_id, identity.candidate_id, identity.source_url))
        evidences = (
            FieldEvidence(
                field_name=name,
                source_kind=source_kind,
                source_document=identity if has_document_reference else None,
                rule_id=rule_id,
            ),
        )

    if value is None or (isinstance(value, str) and not value.strip()):
        return FieldResult(field_name=name, state=FieldState.NOT_EVALUATED, evidences=evidences)
    return FieldResult(field_name=name, state=FieldState.PRESENT, value=value, evidences=evidences)


def serialize_v2(payload: Mapping[str, Any]) -> str:
    """Return stable UTF-8 JSON text without runtime metadata."""

    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def v2_sidecar_path(legacy_path: str | Path) -> Path:
    legacy = Path(legacy_path)
    stem = legacy.stem.removesuffix("_latest")
    return legacy.parent / "v2" / f"{stem}.v2.json"


def write_v2_sidecar(path: str | Path, payload: Mapping[str, Any]) -> Path:
    """Atomically publish a complete V2 sidecar or raise explicitly."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    try:
        temporary.write_text(serialize_v2(payload), encoding="utf-8")
        os.replace(temporary, target)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return target


def write_csv_with_v2(
    records: list[dict[str, Any]],
    filepath: str | Path,
    *,
    columns: Sequence[str] | None = None,
    field_names: Sequence[str] = (),
    enabled: bool | None = None,
    artifact_root: str | Path | None = None,
) -> Path | None:
    """Write the unchanged legacy CSV and an optional, separate V2 sidecar.

    A V2 failure is raised after the successful legacy write. It never leaves
    a temporary sidecar that could be mistaken for a valid artifact.
    """

    csv_writer.write_csv(records, filepath, columns=columns)
    active = get_settings().v2_dual_write if enabled is None else enabled
    if not active:
        return None

    envelope = {
        "schema_version": V2_SCHEMA_VERSION,
        "legacy_artifact": Path(filepath).name,
        "records": [
            adapt_legacy_record(
                record,
                field_names=field_names,
                artifact_root=Path(filepath).parent if artifact_root is None else artifact_root,
            )
            for record in records
        ],
    }
    return write_v2_sidecar(v2_sidecar_path(filepath), envelope)
