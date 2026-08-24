"""Deterministic orchestration of frozen snapshots into derived V2 artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from app.config import get_settings
from app.services.contract_adapters import V2_SCHEMA_VERSION, adapt_legacy_record, write_v2_sidecar
from app.services.portable_paths import PortableArtifactRef
from app.services.pipeline_states import (
    AccessState,
    AcquisitionState,
    DiscoveryState,
    ExtractionState,
    OpeningState,
)


SUPPORTED_FAMILIES = ("act", "pt", "ted", "administrative")

_FAMILY_ALIASES = {
    "act": "act",
    "acordo_cooperacao_tecnica": "act",
    "pt": "pt",
    "plano_trabalho": "pt",
    "ted": "ted",
    "termo_execucao_descentralizada": "ted",
    "administrative": "administrative",
    "administrativo": "administrative",
    "documento_administrativo": "administrative",
    "memorando": "administrative",
}

_FAMILY_FIELDS: dict[str, tuple[str, ...]] = {
    "act": (
        "numero_acordo", "data_assinatura", "datas_assinatura", "data_publicacao",
        "vigencia_raw", "vigencia_inicio", "vigencia_fim", "orgao_convenente",
        "orgao_intermediario", "objeto", "gestor_titular", "gestor_substituto",
        "unidade_responsavel", "relatorio_encerramento",
    ),
    "pt": (
        "parceiro", "data_assinatura", "datas_assinatura", "vigencia_raw",
        "vigencia_inicio", "vigencia_fim", "objeto", "atribuicoes_raw",
        "metas_raw", "acoes_raw",
    ),
    "ted": (
        "numero_ted", "ano_ted", "objeto", "unidade_descentralizadora",
        "unidade_descentralizada", "valor_global", "vigencia_inicio", "vigencia_fim",
        "plano_aplicacao", "cronograma_desembolso", "metas", "prestacao_contas",
    ),
    "administrative": (
        "funcao_administrativa", "origem", "destino", "data", "data_assinatura",
        "datas_assinatura", "assunto", "resumo", "acao_solicitada", "prazo",
        "documentos_mencionados",
    ),
}


class OfflineReprocessorError(RuntimeError):
    """Base error for unsafe or invalid reprocessor configuration."""


@dataclass(frozen=True)
class ReprocessResult:
    source: Path
    status: str
    family: str | None = None
    output: Path | None = None
    stage: str | None = None
    reason: str | None = None
    backfill: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ReprocessReport:
    results: tuple[ReprocessResult, ...]

    @property
    def processed(self) -> int:
        return sum(item.status == "processed" for item in self.results)

    @property
    def failed(self) -> int:
        return sum(item.status == "failed" for item in self.results)

    @property
    def unresolved(self) -> int:
        return sum(item.status == "unresolved" for item in self.results)

    @property
    def backfill_summary(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "records": self.processed,
            "identity_recovered": {"process_id": 0, "document_id": 0, "candidate_id": 0},
            "identity_missing": 0,
            "states": {name: {} for name in ("discovery", "opening", "access", "extraction")},
            "not_inferable": 0,
            "conflicts": 0,
        }
        for result in self.results:
            metadata = result.backfill
            if result.status != "processed" or not isinstance(metadata, Mapping):
                continue
            fields = metadata.get("fields", {})
            for name in summary["identity_recovered"]:
                if fields.get(f"identity.{name}", {}).get("classification") in {"observed", "derived"}:
                    summary["identity_recovered"][name] += 1
            if not any(summary_field.get("value_present") for summary_field in (
                fields.get("identity.document_id", {}), fields.get("identity.candidate_id", {})
            )):
                summary["identity_missing"] += 1
            for dimension in summary["states"]:
                detail = fields.get(f"acquisition_state.{dimension}", {})
                value = str(detail.get("value", ""))
                if value:
                    summary["states"][dimension][value] = summary["states"][dimension].get(value, 0) + 1
            summary["not_inferable"] += sum(
                detail.get("classification") == "not_inferable" for detail in fields.values()
            )
            summary["conflicts"] += len(metadata.get("conflicts", ()))
        for values in summary["states"].values():
            ordered = dict(sorted(values.items()))
            values.clear()
            values.update(ordered)
        return summary


def _clean_signal(value: Any) -> str | None:
    if value is None:
        return None
    return _FAMILY_ALIASES.get(str(value).strip().lower().replace("-", "_"))


def _fixture_payload(document: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = document.get("payload")
    return payload if isinstance(payload, Mapping) else document


def _historical_backfill(document: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Recover only V2 facts supported by persisted historical evidence."""

    from app.documents.common import identity_from_source_url

    payload = _fixture_payload(document)
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), Mapping) else {}
    collection = payload.get("collection") if isinstance(payload.get("collection"), Mapping) else {}
    explicit_identity = payload.get("identity") if isinstance(payload.get("identity"), Mapping) else {}
    containers = (explicit_identity, payload, collection)
    fields: dict[str, dict[str, Any]] = {}
    conflicts: list[str] = []

    def candidates(name: str) -> set[str]:
        return {
            str(container.get(name)).strip()
            for container in containers
            if container.get(name) not in (None, "")
        }

    process_values = candidates("process_id") | candidates("processo")
    process_id = next(iter(process_values)) if len(process_values) == 1 else ""
    if len(process_values) > 1:
        conflicts.append("identity.process_id")
    fields["identity.process_id"] = {
        "classification": "observed" if process_id else "conflict" if process_values else "not_inferable",
        "reason": "explicit_legacy_process" if process_id else "conflicting_sources" if process_values else "not_observable_in_legacy",
        "value_present": bool(process_id),
    }

    source_values = candidates("source_url")
    persisted_url = str(snapshot.get("url") or "").strip()
    if persisted_url:
        source_values.add(persisted_url)
    source_url = next(iter(source_values)) if len(source_values) == 1 else None
    if len(source_values) > 1:
        conflicts.append("identity.source_url")
    fields["identity.source_url"] = {
        "classification": "observed" if source_url else "conflict" if source_values else "not_inferable",
        "reason": "persisted_source_url" if source_url else "conflicting_sources" if source_values else "not_observable_in_legacy",
        "value_present": bool(source_url),
    }
    url_identity = identity_from_source_url(source_url) if source_url else {}

    identity: dict[str, Any] = {"process_id": process_id, "source_url": source_url}
    for name in ("document_id", "candidate_id"):
        explicit = candidates(name)
        structured = str(url_identity.get(name) or "").strip()
        all_values = explicit | ({structured} if structured else set())
        value = next(iter(all_values)) if len(all_values) == 1 else None
        if len(all_values) > 1:
            conflicts.append(f"identity.{name}")
        classification = (
            "observed" if value and value in explicit else "derived" if value else
            "conflict" if all_values else "not_inferable"
        )
        reason = (
            "explicit_identifier" if classification == "observed" else
            "structured_source_url" if classification == "derived" else
            "conflicting_sources" if classification == "conflict" else "not_observable_in_legacy"
        )
        identity[name] = value
        fields[f"identity.{name}"] = {
            "classification": classification, "reason": reason, "value_present": bool(value)
        }

    explicit_state = payload.get("acquisition_state")
    if isinstance(explicit_state, Mapping):
        acquisition = AcquisitionState.from_dict(explicit_state)
        for dimension, value in acquisition.to_dict().items():
            fields[f"acquisition_state.{dimension}"] = {
                "classification": "observed", "reason": "explicit_acquisition_state", "value": value
            }
    else:
        found_values = {container.get("found") for container in (payload, collection) if isinstance(container.get("found"), bool)}
        if found_values == {True}:
            discovery = DiscoveryState.FOUND
            discovery_classification, discovery_reason = "derived", "legacy_found_true"
        elif found_values == {False}:
            discovery = DiscoveryState.NOT_FOUND
            discovery_classification, discovery_reason = "derived", "legacy_found_false"
        elif len(found_values) > 1:
            discovery = DiscoveryState.NOT_SEARCHED
            discovery_classification, discovery_reason = "conflict", "conflicting_found_sources"
            conflicts.append("acquisition_state.discovery")
        else:
            discovery = DiscoveryState.NOT_SEARCHED
            discovery_classification, discovery_reason = "not_inferable", "not_observable_in_legacy"

        extraction_error = str(snapshot.get("extraction_error") or collection.get("extraction_error") or "").strip()
        if discovery is DiscoveryState.FOUND and extraction_error:
            acquisition = AcquisitionState(
                discovery, OpeningState.OPENED, AccessState.ACCESSIBLE, ExtractionState.EXTRACTION_FAILED
            )
            later = {
                "opening": ("derived", "explicit_extractor_failure"),
                "access": ("derived", "explicit_extractor_failure"),
                "extraction": ("observed", "explicit_extraction_error"),
            }
        else:
            acquisition = AcquisitionState(
                discovery, OpeningState.NOT_ATTEMPTED, AccessState.UNKNOWN, ExtractionState.NOT_ATTEMPTED
            )
            later = {name: ("not_inferable", "not_observable_in_legacy") for name in ("opening", "access", "extraction")}
        fields["acquisition_state.discovery"] = {
            "classification": discovery_classification, "reason": discovery_reason, "value": acquisition.discovery.value
        }
        state_dict = acquisition.to_dict()
        for dimension, (classification, reason) in later.items():
            fields[f"acquisition_state.{dimension}"] = {
                "classification": classification, "reason": reason, "value": state_dict[dimension]
            }

    metadata = {"fields": fields, "conflicts": sorted(conflicts)}
    return {"identity": identity, "acquisition_state": acquisition.to_dict()}, metadata


def determine_family(document: Mapping[str, Any], *, explicit: str | None = None) -> str:
    """Resolve only persisted, exact family signals; never inspect document text."""

    if explicit is not None:
        family = _clean_signal(explicit)
        if family is None:
            raise ValueError(f"unsupported explicit family: {explicit!r}")
        return family

    payload = _fixture_payload(document)
    metadata = document.get("metadata") if isinstance(document.get("metadata"), Mapping) else {}
    values = [metadata.get("family")]
    for container in (document, payload):
        values.extend(container.get(key) for key in (
            "family", "document_family", "requested_type", "resolved_document_type", "document_type"
        ))
    resolved = {_clean_signal(value) for value in values if value not in (None, "")}
    resolved.discard(None)
    if not resolved:
        raise ValueError("family cannot be determined from explicit persisted metadata")
    if len(resolved) != 1:
        raise ValueError(f"conflicting family metadata: {sorted(resolved)}")
    return resolved.pop()


def _normalizer(family: str) -> Callable[[Mapping[str, Any], Path], Mapping[str, Any]]:
    # Imports remain inside the dispatch and reference service-only builders.
    # No scraper, driver factory, integration client, or exporter is imported.
    if family == "act":
        from app.services.act_normalizer import build_normalized_record
        return lambda payload, path: build_normalized_record(dict(payload), path)
    if family == "pt":
        from app.services.pt_normalizer import build_normalized_record
        return lambda payload, path: build_normalized_record(dict(payload), {}, path)
    if family == "ted":
        from app.services.ted_normalizer import build_normalized_record
        return lambda payload, path: build_normalized_record(dict(payload), path)[0]
    if family == "administrative":
        from app.services.documento_administrativo_normalizer import build_normalized_record
        return lambda payload, path: build_normalized_record(dict(payload), path)
    raise ValueError(f"unsupported family: {family}")


def _target_for(source: Path, destination: Path, *, relative_to: Path | None) -> Path:
    relative = source.relative_to(relative_to) if relative_to is not None else Path(source.name)
    return destination / relative.parent / f"{relative.stem}.v2.json"


def reprocess_snapshot(
    source: str | Path,
    destination: str | Path,
    *,
    family: str | None = None,
    _relative_to: Path | None = None,
) -> ReprocessResult:
    """Reprocess one JSON snapshot without modifying it or consulting online layers."""

    if not get_settings().offline_only:
        raise OfflineReprocessorError("OFFLINE_ONLY=true is required for offline reprocessing")

    source_path = Path(source).resolve(strict=False)
    destination_path = Path(destination).resolve(strict=False)
    target = _target_for(source_path, destination_path, relative_to=_relative_to)
    try:
        raw = source_path.read_text(encoding="utf-8")
        document = json.loads(raw)
        if not isinstance(document, Mapping):
            raise ValueError("snapshot root must be a JSON object")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        return ReprocessResult(source_path, "failed", stage="input", reason=f"{type(exc).__name__}: {exc}")

    try:
        resolved_family = determine_family(document, explicit=family)
    except ValueError as exc:
        return ReprocessResult(source_path, "unresolved", stage="family", reason=str(exc))

    try:
        legacy_record = _normalizer(resolved_family)(_fixture_payload(document), source_path)
    except Exception as exc:
        return ReprocessResult(
            source_path, "failed", family=resolved_family, stage="normalization",
            reason=f"{type(exc).__name__}: {exc}",
        )
    try:
        source_root = source_path.parent if _relative_to is None else _relative_to
        record_v2 = adapt_legacy_record(
            legacy_record,
            field_names=_FAMILY_FIELDS[resolved_family],
            artifact_root=source_root,
        )
        recovered, backfill_metadata = _historical_backfill(document)
        record_v2["identity"] = recovered["identity"]
        record_v2["acquisition_state"] = recovered["acquisition_state"]
        record_v2["backfill_metadata"] = backfill_metadata
    except Exception as exc:
        return ReprocessResult(
            source_path, "failed", family=resolved_family, stage="adaptation",
            reason=f"{type(exc).__name__}: {exc}",
        )

    envelope = {
        "schema_version": V2_SCHEMA_VERSION,
        "family": resolved_family,
        # Kept for readers of NORM-P1-003 sidecars; the structured reference is
        # the portable V2 location interpreted against the explicit source root.
        "source_artifact": source_path.name if _relative_to is None else source_path.relative_to(_relative_to).as_posix(),
        "source_artifact_ref": PortableArtifactRef.from_path(source_path, root=source_root).to_dict(),
        "record": record_v2,
    }
    try:
        write_v2_sidecar(target, envelope)
    except Exception as exc:
        return ReprocessResult(
            source_path, "failed", family=resolved_family, stage="writing",
            reason=f"{type(exc).__name__}: {exc}",
        )
    return ReprocessResult(
        source_path, "processed", family=resolved_family, output=target, backfill=backfill_metadata
    )


def reprocess_directory(
    source: str | Path,
    destination: str | Path,
    *,
    family: str | None = None,
) -> ReprocessReport:
    """Process every JSON below a directory in stable relative-path order."""

    source_root = Path(source).resolve(strict=True)
    destination_root = Path(destination).resolve(strict=False)
    if not source_root.is_dir():
        raise OfflineReprocessorError(f"source is not a directory: {source_root}")
    if destination_root == source_root or source_root in destination_root.parents:
        raise OfflineReprocessorError("destination must be separate from the frozen source tree")
    paths = sorted(source_root.rglob("*.json"), key=lambda path: path.relative_to(source_root).as_posix())
    return ReprocessReport(tuple(
        reprocess_snapshot(path, destination_root, family=family, _relative_to=source_root)
        for path in paths
    ))
