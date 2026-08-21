"""Deterministic orchestration of frozen snapshots into derived V2 artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from app.config import get_settings
from app.services.contract_adapters import V2_SCHEMA_VERSION, adapt_legacy_record, write_v2_sidecar
from app.services.portable_paths import PortableArtifactRef


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


def _clean_signal(value: Any) -> str | None:
    if value is None:
        return None
    return _FAMILY_ALIASES.get(str(value).strip().lower().replace("-", "_"))


def _fixture_payload(document: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = document.get("payload")
    return payload if isinstance(payload, Mapping) else document


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
    return ReprocessResult(source_path, "processed", family=resolved_family, output=target)


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
