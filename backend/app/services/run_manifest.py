"""Deterministic, offline inventory manifests for private pipeline baselines."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence


SCHEMA_VERSION = "1.0"
DEFAULT_INCLUDES = ("backend/output", "output/execution_log_latest.json")
COMPLETENESS_STATUSES = {"complete", "incomplete", "unknown"}
_TEMPORARY_PARTS = {"browser_downloads", "__pycache__", ".pytest_cache"}
_SENSITIVE_PARTS = {"auth", "cookies", "credentials", "sessions", "secrets"}
_SENSITIVE_FILENAMES = {"secrets.toml", "cookies.json", "credentials.json"}


def _relative_posix(path: Path, source_dir: Path) -> str:
    return path.relative_to(source_dir).as_posix()


def _is_sensitive(relative_path: str) -> bool:
    path = PurePosixPath(relative_path)
    lowered_parts = tuple(part.lower() for part in path.parts)
    name = lowered_parts[-1]
    return (
        name == ".env"
        or name.startswith(".env.")
        or name in _SENSITIVE_FILENAMES
        or any(part in _SENSITIVE_PARTS for part in lowered_parts[:-1])
    )


def _is_temporary(relative_path: str) -> bool:
    path = PurePosixPath(relative_path)
    lowered_parts = tuple(part.lower() for part in path.parts)
    return (
        path.name.lower() == "run_manifest.json"
        or path.suffix.lower() in {".tmp", ".temp"}
        or any(part in _TEMPORARY_PARTS for part in lowered_parts[:-1])
    )


def _artifact_category(relative_path: str) -> str:
    path = PurePosixPath(relative_path)
    name = path.name.lower()
    if relative_path == "output/execution_log_latest.json":
        return "operational_log"
    if "candidates" in (part.lower() for part in path.parts[:-1]):
        return "candidate"
    if "normalizado" in name or name == "dashboard_ready_latest.csv":
        return "gold"
    silver_markers = (
        "status_",
        "auditoria",
        "diagnostic",
        "classificacao",
        "review",
        "divergence",
        "shadow",
        "inventory",
        "discoveries",
    )
    if any(marker in name for marker in silver_markers):
        return "silver"
    if path.suffix.lower() == ".json":
        return "document_snapshot"
    return "other"


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_files(include_path: Path) -> Iterable[Path]:
    if include_path.is_file() or include_path.is_symlink():
        yield include_path
        return
    yield from (path for path in include_path.rglob("*") if path.is_file() or path.is_symlink())


def _resolve_inside_source(path: Path, source_dir: Path) -> Path:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(source_dir)
    except ValueError as exc:
        raise ValueError(f"Artifact escapes source directory: {path}") from exc
    return resolved


def build_run_manifest(
    source_dir: str | Path,
    *,
    run_id: str,
    origin: str,
    execution_mode: str,
    captured_at: str | None = None,
    contract_version: str | None = None,
    completeness: str = "unknown",
    includes: Sequence[str] = DEFAULT_INCLUDES,
) -> dict[str, object]:
    """Build a deterministic manifest without writing to or parsing the baseline."""

    source = Path(source_dir)
    if not source.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source}")
    if not source.is_dir():
        raise NotADirectoryError(f"Source is not a directory: {source}")
    source = source.resolve(strict=True)
    if completeness not in COMPLETENESS_STATUSES:
        raise ValueError(f"Invalid completeness status: {completeness}")
    if not run_id.strip() or not origin.strip() or not execution_mode.strip():
        raise ValueError("run_id, origin, and execution_mode must be non-empty")

    warnings: list[dict[str, object]] = []
    missing_includes: list[str] = []
    candidate_paths: dict[str, Path] = {}
    sensitive_count = 0
    temporary_count = 0

    normalized_includes = sorted({PurePosixPath(item.replace("\\", "/")).as_posix() for item in includes})
    for include in normalized_includes:
        include_path = source.joinpath(*PurePosixPath(include).parts)
        if not include_path.exists() and not include_path.is_symlink():
            missing_includes.append(include)
            continue
        for path in _iter_files(include_path):
            relative_path = _relative_posix(path, source)
            if _is_sensitive(relative_path):
                sensitive_count += 1
                continue
            if _is_temporary(relative_path):
                temporary_count += 1
                continue
            resolved = _resolve_inside_source(path, source)
            candidate_paths[relative_path] = resolved

    if missing_includes:
        warnings.append(
            {"code": "missing_includes", "count": len(missing_includes), "paths": missing_includes}
        )
    if sensitive_count:
        warnings.append({"code": "sensitive_artifacts_excluded", "count": sensitive_count})
    if temporary_count:
        warnings.append({"code": "temporary_artifacts_excluded", "count": temporary_count})

    artifacts: list[dict[str, object]] = []
    for relative_path in sorted(candidate_paths):
        path = candidate_paths[relative_path]
        artifacts.append(
            {
                "path": relative_path,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
                "extension": path.suffix.lower() or "[none]",
                "category": _artifact_category(relative_path),
            }
        )

    extension_counts = Counter(str(item["extension"]) for item in artifacts)
    category_counts = Counter(str(item["category"]) for item in artifacts)
    canonical_inventory = json.dumps(
        artifacts, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    inventory_sha256 = hashlib.sha256(canonical_inventory).hexdigest()
    effective_completeness = "incomplete" if missing_includes else completeness
    warning_codes = [str(warning["code"]) for warning in warnings]

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "capture": {
            "origin": origin,
            "captured_at": captured_at,
            "execution_mode": execution_mode,
        },
        "contract_version": contract_version,
        "completeness": {
            "status": effective_completeness,
            "warnings": warning_codes,
        },
        "artifacts": artifacts,
        "summary": {
            "artifact_count": len(artifacts),
            "total_bytes": sum(int(item["size_bytes"]) for item in artifacts),
            "by_extension": dict(sorted(extension_counts.items())),
            "by_category": dict(sorted(category_counts.items())),
            "inventory_sha256": inventory_sha256,
        },
        "warnings": warnings,
    }


def serialize_manifest(manifest: dict[str, object]) -> str:
    """Serialize a manifest canonically for stable files and comparisons."""

    return json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
