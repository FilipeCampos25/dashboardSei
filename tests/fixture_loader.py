"""Loader and hygiene checks for versioned offline document fixtures."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "documents"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"

FAMILIES = {"pt", "act", "ted", "administrative"}
ORIGINS = {"synthetic", "sanitized"}
TECHNICAL_STATES = {
    "EXTRACTED",
    "EMPTY_CONTENT",
    "IFRAME_UNAVAILABLE",
    "RELATED",
}
EXTRACTION_METHODS = {"html_dom", "pdf_native", "ocr", "zip_docx", "none"}
REQUIRED_METADATA = {
    "id",
    "description",
    "family",
    "document_role",
    "extraction_method",
    "technical_state",
    "origin",
}

_SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "cookie",
    "credentials",
    "password",
    "secret",
    "token",
}
_SENSITIVE_VALUES = (
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}"),
    re.compile(r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\b"),
)
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_PERSONAL_POSIX = re.compile(r"^/(?:Users|home|root)(?:/|$)", re.IGNORECASE)


def _iter_nodes(value: Any, location: str = "fixture"):
    if isinstance(value, dict):
        for key, child in value.items():
            yield key, child, f"{location}.{key}"
            yield from _iter_nodes(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield None, child, f"{location}[{index}]"
            yield from _iter_nodes(child, f"{location}[{index}]")


def validate_fixture(fixture: dict[str, Any]) -> None:
    """Reject malformed, unsafe, or environment-coupled fixture content."""

    metadata = fixture.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("fixture.metadata must be an object")
    missing = REQUIRED_METADATA - metadata.keys()
    if missing:
        raise ValueError(f"fixture metadata missing: {sorted(missing)}")
    if metadata["family"] not in FAMILIES:
        raise ValueError(f"invalid fixture family: {metadata['family']}")
    if metadata["origin"] not in ORIGINS:
        raise ValueError(f"invalid fixture origin: {metadata['origin']}")
    if metadata["technical_state"] not in TECHNICAL_STATES:
        raise ValueError(f"invalid technical state: {metadata['technical_state']}")
    if metadata["extraction_method"] not in EXTRACTION_METHODS:
        raise ValueError(f"invalid extraction method: {metadata['extraction_method']}")
    if not isinstance(fixture.get("payload"), dict):
        raise ValueError("fixture.payload must be an object")

    for key, value, location in _iter_nodes(fixture):
        if isinstance(key, str) and key.lower() in _SENSITIVE_KEYS:
            raise ValueError(f"sensitive key at {location}")
        if not isinstance(value, str):
            continue
        normalized = value.replace("\\", "/").lower()
        if "backend/output" in normalized:
            raise ValueError(f"backend/output reference at {location}")
        if _WINDOWS_ABSOLUTE.match(value) or _PERSONAL_POSIX.match(value):
            raise ValueError(f"absolute personal path at {location}")
        if any(pattern.search(value) for pattern in _SENSITIVE_VALUES):
            raise ValueError(f"sensitive value at {location}")


def load_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "1.0" or not isinstance(manifest.get("fixtures"), list):
        raise ValueError("invalid fixture manifest schema")
    return manifest


def load_fixture(relative_path: str) -> dict[str, Any]:
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("fixture path must remain relative to the fixture directory")
    resolved = (FIXTURE_ROOT / path).resolve(strict=True)
    resolved.relative_to(FIXTURE_ROOT.resolve(strict=True))
    fixture = json.loads(resolved.read_text(encoding="utf-8"))
    validate_fixture(fixture)
    return fixture


def load_all_fixtures() -> list[dict[str, Any]]:
    manifest = load_manifest()
    return [load_fixture(entry["path"]) for entry in manifest["fixtures"]]
