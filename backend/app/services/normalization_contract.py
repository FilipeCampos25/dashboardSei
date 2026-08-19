from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional


SOURCE_DOCUMENT_TEXT = "document_text"
SOURCE_DOCUMENT_TITLE = "document_title"
SOURCE_DOCUMENT_METADATA = "document_metadata"
SOURCE_TABLE = "table"
SOURCE_PREVIEW = "preview"
SOURCE_DERIVED = "derived"
SOURCE_FALLBACK = "fallback"
SOURCE_MISSING = "missing"

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"

QUALITY_HIGH = "high"
QUALITY_MEDIUM = "medium"
QUALITY_LOW = "low"

SOURCE_TYPES = {
    SOURCE_DOCUMENT_TEXT,
    SOURCE_DOCUMENT_TITLE,
    SOURCE_DOCUMENT_METADATA,
    SOURCE_TABLE,
    SOURCE_PREVIEW,
    SOURCE_DERIVED,
    SOURCE_FALLBACK,
    SOURCE_MISSING,
}

CONFIDENCE_LEVELS = {CONFIDENCE_HIGH, CONFIDENCE_MEDIUM, CONFIDENCE_LOW}
QUALITY_STATUSES = {QUALITY_HIGH, QUALITY_MEDIUM, QUALITY_LOW}


@dataclass(frozen=True)
class DocumentIdentity:
    """Additive V2 identity for a document candidate.

    Identifiers are deliberately independent. In particular, ``process_id`` is
    never reused to fill either of the document or candidate identifiers.
    """

    process_id: str
    document_id: Optional[str] = None
    candidate_id: Optional[str] = None
    source_url: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "process_id", _clean_text(self.process_id))
        for field_name in ("document_id", "candidate_id", "source_url"):
            value = _clean_text(getattr(self, field_name))
            object.__setattr__(self, field_name, value or None)

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "process_id": self.process_id,
            "document_id": self.document_id,
            "candidate_id": self.candidate_id,
            "source_url": self.source_url,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DocumentIdentity":
        return cls(
            process_id=value.get("process_id", ""),
            document_id=value.get("document_id"),
            candidate_id=value.get("candidate_id"),
            source_url=value.get("source_url"),
        )


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def normalize_source_type(value: str) -> str:
    cleaned = _clean_text(value)
    return cleaned if cleaned in SOURCE_TYPES else SOURCE_MISSING


def normalize_confidence(value: str) -> str:
    cleaned = _clean_text(value)
    return cleaned if cleaned in CONFIDENCE_LEVELS else CONFIDENCE_LOW


def make_field(
    *,
    value: Any,
    raw_value: Any = None,
    source_type: str,
    confidence: str,
    rule_id: str,
    warning: str = "",
) -> Dict[str, Any]:
    source = normalize_source_type(source_type)
    normalized_confidence = normalize_confidence(confidence)
    field_value = "" if value is None else value
    raw = field_value if raw_value is None else raw_value
    if source == SOURCE_MISSING:
        normalized_confidence = CONFIDENCE_LOW
    return {
        "value": field_value,
        "raw_value": "" if raw is None else raw,
        "source_type": source,
        "confidence": normalized_confidence,
        "rule_id": _clean_text(rule_id),
        "warning": _clean_text(warning),
    }


def make_missing_field(*, rule_id: str, warning: str = "missing") -> Dict[str, Any]:
    return make_field(
        value="",
        raw_value="",
        source_type=SOURCE_MISSING,
        confidence=CONFIDENCE_LOW,
        rule_id=rule_id,
        warning=warning,
    )


def build_field_sources(fields: Mapping[str, Mapping[str, Any]]) -> Dict[str, Dict[str, str]]:
    sources: Dict[str, Dict[str, str]] = {}
    for name, field in fields.items():
        sources[name] = {
            "source_type": normalize_source_type(str(field.get("source_type", "") or "")),
            "confidence": normalize_confidence(str(field.get("confidence", "") or "")),
            "rule_id": _clean_text(field.get("rule_id", "")),
            "warning": _clean_text(field.get("warning", "")),
        }
    return sources


def build_quality(
    *,
    fields: Mapping[str, Mapping[str, Any]],
    is_canonical_candidate: bool,
    validation_status: str,
    publication_status: str,
    normalization_status: str,
    extra_issues: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    issues = [_clean_text(issue) for issue in (extra_issues or []) if _clean_text(issue)]
    present_fields = 0
    weighted_score = 0
    for name, field in fields.items():
        source_type = normalize_source_type(str(field.get("source_type", "") or ""))
        confidence = normalize_confidence(str(field.get("confidence", "") or ""))
        warning = _clean_text(field.get("warning", ""))
        value = field.get("value", "")
        has_value = bool(_clean_text(value))

        if not has_value or source_type == SOURCE_MISSING:
            issues.append(f"{name}:missing")
            continue

        present_fields += 1
        weighted_score += {CONFIDENCE_HIGH: 100, CONFIDENCE_MEDIUM: 70, CONFIDENCE_LOW: 40}[confidence]
        if source_type in {SOURCE_PREVIEW, SOURCE_FALLBACK}:
            issues.append(f"{name}:{source_type}")
        if warning:
            issues.append(f"{name}:{warning}")

    score = round(weighted_score / max(1, len(fields)), 2) if fields else 0.0
    if not is_canonical_candidate:
        issues.append("not_canonical_candidate")
        score = min(score, 55.0)
    if _clean_text(validation_status) not in {"valid_for_requested_type", ""}:
        issues.append(f"validation_status:{_clean_text(validation_status)}")
        score = min(score, 60.0)
    if _clean_text(publication_status) != "published_gold":
        score = min(score, 70.0)
    if present_fields == 0:
        score = 0.0

    unique_issues = list(dict.fromkeys(issues))
    status = QUALITY_HIGH if score >= 85 and not unique_issues else QUALITY_MEDIUM if score >= 55 else QUALITY_LOW
    return {"score": score, "status": status, "issues": unique_issues}


def build_document_contract(
    *,
    processo: str,
    requested_type: str,
    resolved_document_type: str,
    documento: Optional[str],
    found: bool,
    is_canonical_candidate: bool,
    validation_status: str,
    publication_status: str,
    normalization_status: str,
    fields: Mapping[str, Mapping[str, Any]],
    quality: Optional[Mapping[str, Any]] = None,
    extra_issues: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    normalized_fields = {name: dict(field) for name, field in fields.items()}
    computed_quality = quality or build_quality(
        fields=normalized_fields,
        is_canonical_candidate=is_canonical_candidate,
        validation_status=validation_status,
        publication_status=publication_status,
        normalization_status=normalization_status,
        extra_issues=extra_issues,
    )
    status = str(computed_quality.get("status", "") or "")
    if status not in QUALITY_STATUSES:
        computed_quality = {**dict(computed_quality), "status": QUALITY_LOW}
    return {
        "processo": _clean_text(processo),
        "requested_type": _clean_text(requested_type),
        "resolved_document_type": _clean_text(resolved_document_type),
        "documento": None if documento is None else _clean_text(documento),
        "found": bool(found),
        "is_canonical_candidate": bool(is_canonical_candidate),
        "validation_status": _clean_text(validation_status),
        "publication_status": _clean_text(publication_status),
        "normalization_status": _clean_text(normalization_status),
        "fields": normalized_fields,
        "field_sources": build_field_sources(normalized_fields),
        "quality": dict(computed_quality),
    }
