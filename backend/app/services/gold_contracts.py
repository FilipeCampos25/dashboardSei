from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Tuple

from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import SemanticState


def _required_text(value: Any, field_name: str) -> str:
    cleaned = " ".join(str(value or "").replace("\r", "\n").split()).strip()
    if not cleaned:
        raise ValueError(f"{field_name} must be a non-empty string")
    return cleaned


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_index(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer or None")
    return value


class SourceKind(str, Enum):
    """Universal kinds of origin, independent from document families."""

    DOCUMENT = "document"
    PREVIEW = "preview"
    RELATED_DOCUMENT = "related_document"
    EXTERNAL = "external"
    DERIVED = "derived"


@dataclass(frozen=True)
class EvidenceLocation:
    """Optional coordinates that are recorded only when actually observed."""

    source_path: str | None = None
    page: int | None = None
    table_index: int | None = None
    row_index: int | None = None
    column_index: int | None = None
    section: str | None = None
    position: int | None = None

    def __post_init__(self) -> None:
        for field_name in ("source_path", "section"):
            object.__setattr__(self, field_name, _optional_text(getattr(self, field_name)))
        for field_name in ("page", "table_index", "row_index", "column_index", "position"):
            object.__setattr__(self, field_name, _optional_index(getattr(self, field_name), field_name))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_path": self.source_path,
            "page": self.page,
            "table_index": self.table_index,
            "row_index": self.row_index,
            "column_index": self.column_index,
            "section": self.section,
            "position": self.position,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EvidenceLocation":
        return cls(
            source_path=value.get("source_path"),
            page=value.get("page"),
            table_index=value.get("table_index"),
            row_index=value.get("row_index"),
            column_index=value.get("column_index"),
            section=value.get("section"),
            position=value.get("position"),
        )


@dataclass(frozen=True)
class DocumentGoldDecision:
    """V2 decision about one document, without asserting record completeness."""

    identity: DocumentIdentity
    semantic_state: SemanticState
    reason_codes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.identity, DocumentIdentity):
            raise TypeError("identity must be a DocumentIdentity")
        if not isinstance(self.semantic_state, SemanticState):
            raise TypeError("semantic_state must be a SemanticState")
        reasons = tuple(_required_text(reason, "reason_codes item") for reason in self.reason_codes)
        object.__setattr__(self, "reason_codes", reasons)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "semantic_state": self.semantic_state.to_dict(),
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DocumentGoldDecision":
        return cls(
            identity=DocumentIdentity.from_dict(value.get("identity", {})),
            semantic_state=SemanticState.from_dict(value.get("semantic_state", {})),
            reason_codes=tuple(value.get("reason_codes", ())),
        )


@dataclass(frozen=True)
class FieldEvidence:
    """Universal V2 provenance for one field, without publication semantics."""

    field_name: str
    # Keeping this as the second field and defaulting source_kind to DOCUMENT
    # preserves construction and reading of the NORM-P0-004 contract.
    source_document: DocumentIdentity | None = None
    source_kind: SourceKind = SourceKind.DOCUMENT
    relation: str | None = None
    rule_id: str | None = None
    location: EvidenceLocation | None = None
    raw_evidence: str | None = None
    external_reference: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "field_name", _required_text(self.field_name, "field_name"))
        try:
            object.__setattr__(self, "source_kind", SourceKind(self.source_kind))
        except ValueError as exc:
            raise ValueError(f"unknown source_kind: {self.source_kind!r}") from exc
        if self.source_document is not None and not isinstance(self.source_document, DocumentIdentity):
            raise TypeError("source_document must be a DocumentIdentity or None")
        if self.location is not None and not isinstance(self.location, EvidenceLocation):
            raise TypeError("location must be an EvidenceLocation or None")
        for field_name in ("relation", "rule_id", "raw_evidence", "external_reference"):
            object.__setattr__(self, field_name, _optional_text(getattr(self, field_name)))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "source_kind": self.source_kind.value,
            "source_document": self.source_document.to_dict() if self.source_document is not None else None,
            "relation": self.relation,
            "rule_id": self.rule_id,
            "location": self.location.to_dict() if self.location is not None else None,
            "raw_evidence": self.raw_evidence,
            "external_reference": self.external_reference,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FieldEvidence":
        source_document = value.get("source_document")
        return cls(
            field_name=value.get("field_name", ""),
            source_document=DocumentIdentity.from_dict(source_document) if isinstance(source_document, Mapping) else None,
            source_kind=value.get("source_kind", SourceKind.DOCUMENT.value),
            relation=value.get("relation"),
            rule_id=value.get("rule_id"),
            location=EvidenceLocation.from_dict(value["location"]) if isinstance(value.get("location"), Mapping) else None,
            raw_evidence=value.get("raw_evidence"),
            external_reference=value.get("external_reference"),
        )


@dataclass(frozen=True)
class RecordGold:
    """V2 consolidated record composed from a document and field evidence."""

    process_id: str
    primary_document: DocumentGoldDecision
    field_evidence: Tuple[FieldEvidence, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "process_id", _required_text(self.process_id, "process_id"))
        if not isinstance(self.primary_document, DocumentGoldDecision):
            raise TypeError("primary_document must be a DocumentGoldDecision")
        evidence = tuple(self.field_evidence)
        if any(not isinstance(item, FieldEvidence) for item in evidence):
            raise TypeError("field_evidence items must be FieldEvidence")
        object.__setattr__(self, "field_evidence", evidence)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "process_id": self.process_id,
            "primary_document": self.primary_document.to_dict(),
            "field_evidence": [item.to_dict() for item in self.field_evidence],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RecordGold":
        return cls(
            process_id=value.get("process_id", ""),
            primary_document=DocumentGoldDecision.from_dict(value.get("primary_document", {})),
            field_evidence=tuple(FieldEvidence.from_dict(item) for item in value.get("field_evidence", ())),
        )
