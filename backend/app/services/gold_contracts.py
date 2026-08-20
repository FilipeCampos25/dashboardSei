from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Tuple

from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import SemanticState


def _required_text(value: Any, field_name: str) -> str:
    cleaned = " ".join(str(value or "").replace("\r", "\n").split()).strip()
    if not cleaned:
        raise ValueError(f"{field_name} must be a non-empty string")
    return cleaned


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
    """Minimal V2 link between a field and its source document.

    Evidence content, location, relation, derivation and confidence deliberately
    belong to PROV-P0-001.
    """

    field_name: str
    source_document: DocumentIdentity

    def __post_init__(self) -> None:
        object.__setattr__(self, "field_name", _required_text(self.field_name, "field_name"))
        if not isinstance(self.source_document, DocumentIdentity):
            raise TypeError("source_document must be a DocumentIdentity")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "source_document": self.source_document.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FieldEvidence":
        return cls(
            field_name=value.get("field_name", ""),
            source_document=DocumentIdentity.from_dict(value.get("source_document", {})),
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
