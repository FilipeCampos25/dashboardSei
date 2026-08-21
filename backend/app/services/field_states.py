from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Tuple

from app.services.gold_contracts import FieldEvidence


class FieldState(str, Enum):
    """Universal V2 condition of a normalized field."""

    NOT_EVALUATED = "NOT_EVALUATED"
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    EXPECTED_ELSEWHERE = "EXPECTED_ELSEWHERE"
    CONFLICT = "CONFLICT"
    INACCESSIBLE = "INACCESSIBLE"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True)
class FieldResult:
    """Explicit V2 result for a field, kept separate from provenance."""

    field_name: str
    state: FieldState
    value: Any = None
    evidences: Tuple[FieldEvidence, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.field_name, str) or not self.field_name.strip():
            raise ValueError("field_name must be a non-empty string")
        object.__setattr__(self, "field_name", self.field_name.strip())

        if not isinstance(self.state, FieldState):
            raise TypeError("state must be a FieldState")

        evidences = tuple(self.evidences)
        if any(not isinstance(item, FieldEvidence) for item in evidences):
            raise TypeError("evidences items must be FieldEvidence")
        if any(item.field_name != self.field_name for item in evidences):
            raise ValueError("evidence field_name must match the result field_name")
        object.__setattr__(self, "evidences", evidences)

        if self.state is FieldState.PRESENT:
            if self.value is None or (isinstance(self.value, str) and not self.value.strip()):
                raise ValueError("state=PRESENT requires a resolved non-empty value")
        elif self.value is not None:
            raise ValueError(f"state={self.state.value} cannot carry a resolved value")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "state": self.state.value,
            "value": self.value,
            "evidences": [item.to_dict() for item in self.evidences],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FieldResult":
        return cls(
            field_name=value.get("field_name", ""),
            state=FieldState(value.get("state")),
            value=value.get("value"),
            evidences=tuple(FieldEvidence.from_dict(item) for item in value.get("evidences", ())),
        )
