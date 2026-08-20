from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional


class ClassificationState(str, Enum):
    NOT_CLASSIFIED = "NOT_CLASSIFIED"
    CANDIDATE = "CANDIDATE"
    CONFIRMED = "CONFIRMED"
    RELATED = "RELATED"
    REJECTED = "REJECTED"
    AMBIGUOUS = "AMBIGUOUS"


class DocumentFunctionState(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    INSTRUMENT = "INSTRUMENT"
    RELATED = "RELATED"
    AMBIGUOUS = "AMBIGUOUS"
    INELIGIBLE = "INELIGIBLE"


class AffinityState(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    MATCHED = "MATCHED"
    MISMATCHED = "MISMATCHED"
    AMBIGUOUS = "AMBIGUOUS"


class CanonicalState(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    SELECTED = "SELECTED"
    UNRESOLVED = "UNRESOLVED"
    TIE = "TIE"
    INELIGIBLE = "INELIGIBLE"


class PublicationState(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    RETAINED = "RETAINED"
    PUBLISHED = "PUBLISHED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class SemanticState:
    """Additive V2 contract for independent semantic decisions.

    ``resolved_class`` and ``resolved_function`` deliberately remain optional
    strings so later family-specific contracts can supply their own taxonomy.
    """

    classification: ClassificationState
    function: DocumentFunctionState
    affinity: AffinityState
    canonical: CanonicalState
    publication: PublicationState
    resolved_class: Optional[str] = None
    resolved_function: Optional[str] = None

    def __post_init__(self) -> None:
        for field_name, enum_type in (
            ("classification", ClassificationState),
            ("function", DocumentFunctionState),
            ("affinity", AffinityState),
            ("canonical", CanonicalState),
            ("publication", PublicationState),
        ):
            if not isinstance(getattr(self, field_name), enum_type):
                raise TypeError(f"{field_name} must be a {enum_type.__name__}")

        for field_name in ("resolved_class", "resolved_function"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{field_name} must be a non-empty string or None")

        if self.classification is ClassificationState.REJECTED and self.canonical is CanonicalState.SELECTED:
            raise ValueError("canonical=SELECTED is incompatible with classification=REJECTED")

        if self.canonical is CanonicalState.INELIGIBLE and self.publication is PublicationState.PUBLISHED:
            raise ValueError("publication=PUBLISHED is incompatible with canonical=INELIGIBLE")

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "classification": self.classification.value,
            "resolved_class": self.resolved_class,
            "function": self.function.value,
            "resolved_function": self.resolved_function,
            "affinity": self.affinity.value,
            "canonical": self.canonical.value,
            "publication": self.publication.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SemanticState":
        return cls(
            classification=ClassificationState(value.get("classification")),
            resolved_class=value.get("resolved_class"),
            function=DocumentFunctionState(value.get("function")),
            resolved_function=value.get("resolved_function"),
            affinity=AffinityState(value.get("affinity")),
            canonical=CanonicalState(value.get("canonical")),
            publication=PublicationState(value.get("publication")),
        )
