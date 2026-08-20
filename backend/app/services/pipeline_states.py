from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping


class DiscoveryState(str, Enum):
    NOT_SEARCHED = "NOT_SEARCHED"
    NOT_FOUND = "NOT_FOUND"
    FOUND = "FOUND"


class OpeningState(str, Enum):
    NOT_ATTEMPTED = "NOT_ATTEMPTED"
    OPENED = "OPENED"
    OPEN_FAILED = "OPEN_FAILED"
    TIMEOUT = "TIMEOUT"


class AccessState(str, Enum):
    UNKNOWN = "UNKNOWN"
    ACCESSIBLE = "ACCESSIBLE"
    ACCESS_RESTRICTED = "ACCESS_RESTRICTED"
    IFRAME_UNAVAILABLE = "IFRAME_UNAVAILABLE"


class ExtractionState(str, Enum):
    NOT_ATTEMPTED = "NOT_ATTEMPTED"
    EXTRACTED = "EXTRACTED"
    CONTENT_PARTIAL = "CONTENT_PARTIAL"
    EMPTY_CONTENT = "EMPTY_CONTENT"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"


_SUCCESSFUL_EXTRACTION_STATES = {
    ExtractionState.EXTRACTED,
    ExtractionState.CONTENT_PARTIAL,
    ExtractionState.EMPTY_CONTENT,
}


@dataclass(frozen=True)
class AcquisitionState:
    """Additive V2 contract for independent technical acquisition facts."""

    discovery: DiscoveryState
    opening: OpeningState
    access: AccessState
    extraction: ExtractionState

    def __post_init__(self) -> None:
        for field_name, enum_type in (
            ("discovery", DiscoveryState),
            ("opening", OpeningState),
            ("access", AccessState),
            ("extraction", ExtractionState),
        ):
            value = getattr(self, field_name)
            if not isinstance(value, enum_type):
                raise TypeError(f"{field_name} must be a {enum_type.__name__}")

        if self.discovery is not DiscoveryState.FOUND and self.opening is not OpeningState.NOT_ATTEMPTED:
            raise ValueError("opening requires discovery=FOUND")

        if self.opening is not OpeningState.OPENED and self.access is not AccessState.UNKNOWN:
            raise ValueError("confirmed access requires opening=OPENED")

        if self.extraction is not ExtractionState.NOT_ATTEMPTED and self.discovery is not DiscoveryState.FOUND:
            raise ValueError("extraction attempt requires discovery=FOUND")

        if self.extraction in _SUCCESSFUL_EXTRACTION_STATES and self.access is not AccessState.ACCESSIBLE:
            raise ValueError("successful extraction requires access=ACCESSIBLE")

    def to_dict(self) -> Dict[str, str]:
        return {
            "discovery": self.discovery.value,
            "opening": self.opening.value,
            "access": self.access.value,
            "extraction": self.extraction.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AcquisitionState":
        return cls(
            discovery=DiscoveryState(value.get("discovery")),
            opening=OpeningState(value.get("opening")),
            access=AccessState(value.get("access")),
            extraction=ExtractionState(value.get("extraction")),
        )
