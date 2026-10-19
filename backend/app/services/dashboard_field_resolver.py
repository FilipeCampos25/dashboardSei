"""Pure, auditable field resolution for dashboard consolidation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence


PRIMARY = "same_process_primary"
COMPLEMENT = "same_process_complement"
PREVIEW = "preview"


@dataclass(frozen=True)
class FieldCandidate:
    field_name: str
    value: Any
    source: str
    function: str
    relation: str
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolutionPolicy:
    field_name: str
    precedence: tuple[tuple[str, str], ...]
    normalize: Callable[[Any], str] = lambda value: " ".join(str(value or "").split()).casefold()


@dataclass(frozen=True)
class FieldResolution:
    field_name: str
    value: Any | None
    winner: FieldCandidate | None
    conflicts: tuple[FieldCandidate, ...]
    considered: tuple[FieldCandidate, ...]
    rule_id: str

    @property
    def state(self) -> str:
        if self.winner is None:
            return "MISSING"
        return "CONFLICT" if self.conflicts else "PRESENT"


def resolve_field(policy: ResolutionPolicy, candidates: Sequence[FieldCandidate]) -> FieldResolution:
    """Resolve one field by explicit function/relation precedence and retain conflicts."""

    ranks = {key: index for index, key in enumerate(policy.precedence)}
    eligible = [
        candidate
        for candidate in candidates
        if candidate.field_name == policy.field_name
        and candidate.value not in (None, "")
        and (candidate.function, candidate.relation) in ranks
    ]
    eligible.sort(
        key=lambda candidate: (
            ranks[(candidate.function, candidate.relation)],
            candidate.source,
            policy.normalize(candidate.value),
        )
    )
    winner = eligible[0] if eligible else None
    winner_key = policy.normalize(winner.value) if winner else ""
    conflicts = tuple(
        candidate for candidate in eligible[1:] if policy.normalize(candidate.value) != winner_key
    )
    return FieldResolution(
        field_name=policy.field_name,
        value=winner.value if winner else None,
        winner=winner,
        conflicts=conflicts,
        considered=tuple(eligible),
        rule_id=f"dashboard.field_resolution.{policy.field_name}.v1",
    )
