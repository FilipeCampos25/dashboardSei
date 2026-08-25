from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from numbers import Real
from typing import Any, Dict, Iterable, Mapping

from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import CanonicalState


class CanonicalReasonCode(str, Enum):
    """Stable outcomes of the family-independent canonical selector."""

    NO_CANDIDATE = "canonical.unresolved.no_candidate"
    BELOW_THRESHOLD = "canonical.unresolved.below_threshold"
    TIE = "canonical.unresolved.tie"
    INSUFFICIENT_MARGIN = "canonical.unresolved.insufficient_margin"
    SELECTED = "canonical.selected"


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field_name} must be a finite real number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be a finite real number")
    return normalized


@dataclass(frozen=True)
class CanonicalCandidate:
    """An identified candidate already scored by its family policy."""

    identity: DocumentIdentity
    score: float

    def __post_init__(self) -> None:
        if not isinstance(self.identity, DocumentIdentity):
            raise TypeError("identity must be a DocumentIdentity")
        if not self.identity.candidate_id:
            raise ValueError("identity.candidate_id must be present")
        object.__setattr__(self, "score", _finite_number(self.score, "score"))


@dataclass(frozen=True)
class CanonicalDecision:
    """Serializable shadow decision; it does not carry publication state."""

    canonical_state: CanonicalState
    reason: CanonicalReasonCode
    threshold: float
    min_margin: float
    winner_identity: DocumentIdentity | None = None
    top_score: float | None = None
    runner_up_score: float | None = None
    observed_margin: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.canonical_state, CanonicalState):
            raise TypeError("canonical_state must be a CanonicalState")
        if not isinstance(self.reason, CanonicalReasonCode):
            raise TypeError("reason must be a CanonicalReasonCode")
        object.__setattr__(self, "threshold", _finite_number(self.threshold, "threshold"))
        margin = _finite_number(self.min_margin, "min_margin")
        if margin < 0:
            raise ValueError("min_margin must be greater than or equal to zero")
        object.__setattr__(self, "min_margin", margin)
        for field_name in ("top_score", "runner_up_score", "observed_margin"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_number(value, field_name))
        if self.winner_identity is not None and not isinstance(self.winner_identity, DocumentIdentity):
            raise TypeError("winner_identity must be a DocumentIdentity or None")
        selected = self.canonical_state is CanonicalState.SELECTED
        if selected != (self.winner_identity is not None):
            raise ValueError("winner_identity must be present exactly when canonical_state=SELECTED")

    @property
    def winner_candidate_id(self) -> str | None:
        return self.winner_identity.candidate_id if self.winner_identity is not None else None

    @property
    def winner_score(self) -> float | None:
        return self.top_score if self.winner_identity is not None else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "canonical_state": self.canonical_state.value,
            "reason": self.reason.value,
            "threshold": self.threshold,
            "min_margin": self.min_margin,
            "winner_identity": self.winner_identity.to_dict() if self.winner_identity is not None else None,
            "winner_candidate_id": self.winner_candidate_id,
            "winner_score": self.winner_score,
            "top_score": self.top_score,
            "runner_up_score": self.runner_up_score,
            "observed_margin": self.observed_margin,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CanonicalDecision":
        winner = value.get("winner_identity")
        return cls(
            canonical_state=CanonicalState(value.get("canonical_state")),
            reason=CanonicalReasonCode(value.get("reason")),
            threshold=value.get("threshold"),
            min_margin=value.get("min_margin"),
            winner_identity=DocumentIdentity.from_dict(winner) if isinstance(winner, Mapping) else None,
            top_score=value.get("top_score"),
            runner_up_score=value.get("runner_up_score"),
            observed_margin=value.get("observed_margin"),
        )


def select_canonical(
    candidates: Iterable[CanonicalCandidate], *, threshold: float, min_margin: float
) -> CanonicalDecision:
    """Return zero or one winner using inclusive floor and margin boundaries.

    A sole candidate that reaches the floor wins without a margin comparison.
    With multiple candidates, an exact top-score tie always abstains; otherwise
    the top-to-runner-up difference must be at least ``min_margin``.
    """

    floor = _finite_number(threshold, "threshold")
    margin_required = _finite_number(min_margin, "min_margin")
    if margin_required < 0:
        raise ValueError("min_margin must be greater than or equal to zero")

    evaluated = tuple(candidates)
    if any(not isinstance(candidate, CanonicalCandidate) for candidate in evaluated):
        raise TypeError("candidates must contain only CanonicalCandidate values")
    candidate_ids = [candidate.identity.candidate_id for candidate in evaluated]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidate_id values must be unique")
    if not evaluated:
        return _decision(CanonicalState.UNRESOLVED, CanonicalReasonCode.NO_CANDIDATE, floor, margin_required)

    ranked = sorted(evaluated, key=lambda candidate: candidate.score, reverse=True)
    top = ranked[0]
    if top.score < floor:
        return _decision(
            CanonicalState.UNRESOLVED, CanonicalReasonCode.BELOW_THRESHOLD, floor, margin_required, top_score=top.score
        )
    if len(ranked) == 1:
        return _decision(
            CanonicalState.SELECTED,
            CanonicalReasonCode.SELECTED,
            floor,
            margin_required,
            winner_identity=top.identity,
            top_score=top.score,
        )

    runner_up = ranked[1]
    observed_margin = top.score - runner_up.score
    if observed_margin == 0:
        return _decision(
            CanonicalState.TIE,
            CanonicalReasonCode.TIE,
            floor,
            margin_required,
            top_score=top.score,
            runner_up_score=runner_up.score,
            observed_margin=observed_margin,
        )
    if observed_margin < margin_required:
        return _decision(
            CanonicalState.UNRESOLVED,
            CanonicalReasonCode.INSUFFICIENT_MARGIN,
            floor,
            margin_required,
            top_score=top.score,
            runner_up_score=runner_up.score,
            observed_margin=observed_margin,
        )
    return _decision(
        CanonicalState.SELECTED,
        CanonicalReasonCode.SELECTED,
        floor,
        margin_required,
        winner_identity=top.identity,
        top_score=top.score,
        runner_up_score=runner_up.score,
        observed_margin=observed_margin,
    )


def _decision(
    state: CanonicalState,
    reason: CanonicalReasonCode,
    threshold: float,
    min_margin: float,
    **values: Any,
) -> CanonicalDecision:
    return CanonicalDecision(
        canonical_state=state,
        reason=reason,
        threshold=threshold,
        min_margin=min_margin,
        **values,
    )
