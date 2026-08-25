"""Pure, deterministic legacy-versus-V2 shadow comparison."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable

from app.services.canonical_selection import CanonicalDecision
from app.services.gold_contracts import DocumentGoldDecision
from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import CanonicalState, PublicationState


SCHEMA_VERSION = "1.0"


class ComparisonState(str, Enum):
    AGREEMENT = "AGREEMENT"
    DIVERGENCE = "DIVERGENCE"
    NOT_COMPARABLE = "NOT_COMPARABLE"


class ComparisonReasonCode(str, Enum):
    GATE_AGREEMENT = "legacy_vs_v2.agree.gate"
    WINNER_AGREEMENT = "legacy_vs_v2.agree.winner"
    LEGACY_ONLY = "legacy_vs_v2.diverge.legacy_only"
    V2_ONLY = "legacy_vs_v2.diverge.v2_only"
    WINNER_CHANGED = "legacy_vs_v2.diverge.winner_changed"
    V2_UNRESOLVED = "legacy_vs_v2.diverge.v2_unresolved"
    INSUFFICIENT_INFORMATION = "legacy_vs_v2.not_comparable.insufficient_information"
    GATE_INELIGIBLE = "canonical.not_evaluated.gate_ineligible"
    PARAMETERS_UNAVAILABLE = "canonical.not_evaluated.parameters_unavailable"


@dataclass(frozen=True)
class CandidateComparisonInput:
    """Observed legacy facts plus decisions produced by the two V2 mechanisms."""

    family: str
    identity: DocumentIdentity
    legacy_document_gold: bool | None
    legacy_selected: bool | None
    gate_decision: DocumentGoldDecision
    canonical_decision: CanonicalDecision | None = None

    def __post_init__(self) -> None:
        family = " ".join(str(self.family or "").split())
        if not family:
            raise ValueError("family must be a non-empty string")
        object.__setattr__(self, "family", family)
        if not isinstance(self.identity, DocumentIdentity):
            raise TypeError("identity must be a DocumentIdentity")
        if not self.identity.process_id or not self.identity.candidate_id:
            raise ValueError("process_id and candidate_id must be present")
        for field_name in ("legacy_document_gold", "legacy_selected"):
            if getattr(self, field_name) is not None and not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool or None")
        if not isinstance(self.gate_decision, DocumentGoldDecision):
            raise TypeError("gate_decision must be a DocumentGoldDecision")
        if self.gate_decision.identity != self.identity:
            raise ValueError("gate_decision identity must match identity")
        if self.canonical_decision is not None and not isinstance(self.canonical_decision, CanonicalDecision):
            raise TypeError("canonical_decision must be a CanonicalDecision or None")


def build_comparison_report(inputs: Iterable[CandidateComparisonInput]) -> dict[str, Any]:
    """Build candidate rows and family summaries without recalculating either V2 decision."""

    items = tuple(inputs)
    if any(not isinstance(item, CandidateComparisonInput) for item in items):
        raise TypeError("inputs must contain only CandidateComparisonInput values")
    keys = [(item.family, item.identity.process_id, item.identity.candidate_id) for item in items]
    if len(keys) != len(set(keys)):
        raise ValueError("candidate_id must be unique within family and process")

    groups: dict[tuple[str, str], list[CandidateComparisonInput]] = defaultdict(list)
    for item in items:
        groups[(item.family, item.identity.process_id)].append(item)

    rows: list[dict[str, Any]] = []
    for group in groups.values():
        decisions = {serialize_canonical(item.canonical_decision) for item in group if item.canonical_decision is not None}
        if len(decisions) > 1:
            raise ValueError("a family/process cannot contain conflicting canonical decisions")
        legacy_winners = {item.identity.candidate_id for item in group if item.legacy_selected is True}
        canonical = next((item.canonical_decision for item in group if item.canonical_decision is not None), None)
        v2_winner = canonical.winner_candidate_id if canonical is not None else None
        winner_changed = canonical is not None and canonical.canonical_state is CanonicalState.SELECTED and legacy_winners != {v2_winner}
        legacy_has_winner = bool(legacy_winners)
        for item in group:
            rows.append(
                _candidate_row(
                    item,
                    canonical=canonical,
                    winner_changed=winner_changed,
                    legacy_has_winner=legacy_has_winner,
                )
            )

    rows.sort(key=lambda row: (row["family"], row["process_id"], row["candidate_id"], row["document_id"] or ""))
    return {
        "schema_version": SCHEMA_VERSION,
        "candidates": rows,
        "families": [_family_summary(family, rows) for family in sorted({row["family"] for row in rows})],
    }


def _candidate_row(
    item: CandidateComparisonInput,
    *,
    canonical: CanonicalDecision | None,
    winner_changed: bool,
    legacy_has_winner: bool,
) -> dict[str, Any]:
    gate_state = item.gate_decision.semantic_state.publication
    gate_eligible = gate_state is PublicationState.PUBLISHED
    if not gate_eligible:
        canonical_state = CanonicalState.INELIGIBLE.value
        canonical_reason = ComparisonReasonCode.GATE_INELIGIBLE.value
        v2_selected = False
    elif canonical is None:
        canonical_state = CanonicalState.NOT_EVALUATED.value
        canonical_reason = ComparisonReasonCode.PARAMETERS_UNAVAILABLE.value
        v2_selected = None
    else:
        canonical_state = canonical.canonical_state.value
        canonical_reason = canonical.reason.value
        v2_selected = canonical.winner_candidate_id == item.identity.candidate_id

    state, reason = _comparison(item, gate_eligible, canonical, v2_selected, winner_changed, legacy_has_winner)
    return {
        "family": item.family,
        "process_id": item.identity.process_id,
        "candidate_id": item.identity.candidate_id,
        "document_id": item.identity.document_id,
        "legacy_document_gold": item.legacy_document_gold,
        "legacy_selected": item.legacy_selected,
        "v2_gate_state": gate_state.value,
        "v2_gate_reason": item.gate_decision.reason_codes[0] if item.gate_decision.reason_codes else None,
        "v2_canonical_state": canonical_state,
        "v2_canonical_reason": canonical_reason,
        "v2_selected": v2_selected,
        "comparison_state": state.value,
        "comparison_reason": reason.value,
    }


def _comparison(item, gate_eligible, canonical, v2_selected, winner_changed, legacy_has_winner):
    if item.legacy_document_gold is None:
        return ComparisonState.NOT_COMPARABLE, ComparisonReasonCode.INSUFFICIENT_INFORMATION
    if item.legacy_document_gold and not gate_eligible:
        return ComparisonState.DIVERGENCE, ComparisonReasonCode.LEGACY_ONLY
    if not item.legacy_document_gold and gate_eligible:
        return ComparisonState.DIVERGENCE, ComparisonReasonCode.V2_ONLY
    if canonical is None or not gate_eligible:
        return ComparisonState.AGREEMENT, ComparisonReasonCode.GATE_AGREEMENT
    if item.legacy_selected is None:
        return ComparisonState.NOT_COMPARABLE, ComparisonReasonCode.INSUFFICIENT_INFORMATION
    if canonical.canonical_state in {CanonicalState.UNRESOLVED, CanonicalState.TIE}:
        if legacy_has_winner:
            return ComparisonState.DIVERGENCE, ComparisonReasonCode.V2_UNRESOLVED
        return ComparisonState.AGREEMENT, ComparisonReasonCode.WINNER_AGREEMENT
    if winner_changed:
        return ComparisonState.DIVERGENCE, ComparisonReasonCode.WINNER_CHANGED
    if item.legacy_selected == v2_selected:
        return ComparisonState.AGREEMENT, ComparisonReasonCode.WINNER_AGREEMENT
    return ComparisonState.DIVERGENCE, ComparisonReasonCode.WINNER_CHANGED


def _family_summary(family: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [row for row in rows if row["family"] == family]
    count = lambda reason: sum(row["comparison_reason"] == reason.value for row in selected)
    return {
        "family": family,
        "candidate_count": len(selected),
        "comparable_count": sum(row["comparison_state"] != ComparisonState.NOT_COMPARABLE.value for row in selected),
        "agreement_count": sum(row["comparison_state"] == ComparisonState.AGREEMENT.value for row in selected),
        "divergence_count": sum(row["comparison_state"] == ComparisonState.DIVERGENCE.value for row in selected),
        "legacy_only_count": count(ComparisonReasonCode.LEGACY_ONLY),
        "v2_only_count": count(ComparisonReasonCode.V2_ONLY),
        "winner_changed_count": count(ComparisonReasonCode.WINNER_CHANGED),
        "v2_unresolved_count": count(ComparisonReasonCode.V2_UNRESOLVED),
    }


def serialize_canonical(decision: CanonicalDecision | None) -> str:
    return json.dumps(decision.to_dict() if decision is not None else None, sort_keys=True, separators=(",", ":"))


def serialize_comparison_report(report: dict[str, Any]) -> str:
    """Serialize stable report bytes without runtime metadata."""

    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def comparison_input_from_dict(value: dict[str, Any]) -> CandidateComparisonInput:
    """Read an explicit decision envelope without inferring missing legacy facts."""

    canonical = value.get("canonical_decision")
    return CandidateComparisonInput(
        family=value.get("family", ""),
        identity=DocumentIdentity.from_dict(value.get("identity", {})),
        legacy_document_gold=value.get("legacy_document_gold"),
        legacy_selected=value.get("legacy_selected"),
        gate_decision=DocumentGoldDecision.from_dict(value.get("gate_decision", {})),
        canonical_decision=CanonicalDecision.from_dict(canonical) if isinstance(canonical, dict) else None,
    )
