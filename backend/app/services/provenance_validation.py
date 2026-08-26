"""Pure validation of auditability for resolved V2 fields.

This module reports contract violations only.  It is deliberately not wired to
exporters or family normalizers; enforcement policy belongs to a later phase.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import FieldEvidence, SourceKind


class ProvenanceViolationCode(str, Enum):
    MISSING_EVIDENCE = "missing_evidence"
    MISSING_DOCUMENT_REFERENCE = "missing_document_reference"
    MISSING_RELATION = "missing_relation"
    MISSING_EXTERNAL_REFERENCE = "missing_external_reference"
    MISSING_DERIVATION_RULE = "missing_derivation_rule"


class ProvenanceViolationKind(str, Enum):
    """Whether a violation is absent audit data or a contradictory value."""

    INCOMPLETE = "incomplete"
    INVALID = "invalid"


@dataclass(frozen=True)
class ProvenanceViolation:
    field_name: str
    code: ProvenanceViolationCode
    kind: ProvenanceViolationKind
    evidence_index: int | None = None


@dataclass(frozen=True)
class ProvenanceValidationReport:
    field_name: str
    state: FieldState
    is_published: bool
    violations: Tuple[ProvenanceViolation, ...] = ()

    @property
    def is_valid(self) -> bool:
        return not self.violations

    @property
    def codes(self) -> Tuple[ProvenanceViolationCode, ...]:
        return tuple(violation.code for violation in self.violations)


def validate_field_provenance(field: FieldResult) -> ProvenanceValidationReport:
    """Return provenance violations for a resolved/published V2 field.

    ``PRESENT`` is the existing V2 state that carries a resolved value. Other
    states have no winner and therefore are outside this published-field
    invariant. Optional evidence location is never required or synthesized.
    """

    if not isinstance(field, FieldResult):
        raise TypeError("field must be a FieldResult")

    is_published = field.state is FieldState.PRESENT
    if not is_published:
        return ProvenanceValidationReport(field.field_name, field.state, False)

    if not field.evidences:
        return ProvenanceValidationReport(
            field.field_name,
            field.state,
            True,
            (_incomplete(field.field_name, ProvenanceViolationCode.MISSING_EVIDENCE),),
        )

    violations = tuple(
        violation
        for index, evidence in enumerate(field.evidences)
        for violation in _validate_evidence(evidence, index)
    )
    return ProvenanceValidationReport(field.field_name, field.state, True, violations)


def _validate_evidence(evidence: FieldEvidence, index: int) -> Tuple[ProvenanceViolation, ...]:
    missing: list[ProvenanceViolationCode] = []
    if evidence.source_kind in (SourceKind.DOCUMENT, SourceKind.RELATED_DOCUMENT):
        if not _has_document_reference(evidence):
            missing.append(ProvenanceViolationCode.MISSING_DOCUMENT_REFERENCE)
    if evidence.source_kind is SourceKind.RELATED_DOCUMENT and evidence.relation is None:
        missing.append(ProvenanceViolationCode.MISSING_RELATION)
    if evidence.source_kind is SourceKind.EXTERNAL and evidence.external_reference is None:
        missing.append(ProvenanceViolationCode.MISSING_EXTERNAL_REFERENCE)
    if evidence.source_kind is SourceKind.DERIVED and evidence.rule_id is None:
        missing.append(ProvenanceViolationCode.MISSING_DERIVATION_RULE)
    return tuple(_incomplete(evidence.field_name, code, index) for code in missing)


def _has_document_reference(evidence: FieldEvidence) -> bool:
    document = evidence.source_document
    return document is not None and any((document.document_id, document.candidate_id, document.source_url))


def _incomplete(
    field_name: str,
    code: ProvenanceViolationCode,
    evidence_index: int | None = None,
) -> ProvenanceViolation:
    return ProvenanceViolation(
        field_name=field_name,
        code=code,
        kind=ProvenanceViolationKind.INCOMPLETE,
        evidence_index=evidence_index,
    )
