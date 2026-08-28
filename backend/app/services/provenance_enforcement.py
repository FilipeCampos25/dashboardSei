"""Progressive enforcement for the existing provenance invariant."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence

from app.services.field_states import FieldResult
from app.services.provenance_validation import ProvenanceViolation, validate_field_provenance


class ProvenanceEnforcementMode(str, Enum):
    OFF = "off"
    WARN = "warn"
    ERROR = "error"


_MESSAGES = {
    "missing_evidence": "published field has no provenance evidence",
    "missing_document_reference": "document evidence has no auditable document reference",
    "missing_relation": "related-document evidence has no relation",
    "missing_external_reference": "external evidence has no external reference",
    "missing_derivation_rule": "derived evidence has no derivation rule",
}


@dataclass(frozen=True)
class ProvenanceEnforcementReport:
    mode: ProvenanceEnforcementMode
    family: str
    records_checked: int
    fields_checked: int
    violations: tuple[dict[str, Any], ...]

    @property
    def is_valid(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, Any]:
        by_field: dict[str, int] = {}
        by_code: dict[str, int] = {}
        for item in self.violations:
            by_field[item["field_name"]] = by_field.get(item["field_name"], 0) + 1
            by_code[item["code"]] = by_code.get(item["code"], 0) + 1
        return {
            "mode": self.mode.value,
            "family": self.family,
            "records_checked": self.records_checked,
            "fields_checked": self.fields_checked,
            "violation_count": len(self.violations),
            "counts_by_field": dict(sorted(by_field.items())),
            "counts_by_code": dict(sorted(by_code.items())),
            "violations": list(self.violations),
        }


class ProvenanceContractError(RuntimeError):
    """Strict-mode failure carrying the complete deterministic report."""

    def __init__(self, report: ProvenanceEnforcementReport) -> None:
        self.report = report
        super().__init__(
            f"provenance contract violated: family={report.family} "
            f"violations={len(report.violations)}"
        )


def enforce_provenance(
    records: Sequence[Mapping[str, Any]],
    *,
    family: str,
    mode: ProvenanceEnforcementMode | str,
) -> ProvenanceEnforcementReport:
    """Validate serialized V2 records without changing their values or states."""

    resolved_mode = ProvenanceEnforcementMode(mode)
    sanitized_family = str(family).strip() or "unknown"
    violations: list[dict[str, Any]] = []
    fields_checked = 0
    for record_index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise TypeError(f"V2 record at index {record_index} must be a mapping")
        identity = record.get("identity") if isinstance(record.get("identity"), Mapping) else {}
        record_id = _record_id(identity, record_index)
        fields = record.get("fields", ())
        if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
            raise TypeError(f"V2 fields at record index {record_index} must be a sequence")
        for field_index, field_payload in enumerate(fields):
            if not isinstance(field_payload, Mapping):
                raise TypeError(
                    f"V2 field at record index {record_index}, field index {field_index} "
                    "must be a mapping"
                )
            field = FieldResult.from_dict(field_payload)
            fields_checked += 1
            report = validate_field_provenance(field)
            for violation in report.violations:
                violations.append(
                    _violation_dict(
                        violation,
                        field_payload,
                        family=sanitized_family,
                        record_index=record_index,
                        record_id=record_id,
                        state=field.state.value,
                        mode=resolved_mode,
                    )
                )
    result = ProvenanceEnforcementReport(
        mode=resolved_mode,
        family=sanitized_family,
        records_checked=len(records),
        fields_checked=fields_checked,
        violations=tuple(violations),
    )
    if resolved_mode is ProvenanceEnforcementMode.ERROR and not result.is_valid:
        raise ProvenanceContractError(result)
    return result


def _record_id(identity: Mapping[str, Any], record_index: int) -> str:
    for key in ("document_id", "candidate_id", "process_id"):
        value = str(identity.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return f"record_index:{record_index}"


def _violation_dict(
    violation: ProvenanceViolation,
    field_payload: Mapping[str, Any],
    *,
    family: str,
    record_index: int,
    record_id: str,
    state: str,
    mode: ProvenanceEnforcementMode,
) -> dict[str, Any]:
    source_kind = None
    evidences = field_payload.get("evidences", ())
    if violation.evidence_index is not None and isinstance(evidences, Sequence):
        indexed_evidence = (
            evidences[violation.evidence_index]
            if violation.evidence_index < len(evidences)
            else None
        )
        if isinstance(indexed_evidence, Mapping):
            source_kind = indexed_evidence.get("source_kind")
    return {
        "family": family,
        "record_index": record_index,
        "record_id": record_id,
        "field_name": violation.field_name,
        "state": state,
        "code": violation.code.value,
        "kind": violation.kind.value,
        "message": _MESSAGES[violation.code.value],
        "source_kind": source_kind,
        "evidence_index": violation.evidence_index,
        "mode": mode.value,
    }
