"""Pure ACT field consolidation performed after canonical selection."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from typing import Any, Callable, Mapping, Sequence

from app.services.act_field_policy import may_complement_act
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.semantic_states import CanonicalState, SemanticState


CONSOLIDATION_RULE = "act.field_consolidation.authorized_complement"
RELATION_PREFIX = "act.same_process_complement"
VIGENCIA_RELATED_PUBLICATION_RULE = "act.vigencia.related_publication"
VIGENCIA_FIELDS = (
    "vigencia_inicio",
    "vigencia_fim",
    "data_inicio_vigencia",
    "data_fim_vigencia",
)


def _identity_key(identity: DocumentIdentity) -> tuple[str, str, str, str]:
    return (identity.process_id, identity.document_id or "", identity.candidate_id or "", identity.source_url or "")


def _distinct_identified_document(primary: DocumentIdentity, source: DocumentIdentity) -> bool:
    if not source.process_id or source.process_id != primary.process_id:
        return False
    if not (source.document_id or source.candidate_id):
        return False
    if source.document_id and primary.document_id and source.document_id == primary.document_id:
        return False
    if source.candidate_id and primary.candidate_id and source.candidate_id == primary.candidate_id:
        return False
    return True


def _related_evidence(evidence: FieldEvidence, source: DocumentIdentity, source_function: str) -> FieldEvidence:
    return replace(
        evidence,
        source_kind=SourceKind.RELATED_DOCUMENT,
        source_document=source,
        relation=f"{RELATION_PREFIX}:{source_function}",
    )


def _sorted_evidence(evidences: Sequence[FieldEvidence]) -> tuple[FieldEvidence, ...]:
    unique: dict[str, FieldEvidence] = {}
    for evidence in evidences:
        key = json.dumps(evidence.to_dict(), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        unique[key] = evidence
    return tuple(unique[key] for key in sorted(unique))


def _field_map(record: Mapping[str, Any]) -> dict[str, FieldResult]:
    return {
        result.field_name: result
        for item in record.get("fields", ())
        if isinstance(item, Mapping)
        for result in (FieldResult.from_dict(item),)
    }


def _vigencia_evidences(
    field_name: str,
    clause: FieldResult,
    publication: FieldResult,
) -> tuple[FieldEvidence, ...]:
    clause_evidence = tuple(
        replace(
            item,
            field_name=field_name,
            source_kind=SourceKind.DERIVED,
            rule_id=VIGENCIA_RELATED_PUBLICATION_RULE,
        )
        for item in clause.evidences
    )
    publication_evidence = tuple(replace(item, field_name=field_name) for item in publication.evidences)
    return _sorted_evidence(clause_evidence + publication_evidence)


def _resolve_publication_vigencia(
    primary: dict[str, Any],
    resolved: dict[str, FieldResult],
    resolver: Callable[..., Mapping[str, str]],
) -> None:
    clause = resolved.get("vigencia_raw")
    publication = resolved.get("data_publicacao")
    if clause is None or clause.state is not FieldState.PRESENT or publication is None:
        return

    signature = resolved.get("data_assinatura")
    signature_value = str(signature.value) if signature and signature.state is FieldState.PRESENT else ""
    publication_value = str(publication.value) if publication.state is FieldState.PRESENT else ""
    temporal = resolver(
        str(clause.value),
        data_assinatura=signature_value,
        data_publicacao=publication_value,
    )
    if temporal.get("anchor") != "publicacao":
        return

    if publication.state is FieldState.PRESENT and temporal.get("vigencia_inicio"):
        state = FieldState.PRESENT
        reason = "resolved_from_related_publication" if any(
            item.source_kind is SourceKind.RELATED_DOCUMENT for item in publication.evidences
        ) else "resolved_from_publication"
    else:
        state = FieldState.UNRESOLVED
        if publication.state is FieldState.CONFLICT:
            reason = "publication_conflict"
        elif publication.state is FieldState.PRESENT:
            reason = "publication_invalid_for_clause"
        else:
            reason = "publication_missing"

    values = {
        "vigencia_inicio": temporal.get("vigencia_inicio"),
        "vigencia_fim": temporal.get("vigencia_fim"),
        "data_inicio_vigencia": temporal.get("vigencia_inicio"),
        "data_fim_vigencia": temporal.get("vigencia_fim"),
    }
    for field_name in VIGENCIA_FIELDS:
        value = values[field_name]
        field_state = state if state is FieldState.UNRESOLVED or value else FieldState.UNRESOLVED
        resolved[field_name] = FieldResult(
            field_name=field_name,
            state=field_state,
            value=value if field_state is FieldState.PRESENT else None,
            evidences=_vigencia_evidences(field_name, clause, publication),
        )
    primary["act_vigencia_resolution"] = {
        "rule_id": VIGENCIA_RELATED_PUBLICATION_RULE,
        "anchor": "publicacao",
        "state": state.value,
        "reason": reason,
        "amount": temporal.get("amount", ""),
        "unit": temporal.get("unit", ""),
        "warning": temporal.get("warning", ""),
    }


def consolidate_act_fields(
    records: Sequence[Mapping[str, Any]],
    *,
    vigencia_resolver: Callable[..., Mapping[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Add authorized related evidence without selecting or promoting documents."""

    consolidated = [copy.deepcopy(dict(record)) for record in records]
    by_process: dict[str, list[dict[str, Any]]] = {}
    for record in consolidated:
        identity = DocumentIdentity.from_dict(record.get("identity", {}))
        if identity.process_id:
            by_process.setdefault(identity.process_id, []).append(record)

    for process_records in by_process.values():
        winners = [
            record
            for record in process_records
            if SemanticState.from_dict(record["semantic_state"]).canonical is CanonicalState.SELECTED
        ]
        if len(winners) != 1:
            continue
        primary = winners[0]
        primary_identity = DocumentIdentity.from_dict(primary["identity"])
        resolved = _field_map(primary)
        contributions: dict[str, list[tuple[str, DocumentIdentity, FieldResult]]] = {}

        for source_record in process_records:
            if source_record is primary:
                continue
            source_identity = DocumentIdentity.from_dict(source_record.get("identity", {}))
            if not _distinct_identified_document(primary_identity, source_identity):
                continue
            source_function = SemanticState.from_dict(source_record["semantic_state"]).resolved_function
            for field_name, field in _field_map(source_record).items():
                if (
                    source_function
                    and may_complement_act(source_function, field_name)
                    and field.state is FieldState.PRESENT
                    and field.evidences
                ):
                    contributions.setdefault(field_name, []).append((source_function, source_identity, field))

        audit: dict[str, Any] = {}
        for field_name in sorted(contributions):
            current = resolved.get(field_name)
            if current is None or current.state in {
                FieldState.NOT_APPLICABLE,
                FieldState.CONFLICT,
                FieldState.UNRESOLVED,
                FieldState.INACCESSIBLE,
                FieldState.EXTRACTION_FAILED,
            }:
                continue
            candidates: list[tuple[str, DocumentIdentity, Any, tuple[FieldEvidence, ...]]] = []
            if current.state is FieldState.PRESENT:
                candidates.append(("act.instrument", primary_identity, current.value, current.evidences))
            for function, identity, field in contributions[field_name]:
                evidences = tuple(_related_evidence(item, identity, function) for item in field.evidences)
                candidates.append((function, identity, field.value, evidences))
            if not candidates:
                continue

            candidates.sort(key=lambda item: (str(item[2]), item[0], _identity_key(item[1])))
            values = sorted({str(item[2]) for item in candidates})
            evidences = _sorted_evidence(tuple(evidence for item in candidates for evidence in item[3]))
            state = FieldState.PRESENT if len(values) == 1 else FieldState.CONFLICT
            resolved[field_name] = FieldResult(
                field_name=field_name,
                state=state,
                value=candidates[0][2] if state is FieldState.PRESENT else None,
                evidences=evidences,
            )
            audit[field_name] = {
                "rule_id": CONSOLIDATION_RULE,
                "state": state.value,
                "values": values,
                "sources": [
                    {"function": function, "identity": identity.to_dict()}
                    for function, identity, _, _ in candidates
                ],
            }

        if audit:
            primary["act_field_consolidation"] = audit
        if vigencia_resolver is not None:
            _resolve_publication_vigencia(primary, resolved, vigencia_resolver)
        primary["fields"] = [resolved[name].to_dict() for name in sorted(resolved)]
    return consolidated
