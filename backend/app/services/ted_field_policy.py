"""Conservative TED field applicability policy keyed by the audited document function."""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

from app.services.field_states import FieldState
from app.services.ted_classifier import (
    TED_FUNCTION_AMENDMENT,
    TED_FUNCTION_EXTRACT,
    TED_FUNCTION_INSTRUMENT,
    TED_FUNCTION_MEETING_MINUTES,
    TED_FUNCTION_RELATED,
    TED_FUNCTION_WORK_PLAN,
)


class RequirementPolicy(str, Enum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    EXPECTED_ELSEWHERE = "EXPECTED_ELSEWHERE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_EVALUATED = "NOT_EVALUATED"


TED_BUSINESS_FIELDS = (
    "numero_ted",
    "ano_ted",
    "objeto",
    "unidade_descentralizadora",
    "unidade_descentralizada",
    "valor_global",
    "data_assinatura",
    "datas_assinatura",
    "vigencia_inicio",
    "vigencia_fim",
    "vigencia_prazo_quantidade",
    "vigencia_prazo_unidade",
    "plano_aplicacao",
    "cronograma_desembolso",
    "metas",
    "prestacao_contas",
)

_INSTRUMENT_REQUIRED = frozenset({
    "numero_ted", "ano_ted", "objeto", "unidade_descentralizadora",
    "unidade_descentralizada", "valor_global", "vigencia_inicio", "vigencia_fim",
})
_PLAN_SECTIONS = frozenset({"plano_aplicacao", "cronograma_desembolso", "metas"})
_COMPLEMENT_SECTIONS = _PLAN_SECTIONS | {"prestacao_contas"}


def field_policy_for_function(resolved_function: str | None, field_name: str) -> RequirementPolicy:
    """Return only rules supported by current extractors/fixtures; abstain otherwise."""

    if field_name not in TED_BUSINESS_FIELDS:
        return RequirementPolicy.NOT_EVALUATED
    if resolved_function == TED_FUNCTION_INSTRUMENT:
        if field_name in _INSTRUMENT_REQUIRED:
            return RequirementPolicy.REQUIRED
        if field_name in _COMPLEMENT_SECTIONS:
            return RequirementPolicy.EXPECTED_ELSEWHERE
        return RequirementPolicy.OPTIONAL
    if resolved_function == TED_FUNCTION_WORK_PLAN:
        if field_name in _PLAN_SECTIONS:
            return RequirementPolicy.REQUIRED
        if field_name in _INSTRUMENT_REQUIRED:
            return RequirementPolicy.EXPECTED_ELSEWHERE
        return RequirementPolicy.OPTIONAL
    if resolved_function == TED_FUNCTION_MEETING_MINUTES:
        return RequirementPolicy.NOT_APPLICABLE
    if resolved_function in {TED_FUNCTION_AMENDMENT, TED_FUNCTION_EXTRACT, TED_FUNCTION_RELATED}:
        return RequirementPolicy.NOT_EVALUATED
    return RequirementPolicy.NOT_EVALUATED


def may_complement_instrument(source_function: str | None, field_name: str) -> bool:
    """Allow only a field expected on the instrument and required by its source function."""

    return (
        field_policy_for_function(TED_FUNCTION_INSTRUMENT, field_name)
        is RequirementPolicy.EXPECTED_ELSEWHERE
        and field_policy_for_function(source_function, field_name)
        is RequirementPolicy.REQUIRED
    )


def field_state_for_policy(policy: RequirementPolicy, *, value_present: bool) -> FieldState:
    if value_present:
        return FieldState.PRESENT
    return {
        RequirementPolicy.REQUIRED: FieldState.ABSENT,
        RequirementPolicy.EXPECTED_ELSEWHERE: FieldState.EXPECTED_ELSEWHERE,
        RequirementPolicy.NOT_APPLICABLE: FieldState.NOT_APPLICABLE,
        RequirementPolicy.OPTIONAL: FieldState.NOT_EVALUATED,
        RequirementPolicy.NOT_EVALUATED: FieldState.NOT_EVALUATED,
    }[policy]


def apply_ted_field_policy(
    resolved_function: str | None,
    diagnostics: list[Mapping[str, Any]],
) -> dict[str, tuple[RequirementPolicy, FieldState]]:
    result: dict[str, tuple[RequirementPolicy, FieldState]] = {}
    for diagnostic in diagnostics:
        field_name = str(diagnostic.get("field_name", "") or "").strip()
        if not field_name:
            continue
        policy = field_policy_for_function(resolved_function, field_name)
        present = diagnostic.get("value") is not None and bool(str(diagnostic.get("value", "")).strip())
        result[field_name] = (policy, field_state_for_policy(policy, value_present=present))
    return result
