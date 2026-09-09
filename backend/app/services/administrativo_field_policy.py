"""Conservative V2 field policy for the supported administrative classes."""

from __future__ import annotations

from enum import Enum
from typing import Mapping

from app.services.field_states import FieldState
from app.services.pipeline_states import AccessState, AcquisitionState, ExtractionState


class AdministrativeFieldPolicy(str, Enum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    EXPECTED_ELSEWHERE = "EXPECTED_ELSEWHERE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    CONTEXTUAL = "CONTEXTUAL"


ADMINISTRATIVE_FIELDS = (
    "resolved_document_type", "funcao_administrativa", "origem", "destino", "data",
    "data_assinatura", "datas_assinatura", "assunto", "resumo", "acao_solicitada",
    "prazo", "documentos_mencionados",
)

_COMMON = {
    "resolved_document_type": AdministrativeFieldPolicy.REQUIRED,
    "funcao_administrativa": AdministrativeFieldPolicy.REQUIRED,
    "data": AdministrativeFieldPolicy.OPTIONAL,
    "data_assinatura": AdministrativeFieldPolicy.OPTIONAL,
    "datas_assinatura": AdministrativeFieldPolicy.OPTIONAL,
    "resumo": AdministrativeFieldPolicy.OPTIONAL,
    "acao_solicitada": AdministrativeFieldPolicy.CONTEXTUAL,
    "prazo": AdministrativeFieldPolicy.CONTEXTUAL,
    "documentos_mencionados": AdministrativeFieldPolicy.CONTEXTUAL,
}

ADMINISTRATIVE_FIELD_PROFILES: Mapping[str, Mapping[str, AdministrativeFieldPolicy]] = {
    "nota_tecnica": {
        **_COMMON,
        "origem": AdministrativeFieldPolicy.NOT_APPLICABLE,
        "destino": AdministrativeFieldPolicy.NOT_APPLICABLE,
        "assunto": AdministrativeFieldPolicy.OPTIONAL,
    },
    "memorando": {
        **_COMMON,
        "origem": AdministrativeFieldPolicy.EXPECTED_ELSEWHERE,
        "destino": AdministrativeFieldPolicy.EXPECTED_ELSEWHERE,
        "assunto": AdministrativeFieldPolicy.OPTIONAL,
    },
    "despacho": {
        **_COMMON,
        "origem": AdministrativeFieldPolicy.NOT_APPLICABLE,
        "destino": AdministrativeFieldPolicy.NOT_APPLICABLE,
        "assunto": AdministrativeFieldPolicy.OPTIONAL,
    },
    "oficio": {
        **_COMMON,
        "origem": AdministrativeFieldPolicy.EXPECTED_ELSEWHERE,
        "destino": AdministrativeFieldPolicy.EXPECTED_ELSEWHERE,
        "assunto": AdministrativeFieldPolicy.OPTIONAL,
    },
}


def field_policy_for_class(resolved_class: str | None, field_name: str) -> AdministrativeFieldPolicy | None:
    return ADMINISTRATIVE_FIELD_PROFILES.get(resolved_class or "", {}).get(field_name)


def field_state_for_policy(
    policy: AdministrativeFieldPolicy | None,
    *,
    value_present: bool,
    acquisition: AcquisitionState,
) -> FieldState:
    if value_present:
        return FieldState.PRESENT
    if acquisition.extraction is ExtractionState.EXTRACTION_FAILED:
        return FieldState.EXTRACTION_FAILED
    if acquisition.access in {
        AccessState.IFRAME_UNAVAILABLE,
        AccessState.ACCESS_RESTRICTED,
    }:
        return FieldState.INACCESSIBLE
    return {
        AdministrativeFieldPolicy.REQUIRED: FieldState.ABSENT,
        AdministrativeFieldPolicy.EXPECTED_ELSEWHERE: FieldState.EXPECTED_ELSEWHERE,
        AdministrativeFieldPolicy.NOT_APPLICABLE: FieldState.NOT_APPLICABLE,
        AdministrativeFieldPolicy.OPTIONAL: FieldState.NOT_EVALUATED,
        AdministrativeFieldPolicy.CONTEXTUAL: FieldState.NOT_EVALUATED,
        None: FieldState.NOT_EVALUATED,
    }[policy]


def quality_status(states: Mapping[str, FieldState], profile: Mapping[str, AdministrativeFieldPolicy]) -> str:
    evaluated = [states[name] for name in profile]
    if any(state in {FieldState.INACCESSIBLE, FieldState.EXTRACTION_FAILED} for state in evaluated):
        return "low"
    if any(state in {FieldState.CONFLICT, FieldState.UNRESOLVED} for state in evaluated):
        return "low"
    required = [states[name] for name, policy in profile.items() if policy is AdministrativeFieldPolicy.REQUIRED]
    if not required:
        return "not_evaluated"
    present = sum(state is FieldState.PRESENT for state in required)
    return "high" if present == len(required) else "medium" if present else "low"
