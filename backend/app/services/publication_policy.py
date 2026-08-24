from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from app.services.gold_contracts import DocumentGoldDecision, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.pipeline_states import AccessState, AcquisitionState, DiscoveryState, ExtractionState, OpeningState
from app.services.semantic_states import DocumentFunctionState, PublicationState, SemanticState


class PublicationReasonCode(str, Enum):
    """Stable universal outcomes for the document-gold eligibility gate."""

    ELIGIBLE_VERIFIABLE_CONTENT = "document_gold.eligible.verifiable_content"
    ELIGIBLE_EXTERNAL_AUTHORITY = "document_gold.eligible.external_authority"
    DISCOVERY_NOT_FOUND = "document_gold.ineligible.discovery_not_found"
    DISCOVERY_NOT_COMPLETED = "document_gold.ineligible.discovery_not_completed"
    OPENING_NOT_ATTEMPTED = "document_gold.ineligible.opening_not_attempted"
    OPENING_FAILED = "document_gold.ineligible.opening_failed"
    OPENING_TIMEOUT = "document_gold.ineligible.opening_timeout"
    ACCESS_NOT_CONFIRMED = "document_gold.ineligible.access_not_confirmed"
    ACCESS_RESTRICTED = "document_gold.ineligible.access_restricted"
    IFRAME_UNAVAILABLE = "document_gold.ineligible.iframe_unavailable"
    EXTRACTION_NOT_ATTEMPTED = "document_gold.ineligible.extraction_not_attempted"
    EXTRACTION_FAILED = "document_gold.ineligible.extraction_failed"
    EMPTY_CONTENT = "document_gold.ineligible.empty_content"
    NO_VERIFIABLE_CONTENT = "document_gold.ineligible.no_verifiable_content"
    FUNCTION_INELIGIBLE = "document_gold.ineligible.function"
    EXTERNAL_AUTHORITY_NOT_CONFIRMED = "document_gold.ineligible.external_authority_not_confirmed"


@dataclass(frozen=True)
class ExternalAuthority:
    """Caller-provided policy fact; source kind alone never implies authority."""

    source_kind: SourceKind
    authority_confirmed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.source_kind, SourceKind):
            raise TypeError("source_kind must be a SourceKind")
        if not isinstance(self.authority_confirmed, bool):
            raise TypeError("authority_confirmed must be a bool")


def evaluate_document_gold(
    *,
    identity: DocumentIdentity,
    acquisition: AcquisitionState,
    semantic: SemanticState,
    has_verifiable_content: bool,
    external_authority: ExternalAuthority | None = None,
) -> DocumentGoldDecision:
    """Evaluate universal document-gold preconditions without side effects.

    ``has_verifiable_content`` is an explicit upstream fact because a completed
    extraction does not, by itself, prove that usable document content exists.
    The returned publication state is a shadow V2 decision; this function does
    not read or mutate any legacy publication field.
    """

    if not isinstance(identity, DocumentIdentity):
        raise TypeError("identity must be a DocumentIdentity")
    if not isinstance(acquisition, AcquisitionState):
        raise TypeError("acquisition must be an AcquisitionState")
    if not isinstance(semantic, SemanticState):
        raise TypeError("semantic must be a SemanticState")
    if not isinstance(has_verifiable_content, bool):
        raise TypeError("has_verifiable_content must be a bool")
    if external_authority is not None and not isinstance(external_authority, ExternalAuthority):
        raise TypeError("external_authority must be an ExternalAuthority or None")

    technical_failure = _technical_failure(acquisition)
    if technical_failure is not None:
        return _decision(identity, semantic, eligible=False, reason=technical_failure)

    if semantic.function is DocumentFunctionState.INELIGIBLE:
        return _decision(identity, semantic, eligible=False, reason=PublicationReasonCode.FUNCTION_INELIGIBLE)

    if has_verifiable_content and acquisition.extraction in {
        ExtractionState.EXTRACTED,
        ExtractionState.CONTENT_PARTIAL,
    }:
        return _decision(
            identity,
            semantic,
            eligible=True,
            reason=PublicationReasonCode.ELIGIBLE_VERIFIABLE_CONTENT,
        )

    if (
        external_authority is not None
        and external_authority.source_kind is SourceKind.EXTERNAL
        and external_authority.authority_confirmed
    ):
        return _decision(
            identity,
            semantic,
            eligible=True,
            reason=PublicationReasonCode.ELIGIBLE_EXTERNAL_AUTHORITY,
        )

    if external_authority is not None:
        return _decision(
            identity,
            semantic,
            eligible=False,
            reason=PublicationReasonCode.EXTERNAL_AUTHORITY_NOT_CONFIRMED,
        )

    if acquisition.extraction is ExtractionState.EMPTY_CONTENT:
        return _decision(identity, semantic, eligible=False, reason=PublicationReasonCode.EMPTY_CONTENT)

    return _decision(identity, semantic, eligible=False, reason=PublicationReasonCode.NO_VERIFIABLE_CONTENT)


def _technical_failure(acquisition: AcquisitionState) -> PublicationReasonCode | None:
    if acquisition.discovery is DiscoveryState.NOT_FOUND:
        return PublicationReasonCode.DISCOVERY_NOT_FOUND
    if acquisition.discovery is DiscoveryState.NOT_SEARCHED:
        return PublicationReasonCode.DISCOVERY_NOT_COMPLETED
    if acquisition.opening is OpeningState.NOT_ATTEMPTED:
        return PublicationReasonCode.OPENING_NOT_ATTEMPTED
    if acquisition.opening is OpeningState.OPEN_FAILED:
        return PublicationReasonCode.OPENING_FAILED
    if acquisition.opening is OpeningState.TIMEOUT:
        return PublicationReasonCode.OPENING_TIMEOUT
    if acquisition.access is AccessState.ACCESS_RESTRICTED:
        return PublicationReasonCode.ACCESS_RESTRICTED
    if acquisition.access is AccessState.IFRAME_UNAVAILABLE:
        return PublicationReasonCode.IFRAME_UNAVAILABLE
    if acquisition.access is AccessState.UNKNOWN:
        return PublicationReasonCode.ACCESS_NOT_CONFIRMED
    if acquisition.extraction is ExtractionState.NOT_ATTEMPTED:
        return PublicationReasonCode.EXTRACTION_NOT_ATTEMPTED
    if acquisition.extraction is ExtractionState.EXTRACTION_FAILED:
        return PublicationReasonCode.EXTRACTION_FAILED
    return None


def _decision(
    identity: DocumentIdentity,
    semantic: SemanticState,
    *,
    eligible: bool,
    reason: PublicationReasonCode,
) -> DocumentGoldDecision:
    shadow_state = replace(
        semantic,
        publication=PublicationState.PUBLISHED if eligible else PublicationState.BLOCKED,
    )
    return DocumentGoldDecision(identity=identity, semantic_state=shadow_state, reason_codes=(reason.value,))
