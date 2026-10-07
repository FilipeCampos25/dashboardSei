from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Dict, Optional

from app.documents.common import acquisition_diagnostic_payload, acquisition_state_payload
from app.services.pt_normalizer import (
    CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
    REQUESTED_TYPE_PT,
    RESOLVED_TYPE_PT,
    VALIDATION_STATUS_NON_CANONICAL,
    VALIDATION_STATUS_VALID,
)


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    fixed = value
    if any(marker in fixed for marker in ("\u00c3", "\u00c2", "\ufffd")):
        try:
            fixed = fixed.encode("latin1").decode("utf-8")
        except UnicodeError:
            fixed = value

    collapsed = " ".join(fixed.split()).strip().upper()
    deaccented = unicodedata.normalize("NFKD", collapsed)
    return "".join(ch for ch in deaccented if not unicodedata.combining(ch))


def pt_internal_content_score(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    chosen_documento = _normalize_text(str((collection_context or {}).get("chosen_documento", "") or ""))
    title_blob = _normalize_text(str(snapshot.get("title", "") or ""))
    text = str(snapshot.get("text", "") or "")
    text_head = _normalize_text(text[:6000])
    title_context = " ".join(part for part in (chosen_documento, title_blob) if part)
    tables = snapshot.get("tables", [])
    tables_count = len(tables) if isinstance(tables, list) else 0

    signals: list[str] = []
    score = 0

    def add_signal(name: str, points: int, condition: bool) -> None:
        nonlocal score
        if condition:
            signals.append(name)
            score += points

    add_signal("marcador_plano_trabalho", 1, "PLANO DE TRABALHO" in text_head or "PLANO DE TRABALHO" in title_context)
    add_signal("objeto", 1, any(marker in text_head for marker in ("IDENTIFICACAO DO OBJETO", "OBJETO:", "OBJETO DO PRESENTE")))
    add_signal("metas", 1, any(marker in text_head for marker in ("META", "METAS", "METAS A SEREM ATINGIDAS")))
    add_signal("acoes_cronograma", 1, any(marker in text_head for marker in ("ACAO", "ACOES", "ATIVIDADE", "ATIVIDADES", "ETAPA", "CRONOGRAMA")))
    add_signal(
        "prazo_periodo",
        1,
        any(
            marker in text_head
            for marker in ("PREVISAO DE INICIO", "PERIODO DE EXECUCAO", "INICIO", "TERMINO", "VIGORARA PELO PRAZO", "VIGENCIA")
        ),
    )
    add_signal("data_explicita", 1, bool(re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", text)))
    add_signal("tabelas", 1, tables_count > 0)

    penalties: list[str] = []
    if "MODELO DE ACORDO DE COOPERACAO TECNICA" in text_head:
        penalties.append("modelo_act")
    if "XX/20XX" in text_head or "XXXXX.XXXXXX/XXXX-XX" in text_head:
        penalties.append("placeholder")
    if "INSERIR PREVISAO" in text_head or ("INSERIR" in text_head and "PREVISAO DE INICIO" in text_head):
        penalties.append("instrucao_placeholder")
    if penalties:
        score -= 3

    return {
        "internal_content_score": score,
        "internal_content_signals": "|".join(signals),
        "internal_content_penalties": "|".join(penalties),
    }


def classify_pt_snapshot(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
    *,
    content_scorer: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Dict[str, Any]] = pt_internal_content_score,
) -> Dict[str, Any]:
    context = collection_context or {}
    has_acquisition_evidence = isinstance(context.get("acquisition_state"), dict) or isinstance(
        snapshot.get("acquisition_observation"), dict
    )
    if has_acquisition_evidence:
        acquisition_state = acquisition_state_payload(context, snapshot)
        opening_state = acquisition_state["opening"]
        access_state = acquisition_state["access"]
        extraction_state = acquisition_state["extraction"]
        semantic_evaluation_eligible = (
            opening_state == "OPENED"
            and access_state == "ACCESSIBLE"
            and extraction_state in {"EXTRACTED", "CONTENT_PARTIAL"}
        )
        if not semantic_evaluation_eligible:
            diagnostic = acquisition_diagnostic_payload(context, snapshot)
            technical_reason = diagnostic["code"]
            if not technical_reason:
                if opening_state in {"OPEN_FAILED", "TIMEOUT"}:
                    technical_reason = opening_state
                elif access_state in {"IFRAME_UNAVAILABLE", "ACCESS_RESTRICTED"}:
                    technical_reason = access_state
                elif extraction_state == "EXTRACTION_FAILED":
                    technical_reason = extraction_state
                elif extraction_state == "EMPTY_CONTENT":
                    technical_reason = extraction_state
                else:
                    technical_reason = "ACQUISITION_NOT_EVALUABLE"
            return {
                "doc_class": "",
                "requested_type": REQUESTED_TYPE_PT,
                "resolved_document_type": RESOLVED_TYPE_PT,
                "is_canonical_candidate": False,
                "validation_status": "technical_failure",
                "publication_status": "retained_silver",
                "discard_reason": "",
                "classification_reason": technical_reason,
                "semantic_evaluation_eligible": False,
                "acquisition_state": acquisition_state,
            }

    chosen_documento = _normalize_text(str(context.get("chosen_documento", "") or ""))
    title_blob = _normalize_text(str(snapshot.get("title", "") or ""))
    text_head = _normalize_text(str(snapshot.get("text", "") or "")[:1600])
    text_prefix = _normalize_text(str(snapshot.get("text", "") or "")[:400])
    title_context = " ".join(part for part in (chosen_documento, title_blob) if part)

    has_plano_marker = "PLANO DE TRABALHO" in text_head or "PLANO DE TRABALHO" in title_context
    has_documentacao_marker = "DOCUMENTACAO" in title_context
    has_minuta_marker = "MINUTA" in title_context or "MINUTAS" in title_context
    has_minuta_text = "MINUTA" in text_prefix or "MINUTAS" in text_prefix or bool(
        re.search(r"\bMINUTA(?:\s+DE)?\s+PLANO\s+DE\s+TRABALHO\b", text_head, flags=re.IGNORECASE)
    )

    is_non_canonical = has_plano_marker and (has_minuta_text or (has_documentacao_marker and has_minuta_marker))
    quality = content_scorer(snapshot, collection_context)
    if is_non_canonical:
        return {
            "doc_class": "pt_minuta_documentacao",
            "requested_type": REQUESTED_TYPE_PT,
            "resolved_document_type": RESOLVED_TYPE_PT,
            "is_canonical_candidate": False,
            "validation_status": VALIDATION_STATUS_NON_CANONICAL,
            "publication_status": "retained_silver",
            "discard_reason": "minuta_documentacao",
            "classification_reason": CLASSIFICATION_REASON_MINUTA_DOCUMENTACAO,
            "semantic_evaluation_eligible": True,
            **quality,
        }

    internal_score = int(quality.get("internal_content_score", 0) or 0)
    internal_penalties = str(quality.get("internal_content_penalties", "") or "")
    if internal_score < 3 or internal_penalties:
        return {
            "doc_class": "pt_conteudo_interno_insuficiente",
            "requested_type": REQUESTED_TYPE_PT,
            "resolved_document_type": RESOLVED_TYPE_PT,
            "is_canonical_candidate": False,
            "validation_status": VALIDATION_STATUS_NON_CANONICAL,
            "publication_status": "retained_silver",
            "discard_reason": "conteudo_interno_insuficiente",
            "classification_reason": "pt_conteudo_interno_insuficiente",
            "semantic_evaluation_eligible": True,
            **quality,
        }

    return {
        "doc_class": RESOLVED_TYPE_PT,
        "requested_type": REQUESTED_TYPE_PT,
        "resolved_document_type": RESOLVED_TYPE_PT,
        "is_canonical_candidate": True,
        "validation_status": VALIDATION_STATUS_VALID,
        "publication_status": "",
        "discard_reason": "",
        "classification_reason": "",
        "semantic_evaluation_eligible": True,
        **quality,
    }
