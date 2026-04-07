from __future__ import annotations

from typing import Any

from app.services.act_normalizer import (
    DOC_CLASS_TED,
    PUBLICATION_STATUS_GOLD,
    RESOLVED_TYPE_TED,
    SNAPSHOT_PREFIX_TED,
    VALIDATION_STATUS_VALID,
)


def processar_ted_api(payload: list[dict[str, Any]]) -> dict[str, Any]:
    if not payload:
        return {
            "numero_processo": None,
            "objeto": None,
            "valor_global": None,
            "situacao": None,
            "uf": None,
            "itens": None,
        }

    item = payload[0] or {}

    return {
        "numero_processo": item.get("numero_processo", item.get("numeroProcesso")),
        "objeto": item.get("objeto"),
        "valor_global": item.get("valor_global", item.get("valorGlobal")),
        "situacao": item.get("situacao"),
        "uf": item.get("uf"),
        "itens": item.get("itens"),
    }


def build_ted_api_snapshot(
    *,
    processo: str,
    numero_instrumento: str,
    payload_bruto: list[dict[str, Any]],
    payload_processado: dict[str, Any],
) -> dict[str, Any]:
    text_lines = [
        "TED via API",
        f"Processo: {processo}",
    ]
    if numero_instrumento:
        text_lines.append(f"Instrumento: {numero_instrumento}")
    if payload_processado.get("objeto"):
        text_lines.append(f"Objeto: {payload_processado['objeto']}")
    if payload_processado.get("valor_global") is not None:
        text_lines.append(f"Valor global: {payload_processado['valor_global']}")
    if payload_processado.get("situacao"):
        text_lines.append(f"Situacao: {payload_processado['situacao']}")
    if payload_processado.get("uf"):
        text_lines.append(f"UF: {payload_processado['uf']}")

    return {
        "extraction_mode": "api",
        "source": "transferegov_api",
        "title": "TED via API",
        "url": "https://val-siconv.np.estaleiro.serpro.gov.br/maisbrasil-api/v1/services/public/processo-compra/consultar",
        "text": "\n".join(text_lines),
        "tables": [],
        "api_payload": payload_processado,
        "api_raw": payload_bruto,
    }


def build_ted_api_analysis() -> dict[str, Any]:
    return {
        "doc_class": DOC_CLASS_TED,
        "classification_reason": "api_transferegov",
        "requested_type": "ted",
        "accepted_doc_classes": (DOC_CLASS_TED,),
        "resolved_document_type": RESOLVED_TYPE_TED,
        "requested_snapshot_prefix": SNAPSHOT_PREFIX_TED,
        "is_canonical_candidate": True,
        "validation_status": VALIDATION_STATUS_VALID,
        "publication_status": PUBLICATION_STATUS_GOLD,
        "normalization_status": "publicado_canonico",
        "discard_reason": "",
    }
