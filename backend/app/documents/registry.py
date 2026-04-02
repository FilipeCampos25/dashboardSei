from __future__ import annotations

import inspect
from typing import Any, Dict, List

from app.documents.act import build_act_document_type
from app.documents.memorando import build_memorando_document_type
from app.documents.pt import build_pt_document_type
from app.documents.ted import build_ted_document_type
from app.documents.types import DocumentTypeSpec

print("DEBUG registry loaded from:", inspect.getfile(inspect.currentframe()))


def _build_registry() -> Dict[str, DocumentTypeSpec]:
    act = build_act_document_type()
    memorando = build_memorando_document_type()
    pt = build_pt_document_type()
    ted = build_ted_document_type()
    return {
        pt.key: pt,
        act.key: act,
        memorando.key: memorando,
        ted.key: ted,
    }


def _expand_document_key(key: str) -> List[str]:
    return [key]


def resolve_document_types(raw_value: str | None, logger: Any = None) -> List[DocumentTypeSpec]:
    if logger is not None:
        logger.info("DEBUG resolve_document_types input: %s", raw_value)
    registry = _build_registry()
    requested_keys: List[str] = []
    for part in (raw_value or "pt").split(","):
        key = part.strip().lower()
        for expanded_key in _expand_document_key(key):
            if expanded_key and expanded_key not in requested_keys:
                requested_keys.append(expanded_key)

    resolved: List[DocumentTypeSpec] = []
    for key in requested_keys:
        spec = registry.get(key)
        if spec is not None:
            resolved.append(spec)
            continue
        if logger is not None:
            logger.warning("Tipo documental nao suportado e sera ignorado: %s", key)

    if resolved:
        if logger is not None:
            logger.info("DEBUG resolved document types: %s", [spec.key for spec in resolved])
        return resolved
    if logger is not None:
        logger.info("DEBUG resolved document types: %s", [registry["pt"].key])
    return [registry["pt"]]
