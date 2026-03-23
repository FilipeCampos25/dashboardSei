from __future__ import annotations

from typing import Any, Dict, List

from app.documents.act import build_act_document_type
from app.documents.pt import build_pt_document_type
from app.documents.types import DocumentTypeSpec


def _build_registry() -> Dict[str, DocumentTypeSpec]:
    act = build_act_document_type()
    pt = build_pt_document_type()
    return {pt.key: pt, act.key: act}


def resolve_document_types(raw_value: str | None, logger: Any = None) -> List[DocumentTypeSpec]:
    registry = _build_registry()
    requested_keys: List[str] = []
    for part in (raw_value or "pt").split(","):
        key = part.strip().lower()
        if key and key not in requested_keys:
            requested_keys.append(key)

    resolved: List[DocumentTypeSpec] = []
    for key in requested_keys:
        spec = registry.get(key)
        if spec is not None:
            resolved.append(spec)
            continue
        if logger is not None:
            logger.warning("Tipo documental nao suportado e sera ignorado: %s", key)

    if resolved:
        return resolved
    return [registry["pt"]]
