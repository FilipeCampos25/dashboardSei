"""Pure classification helpers for the pre-opening candidate inventory."""

from __future__ import annotations

from typing import Any, Dict

from app.documents.document_utils import normalize_candidate_text, should_skip_candidate


_RELATION_MARKERS = (
    ("extrato", "extract"),
    ("publicacao", "publication"),
    ("termo aditivo", "amendment"),
    ("aditivo", "amendment"),
    ("anexo", "attachment"),
)


def describe_candidate_relation(title: str) -> Dict[str, Any]:
    """Describe a discovered candidate without deciding canonical publication."""

    normalized = normalize_candidate_text(title)
    for marker, relation in _RELATION_MARKERS:
        if marker in normalized:
            return {
                "probable_relation": relation,
                "relation_reason": f"title_marker:{marker}",
                "eligible_for_opening": False,
                "inventory_disposition": "related_not_opened",
            }

    skipped = should_skip_candidate(title)
    return {
        "probable_relation": "other_related" if skipped else "canonical_candidate",
        "relation_reason": "generic_related_marker" if skipped else "document_type_match",
        "eligible_for_opening": not skipped,
        "inventory_disposition": "related_not_opened" if skipped else "eligible_for_opening",
    }
