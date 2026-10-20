"""Pure classification helpers for the pre-opening candidate inventory."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

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


def prioritize_candidates(candidates: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build a stable selective-extraction plan while retaining the full inventory."""

    planned: List[Dict[str, Any]] = []
    for inventory_index, candidate in enumerate(candidates, start=1):
        item = dict(candidate)
        relation = describe_candidate_relation(str(item.get("text", "")))
        for key, value in relation.items():
            item.setdefault(key, value)
        item["inventory_index"] = inventory_index
        item["extraction_decision"] = (
            "open_and_extract" if item.get("eligible_for_opening") else "retain_without_extraction"
        )
        planned.append(item)

    planned.sort(
        key=lambda item: (
            not bool(item.get("eligible_for_opening")),
            -int(item.get("score", 0) or 0),
            int(item["inventory_index"]),
        )
    )
    for priority, item in enumerate(planned, start=1):
        item["extraction_priority"] = priority if item.get("eligible_for_opening") else None
    return planned
