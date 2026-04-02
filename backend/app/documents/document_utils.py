from __future__ import annotations


SKIP_CANDIDATE_MARKERS = (
    "minuta",
    "extrato",
    "email",
    "e-mail",
    "anexo",
    "termo aditivo",
    "termo de adesao",
    "planilha",
)


def should_skip_candidate(text: str) -> bool:
    normalized_text = (text or "").strip().lower()
    return any(marker in normalized_text for marker in SKIP_CANDIDATE_MARKERS)
