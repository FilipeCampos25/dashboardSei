from __future__ import annotations

import re
import unicodedata


SKIP_CANDIDATE_MARKERS = (
    "minuta",
    "minutas",
    "extrato",
    "email",
    "e-mail",
    "anexo",
    "termo aditivo",
    "proposta de termo aditivo",
    "termo de adesao",
    "documentacao",
    "planilha",
    "publicacao",
)


def normalize_candidate_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", (text or "").strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized)


def should_skip_candidate(text: str) -> bool:
    normalized_text = normalize_candidate_text(text)
    return any(marker in normalized_text for marker in SKIP_CANDIDATE_MARKERS)
