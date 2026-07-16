from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse


AFFINITY_RULE_VERSION = "act-process-affinity-v1-shadow"
PROCESS_PATTERN = re.compile(r"(?<!\d)(\d{5}\.\d{6}/\d{4}-\d{2})(?!\d)")


def _spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _norm(value: Any) -> str:
    text = _spaces(value).lower()
    # The snapshots include both valid UTF-8 and common UTF-8-as-latin1 text.
    for _ in range(2):
        if any(marker in text for marker in ("ã", "â", "�")):
            try:
                repaired = text.encode("latin1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                break
            if repaired == text:
                break
            text = repaired
    return "".join(
        char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char)
    )


def _context(text: str, start: int, end: int, radius: int = 150) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    return _spaces(text[left:right])


def _zone(text: str, start: int, context: str) -> str:
    normalized_context = _norm(context)
    ratio = start / max(len(text), 1)
    if ratio >= 0.72 and any(
        marker in normalized_context
        for marker in ("referencia: processo", "autenticidade", "codigo verificador", "codigo crc", "sei n")
    ):
        return "authentication_footer"
    if any(marker in normalized_context for marker in ("documento assinado eletronicamente", "assinam eletronicamente")):
        return "signature"
    before = _norm(text[:start])
    first_clause = min(
        (position for position in (before.find("clausula primeira"), before.find("clausula 1")) if position >= 0),
        default=-1,
    )
    if start <= 1400 and first_clause < 0:
        return "header"
    if first_clause < 0 or start < first_clause:
        return "preamble"
    return "body"


def _label_and_role(context: str, zone: str) -> tuple[str, str]:
    normalized = _norm(context)
    if "referencia: processo" in normalized:
        return "reference_footer", "origin"
    if re.search(r"processo\s+(?:inpe|sei|mcti|md|n|no|numero)", normalized):
        if zone == "header":
            return "institutional_process_header", "related"
        return "labeled_process", "cited"
    if any(marker in normalized for marker in ("processo relacionado", "processo do parceiro", "processo administrativo")):
        return "related_process", "related"
    if any(marker in normalized for marker in ("tendo em vista o que consta", "nos autos do processo", "consta do processo")):
        return "legal_reference", "cited"
    return "unlabeled_process", "unknown"


def _content_occurrences(snapshot: Dict[str, Any], collection: Dict[str, Any]) -> List[Dict[str, Any]]:
    occurrences: List[Dict[str, Any]] = []
    sources = (
        ("snapshot_title", str(snapshot.get("title", "") or ""), "title_tree"),
        ("tree_label", str(collection.get("chosen_documento", "") or ""), "title_tree"),
        ("document_text", str(snapshot.get("text", "") or ""), ""),
    )
    for source, text, fixed_zone in sources:
        for match in PROCESS_PATTERN.finditer(text):
            evidence = _context(text, match.start(), match.end())
            zone = fixed_zone or _zone(text, match.start(), evidence)
            label, role = _label_and_role(evidence, zone)
            occurrences.append(
                {
                    "process": match.group(1),
                    "zone": zone,
                    "label": label,
                    "context": evidence,
                    "position": match.start(),
                    "source": source,
                    "role": role,
                }
            )
    return occurrences


def _metadata_evidence(
    current_process: str, snapshot: Dict[str, Any], collection: Dict[str, Any]
) -> List[Dict[str, Any]]:
    evidence: List[Dict[str, Any]] = []
    if current_process:
        evidence.append(
            {
                "type": "collection_process_context",
                "process": current_process,
                "strength": "low",
                "role": "current_link",
                "proves_origin": False,
            }
        )
    found_in = _spaces(collection.get("found_in", "")).lower()
    if found_in in {"tree", "filter"}:
        evidence.append(
            {
                "type": f"found_in_{found_in}",
                "process": current_process,
                "strength": "medium" if found_in == "tree" else "low",
                "role": "current_link",
                "proves_origin": False,
            }
        )
    url = str(snapshot.get("url", "") or "")
    query = parse_qs(urlparse(url).query)
    for key in ("id_anexo", "id_documento", "id_procedimento"):
        if query.get(key):
            evidence.append(
                {
                    "type": key,
                    "value": query[key][0],
                    "process": current_process,
                    "strength": "medium" if key == "id_anexo" else "low",
                    "role": "current_link",
                    "proves_origin": False,
                }
            )
            break
    # Future collectors may provide an independent SEI relationship/origin field.
    for key in ("document_origin_process", "origin_process", "processo_origem", "processo_documento"):
        value = _spaces(collection.get(key, ""))
        if PROCESS_PATTERN.fullmatch(value):
            evidence.append(
                {
                    "type": key,
                    "process": value,
                    "strength": "high",
                    "role": "origin",
                    "proves_origin": True,
                }
            )
    for key in ("juntado_ao_processo", "related_to_current_process", "encaminhado_no_processo"):
        if collection.get(key) is True:
            evidence.append(
                {
                    "type": key,
                    "process": current_process,
                    "strength": "high",
                    "role": "current_link",
                    "proves_origin": False,
                }
            )
    return evidence


def _origin_candidate(occurrences: List[Dict[str, Any]], metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for item in metadata:
        if item.get("role") == "origin" and item.get("process"):
            candidates.append(
                {"process": item["process"], "source": f"metadata.{item['type']}", "confidence": "high", "score": 120}
            )
    for item in occurrences:
        score = 0
        confidence = "low"
        source = f"{item['zone']}.{item['label']}"
        if item["zone"] == "authentication_footer" and item["label"] == "reference_footer":
            score, confidence = 110, "high"
        elif item["zone"] == "header" and item["label"] == "institutional_process_header":
            score, confidence = 80, "medium"
        elif item["zone"] == "preamble" and item["label"] in {"legal_reference", "labeled_process"}:
            score, confidence = 55, "medium"
        if score:
            candidates.append(
                {"process": item["process"], "source": source, "confidence": confidence, "score": score}
            )
    if not candidates:
        return {"process": "", "source": "", "confidence": "low"}
    winner = max(candidates, key=lambda item: int(item["score"]))
    return {key: winner[key] for key in ("process", "source", "confidence")}


def assess_act_process_affinity(
    snapshot: Dict[str, Any], *, current_process: str, collection: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Produce a shadow-only, evidence-preserving ACT/process affinity diagnosis."""
    collection = collection or {}
    current_process = _spaces(current_process)
    occurrences = _content_occurrences(snapshot, collection)
    metadata = _metadata_evidence(current_process, snapshot, collection)
    current_occurrences = [item for item in occurrences if item["process"] == current_process]
    external_occurrences = [item for item in occurrences if item["process"] != current_process]
    origin = _origin_candidate(occurrences, metadata)

    strong_current_zone = any(item["zone"] in {"header", "authentication_footer"} for item in current_occurrences)
    strong_current_metadata = any(
        item.get("process") == current_process and item.get("proves_origin") for item in metadata
    )
    explicit_link = any(
        item.get("role") == "current_link"
        and item.get("strength") in {"medium", "high"}
        and item.get("type") not in {"collection_process_context"}
        for item in metadata
    )
    origin_external = bool(origin["process"] and origin["process"] != current_process)

    if strong_current_zone or strong_current_metadata:
        status = "strong_match"
        confidence = "high"
    elif origin_external and explicit_link:
        status = "related_document"
        confidence = "high" if origin["confidence"] == "high" else "medium"
    elif origin_external:
        status = "probable_external_document"
        confidence = origin["confidence"]
    elif current_occurrences:
        # A body citation alone is deliberately not promotion evidence.
        status = "ambiguous"
        confidence = "low"
    elif external_occurrences and explicit_link:
        status = "related_document"
        confidence = "medium"
    else:
        status = "ambiguous"
        confidence = "low"

    external_processes: List[Dict[str, Any]] = []
    for process in dict.fromkeys(item["process"] for item in external_occurrences):
        process_occurrences = [item for item in external_occurrences if item["process"] == process]
        role = "origin" if process == origin["process"] else next(
            (item["role"] for item in process_occurrences if item["role"] != "unknown"), "unknown"
        )
        external_processes.append(
            {"process": process, "role": role, "occurrences": process_occurrences}
        )

    evidence = [
        f"current_content_occurrences={len(current_occurrences)}",
        f"external_processes={len(external_processes)}",
        f"origin={origin['process'] or 'unknown'}:{origin['source'] or 'none'}",
        f"current_metadata_link={str(explicit_link).lower()}",
        "shadow_only=true",
    ]
    return {
        "current_process_explicit": {
            "found": bool(current_occurrences),
            "occurrences": current_occurrences,
        },
        "current_process_in_metadata": {
            "found": bool(metadata),
            "evidences": metadata,
        },
        "external_processes_found": external_processes,
        "document_origin_process": origin,
        "affinity_status": status,
        "affinity_confidence": confidence,
        "affinity_evidence": evidence,
        "affinity_rule_version": AFFINITY_RULE_VERSION,
        "shadow_only": True,
    }
