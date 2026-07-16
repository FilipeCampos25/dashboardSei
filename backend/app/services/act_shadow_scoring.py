from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.output import csv_writer
from app.services.act_normalizer import (
    DOC_CLASS_ACT_FINAL,
    PUBLICATION_STATUS_GOLD,
    VALIDATION_STATUS_VALID,
    build_normalized_record,
)


SHADOW_SCORING_VERSION = "act-shadow-v1"
SHADOW_MINIMUM_MARGIN = 20
SUMMARY_FILENAME = "act_shadow_comparison_latest.csv"
DETAIL_FILENAME = "act_shadow_comparison_latest.json"
INVENTORY_FILENAME = "act_candidate_inventory_latest.csv"


def comparison_text(value: Any) -> str:
    """Return a comparison-only key without changing retained evidence."""
    text = str(value or "").strip()
    # Repair the common UTF-8-as-latin1 display corruption only in this key.
    if any(marker in text for marker in ("Ã", "Â", "â€")):
        try:
            repaired = text.encode("latin1").decode("utf-8")
            if repaired:
                text = repaired
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("º", "o").replace("°", "o")
    return re.sub(r"\s+", " ", text).strip()


def instrument_number_key(value: Any) -> str:
    normalized = comparison_text(value).replace(" ", "")
    match = re.search(r"(?<!\d)0*(\d{1,4})\s*/\s*0*(\d{2,4})(?:/([a-z0-9.-]+))?", normalized)
    if not match:
        return re.sub(r"[^a-z0-9/.-]+", "", normalized)
    year = int(match.group(2))
    if year < 100:
        year += 2000
    suffix = f"/{match.group(3)}" if match.group(3) else ""
    return f"{int(match.group(1))}/{year}{suffix}"


def process_number_key(value: Any) -> str:
    digits = re.sub(r"\D", "", comparison_text(value))
    # A SEI NUP has 17 digits; tolerate punctuation and a missing zero in the
    # six-digit sequence, which is observed in exported PDFs.
    if len(digits) == 16:
        digits = digits[:5] + "0" + digits[5:]
    return digits if len(digits) == 17 else ""


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return comparison_text(value) in {"true", "1", "yes", "sim"}


def _file_hash(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate_id(record: Dict[str, Any]) -> str:
    path = Path(str(record.get("candidate_json_path", "") or record.get("json_path", "")))
    payload = record.get("payload", {}) or {}
    snapshot = payload.get("snapshot", {}) or {}
    url = str(snapshot.get("url", "") or "")
    match = re.search(r"[?&](?:id_documento|id_anexo)=([^&]+)", url)
    return match.group(1) if match else path.stem


def _title(record: Dict[str, Any]) -> str:
    payload = record.get("payload", {}) or {}
    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    return str(collection.get("chosen_documento", "") or snapshot.get("title", "") or "")


def _score_current_breakdown(record: Dict[str, Any]) -> Dict[str, Any]:
    """Explain the existing score without participating in selection."""
    payload = record.get("payload", {}) or {}
    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    title = comparison_text(snapshot.get("title", ""))
    selected = comparison_text(collection.get("chosen_documento", ""))
    opening = comparison_text(str(snapshot.get("text", "") or "")[:4200])
    label = " ".join(part for part in (title, selected) if part)
    contributions: List[Dict[str, Any]] = []

    def add(rule: str, points: int, evidence: str = "") -> None:
        contributions.append({"rule": rule, "points": points, "evidence": evidence})

    if record.get("doc_class") != DOC_CLASS_ACT_FINAL:
        add("doc_class_not_act_final", -1000, str(record.get("doc_class", "")))
    else:
        if "acordo de cooperacao tecnica" in opening:
            add("opening_act_tecnica", 140)
        elif "acordo de cooperacao" in opening:
            add("opening_acordo_cooperacao", 100)
        if "acordo de cooperacao tecnica" in label:
            add("label_act_tecnica", 60)
        elif "acordo de cooperacao" in label:
            add("label_acordo_cooperacao", 30)
        contractual = next((m for m in ("que entre si celebram", "resolvem celebrar", "uniao, representada", "participe 1", "participes", "clausula primeira") if m in opening), "")
        if contractual:
            add("opening_contractual_marker", 60, contractual)
        if "para os fins que especifica" in opening:
            add("opening_fins_especifica", 20)
        if "assinado" in opening or "assinado" in label:
            add("opening_or_label_assinado", 10)
        for field, points in (("numero_acordo", 20), ("objeto", 10), ("orgao_convenente", 10)):
            if record.get(field):
                add(f"field_{field}", points)
        if record.get("validation_status") != VALIDATION_STATUS_VALID:
            add("validation_not_valid", -500)
        if not bool(record.get("has_internal_context")):
            add("missing_internal_context", -400)
        alignment = str(record.get("process_alignment_status", "") or "")
        if alignment == "material_mismatch":
            add("process_material_mismatch", -400)
        elif alignment == "external_reference":
            add("process_external_reference", -25)
        for marker in ("anexo", "minuta", "publicacao", "extrato", "reuniao", "alterado", "plano de trabalho", " pt "):
            if marker in label:
                add(f"label_penalty_{marker.strip().replace(' ', '_')}", -20 if marker == "anexo" else -80, marker)
        for marker in ("portaria", "publicacao", "reuniao", "plano de trabalho", "termo aditivo", "termo de adesao"):
            if marker in opening or marker in label:
                add(f"opening_penalty_{marker.replace(' ', '_')}", -180, marker)
        richness = min(len(str(snapshot.get("text", "") or "")) // 5000, 10)
        if richness:
            add("text_length_tiebreak", richness)
    return {"score": sum(item["points"] for item in contributions), "contributions": contributions}


def score_shadow_candidate(record: Dict[str, Any], preview: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    preview = preview or {}
    payload = record.get("payload", {}) or {}
    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    raw_text = str(snapshot.get("text", "") or "")
    title_raw = _title(record)
    title = comparison_text(title_raw)
    header = comparison_text(raw_text[:1800])
    opening = comparison_text(raw_text[:4200])
    full = comparison_text(raw_text)
    gates: List[str] = []
    flags: List[str] = []
    contributions: List[Dict[str, Any]] = []

    def add(rule: str, points: int, evidence: str = "") -> None:
        contributions.append({"rule": rule, "points": points, "evidence": evidence})

    doc_class = str(record.get("doc_class", "") or "")
    report = _as_bool(record.get("relatorio_encerramento")) or any(
        marker in title for marker in ("relatorio", "encerramento", "relatorio final", "relatorio conclusivo")
    )
    forbidden_title = next(
        (marker for marker in ("minuta", "despacho", "nota tecnica", "informacao tecnica", "extrato", "termo aditivo", "termo de adesao", "memorando") if marker in title),
        "",
    )
    if doc_class != DOC_CLASS_ACT_FINAL:
        gates.append(f"doc_class_ineligible:{doc_class or 'unknown'}")
    if report:
        gates.append("report_or_finalization_document")
    if forbidden_title:
        gates.append(f"forbidden_document_title:{forbidden_title}")
    if str(record.get("process_alignment_status", "")) == "material_mismatch":
        gates.append("process_material_mismatch")
    if not _as_bool(record.get("has_internal_context")):
        gates.append("missing_censipam_context")
    contractual = any(marker in opening for marker in ("que entre si celebram", "resolvem celebrar", "resolvem firmar", "clausula primeira", "objeto do presente acordo"))
    if "acordo de cooperacao" not in header or not contractual:
        gates.append("missing_minimum_contractual_evidence")

    add("doc_class_act_final", 100 if doc_class == DOC_CLASS_ACT_FINAL else 0, doc_class)
    if "acordo de cooperacao tecnica" in title:
        add("title_act_tecnica", 60, title_raw)
    if contractual:
        add("contractual_formula", 50)
    alignment = str(record.get("process_alignment_status", "") or "unknown")
    if alignment == "aligned":
        add("process_explicitly_aligned", 100, str(record.get("document_processo", "")))
    elif alignment == "unknown":
        add("process_alignment_unknown", -20)
        flags.append("process_alignment_unknown")
    elif alignment == "external_reference":
        add("process_external_reference", -30)

    candidate_number = instrument_number_key(record.get("numero_acordo", ""))
    preview_number = instrument_number_key(preview.get("numero_act", ""))
    if preview_number and candidate_number:
        if candidate_number.split("/", 2)[:2] == preview_number.split("/", 2)[:2]:
            add("preview_number_compatible", 80, f"{candidate_number}={preview_number}")
        else:
            add("preview_number_conflict", -40, f"{candidate_number}!={preview_number}")
            flags.append("preview_number_conflict")
    elif preview_number:
        add("preview_number_missing_in_candidate", -10, preview_number)
        flags.append("candidate_number_missing")

    if _as_bool(record.get("has_internal_context")):
        add("censipam_internal_context", 35)
    party_extraction = record.get("party_extraction", {}) or {}
    if (party_extraction.get("internal_party") or {}).get("role") == "parte_interna":
        add("censipam_structural_party", 25)
    if record.get("orgao_convenente"):
        add("counterparty_identified", 25, str(record.get("orgao_convenente", ""))[:160])
    if record.get("objeto"):
        add("object_clause", 35)
    if record.get("vigencia_raw") or record.get("vigencia_inicio") or record.get("vigencia_fim"):
        add("validity_clause", 25)
    signature_count = len([item for item in str(record.get("datas_assinatura", "") or "").split("|") if item.strip()])
    if not signature_count:
        signature_count = len(re.findall(r"documento assinado eletronicamente", full))
    if signature_count >= 2:
        add("multiple_signatures", 30, str(signature_count))
    elif signature_count == 1:
        add("single_signature", 15)
    richness = min(len(raw_text) // 20000, 2)
    if richness:
        add("text_length_low_tiebreak", richness)

    return {
        "version": SHADOW_SCORING_VERSION,
        "score": sum(item["points"] for item in contributions),
        "eligible": not gates,
        "gates": gates,
        "flags": flags,
        "contributions": contributions,
        "normalized": {
            "title": title,
            "instrument_number": candidate_number,
            "preview_number": preview_number,
            "process": process_number_key(record.get("processo", "")),
            "document_process": process_number_key(record.get("document_processo", "")),
        },
    }


def _candidate_fields(record: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not record:
        return {}
    return {
        "numero_acordo": str(record.get("numero_acordo", "") or ""),
        "objeto": str(record.get("objeto", "") or ""),
        "vigencia": str(record.get("vigencia_raw", "") or record.get("vigencia_inicio", "") or ""),
        "assinatura": str(record.get("datas_assinatura", "") or record.get("data_assinatura", "") or ""),
        "contraparte": str(record.get("orgao_convenente", "") or ""),
        "processo_documento": str(record.get("document_processo", "") or ""),
    }


def _field_delta(current: Optional[Dict[str, Any]], proposed: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    before, after = _candidate_fields(current), _candidate_fields(proposed)
    gained = [key for key in after if after[key] and not before.get(key)]
    lost = [key for key in before if before[key] and not after.get(key)]
    changed = [key for key in after if before.get(key) and after[key] and comparison_text(before[key]) != comparison_text(after[key])]
    return {"gained": gained, "lost": lost, "changed": changed}


def _compatibility(record: Optional[Dict[str, Any]], preview: Dict[str, str]) -> Dict[str, Any]:
    if not record:
        return {"number": "missing", "process": "missing", "partner": "missing"}
    candidate_number = instrument_number_key(record.get("numero_acordo", ""))
    preview_number = instrument_number_key(preview.get("numero_act", ""))
    number = "unknown"
    if preview_number and candidate_number:
        number = "compatible" if candidate_number.split("/", 2)[:2] == preview_number.split("/", 2)[:2] else "conflict"
    elif preview_number:
        number = "missing"
    process = str(record.get("process_alignment_status", "") or "unknown")
    partner_raw = comparison_text(preview.get("parceiro", ""))
    candidate_partner = comparison_text(record.get("orgao_convenente", ""))
    partner_tokens = {token for token in re.findall(r"[a-z0-9]{3,}", partner_raw) if token not in {"estado", "secretaria", "instituto", "federal"}}
    partner = "unknown"
    if partner_tokens and candidate_partner:
        partner = "compatible" if any(token in candidate_partner for token in partner_tokens) else "conflict"
    elif partner_raw:
        partner = "missing"
    return {"number": number, "process": process, "partner": partner}


def _load_candidates(output_dir: Path) -> List[Dict[str, Any]]:
    audit_rows = _read_csv(output_dir / "act_classificacao_latest.csv")
    candidates: List[Dict[str, Any]] = []
    for row in audit_rows:
        path = Path(str(row.get("candidate_json_path", "") or row.get("json_path", "")))
        if not path.exists() and row.get("candidate_json_path"):
            path = output_dir / "candidates" / Path(row["candidate_json_path"]).name
        payload = _read_json(path)
        if not payload:
            continue
        try:
            rebuilt = build_normalized_record(payload, path)
        except Exception:
            rebuilt = dict(row)
        persisted_score = row.get("canonical_score", "")
        # Rebuild content-derived fields with the current parser, but retain the
        # publication decision that was actually persisted by the current run.
        for field in (
            "publication_status", "normalization_status", "validation_status",
            "discard_reason", "canon_rejection_reason", "candidate_json_path", "json_path",
        ):
            if field in row:
                rebuilt[field] = row[field]
        rebuilt["candidate_json_path"] = str(path)
        rebuilt["payload"] = payload
        rebuilt["persisted_canonical_score"] = persisted_score
        candidates.append(rebuilt)
    return candidates


def _inventory_rows(output_dir: Path, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    discoveries = _read_csv(output_dir / "act_candidate_discoveries_latest.csv")
    rows: List[Dict[str, Any]] = []
    matched_titles: set[Tuple[str, str]] = set()
    for record in candidates:
        payload = record.get("payload", {}) or {}
        collection = payload.get("collection", {}) or {}
        title = _title(record)
        key = (str(record.get("processo", "")), comparison_text(title))
        matched_titles.add(key)
        shadow = record.get("shadow_score", {}) or {}
        current = record.get("current_breakdown", {}) or {}
        rows.append({
            "processo": record.get("processo", ""), "candidate_id": _candidate_id(record),
            "title": title, "normalized_title": comparison_text(title), "source": collection.get("found_in", ""),
            "snapshot_mode": record.get("snapshot_mode", ""), "doc_class": record.get("doc_class", ""),
            "content_excerpt": re.sub(r"\s+", " ", str((payload.get("snapshot", {}) or {}).get("text", "")))[:500],
            "text_chars": record.get("text_chars", 0), "tree_score": _tree_score(collection.get("selection_detail", "")),
            "persisted_current_score": record.get("persisted_canonical_score", ""), "recalculated_current_score": current.get("score", ""),
            "shadow_score": shadow.get("score", ""), "shadow_eligible": shadow.get("eligible", False),
            "shadow_gates": "|".join(shadow.get("gates", [])), "shadow_flags": "|".join(shadow.get("flags", [])),
            "numero_acordo": record.get("numero_acordo", ""), "normalized_numero_acordo": instrument_number_key(record.get("numero_acordo", "")),
            "document_processo": record.get("document_processo", ""), "normalized_document_processo": process_number_key(record.get("document_processo", "")),
            "candidate_json_path": record.get("candidate_json_path", ""), "disposition": "snapshot_extracted",
        })
    for discovery in discoveries:
        key = (str(discovery.get("processo", "")), comparison_text(discovery.get("title", "")))
        if key in matched_titles:
            continue
        rows.append({
            "processo": discovery.get("processo", ""), "candidate_id": "", "title": discovery.get("title", ""),
            "normalized_title": comparison_text(discovery.get("title", "")), "source": discovery.get("source", ""),
            "snapshot_mode": "", "doc_class": "", "content_excerpt": "", "text_chars": 0,
            "tree_score": discovery.get("tree_score", ""), "persisted_current_score": "", "recalculated_current_score": "",
            "shadow_score": "", "shadow_eligible": False, "shadow_gates": "not_extracted", "shadow_flags": "inventory_only",
            "numero_acordo": "", "normalized_numero_acordo": "", "document_processo": "", "normalized_document_processo": "",
            "candidate_json_path": "", "disposition": discovery.get("disposition", "discovered_not_extracted"),
        })
    return rows


def _tree_score(detail: Any) -> str:
    match = re.search(r"\bscore=(-?\d+)", str(detail or ""))
    return match.group(1) if match else ""


def export_shadow_report(output_dir: Path, logger: Any = None) -> Dict[str, Any]:
    """Evaluate every persisted ACT candidate without publishing any result."""
    output_dir = Path(output_dir)
    protected = [output_dir / "act_normalizado_latest.csv", *output_dir.glob("acordo_cooperacao_tecnica_*.json")]
    hashes_before = {str(path): _file_hash(path) for path in protected}
    previews = {row.get("processo", ""): row for row in _read_csv(output_dir / "parcerias_vigentes_latest.csv") if row.get("processo")}
    candidates = _load_candidates(output_dir)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for record in candidates:
        record["current_breakdown"] = _score_current_breakdown(record)
        record["shadow_score"] = score_shadow_candidate(record, previews.get(str(record.get("processo", "")), {}))
        grouped.setdefault(str(record.get("processo", "")), []).append(record)

    attempted_processes = {
        row.get("processo", "")
        for row in _read_csv(output_dir / "act_status_execucao_latest.csv")
        if row.get("processo")
    }
    all_processes = sorted(set(grouped) | (set(previews) & attempted_processes))
    details: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    for processo in all_processes:
        records = grouped.get(processo, [])
        current = next((item for item in records if str(item.get("publication_status", "")) == PUBLICATION_STATUS_GOLD), None)
        eligible = sorted(
            (item for item in records if item["shadow_score"]["eligible"]),
            key=lambda item: (int(item["shadow_score"]["score"]), -len(str(item.get("candidate_json_path", "")))),
            reverse=True,
        )
        winner = eligible[0] if eligible else None
        runner_score = int(eligible[1]["shadow_score"]["score"]) if len(eligible) > 1 else None
        margin = int(winner["shadow_score"]["score"]) - runner_score if winner is not None and runner_score is not None else None
        abstained = winner is None or (margin is not None and margin < SHADOW_MINIMUM_MARGIN)
        proposed = None if abstained else winner
        changed = bool(proposed and current and _candidate_id(proposed) != _candidate_id(current)) or bool(proposed and not current)
        delta = _field_delta(current, proposed)
        preview = previews.get(processo, {})
        current_compatibility = _compatibility(current, preview)
        shadow_compatibility = _compatibility(proposed, preview)
        confidence = "abstained" if abstained else ("high" if margin is None or margin >= 60 else "medium")
        detail = {
            "processo": processo, "preview": preview, "shadow_scoring_version": SHADOW_SCORING_VERSION,
            "current_selected_candidate": _candidate_id(current) if current else "",
            "shadow_selected_candidate": _candidate_id(proposed) if proposed else "",
            "shadow_abstained": abstained, "shadow_margin": margin, "minimum_margin": SHADOW_MINIMUM_MARGIN,
            "winner_changed": changed, "review_required": changed or abstained,
            "severity": "high" if changed else ("medium" if abstained else "none"), "field_delta": delta,
            "confidence": confidence, "current_compatibility": current_compatibility,
            "shadow_compatibility": shadow_compatibility,
            "candidates": [{
                "candidate_id": _candidate_id(item), "title": _title(item), "candidate_json_path": item.get("candidate_json_path", ""),
                "persisted_current_score": item.get("persisted_canonical_score", ""), "current_score_breakdown": item["current_breakdown"],
                "shadow_score_breakdown": item["shadow_score"], "is_current": item is current, "is_shadow": item is proposed,
                "fields": _candidate_fields(item), "candidate_hash": _file_hash(Path(str(item.get("candidate_json_path", "")))),
            } for item in records],
        }
        details.append(detail)
        summary_rows.append({
            "processo": processo, "preview_numero_act": preview.get("numero_act", ""), "preview_parceiro": preview.get("parceiro", ""),
            "current_selected_candidate": detail["current_selected_candidate"], "current_title": _title(current) if current else "",
            "current_score": current.get("persisted_canonical_score", "") if current else "",
            "shadow_selected_candidate": detail["shadow_selected_candidate"], "shadow_title": _title(proposed) if proposed else "",
            "shadow_score": proposed["shadow_score"]["score"] if proposed else "", "shadow_margin": "" if margin is None else margin,
            "winner_changed": changed, "shadow_abstained": abstained, "review_required": detail["review_required"],
            "severity": detail["severity"], "confidence": confidence,
            "current_number_compatibility": current_compatibility["number"], "shadow_number_compatibility": shadow_compatibility["number"],
            "current_process_compatibility": current_compatibility["process"], "shadow_process_compatibility": shadow_compatibility["process"],
            "current_partner_compatibility": current_compatibility["partner"], "shadow_partner_compatibility": shadow_compatibility["partner"],
            "fields_gained": "|".join(delta["gained"]), "fields_lost": "|".join(delta["lost"]),
            "fields_changed": "|".join(delta["changed"]), "shadow_scoring_version": SHADOW_SCORING_VERSION,
        })

    inventory = _inventory_rows(output_dir, candidates)
    inventory_columns = list(inventory[0].keys()) if inventory else ["processo", "candidate_id", "title", "disposition"]
    csv_writer.write_csv(inventory, output_dir / INVENTORY_FILENAME, columns=inventory_columns)
    summary_columns = list(summary_rows[0].keys()) if summary_rows else ["processo", "current_selected_candidate", "shadow_selected_candidate"]
    csv_writer.write_csv(summary_rows, output_dir / SUMMARY_FILENAME, columns=summary_columns)
    metrics = {
        "shadow_scoring_version": SHADOW_SCORING_VERSION, "processes_evaluated": len(all_processes),
        "candidates_total": len(candidates), "inventory_candidates_total": len(inventory),
        "candidates_without_content": sum(1 for row in inventory if not int(row.get("text_chars", 0) or 0)),
        "inventory_without_snapshot": sum(1 for row in inventory if row.get("disposition") != "snapshot_extracted"),
        "processes_with_incomplete_inventory": sorted({str(row.get("processo", "")) for row in inventory if row.get("disposition") != "snapshot_extracted"}),
        "winner_unchanged": sum(1 for item in details if not item["winner_changed"] and not item["shadow_abstained"]),
        "winner_changed": sum(1 for item in details if item["winner_changed"]), "abstentions": sum(1 for item in details if item["shadow_abstained"]),
        "unsafe_shadow_winners": sum(1 for item in details for candidate in item["candidates"] if candidate["is_shadow"] and candidate["shadow_score_breakdown"]["gates"]),
        "current_preview_number_compatible": sum(1 for item in details if item["current_compatibility"]["number"] == "compatible"),
        "shadow_preview_number_compatible": sum(1 for item in details if item["shadow_compatibility"]["number"] == "compatible"),
        "fields_gained": sum(len(item["field_delta"]["gained"]) for item in details),
        "fields_lost": sum(len(item["field_delta"]["lost"]) for item in details),
        "fields_changed": sum(len(item["field_delta"]["changed"]) for item in details),
        "manual_reviews_completed": 0, "manual_false_positives": 0, "manual_false_negatives": 0,
        "changed_processes": [item["processo"] for item in details if item["winner_changed"]],
    }
    payload = {"metrics": metrics, "processes": details}
    (output_dir / DETAIL_FILENAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    hashes_after = {str(path): _file_hash(path) for path in protected}
    metrics["gold_immutable"] = hashes_before == hashes_after
    metrics["gold_hashes"] = hashes_after
    # Rewrite only the shadow JSON to include the completed immutability check.
    (output_dir / DETAIL_FILENAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if logger:
        logger.info("ACT shadow: processos=%d candidatos=%d mudancas=%d abstencoes=%d gold_immutable=%s", len(all_processes), len(candidates), metrics["winner_changed"], metrics["abstentions"], metrics["gold_immutable"])
    return {"summary_path": output_dir / SUMMARY_FILENAME, "detail_path": output_dir / DETAIL_FILENAME, "inventory_path": output_dir / INVENTORY_FILENAME, "metrics": metrics}
