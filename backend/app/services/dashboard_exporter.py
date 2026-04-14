from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from app.output import csv_writer
from app.services.act_normalizer import PUBLICATION_STATUS_GOLD as ACT_PUBLICATION_STATUS_GOLD
from app.services.pt_normalizer import PUBLICATION_STATUS_GOLD as PT_PUBLICATION_STATUS_GOLD


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    if logger is None:
        return
    try:
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(msg, *args)
    except Exception:
        return


def _clean_spaces(value: str) -> str:
    return " ".join((value or "").replace("\r", "\n").split()).strip()


def _read_csv_rows(path: Path, logger: Any = None) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as exc:
        _log(logger, "warning", "Dashboard exporter: falha ao ler %s (%s).", path, exc)
        return []
    return [{key: _clean_spaces(str(value or "")) for key, value in row.items()} for row in df.to_dict(orient="records")]


def _preview_rows(output_dir: Path, logger: Any = None) -> List[Dict[str, str]]:
    rows = _read_csv_rows(output_dir / "parcerias_vigentes_latest.csv", logger=logger)
    unique: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        processo = _clean_spaces(row.get("processo", ""))
        if not processo or processo in seen:
            continue
        seen.add(processo)
        unique.append(row)
    return unique


def _safe_int(value: str) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _group_rows(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        processo = _clean_spaces(row.get("processo", ""))
        if processo:
            grouped.setdefault(processo, []).append(row)
    return grouped


def _read_json_payload(path: Path, logger: Any = None) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _log(logger, "warning", "Dashboard exporter: falha ao ler JSON %s (%s).", path, exc)
        return {}


def _resolve_json_path(raw_path: str, output_dir: Path) -> Path | None:
    cleaned = _clean_spaces(raw_path)
    if not cleaned:
        return None
    candidate = Path(cleaned)
    if candidate.exists():
        return candidate
    fallback = output_dir / candidate.name
    if fallback.exists():
        return fallback
    return None


def _ted_payload(row: Dict[str, str], output_dir: Path, logger: Any = None) -> Dict[str, Any]:
    json_path = _resolve_json_path(row.get("json_path", ""), output_dir)
    if json_path is None:
        return {}
    payload = _read_json_payload(json_path, logger=logger)
    snapshot = payload.get("snapshot", {})
    if not isinstance(snapshot, dict):
        return {}
    api_payload = snapshot.get("api_payload", {})
    return api_payload if isinstance(api_payload, dict) else {}


def _best_pt_row(rows: List[Dict[str, str]]) -> Dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            row.get("publication_status", "") == PT_PUBLICATION_STATUS_GOLD,
            _safe_int(row.get("captured_focus_fields", "")),
            len(row.get("objeto", "")),
        ),
    )


def _best_act_row(rows: List[Dict[str, str]]) -> Dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            row.get("publication_status", "") == ACT_PUBLICATION_STATUS_GOLD,
            _safe_int(row.get("canonical_score", "")),
            len(row.get("objeto", "")),
        ),
    )


def _first_row(rows: List[Dict[str, str]]) -> Dict[str, str]:
    return rows[0] if rows else {}


def _pt_quality(row: Dict[str, str]) -> str:
    if not row:
        return "not_found"
    if row.get("publication_status", "") == PT_PUBLICATION_STATUS_GOLD:
        return "gold"
    return "silver_only"


def _act_quality(row: Dict[str, str]) -> str:
    if not row:
        return "not_found"
    if row.get("publication_status", "") != ACT_PUBLICATION_STATUS_GOLD:
        return "silver_only"
    missing = [
        field
        for field in (
            "numero_acordo",
            "data_inicio_vigencia",
            "data_fim_vigencia",
            "orgao_convenente",
            "objeto",
        )
        if not _clean_spaces(row.get(field, ""))
    ]
    return "gold_partial" if missing else "gold_complete"


def _act_missing_fields(row: Dict[str, str]) -> List[str]:
    if not row or row.get("publication_status", "") != ACT_PUBLICATION_STATUS_GOLD:
        return []
    return [
        field
        for field in ("numero_acordo", "data_inicio_vigencia", "data_fim_vigencia", "orgao_convenente", "objeto")
        if not _clean_spaces(row.get(field, ""))
    ]


def _ted_quality(ted_row: Dict[str, str], status_rows: List[Dict[str, str]]) -> str:
    if ted_row.get("json_path", ""):
        return "gold"
    if status_rows:
        reason = _clean_spaces(status_rows[0].get("selection_reason", ""))
        return reason or "not_found"
    return "not_found"


def _summarize_act_rejections(rows: List[Dict[str, str]]) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        if row.get("publication_status", "") == ACT_PUBLICATION_STATUS_GOLD:
            continue
        label = (
            _clean_spaces(row.get("doc_class", ""))
            or _clean_spaces(row.get("selection_reason", ""))
            or "unknown"
        )
        reason = _clean_spaces(row.get("classification_reason", "")) or _clean_spaces(row.get("discard_reason", ""))
        key = f"{label}:{reason}" if reason else label
        counts[key] = counts.get(key, 0) + 1
    return " | ".join(f"{key}({count})" for key, count in sorted(counts.items()))


def _period_label(row: Dict[str, str]) -> str:
    start = _clean_spaces(row.get("vigencia_inicio", "") or row.get("data_inicio_vigencia", ""))
    end = _clean_spaces(row.get("vigencia_fim", "") or row.get("data_fim_vigencia", ""))
    if start or end:
        return f"{start}..{end}"
    return ""


def _has_process_mismatch(act_row: Dict[str, str]) -> bool:
    warning = _clean_spaces(act_row.get("validation_warning", ""))
    return "processo_divergente_documento=" in warning or "processo_referencia_externa_documento=" in warning


def _overall_quality_status(
    *,
    pt_quality: str,
    act_quality: str,
    memorando_gold: bool,
    ted_gold: bool,
    has_process_mismatch: bool,
) -> str:
    if act_quality == "gold_complete" and not has_process_mismatch:
        return "high"
    if act_quality.startswith("gold") or pt_quality == "gold" or memorando_gold or ted_gold:
        return "medium"
    return "low"


def export_dashboard_ready_csv(output_dir: Path, logger: Any = None) -> Dict[str, Any]:
    csv_writer.ensure_output_dir(output_dir)
    preview_rows = _preview_rows(output_dir, logger=logger)
    if not preview_rows:
        _log(logger, "info", "Dashboard exporter: nenhum preview encontrado em %s.", output_dir)
        return {"records": 0, "csv_path": None}

    pt_grouped = _group_rows(_read_csv_rows(output_dir / "pt_auditoria_latest.csv", logger=logger))
    act_grouped = _group_rows(_read_csv_rows(output_dir / "act_classificacao_latest.csv", logger=logger))
    act_status_grouped = _group_rows(_read_csv_rows(output_dir / "act_status_execucao_latest.csv", logger=logger))
    memorando_grouped = _group_rows(_read_csv_rows(output_dir / "memorando_normalizado_latest.csv", logger=logger))
    ted_grouped = _group_rows(_read_csv_rows(output_dir / "ted_normalizado_latest.csv", logger=logger))
    ted_status_grouped = _group_rows(_read_csv_rows(output_dir / "ted_status_execucao_latest.csv", logger=logger))

    rows: List[Dict[str, Any]] = []
    divergence_rows: List[Dict[str, Any]] = []
    for preview in preview_rows:
        processo = _clean_spaces(preview.get("processo", ""))
        pt_row = _best_pt_row(pt_grouped.get(processo, [])) if pt_grouped.get(processo) else {}
        act_row = _best_act_row(act_grouped.get(processo, [])) if act_grouped.get(processo) else {}
        act_attempt_rows = act_status_grouped.get(processo, [])
        memorando_row = _first_row(memorando_grouped.get(processo, []))
        ted_row = _first_row(ted_grouped.get(processo, []))
        ted_status_rows = ted_status_grouped.get(processo, [])
        ted_api_payload = _ted_payload(ted_row, output_dir, logger=logger) if ted_row else {}

        pt_quality = _pt_quality(pt_row)
        act_quality = _act_quality(act_row)
        ted_quality = _ted_quality(ted_row, ted_status_rows)
        pt_gold = pt_row.get("publication_status", "") == PT_PUBLICATION_STATUS_GOLD
        act_gold = act_row.get("publication_status", "") == ACT_PUBLICATION_STATUS_GOLD
        memorando_gold = bool(memorando_row.get("json_path", ""))
        ted_gold = bool(ted_row.get("json_path", ""))
        has_process_mismatch = _has_process_mismatch(act_row)
        act_missing = _act_missing_fields(act_row)
        act_rejection_summary = _summarize_act_rejections(act_attempt_rows)

        act_orgao = _clean_spaces(act_row.get("orgao_convenente", "")) if act_gold else ""
        preview_partner = _clean_spaces(preview.get("parceiro", ""))
        source_act_parceiro = "act_gold" if act_orgao else ("preview_fallback" if preview_partner else "missing")
        if not act_orgao:
            act_orgao = preview_partner

        act_objeto = _clean_spaces(act_row.get("objeto", "")) if act_gold else ""
        preview_objeto = _clean_spaces(preview.get("objeto", ""))
        source_act_objeto = "act_gold" if act_objeto else ("preview_fallback" if preview_objeto else "missing")
        if not act_objeto:
            act_objeto = preview_objeto

        notes: List[str] = []
        if act_quality != "gold_complete":
            notes.append(f"act={act_quality}")
        if pt_quality != "gold":
            notes.append(f"pt={pt_quality}")
        if ted_quality != "gold":
            notes.append(f"ted={ted_quality}")
        if has_process_mismatch:
            notes.append(_clean_spaces(act_row.get("validation_warning", "")))
        if act_gold and act_quality == "gold_partial":
            if act_missing:
                notes.append(f"act_missing={','.join(act_missing)}")
        if source_act_objeto == "preview_fallback":
            notes.append("act_objeto=preview_fallback")
        if source_act_parceiro == "preview_fallback":
            notes.append("act_parceiro=preview_fallback")
        if pt_quality == "silver_only" and _period_label(pt_row):
            notes.append(f"pt_silver_vigencia={_period_label(pt_row)}")
        if ted_gold:
            notes.append("ted=gold")

        dashboard_row = (
            {
                "processo": processo,
                "preview_parceiro": preview_partner,
                "preview_numero_act": _clean_spaces(preview.get("numero_act", "")),
                "preview_objeto": preview_objeto,
                "preview_vigencia": _clean_spaces(preview.get("vigencia", "")),
                "pt_gold": pt_gold,
                "pt_json_path": pt_row.get("json_path", "") if pt_gold else "",
                "pt_vigencia_inicio": pt_row.get("vigencia_inicio", "") if pt_gold else "",
                "pt_vigencia_fim": pt_row.get("vigencia_fim", "") if pt_gold else "",
                "pt_quality": pt_quality,
                "act_gold": act_gold,
                "act_json_path": act_row.get("json_path", "") if act_gold else "",
                "act_numero_acordo": act_row.get("numero_acordo", "") if act_gold else "",
                "act_data_inicio_vigencia": act_row.get("data_inicio_vigencia", "") if act_gold else "",
                "act_data_fim_vigencia": act_row.get("data_fim_vigencia", "") if act_gold else "",
                "act_orgao_convenente": act_orgao,
                "act_objeto": act_objeto,
                "act_quality": act_quality,
                "has_process_mismatch": has_process_mismatch,
                "source_act_objeto": source_act_objeto,
                "source_act_parceiro": source_act_parceiro,
                "memorando_gold": memorando_gold,
                "memorando_json_path": memorando_row.get("json_path", "") if memorando_gold else "",
                "ted_quality": ted_quality,
                "ted_gold": ted_gold,
                "ted_json_path": ted_row.get("json_path", "") if ted_gold else "",
                "ted_objeto": _clean_spaces(str(ted_api_payload.get("objeto", "") or "")) if ted_gold else "",
                "ted_valor_global": _clean_spaces(str(ted_api_payload.get("valor_global", "") or "")) if ted_gold else "",
                "ted_situacao": _clean_spaces(str(ted_api_payload.get("situacao", "") or "")) if ted_gold else "",
                "ted_uf": _clean_spaces(str(ted_api_payload.get("uf", "") or "")) if ted_gold else "",
                "quality_status": _overall_quality_status(
                    pt_quality=pt_quality,
                    act_quality=act_quality,
                    memorando_gold=memorando_gold,
                    ted_gold=ted_gold,
                    has_process_mismatch=has_process_mismatch,
                ),
                "quality_notes": "; ".join(note for note in notes if note),
                "act_attempts_count": len(act_attempt_rows),
                "act_rejection_summary": act_rejection_summary,
            }
        )
        rows.append(dashboard_row)
        divergence_rows.append(
            {
                "processo": processo,
                "quality_status": dashboard_row["quality_status"],
                "pt_quality": pt_quality,
                "act_quality": act_quality,
                "ted_quality": ted_quality,
                "act_chosen_documento": act_row.get("candidate_json_path", "") or act_row.get("json_path", ""),
                "act_attempts_count": len(act_attempt_rows),
                "act_rejection_summary": act_rejection_summary,
                "act_missing_fields": ",".join(act_missing),
                "preview_numero_act": _clean_spaces(preview.get("numero_act", "")),
                "act_numero_acordo": act_row.get("numero_acordo", "") if act_gold else "",
                "preview_vigencia": _clean_spaces(preview.get("vigencia", "")),
                "pt_vigencia": _period_label(pt_row),
                "act_vigencia": _period_label(act_row),
                "preview_parceiro": preview_partner,
                "act_orgao_convenente": act_row.get("orgao_convenente", "") if act_gold else "",
                "quality_notes": dashboard_row["quality_notes"],
            }
        )

    columns = [
        "processo",
        "preview_parceiro",
        "preview_numero_act",
        "preview_objeto",
        "preview_vigencia",
        "pt_gold",
        "pt_json_path",
        "pt_vigencia_inicio",
        "pt_vigencia_fim",
        "pt_quality",
        "act_gold",
        "act_json_path",
        "act_numero_acordo",
        "act_data_inicio_vigencia",
        "act_data_fim_vigencia",
        "act_orgao_convenente",
        "act_objeto",
        "act_quality",
        "has_process_mismatch",
        "source_act_objeto",
        "source_act_parceiro",
        "memorando_gold",
        "memorando_json_path",
        "ted_quality",
        "ted_gold",
        "ted_json_path",
        "ted_objeto",
        "ted_valor_global",
        "ted_situacao",
        "ted_uf",
        "quality_status",
        "quality_notes",
        "act_attempts_count",
        "act_rejection_summary",
    ]
    csv_path = output_dir / "dashboard_ready_latest.csv"
    csv_writer.write_csv(rows, csv_path, columns=columns)
    divergence_columns = [
        "processo",
        "quality_status",
        "pt_quality",
        "act_quality",
        "ted_quality",
        "act_chosen_documento",
        "act_attempts_count",
        "act_rejection_summary",
        "act_missing_fields",
        "preview_numero_act",
        "act_numero_acordo",
        "preview_vigencia",
        "pt_vigencia",
        "act_vigencia",
        "preview_parceiro",
        "act_orgao_convenente",
        "quality_notes",
    ]
    divergence_path = output_dir / "divergence_matrix_latest.csv"
    csv_writer.write_csv(divergence_rows, divergence_path, columns=divergence_columns)
    _log(logger, "info", "Dashboard exporter: arquivo gerado com %d linha(s) em %s.", len(rows), csv_path)
    return {
        "records": len(rows),
        "csv_path": csv_path,
        "latest_path": csv_path,
        "divergence_path": divergence_path,
    }
