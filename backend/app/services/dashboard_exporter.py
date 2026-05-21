from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from app.output import csv_writer
from app.services.act_normalizer import (
    PUBLICATION_STATUS_GOLD as ACT_PUBLICATION_STATUS_GOLD,
    VALIDATION_STATUS_VALID,
)
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


PROCESSO_RE = re.compile(r"^\d{5}\.\d{6}/\d{4}-\d{2}$")


def _normalize_processo(value: str) -> str:
    cleaned = _clean_spaces(value)
    if not cleaned:
        return ""
    compact = re.sub(r"\s+", "", cleaned)
    compact = compact.translate(
        str.maketrans(
            {
                "\u2010": "-",
                "\u2011": "-",
                "\u2012": "-",
                "\u2013": "-",
                "\u2014": "-",
                "\u2212": "-",
                "\ufe58": "-",
                "\ufe63": "-",
                "\uff0d": "-",
                "\u2044": "/",
                "\u2215": "/",
                "\uff0f": "/",
            }
        )
    )
    if PROCESSO_RE.fullmatch(compact):
        return compact
    digits = re.sub(r"\D", "", compact)
    if len(digits) == 17:
        return f"{digits[:5]}.{digits[5:11]}/{digits[11:15]}-{digits[15:]}"
    return compact


def _is_valid_processo(value: str) -> bool:
    return bool(PROCESSO_RE.fullmatch(_normalize_processo(value)))


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
        processo = _normalize_processo(row.get("processo", ""))
        if not processo or processo in seen:
            continue
        row = {**row, "processo": processo}
        seen.add(processo)
        unique.append(row)
    return unique


def _is_ted_gold(row: Dict[str, str]) -> bool:
    return (
        bool(_clean_spaces(row.get("json_path", "")))
        and row.get("publication_status", "") == ACT_PUBLICATION_STATUS_GOLD
        and row.get("validation_status", "") == VALIDATION_STATUS_VALID
    )


def _dashboard_base_rows(output_dir: Path, ted_rows: List[Dict[str, str]], logger: Any = None) -> List[Dict[str, str]]:
    rows = _preview_rows(output_dir, logger=logger)
    seen = {_normalize_processo(row.get("processo", "")) for row in rows if _normalize_processo(row.get("processo", ""))}
    for ted_row in ted_rows:
        if not _is_ted_gold(ted_row):
            continue
        processo = _normalize_processo(ted_row.get("processo", ""))
        if not processo or processo in seen:
            continue
        seen.add(processo)
        rows.append(
            {
                "interno_descricao": "TED",
                "seq": "",
                "processo": processo,
                "parceiro": "",
                "vigencia": "",
                "numero_act": "",
                "objeto": "",
            }
        )
    return rows


def _safe_int(value: str) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _is_valid_numero_acordo(value: str) -> bool:
    cleaned = _clean_spaces(value)
    if not cleaned:
        return False
    normalized = re.sub(r"\s+", "", cleaned.lower())
    normalized = normalized.replace("º", "o").replace("°", "o")
    if re.fullmatch(r"x+[/.-]?(?:x+|\d*x+\d*)", normalized):
        return False
    if re.fullmatch(r"s[/.-]?n(?:o|umero)?", normalized):
        return False
    return not any(marker in normalized for marker in ("semnumero", "semnúmero", "xxxxx", "xx/20xx"))


def _confidence(source: str) -> str:
    return {
        "act_gold": "high",
        "pt_gold": "high",
        "pt_silver_structured": "medium",
        "preview_fallback": "low",
        "missing": "missing",
    }.get(source, "low")


def _value_source(value: str, source: str) -> Dict[str, str]:
    return {"value": _clean_spaces(value), "source": source, "confidence": _confidence(source)}


def _group_rows(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        processo = _normalize_processo(row.get("processo", ""))
        if processo:
            grouped.setdefault(processo, []).append({**row, "processo": processo})
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
    normalized_payload = {
        "objeto": _clean_spaces(row.get("objeto", "")),
        "valor_global": _clean_spaces(row.get("valor_global", "")),
        "situacao": _clean_spaces(row.get("situacao", "")),
        "uf": _clean_spaces(row.get("uf", "")),
    }
    if any(normalized_payload.values()):
        return normalized_payload
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


def _best_ted_row(rows: List[Dict[str, str]]) -> Dict[str, str]:
    if not rows:
        return {}
    return max(
        rows,
        key=lambda row: (
            _is_ted_gold(row),
            row.get("validation_status", "") == VALIDATION_STATUS_VALID,
            bool(_clean_spaces(row.get("json_path", ""))),
        ),
    )


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
    if ted_row and not _is_ted_gold(ted_row):
        return (
            _clean_spaces(ted_row.get("validation_status", ""))
            or _clean_spaces(ted_row.get("quality_status", ""))
            or "silver_only"
        )
    if ted_row.get("quality_status"):
        return _clean_spaces(ted_row.get("quality_status", ""))
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


def _best_numero(preview: Dict[str, str], act_row: Dict[str, str], act_gold: bool) -> Dict[str, str]:
    act_numero = _clean_spaces(act_row.get("numero_acordo", "")) if act_gold else ""
    if _is_valid_numero_acordo(act_numero):
        return _value_source(act_numero, "act_gold")
    preview_numero = _clean_spaces(preview.get("numero_act", ""))
    if _is_valid_numero_acordo(preview_numero):
        return _value_source(preview_numero, "preview_fallback")
    return _value_source("", "missing")


def _best_partner(
    preview: Dict[str, str],
    pt_row: Dict[str, str],
    act_row: Dict[str, str],
    *,
    act_gold: bool,
    pt_gold: bool,
) -> Dict[str, str]:
    act_partner = _clean_spaces(act_row.get("orgao_convenente", "")) if act_gold else ""
    if act_partner:
        return _value_source(act_partner, "act_gold")
    pt_partner = _clean_spaces(pt_row.get("parceiro", ""))
    if pt_partner:
        return _value_source(pt_partner, "pt_gold" if pt_gold else "pt_silver_structured")
    preview_partner = _clean_spaces(preview.get("parceiro", ""))
    if preview_partner:
        return _value_source(preview_partner, "preview_fallback")
    return _value_source("", "missing")


def _best_object(
    preview: Dict[str, str],
    pt_row: Dict[str, str],
    act_row: Dict[str, str],
    *,
    act_gold: bool,
    pt_gold: bool,
) -> Dict[str, str]:
    act_objeto = _clean_spaces(act_row.get("objeto", "")) if act_gold else ""
    if act_objeto:
        return _value_source(act_objeto, "act_gold")
    pt_objeto = _clean_spaces(pt_row.get("objeto", ""))
    if pt_objeto:
        return _value_source(pt_objeto, "pt_gold" if pt_gold else "pt_silver_structured")
    preview_objeto = _clean_spaces(preview.get("objeto", ""))
    if preview_objeto:
        return _value_source(preview_objeto, "preview_fallback")
    return _value_source("", "missing")


def _best_vigencia(preview: Dict[str, str], pt_row: Dict[str, str], act_row: Dict[str, str], *, act_gold: bool, pt_gold: bool) -> Dict[str, str]:
    act_start = _clean_spaces(act_row.get("data_inicio_vigencia", "")) if act_gold else ""
    act_end = _clean_spaces(act_row.get("data_fim_vigencia", "")) if act_gold else ""
    if act_start and act_end:
        return {"inicio": act_start, "fim": act_end, "raw": _period_label(act_row), "source": "act_gold", "confidence": "high"}

    pt_start = _clean_spaces(pt_row.get("vigencia_inicio", ""))
    pt_end = _clean_spaces(pt_row.get("vigencia_fim", ""))
    if pt_start and pt_end:
        source = "pt_gold" if pt_gold else "pt_silver_structured"
        return {"inicio": pt_start, "fim": pt_end, "raw": _period_label(pt_row), "source": source, "confidence": _confidence(source)}

    preview_raw = _clean_spaces(preview.get("vigencia", ""))
    if preview_raw:
        return {"inicio": "", "fim": "", "raw": preview_raw, "source": "preview_fallback", "confidence": "low"}
    return {"inicio": "", "fim": "", "raw": "", "source": "missing", "confidence": "missing"}


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
    ted_rows = _read_csv_rows(output_dir / "ted_normalizado_latest.csv", logger=logger)
    preview_rows = _dashboard_base_rows(output_dir, ted_rows, logger=logger)
    if not preview_rows:
        _log(logger, "info", "Dashboard exporter: nenhum preview encontrado em %s.", output_dir)
        return {"records": 0, "csv_path": None}

    pt_grouped = _group_rows(_read_csv_rows(output_dir / "pt_auditoria_latest.csv", logger=logger))
    act_grouped = _group_rows(_read_csv_rows(output_dir / "act_classificacao_latest.csv", logger=logger))
    act_status_grouped = _group_rows(_read_csv_rows(output_dir / "act_status_execucao_latest.csv", logger=logger))
    memorando_grouped = _group_rows(_read_csv_rows(output_dir / "memorando_normalizado_latest.csv", logger=logger))
    ted_grouped = _group_rows(ted_rows)
    ted_status_grouped = _group_rows(_read_csv_rows(output_dir / "ted_status_execucao_latest.csv", logger=logger))

    rows: List[Dict[str, Any]] = []
    divergence_rows: List[Dict[str, Any]] = []
    for preview in preview_rows:
        processo = _normalize_processo(preview.get("processo", ""))
        source_universe = "parcerias_vigentes" if _clean_spaces(preview.get("interno_descricao", "")) != "TED" else "ted_normalizado"
        pt_row = _best_pt_row(pt_grouped.get(processo, [])) if pt_grouped.get(processo) else {}
        act_row = _best_act_row(act_grouped.get(processo, [])) if act_grouped.get(processo) else {}
        act_attempt_rows = act_status_grouped.get(processo, [])
        memorando_row = _first_row(memorando_grouped.get(processo, []))
        ted_row = _best_ted_row(ted_grouped.get(processo, []))
        ted_status_rows = ted_status_grouped.get(processo, [])
        ted_gold = _is_ted_gold(ted_row)
        ted_api_payload = _ted_payload(ted_row, output_dir, logger=logger) if ted_gold else {}

        pt_quality = _pt_quality(pt_row)
        act_quality = _act_quality(act_row)
        ted_quality = _ted_quality(ted_row, ted_status_rows)
        pt_gold = pt_row.get("publication_status", "") == PT_PUBLICATION_STATUS_GOLD
        act_gold = act_row.get("publication_status", "") == ACT_PUBLICATION_STATUS_GOLD
        memorando_gold = bool(memorando_row.get("json_path", ""))
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

        best_numero = _best_numero(preview, act_row, act_gold)
        best_parceiro = _best_partner(preview, pt_row, act_row, act_gold=act_gold, pt_gold=pt_gold)
        best_vigencia = _best_vigencia(preview, pt_row, act_row, act_gold=act_gold, pt_gold=pt_gold)
        best_objeto = _best_object(preview, pt_row, act_row, act_gold=act_gold, pt_gold=pt_gold)

        notes: List[str] = []
        issues: List[str] = []
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
        if _clean_spaces(act_row.get("validation_warning", "")):
            for warning in act_row.get("validation_warning", "").split(";"):
                warning = _clean_spaces(warning)
                if warning and warning not in issues:
                    issues.append(warning)
        if act_gold and not _is_valid_numero_acordo(act_row.get("numero_acordo", "")) and act_row.get("numero_acordo", ""):
            issues.append("numero_placeholder")
        if best_numero["source"] == "missing":
            issues.append("numero_missing")
        if best_vigencia["source"] == "preview_fallback" and not (best_vigencia["inicio"] and best_vigencia["fim"]):
            issues.append("vigencia_sem_data_base")
        if best_parceiro["source"] == "preview_fallback":
            issues.append("parceiro_preview_fallback")
        if best_objeto["source"] == "preview_fallback":
            issues.append("objeto_preview_fallback")
        if act_quality == "silver_only":
            issues.append("act_silver_only")
        if not _is_valid_processo(processo):
            issues.append("processo_invalido")
        if ted_row and not ted_gold:
            issues.append(f"ted_ignored={ted_quality}")

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
                "best_numero_acordo": best_numero["value"],
                "best_numero_acordo_source": best_numero["source"],
                "best_numero_acordo_confidence": best_numero["confidence"],
                "best_parceiro": best_parceiro["value"],
                "best_parceiro_source": best_parceiro["source"],
                "best_parceiro_confidence": best_parceiro["confidence"],
                "best_vigencia_inicio": best_vigencia["inicio"],
                "best_vigencia_fim": best_vigencia["fim"],
                "best_vigencia_raw": best_vigencia["raw"],
                "best_vigencia_source": best_vigencia["source"],
                "best_vigencia_confidence": best_vigencia["confidence"],
                "best_objeto": best_objeto["value"],
                "best_objeto_source": best_objeto["source"],
                "best_objeto_confidence": best_objeto["confidence"],
                "normalization_issues": "; ".join(dict.fromkeys(issue for issue in issues if issue)),
            }
        )
        rows.append(dashboard_row)
        divergence_rows.append(
            {
                "processo": processo,
                "source_universe": source_universe,
                "dashboard_join_key": processo,
                "process_key_valid": _is_valid_processo(processo),
                "quality_status": dashboard_row["quality_status"],
                "pt_quality": pt_quality,
                "act_quality": act_quality,
                "ted_quality": ted_quality,
                "ted_gold": ted_gold,
                "ted_json_path": ted_row.get("json_path", "") if ted_gold else "",
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
                "best_numero_acordo": dashboard_row["best_numero_acordo"],
                "best_numero_acordo_source": dashboard_row["best_numero_acordo_source"],
                "best_parceiro": dashboard_row["best_parceiro"],
                "best_parceiro_source": dashboard_row["best_parceiro_source"],
                "best_vigencia": _period_label({"vigencia_inicio": dashboard_row["best_vigencia_inicio"], "vigencia_fim": dashboard_row["best_vigencia_fim"]})
                or dashboard_row["best_vigencia_raw"],
                "best_vigencia_source": dashboard_row["best_vigencia_source"],
                "normalization_issues": dashboard_row["normalization_issues"],
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
        "best_numero_acordo",
        "best_numero_acordo_source",
        "best_numero_acordo_confidence",
        "best_parceiro",
        "best_parceiro_source",
        "best_parceiro_confidence",
        "best_vigencia_inicio",
        "best_vigencia_fim",
        "best_vigencia_raw",
        "best_vigencia_source",
        "best_vigencia_confidence",
        "best_objeto",
        "best_objeto_source",
        "best_objeto_confidence",
        "normalization_issues",
    ]
    csv_path = output_dir / "dashboard_ready_latest.csv"
    csv_writer.write_csv(rows, csv_path, columns=columns)
    divergence_columns = [
        "processo",
        "source_universe",
        "dashboard_join_key",
        "process_key_valid",
        "quality_status",
        "pt_quality",
        "act_quality",
        "ted_quality",
        "ted_gold",
        "ted_json_path",
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
        "best_numero_acordo",
        "best_numero_acordo_source",
        "best_parceiro",
        "best_parceiro_source",
        "best_vigencia",
        "best_vigencia_source",
        "normalization_issues",
        "quality_notes",
    ]
    divergence_path = output_dir / "divergence_matrix_latest.csv"
    csv_writer.write_csv(divergence_rows, divergence_path, columns=divergence_columns)
    review_queue_path = None
    try:
        from app.services.normalization_review import export_review_queue

        review_result = export_review_queue(output_dir, logger=logger)
        review_queue_path = review_result.get("latest_path")
    except Exception as exc:
        _log(logger, "warning", "Dashboard exporter: falha ao gerar normalization_review_queue_latest.csv (%s).", exc)
    _log(logger, "info", "Dashboard exporter: arquivo gerado com %d linha(s) em %s.", len(rows), csv_path)
    return {
        "records": len(rows),
        "csv_path": csv_path,
        "latest_path": csv_path,
        "divergence_path": divergence_path,
        "review_queue_path": review_queue_path,
    }
