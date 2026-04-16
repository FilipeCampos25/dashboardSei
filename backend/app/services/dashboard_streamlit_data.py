from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

DEFAULT_LOG_LIMIT = 20

OVERVIEW_COLUMNS = [
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

PT_DETAIL_COLUMNS = [
    "captured_at",
    "requested_type",
    "resolved_document_type",
    "processo",
    "documento",
    "parceiro",
    "vigencia_raw",
    "vigencia_inicio",
    "vigencia_fim",
    "objeto",
    "atribuicoes_raw",
    "metas_raw",
    "acoes_raw",
    "prazo_inicio_raw",
    "prazo_inicio",
    "prazo_fim_raw",
    "prazo_fim",
    "period_source",
    "period_warning",
    "selection_reason",
    "classification_reason",
    "validation_status",
    "publication_status",
    "snapshot_mode",
    "preview_numero_act",
    "normalization_status",
    "captured_focus_fields",
    "json_path",
]

PT_ATTRIBUICOES_ALIASES = ("atribuicoes_raw", "atribuições_raw")


def _empty_dataframe(columns: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _read_csv(path: Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return _empty_dataframe(columns or [])
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return _empty_dataframe(columns or [])
    if columns:
        for column in columns:
            if column not in df.columns:
                df[column] = ""
        df = df[list(columns)]
    return df


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_json_lines(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    entries: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as file_obj:
            for raw_line in file_obj:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    entries.append(payload)
    except Exception:
        return []
    return entries


def _clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _to_bool(value: Any) -> bool:
    normalized = _clean_spaces(value).lower()
    return normalized in {"1", "true", "sim", "yes"}


def _to_int(value: Any) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(str(value or "").replace(",", ".").strip())
    except Exception:
        return 0.0


def _resolve_json_path(raw_path: Any, backend_output_dir: Path, root_dir: Path) -> Path | None:
    cleaned = _clean_spaces(raw_path)
    if not cleaned:
        return None
    candidate = Path(cleaned)
    if candidate.exists():
        return candidate
    fallback_by_name = backend_output_dir / candidate.name
    if fallback_by_name.exists():
        return fallback_by_name
    fallback_relative = root_dir / candidate
    if fallback_relative.exists():
        return fallback_relative
    return None


def _excerpt(value: Any, limit: int = 360) -> str:
    text = _clean_spaces(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _json_snapshot_text(path: Path | None) -> str:
    if path is None:
        return ""
    payload = _read_json(path)
    snapshot = payload.get("snapshot", {})
    if not isinstance(snapshot, dict):
        return ""
    return _clean_spaces(snapshot.get("text", ""))


def _json_api_payload(path: Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    payload = _read_json(path)
    snapshot = payload.get("snapshot", {})
    if not isinstance(snapshot, dict):
        return {}
    api_payload = snapshot.get("api_payload", {})
    return api_payload if isinstance(api_payload, dict) else {}


def _ensure_pt_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in PT_DETAIL_COLUMNS:
        if column not in result.columns:
            result[column] = ""

    for alias in PT_ATTRIBUICOES_ALIASES:
        if alias in result.columns:
            result["atribuicoes_raw"] = result[alias]
            break
    else:
        result["atribuicoes_raw"] = ""
    result["atribuições_raw"] = result["atribuicoes_raw"]

    result["captured_focus_fields"] = result["captured_focus_fields"].apply(_to_int)
    for column in ("vigencia_inicio", "vigencia_fim", "prazo_inicio", "prazo_fim"):
        result[column] = pd.to_datetime(result[column], errors="coerce")
    result["has_metas"] = result["metas_raw"].apply(lambda value: bool(_clean_spaces(value)))
    result["has_acoes"] = result["acoes_raw"].apply(lambda value: bool(_clean_spaces(value)))
    result["has_prazo_estruturado"] = result["prazo_inicio"].notna() & result["prazo_fim"].notna()
    return result


def _prepare_overview_df(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in OVERVIEW_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    result = result[OVERVIEW_COLUMNS]
    for column in ("pt_gold", "act_gold", "memorando_gold", "ted_gold", "has_process_mismatch"):
        result[column] = result[column].apply(_to_bool)
    result["act_attempts_count"] = result["act_attempts_count"].apply(_to_int)
    result["ted_valor_global_num"] = result["ted_valor_global"].apply(_to_float)
    result["pt_present"] = result["pt_quality"].apply(lambda value: _clean_spaces(value) not in {"", "not_found"})
    result["act_present"] = result["act_quality"].apply(lambda value: _clean_spaces(value) not in {"", "not_found"})
    result["memorando_present"] = result["memorando_gold"]
    result["ted_present"] = result["ted_gold"]
    for column in ("pt_vigencia_inicio", "pt_vigencia_fim", "act_data_inicio_vigencia", "act_data_fim_vigencia"):
        result[column] = pd.to_datetime(result[column], errors="coerce")
    return result


def _prepare_status_df(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "found" in result.columns:
        result["found"] = result["found"].apply(_to_bool)
    if "results_count" in result.columns:
        result["results_count"] = result["results_count"].apply(_to_int)
    if "text_chars" in result.columns:
        result["text_chars"] = result["text_chars"].apply(_to_int)
    if "tables_count" in result.columns:
        result["tables_count"] = result["tables_count"].apply(_to_int)
    return result


def dashboard_source_paths(root_dir: Path) -> List[Path]:
    backend_output_dir = root_dir / "backend" / "output"
    return [
        backend_output_dir / "dashboard_ready_latest.csv",
        backend_output_dir / "pt_normalizado_latest.csv",
        backend_output_dir / "pt_auditoria_latest.csv",
        backend_output_dir / "act_normalizado_latest.csv",
        backend_output_dir / "memorando_normalizado_latest.csv",
        backend_output_dir / "ted_normalizado_latest.csv",
        backend_output_dir / "pt_status_execucao_latest.csv",
        backend_output_dir / "act_status_execucao_latest.csv",
        backend_output_dir / "memorando_status_execucao_latest.csv",
        backend_output_dir / "ted_status_execucao_latest.csv",
        backend_output_dir / "performance_analysis.json",
        root_dir / "output" / "execution_log_latest.json",
    ]


def build_file_signature(paths: Iterable[Path]) -> tuple[tuple[str, bool, int, int], ...]:
    signature: List[tuple[str, bool, int, int]] = []
    for path in paths:
        if path.exists():
            stat = path.stat()
            signature.append((str(path), True, int(stat.st_mtime_ns), int(stat.st_size)))
        else:
            signature.append((str(path), False, 0, 0))
    return tuple(signature)


def load_dashboard_bundle(root_dir: Path) -> Dict[str, Any]:
    backend_output_dir = root_dir / "backend" / "output"
    overview_df = _prepare_overview_df(_read_csv(backend_output_dir / "dashboard_ready_latest.csv", OVERVIEW_COLUMNS))
    pt_normalized_df = _ensure_pt_columns(_read_csv(backend_output_dir / "pt_normalizado_latest.csv"))
    pt_audit_df = _ensure_pt_columns(_read_csv(backend_output_dir / "pt_auditoria_latest.csv"))
    pt_status_df = _prepare_status_df(_read_csv(backend_output_dir / "pt_status_execucao_latest.csv"))
    act_normalized_df = _read_csv(backend_output_dir / "act_normalizado_latest.csv")
    act_status_df = _prepare_status_df(_read_csv(backend_output_dir / "act_status_execucao_latest.csv"))
    memorando_normalized_df = _read_csv(backend_output_dir / "memorando_normalizado_latest.csv")
    memorando_status_df = _prepare_status_df(_read_csv(backend_output_dir / "memorando_status_execucao_latest.csv"))
    ted_normalized_df = _read_csv(backend_output_dir / "ted_normalizado_latest.csv")
    ted_status_df = _prepare_status_df(_read_csv(backend_output_dir / "ted_status_execucao_latest.csv"))
    performance = _read_json(backend_output_dir / "performance_analysis.json")
    log_entries = _read_json_lines(root_dir / "output" / "execution_log_latest.json")
    return {
        "root_dir": root_dir,
        "backend_output_dir": backend_output_dir,
        "overview": overview_df,
        "pt_normalized": pt_normalized_df,
        "pt_audit": pt_audit_df,
        "pt_status": pt_status_df,
        "act_normalized": act_normalized_df,
        "act_status": act_status_df,
        "memorando_normalized": memorando_normalized_df,
        "memorando_status": memorando_status_df,
        "ted_normalized": ted_normalized_df,
        "ted_status": ted_status_df,
        "performance": performance,
        "log_entries": log_entries,
    }


def summarize_log_entries(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"info": 0, "warning": 0, "error": 0, "total": len(entries)}
    for entry in entries:
        level = _clean_spaces(entry.get("level", "")).lower()
        if level == "info":
            summary["info"] += 1
        elif level == "warning":
            summary["warning"] += 1
        elif level == "error":
            summary["error"] += 1
    return summary


def latest_log_rows(entries: List[Dict[str, Any]], limit: int = DEFAULT_LOG_LIMIT) -> pd.DataFrame:
    rows = [
        {
            "timestamp": _clean_spaces(entry.get("timestamp", "")),
            "level": _clean_spaces(entry.get("level", "")),
            "module": _clean_spaces(entry.get("module", "")),
            "message": _clean_spaces(entry.get("message", "")),
        }
        for entry in entries[-max(limit, 0):]
    ]
    return pd.DataFrame(rows)


def runtime_for_processes(performance: Dict[str, Any], processes: Iterable[str]) -> Dict[str, float]:
    selected = [_clean_spaces(process) for process in processes if _clean_spaces(process)]
    total_default = float(performance.get("total_execution_time") or 0.0)
    if not selected:
        return {
            "total_seconds": total_default,
            "total_minutes": total_default / 60 if total_default else 0.0,
            "avg_seconds": 0.0,
        }

    spans = performance.get("spans", {})
    total = 0.0
    counted = 0
    for process in selected:
        span = spans.get(f"processo:{process}", {})
        seconds = float(span.get("total_seconds") or 0.0)
        if seconds > 0:
            total += seconds
            counted += 1
    if counted == 0:
        total = total_default
        counted = len(selected)
    avg = total / counted if counted else 0.0
    return {
        "total_seconds": total,
        "total_minutes": total / 60 if total else 0.0,
        "avg_seconds": avg,
    }


def filter_overview_df(
    overview_df: pd.DataFrame,
    *,
    processos: List[str] | None = None,
    parceiros: List[str] | None = None,
    quality_statuses: List[str] | None = None,
    has_pt: str = "Todos",
    has_act: str = "Todos",
    has_memorando: str = "Todos",
    has_ted: str = "Todos",
) -> pd.DataFrame:
    filtered = overview_df.copy()
    if processos:
        filtered = filtered[filtered["processo"].isin(processos)]
    if parceiros:
        filtered = filtered[filtered["preview_parceiro"].isin(parceiros)]
    if quality_statuses:
        filtered = filtered[filtered["quality_status"].isin(quality_statuses)]

    def apply_presence(df: pd.DataFrame, column: str, mode: str) -> pd.DataFrame:
        normalized = _clean_spaces(mode).lower()
        if normalized == "com":
            return df[df[column]]
        if normalized == "sem":
            return df[~df[column]]
        return df

    filtered = apply_presence(filtered, "pt_present", has_pt)
    filtered = apply_presence(filtered, "act_present", has_act)
    filtered = apply_presence(filtered, "memorando_present", has_memorando)
    filtered = apply_presence(filtered, "ted_present", has_ted)
    return filtered


def filter_by_processes(df: pd.DataFrame, processes: Iterable[str]) -> pd.DataFrame:
    selected = {_clean_spaces(process) for process in processes if _clean_spaces(process)}
    if not selected or "processo" not in df.columns:
        return df.copy()
    return df[df["processo"].isin(selected)].copy()


def pt_detail_dataframe(bundle: Dict[str, Any]) -> pd.DataFrame:
    audit_df = bundle.get("pt_audit", _empty_dataframe([]))
    if not audit_df.empty:
        return audit_df.copy()
    return bundle.get("pt_normalized", _empty_dataframe([])).copy()


def explode_pt_metas(pt_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in pt_df.to_dict(orient="records"):
        processo = _clean_spaces(record.get("processo", ""))
        parceiro = _clean_spaces(record.get("parceiro", ""))
        raw_value = str(record.get("metas_raw", "") or "")
        for item in [part.strip() for part in raw_value.split("||") if part.strip()]:
            parts = [_clean_spaces(part) for part in item.split("|") if _clean_spaces(part)]
            meta_ref = ""
            if parts and re.fullmatch(r"\d+[.)]?", parts[0]):
                meta_ref = parts[0]
                parts = parts[1:]
            rows.append(
                {
                    "processo": processo,
                    "parceiro": parceiro,
                    "meta_ref": meta_ref,
                    "meta_text": " | ".join(parts) or item,
                }
            )
    return pd.DataFrame(rows)


def _looks_like_period_token(value: str) -> bool:
    normalized = _clean_spaces(value).lower()
    if not normalized:
        return False
    markers = (
        "janeiro",
        "fevereiro",
        "marco",
        "abril",
        "maio",
        "junho",
        "julho",
        "agosto",
        "setembro",
        "outubro",
        "novembro",
        "dezembro",
        "inicio",
        "termino",
        "semestre",
        "anual",
        "mensal",
        "cada semestre",
    )
    return any(marker in normalized for marker in markers) or bool(re.search(r"\d{4}", normalized))


def explode_pt_acoes(pt_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in pt_df.to_dict(orient="records"):
        processo = _clean_spaces(record.get("processo", ""))
        parceiro = _clean_spaces(record.get("parceiro", ""))
        raw_value = str(record.get("acoes_raw", "") or "")
        for item in [part.strip() for part in raw_value.split("||") if part.strip()]:
            parts = [_clean_spaces(part) for part in item.split("|") if _clean_spaces(part)]
            acao_ref = ""
            if parts and re.fullmatch(r"\d+[.)]?", parts[0]):
                acao_ref = parts[0]
                parts = parts[1:]

            period_index = next((idx for idx, value in enumerate(parts) if _looks_like_period_token(value)), -1)
            responsavel = ""
            periodo_raw = ""
            descricao_parts = parts
            if period_index >= 0:
                periodo_raw = " | ".join(parts[period_index:])
                if period_index > 0:
                    responsavel = parts[period_index - 1]
                    descricao_parts = parts[: period_index - 1]
                else:
                    descricao_parts = []
            elif len(parts) >= 3:
                responsavel = parts[-1]
                descricao_parts = parts[:-1]

            descricao = " | ".join(descricao_parts).strip(" |")
            if not descricao and parts:
                descricao = parts[0]
            rows.append(
                {
                    "processo": processo,
                    "parceiro": parceiro,
                    "acao_ref": acao_ref,
                    "descricao": descricao,
                    "responsavel": responsavel,
                    "periodo_raw": periodo_raw,
                    "acao_texto_original": item,
                }
            )
    return pd.DataFrame(rows)


def pt_process_metrics(pt_df: pd.DataFrame) -> pd.DataFrame:
    metas_df = explode_pt_metas(pt_df)
    acoes_df = explode_pt_acoes(pt_df)
    metrics = pt_df[["processo"]].drop_duplicates().copy()
    if metas_df.empty:
        metrics["metas_count"] = 0
    else:
        metrics = metrics.merge(metas_df.groupby("processo").size().rename("metas_count"), on="processo", how="left")
    if acoes_df.empty:
        metrics["acoes_count"] = 0
    else:
        metrics = metrics.merge(acoes_df.groupby("processo").size().rename("acoes_count"), on="processo", how="left")
    metrics["metas_count"] = metrics["metas_count"].fillna(0).astype(int)
    metrics["acoes_count"] = metrics["acoes_count"].fillna(0).astype(int)
    return metrics


def parse_act_rejection_summary(overview_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    pattern = re.compile(r"^(?P<label>.+?)\((?P<count>\d+)\)$")
    for record in overview_df.to_dict(orient="records"):
        processo = _clean_spaces(record.get("processo", ""))
        summary = str(record.get("act_rejection_summary", "") or "")
        for item in [part.strip() for part in summary.split("|") if part.strip()]:
            match = pattern.match(item)
            if not match:
                continue
            rows.append(
                {
                    "processo": processo,
                    "rejection": _clean_spaces(match.group("label")),
                    "count": int(match.group("count")),
                }
            )
    return pd.DataFrame(rows)


def memorando_detail_dataframe(bundle: Dict[str, Any]) -> pd.DataFrame:
    root_dir = bundle["root_dir"]
    backend_output_dir = bundle["backend_output_dir"]
    normalized_df = bundle.get("memorando_normalized", _empty_dataframe([]))
    rows: List[Dict[str, Any]] = []
    for record in normalized_df.to_dict(orient="records"):
        json_path = _resolve_json_path(record.get("json_path", ""), backend_output_dir, root_dir)
        rows.append(
            {
                "processo": _clean_spaces(record.get("processo", "")),
                "documento": _clean_spaces(record.get("documento", "")),
                "snapshot_mode": _clean_spaces(record.get("snapshot_mode", "")),
                "excerpt": _excerpt(_json_snapshot_text(json_path)),
                "json_path": str(json_path) if json_path is not None else _clean_spaces(record.get("json_path", "")),
            }
        )
    return pd.DataFrame(rows)


def ted_detail_dataframe(bundle: Dict[str, Any]) -> pd.DataFrame:
    root_dir = bundle["root_dir"]
    backend_output_dir = bundle["backend_output_dir"]
    normalized_df = bundle.get("ted_normalized", _empty_dataframe([]))
    rows: List[Dict[str, Any]] = []
    for record in normalized_df.to_dict(orient="records"):
        json_path = _resolve_json_path(record.get("json_path", ""), backend_output_dir, root_dir)
        api_payload = _json_api_payload(json_path)
        rows.append(
            {
                "processo": _clean_spaces(record.get("processo", "")),
                "objeto": _clean_spaces(api_payload.get("objeto", "")),
                "valor_global": _clean_spaces(api_payload.get("valor_global", "")),
                "valor_global_num": _to_float(api_payload.get("valor_global", "")),
                "situacao": _clean_spaces(api_payload.get("situacao", "")),
                "uf": _clean_spaces(api_payload.get("uf", "")),
                "json_path": str(json_path) if json_path is not None else _clean_spaces(record.get("json_path", "")),
            }
        )
    return pd.DataFrame(rows)


def process_explorer_payload(bundle: Dict[str, Any], processo: str) -> Dict[str, Any]:
    process_id = _clean_spaces(processo)
    overview_rows = filter_by_processes(bundle.get("overview", _empty_dataframe([])), [process_id]).to_dict(orient="records")
    pt_rows = filter_by_processes(pt_detail_dataframe(bundle), [process_id]).to_dict(orient="records")
    act_rows = filter_by_processes(bundle.get("act_normalized", _empty_dataframe([])), [process_id]).to_dict(orient="records")
    memorando_rows = filter_by_processes(bundle.get("memorando_normalized", _empty_dataframe([])), [process_id]).to_dict(orient="records")
    ted_rows = filter_by_processes(ted_detail_dataframe(bundle), [process_id]).to_dict(orient="records")
    return {
        "overview": overview_rows[0] if overview_rows else {},
        "pt": pt_rows[0] if pt_rows else {},
        "act": act_rows[0] if act_rows else {},
        "memorando": memorando_rows[0] if memorando_rows else {},
        "ted": ted_rows[0] if ted_rows else {},
    }
