from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .dashboard_metrics import clean_spaces


OVERVIEW_COLUMNS = [
    "processo",
    "source_universe",
    "act_numero",
    "ted_numero",
    "ted_ano",
    "documento_principal_tipo",
    "documento_principal_numero",
    "preview_parceiro",
    "preview_numero_act",
    "preview_objeto",
    "preview_vigencia",
    "pt_gold",
    "pt_json_path",
    "pt_data_assinatura",
    "pt_vigencia_inicio",
    "pt_vigencia_fim",
    "pt_quality",
    "act_gold",
    "act_json_path",
    "act_numero_acordo",
    "act_data_assinatura",
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
    "memorando_data_assinatura",
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
    "best_data_assinatura",
    "best_datas_assinatura",
    "best_data_assinatura_source",
    "best_data_assinatura_confidence",
    "best_objeto",
    "best_objeto_source",
    "best_objeto_confidence",
    "normalization_issues",
]

PARCERIAS_VIGENTES_COLUMNS = [
    "interno_descricao",
    "seq",
    "processo",
    "parceiro",
    "vigencia",
    "numero_act",
    "objeto",
]

PARCERIAS_DESCONTINUADAS_COLUMNS = [
    "processo",
    "tipo",
    "numero_act",
    "numero_termo_encerramento",
    "parceiro",
    "vigencia",
    "objeto",
    "gestor_titular",
    "gestor_substituto",
    "portaria_designacao",
    "data_assinatura",
    "data_vencimento",
    "termo_encerramento_raw",
    "status_raw",
    "status_normalizado",
    "status_calculado",
    "status_categoria",
    "status_evidencia",
    "status_data_referencia",
    "normalization_status",
    "missing_fields",
    "raw_anotacoes",
]

PT_COLUMNS = [
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
    "period_class",
    "data_assinatura",
    "datas_assinatura",
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

TED_COLUMNS = [
    "processo",
    "documento",
    "numero_ted",
    "ano_ted",
    "objeto",
    "unidade_descentralizadora",
    "unidade_descentralizada",
    "valor_global",
    "vigencia_inicio",
    "vigencia_fim",
    "plano_aplicacao",
    "cronograma_desembolso",
    "metas",
    "prestacao_contas",
    "validation_status",
    "publication_status",
    "normalization_status",
    "quality_status",
    "quality_notes",
    "json_path",
]

ADMIN_COLUMNS = [
    "captured_at",
    "requested_type",
    "processo",
    "documento",
    "resolved_document_type",
    "funcao_administrativa",
    "origem",
    "destino",
    "data",
    "data_assinatura",
    "datas_assinatura",
    "assunto",
    "resumo",
    "acao_solicitada",
    "prazo",
    "documentos_mencionados",
    "selection_reason",
    "classification_reason",
    "validation_status",
    "publication_status",
    "snapshot_mode",
    "json_path",
]

REVIEW_COLUMNS = [
    "code",
    "severity",
    "field",
    "message",
    "suggested_action",
    "document_type",
    "processo",
    "documento",
    "publication_status",
    "validation_status",
    "normalization_status",
    "json_path",
    "is_gold_missing",
    "is_recoverable",
    "is_not_found",
]


def empty_dataframe(columns: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def read_csv_safe(path: Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    expected = list(columns or [])
    if not path.exists() or path.stat().st_size == 0:
        return empty_dataframe(expected)
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return empty_dataframe(expected)
    if expected:
        for column in expected:
            if column not in df.columns:
                df[column] = ""
        return df[expected].copy()
    return df


def read_json_safe(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def read_json_lines_safe(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    entries: list[dict[str, Any]] = []
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


def dashboard_source_paths(root_dir: Path) -> list[Path]:
    backend_output_dir = root_dir / "backend" / "output"
    return [
        backend_output_dir / "dashboard_ready_latest.csv",
        backend_output_dir / "parcerias_vigentes_latest.csv",
        backend_output_dir / "parcerias_descontinuadas_normalizado_latest.csv",
        backend_output_dir / "pt_normalizado_latest.csv",
        backend_output_dir / "pt_auditoria_latest.csv",
        backend_output_dir / "act_normalizado_latest.csv",
        backend_output_dir / "act_classificacao_latest.csv",
        backend_output_dir / "memorando_normalizado_latest.csv",
        backend_output_dir / "documento_administrativo_normalizado_latest.csv",
        backend_output_dir / "ted_normalizado_latest.csv",
        backend_output_dir / "pt_status_execucao_latest.csv",
        backend_output_dir / "act_status_execucao_latest.csv",
        backend_output_dir / "memorando_status_execucao_latest.csv",
        backend_output_dir / "ted_status_execucao_latest.csv",
        backend_output_dir / "divergence_matrix_latest.csv",
        backend_output_dir / "normalization_review_queue_latest.csv",
        backend_output_dir / "performance_analysis.json",
        root_dir / "output" / "execution_log_latest.json",
    ]


def build_file_signature(paths: Iterable[Path]) -> tuple[tuple[str, bool, int, int], ...]:
    signature: list[tuple[str, bool, int, int]] = []
    for path in paths:
        if path.exists():
            stat = path.stat()
            signature.append((str(path), True, int(stat.st_mtime_ns), int(stat.st_size)))
        else:
            signature.append((str(path), False, 0, 0))
    return tuple(signature)


def _ensure_bool_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column in result.columns:
            result[column] = result[column].apply(lambda value: clean_spaces(value).lower() in {"1", "true", "sim", "yes"})
    return result


def _ensure_pt_aliases(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "atribuicoes_raw" not in result.columns:
        for alias in ("atribuiÃ§Ãµes_raw", "atribuições_raw"):
            if alias in result.columns:
                result["atribuicoes_raw"] = result[alias]
                break
        else:
            result["atribuicoes_raw"] = ""
    for column in PT_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    return result[PT_COLUMNS].copy()


def _latest_log_timestamp(entries: list[dict[str, Any]]) -> str:
    timestamps = [clean_spaces(entry.get("timestamp", "")) for entry in entries if clean_spaces(entry.get("timestamp", ""))]
    return max(timestamps) if timestamps else ""


def _latest_captured_at(frames: Iterable[pd.DataFrame]) -> str:
    values: list[str] = []
    for df in frames:
        if "captured_at" not in df.columns:
            continue
        values.extend(value for value in df["captured_at"].astype(str).str.strip().tolist() if value)
    return max(values) if values else ""


def _latest_mtime(paths: Iterable[Path]) -> str:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    if not mtimes:
        return ""
    return pd.Timestamp.fromtimestamp(max(mtimes)).strftime("%Y-%m-%dT%H:%M:%S")


def load_dashboard_bundle(root_dir: Path) -> dict[str, Any]:
    backend_output_dir = root_dir / "backend" / "output"
    overview_df = _ensure_bool_columns(
        read_csv_safe(backend_output_dir / "dashboard_ready_latest.csv", OVERVIEW_COLUMNS),
        ("pt_gold", "act_gold", "memorando_gold", "ted_gold", "has_process_mismatch"),
    )
    pt_normalized_df = _ensure_pt_aliases(read_csv_safe(backend_output_dir / "pt_normalizado_latest.csv"))
    pt_audit_df = _ensure_pt_aliases(read_csv_safe(backend_output_dir / "pt_auditoria_latest.csv"))
    log_entries = read_json_lines_safe(root_dir / "output" / "execution_log_latest.json")
    source_paths = dashboard_source_paths(root_dir)

    bundle = {
        "root_dir": root_dir,
        "backend_output_dir": backend_output_dir,
        "overview": overview_df,
        "parcerias_vigentes": read_csv_safe(backend_output_dir / "parcerias_vigentes_latest.csv", PARCERIAS_VIGENTES_COLUMNS),
        "parcerias_descontinuadas": read_csv_safe(
            backend_output_dir / "parcerias_descontinuadas_normalizado_latest.csv",
            PARCERIAS_DESCONTINUADAS_COLUMNS,
        ),
        "pt_normalized": pt_normalized_df,
        "pt_audit": pt_audit_df,
        "pt_status": read_csv_safe(backend_output_dir / "pt_status_execucao_latest.csv"),
        "act_normalized": read_csv_safe(backend_output_dir / "act_normalizado_latest.csv"),
        "act_classificacao": read_csv_safe(backend_output_dir / "act_classificacao_latest.csv"),
        "act_status": read_csv_safe(backend_output_dir / "act_status_execucao_latest.csv"),
        "memorando_normalized": read_csv_safe(backend_output_dir / "memorando_normalizado_latest.csv", ADMIN_COLUMNS),
        "admin_normalized": read_csv_safe(backend_output_dir / "documento_administrativo_normalizado_latest.csv", ADMIN_COLUMNS),
        "memorando_status": read_csv_safe(backend_output_dir / "memorando_status_execucao_latest.csv"),
        "ted_normalized": read_csv_safe(backend_output_dir / "ted_normalizado_latest.csv", TED_COLUMNS),
        "ted_status": read_csv_safe(backend_output_dir / "ted_status_execucao_latest.csv"),
        "divergence": read_csv_safe(backend_output_dir / "divergence_matrix_latest.csv"),
        "review_queue": read_csv_safe(backend_output_dir / "normalization_review_queue_latest.csv", REVIEW_COLUMNS),
        "performance": read_json_safe(backend_output_dir / "performance_analysis.json"),
        "log_entries": log_entries,
        "source_paths": source_paths,
    }
    collected_at = _latest_log_timestamp(log_entries) or _latest_captured_at(
        [pt_normalized_df, pt_audit_df, bundle["memorando_normalized"], bundle["admin_normalized"]]
    ) or _latest_mtime(source_paths)
    bundle["collection_meta"] = {
        "data_ultima_coleta": collected_at,
        "fonte_data_ultima_coleta": "execution_log" if _latest_log_timestamp(log_entries) else "captured_at_or_mtime",
    }
    return bundle
