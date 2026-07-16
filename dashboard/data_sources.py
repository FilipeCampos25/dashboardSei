from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .data_cleaning import clean_spaces, format_datetime_display


PARCERIAS_VIGENTES_COLUMNS = [
    "interno_descricao",
    "seq",
    "processo",
    "parceiro",
    "vigencia",
    "numero_act",
    "objeto",
]

DASHBOARD_READY_COLUMNS = [
    "processo",
    "preview_parceiro",
    "preview_numero_act",
    "preview_objeto",
    "preview_vigencia",
    "pt_gold",
    "pt_data_assinatura",
    "act_gold",
    "act_data_assinatura",
    "memorando_gold",
    "memorando_data_assinatura",
    "ted_gold",
    "best_numero_acordo",
    "best_numero_acordo_source",
    "best_parceiro",
    "best_parceiro_source",
    "best_vigencia_inicio",
    "best_vigencia_fim",
    "best_vigencia_raw",
    "best_vigencia_source",
    "best_data_assinatura",
    "best_datas_assinatura",
    "best_data_assinatura_source",
    "best_objeto",
    "best_objeto_source",
    "normalization_issues",
    "quality_notes",
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

HISTORY_COLUMNS = [
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

ADMIN_COLUMNS = [
    "captured_at",
    "requested_type",
    "processo",
    "documento",
    "resolved_document_type",
    "funcao_administrativa",
    "data",
    "data_assinatura",
    "datas_assinatura",
    "assunto",
    "resumo",
    "acao_solicitada",
    "prazo",
    "documentos_mencionados",
    "validation_status",
    "publication_status",
    "snapshot_mode",
    "json_path",
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
    rows: list[dict[str, Any]] = []
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
                    rows.append(payload)
    except Exception:
        return []
    return rows


def dashboard_source_paths(root_dir: Path) -> list[Path]:
    backend_output = root_dir / "backend" / "output"
    return [
        backend_output / "parcerias_vigentes_latest.csv",
        backend_output / "dashboard_ready_latest.csv",
        backend_output / "pt_normalizado_latest.csv",
        backend_output / "pt_auditoria_latest.csv",
        backend_output / "act_normalizado_latest.csv",
        backend_output / "memorando_normalizado_latest.csv",
        backend_output / "documento_administrativo_normalizado_latest.csv",
        backend_output / "ted_normalizado_latest.csv",
        backend_output / "parcerias_descontinuadas_latest.csv",
        backend_output / "parcerias_descontinuadas_normalizado_latest.csv",
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
    backend_output = root_dir / "backend" / "output"
    log_entries = read_json_lines_safe(root_dir / "output" / "execution_log_latest.json")
    source_paths = dashboard_source_paths(root_dir)

    pt_normalized = read_csv_safe(backend_output / "pt_normalizado_latest.csv")
    pt_audit = read_csv_safe(backend_output / "pt_auditoria_latest.csv")
    memorando = read_csv_safe(backend_output / "memorando_normalizado_latest.csv", ADMIN_COLUMNS)
    admin = read_csv_safe(backend_output / "documento_administrativo_normalizado_latest.csv", ADMIN_COLUMNS)
    raw_collection = (
        _latest_log_timestamp(log_entries)
        or _latest_captured_at([pt_normalized, pt_audit, memorando, admin])
        or _latest_mtime(source_paths)
    )

    return {
        "root_dir": root_dir,
        "backend_output_dir": backend_output,
        "parcerias_vigentes": read_csv_safe(backend_output / "parcerias_vigentes_latest.csv", PARCERIAS_VIGENTES_COLUMNS),
        "dashboard_ready": read_csv_safe(backend_output / "dashboard_ready_latest.csv", DASHBOARD_READY_COLUMNS),
        "pt_normalized": pt_normalized,
        "pt_audit": pt_audit,
        "act_normalized": read_csv_safe(backend_output / "act_normalizado_latest.csv"),
        "memorando_normalized": memorando,
        "admin_normalized": admin,
        "ted_normalized": read_csv_safe(backend_output / "ted_normalizado_latest.csv", TED_COLUMNS),
        "history_raw": read_csv_safe(backend_output / "parcerias_descontinuadas_latest.csv", ["processo", "anotacoes"]),
        "history_normalized": read_csv_safe(backend_output / "parcerias_descontinuadas_normalizado_latest.csv", HISTORY_COLUMNS),
        "log_entries": log_entries,
        "source_paths": source_paths,
        "collection_meta": {
            "data_ultima_coleta": raw_collection,
            "data_ultima_coleta_display": format_datetime_display(raw_collection),
            "fonte_data_ultima_coleta": "execution_log" if _latest_log_timestamp(log_entries) else "captured_at_or_mtime",
        },
    }
