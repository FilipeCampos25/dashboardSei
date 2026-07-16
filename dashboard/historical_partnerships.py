from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from .data_cleaning import (
    clean_spaces,
    display_text,
    first_non_empty,
    format_date_display,
    format_date_iso,
    join_non_empty,
    normalize_key,
    normalize_processo,
    parse_date,
    summarize_text,
)
from .data_sources import empty_dataframe


HISTORY_MODEL_COLUMNS = [
    "record_id",
    "processo",
    "processo_normalizado",
    "documento_instrumento",
    "tipo",
    "numero_act",
    "numero_termo_encerramento",
    "parceiro",
    "objeto_resumo",
    "objeto_completo",
    "status_normalizado",
    "status_raw",
    "status_calculado",
    "status_categoria",
    "status_evidencia",
    "status_data_referencia",
    "status_gerencial",
    "categoria_gerencial",
    "data_assinatura",
    "data_vencimento",
    "data_assinatura_display",
    "data_vencimento_display",
    "termo_encerramento_raw",
    "observacoes",
    "campos_ausentes",
    "conflitos",
    "fontes_origem",
]


def _status_gerencial(status_categoria: Any, status_calculado: Any, status_normalizado: Any) -> str:
    category = clean_spaces(status_categoria)
    calculated = clean_spaces(status_calculado)
    normalized = clean_spaces(status_normalizado)
    if calculated:
        return calculated
    if category == "encerrado":
        return "Encerrado"
    if category == "nao_realizado":
        return "Não realizado"
    if category == "descontinuado":
        return "Descontinuado"
    if category in {"sem_status", "vigente_em_descontinuadas", ""}:
        return "Inconsistente / revisar"
    return normalized or "Outro"


def _document_label(tipo: Any, numero_act: Any, termo: Any) -> str:
    parts = [clean_spaces(tipo)]
    number = first_non_empty(numero_act, termo)
    if number:
        parts.append(number)
    return " ".join(part for part in parts if part) or "Documento não identificado"


def _missing_fields(record: dict[str, Any]) -> str:
    missing = []
    for label, field in (("parceiro", "parceiro"), ("objeto", "objeto_completo"), ("status", "status_calculado")):
        if not clean_spaces(record.get(field, "")):
            missing.append(label)
    return ", ".join(missing)


def _score(record: dict[str, Any]) -> int:
    fields = ("documento_instrumento", "parceiro", "objeto_completo", "status_calculado", "data_assinatura", "data_vencimento")
    return sum(1 for field in fields if clean_spaces(record.get(field, "")))


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    rows: list[dict[str, Any]] = []
    for _, group in df.groupby("record_id", sort=False):
        records = group.to_dict(orient="records")
        best = max(records, key=_score)
        if len(records) > 1:
            best = dict(best)
            best["conflitos"] = join_non_empty([best.get("conflitos", ""), f"duplicidade_interna={len(records)}"])
        rows.append(best)
    return pd.DataFrame(rows, columns=df.columns)


def build_historical_partnerships(bundle: dict[str, Any]) -> pd.DataFrame:
    source_df = bundle.get("history_normalized", empty_dataframe([]))
    if source_df.empty:
        return empty_dataframe(HISTORY_MODEL_COLUMNS)
    rows: list[dict[str, Any]] = []
    for raw in source_df.to_dict(orient="records"):
        processo = normalize_processo(raw.get("processo", ""))
        if not processo:
            continue
        status = _status_gerencial(
            raw.get("status_categoria", ""), raw.get("status_calculado", ""), raw.get("status_normalizado", "")
        )
        conflicts = []
        raw_status = clean_spaces(raw.get("status_raw", ""))
        normalized_raw = clean_spaces(raw.get("status_normalizado", ""))
        calculated_status = clean_spaces(raw.get("status_calculado", ""))
        if calculated_status and normalized_raw and normalize_key(calculated_status) != normalize_key(normalized_raw):
            conflicts.append("status_raw_diverge_do_calculado")
        elif status == "Inconsistente / revisar":
            conflicts.append(clean_spaces(raw.get("status_categoria", "")) or "sem_status")
        record = {
            "record_id": "|".join(
                [
                    processo,
                    normalize_key(raw.get("tipo", "")),
                    normalize_key(raw.get("numero_act", "")),
                    normalize_key(raw.get("numero_termo_encerramento", "")),
                    normalize_key(raw.get("status_categoria", "")),
                ]
            ),
            "processo": processo,
            "processo_normalizado": processo,
            "documento_instrumento": _document_label(raw.get("tipo", ""), raw.get("numero_act", ""), raw.get("numero_termo_encerramento", "")),
            "tipo": clean_spaces(raw.get("tipo", "")),
            "numero_act": clean_spaces(raw.get("numero_act", "")),
            "numero_termo_encerramento": clean_spaces(raw.get("numero_termo_encerramento", "")),
            "parceiro": clean_spaces(raw.get("parceiro", "")),
            "objeto_resumo": summarize_text(raw.get("objeto", ""), 150),
            "objeto_completo": clean_spaces(raw.get("objeto", "")),
            "status_normalizado": clean_spaces(raw.get("status_normalizado", "")),
            "status_raw": raw_status,
            "status_calculado": calculated_status or status,
            "status_categoria": clean_spaces(raw.get("status_categoria", "")),
            "status_evidencia": clean_spaces(raw.get("status_evidencia", "")),
            "status_data_referencia": format_date_iso(raw.get("status_data_referencia", "")),
            "status_gerencial": status,
            "categoria_gerencial": status,
            "data_assinatura": format_date_iso(raw.get("data_assinatura", "")),
            "data_vencimento": format_date_iso(raw.get("data_vencimento", "")),
            "data_assinatura_display": format_date_display(raw.get("data_assinatura", "")),
            "data_vencimento_display": format_date_display(raw.get("data_vencimento", "")),
            "termo_encerramento_raw": clean_spaces(raw.get("termo_encerramento_raw", "")),
            "observacoes": clean_spaces(raw.get("raw_anotacoes", "")),
            "conflitos": join_non_empty(conflicts),
            "fontes_origem": "parcerias_descontinuadas_normalizado_latest",
        }
        record["campos_ausentes"] = join_non_empty([clean_spaces(raw.get("missing_fields", "")), _missing_fields(record)], ", ")
        rows.append(record)
    result = _deduplicate(pd.DataFrame(rows))
    return result.sort_values(["status_gerencial", "processo"]).reset_index(drop=True)[HISTORY_MODEL_COLUMNS]


def history_metrics(df: pd.DataFrame) -> dict[str, int]:
    counts = df["status_gerencial"].value_counts().to_dict() if not df.empty and "status_gerencial" in df.columns else {}
    return {
        "total": int(len(df)),
        "encerradas": int(counts.get("Encerrado", 0)),
        "nao_realizadas": int(sum(value for key, value in counts.items() if "realizado" in key.casefold())),
        "vencidas": int(counts.get("Vencido", 0)),
        "vigentes": int(counts.get("Vigente", 0)),
        "indeterminadas": int(counts.get("Indeterminado", 0)),
        "inconsistentes": int(counts.get("Inconsistente / revisar", 0) + counts.get("Indeterminado", 0)),
    }


def status_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "status_gerencial" not in df.columns:
        return pd.DataFrame(columns=["Status", "Total"])
    return df["status_gerencial"].value_counts().rename_axis("Status").reset_index(name="Total")


def filter_history(
    df: pd.DataFrame,
    *,
    processos: Iterable[str] | None = None,
    parceiros: Iterable[str] | None = None,
    documentos: Iterable[str] | None = None,
    statuses: Iterable[str] | None = None,
    categorias: Iterable[str] | None = None,
    date_start: Any = None,
    date_end: Any = None,
    query: str = "",
) -> pd.DataFrame:
    filtered = df.copy()
    selected = {clean_spaces(value) for value in (processos or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["processo"].isin(selected)]
    selected = {clean_spaces(value) for value in (parceiros or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["parceiro"].isin(selected)]
    selected = {clean_spaces(value) for value in (documentos or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["tipo"].isin(selected)]
    selected = {clean_spaces(value) for value in (statuses or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["status_gerencial"].isin(selected)]
    selected = {clean_spaces(value) for value in (categorias or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["categoria_gerencial"].isin(selected)]
    start = parse_date(date_start)
    end = parse_date(date_end)
    if not pd.isna(start) or not pd.isna(end):
        assinatura = pd.to_datetime(filtered["data_assinatura"], errors="coerce")
        vencimento = pd.to_datetime(filtered["data_vencimento"], errors="coerce")
        mask = pd.Series(True, index=filtered.index)
        if not pd.isna(start):
            mask = mask & ((assinatura >= start) | (vencimento >= start))
        if not pd.isna(end):
            mask = mask & ((assinatura <= end) | (vencimento <= end))
        filtered = filtered[mask]
    normalized_query = clean_spaces(query).lower()
    if normalized_query:
        searchable = ["processo", "documento_instrumento", "parceiro", "objeto_completo", "status_gerencial", "observacoes"]
        mask = pd.Series(False, index=filtered.index)
        for column in searchable:
            mask = mask | filtered[column].astype(str).str.lower().str.contains(normalized_query, na=False, regex=False)
        filtered = filtered[mask]
    return filtered.reset_index(drop=True)


def display_table(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "processo",
        "documento_instrumento",
        "parceiro",
        "objeto_resumo",
        "status_gerencial",
        "data_assinatura_display",
        "data_vencimento_display",
        "categoria_gerencial",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    table = df[columns].copy()
    table["parceiro"] = table["parceiro"].apply(display_text)
    return table.rename(
        columns={
            "processo": "Processo",
            "documento_instrumento": "Documento / Instrumento",
            "parceiro": "Parceiro",
            "objeto_resumo": "Objeto",
            "status_gerencial": "Status",
            "data_assinatura_display": "Data de Assinatura",
            "data_vencimento_display": "Data de Encerramento / Vencimento",
            "categoria_gerencial": "Categoria",
        }
    )


def detail_label(df: pd.DataFrame, record_id: str) -> str:
    row = df[df["record_id"] == record_id]
    if row.empty:
        return record_id
    record = row.iloc[0]
    return f"{record['processo']} - {record['status_gerencial']}"
