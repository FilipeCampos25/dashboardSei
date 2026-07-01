from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd

from .category_models import VIGENCIA_LABELS
from .data_cleaning import (
    boolish,
    clean_spaces,
    display_text,
    first_non_empty,
    format_date_display,
    format_date_iso,
    join_non_empty,
    normalize_key,
    normalize_processo,
    summarize_text,
)
from .data_sources import empty_dataframe
from .vigencia_rules import add_vigencia_columns


ACTIVE_COLUMNS = [
    "record_id",
    "processo",
    "processo_normalizado",
    "seq",
    "documento_tipo",
    "documento_instrumento",
    "parceiro",
    "objeto_resumo",
    "objeto_completo",
    "vigencia_inicio",
    "vigencia_fim",
    "vigencia_inicio_display",
    "vigencia_fim_display",
    "data_assinatura",
    "data_assinatura_display",
    "datas_assinatura",
    "dias_restantes",
    "situacao_vigencia",
    "situacao_display",
    "possui_pt",
    "possui_ted",
    "possui_memorando",
    "documentos_relacionados",
    "campos_ausentes",
    "conflitos",
    "fontes_origem",
    "vigencia_raw",
]


def _records_by_process(df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if df.empty or "processo" not in df.columns:
        return {}
    rows: dict[str, list[dict[str, Any]]] = {}
    for record in df.to_dict(orient="records"):
        processo = normalize_processo(record.get("processo", ""))
        if processo:
            rows.setdefault(processo, []).append(record)
    return rows


def _first_by_process(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {processo: records[0] for processo, records in _records_by_process(df).items() if records}


def _process_set(df: pd.DataFrame) -> set[str]:
    if df.empty or "processo" not in df.columns:
        return set()
    return {normalize_processo(value) for value in df["processo"].tolist() if normalize_processo(value)}


def _document_type(number: str) -> str:
    return "ACT" if clean_spaces(number) else "Parceria"


def _document_label(document_type: str, number: str) -> str:
    number = clean_spaces(number)
    if document_type == "ACT" and number:
        return f"ACT {number}"
    return "Parceria não identificada"


def _missing_fields(record: dict[str, Any]) -> str:
    missing: list[str] = []
    checks = {
        "documento/instrumento": record.get("documento_instrumento", "") != "Parceria não identificada",
        "parceiro": bool(clean_spaces(record.get("parceiro", ""))),
        "objeto": bool(clean_spaces(record.get("objeto_completo", ""))),
        "fim_vigencia": bool(clean_spaces(record.get("vigencia_fim", ""))),
    }
    for label, ok in checks.items():
        if not ok:
            missing.append(label)
    return ", ".join(missing)


def _score(record: dict[str, Any]) -> int:
    fields = (
        "documento_instrumento",
        "parceiro",
        "objeto_completo",
        "vigencia_inicio",
        "vigencia_fim",
        "documentos_relacionados",
    )
    score = sum(1 for field in fields if clean_spaces(record.get(field, "")))
    score += 1 if record.get("possui_pt") else 0
    score += 1 if record.get("possui_memorando") else 0
    return score


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


def _sort_key(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    order = {"vermelho": 0, "amarelo": 1, "sem_data": 2, "verde": 3}
    result["_status_order"] = result["situacao_vigencia"].map(order).fillna(9)
    result["_days_order"] = result["dias_restantes"].apply(lambda value: 10**9 if pd.isna(value) else int(value))
    result["_fim_order"] = pd.to_datetime(result["vigencia_fim"], errors="coerce")
    return result.sort_values(
        ["_status_order", "_fim_order", "_days_order", "processo"],
        ascending=[True, True, True, True],
        na_position="last",
    ).drop(columns=["_status_order", "_days_order", "_fim_order"])


def build_active_partnerships(
    bundle: dict[str, Any],
    *,
    today: date | datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    source_df = bundle.get("parcerias_vigentes", empty_dataframe([]))
    if source_df.empty:
        return empty_dataframe(ACTIVE_COLUMNS)

    dashboard_by_process = _first_by_process(bundle.get("dashboard_ready", empty_dataframe([])))
    history_processes = _process_set(bundle.get("history_normalized", empty_dataframe([])))
    ted_processes = _process_set(bundle.get("ted_normalized", empty_dataframe([])))
    pt_processes = _process_set(bundle.get("pt_normalized", empty_dataframe([]))) | _process_set(bundle.get("pt_audit", empty_dataframe([])))
    act_processes = _process_set(bundle.get("act_normalized", empty_dataframe([])))
    memorando_processes = _process_set(bundle.get("memorando_normalized", empty_dataframe([]))) | _process_set(bundle.get("admin_normalized", empty_dataframe([])))

    rows: list[dict[str, Any]] = []
    for raw in source_df.to_dict(orient="records"):
        processo = normalize_processo(raw.get("processo", ""))
        if not processo:
            continue
        dash = dashboard_by_process.get(processo, {})
        number = first_non_empty(dash.get("best_numero_acordo", ""), raw.get("numero_act", ""))
        doc_type = _document_type(number)
        parceiro = first_non_empty(dash.get("best_parceiro", ""), raw.get("parceiro", ""))
        objeto = first_non_empty(dash.get("best_objeto", ""), raw.get("objeto", ""))
        vigencia_inicio = format_date_iso(dash.get("best_vigencia_inicio", ""))
        vigencia_fim = format_date_iso(dash.get("best_vigencia_fim", ""))
        data_assinatura = format_date_iso(dash.get("best_data_assinatura", ""))
        docs = []
        if processo in act_processes or boolish(dash.get("act_gold", "")):
            docs.append("ACT")
        if processo in pt_processes or boolish(dash.get("pt_gold", "")):
            docs.append("Plano de Trabalho")
        if processo in memorando_processes or boolish(dash.get("memorando_gold", "")):
            docs.append("Memorando/documento administrativo")
        if processo in ted_processes:
            docs.append("TED")

        conflicts = []
        if processo in history_processes:
            conflicts.append("processo_tambem_no_historico")
        if clean_spaces(dash.get("normalization_issues", "")):
            conflicts.append(clean_spaces(dash.get("normalization_issues", "")))

        sources = ["parcerias_vigentes_latest"]
        for field in ("best_numero_acordo_source", "best_parceiro_source", "best_vigencia_source", "best_objeto_source"):
            source = clean_spaces(dash.get(field, ""))
            if source:
                sources.append(f"{field.replace('best_', '').replace('_source', '')}:{source}")
        if processo in pt_processes:
            sources.append("pt_normalizado/auditoria")
        if processo in act_processes:
            sources.append("act_normalizado")
        if processo in memorando_processes:
            sources.append("memorando/documento_administrativo")

        record = {
            "record_id": f"{processo}|{normalize_key(number, str(raw.get('seq', '')))}",
            "processo": processo,
            "processo_normalizado": processo,
            "seq": clean_spaces(raw.get("seq", "")),
            "documento_tipo": doc_type,
            "documento_instrumento": _document_label(doc_type, number),
            "parceiro": parceiro,
            "objeto_resumo": summarize_text(objeto, 150),
            "objeto_completo": objeto,
            "vigencia_inicio": vigencia_inicio,
            "vigencia_fim": vigencia_fim,
            "vigencia_inicio_display": format_date_display(vigencia_inicio),
            "vigencia_fim_display": format_date_display(vigencia_fim),
            "data_assinatura": data_assinatura,
            "data_assinatura_display": format_date_display(data_assinatura),
            "datas_assinatura": clean_spaces(dash.get("best_datas_assinatura", "")),
            "possui_pt": processo in pt_processes or boolish(dash.get("pt_gold", "")),
            "possui_ted": processo in ted_processes,
            "possui_memorando": processo in memorando_processes or boolish(dash.get("memorando_gold", "")),
            "documentos_relacionados": join_non_empty(docs) or "Nenhum documento relacionado identificado",
            "conflitos": join_non_empty(conflicts),
            "fontes_origem": join_non_empty(sources),
            "vigencia_raw": first_non_empty(dash.get("best_vigencia_raw", ""), raw.get("vigencia", "")),
        }
        record["campos_ausentes"] = _missing_fields(record)
        rows.append(record)

    result = add_vigencia_columns(pd.DataFrame(rows), "vigencia_fim", today=today)
    result["situacao_display"] = result["situacao_vigencia"].map(VIGENCIA_LABELS).fillna("Sem data")
    result = _deduplicate(result)
    return _sort_key(result)[ACTIVE_COLUMNS].reset_index(drop=True)


def active_metrics(df: pd.DataFrame) -> dict[str, int]:
    counts = df["situacao_vigencia"].value_counts().to_dict() if not df.empty and "situacao_vigencia" in df.columns else {}
    return {
        "total": int(len(df)),
        "vermelho": int(counts.get("vermelho", 0)),
        "amarelo": int(counts.get("amarelo", 0)),
        "verde": int(counts.get("verde", 0)),
        "sem_data": int(counts.get("sem_data", 0)),
    }


def deadline_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "situacao_vigencia" not in df.columns:
        return pd.DataFrame(columns=["situacao_vigencia", "Situação", "Total"])
    order = ["vermelho", "amarelo", "verde", "sem_data"]
    counts = df["situacao_vigencia"].value_counts().reindex(order, fill_value=0).reset_index()
    counts.columns = ["situacao_vigencia", "Total"]
    counts["Situação"] = counts["situacao_vigencia"].map(VIGENCIA_LABELS)
    return counts[counts["Total"] > 0]


def filter_active_partnerships(
    df: pd.DataFrame,
    *,
    processos: Iterable[str] | None = None,
    parceiros: Iterable[str] | None = None,
    documento_tipos: Iterable[str] | None = None,
    situacoes: Iterable[str] | None = None,
    has_pt: str = "Todos",
    has_ted: str = "Todos",
    query: str = "",
) -> pd.DataFrame:
    filtered = df.copy()
    selected = {clean_spaces(value) for value in (processos or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["processo"].isin(selected)]
    selected = {clean_spaces(value) for value in (parceiros or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["parceiro"].isin(selected)]
    selected = {clean_spaces(value) for value in (documento_tipos or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["documento_tipo"].isin(selected)]
    selected = {clean_spaces(value) for value in (situacoes or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["situacao_vigencia"].isin(selected)]

    def apply_presence(source: pd.DataFrame, column: str, mode: str) -> pd.DataFrame:
        normalized = clean_spaces(mode).lower()
        if normalized == "com":
            return source[source[column]]
        if normalized == "sem":
            return source[~source[column]]
        return source

    if "possui_pt" in filtered.columns:
        filtered = apply_presence(filtered, "possui_pt", has_pt)
    if "possui_ted" in filtered.columns:
        filtered = apply_presence(filtered, "possui_ted", has_ted)

    normalized_query = clean_spaces(query).lower()
    if normalized_query:
        searchable = ["processo", "documento_instrumento", "parceiro", "objeto_completo", "documentos_relacionados"]
        mask = pd.Series(False, index=filtered.index)
        for column in searchable:
            mask = mask | filtered[column].astype(str).str.lower().str.contains(normalized_query, na=False, regex=False)
        filtered = filtered[mask]
    return _sort_key(filtered).reset_index(drop=True)


def display_table(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "processo",
        "documento_instrumento",
        "parceiro",
        "objeto_resumo",
        "vigencia_inicio_display",
        "vigencia_fim_display",
        "dias_restantes",
        "situacao_display",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    table = df[columns].copy()
    table["dias_restantes"] = table["dias_restantes"].apply(lambda value: "" if pd.isna(value) else str(int(value)))
    return table.rename(
        columns={
            "processo": "Processo",
            "documento_instrumento": "Documento / Instrumento",
            "parceiro": "Parceiro",
            "objeto_resumo": "Objeto / Atribuição",
            "vigencia_inicio_display": "Início da Vigência",
            "vigencia_fim_display": "Fim da Vigência",
            "dias_restantes": "Dias Restantes",
            "situacao_display": "Situação",
        }
    )


def detail_options(df: pd.DataFrame) -> list[str]:
    return df["record_id"].tolist() if not df.empty else []


def detail_label(df: pd.DataFrame, record_id: str) -> str:
    row = df[df["record_id"] == record_id]
    if row.empty:
        return record_id
    record = row.iloc[0]
    return f"{record['processo']} - {display_text(record['documento_instrumento'])}"
