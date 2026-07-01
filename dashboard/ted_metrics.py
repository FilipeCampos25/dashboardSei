from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd

from .category_models import VIGENCIA_LABELS
from .data_cleaning import (
    clean_spaces,
    display_text,
    first_non_empty,
    format_currency,
    format_date_display,
    format_date_iso,
    is_valid_money,
    join_non_empty,
    normalize_key,
    normalize_processo,
    parse_money,
    summarize_text,
)
from .data_sources import empty_dataframe
from .vigencia_rules import add_vigencia_columns


TED_MODEL_COLUMNS = [
    "record_id",
    "processo",
    "processo_normalizado",
    "documento",
    "numero_ted",
    "ano_ted",
    "numero_ted_display",
    "objeto_resumo",
    "objeto_completo",
    "unidade_descentralizadora",
    "unidade_descentralizada",
    "valor_global",
    "valor_global_num",
    "valor_global_display",
    "valor_global_valido",
    "vigencia_inicio",
    "vigencia_fim",
    "vigencia_inicio_display",
    "vigencia_fim_display",
    "dias_restantes",
    "situacao_vigencia",
    "situacao_display",
    "plano_aplicacao",
    "metas",
    "prestacao_contas",
    "quality_status",
    "normalization_status",
    "campos_ausentes",
    "conflitos",
    "fontes_origem",
]


def ted_number_display(numero: Any, ano: Any, documento: Any = "") -> str:
    number = clean_spaces(numero)
    year = clean_spaces(ano)
    if number and year:
        return f"{number}/{year}"
    if number:
        return number
    return clean_spaces(documento) or "Não identificado"


def _missing_fields(record: dict[str, Any]) -> str:
    missing = []
    if not clean_spaces(record.get("numero_ted", "")):
        missing.append("numero_ted")
    if not clean_spaces(record.get("objeto_completo", "")):
        missing.append("objeto")
    if not record.get("valor_global_valido"):
        missing.append("valor_global")
    if not clean_spaces(record.get("vigencia_fim", "")):
        missing.append("vigencia_fim")
    if not clean_spaces(record.get("unidade_descentralizadora", "")):
        missing.append("unidade_descentralizadora")
    return ", ".join(missing)


def _score(record: dict[str, Any]) -> int:
    fields = (
        "numero_ted",
        "ano_ted",
        "objeto_completo",
        "unidade_descentralizadora",
        "unidade_descentralizada",
        "vigencia_inicio",
        "vigencia_fim",
    )
    score = sum(1 for field in fields if clean_spaces(record.get(field, "")))
    score += 2 if record.get("valor_global_valido") else 0
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


def build_teds(bundle: dict[str, Any], *, today: date | datetime | pd.Timestamp | None = None) -> pd.DataFrame:
    source_df = bundle.get("ted_normalized", empty_dataframe([]))
    if source_df.empty:
        return empty_dataframe(TED_MODEL_COLUMNS)
    rows: list[dict[str, Any]] = []
    for raw in source_df.to_dict(orient="records"):
        processo = normalize_processo(raw.get("processo", ""))
        if not processo:
            continue
        value = parse_money(raw.get("valor_global", ""))
        value_valid = value is not None and value > 0
        numero = clean_spaces(raw.get("numero_ted", ""))
        ano = clean_spaces(raw.get("ano_ted", ""))
        documento = clean_spaces(raw.get("documento", ""))
        key_value = normalize_key(f"{numero}{ano}") or normalize_key(documento) or "TED"
        record = {
            "record_id": f"{processo}|{key_value}",
            "processo": processo,
            "processo_normalizado": processo,
            "documento": documento,
            "numero_ted": numero,
            "ano_ted": ano,
            "numero_ted_display": ted_number_display(numero, ano, documento),
            "objeto_resumo": summarize_text(raw.get("objeto", ""), 150),
            "objeto_completo": clean_spaces(raw.get("objeto", "")),
            "unidade_descentralizadora": clean_spaces(raw.get("unidade_descentralizadora", "")),
            "unidade_descentralizada": clean_spaces(raw.get("unidade_descentralizada", "")),
            "valor_global": clean_spaces(raw.get("valor_global", "")),
            "valor_global_num": value if value_valid else 0.0,
            "valor_global_display": format_currency(value),
            "valor_global_valido": value_valid,
            "vigencia_inicio": format_date_iso(raw.get("vigencia_inicio", "")),
            "vigencia_fim": format_date_iso(raw.get("vigencia_fim", "")),
            "vigencia_inicio_display": format_date_display(raw.get("vigencia_inicio", "")),
            "vigencia_fim_display": format_date_display(raw.get("vigencia_fim", "")),
            "plano_aplicacao": clean_spaces(raw.get("plano_aplicacao", "")),
            "metas": clean_spaces(raw.get("metas", "")),
            "prestacao_contas": clean_spaces(raw.get("prestacao_contas", "")),
            "quality_status": clean_spaces(raw.get("quality_status", "")),
            "normalization_status": clean_spaces(raw.get("normalization_status", "")),
            "conflitos": "",
            "fontes_origem": "ted_normalizado_latest",
        }
        record["campos_ausentes"] = _missing_fields(record)
        rows.append(record)
    result = add_vigencia_columns(pd.DataFrame(rows), "vigencia_fim", today=today)
    result["situacao_display"] = result["situacao_vigencia"].map(VIGENCIA_LABELS).fillna("Sem data")
    result = _deduplicate(result)
    return result[TED_MODEL_COLUMNS].reset_index(drop=True)


def ted_metrics(df: pd.DataFrame) -> dict[str, Any]:
    valid = df[df["valor_global_valido"]] if not df.empty and "valor_global_valido" in df.columns else empty_dataframe([])
    counts = df["situacao_vigencia"].value_counts().to_dict() if not df.empty and "situacao_vigencia" in df.columns else {}
    total_value = float(valid["valor_global_num"].sum()) if not valid.empty else 0.0
    return {
        "total": int(len(df)),
        "valor_total": total_value,
        "valor_total_display": format_currency(total_value),
        "valor_validos": int(len(valid)),
        "sem_vigencia": int(counts.get("sem_data", 0)),
        "vermelho": int(counts.get("vermelho", 0)),
    }


def value_ranking(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["label", "valor_global_num", "valor_global_display"])
    ranking = df[df["valor_global_valido"]].sort_values("valor_global_num", ascending=False).head(limit).copy()
    if ranking.empty:
        return pd.DataFrame(columns=["label", "valor_global_num", "valor_global_display"])
    ranking["label"] = ranking.apply(
        lambda row: f"TED {row['numero_ted_display']}" if row["numero_ted_display"] != "Não identificado" else row["processo"],
        axis=1,
    )
    return ranking[["label", "valor_global_num", "valor_global_display", "processo"]]


def chartable_dimension(
    df: pd.DataFrame,
    column: str,
    *,
    min_coverage: float = 0.70,
    max_categories: int = 10,
    max_label_length: int = 60,
) -> dict[str, Any]:
    if df.empty or column not in df.columns:
        return {"allowed": False, "reason": "campo ausente ou sem registros", "coverage": 0.0, "categories": 0, "max_label_length": 0}
    series = df[column].fillna("").astype(str).str.strip()
    non_empty = series[series.ne("")]
    coverage = len(non_empty) / len(series) if len(series) else 0.0
    categories = int(non_empty.nunique())
    longest = int(non_empty.map(len).max()) if not non_empty.empty else 0
    sample = " ".join(non_empty.head(5).tolist()).lower()
    reasons = []
    if coverage < min_coverage:
        reasons.append(f"cobertura abaixo de {int(min_coverage * 100)}%")
    if categories > max_categories:
        reasons.append(f"mais de {max_categories} categorias")
    if longest > max_label_length:
        reasons.append(f"rótulos acima de {max_label_length} caracteres")
    if any(marker in sample for marker in ("documento assinado", "nome da autoridade", "ato de nomeação", "ato de nomeacao", "responsável pelo acompanhamento", "responsavel pelo acompanhamento")):
        reasons.append("conteúdo documental bruto")
    return {
        "allowed": not reasons,
        "reason": "; ".join(reasons) if reasons else "ok",
        "coverage": coverage,
        "categories": categories,
        "max_label_length": longest,
    }


def filter_teds(
    df: pd.DataFrame,
    *,
    processos: Iterable[str] | None = None,
    numeros: Iterable[str] | None = None,
    situacoes: Iterable[str] | None = None,
    unidades: Iterable[str] | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
    query: str = "",
) -> pd.DataFrame:
    filtered = df.copy()
    selected = {clean_spaces(value) for value in (processos or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["processo"].isin(selected)]
    selected = {clean_spaces(value) for value in (numeros or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["numero_ted_display"].isin(selected)]
    selected = {clean_spaces(value) for value in (situacoes or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["situacao_vigencia"].isin(selected)]
    selected = {clean_spaces(value) for value in (unidades or []) if clean_spaces(value)}
    if selected:
        filtered = filtered[filtered["unidade_descentralizadora"].isin(selected)]
    if min_value is not None:
        filtered = filtered[filtered["valor_global_num"] >= float(min_value)]
    if max_value is not None:
        filtered = filtered[filtered["valor_global_num"] <= float(max_value)]
    normalized_query = clean_spaces(query).lower()
    if normalized_query:
        searchable = ["processo", "numero_ted_display", "objeto_completo", "unidade_descentralizadora", "unidade_descentralizada"]
        mask = pd.Series(False, index=filtered.index)
        for column in searchable:
            mask = mask | filtered[column].astype(str).str.lower().str.contains(normalized_query, na=False, regex=False)
        filtered = filtered[mask]
    return filtered.sort_values(["valor_global_num", "processo"], ascending=[False, True]).reset_index(drop=True)


def display_table(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "processo",
        "numero_ted_display",
        "objeto_resumo",
        "unidade_descentralizadora",
        "valor_global_display",
        "vigencia_inicio_display",
        "vigencia_fim_display",
        "dias_restantes",
        "situacao_display",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    table = df[columns].copy()
    table["unidade_descentralizadora"] = table["unidade_descentralizadora"].apply(display_text)
    table["dias_restantes"] = table["dias_restantes"].apply(lambda value: "" if pd.isna(value) else str(int(value)))
    return table.rename(
        columns={
            "processo": "Processo",
            "numero_ted_display": "Número do TED",
            "objeto_resumo": "Objeto",
            "unidade_descentralizadora": "Unidade Descentralizadora",
            "valor_global_display": "Valor Global",
            "vigencia_inicio_display": "Início da Vigência",
            "vigencia_fim_display": "Fim da Vigência",
            "dias_restantes": "Dias Restantes",
            "situacao_display": "Situação",
        }
    )


def detail_label(df: pd.DataFrame, record_id: str) -> str:
    row = df[df["record_id"] == record_id]
    if row.empty:
        return record_id
    record = row.iloc[0]
    return f"{record['processo']} - TED {record['numero_ted_display']}"
