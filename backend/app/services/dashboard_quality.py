from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from .dashboard_data import empty_dataframe
from .dashboard_metrics import clean_spaces


MIN_CHART_COVERAGE = 0.70
MAX_CHART_CATEGORIES = 10
MAX_LABEL_LENGTH = 60


def _non_empty(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().ne("")


def field_coverage(df: pd.DataFrame, fields: Iterable[str]) -> pd.DataFrame:
    total = len(df)
    rows = []
    for field in fields:
        if field not in df.columns:
            present = 0
        elif df[field].dtype == bool:
            present = int(df[field].sum())
        else:
            present = int(_non_empty(df[field]).sum())
        rows.append(
            {
                "campo": field,
                "preenchidos": present,
                "total": total,
                "cobertura": (present / total) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def is_chartable_dimension(
    df: pd.DataFrame,
    column: str,
    *,
    min_coverage: float = MIN_CHART_COVERAGE,
    max_categories: int = MAX_CHART_CATEGORIES,
    max_label_length: int = MAX_LABEL_LENGTH,
) -> dict[str, Any]:
    if column not in df.columns or df.empty:
        return {
            "column": column,
            "allowed": False,
            "reason": "campo ausente ou sem registros",
            "coverage": 0.0,
            "categories": 0,
            "max_label_length": 0,
        }
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
        reasons.append(f"rotulos acima de {max_label_length} caracteres")
    if any(marker in sample for marker in ("documento assinado", "nome da autoridade", "ato de nomeacao", "responsavel pelo acompanhamento")):
        reasons.append("conteudo documental bruto")

    return {
        "column": column,
        "allowed": not reasons,
        "reason": "; ".join(reasons) if reasons else "ok",
        "coverage": coverage,
        "categories": categories,
        "max_label_length": longest,
    }


def chartability_report(checks: Iterable[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for check in checks:
        rows.append(
            {
                "campo": check.get("column", ""),
                "permitido": bool(check.get("allowed", False)),
                "motivo": check.get("reason", ""),
                "cobertura": float(check.get("coverage", 0.0) or 0.0),
                "categorias": int(check.get("categories", 0) or 0),
                "maior_rotulo": int(check.get("max_label_length", 0) or 0),
            }
        )
    return pd.DataFrame(rows)


def review_summary(review_df: pd.DataFrame) -> pd.DataFrame:
    if review_df.empty:
        return empty_dataframe(["severity", "document_type", "code", "total"])
    return (
        review_df.groupby(["severity", "document_type", "code"], dropna=False)
        .size()
        .rename("total")
        .reset_index()
        .sort_values(["severity", "document_type", "code"])
    )


def conflicts_dataframe(portfolio_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if not portfolio_df.empty:
        for record in portfolio_df.to_dict(orient="records"):
            if clean_spaces(record.get("conflitos", "")) or clean_spaces(record.get("situacao_carteira", "")) == "inconsistente_ou_revisar":
                rows.append(
                    {
                        "processo": clean_spaces(record.get("processo", "")),
                        "origem": "carteira",
                        "situacao": clean_spaces(record.get("situacao_carteira", "")),
                        "descricao": clean_spaces(record.get("conflitos", "")),
                    }
                )
    if not history_df.empty:
        for record in history_df.to_dict(orient="records"):
            if clean_spaces(record.get("situacao_carteira", "")) == "inconsistente_ou_revisar":
                rows.append(
                    {
                        "processo": clean_spaces(record.get("processo", "")),
                        "origem": "historico",
                        "situacao": clean_spaces(record.get("situacao_carteira", "")),
                        "descricao": clean_spaces(record.get("conflitos", "")),
                    }
                )
    return pd.DataFrame(rows, columns=["processo", "origem", "situacao", "descricao"])


def duplicate_keys_dataframe(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_df.empty or "chave_canonica" not in portfolio_df.columns:
        return empty_dataframe(["chave_canonica", "total"])
    duplicates = (
        portfolio_df.groupby("chave_canonica", dropna=False)
        .size()
        .rename("total")
        .reset_index()
    )
    return duplicates[duplicates["total"] > 1].copy()


def build_quality_model(bundle: dict[str, Any], dashboard_model: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    active_df = dashboard_model.get("active", empty_dataframe([]))
    ted_df = dashboard_model.get("ted", empty_dataframe([]))
    portfolio_df = dashboard_model.get("portfolio", empty_dataframe([]))
    history_df = dashboard_model.get("history", empty_dataframe([]))
    review_df = bundle.get("review_queue", empty_dataframe([]))

    return {
        "coverage": field_coverage(active_df, ["vigencia_fim", "parceiro", "objeto_completo", "possui_pt", "possui_ted"]),
        "ted_coverage": field_coverage(ted_df, ["valor_global", "vigencia_fim", "unidade_descentralizadora"]),
        "chartability": chartability_report(
            [
                is_chartable_dimension(active_df, "parceiro"),
                is_chartable_dimension(ted_df, "unidade_descentralizadora"),
                is_chartable_dimension(history_df, "status_normalizado"),
            ]
        ),
        "review_summary": review_summary(review_df),
        "review_queue": review_df.drop(columns=[column for column in ("json_path",) if column in review_df.columns], errors="ignore"),
        "conflicts": conflicts_dataframe(portfolio_df, history_df),
        "duplicates": duplicate_keys_dataframe(portfolio_df),
    }
