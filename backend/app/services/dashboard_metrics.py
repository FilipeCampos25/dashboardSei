from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd


DEADLINE_LABELS = {
    "verde": "Verde",
    "amarelo": "Amarelo",
    "vermelho": "Vermelho",
    "sem_data": "Sem data",
}


def clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def parse_date(value: Any) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value.normalize() if not pd.isna(value) else pd.NaT
    if isinstance(value, datetime):
        return pd.Timestamp(value).normalize()
    if isinstance(value, date):
        return pd.Timestamp(value).normalize()

    cleaned = clean_spaces(value)
    if not cleaned:
        return pd.NaT
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cleaned):
        parsed = pd.to_datetime(cleaned, format="%Y-%m-%d", errors="coerce")
    else:
        parsed = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)
    return parsed.normalize() if not pd.isna(parsed) else pd.NaT


def format_date(value: Any) -> str:
    parsed = parse_date(value)
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def days_remaining(end_date: Any, today: date | datetime | pd.Timestamp | None = None) -> int | None:
    parsed = parse_date(end_date)
    if pd.isna(parsed):
        return None
    reference = pd.Timestamp(today or date.today()).normalize()
    return int((parsed.normalize() - reference).days)


def classify_days(days: int | float | None) -> str:
    if days is None or pd.isna(days):
        return "sem_data"
    numeric_days = int(days)
    if numeric_days > 365:
        return "verde"
    if 181 <= numeric_days <= 365:
        return "amarelo"
    return "vermelho"


def classify_deadline(end_date: Any, today: date | datetime | pd.Timestamp | None = None) -> str:
    return classify_days(days_remaining(end_date, today=today))


def add_deadline_columns(
    df: pd.DataFrame,
    end_column: str,
    *,
    today: date | datetime | pd.Timestamp | None = None,
    days_column: str = "dias_restantes",
    indicator_column: str = "indicador_vigencia",
) -> pd.DataFrame:
    result = df.copy()
    if end_column not in result.columns:
        result[end_column] = ""
    result[days_column] = result[end_column].apply(lambda value: days_remaining(value, today=today))
    result[indicator_column] = result[days_column].apply(classify_days)
    return result


def parse_money(value: Any) -> float | None:
    text = clean_spaces(value).replace("R$", "").replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return None


def money_to_float(value: Any) -> float:
    parsed = parse_money(value)
    return float(parsed) if parsed is not None else 0.0


def format_currency(value: Any) -> str:
    try:
        numeric = float(value or 0.0)
    except Exception:
        numeric = 0.0
    formatted = f"R$ {numeric:,.2f}"
    return formatted.replace(",", "_").replace(".", ",").replace("_", ".")


def summarize_text(value: Any, limit: int = 140) -> str:
    text = clean_spaces(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def non_empty_count(series: pd.Series) -> int:
    if series.empty:
        return 0
    if pd.api.types.is_bool_dtype(series):
        return int(series.sum())
    non_null = series.dropna()
    if not non_null.empty and non_null.map(lambda value: isinstance(value, bool)).all():
        return int(non_null.sum())
    return int(series.fillna("").astype(str).str.strip().ne("").sum())


def coverage_rows(df: pd.DataFrame, fields: Iterable[str]) -> pd.DataFrame:
    total = len(df)
    rows = []
    for field in fields:
        if field in df.columns:
            present = non_empty_count(df[field])
        else:
            present = 0
        rows.append(
            {
                "campo": field,
                "preenchidos": present,
                "total": total,
                "cobertura": (present / total) if total else 0.0,
            }
        )
    return pd.DataFrame(rows, columns=["campo", "preenchidos", "total", "cobertura"])
