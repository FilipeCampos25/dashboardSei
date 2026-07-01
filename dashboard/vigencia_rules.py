from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from .data_cleaning import parse_date


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


def classify_vigencia(end_date: Any, today: date | datetime | pd.Timestamp | None = None) -> str:
    return classify_days(days_remaining(end_date, today=today))


def add_vigencia_columns(
    df: pd.DataFrame,
    end_column: str,
    *,
    today: date | datetime | pd.Timestamp | None = None,
    days_column: str = "dias_restantes",
    status_column: str = "situacao_vigencia",
) -> pd.DataFrame:
    result = df.copy()
    if end_column not in result.columns:
        result[end_column] = ""
    result[days_column] = result[end_column].apply(lambda value: days_remaining(value, today=today))
    result[status_column] = result[days_column].apply(classify_days)
    return result

