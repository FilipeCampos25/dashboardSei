from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any

import pandas as pd

from .category_models import MISSING_DATE, MISSING_TEXT


PROCESSO_RE = re.compile(r"^\d{5}\.\d{6}/\d{4}-\d{2}$")


def clean_spaces(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return " ".join(str(value).replace("\r", "\n").split()).strip()


def is_blank(value: Any) -> bool:
    return clean_spaces(value) == ""


def boolish(value: Any) -> bool:
    return clean_spaces(value).lower() in {"1", "true", "sim", "yes"}


def normalize_processo(value: Any) -> str:
    cleaned = clean_spaces(value)
    if not cleaned:
        return ""
    compact = re.sub(r"\s+", "", cleaned).translate(
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


def normalize_key(value: Any, fallback: str = "") -> str:
    cleaned = clean_spaces(value or fallback)
    return re.sub(r"[^A-Za-z0-9]+", "", cleaned).upper()


def first_non_empty(*values: Any) -> str:
    for value in values:
        cleaned = clean_spaces(value)
        if cleaned:
            return cleaned
    return ""


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


def format_date_iso(value: Any) -> str:
    parsed = parse_date(value)
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def format_date_display(value: Any) -> str:
    parsed = parse_date(value)
    if pd.isna(parsed):
        return MISSING_DATE
    return parsed.strftime("%d/%m/%Y")


def format_datetime_display(value: Any) -> str:
    cleaned = clean_spaces(value)
    if not cleaned:
        return MISSING_DATE
    parsed = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(parsed):
        return MISSING_DATE
    return parsed.strftime("%d/%m/%Y %H:%M")


def summarize_text(value: Any, limit: int = 140) -> str:
    text = clean_spaces(value)
    if not text:
        return MISSING_TEXT
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def display_text(value: Any, missing: str = MISSING_TEXT) -> str:
    cleaned = clean_spaces(value)
    return cleaned if cleaned else missing


def parse_money(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(value):
            return None
        return float(value)
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


def is_valid_money(value: Any) -> bool:
    parsed = parse_money(value)
    return parsed is not None and parsed > 0


def format_currency(value: Any, *, missing: str = MISSING_TEXT) -> str:
    parsed = parse_money(value)
    if parsed is None or parsed <= 0:
        return missing
    formatted = f"R$ {parsed:,.2f}"
    return formatted.replace(",", "_").replace(".", ",").replace("_", ".")


def join_non_empty(values: list[str], separator: str = "; ") -> str:
    unique: list[str] = []
    for value in values:
        cleaned = clean_spaces(value)
        if cleaned and cleaned not in unique:
            unique.append(cleaned)
    return separator.join(unique)

