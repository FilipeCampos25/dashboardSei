from __future__ import annotations

import calendar
from datetime import date, timedelta


def add_inclusive_duration(start_iso: str, amount: int, unit: str) -> str:
    """Resolve a calendar duration whose first and last dates are both included."""
    if not start_iso or amount <= 0:
        return ""
    try:
        base = date.fromisoformat(start_iso)
        normalized_unit = " ".join(str(unit or "").lower().split())
        if "ano" in normalized_unit:
            target_year = base.year + amount
            target = date(target_year, base.month, min(base.day, calendar.monthrange(target_year, base.month)[1]))
        elif "mes" in normalized_unit:
            month_index = base.month - 1 + amount
            target_year = base.year + month_index // 12
            target_month = month_index % 12 + 1
            target = date(
                target_year,
                target_month,
                min(base.day, calendar.monthrange(target_year, target_month)[1]),
            )
        elif "dia" in normalized_unit:
            target = base + timedelta(days=amount)
        else:
            return ""
        return (target - timedelta(days=1)).isoformat()
    except (TypeError, ValueError, OverflowError):
        return ""
