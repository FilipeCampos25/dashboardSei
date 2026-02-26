from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd


def ensure_output_dir(path: str | Path) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_csv(
    records: list[dict[str, Any]],
    filepath: str | Path,
    columns: Sequence[str] | None = None,
) -> None:
    df = pd.DataFrame(records, columns=list(columns) if columns is not None else None)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
