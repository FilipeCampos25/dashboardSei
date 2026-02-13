from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


class ReportBuilder:
    def __init__(self, records: list[dict[str, Any]]):
        self.df = pd.DataFrame(records)

    def to_csv(self, path: str | Path) -> None:
        self.df.to_csv(path, index=False, encoding="utf-8-sig")

    def to_excel(self, path: str | Path) -> None:
        self.df.to_excel(path, index=False)
