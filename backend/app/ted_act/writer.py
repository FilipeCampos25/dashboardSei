from __future__ import annotations

"""Persistence helpers for isolated TED/ACT artifacts."""

import json
from pathlib import Path
from typing import Any

from ..output.csv_writer import ensure_output_dir, write_csv
from .normalizer import CONSOLIDATED_COLUMNS


class TedActWriter:
    """Persist consolidated TED/ACT artifacts to a dedicated output directory."""

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = ensure_output_dir(output_dir)

    @property
    def consolidado_json_path(self) -> Path:
        """Canonical consolidated JSON path."""
        return self.output_dir / "ted_act_consolidado.json"

    @property
    def consolidado_csv_path(self) -> Path:
        """Canonical consolidated CSV path."""
        return self.output_dir / "ted_act_consolidado.csv"

    @property
    def nao_encontrados_path(self) -> Path:
        """Canonical not-found JSON path."""
        return self.output_dir / "ted_act_nao_encontrados.json"

    @property
    def relatorio_execucao_path(self) -> Path:
        """Canonical execution report JSON path."""
        return self.output_dir / "relatorio_execucao.json"

    def write_consolidado(self, records: list[dict[str, Any]]) -> dict[str, Path]:
        """Write the consolidated dataset as JSON and CSV."""
        self.consolidado_json_path.write_text(
            json.dumps(self._serialize(records), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        write_csv(records, self.consolidado_csv_path, columns=CONSOLIDATED_COLUMNS)
        return {
            "json": self.consolidado_json_path,
            "csv": self.consolidado_csv_path,
        }

    def write_nao_encontrados(self, payload: dict[str, Any]) -> Path:
        """Write the JSON payload for not-found processes and invalid source files."""
        self.nao_encontrados_path.write_text(
            json.dumps(self._serialize(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return self.nao_encontrados_path

    def write_relatorio_execucao(self, payload: dict[str, Any]) -> Path:
        """Write the execution report JSON."""
        self.relatorio_execucao_path.write_text(
            json.dumps(self._serialize(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return self.relatorio_execucao_path

    def _serialize(self, value: Any) -> Any:
        """Convert non-JSON-safe values such as Path recursively."""
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(key): self._serialize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._serialize(item) for item in value]
        return value
