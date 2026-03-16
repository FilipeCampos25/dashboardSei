from __future__ import annotations

"""Read local process snapshots that will feed future TED/ACT lookups."""

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


PROCESS_FILENAME_PATTERN = "plano_trabalho_*.json"
PROCESS_TEXT_PATTERN = re.compile(r"Processo\s+SEI:\s*([0-9./-]+)", flags=re.IGNORECASE)
PROCESS_VALUE_PATTERN = re.compile(r"\b\d{5}\.\d{6}/\d{4}-\d{2}\b")
INSTRUMENT_NUMBER_PATTERN = re.compile(
    r"(?:acordo\s+de\s+cooperac[aã]o(?:\s+t[eé]cnica)?|memorando\s+de\s+entendimentos|termo\s+de\s+execu[cç][aã]o\s+descentralizada|ted)"
    r"[^\n]{0,40}?[nº°o]\s*([A-Za-z0-9./-]{1,30})",
    flags=re.IGNORECASE,
)
PROCESS_KEY_CANDIDATES = {
    "numeroprocesso",
    "processo",
    "processnumber",
    "processosei",
}


@dataclass(frozen=True)
class ProcessSnapshot:
    """Minimal local representation of a process snapshot."""

    processo: str
    source_path: Path
    payload: dict[str, Any]


@dataclass(frozen=True)
class ExtractedProcessRecord:
    """Normalized process entry extracted from a local JSON snapshot."""

    processo_original: str
    processo_normalizado: str
    source_path: Path
    parceiro_hint: str = ""
    numero_instrumento_hint: str = ""
    objeto_hint: str = ""
    tipo_instrumento_hint: str = ""


@dataclass(frozen=True)
class MissingProcessRecord:
    """Snapshot file that could not produce a process number."""

    source_path: Path
    reason: str


@dataclass(frozen=True)
class ProcessExtractionResult:
    """Result object for batch extraction from snapshot files."""

    processos: list[ExtractedProcessRecord]
    arquivos_sem_processo: list[MissingProcessRecord]


class ProcessReader:
    """Load and enumerate process snapshots from disk."""

    def __init__(self, input_dir: str | Path) -> None:
        self.input_dir = Path(input_dir)

    def list_snapshot_paths(self) -> list[Path]:
        """Return snapshot files ordered by name."""
        if not self.input_dir.exists():
            return []
        return sorted(self.input_dir.glob(PROCESS_FILENAME_PATTERN))

    def read_snapshots(self) -> list[ProcessSnapshot]:
        """Load valid snapshots and skip files without process identifier."""
        snapshots: list[ProcessSnapshot] = []
        for path in self.list_snapshot_paths():
            payload = self._read_json(path)
            processo = self.extract_processo(payload)
            if not processo:
                continue
            snapshots.append(ProcessSnapshot(processo=processo, source_path=path, payload=payload))
        return snapshots

    def extract_process_records(self) -> ProcessExtractionResult:
        """Return deduplicated process entries and files without process number."""
        seen: set[str] = set()
        processos: list[ExtractedProcessRecord] = []
        arquivos_sem_processo: list[MissingProcessRecord] = []
        preview_map = self._load_preview_map()

        for path in self.list_snapshot_paths():
            try:
                payload = self._read_json(path)
            except Exception as exc:
                arquivos_sem_processo.append(
                    MissingProcessRecord(source_path=path, reason=f"json_invalido: {exc}")
                )
                continue

            processo_original = self.extract_processo(payload)
            if not processo_original:
                arquivos_sem_processo.append(
                    MissingProcessRecord(source_path=path, reason="processo_nao_encontrado")
                )
                continue

            processo_normalizado = self.normalize_processo(processo_original)
            if not processo_normalizado or processo_normalizado in seen:
                continue

            seen.add(processo_normalizado)
            preview = preview_map.get(processo_normalizado, {})
            processos.append(
                ExtractedProcessRecord(
                    processo_original=processo_original,
                    processo_normalizado=processo_normalizado,
                    source_path=path,
                    parceiro_hint=str(preview.get("parceiro", "") or "").strip(),
                    numero_instrumento_hint=self._extract_numero_instrumento(payload, preview),
                    objeto_hint=str(preview.get("objeto", "") or "").strip(),
                    tipo_instrumento_hint=self._infer_instrument_type(payload, preview),
                )
            )

        return ProcessExtractionResult(
            processos=processos,
            arquivos_sem_processo=arquivos_sem_processo,
        )

    def extract_process_numbers(self) -> list[str]:
        """Return unique normalized process numbers preserving first-seen order."""
        result = self.extract_process_records()
        return [record.processo_normalizado for record in result.processos]

    def extract_processo(self, payload: dict[str, Any]) -> str:
        """Extract the process number from direct fields or nested structures."""
        for candidate in self._iter_candidate_values(payload):
            processo = self._extract_process_from_value(candidate)
            if processo:
                return processo
        return ""

    def normalize_processo(self, processo: str) -> str:
        """Normalize a process number to the canonical SEI mask."""
        digits = re.sub(r"\D", "", processo or "")
        if len(digits) != 17:
            return (processo or "").strip()
        return f"{digits[:5]}.{digits[5:11]}/{digits[11:15]}-{digits[15:17]}"

    def _read_json(self, path: Path) -> dict[str, Any]:
        """Read a snapshot file as UTF-8 JSON."""
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_preview_map(self) -> dict[str, dict[str, str]]:
        """Load PARCERIAS VIGENTES hints from a local CSV when available."""
        csv_path = self.input_dir / "parcerias_vigentes_latest.csv"
        if not csv_path.exists():
            return {}

        preview_map: dict[str, dict[str, str]] = {}
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                processo = self.normalize_processo(str(row.get("processo", "") or ""))
                if not processo or processo in preview_map:
                    continue
                preview_map[processo] = {str(k): str(v or "") for k, v in row.items()}
        return preview_map

    def _iter_candidate_values(self, payload: Any) -> Iterable[Any]:
        """Yield likely process values first, then all nested scalar values."""
        prioritized: list[Any] = []
        fallback: list[Any] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    normalized_key = self._normalize_key(key)
                    if normalized_key in PROCESS_KEY_CANDIDATES:
                        prioritized.append(value)
                    walk(value)
                return
            if isinstance(node, list):
                for item in node:
                    walk(item)
                return
            if isinstance(node, (str, int, float)):
                fallback.append(node)

        walk(payload)
        yield from prioritized
        yield from fallback

    def _extract_process_from_value(self, value: Any) -> str:
        """Extract a process number from a scalar or nested scalar payload."""
        if isinstance(value, (dict, list)):
            for nested in self._iter_candidate_values(value):
                processo = self._extract_process_from_value(nested)
                if processo:
                    return processo
            return ""

        text = str(value or "").strip()
        if not text:
            return ""

        direct_match = PROCESS_VALUE_PATTERN.search(text)
        if direct_match:
            return direct_match.group(0)

        label_match = PROCESS_TEXT_PATTERN.search(text)
        if label_match:
            return label_match.group(1).strip()

        normalized_digits = re.sub(r"\D", "", text)
        if len(normalized_digits) == 17:
            return self.normalize_processo(normalized_digits)
        return ""

    def _normalize_key(self, key: Any) -> str:
        """Normalize a JSON key for case-insensitive matching."""
        return re.sub(r"[^a-z0-9]", "", str(key or "").lower())

    def _extract_numero_instrumento(self, payload: dict[str, Any], preview: dict[str, str]) -> str:
        """Extract an instrument number hint from preview CSV or snapshot text."""
        numero_preview = str(preview.get("numero_act", "") or "").strip()
        if numero_preview:
            return numero_preview

        text = str(payload.get("snapshot", {}).get("text", "") or "")
        match = INSTRUMENT_NUMBER_PATTERN.search(text)
        return match.group(1).strip() if match else ""

    def _infer_instrument_type(self, payload: dict[str, Any], preview: dict[str, str]) -> str:
        """Infer the likely instrument type from snapshot text and preview data."""
        text = str(payload.get("snapshot", {}).get("text", "") or "").lower()
        numero_act = str(preview.get("numero_act", "") or "").strip()
        if "termo de execução descentralizada" in text or re.search(r"\bted\b", text):
            return "TED"
        if "memorando de entendimentos" in text:
            return "MEMORANDO"
        if "acordo de cooperação técnica" in text:
            return "ACT"
        if "acordo de cooperação" in text or numero_act:
            return "ACORDO"
        return ""


def extract_processes(input_dir: str | Path = "debug") -> ProcessExtractionResult:
    """Convenience function to extract process records from a snapshot directory."""
    return ProcessReader(input_dir).extract_process_records()


if __name__ == "__main__":
    result = extract_processes()
    printable = {
        "processos": [
            {
                **asdict(record),
                "source_path": str(record.source_path),
            }
            for record in result.processos
        ],
        "arquivos_sem_processo": [
            {
                **asdict(record),
                "source_path": str(record.source_path),
            }
            for record in result.arquivos_sem_processo
        ],
    }
    print(json.dumps(printable, ensure_ascii=False, indent=2))
