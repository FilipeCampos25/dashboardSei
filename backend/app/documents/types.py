from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Protocol


class DocumentTypeHandler(Protocol):
    def reset_run(self) -> None:
        ...

    def process_snapshot(
        self,
        *,
        spec: "DocumentTypeSpec",
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        collection_context: Optional[dict[str, Any]] = None,
        analysis: Optional[dict[str, Any]] = None,
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> Optional[Path]:
        ...

    def finalize_run(
        self,
        *,
        spec: "DocumentTypeSpec",
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> None:
        ...


@dataclass(frozen=True)
class DocumentTypeSpec:
    key: str
    display_name: str
    search_terms: tuple[str, ...]
    tree_match_terms: tuple[str, ...]
    snapshot_prefix: str
    log_label: str
    cleanup_patterns: tuple[str, ...]
    handler: DocumentTypeHandler
    accepted_doc_classes: tuple[str, ...] = ()
    filter_type_aliases: tuple[str, ...] = ()
