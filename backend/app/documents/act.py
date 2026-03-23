from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from app.documents.common import build_basic_tracking_record, save_snapshot_json
from app.documents.types import DocumentTypeSpec
from app.output import csv_writer


class ACTDocumentHandler:
    def __init__(self) -> None:
        self._tracking_records: List[Dict[str, Any]] = []

    def reset_run(self) -> None:
        self._tracking_records = []

    def process_snapshot(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        collection_context: Optional[dict[str, Any]] = None,
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> Optional[Path]:
        csv_writer.ensure_output_dir(output_dir)
        output_path = save_snapshot_json(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            output_dir=output_dir,
            logger=logger,
            extra_payload={"collection": collection_context or {}},
        )
        self._tracking_records.append(
            build_basic_tracking_record(
                spec=spec,
                processo=processo,
                protocolo_documento=protocolo_documento,
                snapshot=snapshot,
                output_path=output_path,
                collection_context=collection_context,
            )
        )
        logger.info(
            "Processo %s: snapshot %s extraido (texto_chars=%d, tabelas=%d).",
            processo,
            spec.log_label,
            len(snapshot.get("text", "") or ""),
            len(snapshot.get("tables", []) or []),
        )
        return output_path

    def record_search_outcome(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        collection_context: dict[str, Any],
    ) -> None:
        self._tracking_records.append(
            {
                "captured_at": collection_context.get("captured_at", ""),
                "document_type": spec.key,
                "processo": processo,
                "documento": collection_context.get("chosen_documento", ""),
                "found": bool(collection_context.get("found")),
                "found_in": collection_context.get("found_in", ""),
                "search_term": collection_context.get("search_term", ""),
                "results_count": collection_context.get("results_count", 0),
                "chosen_documento": collection_context.get("chosen_documento", ""),
                "selection_reason": collection_context.get("selection_reason", ""),
                "selection_detail": collection_context.get("selection_detail", ""),
                "snapshot_mode": "",
                "text_chars": 0,
                "tables_count": 0,
                "extraction_error": collection_context.get("extraction_error", ""),
                "json_path": "",
            }
        )

    def record_extraction_failure(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        collection_context: dict[str, Any],
    ) -> None:
        self._tracking_records.append(
            {
                "captured_at": collection_context.get("captured_at", ""),
                "document_type": spec.key,
                "processo": processo,
                "documento": protocolo_documento,
                "found": bool(collection_context.get("found")),
                "found_in": collection_context.get("found_in", ""),
                "search_term": collection_context.get("search_term", ""),
                "results_count": collection_context.get("results_count", 0),
                "chosen_documento": collection_context.get("chosen_documento", protocolo_documento),
                "selection_reason": collection_context.get("selection_reason", ""),
                "selection_detail": collection_context.get("selection_detail", ""),
                "snapshot_mode": "",
                "text_chars": 0,
                "tables_count": 0,
                "extraction_error": collection_context.get("extraction_error", ""),
                "json_path": "",
            }
        )

    def finalize_run(
        self,
        *,
        spec: DocumentTypeSpec,
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> None:
        if not self._tracking_records:
            return

        csv_writer.ensure_output_dir(output_dir)
        columns = [
            "captured_at",
            "document_type",
            "processo",
            "documento",
            "found",
            "found_in",
            "search_term",
            "results_count",
            "chosen_documento",
            "selection_reason",
            "selection_detail",
            "snapshot_mode",
            "text_chars",
            "tables_count",
            "extraction_error",
            "json_path",
        ]
        status_path = output_dir / "act_status_execucao_latest.csv"
        csv_writer.write_csv(self._tracking_records, status_path, columns=columns)
        logger.info(
            "Relatorio %s gerado: total=%d arquivo=%s",
            spec.log_label,
            len(self._tracking_records),
            status_path,
        )


def build_act_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="act",
        display_name="Acordo de Cooperacao Tecnica",
        search_terms=(
            "ACORDO DE COOPERACAO TECNICA - ACT",
            "ACORDO DE COOPERA\u00c7\u00c3O T\u00c9CNICA - ACT",
            "ACORDO DE COOPERACAO TECNICA",
            "ACORDO DE COOPERA\u00c7\u00c3O T\u00c9CNICA",
            "Acordo de Cooperacao Tecnica - ACT",
            "Acordo de Coopera\u00e7\u00e3o T\u00e9cnica - ACT",
            "Acordo de Cooperacao Tecnica",
            "Acordo de Coopera\u00e7\u00e3o T\u00e9cnica",
            "MEMORANDO DE ENTENDIMENTOS",
            "Memorando de Entendimentos",
            "TED - TERMO DE EXECUCAO DESCENTRALIZADA",
            "TED - TERMO DE EXECU\u00c7\u00c3O DESCENTRALIZADA",
            "TED -Termo de Execucao Descentralizada",
            "TED -Termo de Execu\u00e7\u00e3o Descentralizada",
            "TED - Termo de Execucao Descentralizada",
            "TED - Termo de Execu\u00e7\u00e3o Descentralizada",
        ),
        tree_match_terms=(
            "memorando de entendimentos",
            "ted - termo de execucao descentralizada",
            "ted - termo de execu\u00e7\u00e3o descentralizada",
            "termo de execucao descentralizada",
            "termo de execu\u00e7\u00e3o descentralizada",
            "acordo de cooperacao tecnica - act",
            "acordo de coopera\u00e7\u00e3o t\u00e9cnica - act",
            "acordo de cooperacao tecnica",
            "acordo de coopera\u00e7\u00e3o t\u00e9cnica",
            "act",
        ),
        snapshot_prefix="acordo_cooperacao_tecnica",
        log_label="ACT",
        cleanup_patterns=(
            "acordo_cooperacao_tecnica_*.json",
            "act_status_execucao_latest.csv",
        ),
        handler=ACTDocumentHandler(),
    )
