from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from app.documents.common import build_basic_tracking_record, save_snapshot_json
from app.documents.types import DocumentTypeSpec
from app.output import csv_writer
from app.services.act_normalizer import classify_act_snapshot, export_normalized_csv


class CooperationDocumentHandler:
    def __init__(self, *, status_filename: str, export_act_normalized: bool = False) -> None:
        self._tracking_records: List[Dict[str, Any]] = []
        self._status_filename = status_filename
        self._export_act_normalized = export_act_normalized

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
        analysis = classify_act_snapshot(snapshot, collection_context)
        resolved_document_type = analysis.get("resolved_document_type", "")
        snapshot_prefix = analysis.get("snapshot_prefix", spec.snapshot_prefix)
        output_path = save_snapshot_json(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            output_dir=output_dir,
            logger=logger,
            snapshot_prefix_override=snapshot_prefix,
            extra_payload={
                "document_family": "cooperacao",
                "resolved_document_type": resolved_document_type,
                "snapshot_prefix": snapshot_prefix,
                "collection": collection_context or {},
                "analysis": analysis,
            },
        )
        record = build_basic_tracking_record(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            output_path=output_path,
            collection_context=collection_context,
        )
        record.update(
            {
                "doc_class": analysis.get("doc_class", ""),
                "resolved_document_type": resolved_document_type,
                "snapshot_prefix": snapshot_prefix,
                "is_canonical_candidate": bool(analysis.get("is_canonical_candidate")),
                "normalization_status": analysis.get("normalization_status", ""),
                "discard_reason": analysis.get("discard_reason", ""),
                "classification_reason": analysis.get("classification_reason", ""),
            }
        )
        self._tracking_records.append(record)
        logger.info(
            "Processo %s: snapshot %s extraido (texto_chars=%d, tabelas=%d, doc_class=%s).",
            processo,
            spec.log_label,
            len(snapshot.get("text", "") or ""),
            len(snapshot.get("tables", []) or []),
            analysis.get("doc_class", ""),
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
                "doc_class": "",
                "resolved_document_type": "",
                "snapshot_prefix": "",
                "is_canonical_candidate": False,
                "normalization_status": "not_found",
                "discard_reason": "not_found",
                "classification_reason": "",
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
                "doc_class": "",
                "resolved_document_type": "",
                "snapshot_prefix": "",
                "is_canonical_candidate": False,
                "normalization_status": "extraction_failure",
                "discard_reason": "extraction_failure",
                "classification_reason": "",
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
            "doc_class",
            "resolved_document_type",
            "snapshot_prefix",
            "is_canonical_candidate",
            "normalization_status",
            "discard_reason",
            "classification_reason",
            "json_path",
        ]
        status_path = output_dir / self._status_filename
        csv_writer.write_csv(self._tracking_records, status_path, columns=columns)
        logger.info(
            "Relatorio %s gerado: total=%d arquivo=%s",
            spec.log_label,
            len(self._tracking_records),
            status_path,
        )
        if not self._export_act_normalized:
            return
        try:
            export_result = export_normalized_csv(output_dir, logger=logger)
            if export_result.get("latest_path"):
                logger.info(
                    "Relatorio %s normalizado gerado: registros=%d latest=%s auditoria=%s",
                    spec.log_label,
                    int(export_result.get("records", 0) or 0),
                    export_result["latest_path"],
                    export_result.get("audit_path", ""),
                )
        except Exception as exc:
            logger.warning("Falha ao gerar CSV %s normalizado (%s).", spec.log_label, exc)
