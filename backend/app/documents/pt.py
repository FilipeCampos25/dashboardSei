from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.raw_date_field_collector import collect_raw_fields, export_raw_fields_csv
from app.documents.common import (
    build_basic_tracking_record,
    derive_search_outcome_status,
    sanitize_snapshot,
    sanitize_text_payload,
    save_snapshot_json,
)
from app.documents.types import DocumentTypeSpec
from app.output import csv_writer
from app.rpa.sei import document_text_extractor
from app.services.dashboard_exporter import export_dashboard_ready_csv
from app.services.pt_normalizer import PUBLICATION_STATUS_SILVER, export_normalized_csv


class PTDocumentHandler:
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
        analysis: Optional[dict[str, Any]] = None,
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> Optional[Path]:
        csv_writer.ensure_output_dir(output_dir)
        snapshot = sanitize_snapshot(snapshot)
        prazos = document_text_extractor.parse_prazos(snapshot.get("text", "") or "", logger=logger)
        tables = snapshot.get("tables", [])
        text = snapshot.get("text", "") or ""

        inicio_found = bool(prazos.get("inicio_data") or prazos.get("inicio_raw"))
        termino_found = bool(prazos.get("termino_data") or prazos.get("termino_raw"))
        logger.info(
            "Processo %s: snapshot %s extraido (texto_chars=%d, tabelas=%d, inicio=%s, termino=%s).",
            processo,
            spec.log_label,
            len(text),
            len(tables) if isinstance(tables, list) else 0,
            "sim" if inicio_found else "nao",
            "sim" if termino_found else "nao",
        )

        if settings.export_raw_fields_csv:
            try:
                raw_fields = collect_raw_fields(text, tables if isinstance(tables, list) else [])
                raw_csv_path = output_dir / "pt_fields_raw.csv"
                export_raw_fields_csv(
                    out_csv_path=str(raw_csv_path),
                    processo_sei=processo,
                    doc_title=(snapshot.get("title", "") or ""),
                    doc_url=(snapshot.get("url", "") or ""),
                    raw_fields=raw_fields,
                    captured_at=datetime.now().isoformat(timespec="seconds"),
                )
                logger.info(
                    "Processo %s: CSV raw atualizado em %s (+%d linha(s)).",
                    processo,
                    raw_csv_path,
                    len(raw_fields),
                )
            except Exception as exc:
                logger.warning(
                    "Processo %s: falha ao exportar campos raw para CSV (%s).",
                    processo,
                    exc,
                )

        output_path = self._save_snapshot_json(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            prazos=prazos,
            analysis=analysis,
            collection_context=collection_context,
            output_dir=output_dir,
            logger=logger,
        )
        self._register_tracking_record(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            prazos=prazos,
            analysis=analysis,
            output_path=output_path,
            collection_context=collection_context,
        )
        return output_path

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
        all_columns = [
            "captured_at",
            "processo",
            "documento",
            "found",
            "found_in",
            "search_term",
            "results_count",
            "chosen_documento",
            "selection_reason",
            "selection_detail",
            "extraction_error",
            "snapshot_mode",
            "text_chars",
            "tables_count",
            "prazos_status",
            "inicio_data",
            "inicio_raw",
            "termino_data",
            "termino_raw",
            "tem_inicio",
            "tem_termino",
            "sem_prazo",
            "classification_reason",
            "validation_status",
            "publication_status",
            "normalization_status",
            "json_path",
        ]
        _, sem_path, sem_records = self._write_status_exports(
            output_dir=output_dir,
            columns=all_columns,
        )

        logger.info(
            "Relatorio %s gerado: total=%d sem_prazo=%d arquivo=%s",
            spec.log_label,
            len(self._tracking_records),
            sem_records,
            sem_path,
        )
        try:
            export_result = export_normalized_csv(output_dir, logger=logger)
            self._sync_tracking_records_with_audit(export_result.get("audit_path"))
            self._write_status_exports(
                output_dir=output_dir,
                columns=all_columns,
            )
            if export_result.get("latest_path"):
                logger.info(
                    "Relatorio %s normalizado gerado: registros=%d latest=%s",
                    spec.log_label,
                    int(export_result.get("records", 0) or 0),
                    export_result["latest_path"],
                )
        except Exception as exc:
            logger.warning("Falha ao gerar CSV %s normalizado (%s).", spec.log_label, exc)
        try:
            export_dashboard_ready_csv(output_dir, logger=logger)
        except Exception as exc:
            logger.warning("Falha ao gerar CSV dashboard_ready_latest.csv (%s).", exc)

    def _save_snapshot_json(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        prazos: dict[str, str],
        analysis: Optional[dict[str, Any]],
        collection_context: Optional[dict[str, Any]],
        output_dir: Path,
        logger: Any,
    ) -> Optional[Path]:
        return save_snapshot_json(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            output_dir=output_dir,
            logger=logger,
            extra_payload={
                "document_family": "pt",
                "resolved_document_type": "plano_trabalho",
                "requested_type": spec.key,
                "collection": collection_context or {},
                "prazos": prazos,
                "analysis": analysis or {},
            },
        )

    def _register_tracking_record(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        prazos: dict[str, str],
        analysis: Optional[dict[str, Any]],
        output_path: Optional[Path],
        collection_context: Optional[dict[str, Any]] = None,
    ) -> None:
        inicio_found = bool(prazos.get("inicio_data") or prazos.get("inicio_raw"))
        termino_found = bool(prazos.get("termino_data") or prazos.get("termino_raw"))
        sem_prazo = not (inicio_found and termino_found)
        record: Dict[str, Any] = build_basic_tracking_record(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            output_path=output_path,
            collection_context=collection_context,
        )
        record.update(
            {
                "prazos_status": (prazos.get("status", "") or ""),
                "inicio_data": (prazos.get("inicio_data", "") or ""),
                "inicio_raw": (prazos.get("inicio_raw", "") or ""),
                "termino_data": (prazos.get("termino_data", "") or ""),
                "termino_raw": (prazos.get("termino_raw", "") or ""),
                "tem_inicio": inicio_found,
                "tem_termino": termino_found,
                "sem_prazo": sem_prazo,
                "classification_reason": ((analysis or {}).get("classification_reason", "") or ""),
                "validation_status": ((analysis or {}).get("validation_status", "") or ""),
                "publication_status": ((analysis or {}).get("publication_status", "") or ""),
                "normalization_status": "",
            }
        )
        self._tracking_records.append(record)

    def record_search_outcome(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        collection_context: dict[str, Any],
    ) -> None:
        sanitized_context = sanitize_text_payload(collection_context)
        context = sanitized_context if isinstance(sanitized_context, dict) else collection_context
        outcome_status = derive_search_outcome_status(context)
        self._tracking_records.append(
            {
                "captured_at": context.get("captured_at", ""),
                "processo": processo,
                "documento": context.get("chosen_documento", ""),
                "found": bool(context.get("found")),
                "found_in": context.get("found_in", ""),
                "search_term": context.get("search_term", ""),
                "results_count": context.get("results_count", 0),
                "chosen_documento": context.get("chosen_documento", ""),
                "selection_reason": context.get("selection_reason", ""),
                "selection_detail": context.get("selection_detail", ""),
                "extraction_error": context.get("extraction_error", ""),
                "snapshot_mode": "",
                "text_chars": 0,
                "tables_count": 0,
                "prazos_status": "",
                "inicio_data": "",
                "inicio_raw": "",
                "termino_data": "",
                "termino_raw": "",
                "tem_inicio": False,
                "tem_termino": False,
                "sem_prazo": False,
                "classification_reason": "",
                "validation_status": outcome_status["validation_status"],
                "publication_status": PUBLICATION_STATUS_SILVER,
                "normalization_status": outcome_status["normalization_status"],
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
        sanitized_context = sanitize_text_payload(collection_context)
        context = sanitized_context if isinstance(sanitized_context, dict) else collection_context
        self._tracking_records.append(
            {
                "captured_at": context.get("captured_at", ""),
                "processo": processo,
                "documento": protocolo_documento,
                "found": bool(context.get("found")),
                "found_in": context.get("found_in", ""),
                "search_term": context.get("search_term", ""),
                "results_count": context.get("results_count", 0),
                "chosen_documento": context.get("chosen_documento", protocolo_documento),
                "selection_reason": context.get("selection_reason", ""),
                "selection_detail": context.get("selection_detail", ""),
                "extraction_error": context.get("extraction_error", ""),
                "snapshot_mode": "",
                "text_chars": 0,
                "tables_count": 0,
                "prazos_status": "",
                "inicio_data": "",
                "inicio_raw": "",
                "termino_data": "",
                "termino_raw": "",
                "tem_inicio": False,
                "tem_termino": False,
                "sem_prazo": False,
                "classification_reason": "",
                "validation_status": "extraction_failure",
                "publication_status": PUBLICATION_STATUS_SILVER,
                "normalization_status": "extraction_failure",
                "json_path": "",
            }
        )

    def _write_status_exports(
        self,
        *,
        output_dir: Path,
        columns: List[str],
    ) -> tuple[Path, Path, int]:
        all_path = output_dir / "pt_status_execucao_latest.csv"
        csv_writer.write_csv(self._tracking_records, all_path, columns=columns)

        sem_records = [record for record in self._tracking_records if bool(record.get("sem_prazo"))]
        sem_path = output_dir / "pt_sem_prazo_latest.csv"
        csv_writer.write_csv(sem_records, sem_path, columns=columns)
        return all_path, sem_path, len(sem_records)

    def _sync_tracking_records_with_audit(self, audit_path: Any) -> None:
        if not audit_path:
            return

        path = Path(str(audit_path))
        if not path.exists():
            return

        with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            rows = list(csv.DictReader(file_obj))

        audit_by_json_path = {
            str(row.get("json_path", "") or "").strip(): row
            for row in rows
            if str(row.get("json_path", "") or "").strip()
        }
        if not audit_by_json_path:
            return

        for record in self._tracking_records:
            json_path = str(record.get("json_path", "") or "").strip()
            if not json_path:
                continue
            audit_row = audit_by_json_path.get(json_path)
            if not audit_row:
                continue
            for field in ("classification_reason", "validation_status", "publication_status", "normalization_status"):
                value = str(audit_row.get(field, "") or "").strip()
                if value:
                    record[field] = value


def build_pt_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="pt",
        display_name="Plano de Trabalho",
        search_terms=(
            "PLANO DE TRABALHO - PT",
            "Plano de Trabalho - PT",
            "Plano de Trabalho",
            "PLANO DE TRABALHO PT",
        ),
        tree_match_terms=(
            "PLANO DE TRABALHO - PT",
            "PLANO DE TRABALHO PT",
            "PLANO DE TRABALHO",
        ),
        snapshot_prefix="plano_trabalho",
        log_label="PT",
        cleanup_patterns=(
            "plano_trabalho_*.json",
            "pt_fields_raw.csv",
            "pt_status_execucao_latest.csv",
            "pt_sem_prazo_latest.csv",
            "pt_auditoria_latest.csv",
            "pt_normalizado_latest.csv",
            "pt_normalizado_completo_latest.csv",
        ),
        handler=PTDocumentHandler(),
        filter_type_aliases=("Plano de Trabalho - PT", "Plano de Trabalho"),
    )
