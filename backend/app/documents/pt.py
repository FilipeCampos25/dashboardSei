from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.raw_date_field_collector import collect_raw_fields, export_raw_fields_csv
from app.documents.common import build_basic_tracking_record, save_snapshot_json
from app.documents.types import DocumentTypeSpec
from app.output import csv_writer
from app.rpa.sei import document_text_extractor
from app.services.pt_normalizer import export_normalized_csv


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
        output_dir: Path,
        logger: Any,
        settings: Any,
    ) -> Optional[Path]:
        csv_writer.ensure_output_dir(output_dir)
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
            output_dir=output_dir,
            logger=logger,
        )
        self._register_tracking_record(
            spec=spec,
            processo=processo,
            protocolo_documento=protocolo_documento,
            snapshot=snapshot,
            prazos=prazos,
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
            "json_path",
        ]
        all_path = output_dir / "pt_status_execucao_latest.csv"
        csv_writer.write_csv(self._tracking_records, all_path, columns=all_columns)

        sem_records = [r for r in self._tracking_records if bool(r.get("sem_prazo"))]
        sem_path = output_dir / "pt_sem_prazo_latest.csv"
        csv_writer.write_csv(sem_records, sem_path, columns=all_columns)

        logger.info(
            "Relatorio %s gerado: total=%d sem_prazo=%d arquivo=%s",
            spec.log_label,
            len(self._tracking_records),
            len(sem_records),
            sem_path,
        )
        try:
            export_result = export_normalized_csv(output_dir, logger=logger)
            if export_result.get("latest_path"):
                logger.info(
                    "Relatorio %s normalizado gerado: registros=%d latest=%s",
                    spec.log_label,
                    int(export_result.get("records", 0) or 0),
                    export_result["latest_path"],
                )
        except Exception as exc:
            logger.warning("Falha ao gerar CSV %s normalizado (%s).", spec.log_label, exc)

    def _save_snapshot_json(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        prazos: dict[str, str],
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
            extra_payload={"prazos": prazos},
        )

    def _register_tracking_record(
        self,
        *,
        spec: DocumentTypeSpec,
        processo: str,
        protocolo_documento: str,
        snapshot: dict[str, Any],
        prazos: dict[str, str],
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
            }
        )
        self._tracking_records.append(record)


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
            "pt_normalizado_latest.csv",
            "pt_normalizado_completo_latest.csv",
        ),
        handler=PTDocumentHandler(),
    )
