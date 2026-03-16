from __future__ import annotations

"""Orchestration layer for the isolated TED/ACT module."""

import argparse
import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any

from .act_client import ActClient
from .config import TedActModuleConfig, load_ted_act_config
from .normalizer import TedActNormalizer
from .process_reader import ExtractedProcessRecord, ProcessReader
from .ted_client import TedClient
from .writer import TedActWriter


logger = logging.getLogger(__name__)


@dataclass
class TedActService:
    """Coordinate snapshot reading, client calls and local persistence."""

    config: TedActModuleConfig = field(default_factory=load_ted_act_config)
    process_reader: ProcessReader | None = None
    ted_client: TedClient | None = None
    act_client: ActClient | None = None
    normalizer: TedActNormalizer | None = None
    writer: TedActWriter | None = None

    def __post_init__(self) -> None:
        if self.process_reader is None:
            self.process_reader = ProcessReader(self.config.input_path)
        if self.ted_client is None:
            self.ted_client = TedClient(self.config)
        if self.act_client is None:
            self.act_client = ActClient(self.config)
        if self.normalizer is None:
            self.normalizer = TedActNormalizer()
        if self.writer is None:
            self.writer = TedActWriter(self.config.output_path)

    def run(self) -> dict[str, Any]:
        """Execute the isolated TED/ACT collection flow and persist outputs."""
        started_at = self._now_iso()
        extraction_result = self.process_reader.extract_process_records()

        consolidado: list[dict[str, Any]] = []
        nao_encontrados_processos: list[dict[str, Any]] = []
        process_reports: list[dict[str, Any]] = []

        for process_record in extraction_result.processos:
            ted_payload = self._safe_fetch_ted(process_record)
            act_payload = self._safe_fetch_act(process_record)
            normalized = self.normalizer.normalize(process_record, ted_payload, act_payload)

            consolidado.extend(normalized.consolidados)
            if normalized.nao_encontrado is not None:
                nao_encontrados_processos.append(normalized.nao_encontrado)

            process_reports.append(
                {
                    "numero_processo": process_record.processo_normalizado,
                    "numero_processo_origem": process_record.processo_original,
                    "arquivo_origem_processo": str(process_record.source_path),
                    "ted_status": ted_payload.get("status"),
                    "act_status": act_payload.get("status"),
                    "registros_consolidados": len(normalized.consolidados),
                    "ted_error": ted_payload.get("error"),
                    "act_error": act_payload.get("error"),
                }
            )

        nao_encontrados_payload = {
            "processos_sem_resultado": nao_encontrados_processos,
            "arquivos_sem_processo": [
                {
                    "source_path": str(item.source_path),
                    "reason": item.reason,
                }
                for item in extraction_result.arquivos_sem_processo
            ],
        }

        consolidado_paths = self.writer.write_consolidado(consolidado)
        nao_encontrados_path = self.writer.write_nao_encontrados(nao_encontrados_payload)

        relatorio = {
            "started_at": started_at,
            "finished_at": self._now_iso(),
            "input_dir": str(self.config.input_path),
            "output_dir": str(self.writer.output_dir),
            "arquivos_lidos": len(self.process_reader.list_snapshot_paths()),
            "processos_extraidos": len(extraction_result.processos),
            "arquivos_sem_processo": len(extraction_result.arquivos_sem_processo),
            "registros_consolidados": len(consolidado),
            "processos_sem_resultado": len(nao_encontrados_processos),
            "ted_status": self._count_status(process_reports, "ted_status"),
            "act_status": self._count_status(process_reports, "act_status"),
            "arquivos_gerados": {
                "ted_act_consolidado_json": str(consolidado_paths["json"]),
                "ted_act_consolidado_csv": str(consolidado_paths["csv"]),
                "ted_act_nao_encontrados_json": str(nao_encontrados_path),
                "relatorio_execucao_json": str(self.writer.relatorio_execucao_path),
            },
            "processos": process_reports,
        }
        relatorio_path = self.writer.write_relatorio_execucao(relatorio)
        relatorio["arquivos_gerados"]["relatorio_execucao_json"] = str(relatorio_path)
        return relatorio

    def _safe_fetch_ted(self, process_record: ExtractedProcessRecord) -> dict[str, Any]:
        """Call TED client and isolate failures to the current process."""
        try:
            return self.ted_client.fetch_by_process(process_record.processo_original)
        except Exception as exc:
            logger.exception("Unexpected TED failure for processo=%s", process_record.processo_normalizado)
            return {
                "client": "ted",
                "status": "error",
                "processo_original": process_record.processo_original,
                "processo_normalizado": process_record.processo_normalizado,
                "endpoint": self.config.ted_endpoint_url,
                "matched_strategy": None,
                "raw_response": None,
                "records": [],
                "attempts": [],
                "error": f"unexpected_error: {exc}",
            }

    def _safe_fetch_act(self, process_record: ExtractedProcessRecord) -> dict[str, Any]:
        """Call ACT client and isolate failures to the current process."""
        try:
            return self.act_client.fetch_by_process(
                process_record.processo_original,
                hints={
                    "parceiro": process_record.parceiro_hint,
                    "numero_instrumento": process_record.numero_instrumento_hint,
                    "objeto": process_record.objeto_hint,
                    "tipo_instrumento": process_record.tipo_instrumento_hint,
                },
            )
        except Exception as exc:
            logger.exception("Unexpected ACT failure for processo=%s", process_record.processo_normalizado)
            return {
                "client": "act",
                "status": "error",
                "processo_original": process_record.processo_original,
                "processo_normalizado": process_record.processo_normalizado,
                "endpoint": self.config.act_endpoint_url,
                "matched_strategy": None,
                "raw_response": None,
                "value": None,
                "attempts": [],
                "error": f"unexpected_error: {exc}",
            }

    def _count_status(self, process_reports: list[dict[str, Any]], key: str) -> dict[str, int]:
        """Count status values from per-process reports."""
        counts: dict[str, int] = {}
        for item in process_reports:
            status = str(item.get(key) or "unknown")
            counts[status] = counts.get(status, 0) + 1
        return counts

    def _now_iso(self) -> str:
        """Return the current timestamp in UTC ISO format."""
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_service_from_args() -> TedActService:
    """Create a service instance from CLI arguments plus .env defaults."""
    parser = argparse.ArgumentParser(description="TED/ACT isolated collector")
    parser.add_argument("--input-dir", default=None, help="Diretorio com plano_trabalho_*.json")
    parser.add_argument("--output-dir", default=None, help="Diretorio para os artefatos TED/ACT")
    parser.add_argument("--log-level", default="INFO", help="Nivel de log")
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Desabilita verificacao SSL para ambientes com proxy/certificado inspecionado",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    config = load_ted_act_config()
    updates: dict[str, Any] = {}
    if args.input_dir:
        updates["input_dir"] = args.input_dir
    if args.output_dir:
        updates["output_dir"] = args.output_dir
    if args.no_verify_ssl:
        updates["verify_ssl"] = False
    if updates:
        config = config.model_copy(update=updates)

    return TedActService(config=config)


def main() -> None:
    """Run the TED/ACT module in isolation."""
    service = build_service_from_args()
    relatorio = service.run()
    print(json.dumps(relatorio, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
