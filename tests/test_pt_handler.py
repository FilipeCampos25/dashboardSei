from __future__ import annotations

import csv
import json
import logging
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.pt import build_pt_document_type


def _write_preview_csv(output_dir: Path, *, processo: str, parceiro: str, vigencia: str, objeto: str) -> None:
    preview_path = output_dir / "parcerias_vigentes_latest.csv"
    with preview_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "interno_descricao",
                "seq",
                "processo",
                "parceiro",
                "vigencia",
                "numero_act",
                "objeto",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "interno_descricao": "PARCERIAS VIGENTES",
                "seq": "1",
                "processo": processo,
                "parceiro": parceiro,
                "vigencia": vigencia,
                "numero_act": "01/2026",
                "objeto": objeto,
            }
        )


class PTHandlerTests(unittest.TestCase):
    def test_pt_handler_sanitizes_snapshot_and_syncs_audit_status(self) -> None:
        spec = build_pt_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("pt-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_pt_handler"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        _write_preview_csv(
            output_dir,
            processo="60090.000010/2026-00",
            parceiro="UNIVERSIDADE FEDERAL DE TESTE",
            vigencia="12 meses",
            objeto="Cooperacao tecnica para pesquisa aplicada",
        )

        snapshot = {
            "text": (
                "MINISTÃ‰RIO DA DEFESA\n"
                "PLANO DE TRABALHO\n"
                "IDENTIFICAÃ‡ÃƒO DO OBJETO\n"
                "Objeto: Cooperacao tecnica para pesquisa aplicada.\n"
                "Inicio (mes/ano): 01/01/2026 Termino (mes/ano): 31/12/2026.\n"
            ),
            "tables": [],
            "extraction_mode": "html_dom",
            "title": "PLANO DE TRABALHO",
            "url": "https://sei.exemplo/documento",
        }

        try:
            output_path = handler.process_snapshot(
                spec=spec,
                processo="60090.000010/2026-00",
                protocolo_documento="123456",
                snapshot=snapshot,
                collection_context={
                    "captured_at": "2026-03-27T10:00:00",
                    "found": True,
                    "found_in": "filter",
                    "search_term": "Plano de Trabalho - PT",
                    "results_count": 1,
                    "chosen_documento": "123456",
                    "selection_reason": "primeiro_resultado_mais_recente",
                    "selection_detail": "position=1 total=1",
                    "extraction_error": "",
                },
                analysis={
                    "validation_status": "valid_for_requested_type",
                    "publication_status": "",
                    "classification_reason": "",
                    "is_canonical_candidate": True,
                },
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )
            self.assertIsNotNone(output_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIn("MINISTÉRIO DA DEFESA", payload["snapshot"]["text"])
            self.assertNotIn("MINISTÃ‰RIO", payload["snapshot"]["text"])

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            status_path = output_dir / "pt_status_execucao_latest.csv"
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["validation_status"], "valid_for_requested_type")
            self.assertEqual(rows[0]["publication_status"], "retained_silver")
            self.assertEqual(rows[0]["normalization_status"], "parcial_padronizado")
            self.assertEqual(rows[0]["found"], "True")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_pt_handler_records_filter_error_search_outcome(self) -> None:
        spec = build_pt_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("pt-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_pt_handler_filter_error"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            handler.record_search_outcome(
                spec=spec,
                processo="60090.000011/2026-00",
                collection_context={
                    "captured_at": "2026-03-27T10:10:00",
                    "found": False,
                    "found_in": "filter",
                    "search_term": "Plano de Trabalho",
                    "results_count": 0,
                    "chosen_documento": "",
                    "selection_reason": "search_open_error",
                    "selection_detail": "anchor do filtro indisponivel",
                    "extraction_error": "Timeout aguardando elemento no contexto de pesquisa",
                },
            )
            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            status_path = output_dir / "pt_status_execucao_latest.csv"
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["validation_status"], "filter_error")
            self.assertEqual(rows[0]["normalization_status"], "filter_error")
            self.assertEqual(rows[0]["publication_status"], "retained_silver")
            self.assertIn("Timeout", rows[0]["extraction_error"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
