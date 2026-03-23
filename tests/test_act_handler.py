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

from app.documents.act import build_act_document_type


class ACTHandlerTests(unittest.TestCase):
    def test_act_handler_generates_json_and_status_csv(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        snapshot = {
            "text": "Acordo de Cooperacao Tecnica entre orgaos.",
            "tables": [{"rows": [["Clausula", "Valor"], ["Objeto", "Cooperacao tecnica"]]}],
            "extraction_mode": "html_dom",
            "title": "ACT teste",
            "url": "https://sei.exemplo/documento",
        }
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_act_handler"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            output_path = handler.process_snapshot(
                spec=spec,
                processo="60090.000001/2026-00",
                protocolo_documento="123456",
                snapshot=snapshot,
                collection_context={
                    "captured_at": "2026-03-18T10:00:00",
                    "found": True,
                    "found_in": "filter",
                    "search_term": "Acordo de Cooperação Técnica",
                    "results_count": 3,
                    "chosen_documento": "123456",
                    "selection_reason": "primeiro_resultado_mais_recente",
                    "selection_detail": "position=1 total=3",
                    "extraction_error": "",
                },
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )
            self.assertIsNotNone(output_path)
            self.assertTrue(output_path.exists())

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["document_type"], "act")
            self.assertEqual(payload["processo"], "60090.000001/2026-00")
            self.assertEqual(payload["documento"], "123456")
            self.assertEqual(payload["collection"]["results_count"], 3)
            self.assertEqual(payload["collection"]["selection_reason"], "primeiro_resultado_mais_recente")

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            status_path = output_dir / "act_status_execucao_latest.csv"
            self.assertTrue(status_path.exists())
            with status_path.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["document_type"], "act")
            self.assertEqual(rows[0]["processo"], "60090.000001/2026-00")
            self.assertEqual(rows[0]["found"], "True")
            self.assertEqual(rows[0]["results_count"], "3")
            self.assertEqual(rows[0]["chosen_documento"], "123456")
            self.assertEqual(rows[0]["selection_reason"], "primeiro_resultado_mais_recente")
            self.assertEqual(rows[0]["text_chars"], str(len(snapshot["text"])))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_act_handler_records_not_found_search_outcome(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_nf"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            handler.record_search_outcome(
                spec=spec,
                processo="60090.000002/2026-00",
                collection_context={
                    "captured_at": "2026-03-18T10:10:00",
                    "found": False,
                    "found_in": "none",
                    "search_term": "Acordo de Cooperação Técnica",
                    "results_count": 0,
                    "chosen_documento": "",
                    "selection_reason": "not_found",
                    "selection_detail": "nao encontrado no filtro nem na arvore",
                    "extraction_error": "",
                },
            )
            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            status_path = output_dir / "act_status_execucao_latest.csv"
            with status_path.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["found"], "False")
            self.assertEqual(rows[0]["selection_reason"], "not_found")
            self.assertEqual(rows[0]["results_count"], "0")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
