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
from app.documents.memorando import build_memorando_document_type


class ACTHandlerTests(unittest.TestCase):
    def test_act_handler_generates_json_and_status_csv(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        snapshot = {
            "text": (
                "ACORDO DE COOPERACAO TECNICA No 1/2026 QUE ENTRE SI CELEBRAM A UNIAO, "
                "REPRESENTADA PELO MINISTERIO DA DEFESA, POR INTERMEDIO DO CENSIPAM, E A VISIONA. "
                "CLAUSULA PRIMEIRA - DO OBJETO. O objeto do presente Acordo de Cooperacao Tecnica e a cooperacao."
            ),
            "tables": [{"rows": [["Clausula", "Valor"], ["Objeto", "Cooperacao tecnica"]]}],
            "extraction_mode": "html_dom",
            "title": "Acordo de Cooperacao Tecnica No 1/2026",
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
                    "search_term": "Acordo de Cooperacao Tecnica",
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
            self.assertEqual(payload["document_family"], "cooperacao")
            self.assertEqual(payload["resolved_document_type"], "act")
            self.assertEqual(payload["requested_type"], "act")
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
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["document_type"], "act")
            self.assertEqual(rows[0]["requested_type"], "act")
            self.assertEqual(rows[0]["processo"], "60090.000001/2026-00")
            self.assertEqual(rows[0]["found"], "True")
            self.assertEqual(rows[0]["results_count"], "3")
            self.assertEqual(rows[0]["chosen_documento"], "123456")
            self.assertEqual(rows[0]["selection_reason"], "primeiro_resultado_mais_recente")
            self.assertEqual(rows[0]["text_chars"], str(len(snapshot["text"])))
            self.assertEqual(rows[0]["doc_class"], "act_final")
            self.assertEqual(rows[0]["resolved_document_type"], "act")
            self.assertEqual(rows[0]["snapshot_prefix"], "acordo_cooperacao_tecnica")
            self.assertEqual(rows[0]["is_canonical_candidate"], "True")
            self.assertEqual(rows[0]["validation_status"], "valid_for_requested_type")
            self.assertEqual(rows[0]["publication_status"], "published_gold")

            normalized_path = output_dir / "act_normalizado_latest.csv"
            audit_path = output_dir / "act_classificacao_latest.csv"
            self.assertTrue(normalized_path.exists())
            self.assertTrue(audit_path.exists())
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_act_handler_saves_memorando_with_family_publication(self) -> None:
        spec = build_memorando_document_type()
        handler = spec.handler
        handler.reset_run()
        snapshot = {
            "text": "Memorando de Entendimentos no 1 que entre si celebram a Uniao e o Estado de Roraima.",
            "tables": [],
            "extraction_mode": "html_dom",
            "title": "Memorando de Entendimentos",
            "url": "https://sei.exemplo/documento",
        }
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_memorando"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            output_path = handler.process_snapshot(
                spec=spec,
                processo="60091.000060/2023-87",
                protocolo_documento="6256843",
                snapshot=snapshot,
                collection_context={
                    "captured_at": "2026-03-25T10:00:00",
                    "found": True,
                    "found_in": "filter",
                    "search_term": "MEMORANDO DE ENTENDIMENTOS",
                    "results_count": 6,
                    "chosen_documento": "6256843",
                    "selection_reason": "primeiro_resultado_mais_recente",
                    "selection_detail": "position=1 total=6",
                    "extraction_error": "",
                },
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )
            self.assertIsNotNone(output_path)
            self.assertTrue(output_path.exists())
            self.assertTrue(output_path.name.startswith("memorando_entendimentos_"))

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["document_type"], "memorando")
            self.assertEqual(payload["document_family"], "cooperacao")
            self.assertEqual(payload["requested_type"], "memorando")
            self.assertEqual(payload["resolved_document_type"], "memorando_entendimentos")
            self.assertEqual(payload["analysis"]["doc_class"], "memorando")
            self.assertEqual(payload["analysis"]["validation_status"], "valid_for_requested_type")
            self.assertEqual(payload["analysis"]["publication_status"], "published_gold")

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )
            normalized_path = output_dir / "memorando_normalizado_latest.csv"
            self.assertTrue(normalized_path.exists())
            with normalized_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["requested_type"], "memorando")
            self.assertEqual(rows[0]["publication_status"], "published_gold")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_act_handler_keeps_act_snapshot_prefix_for_related_memorando(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        snapshot = {
            "text": "Memorando de Entendimentos no 1 que entre si celebram a Uniao e o Estado de Roraima.",
            "tables": [],
            "extraction_mode": "html_dom",
            "title": "Memorando de Entendimentos",
            "url": "https://sei.exemplo/documento",
        }
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_related_memorando"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            output_path = handler.process_snapshot(
                spec=spec,
                processo="60091.000060/2023-87",
                protocolo_documento="6256843",
                snapshot=snapshot,
                collection_context={
                    "captured_at": "2026-03-25T10:00:00",
                    "found": True,
                    "found_in": "filter",
                    "search_term": "Acordo de Cooperacao Tecnica",
                    "results_count": 6,
                    "chosen_documento": "6256843",
                    "selection_reason": "primeiro_resultado_mais_recente",
                    "selection_detail": "position=1 total=6",
                    "extraction_error": "",
                },
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )
            self.assertIsNotNone(output_path)
            self.assertTrue(output_path.exists())
            self.assertTrue(output_path.name.startswith("acordo_cooperacao_tecnica_"))

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["document_type"], "act")
            self.assertEqual(payload["requested_type"], "act")
            self.assertEqual(payload["resolved_document_type"], "memorando_entendimentos")
            self.assertEqual(payload["snapshot_prefix"], "acordo_cooperacao_tecnica")
            self.assertEqual(payload["analysis"]["snapshot_prefix"], "memorando_entendimentos")

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            status_path = output_dir / "act_status_execucao_latest.csv"
            self.assertTrue(status_path.exists())
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["requested_type"], "act")
            self.assertEqual(rows[0]["resolved_document_type"], "memorando_entendimentos")
            self.assertEqual(rows[0]["snapshot_prefix"], "acordo_cooperacao_tecnica")
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
                    "search_term": "Acordo de Cooperacao Tecnica",
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
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["found"], "False")
            self.assertEqual(rows[0]["selection_reason"], "not_found")
            self.assertEqual(rows[0]["results_count"], "0")
            self.assertEqual(rows[0]["normalization_status"], "not_found")
            self.assertEqual(rows[0]["discard_reason"], "not_found")
            self.assertEqual(rows[0]["publication_status"], "retained_silver")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_act_handler_preserves_filter_error_search_outcome(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")

        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_filter_error"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            handler.record_search_outcome(
                spec=spec,
                processo="60090.000003/2026-00",
                collection_context={
                    "captured_at": "2026-03-18T10:20:00",
                    "found": False,
                    "found_in": "filter",
                    "search_term": "Acordo de Cooperacao Tecnica",
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

            status_path = output_dir / "act_status_execucao_latest.csv"
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["validation_status"], "filter_error")
            self.assertEqual(rows[0]["normalization_status"], "filter_error")
            self.assertEqual(rows[0]["discard_reason"], "filter_error")
            self.assertEqual(rows[0]["publication_status"], "retained_silver")
            self.assertIn("Timeout", rows[0]["extraction_error"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
