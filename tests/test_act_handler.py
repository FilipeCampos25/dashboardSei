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

    def test_act_handler_keeps_unique_candidate_snapshots_and_aliases_only_canonical(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")
        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_candidates"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            attempts = [
                (
                    "8707829",
                    "filter",
                    "position=1 total=6",
                    {
                        "title": "SEI - Termo Aditivo",
                        "url": "https://sei.exemplo/documento?id_documento=8707829",
                        "extraction_mode": "html_dom",
                        "text": "Termo Aditivo ao Acordo de Cooperacao Tecnica 2/2023.",
                    },
                ),
                (
                    "7465364",
                    "filter",
                    "position=2 total=6",
                    {
                        "title": "SEI - Extrato",
                        "url": "https://sei.exemplo/documento?id_documento=7465364",
                        "extraction_mode": "pdf_native",
                        "text": "Extrato de publicacao do Acordo de Cooperacao Tecnica 2/2023.",
                    },
                ),
                (
                    "6451163",
                    "tree",
                    "rank=3/4 score=131 termos=acordo de cooperacao tecnica",
                    {
                        "title": "SEI - Acordo de Cooperacao Tecnica",
                        "url": "https://sei.exemplo/documento?id_documento=6451163",
                        "extraction_mode": "html_dom",
                        "text": (
                            "ACORDO DE COOPERACAO TECNICA No 2/2023. Processo 08650.063489/2021-11. "
                            "Que entre si celebram a Uniao, representada pelo Ministerio da Defesa, "
                            "por intermedio do Centro Gestor e Operacional do Sistema de Protecao da Amazonia - CENSIPAM, "
                            "e a Policia Rodoviaria Federal. CLAUSULA PRIMEIRA - DO OBJETO. "
                            "O objeto do presente Acordo de Cooperacao Tecnica e a cooperacao."
                        ),
                    },
                ),
            ]

            for protocolo, found_in, selection_detail, snapshot in attempts:
                output_path = handler.process_snapshot(
                    spec=spec,
                    processo="08650.063489/2021-11",
                    protocolo_documento=protocolo,
                    snapshot=snapshot,
                    collection_context={
                        "captured_at": "2026-04-10T12:00:00",
                        "found": True,
                        "found_in": found_in,
                        "search_term": "Acordo de Cooperacao Tecnica",
                        "results_count": 6,
                        "chosen_documento": protocolo,
                        "selection_reason": "resultado_ranqueado_por_data"
                        if found_in == "filter"
                        else "highest_tree_match_score",
                        "selection_detail": selection_detail,
                        "extraction_error": "",
                    },
                    output_dir=output_dir,
                    logger=logger,
                    settings=settings,
                )
                self.assertIsNotNone(output_path)
                self.assertEqual(output_path.parent.name, "candidates")

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            candidate_paths = sorted((output_dir / "candidates").glob("acordo_cooperacao_tecnica_*.json"))
            self.assertEqual(len(candidate_paths), 3)
            self.assertEqual(len({path.name for path in candidate_paths}), 3)
            alias_path = output_dir / "acordo_cooperacao_tecnica_08650.063489_2021-11.json"
            self.assertTrue(alias_path.exists())
            alias_payload = json.loads(alias_path.read_text(encoding="utf-8"))
            self.assertEqual(alias_payload["documento"], "6451163")
            self.assertEqual(alias_payload["analysis"]["doc_class"], "act_final")

            status_path = output_dir / "act_status_execucao_latest.csv"
            with status_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                status_rows = list(csv.DictReader(file_obj))
            self.assertEqual(len(status_rows), 3)
            json_paths = {row["json_path"] for row in status_rows}
            self.assertEqual(len(json_paths), 3)
            self.assertTrue(all("\\candidates\\" in path or "/candidates/" in path for path in json_paths))
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_act_handler_does_not_publish_alias_without_canonical_candidate(self) -> None:
        spec = build_act_document_type()
        handler = spec.handler
        handler.reset_run()
        settings = SimpleNamespace(export_raw_fields_csv=False)
        logger = logging.getLogger("act-handler-test")
        output_dir = Path.cwd() / "tests" / "_tmp_act_handler_no_canonical"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            snapshots = [
                {
                    "title": "SEI - Acordo de Cooperacao Tecnica",
                    "url": "https://sei.exemplo/documento?id_documento=5301158",
                    "extraction_mode": "pdf_native",
                    "text": (
                        "ACORDO DE COOPERACAO TECNICA No 109/2022. Processo 14022.172688/2022-07. "
                        "Que entre si celebram a Uniao, por intermedio da Central de Compras, e o Banco do Brasil. "
                        "CLAUSULA PRIMEIRA - DO OBJETO. O objeto do presente acordo e a operacionalizacao."
                    ),
                },
                {
                    "title": "SEI - Termo de Adesao",
                    "url": "https://sei.exemplo/documento?id_documento=5434908",
                    "extraction_mode": "html_dom",
                    "text": "Termo de Adesão ao ACT No 109/2022/CENTRALDECOMPRAS/ME.",
                },
                {
                    "title": "SEI - Minuta",
                    "url": "https://sei.exemplo/documento?id_documento=5303134",
                    "extraction_mode": "html_dom",
                    "text": "Minuta de Termo de Adesao ao ACT No 109/2022.",
                },
            ]
            for index, snapshot in enumerate(snapshots, start=1):
                handler.process_snapshot(
                    spec=spec,
                    processo="60090.000615/2022-10",
                    protocolo_documento=str(index),
                    snapshot=snapshot,
                    collection_context={
                        "captured_at": "2026-04-10T12:00:00",
                        "found": True,
                        "found_in": "tree",
                        "search_term": "Acordo de Cooperacao Tecnica",
                        "results_count": 7,
                        "chosen_documento": str(index),
                        "selection_reason": "highest_tree_match_score",
                        "selection_detail": f"rank={index}/7 score=41 termos=act",
                        "extraction_error": "",
                    },
                    output_dir=output_dir,
                    logger=logger,
                    settings=settings,
                )

            handler.finalize_run(
                spec=spec,
                output_dir=output_dir,
                logger=logger,
                settings=settings,
            )

            alias_path = output_dir / "acordo_cooperacao_tecnica_60090.000615_2022-10.json"
            self.assertFalse(alias_path.exists())
            with (output_dir / "act_classificacao_latest.csv").open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
            self.assertEqual({row["publication_status"] for row in rows}, {"retained_silver"})
            self.assertIn("act_sem_marcador_interno", {row["classification_reason"] for row in rows})
            self.assertIn("termo_adesao", {row["doc_class"] for row in rows})
            self.assertIn("minuta", {row["doc_class"] for row in rows})
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
