from __future__ import annotations

import csv
import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.memorando import build_memorando_document_type
from app.services.documento_administrativo_normalizer import (
    build_administrativo_v2_record,
    build_normalized_record,
)
from app.services.pipeline_states import AccessState, DiscoveryState, ExtractionState, OpeningState
from app.services.semantic_states import ClassificationState, DocumentFunctionState, PublicationState


class AdministrativoTaxonomyTests(unittest.TestCase):
    def test_each_administrative_class_is_the_semantic_requested_type(self) -> None:
        cases = {
            "Nota Técnica nº 1/2026": "nota_tecnica",
            "Memorando nº 2/2026": "memorando",
            "Despacho nº 3/2026": "despacho",
            "Ofício nº 4/2026": "oficio",
        }
        for title, expected in cases.items():
            with self.subTest(title=title):
                payload = self._payload(title)
                record = build_normalized_record(payload, Path("administrativo.json"))
                self.assertEqual(expected, record["doc_class"])
                self.assertEqual(expected, record["resolved_document_type"])
                self.assertEqual(expected, record["requested_type"])

                v2 = build_administrativo_v2_record(record, payload)
                semantic = v2["semantic_state"]
                self.assertEqual(ClassificationState.CONFIRMED.value, semantic["classification"])
                self.assertEqual(expected, semantic["resolved_class"])
                self.assertEqual(DocumentFunctionState.RELATED.value, semantic["function"])
                self.assertEqual(record["funcao_administrativa"], semantic["resolved_function"])

    def test_legacy_memorando_status_is_a_filtered_compatibility_alias(self) -> None:
        root = Path.cwd() / ".tmp_adm_p1_001_status"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir()
        try:
            spec = build_memorando_document_type()
            handler = spec.handler
            handler._tracking_records = [
                {"requested_type": doc_class, "doc_class": doc_class, "resolved_document_type": doc_class}
                for doc_class in ("nota_tecnica", "memorando", "despacho", "oficio")
            ]
            with (
                patch("app.services.documento_administrativo_normalizer.export_normalized_csv"),
                patch("app.documents.cooperation_common.export_dashboard_ready_csv"),
            ):
                handler.finalize_run(spec=spec, output_dir=root, logger=Mock(), settings=SimpleNamespace())

            with (root / "documento_administrativo_status_execucao_latest.csv").open(encoding="utf-8-sig", newline="") as stream:
                administrative = list(csv.DictReader(stream))
            with (root / "memorando_status_execucao_latest.csv").open(encoding="utf-8-sig", newline="") as stream:
                legacy_alias = list(csv.DictReader(stream))
            self.assertEqual(4, len(administrative))
            self.assertEqual(["memorando"], [row["doc_class"] for row in legacy_alias])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_title_only_stays_blocked_in_v2(self) -> None:
        payload = self._payload("Nota Técnica nº 5/2026", text="", extraction=ExtractionState.EMPTY_CONTENT)
        record = build_normalized_record(payload, Path("title-only.json"))
        v2 = build_administrativo_v2_record(record, payload)
        self.assertEqual(ClassificationState.CANDIDATE.value, v2["semantic_state"]["classification"])
        self.assertEqual(DocumentFunctionState.NOT_EVALUATED.value, v2["semantic_state"]["function"])
        self.assertEqual(PublicationState.BLOCKED.value, v2["document_gold_decision"]["semantic_state"]["publication"])

    @staticmethod
    def _payload(title: str, *, text: str | None = None, extraction: ExtractionState = ExtractionState.EXTRACTED) -> dict:
        content = text if text is not None else f"{title}\nEncaminho o processo para conhecimento e providências."
        return {
            "processo": "60090.000001/2020-00",
            "documento": "4455667",
            "snapshot": {"title": title, "text": content, "tables": []},
            "collection": {
                "found": True,
                "acquisition_state": {
                    "discovery": DiscoveryState.FOUND.value,
                    "opening": OpeningState.OPENED.value,
                    "access": AccessState.ACCESSIBLE.value,
                    "extraction": extraction.value,
                },
            },
        }


if __name__ == "__main__":
    unittest.main()
