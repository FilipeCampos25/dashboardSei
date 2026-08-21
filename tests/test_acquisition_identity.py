from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.documents.common import (  # noqa: E402
    build_basic_tracking_record,
    identity_from_source_url,
    save_snapshot_json,
)
from app.documents.types import DocumentTypeSpec  # noqa: E402
from app.services.contract_adapters import adapt_legacy_record  # noqa: E402
from app.services.normalization_review import _dedupe, _issue  # noqa: E402
from app.rpa import scraping  # noqa: E402


SPEC = DocumentTypeSpec(
    key="act",
    display_name="ACT",
    search_terms=("ACT",),
    tree_match_terms=("ACT",),
    snapshot_prefix="act",
    log_label="ACT",
    cleanup_patterns=(),
    handler=Mock(),
)


class AcquisitionIdentityTests(unittest.TestCase):
    def test_tree_discovery_captures_identity_from_candidate_href(self) -> None:
        element = Mock(text="Acordo de Cooperacao Tecnica 1/2026")
        element.get_attribute.return_value = (
            "https://sei.example/controlador.php?id_documento=8707829&id_anexo=1139528"
        )
        second_element = Mock(text=element.text)
        second_element.get_attribute.return_value = (
            "https://sei.example/controlador.php?id_documento=8707830&id_anexo=1139529"
        )
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.performance_profiler = Mock()
        scraper.logger = Mock()
        scraper.selectors = Mock()
        scraper.selectors.get_many.return_value = ["//a"]
        scraper.driver = Mock()
        scraper.driver.find_element.return_value = Mock()
        scraper.driver.find_elements.return_value = [element, second_element]
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        scraper._score_tree_candidate = Mock(return_value=(100, ["ACT"]))

        candidates = scraping.SEIScraper._find_document_candidates_in_tree(scraper, SPEC)

        self.assertEqual(len(candidates), 2)
        self.assertEqual({item["document_id"] for item in candidates}, {"8707829", "8707830"})
        self.assertEqual({item["candidate_id"] for item in candidates}, {"1139528", "1139529"})

    def test_supported_url_parameters_are_explicit_and_strict(self) -> None:
        complete = identity_from_source_url(
            "https://sei.example/controlador.php?id_documento=8707829&id_anexo=1139528"
        )
        self.assertEqual(complete["document_id"], "8707829")
        self.assertEqual(complete["candidate_id"], "1139528")
        self.assertIsNone(identity_from_source_url("https://sei.example/x?foo=8707829")["document_id"])
        self.assertIsNone(identity_from_source_url("https://sei.example/x?id_documento=abc")["document_id"])

    def test_snapshot_tracking_and_v2_keep_complete_identity_and_legacy_fields(self) -> None:
        process_id = "60090.000001/2026-01"
        source_url = "https://sei.example/x?id_documento=8707829&id_anexo=1139528"
        context = {
            "found": True,
            "document_id": "8707829",
            "candidate_id": "1139528",
            "source_url": source_url,
        }
        snapshot = {"url": source_url, "text": "ACT", "tables": []}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = save_snapshot_json(
                spec=SPEC,
                processo=process_id,
                protocolo_documento=process_id,
                snapshot=snapshot,
                output_dir=Path(directory),
                logger=Mock(),
                extra_payload={"collection": context},
            )
            payload = json.loads(path.read_text(encoding="utf-8"))  # type: ignore[union-attr]
            tracking = build_basic_tracking_record(
                spec=SPEC,
                processo=process_id,
                protocolo_documento=process_id,
                snapshot=snapshot,
                output_path=path,
                collection_context=context,
            )

        self.assertEqual(payload["identity"], {
            "process_id": process_id,
            "document_id": "8707829",
            "candidate_id": "1139528",
            "source_url": source_url,
        })
        self.assertEqual(payload["processo"], process_id)
        self.assertEqual(payload["documento"], process_id)
        self.assertEqual(adapt_legacy_record(tracking)["identity"], payload["identity"])

    def test_two_candidates_same_process_remain_distinct_through_review_dedupe(self) -> None:
        process_id = "P"
        issues = []
        for document_id, candidate_id in (("D1", "C1"), ("D2", "C2")):
            row = {
                "processo": process_id,
                "documento": process_id,
                "document_id": document_id,
                "candidate_id": candidate_id,
            }
            issues.append(_issue(
                code="required_field_missing",
                severity="high",
                field="objeto",
                message="missing",
                suggested_action="review",
                document_type="act",
                row=row,
            ))
        self.assertEqual(len(_dedupe(issues)), 2)

    def test_missing_document_and_candidate_ids_stay_none(self) -> None:
        tracking = build_basic_tracking_record(
            spec=SPEC,
            processo="P",
            protocolo_documento="P",
            snapshot={"url": "https://sei.example/x?foo=123"},
            output_path=None,
        )
        self.assertIsNone(tracking["document_id"])
        self.assertIsNone(tracking["candidate_id"])
        self.assertNotEqual(tracking["document_id"], tracking["process_id"])


if __name__ == "__main__":
    unittest.main()
