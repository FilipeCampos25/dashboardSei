from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.documents.common import (  # noqa: E402
    acquisition_diagnostic_payload,
    acquisition_state_payload,
    build_basic_tracking_record,
)
from app.documents.types import DocumentTypeSpec  # noqa: E402
from app.rpa import scraping  # noqa: E402
from app.rpa.sei import document_text_extractor  # noqa: E402
from app.services.contract_adapters import adapt_legacy_record  # noqa: E402


OPENED = {
    "found": True,
    "process_id": "P1",
    "document_id": "D1",
    "candidate_id": "C1",
    "source_url": "https://synthetic.invalid/?id_documento=D1",
    "acquisition_state": {
        "discovery": "FOUND",
        "opening": "OPENED",
        "access": "UNKNOWN",
        "extraction": "NOT_ATTEMPTED",
    },
}


def snapshot_observation(**values: object) -> dict[str, object]:
    return {
        "text": values.pop("text", ""),
        "tables": values.pop("tables", []),
        "acquisition_observation": values,
    }


class AcquisitionFailureCauseTests(unittest.TestCase):
    def test_iframe_unavailable_is_not_empty_and_keeps_identity(self) -> None:
        driver = Mock()
        with patch.object(document_text_extractor, "_switch_to_visualizacao_iframe", return_value=False):
            snapshot = document_text_extractor.extract_document_snapshot(driver, logger=Mock())

        state = acquisition_state_payload(OPENED, snapshot)
        diagnostic = acquisition_diagnostic_payload(OPENED, snapshot)
        self.assertEqual(state, {
            "discovery": "FOUND",
            "opening": "OPENED",
            "access": "IFRAME_UNAVAILABLE",
            "extraction": "NOT_ATTEMPTED",
        })
        self.assertNotEqual(state["access"], "EMPTY_CONTENT")
        self.assertEqual(diagnostic, {"code": "IFRAME_UNAVAILABLE", "stage": "access"})

        record = build_basic_tracking_record(
            spec=Mock(key="pt"), processo="P1", protocolo_documento="legacy",
            snapshot=snapshot, output_path=None, collection_context=OPENED,
        )
        self.assertEqual((record["process_id"], record["document_id"], record["candidate_id"]), ("P1", "D1", "C1"))

    def test_restriction_requires_explicit_structured_observation(self) -> None:
        unknown = acquisition_state_payload(OPENED, snapshot_observation())
        restricted_snapshot = snapshot_observation(
            access_state="ACCESS_RESTRICTED",
            diagnostic_code="ACCESS_RESTRICTED",
            diagnostic_stage="access",
        )
        restricted = acquisition_state_payload(OPENED, restricted_snapshot)
        self.assertEqual(unknown["access"], "UNKNOWN")
        self.assertEqual(restricted["access"], "ACCESS_RESTRICTED")
        self.assertEqual(restricted["extraction"], "NOT_ATTEMPTED")
        self.assertNotEqual(restricted["access"], "EMPTY_CONTENT")

    def test_opening_timeout_is_not_not_found_or_empty(self) -> None:
        state = acquisition_state_payload(
            {**OPENED, "acquisition_state": {**OPENED["acquisition_state"], "opening": "OPEN_FAILED"}},
            snapshot_observation(opening_timeout=True, diagnostic_code="TIMEOUT", diagnostic_stage="opening"),
        )
        self.assertEqual(state["discovery"], "FOUND")
        self.assertEqual(state["opening"], "TIMEOUT")
        self.assertEqual(state["extraction"], "NOT_ATTEMPTED")
        self.assertNotEqual(state["discovery"], "NOT_FOUND")

    def test_real_empty_requires_access_and_completed_extraction(self) -> None:
        snapshot = snapshot_observation(
            text="", tables=[], access_observed=True,
            extraction_attempted=True, extraction_complete=True,
        )
        state = acquisition_state_payload(OPENED, snapshot)
        self.assertEqual(state["access"], "ACCESSIBLE")
        self.assertEqual(state["extraction"], "EMPTY_CONTENT")

        without_access = snapshot_observation(extraction_attempted=True, extraction_complete=True)
        with self.assertRaisesRegex(ValueError, "successful extraction requires"):
            acquisition_state_payload(OPENED, without_access)

    def test_extractor_failure_is_distinct_from_empty(self) -> None:
        snapshot = snapshot_observation(
            access_observed=True, extraction_attempted=True,
            extraction_error="synthetic failure", diagnostic_code="EXTRACTION_FAILED",
            diagnostic_stage="extraction",
        )
        state = acquisition_state_payload(OPENED, snapshot)
        self.assertEqual(state["extraction"], "EXTRACTION_FAILED")
        self.assertNotEqual(state["extraction"], "EMPTY_CONTENT")

    def test_complete_and_partial_paths_remain_supported(self) -> None:
        complete = snapshot_observation(
            text="verified", access_observed=True, extraction_attempted=True, extraction_complete=True,
        )
        partial = snapshot_observation(
            text="verified partial", access_observed=True, extraction_attempted=True, extraction_partial=True,
        )
        self.assertEqual(acquisition_state_payload(OPENED, complete)["extraction"], "EXTRACTED")
        self.assertEqual(acquisition_state_payload(OPENED, partial)["extraction"], "CONTENT_PARTIAL")

    def test_tracking_adapter_round_trip_preserves_code_and_state(self) -> None:
        context = {
            **OPENED,
            "acquisition_state": {
                "discovery": "FOUND", "opening": "OPENED",
                "access": "IFRAME_UNAVAILABLE", "extraction": "NOT_ATTEMPTED",
            },
            "acquisition_diagnostic_code": "IFRAME_UNAVAILABLE",
            "acquisition_diagnostic_stage": "access",
        }
        record = build_basic_tracking_record(
            spec=Mock(key="pt"), processo="P1", protocolo_documento="legacy",
            snapshot={}, output_path=None, collection_context=context,
        )
        adapted = adapt_legacy_record(record)
        self.assertEqual(adapted["acquisition_state"], context["acquisition_state"])
        self.assertEqual(adapted["acquisition_diagnostic"], {"code": "IFRAME_UNAVAILABLE", "stage": "access"})
        self.assertEqual(adapted["semantic_state"]["classification"], "NOT_CLASSIFIED")

        invented = adapt_legacy_record({
            **record,
            "acquisition_diagnostic_code": "ACCESS_RESTRICTED",
            "acquisition_diagnostic_stage": "extraction",
        })
        self.assertEqual(invented["acquisition_diagnostic"], {"code": "", "stage": ""})

    def test_technical_failure_never_reaches_classifier(self) -> None:
        scraper = object.__new__(scraping.SEIScraper)
        scraper.driver = Mock(current_url="synthetic", title="synthetic")
        scraper.logger = Mock()
        scraper.performance_profiler = Mock()
        scraper._record_document_extraction_failure = Mock()
        scraper._validate_snapshot_for_document_type = Mock(side_effect=AssertionError("classifier called"))
        spec = DocumentTypeSpec(
            key="pt", display_name="PT", search_terms=("PT",), tree_match_terms=("PT",),
            snapshot_prefix="pt", log_label="PT", cleanup_patterns=(), handler=Mock(),
        )
        failed_snapshot = snapshot_observation(
            access_state="IFRAME_UNAVAILABLE", diagnostic_code="IFRAME_UNAVAILABLE", diagnostic_stage="access",
        )
        with patch.object(scraping, "get_iframes_info", return_value=[]), patch.object(
            scraping.document_text_extractor, "extract_document_snapshot", return_value=failed_snapshot
        ):
            result = scraping.SEIScraper._extract_and_process_document_snapshot(
                scraper, "P1", "legacy", spec, dict(OPENED)
            )
        self.assertFalse(result)
        scraper._validate_snapshot_for_document_type.assert_not_called()
        failure_context = scraper._record_document_extraction_failure.call_args.kwargs["collection_context"]
        self.assertEqual(failure_context["acquisition_state"]["access"], "IFRAME_UNAVAILABLE")
        self.assertEqual(failure_context["document_id"], "D1")


if __name__ == "__main__":
    unittest.main()
