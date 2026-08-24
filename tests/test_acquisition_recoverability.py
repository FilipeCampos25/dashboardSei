from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.documents.common import acquisition_recovery_payload  # noqa: E402
from app.documents.pt import PTDocumentHandler  # noqa: E402
from app.documents.types import DocumentTypeSpec  # noqa: E402
from app.output import csv_writer  # noqa: E402
from app.services.normalization_review import _not_found_issue, collect_review_issues, export_review_queue  # noqa: E402


SPEC = DocumentTypeSpec(
    key="pt", display_name="PT", search_terms=("PT",), tree_match_terms=("PT",),
    snapshot_prefix="pt", log_label="PT", cleanup_patterns=(), handler=Mock(),
)


def state(discovery="FOUND", opening="OPENED", access="ACCESSIBLE", extraction="NOT_ATTEMPTED"):
    return {"discovery": discovery, "opening": opening, "access": access, "extraction": extraction}


def row_for(acquisition_state, *, diagnostic="", status="extraction_failure", candidate="C1"):
    return {
        "processo": "P1", "documento": "D1", "process_id": "P1", "document_id": "D1",
        "candidate_id": candidate, "source_url": f"https://synthetic.invalid/{candidate}",
        "validation_status": status, "normalization_status": status,
        "publication_status": "retained_silver", "acquisition_state_v2": json.dumps(acquisition_state),
        "acquisition_diagnostic_code": diagnostic,
    }


class AcquisitionRecoverabilityTests(unittest.TestCase):
    def test_recoverability_matrix_uses_structured_state(self) -> None:
        cases = (
            (state("NOT_FOUND", "NOT_ATTEMPTED", "UNKNOWN"), "document_not_found", False),
            (state(access="IFRAME_UNAVAILABLE"), "technical_access", True),
            (state(opening="TIMEOUT", access="UNKNOWN"), "technical_timeout", True),
            (state(extraction="EXTRACTION_FAILED"), "technical_extraction", True),
            (state(access="ACCESS_RESTRICTED"), "technical_access_restricted", None),
            (state(extraction="EMPTY_CONTENT"), "empty_content", False),
            (state(opening="NOT_ATTEMPTED", access="UNKNOWN"), "technical_state_unknown", None),
        )
        for acquisition_state, code, recoverable in cases:
            with self.subTest(code=code):
                result = acquisition_recovery_payload(acquisition_state)
                self.assertEqual(result["issue_code"], code)
                self.assertIs(result["recoverable"], recoverable)

    def test_structured_failures_never_become_document_not_found(self) -> None:
        cases = (
            (state(access="IFRAME_UNAVAILABLE"), "IFRAME_UNAVAILABLE", "technical_access"),
            (state(opening="TIMEOUT", access="UNKNOWN"), "TIMEOUT", "technical_timeout"),
            (state(extraction="EXTRACTION_FAILED"), "EXTRACTION_FAILED", "technical_extraction"),
        )
        for acquisition_state, diagnostic, expected in cases:
            with self.subTest(diagnostic=diagnostic):
                issue = _not_found_issue("pt", row_for(acquisition_state, diagnostic=diagnostic))
                self.assertEqual(issue["code"], expected)
                self.assertFalse(issue["is_not_found"])
                self.assertEqual(issue["acquisition_diagnostic_code"], diagnostic)

    def test_access_restricted_and_unknown_do_not_invent_false(self) -> None:
        restricted = _not_found_issue("pt", row_for(state(access="ACCESS_RESTRICTED"), diagnostic="ACCESS_RESTRICTED"))
        unknown = _not_found_issue("pt", row_for(state(opening="NOT_ATTEMPTED", access="UNKNOWN")))
        self.assertIsNone(restricted["is_recoverable"])
        self.assertIsNone(unknown["is_recoverable"])
        self.assertEqual(unknown["acquisition_root_cause"], "UNKNOWN")

    def test_real_empty_is_distinct_and_healthy_extraction_has_no_issue(self) -> None:
        empty = _not_found_issue("pt", row_for(state(extraction="EMPTY_CONTENT"), diagnostic="EMPTY_CONTENT"))
        healthy = _not_found_issue("pt", row_for(state(extraction="EXTRACTED")))
        self.assertEqual(empty["code"], "empty_content")
        self.assertFalse(empty["is_recoverable"])
        self.assertIsNone(healthy)

    def test_confirmed_absence_is_nonrecoverable_and_legacy_remains_supported(self) -> None:
        absent = _not_found_issue("pt", row_for(state("NOT_FOUND", "NOT_ATTEMPTED", "UNKNOWN"), status="not_found"))
        legacy = _not_found_issue("pt", {"processo": "P2", "validation_status": "not_found"})
        self.assertEqual(absent["code"], "document_not_found")
        self.assertTrue(absent["is_not_found"])
        self.assertFalse(absent["is_recoverable"])
        self.assertEqual(legacy["acquisition_root_cause"], "legacy_status")

    def test_status_preserves_recovery_reason_and_candidate_identity(self) -> None:
        handler = PTDocumentHandler()
        context = {
            "found": True, "process_id": "P1", "document_id": "D1", "candidate_id": "C1",
            "source_url": "https://synthetic.invalid/C1", "selection_reason": "search_open_error",
            "acquisition_state": state(access="IFRAME_UNAVAILABLE"),
            "acquisition_diagnostic_code": "IFRAME_UNAVAILABLE", "acquisition_diagnostic_stage": "access",
        }
        handler.record_search_outcome(spec=SPEC, processo="P1", collection_context=context)
        record = handler._tracking_records[0]
        self.assertEqual(record["acquisition_issue_code"], "technical_access")
        self.assertTrue(record["acquisition_recoverable"])
        self.assertEqual((record["process_id"], record["document_id"], record["candidate_id"]), ("P1", "D1", "C1"))

    def test_two_candidates_and_csv_round_trip_remain_separate(self) -> None:
        output = Path(__file__).resolve().parent / "_tmp_acquisition_recoverability"
        shutil.rmtree(output, ignore_errors=True)
        output.mkdir()
        try:
            rows = [
                row_for(state(access="IFRAME_UNAVAILABLE"), diagnostic="IFRAME_UNAVAILABLE", candidate="C1"),
                row_for(state("NOT_FOUND", "NOT_ATTEMPTED", "UNKNOWN"), status="not_found", candidate="C2"),
            ]
            csv_writer.write_csv(rows, output / "pt_status_execucao_latest.csv")
            issues = [issue for issue in collect_review_issues(output) if issue["document_type"] == "pt"]
            exported = export_review_queue(output)
            csv_text = exported["latest_path"].read_text(encoding="utf-8-sig")
        finally:
            shutil.rmtree(output, ignore_errors=True)
        self.assertEqual({issue["candidate_id"] for issue in issues}, {"C1", "C2"})
        self.assertEqual({issue["is_recoverable"] for issue in issues}, {True, False})
        self.assertIn("IFRAME_UNAVAILABLE", csv_text)
        self.assertIn("technical_access", csv_text)

    def test_recovery_classification_does_not_mutate_semantic_fields(self) -> None:
        record = {"classification": "KEEP", "gold": True, "winner": "C1", "canonical": True, "publication": "published_gold"}
        before = dict(record)
        acquisition_recovery_payload(state(access="IFRAME_UNAVAILABLE"), {"code": "IFRAME_UNAVAILABLE"})
        self.assertEqual(record, before)


if __name__ == "__main__":
    unittest.main()
