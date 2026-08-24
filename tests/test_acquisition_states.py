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

from app.documents.common import acquisition_state_payload, build_basic_tracking_record, save_snapshot_json  # noqa: E402
from app.documents.types import DocumentTypeSpec  # noqa: E402
from app.services.contract_adapters import adapt_legacy_record  # noqa: E402


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


class AcquisitionStatesTests(unittest.TestCase):
    def test_found_does_not_imply_opening_access_or_extraction(self) -> None:
        self.assertEqual(acquisition_state_payload({"found": True}), {
            "discovery": "FOUND",
            "opening": "NOT_ATTEMPTED",
            "access": "UNKNOWN",
            "extraction": "NOT_ATTEMPTED",
        })

    def test_opened_does_not_imply_accessible(self) -> None:
        state = acquisition_state_payload(
            {"found": True},
            {"acquisition_observation": {"opening_attempted": True, "opened": True}},
        )
        self.assertEqual(state["opening"], "OPENED")
        self.assertEqual(state["access"], "UNKNOWN")
        self.assertEqual(state["extraction"], "NOT_ATTEMPTED")

    def test_accessible_does_not_imply_extracted(self) -> None:
        state = acquisition_state_payload(
            {"found": True},
            {"acquisition_observation": {
                "opening_attempted": True,
                "opened": True,
                "access_observed": True,
            }},
        )
        self.assertEqual(state["access"], "ACCESSIBLE")
        self.assertEqual(state["extraction"], "NOT_ATTEMPTED")

    def test_empty_partial_complete_and_failure_require_explicit_evidence(self) -> None:
        cases = (
            ({"text": "", "tables": [], "extraction_complete": True}, "EMPTY_CONTENT"),
            ({"text": "incomplete", "extraction_partial": True}, "CONTENT_PARTIAL"),
            ({"text": "complete", "extraction_complete": True}, "EXTRACTED"),
            ({"text": "", "extraction_error": "extractor failed"}, "EXTRACTION_FAILED"),
        )
        for values, expected in cases:
            with self.subTest(expected=expected):
                observation = {
                    "opening_attempted": True,
                    "opened": True,
                    "access_observed": True,
                    "extraction_attempted": True,
                    **{key: value for key, value in values.items() if key.startswith("extraction_")},
                }
                snapshot = {
                    "text": values.get("text", ""),
                    "tables": values.get("tables", []),
                    "acquisition_observation": observation,
                }
                self.assertEqual(acquisition_state_payload({"found": True}, snapshot)["extraction"], expected)

    def test_two_candidates_keep_distinct_identity_and_state(self) -> None:
        records = []
        for document_id, extraction in (("D1", "EXTRACTED"), ("D2", "EMPTY_CONTENT")):
            state = {
                "discovery": "FOUND", "opening": "OPENED", "access": "ACCESSIBLE", "extraction": extraction
            }
            records.append(build_basic_tracking_record(
                spec=SPEC,
                processo="P",
                protocolo_documento="P",
                snapshot={"text": "x" if extraction == "EXTRACTED" else "", "tables": []},
                output_path=None,
                collection_context={"found": True, "document_id": document_id, "acquisition_state": state},
            ))
        self.assertEqual({record["document_id"] for record in records}, {"D1", "D2"})
        self.assertEqual({record["acquisition_state"]["extraction"] for record in records}, {"EXTRACTED", "EMPTY_CONTENT"})

    def test_snapshot_tracking_and_adapter_round_trip_preserve_state_and_legacy(self) -> None:
        state = {"discovery": "FOUND", "opening": "OPENED", "access": "ACCESSIBLE", "extraction": "EMPTY_CONTENT"}
        context = {"found": True, "document_id": "D1", "candidate_id": "C1", "acquisition_state": state}
        snapshot = {"text": "", "tables": [], "extraction_mode": "html_dom"}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = save_snapshot_json(
                spec=SPEC,
                processo="P",
                protocolo_documento="legacy-document",
                snapshot=snapshot,
                output_dir=Path(directory),
                logger=Mock(),
                extra_payload={"collection": context},
            )
            payload = json.loads(path.read_text(encoding="utf-8"))  # type: ignore[union-attr]
            tracking = build_basic_tracking_record(
                spec=SPEC,
                processo="P",
                protocolo_documento="legacy-document",
                snapshot=snapshot,
                output_path=path,
                collection_context=context,
            )
        self.assertEqual(payload["acquisition_state"], state)
        self.assertEqual(adapt_legacy_record(tracking)["acquisition_state"], state)
        self.assertEqual(tracking["documento"], "legacy-document")
        self.assertTrue(tracking["found"])

    def test_empty_content_does_not_create_semantic_decisions(self) -> None:
        adapted = adapt_legacy_record({
            "processo": "P",
            "found": True,
            "acquisition_state": {
                "discovery": "FOUND", "opening": "OPENED", "access": "ACCESSIBLE", "extraction": "EMPTY_CONTENT"
            },
        })
        self.assertEqual(adapted["semantic_state"]["classification"], "NOT_CLASSIFIED")
        self.assertEqual(adapted["semantic_state"]["publication"], "NOT_EVALUATED")


if __name__ == "__main__":
    unittest.main()
