from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services import offline_reprocessor as module  # noqa: E402


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class HistoricalBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.offline = patch.object(module, "get_settings", return_value=SimpleNamespace(offline_only=True))
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def reprocess(self, root: Path, payload: dict, *, family: str = "administrative") -> tuple[dict, module.ReprocessResult]:
        source = root / "snapshot.json"
        source.write_text(json.dumps({"metadata": {"family": family}, "payload": payload}), encoding="utf-8")
        result = module.reprocess_snapshot(source, root / "derived")
        return json.loads(result.output.read_text(encoding="utf-8"))["record"], result  # type: ignore[union-attr]

    def test_complete_explicit_identity_is_observed(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            record, _ = self.reprocess(Path(temporary), {
                "processo": "P1", "document_id": "D1", "candidate_id": "C1",
                "source_url": "https://synthetic.invalid/view", "snapshot": {},
            })
        self.assertEqual(record["identity"], {
            "process_id": "P1", "document_id": "D1", "candidate_id": "C1",
            "source_url": "https://synthetic.invalid/view",
        })
        self.assertEqual(record["backfill_metadata"]["fields"]["identity.document_id"]["classification"], "observed")

    def test_legacy_document_equal_process_does_not_become_document_id(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            record, _ = self.reprocess(Path(temporary), {"processo": "P1", "documento": "P1", "snapshot": {}})
        self.assertEqual(record["identity"]["process_id"], "P1")
        self.assertIsNone(record["identity"]["document_id"])
        self.assertEqual(record["backfill_metadata"]["fields"]["identity.document_id"]["classification"], "not_inferable")

    def test_structured_url_recovers_ids_with_validated_parser(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            record, _ = self.reprocess(Path(temporary), {
                "processo": "P1", "snapshot": {
                    "url": "https://synthetic.invalid/?id_documento=8707829&id_anexo=1139528"
                },
            })
        self.assertEqual(record["identity"]["document_id"], "8707829")
        self.assertEqual(record["identity"]["candidate_id"], "1139528")
        self.assertEqual(record["backfill_metadata"]["fields"]["identity.document_id"]["classification"], "derived")

    def test_found_only_changes_discovery_and_empty_content_is_ambiguous(self) -> None:
        for found, expected in ((True, "FOUND"), (False, "NOT_FOUND")):
            with self.subTest(found=found), tempfile.TemporaryDirectory(dir=ROOT) as temporary:
                record, _ = self.reprocess(Path(temporary), {
                    "processo": "P1", "collection": {"found": found},
                    "snapshot": {"text": "content" if found else "", "tables": [], "extraction_mode": "html_dom"},
                })
                self.assertEqual(record["acquisition_state"], {
                    "discovery": expected, "opening": "NOT_ATTEMPTED",
                    "access": "UNKNOWN", "extraction": "NOT_ATTEMPTED",
                })

    def test_explicit_extractor_error_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            record, _ = self.reprocess(Path(temporary), {
                "processo": "P1", "collection": {"found": True},
                "snapshot": {"extraction_error": "synthetic extractor exception"},
            })
        self.assertEqual(record["acquisition_state"]["extraction"], "EXTRACTION_FAILED")
        self.assertEqual(record["backfill_metadata"]["fields"]["acquisition_state.extraction"]["classification"], "observed")

    def test_conflicting_identity_sources_do_not_choose_a_winner(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            record, result = self.reprocess(Path(temporary), {
                "processo": "P1", "document_id": "111",
                "snapshot": {"url": "https://synthetic.invalid/?id_documento=222"},
            })
        self.assertIsNone(record["identity"]["document_id"])
        self.assertIn("identity.document_id", record["backfill_metadata"]["conflicts"])
        self.assertEqual(result.status, "processed")

    def test_four_families_original_hashes_and_two_runs_are_identical(self) -> None:
        fixtures = {
            "act": "act_pdf_extracted.json", "pt": "pt_html_extracted.json",
            "ted": "ted_normalizer_rich.json", "administrative": "administrative_docx_extracted.json",
        }
        fixture_root = ROOT / "tests" / "fixtures" / "documents"
        before = {name: digest(fixture_root / filename) for name, filename in fixtures.items()}
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            first, second = root / "one", root / "two"
            for name, filename in fixtures.items():
                module.reprocess_snapshot(fixture_root / filename, first / name)
                module.reprocess_snapshot(fixture_root / filename, second / name)
            first_files = {p.relative_to(first).as_posix(): p.read_bytes() for p in first.rglob("*.json")}
            second_files = {p.relative_to(second).as_posix(): p.read_bytes() for p in second.rglob("*.json")}
            self.assertEqual(first_files, second_files)
        self.assertEqual({name: digest(fixture_root / filename) for name, filename in fixtures.items()}, before)

    def test_batch_report_is_aggregated_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            for name, document_id in (("a", "101"), ("b", None)):
                (source / f"{name}.json").write_text(json.dumps({
                    "metadata": {"family": "administrative"},
                    "payload": {"processo": name, "document_id": document_id, "collection": {"found": True}, "snapshot": {}},
                }), encoding="utf-8")
            first = module.reprocess_directory(source, root / "first").backfill_summary
            second = module.reprocess_directory(source, root / "second").backfill_summary
        self.assertEqual(first, second)
        self.assertEqual(first["records"], 2)
        self.assertEqual(first["identity_recovered"], {"process_id": 2, "document_id": 1, "candidate_id": 0})
        self.assertEqual(first["identity_missing"], 1)


if __name__ == "__main__":
    unittest.main()
