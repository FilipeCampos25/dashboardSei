from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.config import Settings  # noqa: E402
from app.services.contract_adapters import (  # noqa: E402
    adapt_legacy_record,
    v2_sidecar_path,
    write_csv_with_v2,
    write_v2_sidecar,
)


class ContractAdapterTests(unittest.TestCase):
    def test_flag_defaults_off(self) -> None:
        self.assertFalse(Settings(_env_file=None).v2_dual_write)

    def test_flag_off_preserves_legacy_bytes_and_creates_no_sidecar(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            legacy = Path(directory) / "sample_latest.csv"
            records = [{"processo": "123", "campo": "valor"}]
            write_csv_with_v2(records, legacy, columns=("processo", "campo"), enabled=False)
            off_bytes = legacy.read_bytes()

            legacy.unlink()
            write_csv_with_v2(records, legacy, columns=("processo", "campo"), enabled=True, field_names=("campo",))
            self.assertEqual(legacy.read_bytes(), off_bytes)
            self.assertTrue(v2_sidecar_path(legacy).is_file())

    def test_flag_on_writes_parseable_deterministic_separate_v2(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            legacy = Path(directory) / "sample_latest.csv"
            records = [{"processo": "123", "campo": "valor"}]
            first = write_csv_with_v2(records, legacy, enabled=True, field_names=("campo",))
            first_bytes = first.read_bytes()  # type: ignore[union-attr]
            second = write_csv_with_v2(records, legacy, enabled=True, field_names=("campo",))
            self.assertEqual(first_bytes, second.read_bytes())  # type: ignore[union-attr]
            self.assertEqual(json.loads(first_bytes)["schema_version"], "2.0")
            self.assertEqual(first.name, "sample.v2.json")  # type: ignore[union-attr]
            self.assertEqual(first.parent.name, "v2")  # type: ignore[union-attr]

    def test_flag_on_adds_portable_ref_without_changing_legacy_path(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            snapshot = root / "candidates" / "record.json"
            snapshot.parent.mkdir()
            snapshot.write_text("{}", encoding="utf-8")
            legacy = root / "sample_latest.csv"
            records = [{"processo": "123", "json_path": str(snapshot)}]
            sidecar = write_csv_with_v2(
                records,
                legacy,
                columns=("processo", "json_path"),
                enabled=True,
                artifact_root=root,
            )
            legacy_text = legacy.read_text(encoding="utf-8-sig")
            payload = json.loads(sidecar.read_text(encoding="utf-8"))  # type: ignore[union-attr]
            self.assertIn(str(snapshot), legacy_text)
            self.assertEqual(payload["records"][0]["artifact_ref"], {
                "root_kind": "artifact_root", "relative_path": "candidates/record.json",
            })
            self.assertNotIn(str(root), sidecar.read_text(encoding="utf-8"))  # type: ignore[union-attr]

    def test_external_legacy_path_is_diagnostic_not_fabricated(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            adapted = adapt_legacy_record(
                {"processo": "123", "json_path": str(root.parent / "external.json")},
                artifact_root=root,
            )
            self.assertNotIn("artifact_ref", adapted)
            self.assertIn("json_path_not_portable", adapted["diagnostics"])

    def test_previous_v2_payload_without_portable_ref_remains_supported(self) -> None:
        adapted = adapt_legacy_record({"processo": "123", "json_path": "legacy.json"})
        self.assertNotIn("artifact_ref", adapted)

    def test_ambiguous_identity_empty_field_and_partial_found_are_honest(self) -> None:
        adapted = adapt_legacy_record(
            {"processo": "123", "documento": "123", "found": True, "campo": ""},
            field_names=("campo",),
        )
        self.assertIsNone(adapted["identity"]["document_id"])
        self.assertEqual(adapted["acquisition_state"], {
            "discovery": "FOUND", "opening": "NOT_ATTEMPTED", "access": "UNKNOWN", "extraction": "NOT_ATTEMPTED"
        })
        self.assertEqual(adapted["fields"][0]["state"], "NOT_EVALUATED")
        self.assertIn("legacy_documento_not_promoted", adapted["diagnostics"])

    def test_legacy_gold_does_not_create_gold_or_semantic_decisions(self) -> None:
        adapted = adapt_legacy_record({"processo": "123", "publication_status": "published_gold"})
        self.assertEqual(adapted["semantic_state"]["publication"], "NOT_EVALUATED")
        self.assertNotIn("record_gold", adapted)
        self.assertIn("legacy_publication_status_not_expanded", adapted["diagnostics"])

    def test_explicit_preview_provenance_is_mapped_without_fabricated_location(self) -> None:
        adapted = adapt_legacy_record(
            {"processo": "123", "campo": {"value": "x", "source_type": "preview", "rule_id": "r1"}},
            field_names=("campo",),
        )
        evidence = adapted["fields"][0]["evidences"][0]
        self.assertEqual(evidence["source_kind"], "preview")
        self.assertIsNone(evidence["source_document"])
        self.assertIsNone(evidence["location"])
        self.assertNotIn("confidence", evidence)

    def test_missing_provenance_creates_no_evidence(self) -> None:
        adapted = adapt_legacy_record({"processo": "123", "campo": "x"}, field_names=("campo",))
        self.assertEqual(adapted["fields"][0]["evidences"], [])

    def test_v2_write_error_is_explicit_and_leaves_no_partial_sidecar(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            target = Path(directory) / "v2" / "sample.v2.json"
            with patch("pathlib.Path.write_text", side_effect=OSError("simulated")):
                with self.assertRaisesRegex(OSError, "simulated"):
                    write_v2_sidecar(target, {"value": 1})
            self.assertFalse(target.exists())
            self.assertFalse(target.with_suffix(".json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
