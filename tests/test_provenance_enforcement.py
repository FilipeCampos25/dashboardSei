from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.config import Settings
from app.services.contract_adapters import write_v2_sidecar
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import FieldEvidence, SourceKind
from app.services.normalization_contract import DocumentIdentity
from app.services.provenance_enforcement import (
    ProvenanceContractError,
    ProvenanceEnforcementMode,
    enforce_provenance,
)


class ProvenanceEnforcementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd() / ".tmp_prov_p1_005"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir()

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_default_is_off_and_modes_are_explicit(self) -> None:
        self.assertEqual("off", Settings(_env_file=None).provenance_enforcement_mode)
        self.assertEqual(
            {"off", "warn", "error"},
            {mode.value for mode in ProvenanceEnforcementMode},
        )

    def test_warn_reports_structured_violation_and_does_not_block(self) -> None:
        records = [self._record(FieldResult("objeto", FieldState.PRESENT, "valor"))]

        report = enforce_provenance(records, family="ACT", mode="warn")

        self.assertFalse(report.is_valid)
        self.assertEqual("missing_evidence", report.violations[0]["code"])
        self.assertEqual("ACT", report.violations[0]["family"])
        self.assertEqual("document_id:doc-1", report.violations[0]["record_id"])
        self.assertEqual("PRESENT", report.violations[0]["state"])
        self.assertEqual("warn", report.violations[0]["mode"])
        self.assertEqual("valor", records[0]["fields"][0]["value"])

    def test_error_blocks_the_same_violation_with_structured_report(self) -> None:
        records = [self._record(FieldResult("objeto", FieldState.PRESENT, "valor"))]

        with self.assertRaisesRegex(ProvenanceContractError, "provenance contract violated") as raised:
            enforce_provenance(records, family="ACT", mode="error")

        self.assertEqual("missing_evidence", raised.exception.report.violations[0]["code"])

    def test_required_evidence_details_are_classified(self) -> None:
        cases = (
            (FieldEvidence("documento", source_kind=SourceKind.DOCUMENT), "missing_document_reference"),
            (FieldEvidence("relacionado", source_kind=SourceKind.RELATED_DOCUMENT), "missing_document_reference"),
            (FieldEvidence("externo", source_kind=SourceKind.EXTERNAL), "missing_external_reference"),
            (FieldEvidence("derivado", source_kind=SourceKind.DERIVED), "missing_derivation_rule"),
        )
        fields = [FieldResult(e.field_name, FieldState.PRESENT, "valor", (e,)) for e, _ in cases]

        report = enforce_provenance([self._record(*fields)], family="PT", mode="warn")

        codes = [item["code"] for item in report.violations]
        for _, code in cases:
            self.assertIn(code, codes)
        related = next(item for item in report.violations if item["field_name"] == "relacionado")
        self.assertEqual("related_document", related["source_kind"])
        self.assertIn("missing_relation", codes)

    def test_multiple_violations_and_summary_are_deterministic(self) -> None:
        fields = (
            FieldResult("zeta", FieldState.PRESENT, "z"),
            FieldResult("alfa", FieldState.PRESENT, "a"),
        )
        records = [self._record(*fields)]

        first = enforce_provenance(records, family="TED", mode="warn").to_dict()
        second = enforce_provenance(records, family="TED", mode="warn").to_dict()

        self.assertEqual(first, second)
        self.assertEqual(["zeta", "alfa"], [item["field_name"] for item in first["violations"]])
        self.assertEqual({"alfa": 1, "zeta": 1}, first["counts_by_field"])

    def test_all_migrated_families_pass_warn_and_error_without_false_positive(self) -> None:
        valid = self._record(self._valid_field())
        for family in ("ACT", "PT", "TED", "Administrativos", "Descontinuadas"):
            with self.subTest(family=family, mode="warn"):
                self.assertTrue(enforce_provenance([valid], family=family, mode="warn").is_valid)
            with self.subTest(family=family, mode="error"):
                self.assertTrue(enforce_provenance([valid], family=family, mode="error").is_valid)

    def test_warn_sidecar_contains_report_and_error_is_atomic(self) -> None:
        target = self.root / "artifact.v2.json"
        invalid = {"schema_version": "2.0", "records": [self._record(
            FieldResult("objeto", FieldState.PRESENT, "valor")
        )]}

        write_v2_sidecar(target, invalid, family="ACT", enforcement_mode="warn")
        warn_payload = json.loads(target.read_text(encoding="utf-8"))
        self.assertEqual(1, warn_payload["provenance_enforcement"]["violation_count"])
        previous = target.read_bytes()

        with self.assertRaises(ProvenanceContractError):
            write_v2_sidecar(target, invalid, family="ACT", enforcement_mode="error")

        self.assertEqual(previous, target.read_bytes())
        self.assertFalse(target.with_suffix(target.suffix + ".tmp").exists())

    def test_off_preserves_payload_bytes_and_valid_error_publishes(self) -> None:
        payload = {"schema_version": "2.0", "records": [self._record(self._valid_field())]}
        off = self.root / "off.v2.json"
        strict = self.root / "strict.v2.json"

        write_v2_sidecar(off, payload, family="ACT", enforcement_mode="off")
        write_v2_sidecar(strict, payload, family="ACT", enforcement_mode="error")

        off_payload = json.loads(off.read_text(encoding="utf-8"))
        strict_payload = json.loads(strict.read_text(encoding="utf-8"))
        self.assertNotIn("provenance_enforcement", off_payload)
        self.assertEqual(0, strict_payload["provenance_enforcement"]["violation_count"])
        self.assertEqual(payload["records"], strict_payload["records"])

    @staticmethod
    def _valid_field() -> FieldResult:
        evidence = FieldEvidence(
            "objeto",
            source_kind=SourceKind.DOCUMENT,
            source_document=DocumentIdentity(process_id="process-1", document_id="doc-1"),
        )
        return FieldResult("objeto", FieldState.PRESENT, "valor", (evidence,))

    @staticmethod
    def _record(*fields: FieldResult) -> dict[str, object]:
        return {
            "identity": {
                "process_id": "process-1",
                "document_id": "doc-1",
                "candidate_id": None,
                "source_url": None,
            },
            "fields": [field.to_dict() for field in fields],
        }


if __name__ == "__main__":
    unittest.main()
