from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldResult, FieldState
from app.services.provenance_validation import validate_field_provenance
from app.services.ted_normalizer import (
    build_normalized_record,
    build_ted_v2_record,
    export_normalized_csv,
)
from tests.fixture_loader import load_fixture


class TEDProvenanceTests(unittest.TestCase):
    def _rich(self):
        payload = load_fixture("ted_normalizer_rich.json")["payload"]
        payload["collection"] = {
            "source_url": "https://sei.example/documento?id_documento=12345",
        }
        row, diagnostics = build_normalized_record(payload, "ted_normalizer_rich.json")
        return payload, row, diagnostics

    def test_document_fields_preserve_raw_rule_and_structural_location(self) -> None:
        payload, row, diagnostics = self._rich()
        v2 = build_ted_v2_record(row, payload, diagnostics, source_path="ted_normalizer_rich.json")
        fields = {item["field_name"]: item for item in v2["fields"]}

        unit = fields["unidade_descentralizadora"]
        self.assertEqual(row["unidade_descentralizadora"], unit["value"])
        self.assertEqual("PRESENT", unit["state"])
        self.assertEqual("document", unit["evidences"][0]["source_kind"])
        self.assertEqual("12345", unit["evidences"][0]["source_document"]["document_id"])
        self.assertEqual(1, unit["evidences"][0]["location"]["table_index"])
        self.assertEqual(2, unit["evidences"][0]["location"]["row_index"])
        self.assertEqual(2, unit["evidences"][0]["location"]["column_index"])
        self.assertEqual(
            next(item for item in diagnostics if item["field_name"] == "unidade_descentralizadora")["raw_value"],
            unit["evidences"][0]["raw_evidence"],
        )
        self.assertTrue(validate_field_provenance(FieldResult.from_dict(unit)).is_valid)

        numero = fields["numero_ted"]
        self.assertIsNone(numero["evidences"][0]["location"])

    def test_derived_field_and_legacy_diagnostics_are_preserved(self) -> None:
        payload, row, diagnostics = self._rich()
        v2 = build_ted_v2_record(row, payload, diagnostics)
        fields = {item["field_name"]: item for item in v2["fields"]}

        end = fields["vigencia_fim"]
        self.assertEqual("derived", end["evidences"][0]["source_kind"])
        self.assertEqual("ted.vigencia.fim.calculated", end["evidences"][0]["rule_id"])
        self.assertTrue(validate_field_provenance(FieldResult.from_dict(end)).is_valid)
        self.assertEqual(diagnostics, v2["ted_field_diagnostics"])
        unit_diagnostic = next(
            item for item in v2["ted_field_diagnostics"]
            if item["field_name"] == "unidade_descentralizadora"
        )
        self.assertEqual("high", unit_diagnostic["confidence"])
        self.assertEqual("", unit_diagnostic["warning"])

        reports = [validate_field_provenance(FieldResult.from_dict(item)) for item in v2["fields"]]
        self.assertTrue(all(report.is_valid for report in reports), [report.codes for report in reports])

    def test_empty_ted_has_no_fabricated_evidence_and_keeps_legacy_publication(self) -> None:
        payload = load_fixture("ted_empty.json")["payload"]
        row, diagnostics = build_normalized_record(payload, "ted_empty.json")
        v2 = build_ted_v2_record(row, payload, diagnostics)

        self.assertEqual("published_gold", row["publication_status"])
        self.assertEqual("published_gold", v2["legacy_publication_status"])
        self.assertTrue(all(item["state"] != "PRESENT" for item in v2["fields"]))
        self.assertTrue(all(not item["evidences"] for item in v2["fields"]))

    def test_dual_write_is_opt_in_and_keeps_legacy_bytes_equal(self) -> None:
        payload, _, _ = self._rich()
        output_dir = Path(__file__).resolve().parent / "_tmp_ted_provenance"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            source = output_dir / "termo_execucao_descentralizada_fixture.json"
            source.write_text(json.dumps(payload), encoding="utf-8")
            records = [{"publication_status": "published_gold", "json_path": str(source)}]

            with patch("app.services.ted_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off = export_normalized_csv(output_dir, records=records)
            legacy_off = off["latest_path"].read_bytes()
            self.assertIsNone(off["v2_path"])

            with patch("app.services.ted_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on = export_normalized_csv(output_dir, records=records)
            self.assertEqual(legacy_off, on["latest_path"].read_bytes())
            self.assertTrue(on["v2_path"].is_file())
            sidecar = json.loads(on["v2_path"].read_text(encoding="utf-8"))
            self.assertEqual("PRESENT", next(
                item for item in sidecar["records"][0]["fields"] if item["field_name"] == "objeto"
            )["state"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
