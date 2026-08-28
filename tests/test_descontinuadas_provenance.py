from __future__ import annotations

import json
import shutil
import sys
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import SourceKind
from app.services.parcerias_descontinuadas_normalizer import (
    build_descontinuada_v2_record,
    build_normalized_record,
    export_normalized_csv,
)
from app.services.provenance_validation import validate_field_provenance


class DescontinuadasProvenanceTests(unittest.TestCase):
    def test_annotation_label_raw_position_and_statuses_are_preserved(self) -> None:
        row = self._row()
        legacy = build_normalized_record(row, reference_date=date(2026, 1, 1))
        fields = self._fields(build_descontinuada_v2_record(row, legacy))

        parceiro = fields["parceiro"]
        self.assertEqual(legacy["parceiro"], parceiro.value)
        self.assertIs(parceiro.evidences[0].source_kind, SourceKind.PREVIEW)
        self.assertEqual("PARCEIROS", parceiro.evidences[0].location.section)
        self.assertEqual(1, parceiro.evidences[0].location.position)
        self.assertEqual("Parceiro A e Parceiro B", parceiro.evidences[0].raw_evidence)
        self.assertEqual("Vigente", legacy["status_normalizado"])
        self.assertEqual("Encerrado", legacy["status_calculado"])
        self.assertEqual("encerrado", legacy["status_categoria"])
        self.assertEqual(legacy["status_normalizado"], fields["status_normalizado"].value)
        self.assertEqual(legacy["status_calculado"], fields["status_calculado"].value)
        self.assertTrue(all(validate_field_provenance(field).is_valid for field in fields.values()))

    def test_derived_rules_and_absence_have_no_fabricated_evidence(self) -> None:
        row = {"processo": "60090.000001/2020-00", "anotacoes": "Organizacional: Despachos."}
        legacy = build_normalized_record(row, reference_date=date(2026, 1, 1))
        fields = self._fields(build_descontinuada_v2_record(row, legacy))

        self.assertIs(fields["status_calculado"].evidences[0].source_kind, SourceKind.DERIVED)
        self.assertEqual("descontinuadas.status.calculate_legacy", fields["status_calculado"].evidences[0].rule_id)
        self.assertIs(fields["parceiro"].state, FieldState.NOT_EVALUATED)
        self.assertEqual((), fields["parceiro"].evidences)

    def test_dual_write_preserves_legacy_and_writes_preview_sidecar(self) -> None:
        root = Path.cwd() / ".tmp_prov_p1_004_des"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir()
        try:
            with patch("app.services.parcerias_descontinuadas_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off = export_normalized_csv(root, [self._row()], reference_date=date(2026, 1, 1))
            legacy = off["latest_path"].read_bytes()
            self.assertIsNone(off["v2_path"])
            with patch("app.services.parcerias_descontinuadas_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on = export_normalized_csv(root, [self._row()], reference_date=date(2026, 1, 1))
            self.assertEqual(legacy, on["latest_path"].read_bytes())
            sidecar = json.loads(on["v2_path"].read_text(encoding="utf-8"))
            parceiro = next(field for field in sidecar["records"][0]["fields"] if field["field_name"] == "parceiro")
            self.assertEqual("preview", parceiro["evidences"][0]["source_kind"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    @staticmethod
    def _fields(v2: dict[str, object]) -> dict[str, FieldResult]:
        return {item["field_name"]: FieldResult.from_dict(item) for item in v2["fields"]}

    @staticmethod
    def _row() -> dict[str, str]:
        return {
            "processo": "60090.000001/2020-00",
            "anotacoes": (
                "TIPO: ACT\nPARCEIROS: Parceiro A\ne Parceiro B\nOBJETO: Cooperacao.\n"
                "STATUS: Vigente\nDATA DE VENCIMENTO: 31/12/2028\n"
                "TERMO DE ENCERRAMENTO: n 7/2025 (8210725)"
            ),
        }


if __name__ == "__main__":
    unittest.main()
