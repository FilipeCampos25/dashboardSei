from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.documento_administrativo_normalizer import (
    build_administrativo_v2_record,
    build_normalized_record,
    export_normalized_csv,
)
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import SourceKind
from app.services.provenance_validation import validate_field_provenance


class AdministrativoProvenanceTests(unittest.TestCase):
    def test_document_raw_location_and_legacy_values_are_preserved(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, Path("administrativo.json"))
        fields = self._fields(build_administrativo_v2_record(
            legacy, payload, source_path="candidates/administrativo.json"
        ))

        self.assertEqual("12/03/1989", legacy["data"])
        self.assertEqual(legacy["data"], fields["data"].value)
        self.assertIs(fields["data"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertEqual("12/03/1989", fields["data"].evidences[0].raw_evidence)
        self.assertEqual("candidates/administrativo.json", fields["data"].evidences[0].location.source_path)
        self.assertEqual(legacy["documentos_mencionados"], fields["documentos_mencionados"].value)
        self.assertEqual(legacy["documentos_mencionados"], fields["documentos_mencionados"].evidences[0].raw_evidence)
        self.assertTrue(all(validate_field_provenance(field).is_valid for field in fields.values()))

    def test_derived_and_absent_fields_are_honest(self) -> None:
        payload = self._payload()
        payload["snapshot"]["text"] = "Memorando sem campo de prazo."
        legacy = build_normalized_record(payload, Path("administrativo.json"))
        fields = self._fields(build_administrativo_v2_record(legacy, payload))

        self.assertIs(fields["funcao_administrativa"].evidences[0].source_kind, SourceKind.DERIVED)
        self.assertEqual("Memorando sem campo de prazo.", fields["funcao_administrativa"].evidences[0].raw_evidence)
        self.assertEqual("Memorando sem campo de prazo.", fields["resolved_document_type"].evidences[0].raw_evidence)
        self.assertIs(fields["prazo"].state, FieldState.NOT_EVALUATED)
        self.assertIsNone(fields["prazo"].value)
        self.assertEqual((), fields["prazo"].evidences)
        self.assertIsNone(fields["assunto"].evidences[0].location)

    def test_dual_write_is_opt_in_and_legacy_bytes_do_not_change(self) -> None:
        root = Path.cwd() / ".tmp_prov_p1_004_admin"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir()
        try:
            source = root / "documento_administrativo_fixture.json"
            source.write_text(json.dumps(self._payload()), encoding="utf-8")
            records = [{"publication_status": "published_gold", "json_path": str(source)}]
            with patch("app.services.documento_administrativo_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off = export_normalized_csv(root, records)
            legacy = off["latest_path"].read_bytes()
            self.assertIsNone(off["v2_path"])
            with patch("app.services.documento_administrativo_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on = export_normalized_csv(root, records)
            self.assertEqual(legacy, on["latest_path"].read_bytes())
            self.assertTrue(on["v2_path"].is_file())
        finally:
            shutil.rmtree(root, ignore_errors=True)

    @staticmethod
    def _fields(v2: dict[str, object]) -> dict[str, FieldResult]:
        return {item["field_name"]: FieldResult.from_dict(item) for item in v2["fields"]}

    @staticmethod
    def _payload() -> dict[str, object]:
        return {
            "processo": "60090.000001/2020-00",
            "documento": "4455667",
            "snapshot": {
                "title": "Memorando 1/2020",
                "url": "https://sei.example/documento?id_documento=4455667",
                "text": (
                    "De: Unidade A\nPara: Unidade B\nAssunto: Encaminhamento\n"
                    "Lei de 12/03/1989. Encaminho o processo 60090.000002/2020-00 "
                    "e o documento SEI 7788990. Prazo de 10 dias. "
                    "Documento assinado eletronicamente em 20/04/2020."
                ),
            },
            "collection": {"document_id": "4455667", "candidate_id": "candidate-admin"},
        }


if __name__ == "__main__":
    unittest.main()
