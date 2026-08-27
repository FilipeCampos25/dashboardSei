from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.act_normalizer import build_act_v2_record, build_normalized_record, export_normalized_csv
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import SourceKind
from app.services.provenance_validation import ProvenanceViolationCode, validate_field_provenance


class ActProvenanceTests(unittest.TestCase):
    def test_document_and_derived_fields_preserve_values_and_auditable_identity(self) -> None:
        payload = self._payload()
        source_path = Path("candidates/act-source.json")
        legacy = build_normalized_record(payload, source_path)

        v2 = build_act_v2_record(legacy, payload, source_path=source_path)
        fields = self._fields(v2)

        self.assertEqual(legacy["objeto"], fields["objeto"].value)
        self.assertIs(fields["objeto"].state, FieldState.PRESENT)
        evidence = fields["objeto"].evidences[0]
        self.assertIs(evidence.source_kind, SourceKind.DOCUMENT)
        self.assertEqual("4433322", evidence.source_document.document_id)
        self.assertEqual("candidate-7", evidence.source_document.candidate_id)
        self.assertEqual(str(source_path), evidence.location.source_path)
        self.assertTrue(validate_field_provenance(fields["objeto"]).is_valid)

        self.assertEqual(legacy["vigencia_fim"], fields["vigencia_fim"].value)
        self.assertIs(fields["vigencia_fim"].evidences[0].source_kind, SourceKind.DERIVED)
        self.assertEqual(
            legacy["field_source_vigencia"],
            fields["vigencia_fim"].evidences[0].rule_id,
        )
        self.assertTrue(validate_field_provenance(fields["vigencia_fim"]).is_valid)
        self.assertTrue(
            all(
                validate_field_provenance(field).is_valid
                for field in fields.values()
                if field.state is FieldState.PRESENT
            )
        )

    def test_location_is_honestly_absent_and_round_trip_preserves_it(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, Path("act-source.json"))

        v2 = build_act_v2_record(legacy, payload)
        objeto = self._fields(v2)["objeto"]
        round_trip = FieldResult.from_dict(objeto.to_dict())

        self.assertIsNone(objeto.evidences[0].location)
        self.assertIsNone(round_trip.evidences[0].location)
        self.assertTrue(validate_field_provenance(round_trip).is_valid)

    def test_missing_document_id_is_not_replaced_by_process_id(self) -> None:
        payload = self._payload()
        payload["collection"] = {
            "chosen_documento": "Acordo de Cooperacao Tecnica 1/2021",
            "candidate_id": "candidate-only",
            "document_id": payload["processo"],
        }
        payload["snapshot"].pop("url")
        legacy = build_normalized_record(payload, Path("act-source.json"))

        objeto = self._fields(build_act_v2_record(legacy, payload))["objeto"]
        identity = objeto.evidences[0].source_document

        self.assertIsNone(identity.document_id)
        self.assertNotEqual(identity.process_id, identity.document_id)
        self.assertEqual("candidate-only", identity.candidate_id)
        self.assertTrue(validate_field_provenance(objeto).is_valid)

    def test_multiple_number_evidences_remain_separate(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, Path("act-source.json"))

        numero = self._fields(build_act_v2_record(legacy, payload))["numero_acordo"]

        self.assertGreaterEqual(len(numero.evidences), 2)
        self.assertTrue(all(item.raw_evidence for item in numero.evidences))
        self.assertTrue(validate_field_provenance(numero).is_valid)

    def test_present_without_evidence_is_still_rejected_by_test_p0_003_validator(self) -> None:
        invalid = FieldResult("objeto", FieldState.PRESENT, "valor")

        report = validate_field_provenance(invalid)

        self.assertFalse(report.is_valid)
        self.assertEqual((ProvenanceViolationCode.MISSING_EVIDENCE,), report.codes)

    def test_dual_write_is_opt_in_and_does_not_change_legacy_csv(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            off_dir = root / "off"
            on_dir = root / "on"
            off_dir.mkdir()
            on_dir.mkdir()
            fixture = json.dumps(self._payload(), ensure_ascii=False)
            for directory in (off_dir, on_dir):
                (directory / "acordo_cooperacao_tecnica_fixture.json").write_text(fixture, encoding="utf-8")

            with patch("app.services.act_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off_result = export_normalized_csv(off_dir)
            with patch("app.services.act_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on_result = export_normalized_csv(on_dir)

            self.assertEqual(off_result["csv_path"].read_bytes(), on_result["csv_path"].read_bytes())
            self.assertIsNone(off_result["v2_path"])
            self.assertFalse((off_dir / "v2" / "act_normalizado.v2.json").exists())
            self.assertTrue(on_result["v2_path"].exists())
            sidecar = json.loads(on_result["v2_path"].read_text(encoding="utf-8"))
            fields = {item["field_name"]: item for item in sidecar["records"][0]["fields"]}
            self.assertEqual("document", fields["objeto"]["evidences"][0]["source_kind"])

    @staticmethod
    def _fields(v2: dict[str, object]) -> dict[str, FieldResult]:
        return {
            item["field_name"]: FieldResult.from_dict(item)
            for item in v2["fields"]
        }

    @staticmethod
    def _payload() -> dict[str, object]:
        return {
            "processo": "60090.000269/2020-16",
            "snapshot": {
                "title": "SEI - 4433322 - Acordo de Cooperacao Tecnica 1/2021",
                "url": "https://sei.example/controlador.php?acao=documento&id_documento=4433322",
                "extraction_mode": "html_dom",
                "text": """
                    ACORDO DE COOPERACAO TECNICA No 1/2021 QUE ENTRE SI CELEBRAM A UNIAO,
                    REPRESENTADA PELO MINISTERIO DA DEFESA, POR INTERMEDIO DO CENSIPAM,
                    E A EMPRESA BRASILEIRA DE PESQUISA AGROPECUARIA - EMBRAPA.
                    CLAUSULA PRIMEIRA - DO OBJETO
                    O objeto do presente Acordo e a integracao de esforcos para pesquisa conjunta.
                    CLAUSULA NONA - DO PRAZO E VIGENCIA
                    O prazo de vigencia e de 5 anos a partir da data da ultima assinatura.
                    Documento assinado eletronicamente por Participante Um, em 14/12/2021.
                    Documento assinado eletronicamente por Participante Dois, em 20/12/2021.
                """,
            },
            "collection": {
                "chosen_documento": "Acordo de Cooperacao Tecnica 1/2021 (4433322)",
                "document_id": "4433322",
                "candidate_id": "candidate-7",
                "source_url": "https://sei.example/controlador.php?acao=documento&id_documento=4433322",
            },
        }


if __name__ == "__main__":
    unittest.main()
