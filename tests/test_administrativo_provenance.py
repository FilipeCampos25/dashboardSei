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
from app.services.pipeline_states import AccessState, DiscoveryState, ExtractionState, OpeningState
from app.services.provenance_validation import validate_field_provenance
from app.services.publication_policy import PublicationReasonCode
from app.services.semantic_states import PublicationState


class AdministrativoProvenanceTests(unittest.TestCase):
    def test_document_raw_location_and_legacy_values_are_preserved(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, Path("administrativo.json"))
        fields = self._fields(build_administrativo_v2_record(
            legacy, payload, source_path="candidates/administrativo.json"
        ))

        self.assertEqual("12/03/1989", legacy["data"])
        self.assertIsNone(fields["data"].value)
        self.assertIs(fields["data"].state, FieldState.UNRESOLVED)
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
            payload = self._payload()
            payload["collection"].pop("acquisition_state")
            source.write_text(json.dumps(payload), encoding="utf-8")
            records = [{
                "publication_status": "published_gold",
                "json_path": str(source),
                "found": True,
                "acquisition_state": self._successful_state(ExtractionState.EXTRACTED),
            }]
            with patch("app.services.documento_administrativo_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off = export_normalized_csv(root, records)
            legacy = off["latest_path"].read_bytes()
            self.assertIsNone(off["v2_path"])
            with patch("app.services.documento_administrativo_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on = export_normalized_csv(root, records)
            self.assertEqual(legacy, on["latest_path"].read_bytes())
            self.assertTrue(on["v2_path"].is_file())
            sidecar = json.loads(on["v2_path"].read_text(encoding="utf-8"))
            self.assertEqual(
                PublicationState.PUBLISHED.value,
                sidecar["records"][0]["document_gold_decision"]["semantic_state"]["publication"],
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_empty_whitespace_and_title_only_are_blocked_from_document_gold(self) -> None:
        cases = (
            ("empty", "", ""),
            ("whitespace", "Nota Técnica nº 12/2020", " \n\t "),
            ("title-only", "Ofício nº 7/2024", ""),
        )
        for name, title, text in cases:
            with self.subTest(case=name):
                payload = self._payload()
                payload["snapshot"].update({"title": title, "text": text, "tables": []})
                payload["collection"]["acquisition_state"] = self._successful_state(
                    ExtractionState.EMPTY_CONTENT if not text.strip() else ExtractionState.EXTRACTED
                )
                legacy = build_normalized_record(payload, Path(f"{name}.json"))
                v2 = build_administrativo_v2_record(legacy, payload)

                self.assertEqual(PublicationState.BLOCKED.value, v2["document_gold_decision"]["semantic_state"]["publication"])
                self.assertEqual(
                    [PublicationReasonCode.EMPTY_CONTENT.value],
                    v2["document_gold_decision"]["reason_codes"],
                )

    def test_technical_failure_is_not_reinterpreted_as_empty_content(self) -> None:
        payload = self._payload()
        payload["snapshot"].update({"title": "Memorando nº 1/2020", "text": "", "tables": []})
        payload["collection"]["acquisition_state"] = {
            "discovery": DiscoveryState.FOUND.value,
            "opening": OpeningState.OPENED.value,
            "access": AccessState.IFRAME_UNAVAILABLE.value,
            "extraction": ExtractionState.NOT_ATTEMPTED.value,
        }
        legacy = build_normalized_record(payload, Path("iframe.json"))
        v2 = build_administrativo_v2_record(legacy, payload)

        self.assertEqual(
            [PublicationReasonCode.IFRAME_UNAVAILABLE.value],
            v2["document_gold_decision"]["reason_codes"],
        )
        self.assertEqual(payload["collection"]["acquisition_state"], v2["acquisition_state"])

    def test_verifiable_content_remains_eligible_for_document_gold(self) -> None:
        payload = self._payload()
        payload["collection"]["acquisition_state"] = self._successful_state(ExtractionState.EXTRACTED)
        legacy = build_normalized_record(payload, Path("administrativo.json"))
        v2 = build_administrativo_v2_record(legacy, payload)

        self.assertEqual(PublicationState.PUBLISHED.value, v2["document_gold_decision"]["semantic_state"]["publication"])
        self.assertEqual(
            [PublicationReasonCode.ELIGIBLE_VERIFIABLE_CONTENT.value],
            v2["document_gold_decision"]["reason_codes"],
        )

    @staticmethod
    def _successful_state(extraction: ExtractionState) -> dict[str, str]:
        return {
            "discovery": DiscoveryState.FOUND.value,
            "opening": OpeningState.OPENED.value,
            "access": AccessState.ACCESSIBLE.value,
            "extraction": extraction.value,
        }

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
            "collection": {
                "document_id": "4455667",
                "candidate_id": "candidate-admin",
                "found": True,
                "acquisition_state": AdministrativoProvenanceTests._successful_state(ExtractionState.EXTRACTED),
            },
        }


if __name__ == "__main__":
    unittest.main()
