from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import SourceKind
from app.services.provenance_validation import ProvenanceViolationCode, validate_field_provenance
from app.services.pt_normalizer import build_normalized_record, build_pt_v2_record, export_normalized_csv


class PTProvenanceTests(unittest.TestCase):
    def test_document_partner_wins_over_divergent_preview(self) -> None:
        payload = self._payload()
        preview = {"parceiro": "Parceiro da preview"}
        legacy = build_normalized_record(payload, preview, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, preview))

        self.assertEqual("Orgao documental", legacy["parceiro"])
        self.assertIs(fields["parceiro"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertIs(fields["parceiro"].evidences[1].source_kind, SourceKind.PREVIEW)
        self.assertEqual("Parceiro da preview", fields["parceiro"].evidences[1].raw_evidence)

    def test_document_object_wins_over_divergent_preview(self) -> None:
        payload = self._payload()
        preview = {"objeto": "Objeto resolvido pela preview"}
        legacy = build_normalized_record(payload, preview, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, preview))

        self.assertEqual("objeto obtido do documento.", legacy["objeto"])
        self.assertIs(fields["objeto"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertIs(fields["objeto"].evidences[1].source_kind, SourceKind.PREVIEW)
        self.assertEqual("Objeto resolvido pela preview", fields["objeto"].evidences[1].raw_evidence)

    def test_preview_only_partner_remains_enrichment_but_not_document_complete(self) -> None:
        payload = self._payload()
        payload["snapshot"]["text"] = payload["snapshot"]["text"].replace(
            "Participe 2: Orgao documental CNPJ 00.000.000/0001-00.",
            "",
        )
        preview = {"parceiro": "Parceiro da preview"}
        legacy = build_normalized_record(payload, preview, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, preview))

        self.assertEqual("Parceiro da preview", legacy["parceiro"])
        self.assertIs(fields["parceiro"].evidences[0].source_kind, SourceKind.PREVIEW)
        self.assertNotEqual("completo_padronizado", legacy["normalization_status"])
        self.assertEqual("retained_silver", legacy["publication_status"])

    def test_preview_only_object_remains_enrichment_but_not_document_complete(self) -> None:
        payload = self._payload()
        payload["snapshot"]["text"] = payload["snapshot"]["text"].replace(
            "IDENTIFICACAO DO OBJETO Objeto obtido do documento.",
            "",
        )
        preview = {"objeto": "Objeto resolvido pela preview"}
        legacy = build_normalized_record(payload, preview, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, preview))

        self.assertEqual("Objeto resolvido pela preview", legacy["objeto"])
        self.assertIs(fields["objeto"].evidences[0].source_kind, SourceKind.PREVIEW)
        self.assertNotEqual("completo_padronizado", legacy["normalization_status"])
        self.assertEqual("retained_silver", legacy["publication_status"])

    def test_document_fields_without_preview_remain_complete(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, {}, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, {}))

        self.assertIs(fields["parceiro"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertIs(fields["objeto"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertEqual("completo_padronizado", legacy["normalization_status"])

    def test_equal_document_and_preview_values_keep_document_origin(self) -> None:
        payload = self._payload()
        preview = {
            "parceiro": "Orgao documental",
            "objeto": "objeto obtido do documento.",
        }
        legacy = build_normalized_record(payload, preview, Path("plano_trabalho_fixture.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, preview))

        self.assertIs(fields["parceiro"].evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertIs(fields["objeto"].evidences[0].source_kind, SourceKind.DOCUMENT)

    def test_preview_only_cannot_promote_incomplete_document_to_complete(self) -> None:
        payload = self._payload()
        payload["snapshot"]["text"] = payload["snapshot"]["text"].replace(
            "IDENTIFICACAO DO OBJETO Objeto obtido do documento.",
            "",
        )
        legacy = build_normalized_record(
            payload,
            {"objeto": "Objeto resolvido pela preview"},
            Path("plano_trabalho_fixture.json"),
        )

        self.assertNotEqual("completo_padronizado", legacy["normalization_status"])
        self.assertEqual("retained_silver", legacy["publication_status"])

    def test_documentally_complete_pt_is_not_downgraded_by_preview_policy(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, {}, Path("plano_trabalho_fixture.json"))

        self.assertEqual("completo_padronizado", legacy["normalization_status"])
        self.assertEqual("published_gold", legacy["publication_status"])

    def test_document_and_derived_fields_are_auditable_without_fabricated_identity(self) -> None:
        payload = self._payload()
        legacy = build_normalized_record(payload, {}, Path("plano_trabalho_fixture.json"))
        fields = self._fields(
            build_pt_v2_record(legacy, payload, {}, source_path=Path("candidates/pt-source.json"))
        )

        objeto = fields["objeto"]
        self.assertIs(objeto.state, FieldState.PRESENT)
        self.assertIs(objeto.evidences[0].source_kind, SourceKind.DOCUMENT)
        self.assertEqual("4433322", objeto.evidences[0].source_document.document_id)
        self.assertEqual("candidate-pt-7", objeto.evidences[0].source_document.candidate_id)
        self.assertEqual(str(Path("candidates/pt-source.json")), objeto.evidences[0].location.source_path)

        fim = fields["vigencia_fim"]
        self.assertEqual(legacy["vigencia_fim"], fim.value)
        self.assertIs(fim.evidences[0].source_kind, SourceKind.DERIVED)
        self.assertEqual("pt.vigencia.derived_from_signature", fim.evidences[0].rule_id)
        self.assertTrue(all(validate_field_provenance(field).is_valid for field in fields.values()))

        payload["collection"] = {"document_id": payload["processo"]}
        payload["snapshot"].pop("url")
        legacy_without_identity = build_normalized_record(payload, {}, Path("pt.json"))
        evidence = self._fields(build_pt_v2_record(legacy_without_identity, payload, {}))["objeto"].evidences[0]
        self.assertIsNone(evidence.source_document.document_id)
        self.assertNotEqual(evidence.source_document.process_id, evidence.source_document.document_id)

    def test_absent_field_has_no_placeholder_or_fabricated_evidence(self) -> None:
        payload = self._payload()
        payload["snapshot"]["text"] = "PLANO DE TRABALHO entre CENSIPAM e Orgao documental."
        legacy = build_normalized_record(payload, {}, Path("pt.json"))
        fields = self._fields(build_pt_v2_record(legacy, payload, {}))

        self.assertIs(fields["metas_raw"].state, FieldState.NOT_EVALUATED)
        self.assertIsNone(fields["metas_raw"].value)
        self.assertEqual((), fields["metas_raw"].evidences)

    def test_present_without_evidence_remains_rejected(self) -> None:
        report = validate_field_provenance(FieldResult("objeto", FieldState.PRESENT, "valor"))
        self.assertEqual((ProvenanceViolationCode.MISSING_EVIDENCE,), report.codes)

    def test_dual_write_is_opt_in_and_csv_is_stable_between_modes(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            fixture = json.dumps(self._payload(), ensure_ascii=False)
            preview = (
                "processo,parceiro,objeto,vigencia\n"
                "60090.000269/2020-16,Parceiro da preview,Objeto resolvido pela preview,60 meses\n"
            )
            (root / "plano_trabalho_fixture.json").write_text(fixture, encoding="utf-8")
            (root / "parcerias_vigentes_latest.csv").write_text(preview, encoding="utf-8")

            with patch("app.services.pt_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=False)):
                off_result = export_normalized_csv(root)
            legacy_off = off_result["csv_path"].read_bytes()
            with patch("app.services.pt_normalizer.get_settings", return_value=SimpleNamespace(v2_dual_write=True)):
                on_result = export_normalized_csv(root)

            self.assertEqual(legacy_off, on_result["csv_path"].read_bytes())
            self.assertIsNone(off_result["v2_path"])
            sidecar = json.loads(on_result["v2_path"].read_text(encoding="utf-8"))
            fields = {item["field_name"]: item for item in sidecar["records"][0]["fields"]}
            self.assertEqual("document", fields["parceiro"]["evidences"][0]["source_kind"])
            self.assertEqual("document", fields["objeto"]["evidences"][0]["source_kind"])

    @staticmethod
    def _fields(v2: dict[str, object]) -> dict[str, FieldResult]:
        return {item["field_name"]: FieldResult.from_dict(item) for item in v2["fields"]}

    @staticmethod
    def _payload() -> dict[str, object]:
        return {
            "processo": "60090.000269/2020-16",
            "documento": "PT 2",
            "requested_type": "pt",
            "snapshot": {
                "url": "https://sei.example/controlador.php?acao=documento&id_documento=4433322",
                "extraction_mode": "html_dom",
                "text": """
                    PLANO DE TRABALHO. Participe 2: Orgao documental CNPJ 00.000.000/0001-00.
                    IDENTIFICACAO DO OBJETO Objeto obtido do documento.
                    Meta 1 - Entrega documental. Acao 1 - Executar atividade.
                    Inicio: imediatamente apos a assinatura.
                    Termino: cinco anos apos a assinatura.
                    Documento assinado eletronicamente por Pessoa Um, em 14/12/2021.
                """,
            },
            "collection": {
                "document_id": "4433322",
                "candidate_id": "candidate-pt-7",
                "source_url": "https://sei.example/controlador.php?acao=documento&id_documento=4433322",
            },
            "prazos": {
                "inicio_raw": "imediatamente apos a assinatura",
                "termino_raw": "cinco anos apos a assinatura",
            },
        }


if __name__ == "__main__":
    unittest.main()
