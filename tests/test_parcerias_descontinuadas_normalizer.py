from __future__ import annotations

import csv
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.parcerias_descontinuadas_normalizer import (  # noqa: E402
    NORMALIZED_COLUMNS,
    build_normalized_record,
    export_normalized_csv,
)


class ParceriasDescontinuadasNormalizerTests(unittest.TestCase):
    def test_build_normalized_record_extracts_complete_annotation(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60093.000214/2016-91",
                "anotacoes": (
                    "TIPO: ACORDO DE COOPERACAO TECNICA\n"
                    "NUMERO ACT: 04/2020\n"
                    "PARCEIRO: Rede Nacional de Ensino e Pesquisa\n"
                    "OBJETO: Estabelecer as diretrizes\n"
                    "de cooperacao tecnica.\n"
                    "GESTOR TITULAR: Fulano\n"
                    "GESTOR SUBSTITUTO: Beltrano\n"
                    "PORTARIA DE DESIGNACAO: Portaria 123\n"
                    "DATA DE ASSINATURA: 01/02/2020\n"
                    "DATA DE VENCIMENTO: 01/02/2025\n"
                    "STATUS: Nao Realizado"
                ),
            }
        )

        self.assertEqual(record["processo"], "60093.000214/2016-91")
        self.assertEqual(record["tipo"], "ACORDO DE COOPERACAO TECNICA")
        self.assertEqual(record["numero_act"], "04/2020")
        self.assertEqual(record["parceiro"], "Rede Nacional de Ensino e Pesquisa")
        self.assertEqual(record["objeto"], "Estabelecer as diretrizes de cooperacao tecnica.")
        self.assertEqual(record["gestor_titular"], "Fulano")
        self.assertEqual(record["gestor_substituto"], "Beltrano")
        self.assertEqual(record["portaria_designacao"], "Portaria 123")
        self.assertEqual(record["data_assinatura"], "01/02/2020")
        self.assertEqual(record["data_vencimento"], "01/02/2025")
        self.assertEqual(record["status_normalizado"], "Nao Realizado")
        self.assertEqual(record["status_categoria"], "nao_realizado")
        self.assertEqual(record["normalization_status"], "completo")

    def test_build_normalized_record_keeps_singular_partner_unchanged(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60090.000146/2020-77",
                "anotacoes": (
                    "TIPO: ACT\n"
                    "PARCEIRO: Instituto de Hidrologia, Meteorologia e Estudos Ambientais da Colombia (IDEAM)\n"
                    "OBJETO: Troca de informacoes.\n"
                    "STATUS: Nao Realizado"
                ),
            }
        )

        self.assertEqual(
            record["parceiro"],
            "Instituto de Hidrologia, Meteorologia e Estudos Ambientais da Colombia (IDEAM)",
        )

    def test_build_normalized_record_extracts_plural_partner_with_spacing(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60090.000263/2024-64",
                "anotacoes": (
                    "TIPO: Protocolo de Intencoes.\n"
                    "PARCEIROS :   HEX INFORMATICA LTDA (HEX360).\n"
                    "OBJETO: Elaboracao de uma proposta de projeto.\n"
                    "STATUS: Vigente."
                ),
            }
        )

        self.assertEqual(record["parceiro"], "HEX INFORMATICA LTDA (HEX360).")

    def test_build_normalized_record_preserves_multiline_partner_list_and_stops_at_next_label(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60090.000881/2021-61",
                "anotacoes": (
                    "TIPO: Memorando de Entendimentos.\n"
                    "PARCEIROS: Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renovaveis IBAMA, "
                    "Instituto Chico Mendes de Conservacao da Biodiversidade ICMBio, Servico Florestal Brasileiro SFB,\n"
                    "Instituto Nacional de Colonizacao e Reforma Agraria - INCRA, Instituto Nacional de\n"
                    "Pesquisas Espaciais INPE, Agencia Nacional de Mineracao ANM, Policia Rodoviaria\n"
                    "Federal PRF, Policia Federal PF, Fundacao Nacional do Indio Funai, Agencia Brasileira\n"
                    "de Inteligencia ABIN, Secretaria Especial da Receita Federal do Brasil SRF.\n"
                    "OBJETO: Uniao de esforcos para apoiar as atividades do GIPAM.\n"
                    "STATUS: Encerrado."
                ),
            }
        )

        expected = (
            "Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renovaveis IBAMA, "
            "Instituto Chico Mendes de Conservacao da Biodiversidade ICMBio, Servico Florestal Brasileiro SFB, "
            "Instituto Nacional de Colonizacao e Reforma Agraria - INCRA, Instituto Nacional de "
            "Pesquisas Espaciais INPE, Agencia Nacional de Mineracao ANM, Policia Rodoviaria "
            "Federal PRF, Policia Federal PF, Fundacao Nacional do Indio Funai, Agencia Brasileira "
            "de Inteligencia ABIN, Secretaria Especial da Receita Federal do Brasil SRF."
        )
        self.assertEqual(record["parceiro"], expected)
        self.assertNotIn("OBJETO", record["parceiro"])
        self.assertEqual(record["objeto"], "Uniao de esforcos para apoiar as atividades do GIPAM.")
        self.assertIn("PARCEIROS:", record["raw_anotacoes"])

    def test_build_normalized_record_keeps_raw_empty_and_calculates_encerrado_from_identified_termo(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60092.000040/2020-53",
                "anotacoes": (
                    "TIPO: ACORDO DE COOPERACAO TECNICA\n"
                    "PARCEIRO: SEMA AMAPA\n"
                    "OBJETO: Intercambio de informacoes.\n"
                    "TERMO DE ENCERRAMENTO Nº 3/2025 (7843998), PUBLICADO NO DOU EM 03/04/25"
                ),
            }
        )

        self.assertEqual(record["status_raw"], "")
        self.assertEqual(record["status_normalizado"], "")
        self.assertEqual(record["status_calculado"], "Encerrado")
        self.assertEqual(record["status_categoria"], "encerrado")
        self.assertEqual(record["status_evidencia"], "termo_encerramento_identificado")
        self.assertEqual(record["numero_termo_encerramento"], "3/2025")

    def test_build_normalized_record_handles_status_variants(self) -> None:
        cases = {
            "STATUS Encerrado": ("Encerrado", "indeterminado"),
            "STATUS: Vigente.": ("Vigente", "indeterminado"),
            "STATUS: NAO FORMALIZADO": ("Nao Realizado", "nao_realizado"),
            "STATUS: NAO ASSINADO PELO PARCEIRO": ("Nao Realizado", "nao_realizado"),
        }

        for status_line, expected in cases.items():
            with self.subTest(status_line=status_line):
                record = build_normalized_record(
                    {
                        "processo": "60090.000000/2020-00",
                        "anotacoes": (
                            "TIPO: ACT\n"
                            "PARCEIRO: Parceiro\n"
                            "OBJETO: Objeto suficientemente descritivo.\n"
                            f"{status_line}"
                        ),
                    }
                )
                self.assertEqual((record["status_normalizado"], record["status_categoria"]), expected)

    def test_build_normalized_record_keeps_unstructured_rows(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60090.000058/2022-37",
                "anotacoes": "Organizacional: Reunioes.Audiencias. Despachos.",
            }
        )

        self.assertEqual(record["normalization_status"], "sem_campos_estruturados")
        self.assertEqual(record["status_categoria"], "indeterminado")
        self.assertIn("tipo", record["missing_fields"])
        self.assertEqual(record["raw_anotacoes"], "Organizacional: Reunioes.Audiencias. Despachos.")

    def test_export_normalized_csv_writes_fixed_columns(self) -> None:
        output_dir = Path.cwd() / "output" / "test_parcerias_descontinuadas_normalizer"
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "parcerias_descontinuadas_normalizado_latest.csv"
        if csv_path.exists():
            csv_path.unlink()

        try:
            result = export_normalized_csv(
                output_dir,
                records=[
                    {
                        "processo": "60090.000146/2020-77",
                        "anotacoes": "TIPO: ACT\nPARCEIRO: IDEAM\nOBJETO: Troca de informacoes.\nSTATUS: Nao Realizado",
                    }
                ],
            )

            self.assertEqual(result["records"], 1)
            self.assertEqual(result["latest_path"], csv_path)
            with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                rows = list(csv.DictReader(file_obj))
        finally:
            if csv_path.exists():
                csv_path.unlink()

        self.assertEqual(list(rows[0].keys()), NORMALIZED_COLUMNS)
        self.assertEqual(rows[0]["processo"], "60090.000146/2020-77")
        self.assertEqual(rows[0]["status_categoria"], "nao_realizado")

    def test_status_precedence_and_reference_date_are_deterministic(self) -> None:
        reference = date(2026, 1, 1)
        cases = [
            ("31/12/2025", "", "Vencido", "vencido"),
            ("01/01/2026", "", "Vigente", "vigente"),
            ("02/01/2026", "", "Vigente", "vigente"),
            ("invalida", "", "Indeterminado", "indeterminado"),
            ("", "", "Indeterminado", "indeterminado"),
            ("31/12/2025", "nº 7/2025 (8210725)", "Encerrado", "encerrado"),
        ]
        for end_date, term, calculated, category in cases:
            annotation = "TIPO: ACT\nPARCEIRO: Parceiro\nOBJETO: Objeto\nSTATUS: Vigente"
            if end_date:
                annotation += f"\nDATA DE VENCIMENTO: {end_date}"
            if term:
                annotation += f"\nTERMO DE ENCERRAMENTO: {term}"
            with self.subTest(end_date=end_date, term=term):
                record = build_normalized_record(
                    {"processo": "60090.000000/2020-00", "anotacoes": annotation},
                    reference_date=reference,
                )
                self.assertEqual(record["status_calculado"], calculated)
                self.assertEqual(record["status_categoria"], category)
                self.assertEqual(record["status_data_referencia"], "2026-01-01")

    def test_free_encerramento_text_and_unidentified_term_are_insufficient(self) -> None:
        for annotation in (
            "TIPO: ACT\nPARCEIRO: P\nOBJETO: apos o encerramento\nSTATUS: Vigente",
            "TIPO: ACT\nPARCEIRO: P\nOBJETO: O\nTERMO DE ENCERRAMENTO: pendente",
        ):
            record = build_normalized_record(
                {"processo": "60090.000000/2020-00", "anotacoes": annotation},
                reference_date=date(2026, 1, 1),
            )
            self.assertEqual(record["status_calculado"], "Indeterminado")
            self.assertNotIn("termo_encerramento_identificado", record["status_evidencia"])

    def test_reference_process_extracts_vencimento_alias_and_keeps_raw_punctuation(self) -> None:
        record = build_normalized_record(
            {
                "processo": "60090.000263/2024-64",
                "anotacoes": (
                    "TIPO: Protocolo de Intenções.\nPARCEIROS: HEX INFORMÁTICA LTDA (HEX360).\n"
                    "OBJETO: Elaboração de uma proposta de projeto.\nSTATUS: Vigente.\n"
                    "VENCIMENTO: 27/08/2025\nTermo de Encerramento nº 6/2025 (8210725)"
                ),
            },
            reference_date=date(2026, 1, 1),
        )
        self.assertEqual(record["status_raw"], "Vigente.")
        self.assertEqual(record["data_vencimento"], "27/08/2025")
        self.assertEqual(record["status_calculado"], "Encerrado")
        self.assertEqual(
            record["status_evidencia"],
            "termo_encerramento_identificado;data_final_anterior_referencia;status_raw_vigente",
        )


if __name__ == "__main__":
    unittest.main()
