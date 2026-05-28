from __future__ import annotations

import csv
import sys
import unittest
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

    def test_build_normalized_record_infers_encerrado_from_termo(self) -> None:
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

        self.assertEqual(record["status_raw"], "Encerrado")
        self.assertEqual(record["status_normalizado"], "Encerrado")
        self.assertEqual(record["status_categoria"], "encerrado")
        self.assertEqual(record["numero_termo_encerramento"], "3/2025")

    def test_build_normalized_record_handles_status_variants(self) -> None:
        cases = {
            "STATUS Encerrado": ("Encerrado", "encerrado"),
            "STATUS: Vigente.": ("Vigente", "vigente_em_descontinuadas"),
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
        self.assertEqual(record["status_categoria"], "sem_status")
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


if __name__ == "__main__":
    unittest.main()
