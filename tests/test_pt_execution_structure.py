from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.pt_normalizer import build_normalized_record, build_pt_v2_record


class PTExecutionStructureTests(unittest.TestCase):
    def _build(self, rows: list[list[str]]) -> tuple[dict[str, str], dict[str, object]]:
        payload = {
            "processo": "60090.000001/2026-00",
            "documento": "Plano de Trabalho",
            "snapshot": {
                "title": "Plano de Trabalho",
                "text": "PLANO DE TRABALHO. Periodo de Execucao JAN/2026 a DEZ/2026.",
                "tables": [{"rows": rows}],
                "extraction_mode": "html_dom",
            },
            "analysis": {"validation_status": "valid_for_requested_type"},
        }
        legacy = build_normalized_record(payload, {}, Path("pt.json"))
        return legacy, build_pt_v2_record(legacy, payload, {}, source_path=Path("pt.json"))

    def test_cases_a_to_c_and_i_preserve_row_relationships(self) -> None:
        legacy, v2 = self._build(
            [
                ["Meta", "Acao", "Responsavel", "Periodo", "Produto"],
                ["Meta A", "Acao 1", "Unidade X", "Jan-Mar", "Produto A"],
                ["Meta A", "Acao 2", "Unidade Y", "Abr-Jun", "Produto B"],
                ["Meta B", "Acao 3", "Unidade Z", "Jul-Dez", "Produto C"],
            ]
        )

        entries = v2["execution_entries"]
        self.assertEqual(
            [
                ("Meta A", "Acao 1", "Unidade X", "Jan-Mar", "Produto A"),
                ("Meta A", "Acao 2", "Unidade Y", "Abr-Jun", "Produto B"),
                ("Meta B", "Acao 3", "Unidade Z", "Jul-Dez", "Produto C"),
            ],
            [tuple(entry[name]["value"] for name in ("meta", "acao", "responsavel", "periodo", "produto")) for entry in entries],
        )
        self.assertIn("Meta A", legacy["metas_raw"])
        self.assertIn("Acao 3", legacy["acoes_raw"])

    def test_case_d_repeated_action_remains_distinct_by_row_and_meta(self) -> None:
        _, v2 = self._build(
            [
                ["Meta", "Acao", "Responsavel", "Periodo", "Resultado"],
                ["Meta A", "Monitorar", "Unidade X", "Jan-Mar", "Resultado A"],
                ["Meta B", "Monitorar", "Unidade Y", "Abr-Jun", "Resultado B"],
            ]
        )

        entries = v2["execution_entries"]
        self.assertEqual(2, len(entries))
        self.assertEqual(["Meta A", "Meta B"], [entry["meta"]["value"] for entry in entries])
        self.assertEqual([1, 2], [entry["row_index"] for entry in entries])

    def test_case_e_empty_meta_cell_does_not_fabricate_merged_cell_context(self) -> None:
        _, v2 = self._build(
            [
                ["Meta", "Acao", "Responsavel", "Periodo", "Entrega"],
                ["Meta A", "Acao 1", "Unidade X", "Jan-Mar", "Entrega A"],
                ["", "Acao 2", "Unidade Y", "Abr-Jun", "Entrega B"],
            ]
        )

        entries = v2["execution_entries"]
        self.assertEqual("Meta A", entries[0]["meta"]["value"])
        self.assertIsNone(entries[1]["meta"])
        self.assertEqual("Acao 2", entries[1]["acao"]["value"])
        self.assertEqual("Unidade Y", entries[1]["responsavel"]["value"])

    def test_cases_f_to_h_preserve_partial_rows_without_cross_row_fill(self) -> None:
        _, v2 = self._build(
            [
                ["Meta", "Acao", "Responsavel", "Periodo", "Produto"],
                ["Meta A", "Sem responsavel", "", "Jan-Mar", "Produto A"],
                ["Meta B", "Sem periodo", "Unidade Y", "", "Produto B"],
                ["Meta C", "Sem produto", "Unidade Z", "Jul-Dez", ""],
            ]
        )

        entries = v2["execution_entries"]
        self.assertIsNone(entries[0]["responsavel"])
        self.assertIsNone(entries[1]["periodo"])
        self.assertIsNone(entries[2]["produto"])
        self.assertEqual("Unidade Y", entries[1]["responsavel"]["value"])
        self.assertEqual("Jul-Dez", entries[2]["periodo"]["value"])

    def test_output_is_deterministic_and_evidence_has_real_cell_coordinates(self) -> None:
        rows = [
            ["Meta", "Acao", "Responsavel", "Periodo", "Produto"],
            ["Meta A", "Acao 1", "Unidade X", "Jan-Mar", "Produto A"],
        ]
        _, first = self._build(rows)
        _, second = self._build(rows)

        self.assertEqual(first["execution_entries"], second["execution_entries"])
        action = first["execution_entries"][0]["acao"]
        self.assertEqual("document", action["evidence"]["source_kind"])
        self.assertEqual(
            {"table_index": 0, "row_index": 1, "column_index": 1},
            {key: action["evidence"]["location"][key] for key in ("table_index", "row_index", "column_index")},
        )

    def test_unstructured_text_does_not_fabricate_table_relationships(self) -> None:
        payload = {
            "processo": "60090.000001/2026-00",
            "documento": "Plano de Trabalho",
            "snapshot": {
                "text": "Meta 1. Acao: Monitorar. Produto: Relatorio.",
                "tables": [],
                "extraction_mode": "pdf_ocr",
            },
            "analysis": {"validation_status": "valid_for_requested_type"},
        }
        legacy = build_normalized_record(payload, {}, Path("pt.json"))
        v2 = build_pt_v2_record(legacy, payload, {}, source_path=Path("pt.json"))

        self.assertTrue(legacy["metas_raw"])
        self.assertTrue(legacy["acoes_raw"])
        self.assertEqual([], v2["execution_entries"])


if __name__ == "__main__":
    unittest.main()
