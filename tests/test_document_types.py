from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.registry import resolve_document_types
from app.documents.act import build_act_document_type


class DocumentTypesTests(unittest.TestCase):
    def test_default_document_type_is_pt(self) -> None:
        specs = resolve_document_types(None)
        self.assertEqual([spec.key for spec in specs], ["pt"])
        self.assertEqual(specs[0].snapshot_prefix, "plano_trabalho")

    def test_act_document_type_is_available(self) -> None:
        specs = resolve_document_types("act")
        self.assertEqual([spec.key for spec in specs], ["act"])
        self.assertEqual(specs[0].snapshot_prefix, "acordo_cooperacao_tecnica")

    def test_act_document_type_includes_memorando_e_ted_search_terms(self) -> None:
        spec = build_act_document_type()
        self.assertIn("Memorando de Entendimentos", spec.search_terms)
        self.assertIn("TED -Termo de Execução Descentralizada", spec.search_terms)
        self.assertIn("memorando de entendimentos", spec.tree_match_terms)
        self.assertIn("termo de execução descentralizada", spec.tree_match_terms)

    def test_unknown_document_type_falls_back_to_pt(self) -> None:
        specs = resolve_document_types("foo")
        self.assertEqual([spec.key for spec in specs], ["pt"])


if __name__ == "__main__":
    unittest.main()
