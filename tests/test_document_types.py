from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.documents.act import build_act_document_type
from app.documents.memorando import build_memorando_document_type
from app.documents.registry import resolve_document_types
from app.documents.ted import build_ted_document_type


class DocumentTypesTests(unittest.TestCase):
    def test_default_document_type_is_pt(self) -> None:
        specs = resolve_document_types(None)
        self.assertEqual([spec.key for spec in specs], ["pt"])
        self.assertEqual(specs[0].snapshot_prefix, "plano_trabalho")

    def test_act_document_type_is_available(self) -> None:
        specs = resolve_document_types("act")
        self.assertEqual([spec.key for spec in specs], ["act", "memorando", "ted"])
        self.assertEqual(specs[0].snapshot_prefix, "acordo_cooperacao_tecnica")

    def test_cooperation_document_types_are_separated(self) -> None:
        spec = build_act_document_type()
        self.assertNotIn("Memorando de Entendimentos", spec.search_terms)
        self.assertNotIn("TED -Termo de Execução Descentralizada", spec.search_terms)
        self.assertNotIn("memorando de entendimentos", spec.tree_match_terms)
        self.assertNotIn("termo de execução descentralizada", spec.tree_match_terms)

        memorando = build_memorando_document_type()
        self.assertIn("Memorando de Entendimentos", memorando.search_terms)
        self.assertEqual(memorando.snapshot_prefix, "memorando_entendimentos")

        ted = build_ted_document_type()
        self.assertIn("TED -Termo de Execução Descentralizada", ted.search_terms)
        self.assertEqual(ted.snapshot_prefix, "termo_execucao_descentralizada")

    def test_unknown_document_type_falls_back_to_pt(self) -> None:
        specs = resolve_document_types("foo")
        self.assertEqual([spec.key for spec in specs], ["pt"])


if __name__ == "__main__":
    unittest.main()
