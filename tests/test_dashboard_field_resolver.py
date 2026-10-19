from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services.dashboard_field_resolver import (  # noqa: E402
    COMPLEMENT,
    PREVIEW,
    PRIMARY,
    FieldCandidate,
    ResolutionPolicy,
    resolve_field,
)


class DashboardFieldResolverTests(unittest.TestCase):
    def test_resolution_table_is_field_function_and_relation_specific(self) -> None:
        cases = (
            (
                ResolutionPolicy(
                    "numero_acordo",
                    (("act.instrument", PRIMARY), ("act.extract", COMPLEMENT), ("preview", PREVIEW)),
                ),
                "ACT 1/2026",
                True,
            ),
            (
                ResolutionPolicy(
                    "data_publicacao",
                    (("act.extract", COMPLEMENT), ("act.instrument", PRIMARY), ("preview", PREVIEW)),
                ),
                "2026-02-03",
                False,
            ),
            (
                ResolutionPolicy(
                    "vigencia_fim",
                    (("act.addendum", COMPLEMENT), ("act.instrument", PRIMARY), ("pt.work_plan", COMPLEMENT)),
                ),
                "2028-01-31",
                True,
            ),
        )
        candidates = (
            FieldCandidate("numero_acordo", "ACT 1/2026", "act_gold", "act.instrument", PRIMARY, {"document_id": "1"}),
            FieldCandidate("numero_acordo", "Extrato 1/2026", "act_extract", "act.extract", COMPLEMENT, {"document_id": "2"}),
            FieldCandidate("data_publicacao", "2026-02-03", "act_extract", "act.extract", COMPLEMENT, {"document_id": "2"}),
            FieldCandidate("vigencia_fim", "2027-01-31", "act_gold", "act.instrument", PRIMARY, {"document_id": "1"}),
            FieldCandidate("vigencia_fim", "2028-01-31", "act_addendum", "act.addendum", COMPLEMENT, {"document_id": "3"}),
            FieldCandidate("vigencia_fim", "2026-01-31", "pt_gold", "pt.work_plan", COMPLEMENT, {"document_id": "4"}),
            FieldCandidate("numero_acordo", "Prévia", "preview_fallback", "preview", PREVIEW),
        )

        for policy, expected, has_conflict in cases:
            with self.subTest(field=policy.field_name):
                resolution = resolve_field(policy, candidates)
                self.assertEqual(resolution.value, expected)
                self.assertTrue(resolution.rule_id.endswith(f".{policy.field_name}.v1"))
                self.assertEqual(bool(resolution.conflicts), has_conflict)

    def test_external_or_undeclared_relation_is_not_eligible(self) -> None:
        policy = ResolutionPolicy("objeto", (("act.instrument", PRIMARY), ("pt.work_plan", COMPLEMENT)))
        resolution = resolve_field(
            policy,
            (
                FieldCandidate("objeto", "Objeto externo", "external", "act.instrument", "external_reference"),
                FieldCandidate("objeto", "Objeto PT", "pt_gold", "pt.work_plan", COMPLEMENT),
            ),
        )
        self.assertEqual(resolution.value, "Objeto PT")
        self.assertEqual(resolution.winner.source, "pt_gold")
        self.assertFalse(resolution.conflicts)

    def test_equal_values_keep_all_evidence_without_false_conflict(self) -> None:
        policy = ResolutionPolicy("parceiro", (("act.instrument", PRIMARY), ("pt.work_plan", COMPLEMENT)))
        resolution = resolve_field(
            policy,
            (
                FieldCandidate("parceiro", "Órgão Parceiro", "act_gold", "act.instrument", PRIMARY, {"document_id": "1"}),
                FieldCandidate("parceiro", "  órgão parceiro ", "pt_gold", "pt.work_plan", COMPLEMENT, {"document_id": "2"}),
            ),
        )
        self.assertEqual(resolution.state, "PRESENT")
        self.assertEqual(len(resolution.considered), 2)
        self.assertFalse(resolution.conflicts)


if __name__ == "__main__":
    unittest.main()
