from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.documents.types import DocumentTypeSpec  # noqa: E402
from app.rpa import scraping  # noqa: E402
from app.services.candidate_inventory import prioritize_candidates  # noqa: E402


SPEC = DocumentTypeSpec(
    key="act",
    display_name="ACT",
    search_terms=("ACT",),
    tree_match_terms=("ACT",),
    snapshot_prefix="act",
    log_label="ACT",
    cleanup_patterns=(),
    handler=Mock(),
)


class CandidateInventoryTests(unittest.TestCase):
    def test_selective_plan_prioritizes_opening_and_preserves_complements(self) -> None:
        inventory = [
            {"text": "Publicacao do ACT 1/2026", "score": 90},
            {"text": "ACT 1/2026", "score": 40},
            {"text": "Anexo do ACT 1/2026", "score": 80},
        ]

        plan = prioritize_candidates(inventory)

        self.assertEqual([item["text"] for item in plan], [
            "ACT 1/2026",
            "Publicacao do ACT 1/2026",
            "Anexo do ACT 1/2026",
        ])
        self.assertEqual(plan[0]["extraction_decision"], "open_and_extract")
        self.assertEqual(plan[0]["extraction_priority"], 1)
        self.assertTrue(all(item["extraction_decision"] == "retain_without_extraction" for item in plan[1:]))
        self.assertTrue(all(item["extraction_priority"] is None for item in plan[1:]))

    def _scraper_for_scoring(self):
        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)
        return scraper

    def test_tree_ranking_penalties_ignore_case_and_accents(self) -> None:
        scraper = self._scraper_for_scoring()
        titles = (
            "ACT 1/2026",
            "aNeXo do act 1/2026",
            "E-MAIL sobre o act 1/2026",
            "PUBLICAÇÃO do Act 1/2026",
        )

        scores = [scraping.SEIScraper._score_tree_candidate(scraper, SPEC, title)[0] for title in titles]

        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertGreater(scores[0], scores[1])
        self.assertGreater(scores[1], scores[2])
        self.assertGreater(scores[2], scores[3])

    def test_tree_inventory_retains_instrument_and_related_documents(self) -> None:
        titles = (
            "Acordo de Cooperacao Tecnica ACT 1/2026",
            "Extrato do ACT 1/2026",
            "Publicacao do ACT 1/2026",
            "Termo Aditivo do ACT 1/2026",
            "Anexo do ACT 1/2026",
        )
        elements = []
        for index, title in enumerate(titles, start=1):
            element = Mock(text=title)
            element.get_attribute.return_value = f"https://sei.example/?id_documento={index}"
            elements.append(element)

        scraper = scraping.SEIScraper.__new__(scraping.SEIScraper)
        scraper.performance_profiler = Mock()
        scraper.logger = Mock()
        scraper.selectors = Mock()
        scraper.selectors.get_many.return_value = ["//a"]
        scraper.driver = Mock()
        scraper.driver.find_element.return_value = Mock()
        scraper.driver.find_elements.return_value = elements
        scraper._normalize_text = scraping.SEIScraper._normalize_text.__get__(scraper, scraping.SEIScraper)

        inventory = scraping.SEIScraper._find_document_candidates_in_tree(scraper, SPEC)

        self.assertEqual({item["text"] for item in inventory}, set(titles))
        relations = {item["probable_relation"] for item in inventory}
        self.assertEqual(relations, {"canonical_candidate", "extract", "publication", "amendment", "attachment"})
        related = [item for item in inventory if item["probable_relation"] != "canonical_candidate"]
        self.assertTrue(all(item["inventory_disposition"] == "related_not_opened" for item in related))
        self.assertTrue(all(item["relation_reason"].startswith("title_marker:") for item in related))


if __name__ == "__main__":
    unittest.main()
