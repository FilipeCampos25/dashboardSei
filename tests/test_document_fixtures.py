from __future__ import annotations

import json
import os
import socket
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.fixture_loader import (
    EXTRACTION_METHODS,
    FAMILIES,
    FIXTURE_ROOT,
    REQUIRED_METADATA,
    TECHNICAL_STATES,
    load_all_fixtures,
    load_fixture,
    load_manifest,
    validate_fixture,
)


class DocumentFixtureTests(unittest.TestCase):
    def test_all_registered_fixtures_load_and_ids_match_manifest(self) -> None:
        manifest = load_manifest()
        fixtures = load_all_fixtures()
        self.assertEqual(len(fixtures), len(manifest["fixtures"]))
        self.assertEqual(
            [fixture["metadata"]["id"] for fixture in fixtures],
            [entry["id"] for entry in manifest["fixtures"]],
        )

    def test_manifest_rejects_duplicate_ids_and_paths(self) -> None:
        invalid_manifest = {
            "schema_version": "1.0",
            "fixtures": [
                {"id": "duplicate", "path": "first.json"},
                {"id": "duplicate", "path": "first.json"},
            ],
        }
        with patch.object(
            Path,
            "read_text",
            return_value=json.dumps(invalid_manifest),
        ), self.assertRaises(ValueError):
            load_manifest()

    def test_loader_rejects_manifest_id_mismatch(self) -> None:
        fixture = load_fixture("pt_html_extracted.json")
        fixture["metadata"]["id"] = "different_id"
        with patch("tests.fixture_loader.load_fixture", return_value=fixture), self.assertRaises(
            ValueError
        ):
            load_all_fixtures()

    def test_loader_rejects_missing_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_fixture("missing_fixture.json")

    def test_loader_is_independent_of_working_directory(self) -> None:
        original = Path.cwd()
        try:
            os.chdir(FIXTURE_ROOT.parent)
            fixture = load_fixture("pt_html_extracted.json")
        finally:
            os.chdir(original)
        self.assertEqual(fixture["metadata"]["id"], "pt_html_extracted")

    def test_manifest_paths_are_relative_and_do_not_reference_output(self) -> None:
        for entry in load_manifest()["fixtures"]:
            path = Path(entry["path"])
            self.assertFalse(path.is_absolute())
            self.assertNotIn("..", path.parts)
            self.assertNotIn("backend/output", entry["path"].replace("\\", "/").lower())

    def test_minimum_metadata_and_coverage_are_present(self) -> None:
        fixtures = load_all_fixtures()
        for fixture in fixtures:
            self.assertTrue(REQUIRED_METADATA <= fixture["metadata"].keys())
        self.assertEqual({fixture["metadata"]["family"] for fixture in fixtures}, FAMILIES)
        self.assertEqual(
            {fixture["metadata"]["extraction_method"] for fixture in fixtures},
            EXTRACTION_METHODS,
        )
        states = {fixture["metadata"]["technical_state"] for fixture in fixtures}
        self.assertEqual(states, TECHNICAL_STATES)

    def test_hygiene_rejects_sensitive_keys_values_and_environment_paths(self) -> None:
        base = {
            "metadata": {
                "id": "invalid",
                "description": "negative validation case",
                "family": "pt",
                "document_role": "candidate",
                "extraction_method": "none",
                "technical_state": "EMPTY_CONTENT",
                "origin": "synthetic",
            },
            "payload": {},
        }
        invalid_payloads = (
            {**base, "payload": {"token": "placeholder"}},
            {**base, "payload": {"header": "Bearer abcdefghijklmnop"}},
            {**base, "payload": {"path": "C:/Users/example/private.json"}},
            {**base, "payload": {"path": "/home/example/private.json"}},
            {**base, "payload": {"path": "backend/output/snapshot.json"}},
        )
        for fixture in invalid_payloads:
            with self.subTest(payload=fixture["payload"]):
                with self.assertRaises(ValueError):
                    validate_fixture(fixture)

    def test_loading_under_offline_only_performs_no_network_or_selenium_work(self) -> None:
        with patch.dict(os.environ, {"OFFLINE_ONLY": "true"}), patch.object(
            socket, "create_connection", side_effect=AssertionError("network attempted")
        ):
            fixtures = load_all_fixtures()
        self.assertTrue(fixtures)


if __name__ == "__main__":
    unittest.main()
