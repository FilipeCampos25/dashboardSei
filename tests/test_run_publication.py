from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.output.run_publication import RunPublication  # noqa: E402
from app.rpa.scraping import SEIScraper  # noqa: E402


class RunPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = Path(__file__).resolve().parent / "_tmp_run_publication"
        self.addCleanup(self._cleanup)
        self._cleanup()
        self.test_root.mkdir()
        self.published = self.test_root / "published"

    def _cleanup(self) -> None:
        if self.test_root.exists():
            import shutil

            shutil.rmtree(self.test_root)

    def test_success_promotes_all_artifacts_and_records_complete_run(self) -> None:
        publication = RunPublication(self.published)
        staging = publication.begin()
        (staging / "first_latest.csv").write_bytes(b"first-new")
        (staging / "second_latest.json").write_bytes(b"second-new")

        publication.publish()

        self.assertEqual((self.published / "first_latest.csv").read_bytes(), b"first-new")
        self.assertEqual((self.published / "second_latest.json").read_bytes(), b"second-new")
        manifest = json.loads((self.published / "run_manifest_latest.json").read_text(encoding="utf-8"))
        attempt = json.loads(publication.attempt_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "complete")
        self.assertTrue(manifest["published"])
        self.assertEqual(attempt["status"], "complete")
        self.assertFalse(publication.staging_dir.exists())

    def test_intermediate_failure_preserves_previous_outputs_byte_for_byte(self) -> None:
        self.published.mkdir()
        previous = {
            "first_latest.csv": b"first-old\x00",
            "second_latest.json": b'{"old": true}\n',
        }
        for name, content in previous.items():
            (self.published / name).write_bytes(content)
        publication = RunPublication(self.published)
        staging = publication.begin()
        (staging / "first_latest.csv").write_bytes(b"first-new")

        error = ValueError("exporter B failed")
        publication.abort(error)

        self.assertEqual(
            {path.name: path.read_bytes() for path in self.published.iterdir()},
            previous,
        )
        attempt = json.loads(publication.attempt_path.read_text(encoding="utf-8"))
        self.assertEqual(attempt["status"], "incomplete")
        self.assertFalse(attempt["published"])
        self.assertEqual(attempt["error"], {"type": "ValueError", "message": str(error)})

    def test_failed_first_run_creates_no_published_round(self) -> None:
        publication = RunPublication(self.published)
        staging = publication.begin()
        (staging / "partial_latest.csv").write_text("partial", encoding="utf-8")

        publication.abort(RuntimeError("failure"))

        self.assertFalse(self.published.exists())
        self.assertFalse(publication.staging_dir.exists())
        self.assertEqual(
            json.loads(publication.attempt_path.read_text(encoding="utf-8"))["status"],
            "incomplete",
        )

    def test_paths_do_not_depend_on_current_working_directory_and_work_offline(self) -> None:
        old_cwd = Path.cwd()
        old_offline = os.environ.get("OFFLINE_ONLY")
        os.environ["OFFLINE_ONLY"] = "true"
        elsewhere = self.test_root / "elsewhere"
        elsewhere.mkdir()
        try:
            os.chdir(elsewhere)
            publication = RunPublication(self.published)
            staging = publication.begin()
            (staging / "artifact.csv").write_text("ok", encoding="utf-8")
            publication.publish()
        finally:
            os.chdir(old_cwd)
            if old_offline is None:
                os.environ.pop("OFFLINE_ONLY", None)
            else:
                os.environ["OFFLINE_ONLY"] = old_offline

        self.assertEqual((self.published / "artifact.csv").read_text(encoding="utf-8"), "ok")

    def test_exporter_exception_is_recorded_and_propagated_by_round_coordinator(self) -> None:
        error = LookupError("intermediate exporter failed")
        successful_handler = SimpleNamespace(finalize_run=Mock())
        failing_handler = SimpleNamespace(finalize_run=Mock(side_effect=error))
        specs = [
            SimpleNamespace(handler=successful_handler),
            SimpleNamespace(handler=failing_handler),
        ]
        scraper = SEIScraper.__new__(SEIScraper)
        scraper.settings = SimpleNamespace()
        scraper.logger = Mock()
        scraper._run_publication = RunPublication(self.published)
        scraper._run_publication.begin()
        scraper._get_document_types_for_outputs = Mock(return_value=specs)

        with self.assertRaisesRegex(LookupError, "intermediate exporter failed"):
            scraper._finalize_document_runs()

        successful_handler.finalize_run.assert_called_once()
        failing_handler.finalize_run.assert_called_once()
        attempt = json.loads(scraper._run_publication.attempt_path.read_text(encoding="utf-8"))
        self.assertEqual(attempt["status"], "incomplete")
        self.assertEqual(attempt["error"]["type"], "LookupError")


if __name__ == "__main__":
    unittest.main()
