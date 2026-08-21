from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
FIXTURES = ROOT / "tests" / "fixtures" / "documents"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services import offline_reprocessor as module  # noqa: E402
from app.services.field_states import FieldState  # noqa: E402


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _digest(path)
        for path in sorted(root.rglob("*")) if path.is_file()
    }


class OfflineReprocessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.offline = patch.object(module, "get_settings", return_value=SimpleNamespace(offline_only=True))
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def test_requires_offline_mode(self) -> None:
        with patch.object(module, "get_settings", return_value=SimpleNamespace(offline_only=False)):
            with self.assertRaisesRegex(module.OfflineReprocessorError, "OFFLINE_ONLY=true"):
                module.reprocess_snapshot("missing.json", "derived")

    def test_single_frozen_fixture_produces_parseable_v2_without_mutating_source(self) -> None:
        source = FIXTURES / "act_pdf_extracted.json"
        before = (source.stat().st_size, _digest(source))
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            result = module.reprocess_snapshot(source, Path(temporary) / "v2")
            self.assertEqual(result.status, "processed")
            payload = json.loads(result.output.read_text(encoding="utf-8"))
            self.assertEqual(payload["family"], "act")
            self.assertEqual(payload["record"]["identity"]["process_id"], "PROCESSO-EXEMPLO-002")
            self.assertEqual(payload["source_artifact_ref"], {
                "root_kind": "artifact_root", "relative_path": "act_pdf_extracted.json",
            })
            self.assertEqual(payload["record"]["artifact_ref"], payload["source_artifact_ref"])
            self.assertNotIn(str(FIXTURES), result.output.read_text(encoding="utf-8"))
        self.assertEqual((source.stat().st_size, _digest(source)), before)

    def test_all_supported_families_dispatch_to_offline_builders(self) -> None:
        cases = {
            "act_pdf_extracted.json": "act",
            "pt_html_extracted.json": "pt",
            "ted_normalizer_rich.json": "ted",
            "administrative_docx_extracted.json": "administrative",
        }
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            for filename, family in cases.items():
                with self.subTest(family=family):
                    result = module.reprocess_snapshot(FIXTURES / filename, Path(temporary) / family)
                    self.assertEqual((result.status, result.family), ("processed", family))

    def test_explicit_family_is_supported_but_unknown_family_is_not_guessed(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "plain.json"
            source.write_text(json.dumps({"processo": "P-1", "snapshot": {"text": "TED in title only"}}), encoding="utf-8")
            unresolved = module.reprocess_snapshot(source, root / "unresolved")
            self.assertEqual((unresolved.status, unresolved.stage), ("unresolved", "family"))
            self.assertFalse((root / "unresolved").exists())
            explicit = module.reprocess_snapshot(source, root / "explicit", family="ted")
            self.assertEqual((explicit.status, explicit.family), ("processed", "ted"))

    def test_conflicting_persisted_signals_are_unresolved(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "conflict.json"
            source.write_text(json.dumps({"metadata": {"family": "act"}, "payload": {"requested_type": "pt"}}), encoding="utf-8")
            result = module.reprocess_snapshot(source, root / "v2")
            self.assertEqual((result.status, result.stage), ("unresolved", "family"))

    def test_two_destinations_and_same_destination_are_byte_idempotent(self) -> None:
        source = FIXTURES / "pt_html_extracted.json"
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            first = root / "run1"
            second = root / "run2"
            module.reprocess_snapshot(source, first)
            module.reprocess_snapshot(source, second)
            self.assertEqual(_tree(first), _tree(second))
            before = _tree(first)
            module.reprocess_snapshot(source, first)
            self.assertEqual(_tree(first), before)
            self.assertEqual(len(before), 1)

    def test_directory_order_is_stable_and_preserves_source_tree(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "frozen"
            (source / "z").mkdir(parents=True)
            (source / "a").mkdir()
            for relative, fixture in (
                (Path("z") / "one.json", "ted_normalizer_rich.json"),
                (Path("a") / "two.json", "act_pdf_extracted.json"),
            ):
                (source / relative).write_bytes((FIXTURES / fixture).read_bytes())
            before = _tree(source)
            report1 = module.reprocess_directory(source, root / "run1")
            report2 = module.reprocess_directory(source, root / "run2")
            self.assertEqual(report1.processed, 2)
            self.assertEqual([item.source.relative_to(source).as_posix() for item in report1.results], ["a/two.json", "z/one.json"])
            self.assertEqual(_tree(root / "run1"), _tree(root / "run2"))
            self.assertEqual(_tree(source), before)

    def test_ambiguous_legacy_values_keep_conservative_adapter_states(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "legacy.json"
            source.write_text(json.dumps({
                "metadata": {"family": "administrative"},
                "payload": {"processo": "P-1", "documento": "P-1", "found": True, "snapshot": {}},
            }), encoding="utf-8")
            result = module.reprocess_snapshot(source, root / "v2")
            record = json.loads(result.output.read_text(encoding="utf-8"))["record"]
            self.assertIsNone(record["identity"]["document_id"])
            self.assertEqual(record["acquisition_state"]["opening"], "NOT_ATTEMPTED")
            fields = {field["field_name"]: field for field in record["fields"]}
            self.assertEqual(fields["origem"]["state"], FieldState.NOT_EVALUATED.value)
            self.assertEqual(fields["destino"]["state"], FieldState.NOT_EVALUATED.value)

    def test_normalization_failure_is_diagnostic_and_batch_continues(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "frozen"
            source.mkdir()
            (source / "a.json").write_bytes((FIXTURES / "act_pdf_extracted.json").read_bytes())
            (source / "b.json").write_bytes((FIXTURES / "pt_html_extracted.json").read_bytes())
            real = module._normalizer
            def dispatch(family: str):
                if family == "act":
                    return Mock(side_effect=RuntimeError("synthetic normalization failure"))
                return real(family)
            with patch.object(module, "_normalizer", side_effect=dispatch):
                report = module.reprocess_directory(source, root / "v2")
            self.assertEqual((report.processed, report.failed), (1, 1))
            self.assertEqual(report.results[0].stage, "normalization")
            self.assertFalse((root / "v2" / "a.v2.json").exists())
            self.assertTrue((root / "v2" / "b.v2.json").is_file())

    def test_write_failure_leaves_no_partial_and_is_not_success(self) -> None:
        source = FIXTURES / "act_pdf_extracted.json"
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            destination = Path(temporary) / "v2"
            with patch.object(module, "write_v2_sidecar", side_effect=OSError("disk full")):
                result = module.reprocess_snapshot(source, destination)
            self.assertEqual((result.status, result.stage), ("failed", "writing"))
            self.assertEqual(list(destination.rglob("*")) if destination.exists() else [], [])

    def test_aa_no_browser_or_network_modules_are_needed(self) -> None:
        forbidden = {"selenium", "requests", "app.rpa.scraping", "app.core.driver_factory"}
        before = set(sys.modules)
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            result = module.reprocess_snapshot(FIXTURES / "ted_normalizer_rich.json", Path(temporary) / "v2")
        self.assertEqual(result.status, "processed")
        imported = set(sys.modules) - before
        self.assertFalse(any(name == item or name.startswith(item + ".") for item in forbidden for name in imported))

    def test_invalid_json_and_destination_inside_source_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            source = root / "frozen"
            source.mkdir()
            (source / "bad.json").write_text("{", encoding="utf-8")
            report = module.reprocess_directory(source, root / "v2")
            self.assertEqual((report.failed, report.results[0].stage), (1, "input"))
            with self.assertRaisesRegex(module.OfflineReprocessorError, "separate"):
                module.reprocess_directory(source, source / "derived")


if __name__ == "__main__":
    unittest.main()
