from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))
sys.path.insert(0, str(REPO_ROOT))

from app.services.run_manifest import build_run_manifest, serialize_manifest  # noqa: E402
from scripts.freeze_baseline import main as freeze_baseline_main  # noqa: E402


class RunManifestTests(unittest.TestCase):
    def _create_baseline(self, root: Path) -> None:
        backend_output = root / "backend" / "output"
        execution_output = root / "output"
        candidates = backend_output / "candidates"
        candidates.mkdir(parents=True)
        execution_output.mkdir(parents=True)
        (backend_output / "z_status_latest.csv").write_text("status\nok\n", encoding="utf-8")
        (backend_output / "a_normalizado_latest.csv").write_text("field\nvalue\n", encoding="utf-8")
        (candidates / "candidate.json").write_text(
            '{"document_text":"synthetic private document"}\n', encoding="utf-8"
        )
        (execution_output / "execution_log_latest.json").write_text(
            '{"level":"INFO","message":"synthetic"}\n', encoding="utf-8"
        )

    def _build(self, root: Path, **overrides: object) -> dict[str, object]:
        arguments: dict[str, object] = {
            "run_id": "output-3-test",
            "origin": "sanitized-test-baseline",
            "execution_mode": "offline-existing-output",
            "captured_at": "2026-08-05",
            "contract_version": "legacy",
            "completeness": "complete",
        }
        arguments.update(overrides)
        return build_run_manifest(root, **arguments)  # type: ignore[arg-type]

    def test_same_input_produces_identical_manifest_and_stable_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._create_baseline(root)

            first = self._build(root)
            second = self._build(root)

            self.assertEqual(serialize_manifest(first), serialize_manifest(second))
            self.assertEqual(first["summary"], second["summary"])
            paths = [item["path"] for item in first["artifacts"]]  # type: ignore[index]
            self.assertEqual(paths, sorted(paths))
            self.assertTrue(all(not Path(path).is_absolute() and "\\" not in path for path in paths))

    def test_changed_file_changes_file_hash_and_inventory_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._create_baseline(root)
            first = self._build(root)
            target = root / "backend" / "output" / "a_normalizado_latest.csv"
            target.write_text("field\nchanged\n", encoding="utf-8")

            second = self._build(root)

            first_hashes = {item["path"]: item["sha256"] for item in first["artifacts"]}  # type: ignore[index]
            second_hashes = {item["path"]: item["sha256"] for item in second["artifacts"]}  # type: ignore[index]
            self.assertNotEqual(
                first_hashes["backend/output/a_normalizado_latest.csv"],
                second_hashes["backend/output/a_normalizado_latest.csv"],
            )
            self.assertNotEqual(
                first["summary"]["inventory_sha256"],  # type: ignore[index]
                second["summary"]["inventory_sha256"],  # type: ignore[index]
            )

    def test_manifest_never_embeds_file_content_and_excludes_sensitive_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._create_baseline(root)
            sensitive_value = "fictional-secret-value-for-test"
            (root / "backend" / "output" / ".env").write_text(
                f"PASSWORD={sensitive_value}\n", encoding="utf-8"
            )

            serialized = serialize_manifest(self._build(root))
            manifest = json.loads(serialized)

            self.assertNotIn(sensitive_value, serialized)
            self.assertNotIn("synthetic private document", serialized)
            self.assertFalse(any(item["path"].endswith(".env") for item in manifest["artifacts"]))
            self.assertIn(
                {"code": "sensitive_artifacts_excluded", "count": 1}, manifest["warnings"]
            )

    def test_missing_source_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            missing = Path(temporary_directory) / "missing"
            with self.assertRaises(FileNotFoundError):
                self._build(missing)

    def test_missing_required_include_marks_manifest_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "backend" / "output").mkdir(parents=True)
            (root / "backend" / "output" / "artifact.csv").write_text("x\n", encoding="utf-8")

            manifest = self._build(root, completeness="unknown")

            self.assertEqual(manifest["completeness"]["status"], "incomplete")  # type: ignore[index]
            self.assertEqual(manifest["warnings"][0]["code"], "missing_includes")  # type: ignore[index]
            self.assertEqual(
                manifest["warnings"][0]["paths"], ["output/execution_log_latest.json"]  # type: ignore[index]
            )

    def test_hashing_does_not_change_source_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._create_baseline(root)
            files = sorted(path for path in root.rglob("*") if path.is_file())
            before = {path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns) for path in files}

            self._build(root)

            after = {path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns) for path in files}
            self.assertEqual(before, after)

    def test_cli_defaults_to_stdout_without_writing_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._create_baseline(root)
            before_paths = sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = freeze_baseline_main(
                    [
                        "--source",
                        str(root),
                        "--run-id",
                        "output-3-test",
                        "--origin",
                        "sanitized-test-baseline",
                        "--execution-mode",
                        "offline-existing-output",
                    ]
                )

            after_paths = sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())
            self.assertEqual(result, 0)
            self.assertEqual(before_paths, after_paths)
            self.assertEqual(json.loads(stdout.getvalue())["run_id"], "output-3-test")

    def test_cli_writes_only_explicit_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory) / "baseline"
            destination = Path(temporary_directory) / "metadata" / "run_manifest.json"
            self._create_baseline(root)

            result = freeze_baseline_main(
                [
                    "--source",
                    str(root),
                    "--run-id",
                    "output-3-test",
                    "--origin",
                    "sanitized-test-baseline",
                    "--execution-mode",
                    "offline-existing-output",
                    "--output",
                    str(destination),
                ]
            )

            self.assertEqual(result, 0)
            self.assertTrue(destination.is_file())
            self.assertFalse((root / "run_manifest.json").exists())
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8"))["run_id"], "output-3-test")


if __name__ == "__main__":
    unittest.main()
