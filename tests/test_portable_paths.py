from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.normalization_contract import DocumentIdentity  # noqa: E402
from app.services.portable_paths import (  # noqa: E402
    ARTIFACT_ROOT,
    PortableArtifactRef,
    PortablePathError,
)


class PortablePathTests(unittest.TestCase):
    def test_basic_round_trip_uses_stable_posix_serialization(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            target = root / "a" / "b" / "file.json"
            target.parent.mkdir(parents=True)
            target.write_text("{}", encoding="utf-8")
            reference = PortableArtifactRef.from_path(target, root=root)
            self.assertEqual(reference.to_dict(), {"root_kind": ARTIFACT_ROOT, "relative_path": "a/b/file.json"})
            self.assertEqual(reference.resolve(root), target.resolve())

    def test_tree_can_move_from_a_to_b_without_rewriting_reference(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            temporary_root = Path(temporary)
            root_a = temporary_root / "A"
            target_a = root_a / "baseline" / "nested" / "snapshot.json"
            target_a.parent.mkdir(parents=True)
            target_a.write_text("{}", encoding="utf-8")
            serialized = json.dumps(PortableArtifactRef.from_path(target_a, root=root_a).to_dict(), sort_keys=True)
            root_b = temporary_root / "B"
            shutil.copytree(root_a, root_b)
            reference = PortableArtifactRef.from_dict(json.loads(serialized))
            self.assertEqual(reference.resolve(root_b), root_b / "baseline" / "nested" / "snapshot.json")
            self.assertNotIn(str(root_a), serialized)

    def test_serialization_and_resolution_do_not_depend_on_cwd(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            target = root / "output_v2" / "record.json"
            target.parent.mkdir()
            target.write_text("{}", encoding="utf-8")
            original_cwd = Path.cwd()
            first_cwd = root / "cwd1"
            second_cwd = root / "cwd2"
            first_cwd.mkdir()
            second_cwd.mkdir()
            try:
                os.chdir(first_cwd)
                first = PortableArtifactRef.from_path(target, root=root)
                os.chdir(second_cwd)
                second = PortableArtifactRef.from_path(target, root=root)
                self.assertEqual(first.to_dict(), second.to_dict())
                self.assertEqual(first.resolve(root), second.resolve(root))
            finally:
                os.chdir(original_cwd)

    def test_escape_absolute_external_and_windows_syntax_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            outside = root.parent / "outside.json"
            with self.assertRaises(PortablePathError):
                PortableArtifactRef.from_path(outside, root=root)
            for value in ("../secret.env", "../../outside/file.json", "C:\\outside\\file.json", "/outside/file.json"):
                with self.subTest(value=value), self.assertRaises(PortablePathError):
                    PortableArtifactRef(value)

    def test_identity_and_location_coexist_without_inventing_ids(self) -> None:
        identity = DocumentIdentity(process_id="P-1")
        reference = PortableArtifactRef("candidates/act/document.json")
        self.assertEqual(identity.to_dict(), {
            "process_id": "P-1", "document_id": None, "candidate_id": None, "source_url": None,
        })
        self.assertEqual(reference.relative_path, "candidates/act/document.json")


if __name__ == "__main__":
    unittest.main()
