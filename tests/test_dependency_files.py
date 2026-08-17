from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PINNED_REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)$")


def _read_pins(filename: str) -> dict[str, str]:
    pins: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        (ROOT / filename).read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = PINNED_REQUIREMENT.fullmatch(line)
        if match is None:
            raise AssertionError(f"{filename}:{line_number} nao e um pin absoluto: {line}")
        name, version = match.groups()
        normalized_name = re.sub(r"[-_.]+", "-", name).lower()
        if normalized_name in pins:
            raise AssertionError(f"{filename}:{line_number} duplica {normalized_name}")
        pins[normalized_name] = version
    return pins


class DependencyFilesTests(unittest.TestCase):
    def test_dependency_files_use_unique_absolute_pins(self) -> None:
        self.assertTrue(_read_pins("requirements.txt"))
        self.assertTrue(_read_pins("constraints.txt"))

    def test_direct_dependencies_match_constraints(self) -> None:
        requirements = _read_pins("requirements.txt")
        constraints = _read_pins("constraints.txt")
        self.assertEqual(
            {},
            {
                name: (version, constraints.get(name))
                for name, version in requirements.items()
                if constraints.get(name) != version
            },
        )

    def test_pydantic_2_compatibility_is_explicit(self) -> None:
        constraints = _read_pins("constraints.txt")
        self.assertIn("pydantic", constraints)
        self.assertEqual("2", constraints["pydantic"].split(".", 1)[0])


if __name__ == "__main__":
    unittest.main()
