"""Offline guard against versioned or packaged environment secret files."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE_NAMES = {".env.example"}
SENSITIVE_NAME = re.compile(
    r"(?:PASSWORD|PASSWD|SECRET|TOKEN|API_KEY|PRIVATE_KEY|COOKIE|USERNAME|USER|URL)",
    re.IGNORECASE,
)
PLACEHOLDER_VALUE = re.compile(
    r"(?:|change[-_ ]?me|replace[-_ ]?me|your(?:[-_<{ ].*)?|"
    r"example(?:[._-].*)?|placeholder|dummy|fake|test|x+|<.*>|\$\{[^}]+\})",
    re.IGNORECASE,
)


def is_real_env_path(path: str) -> bool:
    name = PurePosixPath(path.replace("\\", "/")).name
    return (name == ".env" or name.startswith(".env.")) and name not in ENV_EXAMPLE_NAMES


def tracked_real_env_files(repo_root: Path = REPO_ROOT) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    return sorted(
        path
        for path in result.stdout.decode("utf-8", errors="surrogateescape").split("\0")
        if path and is_real_env_path(path)
    )


def unsafe_example_lines(path: Path) -> list[int]:
    unsafe: list[int] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)", line)
        if not match:
            unsafe.append(line_number)
            continue
        name, value = match.groups()
        value = value.strip().strip("\"'")
        if SENSITIVE_NAME.search(name) and not PLACEHOLDER_VALUE.fullmatch(value):
            unsafe.append(line_number)
    return unsafe


def packaged_real_env_files(archive_path: Path) -> list[str]:
    with zipfile.ZipFile(archive_path) as archive:
        return sorted(name for name in archive.namelist() if is_real_env_path(name))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", nargs="*", type=Path, help="ZIP artifacts to inspect")
    args = parser.parse_args(argv)

    failures: list[str] = []
    tracked = tracked_real_env_files()
    if tracked:
        failures.append("Git tracks prohibited .env file(s): " + ", ".join(tracked))

    examples = [REPO_ROOT / ".env.example", REPO_ROOT / "backend" / ".env.example"]
    existing_examples = [path for path in examples if path.is_file()]
    if not existing_examples:
        failures.append("No .env.example file exists")
    for example in existing_examples:
        unsafe_lines = unsafe_example_lines(example)
        if unsafe_lines:
            relative = example.relative_to(REPO_ROOT)
            failures.append(f"{relative} has unsafe or invalid entries on line(s): {unsafe_lines}")

    for archive_path in args.archives:
        packaged = packaged_real_env_files(archive_path)
        if packaged:
            failures.append(f"{archive_path} contains prohibited .env file(s): " + ", ".join(packaged))

    if failures:
        for failure in failures:
            print(f"ERROR: {failure}", file=sys.stderr)
        return 1
    print("Secret hygiene check passed (tracked files, examples, and requested ZIP artifacts).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
