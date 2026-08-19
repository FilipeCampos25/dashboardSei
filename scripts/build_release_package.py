"""Build and validate a deterministic release ZIP from an explicit allowlist."""

from __future__ import annotations

import argparse
import subprocess
import zipfile
from io import BytesIO
from pathlib import Path, PurePosixPath

try:
    from scripts.check_secret_hygiene import (
        packaged_real_env_files,
        packaged_secret_findings,
    )
except ModuleNotFoundError:  # Direct execution: python scripts/build_release_package.py
    from check_secret_hygiene import (  # type: ignore[no-redef]
        packaged_real_env_files,
        packaged_secret_findings,
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_ROOT_FILES = {
    ".env.example",
    "README.md",
    "constraints.txt",
    "dashboard_streamlit.py",
    "requirements.txt",
}
ALLOWED_TREES = {"assets", "backend", "dashboard", "scripts"}
REQUIRED_MEMBERS = {
    ".env.example",
    "README.md",
    "backend/main.py",
    "constraints.txt",
    "dashboard_streamlit.py",
    "requirements.txt",
}
PROHIBITED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".selenium",
    ".venv",
    ".wdm",
    "__pycache__",
    "browser_downloads",
    "htmlcov",
    "venv",
}
PROHIBITED_PREFIXES = {"backend/output", "output"}
PROHIBITED_SUFFIXES = {".db", ".log", ".pyc", ".pyo", ".sqlite", ".sqlite3", ".tmp"}
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


class ReleasePackageError(ValueError):
    """Raised when a release package violates its fail-closed contract."""


def is_allowlisted(path: str) -> bool:
    normalized = PurePosixPath(path.replace("\\", "/"))
    if normalized.as_posix() in ALLOWED_ROOT_FILES:
        return True
    return len(normalized.parts) > 1 and normalized.parts[0] in ALLOWED_TREES


def prohibited_reason(path: str) -> str | None:
    normalized = PurePosixPath(path.replace("\\", "/"))
    parts_lower = [part.lower() for part in normalized.parts]
    if not path or normalized.is_absolute() or ".." in normalized.parts:
        return "unsafe path"
    if any(part in PROHIBITED_PARTS for part in parts_lower):
        return "prohibited directory"
    normalized_lower = "/".join(parts_lower)
    if any(
        normalized_lower == prefix or normalized_lower.startswith(prefix + "/")
        for prefix in PROHIBITED_PREFIXES
    ):
        return "generated output"
    if any(part.startswith("_tmp_") for part in parts_lower):
        return "temporary directory"
    if normalized.suffix.lower() in PROHIBITED_SUFFIXES:
        return "prohibited file type"
    if not is_allowlisted(normalized.as_posix()):
        return "not allowlisted"
    return None


def tracked_release_members(repo_root: Path = REPO_ROOT) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    tracked = result.stdout.decode("utf-8", errors="surrogateescape").split("\0")
    members: list[str] = []
    for path in tracked:
        if not path or not is_allowlisted(path):
            continue
        reason = prohibited_reason(path)
        if reason:
            raise ReleasePackageError(f"tracked release member {path!r}: {reason}")
        members.append(path)
    missing = sorted(REQUIRED_MEMBERS - set(members))
    if missing:
        raise ReleasePackageError("missing required release member(s): " + ", ".join(missing))
    return sorted(members)


def build_release_bytes(repo_root: Path = REPO_ROOT) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for relative in tracked_release_members(repo_root):
            info = zipfile.ZipInfo(relative, ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, (repo_root / relative).read_bytes())
    payload = buffer.getvalue()
    validate_release_package(BytesIO(payload))
    return payload


def validate_release_package(archive_source: Path | BytesIO) -> None:
    failures: list[str] = []
    with zipfile.ZipFile(archive_source) as archive:
        names = [info.filename for info in archive.infolist() if not info.is_dir()]
        if len(names) != len(set(names)):
            failures.append("duplicate archive member")
        for name in names:
            reason = prohibited_reason(name)
            if reason:
                failures.append(f"{name}: {reason}")
        missing = sorted(REQUIRED_MEMBERS - set(names))
        if missing:
            failures.append("missing required member(s): " + ", ".join(missing))

    if hasattr(archive_source, "seek"):
        archive_source.seek(0)
    env_files = packaged_real_env_files(archive_source)
    if env_files:
        failures.append("prohibited environment file(s): " + ", ".join(env_files))
    if hasattr(archive_source, "seek"):
        archive_source.seek(0)
    secret_findings = packaged_secret_findings(archive_source)
    if secret_findings:
        failures.append("high-confidence secret material at: " + ", ".join(secret_findings))
    if failures:
        raise ReleasePackageError("; ".join(failures))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new ZIP path; existing files are refused")
    args = parser.parse_args(argv)
    output = args.output.resolve()
    if output.exists():
        raise SystemExit(f"Refusing to overwrite existing release: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = build_release_bytes()
    temporary = output.with_name(output.name + ".tmp")
    if temporary.exists():
        raise SystemExit(f"Refusing to overwrite temporary release: {temporary}")
    try:
        temporary.write_bytes(payload)
        validate_release_package(temporary)
        temporary.replace(output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    print(f"Validated release package created: {output} ({len(payload)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
