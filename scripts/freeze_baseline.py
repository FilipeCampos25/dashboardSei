"""Generate a deterministic offline manifest for an existing private baseline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.services.run_manifest import (  # noqa: E402
    COMPLETENESS_STATUSES,
    DEFAULT_INCLUDES,
    build_run_manifest,
    serialize_manifest,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="Baseline root directory")
    parser.add_argument("--run-id", required=True, help="Operator-supplied run identifier")
    parser.add_argument("--origin", required=True, help="Operator-supplied capture origin")
    parser.add_argument("--execution-mode", required=True, help="Known execution mode")
    parser.add_argument("--captured-at", help="Capture date/time as known by the operator")
    parser.add_argument("--contract-version", help="Output contract version, when known")
    parser.add_argument(
        "--completeness",
        choices=sorted(COMPLETENESS_STATUSES),
        default="unknown",
        help="Known completeness; missing includes always result in incomplete",
    )
    parser.add_argument(
        "--include",
        action="append",
        dest="includes",
        help="Relative file/directory to inventory; repeatable (project defaults when omitted)",
    )
    parser.add_argument("--output", type=Path, help="Write JSON here instead of stdout")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        manifest = build_run_manifest(
            args.source,
            run_id=args.run_id,
            origin=args.origin,
            execution_mode=args.execution_mode,
            captured_at=args.captured_at,
            contract_version=args.contract_version,
            completeness=args.completeness,
            includes=tuple(args.includes) if args.includes else DEFAULT_INCLUDES,
        )
        serialized = serialize_manifest(manifest)
        if args.output is None:
            sys.stdout.write(serialized)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(serialized, encoding="utf-8", newline="\n")
    except (FileNotFoundError, NotADirectoryError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
