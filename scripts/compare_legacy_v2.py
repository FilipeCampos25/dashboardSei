"""Generate a shadow-only report from explicit legacy and V2 candidate decisions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.publication_comparison import (  # noqa: E402
    build_comparison_report,
    comparison_input_from_dict,
    serialize_comparison_report,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON envelope containing explicit candidate decisions")
    parser.add_argument("--output", type=Path, help="Explicit shadow JSON destination; stdout is the default")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
        raw_candidates = payload.get("candidates") if isinstance(payload, dict) else None
        if not isinstance(raw_candidates, list):
            raise ValueError("input must contain a candidates list")
        report = build_comparison_report([comparison_input_from_dict(item) for item in raw_candidates])
        serialized = serialize_comparison_report(report)
        if args.output is None:
            sys.stdout.write(serialized)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(serialized, encoding="utf-8", newline="\n")
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
