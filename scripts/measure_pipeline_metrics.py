"""Measure observable legacy pipeline stages from a frozen output directory.

The report describes existing evidence; it does not reconstruct missing stages or
apply new classification, canonical-selection, or publication rules.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = "1.0"
NOT_OBSERVABLE = "not_observable_in_legacy"

FAMILY_SOURCES = {
    "act": {
        "status": "act_status_execucao_latest.csv",
        "candidates": "act_classificacao_latest.csv",
        "final": "act_normalizado_latest.csv",
        "fields": (
            "numero_acordo",
            "data_assinatura",
            "vigencia_inicio",
            "vigencia_fim",
            "orgao_convenente",
            "objeto",
            "gestor_titular",
            "unidade_responsavel",
        ),
    },
    "administrative": {
        "status": "documento_administrativo_status_execucao_latest.csv",
        "candidates": "documento_administrativo_status_execucao_latest.csv",
        "final": "documento_administrativo_normalizado_latest.csv",
        "fields": (
            "funcao_administrativa",
            "origem",
            "destino",
            "data",
            "assunto",
            "resumo",
            "acao_solicitada",
            "prazo",
            "documentos_mencionados",
        ),
    },
    "pt": {
        "status": "pt_status_execucao_latest.csv",
        "candidates": "pt_auditoria_latest.csv",
        "final": "pt_normalizado_latest.csv",
        "fields": (
            "parceiro",
            "data_assinatura",
            "vigencia_inicio",
            "vigencia_fim",
            "objeto",
            "atribuições_raw",
            "metas_raw",
            "acoes_raw",
            "prazo_inicio",
            "prazo_fim",
        ),
    },
    "ted": {
        "status": "ted_status_execucao_latest.csv",
        "candidates": "ted_status_execucao_latest.csv",
        "final": "ted_normalizado_latest.csv",
        "fields": (
            "numero_ted",
            "ano_ted",
            "objeto",
            "unidade_descentralizadora",
            "unidade_descentralizada",
            "valor_global",
            "data_assinatura",
            "vigencia_inicio",
            "vigencia_fim",
            "plano_aplicacao",
            "cronograma_desembolso",
            "metas",
            "prestacao_contas",
        ),
    },
}


def _read_csv(output_dir: Path, filename: str) -> list[dict[str, str]]:
    path = output_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"Required legacy artifact is missing: {filename}")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _count_values(rows: Iterable[dict[str, str]], field: str) -> dict[str, int]:
    counts = Counter((row.get(field) or "").strip() or "[empty]" for row in rows)
    return dict(sorted(counts.items()))


def _bool_state(value: str) -> str:
    normalized = value.strip().lower()
    if normalized == "true":
        return "found"
    if normalized == "false":
        return "not_found"
    return "unknown"


def _extraction_state(row: dict[str, str]) -> str:
    if (row.get("extraction_error") or "").strip():
        return "failure"
    raw_chars = (row.get("text_chars") or "").strip()
    if not raw_chars:
        return NOT_OBSERVABLE
    try:
        return "extracted_content" if int(raw_chars) > 0 else "empty_content"
    except ValueError:
        return NOT_OBSERVABLE


def _process_key(row: dict[str, str]) -> str:
    return (row.get("processo") or "").strip()


def _dimension(kind: str, sources: list[str], counts: dict[str, int], **extra: object) -> dict[str, object]:
    return {
        "measurement_kind": kind,
        "sources": sorted(sources),
        "counts": dict(sorted(counts.items())),
        **extra,
    }


def _measure_family(output_dir: Path, family: str, config: dict[str, object]) -> dict[str, object]:
    status_name = str(config["status"])
    candidate_name = str(config["candidates"])
    final_name = str(config["final"])
    status_rows = _read_csv(output_dir, status_name)
    candidate_rows = _read_csv(output_dir, candidate_name)
    final_rows = _read_csv(output_dir, final_name)

    discovery_counts = Counter(_bool_state(row.get("found", "")) for row in status_rows)
    extraction_counts = Counter(_extraction_state(row) for row in status_rows)

    classification_counts = _count_values(candidate_rows, "validation_status")
    if set(classification_counts) == {"[empty]"}:
        classification_counts = {NOT_OBSERVABLE: len(candidate_rows)}

    gold_by_process = Counter(
        _process_key(row)
        for row in candidate_rows
        if (row.get("publication_status") or "").strip() == "published_gold"
        and _process_key(row)
    )
    candidate_processes = {_process_key(row) for row in candidate_rows if _process_key(row)}
    canonical_counts = {
        "candidate_rows": len(candidate_rows),
        "candidate_processes": len(candidate_processes),
        "explicit_gold_rows": sum(gold_by_process.values()),
        "explicit_gold_processes": len(gold_by_process),
        "final_rows": len(final_rows),
        "processes_with_multiple_gold": sum(count > 1 for count in gold_by_process.values()),
        "processes_without_explicit_gold": len(candidate_processes - gold_by_process.keys()),
    }

    field_counts: dict[str, dict[str, int]] = {}
    for field in config["fields"]:  # type: ignore[index]
        present = sum(bool((row.get(str(field)) or "").strip()) for row in final_rows)
        field_counts[str(field)] = {
            "absent_or_empty": len(final_rows) - present,
            "evaluable": len(final_rows),
            "present": present,
        }

    preview_fields = [field for field in ("preview_numero_act",) if any(field in row for row in candidate_rows)]
    preview_counts = {
        field: {
            "absent_or_empty": len(candidate_rows)
            - sum(bool((row.get(field) or "").strip()) for row in candidate_rows),
            "evaluable": len(candidate_rows),
            "present": sum(bool((row.get(field) or "").strip()) for row in candidate_rows),
        }
        for field in preview_fields
    }

    return {
        "discovery": _dimension(
            "observed",
            [status_name],
            dict(discovery_counts),
            evidence="legacy found flag",
        ),
        "access": _dimension(
            "unavailable",
            [status_name],
            {NOT_OBSERVABLE: len(status_rows)},
            evidence="legacy artifacts have no explicit access-state field; empty content is not treated as inaccessible",
        ),
        "extraction": _dimension(
            "observed",
            [status_name],
            dict(extraction_counts),
            evidence="legacy text_chars and extraction_error fields; partial extraction is not explicitly represented",
        ),
        "classification": _dimension(
            "observed",
            [candidate_name],
            classification_counts,
            evidence="raw legacy validation_status values; no new classification heuristic",
        ),
        "canonicity": _dimension(
            "mixed_observed_and_derived",
            [candidate_name, final_name],
            canonical_counts,
            observed_metrics=["candidate_rows", "explicit_gold_rows", "final_rows"],
            derived_metrics=[
                "candidate_processes",
                "explicit_gold_processes",
                "processes_with_multiple_gold",
                "processes_without_explicit_gold",
            ],
        ),
        "fields": {
            "measurement_kind": "derived",
            "sources": [final_name],
            "counts": dict(sorted(field_counts.items())),
            "preview_or_fallback_counts": dict(sorted(preview_counts.items())),
            "evidence": "presence counts derived from explicit values in final legacy rows",
        },
    }


def _aggregate_dimension(
    families: dict[str, dict[str, object]], dimension: str
) -> dict[str, object]:
    kinds = sorted(
        {str(families[family][dimension]["measurement_kind"]) for family in families}  # type: ignore[index]
    )
    counts: Counter[str] = Counter()
    if dimension == "fields":
        for family in families:
            field_counts = families[family][dimension]["counts"]  # type: ignore[index]
            counts["business_fields"] += len(field_counts)
            for values in field_counts.values():
                counts.update(values)
    else:
        for family in families:
            counts.update(families[family][dimension]["counts"])  # type: ignore[index]
    return {
        "family_count": len(families),
        "families": sorted(families),
        "measurement_kinds": kinds,
        "counts": dict(sorted(counts.items())),
    }


def measure_pipeline_metrics(output_dir: str | Path) -> dict[str, object]:
    """Return deterministic metrics for an explicitly supplied frozen output directory."""

    source = Path(output_dir)
    if not source.is_dir():
        raise NotADirectoryError(f"Baseline output directory does not exist: {source}")
    source = source.resolve(strict=True)
    families = {
        family: _measure_family(source, family, FAMILY_SOURCES[family])
        for family in sorted(FAMILY_SOURCES)
    }
    dimensions = ("discovery", "access", "extraction", "classification", "canonicity", "fields")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_scope": "legacy_offline_measurement",
        "dimensions": list(dimensions),
        "families": families,
        "global": {
            dimension: _aggregate_dimension(families, dimension)
            for dimension in dimensions
        },
        "limitations": [
            "Access state is not explicitly represented in the frozen legacy artifacts.",
            "Empty extracted content does not imply access restriction or access failure.",
            "Candidate rows do not imply canonical winners; only explicit publication markers are counted as gold.",
            "Partial extraction is not inferred when no explicit legacy state exists.",
        ],
    }


def serialize_report(report: dict[str, object]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-output", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="Explicit JSON destination; stdout is the default")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        serialized = serialize_report(measure_pipeline_metrics(args.baseline_output))
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
