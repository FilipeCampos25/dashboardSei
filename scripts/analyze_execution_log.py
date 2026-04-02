#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable


PROCESS_RE = re.compile(r"\b\d{5}\.\d{6}/\d{4}-\d{2}\b")
OPEN_PROCESS_RE = re.compile(r"Abrindo processo\s+(?P<process>\d{5}\.\d{6}/\d{4}-\d{2})", re.IGNORECASE)
CLOSE_PROCESS_RE = re.compile(
    r"Processo\s+(?P<process>\d{5}\.\d{6}/\d{4}-\d{2}):\s+fechando aba e voltando",
    re.IGNORECASE,
)
SEARCH_START_RE = re.compile(r"iniciando busca do documento\s+'([^']+)'", re.IGNORECASE)
SEARCH_CLICK_RE = re.compile(r"clicando Pesquisar no Processo para\s+(.+?)\.", re.IGNORECASE)
FILTER_ALREADY_OPEN_RE = re.compile(r"filtro ja estava aberto para\s+(.+?)\.", re.IGNORECASE)
SEM_RESULTADO_RE = re.compile(
    r"Processo\s+(?P<process>\d{5}\.\d{6}/\d{4}-\d{2}):\s+(?P<doc>PT|ACT|MEMORANDO|TED)\s+termo\b.*sem resultado no filtro",
    re.IGNORECASE,
)
FALHA_FILTRO_RE = re.compile(
    r"Processo\s+(?P<process>\d{5}\.\d{6}/\d{4}-\d{2}):\s+(?P<doc>PT|ACT|MEMORANDO|TED)\s+termo\b.*falhou no filtro",
    re.IGNORECASE,
)
FALLBACK_START_RE = re.compile(
    r"Processo\s+(?P<process>\d{5}\.\d{6}/\d{4}-\d{2}):\s+nenhum candidato canonico de\s+(?P<doc>PT|ACT|MEMORANDO|TED)\s+consolidado no filtro; tentando fallback pela arvore",
    re.IGNORECASE,
)
FALLBACK_LINE_RE = re.compile(r"Fallback arvore\s+(PT|ACT|MEMORANDO|TED):", re.IGNORECASE)
RELOAD_RE = re.compile(r"restaurando contexto base do processo", re.IGNORECASE)

DOC_TYPES = ("PT", "ACT", "MEMORANDO", "TED")
STAGE_KEYS = (
    "abertura_processo",
    "busca_pt",
    "busca_act",
    "busca_memorando",
    "busca_ted",
    "fallback_arvore",
)
STAGE_LABELS = {
    "abertura_processo": "abertura do processo",
    "busca_pt": "busca PT",
    "busca_act": "busca ACT",
    "busca_memorando": "busca MEMORANDO",
    "busca_ted": "busca TED",
    "fallback_arvore": "fallback pela arvore",
}


@dataclass
class LogEntry:
    timestamp: datetime
    message: str
    raw: dict


@dataclass
class ProcessSummary:
    process_number: str
    started_at: datetime
    ended_at: datetime | None = None
    last_seen_at: datetime | None = None
    last_event_at: datetime | None = None
    current_stage: str | None = "abertura_processo"
    current_doc_type: str | None = None
    attempt_open_doc: str | None = None
    stage_seconds: dict[str, float] = field(
        default_factory=lambda: {stage: 0.0 for stage in STAGE_KEYS}
    )
    attempts: Counter = field(default_factory=Counter)
    sem_resultado: Counter = field(default_factory=Counter)
    falha_filtro: Counter = field(default_factory=Counter)
    reloads_contexto_total: int = 0
    reloads_contexto: Counter = field(default_factory=Counter)
    fallbacks_total: int = 0
    fallbacks: Counter = field(default_factory=Counter)

    def add_elapsed(self, current_time: datetime) -> None:
        if self.last_event_at is None:
            self.last_event_at = current_time
            self.last_seen_at = current_time
            return
        delta = (current_time - self.last_event_at).total_seconds()
        if delta < 0:
            delta = 0.0
        if self.current_stage in self.stage_seconds:
            self.stage_seconds[self.current_stage] += delta
        self.last_event_at = current_time
        self.last_seen_at = current_time

    @property
    def total_seconds(self) -> float:
        end_time = self.ended_at or self.last_seen_at or self.started_at
        return max((end_time - self.started_at).total_seconds(), 0.0)

    @property
    def dominant_stage(self) -> tuple[str, float]:
        stage, seconds = max(self.stage_seconds.items(), key=lambda item: item[1])
        return STAGE_LABELS[stage], seconds


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.upper()


def infer_doc_type(text: str) -> str | None:
    upper_text = normalize_text(text)
    if "PLANO DE TRABALHO" in upper_text:
        return "PT"
    if "MEMORANDO" in upper_text:
        return "MEMORANDO"
    if "EXECUCAO DESCENTRALIZADA" in upper_text or upper_text.startswith("TED"):
        return "TED"
    if "ACORDO DE COOPERACAO" in upper_text or "ACT" in upper_text:
        return "ACT"
    return None


def load_entries(log_path: Path) -> list[LogEntry]:
    entries: list[LogEntry] = []
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            timestamp_raw = payload.get("timestamp")
            message = payload.get("message")
            if not timestamp_raw or not isinstance(message, str):
                continue
            try:
                timestamp = datetime.fromisoformat(timestamp_raw)
            except ValueError:
                continue
            entries.append(LogEntry(timestamp=timestamp, message=message, raw=payload))
    entries.sort(key=lambda entry: entry.timestamp)
    return entries


def resolve_output_path(input_path: Path, output_path: str | None) -> Path:
    if output_path:
        return Path(output_path)
    return input_path.with_name("process_summary.csv")


def set_stage(summary: ProcessSummary, stage: str | None, doc_type: str | None = None) -> None:
    summary.current_stage = stage
    summary.current_doc_type = doc_type


def start_doc_attempt(summary: ProcessSummary, doc_type: str) -> None:
    if summary.attempt_open_doc != doc_type:
        summary.attempts[doc_type] += 1
        summary.attempt_open_doc = doc_type
    set_stage(summary, f"busca_{doc_type.lower()}", doc_type)


def infer_reload_doc_type(message: str, current_doc_type: str | None) -> str | None:
    for doc_type in DOC_TYPES:
        if re.search(rf"\b{doc_type}\b", normalize_text(message)):
            return doc_type
    return current_doc_type


def finalize_process(summary: ProcessSummary, ended_at: datetime) -> ProcessSummary:
    summary.add_elapsed(ended_at)
    summary.ended_at = ended_at
    summary.attempt_open_doc = None
    set_stage(summary, None, None)
    return summary


def analyze_entries(entries: Iterable[LogEntry]) -> list[ProcessSummary]:
    summaries: list[ProcessSummary] = []
    active: ProcessSummary | None = None

    for entry in entries:
        message = entry.message

        open_match = OPEN_PROCESS_RE.search(message)
        if open_match:
            process_number = open_match.group("process")
            if active is not None:
                summaries.append(finalize_process(active, entry.timestamp))
            active = ProcessSummary(process_number=process_number, started_at=entry.timestamp)
            active.last_event_at = entry.timestamp
            active.last_seen_at = entry.timestamp
            continue

        if active is None:
            continue

        active.add_elapsed(entry.timestamp)

        close_match = CLOSE_PROCESS_RE.search(message)
        if close_match and close_match.group("process") == active.process_number:
            summaries.append(finalize_process(active, entry.timestamp))
            active = None
            continue

        search_click_match = SEARCH_CLICK_RE.search(message)
        if search_click_match:
            doc_type = infer_doc_type(search_click_match.group(1))
            if doc_type:
                start_doc_attempt(active, doc_type)

        filter_open_match = FILTER_ALREADY_OPEN_RE.search(message)
        if filter_open_match:
            doc_type = infer_doc_type(filter_open_match.group(1))
            if doc_type:
                start_doc_attempt(active, doc_type)

        search_start_match = SEARCH_START_RE.search(message)
        if search_start_match:
            doc_type = infer_doc_type(search_start_match.group(1))
            if doc_type:
                start_doc_attempt(active, doc_type)

        sem_resultado_match = SEM_RESULTADO_RE.search(message)
        if sem_resultado_match and sem_resultado_match.group("process") == active.process_number:
            doc_type = sem_resultado_match.group("doc").upper()
            active.sem_resultado[doc_type] += 1
            active.attempt_open_doc = None
            set_stage(active, f"busca_{doc_type.lower()}", doc_type)

        falha_filtro_match = FALHA_FILTRO_RE.search(message)
        if falha_filtro_match and falha_filtro_match.group("process") == active.process_number:
            doc_type = falha_filtro_match.group("doc").upper()
            active.falha_filtro[doc_type] += 1
            active.attempt_open_doc = None
            set_stage(active, f"busca_{doc_type.lower()}", doc_type)

        fallback_start_match = FALLBACK_START_RE.search(message)
        if fallback_start_match and fallback_start_match.group("process") == active.process_number:
            doc_type = fallback_start_match.group("doc").upper()
            active.fallbacks_total += 1
            active.fallbacks[doc_type] += 1
            active.attempt_open_doc = None
            set_stage(active, "fallback_arvore", doc_type)

        fallback_line_match = FALLBACK_LINE_RE.search(message)
        if fallback_line_match and active.current_stage != "fallback_arvore":
            doc_type = fallback_line_match.group(1).upper()
            set_stage(active, "fallback_arvore", doc_type)

        if RELOAD_RE.search(message):
            active.reloads_contexto_total += 1
            reload_doc_type = infer_reload_doc_type(message, active.current_doc_type)
            if reload_doc_type:
                active.reloads_contexto[reload_doc_type] += 1
            active.attempt_open_doc = None

    if active is not None:
        fallback_end = active.last_seen_at or active.started_at
        summaries.append(finalize_process(active, fallback_end))

    return summaries


def build_rows(summaries: list[ProcessSummary]) -> list[dict[str, object]]:
    ordered = sorted(summaries, key=lambda summary: summary.total_seconds, reverse=True)
    top_processes = {summary.process_number for summary in ordered[:5]}
    rows: list[dict[str, object]] = []

    for summary in ordered:
        dominant_stage, dominant_stage_seconds = summary.dominant_stage
        row: dict[str, object] = {
            "process_number": summary.process_number,
            "started_at": summary.started_at.isoformat(timespec="seconds"),
            "ended_at": (summary.ended_at or summary.last_seen_at or summary.started_at).isoformat(
                timespec="seconds"
            ),
            "total_seconds": round(summary.total_seconds, 3),
            "abertura_processo_seconds": round(summary.stage_seconds["abertura_processo"], 3),
            "busca_pt_seconds": round(summary.stage_seconds["busca_pt"], 3),
            "busca_act_seconds": round(summary.stage_seconds["busca_act"], 3),
            "busca_memorando_seconds": round(summary.stage_seconds["busca_memorando"], 3),
            "busca_ted_seconds": round(summary.stage_seconds["busca_ted"], 3),
            "fallback_arvore_seconds": round(summary.stage_seconds["fallback_arvore"], 3),
            "pt_attempts": summary.attempts["PT"],
            "act_attempts": summary.attempts["ACT"],
            "memorando_attempts": summary.attempts["MEMORANDO"],
            "ted_attempts": summary.attempts["TED"],
            "pt_sem_resultado_filtro": summary.sem_resultado["PT"],
            "act_sem_resultado_filtro": summary.sem_resultado["ACT"],
            "memorando_sem_resultado_filtro": summary.sem_resultado["MEMORANDO"],
            "ted_sem_resultado_filtro": summary.sem_resultado["TED"],
            "pt_falha_filtro": summary.falha_filtro["PT"],
            "act_falha_filtro": summary.falha_filtro["ACT"],
            "memorando_falha_filtro": summary.falha_filtro["MEMORANDO"],
            "ted_falha_filtro": summary.falha_filtro["TED"],
            "reloads_contexto_total": summary.reloads_contexto_total,
            "pt_reloads_contexto": summary.reloads_contexto["PT"],
            "act_reloads_contexto": summary.reloads_contexto["ACT"],
            "memorando_reloads_contexto": summary.reloads_contexto["MEMORANDO"],
            "ted_reloads_contexto": summary.reloads_contexto["TED"],
            "fallbacks_total": summary.fallbacks_total,
            "pt_fallbacks": summary.fallbacks["PT"],
            "act_fallbacks": summary.fallbacks["ACT"],
            "memorando_fallbacks": summary.fallbacks["MEMORANDO"],
            "ted_fallbacks": summary.fallbacks["TED"],
            "dominant_stage": dominant_stage,
            "dominant_stage_seconds": round(dominant_stage_seconds, 3),
            "top_5_slowest": "yes" if summary.process_number in top_processes else "no",
        }
        rows.append(row)

    return rows


def write_csv(rows: list[dict[str, object]], output_path: Path) -> None:
    if not rows:
        raise ValueError("Nenhum processo foi identificado no log.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_top_five(rows: list[dict[str, object]]) -> None:
    print("TOP 5 processos mais lentos:")
    for index, row in enumerate(rows[:5], start=1):
        print(
            f"{index}. {row['process_number']} | total={row['total_seconds']}s | "
            f"etapa_mais_lenta={row['dominant_stage']} ({row['dominant_stage_seconds']}s)"
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analisa execution_log_latest.json e gera resumo por processo."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="output/execution_log_latest.json",
        help="Caminho do arquivo de log NDJSON. Default: output/execution_log_latest.json",
    )
    parser.add_argument(
        "--output",
        help="Caminho do CSV de saida. Default: mesmo diretorio do input com nome process_summary.csv",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Arquivo de log nao encontrado: {input_path}", file=sys.stderr)
        return 1

    entries = load_entries(input_path)
    if not entries:
        print(f"Nenhuma entrada valida encontrada em: {input_path}", file=sys.stderr)
        return 1

    summaries = analyze_entries(entries)
    rows = build_rows(summaries)
    output_path = resolve_output_path(input_path, args.output)
    write_csv(rows, output_path)

    print(f"Arquivo lido: {input_path}")
    print(f"Processos analisados: {len(rows)}")
    print(f"CSV gerado em: {output_path}")
    print_top_five(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
