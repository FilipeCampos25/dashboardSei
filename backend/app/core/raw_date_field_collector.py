# backend/app/core/raw_date_field_collector.py
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Iterable, Tuple

# palavras que indicam que um campo está relacionado a data/período
DATEY_LABEL_HINTS = [
    "início", "inicio", "término", "termino", "assinatura",
    "vigência", "vigencia", "prazo", "período", "periodo",
    "data", "duração", "duracao", "execução", "execucao"
]

# meses em pt (pra detectar mês/ano e mês por extenso)
MONTH_HINTS = [
    "jan", "fev", "mar", "abr", "mai", "jun",
    "jul", "ago", "set", "out", "nov", "dez",
    "janeiro", "fevereiro", "março", "marco", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
]

RE_LABEL_COLON = re.compile(r"(?P<label>[^:\n]{2,80})\s*:\s*(?P<value>[^:\n]{0,300})", re.IGNORECASE)
RE_DATE_LIKE = re.compile(
    r"(\b\d{1,2}\s*[\/\-\.]\s*\d{1,2}\s*[\/\-\.]\s*\d{2,4}\b)|"  # dd/mm/aaaa
    r"(\b\d{1,2}\s*[\/\-\.]\s*\d{4}\b)|"                         # mm/aaaa
    r"(\b[A-Za-zçãõáéíóúâêôà]{3,12}\s*[\/\-\.]\s*\d{2,4}\b)|"     # mês/ano
    r"(\b(ap[oó]s|antes|imediatamente|sem data|n[aã]o informado|n[aã]o se aplica)\b)",
    re.IGNORECASE
)

def _norm(s: str) -> str:
    s = (s or "").replace("\u00A0", " ").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s)
    return s.strip()

def _looks_like_label(s: str) -> bool:
    s0 = _norm(s).lower()
    if not s0:
        return False
    if len(s0) > 60:
        return False
    # se tem pistas de data/período, forte candidato
    if any(h in s0 for h in DATEY_LABEL_HINTS):
        return True
    # se for bem curtinho, pode ser label genérico
    return len(s0) <= 25

def _classify_field_key(label_raw: str) -> str:
    l = _norm(label_raw).lower()
    if "início" in l or "inicio" in l:
        return "inicio"
    if "término" in l or "termino" in l or "fim" in l:
        return "termino"
    if "assinatura" in l:
        return "assinatura"
    if "vigência" in l or "vigencia" in l:
        return "vigencia"
    if "prazo" in l:
        return "prazo"
    if "período" in l or "periodo" in l:
        return "periodo"
    if "data" in l:
        return "data"
    return "outro"

def _value_has_date_like(value: str) -> int:
    v = _norm(value).lower()
    if not v:
        return 0
    if RE_DATE_LIKE.search(v):
        return 1
    # heurística extra: contém mês por extenso/abrev
    if any(m in v for m in MONTH_HINTS):
        return 1
    # ou contém dígitos
    if any(ch.isdigit() for ch in v):
        return 1
    return 0

@dataclass
class RawField:
    field_key: str
    label_raw: str
    value_raw: str
    origin: str
    origin_ref: str
    section_hint: str
    evidence_snippet: str

def collect_raw_fields(snapshot_text: str, snapshot_tables: Optional[List[List[List[str]]]] = None) -> List[RawField]:
    snapshot_tables = snapshot_tables or []
    text = _norm(snapshot_text or "")

    fields: List[RawField] = []

    # 1) tabelas: label na primeira coluna
    for ti, table in enumerate(snapshot_tables):
        for ri, row in enumerate(table):
            if not row:
                continue
            c0 = _norm(row[0] if len(row) > 0 else "")
            rest = [_norm(c) for c in row[1:]] if len(row) > 1 else []
            value = " | ".join([c for c in rest if c])
            if _looks_like_label(c0):
                fields.append(RawField(
                    field_key=_classify_field_key(c0),
                    label_raw=c0,
                    value_raw=value,
                    origin="table",
                    origin_ref=f"table#{ti} row#{ri}",
                    section_hint="",
                    evidence_snippet=(c0 + ": " + value)[:180]
                ))

    # 2) texto: linhas com "label: valor"
    lines = text.split("\n") if text else []
    for li, line in enumerate(lines):
        line_n = _norm(line)
        if not line_n:
            continue

        matches = list(RE_LABEL_COLON.finditer(line_n))
        if matches:
            for m in matches:
                label = _norm(m.group("label"))
                value = _norm(m.group("value"))
                # só coletar se label tem cara de label OU valor parece data/período (para não capturar tudo do mundo)
                if _looks_like_label(label) or _value_has_date_like(value):
                    fields.append(RawField(
                        field_key=_classify_field_key(label),
                        label_raw=label,
                        value_raw=value,
                        origin="text",
                        origin_ref=f"line#{li}",
                        section_hint="",
                        evidence_snippet=(label + ": " + value)[:180]
                    ))

    # 3) fallback: qualquer data-like solta (sem label)
    # útil quando o documento não tem "Início:" etc.
    for li, line in enumerate(lines):
        line_n = _norm(line)
        if not line_n:
            continue
        if RE_DATE_LIKE.search(line_n) and not RE_LABEL_COLON.search(line_n):
            fields.append(RawField(
                field_key="data",
                label_raw="",
                value_raw=line_n,
                origin="text",
                origin_ref=f"line#{li}",
                section_hint="",
                evidence_snippet=line_n[:180]
            ))

    return fields

def export_raw_fields_csv(
    out_csv_path: str,
    processo_sei: str,
    doc_title: str,
    doc_url: str,
    raw_fields: List[RawField],
    captured_at: Optional[str] = None
) -> None:
    captured_at = captured_at or datetime.utcnow().isoformat()

    header = [
        "captured_at", "processo_sei", "doc_title", "doc_url",
        "field_key", "label_raw", "value_raw",
        "value_is_empty", "value_has_date_like",
        "origin", "origin_ref", "section_hint", "evidence_snippet"
    ]

    # append mode (vai acumulando vários docs no mesmo CSV)
    write_header = False
    try:
        with open(out_csv_path, "r", encoding="utf-8", newline="") as _:
            pass
    except FileNotFoundError:
        write_header = True

    with open(out_csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if write_header:
            w.writeheader()

        for rf in raw_fields:
            value_raw = rf.value_raw or ""
            w.writerow({
                "captured_at": captured_at,
                "processo_sei": processo_sei,
                "doc_title": doc_title,
                "doc_url": doc_url,
                "field_key": rf.field_key,
                "label_raw": rf.label_raw,
                "value_raw": value_raw,
                "value_is_empty": 1 if not value_raw.strip() else 0,
                "value_has_date_like": _value_has_date_like(value_raw),
                "origin": rf.origin,
                "origin_ref": rf.origin_ref,
                "section_hint": rf.section_hint,
                "evidence_snippet": rf.evidence_snippet
            })