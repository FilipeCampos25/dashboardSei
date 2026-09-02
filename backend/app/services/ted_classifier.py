"""Pure, evidence-led TED document-function classification for V2 shadow data."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Mapping

from app.services.semantic_states import ClassificationState, DocumentFunctionState


TED_CLASS = "ted"
TED_FUNCTION_INSTRUMENT = "ted.instrument"
TED_FUNCTION_WORK_PLAN = "ted.work_plan"
TED_FUNCTION_MEETING_MINUTES = "ted.meeting_minutes"
TED_FUNCTION_AMENDMENT = "ted.amendment"
TED_FUNCTION_EXTRACT = "ted.extract"
TED_FUNCTION_RELATED = "ted.related"


@dataclass(frozen=True)
class TedClassification:
    """Auditable result limited to the existing family-specific V2 labels."""

    classification: str
    function: str
    resolved_class: str | None
    resolved_function: str | None
    reason: str
    evidence_source: str


def _normalized(value: Any) -> str:
    text = " ".join(str(value or "").replace("\r", "\n").split())
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower()


def _table_text(snapshot: Mapping[str, Any]) -> str:
    fragments: list[str] = []
    for table in snapshot.get("tables", []) or []:
        if not isinstance(table, Mapping):
            continue
        for row in table.get("rows", []) or []:
            if isinstance(row, list):
                fragments.extend(str(cell or "") for cell in row)
            else:
                fragments.append(str(row or ""))
    return " ".join(fragments)


def _result(*, classification: ClassificationState, function: DocumentFunctionState, resolved_function: str | None, reason: str, evidence_source: str) -> TedClassification:
    return TedClassification(
        classification=classification.value,
        function=function.value,
        resolved_class=TED_CLASS if resolved_function else None,
        resolved_function=resolved_function,
        reason=reason,
        evidence_source=evidence_source,
    )


def _evidence_source(marker: str, *, text: str, tables: str) -> str:
    return "snapshot.text" if marker in text else "snapshot.tables" if marker in tables else "none"


def classify_ted_snapshot(snapshot: Mapping[str, Any]) -> TedClassification:
    """Classify a TED candidate from document evidence without publication effects.

    Document text and extracted tables are decisive. A tree label/title is only
    sufficient to retain an unconfirmed candidate; it cannot confirm an
    instrument or override a specific document function.
    """

    title = _normalized(snapshot.get("title", ""))
    text = _normalized(snapshot.get("text", ""))
    tables = _normalized(_table_text(snapshot))
    content = " ".join(part for part in (text, tables) if part)
    has_ted_reference = bool(re.search(r"\bted\b", content)) or "termo de execucao descentralizada" in content

    if not content:
        if re.search(r"\bted\b", title) or "termo de execucao descentralizada" in title:
            return _result(
                classification=ClassificationState.CANDIDATE,
                function=DocumentFunctionState.NOT_EVALUATED,
                resolved_function=None,
                reason="ted.title_only_candidate",
                evidence_source="snapshot.title",
            )
        return _result(
            classification=ClassificationState.AMBIGUOUS,
            function=DocumentFunctionState.AMBIGUOUS,
            resolved_function=None,
            reason="ted.no_verifiable_content",
            evidence_source="none",
        )

    # Markers that identify the document itself precede markers commonly
    # mentioned inside it. For example, an amendment may alter a work plan.
    specific_functions = (
        (TED_FUNCTION_EXTRACT, "ted.content.extract", ("extrato",)),
        (TED_FUNCTION_MEETING_MINUTES, "ted.content.meeting_minutes", ("ata de reuniao", "reuniao de trabalho")),
        (TED_FUNCTION_AMENDMENT, "ted.content.amendment", ("termo aditivo", "aditivo ao")),
        (TED_FUNCTION_WORK_PLAN, "ted.content.work_plan", ("plano de trabalho",)),
    )
    for resolved_function, reason, markers in specific_functions:
        matched_marker = next((marker for marker in markers if marker in content), None)
        if matched_marker is not None:
            return _result(
                classification=ClassificationState.RELATED,
                function=DocumentFunctionState.RELATED,
                resolved_function=resolved_function,
                reason=reason,
                evidence_source=_evidence_source(matched_marker, text=text, tables=tables),
            )

    related_markers = ("oficio", "relatorio", "memorando", "despacho", "nota tecnica", "informacao tecnica")
    related_marker = next((marker for marker in related_markers if marker in content), None)
    if related_marker is not None:
        return _result(
            classification=ClassificationState.RELATED,
            function=DocumentFunctionState.RELATED,
            resolved_function=TED_FUNCTION_RELATED,
            reason="ted.content.related_document",
            evidence_source=_evidence_source(related_marker, text=text, tables=tables),
        )

    instrument_markers = ("objeto", "unidade descentralizadora", "unidade descentralizada", "participes", "clausula", "valor global")
    if "termo de execucao descentralizada" in content and any(marker in content for marker in instrument_markers):
        return _result(
            classification=ClassificationState.CONFIRMED,
            function=DocumentFunctionState.INSTRUMENT,
            resolved_function=TED_FUNCTION_INSTRUMENT,
            reason="ted.content.instrument",
            evidence_source="snapshot.text" if "termo de execucao descentralizada" in text else "snapshot.tables",
        )

    if has_ted_reference:
        return _result(
            classification=ClassificationState.AMBIGUOUS,
            function=DocumentFunctionState.AMBIGUOUS,
            resolved_function=None,
            reason="ted.content.insufficient_instrument_evidence",
            evidence_source="snapshot.text_or_tables",
        )

    return _result(
        classification=ClassificationState.AMBIGUOUS,
        function=DocumentFunctionState.AMBIGUOUS,
        resolved_function=None,
        reason="ted.content.unclassified",
        evidence_source="snapshot.text_or_tables",
    )
