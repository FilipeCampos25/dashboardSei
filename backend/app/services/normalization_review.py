from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from app.output import csv_writer


QUEUE_FILENAME = "normalization_review_queue_latest.csv"

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
NOT_FOUND_STATUSES = {"not_found", "search_context_stagnation", "filter_error", "extraction_failure"}

REQUIRED_FIELDS: Dict[str, Sequence[str]] = {
    "pt": ("parceiro", "vigencia_inicio", "vigencia_fim", "objeto"),
    "act": ("numero_acordo", "data_inicio_vigencia", "data_fim_vigencia", "orgao_convenente", "objeto"),
    "ted": (
        "numero_ted",
        "ano_ted",
        "objeto",
        "unidade_descentralizadora",
        "unidade_descentralizada",
        "valor_global",
        "vigencia_inicio",
        "vigencia_fim",
        "plano_aplicacao",
    ),
    "documento_administrativo": ("documento", "resolved_document_type", "assunto", "resumo"),
}

GENERIC_OBJECT_MARKERS = {
    "plano de trabalho",
    "acordo de cooperacao",
    "acordo de cooperação",
    "termo de execucao descentralizada",
    "termo de execução descentralizada",
    "objeto do acordo",
    "objeto do termo",
    "nao informado",
    "não informado",
    "sem objeto",
}

COLUMNS = [
    "code",
    "severity",
    "field",
    "message",
    "suggested_action",
    "document_type",
    "processo",
    "documento",
    "process_id",
    "document_id",
    "candidate_id",
    "source_url",
    "publication_status",
    "validation_status",
    "normalization_status",
    "json_path",
    "is_gold_missing",
    "is_recoverable",
    "is_not_found",
]


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    if logger is None:
        return
    try:
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(msg, *args)
    except Exception:
        return


def _clean_spaces(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _norm(value: Any) -> str:
    text = _clean_spaces(value).lower()
    replacements = str.maketrans({"á": "a", "à": "a", "ã": "a", "â": "a", "é": "e", "ê": "e", "í": "i", "ó": "o", "ô": "o", "õ": "o", "ú": "u", "ç": "c"})
    return text.translate(replacements)


def _read_csv_rows(path: Path, logger: Any = None) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as exc:
        _log(logger, "warning", "Fila de revisao: falha ao ler %s (%s).", path, exc)
        return []
    return [{key: _clean_spaces(value) for key, value in row.items()} for row in df.to_dict(orient="records")]


def _boolish(value: Any) -> bool:
    return _norm(value) in {"true", "1", "sim", "yes"}


def _is_gold(row: Dict[str, str]) -> bool:
    return row.get("publication_status", "") == "published_gold"


def _is_missing(value: Any) -> bool:
    cleaned = _clean_spaces(value)
    if not cleaned:
        return True
    normalized = _norm(cleaned)
    return normalized in {"nan", "none", "null", "na", "n/a", "sem informacao", "sem informação"}


def _is_generic_object(value: Any) -> bool:
    cleaned = _clean_spaces(value)
    normalized = _norm(cleaned)
    alpha_chars = re.findall(r"[A-Za-zÀ-ÿ]", cleaned)
    if len(alpha_chars) < 30:
        return True
    words = re.findall(r"[A-Za-zÀ-ÿ]{3,}", cleaned)
    if len(words) < 5:
        return True
    return normalized in {_norm(marker) for marker in GENERIC_OBJECT_MARKERS}


def _issue(
    *,
    code: str,
    severity: str,
    field: str,
    message: str,
    suggested_action: str,
    document_type: str,
    row: Dict[str, str],
    is_gold_missing: bool = False,
    is_recoverable: bool = True,
    is_not_found: bool = False,
) -> Dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "field": field,
        "message": message,
        "suggested_action": suggested_action,
        "document_type": document_type,
        "processo": row.get("processo", ""),
        "documento": row.get("documento", "") or row.get("chosen_documento", ""),
        "process_id": row.get("process_id", "") or row.get("processo", ""),
        "document_id": row.get("document_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "source_url": row.get("source_url", ""),
        "publication_status": row.get("publication_status", ""),
        "validation_status": row.get("validation_status", ""),
        "normalization_status": row.get("normalization_status", ""),
        "json_path": row.get("json_path", "") or row.get("candidate_json_path", ""),
        "is_gold_missing": bool(is_gold_missing),
        "is_recoverable": bool(is_recoverable),
        "is_not_found": bool(is_not_found),
    }


def _required_field_issues(document_type: str, row: Dict[str, str]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for field in REQUIRED_FIELDS.get(document_type, ()):
        if not _is_missing(row.get(field, "")):
            continue
        gold_missing = _is_gold(row)
        issues.append(
            _issue(
                code="required_field_missing",
                severity="high" if gold_missing else "medium",
                field=field,
                message=f"Campo obrigatorio ausente em {document_type}: {field}.",
                suggested_action="Revisar o documento canonico e preencher ou confirmar ausencia real do campo.",
                document_type=document_type,
                row=row,
                is_gold_missing=gold_missing,
            )
        )
    return issues


def _preview_or_fallback_issues(document_type: str, row: Dict[str, str], source_fields: Iterable[str]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for source_field in source_fields:
        source = row.get(source_field, "")
        if source not in {"preview", "fallback", "preview_fallback"}:
            continue
        field = source_field
        for prefix in ("field_source_", "source_", "best_"):
            if field.startswith(prefix):
                field = field[len(prefix) :]
        if field.endswith("_source"):
            field = field[: -len("_source")]
        issues.append(
            _issue(
                code="preview_or_fallback_only",
                severity="medium",
                field=field,
                message=f"Dado de {field} veio apenas de {source}.",
                suggested_action="Conferir o valor no documento aberto antes de publicar como dado confirmado.",
                document_type=document_type,
                row=row,
            )
        )
    return issues


def _not_found_issue(document_type: str, row: Dict[str, str], *, administrative: bool = False) -> Dict[str, Any] | None:
    status = row.get("validation_status", "") or row.get("normalization_status", "")
    if status not in NOT_FOUND_STATUSES:
        return None
    return _issue(
        code="administrative_document_not_found" if administrative else "document_not_found",
        severity="high" if administrative else "medium",
        field="documento",
        message=(
            "Documento administrativo nao encontrado."
            if administrative
            else f"Documento {document_type} nao encontrado ou nao extraido."
        ),
        suggested_action="Reexecutar a busca com termos alternativos ou revisar manualmente a arvore do processo.",
        document_type=document_type,
        row=row,
        is_recoverable=False,
        is_not_found=True,
    )


def _pt_issues(rows: List[Dict[str, str]], status_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for row in rows:
        issues.extend(_required_field_issues("pt", row))
        if row.get("period_source") == "missing_period" or (
            row.get("vigencia_raw") and (not row.get("vigencia_inicio") or not row.get("vigencia_fim"))
        ):
            issues.append(
                _issue(
                    code="validity_without_base_date",
                    severity="high" if _is_gold(row) else "medium",
                    field="vigencia",
                    message="Vigencia encontrada sem datas normalizadas de inicio e fim.",
                    suggested_action="Localizar a data base da vigencia ou registrar que a regra depende de assinatura/publicacao.",
                    document_type="pt",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if row.get("period_source") == "unresolved_relative" or "relativo" in _norm(row.get("period_warning", "")):
            issues.append(
                _issue(
                    code="unresolved_relative_deadline",
                    severity="high" if _is_gold(row) else "medium",
                    field="prazo",
                    message="Prazo relativo nao resolvido para uma data calendario.",
                    suggested_action="Conferir assinatura/publicacao/aprovacao usada como ancora do prazo.",
                    document_type="pt",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if row.get("classification_reason") == "pt_minuta_documentacao" or "minuta" in _norm(row.get("classification_reason", "")):
            issues.append(
                _issue(
                    code="draft_document",
                    severity="high",
                    field="doc_class",
                    message="Documento classificado como minuta/documentacao.",
                    suggested_action="Buscar a versao assinada/final do documento.",
                    document_type="pt",
                    row=row,
                )
            )
        if _is_generic_object(row.get("objeto", "")):
            issues.append(
                _issue(
                    code="object_too_short_or_generic",
                    severity="medium",
                    field="objeto",
                    message="Objeto ausente, muito curto ou generico.",
                    suggested_action="Revisar a clausula/section de objeto no documento.",
                    document_type="pt",
                    row=row,
                )
            )
        issues.extend(_preview_or_fallback_issues("pt", row, ("field_source_objeto", "field_source_vigencia", "field_source_parceiro")))
    for row in status_rows:
        issue = _not_found_issue("pt", row)
        if issue:
            issues.append(issue)
    return issues


def _act_issues(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for row in rows:
        issues.extend(_required_field_issues("act", row))
        doc_class = row.get("doc_class", "")
        if doc_class == "minuta" or "minuta" in _norm(row.get("discard_reason", "")):
            issues.append(
                _issue(
                    code="draft_document",
                    severity="high",
                    field="doc_class",
                    message="Documento classificado como minuta.",
                    suggested_action="Buscar o ACT final assinado.",
                    document_type="act",
                    row=row,
                )
            )
        if doc_class == "termo_aditivo":
            issues.append(
                _issue(
                    code="amendment_confused_with_main",
                    severity="high",
                    field="doc_class",
                    message="Termo aditivo foi confundido com documento principal.",
                    suggested_action="Usar o termo principal como canonico e manter o aditivo como relacionado.",
                    document_type="act",
                    row=row,
                )
            )
        if row.get("validation_status") in {"related_but_not_canonical", "related_but_not_requested", "rejected_snapshot"} or row.get(
            "normalization_status"
        ) in {"descartado_nao_canonico", "descartado_por_desempate"}:
            issues.append(
                _issue(
                    code="related_not_canonical",
                    severity="medium",
                    field="validation_status",
                    message="Documento relacionado, mas nao canonico para o tipo solicitado.",
                    suggested_action="Conferir se existe documento final/canonico melhor no processo.",
                    document_type="act",
                    row=row,
                )
            )
        if row.get("process_alignment_status") not in {"", "aligned", "external_reference_accepted"}:
            issues.append(
                _issue(
                    code="process_mismatch",
                    severity="high",
                    field="processo",
                    message="Processo do documento diverge do processo pesquisado.",
                    suggested_action="Validar se o documento pertence ao processo ou se e apenas referencia cruzada.",
                    document_type="act",
                    row=row,
                )
            )
        affinity_status = row.get("affinity_status", "")
        if _is_gold(row) and affinity_status in {"related_document", "ambiguous", "probable_external_document"}:
            severity = "high" if affinity_status == "probable_external_document" else "medium"
            issues.append(
                _issue(
                    code="act_affinity_shadow_review",
                    severity=severity,
                    field="affinity_status",
                    message=f"Gold atual sinalizado pelo diagnostico sombra de afinidade: {affinity_status}.",
                    suggested_action="Revisar origem, juntada e relacao com o processo atual antes de ativar retencao.",
                    document_type="act",
                    row=row,
                    is_gold_missing=True,
                )
            )
        warning = _norm(row.get("vigencia_warning", "") + " " + row.get("validation_warning", ""))
        if "sem_data" in warning or "sem data" in warning:
            issues.append(
                _issue(
                    code="validity_without_base_date",
                    severity="high" if _is_gold(row) else "medium",
                    field="vigencia",
                    message="Vigencia depende de data base nao localizada.",
                    suggested_action="Localizar data de assinatura/publicacao usada como ancora da vigencia.",
                    document_type="act",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if row.get("vigencia_rule_anchor") and (not row.get("data_inicio_vigencia") or not row.get("data_fim_vigencia")):
            issues.append(
                _issue(
                    code="unresolved_relative_deadline",
                    severity="high" if _is_gold(row) else "medium",
                    field="vigencia",
                    message="Regra relativa de vigencia nao foi resolvida para datas finais.",
                    suggested_action="Conferir a data ancora e recalcular a vigencia.",
                    document_type="act",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if _is_generic_object(row.get("objeto", "")):
            issues.append(
                _issue(
                    code="object_too_short_or_generic",
                    severity="medium",
                    field="objeto",
                    message="Objeto ausente, muito curto ou generico.",
                    suggested_action="Revisar a clausula de objeto no ACT.",
                    document_type="act",
                    row=row,
                )
            )
        issues.extend(_preview_or_fallback_issues("act", row, ("field_source_objeto", "field_source_vigencia", "field_source_gestao")))
    return issues


def _ted_issues(rows: List[Dict[str, str]], status_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for row in rows:
        issues.extend(_required_field_issues("ted", row))
        if _is_missing(row.get("valor_global", "")):
            issues.append(
                _issue(
                    code="ted_missing_financial_value",
                    severity="high",
                    field="valor_global",
                    message="TED sem valor financeiro normalizado.",
                    suggested_action="Revisar tabela ou texto de valor global do TED.",
                    document_type="ted",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if _is_missing(row.get("plano_aplicacao", "")):
            issues.append(
                _issue(
                    code="ted_without_application_plan",
                    severity="high",
                    field="plano_aplicacao",
                    message="TED encontrado sem plano de aplicacao.",
                    suggested_action="Conferir se o plano esta em tabela, anexo ou documento relacionado.",
                    document_type="ted",
                    row=row,
                    is_gold_missing=_is_gold(row),
                )
            )
        if _is_generic_object(row.get("objeto", "")):
            issues.append(
                _issue(
                    code="object_too_short_or_generic",
                    severity="medium",
                    field="objeto",
                    message="Objeto do TED ausente, muito curto ou generico.",
                    suggested_action="Revisar a secao de objeto do TED.",
                    document_type="ted",
                    row=row,
                )
            )
        issues.extend(
            _preview_or_fallback_issues(
                "ted",
                row,
                (
                    "objeto_source",
                    "valor_global_source",
                    "vigencia_inicio_source",
                    "vigencia_fim_source",
                    "plano_aplicacao_source",
                ),
            )
        )
    for row in status_rows:
        issue = _not_found_issue("ted", row)
        if issue:
            issues.append(issue)
    return issues


def _administrative_issues(rows: List[Dict[str, str]], status_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for row in rows:
        issues.extend(_required_field_issues("documento_administrativo", row))
        if row.get("validation_status") in {"related_but_not_canonical", "related_but_not_requested", "rejected_snapshot"}:
            issues.append(
                _issue(
                    code="related_not_canonical",
                    severity="medium",
                    field="validation_status",
                    message="Documento administrativo relacionado, mas nao canonico.",
                    suggested_action="Conferir a funcao administrativa e selecionar o documento correto.",
                    document_type="documento_administrativo",
                    row=row,
                )
            )
    for row in status_rows:
        issue = _not_found_issue("documento_administrativo", row, administrative=True)
        if issue:
            issues.append(issue)
    return issues


def _dashboard_issues(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for row in rows:
        if _boolish(row.get("has_process_mismatch", "")):
            issues.append(
                _issue(
                    code="process_mismatch",
                    severity="high",
                    field="processo",
                    message="Dashboard marcou divergencia entre processo pesquisado e documento escolhido.",
                    suggested_action="Validar o documento canonico escolhido para o processo.",
                    document_type="dashboard",
                    row=row,
                )
            )
        issues.extend(
            _preview_or_fallback_issues(
                "dashboard",
                row,
                ("source_act_objeto", "source_act_parceiro", "best_numero_acordo_source", "best_parceiro_source", "best_vigencia_source", "best_objeto_source"),
            )
        )
        if _is_generic_object(row.get("best_objeto", "")):
            issues.append(
                _issue(
                    code="object_too_short_or_generic",
                    severity="medium",
                    field="best_objeto",
                    message="Objeto consolidado ausente, curto ou generico.",
                    suggested_action="Revisar fontes PT/ACT/preview usadas para consolidar o objeto.",
                    document_type="dashboard",
                    row=row,
                )
            )
        if row.get("best_vigencia_source") == "preview_fallback" and (not row.get("best_vigencia_inicio") or not row.get("best_vigencia_fim")):
            issues.append(
                _issue(
                    code="validity_without_base_date",
                    severity="high",
                    field="best_vigencia",
                    message="Vigencia consolidada veio da previa e nao possui datas base.",
                    suggested_action="Revisar PT/ACT para obter datas normalizadas.",
                    document_type="dashboard",
                    row=row,
                    is_gold_missing=_boolish(row.get("pt_gold", "")) or _boolish(row.get("act_gold", "")),
                )
            )
    return issues


def _dedupe(issues: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for issue in issues:
        key = (
            issue.get("code", ""),
            issue.get("document_type", ""),
            issue.get("processo", ""),
            issue.get("documento", ""),
            issue.get("document_id", ""),
            issue.get("candidate_id", ""),
            issue.get("source_url", ""),
            issue.get("field", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped


def _sort_key(issue: Dict[str, Any]) -> tuple:
    return (
        SEVERITY_RANK.get(str(issue.get("severity", "")), 9),
        0 if issue.get("is_gold_missing") else 1,
        0 if issue.get("is_recoverable") else 1,
        1 if issue.get("is_not_found") else 0,
        str(issue.get("processo", "")),
        str(issue.get("document_type", "")),
        str(issue.get("code", "")),
        str(issue.get("field", "")),
    )


def collect_review_issues(output_dir: Path | str, logger: Any = None) -> List[Dict[str, Any]]:
    output_path = Path(output_dir)
    issues: List[Dict[str, Any]] = []
    issues.extend(_pt_issues(_read_csv_rows(output_path / "pt_normalizado_latest.csv", logger), _read_csv_rows(output_path / "pt_status_execucao_latest.csv", logger)))
    issues.extend(_act_issues(_read_csv_rows(output_path / "act_classificacao_latest.csv", logger)))
    issues.extend(_ted_issues(_read_csv_rows(output_path / "ted_normalizado_latest.csv", logger), _read_csv_rows(output_path / "ted_status_execucao_latest.csv", logger)))
    issues.extend(
        _administrative_issues(
            _read_csv_rows(output_path / "documento_administrativo_normalizado_latest.csv", logger),
            _read_csv_rows(output_path / "memorando_status_execucao_latest.csv", logger),
        )
    )
    issues.extend(_dashboard_issues(_read_csv_rows(output_path / "dashboard_ready_latest.csv", logger)))
    return sorted(_dedupe(issues), key=_sort_key)


def export_review_queue(output_dir: Path | str, logger: Any = None) -> Dict[str, Any]:
    output_path = csv_writer.ensure_output_dir(output_dir)
    issues = collect_review_issues(output_path, logger=logger)
    csv_path = output_path / QUEUE_FILENAME
    csv_writer.write_csv([{column: issue.get(column, "") for column in COLUMNS} for issue in issues], csv_path, columns=COLUMNS)
    _log(logger, "info", "Fila de revisao de normalizacao gerada: registros=%d latest=%s.", len(issues), csv_path)
    return {"records": len(issues), "latest_path": csv_path}
