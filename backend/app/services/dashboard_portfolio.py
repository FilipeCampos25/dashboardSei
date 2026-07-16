from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd

from .dashboard_data import empty_dataframe
from .dashboard_metrics import (
    add_deadline_columns,
    clean_spaces,
    coverage_rows,
    format_date,
    money_to_float,
    parse_date,
    summarize_text,
)


PROCESSO_RE = re.compile(r"^\d{5}\.\d{6}/\d{4}-\d{2}$")

SITUACAO_ATIVA = "ativa_em_acompanhamento"
SITUACAO_HISTORICO_ENCERRADO = "historico_encerrado"
SITUACAO_HISTORICO_DESCONTINUADO = "historico_descontinuado"
SITUACAO_HISTORICO_NAO_REALIZADO = "historico_nao_realizado"
SITUACAO_HISTORICO_VENCIDO = "historico_vencido"
SITUACAO_HISTORICO_VIGENTE = "historico_vigente"
SITUACAO_REVISAR = "inconsistente_ou_revisar"


def normalize_processo(value: Any) -> str:
    cleaned = clean_spaces(value)
    if not cleaned:
        return ""
    compact = re.sub(r"\s+", "", cleaned).translate(
        str.maketrans(
            {
                "\u2010": "-",
                "\u2011": "-",
                "\u2012": "-",
                "\u2013": "-",
                "\u2014": "-",
                "\u2212": "-",
                "\ufe58": "-",
                "\ufe63": "-",
                "\uff0d": "-",
                "\u2044": "/",
                "\u2215": "/",
                "\uff0f": "/",
            }
        )
    )
    if PROCESSO_RE.fullmatch(compact):
        return compact
    digits = re.sub(r"\D", "", compact)
    if len(digits) == 17:
        return f"{digits[:5]}.{digits[5:11]}/{digits[11:15]}-{digits[15:]}"
    return compact


def _boolish(value: Any) -> bool:
    return clean_spaces(value).lower() in {"1", "true", "sim", "yes"}


def _records_by_process(df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if df.empty or "processo" not in df.columns:
        return {}
    rows: dict[str, list[dict[str, Any]]] = {}
    for record in df.to_dict(orient="records"):
        processo = normalize_processo(record.get("processo", ""))
        if processo:
            rows.setdefault(processo, []).append(record)
    return rows


def _first_by_process(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {processo: rows[0] for processo, rows in _records_by_process(df).items() if rows}


def _split_items(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").split("||") if clean_spaces(part)]


def _pt_metrics(pt_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    if pt_df.empty or "processo" not in pt_df.columns:
        return metrics
    for processo, rows in _records_by_process(pt_df).items():
        metas_count = sum(len(_split_items(row.get("metas_raw", ""))) for row in rows)
        acoes_count = sum(len(_split_items(row.get("acoes_raw", ""))) for row in rows)
        prazo_values = [row.get("prazo_fim", "") for row in rows if not pd.isna(parse_date(row.get("prazo_fim", "")))]
        prazo_values = sorted(prazo_values, key=parse_date)
        metrics[processo] = {
            "possui_pt": True,
            "quantidade_metas_pt": metas_count,
            "quantidade_acoes_pt": acoes_count,
            "quantidade_prazos_pt": len(prazo_values),
            "proximo_prazo_pt": format_date(prazo_values[0]) if prazo_values else "",
            "pt_linhas": rows,
        }
    return metrics


def _document_number_ted(row: dict[str, Any]) -> str:
    numero = clean_spaces(row.get("numero_ted", ""))
    ano = clean_spaces(row.get("ano_ted", ""))
    if numero and ano:
        return f"{numero}/{ano}"
    return numero or ano


def _canonical_key(processo: str, document_type: str, number: str) -> str:
    normalized_number = re.sub(r"[^A-Za-z0-9]+", "", clean_spaces(number)).upper()
    if document_type == "TED":
        normalized_number = normalized_number or "TED"
    else:
        normalized_number = normalized_number or "PARCERIA"
    return f"{processo}|{document_type}|{normalized_number}"


def _status_from_history_category(category: Any) -> str:
    normalized = clean_spaces(category).lower()
    if normalized == "encerrado":
        return SITUACAO_HISTORICO_ENCERRADO
    if normalized == "nao_realizado":
        return SITUACAO_HISTORICO_NAO_REALIZADO
    if normalized == "vencido":
        return SITUACAO_HISTORICO_VENCIDO
    if normalized == "vigente":
        return SITUACAO_HISTORICO_VIGENTE
    if normalized == "descontinuado":
        return SITUACAO_HISTORICO_DESCONTINUADO
    return SITUACAO_REVISAR


def _missing_fields(record: dict[str, Any]) -> list[str]:
    fields = []
    required_fields = (
        ("parceiro", "parceiro"),
        ("objeto_completo", "objeto"),
        ("vigencia_fim", "vigencia_fim"),
        ("documento_principal_numero", "documento_principal_numero"),
    )
    for source_field, field_label in required_fields:
        if not clean_spaces(record.get(source_field, "")):
            fields.append(field_label)
    if record.get("possui_ted") and money_to_float(record.get("valor_global_ted", "")) == 0.0:
        fields.append("valor_global_ted")
    return fields


def _sources_for_record(record: dict[str, Any], overview: dict[str, Any], ted_row: dict[str, Any]) -> str:
    sources = []
    for field in ("best_numero_acordo_source", "best_parceiro_source", "best_vigencia_source", "best_objeto_source"):
        value = clean_spaces(overview.get(field, ""))
        if value:
            sources.append(f"{field.replace('best_', '').replace('_source', '')}:{value}")
    if ted_row:
        sources.append("ted:ted_normalizado")
    if record.get("possui_pt"):
        sources.append("pt:pt_auditoria")
    if record.get("possui_memorando"):
        sources.append("memorando:memorando_normalizado")
    return "; ".join(dict.fromkeys(sources))


def build_canonical_portfolio(bundle: dict[str, Any], *, today: date | datetime | pd.Timestamp | None = None) -> pd.DataFrame:
    overview_df = bundle.get("overview", empty_dataframe([]))
    ted_by_process = _first_by_process(bundle.get("ted_normalized", empty_dataframe([])))
    pt_metrics = _pt_metrics(bundle.get("pt_audit", empty_dataframe([])))
    memorando_by_process = _records_by_process(bundle.get("memorando_normalized", empty_dataframe([])))
    admin_by_process = _records_by_process(bundle.get("admin_normalized", empty_dataframe([])))
    history_by_process = _records_by_process(bundle.get("parcerias_descontinuadas", empty_dataframe([])))
    divergence_by_process = _first_by_process(bundle.get("divergence", empty_dataframe([])))
    collection_date = clean_spaces(bundle.get("collection_meta", {}).get("data_ultima_coleta", ""))

    rows: list[dict[str, Any]] = []
    for overview in overview_df.to_dict(orient="records"):
        processo = normalize_processo(overview.get("processo", ""))
        if not processo:
            continue
        ted_row = ted_by_process.get(processo, {})
        divergence = divergence_by_process.get(processo, {})
        source_universe = clean_spaces(overview.get("source_universe", "")) or clean_spaces(divergence.get("source_universe", ""))
        exported_type_raw = overview.get("documento_principal_tipo", "")
        exported_number_raw = overview.get("documento_principal_numero", "")
        exported_document_type = "" if pd.isna(exported_type_raw) else clean_spaces(exported_type_raw)
        exported_document_number = "" if pd.isna(exported_number_raw) else clean_spaces(exported_number_raw)
        if exported_document_type:
            document_type = exported_document_type
            document_number = exported_document_number
            ted_principal = document_type == "TED"
        else:
            # Compatibilidade com dashboard_ready gerado antes do contrato de documento principal.
            ted_principal = bool(source_universe == "ted_normalizado" or (ted_row and not clean_spaces(overview.get("best_numero_acordo", "")) and _boolish(overview.get("ted_gold", ""))))
            document_type = "TED" if ted_principal else "ACT"
            document_number = _document_number_ted(ted_row) if ted_principal else clean_spaces(overview.get("best_numero_acordo", ""))
            if not document_number and not ted_principal:
                document_type = "Parceria"

        parceiro = clean_spaces(overview.get("best_parceiro", ""))
        if not parceiro and ted_principal:
            parceiro = clean_spaces(ted_row.get("unidade_descentralizada", ""))
        objeto = clean_spaces(overview.get("best_objeto", ""))
        if not objeto and ted_principal:
            objeto = clean_spaces(ted_row.get("objeto", ""))
        vigencia_inicio = format_date(overview.get("best_vigencia_inicio", "")) or format_date(ted_row.get("vigencia_inicio", ""))
        vigencia_fim = format_date(overview.get("best_vigencia_fim", "")) or format_date(ted_row.get("vigencia_fim", ""))
        data_assinatura = format_date(overview.get("best_data_assinatura", ""))

        pt_info = pt_metrics.get(processo, {})
        mem_rows = memorando_by_process.get(processo, [])
        admin_rows = admin_by_process.get(processo, [])
        documentos_relacionados = []
        if clean_spaces(overview.get("act_quality", "")) not in {"", "not_found"}:
            documentos_relacionados.append("ACT")
        if pt_info:
            documentos_relacionados.append("PT")
        if ted_row:
            documentos_relacionados.append("TED")
        if mem_rows:
            documentos_relacionados.append("Memorando")

        conflicts = []
        if processo in history_by_process:
            statuses = sorted({clean_spaces(row.get("status_categoria", "")) for row in history_by_process[processo]})
            conflicts.append(f"processo_tambem_no_historico={','.join(statuses)}")
        if _boolish(overview.get("has_process_mismatch", "")):
            conflicts.append("process_mismatch")
        if clean_spaces(divergence.get("process_key_valid", "")).lower() == "false":
            conflicts.append("processo_invalido")
        for raw in (overview.get("normalization_issues", ""), overview.get("quality_notes", "")):
            if clean_spaces(raw):
                conflicts.append(clean_spaces(raw))

        row = {
            "processo": processo,
            "processo_normalizado": processo,
            "documento_principal_tipo": document_type,
            "documento_principal_numero": document_number,
            "documentos_relacionados": "; ".join(dict.fromkeys(documentos_relacionados)),
            "parceiro": parceiro,
            "objeto_resumo": summarize_text(objeto, 150),
            "objeto_completo": objeto,
            "vigencia_inicio": vigencia_inicio,
            "vigencia_fim": vigencia_fim,
            "data_assinatura": data_assinatura,
            "possui_pt": bool(pt_info),
            "quantidade_metas_pt": int(pt_info.get("quantidade_metas_pt", 0) or 0),
            "quantidade_acoes_pt": int(pt_info.get("quantidade_acoes_pt", 0) or 0),
            "quantidade_prazos_pt": int(pt_info.get("quantidade_prazos_pt", 0) or 0),
            "proximo_prazo_pt": pt_info.get("proximo_prazo_pt", ""),
            "possui_ted": bool(ted_row),
            "valor_global_ted": clean_spaces(ted_row.get("valor_global", "")),
            "valor_global_ted_num": money_to_float(ted_row.get("valor_global", "")),
            "unidade_descentralizadora": clean_spaces(ted_row.get("unidade_descentralizadora", "")),
            "unidade_descentralizada": clean_spaces(ted_row.get("unidade_descentralizada", "")),
            "possui_memorando": bool(mem_rows),
            "pendencias_ou_acoes_abertas": "; ".join(
                summarize_text(row.get("acao_solicitada", "") or row.get("prazo", "") or row.get("resumo", ""), 140)
                for row in admin_rows[:3]
                if clean_spaces(row.get("acao_solicitada", "") or row.get("prazo", "") or row.get("resumo", ""))
            ),
            "qualidade_do_registro": clean_spaces(overview.get("quality_status", "")) or "low",
            "data_ultima_coleta": collection_date,
            "conflitos": "; ".join(dict.fromkeys(conflicts)),
        }
        row["campos_ausentes"] = ", ".join(_missing_fields(row))
        row["fontes_de_origem"] = _sources_for_record(row, overview, ted_row)
        row["chave_canonica"] = _canonical_key(processo, document_type, document_number)
        row["situacao_carteira"] = SITUACAO_REVISAR if processo in history_by_process or "processo_invalido" in conflicts else SITUACAO_ATIVA
        rows.append(row)

    portfolio = pd.DataFrame(rows)
    if portfolio.empty:
        return empty_dataframe(
            [
                "processo",
                "processo_normalizado",
                "chave_canonica",
                "situacao_carteira",
                "documento_principal_tipo",
                "documento_principal_numero",
                "documentos_relacionados",
                "parceiro",
                "objeto_resumo",
                "objeto_completo",
                "vigencia_inicio",
                "vigencia_fim",
                "data_assinatura",
                "dias_restantes",
                "indicador_vigencia",
                "possui_pt",
                "quantidade_metas_pt",
                "quantidade_acoes_pt",
                "quantidade_prazos_pt",
                "proximo_prazo_pt",
                "possui_ted",
                "valor_global_ted",
                "valor_global_ted_num",
                "unidade_descentralizadora",
                "unidade_descentralizada",
                "possui_memorando",
                "pendencias_ou_acoes_abertas",
                "qualidade_do_registro",
                "campos_ausentes",
                "fontes_de_origem",
                "data_ultima_coleta",
                "conflitos",
            ]
        )
    portfolio = add_deadline_columns(portfolio, "vigencia_fim", today=today)
    portfolio["indicador_prazo"] = portfolio["indicador_vigencia"]
    portfolio = _deduplicate_portfolio(portfolio)
    return portfolio


def _deduplicate_portfolio(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty or "chave_canonica" not in portfolio.columns:
        return portfolio
    rows: list[dict[str, Any]] = []
    for _, group in portfolio.groupby("chave_canonica", sort=False):
        record = dict(group.iloc[0])
        if len(group) > 1:
            conflicts = [clean_spaces(record.get("conflitos", "")), f"duplicidade_canonica={len(group)}"]
            record["conflitos"] = "; ".join(part for part in conflicts if part)
            record["situacao_carteira"] = SITUACAO_REVISAR
        rows.append(record)
    return pd.DataFrame(rows)


def build_history_dataframe(bundle: dict[str, Any]) -> pd.DataFrame:
    hist_df = bundle.get("parcerias_descontinuadas", empty_dataframe([]))
    active_processes = {
        normalize_processo(value)
        for value in bundle.get("overview", empty_dataframe([])).get("processo", pd.Series(dtype=str)).tolist()
        if normalize_processo(value)
    }
    rows: list[dict[str, Any]] = []
    for record in hist_df.to_dict(orient="records"):
        processo = normalize_processo(record.get("processo", ""))
        status_category = clean_spaces(record.get("status_categoria", ""))
        status_calculado = clean_spaces(record.get("status_calculado", "")) or clean_spaces(record.get("status_normalizado", ""))
        situacao = _status_from_history_category(status_category)
        conflicts = []
        if processo in active_processes:
            situacao = SITUACAO_REVISAR
            conflicts.append("processo_tambem_na_carteira_ativa")
        if clean_spaces(record.get("status_calculado", "")):
            normalized_raw = clean_spaces(record.get("status_normalizado", ""))
            if normalized_raw and normalized_raw.casefold() != status_calculado.casefold():
                conflicts.append("status_raw_diverge_do_calculado")
        elif status_category in {"vigente_em_descontinuadas", "sem_status", ""}:
            situacao = SITUACAO_REVISAR
            conflicts.append(f"status_historico={status_category or 'vazio'}")
        rows.append(
            {
                "processo": processo,
                "tipo": clean_spaces(record.get("tipo", "")),
                "numero_act": clean_spaces(record.get("numero_act", "")),
                "parceiro": clean_spaces(record.get("parceiro", "")),
                "objeto_resumo": summarize_text(record.get("objeto", ""), 150),
                "objeto_completo": clean_spaces(record.get("objeto", "")),
                "data_assinatura": format_date(record.get("data_assinatura", "")),
                "data_vencimento": format_date(record.get("data_vencimento", "")),
                "status_normalizado": clean_spaces(record.get("status_normalizado", "")),
                "status_raw": clean_spaces(record.get("status_raw", "")),
                "status_calculado": status_calculado,
                "status_categoria": status_category,
                "status_evidencia": clean_spaces(record.get("status_evidencia", "")),
                "status_data_referencia": format_date(record.get("status_data_referencia", "")),
                "situacao_carteira": situacao,
                "conflitos": "; ".join(conflicts),
                "missing_fields": clean_spaces(record.get("missing_fields", "")),
            }
        )
    return pd.DataFrame(rows)


def build_ted_dataframe(bundle: dict[str, Any], *, today: date | datetime | pd.Timestamp | None = None) -> pd.DataFrame:
    ted_df = bundle.get("ted_normalized", empty_dataframe([])).copy()
    if ted_df.empty:
        return empty_dataframe(
            [
                "processo",
                "numero_ted",
                "ano_ted",
                "objeto_resumo",
                "objeto_completo",
                "valor_global",
                "valor_global_num",
                "vigencia_inicio",
                "vigencia_fim",
                "dias_restantes",
                "indicador_vigencia",
                "unidade_descentralizadora",
                "unidade_descentralizada",
                "quality_status",
                "normalization_status",
                "campos_ausentes",
            ]
        )
    rows = []
    for record in ted_df.to_dict(orient="records"):
        missing = [
            field
            for field in ("valor_global", "vigencia_fim", "unidade_descentralizadora")
            if not clean_spaces(record.get(field, ""))
        ]
        rows.append(
            {
                "processo": normalize_processo(record.get("processo", "")),
                "numero_ted": clean_spaces(record.get("numero_ted", "")),
                "ano_ted": clean_spaces(record.get("ano_ted", "")),
                "objeto_resumo": summarize_text(record.get("objeto", ""), 150),
                "objeto_completo": clean_spaces(record.get("objeto", "")),
                "valor_global": clean_spaces(record.get("valor_global", "")),
                "valor_global_num": money_to_float(record.get("valor_global", "")),
                "vigencia_inicio": format_date(record.get("vigencia_inicio", "")),
                "vigencia_fim": format_date(record.get("vigencia_fim", "")),
                "unidade_descentralizadora": clean_spaces(record.get("unidade_descentralizadora", "")),
                "unidade_descentralizada": clean_spaces(record.get("unidade_descentralizada", "")),
                "quality_status": clean_spaces(record.get("quality_status", "")),
                "normalization_status": clean_spaces(record.get("normalization_status", "")),
                "campos_ausentes": ", ".join(missing),
            }
        )
    return add_deadline_columns(pd.DataFrame(rows), "vigencia_fim", today=today)


def build_pt_dataframe(bundle: dict[str, Any], *, today: date | datetime | pd.Timestamp | None = None) -> pd.DataFrame:
    pt_df = bundle.get("pt_audit", empty_dataframe([])).copy()
    if pt_df.empty:
        return empty_dataframe(
            [
                "processo",
                "documento",
                "parceiro",
                "objeto_resumo",
                "vigencia_fim",
                "prazo_fim",
                "indicador_vigencia",
                "indicador_prazo_pt",
                "metas_count",
                "acoes_count",
                "possui_metas",
                "possui_acoes",
                "possui_prazo",
                "publication_status",
                "normalization_status",
            ]
        )
    rows = []
    for record in pt_df.to_dict(orient="records"):
        metas_count = len(_split_items(record.get("metas_raw", "")))
        acoes_count = len(_split_items(record.get("acoes_raw", "")))
        rows.append(
            {
                "processo": normalize_processo(record.get("processo", "")),
                "documento": clean_spaces(record.get("documento", "")),
                "parceiro": clean_spaces(record.get("parceiro", "")),
                "objeto_resumo": summarize_text(record.get("objeto", ""), 150),
                "objeto_completo": clean_spaces(record.get("objeto", "")),
                "vigencia_inicio": format_date(record.get("vigencia_inicio", "")),
                "vigencia_fim": format_date(record.get("vigencia_fim", "")),
                "prazo_inicio": format_date(record.get("prazo_inicio", "")),
                "prazo_fim": format_date(record.get("prazo_fim", "")),
                "metas_count": metas_count,
                "acoes_count": acoes_count,
                "possui_metas": metas_count > 0,
                "possui_acoes": acoes_count > 0,
                "possui_prazo": bool(format_date(record.get("prazo_fim", ""))),
                "publication_status": clean_spaces(record.get("publication_status", "")),
                "normalization_status": clean_spaces(record.get("normalization_status", "")),
                "period_source": clean_spaces(record.get("period_source", "")),
                "period_warning": clean_spaces(record.get("period_warning", "")),
                "metas_raw": clean_spaces(record.get("metas_raw", "")),
                "acoes_raw": clean_spaces(record.get("acoes_raw", "")),
            }
        )
    result = add_deadline_columns(pd.DataFrame(rows), "vigencia_fim", today=today)
    result = add_deadline_columns(result, "prazo_fim", today=today, indicator_column="indicador_prazo_pt")
    return result


def active_portfolio(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty or "situacao_carteira" not in portfolio.columns:
        return portfolio.copy()
    return portfolio[portfolio["situacao_carteira"] == SITUACAO_ATIVA].copy()


def priority_dataframe(active_df: pd.DataFrame) -> pd.DataFrame:
    if active_df.empty:
        return active_df.copy()
    order = {"vermelho": 0, "amarelo": 1, "sem_data": 2, "verde": 3}
    result = active_df.copy()
    result["_status_order"] = result["indicador_vigencia"].map(order).fillna(9)
    result["_days_order"] = result["dias_restantes"].apply(lambda value: 10**9 if pd.isna(value) else int(value))
    result = result.sort_values(["_status_order", "_days_order", "processo"], ascending=[True, True, True])
    return result.drop(columns=["_status_order", "_days_order"])


def filter_portfolio(
    df: pd.DataFrame,
    *,
    query: str = "",
    indicadores: Iterable[str] | None = None,
    documentos: Iterable[str] | None = None,
    parceiros: Iterable[str] | None = None,
    has_pt: str = "Todos",
    has_ted: str = "Todos",
) -> pd.DataFrame:
    filtered = df.copy()
    normalized_query = clean_spaces(query).lower()
    if normalized_query:
        searchable = [
            column
            for column in (
                "processo",
                "documento_principal_tipo",
                "documento_principal_numero",
                "parceiro",
                "objeto_resumo",
                "objeto_completo",
            )
            if column in filtered.columns
        ]
        mask = pd.Series(False, index=filtered.index)
        for column in searchable:
            mask = mask | filtered[column].astype(str).str.lower().str.contains(normalized_query, na=False, regex=False)
        filtered = filtered[mask]
    selected = {clean_spaces(value) for value in (indicadores or []) if clean_spaces(value)}
    if selected and "indicador_vigencia" in filtered.columns:
        filtered = filtered[filtered["indicador_vigencia"].isin(selected)]
    selected_docs = {clean_spaces(value) for value in (documentos or []) if clean_spaces(value)}
    if selected_docs and "documento_principal_tipo" in filtered.columns:
        filtered = filtered[filtered["documento_principal_tipo"].isin(selected_docs)]
    selected_partners = {clean_spaces(value) for value in (parceiros or []) if clean_spaces(value)}
    if selected_partners and "parceiro" in filtered.columns:
        filtered = filtered[filtered["parceiro"].isin(selected_partners)]

    def apply_presence(source: pd.DataFrame, column: str, mode: str) -> pd.DataFrame:
        normalized = clean_spaces(mode).lower()
        if normalized == "com":
            return source[source[column]]
        if normalized == "sem":
            return source[~source[column]]
        return source

    if "possui_pt" in filtered.columns:
        filtered = apply_presence(filtered, "possui_pt", has_pt)
    if "possui_ted" in filtered.columns:
        filtered = apply_presence(filtered, "possui_ted", has_ted)
    return filtered.copy()


def portfolio_coverage(active_df: pd.DataFrame) -> pd.DataFrame:
    return coverage_rows(active_df, ["vigencia_fim", "parceiro", "objeto_completo", "possui_pt", "possui_ted"])


def build_dashboard_model(bundle: dict[str, Any], *, today: date | datetime | pd.Timestamp | None = None) -> dict[str, pd.DataFrame]:
    portfolio = build_canonical_portfolio(bundle, today=today)
    return {
        "portfolio": portfolio,
        "active": active_portfolio(portfolio),
        "priorities": priority_dataframe(active_portfolio(portfolio)),
        "history": build_history_dataframe(bundle),
        "ted": build_ted_dataframe(bundle, today=today),
        "pt": build_pt_dataframe(bundle, today=today),
    }
