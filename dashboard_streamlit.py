from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard import historical_partnerships as history
from dashboard import partnerships_active as active
from dashboard import ted_metrics as ted
from dashboard.auth import clear_auth_session, require_dashboard_authentication
from dashboard.category_models import MAIN_TABS, VIGENCIA_LABELS
from dashboard.dashboard_components import (
    plot_bar,
    plot_horizontal_value_ranking,
    render_authenticated_header,
    render_active_detail,
    render_dataframe,
    render_history_detail,
    render_metric_cards,
    render_ted_detail,
)
from dashboard.dashboard_styles import inject_css
from dashboard.data_sources import build_file_signature, dashboard_source_paths, load_dashboard_bundle


ROOT_DIR = Path(__file__).resolve().parent
LOGO_PATH = ROOT_DIR / "assets" / "logo_institucional.png"


@st.cache_data(show_spinner=False)
def _load_bundle_cached(root_dir_str: str, _signature: tuple[tuple[str, bool, int, int], ...]) -> dict[str, Any]:
    return load_dashboard_bundle(Path(root_dir_str))


def _refresh_bundle() -> dict[str, Any]:
    paths = dashboard_source_paths(ROOT_DIR)
    signature = build_file_signature(paths)
    return _load_bundle_cached(str(ROOT_DIR), signature)


def _non_empty_options(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(value for value in df[column].dropna().astype(str).str.strip().unique().tolist() if value)


def _render_active_tab(df: pd.DataFrame) -> None:
    st.subheader("Parcerias Vigentes")
    if df.empty:
        st.info("Nenhuma parceria vigente encontrada na última coleta.")
        return

    with st.container():
        c1, c2, c3, c4 = st.columns([1.2, 1.2, 1, 1])
        processos = c1.multiselect("Processo", _non_empty_options(df, "processo"), key="active_processos")
        parceiros = c2.multiselect("Parceiro", _non_empty_options(df, "parceiro"), key="active_parceiros")
        documentos = c3.multiselect("Tipo de documento", _non_empty_options(df, "documento_tipo"), key="active_docs")
        situacoes = c4.multiselect(
            "Situação da vigência",
            ["vermelho", "amarelo", "verde", "sem_data"],
            format_func=lambda value: VIGENCIA_LABELS.get(value, value),
            key="active_situacoes",
        )
        c5, c6, c7 = st.columns([1, 1, 2])
        has_pt = c5.selectbox("Plano de Trabalho", ["Todos", "Com", "Sem"], key="active_has_pt")
        has_ted = c6.selectbox("TED", ["Todos", "Com", "Sem"], key="active_has_ted")
        query = c7.text_input("Texto livre", key="active_query", placeholder="Processo, parceiro, documento ou objeto")

    filtered = active.filter_active_partnerships(
        df,
        processos=processos,
        parceiros=parceiros,
        documento_tipos=documentos,
        situacoes=situacoes,
        has_pt=has_pt,
        has_ted=has_ted,
        query=query,
    )
    metrics = active.active_metrics(filtered)
    render_metric_cards(
        [
            ("Total de parcerias vigentes", metrics["total"]),
            ("Vermelho", metrics["vermelho"]),
            ("Amarelo", metrics["amarelo"]),
            ("Verde", metrics["verde"]),
            ("Sem vigência final", metrics["sem_data"]),
        ]
    )

    dist = active.deadline_distribution(filtered)
    plot_bar(
        dist,
        x="Situação",
        y="Total",
        color="Situação",
        text="Total",
        title="Distribuição das parcerias vigentes por situação de vigência",
        key="active_deadline_distribution",
    )

    fallback_rows = filtered[filtered["fontes_origem"].astype(str).str.contains("preview_fallback", na=False)]
    if not fallback_rows.empty:
        st.info("Alguns registros usam dados da prévia do interno porque a documentação normalizada ainda não cobre todos os campos.")

    st.subheader("Tabela de consulta")
    render_dataframe(active.display_table(filtered), empty_message="Nenhuma parceria vigente para os filtros atuais.")
    if filtered.empty:
        return
    selected = st.selectbox(
        "Detalhe do registro",
        filtered["record_id"].tolist(),
        format_func=lambda record_id: active.detail_label(filtered, record_id),
        key="active_detail",
    )
    render_active_detail(filtered[filtered["record_id"] == selected].iloc[0])


def _render_ted_tab(df: pd.DataFrame) -> None:
    st.subheader("Termo de Execução Descentralizada")
    if df.empty:
        st.info("Nenhum TED encontrado na última coleta.")
        return

    max_value = float(df["valor_global_num"].max()) if "valor_global_num" in df.columns and not df.empty else 0.0
    with st.container():
        c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.2])
        processos = c1.multiselect("Processo", _non_empty_options(df, "processo"), key="ted_processos")
        numeros = c2.multiselect("Número do TED", _non_empty_options(df, "numero_ted_display"), key="ted_numeros")
        situacoes = c3.multiselect(
            "Situação da vigência",
            ["vermelho", "amarelo", "verde", "sem_data"],
            format_func=lambda value: VIGENCIA_LABELS.get(value, value),
            key="ted_situacoes",
        )
        unidades = c4.multiselect("Unidade descentralizadora", _non_empty_options(df, "unidade_descentralizadora"), key="ted_unidades")
        c5, c6, c7 = st.columns([1, 1, 2])
        min_value = c5.number_input("Valor mínimo", min_value=0.0, max_value=max_value if max_value > 0 else 0.0, value=0.0, key="ted_min_value")
        max_filter = c6.number_input("Valor máximo", min_value=0.0, max_value=max_value if max_value > 0 else 0.0, value=max_value, key="ted_max_value")
        query = c7.text_input("Texto livre", key="ted_query", placeholder="Processo, número, objeto ou unidade")

    filtered = ted.filter_teds(
        df,
        processos=processos,
        numeros=numeros,
        situacoes=situacoes,
        unidades=unidades,
        min_value=min_value if max_value > 0 else None,
        max_value=max_filter if max_value > 0 else None,
        query=query,
    )
    metrics = ted.ted_metrics(filtered)
    coverage = f"{metrics['valor_total_display']} em {metrics['valor_validos']} de {metrics['total']} TEDs"
    render_metric_cards(
        [
            ("Total de TEDs", metrics["total"]),
            ("Valor global conhecido", metrics["valor_total_display"]),
            ("Cobertura do valor global", coverage),
            ("TEDs sem vigência final", metrics["sem_vigencia"]),
            ("TEDs em vermelho", metrics["vermelho"]),
        ]
    )

    plot_horizontal_value_ranking(ted.value_ranking(filtered), key="ted_value_ranking")
    unit_check = ted.chartable_dimension(filtered, "unidade_descentralizadora")
    if not unit_check["allowed"]:
        st.info("A unidade descentralizadora ainda não possui cobertura e padronização suficientes para agrupamento gerencial.")

    st.subheader("Tabela de TEDs")
    render_dataframe(ted.display_table(filtered), empty_message="Nenhum TED para os filtros atuais.")
    if filtered.empty:
        return
    selected = st.selectbox(
        "Detalhe do TED",
        filtered["record_id"].tolist(),
        format_func=lambda record_id: ted.detail_label(filtered, record_id),
        key="ted_detail",
    )
    render_ted_detail(filtered[filtered["record_id"] == selected].iloc[0])


def _render_history_tab(df: pd.DataFrame) -> None:
    st.subheader("Parcerias Descontinuadas / Não Realizadas")
    if df.empty:
        st.info("Nenhum registro histórico encontrado na última coleta.")
        return

    with st.container():
        c1, c2, c3, c4 = st.columns([1.1, 1.2, 1, 1])
        processos = c1.multiselect("Processo", _non_empty_options(df, "processo"), key="hist_processos")
        parceiros = c2.multiselect("Parceiro", _non_empty_options(df, "parceiro"), key="hist_parceiros")
        documentos = c3.multiselect("Documento", _non_empty_options(df, "tipo"), key="hist_documentos")
        statuses = c4.multiselect("Status", _non_empty_options(df, "status_gerencial"), key="hist_statuses")
        c5, c6, c7, c8 = st.columns([1, 1, 1, 2])
        categorias = c5.multiselect("Categoria", _non_empty_options(df, "categoria_gerencial"), key="hist_categorias")
        date_start = c6.date_input("Data inicial", value=None, key="hist_date_start")
        date_end = c7.date_input("Data final", value=None, key="hist_date_end")
        query = c8.text_input("Texto livre", key="hist_query", placeholder="Processo, parceiro, documento, status ou objeto")

    filtered = history.filter_history(
        df,
        processos=processos,
        parceiros=parceiros,
        documentos=documentos,
        statuses=statuses,
        categorias=categorias,
        date_start=date_start,
        date_end=date_end,
        query=query,
    )
    metrics = history.history_metrics(filtered)
    render_metric_cards(
        [
            ("Total de registros históricos", metrics["total"]),
            ("Encerradas", metrics["encerradas"]),
            ("Não realizadas", metrics["nao_realizadas"]),
            ("Sem status ou inconsistentes", metrics["inconsistentes"]),
        ]
    )

    plot_bar(
        history.status_distribution(filtered),
        x="Status",
        y="Total",
        text="Total",
        title="Quantidade de registros por status normalizado",
        key="history_status_distribution",
    )
    if date_start or date_end:
        st.info("As datas históricas têm cobertura parcial; registros sem data identificada podem não aparecer no intervalo filtrado.")

    st.subheader("Tabela histórica")
    render_dataframe(history.display_table(filtered), empty_message="Nenhum registro histórico para os filtros atuais.")
    if filtered.empty:
        return
    selected = st.selectbox(
        "Detalhe do registro histórico",
        filtered["record_id"].tolist(),
        format_func=lambda record_id: history.detail_label(filtered, record_id),
        key="history_detail",
    )
    render_history_detail(filtered[filtered["record_id"] == selected].iloc[0])


def main() -> None:
    st.set_page_config(page_title="Dashboard Gerencial de Projetos", layout="wide")
    inject_css()

    require_dashboard_authentication(ROOT_DIR, LOGO_PATH)

    bundle = _refresh_bundle()
    if render_authenticated_header(
        bundle.get("collection_meta", {}),
        st.session_state.get("authenticated_username", "-"),
        LOGO_PATH,
    ):
        clear_auth_session()
        st.rerun()

    active_df = active.build_active_partnerships(bundle)
    ted_df = ted.build_teds(bundle)
    history_df = history.build_historical_partnerships(bundle)

    tabs = st.tabs(MAIN_TABS)
    with tabs[0]:
        _render_active_tab(active_df)
    with tabs[1]:
        _render_ted_tab(ted_df)
    with tabs[2]:
        _render_history_tab(history_df)


if __name__ == "__main__":
    main()
