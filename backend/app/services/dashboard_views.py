from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from .dashboard_metrics import DEADLINE_LABELS, format_currency
from .dashboard_portfolio import filter_portfolio, priority_dataframe
from .dashboard_quality import is_chartable_dimension


DEADLINE_COLORS = {
    "verde": "#16A34A",
    "amarelo": "#CA8A04",
    "vermelho": "#DC2626",
    "sem_data": "#64748B",
}


def inject_css() -> None:
    st.markdown(
        """
        <style>
            :root {
                --primary: #1F4E79;
                --accent: #0F766E;
                --bg: #F8FAFC;
                --card: #FFFFFF;
                --border: #D8DEE8;
                --text: #172033;
                --muted: #5F6B7A;
                --radius: 8px;
                --shadow: 0 2px 12px rgba(15, 23, 42, 0.05);
            }

            html, body, [class*="css"] {
                font-family: 'Inter', 'Segoe UI', sans-serif;
            }

            .stApp {
                background-color: var(--bg);
                color: var(--text);
            }

            .main-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 0.9rem 1.25rem;
                background: var(--card);
                border: 1px solid var(--border);
                border-radius: var(--radius);
                margin-bottom: 1rem;
                box-shadow: var(--shadow);
            }

            .brand {
                font-size: 1.25rem;
                font-weight: 700;
                color: var(--primary);
            }

            .subtitle {
                font-size: 0.84rem;
                color: var(--muted);
            }

            .page-title {
                padding: 1rem 1.25rem;
                background: #FFFFFF;
                border: 1px solid var(--border);
                border-left: 5px solid var(--accent);
                border-radius: var(--radius);
                margin-bottom: 1.1rem;
                box-shadow: var(--shadow);
            }

            .page-title h1 {
                margin: 0;
                font-size: 1.5rem;
                color: var(--text);
            }

            .page-title p {
                margin: 0.35rem 0 0;
                color: var(--muted);
                font-size: 0.95rem;
            }

            div[data-testid="stMetric"] {
                background: var(--card);
                border-radius: var(--radius);
                border: 1px solid var(--border);
                padding: 0.85rem;
                box-shadow: var(--shadow);
            }

            section[data-testid="stSidebar"] {
                background-color: #FFFFFF;
                border-right: 1px solid var(--border);
            }

            .stTabs [data-baseweb="tab"] {
                border-radius: var(--radius);
                padding: 0.55rem 0.9rem;
                background: #EAF1F7;
                color: var(--muted);
                font-weight: 600;
            }

            .stTabs [aria-selected="true"] {
                background: var(--primary);
                color: white;
            }

            div[data-testid="stPlotlyChart"] {
                background: var(--card);
                border-radius: var(--radius);
                padding: 0.35rem;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
            }

            [data-testid="stDataFrame"] {
                border-radius: var(--radius);
                border: 1px solid var(--border);
                overflow: hidden;
            }

            .stButton button {
                border-radius: var(--radius);
                font-weight: 600;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header(collection_meta: dict[str, Any]) -> None:
    collected_at = collection_meta.get("data_ultima_coleta") or "ultima coleta nao identificada"
    source = collection_meta.get("fonte_data_ultima_coleta") or ""
    st.markdown(
        f"""
        <div class="main-header">
            <div>
                <div class="brand">CENSIPAM</div>
                <div class="subtitle">Painel gerencial de acompanhamento de processos e instrumentos</div>
            </div>
            <div class="subtitle">Ultima coleta: {collected_at} {f"({source})" if source else ""}</div>
        </div>

        <div class="page-title">
            <h1>Dashboard Gerencial de Projetos</h1>
            <p>Carteira ativa, instrumentos, vigencia, parceiros, PT, TEDs, historico e qualidade da base.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _style_figure(fig: Any) -> Any:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        colorway=["#1F4E79", "#0F766E", "#CA8A04", "#DC2626", "#64748B", "#2563EB"],
        font={"family": "Inter, sans-serif", "size": 13},
        title={"x": 0, "font": {"size": 16}},
        margin={"l": 10, "r": 10, "t": 48, "b": 10},
        legend={"orientation": "h", "y": 1.1},
    )
    fig.update_xaxes(showgrid=True, gridcolor="#E5E7EB", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E5E7EB", zeroline=False)
    return fig


def _plot(fig: Any, key: str) -> None:
    st.plotly_chart(
        _style_figure(fig),
        use_container_width=True,
        key=key,
        config={"displayModeBar": False, "responsive": True},
    )


def _deadline_label(value: Any) -> str:
    return DEADLINE_LABELS.get(str(value or "").strip(), "Sem data")


def _deadline_display_df(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in ("indicador_vigencia", "indicador_prazo", "indicador_prazo_pt"):
        if column in result.columns:
            result[column] = result[column].apply(_deadline_label)
    if "dias_restantes" in result.columns:
        result["dias_restantes"] = result["dias_restantes"].apply(lambda value: "" if pd.isna(value) else int(value))
    return result


def _dataframe(df: pd.DataFrame, columns: list[str], labels: dict[str, str], *, empty: str) -> None:
    if df.empty:
        st.info(empty)
        return
    available = [column for column in columns if column in df.columns]
    display_df = _deadline_display_df(df[available]).rename(columns=labels)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def _metric_value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df.columns:
        return {}
    return {str(key): int(value) for key, value in df[column].value_counts().to_dict().items()}


def render_sidebar(active_df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")
    if st.sidebar.button("Atualizar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    query = st.sidebar.text_input("Busca textual", key="filter_query", placeholder="Processo, parceiro, documento ou objeto")
    indicadores = st.sidebar.multiselect(
        "Situacao da vigencia",
        options=["vermelho", "amarelo", "verde", "sem_data"],
        format_func=_deadline_label,
        key="filter_indicadores",
    )
    documentos = st.sidebar.multiselect(
        "Documento principal",
        options=sorted(active_df["documento_principal_tipo"].dropna().astype(str).unique().tolist()) if not active_df.empty else [],
        key="filter_documentos",
    )
    parceiros = st.sidebar.multiselect(
        "Parceiro",
        options=sorted(value for value in active_df.get("parceiro", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if value),
        key="filter_parceiros",
    )
    has_pt = st.sidebar.selectbox("Presenca de PT", ["Todos", "Com", "Sem"], key="filter_has_pt")
    has_ted = st.sidebar.selectbox("Presenca de TED", ["Todos", "Com", "Sem"], key="filter_has_ted")
    return filter_portfolio(
        active_df,
        query=query,
        indicadores=indicadores,
        documentos=documentos,
        parceiros=parceiros,
        has_pt=has_pt,
        has_ted=has_ted,
    )


def render_overview(active_df: pd.DataFrame, ted_df: pd.DataFrame, quality_model: dict[str, pd.DataFrame]) -> None:
    counts = _metric_value_counts(active_df, "indicador_vigencia")
    ted_value_known = ted_df[ted_df.get("valor_global_num", pd.Series(dtype=float)) > 0] if not ted_df.empty else ted_df
    ted_total = float(ted_value_known["valor_global_num"].sum()) if not ted_value_known.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Em acompanhamento", len(active_df))
    c2.metric("Vermelhos", counts.get("vermelho", 0))
    c3.metric("Amarelos", counts.get("amarelo", 0))
    c4.metric("Verdes", counts.get("verde", 0))
    c5.metric("Sem vigencia final", counts.get("sem_data", 0))

    c6, c7 = st.columns([1.2, 1])
    c6.metric("Valor TED conhecido", f"{format_currency(ted_total)} em {len(ted_value_known)} de {len(ted_df)} TEDs")
    c7.metric("Itens em revisao", int((quality_model.get("conflicts", pd.DataFrame()).shape[0])))

    left, right = st.columns([1, 1.2])
    with left:
        dist = (
            active_df["indicador_vigencia"]
            .map(_deadline_label)
            .value_counts()
            .rename_axis("indicador")
            .reset_index(name="total")
            if not active_df.empty
            else pd.DataFrame(columns=["indicador", "total"])
        )
        if dist.empty:
            st.info("Nao ha carteira ativa para distribuir por vigencia.")
        else:
            _plot(
                px.bar(
                    dist,
                    x="indicador",
                    y="total",
                    text="total",
                    color="indicador",
                    title="Distribuicao de vigencia da carteira ativa",
                    color_discrete_map={DEADLINE_LABELS[key]: value for key, value in DEADLINE_COLORS.items()},
                ),
                "overview_deadline_distribution",
            )
    with right:
        st.subheader("Prioridades")
        priorities = priority_dataframe(active_df).head(10)
        _dataframe(
            priorities,
            ["processo", "documento_principal_tipo", "parceiro", "objeto_resumo", "vigencia_fim", "dias_restantes", "indicador_vigencia"],
            {
                "processo": "Processo",
                "documento_principal_tipo": "Documento",
                "parceiro": "Parceiro",
                "objeto_resumo": "Objeto/Atribuicao",
                "vigencia_fim": "Fim da Vigencia",
                "dias_restantes": "Dias",
                "indicador_vigencia": "Situacao",
            },
            empty="Nao ha prioridades para os filtros atuais.",
        )

    st.subheader("Cobertura da carteira ativa")
    coverage = quality_model.get("coverage", pd.DataFrame()).copy()
    if not coverage.empty:
        coverage["cobertura"] = coverage["cobertura"].apply(lambda value: f"{value:.0%}")
    _dataframe(
        coverage,
        ["campo", "preenchidos", "total", "cobertura"],
        {"campo": "Campo", "preenchidos": "Preenchidos", "total": "Total", "cobertura": "Cobertura"},
        empty="Sem dados de cobertura para a carteira ativa.",
    )


def render_portfolio(active_df: pd.DataFrame, portfolio_df: pd.DataFrame) -> None:
    table = active_df.copy()
    if not table.empty:
        table["documento"] = table.apply(
            lambda row: " ".join(
                part
                for part in (str(row.get("documento_principal_tipo", "")), str(row.get("documento_principal_numero", "")))
                if part.strip()
            ),
            axis=1,
        )
    _dataframe(
        table,
        ["processo", "documento", "parceiro", "objeto_resumo", "vigencia_fim", "dias_restantes", "indicador_vigencia"],
        {
            "processo": "Processo",
            "documento": "Documento",
            "parceiro": "Parceiro",
            "objeto_resumo": "Objeto/Atribuicao",
            "vigencia_fim": "Fim da Vigencia",
            "dias_restantes": "Dias Restantes",
            "indicador_vigencia": "Situacao",
        },
        empty="Nao ha processos ativos para os filtros atuais.",
    )

    if active_df.empty:
        return
    st.subheader("Detalhe do processo")
    selected = st.selectbox("Processo", active_df["processo"].tolist(), key="portfolio_detail_process")
    detail_rows = portfolio_df[portfolio_df["processo"] == selected]
    if detail_rows.empty:
        st.info("Detalhe indisponivel para o processo selecionado.")
        return
    row = detail_rows.iloc[0].to_dict()
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Processo:** `{row.get('processo', '')}`")
        st.markdown(f"**Parceiro:** {row.get('parceiro', '') or 'Nao identificado'}")
        st.markdown(f"**Documento principal:** {row.get('documento_principal_tipo', '')} {row.get('documento_principal_numero', '')}")
        st.markdown(f"**Vigencia:** {row.get('vigencia_inicio', '') or '-'} a {row.get('vigencia_fim', '') or '-'}")
        st.markdown(f"**Regra de status:** {_deadline_label(row.get('indicador_vigencia', ''))}, {row.get('dias_restantes', '') if not pd.isna(row.get('dias_restantes', None)) else 'sem data'} dias")
        st.markdown(f"**Documentos relacionados:** {row.get('documentos_relacionados', '') or 'Nenhum'}")
    with right:
        st.markdown(f"**PT:** {'sim' if row.get('possui_pt') else 'nao'}")
        st.markdown(f"**Metas / acoes / prazos PT:** {row.get('quantidade_metas_pt', 0)} / {row.get('quantidade_acoes_pt', 0)} / {row.get('quantidade_prazos_pt', 0)}")
        st.markdown(f"**TED:** {'sim' if row.get('possui_ted') else 'nao'}")
        st.markdown(f"**Valor TED:** {format_currency(row.get('valor_global_ted_num', 0.0)) if row.get('possui_ted') else '-'}")
        st.markdown(f"**Campos ausentes:** {row.get('campos_ausentes', '') or 'Nenhum campo critico ausente'}")
        st.markdown(f"**Origem:** {row.get('fontes_de_origem', '') or '-'}")
    with st.expander("Objeto completo e inconsistencias", expanded=False):
        st.write(row.get("objeto_completo", "") or "Objeto nao identificado.")
        if row.get("pendencias_ou_acoes_abertas"):
            st.markdown(f"**Pendencias ou acoes abertas:** {row.get('pendencias_ou_acoes_abertas')}")
        if row.get("conflitos"):
            st.warning(row.get("conflitos"))


def render_pt(pt_df: pd.DataFrame) -> None:
    if pt_df.empty:
        st.info("Nenhum Plano de Trabalho encontrado na rodada atual.")
        return
    processes = int(pt_df["processo"].nunique())
    metas = int(pt_df["metas_count"].sum())
    acoes = int(pt_df["acoes_count"].sum())
    sem_estrutura = int((~pt_df["possui_metas"] | ~pt_df["possui_acoes"] | ~pt_df["possui_prazo"]).sum())
    prazo_counts = _metric_value_counts(pt_df, "indicador_prazo_pt")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Processos com PT", processes)
    c2.metric("Metas identificadas", metas)
    c3.metric("Acoes identificadas", acoes)
    c4.metric("PT sem estrutura completa", sem_estrutura)

    c5, c6, c7 = st.columns(3)
    c5.metric("Prazos PT vermelhos", prazo_counts.get("vermelho", 0))
    c6.metric("Prazos PT amarelos", prazo_counts.get("amarelo", 0))
    c7.metric("Prazos PT sem data", prazo_counts.get("sem_data", 0))

    summary = (
        pt_df.groupby("processo", as_index=False)
        .agg(
            parceiro=("parceiro", "first"),
            metas_count=("metas_count", "sum"),
            acoes_count=("acoes_count", "sum"),
            prazo_fim=("prazo_fim", "min"),
            publication_status=("publication_status", "first"),
            normalization_status=("normalization_status", "first"),
        )
        .sort_values("processo")
    )
    st.subheader("Processos com PT")
    _dataframe(
        summary,
        ["processo", "parceiro", "metas_count", "acoes_count", "prazo_fim", "publication_status", "normalization_status"],
        {
            "processo": "Processo",
            "parceiro": "Parceiro",
            "metas_count": "Metas",
            "acoes_count": "Acoes",
            "prazo_fim": "Proximo Prazo",
            "publication_status": "Publicacao",
            "normalization_status": "Normalizacao",
        },
        empty="Nao ha PT para exibir.",
    )

    st.subheader("Prazos prioritarios de PT")
    priority = pt_df.sort_values(["indicador_prazo_pt", "prazo_fim"], na_position="last").head(15)
    _dataframe(
        priority,
        ["processo", "parceiro", "prazo_fim", "indicador_prazo_pt", "metas_count", "acoes_count"],
        {
            "processo": "Processo",
            "parceiro": "Parceiro",
            "prazo_fim": "Prazo",
            "indicador_prazo_pt": "Situacao",
            "metas_count": "Metas",
            "acoes_count": "Acoes",
        },
        empty="Nao ha prazos PT estruturados.",
    )

    st.subheader("Detalhe de metas e acoes")
    selected = st.selectbox("Processo com PT", summary["processo"].tolist(), key="pt_detail_process")
    rows = pt_df[pt_df["processo"] == selected]
    for _, record in rows.iterrows():
        with st.expander(record.get("documento", "") or selected, expanded=False):
            st.markdown(f"**Objeto:** {record.get('objeto_completo', '') or 'Nao identificado'}")
            st.markdown(f"**Metas:** {record.get('metas_raw', '') or 'Nenhuma meta estruturada'}")
            st.markdown(f"**Acoes:** {record.get('acoes_raw', '') or 'Nenhuma acao estruturada'}")
            if not record.get("possui_prazo"):
                st.warning("PT sem prazo estruturado.")


def render_ted(ted_df: pd.DataFrame) -> None:
    if ted_df.empty:
        st.info("Nenhum TED encontrado na rodada atual.")
        return
    value_known = ted_df[ted_df["valor_global_num"] > 0]
    vigencia_known = ted_df[ted_df["vigencia_fim"].astype(str).str.strip().ne("")]
    unit_known = ted_df[ted_df["unidade_descentralizadora"].astype(str).str.strip().ne("")]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TEDs", len(ted_df))
    c2.metric("Valor conhecido", f"{format_currency(value_known['valor_global_num'].sum())} em {len(value_known)} de {len(ted_df)}")
    c3.metric("Vigencia conhecida", f"{len(vigencia_known)} de {len(ted_df)}")
    c4.metric("Unidade desc. conhecida", f"{len(unit_known)} de {len(ted_df)}")

    st.subheader("Tabela de TEDs")
    _dataframe(
        ted_df,
        ["processo", "numero_ted", "ano_ted", "objeto_resumo", "valor_global", "vigencia_fim", "indicador_vigencia", "quality_status"],
        {
            "processo": "Processo",
            "numero_ted": "Numero",
            "ano_ted": "Ano",
            "objeto_resumo": "Objeto",
            "valor_global": "Valor",
            "vigencia_fim": "Fim da Vigencia",
            "indicador_vigencia": "Situacao",
            "quality_status": "Qualidade",
        },
        empty="Nenhum TED para exibir.",
    )

    ranking = value_known.sort_values("valor_global_num", ascending=False).head(10)
    if not ranking.empty:
        _plot(
            px.bar(
                ranking,
                x="valor_global_num",
                y="processo",
                orientation="h",
                text="valor_global",
                title="Maiores TEDs por valor global",
                labels={"valor_global_num": "Valor", "processo": "Processo"},
            ),
            "ted_value_ranking",
        )

    unit_check = is_chartable_dimension(ted_df, "unidade_descentralizadora")
    if unit_check["allowed"]:
        unit_df = ted_df.groupby("unidade_descentralizadora", as_index=False)["valor_global_num"].sum().sort_values("valor_global_num", ascending=False)
        _plot(
            px.bar(unit_df, x="unidade_descentralizadora", y="valor_global_num", title="Valor por unidade descentralizadora"),
            "ted_unit_chart",
        )
    else:
        st.info("Nao ha TEDs com unidade descentralizadora suficientemente normalizada para este agrupamento.")


def render_history(history_df: pd.DataFrame) -> None:
    if history_df.empty:
        st.info("Nenhum registro historico encontrado.")
        return
    counts = history_df["situacao_carteira"].value_counts().to_dict()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Encerrados", int(counts.get("historico_encerrado", 0)))
    c2.metric("Nao realizados", int(counts.get("historico_nao_realizado", 0)))
    c3.metric("Descontinuados", int(counts.get("historico_descontinuado", 0)))
    c4.metric("Vencidos", int(counts.get("historico_vencido", 0)))
    c5.metric("Vigentes", int(counts.get("historico_vigente", 0)))
    c6.metric("Indeterminados / revisar", int(counts.get("inconsistente_ou_revisar", 0)))

    status_check = is_chartable_dimension(history_df, "status_calculado")
    if status_check["allowed"]:
        status_df = history_df.groupby("status_calculado", as_index=False).size().rename(columns={"size": "total"})
        _plot(px.bar(status_df, x="status_calculado", y="total", text="total", title="Historico por status"), "history_status")
    else:
        st.info(f"Distribuicao historica por status nao renderizada: {status_check['reason']}.")

    _dataframe(
        history_df,
        ["processo", "tipo", "parceiro", "objeto_resumo", "data_vencimento", "status_calculado", "situacao_carteira", "conflitos"],
        {
            "processo": "Processo",
            "tipo": "Tipo",
            "parceiro": "Parceiro",
            "objeto_resumo": "Objeto",
            "data_vencimento": "Data Vencimento",
            "status_calculado": "Status",
            "situacao_carteira": "Classificacao",
            "conflitos": "Conflitos",
        },
        empty="Nenhum historico para exibir.",
    )


def render_quality(quality_model: dict[str, pd.DataFrame]) -> None:
    st.subheader("Cobertura da carteira ativa")
    coverage = quality_model.get("coverage", pd.DataFrame()).copy()
    if not coverage.empty:
        coverage["cobertura"] = coverage["cobertura"].apply(lambda value: f"{value:.0%}")
    _dataframe(
        coverage,
        ["campo", "preenchidos", "total", "cobertura"],
        {"campo": "Campo", "preenchidos": "Preenchidos", "total": "Total", "cobertura": "Cobertura"},
        empty="Sem cobertura calculada.",
    )

    st.subheader("Cobertura TED")
    ted_coverage = quality_model.get("ted_coverage", pd.DataFrame()).copy()
    if not ted_coverage.empty:
        ted_coverage["cobertura"] = ted_coverage["cobertura"].apply(lambda value: f"{value:.0%}")
    _dataframe(
        ted_coverage,
        ["campo", "preenchidos", "total", "cobertura"],
        {"campo": "Campo", "preenchidos": "Preenchidos", "total": "Total", "cobertura": "Cobertura"},
        empty="Sem TEDs para cobertura.",
    )

    st.subheader("Criterios de graficos")
    chartability = quality_model.get("chartability", pd.DataFrame()).copy()
    if not chartability.empty:
        chartability["cobertura"] = chartability["cobertura"].apply(lambda value: f"{value:.0%}")
    _dataframe(
        chartability,
        ["campo", "permitido", "motivo", "cobertura", "categorias", "maior_rotulo"],
        {
            "campo": "Campo",
            "permitido": "Permitido",
            "motivo": "Motivo",
            "cobertura": "Cobertura",
            "categorias": "Categorias",
            "maior_rotulo": "Maior Rotulo",
        },
        empty="Sem criterios de grafico calculados.",
    )

    st.subheader("Conflitos e inconsistencias")
    _dataframe(
        quality_model.get("conflicts", pd.DataFrame()),
        ["processo", "origem", "situacao", "descricao"],
        {"processo": "Processo", "origem": "Origem", "situacao": "Situacao", "descricao": "Descricao"},
        empty="Nenhum conflito identificado.",
    )

    st.subheader("Fila de revisao")
    summary = quality_model.get("review_summary", pd.DataFrame())
    _dataframe(
        summary,
        ["severity", "document_type", "code", "total"],
        {"severity": "Severidade", "document_type": "Tipo", "code": "Codigo", "total": "Total"},
        empty="Sem itens na fila de revisao.",
    )
    with st.expander("Itens de revisao", expanded=False):
        queue = quality_model.get("review_queue", pd.DataFrame()).head(100)
        _dataframe(
            queue,
            ["severity", "document_type", "code", "field", "message", "suggested_action", "processo"],
            {
                "severity": "Severidade",
                "document_type": "Tipo",
                "code": "Codigo",
                "field": "Campo",
                "message": "Mensagem",
                "suggested_action": "Acao sugerida",
                "processo": "Processo",
            },
            empty="Sem itens detalhados de revisao.",
        )


def render_dashboard(model: dict[str, pd.DataFrame], quality_model: dict[str, pd.DataFrame]) -> None:
    active_df = model["active"]
    tabs = st.tabs(["Visao Geral", "Carteira de Intencoes", "Planos de Trabalho", "TEDs e Recursos", "Historico", "Qualidade da Base"])
    with tabs[0]:
        render_overview(active_df, model["ted"], quality_model)
    with tabs[1]:
        render_portfolio(active_df, model["portfolio"])
    with tabs[2]:
        render_pt(model["pt"])
    with tabs[3]:
        render_ted(model["ted"])
    with tabs[4]:
        render_history(model["history"])
    with tabs[5]:
        render_quality(quality_model)
