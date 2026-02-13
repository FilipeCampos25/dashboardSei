from __future__ import annotations

from datetime import date
from pathlib import Path
import re

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Dashboard SEI - NOC", layout="wide")
st.title("Dashboard SEI - NOC, PIntencoes e Plano de Trabalho")

DATA_PATH = Path("output") / "sei_dashboard.csv"

# Canonical fields expected from the collection.
CANONICAL_COLUMNS = [
    "processo",
    "documento",
    "parceiro",
    "vigencia_inicio",
    "vigencia_fim",
    "objeto",
    "atribuicao",
    "meta",
    "acao",
    "prazo",
    "status",
    "fonte",
    "collected_at",
]

ALIASES = {
    "processo": ["processo", "numero_processo", "n_processo", "proc"],
    "documento": ["documento", "tipo_documento", "doc", "titulo"],
    "parceiro": ["parceiro", "nome_parceiro", "orgao_parceiro", "instituicao"],
    "vigencia_inicio": ["vigencia_inicio", "inicio_vigencia", "data_inicio"],
    "vigencia_fim": ["vigencia_fim", "fim_vigencia", "data_fim"],
    "objeto": ["objeto", "descricao_objeto"],
    "atribuicao": ["atribuicao", "atribuicoes", "responsavel"],
    "meta": ["meta", "metas"],
    "acao": ["acao", "acoes", "atividade"],
    "prazo": ["prazo", "data_prazo", "deadline"],
    "status": ["status", "situacao"],
    "fonte": ["fonte", "origem", "tipo_fonte"],
    "collected_at": ["collected_at", "capturado_em", "data_coleta"],
}


def _normalize(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def _coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {col: _normalize(col) for col in df.columns}
    df = df.rename(columns=rename_map)

    for canonical, options in ALIASES.items():
        for option in options:
            if option in df.columns:
                if canonical not in df.columns:
                    df[canonical] = df[option]
                break

    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df


def _extract_from_linha(df: pd.DataFrame) -> pd.DataFrame:
    if "linha" not in df.columns:
        return df

    patterns = {
        "processo": r"processo\s*[:\-]\s*([^|;]+)",
        "documento": r"documento\s*[:\-]\s*([^|;]+)",
        "parceiro": r"parceiro\s*[:\-]\s*([^|;]+)",
        "objeto": r"objeto\s*[:\-]\s*([^|;]+)",
        "meta": r"meta\s*[:\-]\s*([^|;]+)",
        "acao": r"acao\s*[:\-]\s*([^|;]+)",
        "prazo": r"prazo\s*[:\-]\s*([^|;]+)",
    }

    linha_series = df["linha"].fillna("").astype(str)
    for field, pattern in patterns.items():
        missing_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
        extracted = linha_series.str.extract(pattern, flags=re.IGNORECASE, expand=False)
        df.loc[missing_mask, field] = extracted[missing_mask]

    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["vigencia_inicio", "vigencia_fim", "prazo", "collected_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _vigencia_status(row: pd.Series, today: date) -> str:
    end = row.get("vigencia_fim")
    if pd.isna(end):
        return "sem_data"
    delta = (end.date() - today).days
    if delta < 0:
        return "encerrada"
    if delta <= 30:
        return "vence_ate_30_dias"
    return "vigente"


def _load_data() -> pd.DataFrame:
    if DATA_PATH.exists():
        df = pd.read_csv(DATA_PATH)
    else:
        st.warning("Arquivo output/sei_dashboard.csv nao encontrado. Exibindo dados de exemplo.")
        df = pd.DataFrame(
            [
                {
                    "processo": "001/2026",
                    "documento": "NOC",
                    "parceiro": "Parceiro A",
                    "vigencia_inicio": "2026-01-05",
                    "vigencia_fim": "2026-12-31",
                    "objeto": "Cooperacao tecnica",
                    "atribuicao": "Coordenacao",
                    "meta": "Meta 1",
                    "acao": "Capacitacao inicial",
                    "prazo": "2026-03-15",
                    "status": "em_andamento",
                    "fonte": "NOC",
                    "collected_at": "2026-02-01 10:00:00",
                },
                {
                    "processo": "002/2026",
                    "documento": "PIntencoes",
                    "parceiro": "Parceiro B",
                    "vigencia_inicio": "2026-02-01",
                    "vigencia_fim": "2026-08-30",
                    "objeto": "Intercambio de dados",
                    "atribuicao": "Suporte tecnico",
                    "meta": "Meta 2",
                    "acao": "Integracao API",
                    "prazo": "2026-04-10",
                    "status": "planejada",
                    "fonte": "PIntencoes",
                    "collected_at": "2026-02-02 14:20:00",
                },
                {
                    "processo": "003/2026",
                    "documento": "Plano de Trabalho",
                    "parceiro": "Parceiro A",
                    "vigencia_inicio": "2025-10-01",
                    "vigencia_fim": "2026-02-20",
                    "objeto": "Aprimoramento operacional",
                    "atribuicao": "Gestao e monitoramento",
                    "meta": "Meta 3",
                    "acao": "Revisao de fluxos",
                    "prazo": "2026-02-18",
                    "status": "atrasada",
                    "fonte": "PlanoTrabalho",
                    "collected_at": "2026-02-03 08:10:00",
                },
            ]
        )

    df = _coalesce_columns(df)
    df = _extract_from_linha(df)
    df = _parse_dates(df)

    today = date.today()
    df["vigencia_status"] = df.apply(_vigencia_status, axis=1, today=today)
    return df


def _render_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")

    parceiro_opts = sorted(df["parceiro"].dropna().astype(str).unique().tolist())
    doc_opts = sorted(df["documento"].dropna().astype(str).unique().tolist())
    fonte_opts = sorted(df["fonte"].dropna().astype(str).unique().tolist())
    vig_opts = sorted(df["vigencia_status"].dropna().astype(str).unique().tolist())

    selected_parceiro = st.sidebar.multiselect("Parceiro", parceiro_opts)
    selected_doc = st.sidebar.multiselect("Documento", doc_opts)
    selected_fonte = st.sidebar.multiselect("Fonte", fonte_opts)
    selected_vig = st.sidebar.multiselect("Status de vigencia", vig_opts)

    filtered = df.copy()
    if selected_parceiro:
        filtered = filtered[filtered["parceiro"].astype(str).isin(selected_parceiro)]
    if selected_doc:
        filtered = filtered[filtered["documento"].astype(str).isin(selected_doc)]
    if selected_fonte:
        filtered = filtered[filtered["fonte"].astype(str).isin(selected_fonte)]
    if selected_vig:
        filtered = filtered[filtered["vigencia_status"].astype(str).isin(selected_vig)]

    return filtered


def _render_kpis(df: pd.DataFrame) -> None:
    total_registros = len(df)
    total_processos = df["processo"].dropna().nunique()
    total_parceiros = df["parceiro"].dropna().nunique()

    prazos_validos = df["prazo"].dropna()
    atrasados = int((prazos_validos.dt.date < date.today()).sum()) if not prazos_validos.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Registros", total_registros)
    c2.metric("Processos", total_processos)
    c3.metric("Parceiros", total_parceiros)
    c4.metric("Prazos vencidos", atrasados)


def _render_charts(df: pd.DataFrame) -> None:
    left, right = st.columns(2)

    parceiro_count = (
        df.dropna(subset=["parceiro"])
        .groupby("parceiro", as_index=False)
        .size()
        .rename(columns={"size": "total"})
        .sort_values("total", ascending=False)
    )
    if not parceiro_count.empty:
        fig_parceiro = px.bar(
            parceiro_count,
            x="parceiro",
            y="total",
            title="Processos por parceiro",
            text="total",
        )
        left.plotly_chart(fig_parceiro, use_container_width=True)

    doc_count = (
        df.dropna(subset=["documento"])
        .groupby("documento", as_index=False)
        .size()
        .rename(columns={"size": "total"})
        .sort_values("total", ascending=False)
    )
    if not doc_count.empty:
        fig_doc = px.pie(doc_count, names="documento", values="total", title="Distribuicao por tipo de documento")
        right.plotly_chart(fig_doc, use_container_width=True)

    left2, right2 = st.columns(2)

    prazo_df = df.dropna(subset=["prazo"]).copy()
    if not prazo_df.empty:
        prazo_df["mes_prazo"] = prazo_df["prazo"].dt.to_period("M").astype(str)
        prazo_count = (
            prazo_df.groupby("mes_prazo", as_index=False)
            .size()
            .rename(columns={"size": "total"})
            .sort_values("mes_prazo")
        )
        fig_prazo = px.line(
            prazo_count,
            x="mes_prazo",
            y="total",
            markers=True,
            title="Prazos por mes",
        )
        left2.plotly_chart(fig_prazo, use_container_width=True)

    status_df = (
        df.dropna(subset=["status"])
        .groupby("status", as_index=False)
        .size()
        .rename(columns={"size": "total"})
        .sort_values("total", ascending=False)
    )
    if not status_df.empty:
        fig_status = px.bar(status_df, x="status", y="total", title="Acoes por status", text="total")
        right2.plotly_chart(fig_status, use_container_width=True)


def _render_table(df: pd.DataFrame) -> None:
    st.subheader("Detalhamento")
    display_cols = [
        "processo",
        "documento",
        "parceiro",
        "vigencia_inicio",
        "vigencia_fim",
        "objeto",
        "atribuicao",
        "meta",
        "acao",
        "prazo",
        "status",
        "fonte",
    ]

    available_cols = [col for col in display_cols if col in df.columns]
    table = df[available_cols].copy()
    for col in ["vigencia_inicio", "vigencia_fim", "prazo"]:
        if col in table.columns:
            table[col] = table[col].dt.strftime("%Y-%m-%d")

    st.dataframe(table, use_container_width=True)

    model_df = pd.DataFrame(columns=display_cols)
    st.download_button(
        label="Baixar modelo CSV",
        data=model_df.to_csv(index=False).encode("utf-8"),
        file_name="modelo_dashboard_sei.csv",
        mime="text/csv",
    )


def main() -> None:
    df = _load_data()
    filtered = _render_filters(df)

    st.subheader("Visao geral")
    _render_kpis(filtered)
    _render_charts(filtered)
    _render_table(filtered)


if __name__ == "__main__":
    main()
