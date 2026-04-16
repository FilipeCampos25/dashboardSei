from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import plotly.express as px
import streamlit as st

from backend.app.services.dashboard_streamlit_data import (
    build_file_signature,
    dashboard_source_paths,
    explode_pt_acoes,
    explode_pt_metas,
    filter_by_processes,
    filter_overview_df,
    latest_log_rows,
    load_dashboard_bundle,
    memorando_detail_dataframe,
    parse_act_rejection_summary,
    process_explorer_payload,
    pt_detail_dataframe,
    pt_process_metrics,
    runtime_for_processes,
    summarize_log_entries,
    ted_detail_dataframe,
)

ROOT_DIR = Path(__file__).resolve().parent
BACKEND_MAIN_PATH = ROOT_DIR / "backend" / "main.py"
DOCUMENT_TYPE_OPTIONS = {
    "PT": "pt",
    "ACT": "act",
    "Memorando": "memorando",
    "TED": "ted",
}
PLOTLY_COLORWAY = ["#0F766E", "#2563EB", "#EA580C", "#16A34A", "#C2410C", "#0891B2", "#65A30D", "#DC2626"]


def _inject_css() -> None:
    st.markdown(
        """
        <style>
            :root {
                --primary: #1E3A8A; /* Azul institucional */
                --primary-light: #3B82F6;
                --bg: #F8FAFC;
                --card: #FFFFFF;
                --border: #E2E8F0;
                --text: #0F172A;
                --muted: #64748B;
                --radius: 12px;
                --shadow: 0 4px 20px rgba(0,0,0,0.04);
            }

            html, body, [class*="css"] {
                font-family: 'Inter', 'Segoe UI', sans-serif;
            }

            .stApp {
                background-color: var(--bg);
                color: var(--text);
            }

            /* HEADER */
            .main-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 1rem 1.5rem;
                background: var(--card);
                border-bottom: 1px solid var(--border);
                margin-bottom: 1.5rem;
                border-radius: var(--radius);
                box-shadow: var(--shadow);
            }

            .brand {
                font-size: 1.4rem;
                font-weight: 700;
                color: var(--primary);
            }

            .subtitle {
                font-size: 0.85rem;
                color: var(--muted);
            }

            /* HERO */
            .hero {
                background: linear-gradient(135deg, #1E3A8A, #3B82F6);
                color: white;
                padding: 1.5rem;
                border-radius: var(--radius);
                margin-bottom: 1.5rem;
            }

            .hero h1 {
                margin: 0;
                font-size: 1.8rem;
            }

            .hero p {
                margin-top: 0.4rem;
                font-size: 0.95rem;
                opacity: 0.9;
            }

            /* CARDS */
            div[data-testid="stMetric"] {
                background: var(--card);
                border-radius: var(--radius);
                border: 1px solid var(--border);
                padding: 1rem;
                box-shadow: var(--shadow);
            }

            /* SIDEBAR */
            section[data-testid="stSidebar"] {
                background-color: #FFFFFF;
                border-right: 1px solid var(--border);
            }

            /* TABS */
            .stTabs [data-baseweb="tab"] {
                border-radius: 10px;
                padding: 0.6rem 1rem;
                background: #EEF2FF;
                color: var(--muted);
                font-weight: 600;
            }

            .stTabs [aria-selected="true"] {
                background: var(--primary);
                color: white;
            }

            /* CHART */
            div[data-testid="stPlotlyChart"] {
                background: var(--card);
                border-radius: var(--radius);
                padding: 0.5rem;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
            }

            /* DATAFRAME */
            [data-testid="stDataFrame"] {
                border-radius: var(--radius);
                border: 1px solid var(--border);
                overflow: hidden;
            }

            /* BUTTON */
            .stButton button {
                background: var(--primary);
                color: white;
                border-radius: 10px;
                border: none;
                font-weight: 600;
            }

            .stButton button:hover {
                background: var(--primary-light);
            }

        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header() -> None:
    st.markdown(
        f"""
        <div class="main-header">
            <div>
                <div class="brand">CENSIPAM</div>
                <div class="subtitle">Centro Gestor e Operacional do Sistema de Proteção da Amazônia</div>
            </div>
            <div class="subtitle">
                {datetime.now().strftime("%d/%m/%Y %H:%M")}
            </div>
        </div>

        <div class="hero">
            <h1>Dashboard Operacional SEI</h1>
            <p>Monitoramento de processos, documentos e qualidade dos dados</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _style_figure(fig: Any) -> Any:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        colorway=["#1E3A8A", "#3B82F6", "#60A5FA", "#93C5FD"],
        font={"family": "Inter, sans-serif", "size": 13},
        title={"x": 0, "font": {"size": 16}},
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
        legend={"orientation": "h", "y": 1.1},
    )

    fig.update_xaxes(
        showgrid=True,
        gridcolor="#E5E7EB",
        zeroline=False,
    )

    fig.update_yaxes(
        showgrid=True,
        gridcolor="#E5E7EB",
        zeroline=False,
    )

    return fig


def _plotly_chart(target: Any, fig: Any, key: str) -> None:
    target.plotly_chart(
        _style_figure(fig),
        use_container_width=True,
        key=key,
        config={"displayModeBar": False, "responsive": True},
    )


@st.cache_data(show_spinner=False)
def _load_bundle_cached(root_dir_str: str, _signature: tuple[tuple[str, bool, int, int], ...]) -> Dict[str, Any]:
    return load_dashboard_bundle(Path(root_dir_str))


def _refresh_bundle() -> Dict[str, Any]:
    signature = build_file_signature(dashboard_source_paths(ROOT_DIR))
    return _load_bundle_cached(str(ROOT_DIR), signature)


def _status_badge(status: str) -> None:
    normalized = status.lower()
    if normalized == "em execucao":
        st.warning("Coleta em execucao.")
    elif normalized == "finalizada":
        st.success("Coleta finalizada.")
    elif normalized == "falhou":
        st.error("Coleta finalizada com falha.")
    else:
        st.info("Nenhuma coleta em execucao.")


def _get_collection_meta() -> Dict[str, Any]:
    return st.session_state.setdefault(
        "collection_meta",
        {
            "status": "nao iniciada",
            "started_at": "",
            "finished_at": "",
            "types": [],
            "command": "",
            "returncode": None,
            "pid": None,
        },
    )


def _collection_state() -> Dict[str, Any]:
    meta = _get_collection_meta()
    process = st.session_state.get("collection_process")
    if process is not None:
        returncode = process.poll()
        if returncode is None:
            meta["status"] = "em execucao"
            meta["pid"] = getattr(process, "pid", None)
        else:
            meta["status"] = "finalizada" if returncode == 0 else "falhou"
            meta["returncode"] = returncode
            if not meta.get("finished_at"):
                meta["finished_at"] = datetime.now().isoformat(timespec="seconds")
            st.session_state["collection_process"] = None
    return meta


def _build_collection_command(selected_labels: List[str], max_internos: int, max_processos: int) -> tuple[list[str], Dict[str, str]]:
    command = [sys.executable, str(BACKEND_MAIN_PATH), "--manual-login"]
    if max_internos > 0:
        command.extend(["--max-internos", str(max_internos)])
    if max_processos > 0:
        command.extend(["--max-processos", str(max_processos)])

    env = os.environ.copy()
    env["DOCUMENT_TYPES"] = ",".join(DOCUMENT_TYPE_OPTIONS[label] for label in selected_labels)
    env["PYTHONIOENCODING"] = "utf-8"
    return command, env


def _start_collection(selected_labels: List[str], max_internos: int, max_processos: int) -> None:
    command, env = _build_collection_command(selected_labels, max_internos, max_processos)
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(
        command,
        cwd=str(ROOT_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    st.session_state["collection_process"] = process
    meta = _get_collection_meta()
    meta.update(
        {
            "status": "em execucao",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": "",
            "types": selected_labels,
            "command": " ".join(command[1:]),
            "returncode": None,
            "pid": getattr(process, "pid", None),
        }
    )


def _format_date_column(series: pd.Series) -> pd.Series:
    return series.apply(lambda value: value.strftime("%Y-%m-%d") if pd.notna(value) else "")


def _presence_filter(label: str, key: str) -> str:
    return st.sidebar.selectbox(label, ["Todos", "Com", "Sem"], index=0, key=key)


def _render_sidebar_filters(overview_df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")
    processos = sorted([value for value in overview_df["processo"].dropna().astype(str).unique().tolist() if value])
    parceiros = sorted([value for value in overview_df["preview_parceiro"].dropna().astype(str).unique().tolist() if value])
    quality_values = sorted([value for value in overview_df["quality_status"].dropna().astype(str).unique().tolist() if value])

    selected_processos = st.sidebar.multiselect("Processo", processos, key="filter_processos")
    selected_parceiros = st.sidebar.multiselect("Parceiro", parceiros, key="filter_parceiros")
    selected_quality = st.sidebar.multiselect("Qualidade geral", quality_values, key="filter_quality")
    has_pt = _presence_filter("Presenca de PT", "filter_has_pt")
    has_act = _presence_filter("Presenca de ACT", "filter_has_act")
    has_memorando = _presence_filter("Presenca de Memorando", "filter_has_memorando")
    has_ted = _presence_filter("Presenca de TED", "filter_has_ted")

    return filter_overview_df(
        overview_df,
        processos=selected_processos,
        parceiros=selected_parceiros,
        quality_statuses=selected_quality,
        has_pt=has_pt,
        has_act=has_act,
        has_memorando=has_memorando,
        has_ted=has_ted,
    )


def _render_collection_tab(bundle: Dict[str, Any]) -> None:
    meta = _collection_state()
    running = meta.get("status") == "em execucao"

    left, right = st.columns([1.2, 1])
    with left:
        selected_labels = st.multiselect(
            "Tipos documentais",
            options=list(DOCUMENT_TYPE_OPTIONS.keys()),
            default=list(DOCUMENT_TYPE_OPTIONS.keys()),
            key="collection_types",
        )
        with st.expander("Opcoes avancadas", expanded=False):
            max_internos = st.number_input("Max internos", min_value=0, step=1, value=0, key="collection_max_internos")
            max_processos = st.number_input("Max processos por interno", min_value=0, step=1, value=0, key="collection_max_processos")

        start_disabled = running or not selected_labels
        if st.button("Executar coleta", type="primary", disabled=start_disabled, use_container_width=True):
            _start_collection(selected_labels, int(max_internos), int(max_processos))
            st.rerun()

        if st.button("Atualizar dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    with right:
        _status_badge(str(meta.get("status", "")))
        st.write(f"Inicio: `{meta.get('started_at') or '-'}`")
        st.write(f"Fim: `{meta.get('finished_at') or '-'}`")
        st.write(f"Tipos: `{', '.join(meta.get('types') or []) or '-'}`")
        st.write(f"PID: `{meta.get('pid') or '-'}`")
        st.write(f"Retorno: `{meta.get('returncode') if meta.get('returncode') is not None else '-'}`")
        if meta.get("command"):
            st.code(meta["command"], language="bash")

    runtime = runtime_for_processes(bundle.get("performance", {}), bundle.get("overview", pd.DataFrame()).get("processo", []))
    c1, c2, c3 = st.columns(3)
    c1.metric("Tempo total da rodada", f"{runtime['total_minutes']:.2f} min")
    c2.metric("Tempo medio", f"{runtime['avg_seconds']:.2f} s")
    c3.metric("Linhas de log", len(bundle.get("log_entries", [])))

    st.subheader("Ultimas linhas do log")
    log_df = latest_log_rows(bundle.get("log_entries", []), limit=20)
    if log_df.empty:
        st.info("Nenhum log disponivel.")
    else:
        st.dataframe(log_df, use_container_width=True, hide_index=True)


def _render_overview_tab(bundle: Dict[str, Any], overview_df: pd.DataFrame) -> None:
    if overview_df.empty:
        st.info("Nenhum dado consolidado encontrado em backend/output/dashboard_ready_latest.csv.")
        return

    runtime = runtime_for_processes(bundle.get("performance", {}), overview_df["processo"].tolist())
    log_summary = summarize_log_entries(bundle.get("log_entries", []))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Processos", len(overview_df))
    c2.metric("Tempo total", f"{runtime['total_minutes']:.2f} min")
    c3.metric("Tempo medio", f"{runtime['avg_seconds']:.2f} s")
    c4.metric("Warnings / Errors", f"{log_summary['warning']} / {log_summary['error']}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("PT gold", int(overview_df["pt_gold"].sum()))
    c6.metric("ACT canonico", int(overview_df["act_gold"].sum()))
    c7.metric("Memorando gold", int(overview_df["memorando_gold"].sum()))
    c8.metric("TED gold", int(overview_df["ted_gold"].sum()))

    coverage_df = pd.DataFrame(
        [
            {"tipo": "PT", "total": int(overview_df["pt_present"].sum())},
            {"tipo": "ACT", "total": int(overview_df["act_present"].sum())},
            {"tipo": "Memorando", "total": int(overview_df["memorando_present"].sum())},
            {"tipo": "TED", "total": int(overview_df["ted_present"].sum())},
        ]
    )
    quality_df = overview_df.groupby("quality_status", as_index=False).size().rename(columns={"size": "total"})
    act_quality_df = overview_df.groupby("act_quality", as_index=False).size().rename(columns={"size": "total"})
    attempts_df = overview_df.sort_values("act_attempts_count", ascending=False).head(10)

    left, right = st.columns(2)
    _plotly_chart(
        left,
        px.bar(coverage_df, x="tipo", y="total", text="total", title="Cobertura por tipo documental"),
        "overview_coverage_chart",
    )
    if not quality_df.empty:
        _plotly_chart(
            right,
            px.pie(quality_df, names="quality_status", values="total", title="Distribuicao de quality_status"),
            "overview_quality_status_chart",
        )

    left2, right2 = st.columns(2)
    if not act_quality_df.empty:
        _plotly_chart(
            left2,
            px.bar(act_quality_df, x="act_quality", y="total", text="total", title="Distribuicao de act_quality"),
            "overview_act_quality_chart",
        )
    if not attempts_df.empty:
        _plotly_chart(
            right2,
            px.bar(
                attempts_df,
                x="processo",
                y="act_attempts_count",
                color="quality_status",
                title="Top processos por tentativas/rejeicoes de ACT",
                text="act_attempts_count",
            ),
            "overview_act_attempts_chart",
        )

    st.subheader("Tabela consolidada por processo")
    overview_table = overview_df[
        [
            "processo",
            "preview_parceiro",
            "pt_quality",
            "act_quality",
            "memorando_gold",
            "ted_quality",
            "quality_status",
            "quality_notes",
            "act_rejection_summary",
        ]
    ].copy()
    st.dataframe(overview_table, use_container_width=True, hide_index=True)

    st.subheader("Explorador por processo")
    selected_process = st.selectbox("Selecione um processo", overview_df["processo"].tolist(), key="overview_explorer")
    payload = process_explorer_payload(bundle, selected_process)
    section_left, section_right = st.columns(2)
    with section_left:
        st.caption("Overview")
        st.json(payload.get("overview", {}))
        st.caption("PT")
        st.json(payload.get("pt", {}))
        st.caption("ACT")
        st.json(payload.get("act", {}))
    with section_right:
        st.caption("Memorando")
        st.json(payload.get("memorando", {}))
        st.caption("TED")
        st.json(payload.get("ted", {}))


def _render_pt_tab(bundle: Dict[str, Any], filtered_processes: List[str]) -> None:
    pt_status_df = filter_by_processes(bundle.get("pt_status", pd.DataFrame()), filtered_processes)
    pt_detail_df = filter_by_processes(pt_detail_dataframe(bundle), filtered_processes)
    pt_metrics_df = pt_process_metrics(pt_detail_df) if not pt_detail_df.empty else pd.DataFrame()
    metas_df = explode_pt_metas(pt_detail_df) if not pt_detail_df.empty else pd.DataFrame()
    acoes_df = explode_pt_acoes(pt_detail_df) if not pt_detail_df.empty else pd.DataFrame()

    found_processes = pt_status_df[pt_status_df.get("found", False)]["processo"].nunique() if not pt_status_df.empty else 0
    gold_processes = pt_status_df[pt_status_df.get("publication_status", "") == "published_gold"]["processo"].nunique() if not pt_status_df.empty else 0
    silver_processes = pt_status_df[(pt_status_df.get("found", False)) & (pt_status_df.get("publication_status", "") != "published_gold")]["processo"].nunique() if not pt_status_df.empty else 0
    not_found_processes = pt_status_df[~pt_status_df.get("found", False)]["processo"].nunique() if not pt_status_df.empty else 0
    pt_with_metas = pt_detail_df[pt_detail_df["has_metas"]]["processo"].nunique() if not pt_detail_df.empty else 0
    pt_with_acoes = pt_detail_df[pt_detail_df["has_acoes"]]["processo"].nunique() if not pt_detail_df.empty else 0
    pt_with_prazo = pt_detail_df[pt_detail_df["has_prazo_estruturado"]]["processo"].nunique() if not pt_detail_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("PT encontrado", int(found_processes))
    c2.metric("PT gold", int(gold_processes))
    c3.metric("PT silver", int(silver_processes))
    c4.metric("PT nao encontrado", int(not_found_processes))

    c5, c6, c7 = st.columns(3)
    c5.metric("PT com metas", int(pt_with_metas))
    c6.metric("PT com acoes", int(pt_with_acoes))
    c7.metric("PT com prazo estruturado", int(pt_with_prazo))

    if pt_detail_df.empty:
        st.info("Nenhum PT disponivel para os filtros atuais.")
        return

    status_df = pt_detail_df.groupby("normalization_status", as_index=False).size().rename(columns={"size": "total"})
    partners_df = (
        pt_detail_df[pt_detail_df["publication_status"] == "published_gold"]
        .groupby("parceiro", as_index=False)
        .size()
        .rename(columns={"size": "total"})
        .sort_values("total", ascending=False)
        .head(10)
    )
    period_df = pt_detail_df.groupby("period_source", as_index=False).size().rename(columns={"size": "total"})
    timeline_df = pt_detail_df.dropna(subset=["vigencia_fim"]).copy()
    if not timeline_df.empty:
        timeline_df["vigencia_mes"] = timeline_df["vigencia_fim"].dt.to_period("M").astype(str)
        timeline_df = (
            timeline_df.groupby("vigencia_mes", as_index=False)
            .size()
            .rename(columns={"size": "total"})
            .sort_values("vigencia_mes")
        )

    metrics_chart_df = pt_metrics_df.sort_values(["metas_count", "acoes_count"], ascending=False).head(10) if not pt_metrics_df.empty else pd.DataFrame()

    left, right = st.columns(2)
    _plotly_chart(
        left,
        px.bar(status_df, x="normalization_status", y="total", text="total", title="Distribuicao de normalization_status"),
        "pt_normalization_status_chart",
    )
    if not partners_df.empty:
        _plotly_chart(
            right,
            px.bar(partners_df, x="parceiro", y="total", text="total", title="Parceiros com mais PT gold"),
            "pt_gold_partners_chart",
        )

    left2, right2 = st.columns(2)
    if not timeline_df.empty:
        _plotly_chart(
            left2,
            px.line(timeline_df, x="vigencia_mes", y="total", markers=True, title="Timeline de vigencia_fim"),
            "pt_vigencia_timeline_chart",
        )
    if not period_df.empty:
        _plotly_chart(
            right2,
            px.pie(period_df, names="period_source", values="total", title="Distribuicao de period_source"),
            "pt_period_source_chart",
        )

    if not metrics_chart_df.empty:
        _plotly_chart(
            st,
            px.bar(
                metrics_chart_df,
                x="processo",
                y=["metas_count", "acoes_count"],
                barmode="group",
                title="Quantidade de metas e acoes por processo",
            ),
            "pt_metrics_chart",
        )

    st.subheader("Tabela por processo")
    process_table = pt_detail_df[
        [
            "processo",
            "parceiro",
            "objeto",
            "vigencia_inicio",
            "vigencia_fim",
            "prazo_inicio",
            "prazo_fim",
            "normalization_status",
            "publication_status",
        ]
    ].copy()
    for column in ("vigencia_inicio", "vigencia_fim", "prazo_inicio", "prazo_fim"):
        process_table[column] = _format_date_column(process_table[column])
    st.dataframe(process_table, use_container_width=True, hide_index=True)

    st.subheader("Metas detalhadas")
    if metas_df.empty:
        st.info("Nenhuma meta estruturada encontrada.")
    else:
        st.dataframe(metas_df, use_container_width=True, hide_index=True)

    st.subheader("Acoes detalhadas")
    if acoes_df.empty:
        st.info("Nenhuma acao estruturada encontrada.")
    else:
        st.dataframe(
            acoes_df[["processo", "parceiro", "acao_ref", "descricao", "responsavel", "periodo_raw"]],
            use_container_width=True,
            hide_index=True,
        )


def _render_act_tab(bundle: Dict[str, Any], overview_df: pd.DataFrame, filtered_processes: List[str]) -> None:
    act_status_df = filter_by_processes(bundle.get("act_status", pd.DataFrame()), filtered_processes)
    act_normalized_df = filter_by_processes(bundle.get("act_normalized", pd.DataFrame()), filtered_processes)
    act_rejections_df = parse_act_rejection_summary(overview_df)

    any_candidate = act_status_df[act_status_df.get("found", False)]["processo"].nunique() if not act_status_df.empty else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Processos com candidato ACT", int(any_candidate))
    c2.metric("ACT canonico gold", int(overview_df["act_gold"].sum()))
    c3.metric("gold_complete", int((overview_df["act_quality"] == "gold_complete").sum()))
    c4.metric("gold_partial", int((overview_df["act_quality"] == "gold_partial").sum()))

    c5, c6, c7 = st.columns(3)
    c5.metric("not_found", int((overview_df["act_quality"] == "not_found").sum()))
    c6.metric("silver_only", int((overview_df["act_quality"] == "silver_only").sum()))
    c7.metric("Divergencia de processo", int(overview_df["has_process_mismatch"].sum()))

    if overview_df.empty:
        st.info("Nenhum ACT disponivel para os filtros atuais.")
        return

    act_quality_df = overview_df.groupby("act_quality", as_index=False).size().rename(columns={"size": "total"})
    convenente_df = (
        overview_df[overview_df["act_gold"]]
        .groupby("act_orgao_convenente", as_index=False)
        .size()
        .rename(columns={"size": "total"})
        .sort_values("total", ascending=False)
        .head(10)
    )
    timeline_df = overview_df.dropna(subset=["act_data_fim_vigencia"]).copy()
    if not timeline_df.empty:
        timeline_df["vigencia_mes"] = timeline_df["act_data_fim_vigencia"].dt.to_period("M").astype(str)
        timeline_df = (
            timeline_df.groupby("vigencia_mes", as_index=False)
            .size()
            .rename(columns={"size": "total"})
            .sort_values("vigencia_mes")
        )
    rejection_chart_df = (
        act_rejections_df.groupby("rejection", as_index=False)["count"].sum().sort_values("count", ascending=False).head(10)
        if not act_rejections_df.empty
        else pd.DataFrame()
    )

    left, right = st.columns(2)
    _plotly_chart(
        left,
        px.bar(act_quality_df, x="act_quality", y="total", text="total", title="Distribuicao de act_quality"),
        "act_quality_chart",
    )
    if not convenente_df.empty:
        _plotly_chart(
            right,
            px.bar(convenente_df, x="act_orgao_convenente", y="total", text="total", title="Orgaos convenentes dos ACTs gold"),
            "act_convenente_chart",
        )

    left2, right2 = st.columns(2)
    if not timeline_df.empty:
        _plotly_chart(
            left2,
            px.line(timeline_df, x="vigencia_mes", y="total", markers=True, title="Timeline de act_data_fim_vigencia"),
            "act_vigencia_timeline_chart",
        )
    if not rejection_chart_df.empty:
        _plotly_chart(
            right2,
            px.bar(rejection_chart_df, x="rejection", y="count", text="count", title="Rejeicoes por act_rejection_summary"),
            "act_rejections_chart",
        )

    st.subheader("ACTs canonicos")
    canonical_table = overview_df[
        [
            "processo",
            "act_numero_acordo",
            "act_orgao_convenente",
            "act_objeto",
            "act_data_inicio_vigencia",
            "act_data_fim_vigencia",
            "act_quality",
        ]
    ].copy()
    canonical_table = canonical_table[overview_df["act_gold"]]
    for column in ("act_data_inicio_vigencia", "act_data_fim_vigencia"):
        canonical_table[column] = _format_date_column(canonical_table[column])
    st.dataframe(canonical_table, use_container_width=True, hide_index=True)

    st.subheader("Processos problematicos e trilha de rejeicoes")
    problematic_df = overview_df[
        (~overview_df["act_gold"])
        | (overview_df["has_process_mismatch"])
        | (overview_df["act_attempts_count"] > 1)
    ][["processo", "quality_notes", "act_attempts_count", "act_rejection_summary"]].copy()
    st.dataframe(problematic_df, use_container_width=True, hide_index=True)

    if not act_normalized_df.empty:
        with st.expander("Detalhe do CSV act_normalizado_latest.csv", expanded=False):
            st.dataframe(act_normalized_df, use_container_width=True, hide_index=True)


def _render_memorando_tab(bundle: Dict[str, Any], filtered_processes: List[str]) -> None:
    memorando_status_df = filter_by_processes(bundle.get("memorando_status", pd.DataFrame()), filtered_processes)
    memorando_detail_df = filter_by_processes(memorando_detail_dataframe(bundle), filtered_processes)

    found = memorando_status_df[memorando_status_df.get("found", False)]["processo"].nunique() if not memorando_status_df.empty else 0
    published = memorando_status_df[memorando_status_df.get("publication_status", "") == "published_gold"]["processo"].nunique() if not memorando_status_df.empty else 0
    not_found = memorando_status_df[~memorando_status_df.get("found", False)]["processo"].nunique() if not memorando_status_df.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Encontrados", int(found))
    c2.metric("Publicados", int(published))
    c3.metric("Nao encontrados", int(not_found))

    if memorando_detail_df.empty:
        st.info("Nenhum memorando encontrado para os filtros atuais.")
        return

    if len(memorando_detail_df) > 1:
        mode_df = memorando_detail_df.groupby("snapshot_mode", as_index=False).size().rename(columns={"size": "total"})
        _plotly_chart(
            st,
            px.bar(mode_df, x="snapshot_mode", y="total", text="total", title="Memorandos por modo de extracao"),
            "memorando_snapshot_mode_chart",
        )

    st.dataframe(memorando_detail_df, use_container_width=True, hide_index=True)


def _render_ted_tab(bundle: Dict[str, Any], filtered_processes: List[str]) -> None:
    ted_status_df = filter_by_processes(bundle.get("ted_status", pd.DataFrame()), filtered_processes)
    ted_detail_df = filter_by_processes(ted_detail_dataframe(bundle), filtered_processes)

    found = ted_status_df[ted_status_df.get("found", False)]["processo"].nunique() if not ted_status_df.empty else 0
    published = ted_status_df[ted_status_df.get("publication_status", "") == "published_gold"]["processo"].nunique() if not ted_status_df.empty else 0
    skipped_no_instrument = ted_status_df[ted_status_df.get("selection_reason", "") == "skipped_no_instrument_number"]["processo"].nunique() if not ted_status_df.empty else 0
    skipped_without_act = ted_status_df[ted_status_df.get("selection_reason", "") == "skipped_without_prior_act"]["processo"].nunique() if not ted_status_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Encontrados", int(found))
    c2.metric("Publicados", int(published))
    c3.metric("Sem numero de instrumento", int(skipped_no_instrument))
    c4.metric("Sem ACT previo", int(skipped_without_act))

    if not ted_status_df.empty:
        reasons_df = ted_status_df.groupby("selection_reason", as_index=False).size().rename(columns={"size": "total"})
        _plotly_chart(
            st,
            px.bar(reasons_df, x="selection_reason", y="total", text="total", title="Distribuicao dos motivos de ausencia"),
            "ted_selection_reason_chart",
        )

    if ted_detail_df.empty:
        st.info("Nenhum TED publicado na rodada atual.")
        return

    c5, c6 = st.columns(2)
    c5.metric("Soma valor_global", f"{ted_detail_df['valor_global_num'].sum():,.2f}")
    c6.metric("UFs", int(ted_detail_df["uf"].nunique()))

    left, right = st.columns(2)
    if ted_detail_df["situacao"].astype(str).str.strip().any():
        situacao_df = ted_detail_df.groupby("situacao", as_index=False).size().rename(columns={"size": "total"})
        _plotly_chart(
            left,
            px.bar(situacao_df, x="situacao", y="total", text="total", title="Situacao dos TEDs"),
            "ted_situacao_chart",
        )
    if ted_detail_df["uf"].astype(str).str.strip().any():
        uf_df = ted_detail_df.groupby("uf", as_index=False).size().rename(columns={"size": "total"})
        _plotly_chart(
            right,
            px.bar(uf_df, x="uf", y="total", text="total", title="UF dos TEDs"),
            "ted_uf_chart",
        )

    st.dataframe(
        ted_detail_df[["processo", "objeto", "valor_global", "situacao", "uf", "json_path"]],
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="Dashboard SEI", layout="wide")
    _inject_css()
    _render_page_header()

    bundle = _refresh_bundle()
    overview_df = bundle.get("overview", pd.DataFrame())
    filtered_overview_df = _render_sidebar_filters(overview_df) if not overview_df.empty else overview_df
    filtered_processes = filtered_overview_df["processo"].tolist() if not filtered_overview_df.empty else []

    tabs = st.tabs(["Coleta", "Visao Geral", "PT", "ACT", "Memorando", "TED"])
    with tabs[0]:
        _render_collection_tab(bundle)
    with tabs[1]:
        _render_overview_tab(bundle, filtered_overview_df)
    with tabs[2]:
        _render_pt_tab(bundle, filtered_processes)
    with tabs[3]:
        _render_act_tab(bundle, filtered_overview_df, filtered_processes)
    with tabs[4]:
        _render_memorando_tab(bundle, filtered_processes)
    with tabs[5]:
        _render_ted_tab(bundle, filtered_processes)


if __name__ == "__main__":
    main()
