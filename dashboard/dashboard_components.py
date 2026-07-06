from __future__ import annotations

import base64
import html
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import streamlit as st

from .category_models import VIGENCIA_COLORS, VIGENCIA_LABELS
from .data_cleaning import display_text, format_currency


ORG_NAME = "CENSIPAM"
SYSTEM_NAME = "Dashboard Gerencial de Projetos"
SYSTEM_SUBTITLE = "Painel de acompanhamento de projetos, parcerias e instrumentos"


def _img_to_base64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


def _logo_html(path: Path, *, image_class: str) -> str:
    if path.exists():
        return f"<img src='data:image/png;base64,{_img_to_base64(path)}' alt='{ORG_NAME}'/>"
    return f"<span class='{image_class}'>{ORG_NAME}</span>"


def render_login_header(logo_path: Path) -> None:
    logo_html = _logo_html(logo_path, image_class="logo-fallback-text")
    st.markdown(
        f"""
<div class="login-ajuste">
  <div class="login-header">
    <div class="login-logo">{logo_html}</div>
    <div>
      <div class="login-title">{SYSTEM_NAME}</div>
      <div class="login-subtitle">{SYSTEM_SUBTITLE}</div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


def render_authenticated_header(collection_meta: dict[str, Any], username: str, logo_path: Path) -> bool:
    collected_at = collection_meta.get("data_ultima_coleta_display") or "Nao identificada"
    logo_html = _logo_html(logo_path, image_class="logo-fallback-text")
    username_safe = html.escape(username or "-")
    collected_at_safe = html.escape(str(collected_at))

    with st.container(key="authenticated_header"):
        left, right = st.columns([5, 1.15], vertical_alignment="center")
        with left:
            st.markdown(
                f"""
<div class="app-header">
  <div class="app-logo">{logo_html}</div>
  <div>
    <div class="app-org">{ORG_NAME}</div>
    <div class="app-title">{SYSTEM_NAME}</div>
    <div class="app-subtitle">{SYSTEM_SUBTITLE}</div>
    <div class="app-meta">Ultima coleta disponivel: {collected_at_safe}</div>
  </div>
</div>
""",
                unsafe_allow_html=True,
            )
        with right:
            st.markdown(
                f"""
<div class="app-user-box">
  <div class="app-user-label">Usuario autenticado</div>
  <div class="app-user-name">{username_safe}</div>
</div>
""",
                unsafe_allow_html=True,
            )
            return st.button("Sair", key="auth_logout_button", use_container_width=True)


def render_header(collection_meta: dict[str, Any]) -> None:
    collected_at = collection_meta.get("data_ultima_coleta_display") or "Não identificada"
    st.markdown(
        f"""
        <div class="dashboard-header">
            <div class="dashboard-title">Dashboard Gerencial de Projetos</div>
            <div class="dashboard-subtitle">Última coleta disponível: {collected_at}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_cards(cards: Iterable[tuple[str, Any]]) -> None:
    cards = list(cards)
    if not cards:
        return
    columns = st.columns(len(cards))
    for column, (label, value) in zip(columns, cards):
        column.metric(label, value)


def render_dataframe(df: pd.DataFrame, *, empty_message: str) -> None:
    if df.empty:
        st.info(empty_message)
        return
    st.dataframe(df, width="stretch", hide_index=True)


def _vigencia_color_map() -> dict[str, str]:
    color_map = {VIGENCIA_LABELS[key]: value for key, value in VIGENCIA_COLORS.items()}
    color_map.update(VIGENCIA_COLORS)
    return color_map


def _vigencia_cell_style(value: Any) -> str:
    color = _vigencia_color_map().get(str(value or "").strip())
    if not color:
        return ""
    return f"background-color: {color}; color: #FFFFFF; font-weight: 700;"


def render_vigencia_dataframe(df: pd.DataFrame, *, empty_message: str) -> None:
    if df.empty:
        st.info(empty_message)
        return
    if "Situação" not in df.columns:
        st.dataframe(df, width="stretch", hide_index=True)
        return
    styler = df.style
    if hasattr(styler, "map"):
        styler = styler.map(_vigencia_cell_style, subset=["Situação"])
    else:
        styler = styler.applymap(_vigencia_cell_style, subset=["Situação"])
    st.dataframe(styler, width="stretch", hide_index=True)


def plot_bar(df: pd.DataFrame, *, x: str, y: str, title: str, color: str | None = None, text: str | None = None, key: str) -> None:
    if df.empty:
        st.info("Não há dados suficientes para o gráfico.")
        return
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color,
        text=text,
        title=title,
        color_discrete_map=_vigencia_color_map(),
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        font={"family": "Inter, sans-serif", "size": 13},
        title={"x": 0, "font": {"size": 16}},
        margin={"l": 10, "r": 10, "t": 48, "b": 10},
        showlegend=False,
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E5E7EB", zeroline=False)
    st.plotly_chart(fig, width="stretch", key=key, config={"displayModeBar": False, "responsive": True})


def plot_pie(
    df: pd.DataFrame,
    *,
    names: str,
    values: str,
    title: str,
    color: str | None = None,
    key: str,
) -> None:
    if df.empty:
        st.info("Não há dados suficientes para o gráfico.")
        return
    fig = px.pie(
        df,
        names=names,
        values=values,
        color=color or names,
        title=title,
        hole=0.35,
        color_discrete_map=_vigencia_color_map(),
    )
    fig.update_traces(textposition="inside", textinfo="percent+label", sort=False)
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        font={"family": "Inter, sans-serif", "size": 13},
        title={"x": 0, "font": {"size": 16}},
        margin={"l": 10, "r": 10, "t": 48, "b": 10},
        legend={"orientation": "h", "y": -0.05},
    )
    st.plotly_chart(fig, width="stretch", key=key, config={"displayModeBar": False, "responsive": True})


def plot_horizontal_value_ranking(df: pd.DataFrame, *, key: str) -> None:
    if df.empty:
        st.info("Não há valores válidos suficientes para montar o ranking de TEDs.")
        return
    fig = px.bar(
        df.sort_values("valor_global_num", ascending=True),
        x="valor_global_num",
        y="label",
        orientation="h",
        text="valor_global_display",
        title="Maiores TEDs por valor global",
        labels={"valor_global_num": "Valor Global", "label": "TED"},
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        font={"family": "Inter, sans-serif", "size": 13},
        title={"x": 0, "font": {"size": 16}},
        margin={"l": 10, "r": 10, "t": 48, "b": 10},
    )
    fig.update_xaxes(showgrid=True, gridcolor="#E5E7EB", zeroline=False)
    fig.update_yaxes(showgrid=False, zeroline=False)
    st.plotly_chart(fig, width="stretch", key=key, config={"displayModeBar": False, "responsive": True})


def yes_no(value: Any) -> str:
    return "Sim" if bool(value) else "Não"


def render_active_detail(row: pd.Series) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Processo:** `{row.get('processo', '')}`")
        st.markdown(f"**Documento / instrumento:** {display_text(row.get('documento_instrumento', ''))}")
        st.markdown(f"**Parceiro:** {display_text(row.get('parceiro', ''))}")
        st.markdown(f"**Data de assinatura:** {display_text(row.get('data_assinatura_display', ''))}")
        st.markdown(f"**Vigência:** {row.get('vigencia_inicio_display', '')} a {row.get('vigencia_fim_display', '')}")
        st.markdown(f"**Situação:** {row.get('situacao_display', '')}")
        dias = row.get("dias_restantes")
        st.markdown(f"**Dias restantes:** {'' if pd.isna(dias) else int(dias)}")
    with right:
        st.markdown(f"**Documentos relacionados:** {display_text(row.get('documentos_relacionados', ''))}")
        st.markdown(f"**Plano de Trabalho relacionado:** {yes_no(row.get('possui_pt', False))}")
        st.markdown(f"**TED relacionado:** {yes_no(row.get('possui_ted', False))}")
        st.markdown(f"**Memorandos relacionados:** {yes_no(row.get('possui_memorando', False))}")
        st.markdown(f"**Campos ausentes ou inconsistentes:** {display_text(row.get('campos_ausentes', ''), 'Nenhum campo crítico ausente')}")
        st.markdown(f"**Fonte/origem do dado:** {display_text(row.get('fontes_origem', ''))}")
    with st.expander("Objeto completo e observações", expanded=False):
        st.write(display_text(row.get("objeto_completo", ""), "Objeto não identificado."))
        if display_text(row.get("vigencia_raw", ""), ""):
            st.markdown(f"**Vigência textual original:** {row.get('vigencia_raw')}")
        if display_text(row.get("conflitos", ""), ""):
            st.warning(row.get("conflitos"))


def render_ted_detail(row: pd.Series) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Processo:** `{row.get('processo', '')}`")
        st.markdown(f"**Número do TED:** {display_text(row.get('numero_ted_display', ''))}")
        st.markdown(f"**Valor global:** {format_currency(row.get('valor_global_num', 0.0))}")
        st.markdown(f"**Vigência:** {row.get('vigencia_inicio_display', '')} a {row.get('vigencia_fim_display', '')}")
        st.markdown(f"**Situação da vigência:** {row.get('situacao_display', '')}")
    with right:
        st.markdown(f"**Unidade descentralizadora:** {display_text(row.get('unidade_descentralizadora', ''))}")
        st.markdown(f"**Unidade descentralizada:** {display_text(row.get('unidade_descentralizada', ''))}")
        st.markdown(f"**Campos ausentes:** {display_text(row.get('campos_ausentes', ''), 'Nenhum campo crítico ausente')}")
        st.markdown(f"**Origem dos dados:** {display_text(row.get('fontes_origem', ''))}")
    with st.expander("Objeto, metas e prestação de contas", expanded=False):
        st.markdown("**Objeto completo**")
        st.write(display_text(row.get("objeto_completo", ""), "Objeto não identificado."))
        st.markdown("**Metas ou plano de aplicação**")
        st.write(display_text(row.get("metas", "") or row.get("plano_aplicacao", ""), "Não identificado."))
        st.markdown("**Prestação de contas**")
        st.write(display_text(row.get("prestacao_contas", ""), "Não identificada."))


def render_history_detail(row: pd.Series) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Processo:** `{row.get('processo', '')}`")
        st.markdown(f"**Documento / instrumento:** {display_text(row.get('documento_instrumento', ''))}")
        st.markdown(f"**Parceiro:** {display_text(row.get('parceiro', ''))}")
        st.markdown(f"**Status normalizado:** {display_text(row.get('status_gerencial', ''))}")
        st.markdown(f"**Categoria:** {display_text(row.get('categoria_gerencial', ''))}")
    with right:
        st.markdown(f"**Data de assinatura:** {row.get('data_assinatura_display', '')}")
        st.markdown(f"**Data de encerramento / vencimento:** {row.get('data_vencimento_display', '')}")
        st.markdown(f"**Termo de encerramento:** {display_text(row.get('termo_encerramento_raw', ''))}")
        st.markdown(f"**Campos ausentes:** {display_text(row.get('campos_ausentes', ''), 'Nenhum campo crítico ausente')}")
        st.markdown(f"**Origem dos dados:** {display_text(row.get('fontes_origem', ''))}")
    with st.expander("Objeto completo e observações", expanded=False):
        st.markdown("**Objeto completo**")
        st.write(display_text(row.get("objeto_completo", ""), "Objeto não identificado."))
        if display_text(row.get("observacoes", ""), ""):
            st.markdown("**Observações relevantes**")
            st.write(row.get("observacoes", ""))
        if display_text(row.get("conflitos", ""), ""):
            st.warning(row.get("conflitos"))
