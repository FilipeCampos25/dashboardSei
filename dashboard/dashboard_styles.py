from __future__ import annotations

import streamlit as st


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

                --auth-bg: #ffffff;
                --auth-text: #0f172a;
                --auth-muted: #475569;
                --auth-border: #e5e7eb;
                --auth-input-border: #cbd5e1;
                --auth-shadow: 0 18px 50px rgba(2, 6, 23, 0.10);
                --auth-shadow-soft: 0 10px 30px rgba(2, 6, 23, 0.08);
                --auth-radius: 16px;
                --auth-radius-sm: 12px;
                --login-card-width: 440px;
                --login-card-min-height: 420px;
                --login-card-padding-y: 42px;
                --login-card-padding-x: 34px;
                --login-field-width: 280px;
                --login-control-height: 48px;
                --login-control-font-size: 14px;
                --login-column-max-width: 860px;
            }

            html, body, [class*="css"] {
                font-family: 'Inter', 'Segoe UI', sans-serif;
            }

            .stApp {
                background-color: var(--bg);
                color: var(--text);
            }

            .dashboard-header {
                background: var(--card);
                border: 1px solid var(--border);
                border-left: 5px solid var(--accent);
                border-radius: var(--radius);
                padding: 1rem 1.25rem;
                margin-bottom: 1rem;
                box-shadow: var(--shadow);
            }

            .dashboard-title {
                color: var(--primary);
                font-size: 1.55rem;
                font-weight: 700;
                line-height: 1.2;
                margin: 0;
            }

            .dashboard-subtitle {
                color: var(--muted);
                font-size: 0.95rem;
                margin-top: 0.35rem;
            }

            div[data-testid="stMetric"] {
                background: var(--card);
                border-radius: var(--radius);
                border: 1px solid var(--border);
                padding: 0.85rem;
                box-shadow: var(--shadow);
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

            div[data-testid="InputInstructions"],
            div[data-testid="InputInstructions"] * {
                display: none !important;
            }

            header[data-testid="stHeader"] {
                background: transparent !important;
                border-bottom: none !important;
            }

            .st-key-login_center_column {
                min-height: 100vh;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                width: 100%;
                max-width: 100%;
                margin: 0 auto;
            }

            .login-ajuste {
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                width: 100%;
            }

            .login-header {
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 18px;
                margin: 0 auto 22px auto;
                width: min(var(--login-column-max-width), 94vw);
                max-width: 100%;
                text-align: center;
                font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
            }

            .login-logo {
                width: 150px;
                height: 150px;
                display: flex;
                align-items: center;
                justify-content: center;
                flex: 0 0 auto;
                background: transparent;
                filter: drop-shadow(0 6px 14px rgba(2, 6, 23, .12));
            }

            .login-logo img,
            .app-logo img {
                width: 100%;
                height: 100%;
                object-fit: contain;
                display: block;
            }

            .logo-fallback-text {
                color: var(--auth-text);
                font-size: 20px;
                font-weight: 900;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }

            .login-title {
                margin: 0;
                color: var(--auth-text);
                font-size: 30px;
                font-weight: 900;
                line-height: 1.05;
                letter-spacing: 0.6px;
                text-transform: uppercase;
                white-space: nowrap;
            }

            .login-subtitle {
                margin-top: 6px;
                color: var(--auth-muted);
                font-size: 13px;
                font-weight: 700;
                letter-spacing: 0.2px;
            }

            .st-key-login_card_container {
                box-sizing: border-box !important;
                width: min(100%, calc(var(--login-card-width) + var(--login-card-padding-x) + var(--login-card-padding-x))) !important;
                min-height: var(--login-card-min-height) !important;
                height: auto !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                padding: var(--login-card-padding-y) var(--login-card-padding-x) !important;
                border: 1px solid var(--auth-border) !important;
                border-radius: var(--auth-radius) !important;
                background: var(--auth-bg) !important;
                box-shadow: var(--auth-shadow) !important;
                margin-left: auto !important;
                margin-right: auto !important;
                text-align: center !important;
            }

            .st-key-login_center_column div[data-testid="stLayoutWrapper"],
            .st-key-login_card_container div[data-testid="stLayoutWrapper"] {
                display: flex !important;
                justify-content: center !important;
                width: 100% !important;
            }

            .st-key-login_form_wrapper {
                width: var(--login-field-width) !important;
                max-width: 100% !important;
                margin-inline: auto !important;
                display: flex !important;
                flex-direction: column !important;
                justify-content: center !important;
                align-items: center !important;
            }

            .st-key-login_form_wrapper div[data-testid="stForm"] {
                border: none !important;
                padding: 0 !important;
                background: transparent !important;
                width: 100% !important;
                max-width: 100% !important;
                margin-inline: auto !important;
            }

            .st-key-login_form_wrapper div[data-testid="stForm"] form {
                width: 100% !important;
                margin: 0 auto !important;
                display: flex !important;
                flex-direction: column !important;
                align-items: center !important;
                row-gap: 40px !important;
            }

            .st-key-login_form_wrapper div[data-testid="stForm"] .element-container {
                width: 100% !important;
                margin: 0 auto !important;
            }

            .st-key-login_card_container div[data-testid="stTextInput"] > div {
                width: 100% !important;
            }

            .st-key-login_card_container div[data-testid="stTextInput"] > div > div {
                position: relative !important;
                width: 100% !important;
            }

            .st-key-login_card_container div[data-testid="stTextInput"] input {
                width: 100% !important;
                text-align: center !important;
                height: var(--login-control-height);
                font-size: var(--login-control-font-size);
                font-weight: 700;
                border-radius: var(--auth-radius-sm);
                padding-inline: 44px !important;
            }

            .st-key-login_card_container div[data-testid="stTextInput"] button {
                position: absolute !important;
                right: -12px !important;
                top: 50% !important;
                transform: translateY(-50%) !important;
                margin: 0 !important;
                height: 32px !important;
                width: 32px !important;
                min-width: 32px !important;
                padding: 0 !important;
                border: none !important;
                background: transparent !important;
                box-shadow: none !important;
            }

            .st-key-login_card_container div[data-testid="stFormSubmitButton"] {
                width: 100% !important;
                margin: 0 auto !important;
            }

            .st-key-login_card_container div[data-testid="stFormSubmitButton"] button {
                width: 100% !important;
                height: var(--login-control-height);
                border: 1px solid var(--auth-input-border);
                border-radius: var(--auth-radius-sm);
                background-color: #ffffff;
                color: var(--auth-text);
                font-size: var(--login-control-font-size);
                font-weight: 800;
                text-transform: lowercase;
                box-shadow: var(--auth-shadow-soft);
                transition: transform .12s ease, box-shadow .12s ease, background-color .12s ease;
            }

            .st-key-login_card_container div[data-testid="stFormSubmitButton"] button:hover {
                transform: translateY(-1px);
                box-shadow: 0 14px 28px rgba(2, 6, 23, 0.15);
                background-color: #f1f5f9;
            }

            .st-key-login_card_container div[data-testid="stFormSubmitButton"] button:active {
                transform: translateY(0);
                box-shadow: var(--auth-shadow-soft);
                background-color: #e2e8f0;
            }

            .st-key-authenticated_header {
                width: 100%;
                max-width: 1280px;
                margin: 0 auto 1rem auto;
                padding: 0.25rem 0 0.5rem 0;
                font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
            }

            .app-header {
                display: flex;
                align-items: center;
                justify-content: flex-start;
                gap: 20px;
                padding: 18px 6px 8px 6px;
                width: 100%;
            }

            .app-logo {
                width: 170px;
                height: 170px;
                display: flex;
                align-items: center;
                justify-content: center;
                flex: 0 0 auto;
                background: transparent;
                filter: drop-shadow(0 8px 18px rgba(2, 6, 23, .14));
            }

            .app-org {
                color: var(--auth-muted);
                font-size: 12px;
                font-weight: 900;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                margin-bottom: 4px;
            }

            .app-title {
                margin: 0;
                color: var(--auth-text);
                font-size: 34px;
                font-weight: 900;
                line-height: 1.05;
                letter-spacing: 0.7px;
                text-transform: uppercase;
                white-space: nowrap;
            }

            .app-subtitle {
                margin-top: 6px;
                color: var(--auth-muted);
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 0.25px;
            }

            .app-meta {
                margin-top: 8px;
                color: #64748b;
                font-size: 12px;
                font-weight: 600;
            }

            .app-user-box {
                text-align: right;
                color: var(--auth-text);
                margin-bottom: 0.5rem;
            }

            .app-user-label {
                color: var(--auth-muted);
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }

            .app-user-name {
                color: var(--auth-text);
                font-size: 14px;
                font-weight: 800;
                word-break: break-word;
            }

            .st-key-authenticated_header div[data-testid="stButton"] button {
                border: 1px solid var(--auth-input-border);
                border-radius: var(--auth-radius-sm);
                background: #ffffff;
                color: var(--auth-text);
                font-weight: 800;
                box-shadow: var(--auth-shadow-soft);
            }

            .st-key-authenticated_header div[data-testid="stButton"] button:hover {
                background: #f1f5f9;
                border-color: var(--auth-input-border);
                color: var(--auth-text);
            }

            @media (max-width: 900px) {
                .app-header {
                    flex-direction: column;
                    text-align: center;
                }

                .app-title {
                    white-space: normal;
                    font-size: 30px;
                }

                .app-logo {
                    width: 140px;
                    height: 140px;
                }

                .app-user-box {
                    text-align: center;
                }
            }

            @media (max-width: 720px) {
                .login-title {
                    white-space: normal;
                    font-size: 26px;
                }

                .login-logo {
                    width: 120px;
                    height: 120px;
                }
            }

            @media (max-width: 520px) {
                :root {
                    --login-card-width: 92vw;
                    --login-field-width: 76vw;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
