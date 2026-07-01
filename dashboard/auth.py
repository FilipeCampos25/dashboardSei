from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import secrets
import sqlite3
from typing import Any


AUTH_DB_RELATIVE_PATH = Path("backend") / "data" / "dashboard_auth.db"


def get_auth_db_path(root_dir: Path) -> Path:
    return root_dir / AUTH_DB_RELATIVE_PATH


def _get_auth_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db(db_path: Path) -> None:
    with _get_auth_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )


def hash_password(password: str, salt_hex: str) -> str:
    pwd_bytes = password.encode("utf-8")
    salt_bytes = bytes.fromhex(salt_hex)
    return hashlib.pbkdf2_hmac("sha256", pwd_bytes, salt_bytes, 120_000).hex()


def create_user(username: str, password: str, *, db_path: Path, is_admin: bool = False) -> tuple[bool, str]:
    username_clean = username.strip()
    if len(username_clean) < 3:
        return False, "Usuario deve ter ao menos 3 caracteres."
    if len(password) < 6:
        return False, "Senha deve ter ao menos 6 caracteres."

    salt_hex = secrets.token_hex(16)
    password_hash = hash_password(password, salt_hex)
    created_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

    try:
        with _get_auth_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO dashboard_users (username, password_hash, password_salt, is_admin, is_active, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (username_clean, password_hash, salt_hex, 1 if is_admin else 0, created_at),
            )
        return True, "Usuario criado com sucesso."
    except sqlite3.IntegrityError:
        return False, "Usuario ja existe."


def count_users(db_path: Path) -> int:
    with _get_auth_connection(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM dashboard_users").fetchone()
    return int(row["total"]) if row else 0


def authenticate_user(username: str, password: str, *, db_path: Path) -> dict[str, Any] | None:
    username_clean = username.strip()
    with _get_auth_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, username, password_hash, password_salt, is_admin, is_active
            FROM dashboard_users
            WHERE username = ?
            """,
            (username_clean,),
        ).fetchone()

    if not row or int(row["is_active"]) != 1:
        return None

    candidate_hash = hash_password(password, row["password_salt"])
    if not secrets.compare_digest(candidate_hash, row["password_hash"]):
        return None

    return {"id": int(row["id"]), "username": str(row["username"]), "is_admin": bool(row["is_admin"])}


def is_authenticated() -> bool:
    import streamlit as st

    return bool(st.session_state.get("is_authenticated", False))


def clear_auth_session() -> None:
    import streamlit as st

    st.session_state["is_authenticated"] = False
    st.session_state["authenticated_user_id"] = None
    st.session_state["authenticated_username"] = ""
    st.session_state["authenticated_is_admin"] = False


def _set_auth_session(user: dict[str, Any]) -> None:
    import streamlit as st

    st.session_state["is_authenticated"] = True
    st.session_state["authenticated_user_id"] = user["id"]
    st.session_state["authenticated_username"] = user["username"]
    st.session_state["authenticated_is_admin"] = user["is_admin"]


def _disable_password_manager_hints() -> None:
    import streamlit as st

    st.markdown(
        """
<script>
(() => {
  const apply = () => {
    const inputs = document.querySelectorAll('input');
    inputs.forEach((el) => {
      const type = (el.getAttribute('type') || '').toLowerCase();
      const placeholder = (el.getAttribute('placeholder') || '').toLowerCase();
      if (type === 'password' || placeholder.includes('senha')) {
        el.setAttribute('autocomplete', 'new-password');
        el.setAttribute('data-lpignore', 'true');
        el.setAttribute('data-form-type', 'other');
      }
      if (placeholder.includes('usuario')) {
        el.setAttribute('autocomplete', 'off');
        el.setAttribute('data-lpignore', 'true');
        el.setAttribute('data-form-type', 'other');
      }
      if (placeholder.includes('senha') || placeholder.includes('usuario')) {
        el.addEventListener('keydown', (ev) => {
          if (ev.key === 'Enter') {
            ev.preventDefault();
          }
        }, { passive: false });
      }
    });

    document.querySelectorAll('form').forEach((form) => {
      form.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter') {
          ev.preventDefault();
        }
      }, { passive: false });
    });
  };
  apply();
  setTimeout(apply, 300);
})();
</script>
""",
        unsafe_allow_html=True,
    )


def require_dashboard_authentication(root_dir: Path, logo_path: Path) -> None:
    import streamlit as st

    db_path = get_auth_db_path(root_dir)
    init_auth_db(db_path)
    if is_authenticated():
        return

    from .dashboard_components import render_login_header

    with st.container(key="login_center_column"):
        st.markdown("<div id='login_root'></div>", unsafe_allow_html=True)
        render_login_header(logo_path)

        with st.container(key="login_card_container"):
            with st.container(key="login_form_wrapper"):
                if count_users(db_path) == 0:
                    with st.form("bootstrap_admin_form", clear_on_submit=False):
                        admin_username = st.text_input("admin_u", placeholder="usuario", label_visibility="collapsed")
                        admin_password = st.text_input(
                            "admin_p", type="password", placeholder="senha", label_visibility="collapsed"
                        )
                        admin_password_confirm = st.text_input(
                            "admin_pc", type="password", placeholder="confirmar senha", label_visibility="collapsed"
                        )
                        submitted_admin = st.form_submit_button("entrar", use_container_width=False)

                    _disable_password_manager_hints()

                    if submitted_admin:
                        if admin_password != admin_password_confirm:
                            st.error("As senhas nao conferem.")
                        else:
                            created, msg = create_user(
                                admin_username,
                                admin_password,
                                db_path=db_path,
                                is_admin=True,
                            )
                            if created:
                                st.success("Administrador criado. Faca login para continuar.")
                                st.rerun()
                            else:
                                st.error(msg)
                    st.stop()

                with st.form("login_form", clear_on_submit=False):
                    username = st.text_input("u", placeholder="usuario", label_visibility="collapsed")
                    password = st.text_input("p", type="password", placeholder="senha", label_visibility="collapsed")
                    submitted_login = st.form_submit_button("entrar", use_container_width=False)

                _disable_password_manager_hints()

                if submitted_login:
                    user = authenticate_user(username, password, db_path=db_path)
                    if user:
                        _set_auth_session(user)
                        st.rerun()
                    st.error("Usuario ou senha invalidos.")

                st.stop()
