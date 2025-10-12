# app_spf_portal.py
# --------------------------------------------------------------
# SPF portal for RE-STOCK and Outstanding POs
# - Login (streamlit-authenticator)
# - Authorize & filter by Company (not Location)
# - Uses raw tables: restock, po_outstanding (no views)
# - Preserves exact DB column order in grid & downloads
# - Downloads: Excel (.xlsx) and Word (.docx)
#
# requirements.txt (minimum):
#   streamlit>=1.37
#   streamlit-authenticator==0.2.3
#   pandas>=2.0
#   openpyxl>=3.1
#   xlsxwriter>=3.2
#   python-docx>=1.1
#   pyyaml>=6.0
#   requests>=2.31

from __future__ import annotations
import os, io, sqlite3, textwrap
from pathlib import Path
from collections.abc import Mapping
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.11"

# ---- deps ----
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed. Add to requirements.txt")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.error("python-docx not installed. Add to requirements.txt")
    st.stop()

st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

# ---------- Defaults & config ----------
DEFAULT_DB = "maintainx_po.db"   # local fallback; Cloud will use secrets→GitHub

CONFIG_TEMPLATE_YAML = """
credentials:
  usernames:
    demo:
      name: Demo User
      email: demo@example.com
      password: "$2b$12$y2J3Y0rRrJ3fA76h2o//mO6F1T0m3b1vS7QhQ4bW5iX9b5b5b5b5e"

cookie:
  name: spf_po_portal_v3
  key: super_secret_key_v3
  expiry_days: 7

access:
  admin_usernames: [demo]
  user_companies:
    demo: ['*']    # '*' = all companies

settings:
  db_path: ""
"""

HERE = Path(__file__).resolve().parent

# ---------- helpers ----------
def to_plain(obj):
    """Recursively convert Secrets to plain Python structures."""
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj

def resolve_db_path(cfg: dict) -> str:
    # 1) YAML/secrets-configured path
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db:
        return yaml_db
    # 2) SPF_DB_PATH env
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db:
        return env_db
    # 3) Secrets → GitHub download (supports private repo)
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh:
        try:
            return download_db_from_github(
                repo=gh.get('repo'),
                path=gh.get('path'),
                branch=gh.get('branch', 'main'),
                token=gh.get('token'),
            )
        except Exception as e:
            st.error(f"Failed to download DB from GitHub: {e}")
    # 4) Fallback local
    return DEFAULT_DB

def download_db_from_github(*, repo: str, path: str, branch: str = 'main', token: str | None = None) -> str:
    if not repo or not path:
        raise ValueError("Missing repo/path for GitHub download.")
    import requests, tempfile
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    tmpdir = Path(tempfile.gettempdir()) / "spf_po_cache"
    tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / "maintainx_po.db"
    out.write_bytes(r.content)
    return str(out)

def load_config() -> dict:
    if "app_config" in st.secrets:           # TOML secrets (recommended)
        return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:       # legacy YAML in secrets
        try:
            return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}")
            return {}
    cfg_file = HERE / "app_config.yaml"       # local file for dev
    if cfg_file.exists():
        try:
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    return yaml.safe_load(CONFIG_TEMPLATE_YAML)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def table_columns_in_order(db_path: str, table: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]  # PRAGMA preserves on-disk order

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]
        ws.autofilter(0, 0, max(0, len(df)), max(0, len(df.columns) - 1))
        for i, col in enumerate(df.columns):
            width = min(60, max(10, int(df[col].astype(str).str.len().quantile(0.9)) + 2))
            ws.set_column(i, i, width)
    return buf.getvalue()

def to_docx_bytes(df: pd.DataFrame, title: str) -> bytes:
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)
    doc.add_heading(title, level=1)
    rows, cols = len(df) + 1, len(df.columns)
    tbl = doc.add_table(rows=rows, cols=cols)
    tbl.style = 'Table Grid'
    for j, c in enumerate(df.columns):
        tbl.cell(0, j).text = str(c)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        for j, c in enumerate(df.columns):
            v = '' if pd.isna(r[c]) else str(r[c])
            tbl.cell(i, j).text = v
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

# ---------- App ----------
st.sidebar.caption(f"SPF PO Portal — v{APP_VERSION}")
cfg = load_config()
cfg = to_plain(cfg)  # ensure plain dicts

# Auth (pin streamlit-authenticator==0.2.3)
cookie_cfg = cfg.get('cookie', {})
auth = stauth.Authenticate(
    cfg.get('credentials', {}),
    cookie_cfg.get('name', 'spf_po_portal_v3'),
    cookie_cfg.get('key',  'super_secret_key_v3'),
    cookie_cfg.get('expiry_days', 7),
)

name, auth_status, username = auth.login("Login", "main")

if auth_status is False:
    st.error('Username/password is incorrect')
elif auth_status is None:
    st.info('Please log in.')
else:
    auth.logout('Logout', 'sidebar')
    st.sidebar.success(f"Logged in as {name}")

    db_path = resolve_db_path(cfg)
    st.sidebar.caption(f"DB: {db_path}")
    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()

    # Dataset -> table name (radio)
    ds = st.sidebar.radio('Dataset', ['RE-STOCK', 'Outstanding POs'], index=0)
    src = 'restock' if ds == 'RE-STOCK' else 'po_outstanding'

    # --- Authorization by Company (STRICT) & required selection ---
    all_companies_df = q(f"SELECT DISTINCT [Company] FROM [{src}] WHERE [Company] IS NOT NULL ORDER BY 1", db_path=db_path)
    all_companies = [str(x) for x in all_companies_df['Company'].dropna().tolist()] or []

    admin_users = (cfg.get('access', {}).get('admin_usernames', []) or [])
    is_admin = username in admin_users

    allowed_cfg = cfg.get('access', {}).get('user_companies', {}).get(username, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a.strip() for a in (allowed_cfg or [])]

    # Admins get access to everything; others must match exact strings
    if is_admin or "*" in allowed_cfg:
        allowed_set = set(all_companies)
    else:
        allowed_set = {c for c in all_companies if c in set(allowed_cfg)}

    if not allowed_set:
        st.error(
            "You don’t have access to any companies (or the names don’t match the DB exactly). "
            "An admin should update your company list in Secrets to match the exact strings in the database."
        )
        st.stop()

    company_options = sorted(allowed_set)
    ADMIN_ALL = "« All companies (admin) »"

    # Dropdown: no default selection
    select_options = ["— Choose company —"]
    if is_admin and len(company_options) > 1:
        select_options += [ADMIN_ALL]
    select_options += company_options

    chosen = st.sidebar.selectbox("Choose your Company", options=select_options, index=0)

    # Gate: stop until a real choice is made
    if chosen == "— Choose company —":
        st.info("Select your Company on the left to load data.")
        st.stop()

    # Derive chosen_companies for the query and title text
    if is_admin and chosen == ADMIN_ALL:
        chosen_companies = company_options[:]  # all allowed for admin
        title_companies = "All companies (admin)"
    else:
        chosen_companies = [chosen]
        title_companies = chosen

    # UI: search per dataset (only on existing columns)
    if ds == 'RE-STOCK':
        search = st.sidebar.text_input('Search Part Numbers / Name contains')
        search_clause = "([Part Numbers] LIKE ? OR [Name] LIKE ?)"
        search_fields = 2
        order_by = "[Company], [Name]"
    else:
        search = st.sidebar.text_input('Search PO # / Vendor / Part / Line Name contains')
        search_clause = "([Purchase Order #] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Line Name] LIKE ?)"
        search_fields = 4
        order_by = "[Company], date([Created On]) ASC, [Purchase Order #]"

    # Build WHERE (Company required)
    ph = ','.join(['?'] * len(chosen_companies))
    where = [f"[Company] IN ({ph})"]
    params: list = list(chosen_companies)

    if search:
        like = f"%{search}%"
        where.append(search_clause)
        params += [like] * search_fields

    where_sql = " AND ".join(where)
    sql = f"SELECT * FROM [{src}] WHERE {where_sql} ORDER BY {order_by}"
    df = q(sql, tuple(params), db_path=db_path)

    # Preserve on-disk table column order
    cols_in_order = table_columns_in_order(db_path, src)
    df = df[[c for c in cols_in_order if c in df.columns]]

    # Title & grid
    st.markdown(f"### {ds} — {title_companies}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads (use the exact same df)
    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=to_xlsx_bytes(df, sheet=ds.replace(' ', '_')),
            file_name=f"{ds.replace(' ', '_')}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=to_docx_bytes(df, title=f"{ds} — {title_companies}"),
            file_name=f"{ds.replace(' ', '_')}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    with st.expander('ℹ️ Config template'):
        st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')




