# app_spf_portal.py
# --------------------------------------------------------------
# Streamlit portal for sanitized Re‑Stock & Outstanding POs
# - Username/password login (streamlit-authenticator)
# - User → allowed Location(s) mapping
# - Filter by Location2 (+ quick text filters)
# - Display table
# - Download as Excel (.xlsx) or Word (.docx with gridlines)
# - Reads from SQLite created by your converter:
#     C:\\Users\\Brad\\Desktop\\XRefApp\\data\\maintainx_po.db
#
# How to run:
#   pip install -r requirements.txt
#   streamlit run app_spf_portal.py
#
# Optional config: create app_config.yaml alongside this file to override
# users, passwords, and DB path. See CONFIG_TEMPLATE_YAML below.

from __future__ import annotations
import os, io, sqlite3, textwrap
from pathlib import Path
import pandas as pd
import streamlit as st
import yaml

try:
    import streamlit_authenticator as stauth
except Exception:
    st.warning("streamlit-authenticator not installed. Run: pip install streamlit-authenticator")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.warning("python-docx not installed. Run: pip install python-docx")
    st.stop()

st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

# ---------- Defaults & config ----------
DEFAULT_DB = r"C:\\Users\\Brad\\Desktop\\XRefApp\\data\\maintainx_po.db"
CONFIG_TEMPLATE_YAML = """
credentials:
  usernames:
    demo:
      name: Demo User
      email: demo@example.com
      # bcrypt hash for password 'demo'
      password: "$2b$12$y2J3Y0rRrJ3fA76h2o//mO6F1T0m3b1vS7QhQ4bW5iX9b5b5b5b5e"

cookie:
  name: spf_po_portal
  key: change_me_in_yaml
  expiry_days: 7

access:
  admin_usernames: [demo]
  user_locations:
    demo: ['*']  # '*' means all locations

settings:
  db_path: ""  # leave blank to use DEFAULT_DB
"""

HERE = Path(__file__).resolve().parent

# --- DB path resolver ---
# Priority:
# 1) settings.db_path from YAML (st.secrets['app_config_yaml'] or local app_config.yaml)
# 2) env var SPF_DB_PATH
# 3) GitHub private repo download via st.secrets['github'] (repo, path, branch, token)
# If #3, we fetch to a temp file and return its local path.

def resolve_db_path(cfg: dict) -> str:
    # 1) YAML-configured path
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db:
        return yaml_db
    # 2) environment override
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db:
        return env_db
    # 3) GitHub download using secrets
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
    # Fallback to local default (works when running on your PC)
    return DEFAULT_DB


def download_db_from_github(*, repo: str, path: str, branch: str = 'main', token: str | None = None) -> str:
    """Download a file from a (possibly private) GitHub repo to an app temp path and return its filename.
    Expects st.secrets['github'] with keys: repo ("owner/name"), path ("data/maintainx_po.db"), branch, token.
    """
    if not repo or not path:
        raise ValueError("Missing repo/path for GitHub download.")
    import requests, tempfile
    # Use GitHub API to support private repos
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    # Save to a temp file within the Streamlit runtime
    tmpdir = Path(tempfile.gettempdir()) / "spf_po_cache"
    tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / "maintainx_po.db"
    out.write_bytes(r.content)
    return str(out)


def load_config() -> dict:
    # Priority: Streamlit secrets → app_config.yaml → built-in template
    if "app_config_yaml" in st.secrets:
        cfg = yaml.safe_load(st.secrets["app_config_yaml"]) or {}
    else:
        cfg_file = HERE / "app_config.yaml"
        if cfg_file.exists():
            cfg = yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        else:
            cfg = yaml.safe_load(CONFIG_TEMPLATE_YAML)
    return cfg


def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def view_exists(view_name: str, db_path: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='view' AND name=?", (view_name,))
        return cur.fetchone() is not None


def table_exists(table_name: str, db_path: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cur.fetchone() is not None


def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter  # ensure engine available
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]
        ws.autofilter(0, 0, max(0,len(df)), max(0,len(df.columns)-1))
        # Auto-widths
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
    # header
    for j, c in enumerate(df.columns):
        tbl.cell(0, j).text = str(c)
    # body
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        for j, c in enumerate(df.columns):
            v = '' if pd.isna(r[c]) else str(r[c])
            tbl.cell(i, j).text = v
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()


# ---------- App ----------
cfg = load_config()
auth = stauth.Authenticate(
    cfg.get('credentials', {}),
    cfg.get('cookie', {}).get('name', 'spf_po_cookie'),
    cfg.get('cookie', {}).get('key', 'change_me'),
    cfg.get('cookie', {}).get('expiry_days', 7),
)

# works on current streamlit-authenticator releases
name, auth_status, username = auth.login(location="main")


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

    # Choose dataset
    ds = st.sidebar.radio('Dataset', ['RE-STOCK', "Outstanding POs"], index=0)

    # Access control: which locations can this user see?
    user_locs = cfg.get('access', {}).get('user_locations', {}).get(username, ['*'])
    is_admin = username in cfg.get('access', {}).get('admin_usernames', [])

    # Determine source view/table and available locations
    if ds == 'RE-STOCK':
        src = 'vw_restock_by_location' if view_exists('vw_restock_by_location', db_path) else 'restock'
    else:
        if view_exists('vw_outstanding_by_due', db_path):
            src = 'vw_outstanding_by_due'
        elif table_exists('po_outstanding', db_path):
            src = 'po_outstanding'
        else:
            st.warning('No Outstanding PO data found in DB.'); st.stop()

    # Pull locations present in DB
    locs_df = q(f"SELECT DISTINCT [Location2] FROM [{src}] WHERE [Location2] IS NOT NULL ORDER BY 1", db_path=db_path)
    all_locs = [str(x) for x in locs_df['Location2'].dropna().tolist()]

    # Effective selectable set per user
    if '*' in user_locs:
        selectable = all_locs
    else:
        selectable = [x for x in all_locs if x in user_locs] or all_locs  # fallback to all if mapping is stale

    # Simple ALL toggle + optional multi-select
    all_toggle = st.sidebar.checkbox('All locations', value=True)
    if all_toggle:
        chosen = selectable
    else:
        chosen = st.sidebar.multiselect('Location(s)', options=selectable, default=selectable)
        if not chosen:
            chosen = selectable

    # Quick text filters
    if ds == 'RE-STOCK':
        search = st.sidebar.text_input('Search Part Number / Name contains')
    else:
        search = st.sidebar.text_input('Search PO / Vendor / Part / Name contains')

    # Build SQL with filters
    placeholders = ','.join(['?'] * len(chosen))
    where = [f"[Location2] IN ({placeholders})"]
    params: list = list(chosen)

    if search:
        like = f"%{search}%"
        if ds == 'RE-STOCK':
            where.append("([Part Number] LIKE ? OR [Name] LIKE ?)")
            params += [like, like]
        else:
            where.append("([PO] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Name] LIKE ?)")
            params += [like, like, like, like]

    where_sql = " AND ".join(where)
    sql = f"SELECT * FROM [{src}] WHERE {where_sql}"

    df = q(sql, tuple(params), db_path=db_path)

    # Title & table
    title_locs = 'All locations' if set(chosen) == set(selectable) else (', '.join(chosen) if len(chosen) <= 5 else f'{len(chosen)} locations')
    st.markdown(f"### {ds} — {title_locs}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads
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
            data=to_docx_bytes(df, title=f"{ds} — {', '.join(chosen)}"),
            file_name=f"{ds.replace(' ', '_')}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    with st.expander('ℹ️ Info / Help'):
        st.write("Source:", src)
        st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')

