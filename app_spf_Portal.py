# app_spf_portal.py
# --------------------------------------------------------------
# SPF portal for RE-STOCK and Outstanding POs
# - Login (streamlit-authenticator)
# - Authorize & filter by Company (not Location)
# - Uses raw tables: restock, po_outstanding (no views)
# - Preserves exact DB column order in grid & downloads
# - Dates shown as YYYY-MM-DD (no time)
# - Hides ID columns from grid & downloads
# - Downloads: Excel (.xlsx) and Word (.docx)
# - Grid hides Rsvd/Ord/Company on-screen, has Select checkboxes
# - Quote per Vendor (ZIP) or Single combined Word from selected rows
# - Quote table: Part Number | Part Name | Qty (Min-InStk) + 10 blank rows
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
import os, io, sqlite3, textwrap, re, zipfile
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Optional, Iterable, List
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.17"

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
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj

def resolve_db_path(cfg: dict) -> str:
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db:
        return yaml_db
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db:
        return env_db
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
    if "app_config" in st.secrets:
        return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:
        try:
            return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}")
            return {}
    cfg_file = HERE / "app_config.yaml"
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
    return [r[1] for r in rows]

# ---- Excel/Word export helpers ----
def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]
        ws.autofilter(0, 0, max(0, len(df)), max(0, len(df.columns) - 1))
        for i, col in enumerate(df.columns):
            if df.empty:
                width = 12
            else:
                lens = df[col].astype(str).str.len()
                q90 = lens.quantile(0.9) if not lens.empty else 10
                q90 = 10 if pd.isna(q90) else q90
                width = min(60, max(10, int(q90) + 2))
            ws.set_column(i, i, width)
    return buf.getvalue()

def to_docx_table_bytes(df: pd.DataFrame, title: str) -> bytes:
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

# ---------- Quote-specific helpers ----------
def sanitize_filename(name: str) -> str:
    name = str(name).strip() or "Unknown"
    return re.sub(r'[^A-Za-z0-9._ -]+', '_', name)[:80]

def pick_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def compute_qty_min_minus_stock(df: pd.DataFrame) -> pd.Series:
    """Return a numeric Series for Min - InStock (>=0), with robust column name detection."""
    min_candidates: List[str] = ["Min", "Minimum", "Min Qty", "Minimum Qty", "Reorder Point", "Min Level"]
    instock_candidates: List[str] = [
        "Quantity in Stock", "Available Quantity", "Qty in Stock", "QOH",
        "On Hand", "In Stock", "Available"
    ]
    min_col = pick_first_col(df, min_candidates)
    stk_col = pick_first_col(df, instock_candidates)

    qty = pd.Series([None] * len(df), index=df.index, dtype="float")
    if min_col and stk_col:
        m = pd.to_numeric(df[min_col], errors="coerce")
        s = pd.to_numeric(df[stk_col], errors="coerce")
        diff = (m - s).fillna(0)
        qty = diff.clip(lower=0)
    else:
        # graceful fallback to typical request qty columns if Min/InStock not present
        fallback = pick_first_col(df, [
            "Qty to Order", "Quantity to Order", "Order Qty",
            "Quantity Requested", "Qty Requested", "Qty Ordered", "Qty", "Quantity"
        ])
        if fallback:
            qty = pd.to_numeric(df[fallback], errors="coerce").clip(lower=0)
    return qty

def quote_docx_bytes(lines: pd.DataFrame, *, vendor: Optional[str], title_companies: str, dataset_label: str) -> bytes:
    """
    Build the requested Quote doc:
      Header: Vendor + "Quote Request — YYYY-MM-DD"
      Table: Part Number | Part Name | Qty (Min-InStk) + 10 blank rows
    """
    # detect source columns
    pn_col  = pick_first_col(lines, ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
    name_col= pick_first_col(lines, ["Name","Line Name","Description","Part Name","Item Name"])

    # Build visible frame
    out = pd.DataFrame(index=lines.index)
    out["Part Number"] = lines[pn_col].astype(str) if pn_col else ""
    out["Part Name"]   = lines[name_col].astype(str) if name_col else ""

    qty_series = compute_qty_min_minus_stock(lines)
    # format as int where possible, else plain string
    qty_fmt = qty_series.round(0).astype("Int64").astype(object)
    qty_fmt = qty_fmt.where(qty_fmt.notna(), "")
    out["Qty (Min-InStk)"] = qty_fmt

    # Append 10 blank rows
    blanks = pd.DataFrame([{"Part Number":"", "Part Name":"", "Qty (Min-InStk)":""} for _ in range(10)])
    out_final = pd.concat([out.reset_index(drop=True), blanks], ignore_index=True)

    # ---- build the docx
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    today = datetime.now().date().isoformat()  # date only
    # Header content
    vtxt = vendor if (vendor and str(vendor).strip()) else "Unknown"
    doc.add_paragraph(f"Vendor: {vtxt}")
    doc.add_heading(f"Quote Request — {today}", level=1)
    # Optional context line
    doc.add_paragraph(f"{dataset_label} — {title_companies}")

    # Table
    rows, cols = len(out_final) + 1, len(out_final.columns)
    tbl = doc.add_table(rows=rows, cols=cols)
    tbl.style = 'Table Grid'
    for j, c in enumerate(out_final.columns):
        tbl.cell(0, j).text = str(c)
    for i, (_, r) in enumerate(out_final.iterrows(), start=1):
        for j, c in enumerate(out_final.columns):
            v = '' if pd.isna(r[c]) else str(r[c])
            tbl.cell(i, j).text = v

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()

# ---- Date helpers ----
def strip_time(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            df[c] = s.dt.strftime("%Y-%m-%d").where(~s.isna(), df[c])
    return df

DATE_COLS = {
    "restock": [
        "Created On", "Approved On", "Completed On",
        "Part Updated on", "Posting Date",
        "Needed By", "Needed by", "Last updated", "Last Updated"
    ],
    "po_outstanding": [
        "Created On", "Approved On", "Completed On",
        "Part Updated on", "Posting Date"
    ],
}

HIDE_COLS = {
    "restock": ["ID", "id", "Purchase Order ID"],
    "po_outstanding": ["ID", "id", "Purchase Order ID", "Column2"],
}

# ---- "Data last updated" helper (GitHub commit time or local mtime) ----
def get_data_last_updated(cfg: dict, db_path: str) -> str | None:
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh and gh.get('repo') and gh.get('path'):
        try:
            import requests
            url = f"https://api.github.com/repos/{gh['repo']}/commits"
            params = {"path": gh["path"], "per_page": 1, "sha": gh.get("branch", "main")}
            headers = {"Accept": "application/vnd.github+json"}
            if gh.get("token"):
                headers["Authorization"] = f"token {gh['token']}"
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            iso = r.json()[0]["commit"]["committer"]["date"]
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.strftime("Data last updated: %Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    try:
        ts = Path(db_path).stat().st_mtime
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("Data last updated: %Y-%m-%d %H:%M UTC")
    except Exception:
        return None

# ---------- App ----------
cfg = load_config()
cfg = to_plain(cfg)

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

    # Sidebar caption: only the "last updated" info (no DB path, no version)
    updated_label = get_data_last_updated(cfg, db_path)
    if updated_label:
        st.sidebar.caption(updated_label)

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()

    # Dataset -> table name (radio)
    ds = st.sidebar.radio('Dataset', ['RE-STOCK', 'Outstanding POs'], index=0)
    src = 'restock' if ds == 'RE-STOCK' else 'po_outstanding'

    # --- Authorization by Company ---
    all_companies_df = q(
        f"SELECT DISTINCT [Company] FROM [{src}] WHERE [Company] IS NOT NULL ORDER BY 1",
        db_path=db_path
    )
    all_companies = [str(x) for x in all_companies_df['Company'].dropna().tolist()] or []

    username_ci = str(username).casefold()
    admin_users_ci = {str(u).casefold() for u in (cfg.get('access', {}).get('admin_usernames', []) or [])}
    is_admin = username_ci in admin_users_ci

    uc_raw = (cfg.get('access', {}).get('user_companies', {}) or {})
    uc_ci_map = {str(k).casefold(): v for k, v in uc_raw.items()}
    allowed_cfg = uc_ci_map.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    def norm(s: str) -> str:
        return " ".join(str(s).strip().split()).casefold()

    db_map = {norm(c): c for c in all_companies}
    allowed_norm = {norm(a) for a in allowed_cfg}
    star_granted = any(str(a).strip() == "*" for a in allowed_cfg)

    if is_admin or star_granted:
        allowed_set = set(all_companies)
    else:
        matches = {db_map[n] for n in allowed_norm if n in db_map}
        allowed_set = matches if matches else set(allowed_cfg)

    if not allowed_set:
        st.error("No companies configured for your account. Ask an admin to update your access.")
        with st.expander("Company values present in DB"):
            st.write(sorted(all_companies))
        st.stop()

    company_options = sorted(allowed_set)
    ADMIN_ALL = "« All companies (admin) »"

    select_options = ["— Choose company —"]
    if is_admin and len(all_companies) > 1:
        select_options += [ADMIN_ALL]
    select_options += company_options

    chosen = st.sidebar.selectbox("Choose your Company", options=select_options, index=0)

    if chosen == "— Choose company —":
        st.info("Select your Company on the left to load data.")
        st.stop()

    if is_admin and chosen == ADMIN_ALL:
        chosen_companies = sorted(all_companies)
        title_companies = "All companies (admin)"
    else:
        chosen_companies = [chosen]
        title_companies = chosen

    # --- Determine available columns now (for search + vendor detection) ---
    cols_in_db = table_columns_in_order(db_path, src)
    cols_lower = {c.lower(): c for c in cols_in_db}

    # Detect vendor column for search + grouping later
    vendor_col = None
    if src == 'restock':
        if 'vendors' in cols_lower:
            vendor_col = cols_lower['vendors']
        elif 'vendor' in cols_lower:
            vendor_col = cols_lower['vendor']
    else:
        if 'vendor' in cols_lower:
            vendor_col = cols_lower['vendor']

    # UI: search
    if ds == 'RE-STOCK':
        label = 'Search Part Numbers / Name' + (' / Vendor' if vendor_col else '') + ' contains'
        search = st.sidebar.text_input(label)
        if vendor_col:
            search_clause = f"([Part Numbers] LIKE ? OR [Name] LIKE ? OR [{vendor_col}] LIKE ?)"
            search_fields = 3
        else:
            search_clause = "([Part Numbers] LIKE ? OR [Name] LIKE ?)"
            search_fields = 2
        order_by = "[Company], [Name]"
    else:
        search = st.sidebar.text_input('Search PO # / Vendor / Part / Line Name contains')
        search_clause = "([Purchase Order #] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Line Name] LIKE ?)"
        search_fields = 4
        order_by = "[Company], date([Created On]) ASC, [Purchase Order #]"

    # Build WHERE
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

    # Date-only formatting
    df = strip_time(df, DATE_COLS.get(src, []))

    # Exports (downloads) frame: preserve on-disk order, hide IDs/technical columns
    cols_in_order = table_columns_in_order(db_path, src)
    hide_set = set(HIDE_COLS.get(src, []))
    cols_for_download = [c for c in cols_in_order if (c in df.columns) and (c not in hide_set)]
    df_download = df[cols_for_download]

    # ---------- DISPLAY GRID (with checkboxes; hide Rsvd/Ord/Company only on-screen) ----------
    st.markdown(f"### {ds} — {title_companies}")

    display_hide = {"Rsvd","Ord","Company"}
    display_cols = [c for c in cols_for_download if c not in display_hide]
    df_display = df[display_cols].copy()

    # Insert Select column at left (default False)
    if "Select" not in df_display.columns:
        df_display.insert(0, "Select", False)

    # Build column config: only Select is editable
    col_cfg = {"Select": st.column_config.CheckboxColumn(
        "Add to Quote", help="Check to include this line in a quote request", default=False
    )}
    for c in df_display.columns:
        if c != "Select":
            col_cfg[c] = st.column_config.Column(disabled=True)

    edited = st.data_editor(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config=col_cfg,
        key=f"grid_{src}"
    )

    # Recover selected rows using original index alignment
    try:
        selected_idx = edited.index[edited["Select"] == True]
    except Exception:
        selected_idx = []
    selected_rows = df.loc[selected_idx] if len(selected_idx) else df.iloc[0:0]

    # ---------- Standard downloads (whole result set) ----------
    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=to_xlsx_bytes(df_download, sheet=ds.replace(' ', '_')),
            file_name=f"{ds.replace(' ', '_')}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=to_docx_table_bytes(df_download, title=f"{ds} — {title_companies}"),
            file_name=f"{ds.replace(' ', '_')}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # ---------- Quote Request generation (selected rows) ----------
    st.markdown("#### Quote Request (from selected rows)")

    if selected_rows.empty:
        st.caption("Select rows using the checkboxes to enable quote downloads.")
    else:
        # Group by Vendor if present; else single group "Unknown"
        vcol = vendor_col if (vendor_col and vendor_col in selected_rows.columns) else None
        if vcol:
            vendor_groups = selected_rows.groupby(selected_rows[vcol].fillna("Unknown"))
            vendor_list = [(str(v), g.copy()) for v, g in vendor_groups]
        else:
            vendor_list = [("Unknown", selected_rows.copy())]

        # ZIP: one Word per vendor
        def build_zip_per_vendor() -> bytes:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for vname, gdf in vendor_list:
                    doc_bytes = quote_docx_bytes(
                        gdf, vendor=vname, title_companies=title_companies, dataset_label=ds
                    )
                    fname = f"Quote_{sanitize_filename(title_companies)}_{sanitize_filename(vname)}.docx"
                    zf.writestr(fname, doc_bytes)
            return buf.getvalue()

        # Single combined Word (marks vendor as "Multiple" if many)
        def build_single_doc() -> bytes:
            vend = vendor_list[0][0] if len(vendor_list) == 1 else "Multiple"
            return quote_docx_bytes(
                selected_rows, vendor=vend, title_companies=title_companies, dataset_label=ds
            )

        c3, c4, _ = st.columns([1.8, 1.8, 2.4])
        with c3:
            st.download_button(
                "🧾 Generate Quote per Vendor (ZIP)",
                data=build_zip_per_vendor(),
                file_name=f"Quotes_{ds.replace(' ','_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
            )
        with c4:
            if len(vendor_list) == 1:
                vname, gdf = vendor_list[0]
                st.download_button(
                    f"🧾 Single Word — {vname}",
                    data=quote_docx_bytes(gdf, vendor=vname, title_companies=title_companies, dataset_label=ds),
                    file_name=f"Quote_{sanitize_filename(title_companies)}_{sanitize_filename(vname)}.docx",
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                )
            else:
                st.download_button(
                    "🧾 Single Word — All Selected",
                    data=build_single_doc(),
                    file_name=f"Quote_All_Selected_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx",
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                )

    # ---------- Config template (admins only) ----------
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')






