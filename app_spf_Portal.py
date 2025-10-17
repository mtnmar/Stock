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
# - Shopping-cart flow: Add to Cart, edit Qty, Remove, Clear cart
# - Quote from CART (single Word; no ZIP)
# - Quote table: Part Number | Part Name | Qty + 10 blank rows
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
import os, io, sqlite3, textwrap, re, hashlib
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

# ---- Row key helpers (for persistent cart) ----
KEY_COL_CANDIDATES = ["ID", "id", "Purchase Order ID", "Row ID", "RowID"]

def attach_row_key(df: pd.DataFrame) -> pd.DataFrame:
    """Attach a stable __KEY__ column used to persist items in cart across filters."""
    df = df.copy()
    key_col = next((c for c in KEY_COL_CANDIDATES if c in df.columns), None)
    if key_col:
        df["__KEY__"] = df[key_col].astype(str)
        return df
    # fallback: hash important columns if present; else hash all columns as string
    columns_priority = [
        "Part Number","Part Numbers","Part #","Part No","PN",
        "Name","Line Name","Description",
        "Vendor","Vendors","Company",
        "Created On"
    ]
    cols = [c for c in columns_priority if c in df.columns]
    if not cols:
        cols = list(df.columns)
    s = df[cols].astype(str).agg("|".join, axis=1)
    df["__KEY__"] = s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df

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
    """Exact Min - InStock (no clamping, no fallback). Blank if columns missing."""
    min_candidates: List[str] = ["Min", "Minimum", "Min Qty", "Minimum Qty", "Reorder Point", "Min Level"]
    instock_candidates: List[str] = [
        "Quantity in Stock", "Available Quantity", "Qty in Stock", "QOH",
        "On Hand", "In Stock", "Available"
    ]
    min_col = pick_first_col(df, min_candidates)
    stk_col = pick_first_col(df, instock_candidates)

    if not (min_col and stk_col):
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    m = pd.to_numeric(df[min_col], errors="coerce")
    s = pd.to_numeric(df[stk_col], errors="coerce")
    diff = m - s
    out = diff.apply(lambda x: ("" if pd.isna(x) else (str(int(x)) if float(x).is_integer() else str(x))))
    return out.astype("object")

def qty_series_for_lines(lines: pd.DataFrame) -> pd.Series:
    """Prefer user-edited __QTY__; fallback to computed Min-InStk."""
    if "__QTY__" in lines.columns:
        q = lines["__QTY__"]
        # normalize to stringy display (integers no .0)
        def fmt(x):
            if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
                return None
            try:
                xf = float(x)
                return str(int(xf)) if xf.is_integer() else str(xf)
            except Exception:
                return str(x)
        q = q.apply(fmt)
    else:
        q = pd.Series([None] * len(lines), index=lines.index, dtype="object")

    # Where missing, compute default
    need = q.isna()
    if need.any():
        q_def = compute_qty_min_minus_stock(lines)
        q = q.where(~need, q_def)
    # Final: replace NaN/None with ""
    q = q.apply(lambda x: "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
    return q

def quote_docx_bytes(lines: pd.DataFrame, *, vendor: Optional[str], title_companies: str, dataset_label: str) -> bytes:
    """
    Build the requested Quote doc:
      Header: Vendor + "Quote Request — YYYY-MM-DD"
      Table: Part Number | Part Name | Qty + 10 blank rows
      Qty prefers user-edited values (__QTY__), fallback to Min-InStk
    """
    pn_col   = pick_first_col(lines, ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
    name_col = pick_first_col(lines, ["Name","Line Name","Description","Part Name","Item Name"])

    out = pd.DataFrame(index=lines.index)
    out["Part Number"] = lines[pn_col].astype(str) if pn_col else ""
    out["Part Name"]   = lines[name_col].astype(str) if name_col else ""
    out["Qty"]         = qty_series_for_lines(lines)

    # Append 10 blank rows
    blanks = pd.DataFrame([{"Part Number":"", "Part Name":"", "Qty":""} for _ in range(10)])
    out_final = pd.concat([out.reset_index(drop=True), blanks], ignore_index=True)

    # ---- build the docx
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    today = datetime.now().date().isoformat()
    vtxt = vendor if (vendor and str(vendor).strip()) else "Unknown"
    doc.add_paragraph(f"Vendor: {vtxt}")
    doc.add_heading(f"Quote Request — {today}", level=1)
    doc.add_paragraph(f"{dataset_label} — {title_companies}")

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
        # If none configured, stop here
        if not allowed_set:
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

    # --- Query data ---
    cols_in_db = table_columns_in_order(db_path, src)
    cols_lower = {c.lower(): c for c in cols_in_db}

    # Detect vendor column
    vendor_col = None
    if src == 'restock':
        if 'vendors' in cols_lower:
            vendor_col = cols_lower['vendors']
        elif 'vendor' in cols_lower:
            vendor_col = cols_lower['vendor']
    else:
        if 'vendor' in cols_lower:
            vendor_col = cols_lower['vendor']

    # Search
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

    # WHERE
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

    # Attach keys for cart
    df = attach_row_key(df)

    # Downloads frame (hide IDs + internals)
    cols_in_order = table_columns_in_order(db_path, src)
    hide_set = set(HIDE_COLS.get(src, [])) | {"__KEY__", "__QTY__"}
    cols_for_download = [c for c in cols_in_order if (c in df.columns) and (c not in hide_set)]
    df_download = df[cols_for_download]

    # ---------- GRID (checkboxes; hide Rsvd/Ord/Company; Add to Cart) ----------
    st.markdown(f"### {ds} — {title_companies}")

    display_hide = {"Rsvd","Ord","Company","__KEY__","__QTY__"}
    display_cols = [c for c in cols_for_download if c not in display_hide]
    df_display = df[display_cols].copy()

    if "Select" not in df_display.columns:
        df_display.insert(0, "Select", False)

    col_cfg = {"Select": st.column_config.CheckboxColumn(
        "Add to Cart", help="Check to include this line in the cart", default=False
    )}
    for c in df_display.columns:
        if c != "Select":
            col_cfg[c] = st.column_config.Column(disabled=True)

    base_key = f"grid_{src}"
    ver_key = f"{base_key}_ver"
    if ver_key not in st.session_state:
        st.session_state[ver_key] = 0
    grid_key = f"{base_key}_{st.session_state[ver_key]}"

    edited = st.data_editor(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config=col_cfg,
        key=grid_key,
    )

    # Selected rows in current grid view
    try:
        selected_idx = edited.index[edited["Select"] == True]
    except Exception:
        selected_idx = []
    selected_rows = df.loc[selected_idx] if len(selected_idx) else df.iloc[0:0]

    # ---- Cart state (per dataset+company) ----
    cart_key = f"cart_{src}_{hashlib.md5(('|'.join(chosen_companies)).encode()).hexdigest()}"
    if cart_key not in st.session_state:
        st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])

    # Controls: add to cart / clear selections
    c_add, c_clear_sel, _ = st.columns([1.6, 1.3, 6])
    with c_add:
        if st.button("🛒 Add selected to cart", disabled=selected_rows.empty):
            if not selected_rows.empty:
                # ensure __QTY__ defaults for new additions
                add_df = selected_rows.copy()
                if "__QTY__" not in add_df.columns:
                    add_df["__QTY__"] = compute_qty_min_minus_stock(add_df)
                else:
                    # fill blanks with default
                    mask_blank = add_df["__QTY__"].isna() | (add_df["__QTY__"].astype(str).str.strip() == "")
                    add_df.loc[mask_blank, "__QTY__"] = compute_qty_min_minus_stock(add_df[mask_blank])
                merged = pd.concat([st.session_state[cart_key], add_df], ignore_index=True)
                st.session_state[cart_key] = merged.drop_duplicates(subset="__KEY__", keep="first").reset_index(drop=True)
                st.session_state[ver_key] += 1
                st.rerun()
    with c_clear_sel:
        if st.button("🧹 Clear selections"):
            st.session_state[ver_key] += 1
            st.rerun()

    # ---------- Standard downloads (whole result set) ----------
    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        st.download_button(
            label="⬇️ Excel (.xlsx)",
            data=to_xlsx_bytes(df_download, sheet=ds.replace(" ", "_")),
            file_name=f"{ds.replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c2:
        st.download_button(
            label="⬇️ Word (.docx)",
            data=to_docx_table_bytes(df_download, title=f"{ds} — {title_companies}"),
            file_name=f"{ds.replace(' ', '_')}.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    # ---------- Cart ----------
    cart_df: pd.DataFrame = st.session_state[cart_key]
    st.markdown(f"#### Cart ({len(cart_df)} item{'s' if len(cart_df)!=1 else ''})")

    # Add keys if missing (safety) and ensure __QTY__ exists
    if cart_df.empty:
        cart_df = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])
        st.session_state[cart_key] = cart_df
    else:
        if "__KEY__" not in cart_df.columns:
            cart_df = attach_row_key(cart_df)
        if "__QTY__" not in cart_df.columns:
            cart_df["__QTY__"] = compute_qty_min_minus_stock(cart_df)
        st.session_state[cart_key] = cart_df

    # Build a clean cart display: Remove | Part Number | Part Name | Vendor | Qty (editable)
    pn = pick_first_col(cart_df, ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
    nm = pick_first_col(cart_df, ["Name","Line Name","Description","Part Name","Item Name"])
    vd = pick_first_col(cart_df, ["Vendor","Vendors"])

    cart_display = pd.DataFrame(index=cart_df.index)
    if "Remove" not in cart_display.columns:
        cart_display["Remove"] = False
    if pn: cart_display["Part Number"] = cart_df[pn]
    if nm: cart_display["Part Name"] = cart_df[nm]
    if vd: cart_display["Vendor"] = cart_df[vd]

    # Qty column shown to user; pull from __QTY__ if present, else compute
    if "__QTY__" in cart_df.columns:
        show_qty = cart_df["__QTY__"]
    else:
        show_qty = compute_qty_min_minus_stock(cart_df)
    # Try to present as numeric where possible
    def to_float_or_str(x):
        try:
            return float(x)
        except Exception:
            return None if (x is None or (isinstance(x, str) and x.strip()=="")) else x
    cart_display["Qty"] = show_qty.apply(to_float_or_str)

    cart_col_cfg = {
        "Remove": st.column_config.CheckboxColumn("Remove", help="Check to remove from cart", default=False),
        "Qty": st.column_config.NumberColumn("Qty", help="Edit requested quantity", step=1)
    }

    cart_base = f"cart_{src}_editor"
    cart_ver_key = f"{cart_base}_ver"
    if cart_ver_key not in st.session_state:
        st.session_state[cart_ver_key] = 0
    cart_editor_key = f"{cart_base}_{st.session_state[cart_ver_key]}"

    edited_cart = st.data_editor(
        cart_display,
        use_container_width=True,
        hide_index=True,
        column_config=cart_col_cfg,
        key=cart_editor_key,
    )

    # Persist Qty edits back to __QTY__ immediately
    if "Qty" in edited_cart.columns:
        new_qty = edited_cart["Qty"]
        # convert floats like 3.0 -> "3", leave strings as-is
        def norm_q(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            if isinstance(v, (int, float)):
                return str(int(v)) if float(v).is_integer() else str(v)
            return str(v)
        st.session_state[cart_key].loc[new_qty.index, "__QTY__"] = new_qty.apply(norm_q).values

    c_rm, c_clear_cart, _ = st.columns([1.2, 1.1, 6])
    with c_rm:
        if st.button("🗑️ Remove selected"):
            try:
                to_remove_idx = edited_cart.index[edited_cart["Remove"] == True]
            except Exception:
                to_remove_idx = []
            if len(to_remove_idx):
                keys_to_remove = st.session_state[cart_key].loc[to_remove_idx, "__KEY__"].tolist()
                st.session_state[cart_key] = st.session_state[cart_key].loc[
                    ~st.session_state[cart_key]["__KEY__"].isin(keys_to_remove)
                ].reset_index(drop=True)
                st.session_state[cart_ver_key] += 1
                st.rerun()
    with c_clear_cart:
        if st.button("🧼 Clear cart", disabled=st.session_state[cart_key].empty):
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])
            st.session_state[cart_ver_key] += 1
            st.rerun()

    # ---------- Quote Request (from CART) ----------
    st.markdown("#### Quote Request (from cart)")
    if st.session_state[cart_key].empty:
        st.caption("Add items to the cart above to enable the quote download.")
    else:
        if vendor_col and vendor_col in st.session_state[cart_key].columns:
            vendors = sorted(set(st.session_state[cart_key][vendor_col].dropna().astype(str)))
            v_header = vendors[0] if len(vendors) == 1 else "Multiple"
        else:
            v_header = "Unknown"

        def build_single_doc() -> bytes:
            return quote_docx_bytes(
                st.session_state[cart_key],
                vendor=v_header,
                title_companies=title_companies,
                dataset_label=ds
            )

        st.download_button(
            "🧾 Generate Quote (Word)",
            data=build_single_doc(),
            file_name=f"Quote_{sanitize_filename(title_companies)}_{datetime.now().strftime('%Y%m%d')}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # ---------- Config template (admins only) ----------
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')









