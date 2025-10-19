# app_spf_portal.py
# --------------------------------------------------------------
# SPF portal for RE-STOCK, Outstanding POs, and Quotes
# - Login (streamlit-authenticator)
# - Authorize & filter by Company (not Location)
# - Uses raw tables: restock, po_outstanding (+ optional: addresses, user_contacts)
# - NEW: quotes table (created on the fly in the same DB) for saving/editing requests
# - Preserves exact column order (DB/Parquet) in grid & downloads
# - Dates shown as YYYY-MM-DD (no time)
# - Hides ID columns from grid & downloads
# - Downloads: Excel (.xlsx) and Word (.docx)
# - RE-STOCK: Shopping cart (forms), single-vendor, editable Qty
# - Outstanding POs: NO cart/quote UI
# - Quote table: Part Number | Part Name | Qty + 10 blank rows
# - Qty default = max(Min − InStk, 0); users can edit in cart
# - Buttons row (cart): Remove (left) | Clear • Save • Generate (right)
# - Quote Page: new / browse+edit / download (email placeholder)
# - Auto Parquet engine if files exist; else SQLite
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
#   pyarrow>=17.0   # or fastparquet>=2024.5.0

from __future__ import annotations
import os, io, re, json, sqlite3, textwrap, hashlib, uuid
from pathlib import Path
from collections.abc import Mapping, Iterable
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.19"

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
DEFAULT_DB = "maintainx_po.db"   # local fallback; Cloud may use secrets→GitHub
HERE = Path(__file__).resolve().parent

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
  # Optional direct Parquet paths:
  # parquet:
  #   restock: /path/to/restock.parquet
  #   po_outstanding: /path/to/po_outstanding.parquet
"""

# ---------- helpers ----------
def to_plain(obj):
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj

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

# ---------- Engine detection (Parquet vs SQLite) ----------
def detect_parquet_paths(cfg: dict) -> Dict[str, Optional[Path]]:
    p_cfg = (cfg.get('settings', {}) or {}).get('parquet', {}) or {}
    def as_path(x):
        try:
            return Path(str(x)).expanduser().resolve()
        except Exception:
            return None
    restock = as_path(p_cfg.get('restock')) if p_cfg else None
    po_out  = as_path(p_cfg.get('po_outstanding')) if p_cfg else None
    if not restock:
        env = os.environ.get('SPF_RESTOCK_PARQUET'); restock = as_path(env) if env else None
    if not po_out:
        env = os.environ.get('SPF_PO_PARQUET'); po_out = as_path(env) if env else None
    base = os.environ.get('SPF_PARQUET_DIR')
    if base and not restock: restock = as_path(Path(base) / "restock.parquet")
    if base and not po_out:  po_out  = as_path(Path(base) / "po_outstanding.parquet")
    if not restock:
        cand = HERE / "restock.parquet"; restock = cand if cand.exists() else None
    if not po_out:
        cand = HERE / "po_outstanding.parquet"; po_out = cand if cand.exists() else None
    if restock and not restock.exists(): restock = None
    if po_out  and not po_out.exists():  po_out  = None
    return {"restock": restock, "po_outstanding": po_out}

def _filesig(p: Path) -> int:
    try:
        stt = p.stat()
        return (int(stt.st_mtime_ns) ^ (stt.st_size << 13)) & 0xFFFFFFFFFFFF
    except Exception:
        return 0

@st.cache_data(show_spinner=False)
def read_parquet_cached(path_str: str, sig: int) -> pd.DataFrame:
    return pd.read_parquet(path_str)

def parquet_available_for(src: str, pq_paths: Dict[str, Optional[Path]]) -> Optional[Path]:
    p = pq_paths.get(src)
    return p if p and p.exists() else None

# ---------- SQLite helpers with caching ----------
def _db_sig(db_path: str) -> int:
    try:
        return Path(db_path).stat().st_mtime_ns
    except Exception:
        return 0

@st.cache_data(show_spinner=False)
def q_cached(sql: str, params: Tuple, db_path: str, db_sig: int) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    return q_cached(sql, tuple(params), path, _db_sig(path))

@st.cache_data(show_spinner=False)
def table_columns_in_order_cached(db_path: str, table: str, db_sig: int) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def table_columns_in_order(db_path: str, table: str) -> list[str]:
    return table_columns_in_order_cached(db_path, table, _db_sig(db_path))

# ---- Row key helper (shared) ----
KEY_COL_CANDIDATES = ["ID", "id", "Purchase Order ID", "Row ID", "RowID"]
def attach_row_key(df_in: pd.DataFrame) -> pd.DataFrame:
    df_in = df_in.copy()
    key_col = next((c for c in KEY_COL_CANDIDATES if c in df_in.columns), None)
    if key_col:
        df_in["__KEY__"] = df_in[key_col].astype(str)
        return df_in
    cols = [c for c in ["Part Number","Part Numbers","Part #","Part No","PN",
                        "Name","Line Name","Description",
                        "Vendor","Vendors","Company","Created On"] if c in df_in.columns]
    if not cols: cols = list(df_in.columns)
    s = df_in[cols].astype(str).agg("|".join, axis=1)
    df_in["__KEY__"] = s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df_in

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
    min_candidates: List[str] = ["Min", "Minimum", "Min Qty", "Minimum Qty", "Reorder Point", "Min Level"]
    instock_candidates: List[str] = ["InStk", "Instk", "In Stock", "On Hand", "QOH","Quantity in Stock",
                                     "Available Quantity","Qty in Stock","Available","Stock",
                                     "Qty On Hand","On-Hand","OnHand","In_Stock"]

    def pick_first(df_cols: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
        for c in candidates:
            if c in df_cols:
                return c
        return None

    min_col = pick_first(df.columns, min_candidates)
    stk_col = pick_first(df.columns, instock_candidates)
    if not (min_col and stk_col):
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    m = pd.to_numeric(df[min_col], errors="coerce")
    s = pd.to_numeric(df[stk_col], errors="coerce")
    diff = (m - s).clip(lower=0)

    def fmt(x):
        if pd.isna(x): return ""
        xf = float(x); return str(int(xf)) if xf.is_integer() else str(xf)
    return diff.apply(fmt).astype("object")

def qty_series_for_lines(lines: pd.DataFrame) -> pd.Series:
    if "__QTY__" in lines.columns:
        q = lines["__QTY__"].copy()
    else:
        q = pd.Series([None] * len(lines), index=lines.index, dtype="object")
    need = q.isna() | (q.astype(str).str.strip() == "")
    if need.any():
        q_def = compute_qty_min_minus_stock(lines)
        q = q.where(~need, q_def)
    def to_str(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): return ""
        return str(x)
    return q.apply(to_str).astype("object")

def quote_docx_bytes(lines: pd.DataFrame, *, vendor: Optional[str], title_companies: str, dataset_label: str) -> bytes:
    pn_col   = pick_first_col(lines, ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
    name_col = pick_first_col(lines, ["Name","Line Name","Description","Part Name","Item Name"])
    out = pd.DataFrame(index=lines.index)
    out["Part Number"] = lines[pn_col].astype(str) if pn_col else ""
    out["Part Name"]   = lines[name_col].astype(str) if name_col else ""
    out["Qty"]         = qty_series_for_lines(lines)
    blanks = pd.DataFrame([{"Part Number":"", "Part Name":"", "Qty":""} for _ in range(10)])
    out_final = pd.concat([out.reset_index(drop=True), blanks], ignore_index=True)

    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)
    today = datetime.now().date().isoformat()
    vtxt = vendor if (vendor and str(vendor).strip()) else "_____________________________"
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
    buf = io.BytesIO(); doc.save(buf); return buf.getvalue()

# ---- Date helpers ----
def strip_time(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            df[c] = s.dt.strftime("%Y-%m-%d").where(~s.isna(), df[c])
    return df

DATE_COLS = {
    "restock": ["Created On","Approved On","Completed On","Part Updated on","Posting Date",
                "Needed By","Needed by","Last updated","Last Updated"],
    "po_outstanding": ["Created On","Approved On","Completed On","Part Updated on","Posting Date"],
}

HIDE_COLS = {
    "restock": ["ID", "id", "Purchase Order ID"],
    "po_outstanding": ["ID", "id", "Purchase Order ID", "Column2"],
}

# ---- Source label ----
def label_for_source(engine: str, path: Optional[str]) -> str:
    if engine == "parquet" and path:
        try:
            ts = Path(path).stat().st_mtime
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return f"Engine: Parquet • Updated: {dt.strftime('%Y-%m-%d %H:%M UTC')}"
        except Exception:
            return "Engine: Parquet"
    return "Engine: SQLite"

# ---- Quotes storage (SQLite) ----
def ensure_quotes_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              company TEXT,
              created_by TEXT,
              vendor TEXT,
              ship_to TEXT,
              quote_date TEXT,
              status TEXT,
              source TEXT,
              lines_json TEXT,
              updated_at TEXT
            )
        """)
        conn.commit()

def save_quote(db_path: str, *, company: str, created_by: str, vendor: str, ship_to: str,
               source: str, lines_df: pd.DataFrame, status: str = "draft",
               quote_id: Optional[int] = None) -> int:
    ensure_quotes_table(db_path)
    lines = lines_df.fillna("").astype(str)
    keep = ["Part Number","Part Name","Qty"]
    for col in keep:
        if col not in lines.columns:
            lines[col] = ""
    lines = lines[keep]
    # drop trailing completely empty rows
    mask = (lines[keep].astype(str).apply(lambda r: "".join(r), axis=1).str.strip() != "")
    lines = lines[mask]

    payload = json.dumps(lines.to_dict(orient="records"), ensure_ascii=False)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(db_path) as conn:
        if quote_id is None:
            conn.execute("""
                INSERT INTO quotes(company, created_by, vendor, ship_to, quote_date, status, source, lines_json, updated_at)
                VALUES (?, ?, ?, ?, date('now'), ?, ?, ?, ?)
            """, (company, created_by, vendor, ship_to, status, source, payload, now))
            rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return int(rid)
        else:
            conn.execute("""
                UPDATE quotes
                   SET company=?, created_by=?, vendor=?, ship_to=?, status=?, source=?, lines_json=?, updated_at=?, quote_date=quote_date
                 WHERE id=?
            """, (company, created_by, vendor, ship_to, status, source, payload, now, quote_id))
            conn.commit()
            return int(quote_id)

def load_quote(db_path: str, quote_id: int) -> Optional[dict]:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT * FROM quotes WHERE id=?", (quote_id,)).fetchone()
    if not row: return None
    cols = ["id","company","created_by","vendor","ship_to","quote_date","status","source","lines_json","updated_at"]
    rec = dict(zip(cols, row))
    try:
        rec["lines"] = pd.DataFrame(json.loads(rec["lines_json"]))
    except Exception:
        rec["lines"] = pd.DataFrame(columns=["Part Number","Part Name","Qty"])
    return rec

def list_quotes(db_path: str, company: Optional[str]=None) -> pd.DataFrame:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        if company:
            df = pd.read_sql_query(
                "SELECT id, quote_date, vendor, status, source, length(lines_json) as bytes FROM quotes WHERE company=? ORDER BY id DESC",
                conn, params=(company,))
        else:
            df = pd.read_sql_query(
                "SELECT id, quote_date, vendor, status, source, length(lines_json) as bytes FROM quotes ORDER BY id DESC",
                conn)
    return df

# ---------- App ----------
cfg = load_config()
cfg = to_plain(cfg)

# Auth
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
    pq_paths = detect_parquet_paths(cfg)
    st.sidebar.caption(label_for_source(
        "parquet" if (pq_paths.get("restock") or pq_paths.get("po_outstanding")) else "sqlite",
        str(pq_paths.get("restock") or pq_paths.get("po_outstanding")) if (pq_paths.get("restock") or pq_paths.get("po_outstanding")) else None
    ))
    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()

    # Page selector (now 3 pages)
    page = st.sidebar.radio("Page", ["RE-STOCK", "Outstanding POs", "Quotes"], index=0)

    # --- preload some shared info for auth scoping ---
    def load_src(src: str):
        pq_path = parquet_available_for(src, pq_paths)
        if pq_path:
            df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
            cols_in_db = list(df_all.columns)
            comp_col = "Company" if "Company" in df_all.columns else None
            all_companies = sorted({str(x) for x in df_all[comp_col].dropna().tolist()}) if comp_col else []
            return df_all, cols_in_db, all_companies, pq_path
        else:
            all_companies_df = q(
                f"SELECT DISTINCT [Company] FROM [{src}] WHERE [Company] IS NOT NULL ORDER BY 1",
            )
            all_companies = [str(x) for x in all_companies_df['Company'].dropna().tolist()] or []
            cols_in_db = table_columns_in_order(db_path, src)
            return None, cols_in_db, all_companies, None

    # We’ll base company scoping off RE-STOCK table (it always exists)
    _, cols_any, all_companies, _ = load_src("restock")

    username_ci = str(username).casefold()
    admin_users_ci = {str(u).casefold() for u in (cfg.get('access', {}).get('admin_usernames', []) or [])}
    is_admin = username_ci in admin_users_ci
    uc_raw = (cfg.get('access', {}).get('user_companies', {}) or {})
    uc_ci_map = {str(k).casefold(): v for k, v in uc_raw.items()}
    allowed_cfg = uc_ci_map.get(username_ci, [])
    if isinstance(allowed_cfg, str): allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    def norm(s: str) -> str: return " ".join(str(s).strip().split()).casefold()
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
        with st.expander("Company values present in data"):
            st.write(sorted(all_companies))
        st.stop()

    company_options = sorted(allowed_set)
    ADMIN_ALL = "« All companies (admin) »"
    select_options = ["— Choose company —"]
    if is_admin and len(all_companies) > 1: select_options += [ADMIN_ALL]
    select_options += company_options
    chosen = st.sidebar.selectbox("Choose your Company", options=select_options, index=0)

    if chosen == "— Choose company —":
        st.info("Select your Company on the left to load data.")
        st.stop()
    if is_admin and chosen == ADMIN_ALL:
        chosen_companies = sorted(all_companies); title_companies = "All companies (admin)"
    else:
        chosen_companies = [chosen]; title_companies = chosen

    # ========== PAGE: RE-STOCK ==========
    if page == "RE-STOCK":
        src = "restock"
        df_all, cols_in_db, _, pq_path = load_src(src)

        # Determine vendor column for search/grouping
        cols_lower = {c.lower(): c for c in cols_in_db}
        vendor_col = cols_lower.get("vendors", cols_lower.get("vendor"))

        # Search + sort
        label = 'Search Part Numbers / Name' + (' / Vendor' if vendor_col else '') + ' contains'
        search = st.sidebar.text_input(label)

        # Load data
        if pq_path:
            df = df_all.copy()
            if "Company" in df.columns:
                df = df[df["Company"].astype(str).isin([str(x) for x in chosen_companies])]
            if search:
                s = str(search)
                cols = ["Part Numbers","Name"] + ([vendor_col] if vendor_col else [])
                ok = pd.Series(False, index=df.index)
                for c in cols:
                    if c in df.columns:
                        ok = ok | df[c].astype(str).str.contains(s, case=False, regex=False, na=False)
                df = df[ok]
            order_cols = [c for c in ["Company","Name"] if c in df.columns]
            df = df.sort_values(order_cols) if order_cols else df
        else:
            ph = ','.join(['?'] * len(chosen_companies))
            where = [f"[Company] IN ({ph})"]; params: list = list(chosen_companies)
            if search:
                if vendor_col:
                    search_clause = f"([Part Numbers] LIKE ? OR [Name] LIKE ? OR [{vendor_col}] LIKE ?)"
                    params += [f"%{search}%"] * 3
                else:
                    search_clause = "([Part Numbers] LIKE ? OR [Name] LIKE ?)"
                    params += [f"%{search}%"] * 2
                where.append(search_clause)
            where_sql = " AND ".join(where)
            order_by = "[Company], [Name]"
            sql = f"SELECT * FROM [{src}] WHERE {where_sql} ORDER BY {order_by}"
            df = q(sql, tuple(params))

        df = strip_time(df, DATE_COLS.get(src, []))
        df = attach_row_key(df)

        hide_set = set(HIDE_COLS.get(src, [])) | {"__KEY__", "__QTY__"}
        cols_for_download = [c for c in cols_in_db if (c in df.columns) and (c not in hide_set)]

        st.markdown(f"### RE-STOCK — {title_companies}")

        display_hide = {"Rsvd","Ord","Company","__KEY__","__QTY__"}
        display_cols = [c for c in cols_for_download if c not in display_hide]
        df_display = df[display_cols].copy()
        if "Select" not in df_display.columns:
            df_display.insert(0, "Select", False)

        grid_col_cfg = {"Select": st.column_config.CheckboxColumn("Add", help="Check to include in cart", default=False)}
        for c in df_display.columns:
            if c != "Select": grid_col_cfg[c] = st.column_config.Column(disabled=True)

        base_key = f"grid_{src}"
        if base_key not in st.session_state: st.session_state[base_key] = 0
        grid_key = f"{base_key}_{st.session_state[base_key]}"

        # Data editor in a form (prevents re-run on every keystroke)
        with st.form(f"{grid_key}_form", clear_on_submit=False):
            edited = st.data_editor(
                df_display,
                use_container_width=True,
                hide_index=True,
                column_config=grid_col_cfg,
                key=grid_key,
            )
            c_add, c_clear_sel = st.columns([1.6, 1.3])
            add_clicked = c_add.form_submit_button("🛒 Add selected to cart", use_container_width=True)
            clear_sel_clicked = c_clear_sel.form_submit_button("🧹 Clear selections", use_container_width=True)

        try:
            selected_idx = edited.index[edited["Select"] == True]
        except Exception:
            selected_idx = []
        selected_rows = df.loc[selected_idx] if len(selected_idx) else df.iloc[0:0]

        # Cart state
        cart_key = f"cart_{src}_{hashlib.md5(('|'.join([str(x) for x in chosen_companies])).encode()).hexdigest()}"
        if cart_key not in st.session_state:
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])

        if add_clicked and not selected_rows.empty:
            add_df = selected_rows.copy()
            add_df["__QTY__"] = compute_qty_min_minus_stock(add_df)
            if vendor_col:
                sel_vendors = sorted(set(add_df[vendor_col].dropna().astype(str).str.strip()))
                cart_df = st.session_state[cart_key]
                cart_vendors = sorted(set(cart_df[vendor_col].dropna().astype(str).str.strip())) if (not cart_df.empty and vendor_col in cart_df.columns) else []
                if not cart_vendors:
                    if len(sel_vendors) > 1:
                        chosen_vendor = sel_vendors[0]
                        add_df = add_df[add_df[vendor_col].astype(str).str.strip() == chosen_vendor]
                        st.info(f"Cart is per-vendor. Added only Vendor '{chosen_vendor}' from selection.")
                else:
                    chosen_vendor = cart_vendors[0]
                    before = len(add_df)
                    add_df = add_df[add_df[vendor_col].astype(str).str.strip() == chosen_vendor]
                    skipped = before - len(add_df)
                    if skipped > 0:
                        st.warning(f"Cart locked to Vendor '{chosen_vendor}'. Skipped {skipped} item(s) from other vendor(s).")
            merged = pd.concat([st.session_state[cart_key], add_df], ignore_index=True)
            st.session_state[cart_key] = merged.drop_duplicates(subset="__KEY__", keep="first").reset_index(drop=True)
            st.success(f"Added {len(add_df)} item(s) to cart.")
            st.rerun()

        if clear_sel_clicked:
            st.session_state[base_key] += 1
            st.rerun()

        # Downloads of current view (Excel matches visible columns; adds blank Inventory Check)
        excel_df = df_display.drop(columns=["Select"], errors="ignore").copy()
        excel_df.insert(0, "Inventory Check", "")
        df_download = df[cols_for_download]
        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button(
                label="⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(excel_df, sheet="RE_STOCK"),
                file_name="RE_STOCK.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                label="⬇️ Word (.docx)",
                data=to_docx_table_bytes(df_download, title=f"RE-STOCK — {title_companies}"),
                file_name="RE_STOCK.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

        # ----- CART -----
        cart_df: pd.DataFrame = st.session_state[cart_key]
        st.markdown(f"#### Cart ({len(cart_df)} item{'s' if len(cart_df)!=1 else ''})")
        if cart_df.empty:
            cart_df = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])
            st.session_state[cart_key] = cart_df
        else:
            if "__KEY__" not in cart_df.columns: cart_df = attach_row_key(cart_df)
            if "__QTY__" not in cart_df.columns: cart_df["__QTY__"] = compute_qty_min_minus_stock(cart_df)
            st.session_state[cart_key] = cart_df

        pn = pick_first_col(cart_df, ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
        nm = pick_first_col(cart_df, ["Name","Line Name","Description","Part Name","Item Name"])
        vd = vendor_col

        cart_display = pd.DataFrame(index=cart_df.index)
        cart_display["Remove"] = False
        if pn: cart_display["Part Number"] = cart_df[pn]
        if nm: cart_display["Part Name"]   = cart_df[nm]
        if vd: cart_display["Vendor"]      = cart_df[vd]

        def to_num(x):
            try:
                xf = float(x); return int(xf) if xf.is_integer() else xf
            except Exception:
                return None if (x is None or (isinstance(x, str) and x.strip()=="")) else x
        cart_display["Qty"] = cart_df["__QTY__"].apply(to_num)

        cart_col_cfg = {
            "Remove": st.column_config.CheckboxColumn("Remove", help="Check to remove from cart", default=False),
            "Qty": st.column_config.NumberColumn("Qty", help="Edit requested quantity", step=1, min_value=0),
        }

        cart_base = f"cart_{src}_editor"
        if cart_base not in st.session_state: st.session_state[cart_base] = 0
        cart_editor_key = f"{cart_base}_{st.session_state[cart_base]}"

        # Editor + (Remove, Clear, Save) are inside ONE form
        with st.form(f"{cart_editor_key}_form", clear_on_submit=False):
            edited_cart = st.data_editor(
                cart_display,
                use_container_width=True,
                hide_index=True,
                column_config=cart_col_cfg,
                key=cart_editor_key,
            )
            # Buttons row layout: Remove (left) | Clear • Save (right)
            c_remove, c_space, c_clear, c_save = st.columns([6, 1, 1, 1])
            remove_btn = c_remove.form_submit_button("🗑️ Remove", use_container_width=True)
            clear_cart_btn = c_clear.form_submit_button("🧼 Clear", use_container_width=True, disabled=cart_df.empty)
            save_qty = c_save.form_submit_button("💾 Save", use_container_width=True)

        # Generate button on the same visual row (to the right)
        c_spacer, c_gen = st.columns([10, 1])
        can_download = not st.session_state[cart_key].empty
        v_header = "Unknown"
        if can_download and vendor_col and vendor_col in st.session_state[cart_key].columns:
            vendors = sorted(set(st.session_state[cart_key][vendor_col].dropna().astype(str).str.strip()))
            if len(vendors) == 1:
                v_header = vendors[0]
            else:
                can_download = False
                st.caption("Cart has multiple vendors. Keep one vendor before generating.")

        with c_gen:
            if can_download:
                st.download_button(
                    "🧾 Generate",
                    data=quote_docx_bytes(st.session_state[cart_key], vendor=v_header,
                                          title_companies=title_companies, dataset_label="RE-STOCK"),
                    file_name=f"Quote_{sanitize_filename(title_companies)}_{datetime.now().strftime('%Y%m%d')}.docx",
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    use_container_width=True
                )

        # Apply cart actions
        if save_qty and "Qty" in edited_cart.columns:
            def norm_q(v):
                if v is None: return ""
                if isinstance(v, (int, float)):
                    return str(int(v)) if float(v).is_integer() else str(v)
                return str(v)
            st.session_state[cart_key].loc[edited_cart.index, "__QTY__"] = edited_cart["Qty"].apply(norm_q).values
            st.success("Saved quantities.")

        if remove_btn:
            try:
                to_remove_idx = edited_cart.index[edited_cart["Remove"] == True]
            except Exception:
                to_remove_idx = []
            if len(to_remove_idx):
                keys_to_remove = st.session_state[cart_key].loc[to_remove_idx, "__KEY__"].tolist()
                st.session_state[cart_key] = st.session_state[cart_key].loc[
                    ~st.session_state[cart_key]["__KEY__"].isin(keys_to_remove)
                ].reset_index(drop=True)
                st.session_state[cart_base] += 1
                st.rerun()

        if clear_cart_btn:
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns) + ["__QTY__"])
            st.session_state[cart_base] += 1
            st.rerun()

        # Save to Quotes DB (from cart) — optional convenience
        if not st.session_state[cart_key].empty:
            with st.expander("Save this cart as a Quote (optional)"):
                vendor_text = v_header if v_header != "Unknown" else ""
                ship_to = ""  # can be auto-filled later from addresses
                c1, c2 = st.columns([2,3])
                with c1:
                    vendor_text = st.text_input("Vendor", value=vendor_text)
                with c2:
                    ship_to = st.text_area("Ship To (optional)", value=ship_to, height=80)
                if st.button("Save as Quote"):
                    # create simple 3-col df (Part Number/Name/Qty) for storage
                    pncol = pick_first_col(st.session_state[cart_key], ["Part Number","Part Numbers","Part #","Part","Part No","PN"])
                    nmcol = pick_first_col(st.session_state[cart_key], ["Name","Line Name","Description","Part Name","Item Name"])
                    store = pd.DataFrame({
                        "Part Number": st.session_state[cart_key][pncol].astype(str) if pncol else "",
                        "Part Name":   st.session_state[cart_key][nmcol].astype(str) if nmcol else "",
                        "Qty":         qty_series_for_lines(st.session_state[cart_key])
                    })
                    qid = save_quote(db_path, company=title_companies, created_by=str(username),
                                     vendor=vendor_text, ship_to=ship_to, source="restock", lines_df=store)
                    st.success(f"Saved as quote #{qid}")

    # ========== PAGE: OUTSTANDING POs ==========
    elif page == "Outstanding POs":
        src = "po_outstanding"
        df_all, cols_in_db, _, pq_path = load_src(src)
        search = st.sidebar.text_input('Search PO # / Vendor / Part / Line Name contains')
        if pq_path:
            df = df_all.copy()
            if "Company" in df.columns:
                df = df[df["Company"].astype(str).isin([str(x) for x in chosen_companies])]
            if search:
                s = str(search)
                cols = ["Purchase Order #","Vendor","Part Number","Line Name"]
                ok = pd.Series(False, index=df.index)
                for c in cols:
                    if c in df.columns:
                        ok = ok | df[c].astype(str).str.contains(s, case=False, regex=False, na=False)
                df = df[ok]
            if "Created On" in df.columns:
                co = pd.to_datetime(df["Created On"], errors="coerce")
                if "Company" in df.columns and "Purchase Order #" in df.columns:
                    df = df.assign(_co=co).sort_values(["Company","_co","Purchase Order #"]).drop(columns=["_co"])
                else:
                    df = df.assign(_co=co).sort_values(["_co"]).drop(columns=["_co"])
        else:
            ph = ','.join(['?'] * len(chosen_companies))
            where = [f"[Company] IN ({ph})"]; params: list = list(chosen_companies)
            if search:
                where.append("([Purchase Order #] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Line Name] LIKE ?)")
                params += [f"%{search}%"] * 4
            where_sql = " AND ".join(where)
            sql = f"""SELECT * FROM [{src}] WHERE {where_sql}
                      ORDER BY [Company], date([Created On]) ASC, [Purchase Order #]"""
            df = q(sql, tuple(params))
        df = strip_time(df, DATE_COLS.get(src, []))
        df = attach_row_key(df)
        hide_set = set(HIDE_COLS.get(src, [])) | {"__KEY__", "__QTY__"}
        cols_for_download = [c for c in table_columns_in_order(db_path, src) if (c in df.columns) and (c not in hide_set)]
        display_cols = [c for c in cols_for_download if c not in {"Company"}]
        st.markdown(f"### Outstanding POs — {title_companies}")
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button(
                "⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(df[display_cols], sheet="Outstanding_POs"),
                file_name="Outstanding_POs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "⬇️ Word (.docx)",
                data=to_docx_table_bytes(df[cols_for_download], title=f"Outstanding POs — {title_companies}"),
                file_name="Outstanding_POs.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

    # ========== PAGE: QUOTES ==========
    else:
        st.markdown(f"### Quotes — {title_companies}")
        ensure_quotes_table(db_path)

        tab_new, tab_browse = st.tabs(["🆕 New Quote", "📁 Browse / Edit"])

        # --- New Quote (manual, not from restock) ---
        with tab_new:
            st.caption("Create a new blank quote (not from the RE-STOCK report).")
            vendor = st.text_input("Vendor (choose vendor — non-functional selector to be added later)", value="")
            ship_to = st.text_area("Ship To (optional — can auto-fill from Addresses later)", height=80, value="")
            # Blank 15 rows table
            rows = [{"Part Number":"", "Part Name":"", "Qty":""} for _ in range(15)]
            if "new_quote_rows" not in st.session_state:
                st.session_state.new_quote_rows = pd.DataFrame(rows)
            edited_new = st.data_editor(
                st.session_state.new_quote_rows,
                key="new_quote_editor",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Part Number": st.column_config.TextColumn("Part Number"),
                    "Part Name": st.column_config.TextColumn("Part Name"),
                    "Qty": st.column_config.NumberColumn("Qty", step=1, min_value=0),
                }
            )
            c_left, c_sp, c_save, c_gen, c_email = st.columns([4, 5, 1, 1, 1])
            with c_save:
                if st.button("Save", use_container_width=True):
                    qid = save_quote(db_path, company=title_companies, created_by=str(username),
                                     vendor=vendor, ship_to=ship_to, source="manual",
                                     lines_df=edited_new)
                    st.success(f"Saved as quote #{qid}")
            with c_gen:
                st.download_button(
                    "Generate",
                    data=to_docx_table_bytes(edited_new[["Part Number","Part Name","Qty"]], title="Quote Request"),
                    file_name=f"Quote_{sanitize_filename(title_companies)}_{datetime.now().strftime('%Y%m%d')}.docx",
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    use_container_width=True
                )
            with c_email:
                st.button("Email", use_container_width=True, disabled=True)

        # --- Browse / Edit ---
        with tab_browse:
            dfq = list_quotes(db_path, company=title_companies if chosen != ADMIN_ALL else None)
            if dfq.empty:
                st.info("No saved quotes yet.")
            else:
                st.dataframe(dfq, hide_index=True, use_container_width=True)
                qid = st.number_input("Quote ID to open", min_value=int(dfq["id"].min()),
                                      max_value=int(dfq["id"].max()), value=int(dfq["id"].max()), step=1)
                rec = load_quote(db_path, int(qid))
                if not rec:
                    st.warning("Quote not found.")
                else:
                    st.markdown(f"**Editing Quote #{rec['id']}** — Date: {rec['quote_date']} • Vendor: {rec['vendor']} • Status: {rec['status']}")
                    edited_exist = st.data_editor(
                        rec["lines"],
                        key=f"edit_quote_{rec['id']}",
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Part Number": st.column_config.TextColumn("Part Number"),
                            "Part Name": st.column_config.TextColumn("Part Name"),
                            "Qty": st.column_config.NumberColumn("Qty", step=1, min_value=0),
                        }
                    )
                    c_left, c_sp, c_save, c_gen, c_email = st.columns([4, 5, 1, 1, 1])
                    with c_save:
                        if st.button("Save", key=f"save_quote_{rec['id']}", use_container_width=True):
                            save_quote(db_path, company=title_companies, created_by=str(username),
                                       vendor=rec["vendor"], ship_to=rec["ship_to"], source=rec["source"],
                                       lines_df=edited_exist, quote_id=int(rec["id"]))
                            st.success("Saved")
                    with c_gen:
                        st.download_button(
                            "Generate",
                            data=to_docx_table_bytes(edited_exist[["Part Number","Part Name","Qty"]],
                                                     title=f"Quote Request — {title_companies}"),
                            file_name=f"Quote_{sanitize_filename(title_companies)}_{datetime.now().strftime('%Y%m%d')}.docx",
                            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                            use_container_width=True,
                            key=f"gen_quote_{rec['id']}"
                        )
                    with c_email:
                        st.button("Email", use_container_width=True, disabled=True)

    # ---------- Config template (admins only) ----------
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')




