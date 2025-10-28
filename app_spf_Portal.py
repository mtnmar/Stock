# app_spf_Portal.py
# --------------------------------------------------------------
# SPF portal for RE-STOCK, Outstanding POs, and Quotes
#
# Highlights in this drop-in:
# ✅ Always refresh cached GitHub DB (no stale tmp file)
# ✅ Sidebar button to force re-download + cache clear
# ✅ Shows DB file path and "last modified" timestamp (sidebar + header)
# ✅ Page structure: RE-STOCK • Outstanding POs • Quotes
# ✅ SQLite PRAGMAs + cache keys include DB file signature
# --------------------------------------------------------------

from __future__ import annotations
import os, io, re, json, sqlite3, hashlib, tempfile, time
from pathlib import Path
from typing import Tuple, List
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

APP_VERSION = "2025.10.28-r1"

# Optional Word export
DOCX_OK = True
try:
    from docx import Document
    from docx.shared import Pt, Inches
except Exception:
    DOCX_OK = False

# -------------- Page config --------------
st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

DEFAULT_DB = "maintainx_po.db"   # daily source data (overwritten by you)
QUOTES_DB  = "quotes.db"         # persistent quotes only
HERE = Path(__file__).resolve().parent

# -------------- Utilities --------------
def _filesig(p: Path) -> int:
    """Small signature used to invalidate Streamlit caches when the file changes."""
    try:
        stt = p.stat()
        return (int(stt.st_mtime_ns) ^ (stt.st_size << 13)) & 0xFFFFFFFFFFFF
    except Exception:
        return 0

def _db_sig(path: str) -> int:
    try: return Path(path).stat().st_mtime_ns
    except Exception: return 0

@st.cache_data(show_spinner=False)
def read_parquet_cached(path_str: str, sig: int) -> pd.DataFrame:
    return pd.read_parquet(path_str)

def open_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception:
        pass
    return conn

@st.cache_data(show_spinner=False)
def q_cached(sql: str, params: Tuple, db_path: str, db_sig: int) -> pd.DataFrame:
    with open_conn(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DATA_DB_PATH
    return q_cached(sql, tuple(params), path, _db_sig(path))

@st.cache_data(show_spinner=False)
def table_columns_in_order_cached(db_path: str, table: str, db_sig: int) -> list[str]:
    with open_conn(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def table_columns_in_order(db_path: str | None, table: str) -> list[str]:
    path = db_path or DATA_DB_PATH
    return table_columns_in_order_cached(path, table, _db_sig(path))

# -------------- Config + GitHub download --------------
def _to_plain(obj):
    if isinstance(obj, dict): return {k:_to_plain(v) for k,v in obj.items()}
    if isinstance(obj, (list,tuple)): return [_to_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    # Prefer secrets
    if hasattr(st, "secrets") and "app_config" in st.secrets:
        return _to_plain(st.secrets["app_config"])
    # YAML fallback
    cfg_file = HERE / "app_config.yaml"
    if cfg_file.exists():
        import yaml
        try: return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    # Minimal defaults
    return {
        "settings": {"db_path": str((HERE/DEFAULT_DB).resolve()),
                     "quotes_db_path": str((HERE/QUOTES_DB).resolve())},
        "access": {"admin_usernames": ["demo"], "user_companies": {"demo": ["*"]}},
    }

def download_db_from_github(*, repo: str, path: str, branch: str='main', token: str|None=None) -> str:
    """
    Fetch latest DB bytes from GitHub and overwrite local temp copy unconditionally.
    This guarantees a new mtime so caches invalidate.
    """
    if not repo or not path:
        raise ValueError("Missing repo/path for GitHub download.")
    import requests
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    tmpdir = Path(tempfile.gettempdir()) / "spf_po_cache"
    tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / DEFAULT_DB
    out.write_bytes(r.content)  # ALWAYS overwrite
    try:
        now = time.time()
        os.utime(out, (now, now))
    except Exception:
        pass
    return str(out.resolve())

def resolve_db_path(cfg: dict) -> str:
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db:
        return str(Path(yaml_db).expanduser().resolve())
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db:
        return str(Path(env_db).expanduser().resolve())
    gh = getattr(st, "secrets", {}).get("github") if hasattr(st, "secrets") else None
    if gh:
        try:
            return download_db_from_github(
                repo=gh.get('repo'), path=gh.get('path'),
                branch=gh.get('branch','main'), token=gh.get('token')
            )
        except Exception as e:
            st.error(f"Failed to download DB from GitHub: {e}")
    return str((HERE / DEFAULT_DB).resolve())

def resolve_quotes_db_path(cfg: dict) -> str:
    yaml_q = (cfg or {}).get('settings', {}).get('quotes_db_path')
    if yaml_q: return str(Path(yaml_q).expanduser().resolve())
    env_q = os.environ.get('SPF_QUOTES_DB_PATH')
    if env_q: return str(Path(env_q).expanduser().resolve())
    return str((HERE / QUOTES_DB).resolve())

# -------------- Display helpers --------------
def attach_row_key(df_in: pd.DataFrame) -> pd.DataFrame:
    df_in = df_in.copy()
    key_col = next((c for c in ["ID","id","Purchase Order ID","Row ID","RowID"] if c in df_in.columns), None)
    if key_col:
        df_in["__KEY__"] = df_in[key_col].astype(str)
        return df_in
    cols = [c for c in ["Part Number","Part Numbers","Name","Description","Vendor","Company","Created On"] if c in df_in.columns] or list(df_in.columns)
    s = df_in[cols].astype(str).agg("|".join, axis=1)
    df_in["__KEY__"] = s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df_in

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]
        ws.autofilter(0,0,max(0,len(df)), max(0,len(df.columns)-1))
        for i,_ in enumerate(df.columns):
            try:
                width = 16 if df.empty else min(60, max(12, int(df.iloc[:,i].astype(str).str.len().quantile(0.9))+2))
                ws.set_column(i,i,width)
            except Exception:
                pass
    return buf.getvalue()

def build_quote_docx(company: str, date_str: str, quote_number: str,
                     vendor_text: str, ship_to_text: str, bill_to_text: str,
                     lines_df: pd.DataFrame) -> bytes:
    if not DOCX_OK:
        raise RuntimeError("python-docx not installed.")
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    hdr = doc.add_paragraph(); r = hdr.add_run(company); r.bold = True; r.font.size = Pt(14)
    title = doc.add_paragraph(); r2 = title.add_run("Quote Request"); r2.bold = True; r2.font.size = Pt(16)
    doc.add_paragraph(date_str); doc.add_paragraph(f"Quote #: {quote_number}")

    doc.add_paragraph(""); vr = doc.add_paragraph(); vr.add_run("Vendor").bold = True
    doc.add_paragraph(vendor_text if vendor_text.strip() else "_____________________________")

    doc.add_paragraph("")
    tbl_addr = doc.add_table(rows=2, cols=2)
    tbl_addr.rows[0].cells[0].text = "Ship To Address"
    tbl_addr.rows[0].cells[1].text = "Bill To Address"
    tbl_addr.rows[1].cells[0].text = ship_to_text
    tbl_addr.rows[1].cells[1].text = bill_to_text

    cols = ["Part Number","Description","Quantity","Price/Unit","Total"]
    lines = lines_df.copy()
    for c in cols:
        if c not in lines.columns: lines[c] = ""
    BLANK_ROWS = max(10, 30 - len(lines))
    if BLANK_ROWS>0: lines = pd.concat([lines, pd.DataFrame([dict(zip(cols, [""]*5)) for _ in range(BLANK_ROWS)])], ignore_index=True)

    doc.add_paragraph("")
    tbl = doc.add_table(rows=1 + len(lines), cols=len(cols)); tbl.style = 'Table Grid'
    widths = [Inches(1.7), Inches(3.6), Inches(0.9), Inches(1.2), Inches(1.2)]
    for j in range(len(cols)):
        for row in tbl.rows:
            row.cells[j].width = widths[j]
    for j,c in enumerate(cols): tbl.cell(0,j).text = c
    for i,(_,row) in enumerate(lines.iterrows(), start=1):
        for j,c in enumerate(cols): tbl.cell(i,j).text = str("" if pd.isna(row[c]) else row[c])

    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

# -------------- Boot --------------
cfg = load_config(); cfg = _to_plain(cfg)
DATA_DB_PATH   = resolve_db_path(cfg)
QUOTES_DB_PATH = resolve_quotes_db_path(cfg)

# Sidebar info + controls
def _db_info_caption(path: str) -> str:
    try:
        p = Path(path); ts = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        return f"DB: `{p}`  \nLast modified: **{ts}**"
    except Exception:
        return f"DB: `{path}`"

st.sidebar.markdown("### Data")
st.sidebar.caption(_db_info_caption(DATA_DB_PATH))
st.sidebar.caption(f"Quotes DB: `{Path(QUOTES_DB_PATH).resolve()}`")

# Force-refresh: redownload from GitHub and bust caches
if st.sidebar.button("🕸️ Re-download DB from GitHub (force)", use_container_width=True):
    try:
        cached = Path(tempfile.gettempdir()) / "spf_po_cache" / DEFAULT_DB
        if cached.exists():
            cached.unlink(missing_ok=True)
    except Exception:
        pass
    gh = getattr(st, "secrets", {}).get("github") if hasattr(st, "secrets") else None
    if gh:
        try:
            DATA_DB_PATH = download_db_from_github(
                repo=gh.get('repo'), path=gh.get('path'),
                branch=gh.get('branch','main'), token=gh.get('token')
            )
            st.success("DB re-downloaded.")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Re-download failed: {e}")
    else:
        st.warning("GitHub secrets not configured; using local DB only.")

if st.sidebar.button("🔄 Clear caches", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Topline banner shows newest DB timestamp
try:
    db_ts = datetime.fromtimestamp(Path(DATA_DB_PATH).stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
except Exception:
    db_ts = "(unknown)"
st.markdown(f"## SPF PO Portal  \n**Data last updated:** {db_ts}  \n_Version {APP_VERSION}_")

# Page selection
page = st.radio("Page", ["RE-STOCK", "Outstanding POs", "Quotes"], horizontal=True, key="page_radio")

# Companies list from RE-STOCK
def company_list() -> List[str]:
    try:
        df = q("SELECT DISTINCT [Company] FROM [restock] WHERE [Company] IS NOT NULL ORDER BY 1", db_path=DATA_DB_PATH)
        return [str(x) for x in df["Company"].dropna().tolist()]
    except Exception:
        return []

company_options = company_list() or ["(none)"]
chosen_company = st.selectbox("Location", options=company_options, index=0, key="company_select_main")

def visible_cols(df: pd.DataFrame, extra_hide: set[str] | None = None) -> list[str]:
    hide = {"ID","id","Purchase Order ID","__KEY__"} | (extra_hide or set())
    return [c for c in df.columns if c not in hide]

# --------------------- RE-STOCK ---------------------
if page == "RE-STOCK":
    # Optional parquet (via secrets.settings.parquet.restock)
    pq_path = None
    try:
        p_cfg = (cfg.get('settings', {}) or {}).get('parquet', {}) or {}
        if "restock" in p_cfg:
            p = Path(str(p_cfg["restock"])).expanduser()
            if p.exists(): pq_path = p
    except Exception:
        pq_path = None

    if pq_path:
        df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
    else:
        df_all = q("SELECT * FROM [restock]", db_path=DATA_DB_PATH)

    if df_all.empty:
        st.info("No RE-STOCK data found.")
    else:
        df = df_all.copy()
        if "Company" in df.columns and chosen_company:
            df = df[df["Company"].astype(str).str.strip() == str(chosen_company).strip()]
        df = attach_row_key(df)
        df_disp = df[visible_cols(df, {"__QTY__"})].copy()
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ Download view (.xlsx)",
            data=to_xlsx_bytes(df_disp, sheet="RE_STOCK"),
            file_name=f"RE_STOCK_{chosen_company or 'all'}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_restock_xlsx"
        )

# ----------------- Outstanding POs -----------------
elif page == "Outstanding POs":
    pq_path = None
    try:
        p_cfg = (cfg.get('settings', {}) or {}).get('parquet', {}) or {}
        if "po_outstanding" in p_cfg:
            p = Path(str(p_cfg["po_outstanding"])).expanduser()
            if p.exists(): pq_path = p
    except Exception:
        pq_path = None

    if pq_path:
        df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
    else:
        df_all = q("SELECT * FROM [po_outstanding]", db_path=DATA_DB_PATH)

    if df_all.empty:
        st.info("No Outstanding POs data found.")
    else:
        df = df_all.copy()
        if "Company" in df.columns and chosen_company:
            df = df[df["Company"].astype(str).str.strip() == str(chosen_company).strip()]
        s = st.text_input("Search PO # / Vendor / Part / Line contains")
        if s:
            cols = [c for c in ["Purchase Order #","Vendor","Part Number","Line Name"] if c in df.columns]
            if cols:
                m = pd.Series(False, index=df.index)
                for c in cols:
                    m |= df[c].astype(str).str.contains(s, case=False, na=False)
                df = df[m]
        df = attach_row_key(df)
        df_disp = df[visible_cols(df)].copy()
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ Download view (.xlsx)",
            data=to_xlsx_bytes(df_disp, sheet="Outstanding_POs"),
            file_name=f"Outstanding_POs_{chosen_company or 'all'}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_po_xlsx"
        )

# ----------------------- Quotes -----------------------
else:
    def ensure_quotes_table(db_path: str) -> None:
        with open_conn(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quotes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  quote_number TEXT UNIQUE,
                  company TEXT,
                  created_by TEXT,
                  vendor TEXT,
                  ship_to TEXT,
                  bill_to TEXT,
                  quote_date TEXT,
                  status TEXT,
                  source TEXT,
                  lines_json TEXT,
                  updated_at TEXT
                )
            """)
            conn.commit()

    ensure_quotes_table(QUOTES_DB_PATH)

    def next_quote_number(db_path: str, date_obj: datetime) -> str:
        yr = date_obj.strftime("%Y")
        with open_conn(db_path) as conn:
            rows = conn.execute("SELECT quote_number FROM quotes WHERE quote_number LIKE ?", (f"QR-{yr}-%",)).fetchall()
        used = set()
        for (qn,) in rows:
            try:
                parts = str(qn).split("-"); seq = int(parts[-1])
                if str(parts[1]) == yr: used.add(seq)
            except Exception:
                pass
        seq = 1
        while seq in used: seq += 1
        return f"QR-{yr}-{seq:04d}"

    st.subheader("New Quote")
    c1, c2 = st.columns([1,1])
    with c1:
        company_new = st.selectbox(
            "Location", options=company_options,
            index=max(0, company_options.index(chosen_company) if chosen_company in company_options else 0)
        )
        vendor = st.text_input("Vendor", value="")
        quote_no = st.text_input("Quote #", value=next_quote_number(QUOTES_DB_PATH, datetime.utcnow()))
    with c2:
        ship_to = st.text_area("Ship To", value="", height=120)
        bill_to = st.text_area("Bill To", value="", height=120)

    st.caption("Enter line items (Price/Unit/Total optional):")
    lines = st.data_editor(
        pd.DataFrame([{"Part Number":"","Description":"","Quantity":"","Price/Unit":"","Total":""} for _ in range(12)]),
        hide_index=True, use_container_width=True, num_rows="dynamic",
        column_config={
            "Part Number": st.column_config.TextColumn("Part Number"),
            "Description": st.column_config.TextColumn("Description"),
            "Quantity":    st.column_config.NumberColumn("Qty", min_value=0, step=1),
            "Price/Unit":  st.column_config.TextColumn("Price/Unit"),
            "Total":       st.column_config.TextColumn("Total"),
        }
    )

    a1, a2, _ = st.columns([1,1,5])
    if a1.button("Save Quote"):
        payload = json.dumps(lines.fillna("").astype(str).to_dict(orient="records"), ensure_ascii=False)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with open_conn(QUOTES_DB_PATH) as conn:
            try:
                conn.execute("""
                    INSERT INTO quotes(quote_number, company, created_by, vendor, ship_to, bill_to,
                                       quote_date, status, source, lines_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, date('now'), ?, ?, ?, ?)
                """, (quote_no, company_new, "user", vendor, ship_to, bill_to, "draft", "manual", payload, now))
                conn.commit()
                st.success(f"Saved quote {quote_no}")
            except sqlite3.IntegrityError:
                st.error("Quote # already exists. Change number and try again.")

    if a2.button("Download Word"):
        try:
            doc_bytes = build_quote_docx(
                company=company_new, date_str=datetime.now().strftime("%Y-%m-%d"),
                quote_number=quote_no, vendor_text=vendor,
                ship_to_text=ship_to, bill_to_text=bill_to, lines_df=lines
            )
            st.download_button(
                "Download Quote (Word)",
                data=doc_bytes,
                file_name=f"{quote_no}_{re.sub(r'[^A-Za-z0-9._ -]+','_',company_new)}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key="dl_quote_word_now"
            )
        except Exception as e:
            st.error(f"Could not build Word file: {e}")

    st.divider()
    st.subheader("Saved Quotes")
    with open_conn(QUOTES_DB_PATH) as conn:
        dfq = pd.read_sql_query(
            "SELECT id, quote_number, quote_date, company, vendor, status, length(lines_json) AS bytes FROM quotes ORDER BY id DESC",
            conn
        )
    st.dataframe(dfq, use_container_width=True, hide_index=True)



