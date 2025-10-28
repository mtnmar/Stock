# app_spf_Portal.py — refresh-safe DB selector (GitHub vs local) + recency banner
# ---------------------------------------------------------------------------------
# What’s new
# - Prefers GitHub download when configured; YAML/ENV/local path otherwise
# - Sidebar shows DB path + OS file last-modified timestamp
# - "Re-download from GitHub (force)" fetches a fresh copy to a NEW temp path
# - Manual local DB picker (absolute path) with "Use this DB" button
# - Cache keys include the DB file signature + a manual nonce you can bump
# - "Recency Check" panel: max date detected in each table (restock, po_outstanding)
#
# Minimal deps:
#   streamlit>=1.27
#   pandas>=2.0
#   requests>=2.31     # only if you use the GitHub fetch button
#   xlsxwriter>=3.2    # for Excel downloads
#
# Optional:
#   python-docx>=1.1   # to build Word quote requests (kept simple here)
# ---------------------------------------------------------------------------------

from __future__ import annotations

import os, io, json, sqlite3, hashlib, tempfile, time, re
from pathlib import Path
from typing import Tuple, List, Dict, Optional
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

APP_VERSION = "2025.10.28-refresh-safe"

# Optional Word export
DOCX_OK = True
try:
    from docx import Document
    from docx.shared import Pt, Inches
except Exception:
    DOCX_OK = False

st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

DEFAULT_DB = "maintainx_po.db"
QUOTES_DB  = "quotes.db"
HERE = Path(__file__).resolve().parent

# ---------------- Utils ----------------

def _filesig(p: Path) -> int:
    """Compact signature from mtime_ns and file size."""
    try:
        stt = p.stat()
        return (int(stt.st_mtime_ns) ^ (stt.st_size << 13)) & 0xFFFFFFFFFFFF
    except Exception:
        return 0

def _db_sig(path: str) -> int:
    try:
        return Path(path).stat().st_mtime_ns
    except Exception:
        return 0

@st.cache_data(show_spinner=False)
def q_cached(sql: str, params: Tuple, db_path: str, db_sig: int, nonce: int) -> pd.DataFrame:
    with sqlite3.connect(db_path, timeout=30, check_same_thread=False) as conn:
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return pd.read_sql_query(sql, conn, params=params)

def q(sql: str, params: tuple = (), db_path: Optional[str] = None) -> pd.DataFrame:
    path = db_path or st.session_state.get("DATA_DB_PATH", "")
    return q_cached(sql, tuple(params), path, _db_sig(path), st.session_state.get("CACHE_NONCE", 0))

@st.cache_data(show_spinner=False)
def table_columns_in_order_cached(db_path: str, table: str, db_sig: int, nonce: int) -> list[str]:
    with sqlite3.connect(db_path, timeout=30, check_same_thread=False) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def table_columns_in_order(db_path: Optional[str], table: str) -> list[str]:
    path = db_path or st.session_state.get("DATA_DB_PATH", "")
    return table_columns_in_order_cached(path, table, _db_sig(path), st.session_state.get("CACHE_NONCE", 0))

def attach_row_key(df_in: pd.DataFrame) -> pd.DataFrame:
    df_in = df_in.copy()
    key_col = next((c for c in ["ID","id","Purchase Order ID","Row ID","RowID"] if c in df_in.columns), None)
    if key_col:
        df_in["__KEY__"] = df_in[key_col].astype(str); return df_in
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
    doc.add_paragraph(vendor_text if str(vendor_text).strip() else "_____________________________")

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

# ---------------- Config + source selection ----------------

def cfg_plain(obj):
    if isinstance(obj, dict): return {k: cfg_plain(v) for k,v in obj.items()}
    if isinstance(obj, (list,tuple)): return [cfg_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    # secrets first
    if hasattr(st, "secrets") and "app_config" in st.secrets:
        return cfg_plain(st.secrets["app_config"])
    # yaml fallback
    cfg_file = HERE / "app_config.yaml"
    if cfg_file.exists():
        try:
            import yaml
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
    # default minimal
    return {
        "settings": {"db_path": str((HERE/DEFAULT_DB).resolve()),
                     "quotes_db_path": str((HERE/QUOTES_DB).resolve())},
    }

def get_github_cfg() -> Optional[Dict[str,str]]:
    gh = getattr(st, "secrets", {}).get("github") if hasattr(st, "secrets") else None
    if isinstance(gh, dict) and gh.get("repo") and gh.get("path"):
        return {"repo": gh.get("repo"), "path": gh.get("path"),
                "branch": gh.get("branch","main"), "token": gh.get("token")}
    return None

def download_db_from_github(repo: str, path: str, branch: str='main', token: Optional[str]=None) -> str:
    import requests
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token: headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    tmpdir = Path(tempfile.gettempdir()) / "spf_po_cache"
    tmpdir.mkdir(parents=True, exist_ok=True)
    # write to a NEW, timestamped file so we don't reuse the 10/24 tmp
    out = tmpdir / f"{int(time.time())}_{DEFAULT_DB}"
    out.write_bytes(r.content)
    # touch mtime so caches definitely see a change
    now = time.time()
    os.utime(out, (now, now))
    return str(out.resolve())

def resolve_db_path(cfg: dict) -> str:
    # 1) manual override from UI
    if st.session_state.get("MANUAL_DB_PATH") and Path(st.session_state["MANUAL_DB_PATH"]).exists():
        return st.session_state["MANUAL_DB_PATH"]

    # 2) prefer GitHub if configured
    gh = get_github_cfg()
    if gh:
        try:
            p = download_db_from_github(**gh)
            st.session_state["GITHUB_DB_PATH"] = p
            return p
        except Exception as e:
            st.warning(f"GitHub fetch failed, falling back to local/YAML. Error: {e}")

    # 3) YAML / local path
    yaml_db = (cfg or {}).get("settings",{}).get("db_path")
    if yaml_db:
        return str(Path(yaml_db).expanduser().resolve())

    # 4) env var
    env_db = os.environ.get("SPF_DB_PATH")
    if env_db:
        return str(Path(env_db).expanduser().resolve())

    # 5) default
    return str((HERE/DEFAULT_DB).resolve())

def resolve_quotes_db_path(cfg: dict) -> str:
    yaml_q = (cfg or {}).get('settings', {}).get('quotes_db_path')
    if yaml_q: return str(Path(yaml_q).expanduser().resolve())
    env_q = os.environ.get('SPF_QUOTES_DB_PATH')
    if env_q: return str(Path(env_q).expanduser().resolve())
    return str((HERE / QUOTES_DB).resolve())

# ---------------- Boot ----------------

cfg = load_config(); cfg = cfg_plain(cfg)
if "CACHE_NONCE" not in st.session_state: st.session_state["CACHE_NONCE"] = 0
if "DATA_DB_PATH" not in st.session_state: st.session_state["DATA_DB_PATH"] = resolve_db_path(cfg)
if "QUOTES_DB_PATH" not in st.session_state: st.session_state["QUOTES_DB_PATH"] = resolve_quotes_db_path(cfg)

DATA_DB_PATH   = st.session_state["DATA_DB_PATH"]
QUOTES_DB_PATH = st.session_state["QUOTES_DB_PATH"]

# Sidebar: DB info + controls
def db_info_caption(path: str) -> str:
    try:
        p = Path(path)
        ts = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        return f"**DB:** `{p}`  \n**Last modified:** {ts}"
    except Exception:
        return f"**DB:** `{path}`"

st.sidebar.markdown("### Data Sources")
st.sidebar.caption(db_info_caption(DATA_DB_PATH))
st.sidebar.caption(f"Quotes DB: `{Path(QUOTES_DB_PATH).resolve()}`")

c1, c2 = st.sidebar.columns(2)
if c1.button("🔄 Clear cache", use_container_width=True):
    st.cache_data.clear()
    st.session_state["CACHE_NONCE"] += 1
    st.rerun()
if c2.button("🕸️ Re-download (GitHub)", use_container_width=True):
    gh = get_github_cfg()
    if gh:
        try:
            p = download_db_from_github(**gh)
            st.session_state["MANUAL_DB_PATH"] = None
            st.session_state["GITHUB_DB_PATH"] = p
            st.session_state["DATA_DB_PATH"] = p
            st.cache_data.clear()
            st.session_state["CACHE_NONCE"] += 1
            st.success("Fetched latest DB from GitHub.")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Download failed: {e}")
    else:
        st.sidebar.warning("GitHub secrets not configured.")

st.sidebar.markdown("---")
manual = st.sidebar.text_input("Local DB path (absolute)", value="")
if st.sidebar.button("Use this DB", use_container_width=True):
    if manual and Path(manual).exists():
        st.session_state["MANUAL_DB_PATH"] = str(Path(manual).resolve())
        st.session_state["DATA_DB_PATH"]   = st.session_state["MANUAL_DB_PATH"]
        st.cache_data.clear()
        st.session_state["CACHE_NONCE"] += 1
        st.rerun()
    else:
        st.sidebar.error("Path not found.")

# Header with version + db timestamp
try:
    db_ts = datetime.fromtimestamp(Path(DATA_DB_PATH).stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
except Exception:
    db_ts = "(unknown)"
st.markdown(f"## SPF PO Portal  \n**Data last updated (file mtime):** {db_ts}  \n_Version {APP_VERSION}_")

# Quick recency check (max date in each known table)
def max_date_from(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                return s.max().strftime("%Y-%m-%d")
    return None

rec_cols = {
    "restock": ["Part Updated on","Last Updated","Posting Date","Approved On","Completed On","Created On","Needed By"],
    "po_outstanding": ["Created On","Approved On","Completed On","Posting Date"],
}

with st.expander("Recency Check (max dates detected)"):
    for table, cols in rec_cols.items():
        try:
            df = q(f"SELECT * FROM [{table}]", db_path=DATA_DB_PATH)
            when = max_date_from(df, cols)
            st.write(f"- **{table}**: {when or '(no date columns present)'}")
        except Exception as e:
            st.write(f"- **{table}**: not available ({e})")

# Page selection
page = st.radio("Page", ["RE-STOCK", "Outstanding POs", "Quotes"], horizontal=True, key="page_radio")

def visible_cols(df: pd.DataFrame, extra_hide: set[str] | None = None) -> list[str]:
    hide = {"ID","id","Purchase Order ID","__KEY__"} | (extra_hide or set())
    return [c for c in df.columns if c not in hide]

# Companies list from RE-STOCK
def company_list() -> List[str]:
    try:
        df = q("SELECT DISTINCT [Company] FROM [restock] WHERE [Company] IS NOT NULL ORDER BY 1", db_path=DATA_DB_PATH)
        return [str(x) for x in df["Company"].dropna().tolist()]
    except Exception:
        return []

company_options = company_list() or ["(none)"]
chosen_company = st.selectbox("Location", options=company_options, index=0, key="company_select_main")

# --------------------- RE-STOCK ---------------------
if page == "RE-STOCK":
    try:
        df_all = q("SELECT * FROM [restock]", db_path=DATA_DB_PATH)
    except Exception as e:
        st.error(f"Could not read [restock]: {e}")
        df_all = pd.DataFrame()

    if df_all.empty:
        st.info("No RE-STOCK data found.")
    else:
        df = df_all.copy()
        if "Company" in df.columns and chosen_company and chosen_company != "(none)":
            df = df[df["Company"].astype(str).str.strip() == str(chosen_company).strip()]

        s = st.text_input("Search Part Numbers / Name / Vendor contains", key="restock_search")
        if s:
            cols = [c for c in ["Part Numbers","Name","Vendor","Vendors"] if c in df.columns]
            if cols:
                m = pd.Series(False, index=df.index)
                for c in cols:
                    m |= df[c].astype(str).str.contains(s, case=False, na=False)
                df = df[m]

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
    try:
        df_all = q("SELECT * FROM [po_outstanding]", db_path=DATA_DB_PATH)
    except Exception as e:
        st.error(f"Could not read [po_outstanding]: {e}")
        df_all = pd.DataFrame()

    if df_all.empty:
        st.info("No Outstanding POs data found.")
    else:
        df = df_all.copy()
        if "Company" in df.columns and chosen_company and chosen_company != "(none)":
            df = df[df["Company"].astype(str).str.strip() == str(chosen_company).strip()]
        s = st.text_input("Search PO # / Vendor / Part / Line contains", key="po_search")
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
    # tiny quotes section just to keep parity
    def next_quote_number(db_path: str, date_obj: datetime) -> str:
        with sqlite3.connect(db_path, timeout=30, check_same_thread=False) as conn:
            rows = conn.execute("SELECT quote_number FROM quotes WHERE quote_number LIKE ?", (f"QR-{date_obj:%Y}-%",)).fetchall()
        used = set()
        for (qn,) in rows:
            try:
                parts = str(qn).split("-"); seq = int(parts[-1])
                if str(parts[1]) == f"{date_obj:%Y}": used.add(seq)
            except Exception:
                pass
        seq = 1
        while seq in used: seq += 1
        return f"QR-{date_obj:%Y}-{seq:04d}"

    with sqlite3.connect(QUOTES_DB_PATH, timeout=30, check_same_thread=False) as conn:
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
        """); conn.commit()

    st.subheader("New Quote")
    c1, c2 = st.columns([1,1])
    with c1:
        company_new = st.selectbox("Location", options=(company_options or [""]), index=0)
        vendor      = st.text_input("Vendor", value="")
        quote_no    = st.text_input("Quote #", value=next_quote_number(QUOTES_DB_PATH, datetime.utcnow()))
    with c2:
        ship_to = st.text_area("Ship To", value="", height=120)
        bill_to = st.text_area("Bill To", value="", height=120)

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
        with sqlite3.connect(QUOTES_DB_PATH, timeout=30, check_same_thread=False) as conn:
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
    with sqlite3.connect(QUOTES_DB_PATH, timeout=30, check_same_thread=False) as conn:
        dfq = pd.read_sql_query(
            "SELECT id, quote_number, quote_date, company, vendor, status, length(lines_json) AS bytes FROM quotes ORDER BY id DESC",
            conn
        )
    st.dataframe(dfq, use_container_width=True, hide_index=True)

