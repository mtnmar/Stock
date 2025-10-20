# app_spf_portal.py
# --------------------------------------------------------------
# SPF portal for RE-STOCK, Outstanding POs, and Quotes
#
# ✅ Save quotes to SQLite table 'quotes' and download Word on Generate
# ✅ Remove numeric "### -" prefix from Company ONLY in Word document
# ✅ Billing from 'addresses' (Billing, Billing Contact, BillingEmail, BillingPhone)
# ✅ Ship To from 'addresses' + user contact (User_Data / user_data)
# ✅ RE-STOCK XLSX download kept
# ✅ Cart buttons: Remove (left, small) | Clear, Save, Generate, Email (right, small)
# ✅ Quotes page: New (choose Location & Vendor, autofill addresses), Browse/Edit (filter by company)
#
# requirements.txt (min):
#   streamlit>=1.37
#   streamlit-authenticator==0.2.3
#   pandas>=2.0
#   openpyxl>=3.1
#   xlsxwriter>=3.2
#   python-docx>=1.1
#   pyyaml>=6.0
#   requests>=2.31
#   pyarrow>=17.0   # or fastparquet
from __future__ import annotations
import os, io, re, json, sqlite3, textwrap, hashlib
from pathlib import Path
from collections.abc import Mapping, Iterable
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.20-DOCX-ONLY"

# ---- deps ----
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed. Add to requirements.txt")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
except Exception:
    st.error("python-docx not installed. Add to requirements.txt")
    st.stop()

st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

# ---------- Defaults & config ----------
DEFAULT_DB = "maintainx_po.db"
HERE = Path(__file__).resolve().parent
ACTIVE_DB_PATH: str | None = None  # set after resolve

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
    demo: ['*']

settings:
  db_path: ""
  # parquet:
  #   restock: /path/to/restock.parquet
  #   po_outstanding: /path/to/po_outstanding.parquet
"""

# ---------- helpers ----------
def to_plain(obj):
    if isinstance(obj, Mapping): return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [to_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    if "app_config" in st.secrets:
        return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:
        try: return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}"); return {}
    cfg_file = HERE / "app_config.yaml"
    if cfg_file.exists():
        try: return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}"); return {}
    return yaml.safe_load(CONFIG_TEMPLATE_YAML)

def resolve_db_path(cfg: dict) -> str:
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db: return yaml_db
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db: return env_db
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh:
        try:
            return download_db_from_github(
                repo=gh.get('repo'), path=gh.get('path'),
                branch=gh.get('branch','main'), token=gh.get('token'),
            )
        except Exception as e:
            st.error(f"Failed to download DB from GitHub: {e}")
    return DEFAULT_DB

def download_db_from_github(*, repo: str, path: str, branch: str='main', token: str|None=None) -> str:
    if not repo or not path: raise ValueError("Missing repo/path for GitHub download.")
    import requests, tempfile
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token: headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200: raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    tmpdir = Path(tempfile.gettempdir()) / "spf_po_cache"; tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / "maintainx_po.db"; out.write_bytes(r.content); return str(out)

# ---------- Parquet detection ----------
def detect_parquet_paths(cfg: dict) -> Dict[str, Optional[Path]]:
    p_cfg = (cfg.get('settings', {}) or {}).get('parquet', {}) or {}
    def as_path(x):
        try: return Path(str(x)).expanduser().resolve()
        except Exception: return None
    restock = as_path(p_cfg.get('restock')) if p_cfg else None
    po_out  = as_path(p_cfg.get('po_outstanding')) if p_cfg else None
    if not restock:
        env = os.environ.get('SPF_RESTOCK_PARQUET'); restock = as_path(env) if env else None
    if not po_out:
        env = os.environ.get('SPF_PO_PARQUET'); po_out = as_path(env) if env else None
    base = os.environ.get('SPF_PARQUET_DIR')
    if base and not restock: restock = as_path(Path(base)/"restock.parquet")
    if base and not po_out:  po_out  = as_path(Path(base)/"po_outstanding.parquet")
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
    p = pq_paths.get(src); return p if p and p.exists() else None

# ---------- SQLite helpers ----------
def _db_sig(db_path: str) -> int:
    try: return Path(db_path).stat().st_mtime_ns
    except Exception: return 0

@st.cache_data(show_spinner=False)
def q_cached(sql: str, params: Tuple, db_path: str, db_sig: int) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or ACTIVE_DB_PATH or DEFAULT_DB
    return q_cached(sql, tuple(params), path, _db_sig(path))

@st.cache_data(show_spinner=False)
def table_columns_in_order_cached(db_path: str, table: str, db_sig: int) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def table_columns_in_order(db_path: str | None, table: str) -> list[str]:
    path = db_path or ACTIVE_DB_PATH or DEFAULT_DB
    return table_columns_in_order_cached(path, table, _db_sig(path))

# ---- keys & misc ----
KEY_COL_CANDIDATES = ["ID","id","Purchase Order ID","Row ID","RowID"]
def attach_row_key(df_in: pd.DataFrame) -> pd.DataFrame:
    df_in = df_in.copy()
    key_col = next((c for c in KEY_COL_CANDIDATES if c in df_in.columns), None)
    if key_col:
        df_in["__KEY__"] = df_in[key_col].astype(str); return df_in
    cols = [c for c in ["Part Number","Part Numbers","Part #","Part No","PN","Name","Line Name","Description",
                        "Vendor","Vendors","Company","Created On"] if c in df_in.columns]
    if not cols: cols = list(df_in.columns)
    s = df_in[cols].astype(str).agg("|".join, axis=1)
    df_in["__KEY__"] = s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df_in

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9._ -]+', '_', str(name or "")).strip()[:80] or "file"

# ---- Excel simple export ----
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

# ---------- Quote storage ----------
def ensure_quotes_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
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

def quotes_count(db_path: str) -> int:
    ensure_quotes_table(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            v = conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
        return int(v)
    except Exception:
        return 0

def _parse_year_and_seq(qn: str) -> Tuple[Optional[int], Optional[int]]:
    try:
        parts = qn.split("-")
        if len(parts) != 3: return (None, None)
        if not parts[0].upper().startswith("QR"): return (None,None)
        yr = int(parts[1]); seq = int(parts[2]); return (yr, seq)
    except Exception:
        return (None, None)

def _next_quote_number(db_path: str, date_obj: datetime) -> str:
    yr = date_obj.strftime("%Y")
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT quote_number FROM quotes WHERE quote_number LIKE ?", (f"QR-{yr}-%",)).fetchall()
    used = set()
    for (qn,) in rows:
        y, s = _parse_year_and_seq(qn or "")
        if y is not None and s is not None and str(y) == yr:
            used.add(s)
    seq = 1
    while seq in used: seq += 1
    return f"QR-{yr}-{seq:04d}"

def _coerce_lines_for_storage(df_lines: pd.DataFrame) -> pd.DataFrame:
    cols = ["Part Number","Description","Quantity","Price/Unit","Total"]
    out = df_lines.copy()
    for c in cols:
        if c not in out.columns: out[c] = ""
    return out[cols]

def save_quote(db_path: str, *, quote_number: Optional[str], company: str, created_by: str,
               vendor: str, ship_to: str, bill_to: str, source: str,
               lines_df: pd.DataFrame, status: str = "draft", quote_id: Optional[int] = None) -> Tuple[int, str]:
    ensure_quotes_table(db_path)
    if not quote_number:
        quote_number = _next_quote_number(db_path, datetime.utcnow())
    lines = _coerce_lines_for_storage(lines_df).fillna("").astype(str)
    payload = json.dumps(lines.to_dict(orient="records"), ensure_ascii=False)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(db_path) as conn:
        if quote_id is None:
            conn.execute("""
                INSERT INTO quotes(quote_number, company, created_by, vendor, ship_to, bill_to,
                                   quote_date, status, source, lines_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, date('now'), ?, ?, ?, ?)
            """, (quote_number, company, created_by, vendor, ship_to, bill_to, status, source, payload, now))
            rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return int(rid), quote_number
        else:
            conn.execute("""
                UPDATE quotes
                   SET quote_number=?, company=?, created_by=?, vendor=?, ship_to=?, bill_to=?,
                       status=?, source=?, lines_json=?, updated_at=?, quote_date=quote_date
                 WHERE id=?
            """, (quote_number, company, created_by, vendor, ship_to, bill_to,
                  status, source, payload, now, quote_id))
            conn.commit()
            return int(quote_id), quote_number

def load_quote(db_path: str, quote_id: int) -> Optional[dict]:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT * FROM quotes WHERE id=?", (quote_id,)).fetchone()
    if not row: return None
    cols = ["id","quote_number","company","created_by","vendor","ship_to","bill_to",
            "quote_date","status","source","lines_json","updated_at"]
    rec = dict(zip(cols, row))
    try:
        rec["lines"] = pd.DataFrame(json.loads(rec["lines_json"]))
    except Exception:
        rec["lines"] = pd.DataFrame(columns=["Part Number","Description","Quantity","Price/Unit","Total"])
    return rec

def list_quotes(db_path: str, company: Optional[str]=None, include_all: bool=False) -> pd.DataFrame:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        if company and not include_all:
            df = pd.read_sql_query(
                "SELECT id, quote_number, quote_date, vendor, status, source, length(lines_json) as bytes, company "
                "FROM quotes WHERE company=? ORDER BY id DESC", conn, params=(company,))
        else:
            df = pd.read_sql_query(
                "SELECT id, quote_number, quote_date, vendor, status, source, length(lines_json) as bytes, company "
                "FROM quotes ORDER BY id DESC", conn)
    return df

# ---------- Addresses & contacts ----------
@st.cache_data(show_spinner=False)
def _load_table(db_path: str, name: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql_query(f"SELECT * FROM [{name}]", conn)
    except Exception:
        return pd.DataFrame()

def _user_table(db_path: str) -> Tuple[pd.DataFrame, str]:
    for nm in ["User_Data", "user_data", "user_contacts"]:
        df = _load_table(db_path, nm)
        if not df.empty:
            return df, nm
    return pd.DataFrame(), "User_Data"

def _pick_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for want in candidates:
        for lc, orig in lower.items():
            if want.lower() == lc or want.lower() in lc:
                return orig
    return None

def _join_nonempty(*parts: str, sep: str = " ") -> str:
    items = [str(p).strip() for p in parts if str(p).strip()]
    return sep.join(items)

def _split_semicolon_lines(s: str) -> List[str]:
    s = str(s or "").strip()
    if not s: return []
    return [p.strip() for p in s.split(";") if p.strip()] if ";" in s else [s]

def remove_prefix_for_display(company: str) -> str:
    # Strip leading "### -" (and surrounding spaces) ONLY for the Word doc
    return re.sub(r'^\s*\d+\s*-\s*', '', str(company or "")).strip()

def _match_user_contact(df: pd.DataFrame, company: str, username: str) -> Tuple[str,str,str]:
    """
    Return (Contact, Email, Phone). Prefer exact username match; do NOT fall back to random rows.
    If no username match, return blanks.
    """
    if df.empty:
        return ("", "", "")

    user_col    = _pick_first_col(df, ["Username","User","Login","Email","UserName"])
    contact_col = _pick_first_col(df, ["Contact","Name"]) or "Contact"
    email_col   = _pick_first_col(df, ["Email"]) or "Email"
    phone_col   = _pick_first_col(df, ["Phone","Telephone","Cell"]) or "Phone"

    if user_col and (user_col in df.columns):
        mask_user = df[user_col].astype(str).str.strip().str.casefold() == str(username).strip().casefold()
        narrowed_user = df[mask_user]
        if not narrowed_user.empty:
            row = narrowed_user.iloc[0]
            return (
                str(row.get(contact_col, "") or "").strip(),
                str(row.get(email_col, "") or "").strip(),
                str(row.get(phone_col, "") or "").strip(),
            )
    # No match: return blanks instead of first row to avoid wrong contact
    return ("", "", "")

def build_ship_bill_blocks(db_path: str, company: str, username: str) -> Tuple[str, str]:
    """
    Ship To (for DOC ONLY):
      - Company: prefer addresses.Company (clean, no numeric prefix). Else fallback to prefix-stripped 'company' arg.
      - Address lines: addresses.Address (+ Address 2 if present)
      - City/State/Zip: use combined column if present (e.g., 'City,Sate,Zip', 'City,State,Zip', 'City_State_Zip');
        otherwise compose from City + State/ST + Zip/Postal.
      - Then append user contact (from user_contacts/User_Data) matched by username (if found).
    Bill To:
      - addresses.Billing (semicolon → lines) + BillingContact, BillingEmail, BillingPhone (if present).
    """
    adr = _load_table(db_path, "addresses")
    user_df, _ = _user_table(db_path)

    # ---- Bill To ----
    bill_lines: List[str] = []
    if not adr.empty:
        if "Billing" in adr.columns:
            rows_with = adr[adr["Billing"].astype(str).str.strip() != ""]
            row_b = rows_with.iloc[0] if not rows_with.empty else adr.iloc[0]
            billing_text = str(row_b.get("Billing","")).strip()
            if billing_text:
                bill_lines.extend(_split_semicolon_lines(billing_text))

            b_contact = str(row_b.get(_pick_first_col(adr, ["Billing Contact","BillingContact"]) or "", "")).strip()
            b_email   = str(row_b.get(_pick_first_col(adr, ["BillingEmail","Billing Email"]) or "", "")).strip()
            b_phone   = str(row_b.get(_pick_first_col(adr, ["BillingPhone","Billing Phone"]) or "", "")).strip()
            if b_contact: bill_lines.append(b_contact)
            if b_email:   bill_lines.append(b_email)
            if b_phone:   bill_lines.append(f"Phone: {b_phone}")

    bill_txt = "
".join([ln for ln in bill_lines if ln])

    # ---- Ship To ----
    ship_lines: List[str] = []
    arow = pd.Series(dtype="object")
    printed_company = remove_prefix_for_display(company)

    if not adr.empty:
        # Prefer matching by Location (since RE-STOCK 'Company' values are like '110 - Deckers Creek Limestone')
        loc_col = _pick_first_col(adr, ["Location"])
        comp_col = _pick_first_col(adr, ["Company"])
        # 1) Try exact Location match to the chosen string
        if loc_col and loc_col in adr.columns:
            mloc = adr[loc_col].astype(str).str.strip().str.casefold() == str(company).strip().casefold()
            if mloc.any():
                arow = adr[mloc].iloc[0]
        # 2) Else try Company match to prefix-stripped label
        if arow.empty and comp_col and comp_col in adr.columns:
            mcomp = adr[comp_col].astype(str).str.strip().str.casefold() == remove_prefix_for_display(company).strip().casefold()
            if mcomp.any():
                arow = adr[mcomp].iloc[0]
        # set printed company from addresses.Company if we have it
        if comp_col and comp_col in adr.columns and not arow.empty:
            printed_company = str(arow.get(comp_col, printed_company) or printed_company)

        if arow.empty:
            arow = adr.iloc[0]  # final fallback (should be rare)

    # Company name on first line (clean)
    ship_lines.append(str(printed_company).strip())

    # Address line 1 / 2
    for key in ["Address","Address 1","Address1","Street","Line1"]:
        if key in arow.index and str(arow.get(key, "")).strip():
            ship_lines.append(str(arow.get(key)).strip()); break
    for key in ["Address 2","Address2","Line2","Street 2"]:
        if key in arow.index and str(arow.get(key, "")).strip():
            ship_lines.append(str(arow.get(key)).strip()); break

    # City/State/Zip — support combined column used in your sheet ("City,Sate,Zip")
    combined_candidates = ["City,Sate,Zip","City,State,Zip","City_State_Zip","City State Zip","City, ST Zip"]
    combined_key = None
    for cand in combined_candidates:
        if cand in arow.index and str(arow.get(cand, "")).strip():
            combined_key = cand; break

    if combined_key:
        ship_lines.append(str(arow.get(combined_key)).strip())
    else:
        city  = str(arow.get(_pick_first_col(pd.DataFrame([arow]), ["City"]) or "", "")).strip()
        state = str(arow.get(_pick_first_col(pd.DataFrame([arow]), ["State","ST"]) or "", "")).strip()
        zipc  = str(arow.get(_pick_first_col(pd.DataFrame([arow]), ["Zip","ZIP","Postal","Postal Code"]) or "", "")).strip()
        if city or state or zipc:
            if city and (state or zipc):
                ship_lines.append(f"{city}, {_join_nonempty(state, zipc, sep=' ')}".strip(", "))
            else:
                ship_lines.append(_join_nonempty(city, state, zipc, sep=" "))

    # User contact by Username (no random fallback)
    c_name, c_email, c_phone = _match_user_contact(user_df, printed_company, username)
    if c_name:  ship_lines.append(c_name)
    if c_email: ship_lines.append(c_email)
    if c_phone: ship_lines.append(f"Phone: {c_phone}")

    ship_txt = "
".join([ln for ln in ship_lines if ln])
    return ship_txt, bill_txt

# ---------- Word helpers ----------
def _remove_table_borders(table) -> None:
    """
    Remove all borders from a python-docx table (safe, no AttributeError).
    """
    tbl = table._tbl
    tblPr = tbl.tblPr or tbl.get_or_add_tblPr()
    # Replace existing borders with 'no borders'
    for child in list(tblPr):
        if child.tag == qn('w:tblBorders'):
            tblPr.remove(child)
    borders = OxmlElement('w:tblBorders')
    for side in ['top','left','bottom','right','insideH','insideV']:
        el = OxmlElement(f'w:{side}')
        el.set(qn('w:val'), 'nil')
        el.set(qn('w:sz'), '0')
        el.set(qn('w:space'), '0')
        el.set(qn('w:color'), 'auto')
        borders.append(el)
    tblPr.append(borders)

def build_quote_docx(*, company: str, date_str: str, quote_number: str,
                     vendor_text: str, ship_to_text: str, bill_to_text: str,
                     lines_df: pd.DataFrame) -> bytes:
    """
    Build professional quote request:
      - Header with Company (prefix removed), title, date, quote#
      - Vendor
      - Side-by-side Ship To (left) and Bill To (right) with NO borders
      - Lines table with headers: Part Number, Description, Qty, Price, Total
        (data maps from Quantity and Price/Unit)
    """
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    # Header
    p = doc.add_paragraph()
    run = p.add_run(remove_prefix_for_display(company))
    run.bold = True
    run.font.size = Pt(14)

    title = doc.add_paragraph()
    run2 = title.add_run("Quote Request")
    run2.bold = True
    run2.font.size = Pt(16)

    doc.add_paragraph(date_str)
    doc.add_paragraph(f"Quote #: {quote_number}")

    # Vendor
    doc.add_paragraph("")
    vr = doc.add_paragraph(); vr.add_run("Vendor").bold = True
    doc.add_paragraph(vendor_text if vendor_text.strip() else "_____________________________")

    # Addresses table (2x2): header row + values row
    doc.add_paragraph("")
    tbl_addr = doc.add_table(rows=2, cols=2)
    hdr = tbl_addr.rows[0].cells
    hdr[0].text = "Ship To Address"
    hdr[1].text = "Bill To Address"
    vals = tbl_addr.rows[1].cells
    vals[0].text = ship_to_text
    vals[1].text = bill_to_text

    # No borders around addresses, per request
    _remove_table_borders(tbl_addr)

    # Lines
    doc.add_paragraph("")
    # Map data columns -> display headers
    data_cols   = ["Part Number","Description","Quantity","Price/Unit","Total"]
    headers_out = ["Part Number","Description","Qty","Price","Total"]
    lines = _coerce_lines_for_storage(lines_df).copy()
    # Build display table
    BLANK_ROWS = max(10, 30 - len(lines))
    total_rows = len(lines) + BLANK_ROWS
    tbl = doc.add_table(rows=1 + total_rows, cols=len(headers_out))
    tbl.style = 'Table Grid'
    # Set widths: PN ~ 1.7", Desc ~ 3.6", Qty ~ 0.9", Price ~ 1.2", Total ~ 1.2"
    widths = [Inches(1.7), Inches(3.6), Inches(0.9), Inches(1.2), Inches(1.2)]
    for j in range(len(headers_out)):
        for r in tbl.rows:
            r.cells[j].width = widths[j]

    # headers
    for j,h in enumerate(headers_out):
        tbl.cell(0,j).text = h
    # data rows
    for i,(_,r) in enumerate(lines.iterrows(), start=1):
        tbl.cell(i,0).text = str("" if pd.isna(r[data_cols[0]]) else r[data_cols[0]])
        tbl.cell(i,1).text = str("" if pd.isna(r[data_cols[1]]) else r[data_cols[1]])
        tbl.cell(i,2).text = str("" if pd.isna(r[data_cols[2]]) else r[data_cols[2]])
        tbl.cell(i,3).text = str("" if pd.isna(r[data_cols[3]]) else r[data_cols[3]])
        tbl.cell(i,4).text = str("" if pd.isna(r[data_cols[4]]) else r[data_cols[4]])
    # blanks
    for k in range(BLANK_ROWS):
        base = 1 + len(lines) + k
        for j in range(len(headers_out)):
            tbl.cell(base, j).text = ""

    doc.add_paragraph("")
    qtot = doc.add_paragraph(); run3 = qtot.add_run("Quote Total"); run3.bold = True

    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

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
    "restock": ["ID","id","Purchase Order ID"],
    "po_outstanding": ["ID","id","Purchase Order ID","Column2"],
}

def label_for_source(engine: str, path: Optional[str]) -> str:
    if engine == "parquet" and path:
        try:
            ts = Path(path).stat().st_mtime
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return f"Engine: Parquet • Updated: {dt.strftime('%Y-%m-%d %H:%M UTC')}"
        except Exception:
            return "Engine: Parquet"
    return "Engine: SQLite"

# ---------- App ----------
cfg = load_config(); cfg = to_plain(cfg)

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

    # Resolve DB path + expose in UI
    db_path = resolve_db_path(cfg)
    ACTIVE_DB_PATH = db_path
    pq_paths = detect_parquet_paths(cfg)

    # Sidebar info
    st.sidebar.caption(label_for_source(
        "parquet" if (pq_paths.get("restock") or pq_paths.get("po_outstanding")) else "sqlite",
        str(pq_paths.get("restock") or pq_paths.get("po_outstanding")) if (pq_paths.get("restock") or pq_paths.get("po_outstanding")) else None
    ))
    st.sidebar.caption(f"DB used for quotes: `{Path(db_path).resolve()}`")
    st.sidebar.caption(f"Quotes in DB: **{quotes_count(db_path)}**")
    if st.sidebar.button("🔄 Refresh data"): st.cache_data.clear()

    page = st.sidebar.radio("Page", ["RE-STOCK", "Outstanding POs", "Quotes"], index=0)

    # Loaders (for companies list)
    def load_src(src: str):
        pq_path = parquet_available_for(src, pq_paths)
        if pq_path:
            df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
            cols_in_db = list(df_all.columns)
            comp_col = "Company" if "Company" in df_all.columns else None
            all_companies = sorted({str(x) for x in df_all[comp_col].dropna().tolist()}) if comp_col else []
            return df_all, cols_in_db, all_companies, pq_path
        else:
            all_companies_df = q(f"SELECT DISTINCT [Company] FROM [{src}] WHERE [Company] IS NOT NULL ORDER BY 1")
            all_companies = [str(x) for x in all_companies_df['Company'].dropna().tolist()] or []
            cols_in_db = table_columns_in_order(None, src)
            return None, cols_in_db, all_companies, None

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
    if is_admin or star_granted: allowed_set = set(all_companies)
    else:
        matches = {db_map[n] for n in allowed_norm if n in db_map}
        allowed_set = matches if matches else set(allowed_cfg)
    if not allowed_set:
        st.error("No companies configured for your account. Ask an admin to update your access.")
        with st.expander("Company values present in data"): st.write(sorted(all_companies))
        st.stop()

    company_options = sorted(allowed_set)
    ADMIN_ALL = "« All companies (admin) »"
    select_options = ["— Choose company —"]
    if is_admin and len(all_companies) > 1: select_options += [ADMIN_ALL]
    select_options += company_options
    chosen = st.sidebar.selectbox("Choose your Company", options=select_options, index=0)
    if chosen == "— Choose company —":
        st.info("Select your Company on the left to load data."); st.stop()
    if is_admin and chosen == ADMIN_ALL:
        chosen_companies = sorted(all_companies); title_companies = "All companies (admin)"
    else:
        chosen_companies = [chosen]; title_companies = chosen

    # ----------------- RE-STOCK -----------------
    if page == "RE-STOCK":
        src = "restock"
        pq_path = parquet_available_for(src, pq_paths)
        vendor_col = None
        if pq_path:
            df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
            cols_in_db = list(df_all.columns)
            lower_map = {c.lower(): c for c in cols_in_db}
            if 'vendors' in lower_map: vendor_col = lower_map['vendors']
            elif 'vendor' in lower_map: vendor_col = lower_map['vendor']
            df = df_all.copy()
            if "Company" in df.columns:
                df = df[df["Company"].astype(str).isin([str(x) for x in chosen_companies])]
            label = 'Search Part Numbers / Name' + (' / Vendor' if vendor_col else '') + ' contains'
            search = st.sidebar.text_input(label)
            if search:
                s = str(search); cols = ["Part Numbers","Name"] + ([vendor_col] if vendor_col else [])
                ok = pd.Series(False, index=df.index)
                for c in cols:
                    if c in df.columns:
                        ok |= df[c].astype(str).str.contains(s, case=False, regex=False, na=False)
                df = df[ok]
            order_cols = [c for c in ["Company","Name"] if c in df.columns]
            df = df.sort_values(order_cols) if order_cols else df
        else:
            cols_in_db = table_columns_in_order(None, src)
            vendor_col = "Vendors" if "Vendors" in cols_in_db else ("Vendor" if "Vendor" in cols_in_db else None)
            label = 'Search Part Numbers / Name' + (' / Vendor' if vendor_col else '') + ' contains'
            search = st.sidebar.text_input(label)
            ph = ','.join(['?']*len(chosen_companies)); where = [f"[Company] IN ({ph})"]; params = list(chosen_companies)
            if search:
                if vendor_col:
                    where.append(f"([Part Numbers] LIKE ? OR [Name] LIKE ? OR [{vendor_col}] LIKE ?)")
                    params += [f"%{search}%"]*3
                else:
                    where.append("([Part Numbers] LIKE ? OR [Name] LIKE ?)")
                    params += [f"%{search}%"]*2
            where_sql = " AND ".join(where)
            sql = f"SELECT * FROM [{src}] WHERE {where_sql} ORDER BY [Company], [Name]"
            df = q(sql, tuple(params))

        df = strip_time(df, DATE_COLS.get(src, [])); df = attach_row_key(df)
        hide_set = set(HIDE_COLS.get(src, [])) | {"__KEY__","__QTY__"}
        cols_for_download = [c for c in df.columns if (c not in hide_set)]

        st.markdown(f"### RE-STOCK — {title_companies}")

        display_hide = {"Rsvd","Ord","Company","__KEY__","__QTY__"}
        display_cols = [c for c in cols_for_download if c not in display_hide]
        df_display = df[display_cols].copy()
        if "Select" not in df_display.columns: df_display.insert(0,"Select",False)

        grid_col_cfg = {"Select": st.column_config.CheckboxColumn("Add", help="Check to include in cart", default=False)}
        for c in df_display.columns:
            if c != "Select": grid_col_cfg[c] = st.column_config.Column(disabled=True)

        base_key = f"grid_{src}"
        if base_key not in st.session_state: st.session_state[base_key] = 0
        grid_key = f"{base_key}_{st.session_state[base_key]}"

        with st.form(f"{grid_key}_form", clear_on_submit=False):
            edited = st.data_editor(df_display, use_container_width=True, hide_index=True, column_config=grid_col_cfg, key=grid_key)
            # small buttons via narrow columns
            c_sp1, c_add, c_clear_sel, _ = st.columns([6,1,1,6])
            add_clicked = c_add.form_submit_button("🛒 Add", use_container_width=True)
            clear_sel_clicked = c_clear_sel.form_submit_button("🧹 Clear", use_container_width=True)

        try: selected_idx = edited.index[edited["Select"]==True]
        except Exception: selected_idx = []
        selected_rows = df.loc[selected_idx] if len(selected_idx) else df.iloc[0:0]

        cart_key = f"cart_{src}_{hashlib.md5(('|'.join([str(x) for x in chosen_companies])).encode()).hexdigest()}"
        if cart_key not in st.session_state:
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns)+["__QTY__"])

        if add_clicked and not selected_rows.empty:
            add_df = selected_rows.copy()
            # default qty = max(Min - InStk, 0)
            def compute_qty_min_minus_stock(lines: pd.DataFrame) -> pd.Series:
                min_col = next((c for c in ["Min","Minimum","Min Qty","Minimum Qty"] if c in lines.columns), None)
                stk_col = next((c for c in ["InStk","In Stock","On Hand","QOH","In_Stock"] if c in lines.columns), None)
                if not (min_col and stk_col): return pd.Series([""]*len(lines), index=lines.index, dtype="object")
                m = pd.to_numeric(lines[min_col], errors="coerce"); s = pd.to_numeric(lines[stk_col], errors="coerce")
                diff = (m - s).clip(lower=0); return diff.apply(lambda x: "" if pd.isna(x) else str(int(float(x))) if float(x).is_integer() else str(x))
            add_df["__QTY__"] = compute_qty_min_minus_stock(add_df)

            if vendor_col:
                sel_vendors = sorted(set(add_df[vendor_col].dropna().astype(str).str.strip()))
                cart_df = st.session_state[cart_key]
                cart_vendors = sorted(set(cart_df[vendor_col].dropna().astype(str).str.strip())) if (not cart_df.empty and vendor_col in cart_df.columns) else []
                if not cart_vendors:
                    if len(sel_vendors) > 1:
                        chosen_vendor = sel_vendors[0]
                        add_df = add_df[add_df[vendor_col].astype(str).str.strip()==chosen_vendor]
                        st.info(f"Cart is per-vendor. Added only Vendor '{chosen_vendor}'.")
                else:
                    chosen_vendor = cart_vendors[0]
                    before = len(add_df)
                    add_df = add_df[add_df[vendor_col].astype(str).str.strip()==chosen_vendor]
                    skipped = before - len(add_df)
                    if skipped>0: st.warning(f"Cart locked to Vendor '{chosen_vendor}'. Skipped {skipped} item(s).")
            merged = pd.concat([st.session_state[cart_key], add_df], ignore_index=True)
            st.session_state[cart_key] = merged.drop_duplicates(subset="__KEY__", keep="first").reset_index(drop=True)
            st.success(f"Added {len(add_df)} item(s) to cart."); st.rerun()

        if clear_sel_clicked:
            st.session_state[base_key] += 1; st.rerun()

        # Downloads (current view)
        excel_df = df_display.drop(columns=["Select"], errors="ignore").copy()
        excel_df.insert(0, "Inventory Check", "")
        c1, _, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(excel_df, sheet="RE_STOCK"),
                               file_name="RE_STOCK.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Cart area
        cart_df: pd.DataFrame = st.session_state[cart_key]
        st.markdown(f"#### Cart ({len(cart_df)} item{'s' if len(cart_df)!=1 else ''})")
        if cart_df.empty:
            cart_df = pd.DataFrame(columns=list(df.columns)+["__QTY__"]); st.session_state[cart_key] = cart_df
        else:
            if "__KEY__" not in cart_df.columns: cart_df = attach_row_key(cart_df)
            if "__QTY__" not in cart_df.columns: cart_df["__QTY__"] = ""
            st.session_state[cart_key] = cart_df

        pn = next((c for c in ["Part Number","Part Numbers","Part #","Part No","PN"] if c in cart_df.columns), None)
        nm = next((c for c in ["Name","Line Name","Description","Part Name","Item Name"] if c in cart_df.columns), None)
        vd = vendor_col

        cart_display = pd.DataFrame(index=cart_df.index)
        cart_display["Remove"] = False
        if pn: cart_display["Part Number"] = cart_df[pn]
        if nm: cart_display["Part Name"]   = cart_df[nm]
        if vd: cart_display["Vendor"]      = cart_df[vd]
        def to_num(x):
            try: xf=float(x); return int(xf) if xf.is_integer() else xf
            except Exception: return None if (x is None or (isinstance(x,str) and x.strip()=="")) else x
        cart_display["Qty"] = cart_df["__QTY__"].apply(to_num)

        cart_col_cfg = {
            "Remove": st.column_config.CheckboxColumn("Remove", help="Check to remove", default=False),
            "Qty": st.column_config.NumberColumn("Qty", help="Edit requested quantity", step=1, min_value=0),
        }

        cart_base = f"cart_{src}_editor"
        if cart_base not in st.session_state: st.session_state[cart_base] = 0
        cart_editor_key = f"{cart_base}_{st.session_state[cart_base]}"

        with st.form(f"{cart_editor_key}_form", clear_on_submit=False):
            edited_cart = st.data_editor(cart_display, use_container_width=True, hide_index=True,
                                         column_config=cart_col_cfg, key=cart_editor_key)
            # Buttons row: Remove (left small) | Clear, Save, Generate, Email (right small)
            left, right = st.columns([6,4])
            with left:
                rcol, _ = st.columns([1,7])
                remove_btn = rcol.form_submit_button("🗑️ Remove", use_container_width=True)
            with right:
                c_clear, c_save, c_gen, c_email = st.columns([1,1,1,1])
                clear_cart_btn = c_clear.form_submit_button("🧼 Clear", use_container_width=True, disabled=cart_df.empty)
                save_qty = c_save.form_submit_button("💾 Save", use_container_width=True)
                gen_clicked = c_gen.form_submit_button("🧾 Generate", use_container_width=True)
                _ = c_email.form_submit_button("✉️ Email", use_container_width=True, disabled=True)

        if save_qty and "Qty" in edited_cart.columns:
            def norm_q(v):
                if v is None: return ""
                if isinstance(v,(int,float)): return str(int(v)) if float(v).is_integer() else str(v)
                return str(v)
            st.session_state[cart_key].loc[edited_cart.index,"__QTY__"] = edited_cart["Qty"].apply(norm_q).values
            st.success("Saved quantities.")

        if remove_btn:
            try: to_remove_idx = edited_cart.index[edited_cart["Remove"]==True]
            except Exception: to_remove_idx = []
            if len(to_remove_idx):
                keys_to_remove = st.session_state[cart_key].loc[to_remove_idx,"__KEY__"].tolist()
                st.session_state[cart_key] = st.session_state[cart_key].loc[
                    ~st.session_state[cart_key]["__KEY__"].isin(keys_to_remove)
                ].reset_index(drop=True)
                st.session_state[cart_base] += 1; st.rerun()

        if clear_cart_btn:
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns)+["__QTY__"])
            st.session_state[cart_base] += 1; st.rerun()

        # Generate: save quote, then present download
        if gen_clicked:
            if st.session_state[cart_key].empty:
                st.warning("Cart is empty.")
            else:
                # Vendor text (single vendor preferred)
                vendor_text = "_____________________________"
                if vendor_col and vendor_col in st.session_state[cart_key].columns:
                    vendors = sorted(set(st.session_state[cart_key][vendor_col].dropna().astype(str).str.strip()))
                    if len(vendors) == 1:
                        vendor_text = vendors[0]
                    elif len(vendors) > 1:
                        st.error("Cart has multiple vendors. Keep only one before generating.")
                        st.stop()

                pncol = pn; desc = nm
                lines_df = pd.DataFrame({
                    "Part Number": st.session_state[cart_key][pncol].astype(str) if pncol else "",
                    "Description": st.session_state[cart_key][desc].astype(str) if desc else "",
                    "Quantity":    st.session_state[cart_key]["__QTY__"].astype(str),
                    "Price/Unit":  "",
                    "Total":       ""
                })

                company_for_save = chosen if chosen != ADMIN_ALL else "All companies (admin)"
                ship_to, bill_to = build_ship_bill_blocks(ACTIVE_DB_PATH, company_for_save, str(username))

                next_no = _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow())
                qid, qnum = save_quote(
                    ACTIVE_DB_PATH,
                    quote_number=next_no,
                    company=company_for_save,
                    created_by=str(username),
                    vendor=vendor_text,
                    ship_to=ship_to,
                    bill_to=bill_to,
                    source="restock",
                    lines_df=lines_df
                )
                st.success(f"Saved Quote ID {qid} ({qnum})")

                doc_bytes = build_quote_docx(
                    company=company_for_save,
                    date_str=datetime.now().strftime("%Y-%m-%d"),
                    quote_number=qnum,
                    vendor_text=vendor_text,
                    ship_to_text=ship_to,
                    bill_to_text=bill_to,
                    lines_df=lines_df
                )
                st.download_button(
                    "Download Quote (Word)",
                    data=doc_bytes,
                    file_name=f"{qnum}_{sanitize_filename(company_for_save)}.docx",
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                )

    # ----------------- Outstanding POs -----------------
    elif page == "Outstanding POs":
        src = "po_outstanding"
        pq_path = parquet_available_for(src, pq_paths)
        if pq_path:
            df_all = read_parquet_cached(str(pq_path), _filesig(pq_path))
            df = df_all.copy()
            if "Company" in df.columns:
                df = df[df["Company"].astype(str).isin([str(x) for x in chosen_companies])]
            search = st.sidebar.text_input('Search PO # / Vendor / Part / Line Name contains')
            if search:
                s = str(search); cols = ["Purchase Order #","Vendor","Part Number","Line Name"]
                ok = pd.Series(False, index=df.index)
                for c in cols:
                    if c in df.columns:
                        ok |= df[c].astype(str).str.contains(s, case=False, regex=False, na=False)
                df = df[ok]
            if "Created On" in df.columns:
                co = pd.to_datetime(df["Created On"], errors="coerce")
                if {"Company","Purchase Order #"} <= set(df.columns):
                    df = df.assign(_co=co).sort_values(["Company","_co","Purchase Order #"]).drop(columns=["_co"])
                else:
                    df = df.assign(_co=co).sort_values(["_co"]).drop(columns=["_co"])
        else:
            search = st.sidebar.text_input('Search PO # / Vendor / Part / Line Name contains')
            ph = ','.join(['?']*len(chosen_companies)); where = [f"[Company] IN ({ph})"]; params = list(chosen_companies)
            if search:
                where.append("([Purchase Order #] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Line Name] LIKE ?)")
                params += [f"%{search}%"]*4
            where_sql = " AND ".join(where)
            sql = f"SELECT * FROM [{src}] WHERE {where_sql} ORDER BY [Company], date([Created On]) ASC, [Purchase Order #]"
            df = q(sql, tuple(params))
        df = strip_time(df, DATE_COLS.get(src, [])); df = attach_row_key(df)
        hide_set = set(HIDE_COLS.get(src, [])) | {"__KEY__","__QTY__"}
        cols_for_download = [c for c in table_columns_in_order(None, src) if (c in df.columns) and (c not in hide_set)]
        display_cols = [c for c in cols_for_download if c != "Company"]
        st.markdown(f"### Outstanding POs — {title_companies}")
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
        c1, _, _ = st.columns([1,1,6])
        with c1:
            st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(df[display_cols], sheet="Outstanding_POs"),
                               file_name="Outstanding_POs.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ----------------- Quotes (New / Browse-Edit) -----------------
    else:
        st.markdown(f"### Quotes — {title_companies}")
        ensure_quotes_table(ACTIVE_DB_PATH)

        include_all = is_admin  # admins default to seeing all
        if is_admin:
            include_all = st.toggle("Show all companies", value=True)

        tab_new, tab_browse = st.tabs(["🆕 New Quote", "📁 Browse / Edit"])

        # Load Vendors list if present
        vendors_df = _load_table(ACTIVE_DB_PATH, "vendors")
        vendor_names = sorted(vendors_df["Vendor"].dropna().astype(str).unique().tolist()) if ("Vendor" in vendors_df.columns and not vendors_df.empty) else []

        # NEW QUOTE
        with tab_new:
            # Let user choose Location (Company) for this new quote
            try_default_idx = company_options.index(chosen) if chosen in company_options else 0
            company_new = st.selectbox("Location", options=company_options, index=try_default_idx)
            # Choose vendor
            if vendor_names:
                vendor = st.selectbox("Vendor", options=[""] + vendor_names, index=0)
            else:
                vendor = st.text_input("Vendor", value="")

            if "new_quote_no" not in st.session_state:
                st.session_state.new_quote_no = _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow())
            quote_no = st.text_input("Quote #", value=st.session_state.new_quote_no, help="QR-YYYY-####")

            # Prefill addresses from DB for selected company
            ship_to, bill_to = build_ship_bill_blocks(ACTIVE_DB_PATH, company_new, str(username))
            c1, c2 = st.columns(2)
            with c1:
                ship_to = st.text_area("Ship To Address", value=ship_to, height=120)
            with c2:
                bill_to = st.text_area("Bill To Address", value=bill_to, height=120)

            initial_rows = [{"Part Number":"", "Description":"","Quantity":"","Price/Unit":"","Total":""} for _ in range(15)]
            if "new_quote_rows" not in st.session_state:
                st.session_state.new_quote_rows = pd.DataFrame(initial_rows)
            edited_new = st.data_editor(
                st.session_state.new_quote_rows,
                key="new_quote_editor",
                hide_index=True, use_container_width=True,
                column_config={
                    "Part Number": st.column_config.TextColumn("Part Number"),
                    "Description": st.column_config.TextColumn("Description"),
                    "Quantity":    st.column_config.NumberColumn("Quantity", step=1, min_value=0),
                    "Price/Unit":  st.column_config.TextColumn("Price/Unit"),
                    "Total":       st.column_config.TextColumn("Total"),
                }
            )

            c_left, c_sp, c_save, c_gen, c_email = st.columns([4,5,1,1,1])
            with c_save:
                if st.button("Save", use_container_width=True):
                    qid, qnum = save_quote(ACTIVE_DB_PATH,
                                           quote_number=quote_no or None,
                                           company=company_new,
                                           created_by=str(username),
                                           vendor=vendor, ship_to=ship_to, bill_to=bill_to, source="manual",
                                           lines_df=edited_new)
                    st.success(f"Saved quote #{qid} ({qnum})")
                    st.session_state.new_quote_no = _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow())
            with c_gen:
                if st.button("Generate", use_container_width=True):
                    qid, qnum = save_quote(ACTIVE_DB_PATH,
                                           quote_number=quote_no or None,
                                           company=company_new,
                                           created_by=str(username),
                                           vendor=vendor, ship_to=ship_to, bill_to=bill_to, source="manual",
                                           lines_df=edited_new)
                    st.success(f"Saved quote #{qid} ({qnum})")
                    doc_bytes = build_quote_docx(
                        company=company_new,
                        date_str=datetime.now().strftime("%Y-%m-%d"),
                        quote_number=qnum,
                        vendor_text=vendor, ship_to_text=ship_to, bill_to_text=bill_to,
                        lines_df=edited_new
                    )
                    st.download_button("Download Quote (Word)", data=doc_bytes,
                                       file_name=f"{qnum}_{sanitize_filename(company_new)}.docx",
                                       mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            with c_email:
                st.button("Email", use_container_width=True, disabled=True)

        # BROWSE / EDIT
        with tab_browse:
            # Filter by company (keeps DB prefix names)
            comp_filter = st.selectbox("Filter by Company", options=["(all)"] + company_options, index=0)
            dfq = list_quotes(ACTIVE_DB_PATH,
                              company=(None if comp_filter=="(all)" else comp_filter),
                              include_all=(include_all or comp_filter=="(all)"))
            if dfq.empty:
                st.info("No saved quotes yet.")
            else:
                st.dataframe(dfq, hide_index=True, use_container_width=True)
                qid = st.number_input("Quote ID to open",
                                      min_value=int(dfq["id"].min()),
                                      max_value=int(dfq["id"].max()),
                                      value=int(dfq["id"].max()), step=1)
                rec = load_quote(ACTIVE_DB_PATH, int(qid))
                if not rec:
                    st.warning("Quote not found.")
                else:
                    quote_no = st.text_input("Quote #", value=rec["quote_number"] or _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow()))
                    vendor   = st.text_input("Vendor", value=rec["vendor"] or "")
                    c1, c2 = st.columns(2)
                    with c1:
                        ship_to = st.text_area("Ship To Address", value=rec["ship_to"] or "", height=120)
                    with c2:
                        bill_to = st.text_area("Bill To Address", value=rec["bill_to"] or "", height=120)

                    edited_exist = st.data_editor(
                        rec["lines"],
                        key=f"edit_quote_{rec['id']}",
                        hide_index=True, use_container_width=True,
                        column_config={
                            "Part Number": st.column_config.TextColumn("Part Number"),
                            "Description": st.column_config.TextColumn("Description"),
                            "Quantity":    st.column_config.NumberColumn("Quantity", step=1, min_value=0),
                            "Price/Unit":  st.column_config.TextColumn("Price/Unit"),
                            "Total":       st.column_config.TextColumn("Total"),
                        }
                    )

                    c_left, c_sp, c_save, c_gen, c_email = st.columns([4,5,1,1,1])
                    with c_save:
                        if st.button("Save", key=f"save_quote_{rec['id']}", use_container_width=True):
                            save_quote(ACTIVE_DB_PATH, quote_number=quote_no or None,
                                       company=rec["company"],
                                       created_by=str(username),
                                       vendor=vendor, ship_to=ship_to, bill_to=bill_to, source=rec["source"],
                                       lines_df=edited_exist, quote_id=int(rec["id"]))
                            st.success("Saved")
                    with c_gen:
                        if st.button("Generate", key=f"gen_btn_{rec['id']}", use_container_width=True):
                            qid2, qnum2 = save_quote(ACTIVE_DB_PATH, quote_number=quote_no or None,
                                                     company=rec["company"],
                                                     created_by=str(username),
                                                     vendor=vendor, ship_to=ship_to, bill_to=bill_to, source=rec["source"],
                                                     lines_df=edited_exist, quote_id=int(rec["id"]))
                            st.success(f"Saved quote #{qid2} ({qnum2})")
                            doc_bytes = build_quote_docx(
                                company=rec["company"],
                                date_str=(rec["quote_date"] or datetime.now().strftime("%Y-%m-%d")),
                                quote_number=qnum2,
                                vendor_text=vendor, ship_to_text=ship_to, bill_to_text=bill_to,
                                lines_df=edited_exist
                            )
                            st.download_button("Download Quote (Word)", data=doc_bytes,
                                               file_name=f"{qnum2}_{sanitize_filename(rec['company'])}.docx",
                                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                               key=f"gen_dl_{rec['id']}")

    # ---------- Config template (admins only) ----------
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')
