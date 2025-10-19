# app_spf_portal.py  — RESTOCK, Outstanding POs, Quotes
# --------------------------------------------------------------
# - Restock "Generate": SAVE to DB then show Word download
# - Ship-To contact in Word = current user's entry from user_contacts (UserName match)
# - Company prefix like "110 -" is removed only in the Word document
# - Buttons: smaller; in cart row → Remove, Clear, Save, Generate (all on right)
#
# Requires tables in maintainx_po.db:
#   restock, po_outstanding, addresses, user_contacts
#   quotes (auto-created here if missing)

from __future__ import annotations
import os, io, re, json, sqlite3, textwrap, hashlib
from pathlib import Path
from collections.abc import Mapping, Iterable
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import yaml

# deps
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed")
    st.stop()
try:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
except Exception:
    st.error("python-docx not installed")
    st.stop()

st.set_page_config(page_title="SPF PO Portal", page_icon="📦", layout="wide")

APP_VERSION = "2025.10.19-fix"
DEFAULT_DB = "maintainx_po.db"
HERE = Path(__file__).resolve().parent
ACTIVE_DB_PATH: str | None = None

# ---------- config ----------
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
"""

def to_plain(obj):
    if isinstance(obj, Mapping): return {k: to_plain(v) for k,v in obj.items()}
    if isinstance(obj, (list,tuple)): return [to_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    if "app_config" in st.secrets: return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:
        try: return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML: {e}"); return {}
    cfg = HERE / "app_config.yaml"
    if cfg.exists():
        try: return yaml.safe_load(cfg.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}"); return {}
    return yaml.safe_load(CONFIG_TEMPLATE_YAML)

def resolve_db_path(cfg: dict) -> str:
    yaml_db = (cfg or {}).get("settings", {}).get("db_path")
    if yaml_db: return yaml_db
    env_db = os.environ.get("SPF_DB_PATH")
    if env_db: return env_db
    return DEFAULT_DB

# ---------- DB helpers ----------
def _db_sig(path: str) -> int:
    try: return Path(path).stat().st_mtime_ns
    except Exception: return 0

@st.cache_data(show_spinner=False)
def q_cached(sql: str, params: tuple, db_path: str, sig: int) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or ACTIVE_DB_PATH or DEFAULT_DB
    return q_cached(sql, tuple(params), path, _db_sig(path))

@st.cache_data(show_spinner=False)
def table_columns_in_order_cached(db_path: str, table: str, sig: int) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def table_columns_in_order(db_path: str | None, table: str) -> list[str]:
    p = db_path or ACTIVE_DB_PATH or DEFAULT_DB
    return table_columns_in_order_cached(p, table, _db_sig(p))

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
        )""")
        conn.commit()

def quotes_count(db_path: str) -> int:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0])

def _parse_year_and_seq(qn: str) -> tuple[Optional[int], Optional[int]]:
    try:
        p = qn.split("-")
        if len(p)!=3 or not p[0].upper().startswith("QR"): return (None,None)
        return (int(p[1]), int(p[2]))
    except Exception:
        return (None,None)

def _next_quote_number(db_path: str, when: datetime) -> str:
    yr = when.strftime("%Y")
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT quote_number FROM quotes WHERE quote_number LIKE ?", (f"QR-{yr}-%",)).fetchall()
    used = set()
    for (qn,) in rows:
        y,s = _parse_year_and_seq(qn or "")
        if y and s and str(y)==yr: used.add(s)
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
               lines_df: pd.DataFrame, status: str = "draft", quote_id: Optional[int] = None) -> tuple[int,str]:
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
              VALUES(?, ?, ?, ?, ?, ?, date('now'), ?, ?, ?, ?)
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
                  status, source, payload, now, int(quote_id)))
            conn.commit()
            return int(quote_id), quote_number

def list_quotes(db_path: str, company: Optional[str]=None, include_all: bool=False) -> pd.DataFrame:
    ensure_quotes_table(db_path)
    with sqlite3.connect(db_path) as conn:
        if company and not include_all:
            sql = ("SELECT id, quote_number, quote_date, vendor, status, source, company,"
                   " length(lines_json) AS bytes FROM quotes WHERE company=? ORDER BY id DESC")
            return pd.read_sql_query(sql, conn, params=(company,))
        return pd.read_sql_query(
            "SELECT id, quote_number, quote_date, vendor, status, source, company,"
            " length(lines_json) AS bytes FROM quotes ORDER BY id DESC", conn)

# ---------- utility ----------
def attach_row_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    key_col = next((c for c in ["ID","id","Purchase Order ID","Row ID","RowID"] if c in df.columns), None)
    if key_col:
        df["__KEY__"] = df[key_col].astype(str); return df
    cols = [c for c in ["Part Number","Part Numbers","Part #","Part No","PN","Name","Line Name","Description",
                        "Vendor","Vendors","Company","Created On"] if c in df.columns]
    if not cols: cols = list(df.columns)
    s = df[cols].astype(str).agg("|".join, axis=1)
    df["__KEY__"] = s.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df

def strip_time(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            ts = pd.to_datetime(df[c], errors="coerce")
            df[c] = ts.dt.strftime("%Y-%m-%d").where(~ts.isna(), df[c])
    return df

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name=sheet)
        ws = xw.sheets[sheet]
        ws.autofilter(0,0, max(0,len(df)), max(0, len(df.columns)-1))
        for i,_ in enumerate(df.columns):
            ws.set_column(i,i, 16 if df.empty else min(60, max(12, int(df.iloc[:,i].astype(str).str.len().quantile(0.9))+2)))
    bio.seek(0); return bio.getvalue()

def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._ -]+","_", str(name or "")).strip()[:80] or "file"

# ---------- addresses & contacts ----------
@st.cache_data(show_spinner=False)
def _load_table(db_path: str, name: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql_query(f"SELECT * FROM [{name}]", conn)
    except Exception:
        return pd.DataFrame()

def _find_col(df: pd.DataFrame, candidates: Iterable[str], contains_ok: bool=True) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    if contains_ok:
        for want in candidates:
            w = want.lower()
            for lc,orig in low.items():
                if w in lc: return orig
    return None

def _display_company(name: str) -> str:
    # Remove leading "<digits> - " only for the document
    return re.sub(r"^\s*\d+\s*-\s*","", str(name or "")).strip()

def _unique_append(lines: list[str], text: str):
    t = str(text or "").strip()
    if t and (not lines or lines[-1] != t):
        lines.append(t)

def fetch_user_contact(db_path: str, username: str) -> dict:
    uc = _load_table(db_path, "user_contacts")
    if uc.empty: return {}
    ucol = _find_col(uc, ["UserName","User","Login","Username"])
    ccol = _find_col(uc, ["Contact","Name"])
    ph   = _find_col(uc, ["Phone","Phone1","Phone_1"])
    mail = _find_col(uc, ["Email","E-mail"])
    row = None
    if ucol:
        m = uc[uc[ucol].astype(str).str.casefold() == str(username).casefold()]
        if not m.empty: row = m.iloc[0]
    if row is None: row = uc.iloc[0]
    return {
        "contact": str(row.get(ccol,"")).strip(),
        "phone":   str(row.get(ph,"")).strip(),
        "email":   str(row.get(mail,"")).strip(),
    }

def build_ship_bill_blocks(db_path: str, company_code: str, username: str) -> tuple[str,str,str]:
    adr = _load_table(db_path, "addresses")

    # Bill To (Greer… shared; columns Billing, BillingPhone, BillingEmail, Billing Contact)
    bill_name = "Greer Industries, Inc"
    bcol = _find_col(adr, ["Billing"])
    bph  = _find_col(adr, ["BillingPhone","Billing Phone"])
    bml  = _find_col(adr, ["BillingEmail","Billing E-mail","Billing Mail"])
    bct  = _find_col(adr, ["Billing Contact","BillingContact","AP Contact","Accounts Payable"])
    bill_lines: list[str] = [bill_name]
    if bcol and not adr.empty:
        # pick first non-empty billing row
        val = adr[bcol].dropna().astype(str).str.strip()
        if val.any():
            first = val[val != ""].iloc[0]
            parts = [p.strip() for p in first.split(";") if p.strip()]
            for p in parts: _unique_append(bill_lines, p)
    # contact line then email/phone
    if bct and not adr.empty:
        ct = adr[bct].dropna().astype(str).str.strip()
        if ct.any(): _unique_append(bill_lines, ct.iloc[0])
    if bph and not adr.empty:
        ph = adr[bph].dropna().astype(str).str.strip()
        if ph.any(): _unique_append(bill_lines, f"Phone: {ph.iloc[0]}")
    if bml and not adr.empty:
        ml = adr[bml].dropna().astype(str).str.strip()
        if ml.any(): _unique_append(bill_lines, ml.iloc[0])
    bill_to = "\n".join(bill_lines)

    # Ship To (by company row)
    comp_col = _find_col(adr, ["Company","Location","Site","Name"])
    row = None
    if comp_col and not adr.empty:
        m = adr[adr[comp_col].astype(str).str.strip().str.casefold()
               == str(company_code).strip().casefold()]
        if not m.empty: row = m.iloc[0]
    if row is None:
        row = adr.iloc[0] if not adr.empty else pd.Series(dtype="object")

    ship_lines: list[str] = [_display_company(company_code)]
    addr = str(row.get(_find_col(adr, ["Address","Addr","Street","Address 1","Address1"]) or "", "")).strip()
    cszc = _find_col(adr, ["City,Sta,Zip","City, State, Zip","City,State,Zip","City/State/Zip","City State Zip","City_State_Zip"])
    csz  = str(row.get(cszc,"")).strip()
    if addr: _unique_append(ship_lines, addr)
    if csz:  _unique_append(ship_lines, csz)

    # user contact (from user_contacts by UserName)
    u = fetch_user_contact(db_path, username)
    if u.get("contact"): _unique_append(ship_lines, u["contact"])
    # only one Phone: … line
    if u.get("phone"):   _unique_append(ship_lines, f"Phone: {u['phone']}")
    if u.get("email"):   _unique_append(ship_lines, u["email"])

    return "\n".join(ship_lines), bill_to, _display_company(company_code)

# ---------- Word build ----------
def _remove_table_borders_safe(tbl):
    # Make borders 'nil' even if tblPr/tblBorders not present
    el = tbl._tbl  # CT_Tbl
    tblPr = el.tblPr
    if tblPr is None:
        tblPr = OxmlElement('w:tblPr')
        el.insert(0, tblPr)
    borders = tblPr.find(qn('w:tblBorders'))
    if borders is None:
        borders = OxmlElement('w:tblBorders')
        tblPr.append(borders)
    for edge in ("top","left","bottom","right","insideH","insideV"):
        tag = qn(f"w:{edge}")
        node = borders.find(tag)
        if node is None:
            node = OxmlElement(f"w:{edge}")
            borders.append(node)
        node.set(qn('w:val'), 'nil')
        node.set(qn('w:sz'), '0')
        node.set(qn('w:space'), '0')
        node.set(qn('w:color'), 'auto')

def build_quote_docx(*, company_display: str, date_str: str, quote_number: str,
                     vendor_text: str, ship_to_text: str, bill_to_text: str,
                     lines_df: pd.DataFrame) -> bytes:
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    # Header
    p = doc.add_paragraph()
    run = p.add_run(company_display); run.bold = True; run.font.size = Pt(14)
    title = doc.add_paragraph()
    t = title.add_run("Quote Request"); t.bold = True; t.font.size = Pt(16)
    doc.add_paragraph(date_str)
    doc.add_paragraph(f"Quote #: {quote_number}")

    # Vendor
    doc.add_paragraph("")
    vr = doc.add_paragraph(); vr.add_run("Vendor").bold = True
    doc.add_paragraph(vendor_text if vendor_text.strip() else "_____________________________")

    # Addresses (side-by-side, no borders)
    doc.add_paragraph("")
    tbl_addr = doc.add_table(rows=2, cols=2)
    hdr = tbl_addr.rows[0].cells
    hdr[0].paragraphs[0].add_run("Ship To Address").bold = True
    hdr[1].paragraphs[0].add_run("Bill To Address").bold = True
    tbl_addr.rows[1].cells[0].text = ship_to_text
    tbl_addr.rows[1].cells[1].text = bill_to_text
    _remove_table_borders_safe(tbl_addr)

    # Lines
    doc.add_paragraph("")
    cols = ["Part Number","Description","Quantity","Price/Unit","Total"]
    lines = _coerce_lines_for_storage(lines_df).copy()
    blanks = max(10, 30 - len(lines))
    if blanks>0:
        lines = pd.concat([lines, pd.DataFrame([dict(zip(cols, [""]*5)) for _ in range(blanks)])], ignore_index=True)

    tbl = doc.add_table(rows=1+len(lines), cols=len(cols))
    hdr = tbl.rows[0].cells
    for j,c in enumerate(cols): hdr[j].text = c
    # set column widths
    widths = [Inches(1.3), Inches(4.5), Inches(0.8), Inches(1.1), Inches(1.1)]
    for j, w in enumerate(widths):
        for r in tbl.rows:
            r.cells[j].width = w
    for i,(_,r) in enumerate(lines.iterrows(), start=1):
        tbl.cell(i,0).text = str(r["Part Number"])
        tbl.cell(i,1).text = str(r["Description"])
        tbl.cell(i,2).text = str(r["Quantity"])
        tbl.cell(i,3).text = str(r["Price/Unit"])
        tbl.cell(i,4).text = str(r["Total"])

    doc.add_paragraph("")
    qt = doc.add_paragraph(); qt.add_run("Quote Total").bold = True

    bio = io.BytesIO(); doc.save(bio); bio.seek(0); return bio.getvalue()

# ---------- App ----------
cfg = load_config()
cookie_cfg = cfg.get("cookie", {})
auth = stauth.Authenticate(
    cfg.get("credentials", {}),
    cookie_cfg.get('name', 'spf_po_portal_v3'),
    cookie_cfg.get('key',  'super_secret_key_v3'),
    cookie_cfg.get('expiry_days', 7),
)
name, auth_status, username = auth.login("Login", "main")

if auth_status is False:
    st.error("Username/password is incorrect")
elif auth_status is None:
    st.info("Please log in.")
else:
    auth.logout("Logout", "sidebar")
    st.sidebar.success(f"Logged in as {name}")

    # DB path
    db_path = resolve_db_path(cfg)
    ACTIVE_DB_PATH = db_path
    ensure_quotes_table(db_path)

    # sidebar info
    st.sidebar.caption(f"DB: `{Path(db_path).resolve()}`")
    st.sidebar.caption(f"Quotes in DB: **{quotes_count(db_path)}**")
    if st.sidebar.button("🔄 Refresh"): st.cache_data.clear()

    # Which page?
    page = st.sidebar.radio("Page", ["RE-STOCK", "Outstanding POs", "Quotes"], index=0)

    # Companies list from restock (for filter)
    all_companies_df = q("SELECT DISTINCT [Company] FROM [restock] WHERE [Company] IS NOT NULL ORDER BY 1")
    all_companies = [str(x) for x in all_companies_df["Company"].dropna().tolist()]
    if not all_companies:
        st.error("No companies found in DB."); st.stop()

    chosen = st.sidebar.selectbox("Choose your Company", ["— Choose company —"] + all_companies, index=0)
    if chosen == "— Choose company —":
        st.info("Select your Company on the left to load data.")
        st.stop()

    # ---------- RE-STOCK ----------
    if page == "RE-STOCK":
        # Load
        search = st.sidebar.text_input("Search Part Numbers / Name / Vendor contains")
        where = ["[Company] = ?"]; params = [chosen]
        if search:
            where.append("([Part Numbers] LIKE ? OR [Name] LIKE ? OR [Vendors] LIKE ?)")
            like = f"%{search}%"; params += [like, like, like]
        sql = f"SELECT * FROM [restock] WHERE {' AND '.join(where)} ORDER BY [Name]"
        df = q(sql, tuple(params))
        df = strip_time(df, ["Created On","Approved On","Completed On","Posting Date"])
        df = attach_row_key(df)

        # display
        display_cols = [c for c in df.columns if c not in {"__KEY__","__QTY__","Company","Rsvd","Ord"}]
        df_disp = df[display_cols].copy()
        if "Select" not in df_disp.columns: df_disp.insert(0,"Select",False)

        cfg_cols = {"Select": st.column_config.CheckboxColumn("Add", help="Check to include in cart", default=False)}
        for c in df_disp.columns:
            if c!="Select": cfg_cols[c] = st.column_config.Column(disabled=True)

        base_key = "grid_restock"
        if base_key not in st.session_state: st.session_state[base_key] = 0
        grid_key = f"{base_key}_{st.session_state[base_key]}"

        with st.form(f"{grid_key}_form", clear_on_submit=False):
            edited = st.data_editor(df_disp, use_container_width=True, hide_index=True,
                                    column_config=cfg_cols, key=grid_key)
            c_add, c_clear = st.columns([1,1])
            add_clicked   = c_add.form_submit_button("🛒 Add selected to cart")
            clear_clicked = c_clear.form_submit_button("🧹 Clear selections")

        sel_idx = edited.index[edited["Select"] == True] if "Select" in edited.columns else []
        selected = df.loc[sel_idx] if len(sel_idx) else df.iloc[0:0]

        cart_key = f"cart_{hashlib.md5(chosen.encode()).hexdigest()}"
        if cart_key not in st.session_state:
            st.session_state[cart_key] = pd.DataFrame(columns=list(df.columns)+["__QTY__"])

        if add_clicked and not selected.empty:
            # default qty = max(Min - InStk, 0)
            def def_qty(lines: pd.DataFrame) -> pd.Series:
                min_col = next((c for c in ["Min","Minimum","Min Qty","Minimum Qty"] if c in lines.columns), None)
                stk_col = next((c for c in ["InStk","In Stock","On Hand","QOH","In_Stock"] if c in lines.columns), None)
                if not (min_col and stk_col): return pd.Series([""]*len(lines), index=lines.index, dtype="object")
                m = pd.to_numeric(lines[min_col], errors="coerce")
                s = pd.to_numeric(lines[stk_col], errors="coerce")
                diff = (m-s).clip(lower=0)
                return diff.apply(lambda x: "" if pd.isna(x) else str(int(float(x))) if float(x).is_integer() else str(x))
            add_df = selected.copy()
            add_df["__QTY__"] = def_qty(add_df)

            # single vendor
            vcol = "Vendors" if "Vendors" in add_df.columns else ("Vendor" if "Vendor" in add_df.columns else None)
            if vcol:
                cart = st.session_state[cart_key]
                cart_vendors = sorted(set(cart[vcol].dropna().astype(str).str.strip())) if (not cart.empty and vcol in cart.columns) else []
                sel_vendors = sorted(set(add_df[vcol].dropna().astype(str).str.strip()))
                if not cart_vendors:
                    if len(sel_vendors) > 1:
                        keep = sel_vendors[0]
                        add_df = add_df[add_df[vcol].astype(str).str.strip()==keep]
                        st.info(f"Cart is per-vendor. Added only '{keep}'.")
                else:
                    keep = cart_vendors[0]
                    before = len(add_df)
                    add_df = add_df[add_df[vcol].astype(str).str.strip()==keep]
                    skipped = before - len(add_df)
                    if skipped>0: st.warning(f"Cart locked to '{keep}'. Skipped {skipped} item(s).")

            merged = pd.concat([st.session_state[cart_key], add_df], ignore_index=True)
            st.session_state[cart_key] = merged.drop_duplicates(subset="__KEY__", keep="first").reset_index(drop=True)
            st.success(f"Added {len(add_df)} item(s)."); st.rerun()

        if clear_clicked:
            st.session_state[base_key] += 1; st.rerun()

        # Download current view
        out_xlsx = df_disp.drop(columns=["Select"], errors="ignore").copy()
        out_xlsx.insert(0, "Inventory Check", "")
        st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(out_xlsx, "RE_STOCK"),
                           file_name="RE_STOCK.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # ---- Cart ----
        st.markdown(f"#### Cart ({len(st.session_state[cart_key])} items)")
        cart_df = st.session_state[cart_key]
        if cart_df.empty:
            st.info("Cart is empty.")
        else:
            pn = next((c for c in ["Part Number","Part Numbers","Part #","Part No","PN"] if c in cart_df.columns), None)
            nm = next((c for c in ["Name","Line Name","Description","Part Name","Item Name"] if c in cart_df.columns), None)
            vcol = "Vendors" if "Vendors" in cart_df.columns else ("Vendor" if "Vendor" in cart_df.columns else None)

            disp = pd.DataFrame(index=cart_df.index)
            disp["Remove"] = False
            if pn: disp["Part Number"] = cart_df[pn]
            if nm: disp["Part Name"]   = cart_df[nm]
            if vcol: disp["Vendor"]    = cart_df[vcol]
            def to_num(x):
                try: xf=float(x); return int(xf) if xf.is_integer() else xf
                except Exception: return None if (x is None or (isinstance(x,str) and x.strip()=="")) else x
            disp["Qty"] = cart_df["__QTY__"].apply(to_num)

            cfg_cart = {
                "Remove": st.column_config.CheckboxColumn("Remove", default=False),
                "Qty": st.column_config.NumberColumn("Qty", step=1, min_value=0),
            }

            cart_key_ver = "cart_ver"
            if cart_key_ver not in st.session_state: st.session_state[cart_key_ver] = 0
            editor_key = f"cart_editor_{st.session_state[cart_key_ver]}"

            with st.form(f"{editor_key}_form", clear_on_submit=False):
                ed = st.data_editor(disp, hide_index=True, use_container_width=True,
                                    column_config=cfg_cart, key=editor_key)
                # buttons (RIGHT: small): Remove, Clear, Save, Generate
                r1, r2, r3, r4, spacer = st.columns([1,1,1,1,5])
                btn_remove   = r1.form_submit_button("🗑️ Remove")
                btn_clear    = r2.form_submit_button("🧼 Clear", disabled=cart_df.empty)
                btn_save     = r3.form_submit_button("💾 Save")
                btn_generate = r4.form_submit_button("🧾 Generate")

            if btn_save and "Qty" in ed.columns:
                def norm(v):
                    if v is None: return ""
                    if isinstance(v,(int,float)): return str(int(v)) if float(v).is_integer() else str(v)
                    return str(v)
                st.session_state[cart_key].loc[ed.index, "__QTY__"] = ed["Qty"].apply(norm).values
                st.success("Saved quantities.")

            if btn_remove:
                try: rm_idx = ed.index[ed["Remove"]==True]
                except Exception: rm_idx = []
                if len(rm_idx):
                    keys = st.session_state[cart_key].loc[rm_idx,"__KEY__"].tolist()
                    st.session_state[cart_key] = st.session_state[cart_key].loc[
                        ~st.session_state[cart_key]["__KEY__"].isin(keys)
                    ].reset_index(drop=True)
                    st.session_state[cart_key_ver] += 1
                    st.rerun()

            if btn_clear:
                st.session_state[cart_key] = cart_df.iloc[0:0].copy()
                st.session_state[cart_key_ver] += 1
                st.rerun()

            # Handle Generate: save to DB, then expose download (outside form)
            if btn_generate:
                # vendor text (single)
                vendor_text = ""
                if vcol:
                    vset = sorted(set(st.session_state[cart_key][vcol].dropna().astype(str).str.strip()))
                    if len(vset) == 1: vendor_text = vset[0]
                    elif len(vset) > 1:
                        st.error("Cart has multiple vendors; keep only one before generating.")
                        st.stop()

                # lines for storage/document
                lines_df = pd.DataFrame({
                    "Part Number": st.session_state[cart_key][pn].astype(str) if pn else "",
                    "Description": st.session_state[cart_key][nm].astype(str) if nm else "",
                    "Quantity":    st.session_state[cart_key]["__QTY__"].astype(str),
                    "Price/Unit":  "",
                    "Total":       ""
                })

                # Ship/Bill blocks; use username for user contact
                ship_to, bill_to, company_display = build_ship_bill_blocks(ACTIVE_DB_PATH, chosen, str(username))

                # Save first
                next_no = _next_quote_number(ACTIVE_DB_PATH, datetime.utcnow())
                qid, qnum = save_quote(ACTIVE_DB_PATH,
                                       quote_number=next_no,
                                       company=chosen,          # keep full code in DB
                                       created_by=str(username),
                                       vendor=vendor_text,
                                       ship_to=ship_to,
                                       bill_to=bill_to,
                                       source="restock",
                                       lines_df=lines_df)
                st.session_state["last_quote_num"] = qnum
                st.session_state["last_quote_doc"] = build_quote_docx(
                    company_display=company_display,
                    date_str=datetime.now().strftime("%Y-%m-%d"),
                    quote_number=qnum,
                    vendor_text=vendor_text,
                    ship_to_text=ship_to,
                    bill_to_text=bill_to,
                    lines_df=lines_df
                )
                st.success(f"Saved Quote #{qid} ({qnum})")

        # download button OUTSIDE the form
        if st.session_state.get("last_quote_doc"):
            st.download_button(
                "Download Quote (Word)",
                data=st.session_state["last_quote_doc"],
                file_name=f"{st.session_state.get('last_quote_num','Quote')}_{sanitize_filename(_display_company(chosen))}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

    # ---------- Outstanding POs ----------
    elif page == "Outstanding POs":
        search = st.sidebar.text_input("Search PO # / Vendor / Part / Line Name contains")
        where = ["[Company] = ?"]; params = [chosen]
        if search:
            where.append("([Purchase Order #] LIKE ? OR [Vendor] LIKE ? OR [Part Number] LIKE ? OR [Line Name] LIKE ?)")
            like = f"%{search}%"; params += [like, like, like, like]
        sql = f"SELECT * FROM [po_outstanding] WHERE {' AND '.join(where)} ORDER BY date([Created On]) ASC, [Purchase Order #]"
        df = q(sql, tuple(params))
        df = strip_time(df, ["Created On","Approved On","Completed On","Posting Date"])
        df = attach_row_key(df)
        hide = {"__KEY__","__QTY__","Company"}
        cols = [c for c in table_columns_in_order(None, "po_outstanding") if c in df.columns and c not in hide]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)
        st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(df[cols], "Outstanding_POs"),
                           file_name="Outstanding_POs.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ---------- Quotes ----------
    else:
        st.markdown("### Quotes")
        include_all = st.toggle("Show all companies", value=False)
        # quick filter menu by company codes existing in quotes
        qlist = list_quotes(ACTIVE_DB_PATH, company=(chosen if not include_all else None), include_all=include_all)
        st.caption(f"Saved quotes: {len(qlist)}")
        if qlist.empty:
            st.info("No saved quotes yet.")
        else:
            st.dataframe(qlist, hide_index=True, use_container_width=True)
            qid = st.number_input("Quote ID to open",
                                  min_value=int(qlist["id"].min()),
                                  max_value=int(qlist["id"].max()),
                                  value=int(qlist["id"].max()), step=1)
            rec = None
            if qid:
                with sqlite3.connect(ACTIVE_DB_PATH) as conn:
                    row = conn.execute("SELECT * FROM quotes WHERE id=?", (int(qid),)).fetchone()
                if row:
                    cols = ["id","quote_number","company","created_by","vendor","ship_to","bill_to",
                            "quote_date","status","source","lines_json","updated_at"]
                    d = dict(zip(cols, row))
                    try: lines = pd.DataFrame(json.loads(d["lines_json"]))
                    except Exception: lines = pd.DataFrame(columns=["Part Number","Description","Quantity","Price/Unit","Total"])
                    rec = d | {"lines": lines}
            if rec:
                cdisp = _display_company(rec["company"])
                st.write(f"**Quote #** {rec['quote_number']}  •  **Company:** {rec['company']}  •  **Source:** {rec['source']}")
                vendor = st.text_input("Vendor", value=rec["vendor"] or "", key=f"v_{rec['id']}")
                c1,c2 = st.columns(2)
                with c1:
                    ship_to = st.text_area("Ship To Address", value=rec["ship_to"] or "", height=120, key=f"s_{rec['id']}")
                with c2:
                    bill_to = st.text_area("Bill To Address", value=rec["bill_to"] or "", height=120, key=f"b_{rec['id']}")
                edited = st.data_editor(rec["lines"], hide_index=True, use_container_width=True,
                                        key=f"ed_{rec['id']}",
                                        column_config={
                                            "Part Number": st.column_config.TextColumn("Part Number"),
                                            "Description": st.column_config.TextColumn("Description"),
                                            "Quantity": st.column_config.NumberColumn("Quantity", step=1, min_value=0),
                                            "Price/Unit": st.column_config.TextColumn("Price/Unit"),
                                            "Total": st.column_config.TextColumn("Total"),
                                        })
                r1, r2, r3, r4, spacer = st.columns([1,1,1,1,5])
                if r1.button("Save", key=f"save_{rec['id']}"):
                    save_quote(ACTIVE_DB_PATH, quote_number=rec["quote_number"], company=rec["company"],
                               created_by=rec["created_by"], vendor=vendor, ship_to=ship_to, bill_to=bill_to,
                               source=rec["source"], lines_df=edited, quote_id=int(rec["id"]))
                    st.success("Saved")
                if r2.button("Generate", key=f"gen_{rec['id']}"):
                    bytes_ = build_quote_docx(company_display=cdisp, date_str=(rec["quote_date"] or datetime.now().strftime("%Y-%m-%d")),
                                              quote_number=rec["quote_number"], vendor_text=vendor,
                                              ship_to_text=ship_to, bill_to_text=bill_to, lines_df=edited)
                    st.download_button("Download Quote (Word)", data=bytes_,
                                       file_name=f"{rec['quote_number']}_{sanitize_filename(cdisp)}.docx",
                                       mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                       key=f"dl_{rec['id']}")
                r3.button("Email", disabled=True, key=f"email_{rec['id']}")  # future
                r4.button("Refresh", key=f"ref_{rec['id']}")

