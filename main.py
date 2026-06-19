"""
Online Excell v1.0.0 — Premium Follow-up Tracker
Auto-generates monthly due lists from Insurance Policy Manager API.
"""

import os, re, sqlite3, json, io, csv
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
from contextlib import contextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
import requests
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI(title="Online Excell", version="1.0.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data.db"))

# Turso cloud SQLite config (set these env vars for production)
TURSO_URL = os.environ.get("TURSO_URL", "")       # e.g. libsql://your-db-name.turso.io
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")

# ── Database ───────────────────────────────────────────────────────────────────

def dict_factory(cursor, row):
    cols = [col[0] for col in cursor.description]
    return dict(zip(cols, row))

def get_db():
    """Connect to Turso (cloud) if configured, otherwise local SQLite."""
    if TURSO_URL and TURSO_AUTH_TOKEN:
        import libsql_experimental as libsql
        conn = libsql.connect("local.db", sync_url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN)
        conn.sync()
        conn.row_factory = dict_factory
        return conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = dict_factory
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

def init_db():
    with get_db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS monthly_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_count INTEGER DEFAULT 0,
            UNIQUE(month, year)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS sheet_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_id INTEGER NOT NULL REFERENCES monthly_sheets(id) ON DELETE CASCADE,
            sn INTEGER,
            policyno TEXT,
            name TEXT,
            doc TEXT,
            plan TEXT,
            mode TEXT,
            premium TEXT,
            sumass TEXT,
            mobileno TEXT,
            due_date TEXT,
            status TEXT DEFAULT 'DUE',
            col1 TEXT DEFAULT '',
            col2 TEXT DEFAULT '',
            col3 TEXT DEFAULT '',
            col4 TEXT DEFAULT '',
            col5 TEXT DEFAULT '',
            col6 TEXT DEFAULT '',
            col7 TEXT DEFAULT '',
            col8 TEXT DEFAULT '',
            col9 TEXT DEFAULT '',
            col10 TEXT DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        # Migrate: add col1-col10 if missing on existing DB
        existing_cols = {r["name"] for r in conn.execute("PRAGMA table_info(sheet_entries)").fetchall()}
        for i in range(1, 11):
            cname = f"col{i}"
            if cname not in existing_cols:
                conn.execute(f"ALTER TABLE sheet_entries ADD COLUMN {cname} TEXT DEFAULT ''")

init_db()

# ── Settings helpers ───────────────────────────────────────────────────────────

def get_setting(key: str) -> str | None:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None

def set_setting(key: str, value: str):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)", (key, value))

# ── Mode normalization ─────────────────────────────────────────────────────────

MODE_MAP = {
    "m": 1, "mly": 1, "monthly": 1,
    "q": 3, "qly": 3, "quarterly": 3,
    "h": 6, "hly": 6, "halfyearly": 6, "half yearly": 6, "hy": 6,
    "a": 12, "ann": 12, "y": 12, "yly": 12, "yearly": 12, "annual": 12, "annually": 12,
}

def get_mode_months(mode_str: str) -> int | None:
    if not mode_str:
        return None
    cleaned = re.sub(r"[^a-z ]", "", str(mode_str).lower().strip())
    if cleaned in MODE_MAP:
        return MODE_MAP[cleaned]
    nospace = cleaned.replace(" ", "")
    if nospace in MODE_MAP:
        return MODE_MAP[nospace]
    return None

# ── FUP Calculation Engine ─────────────────────────────────────────────────────

def parse_doc(doc_str: str) -> date | None:
    """Parse various date formats into a date object."""
    if not doc_str:
        return None
    try:
        dt = dateparser.parse(str(doc_str), dayfirst=True)
        return dt.date() if dt else None
    except:
        return None

def is_due_in_month(doc_str: str, mode_str: str, year: int, month: int) -> bool:
    """Check if a policy with given DOC and mode has a premium due in year/month."""
    doc_date = parse_doc(doc_str)
    if not doc_date:
        return False
    interval = get_mode_months(mode_str)
    if not interval:
        return False

    # Generate due dates from DOC forward until we pass the target month
    current = doc_date
    target_start = date(year, month, 1)
    if month == 12:
        target_end = date(year + 1, 1, 1)
    else:
        target_end = date(year, month + 1, 1)

    # If DOC is in the future beyond our target, no due
    if doc_date >= target_end:
        return False

    # Jump forward in intervals from DOC
    # First, quickly skip ahead to near the target month
    if current < target_start:
        months_diff = (target_start.year - current.year) * 12 + (target_start.month - current.month)
        skip_intervals = max(0, (months_diff // interval) - 1)
        if skip_intervals > 0:
            current = doc_date + relativedelta(months=interval * skip_intervals)

    # Now iterate to find if any due date falls in target month
    while current < target_end:
        if target_start <= current < target_end:
            return True
        current = current + relativedelta(months=interval)
        # Safety: if we've gone too far
        if current.year > year + 2:
            break

    return False

def get_due_date(doc_str: str, mode_str: str, year: int, month: int) -> str | None:
    """Get the exact due date in the target month, formatted as DD/MM/YYYY."""
    doc_date = parse_doc(doc_str)
    if not doc_date:
        return None
    interval = get_mode_months(mode_str)
    if not interval:
        return None

    target_start = date(year, month, 1)
    if month == 12:
        target_end = date(year + 1, 1, 1)
    else:
        target_end = date(year, month + 1, 1)

    current = doc_date
    if current < target_start:
        months_diff = (target_start.year - current.year) * 12 + (target_start.month - current.month)
        skip_intervals = max(0, (months_diff // interval) - 1)
        if skip_intervals > 0:
            current = doc_date + relativedelta(months=interval * skip_intervals)

    while current < target_end:
        if target_start <= current < target_end:
            return current.strftime("%d/%m/%Y")
        current = current + relativedelta(months=interval)
        if current.year > year + 2:
            break

    return None

# ── Status normalization ───────────────────────────────────────────────────────

AUTO_STATUSES = {"autodebit", "branchpaid", "dailycollection"}

def normalize_api_status(status_val: str) -> str:
    """Determine the status for a sheet entry based on API status."""
    if not status_val or not str(status_val).strip():
        return "DUE"
    cleaned = re.sub(r"[^a-z]", "", str(status_val).lower().strip())
    if cleaned in AUTO_STATUSES:
        return cleaned.upper()
    # Map common variants
    if cleaned in ("autodebit", "auto debit"):
        return "AUTODEBIT"
    if cleaned in ("branchpaid", "branch paid", "branchpaidonly"):
        return "BRANCHPAID"
    if cleaned in ("dailycollection", "daily collection"):
        return "DAILYCOLLECTION"
    # Everything else (due, paid, blank, unknown) → DUE
    return "DUE"

# ── Sheet generation ───────────────────────────────────────────────────────────

def generate_sheet(year: int, month: int) -> dict:
    """Pull policies from API, calculate FUP, save as monthly sheet."""
    api_key = get_setting("api_key")
    base_url = get_setting("api_base_url")
    if not api_key or not base_url:
        raise HTTPException(400, "API key and base URL not configured. Go to Settings.")

    # Check if sheet already exists
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM monthly_sheets WHERE month=? AND year=?", (month, year)
        ).fetchone()
        if existing:
            # Delete old sheet and regenerate
            conn.execute("DELETE FROM sheet_entries WHERE sheet_id=?", (existing["id"],))
            conn.execute("DELETE FROM monthly_sheets WHERE id=?", (existing["id"],))

    # Pull ALL policies from API (paginated)
    all_policies = []
    offset = 0
    limit = 500
    while True:
        try:
            resp = requests.get(
                f"{base_url.rstrip('/')}/api/v1/policies",
                headers={"X-API-Key": api_key},
                params={"limit": limit, "offset": offset},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            policies = data.get("data", [])
            all_policies.extend(policies)
            if len(policies) < limit:
                break
            offset += limit
        except requests.RequestException as e:
            raise HTTPException(502, f"Failed to fetch from API: {e}")

    # Filter policies due in target month (deduplicate by policyno)
    due_policies = []
    seen_pnos = set()
    for p in all_policies:
        pno = (p.get("policyno") or "").strip().upper()
        if not pno or pno in seen_pnos:
            continue
        doc = p.get("doc", "")
        mode = p.get("mode", "")
        if not doc or not mode:
            continue
        if is_due_in_month(doc, mode, year, month):
            seen_pnos.add(pno)
            due_date = get_due_date(doc, mode, year, month)
            status = normalize_api_status(p.get("status", ""))
            due_policies.append({**p, "due_date": due_date, "status": status})

    # Save to database
    with get_db() as conn:
        conn.execute(
            "INSERT INTO monthly_sheets (month, year, generated_at, total_count) VALUES (?,?,?,?)",
            (month, year, datetime.now().isoformat(), len(due_policies))
        )
        sheet_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        for i, p in enumerate(due_policies, 1):
            conn.execute(
                """INSERT INTO sheet_entries
                (sheet_id, sn, policyno, name, doc, plan, mode, premium, sumass, mobileno, due_date, status, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sheet_id, i, p.get("policyno",""), p.get("name",""), p.get("doc",""),
                 p.get("plan",""), p.get("mode",""), p.get("premium",""), p.get("sumass",""),
                 p.get("mobileno",""), p.get("due_date",""), p["status"],
                 datetime.now().isoformat())
            )

    return {
        "sheet_id": sheet_id,
        "month": month,
        "year": year,
        "total_count": len(due_policies),
        "generated_at": datetime.now().isoformat(),
    }

# ── Background scheduler — auto-generate on 1st of every month ────────────────

def auto_generate_current_month():
    """Background job: generate current month's sheet if not exists."""
    today = date.today()
    try:
        api_key = get_setting("api_key")
        base_url = get_setting("api_base_url")
        if not api_key or not base_url:
            return  # Not configured yet, skip
        with get_db() as conn:
            existing = conn.execute(
                "SELECT id FROM monthly_sheets WHERE month=? AND year=?",
                (today.month, today.year)
            ).fetchone()
        if not existing:
            generate_sheet(today.year, today.month)
            print(f"[AutoGen] Generated sheet for {today.strftime('%B %Y')}")
    except Exception as e:
        print(f"[AutoGen] Error: {e}")

def self_ping():
    """Ping own /health endpoint to prevent Render free tier from sleeping."""
    try:
        port = int(os.environ.get("PORT", 8900))
        requests.get(f"http://127.0.0.1:{port}/health", timeout=5)
        print("[SelfPing] OK")
    except Exception:
        pass  # Server might not be ready yet

scheduler = BackgroundScheduler()
# Run on the 1st of every month at 00:05
scheduler.add_job(auto_generate_current_month, 'cron', day=1, hour=0, minute=5)
# Also run at startup after a short delay
scheduler.add_job(auto_generate_current_month, 'date',
                  run_date=datetime.now() + __import__('datetime').timedelta(seconds=5))
# Self-ping every 10 minutes to stay alive on Render free tier
scheduler.add_job(self_ping, 'interval', minutes=10)
scheduler.start()

# ── API Endpoints ──────────────────────────────────────────────────────────────

# Settings
@app.get("/api/settings")
def api_get_settings():
    return {
        "api_key": get_setting("api_key") or "",
        "api_base_url": get_setting("api_base_url") or "",
    }

# Column header names
@app.get("/api/col-names")
def api_get_col_names():
    names = {}
    for i in range(1, 11):
        names[f"col{i}"] = get_setting(f"col{i}_name") or f"Note {i}"
    return names

@app.patch("/api/col-names")
def api_set_col_name(col: str = Query(...), name: str = Query(...)):
    allowed = {f"col{i}" for i in range(1, 11)}
    if col not in allowed:
        raise HTTPException(400, f"Invalid column: {col}")
    set_setting(f"{col}_name", name.strip())
    return {"col": col, "name": name.strip()}

@app.post("/api/settings")
def api_save_settings(api_key: str = Query(...), api_base_url: str = Query(...)):
    set_setting("api_key", api_key.strip())
    set_setting("api_base_url", api_base_url.strip().rstrip("/"))
    return {"message": "Settings saved."}

@app.post("/api/settings/test")
def api_test_connection():
    api_key = get_setting("api_key")
    base_url = get_setting("api_base_url")
    if not api_key or not base_url:
        raise HTTPException(400, "API key and base URL not configured.")
    try:
        resp = requests.get(
            f"{base_url}/api/v1/policies/count",
            headers={"X-API-Key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "count": data.get("count", 0), "message": f"Connected! {data.get('count',0)} policies found."}
    except requests.RequestException as e:
        return {"success": False, "message": f"Connection failed: {e}"}

# Sheets
@app.get("/api/sheets")
def api_list_sheets():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM monthly_sheets ORDER BY year DESC, month DESC"
        ).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/sheets/current")
def api_current_sheet():
    today = date.today()
    return api_get_sheet(today.year, today.month)

@app.get("/api/sheets/{year}/{month}")
def api_get_sheet(year: int, month: int):
    with get_db() as conn:
        sheet = conn.execute(
            "SELECT * FROM monthly_sheets WHERE month=? AND year=?", (month, year)
        ).fetchone()
        if not sheet:
            return {"exists": False, "month": month, "year": year, "entries": []}
        entries = conn.execute(
            "SELECT * FROM sheet_entries WHERE sheet_id=? ORDER BY sn",
            (sheet["id"],)
        ).fetchall()
    return {
        "exists": True,
        **dict(sheet),
        "entries": [dict(e) for e in entries],
    }

@app.post("/api/sheets/generate/{year}/{month}")
def api_generate_sheet(year: int, month: int):
    return generate_sheet(year, month)

# Toggle status
@app.patch("/api/entries/{entry_id}/toggle")
def api_toggle_status(entry_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM sheet_entries WHERE id=?", (entry_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Entry not found.")
        current = row["status"]
        # Only toggle DUE ↔ PAID. Auto statuses are locked.
        if current in ("AUTODEBIT", "BRANCHPAID", "DAILYCOLLECTION"):
            raise HTTPException(400, "Cannot toggle auto-payment statuses.")
        new_status = "PAID" if current == "DUE" else "DUE"
        conn.execute(
            "UPDATE sheet_entries SET status=?, updated_at=? WHERE id=?",
            (new_status, datetime.now().isoformat(), entry_id)
        )
    return {"id": entry_id, "status": new_status}

# Update a cell (any field)
@app.patch("/api/entries/{entry_id}/cell")
def api_update_cell(entry_id: int, col: str = Query(...), value: str = Query("")):
    allowed = {"sn","policyno","name","doc","plan","mode","premium","sumass","mobileno","due_date",
               "col1","col2","col3","col4","col5","col6","col7","col8","col9","col10"}
    if col not in allowed:
        raise HTTPException(400, f"Invalid column: {col}")
    with get_db() as conn:
        row = conn.execute("SELECT id FROM sheet_entries WHERE id=?", (entry_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Entry not found.")
        conn.execute(
            f"UPDATE sheet_entries SET {col}=?, updated_at=? WHERE id=?",
            (value, datetime.now().isoformat(), entry_id)
        )
    return {"id": entry_id, "col": col, "value": value}

# Unpaid alert
@app.get("/api/unpaid-alert")
def api_unpaid_alert():
    today = date.today()
    # Previous month
    prev = date(today.year, today.month, 1) - __import__('datetime').timedelta(days=1)
    prev_month, prev_year = prev.month, prev.year

    with get_db() as conn:
        sheet = conn.execute(
            "SELECT id, month, year FROM monthly_sheets WHERE month=? AND year=?",
            (prev_month, prev_year)
        ).fetchone()
        if not sheet:
            return {"has_unpaid": False, "count": 0, "month": prev_month, "year": prev_year}
        unpaid = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sheet_entries WHERE sheet_id=? AND status='DUE'",
            (sheet["id"],)
        ).fetchone()
        entries = []
        if unpaid["cnt"] > 0:
            entries = conn.execute(
                "SELECT * FROM sheet_entries WHERE sheet_id=? AND status='DUE' ORDER BY sn",
                (sheet["id"],)
            ).fetchall()
    return {
        "has_unpaid": unpaid["cnt"] > 0,
        "count": unpaid["cnt"],
        "month": prev_month,
        "year": prev_year,
        "entries": [dict(e) for e in entries],
    }

# CSV export
@app.get("/api/sheets/{year}/{month}/csv")
def api_export_csv(year: int, month: int):
    with get_db() as conn:
        sheet = conn.execute(
            "SELECT id FROM monthly_sheets WHERE month=? AND year=?", (month, year)
        ).fetchone()
        if not sheet:
            raise HTTPException(404, "Sheet not found.")
        entries = conn.execute(
            "SELECT sn, policyno, name, doc, plan, mode, premium, sumass, mobileno, due_date, status FROM sheet_entries WHERE sheet_id=? ORDER BY sn",
            (sheet["id"],)
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["SN", "Policy No", "Name", "DOC", "Plan", "Mode", "Premium", "Sum Assured", "Mobile", "Due Date", "Status"])
    for e in entries:
        writer.writerow([e["sn"], e["policyno"], e["name"], e["doc"], e["plan"],
                         e["mode"], e["premium"], e["sumass"], e["mobileno"],
                         e["due_date"], e["status"]])

    output.seek(0)
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    filename = f"DueList_{month_names[month]}_{year}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# Server time endpoint for real-time clock
@app.get("/api/time")
def api_server_time():
    return {"time": datetime.now().isoformat()}

# Serve frontend
@app.get("/")
def root():
    html_path = os.path.join(BASE_DIR, "index.html")
    return HTMLResponse(open(html_path, encoding="utf-8").read())

@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok", "version": "1.0.0"}
