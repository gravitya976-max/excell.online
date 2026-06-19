"""
Online Excell v1.3.0 — Premium Follow-up Tracker
Auto-generates monthly due lists from Insurance Policy Manager API.
"""

import os, re, sqlite3, json, io, csv, logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
from contextlib import contextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
import requests as http_requests   # renamed to avoid clash with Turso pipeline field
from apscheduler.schedulers.background import BackgroundScheduler

log = logging.getLogger("excell")
app = FastAPI(title="Online Excell", version="1.3.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data.db"))

# ── Cloud Config ──────────────────────────────────────────────────────────────
TURSO_URL = os.environ.get("TURSO_URL", "")            # libsql://db-name.turso.io
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")         # https://<acct>.r2.cloudflarestorage.com
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "online-excell")

USE_TURSO = bool(TURSO_URL and TURSO_AUTH_TOKEN)
USE_R2 = bool(R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET)

# ── Turso HTTP Client (no Rust, no extra packages) ────────────────────────────

class TursoResult:
    """Mimics sqlite3 cursor result interface."""
    def __init__(self, result_data):
        self.cols = [c["name"] for c in result_data.get("cols", [])]
        raw_rows = result_data.get("rows", [])
        self._rows = []
        for raw_row in raw_rows:
            row = {}
            for i, val in enumerate(raw_row):
                col_name = self.cols[i] if i < len(self.cols) else f"col{i}"
                row[col_name] = self._extract(val)
            self._rows.append(row)
        self.lastrowid = result_data.get("last_insert_rowid")
        self.rowcount = result_data.get("affected_row_count", 0)

    @staticmethod
    def _extract(v):
        if v is None or v.get("type") == "null":
            return None
        if v.get("type") == "integer":
            return int(v["value"])
        if v.get("type") == "float":
            return float(v["value"])
        return v.get("value", "")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

class TursoConnection:
    """HTTP-based connection to Turso — drop-in for sqlite3.Connection."""
    def __init__(self, url, token):
        self.base_url = url.replace("libsql://", "https://").rstrip("/")
        self.endpoint = f"{self.base_url}/v2/pipeline"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._last_rowid = None

    def execute(self, sql, params=None):
        stmt = {"sql": sql}
        if params:
            stmt["args"] = [self._typed(p) for p in params]
        body = {"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}
        resp = http_requests.post(self.endpoint, json=body, headers=self.headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        res = data["results"][0]
        if res["type"] == "error":
            raise Exception(f"Turso: {res['error'].get('message', res['error'])}")
        result = TursoResult(res["response"]["result"])
        if result.lastrowid:
            self._last_rowid = result.lastrowid
        return result

    @staticmethod
    def _typed(val):
        if val is None:
            return {"type": "null"}
        if isinstance(val, bool):
            return {"type": "integer", "value": str(int(val))}
        if isinstance(val, int):
            return {"type": "integer", "value": str(val)}
        if isinstance(val, float):
            return {"type": "float", "value": str(val)}
        return {"type": "text", "value": str(val)}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass  # HTTP is stateless — each execute auto-commits

    def batch_execute(self, statements):
        """Execute multiple SQL statements in ONE HTTP request.
        statements = [(sql, params), (sql, params), ...]
        """
        reqs = []
        for sql, params in statements:
            stmt = {"sql": sql}
            if params:
                stmt["args"] = [self._typed(p) for p in params]
            reqs.append({"type": "execute", "stmt": stmt})
        reqs.append({"type": "close"})
        # Turso has a max pipeline size, batch in chunks of 200
        chunk_size = 200
        all_results = []
        for i in range(0, len(reqs) - 1, chunk_size):  # -1 to exclude close
            chunk = reqs[i:i + chunk_size] + [{"type": "close"}]
            resp = http_requests.post(self.endpoint, json={"requests": chunk},
                                      headers=self.headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            for res in data["results"]:
                if res.get("type") == "error":
                    raise Exception(f"Turso batch: {res['error']}")
            all_results.extend(data["results"])
        return all_results

    def batch_query(self, statements):
        """Execute multiple queries in ONE HTTP request, return list of TursoResult.
        statements = [(sql, params), ...]
        Returns: list of TursoResult, one per statement.
        """
        reqs = []
        for sql, params in statements:
            stmt = {"sql": sql}
            if params:
                stmt["args"] = [self._typed(p) for p in params]
            reqs.append({"type": "execute", "stmt": stmt})
        reqs.append({"type": "close"})
        resp = http_requests.post(self.endpoint, json={"requests": reqs},
                                  headers=self.headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for res in data["results"]:
            if res.get("type") == "error":
                raise Exception(f"Turso batch_query: {res['error']}")
            if res.get("type") == "ok":
                results.append(TursoResult(res["response"]["result"]))
        return results

# ── Database Layer ─────────────────────────────────────────────────────────────

def dict_factory(cursor, row):
    cols = [col[0] for col in cursor.description]
    return dict(zip(cols, row))

def get_db():
    """Return Turso cloud DB if configured, else local SQLite."""
    if USE_TURSO:
        return TursoConnection(TURSO_URL, TURSO_AUTH_TOKEN)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = dict_factory
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

# ── R2 Backup (Cloudflare) ─────────────────────────────────────────────────────

def get_r2():
    """Return boto3 S3 client pointed at Cloudflare R2."""
    if not USE_R2:
        return None
    import boto3
    return boto3.client("s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )

def backup_to_r2():
    """Backup all DB data to R2 as JSON files."""
    if not USE_R2:
        return
    try:
        s3 = get_r2()
        with get_db() as conn:
            # Backup settings
            settings = conn.execute("SELECT * FROM app_settings").fetchall()
            s3.put_object(Bucket=R2_BUCKET, Key="backup/settings.json",
                          Body=json.dumps(settings, default=str), ContentType="application/json")

            # Backup all sheets + entries
            sheets = conn.execute("SELECT * FROM monthly_sheets ORDER BY year, month").fetchall()
            for sheet in sheets:
                entries = conn.execute(
                    "SELECT * FROM sheet_entries WHERE sheet_id=? ORDER BY sn", (sheet["id"],)
                ).fetchall()
                payload = {"sheet": sheet, "entries": entries}
                key = f"backup/sheets/{sheet['year']}_{sheet['month']:02d}.json"
                s3.put_object(Bucket=R2_BUCKET, Key=key,
                              Body=json.dumps(payload, default=str), ContentType="application/json")
        log.info("[R2 Backup] Success")
    except Exception as e:
        log.error(f"[R2 Backup] Error: {e}")

def restore_from_r2():
    """Restore data from R2 if DB is empty (fallback after Turso failure)."""
    if not USE_R2:
        return
    try:
        with get_db() as conn:
            count = conn.execute("SELECT COUNT(*) AS c FROM monthly_sheets").fetchone()
            if count and count["c"] > 0:
                return  # DB already has data, skip restore

        s3 = get_r2()

        # Restore settings
        try:
            obj = s3.get_object(Bucket=R2_BUCKET, Key="backup/settings.json")
            settings = json.loads(obj["Body"].read().decode())
            with get_db() as conn:
                for s in settings:
                    conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)",
                                 (s["key"], s["value"]))
            log.info(f"[R2 Restore] Restored {len(settings)} settings")
        except Exception:
            pass

        # Restore sheets
        try:
            resp = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix="backup/sheets/")
            for item in resp.get("Contents", []):
                obj = s3.get_object(Bucket=R2_BUCKET, Key=item["Key"])
                data = json.loads(obj["Body"].read().decode())
                sheet = data["sheet"]
                entries = data["entries"]
                with get_db() as conn:
                    conn.execute(
                        "INSERT OR IGNORE INTO monthly_sheets (month, year, generated_at, total_count) VALUES (?,?,?,?)",
                        (sheet["month"], sheet["year"], sheet.get("generated_at",""), sheet.get("total_count",0))
                    )
                    new_sheet = conn.execute(
                        "SELECT id FROM monthly_sheets WHERE month=? AND year=?",
                        (sheet["month"], sheet["year"])
                    ).fetchone()
                    if new_sheet:
                        for e in entries:
                            conn.execute(
                                """INSERT INTO sheet_entries
                                (sheet_id,sn,policyno,name,doc,plan,mode,premium,sumass,mobileno,due_date,status,
                                 col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,updated_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (new_sheet["id"], e.get("sn"), e.get("policyno",""), e.get("name",""),
                                 e.get("doc",""), e.get("plan",""), e.get("mode",""), e.get("premium",""),
                                 e.get("sumass",""), e.get("mobileno",""), e.get("due_date",""),
                                 e.get("status","DUE"), e.get("col1",""), e.get("col2",""),
                                 e.get("col3",""), e.get("col4",""), e.get("col5",""),
                                 e.get("col6",""), e.get("col7",""), e.get("col8",""),
                                 e.get("col9",""), e.get("col10",""), e.get("updated_at",""))
                            )
            log.info("[R2 Restore] Sheets restored")
        except Exception as e:
            log.error(f"[R2 Restore] Sheet error: {e}")
    except Exception as e:
        log.error(f"[R2 Restore] Error: {e}")

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
        # Use try/except instead of PRAGMA (PRAGMA doesn't work over Turso HTTP API)
        for i in range(1, 11):
            cname = f"col{i}"
            try:
                conn.execute(f"ALTER TABLE sheet_entries ADD COLUMN {cname} TEXT DEFAULT ''")
            except Exception:
                pass  # Column already exists

try:
    init_db()
    log.info(f"[Startup] init_db OK — USE_TURSO={USE_TURSO}")
    print(f"[Startup] init_db OK — USE_TURSO={USE_TURSO}")
except Exception as e:
    log.error(f"[Startup] init_db FAILED: {e}")
    print(f"[Startup] init_db FAILED: {e}")

# Restore from R2 if DB is empty (after fresh deploy)
try:
    restore_from_r2()
except Exception as e:
    log.error(f"[Startup] restore_from_r2 FAILED: {e}")
    print(f"[Startup] restore_from_r2 FAILED: {e}")

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
            resp = http_requests.get(
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
        except http_requests.RequestException as e:
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
        sheet_row = conn.execute(
            "SELECT id FROM monthly_sheets WHERE month=? AND year=?", (month, year)
        ).fetchone()
        sheet_id = sheet_row["id"]

        # Batch insert all entries at once (1 HTTP call instead of 400+)
        if hasattr(conn, 'batch_execute') and due_policies:
            stmts = []
            for i, p in enumerate(due_policies, 1):
                stmts.append((
                    """INSERT INTO sheet_entries
                    (sheet_id, sn, policyno, name, doc, plan, mode, premium, sumass, mobileno, due_date, status, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (sheet_id, i, p.get("policyno",""), p.get("name",""), p.get("doc",""),
                     p.get("plan",""), p.get("mode",""), p.get("premium",""), p.get("sumass",""),
                     p.get("mobileno",""), p.get("due_date",""), p["status"],
                     datetime.now().isoformat())
                ))
            conn.batch_execute(stmts)
        else:
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

    # Backup to R2 after generating
    backup_to_r2()

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
        http_requests.get(f"http://127.0.0.1:{port}/health", timeout=5)
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
# Backup to R2 every hour
if USE_R2:
    scheduler.add_job(backup_to_r2, 'interval', hours=1)
scheduler.start()

# ── API Endpoints ──────────────────────────────────────────────────────────────

# Combined init endpoint — fetches EVERYTHING in minimal Turso calls
@app.get("/api/init/{year}/{month}")
def api_init(year: int, month: int):
    """Return settings, col-names, sheet data, and unpaid alert in ONE request."""
    today = date.today()
    prev = date(today.year, today.month, 1) - __import__('datetime').timedelta(days=1)
    prev_month, prev_year = prev.month, prev.year

    try:
        with get_db() as conn:
            # Get ALL settings in one query (instead of 12 separate calls)
            all_settings_rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
            all_settings = {r["key"]: r["value"] for r in all_settings_rows}

            # Get current sheet
            sheet = conn.execute(
                "SELECT * FROM monthly_sheets WHERE month=? AND year=?", (month, year)
            ).fetchone()
            entries = []
            if sheet:
                entries = conn.execute(
                    "SELECT * FROM sheet_entries WHERE sheet_id=? ORDER BY sn", (sheet["id"],)
                ).fetchall()

            # Get previous month unpaid
            prev_sheet = conn.execute(
                "SELECT id FROM monthly_sheets WHERE month=? AND year=?", (prev_month, prev_year)
            ).fetchone()
            unpaid_entries = []
            unpaid_count = 0
            if prev_sheet:
                unpaid_entries = conn.execute(
                    "SELECT * FROM sheet_entries WHERE sheet_id=? AND status='DUE' ORDER BY sn",
                    (prev_sheet["id"],)
                ).fetchall()
                unpaid_count = len(unpaid_entries)
    except Exception as e:
        log.error(f"[api_init] Error: {e}")
        raise HTTPException(500, f"Init failed: {e}")

    # Build col names from settings
    col_names = {}
    for i in range(1, 11):
        col_names[f"col{i}"] = all_settings.get(f"col{i}_name", f"Note {i}")

    return {
        "settings": {
            "api_key": all_settings.get("api_key", ""),
            "api_base_url": all_settings.get("api_base_url", ""),
        },
        "col_names": col_names,
        "sheet": {
            "exists": sheet is not None,
            **(dict(sheet) if sheet else {"month": month, "year": year}),
            "entries": [dict(e) for e in entries],
        },
        "unpaid": {
            "has_unpaid": unpaid_count > 0,
            "count": unpaid_count,
            "month": prev_month,
            "year": prev_year,
            "entries": [dict(e) for e in unpaid_entries],
        },
    }

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
    backup_to_r2()  # persist settings to R2
    return {"message": "Settings saved."}

@app.post("/api/settings/test")
def api_test_connection():
    api_key = get_setting("api_key")
    base_url = get_setting("api_base_url")
    if not api_key or not base_url:
        raise HTTPException(400, "API key and base URL not configured.")
    try:
        resp = http_requests.get(
            f"{base_url}/api/v1/policies/count",
            headers={"X-API-Key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "count": data.get("count", 0), "message": f"Connected! {data.get('count',0)} policies found."}
    except http_requests.RequestException as e:
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

@app.delete("/api/sheets/{year}/{month}")
def api_delete_sheet(year: int, month: int):
    with get_db() as conn:
        sheet = conn.execute(
            "SELECT id FROM monthly_sheets WHERE month=? AND year=?", (month, year)
        ).fetchone()
        if not sheet:
            raise HTTPException(404, "Sheet not found")
        conn.execute("DELETE FROM sheet_entries WHERE sheet_id=?", (sheet["id"],))
        conn.execute("DELETE FROM monthly_sheets WHERE id=?", (sheet["id"],))
    # Also delete from R2 backup
    if USE_R2:
        try:
            s3 = get_r2()
            s3.delete_object(Bucket=R2_BUCKET, Key=f"backup/sheets/{year}_{month:02d}.json")
        except Exception:
            pass
    backup_to_r2()  # re-sync remaining data
    return {"message": f"Sheet for {month}/{year} deleted from everywhere."}

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
    return {"status": "ok", "version": "1.3.0"}

@app.get("/api/debug")
def api_debug():
    """Diagnostic endpoint to check DB connection and data."""
    info = {
        "USE_TURSO": USE_TURSO,
        "TURSO_URL": TURSO_URL[:30] + "..." if TURSO_URL else "(empty)",
        "TURSO_AUTH_TOKEN": "set" if TURSO_AUTH_TOKEN else "(empty)",
        "USE_R2": USE_R2,
        "DB_PATH": DB_PATH,
    }
    try:
        with get_db() as conn:
            sheets = conn.execute("SELECT COUNT(*) AS c FROM monthly_sheets").fetchone()
            entries = conn.execute("SELECT COUNT(*) AS c FROM sheet_entries").fetchone()
            info["sheets_count"] = sheets["c"] if sheets else 0
            info["entries_count"] = entries["c"] if entries else 0
            info["db_ok"] = True
    except Exception as e:
        info["db_ok"] = False
        info["db_error"] = str(e)
    return info
