#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
مدیریت پروژه‌ها — برنامه‌ی تک‌فایلی با SQLite
------------------------------------------------
پروژه‌های طراحی سایت، سئو و نرم‌افزار: مراحل، کار بعدی، کارهای انجام‌شده،
پول‌های دریافتی و هزینه‌ها.

اجرا:   python app.py          (یا python3 app.py)
بعد مرورگر خودش باز می‌شه روی  http://127.0.0.1:8765

هیچ کتابخونه‌ی اضافه‌ای لازم نیست (فقط پایتون ۳.۸ به بالا).
اطلاعات توی فایل projects.db کنار همین فایل ذخیره می‌شه — از اون بکاپ بگیر.

تنظیمات اختیاری (متغیر محیطی):
  PM_PORT=8765         پورت
  PM_HOST=127.0.0.1    برای دسترسی از گوشی تو همون شبکه بذار 0.0.0.0
  PM_DB=projects.db    مسیر فایل دیتابیس
  PM_PASSWORD=...      رمز ورود (برای وقتی که برنامه رو آنلاین می‌کنی)

برای آنلاین کردن روی هاست، راهنمای README.md رو ببین (فایل passenger_wsgi.py).
"""

import json
import os
import re
import sqlite3
import sys
import threading
import webbrowser
from datetime import datetime, timezone
import hashlib
import hmac
import secrets
import tempfile
import time
import urllib.error
import urllib.request
from http.cookies import SimpleCookie
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlencode
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("PM_DB") or os.path.join(BASE_DIR, "projects.db")
HOST = os.environ.get("PM_HOST", "127.0.0.1")
PORT = int(os.environ.get("PM_PORT", "8765"))

KINDS = ("website", "seo", "software", "other")
STATUSES = ("active", "paused", "done", "cancelled")
STAGE_STATUSES = ("todo", "doing", "done")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  client_name TEXT NOT NULL DEFAULT '',
  client_phone TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT 'website',
  status TEXT NOT NULL DEFAULT 'active',
  total_price INTEGER NOT NULL DEFAULT 0,
  start_date TEXT NOT NULL DEFAULT '',
  deadline TEXT NOT NULL DEFAULT '',
  next_action TEXT NOT NULL DEFAULT '',
  notes TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'todo',
  position INTEGER NOT NULL DEFAULT 0,
  done_at TEXT
);
CREATE TABLE IF NOT EXISTS money (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  type TEXT NOT NULL CHECK (type IN ('payment','expense')),
  amount INTEGER NOT NULL,
  date TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  text TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stages_project ON stages(project_id, position);
CREATE INDEX IF NOT EXISTS idx_money_project ON money(project_id);
CREATE INDEX IF NOT EXISTS idx_logs_project ON logs(project_id);
"""

_db_lock = threading.Lock()


# --- Vercel: keep the SQLite file in Vercel Blob ---------------------------
# Vercel wipes the disk between requests, so there the database file lives
# in /tmp and is copied to/from Vercel Blob (Storage → Blob in the Vercel
# dashboard; it sets BLOB_READ_WRITE_TOKEN). Each save is a new blob with a
# random, unguessable name; older copies are deleted. Locally none of this
# runs — it's just projects.db next to app.py.

ON_VERCEL = bool(os.environ.get("VERCEL"))


def _find_blob_token():
    """BLOB_READ_WRITE_TOKEN, or the same token under a custom prefix (e.g. MYSTORE_READ_WRITE_TOKEN)."""
    t = os.environ.get("BLOB_READ_WRITE_TOKEN", "").strip()
    if t:
        return t
    for k, v in sorted(os.environ.items()):
        if k.endswith("_READ_WRITE_TOKEN") and v.strip().startswith("vercel_blob_rw_"):
            return v.strip()
    return ""


# Two ways Vercel connects a Blob store to a project:
#  - a read-write token (BLOB_READ_WRITE_TOKEN = vercel_blob_rw_<storeId>_<secret>), or
#  - newer: BLOB_STORE_ID + Vercel's OIDC token, which arrives with every request
#    in the x-vercel-oidc-token header (or VERCEL_OIDC_TOKEN).
BLOB_RW_TOKEN = _find_blob_token()


def _norm_store(sid):
    sid = (sid or "").strip()
    return sid[len("store_"):] if sid.startswith("store_") else sid


BLOB_STORE_ID = _norm_store((BLOB_RW_TOKEN.split("_") + ["", "", "", ""])[3]) if BLOB_RW_TOKEN \
    else _norm_store(os.environ.get("BLOB_STORE_ID", ""))
_req_oidc = ""  # OIDC token of the request being handled


def _oidc_token():
    if _req_oidc:
        return _req_oidc
    t = os.environ.get("VERCEL_OIDC_TOKEN", "").strip()
    if t:
        return t
    f = os.environ.get("VERCEL_OIDC_TOKEN_FILE", "")
    if f and os.path.exists(f):
        with open(f) as fh:
            return fh.read().strip()
    return ""


def blob_token():
    return BLOB_RW_TOKEN or (_oidc_token() if BLOB_STORE_ID else "")


def blob_ready():
    return bool(ON_VERCEL and blob_token())
BLOB_API = os.environ.get("PM_BLOB_API", "https://blob.vercel-storage.com")  # override only for tests
BLOB_API_VERSION = "13"  # same as @vercel/blob 2.x
BLOB_PREFIX = "project-manager/"
# A store is created "private" (recommended) or "public"; uploads must say which.
_blob_access = os.environ.get("PM_BLOB_ACCESS", "").strip().lower() or None
_blob_url = None  # the copy currently in /tmp
_sync_lock = threading.Lock()

if ON_VERCEL:
    DB_PATH = os.path.join(tempfile.gettempdir(), "projects.db")


class BlobHTTPError(RuntimeError):
    def __init__(self, code, detail):
        super().__init__(f"Vercel Blob HTTP {code}: {detail}")
        self.code, self.detail = code, detail


def _blob_headers(extra=None):
    h = {"authorization": "Bearer " + blob_token(), "x-api-version": BLOB_API_VERSION}
    if BLOB_STORE_ID:
        h["x-vercel-blob-store-id"] = BLOB_STORE_ID
    h.update(extra or {})
    return h


def _blob(method, query="", body=None, headers=None, path="/"):
    req = urllib.request.Request(BLOB_API + path + (("?" + query) if query else ""), data=body, method=method,
                                 headers=_blob_headers(headers))
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()[:400]
        try:
            j = json.loads(raw)
            detail = (j.get("error") or {}).get("message") or (j.get("error") or {}).get("code") or raw.decode("utf-8", "replace")
        except Exception:
            detail = raw.decode("utf-8", "replace")
        raise BlobHTTPError(e.code, detail)


def _blob_download(url):
    # Private stores need the token to read; public ones simply ignore it.
    req = urllib.request.Request(url, headers={"authorization": "Bearer " + blob_token()})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        raise BlobHTTPError(e.code, "download failed")


def _blob_list():
    out, cursor = [], None
    while True:
        q = {"prefix": BLOB_PREFIX, "limit": "1000"}
        if cursor:
            q["cursor"] = cursor
        r = _blob("GET", urlencode(q))
        out += r.get("blobs", [])
        cursor = r.get("cursor")
        if not r.get("hasMore") or not cursor:
            return sorted(out, key=lambda b: b.get("uploadedAt", ""))


def _blob_upload(data):
    """Upload a new copy; works out whether the store is private or public."""
    global _blob_access
    tries = [_blob_access] if _blob_access else ["private", "public"]
    last = None
    for access in tries:
        try:
            r = _blob("PUT", urlencode({"pathname": BLOB_PREFIX + "projects.db"}), data,
                      {"x-vercel-blob-access": access, "x-add-random-suffix": "1",
                       "x-content-type": "application/octet-stream"})
            _blob_access = access
            return r
        except BlobHTTPError as e:
            last = e
            if e.code not in (400, 403, 409, 422):  # only an access mismatch is worth retrying
                raise
    raise last


def blob_pull():
    """Bring /tmp/projects.db up to date with the latest saved copy."""
    global _blob_url
    if not blob_ready():
        return False
    blobs = _blob_list()
    if not blobs:
        return False
    latest = blobs[-1]["url"]
    if latest == _blob_url and os.path.exists(DB_PATH):
        return False
    if True:
        data = _blob_download(latest)
        tmp = DB_PATH + ".dl"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, DB_PATH)
        _blob_url = latest
        return True


def blob_push():
    """Save /tmp/projects.db as a new copy and remove the older ones."""
    global _blob_url
    if not blob_ready():
        return
    with open(DB_PATH, "rb") as f:
        data = f.read()
    r = _blob_upload(data)
    _blob_url = r["url"]
    old = [b["url"] for b in _blob_list() if b["url"] != _blob_url]
    if old:
        _blob("POST", body=json.dumps({"urls": old}).encode(), headers={"content-type": "application/json"}, path="/delete")


def db():
    if ON_VERCEL and not blob_token():
        raise RuntimeError("روی Vercel باید Blob رو وصل کنی: Storage → Create → Blob → Connect (راهنما: README).")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def backup_bytes():
    """A downloadable copy of the SQLite file."""
    tmp = os.path.join(tempfile.gettempdir(), "pm-backup-%s.db" % secrets.token_hex(4))
    try:
        with db() as src, sqlite3.connect(tmp) as dst:
            src.backup(dst)
        with open(tmp, "rb") as f:
            return f.read()
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def init_db():
    with db() as conn:
        conn.executescript(SCHEMA)


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def touch(conn, project_id):
    conn.execute("UPDATE projects SET updated_at=? WHERE id=?", (now(), project_id))


def all_projects():
    with db() as conn:
        projects = [dict(r) for r in conn.execute("SELECT * FROM projects ORDER BY updated_at DESC")]
        by_id = {p["id"]: p for p in projects}
        for p in projects:
            p["stages"], p["payments"], p["expenses"], p["log"] = [], [], [], []
        for r in conn.execute("SELECT * FROM stages ORDER BY position, id"):
            if r["project_id"] in by_id:
                by_id[r["project_id"]]["stages"].append(dict(r))
        for r in conn.execute("SELECT * FROM money ORDER BY id"):
            if r["project_id"] in by_id:
                key = "payments" if r["type"] == "payment" else "expenses"
                by_id[r["project_id"]][key].append(dict(r))
        for r in conn.execute("SELECT * FROM logs ORDER BY id"):
            if r["project_id"] in by_id:
                by_id[r["project_id"]]["log"].append(dict(r))
        return projects


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

class BadRequest(Exception):
    pass


_FA = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def to_en(s):
    return str(s or "").translate(_FA)


def text(v, limit=2000):
    return str(v if v is not None else "").strip()[:limit]


def amount(v, allow_zero=True):
    s = re.sub(r"[,٬،\s]", "", to_en(v))
    if s == "":
        if allow_zero:
            return 0
        raise BadRequest("مبلغ رو بنویس.")
    if not s.isdigit():
        raise BadRequest("مبلغ رو فقط با عدد بنویس (به تومان).")
    n = int(s)
    if not allow_zero and n <= 0:
        raise BadRequest("مبلغ باید بیشتر از صفر باشه.")
    return n


PROJECT_FIELDS = ("title", "client_name", "client_phone", "kind", "status",
                  "total_price", "start_date", "deadline", "next_action", "notes")


def clean_project(data, partial=False):
    out = {}
    for f in PROJECT_FIELDS:
        if f not in data:
            continue
        v = data[f]
        if f == "kind":
            v = v if v in KINDS else "other"
        elif f == "status":
            v = v if v in STATUSES else "active"
        elif f == "total_price":
            v = amount(v)
        elif f == "client_phone":
            v = to_en(text(v, 40))
        elif f in ("notes",):
            v = text(v, 5000)
        else:
            v = text(v, 1000)
        out[f] = v
    if not partial and not out.get("title"):
        raise BadRequest("اسم پروژه رو بنویس.")
    if partial and "title" in out and not out["title"]:
        raise BadRequest("اسم پروژه نمی‌تونه خالی باشه.")
    return out


def project_of(conn, table, row_id):
    r = conn.execute(f"SELECT project_id FROM {table} WHERE id=?", (row_id,)).fetchone()
    if not r:
        raise BadRequest("پیدا نشد.")
    return r["project_id"]


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def api(method, path, body):
    parts = [p for p in path.split("/") if p][1:]  # drop "api"

    if method == "GET" and parts == ["projects"]:
        return all_projects()

    with _db_lock, db() as conn:
        # --- projects ------------------------------------------------------
        if method == "POST" and parts == ["projects"]:
            p = clean_project(body)
            p.setdefault("kind", "website")
            p.setdefault("status", "active")
            t = now()
            cols = list(p.keys()) + ["created_at", "updated_at"]
            cur = conn.execute(
                f"INSERT INTO projects ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                list(p.values()) + [t, t],
            )
            pid = cur.lastrowid
            stages = [text(s, 200) for s in (body.get("stages") or []) if text(s, 200)][:60]
            conn.executemany(
                "INSERT INTO stages (project_id, title, status, position) VALUES (?,?, 'todo', ?)",
                [(pid, s, i) for i, s in enumerate(stages)],
            )
            return {"ok": True, "id": pid}

        if len(parts) == 2 and parts[0] == "projects" and parts[1].isdigit():
            pid = int(parts[1])
            if method == "PUT":
                p = clean_project(body, partial=True)
                if p:
                    sets = ",".join(f"{k}=?" for k in p)
                    conn.execute(f"UPDATE projects SET {sets} WHERE id=?", list(p.values()) + [pid])
                touch(conn, pid)
                return {"ok": True}
            if method == "DELETE":
                # Children first (works even where foreign keys are off).
                for t in ("stages", "money", "logs"):
                    conn.execute(f"DELETE FROM {t} WHERE project_id=?", (pid,))
                conn.execute("DELETE FROM projects WHERE id=?", (pid,))
                return {"ok": True}

        if len(parts) == 3 and parts[0] == "projects" and parts[1].isdigit() and method == "POST":
            pid = int(parts[1])
            if not conn.execute("SELECT 1 FROM projects WHERE id=?", (pid,)).fetchone():
                raise BadRequest("پروژه پیدا نشد.")
            if parts[2] == "stages":
                title = text(body.get("title"), 200)
                if not title:
                    raise BadRequest("اسم مرحله رو بنویس.")
                pos = conn.execute("SELECT COALESCE(MAX(position),-1)+1 FROM stages WHERE project_id=?", (pid,)).fetchone()[0]
                conn.execute("INSERT INTO stages (project_id,title,status,position) VALUES (?,?, 'todo', ?)", (pid, title, pos))
            elif parts[2] == "money":
                typ = body.get("type")
                if typ not in ("payment", "expense"):
                    raise BadRequest("نوع نامعتبره.")
                conn.execute(
                    "INSERT INTO money (project_id,type,amount,date,note,created_at) VALUES (?,?,?,?,?,?)",
                    (pid, typ, amount(body.get("amount"), allow_zero=False), text(body.get("date"), 40), text(body.get("note"), 500), now()),
                )
            elif parts[2] == "logs":
                t = text(body.get("text"))
                if not t:
                    raise BadRequest("بنویس چه کاری انجام دادی.")
                conn.execute("INSERT INTO logs (project_id,text,created_at) VALUES (?,?,?)", (pid, t, now()))
            else:
                raise BadRequest("مسیر نامعتبر.")
            touch(conn, pid)
            return {"ok": True}

        # --- stages --------------------------------------------------------
        if len(parts) >= 2 and parts[0] == "stages" and parts[1].isdigit():
            sid = int(parts[1])
            pid = project_of(conn, "stages", sid)
            if method == "PUT" and len(parts) == 2:
                st = body.get("status")
                if st not in STAGE_STATUSES:
                    raise BadRequest("وضعیت نامعتبره.")
                conn.execute("UPDATE stages SET status=?, done_at=? WHERE id=?", (st, now() if st == "done" else None, sid))
            elif method == "POST" and len(parts) == 3 and parts[2] == "move":
                d = -1 if body.get("dir") == -1 else 1
                rows = [r["id"] for r in conn.execute("SELECT id FROM stages WHERE project_id=? ORDER BY position, id", (pid,))]
                i = rows.index(sid)
                j = i + d
                if 0 <= j < len(rows):
                    rows[i], rows[j] = rows[j], rows[i]
                    conn.executemany("UPDATE stages SET position=? WHERE id=?", [(k, r) for k, r in enumerate(rows)])
            elif method == "DELETE" and len(parts) == 2:
                conn.execute("DELETE FROM stages WHERE id=?", (sid,))
            else:
                raise BadRequest("مسیر نامعتبر.")
            touch(conn, pid)
            return {"ok": True}

        # --- money / logs --------------------------------------------------
        if method == "DELETE" and len(parts) == 2 and parts[0] in ("money", "logs") and parts[1].isdigit():
            rid = int(parts[1])
            pid = project_of(conn, parts[0], rid)
            conn.execute(f"DELETE FROM {parts[0]} WHERE id=?", (rid,))
            touch(conn, pid)
            return {"ok": True}

    raise BadRequest("مسیر نامعتبر.")


# ---------------------------------------------------------------------------
# Login (only when a password is set: PM_PASSWORD)
# ---------------------------------------------------------------------------

SESSION_COOKIE = "pm_session"
SESSION_DAYS = 30
_fails = {}  # ip -> [timestamps of failed logins]


def password():
    return os.environ.get("PM_PASSWORD", "")


def _secret():
    """Random per-install secret kept in the database (survives restarts)."""
    if ON_VERCEL:
        # Derived from the private Blob token, so logging in never has to save the database.
        # Stable across instances: the read-write token if there is one, else the
        # password itself (it's in the signing key anyway) plus the store id.
        return hashlib.sha256(("pm-session|" + (BLOB_RW_TOKEN or password() + "|" + BLOB_STORE_ID)).encode()).hexdigest()
    with _db_lock, db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        r = conn.execute("SELECT value FROM settings WHERE key='secret'").fetchone()
        if r:
            return r["value"]
        s = secrets.token_hex(32)
        conn.execute("INSERT INTO settings (key, value) VALUES ('secret', ?)", (s,))
        return s


def _sign(expiry):
    # The password is part of the key, so changing it logs every device out.
    key = (_secret() + "|" + password()).encode("utf-8")
    return hmac.new(key, str(expiry).encode(), hashlib.sha256).hexdigest()


def make_session():
    expiry = int(time.time()) + SESSION_DAYS * 86400
    return f"{expiry}.{_sign(expiry)}"


def valid_session(token):
    try:
        expiry, sig = (token or "").split(".", 1)
        return int(expiry) > time.time() and hmac.compare_digest(sig, _sign(int(expiry)))
    except Exception:
        return False


def check_password(given):
    a = hashlib.sha256(given.encode("utf-8")).digest()
    b = hashlib.sha256(password().encode("utf-8")).digest()
    return hmac.compare_digest(a, b)


def too_many_fails(ip):
    now_ = time.time()
    recent = [t for t in _fails.get(ip, []) if now_ - t < 900]
    _fails[ip] = recent
    return len(recent) >= 8


LOGIN_PAGE = r"""<!doctype html>
<html lang="fa" dir="rtl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>ورود — پروژه‌هام</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a0a0a;--panel:#121212;--stroke:#2f2f2f;--ink:#f4f4f3;--dim:#8c8c8a;--accent:#8b5cf6;color-scheme:dark}
@media (prefers-color-scheme:light){:root{--bg:#f2f2f0;--panel:#fff;--stroke:#d2d2d0;--ink:#0a0a0a;--dim:#6b6b69;--accent:#7c3aed;color-scheme:light}}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--ink);font-family:Vazirmatn,Tahoma,sans-serif;padding:16px}
form{width:min(360px,100%);background:var(--panel);border:1px solid var(--stroke);border-radius:20px;padding:28px}
.mark{position:relative;width:44px;height:44px;border-radius:12px;background:var(--ink);margin-bottom:18px}
.mark::after{content:"";position:absolute;width:10px;height:10px;border-radius:50%;background:var(--accent);bottom:-3px;left:-3px;border:3px solid var(--panel)}
h1{margin:0;font-size:22px;font-weight:900}
p{margin:4px 0 20px;color:var(--dim);font-size:13.5px}
input{width:100%;font:inherit;color:inherit;background:transparent;border:1px solid var(--stroke);border-radius:10px;padding:11px 12px;outline:none}
input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(139,92,246,.15)}
button{width:100%;margin-top:12px;font:inherit;font-weight:800;border:0;border-radius:10px;padding:11px;background:var(--ink);color:var(--bg);cursor:pointer}
.err{color:var(--accent);font-size:13px;margin-top:10px;min-height:1em}
</style></head><body>
<form method="post" action="/login">
  <div class="mark"></div>
  <h1>پروژه‌هام</h1>
  <p>برای ورود رمز رو بزن.</p>
  <input type="password" name="password" placeholder="رمز ورود" autofocus required autocomplete="current-password">
  <button>ورود</button>
  <div class="err">__ERR__</div>
</form></body></html>"""


# ---------------------------------------------------------------------------
# WSGI app (used by hosting: PythonAnywhere, cPanel "Setup Python App", …)
# ---------------------------------------------------------------------------

STATUS = {200: "200 OK", 302: "302 Found", 400: "400 Bad Request", 401: "401 Unauthorized",
          404: "404 Not Found", 429: "429 Too Many Requests", 500: "500 Internal Server Error"}

_init_done = False


def setup_error_page(err):
    """Plain page that says what's missing on Vercel, with the real error underneath."""
    esc = lambda t: str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    checks = [
        ("رمز ورود (PM_PASSWORD)", len(password()) >= 8, "توی Settings → Environment Variables متغیر PM_PASSWORD رو بذار (حداقل ۸ کاراکتر)."),
        ("اتصال Blob", bool(blob_token()), "توی تب Storage یه Blob بساز و Connect کن به همین پروژه."),
    ]
    blob_vars = sorted(k for k in os.environ if "BLOB" in k.upper() or "OIDC" in k.upper())
    seen = ("متغیرهای مربوط که برنامه می‌بینه: " + "، ".join(blob_vars)) if blob_vars else "برنامه هیچ متغیری مربوط به Blob نمی‌بینه."
    seen += " | توکن OIDC درخواست: " + ("هست" if _req_oidc else "نیست")
    rows = "".join(
        f"<li><b>{'✔' if ok else '✘'}</b> {name}{'' if ok else '<br><small>' + fix + '</small>'}</li>" for name, ok, fix in checks)
    if isinstance(err, BlobHTTPError):
        if "OIDC is enabled" in str(err.detail):
            hint = "Blob به این محیط (Production/Preview) وصل نیست. توی Storage ← Blob ← Projects، اتصال رو با تیک همه‌ی محیط‌ها دوباره بزن و Redeploy کن."
        elif err.code in (401, 403):
            hint = "توکن Blob قبول نشد. Blob رو یه بار Disconnect و دوباره Connect کن، بعد Redeploy."
        else:
            hint = "Vercel Blob جواب نداد یا خطا داد. چند دقیقه بعد صفحه رو رفرش کن."
    elif isinstance(err, BlobHTTPError) is False and BLOB_STORE_ID and not blob_token():
        hint = "Store به پروژه وصله ولی توکن OIDC نرسید. توی Settings → Security، گزینه‌ی Secure Backend Access with OIDC Federation رو روشن کن و Redeploy بزن."
    elif not blob_token():
        hint = "بعد از Connect کردن Blob، حتماً از تب Deployments یه Redeploy بزن. متغیرهای جدید فقط تو Deploy بعدی اعمال می‌شن."
    else:
        hint = ""
    return f"""<!doctype html><html lang="fa" dir="rtl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex">
<title>تنظیمات ناقصه</title>
<style>body{{margin:0;min-height:100vh;display:grid;place-items:center;background:#0a0a0a;color:#f4f4f3;font-family:Vazirmatn,Tahoma,sans-serif;padding:16px}}
.c{{width:min(560px,100%);background:#121212;border:1px solid #2f2f2f;border-radius:18px;padding:24px;line-height:1.9}}
h1{{margin:0 0 6px;font-size:20px}} ul{{list-style:none;padding:0;margin:14px 0}} li{{margin:8px 0}} small{{color:#8c8c8a}}
.h{{border-right:3px solid #8b5cf6;padding-right:10px;margin:12px 0}} code{{display:block;direction:ltr;text-align:left;background:#0a0a0a;border:1px solid #2f2f2f;border-radius:10px;padding:10px;font-size:12px;color:#8c8c8a;white-space:pre-wrap;word-break:break-all}}</style>
</head><body><div class="c"><h1>برنامه هنوز کامل وصل نشده</h1>
<ul>{rows}</ul>{f'<p class="h">{hint}</p>' if hint else ''}<p><small>{seen}</small></p>
<p><small>متن خطا (برای عیب‌یابی):</small></p><code>{esc(repr(err))[:600]}</code></div></body></html>"""


def application(environ, start_response):
    global _init_done, _req_oidc
    _req_oidc = environ.get("HTTP_X_VERCEL_OIDC_TOKEN", "").strip()
    if not _init_done:
        try:
            with _sync_lock:
                blob_pull()
                init_db()
            _init_done = True
        except Exception as e:  # noqa
            print("خطای دیتابیس:", repr(e), file=sys.stderr)
            body = setup_error_page(e).encode("utf-8")
            start_response("500 Internal Server Error", [("Content-Type", "text/html; charset=utf-8"),
                                                         ("Content-Length", str(len(body))), ("Cache-Control", "no-store"),
                                                         ("X-Robots-Tag", "noindex, nofollow")])
            return [body]

    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO") or "/"
    https = environ.get("wsgi.url_scheme") == "https" or environ.get("HTTP_X_FORWARDED_PROTO", "").startswith("https")
    ip = (environ.get("HTTP_X_FORWARDED_FOR") or environ.get("REMOTE_ADDR") or "").split(",")[0].strip()

    def respond(code, body, ctype="application/json; charset=utf-8", extra=None):
        data = body if isinstance(body, bytes) else (
            body.encode("utf-8") if isinstance(body, str) else json.dumps(body, ensure_ascii=False).encode("utf-8"))
        headers = [("Content-Type", ctype), ("Content-Length", str(len(data))),
                   ("Cache-Control", "no-store"),
                   ("X-Robots-Tag", "noindex, nofollow"),  # keep it out of Google
                   ("X-Frame-Options", "DENY"), ("X-Content-Type-Options", "nosniff"),
                   ("Referrer-Policy", "same-origin")]
        headers += list((extra or {}).items()) if isinstance(extra, dict) else (extra or [])
        start_response(STATUS.get(code, f"{code} OK"), headers)
        return [data]

    def read_body():
        n = int(environ.get("CONTENT_LENGTH") or 0)
        if n > 1_000_000:
            raise BadRequest("درخواست خیلی بزرگه.")
        return environ["wsgi.input"].read(n) if n else b""

    try:
        if path == "/robots.txt":
            return respond(200, "User-agent: *\nDisallow: /\n", "text/plain; charset=utf-8")

        # ---- login gate ------------------------------------------------
        if ON_VERCEL and len(password()) < 8:
            return respond(500, setup_error_page(RuntimeError("PM_PASSWORD is not set")), "text/html; charset=utf-8")
        if password():
            cookies = SimpleCookie(environ.get("HTTP_COOKIE", ""))
            token = cookies[SESSION_COOKIE].value if SESSION_COOKIE in cookies else ""
            secure = "; Secure" if https else ""
            if path == "/login" and method == "POST":
                if too_many_fails(ip):
                    return respond(429, LOGIN_PAGE.replace("__ERR__", "تلاش زیاد بود. ۱۵ دقیقه بعد دوباره امتحان کن."), "text/html; charset=utf-8")
                form = parse_qs(read_body().decode("utf-8", "replace"))
                if check_password((form.get("password") or [""])[0]):
                    _fails.pop(ip, None)
                    cookie = f"{SESSION_COOKIE}={make_session()}; Path=/; Max-Age={SESSION_DAYS * 86400}; HttpOnly; SameSite=Lax{secure}"
                    return respond(302, b"", "text/plain", [("Location", "/"), ("Set-Cookie", cookie)])
                _fails.setdefault(ip, []).append(time.time())
                time.sleep(1.2)
                return respond(401, LOGIN_PAGE.replace("__ERR__", "رمز اشتباهه."), "text/html; charset=utf-8")
            if path == "/logout":
                cookie = f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax{secure}"
                return respond(302, b"", "text/plain", [("Location", "/"), ("Set-Cookie", cookie)])
            if not valid_session(token):
                if path.startswith("/api/"):
                    return respond(401, {"ok": False, "message": "دوباره وارد شو.", "login": True})
                return respond(200, LOGIN_PAGE.replace("__ERR__", ""), "text/html; charset=utf-8")

        # ---- app -----------------------------------------------------------
        if ON_VERCEL and (path.startswith("/api/") or path == "/backup"):
            with _sync_lock:
                if blob_pull():
                    init_db()
                # Hold the response until the save to Blob has worked, so a
                # failed save shows up as an error instead of silently losing data.
                held = []
                handle_app(method, path, environ, lambda *a, **k: held.append((a, k)), read_body)
                if method in ("POST", "PUT", "DELETE") and path.startswith("/api/"):
                    try:
                        blob_push()
                    except Exception:
                        global _blob_url
                        _blob_url = None  # next request reloads the last saved copy
                        raise
                a, k = held[0]
                return respond(*a, **k)
        return handle_app(method, path, environ, respond, read_body)
    except BadRequest as e:
        return respond(400, {"ok": False, "message": str(e)})
    except BlobHTTPError as e:
        print("خطای Blob:", repr(e), file=sys.stderr)
        return respond(500, {"ok": False, "message": f"ذخیره روی Vercel Blob نشد ({e.code}): {e.detail}"[:300]})
    except Exception as e:  # noqa
        print("خطا:", repr(e), file=sys.stderr)
        return respond(500, {"ok": False, "message": "یه مشکلی پیش اومد، دوباره امتحان کن."})


def handle_app(method, path, environ, respond, read_body):
    """Page, backup and API (after the login gate). Raises BadRequest for bad input."""
    if True:
        if method == "GET" and path in ("/", "/index.html"):
            page = PAGE.replace("__LOGOUT__", "1" if password() else "")
            return respond(200, page, "text/html; charset=utf-8")
        if method == "GET" and path == "/backup":
            data = backup_bytes()
            name = "projects-backup-%s.db" % datetime.now().strftime("%Y%m%d-%H%M")
            return respond(200, data, "application/octet-stream",
                           {"Content-Disposition": f'attachment; filename="{name}"'})
        if path.startswith("/api/"):
            body = {}
            if method in ("POST", "PUT"):
                # JSON only: a plain cross-site form can't send this, which
                # (with SameSite cookies) keeps other sites from posting here.
                if "application/json" not in environ.get("CONTENT_TYPE", ""):
                    raise BadRequest("درخواست نامعتبر.")
                body = json.loads(read_body() or b"{}")
                if not isinstance(body, dict):
                    raise BadRequest("درخواست نامعتبر.")
            return respond(200, api(method, path, body))
        return respond(404, {"ok": False, "message": "پیدا نشد."})


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, fmt, *args):
        pass


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


# ---------------------------------------------------------------------------
# Page (HTML + CSS + JS)
# ---------------------------------------------------------------------------

PAGE = r"""<!doctype html>
<html lang="fa" dir="rtl" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>پروژه‌هام</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;800;900&display=swap" rel="stylesheet">
<script>
  try { const t = localStorage.getItem("pm-theme"); if (t) document.documentElement.dataset.theme = t;
        else if (matchMedia("(prefers-color-scheme: light)").matches) document.documentElement.dataset.theme = "light"; } catch (e) {}
</script>
<style>
:root{
  --bg:#0a0a0a; --bg2:#0d0d0d;
  --panel:#121212; --panel2:#181818; --panel3:#232323;
  --stroke:#1e1e1e; --stroke2:#2f2f2f;
  --ink:#f4f4f3; --dim:#8c8c8a; --faint:#545452;
  --inv-bg:#f4f4f3; --inv-ink:#0a0a0a; --inv-dim:#6b6b69; --inv-line:#dcdcda;
  --accent:#8b5cf6; --accent-soft:rgba(139,92,246,.12); --accent-line:rgba(139,92,246,.45);
  --shadow:0 1px 0 rgba(255,255,255,.03) inset,0 24px 48px -28px rgba(0,0,0,.9);
  --grain-op:.05;
  color-scheme:dark;
}
[data-theme="light"]{
  --bg:#f2f2f0; --bg2:#f7f7f5;
  --panel:#ffffff; --panel2:#f6f6f4; --panel3:#e9e9e7;
  --stroke:#e4e4e2; --stroke2:#d2d2d0;
  --ink:#0a0a0a; --dim:#6b6b69; --faint:#a6a6a4;
  --inv-bg:#0a0a0a; --inv-ink:#f4f4f3; --inv-dim:#8c8c8a; --inv-line:#2a2a2a;
  --accent:#7c3aed; --accent-soft:rgba(124,58,237,.08); --accent-line:rgba(124,58,237,.4);
  --shadow:0 1px 2px rgba(0,0,0,.04),0 20px 40px -30px rgba(0,0,0,.25);
  --grain-op:.035;
  color-scheme:light;
}
*{box-sizing:border-box}
html,body{margin:0}
body{background:var(--bg);color:var(--ink);font-family:Vazirmatn,Tahoma,sans-serif;font-size:14px;line-height:1.75;min-height:100vh;overflow-x:hidden;-webkit-font-smoothing:antialiased}
button,input,select,textarea{font:inherit;color:inherit}
svg{flex-shrink:0}
::selection{background:var(--accent);color:#fff}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--panel3);border-radius:10px;border:3px solid var(--bg)}

/* ---------- matte grain ---------- */
.bg{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:var(--grain-op);
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E")}

/* ---------- layout ---------- */
.wrap{max-width:1240px;margin:0 auto;padding:0 20px 80px}
.topbar{position:sticky;top:0;z-index:20;backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  background:color-mix(in srgb,var(--bg) 82%,transparent);border-bottom:1px solid var(--stroke)}
.topbar .in{max-width:1240px;margin:0 auto;padding:12px 20px;display:flex;align-items:center;gap:14px}
.logo{display:flex;align-items:center;gap:10px;font-weight:900;font-size:18px;letter-spacing:-.4px;white-space:nowrap}
.logo-mark{position:relative;width:36px;height:36px;border-radius:10px;background:var(--ink);display:grid;place-items:center;color:var(--bg)}
.logo-mark::after{content:"";position:absolute;width:8px;height:8px;border-radius:50%;background:var(--accent);bottom:-2px;left:-2px;border:2px solid var(--bg)}
.search{flex:1;max-width:420px;margin-inline-start:auto;position:relative}
.search input{width:100%;background:var(--panel);border:1px solid var(--stroke);border-radius:10px;padding:8px 38px 8px 12px;outline:none;transition:border-color .2s}
.search input:focus{border-color:var(--stroke2)}
.search svg{position:absolute;right:12px;top:50%;transform:translateY(-50%);color:var(--dim)}
.actions{display:flex;gap:8px}

.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid var(--stroke2);background:transparent;border-radius:10px;padding:8px 14px;cursor:pointer;font-weight:600;font-size:13px;color:var(--ink);text-decoration:none;transition:background .2s,border-color .2s,color .2s,transform .1s;white-space:nowrap}
.btn:hover{background:var(--panel2)}
.btn:active{transform:scale(.97)}
.btn:disabled{opacity:.4;pointer-events:none}
.btn.icon-only{padding:8px;width:38px;height:38px}
.btn.primary{background:var(--ink);border-color:var(--ink);color:var(--bg);font-weight:800}
.btn.primary:hover{background:color-mix(in srgb,var(--ink) 86%,var(--bg))}
.btn.danger:hover{background:var(--ink);color:var(--bg);border-color:var(--ink)}
.btn.sm{padding:6px 12px;font-size:12.5px;border-radius:9px}

.glass{background:var(--panel);border:1px solid var(--stroke);border-radius:18px;box-shadow:var(--shadow)}

/* ---------- hero ---------- */
.hello{margin:34px 0 20px;display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:10px}
.hello h1{margin:0;font-size:32px;font-weight:900;letter-spacing:-.8px;line-height:1.3}
.hello h1 .g{color:var(--accent)}
.hello p{margin:4px 0 0;color:var(--dim)}
.today{color:var(--dim);font-size:13px;display:flex;align-items:center;gap:6px}

.stats{display:grid;grid-template-columns:repeat(4,1fr);grid-template-rows:auto auto;gap:12px;margin-bottom:30px}
.hero{grid-column:span 2;grid-row:span 2;padding:22px 24px;display:flex;flex-direction:column;background:var(--inv-bg);color:var(--inv-ink);border-color:var(--inv-bg)}
.hero .lbl{color:var(--inv-dim);font-size:13px;display:flex;align-items:center;gap:8px}
.hero .big{font-size:42px;font-weight:900;letter-spacing:-1.5px;margin:2px 0 0;line-height:1.35}
.hero .big small{font-size:15px;font-weight:600;color:var(--inv-dim);letter-spacing:0}
.hero .sub{display:flex;flex-wrap:wrap;gap:18px;font-size:13px;color:var(--inv-dim)}
.hero .sub b{font-weight:800;color:var(--inv-ink)}
.chart{margin-top:auto;padding-top:20px;display:flex;align-items:flex-end;gap:10px;height:150px}
.chart .c{flex:1;display:flex;flex-direction:column;align-items:center;gap:6px;height:100%;justify-content:flex-end;position:relative}
.chart .bar{width:100%;max-width:44px;border-radius:8px 8px 2px 2px;background:var(--inv-ink);min-height:4px;transform-origin:bottom;animation:grow .8s cubic-bezier(.2,.9,.3,1) both;opacity:.88}
.chart .bar.zero{background:var(--inv-line);opacity:1}
.chart .bar.now{background:var(--accent);opacity:1}
.chart .c:hover .bar{opacity:1}
.chart .m{font-size:11.5px;color:var(--inv-dim)}
.chart .tip{position:absolute;bottom:calc(100% + 4px);background:var(--inv-ink);color:var(--inv-bg);font-size:11px;padding:2px 8px;border-radius:6px;white-space:nowrap;opacity:0;transform:translateY(4px);transition:.15s;pointer-events:none;font-weight:600}
.chart .c:hover .tip{opacity:1;transform:none}
@keyframes grow{from{transform:scaleY(0)}}

.stat{padding:16px 18px;display:flex;flex-direction:column;gap:12px;transition:border-color .2s}
.stat:hover{border-color:var(--stroke2)}
.stat .ic{width:36px;height:36px;border-radius:10px;display:grid;place-items:center;background:var(--panel2);border:1px solid var(--stroke);color:var(--ink)}
.stat .k{color:var(--dim);font-size:12.5px}
.stat .v{font-size:22px;font-weight:900;letter-spacing:-.5px;line-height:1.3}
.stat .v small{font-size:12px;font-weight:600;color:var(--dim);letter-spacing:0}
.stat.hl .ic{color:var(--accent);border-color:var(--accent-line);background:var(--accent-soft)}

/* ---------- filters ---------- */
.toolbar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:12px;margin-bottom:16px}
.seg{display:inline-flex;flex-wrap:wrap;gap:3px;padding:4px;border-radius:12px;background:var(--panel);border:1px solid var(--stroke)}
.seg button{border:0;background:none;padding:6px 12px;border-radius:9px;cursor:pointer;color:var(--dim);font-weight:600;font-size:12.5px;display:inline-flex;align-items:center;gap:6px;transition:.15s}
.seg button:hover{color:var(--ink)}
.seg button.on{background:var(--ink);color:var(--bg)}
.seg .n{font-size:11px;opacity:.65}
.select{background:var(--panel);border:1px solid var(--stroke);border-radius:10px;padding:7px 12px;outline:none;cursor:pointer}
.select option{background:var(--bg2);color:var(--ink)}

/* ---------- project cards ---------- */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:12px}
.pcard{position:relative;padding:18px;cursor:pointer;transition:border-color .2s,transform .2s}
.pcard:hover{border-color:var(--stroke2);transform:translateY(-2px)}
.pcard.anim{animation:up .45s cubic-bezier(.2,.8,.2,1) both}
@keyframes up{from{opacity:0;transform:translateY(10px)}}
.ptop{display:flex;gap:12px;align-items:flex-start}
.kicon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;background:var(--panel2);border:1px solid var(--stroke);color:var(--ink)}
.pmeta{flex:1;min-width:0}
.ptitle{margin:0;font-size:16px;font-weight:800;line-height:1.5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;letter-spacing:-.2px}
.pclient{color:var(--dim);font-size:12.5px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.ring{position:relative;width:56px;height:56px}
.ring svg{transform:rotate(-90deg)}
.ring .bgc{stroke:var(--panel3)}
.ring .fg{stroke:var(--accent);stroke-linecap:round;transition:stroke-dashoffset 1s cubic-bezier(.2,.8,.2,1)}
.ring span{position:absolute;inset:0;display:grid;place-items:center;font-size:12.5px;font-weight:800}
.pill{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:600;border-radius:99px;padding:0 10px;border:1px solid var(--stroke2);white-space:nowrap;color:var(--dim)}
.pill .d{width:6px;height:6px;border-radius:50%;background:currentColor}
.s-active{background:var(--ink);color:var(--bg);border-color:var(--ink)}
.s-active .d{background:var(--accent);box-shadow:0 0 0 0 var(--accent);animation:pulse 2s infinite}
.s-paused{color:var(--ink)}
.s-paused .d{background:transparent;border:1.5px solid currentColor}
.s-done{color:var(--dim);background:var(--panel2)}
.s-cancelled{color:var(--faint);text-decoration:line-through}
@keyframes pulse{0%{box-shadow:0 0 0 0 var(--accent-line)}70%{box-shadow:0 0 0 6px transparent}100%{box-shadow:0 0 0 0 transparent}}
.stage-now{margin-top:14px;font-size:13px;display:flex;align-items:center;gap:8px}
.stage-now .lab{color:var(--dim);white-space:nowrap}
.stage-now b{font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.steps{display:flex;gap:3px;margin-top:8px}
.steps i{flex:1;height:5px;border-radius:99px;background:var(--panel3)}
.steps i.done{background:var(--ink)}
.steps i.doing{background:var(--accent)}
.next{margin-top:12px;display:flex;align-items:flex-start;gap:8px;font-size:13px;background:var(--panel2);border:1px solid var(--stroke);border-radius:10px;padding:8px 10px}
.next svg{color:var(--dim);margin-top:4px}
.next .muted{color:var(--faint)}
.pmoney{margin-top:14px}
.pmoney .row{display:flex;justify-content:space-between;font-size:12.5px;gap:8px}
.mbar{height:5px;background:var(--panel3);border-radius:99px;overflow:hidden;margin-top:6px}
.mbar i{display:block;height:100%;border-radius:99px;background:var(--ink);transition:width 1s}
.pfoot{display:flex;gap:6px;flex-wrap:wrap;margin-top:12px}
.chip{display:inline-flex;align-items:center;gap:5px;font-size:11.5px;color:var(--dim);background:transparent;border:1px solid var(--stroke2);border-radius:99px;padding:0 9px}
.chip.warn{color:var(--ink);border-color:var(--ink)}
/* money tones: received = ink, owed = violet, spent = muted */
.green{color:var(--ink)} .amber{color:var(--accent)} .red{color:var(--dim)} .dim{color:var(--dim)}

.empty{text-align:center;padding:70px 20px;color:var(--dim)}
.empty .art{width:96px;height:96px;margin:0 auto 18px;border-radius:26px;background:var(--ink);display:grid;place-items:center;color:var(--bg);position:relative}
.empty .art::after{content:"";position:absolute;width:16px;height:16px;border-radius:50%;background:var(--accent);bottom:-4px;left:-4px;border:4px solid var(--bg)}
.empty h3{color:var(--ink);margin:0 0 4px;font-size:19px;font-weight:900}

/* ---------- detail sheet ---------- */
dialog{border:0;padding:0;background:transparent;color:var(--ink);max-width:none;max-height:none}
dialog::backdrop{background:rgba(0,0,0,.6);backdrop-filter:blur(3px);-webkit-backdrop-filter:blur(3px)}
#sheet{position:fixed;inset:0 0 0 auto;margin:0;height:100vh;width:min(940px,100vw)}
#sheet[open]{animation:slide .32s cubic-bezier(.2,.9,.25,1)}
@keyframes slide{from{transform:translateX(100%)}}
.sheet-in{height:100%;overflow-y:auto;background:var(--bg);border-left:1px solid var(--stroke2)}
.sh-head{position:sticky;top:0;z-index:2;padding:14px 22px;display:flex;align-items:center;gap:10px;backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);background:color-mix(in srgb,var(--bg) 85%,transparent);border-bottom:1px solid var(--stroke)}
.sh-head h2{margin:0;font-size:19px;font-weight:900;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;letter-spacing:-.3px}
.sh-body{padding:20px 22px 40px}
.meta-row{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-bottom:16px}
.meta-row a{color:inherit;text-decoration:none}
.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:16px}
.kpi{padding:12px 14px;border-radius:14px;background:var(--panel);border:1px solid var(--stroke)}
.kpi:first-child{background:var(--inv-bg);color:var(--inv-ink);border-color:var(--inv-bg)}
.kpi:first-child .k{color:var(--inv-dim)}
.kpi .k{font-size:11.5px;color:var(--dim)}
.kpi .v{font-size:16px;font-weight:900;margin-top:2px;letter-spacing:-.3px}
.cols{display:grid;grid-template-columns:1.1fr .9fr;gap:12px}
.col{display:flex;flex-direction:column;gap:12px;min-width:0}
.box{padding:16px;border-radius:16px;background:var(--panel);border:1px solid var(--stroke)}
.box h3{margin:0 0 12px;font-size:14.5px;font-weight:800;display:flex;align-items:center;gap:8px}
.box h3 .cnt{margin-inline-start:auto;font-size:12.5px;color:var(--dim);font-weight:600}
.box h3 svg{color:var(--dim)}
.notes{white-space:pre-wrap;background:var(--panel);border:1px dashed var(--stroke2);border-radius:12px;padding:10px 14px;margin-bottom:16px;font-size:13.5px}
.input{width:100%;background:var(--bg);border:1px solid var(--stroke2);border-radius:10px;padding:9px 12px;outline:none;transition:border-color .2s,box-shadow .2s}
[data-theme="light"] .input{background:var(--panel2)}
.input:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-soft)}
.input::placeholder{color:var(--faint)}
textarea.input{resize:vertical}
.addrow{display:flex;gap:8px}
.hint{font-size:11.5px;color:var(--dim)}

/* timeline */
.tl{list-style:none;margin:0;padding:0;position:relative}
.tl li{position:relative;display:flex;align-items:center;gap:12px;padding:7px 8px;border-radius:12px;transition:background .15s}
.tl li:hover{background:var(--panel2)}
.tl li:not(:last-child)::after{content:"";position:absolute;right:23px;top:38px;bottom:-8px;width:2px;background:var(--stroke2)}
.tl li.done:not(:last-child)::after{background:var(--ink)}
.tl li.cur{background:var(--accent-soft);box-shadow:inset 0 0 0 1px var(--accent-line)}
.node{position:relative;z-index:1;width:32px;height:32px;border-radius:50%;border:1.5px solid var(--stroke2);background:var(--panel);display:grid;place-items:center;cursor:pointer;font-size:11.5px;font-weight:800;color:var(--dim);transition:transform .15s}
.node:hover{transform:scale(1.1)}
.node.done{background:var(--ink);border-color:var(--ink);color:var(--bg)}
.node.doing{border-color:var(--accent);color:var(--accent);border-width:2px;animation:ring 2.2s infinite}
@keyframes ring{0%{box-shadow:0 0 0 0 var(--accent-line)}70%{box-shadow:0 0 0 8px transparent}100%{box-shadow:0 0 0 0 transparent}}
.tl .t{flex:1;min-width:0;font-size:13.5px;line-height:1.5}
.tl .t.done>span{color:var(--dim);text-decoration:line-through;text-decoration-color:var(--faint)}
.tl .t small{display:block;color:var(--dim);font-size:11.5px}
.tl .t small .now{color:var(--accent);font-weight:700}
.tools{display:flex;gap:2px;opacity:0;transition:opacity .15s}
.tl li:hover .tools,.tl li.cur .tools{opacity:1}
@media (hover:none){.tools{opacity:1}}
.ib{border:0;background:none;cursor:pointer;color:var(--dim);width:26px;height:26px;border-radius:7px;display:grid;place-items:center;transition:.15s}
.ib:hover{background:var(--panel3);color:var(--ink)}
.ib.x:hover{background:var(--ink);color:var(--bg)}
.ib:disabled{opacity:.2;pointer-events:none}

/* log + money lists */
.feed{list-style:none;margin:12px 0 0;padding:0;display:flex;flex-direction:column;gap:2px;max-height:340px;overflow-y:auto}
.feed li{display:flex;gap:10px;align-items:flex-start;padding:8px;border-radius:10px}
.feed li:hover{background:var(--panel2)}
.feed .dt{width:7px;height:7px;border-radius:50%;background:var(--ink);margin-top:8px;flex-shrink:0}
.feed li:first-child .dt{background:var(--accent)}
.feed .g{flex:1;min-width:0;white-space:pre-wrap;font-size:13.5px}
.feed small{display:block;color:var(--dim);font-size:11.5px}
.feed .ib{opacity:0}.feed li:hover .ib{opacity:1}
@media (hover:none){.feed .ib{opacity:1}}
.mform{display:grid;grid-template-columns:1.1fr .9fr;gap:8px}
.mform .full{grid-column:1/-1;display:flex;gap:8px}
.mlist{list-style:none;margin:12px 0 0;padding:0}
.mlist li{display:flex;align-items:center;gap:10px;padding:9px 6px;border-top:1px solid var(--stroke);font-size:13px}
.mlist li:first-child{border-top:0}
.mlist .amt{font-weight:800;white-space:nowrap}
.mlist .nt{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mlist .ib{opacity:0}.mlist li:hover .ib{opacity:1}
@media (hover:none){.mlist .ib{opacity:1}}
.donut-wrap{display:flex;align-items:center;gap:18px}
.donut{position:relative;width:110px;height:110px;flex-shrink:0}
.donut svg{transform:rotate(-90deg)}
.donut span{position:absolute;inset:0;display:grid;place-items:center;text-align:center;font-weight:900;font-size:19px;line-height:1.2}
.donut span small{display:block;font-size:10.5px;color:var(--dim);font-weight:600}
.legend{display:flex;flex-direction:column;gap:6px;font-size:12.5px;flex:1}
.legend div{display:flex;align-items:center;gap:8px}
.legend i{width:10px;height:10px;border-radius:3px}
.legend b{margin-inline-start:auto;font-weight:800}

/* form dialog */
#formDlg{width:min(720px,calc(100vw - 24px));margin:auto;max-height:calc(100vh - 32px)}
#formDlg[open]{animation:pop .25s cubic-bezier(.2,.9,.3,1)}
@keyframes pop{from{opacity:0;transform:scale(.97) translateY(8px)}}
.fbox{background:var(--bg);border:1px solid var(--stroke2);border-radius:20px;padding:22px;box-shadow:0 30px 80px -30px rgba(0,0,0,.8)}
.fbox h2{margin:0 0 14px;font-size:20px;font-weight:900;letter-spacing:-.4px}
.form{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.form label>span{display:block;font-size:12.5px;color:var(--dim);margin-bottom:4px;font-weight:600}
.form .full{grid-column:1/-1}
.kinds{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.kinds label{cursor:pointer}
.kinds input{position:absolute;opacity:0;pointer-events:none}
.kinds .opt{display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 6px;border-radius:12px;border:1px solid var(--stroke2);background:transparent;font-size:12.5px;font-weight:600;transition:.15s;color:var(--dim)}
.kinds .opt .kicon{width:34px;height:34px;border-radius:9px}
.kinds .opt:hover{color:var(--ink)}
.kinds input:checked+.opt{background:var(--ink);border-color:var(--ink);color:var(--bg)}
.kinds input:checked+.opt .kicon{background:var(--bg);color:var(--ink);border-color:var(--bg)}
.kinds input:focus-visible+.opt{outline:2px solid var(--accent)}
.frow{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
#formErr{color:var(--accent)}

#toast{position:fixed;bottom:24px;left:50%;transform:translate(-50%,16px);background:var(--ink);color:var(--bg);padding:10px 18px;border-radius:12px;font-size:13px;font-weight:700;opacity:0;transition:.25s cubic-bezier(.2,.9,.3,1);pointer-events:none;z-index:100;display:flex;align-items:center;gap:8px;box-shadow:0 20px 40px -16px rgba(0,0,0,.6)}
#toast.show{opacity:1;transform:translate(-50%,0)}
#toast .d{width:7px;height:7px;border-radius:50%;background:var(--accent)}

@media (max-width:1000px){
  .stats{grid-template-columns:repeat(2,1fr)}
  .hero{grid-column:1/-1;grid-row:auto}
  .cols{grid-template-columns:1fr}
  .kpis{grid-template-columns:repeat(3,1fr)}
}
@media (max-width:640px){
  .wrap{padding:0 16px 60px}
  .topbar .in{padding:10px 16px;flex-wrap:wrap}
  .search{order:3;max-width:none;flex-basis:100%}
  .actions{margin-inline-start:auto}
  .btn .lbl{display:none}
  .hello h1{font-size:26px}
  .hero .big{font-size:32px}
  .cards{grid-template-columns:1fr}
  .kpis{grid-template-columns:repeat(2,1fr)}
  .form{grid-template-columns:1fr}
  .kinds{grid-template-columns:repeat(2,1fr)}
  .sh-head,.sh-body{padding-left:16px;padding-right:16px}
  .sh-head{flex-wrap:wrap}
  .sh-head h2{flex:1 1 calc(100% - 60px);font-size:17px}
  .sh-head .select{flex:1}
  .chart{height:120px;gap:6px}
}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<div class="bg"></div>
<svg width="0" height="0" style="position:absolute" aria-hidden="true">
  <defs>
    <linearGradient id="ringGrad" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#8b5cf6"/><stop offset=".5" stop-color="#6366f1"/><stop offset="1" stop-color="#22d3ee"/></linearGradient>
    <linearGradient id="payGrad" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#10b981"/><stop offset="1" stop-color="#34d399"/></linearGradient>
  </defs>
</svg>

<div class="topbar"><div class="in">
  <div class="logo"><div class="logo-mark" id="logoIcon"></div><span>پروژه‌هام</span></div>
  <div class="search"><input id="search" placeholder="جستجوی پروژه، مشتری، شماره…"><span id="searchIcon"></span></div>
  <div class="actions">
    <button class="btn icon-only" id="themeBtn" title="تغییر تم"></button>
    <a class="btn" href="/backup" id="backupBtn" title="دانلود فایل دیتابیس"></a>
    <button class="btn primary" id="newBtn"></button>
    <a class="btn icon-only" href="/logout" id="logoutBtn" title="خروج" hidden></a>
  </div>
</div></div>

<div class="wrap">
  <div class="hello">
    <div><h1 id="greet"></h1><p id="summaryLine"></p></div>
    <div class="today" id="today"></div>
  </div>
  <div class="stats" id="stats"></div>
  <div class="toolbar">
    <div class="seg" id="kindSeg"></div>
    <select class="select" id="statusFilter"></select>
  </div>
  <div id="list"></div>
</div>

<dialog id="sheet"><div class="sheet-in" id="sheetIn"></div></dialog>

<dialog id="formDlg">
  <div class="fbox">
    <h2 id="dlgTitle"></h2>
    <form id="pform" class="form">
      <div class="full kinds" id="kindPick"></div>
      <label class="full"><span>اسم پروژه *</span><input class="input" name="title" required placeholder="مثلاً سایت فروشگاهی کافه رز"></label>
      <label><span>اسم مشتری</span><input class="input" name="client_name"></label>
      <label><span>شماره‌ی مشتری</span><input class="input" name="client_phone" dir="ltr" inputmode="tel" style="text-align:right"></label>
      <label><span>مبلغ کل قرارداد (تومان)</span><input class="input" name="total_price" dir="ltr" inputmode="numeric" placeholder="25,000,000" style="text-align:right"><span class="hint" data-preview="total_price"></span></label>
      <label><span>وضعیت</span><select class="input" name="status"></select></label>
      <label><span>تاریخ شروع</span><input class="input" name="start_date" placeholder="۱۴۰۵/۰۸/۲۷"></label>
      <label><span>ددلاین تحویل</span><input class="input" name="deadline" placeholder="۱۴۰۵/۰۹/۳۰"></label>
      <label class="full"><span>کار بعدی که باید انجام بدم</span><input class="input" name="next_action" placeholder="مثلاً گرفتن لوگو و متن‌ها از مشتری"></label>
      <label class="full"><span>یادداشت (دامنه، هاست، رمزها، توافق‌ها…)</span><textarea class="input" name="notes" rows="3"></textarea></label>
      <label class="full" id="stagesWrap"><span>مراحل پروژه — هر خط یک مرحله (بعداً هم قابل تغییره)</span><textarea class="input" name="stages" rows="7" style="line-height:1.9"></textarea></label>
      <div class="full frow">
        <button class="btn primary" type="submit" id="saveBtn">ذخیره</button>
        <button class="btn" type="button" id="cancelBtn">انصراف</button>
        <span class="hint red" id="formErr"></span>
      </div>
    </form>
  </div>
</dialog>
<div id="toast"><span class="d"></span><span id="toastMsg"></span></div>

<script>
// ---------------------------------------------------------------- icons
const P = {
  website: '<circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>',
  seo: '<path d="m22 7-8.5 8.5-5-5L2 17"/><path d="M16 7h6v6"/>',
  software: '<path d="m16 18 6-6-6-6M8 6l-6 6 6 6"/>',
  other: '<path d="M12 3l1.9 5.8L20 10l-6.1 1.2L12 17l-1.9-5.8L4 10l6.1-1.2z"/><path d="M19 17l.8 2.2L22 20l-2.2.8L19 23l-.8-2.2L16 20l2.2-.8z"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  search: '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>',
  download: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
  moon: '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>',
  x: '<path d="M18 6 6 18M6 6l12 12"/>',
  up: '<path d="m18 15-6-6-6 6"/>', down: '<path d="m6 9 6 6 6-6"/>',
  trash: '<path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/>',
  edit: '<path d="M12 20h9M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4z"/>',
  user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
  phone: '<path d="M22 16.9v3a2 2 0 0 1-2.2 2 19.8 19.8 0 0 1-8.6-3.1 19.5 19.5 0 0 1-6-6A19.8 19.8 0 0 1 2.1 4.2 2 2 0 0 1 4.1 2h3a2 2 0 0 1 2 1.7c.1.9.4 1.8.7 2.7a2 2 0 0 1-.5 2.1L8 9.8a16 16 0 0 0 6 6l1.3-1.3a2 2 0 0 1 2.1-.4c.9.3 1.8.6 2.7.7a2 2 0 0 1 1.7 2z"/>',
  cal: '<rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/>',
  flag: '<path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1zM4 22v-7"/>',
  arrow: '<path d="M19 12H5M12 19l-7-7 7-7"/>',
  zap: '<path d="M13 2 3 14h9l-1 8 10-12h-9z"/>',
  pause: '<rect x="6" y="4" width="4" height="16" rx="1"/><rect x="14" y="4" width="4" height="16" rx="1"/>',
  file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6M9 13h6M9 17h6"/>',
  hourglass: '<path d="M5 22h14M5 2h14M17 22v-4.2a2 2 0 0 0-.6-1.4L12 12l-4.4 4.4a2 2 0 0 0-.6 1.4V22M7 2v4.2a2 2 0 0 0 .6 1.4L12 12l4.4-4.4a2 2 0 0 0 .6-1.4V2"/>',
  wallet: '<path d="M20 12V8H6a2 2 0 0 1 0-4h12v4"/><path d="M4 6v12a2 2 0 0 0 2 2h14v-4"/><path d="M18 12a2 2 0 0 0 0 4h4v-4z"/>',
  trend: '<path d="m22 7-8.5 8.5-5-5L2 17"/><path d="M16 7h6v6"/>',
  list: '<path d="M9 6h11M9 12h11M9 18h11M4 6h.01M4 12h.01M4 18h.01"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  clock: '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
  card: '<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20"/>',
  receipt: '<path d="M4 2v20l3-2 3 2 3-2 3 2 3-2 3 2V2l-3 2-3-2-3 2-3-2-3 2z"/><path d="M8 9h8M8 13h6"/>',
  target: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
  logout: '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9"/>',
  layers: '<path d="m12 2 10 5-10 5L2 7z"/><path d="m2 17 10 5 10-5M2 12l10 5 10-5"/>',
};
const ic = (n, s = 18, w = 2) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round">${P[n]}</svg>`;

// ---------------------------------------------------------------- data
const KINDS = {
  website: {label: "طراحی سایت", stages: ["جلسه‌ی اول و نیازسنجی","قرارداد و پیش‌پرداخت","دریافت محتوا و اطلاعات از مشتری","دامنه و هاست","طراحی ظاهر (UI)","تأیید طرح توسط مشتری","پیاده‌سازی و برنامه‌نویسی","بارگذاری محتوا","تست و رفع اشکال","تحویل، آموزش و تسویه","پشتیبانی"]},
  seo: {label: "سئو", stages: ["آنالیز اولیه‌ی سایت و رقبا","قرارداد و پیش‌پرداخت","سئوی تکنیکال","تحقیق کلمات کلیدی","سئوی داخلی صفحات (On-page)","تولید محتوا","لینک‌سازی","گزارش ماهانه به مشتری"]},
  software: {label: "نرم‌افزار", stages: ["نیازسنجی و تحلیل","قرارداد و پیش‌پرداخت","طراحی معماری و دیتابیس","طراحی رابط کاربری","توسعه","تست","استقرار (Deploy)","تحویل، آموزش و تسویه","پشتیبانی"]},
  other: {label: "سایر", stages: ["شروع و قرارداد","انجام کار","تحویل و تسویه"]},
};
const STATUSES = {active: "در حال انجام", paused: "منتظر مشتری", done: "تمام‌شده", cancelled: "لغو شده"};
const STAGE_LABEL = {todo: "انجام نشده", doing: "در حال انجام", done: "انجام شد"};
const NEXT_STAGE = {todo: "doing", doing: "done", done: "todo"};
const MONTHS = ["فروردین","اردیبهشت","خرداد","تیر","مرداد","شهریور","مهر","آبان","آذر","دی","بهمن","اسفند"];

const nf = new Intl.NumberFormat("fa-IR");
const fa = n => nf.format(n);
const toman = n => fa(n) + " تومان";
/** Short money: ۱۲٫۵ میلیون */
const short = n => {
  const a = Math.abs(n);
  if (a >= 1e9) return nf.format(+(n / 1e9).toFixed(2)) + " میلیارد";
  if (a >= 1e6) return nf.format(+(n / 1e6).toFixed(1)) + " میلیون";
  if (a >= 1e3) return nf.format(Math.round(n / 1e3)) + " هزار";
  return nf.format(n);
};
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const today = () => new Intl.DateTimeFormat("fa-IR", {year:"numeric", month:"2-digit", day:"2-digit"}).format(new Date());
const fmtDT = iso => new Intl.DateTimeFormat("fa-IR", {dateStyle:"medium", timeStyle:"short"}).format(new Date(iso));
const fmtD = iso => new Intl.DateTimeFormat("fa-IR", {dateStyle:"medium"}).format(new Date(iso));
const toEn = s => String(s ?? "").replace(/[۰-۹]/g, d => "۰۱۲۳۴۵۶۷۸۹".indexOf(d)).replace(/[٠-٩]/g, d => "٠١٢٣٤٥٦٧٨٩".indexOf(d));
const parseAmt = s => { const c = toEn(s).replace(/[,٬،\s]/g, ""); return /^\d+$/.test(c) ? Number(c) : null; };
const sum = l => l.reduce((a, m) => a + (m.amount || 0), 0);
const $ = s => document.querySelector(s);

/** Jalali [year, month(1-12)] of a Date. */
function jym(d) {
  const parts = new Intl.DateTimeFormat("en-u-ca-persian-nu-latn", {year: "numeric", month: "numeric"}).formatToParts(d);
  return [Number(parts.find(p => p.type === "year").value), Number(parts.find(p => p.type === "month").value)];
}
/** Month of a payment: from its typed Jalali date if readable, else when it was recorded. */
function payMonth(m) {
  const x = toEn(m.date).match(/^(\d{4})[\/\-.](\d{1,2})/);
  if (x && +x[2] >= 1 && +x[2] <= 12) return [+x[1], +x[2]];
  return jym(new Date(m.created_at));
}

let projects = [];
let openId = null;
let kindFilter = "all";
let statusFilter = "open";
let editingId = null;
let busy = false;
let firstPaint = true;

function toast(msg, err) {
  const t = $("#toast"); $("#toastMsg").textContent = msg; t.className = "show" + (err ? " err" : "");
  clearTimeout(toast._t); toast._t = setTimeout(() => t.className = err ? "err" : "", 2400);
}

async function call(method, url, body) {
  const r = await fetch(url, {method, headers: {"Content-Type": "application/json"}, body: body ? JSON.stringify(body) : undefined});
  let data = {};
  try { data = await r.json(); } catch (e) {}
  if (r.status === 401 && data.login) { location.reload(); throw new Error(data.message); }
  if (!r.ok) throw new Error(data.message || "خطا در ارتباط با برنامه");
  return data;
}

async function load() {
  projects = await call("GET", "/api/projects");
  render();
}

async function act(method, url, body, okMsg) {
  if (busy) return false;
  busy = true;
  try {
    await call(method, url, body);
    await load();
    if (okMsg) toast(okMsg);
    return true;
  } catch (e) { toast(e.message, true); return false; }
  finally { busy = false; }
}

function stats(p) {
  const received = sum(p.payments), spent = sum(p.expenses);
  const done = p.stages.filter(s => s.status === "done").length, total = p.stages.length;
  let cur = p.stages.findIndex(s => s.status === "doing");
  if (cur < 0) cur = p.stages.findIndex(s => s.status !== "done");
  return {received, spent, remaining: Math.max(0, p.total_price - received), profit: received - spent,
          done, total, pct: total ? Math.round(done / total * 100) : 0, cur, current: cur >= 0 ? p.stages[cur] : null};
}

// ---------------------------------------------------------------- top
function renderTop() {
  const h = new Date().getHours();
  const hi = h < 5 ? "شب بخیر" : h < 12 ? "صبح بخیر" : h < 17 ? "ظهر بخیر" : "عصر بخیر";
  const active = projects.filter(p => p.status === "active");
  $("#greet").innerHTML = `${hi}<span class="g">.</span>`;
  const todo = active.filter(p => p.next_action).length;
  $("#summaryLine").textContent = active.length
    ? `${fa(active.length)} پروژه در حال انجام داری${todo ? ` و ${fa(todo)} کار بعدی منتظرته` : ""}.`
    : "هنوز پروژه‌ی فعالی نداری — یکی بساز!";
  $("#today").innerHTML = ic("cal", 15) + new Intl.DateTimeFormat("fa-IR", {weekday: "long", day: "numeric", month: "long", year: "numeric"}).format(new Date());

  const live = projects.filter(p => p.status !== "cancelled");
  const received = sum(projects.flatMap(p => p.payments));
  const spent = sum(projects.flatMap(p => p.expenses));
  const remaining = live.reduce((a, p) => a + Math.max(0, p.total_price - sum(p.payments)), 0);
  const contract = live.reduce((a, p) => a + p.total_price, 0);
  const paused = projects.filter(p => p.status === "paused").length;

  // last 6 Jalali months of received money
  let [y, m] = jym(new Date());
  const months = [];
  for (let i = 0; i < 6; i++) { months.unshift([y, m]); m--; if (!m) { m = 12; y--; } }
  const per = months.map(([yy, mm]) => projects.flatMap(p => p.payments).filter(x => { const [a, b] = payMonth(x); return a === yy && b === mm; }).reduce((a, x) => a + x.amount, 0));
  const max = Math.max(...per, 1);
  const chart = months.map(([yy, mm], i) => `
    <div class="c"><span class="tip">${toman(per[i])}</span>
      <div class="bar ${per[i] ? (i === 5 ? "now" : "") : "zero"}" style="height:${per[i] ? Math.max(6, per[i] / max * 100) : 4}%;animation-delay:${i * 70}ms"></div>
      <span class="m">${MONTHS[mm - 1]}</span></div>`).join("");

  const small = [
    ["zap", "violet", "در حال انجام", fa(projects.filter(p => p.status === "active").length), "پروژه"],
    ["pause", "amber", "منتظر مشتری", fa(paused), "پروژه"],
    ["file", "cyan", "جمع قراردادها", short(contract), "تومان"],
    ["hourglass", "pink", "طلب از مشتری‌ها", short(remaining), "تومان"],
  ];
  $("#stats").innerHTML = `
    <div class="glass hero">
      <div class="lbl">${ic("wallet", 16)} کل پولی که تا الان گرفتی</div>
      <div class="big">${fa(received)} <small>تومان</small></div>
      <div class="sub">
        <span>هزینه‌ها: <b class="red">${short(spent)}</b></span>
        <span>سود خالص: <b class="${received - spent >= 0 ? "green" : "red"}">${short(received - spent)}</b></span>
      </div>
      <div class="chart">${chart}</div>
    </div>
    ${small.map(([i, t, k, v, u]) => `<div class="glass stat ${t === "pink" ? "hl" : ""}"><div class="ic">${ic(i, 19)}</div>
      <div><div class="k">${k}</div><div class="v">${v} <small>${u}</small></div></div></div>`).join("")}`;

  const counts = {all: projects.length};
  for (const k in KINDS) counts[k] = projects.filter(p => p.kind === k).length;
  $("#kindSeg").innerHTML = [["all", "همه", "layers"]].concat(Object.keys(KINDS).map(k => [k, KINDS[k].label, k]))
    .map(([k, l, i]) => `<button class="${kindFilter === k ? "on" : ""}" data-kind="${k}">${ic(i, 15)} ${l} <span class="n">${fa(counts[k])}</span></button>`).join("");
}

// ---------------------------------------------------------------- cards
function visible() {
  const q = $("#search").value.trim();
  const qe = toEn(q);
  return projects.filter(p => {
    if (kindFilter !== "all" && p.kind !== kindFilter) return false;
    if (statusFilter === "open" && (p.status === "done" || p.status === "cancelled")) return false;
    if (statusFilter !== "all" && statusFilter !== "open" && p.status !== statusFilter) return false;
    if (q) { const hay = `${p.title} ${p.client_name} ${p.client_phone} ${p.notes} ${p.next_action}`; if (!hay.includes(q) && !hay.includes(qe)) return false; }
    return true;
  });
}

function ringSVG(pct, size = 58, r = 24, w = 6) {
  const c = 2 * Math.PI * r;
  return `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
    <circle class="bgc" cx="${size/2}" cy="${size/2}" r="${r}" fill="none" stroke-width="${w}"/>
    <circle class="fg" cx="${size/2}" cy="${size/2}" r="${r}" fill="none" stroke-width="${w}" stroke-dasharray="${c}" stroke-dashoffset="${c * (1 - pct / 100)}"/></svg>`;
}

function cardHTML(p, i) {
  const s = stats(p);
  const paidPct = p.total_price ? Math.min(100, Math.round(s.received / p.total_price * 100)) : 0;
  const steps = p.stages.map(st => `<i class="${st.status === "todo" ? "" : st.status}"></i>`).join("");
  return `<article class="glass pcard k-${p.kind} ${firstPaint ? "anim" : ""}" style="animation-delay:${i * 60}ms" data-open="${p.id}">
    <div class="ptop">
      <div class="kicon">${ic(p.kind, 22)}</div>
      <div class="pmeta">
        <h3 class="ptitle">${esc(p.title)}</h3>
        <div class="pclient">${p.client_name ? `${ic("user", 13)} ${esc(p.client_name)}` : `<span>${KINDS[p.kind]?.label ?? ""}</span>`}
          <span class="pill s-${p.status}"><span class="d"></span>${STATUSES[p.status]}</span></div>
      </div>
      <div class="ring">${ringSVG(s.pct)}<span>${fa(s.pct)}٪</span></div>
    </div>
    <div class="stage-now">${s.total === 0 ? '<span class="lab">مرحله‌ای تعریف نشده</span>'
        : s.current ? `<span class="lab">مرحله‌ی ${fa(s.cur + 1)} از ${fa(s.total)}:</span><b>${esc(s.current.title)}</b>`
        : '<b class="green">همه‌ی مراحل انجام شده ✓</b>'}</div>
    ${s.total ? `<div class="steps">${steps}</div>` : ""}
    <div class="next">${ic("arrow", 15)}<div>${p.next_action ? esc(p.next_action) : '<span class="muted">کار بعدی مشخص نشده</span>'}</div></div>
    <div class="pmoney">
      <div class="row"><span><span class="green" style="font-weight:800">${short(s.received)}</span><span class="dim"> دریافت شده${p.total_price ? " از " + short(p.total_price) : ""}</span></span>
      ${s.remaining ? `<span class="amber">مانده ${short(s.remaining)}</span>` : p.total_price ? '<span class="green">تسویه ✓</span>' : ""}</div>
      ${p.total_price ? `<div class="mbar"><i style="width:${paidPct}%"></i></div>` : ""}
    </div>
    ${p.deadline || p.start_date ? `<div class="pfoot">
      ${p.start_date ? `<span class="chip">${ic("cal", 12)} شروع ${esc(p.start_date)}</span>` : ""}
      ${p.deadline ? `<span class="chip ${p.status === "active" ? "warn" : ""}">${ic("flag", 12)} ددلاین ${esc(p.deadline)}</span>` : ""}</div>` : ""}
  </article>`;
}

function render() {
  renderTop();
  const list = visible();
  $("#list").innerHTML = list.length ? `<div class="cards">${list.map(cardHTML).join("")}</div>`
    : projects.length ? `<div class="empty glass"><h3>چیزی پیدا نشد</h3>پروژه‌ای با این فیلتر وجود نداره.</div>`
    : `<div class="empty"><div class="art">${ic("layers", 52, 1.6)}</div><h3>اولین پروژه‌ت رو بساز</h3>
        <p>پروژه‌های طراحی سایت، سئو و نرم‌افزارت رو اینجا مدیریت کن.</p>
        <button class="btn primary" onclick="openForm(null)">${ic("plus", 16)} پروژه‌ی جدید</button></div>`;
  firstPaint = false;
  if (openId != null) {
    const p = projects.find(x => x.id === openId);
    if (p) renderSheet(p); else closeSheet();
  }
}

// ---------------------------------------------------------------- detail sheet
function renderSheet(p) {
  const s = stats(p);
  const sheet = $("#sheet");
  const keepScroll = $("#sheetIn").scrollTop;
  const paidPct = p.total_price ? Math.min(100, Math.round(s.received / p.total_price * 100)) : 0;
  const C = 2 * Math.PI * 44;

  const stages = p.stages.map((st, i) => `
    <li class="${st.status} ${i === s.cur ? "cur" : ""}">
      <button class="node ${st.status}" data-stage="${st.id}" data-next="${NEXT_STAGE[st.status]}" title="${STAGE_LABEL[st.status]} — بزن تا عوض بشه">${st.status === "done" ? ic("check", 15, 3) : fa(i + 1)}</button>
      <div class="t ${st.status === "done" ? "done" : ""}"><span>${esc(st.title)}</span>
        <small>${i === s.cur ? '<span class="now">● مرحله‌ی فعلی</span> · ' : ""}${STAGE_LABEL[st.status]}${st.status === "done" && st.done_at ? " · " + fmtD(st.done_at) : ""}</small></div>
      <div class="tools">
        <button class="ib" data-move="${st.id}" data-dir="-1" ${i === 0 ? "disabled" : ""} title="بالا">${ic("up", 15)}</button>
        <button class="ib" data-move="${st.id}" data-dir="1" ${i === p.stages.length - 1 ? "disabled" : ""} title="پایین">${ic("down", 15)}</button>
        <button class="ib x" data-delstage="${st.id}" data-title="${esc(st.title)}" title="حذف">${ic("x", 15)}</button>
      </div>
    </li>`).join("");

  const money = type => {
    const isPay = type === "payment", list = isPay ? p.payments : p.expenses;
    return `<div class="box"><h3>${ic(isPay ? "card" : "receipt", 17)} ${isPay ? "پول‌هایی که گرفتم" : "هزینه‌ها"}
        <span class="cnt ${isPay ? "green" : "red"}" style="font-weight:800">${toman(sum(list))}</span></h3>
      <form class="mform" data-money="${type}">
        <div><input class="input" name="amount" dir="ltr" inputmode="numeric" placeholder="مبلغ (تومان)" style="text-align:right"><span class="hint" data-preview="amount"></span></div>
        <input class="input" name="date" value="${today()}" placeholder="تاریخ">
        <div class="full"><input class="input" name="note" placeholder="${isPay ? "بابت (پیش‌پرداخت، قسط دوم…)" : "بابت چی؟ (هاست، دامنه، قالب، فریلنسر…)"}">
          <button class="btn sm">${ic("plus", 14)} ثبت</button></div>
      </form>
      ${list.length ? `<ul class="mlist">${list.slice().reverse().map(m => `
        <li><span class="amt ${isPay ? "green" : "red"}">${isPay ? "+" : "−"} ${toman(m.amount)}</span>
        <span class="nt">${esc(m.note) || '<span class="dim">—</span>'}</span><span class="dim" style="font-size:12px">${esc(m.date)}</span>
        <button class="ib x" data-delmoney="${m.id}" title="حذف">${ic("trash", 14)}</button></li>`).join("")}</ul>` : ""}
    </div>`;
  };

  $("#sheetIn").className = `sheet-in k-${p.kind}`;
  $("#sheetIn").innerHTML = `
    <div class="sh-head">
      <div class="kicon">${ic(p.kind, 22)}</div>
      <h2>${esc(p.title)}</h2>
      <select class="select" data-status="${p.id}">${Object.entries(STATUSES).map(([k, l]) => `<option value="${k}" ${k === p.status ? "selected" : ""}>${l}</option>`).join("")}</select>
      <button class="btn icon-only" data-edit="${p.id}" title="ویرایش مشخصات">${ic("edit", 16)}</button>
      <button class="btn icon-only danger" data-delproject="${p.id}" title="حذف پروژه">${ic("trash", 16)}</button>
      <button class="btn icon-only" data-close title="بستن">${ic("x", 18)}</button>
    </div>
    <div class="sh-body">
      <div class="meta-row">
        <span class="pill s-${p.status}"><span class="d"></span>${STATUSES[p.status]}</span>
        <span class="chip">${KINDS[p.kind]?.label ?? ""}</span>
        ${p.client_name ? `<span class="chip">${ic("user", 12)} ${esc(p.client_name)}</span>` : ""}
        ${p.client_phone ? `<a class="chip" href="tel:${esc(p.client_phone)}" dir="ltr">${ic("phone", 12)} ${esc(p.client_phone)}</a>` : ""}
        ${p.start_date ? `<span class="chip">${ic("cal", 12)} شروع ${esc(p.start_date)}</span>` : ""}
        ${p.deadline ? `<span class="chip warn">${ic("flag", 12)} ددلاین ${esc(p.deadline)}</span>` : ""}
        <span class="chip">${ic("clock", 12)} آخرین تغییر ${fmtDT(p.updated_at)}</span>
      </div>
      ${p.notes ? `<div class="notes">${esc(p.notes)}</div>` : ""}
      <div class="kpis">
        <div class="kpi"><div class="k">مبلغ قرارداد</div><div class="v">${p.total_price ? short(p.total_price) : "—"}</div></div>
        <div class="kpi"><div class="k">دریافت شده</div><div class="v green">${short(s.received)}</div></div>
        <div class="kpi"><div class="k">مانده‌ی طلب</div><div class="v ${s.remaining ? "amber" : ""}">${short(s.remaining)}</div></div>
        <div class="kpi"><div class="k">هزینه‌ها</div><div class="v red">${short(s.spent)}</div></div>
        <div class="kpi"><div class="k">سود تا الان</div><div class="v ${s.profit >= 0 ? "green" : "red"}">${short(s.profit)}</div></div>
      </div>
      <div class="cols">
        <div class="col">
          <div class="box"><h3>${ic("target", 17)} کار بعدی که باید انجام بدم</h3>
            <form class="addrow" data-next="${p.id}">
              <input class="input" name="next_action" value="${esc(p.next_action)}" placeholder="مثلاً: ارسال طرح اولیه برای مشتری">
              <button class="btn sm">ذخیره</button></form></div>
          <div class="box"><h3>${ic("list", 17)} مراحل کار <span class="cnt">${fa(s.done)} از ${fa(s.total)} · ${fa(s.pct)}٪</span></h3>
            <div class="hint" style="margin:-6px 0 8px">روی دایره‌ی هر مرحله بزن: انجام نشده ← در حال انجام ← انجام شد</div>
            ${stages ? `<ol class="tl">${stages}</ol>` : '<div class="hint">هنوز مرحله‌ای تعریف نشده.</div>'}
            <form class="addrow" style="margin-top:10px" data-addstage="${p.id}"><input class="input" name="title" placeholder="مرحله‌ی جدید…"><button class="btn sm">${ic("plus", 14)} افزودن</button></form>
          </div>
          <div class="box"><h3>${ic("check", 17)} کارهایی که انجام دادم <span class="cnt">${fa(p.log.length)}</span></h3>
            <form class="addrow" data-addlog="${p.id}"><input class="input" name="text" placeholder="مثلاً: صفحه‌ی اصلی طراحی و برای مشتری ارسال شد"><button class="btn sm">ثبت</button></form>
            ${p.log.length ? `<ul class="feed">${p.log.slice().reverse().map(l => `
              <li><span class="dt"></span><div class="g">${esc(l.text)}<small>${fmtDT(l.created_at)}</small></div>
              <button class="ib x" data-dellog="${l.id}" title="حذف">${ic("trash", 14)}</button></li>`).join("")}</ul>` : ""}
          </div>
        </div>
        <div class="col">
          <div class="box"><h3>${ic("trend", 17)} وضعیت مالی</h3>
            <div class="donut-wrap">
              <div class="donut"><svg width="110" height="110" viewBox="0 0 110 110">
                <circle cx="55" cy="55" r="44" fill="none" stroke="var(--panel3)" stroke-width="12"/>
                <circle cx="55" cy="55" r="44" fill="none" stroke="var(--ink)" stroke-width="12" stroke-linecap="round" stroke-dasharray="${C}" stroke-dashoffset="${C * (1 - paidPct / 100)}" style="transition:stroke-dashoffset 1s"/></svg>
                <span><div>${fa(paidPct)}٪<small>دریافت شده</small></div></span></div>
              <div class="legend">
                <div><i style="background:var(--ink)"></i>دریافتی<b class="green">${short(s.received)}</b></div>
                <div><i style="background:var(--accent)"></i>مانده<b class="amber">${short(s.remaining)}</b></div>
                <div><i style="background:var(--faint)"></i>هزینه<b class="red">${short(s.spent)}</b></div>
                <div><i style="background:transparent;border:1.5px solid var(--ink)"></i>سود<b>${short(s.profit)}</b></div>
              </div>
            </div>
            ${p.total_price ? "" : '<div class="hint" style="margin-top:8px">مبلغ قرارداد ثبت نشده — از دکمه‌ی ویرایش اضافه‌ش کن تا درصد درست حساب بشه.</div>'}
          </div>
          ${money("payment")}
          ${money("expense")}
        </div>
      </div>
    </div>`;
  if (!sheet.open) { sheet.showModal(); $("#sheetIn").scrollTop = 0; }
  else $("#sheetIn").scrollTop = keepScroll;
}

function openSheet(id) { openId = id; const p = projects.find(x => x.id === id); if (p) renderSheet(p); }
function closeSheet() { openId = null; if ($("#sheet").open) $("#sheet").close(); }
$("#sheet").addEventListener("click", e => { if (e.target === $("#sheet")) closeSheet(); });
$("#sheet").addEventListener("close", () => { openId = null; });

// ---------------------------------------------------------------- events
$("#logoIcon").innerHTML = ic("layers", 19, 2.2);
if ("__LOGOUT__" === "1") { $("#logoutBtn").hidden = false; $("#logoutBtn").innerHTML = ic("logout", 16); }
$("#searchIcon").innerHTML = ic("search", 16);
$("#backupBtn").innerHTML = ic("download", 16) + '<span class="lbl">بکاپ</span>';
$("#newBtn").innerHTML = ic("plus", 16, 2.5) + '<span class="lbl">پروژه‌ی جدید</span>';
function paintTheme() { $("#themeBtn").innerHTML = ic(document.documentElement.dataset.theme === "light" ? "moon" : "sun", 17); }
paintTheme();
$("#themeBtn").onclick = () => {
  const t = document.documentElement.dataset.theme === "light" ? "dark" : "light";
  document.documentElement.dataset.theme = t;
  try { localStorage.setItem("pm-theme", t); } catch (e) {}
  paintTheme();
};
$("#statusFilter").innerHTML = `<option value="open">پروژه‌های باز</option><option value="all">همه‌ی وضعیت‌ها</option>` +
  Object.entries(STATUSES).map(([k, l]) => `<option value="${k}">${l}</option>`).join("");
$("#statusFilter").onchange = e => { statusFilter = e.target.value; render(); };
$("#search").oninput = render;
$("#kindSeg").onclick = e => { const b = e.target.closest("[data-kind]"); if (b) { kindFilter = b.dataset.kind; render(); } };
$("#list").onclick = e => { const c = e.target.closest("[data-open]"); if (c) openSheet(Number(c.dataset.open)); };

document.addEventListener("input", e => {
  const f = e.target.closest("form"); if (!f) return;
  const pv = f.querySelector(`[data-preview="${e.target.name}"]`);
  if (pv) { const n = parseAmt(e.target.value); pv.textContent = n ? toman(n) : ""; }
});

$("#sheetIn").addEventListener("click", e => {
  const t = e.target.closest("button"); if (!t) return;
  const d = t.dataset;
  if ("close" in d) return closeSheet();
  if (d.stage) return act("PUT", `/api/stages/${d.stage}`, {status: d.next});
  if (d.move) return act("POST", `/api/stages/${d.move}/move`, {dir: Number(d.dir)});
  if (d.delstage) { if (confirm(`مرحله‌ی «${d.title}» حذف بشه؟`)) act("DELETE", `/api/stages/${d.delstage}`, null, "مرحله حذف شد"); return; }
  if (d.delmoney) { if (confirm("این مورد حذف بشه؟")) act("DELETE", `/api/money/${d.delmoney}`, null, "حذف شد"); return; }
  if (d.dellog) { if (confirm("این مورد حذف بشه؟")) act("DELETE", `/api/logs/${d.dellog}`, null, "حذف شد"); return; }
  if (d.edit) return openForm(projects.find(p => p.id === Number(d.edit)));
  if (d.delproject) {
    const p = projects.find(x => x.id === Number(d.delproject));
    if (confirm(`پروژه‌ی «${p.title}» با همه‌ی مراحل و پرداخت‌هاش حذف بشه؟ برگشت‌پذیر نیست.`)) {
      closeSheet(); act("DELETE", `/api/projects/${p.id}`, null, "پروژه حذف شد");
    }
  }
});
$("#sheetIn").addEventListener("change", e => {
  const id = e.target.dataset.status;
  if (id) act("PUT", `/api/projects/${id}`, {status: e.target.value}, "وضعیت عوض شد");
});
$("#sheetIn").addEventListener("submit", e => {
  e.preventDefault();
  const f = e.target, d = f.dataset, v = Object.fromEntries(new FormData(f));
  if (d.next) return act("PUT", `/api/projects/${d.next}`, {next_action: v.next_action}, "ذخیره شد");
  if (d.addstage) { if (v.title.trim()) act("POST", `/api/projects/${d.addstage}/stages`, v, "مرحله اضافه شد"); return; }
  if (d.addlog) { if (v.text.trim()) act("POST", `/api/projects/${d.addlog}/logs`, v, "ثبت شد"); return; }
  if (d.money) act("POST", `/api/projects/${openId}/money`, {...v, type: d.money}, d.money === "payment" ? "دریافتی ثبت شد" : "هزینه ثبت شد");
});

// ---------------------------------------------------------------- new / edit form
const form = $("#pform");
$("#kindPick").innerHTML = Object.entries(KINDS).map(([k, x]) => `
  <label class="k-${k}"><input type="radio" name="kind" value="${k}"><span class="opt"><span class="kicon">${ic(k, 18)}</span>${x.label}</span></label>`).join("");
form.status.innerHTML = Object.entries(STATUSES).map(([k, l]) => `<option value="${k}">${l}</option>`).join("");
$("#kindPick").addEventListener("change", () => { if (!editingId) form.stages.value = KINDS[form.kind.value].stages.join("\n"); });

function openForm(p) {
  editingId = p ? p.id : null;
  form.reset();
  $("#formErr").textContent = "";
  $("#dlgTitle").textContent = p ? "ویرایش مشخصات پروژه" : "پروژه‌ی جدید";
  $("#stagesWrap").style.display = p ? "none" : "";
  const v = p || {kind: "website", status: "active", start_date: today()};
  for (const k of ["title","client_name","client_phone","status","start_date","deadline","next_action","notes"]) form[k].value = v[k] ?? "";
  form.querySelector(`[name=kind][value="${v.kind}"]`).checked = true;
  form.total_price.value = p && p.total_price ? p.total_price : "";
  form.querySelector('[data-preview="total_price"]').textContent = p && p.total_price ? toman(p.total_price) : "";
  if (!p) form.stages.value = KINDS.website.stages.join("\n");
  $("#formDlg").showModal();
  form.title.focus();
}
$("#newBtn").onclick = () => openForm(null);
$("#cancelBtn").onclick = () => $("#formDlg").close();
$("#formDlg").addEventListener("click", e => { if (e.target === $("#formDlg")) $("#formDlg").close(); });
form.addEventListener("submit", async e => {
  e.preventDefault();
  const v = Object.fromEntries(new FormData(form));
  const stages = v.stages; delete v.stages;
  $("#saveBtn").disabled = true;
  try {
    let newId = null;
    if (editingId) await call("PUT", `/api/projects/${editingId}`, v);
    else newId = (await call("POST", "/api/projects", {...v, stages: (stages || "").split("\n")})).id;
    $("#formDlg").close();
    await load();
    toast(editingId ? "ذخیره شد" : "پروژه ساخته شد");
    if (newId) openSheet(newId);
  } catch (err) { $("#formErr").textContent = err.message; }
  finally { $("#saveBtn").disabled = false; }
});
document.addEventListener("keydown", e => {
  if ((e.key === "n" || e.key === "ن") && !e.target.closest("input,textarea,select") && !document.querySelector("dialog[open]")) { e.preventDefault(); openForm(null); }
  if (e.key === "/" && !e.target.closest("input,textarea")) { e.preventDefault(); $("#search").focus(); }
});

load().catch(e => { $("#list").innerHTML = `<div class="empty glass">${esc(e.message)}</div>`; });
</script>
</body>
</html>
"""


def main():
    init_db()
    try:
        server = make_server(HOST, PORT, application, server_class=_ThreadingWSGIServer, handler_class=_QuietHandler)
    except OSError:
        print(f"پورت {PORT} آزاد نیست. یه پورت دیگه امتحان کن، مثلاً:  PM_PORT=8800 python app.py")
        sys.exit(1)
    url = f"http://127.0.0.1:{PORT}"
    print("مدیریت پروژه‌ها اجرا شد:", url)
    print("فایل دیتابیس:", DB_PATH)
    print("رمز ورود:", "فعاله" if password() else "نداره (فقط روی همین کامپیوتر استفاده کن)")
    print("برای بستن: Ctrl+C")
    if not os.environ.get("PM_NO_BROWSER"):
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nبسته شد.")


if __name__ == "__main__":
    main()


# Some hosts look for a WSGI object named `app`.
app = application
