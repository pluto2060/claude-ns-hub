#!/usr/bin/env python3
"""
Hub server — unified portal aggregating CTX, Entity Corpus, and North Star.
North Star is a first-class built-in page (multi-project manager), not an iframe.
"""
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import sqlite3
import yaml as _yaml

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File, Form, BackgroundTasks, Body
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
import httpx
try:
    import ptyprocess
    _HAS_PTY = True
except ImportError:
    ptyprocess = None  # type: ignore[assignment]
    _HAS_PTY = False  # Windows: ptyprocess requires fcntl (Linux-only)

HERE = Path(__file__).parent
STATIC = HERE / "static"
# M712: data dir migrated to ~/.hub/ — Claude Code independent (like .hermes convention)
_HUB_DATA_DIR = Path(os.environ.get("HUB_DATA_DIR", str(Path.home() / ".hub")))
_DEFAULT_PROJECTS_DIR = _HUB_DATA_DIR / "projects"
PROJECTS_DIR = Path(os.environ.get("HUB_PROJECTS_DIR", str(_DEFAULT_PROJECTS_DIR)))

HOST = os.environ.get("HUB_HOST", "0.0.0.0")
PORT = int(os.environ.get("HUB_PORT", "9000"))

# M705: Per-user config — agent/model defaults that survive hub reinstalls
_HUB_CONFIG_FILE = _HUB_DATA_DIR / "config.yaml"

# M215: Turso (libSQL cloud) dual-write sync
# M929/M1051: telemetry Turso — token must be set via env (never hardcode in public repo)
_TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "").replace("libsql://", "https://")
_TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")
_TURSO_ENABLED = bool(_TURSO_URL and _TURSO_TOKEN)

def _turso_execute(sql: str, args: list = None) -> bool:
    """Execute a single SQL statement on Turso via HTTP API. Returns True on success."""
    if not _TURSO_ENABLED:
        return False
    try:
        stmt = {"sql": sql}
        if args:
            stmt["args"] = [{"type": "text", "value": str(a)} if a is not None else {"type": "null"} for a in args]
        r = httpx.post(
            f"{_TURSO_URL}/v2/pipeline",
            headers={"Authorization": f"Bearer {_TURSO_TOKEN}", "Content-Type": "application/json"},
            json={"requests": [{"type": "execute", "stmt": stmt}]},
            timeout=5.0
        )
        result = r.json().get("results", [{}])[0]
        return result.get("type") == "ok"
    except Exception:
        return False

def _turso_init_schema():
    """Create stones table in Turso if it doesn't exist."""
    _turso_execute("""
        CREATE TABLE IF NOT EXISTS stones (
            proj_id TEXT NOT NULL,
            stone_id TEXT NOT NULL,
            status TEXT,
            text TEXT,
            claude_ack TEXT,
            held INTEGER DEFAULT 0,
            done INTEGER DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (proj_id, stone_id)
        )
    """)

def _turso_sync_project(proj_id: str, milestones: list):
    """M215 next-step: Upsert milestones to Turso using full data_json from milestones_store.
    Falls back to limited-field sync if Turso stones table schema is old."""
    if not _TURSO_ENABLED or not milestones:
        return
    from datetime import datetime as _dt
    import json as _j
    now = _dt.now().isoformat()
    for m in milestones:
        if not isinstance(m, dict) or not m.get("id"):
            continue
        # Try full-JSON upsert first (new schema with data_json column)
        try:
            _turso_execute(
                "INSERT OR REPLACE INTO stones (proj_id, stone_id, status, text, claude_ack, held, done, data_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [proj_id, m.get("id"), m.get("status", ""), (m.get("text") or "")[:500],
                 m.get("claude_ack"), 1 if m.get("held") else 0, 1 if m.get("done") else 0,
                 _j.dumps(m, ensure_ascii=False), now]
            )
        except Exception:
            # Fallback: old schema without data_json column
            try:
                _turso_execute(
                    "INSERT OR REPLACE INTO stones (proj_id, stone_id, status, text, claude_ack, held, done, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [proj_id, m.get("id"), m.get("status", ""), (m.get("text") or "")[:500],
                     m.get("claude_ack"), 1 if m.get("held") else 0, 1 if m.get("done") else 0, now]
                )
            except Exception:
                pass


def _tailscale_interface_ip() -> str:
    """Get the IP assigned to the Tailscale interface (100.x.x.x/32)."""
    try:
        r = subprocess.run(["ip", "addr", "show"], capture_output=True, text=True, timeout=2)
        m = re.search(r"(100\.\d+\.\d+\.\d+)/32", r.stdout)
        if m:
            return m.group(1)
    except Exception:
        pass
    # Windows / psutil fallback
    try:
        import psutil
        for iface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.family == 2 and re.match(r"100\.\d+\.\d+\.\d+", addr.address):
                    return addr.address
    except Exception:
        pass
    return "127.0.0.1"


def _bound_ip(port: int) -> str:
    try:
        r = subprocess.run(["ss", "-tlnp"], capture_output=True, text=True, timeout=2)
        for line in r.stdout.splitlines():
            if f":{port}" in line and "LISTEN" in line:
                m = re.search(r"(\d+\.\d+\.\d+\.\d+):" + str(port), line)
                if m:
                    ip = m.group(1)
                    # 0.0.0.0 means all interfaces — use Tailscale IP so remote clients can reach it
                    return _tailscale_interface_ip() if ip == "0.0.0.0" else ip
    except Exception:
        pass
    # Windows fallback via psutil
    try:
        import psutil
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr.port == port and conn.status == "LISTEN":
                ip = conn.laddr.ip
                return _tailscale_interface_ip() if ip in ("0.0.0.0", "::") else ip
    except Exception:
        pass
    return "127.0.0.1"


def _ctx_url() -> str:
    return "/ctx"


def _corpus_url() -> str:
    # M275: entity-corpus dashboard at port 8989 (entity/dashboard/server.py)
    ip = _bound_ip(8989)
    return f"http://{ip}:8989"


SERVICES = {
    "ctx":    {"port": 8787, "label": "CTX",    "url": _ctx_url()},
    "corpus": {"port": 8989, "label": "Corpus", "url": _corpus_url()},
}

app = FastAPI(title="Hub", version="1.0.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)  # M515: compress large JSON (764KB→~80KB)

# M210 follow-up: when this app is served over plain HTTP, redirect every request to the
# HTTPS endpoint so the browser unlocks the Notification API. Same uvicorn process can
# run two instances (HTTP:9000 → redirect, HTTPS:9443 → serve normally); the request.url.scheme
# is "http" only on the HTTP listener, so this middleware no-ops on the HTTPS listener.
#
# DISABLED: Redirect breaks PATCH/POST for users without Tailscale. For new users,
# HTTP-only mode is sufficient. Re-enable only for Tailscale-enabled deployments.
from fastapi import Request
from fastapi.responses import RedirectResponse

_HTTPS_HOST = "desk-1-1.tailb5ab18.ts.net"
_HTTPS_PORT = 9443

# @app.middleware("http")
# async def _force_https_redirect(request: Request, call_next):
#     if request.url.scheme == "http":
#         target = f"https://{_HTTPS_HOST}:{_HTTPS_PORT}{request.url.path}"
#         if request.url.query:
#             target += f"?{request.url.query}"
#         return RedirectResponse(url=target, status_code=302)
#     return await call_next(request)

@app.get("/static/northstar.html")
async def _northstar_static_nocache():
    return FileResponse(str(STATIC / "northstar.html"),
                        headers={"Cache-Control": "no-store, no-cache, must-revalidate",
                                 "Pragma": "no-cache"})

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

# M60: hub-ctx fully integrated — mount CTX dashboard directly (no separate port)
def _mount_ctx_dashboard():
    """Mount CTX dashboard as a sub-app at /ctx. No subprocess, no port 8787."""
    candidates = [
        Path.home() / ".hub" / "ctx-dashboard" / "server.py",
        Path.home() / ".claude" / "hooks" / "ctx-dashboard" / "server.py",
        Path("/home/desk-1/Project/CTX/src/dashboard/server.py"),
    ]
    server_path = next((p for p in candidates if p.exists()), None)
    if not server_path:
        return
    import importlib.util as _ilu
    spec = _ilu.spec_from_file_location("ctx_dashboard", server_path)
    mod = _ilu.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
        app.mount("/ctx", mod.app)
        print(f"[hub] CTX dashboard mounted at /ctx from {server_path}", file=sys.stderr)
    except Exception as e:
        print(f"[hub] CTX dashboard mount failed: {e}", file=sys.stderr)

_mount_ctx_dashboard()


# M698: log all write API requests + errors to action_log for full observability
@app.middleware("http")
async def _api_request_logger(request: Request, call_next):
    _skip_log = request.method == "GET" or request.url.path.startswith("/static") or request.url.path.startswith("/uploads")
    _t0 = time.time()
    response = await call_next(request)
    _ms = int((time.time() - _t0) * 1000)
    # M1002 fix: force no-store on HTML so clients (e.g. other Tailscale devices)
    # always get the latest UI. The explicit no-store routes for northstar.html are
    # shadowed by the StaticFiles("/static") mount, which serves with etag/last-modified
    # caching — leaving stale UIs on already-cached browsers. Stamping it here at the
    # middleware layer catches every path (mount + routes) uniformly.
    _p = request.url.path
    if _p.endswith(".html") or _p in ("/", "/northstar"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        # drop validators so browsers can't 304-revalidate into a stale cache
        for _h in ("etag", "last-modified"):
            if _h in response.headers:
                del response.headers[_h]
    if not _skip_log or response.status_code >= 400:
        _action = f"api:{request.method}"
        _path = request.url.path
        _detail = f"{_path} → {response.status_code} ({_ms}ms)"
        # extract proj_id from path if possible
        _parts = _path.strip("/").split("/")
        _proj = _parts[3] if len(_parts) > 3 and _parts[1] == "northstar" else ""
        _server_log_action(_proj, "", _action, _detail)
    return response


@app.get("/")
async def index():
    return FileResponse(str(STATIC / "index.html"))


@app.get("/config")
async def config():
    ctx_url    = _ctx_url()
    corpus_url = _corpus_url()
    return JSONResponse({
        "ctx_url":            ctx_url,
        "corpus_url":         corpus_url,
        "northstar_url":      "/northstar",
        "market_signals_url": "/market-signals",
        "ctx_ip":             ctx_url.split("//")[1].split(":")[0] if "//" in ctx_url else "127.0.0.1",
        "ntfy_topic_set":     bool(_get_telegram_config()[0]),
    })

@app.get("/api/hub/defaults")
async def hub_defaults():
    """M572: Return default paths for UI helpers (set-dir pre-fill, etc.)."""
    import os as _os
    home = _os.path.expanduser("~")
    # Detect common project base: ~/Project if it exists, else ~/projects, else home
    candidates = [_os.path.join(home, "Project"), _os.path.join(home, "projects"), home]
    projects_base = next((c for c in candidates if _os.path.isdir(c)), home)
    return JSONResponse({"home": home, "projects_base": projects_base})


@app.get("/api/user-settings/{key}")
async def user_settings_get(key: str):
    """M785: Get a centralized user setting (e.g. model_avatars) — replaces per-device localStorage."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row = conn.execute("SELECT value_json FROM user_settings WHERE key=?", (key,)).fetchone()
        conn.close()
        if not row:
            return JSONResponse({"key": key, "value": None})
        return JSONResponse({"key": key, "value": json.loads(row[0])})
    except Exception as e:
        return JSONResponse({"key": key, "value": None, "error": str(e)}, status_code=500)


@app.put("/api/user-settings/{key}")
async def user_settings_put(key: str, req: Request):
    """M785: Set a centralized user setting. Body: {value: <any-json>}."""
    import datetime as _dt_us
    try:
        body = await req.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
    value = body.get("value")
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute(
            "INSERT OR REPLACE INTO user_settings(key, value_json, updated_at) VALUES(?,?,?)",
            (key, json.dumps(value, ensure_ascii=False), _dt_us.datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
        return JSONResponse({"ok": True, "key": key})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def _mark_blink_server(proj_id: str, s_key: str, mid: str):
    """M1007: server-side blink trigger. When a stone's status changes via the
    PATCH path (incl. exec-session API completions where no browser is watching),
    stamp the affected substar's blink mids+ts into user_settings.blink_state_{proj}
    with ts=now so the client's _blinkCenterHydrate restores a fresh, in-window blink
    on the next board open. Mirrors the client-side _blinkCenterSave bundle shape
    ({sKey: {ts, mids:[...]}}). Root cause of M1007: blink was client-detected only,
    so API-driven changes never produced hydratable blink state."""
    import time as _t_blink
    key = f"blink_state_{proj_id}"
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row = conn.execute("SELECT value_json FROM user_settings WHERE key=?", (key,)).fetchone()
        bundle = {}
        if row and row[0]:
            try:
                bundle = json.loads(row[0])
            except Exception:
                bundle = {}
        if not isinstance(bundle, dict):
            bundle = {}
        _now_ms = int(_t_blink.time() * 1000)
        # M1007 v2: prune entries older than 48h (well beyond the max 24h display
        # window) — client is read-only now and no longer clears expired entries,
        # so the server must self-prune to avoid unbounded growth.
        _prune_cutoff = _now_ms - 48 * 3600 * 1000
        bundle = {k: e for k, e in bundle.items()
                  if isinstance(e, dict) and (e.get("ts") or 0) >= _prune_cutoff}
        entry = bundle.get(s_key) if isinstance(bundle.get(s_key), dict) else {}
        mids = entry.get("mids") if isinstance(entry.get("mids"), list) else []
        if mid not in mids:
            mids = mids + [mid]
        bundle[s_key] = {"ts": _now_ms, "mids": mids}
        conn.execute(
            "INSERT OR REPLACE INTO user_settings(key, value_json, updated_at) VALUES(?,?,?)",
            (key, json.dumps(bundle, ensure_ascii=False), __import__('datetime').datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# M1002: Claude Code remaining-usage badge — undocumented OAuth usage endpoint,
# 5-min cached, fetched in a thread so the blocking urlopen never stalls the event loop.
_USAGE_CACHE: dict = {"data": None, "ts": 0.0}
_USAGE_TTL = 300  # 5 min

def _fetch_usage_blocking():
    """Runs off the event loop. Reads the OAuth token Claude Code maintains in
    ~/.claude/.credentials.json and queries the (undocumented) usage endpoint."""
    import urllib.request as _ur
    cred = Path.home() / ".claude" / ".credentials.json"
    if not cred.exists():
        return {"error": "no credentials"}
    try:
        tok = json.loads(cred.read_text()).get("claudeAiOauth", {}).get("accessToken")
    except Exception:
        return {"error": "credential parse failed"}
    if not tok:
        return {"error": "no access token"}
    try:
        req = _ur.Request(
            "https://api.anthropic.com/api/oauth/usage",
            headers={"Authorization": f"Bearer {tok}", "anthropic-beta": "oauth-2025-04-20"},
        )
        with _ur.urlopen(req, timeout=8) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"error": str(e)[:140]}

@app.get("/api/claude-usage")
async def claude_usage():
    """M1002: remaining session(5h)/weekly(7d) usage % for the hub top-bar badge."""
    import time as _t_u, asyncio as _aio
    now = _t_u.time()
    c = _USAGE_CACHE
    if c["data"] and (now - c["ts"]) < _USAGE_TTL:
        return JSONResponse(c["data"])
    raw = await _aio.to_thread(_fetch_usage_blocking)
    def _pick(k):
        v = raw.get(k) if isinstance(raw, dict) else None
        return {"util": v.get("utilization"), "resets_at": v.get("resets_at")} if isinstance(v, dict) else None
    if isinstance(raw, dict) and not raw.get("error"):
        out = {"ok": True,
               "five_hour": _pick("five_hour"),
               "seven_day": _pick("seven_day"),
               "seven_day_sonnet": _pick("seven_day_sonnet")}  # M1002: per-user request
        c["data"], c["ts"] = out, now
    else:
        out = {"ok": False, "error": (raw or {}).get("error", "unknown") if isinstance(raw, dict) else "unknown"}
        # cache failures for 60s only so a transient error doesn't pin the badge
        c["data"], c["ts"] = out, now - (_USAGE_TTL - 60)
    return JSONResponse(out)


_UPDATE_CHECK_CACHE: dict = {"data": None, "ts": 0.0}
_UPDATE_CHECK_TTL = 3600  # 1 hr — PyPI rate limit safe

@app.get("/api/hub/update-check")
async def hub_update_check():
    """M292: Check PyPI for latest claude-ns-hub version and compare to installed."""
    import importlib.metadata, time as _t
    now = _t.time()
    if _UPDATE_CHECK_CACHE["data"] is not None and (now - _UPDATE_CHECK_CACHE["ts"]) < _UPDATE_CHECK_TTL:
        return JSONResponse(_UPDATE_CHECK_CACHE["data"])
    try:
        current = importlib.metadata.version("claude-ns-hub")
    except Exception:
        current = "unknown"
    try:
        import urllib.request, json as _json
        def _fetch_pypi():
            with urllib.request.urlopen(
                "https://pypi.org/pypi/claude-ns-hub/json", timeout=4
            ) as r:
                return _json.loads(r.read()).get("info", {}).get("version", "")
        latest = await asyncio.to_thread(_fetch_pypi)
    except Exception:
        latest = ""
    except Exception:
        latest = ""
    # Try alternate package names (northstar-hub is the old name)
    if current == "unknown":
        try:
            import importlib.metadata as _imd2
            current = _imd2.version("northstar-hub")
        except Exception:
            pass
    # Show badge when update available OR when package unknown but latest exists (prompt install)
    update_available = bool(latest and (current == "unknown" or (latest != current)))
    pip_cmd = f"pip install --upgrade claude-ns-hub" if current != "unknown" else "pip install claude-ns-hub"
    result = {
        "current": current, "latest": latest,
        "update_available": update_available,
        "pip_cmd": pip_cmd if update_available else "",
    }
    import time as _t2
    _UPDATE_CHECK_CACHE["data"] = result
    _UPDATE_CHECK_CACHE["ts"] = _t2.time()
    return JSONResponse(result)

@app.get("/api/hub/network-info")
async def hub_network_info():
    """M963: Return hub URL and Tailscale IP for NS header button."""
    ts_ip = _tailscale_interface_ip()
    has_tailscale = ts_ip != "127.0.0.1"
    hub_url = f"http://{ts_ip}:{PORT}" if has_tailscale else f"http://127.0.0.1:{PORT}"
    return JSONResponse({"hub_url": hub_url, "tailscale_ip": ts_ip if has_tailscale else None, "port": PORT})

@app.post("/api/hub/restart")
async def hub_restart():
    """Self-restart: replace the current process with a fresh uvicorn (picks up code changes)."""
    import os, sys, threading
    _server_log_action("", "", "hub:restart", "manual restart requested")
    def _do_restart():
        import time; time.sleep(0.5)
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Thread(target=_do_restart, daemon=True).start()
    return JSONResponse({"ok": True, "msg": "restarting in 500ms"})

@app.get("/api/metrics")
async def get_metrics(proj_id: str = "", days: int = 30):
    """M215 monetization: daily active metrics — completions, queued, tokens, sessions."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        q = "SELECT * FROM daily_metrics WHERE 1=1"
        params: list = []
        if proj_id:
            q += " AND proj_id=?"; params.append(proj_id)
        if days:
            from datetime import datetime as _dt, timedelta as _td
            since = (_dt.utcnow() - _td(days=days)).strftime("%Y-%m-%d")
            q += " AND date>=?"; params.append(since)
        q += " ORDER BY date DESC"
        rows = conn.execute(q, params).fetchall()
        # Aggregate totals
        totals: dict = {"stones_completed": 0, "stones_queued": 0, "total_tokens": 0}
        result_rows = []
        for r in rows:
            d = dict(r)
            result_rows.append(d)
            for k in totals: totals[k] += d.get(k) or 0
        # completion rate (done / (done+pending))
        snap_q = "SELECT status, COUNT(*) as cnt FROM stones_snapshot WHERE 1=1"
        snap_params: list = []
        if proj_id: snap_q += " AND proj_id=?"; snap_params.append(proj_id)
        snap_q += " GROUP BY status"
        snap_rows = {r["status"]: r["cnt"] for r in conn.execute(snap_q, snap_params).fetchall()}
        conn.close()
        return JSONResponse({"ok": True, "daily": result_rows, "totals": totals,
                             "stone_status_counts": snap_rows, "proj_id": proj_id or "all"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/events")
async def get_stone_events(proj_id: str = "", limit: int = 100, event_type: str = ""):
    """M215: Query SQLite stone event log for AI/world model data consumption."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        q = "SELECT * FROM stone_events WHERE 1=1"
        params: list = []
        if proj_id:
            q += " AND proj_id=?"; params.append(proj_id)
        if event_type:
            q += " AND event_type=?"; params.append(event_type)
        q += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        total = conn.execute("SELECT COUNT(*) FROM stone_events").fetchone()[0]
        conn.close()
        return JSONResponse({"ok": True, "events": rows, "total": total})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/api/activity/{proj_id}")
async def get_activity_log(proj_id: str, limit: int = 50):
    """M486/M535: Activity log — status transitions + server-side events (spawn/kill/comment)."""
    import json as _jact
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        # stone_events: status transitions, creates, deletes
        se_rows = conn.execute(
            "SELECT stone_id, event_type, payload, ts FROM stone_events "
            "WHERE proj_id=? AND event_type IN ('status_changed','stone_created','stone_deleted') "
            "ORDER BY ts DESC LIMIT ?",
            (proj_id, limit)
        ).fetchall()
        # action_log: server-side events (M535: exec spawn/kill, comments, queue)
        al_rows = conn.execute(
            "SELECT stone_id, action, detail, ts FROM action_log "
            "WHERE proj_id=? ORDER BY ts DESC LIMIT ?",
            (proj_id, limit)
        ).fetchall()
        conn.close()
        events = []
        for r in se_rows:
            p = {}
            try: p = _jact.loads(r["payload"] or "{}")
            except Exception: pass
            label = {
                "status_changed": f"{p.get('status_before','?')} → {p.get('status','')}",
                "stone_created": "created",
                "stone_deleted": "deleted",
            }.get(r["event_type"], r["event_type"])
            events.append({
                "ts": r["ts"], "stone_id": r["stone_id"],
                "event": r["event_type"], "label": label,
                "text_preview": (p.get("text") or "")[:60],
            })
        for r in al_rows:
            events.append({
                "ts": r["ts"], "stone_id": r["stone_id"] or "",
                "event": r["action"], "label": r["action"],
                "text_preview": (r["detail"] or "")[:80],
            })
        # merge sort descending by ts
        events.sort(key=lambda e: e["ts"], reverse=True)
        return JSONResponse({"ok": True, "events": events[:limit]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/events/snapshot")
async def get_stones_snapshot(proj_id: str = ""):
    """M215: Current stones snapshot from SQLite (world model data)."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        q = "SELECT * FROM stones_snapshot WHERE 1=1"
        params: list = []
        if proj_id:
            q += " AND proj_id=?"; params.append(proj_id)
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        conn.close()
        return JSONResponse({"ok": True, "stones": rows, "count": len(rows)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.post("/api/action-log")
async def post_action_log(request: Request):
    """M526: Record a user UI action for debugging stone table interactions."""
    try:
        from datetime import datetime as _dt
        data = await request.json()
        ts = data.get("ts") or _dt.utcnow().isoformat()
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute(
            "INSERT INTO action_log(ts, proj_id, stone_id, action, detail, session_id) VALUES(?,?,?,?,?,?)",
            (ts, data.get("proj_id",""), data.get("stone_id",""),
             data.get("action",""), data.get("detail",""), data.get("session_id",""))
        )
        conn.commit()
        conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/api/action-log")
async def get_action_log(proj_id: str = "", stone_id: str = "", limit: int = 200, since_minutes: int = 0):
    """M526: Query recent user action log for debugging."""
    try:
        from datetime import datetime as _dt, timedelta as _td
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        q = "SELECT * FROM action_log WHERE 1=1"
        params: list = []
        if proj_id:
            q += " AND proj_id=?"; params.append(proj_id)
        if stone_id:
            q += " AND stone_id=?"; params.append(stone_id)
        if since_minutes > 0:
            cutoff = (_dt.utcnow() - _td(minutes=since_minutes)).isoformat()
            q += " AND ts >= ?"; params.append(cutoff)
        q += " ORDER BY ts DESC LIMIT ?"; params.append(limit)
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        conn.close()
        return JSONResponse({"ok": True, "actions": rows, "count": len(rows)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/api/metrics/skill-invocation")
async def skill_invocation_metrics(days: int = 7):
    """M770: skill invocation telemetry — counts and rate per skill name.
    Reads action_log entries with action='invoked_skill' (recorded by
    PostToolUse hook ~/.claude/hooks/skill-invocation-tracker.py) and
    pairs them with stones that had skill_ref/skill_refs assigned during
    the same period (the 'expected' baseline)."""
    try:
        from datetime import datetime as _dt_skm, timedelta as _td_skm
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        since = (_dt_skm.utcnow() - _td_skm(days=days)).isoformat()
        invoked_rows = list(conn.execute(
            "SELECT detail AS skill, COUNT(*) AS n FROM action_log "
            "WHERE action='invoked_skill' AND ts >= ? "
            "GROUP BY detail ORDER BY n DESC",
            (since,),
        ))
        invoked = {r["skill"]: r["n"] for r in invoked_rows}
        # Expected = stones with skill_ref/skill_refs touched in the period
        expected: dict[str, int] = {}
        for r in conn.execute(
            "SELECT data_json FROM milestones_store WHERE updated_at >= ?",
            (since,),
        ):
            try:
                d = json.loads(r["data_json"])
            except Exception:
                continue
            refs = d.get("skill_refs") or ([d["skill_ref"]] if d.get("skill_ref") else [])
            for s in refs or []:
                expected[s] = expected.get(s, 0) + 1
        conn.close()
        skills = sorted(set(invoked) | set(expected))
        rows = []
        for s in skills:
            inv = invoked.get(s, 0)
            exp = expected.get(s, 0)
            rate = (inv / exp) if exp else None
            rows.append({"skill": s, "invoked": inv, "expected": exp, "rate": rate})
        total_inv = sum(invoked.values())
        total_exp = sum(expected.values())
        return JSONResponse({
            "ok": True, "days": days,
            "skills": rows,
            "totals": {
                "invoked": total_inv,
                "expected": total_exp,
                "rate": (total_inv / total_exp) if total_exp else None,
            },
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/metrics/subagent-activity")
async def subagent_activity(days: int = 7, proj_id: str = ""):
    """M722: parallel sub-agent activity from action_log.
    Reports start/stop counts per agent name and concurrent dispatch
    bursts (start events within 60s of each other = parallel wave)."""
    try:
        from datetime import datetime as _dt_sa, timedelta as _td_sa
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        since = (_dt_sa.utcnow() - _td_sa(days=days)).isoformat()
        q = ("SELECT ts, action, detail, proj_id, stone_id FROM action_log "
             "WHERE action IN ('subagent_start','subagent_stop') AND ts >= ? ")
        params: list = [since]
        if proj_id:
            q += "AND proj_id=? "; params.append(proj_id)
        q += "ORDER BY ts ASC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        conn.close()
        # Per-agent tallies
        per_agent: dict[str, dict] = {}
        for r in rows:
            a = r["detail"] or "unknown"
            p = per_agent.setdefault(a, {"start": 0, "stop": 0})
            if r["action"] == "subagent_start": p["start"] += 1
            else: p["stop"] += 1
        # Parallel waves — group starts within 60s windows
        from datetime import datetime as _dt_pw
        starts = [(_dt_pw.fromisoformat(r["ts"].rstrip("Z")), r["detail"] or "unknown",
                   r.get("stone_id") or None)
                  for r in rows if r["action"] == "subagent_start"]
        waves: list = []
        for ts, agent, mid in starts:
            if waves and (ts - waves[-1]["end_ts"]).total_seconds() <= 60:
                waves[-1]["agents"].append({"name": agent, "mid": mid})
                waves[-1]["end_ts"] = ts
                waves[-1]["size"] = len(waves[-1]["agents"])
            else:
                waves.append({"start_ts": ts, "end_ts": ts,
                              "agents": [{"name": agent, "mid": mid}], "size": 1})
        parallel_waves = [w for w in waves if w["size"] >= 2]
        # M722: correlate waves with stone completions — find stones completed near each wave
        conn2 = sqlite3.connect(str(_NS_EVENTS_DB))
        conn2.row_factory = sqlite3.Row
        wave_output = []
        for w in parallel_waves[-10:]:
            # Window: wave_start - 5min to wave_end + 5min
            ws = (w["start_ts"] - _td_sa(minutes=5)).isoformat()
            we = (w["end_ts"] + _td_sa(minutes=5)).isoformat()
            sq = ("SELECT DISTINCT stone_id, proj_id FROM stone_events "
                  "WHERE event_type='status_changed' AND ts BETWEEN ? AND ? "
                  "AND payload LIKE '%pending_confirmation%'")
            _sq_p = [ws, we]
            if proj_id:
                sq += " AND proj_id=?"
                _sq_p.append(proj_id)
            stones_in_wave = [dict(r) for r in conn2.execute(sq, _sq_p).fetchall()]
            wave_output.append({
                "start_ts": w["start_ts"].isoformat(),
                "size": w["size"],
                "agents": w["agents"],  # list of {name, mid}
                "completed_stones": [{"id": s["stone_id"], "proj": s["proj_id"]} for s in stones_in_wave],
            })
        # M722.2: recently completed stones (became pending_confirmation) — last 10
        # Uses json_extract so only stones that ARRIVED at pending_confirmation are matched
        _proj_filter = f" AND proj_id='{proj_id}'" if proj_id else ""
        recent_completed = [
            dict(r) for r in conn2.execute(
                "SELECT stone_id, proj_id, ts FROM stone_events "
                "WHERE event_type='status_changed' "
                "AND json_extract(payload,'$.status')='pending_confirmation'"
                f"{_proj_filter} ORDER BY ts DESC LIMIT 10"
            ).fetchall()
        ]
        # M722.3: recent_invocations — top 20 most recent subagent_start events with mid
        _inv_q = ("SELECT ts, detail, stone_id FROM action_log "
                  "WHERE action='subagent_start' AND ts >= ? ")
        _inv_params: list = [since]
        if proj_id:
            _inv_q += "AND proj_id=? "
            _inv_params.append(proj_id)
        _inv_q += "ORDER BY ts DESC LIMIT 20"
        recent_invocations = [
            {"ts": r["ts"], "agent": r["detail"] or "unknown", "mid": r["stone_id"] or None}
            for r in conn2.execute(_inv_q, _inv_params).fetchall()
        ]
        conn2.close()
        return JSONResponse({
            "ok": True, "days": days, "events": len(rows),
            "per_agent": [{"agent": k, **v} for k, v in
                          sorted(per_agent.items(), key=lambda kv: -kv[1]["start"])],
            "parallel_wave_count": len(parallel_waves),
            "largest_wave_size": max((w["size"] for w in parallel_waves), default=0),
            "recent_waves": wave_output,
            "completed_stones_recent": [{"id": r["stone_id"], "proj": r["proj_id"], "ts": r["ts"]} for r in recent_completed],
            "recent_invocations": recent_invocations,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# M780.6: 60s cache for active-only payload size computation
_efficiency_cache: dict = {}  # key: proj_id → {"ts": float, "data": dict}

@app.get("/api/metrics/efficiency")
async def metrics_efficiency(proj_id: str = "MOAT", hours: int = 24):
    """M780.6: Aggregated efficiency/accuracy dashboard for 4 metrics:
    1. active_only_saving_pct — payload reduction from stripping done-stone conversations
    2. stale_block_count_24h — action_log stale_reference_blocked events
    3. auto_review_count_24h — action_log auto_review_created events
    4. subagent_invocations_24h — action_log subagent_start events
    60s cache for the active-only diff to avoid hammering the milestones endpoint."""
    import time as _time
    from datetime import datetime as _dt_eff, timedelta as _td_eff
    now_ts = _time.monotonic()
    # --- 1. Active-only saving (60s cache) ---
    cache_entry = _efficiency_cache.get(proj_id)
    if cache_entry and (now_ts - cache_entry["ts"]) < 60:
        saving_pct = cache_entry["saving_pct"]
        active_kb = cache_entry["active_kb"]
        full_kb = cache_entry["full_kb"]
    else:
        try:
            # Count active-only stones (no conversation) from milestones_store
            conn_eff = sqlite3.connect(str(_NS_EVENTS_DB))
            ACTIVE = ("queued", "pending_confirmation", "pending", "needs_clarification")
            placeholders = ",".join(["?"] * len(ACTIVE))
            active_rows = conn_eff.execute(
                f"SELECT data_json FROM milestones_store WHERE proj_id=? AND done=0 AND status IN ({placeholders})",
                (proj_id, *ACTIVE)
            ).fetchall()
            all_rows = conn_eff.execute(
                "SELECT data_json FROM milestones_store WHERE proj_id=?",
                (proj_id,)
            ).fetchall()
            conn_eff.close()
            # Compute stripped (active-only) payload size — strip conversation like active-milestones does
            active_stripped = []
            for (dj,) in active_rows:
                try:
                    m = json.loads(dj)
                    m.pop("conversation", None); m.pop("model_used", None); m.pop("evidence_url", None)
                    active_stripped.append(m)
                except Exception:
                    pass
            # Full payload size — strip done-stone conversations (same as /milestones does)
            full_milestones = []
            for (dj,) in all_rows:
                try:
                    m = json.loads(dj)
                    if m.get("done") or m.get("status") == "done":
                        m.pop("conversation", None)
                    full_milestones.append(m)
                except Exception:
                    pass
            active_payload = json.dumps({"ok": True, "milestones": active_stripped})
            full_payload = json.dumps({"ok": True, "milestones": full_milestones})
            active_kb = round(len(active_payload.encode()) / 1024, 1)
            full_kb = round(len(full_payload.encode()) / 1024, 1)
            saving_pct = round((1 - active_kb / full_kb) * 100, 1) if full_kb > 0 else 0.0
        except Exception:
            # Fallback to M780.1 baseline
            active_kb = 42.0; full_kb = 609.0; saving_pct = 93.1
        _efficiency_cache[proj_id] = {"ts": now_ts, "saving_pct": saving_pct, "active_kb": active_kb, "full_kb": full_kb}
    # --- 2 & 3 & 4. action_log counts for last N hours ---
    try:
        since_iso = (_dt_eff.utcnow() - _td_eff(hours=hours)).isoformat()
        conn_al = sqlite3.connect(str(_NS_EVENTS_DB))
        def _count_action(action_name: str, proj: str = "") -> int:
            q = "SELECT COUNT(*) FROM action_log WHERE action=? AND ts>=?"
            params: list = [action_name, since_iso]
            if proj:
                q += " AND proj_id=?"; params.append(proj)
            return conn_al.execute(q, params).fetchone()[0] or 0
        stale_count = _count_action("stale_reference_blocked", proj_id)
        auto_review_count = _count_action("auto_review_created", proj_id)
        subagent_count = _count_action("subagent_start", proj_id)
        conn_al.close()
    except Exception:
        stale_count = 0; auto_review_count = 0; subagent_count = 0
    return JSONResponse({
        "ok": True,
        "proj_id": proj_id,
        "hours": hours,
        "active_only_saving_pct": saving_pct,
        "active_only_kb_now": active_kb,
        "active_only_kb_full": full_kb,
        "stale_block_count_24h": stale_count,
        "auto_review_count_24h": auto_review_count,
        "subagent_invocations_24h": subagent_count,
        "ts": _dt_eff.utcnow().isoformat() + "Z",
    })


@app.get("/api/ntfy/status")
async def ntfy_status():
    """M389: ntfy removed — status now reflects Telegram config only."""
    tg_token, tg_chat_id = _get_telegram_config()
    tg_ok = bool(tg_token and tg_chat_id)
    return JSONResponse({"ok": True, "configured": tg_ok,
                         "provider": "telegram" if tg_ok else "none",
                         "topic_preview": ("tg:" + tg_chat_id[:6] + "…") if tg_ok else ""})

@app.post("/api/ntfy/test")
async def test_ntfy(request: Request):
    _send_ntfy_notification("Hub test", "Telegram push notification is working!")
    tg_token, tg_chat_id = _get_telegram_config()
    sent = bool(tg_token and tg_chat_id)
    return JSONResponse({"ok": sent, "sent": sent})

@app.get("/api/telegram/status")
async def telegram_status():
    token, chat_id = _get_telegram_config()
    return JSONResponse({"ok": True, "configured": bool(token and chat_id),
                         "has_token": bool(token), "has_chat_id": bool(chat_id),
                         "chat_preview": chat_id[:6] + "…" if chat_id else ""})

@app.post("/api/telegram/config")
async def set_telegram_config(request: Request):
    data = await request.json()
    token  = (data.get("token")   or "").strip()
    chat_id = (data.get("chat_id") or "").strip()
    _TG_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    if token:
        _TG_TOKEN_FILE.write_text(token)
    if chat_id:
        _TG_CHAT_FILE.write_text(chat_id)
    return JSONResponse({"ok": True, "token_set": bool(token), "chat_id_set": bool(chat_id)})

@app.get("/api/telegram/updates")
async def telegram_get_updates():
    """Fetch latest updates from Telegram bot — used to discover chat_id after first message."""
    token, _ = _get_telegram_config()
    if not token:
        return JSONResponse({"ok": False, "error": "no token configured"}, status_code=400)
    try:
        def _fetch():
            import urllib.request as _ur
            req = _ur.Request(f"https://api.telegram.org/bot{token}/getUpdates?limit=5")
            with _ur.urlopen(req, timeout=5) as r:
                return json.loads(r.read())
        data = await asyncio.to_thread(_fetch)
        updates = data.get("result", [])
        chats = [{"chat_id": str(u["message"]["chat"]["id"]),
                  "name": u["message"]["chat"].get("first_name", "") + " " + u["message"]["chat"].get("last_name", ""),
                  "text": u["message"].get("text", "")[:30]}
                 for u in updates if "message" in u]
        return JSONResponse({"ok": True, "updates": chats})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/telegram/test")
async def test_telegram(request: Request):
    token, chat_id = _get_telegram_config()
    if not token or not chat_id:
        return JSONResponse({"ok": False, "error": "token or chat_id not configured"}, status_code=400)
    try:
        def _send():
            import urllib.request as _ur, urllib.parse as _up
            payload = _up.urlencode({"chat_id": chat_id, "text": "✅ Hub Telegram notifications working!"}).encode()
            req = _ur.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                              data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
            with _ur.urlopen(req, timeout=5) as r:
                return json.loads(r.read())
        result = await asyncio.to_thread(_send)
        return JSONResponse({"ok": result.get("ok", False), "sent": result.get("ok", False)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── North Star — file-backed multi-project manager ───────────────────────────

_parse_cache: dict[str, tuple[float, dict]] = {}  # path_str → (mtime, data)


def _ts_to_utc_naive(s: str) -> str:
    """Convert any ISO timestamp (with/without tz) to UTC naive YYYY-MM-DDTHH:MM:SS.
    Transcript timestamps are UTC with Z. exec_start/exec_end are often KST (+09:00).
    Naive (no tz) timestamps are treated as local time and converted via datetime."""
    if not s:
        return ""
    s = s.strip()
    try:
        from datetime import datetime as _dt, timezone as _tz
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # fromisoformat handles +09:00 / -05:00 in Python 3.7+
        dt = _dt.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(_tz.utc)
        else:
            # no tz marker → assume local time → convert to UTC
            import time as _time
            offset = _time.timezone if (_time.localtime().tm_isdst == 0) else _time.altzone
            from datetime import timedelta as _td
            dt = dt + _td(seconds=offset)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s[:19]


def _compute_tokens_from_transcript(proj_id: str, t_start: str, t_end: str,
                                    session_id: str | None = None,
                                    return_breakdown: bool = False) -> "int | dict | None":
    """M476/M509: scan Claude Code transcript JSONLs and sum billed tokens.

    Strategy (in order):
    1. If session_id provided → look up <session_id>.jsonl directly (no time filter needed).
       Sum ALL assistant turns in that file within [t_start, t_end] UTC window.
       If time window is bad (future/inverted) but session_id is good → sum the whole file.
    2. Fallback: scan all JSONL files and filter by [t_start, t_end] UTC window.

    t_start / t_end: ISO-format strings (any tz — auto-normalized).
    session_id: Claude Code session UUID (e.g. "fb15d2bb-933e-...") — optional.
    return_breakdown: if True, return dict {total,input,output,cache_creation,cache_read}
                      instead of int total. (M511.1: token cost breakdown for dataset)
    """
    _raw_dir = _get_project_dir(proj_id)
    if _raw_dir:
        _key = _raw_dir.replace(os.sep, "-").lstrip("-")
        _tdir = Path.home() / ".claude" / "projects" / f"-{_key}"
    else:
        _tdir = Path.home() / ".claude" / "projects" / f"-home-desk-1-Project-{proj_id}"
    if not _tdir.exists():
        return None

    def _sum_file(jf: Path, ts_min: str, ts_max: str) -> tuple[int, int, dict]:
        bd = {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0}
        total, found = 0, 0
        use_filter = bool(ts_min and ts_max and ts_min <= ts_max)
        for _line in jf.read_text(encoding="utf-8", errors="replace").splitlines():
            if not _line.strip():
                continue
            try:
                _d = json.loads(_line)
            except Exception:
                continue
            if _d.get("type") != "assistant":
                continue
            if use_filter:
                _ts = _ts_to_utc_naive(_d.get("timestamp") or "")
                if not _ts or _ts < ts_min or _ts > ts_max:
                    continue
            _u = (_d.get("message") or {}).get("usage") or {}
            _inp = _u.get("input_tokens") or 0
            _out = _u.get("output_tokens") or 0
            _ccr = _u.get("cache_creation_input_tokens") or 0
            _crd = _u.get("cache_read_input_tokens") or 0
            bd["input"] += _inp
            bd["output"] += _out
            bd["cache_creation"] += _ccr
            bd["cache_read"] += _crd
            total += _inp + _out + _ccr + _crd
            found += 1
        return total, found, bd

    _s = _ts_to_utc_naive(t_start)
    _e = _ts_to_utc_naive(t_end)
    # Fix inversion where exec_end was stored as UTC naive while exec_start is KST.
    if _s and _e and _e < _s:
        _e_raw = t_end.strip()[:19]
        if _e_raw > _s[:19]:
            _e = _e_raw
        else:
            _e = ""  # invalid → no time filter

    _merged_bd: dict = {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0}

    # Strategy 1: direct session_id lookup
    if session_id:
        _jf = _tdir / f"{session_id}.jsonl"
        if _jf.exists():
            try:
                _total, _found, _bd = _sum_file(_jf, _s, _e)
                if _found > 0:
                    if return_breakdown:
                        return {"total": _total, **_bd}
                    return _total
                # Time window produced no matches (bad timestamps) — sum whole file
                _total, _found, _bd = _sum_file(_jf, "", "")
                if _found > 0:
                    if return_breakdown:
                        return {"total": _total, **_bd}
                    return _total
            except Exception:
                pass

    # Strategy 2: time-window scan across all files
    if not _s or not _e:
        return None
    _total = 0
    _found = 0
    for _jf in sorted(_tdir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime):
        try:
            _t, _f, _bd = _sum_file(_jf, _s, _e)
            _total += _t
            _found += _f
            for _k in _merged_bd:
                _merged_bd[_k] += _bd.get(_k, 0)
        except Exception:
            continue
    if _found == 0:
        return None
    if return_breakdown:
        return {"total": _total, **_merged_bd}
    return _total
# M470 part 2: per-project asyncio lock for PATCH read-modify-write serialization.
# Without this, two concurrent PATCH requests for the same stone (e.g. saveMs from
# contenteditable blur + queue-toggle click) race: each reads proj independently,
# each writes the full proj back, and the later save with a stale read clobbers
# the earlier write — visible to the user as the queue badge auto-flipping back
# to "queue off" on a NEW stone right after they clicked it.
_proj_patch_locks: dict[str, asyncio.Lock] = {}

def _get_proj_lock(proj_id: str) -> asyncio.Lock:
    lk = _proj_patch_locks.get(proj_id)
    if lk is None:
        lk = asyncio.Lock()
        _proj_patch_locks[proj_id] = lk
    return lk

def _parse_md_frontmatter(path: Path) -> dict:
    """M287: SQLite-first load — ~1ms vs YAML parse ~400ms. Falls back to YAML and seeds SQLite.
    Cache (mtime-based) kept as L1 for same-request reads that call this multiple times.
    """
    import copy as _copy_fm
    path_str = str(path)
    # L1: mtime cache (same-process, same-request dedup)
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        # M981: no north-star.md — try SQLite before giving up
        _proj_id_fm = path.parent.name
        _db_fb = _db_load_project(_proj_id_fm)
        return _db_fb if _db_fb is not None else {}
    cached = _parse_cache.get(path_str)
    if cached and cached[0] == mtime:
        return _copy_fm.deepcopy(cached[1])

    # L2: SQLite primary store (M287) — fast single-query load
    proj_id = path.parent.name
    db_data = _db_load_project(proj_id)
    if db_data is not None:
        db_data["_body"] = ""  # body not stored in SQLite (only YAML has it)
        _parse_cache[path_str] = (mtime, _copy_fm.deepcopy(db_data))
        return db_data

    # L3: YAML fallback — parse file and seed SQLite for next call
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    lines = text.splitlines(keepends=True)
    end_idx = None
    for i, line in enumerate(lines[1:], 1):
        if line.rstrip("\r\n") == "---":
            end_idx = i
            break
    if end_idx is None:
        return {}
    try:
        fm_text = "".join(lines[1:end_idx])
        body_text = "".join(lines[end_idx + 1:]).strip()
        data = _yaml.safe_load(fm_text) or {}
        data["_body"] = body_text
        _parse_cache[path_str] = (mtime, _copy_fm.deepcopy(data))
        # Seed SQLite so next call hits L2
        _db_save_project(proj_id, _copy_fm.deepcopy(data))
        return data
    except Exception:
        return {}


# M570: _write_md_frontmatter removed — SQLite (ns-events.db) is the primary store as of M287.
# All callers were migrated to _save_project (M289 + M570). YAML files are no longer written.
# If a future caller needs YAML export, write a one-shot dump rather than restoring an
# always-on write path that would re-introduce the ~350ms full-file rewrite hot-path cost.


def _load_projects() -> list:
    """Load all projects from projects/*/north-star.md."""
    projects = []
    if PROJECTS_DIR.exists():
        for proj_dir in sorted(PROJECTS_DIR.iterdir()):
            md = proj_dir / "north-star.md"
            if md.exists():
                data = _parse_md_frontmatter(md)
                if data.get("name"):
                    data["id"] = proj_dir.name  # preserve case to match folder
                    data["file_path"] = str(md)
                    # Normalize milestones: upgrade old {done,text} → new {id,layer,parent_id,claude_ack,...}
                    raw_ms = data.get("milestones", [])
                    norm_ms = []
                    auto_id = [0]  # counter for auto-generated IDs
                    def _next_id():
                        auto_id[0] += 1
                        return f"M{auto_id[0]}"
                    for m in (raw_ms if isinstance(raw_ms, list) else []):
                        if isinstance(m, dict):
                            # Already has new schema or partial old schema
                            # Derive status: new schema uses status:, old used done:bool
                            raw_status = m.get("status", "")
                            if raw_status in ("pending", "queued", "done", "pending_confirmation", "needs_clarification"):
                                status = raw_status
                            elif m.get("done"):
                                status = "done"
                            else:
                                status = "pending"
                            entry = {
                                "id":        m.get("id") or _next_id(),
                                "text":      str(m.get("text", "")),
                                "layer":     int(m.get("layer") or 0),
                                "parent_id": m.get("parent_id") or None,
                                "status":    status,
                                "done":      status == "done",   # keep for backwards compat
                                "claude_ack": m.get("claude_ack") or None,
                                "queued_at": m.get("queued_at") or None,
                                "done_at":   m.get("done_at") or None,
                                "pending_confirm_at": m.get("pending_confirm_at") or None,
                                "clarification_question": m.get("clarification_question") or None,
                                "clarification_answer": m.get("clarification_answer") or None,
                                "clarification_answered_at": m.get("clarification_answered_at") or None,
                                "claude_comment": m.get("claude_comment") or None,
                                "conversation": m.get("conversation") or None,
                                "star_relation": m.get("star_relation") or None,
                                "star_target_at_completion": m.get("star_target_at_completion") or None,  # M266: stale-flag basis
                                "held": bool(m.get("held")),  # M216: user-paused flag, surfaced to UI
                                "user_added_at": m.get("user_added_at") or None,  # M223: creation timestamp shown in stone pane
                                "skill_ref": m.get("skill_ref") or None,   # M138: assigned skill (single)
                                "agent_ref": m.get("agent_ref") or None,   # M138: assigned agent (single)
                                "skill_refs": m.get("skill_refs") or None, # M258: multiple skills
                                "agent_refs": m.get("agent_refs") or None, # M258: multiple agents
                                # M287: monetization telemetry fields
                                "model_used":   m.get("model_used") or None,
                                "total_tokens": m.get("total_tokens") or None,
                                "exec_start":   m.get("exec_start") or None,
                                "exec_end":     m.get("exec_end") or None,
                            }
                            norm_ms.append(entry)
                        elif isinstance(m, str):
                            done = m.startswith("[x]") or m.startswith("[X]")
                            text = m.lstrip("[x] [X] [ ] ").lstrip("- ").strip()
                            import re as _re2
                            text = _re2.sub(r"^\d{4}-\d{2}-\d{2}:\s*", "", text)
                            norm_ms.append({"id": _next_id(), "text": text, "layer": 0,
                                           "parent_id": None, "done": done, "claude_ack": None})
                    data["milestones"] = norm_ms
                    # Normalize log: accept plain strings or {date,text} dicts
                    raw_log = data.get("log", [])
                    norm_log = []
                    for entry in (raw_log if isinstance(raw_log, list) else []):
                        if isinstance(entry, dict):
                            norm_log.append({"date": str(entry.get("date","")), "text": str(entry.get("text",""))})
                        elif isinstance(entry, str):
                            import re as _re3
                            dm = _re3.match(r"(\d{4}-\d{2}-\d{2}):\s*(.*)", entry)
                            if dm:
                                norm_log.append({"date": dm.group(1), "text": dm.group(2)})
                            else:
                                norm_log.append({"date": "", "text": entry})
                    data["log"] = norm_log
                    data.setdefault("deadline", "")
                    data.setdefault("links", "")
                    # Graph layout fields
                    data.setdefault("layer", 0)
                    data.setdefault("parent", None)
                    data.setdefault("position_x", 0)
                    data.setdefault("position_y", None)
                    data.setdefault("x", None)
                    data.setdefault("y", None)
                    data.setdefault("stage", "unassigned")  # lifecycle stage
                    if not isinstance(data.get("connections"), list):
                        data["connections"] = []
                    # Auto-infer repo_path from standard project layout if not set
                    if not data.get("repo_path"):
                        _project_root = Path.home() / "Project"
                        _target = proj_dir.name.lower()
                        if _project_root.exists():
                            for _candidate in _project_root.iterdir():
                                if _candidate.is_dir() and _candidate.name.lower() == _target:
                                    data["repo_path"] = str(_candidate)
                                    break
                                # Also check nested dirs (e.g., Project/VIDraft/HugwartsBanana)
                                if _candidate.is_dir():
                                    for _nested in _candidate.iterdir():
                                        if _nested.is_dir() and _nested.name.lower() == _target:
                                            data["repo_path"] = str(_nested)
                                            break
                                    if data.get("repo_path"):
                                        break
                    # Compute staleness
                    mtime = md.stat().st_mtime
                    data["last_updated"] = mtime
                    data["stale"] = (time.time() - mtime) > (14 * 86400)
                    projects.append(data)
    return projects


# Shared executor for FUSE-safe filesystem checks — reuse across calls to prevent thread leak
import concurrent.futures as _REPO_CF
_REPO_PATH_EXECUTOR = _REPO_CF.ThreadPoolExecutor(max_workers=2, thread_name_prefix="repo-stat")

def _ensure_repo_path_exists(repo_path: str) -> tuple[bool, str]:
    """M258: when a node points to a server path that doesn't exist, mkdir -p it.

    Returns (created, resolved_path). created=True if the directory was newly
    created on this call; False if it already existed or input was invalid.
    Path is expanded (~) and resolved to absolute; refuses empty input.
    Errors (PermissionError, OSError) are swallowed and reported via created=False
    so that project creation never hard-fails on a transient mkdir issue.
    """
    if not repo_path or not isinstance(repo_path, str):
        return False, ""
    try:
        p = Path(repo_path).expanduser()
        if not p.is_absolute():
            p = (Path.home() / p).resolve()
        else:
            p = p.resolve()
        # Guard against dead FUSE/remote mounts: run blocking stat in a thread
        # with a short timeout so a dead mount never stalls the async event loop.
        # Reuse module-level executor — do NOT create a new one per call (thread leak).
        import concurrent.futures as _cf
        _fut = _REPO_PATH_EXECUTOR.submit(lambda: (p.exists(), p.is_dir()))
        try:
            _exists, _isdir = _fut.result(timeout=2.0)
        except _cf.TimeoutError:
            return False, repo_path  # path unresponsive — skip silently
        if _exists and _isdir:
            return False, str(p)
        p.mkdir(parents=True, exist_ok=True)
        return True, str(p)
    except (PermissionError, OSError):
        return False, repo_path


_NS_EVENTS_DB = Path(os.environ.get("HUB_DB_PATH", str(_HUB_DATA_DIR / "ns-events.db")))

def _exec_idle_file(proj_id: str) -> Path:
    """M536: Path to the .exec-idle sentinel file for a project.
    File exists = stop-hook fired with no remaining work (truly idle).
    File absent = session is running or was just spawned."""
    return PROJECTS_DIR / proj_id / ".exec-idle"

def _server_log_action(proj_id: str, stone_id: str, action: str, detail: str = "", session_id: str = "") -> None:
    """M535: Write server-side event to action_log for debugging (fire-and-forget)."""
    import threading as _t_log
    def _write():
        try:
            from datetime import datetime as _dt2
            import sqlite3 as _sq2
            conn = _sq2.connect(str(_NS_EVENTS_DB))
            conn.execute(
                "INSERT INTO action_log(ts,proj_id,stone_id,action,detail,session_id) VALUES(?,?,?,?,?,?)",
                (_dt2.now().isoformat(), proj_id, stone_id, action, detail, session_id)
            )
            conn.commit(); conn.close()
        except Exception: pass
    _t_log.Thread(target=_write, daemon=True).start()

def _export_milestone_decision(proj_id: str, m: dict) -> None:
    """M562: Auto-export completed milestone to .omc/milestone-decisions.md for CTX/G2 retrieval."""
    import threading as _t_md
    def _write():
        try:
            from datetime import datetime as _dt_ex
            proj_dir = _get_project_dir(proj_id)
            if not proj_dir:
                return
            omc_dir = Path(proj_dir) / ".omc"
            omc_dir.mkdir(exist_ok=True)
            out_file = omc_dir / "milestone-decisions.md"
            mid = m.get("id", "?")
            text = (m.get("text") or m.get("content") or "").strip()
            date = _dt_ex.now().strftime("%Y-%m-%d")
            conv = m.get("conversation") or []
            # Extract last claude reply as decision summary
            last_claude = next((c["text"] for c in reversed(conv) if c.get("role") == "claude"), "")
            # Build YAML-frontmatter-style block for easy BM25 indexing
            entry = (
                f"\n## {mid} — {date}\n"
                f"**Stone**: {text[:200]}\n"
            )
            if last_claude:
                entry += f"**Decision**: {last_claude[:300]}\n"
            star_rel = (m.get("star_relation") or "").strip()
            if star_rel:
                entry += f"**Context**: {star_rel}\n"
            entry += "---\n"
            with open(out_file, "a", encoding="utf-8") as f:
                f.write(entry)
        except Exception:
            pass
    _t_md.Thread(target=_write, daemon=True).start()

def _extract_knowledge_object(proj_id: str, m: dict) -> None:
    """M54.7: Extract structured Knowledge Object (ADR format) from milestone on completion.

    Heuristic extraction — no LLM, <1ms. Appends to {project_dir}/.omc/facts.jsonl.
    Based on arXiv:2603.17781 Knowledge Objects: subject/predicate/object triple + file list.
    Silently skips on any error (project not found, extraction failure, IO error).
    """
    import threading as _t_ko
    def _write():
        try:
            import re as _re
            from datetime import datetime as _dt_ko

            proj_dir = _get_project_dir(proj_id)
            if not proj_dir:
                return

            mid = m.get("id", "?")
            text = (m.get("text") or m.get("content") or "").strip()
            status = m.get("status", "")
            star_rel = (m.get("star_relation") or "").strip()
            conv = m.get("conversation") or []

            # Build full corpus: stone text + star_relation + last claude message
            last_claude_text = next(
                (c.get("text") or c.get("content") or ""
                 for c in reversed(conv)
                 if c.get("role") == "claude"),
                ""
            )
            full_corpus = " ".join(filter(None, [text, star_rel, last_claude_text]))

            # Extract file paths via regex (any path with / or . that looks like a file)
            _FILE_RE = _re.compile(
                r"(?<!\w)"
                r"(?:~?/[A-Za-z0-9_./-]+\.[A-Za-z0-9]{1,6}"  # /absolute/path.ext
                r"|[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+\.[A-Za-z0-9]{1,6}"  # relative/path.ext
                r"|[A-Za-z0-9_.-]+\.(?:py|js|ts|md|json|yaml|yml|toml|sh|txt|sql|html|css))"  # bare file.ext
            )
            file_paths = list(dict.fromkeys(_FILE_RE.findall(full_corpus)))[:20]  # dedup, max 20

            # Build subject: first 100 chars of stone text (the "what")
            subject = text[:100].strip() if text else mid

            # Build predicate: status-based verb phrase
            if status == "done":
                predicate = "completed"
            elif status == "pending_confirmation":
                predicate = "pending_confirmation"
            else:
                predicate = f"status:{status}"

            # Build object: star_relation if set, else last claude reply, else truncated text
            if star_rel:
                obj = star_rel[:300]
            elif last_claude_text:
                obj = last_claude_text[:300]
            else:
                obj = text[100:300].strip() if len(text) > 100 else ""

            ko = {
                "ts": _dt_ko.now().isoformat(),
                "mid": mid,
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "files": file_paths,
                "raw": text[:200],
            }

            omc_dir = Path(proj_dir) / ".omc"
            omc_dir.mkdir(exist_ok=True)
            facts_file = omc_dir / "facts.jsonl"
            import json as _json_ko
            with open(facts_file, "a", encoding="utf-8") as _f:
                _f.write(_json_ko.dumps(ko, ensure_ascii=False) + "\n")
        except Exception:
            pass
    _t_ko.Thread(target=_write, daemon=True).start()


def _init_events_db():
    """M215: Initialize SQLite event log for stone data migration."""
    try:
        _HUB_DATA_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("""CREATE TABLE IF NOT EXISTS stone_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            proj_id TEXT NOT NULL,
            stone_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT,
            ts TEXT NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS stones_snapshot (
            proj_id TEXT NOT NULL,
            stone_id TEXT NOT NULL,
            status TEXT,
            text TEXT,
            claude_ack TEXT,
            held INTEGER DEFAULT 0,
            layer INTEGER DEFAULT 0,
            ts TEXT NOT NULL,
            PRIMARY KEY (proj_id, stone_id)
        )""")
        # M215 monetization fields — added via ALTER TABLE if missing
        for col_sql in [
            "ALTER TABLE stone_events ADD COLUMN agent TEXT",
            "ALTER TABLE stone_events ADD COLUMN model TEXT",
            "ALTER TABLE stone_events ADD COLUMN token_usage INTEGER",
            "ALTER TABLE stones_snapshot ADD COLUMN done_at TEXT",
            "ALTER TABLE stones_snapshot ADD COLUMN agent TEXT",
            "ALTER TABLE stones_snapshot ADD COLUMN model TEXT",
            "ALTER TABLE stones_snapshot ADD COLUMN queued_at TEXT",
            "ALTER TABLE stones_snapshot ADD COLUMN total_tokens INTEGER",
            # M842: 4 base execution columns missing from original CREATE TABLE
            "ALTER TABLE milestones_store ADD COLUMN total_tokens INTEGER DEFAULT NULL",
            "ALTER TABLE milestones_store ADD COLUMN model_used TEXT DEFAULT NULL",
            "ALTER TABLE milestones_store ADD COLUMN exec_start TEXT DEFAULT NULL",
            "ALTER TABLE milestones_store ADD COLUMN exec_end TEXT DEFAULT NULL",
            # M511.1: token cost breakdown for dataset/monetization
            "ALTER TABLE milestones_store ADD COLUMN input_tokens INTEGER",
            "ALTER TABLE milestones_store ADD COLUMN output_tokens INTEGER",
            "ALTER TABLE milestones_store ADD COLUMN cache_creation_tokens INTEGER",
            "ALTER TABLE milestones_store ADD COLUMN cache_read_tokens INTEGER",
            "ALTER TABLE milestones_store ADD COLUMN cost_usd REAL",
            "ALTER TABLE milestones_store ADD COLUMN completion_status TEXT",
            "ALTER TABLE milestones_store ADD COLUMN reopen_count INTEGER",
            # M511.2: failure reason for partial/fail completions
            "ALTER TABLE milestones_store ADD COLUMN failure_reason TEXT",
            # M775: causal dataset — outcome label for done stones
            "ALTER TABLE milestones_store ADD COLUMN outcome_label TEXT",
            # M775.2: counterfactual pair linking — shared UUID across sibling stones
            "ALTER TABLE milestones_store ADD COLUMN counterfactual_pair_id TEXT",
            # M775.3: causal dataset — goal-tree, prompt provenance, confounder
            "ALTER TABLE milestones_store ADD COLUMN goal_tree_snapshot TEXT",
            "ALTER TABLE milestones_store ADD COLUMN prompt_provenance TEXT",
            "ALTER TABLE milestones_store ADD COLUMN confounder TEXT",
        ]:
            try: conn.execute(col_sql)
            except Exception: pass  # column already exists
        # M215 daily active metrics table
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_metrics (
            date TEXT NOT NULL,
            proj_id TEXT NOT NULL,
            stones_completed INTEGER DEFAULT 0,
            stones_queued INTEGER DEFAULT 0,
            active_sessions INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            PRIMARY KEY (date, proj_id)
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_proj ON stone_events(proj_id, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_date ON daily_metrics(date)")
        # M526: user action log for debugging stone table interactions
        conn.execute("""CREATE TABLE IF NOT EXISTS action_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            proj_id TEXT,
            stone_id TEXT,
            action TEXT NOT NULL,
            detail TEXT,
            session_id TEXT
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_action_log_ts ON action_log(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_action_log_proj ON action_log(proj_id, ts)")
        # M785: centralized key-value user settings (replaces per-device localStorage for
        # hub-wide preferences like ns-model-avatars that should follow the user across peers).
        conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )""")
        conn.commit()
        conn.close()
    except Exception:
        pass

_init_events_db()


# ── SQLite primary store (M287) ───────────────────────────────────────────────
# milestones_store + project_meta tables in ns-events.db.
# _save_project writes here first (sync, ~1ms) then writes YAML async as backup.
# _parse_md_frontmatter checks here before parsing YAML (cache always warm after first write).

def _ns_primary_init():
    """M287: Add milestones_store and project_meta tables to ns-events.db."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("""CREATE TABLE IF NOT EXISTS milestones_store (
            proj_id TEXT NOT NULL,
            stone_id TEXT NOT NULL,
            data_json TEXT NOT NULL,
            status TEXT,
            done INTEGER DEFAULT 0,
            held INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL,
            total_tokens INTEGER DEFAULT NULL,
            model_used TEXT DEFAULT NULL,
            exec_start TEXT DEFAULT NULL,
            exec_end TEXT DEFAULT NULL,
            PRIMARY KEY (proj_id, stone_id)
        )""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ms_store_status
            ON milestones_store(proj_id, status, done)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS project_meta (
            proj_id TEXT PRIMARY KEY,
            meta_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )""")
        conn.commit()
        conn.close()
    except Exception:
        pass

_ns_primary_init()


def _migrate_yaml_to_sqlite():
    """M287: One-time migration — seed SQLite from all existing YAML north-star.md files."""
    import copy as _cp_mig
    if not PROJECTS_DIR.exists():
        return
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        already_migrated = {r[0] for r in conn.execute("SELECT proj_id FROM project_meta").fetchall()}
        conn.close()
    except Exception:
        already_migrated = set()
    for proj_dir in sorted(PROJECTS_DIR.iterdir()):
        if not proj_dir.is_dir():
            continue
        proj_id = proj_dir.name
        if proj_id in already_migrated:
            continue
        md = proj_dir / "north-star.md"
        if not md.exists():
            continue
        try:
            text = md.read_text(encoding="utf-8")
            if not text.startswith("---"):
                continue
            lines = text.splitlines(keepends=True)
            end_idx = next((i for i, l in enumerate(lines[1:], 1) if l.rstrip("\r\n") == "---"), None)
            if end_idx is None:
                continue
            fm_text = "".join(lines[1:end_idx])
            data = _yaml.safe_load(fm_text) or {}
            data["_body"] = "".join(lines[end_idx + 1:]).strip()
            _db_save_project(proj_id, _cp_mig.deepcopy(data))
        except Exception:
            pass


def _db_save_project(proj_id: str, data: dict):
    """M287: Write full project data to SQLite (primary store). ~1ms vs ~350ms YAML rewrite."""
    import datetime as _dt_db
    import copy as _cp_db
    _db_t0 = time.time()
    now = _dt_db.datetime.utcnow().isoformat()
    milestones = data.get("milestones") or []
    # project meta = everything except milestones + _body
    meta = {k: v for k, v in data.items() if k not in ("milestones", "_body")}
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("INSERT OR REPLACE INTO project_meta(proj_id, meta_json, updated_at) VALUES(?,?,?)",
                     (proj_id, json.dumps(meta, ensure_ascii=False), now))
        existing_ids = set(
            r[0] for r in conn.execute("SELECT stone_id FROM milestones_store WHERE proj_id=?", (proj_id,)).fetchall()
        )
        new_ids = set()
        for m in milestones:
            if not isinstance(m, dict) or not m.get("id"):
                continue
            sid = m["id"]
            new_ids.add(sid)
            # INSERT OR IGNORE + UPDATE preserves rowid (order), unlike INSERT OR REPLACE
            conn.execute("""INSERT OR IGNORE INTO milestones_store
                (proj_id, stone_id, data_json, status, done, held, updated_at)
                VALUES(?,?,?,?,?,?,?)""",
                (proj_id, sid, json.dumps(m, ensure_ascii=False),
                 m.get("status", "pending"),
                 1 if m.get("done") else 0,
                 1 if m.get("held") else 0,
                 now))
            # M287/M511.1: persist token/model/time/cost fields as indexed columns
            conn.execute("""UPDATE milestones_store SET data_json=?, status=?, done=?, held=?, updated_at=?,
                total_tokens=COALESCE(?,total_tokens), model_used=COALESCE(?,model_used),
                exec_start=COALESCE(?,exec_start), exec_end=COALESCE(?,exec_end),
                input_tokens=COALESCE(?,input_tokens), output_tokens=COALESCE(?,output_tokens),
                cache_creation_tokens=COALESCE(?,cache_creation_tokens),
                cache_read_tokens=COALESCE(?,cache_read_tokens),
                cost_usd=COALESCE(?,cost_usd),
                completion_status=COALESCE(?,completion_status),
                reopen_count=COALESCE(?,reopen_count),
                failure_reason=COALESCE(?,failure_reason),
                outcome_label=COALESCE(?,outcome_label),
                counterfactual_pair_id=COALESCE(?,counterfactual_pair_id),
                goal_tree_snapshot=COALESCE(?,goal_tree_snapshot),
                prompt_provenance=COALESCE(?,prompt_provenance),
                confounder=COALESCE(?,confounder)
                WHERE proj_id=? AND stone_id=?""",
                (json.dumps(m, ensure_ascii=False),
                 m.get("status", "pending"),
                 1 if m.get("done") else 0,
                 1 if m.get("held") else 0,
                 now,
                 m.get("total_tokens"), m.get("model_used"),
                 m.get("exec_start") or m.get("queued_at"),
                 m.get("exec_end") or m.get("pending_confirm_at"),
                 m.get("input_tokens"), m.get("output_tokens"),
                 m.get("cache_creation_tokens"), m.get("cache_read_tokens"),
                 m.get("cost_usd"), m.get("completion_status"),
                 m.get("reopen_count"), m.get("failure_reason"),
                 m.get("outcome_label"),
                 m.get("counterfactual_pair_id"),
                 m.get("goal_tree_snapshot"),
                 m.get("prompt_provenance"),
                 m.get("confounder"),
                 proj_id, sid))
        # M426: DELETE from milestones_store (active only) — stone_events retains deletion history
        # milestones_store = active stones; stone_events with stone_deleted = audit trail
        for removed_id in existing_ids - new_ids:
            conn.execute("DELETE FROM milestones_store WHERE proj_id=? AND stone_id=?", (proj_id, removed_id))
        conn.commit()
        conn.close()
        # M698: log slow DB writes (>50ms) for observability
        _db_ms = int((time.time() - _db_t0) * 1000)
        if _db_ms > 50:
            _server_log_action(proj_id, "", "db:save_slow", f"{len(milestones)} stones, {_db_ms}ms")
    except Exception as _db_ex:
        _server_log_action(proj_id, "", "db:save_error", str(_db_ex)[:120])


def _db_save_single_milestone(proj_id: str, stone: dict):
    """M698 perf: update only one row — O(1) vs O(n) full-project rewrite. ~1ms regardless of project size."""
    if not stone or not stone.get("id"):
        return
    import datetime as _dt_sm
    now = _dt_sm.datetime.utcnow().isoformat()
    sid = stone["id"]
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("""INSERT OR IGNORE INTO milestones_store
            (proj_id, stone_id, data_json, status, done, held, updated_at)
            VALUES(?,?,?,?,?,?,?)""",
            (proj_id, sid, json.dumps(stone, ensure_ascii=False),
             stone.get("status", "pending"),
             1 if stone.get("done") else 0,
             1 if stone.get("held") else 0, now))
        conn.execute("""UPDATE milestones_store SET data_json=?, status=?, done=?, held=?, updated_at=?,
            total_tokens=COALESCE(?,total_tokens), model_used=COALESCE(?,model_used),
            exec_start=COALESCE(?,exec_start), exec_end=COALESCE(?,exec_end),
            input_tokens=COALESCE(?,input_tokens), output_tokens=COALESCE(?,output_tokens),
            cache_creation_tokens=COALESCE(?,cache_creation_tokens),
            cache_read_tokens=COALESCE(?,cache_read_tokens),
            cost_usd=COALESCE(?,cost_usd),
            completion_status=COALESCE(?,completion_status),
            reopen_count=COALESCE(?,reopen_count),
            failure_reason=COALESCE(?,failure_reason),
            outcome_label=COALESCE(?,outcome_label),
            counterfactual_pair_id=COALESCE(?,counterfactual_pair_id),
            goal_tree_snapshot=COALESCE(?,goal_tree_snapshot),
            prompt_provenance=COALESCE(?,prompt_provenance),
            confounder=COALESCE(?,confounder)
            WHERE proj_id=? AND stone_id=?""",
            (json.dumps(stone, ensure_ascii=False),
             stone.get("status", "pending"),
             1 if stone.get("done") else 0,
             1 if stone.get("held") else 0, now,
             stone.get("total_tokens"), stone.get("model_used"),
             stone.get("exec_start") or stone.get("queued_at"),
             stone.get("exec_end") or stone.get("pending_confirm_at"),
             stone.get("input_tokens"), stone.get("output_tokens"),
             stone.get("cache_creation_tokens"), stone.get("cache_read_tokens"),
             stone.get("cost_usd"), stone.get("completion_status"),
             stone.get("reopen_count"), stone.get("failure_reason"),
             stone.get("outcome_label"),
             stone.get("counterfactual_pair_id"),
             stone.get("goal_tree_snapshot"),
             stone.get("prompt_provenance"),
             stone.get("confounder"),
             proj_id, sid))
        conn.commit()
        conn.close()
    except Exception:
        pass


_NEGATIVE_KW = ("잘못", "다시", "왜", "안돼", "이상", "bug", "wrong", "redo", "fix")

_ABANDONED_KW = ("취소", "포기", "abandon", "drop", "skip", "넘김", "패스")

def _derive_outcome_label(stone: dict) -> str | None:
    """M775: Heuristic outcome_label for done stones.
    Returns 'success', 'failure', 'abandoned', or None.
    - 'abandoned': done stone with empty/single-message conversation OR user-keyword indicates abandon.
    - 'failure': user message in last 3 turns contains a negative keyword.
    - 'success': default for done stones with substantive conversation.
    - None: not done."""
    if stone.get("outcome_label"):
        return stone["outcome_label"]  # manual override wins
    status = stone.get("status") or ("done" if stone.get("done") else "pending")
    if status != "done":
        return None
    conv = stone.get("conversation") or []
    user_msgs = [m for m in conv if m.get("role") == "user"]
    # Abandoned (conservative): truly silent skip (no conversation at all) OR
    # explicit abandonment keyword in any user message.
    if len(conv) == 0:
        return "abandoned"
    all_user_text = " ".join(
        str(m.get("text") or m.get("content") or "").lower() for m in user_msgs
    )
    if any(kw in all_user_text for kw in _ABANDONED_KW):
        return "abandoned"
    last3 = conv[-3:] if len(conv) > 3 else conv
    last3_user = [m for m in last3 if m.get("role") == "user"]
    for msg in last3_user:
        text = str(msg.get("text") or msg.get("content") or "").lower()
        if any(kw in text for kw in _NEGATIVE_KW):
            return "failure"
    return "success"


# M775.5: PII scrubbing — masks email, phone, IP, Bearer tokens in conversation text.
# Regex-based; not infallible. Bespoke identifiers may leak.
import re as _re_pii
_PII_PATTERNS = [
    (_re_pii.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"), "<EMAIL>"),
    (_re_pii.compile(r"\b\+?\d{1,3}[-\s.]?\(?\d{2,4}\)?[-\s.]?\d{3,4}[-\s.]?\d{3,4}\b"), "<PHONE>"),
    (_re_pii.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<IP>"),
    (_re_pii.compile(r"Bearer\s+[A-Za-z0-9\-._~+/]+=*", _re_pii.IGNORECASE), "Bearer <TOKEN>"),
    (_re_pii.compile(r"sk-[A-Za-z0-9]{20,}"), "<API_KEY>"),
]

def _scrub_pii(text: str) -> str:
    if not text:
        return text
    s = str(text)
    for pat, repl in _PII_PATTERNS:
        s = pat.sub(repl, s)
    return s

def _scrub_conversation(conv: list) -> list:
    """Apply PII scrubbing to all conversation messages."""
    if not conv:
        return conv
    out = []
    for msg in conv:
        if not isinstance(msg, dict):
            out.append(msg)
            continue
        scrubbed = dict(msg)
        for field in ("text", "content"):
            if field in scrubbed and scrubbed[field]:
                scrubbed[field] = _scrub_pii(scrubbed[field])
        out.append(scrubbed)
    return out


def _capture_goal_tree_snapshot(stone: dict, milestones: list, proj: dict) -> str | None:
    """M775.3: Capture goal-tree state at done-transition (best-effort heuristic)."""
    try:
        substar_id = stone.get("substar_id") or stone.get("parent_id")
        if not substar_id:
            return json.dumps({"substar_id": None, "note": "no_substar_assignment"})
        ns_list = proj.get("north_stars") or []
        # find parent north-star name
        ns_name = None
        ns_target = proj.get("target") or None
        for ns in ns_list:
            if isinstance(ns, dict) and ns.get("id") == substar_id:
                ns_name = ns.get("name") or ns.get("title")
                ns_target = ns.get("target") or ns_target
                break
        # siblings = stones sharing same substar_id
        siblings = [m for m in milestones if isinstance(m, dict) and
                    (m.get("substar_id") or m.get("parent_id")) == substar_id]
        siblings_done = sum(1 for s in siblings if s.get("status") == "done" or s.get("done"))
        mother_done_pct = round(siblings_done / len(siblings), 2) if siblings else 0.0
        snap = {
            "substar_id": substar_id,
            "substar_name": ns_name,
            "siblings_total": len(siblings),
            "siblings_done": siblings_done,
            "mother_done_pct": mother_done_pct,
            "north_star_target": ns_target,
        }
        return json.dumps(snap, ensure_ascii=False)
    except Exception:
        return None


def _capture_prompt_provenance(stone: dict, data: dict) -> str | None:
    """M775.3: Capture dispatch provenance at done-transition (best-effort)."""
    try:
        conv = stone.get("conversation") or []
        # infer dispatch_type from data keys
        if data.get("exec_start") or data.get("exec_end"):
            dispatch_type = "execute_sync"
        elif data.get("claude_comment") or (conv and conv[-1:] and conv[-1].get("role") == "claude"):
            dispatch_type = "reply_sync"
        else:
            dispatch_type = "unknown"
        prov = {
            "dispatch_type": dispatch_type,
            "user_last_msg_skill_refs": stone.get("skill_refs") or [],
            "user_last_msg_agent_refs": stone.get("agent_refs") or [],
            "session_id": stone.get("session_id") or data.get("session_id") or None,
            "conv_turn_count": len(conv),
        }
        return json.dumps(prov, ensure_ascii=False)
    except Exception:
        return None


def _capture_confounder(stone: dict, milestones: list) -> str | None:
    """M775.3: Capture execution confounders at done-transition (best-effort)."""
    try:
        import datetime as _dt_cf
        now_cf = _dt_cf.datetime.utcnow()
        # exec_duration_sec from exec_start/exec_end
        exec_start = stone.get("exec_start")
        exec_end = stone.get("exec_end") or stone.get("pending_confirm_at")
        exec_dur = None
        if exec_start and exec_end:
            try:
                _fmt = "%Y-%m-%dT%H:%M"
                _s = _dt_cf.datetime.strptime(exec_start[:16], _fmt)
                _e = _dt_cf.datetime.strptime(exec_end[:16], _fmt)
                exec_dur = int((_e - _s).total_seconds())
            except Exception:
                pass
        # queue_depth = queued stones in same substar at this moment
        substar_id = stone.get("substar_id") or stone.get("parent_id")
        queue_depth = sum(1 for m in milestones if isinstance(m, dict) and
                          m.get("status") == "queued" and
                          (m.get("substar_id") or m.get("parent_id")) == substar_id)
        conf = {
            "model_used": stone.get("model_used") or None,
            "exec_duration_sec": exec_dur,
            "queue_depth_at_dispatch": queue_depth,
            "hour_of_day": now_cf.hour,
            "weekday": now_cf.weekday(),
        }
        return json.dumps(conf, ensure_ascii=False)
    except Exception:
        return None


def _db_load_project(proj_id: str) -> dict | None:
    """M287: Load project from SQLite. Returns None if not found (caller falls back to YAML)."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row = conn.execute("SELECT meta_json FROM project_meta WHERE proj_id=?", (proj_id,)).fetchone()
        if not row:
            conn.close()
            return None
        meta = json.loads(row[0])
        ms_rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? ORDER BY rowid DESC",
            (proj_id,)
        ).fetchall()
        conn.close()
        meta["milestones"] = [json.loads(r[0]) for r in ms_rows]
        return meta
    except Exception:
        return None


def _db_get_milestone(proj_id: str, stone_id: str) -> dict | None:
    """M215: Single-stone fast SQLite lookup. ~0.1ms."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND stone_id=?",
            (proj_id, stone_id)
        ).fetchone()
        conn.close()
        return json.loads(row[0]) if row else None
    except Exception:
        return None


def _db_get_active_milestones(proj_id: str) -> list | None:
    """M215: Fast indexed SQLite query for active (non-done) milestones. ~1ms vs full project load.
    Returns None on error (caller falls back to full load)."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND done=0 ORDER BY rowid DESC",
            (proj_id,)
        ).fetchall()
        conn.close()
        return [json.loads(r[0]) for r in rows]
    except Exception:
        return None


def _record_stone_events(proj_id: str, milestones: list, prev_milestones: list | None = None):
    """M215: Dual-write stone changes to SQLite event log."""
    import datetime as _dt_ev
    now = _dt_ev.datetime.now().isoformat()
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        prev_map = {m.get("id"): m for m in (prev_milestones or []) if isinstance(m, dict)}
        for m in milestones:
            if not isinstance(m, dict): continue
            mid = m.get("id", "")
            if not mid: continue
            prev = prev_map.get(mid)
            # Detect event type
            if prev is None:
                ev_type = "stone_created"
            elif prev.get("status") != m.get("status"):
                ev_type = "status_changed"
            else:
                ev_type = "stone_updated"
            # Record event — include status_before for transition visibility (M486)
            payload_data = {k: m.get(k) for k in
                ["status","text","claude_ack","held","layer","parent_id","star_relation","user_added_at"]
                if m.get(k) is not None}
            if ev_type == "status_changed" and prev:
                payload_data["status_before"] = prev.get("status", "")
            payload = json.dumps(payload_data, sort_keys=True)
            # Skip stone_updated if content unchanged vs prev (prevents event spam)
            if ev_type == "stone_updated" and prev is not None:
                prev_payload = json.dumps({k: prev.get(k) for k in
                    ["status","text","claude_ack","held","layer","parent_id","star_relation","user_added_at"]
                    if prev.get(k) is not None}, sort_keys=True)
                if payload == prev_payload:
                    # Only update snapshot, skip event insert
                    pass
                else:
                    conn.execute("INSERT INTO stone_events(proj_id,stone_id,event_type,payload,ts) VALUES(?,?,?,?,?)",
                                 (proj_id, mid, ev_type, payload, now))
            else:
                conn.execute("INSERT INTO stone_events(proj_id,stone_id,event_type,payload,ts) VALUES(?,?,?,?,?)",
                             (proj_id, mid, ev_type, payload, now))
            # Update snapshot with monetization fields
            new_status = m.get("status", "pending")
            done_at = m.get("done_at") or (now if new_status == "done" and (prev and prev.get("status") != "done") else None)
            queued_at = m.get("queued_at") or (now if new_status == "queued" and (prev and prev.get("status") != "queued") else None)
            conn.execute("""INSERT OR REPLACE INTO stones_snapshot
                (proj_id,stone_id,status,text,claude_ack,held,layer,ts,done_at,queued_at)
                VALUES(?,?,?,?,?,?,?,?,
                  COALESCE(?,IFNULL((SELECT done_at FROM stones_snapshot WHERE proj_id=? AND stone_id=?),NULL)),
                  COALESCE(?,IFNULL((SELECT queued_at FROM stones_snapshot WHERE proj_id=? AND stone_id=?),NULL)))""",
                (proj_id, mid, new_status, m.get("text",""),
                 m.get("claude_ack"), 1 if m.get("held") else 0,
                 m.get("layer", 0), now,
                 done_at, proj_id, mid,
                 queued_at, proj_id, mid))
            # M215 monetization: update daily_metrics on completion
            if new_status == "done" and (prev is None or prev.get("status") != "done"):
                today = now[:10]
                conn.execute("""INSERT INTO daily_metrics(date,proj_id,stones_completed) VALUES(?,?,1)
                    ON CONFLICT(date,proj_id) DO UPDATE SET stones_completed=stones_completed+1""", (today, proj_id))
            elif new_status == "queued" and (prev is None or prev.get("status") != "queued"):
                today = now[:10]
                conn.execute("""INSERT INTO daily_metrics(date,proj_id,stones_queued) VALUES(?,?,1)
                    ON CONFLICT(date,proj_id) DO UPDATE SET stones_queued=stones_queued+1""", (today, proj_id))
        conn.commit()
        # Validation: YAML count vs snapshot count
        yaml_count = len([m for m in milestones if isinstance(m,dict) and m.get("id")])
        db_count = conn.execute("SELECT COUNT(*) FROM stones_snapshot WHERE proj_id=?", (proj_id,)).fetchone()[0]
        conn.close()
        if yaml_count != db_count and db_count > 0:
            # Log mismatch but don't block (YAML is authoritative)
            pass
    except Exception:
        pass


def _save_project(proj_id: str, data: dict):
    """M287: SQLite-first save — write to SQLite (~1ms) then async YAML backup.
    Callers see no API change; SQLite is now the primary read path via _parse_md_frontmatter.
    """
    import threading as _threading
    import copy as _cp_sp
    proj_dir = PROJECTS_DIR / proj_id
    md = proj_dir / "north-star.md"

    # Capture prev milestones for event delta (from SQLite if available, else YAML)
    prev_data = _db_load_project(proj_id)
    if prev_data is None and md.exists():
        prev_data = _parse_cache.get(str(md), (None, {}))[1]
    prev_ms = (prev_data or {}).get("milestones", [])

    # M287: Primary write — SQLite first, fast (~1ms)
    _db_save_project(proj_id, data)
    # Invalidate mtime cache so next _parse_md_frontmatter skips L1 and hits SQLite (L2)
    _parse_cache.pop(str(md), None)

    # M215: event log (metrics/audit) — background OK, not on read path
    _record_stone_events(proj_id, data.get("milestones", []), prev_ms)

    # M649: YAML backup disabled — SQLite is primary store, YAML write was adding ~350ms background load

    # M278: Turso cloud sync
    _t = _threading.Thread(target=_turso_sync_project, args=(proj_id, data.get("milestones", [])), daemon=True)
    _t.start()


def _load_stone_memory(proj_id: str, limit: int = 6) -> str:
    """M520: load recent stone-memory facts for Execute prompt injection.
    Reads .stone-memory/{stone_id}.jsonl files written by stop hook after completion."""
    import datetime as _dt_mem
    mem_dir = PROJECTS_DIR / proj_id / ".stone-memory"
    if not mem_dir.exists():
        return ""
    cutoff = (_dt_mem.datetime.now() - _dt_mem.timedelta(hours=48)).isoformat()
    entries = []
    for p in sorted(mem_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)[:limit]:
        try:
            raw = p.read_text(encoding="utf-8").splitlines()[0]
            e = json.loads(raw)
            if e.get("ts", "") < cutoff:
                continue
            f = e.get("facts", {})
            lines = []
            for cat, items in f.items():
                for item in items:
                    lines.append(f"  [{cat}] {item}")
            if lines:
                entries.append(f"  [{e['stone_id']}]\n" + "\n".join(lines))
        except Exception:
            continue
    if not entries:
        return ""
    return "STONE MEMORY (facts from recently completed stones):\n" + "\n".join(entries) + "\n\n"


@app.get("/northstar")
async def northstar_page():
    return FileResponse(str(STATIC / "northstar.html"),
                        headers={"Cache-Control": "no-store, no-cache, must-revalidate",
                                 "Pragma": "no-cache"})


@app.get("/landing")
async def landing_page():
    """M231: ns-system landing page (DISABLED M1026 — no separate landing yet).
    Returns 404 until a real landing/marketing page is shipped."""
    return JSONResponse({"ok": False, "detail": "landing page not available"}, status_code=404)


@app.get("/corpus-hub")
async def corpus_hub_page():
    """M275: built-in corpus page served from hub — eliminates need for separate port 8989."""
    # Serve the entity-corpus app.js static dashboard HTML embedded in hub
    entity_dash = _HUB_DATA_DIR / "entity" / "dashboard" / "static"
    idx = entity_dash / "index.html"
    if idx.exists():
        return FileResponse(str(idx), headers={"Cache-Control": "no-store"})
    # Fallback: serve dynamic skills/agents page from hub API
    from fastapi.responses import HTMLResponse as _HR
    return _HR("""<!DOCTYPE html><html><head><meta charset='utf-8'>
<title>Corpus — Hub</title>
<style>body{background:#1a1814;color:#e8e0d6;font-family:monospace;padding:20px}
h2{color:#cc785c}table{border-collapse:collapse;width:100%}
td,th{border:1px solid #3a3630;padding:6px 10px}th{background:#2d2a26}
</style></head><body>
<h2>Corpus — Skills & Agents</h2>
<div id='out'>Loading…</div>
<script>
fetch('/api/corpus/skills-agents').then(r=>r.json()).then(d=>{
  const s=d.skills||[],a=d.agents||[];
  document.getElementById('out').innerHTML=
    '<h3>⚡ Skills ('+s.length+')</h3><table><tr><th>Name</th><th>Description</th></tr>'+
    s.map(x=>'<tr><td>'+x.name+'</td><td>'+x.description+'</td></tr>').join('')+
    '</table><br><h3>🤖 Agents ('+a.length+')</h3><table><tr><th>Name</th><th>Description</th></tr>'+
    a.map(x=>'<tr><td>'+x.name+'</td><td>'+x.description+'</td></tr>').join('')+
    '</table>';
});
</script></body></html>""", status_code=200)


@app.get("/api/northstar")
async def northstar_get():
    projects = _load_projects()
    # Strip internal fields before returning
    clean = [{k: v for k, v in p.items() if k != "_body"} for p in projects]
    return JSONResponse(clean)


@app.post("/api/northstar")
async def northstar_save(request: Request):
    data = await request.json()
    if isinstance(data, list):
        # Bulk save — write each project to its file
        for p in data:
            proj_id = p.get("id", p.get("name", "").lower().replace(" ", "-"))
            if proj_id:
                _save_project(proj_id, {k: v for k, v in p.items() if k not in ("stale","last_updated","file_path")})
    return JSONResponse({"ok": True})


@app.get("/api/northstar/{proj_id}/okrs")
async def northstar_okrs(proj_id: str):
    """Extract OKRs from north-star.md body."""
    import re as _re
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "okrs": [], "section": ""})
    data = _parse_md_frontmatter(md)
    body = data.get("_body", "")
    # Find OKR section
    m = _re.search(r"^##\s*OKR[^\n]*\n((?:[-*]\s+.+\n?)+)", body, _re.MULTILINE)
    if not m:
        return JSONResponse({"ok": True, "okrs": [], "section": ""})
    section_title = _re.search(r"^##\s*(OKR[^\n]*)", body, _re.MULTILINE)
    title = section_title.group(1).strip() if section_title else "OKRs"
    items = [line.lstrip("-* ").strip() for line in m.group(1).splitlines() if line.strip()]
    return JSONResponse({"ok": True, "okrs": items, "section": title})


@app.post("/api/northstar/{proj_id}/sync-milestones")
async def sync_milestones(proj_id: str, request: Request):
    """Auto-detect milestone completion from CTX graph + project log using Claude."""
    body = await request.json()
    milestones = body.get("milestones", [])
    ctx_topics  = body.get("ctx_topics", [])   # hot nodes from CTX
    log_entries = body.get("log", [])           # project progress log

    if not milestones:
        return JSONResponse({"ok": False, "error": "no milestones provided"})

    ms_text = "\n".join(f"{i+1}. [{('DONE' if m.get('done') else 'pending')}] {m.get('text','')}"
                        for i, m in enumerate(milestones))
    ctx_text = "\n".join(f"  [{t.get('type','?')}] {t.get('label','')} (heat={t.get('heat',0)})"
                         for t in ctx_topics[:15]) or "  (no CTX activity)"
    log_text = "\n".join(f"  {l.get('date','')} — {l.get('text','')}"
                         for l in log_entries[-10:]) or "  (no log entries)"

    prompt = f"""You are analyzing a project's milestone completion status based on actual work evidence.

PROJECT: {proj_id}

MILESTONES (current state):
{ms_text}

RECENT WORK (CTX memory — git decisions, hot nodes):
{ctx_text}

PROGRESS LOG:
{log_text}

For each milestone, determine: is it DONE, IN_PROGRESS, or PENDING based on the evidence?
Rules:
- DONE: clear evidence it was completed (log entry, git commit, or explicit mention)
- IN_PROGRESS: work is actively happening on it but not finished
- PENDING: no evidence of work started

Respond in this exact JSON (no markdown):
[{{"text": "...", "done": true/false, "status": "DONE|IN_PROGRESS|PENDING", "reason": "1 short sentence"}}]

Return ALL {len(milestones)} milestones. done=true only for DONE status."""

    result = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: subprocess.run(
            ["claude", "-p", "--model", "claude-haiku-4-5", prompt],
            capture_output=True, text=True, timeout=120
        )
    )
    if result.returncode != 0:
        return JSONResponse({"ok": False, "error": result.stderr[:200] or "claude CLI failed"}, status_code=500)

    raw = result.stdout.strip()
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:])
    if raw.endswith("```"):
        raw = "\n".join(raw.split("\n")[:-1])

    updated = json.loads(raw.strip())

    # M570: write back via _save_project (SQLite primary, mtime cache invalidated)
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if md.exists():
        data = _parse_md_frontmatter(md)
        for i, ms in enumerate(updated):
            if i < len(data.get("milestones", [])):
                data["milestones"][i]["done"] = ms.get("done", False)
        _save_project(proj_id, data)

    return JSONResponse({"ok": True, "milestones": updated})


@app.post("/api/northstar/{proj_id}/update-current")
async def update_current(proj_id: str, request: Request):
    """Update the current metric value in north-star.md frontmatter."""
    body = await request.json()
    current = str(body.get("current", "")).strip()
    if not current:
        return JSONResponse({"ok": False, "error": "current value required"})

    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": f"project {proj_id} not found"})

    data = _parse_md_frontmatter(md)
    old = data.get("current", "—")
    if str(old) == current:
        return JSONResponse({"ok": True, "updated": False, "reason": "no change"})

    data["current"] = current
    _save_project(proj_id, data)  # M570
    return JSONResponse({"ok": True, "updated": True, "old": str(old), "new": current})


@app.post("/api/northstar/{proj_id}/session-log")
async def session_log(proj_id: str, request: Request):
    """Append a session summary entry to the project's north-star.md log."""
    body = await request.json()
    entry_text = body.get("text", "").strip()
    entry_date = body.get("date", "")
    if not entry_text or not entry_date:
        return JSONResponse({"ok": False, "error": "text and date required"})

    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": f"project {proj_id} not found"})

    data = _parse_md_frontmatter(md)
    log = data.get("log", [])
    # Avoid duplicate entries (same date + same text prefix)
    prefix = entry_text[:40]
    if not any(e.get("date") == entry_date and e.get("text","")[:40] == prefix for e in log):
        log.append({"date": entry_date, "text": entry_text})
        data["log"] = log
        _save_project(proj_id, data)  # M570
        return JSONResponse({"ok": True, "appended": True})
    return JSONResponse({"ok": True, "appended": False, "reason": "duplicate"})


@app.get("/api/northstar/{proj_id}/doc")
async def northstar_doc(proj_id: str):
    """Return the full markdown body of a project's north-star.md."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if md.exists():
        data = _parse_md_frontmatter(md)
        return JSONResponse({"ok": True, "body": data.get("_body", ""), "path": str(md)})
    return JSONResponse({"ok": False, "body": "", "path": ""})


@app.get("/api/ctx/state")
async def ctx_state():
    """M562: surface live CTX session state from ~/.claude/ctx-session-state.json.
    Hub-embedded CTX integration so the same data shows up alongside milestones."""
    try:
        p = Path.home() / ".claude" / "ctx-session-state.json"
        if not p.exists():
            return JSONResponse({"ok": False, "error": "ctx-session-state.json missing"}, status_code=404)
        return JSONResponse({"ok": True, "state": json.loads(p.read_text(encoding="utf-8"))})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/ctx/recent-retrievals")
async def ctx_recent_retrievals(limit: int = 50):
    """M562: tail of ~/.claude/ctx-retrieval-events.jsonl — latest CTX hits."""
    try:
        p = Path.home() / ".claude" / "ctx-retrieval-events.jsonl"
        if not p.exists():
            return JSONResponse({"ok": False, "error": "ctx-retrieval-events.jsonl missing"}, status_code=404)
        # Tail efficiently — read last ~64KB only
        size = p.stat().st_size
        with open(p, "rb") as f:
            seek_pos = max(0, size - 64 * 1024)
            f.seek(seek_pos)
            tail = f.read().decode("utf-8", errors="ignore")
        lines = tail.splitlines()[-limit:]
        events = []
        for ln in lines:
            try:
                events.append(json.loads(ln))
            except Exception:
                pass
        return JSONResponse({"ok": True, "count": len(events), "events": events})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/ctx-pulse")
async def ctx_pulse():
    """Pull recent work focus from CTX graph — hot nodes = actual work direction."""
    ctx_url = _ctx_url()
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{ctx_url}/api/graph")
            d = r.json()
        nodes = d.get("nodes", [])
        hot = sorted(nodes, key=lambda n: n.get("utility_heat_raw", 0), reverse=True)[:15]
        topics = [{"type": n.get("type","?"), "label": n.get("label",""), "heat": round(n.get("utility_heat_raw",0),2)} for n in hot]
        stats = d.get("stats", {})
        return JSONResponse({"topics": topics, "stats": stats, "ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "topics": []})


@app.post("/api/northstar/align")
async def northstar_align(request: Request):
    """Alignment check: does recent work direction match the north star?"""
    body = await request.json()
    project = body.get("project", {})
    topics = body.get("topics", [])

    topics_text = "\n".join(
        f"  [{t.get('type','?')}] {t.get('label','')} (heat={t.get('heat',0)})"
        for t in topics[:12]
    ) or "  (no recent activity)"

    prompt = f"""You are evaluating alignment between a project's north star goal and the developer's actual recent work.

PROJECT: {project.get('name','?')}
NORTH STAR METRIC: {project.get('metric','?')}
TARGET: {project.get('target','?')}
STATUS: {project.get('status','?')}
NOTES: {project.get('note','(none)')}

RECENT WORK ACTIVITY (from session memory, hot = frequently referenced):
{topics_text}

Evaluate:
1. Is the recent work ADVANCING the north star? (directly contributing)
2. Is it NEUTRAL? (infrastructure/maintenance that eventually helps)
3. Is it DIVERGENT? (pulling focus away from north star)

Respond in this exact JSON (no markdown):
{{"alignment": "ADVANCING|NEUTRAL|DIVERGENT", "score": 0, "summary": "one sentence", "gap": "what's missing or misaligned", "redirect": "specific action to realign work toward north star"}}

score: 0-100 (100 = perfectly aligned). Be honest and direct."""

    result = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: subprocess.run(
            ["claude", "-p", "--model", "claude-haiku-4-5", prompt],
            capture_output=True, text=True, timeout=120
        )
    )
    if result.returncode != 0:
        return JSONResponse({"error": result.stderr[:200] or "claude CLI failed"}, status_code=500)
    raw = result.stdout.strip()
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:])
    if raw.endswith("```"):
        raw = "\n".join(raw.split("\n")[:-1])
    return JSONResponse(json.loads(raw.strip()))


@app.post("/api/northstar/eval")
async def northstar_eval(request: Request):
    body = await request.json()
    project = body.get("project", {})

    milestones_text = "\n".join(
        f"  {'[x]' if m.get('done') else '[ ]'} {m.get('text','')}"
        for m in project.get("milestones", [])
    ) or "  (none)"

    log_text = "\n".join(
        f"  {l.get('date','')} — {l.get('text','')}"
        for l in project.get("log", [])[-5:]
    ) or "  (none)"

    prompt = f"""You are a multi-expert panel evaluating a project's North Star goal.
Analyze from exactly 5 lenses. Be concise, direct, specific to this project.

PROJECT: {project.get('name','?')}
NORTH STAR METRIC: {project.get('metric','?')}
CURRENT VALUE: {project.get('current','?')}
TARGET: {project.get('target','?')}
STATUS: {project.get('status','?')}
DEADLINE: {project.get('deadline','(not set)')}
NOTES: {project.get('note','(none)')}
MILESTONES:
{milestones_text}
PROGRESS LOG:
{log_text}

Respond in this EXACT JSON (no markdown, no extra text):
{{"lenses":[{{"name":"Clarity","icon":"◈","verdict":"PASS","summary":"one sentence","detail":"2-3 sentences"}},{{"name":"Feasibility","icon":"◉","verdict":"PASS","summary":"one sentence","detail":"2-3 sentences"}},{{"name":"Moat","icon":"◎","verdict":"PASS","summary":"one sentence","detail":"2-3 sentences"}},{{"name":"Leading Indicator","icon":"◇","verdict":"PASS","summary":"one sentence","detail":"2-3 sentences"}},{{"name":"Risk","icon":"△","verdict":"PASS","summary":"one sentence","detail":"2-3 sentences"}}],"verdict":"STRONG","top_action":"specific next action"}}

verdict per lens: PASS or WARN or FAIL. overall verdict: STRONG or MODERATE or WEAK."""

    result = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: subprocess.run(
            ["claude", "-p", "--model", "claude-haiku-4-5", prompt],
            capture_output=True, text=True, timeout=120
        )
    )

    if result.returncode != 0:
        return JSONResponse({"error": result.stderr[:200] or "claude CLI failed"}, status_code=500)

    raw = result.stdout.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:])
    if raw.endswith("```"):
        raw = "\n".join(raw.split("\n")[:-1])
    raw = raw.strip()

    parsed = json.loads(raw)
    return JSONResponse(parsed)


# ── Health checks ─────────────────────────────────────────────────────────────


# ── Market Signals — built-in page ───────────────────────────────────────────

_MARKET_SIGNALS_SCRIPT = Path.home() / "Project" / "CTX" / "scripts" / "market-signals.py"
_MARKET_SIGNALS_LOG    = Path.home() / "Project" / "CTX" / "docs" / "research" / "signal-log.jsonl"
_MS_CACHE: dict = {"data": None, "ts": 0.0}
_MS_TTL = 1800  # 30 min (pypistats rate limit: avoid >3 calls/5min)


def _fetch_signals_sync() -> dict:
    if not _MARKET_SIGNALS_SCRIPT.exists():
        return {"error": f"script not found: {_MARKET_SIGNALS_SCRIPT}"}
    try:
        r = subprocess.run(
            ["python3", str(_MARKET_SIGNALS_SCRIPT), "--json"],
            capture_output=True, text=True, timeout=25,
        )
        return json.loads(r.stdout) if r.returncode == 0 else {"error": r.stderr[:200]}
    except Exception as exc:
        return {"error": str(exc)}


def _load_signal_history(n: int = 10) -> list:
    if not _MARKET_SIGNALS_LOG.exists():
        return []
    lines = []
    try:
        for line in _MARKET_SIGNALS_LOG.read_text().splitlines():
            line = line.strip()
            if line:
                try:
                    lines.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return lines[-n:]


@app.get("/market-signals")
async def market_signals_page():
    return FileResponse(str(STATIC / "market-signals.html"),
                        headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

@app.get("/ns-table")
async def ns_table_test_page():
    """M939: Experimental table view for North Star projects."""
    return FileResponse(str(STATIC / "ns-table-test.html"),
                        headers={"Cache-Control": "no-store"})


@app.get("/ns-test2")
async def ns_test2_page():
    """M939 test2: Circle card swimlane view."""
    return FileResponse(str(STATIC / "ns-test2.html"),
                        headers={"Cache-Control": "no-store"})


@app.get("/api/market-signals")
async def market_signals_api(refresh: bool = False):
    import time
    now = time.time()
    if refresh or _MS_CACHE["data"] is None or (now - _MS_CACHE["ts"]) > _MS_TTL:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, _fetch_signals_sync)
        _MS_CACHE["data"] = data
        _MS_CACHE["ts"] = now
    return JSONResponse({
        "signals":   _MS_CACHE["data"],
        "history":   _load_signal_history(),
        "cached_at": _MS_CACHE["ts"],
        "ttl":       _MS_TTL,
    })


@app.post("/api/exec-complete")
async def record_exec_complete(request: Request):
    """M215 monetization: called by completion hook with agent/model/token_usage for a stone.
    Body: {proj_id, stone_id, agent, model, token_usage, session_id}
    """
    try:
        body = await request.json()
        proj_id = body.get("proj_id", "")
        stone_id = body.get("stone_id", "")
        agent = body.get("agent", "")
        model = body.get("model", "")
        token_usage = int(body.get("token_usage") or 0)
        if not proj_id or not stone_id:
            return JSONResponse({"ok": False, "error": "proj_id and stone_id required"}, status_code=400)
        import datetime as _dt_ec
        now = _dt_ec.datetime.utcnow().isoformat()
        today = now[:10]
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("""INSERT INTO stone_events(proj_id,stone_id,event_type,payload,ts,agent,model,token_usage)
            VALUES(?,?,?,?,?,?,?,?)""",
            (proj_id, stone_id, "exec_complete",
             json.dumps({"agent": agent, "model": model, "token_usage": token_usage}),
             now, agent, model, token_usage or None))
        conn.execute("""UPDATE stones_snapshot SET agent=?, model=?, total_tokens=COALESCE(total_tokens,0)+?
            WHERE proj_id=? AND stone_id=?""", (agent, model, token_usage, proj_id, stone_id))
        if token_usage:
            conn.execute("""INSERT INTO daily_metrics(date,proj_id,total_tokens) VALUES(?,?,?)
                ON CONFLICT(date,proj_id) DO UPDATE SET total_tokens=total_tokens+?""",
                (today, proj_id, token_usage, token_usage))
        conn.commit(); conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/upload-comment-file")
async def upload_comment_file(file: UploadFile = File(...), proj_id: str = Form("")):
    """M281: Accept a file drop from the comment box.
    Saves to ~/.claude/hub/uploads/<proj_id>/ and returns a local URL.
    """
    import shutil as _shutil
    upload_dir = _HUB_DATA_DIR / "uploads" / (proj_id or "shared")
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]", "_", file.filename or "file")
    dest = upload_dir / safe_name
    # Avoid overwrite
    if dest.exists():
        stem, suffix = dest.stem, dest.suffix
        i = 1
        while dest.exists():
            dest = upload_dir / f"{stem}_{i}{suffix}"
            i += 1
    with dest.open("wb") as f:
        _shutil.copyfileobj(file.file, f)
    url = f"/uploads/{proj_id or 'shared'}/{dest.name}"
    return JSONResponse({"ok": True, "url": url, "filename": dest.name, "path": str(dest)})


@app.post("/api/northstar/{proj_id}/upload")
async def upload_project_avatar(proj_id: str, file: UploadFile = File(...)):
    """M525: Project-scoped file upload (avatar image).
    Saves to ~/.claude/hub/uploads/<proj_id>/ and returns a local URL.
    Only affects the current project card — cross-card character placement is future work.
    """
    import shutil as _shutil
    upload_dir = _HUB_DATA_DIR / "uploads" / proj_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]", "_", file.filename or "file")
    dest = upload_dir / safe_name
    if dest.exists():
        stem, suffix = dest.stem, dest.suffix
        i = 1
        while dest.exists():
            dest = upload_dir / f"{stem}_{i}{suffix}"
            i += 1
    with dest.open("wb") as f:
        _shutil.copyfileobj(file.file, f)
    url = f"/uploads/{proj_id}/{dest.name}"
    return JSONResponse({"ok": True, "url": url, "filename": dest.name})


@app.get("/api/northstar/{proj_id}/uploads")
async def list_project_uploads(proj_id: str):
    """M525: List hub-shared character images (uploads/shared/chars/ — reusable across all projects)."""
    IMAGE_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".avif"}
    chars_dir = _HUB_DATA_DIR / "uploads" / "shared" / "chars"
    if not chars_dir.is_dir():
        return JSONResponse({"files": []})
    files = []
    for p in sorted(chars_dir.iterdir(), key=lambda x: -x.stat().st_mtime):
        if p.suffix.lower() in IMAGE_EXT:
            files.append({"filename": p.name, "url": f"/uploads/shared/chars/{p.name}"})
    return JSONResponse({"files": files})


@app.post("/api/northstar/{proj_id}/upload-char")
async def upload_char_image(proj_id: str, request: Request):
    """M525: Upload a character/avatar image to hub-shared chars pool (available to all projects)."""
    form = await request.form()
    file = form.get("file")
    if not file:
        return JSONResponse({"error": "no file"}, status_code=400)
    chars_dir = _HUB_DATA_DIR / "uploads" / "shared" / "chars"
    chars_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(file.filename).suffix or ".png"
    dest = chars_dir / f"{int(__import__('time').time()*1000)}{suffix}"
    dest.write_bytes(await file.read())
    return JSONResponse({"url": f"/uploads/shared/chars/{dest.name}"})


@app.get("/uploads/shared/chars/{filename}")
async def serve_shared_char(filename: str):
    p = _HUB_DATA_DIR / "uploads" / "shared" / "chars" / filename
    if p.exists():
        return FileResponse(str(p))
    return JSONResponse({"error": "not found"}, status_code=404)


@app.get("/uploads/{proj_id}/{filename}")
async def serve_upload(proj_id: str, filename: str):
    p = _HUB_DATA_DIR / "uploads" / proj_id / filename
    if p.exists():
        return FileResponse(str(p))
    return JSONResponse({"error": "not found"}, status_code=404)


@app.post("/api/market-signals/save")
async def market_signals_save():
    """Append current signals to signal-log.jsonl."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _fetch_signals_sync)
    import time, json as _json
    data["ts"] = data.get("ts") or time.time()
    try:
        with _MARKET_SIGNALS_LOG.open("a") as f:
            f.write(_json.dumps(data) + "\n")
        return JSONResponse({"ok": True})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)})


_PYPI_DAILY_CACHE: dict = {"data": None, "ts": 0.0}
_PYPI_DAILY_TTL = 3600  # 1 hr
_PYPI_DAILY_PKG_CACHE: dict = {}  # pkg -> {"data": ..., "ts": ...}


def _fetch_pypi_daily_sync(days: int = 30, pkg: str = "claude-ns-hub") -> dict:
    import urllib.request as _ur
    try:
        req = _ur.Request(
            f"https://pypistats.org/api/packages/{pkg}/overall?total=daily",
            headers={"User-Agent": "ctx-hub/1.0", "Accept": "application/json"},
        )
        with _ur.urlopen(req, timeout=10) as r:
            raw = json.loads(r.read())
        rows = [
            {"date": row["date"], "downloads": row["downloads"]}
            for row in raw.get("data", [])
            if row.get("category") == "without_mirrors"
        ]
        rows.sort(key=lambda x: x["date"])
        return {"days": rows[-days:], "package": pkg}
    except Exception as exc:
        return {"error": str(exc)[:120]}


@app.get("/api/pypi-daily")
async def pypi_daily_api(refresh: bool = False, days: int = 30, pkg: str = "claude-ns-hub"):
    import time
    now = time.time()
    cache = _PYPI_DAILY_PKG_CACHE.setdefault(pkg, {"data": None, "ts": 0.0})
    if refresh or cache["data"] is None or (now - cache["ts"]) > _PYPI_DAILY_TTL:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: _fetch_pypi_daily_sync(days, pkg))
        # M928: on error keep previous successful data; only update cache on success
        if "error" in data and cache["data"] and "days" in cache["data"]:
            cache["data"] = {**cache["data"], "error": data["error"], "stale": True}
        else:
            cache["data"] = data
            if "days" in data:
                cache["ts"] = now  # only refresh timestamp on success
    # keep legacy single-cache in sync for backward compat
    if pkg == "claude-ns-hub":
        _PYPI_DAILY_CACHE["data"] = cache["data"]
        _PYPI_DAILY_CACHE["ts"] = cache["ts"]
    return JSONResponse({**cache["data"], "cached_at": cache["ts"], "pkg": pkg})


# ── Channel Manager — live reaction scraper ──────────────────────────────────

_CHANNEL_LOG = Path.home() / "Project" / "CTX" / "docs" / "marketing" / "channel_reactions_log.md"
_POSTS_DRAFT  = Path.home() / "Project" / "CTX" / "docs" / "marketing" / "20260502-channel-posts-draft.md"
_CH_CACHE: dict = {"data": None, "ts": 0.0}
_CH_TTL = 600  # 10 min (channels: GH/GN/HN don't change faster; PyPI rate-limit safe)

CHANNELS = [
    {
        "id": "github",
        "name": "GitHub",
        "url": "https://github.com/jaytoone/CTX",
        "api": "https://api.github.com/repos/jaytoone/CTX",
        "type": "github_api",
    },
    {
        "id": "geeknews",
        "name": "GeekNews",
        "url": "https://news.hada.io/topic?id=29124",
        "api": "https://news.hada.io/topic?id=29124",
        "type": "html_scrape",
    },
    {
        "id": "hn",
        "name": "Hacker News",
        "url": "https://news.ycombinator.com/item?id=48071940",
        "api": "https://hn.algolia.com/api/v1/items/48071940",
        "type": "hn_api",
    },
    {
        "id": "devto",
        "name": "Dev.to",
        "url": "https://dev.to/jaewon_jang_d63fddcf69ac2/ctx-i-gave-claude-code-a-memory-that-actually-works-45id",
        "api": "https://dev.to/api/articles/2597891",
        "type": "devto_api",
    },
    {
        "id": "linkedin",
        "name": "LinkedIn",
        "url": "https://linkedin.com/feed/",
        "api": None,
        "type": "manual",
    },
    {
        "id": "pinterest",
        "name": "Pinterest",
        "url": "https://www.pinterest.com/",
        "api": None,
        "type": "manual",
    },
    {
        "id": "naver_mail",
        "name": "Naver Mail",
        "url": "https://mail.naver.com/",
        "api": "https://mail.naver.com/json/readMail/",
        "type": "naver_mail",
        "note": "GitHub/GN/HN reply notifications (set NAVER_MAIL_USER / NAVER_APP_PW)",
    },
    {
        "id": "pypi",
        "name": "PyPI",
        "url": "https://pypi.org/project/ctx-retriever/",
        "api": "https://pypistats.org/api/packages/ctx-retriever/recent",
        "type": "pypi_api",
    },
    # M290 v5/v6: hub-specific channels (claude-ns-hub) — independent from ctx
    {
        "id": "hub_pypi",
        "name": "PyPI (claude-ns-hub)",
        "url": "https://pypi.org/project/claude-ns-hub/",
        "api": "https://pypistats.org/api/packages/claude-ns-hub/recent",
        "type": "pypi_api",
    },
    {
        "id": "hub_github",
        "name": "GitHub (claude-ns-hub)",
        "url": "https://github.com/jaytoone/claude-ns-hub",
        "api": "https://api.github.com/repos/jaytoone/claude-ns-hub",
        "type": "github_api",
    },
    {
        "id": "hub_hn",
        "name": "Hacker News (claude-ns-hub)",
        "url": "https://news.ycombinator.com/item?id=42800000",
        "type": "manual",
        "note": "search HN for 'claude-ns-hub' mentions",
    },
]


def _fetch_channel(ch: dict) -> dict:
    result = {"id": ch["id"], "name": ch["name"], "url": ch["url"], "type": ch["type"]}
    if ch["type"] == "manual":
        result["note"] = "manual check required"
        return result
    import urllib.request as _ur
    try:
        req = _ur.Request(ch["api"], headers={"User-Agent": "ctx-channel-monitor/1.0"})
        with _ur.urlopen(req, timeout=6) as r:
            raw = json.loads(r.read())
        if ch["type"] == "github_api":
            result["stars"]    = raw.get("stargazers_count", 0)
            result["forks"]    = raw.get("forks_count", 0)
            result["issues"]   = raw.get("open_issues_count", 0)
            result["watchers"] = raw.get("watchers_count", 0)
        elif ch["type"] == "hn_api":
            result["points"]   = raw.get("points") or 0
            result["comments"] = len(raw.get("children") or [])
        elif ch["type"] == "devto_api":
            result["reactions"] = raw.get("public_reactions_count", 0)
            result["comments"]  = raw.get("comments_count", 0)
            result["reads"]     = raw.get("page_views_count", 0)
        elif ch["type"] == "pypi_api":
            d = raw.get("data", {})
            result["day"]   = d.get("last_day", 0)
            result["week"]  = d.get("last_week", 0)
            result["month"] = d.get("last_month", 0)
    except Exception as exc:
        err = str(exc)
        if "429" in err:
            result["note"] = "rate limited — retry later"
        else:
            result["error"] = err[:80]

    # Naver mail: IMAP check (app password required — set NAVER_APP_PW env var)
    if ch["type"] == "naver_mail":
        try:
            import imaplib, ssl as _ssl, os as _os
            app_pw = _os.environ.get("NAVER_APP_PW", "")
            naver_user = _os.environ.get("NAVER_MAIL_USER", "")
            ctx2 = _ssl.create_default_context()
            mail = imaplib.IMAP4_SSL("imap.naver.com", 993, ssl_context=ctx2)
            mail.login(naver_user, app_pw)
            mail.select("INBOX")
            _, unseen = mail.search(None, "UNSEEN")
            result["unread"] = len(unseen[0].split()) if unseen[0] else 0
            # Search for CTX-related subjects
            ctx_subjects = []
            for keyword in [b"CTX", b"GitHub", b"GeekNews"]:
                try:
                    _, ids = mail.search(None, "UNSEEN", f"SUBJECT \"{keyword.decode()}\"".encode())
                    if ids[0]:
                        ctx_subjects.append(f"{keyword.decode()}:{len(ids[0].split())}")
                except Exception:
                    pass
            result["ctx_unread"] = " ".join(ctx_subjects) if ctx_subjects else "0"
            mail.logout()
        except Exception as exc2:
            result["error"] = str(exc2)[:60]
        return result

    # HTML scrape fallback for channels that don't return JSON
    if ch["type"] == "html_scrape":
        result.pop("error", None)
        try:
            import re as _re
            import urllib.request as _urllib_req
            req = _urllib_req.Request(
                ch["api"], headers={"User-Agent": "ctx-channel-monitor/1.0"}
            )
            with _urllib_req.urlopen(req, timeout=6) as r:
                html = r.read().decode("utf-8", errors="replace")
            # GeekNews: point count in <span class="score"> or similar
            m = _re.search(r'class="[^"]*point[^"]*"[^>]*>\s*(\d+)', html)
            if not m:
                m = _re.search(r'(\d+)\s*포인트|(\d+)\s*point', html, _re.I)
            result["points"] = int(m.group(1) or m.group(2)) if m else 0
            # Comments: count <li class="comment or similar
            result["comments"] = len(_re.findall(r'class="[^"]*comment', html))
        except Exception as exc2:
            result["error"] = str(exc2)[:80]
    return result


def _fetch_all_channels() -> list:
    results = []
    for ch in CHANNELS:
        results.append(_fetch_channel(ch))
    return results


def _load_posts_draft() -> str:
    if _POSTS_DRAFT.exists():
        return _POSTS_DRAFT.read_text()[:8000]
    return ""


@app.get("/api/channel-status")
async def channel_status(refresh: bool = False):
    """Live reaction metrics across all CTX distribution channels."""
    import time
    now = time.time()
    if refresh or _CH_CACHE["data"] is None or (now - _CH_CACHE["ts"]) > _CH_TTL:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, _fetch_all_channels)
        _CH_CACHE["data"] = data
        _CH_CACHE["ts"] = now
    return JSONResponse({
        "channels":  _CH_CACHE["data"],
        "cached_at": _CH_CACHE["ts"],
        "ttl":       _CH_TTL,
    })


@app.get("/api/channel-posts")
async def channel_posts():
    """Return post drafts markdown."""
    loop = asyncio.get_event_loop()
    content = await loop.run_in_executor(None, _load_posts_draft)
    return JSONResponse({"content": content})


@app.post("/api/channel-log")
async def channel_log_save(request: Request):
    """Append a manual reaction update to channel_reactions_log.md."""
    body = await request.json()
    entry = f"\n\n## Manual Update — {body.get('date', 'unknown')}\n"
    for k, v in body.items():
        if k != "date":
            entry += f"- **{k}**: {v}\n"
    try:
        with _CHANNEL_LOG.open("a") as f:
            f.write(entry)
        return JSONResponse({"ok": True})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)})


CELI_CONFIG = Path.home() / ".config" / "tmux-csk-sessions.conf"

def _sync_substars_to_claude_md(proj_id: str, proj: dict) -> None:
    """M532: Write current sub-stars between NS_SUBSTARS markers in project CLAUDE.md.
    Agents reading the project CLAUDE.md at session start see the current north star goals."""
    proj_dir_str = _get_project_dir(proj_id)
    if not proj_dir_str:
        return
    claude_md = Path(proj_dir_str) / "CLAUDE.md"
    if not claude_md.exists():
        return
    ns_list = proj.get("north_stars") or []
    main_name = proj.get("name", proj_id)
    main_metric = proj.get("metric", "")
    lines = [f"## North Star Sub-goals — {main_name} (auto-synced by hub)"]
    if main_metric:
        lines.append(f"- **메인 목표**: {main_metric}")
    if ns_list:
        for ns in ns_list:
            name = ns.get("name", "")
            metric = ns.get("metric", "")
            target = ns.get("target", "")
            status = ns.get("status", "")
            label = f"- **{name}**"
            if metric:
                label += f": {metric}"
            if target:
                label += f" → 목표: {target}"
            if status:
                label += f" [{status}]"
            lines.append(label)
    else:
        lines.append("- (서브 스타 없음)")
    block = "\n".join(lines)
    content = claude_md.read_text(encoding="utf-8")
    start_tag = "<!-- NS_SUBSTARS_START -->"
    end_tag = "<!-- NS_SUBSTARS_END -->"
    if start_tag in content and end_tag in content:
        before = content[:content.index(start_tag) + len(start_tag)]
        after = content[content.index(end_tag):]
        content = before + "\n" + block + "\n" + after
    else:
        content = content.rstrip() + f"\n\n{start_tag}\n{block}\n{end_tag}\n"
    claude_md.write_text(content, encoding="utf-8")


def _get_project_dir(proj_id: str) -> str | None:
    """Find the project directory from celi config or projects dir."""
    # 1. Scan celi sessions config
    if CELI_CONFIG.exists():
        for line in CELI_CONFIG.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            parts = line.split(':')
            if len(parts) >= 3:
                session_name, branch, proj_path = parts[0], parts[1], ':'.join(parts[2:])
                if proj_id.lower() in session_name.lower() or proj_id.lower() in proj_path.lower():
                    p = Path(proj_path)
                    if p.exists(): return str(p)
    # 2. Common base paths (M600: LT-1 SFTP mount added)
    for base in [
        Path.home() / "Project",
        Path.home() / "Project/VIDraft",
        Path.home() / "mnt/lt1-vidraft",  # LT-1 C:\Project\VIDraft via rclone SFTP mount
    ]:
        p = base / proj_id
        if p.exists(): return str(p)
        # Case-insensitive scan
        if base.exists():
            for d in base.iterdir():
                if d.name.lower() == proj_id.lower() and d.is_dir():
                    return str(d)
    return None


# Persistent session registry: proj_id → PtyProcess (survives WS disconnect)
_sessions: dict[str, "ptyprocess.PtyProcess"] = {}
# PTY runtime registry: proj_id → agent runtime used for the current PTY
_pty_agents: dict[str, str] = {}
# M183: track which session_id is running in each PTY so resume requests can force respawn
_pty_session_ids: dict[str, str | None] = {}
# Scrollback buffer: proj_id → accumulated PTY output (last 512 KB, M804)
_buffers: dict[str, list] = {}
_BUFFER_MAX = 524288
# Idle tracking: proj_id → timestamp of last WS detach
_session_idle_since: dict[str, float] = {}
# M181: PTY busy tracking — last time we saw "esc to interrupt" in the byte stream.
# Updated by the background drain task. PTY is "active" if this timestamp is fresh (<3s).
_pty_last_busy_ts: dict[str, float] = {}
# M182: Background drain task per project — single reader on the PTY fd.
# Runs continuously while the PTY is in _sessions, regardless of WS attachment.
# WS clients subscribe via _pty_subscribers to get a copy of each chunk.
_pty_drain_tasks: dict[str, "asyncio.Task"] = {}
_pty_subscribers: dict[str, set] = {}  # proj_id → set of asyncio.Queue
_SESSION_TTL = 30 * 60  # kill after 30 min idle


def _kill_session(proj_id: str) -> None:
    """Terminate a session and clean up all registries."""
    proc = _sessions.pop(proj_id, None)
    _buffers.pop(proj_id, None)
    _session_idle_since.pop(proj_id, None)
    _pty_last_busy_ts.pop(proj_id, None)  # M181: clear busy stamp
    # M182: cancel background drain (will exit cleanly when proc is gone)
    task = _pty_drain_tasks.pop(proj_id, None)
    if task and not task.done():
        try: task.cancel()
        except Exception: pass
    _pty_subscribers.pop(proj_id, None)
    _pty_agents.pop(proj_id, None)
    _pty_session_ids.pop(proj_id, None)
    if proc:
        try:
            proc.terminate(force=True)
        except Exception:
            pass


@app.on_event("startup")
async def _check_for_updates():
    """M292 P1: Check PyPI for newer version of northstar-hub on startup; log banner if found."""
    import importlib.metadata as _meta
    import asyncio as _aio
    async def _do_check():
        await _aio.sleep(5)  # defer so server is fully up first
        try:
            import urllib.request as _ur
            current = _meta.version("northstar-hub")
            data = _ur.urlopen("https://pypi.org/pypi/northstar-hub/json", timeout=5).read()
            latest = json.loads(data)["info"]["version"]
            if latest != current:
                print(f"\n⟳ northstar-hub {latest} available (current: {current})\n"
                      f"  pip install --upgrade northstar-hub\n", flush=True)
        except Exception:
            pass  # offline or not installed as package — silent
    asyncio.create_task(_do_check())


@app.on_event("startup")
async def _seed_sqlite_from_yaml():
    """M287: Migrate existing YAML north-star.md files to SQLite on first startup."""
    import asyncio as _aio
    loop = _aio.get_event_loop()
    await loop.run_in_executor(None, _migrate_yaml_to_sqlite)


@app.on_event("startup")
async def _startup_telemetry():
    """M929: Record startup event for usage analytics (consent-gated, no PII)."""
    _record_usage_event("hub_start")

@app.on_event("startup")
async def _expose_ports_to_tailscale():
    """Auto-expose hub ports to all online Tailscale Windows clients on startup."""
    import subprocess
    for port in [9000]:
        try:
            subprocess.run(
                [str(Path.home() / ".local/bin/wsl-expose"), str(port)],
                capture_output=True, timeout=10
            )
        except Exception:
            pass  # wsl-expose optional — fail silently if clients offline


@app.on_event("startup")
async def _ensure_litellm_proxy():
    """M506: Auto-start LiteLLM proxy on port 4100 if not running.
    Uses ~/.rsk-litellm.yaml config + API keys from ~/.claude/env/shared.env."""
    import shutil as _shutil
    port = 4100
    config = Path.home() / ".rsk-litellm.yaml"
    log = Path.home() / ".rsk-litellm.log"
    if not config.exists():
        return  # no config → skip silently
    if not _shutil.which("litellm"):
        return  # litellm not installed → skip
    try:
        result = subprocess.run(
            ["ss", "-tlnH", f"sport = :{port}"], capture_output=True, text=True, timeout=2
        )
        if "LISTEN" in result.stdout:
            return  # already running
    except Exception:
        pass
    # Load API keys from shared.env
    env = dict(os.environ)
    shared_env = Path.home() / ".claude" / "env" / "shared.env"
    if shared_env.exists():
        try:
            for line in shared_env.read_text().splitlines():
                line = line.strip()
                if line.startswith("export "):
                    line = line[7:]
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip().strip('"').strip("'")
        except Exception:
            pass
    try:
        log.parent.mkdir(parents=True, exist_ok=True)
        subprocess.Popen(
            ["litellm", "--config", str(config), "--port", str(port), "--host", "127.0.0.1"],
            stdout=open(str(log), "a"),
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
    except Exception:
        pass


@app.on_event("startup")
async def _start_milestone_watcher():
    """Background poller (every 5 min): auto-ack pending milestones + promote answered clarifications + completion-log sync."""
    async def _watch():
        while True:
            await asyncio.sleep(300)  # 5 minutes
            try:
                for proj_dir in PROJECTS_DIR.iterdir():
                    if not proj_dir.is_dir():
                        continue
                    proj_id = proj_dir.name
                    md = PROJECTS_DIR / proj_id / "north-star.md"
                    if not md.exists():
                        continue
                    proj = _parse_md_frontmatter(md)
                    if not isinstance(proj, dict) or not proj.get("name"):
                        continue
                    raw_ms = proj.get("milestones", [])
                    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M")
                    changed = False

                    # Step 1: Auto-ack new unreviewed milestones (ack only — keep status=pending)
                    # Promotion to "queued" is now ONLY done via Execute button (per user policy)
                    for m in raw_ms:
                        if not isinstance(m, dict): continue
                        if m.get("claude_ack") or m.get("status") in ("done", "pending_confirmation", "queued", "needs_clarification"):
                            continue
                        if m.get("status") != "pending": continue
                        m["claude_ack"] = now_iso
                        # Vague text still gets routed to needs_clarification (not queued)
                        text = str(m.get("text", "")).strip()
                        if len(text) <= 15:
                            m["status"] = "needs_clarification"
                        # Clear text: stays pending — user must click Execute to promote to queued
                        changed = True

                    # Step 3: Promote answered clarifications → pending
                    for m in raw_ms:
                        if not isinstance(m, dict): continue
                        if m.get("status") == "needs_clarification" and m.get("clarification_answer"):
                            m["status"] = "pending"
                            m["claude_ack"] = None  # reset so it gets re-reviewed
                            changed = True

                    # Completion-log sync: mark logged milestones as pending_confirmation.
                    # M195 fix: only use entries written in the last 30 min to prevent
                    # stale log entries from re-confirming reopened stones.
                    log_file = proj_dir / "completion-log.jsonl"
                    if log_file.exists():
                        entries = []
                        import datetime as _dt_cl
                        cutoff = (_dt_cl.datetime.utcnow() - _dt_cl.timedelta(minutes=30)).isoformat()
                        for line in log_file.read_text().splitlines():
                            line = line.strip()
                            if line:
                                try:
                                    e = json.loads(line)
                                    # Only include recent entries
                                    if e.get("timestamp", "9999") >= cutoff:
                                        entries.append(e)
                                except Exception: pass
                        logged_mids = {e.get("milestone_id") for e in entries if e.get("milestone_id")}
                        for m in raw_ms:
                            if not isinstance(m, dict): continue
                            mid = str(m.get("id", "")).strip()
                            status = m.get("status", "pending")
                            # M470: also exclude `queued` and `needs_clarification` — if the user
                            # has explicitly re-queued a previously-completed stone (or it is
                            # awaiting clarification reply), the recent completion-log entry
                            # must NOT force-revert it back to pending_confirmation. Without
                            # this guard the queue badge can flip back to "queue" within the
                            # 5-minute poll cycle, causing the "queue click sometimes flips
                            # off" bug.
                            if status in ("done", "pending_confirmation", "queued", "needs_clarification") or m.get("done"): continue
                            if mid in logged_mids:
                                m.update({"status": "pending_confirmation", "done": False,
                                          "pending_confirm_at": now_iso, "claude_ack": m.get("claude_ack") or now_iso})
                                changed = True

                    if changed:
                        proj["milestones"] = raw_ms
                        _save_project(proj_id, proj)

                    # M150: detect stones with a user message awaiting Claude reply
                    # (last conv entry is user, no claude reply after it). If the exec
                    # session is alive AND idle, re-fire the trigger so Claude is forced
                    # to read the REPLY PROTOCOL again. Prevents silently skipped replies.
                    pending_reply_ids = []
                    for m in raw_ms:
                        if not isinstance(m, dict): continue
                        # M225: held stones are completely excluded from claude token consumption
                        if m.get("held"): continue
                        conv = m.get("conversation") or []
                        if conv and isinstance(conv, list) and conv[-1].get("role") == "user":
                            pending_reply_ids.append(m.get("id"))
                    if pending_reply_ids:
                        session_name = _live_exec_session_name(proj_id)
                        check = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
                        if check.returncode == 0:
                            # M149: append-only queue (one entry per write)
                            # M172: enforce comment-reply protocol (3-line max, details to doc)
                            _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
                            _qf.parent.mkdir(parents=True, exist_ok=True)
                            from datetime import datetime as _dt_rep
                            _entry = json.dumps({
                                "ts": _dt_rep.now().isoformat(),
                                "body": (
                                    f"[REPLY SYNC] {len(pending_reply_ids)} stone(s) await your reply: "
                                    f"{', '.join(pending_reply_ids[:5])}.\n\n"
                                    f"PROTOCOL (docs/ns-comment-reply-protocol.md):\n"
                                    f"  1. Reply ONLY to stones whose last conversation entry has role=user\n"
                                    f"  2. Reply length ≤ 3 lines. No code blocks, no preamble\n"
                                    f"  3. If details > 3 lines needed: write docs/ns-replies/<DATE>-<MID>.md\n"
                                    f"     and reference the path in the reply\n"
                                    f"  4. Do NOT add arbitrary claude_comment / append_message to other stones\n"
                                    f"  5. Replying does NOT change the stone's status\n"
                                    f"  6. M270: If stone is a CLEAR TASK → 1-line ack only, NO questions.\n"
                                    f"     Ask a question ONLY when critical info is genuinely missing.\n"
                                    f"     Prefer silence over unnecessary questions (token waste).\n\n"
                                    f"  7. LANGUAGE: reply in the same language as the stone text.\n"
                                    f"     Korean stone → Korean reply. English stone → English reply.\n\n"
                                    f"ACTION: GET http://100.119.82.4:9000/api/northstar/{proj_id}/milestones,\n"
                                    f"  for each stone above, read its last user message, then\n"
                                    f"  PATCH milestones/<MID> with append_message {{role:'claude', text:'<≤3 lines>'}}.\n"
                                    f"Mandatory — do it now before any other work."
                                ),
                            }, ensure_ascii=False)
                            with _qf.open("a", encoding="utf-8") as _qh:
                                _qh.write(_entry + "\n")
            except Exception:
                pass
    asyncio.create_task(_watch())


@app.on_event("startup")
async def _start_event_cleanup():
    """M572: Periodic cleanup of old stone_events rows.
    stone_updated events: 3-day retention (debug noise).
    Other events (status_changed, created, comment, etc.): 90-day retention.
    Runs once at startup then every 24h."""
    async def _cleanup():
        while True:
            try:
                import datetime as _dt_cl
                cutoff_updated = (_dt_cl.datetime.utcnow() - _dt_cl.timedelta(days=3)).isoformat()
                cutoff_other   = (_dt_cl.datetime.utcnow() - _dt_cl.timedelta(days=90)).isoformat()
                conn = sqlite3.connect(str(_NS_EVENTS_DB), timeout=5)
                deleted_updated = conn.execute(
                    "DELETE FROM stone_events WHERE event_type='stone_updated' AND ts < ?",
                    (cutoff_updated,)
                ).rowcount
                deleted_other = conn.execute(
                    "DELETE FROM stone_events WHERE event_type != 'stone_updated' AND ts < ?",
                    (cutoff_other,)
                ).rowcount
                conn.commit()
                conn.close()
                if deleted_updated or deleted_other:
                    print(f"[hub] event cleanup: -{deleted_updated} stone_updated (>3d), -{deleted_other} other (>90d)", file=sys.stderr)
            except Exception:
                pass
            await asyncio.sleep(86400)  # 24h
    # Run first pass after 60s (let startup settle)
    async def _delayed_start():
        await asyncio.sleep(60)
        await _cleanup()
    asyncio.create_task(_delayed_start())


def _spawn_ctx_dashboard() -> "subprocess.Popen | None":
    """M55: auto-start ctx-dashboard (port 8787) alongside hub.
    Set CTX_DASHBOARD_DISABLED=1 to skip.
    """
    if os.environ.get("CTX_DASHBOARD_DISABLED", "").strip() in ("1", "true", "yes"):
        return None
    # Check if already running on 8787
    try:
        import socket as _sock
        s = _sock.socket()
        s.settimeout(0.5)
        r = s.connect_ex(("127.0.0.1", 8787))
        s.close()
        if r == 0:
            return None  # already running
    except Exception:
        pass
    # Path resolution: ~/.hub/ctx-dashboard (hub-native location) first, then fallbacks
    candidates = [
        str(Path.home() / ".hub" / "ctx-dashboard" / "server.py"),
        str(Path.home() / ".claude" / "hooks" / "ctx-dashboard" / "server.py"),
        str(Path("/home/desk-1/Project/CTX/src/dashboard/server.py")),
    ]
    server_path = next((p for p in candidates if Path(p).exists()), None)
    if not server_path:
        return None
    # Bind to 0.0.0.0 so LT-1 and other Tailscale peers can reach port 8787 directly
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "server:app",
             "--host", "0.0.0.0", "--port", "8787", "--app-dir", str(Path(server_path).parent)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc
    except Exception:
        return None


@app.on_event("startup")
async def _auto_start_ctx_dashboard():
    """M60: CTX dashboard is now mounted directly — no subprocess needed."""
    print("[hub] CTX dashboard: fully integrated via app.mount('/ctx') — no separate process", file=sys.stderr)


@app.api_route("/ctx/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
async def _ctx_proxy(request: Request, path: str):
    """M55: transparent reverse-proxy to ctx-dashboard (127.0.0.1:8787).
    Makes CTX tab same-origin — no CORS, no cross-origin postMessage needed.
    SSE (/stream) is forwarded as a true streaming response.
    """
    target = f"http://127.0.0.1:8787/{path}"
    params = str(request.url.query)
    if params:
        target += f"?{params}"
    is_sse = path == "stream" or "text/event-stream" in request.headers.get("accept", "")
    try:
        body = await request.body()
        fwd_headers = {k: v for k, v in request.headers.items()
                       if k.lower() not in ("host", "content-length")}
        if is_sse:
            # True streaming: keep httpx connection open, yield chunks as they arrive
            client = httpx.AsyncClient(timeout=None)
            async def _sse_generator():
                try:
                    async with client.stream(request.method, target,
                                             headers=fwd_headers, content=body) as r:
                        async for chunk in r.aiter_bytes(chunk_size=256):
                            yield chunk
                finally:
                    await client.aclose()
            return StreamingResponse(_sse_generator(), media_type="text/event-stream",
                                     headers={"Cache-Control": "no-cache",
                                              "X-Accel-Buffering": "no"})
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.request(method=request.method, url=target,
                                     headers=fwd_headers, content=body)
            ct = r.headers.get("content-type", "")
            content = r.content
            # Rewrite absolute API paths so browser fetches go through /ctx/ proxy,
            # not directly to hub's own /api/* or /stream endpoints.
            if "text/html" in ct or "javascript" in ct:
                try:
                    text = content.decode("utf-8")
                    text = text.replace('"/api/', '"/ctx/api/')
                    text = text.replace("'/api/", "'/ctx/api/")
                    text = text.replace('"/stream"', '"/ctx/stream"')
                    text = text.replace("'/stream'", "'/ctx/stream'")
                    text = text.replace('"/static/', '"/ctx/static/')
                    # Cache-bust: append version query to app.js in HTML so browsers
                    # never serve stale cached JS even without hard-refresh
                    if "text/html" in ct:
                        import time as _t_cb
                        _cbv = str(int(_t_cb.time()) // 3600)  # changes every hour
                        text = text.replace('src="/ctx/static/app.js"',
                                            f'src="/ctx/static/app.js?v={_cbv}"')
                        text = text.replace("src='/ctx/static/app.js'",
                                            f"src='/ctx/static/app.js?v={_cbv}'")
                    content = text.encode("utf-8")
                except Exception:
                    pass
            # Strip hop-by-hop / size headers — FastAPI sets correct content-length
            _skip_headers = {"content-length", "transfer-encoding", "content-encoding",
                             "connection", "keep-alive", "te", "trailers", "upgrade",
                             "etag", "last-modified", "cache-control"}
            fwd_resp_headers = {k: v for k, v in r.headers.items()
                                if k.lower() not in _skip_headers}
            # Force no-cache for rewritten HTML/JS so browsers never serve stale paths
            if "text/html" in ct or "javascript" in ct:
                fwd_resp_headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
                fwd_resp_headers["Pragma"] = "no-cache"
            return Response(content=content, status_code=r.status_code,
                            headers=fwd_resp_headers,
                            media_type=ct)
    except Exception:
        return JSONResponse({"ok": False, "error": "ctx-dashboard not running"}, status_code=502)


@app.get("/ctx")
async def _ctx_proxy_root(request: Request):
    """Root redirect: /ctx → /ctx/ so relative assets resolve correctly."""
    return Response(status_code=307, headers={"Location": "/ctx/"})


@app.on_event("startup")
async def _auto_start_entity_corpus():
    """M275: spawn entity-corpus alongside hub so it doesn't need a separate launch."""
    import atexit, asyncio as _aio
    # Small delay so server socket is fully bound before we try spawning
    await _aio.sleep(0.5)
    try:
        proc = _spawn_entity_corpus()
        if proc:
            atexit.register(proc.terminate)
            print(f"[hub] entity-corpus auto-started (pid {proc.pid})", file=sys.stderr)
        else:
            print("[hub] entity-corpus already running or not found — skipped", file=sys.stderr)
    except Exception as _e:
        print(f"[hub] entity-corpus auto-start error: {_e}", file=sys.stderr)


@app.on_event("startup")
async def _start_session_gc():
    """Background task: reap idle or dead sessions every 60 s."""
    async def _gc():
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for proj_id in list(_sessions.keys()):
                proc = _sessions.get(proj_id)
                # Remove dead processes
                if proc and not proc.isalive():
                    _kill_session(proj_id)
                    continue
                # Kill sessions idle longer than TTL
                idle_since = _session_idle_since.get(proj_id)
                if idle_since and (now - idle_since) > _SESSION_TTL:
                    _kill_session(proj_id)
    asyncio.create_task(_gc())


# v0.2.4: FIFO stream + background watcher removed (restored to v0.2.1 poll-based approach).
# M374/M378 added pipe-pane FIFO and 500ms background loop but caused false positives.
# Reverted: client-poll-triggered detection only, with _push_session_idle() dedup guard kept.


async def _stream_pane_fifo_DISABLED(session_name: str, proj_id: str):
    """M374 fast: stream pane output via pipe-pane → FIFO → asyncio reader.
    Detects spinner appear/disappear within <10ms and pushes SSE immediately."""
    import re as _re
    _FIFO_DIR.mkdir(parents=True, exist_ok=True)
    fifo_path = _FIFO_DIR / f"{session_name}.fifo"
    if not fifo_path.exists():
        try:
            os.mkfifo(str(fifo_path))
        except FileExistsError:
            pass
    # Attach pipe-pane to the tmux session
    subprocess.run(["tmux", "pipe-pane", "-t", session_name, f"cat >> {fifo_path}"],
                   capture_output=True)
    _spinner_last_seen: float = 0.0
    _was_busy = False
    _IDLE_THRESHOLD = 2.5  # M378: 2.5s threshold — 250ms caused false positives on Claude's inter-task pauses
    try:
        # Open FIFO for async reading — non-blocking via asyncio executor
        loop = asyncio.get_event_loop()
        fd = await loop.run_in_executor(None, lambda: os.open(str(fifo_path), os.O_RDONLY | os.O_NONBLOCK))
        buf = b""
        while True:
            now = asyncio.get_event_loop().time()
            try:
                chunk = await loop.run_in_executor(None, lambda: os.read(fd, 4096))
                if chunk:
                    buf += chunk
                    text = _re.sub(rb'\x1b\[[0-9;]*[mKHJA-Z]', b'', buf).decode('utf-8', errors='replace')
                    buf = b""
                    if b"\xe2\x80\xa6 (" in chunk or "… (" in text:  # spinner
                        _spinner_last_seen = now
                        if not _was_busy:
                            _was_busy = True
                            _exec_was_running[session_name] = True
                            _exec_idle_count[session_name] = 0
            except (BlockingIOError, OSError):
                pass
            # Check if spinner gone for threshold → idle
            if _was_busy and _spinner_last_seen and (now - _spinner_last_seen) > _IDLE_THRESHOLD:
                _was_busy = False
                if session_name in _exec_was_running:
                    _push_session_idle(session_name, proj_id)
                    _exec_was_running.pop(session_name, None)
            await asyncio.sleep(0.02)  # 20ms tick
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        try:
            subprocess.run(["tmux", "pipe-pane", "-t", session_name], capture_output=True)  # stop pipe
            fifo_path.unlink(missing_ok=True)
        except Exception:
            pass


# v0.2.4: _ensure_fifo_stream, _cancel_fifo_stream, _start_exec_state_watcher removed.
# Detection is now purely client-poll-triggered (v0.2.1 approach) with _push_session_idle() dedup.


@app.on_event("startup")
async def _start_queue_continuation_poller():
    """M456: hub-side queue continuation — periodically scans alive exec sessions and
    sends 'go' to verified-idle sessions when queued stones exist. Decoupled from stop-hook
    (which only fires on actual Claude Stop event). M442 removed go-injection from queue dispatch
    to avoid killing live sessions; this background task safely resumes idle sessions instead."""
    import re as _re
    POLL_INTERVAL = 30  # seconds — gentle enough to avoid spam, fast enough for UX
    IDLE_MIN_SECS = 10  # session must have been idle for this long (avoids racing turn-start)
    _last_go_sent: dict[str, float] = {}  # session → epoch of last 'go' to dedup
    GO_COOLDOWN = 60  # don't re-send 'go' within 60s of last one

    async def _poll_queue_continuation():
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                # List all alive exec tmux sessions
                result = subprocess.run(
                    ["tmux", "list-sessions", "-F", "#{session_name}"],
                    capture_output=True, text=True, timeout=3
                )
                exec_sessions = []
                for line in result.stdout.splitlines():
                    sn = line.strip()
                    for prefix in ("claude-exec-", "openrouter-exec-", "codex-exec-"):
                        if sn.startswith(prefix):
                            proj_id = sn[len(prefix):]
                            exec_sessions.append((sn, proj_id))
                            break

                now = time.time()
                for sname, proj_id in exec_sessions:
                    # Skip if we just sent 'go' recently
                    if now - _last_go_sent.get(sname, 0) < GO_COOLDOWN:
                        continue

                    # M536: primary idle check — .exec-idle file written by stop-hook when truly idle
                    file_idle = _exec_idle_file(proj_id).exists()

                    # Secondary: pane viewport check (guards against stale file)
                    pane_out = subprocess.run(
                        ["tmux", "capture-pane", "-p", "-t", sname],
                        capture_output=True, text=True, timeout=2
                    ).stdout
                    clean = _re.sub(r'\x1b\[[0-9;]*[mKHJ]', '', pane_out)
                    busy = "… (" in clean or "esc to i" in clean
                    if busy:
                        # Session is actively running — clear stale idle file if present
                        _exec_idle_file(proj_id).unlink(missing_ok=True)
                        continue  # don't interrupt

                    idle_count = _exec_idle_count.get(sname, 0)
                    # Idle if: file-idle (stop-hook confirmed) OR legacy 2-cycle count
                    if not file_idle and idle_count < 2:
                        continue  # not confirmed idle yet

                    # Check: queued stones exist for this project (read from SQLite directly)
                    queued_count = 0
                    try:
                        _qdb = sqlite3.connect(str(_NS_EVENTS_DB))
                        try:
                            queued_count = _qdb.execute(
                                "SELECT COUNT(*) FROM milestones_store WHERE proj_id=? AND status='queued' AND COALESCE(held,0)=0",
                                (proj_id,)
                            ).fetchone()[0]
                        finally:
                            _qdb.close()
                    except Exception:
                        continue

                    if queued_count == 0:
                        continue  # nothing to do

                    # Send 'go' via tmux send-keys — safe because session is verified idle
                    subprocess.run(
                        ["tmux", "send-keys", "-t", sname, "go", "Enter"],
                        capture_output=True, timeout=2
                    )
                    _last_go_sent[sname] = now
            except Exception:
                pass  # poller must never crash

    asyncio.create_task(_poll_queue_continuation())


def _encode_cwd_for_claude(cwd: str) -> str:
    """Replicate Claude Code's transcript-dir encoding: every non-alphanumeric
    character is replaced with `-`. Source: research/20260513-tmux-claude-session-resume-design.md FACT-3.
    """
    import re
    return re.sub(r"[^a-zA-Z0-9]", "-", cwd)


def _get_resume_args(proj_id: str, proj_dir: str, explicit_session_id: str = None, agent: str = None) -> list:
    """Return Claude resume flags for tmux/PTY spawn continuity.

    Spec: docs/research/20260513-tmux-claude-session-resume-design.md MVF #2.
    Supports three continuity modes (from north-star.md frontmatter):
      - "isolated": per-model session history (default, backward-compat)
      - "portable": shared session across models (cross-model continuity)
      - "fresh": start a brand-new conversation (no resume flags)

    M[new]: If explicit_session_id is provided, use it directly (UI explicit selection).
    agent: override the project's default agent (from UI agent selector).
    """
    pdir = PROJECTS_DIR / proj_id
    if agent not in _ALLOWED_AGENTS:
        agent = _get_project_agent_value(proj_id)
    encoded = _encode_cwd_for_claude(str(proj_dir))
    transcripts_dir = Path.home() / ".claude" / "projects" / encoded

    def _try_id(sid: str) -> list:
        if not sid:
            return []
        if agent == "codex":
            if sid == "fresh":
                return []
            if sid in {"last", "_last"}:
                return ["resume", "--last"]
            return ["resume", sid]
        t = transcripts_dir / f"{sid}.jsonl"
        try:
            if t.exists() and t.stat().st_size > 0:
                return ["--resume", sid]
        except Exception:
            pass
        return []

    # M[new]: UI explicit selection takes highest priority
    if explicit_session_id:
        if explicit_session_id == "fresh":
            return []
        if agent == "codex" and explicit_session_id in {"last", "_last"}:
            return ["resume", "--last"]
        args = _try_id(explicit_session_id)
        if args:
            return args

    # Check continuity mode from frontmatter
    continuity_mode = "isolated"  # default
    try:
        md = pdir / "north-star.md"
        if md.exists():
            ns = _parse_md_frontmatter(md)
            continuity_mode = ns.get("continuity_mode", "isolated")
    except Exception:
        pass

    # fresh mode: always start a brand-new conversation
    if continuity_mode == "fresh":
        return []

    if agent == "codex":
        # Codex has CLI-native resume support, but not Claude's transcript-dir
        # layout. Use the most recent Codex session unless the user asked fresh.
        if explicit_session_id:
            return ["resume", explicit_session_id]
        return ["resume", "--last"]

    hist_file = pdir / ".session-history.json"

    # portable mode: use shared _current session across all models
    if continuity_mode == "portable":
        try:
            if hist_file.exists():
                hist = json.loads(hist_file.read_text())
                sid = (hist.get("_current") or "").strip()
                args = _try_id(sid)
                if args:
                    return args
        except Exception:
            pass

    # isolated mode (default): per-model lookup
    try:
        cur_model = ""
        try:
            cur_model = _get_project_model_value(proj_id)
        except NameError:
            pass
        if hist_file.exists():
            hist = json.loads(hist_file.read_text())
            sid = (hist.get(cur_model or "_default") or "").strip()
            args = _try_id(sid)
            if args:
                return args
    except Exception:
        pass

    # Legacy single-file fallback
    last_id_file = pdir / ".last-session-id"
    if last_id_file.exists():
        try:
            sid = last_id_file.read_text().strip()
            args = _try_id(sid)
            if args:
                return args
            try: last_id_file.unlink()
            except Exception: pass
        except Exception:
            pass

    return ["--continue"]


_ALLOWED_MODELS = {
    # Claude CLI accepts aliases and full IDs. Restrict to the ones we want users
    # to pick from in the UI; "" / unset → CLI default model.
    "haiku", "sonnet", "opus",
    "claude-haiku-4-5",
    "claude-sonnet-4-6",
    "claude-opus-4-7",
    "claude-opus-4-8",
    # OSK/LiteLLM proxy on 127.0.0.1:4100 exposes OpenAI as the Anthropic API
    # (config: ~/.osk-litellm.yaml). Selecting this routes the spawned Claude
    # session to GPT — env splice happens in _get_project_spawn_env.
    "gpt-5.4-2026-03-05",
    # Claude exact version IDs
    "claude-haiku-4-5-20251001",
    "claude-sonnet-4-5-20250929",
    # OpenRouter models via LiteLLM proxy (or- aliases map to proxy model_name)
    "or-gemini-flash",
    "or-gemini3-flash",
    "or-deepseek-v4-flash",
    "or-kimi-k2",
    "or-hy3-preview",
    "or-owl-alpha",
    "or-grok-3",
    "or-grok-3-mini",
    "or-nemotron",
    "or-nemotron-nano",
    # Codex agent models (준비중)
    "codex-haiku",
    "codex-sonnet",
    # DSK — Darwin-28B bridge on localhost:8860
    "darwin-28b-coder",
}

_OSK_MODELS = {"gpt-5.4-2026-03-05"}
_OPENROUTER_MODELS = {
    "or-gemini-flash",
    "or-gemini3-flash",
    "or-deepseek-v4-flash",
    "or-kimi-k2",
    "or-hy3-preview",
    "or-owl-alpha",
    "or-grok-3",
    "or-grok-3-mini",
    "or-nemotron",
    "or-nemotron-nano",
}
_DSK_MODELS = {"darwin-28b-coder"}
_ALLOWED_AGENTS = {"claude", "codex", "openrouter", "dsk"}
_ALLOWED_PTY_AGENTS = {"claude", "codex", "openrouter"}
_OSK_PROXY_URL = "http://127.0.0.1:4100"
_OSK_PROXY_KEY = "sk-osk-local"
_OPENROUTER_PROXY_URL = "http://127.0.0.1:4100"  # LiteLLM proxy handles openrouter/ prefix
_DSK_PROXY_URL = "http://127.0.0.1:8860"  # Darwin-28B bridge (SSH tunnel → NIPA:8850)
_DSK_PROXY_KEY = "sk-ant-api03-dummy-dsk-darwin28b-coder-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


# ── M705: Per-user config helpers ─────────────────────────────────────────────
def _read_hub_config() -> dict:
    """Load ~/.hub/config.yaml; returns {} if missing or malformed."""
    try:
        if _HUB_CONFIG_FILE.exists():
            import yaml as _yaml
            return _yaml.safe_load(_HUB_CONFIG_FILE.read_text()) or {}
    except Exception:
        pass
    return {}


def _write_hub_config(cfg: dict) -> None:
    _HUB_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    import yaml as _yaml
    _HUB_CONFIG_FILE.write_text(_yaml.dump(cfg, default_flow_style=False, allow_unicode=True))


def _hub_config_get(proj_id: str | None, key: str) -> str:
    """Return config value: project-level override wins over global default."""
    cfg = _read_hub_config()
    if proj_id:
        v = cfg.get("projects", {}).get(proj_id, {}).get(key)
        if v:
            return str(v).strip()
    return str(cfg.get("defaults", {}).get(key, "")).strip()
# ── end M705 ───────────────────────────────────────────────────────────────────


def _get_project_model_value(proj_id: str) -> str:
    """Read the (validated) model field from project frontmatter. Falls back to config.yaml."""
    try:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        if md.exists():
            proj = _parse_md_frontmatter(md)
            model = (proj.get("model") or "").strip()
            if model in _ALLOWED_MODELS:
                return model
    except Exception:
        pass
    # M705: fall back to per-user config
    cfg_model = _hub_config_get(proj_id, "model")
    return cfg_model if cfg_model in _ALLOWED_MODELS else ""


def _get_project_model(proj_id: str) -> list:
    """Return ['--model', value] if frontmatter has a valid model, else [].
    Spliced into PTY + tmux spawn argv.
    Rewrites or-* aliases to openrouter-* to match the LiteLLM proxy's
    exposed model IDs (seen via GET /v1/models)."""
    model = _get_project_model_value(proj_id)
    if model.startswith("or-"):
        model = "openrouter-" + model[3:]
    return ["--model", model] if model else []


def _get_project_agent_value(proj_id: str) -> str:
    """Read the agent field from project frontmatter. Falls back to config.yaml, then claude."""
    try:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        if md.exists():
            proj = _parse_md_frontmatter(md)
            agent = (proj.get("agent") or "").strip().lower()
            if agent in _ALLOWED_AGENTS:
                return agent
    except Exception:
        pass
    # M705: fall back to per-user config
    cfg_agent = _hub_config_get(proj_id, "agent").lower()
    return cfg_agent if cfg_agent in _ALLOWED_AGENTS else "claude"


def _get_project_pty_agent_value(proj_id: str) -> str:
    """Read the PTY agent field from project frontmatter. Defaults to Claude."""
    try:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        if not md.exists():
            return "claude"
        proj = _parse_md_frontmatter(md)
        agent = (proj.get("pty_agent") or "claude").strip().lower()
        return agent if agent in _ALLOWED_PTY_AGENTS else "claude"
    except Exception:
        return "claude"


def _get_project_agent(proj_id: str) -> str:
    """Return the CLI agent binary selector for this project."""
    return _get_project_agent_value(proj_id)


def _get_agent_spawn_cmd(proj_id: str) -> list:
    """Return the agent binary + safety flags for a project spawn.

    M779: honour hub config codex_path / claude_code_path so nvm-installed
    binaries get launched without depending on systemd PATH.
    """
    import shutil as _shutil_spawn
    agent = _get_project_agent_value(proj_id)
    if agent == "codex":
        cfg_path = _hub_config_get(proj_id, "codex_path")
        if not cfg_path:
            cfg_path = _shutil_spawn.which("codex") or ""
            if not cfg_path:
                for _nvm_base in [Path.home() / ".nvm" / "versions", Path("/usr/local/lib")]:
                    for _bin in _nvm_base.rglob("bin/codex"):
                        cfg_path = str(_bin); break
                    if cfg_path: break
        codex_bin = cfg_path or "codex"
        return [codex_bin, "--dangerously-bypass-approvals-and-sandbox", *_get_project_model(proj_id)]
    claude_bin = _hub_config_get(proj_id, "claude_code_path") or "claude"
    return [claude_bin, "--dangerously-skip-permissions", *_get_project_model(proj_id)]


def _get_pty_spawn_cmd(proj_id: str, agent: str | None = None) -> list:
    """Return the PTY spawn command for a project."""
    import shutil as _shutil
    agent = (agent or _get_project_pty_agent_value(proj_id)).strip().lower()
    if agent == "codex":
        node = _shutil.which("node") or "node"
        codex_js = _shutil.which("codex")
        if not codex_js:
            for _nvm_base in [Path.home() / ".nvm" / "versions", Path("/usr/local/lib")]:
                for _js in _nvm_base.rglob("@openai/codex/bin/codex.js"):
                    codex_js = str(_js); break
        if codex_js and not codex_js.endswith(".js"):
            return [codex_js, "--dangerously-bypass-approvals-and-sandbox"]
        if codex_js:
            return [node, codex_js, "--dangerously-bypass-approvals-and-sandbox"]
        return ["codex", "--dangerously-bypass-approvals-and-sandbox"]
    # openrouter = Claude Code CLI + LiteLLM/OpenRouter env (handled by _get_project_spawn_env)
    claude_bin = _shutil.which("claude") or str(Path.home() / ".local" / "bin" / "claude")
    return [claude_bin, "--dangerously-skip-permissions", *_get_project_model(proj_id)]


def _get_project_spawn_env(proj_id: str) -> dict:
    """Return extra env vars to splice into the Claude spawn for this project.
    For OSK/GPT: routes to LiteLLM proxy. For OpenRouter: routes via LiteLLM proxy.
    Otherwise {}."""
    model = _get_project_model_value(proj_id)
    if model in _OSK_MODELS:
        # Route to LiteLLM proxy on 127.0.0.1:4100 (GPT)
        return {
            "CLAUDE_CODE_OAUTH_TOKEN": "",
            "CLAUDE_CODE_OAUTH_REFRESH_TOKEN": "",
            "ANTHROPIC_API_KEY": _OSK_PROXY_KEY,
            "ANTHROPIC_BASE_URL": _OSK_PROXY_URL,
            "ANTHROPIC_MODEL": model,
            "ANTHROPIC_SMALL_FAST_MODEL": model,
        }
    if model in _OPENROUTER_MODELS:
        # Route to LiteLLM proxy — rewrite or-* to openrouter-* to match proxy's exposed IDs
        or_key = os.environ.get("OPENROUTER_API_KEY", _OSK_PROXY_KEY)
        or_model = ("openrouter-" + model[3:]) if model.startswith("or-") else model
        return {
            "CLAUDE_CODE_OAUTH_TOKEN": "",
            "CLAUDE_CODE_OAUTH_REFRESH_TOKEN": "",
            "ANTHROPIC_API_KEY": or_key,
            "ANTHROPIC_BASE_URL": _OPENROUTER_PROXY_URL,
            "ANTHROPIC_MODEL": or_model,
            "ANTHROPIC_SMALL_FAST_MODEL": or_model,
        }
    if model in _DSK_MODELS:
        # Route to Darwin-28B bridge on localhost:8860 (SSH tunnel → NIPA:8850 → lmdeploy:8790)
        return {
            "CLAUDE_CODE_OAUTH_TOKEN": "",
            "CLAUDE_CODE_OAUTH_REFRESH_TOKEN": "",
            "CLAUDE_CODE_DISABLE_NONAPI_AUTH": "1",
            "ANTHROPIC_API_KEY": _DSK_PROXY_KEY,
            "ANTHROPIC_BASE_URL": _DSK_PROXY_URL,
            "ANTHROPIC_MODEL": model,
            "ANTHROPIC_SMALL_FAST_MODEL": model,
        }
    return {}


def _record_spawn_info(proj_id: str, resume_args: list, agent: str = "claude") -> None:
    """Snapshot what resume flag we used when spawning Claude for this project.

    Surfaces to the UI via /api/exec-sessions so users can see whether the
    currently running tmux session is actually continuing prior stone work
    (--resume <id>) or starting fresh (--continue / nothing).
    """
    from datetime import datetime as _dt
    if agent == "codex":
        if "resume" in resume_args:
            try:
                idx = resume_args.index("resume")
                if idx + 1 < len(resume_args) and resume_args[idx + 1] == "--last":
                    from_id = "last"
                else:
                    from_id = resume_args[idx + 1] if idx + 1 < len(resume_args) else ""
            except Exception:
                from_id = ""
            if "--last" in resume_args:
                info = {"agent": agent, "mode": "resume-last", "from_id": from_id, "at": _dt.now().isoformat(timespec="seconds")}
            else:
                info = {"agent": agent, "mode": "resume", "from_id": from_id, "at": _dt.now().isoformat(timespec="seconds")}
        else:
            info = {"agent": agent, "mode": "fresh", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    elif "--resume" in resume_args:
        try:
            idx = resume_args.index("--resume")
            from_id = resume_args[idx + 1] if idx + 1 < len(resume_args) else ""
        except Exception:
            from_id = ""
        info = {"agent": agent, "mode": "resume", "from_id": from_id, "at": _dt.now().isoformat(timespec="seconds")}
    elif "--continue" in resume_args:
        info = {"agent": agent, "mode": "continue", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    else:
        info = {"agent": agent, "mode": "fresh", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    # M189: add model to spawn info so UI can display it in the session pane
    info["model"] = _get_project_model_value(proj_id) or ""
    try:
        pdir = PROJECTS_DIR / proj_id
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / ".last-spawn-info.json").write_text(json.dumps(info))
    except Exception:
        pass


def _update_session_history_from_transcript(proj_id: str, proj_dir: str, model_key: str = "") -> str | None:
    """Scan the transcript directory for the newest .jsonl file and record it in session-history.
    Returns the session ID if found, else None. Called after a fresh tmux spawn so the new
    session appears in the resume list immediately (before Stop hook fires)."""
    try:
        encoded = _encode_cwd_for_claude(str(proj_dir))
        transcripts_dir = Path.home() / ".claude" / "projects" / encoded
        if not transcripts_dir.exists():
            return None
        jsonls = sorted(transcripts_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not jsonls or jsonls[0].stat().st_size == 0:
            return None
        new_sid = jsonls[0].stem
        hist_file = PROJECTS_DIR / proj_id / ".session-history.json"
        hist = {}
        if hist_file.exists():
            try:
                hist = json.loads(hist_file.read_text())
            except Exception:
                pass
        _is_agent_specific = bool(model_key) and (
            model_key.startswith("or-") or model_key.startswith("codex-")
        )
        if not _is_agent_specific:
            hist["_current"] = new_sid
        if model_key:
            # M316: preserve previous session — save old ID as {model_key}_prev before overwriting
            old_sid = hist.get(model_key)
            if old_sid and old_sid != new_sid:
                hist[f"{model_key}_prev"] = old_sid
            hist[model_key] = new_sid
        if not _is_agent_specific and not hist.get("_default"):
            hist["_default"] = new_sid
        hist_file.write_text(json.dumps(hist))
        return new_sid
    except Exception:
        return None


def _load_branch_sessions(proj_id: str) -> dict:
    """M833: Load persisted branch session IDs from .branch-sessions.json.
    Returns dict keyed by tmux session name → {last_session_id, updated_at}."""
    path = PROJECTS_DIR / proj_id / ".branch-sessions.json"
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return {}


def _save_branch_session(proj_id: str, session_name: str, session_id: str) -> None:
    """M833: Persist a captured branch session_id for future --resume continuity."""
    import datetime
    path = PROJECTS_DIR / proj_id / ".branch-sessions.json"
    data = _load_branch_sessions(proj_id)
    data[session_name] = {
        "last_session_id": session_id,
        "updated_at": datetime.datetime.now().astimezone().isoformat(),
    }
    try:
        path.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def _spawn_claude(proj_id: str, agent: str | None = None, session_id: str | None = None):  # -> ptyprocess.PtyProcess
    # M524.3: graceful error when PTY is unavailable (Windows native without WSL2)
    if not _HAS_PTY:
        raise RuntimeError(
            "PTY sessions are not available on this platform (Windows native). "
            "Run hub inside WSL2 or Docker to enable the terminal. "
            "Alternatively, use the Execute button which dispatches via Claude Code CLI."
        )
    # M183: if session_id provided, spawn with --resume <id> so PTY continues a prior conversation.
    proj_dir = _get_project_dir(proj_id) or str(Path.home())
    runtime = (agent or _get_project_pty_agent_value(proj_id)).strip().lower()
    cmd = _get_pty_spawn_cmd(proj_id, runtime)
    if session_id and session_id != "fresh" and runtime == "claude":
        cmd = [*cmd, "--resume", session_id]
    return ptyprocess.PtyProcessUnicode.spawn(
        cmd,
        cwd=proj_dir,
        dimensions=(30, 120),
        env={
            **os.environ,
            "TERM": "xterm-256color",
            "COLUMNS": "120",
            "LINES": "30",
            "CLAUDE_CODE_TASK_LIST_ID": f"hub-exec-{proj_id}",
            "NS_HUB_URL": f"http://{_tailscale_interface_ip()}:{PORT}",
            **_get_project_spawn_env(proj_id),
        },
    )


def _exec_session_names(proj_id: str) -> list:
    """Return tmux exec session names to probe for a project (all known agents + branched sessions).
    M858: branched sessions (claude-exec-{proj_id}-{suffix}) are also included so they are
    killed when a new agent/session dispatch happens."""
    agent = _get_project_agent_value(proj_id)
    names = [f"{agent}-exec-{proj_id}"]
    for fallback in ("claude", "codex", "openrouter"):
        n = f"{fallback}-exec-{proj_id}"
        if n not in names:
            names.append(n)
    # M858: also enumerate live tmux sessions matching *-exec-{proj_id}-* (branched sessions)
    try:
        _tmux_ls = subprocess.run(
            ["tmux", "list-sessions", "-F", "#{session_name}"],
            capture_output=True, text=True, timeout=2,
        ).stdout.splitlines()
        for sname in _tmux_ls:
            # Match branched pattern: <agent>-exec-<proj_id>-<suffix>
            if f"-exec-{proj_id}-" in sname and sname not in names:
                names.append(sname)
    except Exception:
        pass
    return names


def _live_exec_session_name(proj_id: str) -> str:
    """Return the first running exec session name for a project, if any."""
    for candidate in _exec_session_names(proj_id):
        check = subprocess.run(["tmux", "has-session", "-t", candidate], capture_output=True)
        if check.returncode == 0:
            return candidate
    return f"{_get_project_agent_value(proj_id)}-exec-{proj_id}"


def _kill_all_exec_sessions(proj_id: str) -> None:
    """M206: kill ALL agent-prefixed exec sessions for a project.
    Prevents duplicate panes when agent changes (e.g. claude-exec + openrouter-exec both alive)."""
    killed = []
    for candidate in _exec_session_names(proj_id):
        r = subprocess.run(["tmux", "kill-session", "-t", candidate], capture_output=True)
        if r.returncode == 0:
            killed.append(candidate)
        # M355: clear idle tracking for killed sessions — prevents stale count from
        # triggering spurious "idle" notifications right after respawn during startup
        _exec_idle_count.pop(candidate, None)
        _exec_was_running.pop(candidate, None)
    if killed:
        _server_log_action(proj_id, "", "exec:kill", f"sessions:{','.join(killed)}")
    # M985: also clear assigned_session on all substars when all sessions killed
    try:
        p = _db_load_project(proj_id)
        if p:
            north_stars = p.get("north_stars") or []
            changed = False
            for ns in north_stars:
                if isinstance(ns, dict) and ns.get("assigned_session"):
                    ns["assigned_session"] = None
                    changed = True
            if changed:
                _db_save_project(proj_id, p)
    except Exception:
        pass


def _substar_session_name(proj_id: str, substar_id: str) -> str:
    """M747/M859: per-substar session name — {agent}-exec-{proj_id}-{last 8 chars of substar_id}.
    Uses the project's current agent so branched sessions always share the mother session's prefix."""
    agent = _get_project_agent_value(proj_id)
    return f"{agent}-exec-{proj_id}-{substar_id[-8:]}"


def _get_active_substar_sessions(proj_id: str) -> dict:
    """M747: return {substar_id_short: session_name} for running substar sessions."""
    result = {}
    try:
        _r = subprocess.run(["tmux", "list-sessions", "-F", "#{session_name}"],
                            capture_output=True, text=True)
        prefix = f"claude-exec-{proj_id}-"
        for line in _r.stdout.splitlines():
            sname = line.strip()
            if sname.startswith(prefix):
                short = sname[len(prefix):]
                result[short] = sname
    except Exception:
        pass
    return result


async def _pty_drain(proj_id: str, proc):
    """M182: Single background reader for a PTY — runs as long as the PTY is in
    _sessions. Owns the PTY fd; WS handlers subscribe via queues to get a copy
    of each chunk. Keeps _buffers + _pty_last_busy_ts current even when no WS
    is attached, so the PTY busy/idle badge reflects real Claude state."""
    loop = asyncio.get_event_loop()
    try:
        while proj_id in _sessions and proc.isalive():
            try:
                data = await loop.run_in_executor(None, lambda: proc.read(4096))
            except (EOFError, asyncio.CancelledError):
                break
            except Exception:
                # Transient read error — back off briefly and retry
                await asyncio.sleep(0.1)
                continue
            if not data:
                continue
            # Append to scrollback buffer (cap 64 KB)
            buf = _buffers.setdefault(proj_id, [])
            buf.append(data)
            total = sum(len(c) for c in buf)
            while total > _BUFFER_MAX and buf:
                total -= len(buf.pop(0))
            # M181/M182: busy timestamp. Claude PTY uses spinner glyphs during work
            # (✢ ✶ ✻ ✽ from the "thinking" animation set) — NOT the "esc to interrupt"
            # string used in tmux exec status bar. Detect either form.
            if ("esc to i" in data) or ("… (" in data) or \
               ("✻" in data) or ("✶" in data) or ("✽" in data) or ("✢" in data):
                _pty_last_busy_ts[proj_id] = time.time()
            # Fan-out to subscribers (WS clients)
            for q in list(_pty_subscribers.get(proj_id, set())):
                try:
                    q.put_nowait(data)
                except Exception:
                    pass
    finally:
        # PTY died — clean up. Don't pop from _sessions here; _kill_session does.
        _pty_drain_tasks.pop(proj_id, None)


def _ensure_pty_drain(proj_id: str, proc) -> None:
    """Start the background drain task if not already running for this PTY."""
    existing = _pty_drain_tasks.get(proj_id)
    if existing and not existing.done():
        return
    _pty_drain_tasks[proj_id] = asyncio.create_task(_pty_drain(proj_id, proc))


@app.websocket("/ws/session/{proj_id}")
async def terminal_session(websocket: WebSocket, proj_id: str):
    """Bridge WebSocket ↔ persistent PTY. Reuses existing session on reconnect."""
    await websocket.accept()
    desired_agent = (websocket.query_params.get("agent") or _get_project_pty_agent_value(proj_id) or "claude").strip().lower()
    if desired_agent not in _ALLOWED_PTY_AGENTS:
        desired_agent = "claude"
    # M283: tmux_session → attach to existing exec tmux session instead of spawning new PTY
    tmux_session_name = websocket.query_params.get("tmux_session", "").strip() or None
    if tmux_session_name and _HAS_PTY:
        import ptyprocess as _pty_mod
        try:
            # M804: replay last 2000 lines of tmux scrollback before attaching so mobile
            # users can scroll up to see previous output without needing copy mode.
            import subprocess as _sp
            _cap = _sp.run(
                ["tmux", "capture-pane", "-p", "-J", "-S", "-2000", "-t", tmux_session_name],
                capture_output=True, text=True, timeout=3
            )
            if _cap.returncode == 0 and _cap.stdout.strip():
                await websocket.send_text(_cap.stdout)
                await websocket.send_text("\r\n\x1b[2m[─── scrollback above ───]\x1b[0m\r\n")
        except Exception:
            pass
        try:
            # M283: attach-session with TERM=xterm-256color works correctly —
            # input is forwarded to the exec pane. new-session -t does NOT forward input.
            proc = _pty_mod.PtyProcess.spawn(
                ["tmux", "attach-session", "-t", tmux_session_name],
                env={**os.environ, "TERM": "xterm-256color"},
                dimensions=(40, 120),
            )
        except Exception as e:
            await websocket.send_text(f"\r\n[Failed to attach to tmux session {tmux_session_name}: {e}]\r\n")
            await websocket.close()
            return
        queue: asyncio.Queue = asyncio.Queue(maxsize=512)
        async def _tmux_to_ws():
            loop = asyncio.get_event_loop()
            try:
                while True:
                    if not proc.isalive(): break
                    try:
                        data = await loop.run_in_executor(None, lambda: proc.read(4096))
                        if data:
                            await websocket.send_text(data if isinstance(data, str) else data.decode("utf-8", errors="replace"))
                    except Exception: break
            finally:
                try: await websocket.send_text("\r\n\x1b[33m[Detached from tmux session]\x1b[0m\r\n")
                except Exception: pass
        async def _ws_to_tmux():
            while True:
                try:
                    msg = await websocket.receive_text()
                    if msg.startswith('\x00resize:'):
                        parts = msg[8:].split(',')
                        if len(parts) == 2:
                            try: proc.setwinsize(int(parts[1]), int(parts[0]))
                            except Exception: pass
                    else:
                        proc.write(msg.encode("utf-8") if isinstance(msg, str) else msg)
                except WebSocketDisconnect: break
                except Exception: break
        await asyncio.gather(_tmux_to_ws(), _ws_to_tmux())
        return
    # M183: optional session_id → PTY spawns with --resume <id>; force respawn if session differs
    desired_session_id = websocket.query_params.get("session_id", "").strip() or None

    # Reuse existing session if alive, otherwise spawn new
    proc = _sessions.get(proj_id)
    current_agent = _pty_agents.get(proj_id)
    current_session_id = _pty_session_ids.get(proj_id)
    # Kill and respawn when: dead, agent changed, or a different resume session requested
    _session_changed = (
        desired_session_id is not None
        and desired_session_id != current_session_id
    )
    if not _HAS_PTY:
        await websocket.send_text("\r\n[PTY not available on this platform (Windows native). Use WSL2 or Docker.]\r\n")
        await websocket.close()
        return
    if proc is None or not proc.isalive() or (current_agent and current_agent != desired_agent) or _session_changed:
        _buffers.pop(proj_id, None)  # clear stale buffer
        try:
            if proc is not None and proc.isalive():
                _kill_session(proj_id)
            # M183: send visible indicator before spawning so user knows resume is in progress
            if desired_session_id and desired_session_id != "fresh":
                await websocket.send_text(f"\r\n\x1b[2m[— resuming session {desired_session_id[:8]}… —]\x1b[0m\r\n")
            proc = _spawn_claude(proj_id, desired_agent, session_id=desired_session_id)
            _sessions[proj_id] = proc
            _pty_agents[proj_id] = desired_agent
            _pty_session_ids[proj_id] = desired_session_id
        except Exception as e:
            await websocket.send_text(f"\r\n[Failed to start {desired_agent}: {e}]\r\n")
            await websocket.close()
            return
        # M182: start background drain so PTY busy/idle is tracked continuously
        _ensure_pty_drain(proj_id, proc)
    else:
        _session_idle_since.pop(proj_id, None)  # no longer idle
        # Replay scrollback buffer so user sees previous output
        if proj_id in _buffers and _buffers[proj_id]:
            replay = "".join(_buffers[proj_id])
            await websocket.send_text(replay)
        await websocket.send_text("\r\n\x1b[2m[— reconnected —]\x1b[0m\r\n")
        # SIGWINCH → forces claude to redraw its UI
        try:
            proc.setwinsize(proc.getwinsize()[0], proc.getwinsize()[1])
        except Exception:
            pass
        # M182: drain may have died if previous session ended cleanly — ensure it's running
        _ensure_pty_drain(proj_id, proc)

    # M182: subscribe to the broadcaster — drain task fan-outs each chunk to our queue
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    _pty_subscribers.setdefault(proj_id, set()).add(queue)

    async def pty_to_ws():
        try:
            while True:
                if not proc.isalive():
                    try:
                        await websocket.send_text("\r\n\x1b[33m[Session ended]\x1b[0m\r\n")
                    except Exception:
                        pass
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue  # loop back to check proc.isalive
                try:
                    await websocket.send_text(data)
                except Exception:
                    break  # WS send failed — drain continues without us
        finally:
            _pty_subscribers.get(proj_id, set()).discard(queue)

    async def ws_to_pty():
        while True:
            try:
                msg = await websocket.receive_text()
                if msg.startswith('\x00resize:'):
                    parts = msg[8:].split(',')
                    if len(parts) == 2:
                        try:
                            cols, rows = int(parts[0]), int(parts[1])
                            proc.setwinsize(rows, cols)
                        except Exception:
                            pass
                elif msg == '\x00kill-session':
                    _kill_session(proj_id)
                    await websocket.send_text("\r\n\x1b[31m[Session killed]\x1b[0m\r\n")
                    break
                else:
                    proc.write(msg)
            except WebSocketDisconnect:
                break  # Detach only — do NOT terminate (session persists)
            except Exception:
                break
        # WS detached — record idle start time (GC will kill after SESSION_TTL)
        if proj_id in _sessions:
            _session_idle_since[proj_id] = time.time()
            # M201: push SSE event so iOS clients receive idle notification via SSE
            _ns_push("session_idle", proj_id=proj_id, kind="pty")
            # M389: send Telegram push notification on PTY idle (ntfy removed, Telegram is sole provider)
            _send_ntfy_notification(f"{proj_id} idle", "Hub session went idle", priority="default")

    await asyncio.gather(pty_to_ws(), ws_to_pty())


@app.post("/api/terminal/{proj_id}/inject")
async def terminal_inject(proj_id: str, request: Request):
    """Inject a prompt into the running Claude terminal session for a project."""
    data = await request.json()
    prompt = data.get("prompt", "").strip()
    if not prompt:
        return JSONResponse({"ok": False, "error": "prompt required"}, status_code=400)

    proc = _sessions.get(proj_id)
    if not proc or not proc.isalive():
        return JSONResponse({"ok": False, "error": "No active terminal session for this project",
                             "hint": "Open the terminal first by clicking ›_ claude on the card"}, status_code=404)

    # Send prompt to PTY (append newline to submit)
    try:
        proc.write(prompt + "\r")
        return JSONResponse({"ok": True, "message": f"Prompt injected into {proj_id} terminal"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/northstar/{proj_id}/north-stars")
async def get_north_stars(proj_id: str):
    """Return all North Stars for a project (multi-NS structure)."""
    # M300: single-project load instead of all-projects scan
    md = PROJECTS_DIR / proj_id / "north-star.md"
    p = _parse_md_frontmatter(md) if md.exists() else None
    if not p:
        return JSONResponse({"ok": False, "north_stars": []})
    ns_list = p.get("north_stars")
    # M628: use `is not None` so an explicit empty list [] (after deleting last sub-star)
    # is returned as-is, not treated as absent and re-wrapped in a "default" fallback.
    if ns_list is not None:
        # Normalize milestones inside each NS
        for ns in (ns_list or []):
            raw_ms = ns.get("milestones", [])
            norm = []
            for i, m in enumerate(raw_ms):
                if isinstance(m, dict):
                    norm.append({**m, "id": m.get("id", f"{ns['id']}_M{i+1}"), "ns_id": ns["id"]})
            ns["milestones"] = norm
        # If all NS entries have empty milestones but top-level milestones exist,
        # inject them into the first NS so the detail card can display them.
        if ns_list:  # guard against empty list + index error
            top_level_ms = p.get("milestones", [])
            if top_level_ms:
                all_empty = all(not ns.get("milestones") for ns in ns_list)
                if all_empty:
                    ns_list[0]["milestones"] = top_level_ms
        return JSONResponse({"ok": True, "north_stars": ns_list})
    # Fallback: wrap legacy milestones into a single NS (only when north_stars key is absent entirely)
    legacy = p.get("milestones", [])
    fallback_ns = [{
        "id": "default",
        "name": p.get("name", proj_id),
        "metric": p.get("metric", ""),
        "target": p.get("target", ""),
        "status": p.get("status", ""),
        "current": p.get("current", ""),
        "milestones": legacy,
    }]
    return JSONResponse({"ok": True, "north_stars": fallback_ns})


@app.post("/api/northstar/{proj_id}/north-stars")
async def add_north_star(proj_id: str, request: Request):
    """M204: Add a new sub-star (north star entry) to the project."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ns_list = proj.get("north_stars") or []
    new_id = data.get("id") or f"star_{int(time.time())}"
    new_ns = {
        "id": new_id,
        "name": data.get("name", "New Goal"),
        "metric": data.get("metric", ""),
        "current": data.get("current", ""),
        "target": data.get("target", ""),
        "status": data.get("status", "exploring"),
        "milestones": [],
    }
    ns_list.append(new_ns)
    proj["north_stars"] = ns_list
    _save_project(proj_id, proj)  # M289: was _write_md_frontmatter — bypassed SQLite project_meta
    _sync_substars_to_claude_md(proj_id, proj)  # M532: keep project CLAUDE.md current
    return JSONResponse({"ok": True, "north_star": new_ns})


@app.delete("/api/northstar/{proj_id}/north-stars/{ns_id}")
async def delete_north_star(proj_id: str, ns_id: str):
    """M204: Delete a sub-star (north star entry) from the project.
    M211: 'default' refers to the main project star — clears its metric/target/current fields."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    if ns_id == "default":
        # Clear the main star fields instead of removing from north_stars list
        for field in ("metric", "current", "target"):
            proj.pop(field, None)
        proj["metric"] = ""
    else:
        ns_list = proj.get("north_stars") or []
        ns_list = [ns for ns in ns_list if ns.get("id") != ns_id]
        proj["north_stars"] = ns_list
    _save_project(proj_id, proj)  # M289: was _write_md_frontmatter — bypassed SQLite project_meta
    _sync_substars_to_claude_md(proj_id, proj)  # M532: keep project CLAUDE.md current
    return JSONResponse({"ok": True})


@app.patch("/api/northstar/{proj_id}/north-stars/{ns_id}")
async def update_north_star(proj_id: str, ns_id: str, request: Request):
    """M249: Edit an existing sub-star entry (name, metric, target, current, status)."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ns_list = proj.get("north_stars") or []
    ns = next((x for x in ns_list if x.get("id") == ns_id), None)
    if not ns:
        return JSONResponse({"ok": False, "error": "north-star not found"}, status_code=404)
    for field in ("name", "metric", "target", "current", "status", "default_agent", "assigned_session", "branch_from_session_id"):
        if field in data:
            ns[field] = data[field] or None if field in ("default_agent", "assigned_session", "branch_from_session_id") else data[field]
    proj["north_stars"] = ns_list
    _save_project(proj_id, proj)  # M289: was _write_md_frontmatter — bypassed SQLite project_meta
    _sync_substars_to_claude_md(proj_id, proj)  # M532: keep project CLAUDE.md current
    return JSONResponse({"ok": True, "north_star": ns})


@app.post("/api/northstar/{proj_id}/north-stars/reorder")
async def reorder_north_stars(proj_id: str, request: Request):
    """M242: Reorder sub-stars via drag/drop — move dragged_id to position of target_id."""
    data = await request.json()
    dragged_id = data.get("dragged_id", "")
    target_id = data.get("target_id", "")
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ns_list = proj.get("north_stars") or []
    dragged = next((ns for ns in ns_list if ns.get("id") == dragged_id), None)
    if not dragged:
        return JSONResponse({"ok": False, "error": "dragged item not found"})
    ns_list = [ns for ns in ns_list if ns.get("id") != dragged_id]
    target_idx = next((i for i, ns in enumerate(ns_list) if ns.get("id") == target_id), len(ns_list))
    ns_list.insert(target_idx, dragged)
    proj["north_stars"] = ns_list
    _save_project(proj_id, proj)  # M289
    return JSONResponse({"ok": True})


@app.patch("/api/northstar/{proj_id}/north-stars/{ns_id}/milestones/{mid}")
async def update_ns_milestone(proj_id: str, ns_id: str, mid: str, request: Request):
    """Toggle a milestone inside a specific North Star."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ns_list = proj.get("north_stars", [])
    for ns in ns_list:
        if ns.get("id") == ns_id:
            for ms in ns.get("milestones", []):
                if isinstance(ms, dict) and (ms.get("id") == mid or ms.get("text") == mid):
                    if "done" in data:
                        ms["done"] = bool(data["done"])
                    break
            break
    proj["north_stars"] = ns_list
    _save_project(proj_id, proj)  # M289
    return JSONResponse({"ok": True})


@app.post("/api/northstar/{proj_id}/north-stars/{ns_id}/generate-agent")
async def generate_substar_agent(proj_id: str, ns_id: str, request: Request):
    """M675 v2: Generate a SOTA-level Claude Code skill/agent file using user-provided role description."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ns_list = proj.get("north_stars") or []
    ns = next((n for n in ns_list if n.get("id") == ns_id), None)
    if not ns:
        return JSONResponse({"ok": False, "error": "substar not found"}, status_code=404)

    # Parse user interview inputs
    try:
        body = await request.json()
    except Exception:
        body = {}
    user_role = (body.get("user_role") or "").strip()
    user_constraints = (body.get("user_constraints") or "").strip()

    substar_name = ns.get("name") or ns_id
    substar_metric = ns.get("metric") or ""
    substar_desc = ns.get("description") or ""

    # Collect related stone texts for context
    milestones = proj.get("milestones") or []
    related = [m for m in milestones if m.get("substar_id") == ns_id or m.get("star_relation") == ns_id]
    stone_texts = [m.get("text", "")[:120] for m in related[:8] if m.get("text")]

    import re as _re_local
    _ascii_slug = _re_local.sub(r"[^a-z0-9]+", "-", substar_name.lower()).strip("-")[:24]
    _ns_index = next((i for i, n in enumerate(ns_list) if n.get("id") == ns_id), 0)
    slug = _ascii_slug if _ascii_slug else f"ss-{proj_id[:8].lower()}-{_ns_index}"
    slug = slug[:32]

    # Build SOTA-level SKILL.md — richer when user_role is provided
    if user_role:
        # SOTA path: user described the agent — generate a deep, expert-grade skill
        agent_title = f"{substar_name} Specialist"
        description_line = user_role[:160] if len(user_role) <= 160 else user_role[:157] + "…"
        skill_lines = [
            f"---",
            f"name: {slug}",
            f"description: {description_line}",
            f"---",
            f"",
            f"# {agent_title}",
            f"",
            f"## Identity & Role",
            f"",
            user_role,
            f"",
        ]
        if substar_metric or substar_desc:
            skill_lines += [f"## Domain Context — {substar_name}", f""]
            if substar_metric:
                skill_lines += [f"- **Success metric**: {substar_metric}"]
            if substar_desc:
                skill_lines += [f"- **Background**: {substar_desc}"]
            skill_lines += [f""]
        if user_constraints:
            skill_lines += [
                f"## Behavioral Constraints",
                f"",
                user_constraints,
                f"",
            ]
        if stone_texts:
            skill_lines += [f"## Reference Tasks (from existing work items)"]
            skill_lines += [f"- {t.strip()}" for t in stone_texts[:8] if t.strip()]
            skill_lines += [f""]
        skill_lines += [
            f"## Operating Principles",
            f"",
            f"1. **Deep expertise first** — bring domain-specific knowledge and best practices before acting.",
            f"2. **Clarity over cleverness** — prefer explicit, well-reasoned outputs over clever shortcuts.",
            f"3. **Evidence-anchored** — back recommendations with data, examples, or established frameworks.",
            f"4. **Iterative refinement** — surface ambiguities early; confirm scope before full execution.",
            f"5. **Concise reporting** — completion messages are 1 line, past tense, no preamble.",
            f"",
            f"## NS Hub Completion Protocol",
            f"",
            f"When a task (stone) is complete:",
            f"```",
            f"PATCH /api/northstar/{{proj_id}}/milestones/{{mid}}",
            f"{{",
            f'  "status": "pending_confirmation",',
            f'  "star_relation": "<one-line summary of what was done>",',
            f'  "model_used": "<model-id>",',
            f'  "exec_start": "<ISO timestamp>",',
            f'  "exec_end": "<ISO timestamp>",',
            f'  "append_message": {{"role": "claude", "text": "<1-line past-tense result>"}}',
            f"}}",
            f"```",
        ]
    else:
        # Fallback path: no user description — use substar context only
        skill_lines = [
            f"---",
            f"name: {slug}",
            f"description: Specialist agent for {substar_name} — auto-generated from North Star substar. Handles tasks in this domain end-to-end.",
            f"---",
            f"",
            f"# {substar_name} Agent",
            f"",
            f"You are a specialist agent for the **{substar_name}** domain in the NS Hub system.",
            f"",
        ]
        if substar_metric:
            skill_lines += [f"## Success Metric", f"{substar_metric}", f""]
        if substar_desc:
            skill_lines += [f"## Domain Context", f"{substar_desc}", f""]
        if stone_texts:
            skill_lines += [f"## Typical Tasks (from existing stones)"]
            skill_lines += [f"- {t.strip()}" for t in stone_texts[:8] if t.strip()]
            skill_lines += [""]
        skill_lines += [
            f"## Execution Protocol",
            f"1. Read the full stone text before acting.",
            f"2. Implement the task completely.",
            f"3. PATCH `status=pending_confirmation` with `append_message` (<=1 line, past tense).",
            f"4. Include `star_relation`, `model_used`, `exec_start`, `exec_end` in the same PATCH.",
        ]

    skills_dir = Path.home() / ".claude" / "skills" / slug
    skills_dir.mkdir(parents=True, exist_ok=True)
    skill_file = skills_dir / "SKILL.md"
    skill_file.write_text("\n".join(skill_lines))

    return JSONResponse({"ok": True, "agent_name": slug, "path": str(skill_file)})


@app.get("/api/northstar/{proj_id}/milestones")
async def get_milestones(proj_id: str, request: Request):
    """M215 next-step: ?status= param triggers fast SQLite indexed query instead of full project load.
    M515: GZipMiddleware compresses the response ~10x (764KB→~80KB) for large projects."""
    status_filter = request.query_params.get("status", "").strip().lower() or None
    if status_filter:
        # Fast path: direct SQLite query — no full project parse
        try:
            conn = sqlite3.connect(str(_NS_EVENTS_DB))
            rows = conn.execute(
                "SELECT data_json FROM milestones_store WHERE proj_id=? AND status=? ORDER BY rowid DESC",
                (proj_id, status_filter)
            ).fetchall()
            conn.close()
            return JSONResponse({"ok": True, "milestones": [json.loads(r[0]) for r in rows],
                                 "source": "sqlite_indexed", "filter": status_filter},
                                headers={"Cache-Control": "no-store"})
        except Exception:
            pass  # fallback to full load below
    # M981: load milestones from SQLite (north-star.md removed)
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? ORDER BY rowid",
            (proj_id,)
        ).fetchall()
        conn.close()
        milestones = [json.loads(r[0]) for r in rows]
        # M527: strip conversation from done stones
        for m in milestones:
            if isinstance(m, dict) and (m.get("done") or m.get("status") == "done"):
                m.pop("conversation", None)
        if milestones:
            return JSONResponse({"ok": True, "milestones": milestones},
                                headers={"Cache-Control": "no-store"})
    except Exception:
        pass
    # Legacy YAML fallback (only if SQLite has no data)
    md = PROJECTS_DIR / proj_id / "north-star.md"
    p = _parse_md_frontmatter(md) if md.exists() else None
    if not p:
        return JSONResponse({"ok": False, "milestones": []})
    milestones = p.get("milestones", [])
    # M527: strip conversation from done stones — reduces payload from ~800KB to ~200KB
    for m in milestones:
        if isinstance(m, dict) and (m.get("done") or m.get("status") == "done"):
            m.pop("conversation", None)
    return JSONResponse({"ok": True, "milestones": milestones},
                        headers={"Cache-Control": "no-store"})


@app.get("/api/northstar/{proj_id}/active-milestones")
async def get_active_milestones(proj_id: str):
    """M780.1: active-only milestones (queued / pending_confirmation / pending /
    needs_clarification, but never done/held). Direct SQLite path — strips
    `conversation` from every returned stone to keep payload tiny (~5KB vs the
    full ~569KB), giving the exec agent the minimum it needs to dispatch."""
    ACTIVE = ("queued", "pending_confirmation", "pending", "needs_clarification")
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        placeholders = ",".join(["?"] * len(ACTIVE))
        rows = conn.execute(
            f"SELECT data_json FROM milestones_store "
            f"WHERE proj_id=? AND done=0 AND status IN ({placeholders}) "
            f"ORDER BY rowid DESC",
            (proj_id, *ACTIVE),
        ).fetchall()
        conn.close()
    except Exception as e:
        return JSONResponse({"ok": False, "milestones": [], "error": str(e)}, status_code=500)
    milestones = []
    for (dj,) in rows:
        try:
            m = json.loads(dj)
        except Exception:
            continue
        # Strip heavy fields — exec only needs id/text/status/parent_id/skill_ref
        m.pop("conversation", None)
        m.pop("model_used", None)
        m.pop("evidence_url", None)
        milestones.append(m)
    return JSONResponse({"ok": True, "milestones": milestones, "count": len(milestones),
                         "source": "sqlite_active_only"},
                        headers={"Cache-Control": "no-store"})


@app.get("/api/northstar/{proj_id}/causality-export")
async def causality_export(proj_id: str, request: Request):
    """M775: Export stones with outcome_label for LLM causality dataset research.
    Default format=JSONL. ?format=csv|parquet for tabular. ?no_pii_scrub=1 to skip PII masking
    (default: scrubbed). Default limit=500, hard max 5000."""
    try:
        limit = int(request.query_params.get("limit", 500))
    except (TypeError, ValueError):
        limit = 500
    limit = min(max(1, limit), 5000)
    fmt = (request.query_params.get("format") or "jsonl").lower()
    if fmt not in ("jsonl", "csv", "parquet"):
        fmt = "jsonl"
    scrub_pii = request.query_params.get("no_pii_scrub") not in ("1", "true", "yes")
    _EXPORT_FIELDS = (
        "id", "text", "status", "outcome_label", "model_used", "total_tokens",
        "exec_start", "exec_end", "substar_id", "parent_id", "conversation",
        "queued_at", "pending_confirm_at",
        "counterfactual_pair_id", "counterfactual_of", "alt_model", "alt_outcome",
        "goal_tree_snapshot", "prompt_provenance", "confounder",
    )
    # CSV/Parquet drop the nested conversation array to keep the table flat;
    # JSON-typed fields are serialized as JSON strings.
    _FLAT_FIELDS = tuple(f for f in _EXPORT_FIELDS if f != "conversation")
    _JSON_FIELDS = {"goal_tree_snapshot", "prompt_provenance", "confounder"}
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND done=1 ORDER BY rowid DESC LIMIT ?",
            (proj_id, limit),
        ).fetchall()
        conn.close()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    records = []
    for (dj,) in rows:
        try:
            m = json.loads(dj)
        except Exception:
            continue
        if scrub_pii:
            m["conversation"] = _scrub_conversation(m.get("conversation") or [])
            for tf in ("text",):
                if m.get(tf):
                    m[tf] = _scrub_pii(m[tf])
        record = {k: m.get(k) for k in _EXPORT_FIELDS}
        records.append(record)
    if fmt == "jsonl":
        lines = [json.dumps(r, ensure_ascii=False) for r in records]
        content = "\n".join(lines) + ("\n" if lines else "")
        return Response(content=content, media_type="application/x-ndjson",
                        headers={"Cache-Control": "no-store",
                                 "X-Total-Count": str(len(lines)),
                                 "X-Pii-Scrubbed": "1" if scrub_pii else "0",
                                 "X-Dataset-Version": "1.0.0"})
    if fmt == "csv":
        import csv as _csv, io as _io
        buf = _io.StringIO()
        w = _csv.DictWriter(buf, fieldnames=_FLAT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = {k: r.get(k) for k in _FLAT_FIELDS}
            for jf in _JSON_FIELDS:
                if row.get(jf) is not None and not isinstance(row[jf], str):
                    row[jf] = json.dumps(row[jf], ensure_ascii=False)
            w.writerow(row)
        return Response(content=buf.getvalue(), media_type="text/csv",
                        headers={"Cache-Control": "no-store",
                                 "X-Total-Count": str(len(records)),
                                 "X-Pii-Scrubbed": "1" if scrub_pii else "0",
                                 "X-Dataset-Version": "1.0.0",
                                 "Content-Disposition": f'attachment; filename="causality-{proj_id}-v1.csv"'})
    # parquet
    try:
        import io as _io
        try:
            import pandas as _pd
        except ImportError:
            return JSONResponse({"ok": False, "error": "pandas not installed (pip install pandas pyarrow)"}, status_code=501)
        flat_records = []
        for r in records:
            row = {k: r.get(k) for k in _FLAT_FIELDS}
            for jf in _JSON_FIELDS:
                if row.get(jf) is not None and not isinstance(row[jf], str):
                    row[jf] = json.dumps(row[jf], ensure_ascii=False)
            flat_records.append(row)
        df = _pd.DataFrame(flat_records, columns=list(_FLAT_FIELDS))
        buf = _io.BytesIO()
        try:
            df.to_parquet(buf, index=False)
        except Exception as _pe:
            return JSONResponse({"ok": False, "error": f"parquet engine missing (pip install pyarrow): {_pe}"}, status_code=501)
        return Response(content=buf.getvalue(), media_type="application/octet-stream",
                        headers={"Cache-Control": "no-store",
                                 "X-Total-Count": str(len(records)),
                                 "X-Pii-Scrubbed": "1" if scrub_pii else "0",
                                 "X-Dataset-Version": "1.0.0",
                                 "Content-Disposition": f'attachment; filename="causality-{proj_id}-v1.parquet"'})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/northstar/{proj_id}/causality-backfill")
async def causality_backfill(proj_id: str, request: Request):
    """M775.3: One-shot backfill — outcome_label + goal_tree_snapshot + prompt_provenance + confounder for all done stones.
    Pass ?force=1 to re-derive outcome_label even when already set (clears stale labels from the role-blind bug)."""
    force = request.query_params.get("force") in ("1", "true", "yes")
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        # Fetch all done stones (backfill any missing fields)
        rows = conn.execute(
            "SELECT stone_id, data_json FROM milestones_store WHERE proj_id=? AND done=1",
            (proj_id,),
        ).fetchall()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    # Load full project for goal-tree context
    try:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        proj = _parse_md_frontmatter(md) if md.exists() else {}
        milestones = proj.get("milestones", [])
    except Exception:
        proj = {}
        milestones = []
    updated = 0
    import datetime as _dt_bf
    now_bf = _dt_bf.datetime.utcnow().isoformat()
    for (stone_id, dj) in rows:
        try:
            stone = json.loads(dj)
        except Exception:
            continue
        changed = False
        if force or not stone.get("outcome_label"):
            # When force=1, temporarily clear existing label so _derive_outcome_label
            # re-runs heuristic (bypasses the manual-override-wins guard)
            _orig_label = stone.pop("outcome_label", None) if force else None
            label = _derive_outcome_label(stone)
            if label:
                stone["outcome_label"] = label
                changed = True
            elif _orig_label:
                stone["outcome_label"] = _orig_label  # restore if heuristic returned None
        if not stone.get("goal_tree_snapshot"):
            _gts = _capture_goal_tree_snapshot(stone, milestones, proj)
            if _gts:
                stone["goal_tree_snapshot"] = _gts
                changed = True
        if not stone.get("prompt_provenance"):
            _pp = _capture_prompt_provenance(stone, {})
            if _pp:
                stone["prompt_provenance"] = _pp
                changed = True
        if not stone.get("confounder"):
            _cf = _capture_confounder(stone, milestones)
            if _cf:
                stone["confounder"] = _cf
                changed = True
        if not changed:
            continue
        try:
            conn.execute(
                """UPDATE milestones_store SET data_json=?,
                   outcome_label=COALESCE(?,outcome_label),
                   goal_tree_snapshot=COALESCE(?,goal_tree_snapshot),
                   prompt_provenance=COALESCE(?,prompt_provenance),
                   confounder=COALESCE(?,confounder),
                   updated_at=? WHERE proj_id=? AND stone_id=?""",
                (json.dumps(stone, ensure_ascii=False),
                 stone.get("outcome_label"), stone.get("goal_tree_snapshot"),
                 stone.get("prompt_provenance"), stone.get("confounder"),
                 now_bf, proj_id, stone_id),
            )
            updated += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return JSONResponse({"ok": True, "backfilled": updated, "proj_id": proj_id})


@app.post("/api/northstar/{proj_id}/milestones/{mid}/counterfactual")
async def create_counterfactual(proj_id: str, mid: str, request: Request, background_tasks: BackgroundTasks):
    """M775.2: Create a counterfactual sibling stone linked by a shared UUID pair_id.
    Body: {"alt_text":"...", "alt_model":"<model>", "alt_outcome":"<expected>"}
    Returns: {"ok": True, "pair_id": "<uuid>", "new_mid": "<id>", "original_mid": "<mid>"}
    """
    data = await request.json()
    alt_text = data.get("alt_text", "").strip()
    if not alt_text:
        return JSONResponse({"ok": False, "error": "alt_text required"}, status_code=400)
    alt_model = data.get("alt_model") or None
    alt_outcome = data.get("alt_outcome") or None

    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)

    import uuid as _uuid
    pair_id = str(_uuid.uuid4())

    proj = _parse_md_frontmatter(md)
    milestones = proj.get("milestones", [])

    # Find original stone
    orig = next((m for m in milestones if isinstance(m, dict) and m.get("id") == mid), None)
    if not orig:
        return JSONResponse({"ok": False, "error": f"stone {mid} not found"}, status_code=404)

    # Stamp pair_id on original
    orig["counterfactual_pair_id"] = pair_id

    # Build sibling stone — same parent/substar, alt_text, status=queued
    existing_ids = {m.get("id", "") for m in milestones if isinstance(m, dict)}
    layer = int(orig.get("layer", 0))
    parent_id = orig.get("parent_id") or None
    if layer == 0:
        nums = [int(m.get("id", "M0")[1:]) for m in milestones
                if isinstance(m, dict) and str(m.get("id", "")).startswith("M")
                and str(m.get("id", ""))[1:].isdigit()]
        n = (max(nums) if nums else 0) + 1
        new_id = f"M{n}"
        while new_id in existing_ids:
            n += 1
            new_id = f"M{n}"
    else:
        siblings = [m for m in milestones if isinstance(m, dict) and m.get("parent_id") == parent_id]
        new_id = f"{parent_id}.{len(siblings) + 1}"
        while new_id in existing_ids:
            new_id = new_id + "x"

    from datetime import datetime as _dt_cf
    new_stone = {
        "id": new_id,
        "text": alt_text,
        "layer": layer,
        "parent_id": parent_id,
        "done": False,
        "status": "queued",
        "claude_ack": None,
        "user_added_at": _dt_cf.now().strftime("%Y-%m-%dT%H:%M"),
        "substar_id": orig.get("substar_id") or None,
        "counterfactual_pair_id": pair_id,
        "counterfactual_of": mid,
        "alt_model": alt_model,
        "alt_outcome": alt_outcome,
    }

    milestones.insert(0, new_stone)
    proj["milestones"] = milestones
    _db_save_project(proj_id, proj)
    _parse_cache.pop(str(md), None)
    import copy as _cf_copy
    background_tasks.add_task(_save_project, proj_id, _cf_copy.deepcopy(proj))
    _ns_push("counterfactual_created", proj_id=proj_id, mid=new_id, pair_id=pair_id, original_mid=mid)
    return JSONResponse({"ok": True, "pair_id": pair_id, "new_mid": new_id, "original_mid": mid})


def _jaccard_similarity(a: str, b: str) -> float:
    """Token-level Jaccard similarity between two strings."""
    set_a = set(a.lower().split())
    set_b = set(b.lower().split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


@app.get("/api/northstar/{proj_id}/counterfactual-candidates")
async def counterfactual_candidates(proj_id: str, request: Request):
    """M775.2: Suggest natural counterfactual pairs — done stones with:
    - Same substar_id
    - Different outcome_label (one success, one failure)
    - Jaccard token similarity >= 0.4
    No DB writes — pure suggestion.
    """
    try:
        limit = int(request.query_params.get("limit", 10))
    except (TypeError, ValueError):
        limit = 10
    limit = min(max(1, limit), 100)
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND done=1 AND outcome_label IS NOT NULL AND outcome_label != ''",
            (proj_id,),
        ).fetchall()
        conn.close()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    stones = []
    for (dj,) in rows:
        try:
            m = json.loads(dj)
            stones.append(m)
        except Exception:
            continue

    # Group by substar_id
    from collections import defaultdict
    by_substar: dict = defaultdict(list)
    for s in stones:
        key = s.get("substar_id") or "__none__"
        by_substar[key].append(s)

    pairs = []
    seen_pairs: set = set()
    for group in by_substar.values():
        # Find success vs failure within group
        successes = [s for s in group if s.get("outcome_label") == "success"]
        failures = [s for s in group if s.get("outcome_label") == "failure"]
        for s in successes:
            for f in failures:
                pair_key = tuple(sorted([s.get("id", ""), f.get("id", "")]))
                if pair_key in seen_pairs:
                    continue
                sim = _jaccard_similarity(s.get("text", ""), f.get("text", ""))
                if sim >= 0.4:
                    seen_pairs.add(pair_key)
                    pairs.append({
                        "stone_a": {"id": s.get("id"), "text": s.get("text"), "outcome_label": "success", "substar_id": s.get("substar_id")},
                        "stone_b": {"id": f.get("id"), "text": f.get("text"), "outcome_label": "failure", "substar_id": f.get("substar_id")},
                        "similarity": round(sim, 3),
                        "already_paired": bool(s.get("counterfactual_pair_id") or f.get("counterfactual_pair_id")),
                    })
        if len(pairs) >= limit:
            break

    pairs.sort(key=lambda p: p["similarity"], reverse=True)
    return JSONResponse({"ok": True, "proj_id": proj_id, "candidates": pairs[:limit], "total": len(pairs)})


@app.post("/api/northstar/{proj_id}/milestones/reorder")
async def reorder_milestones(proj_id: str, request: Request):
    """Reorder milestones by providing a new ordered list of IDs."""
    data = await request.json()
    new_order = data.get("order", [])
    if not new_order:
        return JSONResponse({"ok": False, "error": "order required"}, status_code=400)
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ms = proj.get("milestones", [])
    ms_map = {m.get("id"): m for m in ms if isinstance(m, dict) and m.get("id")}
    reordered = [ms_map[mid] for mid in new_order if mid in ms_map]
    leftover = [m for m in ms if isinstance(m, dict) and m.get("id") not in new_order]
    proj["milestones"] = reordered + leftover
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "count": len(reordered)})


@app.post("/api/northstar/{proj_id}/milestones")
async def create_milestone(proj_id: str, request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    milestones = proj.get("milestones", [])
    # Auto-assign ID
    existing_ids = {m.get("id","") for m in milestones if isinstance(m, dict)}
    layer = int(data.get("layer", 0))
    parent_id = data.get("parent_id") or None
    # M603: auto-promote to layer=1 when parent_id is set but layer was not sent
    if parent_id and layer == 0:
        layer = 1
    # Generate ID: M{n} for layer 0, M{parent}.{n} for layer 1
    if layer == 0:
        # M441: use max existing numeric ID + 1 (not count+1) so IDs never recycle after deletions
        # Recycled IDs cause sessionStorage flags from prior stones to incorrectly activate +msg badge.
        nums = [int(m.get("id","M0")[1:]) for m in milestones
                if isinstance(m, dict) and str(m.get("id","")).startswith("M")
                and str(m.get("id",""))[1:].isdigit()]
        n = (max(nums) if nums else 0) + 1
        new_id = f"M{n}"
        while new_id in existing_ids:
            n += 1
            new_id = f"M{n}"
    else:
        siblings = [m for m in milestones if isinstance(m, dict) and m.get("parent_id") == parent_id]
        new_id = f"{parent_id}.{len(siblings)+1}"
        while new_id in existing_ids:
            new_id = new_id + "x"
    from datetime import datetime as _dt
    _now_create = _dt.now().strftime("%Y-%m-%dT%H:%M")
    new_ms = {
        "id": new_id, "text": data.get("text", "New milestone"),
        "layer": layer, "parent_id": parent_id,
        "done": False,
        "status": data.get("status", "pending"),  # M708: default pending — Q badge off; user explicitly queues to activate
        # M224: new stones start in review state (claude_ack=None) so user sees the
        # orange review badge until Claude acknowledges — reverts M214 auto-ack behavior.
        "claude_ack": None,
        "user_added_at": _now_create,
        # M768: include substar_id from POST body so stone is grouped immediately (no two-step race)
        "substar_id": data.get("substar_id") or None,
    }
    milestones.insert(0, new_ms)  # M86: prepend so newest always appears first in UI
    proj["milestones"] = milestones
    # M318: write SQLite + invalidate cache synchronously so GET /milestones after POST sees fresh data
    _db_save_project(proj_id, proj)
    _parse_cache.pop(str(md), None)
    # Background task handles YAML backup + Turso sync (SQLite already written above)
    import copy as _c278c
    background_tasks.add_task(_save_project, proj_id, _c278c.deepcopy(proj))
    # M267: user-originated event — new stone added via UI.
    _ns_push("stone_created", proj_id=proj_id, mid=new_id,
             text=(new_ms.get("text") or "")[:140])
    # M698: record action_log entry so the activity panel shows new-stone creation
    # in real time. Previously POST /milestones was invisible to the log panel.
    _server_log_action(proj_id, new_id, "stone_create",
                       f"L{layer} parent:{parent_id or '-'} status:{new_ms.get('status','')} text:{(new_ms.get('text') or '')[:80]}")
    return JSONResponse({"ok": True, "milestone": new_ms})


_DEV_KW = ["구현", "수정", "버그", "빌드", "배포", "리팩", "코드",
           "fix", "implement", "build", "deploy", "refactor"]
_NON_DEV_KW = ["리서치", "조사", "분석", "가능한가", "공유바람", "공유 바람",
               "연구", "서치", "survey"]
# M722: server-side keyword-detection fallback for [검수] auto-trigger.
# Set False to disable without touching logic (emergency off-switch).
_AUTO_REVIEW_FALLBACK_ENABLED = True


async def _classify_dev_stone_llm(stone_text: str, stone_data: dict = None) -> bool:
    """Dev stone classifier — M964 v4: verify_flag ONLY. Keyword fallback removed per user request."""
    # Only verify_flag=True triggers [검수] auto-creation. Keyword matching removed.
    return bool(stone_data and stone_data.get("verify_flag"))


def _detect_code_change_signals(text: str) -> "dict | None":
    """M722: Deterministic keyword detector for [검수] auto-trigger fallback.

    Returns None when no code-change signal is found.
    Returns dict with matched_files, matched_actions, score when signals meet threshold.

    Threshold: at least 1 matched file AND at least 1 action verb (score used for ranking only).
    """
    import re as _re722
    _FILE_PAT = _re722.compile(
        r'(?<![/\w])([\w./-]+\.(?:py|tsx?|jsx?|html?|css|md|sh|sql|yaml|yml|json|toml))\b',
        _re722.IGNORECASE,
    )
    _LINE_PAT = _re722.compile(
        r'\bL?\d{2,5}(?:[-:]\d{2,5})?\b|\b:?line ?\d+\b',
        _re722.IGNORECASE,
    )
    _ACTION_PAT = _re722.compile(
        r'\b(추가|수정|변경|구현|edited|patched|refactored|fixed|implemented|added|changed)\b',
        _re722.IGNORECASE,
    )
    _HOT_FILES = {"server.py", "northstar.html"}

    # Collect matched files (deduplicated, basename only for hot-file check)
    _raw_files = _FILE_PAT.findall(text)
    _matched_files = list(dict.fromkeys(_raw_files))  # preserve order, dedupe

    if not _matched_files:
        return None

    # Score accumulation
    score = len(_matched_files)  # 1 point per unique file mention

    # Line reference bonus (capped at 3)
    _line_hits = len(_LINE_PAT.findall(text))
    score += min(_line_hits, 3)

    # Hot-file bonus
    for _f in _matched_files:
        if _f.split("/")[-1] in _HOT_FILES:
            score += 2

    # Action verbs
    _raw_actions = _ACTION_PAT.findall(text)
    _matched_actions = list(dict.fromkeys(a.lower() for a in _raw_actions))

    # Threshold check — always require at least 1 action verb.
    # Hot-file bonus raises score for ranking but cannot alone trigger [검수].
    if not _matched_actions:
        return None

    return {
        "matched_files": _matched_files[:10],   # cap to avoid huge payloads
        "matched_actions": _matched_actions[:10],
        "score": score,
    }


# M996 → M1022.1: [검수] stone text now mandates UI-grounded evidence for any UI/visual change.
# Static code review alone produces false PASSes ("the code looks right but did it actually render?").
# When the diff touches HTML/CSS/JS that affects the rendered UI, the reviewer MUST run Playwright
# to verify and attach a screenshot+counted observations as evidence.
def _build_review_stone_text(mid: str, brief: str) -> str:
    # M1063: simplified — UI-focused, concise
    return (
        f"[검수] {mid}: {brief}\n"
        f"①변경파일/함수 명시 + 회귀위험(영향 범위)\n"
        f"②UI 실측 필수(Playwright navigate→동작→DOM 측정·스크린샷) — UI 변경이면 before/after 스크린샷 1쌍\n"
        f"③의도대로 동작하는지 + 엣지케이스(null/empty/동시성)\n"
        f"④OWASP 해당 여부(XSS/SQLi/auth 등)\n"
        f"⑤완성도 0-10점"
    )


@app.patch("/api/northstar/{proj_id}/milestones/{mid}")
async def update_milestone(proj_id: str, mid: str, request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    force_done = (request.query_params.get("force") in ("1", "true", "yes"))
    # M878: caller diagnostics + owner validation — log + warn when session PATCHes non-owned stone
    try:
        _caller_ip = request.client.host if request.client else "unknown"
        _ua = request.headers.get("user-agent", "")[:60]
        _status_delta = data.get("status", "")
        _append_role = (data.get("append_message") or {}).get("role", "")
        # M878 v2: detect which tmux session this PATCH originates from (via NS_SESSION_KEY env header)
        _caller_sess = request.headers.get("x-ns-session-key", "").strip()
        _server_log_action(proj_id, mid, "patch:caller",
                           f"ip={_caller_ip} sess={_caller_sess} status={_status_delta} append={_append_role} ua={_ua[:30]}")
    except Exception:
        pass
    md = PROJECTS_DIR / proj_id / "north-star.md"
    # M981: project dir is sufficient — north-star.md now optional (SQLite fallback)
    if not (PROJECTS_DIR / proj_id).exists() and not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    # M470 part 2: serialize read-modify-write so concurrent PATCH requests on the
    # same project cannot clobber each other (e.g. text-blur saveMs racing with
    # queue-toggle click on the same stone, where each reads proj independently
    # and the later save with a stale read overwrites the earlier status change).
    async with _get_proj_lock(proj_id):
        return await _update_milestone_locked(proj_id, mid, data, md, background_tasks, force_done)


async def _update_milestone_locked(proj_id: str, mid: str, data: dict, md: Path, background_tasks: BackgroundTasks, force_done: bool = False):
    proj = _parse_md_frontmatter(md)
    milestones = proj.get("milestones", [])
    updated = False
    updated_m = None
    now_iso = __import__('datetime').datetime.now().strftime("%Y-%m-%dT%H:%M")
    import datetime as _dt
    for m in milestones:
        if isinstance(m, dict) and m.get("id") == mid:
            _blink_old_status = m.get("status")  # M1007: capture pre-mutation status for server-side blink trigger
            # User-settable fields: text, layer, parent_id, claude_ack, status=queued/pending only
            # M216: `held` = user-paused stone, excluded from EXECUTE/REPLY SYNC queues until released.
            for k in ("text", "layer", "parent_id", "claude_ack", "cron_job_id", "claude_comment", "star_relation", "substar_id", "held", "verify_flag",  # M964: verify toggle
                      "skill_ref", "agent_ref", "skill_refs", "agent_refs",
                      "total_tokens", "model_used", "exec_start", "exec_end",  # M287 monetization fields
                      "watching",   # M514: user-toggleable watch badge
                      "evidence_url",  # M511: proof link
                      "moscow",        # M541: MoSCoW priority (M/S/C/W/null)
                      "redo_count",    # M511: monetization — how many re-queues after completion
                      "wave_index",    # M511: parallel dispatch wave group (0=first wave, 1=second, etc.)
                      # M511 paper-tier dataset fields:
                      "completion_status",       # success / partial / fail
                      "failure_reason",          # rate_limit / wrong_output / ambiguous_task / user_rejected
                      "input_tokens",            # raw token breakdown for cost attribution
                      "output_tokens",
                      "cache_creation_tokens",
                      "cache_read_tokens",
                      "cost_usd",                # computed cost (model pricing × token breakdown)
                      "outcome_label",           # M775: causal dataset label (success/failure/abandoned)
                      # M803: hub-live causality fields (PATCH-writable by Claude)
                      "goal_tree_snapshot",      # JSON snapshot of parent+siblings status
                      "prompt_provenance",       # "PARENT_MID:short-reason" string
                      "confounder",              # JSON: session_start_ts, git_hash, model, exec_duration_sec
                      "counterfactual_pair_id"):  # peer child stone ID at branch points
                if k in data:
                    m[k] = data[k] if data[k] else None
                    # M118: clear stale star_relation when milestone text changes
                    if k == "text" and data[k]:
                        m.pop("star_relation", None)
                    # M266: when star_relation is (re)set, snapshot the current project
                    # target so the UI can later flag the rationale as stale (⚠ overlay)
                    # if the user moves the goalpost. Zero token cost — purely server-side.
                    if k == "star_relation" and data[k]:
                        m["star_target_at_completion"] = proj.get("target") or None
                    # M511: auto-stamp evidence_updated_at when evidence_url changes
                    if k == "evidence_url" and data[k]:
                        m["evidence_updated_at"] = now_iso
                    # M1059: verify_flag child creation moved to queued-time (not flag-set-time)
                    # Previously: M990 created [검수] child immediately when flag toggled ON
                    # Now: flag just marks the stone; child created only when stone is queued
            # M725: rename stone ID when parent_id is newly (or re-)assigned
            _725_new_parent = data.get("parent_id")
            if _725_new_parent and not (m.get("id", "").startswith(_725_new_parent + ".")):
                _725_old_id = mid  # capture original before rename
                _725_existing = {x.get("id", "") for x in milestones if isinstance(x, dict)}
                _725_siblings = [x for x in milestones if isinstance(x, dict)
                                 and x.get("parent_id") == _725_new_parent
                                 and x.get("id") != _725_old_id]
                _725_n = len(_725_siblings) + 1
                _725_new_id = f"{_725_new_parent}.{_725_n}"
                while _725_new_id in _725_existing:
                    _725_n += 1
                    _725_new_id = f"{_725_new_parent}.{_725_n}"
                m["id"] = _725_new_id
                # auto-promote layer if still at 0
                if not m.get("layer"):
                    _725_parent_stone = next((x for x in milestones if isinstance(x, dict) and x.get("id") == _725_new_parent), None)
                    m["layer"] = ((_725_parent_stone.get("layer") or 0) + 1) if _725_parent_stone else 1
                # cascade: fix children that reference the old id
                for _725_child in milestones:
                    if isinstance(_725_child, dict) and _725_child.get("parent_id") == _725_old_id:
                        _725_child["parent_id"] = _725_new_id
                mid = _725_new_id  # keep mid in sync for rest of handler
            # conversation: accumulated chat thread — allow empty list (don't coerce to None)
            if "conversation" in data:
                m["conversation"] = data["conversation"]
            # append_message: single {role,text} dict — appended to conversation (easier for Claude)
            # M643: accept both 'text' and 'content' keys — Claude sometimes sends 'content' by mistake
            if "append_message" in data:
                msg = data["append_message"]
                if isinstance(msg, dict) and not msg.get("text") and msg.get("content"):
                    msg = dict(msg, text=msg["content"])
                if isinstance(msg, dict) and msg.get("role") and msg.get("text"):
                    # M190: forbid claude→claude consecutive appends. Claude can only post when
                    # (a) conversation is empty (initial comment), or (b) last entry is from user.
                    # Prevents self-reply chains that violate ns-comment-reply-protocol.md Rule 1+2.
                    if msg.get("role") == "claude":
                        _conv_so_far = m.get("conversation") or []
                        if _conv_so_far and _conv_so_far[-1].get("role") == "claude":
                            return JSONResponse({
                                "ok": False,
                                "error": "claude_self_reply_blocked",
                                "detail": "Last conversation entry is already claude. Wait for user reply or initial-comment-request trigger (M190, ns-comment-reply-protocol.md Rule 1)."
                            }, status_code=409)
                        # M780.2: stale-reference guard — block fallback phrases that
                        # signal Claude lost context. These come from compression-induced
                        # blind replies (e.g. "이전 답변 완료 — ...참고") and degrade
                        # accuracy. The reply must be retried with the actual freshly-read
                        # conversation[]; pass header X-Allow-Stale: 1 to override.
                        _stale_text = str(msg.get("text", "")).strip()
                        _stale_phrases = (
                            "이전 답변 완료", "이전 분석 답변", "이전 답변 참조",
                            "추가 작업 없음", "재큐 후 추가",
                            # M787 expansion: 7 FN variants surfaced by meta-verification
                            "이전과 동일", "변경사항 없음", "별 작업 없음",
                            "이전 응답으로 갈음", "작업 없음", "이미 답변함",
                            "앞서 설명한 대로",
                            # M780.5 expansion: 7 FN variants (Korean morphological cluster)
                            "직전 답변 참조", "위와 동일", "앞서 언급한",
                            "답변 동일", "추가 변경 없음", "이전 코멘트 참조",
                            "동일한 답변",
                        )
                        import re as _re
                        _stale_regex_patterns = (
                            # morphological variants: 이전과 동일함/동일합니다/같습니다
                            _re.compile(r"이전과\s*(?:동일|같)", _re.UNICODE),
                            # 변경사항 없음/없습니다/없어요
                            _re.compile(r"변경\s*사항\s*없", _re.UNICODE),
                            # 위와 동일함/동일합니다
                            _re.compile(r"위와\s*동일", _re.UNICODE),
                            # 앞서 언급한/말씀드린/설명한
                            _re.compile(r"앞서\s*(?:언급|말씀|설명)", _re.UNICODE),
                            # 추가 변경 없음/없습니다
                            _re.compile(r"추가\s*변경\s*없", _re.UNICODE),
                            # 답변은? 동일
                            _re.compile(r"답변\s*(?:은|이)?\s*동일", _re.UNICODE),
                            # 이전 응답과? 갈음/대체
                            _re.compile(r"이전\s*응답(?:으로|과)?\s*(?:갈음|대체)", _re.UNICODE),
                        )
                        _allow_stale = bool(data.get("allow_stale"))
                        _stale_regex_hit = None
                        for _pat in _stale_regex_patterns:
                            _m = _pat.search(_stale_text)
                            if _m:
                                _stale_regex_hit = _m.group(0)
                                break
                        if (any(p in _stale_text for p in _stale_phrases) or _stale_regex_hit) and not _allow_stale:
                            _literal_hit = next((p for p in _stale_phrases if p in _stale_text), None)
                            _hit = _literal_hit or _stale_regex_hit
                            # M787: telemetry — log every block so hit-rate is verifiable
                            try:
                                _server_log_action(proj_id, mid, "stale_reference_blocked", _hit)
                            except Exception:
                                pass
                            return JSONResponse({
                                "ok": False,
                                "error": "stale_reference_blocked",
                                "matched_phrase": _hit,
                                "detail": (
                                    f"Reply contains stale-reference phrase '{_hit}' which "
                                    f"signals context loss. Re-read conversation[] via GET "
                                    f"and PATCH again with a concrete answer, or set "
                                    f"\"allow_stale\":true in the body to override (M780.2)."
                                ),
                            }, status_code=422)
                    # M185: enforce 3-line TL;DR cap on comments. Long replies are
                    # truncated and a doc-reference suffix is appended per the
                    # ns-comment-reply-protocol.md (details go in docs/ns-replies/).
                    _text = str(msg.get("text", ""))
                    _lines = _text.split("\n")
                    if len(_lines) > 3:
                        # Write the full text to docs/ns-replies for the user to find later
                        try:
                            _replies_dir = Path.home() / "Project" / "Moat" / "docs" / "ns-replies"
                            _replies_dir.mkdir(parents=True, exist_ok=True)
                            _date = _dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")
                            _full_file = _replies_dir / f"{_date}-{proj_id}-{mid}.md"
                            _full_file.write_text(
                                f"# Full reply (truncated in comment thread) — {proj_id} / {mid}\n\n"
                                f"**Truncated to first 3 lines in the stone pane.**\n"
                                f"Posted: {msg.get('ts','')}\n\n---\n\n{_text}\n",
                                encoding="utf-8"
                            )
                            _ref = f"docs/ns-replies/{_full_file.name}"
                        except Exception:
                            _ref = "docs/ns-replies/<save-failed>"
                        msg["text"] = "\n".join(_lines[:3]) + f"\n[details: {_ref}]"
                        msg["truncated"] = True
                    msg.setdefault("ts", _dt.datetime.now().isoformat())
                    conv = m.get("conversation") or []
                    conv.append(msg)
                    m["conversation"] = conv
                    # M1050/M1060: verify_flag resets on every chat message (user or claude)
                    if m.get("verify_flag"):
                        m["verify_flag"] = False
                    # M535: log server-side comment event
                    _server_log_action(proj_id, mid, f"comment:{msg.get('role','?')}",
                                       (msg.get("text",""))[:120])
                    # M222: claude completing work on a queued stone → pending_confirmation.
                    # M247 fix: only promote from queued→pending_confirmation, NOT pending→pending_confirmation.
                    # Promoting pending (review) stones caused auto-queue chain:
                    #   initial-comment → pending_confirmation → user reply → queued (line 2174).
                    # M722: capture pre-M222 status so fallback detector knows if exec-agent path ran.
                    _m722_pre_status = m.get("status") or ("done" if m.get("done") else "pending")
                    if msg.get("role") == "claude":
                        _cur_status = _m722_pre_status
                        if _cur_status == "queued":
                            m["status"] = "pending_confirmation"
                            m["done"] = False
                            m["pending_confirm_at"] = now_iso  # always update so _isLastTurn detects re-completions
                            m.setdefault("claude_ack", now_iso)
                    # M722: server-side keyword-detection fallback for [검수] auto-trigger.
                    # Fires on claude append_message when status was NOT "queued" before this PATCH
                    # (queued→pending_confirmation path already handled by exec-agent's M767 inline
                    # creation in the EXECUTE SYNC prompt — skip to avoid duplicates).
                    # Also fires for REPLY MODE / sub-agent paths where M767 prompt never runs.
                    if (
                        _AUTO_REVIEW_FALLBACK_ENABLED
                        and msg.get("role") == "claude"
                    ):
                        # M851 fix: fire fallback for queued stones too (REPLY MODE path).
                        # Was: only non-queued. Risk of duplicates if exec-agent ALSO creates [검수]
                        # is already handled by the _m722_has_review idempotency check below.
                        if True:
                            # M913: use LLM dev-stone classifier instead of file+action keyword detection
                            # Trigger review stone if stone TEXT is dev-related (not just completion msg keywords)
                            _m722_is_dev = await _classify_dev_stone_llm(m.get("text", ""), m)
                            if _m722_is_dev:
                                _m722_has_review = any(
                                    isinstance(_s, dict)
                                    and _s.get("parent_id") == mid
                                    and str(_s.get("text", "")).startswith("[검수]")
                                    for _s in milestones
                                )
                                if not _m722_has_review:
                                    _m722_existing_ids = {_s.get("id", "") for _s in milestones if isinstance(_s, dict)}
                                    _m722_siblings = [
                                        _s for _s in milestones
                                        if isinstance(_s, dict) and _s.get("parent_id") == mid
                                    ]
                                    _m722_rev_id = f"{mid}.{len(_m722_siblings)+1}"
                                    while _m722_rev_id in _m722_existing_ids:
                                        _m722_rev_id += "x"
                                    _m722_layer = (m.get("layer", 0) or 0) + 1
                                    _m722_rev_text = _build_review_stone_text(mid, str(m.get("text",""))[:80])
                                    milestones.append({
                                        "id": _m722_rev_id,
                                        "text": _m722_rev_text,
                                        "layer": _m722_layer,
                                        "parent_id": mid,
                                        "done": False,
                                        "status": "queued",
                                        "claude_ack": None,
                                        "user_added_at": _dt.datetime.now().strftime("%Y-%m-%dT%H:%M"),
                                        "skill_refs": ["code-review"],  # M996
                                    })
                                    _server_log_action(
                                        proj_id, _m722_rev_id,
                                        "auto_review_created_dev_llm",
                                        f"stone_text_classified_as_dev",
                                    )
                    # M267: emit SSE event ONLY for user-originated comments — NOT claude.
                    if msg.get("role") == "user":
                        _ns_push("user_commented", proj_id=proj_id, mid=mid,
                                 text=(msg.get("text") or "")[:140])
                    # M184: when user adds a comment, dispatch REPLY SYNC immediately.
                    # M194: auto-reopen pending_confirmation → queued on user reply.
                    # M479: extend auto-reopen to pending and needs_clarification:
                    #   when user sends a message on any non-held, non-done, non-queued stone
                    #   it signals engagement → re-queue so Claude picks it up promptly.
                    #   (M247 is unaffected: that rule governs claude comments on pending, not user comments.)
                    if msg.get("role") == "user" and not m.get("held") and not m.get("done"):
                        _cur_s = m.get("status") or ("done" if m.get("done") else "pending")
                        if _cur_s in ("pending_confirmation", "pending", "needs_clarification"):
                            m["status"] = "queued"
                            m["done"] = False
                    # M225: skip REPLY SYNC dispatch if stone is held (zero claude token spend on held).
                    if msg.get("role") == "user" and not m.get("done") and not m.get("held"):
                        session_name = _live_exec_session_name(proj_id)
                        check = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
                        # M523: always write REPLY SYNC queue entry regardless of session state.
                        # Previously gated on check.returncode==0 — but when no exec session is
                        # running, the entry was silently dropped, leaving user comments unanswered
                        # until the next Execute click re-read the milestones list. Now the entry
                        # is always queued; Execute button or next session start will consume it.
                        _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
                        _qf.parent.mkdir(parents=True, exist_ok=True)
                        # M138: skill/agent annotations from the comment message only
                        _skill_parts = []
                        for _sr in (msg.get("skill_refs") or ([msg["skill_ref"]] if msg.get("skill_ref") else [])):
                            _skill_parts.append(f"[skill: /{_sr}]")
                        for _ar in (msg.get("agent_refs") or ([msg["agent_ref"]] if msg.get("agent_ref") else [])):
                            _skill_parts.append(f"[agent: {_ar}]")
                        _skill_annotation = ("\n" + " ".join(_skill_parts) + " — use these when acting on this message.") if _skill_parts else ""
                        _entry = json.dumps({
                            "ts": _dt.datetime.now().isoformat(),
                            "body": (
                                f"[REPLY SYNC — Q&A MODE] User commented on stone {mid}:\n"
                                f"  \"{msg.get('text','')}\"{_skill_annotation}\n"
                                f"⚠️  THIS IS A REPLY TASK, NOT A COMPLETION TASK.\n"
                                f"  DO NOT re-implement the stone. DO NOT change status to pending_confirmation.\n"
                                f"  Your ONLY job: read the comment and reply with ≤3 lines.\n"
                                f"(If the message is long or references context, call\n"
                                f" GET /api/northstar/{proj_id}/milestones to read conversation[] first.)\n\n"
                                f"PROTOCOL:\n"
                                f"  1. Read the user comment above carefully.\n"
                                f"  1b. M246: If [skill:/name] or [agent:name] annotated: invoke it, then reply with 1-line result.\n"
                                f"  2. ANSWER the question OR acknowledge the instruction in ≤3 lines via PATCH "
                                f"http://100.119.82.4:9000/api/northstar/{proj_id}/milestones/{mid} "
                                f"with body append_message {{role:'claude', text:'<≤3 lines>'}}.\n"
                                f"  3. ONLY if user explicitly says 're-do', 'fix', 'redo', '다시', '수정해' → also re-implement.\n"
                                f"  4. DO NOT change status — this reply does NOT advance the stone lifecycle.\n"
                                f"  5. If reply needs more than 3 lines: write docs/ns-replies/<DATE>-{mid}.md and reference path.\n"
                                f"  6. M270: No follow-up questions unless genuinely missing critical info.\n"
                                f"Mandatory — do this BEFORE any other queued work."
                            ),
                        }, ensure_ascii=False)
                        with _qf.open("a", encoding="utf-8") as _qh:
                            _qh.write(_entry + "\n")
                        # M184/M148: wake Claude if pane is idle — stop hook can't fire without response event.
                        if check.returncode == 0:
                            try:
                                _pane = subprocess.run(
                                    ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-8"],
                                    capture_output=True, text=True, timeout=2,
                                ).stdout
                                _pane_idle = ("❯" in _pane and
                                              "esc to i" not in _pane and
                                              "… (" not in _pane)
                                if _pane_idle:
                                    subprocess.run(
                                        ["tmux", "send-keys", "-t", session_name, "go", "Enter"],
                                        capture_output=True, timeout=2,
                                    )
                                    _ns_push("session_running", proj_id=proj_id, kind="exec")
                            except Exception:
                                pass
            # Status: user can set pending/queued; done is Claude-only (set via done=True or status=done)
            new_status = data.get("status")
            if new_status in ("pending", "queued"):
                _prev_status = m.get("status")  # capture before overwrite
                m["status"] = new_status
                m["done"] = False
                if new_status == "queued":
                    m.setdefault("queued_at", now_iso)
                    # M511: redo_count — increments when a completed stone is re-queued
                    if _prev_status in ("pending_confirmation", "done"):
                        m["redo_count"] = (m.get("redo_count") or 0) + 1
                    # M511.3: reopen_count — difficulty proxy (how many times user reopened)
                    if _prev_status == "pending_confirmation":
                        m["reopen_count"] = (m.get("reopen_count") or 0) + 1
                    # M1059: verify_flag=True → create [검수] child NOW (stone is being queued)
                    if m.get("verify_flag"):
                        _vq_has_review = any(
                            isinstance(_s, dict) and _s.get("parent_id") == mid
                            and str(_s.get("text", "")).startswith("[검수]")
                            and _s.get("status") not in ("done",)
                            for _s in milestones
                        )
                        if not _vq_has_review:
                            _vq_existing_ids = {_s.get("id", "") for _s in milestones if isinstance(_s, dict)}
                            _vq_siblings = [_s for _s in milestones if isinstance(_s, dict) and _s.get("parent_id") == mid]
                            _vq_rev_id = f"{mid}.{len(_vq_siblings)+1}"
                            while _vq_rev_id in _vq_existing_ids:
                                _vq_rev_id += "x"
                            milestones.append({
                                "id": _vq_rev_id,
                                "text": _build_review_stone_text(mid, str(m.get("text",""))[:80]),
                                "layer": (m.get("layer", 0) or 0) + 1,
                                "parent_id": mid,
                                "done": False,
                                "status": "queued",
                                "claude_ack": None,
                                "user_added_at": now_iso,
                                "skill_refs": ["code-review"],
                            })
                else:
                    m.pop("queued_at", None)
            elif new_status == "needs_clarification":
                m["status"] = "needs_clarification"
                m["done"] = False
                if "clarification_question" in data:
                    m["clarification_question"] = data["clarification_question"]
                if "clarification_answer" in data:
                    m["clarification_answer"] = data["clarification_answer"]
                    m["clarification_answered_at"] = now_iso
            elif new_status == "pending_confirmation":
                # Stop hook: waiting for user to confirm within 24h
                m["status"] = "pending_confirmation"
                m["done"] = False
                m["pending_confirm_at"] = now_iso  # always update for _isLastTurn green border
                # M1047: evidence_url validation — warn when visual work has no proof attached
                _ev_url = data.get("evidence_url") or m.get("evidence_url") or ""
                _stone_txt = (m.get("text") or "").lower()
                _visual_keywords = ("화면", "ui", "스크린샷", "검수", "screenshot", "visual", "proof", "chart", "table", "증거", "증빙")
                _is_visual = any(kw in _stone_txt for kw in _visual_keywords)
                if _is_visual and not _ev_url:
                    # Flag missing proof — does not block the PATCH
                    m.setdefault("_proof_warning", "evidence_url missing — proof badge will not be generated; add screenshot/link via PATCH evidence_url")
                    _server_log_action(proj_id, mid, "warn:evidence_url_missing",
                                       "pending_confirmation on visual stone without evidence_url — proof badge skipped")
                if "claude_ack" not in data:
                    m["claude_ack"] = now_iso
                # M549.1: guard — if conversation has no recent claude message, auto-append fallback.
                # Claude sometimes PATCHes status=pending_confirmation without append_message,
                # leaving the stone with no visible completion reply.
                _conv_for_guard = m.get("conversation") or []
                _last_msg_role = _conv_for_guard[-1].get("role") if _conv_for_guard else None
                if _last_msg_role != "claude" and not data.get("append_message"):
                    # Build fallback text from stone context
                    _stone_text = (m.get("text") or m.get("content") or "")[:60].strip()
                    _rel = (m.get("star_relation") or "").strip()
                    _fallback_text = (
                        f"완료. ({_rel})" if _rel
                        else f"완료. ({_stone_text})" if _stone_text
                        else "완료."
                    )
                    _fb_msg = {"role": "claude", "text": _fallback_text,
                               "ts": _dt.datetime.now().isoformat(),
                               "_auto_fallback": True}
                    _conv_for_guard.append(_fb_msg)
                    m["conversation"] = _conv_for_guard
                    _server_log_action(proj_id, mid, "comment:claude(auto-fallback)", _fallback_text)
                # M550: sequential child cascade — when a child stone completes, auto-queue
                # the next unprocessed sibling so the dispatch system picks it up.
                # Without this, siblings with status=None are invisible to dispatch.
                if m.get("layer", 0) == 1 and m.get("parent_id"):
                    _par_id = m["parent_id"]
                    _unstarted = [
                        s for s in milestones
                        if isinstance(s, dict)
                        and s.get("parent_id") == _par_id
                        and s.get("id") != mid
                        and not s.get("done")
                        and s.get("status") not in ("pending_confirmation", "queued", "done")
                        and not s.get("held")
                    ]
                    def _sib_sort_key(s):
                        parts = (s.get("id") or "").rsplit(".", 1)
                        try: return int(parts[-1]) if len(parts) > 1 else 0
                        except ValueError: return 0
                    _unstarted.sort(key=_sib_sort_key)
                    if _unstarted:
                        _nxt = _unstarted[0]
                        _nxt["status"] = "queued"
                        _nxt.setdefault("queued_at", _dt.datetime.now().isoformat())
                        _server_log_action(proj_id, _nxt["id"], "auto_queued",
                                           f"cascade from sibling {mid} completion")
                # M476/M509: store session_id if provided, then compute tokens
                if data.get("session_id"):
                    m["session_id"] = data["session_id"]
                if not m.get("total_tokens") and (m.get("exec_start") or data.get("exec_start")) and (m.get("exec_end") or data.get("exec_end")):
                    _t_start = m.get("exec_start") or data.get("exec_start")
                    _t_end   = m.get("exec_end")   or data.get("exec_end")
                    _sid = m.get("session_id") or data.get("session_id")
                    try:
                        _computed = _compute_tokens_from_transcript(proj_id, _t_start, _t_end, session_id=_sid, return_breakdown=True)
                        if isinstance(_computed, dict):
                            m["total_tokens"] = _computed.get("total")
                            if not m.get("input_tokens"):
                                m["input_tokens"] = _computed.get("input") or None
                            if not m.get("output_tokens"):
                                m["output_tokens"] = _computed.get("output") or None
                            if not m.get("cache_creation_tokens"):
                                m["cache_creation_tokens"] = _computed.get("cache_creation") or None
                            if not m.get("cache_read_tokens"):
                                m["cache_read_tokens"] = _computed.get("cache_read") or None
                        elif _computed is not None:
                            m["total_tokens"] = _computed
                    except Exception:
                        pass
                # M511: auto-compute cost_usd from token breakdown if provided
                _model_for_cost = (data.get("model_used") or m.get("model_used") or "").lower()
                _inp = data.get("input_tokens") or m.get("input_tokens") or 0
                _out = data.get("output_tokens") or m.get("output_tokens") or 0
                _ccr = data.get("cache_creation_tokens") or m.get("cache_creation_tokens") or 0
                _crd = data.get("cache_read_tokens") or m.get("cache_read_tokens") or 0
                if not m.get("cost_usd") and (_inp or _out):
                    # Pricing per 1M tokens (Anthropic 2025 public rates)
                    _PRICES = {
                        "sonnet": (3.0, 15.0, 3.75, 0.30),
                        "opus":   (15.0, 75.0, 18.75, 1.50),
                        "haiku":  (0.80, 4.0, 1.0, 0.08),
                    }
                    _tier = next((k for k in _PRICES if k in _model_for_cost), "sonnet")
                    _ip, _op, _ccp, _crp = _PRICES[_tier]
                    _cost = (_inp * _ip + _out * _op + _ccr * _ccp + _crd * _crp) / 1_000_000
                    if _cost > 0:
                        m["cost_usd"] = round(_cost, 6)
                # M562: auto-export to .omc/milestone-decisions.md for CTX/G2 retrieval
                _export_milestone_decision(proj_id, m)
                # M54.7: extract structured Knowledge Object → .omc/facts.jsonl (ADR format)
                _extract_knowledge_object(proj_id, m)
            elif new_status is None and not m.get("total_tokens") and (data.get("exec_start") or data.get("exec_end")):
                # M509: exec times supplied without status change — still compute tokens.
                _t_start = data.get("exec_start") or m.get("exec_start")
                _t_end   = data.get("exec_end")   or m.get("exec_end")
                _sid = m.get("session_id") or data.get("session_id")
                if _t_start and _t_end:
                    try:
                        _computed = _compute_tokens_from_transcript(proj_id, _t_start, _t_end, session_id=_sid)
                        if _computed is not None:
                            m["total_tokens"] = _computed
                    except Exception:
                        pass
            if new_status == "done" or data.get("done") is True:
                # M468: commit-gating — a mother stone cannot be marked done until all
                # its substones are done. Bypass with ?force=1 query param if explicitly
                # needed (e.g. legacy data with orphan children).
                _children = [x for x in milestones
                             if isinstance(x, dict) and x.get("parent_id") == mid]
                _open_children = [c for c in _children
                                  if (c.get("status") or ("done" if c.get("done") else "pending")) != "done"]
                if _open_children and not force_done:
                    _ids = ", ".join(c.get("id", "?") for c in _open_children)
                    return JSONResponse({
                        "ok": False,
                        "error": "substones_not_done",
                        "open_substones": [c.get("id") for c in _open_children],
                        "detail": (
                            f"Mother stone {mid} cannot be committed until all substones are done. "
                            f"Open: {_ids}. Commit each substone first, or pass ?force=1 to override."
                        ),
                    }, status_code=409)
                # Claude-only OR user confirmed: mark done
                m["status"] = "done"
                m["done"] = True
                m["done_at"] = now_iso
                m.pop("pending_confirm_at", None)
                if "claude_ack" not in data:
                    m["claude_ack"] = now_iso
                # M511: auto-label completion_status=success on commit (unless already set)
                if not m.get("completion_status"):
                    m["completion_status"] = "success"
                # M775: auto-derive outcome_label (manual override in data takes precedence)
                if "outcome_label" in data:
                    m["outcome_label"] = data["outcome_label"]
                elif not m.get("outcome_label"):
                    m["outcome_label"] = _derive_outcome_label(m)
                # M775.3: capture goal_tree_snapshot, prompt_provenance, confounder at done-transition
                if not m.get("goal_tree_snapshot"):
                    _gts = _capture_goal_tree_snapshot(m, milestones, proj)
                    if _gts:
                        m["goal_tree_snapshot"] = _gts
                if not m.get("prompt_provenance"):
                    _pp = _capture_prompt_provenance(m, data)
                    if _pp:
                        m["prompt_provenance"] = _pp
                if not m.get("confounder"):
                    _cf = _capture_confounder(m, milestones)
                    if _cf:
                        m["confounder"] = _cf
                # M780.3: compress conversation[] at done-transition into a 3-line
                # summary — first user msg, last user msg, final claude msg — and
                # drop the full thread. /milestones still drops `conversation` for
                # done stones (M527), but `conversation_summary` survives and gives
                # the agent / UI a glanceable history without inflating payload.
                _conv_full = m.get("conversation") or []
                if _conv_full and not m.get("conversation_summary"):
                    def _trim(_t):
                        _t = str(_t or "").replace("\n", " ").strip()
                        return _t if len(_t) <= 160 else _t[:157] + "..."
                    _user_msgs = [c for c in _conv_full if c.get("role") == "user"]
                    _claude_msgs = [c for c in _conv_full if c.get("role") == "claude"]
                    _first_user = _user_msgs[0] if _user_msgs else None
                    _last_user = _user_msgs[-1] if _user_msgs else None
                    _last_claude = _claude_msgs[-1] if _claude_msgs else None
                    _lines = []
                    if _first_user:
                        _lines.append("U1: " + _trim(_first_user.get("text") or _first_user.get("content")))
                    if _last_user and _last_user is not _first_user:
                        _lines.append("Un: " + _trim(_last_user.get("text") or _last_user.get("content")))
                    if _last_claude:
                        _lines.append("C: " + _trim(_last_claude.get("text") or _last_claude.get("content")))
                    m["conversation_summary"] = "\n".join(_lines)
                    m["conversation_turn_count"] = len(_conv_full)
                # M550 v2: when a [검수] review child reaches done, auto-confirm its mother
                # IF (a) mother is in pending_confirmation, AND (b) all other children of the
                # mother are also done. The existing M550 cascade then promotes the next mother
                # sibling — so a mother → review-done → mother-done → next-mother chain runs
                # without manual user confirm clicks. (M468 commit-gating still enforced.)
                if m.get("layer", 0) == 1 and m.get("parent_id") \
                        and str(m.get("text", "")).startswith("[검수]"):
                    _par_id = m["parent_id"]
                    _par_stone = next(
                        (s for s in milestones
                         if isinstance(s, dict) and s.get("id") == _par_id),
                        None,
                    )
                    if _par_stone and _par_stone.get("status") == "pending_confirmation":
                        _par_siblings = [
                            s for s in milestones
                            if isinstance(s, dict) and s.get("parent_id") == _par_id
                            and s.get("id") != mid
                        ]
                        _all_sib_done = all(
                            (s.get("status") or ("done" if s.get("done") else "pending")) == "done"
                            for s in _par_siblings
                        )
                        if _all_sib_done:
                            _par_stone["status"] = "done"
                            _par_stone["done"] = True
                            _par_stone["done_at"] = now_iso
                            _par_stone.pop("pending_confirm_at", None)
                            if not _par_stone.get("completion_status"):
                                _par_stone["completion_status"] = "success"
                            # M775: capture causality fields on auto-confirm path
                            _par_stone["outcome_label"] = _derive_outcome_label(_par_stone)
                            if not _par_stone.get("goal_tree_snapshot"):
                                _gts_par = _capture_goal_tree_snapshot(_par_stone, milestones, proj)
                                if _gts_par:
                                    _par_stone["goal_tree_snapshot"] = _gts_par
                            if not _par_stone.get("prompt_provenance"):
                                _pp_par = _capture_prompt_provenance(_par_stone, {})
                                if _pp_par:
                                    _par_stone["prompt_provenance"] = _pp_par
                            if not _par_stone.get("confounder"):
                                _cf_par = _capture_confounder(_par_stone, milestones)
                                if _cf_par:
                                    _par_stone["confounder"] = _cf_par
                            # M780.3 monitor fix: auto-confirm path was bypassing the
                            # summary computation. Apply the same U1/Un/C 3-line summary
                            # to the mother so dashboard/agent can still read history.
                            _par_conv = _par_stone.get("conversation") or []
                            if _par_conv and not _par_stone.get("conversation_summary"):
                                def _trim_p(_t):
                                    _t = str(_t or "").replace("\n", " ").strip()
                                    return _t if len(_t) <= 160 else _t[:157] + "..."
                                _u_msgs = [c for c in _par_conv if c.get("role") == "user"]
                                _c_msgs = [c for c in _par_conv if c.get("role") == "claude"]
                                _f_user = _u_msgs[0] if _u_msgs else None
                                _l_user = _u_msgs[-1] if _u_msgs else None
                                _l_claude = _c_msgs[-1] if _c_msgs else None
                                _ls = []
                                if _f_user:
                                    _ls.append("U1: " + _trim_p(_f_user.get("text") or _f_user.get("content")))
                                if _l_user and _l_user is not _f_user:
                                    _ls.append("Un: " + _trim_p(_l_user.get("text") or _l_user.get("content")))
                                if _l_claude:
                                    _ls.append("C: " + _trim_p(_l_claude.get("text") or _l_claude.get("content")))
                                _par_stone["conversation_summary"] = "\n".join(_ls)
                                _par_stone["conversation_turn_count"] = len(_par_conv)
                            _server_log_action(proj_id, _par_id, "auto_confirm_after_review",
                                               f"review child {mid} done — mother auto-confirmed")
                # M54.7: extract structured Knowledge Object → .omc/facts.jsonl (ADR format)
                _extract_knowledge_object(proj_id, m)
            # Old-style done=False (from auto-ack) — treat as pending
            elif data.get("done") is False:
                if m.get("status") != "queued":
                    m["status"] = "pending"
                m["done"] = False
            updated = True
            updated_m = m  # M515: keep reference for delta response
            break
    if not updated:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    # M713: auto-create review sub-stone when a dev stone completes (layer 0 or 1 → child/grandchild)
    _m_layer = (updated_m.get("layer", 0) or 0) if updated_m else 99
    if new_status == "pending_confirmation" and updated_m and _m_layer <= 1:
        # M767: exec-session Claude creates [검수] inline (step 5b in dispatch prompt).
        # Server fallback: only call LLM classifier when no [검수] child was posted by exec agent.
        _has_review_child = any(
            isinstance(s, dict) and s.get("parent_id") == mid
            and str(s.get("text", "")).startswith("[검수]")
            for s in milestones
        )
        if _has_review_child:
            pass  # exec agent already created it — skip LLM call entirely
        _rev_txt = (updated_m.get("text") or "")
        _is_dev = False if _has_review_child else await _classify_dev_stone_llm(_rev_txt, updated_m)
        if _is_dev and not any(
            isinstance(s, dict) and s.get("parent_id") == mid
            and str(s.get("text", "")).startswith("[검수]")
            for s in milestones
        ):
            _rev_existing = {s.get("id", "") for s in milestones if isinstance(s, dict)}
            _rev_siblings = [s for s in milestones if isinstance(s, dict) and s.get("parent_id") == mid]
            _rev_id = f"{mid}.{len(_rev_siblings)+1}"
            while _rev_id in _rev_existing:
                _rev_id += "x"
            _brief = (updated_m.get("text") or "")[:50].strip()
            milestones.append({
                "id": _rev_id,
                "text": _build_review_stone_text(mid, _brief),
                "layer": _m_layer + 1, "parent_id": mid, "done": False,
                "status": "queued", "claude_ack": None,
                "user_added_at": _dt.datetime.now().strftime("%Y-%m-%dT%H:%M"),
                "skill_refs": ["code-review"],  # M996
            })
            _server_log_action(proj_id, _rev_id, "auto_review_created", f"review sub-stone for {mid}")
        # M749: e2e auto-creation removed — was creating pairs ([검수]+[e2e]) per completion
    proj["milestones"] = milestones
    # M1007 v2: server-side blink trigger — SERVER is the single source of truth for
    # blink state (client _blinkCenterSave is now read-only/no-op to end the clobber
    # race). Fire on ANY change the client count-delta would have detected: a status
    # transition OR a new conversation message (append_message). This covers the full
    # client hash (status|convLen|lastRole), so removing the client writer loses nothing.
    try:
        _blink_changed = (updated_m and updated_m.get("status") != _blink_old_status) \
                         or ("append_message" in data)
        if updated_m and _blink_changed:
            _blink_skey = updated_m.get("substar_id") or "__ungrouped__"
            _mark_blink_server(proj_id, _blink_skey, updated_m.get("id") or mid)
    except Exception:
        pass
    # M288: save synchronously when queuing so /execute sees the updated status immediately
    # (background save causes race: execute reads stale YAML where stone is still 'pending').
    _queuing_now = new_status == "queued" if new_status else False
    if _queuing_now:
        _save_project(proj_id, proj)
    else:
        # M698: single-stone update (~1ms) instead of full 764-stone rewrite (~112ms)
        if updated_m:
            _db_save_single_milestone(proj_id, updated_m)
        _parse_cache.pop(str(md), None)
        import copy as _c278
        background_tasks.add_task(_save_project, proj_id, _c278.deepcopy(proj))
    # M495: push SSE so open detail card reloads immediately (<100ms) without polling wait
    _ns_push("milestone_updated", proj_id=proj_id, mid=mid)
    # M535: log status change server-side
    if new_status:
        _server_log_action(proj_id, mid, f"status→{new_status}", f"prev:{updated_m.get('status','?') if updated_m else '?'}")
    # M515: return updated milestone so client can do cache-only re-render (skips 234KB full fetch)
    # M1047: include proof_warning in response if evidence_url missing on visual stone
    _resp: dict = {"ok": True, "milestone": updated_m}
    if updated_m and updated_m.get("_proof_warning"):
        _resp["proof_warning"] = updated_m.pop("_proof_warning")
    return JSONResponse(_resp)


@app.delete("/api/northstar/{proj_id}/milestones/{mid}")
async def delete_milestone(proj_id: str, mid: str, background_tasks: BackgroundTasks):
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    before = len(proj.get("milestones", []))
    # Find stone data before removing (for DB event log)
    deleted_stone = next((m for m in proj.get("milestones", []) if isinstance(m, dict) and m.get("id") == mid), None)
    # Remove milestone and all its children from YAML (table)
    proj["milestones"] = [m for m in proj.get("milestones", [])
                          if isinstance(m, dict) and m.get("id") != mid
                          and m.get("parent_id") != mid]
    removed = before - len(proj["milestones"])
    # M426: log stone_deleted event to stone_events for data history (milestones_store NOT deleted)
    if removed > 0:
        try:
            import json as _json_del
            from datetime import datetime as _dt_del
            _conn_del = sqlite3.connect(str(_NS_EVENTS_DB))
            _conn_del.execute(
                "INSERT INTO stone_events(proj_id, stone_id, event_type, payload, ts) VALUES(?,?,?,?,?)",
                (proj_id, mid, "stone_deleted",
                 _json_del.dumps({"text": (deleted_stone or {}).get("text", "")[:200],
                                  "status": (deleted_stone or {}).get("status", ""),
                                  "deleted_at": _dt_del.now().isoformat(timespec="seconds")}),
                 _dt_del.now().isoformat(timespec="seconds"))
            )
            _conn_del.commit()
            _conn_del.close()
        except Exception:
            pass
    # M318: write SQLite + invalidate cache synchronously before response
    _db_save_project(proj_id, proj)
    _parse_cache.pop(str(md), None)
    import copy as _c278d
    background_tasks.add_task(_save_project, proj_id, _c278d.deepcopy(proj))
    return JSONResponse({"ok": True, "removed": removed})






@app.post("/api/northstar/{proj_id}/milestones/{mid}/compress-conv")
async def compress_milestone_conv(proj_id: str, mid: str, request: Request):
    """M819: Compress conversation[] — keep last N turns + inject {role:'summary'} entry for older turns.
    Body: {keep_last: int (default 4)}. Updates milestone in SQLite, returns {ok, summary_text, kept, compressed}."""
    data = await request.json()
    keep_last = max(1, int(data.get("keep_last", 4)))
    try:
        import sqlite3 as _sq3
        conn = _sq3.connect(str(_NS_EVENTS_DB), timeout=5)
        row = conn.execute("SELECT data_json FROM milestones_store WHERE proj_id=? AND stone_id=?",
                           (proj_id, mid)).fetchone()
        if not row:
            conn.close()
            return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
        m = json.loads(row[0])
        conv = m.get("conversation") or []
        if len(conv) <= keep_last:
            conn.close()
            return JSONResponse({"ok": True, "summary_text": None, "kept": len(conv), "compressed": 0})
        old_turns = conv[:-keep_last]
        recent_turns = conv[-keep_last:]
        _trim = lambda t: (str(t or "").replace("\n", " ").strip())[:150]
        _u = [c for c in old_turns if c.get("role") == "user"]
        _c = [c for c in old_turns if c.get("role") == "claude"]
        lines = []
        if _u: lines.append("U: " + _trim(_u[0].get("text") or _u[0].get("content", "")))
        if len(_u) > 1: lines.append("Un: " + _trim(_u[-1].get("text") or _u[-1].get("content", "")))
        if _c: lines.append("C: " + _trim(_c[-1].get("text") or _c[-1].get("content", "")))
        summary_text = " | ".join(lines) if lines else f"({len(old_turns)} turns compressed)"
        summary_entry = {"role": "summary", "text": summary_text,
                         "ts": old_turns[-1].get("ts", ""), "compressed_count": len(old_turns)}
        m["conversation"] = [summary_entry] + recent_turns
        import datetime as _dt2
        conn.execute("UPDATE milestones_store SET data_json=?, updated_at=? WHERE proj_id=? AND stone_id=?",
                     (json.dumps(m, ensure_ascii=False), _dt2.datetime.now().isoformat(), proj_id, mid))
        conn.commit(); conn.close()
        return JSONResponse({"ok": True, "summary_text": summary_text, "kept": keep_last, "compressed": len(old_turns)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/northstar/{proj_id}/milestones/{mid}/rationale")
async def milestone_rationale(proj_id: str, mid: str):
    """M65/M70: Generate star-stone relation and OKR rationale for a milestone."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ms = next((m for m in proj.get("milestones", []) if isinstance(m, dict) and m.get("id") == mid), None)
    if not ms:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    ns_metric = proj.get("metric", "the north star goal")
    ns_current = str(proj.get("current", "") or "")
    ns_target  = str(proj.get("target", "") or "")
    ms_text    = str(ms.get("text", ""))
    gap_str = f" ({ns_current} → {ns_target})" if ns_current and ns_target else ""
    rationale = f"Closes the {ns_metric}{gap_str} gap by: {ms_text}"
    ms["star_relation"] = rationale
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "rationale": rationale})




@app.post("/api/northstar/{proj_id}/milestones/{mid}/commit")
async def commit_milestone(proj_id: str, mid: str):
    """M424: git commit in project dir with milestone ID and text as commit message."""
    proj_dir = _get_project_dir(proj_id)
    if not proj_dir:
        return JSONResponse({"ok": False, "error": "project dir not found"}, status_code=404)
    md_path = PROJECTS_DIR / proj_id / "north-star.md"
    proj = _parse_md_frontmatter(md_path) if md_path.exists() else {}
    milestones = proj.get("milestones", [])
    m = next((x for x in milestones if isinstance(x, dict) and x.get("id") == mid), None)
    if not m:
        # Try DB fallback
        m = _db_get_milestone(proj_id, mid) or {}
    if not m:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    text_short = str(m.get("text", "")).strip()[:72].replace('"', "'").replace('\n', ' ')
    msg = f"feat: {mid} {text_short}"
    try:
        # Check if git repo exists
        check = subprocess.run(["git", "rev-parse", "--git-dir"], cwd=proj_dir, capture_output=True)
        if check.returncode != 0:
            subprocess.run(["git", "init"], cwd=proj_dir, capture_output=True)
        r = subprocess.run(
            ["git", "add", "-A"],
            cwd=proj_dir, capture_output=True, text=True, timeout=15,
        )
        r2 = subprocess.run(
            ["git", "commit", "-m", msg, "--allow-empty"],
            cwd=proj_dir, capture_output=True, text=True, timeout=15,
        )
        if r2.returncode != 0 and "nothing to commit" not in r2.stdout + r2.stderr:
            return JSONResponse({"ok": False, "error": r2.stderr[:300]})
        sha = ""
        r3 = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=proj_dir, capture_output=True, text=True)
        if r3.returncode == 0:
            sha = r3.stdout.strip()
        return JSONResponse({"ok": True, "sha": sha, "message": msg})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/northstar/{proj_id}/milestones/{mid}/commits")
async def get_milestone_commits(proj_id: str, mid: str):
    """M444: Return commits linked to a milestone via milestone_commits table."""
    db_path = _NS_EVENTS_DB  # M712 fix: live ~/.hub/ns-events.db
    if not db_path.exists():
        return JSONResponse({"ok": True, "commits": []})
    import sqlite3 as _sqlite3
    try:
        db = _sqlite3.connect(str(db_path), timeout=5)
        rows = db.execute(
            "SELECT sha, subject, ts FROM milestone_commits WHERE proj_id=? AND mid=? ORDER BY ts DESC",
            (proj_id, mid)
        ).fetchall()
        db.close()
        return JSONResponse({"ok": True, "commits": [
            {"sha": r[0], "short_sha": r[0][:7], "subject": r[1], "ts": r[2]}
            for r in rows
        ]})
    except Exception as e:
        return JSONResponse({"ok": False, "commits": [], "error": str(e)})


@app.get("/api/northstar/{proj_id}/decisions")
async def get_project_decisions(proj_id: str):
    """M562: Return .omc/milestone-decisions.md content for CTX/G2 retrieval."""
    proj_dir = _get_project_dir(proj_id)
    if not proj_dir:
        return JSONResponse({"ok": False, "error": "project not found", "text": ""})
    dec_file = Path(proj_dir) / ".omc" / "milestone-decisions.md"
    if not dec_file.exists():
        return JSONResponse({"ok": True, "text": "", "entries": 0})
    text = dec_file.read_text(encoding="utf-8")
    entries = text.count("\n## M")
    return JSONResponse({"ok": True, "text": text, "entries": entries})


@app.get("/api/northstar/{proj_id}/tmux-output")
async def get_tmux_output(proj_id: str, lines: int = 20, tmux_session: str = ""):
    """Return latest output from a tmux session.
    M837: tmux_session query param targets branched session (claude-exec-{proj}-{suffix});
    when omitted, defaults to the project's main exec session."""
    session_name = (tmux_session or "").strip() or _live_exec_session_name(proj_id)
    # Safety: only allow sessions whose name starts with the project's exec prefix
    if not any(session_name.startswith(f"{a}-exec-{proj_id}") for a in _ALLOWED_AGENTS):
        return JSONResponse({"ok": False, "running": False, "output": "", "error": "invalid_session_name"}, status_code=400)
    # Check if session exists
    check = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
    if check.returncode != 0:
        return JSONResponse({"ok": False, "running": False, "output": ""})
    # Capture pane output
    result = subprocess.run(
        ["tmux", "capture-pane", "-p", "-t", session_name, "-S", f"-{lines}"],
        capture_output=True, text=True
    )
    output = result.stdout.strip()
    return JSONResponse({"ok": True, "running": True, "session": session_name, "output": output})




@app.get("/api/northstar/{proj_id}/task-board")
async def get_task_board(proj_id: str):
    """Return recent completion-log entries as job board (replaces old task-worker system)."""
    log_file = PROJECTS_DIR / proj_id / "completion-log.jsonl"
    jobs = []
    if log_file.exists():
        lines = [l.strip() for l in log_file.read_text().splitlines() if l.strip()]
        for line in reversed(lines[-10:]):  # last 10, newest first
            try:
                entry = json.loads(line)
                mid = entry.get("milestone_id", "?")
                jobs.append({
                    "task_id": f"{mid}-{entry.get('timestamp','')[:16]}",
                    "status": "done",
                    "output": entry.get("evidence", "")[:120],
                    "completed_at": entry.get("timestamp", ""),
                })
            except Exception:
                pass
    # Show queued milestones in task board when exec session is active.
    # M168: identify the actually-running milestone by parsing the tmux pane's TaskList block.
    # Claude Code prints status sigils ("✔ M164" done, "◼ M166" in-progress, "◻ M167" pending)
    # plus a "Working on M<id>" spinner line. We grep the most recent pane snapshot.
    # Fallback to the first-queued heuristic only when no live signal is parseable.
    session_name = _live_exec_session_name(proj_id)
    check = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
    if check.returncode == 0:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        if md.exists():
            proj = _parse_md_frontmatter(md)
            # M160: paused stones (awaiting user reply on Claude comment) excluded from running list
            # M216: held stones (user-paused via hold badge) also excluded
            queued_ms = [m for m in (proj.get("milestones") or [])
                         if m.get("status") == "queued" and not _awaits_user_reply(m) and not m.get("held")]

            # M168: parse pane to find truly-running milestone id
            live_running_id = None
            try:
                _pane = subprocess.run(
                    ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-40"],
                    capture_output=True, text=True, timeout=2,
                ).stdout
                # Priority 1: TaskList in-progress sigil ("◼ M123")
                _m = re.search(r"◼\s+(M\d+)", _pane)
                if _m:
                    live_running_id = _m.group(1)
                else:
                    # Priority 2: spinner "Working on M123"
                    _m = re.search(r"Working on (M\d+)", _pane)
                    if _m:
                        live_running_id = _m.group(1)
            except Exception:
                pass

            queued_ids = {m["id"] for m in queued_ms}
            for i, m in enumerate(queued_ms):
                # M168: only flag "running" when the pane confirms it; otherwise
                # all queued entries stay as "queued" (no false "running" on first).
                if live_running_id and m["id"] == live_running_id:
                    job_status = "running"
                else:
                    job_status = "queued"
                jobs.insert(i, {
                    "task_id": f"{m['id']}-{job_status}",
                    "status": job_status,
                    "output": m.get("text", "")[:80],
                    "completed_at": "",
                })

            # M168: if the running id isn't currently in the queue (e.g. it was just
            # promoted to pending_confirmation but pane still shows it), surface it
            # as a synthetic "running" entry so the task board doesn't drop the signal.
            if live_running_id and live_running_id not in queued_ids:
                live_ms = next((m for m in (proj.get("milestones") or [])
                                if m.get("id") == live_running_id), None)
                if live_ms:
                    jobs.insert(0, {
                        "task_id": f"{live_running_id}-running",
                        "status": "running",
                        "output": live_ms.get("text", "")[:80],
                        "completed_at": "",
                    })
    return JSONResponse({"ok": True, "jobs": jobs[:10]})


def _compute_dispatch_waves(stones: list) -> list:
    """M472: Topological sort of queued stones into parallel dispatch waves.

    Returns list of waves, each wave is a list of stones that can run concurrently.
    A stone must wait until its parent (parent_id) has been dispatched in a prior wave.
    """
    stone_ids = {s["id"] for s in stones}
    remaining = list(stones)
    waves = []
    max_iterations = len(stones) + 1  # guard against cycles
    while remaining and max_iterations > 0:
        max_iterations -= 1
        remaining_ids = {s["id"] for s in remaining}
        independent = [
            s for s in remaining
            if not s.get("parent_id") or s.get("parent_id") not in remaining_ids
        ]
        if not independent:
            independent = [remaining[0]]  # break cycle by forcing first
        waves.append(independent)
        done_ids = {s["id"] for s in independent}
        remaining = [s for s in remaining if s["id"] not in done_ids]
    return waves


def _stamp_wave_indices(proj_id: str, waves: list) -> None:
    """M511: Write wave_index back to each stone in SQLite + YAML so dispatch logs are labeled.

    Each stone receives wave_index = 0-based wave number it was dispatched in.
    Stones in the same wave can run concurrently; sequential dependency is wave N→N+1.
    This labeling enables the hub dataset to be used as parallel/serial causal AI training data.
    """
    if not waves:
        return
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        for wave_idx, wave_stones in enumerate(waves):
            for stone in wave_stones:
                mid = stone.get("id")
                if not mid:
                    continue
                row = conn.execute(
                    "SELECT data_json FROM milestones_store WHERE proj_id=? AND stone_id=?",
                    (proj_id, mid)
                ).fetchone()
                if not row:
                    continue
                try:
                    data = json.loads(row[0])
                except Exception:
                    continue
                data["wave_index"] = wave_idx
                conn.execute(
                    "UPDATE milestones_store SET data_json=? WHERE proj_id=? AND stone_id=?",
                    (json.dumps(data, ensure_ascii=False), proj_id, mid)
                )
        conn.commit()
        conn.close()
    except Exception:
        pass  # best-effort — don't block dispatch on write failure


def _format_dispatch_waves(waves: list) -> str:
    """M472: Format pre-computed dispatch waves for injection into EXECUTE SYNC prompt."""
    if not waves:
        return ""
    lines = ["DISPATCH WAVES (server pre-computed dependency order):"]
    for i, wave in enumerate(waves, 1):
        ids = ", ".join(m["id"] for m in wave)
        mode = "PARALLEL" if len(wave) > 1 else "single"
        lines.append(f"  Wave {i} [{mode}]: {ids}")
    lines.append("Dispatch Wave 1 first; each subsequent wave may start only after prior wave completes.")
    return "\n".join(lines) + "\n\n"


def _find_unassigned_substars(queued_stones: list, ns_list: list) -> tuple:
    """M792: return (unassigned_ids, unassigned_names) for substars that have queued stones
    but no assigned_session.  Applies the same exclusion criteria as new_queued_top:
    held=False, not awaiting user reply, non-empty text.
    """
    ns_by_id: dict = {ns["id"]: ns for ns in ns_list if ns.get("id")}
    unassigned_ids: list = []
    unassigned_names: list = []
    seen: set = set()
    for m in queued_stones:
        # Apply same filters as new_queued_top
        if m.get("held"):
            continue
        if _awaits_user_reply(m):
            continue
        if not str(m.get("text", "")).strip():
            continue
        sid = m.get("substar_id", "") or ""
        if sid and sid not in seen:
            ns_entry = ns_by_id.get(sid) or {}
            assigned = (ns_entry.get("assigned_session") or "").strip()
            if not assigned:
                seen.add(sid)
                unassigned_ids.append(sid)
                unassigned_names.append(ns_entry.get("name") or sid[-8:])
    return unassigned_ids, unassigned_names


def _awaits_user_reply(m: dict) -> bool:
    """M160: True when last conversation entry is from claude — user must reply before this stone may run.

    Stones that have an open Claude comment awaiting user feedback are paused
    regardless of status=queued — preventing Claude from autonomously executing
    on top of an unanswered comment.
    """
    if not isinstance(m, dict):
        return False
    conv = m.get("conversation") or []
    if not conv or not isinstance(conv, list):
        return False
    last = conv[-1]
    return isinstance(last, dict) and last.get("role") == "claude"


@app.post("/api/northstar/{proj_id}/execute")
async def execute_project(proj_id: str, request: Request):
    """Smart dispatcher: if no milestones → init roadmap; if milestones exist → process queued work.
    Writes to session-inbox.jsonl for pickup by UserPromptSubmit hook.
    M[new]: Accepts optional session_id in body for explicit session selection."""
    import json as _json_exec
    data = {}
    raw_body = b""
    try:
        raw_body = await request.body()
    except Exception:
        raw_body = b""
    if raw_body:
        try:
            parsed = _json_exec.loads(raw_body.decode("utf-8"))
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}
    explicit_session_id = (request.query_params.get("session_id") or data.get("session_id") or "").strip() or None
    body_agent = (data.get("agent") or "").strip().lower() or None
    body_model = data.get("model")  # None = not sent; "" = reset to default; "or-owl-alpha" = set model
    if body_model is not None:
        body_model = str(body_model).strip()
    # M360: auto-correct agent when model prefix implies a different agent
    # Prevents claude-exec sessions with or-* models, codex-exec with claude models, etc.
    if body_model and body_model.startswith("or-") and body_agent != "openrouter":
        body_agent = "openrouter"
    elif body_model and body_model.startswith("codex-") and body_agent != "codex":
        body_agent = "codex"

    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)

    # Apply body_agent and body_model to north-star.md before spawning so spawn env is correct
    _persist_changes = {}
    if body_agent and body_agent in _ALLOWED_AGENTS:
        _persist_changes["agent"] = body_agent
    if body_model is not None:  # None = not provided; "" = reset to default
        _model_val = body_model if body_model in _ALLOWED_MODELS else ""
        _persist_changes["model"] = _model_val
    if _persist_changes:
        # M570: SQLite-first via _save_project. Replaces _write_md_frontmatter (YAML-only)
        # so reads via _parse_md_frontmatter pick up the new agent/model immediately.
        _save_project(proj_id, {**proj, **_persist_changes})
        # M302: update ONLY project_meta in SQLite (agent/model fields) so _get_project_model_value
        # reads fresh value. Do NOT save milestones — would race with background add/delete saves.
        try:
            import datetime as _dt302
            _meta302 = {k: v for k, v in {**proj, **_persist_changes}.items()
                        if k not in ("milestones", "_body")}
            conn302 = sqlite3.connect(str(_NS_EVENTS_DB))
            conn302.execute("INSERT OR REPLACE INTO project_meta(proj_id, meta_json, updated_at) VALUES(?,?,?)",
                            (proj_id, json.dumps(_meta302, ensure_ascii=False),
                             _dt302.datetime.utcnow().isoformat()))
            conn302.commit()
            conn302.close()
        except Exception:
            pass
        _parse_cache.pop(str(md), None)  # invalidate mtime cache to force SQLite L2 read
        proj = _parse_md_frontmatter(md)

    # M215: use fast SQLite indexed query for active milestones — ~1ms vs full proj parse
    _fast_ms = _db_get_active_milestones(proj_id)
    if _fast_ms is not None:
        active_ms = _fast_ms  # already filtered to done=0
        milestones = active_ms  # execute only cares about active stones
    else:
        milestones = proj.get("milestones", [])
        active_ms = [m for m in milestones if not m.get("done") and m.get("status") != "done"]

    from datetime import datetime as _dt_
    _host = os.environ.get('HUB_HOST', '0.0.0.0')
    if _host == '0.0.0.0':
        import re as _re_
        try:
            _r = __import__('subprocess').run(['ss','-tlnp'], capture_output=True, text=True, timeout=2)
            _m = _re_.search(r'(100\.\d+\.\d+\.\d+):9000', _r.stdout)
            _host = _m.group(1) if _m else '127.0.0.1'
        except Exception:
            _host = '127.0.0.1'
    hub_api = f"http://{_tailscale_interface_ip()}:{PORT}"

    task_queue = HERE / "task-queue"
    task_queue.mkdir(parents=True, exist_ok=True)
    ts = _dt_.now().strftime("%Y%m%d-%H%M%S")

    if not active_ms:
        # INIT MODE: dispatch a single task to initialize the milestone roadmap
        task_file = task_queue / f"task-{proj_id}-init-{ts}.md"
        task_file.write_text(
            f"# Execute Init: {proj_id}\n\n"
            f"Project '{proj.get('name', proj_id)}' has no milestones.\n"
            f"Use /ns-stone or analyze the project context and create an initial milestone roadmap.\n"
            f"After creating milestones, PATCH them via {hub_api}/api/northstar/{proj_id}/milestones\n"
        )
        mode = "init"
        return JSONResponse({"ok": True, "mode": mode, "tasks_created": 1,
                             "message": f"Init task queued for {proj_id}"})
    else:
        # TMUX SESSION MODE: spawn one persistent claude session, inject cron-creation prompt
        # CronCreate in this session = auto-retry on API errors (built-in resilience)

        # M504-fix: removed execute-time auto-ack for review stones.
        # Race condition: PATCH uses background write; execute read SQLite before write committed →
        # stale read showed just-queued stone as pending → acked it → full-project save clobbered
        # the queued status back to pending+acked. 5-min background poller (_auto_ack_poller) already
        # handles review-stone acking without this race. Removed _promoted block entirely.
        from datetime import datetime as _dt_exec
        _now_iso = _dt_exec.now().strftime("%Y-%m-%dT%H:%M")

        # M160: skip stones with a pending Claude comment awaiting user reply
        # M225: held stones are completely excluded from SessionStart actionable surfaces too.
        # M291: empty-text stones are excluded — no actionable work, prevents token waste.
        actionable_all = [m for m in active_ms if m.get("status") in ("queued", "pending", "needs_clarification") and not m.get("held") and str(m.get("text","")).strip()]
        paused_awaiting_user = [m for m in actionable_all if _awaits_user_reply(m)]
        actionable = [m for m in actionable_all if not _awaits_user_reply(m)][:5]
        # M250: compute new_queued at top level so spawn gate can use it.
        # Only stones the user explicitly queued trigger a Claude spawn — pending/needs_clarification
        # stones do NOT warrant spawning (server already acked them above).
        # M291: empty-text queued stones are excluded from dispatch — no content to act on.
        new_queued_top = [m for m in active_ms if m.get("status") == "queued" and not _awaits_user_reply(m) and not m.get("held") and str(m.get("text","")).strip()]

        # M836.2: auto-assign main session to unassigned substars instead of 409 blocking.
        # Was M792 early 409; user requested fallback behavior: when a substar has no assigned_session,
        # default it to the project main session name so dispatch proceeds without modal interruption.
        _ns_list_early = proj.get("north_stars") or []
        _unassigned_ids_early, _unassigned_names_early = _find_unassigned_substars(new_queued_top, _ns_list_early)
        if _unassigned_ids_early:
            _early_agent = body_agent if body_agent in _ALLOWED_AGENTS else _get_project_agent_value(proj_id)
            _main_sess_name = f"{_early_agent}-exec-{proj_id}"
            for _ns in _ns_list_early:
                if _ns.get("id") in _unassigned_ids_early:
                    _ns["assigned_session"] = _main_sess_name
            proj["north_stars"] = _ns_list_early
            try:
                _save_project(proj_id, proj)
            except Exception:
                pass  # in-memory mutation already applied; persist best-effort
            _server_log_action(proj_id, "", "exec:auto_assign_main",
                               f"auto-assigned main session to {len(_unassigned_ids_early)} unassigned substar(s): {','.join(_unassigned_names_early)}")

        agent = body_agent if body_agent in _ALLOWED_AGENTS else _get_project_agent_value(proj_id)
        session_name = f"{agent}-exec-{proj_id}"
        # M524.3: graceful Windows check — tmux not available on Windows native
        if not _HAS_PTY and sys.platform == "win32":
            return JSONResponse({
                "ok": False,
                "error": "tmux_unavailable_windows",
                "message": "Execute dispatch requires tmux, which is not available on Windows native. "
                           "Run hub inside WSL2 or Docker for full exec session support.",
            }, status_code=501)
        # M359: before checking the new session, check if ANY exec session exists with a DIFFERENT
        # agent prefix and kill it when options changed — prevents old sessions surviving agent switch
        # M858: also scan for orphaned branched sessions (e.g. claude-exec-MOAT-79376871) even when
        # the main session (claude-exec-MOAT) is already dead. Branched sessions are subordinate to
        # their mother; a different-agent dispatch kills ALL old-agent sessions including branches.
        _all_exec_prefixes = [("claude-exec-", "claude"), ("openrouter-exec-", "openrouter"), ("codex-exec-", "codex")]
        try:
            _live_sessions = subprocess.run(
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=2,
            ).stdout.splitlines()
        except Exception:
            _live_sessions = []
        _stale_agent_found = False
        for _pfx, _pfx_agent in _all_exec_prefixes:
            if _pfx_agent != agent:
                # Check base session OR any branched session of this old agent
                _old_base = f"{_pfx}{proj_id}"
                _old_branches = [s for s in _live_sessions if s == _old_base or s.startswith(f"{_old_base}-")]
                if _old_branches:
                    _stale_agent_found = True
                    break
        if _stale_agent_found:
            _kill_all_exec_sessions(proj_id)  # now includes branches via tmux scan (M858)
        proj_dir = _get_project_dir(proj_id) or str(Path.home() / "Project" / proj_id)

        # M24/M123/M125: if tmux session exists, verify Claude is alive before injecting
        existing = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
        if existing.returncode == 0:
            # M123: check Claude process is alive (not just a bare shell after Claude exited)
            _pane_cmds = subprocess.run(
                ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                capture_output=True, text=True
            ).stdout.splitlines()
            _cmds = [c.strip() for c in _pane_cmds if c.strip()]
            _claude_alive = any(c not in _SHELLS for c in _cmds) if _cmds else False
            if _claude_alive:
                if agent == "codex":
                    # Codex does not use the Claude Stop-hook queue path. Restart the
                    # live tmux session so the execute prompt can be re-injected cleanly.
                    subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)
                else:
                    # M206: kill and restart if user selected different options (agent/model/session)
                    # vs what the current session was spawned with. If same → reuse.
                    _options_changed = False
                    _spawn_info_file = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
                    if _spawn_info_file.exists():
                        try:
                            _si = json.loads(_spawn_info_file.read_text())
                            _cur_agent = _si.get("agent", "claude")
                            _cur_model = _si.get("model", "")
                            _cur_sid   = _si.get("from_id") or ""
                            _new_agent = agent
                            _new_model = _get_project_model_value(proj_id) or ""
                            _new_sid   = explicit_session_id or ""
                            _options_changed = (
                                _cur_agent != _new_agent or
                                _cur_model != _new_model or
                                (explicit_session_id and _cur_sid != _new_sid)
                            )
                        except Exception:
                            pass
                    # Explicit session choice must be honored. Do not inject into the
                    # currently running tmux session when the user selected Fresh or a
                    # specific prior session from the UI; restart below with that choice.
                    if explicit_session_id or _options_changed:
                        # M206: kill ALL agent-prefixed sessions to prevent duplicate panes
                        _kill_all_exec_sessions(proj_id)
                    else:
                        # Session alive with Claude running — inject trigger if milestones need attention
                        # M160: skip stones whose last conversation entry is from claude (awaiting user reply)
                        # M216: held stones (user-paused) are excluded from EXECUTE SYNC entirely.
                        # M210: no-badge+unqueued pending stones are excluded from dispatch
                        # (like held stones). Only queued stones are work targets per M199.
                        new_pending = []  # M210: removed — pending stones without explicit queue press are not targets
                        # M858: include substar-assigned queued stones even when awaiting user reply
                        # but ONLY when assigned to a non-main session (branched).
                        # Stones assigned to main session with user-reply stay excluded to avoid
                        # infinite queue-write loops (they are handled inline, not via queue).
                        _ns_list_pre = proj.get("north_stars") or []
                        _main_assign = {ns["id"] for ns in _ns_list_pre
                                        if (ns.get("assigned_session") or "").strip() == session_name}
                        def _is_branched_substar(m):
                            _sid = m.get("substar_id") or ""
                            return bool(_sid) and _sid not in _main_assign
                        new_queued  = [m for m in active_ms if m.get("status") == "queued"
                                       and (not _awaits_user_reply(m) or _is_branched_substar(m))
                                       and not m.get("held")]
                        needs_trigger = new_queued  # M210: only queued stones trigger dispatch
                        # M837 fix Stage 1: split new_queued by owner session so the main queue inject only
                        # carries main-owned stones, and branched-owned stones wake their own session.
                        # Was: ALL queued stones went into main's `_ms_snap` + main wake → main grabbed branched work.
                        _ns_list_inj = proj.get("north_stars") or []
                        _ns_by_id_inj = {ns["id"]: ns for ns in _ns_list_inj if ns.get("id")}
                        def _owner_session(m):
                            _sid = m.get("substar_id") or ""
                            if not _sid:
                                return session_name  # ungrouped → main
                            _ak = ((_ns_by_id_inj.get(_sid) or {}).get("assigned_session") or "").strip()
                            return _ak or session_name
                        _main_owned_q = [m for m in new_queued if _owner_session(m) == session_name]
                        _branched_q_by_sess: dict = {}
                        for _m in new_queued:
                            _ok = _owner_session(_m)
                            if _ok != session_name:
                                _branched_q_by_sess.setdefault(_ok, []).append(_m)
                        _trigger_sent = False
                        if needs_trigger:
                            # M149: append-only JSONL queue — never overwrite. Each Execute click
                            # appends one entry. Hooks track byte offset to consume only new entries.
                            _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
                            _qf.parent.mkdir(parents=True, exist_ok=True)
                            # M473 follow-up: the preview was [:60] which silently dropped
                                # user content. Now send the FULL stone text (newline-escaped) up
                                # to a 1500-char safety cap; on overflow we append an explicit
                                # truncation marker pointing to GET /milestones/<MID>. This makes
                                # the preview accurate-by-default and keeps the queue file
                                # bounded for pathological huge stones.
                            def _ms_snap_line(m):
                                _t = (m.get("text") or "").replace("\n", " / ")
                                _cap = 1500
                                if len(_t) > _cap:
                                    _t = _t[:_cap] + f" ... [+{len(_t)-_cap} chars — call GET /milestones/{m.get('id')} for full body]"
                                base = f"  {m.get('id')} [{m.get('status')}]: \"{_t}\""
                                # M727: if last conversation entry is from user, surface it as REPLY target
                                _conv = m.get("conversation") or []
                                if _conv and _conv[-1].get("role") == "user":
                                    _last_user_msg = (_conv[-1].get("text") or _conv[-1].get("content") or "")[:200].replace("\n", " / ")
                                    base += f"\n    ⚠️ REPLY MODE (M687): last msg is user → ANSWER ONLY: \"{_last_user_msg}\""
                                # M138/M258: inject skill_refs[] (multi) or fallback to skill_ref (single)
                                _srefs = m.get("skill_refs") or ([m["skill_ref"]] if m.get("skill_ref") else [])
                                _arefs = m.get("agent_refs") or ([m["agent_ref"]] if m.get("agent_ref") else [])
                                for _sr in _srefs: base += f"  [skill: /{_sr}]"
                                for _ar in _arefs: base += f"  [agent: {_ar}]"
                                return base
                            # M837 fix Stage 1: main inject sees only main-owned queued + pending; branched-owned go to their own wake
                            _ms_snap = "\n".join(_ms_snap_line(m) for m in (_main_owned_q + new_pending))
                            _waves = _compute_dispatch_waves(_main_owned_q) if _main_owned_q else []
                            _stamp_wave_indices(proj_id, _waves)  # M511: label wave_index on each stone
                            _waves_section = _format_dispatch_waves(_waves) if len(_main_owned_q) > 1 else ""
                            _mem_section = _load_stone_memory(proj_id)
                            _entry = json.dumps({
                                "ts": _dt_exec.now().isoformat(),
                                "body": (
                                    f"[EXECUTE SYNC] New milestones need processing — process ALL queued milestones now.\n\n"
                                    + _mem_section
                                    + f"MANDATORY FIRST STEP — fetch the FULL stone body via\n"
                                    f"  GET {hub_api}/api/northstar/{proj_id}/milestones\n"
                                    f"and read the target stone's full `text` and `conversation[]`.\n"
                                    f"The previews under 'Newly queued:' below are routing hints, NOT\n"
                                    f"the source of truth — they are 1500-char capped and may carry a\n"
                                    f"truncation marker. Acting on the preview alone risks dropping\n"
                                    f"pasted content, image/file refs, or conversation context.\n\n"
                                    f"⚠️  ANSWER vs IMPLEMENT CHECK (M687) — for EACH stone, inspect conversation[]:\n"
                                    f"  • If conversation[-1].role == 'user'  → the user asked a FOLLOW-UP QUESTION.\n"
                                    f"    ANSWER the question only. Do NOT re-implement the task.\n"
                                    f"    PATCH with append_message{{role:'claude', text:'<answer ≤3 lines>'}} only.\n"
                                    f"    Do NOT change status (stone stays queued/pending_confirmation).\n"
                                    f"  • If conversation is empty OR conversation[-1].role == 'claude' → normal task.\n"
                                    f"    Implement and PATCH status=pending_confirmation + append_message.\n"
                                    f"  This check prevents the repeat-completion bug (M687).\n\n"
                                    f"Then TaskCreate per queued milestone, then implement using DISPATCH WAVES below.\n"
                                    f"Wave [PARALLEL] = emit ALL stones in that wave as multiple Agent calls in ONE message.\n"
                                    f"Wave [single] = implement sequentially after prior wave completes.\n\n"
                                    f"COMMENT RULE (M478) — every append_message {{role:'claude'}} posted to a stone\n"
                                    f"  MUST be ≤ 3 lines. One-line past-tense outcome summary is the target;\n"
                                    f"  3 lines is the hard cap enforced server-side (M185: lines beyond 3 are\n"
                                    f"  auto-truncated and a [details: docs/ns-replies/...] pointer is appended).\n"
                                    f"  If full detail is needed: write docs/ns-replies/<DATE>-<MID>.md first,\n"
                                    f"  then reference it in the ≤3-line comment.\n\n"
                                    f"PARALLEL DISPATCH PROTOCOL (M472, Option 2 — native sub-agents) —\n"
                                    f"  IF 2+ queued stones are INDEPENDENT (no parent-child relation in their\n"
                                    f"  parent_id field, no obvious shared-file/shared-resource collision in\n"
                                    f"  their text), dispatch them IN PARALLEL: emit multiple Agent / Task tool\n"
                                    f"  calls in a SINGLE message. Sub-agents share THIS session's token quota\n"
                                    f"  (cheapest path) and each one:\n"
                                    f"    1) reads its own stone via GET /milestones,\n"
                                    f"    2) implements (Edit/Bash/Skill/Agent as needed),\n"
                                    f"    3) PATCHes status=pending_confirmation + append_message + completion\n"
                                    f"       fields (star_relation, model_used, exec_start, exec_end),\n"
                                    f"    4) appends one line to {{proj}}/completion-log.jsonl.\n"
                                    f"  After all parallel sub-agents return, you (the orchestrator) post ONE\n"
                                    f"  consolidated 1-line summary listing the completed MIDs.\n"
                                    f"  FALL BACK TO SEQUENTIAL when: (a) a substone whose mother is also queued\n"
                                    f"  (mother must wait — M468 commit-gate), (b) UI work needing the same\n"
                                    f"  browser session, (c) stones whose text references the same target file\n"
                                    f"  or shared mutable resource.\n\n"
                                    f"COMPLETION PROTOCOL — when patching status=pending_confirmation, ALWAYS include ALL of:\n"
                                    f"  append_message: {{\"role\":\"claude\",\"text\":\"<1-line PAST TENSE completion summary>\"}} — MANDATORY.\n"
                                    f"    Omitting append_message leaves the stone with NO user-visible reply (server inserts generic fallback).\n"
                                    f"    Must be in the SAME PATCH as status=pending_confirmation. Past tense, ≤1 line.\n"
                                    f"  star_relation: <1 English line stating HOW this completion closed the star gap>\n"
                                    f"  (be concrete: which metric moved, by what mechanism. Mandatory.)\n"
                                    f"  model_used: <model name used for this stone, e.g. claude-sonnet-4-6>\n"
                                    f"  total_tokens: <approximate tokens used for this stone if known, else omit>\n"
                                    f"  exec_start: <ISO timestamp when you started working on this stone>\n"
                                    f"  exec_end: <ISO timestamp now, when completing>\n\n"
                                    f"PROVE SHOT PROTOCOL (M332) — for UI/visual work, include a screenshot/video\n"
                                    f"  link in append_message (e.g. GDrive link). Take a Playwright screenshot,\n"
                                    f"  upload via rclone to gdrive:claude-shared/Moat/outbox/, share link.\n"
                                    f"  Non-visual work (logic fixes, CSS): describe before/after instead.\n\n"
                                    f"NO-OP PROTOCOL (Rule 6 of ns-comment-reply-protocol.md) — if you\n"
                                    f"  decide NOT to act on a stone (already done, no actionable work, blocked\n"
                                    f"  on user, ambiguous), POST a 1-line append_message {{role:'claude'}} on\n"
                                    f"  that stone stating the reason. Silent skip is forbidden.\n\n"
                                    f"TOKEN DISCIPLINE (M270) — after completing a stone, post ONLY the 1-line\n"
                                    f"  past-tense completion summary. Do NOT add follow-up questions, suggestions,\n"
                                    f"  or clarifications unless the user has asked something specific.\n"
                                    f"  Extra comments = token waste. One line. Done.\n\n"
                                    f"LANGUAGE RULE (M693) — append_message text must match the stone's language.\n"
                                    f"  Korean stone text → Korean comment. English stone text → English comment.\n"
                                    f"  Mixed: use the dominant language (usually Korean for this project).\n\n"
                                    + _waves_section
                                    + f"Newly queued:\n{_ms_snap}"
                                ),
                            }, ensure_ascii=False)
                            # M837 fix Stage 1: only write main queue entry + wake main when main owns at least one queued stone
                            _wake_sent = False
                            # M837 fix Stage 1: hoist _modal_signatures so both main + branched wake paths share it
                            _modal_signatures = ("extra usage", "Switch to Team plan",
                                                 "Stop and wait", "rate-limit-options",
                                                 "Press Enter to", "Continue?", "[Y/n]")
                            if _main_owned_q or new_pending:
                                with _qf.open("a", encoding="utf-8") as _qh:
                                    _qh.write(_entry + "\n")
                                # M147/M148: wake idle main session so stop hook can fire.
                                try:
                                    _pane = subprocess.run(
                                        ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-8"],
                                        capture_output=True, text=True, timeout=2,
                                    ).stdout
                                    _busy = "esc to i" in _pane or "… (" in _pane
                                    _modal = any(s in _pane for s in _modal_signatures)
                                    _has_prompt = "❯" in _pane
                                    _actively_blocked = "esc to i" in _pane or _modal
                                    if _has_prompt and not _actively_blocked:
                                        subprocess.run(
                                            ["tmux", "send-keys", "-t", session_name, "go", "Enter"],
                                            capture_output=True, timeout=2,
                                        )
                                        _wake_sent = True
                                        _ns_push("session_running", proj_id=proj_id, kind="exec")
                                except Exception:
                                    pass
                            _trigger_sent = True
                            # M837 fix Stage 1: wake each branched session that owns queued stones.
                            # M858 Stage 2: dead branched sessions → spawn them.
                            _branched_wakes_sent: list = []
                            _branched_spawned: list = []
                            _br_spawn_cwd = proj_dir if Path(proj_dir).exists() else str(Path.home())
                            _br_agent = agent
                            _br_model = _get_project_model_value(proj_id)
                            _br_encoded_cwd = _encode_cwd_for_claude(_br_spawn_cwd)
                            _br_transcripts = Path.home() / ".claude" / "projects" / _br_encoded_cwd
                            _br_all_ms = "\n".join(
                                f"  {m.get('id')} [{m.get('status')}]: \"{(m.get('text') or '')[:80].replace(chr(10),' / ')}\""
                                for m in active_ms
                            )
                            for _br_sess, _br_stones in _branched_q_by_sess.items():
                                try:
                                    _br_check = subprocess.run(["tmux", "has-session", "-t", _br_sess], capture_output=True)
                                    if _br_check.returncode != 0:
                                        # M858 Stage 2: session dead → write prompt and spawn
                                        _br_short = _br_sess[-12:]
                                        _br_ns_ids = list(dict.fromkeys(m.get("substar_id","") for m in _br_stones if m.get("substar_id")))
                                        _br_ns_names = [(_ns_by_id_inj.get(sid) or {}).get("name") or sid[-8:] for sid in _br_ns_ids]
                                        _br_snap = "\n".join(
                                            f"  {m.get('id')} [queued]: \"{(m.get('text') or '').replace(chr(10),' / ')[:120]}\""
                                            for m in _br_stones
                                        )
                                        _br_pf = PROJECTS_DIR / proj_id / f"pending-execute-prompt-{_br_short}.txt"
                                        _br_pf.write_text(
                                            _load_stone_memory(proj_id)
                                            + f"[EXECUTE SYNC] Project {proj_id} — Session '{_br_sess}' "
                                            f"(substars: {', '.join(_br_ns_names)}).\n"
                                            f"PRIMARY GOAL: implement ALL queued stones for this session.\n\n"
                                            f"Queued stones:\n{_br_snap}\n\n"
                                            f"Active milestones:\n{_br_all_ms}",
                                            encoding="utf-8",
                                        )
                                        _br_env = [
                                            "-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}-{_br_short}",
                                            "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                                            "-e", f"NS_SESSION_KEY={_br_sess}",
                                        ]
                                        for _k, _v in _get_project_spawn_env(proj_id).items():
                                            _br_env += ["-e", f"{_k}={_v}"]
                                        # M837: use --continue instead of --resume to avoid thinking.signature API 400 errors
                                        _br_claude_cmd = ["claude", "--dangerously-skip-permissions", "--continue"] + _get_project_model(proj_id)
                                        subprocess.Popen(
                                            ["tmux", "new-session", "-d", "-s", _br_sess, "-c", _br_spawn_cwd]
                                            + _br_env + _br_claude_cmd
                                        )
                                        subprocess.run(["tmux", "send-keys", "-t", _br_sess, "go", "Enter"], capture_output=True, timeout=2)
                                        _branched_spawned.append(_br_sess)
                                        _server_log_action(proj_id, "", "exec:branch_spawned", _br_sess)
                                        continue
                                    _br_pane = subprocess.run(
                                        ["tmux", "capture-pane", "-p", "-t", _br_sess, "-S", "-8"],
                                        capture_output=True, text=True, timeout=2,
                                    ).stdout
                                    _br_blocked = ("esc to i" in _br_pane) or any(s in _br_pane for s in _modal_signatures)
                                    _br_has_prompt = "❯" in _br_pane
                                    if _br_has_prompt and not _br_blocked:
                                        subprocess.run(
                                            ["tmux", "send-keys", "-t", _br_sess, "go", "Enter"],
                                            capture_output=True, timeout=2,
                                        )
                                        _branched_wakes_sent.append(_br_sess)
                                except Exception:
                                    pass
                            _server_log_action(proj_id, "", "exec:branched_wake",
                                               f"split: main={len(_main_owned_q)} branched={sum(len(v) for v in _branched_q_by_sess.values())} woke={','.join(_branched_wakes_sent) or 'none'} spawned={','.join(_branched_spawned) or 'none'}")
                        return JSONResponse({
                            "ok": True, "mode": "tmux_active",
                            "session": session_name,
                            "tasks_created": len(actionable),
                            "new_injected": len(needs_trigger) if needs_trigger else 0,
                            "triggered": _trigger_sent if needs_trigger else False,
                            "wake_sent": _wake_sent if needs_trigger else False,
                            "message": ("Session idle — woke with 'go'; queued task injected" if (needs_trigger and _wake_sent) else
                                        "Session active — Stop hook will pick up on next idle" if needs_trigger else
                                        "Session active — no new work"),
                        })
            else:
                # M123: Claude exited — stale shell session. Kill so fresh start below can proceed.
                subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)

        # M250: early return if no explicitly-queued stones — skip spawning Claude entirely.
        # pending/needs_clarification stones don't need a Claude session; server already acked them.
        # M858: substar-assigned queued stones with pending user replies are excluded from new_queued_top
        # (_awaits_user_reply=True) but their branched sessions must still be spawned so Claude can
        # reply AND continue implementation. Include them in the gate check.
        _substar_queued_top = [
            m for m in active_ms
            if m.get("status") == "queued" and m.get("substar_id")
            and not m.get("held") and str(m.get("text", "")).strip()
        ]
        if not new_queued_top and not _substar_queued_top:
            return JSONResponse({
                "ok": True, "mode": "no_queued_work",
                "tasks_created": 0,
                "message": f"No queued milestones — {len(actionable)} stone(s) are pending/needs_clarification but not yet queued by user. Nothing to dispatch.",
                "pending_count": len([m for m in active_ms if m.get("status") == "pending"]),
                "needs_clarification_count": len([m for m in active_ms if m.get("status") == "needs_clarification"]),
            })

        if actionable:
            if agent == "codex":
                # M473 follow-up: use _snap_text (1500-char cap) instead of [:70]
                all_ms_lines = "\n".join(
                    f"  {m.get('id')} [{m.get('status')}]: \"{_snap_text(m)}\""
                    for m in active_ms
                )
                codex_prompt = (
                    f"[EXECUTE SYNC] Project {proj_id} — Execute clicked. PRIMARY GOAL: track the queued milestones and implement them sequentially.\n\n"
                    f"Use `update_plan` to track one item per queued milestone.\n"
                    f"Queued milestones:\n{all_ms_lines}\n\n"
                    f"For each queued milestone:\n"
                    f"  1. Mark the item in progress in your plan.\n"
                    f"  2. Edit/write files to implement the milestone.\n"
                    f"  3. Append completion-log:\n"
                    f'     echo \'{{\"session_id\":\"exec\",\"milestone_id\":\"<MID>\",\"evidence\":\"<one-line summary>\",\"timestamp\":\"\'$(date -Iseconds)\'\"}}\' >> ~/.hub/projects/{proj_id}/completion-log.jsonl\n'
                    f"  4. PATCH {hub_api}/api/northstar/{proj_id}/milestones/<MID> body {{\"status\":\"pending_confirmation\", \"star_relation\":\"<1-line gap closure>\", \"model_used\":\"<model name>\", \"session_id\":\"$CLAUDE_CODE_SESSION_ID\", \"exec_start\":\"<ISO start>\", \"exec_end\":\"<ISO now>\", \"append_message\":{{\"role\":\"claude\",\"text\":\"<1-line PAST TENSE: what was completed and its outcome — NOT what to do>\"}}}}\n"
                    f"  5. Mark the item completed in your plan.\n\n"
                    f"If a milestone is ambiguous, mark it `needs_clarification` and ask for the missing detail.\n"
                )
                agents_path = Path(proj_dir) / "AGENTS.md"
                agents_backup = agents_path.read_text(encoding="utf-8") if agents_path.exists() else None
                agents_path.parent.mkdir(parents=True, exist_ok=True)
                agents_path.write_text(
                    (agents_backup + "\n\n" if agents_backup else "") +
                    "# North Star execute context\n\n" + codex_prompt,
                    encoding="utf-8",
                )
                spawn_cwd = proj_dir if Path(proj_dir).exists() else str(Path.home())
                resume_args = _get_resume_args(proj_id, spawn_cwd, explicit_session_id, agent=agent)
                _record_spawn_info(proj_id, resume_args, agent=agent)
                _tmux_env = ["-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}"]
                for _k, _v in _get_project_spawn_env(proj_id).items():
                    _tmux_env += ["-e", f"{_k}={_v}"]
                subprocess.Popen([
                    "tmux", "new-session", "-d", "-s", session_name,
                    "-c", spawn_cwd,
                    *_tmux_env,
                    *_get_agent_spawn_cmd(proj_id),
                    *resume_args,
                ])
                _exec_idle_file(proj_id).unlink(missing_ok=True)  # M536: clear idle flag on spawn
                _server_log_action(proj_id, "", "exec:spawn", f"session:{session_name} resume:{resume_args[-1] if resume_args else 'fresh'}")
                # M366: immediately push running status so ns-card shows green border without waiting for poller
                _ns_push("session_running", proj_id=proj_id, kind="exec")
                import asyncio as _aio
                deadline = 12
                elapsed = 0
                while elapsed < deadline:
                    await _aio.sleep(1)
                    elapsed += 1
                    _post_panes = subprocess.run(
                        ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                        capture_output=True, text=True
                    ).stdout.split()
                    _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)
                    if _post_alive:
                        break
                if not _post_alive and resume_args:
                    subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)
                    try:
                        (PROJECTS_DIR / proj_id / ".last-session-id").unlink()
                    except Exception:
                        pass
                    resume_args = []
                    _record_spawn_info(proj_id, resume_args, agent=agent)
                    subprocess.Popen([
                        "tmux", "new-session", "-d", "-s", session_name,
                        "-c", spawn_cwd,
                        *_tmux_env,
                        *_get_agent_spawn_cmd(proj_id),
                    ])
                    _ns_push("session_running", proj_id=proj_id, kind="exec")  # M366
                    elapsed = 0
                    while elapsed < deadline:
                        await _aio.sleep(1)
                        elapsed += 1
                        _post_panes = subprocess.run(
                            ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                            capture_output=True, text=True
                        ).stdout.split()
                        _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)
                        if _post_alive:
                            break
                try:
                    if agents_backup is None:
                        agents_path.unlink(missing_ok=True)
                    else:
                        agents_path.write_text(agents_backup, encoding="utf-8")
                except Exception:
                    pass
                if not _post_alive:
                    return JSONResponse({
                        "ok": False, "mode": "tmux_spawn_failed",
                        "session": session_name,
                        "pane_cmds": _post_panes,
                        "resume_args": resume_args,
                        "retried": False,
                        "error": "Codex failed to start in tmux pane (pane fell back to shell).",
                    }, status_code=502)
                subprocess.run(["tmux", "send-keys", "-t", session_name, "go", "Enter"])
                return JSONResponse({
                    "ok": True, "mode": "tmux",
                    "session": session_name,
                    "tasks_created": len(actionable),
                    "message": f"Spawned tmux session '{session_name}' — {len(actionable)} milestone(s) queued as tasks"
                })
            # M473 follow-up: send the FULL stone text (newline-escaped) up to a
            # 1500-char safety cap, with an explicit truncation marker pointing to
            # GET /milestones/<MID> when over-cap. Replaces the silent [:60] /
            # [:70] truncation that could drop user-pasted content.
            def _snap_text(m, cap=1500):
                t = (m.get("text") or "").replace("\n", " / ")
                if len(t) > cap:
                    t = t[:cap] + f" ... [+{len(t)-cap} chars — call GET /milestones/{m.get('id')} for full body]"
                return t
            stone_lines = "\n".join(
                f"  {m.get('id')} [{m.get('status')}]: \"{_snap_text(m)}\""
                for m in actionable
            )
            # Include ALL milestones for full sync context
            def _snap_line_full(m):
                base = f"  {m.get('id')} [{m.get('status')}]: \"{_snap_text(m)}\""
                # M330: include skill/agent annotations so Claude invokes them per M246 fix
                _srefs = m.get("skill_refs") or ([m["skill_ref"]] if m.get("skill_ref") else [])
                _arefs = m.get("agent_refs") or ([m["agent_ref"]] if m.get("agent_ref") else [])
                for _sr in _srefs: base += f"  [skill: /{_sr}]"
                for _ar in _arefs: base += f"  [agent: {_ar}]"
                return base
            all_ms_lines = "\n".join(_snap_line_full(m) for m in active_ms)
            # M472: pre-compute dispatch waves for fresh-session prompt
            _fresh_queued = [m for m in active_ms if m.get("status") == "queued"]
            # M747: group queued stones by substar for per-substar session branching
            # M792: session router — substars sharing same assigned_session merge into one bucket
            _ns_list = proj.get("north_stars") or []
            _ns_by_id: dict = {ns["id"]: ns for ns in _ns_list if ns.get("id")}

            # M836.2: late guard kept as defense-in-depth — early auto-assign should have covered all,
            # but if any slipped through (race / late substar add), apply auto-assign again here.
            _unassigned_ids, _unassigned_names = _find_unassigned_substars(new_queued_top, _ns_list)
            if _unassigned_ids:
                _main_sess_name_late = f"{agent}-exec-{proj_id}"
                for _ns in _ns_list:
                    if _ns.get("id") in _unassigned_ids:
                        _ns["assigned_session"] = _main_sess_name_late
                proj["north_stars"] = _ns_list
                _ns_by_id = {ns["id"]: ns for ns in _ns_list if ns.get("id")}
                try:
                    _save_project(proj_id, proj)
                except Exception:
                    pass

            def _resolve_session_key(proj_id: str, substar_id: str, ns_by_id: dict) -> str:
                """M792: return assigned_session for this substar.
                M855 fix: substar_id on a stone may not match any current north-star
                (stale ID, substar deleted, ID format mismatch). Fall back to main session."""
                _assigned = (ns_by_id.get(substar_id) or {}).get("assigned_session") or ""
                if not _assigned.strip():
                    # Fallback: stone references unknown/stale substar → treat as main
                    return session_name
                return _assigned.strip()

            # _session_qs: session_key -> list of stones (may span multiple substars)
            _session_qs: dict = {}
            _main_qs: list = []
            for _q in _fresh_queued:
                _qsid = _q.get("substar_id", "")
                if _qsid:
                    _skey = _resolve_session_key(proj_id, _qsid, _ns_by_id)
                    _session_qs.setdefault(_skey, []).append(_q)
                else:
                    _main_qs.append(_q)
            # Keep _substar_qs alias for backward compatibility (kill-session loop below)
            _substar_qs = _session_qs
            # M824: branch whenever any assigned_session exists — every assigned tmux session must run on dispatch
            _branch_substars = len(_session_qs) >= 1
            # M824 LEAK FIX: build set of stone IDs owned by branched sessions
            _branched_mids: set = {m.get("id") for stones in _session_qs.values() for m in stones} if _branch_substars else set()
            # Stones for main session: ungrouped only (when branching) or all (when not)
            # M824 COLLISION GUARD: if any _skey == session_name, those stones belong to main
            if _branch_substars:
                _collision_keys = [k for k in list(_session_qs.keys()) if k == session_name]
                for _ck in _collision_keys:
                    _main_qs.extend(_session_qs.pop(_ck))
                    _branched_mids -= {m.get("id") for m in _main_qs}
            _dispatch_queued = _main_qs if _branch_substars else _fresh_queued
            # M824 LEAK FIX: filtered snapshot for main session — omits stones owned by branched sessions
            # so main orchestrator never TaskCreates them (race condition fix)
            if _branch_substars and _branched_mids:
                _main_all_ms_lines = "\n".join(
                    _snap_line_full(m) for m in active_ms if m.get("id") not in _branched_mids
                )
                _branched_session_names = sorted(_session_qs.keys())
            else:
                _main_all_ms_lines = all_ms_lines
                _branched_session_names = []
            _fresh_waves = _compute_dispatch_waves(_dispatch_queued) if _dispatch_queued else []
            _stamp_wave_indices(proj_id, _fresh_waves)  # M511: label wave_index on each stone
            _fresh_waves_section = _format_dispatch_waves(_fresh_waves) if len(_dispatch_queued) > 1 else ""
            # Spawn tmux session — use TaskCreate/TaskUpdate (Claude Code built-in) for task tracking
            _cron_mem_section = _load_stone_memory(proj_id)
            # M472: build per-stone implementation steps (shared by both parallel and sequential sections)
            _stone_impl_steps = (
                f"  Per-stone implementation steps:\n"
                f"  1. TaskUpdate(<id>, status='in_progress')\n"
                f"  1b. SKILL/AGENT INVOCATION (M770 strengthened — current invoke rate is only ~4% vs annotations):\n"
                f"      • If stone text or USER MSG carries [skill:/name] or [agent:name] OR `skill_refs`/`agent_refs` array → MANDATORY: invoke Skill(skill='name') / Agent(subagent_type='name') as the FIRST tool call BEFORE GET/Edit/anything.\n"
                f"      • This applies even in REPLY MODE and even when delegating to sub-agents (orchestrator must invoke skill itself, NOT skip because it's reply-only).\n"
                f"      • Skipping = silent failure. The skill_refs field on user msgs (visible in conversation[]) is the source of truth — re-check every PATCH cycle.\n"
                f"  2. GET {hub_api}/api/northstar/{proj_id}/milestones — read full text + conversation[].\n"
                f"  3. Edit/write files to implement the milestone.\n"
                f"  4. Append completion-log:\n"
                f'     echo \'{{\"session_id\":\"exec\",\"milestone_id\":\"<MID>\",\"evidence\":\"<one-line summary>\",\"timestamp\":\"\'$(date -Iseconds)\'\"}}\' >> ~/.hub/projects/{proj_id}/completion-log.jsonl\n'
                f"  5. PATCH {hub_api}/api/northstar/{proj_id}/milestones/<MID> body:\n"
                f'     {{"status":"pending_confirmation","star_relation":"<1-line gap closure>","model_used":"claude-sonnet-4-6","session_id":"$CLAUDE_CODE_SESSION_ID","exec_start":"<ISO start>","exec_end":"$(date -Iseconds)","append_message":{{"role":"claude","text":"<1-line English PAST TENSE result>"}}}}\n'
                f"     star_relation = HOW this completion reduced the star gap (concrete, 1 line).\n"
                f"     append_message = MANDATORY — ALWAYS include. Omitting it leaves the stone with no reply\n"
                f"       visible to the user (server inserts '완료.' as emergency fallback but it is generic).\n"
                f"       RULE: append_message in the SAME PATCH as status=pending_confirmation. Past tense only.\n"
                f"  5b. [검수] AUTO-REVIEW (M767): if the stone you just completed involved writing or modifying code,\n"
                f"      immediately POST a [검수] child stone based on the ACTUAL changes you made:\n"
                f"      POST {hub_api}/api/northstar/{proj_id}/milestones\n"
                f"      body: {{\"parent_id\":\"<MID>\",\"text\":\"[검수] <MID>: <1-line change summary> — ①변경파일/함수명 명시+회귀위험(영향 범위) ②의도대로 동작하는지+엣지케이스(null/empty/concurrent) ③OWASP 해당 여부(XSS/SQLi/auth 등) ④성능영향(루프/DB쿼리/렌더) ⑤완성도 0-10점\",\"status\":\"queued\"}}\n"
                f"      QUALITY RULE: <1-line change summary> must name the SPECIFIC functions/files changed (e.g. '_db_save_single_milestone in server.py').\n"
                f"                   Each ①-⑤ criterion should be filled with SPECIFIC observations from the diff, not generic placeholders.\n"
                f"      Skip if: stone is research/reply/question only, no files edited, or [검수] child already exists.\n"
                f"  5c. [검수] SCREENSHOT PROOF (M817): if THIS stone IS a [검수] stone (text starts with '[검수]'),\n"
                f"      after completing the review, capture a Playwright screenshot of the affected UI feature\n"
                f"      and attach it as evidence:\n"
                f"      ① Take screenshot: mcp__playwright-session-1__browser_navigate to http://127.0.0.1:{PORT}/northstar,\n"
                f"         then mcp__playwright-session-1__browser_take_screenshot with\n"
                f"         filename='/home/desk-1/Project/Moat/.playwright-mcp/review-<MID>-proof.png'\n"
                f"      ② Upload: rclone copy /home/desk-1/Project/Moat/.playwright-mcp/review-<MID>-proof.png 'gdrive:claude-shared/Moat/outbox/'\n"
                f"         GDRIVE_URL=$(rclone link 'gdrive:claude-shared/Moat/outbox/review-<MID>-proof.png')\n"
                f"      ③ Include in completion PATCH: add \"evidence_url\":\"$GDRIVE_URL\" to the PATCH body.\n"
                f"      Skip if: reviewed stone was research/docs only (no UI to screenshot).\n"
                f"  6. TaskUpdate(<id>, status='completed')\n\n"
            )
            cron_prompt = _cron_mem_section + (
                f"[EXECUTE SYNC] Project {proj_id} — Execute clicked. PRIMARY GOAL: implement ALL queued milestones.\n\n"
                f"STEP 0 — RESET task list:\n"
                f"  TaskList → TaskUpdate each existing task to status='completed' (start fresh).\n\n"
                f"STEP 1 — TaskCreate one task per queued milestone (status=queued in list below).\n\n"
                f"PRE-STEP — Sync unreviewed stones (claude_ack=null):\n"
                f"  PATCH claude_ack=now. Vague → status=needs_clarification + clarification_question.\n\n"
                f"PARALLEL DISPATCH PROTOCOL (M472) —\n"
                f"  The server has pre-computed DISPATCH WAVES below.\n"
                f"  Wave with PARALLEL label = stones are independent → dispatch ALL in ONE message as multiple Agent tool calls.\n"
                f"  Wave with 'single' label = one dependent stone → implement sequentially AFTER prior wave completes.\n"
                f"  RULE: emit ALL parallel-wave Agent calls in a SINGLE message (true concurrent dispatch).\n"
                f"  Each sub-agent is self-contained: reads its own stone, implements, PATCHes, logs completion.\n"
                f"  After each wave completes, move to next wave. Do NOT start wave N+1 before wave N finishes.\n"
                f"  FALL BACK TO SEQUENTIAL when: (a) substone whose mother is also queued, (b) stones editing same file.\n\n"
                f"SUB-AGENT PROTOCOL — each Agent call must:\n"
                + _stone_impl_steps
                + f"ORCHESTRATOR — after all waves complete:\n"
                f"  Post ONE consolidated 1-line summary listing completed MIDs.\n\n"
                f"COMMENT RULE: append_message MUST be ≤3 lines. 1 line preferred.\n\n"
                f"TOKEN DISCIPLINE: NO progress comments during work. ONE past-tense summary at completion.\n\n"
                f"CAVEMAN LITE MODE (M582): Terse output — drop filler/politeness, keep technical precision. ~60% fewer output tokens.\n\n"
                + (
                    f"BRANCHED SESSIONS: substar-assigned queued stones are handled by their per-substar tmux sessions "
                    f"({', '.join(_branched_session_names)}). "
                    f"DO NOT TaskCreate or Agent-dispatch those — they are filtered out of the snapshot below.\n\n"
                    if _branched_session_names else ""
                )
                + _fresh_waves_section
                + f"Active milestones (snapshot — TaskCreate only for status=queued):\n{_main_all_ms_lines}"
            )
            # Write prompt to file — avoids tmux paste-mode for multi-line text
            prompt_file = PROJECTS_DIR / proj_id / "pending-execute-prompt.txt"
            prompt_file.parent.mkdir(parents=True, exist_ok=True)
            prompt_file.write_text(cron_prompt, encoding="utf-8")

            # M747/M792: write per-session dispatch prompts before killing/spawning sessions
            # M792: session_key may cover multiple substars — group by substar_id within each session
            if _branch_substars:
                for _skey, _ss_stones in _session_qs.items():
                    _ss_short = _skey[-12:]  # last 12 chars of session key for file naming
                    # Collect distinct substar_ids in this session bucket (queued_at order)
                    _ss_substar_ids = list(dict.fromkeys(
                        m.get("substar_id", "") for m in sorted(_ss_stones, key=lambda x: x.get("queued_at") or "")
                        if m.get("substar_id")
                    ))
                    _ss_substar_names = [
                        (_ns_by_id.get(sid) or {}).get("name") or sid[-8:] for sid in _ss_substar_ids
                    ]
                    # Group stones by substar_id, ordered by queued_at within each group
                    _ss_snap_parts = []
                    for _sid in _ss_substar_ids:
                        _sid_stones = sorted(
                            [m for m in _ss_stones if m.get("substar_id") == _sid],
                            key=lambda x: x.get("queued_at") or ""
                        )
                        _ss_snap_parts.append(
                            f"  [substar: {(_ns_by_id.get(_sid) or {}).get('name') or _sid[-8:]}]\n"
                            + "\n".join(
                                f"    {m.get('id')} [queued]: \"{(m.get('text') or '')[:120].replace(chr(10), ' / ')}\""
                                for m in _sid_stones
                            )
                        )
                    _ss_snap = "\n".join(_ss_snap_parts)
                    _ss_waves = _compute_dispatch_waves(_ss_stones) if len(_ss_stones) > 1 else []
                    _ss_waves_sec = _format_dispatch_waves(_ss_waves) if _ss_waves else ""
                    _multi_note = (
                        f"NOTE (M792): Stones from substars [{', '.join(_ss_substar_names)}] share this session "
                        f"— process in queued_at order, group by substar in completion-log.\n\n"
                        if len(_ss_substar_ids) > 1 else ""
                    )
                    _ss_prompt = (
                        _cron_mem_section
                        + f"[EXECUTE SYNC] Project {proj_id} — Session '{_skey}' "
                        f"(substars: {', '.join(_ss_substar_names)}).\n"
                        f"PRIMARY GOAL: implement ALL queued stones for this session.\n\n"
                        + _multi_note
                        + _stone_impl_steps
                        + _ss_waves_sec
                        + f"Queued stones for this session (grouped by substar, queued_at order):\n{_ss_snap}\n\n"
                        + f"Active milestones (full context):\n{all_ms_lines}"
                    )
                    _ss_pf = PROJECTS_DIR / proj_id / f"pending-execute-prompt-{_ss_short}.txt"
                    _ss_pf.write_text(_ss_prompt, encoding="utf-8")

            # M255: kill ALL agent-prefixed sessions before spawn — prevents orphaned cross-agent sessions
            # (e.g. codex-exec-FreeOS left alive when switching to claude-exec-FreeOS)
            _kill_all_exec_sessions(proj_id)
            # M864: kill active substar sessions that have no queued work (unrelated sessions)
            _active_substars_now = _get_active_substar_sessions(proj_id)
            for _ss_short2, _ss_sname2 in _active_substars_now.items():
                if _ss_sname2 not in _session_qs:
                    subprocess.run(["tmux", "kill-session", "-t", _ss_sname2], capture_output=True)
                    _exec_idle_count.pop(_ss_sname2, None)
                    _exec_was_running.pop(_ss_sname2, None)
            # M747/M792: also kill stale substar/session-key sessions before respawning
            # M824 ALIVE-BRANCH SKIP: reuse alive sessions that already have Claude running
            _alive_branch_keys: set = set()
            if _branch_substars:
                for _skey in list(_session_qs.keys()):
                    # M824 COLLISION GUARD: never kill main session here (handled by _kill_all_exec_sessions above)
                    if _skey == session_name:
                        continue
                    _pane_cmds = subprocess.run(
                        ["tmux", "list-panes", "-t", _skey, "-F", "#{pane_current_command}"],
                        capture_output=True, text=True
                    ).stdout.split()
                    _branch_alive = any(c.strip() and c.strip() not in _SHELLS for c in _pane_cmds)
                    if _branch_alive:
                        # Session already running Claude — skip kill/respawn; will send "go" after main spawns
                        _alive_branch_keys.add(_skey)
                    else:
                        subprocess.run(["tmux", "kill-session", "-t", _skey], capture_output=True)
            # Session-resume continuity (spec: docs/research/20260513-tmux-claude-session-resume-design.md MVF #2-3)
            spawn_cwd = proj_dir if Path(proj_dir).exists() else str(Path.home())

            # M181: clean up stale .lock in ~/.claude/tasks/hub-exec-{proj_id}/.
            # If a previous Claude held the lock when killed, the next spawn fails
            # to acquire it (Issue anthropics/claude-code#44917 — no auto cleanup).
            # A lock file older than 10 s is definitionally stale because no live
            # Claude could have written to it that long ago without a heartbeat.
            try:
                _task_dir = Path.home() / ".claude" / "tasks" / f"hub-exec-{proj_id}"
                _lock = _task_dir / ".lock"
                if _lock.exists():
                    _age = time.time() - _lock.stat().st_mtime
                    if _age > 10:
                        _lock.unlink()
            except Exception:
                pass

            # M836 CONFIRMED: _get_resume_args always returns safely for fresh projects.
            # When no prior session exists, it falls through all history lookups and returns
            # ["--continue"], which starts a new conversation. No try/except needed.
            resume_args = _get_resume_args(proj_id, spawn_cwd, explicit_session_id, agent=agent)
            _record_spawn_info(proj_id, resume_args, agent=agent)
            _tmux_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}", "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}"]
            for _k, _v in _get_project_spawn_env(proj_id).items():
                _tmux_env += ["-e", f"{_k}={_v}"]
            subprocess.Popen([
                "tmux", "new-session", "-d", "-s", session_name,
                "-c", spawn_cwd,
                *_tmux_env,
                "claude", "--dangerously-skip-permissions", *_get_project_model(proj_id), *resume_args,
            ])
            import asyncio as _aio
            # Wait for Claude + SessionStart hook to complete
            # Hook deletes the prompt file when injected — use file deletion as ready signal
            deadline = 12  # max wait seconds
            elapsed = 0
            while elapsed < deadline:
                await _aio.sleep(1)
                elapsed += 1
                if not prompt_file.exists():
                    break  # hook fired and deleted the file — Claude is ready

            # M180: post-spawn validation. If Claude failed to start (bad
            # --resume id, rate-limit at boot, crash), the pane drops to bash
            # and the SessionStart hook never fires (prompt_file stays).
            # Detect by inspecting pane_current_command — must NOT be a shell.
            _post_panes = subprocess.run(
                ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                capture_output=True, text=True
            ).stdout.split()
            _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)

            # M181: auto-retry once with no resume args if first spawn failed
            # while resume args were in play (likely stale --resume target).
            _retried = False
            if not _post_alive and resume_args:
                # Kill pane, clear stale .last-session-id, respawn without resume
                subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)
                try:
                    (PROJECTS_DIR / proj_id / ".last-session-id").unlink()
                except Exception:
                    pass
                _retried = True
                resume_args = []  # fresh spawn — no continuity
                _record_spawn_info(proj_id, resume_args, agent=agent)
                _tmux_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}", "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}"]
                for _k, _v in _get_project_spawn_env(proj_id).items():
                    _tmux_env += ["-e", f"{_k}={_v}"]
                subprocess.Popen([
                    "tmux", "new-session", "-d", "-s", session_name,
                    "-c", spawn_cwd,
                    *_tmux_env,
                    "claude", "--dangerously-skip-permissions", *_get_project_model(proj_id),
                ])
                elapsed = 0
                while elapsed < deadline:
                    await _aio.sleep(1)
                    elapsed += 1
                    if not prompt_file.exists():
                        break
                _post_panes = subprocess.run(
                    ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                    capture_output=True, text=True
                ).stdout.split()
                _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)

            if not _post_alive:
                # Spawn still failed even after retry — surface the error
                return JSONResponse({
                    "ok": False, "mode": "tmux_spawn_failed",
                    "session": session_name,
                    "pane_cmds": _post_panes,
                    "resume_args": resume_args,
                    "retried": _retried,
                    "error": "Claude failed to start in tmux pane (pane fell back to shell)" +
                             (" — auto-retry without --resume also failed. Likely rate-limit at boot or auth issue." if _retried else
                              ". Likely cause: stale --resume target or rate-limit at boot."),
                }, status_code=502)

            subprocess.run(["tmux", "send-keys", "-t", session_name, "go", "Enter"])

            # M747/M792: spawn per-session sessions after main session is live
            # M792: iterate session_qs (session_key -> stones); session_name IS the key
            if _branch_substars:
                _ss_tmux_base = list(_tmux_env)  # copy main tmux env
                _newly_spawned: list = []
                for _skey, _ss_stones in _session_qs.items():
                    _ss_short = _skey[-12:]  # consistent with prompt file naming
                    # _skey is the full tmux session name (assigned_session or _substar_session_name)
                    _ss_sname = _skey
                    # M824 COLLISION GUARD: _skey == session_name means this bucket was merged into main above
                    if _ss_sname == session_name:
                        continue
                    # M824 ALIVE-BRANCH SKIP: session is already running Claude — just re-inject prompt
                    if _skey in _alive_branch_keys:
                        subprocess.run(["tmux", "send-keys", "-t", _skey, "go", "Enter"])
                        continue
                    _ss_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}-{_ss_short}",
                               "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                               "-e", f"NS_SESSION_KEY={_skey}"]
                    for _k, _v in _get_project_spawn_env(proj_id).items():
                        _ss_env += ["-e", f"{_k}={_v}"]
                    # M792: detect branch-from-main — if assigned_session ends with -branch,
                    # look up branch_from_session_id on the substar and pass --resume <id>.
                    # M833: prefer stored last_session_id from .branch-sessions.json (continuity across dispatches)
                    # M837: use --continue for branched sessions to avoid thinking.signature API 400 errors.
                    # --resume inherits old thinking blocks that may become invalidated across sessions.
                    # --continue uses the most recent session transcript safely.
                    _ss_resume_args: list = ["--continue"]
                    _encoded_cwd = _encode_cwd_for_claude(spawn_cwd)
                    _transcripts_dir = Path.home() / ".claude" / "projects" / _encoded_cwd
                    # M837: --resume blocks removed; --continue is stable. Priority 2 block removed.
                    subprocess.Popen([
                        "tmux", "new-session", "-d", "-s", _ss_sname,
                        "-c", spawn_cwd,
                        *_ss_env,
                        "claude", "--dangerously-skip-permissions", *_get_project_model(proj_id),
                        *_ss_resume_args,
                    ])
                    _newly_spawned.append((_ss_sname, time.time()))
                _server_log_action(proj_id, "", "exec:substar_branch",
                                   f"spawned {len(_newly_spawned)} new + {len(_alive_branch_keys)} reused sessions: "
                                   f"{','.join([s for s, _ in _newly_spawned]+list(_alive_branch_keys))}")

            # Feature: record the new transcript ID so it appears in the resume list immediately
            # (before Stop hook fires). Small delay to let Claude create the transcript file.
            import asyncio as _aio2
            await _aio2.sleep(2)
            # M256: use model from .last-spawn-info.json (just written above) so the session
            # is stored under the correct model_key (e.g. "or-owl-alpha" not "")
            _spawn_info_path = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
            _recorded_model = ""
            try:
                _recorded_model = json.loads(_spawn_info_path.read_text()).get("model", "") or ""
            except Exception:
                _recorded_model = _get_project_model_value(proj_id) or ""
            _new_sid = _update_session_history_from_transcript(proj_id, spawn_cwd, _recorded_model)
            # M833: capture branch session IDs for --resume continuity on next dispatch
            if _branch_substars and _newly_spawned:
                _br_encoded_cwd = _encode_cwd_for_claude(spawn_cwd)
                _br_transcripts_dir = Path.home() / ".claude" / "projects" / _br_encoded_cwd
                try:
                    _all_jsonls = sorted(
                        _br_transcripts_dir.glob("*.jsonl"),
                        key=lambda f: f.stat().st_mtime,
                        reverse=True,
                    )
                except Exception:
                    _all_jsonls = []
                # Assign newest JSONL files (by mtime >= spawn_ts - 5s) to newly spawned sessions
                _br_spawn_ts = min(ts for _, ts in _newly_spawned)
                # M837: exclude main's session_id from capture pool so branched sessions never get assigned main's transcript.
                # Main writes hot transcript (mtime advances during work) and would otherwise win the mtime sort against a
                # truly-new branched JSONL, leaving the branched session unable to surface its own live_session_id.
                _br_excluded_sids: set = set()
                try:
                    _main_sinfo = json.loads(_spawn_info_path.read_text()).get("from_id") or ""
                    if _main_sinfo: _br_excluded_sids.add(_main_sinfo)
                except Exception:
                    pass
                if _new_sid: _br_excluded_sids.add(_new_sid)
                _br_new_jsonls = [
                    f for f in _all_jsonls
                    if f.stat().st_size > 0
                    and f.stat().st_mtime >= _br_spawn_ts - 5
                    and f.stem not in _br_excluded_sids
                ]
                # Distribute: one file per spawned session (mtime order matches spawn order)
                # Skip storage if no candidate JSONL found — better to fall back to fresh next dispatch than store wrong sid.
                for (_br_sname, _br_ts), _br_jf in zip(
                    sorted(_newly_spawned, key=lambda x: x[1]),
                    sorted(_br_new_jsonls, key=lambda f: f.stat().st_mtime),
                ):
                    _save_branch_session(proj_id, _br_sname, _br_jf.stem)
            # M818: include all spawned session names in response for UI visibility
            _all_spawned = [session_name] + (list(_session_qs.keys()) if _branch_substars else [])
            return JSONResponse({
                "ok": True, "mode": "tmux",
                "session": session_name,
                "spawned_sessions": _all_spawned,
                "tasks_created": len(actionable),
                "message": f"Spawned tmux session '{session_name}' — {len(actionable)} milestone(s) queued as tasks"
            })

        tasks_created = 0
        for m in actionable:
            mid = m.get("id", "")
            text = m.get("text", "")
            status = m.get("status", "pending")
            task_file = task_queue / f"task-{proj_id}-{mid}-{ts}.md"
            # Skip if task already queued or result exists
            result_file = HERE / "task-results" / f"task-{proj_id}-{mid}-{ts}.json"
            lock_file = HERE / "task-locks" / f"task-{proj_id}-{mid}-{ts}.lock"
            if task_file.exists() or result_file.exists() or lock_file.exists():
                continue
            task_file.write_text(
                f"# Task: {proj_id}/{mid}\n\n"
                f"Stone: [{status}] \"{text}\"\n\n"
                f"Instructions:\n"
                f"1. PATCH claude_ack=now: {hub_api}/api/northstar/{proj_id}/milestones/{mid}\n"
                f"2. If status=needs_clarification: PATCH status=needs_clarification + clarification_question\n"
                f"3. If clear actionable task: implement it directly\n"
                f"4. On completion: append to ~/.hub/projects/{proj_id}/completion-log.jsonl:\n"
                f'   {{"session_id":"worker","milestone_id":"{mid}","evidence":"<what was done>","timestamp":"<ISO>"}}\n'
                f"5. PATCH {hub_api}/api/northstar/{proj_id}/milestones/{mid} with status=pending_confirmation\n\n"
                f"Hub API: {hub_api}\n"
            )
            tasks_created += 1

        mode = "work"
        return JSONResponse({"ok": True, "mode": mode, "tasks_created": tasks_created,
                             "message": f"Dispatched {tasks_created} stone tasks to worker (no user message needed)"})


@app.post("/api/northstar/create")
async def create_project(request: Request):
    """Create a new project node with a minimal north-star.md."""
    data = await request.json()
    name = (data.get("name") or "").strip()
    repo_path = (data.get("repo_path") or "").strip() or None
    if not name:
        return JSONResponse({"ok": False, "error": "name required"}, status_code=400)
    # Use name as folder ID (sanitize)
    import re as _re_
    folder_id = _re_.sub(r"[^\w\-]", "_", name)
    proj_dir = PROJECTS_DIR / folder_id
    proj_dir.mkdir(parents=True, exist_ok=True)
    md = proj_dir / "north-star.md"
    if md.exists():
        return JSONResponse({"ok": False, "error": "project already exists"}, status_code=409)
    body = f"# {name} — North Star\n\n## Why this metric\n\n## Strategy\n\n## OKRs\n"
    frontmatter = {
        "name": name, "metric": "", "current": "", "target": "",  # M780: blank defaults — user fills in as needed
        "status": "paused", "deadline": "", "note": "",
        "milestones": [], "log": [], "connections": [],
        "north_stars": [],  # M908: explicit empty list prevents fallback default sub-star creation
        "layer": 0, "x": None, "y": None,
    }
    created_dir = False
    if repo_path:
        # M258: if the node's repo_path doesn't exist on the server, mkdir -p it.
        was_created, resolved = _ensure_repo_path_exists(repo_path)
        created_dir = was_created
        frontmatter["repo_path"] = resolved or repo_path
    text = "---\n" + _yaml.dump(frontmatter, allow_unicode=True, default_flow_style=False) + "---\n\n" + body
    md.write_text(text, encoding="utf-8")
    return JSONResponse({"ok": True, "id": folder_id, "created_repo_dir": created_dir})


@app.post("/api/northstar/{proj_id}/semantic-scores")
async def compute_semantic_scores(proj_id: str, request: Request):
    """M658: Compute semantic proximity score (0-100) per stone using sentence-transformers.
    Score = cosine_similarity(stone_embedding, substar_embedding) * completion_weight * 100.
    Results cached in data_json['semantic_score']. Only scores stones assigned to a substar."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    milestones_list = proj.get("milestones", [])
    north_stars = proj.get("north_stars", [])

    # Build substar text map
    substar_text = {}
    for ns in north_stars:
        sid = ns.get("id", "")
        text = (ns.get("name", "") + " " + ns.get("description", "")).strip() or sid
        substar_text[sid] = text

    # Collect stones that have a substar assignment (milestones is a list of dicts)
    scored = {}
    to_embed_stones = []
    stone_index = {}  # mid → list index for in-place update

    for i, m in enumerate(milestones_list):
        if not isinstance(m, dict):
            continue
        mid = m.get("id", "")
        stone_index[mid] = i
        sid = m.get("substar_id", "")
        if not sid or sid not in substar_text:
            continue
        stone_text = m.get("text", "")
        if not stone_text:
            continue
        cached = m.get("semantic_score")
        if cached is not None:
            scored[mid] = cached
            continue
        to_embed_stones.append((mid, stone_text, sid))

    unique_substars = {sid: substar_text[sid] for _, _, sid in to_embed_stones if sid in substar_text}

    if not to_embed_stones:
        return JSONResponse({"ok": True, "scores": scored, "computed": 0})

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
        model = _get_st_model()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"sentence-transformers unavailable: {e}"}, status_code=500)

    substar_ids = list(unique_substars.keys())
    substar_embeddings = model.encode([unique_substars[s] for s in substar_ids], normalize_embeddings=True)
    substar_emb_map = {sid: emb for sid, emb in zip(substar_ids, substar_embeddings)}

    stone_texts = [t for _, t, _ in to_embed_stones]
    stone_embeddings = model.encode(stone_texts, normalize_embeddings=True)

    completion_weights = {"done": 1.0, "pending_confirmation": 0.8, "queued": 0.5, "pending": 0.4, "held": 0.1}
    for (mid, stone_text, sid), stone_emb in zip(to_embed_stones, stone_embeddings):
        ss_emb = substar_emb_map.get(sid)
        if ss_emb is None:
            continue
        cos_sim = float(np.dot(stone_emb, ss_emb))
        cos_sim = max(0.0, cos_sim)
        idx = stone_index.get(mid)
        if idx is None:
            continue
        m = milestones_list[idx]
        weight = completion_weights.get(m.get("status", "queued"), 0.5)
        score = round(cos_sim * weight * 100)
        scored[mid] = score
        milestones_list[idx]["semantic_score"] = score

    proj["milestones"] = milestones_list
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "scores": scored, "computed": len(to_embed_stones)})


_st_model_cache = None
def _get_st_model():
    global _st_model_cache
    if _st_model_cache is None:
        from sentence_transformers import SentenceTransformer
        _st_model_cache = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    return _st_model_cache


@app.post("/api/northstar/{proj_id}/backfill-tokens")
async def backfill_tokens(proj_id: str):
    """M509: Re-compute total_tokens for all stones that have exec_start+exec_end but no tokens.
    Safe — does NOT change status or pending_confirm_at."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    milestones = proj.get("milestones", [])
    filled = 0
    for m in milestones:
        if not isinstance(m, dict): continue
        if m.get("total_tokens"): continue
        t_start = m.get("exec_start")
        t_end   = m.get("exec_end")
        if not t_start or not t_end: continue
        try:
            computed = _compute_tokens_from_transcript(proj_id, t_start, t_end)
            if computed is not None:
                m["total_tokens"] = computed
                filled += 1
        except Exception:
            pass
    if filled:
        _parse_cache.pop(str(md), None)
        _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "filled": filled})


@app.delete("/api/northstar/{proj_id}")
async def delete_project(proj_id: str):
    """Delete a project node (removes north-star.md)."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    # M918: kill all exec sessions for this project before deletion
    try:
        killed = _kill_all_exec_sessions(proj_id)
        if killed:
            print(f"[M918] killed sessions for {proj_id}: {killed}", file=__import__('sys').stderr)
    except Exception as _e:
        print(f"[M918] session kill error for {proj_id}: {_e}", file=__import__('sys').stderr)
    md.unlink()
    return JSONResponse({"ok": True})


@app.post("/api/northstar/{proj_id}/connect")
async def add_connection(proj_id: str, request: Request):
    """Add a connection edge between two projects (bidirectional)."""
    data = await request.json()
    target_id = data.get("target", "").strip()
    if not target_id or target_id == proj_id:
        return JSONResponse({"ok": False, "error": "invalid target"}, status_code=400)
    for pid, tid in [(proj_id, target_id), (target_id, proj_id)]:
        md = PROJECTS_DIR / pid / "north-star.md"
        if not md.exists(): continue
        proj = _parse_md_frontmatter(md)
        conns = proj.get("connections") or []
        if not isinstance(conns, list): conns = []
        if tid not in conns:
            conns.append(tid)
        proj["connections"] = conns
        _save_project(pid, proj)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/connect/{target_id}")
async def remove_connection(proj_id: str, target_id: str):
    """Remove a connection edge between two projects."""
    for pid, tid in [(proj_id, target_id), (target_id, proj_id)]:
        md = PROJECTS_DIR / pid / "north-star.md"
        if not md.exists(): continue
        proj = _parse_md_frontmatter(md)
        conns = [c for c in (proj.get("connections") or []) if c != tid]
        proj["connections"] = conns
        _save_project(pid, proj)
    return JSONResponse({"ok": True})


@app.patch("/api/northstar/{proj_id}/rename")
async def rename_project(proj_id: str, request: Request):
    data = await request.json()
    new_name = data.get("name", "").strip()
    if not new_name:
        return JSONResponse({"ok": False, "error": "name required"}, status_code=400)
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False}, status_code=404)
    proj = _parse_md_frontmatter(md)
    proj["name"] = new_name
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "name": new_name})


@app.patch("/api/northstar/{proj_id}")
async def patch_project(proj_id: str, request: Request):
    """Update simple top-level project fields (deadline, status, note, links, metric/current/target/unit, etc.)."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False}, status_code=404)
    proj = _parse_md_frontmatter(md)
    # M165: allow manual edit of star fields (metric/current/target/unit) from ns-dash UI
    # M214: `model` controls --model flag passed to tmux exec agent spawns for this project.
    # `pty_agent` controls which runtime owns the interactive PTY on the card.
    # `agent` selects which CLI runtime owns the loop (`claude` or `codex`).
    # continuity_mode: session continuity strategy (isolated/portable/fresh)
    allowed = {"deadline", "status", "note", "links", "stage",
               "metric", "current", "target", "unit", "model", "pty_agent", "agent", "continuity_mode",
               "lane_labels"}  # M820: swimlane labels synced server-side
    for k in allowed:
        if k in data:
            if k == "model":
                v = (data[k] or "").strip()
                if v and v not in _ALLOWED_MODELS:
                    return JSONResponse({"ok": False, "error": f"unknown model '{v}'",
                                         "allowed": sorted(_ALLOWED_MODELS)}, status_code=400)
                proj[k] = v  # empty string = unset (CLI default)
            elif k == "agent":
                v = (data[k] or "claude").strip().lower()
                if v not in _ALLOWED_AGENTS:
                    return JSONResponse({"ok": False, "error": f"unknown agent '{v}'",
                                         "allowed": sorted(_ALLOWED_AGENTS)}, status_code=400)
                proj[k] = v
            elif k == "pty_agent":
                v = (data[k] or "claude").strip().lower()
                if v not in _ALLOWED_PTY_AGENTS:
                    return JSONResponse({"ok": False, "error": f"unknown pty_agent '{v}'",
                                         "allowed": sorted(_ALLOWED_PTY_AGENTS)}, status_code=400)
                proj[k] = v
            elif k == "continuity_mode":
                v = (data[k] or "isolated").strip()
                if v not in {"isolated", "portable", "fresh"}:
                    return JSONResponse({"ok": False, "error": f"invalid continuity_mode '{v}'"},
                                        status_code=400)
                proj[k] = v
            else:
                proj[k] = data[k]
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True})


@app.patch("/api/northstar/{proj_id}/layout")
async def update_layout(proj_id: str, request: Request):
    """Update swimlane layout fields: layer, parent, position_x.
    M221: also accepts `parents` (list of project ids) for multi-parent links.
    Mutually consistent: when `parents` is written, `parent` is set to parents[0]
    so legacy single-parent readers still resolve to a valid edge."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False}, status_code=404)
    proj = _parse_md_frontmatter(md)
    created_dir = False
    for k in ("layer", "parent", "position_x", "position_y", "x", "y", "repo_path", "stage", "avatar_url"):
        if k in data:
            if k == "repo_path" and data[k]:
                # M258: mkdir -p when the path doesn't exist yet on the server.
                was_created, resolved = _ensure_repo_path_exists(data[k])
                created_dir = created_dir or was_created
                proj[k] = resolved or data[k]
            else:
                proj[k] = data[k]
                # M331: clearing parent:null also clears parents array
                if k == "parent" and data[k] is None:
                    proj["parents"] = []
    if "parents" in data:
        v = data["parents"]
        if v is None:
            proj["parents"] = []
            proj["parent"] = None
        elif isinstance(v, list):
            # Dedup, drop empty, preserve order
            seen = set()
            cleaned = []
            for item in v:
                s = (str(item).strip() if item else "")
                if s and s not in seen:
                    seen.add(s); cleaned.append(s)
            proj["parents"] = cleaned
            proj["parent"] = cleaned[0] if cleaned else None
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "created_repo_dir": created_dir})


# M267: centralized SSE push channel. Replaces the unreliable OS Notification path.
# Only USER-originated events emit here — claude-originated mutations (autonomous PATCH,
# M222 auto-flip, append_message role:claude) MUST NOT call _ns_push().
_NS_PUSH_SUBSCRIBERS: list[asyncio.Queue] = []

# M389: Telegram-only push notifications (ntfy removed)
_TG_TOKEN_FILE = _HUB_DATA_DIR / ".telegram-token"
_TG_CHAT_FILE  = _HUB_DATA_DIR / ".telegram-chat-id"

def _get_telegram_config() -> tuple[str, str]:
    """Return (bot_token, chat_id). Empty strings if not configured."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token and _TG_TOKEN_FILE.exists():
        token = _TG_TOKEN_FILE.read_text().strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id and _TG_CHAT_FILE.exists():
        chat_id = _TG_CHAT_FILE.read_text().strip()
    return token, chat_id

_notify_last_ts: dict[str, float] = {}  # M389: cooldown removed — Telegram has no rate limits
_NOTIFY_COOLDOWN_SEC = 0               # no cooldown

def _send_ntfy_notification(title: str, body: str, priority: str = "default") -> None:
    """Send Telegram push notification for live→idle events. In-page toast handled separately via SSE."""
    now = time.time()
    last = _notify_last_ts.get(title, 0)
    if now - last < _NOTIFY_COOLDOWN_SEC:
        return
    _notify_last_ts[title] = now
    tg_token, tg_chat_id = _get_telegram_config()
    if not (tg_token and tg_chat_id):
        return
    try:
        import urllib.request as _ur, urllib.parse as _up
        # Use plain text (not Markdown) to avoid parse errors with special chars in title/body
        msg = f"{title}\n{body}"
        payload = _up.urlencode({"chat_id": tg_chat_id, "text": msg}).encode()
        req = _ur.Request(f"https://api.telegram.org/bot{tg_token}/sendMessage",
                          data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
        # M460: increased timeout 5→10s + retry once on network timeout
        for _attempt in range(2):
            try:
                resp = _ur.urlopen(req, timeout=10)
                print(f"[tg] sent '{title}' → {resp.status}", file=sys.stderr)
                break
            except Exception as _tg_err:
                if _attempt == 0:
                    print(f"[tg] retry '{title}': {_tg_err}", file=sys.stderr)
                else:
                    print(f"[tg] FAILED '{title}': {_tg_err}", file=sys.stderr)
    except Exception as _tg_err:
        print(f"[tg] FAILED '{title}': {_tg_err}", file=sys.stderr)


def _ns_push(event_type: str, **payload):
    """Broadcast a user-originated event to every connected SSE subscriber.
    Non-blocking — queues full are dropped so a slow tab can't stall the server."""
    data = {"event": event_type, "ts": time.time(), **payload}
    for q in list(_NS_PUSH_SUBSCRIBERS):
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            pass

@app.get("/api/notifications/stream")
async def notifications_stream(request: Request):
    """SSE stream of user-originated NS events. Each connected browser tab opens
    one EventSource. Server retains every subscriber's Queue and broadcasts via
    _ns_push(). Heartbeat every 25s keeps proxies from closing the connection."""
    q: asyncio.Queue = asyncio.Queue(maxsize=64)
    _NS_PUSH_SUBSCRIBERS.append(q)
    async def gen():
        # initial hello so the client knows the channel is up
        yield f"event: hello\ndata: {json.dumps({'ts': time.time()})}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=25.0)
                    yield f"event: {data['event']}\ndata: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    # heartbeat — comment line, ignored by EventSource
                    yield ": ping\n\n"
        finally:
            try: _NS_PUSH_SUBSCRIBERS.remove(q)
            except ValueError: pass
    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# M238: server-backed swimlane memo (survives hub restarts; localStorage was per-browser only).
_GLOBAL_MEMO_PATH = HERE / "global-memo.txt"

@app.get("/api/hub/memo")
async def get_global_memo():
    content = _GLOBAL_MEMO_PATH.read_text(encoding="utf-8") if _GLOBAL_MEMO_PATH.exists() else ""
    return JSONResponse({"ok": True, "content": content})

@app.post("/api/hub/memo")
async def save_global_memo(request: Request):
    data = await request.json()
    content = str(data.get("content", ""))
    _GLOBAL_MEMO_PATH.parent.mkdir(parents=True, exist_ok=True)
    _GLOBAL_MEMO_PATH.write_text(content, encoding="utf-8")
    return JSONResponse({"ok": True})


@app.get("/api/northstar/{proj_id}/memo")
async def get_memo(proj_id: str):
    memo_path = PROJECTS_DIR / proj_id / "memo.md"
    content = memo_path.read_text(encoding="utf-8") if memo_path.exists() else ""
    return JSONResponse({"ok": True, "content": content})


@app.post("/api/northstar/{proj_id}/memo")
async def save_memo(proj_id: str, request: Request):
    data = await request.json()
    content = data.get("content", "")
    memo_path = PROJECTS_DIR / proj_id / "memo.md"
    memo_path.parent.mkdir(parents=True, exist_ok=True)
    memo_path.write_text(content, encoding="utf-8")
    return JSONResponse({"ok": True})


# Explicit pill status set by hooks (overrides derived WS state)
_pill_status: dict[str, str] = {}  # proj_id → RUNNING|WAITING|IDLE|DONE


@app.patch("/api/northstar/{proj_id}/session-status")
async def set_session_status(proj_id: str, request: Request):
    """Hook endpoint — Stop/Notification hooks POST status updates here."""
    data = await request.json()
    status = data.get("status", "IDLE").upper()
    if status not in ("RUNNING", "WAITING", "IDLE", "DONE"):
        return JSONResponse({"ok": False, "error": "invalid status"}, status_code=400)
    _pill_status[proj_id] = status
    return JSONResponse({"ok": True, "status": status})


@app.delete("/api/northstar/{proj_id}/session")
async def kill_session_http(proj_id: str):
    """Kill a terminal session via HTTP (fallback when WS not available)."""
    _kill_session(proj_id)
    _pill_status.pop(proj_id, None)  # clear stuck waiting/active pill
    return {"ok": True, "killed": proj_id}


@app.post("/api/northstar/{proj_id}/exec-inject")
async def exec_inject(proj_id: str, request: Request):
    """Send a message to the Execute-spawned tmux session via send-keys."""
    data = await request.json()
    prompt = data.get("prompt", "").strip()
    if not prompt:
        return JSONResponse({"ok": False, "error": "prompt required"}, status_code=400)
    exec_session = ""
    for candidate in _exec_session_names(proj_id):
        check = subprocess.run(["tmux", "has-session", "-t", candidate], capture_output=True)
        if check.returncode == 0:
            exec_session = candidate
            break
    if not exec_session:
        return JSONResponse({"ok": False, "error": "exec session not running"}, status_code=404)
    subprocess.run(["tmux", "send-keys", "-t", exec_session, prompt, "Enter"], capture_output=True)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/exec-session")
async def kill_exec_session(proj_id: str):
    """Kill ALL Execute-spawned tmux sessions for a project (all agent prefixes).
    M375: was breaking after first kill — orphaned sessions from prior agent stayed alive."""
    killed_sessions = []
    for candidate in _exec_session_names(proj_id):
        result = subprocess.run(["tmux", "kill-session", "-t", candidate], capture_output=True)
        if result.returncode == 0:
            killed_sessions.append(candidate)
            _exec_idle_count.pop(candidate, None)
            _exec_was_running.pop(candidate, None)
    return {"ok": True, "killed": bool(killed_sessions), "sessions": killed_sessions}


@app.delete("/api/northstar/{proj_id}/tmux-session/{session_name}")
async def kill_named_tmux_session(proj_id: str, session_name: str):
    """M792: kill a specific named tmux session (used from session popup kill button).
    M1019: cascade — also kill child branch sessions of this mother session, and
    clear assigned_session on every substar that referenced the killed session(s).
    Killing a session ≡ unassigning mother/child links."""
    killed = []
    cleared_substars = []

    # Phase 1: figure out cascade BEFORE killing, so we can look up children.
    cascade_targets = {session_name}
    try:
        p = _db_load_project(proj_id)
        if p:
            north_stars = p.get("north_stars") or []
            # Find the substar (if any) whose assigned_session matches the requested session.
            # If that substar has no branch_from_session_id, it IS a mother; kill its branches.
            mother_ns = None
            for ns in north_stars:
                if isinstance(ns, dict) and (ns.get("assigned_session") or "") == session_name:
                    if not ns.get("branch_from_session_id"):
                        mother_ns = ns
                    break
            if mother_ns:
                # Any substar whose branch_from_session_id == mother's substar id (or session id) → child.
                mother_substar_id = mother_ns.get("id") or ""
                for ns in north_stars:
                    if not isinstance(ns, dict): continue
                    if ns is mother_ns: continue
                    bf = (ns.get("branch_from_session_id") or "").strip()
                    if bf and (bf == mother_substar_id or bf == session_name):
                        child_sess = (ns.get("assigned_session") or "").strip()
                        if child_sess:
                            cascade_targets.add(child_sess)
    except Exception:
        pass

    # Phase 2: kill all targeted sessions.
    for sname in cascade_targets:
        result = subprocess.run(["tmux", "kill-session", "-t", sname], capture_output=True)
        if result.returncode == 0:
            killed.append(sname)
        _exec_idle_count.pop(sname, None)
        _exec_was_running.pop(sname, None)

    # Phase 3: clear assigned_session on any substar that pointed at a killed session.
    try:
        p = _db_load_project(proj_id)
        if p:
            north_stars = p.get("north_stars") or []
            changed = False
            for ns in north_stars:
                if not isinstance(ns, dict): continue
                if (ns.get("assigned_session") or "") in cascade_targets:
                    ns["assigned_session"] = None
                    ns["branch_from_session_id"] = None
                    cleared_substars.append(ns.get("id") or "")
                    changed = True
            if changed:
                _db_save_project(proj_id, p)
    except Exception:
        pass

    _server_log_action(proj_id, "", "exec:kill",
                       f"session:{session_name} cascade:{','.join(sorted(cascade_targets))} cleared:{len(cleared_substars)}")
    return {"ok": bool(killed), "session": session_name,
            "killed": killed, "cleared_substars": cleared_substars}


@app.post("/api/northstar/{proj_id}/tmux-session/{session_name}/rename")
async def rename_tmux_session(proj_id: str, session_name: str, body: dict = Body(default={})):
    """M810: rename a specific tmux session to a new name."""
    new_name = (body.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="new_name required")
    result = subprocess.run(["tmux", "rename-session", "-t", session_name, new_name], capture_output=True)
    if result.returncode != 0:
        raise HTTPException(status_code=404, detail=f"Session '{session_name}' not found or rename failed")
    # Update exec tracking keys
    if session_name in _exec_idle_count:
        _exec_idle_count[new_name] = _exec_idle_count.pop(session_name)
    if session_name in _exec_was_running:
        _exec_was_running[new_name] = _exec_was_running.pop(session_name)
    _server_log_action(proj_id, "", "exec:rename", f"{session_name}→{new_name}")
    return {"ok": True, "old_name": session_name, "new_name": new_name}


@app.get("/api/northstar/{proj_id}/substar-sessions")
async def get_substar_sessions(proj_id: str):
    """M747: return running per-substar exec sessions {substar_short_id: session_name}."""
    return {"ok": True, "sessions": _get_active_substar_sessions(proj_id)}


@app.get("/api/northstar/{proj_id}/all-sessions")
async def get_all_sessions(proj_id: str):
    """M747: list all tmux sessions for manual substar session assignment."""
    try:
        r = subprocess.run(["tmux", "list-sessions", "-F", "#{session_name}"],
                           capture_output=True, text=True)
        sessions = [s.strip() for s in r.stdout.splitlines() if s.strip()]
    except Exception:
        sessions = []
    return {"ok": True, "sessions": sessions}


@app.get("/api/northstar/{proj_id}/session-contexts")
async def get_session_contexts(proj_id: str):
    """M777 + M797: return project-related tmux sessions with pane context summary.
    M797: capture-pane calls are now parallel (asyncio.gather + to_thread) — N sessions
    take ~2s total instead of N×2s (e.g. 4 sessions: 8s → 2s)."""
    import asyncio as _asyncio
    try:
        r = subprocess.run(["tmux", "list-sessions", "-F",
                            "#{session_name}\t#{session_created}\t#{session_activity}"],
                           capture_output=True, text=True)
    except Exception:
        return {"ok": True, "sessions": []}

    proj_lower = proj_id.lower()
    targets = []  # list of (sname, created_ts, activity_ts)
    for line in r.stdout.splitlines():
        parts = line.strip().split("\t")
        if not parts[0]: continue
        sname = parts[0]
        if proj_lower not in sname.lower():
            continue
        created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        activity_ts = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        targets.append((sname, created_ts, activity_ts))

    def _capture(sname):
        try:
            cp = subprocess.run(
                ["tmux", "capture-pane", "-t", sname, "-p", "-J", "-S", "-8"],
                capture_output=True, text=True, timeout=2,
            )
            lines = [l.strip() for l in cp.stdout.strip().splitlines() if l.strip()]
            return lines[-1][:120] if lines else ""
        except Exception:
            return ""

    # M797: parallel capture-pane calls so total latency = max(individual) not sum.
    summaries = await _asyncio.gather(
        *[_asyncio.to_thread(_capture, sn) for sn, _, _ in targets]
    )
    sessions = [{
        "name": sn, "session": sn,  # M889: frontend reads s.session; alias name for compatibility
        "created": ct, "activity": at,
        "summary": summary or "(no output)",
    } for (sn, ct, at), summary in zip(targets, summaries)]
    sessions.sort(key=lambda x: x["activity"], reverse=True)
    # M792: attach last_session_id from .last-session-id (main session) so branch-from-main button
    # can find the ID without an extra API call.
    _pdir = PROJECTS_DIR / proj_id
    _main_last_sid = ""
    _last_sid_f = _pdir / ".last-session-id"
    if _last_sid_f.exists():
        try:
            _main_last_sid = _last_sid_f.read_text().strip()
        except Exception:
            pass
    if not _main_last_sid:
        _hist_f = _pdir / ".session-history.json"
        if _hist_f.exists():
            try:
                _hist = json.loads(_hist_f.read_text())
                _main_last_sid = (_hist.get("_current") or _hist.get(_get_project_model_value(proj_id)) or "").strip()
            except Exception:
                pass
    return {"ok": True, "sessions": sessions, "main_last_session_id": _main_last_sid}


@app.post("/api/northstar/{proj_id}/restart-session")
async def restart_session(proj_id: str):
    """Kill PTY + tmux exec sessions for a project so the next spawn picks up
    fresh env (e.g. after a model change). Doesn't auto-respawn — next Execute
    click or terminal open will spawn with the current model env."""
    killed = {"pty": False, "tmux": False}
    # Kill PTY (interactive terminal session)
    if proj_id in _sessions:
        try:
            _kill_session(proj_id)
            killed["pty"] = True
        except Exception:
            pass
    # Kill tmux exec session
    for exec_session in _exec_session_names(proj_id):
        r = subprocess.run(["tmux", "kill-session", "-t", exec_session], capture_output=True)
        if r.returncode == 0:
            killed["tmux"] = True
            break
    return JSONResponse({"ok": True, "killed": killed,
                         "next_spawn_model": _get_project_model_value(proj_id) or "(default)"})


_SHELLS = {"bash", "zsh", "sh", "fish", "dash"}


_OSK_LOG_PATH = Path.home() / ".osk-litellm.log"
_OSK_ERROR_PATTERNS = [
    # (kind, regex, friendly_label)
    ("quota_exceeded", r"insufficient_quota|quota.*exceeded|You exceeded your current quota", "OpenAI quota exhausted — top up billing"),
    ("rate_limit",     r"RateLimitError|rate.?limit",                                         "Rate-limited by OpenAI — wait or upgrade tier"),
    ("auth",           r"AuthenticationError|Incorrect API key|401 Unauthorized",             "Auth failure — OPENAI_API_KEY invalid or missing"),
    ("upstream_5xx",   r"5\d\d (Internal|Bad Gateway|Service Unavailable|Gateway Timeout)",   "OpenAI upstream error"),
    ("timeout",        r"ReadTimeout|TimeoutError|timed out",                                 "Upstream timeout"),
]


def _scan_osk_recent_errors(window_bytes: int = 200_000) -> list:
    """Scan the tail of the LiteLLM proxy log for known upstream error classes
    (quota, rate-limit, auth, 5xx, timeout). Returns the last occurrence per
    class with a short excerpt — used by the UI to surface a banner in the
    detail card so users know why their gpt session stopped responding."""
    import re
    if not _OSK_LOG_PATH.exists():
        return []
    try:
        size = _OSK_LOG_PATH.stat().st_size
        with open(_OSK_LOG_PATH, "r", errors="replace") as f:
            if size > window_bytes:
                f.seek(size - window_bytes)
                f.readline()  # drop partial leading line
            tail = f.read()
        # Mtime of log file is good-enough freshness signal — proxy writes on every request
        log_mtime = _OSK_LOG_PATH.stat().st_mtime
        out = []
        for kind, pat, label in _OSK_ERROR_PATTERNS:
            matches = list(re.finditer(pat, tail, re.IGNORECASE))
            if not matches:
                continue
            last = matches[-1]
            start = max(0, last.start() - 60)
            end = min(len(tail), last.end() + 200)
            excerpt = tail[start:end].replace("\n", " ").strip()[:280]
            out.append({"kind": kind, "label": label, "count": len(matches),
                        "excerpt": excerpt, "log_mtime": log_mtime})
        return out
    except Exception:
        return []


@app.get("/api/osk/health")
async def osk_health():
    """Health check for the OSK LiteLLM proxy + recent-error scan. UI uses
    `ok` to grey out gpt-* options when proxy is down, and `recent_errors`
    to surface upstream failures (quota, rate-limit, auth) in the detail card."""
    import urllib.request
    out: dict = {"url": _OSK_PROXY_URL}
    try:
        t0 = time.time()
        req = urllib.request.Request(_OSK_PROXY_URL + "/health/liveliness")
        with urllib.request.urlopen(req, timeout=1.0) as r:
            out["ok"] = (r.status == 200)
        out["latency_ms"] = int((time.time() - t0) * 1000)
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    errs = _scan_osk_recent_errors()
    if errs:
        out["recent_errors"] = errs
    return JSONResponse(out)


@app.get("/api/dsk/health")
async def dsk_health():
    """Health check for Darwin-28B bridge on localhost:8860 (SSH tunnel → NIPA:8850)."""
    import urllib.request
    out: dict = {"url": _DSK_PROXY_URL}
    try:
        t0 = time.time()
        req = urllib.request.Request(_DSK_PROXY_URL + "/health")
        with urllib.request.urlopen(req, timeout=2.0) as r:
            out["ok"] = (r.status == 200)
        out["latency_ms"] = int((time.time() - t0) * 1000)
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    return JSONResponse(out)


_exec_was_running: dict[str, bool] = {}  # track exec session busy/idle transitions for ntfy
_exec_idle_count: dict[str, int] = {}   # M319: debounce — require 2 consecutive idle readings before notifying
_exec_notified: dict[str, float] = {}   # M998: sessions already notified — reset on running detection
_last_idle_push: dict[str, float] = {}  # M378: dedup — suppress duplicate session_idle SSE within 5s per session
_IDLE_PUSH_COOLDOWN = 5.0  # seconds — minimum gap between session_idle SSE pushes for same session


# M389 fix: server-side background idle detector — runs independently of client polling
# This ensures Telegram notifications fire even when no browser tab is open.
@app.on_event("startup")
async def _start_exec_idle_detector():
    """Server-side background task: detect exec session idle transitions every 5s.
    Previously relied solely on client /api/exec-sessions polling — broke when browser closed."""
    import asyncio as _aio, re as _re
    async def _detect():
        while True:
            await _aio.sleep(5)
            try:
                result = subprocess.run(
                    ["tmux", "list-sessions", "-F", "#{session_name}:#{session_created}"],
                    capture_output=True, text=True
                )
                alive = set()
                for line in result.stdout.splitlines():
                    parts = line.split(":", 1)
                    session_name = parts[0] if parts else ""
                    if not any(session_name.startswith(p) for p in ("claude-exec-", "codex-exec-", "openrouter-exec-")):
                        continue
                    proj_id = ""
                    for pfx in ("claude-exec-", "codex-exec-", "openrouter-exec-"):
                        if session_name.startswith(pfx):
                            proj_id = session_name[len(pfx):]
                            break
                    if not proj_id:
                        continue
                    alive.add(session_name)
                    # Check if runtime is alive
                    pane_cmds = subprocess.run(
                        ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                        capture_output=True, text=True
                    ).stdout.splitlines()
                    # M916 fix: only skip sessions that NEVER ran (not in _exec_was_running)
                    # Old code skipped when pane_cmd=bash (idle state) — prevented idle notification
                    _has_been_running = session_name in _exec_was_running
                    if not _has_been_running and not any(c.strip() and c.strip() not in {"bash","zsh","sh","fish","dash"} for c in pane_cmds):
                        continue
                    # Detect spinner
                    pane_out = subprocess.run(
                        ["tmux", "capture-pane", "-p", "-t", session_name],
                        capture_output=True, text=True
                    ).stdout
                    clean = _re.sub(r'\x1b\[[0-9;]*[mKHJ]', '', pane_out)
                    # M396: busy = spinner OR "esc to interrupt" (feedback dialog hides spinner)
                    idle = "… (" not in clean and "esc to i" not in clean
                    # M389/M434: fire Telegram ONLY on running→idle transition (same state as toast)
                    _was_running = _exec_was_running.get(session_name, False)
                    if not idle:
                        _exec_was_running[session_name] = True
                        _exec_idle_count.pop(session_name, None)
                        _exec_idle_file(proj_id).unlink(missing_ok=True)  # M536: clear when busy
                    else:
                        _exec_idle_count[session_name] = _exec_idle_count.get(session_name, 0) + 1
                    _consec_idle = _exec_idle_count.get(session_name, 0)
                    if _was_running and idle and _consec_idle >= 2:
                        _exec_was_running[session_name] = False
                        _push_session_idle(session_name, proj_id)  # SSE toast
                        _send_ntfy_notification(             # Telegram — same condition as toast
                            f"{proj_id} exec idle",
                            f"Exec session for {proj_id} just went idle",
                            priority="default"
                        )
                # Clean up stale entries — M460 fix: notify if session disappears while running
                for k in list(_exec_was_running.keys()):
                    if k not in alive:
                        was_running = _exec_was_running.pop(k, False)
                        _exec_idle_count.pop(k, None)
                        if was_running:
                            # Session died while running → fire missed idle notification
                            _proj = ""
                            for pfx in ("claude-exec-", "codex-exec-", "openrouter-exec-"):
                                if k.startswith(pfx):
                                    _proj = k[len(pfx):]
                                    break
                            if _proj:
                                _push_session_idle(k, _proj)
                                _send_ntfy_notification(
                                    f"{_proj} exec idle",
                                    f"Exec session for {_proj} just went idle",
                                    priority="default"
                                )
            except Exception:
                pass
    # M460 cold-start fix: pre-scan existing exec sessions at startup.
    # Sessions already running → mark _exec_was_running=True so we catch their next idle.
    # Sessions already idle at startup → skip (we missed the transition, can't go back).
    import re as _re_cs
    try:
        _cs_result = subprocess.run(
            ["tmux", "list-sessions", "-F", "#{session_name}"],
            capture_output=True, text=True)
        for _cs_sname in _cs_result.stdout.splitlines():
            if not any(_cs_sname.startswith(p) for p in ("claude-exec-", "codex-exec-", "openrouter-exec-")):
                continue
            _cs_pane = subprocess.run(
                ["tmux", "capture-pane", "-p", "-t", _cs_sname],
                capture_output=True, text=True).stdout
            _cs_clean = _re_cs.sub(r'\x1b\[[0-9;]*[mKHJ]', '', _cs_pane)
            if "… (" in _cs_clean or "esc to i" in _cs_clean:
                _exec_was_running[_cs_sname] = True  # currently running → will detect idle
    except Exception:
        pass
    asyncio.create_task(_detect())


def _push_session_idle(session_name: str, proj_id: str) -> bool:
    """M378: Deduplicated session_idle push. Returns True if pushed, False if suppressed by cooldown."""
    now = time.time()
    last = _last_idle_push.get(session_name, 0)
    if now - last < _IDLE_PUSH_COOLDOWN:
        return False  # suppress duplicate
    _last_idle_push[session_name] = now
    _ns_push("session_idle", proj_id=proj_id, kind="exec")
    return True


@app.get("/api/exec-sessions")
async def get_exec_sessions():
    """Return agent-exec-* tmux sessions where the runtime is actually running."""
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"],
        capture_output=True, text=True
    )
    sessions = []
    for line in result.stdout.splitlines():
        parts = line.split(":", 2)
        session_name = parts[0] if parts else ""
        agent = ""
        proj_id = ""
        for prefix, prefix_agent in (("claude-exec-", "claude"), ("codex-exec-", "codex"), ("openrouter-exec-", "openrouter")):
            if session_name.startswith(prefix):
                agent = prefix_agent
                proj_id = session_name[len(prefix):]
                break
        if not proj_id:
            continue
        created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

        # Check if the runtime is actually running — not just a shell prompt after exit
        pane_cmds = subprocess.run(
            ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
            capture_output=True, text=True
        ).stdout.splitlines()
        cmds = [c.strip() for c in pane_cmds if c.strip()]
        runtime_running = any(c not in _SHELLS for c in cmds) if cmds else False
        if not runtime_running:
            # M345 v2: fire ntfy when tracked session exits — check key EXISTENCE not value
            # Bug was: idle-at-prompt sets value=False, so _was_running_before was False → no notify
            _was_tracked = session_name in _exec_was_running
            _exec_was_running.pop(session_name, None)
            _exec_idle_count.pop(session_name, None)
            if _was_tracked:
                _ns_push("session_idle", proj_id=proj_id, kind="exec")
                _send_ntfy_notification(f"{proj_id} exec done", f"Exec session for {proj_id} finished/exited", priority="high")
            continue

        # M97/M183: detect busy first (positive signal), then idle is the complement.
        # The unique busy markers are "esc to interrupt" and the spinner timer
        # pattern "… (". Same rules as the M148 wake-detection.
        # M364: use tmux visible viewport (no -S flag) — captures what's currently on screen
        # including spinner in header area. -S flags miss spinner when scrolled past viewport.
        pane_out = subprocess.run(
            ["tmux", "capture-pane", "-p", "-t", session_name],
            capture_output=True, text=True
        ).stdout
        import re as _re
        clean = _re.sub(r'\x1b\[[0-9;]*[mKHJ]', '', pane_out)
        # M371: use spinner-only for both exec_status AND ntfy — avoids all-projects-live from M366
        # SSE session_running handles instant green on spawn; spinner handles ongoing state
        # M396: busy = spinner OR "esc to interrupt" (active even when feedback dialog shown)
        busy_for_ntfy = "… (" in clean or "esc to i" in clean
        idle_for_ntfy = not busy_for_ntfy
        idle = idle_for_ntfy  # unified: spinner/interrupt = busy, otherwise = idle

        # M388: startup grace period — sessions < 30s old without spinner are "starting up",
        # not idle. Prevents poll from overwriting SSE optimistic LIVE state during Claude init.
        if idle and created_ts > 0:
            session_age_s = time.time() - created_ts
            if session_age_s < 30:
                idle = False  # treat as running until startup completes

        from datetime import datetime as _dt
        # Surface the resume flag used at spawn time so UI can show whether this
        # live session is continuing prior stone work or started fresh.
        spawn_mode = None
        spawn_from = None
        spawn_info_file = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
        if spawn_info_file.exists():
            try:
                si = json.loads(spawn_info_file.read_text())
                si_at = si.get("at", "")
                # Only trust spawn_info if it was written at/after this tmux session's creation
                si_epoch = 0
                if si_at:
                    try:
                        si_epoch = int(_dt.fromisoformat(si_at).timestamp())
                    except Exception:
                        si_epoch = 0
                # Tight window: spawn_info must be within ±120s of tmux session creation.
                # Prevents matching a later PTY/respawn write to an older tmux session.
                if si_epoch and abs(si_epoch - created_ts) <= 120:
                    spawn_mode = si.get("mode")
                    spawn_from = si.get("from_id") or None
                    spawn_model = si.get("model") or ""
                else:
                    spawn_model = si.get("model") or ""  # always include model even if time mismatch
            except Exception:
                spawn_model = ""
        else:
            spawn_model = ""
        # v0.2.4: single-transition detection with 2-read idle debounce.
        # M378 false-positive fix: user typing "go" shows brief prompt (no spinner) then
        # spinner appears — without debounce this fires session_idle toast immediately.
        # Require 2 consecutive idle readings (2×3s = 6s) before firing session_idle SSE.
        _was_running = _exec_was_running.get(session_name, False)  # M892: default False → no false idle ntfy on hub restart
        _exec_was_running[session_name] = not idle
        if idle:
            _exec_idle_count[session_name] = _exec_idle_count.get(session_name, 0) + 1
        else:
            _exec_idle_count.pop(session_name, None)
        _consec_idle = _exec_idle_count.get(session_name, 0)
        if idle and (_was_running and _consec_idle >= 2 or _consec_idle >= 3):
            # M998 fix: fire for sessions that ran too quickly to be caught (_consec_idle>=3 fallback)
            _already_notified = session_name in _exec_notified
            if not _already_notified:
                _push_session_idle(session_name, proj_id)
                _send_ntfy_notification(f"{proj_id} exec idle", f"Exec session for {proj_id} just went idle", priority="default")
                _exec_notified[session_name] = time.time()
        elif not idle:
            # session running → reset notified flag so next idle can fire
            _exec_notified.pop(session_name, None)
        if not _was_running and not idle:
            # idle→running: push SSE so detail-card updates within 3s poll
            _exec_idle_count.pop(session_name, None)  # reset idle count on running
            _ns_push("session_running", proj_id=proj_id, kind="exec")

        # M365: find live_session_id = newest transcript modified after session spawn
        _live_session_id = ""
        try:
            _base = Path.home() / ".claude" / "projects"
            # Find the transcript dir for this project (case-insensitive suffix match)
            _candidates = [d for d in _base.iterdir() if d.name.lower().endswith(f"-project-{proj_id.lower()}")]
            if not _candidates:
                _candidates = [d for d in _base.iterdir() if proj_id.lower() in d.name.lower() and "project" in d.name.lower()]
            if _candidates:
                _transcript_dir = sorted(_candidates, key=lambda d: len(d.name))[0]  # shortest match
                _all = sorted(_transcript_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
                # The live session = most recently written file that was modified AFTER session creation
                for _f in _all:
                    if _f.stat().st_mtime > created_ts:
                        _live_session_id = _f.stem
                        break
        except Exception:
            pass
        sessions.append({
            "session": session_name,
            "proj_id": proj_id,
            "agent": agent,
            "created": _dt.fromtimestamp(created_ts).isoformat() if created_ts else "",
            "alive": True,
            "idle": idle,
            "spawn_mode": spawn_mode,
            "spawn_from": spawn_from,
            "live_session_id": _live_session_id,  # M365: actual running session ID
            "model": spawn_model,  # M189: show model in exec session panel
        })
    # Clean up stale entries from _exec_was_running
    _alive_sessions = {s.get("session") for s in sessions if s.get("session")}
    for k in list(_exec_was_running.keys()):
        if k not in _alive_sessions:
            del _exec_was_running[k]
    for k in list(_exec_idle_count.keys()):
        if k not in _alive_sessions:
            del _exec_idle_count[k]
    for k in list(_exec_notified.keys()):
        if k not in _alive_sessions:
            del _exec_notified[k]
    return JSONResponse({"ok": True, "sessions": sessions})


@app.get("/api/northstar/{proj_id}/session-history")
async def get_session_history(proj_id: str):
    """Return available Claude sessions for this project from .session-history.json.
    Returns both per-model sessions and shared (_current) session."""
    pdir = PROJECTS_DIR / proj_id
    hist_file = pdir / ".session-history.json"

    if not hist_file.exists():
        return JSONResponse({"ok": True, "sessions": []})

    try:
        hist = json.loads(hist_file.read_text())
    except Exception:
        return JSONResponse({"ok": True, "sessions": []})

    # Read last session ID file for additional context
    last_sid_file = pdir / ".last-session-id"
    last_sid = last_sid_file.read_text().strip() if last_sid_file.exists() else None

    sessions = []
    for key, sid in hist.items():
        if not sid:
            continue

        # Determine session type
        if key == "_current":
            session_type = "shared"
            model = None
        else:
            session_type = "isolated"
            model = key if key != "_default" else None

        sessions.append({
            "id": sid,
            "model": model,
            "type": session_type,
            "key": key,
            "is_current": sid == last_sid
        })

    return JSONResponse({"ok": True, "sessions": sessions})


@app.get("/api/northstar/{proj_id}/resumable-sessions")
async def get_resumable_sessions(proj_id: str, agent: str = "", model: str = ""):
    """Return resumable sessions for this project, grouped by agent+model.

    Includes live exec sessions, recent transcript sessions, and a "fresh" option.
    Query params:
      agent: "claude" | "codex" | "" (empty = all)
      model: filter by model key or "" for all
    """
    pdir = PROJECTS_DIR / proj_id
    result = {"ok": True, "groups": []}

    # Determine which agents to include
    # M462: unknown agents (dsk, etc.) return empty — no cross-agent session leakage
    agents = []
    if agent in ("claude", "codex", "openrouter"):
        agents = [agent]
    elif agent:
        agents = []  # known unknown agent (e.g. dsk) — no sessions to show
    else:
        agents = ["claude", "codex", "openrouter"]

    # Read session history
    hist_file = pdir / ".session-history.json"
    hist = {}
    if hist_file.exists():
        try:
            hist = json.loads(hist_file.read_text())
        except Exception:
            pass

    # Read live exec sessions + capture spawn time for transcript scan
    live_by_agent = set()
    live_spawn_ts: dict[str, float] = {}  # agent → tmux session created epoch
    try:
        tmux_out = subprocess.run(
            ["tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"],
            capture_output=True, text=True, timeout=3
        )
        for line in tmux_out.stdout.splitlines():
            parts = line.split(":", 2)
            sname = parts[0] if parts else ""
            created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            for prefix, prefix_agent in (("claude-exec-", "claude"), ("codex-exec-", "codex"), ("openrouter-exec-", "openrouter")):
                if sname.startswith(prefix):
                    s_proj = sname[len(prefix):]
                    if s_proj == proj_id:
                        # M342 fix: only mark live if the runtime is actually running (not a dead shell)
                        pane_cmds = subprocess.run(
                            ["tmux", "list-panes", "-t", sname, "-F", "#{pane_current_command}"],
                            capture_output=True, text=True, timeout=2
                        ).stdout.splitlines()
                        runtime_running = any(c.strip() and c.strip() not in _SHELLS for c in pane_cmds) if pane_cmds else False
                        if runtime_running:
                            live_by_agent.add(prefix_agent)
                            live_spawn_ts[prefix_agent] = float(created_ts)
                    break
    except Exception:
        pass

    # M280: detect current live exec session ID by scanning transcript dir for
    # JSONL files created AFTER the exec session spawn time.
    live_exec_sid: dict[str, str] = {}  # agent → session_id
    if live_by_agent:
        try:
            proj_dir_path = _get_project_dir(proj_id)
            if proj_dir_path:
                encoded = _encode_cwd_for_claude(str(proj_dir_path))
                transcript_dir = Path.home() / ".claude" / "projects" / encoded
                if transcript_dir.exists():
                    for ag in live_by_agent:
                        spawn_ts = live_spawn_ts.get(ag, 0)
                        if not spawn_ts:
                            continue
                        best_sid, best_mtime = None, 0.0
                        for f in transcript_dir.glob("*.jsonl"):
                            mt = f.stat().st_mtime
                            if mt >= spawn_ts - 5 and mt > best_mtime and f.stat().st_size > 0:
                                best_mtime = mt
                                best_sid = f.stem
                        if best_sid:
                            live_exec_sid[ag] = best_sid
        except Exception:
            pass

    # Build groups per agent
    # M405: track which agents have had exec_live injected — inject only into first model group
    _exec_live_injected: set[str] = set()
    for ag in agents:
        group = {"agent": ag, "models": []}

        # Determine models for this agent
        if ag == "codex":
            agent_models = [
                {"key": "", "label": "codex-auto"},
                {"key": "codex-haiku", "label": "haiku"},
                {"key": "codex-sonnet", "label": "sonnet"},
            ]
        elif ag == "openrouter":
            agent_models = [
                {"key": "or-hy3-preview", "label": "hy3-preview"},
                {"key": "or-owl-alpha", "label": "owl-alpha"},
                {"key": "or-grok-3", "label": "grok-3"},
                {"key": "or-grok-3-mini", "label": "grok-3-mini"},
                {"key": "or-kimi-k2", "label": "kimi-k2.6"},
                {"key": "or-gemini3-flash", "label": "gemini-3-flash"},
                {"key": "or-gemini-flash", "label": "gemini-2.5-flash"},
                {"key": "or-deepseek-v4-flash", "label": "deepseek-v4-flash"},
                {"key": "or-nemotron", "label": "nemotron (free)"},
                {"key": "or-nemotron-nano", "label": "nemotron-nano (free)"},
            ]
        else:
            agent_models = [
                {"key": "", "label": "sonnet-4.6"},
                {"key": "claude-haiku-4-5-20251001", "label": "Haiku 4.5"},
                {"key": "claude-sonnet-4-5-20250929", "label": "Sonnet 4.5"},
                {"key": "claude-sonnet-4-6", "label": "Sonnet 4.6"},
                {"key": "claude-opus-4-7", "label": "Opus 4.7"},
                {"key": "claude-opus-4-8", "label": "Opus 4.8"},
            ]

        # Filter by model if specified
        if model:
            agent_models = [m for m in agent_models if m["key"] == model]

        for m in agent_models:
            mkey = m["key"]
            sessions = []

            # Check transcript sessions
            if ag == "codex":
                seen_sids = set()
                # M372: exclude SIDs owned by openrouter model keys
                _or_sids = {v.strip() for k, v in hist.items() if k.startswith("or-")}
                for hist_key in ([mkey] if mkey else ["_current", "_default"]):
                    sid = (hist.get(hist_key) or "").strip()
                    if sid and sid not in seen_sids and sid not in _or_sids:
                        seen_sids.add(sid)
                        sessions.append({
                            "id": sid,
                            "type": "transcript",
                            "label": f"Session {sid[:8]}…",
                            "model": mkey,
                        })
            else:
                # Claude: check per-model and _current; deduplicate by session id
                # M217: exclude _interactive key from exec resume list — interactive sessions
                # M279: _current/_default only for claude agent — OR/codex have their own model keys
                seen_sids = set()
                # M372/M406: _or_codex_sids exclusion only applies to CLAUDE group — prevents OR sessions
                # from leaking into claude's resume list via _current/_default. For openrouter/codex
                # groups, they must see their OWN sessions (not exclude them).
                _or_codex_sids = (
                    {v.strip() for k, v in hist.items() if k.startswith("or-") or k.startswith("codex-")}
                    if ag == "claude" else set()
                )
                if mkey:
                    # M316: also include {mkey}_prev to preserve old session when new one spawned
                    hist_keys = [mkey, f"{mkey}_prev"]
                else:
                    # M323: include _current for empty model key — this is the default claude session.
                    # _current is set by _update_session_history_from_transcript only on claude spawns,
                    # so cross-agent contamination is minimal. Previously skipped (M279) causing
                    # the default session to disappear after tmux kill.
                    hist_keys = ["_current", "_default"]
                encoded_path = _encode_cwd_for_claude(str(_get_project_dir(proj_id) or str(Path.home())))
                for hist_key in hist_keys:
                    if hist_key == "_default" and mkey:
                        continue
                    sid = (hist.get(hist_key) or "").strip()
                    # M372: skip SIDs that belong to openrouter/codex in claude group only
                    if sid and sid not in seen_sids and sid not in _or_codex_sids:
                        seen_sids.add(sid)
                        t = Path.home() / ".claude" / "projects" / encoded_path / f"{sid}.jsonl"
                        if t.exists() and t.stat().st_size > 0:
                            sessions.append({
                                "id": sid,
                                "type": "transcript",
                                "label": f"Session {sid[:8]}…",
                                "model": mkey,
                                "activity": t.stat().st_mtime,  # M1018: unix epoch for last-used timestamp
                            })

            # M280/M405: inject live exec session row into first model group for the agent.
            # Previously used mkey=="" which worked for claude (empty-key default) but failed for
            # openrouter (all keys are non-empty like "or-hy3-preview"). Now uses a per-agent
            # injected-set so exec_live appears in the first model group regardless of key name.
            live_sid = live_exec_sid.get(ag)
            if live_sid and ag in live_by_agent and ag not in _exec_live_injected:
                if not any(s["id"] == live_sid for s in sessions):
                    # M283: include tmux_session name so UI can attach directly
                    _agent_prefix = {"claude": "claude-exec-", "codex": "codex-exec-", "openrouter": "openrouter-exec-"}.get(ag, "claude-exec-")
                    sessions.insert(0, {
                        "id": live_sid,
                        "type": "exec_live",
                        "label": f"{live_sid[:12]}",
                        "model": mkey,
                        "tmux_session": f"{_agent_prefix}{proj_id}",
                    })
                    _exec_live_injected.add(ag)

            # Add fresh option
            sessions.append({
                "id": "fresh",
                "type": "fresh",
                "label": "✦ Fresh session",
                "model": mkey,
            })

            group["models"].append({
                "model_key": mkey,
                "model_label": m["label"],
                "has_live": ag in live_by_agent,
                "sessions": sessions,
            })

        result["groups"].append(group)

    # M189: include current running PTY session info so the UI can display spawn options
    proc = _sessions.get(proj_id)
    if proc and proc.isalive():
        result["current_pty"] = {
            "agent": _pty_agents.get(proj_id, "claude"),
            "session_id": _pty_session_ids.get(proj_id),
            "model": _get_project_model_value(proj_id) or "",
            "type": "resume" if _pty_session_ids.get(proj_id) else "fresh",
        }
    else:
        result["current_pty"] = None

    # M189 follow-up: include exec session spawn options from .last-spawn-info.json
    spawn_file = pdir / ".last-spawn-info.json"
    if spawn_file.exists():
        try:
            ce = json.loads(spawn_file.read_text())
            # M220: detect live session ID from transcript dir (newest JSONL newer than spawn)
            try:
                proj_dir = _get_project_dir(proj_id) or str(Path.home())
                encoded = _encode_cwd_for_claude(str(proj_dir))
                transcripts_dir = Path.home() / ".claude" / "projects" / encoded
                spawn_mtime = spawn_file.stat().st_mtime
                if transcripts_dir.exists():
                    candidates = [
                        f for f in transcripts_dir.glob("*.jsonl")
                        if f.stat().st_mtime > spawn_mtime and f.stat().st_size > 0
                    ]
                    if candidates:
                        newest = max(candidates, key=lambda f: f.stat().st_mtime)
                        ce["live_session_id"] = newest.stem
            except Exception:
                pass
            result["current_exec"] = ce
        except Exception:
            result["current_exec"] = None
    else:
        result["current_exec"] = None

    return JSONResponse(result)


@app.get("/api/northstar/sessions")
async def ns_sessions():
    """Return terminal session status for all projects.

    M181: PTY `active`/`idle` semantics now match tmux exec — based on whether
    Claude inside the PTY is actively processing (busy spinner), not whether
    the WS is currently connected. Detection mirrors /api/exec-sessions:
    inspect last ~4 KB of the PTY scrollback buffer; if a busy signature
    (`esc to interrupt` / `… (`) is present, mark `active`; else `idle:N`.
    Dead PTYs map to no entry. WS-not-attached PTYs are STILL tracked.
    """
    import re as _re_ns
    result = {}
    now = time.time()

    for proj_id, proc in list(_sessions.items()):
        if not proc.isalive():
            continue
        # M181: PTY busy detection via timestamp recorded by reader loop on each
        # chunk containing "esc to interrupt" or "… (". Claude redraws ~5x/sec
        # during busy, so 3s staleness threshold survives a single missed chunk.
        # Buffer tail-scan is intentionally NOT used as a fallback because the
        # buffer is append-only — historical busy markers persist forever and
        # would cause permanent false-positive busy state.
        last_busy = _pty_last_busy_ts.get(proj_id, 0)
        agent = _pty_agents.get(proj_id, "claude")  # M163: track agent type (claude/codex)
        if last_busy and now - last_busy < 3:
            result[proj_id] = f"active:{agent}"
        else:
            idle_since = _session_idle_since.get(proj_id) or now
            result[proj_id] = f"idle:{int(now - idle_since)}:{agent}"

    # Override with explicit hook-set status (WAITING, DONE, etc.)
    for proj_id, status in _pill_status.items():
        if status == "WAITING":
            result[proj_id] = "waiting"
        elif status == "DONE" and proj_id not in result:
            result[proj_id] = "done"
        elif status == "IDLE" and proj_id not in result:
            pass  # IDLE is the default (no entry)

    return JSONResponse(result)


@app.get("/api/northstar/resume-info")
async def ns_resume_info():
    """Per-project resume-data inventory for the swimlane badge.

    Reads .last-session-id (written by northstar-stop-inject Stop hook) and
    checks for the matching transcript jsonl. Returns one entry per project
    that has resume data — projects with no data are omitted.
    """
    from datetime import datetime as _dt
    result = {}
    if not PROJECTS_DIR.exists():
        return JSONResponse(result)
    for proj_dir in PROJECTS_DIR.iterdir():
        if not proj_dir.is_dir():
            continue
        sid_file = proj_dir / ".last-session-id"
        if not sid_file.exists():
            continue
        try:
            sid = sid_file.read_text().strip()
        except Exception:
            continue
        if not sid:
            continue
        try:
            mtime = sid_file.stat().st_mtime
        except Exception:
            continue
        # Resolve cwd → encoded transcript path
        cwd = _get_project_dir(proj_dir.name) or ""
        has_transcript = False
        if cwd:
            encoded = _encode_cwd_for_claude(cwd)
            transcript = Path.home() / ".claude" / "projects" / encoded / f"{sid}.jsonl"
            has_transcript = transcript.exists()
        result[proj_dir.name] = {
            "session_id_preview": sid[:8],
            "at": _dt.fromtimestamp(mtime).isoformat(timespec="seconds"),
            "at_epoch": int(mtime),
            "has_transcript": has_transcript,
        }
    return JSONResponse(result)


@app.get("/api/northstar/{proj_id}/sessions")
async def list_project_sessions(proj_id: str):
    """List available sessions for a project, with their associated models.

    Scans:
    1. .session-history.json — per-model session mapping (cur model's session highlighted)
    2. Transcript directory — all .jsonl files, with model detection from content
    3. .last-session-id — legacy fallback

    Returns sessions sorted by mtime descending, plus a "fresh" option.
    The UI replaces the model picker with this session picker.
    """
    from datetime import datetime as _dt
    agent = _get_project_agent_value(proj_id)
    result = {"sessions": [], "fresh": True, "current_model": _get_project_model_value(proj_id), "agent": agent}

    proj_dir = PROJECTS_DIR / proj_id
    if not proj_dir.exists():
        return JSONResponse(result)

    if agent == "codex":
        last_spawn = proj_dir / ".last-spawn-info.json"
        if last_spawn.exists():
            try:
                si = json.loads(last_spawn.read_text())
                if (si.get("mode") or "") != "fresh":
                    at = si.get("at") or ""
                    try:
                        at_iso = _dt.fromisoformat(at).isoformat(timespec="seconds") if at else ""
                        at_mtime = _dt.fromisoformat(at).timestamp() if at else time.time()
                    except Exception:
                        at_iso = ""
                        at_mtime = time.time()
                    result["sessions"] = [{
                        "session_id": "last",
                        "session_id_preview": "last",
                        "mtime": at_mtime,
                        "mtime_iso": at_iso,
                        "model": _get_project_model_value(proj_id),
                        "source": "synthetic",
                        "is_last": True,
                    }]
            except Exception:
                pass
        return JSONResponse(result)

    cwd = _get_project_dir(proj_id) or ""
    if not cwd:
        return JSONResponse(result)

    encoded = _encode_cwd_for_claude(cwd)
    transcripts_dir = Path.home() / ".claude" / "projects" / encoded
    if not transcripts_dir.exists():
        return JSONResponse(result)

    # Collect sessions from transcript files
    sessions = {}
    for jf in transcripts_dir.glob("*.jsonl"):
        sid = jf.stem
        mtime = jf.stat().st_mtime
        # Try to detect model from the first few lines of the transcript
        model = _detect_session_model(jf)
        sessions[sid] = {
            "session_id": sid,
            "session_id_preview": sid[:8],
            "mtime": mtime,
            "mtime_iso": _dt.fromtimestamp(mtime).isoformat(timespec="seconds"),
            "model": model,
            "source": "transcript",
        }

    # Overlay model info from .session-history.json
    hist_file = proj_dir / ".session-history.json"
    if hist_file.exists():
        try:
            hist = json.loads(hist_file.read_text())
            for model_key, sid in hist.items():
                if sid in sessions:
                    sessions[sid]["model"] = model_key if model_key != "_default" else ""
                    sessions[sid]["source"] = "history"
        except Exception:
            pass

    # Mark the last-known session
    last_id_file = proj_dir / ".last-session-id"
    last_sid = ""
    if last_id_file.exists():
        try:
            last_sid = last_id_file.read_text().strip()
        except Exception:
            pass
    if last_sid in sessions:
        sessions[last_sid]["is_last"] = True

    # Sort by mtime descending
    sorted_sessions = sorted(sessions.values(), key=lambda x: x["mtime"], reverse=True)
    result["sessions"] = sorted_sessions
    return JSONResponse(result)


def _detect_session_model(transcript_path: Path, max_lines: int = 50) -> str:
    """Detect which model was used in a Claude session transcript.

    Scans the first max_lines for model-related fields in the JSONL entries.
    Returns the model string, or empty string if undetectable.
    """
    try:
        with transcript_path.open("r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    # Check for model field in assistant messages or system entries
                    if isinstance(entry, dict):
                        # Sometimes model appears in the top-level or nested structures
                        model = entry.get("model") or ""
                        if not model and isinstance(entry.get("message"), dict):
                            model = entry["message"].get("model") or ""
                        if not model and isinstance(entry.get("response"), dict):
                            model = entry["response"].get("model") or ""
                        if model:
                            return str(model)
                except Exception:
                    continue
    except Exception:
        pass
    return ""


@app.get("/health/{service}")
async def health(service: str):
    if service in ("northstar", "market-signals"):
        return JSONResponse({"ok": True})
    # M60: ctx is fully integrated — mounted at /ctx, always available if hub is up
    if service == "ctx":
        return JSONResponse({"ok": True, "status": 200, "mode": "integrated"})
    if False:  # dead code — kept for reference
        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                r = await client.get("http://127.0.0.1:8787/ping")
                return JSONResponse({"ok": r.status_code < 500, "status": r.status_code})
        except Exception:
            return JSONResponse({"ok": False, "status": 0})
    svc = SERVICES.get(service)
    if not svc:
        return JSONResponse({"ok": False, "error": "unknown service"}, status_code=404)
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            r = await client.get(svc["url"])
            return JSONResponse({"ok": r.status_code < 500, "status": r.status_code})
    except Exception:
        return JSONResponse({"ok": False, "status": 0})


@app.get("/api/corpus/skills-agents")
async def corpus_skills_agents():
    """List local Claude skills (~/.claude/skills/*/SKILL.md) and agents (~/.claude/agents/*.md).

    Surfaces the corpus naturally on the Hub Corpus page so the panel stays useful
    when the entity-corpus server (8989) is offline.
    """
    home = Path.home()
    skills_dir = home / ".claude" / "skills"
    agents_dir = home / ".claude" / "agents"

    def _frontmatter(p: Path) -> dict:
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return {}
        if not text.startswith("---"):
            return {}
        lines = text.splitlines(keepends=True)
        end = None
        for i, line in enumerate(lines[1:], 1):
            if line.rstrip("\r\n") == "---":
                end = i
                break
        if end is None:
            return {}
        try:
            return _yaml.safe_load("".join(lines[1:end])) or {}
        except Exception:
            return {}

    skills = []
    if skills_dir.is_dir():
        for p in sorted(skills_dir.iterdir(), key=lambda x: x.name.lower()):
            if not p.is_dir():
                continue
            md = p / "SKILL.md"
            if not md.is_file():
                continue
            fm = _frontmatter(md)
            skills.append({
                "name": (fm.get("name") or p.name).strip(),
                "dir": p.name,  # M944: directory name for delete API
                "description": (fm.get("description") or "").strip(),
                "source": "local",
            })

    # M1023: also discover plugin-installed skills at ~/.claude/plugins/cache/*/*/<version>/skills/<name>/SKILL.md
    plugins_cache = home / ".claude" / "plugins" / "cache"
    if plugins_cache.is_dir():
        for plugin_dir in sorted(plugins_cache.iterdir(), key=lambda x: x.name.lower()):
            if not plugin_dir.is_dir():
                continue
            # Walk: <plugin>/<plugin>/<version>/skills/
            for inner in plugin_dir.iterdir():
                if not inner.is_dir():
                    continue
                for ver in inner.iterdir():
                    if not ver.is_dir():
                        continue
                    plugin_skills = ver / "skills"
                    if not plugin_skills.is_dir():
                        continue
                    for p in sorted(plugin_skills.iterdir(), key=lambda x: x.name.lower()):
                        if not p.is_dir():
                            continue
                        md = p / "SKILL.md"
                        if not md.is_file():
                            continue
                        fm = _frontmatter(md)
                        skills.append({
                            "name": (fm.get("name") or p.name).strip(),
                            "dir": f"plugin:{plugin_dir.name}/{p.name}",
                            "description": (fm.get("description") or "").strip(),
                            "source": f"plugin:{plugin_dir.name}@{ver.name}",
                        })

    # M1023: final alphabetical sort so local + plugin skills interleave correctly in UI
    skills.sort(key=lambda s: s.get("name", "").lower())

    agents = []
    if agents_dir.is_dir():
        for p in sorted(agents_dir.iterdir(), key=lambda x: x.name.lower()):
            if not p.is_file() or p.suffix.lower() != ".md":
                continue
            fm = _frontmatter(p)
            agents.append({
                "name": (fm.get("name") or p.stem).strip(),
                "description": (fm.get("description") or "").strip(),
            })

    return JSONResponse({
        "skills": skills,
        "agents": agents,
        "counts": {"skills": len(skills), "agents": len(agents)},
    })


@app.delete("/api/skill/{skill_name}")
async def delete_skill(skill_name: str):
    """M944: Delete a skill by backing up its SKILL.md (directory preserved)."""
    import re as _re
    if not _re.match(r'^[\w\-]+$', skill_name):
        return JSONResponse({"ok": False, "error": "invalid skill name"}, status_code=400)
    skill_dir = Path.home() / ".claude" / "skills" / skill_name
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.is_file():
        return JSONResponse({"ok": False, "error": "SKILL.md not found"}, status_code=404)
    bak = skill_dir / "SKILL.md.bak"
    try:
        skill_md.rename(bak)
        return JSONResponse({"ok": True, "backed_up_to": str(bak)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/skill-content/{skill_name}")
async def get_skill_content(skill_name: str):
    """M692: Return the raw SKILL.md content for a named skill/agent."""
    home = Path.home()
    # Try skill first (~/.claude/skills/{name}/SKILL.md)
    skill_path = home / ".claude" / "skills" / skill_name / "SKILL.md"
    if not skill_path.exists():
        # Try agent (~/.claude/agents/{name}.md)
        skill_path = home / ".claude" / "agents" / f"{skill_name}.md"
    if not skill_path.exists():
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    try:
        content = skill_path.read_text(encoding="utf-8", errors="replace")
        return JSONResponse({"ok": True, "name": skill_name, "path": str(skill_path), "content": content})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


_CTX_TEL_CACHE: dict = {"data": None, "ts": 0.0}
_CTX_TEL_TTL = 60  # 60s TTL — Turso is slow (Korea→AWS us-west-2), cache to avoid blocking

@app.get("/api/ctx-telemetry")
async def ctx_telemetry():
    """CTX ↔ NS-Hub integration: live telemetry from hub-ctx Turso DB.

    Returns user counts, recent session stats, and version breakdown so the
    hub dashboard can show CTX data collection health alongside milestone progress.
    Uses asyncio.to_thread + TTL cache to avoid blocking the event loop.
    """
    import time as _time
    now = _time.time()
    if _CTX_TEL_CACHE["data"] is not None and (now - _CTX_TEL_CACHE["ts"]) < _CTX_TEL_TTL:
        return JSONResponse(_CTX_TEL_CACHE["data"])

    import os as _os_tel
    _turso_token_check = _os_tel.environ.get("HUB_CTX_TURSO_TOKEN", "")
    if not _turso_token_check:
        return JSONResponse({"error": "HUB_CTX_TURSO_TOKEN not configured", "sources": []})

    def _fetch_sync():
        import urllib.request as _ur, os as _os
        turso_url = _os.environ.get("HUB_CTX_TURSO_URL", "https://hub-ctx-jaytoone.aws-us-west-2.turso.io")
        turso_token = _os.environ.get("HUB_CTX_TURSO_TOKEN", "")

        def _q(sql: str):
            payload = json.dumps({"requests": [
                {"type": "execute", "stmt": {"sql": sql}},
                {"type": "close"}
            ]}).encode()
            req = _ur.Request(f"{turso_url}/v2/pipeline", data=payload,
                headers={"Authorization": f"Bearer {turso_token}", "Content-Type": "application/json"},
                method="POST")
            with _ur.urlopen(req, timeout=6) as r:
                return json.load(r)["results"][0]["response"]["result"]["rows"]

        total_rows = int(_q("SELECT COUNT(*) FROM ctx_session_aggregates")[0][0]["value"])
        total_users = int(_q("SELECT COUNT(DISTINCT user_id) FROM ctx_session_aggregates")[0][0]["value"])
        ext_q = ("SELECT COUNT(DISTINCT user_id) FROM ctx_session_aggregates "
                 "WHERE user_id NOT IN ('6d7f66b2fb843134','2e00a759e17a12c4','validate_test_001') "
                 "AND user_id NOT LIKE 'retry%' AND user_id NOT LIKE 'test%' "
                 "AND user_id NOT LIKE '12293e702290%'")
        ext_users = int(_q(ext_q)[0][0]["value"])
        ver_rows = _q("SELECT ctx_version, COUNT(DISTINCT user_id) u FROM ctx_session_aggregates "
                      "WHERE ctx_version >= '0.3.26' GROUP BY ctx_version ORDER BY ctx_version DESC LIMIT 5")
        versions = {(r[0]["value"] if r[0]["type"] != "null" else "unknown"): int(r[1]["value"])
                    for r in ver_rows}
        return {
            "total_rows": total_rows, "total_users": total_users,
            "external_users": ext_users, "versions": versions,
            "ns2_passed": ext_users > 0,
            "db": "hub-ctx-jaytoone.aws-us-west-2.turso.io",
        }

    try:
        data = await asyncio.to_thread(_fetch_sync)
        _CTX_TEL_CACHE["data"] = data
        _CTX_TEL_CACHE["ts"] = _time.time()
        return JSONResponse(data)
    except Exception as exc:
        stale = _CTX_TEL_CACHE.get("data")
        if stale:
            return JSONResponse({**stale, "_stale": True})
        return JSONResponse({"error": str(exc)[:100]}, status_code=503)


@app.get("/api/verify-plan")
async def verify_plan():
    """Return dynamic check list based on current services config — no hardcoded checks."""
    checks = []
    # 1. Health checks — derived from actual registered services
    built_in = ["northstar", "market-signals"]
    for svc in built_in + list(SERVICES.keys()):
        checks.append({"id": f"health/{svc}", "type": "health", "url": f"/health/{svc}"})
    # 2. Tab loaded checks — one per service
    for svc in built_in + list(SERVICES.keys()):
        checks.append({"id": f"tab_loaded/{svc}", "type": "tab_loaded", "frame_id": f"frame-{svc}"})
    # 3. Dark mode checks — same-origin iframes only
    for svc in ["northstar", "market-signals"]:
        checks.append({"id": f"dark_mode/{svc}", "type": "dark_mode_iframe", "frame_id": f"frame-{svc}"})
    # 4. Cross-origin dark mode listeners
    for svc in SERVICES.keys():
        checks.append({"id": f"dark_mode/{svc}_listener", "type": "dark_mode_listener", "svc": svc})
    # 5. NS board nodes (inside northstar iframe)
    checks.append({"id": "ns_nodes", "type": "selector", "selector": ".ns-node", "frame_id": "frame-northstar", "min": 1})
    # 6. No JS errors
    checks.append({"id": "no_console_errors", "type": "no_js_errors"})
    return JSONResponse({"checks": checks, "count": len(checks), "services": list(SERVICES.keys())})


def _spawn_entity_corpus() -> "subprocess.Popen | None":
    """M275: auto-start entity-corpus dashboard alongside hub.

    Set ENTITY_CORPUS_DISABLED=1 to skip auto-spawn entirely.

    Path resolution order (deployment-safe):
      1. ENTITY_CORPUS_SERVER env var (explicit override; set to 'disabled' to skip)
      2. ~/.claude/skills/entity/dashboard/server.py  (local dev)
      3. hub package sibling: hub/../entity/dashboard/server.py
      4. Skip silently if not found (hub still works without corpus)
    """
    import shutil
    # Allow explicit opt-out
    if os.environ.get("ENTITY_CORPUS_DISABLED", "").strip() in ("1", "true", "yes"):
        return None
    corpus_server_env = os.environ.get("ENTITY_CORPUS_SERVER", "").strip()
    if corpus_server_env.lower() in ("disabled", "off", "0"):
        return None
    candidates = [
        corpus_server_env,
        str(Path(__file__).parent / "entity" / "dashboard" / "server.py"),  # hub-bundled (preferred)
        str(Path.home() / ".claude" / "skills" / "entity" / "dashboard" / "server.py"),  # skills fallback
    ]
    server_path = next((p for p in candidates if p and Path(p).exists()), None)
    if not server_path:
        return None

    # Check if already running on 8989
    _ts_ip = _tailscale_interface_ip()
    _bind_ip = _ts_ip if _ts_ip != "127.0.0.1" else "127.0.0.1"
    try:
        import socket as _sock
        s = _sock.socket()
        s.settimeout(0.5)
        r = s.connect_ex((_bind_ip, 8989))
        s.close()
        if r == 0:
            return None  # already running — don't double-spawn
    except Exception:
        pass

    corpus_env = {**os.environ, "ENTITY_DASH_HOST": _bind_ip, "ENTITY_DASH_PORT": "8989"}
    try:
        proc = subprocess.Popen(
            [sys.executable, server_path],
            env=corpus_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc
    except Exception as e:
        print(f"[hub] entity-corpus auto-start failed: {e}", file=sys.stderr)
        return None


_HUB_GLOBAL_BLOCK = """\
## NS Hub Protocol (auto-injected by hub install-global)
<!-- NS_HUB_GLOBAL_START -->
Stone lifecycle: queued → pending_confirmation → done.
COMPLETION PROTOCOL: PATCH status=pending_confirmation + append_message.role=claude + star_relation + exec_start + exec_end + model_used (all in one PATCH).
COMMENT RULE: append_message ≤3 lines. Longer detail → docs/ns-replies/<DATE>-<MID>.md then reference link in comment.
PARALLEL DISPATCH: 2+ independent queued stones → emit multiple Agent calls in ONE message.
NO-OP PROTOCOL: if skipping a stone, post 1-line reason via append_message. Silent skip forbidden.
TOKEN DISCIPLINE: after completion post ONLY the 1-line summary. No follow-up questions.
<!-- NS_HUB_GLOBAL_END -->
"""

_HUB_PROJECT_BLOCK_TEMPLATE = """\
## NS Hub — Project Config (auto-injected by hub init)
<!-- NS_HUB_PROJECT_START -->
Hub URL: {hub_url}
Project ID: {proj_id}
API base: {hub_url}/api/northstar/{proj_id}
Exec: GET /milestones  →  implement  →  PATCH /{{mid}} {{status,append_message,star_relation,exec_start,exec_end,model_used}}
Log completion: append to {proj_dir}/completion-log.jsonl {{"mid":..., "summary":..., "ts":...}}
<!-- NS_HUB_PROJECT_END -->
"""


def _hub_init(proj_id=None, proj_dir=None):
    """Write NS Hub project config block to CLAUDE.md in current directory."""
    cwd = Path(proj_dir) if proj_dir else Path.cwd()
    pid = proj_id or cwd.name
    _h = os.environ.get("HUB_HOST", HOST)
    if _h in ("0.0.0.0", ""):
        import socket as _sock
        try:
            _h = _sock.gethostbyname(_sock.gethostname())
        except Exception:
            _h = "127.0.0.1"
    hub_url = f"http://{_h}:{PORT}"
    claude_md = cwd / "CLAUDE.md"
    block = _HUB_PROJECT_BLOCK_TEMPLATE.format(hub_url=hub_url, proj_id=pid, proj_dir=str(cwd))
    if claude_md.exists():
        text = claude_md.read_text(encoding="utf-8")
        if "<!-- NS_HUB_PROJECT_START -->" in text:
            import re as _re
            text = _re.sub(r"## NS Hub — Project Config.*?<!-- NS_HUB_PROJECT_END -->", block.strip(), text, flags=_re.DOTALL)
        else:
            text = text.rstrip() + "\n\n" + block
        claude_md.write_text(text, encoding="utf-8")
        print(f"Updated {claude_md}")
    else:
        claude_md.write_text(f"# {pid} — Claude Instructions\n\n" + block, encoding="utf-8")
        print(f"Created {claude_md}")
    print(f"  Hub URL : {hub_url}")
    print(f"  Project : {pid}")


_HUB_HOOKS_DIR = Path(__file__).parent / "static" / "hooks"
_HUB_SETTINGS_HOOKS = {
    "Stop": [
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-stop-inject.py"},
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/stop-decision-capture.py"},
    ],
    "UserPromptSubmit": [{"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-execute-inject.py"}],
    "SessionStart": [{"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-session-start.py", "timeout": 5}],
}

def _hub_deploy_hooks():
    """M842.4: Register hub hook scripts in ~/.claude/settings.json.
    Points directly to .hub/static/hooks/ — no file copy needed."""
    # Register in settings.json if not already present
    settings_path = Path.home() / ".claude" / "settings.json"
    try:
        settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    except Exception:
        settings = {}
    hooks_cfg = settings.setdefault("hooks", {})
    changed = False
    for event, hook_list in _HUB_SETTINGS_HOOKS.items():
        existing = hooks_cfg.get(event, [])
        existing_cmds = set()
        for entry in existing:
            for h in (entry.get("hooks", [entry]) if "hooks" in entry else [entry]):
                if "command" in h:
                    existing_cmds.add(h["command"])
        for h in hook_list:
            if h["command"] not in existing_cmds:
                hooks_cfg.setdefault(event, []).append({"hooks": [h]})
                changed = True
                print(f"Registered hook {event}: {h['command']}")
    if changed:
        settings_path.write_text(json.dumps(settings, indent=2, ensure_ascii=False))

def _hub_install_global():
    """Write NS Hub global protocol block to ~/.claude/CLAUDE.md (once) and deploy hooks.
    Also triggers ctx-install (CTX is a hub dependency) to register memory/intelligence hooks."""
    _hub_deploy_hooks()
    # M959: also run ctx-install so CTX hooks (bm25-memory, chat-memory, utility-rate, etc.)
    # are registered — CTX is a required dependency so it's always available.
    try:
        import subprocess, shutil
        ctx_install_bin = shutil.which("ctx-install")
        if ctx_install_bin:
            result = subprocess.run([ctx_install_bin], capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                print("[hub] ctx-install: CTX hooks registered.")
            else:
                print(f"[hub] ctx-install warning: {result.stderr[:120]}")
        else:
            # Try python -m entry point
            import sys
            result = subprocess.run(
                [sys.executable, "-m", "ctx_retriever.cli.install"],
                capture_output=True, text=True, timeout=15
            )
            if result.returncode == 0:
                print("[hub] ctx-install (module): CTX hooks registered.")
    except Exception as _ctx_err:
        print(f"[hub] ctx-install skipped: {_ctx_err}")
    global_md = Path.home() / ".claude" / "CLAUDE.md"
    if not global_md.parent.exists():
        global_md.parent.mkdir(parents=True, exist_ok=True)
    if global_md.exists():
        text = global_md.read_text(encoding="utf-8")
        if "<!-- NS_HUB_GLOBAL_START -->" in text:
            print(f"Global hub protocol already present in {global_md} — skipping.")
            return
        text = text.rstrip() + "\n\n" + _HUB_GLOBAL_BLOCK
        global_md.write_text(text, encoding="utf-8")
        print(f"Appended global hub protocol to {global_md}")
    else:
        global_md.write_text(_HUB_GLOBAL_BLOCK, encoding="utf-8")
        print(f"Created {global_md} with global hub protocol")


# ── M561: Data collection consent ─────────────────────────────────────────────
_CONSENT_FILE = _HUB_DATA_DIR / ".hub-consent.json"

def _get_consent() -> dict:
    if _CONSENT_FILE.exists():
        try:
            return json.loads(_CONSENT_FILE.read_text())
        except Exception:
            pass
    return {"data_collection": True}

def _save_consent(data: dict):
    _CONSENT_FILE.write_text(json.dumps(data))

# ── M929: Usage telemetry — local stats + optional PyPI ping ──────────────────
_USAGE_FILE = _HUB_DATA_DIR / "usage-stats.jsonl"

def _record_usage_event(event: str, extra: dict = None):
    """M929: Record usage event locally + centrally via Turso (consent-gated, no PII)."""
    if not _get_consent().get("data_collection", True):
        return
    import hashlib, platform, time
    _install_id = hashlib.sha256(platform.node().encode()).hexdigest()[:16]
    entry = {
        "ts": int(time.time()), "event": event,
        "install_id": _install_id,
        "version": "0.2.18",
        "os": platform.system(),
        **(extra or {})
    }
    # Local JSONL
    try:
        with open(_USAGE_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass
    # Central Turso (same DB as CTX stats) — async, non-blocking
    if _TURSO_ENABLED:
        import threading
        def _send_to_turso():
            _turso_execute(
                "CREATE TABLE IF NOT EXISTS hub_usage (ts INTEGER, event TEXT, install_id TEXT, version TEXT, os TEXT)",
            )
            _turso_execute(
                "INSERT INTO hub_usage (ts, event, install_id, version, os) VALUES (?, ?, ?, ?, ?)",
                [entry["ts"], entry["event"], entry["install_id"], entry["version"], entry["os"]],
            )
        threading.Thread(target=_send_to_turso, daemon=True).start()

@app.get("/api/hub/usage-stats")
async def get_usage_stats():
    """Return local usage stats summary."""
    if not _USAGE_FILE.exists():
        return JSONResponse({"events": [], "total": 0})
    lines = _USAGE_FILE.read_text().strip().split("\n") if _USAGE_FILE.exists() else []
    events = []
    for line in lines[-100:]:  # last 100 events
        try: events.append(json.loads(line))
        except: pass
    # Summarize by event type
    from collections import Counter
    counts = Counter(e.get("event") for e in events)
    return JSONResponse({"events": events[-20:], "total": len(events), "counts": dict(counts)})

@app.get("/api/hub/consent")
async def get_consent():
    return JSONResponse(_get_consent())

@app.post("/api/hub/consent")
async def set_consent(request: Request):
    body = await request.json()
    data = _get_consent()
    if "data_collection" in body:
        data["data_collection"] = bool(body["data_collection"])
    _save_consent(data)
    return JSONResponse(data)
# ── end M561 ───────────────────────────────────────────────────────────────────


# ── M705: /api/hub/config REST endpoints ──────────────────────────────────────
@app.get("/api/hub/config")
async def get_hub_config():
    """Return current user config (defaults + per-project overrides)."""
    cfg = _read_hub_config()
    return JSONResponse({"ok": True, "config": cfg,
                         "allowed_agents": sorted(_ALLOWED_AGENTS),
                         "allowed_models": sorted(_ALLOWED_MODELS)})


@app.patch("/api/hub/config")
async def patch_hub_config(request: Request):
    """
    Update user config. Body keys:
      defaults.agent, defaults.model         — global fallback
      defaults.claude_code_path              — path to claude CLI binary
      defaults.codex_path                    — path to codex CLI binary
      projects.<proj_id>.agent               — project-level override
      projects.<proj_id>.model
    Unrecognised keys are ignored.
    """
    body = await request.json()
    cfg = _read_hub_config()
    errors = []

    if "defaults" in body:
        d = body["defaults"]
        cfg.setdefault("defaults", {})
        if "agent" in d:
            v = str(d["agent"]).strip().lower()
            if v and v not in _ALLOWED_AGENTS:
                errors.append(f"Unknown agent '{v}'. Allowed: {sorted(_ALLOWED_AGENTS)}")
            else:
                cfg["defaults"]["agent"] = v or None
        if "model" in d:
            v = str(d["model"]).strip()
            if v and v not in _ALLOWED_MODELS:
                errors.append(f"Unknown model '{v}'.")
            else:
                cfg["defaults"]["model"] = v or None
        if "claude_code_path" in d:
            v = str(d["claude_code_path"]).strip()
            cfg["defaults"]["claude_code_path"] = v or None
        if "codex_path" in d:
            v = str(d["codex_path"]).strip()
            cfg["defaults"]["codex_path"] = v or None

    if "projects" in body:
        for proj_id, overrides in body["projects"].items():
            cfg.setdefault("projects", {}).setdefault(proj_id, {})
            if "agent" in overrides:
                v = str(overrides["agent"]).strip().lower()
                if v and v not in _ALLOWED_AGENTS:
                    errors.append(f"Unknown agent '{v}' for project '{proj_id}'.")
                else:
                    cfg["projects"][proj_id]["agent"] = v or None
            if "model" in overrides:
                v = str(overrides["model"]).strip()
                if v and v not in _ALLOWED_MODELS:
                    errors.append(f"Unknown model '{v}' for project '{proj_id}'.")
                else:
                    cfg["projects"][proj_id]["model"] = v or None

    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    _write_hub_config(cfg)
    return JSONResponse({"ok": True, "config": cfg})
# ── end M705 ───────────────────────────────────────────────────────────────────


def _hub_configure(args: list[str]) -> None:
    """
    hub configure [--project PROJ] [--agent AGENT] [--model MODEL]
    Interactive when called with no flags.
    """
    import yaml as _yaml

    proj_id = None
    agent = None
    model = None
    i = 0
    while i < len(args):
        if args[i] == "--project" and i + 1 < len(args):
            proj_id = args[i + 1]; i += 2
        elif args[i] == "--agent" and i + 1 < len(args):
            agent = args[i + 1]; i += 2
        elif args[i] == "--model" and i + 1 < len(args):
            model = args[i + 1]; i += 2
        else:
            i += 1

    if not agent and not model:
        # Interactive mode
        print("\nhub configure — set per-user defaults for agent and model\n")
        print(f"  Allowed agents : {', '.join(sorted(_ALLOWED_AGENTS))}")
        print(f"  Allowed models : {', '.join(sorted(_ALLOWED_MODELS))}")
        print()
        if proj_id:
            print(f"  Scope: project '{proj_id}' (leave blank to keep current)")
        else:
            print("  Scope: global defaults (leave blank to keep current)")
        print()
        a_in = input("  Agent [claude]: ").strip().lower() or ""
        m_in = input("  Model [leave blank = CLI default]: ").strip() or ""
        if a_in:
            agent = a_in
        if m_in:
            model = m_in

    cfg = _read_hub_config()
    errors = []

    if agent:
        agent = agent.strip().lower()
        if agent not in _ALLOWED_AGENTS:
            errors.append(f"Unknown agent '{agent}'. Allowed: {sorted(_ALLOWED_AGENTS)}")
        else:
            if proj_id:
                cfg.setdefault("projects", {}).setdefault(proj_id, {})["agent"] = agent
            else:
                cfg.setdefault("defaults", {})["agent"] = agent

    if model:
        model = model.strip()
        if model not in _ALLOWED_MODELS:
            errors.append(f"Unknown model '{model}'. Allowed: {sorted(_ALLOWED_MODELS)}")
        else:
            if proj_id:
                cfg.setdefault("projects", {}).setdefault(proj_id, {})["model"] = model
            else:
                cfg.setdefault("defaults", {})["model"] = model

    if errors:
        print("\nErrors:")
        for e in errors:
            print(f"  - {e}")
        return

    _write_hub_config(cfg)
    scope = f"project '{proj_id}'" if proj_id else "global defaults"
    print(f"\nSaved to {_HUB_CONFIG_FILE} ({scope}):")
    print(f"  agent : {agent or '(unchanged)'}")
    print(f"  model : {model or '(unchanged)'}")
    if not proj_id:
        print("\nThese defaults apply to every project that has no explicit agent/model set.")
    print()


def main():
    import sys
    args = sys.argv[1:]

    if args and args[0] == "init":
        pid = None
        pdir = None
        i = 1
        while i < len(args):
            if args[i] == "--dir" and i + 1 < len(args):
                pdir = args[i + 1]; i += 2
            elif not args[i].startswith("-"):
                pid = args[i]; i += 1
            else:
                i += 1
        _hub_init(proj_id=pid, proj_dir=pdir)
        return

    if args and args[0] == "install-global":
        _hub_install_global()
        return

    # M705: hub configure [--project PROJ] [--agent AGENT] [--model MODEL]
    if args and args[0] == "configure":
        _hub_configure(args[1:])
        return

    # M709: --port and --db CLI flags for dev/prod separation
    i = 0
    while i < len(args):
        if args[i] == "--port" and i + 1 < len(args):
            os.environ["HUB_PORT"] = args[i + 1]
            global PORT; PORT = int(args[i + 1]); i += 2
        elif args[i] == "--db" and i + 1 < len(args):
            os.environ["HUB_DB_PATH"] = args[i + 1]
            global _NS_EVENTS_DB; _NS_EVENTS_DB = Path(args[i + 1]); i += 2
        else:
            i += 1

    import uvicorn, atexit, socket

    def _find_free_port(preferred: int) -> int:
        """M842.2: Try preferred port, then preferred+1 if occupied."""
        for candidate in [preferred, preferred + 1]:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(("127.0.0.1", candidate)) != 0:
                    return candidate
        return preferred

    _hub_install_global()
    corpus_proc = _spawn_entity_corpus()
    if corpus_proc:
        atexit.register(corpus_proc.terminate)
    actual_port = _find_free_port(PORT)
    if actual_port != PORT:
        print(f"[hub] Port {PORT} occupied — binding {actual_port} instead.")
    uvicorn.run(app, host=HOST, port=actual_port, log_level="warning")


if __name__ == "__main__":
    main()
