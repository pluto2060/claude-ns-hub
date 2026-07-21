#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 pluto2060 (be2jay67@gmail.com)
"""
Hub server — unified portal aggregating Entity Corpus and North Star.
North Star is a first-class built-in page (multi-project manager), not an iframe.
"""
import asyncio
import json
import logging
import logging.handlers
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import sqlite3
import yaml as _yaml

# M1585-log: file-based rotating log (50MB×5 = 250MB max) — no sudo needed vs journald
_HUB_LOG_DIR = Path.home() / ".hub" / "logs"
_HUB_LOG_DIR.mkdir(parents=True, exist_ok=True)
_HUB_LOG_FILE = _HUB_LOG_DIR / "hub.log"
_hub_file_handler = logging.handlers.RotatingFileHandler(
    _HUB_LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_hub_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

def _attach_hub_log_handler():
    """Attach rotating file handler to root + uvicorn loggers (called after app init)."""
    for name in ("", "uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        lg = logging.getLogger(name)
        if _hub_file_handler not in lg.handlers:
            lg.addHandler(_hub_file_handler)
            lg.setLevel(logging.INFO)

_attach_hub_log_handler()  # attach early; uvicorn will inherit on startup

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

# M1324-P2: shared AsyncClient with connection pool (avoid per-request TCP handshake overhead)
_async_http: httpx.AsyncClient | None = None

def _get_async_http() -> httpx.AsyncClient:
    global _async_http
    if _async_http is None or _async_http.is_closed:
        _async_http = httpx.AsyncClient(timeout=4.0, limits=httpx.Limits(max_connections=20))
    return _async_http
# M712: data dir migrated to ~/.hub/ — Claude Code independent (like .hermes convention)
_HUB_DATA_DIR = Path(os.environ.get("HUB_DATA_DIR", str(Path.home() / ".hub")))
_DEFAULT_PROJECTS_DIR = _HUB_DATA_DIR / "projects"
PROJECTS_DIR = Path(os.environ.get("HUB_PROJECTS_DIR", str(_DEFAULT_PROJECTS_DIR)))

HOST = os.environ.get("HUB_HOST", "")  # resolved below after _tailscale_interface_ip is defined
PORT = int(os.environ.get("HUB_PORT", "9001"))

# M705: Per-user config — agent/model defaults that survive hub reinstalls
_HUB_CONFIG_FILE = _HUB_DATA_DIR / "config.yaml"

# M215: Turso (libSQL cloud) dual-write sync
# M929 security fix: stones sync requires explicit env vars — no hardcoded fallback.
# M929 (rename): HUB_TURSO_URL / HUB_TURSO_TOKEN are the hub-dedicated Turso DB
# (hub-pluto2060). CTX uses HUB_CTX_TURSO_URL/TOKEN (hub-ctx-pluto2060).
# Old generic TURSO_DATABASE_URL / TURSO_AUTH_TOKEN are kept as a deprecation fallback.
_TURSO_URL = (
    os.environ.get("HUB_TURSO_URL")
    or os.environ.get("TURSO_DATABASE_URL", "")
).replace("libsql://", "https://")
_TURSO_TOKEN = (
    os.environ.get("HUB_TURSO_TOKEN")
    or os.environ.get("TURSO_AUTH_TOKEN", "")
)
_TURSO_ENABLED = bool(_TURSO_URL and _TURSO_TOKEN)

# M1517: Direct-to-Turso hardcoded credentials REMOVED. Telemetry now flows through
# Cloudflare Worker relay (HUB_RELAY_URL / HUB_RELAY_SECRET, defined at L11802-11803).
# Legacy env vars HUB_TELEMETRY_URL/TOKEN kept as null fallback so old self-hosters can
# still configure direct Turso writes if they want — but no default credential is shipped.
_HUB_TELEMETRY_URL = os.environ.get("HUB_TELEMETRY_URL", "")
_HUB_TELEMETRY_TOKEN = os.environ.get("HUB_TELEMETRY_TOKEN", "")
_TEL_ENABLED = bool(_HUB_TELEMETRY_URL and _HUB_TELEMETRY_TOKEN)

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


_tailscale_ip_cache: str = ""  # M1940: cached at first call; Tailscale IP never changes at runtime


def _tailscale_interface_ip() -> str:
    """Get the IP assigned to the Tailscale interface (100.x.x.x/32).
    M1940: result cached in _tailscale_ip_cache — avoids repeated `ip addr show` subprocess
    on every spawn/wake call (was called 15+ times per spawn, ~2ms each = ~30ms overhead)."""
    global _tailscale_ip_cache
    if _tailscale_ip_cache:
        return _tailscale_ip_cache
    try:
        r = subprocess.run(["ip", "addr", "show"], capture_output=True, text=True, timeout=2)
        m = re.search(r"(100\.\d+\.\d+\.\d+)/32", r.stdout)
        if m:
            _tailscale_ip_cache = m.group(1)
            return _tailscale_ip_cache
    except Exception:
        pass
    # Windows / psutil fallback
    try:
        import psutil
        for iface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.family == 2 and re.match(r"100\.\d+\.\d+\.\d+", addr.address):
                    _tailscale_ip_cache = addr.address
                    return _tailscale_ip_cache
    except Exception:
        pass
    _tailscale_ip_cache = "127.0.0.1"
    return _tailscale_ip_cache


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


# Resolve HOST: bind 0.0.0.0 so localhost + Tailscale IP both respond.
# Banner still shows the Tailscale URL for mobile access.
if not HOST:
    HOST = "0.0.0.0"


def _ctx_url() -> str:
    return ""  # M1235: CTX disabled


def _corpus_url() -> str:
    # M275: entity-corpus dashboard at port 8989 (entity/dashboard/server.py)
    ip = _bound_ip(8989)
    return f"http://{ip}:8989"


SERVICES = {
    "corpus": {"port": 8989, "label": "Corpus", "url": _corpus_url()},
}

app = FastAPI(title="Hub", version="1.0.0")
_attach_hub_log_handler()  # re-attach after uvicorn loggers are configured by FastAPI init
app.add_middleware(GZipMiddleware, minimum_size=1000)  # M515: compress large JSON (764KB→~80KB)

# M210 follow-up: when this app is served over plain HTTP, redirect every request to the
# HTTPS endpoint so the browser unlocks the Notification API. Same uvicorn process can
# run two instances (HTTP:9000 → redirect, HTTPS:9443 → serve normally); the request.url.scheme
_ui_first_view_recorded = False  # M1348 P0-B: once-per-process flag

@app.get("/static/northstar.html")
async def _northstar_static_nocache():
    global _ui_first_view_recorded
    if not _ui_first_view_recorded:
        _ui_first_view_recorded = True
        _record_usage_event("ui_first_view")
    return FileResponse(str(STATIC / "northstar.html"),
                        headers={"Cache-Control": "no-store, no-cache, must-revalidate",
                                 "Pragma": "no-cache"})

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

# M60: hub-ctx fully integrated — mount CTX dashboard directly (no separate port)
def _mount_ctx_dashboard():
    """M1235: CTX integration disabled — skipping mount."""
    print("[hub] CTX dashboard: disabled (M1235)", file=sys.stderr)

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
    corpus_url = _corpus_url()
    _cfg = _read_hub_config()
    return JSONResponse({
        "corpus_url":         corpus_url,
        "northstar_url":      "/northstar",
        "market_signals_url": "/market-signals",
        "ntfy_topic_set":     bool(_get_telegram_config()[0]),
        "show_market":        bool(_cfg.get("show_market", False)),
        "show_dsk":           bool(_cfg.get("show_dsk", False)),
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


@app.get("/api/blink-effective-window")
async def blink_effective_window(proj_id: str = ""):
    """M1532: simplified single-knob algorithm — idle threshold concept removed.
    Algorithm per project:
    1. Collect blink timestamps within 48h, sort descending (newest first).
    2. Walk through ts list — split into clusters where gap ≥ blink_window_min minutes.
    3. Display = current cluster (containing in-window blinks) + immediately prior cluster.
       Effective window = (now - oldest_ts_in_prior_cluster) so client window filter shows both.
    4. No clusters / no blinks → window = base_min.
    Earlier clusters (gap older than prior cluster) are hidden.
    """
    import datetime as _dt_bew
    # M1527 v3 fix: previous 48h cap filtered out ALL blink mids for inactive projects,
    # leaving window_min=base. User intent ("BLINK interval 이상 차이나는 마지막 블링크
    # 집단은 자동으로 보여주로록"): the LAST cluster must always surface even if days old.
    # Solution: drop the input-side cap (let cluster detection see all history), but cap the
    # final effective window at 7d so a 30d-old cluster doesn't produce window_min=43200.
    _CAP_OUTPUT_MIN = 7 * 24 * 60  # 7d max effective window
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row_base = conn.execute("SELECT value_json FROM user_settings WHERE key='blink_window_min'").fetchone()
        base_min = 60
        try:
            if row_base:
                base_min = int(json.loads(row_base[0])) or 60
        except Exception:
            pass

        now_ms = int(_dt_bew.datetime.now(_dt_bew.timezone.utc).timestamp() * 1000)
        gap_ms = base_min * 60 * 1000

        # Per-project filter when proj_id specified
        if proj_id:
            blink_rows = conn.execute(
                "SELECT key, value_json FROM user_settings WHERE key = ?",
                (f"blink_state_{proj_id}",)
            ).fetchall()
        else:
            blink_rows = conn.execute(
                "SELECT key, value_json FROM user_settings WHERE key LIKE 'blink_state_%'"
            ).fetchall()
        conn.close()

        best_window = base_min
        best_meta = {}

        for (bkey, val_json) in blink_rows:
            try:
                state = json.loads(val_json)
            except Exception:
                continue
            proj_ts = []
            for ns_val in state.values():
                mids = ns_val.get("mids", {}) if isinstance(ns_val, dict) else {}
                for ts_ms in mids.values():
                    # M1527 v3: accept all past mids (no 48h cap on input — output capped instead)
                    if isinstance(ts_ms, (int, float)) and 0 < ts_ms <= now_ms:
                        proj_ts.append(int(ts_ms))
            if not proj_ts:
                continue
            proj_ts.sort(reverse=True)  # newest first

            # M1532: walk + split into clusters by gap_ms
            # cluster boundaries — append cluster end timestamps
            clusters = []  # list of (start_ts, end_ts) where start=newest, end=oldest in cluster
            cluster_start = proj_ts[0]
            cluster_end = proj_ts[0]
            for prev, curr in zip(proj_ts, proj_ts[1:]):
                if prev - curr >= gap_ms:
                    clusters.append((cluster_start, cluster_end))
                    cluster_start = curr
                    cluster_end = curr
                else:
                    cluster_end = curr
            clusters.append((cluster_start, cluster_end))

            # Display = current cluster (clusters[0]) + prior cluster (clusters[1] if exists)
            display_oldest = clusters[1][1] if len(clusters) >= 2 else clusters[0][1]
            age_min = int((now_ms - display_oldest) / 60000)
            # M1527 v3: cap output at 7d (was 48h) — covers inactive-project edge case
            effective_min = min(max(age_min + 1, base_min), _CAP_OUTPUT_MIN)

            if effective_min > best_window:
                best_window = effective_min
                best_meta = {
                    "cluster_count": len(clusters),
                    "current_cluster_size": _cluster_count(proj_ts, clusters[0]),
                    "prior_cluster_age_h": round((now_ms - clusters[1][0]) / 3600000, 1) if len(clusters) >= 2 else None,
                    "display_oldest_age_h": round(age_min / 60, 1),
                }

        return JSONResponse({
            "window_min": best_window,
            "base_min": base_min,
            "mode": "last2",  # M1532: single algorithm
            **best_meta,
        })
    except Exception as e:
        return JSONResponse({"window_min": 60, "mode": "last2", "error": str(e)})


def _cluster_count(ts_list, cluster_range):
    """M1532: count how many ts fall within cluster range (inclusive)."""
    start, end = cluster_range
    return sum(1 for t in ts_list if end <= t <= start)


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


def _mark_blink_server(proj_id: str, s_key: str, mid: str, remove: bool = False):
    """M1007: server-side blink trigger. When a stone's status changes via the
    PATCH path (incl. exec-session API completions where no browser is watching),
    stamp the affected substar's blink mids+ts into user_settings.blink_state_{proj}
    with ts=now so the client's _blinkCenterHydrate restores a fresh, in-window blink
    on the next board open.
    M1216: remove=True removes mid from blink_state (called on done/skipped transition).
    M1227: schema changed to {sKey: {mids: {mid: ts}}} — per-mid timestamps so that
    stones within the same substar do NOT affect each other's blink window."""
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
        _prune_cutoff = _now_ms - 48 * 3600 * 1000
        # M1227: migrate old {sKey:{ts,mids:[]}} shape → new {sKey:{mids:{mid:ts}}} shape
        _new_bundle = {}
        for _k, _e in bundle.items():
            if not isinstance(_e, dict):
                continue
            if "mids" in _e and isinstance(_e["mids"], dict):
                # already new shape — prune per-mid ts
                _mt = {m: t for m, t in _e["mids"].items() if isinstance(t, (int, float)) and t >= _prune_cutoff}
                if _mt:
                    _new_bundle[_k] = {"mids": _mt}
            elif "mids" in _e and isinstance(_e["mids"], list):
                # old shape: all mids share sKey-level ts
                _old_ts = _e.get("ts") or 0
                if _old_ts >= _prune_cutoff:
                    _mt = {m: _old_ts for m in _e["mids"] if isinstance(m, str)}
                    if _mt:
                        _new_bundle[_k] = {"mids": _mt}
        bundle = _new_bundle
        entry = bundle.get(s_key) if isinstance(bundle.get(s_key), dict) else {}
        mid_ts = entry.get("mids") if isinstance(entry.get("mids"), dict) else {}
        # M1222: purge done/skipped mids on every write; M1241: pending_confirmation stays in blink
        try:
            _clear_st = {"done", "skipped"}
            _rows_ms = conn.execute(
                "SELECT stone_id, status FROM milestones_store WHERE proj_id=?", (proj_id,)
            ).fetchall()
            _status_map = {r[0]: (r[1] or "") for r in _rows_ms}
            mid_ts = {m: t for m, t in mid_ts.items() if m in _status_map and _status_map[m] not in _clear_st}
        except Exception:
            pass
        if remove:
            mid_ts.pop(mid, None)
        else:
            mid_ts[mid] = _now_ms  # only this mid's ts is updated
        if mid_ts:
            bundle[s_key] = {"mids": mid_ts}
        elif s_key in bundle:
            del bundle[s_key]
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
_USAGE_TTL = 60  # 60s — M1501: API rate-limit safe (was 30s, 429 at 5s)

def _fetch_usage_blocking():
    """M1296: OAuth usage endpoint re-enabled — credentials from ~/.claude/.credentials.json."""
    import json as _j, urllib.request as _ur, pathlib as _pl
    try:
        creds_path = _pl.Path.home() / ".claude" / ".credentials.json"
        token = _j.loads(creds_path.read_text()).get("claudeAiOauth", {}).get("accessToken", "")
        if not token:
            return {"error": "no oauth token"}
        req = _ur.Request(
            "https://api.anthropic.com/api/oauth/usage",
            headers={"Authorization": f"Bearer {token}", "anthropic-beta": "oauth-2025-04-20"},
        )
        with _ur.urlopen(req, timeout=6) as r:
            return _j.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

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
        # M1501: cache failures for only 30s so rate-limit errors retry quickly
        c["data"], c["ts"] = out, now - (_USAGE_TTL - 30)
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

# ─── M1656-R: unified busy/idle state (surgical simplification) ─────────────────
# SINGLE SOURCE OF TRUTH: _agent_busy_sessions[session_key] — one record per exec session.
#   {proj_id, busy, reason, ts, stone_id}
# Everything else is DERIVED:
#   _session_is_busy(session)   — the one busy predicate (freshness + in-flight stone hold)
#   _first_running_stone(proj)  — which stone a project is working on (for task-board/UI)
#   _oob_is_busy(proj, session) — legacy-compatible wrapper over the two above
# Removed layers (replaced by the above + _send_exec_wake's atomic 90s dedup):
#   _session_running_stone sentinels (__done__/__dispatching__), _dispatch_inflight,
#   _wake_inflight, _last_go_sent/GO_COOLDOWN, poller idle-file/idle-count gating.
_agent_busy_state: dict[str, dict] = {}  # proj_id → legacy proj-level record (harnesses w/o session_key)
_OOB_STALE_SECS = 120        # session/proj report freshness window (heartbeats arrive via PostToolUse)
_WORK_STALE_CAP_SECS = 7200  # max busy hold for an in-flight stone with no heartbeat (crash safety cap)
_agent_busy_cli: dict[str, dict] = {}       # proj_id → last non-exec (CLI) signal — observability only
_agent_busy_sessions: dict[str, dict] = {}  # session_key → {proj_id, busy, reason, ts, stone_id}
_WAKE_SENT_TTL_SECS = 45  # M1675: optimistic busy hold after wake injection (claim measured 6-12s;
                          # expires if the session never picks up so the poller can retry)
# M1683: compacting busy-hold. _COMPACT_HOLD_SECS is the absolute cap in _session_is_busy
# (backstop when the poller self-heal below never runs, e.g. no queued stones). The poller
# releases the hold earlier once the PreCompact marker is older than _COMPACT_SELFHEAL_SECS
# (compaction finished). Marker mtime is set at compaction START, so this must exceed the
# longest realistic compaction (~3.5min observed) to avoid clearing a live compaction.
_COMPACT_HOLD_SECS = 300       # was 600 — self-heal + fixed Stop hook now clear it promptly
_COMPACT_SELFHEAL_SECS = 240   # marker-age past which compaction is deemed complete
_TOOL_HEARTBEAT_HOLD_SECS = 900  # M1741: 15min grace for tool_call_heartbeat (see below)
_CLAIM_TTL_SECS = 120.0  # M1769: shared with claim_task_for_session's local _claim_ttl —
                         # how long a claimed_by_session lock is trusted before being stale


def _validate_stone_hold(session_name: str, rec: dict) -> str | None:
    """M1656-R10: check whether rec['stone_id'] is STILL a real in-flight claim for this
    session (still 'queued' + claimed_by_session matches). Returns the stone_id if valid,
    else None AND clears rec['stone_id'] in place.
    Root cause fixed: tool_call_heartbeat POSTs carry no stone_id, so a session doing
    ANY tool call (including plain interactive conversation, unrelated to dispatch)
    keeps re-confirming busy=True while blindly preserving whatever stone_id was set by
    the last real claim — forever, even after that stone finished. Observed live: mother's
    own pane showed 'running M1672' though M1672 had been pending_confirmation for
    ~40 minutes and belonged to a different (child) session entirely. Previously this
    DB check only ran once heartbeats went stale (>120s); a session with continuous
    heartbeats (<120s gaps) never re-validated at all."""
    _sid = rec.get("stone_id")
    if not _sid:
        return None
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        row = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND stone_id=? AND status='queued'",
            (rec.get("proj_id", ""), _sid)
        ).fetchone()
        conn.close()
        if row:
            _row_d = json.loads(row[0])
            _claimed = (_row_d.get("claimed_by_session") or "").strip()
            _claimed_at = _row_d.get("claimed_at") or 0
            # M1769: name match alone isn't proof of a live claim — a stale claimed_by_session
            # (TTL long expired, e.g. a dead session's leftover claim) must not count as
            # in-flight just because a revived/new session happens to share the same name.
            if not _claimed or (_claimed == session_name and (time.time() - _claimed_at) < _CLAIM_TTL_SECS):
                return _sid  # still genuinely in-flight for this session
        rec["stone_id"] = None
        return None
    except Exception:
        return _sid  # DB hiccup → don't flicker the UI off a transient error


def _session_is_busy(session_name: str) -> bool:
    """THE busy predicate for one exec session.
    Fresh report (<_OOB_STALE_SECS) → trust its busy flag.
    Stale but a stone is in-flight → hold busy up to _WORK_STALE_CAP_SECS, validated
    against the DB (covers long thinking/compaction phases with no tool-call heartbeats,
    while never letting a deleted/finished stone pin a session busy for hours)."""
    rec = _agent_busy_sessions.get(session_name)
    if not rec:
        return False
    _age = time.time() - rec.get("ts", 0)
    # M1675: optimistic wake_sent hold — busy from the instant the wake is injected,
    # but only for _WAKE_SENT_TTL_SECS (a real heartbeat/claim replaces the record;
    # a dead session falls back to idle so the poller can retry).
    if rec.get("reason") == "wake_sent":
        return bool(rec.get("busy", False)) and _age < _WAKE_SENT_TTL_SECS
    # M1672: compaction emits no tool-call heartbeats, so a compacting session went
    # stale after 120s and the poller injected duplicate wakes mid-compaction.
    # Hold busy up to 10min for an explicit compacting report; the session's first
    # post-compaction heartbeat (or Stop) replaces the record either way.
    if rec.get("reason") == "compacting" and rec.get("busy"):
        return _age < _COMPACT_HOLD_SECS
    # M1672-b: PreToolUse tool_start hold — a single >120s tool call (SSH build,
    # subagent) emits no PostToolUse heartbeat. Hold busy up to 1h, but past the
    # normal freshness window require the tmux session to still exist (crash guard).
    if rec.get("reason") == "tool_start" and rec.get("busy"):
        if _age < _OOB_STALE_SECS:
            return True
        return _age < 3600 and _tmux_session_alive(session_name)
    # M131-d: a background subagent (Task/Agent) outlives the parent's own turn —
    # Stop fires (parent turn ended) while the subagent still computes. Same 1h/
    # tmux-alive shape as tool_start; northstar-stop-idle.py sets this reason when
    # its subagent-count marker is >0 instead of posting plain agent_stopped.
    # M1893: SubagentStop may never fire (crash/kill mid-flight), leaving the
    # marker stuck at count=1 and the session locked busy up to 3600s. Server-side
    # self-heal: check marker file mtime directly — if the marker hasn't been touched
    # in _SUBAGENT_MARKER_TTL_SECS (15min), treat as leaked/stale and clear it.
    _SUBAGENT_MARKER_TTL_SECS = 900
    if rec.get("reason") == "subagent_running" and rec.get("busy"):
        if _age < _OOB_STALE_SECS:
            return True
        _marker = Path.home() / ".claude" / f".subagent-count-{session_name}"
        if _marker.exists():
            try:
                _marker_age = time.time() - _marker.stat().st_mtime
                if _marker_age > _SUBAGENT_MARKER_TTL_SECS:
                    _marker.write_text("0")
                    rec["busy"] = False
                    return False
            except Exception:
                pass
        return _age < 3600 and _tmux_session_alive(session_name)
    # M1741: tool_call_heartbeat (PostToolUse, fires after EVERY tracked tool call) only
    # got the generic 120s window, not tool_start's long hold — so a multi-minute stretch
    # of pure generation with no tool call in progress (composing a long response/large
    # tool input between two tool calls) went stale and the poller injected wakes that
    # stacked as unconsumed queued text mid-turn (observed: JGOS, 3 wakes over ~4min while
    # the agent was still actively drafting, no tool_use in between). 15min (vs tool_start's
    # 1h) is a deliberate compromise — a session that crashes right after a tool call now
    # takes up to 15min instead of 120s to be noticed as dead, but a normal tool call
    # refreshes this heartbeat immediately, so real work is unaffected either way.
    if rec.get("reason") == "tool_call_heartbeat" and rec.get("busy"):
        if _age < _OOB_STALE_SECS:
            return True
        return _age < _TOOL_HEARTBEAT_HOLD_SECS and _tmux_session_alive(session_name)
    if _age < _OOB_STALE_SECS:
        return bool(rec.get("busy", False))
    if rec.get("busy") and rec.get("stone_id") and _age < _WORK_STALE_CAP_SECS:
        if _validate_stone_hold(session_name, rec):
            return True
        rec["busy"] = False
        return False
    return False


def _first_running_stone(proj_id: str) -> str | None:
    """Stone id currently being worked on by any busy session of this project.
    M1656-R10: always DB-validates the stone_id (see _validate_stone_hold) — a fresh
    heartbeat alone is not proof the held stone_id is still relevant."""
    for _sk, rec in _agent_busy_sessions.items():
        if rec.get("proj_id") == proj_id and rec.get("stone_id") and _session_is_busy(_sk):
            if _validate_stone_hold(_sk, rec):
                return rec["stone_id"]
    return None


def _oob_is_busy(proj_id: str, session_name: str | None = None) -> bool:
    """Legacy-compatible busy check.
    session_name given + that session has ever reported → its own state only.
    Otherwise: any busy session of the project, then legacy proj-level record.
    M1806: session_name is NOT a narrowing filter on the sibling-scan fallback below —
    a cold-start session with no record of its own falls straight through to "any busy
    session of the project" (line ~776), the exact TTL-less sibling-busy inheritance M1771
    already fixed for the /api/exec-sessions display path. That fix only patched the ONE
    call site (idle-display), leaving this function's own fallback — and every OTHER call
    site that passes session_name expecting a session-scoped answer (dispatch/wake
    decisions, not just display) — still exposed. Use _oob_is_busy_session_scoped for any
    new session-scoped call site instead of calling this function with session_name set;
    kept here unchanged only for the two genuinely project-wide callers (no session_name)."""
    if session_name and session_name in _agent_busy_sessions:
        rec = _agent_busy_sessions[session_name]
        if rec.get("proj_id") == proj_id:
            return _session_is_busy(session_name)
    for _sk, rec in _agent_busy_sessions.items():
        if rec.get("proj_id") == proj_id and _session_is_busy(_sk):
            return True
    oob = _agent_busy_state.get(proj_id)
    if not oob:
        return False
    if (time.time() - oob.get("ts", 0)) >= _OOB_STALE_SECS:
        return False
    return bool(oob.get("busy", False))


def _oob_is_busy_session_scoped(proj_id: str, session_name: str) -> bool:
    """M1806: the session-scoped counterpart of the M1771 fix, generalized so it isn't
    duplicated at every call site. A session with its own busy record → that record decides,
    same as _oob_is_busy. A session with NO record of its own → idle=True (not "inherit
    whatever a sibling last reported", which has no TTL and can be hours/days stale) — the
    only exception is the existing TTL-bound (_OOB_STALE_SECS) legacy proj-level record,
    which is a real "something in this project reported busy recently" signal, not an
    unbounded sibling scan."""
    if session_name in _agent_busy_sessions:
        rec = _agent_busy_sessions[session_name]
        if rec.get("proj_id") == proj_id:
            return _session_is_busy(session_name)
    oob = _agent_busy_state.get(proj_id)
    if not oob:
        return False
    if (time.time() - oob.get("ts", 0)) >= _OOB_STALE_SECS:
        return False
    return bool(oob.get("busy", False))


def _tmux_session_alive(session_name: str) -> bool:
    """M1650: check tmux session existence without inspecting pane commands."""
    import subprocess as _sp
    try:
        r = _sp.run(["tmux", "has-session", "-t", f"={session_name}"],
                    capture_output=True, timeout=2)
        return r.returncode == 0
    except Exception:
        return False


_BUSY_SESSIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS agent_busy_sessions (
    session_key TEXT PRIMARY KEY,
    proj_id     TEXT NOT NULL DEFAULT '',
    busy        INTEGER NOT NULL DEFAULT 0,
    reason      TEXT NOT NULL DEFAULT '',
    ts          REAL NOT NULL DEFAULT 0,
    stone_id    TEXT NOT NULL DEFAULT '',
    updated_at  TEXT NOT NULL DEFAULT ''
)
"""


def _agent_busy_ensure_table(conn: "sqlite3.Connection") -> None:
    """M1890-A: create per-session table on first use (idempotent)."""
    conn.execute(_BUSY_SESSIONS_TABLE_DDL)


def _agent_busy_load_from_db():
    """M1890-A: Hydrate _agent_busy_state and _agent_busy_sessions from SQLite on startup.
    Per-session rows (agent_busy_sessions table) take priority; legacy blob is fallback for
    cold migration (first run after upgrade)."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        _agent_busy_ensure_table(conn)
        conn.commit()

        # --- proj-level mirror (legacy blob, low priority) ---
        row = conn.execute("SELECT value_json FROM user_settings WHERE key='_agent_busy_state'").fetchone()
        if row and row[0]:
            data = json.loads(row[0])
            if isinstance(data, dict):
                canonical_map: dict[str, str] = {}
                try:
                    for d in PROJECTS_DIR.iterdir():
                        if d.is_dir():
                            canonical_map[d.name.lower()] = d.name
                except Exception:
                    pass
                for k, v in data.items():
                    canonical = canonical_map.get(k.lower(), k)
                    existing = _agent_busy_state.get(canonical)
                    if not existing or v.get("ts", 0) > existing.get("ts", 0):
                        _agent_busy_state[canonical] = v

        # --- M1890-A: per-session rows (primary, atomic UPSERT on every write) ---
        per_session_rows = conn.execute(
            "SELECT session_key, proj_id, busy, reason, ts, stone_id FROM agent_busy_sessions"
        ).fetchall()
        _loaded_per_row: dict[str, dict] = {}
        for sk, pid, bsy, rsn, ts_, sid in per_session_rows:
            _loaded_per_row[sk] = {
                "proj_id": pid, "busy": bool(bsy), "reason": rsn,
                "ts": ts_, "stone_id": sid or None,
            }

        # Fallback: legacy blob for sessions not yet in the new table (cold migration).
        row2 = conn.execute("SELECT value_json FROM user_settings WHERE key='_agent_busy_sessions'").fetchone()
        _legacy_blob: dict = {}
        if row2 and row2[0]:
            _legacy_blob = json.loads(row2[0]) if row2[0] else {}
            if not isinstance(_legacy_blob, dict):
                _legacy_blob = {}

        # Merge: per-row wins over blob (per-row is more recent; blob is last-restart snapshot).
        for sk, rec in _legacy_blob.items():
            if sk not in _loaded_per_row:
                _loaded_per_row[sk] = rec

        _agent_busy_sessions.update(_loaded_per_row)

        # M1844: tool_start records from dead processes must not persist across restart.
        _dirty = False
        for _sk, _sv in list(_agent_busy_sessions.items()):
            if isinstance(_sv, dict) and _sv.get("reason") == "tool_start" and _sv.get("busy"):
                _agent_busy_sessions[_sk] = dict(_sv, busy=False, reason="agent_stopped",
                                                  note="reset_on_startup_stale_tool_start")
                _dirty = True
        conn.close()
        if _dirty:
            _agent_busy_persist()
    except Exception:
        pass


def _agent_busy_persist(session_key: str | None = None) -> None:
    """M1890-A: Durable per-session UPSERT (atomic, no blob race on SIGTERM).

    If session_key is given, writes only that one row — used by POST /api/agent-busy for
    O(1) single-row UPSERT with immediate commit durability.
    If session_key is None (startup cleanup path), rewrites all in-memory sessions.
    proj-level _agent_busy_state blob is still written for legacy consumers.
    """
    import datetime as _dt
    _now_iso = _dt.datetime.utcnow().isoformat()
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        _agent_busy_ensure_table(conn)
        if session_key is not None:
            rec = _agent_busy_sessions.get(session_key)
            if rec is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO agent_busy_sessions"
                    "(session_key, proj_id, busy, reason, ts, stone_id, updated_at)"
                    " VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (session_key,
                     rec.get("proj_id") or "",
                     1 if rec.get("busy") else 0,
                     (rec.get("reason") or "")[:200],
                     rec.get("ts") or 0.0,
                     rec.get("stone_id") or "",
                     _now_iso),
                )
        else:
            # Bulk rewrite (startup cleanup only — rare path).
            for sk, rec in _agent_busy_sessions.items():
                if not isinstance(rec, dict):
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO agent_busy_sessions"
                    "(session_key, proj_id, busy, reason, ts, stone_id, updated_at)"
                    " VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (sk,
                     rec.get("proj_id") or "",
                     1 if rec.get("busy") else 0,
                     (rec.get("reason") or "")[:200],
                     rec.get("ts") or 0.0,
                     rec.get("stone_id") or "",
                     _now_iso),
                )
        # proj-level mirror (legacy blob — kept for GET /api/agent-busy consumers).
        conn.execute(
            "INSERT OR REPLACE INTO user_settings(key, value_json, updated_at) VALUES(?, ?, ?)",
            ("_agent_busy_state", json.dumps(_agent_busy_state, ensure_ascii=False), _now_iso),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# Restore on startup event (after _NS_EVENTS_DB is set, around L1667).
@app.on_event("startup")
async def _agent_busy_load_startup():
    _agent_busy_load_from_db()


@app.post("/api/agent-busy")
async def agent_busy(request: Request):
    """M1533 v3: agent-wrapper reports busy/idle. Hub uses this to gate queue-continuation poller.
    M1533 v4: on busy=false transition with queued stones present, trigger immediate dispatch
    (no wait for next 10s poll cycle) — avatar flicker and queue idle time both eliminated."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
    proj_id = (body.get("proj_id") or "").strip()
    if not proj_id:
        return JSONResponse({"ok": False, "error": "missing proj_id"}, status_code=400)
    # M1637-fix: normalize proj_id case against PROJECTS_DIR folder names.
    # Hooks derive proj_id from basename(cwd) ("Moat") while MCP tools use the
    # canonical registry name ("MOAT") — mismatch creates split keys in _agent_busy_state.
    try:
        _canonical = next(
            (d.name for d in PROJECTS_DIR.iterdir()
             if d.is_dir() and d.name.lower() == proj_id.lower()),
            None,
        )
        if _canonical:
            proj_id = _canonical
    except Exception:
        pass
    new_busy = bool(body.get("busy", False))
    _reason = str(body.get("reason") or "")
    # M1656: session-scoped busy. is_exec defaults True — old wrappers/harnesses that don't
    # send the field keep the pre-M1656 project-level behavior unchanged.
    _session_key = str(body.get("session_key") or "").strip()
    _is_exec = bool(body.get("is_exec", True))
    # M1770: an exec session's own name embeds its true project (claude-exec-{proj}[-suffix]) —
    # trust that over whatever proj_id the hook computed. Hooks derive proj_id from
    # NS_PROJ_ID/CLAUDE_PROJECT_DIR/os.getcwd() (northstar-stop-idle.py etc.), any of which can
    # transiently read the wrong cwd (e.g. a shell tool mid-turn cd'd elsewhere when the hook
    # fired) and mislabel a correctly-identified session under a different project's proj_id —
    # session_key stays right, proj_id silently goes wrong (observed: claude-exec-UniversEye's
    # busy record tagged proj_id=MOAT while genuinely idle in UniversEye).
    if _session_key:
        _sk_agent, _sk_proj = _parse_exec_session_name(_session_key)
        if _sk_proj and _sk_proj != proj_id:
            proj_id = _sk_proj
    if not _is_exec:
        # Non-exec (user CLI/IDE) session: record for observability only. Must NOT write
        # _agent_busy_state/_session_running_stone or trigger dispatch — otherwise an open
        # user conversation in the project cwd blocks stone dispatch indefinitely (M1635).
        _agent_busy_cli[proj_id] = {"busy": new_busy, "reason": _reason[:200],
                                    "session_key": _session_key, "ts": time.time()}
        return JSONResponse({"ok": True, "scope": "cli", "gates_dispatch": False})
    # M1656-R: single write to the session record (source of truth). stone_id rides along.
    _stone_id = str(body.get("stone_id") or "").strip()
    # Harnesses that send stone_id but no session_key get a synthetic per-project key so
    # the in-flight stone hold still works for them.
    _rec_key = _session_key or f"legacy::{proj_id}"
    _prev_rec = _agent_busy_sessions.get(_rec_key) or {}
    prev_busy = bool(_prev_rec.get("busy", _agent_busy_state.get(proj_id, {}).get("busy", True)))
    # stone_id resolution: explicit on this POST wins; busy heartbeats WITHOUT stone_id
    # preserve the in-flight stone (heartbeats don't carry it); idle clears it.
    if new_busy:
        _eff_stone = _stone_id or (_prev_rec.get("stone_id") if _prev_rec.get("busy") else None)
    else:
        _eff_stone = None
    # M1919: tool_start has a 1h hold and must not be overwritten by compacting.
    # Sequence: PreToolUse → tool_start (1h), then PreCompact fires for a long Bash
    # that hits context limit mid-run → compacting overwrites tool_start → compaction
    # ends → Stop posts idle → poller wakes the session while Bash is still running.
    # Guard: if the current record is tool_start+busy and the incoming reason is
    # compacting, preserve the tool_start record (don't overwrite). Any other
    # transition (including idle/agent_stopped) still proceeds normally.
    _prev_reason = _prev_rec.get("reason", "")
    if (_reason == "compacting" and new_busy
            and _prev_reason == "tool_start" and _prev_rec.get("busy")):
        return JSONResponse({"ok": True, "scope": "session", "guarded": "tool_start_preserved"})
    _agent_busy_sessions[_rec_key] = {
        "proj_id": proj_id, "busy": new_busy, "reason": _reason[:200],
        "ts": time.time(),
        "stone_id": _eff_stone,
    }
    # Legacy proj-level mirror — kept for GET /api/agent-busy consumers and old harnesses.
    _agent_busy_state[proj_id] = {"busy": new_busy, "reason": _reason[:200], "ts": time.time()}
    _agent_busy_persist(_rec_key)  # M1890-A: per-row UPSERT — atomic, survives SIGTERM
    # M1677: busy-state transition → invalidate exec-sessions cache so the session pane
    # (SSE-triggered or next poll) reflects the new state instead of a ≤2s-old snapshot.
    if prev_busy != new_busy:
        _exec_sessions_cache["ts"] = 0.0
    # M1639: task_complete → immediate session_idle SSE (instant avatar/pane update).
    # M1656-R: longest-prefix match so branched child sessions get the SSE too.
    if _reason == "task_complete" and not new_busy:
        try:
            _ls = subprocess.run(["tmux", "list-sessions", "-F", "#{session_name}"], capture_output=True, text=True, timeout=2)
            for _sn in (_ls.stdout or "").splitlines():
                _sn = _sn.strip()
                _ag, _pid = _parse_exec_session_name(_sn)
                # Push only for the reporting session when identifiable; else all proj sessions
                if _pid == proj_id and (not _session_key or _sn == _session_key):
                    _exec_was_running[_sn] = False  # reset transition detector
                    _push_session_idle(_sn, proj_id)
        except Exception:
            pass
    dispatched = False
    # M1533 v4: busy→idle transition + queued stones present → immediate wake.
    # M1656-R: no separate inflight mutex — _send_exec_wake's atomic 90s dedup
    # (with task_complete force-reset) is the single anti-duplicate gate.
    if prev_busy and not new_busy:
        try:
            _qdb = sqlite3.connect(str(_NS_EVENTS_DB))
            try:
                queued_count = _qdb.execute(
                    "SELECT COUNT(*) FROM milestones_store WHERE proj_id=? AND status='queued' AND COALESCE(held,0)=0",
                    (proj_id,)
                ).fetchone()[0]
            finally:
                _qdb.close()
            if queued_count > 0:
                def _dispatch_blocking():
                    try:
                        ls = subprocess.run(["tmux", "list-sessions", "-F", "#{session_name}"], capture_output=True, text=True, timeout=2)
                    except Exception:
                        return False
                    _names = [x.strip() for x in (ls.stdout or "").splitlines()]
                    # M1656-R: prefer waking the session that just went idle (it is the free one);
                    # fall back to the project's main session.
                    _targets = []
                    if _session_key and _session_key in _names:
                        _targets.append(_session_key)
                    for sn in _names:
                        _ag2, _pid2 = _parse_exec_session_name(sn)
                        if _pid2 == proj_id and sn not in _targets:
                            _targets.append(sn)
                    for sn in _targets:
                        if _session_is_busy(sn):
                            continue  # never inject into a working session
                        # M1676: only wake a session with stones it can actually claim
                        if _session_claimable_queued_count(proj_id, sn) == 0:
                            continue
                        try:
                            _do_reset = _reason == "task_complete"
                            if _send_exec_wake(sn, proj_id, force_dedup_reset=_do_reset):
                                return True
                        except Exception:
                            return False
                    return False
                try:
                    dispatched = await asyncio.to_thread(_dispatch_blocking)
                except Exception:
                    pass
        except Exception:
            pass
    return JSONResponse({"ok": True, "proj_id": proj_id, "busy": new_busy, "immediate_dispatch": dispatched})


@app.get("/api/agent-busy")
async def agent_busy_get(proj_id: str = ""):
    """Read agent busy state. Returns null when wrapper hasn't reported (poller falls back to pane scrape)."""
    import time as _time
    if proj_id:
        # M1637-fix: normalize proj_id case for GET queries too
        try:
            _c = next((d.name for d in PROJECTS_DIR.iterdir() if d.is_dir() and d.name.lower() == proj_id.lower()), None)
            if _c:
                proj_id = _c
        except Exception:
            pass
        oob = _agent_busy_state.get(proj_id) or {}
        # M1574: apply TTL — stale entries report busy=false
        if oob and (_time.time() - oob.get("ts", 0)) >= _OOB_STALE_SECS:
            oob = {**oob, "busy": False, "stale": True}
        # M1656-R: authoritative derived busy + per-session detail for accurate UI display
        oob = dict(oob)
        oob["busy"] = _oob_is_busy(proj_id)
        oob["sessions"] = {
            _sk: {"busy": _session_is_busy(_sk), "reason": rec.get("reason", ""),
                  "stone_id": rec.get("stone_id"), "ts": rec.get("ts", 0)}
            for _sk, rec in _agent_busy_sessions.items() if rec.get("proj_id") == proj_id
        }
        return JSONResponse(oob)
    # Return all entries with TTL applied
    now = _time.time()
    result = {}
    for pid, oob in _agent_busy_state.items():
        if oob and (now - oob.get("ts", 0)) >= _OOB_STALE_SECS:
            result[pid] = {**oob, "busy": False, "stale": True}
        else:
            result[pid] = oob
    return JSONResponse(result)


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

@app.post("/api/tool-trace")
async def post_tool_trace(request: Request):
    """M775: Record per-tool-call trace for causality dataset (SWE-bench style)."""
    try:
        from datetime import datetime as _dt
        data = await request.json()
        ts = data.get("ts") or _dt.utcnow().isoformat() + "Z"
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute(
            "INSERT INTO tool_trace(ts,proj_id,stone_id,session_id,tool_name,input_summary,output_summary,duration_ms) VALUES(?,?,?,?,?,?,?,?)",
            (ts, data.get("proj_id",""), data.get("stone_id",""), data.get("session_id",""),
             data.get("tool_name",""), data.get("input_summary",""), data.get("output_summary",""),
             data.get("duration_ms"))
        )
        conn.commit()
        conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/api/tool-trace")
async def get_tool_trace(stone_id: str = "", proj_id: str = "", limit: int = 500):
    """M775: Query tool trace for a stone (for causality export)."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.row_factory = sqlite3.Row
        q = "SELECT ts,proj_id,stone_id,session_id,tool_name,input_summary,output_summary,duration_ms FROM tool_trace WHERE 1=1"
        params: list = []
        if stone_id:
            q += " AND stone_id=?"; params.append(stone_id)
        if proj_id:
            q += " AND proj_id=?"; params.append(proj_id)
        q += " ORDER BY ts ASC LIMIT ?"
        params.append(limit)
        rows = conn.execute(q, params).fetchall()
        conn.close()
        return JSONResponse({"ok": True, "traces": [dict(r) for r in rows]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/api/action-log")
async def get_action_log(proj_id: str = "", stone_id: str = "", limit: int = 200, since_minutes: int = 0, action: str = ""):
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
        if action:
            q += " AND action=?"; params.append(action)
        if since_minutes > 0:
            cutoff = (_dt.utcnow() - _td(minutes=since_minutes)).isoformat()
            q += " AND ts >= ?"; params.append(cutoff)
        q += " ORDER BY ts DESC LIMIT ?"; params.append(limit)
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
        conn.close()
        return JSONResponse({"ok": True, "rows": rows, "actions": rows, "count": len(rows)})
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
        _username = Path.home().name
        _tdir = Path.home() / ".claude" / "projects" / f"-home-{_username}-Project-{proj_id}"
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
    """M1368: Load all projects from SQLite project_meta (north-star.md no longer primary).
    Falls back to directory scan for projects not yet in DB.
    """
    projects = []
    db_proj_ids: set[str] = set()

    # Primary: SQLite project_meta
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute("SELECT proj_id FROM project_meta").fetchall()
        conn.close()
        db_proj_ids = {r[0] for r in rows}
    except Exception:
        pass

    if PROJECTS_DIR.exists():
        for proj_dir in sorted(PROJECTS_DIR.iterdir()):
            # M1368: use DB as primary; only process md-fallback for dirs not yet in DB
            in_db = proj_dir.name in db_proj_ids
            md = proj_dir / "north-star.md"
            if in_db:
                data = _db_load_project(proj_dir.name) or {}
                if not data.get("name"):
                    continue
            elif md.exists():
                data = _parse_md_frontmatter(md)
            else:
                continue
            if data.get("name"):
                    data["id"] = proj_dir.name  # preserve case to match folder
                    data["file_path"] = str(md) if md.exists() else str(proj_dir)
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
                                # Also check nested dirs one level deep
                                if _candidate.is_dir():
                                    for _nested in _candidate.iterdir():
                                        if _nested.is_dir() and _nested.name.lower() == _target:
                                            data["repo_path"] = str(_nested)
                                            break
                                    if data.get("repo_path"):
                                        break
                    # M1368: stale always False — SQLite is primary, md mtime not meaningful
                    data["last_updated"] = proj_dir.stat().st_mtime
                    data["stale"] = False
                    # M1196: project start timestamp (epoch) — used by frontend D+N display.
                    # M1245 v2: git first-commit date (real project start) > hub dir ctime fallback.
                    # Hub dir ctime is unreliable — recreated on migration/reinstall.
                    _proj_started_ts = None
                    _repo = data.get("repo_path") or ""
                    if _repo:
                        try:
                            import subprocess as _sp
                            _git_out = _sp.run(
                                ["git", "log", "--format=%ct"],
                                cwd=_repo, capture_output=True, text=True, timeout=3
                            )
                            # last line = oldest commit (first in history)
                            _lines = [l.strip() for l in _git_out.stdout.splitlines() if l.strip().isdigit()]
                            if _lines:
                                _proj_started_ts = int(_lines[-1])
                        except Exception:
                            pass
                    if _proj_started_ts is None:
                        try:
                            _proj_started_ts = int(proj_dir.stat().st_ctime)
                        except Exception:
                            pass
                    data["proj_started_ts"] = _proj_started_ts
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
_M1434_backfill_done: bool = False  # M1434: JSONL skill backfill runs once per server process
_M190_DISABLED: bool = True  # M1961: temporary disable — allow claude→claude consecutive appends for long-running tasks

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
            conv = m.get("conversation") or []

            # Build full corpus: stone text + last claude message
            last_claude_text = next(
                (c.get("text") or c.get("content") or ""
                 for c in reversed(conv)
                 if c.get("role") == "claude"),
                ""
            )
            full_corpus = " ".join(filter(None, [text, last_claude_text]))

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

            # Build object: last claude reply, else truncated text
            if last_claude_text:
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
        # M775: tool-level trace for causality dataset (SWE-bench style step-level)
        conn.execute("""CREATE TABLE IF NOT EXISTS tool_trace (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            proj_id TEXT,
            stone_id TEXT,
            session_id TEXT,
            tool_name TEXT NOT NULL,
            input_summary TEXT,
            output_summary TEXT,
            duration_ms INTEGER
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_trace_stone ON tool_trace(stone_id, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_trace_ts ON tool_trace(ts)")
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
        # M1234-D: permanent archive for done milestones — keeps milestones_store lean
        conn.execute("""CREATE TABLE IF NOT EXISTS milestones_archive (
            proj_id TEXT NOT NULL,
            stone_id TEXT NOT NULL,
            data_json TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            PRIMARY KEY (proj_id, stone_id)
        )""")
        # M1355: per-project user-defined concept graph (LLM-style layered nodes).
        # parents_json = JSON array of parent node_ids; layout (x, y) optional.
        conn.execute("""CREATE TABLE IF NOT EXISTS concept_graph_nodes (
            proj_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            layer REAL NOT NULL,
            name TEXT NOT NULL,
            parents_json TEXT NOT NULL DEFAULT '[]',
            x REAL,
            y REAL,
            layer_order REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (proj_id, node_id)
        )""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_cg_proj_layer
            ON concept_graph_nodes(proj_id, layer)""")
        # M1355: add layer_order column to pre-existing tables (idempotent)
        try:
            conn.execute("ALTER TABLE concept_graph_nodes ADD COLUMN layer_order REAL")
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception:
        pass

_ns_primary_init()


def _archive_done_milestones():
    """M1234-D: Move done=1 stones from milestones_store → milestones_archive permanently.
    Runs in background thread every 6 hours. Reduces milestones_store size so queries stay fast."""
    import datetime as _dt_arch
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        now = _dt_arch.datetime.utcnow().isoformat()
        # Find all done stones not yet archived
        rows = conn.execute(
            "SELECT proj_id, stone_id, data_json FROM milestones_store WHERE done=1"
        ).fetchall()
        archived = 0
        for proj_id, stone_id, data_json in rows:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO milestones_archive (proj_id, stone_id, data_json, archived_at) VALUES (?,?,?,?)",
                    (proj_id, stone_id, data_json, now)
                )
                conn.execute(
                    "DELETE FROM milestones_store WHERE proj_id=? AND stone_id=?",
                    (proj_id, stone_id)
                )
                archived += 1
            except Exception:
                pass
        conn.commit()
        conn.close()
        if archived:
            print(f"[M1234-D] Archived {archived} done stones to milestones_archive.", flush=True)
    except Exception as _e:
        print(f"[M1234-D] archive error: {_e}", flush=True)


def _archive_daemon():
    """Background thread: archive done milestones every 6 hours."""
    while True:
        time.sleep(21600)  # 6 hours
        _archive_done_milestones()


threading.Thread(target=_archive_daemon, daemon=True, name="archive-daemon").start()


def _migrate_yaml_to_sqlite():
    """M287: One-time migration — seed SQLite from all existing YAML north-star.md files."""
    import copy as _cp_mig
    if not PROJECTS_DIR.exists():
        return
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        already_migrated = {r[0] for r in conn.execute("SELECT proj_id FROM project_meta").fetchall()}
        # M1406: skip projects that were explicitly deleted (prevent resurrection on restart)
        try:
            deleted = {r[0] for r in conn.execute("SELECT proj_id FROM deleted_projects").fetchall()}
            already_migrated |= deleted
        except Exception:
            pass
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
    # M1732: IP must run BEFORE phone — the phone regex's loose \d{1,3}[.]?\d{2,4}...
    # shape matches the first 3 octets of an IPv4 address (e.g. "100.110.117" out of
    # "100.110.117.8"), consuming it before the IP pattern ever runs and leaving a
    # malformed "<PHONE>.8" residue instead of a clean "<IP>". Verified via direct test.
    (_re_pii.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<IP>"),
    (_re_pii.compile(r"\b\+?\d{1,3}[-\s.]?\(?\d{2,4}\)?[-\s.]?\d{3,4}[-\s.]?\d{3,4}\b"), "<PHONE>"),
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
                ["status","text","claude_ack","held","layer","parent_id","user_added_at"]
                if m.get(k) is not None}
            if ev_type == "status_changed" and prev:
                payload_data["status_before"] = prev.get("status", "")
            payload = json.dumps(payload_data, sort_keys=True)
            # Skip stone_updated if content unchanged vs prev (prevents event spam)
            if ev_type == "stone_updated" and prev is not None:
                prev_payload = json.dumps({k: prev.get(k) for k in
                    ["status","text","claude_ack","held","layer","parent_id","user_added_at"]
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

    # M1368: Capture prev milestones from SQLite only
    prev_data = _db_load_project(proj_id)
    prev_ms = (prev_data or {}).get("milestones", [])

    _md_path = proj_dir / "north-star.md"  # kept for cache invalidation only

    # M287: Primary write — SQLite first, fast (~1ms)
    _db_save_project(proj_id, data)
    # Invalidate mtime cache so next _parse_md_frontmatter skips L1 and hits SQLite (L2)
    _parse_cache.pop(str(_md_path), None)

    # M215: event log (metrics/audit) — background OK, not on read path
    _record_stone_events(proj_id, data.get("milestones", []), prev_ms)

    # M649: YAML backup disabled — SQLite is primary store, YAML write was adding ~350ms background load

    # M278: Turso cloud sync
    _t = _threading.Thread(target=_turso_sync_project, args=(proj_id, data.get("milestones", [])), daemon=True)
    _t.start()


def _load_failure_reflections(proj_id: str, limit: int = 5) -> str:
    """M1021: Failure reflection injection — surface past reopened/failed stones so Claude
    avoids repeating the same mistakes. Reads reopen_count + failure_reason from DB."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            """SELECT stone_id, reopen_count, failure_reason, outcome_label, data_json
               FROM milestones_store
               WHERE proj_id=? AND (reopen_count > 0 OR failure_reason IS NOT NULL)
               ORDER BY reopen_count DESC, updated_at DESC
               LIMIT ?""",
            (proj_id, limit)
        ).fetchall()
        conn.close()
    except Exception:
        return ""
    if not rows:
        return ""
    lines = ["[REFLECTION] Past failures / reopened stones — learn from these, avoid repeating:"]
    for sid, reopen, reason, label, djson in rows:
        try:
            d = json.loads(djson or "{}")
            txt = (d.get("text") or "")[:80].replace("\n", " ")
        except Exception:
            txt = ""
        parts = [f"  • {sid}"]
        if reopen: parts.append(f"reopened×{reopen}")
        if reason: parts.append(f"reason: {reason[:60]}")
        if label: parts.append(f"label: {label}")
        if txt: parts.append(f"text: \"{txt}\"")
        lines.append(" | ".join(parts))
    return "\n".join(lines) + "\n\n"


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
    """BUG-03 fix: redirect /landing → /northstar dashboard (no separate landing page)."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/northstar", status_code=302)


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
async def northstar_get(slim: bool = False):
    # M1493: offload sync SQLite scan to thread so event loop stays free during load
    projects = await asyncio.to_thread(_load_projects)
    # Strip internal fields before returning
    clean = [{k: v for k, v in p.items() if k != "_body"} for p in projects]
    if slim:
        # M1418-D: swimlane only needs card-level fields — strip heavy milestone arrays and meta blobs
        _SLIM_KEYS = {"id", "name", "status", "metric", "position_x", "layer", "agent", "model",
                      "repo_path", "deadline", "proj_started_ts", "stale", "milestones", "north_stars",
                      "parent", "parents"}
        clean = [{k: ([] if k == "milestones" else v) for k, v in p.items() if k in _SLIM_KEYS} for p in clean]
    return JSONResponse(clean)


# M1345: ttyd web-terminal sidecar removed — xterm.js is the only terminal surface.

# M1244 mobile vkey: forward a single keyboard event to a tmux session via
# `tmux send-keys`. The ttyd sidecar is attached to the same session so the
# key appears in the user's mobile terminal exactly as if typed locally.
# Allowlist prevents arbitrary command injection.
_TMUX_VKEY_ALLOW = {
    "Escape": "Escape", "Tab": "Tab", "Enter": "Enter", "Space": "Space",
    "BSpace": "BSpace", "Up": "Up", "Down": "Down", "Left": "Left", "Right": "Right",
    "Home": "Home", "End": "End", "PageUp": "PageUp", "PageDown": "PageDown",
    "C-c": "C-c", "C-d": "C-d", "C-z": "C-z", "C-l": "C-l", "C-r": "C-r",
    "C-a": "C-a", "C-e": "C-e", "C-w": "C-w", "C-u": "C-u", "C-k": "C-k",
    "C-b": "C-b",  # tmux prefix
    "M-.": "M-.",  # alt-dot (last arg recall in shell)
    "/": "/", "|": "|", "\\": "\\", "~": "~", "`": "`",
    # M1244 scroll: enter tmux copy mode (prefix + [) and exit ('q')
    "ScrollMode": ["C-b", "["],
    "q": "q",
}

@app.post("/api/tmux/send-key")
async def tmux_send_key(request: Request):
    body = await request.json()
    session = (body.get("session") or "").strip()
    key = body.get("key") or ""
    if not session or not key:
        return JSONResponse({"ok": False, "error": "session and key required"}, status_code=400)
    if key not in _TMUX_VKEY_ALLOW:
        return JSONResponse({"ok": False, "error": f"key '{key}' not in allowlist"}, status_code=400)
    # Verify session exists before sending — avoids leaking allowlist for arbitrary sessions.
    chk = await asyncio.to_thread(
        subprocess.run, ["tmux", "has-session", "-t", f"={session}"], capture_output=True, timeout=2)
    if chk.returncode != 0:
        return JSONResponse({"ok": False, "error": f"tmux session '{session}' not found"}, status_code=404)
    spec = _TMUX_VKEY_ALLOW[key]
    if isinstance(spec, list):
        for k in spec:
            await asyncio.to_thread(
                subprocess.run, ["tmux", "send-keys", "-t", session, k], capture_output=True, timeout=2)
    else:
        await asyncio.to_thread(
            subprocess.run, ["tmux", "send-keys", "-t", session, spec], capture_output=True, timeout=2)
    return JSONResponse({"ok": True})




@app.post("/api/northstar")
async def northstar_save(request: Request):
    """BUG-04 fix: single-dict POST intended for project creation must use /api/northstar/create.
    Bulk list save (internal UI sync) is still supported."""
    data = await request.json()
    if isinstance(data, list):
        # Bulk save — write each project to its file (internal UI sync path)
        for p in data:
            proj_id = p.get("id", p.get("name", "").lower().replace(" ", "-"))
            if proj_id:
                _save_project(proj_id, {k: v for k, v in p.items() if k not in ("stale","last_updated","file_path")})
        return JSONResponse({"ok": True})
    # Single-dict: user likely tried to create a project via wrong endpoint
    return JSONResponse(
        {"ok": False, "error": "Use POST /api/northstar/create to create a project",
         "hint": "Body: {\"id\": \"MyProj\", \"name\": \"My Project\", \"repo_path\": \"/path\"}"},
        status_code=400
    )


@app.get("/api/northstar/{proj_id}/okrs")
async def northstar_okrs(proj_id: str):
    """Extract OKRs from north-star.md body (M1368: SQLite-first, _body from YAML fallback)."""
    import re as _re
    md = PROJECTS_DIR / proj_id / "north-star.md"
    data = _parse_md_frontmatter(md) if md.exists() else {}
    if not data:
        return JSONResponse({"ok": False, "okrs": [], "section": ""})
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
    log_entries = body.get("log", [])           # project progress log

    if not milestones:
        return JSONResponse({"ok": False, "error": "no milestones provided"})

    ms_text = "\n".join(f"{i+1}. [{('DONE' if m.get('done') else 'pending')}] {m.get('text','')}"
                        for i, m in enumerate(milestones))
    log_text = "\n".join(f"  {l.get('date','')} — {l.get('text','')}"
                         for l in log_entries[-10:]) or "  (no log entries)"

    prompt = f"""You are analyzing a project's milestone completion status based on actual work evidence.

PROJECT: {proj_id}

MILESTONES (current state):
{ms_text}

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

    # M1368: SQLite-first write-back
    data = _db_load_project(proj_id)
    if data:
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

    # M1368: SQLite-first — no md.exists() gate
    data = _db_load_project(proj_id)
    if data is None:
        return JSONResponse({"ok": False, "error": f"project {proj_id} not found"})
    old = data.get("current", "—")
    if str(old) == current:
        return JSONResponse({"ok": True, "updated": False, "reason": "no change"})

    data["current"] = current
    _save_project(proj_id, data)
    return JSONResponse({"ok": True, "updated": True, "old": str(old), "new": current})


@app.post("/api/northstar/{proj_id}/session-log")
async def session_log(proj_id: str, request: Request):
    """Append a session summary entry to the project log."""
    body = await request.json()
    entry_text = body.get("text", "").strip()
    entry_date = body.get("date", "")
    if not entry_text or not entry_date:
        return JSONResponse({"ok": False, "error": "text and date required"})

    # M1368: SQLite-first — no md.exists() gate
    data = _db_load_project(proj_id)
    if data is None:
        return JSONResponse({"ok": False, "error": f"project {proj_id} not found"})

    log = data.get("log", [])
    # Avoid duplicate entries (same date + same text prefix)
    prefix = entry_text[:40]
    if not any(e.get("date") == entry_date and e.get("text","")[:40] == prefix for e in log):
        log.append({"date": entry_date, "text": entry_text})
        data["log"] = log
        _save_project(proj_id, data)
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
    """M1235: CTX disabled."""
    return JSONResponse({"ok": False, "error": "CTX disabled (M1235)"}, status_code=404)


@app.get("/api/ctx/recent-retrievals")
async def ctx_recent_retrievals(limit: int = 50):
    """M1235: CTX disabled."""
    return JSONResponse({"ok": False, "error": "CTX disabled (M1235)", "events": []})


@app.get("/api/ctx-pulse")
async def ctx_pulse():
    """M1235: CTX disabled."""
    return JSONResponse({"ok": False, "topics": [], "error": "CTX disabled (M1235)"})


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
    Saves to ~/.hub/uploads/<proj_id>/ and returns a local URL.
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
    Saves to ~/.hub/uploads/<proj_id>/ and returns a local URL.
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
        "url": "https://github.com/pluto2060/CTX",
        "api": "https://api.github.com/repos/pluto2060/CTX",
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
        "url": "https://github.com/pluto2060/claude-ns-hub",
        "api": "https://api.github.com/repos/pluto2060/claude-ns-hub",
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

# M1347: _sync_substars_to_claude_md removed — violated Moat/CLAUDE.md:63 rule
# ("전역 CLAUDE.md는 hub 관련 내용 기입 안 함"). North-star state belongs in
# the hub DB / hub UI, not in project CLAUDE.md files.


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
# M1685: registry for the live-terminal exec-attach relay (WS /ws/session/{proj}?tmux_session=...).
# That code path spawns `tmux attach-session` PtyProcess objects but never registers them
# anywhere else — unlike _sessions/_pty_subscribers above. If the browser's TCP connection
# dies without a clean close frame (mobile background/sleep, network drop), the websocket
# coroutine can sit blocked in receive_text() indefinitely (uvicorn's ws ping/pong only
# catches DEAD sockets, not a genuinely-idle-but-unclosed one), leaving the tmux client
# permanently attached — which then makes _send_exec_wake's M1337 "user is watching" skip
# fire forever, silently blocking all dispatch to that session. Tracked here so the poller
# can self-heal stale attaches. key = tmux_session_name → {pid, attached_at}.
_exec_attach_relay: dict[str, dict] = {}
_EXEC_ATTACH_STALE_SECS = 900  # 15min — well past any real interactive terminal glance

# M1763: tmux-output HTTP poll viewer registry — tracks sessions currently viewed via
# the /tmux-output endpoint (which uses capture-pane, not a tmux client, so M1337's
# list-clients check never fires). TTL=30s; reset on each poll. _send_exec_wake checks
# this alongside list-clients so both viewer paths block wake injection.
_tmux_output_viewers: dict[str, float] = {}  # session_name → last_poll_time
_TMUX_OUTPUT_VIEWER_TTL = 30.0  # seconds of inactivity before no longer "watching"
# M1836-v2: per-session ring buffer that survives ESC[3J scrollback wipe.
# On each poll, new lines are appended; the buffer is served as the terminal output.
_pane_ring_buf: dict[str, list] = {}   # session_name → list of lines (capped at _PANE_BUF_MAX)
_pane_ring_prev: dict[str, list] = {}  # session_name → last stripped line list (for diff)
_pane_ring_lock: dict[str, asyncio.Lock] = {}  # per-session lock (prevents concurrent extend)
_PANE_BUF_MAX = 2000                   # max lines retained per session


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
        _pid = getattr(proc, "pid", None)
        try:
            proc.terminate(force=True)
        except Exception:
            pass
        # Guarantee cleanup: if ptyprocess terminate() fails (claude ignores SIGHUP/SIGINT),
        # kill by PID directly. proc.isalive() can return False while OS process still runs.
        if _pid:
            try:
                import signal as _sig
                os.kill(_pid, _sig.SIGKILL)
            except (ProcessLookupError, OSError):
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
            # M1324-P1: non-blocking — urlopen must not run on the event loop thread
            def _fetch():
                return _ur.urlopen("https://pypi.org/pypi/northstar-hub/json", timeout=5).read()
            data = await _aio.to_thread(_fetch)
            latest = json.loads(data)["info"]["version"]
            if latest != current:
                print(f"\n⟳ northstar-hub {latest} available (current: {current})\n"
                      f"  pip install --upgrade northstar-hub\n", flush=True)
        except Exception:
            pass  # offline or not installed as package — silent
    asyncio.create_task(_do_check())


# M1345 P0: event-loop blocked detector + threadpool cap. Emits a log line every
# time the loop is stalled > 250 ms so chat-input vs. video-stutter correlation
# becomes measurable instead of subjective.
@app.on_event("startup")
async def _loop_health_monitor():
    import asyncio as _aio
    import concurrent.futures as _cf

    # Cap to_thread workers so unbounded subprocess fan-out can't starve the host.
    try:
        _loop = _aio.get_running_loop()
        _loop.set_default_executor(_cf.ThreadPoolExecutor(max_workers=16,
                                                          thread_name_prefix="hub-to-thread"))
    except Exception:
        pass

    async def _monitor():
        TICK = 0.1
        STALL_MS = 250
        prev = time.monotonic()
        while True:
            await _aio.sleep(TICK)
            now = time.monotonic()
            dt_ms = (now - prev - TICK) * 1000
            prev = now
            if dt_ms > STALL_MS:
                print(f"[loop-stall] {dt_ms:.0f} ms at {time.strftime('%H:%M:%S')}",
                      flush=True)
    _aio.create_task(_monitor())


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
    import asyncio, threading
    def _hub_active_loop():
        import time
        while True:
            time.sleep(86400)  # 24h — DAU tracking
            _record_usage_event("hub_active")
            try:
                _maybe_upload_session_aggregate()  # M1516: daily session aggregate
            except Exception:
                pass
    threading.Thread(target=_hub_active_loop, daemon=True).start()
    # M1516: also try uploading once on startup so first day of activity isn't lost
    def _agg_startup_attempt():
        import time
        time.sleep(60)  # let server settle
        try:
            _maybe_upload_session_aggregate()
        except Exception:
            pass
    threading.Thread(target=_agg_startup_attempt, daemon=True).start()
    # M1516-ext: raw table upload loop — every 30min, watermark-based, no daily gate
    def _raw_table_upload_loop():
        import time
        while True:
            time.sleep(1800)  # 30min
            if not _get_consent().get("data_collection", True):
                continue
            try:
                state = {}
                if _AGG_STATE_FILE.exists():
                    try:
                        state = json.loads(_AGG_STATE_FILE.read_text())
                    except Exception:
                        pass
                state = _upload_raw_tables(state)
                _AGG_STATE_FILE.write_text(json.dumps(state))
            except Exception:
                pass
    threading.Thread(target=_raw_table_upload_loop, daemon=True).start()

@app.on_event("startup")
async def _expose_ports_to_tailscale():
    """Auto-expose hub ports to all online Tailscale Windows clients on startup."""
    import subprocess
    # M1694b: was hardcoded [9000], drifted from actual runtime HUB_PORT (9001 in this
    # deployment) — wsl-expose was silently opening the wrong port. Use the same PORT
    # constant the server itself binds to (module-level, reads HUB_PORT env var).
    for port in [PORT]:
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
    M1925: config priority: ~/.hub-litellm.yaml (hub-canonical) → ~/.rsk-litellm.yaml (legacy).
    API keys sourced from ~/.config/hub/env (EnvironmentFile) or ~/.claude/env/shared.env."""
    import shutil as _shutil
    port = 4100
    # M1925: prefer hub-canonical config over legacy rsk path
    config = _HUB_LITELLM_CONFIG if _HUB_LITELLM_CONFIG.exists() else Path.home() / ".rsk-litellm.yaml"
    log = Path.home() / ".hub-litellm.log"
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
    # M1925: Load API keys — priority: ~/.config/hub/env > ~/.claude/env/shared.env
    env = dict(os.environ)
    def _load_env_file(path: Path):
        try:
            for line in path.read_text().splitlines():
                line = line.strip()
                if line.startswith("export "):
                    line = line[7:]
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip().strip('"').strip("'")
        except Exception:
            pass
    _load_env_file(Path.home() / ".claude" / "env" / "shared.env")   # lower priority
    _load_env_file(Path.home() / ".config" / "hub" / "env")           # higher priority (wins)
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
async def _start_bg_tmux_poller():
    """M1826-v2: Background tmux state refresher — pre-fetches list-sessions + list-panes every 3s
    so the /api/exec-sessions handler reads from memory (0 subprocess cost) instead of forking tmux
    at request time (was 30-300ms under load)."""
    async def _poll():
        while True:
            try:
                async def _run(*cmd):
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3)
                    return stdout.decode("utf-8", errors="replace") if stdout else ""
                ls, lp = await asyncio.gather(
                    _run("tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"),
                    _run("tmux", "list-panes", "-a", "-F", "#{session_name} #{pane_current_command}"),
                )
                _bg_tmux_state["ls"] = ls
                _bg_tmux_state["lp"] = lp
                _bg_tmux_state["ts"] = time.monotonic()
            except Exception:
                pass
            await asyncio.sleep(_BG_TMUX_INTERVAL)
    asyncio.ensure_future(_poll())


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
                    # M1368: SQLite-first; skip if no DB record
                    proj = _db_load_project(proj_id)
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
                    _pending_reply_owner_m = None
                    for m in raw_ms:
                        if not isinstance(m, dict): continue
                        # M225: held stones are completely excluded from claude token consumption
                        if m.get("held"): continue
                        conv = m.get("conversation") or []
                        if conv and isinstance(conv, list) and conv[-1].get("role") == "user":
                            pending_reply_ids.append(m.get("id"))
                            if _pending_reply_owner_m is None:
                                _pending_reply_owner_m = m
                    if pending_reply_ids:
                        # M131-b-followup: resolve via the owning substar's assigned_session
                        # instead of always targeting the main session first.
                        session_name = _owning_exec_session_name(proj_id, _pending_reply_owner_m or {}, proj)
                        # M1940: use bg poller cache instead of subprocess fork per dispatch cycle
                        _cached_ls = _bg_tmux_state.get("ls", "")
                        if _cached_ls:
                            _live_set = {l.split(":")[0] if ":" in l else l for l in _cached_ls.splitlines() if l.strip()}
                            _session_alive = session_name in _live_set
                        else:
                            _chk = subprocess.run(["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
                            _session_alive = _chk.returncode == 0
                        if _session_alive:
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
                                    f"  2. Reply: STRUCTURED (bullets `-`, numbered `1.`, table `|`) → no line limit. Prose → ≤3 lines.\n"
                                    f"  3. If prose overflow: write docs/ns-replies/<DATE>-<MID>.md\n"
                                    f"     and reference the path in the reply\n"
                                    f"  4. Do NOT add arbitrary claude_comment / append_message to other stones\n"
                                    f"  5. Replying does NOT change the stone's status\n"
                                    f"  6. M270: If stone is a CLEAR TASK → 1-line ack only, NO questions.\n"
                                    f"     Ask a question ONLY when critical info is genuinely missing.\n"
                                    f"     Prefer silence over unnecessary questions (token waste).\n\n"
                                    f"  7. LANGUAGE: reply in the same language as the stone text.\n"
                                    f"     Korean stone → Korean reply. English stone → English reply.\n\n"
                                    f"ACTION: GET http://127.0.0.1:{PORT}/api/northstar/{proj_id}/milestones,\n"
                                    f"  for each stone above, read its last user message, then\n"
                                    f"  PATCH milestones/<MID> with append_message {{role:'claude', text:'<structured: no limit | prose: ≤3 lines>'}}.\n"
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
                # M1179: purge expired blink_state entries (>24h) so _blinkCenterSave NO-OP doesn't leak
                _blink_hard_ms = 24 * 60 * 60 * 1000
                _now_ms = int(__import__('time').time() * 1000)
                _bs_keys = conn.execute(
                    "SELECT key, value_json FROM user_settings WHERE key LIKE 'blink_state_%'"
                ).fetchall() if conn else []
                _blink_cleared = 0
                for _bk, _bv in _bs_keys:
                    try:
                        _bd = json.loads(_bv)
                        _changed = False
                        for _sk in list(_bd.keys()):
                            _entry = _bd[_sk]
                            if isinstance(_entry, dict) and (_now_ms - _entry.get('ts', 0)) >= _blink_hard_ms:
                                _bd[_sk] = {'ts': _entry.get('ts', 0), 'mids': {}}
                                _changed = True
                                _blink_cleared += 1
                        if _changed:
                            conn.execute("UPDATE user_settings SET value_json=? WHERE key=?", (json.dumps(_bd), _bk))
                    except Exception:
                        pass
                if _blink_cleared:
                    conn.commit()
                    print(f"[hub] blink_state cleanup: cleared {_blink_cleared} expired entries", file=sys.stderr)
            except Exception:
                pass
            await asyncio.sleep(86400)  # 24h
    # Run first pass after 60s (let startup settle)
    async def _delayed_start():
        await asyncio.sleep(60)
        await _cleanup()
    asyncio.create_task(_delayed_start())


@app.on_event("startup")
async def _auto_start_ctx_dashboard():
    """M1235: CTX disabled — no-op."""
    pass


@app.api_route("/ctx/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
async def _ctx_proxy(request: Request, path: str):
    """M1235: CTX disabled."""
    return JSONResponse({"ok": False, "error": "CTX disabled (M1235)"}, status_code=404)


@app.get("/ctx")
async def _ctx_proxy_root(request: Request):
    """M1235: CTX disabled."""
    return JSONResponse({"ok": False, "error": "CTX disabled (M1235)"}, status_code=404)


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
async def _m1434_startup_backfill():
    """M1434/M1438: run JSONL skill backfill at startup (background) so first corpus/skills-agents call is fast."""
    import asyncio as _aio
    global _M1434_backfill_done
    def _backfill_sync():
        """M1614: moved to thread — JSONL file I/O was blocking event loop for ~4s on startup."""
        global _M1434_backfill_done
        if _M1434_backfill_done:
            return
        _M1434_backfill_done = True
        try:
            _bf_conn = sqlite3.connect(str(_NS_EVENTS_DB))
            _existing_keys: set = set()
            for _row in _bf_conn.execute(
                "SELECT ts || '|' || COALESCE(session_id,'') || '|' || detail FROM action_log WHERE action='invoked_skill'"
            ):
                _existing_keys.add(_row[0])
            _claude_projects = Path.home() / ".claude" / "projects"
            if _claude_projects.is_dir():
                for _pdir in _claude_projects.iterdir():
                    if not _pdir.is_dir():
                        continue
                    for _jf in sorted(_pdir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)[:3]:
                        try:
                            _lines = _jf.read_text(encoding="utf-8", errors="ignore").splitlines()[-500:]
                            for _ln in _lines:
                                try:
                                    _e = json.loads(_ln)
                                except Exception:
                                    continue
                                _msg = _e.get("message") or {}
                                _content = _msg.get("content") or []
                                if not isinstance(_content, list):
                                    continue
                                for _blk in _content:
                                    if not isinstance(_blk, dict):
                                        continue
                                    if _blk.get("type") == "tool_use" and _blk.get("name") == "Skill":
                                        _skill_name = (_blk.get("input") or {}).get("skill", "")
                                        if not _skill_name:
                                            continue
                                        _ts = _e.get("timestamp") or ""
                                        _sid = _e.get("sessionId") or ""
                                        _key = f"{_ts}|{_sid}|{_skill_name}"
                                        if _key not in _existing_keys:
                                            _existing_keys.add(_key)
                                            _bf_conn.execute(
                                                "INSERT INTO action_log(ts,proj_id,stone_id,action,detail,session_id) VALUES(?,?,?,?,?,?)",
                                                (_ts, "", "", "invoked_skill", _skill_name, _sid)
                                            )
                        except Exception:
                            pass
            _bf_conn.commit()
            _bf_conn.close()
            print("[hub] M1434 JSONL skill backfill complete", file=sys.stderr)
        except Exception as _ex:
            print(f"[hub] M1434 backfill error: {_ex}", file=sys.stderr)
    async def _run():
        await _aio.sleep(1.0)  # let server fully bind first
        await _aio.to_thread(_backfill_sync)  # M1614: offload blocking file I/O to thread pool
    _aio.create_task(_run())


@app.on_event("startup")
async def _cleanup_orphan_attach_procs():
    """Kill orphan 'tmux attach-session' procs from previous hub instance.
    Hub restart cancels asyncio tasks but cannot cancel OS threads running proc.read(),
    so ptyprocess attach-session procs survive as phantom tmux clients."""
    import subprocess as _sp
    try:
        result = _sp.run(["pgrep", "-f", "tmux attach-session"], capture_output=True, text=True)
        for pid_str in result.stdout.splitlines():
            try:
                import os as _os
                _os.kill(int(pid_str.strip()), 9)  # SIGKILL — SIGTERM doesn't work on pty procs
            except Exception:
                pass
    except Exception:
        pass


@app.on_event("startup")
async def _cleanup_orphan_pty_procs():
    """Kill orphan claude PTY procs left over from execv() restart.
    os.execv() keeps the same PID but resets asyncio state — _sessions dict is empty,
    so old ptyprocess-spawned claude children (PPID=self) are never terminated.
    On startup, find all claude children of this process and kill them."""
    import signal as _sig
    my_pid = os.getpid()
    try:
        result = subprocess.run(
            ["ps", "--ppid", str(my_pid), "-o", "pid=,comm="],
            capture_output=True, text=True, timeout=3
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            pid_str, comm = parts[0], parts[1]
            if comm.lower() in {"claude", "codex"}:
                try:
                    os.kill(int(pid_str), _sig.SIGKILL)
                    # Reap zombie so it doesn't linger in process table
                    try:
                        os.waitpid(int(pid_str), os.WNOHANG)
                    except Exception:
                        pass
                    print(f"[hub] killed orphan PTY proc {pid_str} ({comm})", file=sys.stderr)
                except (ProcessLookupError, OSError):
                    pass
    except Exception as _e:
        print(f"[hub] orphan PTY cleanup error: {_e}", file=sys.stderr)


@app.on_event("startup")
async def _dedup_exec_sessions_on_startup():
    """M1569: on startup, ensure at most 1 exec session per proj_id.
    Kills all but the most recently created exec session per project.
    Prevents orphan accumulation across hub restarts."""
    try:
        result = subprocess.run(
            ["tmux", "list-sessions", "-F", "#{session_name}\t#{session_created}"],
            capture_output=True, text=True, timeout=5
        )
        # group exec sessions by proj_id
        from collections import defaultdict
        _by_proj: dict = defaultdict(list)
        _agents = ("claude", "openrouter", "codex", "dsk")
        for line in result.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            sname, screated = parts[0], parts[1]
            for ag in _agents:
                prefix = f"{ag}-exec-"
                if sname.startswith(prefix):
                    proj_id = sname[len(prefix):].split("-")[0] if "-" in sname[len(prefix):] else sname[len(prefix):]
                    # use full suffix after "{ag}-exec-" as proj_id key (handles hyphens in proj names)
                    proj_id = sname[len(prefix):]
                    _by_proj[proj_id].append((int(screated), sname))
                    break
        # for each proj with >1 session, kill all but newest
        for proj_id, sessions in _by_proj.items():
            if len(sessions) <= 1:
                continue
            sessions.sort(key=lambda x: x[0], reverse=True)  # newest first
            for _, sname in sessions[1:]:
                try:
                    subprocess.run(["tmux", "kill-session", "-t", f"={sname}"],
                                   capture_output=True, timeout=3)
                    print(f"[hub] startup dedup: killed stale exec session {sname}", file=sys.stderr)
                except Exception:
                    pass
    except Exception as _e:
        print(f"[hub] startup dedup error: {_e}", file=sys.stderr)


@app.on_event("startup")
async def _start_session_gc():
    """Background task: reap idle or dead sessions every 60 s."""
    async def _gc():
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for proj_id in list(_sessions.keys()):
                proc = _sessions.get(proj_id)
                # Remove dead pty processes only — tmux session + assigned_session are preserved
                if proc and not proc.isalive():
                    _kill_session(proj_id)
                    continue
                # Kill idle pty processes only — tmux session + assigned_session are preserved
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
                   capture_output=True, timeout=3)
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
            subprocess.run(["tmux", "pipe-pane", "-t", session_name], capture_output=True, timeout=3)  # stop pipe
            fifo_path.unlink(missing_ok=True)
        except Exception:
            pass


# v0.2.4: _ensure_fifo_stream, _cancel_fifo_stream, _start_exec_state_watcher removed.
# Detection is now purely client-poll-triggered (v0.2.1 approach) with _push_session_idle() dedup.


def _session_claimable_queued_count(proj_id: str, session_name: str) -> int:
    """M1676: count queued stones THIS session could actually claim — mirrors
    /claim-task's ownership filter (substar assigned_session; free pool = main only;
    skip stones in-flight on another session within the 120s claim TTL).
    The poller previously used a project-wide queued count, so an idle child was
    woken every dedup cycle while the main session worked its own stone (claim
    keeps status='queued'), producing endless empty 'Tasks ready' turns."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        try:
            rows = conn.execute(
                "SELECT data_json FROM milestones_store WHERE proj_id=? AND status='queued' AND COALESCE(held,0)=0",
                (proj_id,)).fetchall()
        finally:
            conn.close()
        stones = [json.loads(r[0]) for r in rows]
        if not stones:
            return 0
        proj = _db_load_project(proj_id) or {}
        sub_sess = {ns.get("id"): (ns.get("assigned_session") or "").strip()
                    for ns in (proj.get("north_stars") or []) if isinstance(ns, dict)}
        # M1797: mother/child concept removed — all sessions can claim free-pool stones.
        _now_e = time.time()
        n = 0
        for m in stones:
            # M1741: mirror claim-task's non-empty-text requirement (server.py claim_task_for_session
            # candidates filter). Without this, a blank-text stub stone (the initial POST of the
            # two-step create flow, before the follow-up text PATCH lands) counts as claimable here
            # but claim-task always skips it — the poller then wakes the session every dedup cycle
            # forever for a stone it can never actually claim (observed: a3237e6e, 141 empty-queue
            # wakes over 31h, all reporting queued_count:0 from claim-task while this function's
            # count — used by the poller's gate — must have disagreed).
            if not str(m.get("text", "")).strip():
                continue
            cb = (m.get("claimed_by_session") or "").strip()
            if cb and cb != session_name and (_now_e - (m.get("claimed_at") or 0)) < _CLAIM_TTL_SECS:
                continue  # in-flight on another session
            sid = (m.get("substar_id") or "").strip()
            # M1916: per-stone session_override takes priority over substar assignment
            _ov = (m.get("session_override") or "").strip()
            owner = _ov if _ov else (sub_sess.get(sid, "") if sid else "")
            # M1860: free-pool removed — unassigned substar stones are NOT claimable.
            # Only count stones whose substar is explicitly assigned to this session.
            if owner == session_name:
                n += 1
        return n
    except Exception:
        return 0


@app.on_event("startup")
async def _start_queue_continuation_poller():
    """M456: hub-side queue continuation — periodically scans alive exec sessions and
    sends 'go' to verified-idle sessions when queued stones exist. Decoupled from stop-hook
    (which only fires on actual Claude Stop event). M442 removed go-injection from queue dispatch
    to avoid killing live sessions; this background task safely resumes idle sessions instead."""
    import re as _re
    POLL_INTERVAL = 10  # M1351: 30→10s — measured 44ms/tick × 6/min = 0.44% one-core CPU, ~20s faster wake
    IDLE_MIN_SECS = 10  # session must have been idle for this long (avoids racing turn-start)
    # M1656-R: per-session wake dedup lives inside _send_exec_wake (_wake_last_sent, 90s)

    async def _arun(*cmd, timeout=3):
        """M1493: async subprocess helper for BG loop — avoids blocking event loop."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return stdout.decode("utf-8", errors="replace") if stdout else ""
        except Exception:
            return ""

    async def _poll_queue_continuation():
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                # List all alive exec tmux sessions — M1493: async to avoid blocking event loop
                _ls_out = await _arun("tmux", "list-sessions", "-F", "#{session_name}")
                exec_sessions = []
                for line in _ls_out.splitlines():
                    sn = line.strip()
                    # M1656-④: longest-prefix match so branched child sessions
                    # (claude-exec-MOAT-Marketing) resolve to proj_id=MOAT and get woken
                    _ag, _pid = _parse_exec_session_name(sn)
                    if _pid:
                        exec_sessions.append((sn, _pid))

                now = time.time()
                for sname, proj_id in exec_sessions:
                    # M1337: skip if user has the live panel open (tmux client attached) —
                    # they're watching the terminal, no need to auto-inject wake.
                    _clients_out = await _arun("tmux", "list-clients", "-t", sname, timeout=2)
                    if _clients_out.strip():
                        # M1685: self-heal a LEAKED exec-attach relay (browser tab closed
                        # without a clean WS close frame — see _exec_attach_relay registry
                        # comment). If this session's client is our own tracked relay PID
                        # and it's been attached far longer than any real interactive glance,
                        # kill it so M1337 stops permanently blocking dispatch on a phantom.
                        _relay = _exec_attach_relay.get(sname)
                        if _relay and (now - _relay.get("attached_at", now)) > _EXEC_ATTACH_STALE_SECS:
                            try:
                                os.kill(_relay["pid"], 9)
                                _exec_attach_relay.pop(sname, None)
                                _server_log_action(proj_id, "", "exec:attach_relay_selfheal",
                                                   f"session:{sname} pid:{_relay['pid']} age:{int(now - _relay['attached_at'])}s")
                            except Exception:
                                pass
                            # fall through to re-check clients this tick — next poll picks up the change
                        continue  # user is watching (or was, until the kill above) — don't inject

                    # M1683: self-heal a stranded 'compacting' hold. A compaction that
                    # finishes at a prompt with no follow-up turn fires no event to clear
                    # the hold, so it pinned the session busy for the full 600s (server.py
                    # _session_is_busy) and blocked dispatch. If the record is 'compacting'
                    # but the PreCompact marker (~/.claude/.compact-marker, refreshed at each
                    # compaction start) is older than _COMPACT_SELFHEAL_SECS, the compaction
                    # is done — release the hold so the busy check below sees the true state.
                    _rec_ch = _agent_busy_sessions.get(sname)
                    if _rec_ch and _rec_ch.get("reason") == "compacting" and _rec_ch.get("busy"):
                        try:
                            _mk = Path.home() / ".claude" / ".compact-marker"
                            _mk_age = (now - _mk.stat().st_mtime) if _mk.exists() else 1e9
                            if _mk_age > _COMPACT_SELFHEAL_SECS:
                                _rec_ch["busy"] = False
                                _rec_ch["reason"] = "compact_selfheal"
                                _server_log_action(proj_id, "", "exec:compact_selfheal",
                                                   f"session:{sname} marker_age:{int(_mk_age)}s")
                        except Exception:
                            pass

                    # M1678: session-scoped busy pre-filter (final gate lives in _send_exec_wake).
                    # Was _oob_is_busy(proj, session) — its proj-aggregate fallback made a busy
                    # mother block her fresh no-record child forever (why blind spawn-wakes existed).
                    if _session_is_busy(sname):
                        continue  # don't interrupt

                    # M1676: per-session eligibility — only wake a session that has stones
                    # IT can claim (was project-wide count → idle children were woken for
                    # main-owned work they can never pick up, every 90s dedup cycle).
                    queued_count = await asyncio.to_thread(
                        _session_claimable_queued_count, proj_id, sname)

                    if queued_count == 0:
                        continue  # nothing this session could claim

                    # M1802: poller auto-assign removed — substars must be manually assigned.

                    # Send wake message — MCP sessions get tool-call instruction, others get 'go'.
                    # M1656-R: _send_exec_wake's atomic per-session 90s dedup is the ONLY
                    # anti-duplicate gate (per-session, so children never starve behind mother).
                    # M1741: this call site previously had no action_log entry — the poller's
                    # own wakes were invisible next to /execute's logged branched-wake path,
                    # which hid a real bug (a3237e6e: 141 empty-queue wakes over 31h, ~90s
                    # cadence matching _WAKE_DEDUP_SECS) from any post-hoc trace. Log the
                    # queued_count that let this wake through the M1676 gate so the next
                    # occurrence pins down whether the count was a genuine (if transient)
                    # non-zero race or a gate bypass.
                    if _send_exec_wake(sname, proj_id):
                        _server_log_action(proj_id, "", "exec:poller_wake",
                                           f"session:{sname} queued_count:{queued_count}")

                # M1742: removed the M1635 fork-protocol auto-spawn block. It scanned for
                # projects with queued stones but no alive exec session and, after a 60s
                # cooldown, self-POSTed to /execute with an EMPTY body (`data=b"{}"`) — so
                # `from_badge = bool(data.get("from_badge"))` evaluated to False inside
                # execute_project, meaning this poller-triggered call was indistinguishable
                # from an explicit human dispatch-button click and fell through to the
                # unconditional tmux-new-session spawn code. The intended guard here
                # (queue_source != "badge") only catches the literal queue-toggle badge
                # click — stone creation, reopen, and comment-reply requeue all leave
                # queue_source empty, so in practice ANY queued stone with no live session
                # anywhere caused a brand-new session to be spawned automatically within
                # ~60s, with zero human action. Per explicit instruction: queue state may
                # only WAKE an already-alive session (handled above); spawning a new
                # session must happen ONLY from the human clicking the dispatch button
                # (execute_project's own from_badge=True gate, which is correct — the bug
                # was this caller lying to it). No auto-spawn replacement — a project with
                # queued stones and no alive session now simply waits for dispatch.

                # M1095: scan all projects' substars — clear assigned_session if the
                # session is dead (handles external kills that bypass _kill_all_exec_sessions)
                # M1768: `_ls_out` came from `_arun`, whose bare except silently returns "" on
                # ANY tmux failure/timeout (box under load) — an empty string parses to an
                # EMPTY alive_sessions set, which then looked like every assigned session on
                # the box died at once and wiped ALL substar assignments in a single tick
                # (observed: MOAT cleared 6 substars simultaneously, including a fork session
                # that was demonstrably still alive and mid-task — its freed substar then fell
                # into the free pool and got double-claimed by the mother, duplicating a 5min+
                # subagent audit in parallel). A truly empty tmux server (no sessions at all)
                # is exceedingly rare on a live box with an active exec session running this
                # very poller loop, so treat an empty _ls_out as "list-sessions failed this
                # tick" and skip the unassign scan entirely rather than trusting it.
                try:
                  if not _ls_out.strip():
                    _server_log_action("", "", "exec:dead_unassign_skip_empty_ls",
                                       "tmux list-sessions returned empty — skipping unassign scan")
                  else:
                    alive_sessions: set = set(_ls_out.splitlines())
                    # Also add any custom sess-* via tmux list-sessions (M1493: reuse _ls_out, no extra call)
                    alive_sessions.update(s.strip() for s in _ls_out.splitlines() if s.strip())
                    for _pdir in PROJECTS_DIR.iterdir():
                        try:
                            _pid = _pdir.name
                            _p = _db_load_project(_pid)
                            if not _p:
                                continue
                            _changed = False
                            _now_ts = time.time()
                            _cleared_ns_ids = []
                            for _ns in (_p.get("north_stars") or []):
                                if not isinstance(_ns, dict):
                                    continue
                                _assigned = (_ns.get("assigned_session") or "").strip()
                                if not _assigned or _assigned in alive_sessions:
                                    continue
                                # M1656-①: grace window — skip clear for 60s after assign_spawn
                                _grace = _ns.get("spawning_grace_until") or 0
                                if _grace > _now_ts:
                                    continue
                                _cleared_ns_ids.append(f"{_ns.get('id','?')}:{_assigned}")
                                _ns["assigned_session"] = None
                                _changed = True
                            if _changed:
                                _db_save_project(_pid, _p)
                                _parse_cache.pop(str(PROJECTS_DIR / _pid / "north-star.md"), None)
                                # M131-c: this cleanup previously had NO log line — the only silent
                                # assigned_session-clearing path (vs. exec:kill / exec:mother_death_cascade,
                                # both logged). Made a user unable to tell "I killed it" from "poller found
                                # it dead for an unrelated reason" after the fact.
                                _server_log_action(_pid, "", "exec:dead_session_unassign",
                                                   f"cleared:{','.join(_cleared_ns_ids)}")
                        except Exception:
                            pass
                except Exception:
                    pass  # cleanup must never crash the poller

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

    # Check continuity mode — M1368: SQLite-first
    continuity_mode = "isolated"  # default
    try:
        _ns = _db_load_project(proj_id) or {}
        continuity_mode = _ns.get("continuity_mode", "isolated")
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

    # M1896: no prior session ID found — return [] so the caller's presigned --session-id
    # path takes over (fresh new conversation with deterministic UUID). --continue is removed:
    # it lets Claude CLI pick its own UUID internally, making live_session_id untrackable.
    return []


_ALLOWED_MODELS = {
    # Claude CLI accepts aliases and full IDs. Restrict to the ones we want users
    # to pick from in the UI; "" / unset → CLI default model.
    "haiku", "sonnet", "opus", "fable",
    "claude-haiku-4-5",
    "claude-sonnet-4-6",
    "claude-sonnet-5",
    "claude-opus-4-7",
    "claude-opus-4-8",
    "claude-fable-5",
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
    "or-hy3",
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
    "or-hy3",
    "or-owl-alpha",
    "or-grok-3",
    "or-grok-3-mini",
    "or-nemotron",
    "or-nemotron-nano",
}
_DSK_MODELS = {"darwin-28b-coder"}
_ALLOWED_AGENTS = {"claude", "codex", "openrouter", "dsk"}
_ALLOWED_PTY_AGENTS = {"claude", "codex", "openrouter"}
# M1609 v2: idle-at-prompt cmds auto-derived from _ALLOWED_AGENTS — no manual sync needed.
# "openrouter" and "dsk" sessions show "python"/"uvicorn" etc as pane cmd, not the agent name,
# so we use the agent name set directly. New agent → add to _ALLOWED_AGENTS only.
_IDLE_HARNESS_CMDS = _ALLOWED_AGENTS.copy()
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
    v = str(cfg.get("defaults", {}).get(key, "")).strip()
    # A-4/A-5: if a path key is set but the file no longer exists, auto-detect
    if key in ("claude_code_path", "codex_path") and v and not Path(v).exists():
        v = ""
    if not v and key == "claude_code_path":
        import shutil as _sh
        v = _sh.which("claude") or str(Path.home() / ".local" / "bin" / "claude")
    return v
# ── end M705 ───────────────────────────────────────────────────────────────────


def _get_project_model_value(proj_id: str) -> str:
    """Read the (validated) model field from project meta. Falls back to config.yaml."""
    try:
        # M1368: SQLite-first via _db_load_project; md no longer required
        proj = _db_load_project(proj_id) or {}
        model = (proj.get("model") or "").strip()
        if model in _ALLOWED_MODELS:
            return model
    except Exception:
        pass
    # M705: fall back to per-user config
    cfg_model = _hub_config_get(proj_id, "model")
    return cfg_model if cfg_model in _ALLOWED_MODELS else ""


def _get_project_model(proj_id: str, override_model: str = None) -> list:
    """Return ['--model', value] if frontmatter has a valid model, else [].
    Spliced into PTY + tmux spawn argv.
    Rewrites or-* aliases to openrouter-* to match the LiteLLM proxy's
    exposed model IDs (seen via GET /v1/models).
    M1699-fork: override_model lets a fork session apply a target model without
    mutating the project's stored model (source session stays on its own model)."""
    model = override_model if override_model is not None else _get_project_model_value(proj_id)
    if model.startswith("or-"):
        model = "openrouter-" + model[3:]
    return ["--model", model] if model else []


def _get_project_agent_value(proj_id: str) -> str:
    """Read the agent field from project meta. Falls back to config.yaml, then claude."""
    try:
        # M1368: SQLite-first
        proj = _db_load_project(proj_id) or {}
        agent = (proj.get("agent") or "").strip().lower()
        if agent in _ALLOWED_AGENTS:
            return agent
    except Exception:
        pass
    # M705: fall back to per-user config
    cfg_agent = _hub_config_get(proj_id, "agent").lower()
    return cfg_agent if cfg_agent in _ALLOWED_AGENTS else "claude"


def _get_project_pty_agent_value(proj_id: str) -> str:
    """Read the PTY agent field from project meta. Defaults to Claude."""
    try:
        # M1368: SQLite-first
        proj = _db_load_project(proj_id) or {}
        agent = (proj.get("pty_agent") or "claude").strip().lower()
        return agent if agent in _ALLOWED_PTY_AGENTS else "claude"
    except Exception:
        return "claude"


def _get_project_agent(proj_id: str) -> str:
    """Return the CLI agent binary selector for this project."""
    return _get_project_agent_value(proj_id)


def _resolve_claude_bin(proj_id: str = None) -> str:
    """Resolve the claude CLI binary path with the same resilience chain as
    _get_agent_spawn_cmd (nvm / .local/bin fallback) — without the embedded --model,
    so callers can assemble their own argv (e.g. fork path that injects model_args separately)."""
    import shutil as _shutil_spawn
    cfg_claude = _hub_config_get(proj_id or "", "claude_code_path") if proj_id else ""
    if not cfg_claude:
        cfg_claude = _shutil_spawn.which("claude") or ""
        if not cfg_claude:
            for _cbin_base in [Path.home() / ".local" / "bin", Path.home() / ".nvm" / "versions", Path("/usr/local/lib")]:
                for _bin in _cbin_base.rglob("claude"):
                    if _bin.is_file():
                        cfg_claude = str(_bin); break
                if cfg_claude: break
    return cfg_claude or "claude"


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
    # M1683: mirror codex's resilience chain above — was a bare "claude" literal relying
    # entirely on the spawning process's PATH (worked here only because hub.service's
    # Environment= happens to include ~/.local/bin; a silent, non-obvious dependency that
    # breaks on any other machine/unit-file/container lacking that same override).
    cfg_claude = _hub_config_get(proj_id, "claude_code_path")
    if not cfg_claude:
        cfg_claude = _shutil_spawn.which("claude") or ""
        if not cfg_claude:
            for _cbin_base in [Path.home() / ".local" / "bin", Path.home() / ".nvm" / "versions", Path("/usr/local/lib")]:
                for _bin in _cbin_base.rglob("claude"):
                    if _bin.is_file():
                        cfg_claude = str(_bin); break
                if cfg_claude: break
    claude_bin = cfg_claude or "claude"
    return [claude_bin, "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS, *_get_project_model(proj_id)]


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
    return [claude_bin, "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS, *_get_project_model(proj_id)]


def _get_project_spawn_env(proj_id: str, override_model: str = None) -> dict:
    """Return extra env vars to splice into the Claude spawn for this project.
    For OSK/GPT: routes to LiteLLM proxy. For OpenRouter: routes via LiteLLM proxy.
    Otherwise {}.
    M1699-fork: override_model lets a fork session route the proxy env to a target
    model independent of the project's stored model."""
    model = override_model if override_model is not None else _get_project_model_value(proj_id)
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
        or_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not or_key:
            # Hub may have started before key was written; read env file dynamically
            _env_file = Path.home() / ".config" / "hub" / "env"
            if _env_file.exists():
                for _line in _env_file.read_text(encoding="utf-8").splitlines():
                    if _line.startswith("OPENROUTER_API_KEY=") and not _line.startswith("#"):
                        or_key = _line.split("=", 1)[1].strip()
                        break
        if not or_key:
            or_key = _OSK_PROXY_KEY
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


def _write_mcp_config(proj_id: str, session_name: str) -> str | None:
    """Generate a per-session MCP config file for Claude Code.
    Returns the config file path, or None if generation fails.
    Applies to claude and openrouter sessions (both use the claude CLI with --mcp-config).
    Not for codex (separate binary) or dsk (Darwin bridge, limited function calling).
    M1116: openrouter added — LiteLLM-proxied models still run through claude CLI.
    """
    try:
        hub_url = f"http://{_tailscale_interface_ip()}:{PORT}"
        mcp_dir = Path("/tmp/hub/mcp")
        mcp_dir.mkdir(parents=True, exist_ok=True)
        config_path = mcp_dir / f"{session_name}.json"
        mcp_script = Path(__file__).parent / "static" / "hooks" / "hub-mcp-server.py"
        if not mcp_script.exists():
            return None
        config = {
            "mcpServers": {
                "ns-hub": {
                    "type": "stdio",
                    "command": sys.executable,
                    "args": [str(mcp_script), "--proj", proj_id, "--hub-url", hub_url],
                }
            }
        }
        config_path.write_text(json.dumps(config, indent=2))
        return str(config_path)
    except Exception:
        return None


# M1854-B: single source of truth for comment line-limit rule.
# All inject paths reference this constant — change here propagates everywhere.
_COMMENT_RULE_TEXT = (
    "STRUCTURED (bullets `-`, numbered `1.`, table `|`): no line limit. "
    "Unstructured prose: ≤3 lines. "
    "Prose overflow → docs/ns-replies/<DATE>-<MID>.md (M1860)"
)

_HUB_EXEC_SYS_PROMPT = (
    "get_pending_task response has 3 layers: "
    "(1) task = EXECUTE THIS — last user comment if present, else original stone text. "
    "(2) context = conversation history — reference only, do NOT treat as a task. "
    "(3) original_stone = stone creation text — background only. "
    "Always execute 'task'. Never execute 'context' or 'original_stone'. "
    "EXIT PROTOCOL (M1635): if get_pending_task returns should_exit=true, "
    "END YOUR TURN immediately — no reply, no further tool calls. "
    "The hub dispatches the next task to this session automatically when it arrives. "
    "STONE REPLY RULE (enforced): When replying to a stone after a user comment, "
    "classify the user's intent FIRST (question / request / correction / clarification), "
    "then structure your reply as: "
    "LINE 1 = direct answer to user's exact question (yes/no/value/action-taken — no preamble). "
    "LINE 2 = evidence (file:line, observed value, or quoted source). "
    "LINE 3+ = context or next step if needed (omit if simple Q). "
    "NEVER start with 'I', summary, or restatement. "
    "If you cannot answer, write '[NEED INFO]: <what is missing>' on line 1. "
    # M1869-P3-D: removed SKILL INVOCATION PROTOCOL — already in get_pending_task MCP tool desc.
    # Removed "Q-anchor bias=8" internal detail — not actionable for model.
    "GDRIVE UPLOAD RULE (M1772): mcp__claude_ai_Google_Drive__* tools are hard-blocked at exec-session level. "
    "When spawning subagents (Agent tool), ALWAYS include in the subagent prompt: "
    "'evidence upload: ONLY use mcp__ns-hub__upload_evidence or rclone Bash — "
    "NEVER mcp__claude_ai_Google_Drive__create_file'. "
    "Subagents do NOT inherit the exec-session tool block. "
    f"COMMENT RULE (M478/M1860/M1854-C): append_message {{role:'claude'}}: {_COMMENT_RULE_TEXT}"
)


# M1712-b: Group-A (mechanical) block on AskUserQuestion for all hub-spawned exec sessions.
# The Group-B prompt hint (_option_choice_hint in get_pending_task's response) only reduces
# the odds of AskUserQuestion firing — proven insufficient live this session (fired twice
# despite the hint being loaded and delivered on every task fetch). --disallowedTools is a
# genuine CLI-level deny-list, distinct from --dangerously-skip-permissions (which only
# bypasses the interactive approval prompt for tools that ARE available) — a denied tool is
# never offered to the model at all. Spliced into every spawn command below.
_DISALLOWED_TOOLS_ARGS = [
    "--disallowedTools",
    # M1712-b: AskUserQuestion blocks exec session turn with no hub-side continuation
    # M1772: mcp__claude_ai_Google_Drive__* blocked at CLI level — base64 encoding is
    # extremely slow; use mcp__ns-hub__upload_evidence or rclone Bash instead.
    "AskUserQuestion,mcp__claude_ai_Google_Drive__copy_file,"
    "mcp__claude_ai_Google_Drive__create_file,mcp__claude_ai_Google_Drive__download_file_content,"
    "mcp__claude_ai_Google_Drive__get_file_metadata,mcp__claude_ai_Google_Drive__get_file_permissions,"
    "mcp__claude_ai_Google_Drive__list_recent_files,mcp__claude_ai_Google_Drive__read_file_content,"
    "mcp__claude_ai_Google_Drive__search_files",
]


def _hub_mcp_spawn_args(proj_id: str, session_name: str, agent: str = "claude") -> list:
    """M1656-R6: shared MCP+system-prompt spawn args for main AND child sessions.
    Children previously spawned without --mcp-config/--append-system-prompt, so
    _exec_wake_msg fell back to raw 'go' injection and the child lacked hub rules."""
    if agent not in (None, "", "claude", "openrouter"):
        return []
    _cfg = _write_mcp_config(proj_id, session_name)
    if not _cfg:
        return []
    return ["--mcp-config", _cfg, "--append-system-prompt", _HUB_EXEC_SYS_PROMPT]


def _exec_wake_msg(session_name: str) -> str:
    """Return the tmux wake message for an exec session.
    MCP-enabled sessions get a direct tool-call instruction; others get 'go'."""
    if Path(f"/tmp/hub/mcp/{session_name}.json").exists():
        return "Tasks ready. Call mcp__ns-hub__get_pending_task() now."
    return "go"


def _get_first_queued_skill(proj_id: str) -> str | None:
    """M1114: Return skill_refs[0] for the first queued stone, or None.
    Used to pre-inject /skill-name into exec session terminal before wake message."""
    try:
        ms = _db_get_active_milestones(proj_id)
        if not ms:
            return None
        # M1655: exclude stones already in-flight (exec_start set) — prevents skill re-inject
        # when dispatch fires again while Claude is already executing the stone.
        queued = sorted(
            [m for m in ms if m.get("status") == "queued" and not m.get("done") and not m.get("exec_start")],
            key=lambda m: m.get("queued_at") or m.get("user_added_at") or ""
        )
        if not queued:
            return None
        first = queued[0]
        refs = first.get("skill_refs") or ([first["skill_ref"]] if first.get("skill_ref") else [])
        return refs[0] if refs else None
    except Exception:
        return None


_wake_last_sent: dict[str, float] = {}  # M1322: dedup guard — session → last send timestamp
_session_spawn_ts: dict[str, float] = {}  # M1678-c: session → spawn time (in-memory boot-grace source;
                                          # replaces tmux display-message query — tmux 3.2a segfaulted
                                          # under per-wake-attempt queries against booting servers)
_WAKE_DEDUP_SECS = 90  # M1322 v2: 5→90s — compaction takes 30-60s; 5s window let second inject slip through
_wake_send_lock = __import__('threading').Lock()  # M1585 v2: atomic dedup check+set across _queued_dispatch / _dispatch_blocking race


def _send_exec_wake(session_name: str, proj_id: str | None = None, force_dedup_reset: bool = False,
                    _skip_boot_grace: bool = False, _force_viewer_bypass: bool = False) -> bool:
    """M1114: Send exec wake message to tmux, with optional skill pre-inject.
    If the first queued stone has skill_refs, sends /skill-name first (CLI intercept)
    then sleeps 1s, then sends the main wake message.
    This achieves ~100% skill load rate vs ~50-60% for _skill_instruction alone.
    M1322: dedup guard prevents duplicate injection within _WAKE_DEDUP_SECS seconds.
    M1533 v5b: also push session_running SSE so NS-card avatar updates instantly without
    waiting for next 5s exec-sessions poll (the gap user observed).
    M1585 v2: threading.Lock() makes dedup check+set atomic — prevents double-inject race
    between _queued_dispatch and _dispatch_blocking threads.
    M1585 v3: force_dedup_reset moves .pop() inside the lock so reset+check+set is atomic.
    M1678: THE single wake choke point — alive/attached/busy gates live HERE so every
    caller (poller, task_complete dispatch, create_wake, spawn paths) shares identical
    semantics. Busy gate is session-scoped (_session_is_busy): a busy mother must not
    block her child's wake, and a fresh no-record session counts as idle.
    Returns True if wake was sent, False if suppressed by any gate."""
    import time as _time_wake
    # M1678-c: CHEAP in-memory gates first — tmux 3.2a segfaulted under repeated
    # per-attempt queries (display-message/has-session every poller cycle against a
    # booting server, dmesg signal-11 loop 21:07-21:12). Order: dedup-peek → busy →
    # boot-grace (memory) → tmux alive/attached (rare, only when a send is imminent).
    now = _time_wake.time()
    if now - _wake_last_sent.get(session_name, 0.0) < _WAKE_DEDUP_SECS and not force_dedup_reset:
        return False  # read-only peek — atomic check+set happens below under the lock
    # M131 fix: a stale 'wake_sent' optimistic-busy record (from a PREVIOUS wake that found
    # nothing queued and let the session go idle) can outlive its own turn and then block
    # the NEXT, genuinely-needed wake for the rest of its 45s TTL (observed: 56s dispatch
    # delay on a freshly-spawned child — courtesy boot-wake found no stone, went idle, then
    # a real stone arrived and was silently skipped here). force_dedup_reset callers have
    # already confirmed real claimable work exists for this exact session, so they may also
    # clear a wake_sent hold — same trust level that already bypasses the dedup timer above.
    # Never clears a REAL busy signal (heartbeat/claim/tool_start/compacting).
    if force_dedup_reset:
        _rec_fdr = _agent_busy_sessions.get(session_name)
        if _rec_fdr and _rec_fdr.get("reason") == "wake_sent":
            _agent_busy_sessions.pop(session_name, None)
    if _session_is_busy(session_name):
        return False  # never inject into a working session
    # M1678-b: boot grace — a wake landing while the harness TUI is still initializing
    # parks as unsubmitted text in the input box. _spawn_wake_when_ready owns the boot
    # window (_skip_boot_grace=True after its readiness probe); everyone else waits 20s.
    if not _skip_boot_grace and (now - _session_spawn_ts.get(session_name, 0.0)) < 20:
        return False  # booting — readiness thread / next poller pass will wake
    if not _tmux_session_alive(session_name):
        return False
    # M1882: HTTP poll viewer (_tmux_output_viewers / M1763) gate removed.
    # live session overlay open = HTTP poll only, no tmux client attached —
    # wake must still fire (user intent: assign/queue while watching the pane).
    # Only real tmux attach (list-clients) blocks wake — that's the correct boundary.
    try:
        _cl = subprocess.run(["tmux", "list-clients", "-t", f"={session_name}"],
                             capture_output=True, text=True, timeout=2)
        if (_cl.stdout or "").strip():
            return False  # M1337: user has the pane open — never auto-inject
    except Exception:
        pass
    # M1585 v3: atomic reset+check+set — pop() is inside the lock so no thread can
    # reset the guard while another thread is between pop() and send_exec_wake().
    with _wake_send_lock:
        if force_dedup_reset:
            _wake_last_sent.pop(session_name, None)  # reset inside lock — atomic with check+set
        last = _wake_last_sent.get(session_name, 0.0)
        if now - last < _WAKE_DEDUP_SECS:
            return False  # duplicate wake suppressed
        _wake_last_sent[session_name] = now
    # lock released — proceed with actual tmux injection (subprocess is slow; holding lock here would starve callers)
    wake_msg = _exec_wake_msg(session_name)

    # Attempt skill pre-inject if proj_id is available
    skill = _get_first_queued_skill(proj_id) if proj_id else None
    if skill:
        subprocess.run(
            ["tmux", "send-keys", "-t", session_name, f"/{skill}", "Enter"],
            capture_output=True, timeout=2,
        )
        _time_wake.sleep(1)  # wait for SKILL.md to load into system context

    subprocess.run(
        ["tmux", "send-keys", "-t", session_name, wake_msg, "Enter"],
        capture_output=True, timeout=2,
    )
    # M1675: optimistic busy from the moment of injection — wake→busy was previously
    # invisible until the session's get_pending_task claim (6-12s UI idle gap).
    # Never clobber a fresh real busy record (heartbeat/claim); wake_sent has its own
    # short TTL (_WAKE_SENT_TTL_SECS) inside _session_is_busy.
    if proj_id:
        _rec_w = _agent_busy_sessions.get(session_name)
        _fresh_busy = bool(_rec_w and _rec_w.get("busy")
                           and (_time_wake.time() - _rec_w.get("ts", 0)) < _OOB_STALE_SECS
                           and _rec_w.get("reason") != "wake_sent")
        if not _fresh_busy:
            _agent_busy_sessions[session_name] = {
                "proj_id": proj_id, "busy": True, "reason": "wake_sent",
                "ts": _time_wake.time(), "stone_id": None,
            }
    # M1533 v5b: emit session_running SSE so swimlane avatar opacity updates without 5s poll lag.
    if proj_id:
        _exec_sessions_cache["ts"] = 0.0  # M1677: pane refresh must see post-wake state
        try: _ns_push("session_running", proj_id=proj_id, kind="exec")
        except Exception: pass
    return True


def _spawn_wake_when_ready(session_name: str, proj_id: str, timeout: float = 30.0) -> None:
    """M1678: readiness-gated spawn wake — replaces blind `tmux send-keys` fired
    immediately after `tmux new-session` (child spawns), which parked the wake text
    in the input box while the harness was still booting (mother never showed this
    because /execute verifies pane-alive before waking). Polls until the pane runs a
    non-shell command, settles 1.5s for the TUI, then goes through _send_exec_wake
    (single choke point: alive/attached/busy gates + dedup + optimistic busy + SSE).
    Run in a background thread — never on the event loop."""
    import time as _t_sw
    _session_spawn_ts[session_name] = _t_sw.time()  # M1678-c: in-memory boot-grace source
    deadline = _t_sw.time() + timeout
    while _t_sw.time() < deadline:
        try:
            r = subprocess.run(["tmux", "list-panes", "-t", f"={session_name}",
                                "-F", "#{pane_current_command}"],
                               capture_output=True, text=True, timeout=2)
            cmds = [c.strip() for c in (r.stdout or "").splitlines() if c.strip()]
            if r.returncode != 0:
                return  # session died during boot — fork-protocol will handle respawn
            if cmds and any(c not in _SHELLS for c in cmds):
                _t_sw.sleep(1.5)  # TUI settle — keys sent mid-init can be dropped
                break
        except Exception:
            pass
        _t_sw.sleep(1.0)  # M1678-c: gentle poll — tmux 3.2a fragile under rapid queries
    # M1688: re-check claimable work right before waking — this call site (assign-spawn,
    # manual "Assign" action in the UI) can fire with zero queued stones for the substar
    # (assigning a session ahead of any real work), and CLI boot alone takes 10-30s+ during
    # which the ORIGINAL triggering stone (if any) may already have been claimed elsewhere
    # or deleted. M1676 added this same guard to the queue-continuation poller but this
    # function (written afterward for M1678) never got it — the gap this stone reported:
    # "child shows busy right at spawn even with no queue."
    if _session_claimable_queued_count(proj_id, session_name) == 0:
        return
    _send_exec_wake(session_name, proj_id, _skip_boot_grace=True)


_COMPACT_THRESHOLD_MB = 4  # M1185: ~80% of 200K window (calibrated: 5MB≈88%, 4MB≈80%)


def _transcript_too_large(proj_id: str) -> bool:
    """M1175: Return True when the current session transcript exceeds COMPACT_THRESHOLD_MB.
    Used to decide whether to inject /compact before the next task wake message."""
    try:
        pdir = PROJECTS_DIR / proj_id
        # M1175 fix: prefer explicit config, then case-insensitive dir search, then hub pdir fallback.
        # _get_project_dir() is case-sensitive; MOAT lives at ~/Project/Moat not ~/Project/MOAT.
        _cfg_dir = _hub_config_get(proj_id, "project_dir")
        if not _cfg_dir:
            _cfg_dir = _get_project_dir(proj_id) or ""
        if not _cfg_dir:
            # Case-insensitive fallback — scan common project bases (no hardcoded usernames)
            _proj_base = Path.home() / "Project"
            _scan_bases = [_proj_base] + ([d for d in _proj_base.iterdir() if d.is_dir()] if _proj_base.exists() else [])
            for _base in _scan_bases:
                if _base.exists():
                    _match = next((str(c) for c in _base.iterdir() if c.name.lower() == proj_id.lower() and c.is_dir()), None)
                    if _match:
                        _cfg_dir = _match
                        break
        proj_dir = _cfg_dir or str(pdir)
        encoded = _encode_cwd_for_claude(str(proj_dir))
        transcripts_dir = Path.home() / ".claude" / "projects" / encoded
        # Look up the active session ID from history
        hist_file = pdir / ".session-history.json"
        sid = ""
        if hist_file.exists():
            try:
                hist = json.loads(hist_file.read_text())
                model = _get_project_model_value(proj_id) or "_default"
                sid = (hist.get(model) or hist.get("_current") or "").strip()
            except Exception:
                pass
        if not sid:
            last_id_file = pdir / ".last-session-id"
            if last_id_file.exists():
                sid = last_id_file.read_text().strip()
        if not sid:
            return False
        t = transcripts_dir / f"{sid}.jsonl"
        if t.exists():
            return t.stat().st_size > _COMPACT_THRESHOLD_MB * 1024 * 1024
    except Exception:
        pass
    return False


def _record_spawn_info(proj_id: str, resume_args: list, agent: str = "claude", session_name: str = "", model: str = None, is_mother: bool = False) -> None:
    """Snapshot what resume flag we used when spawning Claude for this project/session.

    Surfaces to the UI via /api/exec-sessions so users can see whether the
    currently running tmux session is actually continuing prior stone work
    (--resume <id>) or starting fresh (--continue / nothing).

    M1656-R9: keyed by session_name (dict inside one file) — was a single
    per-project record, so only whichever session spawned LAST within a 120s
    guess-window showed real data; every sibling (mother vs children) showed
    "— unknown". session_name="" falls back to the legacy "_main" key so old
    call sites (main execute path) keep working unchanged.
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
        _mode = "fork" if "--fork-session" in resume_args else "resume"
        # M1806-fix: resume reuses the SAME session id as from_id (no new .jsonl created),
        # so live_session_id == from_id deterministically.
        # M1896-fix: fork+presigned — when --session-id is also in resume_args (M1775 path),
        # the presigned sid IS the live session id at write time; set it here so spawn-info is
        # complete from first write, eliminating the race window between _record_spawn_info
        # (live_session_id="") and the subsequent _write_sid_direct call.
        # M1774-correction (original): fork without --session-id → leave "" so
        # _capture_live_session_id_bg remains the single source of truth (still applies when
        # _br_presigned_sid path is absent, e.g. --continue fallback).
        if _mode == "resume":
            _live = from_id
        elif "--session-id" in resume_args:
            try:
                _sid_idx = resume_args.index("--session-id")
                _live = resume_args[_sid_idx + 1] if _sid_idx + 1 < len(resume_args) else ""
            except Exception:
                _live = ""
        else:
            _live = ""  # fork without presigned -- _capture_live_session_id_bg will fill
        info = {"agent": agent, "mode": _mode, "from_id": from_id, "live_session_id": _live, "at": _dt.now().isoformat(timespec="seconds")}
    elif "--continue" in resume_args:
        info = {"agent": agent, "mode": "continue", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    elif "--session-id" in resume_args:
        # M1803: a fresh spawn's presigned --session-id IS the live session id — known
        # deterministically at spawn time (M1775), unlike --fork-session's ambiguous case
        # above. Was falling into the plain "fresh" branch below with live_session_id never
        # set at all, even though the exact value was right there in resume_args (confirmed
        # live: actual tmux pane argv `--session-id 4d4a5545-...` matched .session-history.json
        # exactly, but .spawn-info-by-session.json recorded neither).
        try:
            idx = resume_args.index("--session-id")
            _sid = resume_args[idx + 1] if idx + 1 < len(resume_args) else ""
        except Exception:
            _sid = ""
        info = {"agent": agent, "mode": "fresh", "from_id": "", "live_session_id": _sid,
                "at": _dt.now().isoformat(timespec="seconds")}
    else:
        info = {"agent": agent, "mode": "fresh", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    # M189: add model to spawn info so UI can display it in the session pane
    # M1131: when no explicit model is set for the project, read the effective
    # default from ~/.claude/settings.json so _execModel is never empty.
    # M1699-fork: explicit model param (fork passes the target model, not the
    # project's stored model) takes priority when provided.
    if model is not None:
        _proj_model = model
    else:
        _proj_model = _get_project_model_value(proj_id) or ""
        if not _proj_model:
            try:
                import json as _json
                _cc_settings = Path.home() / ".claude" / "settings.json"
                if _cc_settings.exists():
                    _proj_model = _json.loads(_cc_settings.read_text()).get("model", "") or ""
            except Exception:
                pass
    info["model"] = _proj_model
    # M1796: explicit mother flag — now that every session has a suffix, suffix presence
    # alone can no longer distinguish mother from children.
    if is_mother:
        info["is_mother"] = True
    _key = session_name or "_main"
    try:
        pdir = PROJECTS_DIR / proj_id
        pdir.mkdir(parents=True, exist_ok=True)
        _f = pdir / ".spawn-info-by-session.json"
        _all = {}
        if _f.exists():
            try:
                _all = json.loads(_f.read_text())
            except Exception:
                _all = {}
        _all[_key] = info
        _f.write_text(json.dumps(_all, ensure_ascii=False))
        # M1656-R9: legacy single-file mirror for the main session only — keeps any
        # unrefactored reader working during rollout.
        if _key == "_main":
            (pdir / ".last-spawn-info.json").write_text(json.dumps(info))
    except Exception:
        pass


def _write_sid_direct(proj_id: str, session_name: str, sid: str) -> None:
    """M1775: write a known-in-advance live_session_id (e.g. from --session-id pre-assignment)
    directly into spawn-info, bypassing the poll-and-diff capture entirely. Same target file
    as _capture_live_session_id_bg's internal _write_sid, exposed standalone so spawn call
    sites that pre-generate the UUID don't need a background thread at all.
    M1884: also push into .session-history.json ring so assign_spawn sessions (children
    spawned via the substar assign path, not /execute) appear in the resume session list."""
    _f = PROJECTS_DIR / proj_id / ".spawn-info-by-session.json"
    _all: dict = {}
    if _f.exists():
        try:
            _all = json.loads(_f.read_text())
        except Exception:
            pass
    if session_name in _all:
        _all[session_name]["live_session_id"] = sid
        _f.write_text(json.dumps(_all, ensure_ascii=False))
    # M1884: push into session-history ring so this session appears in the resume list.
    # assign_spawn never went through /execute, so _update_session_history_from_transcript
    # was never called — the session was invisible in the resume popup after being killed.
    try:
        _model_key = (_all.get(session_name) or {}).get("model", "") or ""
        if _model_key and sid:
            _update_session_history_locked(proj_id,
                lambda h: _push_session_history_ring(h, _model_key, sid))
    except Exception:
        pass


def _write_spawn_marker(tdir: Path, session_name: str) -> Optional[Path]:
    """M1773-C: write a marker file immediately before tmux spawn so
    _capture_live_session_id_bg can resolve ambiguity by matching the new .jsonl
    whose mtime is closest to (and within 2s of) the marker's mtime."""
    try:
        tdir.mkdir(parents=True, exist_ok=True)
        marker = tdir / f".spawning-{session_name}"
        marker.touch()
        return marker
    except Exception:
        return None


def _capture_live_session_id_bg(proj_id: str, proj_dir: str, session_name: str, pre_existing: set,
                                 poll_interval: float = 1.0, timeout: float = 20.0,
                                 spawn_marker: Optional[Path] = None) -> None:
    """M1656-R9: session-scoped live_session_id capture — runs in a background thread so
    the spawn call site doesn't block. POLLS the transcript directory (Claude CLI can take
    well over 2s to boot and write its first transcript line, especially forking a large
    context) diffing against a pre-spawn snapshot to find the ONE new file this spawn
    created — unambiguous even when sibling sessions (mother/other children) share the
    same transcript directory and write concurrently (the old heuristic just grabbed
    'newest mtime file', which frequently picked a SIBLING's transcript instead of this
    session's own — confirmed root cause of the mother/child pane info mismatch).

    M1773-C: When multiple new files appear simultaneously (ambiguous), fall back to the
    marker-file strategy: pick the .jsonl whose mtime is closest to spawn_marker's mtime
    and within a 2-second window. This eliminates the race without relying on OS timestamp
    precision — the marker is written synchronously on the hub side, giving a reliable
    reference point."""
    import time as _t, threading as _th

    def _write_sid(sid: str) -> None:
        _write_sid_direct(proj_id, session_name, sid)

    def _run():
        encoded = _encode_cwd_for_claude(str(proj_dir))
        tdir = Path.home() / ".claude" / "projects" / encoded
        _marker_mtime = spawn_marker.stat().st_mtime if (spawn_marker and spawn_marker.exists()) else None
        _deadline = _t.time() + timeout
        try:
            while _t.time() < _deadline:
                _t.sleep(poll_interval)
                try:
                    if not tdir.exists():
                        continue
                    _now_files = {f.name for f in tdir.glob("*.jsonl")}
                    _new_files = _now_files - pre_existing
                    if len(_new_files) == 1:
                        _write_sid(next(iter(_new_files))[:-6])
                        return
                    if len(_new_files) > 1:
                        if _marker_mtime is not None:
                            # M1773-C: pick the candidate closest to marker mtime within ±2s
                            _candidates = [
                                (f, abs((tdir / f).stat().st_mtime - _marker_mtime))
                                for f in _new_files
                                if abs((tdir / f).stat().st_mtime - _marker_mtime) <= 2.0
                            ]
                            if _candidates:
                                _best = min(_candidates, key=lambda x: x[1])[0]
                                _write_sid(_best[:-6])
                        return  # ambiguous without marker — leave unset
                except Exception:
                    return
        finally:
            # Always clean up marker regardless of outcome
            if spawn_marker:
                try:
                    spawn_marker.unlink(missing_ok=True)
                except Exception:
                    pass

    _th.Thread(target=_run, daemon=True).start()


def _read_spawn_info(proj_id: str, session_name: str) -> dict:
    """M1656-R9: session-scoped read counterpart to _record_spawn_info."""
    _ag, _pid = _parse_exec_session_name(session_name)
    _is_main = bool(_pid) and session_name == f"{_ag}-exec-{_pid}"
    try:
        _f = PROJECTS_DIR / proj_id / ".spawn-info-by-session.json"
        if _f.exists():
            _all = json.loads(_f.read_text())
            if session_name in _all:
                return _all[session_name]
            if _is_main and "_main" in _all:
                return _all["_main"]
    except Exception:
        pass
    # M1656-R9: legacy single-file fallback — covers main sessions spawned before this
    # fix was deployed (their data lives only in the old per-project file).
    if _is_main:
        try:
            _legacy = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
            if _legacy.exists():
                return json.loads(_legacy.read_text())
        except Exception:
            pass
    return {}


def _update_session_history_locked(proj_id: str, mutate_fn) -> None:
    """M1804: read-modify-write .session-history.json under an exclusive file lock.
    Root cause of real data loss (verified live, 2026-07-13: 6 of 9 same-day transcripts
    missing from the file entirely, plus one SID duplicated across 3 ring slots): the two
    write sites (_update_session_history_from_transcript + the fork endpoint) each did a
    bare read → mutate-in-memory → write with NO locking. When spawns land close together
    (repeated fork/idle-spawn testing is exactly this pattern, but any two near-simultaneous
    dispatches would trigger it), a later write's read snapshot predates an earlier write's
    result — the earlier write is silently discarded wholesale, not just its one key.
    mutate_fn(hist: dict) -> None mutates the loaded dict in place; this function persists it.
    fcntl.flock blocks (not spins) until the lock is free, so callers see zero added latency
    under normal (non-contended) load and correctly serialize under real contention."""
    import fcntl
    hist_file = PROJECTS_DIR / proj_id / ".session-history.json"
    hist_file.parent.mkdir(parents=True, exist_ok=True)
    hist_file.touch(exist_ok=True)
    with open(hist_file, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            try:
                hist = json.loads(raw) if raw.strip() else {}
            except Exception:
                hist = {}
            mutate_fn(hist)
            f.seek(0)
            f.truncate()
            f.write(json.dumps(hist))
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


_SESSION_HISTORY_RING_SIZE = 5  # M1802: model_key + 4 _prev.._prev4 slots


def _session_history_slot_keys(model_key: str) -> list[str]:
    """M1802: the 5 key names in this model's history ring, newest-first."""
    return [model_key] + [f"{model_key}_prev{i if i > 1 else ''}"
                           for i in range(1, _SESSION_HISTORY_RING_SIZE)]


def _push_session_history_ring(hist: dict, model_key: str, new_sid: str) -> None:
    """M1802: shift model_key's 5-slot ring (model_key, _prev, _prev2, _prev3, _prev4) down
    one and insert new_sid at the front. Was a 2-slot chain (model_key + _prev only) — a 3rd
    spawn under the same model within a short window silently discarded the 1st session with
    no way to resume it, even though its transcript still exists on disk (observed live:
    repeated fork/idle-spawn testing on MOAT). Mutates hist in place; caller persists it.
    Shared by both write sites (fresh/resume spawn + fork spawn) so the ring-shift logic
    can't drift between them."""
    _slots = _session_history_slot_keys(model_key)
    old_chain = [hist.get(k) for k in _slots]
    if old_chain[0] and old_chain[0] != new_sid:
        for _i in range(len(_slots) - 1, 0, -1):
            if old_chain[_i - 1]:
                hist[_slots[_i]] = old_chain[_i - 1]
    hist[model_key] = new_sid


def _update_session_history_from_transcript(proj_id: str, proj_dir: str, model_key: str = "", preserve_current: bool = False, known_sid: str = "") -> str | None:
    """Scan the transcript directory for the newest .jsonl file and record it in session-history.
    Returns the session ID if found, else None. Called after a fresh tmux spawn so the new
    session appears in the resume list immediately (before Stop hook fires).
    M1699-fork: preserve_current=True (fork sessions) leaves the source session's _current/
    _default anchors untouched so the original model's session can still be resumed.
    M1775: known_sid, when provided (e.g. pre-assigned via --session-id, or an existing
    --resume target), skips the newest-mtime scan entirely — that scan is the same class of
    mis-attribution bug _capture_live_session_id_bg was built to fix for forks (a concurrently
    writing sibling session can win the mtime sort). The rest of the history-anchor/
    cross-agent-contamination-guard logic below is unchanged and still applies to known_sid."""
    try:
        encoded = _encode_cwd_for_claude(str(proj_dir))
        transcripts_dir = Path.home() / ".claude" / "projects" / encoded
        if known_sid:
            new_sid = known_sid
        else:
            if not transcripts_dir.exists():
                return None
            jsonls = sorted(transcripts_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True)
            if not jsonls or jsonls[0].stat().st_size == 0:
                return None
            new_sid = jsonls[0].stem
        _is_agent_specific = bool(model_key) and (
            model_key.startswith("or-") or model_key.startswith("codex-")
        )
        # M1740: cross-agent contamination guard — detection reads a file, not hist, so it
        # can run outside the lock. A fork spawns a new session that may reuse/inherit a
        # SIBLING agent's transcript (e.g. forking a claude session into openrouter). Without
        # this guard the claude session id leaks into the openrouter model key (and its
        # ring), so it shows up in the wrong agent's resume list.
        _record_under_model_key = False
        if model_key:
            _mk_agent = "openrouter" if model_key.startswith("or-") else "codex" if model_key.startswith("codex-") else "claude"
            _new_agent = "claude"
            _det_empty = False  # M1750: track whether detection was inconclusive
            try:
                _new_t = transcripts_dir / f"{new_sid}.jsonl"
                if _new_t.exists() and _new_t.stat().st_size > 0:
                    _det = _detect_session_model(_new_t)
                    if _det.startswith("or-"):
                        _new_agent = "openrouter"
                    elif _det.startswith("codex-"):
                        _new_agent = "codex"
                    elif not _det:
                        # M1750: transcript too new — no API responses written yet (only metadata
                        # lines). Detection is inconclusive; trust model_key (from spawn-info)
                        # instead of defaulting to "claude" and blocking the write.
                        _det_empty = True
            except Exception:
                _det_empty = True
            # Foreign-agent session: do NOT record under this model key (prevents the leak).
            # Still advance the ring for the *true* owner if it later writes here.
            _record_under_model_key = not (_new_agent != _mk_agent and not _det_empty)

        # M1804: entire read-mutate-write happens under one file lock — was a bare
        # read/mutate/write with no locking (see _update_session_history_locked docstring
        # for the real data-loss incident this fixes: 6 of 9 same-day transcripts missing,
        # one SID duplicated across 3 ring slots, all from concurrent spawns racing on this
        # exact file).
        def _mutate(hist: dict) -> None:
            if not _is_agent_specific and not preserve_current:
                hist["_current"] = new_sid
            if model_key and _record_under_model_key:
                _push_session_history_ring(hist, model_key, new_sid)
            if not _is_agent_specific and not hist.get("_default"):
                hist["_default"] = new_sid
        _update_session_history_locked(proj_id, _mutate)
        return new_sid
    except Exception:
        return None


_known_projs_cache: dict = {"ts": 0.0, "ids": set()}


def _parse_exec_session_name(session_name: str) -> tuple:
    """M1656-④ shared: return (agent, proj_id) for an exec tmux session name.
    Longest-prefix match against known project dirs so branched child names
    like 'claude-exec-MOAT-Marketing' resolve to proj_id='MOAT' (not 'MOAT-Marketing').
    Returns ('','') for non-exec sessions."""
    _now = time.monotonic()
    if _now - _known_projs_cache["ts"] > 10.0:
        try:
            _known_projs_cache["ids"] = {p.name for p in PROJECTS_DIR.iterdir() if p.is_dir()}
        except Exception:
            pass
        _known_projs_cache["ts"] = _now
    for prefix, prefix_agent in (("claude-exec-", "claude"), ("codex-exec-", "codex"),
                                 ("openrouter-exec-", "openrouter"), ("dsk-exec-", "dsk")):
        if not session_name.startswith(prefix):
            continue
        remainder = session_name[len(prefix):]
        best = ""
        for pid in _known_projs_cache["ids"]:
            if remainder == pid or remainder.startswith(pid + "-"):
                if len(pid) > len(best):
                    best = pid
        return prefix_agent, (best or remainder)
    return "", ""


def _truncate_transcript_before_stone(transcript_path: Path, stone_id: str) -> "Path | None":
    """M1679: cut point for fork-while-busy. A branched child spawned via --fork-session
    inherits the mother's FULL live conversation verbatim — if the mother is mid-work on
    an in-flight stone (tool calls issued, reply not yet posted), the fork carries that
    unfinished work along, and the child (being told 'just continue') completes it
    instead of starting its own assignment (observed: FromScratch M127, VLM-assigned,
    finished by a child spawned for VLA). Root cause is fork operating at the session
    layer with no concept of stone ownership.
    Fix: locate the get_pending_task tool_result line where {"has_task":true,"task_id":
    "<stone_id>"} first appears in the mother's JSONL transcript, and write a COPY
    containing only the lines before it to a new synthetic-uuid file in the same
    directory. The mother's own file is never touched — she keeps working normally.
    Returns the new file's session id (stem) on success, None if the marker wasn't
    found or on any I/O error (caller must fall back to forking the mother's raw id)."""
    import uuid as _uuid_trunc
    try:
        # M1679: the tool_result is a JSON string EMBEDDED inside the JSONL line, so its
        # own quotes are backslash-escaped one level (\"has_task\":true, not "has_task":true).
        needle = '\\"task_id\\":\\"' + stone_id + '\\"'
        marker = '\\"has_task\\":true'
        lines = transcript_path.read_text(encoding="utf-8", errors="replace").splitlines()
        cut_idx = None
        for i, line in enumerate(lines):
            if marker in line and needle in line:
                cut_idx = i
                break
        if cut_idx is None:
            return None  # marker not found — nothing safe to cut, caller falls back
        kept = lines[:cut_idx]
        if not kept:
            return None  # nothing precedes the marker — forking an empty transcript is pointless
        new_sid = str(_uuid_trunc.uuid4())
        new_path = transcript_path.parent / f"{new_sid}.jsonl"
        new_path.write_text("\n".join(kept) + "\n", encoding="utf-8")
        return new_sid
    except Exception:
        return None


def _truncate_transcript_before_last_user_turn(transcript_path: Path) -> "str | None":
    """M1780: cut point for fork-while-busy when the mother is mid-work on a DIRECT
    conversational turn (no stone claim) — _truncate_transcript_before_stone only fires
    when _agent_busy_sessions has a stone_id, which is None for interactive chat (e.g. a
    user asking the mother to investigate something, exactly the shape of session that
    forked this very hub session's own work). Same problem, different trigger: a fork
    taken mid-turn inherits the mother's unfinished direct-conversation work and a naive
    'just continue' child would carry on answering the mother's question instead of
    starting fresh.
    Fix: cut right before the LAST {"type":"user",...} line in the transcript — that is
    the start of whatever turn the mother is currently mid-response to. Everything before
    it (completed prior exchanges) is kept so context isn't lost, but the in-flight
    unanswered turn itself is excluded.
    Returns the new file's session id (stem) on success, None if no user turn was found
    (nothing to cut — e.g. mother hasn't received any message yet) or on I/O error."""
    import uuid as _uuid_trunc2
    try:
        lines = transcript_path.read_text(encoding="utf-8", errors="replace").splitlines()
        cut_idx = None
        for i in range(len(lines) - 1, -1, -1):
            try:
                entry = json.loads(lines[i])
            except Exception:
                continue
            if isinstance(entry, dict) and entry.get("type") == "user":
                cut_idx = i
                break
        if cut_idx is None:
            return None  # no user turn found — nothing safe to cut, caller falls back
        kept = lines[:cut_idx]
        if not kept:
            return None  # nothing precedes it — forking an empty transcript is pointless
        new_sid = str(_uuid_trunc2.uuid4())
        new_path = transcript_path.parent / f"{new_sid}.jsonl"
        new_path.write_text("\n".join(kept) + "\n", encoding="utf-8")
        return new_sid
    except Exception:
        return None


_HUB_WAKE_NEEDLE = "mcp__ns-hub__get_pending_task"

def _truncate_transcript_before_last_hub_wake(transcript_path: Path) -> "str | None":
    """M1980: cut point for fork on an IDLE session whose transcript ends with a
    completed hub-task exchange. The mother finished her last stone, and the transcript
    tail looks like:
        user:  "Tasks ready. Call mcp__ns-hub__get_pending_task() now."
        assistant: <called get_pending_task, did the work, replied>
        user:  <tool_result chains>  ← subsequent turns completing the task
        ...
    When the child inherits this verbatim, Claude 'sees' the hub-wake injection and
    re-executes the last task instead of waiting for its own fresh wake. Fix: scan
    backwards for the LAST user turn that contains the hub wake text, and cut before it.
    If no such turn exists (conversation is pure interactive, not hub-driven), return None
    so the caller falls back to a raw fork (preserving full context is correct there)."""
    import uuid as _uuid_trunc3
    try:
        lines = transcript_path.read_text(encoding="utf-8", errors="replace").splitlines()
        cut_idx = None
        for i in range(len(lines) - 1, -1, -1):
            try:
                entry = json.loads(lines[i])
            except Exception:
                continue
            if not (isinstance(entry, dict) and entry.get("type") == "user"):
                continue
            msg = entry.get("message", {})
            content = msg.get("content", "") if isinstance(msg, dict) else ""
            if isinstance(content, str) and _HUB_WAKE_NEEDLE in content:
                cut_idx = i
                break
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and isinstance(item.get("text"), str) and _HUB_WAKE_NEEDLE in item["text"]:
                        cut_idx = i
                        break
                if cut_idx is not None:
                    break
        if cut_idx is None:
            return None  # no hub-wake turn found — raw fork is safe
        kept = lines[:cut_idx]
        if not kept:
            return None
        new_sid = str(_uuid_trunc3.uuid4())
        new_path = transcript_path.parent / f"{new_sid}.jsonl"
        new_path.write_text("\n".join(kept) + "\n", encoding="utf-8")
        return new_sid
    except Exception:
        return None


def _mother_fork_args(proj_id: str, proj_dir: str) -> list:
    """M1656-fork: return ['--resume', <mother_sid>, '--fork-session'] so a branched child
    session inherits the mother conversation's full context while writing to its OWN new
    session id (no transcript contention with the still-running mother).
    Falls back to ['--continue'] when no valid mother transcript is found (M837-safe).
    M1656-R8 (user decision): ALWAYS fork — never fresh. Oversized mother context is
    handled by Claude's built-in auto-compact on the child's first turn.
    M1679: if the mother is currently busy with an in-flight stone, fork from a
    truncated COPY that ends right before that stone's assignment (see
    _truncate_transcript_before_stone) instead of her live, in-progress conversation —
    the child then starts its own turn clean instead of finishing the mother's work.
    M1780: extended to the no-stone-claim case — a mother busy from a direct interactive
    conversation (stone_id is None, e.g. answering a user's question, exactly the shape
    of session that originally forked THIS fix's own work) previously fell straight
    through to the raw, untruncated fork since the M1679 condition never fired without a
    stone_id. Falls back to cutting before her last unanswered user turn instead (see
    _truncate_transcript_before_last_user_turn)."""
    try:
        hist_file = PROJECTS_DIR / proj_id / ".session-history.json"
        if hist_file.exists():
            hist = json.loads(hist_file.read_text())
            for _k in ("_current", "_default"):
                sid = (hist.get(_k) or "").strip()
                if sid:
                    t = (Path.home() / ".claude" / "projects"
                         / _encode_cwd_for_claude(str(proj_dir)) / f"{sid}.jsonl")
                    if t.exists() and t.stat().st_size > 0:
                        try:
                            # M1796: mother session now has UUID suffix — look up live session name
                            _mother_sess = _live_exec_session_name(proj_id) or f"{_get_project_agent_value(proj_id)}-exec-{proj_id}"
                            _mrec = _agent_busy_sessions.get(_mother_sess) or {}
                            _mother_stone = _mrec.get("stone_id")
                            if _session_is_busy(_mother_sess):
                                if _mother_stone:
                                    _cut_sid = _truncate_transcript_before_stone(t, _mother_stone)
                                else:
                                    _cut_sid = _truncate_transcript_before_last_user_turn(t)
                                if _cut_sid:
                                    _server_log_action(proj_id, _mother_stone or "", "exec:fork_truncated",
                                                       f"mother:{_mother_sess} orig_sid:{sid} cut_sid:{_cut_sid}")
                                    return ["--resume", _cut_sid, "--fork-session"]
                        except Exception:
                            pass  # any failure in the busy/cut check — fall back to raw fork below
                        return ["--resume", sid, "--fork-session"]
    except Exception:
        pass
    # M1896: fork fallback — no mother transcript found. Return [] so the caller's presigned
    # --session-id path creates a fresh session with known UUID. --continue removed (same
    # reason as _get_resume_args L5053: live_session_id would be untrackable).
    return []



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
    for fallback in ("claude", "codex", "openrouter", "dsk"):
        n = f"{fallback}-exec-{proj_id}"
        if n not in names:
            names.append(n)
    # M858: also enumerate live tmux sessions matching *-exec-{proj_id}-* (branched sessions)
    # M1940: use bg poller cache (_bg_tmux_state["ls"]) instead of forking tmux per call (~15ms saved)
    try:
        _cached_ls = _bg_tmux_state.get("ls", "")
        if _cached_ls:
            _tmux_ls = _cached_ls.splitlines()
        else:
            _tmux_ls = subprocess.run(
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=2,
            ).stdout.splitlines()
        for sname in _tmux_ls:
            # Extract session_name from "name:created:windows" format used by bg poller
            sname = sname.split(":")[0] if ":" in sname else sname
            # Match branched pattern: <agent>-exec-<proj_id>-<suffix>
            if f"-exec-{proj_id}-" in sname and sname not in names:
                names.append(sname)
    except Exception:
        pass
    return names


def _live_exec_session_name(proj_id: str) -> str:
    """Return the first running exec session name for a project, if any.
    M1796: sessions now have UUID suffix — no predictable fallback name exists when none are
    running. Returns empty string in that case; callers must handle the no-session state.
    M1940: use bg poller cache for membership check to avoid tmux has-session fork per candidate."""
    # Build live session set from bg cache (avoids per-candidate subprocess fork)
    _cached_ls = _bg_tmux_state.get("ls", "")
    if _cached_ls:
        _live_sessions = {
            line.split(":")[0] if ":" in line else line
            for line in _cached_ls.splitlines() if line.strip()
        }
        for candidate in _exec_session_names(proj_id):
            if candidate in _live_sessions:
                return candidate
        return ""
    # Fallback: bg poller not yet populated (startup race) — use subprocess
    for candidate in _exec_session_names(proj_id):
        check = subprocess.run(["tmux", "has-session", "-t", f"={candidate}"], capture_output=True, timeout=2)
        if check.returncode == 0:
            return candidate
    return ""


def _owning_exec_session_name(proj_id: str, milestone: dict, proj: dict = None) -> str:
    """M131-b-followup: resolve the session that OWNS a stone via its substar's
    assigned_session, falling back to _live_exec_session_name only when the stone has no
    substar or the substar has no assigned session. M1796: no predictable main-session name
    exists — _live_exec_session_name returns the first live session from tmux discovery."""
    _sid_own = (milestone.get("substar_id") or "").strip() if isinstance(milestone, dict) else ""
    if _sid_own:
        if proj is None:
            proj = _db_load_project(proj_id) or {}
        for _ns_own in (proj.get("north_stars") or []):
            if isinstance(_ns_own, dict) and _ns_own.get("id") == _sid_own:
                _owner = (_ns_own.get("assigned_session") or "").strip()
                if _owner:
                    return _owner
                break
    return _live_exec_session_name(proj_id)


def _kill_all_exec_sessions(proj_id: str, spare: set = None) -> list:
    """M206: kill agent-prefixed exec sessions for a project.
    Prevents duplicate panes when agent changes (e.g. claude-exec + openrouter-exec both alive).
    M1087: also kills custom assigned_session values from substars (sess- or any non-exec pattern).
    M1797: mother/child distinction removed — all sessions are peers, so sparing is no longer
    an is_child predicate. M1797-followup: callers that only mean to replace ONE session (e.g.
    respawning the session whose options changed) must pass spare={that other alive sessions'
    names} — otherwise every unrelated, healthy sibling session for the project gets killed too
    (regression found in M1797 self-audit: a plain model/agent change on one dispatch target was
    wiping every other working session in the project).
    M1656-R3: kill uses tmux exact-match (=name) — plain -t does PREFIX matching, so
    killing a dead 'claude-exec-MOAT' silently killed 'claude-exec-MOAT-Marketing'."""
    killed = []
    _spare = spare or set()

    # Collect custom assigned sessions from substars before clearing them
    extra_sessions: list = []
    try:
        p = _db_load_project(proj_id)
        if p:
            for ns in (p.get("north_stars") or []):
                assigned = (ns.get("assigned_session") or "").strip() if isinstance(ns, dict) else ""
                if assigned and assigned not in extra_sessions:
                    extra_sessions.append(assigned)
    except Exception:
        pass
    all_candidates = _exec_session_names(proj_id)
    # M1087: add any custom assigned sessions not already in the list
    for s in extra_sessions:
        if s not in all_candidates:
            all_candidates.append(s)
    targets = [c for c in all_candidates if c not in _spare]
    # M1940: parallel kill — tmux kill-session is I/O-bound; firing all at once cuts wall time
    # from O(n×5s-timeout) sequential to O(1×5s) parallel for n sessions.
    import concurrent.futures as _cf_kill
    def _kill_one(candidate):
        return candidate, subprocess.run(
            ["tmux", "kill-session", "-t", f"={candidate}"], capture_output=True, timeout=5)
    with _cf_kill.ThreadPoolExecutor(max_workers=min(len(targets), 8)) as _ex:
        for candidate, r in _ex.map(_kill_one, targets):
            if r.returncode == 0:
                killed.append(candidate)
            # M355: clear idle tracking for killed sessions
            _exec_idle_count.pop(candidate, None)
            _exec_was_running.pop(candidate, None)
    if killed:
        _server_log_action(proj_id, "", "exec:kill", f"sessions:{','.join(killed)}{f' (spared {len(_spare)})' if _spare else ''}")
    # M985/M1656-R3: clear assigned_session ONLY for substars whose session was actually
    # killed — spared children keep their assignments (prevents auto-assign absorption).
    try:
        p = _db_load_project(proj_id)
        if p:
            north_stars = p.get("north_stars") or []
            changed = False
            for ns in north_stars:
                if not isinstance(ns, dict):
                    continue
                _a = (ns.get("assigned_session") or "").strip()
                if _a and _a in killed:
                    ns["assigned_session"] = None
                    changed = True
            if changed:
                _db_save_project(proj_id, p)
                # M1095: invalidate parse cache so UI reads fresh SQLite, not stale L1
                _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
    except Exception:
        pass
    return killed


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
                            capture_output=True, text=True, timeout=2)
        prefix = f"{_get_project_agent_value(proj_id)}-exec-{proj_id}-"
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
    _pty_cols = int(websocket.query_params.get("cols", 220))
    _pty_rows = int(websocket.query_params.get("rows", 50))
    if tmux_session_name and _HAS_PTY:
        import ptyprocess as _pty_mod
        try:
            # M1125 removed: scrollback replay disabled — ANSI escape codes in capture-pane
            # output corrupt xterm.js rendering. Live stream from ptyprocess is sufficient.
            import subprocess as _sp
        except Exception:
            pass
        # M1371: verify session exists before attach — attach-session exits immediately
        # with code 1 when session is missing, causing instant "session closed" in UI.
        _sess_check = _sp.run(
            ["tmux", "has-session", "-t", f"={tmux_session_name}"],
            capture_output=True, timeout=3
        )
        if _sess_check.returncode != 0:
            await websocket.send_text(
                f"\r\n\x1b[33m[세션 없음: {tmux_session_name}]\x1b[0m\r\n"
                f"\r\n\x1b[2m[Execute 버튼으로 세션을 먼저 시작하세요.]\x1b[0m\r\n"
            )
            await websocket.close()
            return
        try:
            # M283: attach-session with TERM=xterm-256color works correctly —
            # input is forwarded to the exec pane. new-session -t does NOT forward input.
            # timeout=None: disable pexpect's 30s TIMEOUT so idle sessions don't disconnect.
            proc = _pty_mod.PtyProcess.spawn(
                ["tmux", "attach-session", "-t", tmux_session_name],
                env={**os.environ, "TERM": "xterm-256color"},
                dimensions=(_pty_rows, _pty_cols),
            )
            proc.timeout = None
        except Exception as e:
            await websocket.send_text(f"\r\n[Failed to attach to tmux session {tmux_session_name}: {e}]\r\n")
            await websocket.close()
            return
        # M1685: register so the poller can self-heal if this relay leaks (see registry comment above)
        _exec_attach_relay[tmux_session_name] = {"pid": proc.pid, "attached_at": time.time()}
        import signal as _signal
        _proc_killed = False
        def _kill_proc_sigkill():
            nonlocal _proc_killed
            if _proc_killed:
                return
            _proc_killed = True
            try:
                if proc and proc.isalive():
                    os.kill(proc.pid, _signal.SIGKILL)
            except Exception:
                pass
            _exec_attach_relay.pop(tmux_session_name, None)

        queue: asyncio.Queue = asyncio.Queue(maxsize=512)
        async def _tmux_to_ws():
            import select as _select
            loop = asyncio.get_event_loop()
            def _read_chunk():
                # M1425: ptyprocess has no read_nonblocking; use select+os.read for 100ms timeout.
                r, _, _ = _select.select([proc.fd], [], [], 0.1)
                if not r:
                    return None
                try:
                    return os.read(proc.fd, 4096)
                except OSError:
                    return b""
            try:
                while True:
                    if not proc.isalive(): break
                    try:
                        data = await loop.run_in_executor(None, _read_chunk)
                        if data is None:
                            continue
                        if not data:
                            break
                        await websocket.send_text(data if isinstance(data, str) else data.decode("utf-8", errors="replace"))
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        break
            finally:
                try: await websocket.send_text("\r\n\x1b[33m[Detached from tmux session]\x1b[0m\r\n")
                except Exception: pass
        _tmux_resized_once = False
        async def _ws_to_tmux():
            nonlocal _tmux_resized_once
            while True:
                try:
                    msg = await websocket.receive_text()
                    if msg.startswith('\x00resize:'):
                        parts = msg[8:].split(',')
                        if len(parts) == 2:
                            try:
                                proc.setwinsize(int(parts[1]), int(parts[0]))
                                if not _tmux_resized_once:
                                    _tmux_resized_once = True
                                    # First resize — force tmux to redraw the full screen.
                                    asyncio.get_event_loop().run_in_executor(
                                        None, lambda: _sp.run(["tmux", "refresh-client", "-t", tmux_session_name], timeout=3)
                                    )
                            except Exception: pass
                    else:
                        proc.write(msg.encode("utf-8") if isinstance(msg, str) else msg)
                except WebSocketDisconnect: break
                except Exception: break
            # M1425: SIGKILL directly — os.read via select wakes within 100ms after proc dies.
            _kill_proc_sigkill()
        try:
            tmux_task = asyncio.create_task(_tmux_to_ws())
            ws_task = asyncio.create_task(_ws_to_tmux())
            done, pending = await asyncio.wait(
                {tmux_task, ws_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            ws_done = ws_task in done
            if ws_done:
                _kill_proc_sigkill()
            for task in pending:
                task.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=2.0)
            except asyncio.TimeoutError:
                _kill_proc_sigkill()
        finally:
            _kill_proc_sigkill()
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
    # M1368: SQLite-first
    p = _db_load_project(proj_id)
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
        # M1081: Distribute milestones_store stones into substars by substar_id
        # instead of dumping everything into the first substar.
        try:
            conn = sqlite3.connect(str(_NS_EVENTS_DB))
            ms_rows = conn.execute(
                "SELECT data_json FROM milestones_store WHERE proj_id=?",
                (proj_id,)
            ).fetchall()
            conn.close()
            for r in ms_rows:
                try:
                    m = json.loads(r[0])
                    sid = m.get("substar_id", "")
                    if not sid:
                        continue
                    for ns in ns_list:
                        if ns.get("id") == sid:
                            ns["milestones"].append(m)
                            break
                except Exception:
                    pass
        except Exception:
            pass
        # If all NS entries still have empty milestones but top-level milestones exist,
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


@app.get("/api/northstar/{proj_id}/north-stars/{ns_id}/last-worked-session")
async def get_last_worked_session(proj_id: str, ns_id: str):
    """M1787: pure read — surfaces the "직전 세션" (last-worked session) candidate for an
    unassigned substar, for the sess popup to show as a clearly-labeled pick rather than
    something the server auto-assigns/auto-spawns on its own (M1780: no silent auto-spawn on
    unassigned; a human should see and choose). Never writes assigned_session, never spawns —
    same _last_worked_session_for_substar lookup used server-side, just exposed read-only."""
    _active = _db_get_active_milestones(proj_id) or []
    _last = _last_worked_session_for_substar(proj_id, ns_id, _active)
    if not _last:
        # M1879 fallback: UUID-suffix guard filtered out all candidates (legacy pre-M1796 sessions).
        # Do a display-only scan that accepts ANY claimed_by_session — returns is_resumable=False
        # so the UI can still show a "prior work" badge without misleading the user about resumability.
        _any_cb: str | None = None
        _any_at: float = 0.0
        for _m in _active:
            if not isinstance(_m, dict) or (_m.get("substar_id") or "") != ns_id:
                continue
            _cb = (_m.get("claimed_by_session") or "").strip()
            if not _cb:
                continue
            _at = _m.get("claimed_at") or 0
            if _at > _any_at:
                _any_at = _at
                _any_cb = _cb
        if _any_cb:
            return JSONResponse({"ok": True, "session_name": _any_cb,
                                 "resumable_sid": None, "is_live": False,
                                 "is_resumable": False})
        return JSONResponse({"ok": True, "session_name": None})
    _name, _sid = _last
    return JSONResponse({"ok": True, "session_name": _name, "resumable_sid": _sid or None,
                         "is_live": _tmux_session_alive(_name), "is_resumable": True})


@app.post("/api/northstar/{proj_id}/north-stars")
async def add_north_star(proj_id: str, request: Request):
    """M204: Add a new sub-star (north star entry) to the project."""
    data = await request.json()
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    ns_list = proj.get("north_stars") or []
    new_id = data.get("id") or f"star_{int(time.time())}"
    # M1449: idempotency guard — skip if same id already exists (prevents CG node double-POST duplicates)
    if any(ns.get("id") == new_id for ns in ns_list):
        existing = next(ns for ns in ns_list if ns.get("id") == new_id)
        return JSONResponse({"ok": True, "north_star": existing, "duplicate": True})
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
    # M1347: removed _sync_substars_to_claude_md call — hub no longer writes project CLAUDE.md
    # M1460: auto-create matching concept graph node so CG reflects new substar immediately
    try:
        cg_conn = sqlite3.connect(str(_NS_EVENTS_DB))
        exists = cg_conn.execute(
            "SELECT 1 FROM concept_graph_nodes WHERE proj_id=? AND node_id=?", (proj_id, new_id)
        ).fetchone()
        if not exists:
            cg_conn.execute(
                "INSERT INTO concept_graph_nodes(proj_id, node_id, layer, name, parents_json, x, y, layer_order, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (proj_id, new_id, 0, new_ns["name"], "[]", None, None, None, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
            cg_conn.commit()
        cg_conn.close()
    except Exception:
        pass  # CG node auto-create is best-effort; substar creation already succeeded
    return JSONResponse({"ok": True, "north_star": new_ns})


@app.delete("/api/northstar/{proj_id}/north-stars/{ns_id}")
async def delete_north_star(proj_id: str, ns_id: str):
    """M204: Delete a sub-star (north star entry) from the project.
    M211: 'default' refers to the main project star — clears its metric/target/current fields."""
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    # M1347: removed _sync_substars_to_claude_md call — hub no longer writes project CLAUDE.md
    # M1423: cascade-delete matching CG node (cg_{node_id} → node_id) when substar is removed
    if ns_id.startswith("cg_"):
        _cg_node_id = ns_id[3:]  # strip "cg_" prefix
        try:
            _cg_conn = sqlite3.connect(str(_NS_EVENTS_DB))
            _cg_conn.execute(
                "DELETE FROM concept_graph_nodes WHERE proj_id=? AND node_id=?",
                (proj_id, _cg_node_id),
            )
            _cg_conn.commit()
            _cg_conn.close()
        except Exception:
            pass
    return JSONResponse({"ok": True, "deleted_cg_node": ns_id[3:] if ns_id.startswith("cg_") else None})


@app.patch("/api/northstar/{proj_id}/north-stars/{ns_id}")
async def update_north_star(proj_id: str, ns_id: str, request: Request):
    """M249: Edit an existing sub-star entry (name, metric, target, current, status)."""
    data = await request.json()
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    ns_list = proj.get("north_stars") or []
    ns = next((x for x in ns_list if x.get("id") == ns_id), None)
    if not ns:
        return JSONResponse({"ok": False, "error": "north-star not found"}, status_code=404)
    _old_assigned = (ns.get("assigned_session") or "").strip()
    for field in ("name", "metric", "target", "current", "status", "default_agent", "assigned_session"):
        if field in data:
            ns[field] = data[field] or None if field in ("default_agent", "assigned_session") else data[field]
    proj["north_stars"] = ns_list
    # M1745: Assign panel's agent/model picker — request-scoped only (not persisted on the
    # substar), consumed below by the assign-spawn block. "" falls back to claude / project model.
    _spawn_agent = (data.get("spawn_agent") or "").strip().lower()
    _spawn_model = (data.get("spawn_model") or "").strip()
    _save_project(proj_id, proj)  # M289: was _write_md_frontmatter — bypassed SQLite project_meta
    # M1347: removed _sync_substars_to_claude_md call — hub no longer writes project CLAUDE.md  # PermissionError or missing CLAUDE.md must not abort the primary SQLite save

    # M1656-①: Assign button → auto-spawn if session is dead so M1095 poller doesn't wipe it
    _new_assigned = (ns.get("assigned_session") or "").strip()
    if _new_assigned and _new_assigned != _old_assigned:
        # M1809-assign: same UUID-suffix guard as assigned-respawn path — legacy names (no 8-char suffix)
        # get a fresh UUID-suffixed name here too, so assign-button spawns never create bare
        # 'claude-exec-{proj_id}' sessions that break _parse_exec_session_name (M1770 re-risk).
        import uuid as _uuid_asn, re as _re_asn
        if not _re_asn.search(r'-exec-[^-]+-[0-9a-f]{8}$', _new_assigned):
            _asn_uuid = str(_uuid_asn.uuid4()).replace("-", "")[:8]
            _asn_new = f"claude-exec-{proj_id}-{_asn_uuid}"
            ns["assigned_session"] = _asn_new
            proj["north_stars"] = ns_list
            _save_project(proj_id, proj)
            _server_log_action(proj_id, ns_id, "exec:assign_sess_renamed", f"{_new_assigned}→{_asn_new}")
            _new_assigned = _asn_new
        # M1838: clear stale claims on this substar's queued stones when session changes.
        # A stale claim from the OLD session blocks the NEW session from picking up stones
        # until claim TTL (120s) expires. Since the user explicitly reassigned the substar,
        # claims from any session other than _new_assigned are now dead weight — clear them.
        try:
            _now_clr = time.time()
            _clr_conn = sqlite3.connect(str(_NS_EVENTS_DB))
            _clr_rows = _clr_conn.execute(
                "SELECT stone_id, data_json FROM milestones_store "
                "WHERE proj_id=? AND status='queued' AND done=0",
                (proj_id,)
            ).fetchall()
            _clr_count = 0
            for _sid_clr, _dj_clr in _clr_rows:
                try:
                    _m_clr = json.loads(_dj_clr)
                except Exception:
                    continue
                if (_m_clr.get("substar_id") or "") != ns_id:
                    continue
                _cb_clr = (_m_clr.get("claimed_by_session") or "").strip()
                _ca_clr = _m_clr.get("claimed_at") or 0
                # Only clear if claimed by a DIFFERENT session and still within TTL
                if _cb_clr and _cb_clr != _new_assigned and (_now_clr - _ca_clr) < _CLAIM_TTL_SECS:
                    _m_clr["claimed_by_session"] = None
                    _m_clr["claimed_at"] = None
                    _clr_conn.execute(
                        "UPDATE milestones_store SET data_json=? WHERE proj_id=? AND stone_id=?",
                        (json.dumps(_m_clr, ensure_ascii=False), proj_id, _sid_clr)
                    )
                    _clr_count += 1
            _clr_conn.commit()
            _clr_conn.close()
            if _clr_count:
                _server_log_action(proj_id, ns_id, "exec:assign_claim_cleared",
                                   f"cleared {_clr_count} stale claim(s) for {_new_assigned}")
        except Exception as _clr_e:
            pass  # fail-open — don't abort the assign for a claim-clear error
        try:
            _chk = subprocess.run(["tmux", "has-session", "-t", f"={_new_assigned}"],
                                   capture_output=True, timeout=2)
            if _chk.returncode != 0:
                # M1656-① fix2: use repo_path resolver (was PROJECTS_DIR = ~/.hub metadata dir)
                _proj_dir = _get_project_dir(proj_id) or ""
                _spawn_cwd = _proj_dir if _proj_dir and Path(_proj_dir).exists() else str(Path.home())
                _pf = PROJECTS_DIR / proj_id / f"pending-execute-prompt-{_new_assigned[-12:]}.txt"
                _pf.write_text(
                    _load_stone_memory(proj_id)
                    + f"[EXECUTE SYNC] Project {proj_id} — Session '{_new_assigned}' just assigned.\n"
                    f"Call get_pending_task() to find your assigned stones.",
                    encoding="utf-8",
                )
                # M1745: agent/model chosen in the Assign panel picker — falls back to
                # claude + project default model when the request omits them (e.g. programmatic
                # PATCH calls that predate this field).
                _sp_agent = _spawn_agent if _spawn_agent in ("claude", "openrouter") else "claude"
                _sp_override_model = _spawn_model or None
                _sp_env = ["-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                           "-e", f"NS_SESSION_KEY={_new_assigned}",
                           "-e", "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80"]  # M1656-R8: children rely on CLI auto-compact
                for _k, _v in _get_project_spawn_env(proj_id, override_model=_sp_override_model).items():
                    _sp_env += ["-e", f"{_k}={_v}"]
                # M1787: if the session name being (re)assigned matches a resumable transcript
                # this exact name previously owned (the "직전 세션" candidate M1780 asked to
                # surface in the sess popup instead of auto-spawning it), resume THAT transcript
                # directly — --resume <sid>, not a mother-fork. A human explicitly picking "last
                # worked session X" wants X's own conversation back, not a fresh branch off
                # whatever mother happens to be doing right now.
                _sp_active_ms = _db_get_active_milestones(proj_id) or []
                _sp_last = _last_worked_session_for_substar(proj_id, ns_id, _sp_active_ms)
                if _sp_last and _sp_last[0] == _new_assigned and _sp_last[1]:
                    _sp_resume_args = ["--resume", _sp_last[1]]
                    # M1796-assign: align session name suffix with resume transcript UUID so
                    # tmux session name and live_session_id are consistent (same as /execute path).
                    # _sp_last[1] is the full UUID string; take first 8 hex chars after stripping dashes.
                    _sp_resume_suffix = _sp_last[1].replace("-", "")[:8]
                    _sp_aligned_name = f"claude-exec-{proj_id}-{_sp_resume_suffix}"
                    if _sp_aligned_name != _new_assigned:
                        _old_pf = PROJECTS_DIR / proj_id / f"pending-execute-prompt-{_new_assigned[-12:]}.txt"
                        try:
                            _old_pf.unlink(missing_ok=True)  # remove stale file written under old name
                        except Exception:
                            pass
                        _new_assigned = _sp_aligned_name
                        ns["assigned_session"] = _new_assigned
                        proj["north_stars"] = ns_list
                        _save_project(proj_id, proj)
                        _server_log_action(proj_id, ns_id, "exec:assign_sess_aligned",
                                           f"suffix aligned to resume sid → {_new_assigned}")
                        # NS_SESSION_KEY was built with the pre-alignment name — patch it now
                        _sp_env = [
                            e if not e.startswith(f"NS_SESSION_KEY=") else f"NS_SESSION_KEY={_new_assigned}"
                            for e in _sp_env
                        ]
                        # re-write spawn-prompt file with corrected name
                        _pf = PROJECTS_DIR / proj_id / f"pending-execute-prompt-{_new_assigned[-12:]}.txt"
                        _pf.write_text(
                            _load_stone_memory(proj_id)
                            + f"[EXECUTE SYNC] Project {proj_id} — Session '{_new_assigned}' just assigned.\n"
                            f"Call get_pending_task() to find your assigned stones.",
                            encoding="utf-8",
                        )
                else:
                    # M1656-fork: inherit mother conversation context via fork (fallback --continue)
                    _sp_resume_args = _mother_fork_args(proj_id, _spawn_cwd)
                # M1775: when actually forking (--fork-session present), pre-assign the new
                # session id via --session-id <uuid> instead of polling the transcript dir for
                # the new file — verified via `claude --help` + live test that --session-id
                # composes with --resume/--fork-session and the transcript is written to
                # exactly that UUID's .jsonl. Eliminates the capture race entirely for this
                # path. --continue fallback (no fork) is untouched — that path's session id
                # is the CLI's own concern (continuation semantics), not ours to pre-assign.
                _sp_presigned_sid = ""
                if "--fork-session" in _sp_resume_args:
                    import uuid as _uuid_sp
                    _sp_presigned_sid = str(_uuid_sp.uuid4())
                    _sp_resume_args = [*_sp_resume_args, "--session-id", _sp_presigned_sid]
                # M1656-R9: session-scoped spawn-info record — was never written for children,
                # so their exec-pane badge always showed "— unknown" instead of ↻ fork/resume.
                _record_spawn_info(proj_id, _sp_resume_args, agent=_sp_agent, session_name=_new_assigned,
                                   model=_sp_override_model or _get_project_model_value(proj_id) or "")
                _sp_tdir = Path.home() / ".claude" / "projects" / _encode_cwd_for_claude(str(_spawn_cwd))
                _sp_pre_files: set = set()
                _sp_marker = None
                if not _sp_presigned_sid:
                    # Non-fork fallback (--continue): keep the polling capture for this case.
                    # Snapshot BEFORE Popen so the bg capture can find the one file THIS spawn
                    # creates (M1762-b: capturing after Popen risks a 0-diff on a fast boot).
                    try:
                        _sp_pre_files = {f.name for f in _sp_tdir.glob("*.jsonl")}
                    except Exception:
                        _sp_pre_files = set()
                    _sp_marker = _write_spawn_marker(_sp_tdir, _new_assigned)
                _sp_cmd = (["claude", "--dangerously-skip-permissions"] + _DISALLOWED_TOOLS_ARGS
                           + _hub_mcp_spawn_args(proj_id, _new_assigned, agent=_sp_agent)
                           + _sp_resume_args + _get_project_model(proj_id, override_model=_sp_override_model))
                subprocess.Popen(["tmux", "new-session", "-d", "-s", _new_assigned, "-c", _spawn_cwd]
                                 + _sp_env + _sp_cmd)
                subprocess.run(["tmux", "set-option", "-t", _new_assigned, "history-limit", "5000"],
                               capture_output=True, timeout=2)  # M1757: parity with branch/fork
                if _sp_presigned_sid:
                    _write_sid_direct(proj_id, _new_assigned, _sp_presigned_sid)
                else:
                    _capture_live_session_id_bg(proj_id, _spawn_cwd, _new_assigned, _sp_pre_files, spawn_marker=_sp_marker)
                # M1656-R8 (user decision): no server-side compact gating — Claude's
                # built-in auto-compact (CLAUDE_AUTOCOMPACT_PCT_OVERRIDE) handles full context.
                # M1678: readiness-gated unified wake (was blind send-keys during boot)
                threading.Thread(target=_spawn_wake_when_ready,
                                 args=(_new_assigned, proj_id), daemon=True).start()
                _server_log_action(proj_id, ns_id, "exec:assign_spawn", _new_assigned)
                # Grace window: M1095 poller skips clearing for 60s after fresh spawn
                ns["spawning_grace_until"] = time.time() + 60
                proj["north_stars"] = ns_list
                _save_project(proj_id, proj)
                # Register idle in OOB so poller can immediately wake
                _agent_busy_sessions[_new_assigned] = {
                    "proj_id": proj_id, "busy": False,
                    "reason": "assign_spawn_init", "ts": time.time()
                }
                _ns_push("session_update", proj_id=proj_id)
            else:
                # M1799: session already alive — the old code path had no wake here.
                # The poller eventually fires (up to 90s dedup window), but if user just
                # re-assigned an idle alive session to a queued stone, the session should
                # pick it up immediately. force_dedup_reset=True clears any stale
                # _wake_last_sent[_new_assigned] from before the reassignment.
                if _session_claimable_queued_count(proj_id, _new_assigned) > 0:
                    _send_exec_wake(_new_assigned, proj_id, force_dedup_reset=True)
                    _server_log_action(proj_id, ns_id, "exec:assign_wake", _new_assigned)
        except Exception as _e:
            _server_log_action(proj_id, ns_id, "exec:assign_spawn_err", str(_e)[:120])

    # M1469: sync CG node name when substar name changes
    # substar id = "cg_<node_id>"; strip "cg_" prefix to get CG node_id
    if "name" in data:
        try:
            _cg_node_id = ns_id[3:] if ns_id.startswith("cg_") else ns_id
            _cg_conn = sqlite3.connect(str(_NS_EVENTS_DB))
            _cg_conn.execute(
                "UPDATE concept_graph_nodes SET name=?, updated_at=? WHERE proj_id=? AND node_id=?",
                (data["name"], time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), proj_id, _cg_node_id),
            )
            _cg_conn.commit()
            _cg_conn.close()
        except Exception:
            pass
    return JSONResponse({"ok": True, "north_star": ns})


@app.post("/api/northstar/{proj_id}/north-stars/reorder")
async def reorder_north_stars(proj_id: str, request: Request):
    """M242: Reorder sub-stars via drag/drop — move dragged_id to position of target_id."""
    data = await request.json()
    dragged_id = data.get("dragged_id", "")
    target_id = data.get("target_id", "")
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    related = [m for m in milestones if m.get("substar_id") == ns_id]
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
            f"5. **Concise reporting** — completion messages: STRUCTURED (bullets/table/numbered) = no line limit; UNSTRUCTURED prose = ≤3 lines (1 line ideal). Past tense, no preamble.",
            f"",
            f"## NS Hub Completion Protocol",
            f"",
            f"When a task (stone) is complete:",
            f"```",
            f"PATCH /api/northstar/{{proj_id}}/milestones/{{mid}}",
            f"{{",
            f'  "status": "pending_confirmation",',
            f'  "model_used": "<model-id>",',
            f'  "exec_start": "<ISO timestamp>",',
            f'  "exec_end": "<ISO timestamp>",',
            f'  "append_message": {{"role": "claude", "text": "<structured: no limit | prose: ≤3 lines, past-tense result>"}}',
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
            f"3. PATCH `status=pending_confirmation` with `append_message` (STRUCTURED: no limit; prose: ≤3 lines, past tense; 1 line prose ideal).",
            f"4. Include `model_used`, `exec_start`, `exec_end` in the same PATCH.",
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
            import hashlib as _hl
            _body = json.dumps({"ok": True, "milestones": milestones}, separators=(',', ':'))
            _etag = '"' + _hl.md5(_body.encode()).hexdigest()[:16] + '"'
            if request.headers.get("If-None-Match") == _etag:
                return Response(status_code=304, headers={"ETag": _etag, "Cache-Control": "no-store"})
            return Response(content=_body, media_type="application/json",
                            headers={"Cache-Control": "no-store", "ETag": _etag})
    except Exception:
        pass
    # M1368: DB fallback (no YAML needed)
    p = _db_load_project(proj_id)
    if not p:
        return JSONResponse({"ok": False, "milestones": []})
    milestones = p.get("milestones", [])
    # M527: strip conversation from done stones — reduces payload from ~800KB to ~200KB
    for m in milestones:
        if isinstance(m, dict) and (m.get("done") or m.get("status") == "done"):
            m.pop("conversation", None)
    return JSONResponse({"ok": True, "milestones": milestones},
                        headers={"Cache-Control": "no-store"})


@app.post("/api/northstar/{proj_id}/claim-task")
async def claim_task_for_session(proj_id: str, request: Request):
    """M1656-③: atomically claim the next queued stone for a session.
    POST body: {"session_name": "claude-exec-MOAT-M1656"}
    Priority: stones whose substar is assigned to session_name first, then free-pool stones.
    Claim = claimed_by_session + claimed_at fields with 120s TTL — status stays 'queued'
    (UI has no 'in_progress' renderer; status mutation would hide the stone from the table)."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    session_name = (body.get("session_name") or "").strip()

    conn = sqlite3.connect(str(_NS_EVENTS_DB))
    try:
        # Fetch all queued stones for project
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND status='queued' ORDER BY rowid",
            (proj_id,)
        ).fetchall()
        stones = [json.loads(r[0]) for r in rows]
        # Filter: not held, has text, and not claimed by ANOTHER session within TTL (120s)
        _claim_ttl = _CLAIM_TTL_SECS
        _now_c = time.time()

        def _claim_blocked(m: dict) -> bool:
            _cb = (m.get("claimed_by_session") or "").strip()
            _ca = m.get("claimed_at") or 0
            if _cb and _cb != session_name and (_now_c - _ca) < _claim_ttl:
                return True
            # M1783: same-session re-claim while still actively working the SAME task.
            # Status stays 'queued' by design (no in_progress UI state), so a stone
            # this exact session already claimed is otherwise indistinguishable from
            # a fresh one once _claim_ttl (120s) elapses — a stray wake mid-work
            # (e.g. landing right after a compaction boundary while a background
            # subagent, like expert-research's Fact Finder, is still in flight) then
            # re-claims and re-runs the same task.
            # M1785 fix: this must NOT block a legitimate re-claim to answer a NEW user
            # reply posted on a stone the session already owns (long-lived exec sessions
            # revisit their own stones constantly across a REPLY-SYNC cycle) — observed on
            # FromScratch M117, where a real "현재 aft 상태 확인" follow-up got silently
            # swallowed because the owning session was transiently busy with other work.
            # Only block when there's no conversation activity after claimed_at — i.e. the
            # stone genuinely has nothing new since this session last picked it up.
            # M1803 fix: same-session busy-state alone is not proof the claim is real —
            # _session_is_busy also returns True for a "wake_sent" optimistic record set the
            # instant _send_exec_wake injects a wake, purely so the poller doesn't double-wake
            # while the just-woken session is still mid-claim (M1675). That record has nothing
            # to do with whether THIS claim is actually still in flight. Root cause traced
            # further: no kill path in this file clears claimed_by_session when a session dies
            # (only assigned_session, at the substar level, gets cleared — server.py's own
            # _kill_all_exec_sessions). Since this hub always respawns under the exact same
            # deterministic tmux name, a session killed mid-claim leaves a claim that the
            # respawned process (same name) can never distinguish from "I claimed this a
            # moment ago" without an explicit staleness check — the cross-session branch above
            # already has one (_claim_ttl), the same-session branch didn't. Require the claim to
            # still be within _claim_ttl before even considering it a same-session in-flight
            # candidate; past that window, wake_sent's momentary busy=true must not resurrect a
            # claim whose owning process may well be dead. Observed live: MOAT M1782, killed
            # mid-work, respawned under the identical tmux name, then blocked from re-claiming
            # its own inherited stone for 15+ min despite the poller correctly reporting
            # queued_count:1 every cycle — claimed_at was ~1000s old, far past _claim_ttl.
            if _cb == session_name and (_now_c - _ca) < _claim_ttl and _session_is_busy(session_name):
                import datetime as _dt_claim
                _conv_m = m.get("conversation") or []
                _has_new_activity = False
                for c in _conv_m:
                    if not isinstance(c, dict):
                        continue
                    try:
                        _c_ts = _dt_claim.datetime.fromisoformat((c.get("ts") or "").replace("Z", "+00:00")).timestamp()
                    except Exception:
                        continue
                    if _c_ts > _ca:
                        _has_new_activity = True
                        break
                if not _has_new_activity:
                    return True
            return False

        candidates = [m for m in stones
                      if not m.get("held") and str(m.get("text", "")).strip()
                      and not _claim_blocked(m)]

        # M1656-③ v2: stones link to sessions via substar_id → substar.assigned_session
        # (stones do NOT carry assigned_session directly — that field lives on substars)
        _proj = _db_load_project(proj_id) or {}
        _substar_session: dict = {}   # substar_id → assigned_session
        for _ns in (_proj.get("north_stars") or []):
            if isinstance(_ns, dict) and _ns.get("id"):
                _substar_session[_ns["id"]] = (_ns.get("assigned_session") or "").strip()

        def _stone_session(m: dict) -> str:
            """Resolve which session a stone belongs to: per-stone override first, then substar."""
            _ov = (m.get("session_override") or "").strip()
            if _ov:
                return _ov
            _sid = (m.get("substar_id") or "").strip()
            return _substar_session.get(_sid, "") if _sid else ""

        # M1797: mother/child concept removed — all sessions can claim free-pool stones.

        stone = None
        if session_name:
            # M1860: only claim stones whose substar is explicitly assigned to this session.
            # Free-pool (unassigned substar) stones are no longer claimable by any session —
            # substar must be assigned before work begins.
            for m in candidates:
                if _stone_session(m) == session_name:
                    stone = m
                    break
            # NOTE: stones assigned to OTHER sessions, or to no session, are never returned.
        else:
            # No session context — return first queued (original behaviour, internal callers only)
            stone = candidates[0] if candidates else None

        if stone is None:
            conn.close()
            # M1800: log the empty-handed case with the received session_name — previously
            # only the success/race-lost paths logged anything, so a claim-task call that
            # silently returned nothing left no trace of what session_name it actually saw.
            # This mattered concretely: MOAT's claude-exec-MOAT called get_pending_task 11x
            # in a row with queued_count:1 confirmed by the poller each time, all returning
            # has_task:false, while a manual curl with session_name="claude-exec-MOAT" (the
            # supposedly-correct value) succeeded immediately — with no log of what session_name
            # the live calls actually sent, the mismatch (if any) was unprovable after the fact.
            _server_log_action(
                proj_id, "", "task:claim_empty",
                f"session:{session_name or '(empty)'} candidates:{len(candidates)} "
                f"assigned_substars:{sorted(_substar_session.values())}"
            )
            return JSONResponse({"has_task": False, "queued_count": 0})

        # Claim via TTL fields only — status remains 'queued' (UI-safe)
        # M1656-⑥ atomicity: conditional UPDATE guards against concurrent claims —
        # two sessions POSTing within the same second both passed the read-time filter
        # (observed 14:51:29 dual-claim). The WHERE clause re-checks the stored JSON's
        # claim fields so only ONE writer wins; the loser retries the next candidate.
        stone_id = stone.get("id", "")
        _prev_json = json.dumps(stone, ensure_ascii=False)  # pre-claim serialization for CAS
        stone["claimed_by_session"] = session_name
        stone["claimed_at"] = _now_c
        cur = conn.execute(
            "UPDATE milestones_store SET data_json=? WHERE proj_id=? AND stone_id=? AND data_json=?",
            (json.dumps(stone, ensure_ascii=False), proj_id, stone_id, _prev_json)
        )
        conn.commit()
        if cur.rowcount == 0:
            # Lost the race — another session claimed/modified this stone between our
            # read and write. Return no-task; caller's next poll re-evaluates.
            conn.close()
            _server_log_action(proj_id, stone_id, "task:claim_race_lost", f"session:{session_name}")
            return JSONResponse({"has_task": False, "queued_count": len(candidates),
                                 "race_lost": True})
        conn.close()
        _server_log_action(proj_id, stone_id, "task:claimed", f"session:{session_name}")
        return JSONResponse({"has_task": True, "stone": stone, "queued_count": len(candidates)})
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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
        "tool_sequence",  # M775: step-level tool trace (list of {tool,input_summary,duration_ms})
    )
    # CSV/Parquet drop the nested conversation/tool_sequence arrays to keep the table flat;
    # JSON-typed fields are serialized as JSON strings.
    _FLAT_FIELDS = tuple(f for f in _EXPORT_FIELDS if f not in ("conversation", "tool_sequence"))
    _JSON_FIELDS = {"goal_tree_snapshot", "prompt_provenance", "confounder"}
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        rows = conn.execute(
            "SELECT data_json FROM milestones_store WHERE proj_id=? AND done=1 ORDER BY rowid DESC LIMIT ?",
            (proj_id, limit),
        ).fetchall()
        # M775: bulk-fetch tool_trace for all stone_ids in result
        stone_ids = []
        for (dj,) in rows:
            try:
                m = json.loads(dj)
                if m.get("id"):
                    stone_ids.append(m["id"])
            except Exception:
                pass
        _tool_seq_map: dict = {}
        if stone_ids:
            placeholders = ",".join("?" * len(stone_ids))
            trace_rows = conn.execute(
                f"SELECT stone_id,tool_name,input_summary,output_summary,duration_ms,ts FROM tool_trace WHERE stone_id IN ({placeholders}) ORDER BY ts ASC",
                stone_ids,
            ).fetchall()
            for tr in trace_rows:
                sid = tr[0]
                _tool_seq_map.setdefault(sid, []).append({
                    "tool": tr[1], "input": tr[2], "output": tr[3],
                    "duration_ms": tr[4], "ts": tr[5]
                })
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
        record["tool_sequence"] = _tool_seq_map.get(m.get("id"), [])
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
    # M1368: SQLite-first for goal-tree context
    try:
        proj = _db_load_project(proj_id) or {}
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

    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)

    import uuid as _uuid
    pair_id = str(_uuid.uuid4())

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
    _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
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
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    # Project existence check: project dir must exist (north-star.md not required — SQLite is primary)
    if not (PROJECTS_DIR / proj_id).is_dir():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    # Load existing milestones from SQLite (primary store); fallback to empty if new project
    proj = _db_load_project(proj_id) or {}
    milestones = proj.get("milestones", [])
    # Auto-assign ID
    existing_ids = {m.get("id","") for m in milestones if isinstance(m, dict)}
    # BUG-06 fix: layer must be int; string aliases ("root"→0, "substar"→1) accepted for UX
    _raw_layer = data.get("layer", 0)
    _LAYER_ALIASES = {"root": 0, "top": 0, "substar": 1, "child": 1, "sub": 1}
    if isinstance(_raw_layer, str):
        _raw_layer = _LAYER_ALIASES.get(_raw_layer.lower())
        if _raw_layer is None:
            return JSONResponse(
                {"ok": False, "error": "layer must be an integer (0=root, 1=substar/child). "
                 "Accepted aliases: 'root'→0, 'substar'→1."},
                status_code=422
            )
    try:
        layer = int(_raw_layer)
    except (TypeError, ValueError):
        return JSONResponse(
            {"ok": False, "error": f"layer must be an integer, got: {_raw_layer!r}"},
            status_code=422
        )
    parent_id = data.get("parent_id") or None
    # M603: auto-promote to layer=1 when parent_id is set but layer was not sent
    if parent_id and layer == 0:
        layer = 1
    # Generate ID: M{n} for layer 0, M{parent}.{n} for layer 1
    if layer == 0:
        # M441: use max existing numeric ID + 1 (not count+1) so IDs never recycle after deletions
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
    # M1935: inherit parent's substar_id when not explicitly set — child stones created via
    # create_child_stone MCP don't include substar_id, leaving them in the free-pool (unclaimable).
    _explicit_substar = data.get("substar_id") or None
    if not _explicit_substar and parent_id:
        _par_ms = next((m for m in milestones if isinstance(m, dict) and m.get("id") == parent_id), None)
        _explicit_substar = (_par_ms.get("substar_id") or None) if _par_ms else None
    new_ms = {
        "id": new_id, "text": data.get("text", "New milestone"),
        "layer": layer, "parent_id": parent_id,
        "done": False,
        "status": data.get("status", "pending"),
        "claude_ack": None,
        "user_added_at": _now_create,
        "substar_id": _explicit_substar,
        # M1907: is_progress=True marks mid-flight M190-bypass progress children.
        # These are auto-collapsed in UI and never dispatched to the free-pool queue.
        # Enforce pc status server-side so direct API callers can't accidentally set queued.
        "is_progress": bool(data.get("is_progress", False)),
    }
    if new_ms["is_progress"] and new_ms.get("status") not in ("pending_confirmation", "done"):
        new_ms["status"] = "pending_confirmation"
    milestones.insert(0, new_ms)  # M86: prepend so newest always appears first in UI
    proj["milestones"] = milestones
    # Write to SQLite synchronously (primary store) — no MD dependency
    _db_save_project(proj_id, proj)
    # Background: Turso cloud sync
    import copy as _c278c
    background_tasks.add_task(_save_project, proj_id, _c278c.deepcopy(proj))
    # M267: user-originated event — new stone added via UI.
    _ns_push("stone_created", proj_id=proj_id, mid=new_id,
             text=(new_ms.get("text") or "")[:140])
    # M698: record action_log entry so the activity panel shows new-stone creation
    # in real time. Previously POST /milestones was invisible to the log panel.
    _server_log_action(proj_id, new_id, "stone_create",
                       f"L{layer} parent:{parent_id or '-'} status:{new_ms.get('status','')} text:{(new_ms.get('text') or '')[:80]}")
    # M1348 P0-A: stone_create telemetry — funnel step 5
    _record_usage_event("stone_create", {"proj_id": proj_id, "layer": layer})
    # M1211: stamp server blink on creation — new stone needs user attention
    _blink_skey_create = new_ms.get("substar_id") or "__ungrouped__"
    _mark_blink_server(proj_id, _blink_skey_create, new_id)
    # M1673: instant dispatch on stone creation. An already-idle session otherwise waits
    # for the 10s poller AND the 90s wake dedup (measured 80s queue→claim when a wake
    # fired just before creation). Wake the target session directly when it is alive,
    # idle, and unwatched — same gates as the queue-continuation poller.
    if new_ms.get("status") == "queued" and not new_ms.get("held"):
        _ns_list_cw = proj.get("north_stars") or []
        _assigned_cw = ""
        for _ns_cw in _ns_list_cw:
            if isinstance(_ns_cw, dict) and _ns_cw.get("id") == new_ms.get("substar_id"):
                _assigned_cw = (_ns_cw.get("assigned_session") or "").strip()
                break
        # M1860: only wake the explicitly assigned session — no fallback to any live session.
        # Previously: _assigned_cw or _live_exec_session_name(proj_id) caused any alive session
        # to be woken for unassigned substars, producing the free-pool multi-session race.
        _target_cw = _assigned_cw

        def _stone_create_dispatch(_sn=_target_cw, _pid=proj_id, _mid=new_id):
            try:
                # M1678: alive/attached/busy gates unified inside _send_exec_wake.
                # Coalesce rapid stone bursts: bypass the 90s dedup only when the last
                # wake to this session is >10s old; within 10s the first wake suffices
                # (session drains the queue via task_complete immediate dispatch).
                _force = (time.time() - _wake_last_sent.get(_sn, 0.0)) > 10
                if _send_exec_wake(_sn, _pid, force_dedup_reset=_force):
                    _server_log_action(_pid, _mid, "exec:create_wake",
                                       f"session:{_sn} force_dedup_reset:{_force}")
            except Exception:
                pass

        background_tasks.add_task(_stone_create_dispatch)
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
def _stone_completion_summary(m: dict) -> str:
    """M1126: extract 'what was ACTUALLY done' from a completed stone — the last
    claude completion message (report_task_complete append_message).
    Fed into the review stone so the reviewer understands the
    real change instead of re-deriving it from the original request text."""
    if not isinstance(m, dict):
        return ""
    conv = m.get("conversation") or []
    for e in reversed(conv):
        if isinstance(e, dict) and e.get("role") == "claude" and str(e.get("text", "")).strip():
            return str(e.get("text", "")).strip()[:240]
    return ""


def _build_review_stone_text(mid: str, brief: str, change_summary: str = "") -> str:
    # M1126: change-aware + screen-verify focused. The old template prepended a
    # generic OWASP/0-10 checklist to a truncated COPY OF THE ORIGINAL REQUEST, so
    # the reviewer never knew what was actually changed ("허접"). Now we embed the
    # actual completion summary and reduce the checklist to "does the applied
    # update work on screen?" — a simple visual verification, not a code audit.
    _applied = f"적용내용: {change_summary}\n" if change_summary else ""
    return (
        f"[검수] {mid}: {brief}\n"
        f"{_applied}"
        f"→ 위 변경이 화면에서 실제로 잘 동작하는지만 확인 (해당 UI를 Playwright로 열어 동작 실측).\n"
        f"①무엇이 바뀌었나 1줄 + 변경 파일\n"
        f"②화면 실측: 의도대로 동작하나? (UI 변경이면 스크린샷 1장)\n"
        f"③눈에 띄는 깨짐/빈값 없나 빠르게 점검\n"
        f"④판정: 동작 OK / 수정 필요 — 이유 1줄"
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
    # M1368: project dir existence only (SQLite is primary)
    if not (PROJECTS_DIR / proj_id).exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    # M470 part 2: serialize read-modify-write so concurrent PATCH requests on the
    # same project cannot clobber each other (e.g. text-blur saveMs racing with
    # queue-toggle click on the same stone, where each reads proj independently
    # and the later save with a stale read overwrites the earlier status change).
    async with _get_proj_lock(proj_id):
        return await _update_milestone_locked(proj_id, mid, data, background_tasks, force_done)


async def _update_milestone_locked(proj_id: str, mid: str, data: dict, background_tasks: BackgroundTasks, force_done: bool = False):
    # M1368: SQLite-first
    proj = _db_load_project(proj_id) or {}
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
            for k in ("text", "layer", "parent_id", "claude_ack", "cron_job_id", "claude_comment", "substar_id", "held", "verify_flag",  # M964: verify toggle
                      "skill_ref", "agent_ref", "skill_refs", "agent_refs", "adj_refs",
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
                      "counterfactual_pair_id",  # peer child stone ID at branch points
                      "conversation_summary",     # M1253: LLM-generated stone-conversation summary
                      "summary_state",           # M1253: {last_compressed_len, last_compressed_at, version}
                      "evidence_filename",         # M1304: local filename hint — avoids GDrive API lookup
                      "session_override"):         # M1916: per-stone session assignment (overrides substar)
                if k in data:
                    # M1316 fix: capture old evidence_url BEFORE overwriting m[k].
                    # Previously _old_ev was read after m[k] = data[k] so it always
                    # equalled the new value → history never appended.
                    _old_ev_pre = m.get("evidence_url") if k == "evidence_url" else None
                    _old_fname_pre = m.get("evidence_filename") if k == "evidence_url" else None
                    _old_ts_pre = m.get("evidence_updated_at") if k == "evidence_url" else None
                    m[k] = data[k] if data[k] is not None else None
                    # M511: auto-stamp evidence_updated_at when evidence_url changes
                    if k == "evidence_url" and data[k]:
                        # M1327: reject file:// and absolute local paths — only http(s)/ drive links valid
                        _ev_raw = (data[k] or "").strip()
                        if _ev_raw.startswith("file://") or (_ev_raw.startswith("/") and not _ev_raw.startswith("//")):
                            m.setdefault("_proof_warning", f"M1327:evidence_url rejected — local path not accessible from remote clients; upload to GDrive first: rclone copy <file> 'gdrive:claude-shared/{proj_id}/outbox/'")
                            data[k] = None  # discard, do not store
                            m[k] = _old_ev_pre  # M1404 B1 fix: revert m[k] — was written before validation
                        # M1316: append old evidence_url to evidence_history before overwriting
                        if _old_ev_pre and _old_ev_pre != data[k]:
                            _ev_hist = m.get("evidence_history") or []
                            _ev_hist.append({
                                "url": _old_ev_pre,
                                "ts": _old_ts_pre or now_iso,
                                "filename": _old_fname_pre or "",
                            })
                            m["evidence_history"] = _ev_hist
                        m["evidence_updated_at"] = now_iso
                        # M1304: auto-seed filename cache from evidence_filename hint
                        _ev_fname = (data.get("evidence_filename") or "").strip()
                        if _ev_fname:
                            import re as _re
                            _fid_m = _re.search(r"/file/d/([^/]+)", data[k])
                            if _fid_m:
                                _gdrive_name_cache[_fid_m.group(1)] = _ev_fname
                        # M1697: soft warning (UI-visible, non-blocking) when evidence_url
                        # is accepted (survived the M1327 file:// check above) but no
                        # filename hint came with it — badge will depend entirely on the
                        # async /api/gdrive-filename fallback, which can fail under GDrive
                        # API quota pressure (root cause of the M1694 incident).
                        if data[k] and not _ev_fname:
                            m.setdefault("_proof_warning", "M1697:evidence_url attached without evidence_filename — badge display depends on a live GDrive API lookup that can fail under quota pressure; pass evidence_filename to avoid this.")
                    # M1253: bump summary_state when conversation_summary is replaced.
                    if k == "conversation_summary" and data[k] and data.get("summary_state_bump"):
                        _conv_len_now = len(m.get("conversation") or [])
                        m["summary_state"] = {
                            "last_compressed_len": _conv_len_now,
                            "last_compressed_at": now_iso,
                            "version": (m.get("summary_state", {}) or {}).get("version", 0) + 1,
                        }
                    # M1916: when session_override is set/changed, evict any stale claim from
                    # the OLD session (substar-assigned or previous override). Without this,
                    # the old session's claim (within 120s TTL) causes it to pick up and
                    # execute the stone despite the explicit override — same bug pattern as
                    # substar reassignment (fixed at exec:assign path L6930).
                    if k == "session_override":
                        _new_ov = (data[k] or "").strip()
                        _old_cb = (m.get("claimed_by_session") or "").strip()
                        _old_ca = m.get("claimed_at") or 0
                        if _old_cb and _old_cb != _new_ov and (time.time() - _old_ca) < _CLAIM_TTL_SECS:
                            m["claimed_by_session"] = None
                            m["claimed_at"] = None
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
            # M1238: treat top-level "comment" key as append_message alias (plain text, claude role)
            if "comment" in data and "append_message" not in data:
                _comment_text = data["comment"]
                if isinstance(_comment_text, str) and _comment_text.strip():
                    data = dict(data, append_message={"role": "claude", "text": _comment_text.strip()})
            # append_message: single {role,text} dict — appended to conversation (easier for Claude)
            # M643: accept both 'text' and 'content' keys — Claude sometimes sends 'content' by mistake
            if "append_message" in data:
                msg = data["append_message"]
                # M1618-A: reject plain string — LLM must send {"role":"claude","text":"..."} dict.
                # Silent-fail caused missed chatbox entries; explicit 400 forces LLM retry.
                if isinstance(msg, str):
                    return JSONResponse({
                        "ok": False,
                        "error": "append_message_type_error",
                        "detail": "append_message must be a dict {\"role\": \"claude\", \"text\": \"...\"}, not a plain string. Retry with correct format."
                    }, status_code=400)
                if isinstance(msg, dict) and not msg.get("text") and msg.get("content"):
                    msg = dict(msg, text=msg["content"])
                if isinstance(msg, dict) and msg.get("role") and msg.get("text"):
                    # M190: forbid claude→claude consecutive appends. Claude can only post when
                    # (a) conversation is empty (initial comment), or (b) last entry is from user.
                    # Prevents self-reply chains that violate ns-comment-reply-protocol.md Rule 1+2.
                    # M1961: _M190_DISABLED=True bypasses this gate temporarily.
                    if msg.get("role") == "claude" and not _M190_DISABLED:
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
                        # M1709: Group-A hard-reject for reply length — mirrors the M780.2
                        # pattern exactly (422 + retry, not a schema hint). Threshold is 3
                        # lines, matching the convention already documented everywhere else in
                        # this file (M172, M780.3, M270 TOKEN DISCIPLINE — all say "≤3-line");
                        # the reply_to_stone tool schema's stricter "max 1 line" is description
                        # text only and was proven unenforced this session (long prose replies
                        # went through anyway, M1709/M110 screenshot). Deliberately scoped to a
                        # MECHANICALLY verifiable rule (raw newline count) — semantic rules
                        # ("is this a real direct answer") stay unenforced by design (M1706:
                        # Group B, would need a second LLM judge call, not worth the FP rate).
                        # allow_long_reply:true overrides for legitimate long analyses.
                        # M1709-c: re-enabled with a marker-aware exception. The pure line-count
                        # gate (M1709-b) was disabled after proving it false-positives on
                        # legitimate well-structured replies (RobotAI spot_voice_pipeline example:
                        # 8 real lines, clean per-item bullets, blocked identically to unstructured
                        # prose since raw newline-count has no concept of bullet/list/table
                        # markers). Root complaint was never "too many lines" — it was unbroken
                        # PROSE walls (M1709 screenshot: one long paragraph, zero structure).
                        # Fix: exempt replies where at least half the non-blank lines carry a
                        # structural marker (bullet -/•/*, numbered 1./①, or a markdown table
                        # row with |) — allows a one-line header/intro before a bulleted body
                        # (the RobotAI example: 1 header + 7 bullets = 7/8 marked, exempted).
                        # A reply that's just N short unmarked lines (plain prose broken by
                        # newlines, no real structure) still gets blocked, matching the actual
                        # complaint (unbroken/unstructured walls, not raw line count).
                        _LINE_GATE_ENABLED = False  # M1709-d: temp disabled per user request (token cost concern)
                        _len_text = str(msg.get("text", ""))
                        _len_lines = [l for l in _len_text.split("\n") if l.strip()]
                        _allow_long_reply = bool(data.get("allow_long_reply"))
                        import re as _re_gate
                        _marker_re = _re_gate.compile(
                            r'^\s*([-•*]|\d+[.)]|[①②③④⑤⑥⑦⑧⑨⑩]|\|.*\|)\s'
                        )
                        _marked_count = sum(1 for l in _len_lines if _marker_re.match(l))
                        _is_structured = len(_len_lines) >= 2 and _marked_count >= max(2, len(_len_lines) // 2)
                        if (_LINE_GATE_ENABLED and len(_len_lines) > 3
                                and not _allow_long_reply and not _is_structured):
                            try:
                                _server_log_action(proj_id, mid, "reply_line_limit_blocked",
                                                   f"{len(_len_lines)} lines")
                            except Exception:
                                pass
                            return JSONResponse({
                                "ok": False,
                                "error": "reply_line_limit_blocked",
                                "line_count": len(_len_lines),
                                "detail": (
                                    f"Reply has {len(_len_lines)} lines of unstructured prose — "
                                    f"max 3 lines unless using bullets/numbered-list/table markers "
                                    f"(M172/M270 convention). Condense or restructure with "
                                    f"-/1./| markers, or set \"allow_long_reply\":true to override "
                                    f"(M1709)."
                                ),
                            }, status_code=422)
                    # M185/M1064 v2: line-summary — extract key lines, skip blanks
                    # M1161 v3: first-line bias raised 1→8 so the direct answer (line 0) survives
                    # compression. Faithfulness gate ensures line 0 is always kept when no Q-token
                    # appears in any chosen line. Last-line bias stays 1 (summary/next-action).
                    # M1701: disabled provisionally — code-fence unit not grouped atomically
                    # (same bug class M1692 fixed for tables), reproduced live via
                    # test_stone_compression.py. Re-enable (set True) once _group_table_units
                    # is generalized to _group_structural_units (tables + code fences + lists).
                    _STONE_COMPRESSION_ENABLED = False
                    _MAX_LINES = 6 if _STONE_COMPRESSION_ENABLED else 10**9  # M1664: restored 12→6 per user request
                    # M1849: Claude Code MCP sometimes emits literal \n (0x5c 0x6e) in tool
                    # parameter strings instead of real newlines (0x0a). This happens when the
                    # model writes "\n" as a JSON-style escape sequence in its generated text
                    # rather than using an actual newline character. The MCP JSON-RPC layer
                    # preserves the double-escaped form: "\\n" in JSON → "\n" (two chars) in
                    # Python. Normalize these to real newlines before storage/compression so
                    # the chatbox renderer (which converts \n→<br>) can break lines correctly.
                    _text = str(msg.get("text", "")).replace('\\n', '\n')
                    msg["text"] = _text  # M1849: persist normalization before further processing
                    # M1687: the entire line-count gate below is blind to LINE LENGTH — a single
                    # 1000-char sentence with zero \n counts as "1 line" and sails through
                    # untouched, rendering as an unreadable prose wall on mobile (user-reported,
                    # screenshot-confirmed). Force-split any unbroken run longer than
                    # _SOFT_WRAP_CHARS onto natural clause boundaries BEFORE the line-count logic
                    # runs, so a long reply gets real lines to compress/display instead of one blob.
                    # Client renderer already converts \n→<br> (_renderNtTextHtml) — this only
                    # adds the \n that was missing, it doesn't change rendering behavior.
                    _SOFT_WRAP_CHARS = 80
                    if _STONE_COMPRESSION_ENABLED and "\n" not in _text and len(_text) > _SOFT_WRAP_CHARS:
                        import re as _re_wrap
                        # Primary split: sentence/clause-ending punctuation followed by a space —
                        # covers Korean (다./음./함. 등 + —) and English (. ! ? followed by space).
                        _clauses = _re_wrap.split(r'(?<=[.!?다음함音—])\s+(?=\S)', _text)
                        # Secondary: a clause that STILL runs far over budget (long comma-chained
                        # run-on with no sentence-ending punctuation in between — the common case
                        # in dense technical replies) also splits on comma+space.
                        _expanded = []
                        for _cl in _clauses:
                            if len(_cl) > _SOFT_WRAP_CHARS * 1.5:
                                _expanded.extend(_re_wrap.split(r'(?<=,)\s+(?=\S)', _cl))
                            else:
                                _expanded.append(_cl)
                        _wrapped_lines = []
                        _buf = ""
                        for _cl in _expanded:
                            if not _cl:
                                continue
                            if _buf and len(_buf) + 1 + len(_cl) > _SOFT_WRAP_CHARS:
                                _wrapped_lines.append(_buf)
                                _buf = _cl
                            else:
                                _buf = f"{_buf} {_cl}" if _buf else _cl
                        if _buf:
                            _wrapped_lines.append(_buf)
                        # Only apply if splitting actually produced multiple lines — a single
                        # clause longer than _SOFT_WRAP_CHARS with no punctuation at all stays
                        # as-is rather than being cut mid-word.
                        if len(_wrapped_lines) > 1:
                            _text = "\n".join(_wrapped_lines)
                            msg["text"] = _text
                    _all_lines = _text.split("\n")
                    _non_empty = [l for l in _all_lines if l.strip()]
                    # M1692: markdown tables were getting gutted by this compressor — table
                    # rows (e.g. "| /venv | ~8GB |") share almost no word-tokens with the
                    # user's question, so the Q-anchor scorer ranks them lowest and drops
                    # them first, leaving an orphaned header+separator with no data rows
                    # (observed live: a real capacity-estimate table survived as
                    # "| 항목 | 크기 |\n|------|------|" with every data row deleted —
                    # worse than no table, since it now reads as broken rather than short).
                    # Fix: group contiguous "|"-prefixed lines into one atomic UNIT up front
                    # (before the length gate, so the gate itself sees post-grouping counts —
                    # otherwise a message that's short in units but long in raw table rows
                    # would wrongly enter the truncation path and hit the overlapping
                    # head+tail fallback slice). A table then counts as ONE candidate against
                    # _MAX_LINES and is kept or dropped as a whole — never split into a
                    # headless/bodyless stub.
                    def _group_table_units(_lines):
                        _units = []
                        _i = 0
                        while _i < len(_lines):
                            if _lines[_i].lstrip().startswith("|"):
                                _j = _i
                                while _j < len(_lines) and _lines[_j].lstrip().startswith("|"):
                                    _j += 1
                                _units.append("\n".join(_lines[_i:_j]))
                                _i = _j
                            else:
                                _units.append(_lines[_i])
                                _i += 1
                        return _units
                    _units = _group_table_units(_non_empty)
                    if len(_units) > _MAX_LINES:
                        _chosen = None
                        # Q-anchored top-N (claude responses only)
                        if msg.get("role") == "claude":
                            _prev_conv = m.get("conversation") or []
                            _q_text = ""
                            for _prev in reversed(_prev_conv):
                                if isinstance(_prev, dict) and _prev.get("role") == "user":
                                    _q_text = str(_prev.get("text", ""))
                                    break
                            if _q_text:
                                import re as _re_qa
                                def _toks(s):
                                    return set(t for t in _re_qa.findall(r"[\w가-힣]+", (s or "").lower()) if len(t) > 1)
                                _q_tokens = _toks(_q_text)
                                if _q_tokens:
                                    _n_units = len(_units)
                                    _scored = []
                                    for _idx, _un in enumerate(_units):
                                        _is_table = _un.lstrip().startswith("|")
                                        _ov = len(_q_tokens & _toks(_un))
                                        # M1161 v3: first-line bias 1→8 so direct answer survives
                                        _bias = (8 if _idx == 0 else 0) + (1 if _idx == _n_units - 1 else 0)
                                        # M1692: small structural bias so a table unit isn't
                                        # automatically last-ranked purely for having 0 word-tokens
                                        # (table cells are labels/numbers, not prose) — still loses
                                        # to any unit with a real Q-token hit, just not to every
                                        # zero-overlap prose line by tie-breaking order.
                                        _bias += 2 if _is_table else 0
                                        _scored.append((_idx, _un, _ov * 10 + _bias))
                                    _top = sorted(_scored, key=lambda x: -x[2])[:_MAX_LINES]
                                    _top_ordered = sorted(_top, key=lambda x: x[0])
                                    _chosen = [t[1] for t in _top_ordered]
                                    # Faithfulness gate: if line 0 was dropped and no Q-token
                                    # in any chosen line, prepend line 0 as direct-answer anchor.
                                    _chosen_set = set(t[0] for t in _top)
                                    _has_q_hit = any(len(_q_tokens & _toks(_un)) > 0 for _un in _chosen)
                                    if 0 not in _chosen_set and not _has_q_hit:
                                        _chosen = [_units[0]] + _chosen[:_MAX_LINES - 1]
                        if _chosen is None:
                            # Fallback: head + tail (preserve flow when no Q-anchor)
                            _chosen = _units[: _MAX_LINES // 2] + _units[-(_MAX_LINES - _MAX_LINES // 2):]
                        msg["text"] = "\n".join(_chosen[:_MAX_LINES])
                        msg["truncated"] = True
                    else:
                        # ≤MAX_LINES — keep as-is but strip pure-blank lines for cleanliness
                        if len(_all_lines) != len(_non_empty):
                            msg["text"] = "\n".join(_non_empty)
                    # M1257: force server time on every append_message so user (browser TZ/CT ISO) and claude (server local) timestamps line up in the chatbox.
                    msg["ts"] = _dt.datetime.now().isoformat()
                    _m1981_skipped_append = False  # set True when Layer A blocks the append
                    # M1986: identical-comment dedup — block duplicate claude comments within 90s.
                    # Unlike M190 (blanket consecutive-claude block), this only rejects when the
                    # incoming text is ≥80% identical to the last claude comment (prefix match on
                    # first 200 chars). Allows legitimate follow-up comments from long-form tasks
                    # while stopping retry storms (Layer A, latency, output_check double-fire).
                    _m1986_conv_pre = m.get("conversation") or []
                    _m1986_dedup_blocked = False
                    if msg.get("role") == "claude" and _m1986_conv_pre:
                        _m1986_last = _m1986_conv_pre[-1]
                        if isinstance(_m1986_last, dict) and _m1986_last.get("role") == "claude":
                            _m1986_last_txt = (str(_m1986_last.get("text") or ""))[:200].strip()
                            _m1986_new_txt = (str(msg.get("text") or ""))[:200].strip()
                            _m1986_last_ts_str = _m1986_last.get("ts") or ""
                            _m1986_age = 9999
                            try:
                                _m1986_age = (_dt.datetime.now() - _dt.datetime.fromisoformat(_m1986_last_ts_str)).total_seconds()
                            except Exception:
                                pass
                            if _m1986_last_txt and _m1986_new_txt and _m1986_age < 90:
                                _m1986_sim = sum(a == b for a, b in zip(_m1986_last_txt, _m1986_new_txt)) / max(len(_m1986_last_txt), len(_m1986_new_txt), 1)
                                if _m1986_sim >= 0.80:
                                    _m1981_skipped_append = True
                                    _m1986_dedup_blocked = True
                                    _server_log_action(proj_id, mid, "warn:comment_dedup_blocked",
                                                       f"M1986: duplicate claude comment within {_m1986_age:.0f}s sim={_m1986_sim:.2f} skipped")
                    # M1981: Layer A pre-flight — do NOT write the comment when this PATCH will be
                    # blocked for missing evidence_url. Without this guard, every LAYER_A_BLOCKED
                    # retry appends a new duplicate comment (observed: FromScratch M181 × 5 dupes).
                    # Condition: completion PATCH (status=pending_confirmation, role=claude) + Layer A
                    # keyword in stone text + no evidence_url in data or current stone.
                    _m1981_is_completion_patch = (
                        data.get("status") == "pending_confirmation"
                        and msg.get("role") == "claude"
                    )
                    if _m1981_is_completion_patch:
                        import re as _re_m1981
                        _m1981_ev = data.get("evidence_url") or m.get("evidence_url") or ""
                        _m1981_txt = _re_m1981.sub(
                            r'\S?PASTE\S?.*?\S?/PASTE\S?', '',
                            (m.get("text") or ""), flags=_re_m1981.S
                        ).lower()
                        _m1981_layer_a_kws = (
                            # M1981: action verbs + verbal nouns → Layer A hard block (mirrors _layer_a_keywords)
                            "구현", "기능", "추가", "수정", "개선", "만들", "작성", "생성",
                            "implement", "create", "build", "design", "develop",
                            "검수", "리서치", "research", "정리", "분석", "analysis",
                        )
                        _m1981_will_block = (
                            any(kw in _m1981_txt for kw in _m1981_layer_a_kws)
                            and not _m1981_ev
                        )
                        if _m1981_will_block:
                            # Skip conv append — the comment will be written on the successful
                            # retry that includes evidence_url. Signal to downstream skip-blocks via
                            # _m1981_skipped_append so log/blink side effects are also suppressed.
                            msg.pop("ts", None)  # discard the timestamped version
                            _m1981_skipped_append = True
                        else:
                            conv = m.get("conversation") or []
                            conv.append(msg)
                            m["conversation"] = conv
                    else:
                        conv = m.get("conversation") or []
                        conv.append(msg)
                        m["conversation"] = conv
                    # M1154 v4: auto-compression DISABLED — user requested full conversation
                    # history in the chatbox. The destructive squash was losing original turns
                    # which violated UI contract. LLM context grows accordingly (acceptable
                    # trade-off per user). Leaving the code path in place for legacy stones
                    # (existing role:'summary' entries still render via badge), guarded by False.
                    _AUTO_KEEP_LAST = 4
                    _AUTO_THRESHOLD = 5
                    if False and len(conv) > _AUTO_THRESHOLD:
                        # Treat existing role:'summary' (if at index 0) as already-compressed older history.
                        _old_summary = None
                        if conv and isinstance(conv[0], dict) and conv[0].get("role") == "summary":
                            _old_summary = conv[0]
                            _raw_conv = conv[1:]
                        else:
                            _raw_conv = conv[:]
                        # Compress all but the last KEEP_LAST raw turns
                        if len(_raw_conv) > _AUTO_KEEP_LAST:
                            _to_squash = _raw_conv[:-_AUTO_KEEP_LAST]
                            _keep = _raw_conv[-_AUTO_KEEP_LAST:]
                            _trim = lambda t: (str(t or "").replace("\n", " ").strip())[:150]
                            _u_turns = [c for c in _to_squash if isinstance(c, dict) and c.get("role") == "user"]
                            _c_turns = [c for c in _to_squash if isinstance(c, dict) and c.get("role") == "claude"]
                            _sum_lines = []
                            if _u_turns: _sum_lines.append("U: " + _trim(_u_turns[0].get("text") or _u_turns[0].get("content", "")))
                            if len(_u_turns) > 1: _sum_lines.append("Un: " + _trim(_u_turns[-1].get("text") or _u_turns[-1].get("content", "")))
                            if _c_turns: _sum_lines.append("C: " + _trim(_c_turns[-1].get("text") or _c_turns[-1].get("content", "")))
                            _new_summary_text = " | ".join(_sum_lines) if _sum_lines else f"({len(_to_squash)} turns compressed)"
                            _new_compressed_count = len(_to_squash) + (
                                int(_old_summary.get("compressed_count", 0)) if _old_summary else 0
                            )
                            if _old_summary:
                                # Merge: keep old summary text up front, append fresh squash digest
                                _merged_text = (str(_old_summary.get("text", "")).strip() + " ‖ " + _new_summary_text).strip(" ‖")
                                _summary_entry = {
                                    "role": "summary",
                                    "text": _merged_text,
                                    "ts": _to_squash[-1].get("ts", _old_summary.get("ts", "")),
                                    "compressed_count": _new_compressed_count,
                                }
                            else:
                                _summary_entry = {
                                    "role": "summary",
                                    "text": _new_summary_text,
                                    "ts": _to_squash[-1].get("ts", ""),
                                    "compressed_count": _new_compressed_count,
                                }
                            m["conversation"] = [_summary_entry] + _keep
                            conv = m["conversation"]
                            _server_log_action(proj_id, mid, "auto_compress_conv",
                                               f"squashed={len(_to_squash)} kept={len(_keep)} total_compressed={_new_compressed_count}")
                    # M1050/M1060: verify_flag resets on every chat message (user or claude)
                    if m.get("verify_flag"):
                        m["verify_flag"] = False
                    # M535: log server-side comment event (M1981: skip when Layer A pre-flight blocked)
                    if not _m1981_skipped_append:
                        _server_log_action(proj_id, mid, f"comment:{msg.get('role','?')}",
                                           (msg.get("text",""))[:120])
                        # M1227: any comment (user or claude) on a stone → blink so participant sees new reply
                        if msg.get("role") in ("user", "claude"):
                            try:
                                _comment_skey = m.get("substar_id") or "__ungrouped__"
                                _mark_blink_server(proj_id, _comment_skey, m.get("id") or mid)
                            except Exception:
                                pass
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
                            # M1612: if stone was watching when re-queued, clear watching on completion.
                            if m.get("watching"):
                                m["watching"] = False
                            # M1253: schedule conversation-summary task if conversation grew too long.
                            _maybe_queue_compress(proj_id, m, milestones)
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
                                    _m722_rev_text = _build_review_stone_text(mid, str(m.get("text",""))[:80], _stone_completion_summary(m))
                                    milestones.append({
                                        "id": _m722_rev_id,
                                        "text": _m722_rev_text,
                                        "layer": _m722_layer,
                                        "parent_id": mid,
                                        "done": False,
                                        "status": "queued",
                                        "claude_ack": None,
                                        "user_added_at": _dt.datetime.now().strftime("%Y-%m-%dT%H:%M"),
                                        "skill_refs": ["e2e"],  # M996
                                        "substar_id": m.get("substar_id") or None,  # M1945: inherit parent substar
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
                    # M1253: on any append_message, immediately check compress threshold (no 30-min wait).
                    # Only queue a [compress] child when conv_len >= 10 (same logic as _compress_trigger_reason).
                    # M1621: removed layer==0 gate — child stones (layer=1) also need compress.
                    if not m.get("held") and not m.get("done"):
                        _maybe_queue_compress(proj_id, m, milestones)
                    # M225: skip REPLY SYNC dispatch if stone is held (zero claude token spend on held).
                    if msg.get("role") == "user" and not m.get("done") and not m.get("held"):
                        # M131-b: resolve the session that OWNS this stone's substar — was
                        # _live_exec_session_name(proj_id), which always returns the MAIN
                        # session first regardless of ownership (see _exec_session_names:
                        # main is index 0, children appended after). That woke the mother on
                        # every user reply to a CHILD-owned stone (e.g. FromScratch/cg_vla),
                        # even though /execute's own branched-split logic correctly resolves
                        # ownership via substar.assigned_session. Mirror that resolution here.
                        _sid_own = (m.get("substar_id") or "").strip()
                        _owner_sess = ""
                        if _sid_own:
                            for _ns_own in (proj.get("north_stars") or []):
                                if isinstance(_ns_own, dict) and _ns_own.get("id") == _sid_own:
                                    _owner_sess = (_ns_own.get("assigned_session") or "").strip()
                                    break
                        # M1912-b: _sid_own set but _owner_sess empty = substar explicitly unassigned
                        # (same M1860 principle: don't wake arbitrary sessions for unassigned substar stones).
                        # Only fall back to _live_exec_session_name for ungrouped stones (no substar_id),
                        # which are main-session domain regardless of which session happens to be alive.
                        if _owner_sess:
                            session_name = _owner_sess
                        elif not _sid_own:
                            session_name = _live_exec_session_name(proj_id)
                        else:
                            session_name = ""  # unassigned substar — no wake target
                        check = await asyncio.to_thread(  # M1345
                            subprocess.run, ["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
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
                                # M1869-P2: quiet-mode REPLY SYNC — stripped static 8-rule protocol
                                # (_qa_instruction in get_pending_task covers M687/M190/M1781 rules)
                                f"[REPLY SYNC] Stone {mid} has a user comment. Call mcp__ns-hub__get_pending_task() now."
                                + (f"\n{_skill_annotation.strip()}" if _skill_parts else "")
                            ),
                        }, ensure_ascii=False)
                        with _qf.open("a", encoding="utf-8") as _qh:
                            _qh.write(_entry + "\n")
                        # M1579 Phase 1: OOB-based idle check (replaces ❯-prompt pane-scrape).
                        # OOB is harness-agnostic — any agent type posting /api/agent-busy works.
                        # M1615: user-initiated chatbox reply = highest priority trigger.
                        # Bypass _wake_inflight guard (poller dedup) and force dedup reset so
                        # 90s cooldown never blocks a user comment wake.
                        if check.returncode == 0:
                            try:
                                # M1656-fix (option②): check this specific session's own reported state.
                                # M1806: session-scoped variant — no unbounded sibling-busy inheritance
                                # for a cold-start session with no record of its own.
                                _oob_idle = not _oob_is_busy_session_scoped(proj_id, session_name)
                                if _oob_idle:
                                    if _send_exec_wake(session_name, proj_id, force_dedup_reset=True,
                                                       _force_viewer_bypass=True):  # M1765: user comment wake
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
                    # M1702-b: record how this stone became queued. "badge" = user clicked the
                    # queue-toggle button (client sends queue_source:"badge" — see northstar.html
                    # toggleQueueStatus). The fork-protocol poller (below, _find_unassigned_substars
                    # region / ~L4631) reads this to decide whether "queued, no session" should
                    # auto-spawn a session or wait for the explicit dispatch button.
                    m["queue_source"] = data.get("queue_source") or ""
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
                                "text": _build_review_stone_text(mid, str(m.get("text",""))[:80], _stone_completion_summary(m)),
                                "layer": (m.get("layer", 0) or 0) + 1,
                                "parent_id": mid,
                                "done": False,
                                "status": "queued",
                                "claude_ack": None,
                                "user_added_at": now_iso,
                                "skill_refs": ["e2e"],
                                "substar_id": m.get("substar_id") or None,  # M1945: inherit parent substar
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
                # M1253 auto-confirm: compress meta-stones need no user review — mark done immediately
                if str(m.get("category") or "").startswith("meta/compress"):
                    m["status"] = "done"
                    m["done"] = True
                    m["done_at"] = now_iso
                    m.setdefault("completion_status", "success")
                    m.setdefault("claude_ack", now_iso)
                    _server_log_action(proj_id, mid, "auto_done:compress",
                                       "meta/compress child auto-confirmed on pending_confirmation")
                    # M1269: update parent summary_state so compress doesn't re-trigger immediately.
                    # Without this, parent conv_len still > last_compressed_len → infinite child creation.
                    _par_id = m.get("parent_id")
                    if _par_id:
                        _par = next((x for x in milestones if isinstance(x, dict) and x.get("id") == _par_id), None)
                        if _par is None:
                            _par = _db_get_milestone(proj_id, _par_id)
                        if _par:
                            _par_conv_len = len(_par.get("conversation") or [])
                            _par["summary_state"] = {
                                **((_par.get("summary_state") or {})),
                                "last_compressed_len": _par_conv_len,
                                "last_compressed_at": now_iso,
                            }
                            _server_log_action(proj_id, _par_id, "compress_state_updated",
                                               f"summary_state.last_compressed_len={_par_conv_len} (M1269 auto-done guard)")
                if not str(m.get("category") or "").startswith("meta/compress"):
                    # Stop hook: waiting for user to confirm within 24h
                    m["status"] = "pending_confirmation"
                    m["done"] = False
                    m["pending_confirm_at"] = now_iso  # always update for _isLastTurn green border
                    # M1612: clear watching badge on task completion (watching was set while reviewing, now done)
                    if m.get("watching"):
                        m["watching"] = False
                    # M1047/M1145: evidence_url validation — warn when result-producing work has no proof attached
                    _ev_url = data.get("evidence_url") or m.get("evidence_url") or ""
                    # Strip PASTE attachment paths before keyword check — filenames like
                    # "스크린샷_....png" inside PASTE/.../PASTE blocks would falsely trigger.
                    # M1709: the client wraps the sentinel in PUA chars (_PASTE_OPEN/_CLOSE =
                    # 'PASTE' / '/PASTE', northstar.html) but this regex
                    # only matched the bare 'PASTE/...  /PASTE' string — the PUA wrapper broke the
                    # literal 'PASTE/' adjacency, so the strip silently no-op'd on every real
                    # attachment and a filename like "스크린샷_*.png" leaked straight through into
                    # the Layer A keyword check. Verified live: M1709's own stone text triggered
                    # a false LAYER_A_BLOCKED for a pure Q&A task with no artifact to upload.
                    # Fixed with a wrapper-agnostic pattern (optional \S either side of the
                    # PASTE/…/PASTE markers, non-greedy body) — strips both the current PUA-
                    # wrapped sentinel and the legacy bare-string sentinel in older stone text.
                    import re as _re_ev
                    _stone_txt = _re_ev.sub(r'\S?PASTE\S?.*?\S?/PASTE\S?', '', (m.get("text") or ""), flags=_re_ev.S).lower()
                    # M1981: Layer A (action verbs + verbal nouns) → hard block
                    # Layer B (artifact/file nouns) → soft warning only
                    _layer_a_keywords = (
                        # Action verbs
                        "구현", "기능", "추가", "수정", "개선", "만들", "작성", "생성",
                        "implement", "create", "build", "design", "develop",
                        # Verbal nouns (동사의 명사형) — imply task completion, not just file reference
                        "검수", "리서치", "research", "정리", "분석", "analysis",
                    )
                    _layer_b_keywords = (
                        # File-type artifact nouns
                        "스크린샷", "screenshot", "excel", "엑셀", "pdf", "docx",
                        "보고서", "report", "chart", "분석결과",
                        # Visual/UI artifact nouns
                        "이미지", "image", "화면", "ui", "visual",
                        # Evidence/document nouns
                        "proof", "table", "증거", "증빙", "문서",
                        # Generic output nouns
                        "결과물", "산출물",
                    )
                    _is_layer_a = any(kw in _stone_txt for kw in _layer_a_keywords)
                    _is_layer_b = not _is_layer_a and any(kw in _stone_txt for kw in _layer_b_keywords)
                    if _is_layer_a and not _ev_url:
                        # Layer A: artifact noun detected — evidence_url REQUIRED (force)
                        m.setdefault("_proof_warning", "LAYER_A:evidence_url missing — artifact stone requires evidence_url; upload via rclone and retry")
                        m["_layer_a_required"] = True
                        _server_log_action(proj_id, mid, "warn:layer_a_evidence_missing",
                                           "Layer A artifact keyword detected — evidence_url required but missing")
                    elif _is_layer_b and not _ev_url:
                        # Layer B (M1277): action verb — soft gate. Set _output_check flag so MCP
                        # surfaces a structured prompt to Claude: "did you produce output? if yes, upload."
                        # This converts passive warning → active confirmation, improving evidence rate.
                        m.setdefault("_proof_warning", "evidence_url missing — result badge will not be generated; add screenshot/link via PATCH evidence_url")
                        m["_output_check"] = True
                        _server_log_action(proj_id, mid, "warn:layer_b_evidence_missing",
                                           "Layer B action keyword — evidence_url missing, output_check flagged")
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
                        _fallback_text = f"완료. ({_stone_text})" if _stone_text else "완료."
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
                # M929: if exec_end supplied but exec_start missing, fall back to queued_at so token computation can run
                if (data.get("exec_end") or m.get("exec_end")) and not (m.get("exec_start") or data.get("exec_start")):
                    _fallback_start = m.get("queued_at") or m.get("created_at")
                    if _fallback_start:
                        m["exec_start"] = _fallback_start
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
                # M929: stone_complete telemetry — after token computation so model_used/tokens are populated
                _record_usage_event("stone_complete", {
                    "proj_id": proj_id,
                    "stone_id": mid,
                    "model_used": m.get("model_used") or None,
                    "total_tokens": m.get("total_tokens") or None,
                    "input_tokens": m.get("input_tokens") or None,
                    "output_tokens": m.get("output_tokens") or None,
                    "cost_usd": m.get("cost_usd") or None,
                    "exec_start": m.get("exec_start") or None,
                    "exec_end": m.get("exec_end") or None,
                })
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
                _restore_status = data.get("status")
                if _restore_status == "pending_confirmation":
                    # M1769: explicit restore-to-pc; preserve pending_confirm_at if present
                    m["status"] = "pending_confirmation"
                    if "pending_confirm_at" in data:
                        m["pending_confirm_at"] = data["pending_confirm_at"]
                    elif not m.get("pending_confirm_at"):
                        m["pending_confirm_at"] = now_iso
                elif m.get("status") != "queued":
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
                "text": _build_review_stone_text(mid, _brief, _stone_completion_summary(updated_m)),
                "layer": _m_layer + 1, "parent_id": mid, "done": False,
                "status": "queued", "claude_ack": None,
                "user_added_at": _dt.datetime.now().strftime("%Y-%m-%dT%H:%M"),
                "skill_refs": ["e2e"],  # M996
                "substar_id": updated_m.get("substar_id") or None,  # M1945: inherit parent substar
            })
            _server_log_action(proj_id, _rev_id, "auto_review_created", f"review sub-stone for {mid}")
        # M749: e2e auto-creation removed — was creating pairs ([검수]+[e2e]) per completion
    proj["milestones"] = milestones
    # M1007 v2 / M1213: server-side blink trigger — fire ONLY on status transitions.
    # Removed `or ("append_message" in data)`: exec sessions append messages every ~60s
    # which refreshed blink_state ts continuously, causing stones like M1016 to blink
    # indefinitely despite being unchanged in status for days. Client hash is now
    # status-only (M1213), server aligns: blink = status change only.
    # M1216: done/skipped transitions REMOVE the mid from blink_state (not add) so
    # section bars don't blink indefinitely after all stones are completed.
    try:
        _BLINK_DONE_ST = {"done", "skipped"}  # M1241: pending_confirmation → blink ON (user needs to review); M1222 was wrong
        _new_status = updated_m.get("status") if updated_m else None
        _blink_changed = (updated_m and _new_status != _blink_old_status)
        if updated_m and _blink_changed:
            _blink_skey = updated_m.get("substar_id")
            # M1588: child stones (layer>0) inherit parent's substar_id for correct section blink
            if not _blink_skey and (updated_m.get("layer") or 0) > 0 and updated_m.get("parent_id"):
                _par = _db_get_milestone(proj_id, updated_m["parent_id"])
                _blink_skey = (_par.get("substar_id") if _par else None)
            _blink_skey = _blink_skey or "__ungrouped__"
            _mid_id = updated_m.get("id") or mid
            _mark_blink_server(proj_id, _blink_skey, _mid_id, remove=(_new_status in _BLINK_DONE_ST))
    except Exception:
        pass
    # M288: save synchronously when queuing so /execute sees the updated status immediately
    # (background save causes race: execute reads stale YAML where stone is still 'pending').
    _queuing_now = new_status == "queued" if new_status else False
    if _queuing_now:
        _save_project(proj_id, proj)
        # M1533 v6: new stone queued → if agent is idle, fire immediate wake (no 10s poll wait)
        # M131-b: resolve the OWNING session for this stone's substar first — was always
        # targeting {agent}-exec-{proj_id} (main only; the loop's `== proj_id` match never
        # matches a branched child name like claude-exec-FromScratch-1d987699), so a user
        # reply that reopens a CHILD-owned stone always woke the MOTHER instead, regardless
        # of ownership. Mirrors /execute's _owner_session resolution.
        _sid_qd = (updated_m.get("substar_id") or "").strip() if updated_m else ""
        # M1916-b: session_override on the stone takes priority over substar.assigned_session —
        # same chain as _stone_session() and _session_claimable_queued_count(). Without this,
        # the instant wake here targeted the SUBSTAR session (ignoring override), waking the
        # wrong session immediately; the poller's _session_claimable_queued_count (which DOES
        # check override) then also woke the OVERRIDE session next cycle → both sessions woken.
        _owner_sess_qd = (updated_m.get("session_override") or "").strip() if updated_m else ""
        if not _owner_sess_qd and _sid_qd:
            for _ns_qd in (proj.get("north_stars") or []):
                if isinstance(_ns_qd, dict) and _ns_qd.get("id") == _sid_qd:
                    _owner_sess_qd = (_ns_qd.get("assigned_session") or "").strip()
                    break
        # M1806: session-scoped variant when a specific owner is known (no unbounded
        # sibling-busy inheritance); unassigned substar (_owner_sess_qd empty) keeps the
        # original project-wide aggregation via _oob_is_busy(proj_id) — that fallback is
        # intentional here (no specific session to scope to), unlike the two call sites
        # above that had a real session_name being defeated by the sibling-scan fallback.
        _agent_idle = (not _oob_is_busy_session_scoped(proj_id, _owner_sess_qd)) if _owner_sess_qd \
            else (not _oob_is_busy(proj_id))
        if _agent_idle:
            # M1633-fix: reopen path (prev status was non-queued, e.g. pending_confirmation→queued
            # via user comment) must reset dedup so the stone is dispatched immediately even if a
            # wake was sent within the last 90s. Pure new-stone-queue keeps force_dedup_reset=False
            # to avoid rapid re-injection (M1585 v5).
            _is_reopen = (_blink_old_status or "") not in ("queued", "")
            def _queued_dispatch(_owner=_owner_sess_qd):
                try:
                    if _owner:
                        if _tmux_session_alive(_owner):
                            _send_exec_wake(_owner, proj_id, force_dedup_reset=_is_reopen)
                    # M1860: no fallback wake for unassigned substar — only wake the assigned session.
                    # Previously woke the first alive session for the project when _owner was empty,
                    # causing any idle session to pick up unassigned stones (free-pool race).
                except Exception:
                    pass
            import threading as _thr
            _thr.Thread(target=_queued_dispatch, daemon=True).start()
    else:
        # M698/M1234: single-stone update (~1ms); skip full background rewrite (was 100~244ms for 1330 stones)
        _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
        if updated_m:
            _db_save_single_milestone(proj_id, updated_m)
            # M1234: no background _save_project — single-stone write is sufficient for PATCH
        else:
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
    if updated_m and updated_m.pop("_layer_a_required", False):
        _resp["requires_evidence"] = True  # M1265: Layer A force — MCP surfaces this to Claude
    if updated_m and updated_m.pop("_output_check", False):
        _resp["output_check"] = True  # M1277: Layer B soft gate — MCP prompts Claude to confirm output existence
    return JSONResponse(_resp)


@app.delete("/api/northstar/{proj_id}/milestones/{mid}")
async def delete_milestone(proj_id: str, mid: str, background_tasks: BackgroundTasks):
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
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
    _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
    import copy as _c278d
    background_tasks.add_task(_save_project, proj_id, _c278d.deepcopy(proj))
    # M1219: remove deleted stone from blink_state so stale blink doesn't persist
    if removed > 0 and deleted_stone:
        try:
            _blink_skey_del = (deleted_stone.get("substar_id") or "__ungrouped__")
            _mark_blink_server(proj_id, _blink_skey_del, mid, remove=True)
        except Exception:
            pass
    return JSONResponse({"ok": True, "removed": removed})






@app.post("/api/northstar/{proj_id}/milestones/{mid}/move-to-project")
async def move_milestone_to_project(proj_id: str, mid: str, request: Request, background_tasks: BackgroundTasks):
    """M1226: Move a stone from proj_id to target_proj_id. Assigns new ID in target, preserves content + conversation."""
    data = await request.json()
    target_proj_id = (data.get("target_proj_id") or "").strip()
    if not target_proj_id:
        return JSONResponse({"ok": False, "error": "target_proj_id required"}, status_code=400)
    if target_proj_id == proj_id:
        return JSONResponse({"ok": False, "error": "source and target project are the same"}, status_code=400)

    # Verify both projects exist
    src_dir = PROJECTS_DIR / proj_id
    tgt_dir = PROJECTS_DIR / target_proj_id
    if not src_dir.is_dir():
        return JSONResponse({"ok": False, "error": f"source project {proj_id} not found"}, status_code=404)
    if not tgt_dir.is_dir():
        return JSONResponse({"ok": False, "error": f"target project {target_proj_id} not found"}, status_code=404)

    # Load source project, find stone
    src_proj = _db_load_project(proj_id) or {}
    src_ms = src_proj.get("milestones", [])
    stone = next((m for m in src_ms if isinstance(m, dict) and m.get("id") == mid), None)
    if not stone:
        return JSONResponse({"ok": False, "error": f"stone {mid} not found in {proj_id}"}, status_code=404)

    # Load target project, generate new ID
    tgt_proj = _db_load_project(target_proj_id) or {}
    tgt_ms = tgt_proj.get("milestones", [])
    existing_ids = {m.get("id", "") for m in tgt_ms if isinstance(m, dict)}
    nums = [int(m.get("id", "M0")[1:]) for m in tgt_ms
            if isinstance(m, dict) and str(m.get("id", "")).startswith("M")
            and str(m.get("id", ""))[1:].isdigit()]
    n = (max(nums) if nums else 0) + 1
    new_id = f"M{n}"
    while new_id in existing_ids:
        n += 1
        new_id = f"M{n}"

    # Build moved stone — copy all fields, assign new ID, record moved_from
    import copy as _cp_move
    moved = _cp_move.deepcopy(stone)
    moved["id"] = new_id
    moved["moved_from"] = f"{proj_id}/{mid}"
    moved["substar_id"] = None  # ungrouped in target — user can reassign
    moved["layer"] = 0
    moved["parent_id"] = None

    # Insert at top of target project
    tgt_ms.insert(0, moved)
    tgt_proj["milestones"] = tgt_ms
    _db_save_project(target_proj_id, tgt_proj)
    import copy as _cp_move2
    background_tasks.add_task(_save_project, target_proj_id, _cp_move2.deepcopy(tgt_proj))

    # Remove from source project (and its children)
    src_proj["milestones"] = [m for m in src_ms
                               if isinstance(m, dict) and m.get("id") != mid
                               and m.get("parent_id") != mid]
    _db_save_project(proj_id, src_proj)
    _parse_cache.pop(str(src_dir / "north-star.md"), None)
    import copy as _cp_move3
    background_tasks.add_task(_save_project, proj_id, _cp_move3.deepcopy(src_proj))

    # Remove stale blink entry from source
    try:
        _blink_skey_mv = stone.get("substar_id") or "__ungrouped__"
        _mark_blink_server(proj_id, _blink_skey_mv, mid, remove=True)
    except Exception:
        pass

    _server_log_action(proj_id, mid, "stone_move",
                       f"moved to {target_proj_id} as {new_id}")
    return JSONResponse({"ok": True, "new_id": new_id, "target_proj_id": target_proj_id})


@app.post("/api/northstar/{proj_id}/milestones/{mid}/compress-conv")
async def compress_milestone_conv(proj_id: str, mid: str, request: Request):
    """M819: Compress conversation[] — keep last N turns + inject {role:'summary'} entry for older turns.
    M819 fix: uses _parse_md_frontmatter + _save_project (same path as PATCH) so result survives next PATCH."""
    data = await request.json()
    keep_last = max(1, int(data.get("keep_last", 4)))
    try:
        # M1368: SQLite-first
        async with _get_proj_lock(proj_id):
            proj = _db_load_project(proj_id)
            if not proj:
                return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
            milestones = proj.get("milestones", [])
            m = next((x for x in milestones if isinstance(x, dict) and x.get("id") == mid), None)
            if not m:
                return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
            conv = m.get("conversation") or []
            if len(conv) <= keep_last:
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
            _save_project(proj_id, proj)  # saves to YAML + SQLite — survives subsequent PATCH
        return JSONResponse({"ok": True, "summary_text": summary_text, "kept": keep_last, "compressed": len(old_turns)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/northstar/{proj_id}/milestones/{mid}/rationale")
async def milestone_rationale(proj_id: str, mid: str):
    """M65/M70: Generate star-stone relation and OKR rationale for a milestone."""
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    ms = next((m for m in proj.get("milestones", []) if isinstance(m, dict) and m.get("id") == mid), None)
    if not ms:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    ns_metric = proj.get("metric", "the north star goal")
    ns_current = str(proj.get("current", "") or "")
    ns_target  = str(proj.get("target", "") or "")
    ms_text    = str(ms.get("text", ""))
    gap_str = f" ({ns_current} → {ns_target})" if ns_current and ns_target else ""
    rationale = f"Closes the {ns_metric}{gap_str} gap by: {ms_text}"
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "rationale": rationale})




@app.post("/api/northstar/{proj_id}/milestones/{mid}/commit")
async def commit_milestone(proj_id: str, mid: str):
    """M424: git commit in project dir with milestone ID and text as commit message."""
    proj_dir = _get_project_dir(proj_id)
    if not proj_dir:
        return JSONResponse({"ok": False, "error": "project dir not found"}, status_code=404)
    # M1383: resolve actual folder-case so md_path and DB lookup use canonical proj_id
    _actual_folder = Path(proj_dir).name
    if _actual_folder.lower() == proj_id.lower():
        proj_id = _actual_folder
    # M1368: SQLite-first
    proj = _db_load_project(proj_id) or {}
    milestones = proj.get("milestones", [])
    m = next((x for x in milestones if isinstance(x, dict) and x.get("id") == mid), None)
    if not m:
        # Try DB fallback — first exact, then case-insensitive
        m = _db_get_milestone(proj_id, mid)
        if not m:
            try:
                conn = sqlite3.connect(str(_NS_EVENTS_DB))
                row = conn.execute(
                    "SELECT data_json FROM milestones_store WHERE lower(proj_id)=lower(?) AND stone_id=?",
                    (proj_id, mid)
                ).fetchone()
                conn.close()
                if row:
                    m = json.loads(row[0])
            except Exception:
                pass
        m = m or {}
    if not m:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    text_short = str(m.get("text", "")).strip()[:72].replace('"', "'").replace('\n', ' ')
    msg = f"feat: {mid} {text_short}"
    try:
        # Check if git repo exists
        check = subprocess.run(["git", "rev-parse", "--git-dir"], cwd=proj_dir, capture_output=True, timeout=5)
        if check.returncode != 0:
            subprocess.run(["git", "init"], cwd=proj_dir, capture_output=True, timeout=10)
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
        r3 = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=proj_dir, capture_output=True, text=True, timeout=5)
        if r3.returncode == 0:
            sha = r3.stdout.strip()
        # M1267: persist SHA to milestone_commits so GET /commits returns history
        if sha:
            try:
                import sqlite3 as _sq3, datetime as _dt
                _db = _sq3.connect(str(_NS_EVENTS_DB), timeout=5)
                _db.execute(
                    "CREATE TABLE IF NOT EXISTS milestone_commits "
                    "(proj_id TEXT, mid TEXT, sha TEXT, subject TEXT, ts TEXT)"
                )
                _db.execute(
                    "INSERT INTO milestone_commits (proj_id, mid, sha, subject, ts) VALUES (?,?,?,?,?)",
                    (proj_id, mid, sha, msg, _dt.datetime.utcnow().isoformat())
                )
                _db.commit(); _db.close()
            except Exception:
                pass  # non-blocking — commit already succeeded
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
    check = await asyncio.to_thread(
        subprocess.run, ["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
    if check.returncode != 0:
        return JSONResponse({"ok": False, "running": False, "output": ""})
    # M1763: mark this session as actively watched via HTTP poll — _send_exec_wake checks
    # this registry alongside tmux list-clients so wake injection is suppressed either way.
    # M1765: only set viewer gate when user has the full terminal panel open (explicit
    # tmux_session param). The ?lines=15 call (no tmux_session) is the NS-card API-error
    # health check — background poll, not active viewing. Treating it as "watching" falsely
    # blocked wakes when the user wasn't at the terminal.
    if tmux_session.strip():
        _tmux_output_viewers[session_name] = time.time()
    # Capture pane output — always request 500 lines from tmux scrollback
    result = await asyncio.to_thread(
        subprocess.run,
        ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-500"],
        capture_output=True, text=True, timeout=2
    )
    raw_capture = result.stdout.rstrip("\n")
    # M1836-v2: maintain a server-side ring buffer that survives ESC[3J scrollback wipes.
    output = raw_capture
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
    # M1579 Phase 2: running milestone identified via _session_running_stone (OOB, harness-agnostic).
    # Populated by get_pending_task MCP call → POST /api/agent-busy with stone_id.
    # Fallback to first-queued heuristic when OOB has no running stone.
    session_name = _live_exec_session_name(proj_id)
    check = await asyncio.to_thread(
        subprocess.run, ["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
    if check.returncode == 0:
        # M1368: SQLite-first
        proj = _db_load_project(proj_id)
        if proj:
            # M160: paused stones (awaiting user reply on Claude comment) excluded from running list
            # M216: held stones (user-paused via hold badge) also excluded
            queued_ms = [m for m in (proj.get("milestones") or [])
                         if m.get("status") == "queued" and not _awaits_user_reply(m) and not m.get("held")]

            # M1579 Phase 2: OOB-based running stone detection (replaces Claude Code pane-scrape).
            # get_pending_task MCP call POSTs stone_id to /api/agent-busy → _session_running_stone.
            # Harness-agnostic: works for any agent type (Claude Code, codex, hermes, etc).
            live_running_id = _first_running_stone(proj_id)  # M1656-R: derived from session records

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

            # M1579: if the OOB running stone_id isn't currently in the queue (e.g. just
            # promoted to pending_confirmation), surface it as a synthetic "running" entry.
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


def _last_worked_session_for_substar(proj_id: str, substar_id: str, all_stones: list) -> tuple[str, str] | None:
    """M1786: when a substar has no assigned_session (fresh, or cleared by a kill), prefer
    re-assigning the session that most recently worked one of its stones over grabbing
    whichever idle session happens to be polling. Returns (session_name, resume_sid) —
    resume_sid is "" when the session is still alive (no explicit --resume needed) or when
    it's dead but has no recoverable transcript. Returns None if no viable candidate is
    found at all — callers must fall back to their existing auto-assign behavior.

    Judged by: the most recent claimed_by_session value across this substar's stones
    (per user direction — not assigned_session itself, which may already be cleared).

    'Resumable' means either:
      - a live tmux session by that exact name still exists, OR
      - .spawn-info-by-session.json has a live_session_id for that name AND the matching
        .jsonl transcript still exists on disk (so a deterministic-name respawn can
        --resume it and pick the conversation back up).
    Does not itself spawn or PATCH anything — pure lookup, side-effect free.
    """
    import re as _re_lws
    # M1828: only consider UUID-suffixed session names (M1796 invariant).
    # Legacy suffix-less names (claude-exec-PROJ) in claimed_by_session are stale pre-M1796
    # records — skip them so the UI doesn't surface a dead/unresumable suffix-less session
    # as "last session" in the sess badge popup.
    _uuid_suffix_pat = _re_lws.compile(r'-exec-[^-]+-[0-9a-f]{8}$')
    candidates: list[tuple[float, str]] = []  # (claimed_at, session_name)
    for m in all_stones:
        if not isinstance(m, dict) or (m.get("substar_id") or "") != substar_id:
            continue
        _cb = (m.get("claimed_by_session") or "").strip()
        if not _cb:
            continue
        if not _uuid_suffix_pat.search(_cb):
            continue  # M1828: skip legacy suffix-less session names
        candidates.append((m.get("claimed_at") or 0, _cb))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    _last_session = candidates[0][1]

    if _tmux_session_alive(_last_session):
        return (_last_session, "")

    _si = _read_spawn_info(proj_id, _last_session)
    _live_sid = _si.get("live_session_id") or _si.get("from_id") or ""
    try:
        _proj_dir = _get_project_dir(proj_id)
        if not _proj_dir:
            return None
        _encoded = _encode_cwd_for_claude(str(_proj_dir))
        _tdir = Path.home() / ".claude" / "projects" / _encoded
        if _live_sid:
            _t = _tdir / f"{_live_sid}.jsonl"
            if _t.exists() and _t.stat().st_size > 0:
                return (_last_session, _live_sid)
        # M1887: spawn-info live_session_id may have been overwritten by a failed re-spawn.
        # Fall back to searching the transcript dir for any .jsonl whose UUID prefix matches
        # the session name's 8-char suffix (session "claude-exec-PROJ-XXXXXXXX" → uuid "XXXXXXXX-...").
        import re as _re_fb
        _suffix_m = _re_fb.search(r'-([0-9a-f]{8})$', _last_session)
        if _suffix_m and _tdir.exists():
            _pfx = _suffix_m.group(1)
            _candidates = sorted(
                [f for f in _tdir.glob("*.jsonl")
                 if f.name.replace("-", "").startswith(_pfx) and f.stat().st_size > 0],
                key=lambda f: f.stat().st_mtime, reverse=True
            )
            if _candidates:
                _fallback_sid = _candidates[0].stem  # UUID without .jsonl
                return (_last_session, _fallback_sid)
    except Exception:
        pass
    return None


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
    # M1096: badge-triggered executes should not spawn a new session when current one is dead.
    # Only the explicit dispatch button may spawn. Badges pass from_badge=true to signal this constraint.
    from_badge = bool(data.get("from_badge"))
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

    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)

    # Apply body_agent and body_model before spawning
    _persist_changes = {}
    if body_agent and body_agent in _ALLOWED_AGENTS:
        _persist_changes["agent"] = body_agent
    if body_model is not None:  # None = not provided; "" = reset to default
        _model_val = body_model if body_model in _ALLOWED_MODELS else ""
        _persist_changes["model"] = _model_val
    if _persist_changes:
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
        _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
        proj = _db_load_project(proj_id) or proj

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

        agent = body_agent if body_agent in _ALLOWED_AGENTS else _get_project_agent_value(proj_id)
        # M1796: every ▶ spawn creates a uniquely-named session using the session UUID as suffix
        # (like fork sessions), eliminating the "main session" special-name concept entirely.
        # - Resume row click: explicit_session_id IS the UUID → suffix = uuid8
        # - Fresh spawn: pre-sign a UUID now so suffix is deterministic before spawn
        # Both cases: session_name = "{agent}-exec-{proj_id}-{uuid8}"
        # NOTE: _presigned_main_sid is also read at L10734+ where the old presign logic lived;
        # that block now reuses this value instead of generating a second UUID.
        import uuid as _uuid_m1796
        _eff_sid = (explicit_session_id or "").strip()
        if _eff_sid and _eff_sid not in ("fresh", "_last", "last"):
            # Resume: suffix from the selected session's UUID
            _presigned_main_sid = _eff_sid
        else:
            # Fresh: generate UUID now (deterministic suffix before any spawning)
            _presigned_main_sid = str(_uuid_m1796.uuid4())
        _main_suffix = _presigned_main_sid.replace("-", "")[:8]
        session_name = f"{agent}-exec-{proj_id}-{_main_suffix}"

        # M1802: auto-assign removed — substars without assigned_session stay unassigned.
        # User must manually assign via the sess popup. This prevents unexpected session routing.

        # M524.3: graceful Windows check — tmux not available on Windows native
        if not _HAS_PTY and sys.platform == "win32":
            return JSONResponse({
                "ok": False,
                "error": "tmux_unavailable_windows",
                "message": "Execute dispatch requires tmux, which is not available on Windows native. "
                           "Run hub inside WSL2 or Docker for full exec session support.",
            }, status_code=501)
        # M359: before checking the new session, check if a DIFFERENT agent's MAIN session
        # exists and kill it when options changed — prevents old sessions surviving agent switch.
        # M858 ORIGINALLY also matched branched sessions of the old agent here (comment used to
        # read "a different-agent dispatch kills ALL old-agent sessions including branches"), but
        # that made ANY cross-agent fork (e.g. the Fork-to-openrouter feature spawning
        # openrouter-exec-Clone-aa2049be as a deliberate, coexisting child) look identical to a
        # leftover from switching away from that agent — so a plain queue action on the CURRENT
        # agent's own healthy main session killed it, just because an unrelated forked child of a
        # different agent happened to be alive (observed live: Clone, 2026-07-09, claude-exec-Clone
        # killed by a queue toggle solely because openrouter-exec-Clone-aa2049be existed).
        # M1569 (M1656-R5) already fixed this exact class of bug for SAME-agent children ("Mother +
        # her own children is the NORMAL state, never stale") — this extends that principle to
        # cross-agent children: only a different agent's MAIN session (never its branches) counts
        # as a stale leftover. A per-substar/session-scoped check would be more precise still, but
        # main-only closes the reported bug without weakening the original agent-switch cleanup.
        _all_exec_prefixes = [("claude-exec-", "claude"), ("openrouter-exec-", "openrouter"), ("codex-exec-", "codex"), ("dsk-exec-", "dsk")]
        try:
            _live_sessions = (await asyncio.to_thread(  # M1345: non-blocking tmux list
                subprocess.run,
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=2,
            )).stdout.splitlines()
        except Exception:
            _live_sessions = []
        _stale_agent_found = False
        for _pfx, _pfx_agent in _all_exec_prefixes:
            if _pfx_agent != agent:
                # Only the OTHER agent's exact MAIN session name counts — never its branches.
                # M1796: sessions now have suffix — match any suffix variant of the other agent's main
                _old_prefix = f"{_pfx}{proj_id}-"
                _old_base_legacy = f"{_pfx}{proj_id}"
                for _ls in _live_sessions:
                    if _ls == _old_base_legacy or _ls.startswith(_old_prefix):
                        # M1797: any session of a different agent is stale (no mother/child distinction)
                        _stale_agent_found = True
                        break
                if _stale_agent_found:
                    break
        # M1797-followup (M1797 bug found in M1797 self-audit): the "peer count > 1 = stale"
        # rule above was wrong — it treated ANY two coexisting sessions for this project as
        # stale, which kills legitimate multi-session assignments (e.g. two substars each
        # assigned their own alive session) on every /execute call. M1656-R5/M1569 already
        # established "two coexisting exec sessions is the NORMAL state, never stale" — that
        # invariant was never mother/child-specific, so removing the mother/child concept in
        # M1797 must not resurrect this false-positive kill. Multiple alive sessions are only
        # stale when a DIFFERENT agent's session is alive (already handled above by
        # _stale_agent_found); same-agent multi-session coexistence is intentional.
        if _stale_agent_found:
            _kill_all_exec_sessions(proj_id)  # M1797: no mother/child distinction — kill all stale sessions
        proj_dir = _get_project_dir(proj_id) or str(Path.home() / "Project" / proj_id)

        # M1096: badge-triggered execute must NOT spawn a dead session — only dispatch button may.
        if from_badge:
            _badge_check = await asyncio.to_thread(  # M1345
                subprocess.run, ["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
            if _badge_check.returncode != 0:
                # Session is dead — badge cannot spawn. Return "no-op" so badge does not hang.
                return JSONResponse({"ok": True, "status": "badge_no_spawn",
                                     "message": "Session dead — use dispatch button to start a new session"})

        # M24/M123/M125: if tmux session exists, verify Claude is alive before injecting
        existing = await asyncio.to_thread(  # M1345
            subprocess.run, ["tmux", "has-session", "-t", f"={session_name}"], capture_output=True, timeout=2)
        if existing.returncode == 0:
            # M123: check Claude process is alive (not just a bare shell after Claude exited)
            _pane_cmds = (await asyncio.to_thread(  # M1345
                subprocess.run,
                ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                capture_output=True, text=True,timeout=2
            )).stdout.splitlines()
            _cmds = [c.strip() for c in _pane_cmds if c.strip()]
            _claude_alive = any(c not in _SHELLS for c in _cmds) if _cmds else False
            if _claude_alive:
                if agent == "codex":
                    # Codex does not use the Claude Stop-hook queue path. Restart the
                    # live tmux session so the execute prompt can be re-injected cleanly.
                    await asyncio.to_thread(  # M1345
                        subprocess.run, ["tmux", "kill-session", "-t", f"={session_name}"], capture_output=True, timeout=5)
                else:
                    # M206: kill and restart if user selected different options (agent/model/session)
                    # vs what the current session was spawned with. If same → reuse.
                    # M1685-j: was reading .last-spawn-info.json (legacy mirror, only updated for
                    # _key=="_main" — stale since M1656-R9 session-scoped keying, see fix at
                    # _update_session_history_from_transcript call site). Use the session-scoped
                    # reader so _cur_model reflects what THIS session actually spawned with.
                    _options_changed = False
                    _si = _read_spawn_info(proj_id, session_name)
                    if _si:
                        try:
                            _cur_agent = _si.get("agent", "claude")
                            _cur_model = _si.get("model", "")
                            # M1796: match against live_session_id (covers both fresh --session-id
                            # and resume --resume cases) rather than from_id alone (from_id is ""
                            # for fresh sessions, causing false _options_changed=True on every
                            # re-dispatch to the currently running fresh session).
                            _cur_sid_live = _si.get("live_session_id") or _si.get("from_id") or ""
                            _new_agent = agent
                            _new_model = _get_project_model_value(proj_id) or ""
                            _new_sid   = explicit_session_id or ""
                            _options_changed = (
                                _cur_agent != _new_agent or
                                _cur_model != _new_model or
                                (explicit_session_id and _cur_sid_live and _cur_sid_live != _new_sid)
                            )
                        except Exception:
                            pass
                    # M1796: explicit_session_id is now ALWAYS provided (every session has a UUID
                    # suffix). Kill+respawn only when options actually changed — not just because
                    # the user clicked ▶ on the currently-running session's row (same uuid,
                    # same model, same agent → inject without killing).
                    if _options_changed:
                        # M206: kill THIS session (options changed) — spare all OTHER alive sessions.
                        # M1797-followup: kill only session_name; any sibling sessions for other
                        # substars are unrelated and must survive this options-changed respawn.
                        _spare_siblings = {s for s in _live_sessions if s != session_name
                                           and f"-exec-{proj_id}" in s}
                        _kill_all_exec_sessions(proj_id, spare=_spare_siblings)
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
                        def _is_assigned_substar(m):
                            _sid = m.get("substar_id") or ""
                            return bool(_sid) and _sid not in _main_assign
                        new_queued  = [m for m in active_ms if m.get("status") == "queued"
                                       and (not _awaits_user_reply(m) or _is_assigned_substar(m))
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
                                return session_name  # ungrouped (no substar) → main
                            _ak = ((_ns_by_id_inj.get(_sid) or {}).get("assigned_session") or "").strip()
                            # M1860: unassigned substar → no owner (not this session).
                            # Previously returned session_name as fallback, routing any
                            # unassigned-substar stone to the current/new session.
                            return _ak if _ak else ""
                        _main_owned_q = [m for m in new_queued if _owner_session(m) == session_name]
                        _assigned_q_by_sess: dict = {}
                        for _m in new_queued:
                            _ok = _owner_session(_m)
                            # M1860: skip unassigned-substar stones (_ok=="") and own-session stones
                            if _ok and _ok != session_name:
                                _assigned_q_by_sess.setdefault(_ok, []).append(_m)
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
                            _ms_snap = "\n".join(_ms_snap_line(m) for m in (_main_owned_q + new_pending))  # main-owned only
                            _waves = _compute_dispatch_waves(_main_owned_q) if _main_owned_q else []
                            _stamp_wave_indices(proj_id, _waves)  # M511: label wave_index on each stone
                            _waves_section = _format_dispatch_waves(_waves) if len(_main_owned_q) > 1 else ""
                            _mem_section = _load_stone_memory(proj_id)
                            _reflection_section = _load_failure_reflections(proj_id)
                            _mcp_cfg_exists = Path(f"/tmp/hub/mcp/{session_name}.json").exists()
                            _common_guidance = (
                                f"PARALLEL DISPATCH PROTOCOL (M472, Option 2 — native sub-agents) —\n"
                                f"  IF 2+ queued stones are INDEPENDENT (no parent-child relation in their\n"
                                f"  parent_id field, no obvious shared-file/shared-resource collision in\n"
                                f"  their text), dispatch them IN PARALLEL: emit multiple Agent / Task tool\n"
                                f"  calls in a SINGLE message. Sub-agents share THIS session's token quota.\n"
                                f"  After all parallel sub-agents return, you (the orchestrator) post ONE\n"
                                f"  consolidated summary listing the completed MIDs (STRUCTURED: no limit; prose: ≤3 lines).\n"
                                f"  FALL BACK TO SEQUENTIAL when: (a) a substone whose mother is also queued\n"
                                f"  (mother must wait — M468 commit-gate), (b) UI work needing the same\n"
                                f"  browser session, (c) stones whose text references the same target file\n"
                                f"  or shared mutable resource.\n\n"
                                f"PROVE SHOT PROTOCOL (M332) — for UI/visual work, include a screenshot/video\n"
                                f"  link in append_message (e.g. GDrive link). Take a Playwright screenshot,\n"
                                f"  upload via rclone to gdrive:claude-shared/Moat/outbox/, share link.\n"
                                f"  Non-visual work (logic fixes, CSS): describe before/after instead.\n\n"
                                f"LANGUAGE RULE (M693) — append_message text must match the stone's language.\n"
                                f"  Korean stone text → Korean comment. English stone text → English comment.\n"
                                f"  Mixed: use the dominant language (usually Korean for this project).\n\n"
                            )
                            if _mcp_cfg_exists:
                                # M1869-P2: quiet-mode EXECUTE SYNC (MCP path).
                                # Static protocol removed — covered by _HUB_EXEC_SYS_PROMPT + get_pending_task().
                                # Dynamic sections retained: reflections, memory, waves, stone snapshot.
                                _dispatch_body = (
                                    f"[EXECUTE SYNC] {len(_main_owned_q + new_pending)} stone(s) queued. "
                                    f"Call mcp__ns-hub__get_pending_task() now.\n\n"
                                    + _reflection_section
                                    + _mem_section
                                    + _waves_section
                                    + (f"Newly queued:\n{_ms_snap}" if _ms_snap else "")
                                )
                            else:
                                # M1869-P2: quiet-mode EXECUTE SYNC (non-MCP/curl path).
                                # Static protocol (MANDATORY FIRST STEP, M687 check, completion protocol) removed.
                                # Retained: reflections, memory, waves, snapshot + the mandatory fetch reminder.
                                _dispatch_body = (
                                    f"[EXECUTE SYNC] {len(_main_owned_q + new_pending)} stone(s) queued.\n"
                                    f"GET {hub_api}/api/northstar/{proj_id}/milestones — read full text + conversation[] before acting.\n\n"
                                    + _reflection_section
                                    + _mem_section
                                    + _waves_section
                                    + (f"Newly queued:\n{_ms_snap}" if _ms_snap else "")
                                )
                            _entry = json.dumps({
                                "ts": _dt_exec.now().isoformat(),
                                "body": _dispatch_body,
                            }, ensure_ascii=False)
                            # M837 fix Stage 1: only write main queue entry + wake main when main owns at least one queued stone
                            _wake_sent = False
                            # M837 fix Stage 1: hoist _modal_signatures so both main + branched wake paths share it
                            _modal_signatures = ("extra usage", "Switch to Team plan",
                                                 "Stop and wait", "rate-limit-options",
                                                 "Press Enter to", "Continue?", "[Y/n]")
                            # M1585: dedup — if REPLY SYNC for same stone was written ≤5s ago, skip EXECUTE SYNC
                            _skip_exec_sync = False
                            if _qf.exists() and (_main_owned_q or new_pending):
                                try:
                                    _all_mid_ids = {m.get("id") for m in (_main_owned_q + new_pending)}
                                    _now_ts = _dt_exec.now().timestamp()
                                    _lines = _qf.read_bytes().decode("utf-8", errors="ignore").splitlines()
                                    for _ql in reversed(_lines[-20:]):
                                        try:
                                            _qe = json.loads(_ql)
                                            _qe_ts = _qe.get("ts", "")
                                            _qe_body = _qe.get("body", "")
                                            if "[REPLY SYNC" not in _qe_body:
                                                continue
                                            _qe_age = _now_ts - _dt_exec.fromisoformat(_qe_ts.replace("Z", "+00:00")).timestamp()
                                            if _qe_age > 5:
                                                break
                                            # Check if the REPLY SYNC references any of our stones
                                            if any(mid in _qe_body for mid in _all_mid_ids if mid):
                                                _skip_exec_sync = True
                                                break
                                        except Exception:
                                            continue
                                except Exception:
                                    pass
                            if (_main_owned_q or new_pending) and not _skip_exec_sync:
                                with _qf.open("a", encoding="utf-8") as _qh:
                                    _qh.write(_entry + "\n")
                                # M1678: busy/attached/alive gates live inside _send_exec_wake
                                # (single choke point) — no outer pre-check needed here.
                                # M131: force_dedup_reset=True — _main_owned_q/new_pending is
                                # confirmed non-empty, so a stale wake_sent hold from an earlier
                                # courtesy boot-wake (found nothing queued, went idle) must not
                                # block this now-genuinely-needed wake.
                                try:
                                    if _send_exec_wake(session_name, proj_id, force_dedup_reset=True,
                                                       _force_viewer_bypass=True):  # M1765: user-submitted stone
                                        _wake_sent = True
                                        _ns_push("session_running", proj_id=proj_id, kind="exec")
                                        _ns_push("milestone_updated", proj_id=proj_id)  # M1649: q-badge sync on dispatch
                                except Exception:
                                    pass
                                # M1656-R: dispatch-gap coverage now lives in _send_exec_wake's
                                # 90s dedup — no pre-set sentinel needed (removed __dispatching__).
                            _trigger_sent = True
                            # M837 fix Stage 1: wake each branched session that owns queued stones.
                            # M858 Stage 2: dead branched sessions → spawn them.
                            _assigned_wakes_sent: list = []
                            _assigned_spawned: list = []
                            _br_spawn_cwd = proj_dir if Path(proj_dir).exists() else str(Path.home())
                            _br_agent = agent
                            _br_model = _get_project_model_value(proj_id)
                            _br_encoded_cwd = _encode_cwd_for_claude(_br_spawn_cwd)
                            _br_transcripts = Path.home() / ".claude" / "projects" / _br_encoded_cwd
                            _br_all_ms = "\n".join(
                                f"  {m.get('id')} [{m.get('status')}]: \"{(m.get('text') or '')[:80].replace(chr(10),' / ')}\""
                                for m in active_ms
                            )
                            for _br_sess, _br_stones in _assigned_q_by_sess.items():
                                try:
                                    _br_check = await asyncio.to_thread(  # M1345
                                        subprocess.run, ["tmux", "has-session", "-t", f"={_br_sess}"], capture_output=True, timeout=2)
                                    if _br_check.returncode != 0 and from_badge:
                                        # M1702: the from_badge no-spawn guard (M1096, line ~9281)
                                        # only checked the MAIN session — this branched-session
                                        # spawn path (M858 Stage 2, added later) had no from_badge
                                        # check at all, so simply queuing a stone whose substar is
                                        # assigned to a dead child session silently spawned that
                                        # child even though the click was just a queue-toggle, not
                                        # the explicit dispatch button. Skip exactly like the main
                                        # session does — badge cannot spawn, only dispatch can.
                                        _server_log_action(proj_id, "", "exec:assigned_respawn_badge_skip", _br_sess)
                                        continue
                                    if _br_check.returncode != 0:
                                        # M858 Stage 2: session dead → write prompt and spawn
                                        # M1809: if _br_sess has no UUID suffix (legacy name pre-M1796),
                                        # generate a new UUID-suffixed name and update assigned_session in DB
                                        # so all future spawns for this substar use the consistent M1796 format.
                                        import uuid as _uuid_br_m1809, re as _re_br_m1809
                                        _br_suffix_pattern = _re_br_m1809.compile(r'-exec-[^-]+-[0-9a-f]{8}$')
                                        if not _br_suffix_pattern.search(_br_sess):
                                            _br_new_uuid = str(_uuid_br_m1809.uuid4()).replace("-", "")[:8]
                                            _br_agent_pfx = agent or "claude"
                                            _br_sess_new = f"{_br_agent_pfx}-exec-{proj_id}-{_br_new_uuid}"
                                            # Update assigned_session in DB for all substars that pointed at the legacy name
                                            try:
                                                _br_proj_obj = _db_load_project(proj_id)
                                                if _br_proj_obj:
                                                    _br_changed = False
                                                    for _br_ns_upd in (_br_proj_obj.get("north_stars") or []):
                                                        if isinstance(_br_ns_upd, dict) and (_br_ns_upd.get("assigned_session") or "").strip() == _br_sess:
                                                            _br_ns_upd["assigned_session"] = _br_sess_new
                                                            _br_changed = True
                                                    if _br_changed:
                                                        _save_project(proj_id, _br_proj_obj)
                                            except Exception:
                                                pass
                                            _server_log_action(proj_id, "", "exec:assigned_respawn_renamed", f"{_br_sess}→{_br_sess_new}")
                                            _br_sess = _br_sess_new
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
                                            "-e", "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80",  # M1656-R8
                                        ]
                                        for _k, _v in _get_project_spawn_env(proj_id).items():
                                            _br_env += ["-e", f"{_k}={_v}"]
                                        # M1656-fork: fork from MOTHER conversation (--resume <sid> --fork-session)
                                        # so the child inherits mother context; falls back to --continue (M837-safe)
                                        _br_resume_args = _mother_fork_args(proj_id, _br_spawn_cwd)
                                        # M1775/M1896: pre-assign --session-id unconditionally (fork OR continue)
                                        # so suffix(_br_sess) == live_session_id at all times.
                                        # M1809 already set _br_sess suffix from a fresh uuid8; we derive
                                        # _br_presigned_sid from that same suffix so the two are in sync
                                        # without regenerating a second random uuid.
                                        import uuid as _uuid_br
                                        import re as _re_br_sid
                                        _br_suffix_m = _re_br_sid.search(r'-([0-9a-f]{8})$', _br_sess)
                                        if _br_suffix_m:
                                            # Expand the 8-hex suffix back to a full UUID via uuid5 deterministic
                                            # mapping is lossy — generate fresh and align suffix instead:
                                            # suffix is already baked into _br_sess; generate presigned with
                                            # matching prefix so [:8] == suffix.
                                            _br_sfx = _br_suffix_m.group(1)
                                            _br_presigned_sid = f"{_br_sfx[:8]}-{str(_uuid_br.uuid4())[9:]}"
                                        else:
                                            _br_presigned_sid = str(_uuid_br.uuid4())
                                        _br_resume_args = [*_br_resume_args, "--session-id", _br_presigned_sid]
                                        # M1656-R9: session-scoped spawn-info record (see assign-spawn twin fix)
                                        _record_spawn_info(proj_id, _br_resume_args, agent="claude", session_name=_br_sess)
                                        _br_tdir = Path.home() / ".claude" / "projects" / _encode_cwd_for_claude(str(_br_spawn_cwd))
                                        _br_pre_files: set = set()
                                        _br_marker = None
                                        if False:  # M1896: presigned_sid always set now — _capture_live path retired
                                            try:
                                                _br_pre_files = {f.name for f in _br_tdir.glob("*.jsonl")}
                                            except Exception:
                                                _br_pre_files = set()
                                            # M1773-C: marker before Popen for ambiguity resolution
                                            _br_marker = _write_spawn_marker(_br_tdir, _br_sess)
                                        _br_claude_cmd = (["claude", "--dangerously-skip-permissions"] + _DISALLOWED_TOOLS_ARGS
                                                          + _hub_mcp_spawn_args(proj_id, _br_sess)
                                                          + _br_resume_args + _get_project_model(proj_id))
                                        subprocess.Popen(
                                            ["tmux", "new-session", "-d", "-s", _br_sess, "-c", _br_spawn_cwd]
                                            + _br_env + _br_claude_cmd
                                        )
                                        if _br_presigned_sid:
                                            _write_sid_direct(proj_id, _br_sess, _br_presigned_sid)
                                        else:
                                            _capture_live_session_id_bg(proj_id, _br_spawn_cwd, _br_sess, _br_pre_files, spawn_marker=_br_marker)
                                        await asyncio.to_thread(subprocess.run, ["tmux", "set-option", "-t", _br_sess, "history-limit", "5000"], capture_output=True, timeout=2)  # M1345
                                        # M1678: readiness-gated unified wake (was blind send-keys during boot — parked text in input box)
                                        threading.Thread(target=_spawn_wake_when_ready,
                                                         args=(_br_sess, proj_id), daemon=True).start()
                                        _assigned_spawned.append(_br_sess)
                                        _server_log_action(proj_id, "", "exec:assigned_respawn_spawned", _br_sess)
                                        continue
                                    # M1678: busy/attached/alive gates live inside _send_exec_wake
                                    # (single choke point) — no outer pre-check needed here.
                                    # M131: force_dedup_reset=True — _br_stones is confirmed
                                    # non-empty for THIS session, so a stale wake_sent hold from
                                    # an earlier courtesy boot-wake (found nothing queued, went
                                    # idle) must not block this now-genuinely-needed wake.
                                    if _send_exec_wake(_br_sess, proj_id, force_dedup_reset=True):
                                        _assigned_wakes_sent.append(_br_sess)
                                except Exception:
                                    pass
                            _server_log_action(proj_id, "", "exec:assigned_respawn_wake",
                                               f"split: main={len(_main_owned_q)} assigned={sum(len(v) for v in _assigned_q_by_sess.values())} woke={','.join(_assigned_wakes_sent) or 'none'} spawned={','.join(_assigned_spawned) or 'none'}")
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
                # M1601: but first check if we're inside the spawn grace window (30s after _record_spawn_info).
                # spawn → pane=shell for ~5-15s before Claude appears. Killing during this window
                # destroys a healthy spawning session. If within grace, inject wake and return instead.
                _spawn_grace_secs = 30
                _within_spawn_grace = False
                _spawn_info_f = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
                if _spawn_info_f.exists():
                    try:
                        _spawn_age = time.time() - _spawn_info_f.stat().st_mtime
                        _within_spawn_grace = _spawn_age < _spawn_grace_secs
                    except Exception:
                        pass
                if _within_spawn_grace:
                    # Session is spawning — don't kill. Inject wake so Claude picks it up on first prompt.
                    _send_exec_wake(session_name, proj_id)
                    return JSONResponse({
                        "ok": True, "mode": "spawn_in_progress",
                        "session": session_name,
                        "message": f"Session spawning (age<{_spawn_grace_secs}s) — wake injected, not killed",
                    })
                await asyncio.to_thread(  # M1345
                    subprocess.run, ["tmux", "kill-session", "-t", f"={session_name}"], capture_output=True, timeout=5)

        # M250: early return if no explicitly-queued stones — skip spawning Claude entirely.
        # pending/needs_clarification stones don't need a Claude session; server already acked them.
        # M858: substar-assigned queued stones with pending user replies are excluded from new_queued_top
        # (_awaits_user_reply=True) but their branched sessions must still be spawned so Claude can
        # reply AND continue implementation. Include them in the gate check.
        # M1788: an explicit session pick (▶ click on a specific resume-list row, M1784) means the
        # user wants THAT session spawned/resumed regardless of queue state — e.g. to keep chatting
        # with it idle. Only the no-target dispatch path (no explicit_session_id) should stay gated
        # on queued work; a human picking a specific row already expressed clear intent to spawn it.
        _substar_queued_top = [
            m for m in active_ms
            if m.get("status") == "queued" and m.get("substar_id")
            and not m.get("held") and str(m.get("text", "")).strip()
        ]
        if not explicit_session_id and not new_queued_top and not _substar_queued_top:
            return JSONResponse({
                "ok": True, "mode": "no_queued_work",
                "tasks_created": 0,
                "message": f"No queued milestones — {len(actionable)} stone(s) are pending/needs_clarification but not yet queued by user. Nothing to dispatch.",
                "pending_count": len([m for m in active_ms if m.get("status") == "pending"]),
                "needs_clarification_count": len([m for m in active_ms if m.get("status") == "needs_clarification"]),
            })

        # M1788: explicit_session_id (▶ click on a specific resume-list row, M1784) must be able
        # to spawn/resume even with zero actionable stones — the human already expressed clear
        # intent to open that session, e.g. to keep chatting with it idle. Without actionable work
        # the per-stone prompt sections below are simply empty/no-op (task_file loop, _ms_snap,
        # etc. all iterate empty lists harmlessly) — the tmux spawn/resume itself is unaffected.
        if actionable or explicit_session_id:
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
                    f"  4. PATCH {hub_api}/api/northstar/{proj_id}/milestones/<MID> body {{\"status\":\"pending_confirmation\", \"model_used\":\"<model name>\", \"session_id\":\"$CLAUDE_CODE_SESSION_ID\", \"exec_start\":\"<ISO start>\", \"exec_end\":\"<ISO now>\", \"append_message\":{{\"role\":\"claude\",\"text\":\"<STRUCTURED(bullets/table/numbered): no limit | prose: ≤3 lines PAST TENSE — what was done + key result/finding>\"}}}}\n"
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
                _record_spawn_info(proj_id, resume_args, agent=agent, session_name=session_name, is_mother=True)
                _tmux_env = ["-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                             "-e", f"NS_SESSION_KEY={session_name}"]  # M1792: fix Stop hook key routing
                for _k, _v in _get_project_spawn_env(proj_id).items():
                    _tmux_env += ["-e", f"{_k}={_v}"]
                subprocess.Popen([
                    "tmux", "new-session", "-d", "-s", session_name,
                    "-c", spawn_cwd,
                    *_tmux_env,
                    *_get_agent_spawn_cmd(proj_id),
                    *resume_args,
                ])
                subprocess.run(["tmux", "set-option", "-t", session_name, "history-limit", "5000"], capture_output=True, timeout=2)
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
                        capture_output=True, text=True, timeout=2
                    ).stdout.split()
                    _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)
                    if _post_alive:
                        break
                if not _post_alive and resume_args:
                    await asyncio.to_thread(  # M1345
                        subprocess.run, ["tmux", "kill-session", "-t", f"={session_name}"], capture_output=True, timeout=5)
                    try:
                        (PROJECTS_DIR / proj_id / ".last-session-id").unlink()
                    except Exception:
                        pass
                    resume_args = []
                    _record_spawn_info(proj_id, resume_args, agent=agent, session_name=session_name, is_mother=True)
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
                            capture_output=True, text=True, timeout=2
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
                # M1790: same idle-spawn wake-suppression as the claude branch below —
                # skip the optimistic wake_sent busy record when there's nothing to claim.
                # M1795: force_dedup_reset=True — a freshly-spawned session shares the same
                # session_name as the just-killed session; _wake_last_sent[session_name] may
                # still hold a timestamp <90s old, silently blocking the post-spawn wake.
                if new_queued_top or _substar_queued_top:
                    _send_exec_wake(session_name, proj_id, force_dedup_reset=True)  # M1678: unified wake (was hardcoded "go")
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
            # M747: group queued stones by substar for per-substar assigned-session dispatch
            # M792: session router — substars sharing same assigned_session merge into one bucket
            _ns_list = proj.get("north_stars") or []
            _ns_by_id: dict = {ns["id"]: ns for ns in _ns_list if ns.get("id")}

            # M1802: late auto-assign removed — unassigned substars stay unassigned (manual only).
            _ns_by_id = {ns["id"]: ns for ns in _ns_list if ns.get("id")}

            def _resolve_session_key(proj_id: str, substar_id: str, ns_by_id: dict) -> str:
                """M792: return assigned_session for this substar.
                M855 fix: substar_id on a stone may not match any current north-star
                (stale ID, substar deleted, ID format mismatch).
                M1860: do NOT fall back to main session for unassigned substars —
                return "" so the stone is excluded from all dispatch queues until
                a session is explicitly assigned to its substar."""
                _assigned = (ns_by_id.get(substar_id) or {}).get("assigned_session") or ""
                return _assigned.strip()

            # _session_qs: session_key -> list of stones (may span multiple substars)
            _session_qs: dict = {}
            _main_qs: list = []
            for _q in _fresh_queued:
                _qsid = _q.get("substar_id", "")
                if _qsid:
                    _skey = _resolve_session_key(proj_id, _qsid, _ns_by_id)
                    if _skey:
                        _session_qs.setdefault(_skey, []).append(_q)
                    # M1860: _skey=="" → unassigned substar → exclude from all dispatch queues.
                    # Do NOT fall through to _main_qs (was the implicit effect of the old "" key).
                else:
                    _main_qs.append(_q)  # no substar_id → ungrouped → main
            # Keep _substar_qs alias for backward compatibility (kill-session loop below)
            _substar_qs = _session_qs
            # M824: respawn whenever any assigned_session exists — every assigned tmux session must run on dispatch
            _has_assigned_sessions = len(_session_qs) >= 1
            # M824 LEAK FIX: build set of stone IDs owned by assigned sessions
            _assigned_mids: set = {m.get("id") for stones in _session_qs.values() for m in stones} if _has_assigned_sessions else set()
            # Stones for main session: ungrouped only (when assigned sessions exist) or all (when not)
            # M824 COLLISION GUARD: if any _skey == session_name, those stones belong to main
            if _has_assigned_sessions:
                _collision_keys = [k for k in list(_session_qs.keys()) if k == session_name]
                for _ck in _collision_keys:
                    _main_qs.extend(_session_qs.pop(_ck))
                    _assigned_mids -= {m.get("id") for m in _main_qs}
            _dispatch_queued = _main_qs if _has_assigned_sessions else _fresh_queued
            # M824 LEAK FIX: filtered snapshot for main session — omits stones owned by assigned sessions
            # so main orchestrator never TaskCreates them (race condition fix)
            if _has_assigned_sessions and _assigned_mids:
                _main_all_ms_lines = "\n".join(
                    _snap_line_full(m) for m in active_ms if m.get("id") not in _assigned_mids
                )
                _assigned_session_names = sorted(_session_qs.keys())
            else:
                _main_all_ms_lines = all_ms_lines
                _assigned_session_names = []
            _fresh_waves = _compute_dispatch_waves(_dispatch_queued) if _dispatch_queued else []
            _stamp_wave_indices(proj_id, _fresh_waves)  # M511: label wave_index on each stone
            _fresh_waves_section = _format_dispatch_waves(_fresh_waves) if len(_dispatch_queued) > 1 else ""
            # Spawn tmux session — use TaskCreate/TaskUpdate (Claude Code built-in) for task tracking
            _cron_mem_section = _load_stone_memory(proj_id)
            # M472: build per-stone implementation steps (shared by both parallel and sequential sections)
            # M1869-P1: _stone_impl_steps compressed — removed verbose duplication with
            # _HUB_EXEC_SYS_PROMPT (skill protocol, gdrive rule) and collapsed rclone macro.
            # Before: ~1381 tok. After: ~580 tok. Savings: ~800 tok/wake.
            _rclone_evd = (
                f"     rclone copy <file> 'gdrive:claude-shared/{proj_id}/outbox/' && "
                f"FILE_ID=$(rclone lsjson 'gdrive:claude-shared/{proj_id}/outbox/' --include '<file>' | python3 -c \"import sys,json;d=json.load(sys.stdin);print(next((x['ID'] for x in d if not x.get('IsDir')),'' ))\") && "
                f"RESULT_URL=\"https://drive.google.com/file/d/$FILE_ID/view?usp=sharing\"\n"
            )
            # M1869-P3-A: _stone_impl_steps — removed steps already covered by MCP tool descriptions:
            # step 1b (skill_refs) → get_pending_task tool desc; step 5 PATCH format + 5a rclone evd
            # → report_task_complete tool desc; step 5b [검수] child → report_task_complete CHILD STONE PRE-CHECK.
            # Retained: silent-work rule, task status updates, milestones GET URL, implement, completion-log,
            # 5c [검수] screenshot (Playwright-specific context not in tool desc).
            _stone_impl_steps = (
                f"  Per-stone steps:\n"
                f"  ❌ Silent work — NO reply_to_stone before completion.\n"
                f"  1. TaskUpdate(<id>, 'in_progress')\n"
                f"  2. GET {hub_api}/api/northstar/{proj_id}/milestones — read full text + conversation[].\n"
                f"  3. Implement (Edit/Write files).\n"
                f"  4. completion-log: echo '{{\"session_id\":\"exec\",\"milestone_id\":\"<MID>\",\"evidence\":\"<summary>\",\"timestamp\":\"'$(date -Iseconds)'\"}}' >> ~/.hub/projects/{proj_id}/completion-log.jsonl\n"
                f"  5. report_task_complete(task_id='<MID>', summary='...') — see MCP tool desc for PATCH format, rclone upload, [검수] child stone rules.\n"
                f"  5d. M190 blocked (parent already has claude comment + follow-up work done) → create_child_stone(parent_id='<MID>', text='[진행 N/M] <MID>: <1-line result>', status='pending_confirmation', is_progress=True) + reply_to_stone(child_id, detail). Do NOT use status=queued (free-pool claim risk).\n"
                f"  5c. [검수] stone (text starts '[검수]') → screenshot result badge:\n"
                f"      mcp__playwright-session-1__browser_navigate http://127.0.0.1:{PORT}/northstar → browser_take_screenshot filename='${{HOME}}/.playwright-mcp/review-<MID>-result.png'\n"
                + _rclone_evd.replace("<file>", "review-<MID>-result.png") +
                f"      Add evidence_url to report_task_complete. Skip if research/docs only.\n"
                f"  6. TaskUpdate(<id>, 'completed')\n\n"
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
                f"  Post ONE consolidated summary listing completed MIDs (STRUCTURED: no limit; prose: ≤3 lines).\n\n"
                f"CAVEMAN LITE MODE (M582): Terse output — drop filler/politeness, keep technical precision. ~60% fewer output tokens.\n\n"
                + (
                    f"BRANCHED SESSIONS: substar-assigned queued stones are handled by their per-substar tmux sessions "
                    f"({', '.join(_assigned_session_names)}). "
                    f"DO NOT TaskCreate or Agent-dispatch those — they are filtered out of the snapshot below.\n\n"
                    if _assigned_session_names else ""
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
            if _has_assigned_sessions:
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

            # M255: kill agent-prefixed sessions before spawn — prevents orphaned cross-agent sessions
            # (e.g. codex-exec-FreeOS left alive when switching to claude-exec-FreeOS)
            # M1797: no mother/child distinction — kill all sessions before spawning a new one
            # M1807: spare branch sessions that already have queued work assigned — they were
            # spawned via assign_spawn and must not be killed here only to be re-spawned under
            # the main execute context.
            # M1816: also spare sessions currently assigned to a substar (even with no queued work)
            # — M1656-R4 intent was to protect idle assigned children but the _assigned_now set
            # was only checked AFTER _kill_all_exec_sessions already ran, so idle assigned children
            # were silently killed here before the M864 guard could protect them.
            _assigned_now: set = set()
            try:
                _p864 = _db_load_project(proj_id)
                for _ns864 in ((_p864 or {}).get("north_stars") or []):
                    _a864 = (_ns864.get("assigned_session") or "").strip() if isinstance(_ns864, dict) else ""
                    if _a864:
                        _assigned_now.add(_a864)
            except Exception:
                pass
            _assigned_spare = (set(_session_qs.keys()) if _has_assigned_sessions else set()) | _assigned_now
            # M1870: also spare any session currently running Claude — a live unassigned session
            # (e.g. resume-spawned but not yet assigned to a substar) must not be silently killed
            # just because the user spawns a second session. Only truly idle/shell-only sessions
            # should be pruned; if claude is running, keep it alive.
            try:
                for _ls in _live_sessions:
                    if f"-exec-{proj_id}-" in _ls and _ls != session_name:
                        _lp = subprocess.run(
                            ["tmux", "list-panes", "-t", f"={_ls}", "-F", "#{pane_current_command}"],
                            capture_output=True, text=True, timeout=2,
                        ).stdout.splitlines()
                        if any(c.strip() and c.strip() not in _SHELLS for c in _lp):
                            _assigned_spare.add(_ls)
            except Exception:
                pass
            _kill_all_exec_sessions(proj_id, spare=_assigned_spare)
            # M864: kill active substar sessions that have no queued work AND no assignment
            # M1870-b: also skip sessions currently running Claude — same invariant as M1870 above.
            _active_substars_now = _get_active_substar_sessions(proj_id)
            for _ss_short2, _ss_sname2 in _active_substars_now.items():
                if _ss_sname2 not in _session_qs and _ss_sname2 not in _assigned_now:
                    # M1870-b: do not kill if claude is currently running in this session
                    _m864_pane = subprocess.run(
                        ["tmux", "list-panes", "-t", f"={_ss_sname2}", "-F", "#{pane_current_command}"],
                        capture_output=True, text=True, timeout=2,
                    ).stdout.splitlines()
                    if any(c.strip() and c.strip() not in _SHELLS for c in _m864_pane):
                        continue  # claude is running — keep it alive
                    subprocess.run(["tmux", "kill-session", "-t", f"={_ss_sname2}"], capture_output=True, timeout=5)
                    _exec_idle_count.pop(_ss_sname2, None)
                    _exec_was_running.pop(_ss_sname2, None)
                    _server_log_action(proj_id, "", "exec:m864_stale_substar_kill", _ss_sname2)
            # M747/M792: also kill stale substar/session-key sessions before respawning
            # M824 ALIVE-BRANCH SKIP: reuse alive sessions that already have Claude running
            _alive_branch_keys: set = set()
            if _has_assigned_sessions:
                for _skey in list(_session_qs.keys()):
                    # M824 COLLISION GUARD: never kill main session here (handled by _kill_all_exec_sessions above)
                    if _skey == session_name:
                        continue
                    _pane_cmds = subprocess.run(
                        ["tmux", "list-panes", "-t", _skey, "-F", "#{pane_current_command}"],
                        capture_output=True, text=True, timeout=2
                    ).stdout.split()
                    _branch_alive = any(c.strip() and c.strip() not in _SHELLS for c in _pane_cmds)
                    if _branch_alive:
                        # Session already running Claude — skip kill/respawn; will send "go" after main spawns
                        _alive_branch_keys.add(_skey)
                    else:
                        subprocess.run(["tmux", "kill-session", "-t", f"={_skey}"], capture_output=True, timeout=5)
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
            # M1775: resolve the live session id deterministically instead of the L10557
            # sleep(2)+newest-mtime scan (same class of bug _capture_live_session_id_bg was
            # built to fix for forks — see critique: mtime-scan can pick a sibling branch
            # session's transcript, and 2s is not long enough to wait for a --resume spawn's
            # first write). Two cases, both already fully known before Popen:
            #  - resume_args == ["--resume", sid]: sid already exists (_get_resume_args'
            #    _try_id verified t.exists() before returning it) — that IS the live id.
            #  - resume_args == []: fresh conversation — pre-assign a UUID via --session-id
            #    so the transcript filename is deterministic (verified: claude --help +
            #    live fork test; applies equally to a fresh, non-forked spawn).
            # M1796: _presigned_main_sid already set at L9794 (before session_name was formed)
            # — reuse it; do NOT generate a second UUID here.
            _was_true_resume = bool(resume_args and resume_args[0] == "--resume" and len(resume_args) > 1)
            if not _was_true_resume and agent in (None, "claude", "openrouter") and not resume_args:
                resume_args = ["--session-id", _presigned_main_sid]
            _record_spawn_info(proj_id, resume_args, agent=agent, session_name=session_name, is_mother=True)
            # M1896: clear stale live_session_id cache so the poller reads fresh spawn-info
            # instead of returning a previous session's UUID under the same session_name.
            _live_sid_cache.pop(session_name, None)
            _tmux_env = [
                "-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}",
                "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                # M1184: PCT override only — window defaults to model's context size (no need to set explicitly)
                "-e", "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80",
                # M1792: NS_SESSION_KEY was missing from main-session spawn — Stop hook fell back to
                # CLAUDE_CODE_SESSION_ID (UUID), posting agent-busy to wrong key so _exec_was_running
                # never became True and idle→Telegram notification never fired.
                "-e", f"NS_SESSION_KEY={session_name}",
            ]
            for _k, _v in _get_project_spawn_env(proj_id).items():
                _tmux_env += ["-e", f"{_k}={_v}"]
            # M1656-R6: shared MCP+system-prompt args (same for main and child sessions)
            _mcp_full_args = _hub_mcp_spawn_args(proj_id, session_name, agent or "claude")
            subprocess.Popen([
                "tmux", "new-session", "-d", "-s", session_name,
                "-c", spawn_cwd,
                *_tmux_env,
                "claude", "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS, *_get_project_model(proj_id),
                *_mcp_full_args, *resume_args,
            ])
            subprocess.run(["tmux", "set-option", "-t", session_name, "history-limit", "5000"], capture_output=True, timeout=2)
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
                capture_output=True, text=True, timeout=2
            ).stdout.split()
            _post_alive = any(c.strip() and c.strip() not in _SHELLS for c in _post_panes)

            # M181: auto-retry once with no resume args if first spawn failed
            # while resume args were in play (likely stale --resume target).
            # M1775: gate on _was_true_resume (not resume_args' truthiness) — a fresh spawn's
            # presigned --session-id is never the cause of a boot failure, so it must not
            # trigger this retry path (retrying would just burn another 12s deadline wait).
            _retried = False
            if not _post_alive and _was_true_resume:
                # Kill pane, clear stale .last-session-id, respawn without resume
                await asyncio.to_thread(  # M1345
                    subprocess.run, ["tmux", "kill-session", "-t", f"={session_name}"], capture_output=True, timeout=5)
                try:
                    (PROJECTS_DIR / proj_id / ".last-session-id").unlink()
                except Exception:
                    pass
                _retried = True
                import uuid as _uuid_main_retry
                _presigned_main_sid = str(_uuid_main_retry.uuid4())
                resume_args = ["--session-id", _presigned_main_sid]  # fresh spawn — no continuity
                _record_spawn_info(proj_id, resume_args, agent=agent, session_name=session_name, is_mother=True)
                _tmux_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}", "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                             "-e", f"NS_SESSION_KEY={session_name}"]  # M1792: same fix as first spawn
                for _k, _v in _get_project_spawn_env(proj_id).items():
                    _tmux_env += ["-e", f"{_k}={_v}"]
                _mcp_args_retry = []
                if agent in (None, "claude", "openrouter"):
                    _mcp_args_retry = _hub_mcp_spawn_args(proj_id, session_name, agent or "claude")
                subprocess.Popen([
                    "tmux", "new-session", "-d", "-s", session_name,
                    "-c", spawn_cwd,
                    *_tmux_env,
                    "claude", "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS, *_get_project_model(proj_id), *_mcp_args_retry,
                    *resume_args,
                ])
                subprocess.run(["tmux", "set-option", "-t", session_name, "history-limit", "5000"], capture_output=True, timeout=2)
                elapsed = 0
                while elapsed < deadline:
                    await _aio.sleep(1)
                    elapsed += 1
                    if not prompt_file.exists():
                        break
                _post_panes = subprocess.run(
                    ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                    capture_output=True, text=True, timeout=2
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

            # M1129: use _exec_wake_msg (was hardcoded "go") so MCP-enabled sessions receive
            # the explicit tool-call instruction instead of bare "go".
            # M1230: auto-compact disabled by user request
            # M1114: two-step skill pre-inject
            # M1790: M1788's explicit_session_id idle-spawn path (▶ click, zero queued work)
            # reaches this same call site — same class of gap M1688 already fixed for the
            # assign-spawn wake path (_spawn_wake_when_ready): waking with nothing claimable
            # only sets a 45s optimistic wake_sent busy record (M1675) for no reason, making a
            # genuinely idle just-spawned session LOOK busy for 45s. Skip the wake entirely when
            # there's no actionable work for THIS session to claim.
            # M1795: force_dedup_reset=True — if user killed a session and re-spawned, the new
            # session shares the same session_name; _wake_last_sent[session_name] may hold a
            # timestamp <90s old, silently suppressing the post-spawn wake injection.
            # M1912: session-scoped wake gate — replace project-wide _substar_queued_top with
            # stones actually claimable by THIS session: ungrouped (no substar_id) go to main,
            # assigned substars only wake their own session. Prevents non-main sessions being
            # woken for other sessions' substar stones (_substar_queued_top was project-wide).
            _this_session_has_work = bool(
                _main_qs  # ungrouped stones → always this session's
                or _session_qs.get(session_name)  # substar stones explicitly assigned here
            )
            if _this_session_has_work:
                _send_exec_wake(session_name, proj_id, force_dedup_reset=True)

            # M747/M792: spawn per-session sessions after main session is live
            # M792: iterate session_qs (session_key -> stones); session_name IS the key
            if _has_assigned_sessions:
                _ss_tmux_base = list(_tmux_env)  # copy main tmux env
                _newly_spawned: list = []
                # M1166: assigned sessions should inherit the MOTHER (main) session's actual
                # running model, not the current project_meta model (which may have been
                # changed via dropdown between main spawn and assigned-session spawn). Read the
                # main session's spawn_info ONCE before the assigned-session loop — main was
                # spawned earlier so its model is what's recorded there. Falls back to project model
                # when spawn_info is missing or has no model field.
                _mother_model_args = _get_project_model(proj_id)
                try:
                    _msi_file = PROJECTS_DIR / proj_id / ".last-spawn-info.json"
                    if _msi_file.exists():
                        import json as _j_m1166
                        _msi = _j_m1166.loads(_msi_file.read_text())
                        _mother_model = (_msi.get("model") or "").strip()
                        if _mother_model:
                            _mother_model_args = ["--model", _mother_model]
                            _server_log_action(proj_id, "", "branched_model_inherit",
                                               f"mother model={_mother_model} (project default would have been {' '.join(_get_project_model(proj_id))})")
                except Exception:
                    pass
                for _skey, _ss_stones in _session_qs.items():
                    _ss_short = _skey[-12:]  # consistent with prompt file naming
                    # _skey is the full tmux session name (assigned_session or _substar_session_name)
                    _ss_sname = _skey
                    # M824 COLLISION GUARD: _skey == session_name means this bucket was merged into main above
                    if _ss_sname == session_name:
                        continue
                    # M824 ALIVE-BRANCH SKIP: session is already running Claude — just re-inject prompt
                    if _skey in _alive_branch_keys:
                        _send_exec_wake(_skey, proj_id)  # M1114: skill pre-inject
                        continue
                    _ss_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}-{_ss_short}",
                               "-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                               "-e", f"NS_SESSION_KEY={_skey}"]
                    for _k, _v in _get_project_spawn_env(proj_id).items():
                        _ss_env += ["-e", f"{_k}={_v}"]
                    # M1787: dead-session-reclaim --resume targeting now happens where the
                    # assignment itself is made (update_north_star's manual-assign spawn path),
                    # not here — this dispatch-time assigned-respawn loop no longer auto-assigns
                    # substars to a last-worked session (see M1787 note above), so any session
                    # reaching this point either already spawned via that path (and lands in
                    # _alive_branch_keys, skipping this loop entirely) or has no known prior
                    # transcript to target — --continue is the correct fallback either way.
                    # M1936: presign a UUID so live_session_id is known at spawn time — same
                    # pattern as M1803/M1775 for the main session. Without this, --continue
                    # sub-sessions left live_session_id="" forever (mtime scan ran immediately
                    # after Popen, before Claude had written any .jsonl, so no candidate found).
                    import uuid as _uuid_ss
                    _ss_presigned_sid = str(_uuid_ss.uuid4())
                    _ss_resume_args: list = ["--session-id", _ss_presigned_sid]
                    _encoded_cwd = _encode_cwd_for_claude(spawn_cwd)
                    _transcripts_dir = Path.home() / ".claude" / "projects" / _encoded_cwd
                    # M1944: presigned --session-id path (M1896 removed --continue entirely).
                    subprocess.Popen([
                        "tmux", "new-session", "-d", "-s", _ss_sname,
                        "-c", spawn_cwd,
                        *_ss_env,
                        "claude", "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS, *_mother_model_args,  # M1166
                        *_ss_resume_args,
                    ])
                    subprocess.run(["tmux", "set-option", "-t", _ss_sname, "history-limit", "5000"], capture_output=True, timeout=2)
                    # M1936: write spawn-info + live_session_id immediately — same as M1803 main path.
                    # Pass None for model so _record_spawn_info reads the project default (same as
                    # it would for a main session when no explicit model is provided).
                    _record_spawn_info(proj_id, _ss_resume_args, agent="claude", session_name=_ss_sname)
                    _write_sid_direct(proj_id, _ss_sname, _ss_presigned_sid)
                    # M1944: align assigned_session suffix with presigned UUID so tmux name and
                    # live_session_id are consistent — mirrors update_north_star's L7054 logic.
                    _ss_aligned_suffix = _ss_presigned_sid.replace("-", "")[:8]
                    _ss_aligned_name = f"claude-exec-{proj_id}-{_ss_aligned_suffix}"
                    if _ss_aligned_name != _skey:
                        try:
                            _proj_data = _db_load_project(proj_id)
                            _ns_list2 = _proj_data.get("north_stars", []) if _proj_data else []
                            for _ns2 in _ns_list2:
                                if _ns2.get("assigned_session") == _skey:
                                    _ns2["assigned_session"] = _ss_aligned_name
                                    _save_project(proj_id, _proj_data)
                                    _server_log_action(proj_id, "", "exec:substar_suffix_aligned",
                                                       f"{_skey} → {_ss_aligned_name}")
                                    break
                        except Exception:
                            pass
                    _newly_spawned.append((_ss_sname, time.time()))
                _server_log_action(proj_id, "", "exec:substar_branch",
                                   f"spawned {len(_newly_spawned)} new + {len(_alive_branch_keys)} reused sessions: "
                                   f"{','.join([s for s, _ in _newly_spawned]+list(_alive_branch_keys))}")

            # M256: use model from spawn-info (just written above) so the session is stored
            # under the correct model_key (e.g. "or-owl-alpha" not "").
            # M1685-j fix: was reading .last-spawn-info.json (the LEGACY mirror), which
            # _record_spawn_info only updates when _key=="_main" (server.py:5545) — but the
            # call above passes the real session_name, so _key is never "_main" and that file
            # goes stale the moment M1656-R9's session-scoped keying took over. Concretely:
            # a model switch (e.g. → claude-sonnet-5) spawned a session whose OWN transcript
            # ID then got recorded under the STALE legacy model (claude-sonnet-4-6) instead of
            # the new one — so .session-history.json never gained a claude-sonnet-5 key, and
            # every subsequent spawn fell through _get_resume_args's per-model lookup straight
            # to the final --continue fallback (badge shows bare "resume" with no id, forever,
            # until a manual model-history fixup). Read the session-scoped file instead
            # (_read_spawn_info, the correct M1656-R9 counterpart) so the model always matches
            # what THIS session actually spawned with.
            _recorded_model = _read_spawn_info(proj_id, session_name).get("model", "") or _get_project_model_value(proj_id) or ""
            # M1775: pass the already-known sid (pre-assigned via --session-id, or the verified
            # --resume target) — no sleep(2) wait, no newest-mtime scan race against concurrently
            # spawning branch sessions.
            _new_sid = _update_session_history_from_transcript(proj_id, spawn_cwd, _recorded_model, known_sid=_presigned_main_sid)
            # M833: capture branch session IDs for --resume continuity on next dispatch
            if _has_assigned_sessions and _newly_spawned:
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
            _all_spawned = [session_name] + (list(_session_qs.keys()) if _has_assigned_sessions else [])
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


@app.post("/api/northstar/{proj_id}/fork-session")
async def fork_session(proj_id: str, request: Request):
    """M1699-fork: spawn a NEW independent tmux session that inherits a SOURCE session's
    full conversation (via --resume <sid> --fork-session) but runs on a TARGET cross-provider
    model. The source session is never touched — its transcript, session-history anchor
    (_current/_default), and live tmux process all stay intact, so the user can resume the
    original model's work at any time after switching back.

    claude / openrouter only (both run the Claude Code CLI, so --fork-session + per-model
    --model/env override composes cleanly). codex/dsk not supported (different runtimes).

    body: { source_session_id: str, target_model: str, agent?: str }
      - source_session_id: the .jsonl session id to fork from (from /resumable-sessions)
      - target_model: any _ALLOWED_MODELS value (claude-* / or-*); "" → CLI default
      - agent: optional; auto-derived from target_model prefix (or-* → openrouter)
    """
    import json as _json_fork, uuid as _uuid_fork, asyncio as _aio_fork
    _raw = await request.body()
    data = {}
    if _raw:
        try:
            _parsed = _json_fork.loads(_raw.decode("utf-8"))
            if isinstance(_parsed, dict):
                data = _parsed
        except Exception:
            data = {}
    source_sid = (data.get("source_session_id") or "").strip()
    target_model = (data.get("target_model") or "").strip()
    agent = (data.get("agent") or "").strip().lower() or None

    # Validate source session exists as a transcript
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj_dir = _get_project_dir(proj_id) or str(Path.home())
    if not source_sid:
        return JSONResponse({"ok": False, "error": "source_session_id required"}, status_code=400)
    encoded = _encode_cwd_for_claude(str(proj_dir))
    src_t = Path.home() / ".claude" / "projects" / encoded / f"{source_sid}.jsonl"
    if not src_t.exists() or src_t.stat().st_size == 0:
        return JSONResponse({"ok": False, "error": "source session not found"}, status_code=404)

    # Agent resolution for fork (claude / openrouter only — both CLI-based):
    # or-* models MUST run under openrouter (LiteLLM proxy); non-or models use the
    # explicitly requested agent (default claude) when valid, else fall back to claude.
    if target_model.startswith("or-"):
        agent = "openrouter"
    elif agent in ("claude", "openrouter"):
        pass  # respect explicit valid agent
    elif agent in _ALLOWED_PTY_AGENTS:
        agent = "claude"  # codex/dsk not supported for fork
    else:
        agent = "claude"

    spawn_cwd = proj_dir if Path(proj_dir).exists() else str(Path.home())

    # Fork argv: inherit source conversation, write to OWN new session id.
    # M1775: pre-assign the new session id via --session-id so the transcript filename is
    # known deterministically at spawn time — verified via `claude --help` + live test that
    # --session-id composes with --resume/--fork-session.
    presigned_sid = str(_uuid_fork.uuid4())

    # M1792: branched tmux session name — distinct from the source session so it's
    # never killed by dispatch/restart (M1797: all sessions are peers; kill-all clears all).
    # Suffix now derives from presigned_sid (this fork's own live session id) instead of an
    # unrelated random UUID — the sess popup tooltip already shows "own live session: <id>"
    # separately from the tmux name, which was confusing when the two had nothing in common.
    suffix = presigned_sid.replace("-", "")[:8]
    session_name = f"{agent}-exec-{proj_id}-{suffix}"
    _fork_source_sid = source_sid
    # M1778: this endpoint (the "⑂ fork" button, M1699) never applied the M1679 truncation
    # fix — that fix only wired into _mother_fork_args, used by the OTHER two spawn paths
    # (assign-spawn/assigned-respawn), so forking via this button always inherited whatever
    # in-flight, unrelated work the source session was mid-turn on (reported: "fork session
    # 에서 작업중이던 작업을 재개"). Apply the same truncate-before-claim logic here: if the
    # source session is a live, busy tmux session with an in-flight stone claim, fork from a
    # truncated copy that ends right before that claim instead of her live conversation.
    try:
        _src_sess_name = None
        for _bsk, _brec in _agent_busy_sessions.items():
            if _brec.get("proj_id") == proj_id:
                _bsi = _read_spawn_info(proj_id, _bsk)
                if (_bsi.get("live_session_id") or _bsi.get("from_id")) == source_sid:
                    _src_sess_name = _bsk
                    break
        if _src_sess_name:
            _src_rec = _agent_busy_sessions.get(_src_sess_name) or {}
            _src_stone = _src_rec.get("stone_id")
            if _session_is_busy(_src_sess_name):
                # M1780: no-stone-claim case (direct interactive conversation) — see
                # _mother_fork_args twin fix for the same extension and rationale.
                if _src_stone:
                    _cut_sid = _truncate_transcript_before_stone(src_t, _src_stone)
                else:
                    _cut_sid = _truncate_transcript_before_last_user_turn(src_t)
                if _cut_sid:
                    _server_log_action(proj_id, _src_stone or "", "exec:fork_truncated",
                                       f"source:{_src_sess_name} orig_sid:{source_sid} cut_sid:{_cut_sid}")
                    _fork_source_sid = _cut_sid
            else:
                # M1980: idle-session fork — source session is done but transcript ends with
                # a hub-wake exchange. If not truncated, the child inherits "Tasks ready. Call
                # get_pending_task()..." and re-executes the last stone on startup (observed:
                # FRWP capital-flow roadmap rebuilt twice). Cut before the last hub-wake turn.
                _cut_sid = _truncate_transcript_before_last_hub_wake(src_t)
                if _cut_sid:
                    _server_log_action(proj_id, "", "exec:fork_truncated",
                                       f"source:{_src_sess_name} orig_sid:{source_sid} cut_sid:{_cut_sid} reason:idle-hub-wake")
                    _fork_source_sid = _cut_sid
        else:
            # M1980: source session not in agent_busy_sessions (e.g. very old session, hub
            # restarted). Still apply hub-wake truncation by scanning transcript directly.
            _cut_sid = _truncate_transcript_before_last_hub_wake(src_t)
            if _cut_sid:
                _server_log_action(proj_id, "", "exec:fork_truncated",
                                   f"source:(unknown) orig_sid:{source_sid} cut_sid:{_cut_sid} reason:idle-hub-wake-fallback")
                _fork_source_sid = _cut_sid
    except Exception:
        pass  # any failure — fall back to raw fork below (M1679 has the same fallback contract)
    resume_args = ["--resume", _fork_source_sid, "--fork-session", "--session-id", presigned_sid]
    # Target model override (does NOT mutate project's stored model).
    model_args = _get_project_model(proj_id, override_model=target_model if target_model else None)

    _record_spawn_info(proj_id, resume_args, agent=agent, session_name=session_name,
                       model=target_model if target_model else _get_project_model_value(proj_id) or "")

    # Env: splice target-model proxy routing (openrouter → LiteLLM proxy).
    # M1757: NS_SESSION_KEY must match session_name so agent-busy POST uses session-scoped key
    # (not legacy::proj_id) — same as assign-spawn (L6449) and main-spawn (L10316).
    _tmux_env = ["-e", f"NS_HUB_URL=http://{_tailscale_interface_ip()}:{PORT}",
                 "-e", f"NS_SESSION_KEY={session_name}",
                 "-e", "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80"]
    for _k, _v in _get_project_spawn_env(proj_id, override_model=target_model if target_model else None).items():
        _tmux_env += ["-e", f"{_k}={_v}"]

    # M1116: write per-session MCP config for claude/openrouter fork (hub-mcp-server bridge)
    try:
        _mcp_cfg = _write_mcp_config(proj_id, session_name)
        if _mcp_cfg:
            model_args = [*model_args, "--mcp-config", _mcp_cfg,
                          "--append-system-prompt", _HUB_EXEC_SYS_PROMPT]
    except Exception:
        pass

    # M1750: build the claude binary WITHOUT _get_agent_spawn_cmd's embedded --model,
    # because model_args (below) already carries the (override) model. Using _get_agent_spawn_cmd
    # here double-injected --model (once from the agent cmd, once from model_args), producing
    # "... --model openrouter-hy3 --mcp-config X --model openrouter-hy3 --resume ...". The duplicate
    # --model corrupts arg parsing / model validation and is the cause of "issue with the selected
    # model" on fork (new-session path injects --model exactly once and works fine).
    claude_bin = _resolve_claude_bin()
    _agent_base_cmd = [claude_bin, "--dangerously-skip-permissions", *_DISALLOWED_TOOLS_ARGS]
    subprocess.Popen([
        "tmux", "new-session", "-d", "-s", session_name,
        "-c", spawn_cwd,
        *_tmux_env,
        *_agent_base_cmd,
        *model_args,
        *resume_args,
    ])
    subprocess.run(["tmux", "set-option", "-t", session_name, "history-limit", "5000"],
                   capture_output=True, timeout=2)
    _exec_idle_file(proj_id).unlink(missing_ok=True)
    _server_log_action(proj_id, "", "exec:fork",
                       f"session:{session_name} from:{source_sid} model:{target_model or 'default'}")
    _ns_push("session_running", proj_id=proj_id, kind="exec")

    # M1775: live session id is already known (pre-assigned via --session-id above) — write
    # it directly instead of polling the transcript dir for the new file.
    _write_sid_direct(proj_id, session_name, presigned_sid)
    # M1757: fork-spawn was missing readiness-gated wake — parity with assign/branch paths
    threading.Thread(target=_spawn_wake_when_ready, args=(session_name, proj_id), daemon=True).start()
    # M1743: record the fork in session-history AFTER _capture_live_session_id_bg confirms
    # the new transcript SID. Running both threads concurrently caused _update_session_history
    # to grab the pre-fork newest JSONL (old session) instead of the fork's actual new JSONL.
    # Fix: poll spawn-info for live_session_id (written by _capture_live_session_id_bg), then
    # write directly to hist — never touch _current/_default (preserve_current=True semantics).
    _fork_hist_key = target_model or _get_project_model_value(proj_id) or "claude-sonnet-4-5"

    def _fork_hist_update_bg():
        import time as _ft
        _deadline = _ft.time() + 25.0
        while _ft.time() < _deadline:
            _ft.sleep(1.0)
            _si = _read_spawn_info(proj_id, session_name)
            _new_sid = _si.get("live_session_id", "")
            if not _new_sid:
                continue
            # M1747: contamination guard — derive expected agent from tmux session_name prefix
            # (immutable ground truth set at spawn time) instead of _fork_hist_key model prefix
            # or transcript content (unreliable at 0-byte). Prevents claude SID landing in or-* key.
            _tmux_agent, _ = _parse_exec_session_name(session_name)
            _mk_agent = _tmux_agent or ("openrouter" if _fork_hist_key.startswith("or-") else "codex" if _fork_hist_key.startswith("codex-") else "claude")
            try:
                _enc = _encode_cwd_for_claude(str(spawn_cwd))
                _t = Path.home() / ".claude" / "projects" / _enc / f"{_new_sid}.jsonl"
                if _t.exists() and _t.stat().st_size > 0:
                    _det = _detect_session_model(_t)
                    if _det:  # M1750-b: only check agent when detection is conclusive; empty = no API calls yet, trust _mk_agent (tmux session name)
                        _det_agent = "openrouter" if _det.startswith("or-") else "codex" if _det.startswith("codex-") else "claude"
                        # M1750-c: OpenRouter aliases hy3:free → claude-sonnet-5 in transcript model field.
                        # If _mk_agent is openrouter but _det_agent is claude, this is OR upstream aliasing,
                        # NOT a true agent mismatch — skip the guard (trust tmux session name prefix).
                        if _det_agent != _mk_agent and not (_mk_agent == "openrouter" and _det_agent == "claude"):
                            return  # wrong agent — abort, do not write
            except Exception:
                pass
            # Write fork SID under its model key without touching _current/_default.
            # M1804: locked read-mutate-write — see _update_session_history_locked.
            try:
                _update_session_history_locked(
                    proj_id, lambda h: _push_session_history_ring(h, _fork_hist_key, _new_sid))
            except Exception:
                pass
            return

    threading.Thread(target=_fork_hist_update_bg, daemon=True).start()

    # M1763: record fork spawn time so _send_exec_wake's 20s boot-grace fires correctly.
    # Without this, _session_spawn_ts[session_name] == 0 → grace evaluates to ∞s ago →
    # wake lands in the input box while the harness TUI is still initialising.
    _session_spawn_ts[session_name] = time.time()
    threading.Thread(
        target=_spawn_wake_when_ready, args=(session_name, proj_id), daemon=True
    ).start()  # M1763: use readiness-gated wake (same as fresh spawn) instead of blind immediate send
    return JSONResponse({
        "ok": True, "mode": "fork", "session": session_name,
        "source_session_id": source_sid, "target_model": target_model or "(default)",
        "message": f"Forked session {source_sid} → {session_name} on {target_model or 'default model'}",
    })


@app.post("/api/northstar/create")
async def create_project(request: Request):
    """Create a new project node. M1368: SQLite-first — duplicate check via DB, no md check needed."""
    data = await request.json()
    name = (data.get("name") or "").strip()
    repo_path = (data.get("repo_path") or "").strip() or None
    if not name:
        return JSONResponse({"ok": False, "error": "name required"}, status_code=400)
    import re as _re_
    folder_id = _re_.sub(r"[^\w\-]", "_", name)
    # M1368: check SQLite first — md existence no longer required as duplicate gate
    if _db_load_project(folder_id) is not None:
        return JSONResponse({"ok": False, "error": "project already exists"}, status_code=409)
    proj_dir = PROJECTS_DIR / folder_id
    proj_dir.mkdir(parents=True, exist_ok=True)
    project_data = {
        "name": name, "metric": "", "current": "", "target": "",
        "status": "paused", "deadline": "", "note": "",
        "connections": [],
        "north_stars": [],
        "layer": 0, "x": None, "y": None,
    }
    created_dir = False
    if repo_path:
        was_created, resolved = _ensure_repo_path_exists(repo_path)
        created_dir = was_created
        project_data["repo_path"] = resolved or repo_path
    # Primary store: SQLite (no north-star.md created — OKR body removed per M1368)
    _db_save_project(folder_id, project_data)
    return JSONResponse({"ok": True, "id": folder_id, "created_repo_dir": created_dir})


@app.post("/api/northstar/{proj_id}/semantic-scores")
async def compute_semantic_scores(proj_id: str, request: Request):
    """M658: Compute semantic proximity score (0-100) per stone using sentence-transformers.
    Score = cosine_similarity(stone_embedding, substar_embedding) * completion_weight * 100.
    Results cached in data_json['semantic_score']. Only scores stones assigned to a substar."""
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
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
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
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
        _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
        _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "filled": filled})


@app.delete("/api/northstar/{proj_id}")
async def delete_project(proj_id: str):
    """Delete a project node (removes north-star.md if present)."""
    # M1694 P2 fix: was checking proj_dir.exists() despite the comment saying "SQLite is
    # primary" — if the directory was already removed by any other path (manual cleanup,
    # a crashed prior delete, external tooling), this 404'd and the project_meta row below
    # was never reached, permanently orphaning it (found live via the M1694 smoke test
    # suite's own cleanup fixture). Check the DB record instead, matching the comment's
    # actual intent; still guard the filesystem ops below with .exists() individually.
    proj_dir = PROJECTS_DIR / proj_id
    if _db_load_project(proj_id) is None:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    # M918: kill all exec sessions for this project before deletion
    try:
        killed = _kill_all_exec_sessions(proj_id)
        if killed:
            print(f"[M918] killed sessions for {proj_id}: {killed}", file=__import__('sys').stderr)
    except Exception as _e:
        print(f"[M918] session kill error for {proj_id}: {_e}", file=__import__('sys').stderr)
    md = proj_dir / "north-star.md"
    if md.exists():
        md.unlink()
    # M1368: delete from SQLite project_meta (was missing — caused card resurrection on reload)
    # M1406: also write to deleted_projects table to prevent migration re-seeding on restart
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute("DELETE FROM project_meta WHERE proj_id=?", (proj_id,))
        conn.execute("""CREATE TABLE IF NOT EXISTS deleted_projects
                        (proj_id TEXT PRIMARY KEY, deleted_at TEXT NOT NULL)""")
        conn.execute("INSERT OR REPLACE INTO deleted_projects(proj_id, deleted_at) VALUES(?,?)",
                     (proj_id, __import__('datetime').datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
    except Exception as _del_e:
        print(f"[delete_project] SQLite delete error: {_del_e}", file=__import__('sys').stderr)
    return JSONResponse({"ok": True})


@app.post("/api/northstar/{proj_id}/connect")
async def add_connection(proj_id: str, request: Request):
    """Add a connection edge between two projects (bidirectional)."""
    data = await request.json()
    target_id = data.get("target", "").strip()
    if not target_id or target_id == proj_id:
        return JSONResponse({"ok": False, "error": "invalid target"}, status_code=400)
    # M1368: SQLite-first
    for pid, tid in [(proj_id, target_id), (target_id, proj_id)]:
        _p = _db_load_project(pid)
        if not _p: continue
        conns = _p.get("connections") or []
        if not isinstance(conns, list): conns = []
        if tid not in conns:
            conns.append(tid)
        _p["connections"] = conns
        _save_project(pid, _p)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/connect/{target_id}")
async def remove_connection(proj_id: str, target_id: str):
    """Remove a connection edge between two projects."""
    # M1368: SQLite-first
    for pid, tid in [(proj_id, target_id), (target_id, proj_id)]:
        _p = _db_load_project(pid)
        if not _p: continue
        conns = [c for c in (_p.get("connections") or []) if c != tid]
        _p["connections"] = conns
        _save_project(pid, _p)
    return JSONResponse({"ok": True})


@app.patch("/api/northstar/{proj_id}/rename")
async def rename_project(proj_id: str, request: Request):
    data = await request.json()
    new_name = data.get("name", "").strip()
    if not new_name:
        return JSONResponse({"ok": False, "error": "name required"}, status_code=400)
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False}, status_code=404)
    proj["name"] = new_name
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "name": new_name})


# ── M1355: per-project concept graph (user-defined LLM-layer nodes) ────────────
@app.get("/api/northstar/{proj_id}/concept-graph")
async def get_concept_graph(proj_id: str):
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT node_id, layer, name, parents_json, x, y, layer_order, updated_at "
            "FROM concept_graph_nodes WHERE proj_id=? "
            "ORDER BY layer, COALESCE(layer_order, 1e18), node_id",
            (proj_id,),
        ).fetchall()
        conn.close()
        nodes = []
        for r in rows:
            try:
                parents = json.loads(r[3] or "[]")
            except Exception:
                parents = []
            nodes.append({
                "id": r[0], "layer": r[1], "name": r[2],
                "parents": parents, "x": r[4], "y": r[5],
                "layer_order": r[6],
                "updated_at": r[7],
            })
        return JSONResponse({"ok": True, "nodes": nodes})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/northstar/{proj_id}/concept-graph/nodes")
async def upsert_concept_node(proj_id: str, request: Request):
    """Create or update a single node (idempotent by node_id)."""
    data = await request.json()
    node_id = (data.get("id") or "").strip()
    if not node_id:
        return JSONResponse({"ok": False, "error": "id required"}, status_code=400)
    layer = float(data.get("layer") or 0)
    name = (data.get("name") or "").strip()
    parents = data.get("parents") or []
    if not isinstance(parents, list):
        parents = []
    # dedup while preserving order, and strip self-references
    _seen_p: set = set()
    parents = [p for p in parents if p != node_id and p not in _seen_p and not _seen_p.add(p)]
    x = data.get("x")
    y = data.get("y")
    layer_order = data.get("layer_order")
    import datetime as _dt_cg
    now = _dt_cg.datetime.utcnow().isoformat()
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        existing = conn.execute(
            "SELECT node_id FROM concept_graph_nodes WHERE proj_id=? AND node_id=?",
            (proj_id, node_id),
        ).fetchone()
        is_new = existing is None
        conn.execute(
            "INSERT INTO concept_graph_nodes(proj_id, node_id, layer, name, parents_json, x, y, layer_order, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(proj_id, node_id) DO UPDATE SET "
            "layer=excluded.layer, name=excluded.name, parents_json=excluded.parents_json, "
            "x=excluded.x, y=excluded.y, layer_order=excluded.layer_order, "
            "updated_at=excluded.updated_at",
            (proj_id, node_id, layer, name, json.dumps(parents, ensure_ascii=False),
             x, y, layer_order, now),
        )
        conn.commit()
        conn.close()
        # M1423: auto-create matching substar (cg_{node_id}) when a NEW node is inserted
        if is_new:
            try:
                _proj = _db_load_project(proj_id)
                if _proj is not None:
                    _cg_ns_id = f"cg_{node_id}"
                    _ns_list = _proj.get("north_stars") or []
                    if not any(ns.get("id") == _cg_ns_id for ns in _ns_list):
                        _ns_list.append({"id": _cg_ns_id, "name": name, "milestones": []})
                        _proj["north_stars"] = _ns_list
                        _save_project(proj_id, _proj)
            except Exception:
                pass
        return JSONResponse({"ok": True, "id": node_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/northstar/{proj_id}/concept-graph/nodes/{node_id}/order")
async def reorder_concept_node(proj_id: str, node_id: str, request: Request):
    """Update only layer_order (lighter than full upsert)."""
    import datetime as _dt_cg
    data = await request.json()
    order = data.get("layer_order")
    if order is None:
        return JSONResponse({"ok": False, "error": "layer_order required"}, status_code=400)
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        conn.execute(
            "UPDATE concept_graph_nodes SET layer_order=?, updated_at=? "
            "WHERE proj_id=? AND node_id=?",
            (order, _dt_cg.datetime.utcnow().isoformat(), proj_id, node_id),
        )
        conn.commit()
        conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/northstar/{proj_id}/concept-graph/nodes/{node_id}")
async def delete_concept_node(proj_id: str, node_id: str):
    import datetime as _dt_cg
    now = _dt_cg.datetime.utcnow().isoformat()
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        # Also strip this node from other nodes' parents lists
        rows = conn.execute(
            "SELECT node_id, parents_json FROM concept_graph_nodes WHERE proj_id=?",
            (proj_id,),
        ).fetchall()
        for r in rows:
            try:
                pl = json.loads(r[1] or "[]")
            except Exception:
                pl = []
            if node_id in pl:
                pl = [p for p in pl if p != node_id]
                conn.execute(
                    "UPDATE concept_graph_nodes SET parents_json=?, updated_at=? "
                    "WHERE proj_id=? AND node_id=?",
                    (json.dumps(pl, ensure_ascii=False), now,
                     proj_id, r[0]),
                )
        conn.execute(
            "DELETE FROM concept_graph_nodes WHERE proj_id=? AND node_id=?",
            (proj_id, node_id),
        )
        conn.commit()
        conn.close()
        # M1415: cascade-delete matching substar (id=cg_{node_id}) when CG node is removed
        try:
            _proj = _db_load_project(proj_id)
            if _proj:
                _cg_ns_id = f"cg_{node_id}"
                _ns_list = _proj.get("north_stars") or []
                _new_ns = [ns for ns in _ns_list if ns.get("id") != _cg_ns_id]
                if len(_new_ns) != len(_ns_list):
                    _proj["north_stars"] = _new_ns
                    _save_project(proj_id, _proj)
        except Exception:
            pass
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/northstar/{proj_id}")
async def patch_project(proj_id: str, request: Request):
    """Update simple top-level project fields (deadline, status, note, links, metric/current/target/unit, etc.)."""
    data = await request.json()
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False}, status_code=404)
    # M165: allow manual edit of star fields (metric/current/target/unit) from ns-dash UI
    # M214: `model` controls --model flag passed to tmux exec agent spawns for this project.
    # `agent` selects which CLI runtime owns the loop (`claude` or `codex`).
    # continuity_mode: session continuity strategy (isolated/portable/fresh)
    allowed = {"deadline", "status", "note", "links", "stage",
               "metric", "current", "target", "unit", "model", "agent", "continuity_mode",
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
    # M1368: SQLite-first
    proj = _db_load_project(proj_id)
    if not proj:
        return JSONResponse({"ok": False}, status_code=404)
    created_dir = False
    for k in ("layer", "parent", "position_x", "position_y", "x", "y", "repo_path", "stage", "avatar_url", "hidden"):
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
# M1899-R17: ring buffer for mobile long-poll. Max 60 events; older entries dropped.
import collections as _collections
_NS_PUSH_BUF: _collections.deque = _collections.deque(maxlen=60)

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

_notify_last_ts: dict[str, float] = {}  # M1171: 60s cooldown
_NOTIFY_COOLDOWN_SEC = 60              # same title within 60s → drop

def _has_queued_stones(proj_id: str) -> bool:
    """M1171 v2: True if project still has stones waiting to execute (status='queued', not held).
    Used to suppress idle notifications when back-to-back stones are running.
    M1718-b: project-wide — see _session_has_queued_stones for the session-scoped variant
    that should be used wherever the caller has a specific session_name in hand."""
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB), timeout=2)
        count = conn.execute(
            "SELECT COUNT(*) FROM milestones_store WHERE proj_id=? AND status='queued' AND COALESCE(held,0)=0",
            (proj_id,)
        ).fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False  # fail-open: if DB unreachable, allow notification


def _session_has_queued_stones(proj_id: str, session_name: str) -> bool:
    """M1718-b: session-scoped idle-notify suppression check. _has_queued_stones counted
    ANY queued stone project-wide, so a child session (e.g. claude-exec-MOAT-b5756107)
    going genuinely idle had its Telegram notification suppressed just because an
    unrelated stone was queued for a DIFFERENT session (e.g. the mother's own queue, or
    a sibling child's substar) — verified live: MOAT mother went idle 2026-07-07 09:59:40
    while cg_하네스's M1712 sat queued for child -b5756107, and the mother's idle
    notification never fired (no [tg] log line, no cooldown-file entry — the call was
    gated off before _send_ntfy_notification was ever invoked). Reuses
    _session_claimable_queued_count (M1676) — same "could THIS session claim it" logic
    already used for wake-eligibility, so a session with zero claimable work is treated
    as genuinely idle regardless of what other sessions still have queued."""
    try:
        return _session_claimable_queued_count(proj_id, session_name) > 0
    except Exception:
        return False  # fail-open: if check fails, allow notification (same fail-open as _has_queued_stones)


def _idle_notify_label(session_name: str, proj_id: str) -> str:
    """M1718-c: display label for idle-notify title — strips the "{agent}-exec-" prefix
    so the mother session shows just "HugwartsBanana" (clean, same as before M1718-c) while
    a branched child shows "HugwartsBanana-b5756107" (distinguishable from the mother).
    First attempt at this (using the raw session_name including the exec- prefix) was too
    noisy per user feedback ("claude-exec-HugwartsBanana exec idle" / full sentence body) —
    this strips the redundant agent+exec prefix and keeps the body short like the original."""
    for prefix in ("claude-exec-", "codex-exec-", "openrouter-exec-", "dsk-exec-"):
        if session_name.startswith(prefix):
            return session_name[len(prefix):]
    return session_name or proj_id

_TG_COOLDOWN_FILE = _HUB_DATA_DIR / ".tg-last-sent.json"  # M1171 v3: cross-process cooldown

def _send_ntfy_notification(title: str, body: str, priority: str = "default") -> None:
    """Send Telegram push notification for live→idle events. In-page toast handled separately via SSE.
    Safe to call from async context — runs in a daemon thread so urlopen never blocks the event loop."""
    import threading as _thr
    _thr.Thread(target=_send_ntfy_notification_blocking, args=(title, body, priority), daemon=True).start()


def _send_ntfy_notification_blocking(title: str, body: str, priority: str = "default") -> None:
    """Blocking implementation — always called from a background thread, never from the event loop."""
    now = time.time()
    # M1171 v3: file-based cooldown so multiple hub processes (dev instances, zombie restarts)
    # share the same dedup state — prevents 3x fires when extra hub instances are alive.
    try:
        _ts_data = json.loads(_TG_COOLDOWN_FILE.read_text()) if _TG_COOLDOWN_FILE.exists() else {}
    except Exception:
        _ts_data = {}
    last_file = float(_ts_data.get(title, 0))
    last_mem  = _notify_last_ts.get(title, 0)
    last = max(last_file, last_mem)
    if now - last < _NOTIFY_COOLDOWN_SEC:
        return
    _notify_last_ts[title] = now
    try:
        _ts_data[title] = now
        # Prune entries older than 2x cooldown to keep file small
        _ts_data = {k: v for k, v in _ts_data.items() if now - v < _NOTIFY_COOLDOWN_SEC * 2}
        _TG_COOLDOWN_FILE.write_text(json.dumps(_ts_data))
    except Exception:
        pass
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
    _NS_PUSH_BUF.append(data)  # M1899-R17: also buffer for mobile long-poll
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
                    data = await asyncio.wait_for(q.get(), timeout=180.0)  # M1899-R5: 45s→90s; M1899-R7: 90s→180s (ROLLBACK: change 180.0 back to 90.0) — halves radio wakeup on mobile
                    yield f"event: {data['event']}\ndata: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    # heartbeat — comment line, ignored by EventSource
                    yield ": ping\n\n"
        finally:
            try: _NS_PUSH_SUBSCRIBERS.remove(q)
            except ValueError: pass
    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/notifications/poll")
async def notifications_poll(since: float = 0):
    """M1899-R17: Mobile long-poll endpoint. Returns events from _NS_PUSH_BUF newer than
    `since` (Unix epoch seconds, float). Client passes last received event ts as since.
    Max 60 events buffered; events older than buffer window are silently dropped."""
    events = [e for e in list(_NS_PUSH_BUF) if e["ts"] > since]
    return JSONResponse({"ok": True, "events": events, "server_ts": time.time()})


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
_session_ctx: dict[str, dict] = {}  # M1263: proj_id → {used, total, ts} context window usage


@app.patch("/api/northstar/{proj_id}/session-status")
async def set_session_status(proj_id: str, request: Request):
    """Hook endpoint — Stop/Notification hooks POST status updates here."""
    data = await request.json()
    status = data.get("status", "IDLE").upper()
    if status not in ("RUNNING", "WAITING", "IDLE", "DONE"):
        return JSONResponse({"ok": False, "error": "invalid status"}, status_code=400)
    _pill_status[proj_id] = status
    return JSONResponse({"ok": True, "status": status})


@app.post("/api/northstar/{proj_id}/session-ctx")
async def set_session_ctx(proj_id: str, request: Request):
    """M1263: Stop hook reports context_window usage — stored in memory for terminal badge."""
    data = await request.json()
    used = data.get("used", 0)
    total = data.get("total", 0)
    import datetime as _dt_ctx
    _session_ctx[proj_id] = {"used": used, "total": total, "ts": _dt_ctx.datetime.now().isoformat()}
    _ns_push("ctx_updated", proj_id=proj_id, used=used, total=total)
    return JSONResponse({"ok": True, "used": used, "total": total})


@app.get("/api/northstar/{proj_id}/session-ctx")
async def get_session_ctx(proj_id: str):
    """M1263: Return latest context window usage for this project."""
    return JSONResponse(_session_ctx.get(proj_id) or {"used": 0, "total": 0})


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
        check = subprocess.run(["tmux", "has-session", "-t", f"={candidate}"], capture_output=True, timeout=2)
        if check.returncode == 0:
            exec_session = candidate
            break
    if not exec_session:
        return JSONResponse({"ok": False, "error": "exec session not running"}, status_code=404)
    subprocess.run(["tmux", "send-keys", "-t", exec_session, prompt, "Enter"], capture_output=True, timeout=3)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/exec-session")
async def kill_exec_session(proj_id: str):
    """Kill ALL Execute-spawned tmux sessions for a project (all agent prefixes).
    M375: was breaking after first kill — orphaned sessions from prior agent stayed alive.
    M985: now delegates to _kill_all_exec_sessions so substar assigned_session is also cleared."""
    killed_sessions = []
    for candidate in _exec_session_names(proj_id):
        result = subprocess.run(["tmux", "kill-session", "-t", f"={candidate}"], capture_output=True, timeout=5)
        if result.returncode == 0:
            killed_sessions.append(candidate)
            _exec_idle_count.pop(candidate, None)
            _exec_was_running.pop(candidate, None)
    # M985: clear assigned_session on all substars when sessions are killed via this API
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
                # M1095: invalidate mtime cache so next read hits SQLite (not stale L1)
                _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
    except Exception:
        pass
    _exec_sessions_cache["ts"] = 0.0  # M1557: invalidate cache so next poll reflects kill immediately
    _bg_tmux_state["ts"] = 0.0  # M1963: force on-demand tmux query (bg cache may be up to 3s stale)
    # M1613: push SSE so client clears exec-pane immediately (no 8s poll wait)
    if killed_sessions:
        _ns_push("session_idle", proj_id=proj_id, kind="exec")
    return {"ok": True, "killed": bool(killed_sessions), "sessions": killed_sessions}


@app.delete("/api/northstar/{proj_id}/tmux-session/{session_name}")
async def kill_named_tmux_session(proj_id: str, session_name: str):
    """M792: kill a specific named tmux session (used from session popup kill button).
    clear assigned_session on every substar that referenced the killed session."""
    killed = []
    cleared_substars = []

    # Kill the requested session.
    result = subprocess.run(["tmux", "kill-session", "-t", f"={session_name}"], capture_output=True, timeout=5)
    if result.returncode == 0:
        killed.append(session_name)
    _exec_idle_count.pop(session_name, None)
    _exec_was_running.pop(session_name, None)
    _pane_ring_buf.pop(session_name, None)   # M1836-v2: clear ring buffer so new session starts fresh
    _pane_ring_prev.pop(session_name, None)
    _pane_ring_lock.pop(session_name, None)
    _live_sid_cache.pop(session_name, None)  # M1826-v2: clear cached live_session_id on kill

    # Clear assigned_session on any substar that pointed at the killed session.
    try:
        p = _db_load_project(proj_id)
        if p:
            north_stars = p.get("north_stars") or []
            changed = False
            for ns in north_stars:
                if not isinstance(ns, dict): continue
                if (ns.get("assigned_session") or "") == session_name:
                    ns["assigned_session"] = None
                    cleared_substars.append(ns.get("id") or "")
                    changed = True
            if changed:
                _db_save_project(proj_id, p)
                # M1095: invalidate parse cache so UI reads fresh SQLite data
                _parse_cache.pop(str(PROJECTS_DIR / proj_id / "north-star.md"), None)
    except Exception:
        pass

    _exec_sessions_cache["ts"] = 0.0  # M1557: invalidate cache so next poll reflects kill immediately
    _bg_tmux_state["ts"] = 0.0  # M1963: force on-demand tmux query (bg cache may be up to 3s stale)
    # M1663: push SSE so client clears exec-pane immediately (no poll-wait) — this endpoint
    # was missing the push that _kill_all_exec_sessions' sibling (/exec-session DELETE) already had,
    # so single/branched-session kills relied purely on client-side burst-poll and lagged visibly.
    if killed:
        _ns_push("session_idle", proj_id=proj_id, kind="exec")
    _server_log_action(proj_id, "", "exec:kill",
                       f"session:{session_name} killed:{','.join(killed)} cleared:{len(cleared_substars)}")
    return {"ok": bool(killed), "session": session_name,
            "killed": killed, "cleared_substars": cleared_substars}


@app.post("/api/northstar/{proj_id}/tmux-session/{session_name}/rename")
async def rename_tmux_session(proj_id: str, session_name: str, body: dict = Body(default={})):
    """M810: rename a specific tmux session to a new name."""
    new_name = (body.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="new_name required")
    result = subprocess.run(["tmux", "rename-session", "-t", session_name, new_name], capture_output=True, timeout=3)
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
        r = await asyncio.to_thread(
            subprocess.run, ["tmux", "list-sessions", "-F", "#{session_name}"],
            capture_output=True, text=True, timeout=2)
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
        r = await asyncio.to_thread(
            subprocess.run,
            ["tmux", "list-sessions", "-F",
             "#{session_name}\t#{session_created}\t#{session_activity}"],
            capture_output=True, text=True, timeout=2)
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
    # M1762: load spawn-info-by-session once and merge into each session entry so
    # the frontend can show live_session_id immediately without a separate API call.
    _spawn_info_all: dict = {}
    try:
        _si_f = PROJECTS_DIR / proj_id / ".spawn-info-by-session.json"
        if _si_f.exists():
            _spawn_info_all = json.loads(_si_f.read_text())
    except Exception:
        pass
    _proj_dir_for_ctx = _get_project_dir(proj_id)
    _ctx_enc = _encode_cwd_for_claude(str(_proj_dir_for_ctx)) if _proj_dir_for_ctx else None
    sessions = []
    for (sn, ct, at), summary in zip(targets, summaries):
        _si = _spawn_info_all.get(sn, {})
        _live_sid = _si.get("live_session_id", "")
        # M1762-c: fallback when spawn-info has no live_session_id.
        # resume mode does NOT create a new .jsonl — it continues from_id directly.
        # fork/fresh/continue modes create a new .jsonl; scan for it excluding (1) from_id
        # and (2) live_session_ids already claimed by sibling sessions.
        if not _live_sid:
            _si_mode = (_si or {}).get("mode", "")
            _si_from = (_si or {}).get("from_id", "")
            if _si_mode == "resume" and _si_from:
                _live_sid = _si_from  # resume = continues existing conversation, live_id = from_id
            elif _ctx_enc:
                try:
                    _td = Path.home() / ".claude" / "projects" / _ctx_enc
                    _from_stem = _si_from.split("-")[0] if _si_from else ""
                    _claimed = {v.get("live_session_id","")[:8] for v in _spawn_info_all.values() if v.get("live_session_id")}
                    _candidates = [
                        _f for _f in _td.glob("*.jsonl")
                        if _f.stat().st_mtime > ct
                        and (_from_stem == "" or not _f.stem.startswith(_from_stem))
                        and _f.stem[:8] not in _claimed
                    ]
                    if len(_candidates) == 1:
                        _live_sid = _candidates[0].stem
                    elif len(_candidates) > 1 and _si_mode == "fresh":
                        _live_sid = max(_candidates, key=lambda f: f.stat().st_mtime).stem
                except Exception:
                    pass
        sessions.append({
            "name": sn, "session": sn,
            "created": ct, "activity": at,
            "summary": summary or "(no output)",
            "spawn_mode": _si.get("mode", ""),
            "from_id": _si.get("from_id", ""),
            "live_session_id": _live_sid,
            "spawn_model": _si.get("model", ""),
        })
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
        r = subprocess.run(["tmux", "kill-session", "-t", f"={exec_session}"], capture_output=True, timeout=5)
        if r.returncode == 0:
            killed["tmux"] = True
            break
    return JSONResponse({"ok": True, "killed": killed,
                         "next_spawn_model": _get_project_model_value(proj_id) or "(default)"})


_SHELLS = {"bash", "zsh", "sh", "fish", "dash"}
# M1609 v2: _IDLE_HARNESS_CMDS is defined at L4604 (_ALLOWED_AGENTS.copy()).
# M1635-fix: removed stale placeholder set() that was overwriting the real value.


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
    # M1324-P1: wrap blocking urlopen in asyncio.to_thread — prevents event loop stall
    def _do_osk_check():
        t0 = time.time()
        req = urllib.request.Request(_OSK_PROXY_URL + "/health/liveliness")
        with urllib.request.urlopen(req, timeout=1.0) as r:
            return r.status == 200, int((time.time() - t0) * 1000)
    try:
        ok, latency = await asyncio.to_thread(_do_osk_check)
        out["ok"] = ok
        out["latency_ms"] = latency
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
    # M1324-P1: wrap blocking urlopen in asyncio.to_thread — prevents event loop stall
    def _do_dsk_check():
        t0 = time.time()
        req = urllib.request.Request(_DSK_PROXY_URL + "/health")
        with urllib.request.urlopen(req, timeout=2.0) as r:
            return r.status == 200, int((time.time() - t0) * 1000)
    try:
        ok, latency = await asyncio.to_thread(_do_dsk_check)
        out["ok"] = ok
        out["latency_ms"] = latency
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
async def _start_compress_scanner():
    """M1253 P1: periodic scan of all stones for missed compress triggers (token/time/explicit).
    Runs every 30 min, complements the queued→pending_confirmation hook in PATCH handler."""
    import asyncio as _aio
    async def _scan():
        while True:
            await _aio.sleep(1800)  # 30 minutes
            try:
                if not PROJECTS_DIR.is_dir():
                    continue
                for proj_dir in PROJECTS_DIR.iterdir():
                    if not proj_dir.is_dir():
                        continue
                    proj_id = proj_dir.name
                    proj = _db_load_project(proj_id)
                    if not proj:
                        continue
                    ms = proj.get("milestones") or []
                    if not isinstance(ms, list) or not ms:
                        continue
                    dirty = False
                    for stone in list(ms):
                        if not isinstance(stone, dict):
                            continue
                        if stone.get("done") or str(stone.get("category") or "").startswith("meta/"):
                            continue
                        reason = _compress_trigger_reason(stone)
                        if reason and (reason != "length" or (stone.get("layer") or 0) > 0):
                            # M1621: child stones (layer>0) missed length trigger (inline PATCH had layer==0 gate).
                            # Scanner now covers length for child stones; inline PATCH covers layer-0 real-time.
                            _maybe_queue_compress(proj_id, stone, ms, reason_override=reason)
                            dirty = True
                    if dirty:
                        proj["milestones"] = ms
                        _save_project(proj_id, proj)
            except Exception:
                pass  # never crash the scanner
    _aio.create_task(_scan())


@app.on_event("startup")
async def _start_exec_idle_detector():
    """Server-side background task: detect exec session idle transitions every 5s.
    Previously relied solely on client /api/exec-sessions polling — broke when browser closed."""
    import asyncio as _aio, re as _re
    async def _arun_detect(*cmd, timeout=2):
        """M1493: async subprocess for idle detector — avoids blocking event loop every 5s."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return stdout.decode("utf-8", errors="replace") if stdout else ""
        except Exception:
            return ""

    async def _detect():
        while True:
            await _aio.sleep(5)
            try:
                # M1493: async subprocess — was blocking event loop every 5s with 8×3 subprocess.run()
                _ls_raw = await _arun_detect("tmux", "list-sessions", "-F", "#{session_name}:#{session_created}")
                alive = set()
                _session_entries = []
                for line in _ls_raw.splitlines():
                    parts = line.split(":", 1)
                    session_name = parts[0] if parts else ""
                    if not any(session_name.startswith(p) for p in ("claude-exec-", "codex-exec-", "openrouter-exec-", "dsk-exec-")):
                        continue
                    # M1681: was a naive prefix strip (session_name[len(pfx):]) — for a branched
                    # child like "claude-exec-FromScratch-5752fa2c" this yielded a WRONG proj_id of
                    # "FromScratch-5752fa2c" (branch suffix included), breaking DB/ownership lookups
                    # keyed on proj_id. _parse_exec_session_name does the same longest-prefix-match
                    # against known project dirs used everywhere else (get_exec_sessions, poller) —
                    # use it here too for a correctly resolved proj_id.
                    # M1718-c: the notification TITLE below intentionally uses the full session_name
                    # (not this resolved proj_id) so a child's idle notification is distinguishable
                    # from the mother's — e.g. "FromScratch-5752fa2c exec idle" vs "FromScratch exec
                    # idle" is the desired/correct display now, by user request. Not a regression of
                    # this fix — proj_id here still must stay correctly resolved for DB lookups.
                    _agent_eid, proj_id = _parse_exec_session_name(session_name)
                    if not proj_id:
                        continue
                    alive.add(session_name)
                    _session_entries.append((session_name, proj_id))

                # M1493v2: gather all sessions in parallel (was sequential — 8×2×8ms = 128ms/tick)
                async def _check_one(session_name, proj_id):
                    # Check if runtime is alive
                    pane_cmds_raw = await _arun_detect("tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}")
                    pane_cmds = pane_cmds_raw.splitlines()
                    # M916 fix: only skip sessions that NEVER ran (not in _exec_was_running)
                    _has_been_running = session_name in _exec_was_running
                    if not _has_been_running and not any(c.strip() and c.strip() not in {"bash","zsh","sh","fish","dash"} for c in pane_cmds):
                        return session_name, proj_id, None
                    # M1533 v5: OOB-first idle detection — pane-scrape removed.
                    # OOB busy state is authoritative; fall back to file-idle + idle_count (set by stop-hook / 2-cycle).
                    # M1574: TTL-aware — _oob_is_busy returns False when stale (>120s) → idle=True
                    # M1656-fix (option②): pass session_name — this session's own report is authoritative,
                    # so an idle branched session no longer shows busy just because a sibling is running.
                    # M1806: was still calling _oob_is_busy directly, whose sibling-scan fallback (no
                    # TTL) applies whenever this session has no record of its own — the exact bug
                    # M1771 fixed for the OTHER idle-display call site, unfixed here.
                    idle = not _oob_is_busy_session_scoped(proj_id, session_name)
                    return session_name, proj_id, idle

                _results = await _aio.gather(*[_check_one(sn, pid) for sn, pid in _session_entries])

                for session_name, proj_id, idle in _results:
                    if idle is None:
                        continue
                    # M396/M1370: busy = spinner only. "esc to interrupt" appears in claude's
                    # status bar even when idle, so it cannot be used as a busy signal.
                    # M389/M434: fire Telegram ONLY on running→idle transition (same state as toast)
                    _was_running = _exec_was_running.get(session_name, False)
                    if not idle:
                        _exec_was_running[session_name] = True
                        _exec_idle_count.pop(session_name, None)
                        _exec_idle_file(proj_id).unlink(missing_ok=True)  # M536: clear when busy
                    else:
                        # M1656-fix: keep incrementing on idle — the queue-continuation poller
                        # (L4448) requires idle_count >= 1 to dispatch when .exec-idle file is
                        # absent. M1655 wrongly popped here ("debounce only"), starving the poller.
                        _exec_idle_count[session_name] = _exec_idle_count.get(session_name, 0) + 1
                    # M1655: 2-cycle debounce removed — OOB-first detection is authoritative (1-shot).
                    # pane-scrape flicker was the reason for debounce; it's gone now.
                    if idle:
                        if _was_running:
                            _exec_was_running[session_name] = False
                            _exec_notified[session_name] = time.time()  # M1171 Fix B: prevent browser-poll duplicate
                            _push_session_idle(session_name, proj_id)  # SSE toast
                            # M1171 v2: only notify when queue is empty — suppress mid-batch notifications
                            # M1718-b: session-scoped, not project-wide — a child session with no
                            # claimable work left is genuinely idle even if a sibling/mother queue isn't.
                            # M1718-c: label strips exec- prefix — mother shows "HugwartsBanana"
                            # (clean, unchanged), child shows "HugwartsBanana-b5756107" (distinguishable).
                            if not _session_has_queued_stones(proj_id, session_name):
                                _lbl = _idle_notify_label(session_name, proj_id)
                                _send_ntfy_notification(
                                    f"{_lbl} exec idle",
                                    f"Exec session for {_lbl} just went idle",
                                    priority="default"
                                )
                        elif session_name not in _exec_notified:
                            # M1635: hub restart clears _exec_was_running — push once to reset stale exec-live UI.
                            _exec_notified[session_name] = time.time()
                            _push_session_idle(session_name, proj_id)
                # Clean up stale entries — M460 fix: notify if session disappears while running
                for k in list(_exec_was_running.keys()):
                    if k not in alive:
                        was_running = _exec_was_running.pop(k, False)
                        _exec_idle_count.pop(k, None)
                        if was_running:
                            # Session died while running → fire missed idle notification
                            # M1681: same naive-strip bug as above — use _parse_exec_session_name
                            # so branched session names resolve to the bare proj_id, not proj+suffix.
                            _agent_dc, _proj = _parse_exec_session_name(k)
                            if _proj:
                                _push_session_idle(k, _proj)
                                # M1718-b: session-scoped (k = the session that just died)
                                # M1718-c: clean label (see _idle_notify_label)
                                if not _session_has_queued_stones(_proj, k):
                                    _lbl2 = _idle_notify_label(k, _proj)
                                    _send_ntfy_notification(
                                        f"{_lbl2} exec idle",
                                        f"Exec session for {_lbl2} just went idle",
                                        priority="default"
                                    )
            except Exception:
                pass
    # M460 cold-start fix: pre-scan existing exec sessions at startup.
    # Sessions already running → mark _exec_was_running=True so we catch their next idle.
    # M1635 Stage-1: spinner capture-pane scan replaced with OOB state (hydrated from SQLite
    # at L747 startup handler, which runs before this one — registration order guarantees it).
    # This removes the LAST content-based pane scrape from the idle-detection path.
    try:
        _cs_result = subprocess.run(
            ["tmux", "list-sessions", "-F", "#{session_name}"],
            capture_output=True, text=True, timeout=2)
        for _cs_sname in _cs_result.stdout.splitlines():
            _cs_proj = ""
            for _cs_pfx in ("claude-exec-", "codex-exec-", "openrouter-exec-", "dsk-exec-"):
                if _cs_sname.startswith(_cs_pfx):
                    _cs_proj = _cs_sname[len(_cs_pfx):]
                    break
            if not _cs_proj:
                continue
            if _oob_is_busy(_cs_proj):
                _exec_was_running[_cs_sname] = True  # currently running → will detect idle
    except Exception:
        pass
    asyncio.create_task(_detect())


@app.on_event("startup")
async def _startup_db_warmup():
    """M1452c: Pre-warm SQLite page cache on startup to eliminate first-request 150-200ms cold start.
    Reads project_meta + milestones_store once so the OS page cache is hot before any client request."""
    import asyncio as _aio
    async def _warm():
        await _aio.sleep(1)  # let other startup hooks finish first
        try:
            conn = sqlite3.connect(str(_NS_EVENTS_DB))
            # Touch project_meta and milestones_store to warm OS page cache
            conn.execute("SELECT COUNT(*) FROM project_meta").fetchone()
            conn.execute("SELECT COUNT(*) FROM milestones_store").fetchone()
            conn.execute("SELECT COUNT(*) FROM concept_graph_nodes").fetchone()
            conn.execute("SELECT COUNT(*) FROM user_settings").fetchone()
            conn.close()
        except Exception:
            pass
    asyncio.create_task(_warm())


def _push_session_idle(session_name: str, proj_id: str) -> bool:
    """M378: Deduplicated session_idle push. Returns True if pushed, False if suppressed by cooldown."""
    now = time.time()
    last = _last_idle_push.get(session_name, 0)
    if now - last < _IDLE_PUSH_COOLDOWN:
        return False  # suppress duplicate
    _last_idle_push[session_name] = now
    # M1677: invalidate exec-sessions cache on state transition — SSE-triggered client
    # refresh must not receive a pre-transition snapshot (cache is fork-cost protection
    # for steady-state polling, not a freshness ceiling).
    _exec_sessions_cache["ts"] = 0.0
    _ns_push("session_idle", proj_id=proj_id, kind="exec")
    return True


_exec_sessions_cache: dict = {"ts": 0.0, "data": None}
_EXEC_SESSIONS_TTL = 5.0  # M1826-v2: 5s cache — UI polls every 8s; 2s was always-cold vs 8s browser poll
# M1826-v2: background-refreshed tmux state — avoids 30ms subprocess fork at request time.
# Updated every 3s by _bg_tmux_poller; handler reads synchronously (zero subprocess cost).
_bg_tmux_state: dict = {"ls": "", "lp": "", "ts": 0.0}
_BG_TMUX_INTERVAL = 3.0  # seconds between background list-sessions + list-panes refreshes
# M1826-v2: permanent per-session live_session_id cache — immutable once resolved, avoids
# repeated 40ms transcript-dir scans (the dominant cold-path bottleneck).
_live_sid_cache: dict[str, str] = {}  # session_name → live_session_id

@app.get("/api/exec-sessions")
async def get_exec_sessions():
    """Return agent-exec-* tmux sessions where the runtime is actually running."""
    # M1493: serve cached response if within TTL — fork() is 40-150ms per subprocess in busy server
    _now = time.monotonic()
    if _exec_sessions_cache["data"] is not None and (_now - _exec_sessions_cache["ts"]) < _EXEC_SESSIONS_TTL:
        return JSONResponse(_exec_sessions_cache["data"])

    _t_start = time.perf_counter()

    # M1826-v2: use background-refreshed tmux state (updated every 3s by _start_bg_tmux_poller).
    # Falls back to on-demand subprocess if background state is stale (>10s, e.g. startup).
    _bg_age = time.monotonic() - _bg_tmux_state.get("ts", 0.0)
    if _bg_age < 10.0 and _bg_tmux_state.get("ls"):
        _ls_out = _bg_tmux_state["ls"]
        _lp_out = _bg_tmux_state["lp"]
    else:
        # Background poller not yet ready or stale — fall back to on-demand subprocess
        try:
            async def _run(*cmd):
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3)
                return stdout.decode("utf-8", errors="replace") if stdout else ""
            _ls_out, _lp_out = await asyncio.gather(
                _run("tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"),
                _run("tmux", "list-panes", "-a", "-F", "#{session_name} #{pane_current_command}"),
            )
        except Exception:
            _ls_out = _lp_out = ""

    # Build session → pane-commands map from the single batch call
    _batch_pane_cmds: dict[str, list[str]] = {}
    for _pl in (_lp_out or "").splitlines():
        _pp = _pl.split(" ", 1)
        if len(_pp) == 2:
            _batch_pane_cmds.setdefault(_pp[0], []).append(_pp[1].strip())

    # Parse session lines and filter to exec sessions only
    # M1656-④: _parse_exec_session_name does longest-prefix match for branched child names
    _session_lines = []
    for line in (_ls_out or "").splitlines():
        parts = line.split(":", 2)
        session_name = parts[0] if parts else ""
        agent, proj_id = _parse_exec_session_name(session_name)
        if not proj_id:
            continue
        created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        _session_lines.append((session_name, agent, proj_id, created_ts))

    # M1826-v2: batch SQLite project_meta query — single query for all unique proj_ids,
    # replaces N concurrent sqlite3.connect() calls (was ~11ms for 12 sessions).
    _unique_proj_ids = list({pid for _, _, pid, _ in _session_lines})
    _batch_model: dict[str, str] = {}
    if _unique_proj_ids:
        try:
            def _batch_meta_query():
                _bc = sqlite3.connect(str(_NS_EVENTS_DB))
                _ph = ",".join("?" * len(_unique_proj_ids))
                _rows = _bc.execute(
                    f"SELECT proj_id, meta_json FROM project_meta WHERE proj_id IN ({_ph})",
                    _unique_proj_ids
                ).fetchall()
                _bc.close()
                return _rows
            _meta_rows = await asyncio.to_thread(_batch_meta_query)
            for _mpid, _mjson in _meta_rows:
                try:
                    _mm = json.loads(_mjson)
                    _mv = (_mm.get("model") or "").strip()
                    if _mv in _ALLOWED_MODELS:
                        _batch_model[_mpid] = _mv
                except Exception:
                    pass
        except Exception:
            pass

    # M1234-C2: process each session concurrently via asyncio.gather
    async def _process_session(session_name, agent, proj_id, created_ts):
        import re as _re
        from datetime import datetime as _dt

        # Check if the runtime is actually running — not just a shell prompt after exit
        # M1826: use pre-fetched batch pane map instead of per-session subprocess
        pane_cmds = _batch_pane_cmds.get(session_name, [])
        cmds = [c.strip() for c in pane_cmds if c.strip()]
        runtime_running = any(c not in _SHELLS for c in cmds) if cmds else False
        # M1609: harness-agnostic idle-at-prompt detection.
        # If ALL pane cmds are known idle-at-prompt processes (_IDLE_HARNESS_CMDS) and OOB is stale,
        # the session IS alive but the harness is waiting at prompt → keep in exec-sessions as idle=True.
        # Do NOT set runtime_running=False here (that would return None and remove the session from UI).
        _harness_at_prompt = cmds and all(c in _IDLE_HARNESS_CMDS for c in cmds)
        if not runtime_running:
            # M345 v2: fire ntfy when tracked session exits — check key EXISTENCE not value
            _was_tracked = session_name in _exec_was_running
            _exec_was_running.pop(session_name, None)
            _exec_idle_count.pop(session_name, None)
            if _was_tracked:
                _ns_push("session_idle", proj_id=proj_id, kind="exec")
                _send_ntfy_notification(f"{proj_id} exec done", f"Exec session for {proj_id} finished/exited", priority="high")
            return None

        # M1656-R: DISPLAY is strictly per-session — a busy sibling must not make this
        # session look busy. Session has a record → its own state; no record → the
        # pane-activity / at-prompt heuristics below decide.
        # M1771: _oob_is_busy's session-name branch only trusts a record for THIS session;
        # with none, it falls through to "any busy session of the project" — the exact
        # sibling-contamination the comment above says must not happen (same bug class
        # M1678 already fixed for wake dispatch: a cold-start fork with zero hook events
        # yet showed idle=false purely because its mother happened to be busy). A session
        # that has never reported has no busy signal of its own — treat as idle here;
        # the pane-active/at-prompt heuristics below still get their say via _oob_rec.
        if session_name in _agent_busy_sessions:
            idle = not _session_is_busy(session_name)
        else:
            idle = True
        # M1609: harness at prompt → force idle=True regardless of OOB (harness is alive but not executing)
        # M1635/M1656-R: skip override when THIS session has an in-flight stone
        # M1677: also skip for explicit busy signals (wake_sent/compacting/tool_start/
        # subagent_running) — pane_current_command is ALWAYS the harness binary for
        # claude sessions, so _harness_at_prompt cannot distinguish working from
        # at-prompt; without this exemption the pane showed idle from wake until the
        # stone claim (~14s), and (M131-d) showed idle while a background subagent
        # was still computing on the session's behalf.
        _own_rec = _agent_busy_sessions.get(session_name) or {}
        _own_busy_hold = _session_is_busy(session_name) and (
            _own_rec.get("stone_id")
            or _own_rec.get("reason") in ("wake_sent", "compacting", "tool_start", "subagent_running"))
        if _harness_at_prompt and not _own_busy_hold:
            _oob_chk = _agent_busy_state.get(proj_id)
            _oob_fresh = _oob_chk and _oob_chk.get("busy") and (time.time() - _oob_chk.get("ts", 0)) < _OOB_STALE_SECS
            if not _oob_fresh:
                idle = True

        # M1602 v2: pane-active fallback — only when OOB was recently busy (within TTL).
        # M1656-R: applies ONLY to sessions with NO own record — a session that reports
        # its own state must not be flipped busy by the proj-level record (which reflects
        # whichever sibling posted last, e.g. a busy child's heartbeat).
        # M1797: exclude sessions at harness prompt — definitionally idle; OOB busy here is
        # sibling contamination (e.g. mother busy while fork sits at prompt).
        if idle and runtime_running and session_name not in _agent_busy_sessions and not _harness_at_prompt:
            _oob_rec = _agent_busy_state.get(proj_id)
            _oob_age = (time.time() - _oob_rec.get("ts", 0)) if _oob_rec else float("inf")
            _oob_explicit_idle = _oob_rec and not _oob_rec.get("busy", True)
            if _oob_age < _OOB_STALE_SECS and not _oob_explicit_idle:  # OOB active within TTL → trust pane-active
                idle = False

        # M1798: M388's 30s startup-grace force-busy removed. It was fully redundant with
        # _send_exec_wake's own wake_sent optimistic busy record (M1675, 45s) for the real
        # dispatch case — that record already exists the instant wake is injected, so idle
        # never leaked through in that scenario even without this grace. The ONE case where
        # M388 actually did something was M1790's idle-spawn path (▶ click, zero queued
        # work) — there wake is deliberately never sent (M1790), so this grace was the sole
        # reason a genuinely idle just-spawned session showed busy for 30s, directly
        # defeating M1790's whole point. It had also already caused a real bug once before
        # (M1717-b: a child's spawn could force-lock an unrelated idle mother's avatar into
        # 'running' for up to 30s) — a pattern of overreach requiring follow-up patches.

        # Spawn info / model
        spawn_mode = None
        spawn_from = None
        spawn_model = ""
        si = _read_spawn_info(proj_id, session_name)
        if si:
            spawn_mode = si.get("mode")
            spawn_from = si.get("from_id") or None
            spawn_model = si.get("model") or ""
        if not spawn_model:
            # M1826-v2: use pre-fetched batch model lookup (single SQLite query for all sessions)
            spawn_model = _batch_model.get(proj_id, "")
        if not spawn_model:
            try:
                # M1493: cache settings.json model at module level to avoid per-session file reads
                if not hasattr(_process_session, "_settings_model_cache"):
                    _process_session._settings_model_cache = None
                    _process_session._settings_model_ts = 0
                _cc = Path.home() / ".claude" / "settings.json"
                _cc_mtime = _cc.stat().st_mtime if _cc.exists() else 0
                if _cc_mtime != _process_session._settings_model_ts:
                    _process_session._settings_model_cache = json.loads(_cc.read_text()).get("model", "") if _cc.exists() else ""
                    _process_session._settings_model_ts = _cc_mtime
                spawn_model = _process_session._settings_model_cache or ""
            except Exception:
                pass

        # v0.2.4 idle debounce + SSE events (M378/M998/M1171)
        _was_running = _exec_was_running.get(session_name, False)
        _exec_was_running[session_name] = not idle
        if idle:
            _exec_idle_count[session_name] = _exec_idle_count.get(session_name, 0) + 1
        else:
            _exec_idle_count.pop(session_name, None)
        _consec_idle = _exec_idle_count.get(session_name, 0)
        if idle and (_was_running and _consec_idle >= 2 or _consec_idle >= 3):
            if session_name not in _exec_notified:
                _push_session_idle(session_name, proj_id)
                # M1718-b: session-scoped suppression check (see _session_has_queued_stones)
                # M1718-c: clean label (see _idle_notify_label) — distinguishes children, mother unchanged
                if not _session_has_queued_stones(proj_id, session_name):
                    _lbl3 = _idle_notify_label(session_name, proj_id)
                    _send_ntfy_notification(f"{_lbl3} exec idle", f"Exec session for {_lbl3} just went idle", priority="default")
                _exec_notified[session_name] = time.time()
        elif not idle:
            _exec_notified.pop(session_name, None)
        if not _was_running and not idle:
            _exec_idle_count.pop(session_name, None)
            _ns_push("session_running", proj_id=proj_id, kind="exec")

        # M365/M1656-R9: live session id — prefer the session-scoped value captured at
        # spawn time (unambiguous diff-based detection, see _capture_live_session_id_bg).
        # Directory-scan fallback ("newest mtime file") is kept ONLY for sessions spawned
        # before this fix / where the diff was ambiguous — it is known to mis-attribute a
        # SIBLING session's transcript when multiple exec sessions share one project
        # directory and write concurrently (root cause of the mother/child pane mismatch).
        _live_session_id = (si or {}).get("live_session_id", "") or _live_sid_cache.get(session_name, "")
        # M1826-v2: skip transcript scan if sentinel ("") already set (previously scanned, no match found)
        _skip_scan = session_name in _live_sid_cache
        if not _live_session_id and not _skip_scan:
            try:
                _base = Path.home() / ".claude" / "projects"
                # M1766: prefer exact encoding via _get_project_dir to avoid picking up
                # sibling dirs like HugwartsBanana2 when project is VIDraft/HugwartsBanana.
                _proj_dir_exact = _get_project_dir(proj_id)
                _transcript_dir = None
                if _proj_dir_exact:
                    _enc_exact = _encode_cwd_for_claude(str(_proj_dir_exact))
                    _exact_path = _base / _enc_exact
                    if _exact_path.exists():
                        _transcript_dir = _exact_path
                if not _transcript_dir:
                    _tdirs = [d for d in _base.iterdir() if d.name.lower().endswith(f"-project-{proj_id.lower()}")]
                    if not _tdirs:
                        _tdirs = [d for d in _base.iterdir() if proj_id.lower() in d.name.lower() and "project" in d.name.lower()]
                    if _tdirs:
                        _transcript_dir = sorted(_tdirs, key=lambda d: len(d.name))[0]
                if _transcript_dir:
                    # M1762-c: resume mode continues from_id (no new .jsonl created).
                    # fork/fresh scan for new .jsonl excluding from_id + sibling-claimed IDs.
                    _si_from = (si or {}).get("from_id", "")
                    _si_stored_live = (si or {}).get("live_session_id", "")
                    if (si or {}).get("mode") == "resume" and _si_from:
                        _live_session_id = _si_from
                    elif _si_stored_live:
                        # Background capture already resolved it (fork/fresh both write here)
                        _live_session_id = _si_stored_live
                    else:
                        _from_stem = _si_from.split("-")[0] if _si_from else ""
                        try:
                            _si_all_fb = json.loads((PROJECTS_DIR / proj_id / ".spawn-info-by-session.json").read_text())
                            _claimed = {v.get("live_session_id","")[:8] for v in _si_all_fb.values() if v.get("live_session_id")}
                        except Exception:
                            _claimed = set()
                        _new_files = [
                            _f for _f in _transcript_dir.glob("*.jsonl")
                            if _f.stat().st_mtime > created_ts
                            and (_from_stem == "" or not _f.stem.startswith(_from_stem))
                            and _f.stem[:8] not in _claimed
                        ]
                        if len(_new_files) == 1:
                            _live_session_id = _new_files[0].stem
                        elif len(_new_files) > 1 and (si or {}).get("mode") == "fresh":
                            # M1766: fresh sessions with ambiguous scan — pick most-recently
                            # written transcript (the active one is writing, others are idle)
                            _live_session_id = max(_new_files, key=lambda f: f.stat().st_mtime).stem
            except Exception:
                pass
        # M1826-v2: cache resolved live_session_id permanently — immutable once known,
        # avoids repeated transcript-dir glob+stat scans (dominant cold-path bottleneck).
        # Also cache empty string ("") to skip future scans for sessions that had no
        # matching .jsonl yet — sentinel prevents redundant re-scans every cold poll.
        if session_name not in _live_sid_cache or _live_session_id:
            _live_sid_cache[session_name] = _live_session_id  # "" = already scanned, nothing found

        # M1754-revert: spawn_model is user intent (what was requested at dispatch time).
        # Transcript model cannot be trusted for openrouter sessions — OpenRouter returns
        # claude-sonnet-5 as the model ID in responses even when hy3:free was requested,
        # so _detect_session_model would misreport the session model. spawn_model is ground truth.

        # M1656-R: expose accurate per-session state for UI (what stone, why busy)
        _rec = _agent_busy_sessions.get(session_name) or {}
        # M1881: activity = transcript mtime (matches resumeRows sort key in detail panel).
        # Allows avatar stack to use the same tie-break as the session list → order stays in sync.
        _activity = 0.0
        if _live_session_id:
            try:
                _proj_dir_a = _get_project_dir(proj_id)
                if _proj_dir_a:
                    _tf = Path.home() / ".claude" / "projects" / _encode_cwd_for_claude(str(_proj_dir_a)) / f"{_live_session_id}.jsonl"
                    if _tf.exists():
                        _activity = _tf.stat().st_mtime
            except Exception:
                pass
        return {
            "session": session_name,
            "proj_id": proj_id,
            "agent": agent,
            "created": _dt.fromtimestamp(created_ts).isoformat() if created_ts else "",
            "created_ts": created_ts,
            "alive": True,
            "idle": idle,
            "busy_reason": _rec.get("reason", "") if not idle else "",
            "running_stone": (_validate_stone_hold(session_name, _rec) if _session_is_busy(session_name) else None),
            "spawn_mode": spawn_mode,
            "spawn_from": spawn_from,
            "live_session_id": _live_session_id,
            "model": spawn_model,
            "activity": _activity,
        }

    # Run all sessions in parallel
    _results = await asyncio.gather(*[
        _process_session(sn, ag, pid, cts) for sn, ag, pid, cts in _session_lines
    ])
    sessions = [r for r in _results if r is not None]

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
    # M1158: also include all tmux sessions (main, branch, etc.) for sess badge
    # M1826: reuse _ls_out from batch query above — no extra subprocess needed.
    # M1826-fix: `p` is not in scope here (was NameError, silently caught). Return all
    # exec-pattern sessions unfiltered; the UI filters by proj_id client-side.
    _all_sx = [l.split(":")[0] for l in (_ls_out or "").splitlines() if l.split(":")[0]]
    _t_total = (time.perf_counter() - _t_start) * 1000
    if _t_total > 100:
        print(f"[M1493] exec-sessions handler: {_t_total:.0f}ms (sessions={len(sessions)})", flush=True)
    _resp_data = {"ok": True, "sessions": sessions, "all_sessions": _all_sx}
    _exec_sessions_cache["data"] = _resp_data
    _exec_sessions_cache["ts"] = time.monotonic()
    return JSONResponse(_resp_data)


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

    # M1806: build spawn-info sid→model reverse lookup for high-fidelity model detection.
    # spawn-info records the model that was explicitly passed at spawn time (--model flag),
    # which is immune to --resume context inheritance contamination. Used as primary source;
    # _detect_session_model() is fallback when no spawn-info entry exists for a sid.
    _spawn_sid_to_model: dict[str, str] = {}
    _spawn_info_file = pdir / ".spawn-info-by-session.json"
    if _spawn_info_file.exists():
        try:
            _spawn_info_all = json.loads(_spawn_info_file.read_text())
            for _si_model_val in _spawn_info_all.values():
                _si_m = (_si_model_val.get("model") or "").strip() if isinstance(_si_model_val, dict) else ""
                if not _si_m:
                    continue
                # M1806-fix: index only live_session_id; resume mode now sets live=from_id
                # so from_id indexing is no longer needed.
                # Skip fork entries where live_session_id == from_id — _capture_live_session_id_bg
                # falls back to from_id when it can't distinguish new file; indexing it would
                # map the parent session ID → child model, shadowing the parent's own entry.
                _si_sid = (_si_model_val.get("live_session_id") or "").strip()
                _si_fid = (_si_model_val.get("from_id") or "").strip()
                _si_mode = (_si_model_val.get("mode") or "").strip()
                if (_si_mode == "fork" and _si_sid == _si_fid):
                    continue  # unresolved fork capture — skip to avoid parent ID collision
                if _si_sid and len(_si_sid) > 10:
                    _spawn_sid_to_model[_si_sid] = _si_m
        except Exception:
            pass

    # Read live exec sessions + capture spawn time for transcript scan
    # M1498: subprocess.run blocks the async event loop; wrap in asyncio.to_thread
    # M1765: track ALL live sessions per agent (main + fork children), not just one
    # M1821-P2: single list-panes -a batch replaces per-session subprocess loop.
    # M1852-①: reuse _bg_tmux_state (3s cached) instead of spawning own subprocess.
    live_by_agent = set()
    live_sessions_by_agent: dict[str, list[dict]] = {}  # agent → [{name, created_ts}]
    try:
        _bg_age_rs = time.monotonic() - _bg_tmux_state.get("ts", 0.0)
        if _bg_age_rs < 10.0 and _bg_tmux_state.get("ls"):
            _ls_raw = _bg_tmux_state["ls"]
            _lp_raw = _bg_tmux_state["lp"]
        else:
            def _tmux_batch_query():
                sess_r = subprocess.run(
                    ["tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"],
                    capture_output=True, text=True, timeout=3
                )
                pane_r = subprocess.run(
                    ["tmux", "list-panes", "-a", "-F", "#{session_name} #{pane_current_command}"],
                    capture_output=True, text=True, timeout=3
                )
                return sess_r.stdout, pane_r.stdout
            _ls_raw, _lp_raw = await asyncio.to_thread(_tmux_batch_query)
        # Build session_name → set(pane commands) map from the single batch call
        _sess_pane_cmds: dict[str, set[str]] = {}
        for _pl in (_lp_raw or "").splitlines():
            _parts = _pl.split(" ", 1)
            if len(_parts) == 2:
                _sess_pane_cmds.setdefault(_parts[0], set()).add(_parts[1].strip())
        for line in (_ls_raw or "").splitlines():
            parts = line.split(":", 2)
            sname = parts[0] if parts else ""
            created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            _sn_agent, _sn_proj = _parse_exec_session_name(sname)
            if _sn_agent and _sn_proj == proj_id:
                # M342 fix: only mark live if the runtime is actually running (not a dead shell)
                pane_cmds_set = _sess_pane_cmds.get(sname, set())
                runtime_running = any(c and c not in _SHELLS for c in pane_cmds_set)
                if runtime_running:
                    live_by_agent.add(_sn_agent)
                    live_sessions_by_agent.setdefault(_sn_agent, []).append({
                        "name": sname, "created_ts": float(created_ts)
                    })
    except Exception:
        pass

    # M280/M1765: resolve live_session_id for each live tmux session.
    # Uses spawn-info first (M1762-c logic), falls back to transcript scan.
    # M1765: produces list per agent so fork children also appear in resume dropdown.
    live_exec_sessions: dict[str, list[dict]] = {}  # agent → [{tmux_session, live_sid}]
    if live_by_agent:
        try:
            proj_dir_path = _get_project_dir(proj_id)
            if proj_dir_path:
                encoded = _encode_cwd_for_claude(str(proj_dir_path))
                transcript_dir = Path.home() / ".claude" / "projects" / encoded
                # Load spawn-info for live_session_id resolution (M1762-c)
                _si_all: dict = {}
                try:
                    _si_all = json.loads((PROJECTS_DIR / proj_id / ".spawn-info-by-session.json").read_text())
                except Exception:
                    pass
                # Pre-collect agent SIDs for cross-agent exclusion (M1173/M1555)
                _or_sids = {v.strip() for k, v in hist.items() if k.startswith("or-")}
                _codex_sids = {v.strip() for k, v in hist.items() if k.startswith("codex-")}
                _claude_sids = {v.strip() for k, v in hist.items() if not k.startswith("or-") and not k.startswith("codex-")}
                for ag, _ag_sessions in live_sessions_by_agent.items():
                    for _sess_info in _ag_sessions:
                        sname = _sess_info["name"]
                        created_ts = _sess_info["created_ts"]
                        # Step 1: try spawn-info for this specific tmux session (M1762-c)
                        _si = _si_all.get(sname, {})
                        _si_mode = _si.get("mode", "")
                        _si_from = _si.get("from_id", "")
                        _live_sid = _si.get("live_session_id", "") or ""
                        if not _live_sid:
                            if _si_mode == "resume" and _si_from:
                                _live_sid = _si_from  # resume = continues same conversation
                            elif sname in _live_sid_cache and _live_sid_cache[sname]:
                                # M1852-②: reuse cache from get_all_sessions (avoids repeated glob)
                                _live_sid = _live_sid_cache[sname]
                            elif transcript_dir.exists():
                                # Step 2: scan transcript dir for new .jsonl (fork/fresh)
                                _claimed = {v.get("live_session_id","")[:8] for v in _si_all.values() if v.get("live_session_id")}
                                _from_stem = _si_from.split("-")[0] if _si_from else ""
                                _candidates = []
                                for f in transcript_dir.glob("*.jsonl"):
                                    mt = f.stat().st_mtime
                                    if mt < created_ts - 5 or f.stat().st_size == 0:
                                        continue
                                    fstem = f.stem
                                    if ag == "claude" and (fstem in _or_sids or fstem in _codex_sids):
                                        continue
                                    if ag == "openrouter" and (fstem in _codex_sids or fstem in _claude_sids):
                                        continue
                                    if ag == "codex" and (fstem in _or_sids or fstem in _claude_sids):
                                        continue
                                    if _from_stem and fstem.startswith(_from_stem):
                                        continue
                                    if fstem[:8] in _claimed:
                                        continue
                                    _candidates.append((mt, fstem))
                                if len(_candidates) == 1:
                                    _live_sid = _candidates[0][1]
                                    if _live_sid:
                                        _live_sid_cache[sname] = _live_sid  # M1852-②: populate shared cache
                        if _live_sid:
                            live_exec_sessions.setdefault(ag, []).append({
                                "tmux_session": sname, "live_sid": _live_sid,
                                "spawn_mode": _si_mode,  # M1774: preserve for fork detection
                            })
        except Exception:
            pass
    # Compat shim: live_exec_sid[ag] = first live sid (used by existing injection guard below)
    live_exec_sid: dict[str, str] = {
        ag: entries[0]["live_sid"] for ag, entries in live_exec_sessions.items() if entries
    }

    # Build groups per agent
    # M405: track which agents have had exec_live injected — inject only into first model group
    # M1080: global seen_sids per agent — prevent same session from appearing in multiple model groups
    # M1765: build cross-agent live_sid→tmux_session lookup so transcript rows get tmux_session set
    _live_sid_to_tmux: dict[str, str] = {}
    for _ag_entries in live_exec_sessions.values():
        for _e in _ag_entries:
            # M1774: skip fork entries — forks share live_sid with mother but should not
            # override the mother's tmux_session on the transcript row.
            if _e.get("live_sid") and _e.get("tmux_session") and _e.get("spawn_mode") != "fork":
                _live_sid_to_tmux[_e["live_sid"]] = _e["tmux_session"]
    _exec_live_injected: set[str] = set()
    for ag in agents:
        _global_seen_sids_for_agent: set[str] = set()
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
                {"key": "or-hy3", "label": "hy3 (free)"},
                {"key": "or-owl-alpha", "label": "owl-alpha (free)"},
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
            agent_models = [  # gitleaks:allow
                {"key": "", "label": "sonnet-4.6"},
                {"key": "claude-haiku-4-5-20251001", "label": "Haiku 4.5"},
                {"key": "claude-sonnet-4-5-20250929", "label": "Sonnet 4.5"},
                {"key": "claude-sonnet-4-6", "label": "Sonnet 4.6"},
                {"key": "claude-sonnet-5", "label": "Sonnet 5"},
                {"key": "claude-opus-4-7", "label": "Opus 4.7"},
                {"key": "claude-opus-4-8", "label": "Opus 4.8"},
                {"key": "claude-fable-5", "label": "Fable 5"},
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
                            "label": sid[:12],
                            "model": mkey,
                        })
            else:
                # Claude: check per-model and _current; deduplicate by session id
                # M217: exclude _interactive key from exec resume list — interactive sessions
                # M279: _current/_default only for claude agent — OR/codex have their own model keys
                seen_sids = set()
                # M1740: exclude foreign-agent SIDs from EVERY agent group (symmetric guard).
                # Claude group drops or-/codex- SIDs; openrouter/codex groups drop claude SIDs.
                # Prevents cross-agent contamination (e.g. a claude session surfacing under an
                # openrouter model bucket) even if session-history.json was already polluted.
                if ag == "claude":
                    _foreign_sids = {v.strip() for k, v in hist.items()
                                     if k.startswith("or-") or k.startswith("codex-")}
                elif ag == "openrouter":
                    _foreign_sids = {v.strip() for k, v in hist.items()
                                     if not k.startswith("or-") and not k.startswith("codex-")}
                elif ag == "codex":
                    _foreign_sids = {v.strip() for k, v in hist.items()
                                     if not k.startswith("codex-")}
                else:
                    _foreign_sids = set()
                if mkey:
                    # M1802: read the full 5-slot ring (was mkey + mkey_prev only, M316) — see
                    # _push_session_history_ring for the write side.
                    hist_keys = _session_history_slot_keys(mkey)
                else:
                    # M323: include _current/_default for empty model key (default claude session).
                    # M1876: also include the full claude-sonnet-4-6 5-slot ring — sessions spawned
                    # with --model claude-sonnet-4-6 write into that ring, not _current/_default,
                    # so they were invisible in the default "sonnet-4.6" bucket. This caused sessions
                    # like 77e38ee2 (stored in claude-sonnet-4-6_prev) to disappear from the UI even
                    # though they were alive and accessible via the explicit "Sonnet 4.6" bucket.
                    hist_keys = ["_current", "_default"] + _session_history_slot_keys("claude-sonnet-4-6")
                encoded_path = _encode_cwd_for_claude(str(_get_project_dir(proj_id) or str(Path.home())))
                for hist_key in hist_keys:
                    if hist_key == "_default" and mkey:
                        continue
                    sid = (hist.get(hist_key) or "").strip()
                    # M1740: skip SIDs that belong to a foreign agent (symmetric guard)
                    # M1080: also skip if already shown in another model group for this agent
                    if sid and sid not in seen_sids and sid not in _foreign_sids and sid not in _global_seen_sids_for_agent:
                        seen_sids.add(sid)
                        _global_seen_sids_for_agent.add(sid)
                        t = Path.home() / ".claude" / "projects" / encoded_path / f"{sid}.jsonl"
                        if t.exists() and t.stat().st_size > 0:
                            # M1071/M1519: always detect actual model from transcript so row
                            # label reflects runtime model, not historical bucket key. Fixes
                            # case where a sonnet-bucket session is restarted with opus —
                            # bucket key stays "sonnet-4.6" but transcript records opus calls.
                            # M1806: spawn-info is the highest-fidelity source — records the
                            # --model flag at spawn time, immune to --resume context inheritance.
                            # Falls back to _detect_session_model only when no spawn record exists.
                            _detected = _spawn_sid_to_model.get(sid) or _detect_session_model(t)
                            # M1760/M1750-c: OpenRouter upstream aliases hy3:free → claude-sonnet-5
                            # in transcript model field. When the bucket is an OR agent, a detected
                            # claude-* model is the upstream aliasing artifact — use the bucket key
                            # (or-*) instead so the session label shows the correct OR model name.
                            if ag == "openrouter" and _detected and not _detected.startswith("or-"):
                                _detected = None
                            _effective_model = _detected if _detected else mkey
                            _row: dict = {
                                "id": sid,
                                "type": "transcript",
                                "label": sid[:12],
                                "model": _effective_model,
                                "activity": t.stat().st_mtime,  # M1018: unix epoch for last-used timestamp
                            }
                            if sid in _live_sid_to_tmux:
                                _row["tmux_session"] = _live_sid_to_tmux[sid]
                            sessions.append(_row)

            # M280/M405/M1765: inject live exec session rows (main + fork children) into the
            # model group matching their actual runtime model (M1519).
            # M1765: iterate all live sessions for this agent, not just the first one.
            _known_keys = {x["key"] for x in agent_models}
            _proj_dir_for_live = _get_project_dir(proj_id)
            _proj_enc_live = _encode_cwd_for_claude(str(_proj_dir_for_live)) if _proj_dir_for_live else ""
            for _live_entry in live_exec_sessions.get(ag, []):
                live_sid = _live_entry["live_sid"]
                _tmux_sess = _live_entry["tmux_session"]
                # M1774: fork session shares live_sid with existing transcript row.
                # Only treat as fork if spawn_mode=fork (not resume/fresh which legitimately share live_sid).
                # M1784: previously emitted a SEPARATE "fork" type row alongside the existing
                # transcript row for the same live_sid, so the same session showed twice in the
                # resume list (once as "transcript", once as "fork") — filed as MOAT M1774
                # ("Universeye resume session list 에 중복되는 세션이 확인됨"). Now that the UI no
                # longer visually distinguishes fork rows (per-user direction: forks should just
                # be visible, not specially tagged), mutate the existing row in place instead of
                # inserting a duplicate.
                _spawn_mode = _live_entry.get("spawn_mode", "")
                _existing_row = next((s for s in sessions if s["id"] == live_sid), None) if _spawn_mode == "fork" else None
                _is_fork_of_existing = _existing_row is not None
                if _is_fork_of_existing:
                    if _tmux_sess not in _exec_live_injected:
                        _existing_row["tmux_session"] = _tmux_sess
                        _exec_live_injected.add(_tmux_sess)
                    continue
                if live_sid in _global_seen_sids_for_agent:
                    continue  # already shown in another model bucket for this agent (e.g. empty-model _current)
                if _tmux_sess in _exec_live_injected:
                    continue  # already injected this specific tmux session into some bucket
                # M1519: detect actual runtime model from live exec transcript
                # M1806: spawn-info takes priority over transcript scan for live sessions
                _live_model = mkey
                try:
                    _live_model_from_spawn = _spawn_sid_to_model.get(live_sid)
                    if _live_model_from_spawn:
                        _live_model = _live_model_from_spawn
                    elif _proj_enc_live:
                        _live_t = Path.home() / ".claude" / "projects" / _proj_enc_live / f"{live_sid}.jsonl"
                        if _live_t.exists() and _live_t.stat().st_size > 0:
                            _detected_live = _detect_session_model(_live_t)
                            if _detected_live and ag == "openrouter" and not _detected_live.startswith("or-"):
                                _detected_live = None
                            if _detected_live:
                                _live_model = _detected_live
                except Exception:
                    pass
                _bucket_match = (_live_model == mkey) or (not _live_model and not mkey)
                _detect_unknown = _live_model and _live_model not in _known_keys
                if _bucket_match or _detect_unknown:
                    # M1765: inject as transcript type (same form as other resume rows) but
                    # include tmux_session so UI can show process-alive dot without special styling.
                    # Populate activity from transcript mtime so date column renders correctly.
                    _inj_activity: float = 0.0
                    try:
                        if _proj_enc_live:
                            _inj_t = Path.home() / ".claude" / "projects" / _proj_enc_live / f"{live_sid}.jsonl"
                            if _inj_t.exists():
                                _inj_activity = _inj_t.stat().st_mtime
                    except Exception:
                        pass
                    _inj_row: dict = {
                        "id": live_sid,
                        "type": "transcript",
                        "label": live_sid[:12],
                        "model": _live_model or mkey,
                        "tmux_session": _tmux_sess,
                    }
                    # M1804: fallback to spawn time when transcript not yet written (brand-new session)
                    _inj_row["activity"] = _inj_activity if _inj_activity else time.time()
                    sessions.insert(0, _inj_row)
                    _global_seen_sids_for_agent.add(live_sid)
                    _exec_live_injected.add(_tmux_sess)  # M1765: key by tmux name, not agent

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


@app.get("/api/northstar/mobile-status")
async def ns_mobile_status():
    """M1899-R20: Mobile batch endpoint — sessions + exec-sessions in one HTTP round trip.
    Halves TCP connections vs Promise.all([fetch(sessions), fetch(exec-sessions)]).
    ROLLBACK: remove this endpoint; revert client to Promise.all of 2 fetches."""
    sess_resp = await ns_sessions()
    exec_resp = await get_exec_sessions()
    import json as _json
    return JSONResponse({
        "sessions": _json.loads(sess_resp.body),
        "exec": _json.loads(exec_resp.body),
    })


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


def _norm_session_model(raw: str) -> str:
    """M1550: normalize raw transcript model string → catalog key (or-*, claude-*, etc.)
    so the session list always stores a clean key instead of LiteLLM/proxy-specific strings.
    Priority: exact _ALLOWED_MODELS match → openrouter- prefix rewrite (openrouter-kimi-k2 → or-kimi-k2)
    → strip openrouter/ path prefix and try remaining slug against _OPENROUTER_MODELS.
    Falls back to raw if no match (claude models are already in catalog form)."""
    if not raw:
        return raw
    if raw in _ALLOWED_MODELS:
        return raw
    # LiteLLM proxy echoes "openrouter-kimi-k2" (the model_name alias) — rewrite to or-*
    if raw.startswith("openrouter-"):
        candidate = "or-" + raw[len("openrouter-"):]
        if candidate in _ALLOWED_MODELS:
            return candidate
    # Claude Code may emit full OpenRouter path "openrouter/provider/model-slug"
    if raw.startswith("openrouter/"):
        slug = raw.split("/")[-1].lower()
        # Match slug against or-* keys by stripping "or-" prefix
        for key in _OPENROUTER_MODELS:
            if key[3:] in slug or slug in key[3:]:
                return key
    # M1550-fix: bare "vendor/model" path (no openrouter/ prefix) stored from old sessions
    if "/" in raw:
        slug = raw.split("/")[-1].lower()
        for key in _OPENROUTER_MODELS:
            if key[3:] in slug or slug in key[3:]:
                return key
    return raw


# M1821-P1: mtime-keyed in-memory cache — avoids full JSONL re-read on every 15s poll.
# Key: str(path), Value: (mtime_float, detected_model_str). Invalidated on mtime change.
_session_model_cache: dict[str, tuple[float, str]] = {}


def _detect_session_model(transcript_path: Path, max_lines: int = 50, prefer_tail: bool = True) -> str:
    """Detect which model was used in a Claude session transcript.

    M1519: prefer_tail=True scans from the END of the transcript first so that
    sessions restarted with a different model report the CURRENT runtime model
    instead of the original spawning model. Falls back to head scan when no
    model field is found in the tail (e.g. very short transcripts).
    M1550: result is normalized via _norm_session_model so caller always gets a catalog key.
    M1806: scan from the last session-boundary marker (type=mode or permission-mode)
    forward, not just the tail. --resume sessions inherit context from the previous
    session's JSONL entries (which carry the previous model), so a simple tail scan
    picks up the inherited model instead of the current invocation's model. The
    type=mode / permission-mode entries are emitted by Claude Code at the start of each
    new invocation; entries after the LAST such marker belong to the current run.
    M1821-P1: check mtime cache before reading file — active sessions update mtime on
    each tool call; idle sessions' mtime is stable → cache hit rate is high in practice.
    """
    # P1 cache check
    _cache_key = str(transcript_path)
    try:
        _cur_mtime = transcript_path.stat().st_mtime
        _cached = _session_model_cache.get(_cache_key)
        if _cached and _cached[0] == _cur_mtime:
            return _cached[1]
    except Exception:
        _cur_mtime = None

    def _extract_model(entry):
        if not isinstance(entry, dict):
            return ""
        model = entry.get("model") or ""
        if not model and isinstance(entry.get("message"), dict):
            model = entry["message"].get("model") or ""
        if not model and isinstance(entry.get("response"), dict):
            model = entry["response"].get("model") or ""
        s = str(model) if model else ""
        # M1519 v3: Claude Code emits "<synthetic>" model for locally-generated assistant
        # entries (compact summaries, "No response requested." placeholders, etc.) — these
        # are not real API calls and don't reflect the session's runtime model. Skip them.
        if s.startswith("<") or s in ("synthetic", "<synthetic>"):
            return ""
        return s

    _SESSION_BOUNDARY_TYPES = {"mode", "permission-mode"}

    _result = ""
    try:
        if prefer_tail:
            # M1806: find last session-boundary marker then scan only entries after it.
            # M1852-③: read only last 32KB (covers ~200-400 lines) instead of the full file.
            # Session-boundary markers and current-run model fields are always near the END
            # of the transcript; active sessions with large transcripts (multi-MB) were
            # re-reading the entire file on every poll. 32KB is sufficient in ~99% of cases;
            # falls back to full read if no boundary or model found in the tail window.
            try:
                _TAIL_BYTES = 32768
                fsize = transcript_path.stat().st_size
                if fsize <= _TAIL_BYTES:
                    all_lines = transcript_path.read_text(encoding="utf-8", errors="replace").splitlines()
                else:
                    with transcript_path.open("rb") as _f:
                        _f.seek(-_TAIL_BYTES, 2)
                        _tail_raw = _f.read().decode("utf-8", errors="replace")
                    # Drop the first (likely partial) line from the seek
                    _tail_lines = _tail_raw.splitlines()
                    all_lines = _tail_lines[1:] if len(_tail_lines) > 1 else _tail_lines
                last_boundary_idx = -1
                for _i, _ln in enumerate(all_lines):
                    if not _ln.strip():
                        continue
                    try:
                        _e = json.loads(_ln)
                        if isinstance(_e, dict) and _e.get("type") in _SESSION_BOUNDARY_TYPES:
                            last_boundary_idx = _i
                    except Exception:
                        continue
                # Scan entries after the last boundary (current-run entries only)
                scan_lines = all_lines[last_boundary_idx + 1:] if last_boundary_idx >= 0 else all_lines[-max_lines:]
                for line in reversed(scan_lines):
                    if not line.strip():
                        continue
                    try:
                        m = _extract_model(json.loads(line))
                        if m:
                            _result = _norm_session_model(m)
                            break
                    except Exception:
                        continue
                # No model found in current-run entries — fall through to head scan
            except Exception:
                pass
        # Head scan fallback
        if not _result:
            with transcript_path.open("r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= max_lines:
                        break
                    if not line.strip():
                        continue
                    try:
                        m = _extract_model(json.loads(line))
                        if m:
                            _result = _norm_session_model(m)
                            break
                    except Exception:
                        continue
    except Exception:
        pass
    # P1: store in cache keyed by mtime so next call on unchanged file is free
    if _cur_mtime is not None:
        _session_model_cache[_cache_key] = (_cur_mtime, _result)
    return _result


@app.get("/health/{service}")
async def health(service: str):
    if service in ("northstar", "market-signals"):
        return JSONResponse({"ok": True})
    # M1235: CTX disabled
    if service == "ctx":
        return JSONResponse({"ok": False, "error": "CTX disabled (M1235)"}, status_code=404)
    if False:  # dead code — kept for reference
        try:
            r = await _get_async_http().get("http://127.0.0.1:8787/ping", timeout=1.5)
            return JSONResponse({"ok": r.status_code < 500, "status": r.status_code})
        except Exception:
            return JSONResponse({"ok": False, "status": 0})
    svc = SERVICES.get(service)
    if not svc:
        return JSONResponse({"ok": False, "error": "unknown service"}, status_code=404)
    try:
        r = await _get_async_http().get(svc["url"], timeout=1.5)  # M1324-P2: reuse pool
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
    # Owner prefix: name → "{owner}-{skill}" using known_marketplaces.json source.repo (github owner)
    plugins_cache = home / ".claude" / "plugins" / "cache"
    plugin_owners = {}
    km_file = home / ".claude" / "plugins" / "known_marketplaces.json"
    if km_file.is_file():
        try:
            km = json.loads(km_file.read_text(encoding="utf-8"))
            for plugin_name, meta in km.items():
                repo = (meta.get("source") or {}).get("repo") or ""
                if "/" in repo:
                    plugin_owners[plugin_name] = repo.split("/")[0]
        except Exception:
            pass

    if plugins_cache.is_dir():
        for plugin_dir in sorted(plugins_cache.iterdir(), key=lambda x: x.name.lower()):
            if not plugin_dir.is_dir():
                continue
            owner = plugin_owners.get(plugin_dir.name, plugin_dir.name)
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
                        raw_name = (fm.get("name") or p.name).strip()
                        # Prefix with owner (e.g., "fcakyon-reviewer-defense")
                        display_name = f"{owner}-{raw_name}"
                        skills.append({
                            "name": display_name,
                            "dir": f"plugin:{plugin_dir.name}/{p.name}",
                            "description": (fm.get("description") or "").strip(),
                            "source": f"plugin:{plugin_dir.name}@{ver.name}",
                            "owner": owner,
                        })

    # M1023: final alphabetical sort so local + plugin skills interleave correctly in UI
    skills.sort(key=lambda s: s.get("name", "").lower())

    # M1434/M1438: JSONL backfill runs at startup (see _m1434_startup_backfill), not here

    # M1221: annotate usage_count from action_log (invoked_skill events, all-time)
    try:
        _conn_uc = sqlite3.connect(str(_NS_EVENTS_DB))
        _rows_uc = list(_conn_uc.execute(
            "SELECT detail AS skill, COUNT(*) AS n FROM action_log "
            "WHERE action='invoked_skill' GROUP BY detail"
        ))
        _conn_uc.close()
        _usage_map = {r[0]: r[1] for r in _rows_uc}
    except Exception:
        _usage_map = {}
    for s in skills:
        s["usage_count"] = _usage_map.get(s["name"], 0)

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

    # Corpus docs: ~/.hub/projects/*/corpus/*.md + ~/.hub/corpus/*.md
    docs = []
    hub_home = Path.home() / ".hub"
    corpus_dirs = []
    projects_dir = hub_home / "projects"
    if projects_dir.is_dir():
        for proj in sorted(projects_dir.iterdir(), key=lambda x: x.name.lower()):
            cdir = proj / "corpus"
            if cdir.is_dir():
                corpus_dirs.append((proj.name, cdir))
    global_corpus = hub_home / "corpus"
    if global_corpus.is_dir():
        corpus_dirs.append(("global", global_corpus))
    for proj_name, cdir in corpus_dirs:
        for f in sorted(cdir.iterdir(), key=lambda x: x.name.lower()):
            if f.is_file() and f.suffix.lower() == ".md":
                docs.append({
                    "name": f.stem,
                    "project": proj_name,
                    "path": str(f),
                    "size": f.stat().st_size,
                    "mtime": int(f.stat().st_mtime),
                })

    return JSONResponse({
        "skills": skills,
        "agents": agents,
        "docs": docs,
        "counts": {"skills": len(skills), "agents": len(agents), "docs": len(docs)},
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


@app.delete("/api/agent/{agent_name}")
async def delete_agent(agent_name: str):
    """M1275: Delete an agent by backing up its .md file."""
    import re as _re
    if not _re.match(r'^[\w\-]+$', agent_name):
        return JSONResponse({"ok": False, "error": "invalid agent name"}, status_code=400)
    agent_file = Path.home() / ".claude" / "agents" / f"{agent_name}.md"
    if not agent_file.is_file():
        return JSONResponse({"ok": False, "error": "agent file not found"}, status_code=404)
    bak = agent_file.with_suffix(".md.bak")
    try:
        agent_file.rename(bak)
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


@app.get("/api/ctx-telemetry")
async def ctx_telemetry():
    """M1235: CTX disabled."""
    return JSONResponse({"error": "CTX disabled (M1235)"}, status_code=404)


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


def _hub_init(proj_id=None, proj_dir=None):
    """Register a project with NS Hub (DB + optional CLAUDE.md cleanup).

    M1925: CLAUDE.md block removed — MCP tool descriptions carry all protocol
    context that the old raw-REST block provided. project CLAUDE.md is now
    hub-agnostic (M1347 policy enforced).
    """
    import re as _re
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

    # -- 1. Register project in DB (idempotent) --
    existing = _db_load_project(pid)
    if existing is None:
        project_data = {
            "name": pid, "metric": "", "current": "", "target": "",
            "status": "paused", "deadline": "", "note": "",
            "connections": [], "north_stars": [], "layer": 0, "x": None, "y": None,
            "repo_path": str(cwd),
        }
        _db_save_project(pid, project_data)
        print(f"Registered project '{pid}' in hub DB")
    else:
        print(f"Project '{pid}' already registered")

    # -- 2. Clean up legacy NS Hub block from CLAUDE.md if present --
    claude_md = cwd / "CLAUDE.md"
    if claude_md.exists():
        text = claude_md.read_text(encoding="utf-8")
        if "<!-- NS_HUB_PROJECT_START -->" in text:
            cleaned = _re.sub(
                r"\n*## NS Hub — Project Config.*?<!-- NS_HUB_PROJECT_END -->\n*",
                "\n", text, flags=_re.DOTALL
            ).strip()
            claude_md.write_text(cleaned + "\n", encoding="utf-8")
            print(f"Removed legacy NS Hub block from {claude_md}")

    print(f"  Hub URL : {hub_url}")
    print(f"  Project : {pid}")
    print(f"  Dashboard: {hub_url}/northstar")
    print()
    print("Next: restart Claude Code so hooks + MCP are active, then open the dashboard.")


_HUB_HOOKS_DIR = Path(__file__).parent / "static" / "hooks"
_HUB_SETTINGS_HOOKS = {
    # Hub-owned hooks — F2: expanded to 4 hooks for full busy/idle tracking
    "PreToolUse": [
        # F2: busy heartbeat on tool invocation
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-pretool-busy.py", "async": True, "matcher": "Bash|Agent|Task|mcp__.*"},
    ],
    "PostToolUse": [
        # M775: causality dataset — action-log + tool_trace + busy heartbeat (M1577)
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-action-log.py", "async": True, "matcher": "Bash|Edit|Write|Read|Glob|Grep|WebFetch|WebSearch"},
        # NOTE: stone-ctx-hook.py is CTX-feature-specific — register manually when CTX is enabled.
        # Do NOT add it here; hub install-global must not deploy it to all users.
    ],
    "Stop": [
        # F2: idle signal when claude session ends
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-stop-idle.py", "async": True},
    ],
    # M1951: compact + subagent busy coverage — previously missing from install-global
    "PreCompact": [
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-precompact-busy.py", "async": False},
    ],
    "SubagentStart": [
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-subagent-busy.py", "async": False},
    ],
    "SubagentStop": [
        {"type": "command", "command": "python3 $HOME/.hub/static/hooks/northstar-subagent-busy.py", "async": False},
    ],
}

def _hub_deploy_hooks():
    """M842.4: Register hub hook scripts in ~/.claude/settings.json.
    All hooks (NS + CTX) live in static/hooks/ — packaged with hub, no external ctx-retriever needed.
    M775: also registers northstar-action-log.py for tool_trace causality dataset collection."""
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
        existing_basenames = set()  # M1081.2: also track basename to catch path-variant duplicates
        for entry in existing:
            for h in (entry.get("hooks", [entry]) if "hooks" in entry else [entry]):
                if "command" in h:
                    existing_cmds.add(h["command"])
                    # Extract script basename (e.g. "bm25-memory.py") ignoring path prefix
                    existing_basenames.add(Path(h["command"].split()[0].strip('"')).name)
        for h in hook_list:
            basename = Path(h["command"].split()[0].strip('"')).name
            if h["command"] not in existing_cmds and basename not in existing_basenames:
                hooks_cfg.setdefault(event, []).append({"hooks": [h]})
                changed = True
                print(f"Registered hook {event}: {h['command']}")
            elif basename in existing_basenames and h["command"] not in existing_cmds:
                print(f"Skipped duplicate hook (basename match) {event}: {h['command']}")
    if changed:
        settings_path.write_text(json.dumps(settings, indent=2, ensure_ascii=False))

    # F1: register MCP server globally in ~/.claude/settings.json
    mcp_script = Path(__file__).parent / "static" / "hooks" / "hub-mcp-server.py"
    if mcp_script.exists():
        try:
            settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
        except Exception:
            settings = {}
        mcp_cfg = settings.setdefault("mcpServers", {})
        if "ns-hub" not in mcp_cfg:
            import sys as _sys
            mcp_cfg["ns-hub"] = {
                "type": "stdio",
                "command": _sys.executable,
                "args": [str(mcp_script), "--hub-url", "http://127.0.0.1:9001"],
            }
            settings_path.write_text(json.dumps(settings, indent=2, ensure_ascii=False))
            print(f"Registered MCP ns-hub in {settings_path}")
        else:
            print("MCP ns-hub already registered — skipping.")

_SYSTEMD_UNIT_TEMPLATE = """\
[Unit]
Description=Claude Hub Dashboard (code+data in ~/.hub)
After=network-online.target

[Service]
Environment=PATH={bin_dir}:/usr/local/bin:/usr/bin:/bin
EnvironmentFile=%h/.config/hub/env
EnvironmentFile=-%h/.config/hub/env.host
Type=simple
WorkingDirectory=%h/.hub
# M1160: KillMode=process so `systemctl restart hub` does NOT terminate the tmux server
# (which spawns inside hub.service cgroup on first session). Without this, default
# KillMode=control-group SIGKILLs the entire cgroup on restart, dropping every exec session.
KillMode=process
KillSignal=SIGINT
# M488: wait up to 60s for Tailscale IP before binding — prevents bind failure when hub
# starts before tailscaled assigns the 100.x.x.x address (race at WSL boot without VS Code)
# M1253: bind 0.0.0.0 (all interfaces) so local clients on 127.0.0.1 — MCP server, hooks,
# curl — also work. Binding a single Tailscale IP refused 127.0.0.1:9001 (MCP "Connection
# refused"). The TS-IP wait below is kept only as a readiness gate, not as the bind host.
ExecStartPre=/bin/bash -c 'for i in $(seq 1 30); do ip addr show tailscale0 2>/dev/null | grep -q "100\\." && exit 0; sleep 2; done; exit 0'
ExecStart={uvicorn_bin} server:app --host 0.0.0.0 --port ${{HUB_PORT}} --log-level info --access-log --no-server-header
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
"""


def _hub_generate_systemd_unit():
    """M1694b: render ~/.config/systemd/user/hub.service from shutil.which("uvicorn")
    instead of a hardcoded absolute path + username — the old unit baked in the installing
    user's home dir literally (/home/desk-1/.local/bin/...), breaking for any other install.
    Idempotent: skips if a unit already exists with the correct uvicorn path (avoids
    clobbering hand-tuned units on existing installs); does NOT reload/restart the service —
    that's the operator's call (`systemctl --user daemon-reload && systemctl --user restart hub`)."""
    import shutil as _shutil_su, platform as _plat
    if _plat.system() == "Darwin":
        print("macOS detected — systemd is not available. Start hub manually with `hub` or add it to your shell login profile (e.g. ~/.zprofile). A launchd plist is not generated automatically.")
        return
    uvicorn_bin = _shutil_su.which("uvicorn")
    if not uvicorn_bin:
        print("uvicorn not found on PATH — skipping systemd unit generation.")
        return
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_path = unit_dir / "hub.service"
    rendered = _SYSTEMD_UNIT_TEMPLATE.format(
        bin_dir=str(Path(uvicorn_bin).parent), uvicorn_bin=uvicorn_bin,
    )
    if unit_path.exists():
        existing = unit_path.read_text(encoding="utf-8")
        if f"ExecStart={uvicorn_bin} " in existing:
            print(f"{unit_path} already points at the correct uvicorn path — skipping.")
            return
        print(f"{unit_path} exists with a different uvicorn path — not overwriting "
              f"(regenerate manually if intentional: rm {unit_path} && rerun hub install-global).")
        return
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path.write_text(rendered, encoding="utf-8")
    print(f"Wrote {unit_path} (uvicorn={uvicorn_bin}). Run 'systemctl --user daemon-reload "
          f"&& systemctl --user enable --now hub' to activate.")


def _hub_doctor(exit_code: bool = False):
    """F9: Self-diagnostic command — checks all prerequisites for a working hub install.
    Each check prints ✅/❌ + a one-line fix command on failure.
    P1: --exit-code flag returns sys.exit(failed_count) for CI integration."""
    import shutil as _sh
    import urllib.request as _ur

    checks = []

    def chk(label, ok, fix=""):
        mark = "✅" if ok else "❌"
        line = f"  {mark}  {label}"
        if not ok and fix:
            line += f"\n       → {fix}"
        checks.append((ok, line))
        print(line)

    print("\nhub doctor — NS Hub install diagnostics\n")

    # Python version
    import sys as _sys
    py_ok = _sys.version_info >= (3, 10)
    chk(f"Python ≥ 3.10  (found {_sys.version.split()[0]})", py_ok,
        "upgrade Python: https://python.org/downloads")

    # tmux
    chk("tmux installed", bool(_sh.which("tmux")),
        "sudo apt install tmux  # or brew install tmux")

    # claude CLI
    chk("claude CLI installed", bool(_sh.which("claude")),
        "npm install -g @anthropic-ai/claude-code")

    # ~/.config/hub/env
    env_file = Path.home() / ".config" / "hub" / "env"
    chk(f"~/.config/hub/env exists", env_file.exists(),
        "hub install-global  # auto-creates this file")

    # settings.json MCP
    settings_path = Path.home() / ".claude" / "settings.json"
    try:
        _sd = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    except Exception:
        _sd = {}
    chk("MCP ns-hub registered in ~/.claude/settings.json",
        "ns-hub" in _sd.get("mcpServers", {}),
        "hub install-global")

    # core hooks (stone-ctx-hook.py excluded — CTX-feature-specific, not auto-deployed)
    _core_hooks = ["northstar-pretool-busy.py", "northstar-stop-idle.py",
                   "northstar-action-log.py", "northstar-precompact-busy.py",
                   "northstar-subagent-busy.py"]
    _all_hook_cmds = " ".join(
        h.get("command", "")
        for ev in _sd.get("hooks", {}).values()
        for entry in ev
        for h in entry.get("hooks", [entry])
    )
    for _hk in _core_hooks:
        chk(f"Hook registered: {_hk}", _hk in _all_hook_cmds,
            "hub install-global")

    # hub server responding — NS_HUB_URL env takes priority (Tailscale / custom port)
    _hub_check_url = os.environ.get("NS_HUB_URL", "http://127.0.0.1:9001")
    try:
        _r = _ur.urlopen(f"{_hub_check_url}/api/hub/defaults", timeout=2)
        hub_ok = _r.status == 200
    except Exception:
        hub_ok = False
    chk(f"Hub server responding at {_hub_check_url}", hub_ok,
        "systemctl --user start hub  # or: hub (to start manually)")

    # M1925: OpenRouter checks (optional — informational only, not counted in failures)
    print("\nhub doctor — OpenRouter (optional)\n")
    _or_key_set = False
    try:
        _env_text = env_file.read_text() if env_file.exists() else ""
        _or_key_set = any(
            l.startswith("OPENROUTER_API_KEY=") and not l.startswith("#")
            for l in _env_text.splitlines()
        )
    except Exception:
        pass
    _litellm_ok = bool(_sh.which("litellm"))
    _hub_ll_cfg = Path.home() / ".hub-litellm.yaml"
    _rsk_ll_cfg = Path.home() / ".rsk-litellm.yaml"
    _ll_cfg_ok = _hub_ll_cfg.exists() or _rsk_ll_cfg.exists()
    _ll_cfg_path = _hub_ll_cfg if _hub_ll_cfg.exists() else (_rsk_ll_cfg if _rsk_ll_cfg.exists() else _hub_ll_cfg)
    _ll_proxy_ok = False
    try:
        _pr = _ur.urlopen("http://127.0.0.1:4100/v1/models", timeout=1)
        _ll_proxy_ok = _pr.status == 200
    except Exception:
        pass
    def _info(label, ok, fix=""):
        mark = "✅" if ok else "⚪"
        line = f"  {mark}  {label}"
        if not ok and fix:
            line += f"\n       → {fix}"
        print(line)
    _info(f"OPENROUTER_API_KEY set in ~/.config/hub/env", _or_key_set,
          f"echo 'OPENROUTER_API_KEY=sk-or-v1-...' >> {env_file}")
    _info("litellm CLI installed", _litellm_ok,
          "pip install litellm")
    _info(f"LiteLLM config exists ({_ll_cfg_path.name})", _ll_cfg_ok,
          "hub install-global  # auto-generates ~/.hub-litellm.yaml")
    _info("LiteLLM proxy responding at :4100", _ll_proxy_ok,
          "hub will auto-start it on next restart if key + litellm are both set")

    failed = sum(1 for ok, _ in checks if not ok)
    print(f"\n{'All checks passed ✅' if failed == 0 else f'{failed} check(s) failed — run the fix commands above.'}\n")
    if exit_code:
        import sys as _sys2
        _sys2.exit(failed)


def _hub_ensure_env_file():
    """F5: Create ~/.config/hub/env with defaults if missing.
    systemd EnvironmentFile= fails silently when file absent — prevents service start.
    M1925: also injects OPENROUTER_API_KEY placeholder so users know where to set it."""
    env_dir = Path.home() / ".config" / "hub"
    env_file = env_dir / "env"
    if not env_file.exists():
        env_dir.mkdir(parents=True, exist_ok=True)
        env_file.write_text(
            "HUB_HOST=127.0.0.1\nHUB_PORT=9001\n"
            "# OpenRouter — set key to enable or-* models (e.g. or-kimi-k2, or-grok-3)\n"
            "#OPENROUTER_API_KEY=sk-or-v1-...\n",
            encoding="utf-8",
        )
        print(f"Created {env_file} (HUB_HOST=127.0.0.1 HUB_PORT=9001)")
    else:
        # Idempotent: inject OR placeholder comment if key not mentioned yet
        existing = env_file.read_text(encoding="utf-8")
        if "OPENROUTER_API_KEY" not in existing:
            with env_file.open("a", encoding="utf-8") as _f:
                _f.write(
                    "\n# OpenRouter — set key to enable or-* models (e.g. or-kimi-k2, or-grok-3)\n"
                    "#OPENROUTER_API_KEY=sk-or-v1-...\n"
                )
            print(f"Updated {env_file}: added OPENROUTER_API_KEY placeholder")
    return env_file


# Mapping from or-* hub alias to upstream OpenRouter model ID (used to generate litellm config)
_OR_MODEL_UPSTREAM = {
    "or-gemini-flash":      "openrouter/google/gemini-2.5-flash",
    "or-gemini3-flash":     "openrouter/google/gemini-3-flash-preview",
    "or-deepseek-v4-flash": "openrouter/deepseek/deepseek-v4-flash",
    "or-kimi-k2":           "openrouter/moonshotai/kimi-k2.6",
    "or-hy3-preview":       "openrouter/tencent/hy3-preview",
    "or-hy3":               "openrouter/tencent/hy3:free",
    "or-owl-alpha":         "openrouter/openrouter/owl-alpha",
    "or-grok-3":            "openrouter/x-ai/grok-3",
    "or-grok-3-mini":       "openrouter/x-ai/grok-3-mini",
    "or-nemotron":          "openrouter/nvidia/nemotron-3-super-120b-a12b:free",
    "or-nemotron-nano":     "openrouter/nvidia/nemotron-3-nano-30b-a3b:free",
    # Claude models via OpenRouter (API-key fallback, not OAuth)
    "or-sonnet":            "openai/anthropic/claude-sonnet-4.6",
    "or-opus":              "openai/anthropic/claude-opus-4.7",
    "or-haiku":             "openai/anthropic/claude-haiku-4.5",
}

_HUB_LITELLM_CONFIG = Path.home() / ".hub-litellm.yaml"


def _hub_generate_litellm_config() -> Path | None:
    """M1925: Generate ~/.hub-litellm.yaml from _OR_MODEL_UPSTREAM if not present.
    Idempotent — skips if file already exists (user may have customized it).
    Returns path written, or None if skipped."""
    if _HUB_LITELLM_CONFIG.exists():
        return None  # already present — do not overwrite
    import shutil as _shutil
    lines = [
        "# NS Hub — OpenRouter LiteLLM proxy config (auto-generated by hub install-global)",
        "# Edit model list or API base as needed. Hub reads this file on startup.",
        "model_list:",
    ]
    for or_key, upstream in _OR_MODEL_UPSTREAM.items():
        proxy_name = "openrouter-" + or_key[3:]  # or-kimi-k2 → openrouter-kimi-k2
        lines += [
            f"  - model_name: {proxy_name}",
            f"    litellm_params:",
            f"      model: {upstream}",
            f"      api_key: os.environ/OPENROUTER_API_KEY",
            f"      api_base: https://openrouter.ai/api/v1",
        ]
    lines += [
        "",
        "litellm_settings:",
        "  drop_params: true",
        "",
        "general_settings:",
        "  drop_params: true",
    ]
    _HUB_LITELLM_CONFIG.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Created {_HUB_LITELLM_CONFIG}")
    return _HUB_LITELLM_CONFIG


def _hub_install_global():
    """Deploy hub hooks and systemd unit. Does NOT touch user CLAUDE.md.
    M1694b: CTX (bm25-memory, chat-memory, vec-daemon, etc.) is a fully independent,
    separately-installed project — hub does NOT own, bundle, or deploy CTX hooks. The only
    hub-owned hooks auto-deployed here are: northstar-pretool-busy.py, northstar-stop-idle.py,
    northstar-action-log.py (tool_trace causality dataset collection, see _HUB_SETTINGS_HOOKS).
    stone-ctx-hook.py lives in static/hooks/ but is CTX-feature-specific — NOT auto-deployed
    by install-global. Users who enable CTX must register it manually.
    M1869: CLAUDE.md injection removed — _HUB_GLOBAL_BLOCK was 100% duplicate of
    _HUB_EXEC_SYS_PROMPT (injected via --append-system-prompt on every exec session spawn).
    Writing to user's ~/.claude/CLAUDE.md is user-environment pollution; exec sessions already
    receive all hub protocol rules through the MCP spawn path."""
    _hub_ensure_env_file()  # F5: must precede systemd unit gen (unit references this file)
    _hub_generate_litellm_config()  # M1925: generate ~/.hub-litellm.yaml if absent
    _hub_deploy_hooks()
    _hub_generate_systemd_unit()
    # M1925: OpenRouter interactive setup
    _hub_setup_openrouter_key()
    print("\n⚠  Restart Claude Code now to activate MCP + hooks: close all Claude Code windows and reopen.\n")


def _hub_setup_openrouter_key():
    """M1925-b: Prompt for OPENROUTER_API_KEY interactively during install-global.
    - Reads current ~/.config/hub/env; skips if key already set (active, not commented).
    - TTY-gated: non-interactive environments (CI, piped) skip silently.
    - Writes the key as an uncommented active line; removes any existing placeholder comment.
    - Never stores a key that looks like a template (contains '...' or 'sk-or-v1-..')."""
    import shutil as _shutil
    import sys as _sys
    env_file = Path.home() / ".config" / "hub" / "env"

    # Check if already set
    try:
        env_text = env_file.read_text(encoding="utf-8")
    except Exception:
        env_text = ""
    active_key = next(
        (l.split("=", 1)[1].strip() for l in env_text.splitlines()
         if l.startswith("OPENROUTER_API_KEY=") and not l.startswith("#")),
        None
    )
    litellm_ok = bool(_shutil.which("litellm"))

    print("\n── OpenRouter setup (optional) ─────────────────────────────────────────────")
    if active_key:
        masked = active_key[:12] + "..." + active_key[-4:] if len(active_key) > 16 else "****"
        print(f"  OPENROUTER_API_KEY: ✅ already set ({masked})")
        print(f"  litellm CLI:        {'✅ found' if litellm_ok else '❌ not found — run: pip install litellm'}")
        print("─────────────────────────────────────────────────────────────────────────────\n")
        return

    # Non-interactive: skip prompt
    if not _sys.stdin.isatty():
        print(f"  Skipped (non-interactive). Set OPENROUTER_API_KEY in {env_file} manually.")
        print("─────────────────────────────────────────────────────────────────────────────\n")
        return

    print("  Enable or-* models (or-kimi-k2, or-grok-3, or-gemini-flash, …)")
    print("  Get a key at: https://openrouter.ai/keys")
    try:
        entered = input("  Enter OpenRouter API key (sk-or-...) or press Enter to skip: ").strip()
    except (EOFError, KeyboardInterrupt):
        entered = ""

    if not entered or "..." in entered or entered == "sk-or-v1-..":
        print("  Skipped — add OPENROUTER_API_KEY manually to:")
        print(f"    {env_file}")
        print("─────────────────────────────────────────────────────────────────────────────\n")
        return

    if not entered.startswith("sk-or-"):
        print(f"  ⚠  Key doesn't start with 'sk-or-' — saved anyway, but verify it's correct.")

    # Write: remove old placeholder comment, append active key
    lines = [l for l in env_text.splitlines()
             if not l.startswith("#OPENROUTER_API_KEY=") and not l.startswith("OPENROUTER_API_KEY=")]
    lines.append(f"OPENROUTER_API_KEY={entered}")
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  ✅ OPENROUTER_API_KEY saved to {env_file}")
    if not litellm_ok:
        print("  Next: pip install litellm  (hub auto-starts proxy on restart)")
    else:
        print("  ✅ litellm installed — restart hub to activate OpenRouter models")
    print("─────────────────────────────────────────────────────────────────────────────\n")


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

def _hub_pkg_version() -> str:
    try:
        from importlib.metadata import version as _v
        return _v("claude-ns-hub")
    except Exception:
        return "unknown"

def _compress_trigger_reason(parent: dict) -> str | None:
    """M1253: evaluate all 4 trigger conditions, return reason string if any match, else None.
    Triggers:
      1) length: conv_len >= 10 AND (conv_len - last_compressed_len) >= 5
      2) tokens: sum(input+output) >= 50_000 stone-wide
      3) time: now - last conversation entry >= 7 days AND conv_len > 10
      4) explicit: parent.text or claude_comment contains '[compress]' tag
    """
    try:
        conv = parent.get("conversation") or []
        conv_len = len(conv)
        if conv_len < 1:
            return None
        state = parent.get("summary_state") or {}
        last_len = int(state.get("last_compressed_len") or 0)
        # Trigger 4: explicit tag (highest priority — manual override)
        for k in ("text", "claude_comment"):
            v = (parent.get(k) or "")
            if "[compress]" in str(v).lower():
                return "explicit"
        # Trigger 1: length-based (M1253: 25/10→10/5; M1869-P2: delta 5→3 for more frequent compression)
        if conv_len >= 10 and (conv_len - last_len) >= 3:
            return "length"
        # Trigger 2: token-based (API-key env only — subscription users have no token fields,
        # so tok=0 always and this branch never fires; effectively a no-op in that case)
        tok = int(parent.get("input_tokens") or 0) + int(parent.get("output_tokens") or 0)
        if tok >= 50_000 and (conv_len - last_len) >= 3:
            return "tokens"
        # Trigger 3: time-based — stagnant >= 7 days
        if conv_len > 10 and (conv_len - last_len) >= 3:
            last_ts = conv[-1].get("ts") or ""
            if last_ts:
                try:
                    import datetime as _dt
                    last_dt = _dt.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    now_naive = _dt.datetime.now(last_dt.tzinfo) if last_dt.tzinfo else _dt.datetime.now()
                    if (now_naive - last_dt).days >= 7:
                        return "time"
                except (ValueError, TypeError):
                    pass
        return None
    except Exception:
        return None


def _maybe_queue_compress(proj_id: str, parent: dict, milestones: list, reason_override: str | None = None) -> None:
    """M1253: idempotently queue a [compress] child stone when parent conversation grew enough.
    See _compress_trigger_reason for the 4 trigger types.
    Skip if a [compress] child already exists for this parent (in-flight idempotency).
    """
    try:
        # M1724-b: never compress a compress-child itself. Making compress children real
        # queued/dispatched stones (M1724) exposed a self-trigger loop: handle_attach_artifact's
        # auto-close PATCH appends a "압축 완료." message to the child, which runs this same
        # function against the child — and the child's own instruction text contains the
        # literal "[compress]" tag, matching trigger 4 (explicit) regardless of conv_len,
        # spawning a grandchild .cmp.cmp.cmp... forever. Compress children are meta-only;
        # they never need their own summary.
        if str(parent.get("category") or "").startswith("meta/compress"):
            return
        reason = reason_override or _compress_trigger_reason(parent)
        if not reason:
            return
        parent_id = parent.get("id")
        if not parent_id:
            return
        # In-flight idempotency: skip if a [compress] child for this parent is still open.
        for sib in milestones:
            if not isinstance(sib, dict):
                continue
            if sib.get("parent_id") == parent_id and str(sib.get("text", "")).startswith("[compress]"):
                if sib.get("status") in ("queued", "pending", "pending_confirmation", "needs_clarification") and not sib.get("done"):
                    return
        # M1724: re-enabled real dispatch — M1655-fix made this child done=True at birth to
        # avoid a stuck-queue busy-lock, but that also meant NO agent was ever dispatched to
        # actually read the conversation and write a summary: compress_summary/attach_artifact
        # was never called, parent.conversation_summary/summary_state stayed None forever
        # (confirmed live: M1608 had 28 dead .cmp children, all born done, zero summaries).
        # The stuck-queue risk M1655-fix worried about is already covered without a new
        # timeout tier: _validate_stone_hold (server.py ~657) re-checks this child's DB status
        # on every busy check and clears the hold the instant it leaves "queued" — independent
        # of the 7200s crash-safety cap, which is a last-resort ceiling, not the expected path.
        # Also fixes the tool-name bug: the instruction previously told the agent to call a
        # nonexistent "attach_summary" — the real registered tool is compress_summary
        # (aliased attach_artifact in hub-mcp-server.py's dispatcher).
        import datetime as _dt
        child_id = f"{parent_id}.cmp{int(_dt.datetime.now().timestamp()) % 10000}"
        now_c = _dt.datetime.utcnow().isoformat()
        child = {
            "id": child_id,
            "parent_id": parent_id,
            "layer": 1,  # M1264: layer=1 required for grouping border + toggle button on parent
            "substar_id": parent.get("substar_id") or None,  # inherit parent's group — avoids Ungrouped
            "text": (
                f"[compress] (trigger: {reason}) Read stone {parent_id}'s full conversation, write a 3-8 line summary "
                f"of key decisions/constraints/open items, then call compress_summary(task_id='{parent_id}', summary='<your-summary>'). "
                f"If the stone already has a conversation_summary field, your summary should cover the FULL arc (old context + new turns) — "
                f"compress_summary will auto-merge old ‖ new so do NOT duplicate the old text verbatim. "
                f"Do NOT touch other status fields. This is a meta task — keep it brief."
            ),
            "status": "queued",
            "done": False,
            "category": "meta/compress",
            "created_at": now_c,
        }
        milestones.append(child)
        # M1655-fix Bug A: immediately persist child to DB so dispatcher and periodic scanner
        # see a consistent state. Without this, 30-min scanner re-creates and saves it as queued.
        try:
            _db_save_single_milestone(proj_id, child)
        except Exception:
            pass
    except Exception:
        pass  # never block stone completion on compress queuing


def _record_usage_event(event: str, extra: dict = None):
    """M929: Record usage event locally + centrally via Turso (consent-gated, no PII)."""
    if not _get_consent().get("data_collection", True):
        return
    import hashlib, platform, time
    _install_id = hashlib.sha256(platform.node().encode()).hexdigest()[:16]
    entry = {
        "ts": int(time.time()), "event": event,
        "install_id": _install_id,
        "version": _hub_pkg_version(),
        "os": platform.system(),
        **(extra or {})
    }
    # Local JSONL
    try:
        with open(_USAGE_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass
    # M1517: Central telemetry via Cloudflare Worker relay. Async, fire-and-forget.
    # Relay proxies to Turso server-side so credentials never live in client code.
    if _HUB_RELAY_URL:
        import threading
        def _send_telemetry_via_relay():
            try:
                import urllib.request as _ur2, json as _j2
                payload = {
                    "kind": "hub_usage",  # tells relay which table to write
                    "ts": entry["ts"], "event": entry["event"],
                    "install_id": entry["install_id"], "version": entry["version"],
                    "os": entry["os"], "extra_json": _j2.dumps({k: v for k, v in (extra or {}).items()}),
                }
                headers = {"Content-Type": "application/json"}
                if _HUB_RELAY_SECRET:
                    headers["X-Relay-Secret"] = _HUB_RELAY_SECRET
                req = _ur2.Request(f"{_HUB_RELAY_URL}/v1/hub-usage", data=_j2.dumps(payload).encode(),
                    headers=headers, method="POST")
                _ur2.urlopen(req, timeout=5)
            except Exception:
                pass
        threading.Thread(target=_send_telemetry_via_relay, daemon=True).start()

# ── M1516: hub_session_aggregates — CTX-equivalent 15-field session aggregate ─
# Privacy model: k-anonymity gate(threshold = k_min, dynamic by user count) + no raw text + schema_version
_HUB_AGG_SCHEMA_VERSION = "v1"
_HUB_AGG_K_MIN = int(os.environ.get("HUB_AGG_K_MIN", "1"))  # initial k=1 (relax); raise as user base grows

def _compute_session_aggregate(proj_id: str = None) -> dict | None:
    """M1516: build a single session_aggregate row (numeric/histogram fields only).
    Aggregates over last 24h activity across all projects when proj_id=None.
    Returns dict matching hub_session_aggregates schema, or None if insufficient data."""
    import hashlib, platform, time
    from collections import Counter
    try:
        _install_id = hashlib.sha256(platform.node().encode()).hexdigest()[:16]
        cutoff_ts = int(time.time()) - 86400  # last 24h
        ts_date = time.strftime("%Y-%m-%d", time.gmtime())

        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        try:
            # action_log counts (action_log.ts is ISO string, action_log.action is the verb)
            tool_hist = {}
            cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_ts))
            try:
                for row in conn.execute(
                    "SELECT action, COUNT(*) FROM action_log WHERE ts > ? GROUP BY action ORDER BY 2 DESC LIMIT 50",
                    (cutoff_iso,)
                ).fetchall():
                    tool_hist[str(row[0])[:32]] = int(row[1])
            except Exception:
                pass

            action_count = sum(tool_hist.values())

            # tool_trace count + mean duration (ts is ISO string)
            tool_trace_count = 0
            mean_duration_ms = 0
            try:
                row = conn.execute(
                    "SELECT COUNT(*), AVG(duration_ms) FROM tool_trace WHERE ts > ?",
                    (cutoff_iso,)
                ).fetchone()
                if row:
                    tool_trace_count = int(row[0] or 0)
                    mean_duration_ms = round(float(row[1] or 0), 1)
            except Exception:
                pass

            # milestone outcomes (done in last 24h)
            outcome_hist = {}
            stone_complete = 0
            try:
                for row in conn.execute(
                    "SELECT COALESCE(json_extract(data_json,'$.outcome_label'),'unknown') as outcome, COUNT(*) "
                    "FROM milestones_store WHERE status='done' "
                    "AND COALESCE(json_extract(data_json,'$.done_at'),'') > ? "
                    "GROUP BY outcome",
                    (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_ts)),)
                ).fetchall():
                    outcome_hist[str(row[0])[:16]] = int(row[1])
                    stone_complete += int(row[1])
            except Exception:
                pass

            # project count (active in last 24h)
            project_count = 0
            try:
                project_count = conn.execute(
                    "SELECT COUNT(DISTINCT proj_id) FROM milestones_store WHERE "
                    "COALESCE(json_extract(data_json,'$.user_added_at'),'') > ?",
                    (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_ts)),)
                ).fetchone()[0] or 0
            except Exception:
                pass
        finally:
            conn.close()

        # Skip if no activity at all
        if action_count == 0 and stone_complete == 0:
            return None

        return {
            "schema_version": _HUB_AGG_SCHEMA_VERSION,
            "install_id": _install_id,
            "ts_date": ts_date,
            "ts_uploaded": int(time.time()),
            "hub_version": _hub_pkg_version(),
            "os": platform.system(),
            # depth fields (Hub-specific extensions vs CTX 15 fields)
            "action_count": action_count,
            "tool_trace_count": tool_trace_count,
            "mean_tool_duration_ms": mean_duration_ms,
            "stone_complete_count": stone_complete,
            "project_count": project_count,
            "tool_hist_json": json.dumps(tool_hist),
            "outcome_hist_json": json.dumps(outcome_hist),
            # safety
            "k_count": _HUB_AGG_K_MIN,  # initial threshold; suppressed server-side when total k < threshold
        }
    except Exception:
        return None


_HUB_RELAY_URL = os.environ.get("HUB_RELAY_URL", "https://hub-telemetry-relay.be2jay67.workers.dev").rstrip("/")
_HUB_RELAY_SECRET = os.environ.get("HUB_RELAY_SECRET", "")

def _send_session_aggregate(agg: dict) -> bool:
    """M1516/M1517: upload session_aggregate to hub_session_aggregates.
    Preferred path (M1517): POST to relay URL (HUB_RELAY_URL env) so Turso credentials
    stay server-side. Fallback: direct Turso INSERT (legacy, for trusted local installs).
    Returns True on success, silent on failure (telemetry must never break runtime)."""
    if not agg:
        return False
    try:
        import urllib.request as _ur, json as _j
        # Preferred — proxy via relay (no Turso URL/token in client)
        if _HUB_RELAY_URL:
            headers = {"Content-Type": "application/json"}
            if _HUB_RELAY_SECRET:
                headers["X-Relay-Secret"] = _HUB_RELAY_SECRET
            req = _ur.Request(
                f"{_HUB_RELAY_URL}/v1/session-aggregate",
                data=_j.dumps(agg).encode(),
                headers=headers, method="POST",
            )
            with _ur.urlopen(req, timeout=10) as resp:
                result = _j.load(resp)
            return bool(result.get("ok"))
        # Fallback — direct Turso (legacy path, only safe for trusted local installs)
        if not _TEL_ENABLED:
            return False
        cols = list(agg.keys())
        placeholders = ", ".join("?" for _ in cols)
        args = []
        for c in cols:
            v = agg[c]
            if isinstance(v, int):
                args.append({"type": "integer", "value": str(v)})
            elif isinstance(v, float):
                args.append({"type": "float", "value": v})
            else:
                args.append({"type": "text", "value": str(v)})
        payload = _j.dumps({"requests": [
            {"type": "execute", "stmt": {"sql": (
                "CREATE TABLE IF NOT EXISTS hub_session_aggregates ("
                "schema_version TEXT, install_id TEXT, ts_date TEXT, ts_uploaded INTEGER, "
                "hub_version TEXT, os TEXT, action_count INTEGER, tool_trace_count INTEGER, "
                "mean_tool_duration_ms REAL, stone_complete_count INTEGER, project_count INTEGER, "
                "tool_hist_json TEXT, outcome_hist_json TEXT, k_count INTEGER)"
            )}},
            {"type": "execute", "stmt": {
                "sql": f"INSERT INTO hub_session_aggregates ({', '.join(cols)}) VALUES ({placeholders})",
                "args": args
            }},
        ]}).encode()
        req = _ur.Request(
            f"{_HUB_TELEMETRY_URL}/v2/pipeline", data=payload,
            headers={"Authorization": f"Bearer {_HUB_TELEMETRY_TOKEN}", "Content-Type": "application/json"},
        )
        with _ur.urlopen(req, timeout=10) as resp:
            result = _j.load(resp)
        errors = [r for r in result.get("results", []) if r.get("type") == "error"]
        return not errors
    except Exception:
        return False


def _send_raw_batch(endpoint: str, rows: list[dict]) -> bool:
    """M1516-ext: POST a batch of raw rows to relay endpoint.
    Used for tool_trace and action_log harness-training upload.
    Payload: {"kind": endpoint_kind, "install_id": ..., "rows": [...]}"""
    if not _HUB_RELAY_URL or not rows:
        return False
    try:
        import urllib.request as _ur, json as _j, hashlib, platform
        _install_id = hashlib.sha256(platform.node().encode()).hexdigest()[:16]
        headers = {"Content-Type": "application/json"}
        if _HUB_RELAY_SECRET:
            headers["X-Relay-Secret"] = _HUB_RELAY_SECRET
        payload = _j.dumps({"install_id": _install_id, "rows": rows}).encode()
        req = _ur.Request(
            f"{_HUB_RELAY_URL}/{endpoint}",
            data=payload, headers=headers, method="POST",
        )
        with _ur.urlopen(req, timeout=15) as resp:
            result = _j.load(resp)
        return bool(result.get("ok"))
    except Exception:
        return False


def _upload_raw_tables(state: dict) -> dict:
    """M1516-ext: upload new tool_trace + action_log rows since last watermark.
    Watermarks stored in .hub-agg-state.json under 'tool_trace_last_id' / 'action_log_last_id'.
    Rows are stripped of any raw text — only sanitized summaries and metadata are sent."""
    if not _HUB_RELAY_URL:
        return state
    try:
        conn = sqlite3.connect(str(_NS_EVENTS_DB))
        try:
            # ── tool_trace batch ──────────────────────────────────────────
            tt_watermark = int(state.get("tool_trace_last_id", 0))
            tt_rows = []
            try:
                for row in conn.execute(
                    "SELECT id, ts, proj_id, stone_id, session_id, tool_name, input_summary, output_summary, duration_ms "
                    "FROM tool_trace WHERE id > ? ORDER BY id LIMIT 500",
                    (tt_watermark,)
                ).fetchall():
                    tt_rows.append({
                        "id": row[0], "ts": row[1],
                        "proj_id": row[2] or "", "stone_id": row[3] or "",
                        "session_id": row[4] or "", "tool_name": row[5] or "",
                        # M1732: PII scrub was never wired into this path (docstring claimed
                        # "stripped of raw text" but no _scrub_pii call existed) — added.
                        "input_summary": _scrub_pii((row[6] or "")[:200]),
                        "output_summary": _scrub_pii((row[7] or "")[:200]),
                        "duration_ms": int(row[8] or 0),
                    })
            except Exception:
                pass
            if tt_rows and _send_raw_batch("v1/tool-trace-batch", tt_rows):
                state["tool_trace_last_id"] = tt_rows[-1]["id"]

            # ── action_log batch ──────────────────────────────────────────
            al_watermark = int(state.get("action_log_last_id", 0))
            al_rows = []
            try:
                for row in conn.execute(
                    "SELECT id, ts, proj_id, stone_id, action, detail, session_id "
                    "FROM action_log WHERE id > ? ORDER BY id LIMIT 500",
                    (al_watermark,)
                ).fetchall():
                    al_rows.append({
                        "id": row[0], "ts": row[1],
                        "proj_id": row[2] or "", "stone_id": row[3] or "",
                        "action": row[4] or "",
                        # M1732: detail carries raw client IP + User-Agent for at least the
                        # update_milestone endpoint (confirmed via live DB read) — scrub before
                        # sending, matching the docstring's original (unenforced) claim.
                        "detail": _scrub_pii((row[5] or "")[:200]),
                        "session_id": row[6] or "",
                    })
            except Exception:
                pass
            if al_rows and _send_raw_batch("v1/action-log-batch", al_rows):
                state["action_log_last_id"] = al_rows[-1]["id"]

            # ── milestone causal batch (M775) ─────────────────────────────
            # Transmit causal dataset fields for done stones only.
            # Uses rowid watermark; only rows where at least one M775 field is non-null.
            mc_watermark = int(state.get("milestone_causal_last_rowid", 0))
            mc_rows = []
            try:
                for row in conn.execute(
                    "SELECT rowid, stone_id, proj_id, "
                    "json_extract(data_json,'$.outcome_label'), "
                    "json_extract(data_json,'$.counterfactual_pair_id'), "
                    "json_extract(data_json,'$.goal_tree_snapshot'), "
                    "json_extract(data_json,'$.prompt_provenance'), "
                    "json_extract(data_json,'$.confounder'), "
                    "json_extract(data_json,'$.done_at') "
                    "FROM milestones_store WHERE rowid > ? "
                    "AND status='done' "
                    "AND (json_extract(data_json,'$.outcome_label') IS NOT NULL "
                    " OR json_extract(data_json,'$.counterfactual_pair_id') IS NOT NULL "
                    " OR json_extract(data_json,'$.goal_tree_snapshot') IS NOT NULL) "
                    "ORDER BY rowid LIMIT 200",
                    (mc_watermark,)
                ).fetchall():
                    mc_rows.append({
                        "rowid": row[0], "stone_id": row[1] or "", "proj_id": row[2] or "",
                        "outcome_label": (row[3] or "")[:64],
                        "counterfactual_pair_id": (row[4] or "")[:64],
                        # M1732: these 3 free-text fields (can contain arbitrary stone/project
                        # content) were never scrubbed before upload — added.
                        "goal_tree_snapshot": _scrub_pii((row[5] or "")[:2000]),
                        "prompt_provenance": _scrub_pii((row[6] or "")[:500]),
                        "confounder": _scrub_pii((row[7] or "")[:500]),
                        "done_at": row[8] or "",
                    })
            except Exception:
                pass
            if mc_rows and _send_raw_batch("v1/milestone-causal-batch", mc_rows):
                state["milestone_causal_last_rowid"] = mc_rows[-1]["rowid"]

            # ── stone_text batch (M1516-text) ─────────────────────────────
            # Send stone text + prompt for harness/agent training dataset.
            # Watermark on rowid; text truncated to 1000 chars.
            st_watermark = int(state.get("stone_text_last_rowid", 0))
            st_rows = []
            try:
                for row in conn.execute(
                    "SELECT rowid, stone_id, proj_id, "
                    "json_extract(data_json,'$.text'), "
                    "json_extract(data_json,'$.status'), "
                    "json_extract(data_json,'$.done_at'), "
                    "json_extract(data_json,'$.model_used') "
                    "FROM milestones_store WHERE rowid > ? ORDER BY rowid LIMIT 500",
                    (st_watermark,)
                ).fetchall():
                    _text = row[3] or ""
                    if not _text:
                        continue
                    st_rows.append({
                        "rowid": row[0], "stone_id": row[1] or "", "proj_id": row[2] or "",
                        # M1732 CRITICAL: this field was sent FULL/UNTRUNCATED despite the
                        # docstring above claiming "text truncated to 1000 chars" — confirmed
                        # via live DB read to contain real user content including health
                        # (Art.9 special-category) and financial details. Now scrubbed AND
                        # actually truncated to match the docstring's original intent.
                        "text": _scrub_pii(_text)[:1000],
                        "status": (row[4] or "")[:32],
                        "done_at": row[5] or "",
                        "model_used": (row[6] or "")[:64],
                    })
            except Exception:
                pass
            if st_rows and _send_raw_batch("v1/stone-text-batch", st_rows):
                state["stone_text_last_rowid"] = st_rows[-1]["rowid"]

        finally:
            conn.close()
    except Exception:
        pass
    return state


_AGG_STATE_FILE = _HUB_DATA_DIR / ".hub-agg-state.json"

def _maybe_upload_session_aggregate():
    """M1516: send session_aggregate once per ts_date (UTC day) — idempotent.
    Also uploads new tool_trace + action_log rows (watermark-based, every call).
    Called from startup heartbeat loop. Skipped when consent off."""
    if not _get_consent().get("data_collection", True):
        return
    import time
    today = time.strftime("%Y-%m-%d", time.gmtime())
    state = {}
    if _AGG_STATE_FILE.exists():
        try:
            state = json.loads(_AGG_STATE_FILE.read_text())
        except Exception:
            pass

    # M1516-ext: upload raw tool_trace + action_log rows (watermark-based, runs every cycle)
    state = _upload_raw_tables(state)

    if state.get("last_upload_date") == today:
        try:
            _AGG_STATE_FILE.write_text(json.dumps(state))
        except Exception:
            pass
        return  # aggregate already uploaded today

    agg = _compute_session_aggregate()
    if not agg:
        try:
            _AGG_STATE_FILE.write_text(json.dumps(state))
        except Exception:
            pass
        return
    if _send_session_aggregate(agg):
        state["last_upload_date"] = today
        state["last_install_id"] = agg["install_id"]
    try:
        _AGG_STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass


@app.get("/api/hub/session-aggregate-preview")
async def session_aggregate_preview():
    """M1516: preview what would be uploaded — for transparency / debugging."""
    agg = _compute_session_aggregate()
    return JSONResponse({
        "consent_data_collection": _get_consent().get("data_collection", True),
        "schema_version": _HUB_AGG_SCHEMA_VERSION,
        "k_min": _HUB_AGG_K_MIN,
        "aggregate": agg,
        "would_upload": bool(agg and _get_consent().get("data_collection", True)),
    })


@app.post("/api/hub/session-aggregate-upload-now")
async def session_aggregate_upload_now():
    """M1516: manual trigger for session aggregate upload (bypasses daily idempotency)."""
    if not _get_consent().get("data_collection", True):
        return JSONResponse({"ok": False, "error": "data_collection consent off"}, status_code=403)
    agg = _compute_session_aggregate()
    if not agg:
        return JSONResponse({"ok": False, "error": "no activity to aggregate"})
    sent = _send_session_aggregate(agg)
    return JSONResponse({"ok": sent, "aggregate": agg})


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
_gdrive_name_cache: dict = {}  # file_id → name, permanent in-memory cache

def _gdrive_token_from_rclone() -> str | None:
    """Read access_token from rclone.conf [gdrive] — used for direct Drive API calls."""
    import json as _json
    conf = Path.home() / ".config" / "rclone" / "rclone.conf"
    try:
        text = conf.read_text()
        in_gdrive = False
        for line in text.splitlines():
            if line.strip().startswith("["):
                in_gdrive = line.strip().lower() in ("[gdrive]",)
            elif in_gdrive and line.strip().startswith("token"):
                raw = line.split("=", 1)[1].strip()
                tok = _json.loads(raw)
                return tok.get("access_token")
    except Exception:
        pass
    return None

@app.post("/api/gdrive-filename")
async def gdrive_filename_seed(request: Request):
    """M1304: Pre-seed the in-memory filename cache (file_id → name) without any GDrive API call.
    Called at upload time when the local filename is already known.
    Body: {"id": "<gdrive_file_id>", "name": "<filename>"}"""
    try:
        body = await request.json()
        fid = (body.get("id") or "").strip()
        name = (body.get("name") or "").strip()
        if fid and name:
            _gdrive_name_cache[fid] = name
            return JSONResponse({"ok": True, "cached": True})
    except Exception:
        pass
    return JSONResponse({"ok": False})


@app.get("/api/gdrive-filename")
async def gdrive_filename(id: str = ""):
    """M1295: Resolve GDrive file ID → filename.
    Strategy: in-memory cache → GDrive API (fast, ~100ms) → rclone lsjson fallback."""
    if not id or len(id) < 10:
        return JSONResponse({"ok": False, "name": None})
    # Cache hit — instant
    if id in _gdrive_name_cache:
        return JSONResponse({"ok": True, "name": _gdrive_name_cache[id], "cached": True})
    import json as _json
    # Fast path: direct Google Drive API v3 using rclone oauth token
    token = _gdrive_token_from_rclone()
    if token:
        try:
            import urllib.request as _ur
            _gd_req = _ur.Request(
                f"https://www.googleapis.com/drive/v3/files/{id}?fields=name",
                headers={"Authorization": f"Bearer {token}"}
            )
            def _do_gdrive_fetch():
                with _ur.urlopen(_gd_req, timeout=4) as resp:
                    return _json.loads(resp.read())
            data = await asyncio.to_thread(_do_gdrive_fetch)  # M1324-P2: non-blocking
            name = data.get("name")
            if name:
                _gdrive_name_cache[id] = name
                return JSONResponse({"ok": True, "name": name})
        except Exception:
            pass
    # Fallback: rclone lsjson per-project outbox
    import shutil as _sh
    if not _sh.which("rclone"):
        return JSONResponse({"ok": False, "name": None})
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        for proj in ["MOAT", "Sales", "FRWP", "UniversEye", "Hub", "Crons"]:
            r2 = await loop.run_in_executor(None, lambda p=proj: subprocess.run(
                ["rclone", "lsjson", f"gdrive:claude-shared/{p}/outbox/"],
                capture_output=True, text=True, timeout=6
            ))
            if r2.returncode == 0:
                for item in _json.loads(r2.stdout or "[]"):
                    if item.get("ID") == id:
                        name = item.get("Name")
                        if name:
                            _gdrive_name_cache[id] = name
                        return JSONResponse({"ok": True, "name": name})
    except Exception as e:
        return JSONResponse({"ok": False, "name": None, "error": str(e)})
    return JSONResponse({"ok": False, "name": None})


@app.get("/api/setup/rclone-status")
async def get_rclone_status():
    """M1265: Check if rclone gdrive remote is configured — used for new-user onboarding banner."""
    import shutil as _sh
    rclone_bin = _sh.which("rclone")
    if not rclone_bin:
        return JSONResponse({"ok": True, "rclone_installed": False, "gdrive_configured": False,
                             "setup_required": True,
                             "install_hint": "Install rclone: curl https://rclone.org/install.sh | sudo bash"})
    conf = Path.home() / ".config" / "rclone" / "rclone.conf"
    if not conf.exists():
        return JSONResponse({"ok": True, "rclone_installed": True, "gdrive_configured": False,
                             "setup_required": True,
                             "setup_hint": "Run: rclone config  →  n (new remote) → name: gdrive → Google Drive → OAuth in browser"})
    # Check if a drive-type remote exists
    try:
        r = subprocess.run(["rclone", "listremotes"], capture_output=True, text=True, timeout=5)
        remotes = [x.rstrip(":") for x in r.stdout.splitlines() if x.strip()]
        # Check each remote's type
        drive_remotes = []
        for rem in remotes:
            tr = subprocess.run(["rclone", "config", "show", rem], capture_output=True, text=True, timeout=5)
            if "type = drive" in tr.stdout:
                drive_remotes.append(rem)
        if not drive_remotes:
            return JSONResponse({"ok": True, "rclone_installed": True, "gdrive_configured": False,
                                 "setup_required": True,
                                 "setup_hint": "Run: rclone config  →  n → name: gdrive → Google Drive → OAuth"})
        return JSONResponse({"ok": True, "rclone_installed": True, "gdrive_configured": True,
                             "setup_required": False, "drive_remotes": drive_remotes})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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

    # BUG-01/02: --help and --version must not start the server
    if args and args[0] in ("--help", "-h", "help"):
        print(
            "claude-ns-hub — Personal AI project hub for Claude Code users.\n\n"
            "Usage: hub [COMMAND] [OPTIONS]\n\n"
            "Commands:\n"
            "  (no command)       Start the hub server\n"
            "  install-global     Register MCP + hooks in ~/.claude/settings.json\n"
            "  init <proj> --dir  Register a project directory\n"
            "  doctor             Run system health checks\n"
            "  configure          Set default agent/model/hub_url\n\n"
            "Options:\n"
            "  --port PORT        Bind to specific port (default: 9001)\n"
            "  --db PATH          Use custom SQLite DB path\n"
            "  --help             Show this help message\n"
            "  --version          Show version and exit\n\n"
            "Quick start:\n"
            "  hub                      # start server at http://localhost:9001\n"
            "  hub install-global       # one-time setup (run after install)\n"
            "  hub doctor               # verify all components are working\n"
        )
        return

    if args and args[0] in ("--version", "-V", "version"):
        from importlib.metadata import version as _pkg_version
        try:
            _v = _pkg_version("claude-ns-hub")
        except Exception:
            _v = "unknown"
        print(f"claude-ns-hub {_v}")
        return

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

    if args and args[0] == "doctor":
        _hub_doctor(exit_code="--exit-code" in args)
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

    corpus_proc = _spawn_entity_corpus()
    if corpus_proc:
        atexit.register(corpus_proc.terminate)
    actual_port = _find_free_port(PORT)
    if actual_port != PORT:
        print(f"[hub] Port {PORT} occupied — binding {actual_port} instead.")
    # M929: first-run startup banner — confirms ALL features active so new users
    # see the hub came up correctly and know what's enabled (incl. telemetry).
    try:
        _ver = _hub_pkg_version()
    except Exception:
        _ver = "unknown"
    _consent_on = bool(_get_consent().get("data_collection", True))
    _tel_on = "ON (Turso, no PII)" if _consent_on and _TEL_ENABLED else "OFF"
    # F3: detect first-run (MCP not registered) and show next-step guide
    _settings_path = Path.home() / ".claude" / "settings.json"
    try:
        _settings_data = json.loads(_settings_path.read_text()) if _settings_path.exists() else {}
    except Exception:
        _settings_data = {}
    _mcp_registered = "ns-hub" in _settings_data.get("mcpServers", {})
    _banner_lines = [
        "",
        f"  claude-ns-hub v{_ver}",
        f"  ──────────────────────────────────────────────",
        f"   Dashboard:   http://localhost:{actual_port}/northstar",
        f"   Mobile:      http://{_tailscale_interface_ip()}:{actual_port}/northstar",
        f"   Bind:        {HOST}:{actual_port}  (all interfaces)",
        f"   Entity corpus:  {'spawned' if corpus_proc else 'disabled'}",
        f"   Tailscale expose:  attempted (port {actual_port})",
        f"   Telemetry:   {_tel_on}",
        f"   Opt-out:     POST /api/hub/consent  body {{\"data_collection\": false}}",
        f"   Quickstart:  https://github.com/pluto2060/claude-ns-hub#quick-start",
        f"  ──────────────────────────────────────────────",
    ]
    if not _mcp_registered:
        _banner_lines += [
            f"",
            f"  ⚠  First-run setup required:",
            f"     1) hub install-global          # register MCP + hooks in Claude Code",
            f"     2) hub init <proj_id> --dir .  # register this project",
            f"     3) Open Dashboard → create a Stone",
            f"     Then restart Claude Code to pick up MCP.",
            f"",
        ]
    else:
        _banner_lines.append("")
    for _ln in _banner_lines:
        print(_ln)
    # M1345 P0: enable access_log so input → response correlation is measurable.
    try:
        uvicorn.run(app, host=HOST, port=actual_port,
                    log_level="warning", access_log=True)
    except Exception as _boot_exc:
        # M1348 P1: boot_fail telemetry — silent startup failure detection
        _record_usage_event("boot_fail", {"error": str(_boot_exc)[:200], "port": actual_port})
        raise


if __name__ == "__main__":
    main()
