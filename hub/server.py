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
import time
from pathlib import Path

import yaml as _yaml

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import httpx
import ptyprocess

HERE = Path(__file__).parent
STATIC = HERE / "static"
NORTHSTAR_DATA = HERE / "northstar.json"        # legacy fallback
PROJECTS_DIR = HERE / "projects"                 # new: per-project markdown files

HOST = os.environ.get("HUB_HOST", "0.0.0.0")
PORT = int(os.environ.get("HUB_PORT", "9000"))


def _tailscale_interface_ip() -> str:
    """Get the IP assigned to the Tailscale interface (100.x.x.x/32)."""
    try:
        r = subprocess.run(["ip", "addr", "show"], capture_output=True, text=True, timeout=2)
        m = re.search(r"(100\.\d+\.\d+\.\d+)/32", r.stdout)
        if m:
            return m.group(1)
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
    return "127.0.0.1"


def _ctx_url() -> str:
    return f"http://{_bound_ip(8787)}:8787"


def _corpus_url() -> str:
    ip = _bound_ip(8989)
    return f"http://{ip}:8989"


SERVICES = {
    "ctx":    {"port": 8787, "label": "CTX",    "url": _ctx_url()},
    "corpus": {"port": 8989, "label": "Corpus", "url": _corpus_url()},
}

app = FastAPI(title="Hub", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


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
        "ctx_ip":             ctx_url.split("//")[1].split(":")[0],
    })


# ── North Star — file-backed multi-project manager ───────────────────────────

def _parse_md_frontmatter(path: Path) -> dict:
    """Extract YAML frontmatter from a markdown file."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    try:
        data = _yaml.safe_load(parts[1]) or {}
        data["_body"] = parts[2].strip()
        return data
    except Exception:
        return {}


def _write_md_frontmatter(path: Path, data: dict):
    """Write YAML frontmatter back to a markdown file, preserving body."""
    body = data.pop("_body", "")
    # Strip internal-only fields
    data.pop("file_path", None)
    # Re-encode milestones/log as proper YAML types
    text = "---\n" + _yaml.dump(data, allow_unicode=True, default_flow_style=False) + "---\n\n" + body
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _load_projects() -> list:
    """Load all projects from projects/*/north-star.md, fallback to northstar.json."""
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
                    data.setdefault("x", None)
                    data.setdefault("y", None)
                    data.setdefault("stage", "unassigned")  # lifecycle stage
                    if not isinstance(data.get("connections"), list):
                        data["connections"] = []
                    # Compute staleness
                    mtime = md.stat().st_mtime
                    data["last_updated"] = mtime
                    data["stale"] = (time.time() - mtime) > (14 * 86400)
                    projects.append(data)
    if not projects and NORTHSTAR_DATA.exists():
        # Legacy fallback
        projects = json.loads(NORTHSTAR_DATA.read_text())
    return projects


def _save_project(proj_id: str, data: dict):
    """Save project data back to its north-star.md file."""
    proj_dir = PROJECTS_DIR / proj_id
    md = proj_dir / "north-star.md"
    if md.exists():
        existing = _parse_md_frontmatter(md)
        body = existing.get("_body", "")
        data["_body"] = body
        _write_md_frontmatter(md, data)
    else:
        # New project — create file
        data["_body"] = f"# {data.get('name', proj_id)} — North Star\n\n## Strategy\n\n## OKRs\n"
        _write_md_frontmatter(md, data)


@app.get("/northstar")
async def northstar_page():
    return FileResponse(str(STATIC / "northstar.html"))


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
        # Also keep legacy JSON in sync
        NORTHSTAR_DATA.write_text(json.dumps(data, ensure_ascii=False, indent=2))
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
            ["claude", "-p", "--model", "claude-haiku-4-5-20251001", prompt],
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

    # Write back to north-star.md
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if md.exists():
        data = _parse_md_frontmatter(md)
        # Merge updated done states back
        for i, ms in enumerate(updated):
            if i < len(data.get("milestones", [])):
                data["milestones"][i]["done"] = ms.get("done", False)
        _write_md_frontmatter(md, data)

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
    _write_md_frontmatter(md, data)
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
        _write_md_frontmatter(md, data)
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
            ["claude", "-p", "--model", "claude-haiku-4-5-20251001", prompt],
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
            ["claude", "-p", "--model", "claude-haiku-4-5-20251001", prompt],
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
    return FileResponse(str(STATIC / "market-signals.html"))


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
        "id": "naver_mail",
        "name": "Naver Mail",
        "url": "https://mail.naver.com/",
        "api": "https://mail.naver.com/json/readMail/",
        "type": "naver_mail",
        "note": "nave94@naver.com — GitHub/GN/HN reply notifications",
    },
    {
        "id": "pypi",
        "name": "PyPI",
        "url": "https://pypi.org/project/ctx-retriever/",
        "api": "https://pypistats.org/api/packages/ctx-retriever/recent",
        "type": "pypi_api",
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
            app_pw = _os.environ.get("NAVER_APP_PW", "7RZ2YNB1XEC8")
            ctx2 = _ssl.create_default_context()
            mail = imaplib.IMAP4_SSL("imap.naver.com", 993, ssl_context=ctx2)
            mail.login("nave94@naver.com", app_pw)
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


CELI_CONFIG = Path.home() / ".config/tmux-csk-sessions.conf"

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
    # 2. Common base paths
    for base in [Path.home() / "Project", Path.home() / "Project/VIDraft"]:
        p = base / proj_id
        if p.exists(): return str(p)
        # Case-insensitive scan
        if base.exists():
            for d in base.iterdir():
                if d.name.lower() == proj_id.lower() and d.is_dir():
                    return str(d)
    return None


# Persistent session registry: proj_id → PtyProcess (survives WS disconnect)
_sessions: dict[str, ptyprocess.PtyProcess] = {}
# Scrollback buffer: proj_id → accumulated PTY output (last 64 KB)
_buffers: dict[str, list] = {}
_BUFFER_MAX = 65536
# Idle tracking: proj_id → timestamp of last WS detach
_session_idle_since: dict[str, float] = {}
_SESSION_TTL = 30 * 60  # kill after 30 min idle


def _kill_session(proj_id: str) -> None:
    """Terminate a session and clean up all registries."""
    proc = _sessions.pop(proj_id, None)
    _buffers.pop(proj_id, None)
    _session_idle_since.pop(proj_id, None)
    if proc:
        try:
            proc.terminate(force=True)
        except Exception:
            pass


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
async def _start_milestone_watcher():
    """Background cron: check completion-log.jsonl every 5 min → auto-mark queued milestones."""
    async def _watch():
        while True:
            await asyncio.sleep(300)  # 5 minutes
            try:
                for proj_dir in PROJECTS_DIR.iterdir():
                    if not proj_dir.is_dir():
                        continue
                    proj_id = proj_dir.name
                    log_file = proj_dir / "completion-log.jsonl"
                    if not log_file.exists():
                        continue
                    # Read completion log entries
                    entries = []
                    for line in log_file.read_text().splitlines():
                        line = line.strip()
                        if line:
                            try: entries.append(json.loads(line))
                            except Exception: pass
                    if not entries:
                        continue
                    logged_mids = {e.get("milestone_id"): e for e in entries if e.get("milestone_id")}
                    # Load milestones via internal function (normalized)
                    md = PROJECTS_DIR / proj_id / "north-star.md"
                    if not md.exists():
                        continue
                    proj = _parse_md_frontmatter(md)
                    raw_ms = proj.get("milestones", []) if isinstance(proj, dict) else []
                    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M")
                    for m in raw_ms:
                        if not isinstance(m, dict): continue
                        mid = str(m.get("id", "")).strip()
                        if not mid: continue
                        status = m.get("status", "pending")
                        if status in ("done", "pending_confirmation") or m.get("done"): continue
                        if mid in logged_mids:
                            patch = {"status": "pending_confirmation", "done": False,
                                     "pending_confirm_at": now_iso, "claude_ack": now_iso}
                            m.update(patch)
                            proj["milestones"] = raw_ms
                            _save_project(proj_id, proj)
            except Exception:
                pass
    asyncio.create_task(_watch())


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


def _spawn_claude(proj_id: str) -> ptyprocess.PtyProcess:
    proj_dir = _get_project_dir(proj_id) or str(Path.home())
    return ptyprocess.PtyProcessUnicode.spawn(
        ["claude", "--dangerously-skip-permissions", "--continue"],
        cwd=proj_dir,
        dimensions=(30, 120),
        env={**os.environ, "TERM": "xterm-256color", "COLUMNS": "120", "LINES": "30"},
    )


@app.websocket("/ws/session/{proj_id}")
async def terminal_session(websocket: WebSocket, proj_id: str):
    """Bridge WebSocket ↔ persistent PTY. Reuses existing session on reconnect."""
    await websocket.accept()

    # Reuse existing session if alive, otherwise spawn new
    proc = _sessions.get(proj_id)
    if proc is None or not proc.isalive():
        _buffers.pop(proj_id, None)  # clear stale buffer
        try:
            proc = _spawn_claude(proj_id)
            _sessions[proj_id] = proc
        except Exception as e:
            await websocket.send_text(f"\r\n[Failed to start claude: {e}]\r\n")
            await websocket.close()
            return
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

    loop = asyncio.get_event_loop()

    async def pty_to_ws():
        while True:
            if not proc.isalive():
                # Process died naturally — clean up registry
                _sessions.pop(proj_id, None)
                try:
                    await websocket.send_text("\r\n\x1b[33m[Session ended]\x1b[0m\r\n")
                except Exception:
                    pass
                break
            try:
                data = await loop.run_in_executor(None, lambda: proc.read(4096))
            except EOFError:
                # PTY closed — process exited
                _sessions.pop(proj_id, None)
                _buffers.pop(proj_id, None)
                try:
                    await websocket.send_text("\r\n\x1b[33m[Session ended]\x1b[0m\r\n")
                except Exception:
                    pass
                break
            except Exception:
                # PTY read error — stop this reader but keep session alive
                break
            # Accumulate into scrollback buffer (cap at 64 KB)
            buf = _buffers.setdefault(proj_id, [])
            buf.append(data)
            total = sum(len(c) for c in buf)
            while total > _BUFFER_MAX and buf:
                total -= len(buf.pop(0))
            try:
                await websocket.send_text(data)
            except Exception:
                # WS send failed (client disconnected) — stop forwarding, keep proc alive
                break

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
    projects = _load_projects()
    p = next((p for p in projects if p.get("id") == proj_id), None)
    if not p:
        return JSONResponse({"ok": False, "north_stars": []})
    ns_list = p.get("north_stars")
    if ns_list:
        # Normalize milestones inside each NS
        for ns in ns_list:
            raw_ms = ns.get("milestones", [])
            norm = []
            for i, m in enumerate(raw_ms):
                if isinstance(m, dict):
                    norm.append({
                        "id":   m.get("id", f"{ns['id']}_M{i+1}"),
                        "text": str(m.get("text", "")),
                        "done": bool(m.get("done", False)),
                        "ns_id": ns["id"],
                    })
            ns["milestones"] = norm
        return JSONResponse({"ok": True, "north_stars": ns_list})
    # Fallback: wrap legacy milestones into a single NS
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
    _write_md_frontmatter(md, proj)
    return JSONResponse({"ok": True})


@app.get("/api/northstar/{proj_id}/milestones")
async def get_milestones(proj_id: str):
    projects = _load_projects()
    p = next((p for p in projects if p.get("id") == proj_id), None)
    if not p:
        return JSONResponse({"ok": False, "milestones": []})
    return JSONResponse({"ok": True, "milestones": p.get("milestones", [])})


@app.post("/api/northstar/{proj_id}/milestones")
async def create_milestone(proj_id: str, request: Request):
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
    # Generate ID: M{n} for layer 0, M{parent}.{n} for layer 1
    if layer == 0:
        n = sum(1 for m in milestones if isinstance(m, dict) and m.get("layer", 0) == 0) + 1
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
    new_ms = {
        "id": new_id, "text": data.get("text", "New milestone"),
        "layer": layer, "parent_id": parent_id,
        "done": False, "claude_ack": None,
        "user_added_at": _dt.now().strftime("%Y-%m-%dT%H:%M"),  # badge for unacknowledged
    }
    milestones.append(new_ms)
    proj["milestones"] = milestones
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "milestone": new_ms})


@app.patch("/api/northstar/{proj_id}/milestones/{mid}")
async def update_milestone(proj_id: str, mid: str, request: Request):
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    milestones = proj.get("milestones", [])
    updated = False
    now_iso = __import__('datetime').datetime.now().strftime("%Y-%m-%dT%H:%M")
    for m in milestones:
        if isinstance(m, dict) and m.get("id") == mid:
            # User-settable fields: text, layer, parent_id, claude_ack, status=queued/pending only
            for k in ("text", "layer", "parent_id", "claude_ack", "cron_job_id", "claude_comment"):
                if k in data:
                    m[k] = data[k] if data[k] else None
            # Status: user can set pending/queued; done is Claude-only (set via done=True or status=done)
            new_status = data.get("status")
            if new_status in ("pending", "queued"):
                m["status"] = new_status
                m["done"] = False
                if new_status == "queued":
                    m.setdefault("queued_at", now_iso)
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
                m.setdefault("pending_confirm_at", now_iso)
                if "claude_ack" not in data:
                    m["claude_ack"] = now_iso
            elif new_status == "done" or data.get("done") is True:
                # Claude-only OR user confirmed: mark done
                m["status"] = "done"
                m["done"] = True
                m["done_at"] = now_iso
                m.pop("pending_confirm_at", None)
                if "claude_ack" not in data:
                    m["claude_ack"] = now_iso
            # Old-style done=False (from auto-ack) — treat as pending
            elif data.get("done") is False:
                if m.get("status") != "queued":
                    m["status"] = "pending"
                m["done"] = False
            updated = True
            break
    if not updated:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    proj["milestones"] = milestones
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/milestones/{mid}")
async def delete_milestone(proj_id: str, mid: str):
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    before = len(proj.get("milestones", []))
    # Remove milestone and all its children
    proj["milestones"] = [m for m in proj.get("milestones", [])
                          if isinstance(m, dict) and m.get("id") != mid
                          and m.get("parent_id") != mid]
    _save_project(proj_id, proj)
    return JSONResponse({"ok": True, "removed": before - len(proj["milestones"])})


@app.post("/api/northstar/{proj_id}/milestones/{mid}/run")
async def run_milestone(proj_id: str, mid: str):
    """Queue milestone for injection into the active Claude Code session via UserPromptSubmit hook."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    milestone = next((m for m in proj.get("milestones", []) if m.get("id") == mid), None)
    if not milestone:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)

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
    inbox = Path.home() / ".claude/hub/session-inbox.jsonl"
    entry = {
        "ts": _dt_.now().isoformat(timespec="seconds"),
        "proj_id": proj_id,
        "mid": mid,
        "text": milestone.get("text", ""),
        "status": milestone.get("status", "pending"),
        "hub_api": hub_api,
    }
    with open(inbox, "a") as f:
        f.write(__import__("json").dumps(entry, ensure_ascii=False) + "\n")
    return JSONResponse({"ok": True, "queued": True, "message": "Injected into Claude session on next prompt"})


@app.get("/api/northstar/{proj_id}/tmux-output")
async def get_tmux_output(proj_id: str, lines: int = 20):
    """Return latest output from the tmux execute session."""
    session_name = f"claude-exec-{proj_id}"
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
    """Return queued + running + completed tasks for a project (job board view)."""
    queue_dir = HERE / "task-queue"
    results_dir = HERE / "task-results"
    locks_dir = HERE / "task-locks"
    pattern = f"task-{proj_id}-*"

    jobs = {}
    # Queued (task file exists, no result, no lock)
    for f in sorted((queue_dir).glob(pattern) if queue_dir.exists() else []):
        tid = f.stem
        result = results_dir / f"{tid}.json"
        lock = locks_dir / f"{tid}.lock"
        if not result.exists() and not lock.exists():
            jobs[tid] = {"status": "queued", "task_id": tid, "output": None}

    # Running (lock exists, no result)
    for f in sorted((locks_dir).glob(pattern) if locks_dir.exists() else []):
        tid = f.stem
        result = results_dir / f"{tid}.json"
        if not result.exists():
            jobs[tid] = {"status": "running", "task_id": tid, "output": None}

    # Completed (result exists)
    if results_dir.exists():
        for f in sorted(results_dir.glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
            try:
                data = __import__("json").loads(f.read_text())
                tid = f.stem
                jobs[tid] = {
                    "status": data.get("status", "done"),
                    "task_id": tid,
                    "output": (data.get("output") or "")[:200],
                    "completed_at": data.get("completed_at", ""),
                }
            except Exception:
                pass

    board = sorted(jobs.values(), key=lambda j: (
        0 if j["status"] == "running" else 1 if j["status"] == "queued" else 2
    ))
    return JSONResponse({"ok": True, "jobs": board})


@app.get("/api/northstar/{proj_id}/task-results")
async def get_task_results(proj_id: str, limit: int = 5):
    """Return recent task execution results for a project."""
    results_dir = HERE / "task-results"
    if not results_dir.exists():
        return JSONResponse({"ok": True, "results": []})
    results = []
    pattern = f"task-{proj_id}-*"
    files = sorted(results_dir.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True)
    for f in files[:limit]:
        try:
            data = __import__("json").loads(f.read_text())
            results.append({
                "task_id": data.get("task_id", f.stem),
                "status": data.get("status", "?"),
                "output": (data.get("output", "") or "")[:400],
                "completed_at": data.get("completed_at", ""),
            })
        except Exception:
            pass
    return JSONResponse({"ok": True, "results": results})


@app.post("/api/northstar/{proj_id}/execute")
async def execute_project(proj_id: str):
    """Smart dispatcher: if no milestones → init roadmap; if milestones exist → process queued work.
    Writes to session-inbox.jsonl for pickup by UserPromptSubmit hook."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
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
        actionable = [m for m in active_ms if m.get("status") in ("queued", "pending", "needs_clarification")][:5]
        session_name = f"claude-exec-{proj_id}"
        proj_dirs = {
            "MOAT": "/home/desk-1/Project/Moat", "CTX": "/home/desk-1/Project/CTX",
            "FromScratch": "/home/desk-1/Project/FromScratch",
            "HugwartsBanana": "/home/desk-1/Project/VIDraft/HugwartsBanana",
            "AIKB": "/home/desk-1/Project/AIKB", "FRWP": "/home/desk-1/Project/FRWP",
        }
        proj_dir = proj_dirs.get(proj_id, str(Path.home() / "Project" / proj_id))

        # M24 fix: if tmux session already running, return status instead of spawning duplicate
        existing = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
        if existing.returncode == 0:
            # Session already active — return status, don't duplicate
            return JSONResponse({
                "ok": True, "mode": "tmux_active",
                "session": session_name,
                "message": f"Claude is already working on {proj_id} stones (session '{session_name}' active). Check live session for progress.",
            })

        if actionable:
            stone_lines = "\n".join(
                f"  {m.get('id')} [{m.get('status')}]: \"{m.get('text','')[:60]}\""
                for m in actionable
            )
            # Include ALL milestones for full sync context
            all_ms_lines = "\n".join(
                f"  {m.get('id')} [{m.get('status')}]: \"{m.get('text','')[:70]}\""
                for m in active_ms
            )
            # Write to persistent task queue (crash-recovery watcher picks this up)
            task_queue_file = PROJECTS_DIR / proj_id / "task-queue.jsonl"
            task_queue_file.parent.mkdir(parents=True, exist_ok=True)
            sync_task = {
                "id": f"sync-{ts}",
                "type": "execute_sync",
                "status": "pending",
                "created_at": _dt_.now().isoformat(timespec="seconds"),
                "prompt": (
                    f"[EXECUTE SYNC] Project {proj_id} — user clicked Execute.\n\n"
                    f"Step 1 — MILESTONE SYNC: Review ALL active stones. "
                    f"PATCH claude_ack=now on {hub_api}/api/northstar/{proj_id}/milestones/MID. "
                    f"Clear text → queued. Vague/incomplete → needs_clarification.\n\n"
                    f"Step 2 — For each queued stone, implement it: "
                    f"write completion-log.jsonl, PATCH pending_confirmation.\n\n"
                    f"Step 3 — Update spec doc to reflect current milestone roadmap.\n\n"
                    f"All active milestones:\n{all_ms_lines}"
                )
            }
            with open(task_queue_file, "a") as f:
                f.write(json.dumps(sync_task, ensure_ascii=False) + "\n")

            # Also spawn tmux session for immediate processing (watcher handles retries)
            cron_prompt = (
                f"[EXECUTE SYNC] Project {proj_id} — user just clicked Execute after setting milestones.\n\n"
                f"Step 1 — MILESTONE SYNC: Review ALL active stones below. "
                f"For each unreviewed (claude_ack=null): PATCH claude_ack=now on "
                f"{hub_api}/api/northstar/{proj_id}/milestones/MID. "
                f"If text is clear → PATCH status=queued. "
                f"If text is vague/incomplete → PATCH status=needs_clarification + clarification_question.\n\n"
                f"Step 2 — CREATE CRONS: For each queued stone, create CronCreate("
                f"cron='*/2 * * * *', recurring=False) that implements the stone, "
                f"writes completion-log.jsonl, patches pending_confirmation, CronDeletes itself.\n\n"
                f"Step 3 — SPEC DOC UPDATE: After syncing milestones, update the project spec doc. "
                f"Read the current spec doc (if exists via {hub_api}/api/northstar/{proj_id}/doc), "
                f"then update it to reflect the current milestone roadmap and progress. "
                f"Save back via the project's markdown file. This publishes the updated plan.\n\n"
                f"All active milestones:\n{all_ms_lines}"
            )
            # Kill existing session if any, start fresh
            subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)
            subprocess.Popen([
                "tmux", "new-session", "-d", "-s", session_name,
                "-c", proj_dir if Path(proj_dir).exists() else str(Path.home()),
                "claude", "--dangerously-skip-permissions", "--continue"
            ])
            # Wait for claude to start, then send the prompt
            import asyncio as _aio
            await _aio.sleep(3)
            subprocess.run(["tmux", "send-keys", "-t", session_name, cron_prompt, "Enter"])
            return JSONResponse({
                "ok": True, "mode": "tmux",
                "session": session_name,
                "tasks_created": len(actionable),
                "message": f"Spawned tmux session '{session_name}' — {len(actionable)} cron jobs being created"
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
                f"4. On completion: append to ~/.claude/hub/projects/{proj_id}/completion-log.jsonl:\n"
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
        "name": name, "metric": "—", "current": "—", "target": "—",
        "status": "paused", "deadline": "", "note": "",
        "milestones": [], "log": [], "connections": [],
        "layer": 0, "x": None, "y": None,
    }
    if repo_path:
        frontmatter["repo_path"] = repo_path
    text = "---\n" + _yaml.dump(frontmatter, allow_unicode=True, default_flow_style=False) + "---\n\n" + body
    md.write_text(text, encoding="utf-8")
    return JSONResponse({"ok": True, "id": folder_id})


@app.delete("/api/northstar/{proj_id}")
async def delete_project(proj_id: str):
    """Delete a project node (removes north-star.md)."""
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
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


@app.patch("/api/northstar/{proj_id}/layout")
async def update_layout(proj_id: str, request: Request):
    """Update swimlane layout fields: layer, parent, position_x."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False}, status_code=404)
    proj = _parse_md_frontmatter(md)
    for k in ("layer", "parent", "position_x", "x", "y", "repo_path", "stage"):
        if k in data:
            proj[k] = data[k]
    _save_project(proj_id, proj)
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


@app.get("/api/northstar/sessions")
async def ns_sessions():
    """Return terminal session status for all projects."""
    result = {}
    now = time.time()

    # WS-connected sessions
    for proj_id, proc in list(_sessions.items()):
        if not proc.isalive():
            result[proj_id] = "dead"
        elif proj_id in _session_idle_since:
            idle_secs = int(now - _session_idle_since[proj_id])
            result[proj_id] = f"idle:{idle_secs}"
        else:
            result[proj_id] = "active"

    # Override with explicit hook-set status (WAITING, DONE, etc.)
    for proj_id, status in _pill_status.items():
        if status == "WAITING":
            result[proj_id] = "waiting"
        elif status == "DONE" and proj_id not in result:
            result[proj_id] = "done"
        elif status == "IDLE" and proj_id not in result:
            pass  # IDLE is the default (no entry)

    return JSONResponse(result)


@app.get("/health/{service}")
async def health(service: str):
    if service in ("northstar", "market-signals"):
        return JSONResponse({"ok": True})
    svc = SERVICES.get(service)
    if not svc:
        return JSONResponse({"ok": False, "error": "unknown service"}, status_code=404)
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            r = await client.get(svc["url"])
            return JSONResponse({"ok": r.status_code < 500, "status": r.status_code})
    except Exception:
        return JSONResponse({"ok": False, "status": 0})


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
