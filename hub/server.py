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

HOST = os.environ.get("HUB_HOST", "127.0.0.1")
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
                    return "127.0.0.1" if ip == "0.0.0.0" else ip
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
                    data.setdefault("milestones", [])
                    data.setdefault("log", [])
                    data.setdefault("deadline", "")
                    data.setdefault("links", "")
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
_MS_TTL = 300  # 5 min


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
_CH_TTL = 180  # 3 min (channels don't change faster)

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
        "url": "https://news.ycombinator.com/item?id=48017090",
        "api": "https://hn.algolia.com/api/v1/items/48017090",
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


@app.get("/api/northstar/sessions")
async def ns_sessions():
    """Return terminal session status for all projects."""
    result = {}
    now = time.time()
    for proj_id, proc in list(_sessions.items()):
        if not proc.isalive():
            result[proj_id] = "dead"
        elif proj_id in _session_idle_since:
            idle_secs = int(now - _session_idle_since[proj_id])
            result[proj_id] = f"idle:{idle_secs}"
        else:
            result[proj_id] = "active"
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
