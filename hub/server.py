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
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import httpx
import ptyprocess

HERE = Path(__file__).parent
STATIC = HERE / "static"
PROJECTS_DIR = HERE / "projects"

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

# M210 follow-up: when this app is served over plain HTTP, redirect every request to the
# HTTPS endpoint so the browser unlocks the Notification API. Same uvicorn process can
# run two instances (HTTP:9000 → redirect, HTTPS:9443 → serve normally); the request.url.scheme
# is "http" only on the HTTP listener, so this middleware no-ops on the HTTPS listener.
from fastapi import Request
from fastapi.responses import RedirectResponse

_HTTPS_HOST = "desk-1-1.tailb5ab18.ts.net"
_HTTPS_PORT = 9443

@app.middleware("http")
async def _force_https_redirect(request: Request, call_next):
    if request.url.scheme == "http":
        target = f"https://{_HTTPS_HOST}:{_HTTPS_PORT}{request.url.path}"
        if request.url.query:
            target += f"?{request.url.query}"
        return RedirectResponse(url=target, status_code=302)
    return await call_next(request)

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
    # Find the closing --- using line-by-line scan (avoids splitting on --- inside YAML values)
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
        return data
    except Exception:
        return {}


def _write_md_frontmatter(path: Path, data: dict):
    """Write YAML frontmatter back to a markdown file, preserving body."""
    body = data.pop("_body", "")
    # Strip internal-only fields
    data.pop("file_path", None)
    yaml_str = _yaml.dump(data, allow_unicode=True, default_flow_style=False)
    # Ensure yaml_str doesn't accidentally contain a bare '---' line that would break parsing
    yaml_str = re.sub(r'(?m)^---$', "'---'", yaml_str)
    text = "---\n" + yaml_str + "---\n\n" + body
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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
        # Make absolute relative to user's home if a bare relative path was given.
        if not p.is_absolute():
            p = (Path.home() / p).resolve()
        else:
            p = p.resolve()
        if p.exists() and p.is_dir():
            return False, str(p)
        p.mkdir(parents=True, exist_ok=True)
        return True, str(p)
    except (PermissionError, OSError):
        return False, repo_path


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


_PYPI_DAILY_CACHE: dict = {"data": None, "ts": 0.0}
_PYPI_DAILY_TTL = 3600  # 1 hr


def _fetch_pypi_daily_sync(days: int = 30) -> dict:
    import urllib.request as _ur
    try:
        req = _ur.Request(
            "https://pypistats.org/api/packages/ctx-retriever/overall?total=daily",
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
        return {"days": rows[-days:], "package": "ctx-retriever"}
    except Exception as exc:
        return {"error": str(exc)[:120]}


@app.get("/api/pypi-daily")
async def pypi_daily_api(refresh: bool = False, days: int = 30):
    import time
    now = time.time()
    if refresh or _PYPI_DAILY_CACHE["data"] is None or (now - _PYPI_DAILY_CACHE["ts"]) > _PYPI_DAILY_TTL:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: _fetch_pypi_daily_sync(days))
        _PYPI_DAILY_CACHE["data"] = data
        _PYPI_DAILY_CACHE["ts"] = now
    return JSONResponse({**_PYPI_DAILY_CACHE["data"], "cached_at": _PYPI_DAILY_CACHE["ts"]})


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

                    # Completion-log sync: mark logged milestones as pending_confirmation
                    log_file = proj_dir / "completion-log.jsonl"
                    if log_file.exists():
                        entries = []
                        for line in log_file.read_text().splitlines():
                            line = line.strip()
                            if line:
                                try: entries.append(json.loads(line))
                                except Exception: pass
                        logged_mids = {e.get("milestone_id") for e in entries if e.get("milestone_id")}
                        for m in raw_ms:
                            if not isinstance(m, dict): continue
                            mid = str(m.get("id", "")).strip()
                            status = m.get("status", "pending")
                            if status in ("done", "pending_confirmation") or m.get("done"): continue
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
                        session_name = f"claude-exec-{proj_id}"
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
                                    f"  5. Replying does NOT change the stone's status\n\n"
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


def _encode_cwd_for_claude(cwd: str) -> str:
    """Replicate Claude Code's transcript-dir encoding: every non-alphanumeric
    character is replaced with `-`. Source: research/20260513-tmux-claude-session-resume-design.md FACT-3.
    """
    import re
    return re.sub(r"[^a-zA-Z0-9]", "-", cwd)


def _get_resume_args(proj_id: str, proj_dir: str) -> list:
    """Return Claude resume flags for tmux/PTY spawn continuity.

    Spec: docs/research/20260513-tmux-claude-session-resume-design.md MVF #2.
    Looks up the last session id for the project's CURRENT model first
    (.session-history.json keyed by model — switching back to a previous
    model resumes its own thread). Falls back to .last-session-id for
    backward compat. Verifies the transcript exists+non-empty at the
    canonical encoded-cwd path before returning --resume.
    """
    pdir = PROJECTS_DIR / proj_id
    encoded = _encode_cwd_for_claude(str(proj_dir))
    transcripts_dir = Path.home() / ".claude" / "projects" / encoded

    def _try_id(sid: str) -> list:
        if not sid:
            return []
        t = transcripts_dir / f"{sid}.jsonl"
        try:
            if t.exists() and t.stat().st_size > 0:
                return ["--resume", sid]
        except Exception:
            pass
        return []

    # 1) Per-model lookup — picks up the right thread when user switches models.
    try:
        cur_model = ""
        try:
            cur_model = _get_project_model_value(proj_id)
        except NameError:
            pass
        hist_file = pdir / ".session-history.json"
        if hist_file.exists():
            hist = json.loads(hist_file.read_text())
            sid = (hist.get(cur_model or "_default") or "").strip()
            args = _try_id(sid)
            if args:
                return args
    except Exception:
        pass

    # 2) Legacy single-file fallback.
    last_id_file = pdir / ".last-session-id"
    if last_id_file.exists():
        try:
            sid = last_id_file.read_text().strip()
            args = _try_id(sid)
            if args:
                return args
            # Stale id → clean up so spawns don't keep failing with "No conversation found"
            try: last_id_file.unlink()
            except Exception: pass
        except Exception:
            pass

    return ["--continue"]


_ALLOWED_MODELS = {
    # Claude CLI accepts aliases and full IDs. Restrict to the ones we want users
    # to pick from in the UI; "" / unset → CLI default model.
    "haiku", "sonnet", "opus",
    "claude-haiku-4-5-20251001",
    "claude-sonnet-4-6",
    "claude-opus-4-7",
    # OSK/LiteLLM proxy on 127.0.0.1:4100 exposes OpenAI as the Anthropic API
    # (config: ~/.osk-litellm.yaml). Selecting this routes the spawned Claude
    # session to GPT — env splice happens in _get_project_spawn_env.
    "gpt-5.4-2026-03-05",
    # MiniMax via direct API (not LiteLLM) — uses shared.env MINIMAX_API_KEY
    "MiniMax-M2.5",
    "MiniMax-M2.7",
}

_OSK_MODELS = {"gpt-5.4-2026-03-05"}
_MINIMAX_MODELS = {"MiniMax-M2.5", "MiniMax-M2.7"}
_OSK_PROXY_URL = "http://127.0.0.1:4100"
_OSK_PROXY_KEY = "sk-osk-local"


def _get_project_model_value(proj_id: str) -> str:
    """Read the (validated) model field from project frontmatter. '' if unset."""
    try:
        md = PROJECTS_DIR / proj_id / "north-star.md"
        if not md.exists():
            return ""
        proj = _parse_md_frontmatter(md)
        model = (proj.get("model") or "").strip()
        return model if model in _ALLOWED_MODELS else ""
    except Exception:
        return ""


def _get_project_model(proj_id: str) -> list:
    """Return ['--model', value] if frontmatter has a valid model, else [].
    Spliced into PTY + tmux spawn argv."""
    model = _get_project_model_value(proj_id)
    return ["--model", model] if model else []


def _get_project_spawn_env(proj_id: str) -> dict:
    """Return extra env vars to splice into the Claude spawn for this project.
    For OSK/GPT: routes to LiteLLM proxy. For MiniMax: routes to direct API.
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
    if model in _MINIMAX_MODELS:
        # Route directly to MiniMax API (not LiteLLM)
        return {
            "CLAUDE_CODE_OAUTH_TOKEN": "",
            "CLAUDE_CODE_OAUTH_REFRESH_TOKEN": "",
            "ANTHROPIC_API_KEY": os.environ.get("MINIMAX_API_KEY", ""),
            "ANTHROPIC_BASE_URL": "https://api.minimax.io/v1",
            "ANTHROPIC_MODEL": model,
            "ANTHROPIC_SMALL_FAST_MODEL": model,
        }
    return {}


def _record_spawn_info(proj_id: str, resume_args: list) -> None:
    """Snapshot what resume flag we used when spawning Claude for this project.

    Surfaces to the UI via /api/exec-sessions so users can see whether the
    currently running tmux session is actually continuing prior stone work
    (--resume <id>) or starting fresh (--continue / nothing).
    """
    from datetime import datetime as _dt
    if "--resume" in resume_args:
        try:
            idx = resume_args.index("--resume")
            from_id = resume_args[idx + 1] if idx + 1 < len(resume_args) else ""
        except Exception:
            from_id = ""
        info = {"mode": "resume", "from_id": from_id, "at": _dt.now().isoformat(timespec="seconds")}
    elif "--continue" in resume_args:
        info = {"mode": "continue", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    else:
        info = {"mode": "fresh", "from_id": "", "at": _dt.now().isoformat(timespec="seconds")}
    try:
        pdir = PROJECTS_DIR / proj_id
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / ".last-spawn-info.json").write_text(json.dumps(info))
    except Exception:
        pass


def _spawn_claude(proj_id: str) -> ptyprocess.PtyProcess:
    # M173: PTY terminal spawns FRESH — no --continue/--resume. The PTY is for
    # interactive debugging; users want a clean session each time. Tmux exec
    # sessions still use _get_resume_args for autonomous-loop continuity.
    proj_dir = _get_project_dir(proj_id) or str(Path.home())
    return ptyprocess.PtyProcessUnicode.spawn(
        ["claude", "--dangerously-skip-permissions", *_get_project_model(proj_id)],
        cwd=proj_dir,
        dimensions=(30, 120),
        env={
            **os.environ,
            "TERM": "xterm-256color",
            "COLUMNS": "120",
            "LINES": "30",
            "CLAUDE_CODE_TASK_LIST_ID": f"hub-exec-{proj_id}",
            **_get_project_spawn_env(proj_id),
        },
    )


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
            if ("esc to interrupt" in data) or ("… (" in data) or \
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
    milestones.insert(0, new_ms)  # M86: prepend so newest always appears first in UI
    proj["milestones"] = milestones
    _save_project(proj_id, proj)
    # M267: user-originated event — new stone added via UI.
    _ns_push("stone_created", proj_id=proj_id, mid=new_id,
             text=(new_ms.get("text") or "")[:140])
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
            # M216: `held` = user-paused stone, excluded from EXECUTE/REPLY SYNC queues until released.
            for k in ("text", "layer", "parent_id", "claude_ack", "cron_job_id", "claude_comment", "star_relation", "held"):
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
            # conversation: accumulated chat thread — allow empty list (don't coerce to None)
            if "conversation" in data:
                m["conversation"] = data["conversation"]
            # append_message: single {role,text} dict — appended to conversation (easier for Claude)
            if "append_message" in data:
                msg = data["append_message"]
                if isinstance(msg, dict) and msg.get("role") and msg.get("text"):
                    import datetime as _dt
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
                    # M222 (final stone-status protocol): any claude action on a stone
                    # (reply / apply / change) auto-flips status to pending_confirmation
                    # unless it's already done or needs_clarification (those are user-gated).
                    if msg.get("role") == "claude":
                        _cur_status = m.get("status") or ("done" if m.get("done") else "pending")
                        if _cur_status not in ("done", "needs_clarification", "pending_confirmation"):
                            m["status"] = "pending_confirmation"
                            m["done"] = False
                            m.setdefault("pending_confirm_at", now_iso)
                            m.setdefault("claude_ack", now_iso)
                    # M267: emit SSE event ONLY for user-originated comments — NOT claude.
                    if msg.get("role") == "user":
                        _ns_push("user_commented", proj_id=proj_id, mid=mid,
                                 text=(msg.get("text") or "")[:140])
                    # M184: when user adds a comment, dispatch REPLY SYNC immediately
                    # (don't wait for the 5-min watcher). Also do NOT re-promote to queued
                    # — the comment is a question/instruction, not a request to redo work.
                    # If user wants to re-run the stone, they'd change status explicitly.
                    # M225: skip REPLY SYNC dispatch if stone is held (zero claude token spend on held).
                    if msg.get("role") == "user" and not m.get("done") and not m.get("held"):
                        session_name = f"claude-exec-{proj_id}"
                        check = subprocess.run(["tmux", "has-session", "-t", session_name], capture_output=True)
                        if check.returncode == 0:
                            _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
                            _qf.parent.mkdir(parents=True, exist_ok=True)
                            _entry = json.dumps({
                                "ts": _dt.datetime.now().isoformat(),
                                "body": (
                                    f"[REPLY SYNC] User commented on stone {mid}: \"{msg.get('text','')[:200]}\"\n\n"
                                    f"PROTOCOL (docs/ns-comment-reply-protocol.md):\n"
                                    f"  1. Read the comment above.\n"
                                    f"  2. Answer/act on it in ≤3 lines via PATCH "
                                    f"http://100.119.82.4:9000/api/northstar/{proj_id}/milestones/{mid} "
                                    f"with body append_message {{role:'claude', text:'<≤3 lines>'}}.\n"
                                    f"  3. If user gave an instruction to re-do the stone, you may also act on it directly.\n"
                                    f"  4. Do NOT change status from this — replying does NOT advance the stone.\n"
                                    f"  5. If reply requires more than 3 lines: write docs/ns-replies/<DATE>-{mid}.md and reference the path.\n"
                                    f"Mandatory — do this before any other work."
                                ),
                            }, ensure_ascii=False)
                            with _qf.open("a", encoding="utf-8") as _qh:
                                _qh.write(_entry + "\n")
                            # M184/M148: wake Claude if pane is idle. Without this, Stop hook
                            # can't fire (no response event) and the queue sits unread.
                            try:
                                _pane = subprocess.run(
                                    ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-8"],
                                    capture_output=True, text=True, timeout=2,
                                ).stdout
                                if not (("esc to interrupt" in _pane) or ("… (" in _pane)):
                                    subprocess.run(
                                        ["tmux", "send-keys", "-t", session_name, "go", "Enter"],
                                        capture_output=True, timeout=2,
                                    )
                            except Exception:
                                pass
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


@app.post("/api/northstar/{proj_id}/milestones/{mid}/request-comment")
async def request_milestone_comment(proj_id: str, mid: str):
    """M182: queue a request for Claude to post an initial 1-line comment on
    a new stone. Triggered when user opens msg popup on a stone with no prior
    claude_comment and empty conversation. Server appends a queue entry; Stop
    hook delivers to Claude on next idle; Claude PATCHes append_message{role:'claude'}.
    """
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False, "error": "project not found"}, status_code=404)
    proj = _parse_md_frontmatter(md)
    ms = next((m for m in proj.get("milestones", []) if isinstance(m, dict) and m.get("id") == mid), None)
    if not ms:
        return JSONResponse({"ok": False, "error": "milestone not found"}, status_code=404)
    # Skip if already has a comment or conversation
    if (ms.get("claude_comment") or "").strip():
        return JSONResponse({"ok": True, "skipped": "already_commented"})
    conv = ms.get("conversation") or []
    if conv:
        return JSONResponse({"ok": True, "skipped": "conversation_not_empty"})

    from datetime import datetime as _dt_rc
    _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
    _qf.parent.mkdir(parents=True, exist_ok=True)
    _entry = json.dumps({
        "ts": _dt_rc.now().isoformat(),
        "body": (
            f"[INITIAL COMMENT REQUEST] User just opened msg popup on a new stone with no prior comment.\n"
            f"Stone: {mid} — \"{(ms.get('text') or '')[:100]}\"\n\n"
            f"Action: PATCH http://100.119.82.4:9000/api/northstar/{proj_id}/milestones/{mid}\n"
            f"  body: {{\"append_message\":{{\"role\":\"claude\",\"text\":\"<≤3 line initial take on this stone>\"}}}}\n\n"
            f"Rules (per docs/ns-comment-reply-protocol.md):\n"
            f"  - ≤3 lines, no preamble, no code blocks\n"
            f"  - Acknowledge the stone, state your initial read, optionally flag one risk/question\n"
            f"  - If you need clarification, append a question instead of attempting it"
        ),
    }, ensure_ascii=False)
    with _qf.open("a", encoding="utf-8") as _qh:
        _qh.write(_entry + "\n")
    return JSONResponse({"ok": True, "queued": True})


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
    session_name = f"claude-exec-{proj_id}"
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

        # On Execute: ack any unreviewed milestones first, then promote all acked pending → queued
        from datetime import datetime as _dt_exec
        _now_iso = _dt_exec.now().strftime("%Y-%m-%dT%H:%M")
        _promoted = False
        for _m in milestones:
            if not isinstance(_m, dict) or _m.get("done"): continue
            _st = _m.get("status") or ""
            if _st in ("done", "pending_confirmation", "queued", "needs_clarification"): continue
            # Ack if unreviewed
            if not _m.get("claude_ack"):
                _m["claude_ack"] = _now_iso
            # Promote acked pending → queued
            if _m.get("claude_ack") and _st in ("pending", None, ""):
                _m["status"] = "queued"
                _promoted = True
        if _promoted:
            import copy as _copy
            _proj_to_save = _copy.deepcopy(proj)
            _save_project(proj_id, _proj_to_save)
            # Reload active_ms with updated statuses
            proj = _parse_md_frontmatter(PROJECTS_DIR / proj_id / "north-star.md")
            milestones = proj.get("milestones", [])
            active_ms = [m for m in milestones if not m.get("done") and m.get("status") != "done"]

        # M160: skip stones with a pending Claude comment awaiting user reply
        # M225: held stones are completely excluded from SessionStart actionable surfaces too.
        actionable_all = [m for m in active_ms if m.get("status") in ("queued", "pending", "needs_clarification") and not m.get("held")]
        paused_awaiting_user = [m for m in actionable_all if _awaits_user_reply(m)]
        actionable = [m for m in actionable_all if not _awaits_user_reply(m)][:5]
        session_name = f"claude-exec-{proj_id}"
        proj_dirs = {
            "MOAT": "/home/desk-1/Project/Moat", "CTX": "/home/desk-1/Project/CTX",
            "FromScratch": "/home/desk-1/Project/FromScratch",
            "HugwartsBanana": "/home/desk-1/Project/VIDraft/HugwartsBanana",
            "AIKB": "/home/desk-1/Project/AIKB", "FRWP": "/home/desk-1/Project/FRWP",
        }
        proj_dir = proj_dirs.get(proj_id, str(Path.home() / "Project" / proj_id))

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
                # Session alive with Claude running — inject trigger if milestones need attention
                # M160: skip stones whose last conversation entry is from claude (awaiting user reply)
                # M216: held stones (user-paused) are excluded from EXECUTE SYNC entirely.
                new_pending = [m for m in active_ms if m.get("status") == "pending" and not m.get("claude_ack") and not _awaits_user_reply(m) and not m.get("held")]
                new_queued  = [m for m in active_ms if m.get("status") == "queued" and not _awaits_user_reply(m) and not m.get("held")]
                needs_trigger = new_pending or new_queued
                _trigger_sent = False
                if needs_trigger:
                    # M149: append-only JSONL queue — never overwrite. Each Execute click
                    # appends one entry. Hooks track byte offset to consume only new entries.
                    _qf = PROJECTS_DIR / proj_id / "pending-execute-queue.jsonl"
                    _qf.parent.mkdir(parents=True, exist_ok=True)
                    _ms_snap = "\n".join(
                        f"  {m.get('id')} [{m.get('status')}]: \"{m.get('text','')[:60]}\""
                        for m in (new_queued + new_pending)
                    )
                    _entry = json.dumps({
                        "ts": _dt_exec.now().isoformat(),
                        "body": (
                            f"[EXECUTE SYNC] New milestones need processing — process ALL queued milestones now.\n\n"
                            f"GET {hub_api}/api/northstar/{proj_id}/milestones for current state.\n"
                            f"TaskCreate + implement each queued milestone sequentially.\n\n"
                            f"COMPLETION PROTOCOL — when patching status=pending_confirmation, include\n"
                            f"  star_relation: <1 English line stating HOW this completion closed the star gap>\n"
                            f"  (be concrete: which metric moved, by what mechanism. Mandatory.)\n\n"
                            f"NO-OP PROTOCOL (M187, Rule 6 of ns-comment-reply-protocol.md) — if you\n"
                            f"  decide NOT to act on a stone (already done, no actionable work, blocked\n"
                            f"  on user, ambiguous), POST a 1-line append_message {{role:'claude'}} on\n"
                            f"  that stone stating the reason. Silent skip is forbidden.\n\n"
                            f"Newly queued:\n{_ms_snap}"
                        ),
                    }, ensure_ascii=False)
                    with _qf.open("a", encoding="utf-8") as _qh:
                        _qh.write(_entry + "\n")
                    # M147: Stop-hook handles busy→idle transition.
                    # M148: For truly-idle sessions (no spinner, no modal), also send "go"
                    # to wake them up — Stop hook won't fire without a response event.
                    _trigger_sent = True
                    _wake_sent = False
                    try:
                        _pane = subprocess.run(
                            ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-8"],
                            capture_output=True, text=True, timeout=2,
                        ).stdout
                        # Busy detection: Claude Code's status bar shows
                        # `· esc to interrupt ·` ONLY while a response is in progress.
                        # Also "… (" matches the active spinner timer format.
                        # Both are pattern-based — robust to verb rotation (Flowing/Boogieing/etc).
                        _modal_signatures = ("extra usage", "Switch to Team plan",
                                             "Stop and wait", "rate-limit-options",
                                             "Press Enter to", "Continue?", "[Y/n]")
                        _busy = "esc to interrupt" in _pane or "… (" in _pane
                        _modal = any(s in _pane for s in _modal_signatures)
                        _has_prompt = "❯" in _pane  # Claude's idle prompt marker
                        if _has_prompt and not _busy and not _modal:
                            subprocess.run(
                                ["tmux", "send-keys", "-t", session_name, "go", "Enter"],
                                capture_output=True, timeout=2,
                            )
                            _wake_sent = True
                    except Exception:
                        pass
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
            # Spawn tmux session — use TaskCreate/TaskUpdate (Claude Code built-in) for task tracking
            cron_prompt = (
                f"[EXECUTE SYNC] Project {proj_id} — Execute clicked. PRIMARY GOAL: build a fresh task list from queued milestones.\n\n"
                f"STEP 0 — RESET task list (continued session may have stale tasks):\n"
                f"  1. Call TaskList to see existing tasks.\n"
                f"  2. For each existing task: TaskUpdate(<id>, status='completed') to clear it.\n"
                f"     (Do NOT keep stale tasks from previous work — start fresh for this Execute.)\n\n"
                f"PRIMARY ACTION — TaskCreate per queued milestone:\n"
                f"  For each milestone with status=queued in the list below:\n"
                f'    TaskCreate(subject="<milestone_id>", description="<milestone text>")\n'
                f"  (Use Claude Code built-in TaskCreate tool. NOT TodoWrite.)\n\n"
                f"PRE-STEP — Sync unreviewed (only if claude_ack=null exists):\n"
                f"  PATCH {hub_api}/api/northstar/{proj_id}/milestones/<id>:\n"
                f"    claude_ack=now. Keep status=pending (promotion already done by server). Vague → status=needs_clarification + clarification_question.\n\n"
                f"IMPLEMENT each task sequentially:\n"
                f"  1. TaskUpdate(<id>, status='in_progress')\n"
                f"  2. Edit/write files to implement the milestone.\n"
                f"  3. Append completion-log:\n"
                f'     echo \'{{\"session_id\":\"exec\",\"milestone_id\":\"<MID>\",\"evidence\":\"<one-line summary>\",\"timestamp\":\"\'$(date -Iseconds)\'\"}}\' >> ~/.claude/hub/projects/{proj_id}/completion-log.jsonl\n'
                f"  4. PATCH {hub_api}/api/northstar/{proj_id}/milestones/<MID> body {{\"status\":\"pending_confirmation\", \"star_relation\":\"<1-line gap closure>\"}}\n"
                f"     star_relation = ONE English line stating HOW this completion reduced the star gap (be concrete: which metric moved, by what mechanism). Mandatory.\n"
                f"  5. TaskUpdate(<id>, status='completed')\n\n"
                f"OPTIONAL — After all tasks done, update spec via {hub_api}/api/northstar/{proj_id}/doc.\n\n"
                f"Active milestones (snapshot — TaskCreate only for status=queued):\n{all_ms_lines}"
            )
            # Write prompt to file — avoids tmux paste-mode for multi-line text
            prompt_file = PROJECTS_DIR / proj_id / "pending-execute-prompt.txt"
            prompt_file.parent.mkdir(parents=True, exist_ok=True)
            prompt_file.write_text(cron_prompt, encoding="utf-8")

            # Kill existing session if any, start fresh
            # SessionStart hook injects directive via additionalContext from pending-execute-prompt.txt
            # A minimal trigger ("go") kicks Claude to process the injected context
            subprocess.run(["tmux", "kill-session", "-t", session_name], capture_output=True)
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

            resume_args = _get_resume_args(proj_id, spawn_cwd)
            _record_spawn_info(proj_id, resume_args)
            _tmux_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}"]
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
                _record_spawn_info(proj_id, resume_args)
                _tmux_env = ["-e", f"CLAUDE_CODE_TASK_LIST_ID=hub-exec-{proj_id}"]
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
            return JSONResponse({
                "ok": True, "mode": "tmux",
                "session": session_name,
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
    created_dir = False
    if repo_path:
        # M258: if the node's repo_path doesn't exist on the server, mkdir -p it.
        was_created, resolved = _ensure_repo_path_exists(repo_path)
        created_dir = was_created
        frontmatter["repo_path"] = resolved or repo_path
    text = "---\n" + _yaml.dump(frontmatter, allow_unicode=True, default_flow_style=False) + "---\n\n" + body
    md.write_text(text, encoding="utf-8")
    return JSONResponse({"ok": True, "id": folder_id, "created_repo_dir": created_dir})


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


@app.patch("/api/northstar/{proj_id}")
async def patch_project(proj_id: str, request: Request):
    """Update simple top-level project fields (deadline, status, note, links, metric/current/target/unit, etc.)."""
    data = await request.json()
    md = PROJECTS_DIR / proj_id / "north-star.md"
    if not md.exists():
        return JSONResponse({"ok": False}, status_code=404)
    proj = _parse_md_frontmatter(md)
    # M165: allow manual edit of star fields (metric/current/target/unit) from ns-dash UI
    # M214: `model` controls --model flag passed to PTY/tmux Claude spawns for this project.
    allowed = {"deadline", "status", "note", "links", "stage",
               "metric", "current", "target", "unit", "model"}
    for k in allowed:
        if k in data:
            if k == "model":
                v = (data[k] or "").strip()
                if v and v not in _ALLOWED_MODELS:
                    return JSONResponse({"ok": False, "error": f"unknown model '{v}'",
                                         "allowed": sorted(_ALLOWED_MODELS)}, status_code=400)
                proj[k] = v  # empty string = unset (CLI default)
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
    for k in ("layer", "parent", "position_x", "x", "y", "repo_path", "stage"):
        if k in data:
            if k == "repo_path" and data[k]:
                # M258: mkdir -p when the path doesn't exist yet on the server.
                was_created, resolved = _ensure_repo_path_exists(data[k])
                created_dir = created_dir or was_created
                proj[k] = resolved or data[k]
            else:
                proj[k] = data[k]
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
    exec_session = f"claude-exec-{proj_id}"
    check = subprocess.run(["tmux", "has-session", "-t", exec_session], capture_output=True)
    if check.returncode != 0:
        return JSONResponse({"ok": False, "error": "exec session not running"}, status_code=404)
    subprocess.run(["tmux", "send-keys", "-t", exec_session, prompt, "Enter"], capture_output=True)
    return JSONResponse({"ok": True})


@app.delete("/api/northstar/{proj_id}/exec-session")
async def kill_exec_session(proj_id: str):
    """Kill the Execute-spawned tmux session (claude-exec-{proj_id})."""
    exec_session = f"claude-exec-{proj_id}"
    result = subprocess.run(["tmux", "kill-session", "-t", exec_session], capture_output=True)
    killed = result.returncode == 0
    return {"ok": True, "killed": killed, "session": exec_session}


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
    exec_session = f"claude-exec-{proj_id}"
    r = subprocess.run(["tmux", "kill-session", "-t", exec_session], capture_output=True)
    killed["tmux"] = (r.returncode == 0)
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


@app.get("/api/exec-sessions")
async def get_exec_sessions():
    """Return claude-exec-* tmux sessions where Claude is actually running (not just a shell prompt)."""
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}:#{session_created}:#{session_windows}"],
        capture_output=True, text=True
    )
    sessions = []
    for line in result.stdout.splitlines():
        parts = line.split(":", 2)
        if len(parts) >= 1 and parts[0].startswith("claude-exec-"):
            session_name = parts[0]
            proj_id = session_name[len("claude-exec-"):]
            created_ts = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

            # Check if Claude is actually running — not just a shell prompt after Claude exited
            pane_cmds = subprocess.run(
                ["tmux", "list-panes", "-t", session_name, "-F", "#{pane_current_command}"],
                capture_output=True, text=True
            ).stdout.splitlines()
            cmds = [c.strip() for c in pane_cmds if c.strip()]
            claude_running = any(c not in _SHELLS for c in cmds) if cmds else False
            if not claude_running:
                continue

            # M97/M183: detect busy first (positive signal), then idle is the complement.
            # Claude's status bar always contains "bypass permissions on (shift+tab to cycle)"
            # — that text is present during BOTH busy and idle, so it can't be used as
            # an idle marker. The unique busy markers are "esc to interrupt" and the
            # spinner timer pattern "… (". Same rules as the M148 wake-detection.
            pane_out = subprocess.run(
                ["tmux", "capture-pane", "-p", "-t", session_name, "-S", "-5"],
                capture_output=True, text=True
            ).stdout
            import re as _re
            clean = _re.sub(r'\x1b\[[0-9;]*[mKHJ]', '', pane_out)
            busy = ("esc to interrupt" in clean) or ("… (" in clean)
            idle = not busy

            # M133 REMOVED (M179): the live/idle pill reflects ACTUAL pane state,
            # not "pending work exists". Queued milestones are surfaced separately
            # via D+N stone count and the task queue list; the badge must not lie
            # about whether Claude is currently processing. Conflating these caused
            # the badge to read "live" when Claude was at the ❯ prompt.

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
                except Exception:
                    pass
            sessions.append({
                "session": session_name,
                "proj_id": proj_id,
                "created": _dt.fromtimestamp(created_ts).isoformat() if created_ts else "",
                "alive": True,
                "idle": idle,
                "spawn_mode": spawn_mode,
                "spawn_from": spawn_from,
            })
    return JSONResponse({"ok": True, "sessions": sessions})


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
        if last_busy and now - last_busy < 3:
            result[proj_id] = "active"
        else:
            idle_since = _session_idle_since.get(proj_id) or now
            result[proj_id] = f"idle:{int(now - idle_since)}"

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
                "description": (fm.get("description") or "").strip(),
            })

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


def main():
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
