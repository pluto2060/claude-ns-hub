#!/usr/bin/env python3
"""PostToolUse hook — forward Claude tool use events to hub action-log + tool_trace.
Captures Edit/Write/Bash/Read/Glob/Grep so debugging sessions have both
user UI actions AND Claude tool actions in one timeline.
M775: also writes to /api/tool-trace for causality dataset step-level trace.
"""
import json, sys, os, urllib.request, urllib.error, datetime

HUB_URL = os.environ.get("NS_HUB_URL", "http://127.0.0.1:9001")

TRACK = {"Edit", "Write", "Bash", "Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent", "Task"}

try:
    raw = sys.stdin.read()
    d = json.loads(raw)
    tool_name = d.get("tool_name", "")
    if tool_name not in TRACK:
        sys.exit(0)

    ti = d.get("tool_input", {})
    # Build concise detail per tool type
    if tool_name in ("Edit", "Write", "Read"):
        detail = (ti.get("file_path") or "")[-80:]
    elif tool_name == "Bash":
        detail = (ti.get("command") or "")[:80]
    elif tool_name in ("Glob", "Grep"):
        detail = (ti.get("pattern") or ti.get("query") or "")[:80]
    elif tool_name in ("WebFetch", "WebSearch"):
        detail = (ti.get("url") or ti.get("query") or "")[:80]
    else:
        detail = ""

    session_id = os.environ.get("CLAUDE_SESSION_ID", "")
    proj_id = os.environ.get("NS_PROJ_ID", "")
    stone_id = os.environ.get("NS_STONE_ID", "")
    # M1751: fallback — read per-session marker written by hub-mcp-server.py at task claim time.
    # NS_STONE_ID env cannot be set post-spawn; marker file bridges the gap for tool_trace linkage.
    if not stone_id:
        try:
            import pathlib
            _sk_env = os.environ.get("NS_SESSION_KEY", "")
            if not _sk_env and os.environ.get("TMUX"):
                import subprocess as _sp
                _sk_env = _sp.run(["tmux", "display-message", "-p", "#S"],
                                  capture_output=True, text=True, timeout=1).stdout.strip()
            if _sk_env:
                _m = pathlib.Path.home() / ".claude" / f".stone-id-{_sk_env}"
                if _m.exists():
                    stone_id = _m.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    # Fallback: derive proj_id from CLAUDE_PROJECT_DIR if NS_PROJ_ID not set
    # e.g. /home/desk-1/Project/MOAT → "MOAT"
    if not proj_id:
        cwd = os.environ.get("CLAUDE_PROJECT_DIR", os.getcwd())
        proj_id = os.path.basename(cwd.rstrip("/"))
    ts = datetime.datetime.utcnow().isoformat() + "Z"

    # ① action-log (existing — tool call timeline for debugging)
    action_payload = json.dumps({
        "ts": ts,
        "proj_id": proj_id,
        "stone_id": stone_id,
        "action": f"claude:{tool_name.lower()}",
        "detail": detail,
        "session_id": session_id,
    }).encode()
    req = urllib.request.Request(
        f"{HUB_URL}/api/action-log",
        data=action_payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=1)

    # ② tool_trace (M775 — step-level causality dataset)
    # output_summary: truncated tool result if available
    tool_resp = d.get("tool_response") or d.get("result") or {}
    if isinstance(tool_resp, dict):
        out_text = str(tool_resp.get("content") or tool_resp.get("output") or "")[:200]
    else:
        out_text = str(tool_resp)[:200]

    duration_ms = None
    if d.get("duration_ms") is not None:
        duration_ms = int(d["duration_ms"])

    trace_payload = json.dumps({
        "ts": ts,
        "proj_id": proj_id,
        "stone_id": stone_id,
        "session_id": session_id,
        "tool_name": tool_name,
        "input_summary": detail,
        "output_summary": out_text,
        "duration_ms": duration_ms,
    }).encode()
    req2 = urllib.request.Request(
        f"{HUB_URL}/api/tool-trace",
        data=trace_payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req2, timeout=1)

    # ③ busy heartbeat (M1577 — reset OOB TTL on every tool call)
    # Prevents false-idle when Claude runs long tasks (subagents, scripts) with >120s gaps.
    # M1656: attach session_key + is_exec — hub ignores non-exec (user CLI) sessions for
    # dispatch gating, so a user conversation in a project cwd no longer blocks stone dispatch.
    if proj_id:
        _sk, _is_exec = "", False
        if os.environ.get("TMUX"):
            try:
                import subprocess
                _sk = subprocess.run(["tmux", "display-message", "-p", "#S"],
                                     capture_output=True, text=True, timeout=1).stdout.strip()
                _is_exec = "-exec-" in _sk
            except Exception:
                _sk = ""
        if not _sk:
            _sk = os.environ.get("CLAUDE_CODE_SESSION_ID", "")
        busy_payload = json.dumps({"proj_id": proj_id, "busy": True, "reason": "tool_call_heartbeat",
                                   "session_key": _sk, "is_exec": _is_exec}).encode()
        req3 = urllib.request.Request(
            f"{HUB_URL}/api/agent-busy",
            data=busy_payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req3, timeout=1)
except Exception:
    pass  # never block Claude
