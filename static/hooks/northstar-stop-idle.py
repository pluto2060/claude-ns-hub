#!/usr/bin/env python3
"""Stop hook — post session-scoped busy=false to hub on turn end (M1675).

Fixes the 120s false-busy window: idle was only reported via get_pending_task-empty
or report_task_complete, so an exec agent whose final turn ended without an MCP call
(e.g. post-completion self-verification) stayed "busy" in the UI until the
_OOB_STALE_SECS=120 TTL expired. Stop is the authoritative "turn ended" signal.

M1683 fix: Stop ALWAYS posts idle. A Stop event is the authoritative "turn ended"
signal — by the time it fires, any compaction this turn has ALREADY completed (the
pane is back at the prompt). The earlier design re-armed reason=compacting whenever the
PreCompact marker was <120s old, but a compaction that finishes at a prompt with no
follow-up turn then produces NO further event to clear it, stranding the session in the
server's 600s compacting hold and blocking queue dispatch (observed: MOAT main stuck
~10min with a queued stone). The "compaction in progress → hold busy" case is owned
solely by the PreCompact hook (northstar-precompact-busy.py), which fires at compaction
START; Stop firing means we are past that window.

M131-d fix: Stop does NOT always mean idle when a background subagent is still
running (Task/Agent tool dispatched a subagent that outlives the parent's own
turn). Checks the per-session marker written by northstar-subagent-busy.py
(SubagentStart/SubagentStop); if a subagent is still in flight, holds
busy=true/reason=subagent_running instead — otherwise the poller repeatedly woke
a session that reported idle while a subagent was still chewing through tokens on
its behalf (observed: FromScratch, "Fact Finder" research-doc-specialist, 4+ min).
"""
import json, os, sys, urllib.request

HUB_URL = os.environ.get("NS_HUB_URL", "http://100.119.82.4:9001")

try:
    proj_id = os.environ.get("NS_PROJ_ID", "")
    if not proj_id:
        cwd = os.environ.get("CLAUDE_PROJECT_DIR", os.getcwd())
        proj_id = os.path.basename(cwd.rstrip("/"))
    if not proj_id:
        sys.exit(0)

    # Session identity — same derivation as northstar-action-log.py / hub-mcp-server.py
    sk, is_exec = "", False
    if os.environ.get("TMUX"):
        try:
            import subprocess
            sk = subprocess.run(["tmux", "display-message", "-p", "#S"],
                                capture_output=True, text=True, timeout=1).stdout.strip()
            is_exec = "-exec-" in sk
        except Exception:
            sk = ""
    if not sk:
        sk = os.environ.get("CLAUDE_CODE_SESSION_ID", "")

    # M1683: always idle on Stop — see module docstring. Compaction-in-progress is the
    # PreCompact hook's responsibility, not Stop's.
    busy, reason = False, "agent_stopped"

    # M131-d: a background subagent may still be running for this session (see docstring).
    marker = os.path.join(os.path.expanduser("~"), ".claude", f".subagent-count-{sk}")
    try:
        if os.path.exists(marker) and int(open(marker).read().strip() or "0") > 0:
            busy, reason = True, "subagent_running"
    except Exception:
        pass

    payload = json.dumps({"proj_id": proj_id, "busy": busy, "reason": reason,
                          "session_key": sk, "is_exec": is_exec}).encode()
    req = urllib.request.Request(f"{HUB_URL}/api/agent-busy", data=payload,
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=2)
except Exception:
    pass  # never block Claude
