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
    # M1766: NS_SESSION_KEY (set at spawn time) first — zero subprocess cost, zero timeout
    # risk. tmux display-message only as fallback for sessions spawned without it (rare).
    # A transient tmux timeout here used to silently misroute the busy-state write to a
    # legacy::proj fallback key, leaving the real session's stale busy record uncleared
    # (observed: FromScratch cd3364e0 stuck tool_start ~7min after Stop fired cleanly).
    sk, is_exec = os.environ.get("NS_SESSION_KEY", "").strip(), False
    if sk:
        is_exec = "-exec-" in sk
    elif os.environ.get("TMUX"):
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
    # M1731: SubagentStop does not always fire for a SubagentStart (crash/timeout/kill
    # mid-flight), leaking the counter upward forever with no matching decrement — observed
    # 2026-07-08: every exec session on the box stuck at count=1, some for 20+ hours, silently
    # blocking dispatch (UniversEye M84 sat unconsumed 45+ min while its pane was actually idle
    # at the prompt). A real subagent finishes in seconds-to-minutes, never hours — so treat the
    # marker as stale/leaked past this TTL and fall back to the Stop-is-authoritative idle signal
    # rather than trusting a counter that can only leak upward, never self-heal.
    _SUBAGENT_MARKER_TTL_SECS = 900  # 15min — generous vs. observed subagent durations (~4min max)
    marker = os.path.join(os.path.expanduser("~"), ".claude", f".subagent-count-{sk}")
    try:
        if os.path.exists(marker):
            _age = __import__("time").time() - os.path.getmtime(marker)
            if _age > _SUBAGENT_MARKER_TTL_SECS:
                with open(marker, "w") as _f:
                    _f.write("0")
            elif int(open(marker).read().strip() or "0") > 0:
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
