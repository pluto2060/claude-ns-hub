#!/usr/bin/env python3
"""SubagentStart/SubagentStop hook — track in-flight background subagents per session.

Gap discovered 2026-07-05 (FromScratch): a main session dispatched a Task/Agent
subagent ("Fact Finder", research-doc-specialist) that ran 4+ minutes in the
background. The parent's own turn ended each time it checked in (Stop fired,
northstar-stop-idle.py posted busy=false/agent_stopped) even though the subagent
was still actively computing — SubagentStart/SubagentStop were registered as empty
hooks, so nothing tracked "a subagent is still running for this session". Result:
the poller repeatedly woke the session ("Tasks ready..."), it replied "still
waiting for Fact Finder", and went idle again — a wasted wake every ~10s poll tick
while real background work continued underneath, invisible to busy/idle.

This hook writes a per-session in-flight subagent COUNT to a local marker file
(~/.claude/.subagent-count-{session_key}) — SubagentStart increments, SubagentStop
decrements. northstar-stop-idle.py checks this file: if count > 0, it holds
busy=true/reason=subagent_running instead of posting idle, so the poller does not
inject wakes while a subagent is still working on this session's behalf.
"""
import json, os, sys

MARKER_DIR = os.path.join(os.path.expanduser("~"), ".claude")

try:
    raw = sys.stdin.read()
    data = json.loads(raw) if raw else {}
    hook_event = data.get("hook_event_name", "")

    # M1766: NS_SESSION_KEY (set at spawn time) first — see northstar-stop-idle.py docstring.
    sk = os.environ.get("NS_SESSION_KEY", "").strip()
    if not sk and os.environ.get("TMUX"):
        try:
            import subprocess
            sk = subprocess.run(["tmux", "display-message", "-p", "#S"],
                                capture_output=True, text=True, timeout=1).stdout.strip()
        except Exception:
            sk = ""
    if not sk:
        sk = os.environ.get("CLAUDE_CODE_SESSION_ID", "")
    if not sk or "-exec-" not in sk:
        sys.exit(0)  # only exec sessions gate dispatch

    marker = os.path.join(MARKER_DIR, f".subagent-count-{sk}")
    try:
        count = int(open(marker).read().strip()) if os.path.exists(marker) else 0
    except Exception:
        count = 0

    if hook_event == "SubagentStart":
        count += 1
    elif hook_event == "SubagentStop":
        count = max(0, count - 1)
    else:
        sys.exit(0)

    with open(marker, "w") as f:
        f.write(str(count))
except Exception:
    pass  # never block Claude
