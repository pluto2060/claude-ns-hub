#!/usr/bin/env python3
"""PreToolUse hook — post session-scoped busy=true reason=tool_start (M1672 follow-up).

Closes the last false-idle gap: a single >120s tool call (SSH build, subagent, MCP
tool) emits no PostToolUse heartbeat, so unclaimed work went stale-idle mid-call and
the poller injected wakes. tool_start gets a long server-side hold (1h, tmux-alive-
validated in _session_is_busy); the tool's own PostToolUse heartbeat or Stop replaces
the record.

M131-e: matcher widened Bash|Agent|Task → Bash|Agent|Task|mcp__.* (2026-07-06).
northstar-action-log.py's PostToolUse TRACK set never covered mcp__* tools, so a
long-running MCP call (observed: mcp__playwright-session-3__*, "Analyzing M1161"
5m15s) had NEITHER a PreToolUse nor PostToolUse heartbeat — busy went stale at 120s,
the poller injected repeated "Tasks ready" text into the live input box while the
tool was still running (stacked duplicate injections, same class of bug as the
original SSH-build gap this hook was built for).
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

    # M1766: NS_SESSION_KEY (set at spawn time) first — see northstar-stop-idle.py docstring.
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
    if not is_exec:
        sys.exit(0)  # only exec sessions gate dispatch — skip the HTTP cost for user CLIs

    payload = json.dumps({"proj_id": proj_id, "busy": True, "reason": "tool_start",
                          "session_key": sk, "is_exec": True}).encode()
    req = urllib.request.Request(f"{HUB_URL}/api/agent-busy", data=payload,
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=1)
except Exception:
    pass  # never block Claude
