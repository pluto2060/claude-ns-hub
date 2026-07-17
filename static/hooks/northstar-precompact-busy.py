#!/usr/bin/env python3
"""PreCompact hook — post session-scoped busy=true reason=compacting to hub (M1672).

Compaction emits no PostToolUse heartbeats, so the session's busy record went stale
(120s) mid-compaction and the queue poller injected duplicate 'Tasks ready' wakes
(observed: 4 duplicates during one long turn + compaction). This hook marks the
session busy at compaction start; the server holds reason=compacting for up to 10min
(_session_is_busy). First post-compaction heartbeat or Stop replaces the record.
"""
import json, os, sys, urllib.request

HUB_URL = os.environ.get("NS_HUB_URL", "http://127.0.0.1:9001")

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

    payload = json.dumps({"proj_id": proj_id, "busy": True, "reason": "compacting",
                          "session_key": sk, "is_exec": is_exec}).encode()
    req = urllib.request.Request(f"{HUB_URL}/api/agent-busy", data=payload,
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=2)
except Exception:
    pass  # never block Claude
