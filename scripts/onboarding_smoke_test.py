#!/usr/bin/env python3
"""P2: NS Hub onboarding smoke test — validates the full install→use flow.

Run after `hub install-global` to verify all layers work end-to-end.
Exit code = number of failed checks (0 = pass, CI-safe).

Usage:
    python3 onboarding_smoke_test.py
    python3 onboarding_smoke_test.py --verbose
    python3 onboarding_smoke_test.py --hub-url http://127.0.0.1:9001
"""
import sys, json, pathlib, subprocess, argparse, urllib.request, urllib.error, time

HUB_URL = "http://127.0.0.1:9001"
SETTINGS = pathlib.Path.home() / ".claude" / "settings.json"
CONFIG_ENV = pathlib.Path.home() / ".config" / "hub" / "env"
CORE_HOOKS = ["northstar-pretool-busy.py", "northstar-stop-idle.py", "northstar-action-log.py",
              "northstar-precompact-busy.py", "northstar-subagent-busy.py"]

PASS = "\033[32m✅\033[0m"
FAIL = "\033[31m❌\033[0m"
WARN = "\033[33m⚠️ \033[0m"


def _get(url, timeout=3):
    try:
        r = urllib.request.urlopen(url, timeout=timeout)
        return r.status, json.loads(r.read())
    except Exception as e:
        return None, str(e)


def _post(url, body, timeout=5):
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)


def _patch(url, body, timeout=5):
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="PATCH")
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)


def run(verbose=False, hub_url=HUB_URL):
    results = []

    def chk(name, ok, detail="", fix=""):
        mark = PASS if ok else FAIL
        msg = f"  {mark}  {name}"
        if verbose and detail:
            msg += f"\n       detail: {detail}"
        if not ok and fix:
            msg += f"\n       fix: {fix}"
        results.append((name, ok, detail))
        print(msg)
        return ok

    print("\n── NS Hub Onboarding Smoke Test ──\n")

    # Layer 0: Prerequisites
    print("Layer 0 — Prerequisites")
    import shutil
    chk("Python ≥ 3.10", sys.version_info >= (3, 10), f"found {sys.version.split()[0]}")
    chk("tmux installed", bool(shutil.which("tmux")), fix="sudo apt install tmux")
    chk("claude CLI installed", bool(shutil.which("claude")), fix="npm install -g @anthropic-ai/claude-code")

    # Layer 1: Install artifacts
    print("\nLayer 1 — Install artifacts")
    chk("~/.config/hub/env exists", CONFIG_ENV.exists(), fix="hub install-global")
    try:
        sd = json.loads(SETTINGS.read_text()) if SETTINGS.exists() else {}
    except Exception:
        sd = {}
    chk("MCP ns-hub in settings.json", "ns-hub" in sd.get("mcpServers", {}), fix="hub install-global")
    all_hook_cmds = " ".join(
        h.get("command", "")
        for ev in sd.get("hooks", {}).values()
        for entry in ev
        for h in entry.get("hooks", [entry])
    )
    for hk in CORE_HOOKS:
        chk(f"Hook registered: {hk}", hk in all_hook_cmds, fix="hub install-global")

    # Layer 2: Hub server health (/api/hub/defaults is the lightest readable endpoint)
    print("\nLayer 2 — Hub server")
    status, body = _get(f"{hub_url}/api/hub/defaults")
    hub_live = status == 200
    chk("Hub server responding", hub_live, str(body)[:80], fix="hub  # or: systemctl --user start hub")
    if not hub_live:
        print(f"\n  {WARN} Hub not running — skipping server-side checks (Layers 3-4)\n")
        _summarize(results)
        return sum(1 for _, ok, _ in results if not ok)

    # Layer 3: Stone CRUD cycle
    print("\nLayer 3 — Stone lifecycle")
    proj = "_SMOKETEST"
    mid = None

    # Ensure project exists (create if not)
    _post(f"{hub_url}/api/northstar/create", {"name": proj})  # ok if already exists

    # Create stone (response: {"ok": true, "milestone": {"id": "M1", ...}})
    s, b = _post(f"{hub_url}/api/northstar/{proj}/milestones", {"text": "onboarding smoke test stone"})
    if isinstance(b, dict):
        mid = b.get("id") or (b.get("milestone") or {}).get("id")
    created = s in (200, 201) and bool(mid)
    chk("Create stone (POST /api/northstar)", created, str(b)[:80] if not created else f"id={mid}")

    # Read stone back (response: {"ok": True, "milestones": [...]})
    if mid:
        s2, b2 = _get(f"{hub_url}/api/northstar/{proj}/milestones")
        milestones = b2.get("milestones", []) if isinstance(b2, dict) else (b2 if isinstance(b2, list) else [])
        stone_found = s2 == 200 and any(m.get("id") == mid for m in milestones)
        chk("Read stone back (GET /api/northstar)", stone_found, f"listed {len(milestones)} stones")
    else:
        chk("Read stone back", False, "skipped — create failed")

    # PATCH exec_start → exec_end → pending_confirmation (simulate completion)
    if mid:
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        s3, b3 = _patch(f"{hub_url}/api/northstar/{proj}/milestones/{mid}", {
            "exec_start": now, "exec_end": now,
            "status": "pending_confirmation",
            "model_used": "smoke-test",
            "append_message": {"role": "claude", "text": "smoke test complete"},
        })
        patched = s3 in (200, 201)
        chk("PATCH stone exec fields", patched, str(b3)[:80] if not patched else "ok")

    # Layer 4: action-log endpoint
    print("\nLayer 4 — API endpoints")
    s4, b4 = _post(f"{hub_url}/api/action-log", {
        "proj_id": proj, "stone_id": mid or "0", "action": "smoke:test",
        "detail": "onboarding_smoke_test.py", "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    chk("action-log endpoint", s4 in (200, 204), str(b4)[:60])

    s5, b5 = _post(f"{hub_url}/api/tool-trace", {
        "proj_id": proj, "stone_id": mid or "0", "tool_name": "Bash",
        "input_summary": "smoke", "output_summary": "ok",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    chk("tool-trace endpoint", s5 in (200, 204), str(b5)[:60])

    # Cleanup: mark smoke stone done
    if mid:
        _patch(f"{hub_url}/api/northstar/{proj}/milestones/{mid}", {"status": "done", "done": True})

    _summarize(results)
    return sum(1 for _, ok, _ in results if not ok)


def _summarize(results):
    failed = sum(1 for _, ok, _ in results if not ok)
    total = len(results)
    print(f"\n── {total - failed}/{total} checks passed", end="")
    if failed == 0:
        print(f" {PASS} All good — hub is ready for new users.\n")
    else:
        print(f" — {failed} failed. Fix the issues above and re-run.\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="NS Hub onboarding smoke test")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--hub-url", default=HUB_URL)
    a = p.parse_args()
    sys.exit(run(verbose=a.verbose, hub_url=a.hub_url))
