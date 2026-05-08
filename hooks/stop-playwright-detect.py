#!/usr/bin/env python3
"""
stop-playwright-detect.py v6.0.0 (2026-05-08)
Generic UI verification at session end.

v6.0.0: .SH → .PY conversion + generalized UI detection
  - Removed playwright.config.* gate — works for ANY project with a running web UI
  - Generic UI detection: port scanning instead of config file lookup
  - Generic playwright invariants: HTTP 200, no JS errors, main content visible
  - Preserves all session tracking / flag / dedup logic from v5.1.1

Generic invariants (apply to any web UI, no framework assumptions):
  1. HTTP 200 on root URL
  2. Page loads without JS console errors
  3. Main content visible (not blank page)
  4. Page title doesn't indicate error
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

# ── Parse stop hook input ─────────────────────────────────────────────────────

def parse_input() -> dict:
    try:
        return json.loads(sys.stdin.read())
    except Exception:
        return {}

data = parse_input()
CWD = data.get("cwd", os.getcwd())
SESSION_ID = data.get("session_id", "unknown")
STOP_HOOK_ACTIVE = data.get("stop_hook_active", False)

if STOP_HOOK_ACTIVE:
    sys.exit(0)
if Path(f"{CWD}/.playwright-skip").exists():
    sys.exit(0)

# ── Helpers ───────────────────────────────────────────────────────────────────

def block(reason: str):
    print(json.dumps({"decision": "block", "reason": reason}))
    sys.exit(0)

def run(cmd: list, cwd: str = None, timeout: int = 5) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return r.stdout.strip()
    except Exception:
        return ""

def session_hash(sid: str) -> str:
    return hashlib.md5(sid.encode()).hexdigest()[:8]

SESSION_HASH = session_hash(SESSION_ID)

# ── UI Detection: scan for running web servers ────────────────────────────────

UI_PORTS = [3000, 3001, 3002, 3003, 4200, 5000, 5173, 5174,
            8000, 8080, 8081, 8989, 9000]  # 8787 excluded (ctx-dashboard, not a project UI)

def detect_ui_url(cwd: str) -> str | None:
    """Scan listening ports for a running web server. Return URL or None."""
    # Check .playwright-hook.json first — project-level override wins
    hook_cfg = Path(cwd) / ".playwright-hook.json"
    if hook_cfg.exists():
        try:
            cfg = json.loads(hook_cfg.read_text())
            base = cfg.get("baseUrl")
            if base:
                return base
        except Exception:
            pass

    # Only proceed if project looks like it has UI files
    ui_signals = [
        "package.json", "index.html", "app.py", "server.py", "main.py",
        "src", "public", "static", "templates",
    ]
    if not any((Path(cwd) / f).exists() for f in ui_signals):
        return None

    ss_out = run(["ss", "-tlnp"])
    # Parse port → actual bind address from ss output
    # Format: "LISTEN 0 128 0.0.0.0:3000  0.0.0.0:*" or "127.0.0.1:8787"
    port_bind = {}
    for line in ss_out.splitlines():
        m = re.search(r"([\d.]+|\*|\[::\]):(\d+)\s", line)
        if m:
            addr, port_str = m.group(1), m.group(2)
            port_bind[int(port_str)] = addr

    for port in UI_PORTS:
        if port in port_bind:
            bind_addr = port_bind[port]
            # Normalize: * and 0.0.0.0 are all-interface binds
            reach_addr = "127.0.0.1" if bind_addr not in ("0.0.0.0", "*", "[::]") else "127.0.0.1"
            try:
                urllib.request.urlopen(f"http://{reach_addr}:{port}/", timeout=2)
                # Report the ACTUAL bind address so it's accurate in hook output
                display_addr = bind_addr if bind_addr not in ("*", "[::]") else "0.0.0.0"
                return f"http://{display_addr}:{port}"
            except Exception:
                continue
    return None

UI_URL = detect_ui_url(CWD)
# Also check playwright.config.* (original behavior — keep backwards compat)
PW_CONFIG = next(Path(CWD).rglob("playwright.config.*"), None) if Path(CWD).exists() else None

if not UI_URL and not PW_CONFIG:
    sys.exit(0)  # No UI detected — skip silently

# ── Skip patterns ─────────────────────────────────────────────────────────────

SKIP_PAT = re.compile(
    r'\.md$|\.txt$|\.rst$|\.pdf$|\.csv$|\.log$'
    r'|\.py$|\.pyc$|__pycache__'
    r'|\.ipynb$|\.pkl$|\.pt$|\.safetensors$|\.bin$|\.npy$'
    r'|\.lock$|\.gitignore$|Dockerfile'
    r'|\.sh$|\.bash$|\.zsh$'
    r'|\.yml$|\.yaml$'
    r'|\.db$|\.sqlite'
    r'|\.spec\.(ts|tsx|js|jsx)$|\.test\.(ts|tsx|js|jsx)$'
    r'|tsconfig\.tsbuildinfo$|next-env\.d\.ts$'
    r'|commit_tree\.txt$|MEMORY\.md$'
    r'|\.png$|\.jpg$|\.jpeg$|\.gif$|\.svg$|\.ico$|\.webp$'
    r'|\.woff2?$|\.ttf$|\.eot$'
    r'|^dist/|^\.next/|^build/|^out/|^\.git/|^node_modules/'
    r'|^playwright-report/|^\.claude/|^coverage/|^\.vercel/'
)

def should_skip(f: str) -> bool:
    return bool(SKIP_PAT.search(f))

# ── Changed files (session tracking) ─────────────────────────────────────────

SESSION_SNAPSHOT = f"/tmp/pw-session-snapshot-{SESSION_HASH}.txt"
SESSION_START_HEAD = f"/tmp/pw-session-head-{SESSION_HASH}.txt"

git = lambda *args: run(["git", "-C", CWD, "-c", "core.quotepath=false", *args])

current_unstaged = git("diff", "HEAD", "--name-only")
porcelain = git("status", "--porcelain")
untracked = "\n".join(l[3:] for l in porcelain.splitlines() if l.startswith("??"))
deleted = "\n".join(l[3:] for l in porcelain.splitlines() if re.match(r"[ D]D ", l))
current_all = sorted(set(filter(None, (current_unstaged + "\n" + untracked + "\n" + deleted).splitlines())))

if not Path(SESSION_START_HEAD).exists():
    head = git("rev-parse", "HEAD")
    Path(SESSION_START_HEAD).write_text(head)
    Path(SESSION_SNAPSHOT).write_text("\n".join(current_all))
    if not current_all:
        # Non-git project with .playwright-hook.json: skip git tracking, go straight to verify
        if (Path(CWD) / ".playwright-hook.json").exists() and not head:
            pass  # fall through to UI verification below
        else:
            sys.exit(0)
    else:
        block("New Claude session started — ignoring prior uncommitted changes.\n\nIf you have new work from this session, please mention it.")

session_base = set(Path(SESSION_SNAPSHOT).read_text().splitlines()) if Path(SESSION_SNAPSHOT).exists() else set()
session_init_head = Path(SESSION_START_HEAD).read_text().strip() if Path(SESSION_START_HEAD).exists() else ""
current_head = git("rev-parse", "HEAD")

committed_since = ""
if session_init_head and session_init_head != current_head:
    committed_since = git("diff", "--name-only", f"{session_init_head}..HEAD")

new_files = [f for f in current_all if f not in session_base]
changed_raw = list(set(new_files + [f for f in committed_since.splitlines() if f]))
changed_raw = sorted(set(filter(None, changed_raw)))

_no_git_hook = (Path(CWD) / ".playwright-hook.json").exists() and not git("rev-parse", "HEAD")
if not changed_raw and not _no_git_hook:
    block("No new work in this Claude session.\n\nIf you have newly changed files, please mention them.")

TARGETS = [f for f in changed_raw if not should_skip(f)]

# ── Dedup guard (flag files) ──────────────────────────────────────────────────

def file_hash(files: list[str]) -> str:
    return hashlib.md5("\n".join(sorted(files)).encode()).hexdigest()[:8]

diff_hash = file_hash(TARGETS) if TARGETS else file_hash(changed_raw)
_raw_head = git("rev-parse", "--short", "HEAD")
# For non-git projects, use session+timestamp hash so each session gets a fresh flag
head_hash = _raw_head or hashlib.md5(f"{SESSION_ID}:{__import__('time').time()//3600}".encode()).hexdigest()[:8]
FLAG = f"/tmp/pw-checked-{SESSION_HASH}-{head_hash}-{diff_hash}.flag"
BLOCK_FLAG = f"/tmp/pw-block-{SESSION_HASH}.flag"

def uncommitted_ui_files() -> list[str]:
    return [f for f in (git("diff", "HEAD", "--name-only") + "\n" + git("diff", "--cached", "--name-only")).splitlines()
            if f and not should_skip(f)]

# Cross-session: same HEAD+diff already verified
cross = next(Path("/tmp").glob(f"pw-checked-*-{head_hash}-{diff_hash}.flag"), None)
if cross:
    Path(BLOCK_FLAG).unlink(missing_ok=True)
    unc = uncommitted_ui_files()
    msg = "Cross-session verification passed."
    if unc:
        msg += f"\n\nUncommitted changes:\n" + "\n".join(unc[:5])
        msg += "\n\nCommit: git add <files> && git commit -m '...'"
    block(msg)

# Same-session: already verified
if Path(FLAG).exists():
    Path(BLOCK_FLAG).unlink(missing_ok=True)
    unc = uncommitted_ui_files()
    msg = "Playwright verification complete."
    if unc:
        msg += f"\n\nUncommitted changes:\n" + "\n".join(unc[:5])
        msg += "\n\nCommit: git add <files> && git commit -m '...'"
    block(msg)

# Re-entry: verification still pending
if Path(BLOCK_FLAG).exists():
    url = UI_URL or "http://localhost:3000"
    msg = f"[PLAYWRIGHT PENDING] Verify {url} before continuing.\nFlag: touch {FLAG}"
    ctx_file = f"/tmp/pw-context-{SESSION_HASH}.json"
    if Path(ctx_file).exists():
        try:
            ctx = json.loads(Path(ctx_file).read_text())
            msg = f"[PLAYWRIGHT PENDING] {ctx.get('magnitude','?')}: {len(ctx.get('targets',[]))} files\nServer: {ctx.get('url', url)}\nFlag: touch {FLAG}"
        except Exception:
            pass
    block(msg)

if not TARGETS and not _no_git_hook:
    sys.exit(0)

# ── Generic Playwright invariant check ───────────────────────────────────────

def run_ui_check(url: str) -> list[dict]:
    """Generic UI invariants — no framework assumptions."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return []

    results = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            errors: list[str] = []
            page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
            page.on("pageerror", lambda e: errors.append(str(e)))

            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=15000)
                status = resp.status if resp else 0
                final_url = page.url  # may differ from url if redirected
                redirected = final_url.rstrip("/") != url.rstrip("/")
                results.append({"check": "http_ok", "ok": 200 <= status < 400, "detail": str(status)})

                title = page.title()
                bad_title = not title or any(w in title.lower() for w in ["error", "not found", "404", "500"])
                results.append({"check": "page_title", "ok": not bad_title, "detail": title[:60]})

                # Wait up to 3s for async-rendered content (SPA data loads after DOMContentLoaded)
                try:
                    page.wait_for_function(
                        """() => {
                            const sel = 'main, #app, #root, #__next, [role="main"], body > div, .content, .wrap, .layout, .ns-node, .ns-swimlane';
                            const el = document.querySelector(sel);
                            return el && (el.innerText || el.textContent || '').trim().length > 5;
                        }""",
                        timeout=3000
                    )
                except Exception:
                    pass  # timed out waiting — proceed anyway, check will show false

                # If server redirected (e.g. / → /ko), it's clearly alive — skip blank-page check
                if redirected:
                    has_content = True
                else:
                    has_content = page.evaluate("""() => {
                        const sel = 'main, #app, #root, #__next, [role="main"], body > div, .content, .wrap, .layout, .ns-node, .ns-swimlane';
                        const el = document.querySelector(sel);
                        return el ? (el.innerText || el.textContent || '').trim().length > 5 : (document.body.innerText || '').trim().length > 10;
                    }""")
                results.append({"check": "has_content", "ok": has_content, "detail": "" if has_content else "blank page"})

            except PWTimeout:
                results.append({"check": "page_load", "ok": False, "detail": "timeout"})

            results.append({"check": "no_js_errors", "ok": len(errors) == 0,
                           "detail": " | ".join(errors[:2]) if errors else ""})
            browser.close()
    except Exception as e:
        results.append({"check": "playwright", "ok": False, "detail": str(e)[:100]})
    return results

# ── Build output ──────────────────────────────────────────────────────────────

display_url = UI_URL or "http://localhost:3000"
# Playwright/Chromium needs a routable address — 0.0.0.0 is a bind wildcard, not a destination
check_url = re.sub(r'http://0\.0\.0\.0:', 'http://127.0.0.1:', display_url)
url_to_check = check_url  # kept for compat with check_results usage below
n_targets = len(TARGETS)
changed_summary = "\n".join(TARGETS[:8]) + ("\n..." if n_targets > 8 else "")

# Run generic checks (fast, headless)
check_results = run_ui_check(url_to_check) if UI_URL else []
checks_ok = all(r["ok"] for r in check_results)
failed = [r for r in check_results if not r["ok"]]

check_summary = ""
if check_results:
    pass_n = sum(1 for r in check_results if r["ok"])
    check_summary = f"\nInvariant checks: {pass_n}/{len(check_results)} passed"
    if failed:
        check_summary += "\nFailed: " + ", ".join(f"{r['check']} ({r['detail']})" for r in failed)

# Save context for re-entry
ctx = {
    "url": display_url,
    "targets": TARGETS,
    "magnitude": f"{n_targets} file{'s' if n_targets != 1 else ''}",
    "checks": check_results,
    "fw": "generic",
    "dev_cmd": "npm run dev" if (Path(CWD) / "package.json").exists() else "python3 server.py",
    "can_start_local": True,
    "has_uncommitted": bool(uncommitted_ui_files()),
}
Path(f"/tmp/pw-context-{SESSION_HASH}.json").write_text(json.dumps(ctx))
Path(BLOCK_FLAG).touch()

# Build the block message
if checks_ok and check_results:
    # Auto-passed — just show commit instructions
    Path(FLAG).touch()
    Path(BLOCK_FLAG).unlink(missing_ok=True)
    unc = uncommitted_ui_files()
    msg = f"[UI VERIFIED] {n_targets} file{'s' if n_targets != 1 else ''} — all {len(check_results)} checks passed.{check_summary}"
    if unc:
        msg += f"\n\nUncommitted:\n" + "\n".join(unc[:5]) + "\n\nCommit: git add <files> && git commit -m '...'"
    block(msg)
else:
    # Needs manual verification
    msg = f"[PLAYWRIGHT] {n_targets} UI file{'s' if n_targets != 1 else ''} changed — verify before committing."
    msg += f"\n\nChanged:\n{changed_summary}"
    msg += f"\nURL: {display_url}"
    if check_summary:
        msg += check_summary
    msg += f"\n\nVerify with Playwright, then:\n  touch {FLAG}"
    block(msg)
