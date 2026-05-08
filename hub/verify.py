#!/usr/bin/env python3
"""
hub-verify.py — Generic E2E invariant checker for the Hub dashboard.

Runs headlessly via Playwright. Verifies 5 invariants that must hold
regardless of what was last changed:

  1. Health APIs (all services respond ok:true)
  2. Each tab loads (no offline overlay)
  3. Dark mode propagates to cross-origin iframes
  4. NS table renders data (≥1 row)
  5. No JS console errors on any page

Usage:
  python3 ~/.claude/hub/verify.py
  python3 ~/.claude/hub/verify.py --url http://100.119.82.4:9000
  python3 ~/.claude/hub/verify.py --screenshot  # save screenshots to /tmp/

Exit: 0=all pass, 1=failures found
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def hub_url() -> str:
    """Detect hub URL from bound port."""
    try:
        r = subprocess.run(["ss", "-tlnp"], capture_output=True, text=True, timeout=2)
        import re
        for line in r.stdout.splitlines():
            if ":9000" in line and "LISTEN" in line:
                m = re.search(r"(\d+\.\d+\.\d+\.\d+):9000", line)
                if m:
                    return f"http://{m.group(1)}:9000"
    except Exception:
        pass
    return "http://127.0.0.1:9000"


def fetch_json(url: str, timeout: int = 5) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"ok": False, "_error": str(e)}


# ── Check 1: Health APIs ──────────────────────────────────────────────────────

def check_health(base: str) -> list[dict]:
    results = []
    for svc in ["northstar", "ctx", "corpus", "market-signals"]:
        d = fetch_json(f"{base}/health/{svc}")
        ok = d.get("ok", False)
        results.append({
            "check": f"health/{svc}",
            "ok": ok,
            "detail": "" if ok else d.get("_error", "not ok"),
        })
    return results


# ── Check 2-5: Playwright browser checks ─────────────────────────────────────

def run_playwright_checks(base: str, screenshot: bool = False) -> list[dict]:
    """Run browser-based invariant checks using Python playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return [{"check": "playwright", "ok": False, "detail": "playwright not installed"}]

    results = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1280, "height": 900})
            page = ctx.new_page()

            console_errors = []
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("pageerror", lambda e: console_errors.append(str(e)))

            # Load hub and set dark mode before iframes load
            page.goto(f"{base}/", wait_until="domcontentloaded", timeout=15000)
            page.evaluate("""() => {
                document.documentElement.setAttribute('data-theme', 'dark');
                localStorage.setItem('hub-theme', 'dark');
            }""")
            # Wait for initConfig → iframes to load
            page.wait_for_timeout(4000)

            # Check 2: offline overlays hidden
            for tab in ["northstar", "ctx", "corpus", "market-signals"]:
                el = page.query_selector(f"#offline-{tab}")
                visible = el.is_visible() if el else False
                results.append({
                    "check": f"tab_loaded/{tab}",
                    "ok": not visible,
                    "detail": "offline overlay showing" if visible else "",
                })

            # Check 3: dark mode on same-origin frames, note cross-origin
            for frame_id in ["frame-northstar", "frame-market-signals"]:
                theme = page.evaluate(f"""() => {{
                    const f = document.getElementById('{frame_id}');
                    try {{ return f.contentDocument.documentElement.getAttribute('data-theme'); }}
                    catch(e) {{ return 'cross-origin'; }}
                }}""")
                ok = theme == "dark"
                results.append({
                    "check": f"dark_mode/{frame_id.replace('frame-', '')}",
                    "ok": ok,
                    "detail": "" if ok else f"theme={theme}",
                })

            # Cross-origin frames (ctx/corpus): verify postMessage listener exists in source
            for url_part, svc in [("8787", "ctx"), ("8989", "corpus")]:
                d = fetch_json(f"{base}/health/{svc}")
                # Check that dark mode CSS is in the served HTML (API call)
                try:
                    svc_base = None
                    cfg = fetch_json(f"{base}/config")
                    svc_base = cfg.get(f"{svc}_url") if svc != "corpus" else cfg.get("corpus_url")
                    if svc == "ctx":
                        svc_base = cfg.get("ctx_url")
                    html = urllib.request.urlopen(svc_base or f"http://127.0.0.1:{url_part}/", timeout=3).read().decode()
                    has_listener = "hub-theme" in html and "postMessage" in html or "message" in html
                    results.append({
                        "check": f"dark_mode/{svc}_listener",
                        "ok": has_listener and "data-theme" in html,
                        "detail": "" if has_listener else "missing postMessage listener or dark vars",
                    })
                except Exception as e:
                    results.append({"check": f"dark_mode/{svc}_listener", "ok": False, "detail": str(e)[:80]})

            # Check 4: NS table rows (same-origin iframe)
            rows = page.evaluate("""() => {
                const f = document.getElementById('frame-northstar');
                try {
                    const tbody = f.contentDocument.querySelector('#grid');
                    return tbody ? tbody.querySelectorAll('tr[data-orig-idx]').length : 0;
                } catch(e) { return -1; }
            }""")
            results.append({
                "check": "ns_table_rows",
                "ok": rows > 0,
                "detail": f"{rows} rows" if rows > 0 else "table empty or not loaded",
            })

            # Check 5: no JS console errors
            results.append({
                "check": "no_console_errors",
                "ok": len(console_errors) == 0,
                "detail": " | ".join(console_errors[:3]) if console_errors else "",
            })

            if screenshot:
                page.screenshot(path="/tmp/hub-verify.png")
                print("[hub-verify] screenshot → /tmp/hub-verify.png")

            browser.close()
    except PWTimeout:
        results.append({"check": "playwright", "ok": False, "detail": "page load timeout"})
    except Exception as e:
        results.append({"check": "playwright", "ok": False, "detail": str(e)[:200]})

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--screenshot", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_out")
    args = parser.parse_args()

    base = args.url or hub_url()

    print(f"[hub-verify] target: {base}")

    all_results = []
    all_results += check_health(base)

    # Only run playwright checks if playwright is available
    pw_results = run_playwright_checks(base, screenshot=args.screenshot)
    all_results += pw_results

    # Report
    passed = [r for r in all_results if r["ok"]]
    failed = [r for r in all_results if not r["ok"]]

    if args.json_out:
        print(json.dumps({"passed": len(passed), "failed": len(failed), "results": all_results}))
    else:
        print(f"\n{'─'*50}")
        for r in all_results:
            icon = "✅" if r["ok"] else "❌"
            detail = f"  → {r['detail']}" if r.get("detail") else ""
            print(f"  {icon} {r['check']}{detail}")
        print(f"{'─'*50}")
        print(f"  {len(passed)}/{len(all_results)} checks passed")
        if failed:
            print(f"\n  FAILED: {', '.join(r['check'] for r in failed)}")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
