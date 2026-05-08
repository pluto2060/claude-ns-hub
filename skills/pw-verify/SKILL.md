---
name: pw-verify
description: Generic E2E invariant verification for the Hub dashboard. Checks health APIs, tab loading, dark mode propagation, NS table data, and no JS console errors — regardless of what was last changed. Use after any Hub UI change to catch regressions.
---

# /pw-verify — Hub E2E Invariant Verifier

Generic verification that catches ANY regression across all hub tabs, not just specific features.

## What it checks (invariants — always true regardless of change)

| # | Invariant | How |
|---|---|---|
| 1 | All services healthy | `GET /health/{northstar,ctx,corpus,market-signals}` → `ok:true` |
| 2 | All tabs load | No offline overlay visible after iframe load |
| 3 | Dark mode propagates | `data-theme=dark` on all iframes (or cross-origin postMessage confirmed) |
| 4 | NS table renders | `#grid tr[data-orig-idx]` count > 0 |
| 5 | No JS console errors | `page.on('error')` catches nothing |

## Execution Protocol

### Step 1: Run the verify script
```bash
python3 ~/.claude/hub/verify.py --screenshot
```

Check exit code:
- `0` = all pass → done
- `1` = failures found → investigate

### Step 2: Read results

Parse the output. For each `❌` failure:

| Failure | Likely cause | Fix |
|---|---|---|
| `health/ctx` | CTX server crashed | `ps aux \| grep ctx`, restart |
| `health/corpus` | Corpus server down | Check entity dashboard server |
| `tab_loaded/corpus` | Iframe URL wrong | Check `/config` API response |
| `dark_mode/corpus` | postMessage not firing | Check initConfig load listener |
| `dark_mode/ctx` | Same as above | Check CTX iframe load listener |
| `ns_table_rows` | NS API returning empty | `curl /api/northstar` → check projects dir |
| `no_console_errors` | JS error in last change | Screenshot + console log → fix |

### Step 3: Take screenshot (if needed)
Screenshot saved to `/tmp/hub-verify.png` with `--screenshot` flag.

### Step 4: If dark_mode failure specifically
Run the cross-origin diagnosis:
```bash
# Check if postMessage listener exists in corpus page
curl -s http://$(ss -tlnp | grep ':8989' | grep -oP '\d+\.\d+\.\d+\.\d+' | head -1):8989/ | grep -c "hub-theme"
```
Expected: `2` (CSS vars + listener). If `0`, dark mode not added yet.

## When to run

- After any Hub CSS/JS change
- After adding a new tab or iframe
- After server restart to confirm all services healthy
- Stop hook auto-runs this on session end (when Hub files were modified)

## Quick mode (health only)

For fast check without browser:
```bash
python3 -c "
import urllib.request, json
base = 'http://100.119.82.4:9000'
for s in ['northstar','ctx','corpus','market-signals']:
    d = json.loads(urllib.request.urlopen(f'{base}/health/{s}', timeout=3).read())
    print(f'{'✅' if d.get('ok') else '❌'} {s}')
"
```
