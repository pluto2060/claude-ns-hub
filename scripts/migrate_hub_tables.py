#!/usr/bin/env python3
"""M1300: Migrate hub-related tables from ctx DB (hub-ctx-jaytoone) to hub DB (hub-jaytoone).
Tables: stones, hub_milestone_thread, hub_usage
"""
import os, sys, json, requests, time

def _load_env(path):
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

_env = _load_env(os.path.expanduser("~/.config/hub/env"))

def _get(key):
    return os.environ.get(key) or _env.get(key, "")

CTX_URL = _get("HUB_CTX_TURSO_URL").replace("libsql://", "https://")
CTX_TOKEN = _get("HUB_CTX_TURSO_TOKEN")
HUB_URL = _get("TURSO_DATABASE_URL").replace("libsql://", "https://")
HUB_TOKEN = _get("TURSO_AUTH_TOKEN")

if not all([CTX_URL, CTX_TOKEN, HUB_URL, HUB_TOKEN]):
    print("ERROR: Missing env vars.")
    print(f"  CTX_URL={CTX_URL[:30]}...")
    print(f"  HUB_URL={HUB_URL[:30]}...")
    sys.exit(1)

def ctx_exec(sql, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [{"type": "text", "value": str(a)} if a is not None else {"type": "null"} for a in args]
    r = requests.post(f"{CTX_URL}/v2/pipeline",
        headers={"Authorization": f"Bearer {CTX_TOKEN}", "Content-Type": "application/json"},
        json={"requests": [{"type": "execute", "stmt": stmt}]}, timeout=30)
    r.raise_for_status()
    return r.json()["results"][0]["response"]["result"]

def hub_exec(sql, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [{"type": "text", "value": str(a)} if a is not None else {"type": "null"} for a in args]
    r = requests.post(f"{HUB_URL}/v2/pipeline",
        headers={"Authorization": f"Bearer {HUB_TOKEN}", "Content-Type": "application/json"},
        json={"requests": [{"type": "execute", "stmt": stmt}]}, timeout=30)
    r.raise_for_status()
    return r.json()["results"][0]["response"]["result"]

def hub_batch(stmts):
    """Execute multiple statements in one pipeline call."""
    r = requests.post(f"{HUB_URL}/v2/pipeline",
        headers={"Authorization": f"Bearer {HUB_TOKEN}", "Content-Type": "application/json"},
        json={"requests": [{"type": "execute", "stmt": s} for s in stmts]}, timeout=60)
    r.raise_for_status()
    return r.json()["results"]

def row_to_dict(result, row):
    cols = [c["name"] for c in result["cols"]]
    return {c: (v["value"] if v["type"] != "null" else None) for c, v in zip(cols, row)}

# ─── 1. Create tables in hub DB ──────────────────────────────────────────────
print("Creating tables in hub DB...")

hub_exec("""CREATE TABLE IF NOT EXISTS stones (
    proj_id TEXT NOT NULL, stone_id TEXT NOT NULL, status TEXT, text TEXT,
    claude_ack TEXT, held INTEGER DEFAULT 0, done INTEGER DEFAULT 0,
    updated_at TEXT, data_json TEXT, total_tokens INTEGER, model_used TEXT,
    exec_start TEXT, exec_end TEXT,
    PRIMARY KEY (proj_id, stone_id)
)""")

hub_exec("""CREATE TABLE IF NOT EXISTS hub_milestone_thread (
    id INTEGER PRIMARY KEY AUTOINCREMENT, milestone_id TEXT NOT NULL,
    project_id TEXT NOT NULL DEFAULT '', thread_text TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT '', days_elapsed INTEGER DEFAULT 0,
    project_type_id TEXT DEFAULT '', decision_keywords TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')), consent_tier INTEGER DEFAULT 1,
    UNIQUE(milestone_id, project_id)
)""")

hub_exec("""CREATE TABLE IF NOT EXISTS hub_usage (
    ts INTEGER, event TEXT, install_id TEXT, version TEXT, os TEXT,
    model_used TEXT, total_tokens INTEGER, exec_time_sec INTEGER,
    layer INTEGER, proj_hash TEXT, extra_json TEXT
)""")

print("Tables created.")

# ─── 2. Migrate stones ───────────────────────────────────────────────────────
print("\nMigrating stones...")
result = ctx_exec("SELECT proj_id, stone_id, status, text, claude_ack, held, done, updated_at, data_json, total_tokens, model_used, exec_start, exec_end FROM stones")
rows = result["rows"]
print(f"  Found {len(rows)} stones in ctx DB")

BATCH = 50
ok = err = 0
for i in range(0, len(rows), BATCH):
    batch_rows = rows[i:i+BATCH]
    stmts = []
    for row in batch_rows:
        d = row_to_dict(result, row)
        def tv(val, typ="text"):
            # Turso v2 HTTP: value must always be a string
            if val is None:
                return {"type": "null"}
            return {"type": typ, "value": str(val)}
        stmts.append({
            "sql": "INSERT OR REPLACE INTO stones (proj_id, stone_id, status, text, claude_ack, held, done, updated_at, data_json, total_tokens, model_used, exec_start, exec_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            "args": [
                tv(d["proj_id"] or ""),
                tv(d["stone_id"] or ""),
                tv(d["status"]),
                tv((d["text"] or "")[:2000]),
                tv(d["claude_ack"]),
                tv(d["held"] if d["held"] is not None else 0, "integer"),
                tv(d["done"] if d["done"] is not None else 0, "integer"),
                tv(d["updated_at"]),
                tv(d["data_json"]),
                tv(d["total_tokens"], "integer") if d["total_tokens"] is not None else {"type": "null"},
                tv(d["model_used"]),
                tv(d["exec_start"]),
                tv(d["exec_end"]),
            ]
        })
    try:
        hub_batch(stmts)
        ok += len(batch_rows)
        print(f"  stones batch {i//BATCH+1}: {ok}/{len(rows)} ok")
    except Exception as e:
        err += len(batch_rows)
        print(f"  ERROR batch {i//BATCH+1}: {e}")
    time.sleep(0.2)

print(f"  stones done: {ok} ok, {err} errors")

# ─── 3. Migrate hub_usage ────────────────────────────────────────────────────
print("\nMigrating hub_usage...")
result2 = ctx_exec("SELECT ts, event, install_id, version, os, model_used, total_tokens, exec_time_sec, layer, proj_hash, extra_json FROM hub_usage")
rows2 = result2["rows"]
print(f"  Found {len(rows2)} hub_usage rows")

ok2 = err2 = 0
for i in range(0, len(rows2), BATCH):
    batch_rows = rows2[i:i+BATCH]
    stmts = []
    for row in batch_rows:
        d = row_to_dict(result2, row)
        def tv2(val, typ="text"):
            if val is None:
                return {"type": "null"}
            return {"type": typ, "value": str(val)}
        stmts.append({
            "sql": "INSERT INTO hub_usage (ts, event, install_id, version, os, model_used, total_tokens, exec_time_sec, layer, proj_hash, extra_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            "args": [
                tv2(d["ts"], "integer"), tv2(d["event"]), tv2(d["install_id"]),
                tv2(d["version"]), tv2(d["os"]), tv2(d["model_used"]),
                tv2(d["total_tokens"], "integer"), tv2(d["exec_time_sec"], "integer"),
                tv2(d["layer"], "integer"), tv2(d["proj_hash"]), tv2(d["extra_json"]),
            ]
        })
    try:
        hub_batch(stmts)
        ok2 += len(batch_rows)
        print(f"  hub_usage batch {i//BATCH+1}: {ok2}/{len(rows2)} ok")
    except Exception as e:
        err2 += len(batch_rows)
        print(f"  ERROR batch {i//BATCH+1}: {e}")
    time.sleep(0.2)

print(f"  hub_usage done: {ok2} ok, {err2} errors")

# ─── 4. Verify ───────────────────────────────────────────────────────────────
print("\nVerification:")
for tbl in ["stones", "hub_milestone_thread", "hub_usage"]:
    cnt = hub_exec(f"SELECT COUNT(*) FROM {tbl}")["rows"][0][0]["value"]
    print(f"  hub DB {tbl}: {cnt} rows")

print("\nMigration complete.")
