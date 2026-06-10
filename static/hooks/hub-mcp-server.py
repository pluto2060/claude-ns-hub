#!/usr/bin/env python3
"""
NS Hub MCP Server — stdio JSON-RPC 2.0 transport
Exposes hub task dispatch/completion tools to Claude Code sessions.

Usage:
  python3 hub-mcp-server.py --proj MOAT --hub-url http://100.x.x.x:9001

Claude Code connects via --mcp-config pointing to a JSON file that references this script.
The session calls get_pending_task() at start, report_task_complete() on finish.
"""
import argparse
import json
import sys
import urllib.request
import datetime


def _hub_request(url: str, method: str = "GET", body: dict = None, timeout: int = 15) -> dict:
    # M1115: increased timeout 5→15s for slow networks / hub load; retries on transient failure
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body else None,
        headers={"Content-Type": "application/json"} if body else {},
        method=method,
    )
    last_err = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
    raise last_err


# M1115: improved tool descriptions for cross-model compatibility (Claude, Gemini, DeepSeek, Kimi via OpenRouter/LiteLLM)
TOOLS = [
    {
        "name": "get_pending_task",
        "description": (
            "Fetch the next queued task (stone) assigned to this exec session from the hub. "
            "Call this at the start of your session to know what to work on. "
            "Returns task_id, text (full task description), conversation history, and metadata. "
            "If has_task is false, there is nothing to do — report idle and wait."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "report_task_complete",
        "description": (
            "Report that you have completed a task (stone). "
            "Call this after finishing work on a task to mark it done and notify the user. "
            "task_id must match the ID returned by get_pending_task (e.g. 'M1090'). "
            "summary must be a one-line past-tense description of what you did (max 120 chars). "
            "status should be 'pending_confirmation' for completed work. "
            "CHILD STONE PRE-CHECK: If the task involved decomposing work into sub-tasks (자녀 스톤, 분해, etc.), "
            "you MUST have already called create_child_stone() for each sub-task before calling this. "
            "Do NOT use this tool's summary field to claim child stones exist — they only exist if create_child_stone() was called. "
            "M1133 RESULT RULE: if the task produced a shareable result (image, document, Excel, PDF, "
            "code file, analysis), FIRST upload it: "
            "rclone copy <file> 'gdrive:claude-shared/<proj>/outbox/' && "
            "rclone link 'gdrive:claude-shared/<proj>/outbox/<filename>' → then pass the URL as evidence_url. "
            "This URL appears as the 'result' badge on the stone. Omitting it for result-producing tasks is a failure."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "The milestone ID returned by get_pending_task (e.g. 'M1090')",
                },
                "summary": {
                    "type": "string",
                    "description": "One-line past-tense summary of what was done (max 120 chars)",
                },
                "star_relation": {
                    "type": "string",
                    "description": "One sentence describing how this completion advances the parent goal (optional)",
                },
                "status": {
                    "type": "string",
                    "description": "Use 'pending_confirmation' for completed work, 'skipped' if not actionable. Default: pending_confirmation",
                },
                "evidence_url": {
                    "type": "string",
                    "description": "M1133: GDrive URL of the result artifact (image, doc, file). Upload via rclone first, then pass the link here. Shows as 'result' badge on the stone.",
                },
            },
            "required": ["task_id", "summary"],
        },
    },
    {
        "name": "reply_to_stone",
        "description": (
            "Post a reply comment to a task (stone) in Q&A mode. "
            "Use this when the task conversation ends with a user question — answer it here without changing task status. "
            "message must be ≤3 lines of plain text answering the user's question."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Milestone/stone ID (e.g. 'M1090')"},
                "message": {"type": "string", "description": "Reply text answering the user's question, max 3 lines"},
            },
            "required": ["task_id", "message"],
        },
    },
    {
        "name": "get_task_details",
        "description": (
            "Get full details of a specific task including conversation history and sub-stones. "
            "Use when you need more context about a task before working on it."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Milestone/stone ID (e.g. 'M1090')"},
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "create_child_stone",
        "description": (
            "MANDATORY tool to register a child (sub) stone in the hub database. "
            "WHEN YOU MUST CALL THIS: "
            "(1) Task text contains '자녀 스톤', '분해', 'sub-task', 'child stone', '작업 분해', or similar decomposition keywords. "
            "(2) User message asks to 'register child stones', '자녀 스톤 등록', or 'create sub-stones'. "
            "(3) get_pending_task returns _child_stone_required=true. "
            "CRITICAL: Writing 'M19.1 done, M19.2 done' in a reply_to_stone message is NOT the same as calling this tool. "
            "Child stones only appear in the hub table when THIS tool is called — not from summary text. "
            "For N child stones → call this tool N times (once per stone). "
            "WORKFLOW: call create_child_stone() for each child FIRST, then do the implementation work, then report_task_complete(). "
            "parent_id: the parent stone ID (e.g. 'M19'). "
            "text: full description of the child task. "
            "status: 'queued' to enter work queue immediately. "
            "For [검수] review stones format: '[검수] <MID>: <summary>\\n적용내용: <what changed>\\n→ ...'."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "parent_id": {
                    "type": "string",
                    "description": "ID of the parent stone (e.g. 'M1090')",
                },
                "text": {
                    "type": "string",
                    "description": "Full text of the new child stone",
                },
                "status": {
                    "type": "string",
                    "description": "Initial status: 'queued' (default) to enter work queue, or 'pending' for informational",
                },
            },
            "required": ["parent_id", "text"],
        },
    },
    {
        "name": "get_session_overview",
        "description": (
            "Get a full status overview for this session: queued tasks, stones awaiting your reply, "
            "stones needing clarification, and paused stones. "
            "Call this at session start to understand the complete landscape of work before fetching a specific task."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    # M1149 v2 CTX tools (get_recent_chats, search_decision_history, search_codespace, search_memory)
    # DISABLED — moved back to PUSH-injection via UserPromptSubmit hooks (hub_ctx hooks restored).
    # Token overhead of 4 tool definitions (~1k tokens fixed) outweighs on-demand benefit.
]


def _compress_conv_for_llm(conv: list, threshold: int = 5, keep_last: int = 4) -> list:
    """M1154 v6: Smart ON-THE-FLY compression for LLM context.
    - 1st compression: fires when raw turns > threshold (5).
    - Re-compression: only fires when raw turns > keep_last + threshold (9),
      preventing pointless recompression after every single new turn.
    - Between compressions: return [existing_summary] + raw (preserve history).
    DB is never modified; UI fetches full conv from /api/northstar/{proj}/milestones."""
    if not conv or not isinstance(conv, list):
        return conv or []
    existing_summaries = [c for c in conv if isinstance(c, dict) and c.get("role") == "summary"]
    raw = [c for c in conv if isinstance(c, dict) and c.get("role") != "summary"]
    has_prior_summary = bool(existing_summaries)
    if has_prior_summary:
        # After prior compression: only re-compress when enough new turns have accumulated
        recompress_threshold = keep_last + threshold  # default 4+5=9
        if len(raw) <= recompress_threshold:
            return existing_summaries + raw  # not enough new turns — keep existing summary
    else:
        if len(raw) <= threshold:
            return raw  # first time, not enough turns yet
    # Build compression (first-time or re-compress)
    to_squash = raw[:-keep_last]
    keep = raw[-keep_last:]
    _trim = lambda t: (str(t or "").replace("\n", " ").strip())[:200]
    u_turns = [c for c in to_squash if c.get("role") == "user"]
    c_turns = [c for c in to_squash if c.get("role") == "claude"]
    sum_lines = []
    if u_turns:
        sum_lines.append("U: " + _trim(u_turns[0].get("text") or u_turns[0].get("content", "")))
    if len(u_turns) > 1:
        sum_lines.append("Un: " + _trim(u_turns[-1].get("text") or u_turns[-1].get("content", "")))
    if c_turns:
        sum_lines.append("C: " + _trim(c_turns[-1].get("text") or c_turns[-1].get("content", "")))
    summary_text = " | ".join(sum_lines) if sum_lines else f"({len(to_squash)} earlier turns omitted)"
    summary_entry = {
        "role": "summary",
        "text": summary_text,
        "ts": to_squash[-1].get("ts", "") if to_squash else "",
        "compressed_count": len(to_squash),
    }
    return [summary_entry] + keep


def handle_get_pending_task(proj_id: str, hub_url: str) -> dict:
    try:
        data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones")
        milestones = data if isinstance(data, list) else data.get("milestones", [])
        queued = [
            m for m in milestones
            if m.get("status") == "queued"
            and not m.get("held")
            and not m.get("done")
            and str(m.get("text", "")).strip()
        ]
        if not queued:
            return {
                "has_task": False,
                "message": "No queued tasks. Session is idle.",
                "queued_count": 0,
            }
        stone = queued[0]
        # M1154 v5: compress conversation FOR THE LLM on the fly. The DB stays untouched
        # (UI fetches full history from /api/northstar/{proj}/milestones).
        _full_conv = stone.get("conversation") or []
        _llm_conv = _compress_conv_for_llm(_full_conv, threshold=5, keep_last=4)
        result = {
            "has_task": True,
            "task_id": stone.get("id"),
            "text": stone.get("text", ""),
            "conversation": _llm_conv,
            "conversation_full_count": len(_full_conv),
            "queued_count": len(queued),
            "substar": stone.get("substar_id"),
            "added_at": stone.get("user_added_at"),
        }
        # M1143: auto-detect decomposition tasks — remind model to call create_child_stone
        _CHILD_KEYWORDS = ["자녀 스톤", "자녀스톤", "child stone", "분해", "sub-task",
                           "subtask", "작업 분해", "작업분해", "하위 스톤", "서브스톤"]
        _text_lower = stone.get("text", "").lower()
        _conv_last = (stone.get("conversation") or [{}])[-1].get("text", "").lower()
        if any(kw.lower() in _text_lower or kw.lower() in _conv_last for kw in _CHILD_KEYWORDS):
            result["_child_stone_required"] = True
            result["_instruction"] = (
                "CHILD STONE REQUIRED: This task involves sub-task decomposition. "
                "You MUST call create_child_stone() for EACH sub-task before doing any implementation. "
                "Do NOT just write a reply_to_stone message claiming the stones exist — call the MCP tool."
            )
        return result
    except Exception as e:
        return {"error": str(e), "has_task": False}


def handle_report_task_complete(
    proj_id: str, hub_url: str,
    task_id: str, summary: str,
    star_relation: str = "", status: str = "pending_confirmation",
    evidence_url: str = "",
) -> dict:
    try:
        patch = {
            "status": status,
            "model_used": "claude-mcp",
            "pending_confirm_at": datetime.datetime.now().isoformat(),
            "append_message": {"role": "claude", "text": summary},  # M1117: no client-side truncation — server enforces 3-line limit
        }
        if star_relation:
            patch["star_relation"] = star_relation
        # M1133: pass evidence_url so the 'result' badge appears on the stone
        if evidence_url:
            patch["evidence_url"] = evidence_url
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        resp = {"ok": result.get("ok", False), "task_id": task_id, "status": status}
        # M1145: surface proof_warning from server — exec session must upload evidence_url and retry
        if result.get("proof_warning"):
            resp["ok"] = False
            resp["requires_evidence"] = True
            resp["error"] = (
                "evidence_url MISSING — result badge will not appear. "
                "Upload the result file first: "
                "rclone copy <file> 'gdrive:claude-shared/<proj>/outbox/' && "
                "rclone link 'gdrive:claude-shared/<proj>/outbox/<filename>' → "
                "then call report_task_complete again with evidence_url=<link>."
            )
        return resp
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_get_session_overview(proj_id: str, hub_url: str) -> dict:
    try:
        data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones")
        milestones = data if isinstance(data, list) else data.get("milestones", [])

        def _awaits_user(m):
            conv = m.get("conversation") or []
            return bool(conv) and conv[-1].get("role") == "user"

        queued = [
            m for m in milestones
            if m.get("status") == "queued" and not m.get("held") and not m.get("done")
        ]
        pending_replies = [
            m for m in milestones
            if _awaits_user(m)
        ]
        clarifications = [
            m for m in milestones
            if m.get("status") == "needs_clarification" and not (m.get("clarification_answer") or "").strip()
        ]
        paused = [
            m for m in milestones
            if m.get("status") in ("queued", "pending") and _awaits_user(m)
        ]

        def _stone_summary(m):
            conv = m.get("conversation") or []
            last_user = next((e.get("text", "")[:100] for e in reversed(conv) if e.get("role") == "user"), "")
            return {
                "task_id": m.get("id"),
                "text_preview": (m.get("text") or "")[:120],
                "is_qa": _awaits_user(m),
                "last_user_message": last_user or None,
            }

        return {
            "queued": [_stone_summary(m) for m in queued],
            "pending_replies": [_stone_summary(m) for m in pending_replies],
            "clarifications": [
                {"task_id": m.get("id"), "question": (m.get("clarification_question") or "")[:120]}
                for m in clarifications
            ],
            "paused_count": len(paused),
            "summary": f"{len(queued)} queued, {len(pending_replies)} pending reply, {len(clarifications)} clarification(s)",
        }
    except Exception as e:
        return {"error": str(e)}


def handle_reply_to_stone(proj_id: str, hub_url: str, task_id: str, message: str) -> dict:
    try:
        patch = {
            "append_message": {"role": "claude", "text": message},
        }
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        return {"ok": result.get("ok", False), "task_id": task_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_get_task_details(proj_id: str, hub_url: str, task_id: str) -> dict:
    try:
        data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones")
        milestones = data if isinstance(data, list) else data.get("milestones", [])
        stone = next((m for m in milestones if m.get("id") == task_id), None)
        if not stone:
            return {"error": f"Task {task_id} not found"}
        return stone
    except Exception as e:
        return {"error": str(e)}


def handle_create_child_stone(
    proj_id: str, hub_url: str,
    parent_id: str, text: str, status: str = "queued"
) -> dict:
    """M1137: Create a child stone via the hub POST /milestones API."""
    try:
        valid_statuses = {"queued", "pending", "needs_clarification"}
        if status not in valid_statuses:
            status = "queued"
        body = {
            "parent_id": parent_id,
            "text": text,
            "status": status,
        }
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones",
            method="POST",
            body=body,
        )
        return {"ok": result.get("ok", False), "id": result.get("id", ""), "parent_id": parent_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# M1149 v2: subprocess wrappers exposing CTX memory hooks as PULL-mode MCP tools.
# Each tool spawns the existing hook script with synthesized stdin JSON and captures
# its additionalContext output (the same payload it would have PUSH-injected).
import subprocess
import os as _os

_HOOKS_DIR = _os.path.dirname(_os.path.abspath(__file__))
_CHAT_MEMORY = _os.path.join(_HOOKS_DIR, "chat-memory.py")
_BM25_MEMORY = _os.path.join(_HOOKS_DIR, "bm25-memory.py")
_PY = "/usr/bin/python3.10"

def _run_hook(script_path: str, stdin_payload: dict, extra_args: list = None, timeout: int = 10) -> dict:
    """Spawn a hook script, feed it JSON on stdin, capture stdout JSON (additionalContext payload)."""
    if not _os.path.exists(script_path):
        return {"ok": False, "error": f"hook not found: {script_path}"}
    try:
        cmd = [_PY, script_path] + (extra_args or [])
        proc = subprocess.run(
            cmd,
            input=json.dumps(stdin_payload),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        # Hook protocol: stdout may be empty (no result) or JSON {hookSpecificOutput:{additionalContext:...}}
        out = (proc.stdout or "").strip()
        if not out:
            return {"ok": True, "results": [], "raw": "", "stderr": (proc.stderr or "")[:200]}
        try:
            parsed = json.loads(out)
        except Exception:
            return {"ok": True, "results": [], "raw": out[:4000]}
        ctx = (parsed.get("hookSpecificOutput") or {}).get("additionalContext") or ""
        return {"ok": True, "additionalContext": ctx, "raw_keys": list(parsed.keys())}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "hook timeout"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_get_recent_chats(proj_id: str, hub_url: str, query: str, limit: int = 5, cwd: str = None) -> dict:
    """CM: query chat-memory.py for past Claude chat snippets matching the query."""
    payload = {
        "prompt": query,
        "cwd": cwd or _os.getcwd(),
        "session_id": "mcp-pull",
    }
    res = _run_hook(_CHAT_MEMORY, payload, timeout=8)
    res["tool"] = "get_recent_chats"
    res["query"] = query
    return res


def handle_search_decision_history(proj_id: str, hub_url: str, query: str, since_days: int = None, cwd: str = None) -> dict:
    """G1: BM25 search over decisions/commits/world-model via bm25-memory.py."""
    payload = {
        "prompt": query,
        "cwd": cwd or _os.getcwd(),
        "session_id": "mcp-pull",
    }
    if since_days is not None:
        payload["since_days"] = since_days
    res = _run_hook(_BM25_MEMORY, payload, extra_args=["--rich"], timeout=12)
    res["tool"] = "search_decision_history"
    res["query"] = query
    return res


def handle_search_codespace(proj_id: str, hub_url: str, query: str, limit: int = 5, cwd: str = None) -> dict:
    """G2: code/space graph search — bm25-memory.py without --rich (graph-only)."""
    payload = {
        "prompt": query,
        "cwd": cwd or _os.getcwd(),
        "session_id": "mcp-pull",
    }
    res = _run_hook(_BM25_MEMORY, payload, timeout=10)
    res["tool"] = "search_codespace"
    res["query"] = query
    return res


# M1149 v3: unified search_memory(scope) MCP tool — MECE-correct dispatcher with
# in-wrapper splitting of bm25-memory output by section markers ("G1" / "G2").
def _split_g1_g2(additional_context: str) -> dict:
    """Parse bm25-memory additionalContext and split G1 / G2 / leftover sections."""
    import re
    out = {"g1": "", "g2": "", "leftover": ""}
    if not additional_context:
        return out
    lines = additional_context.split("\n")
    cur = "leftover"
    buf = {"g1": [], "g2": [], "leftover": []}
    for line in lines:
        if re.match(r"^\s*>?\s*\*\*G1\*\*", line):
            cur = "g1"; buf["g1"].append(line); continue
        if re.match(r"^\s*>?\s*\*\*G2\*\*", line):
            cur = "g2"; buf["g2"].append(line); continue
        if re.match(r"^\s*>?\s*\*\*CM\*\*", line):
            cur = "leftover"; buf["leftover"].append(line); continue
        buf[cur].append(line)
    out["g1"] = "\n".join(buf["g1"]).strip()
    out["g2"] = "\n".join(buf["g2"]).strip()
    out["leftover"] = "\n".join(buf["leftover"]).strip()
    return out


def handle_search_memory(proj_id: str, hub_url: str, query: str, scope: str = "all",
                        limit: int = 5, cwd: str = None) -> dict:
    """Unified MECE memory search — dispatches by scope.
    scope='chat' → CM only (chat-memory.py)
    scope='time' → G1 only (bm25-memory, --rich, G1 section)
    scope='space' → G2 only (bm25-memory, G2 section)
    scope='all'   → CM + G1 + G2 combined, sectioned"""
    scope = (scope or "all").lower().strip()
    if scope not in ("chat", "time", "space", "all"):
        scope = "all"
    payload = {"prompt": query, "cwd": cwd or _os.getcwd(), "session_id": "mcp-pull"}
    result = {"ok": True, "tool": "search_memory", "scope": scope, "query": query}

    if scope in ("chat", "all"):
        cm_res = _run_hook(_CHAT_MEMORY, payload, timeout=8)
        result["chat"] = (cm_res.get("additionalContext") or "").strip()
        result["chat_ok"] = cm_res.get("ok", False)

    if scope in ("time", "space", "all"):
        bm_res = _run_hook(_BM25_MEMORY, payload, extra_args=["--rich"], timeout=12)
        split = _split_g1_g2(bm_res.get("additionalContext") or "")
        if scope in ("time", "all"):
            result["time"] = split["g1"]
        if scope in ("space", "all"):
            result["space"] = split["g2"]
        if scope == "all":
            result["leftover"] = split["leftover"]
        result["bm25_ok"] = bm_res.get("ok", False)

    # MECE summary
    result["sections_present"] = sorted([
        k for k in ("chat", "time", "space")
        if k in result and result.get(k)
    ])
    return result


def send(obj: dict):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proj", required=True)
    parser.add_argument("--hub-url", required=True)
    args = parser.parse_args()

    proj_id = args.proj
    hub_url = args.hub_url.rstrip("/")

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            msg = json.loads(raw_line)
        except Exception:
            continue

        msg_id = msg.get("id")
        method = msg.get("method", "")
        params = msg.get("params", {})

        # MCP initialization handshake
        if method == "initialize":
            send({
                "jsonrpc": "2.0", "id": msg_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "ns-hub", "version": "1.0.0"},
                },
            })

        elif method == "initialized":
            pass  # notification, no response

        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}})

        elif method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments", {})
            try:
                if tool_name == "get_pending_task":
                    result = handle_get_pending_task(proj_id, hub_url)
                elif tool_name == "get_session_overview":
                    result = handle_get_session_overview(proj_id, hub_url)
                elif tool_name == "reply_to_stone":
                    result = handle_reply_to_stone(proj_id, hub_url,
                        task_id=tool_args.get("task_id", ""),
                        message=tool_args.get("message", ""),
                    )
                elif tool_name == "report_task_complete":
                    # M1115: normalize status — non-Claude models may send "done" or "complete"; map to valid values
                    raw_status = tool_args.get("status", "pending_confirmation")
                    valid_statuses = {"pending_confirmation", "skipped"}
                    if raw_status not in valid_statuses:
                        raw_status = "pending_confirmation"  # safe default
                    result = handle_report_task_complete(
                        proj_id, hub_url,
                        task_id=tool_args.get("task_id", ""),
                        summary=tool_args.get("summary", ""),
                        star_relation=tool_args.get("star_relation", ""),
                        status=raw_status,
                        evidence_url=tool_args.get("evidence_url", ""),
                    )
                elif tool_name == "get_task_details":
                    result = handle_get_task_details(proj_id, hub_url, tool_args.get("task_id", ""))
                elif tool_name == "create_child_stone":
                    result = handle_create_child_stone(
                        proj_id, hub_url,
                        parent_id=tool_args.get("parent_id", ""),
                        text=tool_args.get("text", ""),
                        status=tool_args.get("status", "queued"),
                    )
                else:
                    result = {"error": f"Unknown tool: {tool_name}"}

                # M1115: compact JSON (no indent) to reduce token count for non-Claude models
                result_text = json.dumps(result, ensure_ascii=False, separators=(',', ':'))
                send({
                    "jsonrpc": "2.0", "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": result_text}],
                        "isError": bool(result.get("error")),
                    },
                })
            except Exception as e:
                send({
                    "jsonrpc": "2.0", "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps({"error": str(e)})}],
                        "isError": True,
                    },
                })

        elif method == "ping":
            send({"jsonrpc": "2.0", "id": msg_id, "result": {}})

        # Ignore unknown notifications (no id = notification)
        elif msg_id is not None:
            send({
                "jsonrpc": "2.0", "id": msg_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            })


if __name__ == "__main__":
    main()
