#!/usr/bin/env python3
"""
stone-ctx-hook: PostToolUse hook for mcp__ns-hub__get_pending_task.

Intercepts the get_pending_task MCP response and feeds the stone text into
CTX/BM25 retrieval so context is grounded on the real task intent.

Fires on: PostToolUse(matcher=mcp__ns-hub__get_pending_task)
Output: hookSpecificOutput.additionalContext injected into Claude's context
"""
import json
import os
import sys


def _parse_tool_response(raw):
    """Handle both dict and content-block list formats for tool_response."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        # Claude Code PostToolUse: [{type: text, text: <json_str>}, ...]
        for block in raw:
            if isinstance(block, dict) and block.get("type") == "text":
                try:
                    return json.loads(block["text"])
                except Exception:
                    pass
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {}


# ── Input ────────────────────────────────────────────────────────────────────
try:
    input_data = json.load(sys.stdin)
except Exception:
    sys.exit(0)

tool_name = input_data.get("tool_name", "") or input_data.get("toolName", "")
if "get_pending_task" not in tool_name:
    sys.exit(0)

raw_response = input_data.get("tool_response") or input_data.get("toolResponse")
tool_response = _parse_tool_response(raw_response)

has_task = tool_response.get("has_task", False)
if not has_task:
    sys.exit(0)

# M1533 v4: busy=true POST moved into hub-mcp-server.py handle_get_pending_task
# (in-process _post_busy). This hook no longer manages agent-busy state.

stone_text = tool_response.get("text", "").strip()
if not stone_text:
    sys.exit(0)

conversation = tool_response.get("conversation") or []
last_user = next(
    (c.get("text", "") for c in reversed(conversation)
     if isinstance(c, dict) and c.get("role") == "user" and c.get("text", "").strip()),
    ""
)
query = stone_text
if last_user and last_user.strip() != stone_text.strip():
    query = stone_text + " " + last_user.strip()
query = query[:600]

# ── CTX Retrieval ─────────────────────────────────────────────────────────
project_dir = os.environ.get("CLAUDE_PROJECT_DIR", os.getcwd())
session_id = input_data.get("session_id", "")

try:
    # ctx_retriever uses `from src.retrieval...` internally — needs its own package root in sys.path
    import importlib.util as _ilu
    _ctx_spec = _ilu.find_spec("ctx_retriever")
    if _ctx_spec and _ctx_spec.submodule_search_locations:
        _ctx_pkg_root = os.path.dirname(list(_ctx_spec.submodule_search_locations)[0])
        if _ctx_pkg_root not in sys.path:
            sys.path.insert(0, _ctx_pkg_root)

    from ctx_retriever.retrieval.adaptive_trigger import AdaptiveTriggerRetriever
    retriever = AdaptiveTriggerRetriever(codebase_dir=project_dir)
    result = retriever.retrieve(query_id="stone-ctx", query_text=query, k=5)

    lines = [f"[CTX-STONE] Task context for: {stone_text[:80]}"]
    for f in result.retrieved_files[:5]:
        score = result.scores.get(f, 0)
        lines.append(f"  [{score:.3f}] {f}")

    # ── M88: G1+G2-DOCS via bm25-memory.py subprocess (stone.text as query) ──
    # bm25-memory.py's UserPromptSubmit fires on "Tasks ready" (trivial, skipped).
    # We call it here in PostToolUse with real stone.text so G1+G2-DOCS are
    # grounded on task intent instead of the wake message.
    try:
        import subprocess as _sp
        _bm25_hooks = [
            os.path.expanduser("~/.ctx/hooks/bm25-memory.py"),
            os.path.expanduser("~/.claude/hooks/bm25-memory.py"),
        ]
        _bm25_path = next((p for p in _bm25_hooks if os.path.exists(p)), None)
        if _bm25_path:
            _bm25_input = json.dumps({
                "prompt": stone_text,
                "session_id": session_id,
                "cwd": project_dir,
            })
            _bm25_result = _sp.run(
                ["python3", _bm25_path],
                input=_bm25_input,
                capture_output=True,
                text=True,
                timeout=25,
                env={**os.environ, "CLAUDE_PROJECT_DIR": project_dir},
            )
            if _bm25_result.returncode == 0 and _bm25_result.stdout.strip():
                try:
                    _bm25_out = json.loads(_bm25_result.stdout)
                    _bm25_ctx = (
                        _bm25_out.get("hookSpecificOutput", {}).get("additionalContext", "")
                    )
                    if _bm25_ctx:
                        lines.append("")
                        lines.append(_bm25_ctx)
                except Exception:
                    pass
    except Exception:
        pass

    # ── M88: chat-memory.py subprocess (stone.text as query) ─────────────────
    # chat-memory.py queries claude-vault FTS5+vec for past conversation context.
    try:
        import subprocess as _sp  # noqa: F811 (re-import for scoping safety)
        _chat_hooks = [
            os.path.expanduser("~/.ctx/hooks/chat-memory.py"),
            os.path.expanduser("~/.claude/hooks/chat-memory.py"),
        ]
        _chat_path = next((p for p in _chat_hooks if os.path.exists(p)), None)
        if _chat_path:
            _chat_input = json.dumps({
                "prompt": stone_text,
                "session_id": session_id,
                "cwd": project_dir,
            })
            _chat_result = _sp.run(
                ["python3", _chat_path],
                input=_chat_input,
                capture_output=True,
                text=True,
                timeout=8,
                env={**os.environ, "CLAUDE_PROJECT_DIR": project_dir},
            )
            if _chat_result.returncode == 0 and _chat_result.stdout.strip():
                try:
                    _chat_out = json.loads(_chat_result.stdout)
                    _chat_ctx = (
                        _chat_out.get("hookSpecificOutput", {}).get("additionalContext", "")
                    )
                    if _chat_ctx:
                        lines.append("")
                        lines.append(_chat_ctx)
                except Exception:
                    pass
    except Exception:
        pass

    output = {
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": "\n".join(lines)
        }
    }
    json.dump(output, sys.stdout)
    sys.stdout.flush()

    # ── Persist stone query for CTX dashboard graph ────────────────────────
    # Dashboard reads this to replace trivial hub-trigger prompts with real
    # stone text so the knowledge graph reflects actual task intent.
    try:
        import time
        _stone_path = os.path.expanduser("~/.claude/last-stone-query.json")
        _stone_data = {
            "ts": time.time(),
            "stone_text": stone_text,
            "query": query,
            "project": os.path.basename(project_dir),
            "retrieved_files": result.retrieved_files[:5],
        }
        with open(_stone_path, "w") as _f:
            json.dump(_stone_data, _f)
    except Exception:
        pass

    # ── Telemetry ─────────────────────────────────────────────────────────
    try:
        import importlib.util
        _telem_paths = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "_ctx_telemetry.py"),
            os.path.expanduser("~/.claude/hooks/_ctx_telemetry.py"),
        ]
        for _p in _telem_paths:
            if os.path.exists(_p):
                _spec = importlib.util.spec_from_file_location("_ctx_telemetry", _p)
                _mod = importlib.util.module_from_spec(_spec)
                _spec.loader.exec_module(_mod)
                _mod.log_event("stone_ctx_retrieval", {
                    "hook_source": "STONE_CTX",
                    "query_char_count": len(query),
                    "candidates_returned": len(result.retrieved_files),
                    "session_id": session_id,
                    "strategy": result.strategy,
                })
                break
    except Exception:
        pass

except Exception:
    sys.exit(0)
