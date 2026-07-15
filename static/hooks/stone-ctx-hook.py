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

# M1782: server now sends last_user_comment directly (get_pending_task no longer
# duplicates the full conversation array under this key). Fall back to the old
# conversation-array extraction for compatibility with any not-yet-restarted hub.
last_user = tool_response.get("last_user_comment", "").strip()
if not last_user:
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
    import subprocess as _sp
    from concurrent.futures import ThreadPoolExecutor as _TPE

    _bm25_hooks = [
        os.path.expanduser("~/.ctx/hooks/bm25-memory.py"),
        os.path.expanduser("~/.claude/hooks/bm25-memory.py"),
    ]
    _chat_hooks = [
        os.path.expanduser("~/.ctx/hooks/chat-memory.py"),
        os.path.expanduser("~/.claude/hooks/chat-memory.py"),
    ]
    _bm25_path = next((p for p in _bm25_hooks if os.path.exists(p)), None)
    _chat_path = next((p for p in _chat_hooks if os.path.exists(p)), None)

    _hook_input = json.dumps({
        "prompt": stone_text,
        "session_id": session_id,
        "cwd": project_dir,
    })
    _hook_env = {**os.environ, "CLAUDE_PROJECT_DIR": project_dir}

    def _run_hook(path, timeout):
        return _sp.run(
            ["python3", path],
            input=_hook_input,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_hook_env,
        )

    # ── AdaptiveTriggerRetriever runs IN PARALLEL with bm25+chat ──────────────
    # Previously: AdaptiveTrigger(1.5s) serial → then bm25+chat parallel(2.4s) = 3.9s
    # Now: all 3 concurrent → max(1.5s, 2.4s) = ~2.4s
    _ctx_spec_result = None
    _bm25_result = None
    _chat_result = None

    def _run_adaptive_trigger():
        try:
            import importlib.util as _ilu
            _spec = _ilu.find_spec("ctx_retriever")
            if _spec and _spec.submodule_search_locations:
                import sys as _sys
                _pkg_root = os.path.dirname(list(_spec.submodule_search_locations)[0])
                if _pkg_root not in _sys.path:
                    _sys.path.insert(0, _pkg_root)
            from ctx_retriever.retrieval.adaptive_trigger import AdaptiveTriggerRetriever
            _ret = AdaptiveTriggerRetriever(codebase_dir=project_dir)
            _res = _ret.retrieve(query_id="stone-ctx", query_text=query, k=5)
            return _res
        except Exception:
            return None

    _tasks = {}
    with _TPE(max_workers=3) as _ex:
        _tasks["adaptive"] = _ex.submit(_run_adaptive_trigger)
        if _bm25_path:
            _tasks["bm25"] = _ex.submit(_run_hook, _bm25_path, 25)
        if _chat_path:
            _tasks["chat"] = _ex.submit(_run_hook, _chat_path, 8)
        for _name, _fut in _tasks.items():
            try:
                _r = _fut.result(timeout=30)
                if _name == "adaptive":
                    _ctx_spec_result = _r
                elif _name == "bm25":
                    _bm25_result = _r
                else:
                    _chat_result = _r
            except Exception:
                pass

    # Build CTX-STONE lines from adaptive trigger result
    lines = [f"[CTX-STONE] Task context for: {stone_text[:80]}"]
    if _ctx_spec_result is not None:
        result = _ctx_spec_result
        for f in result.retrieved_files[:5]:
            score = result.scores.get(f, 0)
            lines.append(f"  [{score:.3f}] {f}")

    _bm25_ctx = ""
    if _bm25_result and _bm25_result.returncode == 0 and _bm25_result.stdout.strip():
        try:
            _bm25_out = json.loads(_bm25_result.stdout)
            _bm25_ctx = (
                _bm25_out.get("hookSpecificOutput", {}).get("additionalContext", "")
            )
            if _bm25_ctx:
                lines.append("")
                lines.append(_bm25_ctx)
            # Write last-injection.json so utility-rate.py can measure Hub sessions.
            try:
                import time as _time
                _inj_items = []
                _cur_block = "g1_decisions"
                _block_markers = {
                    "[G1]": "g1_decisions", "[G2]": "g2_docs",
                    "[G2-GREP]": "g2_grep", "[G2-CODE]": "g2_code",
                    "[CM]": "cm", "[CTX-STONE]": "stone",
                }
                for _ln in _bm25_ctx.splitlines():
                    _stripped = _ln.strip()
                    for _marker, _bk in _block_markers.items():
                        if _stripped.startswith(_marker):
                            _cur_block = _bk
                            break
                    if _stripped and len(_stripped) > 20:
                        _inj_items.append({
                            "block": _cur_block,
                            "subject": _stripped[:120],
                            "tokens": _stripped.split(),
                        })
                for _ln in lines[:1]:
                    for _sln in str(_ln).splitlines():
                        if _sln.strip().startswith("[") and "]" in _sln:
                            _inj_items.append({
                                "block": "stone",
                                "subject": _sln.strip()[:120],
                                "tokens": _sln.strip().split(),
                            })
                _inj_path = os.path.expanduser("~/.claude/last-injection.json")
                with open(_inj_path, "w") as _f:
                    json.dump({
                        "ts": _time.time(),
                        "items": _inj_items[:40],
                        "session_id": session_id,
                        "source": "stone-ctx-hook",
                    }, _f)
            except Exception:
                pass
        except Exception:
            pass

    if _chat_result and _chat_result.returncode == 0 and _chat_result.stdout.strip():
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
