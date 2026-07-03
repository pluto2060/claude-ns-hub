#!/usr/bin/env python3
"""
NS Hub MCP Server — stdio JSON-RPC 2.0 transport
Exposes hub task dispatch/completion tools to Claude Code sessions.

Usage (all args optional — auto-detected from env/CWD):
  python3 hub-mcp-server.py [--proj PROJ_ID] [--hub-url http://HOST:PORT]

Priority: --arg → NS_HUB_PROJ/NS_HUB_URL env → ~/.hub/config.yaml → default localhost:9001
"""
import argparse
import json
import os
import sqlite3
import sys
import urllib.request
import datetime
from pathlib import Path


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


# M1212: P3/P4 tool description compression — keep Purpose component, trim Examples/redundant prose.
# Total savings: ~235 tokens fixed overhead per session (prompt-cached after first call).
TOOLS = [
    {
        "name": "get_pending_task",
        "description": (
            "Fetch the next queued task for this exec session (returns exactly 1 task). "
            "M1242 Option D: response is structurally separated into 3 layers — "
            "task = WHAT TO DO NOW (execute this), "
            "context = conversation history (reference only, do NOT execute), "
            "original_stone = the stone's original creation text (background only). "
            "has_task=false + should_exit=true → no tasks queued; call exit() or stop immediately (poller will re-spawn when next task arrives). "
            "SKILL PROTOCOL: skill_refs non-empty → call Skill(skill=skill_refs[0]) FIRST before any other tool. "
            "_child_stone_required=true → call create_child_stone() for each sub-task FIRST."
        ),
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "report_task_complete",
        "description": (
            "CLOSE/COMPLETE a task (changes status). "
            "Mark task done and notify user. "
            "CRITICAL: Do NOT call PATCH API directly — always use THIS MCP tool. The server requires append_message as {\"role\":\"claude\",\"text\":\"...\"} dict; passing a plain string returns HTTP 400 (append_message_type_error) and the chatbox entry is dropped. "
            "LANGUAGE RULE: summary must match the language of the user's stone input (Korean if user wrote Korean, English if English) — it appears as a stone comment. "
            "DOCUMENT LANGUAGE RULE (ENFORCED): when generating any document (Excel, docx, CSV, report), use the SAME language as the user's stone request. "
            "Korean stone → Korean headers/column names/content. English stone → English headers/content. "
            "Technical terms (SHA, API, URL, endpoint names) may remain English. File names stay ASCII. "
            "CHILD STONE PRE-CHECK: decomposition tasks (자녀 스톤/분해/sub-task) → call create_child_stone() for each sub-task FIRST. "
            "RESULT RULE — If the stone text contains any Layer A keyword, you MUST upload before calling this tool: "
            "Layer A (server blocks completion — MUST upload): excel/엑셀/pdf/docx/스크린샷/screenshot/보고서/report/chart/이미지/image/분석결과. "
            "Layer B (warning only, not blocked): 구현/추가/수정/개선/기능. "
            "UPLOAD STEPS (run in Bash tool, capture output): "
            "❌ NEVER use GDrive MCP tools (mcp__claude_ai_Google_Drive__*) for evidence upload — they encode files as base64 and are extremely slow. "
            "✅ ALWAYS use rclone via Bash tool. "
            "① rclone copy <file> 'gdrive:claude-shared/<proj>/outbox/' "
            "② GDRIVE_URL=$(rclone lsjson 'gdrive:claude-shared/<proj>/outbox/' --include '<filename>' "
            "   | python3 -c \"import sys,json;d=json.load(sys.stdin);items=[x for x in d if not x.get('IsDir',False)];print('https://drive.google.com/file/d/'+items[0]['ID']+'/view?usp=sharing') if items else print('')\") "
            "   echo $GDRIVE_URL "
            "③ pass the printed URL as evidence_url= in THIS call — NOT in summary text. "
            "FILENAME RULE: name output files as M{ID}-{suffix}.{ext} where suffix is a short descriptor or date (e.g. -v2, -YYYYMMDD, -review, -draft). "
            "For updates, delete the old GDrive file (rclone deletefile) then re-upload so a new GDrive ID is always assigned. "
            "NOTE: ❌ rclone link is prohibited — returns open?id= format that breaks on mobile. "
            "✅ Always use rclone lsjson | python3 pipeline (steps ①② above) to get the /file/d/.../view?usp=sharing URL. "
            "If you already called this tool and got requires_evidence=True: re-run steps ①② then call again with evidence_url."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "summary": {"type": "string", "description": "1-line past-tense summary including key result/finding — match user's stone input language. Text/analysis results: include the actual conclusion, not just 'done'."},
                "status": {"type": "string", "enum": ["pending_confirmation", "skipped"], "description": "default: pending_confirmation"},
                "evidence_url": {"type": "string", "description": "GDrive URL (https://drive.google.com/file/d/<ID>/view?usp=sharing) — paste here, NOT inside summary text. REQUIRED for Layer A stones; omitting returns ok=False. ❌ DO NOT use rclone link (returns open?id= format that breaks on mobile). ✅ Use: rclone lsjson 'gdrive:...' --include '<file>' | python3 -c \"import sys,json;d=json.load(sys.stdin);print('https://drive.google.com/file/d/'+d[0]['ID']+'/view?usp=sharing')\""},
                "evidence_filename": {"type": "string", "description": "Local filename of the uploaded file (e.g. 'M1234-report.xlsx'). Pass this so the UI shows the real filename instantly without a GDrive API call."},
            },
            "required": ["task_id", "summary"],
        },
    },
    {
        "name": "reply_to_stone",
        "description": (
            "REPLY without closing (no status change). "
            "ONLY for Q&A: answer a user question when the last conversation entry is from the user and you need clarification or to answer before implementing. "
            "❌ DO NOT use to report work-in-progress ('분석 중', '조사 중', '작업 시작', 'starting...', 'working on...') — silent work is correct. "
            "❌ DO NOT use as a work-start notification. "
            "✅ USE ONLY when: user asked a question you must answer before proceeding, OR you are blocked and need user input. "
            "For completed work → use report_task_complete instead. "
            "LANGUAGE: reply must match the language of the user's stone input."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "message": {"type": "string", "description": "Reply text, max 1 line"},
                "evidence_url": {"type": "string", "description": "Optional GDrive URL (https://drive.google.com/file/d/<ID>/view?usp=sharing). Pass when the QA reply delivers an artifact (e.g. revised Excel). Sets the result badge without calling attach_evidence_url separately."},
            },
            "required": ["task_id", "message"],
        },
    },
    {
        "name": "compress_summary",
        "description": (
            "SUMMARY-ONLY: attach a conversation summary to a stone WITHOUT touching conversation/status. "
            "Use ONLY for [compress] meta-tasks — read parent stone's full conversation, write a concise summary, "
            "call with the PARENT stone's task_id (not the meta-task id). "
            "evidence_url belongs in report_task_complete, not here. "
            "Updates summary_state.last_compressed_len so compress won't re-fire until conversation grows further."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "summary": {"type": "string", "description": "Concise 3-8 line summary of the conversation's key decisions, constraints, and open items. Required."},
            },
            "required": ["task_id", "summary"],
        },
    },
    {
        "name": "get_task_details",
        "description": "Get full details + conversation history for a specific task. Use when more context is needed. Returns error key if task_id not found.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "attach_evidence_url",
        "description": (
            "IDEMPOTENT evidence_url attach for ANY stone state (queued/pending/pending_confirmation/done). "
            "Use when you need to set/overwrite a stone's evidence_url AFTER it was closed, or when you forgot to "
            "pass evidence_url in the original report_task_complete call. Old URL is appended to evidence_history. "
            "Does NOT change status or append a conversation message — pure metadata update. "
            "URL FORMAT REQUIRED: https://drive.google.com/file/d/<ID>/view?usp=sharing — ❌ NEVER use rclone link "
            "(returns open?id= format that breaks on mobile). ✅ Use: rclone lsjson 'gdrive:...' --include '<file>' "
            "| python3 -c \"import sys,json;d=json.load(sys.stdin);items=[x for x in d if not x.get('IsDir',False)];"
            "print('https://drive.google.com/file/d/'+items[0]['ID']+'/view?usp=sharing') if items else print('')\". "
            "FILENAME CONVENTION: M{ID}-{suffix}.{ext} (e.g. M70-comparison.xlsx, M85-rca.xlsx)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "evidence_url": {"type": "string", "description": "GDrive /file/d/<ID>/view URL. Required."},
                "evidence_filename": {"type": "string", "description": "Local filename hint (M{ID}-{suffix}.{ext} convention). Optional but recommended."},
            },
            "required": ["task_id", "evidence_url"],
        },
    },
    {
        "name": "create_child_stone",
        "description": (
            "Register a child sub-stone in the hub. "
            "CALL WHEN: task has 자녀 스톤/분해/sub-task/child stone keywords, or _child_stone_required=true. "
            "CRITICAL: reply_to_stone claiming stones exist ≠ calling this. Only THIS tool creates them. "
            "N sub-tasks → call N times. WORKFLOW: create_child_stone × N → implement → report_task_complete. "
            "EXAMPLE: parent M123 needs 3 substeps → call create_child_stone(parent_id='M123', text='[1/3] DB schema migration', status='queued') × 3 → then implement each → report_task_complete on each child + parent."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "parent_id": {"type": "string"},
                "text": {"type": "string", "description": "Full text of the child stone"},
                "status": {"type": "string", "enum": ["queued", "pending", "needs_clarification"], "description": "queued=ready to execute (default), pending=blocked/waiting, needs_clarification=missing info"},
            },
            "required": ["parent_id", "text"],
        },
    },
    {
        "name": "get_session_overview",
        "description": "Overview of ALL tasks at once: queued, pending reply, clarifications needed. Use this to survey the full landscape; use get_pending_task to fetch exactly 1 task to work on.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_concept_graph",
        "description": (
            "M1470: Fetch the Concept Graph (CG) for a project — returns all nodes with their layer, name, and parent relationships. "
            "Use this when a task involves CG topology (adding links, reorganizing layers, understanding node hierarchy). "
            "Response: {ok, nodes: [{id, name, layer, parents, layer_order}]}. "
            "Layers are integers (0=root, 1=child, 2=grandchild). parents=[] means root node. "
            "Call with the project's proj_id (e.g. 'MOAT', 'RobotAI')."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "proj_id": {"type": "string", "description": "Project ID (e.g. 'MOAT', 'RobotAI'). Required."},
            },
            "required": ["proj_id"],
        },
    },
    # M1149 v2 CTX tools (get_recent_chats, search_decision_history, search_codespace, search_memory)
    # DISABLED — moved back to PUSH-injection via UserPromptSubmit hooks (hub_ctx hooks restored).
    # Token overhead of 4 tool definitions (~1k tokens fixed) outweighs on-demand benefit.
    {
        "name": "report_busy_state",
        "description": (
            "M1533 v3 OOB CHANNEL: agent-wrapper reports its own busy/idle state to hub directly. "
            "Replaces brittle pane-scrape (tmux capture-pane + substring match) that false-positives on slash-command mentions. "
            "Call IMMEDIATELY when transitioning: busy=true at task start, busy=false at task end. "
            "Hub uses this to gate queue-continuation poller's go-injection — busy → skip, idle → safe to inject."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "busy": {"type": "boolean", "description": "true = currently processing a task; false = idle and safe to receive next 'go'"},
                "reason": {"type": "string", "description": "Optional context (e.g. 'task_started:M1533', 'task_done:M1533', 'compacting')"},
            },
            "required": ["busy"],
        },
    },
]


def _score_sentence(sent: str, role: str) -> float:
    """M1193 v2: Non-LLM sentence importance scoring (decision-anchored extractive).
    Scores by semantic signal density: decisions > code/files > errors > questions > acks."""
    import re as _re
    s = sent.strip()
    if not s:
        return 0.0
    score = 1.0
    # Decision/completion signals — highest value (task outcomes)
    if _re.search(r'\b(완료|수정|해결|적용|구현|commit|fix[ed]?|done|implemented|added|removed|replaced)\b', s, _re.I):
        score += 4.0
    # Code/file references — strong anchors
    if _re.search(r'(```|\.py|\.ts|\.html|\.md|server\.py|northstar\.html|:\d+|commit [0-9a-f]{5,})', s):
        score += 3.0
    # Error/bug/warning signals
    if _re.search(r'\b(error|bug|fail|warn|exception|traceback|오류|버그|경고|실패)\b', s, _re.I):
        score += 2.5
    # Questions (user intent signals)
    if s.endswith('?') or s.endswith('？') or _re.search(r'\b(왜|어떻게|무엇|어디|언제|why|how|what|where|when)\b', s, _re.I):
        score += 2.0
    # Causal/reasoning markers
    if _re.search(r'\b(원인|이유|because|caused|→|therefore|결론|핵심)\b', s, _re.I):
        score += 1.5
    # Short acks/noise — penalize
    if len(s) < 25:
        score -= 2.0
    # Markdown headers often introduce important sections
    if s.startswith('#') or s.startswith('**') or s.startswith('- '):
        score += 0.5
    return max(0.0, score)


def _extract_key_sentence(text: str, role: str, max_chars: int = 200) -> str:
    """M1193 v2: Extract the single most important sentence from a turn using importance scoring.
    Falls back to first sentence if no scored sentence is found."""
    import re as _re
    sentences = [s.strip() for s in _re.split(r'(?<=[.!?。])\s+|\n', str(text or "")) if s.strip()]
    if not sentences:
        return (str(text or "").replace("\n", " ").strip())[:max_chars]
    scored = [(s, _score_sentence(s, role)) for s in sentences if len(s) > 5]
    if not scored:
        return sentences[0][:max_chars]
    best = max(scored, key=lambda x: x[1])
    return best[0][:max_chars]


def _semantic_compress_turns(turns: list) -> str:
    """M1193 v2: Decision-anchored extractive compression (non-LLM SOTA).
    For each turn: score all sentences by signal density (decisions > code > errors > questions),
    extract the single most important sentence. Output: U1: <key> | C1: <key> | U2: ...
    Korean/English mixed support via multilingual keyword patterns."""
    u_idx = c_idx = 0
    parts = []
    for turn in (turns or []):
        if not isinstance(turn, dict):
            continue
        role = turn.get("role", "")
        text = turn.get("text") or turn.get("content") or ""
        if role == "user":
            u_idx += 1
            parts.append(f"U{u_idx}: {_extract_key_sentence(text, role)}")
        elif role == "claude":
            c_idx += 1
            parts.append(f"C{c_idx}: {_extract_key_sentence(text, role)}")
    return " | ".join(parts) if parts else f"({len(turns)} turns)"


def _compress_conv_for_llm(conv: list, threshold: int = 5, keep_last: int = 4) -> list:
    """M1154 v6 / M1193: Smart ON-THE-FLY compression for LLM context.
    - 1st compression: fires when raw turns > threshold (5).
    - Re-compression: only fires when raw turns > keep_last + threshold (9),
      preventing pointless recompression after every single new turn.
    - Between compressions: return [existing_summary] + raw (preserve history).
    - Compression uses semantic extraction (all U turns + C conclusions) not first/last truncation.
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
    # Build semantic compression
    to_squash = raw[:-keep_last]
    keep = raw[-keep_last:]
    summary_text = _semantic_compress_turns(to_squash)
    summary_entry = {
        "role": "summary",
        "text": summary_text,
        "ts": to_squash[-1].get("ts", "") if to_squash else "",
        "compressed_count": len(to_squash),
    }
    return [summary_entry] + keep


_SESSION_IDENTITY_CACHE: tuple | None = None


def _session_identity() -> tuple:
    """M1656: identify THIS claude session for session-scoped busy state.
    Returns (session_key, is_exec).
    - Inside a tmux exec session (claude-exec-{proj} etc.): key = tmux session name, is_exec=True.
    - Anywhere else (user CLI/IDE conversations): key = CLAUDE_CODE_SESSION_ID, is_exec=False.
    Cached — tmux session name cannot change for the lifetime of this process."""
    global _SESSION_IDENTITY_CACHE
    if _SESSION_IDENTITY_CACHE is not None:
        return _SESSION_IDENTITY_CACHE
    sk, is_exec = "", False
    if os.environ.get("TMUX"):
        try:
            import subprocess
            sk = subprocess.run(["tmux", "display-message", "-p", "#S"],
                                capture_output=True, text=True, timeout=1).stdout.strip()
            is_exec = "-exec-" in sk
        except Exception:
            sk = ""
    if not sk:
        sk = os.environ.get("CLAUDE_CODE_SESSION_ID", "")
    _SESSION_IDENTITY_CACHE = (sk, is_exec)
    return _SESSION_IDENTITY_CACHE


def _post_busy(proj_id: str, hub_url: str, busy: bool, reason: str, stone_id: str = ""):
    """M1533 v4: in-process OOB toggle — replaces external Stop hook. Fire-and-forget.
    M1579 Phase 2: also sends stone_id so hub can build harness-agnostic task-board.
    M1656: sends session_key + is_exec so hub can ignore non-exec (CLI) sessions for dispatch."""
    try:
        _sk, _is_exec = _session_identity()
        body: dict = {"proj_id": proj_id, "busy": bool(busy), "reason": reason,
                      "session_key": _sk, "is_exec": _is_exec}
        if stone_id:
            body["stone_id"] = stone_id
        _hub_request(f"{hub_url}/api/agent-busy", method="POST", body=body, timeout=3)
    except Exception:
        pass


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
            # M1533 v4: agent has no task → idle. Toggle OFF for poller to allow next dispatch.
            # M1635 fork-protocol: should_exit=True signals agent to exit cleanly; queue poller
            # will re-spawn when next stone arrives (no pane-scrape needed for idle detection).
            _post_busy(proj_id, hub_url, False, "no_pending_task")
            return {
                "has_task": False,
                "message": "No queued tasks. Session is idle.",
                "queued_count": 0,
                "should_exit": True,
            }
        stone = queued[0]
        # M1154 v5: compress conversation FOR THE LLM on the fly. The DB stays untouched
        # (UI fetches full history from /api/northstar/{proj}/milestones).
        _full_conv = stone.get("conversation") or []
        # M1630: prefer AI-written conversation_summary (from compress child) over non-AI extractive.
        # If DB has conversation_summary → inject as summary role entry + keep last 4 raw turns.
        # Falls back to _compress_conv_for_llm (non-AI extractive) when no AI summary exists.
        _ai_summary = (stone.get("conversation_summary") or "").strip()
        if _ai_summary:
            _raw_turns = [c for c in _full_conv if isinstance(c, dict) and c.get("role") != "summary"]
            _keep = _raw_turns[-4:] if len(_raw_turns) > 4 else _raw_turns
            _summary_entry = {
                "role": "summary",
                "text": _ai_summary,
                "compressed_count": max(0, len(_raw_turns) - len(_keep)),
                "source": "ai",
            }
            _llm_conv = [_summary_entry] + _keep
        else:
            _llm_conv = _compress_conv_for_llm(_full_conv, threshold=5, keep_last=4)
        # M1242 Option D: structural separation — extract last user comment as authoritative task.
        # _full_conv used (not compressed) so primary task is never lost to compression.
        _last_user_comment = None
        for _turn in reversed(_full_conv):
            if isinstance(_turn, dict) and _turn.get("role") == "user":
                _last_user_comment = _turn.get("text", "").strip()
                break
        _stone_text = stone.get("text", "")
        # task = last user comment (if present) else original stone text — this is WHAT TO DO NOW
        _task = _last_user_comment if _last_user_comment else _stone_text
        # M687-fix: Q&A stone (conv[-1].role=='user') = reply_to_stone only, not full impl.
        # M1629-fix: logic was inverted — last_role=='claude' means user hasn't responded yet
        #   (Claude's turn is done, waiting for user). last_role=='user' means user asked something
        #   and Claude should answer. Empty conv = fresh stone = normal impl task.
        # These are short-lived — don't mark busy=true (would strand the indicator if agent
        # completes the reply but skips report_task_complete, which is not called for Q&A).
        _last_conv_role = _full_conv[-1].get("role") if _full_conv else None
        _is_qa_stone = _last_conv_role == "user"
        if _is_qa_stone:
            # M1641: QA also shows busy (avatar + session indicator) — no _session_running_stone
            # registration so dispatch is NOT blocked; TTL(120s) auto-expires after reply.
            _post_busy(proj_id, hub_url, True, "qa_stone_reply")
        else:
            # M1533 v4: agent received an impl task → busy. Poller skips redundant go-injection.
            # M1579 Phase 2: pass stone_id so hub can track running stone without pane-scrape.
            _post_busy(proj_id, hub_url, True, "pending_task_received", stone_id=stone.get("id", ""))
        # M1114: surface skill_refs as a structured field (not buried in text annotations)
        _srefs = stone.get("skill_refs") or ([stone["skill_ref"]] if stone.get("skill_ref") else [])
        # M1414: adj_refs — stone-level overrides global dp_adj_selected user-setting
        _adj_refs = stone.get("adj_refs") or []
        if not _adj_refs:
            try:
                _us = _hub_request(f"{hub_url}/api/user-settings/dp_adj_selected")
                _global_sel = _us.get("value") or []
                if isinstance(_global_sel, list):
                    _adj_refs = _global_sel
            except Exception:
                pass
        # M1242: 3-layer structural response — task/context/original_stone
        result = {
            "has_task": True,
            "task_id": stone.get("id"),
            "task": _task,                  # M1242: EXECUTE THIS — last user comment or original stone
            "context": _llm_conv,           # M1242: reference history only (do not execute)
            "original_stone": _stone_text,  # M1242: stone creation text (background)
            "queued_count": len(queued),
            "is_qa": _is_qa_stone,          # M687-fix: True = reply_to_stone only (no report_task_complete needed)
        }
        if _is_qa_stone:
            result["_qa_instruction"] = (
                "M687: conv[-1].role=='user' → user asked a follow-up question. "
                "ONLY call reply_to_stone(task_id, message). "
                "Do NOT call report_task_complete — it will be auto-completed server-side after your reply."
            )
        # Backward compat: keep text/conversation for any callers that read them
        result["text"] = _stone_text
        result["conversation"] = _llm_conv
        # M1414: inject adj_refs into task text as style instruction (appended, not replacing)
        if _adj_refs:
            result["adj_refs"] = _adj_refs
            _adj_str = ", ".join(_adj_refs)
            result["task"] = result["task"] + f"\n\n[STYLE ADJECTIVES: Approach this task in an {_adj_str} manner.]"
        if _srefs:
            result["skill_refs"] = _srefs
            # M1565: skill_refs_remaining carries [1:] for sequential chaining
            if len(_srefs) > 1:
                result["skill_refs_remaining"] = _srefs[1:]
            # Compact skill instruction — include chaining rule when multiple skills
            if len(_srefs) > 1:
                _remaining_str = ", ".join(f"'{s}'" for s in _srefs[1:])
                result["_skill_instruction"] = (
                    f"SKILL: call Skill(skill='{_srefs[0]}') FIRST. "
                    f"After it completes, call Skill(skill={_remaining_str}) sequentially in order. "
                    f"skill_refs_remaining={_srefs[1:]} — do NOT skip any."
                )
            else:
                result["_skill_instruction"] = f"SKILL: call Skill(skill='{_srefs[0]}') FIRST."
            # M1221: record invocation server-side — works from any device, no client hook needed
            # M1577: loop all _srefs (not just [0]) so multi-skill chains all get counted
            for _sref in _srefs:
                try:
                    _body = {
                        "proj_id": proj_id,
                        "stone_id": stone.get("id", ""),
                        "action": "invoked_skill",
                        "detail": _sref,
                        "session_id": os.environ.get("CLAUDE_CODE_SESSION_ID", ""),
                    }
                    _hub_request(f"{hub_url}/api/action-log", method="POST", body=_body, timeout=3)
                except Exception:
                    pass
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
    status: str = "pending_confirmation",
    evidence_url: str = "",
    evidence_filename: str = "",
) -> dict:
    try:
        patch = {
            "status": status,
            "model_used": "claude-mcp",
            "exec_end": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),  # M929: auto-set exec_end so token computation triggers
            "pending_confirm_at": datetime.datetime.now().isoformat(),
            "append_message": {"role": "claude", "text": summary},  # M1117: no client-side truncation — server enforces 3-line limit
        }
        # M1133: pass evidence_url so the 'result' badge appears on the stone
        if evidence_url:
            patch["evidence_url"] = evidence_url
        # M1304: pass local filename so server can cache it — avoids GDrive API call in UI
        if evidence_filename:
            patch["evidence_filename"] = evidence_filename
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        resp = {"ok": result.get("ok", False), "task_id": task_id, "status": status}
        # M1265: Layer A (artifact noun) → force block; Layer B (action verb) → warning only
        if result.get("requires_evidence"):
            # Layer A: artifact keyword detected — must upload before completing
            resp["ok"] = False
            resp["requires_evidence"] = True
            resp["next_action"] = "UPLOAD_THEN_RETRY"
            resp["error"] = (
                "LAYER_A_BLOCKED: evidence_url is REQUIRED for this stone (artifact keyword detected). "
                "❌ NEVER use GDrive MCP (mcp__claude_ai_Google_Drive__*) — base64 encoding makes it extremely slow. Use rclone only. "
                "MANDATORY NEXT STEPS — do NOT skip: "
                "① rclone copy <your_file> 'gdrive:claude-shared/<proj>/outbox/' "
                "② GDRIVE_URL=$(rclone lsjson 'gdrive:claude-shared/<proj>/outbox/' --include '<filename>'"
                "    | python3 -c \"import sys,json;d=json.load(sys.stdin);items=[x for x in d if not x.get('IsDir',False)];print('https://drive.google.com/file/d/'+items[0]['ID']+'/view?usp=sharing') if items else print('')\")"
                "   echo $GDRIVE_URL "
                "   ❌ DO NOT use rclone link — it returns open?id= format which breaks on mobile. "
                "③ call report_task_complete(task_id=..., summary=..., evidence_url=GDRIVE_URL) again. "
                "Task is NOT completed until evidence_url is provided."
            )
        elif result.get("output_check"):
            # M1277: Layer B soft gate — server flagged _output_check.
            # Prompt Claude to actively confirm whether output was produced.
            resp["ok"] = True
            resp["output_check"] = True
            resp["next_action"] = (
                "OUTPUT_CHECK: Layer B keyword detected. "
                "Did this task produce a shareable output (file, chart, table, analysis doc)? "
                "YES → upload via rclone and call report_task_complete again with evidence_url=<url>. "
                "NO → task is complete, no further action needed."
            )
        elif result.get("proof_warning"):
            # Layer B fallback warning (no output_check flag set)
            resp["ok"] = True
            resp["proof_warning"] = "Layer B: evidence_url missing — result badge will not show. Provide evidence_url if a file was produced."
        # M1533 v4: agent reports completion → idle. Toggle OFF for poller dispatch readiness.
        # SKIP when task did NOT actually complete: Layer A blocked (requires_evidence) → agent
        # will re-upload and call again. Setting busy=false here would race-fire immediate dispatch
        # of the SAME stone while agent is still preparing evidence.
        if not result.get("requires_evidence"):
            _post_busy(proj_id, hub_url, False, "task_complete")
        return resp
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_attach_evidence_url(
    proj_id: str, hub_url: str,
    task_id: str, evidence_url: str, evidence_filename: str = "",
) -> dict:
    """Idempotent evidence_url overwrite. Works on any stone state.
    Does NOT change status, does NOT append conversation message."""
    try:
        if not evidence_url:
            return {"ok": False, "error": "evidence_url is required"}
        patch = {"evidence_url": evidence_url}
        if evidence_filename:
            patch["evidence_filename"] = evidence_filename
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        return {
            "ok": result.get("ok", False),
            "task_id": task_id,
            "evidence_url": evidence_url,
            "evidence_filename": evidence_filename or None,
        }
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
            if _awaits_user(m) and not m.get("done")
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
            # M1212 P2: sparse projection — drop is_qa (redundant), trim text_preview, include last_user only when present
            conv = m.get("conversation") or []
            last_user = next((e.get("text", "")[:80] for e in reversed(conv) if e.get("role") == "user"), "")
            s = {"id": m.get("id"), "text": (m.get("text") or "")[:80]}
            if last_user:
                s["last_user"] = last_user
            return s

        # M1212 P2: compact summary-first response
        return {
            "summary": f"{len(queued)} queued, {len(pending_replies)} pending reply, {len(clarifications)} clarification(s)",
            "queued": [_stone_summary(m) for m in queued],
            "pending_replies": [_stone_summary(m) for m in pending_replies],
            "clarifications": [
                {"id": m.get("id"), "q": (m.get("clarification_question") or "")[:80]}
                for m in clarifications
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def handle_reply_to_stone(proj_id: str, hub_url: str, task_id: str, message: str, evidence_url: str = "") -> dict:
    try:
        patch = {
            "append_message": {"role": "claude", "text": message},
        }
        # M1638: attach evidence_url directly from reply_to_stone (QA artifact delivery)
        if evidence_url:
            patch["evidence_url"] = evidence_url
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        # M687-fix / M1629-fix: after reply_to_stone, auto-advance to pending_confirmation.
        # reply_to_stone is ONLY called for Q&A stones — impl stones use report_task_complete.
        # So we can always advance: fetch conv, verify our reply landed (last_role=='claude').
        try:
            stone_data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}")
            _conv = stone_data.get("conversation") or []
            _last_role = _conv[-1].get("role") if _conv else None
            if _last_role == "claude":
                # QA stone only — our reply landed, waiting for user again
                _hub_request(
                    f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
                    method="PATCH",
                    body={
                        "status": "pending_confirmation",
                        "model_used": "claude-mcp",
                        "exec_end": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "pending_confirm_at": datetime.datetime.now().isoformat(),
                    },
                )
                _post_busy(proj_id, hub_url, False, "qa_reply_complete")
        except Exception:
            pass
        return {"ok": result.get("ok", False), "task_id": task_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_attach_artifact(
    proj_id: str, hub_url: str, task_id: str,
    summary: str = ""
) -> dict:
    """Summary-only attach for [compress] meta-tasks — no append_message, bypasses M190. evidence_url goes in report_task_complete."""
    if not summary:
        return {"ok": False, "error": "summary is required (attach_artifact is summary-only; use report_task_complete for evidence_url)"}
    patch: dict = {}
    s_len = len(summary.strip())
    if s_len < 100:
        return {"ok": False, "error": f"summary too short ({s_len} chars) — minimum 100 chars required"}
    if s_len > 8000:
        return {"ok": False, "error": f"summary too long ({s_len} chars) — maximum 8000 chars; condense further"}
    try:
        stone_data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}")
        conv = stone_data.get("conversation") or []
        conv_text = " ".join((m.get("text") or "") for m in conv if isinstance(m, dict))
        if conv_text:
            max_proportional = max(8000, int(len(conv_text) * 0.4))
            if s_len > max_proportional:
                return {"ok": False, "error": (
                    f"summary too long ({s_len} chars) — exceeds 40% of conversation length "
                    f"({len(conv_text)} chars). Max allowed: {max_proportional}. Condense further."
                )}
    except Exception:
        pass
    patch["conversation_summary"] = summary
    patch["summary_state_bump"] = True
    try:
        result = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
            method="PATCH",
            body=patch,
        )
        # Auto-close the compress child stone so no separate report_task_complete needed.
        # Find the queued .cmp* child of this parent and mark it pending_confirmation → server auto-dones it.
        try:
            all_data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones")
            all_stones = all_data if isinstance(all_data, list) else all_data.get("milestones", [])
            cmp_child = next(
                (m for m in all_stones
                 if str(m.get("id", "")).startswith(task_id + ".cmp")
                 and m.get("status") == "queued"
                 and not m.get("done")),
                None,
            )
            if cmp_child:
                # R2 fix: lock parent with held=True before auto-confirming child,
                # so M194/M479 user-comment reset cannot flip parent back to queued
                # during the brief window between child PATCH and server auto-done.
                try:
                    _hub_request(
                        f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
                        method="PATCH",
                        body={"held": True},
                    )
                except Exception:
                    pass
                try:
                    _hub_request(
                        f"{hub_url}/api/northstar/{proj_id}/milestones/{cmp_child['id']}",
                        method="PATCH",
                        body={"status": "pending_confirmation", "model_used": "claude-mcp",
                              "pending_confirm_at": datetime.datetime.now().isoformat(),
                              "append_message": {"role": "claude", "text": "압축 완료."}},
                    )
                finally:
                    # Release hold after child PATCH so parent becomes editable again.
                    try:
                        _hub_request(
                            f"{hub_url}/api/northstar/{proj_id}/milestones/{task_id}",
                            method="PATCH",
                            body={"held": False},
                        )
                    except Exception:
                        pass
        except Exception:
            pass
        # M1633-fix: signal idle so _dispatch_blocking can fire for next queued stone.
        # compress_summary has no report_task_complete call, so busy state was never cleared.
        _post_busy(proj_id, hub_url, False, "task_complete")
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
        # M1228: compress conversation for LLM (same as get_pending_task) — raw conv is token-wasteful
        _full_conv = stone.get("conversation") or []
        stone = dict(stone)
        stone["conversation"] = _compress_conv_for_llm(_full_conv, threshold=5, keep_last=4)
        stone["conversation_full_count"] = len(_full_conv)
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


def handle_report_busy_state(proj_id: str, hub_url: str, busy: bool, reason: str = "") -> dict:
    """M1533 v3: agent-wrapper OOB busy/idle channel — POST to hub /api/agent-busy."""
    try:
        body = {"proj_id": proj_id, "busy": bool(busy), "reason": reason or ""}
        return _hub_request(f"{hub_url}/api/agent-busy", method="POST", body=body, timeout=5)
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_get_concept_graph(hub_url: str, proj_id: str) -> dict:
    """M1470: Fetch concept graph nodes for a project via hub API."""
    try:
        data = _hub_request(f"{hub_url}/api/northstar/{proj_id}/concept-graph")
        if not data.get("ok"):
            return {"ok": False, "error": data.get("error", "unknown")}
        nodes = data.get("nodes", [])
        # strip heavy fields not useful for LLM topology reasoning
        lite = [{"id": n["id"], "name": n.get("name") or n["id"], "layer": n.get("layer", 0),
                 "parents": n.get("parents", []), "layer_order": n.get("layer_order")}
                for n in nodes]
        return {"ok": True, "proj_id": proj_id, "node_count": len(lite), "nodes": lite}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send(obj: dict):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _detect_proj_id() -> str:
    cwd = Path.cwd().resolve()
    db_path = Path(os.environ.get("HUB_DATA_DIR", str(Path.home() / ".hub"))) / "ns-events.db"
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute("SELECT proj_id, meta_json FROM project_meta").fetchall()
            conn.close()
            for proj_id, meta_json in rows:
                if meta_json:
                    meta = json.loads(meta_json)
                    if meta.get("repo_path") and Path(meta["repo_path"]).resolve() == cwd:
                        return proj_id
        except Exception:
            pass
    return cwd.name.upper()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proj", default=None)
    parser.add_argument("--hub-url", default=None)
    args = parser.parse_args()

    # M1236: resolve hub URL — priority: --arg > env > ~/.hub/config.yaml > default
    def _hub_url_from_config() -> str:
        try:
            import yaml
            cfg_path = Path.home() / ".hub" / "config.yaml"
            with open(cfg_path) as _f:
                _cfg = yaml.safe_load(_f) or {}
            return (_cfg.get("defaults", {}) or {}).get("hub_url") or ""
        except Exception:
            return ""
    hub_url = (args.hub_url or os.environ.get("NS_HUB_URL") or _hub_url_from_config() or "http://localhost:9001").rstrip("/")
    proj_id = args.proj or os.environ.get("NS_HUB_PROJ") or _detect_proj_id()

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
                        evidence_url=tool_args.get("evidence_url", ""),
                    )
                elif tool_name in ("compress_summary", "attach_artifact"):
                    result = handle_attach_artifact(proj_id, hub_url,
                        task_id=tool_args.get("task_id", ""),
                        summary=tool_args.get("summary", ""),
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
                        status=raw_status,
                        evidence_url=tool_args.get("evidence_url", ""),
                        evidence_filename=tool_args.get("evidence_filename", ""),
                    )
                elif tool_name == "get_task_details":
                    result = handle_get_task_details(proj_id, hub_url, tool_args.get("task_id", ""))
                elif tool_name == "attach_evidence_url":
                    result = handle_attach_evidence_url(
                        proj_id, hub_url,
                        task_id=tool_args.get("task_id", ""),
                        evidence_url=tool_args.get("evidence_url", ""),
                        evidence_filename=tool_args.get("evidence_filename", ""),
                    )
                elif tool_name == "create_child_stone":
                    result = handle_create_child_stone(
                        proj_id, hub_url,
                        parent_id=tool_args.get("parent_id", ""),
                        text=tool_args.get("text", ""),
                        status=tool_args.get("status", "queued"),
                    )
                elif tool_name == "report_busy_state":
                    result = handle_report_busy_state(
                        proj_id, hub_url,
                        busy=bool(tool_args.get("busy", False)),
                        reason=tool_args.get("reason", ""),
                    )
                elif tool_name == "get_concept_graph":
                    result = handle_get_concept_graph(
                        hub_url,
                        proj_id=tool_args.get("proj_id", proj_id),
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
