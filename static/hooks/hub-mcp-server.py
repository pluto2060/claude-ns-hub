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
import re
import subprocess
import sqlite3
import sys
import urllib.request
import urllib.error
import datetime
from pathlib import Path

# M1854-B: single source of truth for comment line-limit rule (mirrors server.py _COMMENT_RULE_TEXT).
_COMMENT_RULE_TEXT = (
    "STRUCTURED (bullets `-`, numbered `1.`, table `|`): no line limit. "
    "Unstructured prose: ≤3 lines. "
    "Prose overflow → docs/ns-replies/<DATE>-<MID>.md (M1860)"
)


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
        except urllib.error.HTTPError as e:
            # M1709: server hard-rejects (422 stale_reference_blocked, reply_line_limit_blocked,
            # etc.) return a structured JSON body with a "detail" field the LLM needs to see to
            # self-correct (e.g. "max 3 lines... or set allow_long_reply:true"). Previously the
            # body was never read — callers' `except Exception as e: return {"error": str(e)}`
            # only got Python's generic "HTTP Error 422: Unprocessable Entity", so the LLM had no
            # idea WHY it failed or how to fix it. Observed live: HugwartsBanana M110 retried
            # blindly 4 times against the new line-limit gate with no visibility into the actual
            # rule. Read the body and re-raise with it attached so callers can surface it.
            try:
                _err_body = json.loads(e.read())
            except Exception:
                _err_body = None
            last_err = HubHTTPError(e.code, _err_body) if _err_body else e
        except Exception as e:
            last_err = e
    raise last_err


class HubHTTPError(Exception):
    """M1709: carries the server's structured error JSON body through _hub_request's retry
    loop so callers can surface `detail`/`error`/etc. to the LLM instead of a bare HTTP status
    string."""
    def __init__(self, code: int, body: dict):
        self.code = code
        self.body = body
        super().__init__(f"HTTP {code}: {body.get('error', body)}")


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
            "has_task=false + should_exit=true → no tasks queued; END YOUR TURN immediately, no further tool calls (hub auto-dispatches the next task when it arrives). "
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
            "If you already called this tool and got requires_evidence=True: re-run steps ①② then call again with evidence_url. "
            f"LONG-FORM (M1817/M1860): {_COMMENT_RULE_TEXT} "
            "Never attempt reply_to_stone AFTER report_task_complete."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "summary": {"type": "string", "description": f"Past-tense summary — match user's stone input language. {_COMMENT_RULE_TEXT} Text/analysis results: include the actual conclusion, not just 'done'. (M1860/M1709/M1817)"},
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
                "message": {"type": "string", "description": f"Reply text. {_COMMENT_RULE_TEXT} Prefer structured format for multi-point answers. (M1860/M172/M270)"},
                "evidence_url": {"type": "string", "description": "Optional GDrive URL (https://drive.google.com/file/d/<ID>/view?usp=sharing). Pass when the QA reply delivers an artifact (e.g. revised Excel). Sets the result badge without calling attach_evidence_url separately."},
                "evidence_filename": {"type": "string", "description": "Local filename hint (M{ID}-{suffix}.{ext} convention). Pass alongside evidence_url so the UI shows the real filename instantly without a GDrive API call — omitting this leaves the evd badge dependent on a live GDrive lookup that can fail under quota pressure."},
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
            "Updates summary_state.last_compressed_len so compress won't re-fire until conversation grows further. "
            "CHAR BUDGET: write the summary field at ≤1500 chars. The server merges it with the prior summary "
            "(‖ chain); combined must stay under 8000 chars. Write dense facts/decisions/blockers only — "
            "no boilerplate, no preamble. This is the primary guard against unbounded summary growth. "
            "M168 FACTS RULE: The summary field must NEVER re-state, paraphrase, or re-summarize key=value lines "
            "from the ## FACTS section of MEMORY.md. Those entries use overwrite semantics and are injected "
            "automatically by the server — omit them entirely from your summary text. "
            "RE-COMPRESS FALLBACK: If the tool returns ok=False with 'previous_summary' in the response, "
            "the ‖ chain already exceeded 8000 chars (likely from old sessions before this rule). "
            "Re-compress: merge 'previous_summary' + your new summary into a single coherent text under 7500 chars, "
            "then call compress_summary again. Do NOT drop content arbitrarily — synthesize intelligently."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "summary": {"type": "string", "description": "Concise summary of key decisions, constraints, and open items. HARD LIMIT: ≤1500 chars — the server merges your new summary with the prior accumulated summary (‖ chain), and the combined result must stay under 8000 chars. Write dense, information-rich prose: drop boilerplate, keep facts, decisions, and blockers only. NEVER include MEMORY.md ## FACTS key=value lines — server auto-prepends them. Required."},
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
        "name": "upload_evidence",
        "description": (
            "M1592: ONE-CALL evidence upload — runs rclone copy + rclone lsjson + URL assembly + "
            "evidence_url PATCH internally, so the stone's evd badge is attached the INSTANT the "
            "file lands on GDrive, independent of whichever tool you call next (report_task_complete "
            "OR reply_to_stone — both skip the badge if evidence_url isn't passed explicitly; this "
            "tool removes that failure mode by not depending on either). "
            "Use this INSTEAD OF manually chaining `rclone copy` + `rclone lsjson` + attach_evidence_url. "
            "❌ NEVER use GDrive MCP (mcp__claude_ai_Google_Drive__*) — base64 encoding is extremely slow. "
            "FILENAME CONVENTION: M{ID}-{suffix}.{ext} (e.g. M70-comparison.xlsx) — pass as evidence_filename "
            "or the local basename of file_path is used."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "file_path": {"type": "string", "description": "Absolute local path of the file to upload (already written to disk)."},
                "evidence_filename": {"type": "string", "description": "Optional override filename for the GDrive copy (M{ID}-{suffix}.{ext} convention). Defaults to the basename of file_path."},
            },
            "required": ["task_id", "file_path"],
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
                "text": {"type": "string", "description": "Full text of the child stone. For M190-bypass progress updates use '[진행 N/M] parentMID: step name' convention."},
                "status": {"type": "string", "enum": ["queued", "pending", "needs_clarification", "pending_confirmation"], "description": "queued=ready to execute (default), pending=blocked/waiting, needs_clarification=missing info, pending_confirmation=M190 bypass progress update (auto-collapsed in UI, not dispatched to other sessions)"},
                "is_progress": {"type": "boolean", "description": "True = this is a mid-flight progress-update child (M190 bypass pattern). UI auto-collapses these under the parent; they are NOT dispatched to the free-pool queue."},
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


_GDRIVE_URL_RE = re.compile(r'https://drive\.google\.com/(?:file/d/|open\?id=)\S+')


def _url_param_gap_warning(message_text: str, evidence_url_param: str) -> str:
    """M1798: reply_to_stone/report_task_complete already accept evidence_url/evidence_filename
    as first-class params (M1638/M1699/M1133/M1304) — a single call can both post the reply
    AND set the result badge. Observed failure (FromScratch M140, 2026-07-13): the LLM had a
    GDrive URL in hand (from a manual rclone upload after avoiding the GDrive-MCP-prohibited
    path) and pasted it directly into the message/summary text instead of passing it via
    evidence_url — the badge then depended on the async GDrive lookup (M1697) instead of being
    set immediately. This is a soft nudge, not a hard block: a URL in the text isn't always
    evidence (could be a reference link), so we warn rather than reject.
    Returns a warning string, or "" if nothing looks off."""
    if evidence_url_param:
        return ""  # already using the parameter — nothing to warn about
    if _GDRIVE_URL_RE.search(message_text or ""):
        return (
            "M1798: a GDrive URL appears in your message/summary text but evidence_url was not "
            "passed as a parameter. Pass the URL via evidence_url (and evidence_filename) in the "
            "SAME call instead of pasting it into the text — this sets the result badge "
            "immediately instead of leaving it dependent on an async GDrive lookup."
        )
    return ""


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
    # M1766: NS_SESSION_KEY (set at spawn time) first — zero subprocess cost, zero timeout
    # risk. tmux display-message only as fallback. A transient tmux timeout used to silently
    # misroute the busy-state write to a wrong/legacy key, leaving the real session's stale
    # busy record uncleared (root cause of a session stuck "busy" ~7min after work finished).
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
        # M1656-③: use session-scoped claim endpoint so each session only picks up
        # stones assigned to it (or unassigned stones), with atomic claim-at-pickup.
        _sk, _is_exec = _session_identity()
        _claim_resp = _hub_request(
            f"{hub_url}/api/northstar/{proj_id}/claim-task",
            method="POST",
            body={"session_name": _sk},
        )
        if not _claim_resp.get("has_task"):
            # No queued tasks for this session → idle
            _post_busy(proj_id, hub_url, False, "no_pending_task")
            return {
                "has_task": False,
                "message": "No queued tasks. Session is idle.",
                "queued_count": _claim_resp.get("queued_count", 0),
                "should_exit": True,
            }
        stone = _claim_resp["stone"]
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
        # M1751: write per-session stone_id marker so PostToolUse hooks can link tool_trace rows
        # to the active stone without requiring NS_STONE_ID env (which cannot be set post-spawn).
        # Marker: ~/.claude/.stone-id-{session_key} — cleared by northstar-stop-idle.py on Stop.
        try:
            _sid = stone.get("id", "")
            if _sid and _sk:
                _marker_path = __import__("pathlib").Path.home() / ".claude" / f".stone-id-{_sk}"
                _marker_path.write_text(_sid, encoding="utf-8")
        except Exception:
            pass
        # M1114: surface skill_refs as a structured field (not buried in text annotations)
        _srefs = stone.get("skill_refs") or ([stone["skill_ref"]] if stone.get("skill_ref") else [])
        # M1825: suppress skill_refs re-injection when the skill was already invoked for this stone
        # and the stone has at least one claude conversation turn (i.e. work is in progress / was done).
        # Prevents compaction-boundary re-trigger: action_log already records invoked_skill per stone_id
        # (M1221 infrastructure). Only suppress when conv has a prior claude turn — a fresh stone with
        # no history must always get the skill, even if action_log has a stale entry from a prior run.
        if _srefs and _full_conv and any(c.get("role") == "claude" for c in _full_conv if isinstance(c, dict)):
            try:
                _stone_id = stone.get("id", "")
                _already_invoked = _hub_request(
                    f"{hub_url}/api/action-log?stone_id={_stone_id}&limit=1&action=invoked_skill",
                    method="GET",
                )
                if _already_invoked.get("rows"):
                    _srefs = []  # suppress — skill already ran for this stone in a prior turn
            except Exception:
                pass  # fail-open: if check fails, inject skill_refs as normal
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
            "queued_count": _claim_resp.get("queued_count", 0),
            "is_qa": _is_qa_stone,          # M687-fix: True = reply_to_stone only (no report_task_complete needed)
        }
        if _is_qa_stone:
            result["_qa_instruction"] = (
                "M687: conv[-1].role=='user' → user asked a follow-up question. "
                "ONLY call reply_to_stone(task_id, message). "
                "Do NOT call report_task_complete — it will be auto-completed server-side after your reply."
            )
        elif _last_conv_role == "claude":
            # M1817-P1: stone was re-queued after a prior claude turn (e.g. user re-ran it).
            # The stone has context history but the TASK field above is what to execute now.
            # DO NOT attempt to reply_to_stone or report_task_complete based on the old turn —
            # implement the task fresh. M190 gate will block any claude→claude append if you
            # try to "complete" without new user input. Treat as a new impl task.
            result["_claude_last_warning"] = (
                "M1817-P1: conv[-1].role=='claude' — this stone was re-queued after a prior completion. "
                "Execute the TASK field as a fresh implementation. "
                "Do NOT call reply_to_stone (no user question pending). "
                "Call report_task_complete when your new work is done."
            )
        # M1712: AskUserQuestion blocks this exec session's turn until a human answers in the
        # terminal — unlike report_task_complete/reply_to_stone, it has no queue-continuation
        # path, so the session sits fully idle from the hub's perspective for however long the
        # human takes to notice and respond. Injected per-task-fetch (not into the always-on
        # system prompt) since this only matters when a real choice-point is likely, keeping
        # the fixed per-spawn token cost at zero.
        result["_option_choice_hint"] = (
            "If this task requires you to choose between options/approaches: prefer stating the "
            "choice you need clarified in your reply_to_stone/report_task_complete message and "
            "leaving the stone in a state the user can respond to on their own time, rather than "
            "calling AskUserQuestion — that tool blocks this exec session's turn until a human "
            "answers in the terminal, with no hub-side queue-continuation while blocked."
        )
        # M1799: removed result["text"] (was an unlabeled duplicate of original_stone, with
        # zero consumers in hub-mcp-server.py or server.py — confirmed via grep, same dead-
        # code pattern M1782 found for the old `conversation` key). Unlike original_stone,
        # this field had no "background only" qualifier anywhere the LLM could see, and no
        # mention in the tool's schema description — a real residual risk that the LLM could
        # treat it as equally actionable as `task`, especially right after context compaction.
        # This is the same failure class observed on UniversEye M166: Claude answered the
        # stone's original creation-time question instead of the latest user comment.
        # M1782: expose last_user_comment directly instead of the full conversation array —
        # stone-ctx-hook.py only ever needed this one string (for CTX/BM25 query-building),
        # not the whole context blob, which duplicated the AI conversation_summary already
        # present in `context` (both keys held the same _llm_conv list, so any summary entry
        # was delivered to the LLM twice per get_pending_task() call).
        result["last_user_comment"] = _last_user_comment or ""
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
            # Layer B fallback warning (no output_check flag set).
            # M1699 fix: pass through the server's actual message instead of overwriting it
            # with a hardcoded "evidence_url missing" string — that string was misleading
            # for cases where evidence_url WAS provided but evidence_filename wasn't
            # (M1697's warning), making it indistinguishable from a genuine missing-URL case.
            resp["ok"] = True
            resp["proof_warning"] = result.get("proof_warning")
        _url_gap = _url_param_gap_warning(summary, evidence_url)
        if _url_gap:
            resp["evidence_param_warning"] = _url_gap
        # M1533 v4: agent reports completion → idle. Toggle OFF for poller dispatch readiness.
        # SKIP when task did NOT actually complete: Layer A blocked (requires_evidence) → agent
        # will re-upload and call again. Setting busy=false here would race-fire immediate dispatch
        # of the SAME stone while agent is still preparing evidence.
        if not result.get("requires_evidence"):
            _post_busy(proj_id, hub_url, False, "task_complete")
        return resp
    except HubHTTPError as e:
        # M1709: same fix as handle_reply_to_stone — surface the server's structured
        # error body (e.g. reply_line_limit_blocked detail) instead of a bare HTTP status.
        # M1941: if M190 blocked the append_message (claude→claude consecutive write),
        # the evidence_url was bundled in the same PATCH and silently discarded with the 409.
        # Fallback: attach evidence_url-only via a separate PATCH so the evd badge is not lost
        # even when the completion summary cannot be written (e.g. subagent posted interim comment).
        if e.code == 409 and (e.body or {}).get("error") == "claude_self_reply_blocked" and evidence_url:
            evd_result = handle_attach_evidence_url(proj_id, hub_url, task_id, evidence_url, evidence_filename)
            return {
                "ok": evd_result.get("ok", False),
                "task_id": task_id,
                "m190_blocked": True,
                "evidence_attached": evd_result.get("ok", False),
                "note": (
                    "M190: conv[-1] is already claude — append_message blocked. "
                    "evidence_url was saved via separate PATCH. "
                    "Stone stays in current status; user must re-queue or the next turn will complete it."
                ),
            }
        return {"ok": False, **(e.body or {})}
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
        out = {
            "ok": result.get("ok", False),
            "task_id": task_id,
            "evidence_url": evidence_url,
            "evidence_filename": evidence_filename or None,
        }
        # M1697: no local file_path exists in this standalone path, so there is no
        # server-verified filename to fall back on — if the caller also omits the
        # filename hint, the UI badge is permanently dependent on an async GDrive API
        # call that can fail (quota, network). Surface this immediately in the tool
        # result so the calling agent can self-correct on the same turn.
        if not evidence_filename:
            out["warning"] = (
                "evidence_filename not provided — the UI's evd badge will depend on a "
                "live GDrive API lookup that can fail (rate limits, network). Prefer "
                "upload_evidence(file_path=...) which derives the filename from the "
                "verified local file, or pass evidence_filename explicitly here."
            )
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_upload_evidence(
    proj_id: str, hub_url: str,
    task_id: str, file_path: str, evidence_filename: str = "",
) -> dict:
    """M1592: single-call evidence upload. Runs rclone copy + rclone lsjson + URL assembly
    + evidence_url PATCH (via handle_attach_evidence_url) in one shot, so the badge is
    attached the instant the file lands on GDrive — independent of whether the calling
    session later closes the stone with report_task_complete or replies with
    reply_to_stone (both silently skip the badge if evidence_url isn't passed explicitly;
    this tool removes that dependency entirely, see M130/M1592 recurrence).
    Root cause this fixes: model runs `rclone copy` + `rclone lsjson` manually in Bash,
    then forgets to also pass the resulting URL into the completion/reply call — two
    independent tool calls with no server-side link between them."""
    try:
        fp = Path(file_path)
        if not fp.is_file():
            return {"ok": False, "error": f"file_path not found: {file_path}"}
        # M1697: prefer the server-verified real filename (fp.name, just confirmed to
        # exist via fp.is_file()) over the caller-supplied evidence_filename hint — the
        # hint is an arbitrary string that can be stale/typo'd, while fp.name is ground
        # truth. Previously this was (evidence_filename or fp.name), letting a wrong
        # hint silently override a verified value.
        fname = (fp.name or evidence_filename).strip()
        dest = f"gdrive:claude-shared/{proj_id}/outbox/"
        cp = subprocess.run(["rclone", "copy", str(fp), dest], capture_output=True, text=True, timeout=120)
        if cp.returncode != 0:
            return {"ok": False, "error": f"rclone copy failed: {(cp.stderr or cp.stdout)[:300]}"}
        ls = subprocess.run(
            ["rclone", "lsjson", dest, "--include", fp.name],
            capture_output=True, text=True, timeout=45,
        )
        if ls.returncode != 0:
            return {"ok": False, "error": f"rclone lsjson failed: {(ls.stderr or ls.stdout)[:300]}"}
        items = [x for x in json.loads(ls.stdout or "[]") if not x.get("IsDir", False)]
        if not items:
            return {"ok": False, "error": f"upload succeeded but file not found via lsjson: {fp.name}"}
        file_id = items[0]["ID"]
        evidence_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        attach_result = handle_attach_evidence_url(
            proj_id, hub_url, task_id=task_id, evidence_url=evidence_url, evidence_filename=fname,
        )
        attach_result["uploaded_from"] = str(fp)
        return attach_result
    except subprocess.TimeoutExpired as e:
        return {"ok": False, "error": f"rclone timed out: {e}"}
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


def handle_reply_to_stone(proj_id: str, hub_url: str, task_id: str, message: str, evidence_url: str = "", evidence_filename: str = "") -> dict:
    try:
        patch = {
            "append_message": {"role": "claude", "text": message},
        }
        # M1638: attach evidence_url directly from reply_to_stone (QA artifact delivery)
        if evidence_url:
            patch["evidence_url"] = evidence_url
        # M1699: pass local filename hint through, same as report_task_complete/attach_evidence_url —
        # without this, reply_to_stone had no way to set it at all, guaranteeing the evd badge
        # would always depend on the async GDrive lookup for any evidence attached this way.
        if evidence_filename:
            patch["evidence_filename"] = evidence_filename
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
        out = {"ok": result.get("ok", False), "task_id": task_id}
        if result.get("proof_warning"):
            out["proof_warning"] = result.get("proof_warning")
        _url_gap = _url_param_gap_warning(message, evidence_url)
        if _url_gap:
            out["evidence_param_warning"] = _url_gap
        return out
    except HubHTTPError as e:
        # M1709: surface the server's structured 422/409 body (error code + detail +
        # any override field like allow_long_reply) so the LLM can actually self-correct
        # instead of retrying blind against a bare "HTTP Error 422" string.
        return {"ok": False, **(e.body or {})}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _extract_facts_preamble(proj_id: str) -> str:
    """M168: Extract ## FACTS key=value lines from project MEMORY.md and return as read-only preamble."""
    try:
        # Resolve project memory path: ~/.claude/projects/<cwd-as-dashes>/memory/MEMORY.md
        # proj_id is e.g. "FromScratch" — map to cwd via HOME
        home = Path.home()
        # Try direct path derivation from common project layout
        candidates = [
            home / ".claude" / "projects" / f"-home-{home.name}-Project-{proj_id}" / "memory" / "MEMORY.md",
        ]
        memory_path = next((p for p in candidates if p.exists()), None)
        if not memory_path:
            return ""
        text = memory_path.read_text(encoding="utf-8")
        # Extract lines between ## FACTS header and next ## header
        in_facts = False
        fact_lines = []
        for line in text.splitlines():
            if re.match(r"^## FACTS", line):
                in_facts = True
                continue
            if in_facts:
                if line.startswith("## "):
                    break
                stripped = line.strip()
                # Only keep key=value lines (no markdown comments, no blank lines)
                if stripped and not stripped.startswith("<!--") and "=" in stripped and not stripped.startswith("-"):
                    fact_lines.append(stripped)
        if not fact_lines:
            return ""
        facts_block = "\n".join(fact_lines)
        return f"<!-- FACT STORE (M168 — READ-ONLY, do not re-state in next compaction) -->\n{facts_block}\n<!-- END FACT STORE -->\n\n"
    except Exception:
        return ""


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
    # M1957: fetch stone via list endpoint (single-item /milestones/{id} returns 405).
    # Used for: (a) proportional-length check, (b) old ‖ new summary merge.
    stone_data: dict = {}
    try:
        _all = _hub_request(f"{hub_url}/api/northstar/{proj_id}/milestones")
        _all_items = _all if isinstance(_all, list) else _all.get("milestones", [])
        stone_data = next((m for m in _all_items if m.get("id") == task_id), {})
    except Exception:
        pass
    try:
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
    # M168: prepend FACTS block from project MEMORY.md as read-only preamble.
    # This ensures key=value env facts survive future compaction cycles unchanged.
    facts_preamble = _extract_facts_preamble(proj_id)
    # M1957: merge with previous conversation_summary so early-cycle decisions aren't lost.
    # Pattern mirrors M819 server-side compress (old ‖ new). Without this, each compress
    # cycle completely overwrites the prior AI summary — context from summary_A is gone
    # once summary_B is written, even though the raw conv entries it described are also gone.
    # M1979-fix: when merged chain exceeds cap, return error instructing Claude to re-compress
    # the full merged text down to the target size. This preserves semantic content via LLM
    # compression rather than dropping oldest segments (lossy context loss).
    _MERGE_CAP = 8000
    try:
        _prev_summary = (stone_data.get("conversation_summary") or "").strip()
        # Strip leading FACT STORE preamble from prev summary before merging (will be re-prepended)
        if _prev_summary.startswith("<!-- FACT STORE"):
            _end_marker = "<!-- END FACT STORE -->"
            _end_idx = _prev_summary.find(_end_marker)
            if _end_idx != -1:
                _prev_summary = _prev_summary[_end_idx + len(_end_marker):].strip()
        if _prev_summary and _prev_summary != summary.strip():
            _merged = _prev_summary + " ‖ " + summary.strip()
            if len(_merged) > _MERGE_CAP:
                # Do NOT drop segments — instruct Claude to re-compress intelligently.
                return {
                    "ok": False,
                    "error": (
                        f"Merged summary ({len(_merged)} chars) exceeds {_MERGE_CAP} char cap. "
                        f"Re-compress the following combined history into a single coherent summary "
                        f"under {_MERGE_CAP - 500} chars, preserving key decisions and context. "
                        f"Then call compress_summary again with your re-compressed text."
                    ),
                    "previous_summary": _merged,
                }
            summary = _merged
    except Exception:
        pass
    stored_summary = (facts_preamble + summary) if facts_preamble else summary
    patch["conversation_summary"] = stored_summary
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
        # M1919-fix: _post_busy(False) removed. It fired BEFORE the session's turn ended,
        # making the poller see idle + claimable stones and dispatch a second session to
        # claim the same parent stone while the compress session was still processing
        # get_pending_task(should_exit). Stop hook (northstar-stop-idle.py) already sends
        # busy=false/agent_stopped when the turn truly ends — no duplicate idle signal needed.
        # M1633 concern (compress_summary never calls report_task_complete → busy never cleared):
        # moot because Stop always fires after any MCP call returns.
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
    parent_id: str, text: str, status: str = "queued",
    is_progress: bool = False,
) -> dict:
    """M1137: Create a child stone via the hub POST /milestones API.
    M1907: is_progress=True marks mid-flight M190-bypass progress updates — status forced to
    pending_confirmation so free-pool sessions don't claim it."""
    try:
        valid_statuses = {"queued", "pending", "needs_clarification", "pending_confirmation"}
        if is_progress:
            status = "pending_confirmation"  # M1907: progress children must not enter free-pool queue
        elif status not in valid_statuses:
            status = "queued"
        body = {
            "parent_id": parent_id,
            "text": text,
            "status": status,
            "is_progress": is_progress,
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
                        evidence_filename=tool_args.get("evidence_filename", ""),
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
                elif tool_name == "upload_evidence":
                    result = handle_upload_evidence(
                        proj_id, hub_url,
                        task_id=tool_args.get("task_id", ""),
                        file_path=tool_args.get("file_path", ""),
                        evidence_filename=tool_args.get("evidence_filename", ""),
                    )
                elif tool_name == "create_child_stone":
                    result = handle_create_child_stone(
                        proj_id, hub_url,
                        parent_id=tool_args.get("parent_id", ""),
                        text=tool_args.get("text", ""),
                        status=tool_args.get("status", "queued"),
                        is_progress=bool(tool_args.get("is_progress", False)),
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
