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
            "has_task=false → idle, nothing to do. "
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
            "LANGUAGE RULE: summary must match the language of the user's stone input (Korean if user wrote Korean, English if English) — it appears as a stone comment. "
            "DOCUMENT LANGUAGE RULE (ENFORCED): when generating any document (Excel, docx, CSV, report), use the SAME language as the user's stone request. "
            "Korean stone → Korean headers/column names/content. English stone → English headers/content. "
            "Technical terms (SHA, API, URL, endpoint names) may remain English. File names stay ASCII. "
            "CHILD STONE PRE-CHECK: decomposition tasks (자녀 스톤/분해/sub-task) → call create_child_stone() for each sub-task FIRST. "
            "RESULT RULE — If the stone text contains any Layer A keyword, you MUST upload before calling this tool: "
            "Layer A (server blocks completion — MUST upload): excel/엑셀/pdf/docx/스크린샷/screenshot/보고서/report/chart/이미지/image/분석결과. "
            "Layer B (warning only, not blocked): 구현/추가/수정/개선/기능. "
            "UPLOAD STEPS (run in Bash tool, capture output): "
            "① rclone copy <file> 'gdrive:claude-shared/<proj>/outbox/' "
            "② GDRIVE_URL=$(rclone lsjson 'gdrive:claude-shared/<proj>/outbox/' --include '<filename>' "
            "   | python3 -c \"import sys,json;d=json.load(sys.stdin);items=[x for x in d if not x.get('IsDir',False)];print('https://drive.google.com/file/d/'+items[0]['ID']+'/view?usp=sharing') if items else print('')\") "
            "   echo $GDRIVE_URL "
            "③ pass the printed URL as evidence_url= in THIS call — NOT in summary text. "
            "FILENAME RULE: name output files as M{ID}-{suffix}.{ext} where suffix is a short descriptor or date (e.g. -v2, -YYYYMMDD, -review, -draft). "
            "For updates, delete the old GDrive file (rclone deletefile) then re-upload so a new GDrive ID is always assigned. "
            "NOTE: rclone link returns open?id= format which may prompt login on some devices. "
            "The sed conversion to /file/d/.../view?usp=sharing is the universally accessible format. "
            "If you already called this tool and got requires_evidence=True: re-run steps ①② then call again with evidence_url."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "summary": {"type": "string", "description": "One-line past-tense summary, max 120 chars — match user's stone input language"},
                "status": {"type": "string", "enum": ["pending_confirmation", "skipped"], "description": "default: pending_confirmation"},
                "evidence_url": {"type": "string", "description": "GDrive URL from `rclone link` output — paste here, NOT inside summary text. REQUIRED for Layer A stones; omitting returns ok=False."},
                "evidence_filename": {"type": "string", "description": "Local filename of the uploaded file (e.g. 'M1234-report.xlsx'). Pass this so the UI shows the real filename instantly without a GDrive API call."},
            },
            "required": ["task_id", "summary"],
        },
    },
    {
        "name": "reply_to_stone",
        "description": (
            "REPLY without closing (no status change). "
            "Post a reply comment to a stone in Q&A mode — answer user question without changing status. "
            "DO NOT USE to mark work done — use report_task_complete for that. "
            "LANGUAGE: reply must match the language of the user's stone input."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "message": {"type": "string", "description": "Reply text, max 3 lines"},
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
    # M1149 v2 CTX tools (get_recent_chats, search_decision_history, search_codespace, search_memory)
    # DISABLED — moved back to PUSH-injection via UserPromptSubmit hooks (hub_ctx hooks restored).
    # Token overhead of 4 tool definitions (~1k tokens fixed) outweighs on-demand benefit.
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
        # M1114: surface skill_refs as a structured field (not buried in text annotations)
        _srefs = stone.get("skill_refs") or ([stone["skill_ref"]] if stone.get("skill_ref") else [])
        # M1242: 3-layer structural response — task/context/original_stone
        result = {
            "has_task": True,
            "task_id": stone.get("id"),
            "task": _task,                  # M1242: EXECUTE THIS — last user comment or original stone
            "context": _llm_conv,           # M1242: reference history only (do not execute)
            "original_stone": _stone_text,  # M1242: stone creation text (background)
            "queued_count": len(queued),
        }
        # Backward compat: keep text/conversation for any callers that read them
        result["text"] = _stone_text
        result["conversation"] = _llm_conv
        if _srefs:
            result["skill_refs"] = _srefs
            # Compact skill instruction (was ~40 tokens, now ~15)
            result["_skill_instruction"] = f"SKILL: call Skill(skill='{_srefs[0]}') FIRST."
            # M1221: record invocation server-side — works from any device, no client hook needed
            try:
                _body = {
                    "proj_id": proj_id,
                    "stone_id": stone.get("id", ""),
                    "action": "invoked_skill",
                    "detail": _srefs[0],
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
                "MANDATORY NEXT STEPS — do NOT skip: "
                "① rclone copy <your_file> 'gdrive:claude-shared/<proj>/outbox/' "
                "② RAW=$(rclone link 'gdrive:claude-shared/<proj>/outbox/<filename>') "
                "   GDRIVE_URL=$(echo $RAW | sed 's|drive\\.google\\.com/open?id=\\(.*\\)|drive.google.com/file/d/\\1/view?usp=sharing|') "
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
