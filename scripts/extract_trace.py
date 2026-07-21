#!/usr/bin/env python3
"""
extract_trace.py — Claude Code JSONL → training-ready trace extractor

Reads Claude Code session JSONL transcripts and produces per-model JSONL trace files
suitable for SFT/RLHF training (model-agnostic, not Fable-5-specific).

Output format per line:
{
  "session_id": str,
  "model": str,
  "turn_index": int,
  "messages": [                  # OpenAI-compatible messages
    {"role": "user"|"assistant"|"tool", "content": str|list},
    ...
  ],
  "thinking": str | null,        # extended thinking block (if present)
  "tool_calls": [...],           # tool_use blocks from assistant turn
  "tool_results": [...],         # tool_result blocks from following user turn
  "usage": {...},                # token counts from assistant message
  "timestamp": str,
  "stop_reason": str,
  "proj_id": str | null,
  "cwd": str | null
}

Usage:
  python3 extract_trace.py --session <session_uuid> [--out ~/.hub/data/traces/]
  python3 extract_trace.py --all-project <proj_slug> [--out ~/.hub/data/traces/]
  python3 extract_trace.py --auto  # extract all sessions, auto-detect project from path
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


_CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
_DEFAULT_OUT = Path.home() / ".hub" / "data" / "traces"


def _parse_proj_id_from_path(proj_path: Path) -> str | None:
    """Convert projects dir slug back to proj_id.
    E.g. '-home-desk-1-Project-Moat' → 'Moat' (last segment after last dash+capital)
    """
    name = proj_path.name
    # Try to extract the last meaningful segment
    parts = name.split("-")
    # Find segments that look like project names (capitalized or all-upper)
    candidates = [p for p in parts if p and p[0].isupper()]
    if candidates:
        return candidates[-1]
    return name  # fallback: use raw slug


def _build_turns(lines: list[dict]) -> list[dict]:
    """Pair up assistant + following user (tool_results) into turns."""
    turns = []
    i = 0
    while i < len(lines):
        entry = lines[i]
        if entry.get("type") != "assistant":
            i += 1
            continue
        msg = entry.get("message", {})
        content = msg.get("content", [])
        if not isinstance(content, list):
            i += 1
            continue

        # Extract thinking + text + tool_use from assistant content
        thinking_blocks = [c for c in content if c.get("type") == "thinking"]
        text_blocks = [c for c in content if c.get("type") == "text"]
        tool_use_blocks = [c for c in content if c.get("type") == "tool_use"]

        # Look ahead for tool_results in next user entry
        tool_result_blocks = []
        next_user_text = None
        if i + 1 < len(lines) and lines[i + 1].get("type") == "user":
            next_msg = lines[i + 1].get("message", {})
            next_content = next_msg.get("content", [])
            if isinstance(next_content, list):
                tool_result_blocks = [c for c in next_content if c.get("type") == "tool_result"]
                text_parts = [c for c in next_content if c.get("type") == "text"]
                if text_parts:
                    next_user_text = " ".join(c.get("text", "") for c in text_parts)
            elif isinstance(next_content, str):
                next_user_text = next_content

        # Build OpenAI-compatible messages list for this turn
        messages = []
        # Assistant message
        assistant_content: list | str
        if tool_use_blocks:
            assistant_content = []
            if text_blocks:
                assistant_content.append({"type": "text", "text": text_blocks[0].get("text", "")})
            for tu in tool_use_blocks:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tu.get("id", ""),
                    "name": tu.get("name", ""),
                    "input": tu.get("input", {}),
                })
        else:
            assistant_content = " ".join(b.get("text", "") for b in text_blocks)

        messages.append({"role": "assistant", "content": assistant_content})

        # Tool results (as tool role messages)
        for tr in tool_result_blocks:
            result_content = tr.get("content", "")
            if isinstance(result_content, list):
                result_content = " ".join(
                    c.get("text", "") for c in result_content if c.get("type") == "text"
                )
            messages.append({
                "role": "tool",
                "tool_use_id": tr.get("tool_use_id", ""),
                "content": str(result_content)[:4000],  # cap to 4K chars
            })

        # Next user text (if not tool results only)
        if next_user_text and not tool_result_blocks:
            messages.append({"role": "user", "content": next_user_text})

        turn = {
            "session_id": entry.get("sessionId") or entry.get("session_id", ""),
            "model": msg.get("model", "unknown"),
            "turn_index": len(turns),
            "messages": messages,
            "thinking": thinking_blocks[0].get("thinking") if thinking_blocks else None,
            "tool_calls": [
                {"name": tu.get("name"), "input": tu.get("input", {})}
                for tu in tool_use_blocks
            ],
            "tool_results": [
                {"tool_use_id": tr.get("tool_use_id"), "content": tr.get("content", "")[:2000]}
                for tr in tool_result_blocks
            ],
            "usage": msg.get("usage", {}),
            "timestamp": entry.get("timestamp", ""),
            "stop_reason": msg.get("stop_reason", ""),
            "cwd": entry.get("cwd"),
            "proj_id": None,  # filled in by caller
        }
        turns.append(turn)
        i += 1

    return turns


def extract_session(jsonl_path: Path, proj_id: str | None = None) -> list[dict]:
    """Extract turns from a single session JSONL file."""
    lines = []
    for raw in jsonl_path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            lines.append(json.loads(raw))
        except json.JSONDecodeError:
            continue

    turns = _build_turns(lines)
    for t in turns:
        t["proj_id"] = proj_id
        t["source_file"] = str(jsonl_path)
    return turns


def write_traces(turns: list[dict], out_dir: Path, model_split: bool = True) -> dict[str, Path]:
    """Write turns to per-model JSONL files in out_dir. Returns {model: path}."""
    out_dir.mkdir(parents=True, exist_ok=True)
    by_model: dict[str, list[dict]] = defaultdict(list)
    for t in turns:
        by_model[t["model"]].append(t)

    written = {}
    if model_split:
        for model, model_turns in by_model.items():
            safe_name = model.replace("/", "_").replace(":", "_")
            out_path = out_dir / f"{safe_name}.jsonl"
            mode = "a"  # append so multiple sessions accumulate
            with out_path.open(mode, encoding="utf-8") as f:
                for t in model_turns:
                    f.write(json.dumps(t, ensure_ascii=False) + "\n")
            written[model] = out_path
    else:
        out_path = out_dir / "traces.jsonl"
        with out_path.open("a", encoding="utf-8") as f:
            for t in turns:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")
        written["all"] = out_path

    return written


def stats(out_dir: Path) -> None:
    """Print per-model trace counts from out_dir."""
    from collections import Counter
    print(f"\nTrace stats in {out_dir}:\n")
    total = 0
    for p in sorted(out_dir.glob("*.jsonl")):
        n = sum(1 for _ in p.open(encoding="utf-8"))
        total += n
        size_kb = p.stat().st_size // 1024
        print(f"  {p.name:<45} {n:>6} turns  {size_kb:>5} KB")
    print(f"\n  Total: {total} turns\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract training traces from Claude Code JSONL")
    ap.add_argument("--session", help="Single session UUID to extract")
    ap.add_argument("--all-project", metavar="PROJ_SLUG",
                    help="Extract all sessions under a projects/ slug")
    ap.add_argument("--auto", action="store_true",
                    help="Extract ALL sessions across ALL projects")
    ap.add_argument("--out", default=str(_DEFAULT_OUT), help="Output directory")
    ap.add_argument("--stats", action="store_true", help="Print stats from existing out dir")
    ap.add_argument("--no-model-split", action="store_true",
                    help="Write single traces.jsonl instead of per-model files")
    args = ap.parse_args()

    out_dir = Path(args.out)

    if args.stats:
        stats(out_dir)
        return

    model_split = not args.no_model_split
    total_turns = 0

    if args.session:
        # Find the JSONL file anywhere under projects/
        matches = list(_CLAUDE_PROJECTS_DIR.rglob(f"{args.session}.jsonl"))
        if not matches:
            print(f"ERROR: session {args.session} not found under {_CLAUDE_PROJECTS_DIR}")
            sys.exit(1)
        jsonl_path = matches[0]
        proj_id = _parse_proj_id_from_path(jsonl_path.parent)
        turns = extract_session(jsonl_path, proj_id)
        written = write_traces(turns, out_dir, model_split)
        total_turns = len(turns)
        print(f"Extracted {total_turns} turns from {jsonl_path.name}")
        for model, path in written.items():
            print(f"  → {path}")

    elif args.all_project:
        proj_dir = _CLAUDE_PROJECTS_DIR / args.all_project
        if not proj_dir.exists():
            print(f"ERROR: {proj_dir} does not exist")
            sys.exit(1)
        proj_id = _parse_proj_id_from_path(proj_dir)
        for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
            turns = extract_session(jsonl_path, proj_id)
            if turns:
                write_traces(turns, out_dir, model_split)
                total_turns += len(turns)
                print(f"  {jsonl_path.name}: {len(turns)} turns")
        print(f"\nTotal: {total_turns} turns written to {out_dir}")

    elif args.auto:
        for proj_dir in sorted(_CLAUDE_PROJECTS_DIR.iterdir()):
            if not proj_dir.is_dir():
                continue
            proj_id = _parse_proj_id_from_path(proj_dir)
            proj_turns = 0
            for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
                turns = extract_session(jsonl_path, proj_id)
                if turns:
                    write_traces(turns, out_dir, model_split)
                    total_turns += len(turns)
                    proj_turns += len(turns)
            if proj_turns:
                print(f"  {proj_dir.name}: {proj_turns} turns")
        print(f"\nTotal: {total_turns} turns written to {out_dir}")

    else:
        ap.print_help()
        sys.exit(1)

    stats(out_dir)


if __name__ == "__main__":
    main()
