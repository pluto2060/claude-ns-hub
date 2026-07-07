#!/usr/bin/env python3
"""Clone NS Hub → Telegram status reporter (text-only, cron: every 3h)."""
import json
import os
import re
import urllib.request
import urllib.parse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

HUB_BASE = "http://127.0.0.1:9001/api/northstar/Clone"
HUB_API = f"{HUB_BASE}/milestones"
HUB_NS_API = f"{HUB_BASE}/north-stars"
SHARED_ENV = Path.home() / ".claude" / "env" / "shared.env"


def _load_shared_env(path: Path) -> dict:
    """Parse simple KEY=VALUE / export KEY="VALUE" lines."""
    out = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        out[k.strip()] = v
    return out


def _get_credentials():
    """Prefer process env, fall back to shared.env file."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN_CLONE", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not (token and chat_id):
        env = _load_shared_env(SHARED_ENV)
        token = token or env.get("TELEGRAM_BOT_TOKEN_CLONE", "").strip()
        chat_id = chat_id or env.get("TELEGRAM_CHAT_ID", "").strip()
    return token, chat_id

STATUS_META = {
    "queued":               ("🔥", "긴급 대기"),
    "in_progress":          ("⚡", "진행 중"),
    "needs_clarification":  ("❓", "추가 정보 필요"),
    "pending_confirmation": ("✅", "확인 대기"),
    "paused":               ("⏸️", "일시 정지"),
    "pending":              ("📋", "대기 큐"),
}
STATUS_ORDER = ["queued", "in_progress", "needs_clarification",
                "pending_confirmation", "paused", "pending"]


def _esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _clean(t):
    t = (t or "").replace("\n", " ").strip()
    t = re.sub(r"PASTE[^P]*?(?:/PASTE)?(?=PASTE|$|\s)", "[file] ", t)
    t = re.sub(r"�+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def fetch_milestones():
    with urllib.request.urlopen(HUB_API, timeout=10) as r:
        return json.loads(r.read().decode("utf-8")).get("milestones", [])


def fetch_substar_names():
    """Return {substar_id: substar_name}."""
    try:
        with urllib.request.urlopen(HUB_NS_API, timeout=10) as r:
            d = json.loads(r.read().decode("utf-8"))
        return {ns["id"]: ns.get("name") or ns["id"] for ns in d.get("north_stars", [])}
    except Exception:
        return {}


def format_report(milestones, substar_names):
    # Drop done; group by substar then by status
    active = [m for m in milestones if m.get("status") != "done"]
    total = len(active)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    L = []
    L.append(f"📊 <b>Clone 프로젝트 상태 보고</b>")
    L.append(f"🕐 {now}")
    L.append("")

    if total == 0:
        L.append("🎉 활성 마일스톤 없음 — 모두 완료/대기")
        L.append("━━━━━━━━━━━━━━━")
        L.append("🤖 <i>다음 보고: 1시간 후</i>")
        return "\n".join(L)

    # Group by substar
    by_substar = defaultdict(list)
    for m in active:
        sid = m.get("substar_id") or ""
        by_substar[sid].append(m)

    # Order: known substars first (by name), then temp/unassigned at end
    def _sort_key(sid):
        name = substar_names.get(sid, "")
        if not sid:
            return (2, "")
        if sid.startswith("temp_"):
            return (1, name or sid)
        if not name:
            return (1, sid)
        return (0, name)

    L.append(f"📈 총 활성 <b>{total}</b>건 · 서브스타 <b>{len(by_substar)}</b>개")
    L.append("")

    for sid in sorted(by_substar.keys(), key=_sort_key):
        items = by_substar[sid]
        name = substar_names.get(sid) or (sid if sid else "(미지정)")
        if sid.startswith("temp_"):
            name = f"임시 ({sid[-6:]})"

        # Status breakdown for this substar
        by_status = defaultdict(list)
        for m in items:
            by_status[m.get("status", "unknown")].append(m)

        chips = []
        for s in STATUS_ORDER:
            n = len(by_status.get(s, []))
            if n > 0:
                emoji, _ = STATUS_META[s]
                chips.append(f"{emoji}{n}")
        chips_str = " · ".join(chips) if chips else "—"

        L.append(f"🌟 <b>{_esc(name)}</b>  <i>({len(items)}건)</i>  {chips_str}")

        # List items grouped by status under this substar
        for s in STATUS_ORDER:
            sitems = by_status.get(s, [])
            if not sitems:
                continue
            emoji, label = STATUS_META[s]
            L.append(f"  {emoji} <b>{label}</b>")
            for m in sitems:
                text = _clean(m.get("text", ""))
                if len(text) > 60:
                    text = text[:57] + "…"
                L.append(f"    • <code>{m['id']}</code>  {_esc(text)}")
        L.append("")

    L.append("━━━━━━━━━━━━━━━")
    L.append("🤖 <i>다음 보고: 1시간 후 · @Plutto02_bot</i>")
    return "\n".join(L)


def send_telegram(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {e.code}: {err}") from e


def main():
    token, chat_id = _get_credentials()
    if not (token and chat_id):
        print(f"ERROR: missing TELEGRAM_BOT_TOKEN_CLONE or TELEGRAM_CHAT_ID in env or {SHARED_ENV}")
        return 1

    try:
        ms = fetch_milestones()
    except Exception as e:
        print(f"ERROR fetching: {e}")
        return 2

    substar_names = fetch_substar_names()
    text = format_report(ms, substar_names)
    try:
        status, _ = send_telegram(token, chat_id, text)
        print(f"OK status={status}")
    except Exception as e:
        print(f"ERROR sending: {e}")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
