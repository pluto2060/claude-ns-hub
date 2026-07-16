# claude-ns-hub

[![PyPI Downloads](https://img.shields.io/pypi/dm/claude-ns-hub?label=downloads%2Fmonth&color=orange)](https://pypi.org/project/claude-ns-hub/)
[![PyPI Version](https://img.shields.io/pypi/v/claude-ns-hub?color=blue)](https://pypi.org/project/claude-ns-hub/)
[![GitHub Stars](https://img.shields.io/github/stars/pluto2060/claude-ns-hub?style=flat&color=yellow)](https://github.com/pluto2060/claude-ns-hub)
[![Python](https://img.shields.io/pypi/pyversions/claude-ns-hub)](https://pypi.org/project/claude-ns-hub/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

**Drop a task. Walk away. Come back to results.**

`pip install claude-ns-hub` — 60-second install, zero config files, AGPL open source.

> I built this because I kept babysitting Claude Code sessions — checking every 10 minutes whether they'd finished. NS Hub fixes that: drop a Stone, go make coffee, get a push notification when Claude's done. I've been using it daily for 6 months.

![NS Hub demo — Stone queue to autonomous execution](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/ns-hub-banner-v9.png)

---

## What it does

NS Hub is a **local Claude Code session orchestrator** with a Stone queue and multi-session dispatch.

Instead of babysitting one Claude session, you drop tasks ("Stones") into a local SQLite queue. NS Hub dispatches them to idle Claude sessions automatically — or spins up child sessions in parallel. You monitor everything from your phone and get a push notification when work is done.

**Architecture choices:**
- **SQLite** (not a cloud service) — your data stays local, no vendor lock-in, works offline
- **tmux + pexpect for PTY control** — NS Hub injects input and reads Claude's output without a custom API wrapper
- **Mother/child session dispatch** — a mother session manages the queue; children are forked with truncated context (transcript sliced at the exact point before the forked task was claimed, so child sessions start clean)
- **180+ skills/agents** indexed locally for inline search across your corpus

| Without NS Hub | With NS Hub |
|---|---|
| Babysit one session, check every 10 min | Drop Stones, walk away, get notified |
| Claude runs blind — no visibility | Live session monitoring from your phone |
| Must open laptop to check progress | Mobile terminal — type directly from phone |
| Ideas lost before you open a laptop | Stone persistence — local SQLite, never lost |

---

## Why this exists

When Claude Code runs autonomously, you are **blind** — you cannot tell which sessions are alive, stalled, or finished.

The deeper problem is **idea loss**. Thoughts on the go, insights during commute, 3am realizations — most of them scatter without context.

NS Hub solves both at once:

- **Second Brain**: Capture ideas instantly as Stones → preserved with full context in local SQLite
- **Agent execution hub**: Stone → Claude runs autonomously → completion notification — entire loop closes on your phone

---

## Core loop

```
Idea surfaces
    ↓
Create a Stone (5 seconds from your phone)
    ↓
Claude Code picks it up from the queue
    ↓
Running — live session monitoring from phone
    ↓
Completion notification → review results
    ↓
Next Stone dispatched automatically
```

The entire loop runs without touching a computer.

---

## Screenshots

**North Star swimlane** — all projects, all lanes, live execution indicators:

![North Star swimlane](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/northstar-swimlane.png)

**Corpus browser** — 58 skills · 54 agents · 75 docs, all searchable inline:

![Corpus browser](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/corpus-browser.png)

### Mobile dark mode

| North Star swimlane | Detail card | CG panel |
|---|---|---|
| ![North Star mobile](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-northstar.png) | ![Detail card mobile](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-detail-card-open.png) | ![CG panel mobile](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-cg-panel.png) |

| Corpus browser | Live terminal overlay | Hub index |
|---|---|---|
| ![Corpus mobile](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-corpus.png) | ![Terminal overlay](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-terminal-overlay.png) | ![Hub index mobile](https://raw.githubusercontent.com/pluto2060/claude-ns-hub/master/assets/mobile-hub-index.png) |

---

## Install (60 seconds)

```bash
pip install claude-ns-hub
hub                          # starts at http://localhost:9001
```

No config files. No environment variables. No separate daemon. Open the printed URL in your phone browser and you're done.

### Prerequisites

- Python 3.10+
- [Claude Code CLI](https://claude.ai/code) (`claude --version`)
- `tmux` (`brew install tmux` / `apt install tmux`)
- Tailscale (optional — for remote mobile access)
- `litellm` (optional — only if you want to route sessions through OpenRouter instead of Claude directly; see below)

**Using OpenRouter instead of (or alongside) Claude?** Hub can auto-start a local LiteLLM proxy that routes `openrouter`-tagged sessions to OpenRouter's API. It's off by default and requires two manual setup steps hub does not create for you:
1. Install `litellm` (`pip install litellm`) and create `~/.rsk-litellm.yaml` with your model routing config.
2. Put your `OPENROUTER_API_KEY` (and any other provider keys) in `~/.claude/env/shared.env`.

Without both of these, OpenRouter sessions silently stay unavailable — hub does not error, it just never starts the proxy.

**Running on WSL2?** Hub works out of the box, with two things worth knowing:
- Port exposure to your Windows host/Tailscale is automatic on startup (`wsl-expose`, if installed) — no manual `ssh -L` forwarding needed.
- On boot, hub waits up to 60s for `tailscaled` to assign an IP before binding — if `tailscale status` isn't ready yet (common right after a fresh WSL2 launch), the first request or two may briefly fail; retrying after a few seconds resolves it.

---

## Quick start

```bash
# 1. Start the hub
hub

# 2. Register Claude Code hooks + MCP (run once per machine)
hub install-global
# Writes stone lifecycle protocol to ~/.claude/CLAUDE.md
# Registers MCP server (ns-hub) + 4 hooks in ~/.claude/settings.json
# Auto-creates ~/.config/hub/env if missing

# 3. Add your first project (two options)
#   Option A — CLI:
hub init MyProject --dir ~/Projects/MyProject
#   Option B — UI: North Star tab → "+ node" → set repo_path

# 4. (Optional) Verify setup
hub doctor
# Checks Python / tmux / claude CLI / env file / MCP / hooks / server

# 5. Drop a Stone and let Claude run it
# Click project card → "+ milestone" → type your task → "live"
# Claude picks it up on next idle turn via mcp__ns-hub__get_pending_task
```

> **Restart Claude Code** after `hub install-global` so the new MCP server and hooks are loaded.

---

## What you get

| Feature | What it does |
|---------|-------------|
| **Stone capture** | Drop any idea as a Stone — Claude picks it up on next idle |
| **Live exec sessions** | Real-time visibility: busy/idle state, session ID, last tool used |
| **Mobile terminal** | Type directly into the running Claude session from your phone |
| **Session resume** | ↻ resumes exact prior context — no re-explaining, no lost work |
| **Context persistence** | Stone history, evidence URLs, conversation summaries — all local SQLite, fully portable |
| **North Star swimlane** | All projects + milestones on one screen, any device |
| **Corpus browser** | Browse all local skills/agents/docs; inline search across 180+ entries |
| **Zero-config install** | `pip install claude-ns-hub && hub` — that's the entire setup |

---

## Directory structure

```
~/.hub/
├── server.py              — main FastAPI server
├── ns-events.db           — SQLite: stones (milestones), exec sessions, action log
├── config.yaml            — optional overrides (port, tailscale IP, etc.)
├── static/
│   ├── northstar.html     — web UI
│   └── hooks/             — Claude Code hooks (PostToolUse / Stop / PreToolUse)
├── corpora/               — local corpus collections (skills, agents, docs)
├── ee/                    — enterprise extensions (source-available)
└── relay/                 — optional Cloudflare Workers relay for remote access
```

---

## Telemetry & privacy

If data collection is enabled (default: on — see opt-out below), hub sends:

- **On startup**: one `hub_start` event — `ts`, `event`, `install_id=sha256(hostname)[:16]`,
  `version`, `os`. No PII, no code, no Stone text.
- **Every 30 minutes** (if there's new activity): batches of tool-call summaries, action-log
  entries, and Stone text (truncated to 1000 chars) — used to build an agent-training dataset.
  These batches pass through a PII scrubber (masks emails, phone numbers, IP addresses, API
  keys/tokens) before upload, but the scrubber is regex-based and cannot detect free-text
  personal content (e.g. health or financial details written into a Stone's task description).
  **If you write sensitive information directly into a Stone's text, it may be included in
  this upload even with data collection consent left at its default.** Opt out below if this
  matters to you, or avoid putting sensitive free text in Stone descriptions.

Opt out anytime:

```bash
curl -X POST http://localhost:9001/api/hub/consent \
  -H 'Content-Type: application/json' \
  -d '{"data_collection": false}'
```

---

## Push Notifications (optional)

Get a phone notification when Claude finishes a Stone.

**Setup (2 minutes):**
1. Install the [ntfy app](https://ntfy.sh) on your phone (free, open-source)
2. Pick a unique topic name (e.g. `my-hub-abc123`)
3. Edit `~/.hub/config.yaml`:
   ```yaml
   ntfy_url: https://ntfy.sh/my-hub-abc123
   ```
4. Subscribe to the same topic in the ntfy app

Hub sends a notification whenever a Stone transitions to `pending_confirmation` or `done`.

> **Self-hosted**: replace `https://ntfy.sh/` with your own ntfy server URL.
> **Local only (no internet)**: set `ntfy_url: http://127.0.0.1:9001/ntfy` (built-in relay, Tailscale required for phone access).

---

## Troubleshooting

### tmux not found
```bash
sudo apt install tmux   # Ubuntu/WSL
brew install tmux        # macOS
tmux -V                  # verify
```

### Claude Code not authenticated
```bash
claude --version
npm install -g @anthropic-ai/claude-code   # if missing
claude login
```

### Hub can't find my project
```bash
hub init <PROJECT_ID> --dir /path/to/your/project
# or: North Star → "+ node" → set repo_path manually
```

### Hooks not firing
```bash
hub install-global
cat ~/.claude/settings.json | grep hub
```

---

## Data schema & portability

All data lives in local SQLite (`~/.hub/ns-events.db`). No vendor lock-in.

```sql
-- milestones_store (Stones) — stone fields (text, status, evidence_url, etc.)
-- live inside the data_json blob, not as flat columns
CREATE TABLE milestones_store (
  proj_id TEXT NOT NULL,
  stone_id TEXT NOT NULL,        -- e.g. "M1301"
  data_json TEXT NOT NULL,       -- {"text", "status", "evidence_url", "conversation", ...}
  status TEXT,                   -- queued | pending_confirmation | done
  done INTEGER DEFAULT 0,
  held INTEGER DEFAULT 0,
  updated_at TEXT NOT NULL,
  model_used TEXT,
  exec_start TEXT,
  exec_end TEXT,
  cost_usd REAL,
  PRIMARY KEY (proj_id, stone_id)
);
```

Export/import:
```bash
sqlite3 ~/.hub/ns-events.db .dump > backup.sql
```

---

## Metrics endpoint

```bash
curl http://localhost:9001/api/metrics?proj_id=MOAT
# → stones_completed, stones_queued, total_tokens per day
```

---

**pip install claude-ns-hub** — stop babysitting Claude sessions.

---

## Current limitations (honest)

- **Linux/WSL2 only** — macOS support is on the roadmap; Windows-native is not planned
- **Requires tmux** in your PATH
- **Web UI** is functional but unpolished — PRs welcome

---

## License

This project is licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0-or-later).

If you run a modified version of this software as a network service, you must make the complete corresponding source code available to users of that service. See [LICENSE](LICENSE) for full terms.

Personal use, self-hosting, and community contributions are always free. © 2026 pluto2060 — be2jay67@gmail.com
