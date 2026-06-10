# claude-ns-hub

[![PyPI Downloads](https://img.shields.io/pypi/dm/claude-ns-hub?label=downloads%2Fmonth&color=orange)](https://pypi.org/project/claude-ns-hub/)
[![PyPI Version](https://img.shields.io/pypi/v/claude-ns-hub?color=blue)](https://pypi.org/project/claude-ns-hub/)
[![GitHub Stars](https://img.shields.io/github/stars/jaytoone/claude-ns-hub?style=flat&color=yellow)](https://github.com/jaytoone/claude-ns-hub)
[![Python](https://img.shields.io/pypi/pyversions/claude-ns-hub)](https://pypi.org/project/claude-ns-hub/)

**The personal AI project hub that runs while you work.** North Star milestone tracking · live Claude exec sessions · entity corpus browser · mobile-ready terminal.

> One command. Your whole AI workflow, visible from any device.

![Hub Dashboard — North Star swimlane with live exec sessions](https://i.imgur.com/nM5naaI.png)

## Why you need this

While Claude Code runs your tasks autonomously, **you're flying blind** — no idea what it just did, which session is live, or whether it's stuck. claude-ns-hub fixes that:

- **See everything live**: exec sessions, session IDs, idle/busy state — on your phone while you're away
- **Queue work without interrupting Claude**: tap a stone, queue it, it runs on next idle
- **Resume any session**: ↻ button resumes exact conversation context, never lose work
- **One install, zero config**: auto-discovers projects, spawns entity corpus, exposes to Tailscale

The engineers shipping the most with Claude Code are the ones who can monitor, queue, and intervene — without context-switching.

## Prerequisites

- Python 3.10+
- [Claude Code CLI](https://claude.ai/code) installed and authenticated (`claude --version`)
- `tmux` installed (`brew install tmux` / `apt install tmux`)
- Tailscale (optional, for remote access)

## Install (60 seconds)

```bash
pip install claude-ns-hub
hub                          # starts immediately on http://localhost:9001
# → All features auto-active: North Star, exec sessions, entity corpus,
#   Tailscale port-expose, anonymous usage telemetry (opt-out below).
```

That's it — no config file, no env vars, no separate daemon. Open the printed URL in any browser (phone or laptop, via Tailscale) and you're in.

## Telemetry & privacy

The hub sends one anonymized `hub_start` event on startup (fields: `ts`, `event`, `install_id=sha256(hostname)[:16]`, `version`, `os`). **No PII, no code, no stone text** is transmitted. Disable any time:

```bash
curl -X POST http://localhost:9001/api/hub/consent \
  -H 'Content-Type: application/json' \
  -d '{"data_collection": false}'
```

<a id="quick-start"></a>
## Quick start (manual install)

```bash
# 1. Start the hub
claude-ns-hub
# Hub starts at http://<your-ip>:9001
# North Star · CTX · Corpus · Market — all tabs, live

# 2. Inject the NS Hub protocol into your global Claude config (run once)
hub install-global
# Writes the stone lifecycle protocol to ~/.claude/CLAUDE.md
# Without this, exec sessions won't know how to update stone status

# 3. Add your first project
# In the hub UI: North Star tab → "+ node" button
# Set the project name and repo_path to your local project directory

# 4. Queue a stone and dispatch
# Click a project card → "+ milestone" → type your task
# Click "live" to start an exec session — Claude Code picks up the stone automatically
```

## Exec session setup

The hub launches Claude Code in a `tmux` session named `claude-exec-<PROJECT>`.
For this to work on a new machine:

```bash
# Verify Claude Code is authenticated
claude --version

# Install hub hooks into Claude Code's global settings (run once per machine)
hub install-global

# The hub will auto-create tmux sessions when you dispatch work
# Monitor live progress in the "session" pane of any project card
```

## What you get

| Feature | What it does |
|---------|-------------|
| **North Star swimlane** | Visualize all projects + milestones on one screen |
| **Live exec sessions** | See `claude-exec-MOAT` running, its session ID, busy/idle state |
| **Mobile terminal** | `⌨_` button attaches browser terminal to the running Claude session — type from your phone |
| **Session resume** | ↻ rows resume exact prior conversation; ✦ starts fresh — your choice per stone |
| **Entity corpus browser** | Browse all local skills/agents/corpora; inline search |
| **Drag-and-drop comments** | Drop files into stone comments; upload auto-appended as links |
| **PyPI installable** | `pip install claude-ns-hub && claude-ns-hub` — done |

## Metrics endpoint

```bash
curl http://localhost:9000/api/metrics?proj_id=MOAT
# → stones_completed, stones_queued, total_tokens per day
```

## Configuration

```bash
# Disable entity corpus auto-spawn
ENTITY_CORPUS_DISABLED=1 claude-ns-hub

# Custom entity corpus path
ENTITY_CORPUS_SERVER=~/my-corpus/server.py claude-ns-hub
```

## Screenshots

**Mobile dark theme** — full UI on iOS/Android. Tap a card to see exec session, queue stones, resume Claude:

![Mobile dark — detail card](https://i.imgur.com/tjM3kwD.png)

![Mobile dark — swimlane overview](https://i.imgur.com/riH661r.png)

**North Star swimlane** — all projects across lanes (Cron / HI-TECH / Vertical / SVTool), badge counts, live exec indicator, parent-child links:

![North Star swimlane](https://i.imgur.com/nM5naaI.png)

**Project detail card** — North Star goal, progress bars, model/session selector, live exec session row with resume ID, milestone sub-star list:

![Project detail card](https://i.imgur.com/KjCAx1B.png)

**Skill / Agent badge picker** — assign `/expert-research` or any agent to a stone directly from the milestone row:

![Skill badge picker](https://i.imgur.com/v8VRaAz.png)

**North Star swimlane (previous)** — earlier view showing project card layout:

![North Star swimlane v1](https://i.imgur.com/TG233OE.png)

---

**pip install claude-ns-hub** — because you should know what Claude is doing right now.
