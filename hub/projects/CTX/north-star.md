---
category: 'SVTool'
current: —
deadline: '2026-09-30'
id: CTX
links: ''
log:
- date: '2026-05-07'
  text: '08e262b fix: Korean tokenizer gap in eval pipeline + 6 regression tests;
    9cd2371 docs: fix HN item ID 47996700→48017090 (4pts), update reactions log (+1
    more)'
- date: '2026-05-08'
  text: 'fd84cf9 docs: add CJK intent comment to production tokenize() regex; 08e262b
    fix: Korean tokenizer gap in eval pipeline + 6 regression tests (+1 more)'
metric: Plugin active installs
milestones:
- done: false
  text: Publish to Claude Code plugin marketplace
- done: false
  text: Reach 50 installs
- done: false
  text: Reach 100 installs
- done: false
  text: Reach 500 installs
name: CTX
note: Claude Code memory + context retrieval plugin. Measures real adoption, not downloads.
status: on-track
target: '500'
unit: installs
---

# CTX — North Star

## Why this metric
Active installs (users with CTX running, not just downloaders) directly measures product-market fit. It's the leading indicator for future monetization (cloud tier) and credibility for consulting/teaching.

## What CTX does
Gives Claude Code persistent memory across sessions — G1 (time), G2 (space), CM (chat) retrieval hooks. Saves context to a local vault.db, retrieves relevant past decisions/docs on every prompt.

## Strategy
- Distribution: Claude Code plugin marketplace (primary), HN Show HN, GeekNews, Dev.to
- Activation: `ctx-install` one-command setup + CTX dashboard showing memory health
- Retention: per-session utility rate visible to user (they see CTX working)

## OKRs — 2026 Q2
- K1: Ship v1.0 to marketplace
- K2: Reach 50 active installs
- K3: Achieve >70% utility rate (sessions where CTX retrieval is used)

## Links
- Repo: /home/desk-1/Project/CTX
- Dashboard: http://100.119.82.4:8787