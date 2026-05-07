---
category: 'Research'
current: 90%
deadline: '2026-06-30'
id: FromScratch
links: ''
log:
- date: '2026-05-08'
  text: 'Phase 3 v5 DONE: 90% two-pass (18/20, +5pp over baseline). Root cause fixed: all
    2384 original traces were corrupted garbage. New clean traces (119) from darwin36b
    direct inference. Phase 4 chem/bio trace gen running (33 traces so far).'
- date: '2026-05-07'
  text: Phase 3 v4 DONE (800 steps, darwin36b base, think-tags, LoRA r=32). Greedy
    checkpoint eval running on GPUs 1-5. Baseline 85% confirmed.
- date: '2026-05-07'
  text: 'darwin36b baseline reproduced at 85% (17/20) with two-pass eval. Format fix:
    think tags required. CUDA zombie fix: empty_cache+synchronize.'
- date: '2026-05-06'
  text: 'PIVOT v2: skip FFN merge, go darwin36b direct base. Phase 3 v3 failed (plain
    traces), v4 adds think tags.'
- date: '2026-05-05'
  text: 'NCCL deadlock fixed: no_sync() + expandable_segments + 1800s init timeout'
- date: '2026-05-07'
  text: '[auto] test: endpoint working'
- date: '2026-05-07'
  text: '[auto] 20260507-test-session-status.md: Phase 3 v4 eval running on GPUs 1-5'
- date: '2026-05-07'
  text: 23d61fc 🎯 Isolation tests DEFINITIVE — Mamba2×GDN interaction is the real
    culprit; a50ba0c 5/5 on DS FINAL REJECT — 4-patch fix insufficient, pivot to mcore
    (+1 more)
- date: '2026-05-08'
  text: 23d61fc 🎯 Isolation tests DEFINITIVE — Mamba2×GDN interaction is the real
    culprit; a50ba0c 5/5 on DS FINAL REJECT — 4-patch fix insufficient, pivot to mcore
    (+1 more)
metric: GPQA Diamond single-model score
milestones:
- done: true
  text: 'Phase 1: Clean trace gen — 119 traces (fixed corruption: all traces were garbage)'
- done: true
  text: 'Phase 2: darwin36b direct base — 85% two-pass (17/20) confirmed'
- done: true
  text: 'Phase 3 v5: LoRA KD (clean think-format traces) → 90% two-pass (18/20) +5pp'
- done: false
  text: 'Phase 4: Chemistry/Biology domain injection — trace gen running (~33 traces so far)'
- done: false
  text: 'Full 198Q eval: two-pass on v5 step_200 → get reliable ±3% score (currently 90% from 20Q proxy)'
- done: false
  text: 'Phase 5: GRPO on-policy if needed (+2-3pp)'
- done: false
  text: 'Final: ≥93% single-model GPQA Diamond (north star: 93.9%)'
name: FromScratch
note: OR-Ensemble distillation → darwin36b v2 plan (FFN-only selective merge + LoRA
  KD)
status: on_track
target: 93.9%
unit: '%'
---

# FromScratch — North Star

## Why this metric
GPQA Diamond is the hardest reasoning benchmark for science PhDs. The north star is matching the OR-ensemble (93.9%) with a single model — proving distillation/merging can capture ensemble-level intelligence. This is the technical moat and content foundation for MOAT.

## Current Plan (v2)
- **Base**: darwin36b (84.3% GPQA, standard attn)
- **Phase 2**: FFN-only DARE-TIES merge from jackrong35b (gate/up/down_proj only)
- **Phase 3 v3**: LoRA KD r=32, α=64, 800 steps, lr=5e-5
- **Phase 4**: Chemistry/Biology domain injection (12 failure questions)
- **Phase 5**: GRPO on-policy if needed

## Why v1 failed
Simple 50/50 average of FLA+std-attn architectures → 30% (arch incompatible). Pivoted to jackrong35b direct → 45% (FLA O(T²) eval bottleneck + format collapse). v2 uses darwin36b as base to avoid both issues.

## Links
- Planner: /home/desk-1/Project/FromScratch/docs/north-star-dashboard.html
- Plan v2: /home/desk-1/Project/FromScratch/docs/20260506-distillation-plan-v2.md