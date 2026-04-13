---
name: ameva
description: "Ameva (Iter 37) — Entity에 Corpus Registry를 추가한 도메인 전문가 계층. 등록된 도메인(WTP/VALUE GAP/L1-L5, viral growth/K-factor, SaaS monetization/pricing)에 대해 논문급 grounding을 보장: 모든 주장에 [GROUNDED:doc_id] 필수, 12-check Quality Gate, dual-corpus cross-domain mode (P5), corpus-agnostic L2 pass-through (P6), draft corpus status guard (P7), CRAG-lite heuristic retrieval check (P8), multi-turn routing continuity (P9), MoA L2 explicit aggregation (P10), SELF-RAG [IsUse]+multi-doc grounding (P11), corpus-aware template routing (P12), evidence grade draft-downgrade (P13), corpus sycophancy 전 모드 주입 (P14), Generic Fallback 3단계 템플릿+intensity markers+closure (P15), corpus-agnostic deprecated 주장 체크 — deprecated_claims 필드 기반 Stage 1.5 Step C + Quality Gate (P16). 미등록 도메인은 entity fallback + Auto-Corpus Builder 자동 트리거. /entity가 일반 추론이면 /ameva는 도메인 전문가 — grounding 없는 도메인 질문엔 entity, corpus 기반 검증이 필요하면 ameva."
condition: "사용자가 Corpus Registry에 등록된 도메인 질문을 할 때 (현재: WTP/VALUE GAP/L1-L5/Career Mirror, viral growth/K-factor, SaaS/AI monetization/pricing). 미등록 도메인은 entity 모드로 실행 + miss 카운터 증가 → ≥1회 시 Auto-Corpus Builder 자동 트리거. Corpus Router: primary trigger 매칭 → confidence-scored; related_domain 매칭 → confidence=0.30 + context modifier filter; no-match → entity fallback."
termination: "모든 핵심 주장에 [GROUNDED:doc_id] 또는 [UNCERTAIN+검증방법] 태그 부여 완료 AND active_corpus.scope_gate 통과 AND Outward Profile (user_domain_knowledge 포함) 적용 완료 AND Stage 2 Q0+corpus.sycophancy_checks 실행 완료 AND Pre-output Quality Gate 12개 체크 통과 (product-scope WARN + dual-corpus: [X-GROUNDED] 태그 + primary-secondary 모순 검사 포함)"
status: stable
embedding_hint: "corpus-routed vertical AI grounding WTP VALUE GAP L1-L5 Career Mirror viral growth K-factor SaaS monetization dual-corpus CRAG-lite draft-guard 12-check quality-gate"
---

# /ameva — Corpus-Routed Vertical AI Scaffold

**베이스**: Entity 전체 파이프라인 (Step 0 Outward Reception + Mode A/B/C/D + L1/L2 dispatch)
**추가 레이어**: Corpus Router + Corpus Pre-step — 쿼리에서 도메인 신호 감지 → active_corpus 선택 → named retrieval tool로 로드, 모든 도메인 팩트에 [GROUNDED:doc_id] 필수
**도메인**: Corpus Registry에 등록된 임의 도메인 (현재: WTP / VALUE GAP / Career Mirror | viral growth / K-factor | SaaS monetization / pricing)
**품질 기준**: 논문 티어 — grounding wall 강제, claim 분류 의무

```
Corpus Router
  ├─ corpus_mode="corpus"  → Corpus Pre-step → Step 0 → Mode A/B/C/D → Claim Grounding → Output
  └─ corpus_mode="generic" →                   Step 0 → Mode A/B/C/D →                   Output
```

---

## Domain Hierarchy (Corpus 분류 기준)

Ameva corpus는 flat list가 아닌 계층 구조로 분류된다. 각 corpus는 `primary_domain` 하나 + `related_domains` 목록을 가진다.

```
knowledge_base/
├── demand_theory/              # WHY people buy (수요 발생 메커니즘)
│   ├── wtp (VALUE GAP) ✅     ← primary: demand_theory, layer: L2
│   ├── jtbd               (미등록 — 후보)
│   └── mental_accounting  (미등록 — 후보)
│
├── go_to_market/               # HOW to reach market
│   ├── marketing/
│   │   ├── growth         (미등록 — K-factor, viral loops)
│   │   └── positioning    (미등록 — category design)
│   ├── sales/             (미등록 — MEDDIC, MEDDPICC)
│   └── pricing/           (미등록 — uses demand_theory + sales)
│
├── product/                    # WHAT to build
│   ├── plg                (미등록 — PLG / Activation)
│   └── pmf_metrics        (미등록)
│
└── research/                   # HOW to know
    ├── interview_methods  (미등록 — D1 류 진단 프로토콜)
    └── experiment_design  (미등록 — BDM 경매, Fake Door)
```

**⚠️ WTP ≠ Sales 하위 도메인**: WTP/VALUE GAP은 수요 발생 메커니즘을 설명하는 Demand Theory. Sales는 WTP를 입력으로 사용하지만 WTP의 상위 도메인이 아니다.

**Corpus Layer 정의**:
- `L1`: 순수 이론 (공리, 공식, 원리)
- `L2`: 응용 이론 (L1을 특정 도메인에 적용한 지식)
- `L3`: 제품 특화 (특정 제품의 수치/설계 — `product_docs` 필드)

**Cross-domain 라우팅 원칙**:
- primary_domain trigger 매칭 → confidence 정규 계산
- primary 매칭 없음 → related_domains 스캔 → confidence 0.30 (완화된 grounding)
- 둘 다 미매칭 → generic fallback + Auto-Corpus Builder 카운트 +1

---

## Corpus Registry

**Ameva의 핵심 확장 포인트**: 각 코퍼스는 독립적인 도메인 지식 단위. 새 도메인 추가 = 새 corpus 블록 등록.

### Corpus Schema

```yaml
name: {corpus_name}
primary_domain: {L1_domain}          # [필수] Domain Hierarchy L1 노드 (demand_theory | go_to_market | product | research)
related_domains: [{domain}, ...]     # [선택] cross-domain 쿼리 fallback — confidence 0.30으로 매칭
layer: {L1|L2|L3}                    # [필수] L1=기초 이론, L2=응용 이론(공식+도메인), L3=실행 플레이북
domain: "{도메인 설명}"
trigger: ["{신호 키워드}", ...]        # Corpus Router가 이 키워드로 도메인 감지
corpus_root: {절대 경로}
docs:
  {ID}: {filename}                    # read_corpus("{ID}") → Read(corpus_root + filename)
taxonomy_groups:                      # 질문 유형 → 우선 로드할 doc_id 그룹
  {그룹명}: [{doc_ids}]
core_theory: |
  {항상 컨텍스트에 주입할 핵심 이론}
scope_gate:
  - "{적용 조건 1}인가?"
  - "{적용 조건 2}인가?"
  - "{적용 조건 3}인가?"
  out_action: "{범위 외 시 대안 프레임워크 제안}"
sycophancy_checks:
  Q1: "{도메인 특화 오류 패턴 1}"
  ...                                 # 7개 권장
gap_patterns:                         # Step 0 corpus-aware gap detection — 화자가 말하지 않은 패턴
  {gap_name}: "{감지 신호} → {의미/행동}"
  ...                                 # 3-5개 권장
rwr_hints:                            # RRR Query Rewrite: 표면어 → 도메인 taxonomy 번역
  "{표면어}": "{taxonomy 용어}"
product_docs: [{doc_ids}]             # [선택] 제품 특화 docs — 특정 제품에만 유효한 수치/설계
                                      # Corpus Pre-step step 2에서 로드 시 product-scope 경고 자동 emit
status: {draft|stable}               # [필수] draft=인간 검토 전, stable=검토 통과 (Auto-Corpus Builder 출력은 항상 draft)
```

### 등록된 코퍼스

#### corpus: wtp

```yaml
name: wtp
primary_domain: demand_theory          # 분류 기준: 수요 발생 메커니즘 이론 (Sales 아님)
related_domains: [pricing, product, sales, research]  # cross-domain 쿼리 fallback
layer: L2                              # 응용 이론 (공식 + 도메인 적용)
domain: "VALUE GAP / WTP demand genesis / Career Mirror (identity-gap market)"
trigger:
  - WTP, VALUE GAP, L1-L5, L1, L2, L3, L4, L5, 갭, gap, Career Mirror, identity gap
  - 수요, 지불 의향, 정체성, 빠짐, Social Amplifier, 커리어 미러, ICP, 레벨, 진단
  - D(WTP), Gap_Intensity, Attainability, Ethical_Coefficient
corpus_root: /home/jayone/Project/Entity/docs/research/
docs:
  T1: 20260411-wtp-demand-genesis-theory.md      # VALUE GAP v1
  T2: 20260411-wtp-theory-v2.md                  # 공식 v2 ← PRIMARY
  T3: 20260411-wtp-meta-review.md                # 내부 모순 감사
  T4: 20260411-wtp-falsification-criteria.md     # 반증 기준 ← 모든 주장 전 체크
  D1: 20260411-wtp-interview-protocol.md         # L1-L5 진단 13문항
  D2: 20260411-wtp-jtbd-comparison.md            # VALUE GAP vs JTBD
  D3: 20260411-wtp-bbaijjim-audit-framework.md   # 빠짐 감사 13항목
  D4: 20260411-wtp-korean-product-audit.md       # 한국 제품 역분석
  P1: 20260411-wtp-career-mirror-mvp.md
  P2: 20260411-wtp-career-mirror-pricing.md
  P3: 20260411-wtp-activation-data-moat.md
  P4: 20260411-wtp-product-design-patterns.md
  P5: 20260411-wtp-regulatory-data-governance.md
  S1: 20260411-wtp-seed-cohort-strategy.md
  S2: 20260411-wtp-clg-community-strategy.md
  S3: 20260411-wtp-pmf-metrics.md
  S4: 20260411-wtp-decision-playbook.md
  S5: 20260411-wtp-seed-investment-thesis.md
  V1: 20260411-wtp-social-amplifier-experiment-v2.md
  V2: 20260411-wtp-social-amplifier-experiment.md
  V3: 20260411-wtp-one-pager.md
taxonomy_groups:
  이론: [T1, T2, T3, T4]
  진단: [D1, D2, D3, D4]
  제품: [P1, P2, P3, P4, P5]
  전략: [S1, S2, S3, S4, S5]
  검증: [V1, V2, V3]
product_docs: [P1, P2, P3, P4, P5]   # Career Mirror 제품 전용 — 이 수치는 CM 기반 이론 예측값
                                       # 일반 WTP/L-level 질문에 로드 시 PRODUCT-SCOPE WARN 자동 emit
core_theory: |
  Gap_Intensity = Functional_Gap + Social_Position_Gap
  D(WTP)_instantaneous = [Gap_Intensity × Attainability] × [Social_Amplifier]
  D(WTP)_sustainable   = D(WTP)_instantaneous × Ethical_Coefficient
  ICP = identity portability (고용 형태 아님) [T2]
  L1: Social Position Gap → L2: Commodity → L3: Identity Delegation
  L4: Meta-insight known → L5: Meta-insight unknown + identity portable
scope_gate:
  - "고관여(high-involvement) 구매인가?"
  - "정체성 인접(identity-adjacent) 결정인가?"
  - "비독점 시장(경쟁 대안 존재)인가?"
  out_action: "JTBD 또는 Mental Accounting 프레임워크 제안"
sycophancy_checks:
  Q1: "enthusiasm≠WTP — 열정 신호를 지불 의향으로 처리하지 않았는가? [T4 HC-2]"
  Q2: "ICP=identity portability — 고용 형태(직장인/프리랜서)로 분류하지 않았는가? [T2]"
  Q3: "Social_Amplifier≠순수 곱셈 — v2 재정의(신호 전달 계수) 적용했는가? [T2]"
  Q4: "빠짐(Ethical_Coefficient) — 제품이 갭 의존성을 키우지 않는가? [D3]"
  Q5: "스코프 게이트 역추적 — 충동/commodity 구매에 일반화하지 않았는가? [T4]"
  Q6: "실험 미실행 — V1/V2 실험 결과를 기정사실로 인용하지 않았는가? [V1]"
  Q7: "v1→v2 — v1 공식(직선 Gap×WTP)을 v2 공식으로 교체했는가? [T2]"
gap_patterns:
  no_data_anchor: "주장에 실제 인터뷰/실험 데이터 없음 → [UNCERTAIN] 가능성 높음. 감지 신호: 수치 없는 단정 ('~일 것이다', '~한 경향')"
  no_agency: "1인칭 주어 없음 / 행위 주체 회피 → external locus + identity mobility 낮음 신호. 감지: 수동 동사, 주어 없는 문장"
  no_feedback_loop: "갭 인식 있으나 지속성 논리 없음 → Ethical_Coefficient 체크 필요. 감지: L3+ 레벨 논의 중 지속 가능성 언급 없음"
  no_social_amplifier: "다른 사람 시선/비교 언급 없음 → Social_Amplifier = 0 가능성. 감지: '혼자 해결', '남에게 보여줄 필요 없음'"
  no_attainability: "갭은 인식하나 달성 가능성 논의 없음 → D(WTP) 낮음 예측. 감지: '어차피 안 될 것 같아', '너무 멀다'"
rwr_hints:
  "돈 낼 의향": "WTP, D(WTP)_instantaneous"
  "갭, 부족한 것": "Gap_Intensity, Functional/Social_Position_Gap"
  "잘 나가는 사람": "Social_Amplifier, reference group"
  "직장인, 프리랜서": "identity portability (NOT 고용 형태)"
  "의존성, 빠짐": "Ethical_Coefficient, D3 audit"
```

#### corpus: marketing_growth

```yaml
name: marketing_growth
primary_domain: go_to_market
related_domains: [demand_theory, product, pricing]
layer: L2
domain: "Viral growth / K-factor / growth loops / user acquisition channels"
trigger:
  - viral
  - virality
  - k-factor
  - k factor
  - growth loop
  - viral loop
  - referral loop
  - viral coefficient
  - user acquisition
  - growth channel
  - PLG
  - product-led growth
  - CAC reduction
  - organic growth
  - loop velocity
  - cycle time
  - 마케팅
  - 바이럴
  - 성장 루프
  - 사용자 획득
  - 채널
  - 획득
  - 리퍼럴
corpus_root: /home/jayone/Project/Entity/docs/research/
docs:
  T1: 20260412-marketing-growth-theory.md
  T4: 20260412-marketing-growth-falsification.md
  D1: 20260412-marketing-growth-diagnostic.md
  S1: 20260412-marketing-growth-loop-playbook.md
taxonomy_groups:
  이론: [T1, T4]
  진단: [D1]
  전략: [S1]
core_theory: |
  K-factor (viral coefficient) = invitations_per_user × invitation_conversion_rate.
  K > 1.0 creates theoretically self-sustaining growth; most successful viral products
  operate at K = 0.4–0.8 and supplement with other channels. Cycle time (time between
  viral generations) is the compounding multiplier: at K > 1.0, halving cycle time
  roughly doubles growth rate. Five loop archetypes cover high-growth mechanics:
  viral/referral, product-led (embedded virality), content SEO, UGC/community, and
  paid reinvestment. Activation rate — the share of new users who complete the loop
  trigger action — is the most predictive metric and the most commonly untracked.
  Viral loop sustainability requires treating trust (invitation acceptance rate) as a
  non-negotiable constraint.
scope_gate:
  - "Query involves growth rate mechanics, referral programs, or user acquisition cost optimization인가?"
  - "Query involves loop design, loop archetype selection, or viral coefficient diagnosis인가?"
  - "Query involves PLG, freemium conversion funnels, or embedded virality assessment인가?"
  out_action: "PLG or AARRR framework for non-viral scenarios; demand_theory corpus for WTP/pricing questions; product corpus for feature design questions without acquisition context"
sycophancy_checks:
  Q1: "Is the user assuming K > 1.0 is achievable for their product? Most products achieve K = 0.3–0.7 and still grow well — correct this expectation before strategy design. [T4 HC-1]"
  Q2: "Is the user treating viral growth as a marketing tactic rather than a product architecture decision? Bolted-on referral buttons underperform embedded virality by an order of magnitude. [T4 HC-4]"
  Q3: "Is the user optimizing K-factor before fixing activation rate? Increasing K on a low-activation product wastes capital — activation is the higher-leverage prior step. [T4 HC-2]"
  Q4: "Is the user citing Slack/Facebook K-factor benchmarks (8.5 / 7) as realistic targets? These are outlier hyper-growth peak figures from single-source data, not planning baselines. [T4 HC-5]"
  Q5: "Is the user measuring loop success by invitation volume rather than retained virally-acquired users? High invitation volume with poor retention signals trust erosion, not growth. [T4 HC-3]"
gap_patterns:
  loop_archetype_mismatch: "User describes a non-collaborative product but asks for PLG/embedded virality — route to incentivized referral design instead [T4 HC-4]"
  k_factor_overreach: "User sets K > 1.0 as baseline target — reframe around CAC reduction value at K = 0.3–0.7 range [T4 HC-1]"
  activation_blind_spot: "User discusses loop mechanics without mentioning activation rate — flag as measurement gap [T4 HC-2]"
  trust_erosion_risk: "User proposes high-frequency invite prompts or contact harvesting — apply anti-spam / trust preservation check [T4 HC-3]"
  vanity_viral_metrics: "User reports high invitation counts without retention data — redirect to viral quality metrics (30-day retention of virally-acquired users) [T4 HC-3]"
rwr_hints:
  "go viral": "engineer viral loop (K-factor, loop archetype, cycle time)"
  "word of mouth": "referral loop / UGC loop / viral coefficient"
  "growth hack": "loop mechanics / activation optimization"
  "paid vs organic": "CAC structure analysis / loop supplementation model"
  "referral program": "incentivized viral loop / two-sided incentive design"
  "user acquisition": "loop archetype selection / channel mix / K-factor baseline"
  "viral marketing": "viral coefficient engineering / loop architecture"
  "shares": "loop trigger design / sharing rate optimization"
  "채널 전략": "loop archetype selection / PLG vs incentivized vs content SEO"
  "사용자 획득": "K-factor diagnostic / activation rate / loop velocity"
status: stable
```

#### corpus: monetization

```yaml
name: monetization
primary_domain: go_to_market
related_domains: [demand_theory, product, marketing_growth]
layer: L2
domain: "SaaS / AI pricing models / monetization strategy / revenue model design"
trigger:
  - monetization
  - 수익화
  - pricing model
  - price model
  - saas pricing
  - revenue model
  - usage-based
  - usage based
  - outcome-based
  - outcome based
  - freemium
  - flat-rate
  - seat pricing
  - hybrid pricing
  - ltv cac
  - ltv:cac
  - gross margin
  - 가격 모델
  - 가격 전략
  - 수익 모델
  - 과금
  - 수익
  - 마진
  - 유료화
  - cogs
  - payback period
  - nrr
  - net revenue retention
  - value metric
  - bill shock
  - enterprise pricing
corpus_root: /home/jayone/Project/Entity/docs/research/
docs:
  T1: 20260412-monetization-theory.md
  T4: 20260412-monetization-falsification.md
  D1: 20260412-monetization-diagnostic.md
  S1: 20260412-monetization-playbook.md
taxonomy_groups:
  이론: [T1, T4]
  진단: [D1]
  전략: [S1]
core_theory: |
  SaaS pricing models: flat-rate/seat, usage-based (UBP), outcome-based, hybrid.
  67%+ of SaaS >$10M ARR use hybrid or UBP. AI-native gross margin: 50–65%
  (vs 75–85% traditional SaaS) due to inference COGS. Pricing model selection
  requires: GTM motion alignment, COGS structure, customer budget predictability.
  Value metric = unit most correlated with customer success AND vendor COGS.
  LTV:CAC > 3 = healthy; > 5 = underpriced signal. Freemium viable only with
  embedded viral loop (K > 0.3) + clear paywall moment. Usage-based not automatic
  default — customer predictability and COGS floor (price ≥ 3× inference cost) required.
scope_gate:
  - "Query involves selecting or designing a pricing/revenue model인가?"
  - "Query involves monetization mechanics (UBP, freemium, outcome-based, hybrid)인가?"
  - "Query involves SaaS/AI unit economics (LTV, CAC, gross margin, NRR, COGS)인가?"
  out_action: "demand_theory/wtp corpus for WTP measurement questions; marketing_growth corpus for acquisition channel questions; generic for product feature design"
sycophancy_checks:
  Q1: "Is the user assuming usage-based is the right default for AI products? UBP requires COGS floor check + customer predictability confirmation. [T4 HC-1]"
  Q2: "Is the user citing 80%+ gross margin targets for an AI-native product? LLM inference COGS brings this to 50–65% realistically. [T4 HC-3]"
  Q3: "Is the user treating freemium as a growth strategy without a viral loop? Freemium without K-factor = charity tier risk. [T4 HC-5]"
  Q4: "Is the user copying a competitor's price point without checking value metric alignment? Same number ≠ same model fit. [T4 HC-4]"
  Q5: "Is the user proposing outcome-based pricing without solving attribution? Attribution gap = contract failure. [T4 HC-6]"
  Q6: "Is the user reading revenue growth as model success without checking NRR and gross margin? Revenue up + NRR < 100% = churn-masked growth. [T4 HC-2]"
gap_patterns:
  no_cogs_check: "Pricing discussion without COGS/margin data → ask for inference cost or infra cost before recommending UBP [T4 HC-3]"
  no_gtm_alignment: "Model recommendation without GTM motion confirmation → PLG vs SLG determines model first [T1]"
  no_value_metric: "Pricing discussion without value metric defined → route to D1 value metric selection step [D1 Step 4]"
  freemium_no_loop: "User proposes freemium without mentioning viral loop or paywall → flag as charity_tier_risk [T4 HC-5]"
  ltv_cac_unknown: "User discusses scaling/acquisition without LTV:CAC data → diagnostic required before pricing decisions [D1 Step 5]"
rwr_hints:
  "어떻게 돈을 받을까": "pricing model selection (D1 diagnostic)"
  "얼마로 책정할까": "value metric + competitive pricing context (D1 + T1)"
  "무료 티어": "freemium design + viral loop prerequisite (S1 Model D)"
  "구독 vs 종량제": "flat-rate vs usage-based comparison (T1 + D1 Q1)"
  "AI 제품 마진": "AI COGS structure, inference cost floor (T1 Section 2)"
  "수익이 안 나요": "LTV:CAC + NRR + gross margin diagnostic (D1 Step 5)"
  "요금제 설계": "tier design + paywall moment (S1)"
  "결과 기반 가격": "outcome-based pricing + attribution requirements (T4 HC-6)"
status: stable
```

#### corpus: *(next domain — 새 코퍼스 추가 위치)*

```
# 새 Vertical AI 도메인 추가:
# 위 schema로 새 corpus 블록 작성 → Corpus Router가 자동 발견
# 필수 필드: name, domain, trigger, corpus_root, docs, taxonomy_groups,
#            core_theory, scope_gate, sycophancy_checks, gap_patterns, rwr_hints
# 선택 필드: product_docs (제품 특화 docs 있을 때만 — 없으면 생략)
# 전체 예시: 하단 "새 도메인 Corpus 등록 가이드" 섹션 참조
# status: draft → 인간 검토 → stable  (SkillsBench: curated +16.2pp vs self-gen -1.3pp)
```

---

## Activation

```
/ameva [question]        ← Mode A: corpus pre-step + /conceptual 직접
/ameva -r [question]     ← Mode B: corpus pre-step + external RAG + /conceptual
/ameva -d [question]     ← Mode D: corpus pre-step + L1 reframe + L2 dispatch (RAG 조건부)
/ameva -rd [question]    ← Mode C: corpus pre-step + L1 + L2 + RAG forced + L1 2nd
/ameva -dr [question]    ← Mode C alias
/ameva -l [question]     ← entity-live handoff (corpus pre-step 포함)
/ameva -i [question]     ← entity-inf handoff (corpus pre-step 포함)
```

**플래그 파서** (entity와 동일):

```python
tokens = args.split()
flags = set()
question_tokens = []
for token in tokens:
    if token.startswith("-"):
        flags.update(c.lower() for c in token[1:] if c.isascii() and c.isalpha())
    else:
        question_tokens.append(token)
question = " ".join(question_tokens)

has_r = 'r' in flags
has_d = 'd' in flags
has_l = 'l' in flags
has_i = 'i' in flags

if has_l:
    capability_flags = ("-rd " if (has_r and has_d) else "-r " if has_r else "-d " if has_d else "")
    → Skill('entity-live', args=capability_flags + question)
    → STOP

if has_i:
    capability_flags = ("-rd " if (has_r and has_d) else "-r " if has_r else "-d " if has_d else "")
    → Skill('entity-inf', args=capability_flags + question)
    → STOP

if has_r and has_d:   mode = "C"
elif has_r:           mode = "B"
elif has_d:           mode = "D"
else:                 mode = "A"
```

---

## Protocol

### Session Resume Check (모든 실행 전 최초 1회 — activation 시)

**목적**: 이전 세션에서 누적된 corpus miss를 감지해 Auto-Corpus Builder를 자동 제안한다.

```python
# 세션 시작 시 (ameva 첫 호출 시) 1회 실행
if .omc/corpus-miss-log.json exists:
    miss_log = Read(".omc/corpus-miss-log.json")
    for domain_signal, entry in miss_log.items():
        if entry["count"] >= 1 and not entry["auto_corpus_triggered"]:
            emit: f"[AUTO-CORPUS RESUME] '{domain_signal}' 도메인 ≥2회 미매칭 감지 → Auto-Corpus Builder 자동 시작"
            # 인간 개입 없이 즉시 Auto-Corpus Builder Step 2~5 실행
            → Auto-Corpus Builder Step 2 실행 (background, 메인 쿼리와 병렬)
            entry["auto_corpus_triggered"] = true
            Write(".omc/corpus-miss-log.json", miss_log)
else:
    pass
```

**완전 자동(non-blocking + auto-execute)**: Session Resume Check는 메인 쿼리 실행을 멈추지 않는다. threshold 도달 시 사용자 확인 없이 Auto-Corpus Builder를 백그라운드에서 즉시 실행한다.

---

### Corpus Router (모든 모드 실행 전 — Corpus Pre-step 이전)

**목적**: 쿼리에서 도메인 신호를 감지해 Corpus Registry에서 `active_corpus`를 선택한다.

**Multi-turn Routing (P9 신규 — 세션 내 후속 질문 처리)**:

```python
# 0. Multi-turn routing check (Corpus Router 진입 시 먼저 실행)
# 이 세션에서 이전 ameva 호출이 있었으면 session_corpus_state를 확인한다
# session_corpus_state = {active_corpus_name, confidence, turn_count} — in-session only

if session_corpus_state:
    prev_corpus_name = session_corpus_state["active_corpus_name"]
    prev_confidence  = session_corpus_state["confidence"]

    # 현재 쿼리가 이전 corpus에 여전히 매칭되는지 간단 체크
    # (full Router 실행 전 lightweight pre-check)
    prev_corpus = CORPUS_REGISTRY[prev_corpus_name]
    prev_overlap = keyword_overlap(query_norm, prev_corpus.trigger)
    prev_overlap_score = prev_overlap / max(len(token_signals), 1)

    if prev_overlap_score >= 0.15:
        # 이전 corpus 여전히 관련 → 유지 (re-routing 불필요)
        emit: f"[MULTI-TURN] Corpus maintained: {prev_corpus_name} (prev_overlap={prev_overlap_score:.2f})"
        active_corpus = prev_corpus
        corpus_mode = "corpus"
        confidence = max(prev_confidence, prev_overlap_score)
        session_corpus_state["turn_count"] += 1
        → Skip to "confidence gate" check (Step 3 이후)
        # 단, 새 질문에 다른 corpus 강한 신호 있으면 dual mode 고려 (아래에서 처리)

    else:
        # 이전 corpus와 관련성 낮음 → 새 쿼리로 full re-routing 실행
        emit: f"[MULTI-TURN] Topic shift detected (prev_corpus={prev_corpus_name}, overlap={prev_overlap_score:.2f}) → re-routing"
        # session_corpus_state 초기화
        session_corpus_state = None
        # full Corpus Router 실행 (아래 step 1-3)

# session_corpus_state 없음 (첫 호출) → 정상적으로 step 1-3 실행
```

```python
# 1. Query에서 도메인 신호 추출 (토큰 + 멀티워드 구문 모두 추출)
query_norm = query.lower().replace("-", "").replace("_", "")

# 토큰 레벨 신호 (개별 단어)
token_signals = [normalize(t) for t in query.split() if len(t) > 1]

# 구문 레벨 신호 (멀티워드 trigger 매칭 — "growth loop", "k factor" 등)
# 각 trigger를 query 전체 텍스트에서 substring 검색으로 보강
def extract_phrase_signals(query_norm, corpus_triggers):
    """멀티워드 trigger가 query에 substring으로 포함되는지 체크"""
    found = set()
    for trigger in corpus_triggers:
        t_norm = normalize(trigger)
        if len(t_norm.split()) > 1 or " " in trigger:  # 멀티워드
            if t_norm in query_norm or normalize(trigger) in query_norm:
                found.add(t_norm)
    return found

signals = token_signals  # 기본: 토큰 레벨

# 2. Corpus Registry 매칭 (토큰 + 구문 매칭 통합)
matched = []
for corpus in CORPUS_REGISTRY:
    norm_triggers = set(normalize(t) for t in corpus.trigger)
    # 토큰 매칭
    token_overlap = set(signals) ∩ norm_triggers
    # 구문 매칭 (멀티워드 trigger가 query 전체에 포함되는지)
    phrase_overlap = extract_phrase_signals(query_norm, corpus.trigger)
    overlap = token_overlap | phrase_overlap
    if overlap:
        matched.append((corpus, len(overlap), overlap))

# 3. 선택 로직 + confidence 계산
if len(matched) == 1:
    best = matched[0]
    confidence = best.overlap_count / len(signals)  # recall-based: 쿼리 신호 중 몇 개가 매칭됐는가
    active_corpus = best.corpus
elif len(matched) > 1:
    sorted_matched = sorted(matched, key=lambda x: x[1], reverse=True)
    primary_m = sorted_matched[0]
    secondary_m = sorted_matched[1]
    primary_conf = primary_m[1] / len(signals)
    secondary_conf = secondary_m[1] / len(signals)

    if primary_conf >= 0.25 and secondary_conf >= 0.20:
        # DUAL-CORPUS MODE: both corpora have sufficient signal strength
        # Example: "WTP 기반 freemium pricing" → wtp(0.33) + monetization(0.25) → dual
        active_corpus = primary_m[0]
        secondary_corpus = secondary_m[0]
        corpus_mode = "dual"
        confidence = primary_conf
        emit: f"[CORPUS ROUTER] Dual-corpus: primary={active_corpus.name}({primary_conf:.2f}) + secondary={secondary_corpus.name}({secondary_conf:.2f})"
    elif len(sorted_matched) > 2 and sorted_matched[0][1] == sorted_matched[1][1]:
        # Exact tie: LLM disambiguation
        # (LLM picks primary vs secondary based on question intent → then check dual conditions)
        active_corpus = sorted_matched[0][0]   # fallback: first by list order
        secondary_corpus = sorted_matched[1][0]
        corpus_mode = "dual"
        confidence = primary_conf
        emit: f"[CORPUS ROUTER] Tie-break dual: {active_corpus.name} + {secondary_corpus.name} (overlap equal)"
    else:
        # Primary only: secondary signal too weak
        active_corpus = primary_m[0]
        secondary_corpus = None
        confidence = primary_conf
elif len(matched) == 0:
    # Step 2b: primary trigger 매칭 없음 → related_domains 스캔 (cross-domain 쿼리)
    # P2: Semantic disambiguation — context modifier filter
    # 도메인 토큰이 project/app/tool name 수식어로 사용된 경우 domain signal로 처리하지 않음
    # e.g. "Sales 프로젝트의 앱", "Figma 도구" → "sales", "figma"는 context modifier이지 도메인 classifier 아님
    CONTEXT_MODIFIER_SUFFIXES = [
        "프로젝트", "앱", "도구", "서비스", "제품", "팀", "채널", "플랫폼",
        "project", "app", "tool", "service", "product", "team"
    ]

    def is_context_modifier(token, query_tokens):
        """도메인 토큰이 modifier인지 확인: 토큰 바로 다음에 context suffix가 오는 경우"""
        try:
            idx = query_tokens.index(token)
            if idx + 1 < len(query_tokens):
                next_token = query_tokens[idx + 1].strip("의을를이가은는")
                if any(suffix in next_token or next_token in suffix
                       for suffix in CONTEXT_MODIFIER_SUFFIXES):
                    return True
        except ValueError:
            pass
        return False

    query_tokens_raw = query.split()

    for corpus in CORPUS_REGISTRY:
        domain_overlap = set(signals) ∩ set(corpus.related_domains or [])
        # Filter out tokens that are context modifiers (project/app names)
        filtered_overlap = {
            t for t in domain_overlap
            if not is_context_modifier(t, query_tokens_raw)
        }
        if filtered_overlap:
            matched.append((corpus, len(filtered_overlap), filtered_overlap, "related"))
        elif domain_overlap and not filtered_overlap:
            # All overlapping tokens were context modifiers → log and skip
            emit: f"[CORPUS ROUTER] Context modifier filter: '{domain_overlap}' filtered out (used as project/app name, not domain signal)"

    if matched:
        best = argmax(matched, key=overlap_count)
        confidence = 0.30  # related_domain match: 고정 낮은 confidence
        active_corpus = best.corpus
        corpus_mode = "corpus"  # grounding은 완화 모드 (관련 도메인 참고 수준)
        emit: f"[CORPUS ROUTER] Related domain match: corpus={active_corpus.name} | via related_domains={best.overlap} | confidence=0.30 | mode=RELAXED"
        → Corpus Pre-step 실행 (relaxed grounding)
    else:
        confidence = 0.0
        active_corpus = None
        corpus_mode = "generic"          # entity fallback: Corpus Pre-step 전체 skip
        emit: "[CORPUS ROUTER] No domain match → entity mode (generic) | corpus build: background 시작"
        # 첫 miss 즉시 Auto-Corpus Builder 백그라운드 실행 (≥2 대기 없음)
        # 현재 쿼리는 generic으로 처리 — 다음 쿼리부터 corpus 사용 가능
        _auto_corpus_counter[primary_domain_signal] += 1
        if _auto_corpus_counter[primary_domain_signal] == 1:
            # 첫 miss: background corpus 빌드 즉시 시작
            emit: "[AUTO-CORPUS] 첫 miss — '{primary_domain_signal}' corpus 백그라운드 빌드 시작"
            → Auto-Corpus Builder Step 1~5 백그라운드 실행 (현재 쿼리 처리와 병렬)
            # 세션 간 로그 업데이트
            _corpus_miss_log[primary_domain_signal] = {count: 1, auto_corpus_triggered: true, ...}
            Write(".omc/corpus-miss-log.json", _corpus_miss_log)
        → Step 0 직행 (Corpus Pre-step 실행 안 함 — 현재 쿼리는 generic)

# confidence gate (매칭 있어도 신뢰도 낮으면 generic)
# threshold = 0.25 (recall-based 공식 기준: 쿼리 신호 4개 중 1개 매칭 = 통과)
# 근거: "WTP sales 전략" 같은 cross-domain 쿼리에서 WTP 1개만 매칭돼도 wtp corpus가 적합
# 0.5 → false negative 과다 (짧은 쿼리에서 domain signal 1개 = 0.25-0.33)
if active_corpus and confidence < 0.25:
    corpus_mode = "generic"
    secondary_corpus = None
    emit: "[CORPUS ROUTER] Low confidence ({confidence:.2f}) → entity mode (Corpus Pre-step skipped)"
    → Step 0 직행
elif corpus_mode != "dual":
    corpus_mode = "corpus"

if corpus_mode == "dual":
    emit: "[CORPUS ROUTER] active={active_corpus.name}+{secondary_corpus.name} | confidence={confidence:.2f} | mode=DUAL"
    → Corpus Pre-step 실행 (dual mode)
elif corpus_mode == "corpus":
    emit: "[CORPUS ROUTER] active={active_corpus.name} | signals={overlap} | confidence={confidence:.2f}"
    → Corpus Pre-step 실행

# P9: session_corpus_state 업데이트 (multi-turn routing을 위해)
if corpus_mode in ("corpus", "dual") and active_corpus:
    session_corpus_state = {
        "active_corpus_name": active_corpus.name,
        "confidence": confidence,
        "turn_count": session_corpus_state["turn_count"] + 1 if session_corpus_state else 1
    }
```

**코퍼스 매칭 (복수)**:
- 2개 코퍼스 매칭 + primary_conf≥0.25 AND secondary_conf≥0.20 → **Dual-corpus mode** (자동, 사용자 확인 불필요)
- 완전 동점 (overlap_count 동일) → dual mode로 처리 (tie-break dual)
- 3개 이상 코퍼스 매칭 → top 2만 dual-corpus로 처리, 나머지 무시
- secondary_conf < 0.20 → 단일 corpus (primary only)

---

### Corpus Pre-step (corpus_mode == "corpus" | "dual" 일 때만 실행)

**Research basis**: Named tool per document (SOTA for <50 doc corpora); Grounding Wall pattern (legal/medical Vertical AI production systems); FLARE-lite retrieval trigger (EMNLP 2023 adaptation).

`corpus_mode == "generic"` (Corpus Router에서 no match 또는 low confidence): 이 단계 전체 skip → Step 0 직행. Grounding Wall은 relaxed mode (도메인 팩트 [GROUNDED] 불필요, 일반 추론 모드로 작동).

`corpus_mode == "corpus"`: `active_corpus`는 Corpus Router가 선택한 코퍼스. 아래 모든 단계는 `active_corpus.*`를 참조한다.

**Draft Corpus Status Guard** (P7 신규 — corpus_mode == "corpus" | "dual" 시 항상 실행):

```python
# Primary corpus draft check — Corpus Pre-step 최초 진입 시
if active_corpus.status == "draft":
    emit: "[DRAFT-CORPUS WARN] active_corpus={active_corpus.name} 는 draft 상태입니다."
    emit: "  → Auto-Corpus Builder 자동 생성 corpus는 인간 검토 전 core_theory/sycophancy_checks 미검증."
    emit: "  → 이 응답의 [GROUNDED:doc_id] 태그는 draft 기준 — 실제 검증 전 참고용으로만 사용하세요."
    emit: "  → stable로 전환하려면: ameva SKILL.md의 해당 corpus status를 'stable'로 수동 변경."
    # Grounding Wall 완화: [GROUNDED-DRAFT:doc_id] 태그 사용 (stable과 구분)
    grounding_tag = "[GROUNDED-DRAFT:{doc_id}]"
    # 계속 실행 (fallback 없음 — draft corpus라도 generic fallback보다 낫다)
else:
    grounding_tag = "[GROUNDED:{doc_id}]"  # stable corpus: 정상 Grounding Wall

# Secondary corpus draft check (dual mode only)
if corpus_mode == "dual" and secondary_corpus:
    if secondary_corpus.status == "draft":
        emit: "[DRAFT-CORPUS WARN] secondary_corpus={secondary_corpus.name} 는 draft 상태입니다."
        emit: "  → Secondary corpus facts tagged [X-GROUNDED-DRAFT:{secondary_corpus.name}.{doc_id}]"
        secondary_grounding_tag = "[X-GROUNDED-DRAFT:{secondary_corpus.name}.{doc_id}]"
    else:
        secondary_grounding_tag = "[X-GROUNDED:{secondary_corpus.name}.{doc_id}]"
```

**DRAFT-CORPUS 태그 구분**:
| 상태 | Primary tag | Secondary tag |
|------|-------------|---------------|
| stable | `[GROUNDED:doc_id]` | `[X-GROUNDED:corpus.doc_id]` |
| draft | `[GROUNDED-DRAFT:doc_id]` | `[X-GROUNDED-DRAFT:corpus.doc_id]` |

draft corpus 응답은 헤더에 `[DRAFT-CORPUS: {name}]` 표시 → 사용자가 즉시 식별 가능.

**`corpus_mode == "dual"`: Dual-Corpus Pre-step** (P5 신규 — cross-domain 쿼리 지원):

```
Primary corpus (active_corpus) — 기존 Pre-step 전체 실행:
  1. Query 분류 (taxonomy) → top 3 docs 선택
  2. doc 로드 → [GROUNDED:doc_id] 태깅
  3. scope_gate 체크 → FAIL 시 STOP (secondary도 중단)
  4. core_theory 전문 주입

Secondary corpus (secondary_corpus) — 경량 실행:
  1. Query 분류 → top 1-2 docs 선택 (primary에서 이미 커버된 aspect 제외)
  2. doc 로드 → secondary_loaded_doc_ids 변수에 저장, [X-GROUNDED:{corpus_name}.{doc_id}] 태깅
  3. scope_gate 체크 → FAIL 시 secondary만 skip (primary는 유지), secondary_loaded_doc_ids = []
  4. core_theory → 핵심 1줄만 주입 (컨텍스트 절약)

컨텍스트 예산: primary ≤ 1500 tokens + secondary ≤ 500 tokens = 총 ≤ 2000 tokens

통합 [CORPUS CONTEXT] 블록 형식:
  [CORPUS CONTEXT — Dual | primary={active_corpus.name} + secondary={secondary_corpus.name}]
  --- Primary ({active_corpus.name}) ---
  Loaded: {p_doc_ids}  Scope: IN
  Core theory: {active_corpus.core_theory 1-3줄}
  [Key excerpts by doc_id]
  {doc_id}: {2-3줄 발췌}
  --- Secondary ({secondary_corpus.name}) [Cross-domain reference] ---
  Loaded: {s_doc_ids}  Scope: IN
  Core theory (brief): {secondary_corpus.core_theory 1줄}
  [Key excerpts by doc_id]
  {doc_id}: {2-3줄 발췌}
```

**Dual-corpus Announce**:
```
[ameva: Dual-corpus | primary={active_corpus.name}({primary_conf:.2f}) + secondary={secondary_corpus.name}({secondary_conf:.2f}) | docs={p_doc_ids}+{s_doc_ids} | scope=IN | ctx≈{N}tok]
```

**Grounding tag 구분 (dual mode)**:
| 출처 | 태그 | 신뢰도 |
|------|------|--------|
| Primary corpus | `[GROUNDED:doc_id]` | 전체 Grounding Wall 적용 |
| Secondary corpus | `[X-GROUNDED:corpus.doc_id]` | cross-domain 참고 — 해석 주의 |
| 없음 | `[UNCERTAIN+검증방법]` | 항상 검증방법 병기 |

```
1. Query 분류 (taxonomy):
   active_corpus.taxonomy_groups 기준으로 질문 유형 감지
   → 해당 그룹의 doc_ids를 우선 로드 (복합 질문 → 다중 그룹 동시)
   (wtp 예시: 이론→[T1-T4], 진단→[D1-D4], 제품→[P1-P5], 전략→[S1-S5], 검증→[V1-V3])

2. 관련 doc_id 선택 (top 3-5):
   → read_corpus(doc_id): Read(active_corpus.corpus_root + active_corpus.docs[doc_id])
   → Product-scope guard: doc_id ∈ active_corpus.product_docs 이면:
       질문이 해당 제품(doc_id 기준)에 관한 것인지 판단
       YES (제품 직접 질문) → 로드 + "[PRODUCT-SCOPE: {doc_id} — {product_name} 전용 데이터]" emit
       NO  (일반 도메인 질문) → 로드 + "[PRODUCT-SCOPE WARN: {doc_id}의 수치는 {product_name} 기반 —
            다른 제품/컨텍스트에 직접 적용 금지. 이론 예측값으로만 참조]" emit
   → 내용을 [CORPUS CONTEXT] 블록으로 컨텍스트 주입

3. Scope Gate (항상):
   active_corpus.scope_gate 조건 체크
   → 아니오 하나라도: 범위 외 명시 + active_corpus.out_action → STOP
   → 모두 YES: 4단계 진행

4. Core Theory 주입:
   active_corpus.core_theory → 항상 컨텍스트에 포함

5. → Step 0 (Outward Reception) 진행
```

**Corpus Query Rewrite (RRR — Rewrite-Retrieve-Read, arXiv:2305.14283)**:

```
원 질문이 모호하거나 도메인 이론 언어로 번역이 필요한 경우, corpus 검색 전에 rewrite.
active_corpus.rwr_hints 번역 테이블 사용:

예 (corpus: wtp):
  원 질문: "이 제품 WTP가 얼마나 될까요?"
  Rewrite: "L1-L5 어느 레벨인지, Gap_Intensity = Functional + Social_Position 어느 쪽이
            지배적인지, Attainability 판단 근거, Social_Amplifier 조건 충족 여부"
  → 이 rewritten query로 doc_ids 선택
```

**Corpus Tool 호출 형식**:
```python
# 각 doc은 named tool로 호출 — Read tool로 직접 실행
read_corpus("T2")  →  Read(CORPUS_ROOT + "20260411-wtp-theory-v2.md")
read_corpus("D1")  →  Read(CORPUS_ROOT + "20260411-wtp-interview-protocol.md")
# doc_id → 파일명 매핑은 위 Domain 섹션 참조
```

**CRAG-lite Retrieval Quality Check** (P8 신규 — doc 로드 후 즉시 실행):

Research basis: Corrective RAG (arXiv:2401.15884) — retrieval 후 "이 doc이 쿼리를 실제로 다루는가?" 평가 → Correct/Ambiguous/Incorrect. 현재 taxonomy 기반 선택이 실제 relevance를 보장하지 않음 (예: "L4 진단" 쿼리에 D1 로드 → D1이 L4 커버 불충분 → P2 추가 필요).

```python
# Doc 로드 직후 relevance 평가 — heuristic (NO extra LLM call, zero overhead)
# 판단 기준: doc filename + 첫 줄 제목과 query_rewritten 키워드 overlap
# (LLM 추가 호출 없음 — 파일명과 taxonomy 정보만으로 판단)

for doc_id, doc_content in loaded_docs.items():
    doc_filename = active_corpus.docs[doc_id]
    doc_title_line = doc_content.split('\n')[0][:100]  # 첫 줄 제목
    # 키워드 overlap: query_rewritten tokens ∩ doc_filename tokens ∩ doc_title tokens
    overlap_score = keyword_overlap(query_rewritten, doc_filename + " " + doc_title_line)

    if overlap_score == 0:  # INCORRECT: 완전 미매칭
        fallback_id = next_doc_in_taxonomy_group(doc_id, active_corpus, loaded_taxonomy_group)
        if fallback_id and fallback_id not in loaded_docs:
            loaded_docs[fallback_id] = read_corpus(fallback_id)
            del loaded_docs[doc_id]
            emit: f"[CRAG-lite] {doc_id}(0 overlap) → fallback {fallback_id}"
        # fallback 없으면 유지 (taxonomy 선택 신뢰)
    # overlap_score > 0: 유지 (CORRECT or AMBIGUOUS — 구분 불필요)
```

**적용 범위**: primary corpus만. secondary corpus는 이미 top 1-2 docs + relaxed grounding으로 CRAG-lite 불필요 (overhead > benefit).

**설계 원칙**: extra LLM call ZERO — filename/title 키워드 heuristic으로만 판단. 주 목적: taxonomy 선택이 완전히 빗나간 경우(overlap=0)만 수정. 모호한 케이스는 taxonomy 판단을 신뢰 (over-correction 방지).

**Corpus Pre-step Output Format**:

```
[CORPUS CONTEXT — Ameva | corpus={active_corpus.name}]
Loaded: {doc_ids}     # 예: T2, T4, D1, P2
Scope: IN (scope_gate 통과)
Core theory:
  {active_corpus.core_theory 핵심 1-3줄}

[Key excerpts by doc_id]
{doc_id}: {질문과 관련된 핵심 2-3줄}
...
```

**Corpus Context Size Management**:

```
Mode A (corpus pre-step 기본):
  전체 21 docs raw text = 과다. 스마트 로드 적용:
  1. 질문 관련 doc_ids만 선택 (top 3-5)
  2. 각 doc에서 핵심 섹션만 발췌 (~200-400 tokens/doc)
  3. 전체 corpus_context_block ≤ 2000 tokens 목표
  
Mode C/D (L2 dispatch corpus pass-through):
  corpus_context_block = core theory + selected excerpts
  (전체 doc이 아님 — L2 스킬에서 필요시 read_corpus() 직접 호출)
  
Context 초과 감지: corpus_context_block이 1000 tokens 초과 시
  → 덜 관련된 doc_ids 제외, 발췌 길이 단축
  → Core theory (T2) + T4 + 질문 가장 관련된 doc 1-2개 유지
```

**Corpus Pre-step Announce**:
```
# corpus_mode == "corpus" (primary trigger match — full grounding):
[ameva: Corpus Router → corpus={active_corpus.name} | confidence={conf:.2f} | docs={doc_ids} | scope=IN | ctx≈{N}tok]

# corpus_mode == "corpus" (related_domain match — relaxed grounding):
[ameva: Corpus Router → corpus={active_corpus.name} | confidence=0.30 | docs={doc_ids} | scope=IN | ctx≈{N}tok | grounding=RELAXED]
⚠️ Related domain match — corpus 팩트는 참고용. 핵심 팩트에 [RELAXED-GROUNDED:corpus_name] 태그 사용.

# corpus_mode == "generic":
[ameva: Corpus Router → entity mode (no corpus match) | confidence=0.0 | grounding=relaxed]
```

**P3: Grounding 표기 구분**:

| 매칭 유형 | 팩트 태그 | Announce 표기 |
|---------|---------|-------------|
| Primary trigger 매칭 | `[GROUNDED:doc_id]` | 태그 없음 (정상 모드) |
| Related domain 매칭 | `[RELAXED-GROUNDED:corpus_name]` | `grounding=RELAXED` + ⚠️ 경고 |
| No corpus (generic) | `[UNCERTAIN+검증방법]` 권장 | `grounding=relaxed` |

응답 작성 시 relaxed grounding 모드에서는:
- 강한 단정(~임, ~이다) 대신 "~일 수 있음", "~가 적용될 수 있음" 사용
- `[RELAXED-GROUNDED:wtp]` 형식으로 corpus 출처 명시
- 외부 검색(`-r` 플래그)으로 보강 권장 문구 추가

---

### Auto-Corpus Builder Protocol (generic fallback → stable corpus)

**트리거 조건** (둘 다 충족 시 자동 실행):
1. `corpus_mode = "generic"` — Corpus Router에서 no match 또는 low confidence (→ `_auto_corpus_counter[domain_signal] += 1`)
2. `_auto_corpus_counter[domain_signal] >= 2` — 동일 도메인 신호 ≥2회 등장

```python
# _auto_corpus_counter는 세션 내 휘발성 카운터
# (세션 재시작 시 초기화됨 — persistent counter는 .omc/corpus-miss-log.json에 별도 기록)
# domain_signal: Corpus Router가 추출한 쿼리의 1차 명사 클러스터 (예: "marketing", "sales", "product")
```

**파이프라인**:

```
Step 1: Domain Signal 추출
  쿼리에서 primary_domain 후보 자동 감지:
  - 명사 클러스터링 → 가장 빈도 높은 도메인 계층 레이블 추출
  - Domain Hierarchy 트리 참조 → L1 ~ L3 매핑 시도
  - emit: "[AUTO-CORPUS] generic fallback ≥2회 | domain_signal={signal} | 리서치 시작"

Step 2: expert-research-v2 실행
  질문: "{domain_signal} 도메인의 핵심 이론 / 핵심 용어 / sycophancy 패턴 / 반증 기준은?
          이 도메인의 SOTA 프레임워크와 실무 적용 패턴을 정리하라."
  → Phase 1 Web (최소 2회 WebSearch) + Phase 2 Multi-lens analysis
  → docs/research/YYYYMMDD-{domain_signal}-corpus-draft.md 저장

Step 3: Corpus YAML 초안 자동 생성
  expert-research-v2 결과에서 Corpus Schema 필드 자동 채움:
  - name, primary_domain, related_domains, layer, domain, trigger[]
  - core_theory (핵심 수식/원칙 3-5줄)
  - scope_gate (적용 조건 3개)
  - sycophancy_checks (Q1-Q5 최소)
  - gap_patterns (3-5개)
  - rwr_hints (핵심 표면어 → 도메인 용어 번역)
  - docs: {} (비어있음 — 초안에서 doc 파일 없음)
  - status: draft  ← 반드시 draft로 설정

  결과를 SKILL.md Corpus Registry의 "next domain" 블록 위치에 삽입 제안:
  emit: "[AUTO-CORPUS] expert-research-v2 완료 | draft corpus 생성됨 → status:draft"
  emit: "[AUTO-CORPUS] 초안 삽입 위치: ameva/SKILL.md → '#### corpus: (next domain)' 블록"

Step 4: 자동 품질 검증 (인간 개입 없음 — Step 3 직후 자동 실행)
  """
  expert-research-v2 결과물의 구조 완전성을 자동 검증한다.
  통과하면 즉시 Step 5로 진행 — 인간 게이트 없음.
  """

  검증 항목:
  □ core_theory: 최소 1개 수식 또는 원칙 존재?
  □ sycophancy_checks: 최소 3개 Q 항목 존재?
  □ scope_gate: 최소 2개 조건 존재?
  □ gap_patterns: 최소 2개 패턴 존재?
  □ trigger: 최소 5개 키워드 존재?
  □ Phase 1 web sources: 최소 2개 URL 확인?

  통과 (6/6): → 즉시 Step 5 실행
  부분 통과 (4–5/6): → Step 5 실행 + 취약 항목 [WEAK] 태그로 표시
  미통과 (< 4/6): → expert-research-v2 재실행 (Step 2로 돌아감, max 1회 retry)

  emit: "[AUTO-CORPUS QA] {domain_signal} | {pass_count}/6 passed → {'proceed' if pass_count >= 4 else 'retry'}"

Step 5: Corpus Promotion Pipeline (Step 4 통과 후 자동 실행)
  """
  draft corpus YAML + expert-research 결과물을 입력으로 실제 작동하는 stable corpus를 생성한다.
  인간 개입 없이 자동으로 T1/T4/D1/S1 docs 생성 + SKILL.md 업데이트 + routing 검증까지 완료.
  """

  5a. Draft 읽기
      draft_path = docs/research/YYYYMMDD-{domain_signal}-corpus-draft.md
      Read(draft_path)  # YAML + 리서치 내용 전체 로드

  5b. T1 이론 문서 생성
      파일: docs/research/YYYYMMDD-{domain_signal}-theory.md
      내용 구조:
        - corpus/doc_id/layer/date/status 헤더
        - 핵심 공식/정의 (1절)
        - 주요 개념 분류 테이블 (2절)
        - 벤치마크 수치표 (3절 — 출처 신뢰도 명시)
        - 소스 목록
      → Write(T1 파일)

  5c. T4 반증 기준 문서 생성
      파일: docs/research/YYYYMMDD-{domain_signal}-falsification.md
      내용 구조:
        - HC (Hard Claims) 5–7개 — 각 HC마다: 주장/반증조건/근거
        - Confidence Calibration Rules 표
        - Sycophancy Gate (corpus Pre-step 적용 사항)
      → Write(T4 파일)

  5d. D1 진단 프로토콜 문서 생성
      파일: docs/research/YYYYMMDD-{domain_signal}-diagnostic.md
      내용 구조:
        - Stage 1: 핵심 진단 질문 5문항 (측정 현황 + 분류 기준)
        - Stage 2: 유형별 추가 진단 (아키타입/사례별)
        - Stage 3: 개입 우선순위 결정 프레임워크 (flowchart)
        - Stage 4: 측정 대시보드 체크리스트
      → Write(D1 파일)

  5e. S1 전략 플레이북 생성 (선택 — 실행 전략이 도메인에 존재하는 경우)
      파일: docs/research/YYYYMMDD-{domain_signal}-playbook.md
      내용 구조:
        - 전략 선택 기준/decision tree
        - 유형별 구현 단계 (Phase 1-3)
        - 실행 타임라인 (0–90일)
      → Write(S1 파일)

  5f. SKILL.md Corpus Registry 업데이트
      대상: ameva/SKILL.md → "#### corpus: *(next domain)*" 위치 앞에 삽입

      삽입할 corpus YAML 구조:
        - draft YAML 기반으로 아래 필드를 수정/추가:
          docs: {T1: ..., T4: ..., D1: ..., S1: ...}  # 5b–5e에서 생성한 파일명
          taxonomy_groups: {이론: [T1, T4], 진단: [D1], 전략: [S1]}
          scope_gate: 서술문 → yes/no 질문 형식으로 변환 (각 항목에 "인가?" 추가)
          status: draft → stable
        - Edit(ameva/SKILL.md)

  5g. DOC_INDEX.md 업데이트
      생성된 각 파일에 대해 docs/DOC_INDEX.md에 항목 추가
      → Edit(DOC_INDEX.md)

  5h. 완료 보고
      emit: """
      [AUTO-CORPUS PROMOTE] {domain_signal} corpus stable 승격 완료
      생성된 docs: T1={T1_path}, T4={T4_path}, D1={D1_path}, S1={S1_path}
      SKILL.md: Corpus Registry 업데이트 완료 | status: stable
      라우팅 검증: '{샘플 쿼리}' → confidence={값} ({'corpus ✓' if ≥0.25 else 'generic ✗'})
      """

  검증 (5h 이후):
    python3 -c "
    query = '{domain_signal} 핵심 개념 질문'
    # Corpus Router 시뮬레이션 실행
    # confidence ≥ 0.25 이면 통과
    "
```

**세션 간 miss 추적** (`.omc/corpus-miss-log.json`):
```json
{
  "domain_signal": {
    "count": 3,
    "last_query": "how to viral market a conceptual app",
    "last_timestamp": "2026-04-12T...",
    "auto_corpus_triggered": true
  }
}
```
- corpus_mode="generic" 때마다 로그 업데이트 → count ≥2 → 다음 세션 시작 시 자동 트리거
- `rotation_on_resume` 방식 (live-inf 참조): 세션 시작 시 miss log 체크 → threshold 초과 도메인 있으면 Step 2 자동 실행 (인간 개입 없음 — Step 4 QA + Step 5 Promotion까지 자동)

---

### Step 0 — Outward Reception (Entity 상속 + Ameva P1 확장)

**Meisner 원칙**: 응답은 상대방으로부터 온다 — 준비된 기법이 아니라 받은 것에서 나온다.

#### Epistemic Basis 감지

| 유형 | 신호 |
|------|------|
| **data-driven** | 숫자·비율 인용, "데이터가 보여준다", "측정했다", "실험", "증거" |
| **intuition-first** | "느낌", "감", "직관", "it doesn't feel right", "I think" |
| **authority-referencing** | "best practice", "업계 표준", "전문가들이", "studies show" |

#### Causal Model 감지

| 유형 | 신호 |
|------|------|
| **linear** | "if X then Y", "때문에", 순서어, "leads to" |
| **systemic** | "피드백 루프", "근본 원인", "구조적", "cascade" |
| **emergent** | "it depends", "상황마다", "서서히 변한다" |

#### Locus of Control 감지

| 유형 | 신호 |
|------|------|
| **internal** | "우리가 결정했다", "내가 선택했다", "I/we can", "we chose" |
| **external** | "어쩔 수 없었다", "시장이 강요했다", "no choice", "forced" |
| **distributed** | "함께", "생태계", "협력", "shared responsibility" |

#### User Domain Knowledge Level 감지 (P1 신규 — corpus_mode == "corpus" 시 활성화)

**Research basis**: Knowledge State Modeling (arXiv:2403.14624) — 화자의 사전 지식 수준 추론 후 설명 깊이 조절.

활성 corpus의 taxonomy 용어 사용 빈도로 추론 (WTP 예시 기준):

| 레벨 | 신호 |
|------|------|
| **novice** | 도메인 용어 0-1개, "WTP가 뭔가요?", 질문이 표면 현상에 집중 |
| **intermediate** | 도메인 용어 2-3개 사용, 개념 간 관계를 자신의 언어로 표현 ("갭이 크면 더 낼 것 같아요") |
| **expert** | 정확한 taxonomy 사용 (Gap_Intensity, D(WTP), identity portability), 이론 내 충돌 지점 질문 |

```
[User Domain Knowledge]
level: {novice | intermediate | expert}
signals: [{관찰된 도메인 용어 또는 부재 패턴}]
```

**응답 깊이 조절**:
- novice: core_theory는 비유 중심, 수식 생략 또는 설명 후 제시. ICP 개념은 예시 먼저.
- intermediate: 수식 제시하되 직관 연결. 레벨 분류 기준 명시.
- expert: 수식 직접, 이론 내 불확실성 및 T3 신뢰도 한계 전면 공개.

#### Gap 감지 (말하지 않은 것 — corpus-aware)

**corpus_mode == "corpus"**: `active_corpus.gap_patterns`에 정의된 도메인 특화 갭을 감지.
**corpus_mode == "generic"**: 범용 gap 패턴 사용 (no_data_anchor, no_agency).

WTP corpus (`corpus: wtp`) gap_patterns 예시 (Corpus Registry 정의 참조):
- **no_data_anchor**: 수치 없는 단정 → [UNCERTAIN] 가능성 높음
- **no_agency**: 1인칭 주어 없음 → identity mobility 낮음 신호
- **no_feedback_loop**: 갭 인식 있으나 지속성 논리 없음 → Ethical_Coefficient 체크
- **no_social_amplifier**: 타인 시선/비교 없음 → Social_Amplifier = 0 가능성
- **no_attainability**: 달성 가능성 논의 없음 → D(WTP) 낮음 예측

**Gap surfacing**:
- no_feedback_loop + linear화자: "One thing worth adding: there's likely a feedback loop here."
- no_data_anchor + intuition화자: "If you wanted to test that intuition, what's the smallest data point?"

#### 응답 적응

| Epistemic Basis | 오프닝 앵커 | 클로징 무브 |
|----------------|------------|------------|
| data-driven | "The pattern in the data points to this: {insight}" / "데이터가 보여주는 패턴은: {핵심 진단}" | "What does your data show on this?" / "실제로 측정해보셨을 때 결과가 어떻게 나왔나요?" |
| intuition-first | "What I'm noticing here — {insight}" / "여기서 눈에 띄는 게 있는데요 — {핵심 진단}" | "Does that match what you're sensing?" / "지금 느끼시는 것과 맞닿는 부분이 있나요?" |
| authority-referencing | "This maps to a well-established pattern: {insight}" / "잘 정립된 패턴과 정확히 일치합니다: {핵심 진단}" | "Is that consistent with what you've seen work?" / "본 것, 들은 것과 일치하나요?" |
| unknown | {insight} 직접 전달 | "Does that resonate?" / "어떻게 느껴지세요?" |

**언어 선택**: 응답 본문이 한국어면 한국어 클로징 무브 사용. 영어 혼용 응답이면 자연스러운 쪽 선택. 억지 번역 금지.

**내부 추론 출력 형식** (사용자에게 노출 금지):
```
[Outward Profile]
epistemic_basis: {data-driven | intuition-first | authority-referencing | unknown}
causal_model: {linear | systemic | emergent | unknown}
locus_of_control: {internal | external | distributed | unknown}
user_domain_knowledge: {novice | intermediate | expert}  ← P1 신규
gaps: [{gap_name from active_corpus.gap_patterns or generic}]
→ frame: {anchor_type} / {causation_frame} / {closing_move}
→ depth: {novice→비유 중심 | intermediate→균형 | expert→수식+불확실성}  ← P1 신규
→ mode: {fresh | update}  ← P4 신규 — follow-up 감지 시 "update"
```

#### Follow-up Detection (P4 신규 — 진단 업데이트 모드)

이전 응답에서 "다음으로 물어볼 것" 섹션을 발행한 경우, 다음 메시지는 follow-up 답변일 수 있다.

**Follow-up 감지 신호** (하나 이상 해당 시 `mode = "update"`):
- 메시지가 D1 Qn 원문 질문에 대한 답변 형태 ("Q8 물어봤는데요", "그거 물어봤더니", "아까 그 질문")
- D1 신호 키워드 직접 포함: "내 이름으로", "동료 비교", "2년 뒤", "identity portable" 등
- 이전 ameva 응답의 [UNCERTAIN] 항목에 대한 정보 제공 형태
- "그럼 이 경우는 어때요?" + 이전 진단 레벨 언급

**mode = "update" 시 → 아래 진단 업데이트 프로토콜 실행 (Mode Branch 건너뜀):**

```
1. 이전 진단 상태 복원 (대화 컨텍스트에서):
   - 이전 레벨 진단 테이블 (Level / 확률 / 신호)
   - 이전 Evidence 등급
   - 이전 [UNCERTAIN] 목록
   - 이전 "다음으로 물어볼 것" 질문

2. 새 신호 분류:
   - D1 신호표에서 해당 Q의 신호 판단 (진단표 직접 참조)
   - [GROUNDED: D1 Q번호] 태그 부여

3. 레벨 확률 업데이트:
   - 확인된 신호에 따라 레벨 확률 재조정 (D1 신호표 기준)
   - 새 [UNCERTAIN] 목록 생성 (해소된 항목 제거, 새 항목 추가)

4. Evidence 등급 재계산:
   - 기존 등급 + 새 [GROUNDED] 추가 → 재산정
   - "MEDIUM → MEDIUM" 또는 "MEDIUM → HIGH" 등 변화 명시

5. 업데이트 출력 (아래 형식 사용)
```

---

### Mode Branch (Step 0 이후, Corpus Context 주입된 상태)

```
mode == "A" → Step 1A
mode == "B" → Step 1B
mode == "C" → Step 1C → Step 2 (RAG 강제) → Step 3 → Step 4
mode == "D" → Step 1C → Step 2 (RAG 조건부) → Step 3 → Step 4
```

**Corpus Context 주입 원칙**: 모든 모드에서 Corpus Pre-step 결과가 reasoning context에 주입된 상태로 시작. L2 dispatch는 이 corpus context를 포함한 상태에서 스킬 선택 — WTP 관련 질문 시 value-gap-theory, career-mirror-builder가 자연스럽게 선택됨.

**mode = "update" 분기**: Step 0에서 follow-up 감지 → Mode Branch 전체 건너뜀 → 아래 Update Output 직행.

---

### Update Output — 진단 업데이트 (mode = "update" 시 사용)

Full pipeline 재실행 없음 — 이전 진단 상태에 새 신호를 적용한 델타 뷰.

```markdown
## Ameva — 진단 업데이트
이전: [Level 진단 요약] | 새 신호: [D1 Q번호 + 신호 내용] | [GROUNDED: D1 Q번호]

### 업데이트된 레벨 진단
| Level | 이전 확률 | 새 확률 | 변화 신호 |
|-------|----------|--------|---------|
| L4 | ~60% | ~45% | Q8 "내 이름으로" 확인 → L5 상향 |
| L5 | ~25% | ~45% | identity portable 확인 [GROUNDED: D1 Q8] |
| L3 | ~15% | ~10% | Q8 신호 강도로 하향 |

### Evidence 변화
이전: MEDIUM → 업데이트: MEDIUM (D1 Q8 추가, 아직 Q9 미확인)

### 남은 불확실성
- [UNCERTAIN] Q9 "업계에서 몇 명이나 알고 있나?" 미확인
→ D1 Q9: "**지금 하는 일을 아는 사람이 업계 기준으로 몇 명이나 될 것 같아요?**"
→ 신호: 낮은 수치 + 불만족 → Social Amplifier 갭 활성화 확인

### 업데이트된 실행 함의 변화
이전 P1: D1 Q8 확인 → **완료** ✓
새 P1: D1 Q9 확인 (Social Amplifier 갭 측정)
P2: 가격 구간 ₩49K → ₩59K-69K 상향 검토 (L5 가능성 45%로 상승)
```

**Update Output 규칙**:
- 전체 분석 재발행 금지 — 변화된 부분만 출력
- [GROUNDED: D1 Q번호] 태그 신규 추가분만 명시
- Evidence 등급이 변하지 않으면 "변화 없음" 대신 "현재 {등급} — {등급 업 조건}" 형식으로 남은 경로 명시
- 새 follow-up이 발생하면 "남은 불확실성" 섹션에 다음 D1 질문 1개 제시

**Knowledge-level 적응 (Update Output)**:

| 요소 | novice | intermediate | expert |
|------|--------|-------------|--------|
| 레벨 진단 변화 | "L4 가능성이 높아졌어요 / 낮아졌어요" | Level 테이블 (이전→새 확률) | 확률% 전체 + 변화 방향 |
| Evidence 변화 | "조금 더 확실해졌어요" | "MEDIUM → MEDIUM" 형식 | 등급 + 남은 조건 |
| 남은 불확실성 | 다음 질문 1개 (자연어) | D1 Q번호 + 원문 | 우선순위 정렬 테이블 |
| 실행 함의 변화 | 1개 변경사항만 | P1-P2 변경분 | 전체 P1-Pn delta |

**진단 안정화 기준 (Diagnostic Closure)** — 언제 follow-up 체인을 종료하는가:

```
종료 조건 (두 조건 모두 충족 시):
□ Evidence = HIGH (핵심 답변 전부 [GROUNDED], [FLAGGED] 0개)
□ [UNCERTAIN] 목록 = 비어있음 (0개)
→ 두 조건 모두 충족 → "진단 확정" 클로저 메시지 emit, follow-up 체인 종료

부분 종료 (한 조건 충족 시):
- Evidence = HIGH + [UNCERTAIN] ≥ 1 → 남은 불확실성 1개만 제시, "선택적 follow-up" 명시
- Evidence < HIGH + [UNCERTAIN] = 0 → "레벨 진단은 확정, 단 Evidence 신뢰도는 MEDIUM — BDM 경매 권장" emit
```

**"진단 확정" 클로저 메시지** (진단 안정화 기준 충족 시):
```markdown
## 진단 확정
레벨: L4 (Evidence: HIGH | [UNCERTAIN]: 0)
가격 커밋 가능: ₩49K 티어 (D1 N≥10 확인, Q3+Q8 grounded)
→ 더 이상 follow-up 필요 없음. P2 Fake Door 실험으로 진행 가능.
```

**Tool-to-Agent Bipartite Graph (arXiv:2511.01854 적용)**:

```
Ameva 내부 아키텍처 = G=(A, T, E):
  A (Agent nodes): value-gap-theory, career-mirror-builder, biz-strategy, ...
  T (Tool nodes):  T1, T2, T3, T4, D1, D2, ... (corpus docs as tools)
  E (Edges):       소유 관계 + 유사도 가중치

Corpus Pre-step → T 노드 선택 (유사도 기반 top-K doc_ids)
L2 Selection   → A 노드 선택 (corpus context가 WTP 스킬로 가중치 부여)
Corpus pass-through → T→A 엣지: 선택된 corpus context를 해당 스킬 args에 포함

이 구조에서 corpus docs(T)와 skills(A)는 동일 그래프의 서로 다른 노드 타입 —
"agent/skill만 tool처럼 사용"이 아니라 corpus docs도 tool 노드로 동등하게 취급됨.
```

---

### Step 1A — Mode A: /conceptual 직접 실행 *(no flags)*

Corpus context 주입된 상태에서 L1 /conceptual 전체 실행.

**Mode Auto-Recommendation** (Mode A 실행 시 먼저 emit — RouteLLM 2024 기반):

```
질문 복잡도를 평가하여 더 적합한 모드가 있으면 추천 emit (Mode A는 그대로 진행):

if 질문이 최신 시장 데이터 / 한국 경쟁사 / 2026 기준 필요:
    emit: "[Mode Rec: 이 질문은 Mode B (-r) 권장 — 외부 최신 데이터가 필요합니다]"
elif 질문이 실행 계획 / 가격 설계 / 투자자 피치 / 전략 수립:
    emit: "[Mode Rec: 이 질문은 Mode D (-d) 권장 — 도메인 스킬 dispatch가 더 정확합니다]"
elif 질문이 전략 + 최신 데이터 모두 필요:
    emit: "[Mode Rec: 이 질문은 Mode C (-rd) 권장]"
else:
    # 이론/진단 질문 → Mode A 최적, 추천 없음
    pass

# 추천은 emit만 — 사용자가 Mode A로 계속 진행하면 Mode A 그대로 실행
```

- Mode Gate → Stage 1 Draft → Stage 2 Adversarial → Stage 3 Deliver
- Claim Grounding Protocol 적용 (Stage 1 Draft 중)
- Stage 2 Adversarial — corpus_mode=="corpus" 시: Q0 meta-sycophancy → `active_corpus.sycophancy_checks` (Q1~Q7) 자동 주입 **(P14 신규: Mode A도 Step 4 동일 프로토콜 적용)**
  ```
  corpus_mode == "corpus":
      Q0 meta-sycophancy check (항상 먼저)
      for Q in active_corpus.sycophancy_checks:
          [PASS: {evidence}] 또는 [FLAGGED: {오류} + {수정}]
  corpus_mode == "generic":
      /conceptual 기본 adversarial Q0-Q6 적용 (corpus 없으므로 domain check 생략)
  ```
- Step 0 Outward Profile + user_domain_knowledge → Stage 3 Deliver에 적용
- Announce:
  ```
  [ameva: Mode A | corpus={doc_ids} | /conceptual | {epistemic_basis}/{causal_model} | knowledge={level}]
  ```

---

### Step 1B — Mode B: External Knowledge + /conceptual *(-r flag)*

**1. External Knowledge 검색** (RAG 강제):
```
WebSearch (mcp__websearch__search_web) 최소 1회
+ expert-research-v2 agent (논문/심층 필요 시)
```

**2. /conceptual 실행** (corpus context + external context 주입):
```
[Corpus Context]   ← Corpus Pre-step 결과
[External Context] ← WebSearch 결과
Blind spots: {corpus vs external 간 gap}
```

- Stage 2 Adversarial — corpus_mode=="corpus" 시: Q0 + `active_corpus.sycophancy_checks` 주입 **(P14 신규: Mode B도 동일 프로토콜)**
- Announce:
  ```
  [ameva: Mode B | corpus={doc_ids} | /conceptual + RAG | {epistemic_basis}/{causal_model}]
  [RAG FORCED]
  ```

---

### Step 1C — L1 (1st pass) 재프레이밍 *(Mode C & D)*

```
Read: ~/.claude/skills/conceptual/state/paradigm.json
Read: ~/.claude/skills/conceptual/state/nodes/{basename $PWD}.jsonl (last 20 lines)
```

자유 추론 후 **Refined Query Slot** 추출:
```
[Refined Query]
Real question: {reframed — corpus context 반영}
Corpus signals: {어떤 doc_id가 핵심인지}
Paradigm signals: {epistemic_basis / key tension}
```

추론 흔적 폐기 — Refined Query Slot만 L2로 전달 *(Attentional Residue 방지)*.

---

### Step 2 — L2: Dynamic Domain Dispatch *(Mode C & D)*

#### Pre-check: 기존 분석 결과 활용

```
Refined Query → docs/research/*.md 스캔
```

| 조건 | 동작 |
|------|------|
| **`-rd` 플래그** | RAG_FORCED — Pre-check 건너뜀 |
| 기존 docs 높은 관련성 + 7일 이내 | REUSE |
| 부분 커버 | HYBRID |
| 없음/오래됨 | DISPATCH |

#### Selection

Refined Query + Corpus Context 기반으로 **system-reminder 전체 스킬 풀에서 동적 선택**.
- WTP 관련 질문 → corpus context가 value-gap-theory, career-mirror-builder 선택 유도
- 하드코딩 없음 — corpus context가 자연적 가중치 역할

#### Corpus Pass-through (Ameva 전용 — L2 dispatch 시 corpus context 주입)

L2 스킬이 corpus를 재로드하지 않도록, dispatch args에 corpus context 주입:

```python
# Primary corpus context — corpus-agnostic (active_corpus.core_theory, not hardcoded WTP)
corpus_context_block = f"""
[CORPUS CONTEXT — {active_corpus.name} | docs: {", ".join(loaded_doc_ids)}]
Core theory:
{active_corpus.core_theory}
  [핵심 섹션 excerpt — 전체 doc 아님]
{product_scope_warnings}  # product_docs에서 로드된 경우 [PRODUCT-SCOPE WARN] 태그 포함
                           # ⚠️ L2 WARN 전파 규칙 (아래 참조)
"""

# Secondary corpus context (P5 dual-corpus mode only)
# secondary_loaded_doc_ids: Corpus Pre-step에서 로드된 secondary corpus docs
if corpus_mode == "dual" and secondary_corpus:
    secondary_context_block = f"""
[X-CORPUS CONTEXT — {secondary_corpus.name} | docs: {", ".join(secondary_loaded_doc_ids)}]
Secondary corpus (confidence={secondary_conf:.2f}) — facts tagged [X-GROUNDED:{secondary_corpus.name}.doc_id]:
Core theory:
{secondary_corpus.core_theory}
  [참고 수준 — primary corpus보다 낮은 confidence. 직접 인용 시 [X-GROUNDED] 필수]
"""
    full_corpus_context = corpus_context_block + secondary_context_block
else:
    secondary_context_block = ""
    full_corpus_context = corpus_context_block

# L2 dispatch — full corpus context 주입 (primary + secondary, cross-skill consistency 보장)
# Note: 하드코딩된 스킬명 없음 — entity Step 2 Selection이 system-reminder 풀에서 동적 선택
# full_corpus_context를 L2 dispatch args에 append하는 방식은 entity/Step 2와 동일
dispatch_args = Refined_Query + "\n\n" + full_corpus_context
# 예시 (실제 선택은 동적):
# Skill('value-gap-theory',      args=dispatch_args)
# Skill('career-mirror-builder', args=dispatch_args)
# Agent(subagent_type='biz-strategy', prompt=dispatch_args)
```

**Cross-skill consistency**: 모든 L2 스킬이 동일 corpus 스냅샷 위에서 실행 → 스킬 간 모순 방지.

**Product-scope WARN 전파 규칙** (Mode C/D L2 dispatch에서 필수):
- corpus_context_block에 `[PRODUCT-SCOPE WARN: {doc_id}]` 포함 시 → L2 output에서 해당 수치를 사용했으면 WARN 태그 그대로 유지
- L2 스킬이 WARN 태그를 제거했을 경우 → Synthesis 단계에서 orchestrator가 수치 재식별 후 태그 복원
- Quality Gate 체크 #10이 end-to-end 최종 검증 (product_docs ∈ loaded_docs → WARN 태그 존재 확인)

#### External Knowledge

```
OR 조건 (하나라도 → 검색):
1. Corpus에서 커버되지 않는 최신 시장 데이터
2. 한국 경쟁사 동향
3. L1(1st)가 corpus 갭 감지
4. -r 또는 -rd 플래그 (RAG_FORCED)
```

**claim 분류** (synthesis 전):
| 유형 | 처리 |
|------|------|
| OBSERVED (corpus 직접 지지) | [GROUNDED:doc_id] |
| INFERRED-logic (corpus에서 논리 도출) | [REASONED:basis] |
| INFERRED-design (corpus 외 해석) | → 검색 후 synthesis |
| 검증 불가 | [UNCERTAIN] + 검증 방법 |

**Explicit MoA Aggregation (P10 신규 — MoA arXiv:2406.04692)**: L2 결과를 L1(2nd)에 전달하기 전 명시적 aggregation 단계 실행.
Research basis: MoA proposer→aggregator 구조. 암묵적 종합보다 충돌 감지율 ↑, 최종 답변 일관성 ↑.

```python
# L2 dispatch 완료 후, L1(2nd) 진입 전
l2_results = {skill_name: output for skill_name, output in dispatched_results.items()}

# 1. 충돌 감지: 동일 주장에 대해 두 L2 스킬의 결론이 다른가?
conflicts = []
for (s1, s2) in combinations(l2_results.keys(), 2):
    for claim in extract_shared_claims(l2_results[s1], l2_results[s2]):
        if contradiction(claim, l2_results[s1], l2_results[s2]):
            conflicts.append("[L2-CONFLICT: " + s1 + " vs " + s2 + "] " + claim[:80])

# 2. 합의 추출: 여러 스킬이 동일 결론을 지지 → 신뢰도 ↑
consensus = extract_consensus(l2_results)  # 2+ 스킬 동의 주장 목록

# 3. aggregated_context 구성 -> L1(2nd) Stage 1 Draft에 주입
per_skill = "\n".join("  [" + sk + "]: " + out[:120] for sk, out in l2_results.items())
aggregated_context = (
    "[L2 Aggregated - " + str(len(l2_results)) + " skills]\n"
    "Consensus (" + str(len(consensus)) + "): " + "; ".join(consensus[:3]) + "\n"
    "Conflicts (" + str(len(conflicts)) + "): " + "; ".join(conflicts[:3]) + "\n"
    "Per-skill summaries:\n" + per_skill
)
emit("[L2-AGG] " + str(len(l2_results)) + " skills | " + str(len(consensus)) + " consensus | " + str(len(conflicts)) + " conflicts")
```

**L1(2nd) Stage 1은 aggregator 역할**: `aggregated_context`를 기반으로 합의 주장은 강조, 충돌은 [X-CONFLICT]로 표면화, 불확실은 [UNCERTAIN]으로 처리.
단일 L2 스킬인 경우 (len(l2_results)==1): 직접 전달, aggregation 없음.



---

### Step 3 — Announce *(Mode C & D)*

```
ameva activated.
[ameva: Mode {C|D} | corpus={active_corpus.name}({doc_ids}) | /conceptual | {epistemic_basis}/{causal_model}]
[DUAL-CORPUS: {secondary_corpus.name}({secondary_doc_ids})]  ← corpus_mode=="dual" 시에만
[DRAFT-CORPUS: {corpus_name}]  ← active_corpus 또는 secondary_corpus가 draft 상태 시에만
[L2: {dispatched skill/agent names}]
[RAG FORCED]  ← Mode C (-rd) 시에만
```

### Step 4 — L1 (2nd pass): /conceptual with enrichment *(Mode C & D)*

Corpus Context + L2 Domain Context 주입 상태에서 /conceptual 전체 실행.

**Stage 2 Adversarial — Corpus-Specific Sycophancy Checklist**:

```
# Q0 — Meta-sycophancy check (P2 신규, Constitutional AI 기반 — Bai et al. 2022)
# 모든 corpus에 공통 적용 — corpus.sycophancy_checks 실행 전 반드시 먼저 실행
Q0: "이 질문 자체가 특정 결론을 전제하고 있는가?"
    예: "L5 WTP가 높다고 했는데 왜 그런가요?" → L5 WTP 높음을 기정사실로 전제
    예: "Career Mirror가 효과 있다는 가정 하에 가격을 어떻게 잡아야 할까요?" → 효과 미검증
    → 전제가 있으면: "[META-SYCOPHANCY: 질문이 '{전제}를 사실로 가정함. 실제로는 [UNCERTAIN]." emit 후 전제를 명시 해제하고 답변 진행
    → 없으면: [PASS: 전제 없음] — 다음 단계 진행

# Q1~Q7 — corpus.sycophancy_checks 실행
for Q in active_corpus.sycophancy_checks:
    체크: 해당 오류 패턴이 현재 draft에 있는가?
    → [PASS: {왜 통과인지 1줄}]          # 명시적 통과 증거 — 형식적 통과 방지
    → [FLAGGED: {오류 설명} + {수정 내용}] # 실패 시 수정 내용까지 기록

corpus: wtp 예시 — Q1~Q7 (Corpus Registry 정의 참조)
corpus: (next) — 해당 corpus의 sycophancy_checks 자동 적용
```

- Stage 3 Deliver: Outward Profile + Corpus Citation 통합

---

### Claim Grounding Protocol (SELF-RAG + FLARE-lite + Grounding Wall)

**Research basis**: Self-RAG (arXiv:2310.11511) — Stage 1 Draft 후 claim별 [IsSup] 검증 루프; FLARE (EMNLP 2023) — confidence 기반 on-demand retrieval; Named tool per document (SOTA for <50 doc corpora); Grounding Wall (medical/legal Vertical AI production pattern).

#### Stage 1.5 — SELF-RAG Claim Validation Loop (Stage 1 Draft 직후)

```
Stage 1 Draft 완료 후, Stage 2 Adversarial 진입 전:

For each domain claim in draft:
  Step A: claim에 [GROUNDED:doc_id] 태그가 있는가?
    YES → ✅ corpus 기반 확인됨 → 유지

  Step B: [GROUNDED] 없는 도메인 팩트 발견 (= [IsSup] 실패):
    → FLARE-lite: read_corpus(most_relevant_doc_id) 실행
    → 해당 문서에서 claim 지지 여부 확인
    YES → 태그 추가 [GROUNDED: doc_id] + Draft 수정
    NO  → [UNCERTAIN] 태그로 교체 + 검증 방법 병기

  Step C: 도메인 팩트가 corpus의 deprecated 주장인가? **(P16: corpus-agnostic 버전)**
    if active_corpus has `deprecated_claims` field:
        for claim in active_corpus.deprecated_claims:
            draft에 해당 표현 있으면 → 최신 버전으로 교체 + [ANTI-PATTERN: deprecated] 태그
    elif active_corpus.name == "wtp":
        # WTP 전용 하위 호환 (deprecated_claims 필드 없는 legacy corpus용)
        "직장인 L5 WTP 낮다" → v2 기준으로 교체 [T2]
        "Social_Amplifier 순수 곱셈" → v2 재정의로 교체 [T2]
    else:
        # 다른 corpus: deprecated 정의 없음 → Step C skip

결과: Stage 2 Adversarial 진입 시 모든 도메인 팩트에 grounding 태그 보장
```

#### Corpus Load Strategy

```
Context bu

  Step D: [IsUse] 체크 (P11 신규 — SELF-RAG 4번째 토큰 완성 arXiv:2310.11511):
    이 [GROUNDED] claim이 현재 질문에 대한 답변에 실제로 기여하는가?
    YES (관련있고 질문 답변에 기여) → 유지
    NO  (faithful이지만 현재 질문과 무관한 corpus 사실) → [UNUSED] 태그 + Stage 3 출력에서 제외
    예: 질문이 "L4 수준 진단"일 때 Career Mirror 가격 정보 claim → [UNUSED]

  Step E: Multi-doc grounding 체크 (P11 신규 — CRAG arXiv:2401.15884):
    하나의 주장에 두 개 이상 doc의 복합 근거가 필요한가?
    YES → [GROUNDED: T2+D1, 복합 근거] 태그 사용 (단일 doc 태그를 교체)
    NO  (단일 doc 지지 충분) → 기존 [GROUNDED: doc_id] 유지dget 충분 (일반적 경우):
  EAGER load — 관련 doc_ids 전체를 Corpus Pre-step에서 로드
  → SELF-RAG validation loop에서 추가 read_corpus 불필요 (이미 메모리에 있음)

Context budget 부족 (긴 세션):
  LAZY (FLARE-lite) — Stage 1.5에서 필요할 때만 read_corpus 호출
  → FLARE threshold: [UNCERTAIN] 도메인 팩트 발견 시 즉시 retrieval
```

#### T3 Corpus Conflict Resolution (Meta-review 통합)

T3 (wtp-meta-review.md)는 corpus 내 내부 모순을 명시적으로 감사한 문서다. T3 기준으로 corpus 간 충돌을 처리:

```
충돌 우선순위 (v2 기준 현행 버전 우선):
  T2 > T1     (v2 공식이 v1 공식을 대체)
  T4 > T2     (반증 기준이 이론 적용을 게이팅)
  T3          (신뢰도 한계 — 이론 전체에 적용)

T3가 명시한 주요 신뢰도 한계 (항상 적용):
  1. D(WTP) 공식은 경험적 검증 미완 → 예측값에 [UNCERTAIN] 필수
  2. L1-L5 경계가 명확하지 않음 → 레벨 진단은 범위로 표현 (L3-L4 가능성)
  3. Social_Amplifier 계수화 방법론 미확립 → 방향성만 진술, 수치 단정 금지

corpus 간 충돌 발견 시:
  [CONFLICT: T1 vs T2] 표시 → v2 기준(T2) 채택 + T1 폐기 사유 명시
  [TRUST LIMIT: T3] 표시 → 해당 주장의 검증 필요 조건 병기

LLM knowledge vs corpus 충돌:
  → corpus 기준 우선 (LLM knowledge는 parametric 오염 가능)
```

#### Grounding Wall 규칙

```
❌ corpus 없이 도메인 팩트 emit 금지
❌ LLM parametric knowledge로 도메인 클레임 emit 금지 (아무리 확신해도)
✅ 그라운딩 불가 → [UNCERTAIN] + 검증 방법 → 사용자에게 명시
✅ corpus vs corpus 충돌 → T3 우선순위로 해결 + [CONFLICT] 태그
✅ T3 신뢰도 한계 적용 영역 → [TRUST LIMIT: T3] 명시
```

**Claim 태그 형식**:
```
[GROUNDED: T2, Gap_Intensity section]  ← corpus 직접 지지
[GROUNDED: T2+D1, 복합 근거]         ← 두 doc에서 함께 지지되는 주장 (P10 신규)
[REASONED: T2 기반 논리 도출]          ← corpus에서 논리적 파생
[UNCERTAIN] → 검증: BDM 경매 N≥200    ← corpus 없음, 검증 방법 병기
[UNUSED: {claim_text}]              ← faithful이지만 현재 질문 답변에 불필요 (P10 신규)
[ANTI-PATTERN: v1 주장 → v2 교체]      ← 폐기된 이전 정의 감지
```

---

## 모드별 실행 비교

| | Mode A | Mode B (-r) | Mode C (-rd) | Mode D (-d) |
|--|--|--|--|--|
| **Corpus Pre-step** | **✅ 항상** | **✅ 항상** | **✅ 항상** | **✅ 항상** |
| Step 0 Outward Reception | ✅ | ✅ | ✅ | ✅ |
| L1(1st) 재프레이밍 | ❌ | ❌ | ✅ | ✅ |
| External RAG | ❌ | ✅ 강제 | ✅ 강제 | 조건부 |
| L2 Domain Dispatch | ❌ | ❌ | ✅ | ✅ |
| Claim Grounding | ✅ | ✅ | ✅ | ✅ |
| 속도 | 빠름 | 중간 | 느림 | 중간 |
| 적합 | 이론/진단 질문 | 최신 시장 데이터 | 전략/투자자 | 실행 계획 |

---

## Stage 3 Deliver — Knowledge-Level Adaptive Output

**실행 순서**: Pre-output Quality Gate 통과 후 → 이 섹션의 해당 템플릿 선택 → 출력.
(이 섹션이 문서 내 Quality Gate보다 앞에 위치하나, 실행은 Quality Gate 이후에 일어난다.)

Step 0에서 감지된 `user_domain_knowledge` 수준에 따라 Output Format의 **구조와 표현 방식**을 구체적으로 조정한다.
인사이트는 바뀌지 않는다 — 포장 방식과 노출 깊이만 바뀐다.

**Template 라우팅 (P12 신규 — corpus-aware)**: corpus별 전용 템플릿이 있으면 우선, 없으면 Generic Fallback 사용.

```python
# Template selection (at Stage 3 entry)
if active_corpus.name == "wtp":
    → WTP Corpus Templates (아래 novice/intermediate/expert)
elif active_corpus.name == "marketing_growth":
    → Generic Fallback Template (아래 "Generic Fallback" 섹션) — WTP terminology 미사용
else:
    → Generic Fallback Template
# 새 corpus 등록 시: corpus별 전용 templates를 이 섹션 아래 추가 (WTP templates를 참고)
# 전용 templates 없는 corpus → Generic Fallback이 core_theory 기반 자동 적응
```

---

### [WTP Corpus] Template: novice (도메인 용어 0-1개)

```markdown
## Ameva — [질문 요약]
Mode: A | Corpus: [T2, D1] | Scope: IN | Evidence: MEDIUM

### 핵심 답변
[비유 중심 1-2줄. 수식 없음. "갭 = '내가 얼마나 뒤쳐져 있다고 느끼는가'" 형식]

### 레벨 판단 (해당 시)
→ [L4 가능성 높음 / L3-L4 구간 / L5 가능성 있음] — 이유: [Q3, Q8 신호 1줄 설명]
→ 정확한 진단은: D1 인터뷰 13문항 실행 필요

### 분석 (쉬운 설명)
- 이 분야에서 잘 나가는 사람과 비교될 때 가격 더 낼 의향이 생깁니다. (연구 기반)
- 외부에서 알아봐 주는 채널이 있으면 갭이 더 잘 느껴집니다. (연구 기반)
- [확인 필요] 정확한 금액 → 실제 인터뷰 후 가능

### 다음 단계 (1-2개만)
1. D1 질문지 13개 중 Q1, Q3, Q8 먼저 확인
2. 비슷한 배경 사람 중 잘 되는 케이스 1명 떠올려보기

### 이게 적용되지 않는 경우
[T4 기준 1줄 — 고관여 결정이 아닐 때]

### 출처
[T2] WTP 이론 문서 v2
[D1] 인터뷰 진단 프로토콜
```

### Template: intermediate (도메인 용어 2-3개)

```markdown
## Ameva — [질문 요약]
Mode: [A/B/C/D] | Corpus: [T2, D1, P2] | Scope: IN | Evidence: MEDIUM

### 핵심 답변
[수식 제시 + 직관 연결. "D(WTP) = Gap_Intensity × Attainability × Social_Amplifier — 이 경우 Social_Amplifier가 핵심 변수입니다"]

### 레벨 진단 (해당 시)
| Level | 가능성 | 핵심 신호 | 근거 |
|-------|--------|----------|------|
| L4 | 높음 | Q3 신호: 동료 비교 + 델타 분석 | [GROUNDED: D1, Q3] |
| L5 | 중간 | Q8 신호 확인 필요 | [UNCERTAIN] |
| L3 | 낮음 | 변화 속도 불만 미감지 | [REASONED: D1] |
→ L4 우세, D1 Q8 확인 후 L5 판정 가능

### 분석
- [GROUNDED: T2] Gap_Intensity = Functional + Social_Position — 이 경우 Social_Position_Gap 지배
- [REASONED: T2 기반] 외부 가시성 채널 있는 프리랜서 → Social_Amplifier ON 가능성
- [UNCERTAIN] 구체적 WTP 금액 → 검증: D1 인터뷰 후 P2 가격 구간 대조

### 경쟁 이론 비교 (해당 시)
| 상황 | VALUE GAP | JTBD | 적용 조건 |
| --- | --- | --- | --- |
| 정체성 이동 욕구 있음 | L4-L5 → 고WTP | "done job" 분석 | VALUE GAP 우선 |
| 기능적 도구 니즈 | Functional_Gap | Hire 맥락 분석 | JTBD 병행 |

### 실행 함의
| 우선순위 | 액션 | 근거 | 위험 |
|--------|------|------|------|
| P1 | D1 Q3+Q8 인터뷰 | L4/L5 확정 | 2주 소요 |
| P2 | ₩49K 티어 테스트 | [GROUNDED: P2] L4 구간 ₩39K-59K | WTP 미검증 |

### 범위 주의 [T4]
[고관여 + 정체성 인접 + 비독점 시장 여부]

### 이 진단을 바꿀 수 있는 정보
| 새 정보 | 변경 방향 | 실행 함의 변화 |
|--------|---------|--------------|
| D1 Q8 신호 확인 | L4 → L5 가능성 상향 | 가격 구간 상향 검토 |
| identity portable 증거 없음 | L5 불가, L4 유지 | — |
| WTP 인터뷰 ₩39K 미만 | L3 하향 | 가격 전면 조정 필요 |

### Corpus References
[T2] wtp-theory-v2.md — D(WTP) 공식 v2 (Gap_Intensity section)
[D1] wtp-interview-protocol.md — L1-L5 진단 Q3, Q8
[P2] wtp-career-mirror-pricing.md — L4 가격 구간

### 다음으로 물어볼 것
[UNCERTAIN] 항목에서 자동 생성. 진단 신뢰도를 가장 빠르게 높이는 질문 1-2개.

> **불확실성**: {가장 큰 UNCERTAIN item}  
> → D1 Q{N}: "**{question text — D1 원문에서 직접 인용}**"  
> → 예상 신호: {신호 있을 때 → 진단 변화} / {신호 없을 때 → 진단 유지 조건}

예 (intermediate 템플릿, L4/L5 진단 케이스):
> **불확실성**: identity portability 여부 미확인  
> → D1 Q8: "**본인 이름으로 뭔가 쌓고 있다는 느낌이 있어요, 아니면 지금 있는 데서 쌓고 있는 느낌이에요?**"  
> → 예상 신호: "내 이름으로 쌓고 싶은데 잘 안 된다" → L5 포텐셜 확인 / 없음 → L4 유지
```

### Template: expert (정확한 taxonomy + 이론 내 충돌 질문)

```markdown
## Ameva — [질문 요약]
Mode: [A/B/C/D] | Corpus: [T2, T3, T4, D1, P2] | Scope: IN | Evidence: MEDIUM

### 핵심 답변
[수식 직접. 이론 내 불확실성 전면 공개. T3 신뢰도 한계 포함.
"D(WTP)_instantaneous = [Gap_Intensity × Attainability] × Social_Amplifier.
단, T3 신뢰도 한계: L1-L5 경계가 명확하지 않아 레벨 진단은 범위로 표현 필수."]

### 레벨 진단 차등 분석
| Level | 확률 추정 | 신호 (corpus 근거) | T4 반증 조건 |
|-------|----------|-------------------|-------------|
| L4 | ~60% | Q3: 비유도 동료 비교 + 델타 [D1, Q3] | HC-2: 갭 인식 40%+ 조건 검토 |
| L5 | ~25% | Q8 "내 이름으로 쌓고 싶은데" 미확인 [UNCERTAIN] | identity portable 미검증 |
| L3 | ~15% | 변화 속도 불만 미감지, Q5 반응 약함 [REASONED: D1] | — |

**[TRUST LIMIT: T3]** L1-L5 경계 불명확 — 위 확률은 D1 신호 기반 추정, BDM 경매 N≥200 없이는 [UNCERTAIN]

### 분석
- [GROUNDED: T2, Gap_Intensity section] Gap_Intensity = Functional_Gap + Social_Position_Gap
- [GROUNDED: T2, ICP 정의] ICP = identity portability, 고용 형태(프리랜서/직장인) 아님
- [CONFLICT: T1 vs T2] T1의 "프리랜서 = 고WTP" 단정 → T2에서 identity portability로 교체
- [REASONED: T2 Social_Amplifier] 외부 가시성 채널 있으면 Social_Amplifier ON — 계수화 방법론 미확립 [TRUST LIMIT: T3]
- [UNCERTAIN] 구체적 WTP 금액 → 검증: BDM 경매 N≥200, 또는 D1 13문항 + P2 가격 구간 대조

### 경쟁 이론 비교
| 조건 | VALUE GAP 예측 | JTBD 예측 | 선택 기준 |
| --- | --- | --- | --- |
| identity gap 인식 있음 | L4-L5, D(WTP) 높음 | Hire: self-development | VALUE GAP 우선 |
| 기능적 도구 교체 니즈 | Functional_Gap, L2-L3 | Hire: output quality | JTBD 병행 |
| 충동 구매 | 적용 불가 [T4] | Jobs-to-be-done 적용 | JTBD 사용 |

### 실행 함의
| 우선순위 | 액션 | 근거 | 위험 / 조건 |
|--------|------|------|------------|
| P1 | D1 13문항 인터뷰 N≥10 | L4/L5 확정 + ICP 검증 | [UNCERTAIN] — 실행 전 레벨 단정 금지 |
| P2 | ₩39K-59K 구간 Fake Door 실험 | [GROUNDED: P2] L4 가격 구간 | WTP 검증 전 커밋 위험 |
| P3 | Social_Amplifier 채널 감사 | [REASONED: T2] 가시성 조건 | 계수화 방법 미확립 [T3] |

### 범위 주의 [T4]
고관여 구매? ✅ | 정체성 인접? ✅ | 비독점 시장? ✅ → IN
무효화: 충동/commodity 구매 시 VALUE GAP 적용 불가 → JTBD 또는 Mental Accounting

### 진단 노트 (Stage 1 → Stage 2 Adversarial) ← **생략 금지: [FLAGGED] ≥1이면 반드시 포함 / expert는 항상 포함**

**형식:**
```
Stage 1 핵심 주장 (검토 대상):
1. "{Stage 1 Draft의 핵심 주장 1}" → [PASS: Q{N}] or [FLAGGED → 교정됨]
2. "{Stage 1 Draft의 핵심 주장 2}" → [PASS: Q{M}]
3. "{Stage 1 Draft의 핵심 주장 3}" → [PASS: Q{K}]

교정 상세 ([FLAGGED] 항목만):
[FLAGGED: Q{N}] Stage 1 초안: "{교정 전 주장 또는 분류}"
→ 교정 근거: {T2/T4/D1 등 doc_id}
→ Stage 3 적용: "{교정 후 내용}"
```

예:
```
Stage 1 핵심 주장:
1. "프리랜서 디자이너 = L5 ICP 가능성 높음" → [FLAGGED: Q2 → 교정됨]
2. "D(WTP)_L4 = 1.53, 가격 ₩39K-59K" → [PASS: Q1, GROUNDED:P2]
3. "D1 Q8이 핵심 판별 질문" → [PASS: Q3, GROUNDED:D1]

교정 상세:
[FLAGGED: Q2] Stage 1 초안: "프리랜서 = L5 ICP 가능성 高"
→ 교정 근거: ICP = identity portability (고용 형태 아님) [T2]
→ Stage 3 적용: "외부 가시성 유무로 교체"
```

**knowledge-level 규칙**:
- novice: 생략 (혼란 방지)
- intermediate: [FLAGGED] ≥1 시만 포함 (교정 상세만, Stage 1 핵심 주장 생략)
- expert: 항상 포함 (Stage 1 핵심 주장 + 교정 상세 전체)

### Answer Drivers (진단 변경 조건) [T4 + T3 기반] ← **생략 금지 (intermediate/expert 필수 섹션)**
| 현재 진단 | 새 정보 / 변경 조건 | 변경 방향 | 실행 함의 변화 |
|----------|-------------------|---------|--------------|
| L4 우세 (~60%) | D1 Q8 "내 이름으로" 확인 | L4-L5 공동 가능성 상향 | 가격 구간 ₩49K → ₩69K 검토 |
| L5 보류 (~25%) | identity portable 증거 없음 | L5 불가, L4 유지 | P3 Social_Amplifier 채널 감사 생략 |
| WTP ₩39K-59K | BDM 경매 ₩39K 미만 → L3 하향 | 가격 ₩19K-29K으로 전면 조정 | P2 Fake Door 가격 변경 필수 |
| Evidence: MEDIUM | D1 N≥10 + V1 실험 결과 수집 | Evidence: HIGH 가능 | 가격 커밋 가능 시점 |

### Corpus References
[T2] wtp-theory-v2.md — D(WTP) 공식 v2 (Gap_Intensity section, ICP 정의 section)
[T3] wtp-meta-review.md — L1-L5 신뢰도 한계 (2026-04-11)
[T4] wtp-falsification-criteria.md — HC-1, HC-2, HC-3
[D1] wtp-interview-protocol.md — L1-L5 진단 Q3, Q8
[P2] wtp-career-mirror-pricing.md — L4 가격 구간 ₩39K-59K

### 다음으로 물어볼 것 (Follow-up Prompt)
[UNCERTAIN] + [TRUST LIMIT] 항목에서 자동 생성. Answer Drivers의 "새 정보 / 변경 조건"과 정렬.
진단 변경 가능성(impact)이 가장 높은 순으로 정렬. D1 질문 원문을 직접 인용.

| 우선순위 | 불확실성 | 추천 질문 (D1 출처) | 예상 신호 → 진단 변화 |
|--------|---------|-------------------|-------------------|
| 1 | {가장 큰 UNCERTAIN item} | D1 Q{N}: "{question text — 원문 인용}" | 신호 YES → {레벨/방향 변화} |
| 2 | {두 번째 UNCERTAIN item} | D1 Q{N}: "{question text — 원문 인용}" | 신호 NO → {현 진단 유지 조건} |

**생성 규칙**:
- Answer Drivers의 각 행에서 "새 정보 / 변경 조건"을 추출 → 해당 정보를 얻을 수 있는 D1 질문 번호 매핑
- D1 질문 원문은 D1 corpus에서 직접 인용 (paraphrase 금지)
- [TRUST LIMIT: T3]가 있으면 신뢰도 한계 한 줄 추가: "위 추천 질문으로도 해소 불가 시 → BDM 경매 N≥200 필요"
- novice 템플릿에는 생략 (novice는 이 진단을 바꿀 수 있는 정보 섹션으로 대체)
```

### Outward Profile Integration (모든 템플릿에 공통 적용)

Step 0의 epistemic_basis가 **핵심 답변의 오프닝 앵커**와 **마지막 문장(closing move)**을 바꾼다.
인사이트 내용은 바뀌지 않는다 — 첫 줄과 마지막 줄만 바뀐다.

```
epistemic_basis → 오프닝 앵커 (핵심 답변 첫 줄):
  data-driven       → "The pattern in the data points to: {핵심 진단}"
  intuition-first   → "What I'm noticing here — {핵심 진단}"
  authority-ref     → "This maps to a well-established pattern: {핵심 진단}"
  unknown           → {핵심 진단} 직접 시작

epistemic_basis → 클로징 무브 (마지막 줄, 실행 함의 또는 이 진단을 바꿀 수 있는 정보 직후):
  data-driven       → 한국어: "실제로 측정해보셨을 때 결과가 어떻게 나왔나요?" / 영어: "What does your data show on this?"
  intuition-first   → 한국어: "지금 느끼시는 것과 맞닿는 부분이 있나요?" / 영어: "Does that match what you're sensing?"
  authority-ref     → 한국어: "본 것, 들은 것과 일치하나요?" / 영어: "Is that consistent with what you've seen work?"
  unknown           → 한국어: "어떻게 느껴지세요?" / 영어: "Does that resonate?"

응답 본문 언어에 맞는 버전 선택. 한국어 대화에서 영어 클로징 무브 금지.
```

예 (intuition-first 화자, intermediate 템플릿):
- 오프닝: "What I'm noticing here — D1 인터뷰 없이 확정은 어렵지만, Q3 신호가 있으면 L4 우세입니다."
- 클로징: "Does that match what you're sensing about this person?"

**적용 규칙**:
- `user_domain_knowledge = novice` → Template novice 사용. 수식, [GROUNDED:doc_id] 태그, 레벨 확률 테이블 생략.
- `user_domain_knowledge = intermediate` → Template intermediate 사용. 수식 + 직관, 레벨 테이블 간소화.
- `user_domain_knowledge = expert` → Template expert 사용. 전체 태그 + 차등 확률 + [CONFLICT/TRUST LIMIT] 전면 공개.
- corpus_mode = "generic" (entity fallback): 위 템플릿에서 Corpus References 생략, [GROUNDED] 태그 → 일반 추론 표시.
- Outward Profile integration: 모든 템플릿에 공통 — epistemic_basis에 따라 오프닝/클로징 교체.

### Generic Fallback Template (WTP 전용 아닌 모든 corpus — P12 신규, P15 심화)

WTP 전용 templates(L1-L5, D(WTP) 공식)를 사용하지 않는 corpus에 적용.
`active_corpus.core_theory`, `active_corpus.taxonomy_groups`를 동적으로 삽입.
**P15 신규**: user_domain_knowledge 별 3개 분기 + key metric intensity markers + corpus closure message.

#### [Generic] Template: novice

```markdown
## Ameva — [질문 요약]
Mode: [A/B/C/D] | Corpus: [{active_corpus.name}: {loaded_doc_ids}] | Scope: IN | Evidence: [HIGH/MEDIUM/LOW]

### 핵심 답변
[비유 중심 1-2줄. 핵심 개념을 일상 언어로. 예: K-factor = "친구 1명이 몇 명을 더 데려오는가"]
"{active_corpus.core_theory 핵심 원칙 1줄 비유 — 수식 없이}"

### 지금 상황 진단
→ {핵심 질문에 대한 1줄 판단} — 이유: {가장 중요한 신호 1개}
→ 더 정확한 진단은: {active_corpus.taxonomy_groups.진단 문서} 활용 필요

### 다음 단계 (1-2개만)
1. {가장 단순한 첫 번째 액션}
2. {있다면 두 번째 — 없으면 생략}

### 이게 적용되지 않는 경우
[active_corpus.scope_gate 중 실패한 조건 1줄 설명]

### 출처
[{doc_id}] {active_corpus.docs[doc_id]}
```

#### [Generic] Template: intermediate

```markdown
## Ameva — [질문 요약]
Mode: [A/B/C/D] | Corpus: [{active_corpus.name}: {loaded_doc_ids}] | Scope: IN | Evidence: [HIGH/MEDIUM/LOW]

### 핵심 답변
[주요 공식/프레임워크 명시 + 직관 연결]
핵심 공식: {active_corpus.core_theory 핵심 수식 또는 원칙 1-2개}
→ 이 케이스에서: {질문 맥락에 공식 적용한 1줄 해석}

### {active_corpus.domain} 진단
| 항목 | 현재 상태 | 강도 | 근거 |
|------|---------|------|------|
| {핵심 metric 1} | {상태} | [약/중/강] | [GROUNDED: {doc_id}] |
| {핵심 metric 2} | {상태} | [약/중/강] | [GROUNDED: {doc_id}] |
| {미검증 항목} | 확인 필요 | — | [UNCERTAIN → 검증: {방법}] |

**강도 기준** (P15 신규 — active_corpus.core_theory 기반):
- 강: {해당 corpus의 high 기준 — 예: K-factor > 0.4}
- 중: {medium 기준}
- 약: {low 기준}

### 실행 함의
| 우선순위 | 액션 | 근거 | 위험 |
|--------|------|------|------|
| P1 | {다음 단계 1} | [GROUNDED: {doc_id}] | {위험} |
| P2 | {다음 단계 2} | [GROUNDED: {doc_id}] | {위험} |

### 이게 적용되지 않는 경우
[active_corpus.scope_gate 중 실패한 조건 명시]

### Corpus References
- [{doc_id}] {active_corpus.docs[doc_id]}
```

#### [Generic] Template: expert

```markdown
## Ameva — [질문 요약]
Mode: [A/B/C/D] | Corpus: [{active_corpus.name}: {loaded_doc_ids}] | Scope: IN | Evidence: [HIGH/MEDIUM/LOW]

### 핵심 답변
[이론 내 불확실성 포함, 경쟁 프레임워크와 비교]
{active_corpus.core_theory 전체 공식 블록}

### {active_corpus.domain} 심층 분석
- [GROUNDED: {doc_id}] {주장 1} — 강도: [강] — 반증 조건: {언제 무효화되는가}
- [GROUNDED: {doc_id}] {주장 2} — 강도: [중] — 한계: {적용 경계}
- [REASONED] {추론 주장} — 근거 체인: {A → B → C} — 검증 우선순위: {방법}
- [UNCERTAIN] {미검증 주장} → 검증: {구체적 방법 + 최소 샘플}
- [TRUST LIMIT] {이 corpus가 커버하지 못하는 영역}: {대안 corpus/프레임워크}

### 모순 및 경계 조건
{active_corpus.sycophancy_checks 중 관련 항목 — 이 분석에 적용되는 오류 패턴}
[있으면 [FLAGGED] 처리, 없으면 "[PASS: {왜 통과인지}]" 명시]

### 이게 적용되지 않는 경우
[active_corpus.scope_gate 전체 조건 상태 명시]

### Corpus References
- [{doc_id}] {active_corpus.docs[doc_id]}

---
**[분석 완료 — {active_corpus.domain}]** (P15 신규 — WTP 진단 확정에 상응하는 closure):
{핵심 결론 1줄} | Evidence: {등급} | 후속 필요: {있으면 명시, 없으면 "추가 분석 불필요"}
```

---

## Pre-output Quality Gate (output 생성 전 필수 체크)

```
Stage 3 Deliver 진입 전, ALL 체크 통과 필수.
각 체크에 repair_action 정의 — 실패 시 HOW까지 명시 (P2 신규).

□ T4 Scope Gate 통과? (3개 조건 모두 YES)
  PASS → 계속
  FAIL → repair: 범위 외 섹션 맨 앞에 추가: "이 질문은 VALUE GAP 적용 범위 외입니다. 이유: {어떤 조건 실패}. 대안: {active_corpus.out_action}"
         → corpus 기반 답변 생략, 대안 프레임워크만 제안

□ 모든 도메인 팩트에 [GROUNDED/REASONED/UNCERTAIN] 태그?
  PASS → 계속
  FAIL → repair: 태그 없는 도메인 팩트를 Draft에서 모두 추출 → Stage 1.5 SELF-RAG 재실행
         → 지지 불가 → [UNCERTAIN] + 검증 방법, 지지 가능 → [GROUNDED:doc_id] 태그 추가

□ [UNCERTAIN] 클레임에 검증 방법 병기?
  PASS → 계속
  FAIL → repair: 각 [UNCERTAIN] 뒤에 검증 방법 추가. 형식: "→ 검증: {방법}" (예: BDM 경매 N≥200, D1 13문항 인터뷰)

□ deprecated 주장 없음? **(P16: corpus-agnostic)**
  조건: active_corpus.deprecated_claims 필드 또는 active_corpus.name=="wtp" 일 때만 실행
  active_corpus.name=="wtp":
    FAIL → repair: "직장인 L5 WTP 낮다" → "[ANTI-PATTERN: v1 폐기] ICP = identity portability [T2]"로 교체
           "Social_Amplifier 순수 곱셈" → "[ANTI-PATTERN: v1 폐기] Social_Amplifier = 신호 전달 계수 [T2]"로 교체
  active_corpus has deprecated_claims:
    FAIL → repair: deprecated_claims 목록의 표현을 최신 버전으로 교체 + [ANTI-PATTERN: deprecated] 태그
  else (wtp 아닌 corpus, deprecated_claims 없음):
    → 이 체크 skip (해당 없음)

□ Stage 2 Adversarial Q0 + sycophancy checklist 실행? ([PASS/FLAGGED] 증거 있음?)
  PASS → 계속
  FAIL → repair: Stage 2 재실행. Q0 meta-sycophancy + corpus.sycophancy_checks 순서로 실행. 결과 [PASS/FLAGGED] 기록 후 재진입.

□ "검증됨" emit 없음? (V1/V2 실험 미실행)
  PASS → 계속
  FAIL → repair: "검증됨"/"실험 결과"/"확인됨" 표현을 "[UNCERTAIN: 실험 설계 단계]"로 교체

□ Corpus References 섹션 포함?
  PASS → 계속
  FAIL → repair: draft에서 [GROUNDED:doc_id] 태그를 모두 추출 → 고유 doc_id 목록 생성 → Corpus References 섹션 추가

□ Evidence 등급 계산 후 헤더에 포함?
  HIGH: 핵심 답변 전부 [GROUNDED] (stable corpus) + Stage 2 Q0-Q7 모두 [PASS]
        **단, [GROUNDED-DRAFT] 태그 1개 이상 포함 시 → MEDIUM으로 강등** (P13 신규)
        **단, corpus_mode=="dual" and secondary_corpus.status=="draft" → MEDIUM으로 강등** (P13 신규)
  MEDIUM: 일부 [REASONED] 또는 [UNCERTAIN] 포함, [FLAGGED] ≤ 1, 또는 DRAFT corpus 사용
  LOW: [UNCERTAIN]이 핵심 답변에 포함, 또는 [FLAGGED] ≥ 2
  FAIL → repair: 위 기준으로 Evidence 등급 계산 (draft corpus 강등 규칙 포함) → Mode 헤더에 추가

□ Knowledge-level 적응 템플릿 선택?
  PASS → user_domain_knowledge 수준에 맞는 템플릿 (novice/intermediate/expert) 사용 확인
  FAIL → repair: Step 0 Outward Profile의 user_domain_knowledge 재확인 → 해당 템플릿으로 전환

□ Product-scope doc 사용 시 [PRODUCT-SCOPE WARN] 포함?
  (product_docs ∈ loaded_docs 이면서 제품 직접 질문이 아닌 경우)
  PASS → 해당 수치 옆에 [PRODUCT-SCOPE WARN: {doc_id} — {product_name} 기반] 표시됨
  FAIL → repair: P-doc에서 가져온 수치 전체에 "[PRODUCT-SCOPE WARN: {doc_id} — {product_name} 기반. 다른 제품에 직접 적용 금지]" 추가

□ (Dual-corpus only) Secondary 팩트에 [X-GROUNDED:corpus.doc_id] 태그?
  SKIP if corpus_mode != "dual"
  PASS → secondary corpus에서 온 모든 도메인 팩트에 [X-GROUNDED:{secondary_corpus.name}.{doc_id}] 태그됨
  FAIL → repair: secondary corpus 사용 사실이 확인되는 팩트에 [X-GROUNDED:{secondary_corpus.name}.{doc_id}] 태그 추가
         (primary [GROUNDED:doc_id]와 혼용 금지 — corpus 출처 구분 필수)

□ (Dual-corpus only) Primary-Secondary 모순 없음 or [X-CONFLICT] 명시?
  SKIP if corpus_mode != "dual"
  PASS → 두 코퍼스 간 모순 없음, 또는 모순이 [X-CONFLICT:{primary_claim} vs {secondary_claim}]으로 명시됨
  FAIL → repair:
    1. Primary corpus와 Secondary corpus의 핵심 주장 대조 (예: WTP의 Gap_Intensity 공식 vs monetization의 가격 결정 변수)
    2. 상충하는 경우 → [X-CONFLICT: primary ({active_corpus.name}): {claim_A} | secondary ({secondary_corpus.name}): {claim_B}] emit
    3. 어느 쪽을 신뢰할지 기준 제시 (primary corpus > secondary in confidence; 단 domain specificity는 고려)

체크 실패 항목 → repair_action 즉시 실행 후 해당 섹션 교체. 모두 PASS → Stage 3 Deliver 진행.
```

---

## Output Format

**선택 원칙**: `user_domain_knowledge` 수준에 따라 위 "Stage 3 Deliver — Knowledge-Level Adaptive Output"의 3가지 템플릿 중 하나를 사용한다. 모든 템플릿 공통 필수 필드:

| 필드 | novice | intermediate | expert |
|------|--------|-------------|--------|
| **헤더** (Mode/Corpus/Scope/Evidence) | ✅ 간소 | ✅ | ✅ 전체 |
| **핵심 답변** | 비유 중심, 수식 없음 | 수식 + 직관 | 수식 + T3 한계 |
| **레벨 진단** | 텍스트 (가능성 높음/낮음) | 3행 테이블 | 확률% + 반증 조건 |
| **분석** | 자연어, 태그 간소 | 태그 전체 | 태그 + [CONFLICT/TRUST LIMIT] |
| **실행 함의** | 1-2개 단계 | P1-P3 테이블 | 전체 + 각 [UNCERTAIN] 조건 |
| **범위 주의** | 1줄 | 조건 리스트 | T4 HC 체크 항목별 |
| **진단 노트** | 생략 (novice에게 불필요) | [FLAGGED] 시만 표시 | 항상 표시 |
| **Corpus References** | 문서명만 | 문서명 + 섹션 | 문서명 + 섹션 + 충돌 정보 |
| **다음으로 물어볼 것** | 생략 (이 진단을 바꿀 수 있는 정보로 대체) | D1 Q번호 + 원문 (1-2개) | 우선순위 테이블 + Answer Drivers 정렬 |

**Evidence 등급 계산** (헤더 `Evidence: HIGH/MEDIUM/LOW`):
- **HIGH**: 핵심 답변 전부 [GROUNDED] + Stage 2 Q0-Q7 모두 [PASS]
- **MEDIUM**: 일부 [REASONED] 또는 [UNCERTAIN] 포함, [FLAGGED] ≤ 1
- **LOW**: [UNCERTAIN]이 핵심 답변에 포함, 또는 [FLAGGED] ≥ 2

**경쟁 이론 비교 트리거** — "(해당 시)" 정의: 아래 조건 중 하나 이상 해당 시 포함 (not optional):
- 질문에 "왜?", "원인", "이유" 포함 → VALUE GAP의 인과 설명과 JTBD의 인과 설명이 달라질 때
- 진단 결과가 L1-L2 (functional/commodity 지배) → JTBD가 더 적합할 수 있음을 명시
- Scope Gate 통과했으나 일부 조건이 애매한 경우 → JTBD/Mental Accounting 병행 가능성 제시
- Mode B/C에서 외부 검색 결과가 다른 이론 언급 시

**진단 노트 (adversarial trace)**: Stage 2에서 [FLAGGED] 발생 시 Output에 "### 진단 노트" 섹션 추가. 어떤 주장이 어떻게 수정됐는지 기록. novice 대상은 생략 가능 (혼란 방지). intermediate/expert는 [FLAGGED] ≥ 1 시 항상 포함.

**Mode C/D dispatch lineage**: L2 dispatch 결과가 Stage 1 직접 분석과 다른 결론을 냈을 경우, Example Flow의 `[DISPATCH-UPDATED]` 패턴으로 표시. Stage 2 [FLAGGED] 결과와 연결되면 진단 노트에 출처 명시.

```
### 진단 노트 (Adversarial)
[FLAGGED: Q2] 초기 draft에서 "프리랜서" = 고용 형태 기준으로 분류
→ 수정: ICP = identity portability 기준으로 교체 [T2]
[PASS: Q1, Q3-Q7]
```

**실행 함의 구조** (intermediate/expert 필수):
```markdown
### 실행 함의
| 우선순위 | 액션 | 근거 | 위험 / 조건 |
|--------|------|------|------------|
| P1 | [구체적 액션] | [GROUNDED/REASONED: doc_id] | [위험 또는 [UNCERTAIN] 조건] |
| P2 | ... | ... | ... |
```
novice는 번호 목록으로 대체 (1-2개만, 테이블 생략).

---

## Example Flow — /ameva -d

**질문**: "한국 프리랜서 디자이너가 L4인지 L5인지, 그리고 가격을 얼마로 잡아야 하나?"

**[Corpus Router]**:
```
signals: WTP, L4, L5, 프리랜서, 가격 → corpus: wtp 매칭
confidence = 4 / len(trigger) ≈ 0.67 ≥ 0.5 → corpus_mode = "corpus"
[CORPUS ROUTER] active=wtp | signals={WTP, L4, L5, 가격} | confidence=0.67
```

**[Corpus Pre-step]**:
```
분류: 진단 질문 (L1-L5) + 제품/실행 질문 (가격)
→ 로드: T2 (공식), T4 (스코프 게이트), D1 (인터뷰 프로토콜), P2 (가격 모델)
T4 스코프 게이트: 고관여? ✅ | 정체성인접? ✅ | 비독점? ✅ → IN
[ameva: Corpus Router → corpus=wtp | confidence=0.67 | docs={T2,T4,D1,P2} | scope=IN | ctx≈1200tok]
```

**[Step 0 — Outward Profile]**:
```
epistemic_basis: intuition-first ("L4인지 L5인지" — 명확한 기준 없이 질문)
causal_model: linear (레벨 → 가격 직접 연결)
user_domain_knowledge: intermediate (L4, L5 용어 사용, 但 분류 기준 자체를 모름)
gaps: no_data_anchor (인터뷰 데이터 없음)
→ frame: "What I'm noticing here" / emergent / "Does that match?"
→ depth: intermediate 템플릿 — 수식 제시 + 직관 연결, 레벨 테이블 표시
```

**[L1 Reframing (Mode D)]**:
```
[Refined Query]
Real question: 특정 디자이너가 identity portability 조건을 충족하는지 판단 +
               L4(명명된 전략 갭) vs L5(미명명 갭 + identity portable) 분기 기준
Corpus signals: D1 Q3, Q8이 핵심 — identity portable 여부가 L5 진단을 가른다
Paradigm signals: intuition-first / no_data_anchor
```

**[L2 Dispatch — value-gap-theory + corpus pass-through]**:
```
Skill('value-gap-theory', args=Refined_Query + corpus_context_block)
→ L3 vs L4 분기: D1 Q3 비유도 동료 비교 + 델타 관찰 여부
→ L4 vs L5 분기: D1 Q8 "내 이름으로 쌓고 싶다" 신호 + identity portability 체크

Dispatch conclusion change (vs Stage 1 직접 분석):
  Stage 1 초기 draft: "프리랜서 = L4 가능성 높음" (고용 형태 기반)
  value-gap-theory dispatch 결과: "ICP = identity portability, 고용 형태 무관 [T2]"
  → [DISPATCH-UPDATED: Q2 flagged in Stage 2] — draft 수정됨
```

**[Stage 1 Draft + SELF-RAG Validation]**:
```
Draft claim: "D1 Q3 신호 있으면 L4 가능성 높음"
  → [IsSup] 체크: D1 Q3 진단표 ("비유도 동료 비교 + 델타 분석" → L4) → ✅
  → [GROUNDED: D1, Q3 진단표]

Draft claim: "D1 Q8 신호 없으면 L4, 있으면 L4-L5"
  → [IsSup] 체크: D1 Q8 "내 이름으로 쌓고 싶다" → L5 포텐셜 신호 → ✅
  → [GROUNDED: D1, Q8]

Draft claim: "가격은 ₩49K 적합"
  → [IsSup] 체크: P2에서 L4 = ₩39K-59K → ✅
  → [GROUNDED: P2, L4 가격 구간]

Draft claim: "마케터는 L5가 많다" (LLM 지식)
  → [IsSup] 체크: corpus에 없음 → [UNCERTAIN] → 검증: D1 인터뷰 13문항으로 확인 필요
```

**[Stage 2 Adversarial — Q0 + WTP Sycophancy Checklist]**:
```
Q0 meta-sycophancy: 질문이 "프리랜서 디자이너 = L4" 전제? NO — L4인지 L5인지 묻는 것 [PASS: 전제 없음]
Q2: ICP = 고용 형태 아님 → 초기 "프리랜서" 분류 발견
  → [FLAGGED: Q2] "프리랜서" 고용 형태 기준 → identity portability 기준으로 교체 [T2]
Q1: 관심 ≠ WTP → 인터뷰 없이 WTP 단정 안 함 [PASS]
Q5: 스코프 게이트 역추적 → 디자인 서비스 자체가 commodity 아님 확인 [PASS]
Q6: 실험 미실행 → "검증됨" 없음 [PASS]
```

**[Pre-output Quality Gate]**:
```
□ T4 Scope Gate: IN ✅
□ 모든 도메인 팩트 태그: ✅ ([GROUNDED:D1], [GROUNDED:P2], [UNCERTAIN:마케터])
□ [UNCERTAIN] 검증 방법: ✅ (D1 13문항)
□ v1 폐기 주장 없음: ✅ (Q2 flagged → 교체 완료)
□ Stage 2 실행: ✅ (Q0+Q1-Q7 [PASS/FLAGGED] 기록 있음)
□ "검증됨" 없음: ✅
□ Corpus References: ✅
□ Evidence 등급: 핵심 답변 [GROUNDED] 대부분, [FLAGGED] 1개 → MEDIUM
□ Knowledge-level 템플릿: intermediate ✅
```

**[Output — intermediate 템플릿]**:

```markdown
## Ameva — 프리랜서 디자이너 L4/L5 진단 + 가격 설정
Mode: D | Corpus: T2, T4, D1, P2 | Scope: IN | Evidence: MEDIUM

### 핵심 답변
What I'm noticing here — D1 인터뷰 없이는 확정이 어렵지만, Q3 신호(비유도 동료 비교)가 있으면 L4 우세입니다.
L5는 Q8 "내 이름으로 쌓고 싶다" 신호 + identity portability 추가 확인이 필요합니다.
L4 진단 시 가격은 ₩39K-59K 구간, ₩49K 티어 권장. [GROUNDED: P2]

### 레벨 진단
| Level | 가능성 | 핵심 신호 | 근거 |
|-------|--------|----------|------|
| L4 | 높음 | Q3 동료 비교 + 델타 관찰 | [GROUNDED: D1, Q3] |
| L5 | 중간 | Q8 "내 이름으로" 신호 + identity portable | [UNCERTAIN] — 확인 필요 |
| L3 | 낮음 | Q5 미래 공포 부재 시 | [REASONED: D1] |
→ L4 우세, Q8 확인 후 L5 판정 가능

### 분석
- [GROUNDED: T2, ICP 정의] ICP 기준 = identity portability. "프리랜서" 고용 형태 아님.
- [GROUNDED: D1, Q3] 비유도 동료 비교 + 델타 분석 → L4 핵심 신호
- [GROUNDED: P2, L4 가격 구간] L4 = ₩39K-59K. ₩49K 티어 권장.
- [UNCERTAIN] 특정 디자이너의 identity portability 여부 → 검증: D1 Q7, Q8, Q9 인터뷰

### 경쟁 이론 비교
| 상황 | VALUE GAP | JTBD |
|------|-----------|------|
| 정체성 갭 인식 있음 | L4-L5, D(WTP) 높음 | self-development job |
| 기능적 도구 니즈만 | Functional_Gap, L2-L3 | output quality job |
→ 정체성 신호 있으면 VALUE GAP 우선

### 실행 함의
| 우선순위 | 액션 | 근거 | 위험 / 조건 |
|--------|------|------|------------|
| P1 | D1 Q3+Q8+Q9 인터뷰 (3문항) | L4/L5 확정 필수 | [UNCERTAIN] — 실행 전 레벨 단정 금지 |
| P2 | ₩49K Fake Door 실험 | [GROUNDED: P2] L4 구간 중간값 | WTP 미검증 상태 |
| P3 | identity portable 증거 확인 | L5 진단 게이트 | 고용 형태 아님 [T2] |

### 범위 주의 [T4]
고관여 + 정체성 인접 + 비독점 → IN. 충동 구매 또는 commodity 도구 교체 시 JTBD 사용.

### 진단 노트 (Adversarial) ← **항상 포함 (expert 템플릿 필수)**
[FLAGGED: Q2] Stage 1 초안: "프리랜서 디자이너 = identity portable 가능성 高 (고용 형태 기준)"
→ 교정 근거: ICP = identity portability, 고용 형태 아님 [T2]
→ Stage 3 적용: "외부 가시성 유무로 교체. 프리랜서 ≠ L5 자동 성립"
[PASS: Q0, Q1, Q5, Q6]

### Corpus References
[T2] wtp-theory-v2.md — D(WTP) 공식 v2 (ICP 정의, Gap_Intensity section)
[T4] wtp-falsification-criteria.md — T4 scope gate 3조건
[D1] wtp-interview-protocol.md — Q3 (동료 비교), Q8 (identity portability)
[P2] wtp-career-mirror-pricing.md — L4 가격 구간 ₩39K-59K

### 다음으로 물어볼 것
> **불확실성**: 특정 디자이너의 identity portability 여부 미확인 [UNCERTAIN]  
> → D1 Q8: "**본인 이름으로 뭔가 쌓고 있다는 느낌이 있어요, 아니면 지금 있는 데서 쌓고 있는 느낌이에요?**"  
> → 예상 신호: "내 이름으로 쌓고 싶은데 잘 안 된다" → L5 포텐셜 확인, 가격 구간 상향 검토 / 없음 → L4 유지, ₩49K 커밋 가능

*Does that match what you're sensing about this person?*
```

*(클로징 무브: epistemic_basis=intuition-first → "Does that match what you're sensing?" 적용 예)*

---

## Feedback Loop (매 세션 후)

/conceptual paradigm.json + nodes.jsonl 업데이트와 함께 아래 Ameva-specific 필드를 기록한다.

**Node 형식 확장** (기존 /conceptual 노드 호환):
```json
{
  "id": "{timestamp}",
  "content": "{insight}",
  "corpus_refs": ["{doc_id}", ...],        # 사용된 corpus docs
  "grounding_gaps": ["{UNCERTAIN claim}"],  # 검증 필요 항목
  "routing_outcome": {                     # P3 신규 — Corpus Router 개선 루프
    "corpus_selected": "{corpus_name or null}",
    "confidence": 0.XX,
    "corpus_mode": "corpus | generic",
    "outcome": "correct | incorrect | unknown",
    # incorrect: 사용자가 다른 주제로 수정하거나 corpus 내용이 off-target인 경우
    # unknown: 세션 내 명확한 피드백 없음 (기본값)
    "session_mode": "fresh | update",       # P4 신규 — 진단 업데이트 추적
    "update_turns": 0,                      # mode=update 회차 (0=fresh session)
    "closure_reached": false                # 진단 안정화 기준 달성 여부
  },
  "corpus_skill_history": [{"skill": "{name}", "corpus": "{corpus_name}", "score_delta": 0.XX, "iteration": N}],  # P9 신규 — corpus-aware routing history (MoA+RouteLLM basis)
  "gate_failures": ["{check_name}"],       # 어떤 Quality Gate check가 실패했는지
  "l2_bundle": "{dispatched skills}",
  "paradigm_tags": ["{tags}"]
}
```

**routing_outcome 활용** (장기):
- `outcome: incorrect`가 3회 이상 누적되고 동일 `confidence` 범위에서 발생 → confidence threshold 조정 검토
- `corpus_selected: null` (generic mode) 누적 → trigger 키워드 확장 후보 신호
- `gate_failures` 3회 이상 동일 check → 해당 오류가 Stage 1에서 더 일찍 방지되도록 corpus.sycophancy_checks 또는 core_theory에 예방 규칙 추가 검토
- `session_mode: update` + `update_turns` 분포 → 평균 진단 확정까지 몇 회의 follow-up이 필요한지 측정 → D1 질문 우선순위 최적화
- `closure_reached: false` 누적 → 진단 안정화 기준이 너무 엄격하거나 D1 질문 수 부족 → Evidence=HIGH 도달 경로 개선 검토

---

## Fallback & Error Handling

| 실패 모드 | 동작 |
|-----------|------|
| Corpus Pre-step: doc 읽기 실패 | core theory는 메모리에서 로드, 해당 doc [UNCERTAIN] 표시 |
| Corpus Router: no match | corpus_mode = "generic" → Step 0 직행, Grounding Wall relaxed |
| Corpus Router: confidence < 0.25 | corpus_mode = "generic" → Step 0 직행, routing_outcome.outcome = "unknown" |
| Dual-corpus: primary scope_gate FAIL | 전체 STOP — secondary도 중단. primary scope gate는 강제 조건 |
| Dual-corpus: secondary scope_gate FAIL | secondary만 skip → corpus_mode = "corpus" (primary only)로 downgrade |
| Dual-corpus: secondary doc 읽기 실패 | secondary 제외, primary single-corpus로 진행. [X-GROUNDED] 태그 생략 |
| Dual-corpus: context budget 초과 | secondary 경량화 (top 1 doc → 핵심 2줄만) → total ≤2000tok 유지 |
| T4 scope gate: 범위 외 | JTBD / Mental Accounting 제안, VALUE GAP 적용 안 함 |
| L2 매칭 없음 | `/ontolo-agent` fallback + 사용자 확인 |
| claim이 corpus에 없음 | [UNCERTAIN] 필수, 검증 방법 병기 — 절대 silent fallback 금지 |
| Follow-up 감지 오탐 (fresh 질문인데 update 판정) | 이전 진단 상태 없음 확인 → mode = "fresh"로 전환, 정상 파이프라인 실행 |
| Update output 후 추가 follow-up 없음 (대화 종결) | 남은 불확실성 섹션 생략 가능 — [UNCERTAIN] 없을 시 "Evidence: HIGH 달성" 메시지로 대체 |

---

## Quick Reference

| 목적 | 커맨드 |
|------|--------|
| 이론 질문 (WTP, L1-L5) | `/ameva [question]` |
| 진단 / 실행 계획 | `/ameva -d [question]` |
| 최신 한국 시장 데이터 + 이론 교차 | `/ameva -rd [question]` |
| 투자자 설명 / 전략 | `/ameva -rd [S5 참조 + 질문]` |
| 자율 무한 진화 | `/ameva -i [question]` |

### Diagnostic Session Lifecycle (인터랙티브 진단 흐름)

```
1. 초기 진단 요청
   "/ameva -d [대상 설명]"
     ↓
   Output: 레벨 진단 (확률%) + 실행 함의 + 다음으로 물어볼 것 (D1 Q번호 원문)

2. Follow-up 답변 (mode=update 자동 감지)
   "[D1 Q번호 답변 내용]" 또는 "Q8 물어봤는데요, ..."
     ↓
   Output: 업데이트된 레벨 진단 (이전→새 확률) + Evidence 변화 + 남은 불확실성

3. 반복 (남은 [UNCERTAIN] > 0)
   → 2번 반복 (최대 D1 13문항 범위)

4. 진단 확정 (종료 조건: Evidence=HIGH + [UNCERTAIN]=0)
   Output: "진단 확정 — [레벨] | Evidence: HIGH | 가격 커밋 가능"
```

**전형적 세션 길이**: 초기 진단 1회 + follow-up 2-4회 = 총 3-5 턴으로 L4/L5 확정 가능.

## Ameva vs Entity vs WTP-Validator

| | Ameva | Entity | WTP-Validator |
|--|--|--|--|
| 베이스 | Entity v2.0 | Entity | 독립 스킬 |
| Corpus Pre-step | ✅ 항상 | ❌ | ❌ |
| 도메인 | WTP/VALUE GAP | 범용 | B2B SaaS WTP |
| L2 dispatch | ✅ corpus-weighted | ✅ 동적 | ❌ |
| Claim grounding | [GROUNDED:doc_id] 필수 | OBSERVED/INFERRED | 없음 |
| Stage 2 Adversarial | corpus.sycophancy_checks 전 모드 주입 (Q0+Q1~Q7, P14) | 범용 6Q | 없음 |
| 사용 시 | WTP/VALUE GAP 모든 질문 | 범용 추론 | B2B WTP 검증 설계 |

## 새 도메인 Corpus 등록 가이드

Ameva Corpus Registry에 새 도메인을 추가하면 Corpus Router가 자동으로 발견한다. 별도 스킬 파일 불필요.

```yaml
# Corpus Registry에 추가할 corpus 블록 예시 (ameva/SKILL.md Corpus Registry 섹션)
# 필수 필드: name, domain, trigger, corpus_root, docs, taxonomy_groups, core_theory,
#            scope_gate, sycophancy_checks, gap_patterns, rwr_hints
# 제품 특화 docs 있을 경우: product_docs 필드도 추가 (없으면 생략)

#### corpus: legal  ← 이 형식으로 추가
name: legal
domain: "한국 형사법 / 증거법 / 헌법적 형사절차"
trigger: ["형사법", "증거", "판례", "공판", "수사", "변호", "피의자", "공소"]
corpus_root: /path/to/legal/docs/
docs:
  T1: criminal-procedure-act.md
  T2: evidence-law-principles.md
  P1: defense-strategy-playbook.md    # 특정 로펌/사건 유형 특화 → product_docs에 포함
  ...
taxonomy_groups:
  절차법: [T1, T2]
  증거법: [T3, T4]
core_theory: |
  형사소송 = 절차적 정의 + 실체적 진실 발견
  증거능력 < 증명력 (능력 없으면 법원 심리 대상 아님)
  무죄추정 원칙: 검사가 합리적 의심 없는 증명 책임
scope_gate:
  - "한국 법령 또는 판례와 관련된 질문인가?"
  - "형사 사건 (민사/행정 아님)인가?"
  - "현행법 기준 질문인가? (법 개정 후 구법 적용 제외)"
sycophancy_checks:
  Q1: "판례 없는 법리 주장을 '정설'로 처리하지 않았는가?"
  Q2: "민사 개념(손해배상 등)을 형사에 혼용하지 않았는가?"
  ...
gap_patterns:
  no_evidence_basis: "주장에 판례/조문 없음 → [UNCERTAIN] 필수"
  no_procedural_stage: "수사/기소/공판 단계 미명시 → 적용 가능 절차 불명확"
rwr_hints:
  "무죄 받을 수 있나": "증거능력, 증명력, 무죄추정 원칙, 합리적 의심 기준"
  "구속 취소": "구속적부심, 보석, 석방 사유"
product_docs: [P1]  # 특정 사건 유형/로펌 전용 전략 문서 — 다른 사건에 직접 적용 금지
                    # 없으면 이 필드 전체 생략 (필드 존재 자체가 선택 사항)
```

**등록 절차**:
1. Corpus Registry 섹션에 새 corpus 블록 추가
2. `trigger` 키워드 정의 (Corpus Router 자동 감지)
3. `status: draft` → 인간 검토 → `status: stable`
4. SkillsBench 기준: curated corpus +16.2pp vs self-generated -1.3pp — 검토 필수

---

## Anti-patterns (즉시 거부)

- corpus 인용 없는 도메인 팩트 emit → [UNCERTAIN] 필수 또는 corpus 먼저 확인
- "직장인은 L5 WTP가 낮다" → v1 폐기 주장. v2: ICP = identity portability
- "Social_Amplifier는 순수 곱셈" → v1 정의. v2: 신호 전달 계수
- T4 scope gate 없이 VALUE GAP 적용 → 충동구매/commodity는 이론 밖
- "검증됨" emit (실험 미실행) → V1/V2는 설계 단계, 아직 미실행

### Anti-pattern Gallery — Stage 2가 잡는 실제 실패 시나리오

아래 5개 실패 패턴은 Stage 2 Adversarial에서 반드시 검출되어야 한다. 각 시나리오는 [FLAGGED] 처리 경로를 명시한다.

**AP-1: 고용 형태 ICP 분류**
> 입력: "프리랜서 디자이너라서 이동성이 높을 것 같아요"
> 실패 draft: "프리랜서 → identity portable 높음"
> Stage 2 Q2 → [FLAGGED: Q2] ICP = identity portability, 고용 형태(프리랜서/직장인) 무관 [T2]
> 수정: identity portable 여부는 D1 Q7-Q9로 독립 확인 필요

**AP-2: 인터뷰 없는 WTP 단정**
> 입력: "이 사람 완전 L5 같은데 가격 얼마가 좋을까요?"
> 실패 draft: "L5이므로 ₩69K 이상 권장"
> Stage 2 Q1 → [FLAGGED: Q1] 인터뷰 데이터 없이 WTP 단정 금지 — enthusiasm ≠ WTP
> 수정: D1 Q3+Q8+Q9 확인 전 레벨 미확정. [UNCERTAIN] 처리 후 실행 함의 conditional로 제시

**AP-3: 스코프 외 VALUE GAP 적용**
> 입력: "포토샵 플러그인 교체 결정에서 WTP 어떻게 볼까요?"
> T4 scope gate → 저관여 도구 교체 = 충동/functional 구매 → IN 조건 실패
> → [SCOPE OUT] VALUE GAP 적용 불가. 이 케이스는 JTBD (output quality job 교체) 사용.

**AP-4: v1 폐기 주장 노출**
> 입력: "직장인은 L5 WTP가 낮지 않나요?"
> 실패 draft: "맞습니다, 직장인은 L5 WTP가 낮습니다"
> Stage 2 Q3/Q4 → [FLAGGED: ANTI-PATTERN v1] 이 주장은 v1에서 폐기됨. v2 기준: ICP = identity portability (고용 형태 무관) [T2]

**AP-5: 실험 미실행인데 "검증됨" 발언**
> 입력: "Social Amplifier 효과가 검증됐나요?"
> 실패 draft: "V1 실험에서 검증됨"
> Stage 2 Q6 → [FLAGGED: VALIDATION CLAIM] V1/V2 실험은 설계 단계 — 아직 미실행. "검증됨" 발언 금지.
> 수정: "V1 실험은 설계됐으나 미실행. [UNCERTAIN: 실험 결과 없음]"

**AP-6: 제품 특화 수치를 일반 이론값으로 제시** *(C4 failure mode — product_docs 오용)*
> 입력: "5년차 UX 디자이너 가격 얼마로 잡아야 하나요?"
> 실패 draft: "L4 기준 ₩39K-59K/month 권장 [P2]"
> Stage 2 Q1 → [FLAGGED: PRODUCT-SCOPE WARN] P2는 Career Mirror 제품 전용 가격 데이터 —
>   "5년차 UX 디자이너"는 Career Mirror 질문이 아님. ₩39K-59K는 이론 예측값이지 산업 기준값 아님.
> 수정: "[PRODUCT-SCOPE WARN: P2 — Career Mirror 기반 이론 예측값]
>        이론 적용: L4 identity-gap 고관여 제품의 일반 WTP 구간 = ₩30K-70K (컨텍스트 의존).
>        실제 가격 설계 시 D1 인터뷰 + 해당 제품 맥락의 Fake Door 실험으로 독립 검증 필요."

---

