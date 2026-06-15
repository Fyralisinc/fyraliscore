# Think Cost-Optimization Plan

> **Status:** Proposed — not started. **Date:** 2026-06-11.
> **Source:** multi-agent cost audit of the Think pipeline (8 subsystem readers,
> 3-lens proposal panel, adversarial per-proposal verification, completeness
> critique). 10 proposals confirmed, 3 rejected.
> **Hard constraint:** cost goes down; reasoning performance does not regress.
> Every quality-affecting change ships flag-gated, with the storyline benchmark
> and the think quality replay cases as gates (see §9).

---

## 1. Baseline — where the money goes

Per full-schema Think run (code-verified sizes; ~13k input tokens total is an
*estimate*, not telemetry — no per-call token counts are recorded today):

| Component | Size | Source |
| --- | --- | --- |
| Static system prompt (`_SYSTEM_PROMPT`) | 20,150 chars ≈ 5.0–5.6k tok | `services/reasoning/think/prompt.py:49` (claims-only variant: 8,314 chars) |
| Machine schema, sent **again** every call | 12,983 chars ≈ 3.5–4.3k tok | DeepSeek strict tool params (`strict_schema.py:476-495`); non-strict paths append an 8,955-char Pydantic hint to the system message (`lib/llm/provider.py:1113-1117`) |
| Dynamic context, budgeted | obs 4k + models 4k + acts 12k + resources 1k chars | hardcoded `prompt.py:37-46`, not env-configurable |
| Dynamic context, **unbudgeted** | worst case tens of k chars | relationship_candidates, inquiry packet (~20k), actors — no aggregate caps |
| Output | ≤ 2,048 tok (claims-only 1,024) | `llm_reason.py:61`; `reasoning_trace` is a required string with no length cap |
| Inquiry question-planning | up to 2 × ~900-tok LLM calls per **T1** run | `context_planner.py:121` hardcodes `mode="deep"`; T1-only (non-T1 uses seeded retrieval, `inquiry.py:2093-2111`) |
| Retry multiplier | worst case 4 model calls per `llm_reason` | parse-repair re-sends the full prompt (`provider.py:998-1024`); DeepSeek strict adds a JSON-mode rescue loop |

### Root defects

1. **Resolved: cache-friendly layout is now unconditional.** `build_prompt`
   keeps the static base plus per-trigger-kind operating instructions in the
   system message, while the dynamic per-trigger reasoning profile leads the
   user message. This gives provider prefix caches a stable system bucket per
   schema/trigger-kind class. The Anthropic path still sends a plain
   `system=` string with no `cache_control` (`provider.py:1109-1126`), so
   explicit Anthropic cache-control remains a provider-switch readiness item.
2. **The output contract is paid 2–3×.** Full prose JSON-shape spec in the
   system prompt *plus* the machine schema as tool params/hint, *plus* ~2.6k
   chars of edge rules restated inside `<retrieval_priority>` on every
   graph-anchor call (`prompt.py:1333-1398`) and again in
   operating_instructions (`prompt.py:1686-1707`).
3. **No value-based routing.** One main Codex model for Think reasoning; every
   ingested signal → one full-price inferential run. A cost-triage router
   (`routing.py decide_route`) was built and deleted; T1 trigger batching is
   built but disabled; a per-role cheap-model seam
   (`select_question_planning_provider`) exists and is Codex-only.

Production config: `LLM_PROVIDER=codex`; Think main uses `CODEX_MODEL`, and
question planning uses `INQUIRY_CODEX_QUESTION_MODEL`.
Savings compose **multiplicatively**: call-count reductions delete calls;
per-call reductions shrink the survivors. Do not sum headline percentages
(see §8).

---

## 2. Phase 0 — make spend measurable (prerequisite)

Saves ~$0 itself; everything else is unverifiable without it.

**0.1 Cost-ledger fixes** (`lib/llm/provider.py`, `services/reasoning/think/observability.py`):

- Record cache tokens: extend `_extract_openai_usage` (`provider.py:237-263`)
  to capture `usage.prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`
  (DeepSeek/OpenAI) and `_extract_anthropic_usage` (`provider.py:227-234`) for
  `cache_read_input_tokens` / `cache_creation_input_tokens`; add cache-tier
  pricing to `MODEL_PRICING` so `compute_cost_usd` (`provider.py:109-120`)
  stops pricing all input at full rate. (This was mis-rejected during
  verification — the rejection contained only confirmations. Reinstated here:
  it is the only way to verify Phase 1.)
- Update stale `MODEL_PRICING` (`provider.py:72-90`): `claude-opus-4-7` is
  $5/$25 per MTok, not the coded $15/$75; deepseek-chat has been repriced.
  All absolute-$ figures in this plan inherit ~2–4× staleness in the same
  direction; percentage claims survive.
- Add a per-call **purpose** dimension (main reasoning vs question-planning vs
  parse-repair) to `think_run_costs`; today planning and repair calls are
  indistinguishable from the main call.
- Thread real `transaction_retry_count` / `rerun_count` into
  `ThinkRunOutcome` and replace the hardcoded `retry_count=0` at
  `reason.py:443`. Do **not** use `LLMUsageAggregator.call_count` (conflates
  planning calls with retries).
- Fix the silent `(0, 0)` usage fallback so unbilled-looking calls are visible.

**0.2 Sizing queries** (run against the demo/production DB — local dev DB is
empty; these gate Phase 2 items):

1. Burst share: same-tenant clusters of ≥20 T1 `event_arrival` rows within
   30s over `think_trigger_queue` / `observations.enqueued_at` → gates 2.3.
2. `SELECT count(*), sum(llm_cost_usd) FROM think_run_costs WHERE
   trigger_kind = 'T4:model_reeval'` → gates 2.1.
3. Validation-failure waste: cost over triggers with repeated
   `validation_failure` rows + batch-parent dead-letter count → gates 2.4.
4. Transaction-retry / `OutOfRegion` rerun rates → gates 2.2.
5. Per-outcome spend breakdown (success / failed / validation_failure /
   reasoning_exhausted) → overall prioritization.

---

## 3. Phase 1 — per-call input cost (low risk, unconditional)

### 1.1 Cache-friendly prompt layout — unconditional

The single highest-leverage input-cost change. Token content is largely
preserved, with static and dynamic sections positioned for provider prefix
cache reuse.

- Move the static per-trigger-kind operating_instructions block
  (`prompt.py:1595-1714`) from the end of the user message into the system
  message after the base → one stable prefix per
  (schema-variant × trigger-kind) bucket; DeepSeek caches each bucket
  independently and on the strict path the tool schema renders into the
  chat-template prefix, so it likely shares the cached prefix.
- `provider.py:1092-1140` (`AnthropicProvider._raw_call`): send `system` as a
  content-block list with `cache_control: {type: ephemeral}` on the static
  block (system text **and** schema_hint together, so the claims-only variant
  clears Anthropic's minimum cacheable prefix). Free readiness if the provider
  ever switches to Claude (~0.1× read price).

**Verified savings:** post-fix stable prefix 5.7–9.7k of ~13k input tokens
(44–75%); at DeepSeek's ~74% cache-hit discount and 50–80% hit rate →
**16–44% of input cost**. Input is 61–80% of call cost.
**Risk:** ~none. **Gates:** replay harness + storyline benchmark, byte-diff of
assembled prompts under the flag. **Verify via** Phase 0.1 cache-token fields.

### 1.2 Send the output contract once — flag `THINK_STRICT_LEAN_PROMPT`

- Expose a provider capability `uses_strict_tool_schema(schema)` (reuse
  `_deepseek_supports_strict_tool_calling`, `provider.py:1954-1965`); thread
  it through `llm_reason.py:71-81` into `build_prompt`.
- When strict: select a shape-light system base that keeps **one** worked
  end-to-end example and **all** semantic rules (falsifier quality bar, scope
  discipline, when-to-emit guidance) but deletes field-enumeration prose the
  tool schema enforces (~3.5–4.5k chars). Non-strict paths keep full prose.
- Collapse the `<retrieval_priority>` rule prose (`prompt.py:1333-1398`,
  2,644 chars) to a ~2-line pointer (dynamic ID lists stay); same for the
  operating_instructions restatements at `prompt.py:1686-1707`.

**Verified savings:** ~1.2–2k tok/call on the *uncached* share (overlaps 1.1 —
count multiplicatively). **Risk:** moderate — the strict schema is a deliberate
*subset* of RawDiff: `edge_kind` is pattern-validated, not the 16-kind enum
(`strict_schema.py:401`), and weight-nullability is prose-only. The lean
variant must keep prose for everything the schema does not enforce.
**Gates:** benchmark + per-trigger-kind validator drop-rate comparison.

### 1.3 Aggregate caps on unbudgeted prompt sections

New env-overridable constants beside `prompt.py:37-46`:
`_CANDIDATES_CHAR_BUDGET=12000` (drop whole tail candidates into the existing
"N more omitted" marker at `prompt.py:792-798`),
`_INQUIRY_PACKET_CHAR_BUDGET=8000` (tiers are already priority-ordered; drop
`omission_ledger` / `supporting_groups` first, never truncate mid-item),
`_ACTORS_CHAR_BUDGET=1200`. Skip the proposed topology cap —
`assembler.py:879` hardcodes `topology_context=None`; it is a dead field.

**Verified savings:** tail protection (~10–15% of calls average +2.5k tok
today). **Risk:** moderate but bounded — budget-dropped T4 candidates remain
`pending` and are deferred, not lost; verify the requeue path before shipping.

---

## 4. Phase 2 — call count and retry waste (sizing-gated)

### 2.1 Deterministic routing for T4 `model_reeval` — flag `THINK_DETERMINISTIC_MODEL_REEVAL`

The deterministic handler already implements the subkind; it is simply not
authoritative for it. Add `model_reeval` to the T4 authoritative tuple
(`deterministic.py:64-69`) for **mechanical cause_kinds only**
(`supporting_archived/deprecated/superseded`,
`contributor/pattern/instance_archived`); keep the LLM path for
`contested_cluster` and `falsifier_triggered_upstream`.

Rollout: `shadow` mode for 2 weeks (compute the deterministic diff alongside
`llm_reason`, log both via `debug_capture` / `think_run_artifacts`, diff
offline) → `on`. Record the decision in `docs/adr/` — the original exclusion
rationale lives only in the lost THINK-DESIGN-AUDIT.

> Correction from verification: a T4 run makes exactly **one** LLM call
> (planning is T1-only), so this eliminates 100% of the class's LLM spend.

### 2.2 Stop re-buying identical results — flag `THINK_REUSE_DIFF_ON_TX_RETRY`

- **Step 1 (trivial, do regardless):** real retry counts into the ledger
  (folded into Phase 0.1).
- **Step 2 (~30 lines):** extract the already-applied SELECT
  (`applier.py:158-167`) into `check_already_applied(conn, trigger_ref)`;
  call it at the top of `think()` (before the aggregator install,
  `reason.py:222`); on hit return a `skipped_idempotent` outcome with its own
  out-of-band `think_runs` insert.
- **Step 3:** on `DeadlockDetected`/`SerializationError` retry only, hash the
  rebuilt ContextBundle (model ids+versions, observation ids, trigger
  payload); if unchanged, reuse the prior `raw_diff` and jump to
  validate/apply (validation re-runs against fresh DB state in the new tx).
  `OutOfRegionError` retries always re-reason. Invalidate on OutOfRegion or
  >50% dropped ops.
  *Caveat from verification:* rebuilding the bundle re-runs retrieval +
  planning; diff-reuse skips only the main `llm_reason` call (the dominant
  one, ~55% of pass cost) unless the response cache also covers planning.
- **Step 4:** wire the existing module-level response-cache hook
  (`provider.py:778-792`) for byte-identical queue re-attempts — **only after
  2.4 ships** (see interaction C5, §8).

**Verified savings:** ~5–8% of Think spend at plausible retry rates
(sizing query 4). **Risk:** low.

### 2.3 Enable T1/downstream trigger batching (built, default-on)

`THINK_T1_BATCH_WINDOW_S=30`, `THINK_T1_BATCH_MIN_SIZE=20`,
`THINK_DOWNSTREAM_BATCH_WINDOW_S=60`. Single signals keep today's exact path
after the window expires; genuine bursts coalesce first. Operators can still
force immediate single-signal processing with `THINK_T1_BATCH_WINDOW_S=0` and
`THINK_DOWNSTREAM_BATCH_WINDOW_S=0`.

Pre-work, in order:

1. Burst-share measurement (sizing query 1) — decides whether this is worth
   anything at all.
2. **Fix the dead-letter unbundle amplifier first:** one failed 30-member
   batch currently re-enqueues up to 30 members × 5 attempts
   (`worker.py:1663`, unbundle branch `:1706-1723`). Stamp members
   `unbatched_from: parent_id` and exclude them from both
   `_create_t1_batch_rows` candidate queries (`worker.py:941-955, 962-987`)
   so they can never re-batch; add a test beside
   `test_t1_batch_terminal_failure_releases_member_triggers`.
3. Add an unbatched arm to `scripts/run_storyline_batch_benchmark.py`
   (a `window=0` drain path) so a batched-vs-unbatched A/B is runnable, and
   produce the same-config variance band first (CAPABILITY-PLAN task A3 —
   the 0.58/0.79 same-day runs were under config changes; no variance bound
   exists yet, so no delta is interpretable without one).

**Verified savings:** 95–96.7% per qualifying burst (B calls → 1);
portfolio = burst-share × that. **Risk:** moderate — batch members reach the
reasoner as 280-char truncated lines under a 2,000-char summary cap
(`worker.py:1136-1143`); the A/B gate is mandatory, rollback is an env knob.

### 2.4 Escalation ladder on validation no-survivors (structural)

Replace 5 blind same-model resamples with: cap validation-failure retries at
2 (`THINK_VALIDATION_MAX_ATTEMPTS`); attempt 2 re-runs `llm_reason` with the
validator's `dropped_op_errors` appended to the user message and an
escalation model from `THINK_ESCALATION_MODEL`; escalate at most once per
trigger; exempt escalated batch parents from member unbundling unless the
escalated attempt also fails. `quality_report` already joins outcomes to
`model_name`, so the escalation model is measurable from day one.

Implementation: classify `outcome.exception` in `worker.py` `_run_trigger`
(`:1554-1571`), pass a `failure_class` into `_mark_trigger_failed` (`:1663`),
persist validator feedback into the queue-row payload.

**Verified savings:** conditional on no-survivor rate (sizing query 3); per
persistent failing trigger, ~5 calls → ~3 with a better chance of success.
**Risk:** low — happy path untouched; feedback-append also changes the prompt
bytes, which is what makes 2.2's response cache safe (§8, C5).

### 2.5 Cut T1 question-planning spend (audit gap A1 — largest unconditional leftover)

Up to 2 × ~900-tok planning calls ride on **every** T1 run; `mode="deep"` is
hardcoded at `context_planner.py:121` and the calls repeat on every
OutOfRegion rerun and tx retry. Levers, smallest first:

- Keep the existing `select_question_planning_provider` seam
  (`services/platform/execution/question_planning_provider.py`) Codex-specific:
  it downgrades planning to a dedicated low-effort Codex model while Think main
  stays on the configured Codex model.
- Reduce default planning rounds 2 → 1 for low-value T1 classes; fast mode
  for sparse-context triggers.
- Reuse planning output across tx retries (pairs with 2.2 step 3).

**Risk:** low-moderate; gate on retrieval-quality metrics
(`unused_selected_context_rate`, context coverage in
`quality_report.py:332-376`).

---

## 5. Phase 3 — guardrails and insurance (~$0 steady-state, caps incidents)

### 3.1 Misconfiguration footguns + R3 daily spend ceiling

- `LLMConfig.from_env` (`provider.py`): default to Codex and keep production
  env files explicit with `LLM_PROVIDER=codex`, `CODEX_MODEL`, and
  `CODEX_REASONING_EFFORT`.
- Log provider/model/$-per-MTok at `build_provider` (`provider.py:1989`).
- Cap Codex transport output — it currently drops `max_tokens` entirely
  (`provider.py:1327,1358`).
- R3 ceiling: per-tenant `LLM_DAILY_BUDGET_USD_PER_TENANT` checked at the
  **worker dispatch point** (not inside `LLMProvider.structured`); on breach
  the ThinkWorker pauses dispatch for that tenant (triggers wait in queue,
  attempts budget intact, resume next day) and alerts — never dead-letters.

A misconfigured-Opus incident is a real ~19× cost ratio (at *corrected*
prices), ~$200/day excess at current volume.

### 3.2 Cascade lineage budget

`propagate_cascade_depth` (`cascade.py:69-92`) exists but is not threaded
through every Think-spawns-Think enqueue site: applier's T2 enqueue
(`applier.py:622-631` → `cascade.py:603-654`), context_planner's T3 emission
via `trigger_emitter.py`, and topology's insert-time T4 (`field.py:832-879`).
Thread the parent payload through all three; add
`THINK_MAX_INFERENTIAL_LINEAGE_DEPTH` (default ~5) enforced at the existing
worker rejection point, emitting a visible `cascade_bound_violation` rather
than a silent drop.

> Correction from verification: `reason.py:1106` records **intra-run BFS
> depth**, not cross-trigger lineage — no lineage distribution exists to set
> the bound from. Ship the threading first, observe the new lineage metric,
> then enable the bound. Largely redundant with 3.1's ceiling as incident
> insurance (§8, C6); retains independent value as queue-starvation
> prevention.

---

## 6. Phase 4 — higher-risk compression (do last)

### 4.1 Short per-call ID aliases (m1/o2/a3) — flag `THINK_ID_ALIASES`

Replace ~50–70 full UUIDs (~17 tok each) in the prompt and 10–20 in the
output with short aliases + a per-section legend; expand back to UUIDs before
validation. ~600–900 input + 150–300 output tok/call (~8–12% post-caching).

**Why last:** the entire reasoning contract anchors on UUID citation
(cite-selected-UUID discipline `prompt.py:72-74`, edge-endpoint rules
`:305-310`), and verification found the proposed expansion point is wrong —
Pydantic validation happens *inside* the provider
(`provider.py:969,1067,1915`), so this needs alias-tolerant output models
(`RawDiffAliased` in `diff_schema.py`, registered in `_strict_schema_for`,
`provider.py:1968-1986`) rather than post-hoc expansion in `llm_reason`. Both
strict schemas embed the UUID pattern (13 occurrences in claims-only).
Validator/repair feedback is generated in UUID space and must be mapped back
to alias space or retry quality drops (§8, C9). Never relax `RawDiff` itself.

---

## 7. Structural backlog (beyond the verified set)

| Item | Sketch | Trigger to act |
| --- | --- | --- |
| Tiered model routing by trigger kind | Claims-only / sparse-context / T4-latent / batch-summarization runs on a cheap model; frontier only for graph-anchor full diffs. A/B machinery exists (quality gates + `model_name` join). 2.4's ladder is the reactive half; this is the proactive half. | After Phase 0 telemetry shows per-class quality headroom |
| T1 triage gate | Resurrect deleted `routing.py decide_route`: trivial signals → claims-only or no LLM. Today a calendar-accept costs the same as a strategic email. Steady-state trickle is untouched by batching. | After Phase 1; biggest remaining unconditional lever with 2.5 |
| Dynamic schema subsetting | Strict schema ships 11 proposition variants + a 17-nullable-field recommendation payload even when the trigger's op budget forbids those ops; claims-only is only ~23% smaller because of it. Subset per trigger kind. **Bound the variant count** — each variant is a separate cache bucket (interacts with 1.1). | With/after 1.2 |
| Output budgets from op budgets | Derive per-trigger `max_tokens` from `reasoning_frame` op budgets (e.g. T2: ≤2 claim_ops); cap `reasoning_trace` length; add stop sequences. Output is billed at ~4× input. | Anytime; small |
| Delta/region context reuse | Full retrieved context is rebuilt and re-sent at full price every run; no region-level coalescing of distinct back-to-back triggers. | After 1.1 telemetry shows dynamic share dominating |
| Context-use feedback loop | `context_use.py` computes `unused_selected_model_ids` per run but never feeds back into assembler/prompt budgets ("value-per-token compiler", endorsed in inquiry-retrieval-gap-analysis.md:211-214). `_MODELS_CHAR_BUDGET=4000` binds (24 selected, ~7–18 fit) while the 12k acts budget never binds. | After Phase 0 |
| Batch API (50% off) | Every Think path is async with nobody waiting — textbook batch fit — but DeepSeek has no batch API. **If the provider ever moves to Anthropic/OpenAI, this + prompt caching is the dominant stack.** | On any provider switch |
| Entity-resolver fan-out | 1 LLM call per unresolved phrase (≤50/observation) + an extra T1 full run per high-confidence late resolution (`entity_resolver/worker.py:586-642`). Deployment status uncertain (not in process manifest). | Size first, same treatment as 2.1 |
| Greeting render half of R3 | ~15 surfaces re-rendered every 15 min/tenant with no content-hash skip. Outside the Think module but inside the documented R3 decision. | Separate workstream |
| Demo overlay augmentor | `hooks.py:1-75` full-ledger overlay silently inflates every prompt on demo deployments. | Check before demo.fyralis.xyz benchmarking |

---

## 8. Interaction rules — how to count savings and sequence

- **C1 (1.1 × 1.2):** both monetize the same static tokens. 1.2 deletes some;
  1.1 discounts the survivors. Joint effect = multiplicative, and 1.2's lean
  split doubles 1.1's cache-bucket count (lower per-bucket hit rate). Ship
  1.1 first, measure, then 1.2.
- **C4 (per-call × per-count):** portfolio total =
  (1 − batch/triage reduction) × (1 − per-call reduction). Never sum.
- **C5 (2.2 × 2.4):** an exact-match response cache replays the *same invalid
  diff* on validation-failure re-attempts — zero cost, zero progress. 2.4's
  feedback-append changes the prompt bytes and is the cache-bypass condition.
  **Sequence 2.4 before (or with) 2.2's response cache.**
- **C6 (3.1 × 3.2):** same incident insurance; don't add their
  expected-loss-avoided.
- **C7 (2.3 × dead-letters):** batching activates the unbundle amplifier;
  the fix in 2.3 pre-work is mandatory, and 2.3's rollout gate must watch
  dead-letter rate.
- **C8 (2.1 × 3.2):** deterministic model_reeval removes the main
  cascade-loop incident class; don't count both.
- **C9 (4.1 × 2.2/2.4):** repair/escalation feedback must be translated to
  alias space under 4.1.

---

## 9. Verification protocol (applies to every quality-affecting change)

1. **Storyline benchmark** (`scripts/run_storyline_batch_benchmark.py`):
   baseline $10.92/10k signals, CI 0.8879. Prerequisite: same-config variance
   band (CAPABILITY-PLAN task A3 — 3× identical runs) before reading any
   delta.
2. **Think quality replay cases** (`quality_report.py`, exposed via
   debug_router): per-trigger-kind `flagged_success_rate`,
   `unused_selected_context_rate`, context coverage.
3. **Flag-gating + instant rollback** via env knobs; shadow mode where the
   output could differ (2.1, 2.4).
4. **Ledger-verified savings** (Phase 0.1): cache-hit tokens, purpose
   dimension, real retry counts — claimed savings must show up in
   `think_run_costs`, not in estimates.

---

## 10. Rejected during verification (do not re-litigate without new evidence)

| Proposal | Why rejected |
| --- | --- |
| Skip planning + deep retrieval on deterministic routes | Refuted: an early-return (`inquiry.py:827-837`) already zeroes planning rounds for the fast/deterministic set; the claimed waste does not occur. |
| DeepSeek off-peak window deferral | The off-peak discount window was discontinued. The batching half survives as 2.3; Batch API survives as a provider-switch contingency (§7). |
| Cache-token ledger recording | Rejection was misfiled (verdict contained only confirmations, no refutation). **Reinstated as Phase 0.1.** |

---

## 11. Expected outcome

With Phase 0 + Phase 1 alone: **~16–44% input-cost reduction** (cache layout)
plus ~1–2k tok/call contract dedup on the uncached share, at near-zero quality
risk. Phases 2–4 add call-count reductions that are burst-/rate-conditional
and must be sized first (sizing queries, §2.2). All dollar figures inherit the
stale-pricing caveat until Phase 0.1's `MODEL_PRICING` fix lands.

> **Production provider/model note:** the app path is Codex. Decide the
> escalation model for 2.4 only within the Codex model family.
