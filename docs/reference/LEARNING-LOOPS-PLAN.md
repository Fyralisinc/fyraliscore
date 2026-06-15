# Learning Loops: Structural Diagnosis & Closure Plan

**Date:** 2026-06-12
**Source:** Multi-agent architecture review (9 subsystem maps → 6 verified root-cause
diagnoses → 3 independent designs → adversarial synthesis), grounded against run
`tests/real_llm/reports/runs/storyline-5b-lifecycle-loops-rerun-20260612T0159NPT`.
All file:line claims below were verified against the working tree on 2026-06-12.
**Companion plans:** extends CAPABILITY-PLAN Tranche C and ARCHITECTURE-REPAIR-PLAN
Moves 4/5; constrained by THINK-COST-PLAN/ROLLOUT ($10.92/10k-signal baseline,
flag-gated default-off, no per-trigger LLM-call additions without sizing gates).
See `LEARNING-LOOPS-SIMPLIFICATION-PLAN.md` for components to delete, merge, or
simplify while implementing this architecture.

---

## 1. The structural diagnosis

The six weak evals do not share a single missing feature. They share a **shape**:

> Every durable object in the system is **born terminal**. It gets exactly one
> chance to exist — the initial T1 LLM pass — and once written, no clock, queue,
> trigger, or consumer ever revisits it. Meanwhile, nearly all of the machinery
> that *would* revisit it is already built, unit-tested, and **launched by nothing**.

Three layers of the same failure, one per design lens:

1. **No temporal spine** — Think is purely reactive (signal in → diff out).
   Time-based revisiting exists only for predictions via `models.evaluate_at`,
   and the sole worker that polls it (DeadlineResolver) has no entrypoint.
2. **No lifecycle contracts** — objects lack state machines with transition
   owners. Predictions can't come due (NULL `evaluate_at`), edges are write-once
   (mutual exclusion *forbids* supports→weakens upgrades), evidence-attachment
   is not an expressible op, forecasting is optional-and-sometimes-forbidden.
3. **No feedback consumption** — every outcome is durably recorded
   (reader-decision attributions, neutralized ops, Approve/Dismiss, edge usage,
   prediction resolutions) and **nothing consumes any of it** into future behavior.

The dormant-but-built inventory (all verified):

| Built, tested, never launched | Where | Missing piece |
|---|---|---|
| DeadlineResolver (T2 `prediction_overdue` producer) | `services/workers/deadline_resolver/worker.py:213` | no compose service, no script, no app entrypoint |
| MaintenanceScheduler (hourly decay, daily decay-archival, weekly sweep) | `services/workers/maintenance/scheduler.py:103` | instantiated only in tests |
| OutcomeEvaluator (sole emitter of question-policy credit events) | `services/reasoning/sage/outcome_evaluator.py:432` | zero production callers (two stress scripts only) |
| `propagate_consequence` prompt (MUST-emit create-commitment rule, `act_ops:1` budget) | `services/reasoning/think/prompt.py:1599-1638` | unreachable: T2 `belief_updated` routes to a deterministic no-op (`applier.py:230-236` `_T2_REEVALUATION_KINDS={'prediction'}`) |
| Residual prediction matcher | `services/reasoning/sage/model_predictions/residual.py` | zero production callers; live path uses a UUID-substring placeholder (`deterministic.py:183-191`) |
| Candidate promoter `promote_high_confidence_edges` | `services/reasoning/relationships/promoter.py:25` | sole caller is `run_weekly`, which nothing runs |
| Evidence-attachment machinery `_append_observe_reading` | `services/reasoning/think/applier.py:1341+` | not an expressible ClaimOp (`diff_schema.py:42` is `insert\|update\|archive`) |
| Question-policy READ path (applies budgets at inquiry start) | `inquiry.py:3344-3450` | table `sage_question_policy_stats` has no production writer |
| Calibration read side `apply_calibration` | `validator.py:910` | `calibration_updater` only called from never-run `run_weekly` |
| `_EDGE_KIND_ENUM` (16 registered kinds) | `strict_schema.py:361` | dead code; live schema validates a snake_case regex (`:401`) |
| `contested_count` on edges | `edges_repo.py:120` | SELECT-only; no writer anywhere |

---

## 2. Verified root causes per loop

Layer legend: **harness** = eval never exercised it; **trigger** = code exists,
nothing invokes it; **temporality** = no clock/queue to revisit; **prompt/schema** =
LLM never asked or actively prevented; **data-model** = object/state/op missing;
**feedback** = recorded but never consumed.

### 2.1 Prediction lifecycle — 0.04 (= 0.35 × 1/9 exactly)

- **[prompt/schema]** Predictions are optional taxonomy under a minimality bias
  (`prompt.py:86-91,187`), and the claims-only compact pass **bans** them
  ("Do not emit … predictions in this pass", `prompt.py:372-376`) — auto-selected
  exactly on cold-start/sparse bundles (`llm_reason.py:156-167`), with
  `new_predictions=[]` forced in coercion (`llm_reason.py:225`).
- **[data-model]** Date-slot mismatch: the prompt's only blessed date slot is
  `falsifier.prediction_deadline.evaluate_at` (`prompt.py:100`), but
  `_infer_evaluate_at` (`prediction_lifecycle.py:232-260`) never reads
  `falsifier["evaluate_at"]` → LLM predictions land with `evaluate_at NULL`,
  invisible to the due-sweep (`repo.py:2851-2852` requires `IS NOT NULL`).
- **[trigger]** DeadlineResolver never started by anything; no
  `prediction_overdue` trigger has ever been enqueued.
- **[trigger]** The one path that did run (immediate T2 on insert) resolves via
  a placeholder that only confirms if an observation literally contains the
  model UUID (`deterministic.py:183-191`).
- **[feedback]** Resolutions feed nothing: `calibration_updater` never runs,
  `prediction_falsification_value` hardcoded 0.0 (`outcome_evaluator.py:902`),
  `model_later_confirmed/falsified` events are a TODO.
- **[harness]** Frozen clock; first future-validation wave only possible at
  batch 25 (`run_storyline_batch_benchmark.py:707,796-802`) → 0.65 of the
  dimension weight zeroed by construction in a 5-batch run; the 0.35 creation
  term divides by all 9 storylines though only 5 ran.
- **NEW BUG (found in review):** `_infer_evaluate_at`'s `within_window` branch
  computes `base = _parse_datetime(entry.get("born_from_event_id")) or now()` —
  parsing an event **UUID** as a datetime; always falls back to `now()`, so
  window-relative due dates anchor to processing time, not event time.

### 2.2 Latent bridge inference — 0.00

- **[harness] DOMINANT:** long-horizon mode assigns `STORYLINES[batch % 9]` and
  the Northstar bridge spec is index 8 — a 5-batch run never injects it, yet it
  is still scored against the gold spec with no N/A fallback
  (`run_storyline_batch_benchmark.py:755,3711-3743`). The injector's regex
  conjunction *would* fire within one forced bridge batch (~0.7 of the score is
  harness-recoverable).
- **[trigger]** `maybe_inject_latent_bridge` is the only injector called
  **without** the retrieval bundle (`reason.py:1000-1005`) — it sees only the
  current trigger's batch fragments, so cross-batch before/after/gap evidence
  is invisible. No cross-batch latent scan exists anywhere.
- **[prompt/schema]** The LLM is never asked: zero occurrences of
  bridge/off-sensor/unobserved in `prompt.py`. The capability is a regex whose
  vocabulary (`_PRICING_RE` mandatory gate, `bridge_inference.py:42-45,72-73`)
  is hardcoded to the one benchmark storyline.
- **[temporality]** The 0.15 future-confirmation component has no code path:
  hypotheses are born `kind=belief, time_mode=past` with no `evaluate_at`, no
  revisit, no dedup against the DB (`_has_existing_bridge_claim` checks only
  the in-flight diff).

### 2.3 Memory lifecycle — 0.33

- **[trigger] DOMINANT for archival:** the entire maintenance layer (hourly
  decay, decay-archival at `decay.py:54-75`, staleness flagging) is never
  launched: no compose service, no script. The T4
  `background_maintenance/suggest_archival` **consumer** exists
  (`deterministic.py:360-372`) with zero producers anywhere.
- **[data-model] DOMINANT for evidence:** `attach_evidence` is not an
  expressible op (`diff_schema.py:42`); the only path is a quality-gate
  downgrade fallback whose own message admits "evidence path not yet wired",
  with anchor gates so narrow 0/25 inserts traversed it (`applier.py:1216-1336`).
- **[temporality]** No clock under which memory can become stale: the 5-day
  half-life hourly tick never runs, the harness freezes time, and nothing
  schedules staleness review. `relationship_maintenance_log` is written and
  read by no one; `model_reeval_queue` is fed only from inside `archive` — with
  zero archives, the cleanup cascade can never start (circular).
- **[prompt/schema]** Claims-only pass restricts to insert-only; the full prompt
  frames archive purely as a dedup move — against 5 waves of fresh storylines,
  0 archives is expected LLM behavior absent a maintenance trigger.
- **[harness]** FV-memory-touch term (0.20) structurally zero in 5-batch runs.
  An untruncated run would fix *only* that 0.20; archives and evidence would
  still be 0.

### 2.4 Decision impact — 0.37

Exact decomposition: 0.30×0.111 (recommendations) + 0.20×0.889 (situations) +
0.20×0.333 (act_ops) + 0.10×0 (resource_ops) + 0.10×0.929 (context) + 0.10×0 (fv).
System layer owns ~0.45 of the 0.63 shortfall.

- **[trigger]** The situation→recommendation consequence loop is dead:
  `_T2_REEVALUATION_KINDS={'prediction'}` with an explicit "now routes to a
  no-op" comment (`applier.py:230-235`); T2 `belief_updated` is classified
  authoritative → deterministic handler, never the LLM (`deterministic.py:39-56`,
  `reason.py:942-943`); worker prunes residuals (`worker.py:714-752`). The
  act-capable prompt for exactly this intent exists unreachably.
- **[prompt/schema]** The one shot that can emit act_ops is suppressed in the
  benchmark's exact conditions: claims-only schema strips
  act/resource/edge ops on sparse bundles; minimality bias ("0 act_ops …
  Empty diffs are valid"); all T3/T4 reflection intents carry
  `act_ops:0, resource_ops:0` budgets (`reasoning_frame.py:298-345`).
- **[feedback]** Silent neutralization: `_validate_act_op` returns `None` (not
  an error) on sub-threshold confidence basis (`validator.py:1019-1021`) — no
  `dropped_op_errors` string, no retry, the LLM never learns why its op
  vanished. `recommendation_feedback_stats` (Approve/Dismiss) feeds only the
  list-ranking multiplier, never generation.
- **[temporality]** Situations are terminal: the only reader of
  `claim_role='situation'` is the applier's same-event coalescer. 8/9
  situations existed; nothing ever re-presented them for a decision.

### 2.5 Question policy — 0.42 (= 0.55×0 + 0.25×0.929 + 0.20×0.929 exactly)

- **[trigger] DOMINANT — and a genuine production hole, not a harness gap:**
  `OutcomeEvaluator.evaluate` is the sole emitter of the credit events
  (`reader_decision_used_in_valid_diff`/`_low_value`,
  `outcome_evaluator.py:683-688,792-797`) that the policy writer requires; it
  has zero production callers. The writer hard-gates on those events
  (`optimizer.py:819-828` early-return 0). The read path is live and complete.
- **[temporality]** One-shot session consumption: the topology optimizer claims
  sessions via `INSERT … ON CONFLICT DO NOTHING` (`worker.py:89-124`) on *any*
  outcome event — sessions are permanently checkpointed before credit events
  could exist. The 14 sessions of the scored run are consumed forever. The
  correct ordering (evaluate → optimize) exists only in a stress script.
- **[data-model]** The human-feedback half of the event vocabulary is dead
  schema: `user_accepted_node`, `recommendation_acted_on/ignored`,
  `model_later_confirmed/falsified`, `omitted_evidence_later_requested` have
  zero emitters (TODO "Phase 14+"). The CEO's daily Approve/Discuss/Not-now can
  never teach the asking policy.
- **[data-model]** Attribution rows are written only on reader-enabled deep
  paths (`inquiry.py:8624-8634`); fast/weak/no-op routes produce no attempts
  data, so "ask less" can't learn from cheap routes.

### 2.6 Edge intelligence — 0.52

- **[data-model] DOMINANT for the dominance statistic:** 904/974 supports edges
  (92.9%) came from the legacy `supporting_model_ids` array dual-write — a
  production side-channel that hardcodes `kind='supports'`,
  `confidence=1.0`, `review_status='accepted'`, no explanation, bypassing the
  canonicalizer, validator, and review entirely (`repo.py:897-906,1375-1389,
  1443-1471`; `edges_repo.py:176-192`). The harness's 15k-model seed stage
  flowed through it (`seed_status.sidecars.model_edges=960`). Think itself
  emitted only 7 edges in 14 runs — just 1 of them `supports`.
- **[prompt/schema]** Precision is taxed: prompt caps 0-1 edge_ops; claims-only
  forbids edges; the 16-kind enum is dead code (regex instead,
  `strict_schema.py:361 vs :401`); explanation+weight required for precise
  kinds but not for `supports` (`validator.py:1217-1237`). `weakens` and
  `early_warning_for` were expected by borealis/foundryworks — storylines that
  **did** run — and never appeared: not harness-only.
- **[temporality]** Edge kinds are write-once: the canonicalizer runs pre-insert
  only; all three `UPDATE model_edges` statements are status-only; the upsert
  never touches kind; mutual exclusion **actively rejects** adding `weakens`
  where `supports` exists (`edge_registry.py:339`, `edges_repo.py:328-361`),
  and the apply-time drop is never fed back. Generic edges are frozen forever.
- **[trigger]** Candidate promotion unreachable (`run_weekly` never runs); 231/245
  candidates are ontology-gap rows stuck behind a manual debug endpoint.
- **[feedback]** `path_used_in_valid_diff` usage events have one consumer that
  "NEVER writes to models, model_edges" (`optimizer.py:21-25`);
  `contested_count` has no writer.

---

## 3. Unified architecture

**Organizing principle:** lifecycle contracts say *what must happen* to each
object (D2); the temporal spine says *when and by whom* (D1); the feedback
plane says *what the system learns from it* (D3).

| # | Component | Home | Role |
|---|---|---|---|
| C1 | **Lifecycle Registry** | `lib/shared/lifecycle_registry.py` (new) | Declarative state machines per object class (prediction, model, edge, situation, bridge_hypothesis, recommendation, question_session): legal transitions, owner (`llm`/`deterministic`/`worker`/`human`), required fields, clock column. Consumed by validator, Housekeeper, benchmark scorer. Mirrors `edge_registry.py`. |
| C2 | **Object Lifecycle Events ledger** | new migration (number TBD vs origin/main — local 0126-0132 renumber in flight) | Append-only `(object_kind, object_id, from_state, to_state, owner, reason, evidence_event_ids, think_run_id)`. Written by applier, Housekeeper, recommendation bridge. **The benchmark scores transitions, not row existence.** |
| C3 | **Think Obligations** | new migration + helpers in C4 | Durable "later" carrier — **scoped to object classes with no existing clock column**: `situation_review`, `hypothesis_review`, `edge_dispute_review`. Open-dedup unique index, `max_fires`, `due_condition` (evidence-arrival acceleration), outcome record. Predictions/`valid_until` are explicitly **excluded** — they already have clocks; obligations must not double-book. |
| C4 | **Trigger/Obligation helper** (Repair-Plan Move 5) | `services/domain/triggers.py` (new) | `enqueue_trigger(...)` + `open_obligation(...)`, idempotent, single choke-point for all producers. |
| C5 | **Housekeeper worker** (Repair-Plan Move 4) | `services/workers/housekeeper/worker.py` + `scripts/run_housekeeper_worker.py` + profile-gated compose service | Thin shell over the existing `MaintenanceScheduler` (advisory-locked). Jobs (mostly existing dormant bodies): `deadline_resolver` (60s), `prediction_expiry`, `hourly_decay`/`archive_decayed`, `staleness_sweep` (new ~60 lines: first consumer of `relationship_maintenance_log` + `valid_until` → T4 `suggest_archival` for the existing consumer), `obligation_due_sweep` (new ~80 lines: C3 → trigger queue), `edge_reclass_sweep` (C8), `candidate_promotion`, `calibration_update`, `policy_compaction` (C9). **Every job exposes `run_once()` for the benchmark drain.** |
| C6 | **Revisit-run contract** | `reasoning_frame.py`, `deterministic.py`, `llm_reason.py`, `worker.py` (modified) | New T2 subkinds: `consequence_review` (coalesced, ≤1 per T1 batch), `hypothesis_review`, plus existing `prediction_overdue`. First two route non-authoritative → `llm_reason` with the existing `propagate_consequence` / `evaluate_existing_belief` jobs. Revisit runs: no planning calls (SQL-seeded bundle: subject + linked edges + observations newer than obligation), full RawDiff always (claims-only forbidden), emit-or-structured-decline, ≤1,024-tok output, excluded from low-value prune. |
| C7 | **Forcing ops** (schema/prompt/validator) | `diff_schema.py`, `strict_schema.py`, `validator.py`, `quality_gate.py`, `prompt.py`, `llm_reason.py` | (a) `attach_evidence` + `supersede` ClaimOps dispatching onto `_append_observe_reading`; (b) `forecast_decision` forced choice (predict-with-`evaluate_at` or enumerated decline) — **staged**: prompt directive first, structural field only after cache re-measure, with coercion fallback (absent field → `decline(unspecified)`, never a failed run); (c) claims-only un-bans predictions + coercion fix (`llm_reason.py:225`); (d) wire `_EDGE_KIND_ENUM` into strict schema; require explanation for **all** kinds incl. `supports` (removes the precision tax); add `reclassify` EdgeOp = atomic retire-and-replace; (e) `_validate_act_op` None-returns and apply-time `mutually_exclusive_edge`/cycle drops produce `dropped_op_errors` visible to the retry prompt. |
| C8 | **Edge birth-state demotion + reclass sweep** | `repo.py`/`edges_repo.py` (demotion); sweep job in C5 reusing `edge_semantics.py` regexes | Array dual-write edges land `review_status='candidate'`, `detected_by='array_dual_write'`, conf 0.6 — the dominance statistic finally measures Think. Sweep re-kinds generic edges on regex match over endpoint text **or** `contested_count ≥ 2`; retire-and-replace (the only legal path past mutual exclusion); replacement lands `needs_review` unless corroborated. |
| C9 | **Op Outcome ledger + Policy Digest** | `op_outcomes.py` (new), `think_op_outcome_stats` migration, `policy_digest.py` (new), injection at `prompt.py:609` | Decayed per-(tenant, signal_type, op_kind, outcome) counters at validator/gate/applier outcome sites; compacted into a ≤600-char digest injected into the **dynamic user-message profile only** (static prefix untouched → cache savings preserved). n≥3 noise floor; numerators count only gate-surviving ops (anti-gaming). |
| C10 | **Question-policy closure** | `services/workers/sage_topology_optimizer/worker.py` | `await OutcomeEvaluator(...).evaluate(inquiry_session_id=...)` inside the claimed-session loop **before** `optimize_topology`, try/except-wrapped. Structurally guarantees evaluate-before-aggregate inside the once-only claim window. Plus one-shot backfill/re-claim for already-consumed sessions. Read path needs zero changes. |
| C11 | **Recommendation feedback bridge** | `services/product/recommendations/feedback.py` | Approve/Discuss/Not-now additionally emits `recommendation_acted_on/ignored` outcome events + C2 transitions + C9 upserts. Consumption stays ranking/policy-only ("never belief content"). |
| C12 | **Prediction resolution chain** | (ownership resolved across designs) | Due-detection: Housekeeper `deadline_resolver` job. Resolution: Think deterministic handler with the **residual matcher** replacing the UUID-substring placeholder; LLM escalation only on ambiguity, flag-gated. Consumption: `calibration_update` → `calibration_offsets` → already-live `apply_calibration`; `model_later_confirmed/falsified` events; un-hardcode `prediction_falsification_value`. Creation: `_infer_evaluate_at` reads `falsifier["evaluate_at"]` + C7 forcing. |
| C13 | **Bridge detection + confirmation** | `reason.py`, `bridge_inference.py`, `prompt.py` | Pass `bundle` to `maybe_inject_latent_bridge` (the only injector without it); DB-aware dedup; domain-general prompt paragraph (replaces pricing-only regex dependence). Hypotheses born `claim_role='hypothesis'` with a `hypothesis_review` obligation (`max_fires=2`, evidence-accelerated) → revisit attaches confirming observation and upgrades, or retires — the rubric's future-confirmation component's first-ever code path. |
| C14 | **Eval hooks** | `scripts/run_storyline_batch_benchmark.py` | See Phase 0. |

---

## 4. Per-loop closure table

| Loop (score) | Dominant layer(s) | Components | Forcing (creation) | Revisit | Feedback | Eval terms moved |
|---|---|---|---|---|---|---|
| Prediction (0.04) | trigger + prompt/schema + data-model + harness | C12, C5, C7, C14 | `forecast_decision` predict-or-decline; `falsifier.evaluate_at` read; claims-only un-ban | Housekeeper deadline job → T2 `prediction_overdue` → residual matcher; expiry sweep | resolutions → calibration (read side already live); confirmed/falsified events; digest line | 0.35 creation + 0.25 proxy + 0.40 fv terms |
| Bridge (0.00) | harness + trigger + prompt + temporality | C13, C3, C14 | bundle-aware injector + domain-general prompt; DB dedup | `hypothesis_review` obligation, evidence-accelerated | hypothesis outcomes in C2/C9 | ~0.7 harness-recoverable; 0.15 confirmation via C3 |
| Memory (0.33) | trigger + data-model + temporality + harness | C5, C7, C14 | `attach_evidence`/`supersede` ops; maintenance-framed archive prompt | decay/staleness sweeps → T4 `suggest_archival` → existing deterministic archiver (zero LLM cost) | first `relationship_maintenance_log` consumer; archived-then-retrieved counter | archives (0.20) + evidence (0.20) + fv (0.20, harness) |
| Decisions (0.37) | trigger + prompt/schema + feedback + temporality | C6, C7, C9, C11 | coalesced `consequence_review` finally executes the act-capable prompt; recommendation-or-decline | `situation_review` obligations; open-rec follow-up | neutralization reasons in retry prompt + C9; Approve/Dismiss → events → digest | rec_coverage (0.30w @ 0.11) + act_ops (0.20w @ 0.33) |
| Question policy (0.42) | trigger + temporality + data-model | C10, C11, C5 | n/a (read path live) | session re-claim/backfill | credit events → policy upserts → live budget application | 0.55 term: 0.42 → ~0.9 |
| Edges (0.52) | data-model + trigger + feedback + harness | C8, C7, C5, C9, C14 | enum kinds + universal justification + `reclassify` op; birth-state demotion | reclass sweep (first mechanism ever able to change a kind post-insert); promotion job; dispute obligations (0131 `disputed` status) | `contested_count` writer; drop reasons in retry prompt; kind priors in digest | coverage (0.38w) + lifecycle (0.10w @ 0) + generic-share penalty; metric segregation by `detected_by` |

---

## 5. Sequenced rollout

### Phase 0 — Harness + measurement-gating bug fixes (days)

Harness (`scripts/run_storyline_batch_benchmark.py`):
- Coverage-first storyline ordering (`:755`) so ≤9-wave runs include the bridge
  spec; N/A-score storylines with zero injected signals; restrict coverage
  denominators (`:3600,:2926-2933`) to injected storylines.
- Force the final batch of short runs to `future_validation` (`:707,:796-802`).
- **Simulated clock by world-shift:** per-wave `--sim-advance-hours`, one UPDATE
  shifting tenant temporal columns backward (`evaluate_at`, `check_after`,
  `last_retrieved_at`, `valid_until`, obligation `due_at`, queue timestamps) +
  decay-multiplier application; zero production-code awareness.
- Per-wave drain: `DeadlineResolver.run_once`, decay tick, staleness sweep,
  reclass sweep, promoter (via job `run_once` bodies); widen drain SQL
  (`:1860-1890`) to `prediction_overdue`, `consequence_review`,
  `hypothesis_review`, `background_maintenance`.
- **Score transitions, not existence:** `model_predictions`
  confirmed/falsified/expired (replacing the binary fv proxy `:3764-3771`),
  archives by reason, evidence attachments, reclassified edges, policy upserts;
  score decline rates with reasons (justified declines are correct behavior);
  segregate `detected_by='array_dual_write'` edges from edge metrics.
- New `policy_delta` run-over-run learning dimension (first-third vs last-third
  behavior change); invoke digest compaction in the end-of-wave drain so wave N
  teaches wave N+1.

Production bug fixes that gate measurement (un-gated, no LLM-visible change):
- `prediction_lifecycle.py`: `_infer_evaluate_at` reads `falsifier["evaluate_at"]`;
  fix the `born_from_event_id`-parsed-as-datetime bug in `within_window`.
- `sage_topology_optimizer/worker.py`: C10 evaluate-before-optimize (~5 lines)
  + backfill for consumed sessions.
- `deterministic.py`: residual matcher replaces the UUID-substring heuristic.

**Expected movement from Phase 0 alone:** question policy 0.42→~0.85-0.9,
bridge 0.00→~0.7, prediction 0.04→~0.3-0.4, memory +~0.2, decisions +~0.1,
edges +~0.1. **Re-baseline with the A3 variance band before any Phase 1+ flag
decision.**

### Phase 1 — Forcing functions (flag-gated, default off)

Files: `diff_schema.py`, `strict_schema.py`, `validator.py`, `quality_gate.py`,
`prompt.py`, `llm_reason.py`, `reason.py`, `bridge_inference.py`,
`repo.py`/`edges_repo.py`, `think_op_outcome_stats` migration (writer only).
- `THINK_LIFECYCLE_OPS`: `attach_evidence` + `supersede`.
- `THINK_FORECAST_DECISION`: staged (prompt directive → structural field after
  cache re-measure); claims-only un-ban + coercion fix.
- `THINK_EDGE_CONTRACT`: enum, universal explanation, `reclassify` op.
- `EDGE_BIRTH_STATE_CANDIDATE`: dual-write demotion, A/B on retrieval metrics.
- Un-gated: drop/neutralization reasons into retry prompt; `contested_count`
  first writer; bundle to bridge injector + DB dedup + hypothesis role +
  domain-general bridge prompt paragraph.

### Phase 2 — Temporal spine

New: lifecycle-events + obligations migrations, `lib/shared/lifecycle_registry.py`,
`services/domain/triggers.py`, `services/workers/housekeeper/` + script +
profile-gated compose service. Modified: `applier.py` (producers: coalesced
`consequence_review` T2 — T1-origin only, confidence floor, loop guard;
obligations via C4), `worker.py` (evidence-arrival accelerator ~30 lines; prune
exclusions), routing (`reasoning_frame.py`/`deterministic.py`/`llm_reason.py`),
`weekly.py`.
Flags: `HOUSEKEEPER_ENABLED`, `THINK_OBLIGATIONS_ENABLED`,
`THINK_CONSEQUENCE_REVIEW`, `THINK_OBLIGATION_LLM_KINDS` (per-kind kill switch,
default empty = deterministic-only spine), `THINK_REVISIT_DAILY_CAP_PER_TENANT`.

### Phase 3 — Feedback plane

Files: `op_outcomes.py`, `policy_digest.py`, `prediction_lifecycle.py`,
`outcome_evaluator.py`, `recommendations/feedback.py`, `inquiry.py` (attempts
coverage), `prompt.py:609` injection.
- `THINK_POLICY_DIGEST`; C11 bridge; confirmed/falsified events + non-zero
  `prediction_falsification_value`; `omitted_evidence_later_requested` emitter;
  attempts coverage for reader-disabled/zero-round routes; obligation-outcome
  learning ("revisit less / revisit better").
- Enable order per THINK-COST-PLAN §9: writers → C10 → digest, each gated on
  storyline + variance band + $10.92/10k no-regress.

**Cost envelope:** Phases 0/1/3 ≈ zero new LLM calls (+~150 dynamic tokens for
digest, +80-150 output tokens for forecast_decision; static prefix untouched).
Phase 2 ceiling: +1 coalesced `consequence_review` per T1 batch + 0-1
`hypothesis_review`, planning-free SQL-seeded bundles → worst case roughly
+15-35% Think spend, hard-capped per tenant, deferral-not-dead-letter on budget
pause.

---

## 6. Conflicts, ADRs, and review-flagged errors

1. **ADR required — rescind CAPABILITY-PLAN (~line 313) deletion of
   `model_predictions` + `outcome_evaluator`.** The plan's "tables never
   written / test-only" claims are stale: the new `prediction_lifecycle.py`
   writes `model_predictions`, and C10/C12 make both load-bearing. Must be
   adjudicated explicitly or the deletion plan and this architecture will fight.
2. ADRs for the obligation object (C3) and policy-digest contract (C9); same-PR
   doc updates to `SYSTEM-ARCHITECTURE.md` and `docs/architecture/{workers,reasoning}.md`.
3. **Errors caught during adversarial review** (excluded from this plan):
   a fabricated "3,553-candidate backlog" (actual: 245 in the run artifact);
   obligation kinds that would double-book existing clocks
   (`prediction_due`, `belief_expiry` — dropped); a *required*
   `forecast_decision` RawDiff field as initially proposed (breaking
   output-contract change across providers; staged with coercion fallback
   instead).
4. Migration numbers must be re-confirmed against origin/main (local 0126-0132
   renumber in flight). `OutcomeEvaluator` constructor kwargs unverified
   (only `evaluate`'s keyword-only signature was checked) — confirm at
   implementation.

---

## 7. The three highest-leverage edits, in order

1. **`OutcomeEvaluator.evaluate` before `optimize_topology`** in the topology
   optimizer worker (~5 lines): one loop fully closed; benchmark exercises it
   for free via the existing drain; 0.42 → ~0.9.
2. **The Housekeeper entrypoint** over already-built dormant job bodies
   (DeadlineResolver, decay/archival, promoter, calibration): converts five
   "built but never launched" subsystems into running ones with near-zero new
   logic.
3. **The coalesced `consequence_review` T2** (3 small edits in
   `applier.py`/`deterministic.py`/`worker.py` + 1 drain line): finally executes
   the already-written act-capable `propagate_consequence` prompt, attacking
   the two largest decision-impact loss terms.
