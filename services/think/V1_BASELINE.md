# Fyralis v1 Substrate — Baseline Audit

Read-only baseline for the v1 substrate work described in [V1_PR_PROMPTS.md](../../V1_PR_PROMPTS.md) and [SUBSTRATE_SEMANTICS.md](SUBSTRATE_SEMANTICS.md). Audit performed at HEAD `415abe8` on branch `demo-deploy` (2026-05-08); readiness check re-verified at `3bc8de3` (2026-05-09) after B1+B2 fixes landed.

## Summary

The codebase has shipped through T5 (reconciliation as a first-class pipeline step). Of the five v1 decisions:

- **Q5 (Audit chain)** — partial. `reconciliation_events` exists but is reconciliation-only; no general audit-event table tracks `previous_state` / `new_state` / `re_asserts_event_id` for Model state changes.
- **Q4 (Confidence-as-strength)** — partial. The reconciler already uses `max(existing, candidate)` on auto-merge, but the extraction prompt has no aspirational/targeted/committed calibration anchor and no Commitment-specific exception path.
- **Q3 (Preconditions)** — absent. `commitments` has a `state` column but no `precondition` column, no enum constraint, no `latent` state, and no precondition-satisfaction logic in cascade.
- **Q1 (Reconciliation)** — single-pass. Cosine ≥ 0.85 → auto_merge, [0.70, 0.85) → human_review, < 0.70 → no_match. No LLM second pass and no `reconciliation_decisions` cache.
- **Q2 (Hierarchy)** — absent. No `entities` or `entity_relationships` table; `scope_entities` is a flat JSONB array; Pathway A is actor-scoped lookup, not graph walk.

**Readiness verdict:** PR 1 is **CLEAR**. All five prerequisite fixes are merged as of commit `3bc8de3`. Details in the [Readiness check](#readiness-check) section.

---

## Migration framework

Raw SQL files in [db/migrations/](../../db/migrations/) (29 files, `0001_foundation.sql` … `0029_reconciliation_events.sql`). Two runners:

- **Production**: [scripts/docker-migrate.sh](../../scripts/docker-migrate.sh) — `psql --single-transaction`.
- **Python**: [lib/shared/migrations.py](../../lib/shared/migrations.py) — `apply_migration()` and `apply_migrations_dir()` wrap each file in `async with conn.transaction():`. Multi-statement transaction safety landed in commit `6b6774e`. The harness uses `on_error="warn"` to tolerate partial-state dev databases — this hides genuine failures and is a known divergence from prod behavior.

Idempotency convention: every `CREATE TABLE`/`CREATE INDEX` uses `IF NOT EXISTS`; every drop uses `IF EXISTS`.

## Harness conventions

Cases in [tests/synthesis_harness/](../../tests/synthesis_harness/) follow a five-part `Case` dataclass:

1. `setup(pool, ctx) -> ctx` — seed state inside a transaction; returns the context dict
2. `run(pool, ctx) -> actual` — execute production code; returns a result dict
3. `expected(ctx) -> expected` — derive expected output dict
4. `assertion(actual, expected, ctx) -> (bool, diff)` — pure comparison
5. Metadata: `stage`, `name` (snake_case), `intent` (human description)

Assertions are specific (e.g., "in-scope score > out-of-scope score" rather than "in-scope present"). Embedding fixtures use `F.deterministic_vector(seed_string)` for reproducible 768-dim vectors. Adversarial cases under [tests/synthesis_harness/adversarial/](../../tests/synthesis_harness/adversarial/) add `failure_mode_under_test` and `expected_behavior` fields.

---

## Q5 — Audit chain

**Current state.** No general audit-event table exists. The closest analogue is [db/migrations/0029_reconciliation_events.sql](../../db/migrations/0029_reconciliation_events.sql), which records reconciliation outcomes only (fields: `id`, `tenant_id`, `occurred_at`, `decision`, `original_claim_op`, `matched_model_id`, `cosine_similarity`, `proposition_kind`, `trigger_id`, `think_run_id`, `resolved_at`, `resolved_decision`, `resolved_by_actor_id`). It is append-only and decision-scoped — it does not track Model state transitions.

State-change tracking today flows through observations: [services/observations/state_change.py](../../services/observations/state_change.py) `emit_state_change(cause_id=prior_event)`. There is no structured table with `previous_state`, `new_state`, `changed_fields`, or `re_asserts_event_id`. Reversal-of-reversal (A → B → A) produces three observation rows in temporal order, but the third does not point back to the first; the linkage is implicit in event ordering, not explicit.

**Reachability from Model query.** `SELECT * FROM models WHERE id=$1` returns no audit pointers; there is no `get_audit_chain(model_id)` API today. Consumers that need history must walk observations by `cause_id`, which is fragile.

**Implication for PR 1.** Greenfield audit chain table per the PR 1 schema is the right move. The existing `reconciliation_events` table can remain alongside, narrowed to its current single-purpose role. The reconciliation-merge audit must read source Models' chains from the new audit table, not from `reconciliation_events`.

## Q4 — Confidence-as-strength

**Extraction prompt.** [services/think/prompt.py](prompt.py) gives confidence the range `0.05–0.95` (line ~78) with the note "calibration will be applied to your confidence numbers; assert honestly" (~line 47-50). It does **not** distinguish aspirational vs targeted vs committed linguistic markers. The two failing adversarial scenarios (`hedged_commitment_low_confidence`, `compound_linguistic_pressure`) trace to this gap.

**Reconciliation merge logic.** [services/think/reconciler.py](reconciler.py) lines ~283-316 already use `max(candidate_conf, existing_conf)` on auto_merge — there is no `bulk_confidence_update` function and no per-Node-type branching. The PR 2 work is therefore additive rather than corrective: introduce `commitment_merge_confidence(existing, new)` as the named exception, route Commitment merges through it, and route everything else through whatever rule we adopt as default (today's behavior is uniform max — PR 2 will need to decide whether non-Commitment Nodes should switch to `bulk_confidence_update` or remain on max).

**Strength field.** No `strength` column on `commitments` (per [db/migrations/0001_foundation.sql](../../db/migrations/0001_foundation.sql)). Q4-B (separate strength field) is correctly out of scope.

**Calibration measurement.** [tests/synthesis_harness/calibration.py](../../tests/synthesis_harness/calibration.py) (commit `95b2ce7`) tracks ECE per stage. PR 2 should run before/after measurements through this layer to verify non-Commitment ECE doesn't regress.

## Q3 — Preconditions

**Schema.** `commitments` (migration 0001) has columns: `id`, `tenant_id`, `title`, `description`, `state` (varchar, default `'proposed'`), `owner_id`, `due_date`, `created_at`, `last_state_change_at`, `terminal_at`, `created_by_event_id`, `last_confidence_basis`. **No `precondition` column.** `state` is varchar, not an enum — current values per schema lock: `proposed`, `active`, `blocked`, `doneverified`, `closed`, `at_risk`. No `latent` state.

**Cascade behavior.** [services/think/cascade.py](cascade.py) Branch A handles "commitment state change" by scanning for dependents in `blocked` whose other dependencies are satisfied, then transitioning them to `active` via `commitments_svc.transition(dep_id, "active", cause_event_id=…)`. Invariant C4 enforces `cause_event_id`. There is no precondition-satisfaction code path — cascade walks blocked-dependency chains, not preconditions.

**Latent / conditional concepts.** None. The prompt does not extract preconditions, the schema cannot store them, and cascade has nothing to satisfy. PR 3 is greenfield.

**Backfill consideration.** With `state` already varchar, the PR 3 migration must either ALTER the column to a new `commitment_state` enum (requires data validation + cast) or add the enum alongside and migrate via a second pass. The PR 3 design doc should answer this explicitly because the existing varchar values (`doneverified`, `at_risk`, etc.) are not in the proposed enum (`latent`, `active`, `completed`, `cancelled`) and need a mapping.

## Q1 — Reconciliation

**Reconciler.** [services/think/reconciler.py](reconciler.py) is single-pass, cosine-only. Thresholds (lines ~101-108):

| Threshold | Default | Env var |
|---|---|---|
| `RECONCILE_AUTO_MERGE_COSINE` | 0.85 | `RECONCILE_AUTO_MERGE_COSINE` |
| `RECONCILE_HUMAN_REVIEW_COSINE` | 0.70 | `RECONCILE_HUMAN_REVIEW_COSINE` |
| `RECONCILE_RECENCY_WINDOW_DAYS` | 30 | `RECONCILE_RECENCY_WINDOW_DAYS` |
| `RECONCILE_ENABLED` | true | `RECONCILE_ENABLED` |

Decision tree: cosine ≥ 0.85 → `auto_merge`; [0.70, 0.85) → `human_review` (queue); < 0.70 → `no_match` (create new). Match candidates also require overlapping scope (`scope_actors`/`scope_entities`), identical `proposition_kind`, and creation within the recency window (lines ~168-178).

**Cache.** No `reconciliation_decisions` cache table exists. Each think_run computes fresh cosine matches. The existing `reconciliation_events` table is audit/queue, not a cache — its `(model_id_a, model_id_b)` pairs are decision history, not lookup keyed.

**Implication for PR 4.** PR 4 thresholds (0.65 lower bound, 0.85 upper) differ from current (0.70 lower). The PR 4 design must decide whether to lower the bottom band (more LLM second-pass calls) or hold at 0.70 (smaller LLM cost surface). Either way, `human_review` semantics change — today's middle band becomes "second-pass call", and the human-review queue becomes a fallback after second-pass non-match (or after budget exhaustion).

## Q2 — Hierarchy

**Schema.** No `entities` table, no `entity_relationships` table, no relationship migration anywhere in [db/migrations/](../../db/migrations/). `scope_entities` lives on Models as a flat JSONB array of `{type, id}` objects. There is no canonical entity registry — scope_entities IDs are referent UUIDs, not pointers into an entity table.

**Retrieval.** [services/retrieval/pathways.py](../../services/retrieval/pathways.py) Pathway A is actor-scoped lookup via `obs_actor_time_idx`. It does not walk relationships. Hierarchy does not exist in retrieval.

**Extraction.** The prompt does not ask the LLM to emit relationships. PR 5's LLM-driven authoring is greenfield in the prompt as well as the schema.

**Implication for PR 5.** PR 5 introduces `entities` and `entity_relationships` as a parallel structure that the existing `scope_entities` JSONB references into. This requires either backfilling `entities` from existing scope_entities IDs (auto-create canonical entries from observed UUIDs) or accepting that pre-PR-5 Models have no walkable hierarchy until backfilled. The design doc should answer.

---

## Readiness check

Per the PR 0 prompt, PR 1 must not start until five prerequisite fixes are merged.

| Fix | Status | Evidence |
|---|---|---|
| **B1** — `apply_diff` callers race-create duplicates (region lock) | MERGED | Commit `3bc8de3`. `apply_diff` now acquires its own region lock at the top via `touched_entity_ids_from_diff()`. Direct callers no longer rely on a docstring contract. CC1 (`five_parallel_identical_inserts_collapse_to_one`) and CC10 (`permuted_scope_holds_same_region_lock`) pass. |
| **B2** — Trigger-id race leaks `UniqueViolationError` | MERGED | Commit `3bc8de3`. `INSERT INTO applied_triggers` is wrapped in `try/except asyncpg.exceptions.UniqueViolationError` and re-raised as `AlreadyAppliedError`. CC8 (`parallel_trigger_id_idempotency`) passes. |
| Falsifier `within_window` parser silent failure | MERGED | Commit `a2d6d6f` (T1a). `parse_within_window()` accepts both human-readable (`"7 days"`) and ISO-8601 (`"P7D"`); malformed strings raise `MalformedFalsifierError` (loud). |
| Cascade invariant violations silent failure | MERGED | Same commit `a2d6d6f` (T1b). `CascadeResult.invariant_violations` list + WARNING log + metric bump. No longer logged at INFO. |
| Multi-statement migration transaction safety | MERGED | Commit `6b6774e`. Each migration wrapped in `async with conn.transaction():` in [lib/shared/migrations.py](../../lib/shared/migrations.py). |

**CLEAR.** All five prerequisites are merged. Verification: adversarial.concurrency 10/10, base 52/52, full adversarial 112/112 (with `HARNESS_SKIP_LLM=1`). PR 1 may proceed.

---

## Surprising findings

These deviations from the SUBSTRATE_SEMANTICS.md text are worth flagging during user review of this baseline:

1. **Reconciler already uses `max()` on auto-merge.** Q4's "exception" is therefore introducing a *named function* and a Commitment-specific *path*, not changing the math. The actual behavioral change is for non-Commitment merges if PR 2 chooses to switch them to `bulk_confidence_update`.
2. **`reconciliation_events` is audit-flavored but is not the audit chain.** PR 1's audit_events table is parallel infrastructure. The two tables should be distinguished clearly in the SUBSTRATE_SEMANTICS.md update.
3. **`commitments.state` is varchar, not enum.** Existing values (`doneverified`, `at_risk`) don't map cleanly to PR 3's proposed enum (`latent`, `active`, `completed`, `cancelled`). PR 3 design doc must address the mapping.
4. **Two migration runners with different failure semantics.** Prod uses `psql --single-transaction`; harness uses `on_error="warn"`. Schema migrations in PR 1, 3, 4, 5 should be tested against both.
5. **pgvector codec is pool-sticky.** [tests/synthesis_harness/_fixtures.py](../../tests/synthesis_harness/_fixtures.py) registers the codec per pool and tracks connection IDs. Any new test pool in PR 1-5 must follow this pattern or vector ops silently corrupt.
