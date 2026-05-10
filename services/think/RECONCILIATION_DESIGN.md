# Reconciliation as a first-class pipeline step — design

**Status:** DRAFT — awaiting user review. Implementation does not
begin until this document is signed off.

**Scope of this document:** the *what* and *why*, not the *how*.
Implementation specifics (file names, exact SQL, threshold values
beyond their type) are deferred to PR review.

## Problem statement

The harness finding (`tests/synthesis_harness/REPORT.md` §1) was:
the codebase pretends reconciliation exists ("contested_false,"
"superseded," `claim_op.update`/`archive`) but only enforces
per-trigger-id idempotency through `applied_triggers`. Two
semantically identical observations arriving via *different*
`trigger_id`s produce two near-duplicate Models. The LLM is the
only mechanism keeping the Models surface deduplicated, and the
harness has no contract test for that behavior.

Consequence: the cardinality of the Models surface drifts upward
over time as duplicate-but-distinct rows accumulate. Retrieval
returns multiple variants of the "same" belief, the LLM sees
contradictory beliefs that are actually copies, and the calibration
ECE shifts in unpredictable ways because the same proposition is
expressed at different confidences across copies.

## Goals

1. **Detect** content-level duplication at insert time and
   short-circuit it before another row lands.
2. **Preserve audit** — every reconcile decision is durable, with
   the original create op, the matched candidate, the similarity
   score, and the resolution.
3. **Fail safely** — when the system is unsure, it does NOT
   auto-merge; it queues for human review. False positives in
   reconciliation are worse than false negatives because they
   destroy distinct information.
4. **Reuse the contestation infrastructure** — when reconciliation
   updates a Model's confidence based on new evidence, it goes
   through the same `bulk_confidence_update` path the calibration
   updater already uses, so confidence dynamics stay coherent.

## Non-goals

* Cross-tenant reconciliation. Every reconcile decision is
  scoped to a single `tenant_id`. The schema's tenant column
  is load-bearing here.
* Backfilling reconciliation against existing duplicate Models.
  That's a separate batch job; the design provides hooks
  (`reconcile_recent_models` from §5) but doesn't run it
  automatically.
* Replacing the LLM's `claim_op.update` / `claim_op.archive` ops.
  The reconciler runs *before* validation, on `claim_op.insert`
  proposals. If the LLM emitted an explicit update or archive,
  the reconciler does not interfere — that's already an explicit
  reconciliation decision the LLM made.

## Where in the pipeline

The pipeline today is:

```
trigger → retrieve → (region lock) → reason (LLM/deterministic)
        → validate → apply → cascade
```

The reconciler runs **between validate and apply**, on each
`ValidatedDiff.claim_ops` whose `op == "insert"`:

```
trigger → retrieve → reason → validate
        → reconcile  ◀── NEW
        → apply → cascade
```

Why between validate and apply, not earlier:

* **After validate** because validate strips malformed inserts
  (T1a `MalformedFalsifierError`, falsifier inadequacy, scope
  errors). Reconciling against a doomed insert is wasted work.
* **Before apply** because apply mutates the Models surface and
  emits `state_change`. Once the row has landed, the reconcile
  decision is no longer "skip the insert"; it's "merge two rows,"
  which is a strictly harder operation. Reconciling pre-apply
  keeps the merge as a *cheap conversion* of `op="insert"` →
  `op="update"`.

Important: the reconciler runs *inside* the apply transaction so
its read of the existing Models surface and its decision are
serialized with respect to other Think runs in the same region
(via the existing region lock). The transaction also ensures that
if apply rolls back, the reconciliation_events row rolls back too.

## Decision signals

The reconciler matches a candidate `ClaimOp(op="insert", entry=…)`
against existing rows in `models` using **all four** of:

1. **Embedding cosine similarity** between the candidate's
   `entry["natural"]` (or `entry["embedding"]` if provided) and
   the existing Model's `embedding`. Computed via the existing
   HNSW index. We use cosine because that's what Pathway B uses
   and what `register_vector` exposes by default.

2. **Scope overlap** between the candidate's `scope_entities` /
   `scope_actors` and the existing Model's. Definition: at least
   one entity in `scope_entities` must intersect (same `(type,
   id)` tuple) AND, if both have `scope_actors`, the actor sets
   must overlap. Two Models in disjoint scopes are not duplicates
   even if their text is identical — they're parallel beliefs
   about different entities.

3. **Proposition kind** must be exact match. `concern` does not
   reconcile against `state`; `prediction` does not reconcile
   against `pattern`; `recommendation` does not reconcile against
   `state` even if they describe the same thing. The kinds carry
   structural information that the embedding does not capture.

4. **Recency window**, configurable via env. Default 30 days from
   `models.created_at`. Older Models are not auto-reconciled
   because their context has likely changed; if a new claim
   resembles an old one, that's grounds for the LLM to consider
   superseding rather than for the reconciler to silently merge.

A candidate Model is a "match" iff all four signals agree. The
similarity threshold determines the *action*.

## Decision thresholds

Three thresholds, all bounded to `[0.0, 1.0]` and configurable via
environment variables:

| Threshold name                       | Default | Meaning |
|--------------------------------------|---------|---------|
| `RECONCILE_AUTO_MERGE_COSINE`        | `0.85`  | Cosine ≥ this AND all four signals agree → auto-reconcile (convert insert → update against the matched Model). |
| `RECONCILE_HUMAN_REVIEW_COSINE`      | `0.70`  | Cosine in `[0.70, 0.85)` AND all four signals agree → write to `pending_reconciliation` queue, proceed with the original insert. |
| `RECONCILE_RECENCY_WINDOW_DAYS`      | `30`    | Existing Models older than this in `created_at` are excluded from candidate set. |

Below `0.70`, or if any non-cosine signal disagrees, the candidate
proceeds as a normal insert. The original `claim_op` is preserved.

These defaults are guesses. They MUST be empirically tuned. The
design accepts that they will move; the env-var configurability
is so a deployer can move them without a code change. The
calibration harness from T4 is the right surface for measuring
the impact — adding "reconciler false positive rate" and
"reconciler false negative rate" labels to harness scenarios is
the long-tail follow-up.

## Confidence math on auto-reconcile

When `op="insert"` is converted to an update against an existing
Model M, the new evidence's confidence does not simply replace M's
confidence. We treat the new claim as a **supporting observation**
and recompute via the existing
`services.models.repo.bulk_confidence_update` machinery (the
infrastructure the calibration updater uses today).

Specifically:

* If the new claim's confidence is **higher** than M's: this is a
  confirming signal. M's `confirmed_count` increments,
  `last_confirmed_at` updates, and confidence rises by a small
  step (we'll borrow the exact formula from the calibration
  updater rather than invent one — single source of truth).
* If the new claim's confidence is **lower** than M's: this is a
  weakening signal. M's `contested_count` does NOT increment
  (that's reserved for explicit contestation per §11 of the
  spec); instead, confidence drifts toward the new value with the
  same step machinery.
* In **both cases**, the new claim's `supporting_event_ids` are
  appended to M's array, and a `signal_readings` entry is added
  recording the new actor's contribution.

This reuses contestation/calibration plumbing rather than
inventing a parallel confidence-update path. Anyone debugging
"why did this Model's confidence change" looks at one source.

## Audit schema

A new table:

```sql
CREATE TABLE reconciliation_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decision TEXT NOT NULL CHECK (
    decision IN ('auto_merge', 'human_review', 'no_match')
  ),
  -- The original ClaimOp.entry that was being inserted. Stored as
  -- JSONB so we can reconstruct exactly what the LLM proposed.
  original_claim_op JSONB NOT NULL,
  -- The matched existing Model id, if any. NULL when decision='no_match'
  -- (still recorded so we can measure the "near miss" rate).
  matched_model_id UUID REFERENCES models(id),
  cosine_similarity FLOAT,
  -- Trigger that produced the candidate. Joins to applied_triggers /
  -- think_runs for upstream context.
  trigger_id UUID NOT NULL,
  think_run_id UUID,
  -- For human_review decisions: NULL until a human resolves it.
  resolved_at TIMESTAMPTZ,
  resolved_decision TEXT CHECK (
    resolved_decision IN ('merge', 'keep_separate', 'reject')
  ),
  resolved_by_actor_id UUID
);

CREATE INDEX recon_events_tenant_unresolved_idx
  ON reconciliation_events (tenant_id, occurred_at DESC)
  WHERE resolved_at IS NULL;

CREATE INDEX recon_events_matched_model_idx
  ON reconciliation_events (matched_model_id)
  WHERE matched_model_id IS NOT NULL;
```

Notes:

* `decision='no_match'` rows are written too. They cost a row per
  insert but give us the data to retune thresholds.
* `original_claim_op` is JSONB rather than denormalized columns
  because the LLM may emit fields we haven't planned for; we'd
  rather store the whole shape than lose it.
* Foreign-key to `models.id` is `ON DELETE SET NULL` (not shown):
  if the matched Model is later archived/deleted, the audit trail
  survives.

## Failure modes and mitigations

### False positive (reconciler merges two Models that should have stayed separate)

**Cause:** scope-overlap heuristic too loose, or text is
superficially similar but semantically distinct (e.g., two
predictions about different deadlines for the same goal).

**Mitigation:**

1. **Human review queue** at the medium-similarity threshold
   catches the ambiguous middle. The default cutoff at `0.85`
   for auto-merge is deliberately conservative; future tuning
   should drift downward from here only after empirical evidence
   that the false-positive rate is low.
2. **Un-merge support.** When a `human_review` row is resolved
   with `merge`, the merge is a confidence update — the original
   insert's intent is preserved in `original_claim_op` JSONB. If
   later determined wrong, the reverse is to re-emit the original
   claim_op as a fresh insert. We do not provide a SQL-level
   "split a Model in two" operation; that's intentionally hard.
3. **Reconciler can be turned off.** Env var
   `RECONCILE_ENABLED=false` short-circuits the entire step. If
   we observe a regression in production, this is the kill switch
   while we tune.

### False negative (reconciler fails to merge two Models that should have been merged)

**Cause:** thresholds too tight, or scope predicate too strict
(e.g., scope_entities lists differ by one trivial entry).

**Mitigation:**

1. **Periodic background pass.** A worker
   `services.workers.reconciliation_sweeper` runs hourly,
   selects all Models created in the last 24 hours, and re-runs
   the reconciler at *looser* thresholds (e.g.,
   `RECONCILE_AUTO_MERGE_COSINE - 0.05`). Matches surface in
   the human-review queue; nothing is auto-merged by the sweeper
   regardless of similarity. The sweeper exists to find
   candidates a human can decide on.
2. **Calibration harness coverage.** New scenarios in the
   reconciliation stage of the harness exercise the false-negative
   case explicitly: two near-duplicate signals pass through with
   no reconcile decision; the assertion catches this. If the
   tuning of the thresholds drifts into "almost everything is a
   false negative," the harness fails.

## What's preserved on no-match

When reconciler returns `no_match`, the original `claim_op` is
applied unmodified. The reconciler is purely additive in this
case — it writes one row to `reconciliation_events` recording
the decision (with cosine of the closest non-matching candidate,
if any) and gets out of the way. The `apply_diff` path is
untouched.

## What changes for callers

* `apply_diff(diff, conn, ...)` gains an internal call to
  `reconcile_diff(diff, conn, ...)` before the loop over
  `claim_ops`. Existing callers don't change; the surface is
  internal to Think.
* The Think prompt does **not** change. The LLM continues to
  emit `claim_op.insert` proposals freely. The reconciler is
  invisible to it, by design — making the LLM aware of the
  reconciler would invite it to game thresholds.
* Retrieval is unchanged: reconciler reads from existing
  `services.models.repo.search_by_embedding` and the GIN
  scope-entity index already in place.

## Configuration

| Env var                              | Type   | Default | Notes |
|--------------------------------------|--------|---------|-------|
| `RECONCILE_ENABLED`                  | bool   | `true`  | Kill switch. |
| `RECONCILE_AUTO_MERGE_COSINE`        | float  | `0.85`  | Auto-merge threshold. |
| `RECONCILE_HUMAN_REVIEW_COSINE`      | float  | `0.70`  | Lower bound for review queue. |
| `RECONCILE_RECENCY_WINDOW_DAYS`      | int    | `30`    | Candidate filter on `created_at`. |
| `RECONCILE_LOG_NO_MATCH`             | bool   | `true`  | Whether to write rows for `no_match` decisions (tuning data). |

## Logging and metrics

Per reconcile decision, log:

* `reconcile.decision` event with `decision`, `cosine`,
  `proposition_kind`, `tenant_id` (extra-hashed), `trigger_id`.
* `reconcile.error` event when the reconciler raises (the
  reconcile failure must NOT abort the apply — log and proceed
  with the original insert).

Counters (added to `services.think.observability.METRICS`):

* `reconcile_decisions_total{decision}` — rate of auto_merge
  vs human_review vs no_match.
* `pending_reconciliation_depth{tenant_id}` — gauge fed by a
  scrape over `reconciliation_events` where `resolved_at IS
  NULL`.
* `reconcile_latency_ms` — histogram of how long the reconciler
  step takes per insert (so we can tell if it's becoming a hot
  path).

## Harness scenarios

Per the prompt, at least 10 new scenarios under
`tests/synthesis_harness/cases_reconcile.py` (or a new
`reconciliation/` subdirectory if we move to per-stage dirs):

* **Auto-merge (3):** identical text + same scope + same kind +
  recent — three flavors covering `state`, `concern`,
  `prediction`.
* **Should NOT reconcile (3):**
  * Same text, **different scope** (different
    `scope_entities`).
  * Same text, **different proposition kind** (`concern` vs
    `state`).
  * Same text, same scope, but **stale** (existing Model older
    than `RECONCILE_RECENCY_WINDOW_DAYS`).
* **Human review queue (2):** medium cosine in
  `[0.70, 0.85)`. Verify the row lands in
  `reconciliation_events` with `decision='human_review'` and
  the original insert proceeds.
* **Supersession (2):** existing Model with state X; new
  evidence stating the contradicting state. Decision: this is
  *contestation*, not reconciliation. The reconciler returns
  `no_match` (different proposition kinds — usually — or
  contradictory truth values caught by a new "polarity"
  signal). The LLM remains responsible for emitting the
  archive op. Document this explicitly so future contributors
  don't try to expand reconciliation into supersession.

Each scenario asserts:

* The expected reconciliation decision (`auto_merge` /
  `human_review` / `no_match`).
* The expected resulting Model state (one row vs two,
  confidence drift if auto-merged).
* The expected `reconciliation_events` row (decision, cosine,
  matched_model_id presence).

## Out of scope for this design (deferred)

* Cross-tenant reconciliation. Hard schema change; not now.
* Reconciler-driven supersession. Either supersession stays
  with the LLM (current direction) or we add a new "polarity"
  axis to propositions and let the reconciler detect
  contradiction. Either is a follow-up.
* Embedding model swaps. The cosine threshold is tied to
  `nomic-embed-text:v1.5`; swapping the embedding model
  invalidates the threshold. This is a known coupling.
* Reconciler-driven merging of distinct LLM-emitted Models that
  arrive in the *same* `RawDiff`. The LLM controls within-diff
  consistency; we don't intervene there.

## Implementation order (for the eventual PR)

1. Migration `db/migrations/00XX_reconciliation_events.sql` — new
   table, indexes. Uses transaction-safe migration runner from
   T3.
2. `services/think/reconciler.py` — pure logic + config + the
   four-signal match function.
3. Wiring in `services/think/applier.py` to call the reconciler
   on each `claim_op.insert` before applying.
4. Metrics on `Metrics` class (T2-style).
5. Harness scenarios (10 minimum) covering the four behaviors.
6. README at `services/think/RECONCILIATION_README.md`
   (operator-facing) explaining the env vars + the kill switch.

## Open questions for review

1. **Threshold values.** `0.85` / `0.70` are intuitions, not
   measurements. Is this an acceptable default range, or do you
   want to defer the actual numbers to an empirical study before
   merge?
2. **Recency window.** 30 days defaults to "recent enough that
   the proposition is the same belief, not a re-derivation." Too
   short? Too long?
3. **The `no_match` row.** Writing one per insert costs roughly
   one row per Think run. Most rows will be `no_match`. Worth
   the storage for the tuning data? The kill via
   `RECONCILE_LOG_NO_MATCH=false` is in the design; what's the
   default?
4. **Should reconciliation also fire for `op="update"` /
   `op="archive"`?** Current design: no, because explicit
   updates are already an LLM judgment about which Model to
   touch. But "two LLM runs both decide to update the same
   Model in the same trigger window" is a real failure mode I'm
   leaving on the table.
5. **Sweeper cadence.** Hourly is a guess. Daily? Tied to
   tenant size? I'm proposing hourly because it's simple; happy
   to defer this to ops.
