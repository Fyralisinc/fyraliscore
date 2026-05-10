# Synthesis-layer harness — structural learnings

52/52 synthetic cases pass. Total wall time 16s with concurrency=4
(LLM cases dominate; non-LLM stages run in <0.5s in aggregate).
The harness now also produces a calibration report under
`--calibration` (T4) and gates regressions against
`baselines/calibration.json`.

The harness lives in [tests/synthesis_harness/](.) and runs end-to-end
with `python -m tests.synthesis_harness`. It uses per-tenant
isolation against a single shared Postgres pool so cases parallelize
without colliding (every production query filters by `tenant_id`).

## Coverage matrix

| Stage           | Cases | Exercises                                                                                                  |
|-----------------|-------|------------------------------------------------------------------------------------------------------------|
| retrieval       | 6     | All 4 pathways (A/B/C/D), RRF multi-pathway fusion, second-pass sparse-result activation                   |
| scope routing   | 5     | Entity precedence, lock-key permutation invariance, tenant isolation, `touched_entity_ids` aggregation     |
| contestation    | 7     | Primary/secondary multipliers, floor clamp, no-standing rejection, owner & contributor standing, reading   |
| falsifier       | 14    | Adequacy across kinds; live evaluators; ISO-8601 + human window parser; malformed-window rejection         |
| cascade         | 6     | Unblock, no-unblock-when-other-deps, decision-revisited flags, depth bound, halt, invariant-violation surfacing |
| reconciliation  | 14    | Applier idempotency, Think+DeepSeek E2E, plus the 10 T5 cases: auto-merge × 3, no-match × 3, human-review × 2, supersession boundary, kill switch |

## Status of original findings

| #  | Finding                                                       | Status |
|----|---------------------------------------------------------------|--------|
| 1  | No "reconciliation" component to test in isolation            | **resolved (T5)** — `services/think/reconciler.py` runs between validate and apply on every `claim_op.insert`; four-signal match (cosine + scope + kind + recency); decisions audited in `reconciliation_events`; 10 harness scenarios cover the four behaviors |
| 2  | Scope routing has 3 orthogonal concerns                       | resolved — independent test cases per concern |
| 3  | Pathway B always pulls everything below k                     | open — known design choice; rank-based assertions used instead |
| 4  | RRF rank-position-based, not score-additive                   | open — informational; assertion uses score ordering |
| 5  | Falsifier evaluation has tight, undocumented vocabulary       | **resolved (T1a)** — parser accepts ISO-8601 + human; malformed raises `MalformedFalsifierError` |
| 6  | Contestability has two paths that look one but aren't         | resolved — separate test cases for belief vs reading |
| 7  | Cascade has hidden invariant coupling                         | **resolved (T1b)** — invariant violations surface on `CascadeResult.invariant_violations` + metric |
| 8  | asyncpg + pgvector codec state is connection-sticky           | **resolved (T2)** — `pgvector_pool_init` + `PGVECTOR_REGISTERED_POOL_IDS` documented in `services/models/PGVECTOR_REGISTRY.md` |
| 9  | Migrations are idempotent in spirit, not in transaction handling | **resolved (T3)** — `lib/shared/migrations.py` wraps each file in a transaction; production `psql --single-transaction` |
| 10 | No automatic test for "Think produced a sensible Model"       | partly addressed (T4) — calibration measurement layer now tracks ECE drift over time; absolute calibration remains a future-work concern |

## Structural learnings — what the harness build process taught me

### 1. There is no "reconciliation" component to test in isolation — *resolved by T5*

The original observation: reconciliation existed only as
per-trigger-id idempotency. Two semantically identical observations
arriving via different `trigger_id`s produced two near-duplicate
Models; the LLM was the only mechanism keeping the surface
deduplicated.

**T5 fix:** `services/think/reconciler.py` runs between validate
and apply on every `claim_op.insert`. It looks for an existing
active Model in the same tenant matching on **all four** of:
embedding cosine similarity, scope overlap, identical proposition
kind, and recency. Three outcomes:

* `auto_merge` (cosine ≥ `RECONCILE_AUTO_MERGE_COSINE`, default
  0.85) — convert insert into a confidence update against the
  matched Model. One row, not two.
* `human_review` (cosine in `[0.70, 0.85)`) — write to
  `reconciliation_events` for triage; original insert proceeds.
* `no_match` — pass through unchanged; audit row records the
  near-miss for tuning data.

The reconciler runs inside the apply transaction, never aborts
apply on its own account, and is opt-out via `RECONCILE_ENABLED`.
Per-trigger-id idempotency via `applied_triggers` is unchanged —
T5 added a *content-level* dedup pass alongside it.

See [services/think/RECONCILIATION_DESIGN.md](../../services/think/RECONCILIATION_DESIGN.md)
for the design rationale and
[services/think/RECONCILIATION_README.md](../../services/think/RECONCILIATION_README.md)
for the operator guide.

### 2. Scope routing has three orthogonal concerns that are easy to conflate

- **Region key** (`region_lock_key` /
  [region_locks.py:134-161](../../services/think/region_locks.py#L134))
  — deterministic SHA-256-based hash for `pg_advisory_xact_lock`.
  Tenant-partitioned. Permutation-stable.
- **Primary entity** (`compute_primary_entity` /
  [region_locks.py:56-85](../../services/think/region_locks.py#L56))
  — fixed precedence `commitment > goal > decision >
  resource/customer > actor`, with id-asc tiebreak. Used in T1 region
  key. Without this, two semantically identical triggers whose
  `entities_mentioned` differ in order would land on different
  advisory locks and racing workers would not serialize.
- **Touched entity set** (`touched_entity_ids` /
  [region_locks.py:227-291](../../services/think/region_locks.py#L227))
  — what region the LLM is *allowed* to mutate. Computed from
  retrieval output before the LLM call; the validator rejects diffs
  that touch entities outside this set, and the caller re-runs
  retrieval with an expanded region.

These three are independent and can fail independently. The harness
tests each in isolation rather than only via Think round-trips.

### 3. Pathway B always pulls everything below `k`

I started writing exclusion-style retrieval assertions ("model X
must NOT be returned"). Those failed because pathway B (semantic
HNSW with default `k=40`) returns every active Model in a tenant
when there are fewer than 40. The merge step combines pathway sets
*by union*, not intersection.

Correct retrieval assertions are about **rank** (multi-pathway hits
must outrank single-pathway), or **per-pathway containment** (this
Model must be in pathway C's result), not exclusion from the merged
list. Adjusted [cases_retrieval.py](cases_retrieval.py) accordingly.

This also means the spec's "diversity" guarantee comes from RRF
weight rebalancing, not from any pathway filtering its own results.

### 4. RRF is rank-position-based, not score-additive

Six dimensions (structural, semantic, temporal, pattern, activation,
provenance) each rank candidates. The fused score is `Σ w_dim / (k +
rank_dim)` with `k=60`
([scoring.py:65](../../services/retrieval/scoring.py#L65)). A Model
missing from a dimension contributes zero to that term — it is *not*
penalized. So a Model present in two pathways at any rank beats a
Model present in only one at the same rank.

The harness's `rrf_fusion_multipathway` case proves this empirically:
a Model with low activation (0.5) but in both A and B beats a Model
with high activation (0.9) present only in B.

### 5. Falsifier evaluation has a tight, undocumented vocabulary

Five legal kinds, each with a kind-specific shape and adequacy rule
([falsifier.py:46-141](../../services/models/falsifier.py#L46)). What
tripped the harness build:

- `within_window` is **not** ISO-8601 ("P7D" silently parses as
  None and the evaluator returns `inconclusive`). It's a regex that
  matches phrases like `"7 days"`, `"4 weeks"`, `"6 hours"`,
  `"any 4-week period"`
  ([evaluators.py:121-134](../../services/workers/deadline_resolver/evaluators.py#L121)).
- `explicit_contestation`'s evaluator joins on
  `observations.content->>'contested_model_id' = prediction_id`. A
  contestation observation that doesn't carry the `contested_model_id`
  field counts as zero contestations → `confirmed`
  ([evaluators.py:561-585](../../services/workers/deadline_resolver/evaluators.py#L561)).
- The default `direction` for `observation_pattern` is `"violates"`
  — a match means the prediction failed, not held. Inverting via
  `direction: "confirms"` is the opt-in.
- `confirmed` for `explicit_contestation` is the *no-contestation*
  case (`count < required actors`). Initially counter-intuitive — but
  consistent with "the prediction holds because no one challenged
  it."

Anything that doesn't match the tiny grammar collapses to
`inconclusive`. There is no surfaced error; the evaluator just
returns `inconclusive` and the LLM gets to decide downstream. This
keeps the worker robust but makes silent misconfiguration easy.

### 6. Contestability has *two* paths that look one but aren't

- **Belief** contestation runs the first-person-override rule
  (×0.3 primary, ×0.5 secondary, floor 0.15) and writes a
  `model_status_notes` row.
- **Reading** contestation marks the contesting actor's
  `signal_readings` entry with `contested: true` and does **not**
  touch `confidence`. `override_applied` is False.

Both paths increment `contested_count` and enqueue a T3 trigger. The
status `contested_false` is in the schema but not written by any
current code path — it's reserved for future T3 LLM output
([prompt.py:601](../../services/think/prompt.py#L601),
[types.py:57](../../lib/shared/types.py#L57)). Confirmed this by
exhaustive grep.

The standing matrix has four bases (scope, owner, contributor,
manager_chain). manager_chain depends on
`services.access_control.hierarchy.is_in_manager_chain`, which is
real now — so a synthetic test that *omits* a manager chain still
relies on it returning False, not raising.

### 7. Cascade has hidden invariant coupling

The cascade unblock branch calls
`commitments_svc.transition(dep_id, "active", cause_event_id=…)`,
which enforces invariant C4 (cause_event_id required) and the
"non-orphan commitment" invariant (must contribute to a goal or be
maintenance). Both must hold or the unblock is silently logged as
`unblock_rejected` and the cascade visits 1 event. Initial cascade
test fixture omitted both → the seed advanced but no children
appeared.

This is fine in production (real LLM-driven flows always create
contributes_to edges), but it's a pitfall when synthesizing minimal
test fixtures. The harness now wires both `cause_obs` and a
`contributes_to` edge for the unblock case.

The `cascade_bound_violation` path **logs but does not raise** —
intentional, per the module docstring. So a cascade that hits
`max_depth` returns a result with `bound_violated=True` and the
caller sees a normal completion.

### 8. asyncpg + pgvector codec state is connection-sticky and pool-shared

The most expensive bug in the harness build. Three observations:

1. `pgvector.asyncpg.register_vector(conn)` mutates the
   connection-level codec map so `vector` columns marshal to/from
   Python lists.
2. Without registration, the same `vector` columns must be passed as
   text literals like `'[0.1, 0.2, …]'::vector`.
3. `services/models/repo.py` registers vector lazily on each
   connection it touches and tracks the registered set in a
   module-level `_VECTOR_REGISTERED_IDS` int-id set. Pathway B
   branches on this set
   ([pathways.py:638-656](../../services/retrieval/pathways.py#L638))
   to decide whether to bind the seed vector as numpy array or
   string literal.

When the harness pool is shared across cases, one case's call to
ModelsRepo can register vector on a connection, which then makes
*any subsequent* fixture write that uses `'[…]'::vector` syntax
silently fail with `could not convert string to float`. The fix
that worked is to register vector on every pooled connection at
init time **and** add the connection's id to
`_VECTOR_REGISTERED_IDS` so retrieval picks the numpy-array
branch. See
[__main__.py:_init_conn](__main__.py).

Treat `_VECTOR_REGISTERED_IDS` as part of the public API of any
test harness that talks to both fixtures and ModelsRepo against
the same pool — it's not, but in practice it has to be.

### 9. Migrations are idempotent in spirit, not in transaction handling

When the harness runs migrations (`for path in sorted(...): await
conn.execute(path.read_text())`), several later migrations fail to
parse against an already-migrated DB and the failure leaves the
*connection* in an aborted-transaction state. Subsequent migrations
on the same connection see "current transaction is aborted, commands
ignored." The harness logs this as a warning and proceeds — every
migration the harness *needs* has already run from prior test
sessions, so the warnings are cosmetic.

Production migration is via a separate, per-file transaction strategy
in `scripts/docker-migrate.sh`. Treating `await conn.execute(file)`
as a drop-in is incorrect for any migration that does multi-statement
work plus expects rollback isolation per file.

### 10. There is no automatic test for "Think produced a sensible Model"

The harness exercises Think end-to-end with the real DeepSeek API,
and asserts only:
- `outcome.status` is `success` or `skipped_idempotent`
- `llm_calls_count >= 1` for the success case
- second call with same `trigger_id` is `skipped_idempotent`

It deliberately does not assert on the LLM's semantic choices — what
proposition it inserts, what falsifier it picks, etc. Asserting that
in a regression harness would either be flaky (random LLM variance)
or shallow (asserting only generic shape). The validator + applier
are the actual contract: anything the LLM emits that survives both
is well-formed by construction.

Implication: confidence in Think correctness comes from
(a) **strict-output schema** rejecting malformed diffs at the
provider boundary, (b) **validator** dropping ops that fail
falsifier-adequacy / threshold / region checks, (c) **applier**
idempotency keyed on trigger_id, and (d) **cascade** invariant
checks. Each of those layers is independently testable; the LLM
itself is not. The harness reflects that — synthetic data + real
deterministic stages, real LLM only at the integration boundary.

## How to extend

Add a case file under [tests/synthesis_harness/](.) following the
pattern in `cases_*.py`:

1. `setup(pool, ctx) -> ctx` — seed synthetic rows under a fresh
   tenant_id (use `await F.make_tenant(conn)`). Setup runs in its
   own transaction.
2. `run(pool, ctx) -> actual` — call the production code path under
   test. Use a fresh transaction.
3. `expected(ctx) -> expected_dict` — derive the intended output.
4. `assertion(actual, expected, ctx) -> (passed, diff_str)` — pure.

Append the case to that file's `CASES` list and to the import block
in [__main__.py](__main__.py). Cases run with `concurrency=8` (or
4 when LLM cases are present). Set `HARNESS_SKIP_LLM=1` to skip
DeepSeek-using cases for fast inner-loop iteration.
