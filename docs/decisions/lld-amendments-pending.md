# LLD Amendments Pending Next Coherence Audit

Findings surfaced during M1 + M2 implementation that require small
corrections / additions to `docs/ingestion/03-low-level-design.md`
or its sibling docs. None is urgent enough to amend the LLD now;
all should land in the next coherence pass.

---

## 1. §1.6 — Migration 0049 must NOT carry inline `BEGIN/COMMIT`

**Current LLD text:** "this migration uses `BEGIN`/`COMMIT` for the
file structure only — the migration runner must detect
`CONCURRENTLY` and dispatch outside a transaction."

**What we actually implemented (M1.1):**

The runner (`lib/shared/migrations.py:_needs_no_transaction`)
correctly detects `CONCURRENTLY` and skips the txn wrapper. But the
LLD's suggestion to keep inline `BEGIN; … COMMIT;` "for file
structure" is incorrect: an inline explicit `BEGIN` opens a
transaction that Postgres uses for the `CREATE INDEX CONCURRENTLY`
statement, and raises SQLSTATE 25001 ("CREATE INDEX CONCURRENTLY
cannot run inside a transaction block"), regardless of the runner's
choice to skip its own wrapper.

The actual `db/migrations/0049_entity_aliases_normalized_index.sql`
therefore omits `BEGIN/COMMIT` entirely. The file header documents
this as a deliberate departure from the LLD's literal text.

**Proposed amendment:**

> "this migration is non-transactional. The file omits inline
> `BEGIN/COMMIT` because the runner's non-transactional dispatch
> skips its txn wrapper and an inline explicit `BEGIN` would
> reintroduce the txn that `CREATE INDEX CONCURRENTLY` forbids
> (SQLSTATE 25001)."

**Also:** §1.6 should mention the runner's opt-in signal:
`-- migration:no-transaction` (explicit, preferred) with
`CONCURRENTLY` keyword detection as a fallback. 0049 carries the
explicit marker per the M1.1 gate review.

---

## 2. §13 — `acquire.lua` zero-refill sentinel

**Current LLD text (acquire.lua):** computes
`retry_after_ms = math.ceil(deficit / refill_per_sec * 1000)` with
no guard for `refill_per_sec == 0`.

**What breaks:** with `refill_per_sec=0` and an empty bucket, the
division yields `math.huge` in Lua. Redis cannot serialise infinity
as an integer return value — fakeredis raises
`OverflowError: cannot convert float infinity to integer`; real
Redis (via the C-side response writer) similarly fails.

**What we actually implemented (M1 closeout):**

A guard branch in `acquire.lua` returns the sentinel
`retry_after_ms = -1` meaning "indefinite lockout, not recoverable
on its own." The pre-existing lockout-check branch (step 1) still
runs FIRST, so a caller can clear the sentinel state by issuing a
finite `report_retry_after`. Two unit tests pin the contract:
`test_lua_acquire_zero_refill_denies_with_sentinel` and
`test_lua_zero_refill_cleared_by_report_retry_after`.

**Proposed amendment to §13:**

The script header gets the new retry_after_ms contract:

> retry_after_ms semantics:
>   = 0   on grant.
>   > 0   on deny; ms until the bucket can serve `cost` (or
>         lockout-window remainder, whichever applies).
>   = -1  SENTINEL: indefinite lockout. Returned ONLY when
>         refill_per_sec == 0 and tokens < cost. Callers must
>         handle -1 explicitly; do NOT treat as a sleep duration.

And the new branch in the script body, placed BEFORE the existing
deficit-math statement:

```lua
if refill_per_sec == 0 then
    redis.call('HMSET', KEYS[1],
        'tokens', tokens, 'updated_at_ms', now_ms)
    redis.call('PEXPIRE', KEYS[1], 86400000)
    return {0, tokens, -1}
end
```

**Caller responsibility note:** the LLD §13 Python client contract
(`AcquireResult.retry_after_ms: int`) should also be amended to
state the -1 sentinel and require callers to branch on it. The
M1.3 `client.py` already returns it as-is; callers added in M3+
must handle it.

---

## 3. Shadow-write ordering relative to inline `ingest()` (M2.1)

**Current spec state:** neither the LLD nor the HLD specifies an
ordering between the shadow write (S3 PUT + Kafka publish) and
the inline `ingest()` call. HLD "Migration Path" step 2 (line
510 of `02-high-level-design.md`) says the router does both "in
addition to" each other, but is silent on order. LLD §5.4 is the
embedding worker pool and is unrelated. M2.1 had to choose; the
choice + reasoning is documented at
[services/webhooks/router.py:741-771](services/webhooks/router.py#L741-L771).

**Decision:** shadow write runs AFTER successful inline `ingest()`,
not before, not in parallel.

**Rationale (verbatim from the code comment):**
1. Inline is the source of truth during M2. Anything that risks
   inline correctness is wrong.
2. Skips wasted shadow writes when inline rejected the payload
   (`PayloadTooLarge` / `ValidationError` / `HandlerNotFound` — all
   caught above and returned as 4xx before reaching the shadow
   block).
3. The observable divergence shape becomes "inline observation
   exists, shadow record missing" — M2.4's E2E test asserts
   against this direction and ops can detect cleanly via count
   comparison. The opposite ordering (shadow first) would let
   transient inline crashes leave orphan shadow records.

**Proposed amendment:** add one paragraph under HLD "Migration
Path" step 2 (or wherever the shadow-path narrative consolidates)
stating: "The shadow write runs after the inline `ingest()`
returns successfully, before the HTTP 200/201 response. Reason:
preserve inline as the source of truth; constrain the observable
divergence shape to 'inline exists, shadow missing' which the E2E
test (M2.4) asserts against."

---

## Tracking

When the next coherence audit runs, apply all three amendments and
remove this file. No other items pending as of 2026-05-17.
