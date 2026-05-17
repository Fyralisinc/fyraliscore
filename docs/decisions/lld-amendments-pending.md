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

## 4. Parsed-dict surface → raw bytes contract (M2.2, Discord Gateway)

**Current spec state:** the LLD/HLD describes the shadow path in terms
of "raw body bytes" as if every surface has a wire-level byte stream
available. That is true for HTTP webhook surfaces (`services/webhooks/`)
and the Gmail Pub/Sub push endpoint (raw `request.body()` captured
before JSON parsing). It is NOT true for the Discord Gateway: the WSS
client decodes the JSON frame into a Python dict before the dispatch
layer ever sees it. M2.2 had to choose how to derive the shadow body
for parsed-dict surfaces; the spec doesn't say.

**Decision (M2.2):**

1. **Canonical re-serialisation via orjson.** The Gateway shadow path
   computes `raw_body = orjson.dumps(message, option=OPT_SORT_KEYS)`.
   Sorted keys + orjson's deterministic number/escape formatting give
   byte-equal output for byte-equal logical content, which is what the
   `content_hash` dedup property requires. Discord retransmissions of
   the same message_id arrive byte-identical at the WSS layer, so the
   canonical form also matches across retransmissions.
   See [services/integrations/discord/gateway/dispatch.py:184-244](services/integrations/discord/gateway/dispatch.py#L184-L244).

2. **orjson minor pin.** Because canonical bytes are now part of the
   shadow-path contract (content_hash + N2 replay-from-raw), orjson
   is pinned to `>=3.11,<3.12` in
   [pyproject.toml](pyproject.toml). Patch updates are allowed
   (bug fixes only); minor/major bumps must re-run the round-trip
   test below before being accepted. The previous bound (`>=3.9`)
   was a lower-bound-only spec — too loose for replay determinism.

3. **Round-trip property test.** A new test in
   [services/integrations/discord/gateway/tests/test_dispatch_shadow.py](services/integrations/discord/gateway/tests/test_dispatch_shadow.py)
   (`test_gateway_shadow_body_round_trips_through_handler`) invokes
   the production `_maybe_shadow_write_gateway` helper on a fixture
   MESSAGE_CREATE, captures the bytes that landed in S3, replays them
   through `handle_discord_message` from
   `services/ingestion/handlers/discord.py`, and asserts the resulting
   `ObservationDraft` equals the live dispatch's draft. This pins the
   "canonical bytes round-trip through Fyralis's own normalization
   plane" property against future orjson bumps, canonical-form
   refactors, or handler regressions.

**Proposed amendment to LLD §5 (Raw Tier) — new sub-section "Parsed-dict surfaces":**

> Surfaces whose transport delivers parsed structures (Discord Gateway
> WSS, future SDK clients) MUST derive the raw body via canonical
> JSON re-serialisation:
>
> ```python
> raw_body = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
> ```
>
> Sorted-keys + orjson's deterministic formatting are the contract
> between the shadow writer and the M6 replay path. orjson MUST be
> minor-pinned in `pyproject.toml`. A round-trip test (parsed-dict →
> canonical bytes → handler → ObservationDraft equal to live) MUST
> exist for each parsed-dict surface.

**Why this matters operationally:** without the canonical form
contract, an orjson minor bump that changes float-formatting or key-
escape behaviour would silently break `content_hash` dedup at the S3
layer (two byte-equal frames would hash differently) and produce
divergent observations on replay (M6 replay-from-raw would write a
different `ObservationDraft` than the original live dispatch).
Locking the form + a property test makes the contract enforced, not
implicit.

---

## 5. Handler-registry import graph must be DB-free (M2.3 prerequisite)

**Current spec state:** The LLD describes the normalizer as Path B
("pure transform, no DB access") and lists the handler registry as
its dispatch substrate. What the LLD does NOT say is that the
handler MODULES themselves must not perform DB-touching imports at
module level — i.e. the registry's import graph must be Path B-clean.

**What broke (and why the M2.3 work order forced us to fix it):**

Before M2.3, `services/ingestion/handlers/gmail.py` imported, at
module top level:

  - `lib.shared.tenant_context` (which imports asyncpg)
  - `services.ingestion.core` (which imports asyncpg)
  - `services.integrations.gmail.threading` (which imports
    tenant_context)

These imports were only USED by `dispatch_gmail_message_resource`
(the inline-path dispatcher invoked by push_handler /
history_poller), NOT by the registered `@register("gmail:")`
handler. But Python evaluates module-level imports eagerly, so
importing the handler registry pulled asyncpg + tenant_context
into every consumer of the registry — including the M2.3
normalizer worker, which MUST be DB-free per the load-bearing
Path B invariant.

**What we changed (M2.3):**

`services/ingestion/handlers/gmail.py` now imports those modules
LAZILY (inside the dispatcher function that needs them). The
registered handler function's import graph is unchanged. The dead
import of `bind_tenant` was also removed.

After the change, `import services.ingestion.normalizer.worker`
loads zero DB-related modules. The static proof
(`test_worker_import_graph_contains_no_db_modules`) asserts this
via a subprocess + `sys.modules` inspection.

**Proposed amendment to LLD §5 (Handler Registry section) — new
paragraph "Path B import discipline":**

> Every handler module registered with `@register(channel)` MUST
> keep its module-level import graph free of DB-touching modules
> (asyncpg, `lib.shared.tenant_context`, `services.ingestion.core`,
> `services.observations.repo`, and any module that transitively
> imports them). Handlers that ALSO expose inline-path dispatchers
> (e.g. `gmail.py`'s `dispatch_gmail_message_resource`) must
> lazy-import the DB modules inside the dispatcher function body.
>
> Rationale: the normalizer worker imports the handler registry to
> obtain pure-transform handlers. A top-level DB import in any
> handler module propagates into the normalizer process and breaks
> the Path B contract (LLD §5.2). The contract is enforced by two
> tests in
> `services/ingestion/normalizer/tests/test_worker_no_db_access.py`:
>
>   - `test_worker_import_graph_contains_no_db_modules` — static.
>   - `test_worker_normalize_does_not_touch_asyncpg_under_load`
>     — runtime tripwire.

**Also for M2.3 specifically — testing infrastructure additions:**

  - `testcontainers[kafka]>=4.0` added to dev deps. Drives the
    cooperative-sticky rebalance test (`test_worker_cooperative_
    sticky_rebalance.py`) against a real Kafka broker.
  - `cramjam>=2.8` added to runtime deps. aiokafka's zstd / lz4
    decompression codecs require it. Without this, the normalizer
    consumer raises `UnsupportedCodecError` on the first
    zstd-compressed batch from the shadow path's producer (which
    writes `compression.type=zstd` per LLD §5.2).

**aiokafka assignor strategy note:** LLD §5.2 specifies
"cooperative-sticky" rebalance. aiokafka 0.14.x does NOT expose a
separate `CooperativeStickyAssignor` class; its
`StickyPartitionAssignor` is the nearest equivalent and is what the
worker uses (`services/ingestion/normalizer/worker.py:WorkerConfig.
partition_assignment_strategy`). The LLD should clarify the
class name when it does the next coherence pass, or state explicitly
that aiokafka's Sticky is the implementation choice and the
cooperative semantics come from the broker-side rebalance protocol
(Kafka 2.4+).

---

## Tracking

When the next coherence audit runs, apply all five amendments and
remove this file. No other items pending as of 2026-05-17.
