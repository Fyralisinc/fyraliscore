# Q1 Scope Audit — M6 backfill producer gap

**Status:** Audit complete (verify-first, no production code changed).
**Branch:** `feat/ingestion-x3-harness-e2e-hardening`.
**Date:** 2026-05-20.
**Author:** pre-implementation audit for mega-prompt 5 → Q1 (X3 harness E2E hardening).

---

## Finding

**M6 backfill has never produced an observation in any test or environment.** The backfill orchestration (plan → fetch → cursor-advance → completion signal) works and reports success, but the final hop — turning fetched records into `observations` rows — is unbuilt. The X3 E2E test never caught this because (a) it is gated off by default (`X3_HARNESS_E2E`), (b) it timed out under pytest's 30s limit before reaching state collection, and (c) its assertions never check observation *count* (only completion + no-duplicates, the latter trivially true on an empty set).

Empirical confirmation (single-tenant Gmail backfill, standalone runner, this environment):

```
onboarding_runs.status            = complete
source_onboarding_runs.status     = completed
tenant_onboarding_completed signal = 1
observations                      = 0      ← expected 5
harness.run() raised: UndefinedColumnError: column "observed_at" does not exist
```

### Root cause (three independent layers)

1. **Harness collection bug.** [`harness.py:593`](../../services/synthetic/backfill_harness/harness.py#L593) `_collect_state` selects `observations.observed_at`; the column is `occurred_at` ([`0001_foundation.sql:68`](../../db/migrations/0001_foundation.sql#L68)). `harness.run()` always raises after the wait phase. *(Trivial.)*

2. **Producer/consumer envelope contract mismatch.** [`shard_fetch.py:414-432`](../../services/ingestion/workflows/shard_fetch.py#L414) (`_build_kafka_message`, used at line 676) publishes an **inline** envelope `{tenant_id, source, shard_id, record}` with **no S3 write**. [`advance_cursor_atomic_with_kafka_publish`](../../services/ingestion/workflows/state.py) publishes those bytes verbatim. But the normalizer ([`normalizer/worker.py:370`](../../services/ingestion/normalizer/worker.py#L370)) does `RawEnvelope.model_validate(...)` then `s3.get(envelope.raw_s3_key)`, and `RawEnvelope` ([`envelope.py:52`](../../services/ingestion/raw_tier/envelope.py#L52)) **requires `raw_s3_key` and is `extra="forbid"`**. The backfill envelope fails validation → skipped. This **contradicts the documented design** (HLD [§02 L208](../ingestion/02-high-level-design.md): "write the raw response to S3 … publish a tiny pointer envelope"; system-design N5: "one normalizer pool consumes all three").

3. **No backfill normalization path even if (2) were fixed.** Two further sub-gaps:
   - **`channel_mapping` has no `backfill` entries.** [`channel_mapping.py:26-40`](../../services/ingestion/normalizer/channel_mapping.py#L26) maps only `(*, webhook|gateway)`. `resolve_channel(source, "backfill")` returns `None` for all four sources → normalizer drops the envelope as `unsupported_combination`.
   - **Fetcher records are wrapper-shaped, not handler-shaped.** The handlers the normalizer dispatches to ([`get_handler(channel)(payload, {})`](../../services/ingestion/normalizer/worker.py#L407)) expect the *webhook/gateway* payload shape, but the backfill fetchers emit wrappers tagged `read_path: "backfill"`:
     - GitHub ([`fetchers/github.py:81`](../../services/ingestion/fetchers/github.py#L81)): `{event_type, repo_full_name, installation_id, payload, read_path}`. The GitHub handler ([`handlers/github.py:495`](../../services/ingestion/handlers/github.py#L495)) reads the event type from the **`X-GitHub-Event` header** — which the normalizer passes as `{}` → handler raises "missing X-GitHub-Event header".
     - Slack ([`fetchers/slack.py`](../../services/ingestion/fetchers/slack.py)): `{channel_id, team_id, installation_id, message, read_path}`. The Slack handler ([`handlers/slack.py:160`](../../services/ingestion/handlers/slack.py#L160)) expects `{event:{type,channel,user,text,ts}}` (event_callback shape).
     - Gmail ([`fetchers/gmail.py:76-81`](../../services/ingestion/fetchers/gmail.py#L76)): docstring states the record "matches the existing inline-handler's raw_payload" → **likely conformant**.
     - Discord: same wrapper pattern as Slack (needs the same treatment).

4. **Writer is flag-gated to a no-op.** [`observation_writer.py:8-11`](../../services/ingestion/writers/observation_writer.py#L8): the writer only calls `ingest_from_draft(...)` when `ingestion.kafka_path_enabled` is TRUE for the tenant; default FALSE → **no Postgres write**. The harness never sets this flag, so even a fully-wired normalizer→writer chain would write nothing.

**Conclusion:** Q1.1's premise ("the normalizer + writer just aren't spawned; add them, 5→7") is insufficient. Producing backfill observations requires finishing the backfill normalization path across **shard_fetch + channel_mapping + per-source handler conformance + writer flag-gating + harness wiring** — a framework work-unit, not a harness fix.

---

## Scope — required changes per file

### A. Trivial / in original Q1 scope

| File | Change | Size |
|---|---|---|
| `services/synthetic/backfill_harness/harness.py:593` | `observed_at` → `occurred_at` | 1 line |
| `services/synthetic/backfill_harness/tests/test_harness_e2e.py` | add `assert_observation_count_matches_fixture` | ~3 lines (will FAIL until B+C land — correctly exposes the gap) |

### B. Producer side (M6 framework)

| File | Change | Size |
|---|---|---|
| `services/ingestion/workflows/shard_fetch.py` | Replace inline `_build_kafka_message` with: serialize `record`→bytes, `compute_content_hash`, `build_raw_s3_key`, `s3.put_if_absent`, build `RawEnvelope(ingress_kind="backfill", …)`. Add `S3Client` to `ShardFetch.__init__` + `main()` wiring (env `S3_ENDPOINT_URL`, `S3_RAW_BUCKET`). Preserve N1: S3-write (content-addressed, idempotent) → Kafka publish → cursor advance. | ~60-100 lines + S3 wiring |
| `services/ingestion/workflows/state.py` | Likely **unchanged** — S3 write happens *before* `advance_cursor_atomic_with_kafka_publish`; the content-addressed PutIfAbsent makes Kafka-retry safe without touching the primitive. (Decision: keep the primitive's contract; do the S3 write in shard_fetch's loop before calling it.) | 0 (preferred) |

### C. Normalization path (M6 framework)

| File | Change | Size |
|---|---|---|
| `services/ingestion/normalizer/channel_mapping.py` | Add 4 entries: `(gmail,backfill)→"gmail:"`, `(github,backfill)→"github:webhook"`, `(slack,backfill)→"slack:message"`, `(discord,backfill)→"discord:message"`. | ~6 lines |
| Per-source handler conformance | The wrapper records (`read_path:"backfill"`) must reach the handlers in the shape they expect. Two viable approaches: **(i)** the normalizer unwraps backfill records and synthesizes the headers the handler needs (e.g. `X-GitHub-Event` from `record["event_type"]`); or **(ii)** register backfill-specific handlers. GitHub + Slack + Discord each need this; Gmail likely already conformant. This is the **bulk + the risk**. | ~80-150 lines across 3 sources + the normalizer dispatch tweak |

### D. Writer + harness (mixed)

| File | Change | Size |
|---|---|---|
| `services/synthetic/backfill_harness/harness.py` | Spawn `normalizer` (`services/ingestion/normalizer/worker.py`) + `observation_writer` (`services/ingestion/writers/observation_writer.py`) → 7 subprocesses; pass `S3_ENDPOINT_URL`/`S3_RAW_BUCKET` env; SIGTERM all 7 in teardown. Set `ingestion.kafka_path_enabled=TRUE` per tenant (write the tenant_flags row). Ensure moto-S3 bucket `fyralis-raw` exists (create-bucket at setup). | ~50-80 lines |
| `_HELPER_TEMPLATE` in harness.py | The injected helper monkeypatches fetcher/reconciler factories; the normalizer + writer subprocesses also import it — verify they tolerate the helper (they don't use the mock factories, but the import must not error). | verify + possibly ~10 lines |

### E. Tests + docs

| File | Change | Size |
|---|---|---|
| `services/synthetic/backfill_harness/tests/test_harness_e2e.py` | Now asserts observation counts per source; runs the 7-subprocess shape. | ~moderate |
| `services/ingestion/normalizer/tests/`, `handlers/tests/` | New tests: `resolve_channel(*, "backfill")`; each backfill record → correct observation via the handler. | ~1 file/source |
| M6.3–M6.6 existing tests | Likely need updates — any test asserting the inline `{shard_id, record}` Kafka shape from shard_fetch breaks when it becomes a RawEnvelope pointer. Audit `services/ingestion/workflows/tests/` + `fetchers/tests/`. | unknown until grep; budget moderate |
| `docs/ingestion/05-lld-amendments.md` | New amendment (A26 or A27): "Backfill producer-side envelope contract — S3-write-then-RawEnvelope-publish; backfill channel mappings; per-source backfill handler conformance." Document the N1 extension to S3. | ~1 amendment |

---

## Risk assessment

| Area | Risk | Notes |
|---|---|---|
| Harness column fix (A) | **None** | Mechanical. |
| shard_fetch S3+envelope (B) | **Medium** | Touches the N1 invariant's hot path. Mitigated by content-addressed PutIfAbsent (idempotent on retry) + keeping the cursor-advance primitive unchanged (S3 write *before* publish). Must verify the existing N1 atomic-rollback tests still hold. |
| Per-source handler conformance (C) | **High** | The biggest unknown. Changing handler input shape or adding backfill handlers risks divergence between the backfill and webhook observation outputs — the whole point (HLD §02 L278) is that a webhook event and a backfilled event produce the *same* `external_id` so the UNIQUE constraint dedups them. Any reshape must preserve external_id derivation exactly. GitHub's header dependency is the sharpest edge. |
| Writer flag-gating (D) | **Low-Medium** | Setting `kafka_path_enabled=TRUE` in the harness is simple, but it routes the tenant onto the Kafka write path — must confirm no interaction with the inline path in the synthetic context. |
| M6.3-M6.6 test updates (E) | **Medium** | Count unknown until audited; changing shard_fetch's published shape will break any test asserting the old inline shape. |
| moto-S3 dependency | **Low** | moto container already running (:5001); needs bucket creation + `S3_ENDPOINT_URL` wiring. aioboto3 path is exercised by existing M2 shadow tests. |

---

## Effort estimate

Rough, in sessions of Claude Code work (1 session ≈ this audit's depth of focused implementation + test):

- B (shard_fetch S3 + envelope + wiring): **~0.5 session**
- C (channel_mapping + per-source backfill conformance, 3 sources + dispatch): **~1.0–1.5 sessions** (bulk + risk)
- D (harness: spawn 2, S3 env, moto bucket, kafka_path flag, teardown, column): **~0.5 session**
- E (E2E + per-source conformance tests + M6.3-M6.6 fixups + amendment): **~1.0 session**

**Total: ~3–4 sessions.** This is an M6.7-sized framework milestone, not a harness hardening task.

---

## Recommendation

**Ship Q1-minimal now + queue a separate M6.7 framework work-unit for the producer fix.**

Rationale:
- The producer gap is genuinely framework-scope (B+C+D), high-risk in the handler-conformance layer (external_id parity is load-bearing), and ~3–4 sessions — well beyond Q1's "additive harness changes" budget. Folding it into Q1 would silently turn a hardening task into a milestone.
- Q1-minimal is safe and immediately valuable: fix the column bug (so `harness.run()` returns) and add `assert_observation_count_matches_fixture` (which will **fail loudly**, converting a silent, untested gap into a visible, tracked failure with a clear message). That assertion failure is the regression-prevention surface that should have existed since A22.
- Mega-prompt 5's backfill validation must wait for M6.7. The **live half (Y1/Y2/Z1) is sound and independently validatable today** — a re-scoped live-only validation can proceed in parallel if desired.

Concretely:
1. **Q1-minimal** (this branch): harness column fix + failing assertion + an amendment documenting the gap and pointing at M6.7. Commit + merge.
2. **M6.7** (new work-unit, own branch): finish the backfill producer (B+C+D+E). Owns the producer-side envelope-contract amendment.
3. **Mega-prompt 5**: resumes after M6.7 merges, unchanged — at that point `harness.run()` genuinely produces observations.

Alternative if backfill validation is not urgent: **live-only mega-prompt 5 now** (Y1+Y2+Z1), defer all backfill validation to post-M6.7.

---

## Cross-references

- A22 (X3 harness architecture) — this audit corrects its implicit "harness produces observations" claim; A22's `assert_observation_count_matches_fixture` exists but was never wired into the E2E test.
- A12/A15/A16 (M6.0 substrate) — **not touched** by the recommended fix; the cursor-advance primitive's contract is preserved (S3 write sits before it).
- HLD §02 L208 + system-design N5 — the design this fix conforms `shard_fetch` to.
- Ticket #39 (concurrent-completion flake) — same X3 path; worth re-checking once observations actually flow.
