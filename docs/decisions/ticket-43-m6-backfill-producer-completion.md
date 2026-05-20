# Ticket #43 — M6.7: Backfill producer completion

**Title:** M6.7 — Backfill producer completion (shard_fetch S3 + envelope + channel mapping + handler conformance + writer flag).
**Status:** Queued. Required before mega-prompt 5's backfill validation can proceed. **Not deferred indefinitely** — next focused work-unit conversation.
**Filed:** 2026-05-20.
**Origin:** Pre-implementation audit for mega-prompt 5; see [`q1-backfill-producer-gap-scope.md`](./q1-backfill-producer-gap-scope.md) for the full per-file enumeration, risk assessment, and effort estimate, and [A26](../ingestion/05-lld-amendments.md) for the architectural framing.

## Problem

M6 backfill has never produced an observation end-to-end in any test or environment. The orchestration completes and signals success, but fetched records never reach the `observations` table. Q1-minimal (commit on `feat/ingestion-x3-harness-e2e-hardening`) fixed the harness column bug and added a **failing** `assert_observation_count_matches_fixture` to the X3 E2E test to make the gap visible. This ticket tracks the framework fix that makes that assertion pass.

## Load-bearing constraint

**`external_id` parity.** A webhook-delivered event and the *same* event later re-fetched by backfill/reconciliation must derive the **identical** `external_id` so the `observations UNIQUE(source_channel, external_id, occurred_at)` index dedups them to one row (HLD §02 L278). Any handler-conformance reshape (sub-block M6.7.3) MUST preserve each source's existing external_id derivation exactly. This is the highest-risk part of the work and needs an explicit verification approach (e.g., a test that feeds the same logical event through both the webhook handler and the backfill path and asserts equal external_id).

## Scope — four discrete sub-blocks

- **M6.7.1 — Producer S3 + envelope.** `shard_fetch` writes each fetched record's raw bytes to S3 (`put_if_absent`, content-addressed) and publishes a `RawEnvelope(ingress_kind="backfill", raw_s3_key, content_hash, …)` instead of the inline `{shard_id, record}` shape. Wire an `S3Client` into the `ShardFetch` service + `main()` (env `S3_ENDPOINT_URL`, `S3_RAW_BUCKET`). Preserve N1: S3-write → Kafka-publish → cursor-advance, with the cursor primitive's contract unchanged (A12/A15/A16 untouched).
- **M6.7.2 — Channel mapping.** Add `(gmail,backfill)`, `(github,backfill)`, `(slack,backfill)`, `(discord,backfill)` entries to `normalizer/channel_mapping.py`.
- **M6.7.3 — Per-source handler conformance.** Make the wrapper-shaped backfill fetcher records (`read_path:"backfill"`) dispatch correctly through the handler registry — either the normalizer unwraps + synthesizes the headers the handler needs (e.g. `X-GitHub-Event` from `record["event_type"]`), or backfill-specific handlers are registered. Sources: GitHub, Slack, Discord (Gmail likely already conformant). **Preserve external_id parity (see above).**
- **M6.7.4 — Writer flag + harness co-spawn.** The X3 harness sets `ingestion.kafka_path_enabled=TRUE` per tenant (so `observation_writer` writes instead of no-oping), spawns the `normalizer` + `observation_writer` subprocesses (5→7), wires S3 env, creates the moto-S3 bucket `fyralis-raw` at setup, and SIGTERMs all 7 in teardown.

## Acceptance

- `test_harness_single_tenant_gmail_completes` (and the parallel-4-sources test, with per-source `expected_observation_count`) pass `assert_observation_count_matches_fixture` — green, no suppression.
- A test demonstrating `external_id` parity between the webhook and backfill paths for at least one source.
- Existing M6.3–M6.6 tests updated for the new shard_fetch published shape; full ingestion + synthetic suites green.
- New amendment documenting the producer-side envelope contract + the S3-write-before-publish invariant (supersedes A26's "pending" status).

## Sequencing

1. ✅ Q1-minimal (column fix + failing assertion + A26 + this ticket) — `feat/ingestion-x3-harness-e2e-hardening`.
2. M6.7 architectural conversation (per-source M6.7.3 decisions; external_id parity verification approach).
3. M6.7 mega-prompt drafted + executed.
4. M6.7 merges to the integration branch.
5. Mega-prompt 5 resumes, unchanged — now against a foundation that produces backfill observations.

## Cross-references

- [`q1-backfill-producer-gap-scope.md`](./q1-backfill-producer-gap-scope.md) — full scope/risk/effort.
- [A26](../ingestion/05-lld-amendments.md) — architectural framing; "pending M6.7" status this ticket resolves.
- A22 (X3 harness), A12/A15/A16 (M6.0 substrate — not affected), HLD §02 L208 + L278, system-design N5.
- Ticket #39 (concurrent-completion flake) — same X3 path; re-check once observations flow.
