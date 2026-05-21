# Ticket #47 — M-Validate-Live: live-phase composition + Runs 2/3

**Status:** **RESOLVED** with the M-Validate-Live commit on `feat/ingestion-validation-runs-live-composition` (see [A30](../ingestion/05-lld-amendments.md)). Live-phase orchestration, cross-path dedup (gmail/github/slack), Run 2 (fault injection + A28 positive assertion), and Run 3 (50-tenant concurrency) all delivered + verified. Discord cross-path twin documented as architecturally excluded (disjoint id namespaces), not deferred.

**Originally:** QUEUED. Follow-up to the M-Validate spine (A29). Its own focused work-unit.
**Filed:** 2026-05-20.
**Origin:** Deferred from the validation-run spine (`feat/ingestion-validation-runs-spine`). See [A29's deferral sub-section](../ingestion/05-lld-amendments.md).

## What the spine already delivers (NOT this ticket)

The spine ships the standalone runner + Run 1 (clean-path E2E backfill across all four sources), the fixture-realism pre-flight (A29.4), state reset (A29.2), runner-owned moto (A29.1), the consumer-rc policy (A29.3), and markdown reports. Run 1 is verified: 16 tenants, exact per-source observation counts, external_id parity, zero partition-missing.

## Scope of this ticket

**1. Live-phase orchestration.** Compose the four in-process live generators with the runner so each tenant, after its backfill drains, also ingests live events:
- slack / github / gmail — `services.gateway.main.build_app(pool=...)` mounts all three webhook routers; drive via `SlackWebhookGenerator` / `GithubWebhookGenerator` / `GmailPubSubGenerator` (in-process ASGI). Each needs its env-var signing secret + a seeded `provider_installations` (or `gmail_installations` + watch) row.
- discord — no HTTP layer; build `DispatchDeps` via `build_tenant_resolver` and drive `DiscordGatewayGenerator` directly.
- Phase order per Decision 4: all tenants backfill concurrently → drain → all tenants live concurrently → drain.

**2. Live + cross-path assertions (extends A29 / Decision 5).**
- Live attribution correct, signature gates enforced (tamper → 401, no observation), replay idempotency (at-least-once redelivery dedups).
- **Cross-path**: a backfilled event and a live event for the SAME logical event collapse to one `observations` row (the `(source_channel, external_id, occurred_at)` unique index — `assert_external_id_unique_across_paths` already exists in the spine and will then have live rows to check). Per-tenant timeline monotonic.

**3. Run 2 — fault injection** across all paths (uses the existing `FaultProfile` seam on `BackfillScenario`). Includes the **positive** partition-missing assertion: deliberately inject an out-of-range `occurred_at` and verify A28's DLQ routing fires (`partition_missing` on `ingestion.dlq`), the consumer does NOT crash-loop, and the run continues.

**4. Run 3 — concurrency stress.** 50 tenants (Decision 3). Per-tenant isolation, bounded signal-table backlog, no cross-tenant contamination. (50 tenants flow through the same 7 shared subprocesses — not 50× processes.)

## Discipline

Same verify-before-green discipline as M6.7 and the spine: each phase exercised against the real substrate before claiming pass. The live generators are heterogeneous in-process drivers — this is an architectural surface in its own right, which is why it's a separate work-unit rather than a spine add-on.

## Cross-references

- [A29](../ingestion/05-lld-amendments.md) — the spine + the four new decisions (9–12); this ticket is its deferred remainder.
- A22 / A27 (backfill engine + producer completion). A28 (partition-missing DLQ — Run 2's positive assertion). Ticket #45 (consumer shutdown — once shipped, the spine's rc annotations auto-green).
