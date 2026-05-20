# Ticket #44 — Operational decision: observations partition coverage range

**Title:** Operational decision — `observations` partition coverage range.
**Status:** **TRACKED**, not blocking. Deferred to an operational conversation (post-pilot).
**Filed:** 2026-05-20.
**Origin:** Surfaced by [A28](../ingestion/05-lld-amendments.md) during M6.7 verification on `feat/ingestion-x3-harness-e2e-fixes`.

## Trigger

A28 made the observation writer route missing-partition inserts to the DLQ (permanent error) instead of crash-looping. That fix surfaced the underlying operational question: the `observations` table is range-partitioned by `occurred_at` with monthly partitions currently covering **2025-01 → 2027-01**. Backfill of historical data older than that coverage produces observations whose `occurred_at` finds no partition — they now DLQ (`partition_missing`) rather than land.

## The decision to make

For each tenant/source, how far back do customers expect to backfill? Two levers:

1. **Extend partition coverage** backwards (and/or forwards) to cover the expected backfill horizon, so historical observations land directly.
2. **Accept DLQ-routing** of pre-coverage observations and reprocess from the DLQ after extending coverage, treating very-old data as opt-in.

## Inputs needed

- **Customer expectation:** how much history a typical onboarding backfills (days? months? years?).
- **Storage cost:** each monthly partition's footprint × the extension horizon.
- **Query performance:** partition count vs. planner overhead; whether very-old partitions need different indexing/retention.
- **Retention policy:** interaction with any observations TTL/archival.

## Scope

Operational analysis + a partition-provisioning decision (likely a migration + a partition-management routine). **Not** a code-classification change — A28 already made the writer behave safely when a partition is missing. Defer to an operational conversation post-pilot.

## Cross-references

- [A28](../ingestion/05-lld-amendments.md) — the writer fix that made missing-partition safe (DLQ, not crash-loop) and surfaced this question.
- Ticket #43 (M6.7 backfill producer completion) — the work-unit whose verification surfaced this.
- Migration `0046_ingestion_failures.sql` (`partition_missing` DLQs land as `observation_insert_error`).
