# Ticket #35 — Retire `services/integrations/gmail/watch_scheduler.py` into the M6 framework

**Status:** Deferred. Filed in M6.3 Phase 3 closeout.
**Target milestone:** M7.
**Filed:** 2026-05-19.

## Summary

Convert Gmail's watch-renewal lifecycle (currently a standalone scheduler at [services/integrations/gmail/watch_scheduler.py](../../services/integrations/gmail/watch_scheduler.py)) into a sixth M6 `LongRunningService` that polls for watches needing renewal and renews them through the M6 substrate's named retry helpers and `workflow_states` heartbeat surfaces.

## Why this is needed

The watch scheduler is **steady-state push-notification machinery**: it renews `users.watch` registrations every ~7 days, before Gmail's expiration revokes the subscription. Today it runs as a separate asyncio loop with its own DB poll pattern, its own retry shape, and its own logging — none of which match the M6 framework's pattern-alignment requirements.

Coexistence is stable in M6.3 — the scheduler doesn't conflict with the M6 backfill chain — but the M6 framework is the design north star, and the scheduler is the last Gmail-side asyncio worker outside it.

## Why this is deferred (out of M6.3 scope)

- M6.3's scope is backfill, not steady-state push management.
- The scheduler has no architectural conflict with M6.3; both can run in parallel without sharing state.
- Retirement requires a sixth `LongRunningService` shape (different cadence, different signal model) that's a non-trivial design exercise.

## Scope of work

1. Define a new `services/ingestion/workflows/watch_renewal.py` `LongRunningService` with tick interval ~1 hour.
2. Define the signal shape (no incoming signals — pure DB-scan service) and its CLAIM-VIA-UPDATE pattern for leasing watches due for renewal.
3. Migrate the renewal logic from [watch_scheduler.py::_renew_due_watches](../../services/integrations/gmail/watch_scheduler.py) into the new service, using `retry_with_backoff_on_429` for Gmail-API rate limits.
4. Wire into `services/ingestion/workflows/__main__.py` as a new `WORKFLOW_SERVICE` value.
5. Update the runbook (`docs/ingestion/m5-cutover-runbook.md` §6.D + a new §6.H or similar) with operator procedures.
6. Delete `services/integrations/gmail/watch_scheduler.py` and its callsites (the operator script `scripts/run_gmail_watch_scheduler.py` or equivalent — verify before deletion).
7. Tests:
   - Unit: claim-via-update correctness; renewal retries on 429; expired-token failure path.
   - Subprocess: SIGTERM rc=0 mid-renewal.
   - E2E: existing watch renewal still happens after the cutover.

## Coordination

- Coordinates with **Ticket #37 (Gmail inline-ingestion retirement)** — both are Gmail-side asyncio retirement work; landing them in the same release window minimizes operator confusion.
- Coordinates with **Ticket #36 (OAuth callbacks retrofit)** — the watch scheduler doesn't depend on `onboarding_triggers`, but the operator runbook becomes simpler when all four are landed.

## Out of scope

- The Pub/Sub push handling itself (`services/integrations/gmail/push_handler.py`). That's Ticket #37's territory.
- The inline-ingestion path (`services/ingestion/handlers/gmail.py`). Also Ticket #37.

## Risk if deferred

Low. Coexistence is stable. The risk is operational complexity (two patterns to understand instead of one); not a correctness or availability risk.
