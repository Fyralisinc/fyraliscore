# Ticket #37 — Retire Gmail steady-state inline-ingestion path

**Status:** Deferred. Filed in M6.3 Phase 3 closeout.
**Target milestone:** M7. Lands after Ticket #36 (OAuth retrofit) so backfill is exercising the framework end-to-end first.
**Filed:** 2026-05-19.

## Summary

Convert [services/integrations/gmail/fetcher.py::drain_mailbox_history](../../services/integrations/gmail/fetcher.py) to publish to Kafka `ingestion.raw` instead of dispatching inline through [services/ingestion/handlers/gmail.py::dispatch_gmail_message_resource](../../services/ingestion/handlers/gmail.py). After this change, all Gmail observations — whether from backfill or from steady-state push/poll — flow through the same Kafka topic, normalizer, and writer.

## Why this is needed

Today's Gmail data flow is bifurcated:

| Path | Trigger | Writer |
|---|---|---|
| **Backfill (M6.3, new)** | `onboarding_triggers` → M6 chain | Kafka `ingestion.raw` → normalizer → observation writer |
| **Steady-state (existing)** | Pub/Sub push or 10-min poll | Inline via `dispatch_gmail_message_resource` → `observations` directly |

The two writers have **subtly different shapes**: the inline path does its own thread canonicalization (via `services/integrations/gmail/threading.py`) before writing; the Kafka-published path defers thread canonicalization to the writer. Over time, divergence is a correctness risk — bugs found in one path don't necessarily propagate to the other.

After this ticket lands, only one writer path exists. The existing steady-state code (`fetcher.py`, `history_poller.py`, `push_handler.py`) keeps its responsibility for discovering new messages via Gmail's `users.history.list`, but instead of dispatching them inline, it publishes them to Kafka in the same envelope shape M6.3 produces.

## Why this is deferred from M6.3

- Touching `services/ingestion/handlers/gmail.py` affects every downstream consumer of `observations` (writer ordering, dedup, threading); substrate-level change requiring its own work-unit.
- Pre-M6.3, no observations would flow through the framework's Kafka path; testing the cutover would have no real traffic until the F4 retrofit landed.
- M6.3's backfill path provides the parallel implementation to validate the cutover against.

## Scope of work

1. **`services/integrations/gmail/fetcher.py::drain_mailbox_history`:** replace the `dispatch_gmail_message_resource` call with `producer.produce(topic="ingestion.raw", value=envelope, key=tenant_id_bytes)` where `envelope` matches the M6.3 fetcher's envelope shape (see [services/ingestion/fetchers/gmail.py::_build_record](../../services/ingestion/fetchers/gmail.py)).
2. **Watermark handoff:** today `gmail_mailbox_watches.history_id` is the steady-state watermark. After cutover, it should be:
   - The single source of truth for both backfill and steady-state OR
   - Backfill's `workflow_states.state_data["cursor"]["final_history_id"]` becomes the canonical watermark, and `gmail_mailbox_watches.history_id` becomes a mirror.
   - Choose during ticket implementation; both have tradeoffs.
3. **Remove `services/ingestion/handlers/gmail.py::dispatch_gmail_message_resource`:** after the inline path is retired, nothing calls it. The pure-function `handle_gmail` is still useful as the normalizer's `gmail:` handler (it doesn't touch the DB; safe to keep in the registry).
4. **Migrate `services/integrations/gmail/threading.py`'s canonicalization** into the writer or into the normalizer; today it's called by the inline path.
5. **Update [push_handler.py::_drain_history](../../services/integrations/gmail/push_handler.py):** the import target stays the same (`drain_mailbox_history`) but its semantics now publish to Kafka. Update the docstring.
6. **Update `scripts/run_gmail_history_poller.py`:** no code change needed if it just invokes the existing entry point, but its docstring should reflect the new write path.
7. **Tests:**
   - Steady-state Gmail messages land in Kafka `ingestion.raw` with the same envelope shape as backfill.
   - Existing dedup invariants hold (observations.UNIQUE + gmail_thread_members.PK).
   - Watermark advances correctly across both backfill and steady-state.
8. **Update runbook §6.D** to retire the two-path coexistence section.

## Out of scope

- The Pub/Sub push subscription / topic management. That's stable.
- Discovery (`users.history.list`) itself. The discovery code is unchanged; only its write-side dispatch changes.
- M-Load (the Kafka readers + synthetic harness for the cutover).

## Risk if deferred indefinitely

Medium. The two paths can drift over time; bugs found in one don't necessarily propagate. But coexistence is stable in the short term — both paths produce correct observations. The cost of deferral is operational complexity (two code paths to maintain) rather than correctness.

## Coordination

- **Depends on Ticket #36** landing first (so backfill is producing real traffic; the cutover has something to validate against).
- **Coordinates with Ticket #35** (watch_scheduler retirement) — both are Gmail-side asyncio retirement; landing both in the same release minimizes operator confusion.
- **Sets the pattern for analogous retirement work** in M6.4-M6.6 sources (Slack/GitHub/Discord each have inline ingestion paths that will also need retirement; file analogous tickets when those sources' M6.x sub-blocks close).
