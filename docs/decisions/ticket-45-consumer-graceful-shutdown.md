# Ticket #45 — Consumer service graceful shutdown semantics (normalizer + observation_writer)

**Title:** Consumer service graceful shutdown semantics — normalizer + observation_writer.
**Status:** **QUEUED.** Not blocking M6.7's observation-production gate (green). Standalone work-unit.
**Filed:** 2026-05-20.
**Origin:** Surfaced during M6.7 verification on `feat/ingestion-x3-harness-e2e-fixes` by `test_harness_sigterm_cleanly_stops_all_seven`. See [A27.6 continuation](../ingestion/05-lld-amendments.md).

## Problem

The X3 harness SIGTERMs all seven subprocesses at teardown and asserts each exits `rc==0`. The five framework services do; the two Kafka-consumer services do not:

- **Normalizer** ([`services/ingestion/normalizer/worker.py:198-213`](../../services/ingestion/normalizer/worker.py)) installs a SIGTERM handler that sets `stop=True`, but the consume loop is `async for msg in consumer:` and only checks `stop` at the top of each iteration. aiokafka's `async for` blocks indefinitely with no timeout when idle, so at teardown (backfill done → no new messages) the stop flag is never observed → the harness SIGKILLs after its 15s deadline (**rc=-9**).
- **Observation writer** ([`services/ingestion/writers/observation_writer.py:407-450`](../../services/ingestion/writers/observation_writer.py)) installs no signal handler at all. SIGTERM hits Python's default handler → immediate termination (**rc=-15**), with no graceful `consumer.stop()`.

Pre-existing: the normalizer/writer signal handling predates M6.7. It surfaced now because M6.7's harness teardown is the first scenario that exercises SIGTERM on these consumers in a live subprocess test.

## Reference pattern (the framework services that exit rc=0)

`LongRunningService` ([`services/ingestion/workflows/runtime.py`](../../services/ingestion/workflows/runtime.py)) loops `while not stop_event.is_set()` and sleeps via `asyncio.wait_for(stop_event.wait(), timeout)`. Polling-with-timeout makes the stop flag observable every cycle, so SIGTERM → set stop_event → loop exits cleanly → `rc=0`. The consumer services should mirror this: poll with `getmany(timeout_ms=...)` instead of an unbounded `async for`.

## Fix shape

- Restructure both consume loops to `await consumer.getmany(timeout_ms=500)` (or similar) inside `while not stop_event.is_set()`.
- Add a SIGTERM handler to the writer's `main()` parallel to the normalizer's.
- **Preserve at-least-once commit semantics** across the restructure: commit after processing a batch, before the next `getmany`.
- Test coverage: SIGTERM-while-idle (no messages in flight) AND SIGTERM-while-processing (mid-batch). Both must exit `rc=0` and not lose/duplicate observations.

## Scope

~10-20 lines per service across two production files + tests + an amendment. Standalone work-unit; **not** part of M6.7.

## Visible-failure mechanism

`test_harness_sigterm_cleanly_stops_all_seven` stays **RED** on `integration/ingestion-hardening` until this ticket ships. The failing test is the regression-prevention surface — **DO NOT** suppress, `@xfail`, or otherwise mask it to make CI green (same discipline as Q1-minimal's observation-count assertion before M6.7 closed it).

## Cross-references

- [A27.6 continuation](../ingestion/05-lld-amendments.md) — where this was surfaced.
- A19 (framework exception handling) — the framework dispatch loop's discipline the consumers should match.
- Ticket #43 (M6.7 backfill producer completion) — the work-unit whose verification surfaced this.
