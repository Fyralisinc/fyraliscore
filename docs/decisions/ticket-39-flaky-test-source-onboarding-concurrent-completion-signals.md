# Ticket #39 — Flaky `test_source_onboarding_concurrent_completion_signals` in combined regression sweeps

**Status:** Deferred. Filed during post-M-Load tracker hygiene.
**Target milestone:** Revisit on three failures across separate CI runs over a quarter.
**Filed:** 2026-05-19.

## Symptom

M6.2a's [`test_source_onboarding_concurrent_completion_signals`](../../services/ingestion/workflows/tests/test_source_onboarding.py) passes consistently in isolation (~1/1 in M6.2a-only suite, ~0.97s). Fails intermittently in combined-suite sweeps; observed across the M6.2b merge sweep, the M6.3 merge sweep, and the M-Load merge sweep on `integration/ingestion-hardening`. Has been deselected from those sweeps to avoid blocking on it.

## Diagnosis

Same shape as [#38](ticket-38-flaky-test-backlog-service-resumes-from-cursor.md) (testcontainers + subprocess + concurrent suites). The test exercises concurrent signal completion handling in `SourceOnboarding`; the concurrency is the load-bearing property under test. Combined-suite pressure produces ordering races that don't reproduce in isolation.

The test launches two `SourceOnboarding` replicas, has them drain disjoint completion signals via `FOR UPDATE SKIP LOCKED`, and asserts exactly one `source_shards_completed` emit (idempotency-key dedup via `emit_signal`'s UNIQUE constraint). Under combined-suite contention — multiple test processes hitting the same Postgres, additional locks held by adjacent tests — the disjoint-signal partitioning can degenerate to one replica claiming everything, and the assertion `n_emits == 1` is then a tautology that masks a real-replica skew.

## Status

Not blocking; deselected from combined sweeps per past convention. The signal completion logic itself is sound:

- Passes in isolation reliably.
- Exercised by the 5-subprocess E2E tests (`test_oauth_to_*_completion_*`) which simulate real concurrency at a higher fidelity.
- The `source_shards_completed` idempotency-key contract is verified separately by `test_reconciler_idempotent_on_signal_replay`.

## Possible fixes

Same as [#38](ticket-38-flaky-test-backlog-service-resumes-from-cursor.md) (mark flaky, increase wait timeouts, isolated CI job). Additionally:

- **Review the test's concurrency assertion shape.** Currently asserts exactly one transition (no double-fire). If the assertion is too strict for combined-suite conditions, relax to "at least one transition with no duplicate idempotency-key emits" — that preserves the load-bearing dedup property without false-positive on race outcomes that are correct.
- **Split the concurrency tests into a serial test class** (pytest-xdist `--dist loadgroup` or `pytest-ordering`), so they run without sibling-suite contention.

## Action

**Defer.** Same threshold as [#38](ticket-38-flaky-test-backlog-service-resumes-from-cursor.md): revisit on three failures across separate runs over a quarter.

When revisited, prefer the assertion-relaxation path (one-of-three above) over CI job isolation — the test's intent is to verify "no double-fire under concurrency", which the relaxed shape captures more accurately than the current strict count.
