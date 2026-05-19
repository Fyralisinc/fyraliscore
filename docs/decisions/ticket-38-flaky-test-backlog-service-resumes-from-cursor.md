# Ticket #38 — Flaky `test_backlog_service_resumes_from_cursor` in combined regression sweeps

**Status:** Deferred. Filed during post-M-Load tracker hygiene.
**Target milestone:** Revisit on three failures across separate CI runs over a quarter.
**Filed:** 2026-05-19.

## Symptom

M3.3's [`test_backlog_service_resumes_from_cursor`](../../services/ingestion/recovery/embedding_backlog/tests/test_embedding_backlog.py) passes consistently in isolation (4/4 green in M3.3-only suite). Failed once during the M6.0 substrate amendment Phase 2 regression sweep (commit `0a5736c`) when run alongside the full workflows + progress + adjacent test suites. Failed again during the post-M-Load A19 regression sweep on `integration/ingestion-hardening` (passed in isolation, failed under combined suite pressure).

## Diagnosis

Testcontainers Redis startup + subprocess interaction timing. The test launches a real subprocess via `subprocess.Popen`; under combined-suite test pressure (multiple containers warming, multiple subprocess inits), the Redis-ready-to-accept-connections window can collide with the subprocess's first Redis call. The flake is test-infrastructure noise, not an M3.3 production bug.

The signal characteristics that point to infrastructure rather than logic:

- Passes in isolation every time (sub-second).
- Fails only when the suite has already exercised multiple containers and subprocesses.
- The failure mode (when observed in detail) shows the subprocess connecting to a Redis instance that's not yet accepting writes, then exiting before the test asserts the resumption checkpoint.
- The M3.3 production code (`embedding_backlog.py`) is itself stable and has shipped against real traffic without producing this signature.

## Status

Not blocking; M3.3 has shipped and is stable. Subsequent regression sweeps (M6.1, M6.2a, M6.2b) did not reproduce this specific flake; the M6.3-M-Load sequence reproduced it once, in the same shape as the M6.0 occurrence.

## Possible fixes (increasing intrusiveness)

1. `@pytest.mark.flaky(reruns=2)` on the specific test. Smallest change; lets the existing infrastructure self-recover.
2. Increase Redis container-ready-wait timeout in the test's fixture. Targets the suspected root cause directly.
3. Split M3.3 backlog tests into a dedicated CI job with isolated container scope. Eliminates cross-suite container contention.
4. Replace subprocess + testcontainers Redis with an in-process stub. Largest change; removes the subprocess + real-container boundary entirely.

## Action

**Defer.** Revisit if this test fails three or more times across separate CI runs over a quarter. This entry exists so future flake observers find a documented diagnosis rather than re-investigating from scratch.

If the threshold is hit, start with option 1 (`pytest.mark.flaky`) and only escalate if the rerun-2 baseline still fails.
