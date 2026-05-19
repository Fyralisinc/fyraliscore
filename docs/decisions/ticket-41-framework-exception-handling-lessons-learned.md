# Ticket #41 — Lessons learned: narrow exception catches at framework dispatch sites (A19 origin)

**Status:** Documentation. Filed during post-M-Load tracker hygiene.
**Target milestone:** n/a (encoded in A19 + `pattern-alignment-rules.md`).
**Filed:** 2026-05-19.

## Symptom

M6.5's merge surfaced that [`SourceOnboarding`'s](../../services/ingestion/workflows/source_onboarding.py) narrow `except NotImplementedError` crashed the orchestrator subprocess when Slack's real planner raised `RuntimeError` on missing `source_client`. The narrow-catch pattern had been inherited at multiple framework dispatch call sites and propagated unchecked through M6.3-M6.5.

The crash signature:

```
Traceback (most recent call last):
  File ".../services/ingestion/workflows/source_onboarding.py", line 602, in _handle_source_requested
    shards = await PLANNER_DISPATCH[source](ctx)
  File ".../services/ingestion/planners/slack.py", line 24, in plan_shards_slack
    raise RuntimeError(
RuntimeError: Slack planner: source_client=None. The PlannerContext factory must supply a SlackClient.
```

Test impact: four tests broke on the M6.5 merge (`test_oauth_trigger_to_source_completion_end_to_end`, `test_shard_fetch_handles_not_implemented_fetcher`, `test_source_onboarding_handles_not_implemented_planner`, `test_source_onboarding_sigterm_subprocess`) — see post-M6.5 fixup commit `29b797c` for the resolution.

## Resolution

[A19 amendment](../ingestion/05-lld-amendments.md#a19--framework-exception-handling-for-per-source-dispatch-failures) (filed concurrent with this ticket) broadens `except NotImplementedError` to `except Exception` at all framework dispatch call sites:

- `SourceOnboarding._handle_source_requested` — broadened in `29b797c`.
- `ShardFetch._run_fetch_loop` — already had broad catch pre-A19; codified by the amendment.
- `Reconciler._handle_source_shards_completed` — broadened in the A19 follow-up commit, including new `_mark_run_failed` helper + `_MARK_RUN_FAILED_SQL`.

Tests verify each broadened site under simulated unexpected exceptions (`RuntimeError` stub injected via `monkeypatch.setitem`):

- [`test_shard_fetch_handles_unexpected_fetcher_exception`](../../services/ingestion/workflows/tests/test_shard_fetch.py)
- [`test_source_onboarding_handles_unexpected_planner_exception`](../../services/ingestion/workflows/tests/test_source_onboarding.py)
- [`test_reconciler_handles_unexpected_dispatch_exception`](../../services/ingestion/workflows/tests/test_reconciler.py)

## Lesson for future framework work

Per-source dispatch entries are net-new code (per [A18.1](../ingestion/05-lld-amendments.md#a181--per-source-backfill-is-net-new-code-not-a-behavior-preserving-refactor)) with realistic failure modes beyond `NotImplementedError`:

- Rate limits (`RuntimeError`, provider-specific exception types).
- Expired credentials (auth-layer exceptions).
- Transient network failures (`httpx.HTTPError`, `asyncio.TimeoutError`).
- Unexpected API responses (`KeyError`, `pydantic.ValidationError`).
- Configuration errors at runtime (the M6.5 case).

**Framework call sites that wrap dispatch calls should catch `Exception` (not narrow subclasses) from day one.** Stub messages can still raise `NotImplementedError` specifically — that's purely a `failure_reason` formatting distinction (operator-facing "not yet implemented" message). The framework's catch is broad regardless; control flow is identical between the narrow and broad branches.

A second-order lesson: when a new framework call site is added in M6.7+ or mega-prompt-2 work, the reviewer should explicitly confirm the broad catch + per-entity failure-marking pattern is present. The pattern-alignment analyzer does NOT enforce this (A19 is a runtime resilience contract, not a structural one); code review is the enforcement surface.

## Action

This is documentation, not work. The lesson is encoded in:

- [A19 amendment](../ingestion/05-lld-amendments.md#a19--framework-exception-handling-for-per-source-dispatch-failures).
- [`pattern-alignment-rules.md`](../ingestion/pattern-alignment-rules.md) — "A19 framework resilience contract — analyzer impact (nil)" section.
- The three new `test_*_handles_unexpected_*_exception` tests.

Future contributors auditing dispatch call sites in new framework code should follow A19.
