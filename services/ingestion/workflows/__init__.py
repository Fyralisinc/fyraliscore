"""Asyncio orchestration substrate for M6 ingestion workflows.

Per ingestion LLD §2 (workflow orchestration) and
[04-implementation-plan.md §M6](../../../docs/ingestion/04-implementation-plan.md).
Per [05-lld-amendments.md A11](../../../docs/ingestion/05-lld-amendments.md):
Temporal infrastructure is deferred indefinitely. M6 services ship as
long-running asyncio services following M3.3's cursor-persistence
pattern; the modules below provide the substrate that makes those
services portable to Temporal under the A11 trigger conditions.

Substrate modules (Phase 1 of M6.0):
  - `state.py` — `WorkflowState` envelope, `load_state` / `persist_state`,
    and the load-bearing `advance_cursor_atomic_with_kafka_publish`
    primitive (the N1 cursor-data ordering invariant from LLD §3.1).
  - `retry.py` — named retry helpers (`retry_with_backoff_on_429`,
    `retry_with_jitter_on_5xx`, `retry_indefinitely_on_transient`).
    Inline `try/except` retry loops are forbidden by the pattern-
    alignment static analyzer (Rule 3); use these helpers.
  - `runtime.py` — `LongRunningService` abstract base, `make_workflow_pool`
    (sixth `statement_cache_size=0` activation), SIGTERM-clean exit.

Substrate modules (Phase 2 of M6.0):
  - `signals.py` — `emit_signal` / `poll_signals` Postgres-table-based
    signaling (Rule 4 enforcement).
  - `feels_onboarded_monitor.py` — the first real consumer of the
    substrate; LLD §2.6 + §6 progress event emitter.

Pattern-alignment gate (Phase 3 of M6.0):
  - `tests/test_pattern_alignment.py` — static analyzer that walks the
    AST of every module under `services/ingestion/workflows/` and
    asserts the five pattern-alignment requirements. Maintaining the
    rules is what keeps the Temporal port mechanical when A11's trigger
    conditions fire.

The full set of M6 services (OAuth poller / TenantOnboarding /
SourceOnboarding / ShardFetch / per-source backfill) lands in M6.1
through M6.6.
"""
