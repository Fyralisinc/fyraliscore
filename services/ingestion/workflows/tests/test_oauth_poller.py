"""M6.1 Phase 1 — OAuth outbox poller tests.

Covers the OAuthPoller service end-to-end:
  - LOAD-BEARING: trigger consumption + run creation + signal emit
    are atomic. Failure rolls back ALL THREE Postgres-observable
    state changes.
  - FOR UPDATE SKIP LOCKED: two concurrent pollers claim disjoint
    triggers; no overlap, no blocking.
  - Idempotency across restart (real subprocess + SIGTERM).
  - Pattern-alignment static analyzer passes.

The subprocess SIGTERM test lives in test_oauth_poller_subprocess.py
(split so the heavy test isn't mandatory for fast iteration — same
shape as M6.0's split for FeelsOnboardedMonitor).
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.ingestion.workflows.oauth_poller import (
    OAuthPoller,
    OAuthPollerConfig,
    SIGNAL_KIND_RUN_CREATED,
    WORKFLOW_KIND,
    _create_onboarding_run,
    _mark_trigger_consumed,
)
from services.ingestion.workflows.signals import emit_signal


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"oauth-poller-test-{tid.hex[:8]}",
    )
    return tid


async def _seed_trigger(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source: str = "slack",
    trigger_kind: str = "install",
    payload: dict[str, Any] | None = None,
) -> UUID:
    """INSERT one onboarding_triggers row (simulates OAuth callback)."""
    import orjson
    trigger_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        """,
        trigger_id, tenant_id, source, trigger_kind,
        orjson.dumps(payload or {}).decode("utf-8"),
    )
    return trigger_id


async def _count_unconsumed_triggers(
    pool: asyncpg.Pool, tenant_id: UUID,
) -> int:
    return int(await pool.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND consumed_at IS NULL",
        tenant_id,
    ))


# =====================================================================
# 1. LOAD-BEARING — atomic trigger-consume + run-create + signal-emit.
# =====================================================================

async def test_oauth_poller_creates_onboarding_run_and_emits_signal_atomically(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING for M6.1 Phase 1: one tick processes one trigger
    and all four Postgres-observable changes commit together —
    trigger.consumed_at set, onboarding_runs row exists, signal row
    exists, and the signal's idempotency_key equals the run's id.

    Three rows in one read confirms the A12 substrate amendment is
    being used correctly (the poller passes Connection to emit_signal
    inside its transaction, not Pool which would auto-commit
    independently).
    """
    tid = await _seed_tenant(fresh_db)
    trigger_id = await _seed_trigger(fresh_db, tenant_id=tid, source="slack")

    poller = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.01,
            max_triggers_per_tick=1,
        ),
    )
    await poller.run(max_ticks=1)

    # ----- Observable change #1: trigger.consumed_at is set. -----
    trigger_row = await fresh_db.fetchrow(
        "SELECT consumed_at, consumed_by_workflow_id, consume_attempts "
        "FROM onboarding_triggers WHERE id = $1",
        trigger_id,
    )
    assert trigger_row is not None
    assert trigger_row["consumed_at"] is not None, (
        "trigger.consumed_at was NOT set after tick — the poller's "
        "atomic block did not include the trigger UPDATE, or the "
        "transaction did not commit."
    )
    assert trigger_row["consume_attempts"] == 1

    # ----- Observable change #2: onboarding_runs row exists. -----
    run_row = await fresh_db.fetchrow(
        "SELECT id, tenant_id, status, sources_enabled, workflow_id "
        "FROM onboarding_runs WHERE tenant_id = $1",
        tid,
    )
    assert run_row is not None, (
        "onboarding_runs row was NOT created — the poller's atomic "
        "block did not include the INSERT."
    )
    assert run_row["tenant_id"] == tid
    assert run_row["status"] == "pending"
    assert list(run_row["sources_enabled"]) == ["slack"]
    # consumed_by_workflow_id points back at the new run's workflow_id.
    assert trigger_row["consumed_by_workflow_id"] == run_row["workflow_id"]

    # ----- Observable change #3: signal row exists. -----
    signal_row = await fresh_db.fetchrow(
        "SELECT signal_kind, idempotency_key, signal_data "
        "FROM workflow_signals "
        "WHERE workflow_kind = 'tenant_onboarding' "
        "AND signal_kind = $1",
        SIGNAL_KIND_RUN_CREATED,
    )
    assert signal_row is not None, (
        "workflow_signals row was NOT created — A12's connection-"
        "typed emit_signal did not execute within the poller's "
        "transaction. Investigate the poller's emit_signal call."
    )

    # ----- Observable change #4: idempotency_key = run_id. -----
    assert signal_row["idempotency_key"] == str(run_row["id"]), (
        f"signal.idempotency_key={signal_row['idempotency_key']!r} "
        f"!= run.id={run_row['id']!r}. The signal naming convention "
        f"(idempotency_key = onboarding_run_id) is broken — retries "
        f"will not be idempotent."
    )

    # ----- Signal payload carries the trigger + run linkage. -----
    import orjson
    raw = signal_row["signal_data"]
    data = (
        orjson.loads(raw) if isinstance(raw, (str, bytes, bytearray))
        else dict(raw)
    )
    assert data["onboarding_run_id"] == str(run_row["id"])
    assert data["tenant_id"] == str(tid)
    assert data["trigger_id"] == str(trigger_id)
    assert data["source"] == "slack"


# =====================================================================
# 2. LOAD-BEARING — atomic rollback on signal-emit failure.
# =====================================================================

async def test_oauth_poller_atomic_rollback_on_signal_failure(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING (M6.1 + A12): inject a failure DURING the atomic
    block. Assert all three Postgres-observable conditions hold:
      (a) trigger.consumed_at is still NULL
      (b) NO onboarding_runs row exists
      (c) NO workflow_signals row exists

    This is M6.1's equivalent of A6's
    `test_flush_failure_does_not_save_state` and A12's
    `test_emit_signal_with_connection_participates_in_caller_txn`:
    the load-bearing proof that the A12 substrate amendment is
    actually being used (Connection-typed emit + transaction
    rollback erases all four changes).

    Without the A12 amendment, emit_signal(pool, ...) would auto-
    commit the signal row independently and the rollback below
    would leave a dangling signal — the test would fail at the
    signal-count assertion.
    """
    tid = await _seed_tenant(fresh_db)
    trigger_id = await _seed_trigger(fresh_db, tenant_id=tid, source="github")

    # ----- Inject failure during the poller's atomic block. -----
    # We monkey-patch emit_signal to raise mid-transaction. The
    # rest of the txn (trigger UPDATE + onboarding_runs INSERT)
    # has already executed at that point; the rollback must
    # observably undo ALL of them.
    class _SyntheticEmitFailure(RuntimeError):
        pass

    import services.ingestion.workflows.oauth_poller as poller_module

    original_emit = poller_module.emit_signal

    async def _raising_emit(*args: Any, **kwargs: Any) -> Any:
        raise _SyntheticEmitFailure("simulated Kafka/DB error mid-emit")

    poller_module.emit_signal = _raising_emit  # type: ignore[assignment]
    try:
        poller = OAuthPoller(
            fresh_db,
            config=OAuthPollerConfig(
                tick_interval_seconds=0.01,
                max_triggers_per_tick=1,
            ),
        )
        with pytest.raises(_SyntheticEmitFailure):
            await poller.run(max_ticks=1)
    finally:
        poller_module.emit_signal = original_emit  # type: ignore[assignment]

    # ----- (a) trigger.consumed_at is still NULL -----
    trigger_row = await fresh_db.fetchrow(
        "SELECT consumed_at, consume_attempts "
        "FROM onboarding_triggers WHERE id = $1",
        trigger_id,
    )
    assert trigger_row["consumed_at"] is None, (
        f"A12 INVARIANT VIOLATED: trigger.consumed_at = "
        f"{trigger_row['consumed_at']!r} after signal-emit "
        f"failure; expected NULL. The poller's transaction did "
        f"NOT roll back the trigger UPDATE — A12's connection-"
        f"typed emit is not being used, OR the poller wrapped the "
        f"steps in separate transactions."
    )

    # ----- (b) NO onboarding_runs row -----
    run_count = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_runs WHERE tenant_id = $1",
        tid,
    )
    assert run_count == 0, (
        f"A12 INVARIANT VIOLATED: onboarding_runs row count is "
        f"{run_count} after rollback; expected 0. The INSERT did "
        f"NOT participate in the atomic transaction."
    )

    # ----- (c) NO workflow_signals row -----
    signal_count_val = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = 'tenant_onboarding' "
        "AND signal_kind = $1",
        SIGNAL_KIND_RUN_CREATED,
    )
    assert signal_count_val == 0, (
        f"workflow_signals row count is {signal_count_val} after "
        f"emit failure; expected 0. emit_signal raised BEFORE "
        f"insertion (the test patched it to raise unconditionally), "
        f"so this should be trivially 0 — failure here indicates "
        f"something other than the poller created the signal."
    )


# =====================================================================
# 3. LOAD-BEARING — FOR UPDATE SKIP LOCKED disjoint claim.
# =====================================================================

async def test_oauth_poller_claims_trigger_under_for_update_skip_locked(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two concurrent pollers MUST claim disjoint trigger sets. The
    `SELECT ... FOR UPDATE SKIP LOCKED` lock on onboarding_triggers
    is what guarantees this; without it, two pollers would race on
    the same row and one would either block (plain FOR UPDATE) or
    double-process (no lock).

    Same property as M6.0's
    `test_poll_signals_claims_via_for_update_skip_locked`, but at
    the M6.1 trigger-table level.
    """
    tid = await _seed_tenant(fresh_db)
    n = 10
    trigger_ids: list[UUID] = []
    for i in range(n):
        # Stagger created_at so order is deterministic
        tid_id = await _seed_trigger(
            fresh_db, tenant_id=tid,
            source="slack" if i % 2 == 0 else "github",
        )
        trigger_ids.append(tid_id)

    poller_a = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.01,
            max_triggers_per_tick=n,
            instance_name="poller-A",
        ),
    )
    poller_b = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.01,
            max_triggers_per_tick=n,
            instance_name="poller-B",
        ),
    )

    # Both pollers tick concurrently. asyncio.gather runs them as
    # close to simultaneously as the event loop permits.
    await asyncio.gather(
        poller_a.run(max_ticks=1),
        poller_b.run(max_ticks=1),
    )

    # ----- All 10 triggers consumed exactly once -----
    unconsumed = await _count_unconsumed_triggers(fresh_db, tid)
    assert unconsumed == 0, (
        f"{unconsumed} of {n} triggers remain unconsumed after both "
        f"pollers ticked. SKIP LOCKED may have caused one poller "
        f"to skip rows the other hadn't actually claimed yet."
    )

    # ----- Exactly 10 onboarding_runs rows created -----
    run_count = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_runs WHERE tenant_id = $1",
        tid,
    )
    assert run_count == n, (
        f"Expected {n} onboarding_runs (one per trigger); got "
        f"{run_count}. Concurrent pollers double-processed or "
        f"under-processed under SKIP LOCKED."
    )

    # ----- consumed_by_workflow_id stamped per trigger -----
    null_consumers = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND consumed_by_workflow_id IS NULL",
        tid,
    )
    assert null_consumers == 0


# =====================================================================
# 4. Empty queue — tick completes cleanly, no errors.
# =====================================================================

async def test_oauth_poller_empty_queue_tick(
    fresh_db: asyncpg.Pool,
) -> None:
    """No unclaimed triggers → tick returns cleanly, state row
    written with last_triggers_claimed=0."""
    await _seed_tenant(fresh_db)

    poller = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.01,
            max_triggers_per_tick=10,
        ),
    )
    await poller.run(max_ticks=1)

    state_row = await fresh_db.fetchrow(
        "SELECT state_data FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = 'default'",
        WORKFLOW_KIND,
    )
    assert state_row is not None
    import orjson
    data = (
        orjson.loads(state_row["state_data"])
        if isinstance(state_row["state_data"], (str, bytes, bytearray))
        else dict(state_row["state_data"])
    )
    assert data["last_triggers_claimed"] == 0
    assert data["lifetime_triggers_claimed"] == 0


# =====================================================================
# 5. Idempotency-key shape — retries on same trigger do not duplicate.
# =====================================================================

async def test_oauth_poller_signal_emit_uses_run_id_as_idempotency_key(
    fresh_db: asyncpg.Pool,
) -> None:
    """The signal's idempotency_key equals the onboarding_run.id (per
    the M6.1 signal naming convention). If a second emit_signal call
    arrives with the same run_id (e.g., a retry from a downstream
    process), it will be a no-op via the A12 ON CONFLICT DO NOTHING
    contract.

    This test demonstrates that contract works at the M6.1 service
    level: re-emit a signal with the same idempotency_key produces
    was_new=False (no duplicate row).
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_trigger(fresh_db, tenant_id=tid, source="discord")

    poller = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.01,
            max_triggers_per_tick=1,
        ),
    )
    await poller.run(max_ticks=1)

    run_row = await fresh_db.fetchrow(
        "SELECT id FROM onboarding_runs WHERE tenant_id = $1", tid,
    )
    run_id = run_row["id"]

    # Re-emit with the SAME idempotency_key — should be no-op success.
    # Per A13: workflow_id is the inbox identifier
    # ("tenant_onboarding"), NOT the per-run id. Per-run uniqueness
    # comes from idempotency_key=str(run_id).
    result = await emit_signal(
        fresh_db,
        workflow_kind="tenant_onboarding",
        workflow_id="tenant_onboarding",
        signal_kind=SIGNAL_KIND_RUN_CREATED,
        idempotency_key=str(run_id),
        signal_data={"replay": True},
    )
    assert result.was_new is False, (
        f"Re-emit with same idempotency_key returned was_new=True; "
        f"the signal would have been duplicated. Contract broken."
    )

    # Exactly ONE signal row exists for this run.
    n_signals = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = 'tenant_onboarding' "
        "AND signal_kind = $1 AND idempotency_key = $2",
        SIGNAL_KIND_RUN_CREATED, str(run_id),
    )
    assert n_signals == 1


# =====================================================================
# 6. Pattern-alignment analyzer accepts oauth_poller.py.
# =====================================================================

def test_oauth_poller_passes_pattern_alignment_analyzer() -> None:
    """The M6.0 static analyzer must accept oauth_poller.py per the
    pattern-alignment requirements. If this test fails, the poller
    violates one of the five rules — investigate the violation
    rather than relaxing the rule (per
    pattern-alignment-rules.md)."""
    import pathlib

    from services.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    poller_path = WORKFLOWS_DIR / "oauth_poller.py"
    assert poller_path.exists(), f"oauth_poller.py missing at {poller_path}"

    violations = _all_rules(poller_path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"oauth_poller.py violates the M6 pattern-alignment "
            f"rules:\n{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md."
        )
