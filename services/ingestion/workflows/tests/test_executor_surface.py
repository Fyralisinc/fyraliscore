"""M6.0 substrate amendment (A12) — executor-surface tests.

Covers the connection-aware amendment to the M6.0 substrate. Per
[05-lld-amendments.md A12](../../../../docs/ingestion/05-lld-amendments.md):
the substrate's non-N1 functions accept `asyncpg.Pool |
asyncpg.Connection`. This file holds:

  - Backwards-compat smoke for the Pool path (existing callers).
  - LOAD-BEARING transactional-participation tests for the
    Connection path (the property M6.1 needs).
  - The new `claim_signals(conn, ...)` primitive — return shape,
    transaction-discipline contract, concurrent SKIP LOCKED.

The existing M6.0 Phase 2 tests (`test_signals.py`, `test_state.py`,
`test_feels_onboarded_monitor.py`) cover the high-level behaviour of
the amended functions; this file isolates the executor-surface
property tests that the amendment specifically enables.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingestion.workflows.signals import (
    EmitResult,
    WorkflowSignal,
    claim_signals,
    emit_signal,
    poll_signals,
    signal_count,
)
from services.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


pytestmark = [pytest.mark.timeout(60)]


_NOW = dt.datetime(2026, 5, 18, 12, 0, 0, tzinfo=dt.timezone.utc)


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"executor-surface-{tid.hex[:8]}",
    )
    return tid


async def _drain(it) -> list[WorkflowSignal]:
    return [s async for s in it]


# =====================================================================
# 1. Backwards-compat: Pool path unchanged.
# =====================================================================

async def test_emit_signal_with_pool_unchanged(
    fresh_db: asyncpg.Pool,
) -> None:
    """Backwards-compat smoke: the Pool-path of `emit_signal` behaves
    byte-identically to the pre-A12 surface. The FeelsOnboardedMonitor
    + every M5.x service that holds a Pool continues to work."""
    await _seed_tenant(fresh_db)
    result = await emit_signal(
        fresh_db,
        workflow_kind="tenant_onboarding",
        workflow_id="run-pool",
        signal_kind="ping",
        idempotency_key="pool-key-1",
    )
    assert isinstance(result, EmitResult)
    assert result.was_new is True

    # Second call with same key → was_new=False, same signal_id.
    again = await emit_signal(
        fresh_db,
        workflow_kind="tenant_onboarding",
        workflow_id="run-pool",
        signal_kind="ping",
        idempotency_key="pool-key-1",
    )
    assert again.was_new is False
    assert again.signal_id == result.signal_id


# =====================================================================
# 2. LOAD-BEARING — emit_signal with Connection rolls back with caller.
# =====================================================================

async def test_emit_signal_with_connection_participates_in_caller_txn(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING (M6.1): `emit_signal(conn, ...)` inside
    `conn.transaction()` participates in the caller's transaction.
    If the caller rolls back, NO `workflow_signals` row exists.

    This is the property M6.1's OAuth poller relies on: if the
    onboarding_runs INSERT fails (or any other step in the poller's
    atomic block raises), the signal-emit is rolled back too, so the
    trigger row's consumed_at is also undone, and the next tick
    re-claims and retries cleanly.

    Without this property, the poller would have a window where the
    signal exists but the onboarding_run does not — a downstream
    orchestrator consuming the signal would find no run to act on.
    """
    await _seed_tenant(fresh_db)

    class _SyntheticFailure(Exception):
        pass

    with pytest.raises(_SyntheticFailure):
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                result = await emit_signal(
                    conn,
                    workflow_kind="oauth_poller",
                    workflow_id="default",
                    signal_kind="onboarding_run_created",
                    idempotency_key="rollback-test-1",
                    signal_data={"onboarding_run_id": str(uuid4())},
                )
                # Verify within-txn visibility: the emit returned
                # was_new=True (the row exists from THIS txn's POV).
                assert result.was_new is True

                # Force rollback by raising before the with-block exits.
                raise _SyntheticFailure(
                    "synthetic failure mid-transaction to force rollback"
                )

    # ----- LOAD-BEARING assertion: post-rollback, NO row exists. -----
    row_count = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND idempotency_key = $3",
        "oauth_poller", "default", "rollback-test-1",
    )
    assert row_count == 0, (
        f"A12 INVARIANT VIOLATED: workflow_signals row count is "
        f"{row_count} after a rollback-on-emit; expected 0. The "
        f"Connection-typed emit_signal did NOT participate in the "
        f"caller's transaction — M6.1's atomic-poller contract is "
        f"broken. Investigate emit_signal's executor handling."
    )


# =====================================================================
# 3. LOAD-BEARING — persist_state with Connection rolls back.
# =====================================================================

async def test_persist_state_with_connection_participates_in_caller_txn(
    fresh_db: asyncpg.Pool,
) -> None:
    """Same shape as #2 but for `persist_state(conn, ...)`. M6.1's
    orchestrator persists workflow_states diagnostics alongside its
    signal emits in one transaction; rollback must undo both."""
    tid = await _seed_tenant(fresh_db)

    class _SyntheticFailure(Exception):
        pass

    state = WorkflowState(
        workflow_kind="executor_test", workflow_id="rollback",
        tenant_id=tid, state_data={"v": 1}, last_advanced_at=_NOW,
    )

    with pytest.raises(_SyntheticFailure):
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                await persist_state(conn, state)
                raise _SyntheticFailure("force rollback")

    row_count = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        "executor_test", "rollback",
    )
    assert row_count == 0, (
        f"A12 INVARIANT VIOLATED: workflow_states row count is "
        f"{row_count} after rollback; expected 0. persist_state did "
        f"NOT participate in the caller's transaction."
    )


# =====================================================================
# 4. load_state with Connection inside an open txn.
# =====================================================================

async def test_load_state_with_connection_in_open_txn(
    fresh_db: asyncpg.Pool,
) -> None:
    """`load_state(conn, ...)` reads via the caller's connection.
    Inside an open transaction, it sees the caller's pending writes
    (since they're visible to the same connection)."""
    tid = await _seed_tenant(fresh_db)

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            state = WorkflowState(
                workflow_kind="executor_test",
                workflow_id="load_in_txn",
                tenant_id=tid,
                state_data={"counter": 42},
                last_advanced_at=_NOW,
            )
            await persist_state(conn, state)

            # Read-after-write within the same txn — the caller sees
            # their own pending insert.
            loaded = await load_state(
                conn, "executor_test", "load_in_txn",
            )
            assert loaded is not None
            assert loaded.state_data == {"counter": 42}
            assert loaded.tenant_id == tid

    # Outside the transaction, the committed row is also visible
    # via the Pool path — confirms backwards-compat for the
    # post-commit reader.
    after_commit = await load_state(
        fresh_db, "executor_test", "load_in_txn",
    )
    assert after_commit is not None
    assert after_commit.state_data == {"counter": 42}


# =====================================================================
# 5. claim_signals returns a list.
# =====================================================================

async def test_claim_signals_returns_list_not_iterator(
    fresh_db: asyncpg.Pool,
) -> None:
    """`claim_signals` returns `list[WorkflowSignal]` (not an async
    iterator). Different shape from `poll_signals` because the
    caller is already inside their own transaction; eager list is
    simpler than wrangling iterator lifetime against the
    transaction's commit/rollback."""
    await _seed_tenant(fresh_db)
    for i in range(3):
        await emit_signal(
            fresh_db,
            workflow_kind="list_test", workflow_id="instance",
            signal_kind="event", idempotency_key=f"k-{i}",
        )

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            result = await claim_signals(
                conn,
                workflow_kind="list_test", workflow_id="instance",
                consumed_by="caller", batch_size=10,
            )

    assert isinstance(result, list), (
        f"claim_signals must return a list, got {type(result)!r}"
    )
    assert len(result) == 3
    assert all(isinstance(s, WorkflowSignal) for s in result)


# =====================================================================
# 6. claim_signals outside a caller-managed transaction — observed
#    behaviour locked in.
# =====================================================================

async def test_claim_signals_without_transaction_autocommits(
    fresh_db: asyncpg.Pool,
) -> None:
    """`claim_signals(conn, ...)` called on a bare connection (no
    open transaction) auto-commits the claim. This is asyncpg's
    default behaviour — every statement on a connection is its own
    implicit transaction unless wrapped.

    This test documents the observed behaviour rather than enforcing
    one. Callers MUST wrap `claim_signals` in `async with
    conn.transaction()` to get the rollback semantics they likely
    want. The docstring on `claim_signals` calls this out; this
    test locks the behaviour in so a future asyncpg upgrade or
    substrate refactor doesn't silently change it.

    Why not raise? An explicit "must-be-in-transaction" check would
    require runtime introspection of conn's transaction stack, which
    asyncpg doesn't expose. The discipline is documented in the
    docstring; this test asserts the failure mode is "claim
    auto-commits" rather than "claim raises" or "claim silently no-
    ops."
    """
    await _seed_tenant(fresh_db)
    await emit_signal(
        fresh_db,
        workflow_kind="autocommit_test", workflow_id="instance",
        signal_kind="ping", idempotency_key="autocommit-1",
    )

    async with fresh_db.acquire() as conn:
        # NO `async with conn.transaction():` wrapper. Bare claim.
        result = await claim_signals(
            conn,
            workflow_kind="autocommit_test", workflow_id="instance",
            consumed_by="bare-caller", batch_size=10,
        )

    assert len(result) == 1
    # ----- The claim auto-committed: the row is consumed in the DB. -----
    unconsumed = await signal_count(
        fresh_db,
        workflow_kind="autocommit_test", workflow_id="instance",
    )
    assert unconsumed == 0, (
        f"Locked-in observed behaviour: bare claim_signals call "
        f"(no caller transaction) auto-committed the claim — DB "
        f"shows {unconsumed} unconsumed signals; expected 0. "
        f"If this changes (e.g., asyncpg requires explicit "
        f"transaction), update the docstring + this test together."
    )


# =====================================================================
# 7. LOAD-BEARING — claim_signals disjoint under concurrent caller txns.
# =====================================================================

async def test_claim_signals_disjoint_under_concurrency(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two concurrent connections in two concurrent transactions
    both call `claim_signals`. They MUST claim DISJOINT subsets via
    FOR UPDATE SKIP LOCKED. Same property the existing
    `test_poll_signals_claims_via_for_update_skip_locked` proves for
    `poll_signals`; this test proves it at the new `claim_signals`
    level.

    If SKIP LOCKED breaks (e.g., a future refactor changes the SQL),
    we'd see overlap OR one poller blocks indefinitely on the other.
    Both failure modes break M6.1's parallel-orchestrator story.
    """
    await _seed_tenant(fresh_db)
    n = 20
    for i in range(n):
        await emit_signal(
            fresh_db,
            workflow_kind="claim_concurrency",
            workflow_id="multi",
            signal_kind="ping",
            idempotency_key=f"ping-{i}",
        )

    async def _claim_one(label: str) -> list[WorkflowSignal]:
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                return await claim_signals(
                    conn,
                    workflow_kind="claim_concurrency",
                    workflow_id="multi",
                    consumed_by=label, batch_size=n,
                )

    batch_a, batch_b = await asyncio.gather(
        _claim_one("caller-A"), _claim_one("caller-B"),
    )
    ids_a = {s.id for s in batch_a}
    ids_b = {s.id for s in batch_b}

    # DISJOINT.
    overlap = ids_a & ids_b
    assert not overlap, (
        f"claim_signals FOR UPDATE SKIP LOCKED contract violated: "
        f"callers A and B both claimed {len(overlap)} signals: "
        f"{overlap}. The new claim_signals primitive does not "
        f"properly use SKIP LOCKED, or the caller-managed "
        f"transaction wrapping doesn't release the lock at commit."
    )
    # TOGETHER cover all 20.
    assert len(ids_a | ids_b) == n


# =====================================================================
# 8. poll_signals still works (substrate-managed atomicity path).
# =====================================================================

async def test_poll_signals_still_works_after_refactor(
    fresh_db: asyncpg.Pool,
) -> None:
    """`poll_signals(pool, ...)` was refactored to delegate to
    `claim_signals(conn, ...)` internally. External contract is
    unchanged: returns an async iterator; signals are committed-
    consumed before being yielded. M3.3 + M5.1 + FeelsOnboardedMonitor
    style callers see no behaviour change.

    The existing
    `test_poll_signals_claims_via_for_update_skip_locked` in
    `test_signals.py` is the primary load-bearing proof; this test
    is the executor-surface-side smoke that the refactor preserved
    the property.
    """
    await _seed_tenant(fresh_db)
    for i in range(5):
        await emit_signal(
            fresh_db,
            workflow_kind="refactor_smoke", workflow_id="instance",
            signal_kind="ping", idempotency_key=f"r-{i}",
            signal_data={"i": i},
        )

    batch = await _drain(poll_signals(
        fresh_db,
        workflow_kind="refactor_smoke", workflow_id="instance",
        consumed_by="poll-caller", batch_size=10,
    ))
    assert len(batch) == 5
    assert all(isinstance(s, WorkflowSignal) for s in batch)
    # Signals are committed-consumed before yielding.
    unconsumed = await signal_count(
        fresh_db,
        workflow_kind="refactor_smoke", workflow_id="instance",
    )
    assert unconsumed == 0
