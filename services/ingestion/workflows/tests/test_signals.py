"""M6.0 Phase 2 — workflow signals tests.

Covers signals.py:
  - emit_signal idempotent contract:
      (a) same key across two emits.
      (b) exactly one row after both emits.
      (c) second emit SUCCEEDS WITHOUT EXCEPTION (no-op success).
  - poll_signals claim-and-mark via FOR UPDATE SKIP LOCKED:
      two concurrent pollers claim DISJOINT subsets — zero overlap.
  - signal_count returns unconsumed signal count.
  - Empty idempotency_key raises (the WorkflowSignal model also
    enforces this; we test the function-level guard separately).

A11 maps signal_workflow 1:1 onto these primitives; if (c) breaks,
callers retry endlessly assuming the second emit failed, then build
their own ad-hoc dedup tables — exactly the kind of "no introspectable
history" failure mode A11 warns about.
"""
from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingestion.workflows.signals import (
    EmitResult,
    WorkflowSignal,
    emit_signal,
    poll_signals,
    signal_count,
)


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"signals-test-{tid.hex[:8]}",
    )
    return tid


async def _drain(it) -> list[WorkflowSignal]:
    return [s async for s in it]


# =====================================================================
# 1. LOAD-BEARING idempotency contract.
# =====================================================================

async def test_emit_signal_idempotent_via_idempotency_key(
    fresh_db: asyncpg.Pool,
) -> None:
    """The three-part idempotency contract:
       (a) same (workflow_kind, workflow_id, signal_kind,
           idempotency_key) across two emits.
       (b) Exactly ONE row in `workflow_signals` after both emits.
       (c) Second emit SUCCEEDS WITHOUT EXCEPTION; `was_new=False`
           identifies the no-op.

    Without (c), callers retrying on "exception" would loop forever
    on a key that already landed — exactly the kind of operator-
    tooling friction A11 trigger #2 warns about.
    """
    await _seed_tenant(fresh_db)
    key = "tenant-feels-onboarded:abc:slack"

    first = await emit_signal(
        fresh_db,
        workflow_kind="tenant_onboarding", workflow_id="run-1",
        signal_kind="source_complete", idempotency_key=key,
        signal_data={"source": "slack", "obs_count": 42},
    )
    assert isinstance(first, EmitResult)
    assert first.was_new is True

    # ----- (c): the second emit succeeds WITHOUT raising -----
    second = await emit_signal(
        fresh_db,
        workflow_kind="tenant_onboarding", workflow_id="run-1",
        signal_kind="source_complete", idempotency_key=key,
        signal_data={"source": "slack", "obs_count": 42},
    )
    assert second.was_new is False, (
        "Second emit with same idempotency_key was marked was_new=True. "
        "The (c) clause of the contract is broken: callers will think "
        "this was a fresh insert and double-process."
    )
    assert second.signal_id == first.signal_id, (
        f"Second emit returned signal_id={second.signal_id} but first "
        f"returned {first.signal_id}. Callers need a STABLE id across "
        f"retries; the schema's existing row is the canonical one."
    )

    # ----- (b): exactly one row exists post-both-emits -----
    row_count = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        "tenant_onboarding", "run-1", "source_complete", key,
    )
    assert row_count == 1, (
        f"Expected exactly 1 row after two emits with same "
        f"idempotency_key; got {row_count}. The UNIQUE constraint "
        f"is the load-bearing schema invariant — investigate "
        f"migration 0054."
    )


async def test_emit_signal_empty_key_raises(
    fresh_db: asyncpg.Pool,
) -> None:
    """Empty idempotency_key must raise. The schema would store an
    empty string (NOT NULL doesn't reject ''), but the contract is
    that every signal has an idempotency identity. The function-level
    guard enforces this."""
    await _seed_tenant(fresh_db)
    with pytest.raises(ValueError, match="idempotency_key"):
        await emit_signal(
            fresh_db,
            workflow_kind="x", workflow_id="y",
            signal_kind="z", idempotency_key="",
        )


# =====================================================================
# 2. LOAD-BEARING — poll_signals claims via SKIP LOCKED.
# =====================================================================

async def test_poll_signals_claims_via_for_update_skip_locked(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two concurrent pollers MUST claim disjoint signal subsets.

    Plumbing:
      - Seed N=20 signals for the same (workflow_kind, workflow_id).
      - Launch two pollers concurrently, each calling poll_signals
        with batch_size=20.
      - Assert: union of claimed ids == all 20 ids; intersection == ∅.

    If SKIP LOCKED is wrong (e.g. plain FOR UPDATE), one poller
    blocks on the other and we see batch_a == 20, batch_b == 0.
    If there's no lock at all (e.g. plain SELECT), we see overlap.
    Both failure modes break the M1 outbox-poller / M6 cross-service
    handoff contract.
    """
    await _seed_tenant(fresh_db)
    n = 20
    for i in range(n):
        await emit_signal(
            fresh_db,
            workflow_kind="tenant_onboarding",
            workflow_id="multi-poller",
            signal_kind="ping",
            idempotency_key=f"ping-{i}",
            signal_data={"i": i},
        )
    assert await signal_count(
        fresh_db, workflow_kind="tenant_onboarding",
        workflow_id="multi-poller",
    ) == n

    async def _claim(poller_id: str) -> list[WorkflowSignal]:
        return await _drain(poll_signals(
            fresh_db,
            workflow_kind="tenant_onboarding",
            workflow_id="multi-poller",
            consumed_by=poller_id,
            batch_size=n,
        ))

    # Launch both pollers concurrently. asyncio.gather runs them as
    # close to simultaneously as the event loop permits; the FOR
    # UPDATE SKIP LOCKED contract is what makes them disjoint.
    batch_a, batch_b = await asyncio.gather(
        _claim("poller-A"), _claim("poller-B"),
    )

    ids_a = {s.id for s in batch_a}
    ids_b = {s.id for s in batch_b}

    # ----- DISJOINT: zero overlap -----
    overlap = ids_a & ids_b
    assert not overlap, (
        f"FOR UPDATE SKIP LOCKED contract violated: pollers A and B "
        f"both claimed {len(overlap)} signal(s): {overlap}. "
        f"The claim CTE is not actually using SKIP LOCKED, or is "
        f"committing in a way that lets another poller observe "
        f"unconsumed rows it should have skipped."
    )

    # ----- TOGETHER cover all 20 -----
    total = ids_a | ids_b
    assert len(total) == n, (
        f"Pollers A and B together claimed {len(total)} of {n} "
        f"signals — {n - len(total)} were left unclaimed despite "
        f"both pollers asking for batch_size={n}. SKIP LOCKED is "
        f"skipping locked-by-the-other-poller rows but the rows "
        f"are not committing fast enough; investigate the claim "
        f"transaction shape."
    )

    # ----- Every claimed row is marked consumed in DB -----
    unconsumed = await signal_count(
        fresh_db, workflow_kind="tenant_onboarding",
        workflow_id="multi-poller",
    )
    assert unconsumed == 0

    # ----- consumed_by stamped correctly -----
    consumed_by_a = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND consumed_by = $3",
        "tenant_onboarding", "multi-poller", "poller-A",
    )
    consumed_by_b = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND consumed_by = $3",
        "tenant_onboarding", "multi-poller", "poller-B",
    )
    assert consumed_by_a + consumed_by_b == n
    assert consumed_by_a == len(ids_a)
    assert consumed_by_b == len(ids_b)


# =====================================================================
# 3. Polling behaviour: empty batch, partial batch, ordering.
# =====================================================================

async def test_poll_signals_empty_when_no_signals(
    fresh_db: asyncpg.Pool,
) -> None:
    """No signals for this (workflow_kind, workflow_id) → empty iterator."""
    await _seed_tenant(fresh_db)
    batch = await _drain(poll_signals(
        fresh_db,
        workflow_kind="absent_kind", workflow_id="nothing",
        consumed_by="solo",
    ))
    assert batch == []


async def test_poll_signals_oldest_first(
    fresh_db: asyncpg.Pool,
) -> None:
    """Polling respects `ORDER BY created_at ASC` — oldest first."""
    await _seed_tenant(fresh_db)
    for i in range(5):
        await emit_signal(
            fresh_db,
            workflow_kind="ordered", workflow_id="instance",
            signal_kind="event", idempotency_key=f"e-{i}",
            signal_data={"i": i},
        )
        # A short sleep guarantees distinct created_at values
        # (the uuid7 id alone doesn't determine ordering — the
        # query orders by created_at).
        await asyncio.sleep(0.005)

    batch = await _drain(poll_signals(
        fresh_db,
        workflow_kind="ordered", workflow_id="instance",
        consumed_by="solo", batch_size=10,
    ))
    assert len(batch) == 5
    seen_i = [s.signal_data["i"] for s in batch]
    assert seen_i == [0, 1, 2, 3, 4], (
        f"Polling order is not oldest-first; got i sequence {seen_i}. "
        f"Operators rely on FIFO semantics for backlog reasoning."
    )


async def test_signal_count_decrements_after_consumption(
    fresh_db: asyncpg.Pool,
) -> None:
    """`signal_count` returns only UNCONSUMED signals — claim-and-mark
    must reflect immediately in the count."""
    await _seed_tenant(fresh_db)
    for i in range(3):
        await emit_signal(
            fresh_db,
            workflow_kind="counted", workflow_id="instance",
            signal_kind="event", idempotency_key=f"c-{i}",
        )
    assert await signal_count(
        fresh_db, workflow_kind="counted", workflow_id="instance",
    ) == 3
    await _drain(poll_signals(
        fresh_db,
        workflow_kind="counted", workflow_id="instance",
        consumed_by="solo", batch_size=2,
    ))
    assert await signal_count(
        fresh_db, workflow_kind="counted", workflow_id="instance",
    ) == 1
