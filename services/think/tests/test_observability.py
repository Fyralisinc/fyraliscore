"""services/think/tests/test_observability.py — think_runs lifecycle +
region lock log + structured emit ordering.

Covers Wave 3-B Outstanding #7:
  * insert_think_run + update_think_run round-trip on a real DB.
  * think_runs row has status='running' at insert then transitions to
    'success' / 'failed' / 'skipped_idempotent' with ended_at populated.
  * think_region_lock_log best-effort write succeeds once per run.
  * write_region_lock_log swallows DB errors (warning logged), Think
    succeeds anyway.
  * METRICS counters move on inc_run / inc_failed / observe_latency.
"""
from __future__ import annotations

import json
import time

import pytest

from lib.shared.ids import uuid7

from services.think.observability import (
    Metrics, ThinkRunRecord, emit,
    insert_think_run, update_think_run, write_region_lock_log,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# think_runs — insert + update lifecycle
# =====================================================================


async def test_insert_think_run_creates_running_row(fresh_db, tenant, tenant_cleanup):
    record = ThinkRunRecord(
        id=uuid7(),
        tenant_id=tenant,
        trigger_id=uuid7(),
        trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(
                conn, record,
                region_tenant_hash=1234,
                region_entity_hash=5678,
            )
        row = await conn.fetchrow(
            "SELECT * FROM think_runs WHERE id = $1", record.id,
        )
    assert row is not None
    assert row["status"] == "running"
    assert row["ended_at"] is None
    assert row["trigger_kind"] == "T1"
    assert row["tenant_id"] == tenant
    assert row["region_tenant_hash"] == 1234
    assert row["region_entity_hash"] == 5678


async def test_update_think_run_success_sets_ended_at(
    fresh_db, tenant, tenant_cleanup,
):
    record = ThinkRunRecord(
        id=uuid7(),
        tenant_id=tenant,
        trigger_id=uuid7(),
        trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(conn, record)
            await update_think_run(
                conn, record.id,
                status="success",
                retrieval_model_count=5,
                retrieval_observation_count=20,
                cascade_depth=3,
                ops_applied={"claim_ops": [], "act_ops": [], "resource_ops": []},
            )
        row = await conn.fetchrow(
            "SELECT * FROM think_runs WHERE id = $1", record.id,
        )
    assert row["status"] == "success"
    assert row["ended_at"] is not None
    assert row["retrieval_model_count"] == 5
    assert row["retrieval_observation_count"] == 20
    assert row["cascade_depth"] == 3


async def test_update_think_run_failed_sets_ended_at(
    fresh_db, tenant, tenant_cleanup,
):
    record = ThinkRunRecord(
        id=uuid7(), tenant_id=tenant,
        trigger_id=uuid7(), trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(conn, record)
            await update_think_run(
                conn, record.id,
                status="failed",
                error="boom",
            )
        row = await conn.fetchrow(
            "SELECT * FROM think_runs WHERE id = $1", record.id,
        )
    assert row["status"] == "failed"
    assert row["ended_at"] is not None
    assert row["error"] == "boom"


async def test_update_think_run_skipped_idempotent_sets_ended_at(
    fresh_db, tenant, tenant_cleanup,
):
    record = ThinkRunRecord(
        id=uuid7(), tenant_id=tenant,
        trigger_id=uuid7(), trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(conn, record)
            await update_think_run(
                conn, record.id, status="skipped_idempotent",
                error="already applied",
            )
        row = await conn.fetchrow(
            "SELECT status, ended_at, error FROM think_runs WHERE id = $1",
            record.id,
        )
    assert row["status"] == "skipped_idempotent"
    assert row["ended_at"] is not None


async def test_update_think_run_no_fields_noop(fresh_db, tenant, tenant_cleanup):
    record = ThinkRunRecord(
        id=uuid7(), tenant_id=tenant,
        trigger_id=uuid7(), trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(conn, record)
            # No-op: no fields passed.
            await update_think_run(conn, record.id)
        row = await conn.fetchrow(
            "SELECT status FROM think_runs WHERE id = $1", record.id,
        )
    assert row["status"] == "running"


async def test_update_think_run_progressive_fields(
    fresh_db, tenant, tenant_cleanup,
):
    """Multiple update_think_run calls patch different columns."""
    record = ThinkRunRecord(
        id=uuid7(), tenant_id=tenant,
        trigger_id=uuid7(), trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await insert_think_run(conn, record)
            await update_think_run(
                conn, record.id,
                retrieval_model_count=3,
            )
            await update_think_run(
                conn, record.id,
                retrieval_observation_count=10,
            )
            await update_think_run(
                conn, record.id,
                llm_latency_ms=200,
            )
            await update_think_run(
                conn, record.id,
                validation_error_count=0,
            )
            await update_think_run(
                conn, record.id,
                status="success",
                ops_applied={"x": 1},
                cascade_depth=2,
            )
        row = await conn.fetchrow(
            "SELECT * FROM think_runs WHERE id = $1", record.id,
        )
    assert row["retrieval_model_count"] == 3
    assert row["retrieval_observation_count"] == 10
    assert row["llm_latency_ms"] == 200
    assert row["validation_error_count"] == 0
    assert row["cascade_depth"] == 2
    assert row["status"] == "success"


async def test_think_runs_rolls_back_on_transaction_rollback(
    fresh_db, tenant, tenant_cleanup,
):
    record = ThinkRunRecord(
        id=uuid7(), tenant_id=tenant,
        trigger_id=uuid7(), trigger_kind="T1",
    )
    async with fresh_db.acquire() as conn:
        try:
            async with conn.transaction():
                await insert_think_run(conn, record)
                raise RuntimeError("abort")
        except RuntimeError:
            pass
        # Row must not exist — rolled back with the tx.
        row = await conn.fetchrow(
            "SELECT 1 FROM think_runs WHERE id = $1", record.id,
        )
    assert row is None


# =====================================================================
# region lock log — post-commit, fire-and-forget
# =====================================================================


async def test_write_region_lock_log_happy_path(fresh_db, tenant, tenant_cleanup):
    run_id = uuid7()
    await write_region_lock_log(
        fresh_db,
        tenant_id=tenant,
        think_run_id=run_id,
        tenant_hash=111,
        entity_hash=222,
        entity_ids=[("commitment", str(uuid7()))],
        acquired_at=time.monotonic(),
        released_at=time.monotonic() + 0.05,
        wait_duration_ms=5,
        hold_duration_ms=50,
    )
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM think_region_lock_log WHERE think_run_id = $1",
            run_id,
        )
    assert row is not None
    assert row["tenant_hash"] == 111
    assert row["entity_hash"] == 222
    assert row["acquired_at"] is not None
    assert row["released_at"] is not None
    assert row["wait_duration_ms"] == 5
    assert row["hold_duration_ms"] == 50


async def test_write_region_lock_log_pool_none_is_noop():
    """Pool=None → function returns cleanly without raising."""
    await write_region_lock_log(
        None,  # type: ignore[arg-type]
        tenant_id=uuid7(),
        think_run_id=uuid7(),
        tenant_hash=0, entity_hash=0,
        entity_ids=[], acquired_at=0.0, released_at=0.0,
        wait_duration_ms=0, hold_duration_ms=0,
    )
    # no exception = pass


async def test_write_region_lock_log_swallows_insert_errors(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
):
    """If the insert raises (e.g. schema drift dropped the table), the
    helper logs a warning and returns — it does NOT propagate."""
    class FakePool:
        def acquire(self):
            raise RuntimeError("simulated DB outage")
    # This should not raise.
    await write_region_lock_log(
        FakePool(),  # type: ignore[arg-type]
        tenant_id=tenant,
        think_run_id=uuid7(),
        tenant_hash=0, entity_hash=0,
        entity_ids=[], acquired_at=0.0, released_at=0.0,
        wait_duration_ms=0, hold_duration_ms=0,
    )


async def test_region_lock_log_writes_entity_ids_jsonb(
    fresh_db, tenant, tenant_cleanup,
):
    run_id = uuid7()
    eids = [("commitment", str(uuid7())), ("goal", str(uuid7()))]
    await write_region_lock_log(
        fresh_db,
        tenant_id=tenant,
        think_run_id=run_id,
        tenant_hash=1, entity_hash=2,
        entity_ids=eids,
        acquired_at=time.monotonic(),
        released_at=time.monotonic() + 0.01,
        wait_duration_ms=0, hold_duration_ms=10,
    )
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT entity_ids FROM think_region_lock_log WHERE think_run_id = $1",
            run_id,
        )
    raw = row["entity_ids"]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        parsed = json.loads(raw)
    else:
        parsed = raw
    # JSON serialisation turns tuples into 2-element lists.
    assert len(parsed) == 2
    assert parsed[0][0] in ("commitment", "goal")


# =====================================================================
# METRICS — counter + histogram tracking
# =====================================================================


async def test_metrics_inc_and_snapshot_roundtrip():
    m = Metrics()
    m.inc_run("T1")
    m.inc_run("T1")
    m.inc_run("T2")
    m.inc_failed("T1")
    m.observe_latency("T1", 100.0)
    m.observe_latency("T1", 200.0)
    m.inc_op("claim_insert")
    m.inc_op("act_transition_commitment", 2)
    m.observe_cascade_depth("T1", 5)
    m.observe_region_lock_wait(25.0)
    m.set_queue_depth("tenant_a", 10)

    snap = m.snapshot()
    assert snap["runs_total"]["T1"] == 2
    assert snap["runs_total"]["T2"] == 1
    assert snap["runs_failed"]["T1"] == 1
    assert snap["run_latency_ms"]["T1"] == [100.0, 200.0]
    assert snap["ops_by_kind"]["act_transition_commitment"] == 2
    assert snap["cascade_depth_reached"]["T1"] == [5]
    assert snap["region_lock_waits_ms"] == [25.0]
    assert snap["queue_depth"]["tenant_a"] == 10


async def test_metrics_reset_clears_state():
    m = Metrics()
    m.inc_run("T1")
    m.observe_latency("T1", 100.0)
    m.reset()
    snap = m.snapshot()
    assert snap["runs_total"] == {}
    assert snap["runs_failed"] == {}
    assert snap["run_latency_ms"] == {}


async def test_emit_helper_does_not_raise():
    # emit() is a structlog wrapper — just verify it doesn't blow up.
    emit("think.test_event", tenant="x", foo=1, nested={"a": 1})
