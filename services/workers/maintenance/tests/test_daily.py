"""Tests for daily maintenance jobs — Wave 4-D.

Covers (from the BUILD-PLAN §5 Prompt 4.D test list):
* #8 hourly_decay_job: activations decrease predictably.
* #9 archive_decayed_job: low-act + stale → archive; fresh stays.
* #10 entity_alias_cleanup: unused + stale → deleted; recent stays.
* #11 orphan_detection: orphans flagged; non-orphans left alone.
* #12 orphan_detection does NOT delete Observations.
* #13 region_lock_log_cleanup: old rows deleted, recent kept.
* #19 (partial): daily job is resumable / idempotent (no half-state).
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.workers.maintenance.daily import (
    archive_decayed_job,
    entity_alias_cleanup,
    hourly_decay_job,
    orphan_detection,
    region_lock_log_cleanup,
    run_daily,
    think_runs_cleanup,
)

from .conftest import seed_observation


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _seed_actor(
    conn: asyncpg.Connection, tenant_id: UUID
) -> UUID:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'A', 'active')
        """,
        actor_id,
        tenant_id,
    )
    return actor_id


async def _seed_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    born_event: UUID,
    activation: float = 1.0,
    last_retrieved_at: datetime | None = None,
) -> UUID:
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_temporal, confidence, activation,
            confidence_at_assertion,
            last_retrieved_at
        ) VALUES (
            $1, $2, $3, '{"kind": "test"}'::jsonb, 'natural',
            array_fill(0.0::real, ARRAY[768])::vector,
            '{}'::jsonb, 0.5, $4, 0.5, $5
        )
        """,
        mid,
        tenant_id,
        born_event,
        float(activation),
        last_retrieved_at,
    )
    return mid


# ---------------------------------------------------------------------
# #8 hourly_decay multiplies activation by exp(-1/120)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hourly_decay_reduces_activation_predictably(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    async with m_pool.acquire() as c:
        born = await seed_observation(c, tenant_id=tenant_id)
        mid = await _seed_model(
            c, tenant_id=tenant_id, born_event=born, activation=1.0
        )
        updated = await hourly_decay_job(conn=c)
        assert updated >= 1
        val = await c.fetchval(
            "SELECT activation FROM models WHERE id = $1", mid
        )
        # e^(-1/120) ≈ 0.99170
        assert math.isclose(val, math.exp(-1 / 120), abs_tol=1e-6)


# ---------------------------------------------------------------------
# #9 archive_decayed only archives activation<0.05 AND stale
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_decayed_respects_both_criteria(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    now = datetime.now(timezone.utc)
    async with m_pool.acquire() as c:
        born = await seed_observation(c, tenant_id=tenant_id)
        # Model A: low activation, stale retrieved → should archive.
        a = await _seed_model(
            c,
            tenant_id=tenant_id,
            born_event=born,
            activation=0.01,
            last_retrieved_at=now - timedelta(days=45),
        )
        # Model B: low activation but recent retrieval → should NOT archive.
        b = await _seed_model(
            c,
            tenant_id=tenant_id,
            born_event=born,
            activation=0.01,
            last_retrieved_at=now - timedelta(days=5),
        )
        # Model C: high activation + stale → should NOT archive.
        cc = await _seed_model(
            c,
            tenant_id=tenant_id,
            born_event=born,
            activation=0.9,
            last_retrieved_at=now - timedelta(days=45),
        )
        rows = await archive_decayed_job(conn=c)
        assert rows >= 1
        statuses = await c.fetch(
            "SELECT id, status FROM models WHERE id = ANY($1::uuid[])",
            [a, b, cc],
        )
        by_id = {r["id"]: r["status"] for r in statuses}
        assert by_id[a] == "archived"
        assert by_id[b] == "active"
        assert by_id[cc] == "active"


# ---------------------------------------------------------------------
# #10 entity_alias_cleanup — unused + stale → deleted; recent → stays
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entity_alias_cleanup_stale_vs_recent(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    async with m_pool.acquire() as c:
        actor_id = await _seed_actor(c, tenant_id)
        stale_id = uuid7()
        recent_id = uuid7()
        used_id = uuid7()
        # Stale: both counts zero, last_used_at 100 days ago.
        await c.execute(
            """
            INSERT INTO entity_aliases (
                id, tenant_id, alias_text, actor_id,
                resolved_entity_ref, confidence,
                confirmed_count, contested_count,
                first_seen_at, last_used_at
            ) VALUES (
                $1, $2, 'stale', $3, '{}'::jsonb, 0.5,
                0, 0,
                now() - interval '100 days',
                now() - interval '100 days'
            )
            """,
            stale_id,
            tenant_id,
            actor_id,
        )
        await c.execute(
            """
            INSERT INTO entity_aliases (
                id, tenant_id, alias_text, actor_id,
                resolved_entity_ref, confidence,
                confirmed_count, contested_count,
                first_seen_at, last_used_at
            ) VALUES (
                $1, $2, 'recent', $3, '{}'::jsonb, 0.5,
                0, 0,
                now() - interval '10 days',
                now() - interval '10 days'
            )
            """,
            recent_id,
            tenant_id,
            actor_id,
        )
        # Used (stale but confirmed_count>0) — must survive.
        await c.execute(
            """
            INSERT INTO entity_aliases (
                id, tenant_id, alias_text, actor_id,
                resolved_entity_ref, confidence,
                confirmed_count, contested_count,
                first_seen_at, last_used_at
            ) VALUES (
                $1, $2, 'used', $3, '{}'::jsonb, 0.9,
                5, 0,
                now() - interval '100 days',
                now() - interval '100 days'
            )
            """,
            used_id,
            tenant_id,
            actor_id,
        )
        deleted = await entity_alias_cleanup(conn=c)
        assert deleted == 1
        surviving = await c.fetch(
            "SELECT id FROM entity_aliases WHERE tenant_id = $1",
            tenant_id,
        )
        ids = {r["id"] for r in surviving}
        assert stale_id not in ids
        assert recent_id in ids
        assert used_id in ids


# ---------------------------------------------------------------------
# #11 orphan_detection flags the orphan; non-orphans left alone
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orphan_detection_flags_orphans(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    old = datetime.now(timezone.utc) - timedelta(days=30)
    async with m_pool.acquire() as c:
        # Orphan: no downstream reference.
        orphan_id = await seed_observation(
            c, tenant_id=tenant_id, occurred_at=old
        )
        # Non-orphan: referenced by a model.
        non_orphan_id = await seed_observation(
            c, tenant_id=tenant_id, occurred_at=old
        )
        await _seed_model(
            c, tenant_id=tenant_id, born_event=non_orphan_id
        )
        inserted = await orphan_detection(conn=c, grace_days=14)
        assert inserted == 1
        rows = await c.fetch(
            "SELECT observation_id, reason FROM orphan_log WHERE tenant_id = $1",
            tenant_id,
        )
        assert len(rows) == 1
        assert rows[0]["observation_id"] == orphan_id
        assert rows[0]["reason"] == "both"


# ---------------------------------------------------------------------
# #12 orphan_detection does NOT delete observations
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orphan_detection_never_deletes_observations(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    old = datetime.now(timezone.utc) - timedelta(days=30)
    async with m_pool.acquire() as c:
        orphan_id = await seed_observation(
            c, tenant_id=tenant_id, occurred_at=old
        )
        await orphan_detection(conn=c)
        still_there = await c.fetchval(
            "SELECT COUNT(*) FROM observations WHERE id = $1", orphan_id
        )
        assert still_there == 1


# ---------------------------------------------------------------------
# #13 region_lock_log_cleanup — old gone, recent kept
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_region_lock_log_cleanup_age_threshold(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    async with m_pool.acquire() as c:
        old = uuid7()
        new = uuid7()
        await c.execute(
            """
            INSERT INTO think_region_lock_log (
                id, tenant_id, think_run_id, tenant_hash, entity_hash,
                entity_ids, acquired_at, released_at
            ) VALUES (
                $1, $2, $3, 1, 1, '[]'::jsonb,
                now() - interval '60 days',
                now() - interval '60 days'
            )
            """,
            old,
            tenant_id,
            uuid7(),
        )
        await c.execute(
            """
            INSERT INTO think_region_lock_log (
                id, tenant_id, think_run_id, tenant_hash, entity_hash,
                entity_ids, acquired_at
            ) VALUES (
                $1, $2, $3, 2, 2, '[]'::jsonb,
                now() - interval '1 day'
            )
            """,
            new,
            tenant_id,
            uuid7(),
        )
        deleted = await region_lock_log_cleanup(conn=c)
        assert deleted == 1
        remaining = await c.fetch(
            "SELECT id FROM think_region_lock_log WHERE tenant_id = $1",
            tenant_id,
        )
        ids = {r["id"] for r in remaining}
        assert old not in ids and new in ids


# ---------------------------------------------------------------------
# #19 Crash-safe / resumable: re-running run_daily is safe + idempotent
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_daily_idempotent_and_resumable(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    old = datetime.now(timezone.utc) - timedelta(days=30)
    async with m_pool.acquire() as c:
        await seed_observation(
            c, tenant_id=tenant_id, occurred_at=old
        )
    r1 = await run_daily(pool=m_pool)
    r2 = await run_daily(pool=m_pool)
    assert not r1.errors
    assert not r2.errors
    # Second run does NOT re-flag the same orphan (dedup: NOT EXISTS
    # within 1 day).
    assert r2.orphans_flagged == 0


# ---------------------------------------------------------------------
# think_runs_cleanup counts but never deletes
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_think_runs_cleanup_counts_but_doesnt_delete(
    m_pool: asyncpg.Pool, tenant_id: UUID
) -> None:
    async with m_pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO think_runs (
                id, tenant_id, trigger_id, trigger_kind,
                started_at, status
            ) VALUES (
                $1, $2, $3, 'T1',
                now() - interval '120 days',
                'success'
            )
            """,
            uuid7(),
            tenant_id,
            uuid7(),
        )
        n = await think_runs_cleanup(conn=c)
        assert n == 1
        # Row still present.
        assert (
            await c.fetchval(
                "SELECT COUNT(*) FROM think_runs WHERE tenant_id = $1",
                tenant_id,
            )
            == 1
        )
