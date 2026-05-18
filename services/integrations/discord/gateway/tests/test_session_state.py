"""M4.2 — session_state load/save tests.

Real Postgres via fresh_db fixture. The save site has no Redis or
Kafka dependency — this is pure Path A (DB only)."""
from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import asyncpg
import pytest

from services.integrations.discord.gateway.session_state import (
    PersistedGatewaySession,
    STALENESS_THRESHOLD,
    load_session_state,
    make_session_state_pool,
    save_session_state,
)


pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _app_id() -> str:
    """Per-test application_id so concurrent runs don't collide on the
    UNIQUE (application_id, shard_id) constraint."""
    return f"app-{uuid4().hex[:12]}"


# =====================================================================
# 1. Round trip.
# =====================================================================

async def test_save_and_load_round_trip(fresh_db: asyncpg.Pool):
    """Save a fully-populated state; load returns equal values."""
    app_id = _app_id()
    await save_session_state(
        fresh_db,
        application_id=app_id,
        session_id="session-xyz",
        resume_gateway_url="wss://resume.gateway.discord.gg/",
        last_seq=4242,
        heartbeat_interval_ms=41250,
        last_heartbeat_ack_at=_NOW,
        last_dispatched_at=_NOW,
        leader_lease_holder="pod-a",
        leader_lease_expires_at=_NOW + dt.timedelta(seconds=30),
        now=_NOW,
    )

    loaded = await load_session_state(
        fresh_db, application_id=app_id, now=_NOW,
    )
    assert loaded is not None
    assert isinstance(loaded, PersistedGatewaySession)
    assert loaded.application_id == app_id
    assert loaded.session_id == "session-xyz"
    assert loaded.resume_gateway_url == "wss://resume.gateway.discord.gg/"
    assert loaded.last_seq == 4242
    assert loaded.heartbeat_interval_ms == 41250
    assert loaded.last_heartbeat_ack_at == _NOW
    assert loaded.last_dispatched_at == _NOW
    assert loaded.leader_lease_holder == "pod-a"
    assert loaded.shard_id == 0
    assert loaded.updated_at == _NOW


# =====================================================================
# 2. Absent → None.
# =====================================================================

async def test_load_returns_none_when_absent(fresh_db: asyncpg.Pool):
    """No row for this application_id → load returns None."""
    loaded = await load_session_state(
        fresh_db, application_id=_app_id(),
    )
    assert loaded is None


# =====================================================================
# 3. Stale → None. LOAD-BEARING.
# =====================================================================

async def test_load_returns_none_when_stale(fresh_db: asyncpg.Pool):
    """LOAD-BEARING (M4.2): a row whose `updated_at` is older than
    STALENESS_THRESHOLD (4 minutes) must return None from
    `load_session_state`. Discord's session retention is approximately
    4-5 minutes server-side; RESUMing a stale session_id wastes a
    roundtrip (Invalid Session → fresh IDENTIFY anyway).

    This test pins the 4-minute cutoff. If someone "loosens" the
    threshold to 5 or 10 minutes thinking it's harmless, this test
    fails — and the fix should be a deliberate LLD amendment, not a
    quiet change.
    """
    app_id = _app_id()
    # Save a state with updated_at = now - 5 minutes (past the 4-min
    # cutoff).
    stale_time = _NOW - dt.timedelta(minutes=5)
    await save_session_state(
        fresh_db,
        application_id=app_id,
        session_id="abandoned-session",
        resume_gateway_url="wss://resume.gateway.discord.gg/",
        last_seq=99,
        now=stale_time,
    )

    # Load with now=_NOW: should be None because the row is 5 min old.
    loaded = await load_session_state(
        fresh_db, application_id=app_id, now=_NOW,
    )
    assert loaded is None, (
        "Stale state (5 min old) must return None — Discord's session "
        "retention is ~4-5 min and we use 4 min as the conservative "
        "cutoff. If you're changing this threshold, surface it as an "
        "LLD amendment first."
    )

    # Boundary check: 3 min 30 sec old returns the row (under 4 min).
    fresh_time = _NOW - dt.timedelta(minutes=3, seconds=30)
    app_id2 = _app_id()
    await save_session_state(
        fresh_db,
        application_id=app_id2,
        session_id="still-live-session",
        resume_gateway_url="wss://resume.gateway.discord.gg/",
        last_seq=42,
        now=fresh_time,
    )
    loaded_fresh = await load_session_state(
        fresh_db, application_id=app_id2, now=_NOW,
    )
    assert loaded_fresh is not None, (
        "3min30s is INSIDE the 4-min staleness window; load should "
        "return the row."
    )
    assert loaded_fresh.session_id == "still-live-session"


# =====================================================================
# 4. UPSERT semantics — same key → second save overwrites.
# =====================================================================

async def test_save_upserts_existing_row(fresh_db: asyncpg.Pool):
    """Save with seq=100, save again with seq=200, load returns 200.
    The UNIQUE (application_id, shard_id) + ON CONFLICT DO UPDATE
    path is what the LLD §1.5 specifies."""
    app_id = _app_id()
    await save_session_state(
        fresh_db,
        application_id=app_id,
        session_id="s1",
        resume_gateway_url="wss://a.example/",
        last_seq=100,
        now=_NOW,
    )
    await save_session_state(
        fresh_db,
        application_id=app_id,
        session_id="s2",
        resume_gateway_url="wss://b.example/",
        last_seq=200,
        now=_NOW + dt.timedelta(seconds=1),
    )

    loaded = await load_session_state(
        fresh_db, application_id=app_id, now=_NOW + dt.timedelta(seconds=2),
    )
    assert loaded is not None
    assert loaded.last_seq == 200
    assert loaded.session_id == "s2"
    assert loaded.resume_gateway_url == "wss://b.example/"

    # Exactly one row in the DB for this (app_id, shard_id).
    async with fresh_db.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM gateway_session_state "
            "WHERE application_id = $1 AND shard_id = $2",
            app_id, 0,
        )
    assert count == 1, (
        f"UPSERT path produced {count} rows; expected 1. "
        f"Migration 0048's UNIQUE (application_id, shard_id) is "
        f"missing or DO UPDATE not firing."
    )


# =====================================================================
# 5. Pool config — pgbouncer-compatible. M1.3 ADR Q1 reactivation.
# =====================================================================

async def test_save_uses_pgbouncer_compatible_pool():
    """`make_session_state_pool` MUST pass `statement_cache_size=0`
    to asyncpg.create_pool — second activation of the M1.3 ADR Q1
    flag in production code (first was M3.1's DLQ writer at
    services/ingestion/writers/dlq_writer/dlq_writer.py:345-351).

    Verifies via mock — the live pool object's asyncpg internals
    don't expose the config in a way that's stable across versions.
    """
    captured_kwargs: dict[str, Any] = {}

    async def _fake_create_pool(*args: Any, **kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        captured_kwargs["__args"] = args
        return AsyncMock()  # not used; we only check the call kwargs

    with patch("asyncpg.create_pool", _fake_create_pool):
        await make_session_state_pool("postgresql://example/db")

    assert captured_kwargs["statement_cache_size"] == 0, (
        f"make_session_state_pool did not pass statement_cache_size=0; "
        f"kwargs={captured_kwargs}. Pool will NOT be pgbouncer-"
        f"compatible. See M1.3 ADR Q1 + M3.1 dlq_writer reference."
    )
    assert captured_kwargs["min_size"] == 1
    assert captured_kwargs["max_size"] == 5
    # DSN positional.
    assert captured_kwargs["__args"] == ("postgresql://example/db",)


# =====================================================================
# 6. Threshold constant pinned (mirrors the M4.1 constants test).
# =====================================================================

def test_staleness_threshold_pinned():
    """STALENESS_THRESHOLD is 4 minutes — the load-bearing operational
    cutoff. If anyone loosens this without an LLD amendment, fail."""
    assert STALENESS_THRESHOLD == dt.timedelta(minutes=4)
