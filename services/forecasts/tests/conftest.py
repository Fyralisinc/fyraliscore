"""services/forecasts/tests/conftest.py — shared fixtures for the
forecasts repo + router tests.

Reuses the gateway fixture stack so router tests get a fully-wired
FastAPI app. The `tenants` row is inserted explicitly because
migration 0037 enforces an immediate FK from `predictions.tenant_id`
to `tenants.id`.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


# Mirror the deadlock-retry hook from services/gateway/tests/conftest.py
# so concurrent integration suites (which TRUNCATE the shared schema)
# don't fail this suite with transient deadlock / serialization errors.
_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
    "MigrationError",
)


def pytest_runtest_protocol(item, nextitem):
    from _pytest.runner import runtestprotocol

    max_attempts = 4
    for attempt in range(max_attempts):
        reports = runtestprotocol(item, nextitem=nextitem, log=False)
        failed = any(r.failed for r in reports)
        transient = any(
            r.failed
            and r.longrepr is not None
            and any(err in repr(r.longrepr) for err in _TRANSIENT_ERRORS)
            for r in reports
        )
        if (not failed or not transient) or attempt == max_attempts - 1:
            for r in reports:
                item.ihook.pytest_runtest_logreport(report=r)
            return True
    return True


# Pull the gateway fixture stack into scope. `client`, `gateway_pool`,
# `valid_session` come from there. We deliberately re-define
# `seeded_actor` + `tenant_id` below so the tenants row exists before
# the actor row is inserted (migration 0037 enforces an immediate FK
# from actors.tenant_id to tenants.id).
from services.gateway.tests.conftest import (  # noqa: F401
    SLACK_TEST_SECRET,
    _DeterministicEmbedder,
    app_deps,
    client,
    gateway_pool,
    rate_limiter,
    seeded_actor_b,
    tenant_id_b,
    valid_session_b,
)
from services.gateway.auth import create_session


@pytest_asyncio.fixture
async def tenant_id(gateway_pool: asyncpg.Pool) -> UUID:
    """Override the gateway-level tenant_id fixture so the tenants row
    is inserted before any tenant-FK insert (actors / predictions /
    sessions) fires."""
    tid = uuid7()
    await gateway_pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tid, f"forecasts_test_{tid}",
    )
    return tid


@pytest_asyncio.fixture
async def seeded_actor(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
) -> UUID:
    """Override: depends on the local `tenant_id` fixture (which
    registers the tenant)."""
    actor_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Alice', 'active')
        """,
        actor_id, tenant_id,
    )
    return actor_id


@pytest_asyncio.fixture
async def valid_session(
    gateway_pool: asyncpg.Pool, seeded_actor: UUID, tenant_id: UUID,
) -> tuple[str, UUID]:
    """Override: depends on the local seeded_actor / tenant_id."""
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor, tenant_id=tenant_id,
    )
    return token, ctx.actor_id


@pytest_asyncio.fixture
async def registered_tenant(tenant_id: UUID) -> UUID:
    """Alias for tenant_id — the override above already inserts the
    tenants row. Kept as a separate fixture name so the tests read
    explicitly."""
    return tenant_id


async def seed_prediction(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    statement: str = "Test prediction",
    category: str = "customer_risk",
    confidence: float = 0.7,
    status: str = "active",
    resolution_days: int = 5,
    impact: dict[str, Any] | None = None,
    key_drivers: list[dict[str, Any]] | None = None,
    target_label: str | None = None,
    outcome: str | None = None,
    timeliness: str | None = None,
    resolved_days_ago: int | None = None,
) -> UUID:
    pid = uuid4()
    resolution_at = datetime.now(timezone.utc) + timedelta(days=resolution_days)
    resolved_at = (
        datetime.now(timezone.utc) - timedelta(days=resolved_days_ago)
        if resolved_days_ago is not None else None
    )
    await pool.execute(
        """
        INSERT INTO predictions (
          id, tenant_id, status, statement, category, target_label,
          confidence, key_drivers, impact,
          resolution_at, resolved_at, outcome, resolution_timeliness
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8::jsonb, $9::jsonb,
          $10, $11, $12, $13
        )
        """,
        pid, tenant, status, statement, category, target_label,
        confidence,
        json.dumps(key_drivers or []),
        json.dumps(impact or {}),
        resolution_at, resolved_at, outcome, timeliness,
    )
    return pid


async def seed_signal(
    pool: asyncpg.Pool,
    *,
    prediction_id: UUID,
    source: str = "test",
    title: str = "test signal",
    ordinal: int = 0,
    weight: float | None = 0.5,
) -> UUID:
    sid = uuid4()
    await pool.execute(
        """
        INSERT INTO prediction_signals
          (id, prediction_id, source, title, ts, trust_tier, weight, ordinal)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        sid, prediction_id, source, title,
        datetime.now(timezone.utc), "authoritative", weight, ordinal,
    )
    return sid


__all__ = [
    "registered_tenant",
    "seed_prediction",
    "seed_signal",
]
