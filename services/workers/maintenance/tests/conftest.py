"""Maintenance test conftest — per-test pool + migrations + TRUNCATE.

Pattern copied from the realtime conftest and the retrieval maintenance
tests. Real Postgres, no mocks.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


pytestmark = pytest.mark.integration


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


async def _run_migrations(conn: asyncpg.Connection) -> None:
    for p in sorted((REPO_ROOT / "db" / "migrations").glob("*.sql")):
        await conn.execute(p.read_text())


async def _truncate_all(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
        """
    )
    names = [r["relname"] for r in rows]
    if not names:
        return
    lst = ", ".join(f'"{t}"' for t in names)
    await conn.execute("SET lock_timeout = '1s'")
    for _ in range(5):
        try:
            await conn.execute(f"TRUNCATE {lst} RESTART IDENTITY CASCADE")
            return
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.LockNotAvailableError,
        ):
            await asyncio.sleep(0.2)
    await conn.execute(f"TRUNCATE {lst} RESTART IDENTITY CASCADE")


async def _wait_idle(dsn: str, max_wait_ms: float = 2000.0) -> None:
    import time as _t

    start = _t.monotonic()
    while True:
        probe = await asyncpg.connect(dsn)
        try:
            active = await probe.fetchval(
                """
                SELECT COUNT(*) FROM pg_stat_activity
                WHERE datname = current_database()
                  AND state IN ('active', 'idle in transaction',
                                'idle in transaction (aborted)')
                  AND pid <> pg_backend_pid()
                """
            )
        finally:
            await probe.close()
        if (active or 0) == 0:
            return
        if (_t.monotonic() - start) * 1000 > max_wait_ms:
            return
        await asyncio.sleep(0.02)


@pytest_asyncio.fixture
async def m_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    await _wait_idle(dsn)
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await _run_migrations(conn)
        await _truncate_all(conn)
    try:
        yield pool
    finally:
        try:
            pool.terminate()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------


async def _ensure_obs_partition(conn: asyncpg.Connection, occurred_at: datetime) -> None:
    """Ensure a monthly observations partition covering `occurred_at`
    exists. Idempotent (CREATE TABLE IF NOT EXISTS).
    """
    # Month bounds for the partition.
    d = occurred_at.astimezone(timezone.utc).date().replace(day=1)
    if d.month == 12:
        end = d.replace(year=d.year + 1, month=1)
    else:
        end = d.replace(month=d.month + 1)
    name = f"observations_{d.strftime('%Y_%m')}"
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{name}" '
        f'PARTITION OF "observations" '
        f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}')"
    )
    await conn.execute(sql)


async def seed_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    occurred_at: datetime | None = None,
    kind: str = "signal",
    source_channel: str = "internal:test",
    trust_tier: str = "authoritative",
    content: dict | None = None,
    content_text: str = "seed",
    cause_id: UUID | None = None,
) -> UUID:
    import json

    obs_id = uuid7()
    content = content or {}
    occurred_at = occurred_at or datetime.now(timezone.utc)
    await _ensure_obs_partition(conn, occurred_at)
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier, cause_id
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
        """,
        obs_id,
        tenant_id,
        occurred_at,
        kind,
        source_channel,
        json.dumps(content),
        content_text,
        trust_tier,
        cause_id,
    )
    return obs_id


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()
