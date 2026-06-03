"""Fixtures for scripts/ tests.

These tests are real-DB integration tests. We follow the same pattern
the Think and Models test suites use:

  * per-test asyncpg pool
  * pgvector codec registered on each connection
  * tenant UUID is the hermetic boundary
  * `tenant_cleanup` deletes every row this tenant inserted so the
    shared local DB stays tidy between tests
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import random
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.domain.models.repo import pgvector_pool_init


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=8, init=_init_connection,
    )
    async with pool.acquire() as conn:
        from lib.shared.migrations import apply_migrations_dir

        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
    try:
        yield pool
    finally:
        await pool.close()


async def _init_connection(conn: asyncpg.Connection) -> None:
    await pgvector_pool_init(conn)


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """Override root `fresh_db`: tenant isolation, no TRUNCATE."""
    yield db_pool


async def _insert_test_tenant(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    name: str = "backfill test tenant",
) -> None:
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        name,
    )


@pytest_asyncio.fixture
async def tenant(fresh_db: asyncpg.Pool) -> uuid.UUID:
    tid = uuid7()
    async with fresh_db.acquire() as conn:
        await _insert_test_tenant(conn, tid)
    return tid


@pytest_asyncio.fixture
async def tenant_cleanup(fresh_db: asyncpg.Pool, tenant: uuid.UUID):
    yield
    async with fresh_db.acquire() as conn:
        for table in (
            "relationship_candidates",
            "reconciliation_events",
            "audit_events",
            "model_edges",
            "model_scope_actors",
            "model_scope_entities",
            "model_reeval_queue",
            "models",
            "observations",
            "actors",
        ):
            try:
                await conn.execute(
                    f"DELETE FROM {table} WHERE tenant_id = $1", tenant,
                )
            except asyncpg.UndefinedTableError:
                pass


# ---------------------------------------------------------------------
# Embedding helpers (same shape as services/domain/models/tests/conftest.py)
# ---------------------------------------------------------------------


def make_embedding(text: str, *, dim: int = 768) -> list[float]:
    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def near_embedding(base: list[float], jitter: float = 0.05) -> list[float]:
    rng = random.Random(0xC0FFEE)
    v = [x + rng.gauss(0.0, jitter) for x in base]
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm else v


# ---------------------------------------------------------------------
# Seed helpers — direct SQL inserts (the backfill script reads `models`
# directly, so we don't need to go through the repo).
# ---------------------------------------------------------------------


async def insert_actor(
    conn: asyncpg.Connection, tenant_id: uuid.UUID, name: str = "Alice",
) -> uuid.UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors
          (id, tenant_id, type, display_name, status,
           metadata, specification_id, created_at)
        VALUES ($1, $2, 'human_internal', $3, 'active',
                '{}'::jsonb, NULL, now())
        ON CONFLICT (id) DO NOTHING
        """,
        aid, tenant_id, name,
    )
    return aid


async def insert_observation(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    actor_id: uuid.UUID,
    content_text: str = "event",
) -> uuid.UUID:
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending,
           trust_tier, external_id, entities_mentioned)
        VALUES ($1, $2, now(), 'signal', 'test:signal', $3,
                '{}'::jsonb, $4, NULL, TRUE, 'authoritative',
                $5, '[]'::jsonb)
        """,
        oid, tenant_id, actor_id, content_text, f"ext-{oid}",
    )
    return oid


async def insert_model(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    born_event_id: uuid.UUID,
    proposition: dict[str, Any],
    natural: str,
    embedding: list[float],
    confidence: float = 0.6,
    activation: float = 1.0,
    supporting_event_ids: list[uuid.UUID] | None = None,
    created_at_offset_s: int = 0,
) -> uuid.UUID:
    mid = uuid7()
    created_at_clause = ""
    params: list[Any] = [
        mid,
        tenant_id,
        born_event_id,
        json.dumps(proposition),
        natural,
        embedding,
        confidence,
        activation,
        confidence,  # confidence_at_assertion (clipped same as confidence)
        list(supporting_event_ids or []),
    ]
    if created_at_offset_s:
        # Allow tests to control creation ordering.
        created_at_clause = ", created_at"
        params.append(
            datetime.now(timezone.utc).replace(microsecond=0)
            + _td_seconds(created_at_offset_s)
        )
    sql = f"""
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient, supporting_event_ids,
           supporting_model_ids, falsifier
           {created_at_clause})
        VALUES ($1, $2, $3, $4::jsonb, $5, $6,
                '{{}}'::uuid[], '[]'::jsonb, '{{}}'::jsonb,
                $7, $8, 'active', $9, 1.0,
                $10::uuid[], '{{}}'::uuid[], NULL
                {", $11" if created_at_offset_s else ""})
    """
    await conn.execute(sql, *params)
    return mid


def _td_seconds(s: int):
    from datetime import timedelta
    return timedelta(seconds=s)
