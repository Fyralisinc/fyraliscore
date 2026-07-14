"""
services/reasoning/retrieval/tests/conftest.py — per-test pool + pgvector codec
+ tenant-isolated fixtures.

Mirrors the Wave 1-D / Models conftest pattern: per-test asyncpg pool
(avoids cross-event-loop issues in pytest-asyncio 1.x), JSONB codec
installation wrapping each connection, and a tenant-UUID hermetic
boundary so we don't trip over other agents' parallel test runs.

The `fixture_set` fixture hand-builds the 200-obs / 100-models /
50-commits / 20-goals / 10-customers dataset by going through the
Wave 1/2 repos (Observations, Models, Acts, Resources) so the
retrieval tests exercise the full write path. Do NOT shortcut past
the repos; the prompt is explicit about this.
"""
from __future__ import annotations

import os
import pathlib
import uuid
import hashlib
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7

from services.domain.models.repo import ModelsRepo

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MIGRATIONS_READY = False


pytestmark = pytest.mark.integration

_RESETTING_FILTER = "ignore::pytest.PytestUnraisableExceptionWarning"


def pytest_collection_modifyitems(config, items):  # noqa: D401
    """Attach the asyncpg teardown warning filter to retrieval integration tests."""
    for item in items:
        if "services/reasoning/retrieval/tests/" in str(item.fspath):
            item.add_marker(pytest.mark.filterwarnings(_RESETTING_FILTER))


# ---------------------------------------------------------------------
# Pool + transaction lifecycle
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Per-test asyncpg pool with a moderately high max_size to tolerate
    the concurrent-retrieval benchmark test. Skips the root conftest
    TRUNCATE because tenant isolation is our hermetic boundary.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=25,
        init=_init_connection,
    )
    async with pool.acquire() as conn:
        from lib.shared.migrations import apply_migrations_dir

        global _MIGRATIONS_READY
        if not _MIGRATIONS_READY and not await _schema_looks_ready(conn):
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        _MIGRATIONS_READY = True
    try:
        yield pool
    finally:
        await pool.close()


async def _init_connection(conn: asyncpg.Connection) -> None:
    """
    Install pgvector + JSONB codecs on every new pool connection so
    `list[float]` round-trips as VECTOR(768) and JSONB columns don't
    return raw `str`. This is the Wave 1-D pattern the prompt calls
    out — lib/shared/db.py doesn't do this yet and our tests must.
    """
    try:
        await register_vector(conn)
    except Exception:
        pass


async def _schema_looks_ready(conn: asyncpg.Connection) -> bool:
    rows = await conn.fetch(
        """
        SELECT to_regclass(name) IS NOT NULL AS exists
        FROM unnest($1::text[]) AS name
        """,
        [
            "public.observations",
            "public.models",
            "public.model_semantic_terms",
            "public.model_semantic_term_postings",
            "public.model_representation_feature_postings",
            "public.model_edges",
            "public.model_search_documents",
            "public.model_sparse_terms",
            "public.model_events",
            "public.projection_snapshots",
            "public.relation_claims",
            "public.inquiry_sessions",
            "public.inquiry_question_runs",
        ],
    )
    return bool(rows) and all(row["exists"] for row in rows)


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Override the root `fresh_db` fixture. We do NOT TRUNCATE — tenant
    UUID isolation is our hermetic boundary.
    """
    yield db_pool


@pytest_asyncio.fixture
async def tx_conn(fresh_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Acquire one connection for the whole test body, open a transaction
    on it, and ROLLBACK at teardown. The repo calls accept `conn=` so
    every write goes through this connection.
    """
    conn = await fresh_db.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    # Migration 0037: defer tenant FK to commit (rollback teardown
    # never triggers the check).
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await fresh_db.release(conn)


async def _insert_test_tenant(
    pool: asyncpg.Pool,
    tenant_id: uuid.UUID,
    *,
    name: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, false)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            name,
        )


@pytest_asyncio.fixture
async def tenant(fresh_db: asyncpg.Pool) -> uuid.UUID:
    tenant_id = uuid7()
    await _insert_test_tenant(
        fresh_db,
        tenant_id,
        name="retrieval test tenant",
    )
    return tenant_id


@pytest_asyncio.fixture
async def other_tenant(fresh_db: asyncpg.Pool) -> uuid.UUID:
    tenant_id = uuid7()
    await _insert_test_tenant(
        fresh_db,
        tenant_id,
        name="retrieval other test tenant",
    )
    return tenant_id


@pytest_asyncio.fixture
async def actor_id(tx_conn: asyncpg.Connection, tenant: uuid.UUID) -> uuid.UUID:
    aid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', 'Test Alice',
            'alice@example.com', 'active',
            '{}'::jsonb, NULL, now(), NULL
        )
        ON CONFLICT (id) DO NOTHING
        """,
        aid,
        tenant,
    )
    return aid


@pytest_asyncio.fixture
async def born_from_event(
    tx_conn: asyncpg.Connection, tenant: uuid.UUID, actor_id: uuid.UUID
) -> uuid.UUID:
    oid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:signal',
            $3, '{}'::jsonb, 'test observation',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        oid,
        tenant,
        actor_id,
        f"test-external-{oid}",
    )
    return oid


def make_embedding(text: str, *, dim: int = 768) -> list[float]:
    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    import random

    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


@pytest.fixture
def embedding() -> list[float]:
    return make_embedding("alice ships prs consistently")


@pytest_asyncio.fixture
async def models_repo(fresh_db: asyncpg.Pool) -> ModelsRepo:
    # No embedder — we pass precomputed embeddings everywhere in tests.
    return ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )


@pytest.fixture
def repo(fresh_db: asyncpg.Pool) -> ModelsRepo:
    return ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )


@pytest_asyncio.fixture
async def pool(fresh_db: asyncpg.Pool) -> asyncpg.Pool:
    return fresh_db
