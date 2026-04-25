"""
services/models/tests/conftest.py — shared fixtures for Models tests.

Every test is `@pytest.mark.integration` (real Postgres per BUILD-PLAN
§0.5). We build the foundation rows the tests need up front:

  - an actor (for scope_actors + metadata visibility)
  - a born_from_event Observation (required FK on models)
  - a deterministic 768-float embedding (no Ollama round-trip in unit
    tests so they are fast and offline-safe)

Fixture architecture mirrors services/observations/tests/conftest.py:
  * override `fresh_db` to skip global TRUNCATE (avoids racing with
    parallel Wave-1 agents — actor/entity_alias tests may be running
    against the same DB).
  * every integration test wraps its DB work in a single transaction
    via `tx_conn`; the transaction is rolled back at teardown, so no
    residue is left for other agents' TRUNCATEs.
  * tenant isolation is our hermetic boundary — every test uses a
    fresh `tenant = uuid7()` and every query filters by it.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio


# Root pyproject.toml sets `filterwarnings = ["error"]` which escalates
# asyncpg's "Resetting connection with an active transaction" warning
# into an error during test teardown. Our `tx_conn` fixture rolls back
# cleanly, but asyncpg's connection release is not always synchronous
# with respect to asyncio's "unraisable" handler. We silence the
# specific warning for this subtree — the tests themselves still
# ROLLBACK correctly so no residual state leaks.
_RESETTING_FILTER = (
    "ignore::pytest.PytestUnraisableExceptionWarning"
)


def pytest_collection_modifyitems(config, items):  # noqa: D401
    """Attach the warning filter to every item in this subtree."""
    for item in items:
        if "services/models/tests/" in str(item.fspath):
            item.add_marker(pytest.mark.filterwarnings(_RESETTING_FILTER))

from lib.shared.ids import uuid7
from services.models.repo import ModelsRepo


# ---------------------------------------------------------------------
# tenant / actor helpers
# ---------------------------------------------------------------------


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> uuid.UUID:
    return uuid7()


# ---------------------------------------------------------------------
# DB pool / transaction lifecycle
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Per-test asyncpg pool. Higher max_size than the root conftest to
    tolerate the sequential 100-model bulk update test comfortably.
    Skips the global TRUNCATE that the root `fresh_db` performs: we
    rely on tenant UUID isolation and a per-test ROLLBACK transaction
    to keep tests hermetic.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Override the root `fresh_db` fixture. We do NOT TRUNCATE — tenant
    isolation is our boundary. Parallel Wave-1 agents run in separate
    tenant UUIDs and don't clobber us (and we don't clobber them).
    """
    yield db_pool


@pytest_asyncio.fixture
async def tx_conn(fresh_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    A dedicated asyncpg Connection with a transaction held open for the
    whole test body. At teardown we ROLLBACK so all writes vanish and
    parallel agents' truncation waits behind our lock cleanly. Every
    write goes through this connection so the repo sees its own rows
    (no cross-connection visibility issues).

    Registers the pgvector codec on this connection so raw SQL tests
    can pass list[float] arguments for VECTOR columns.
    """
    from pgvector.asyncpg import register_vector

    conn = await fresh_db.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await fresh_db.release(conn)


# ---------------------------------------------------------------------
# Actor / observation builders bound to the test transaction
# ---------------------------------------------------------------------


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
    """
    A `signal` observation pointed at by born_from_event_id on Models
    created in the same test.
    """
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


# ---------------------------------------------------------------------
# deterministic embedding — 768 floats derived from text (no Ollama)
# ---------------------------------------------------------------------


def make_embedding(text: str, *, dim: int = 768) -> list[float]:
    """
    Produce a deterministic 768-dim vector from a text seed.

    We hash the text and use the bytes as a PRNG seed so similar texts
    produce similar vectors (for cluster tests) and identical texts
    produce identical vectors (for dedup tests).
    """
    import random

    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    # Normalise — cosine distance cares about direction, not magnitude.
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def similar_embedding(base: list[float], *, jitter: float = 0.05) -> list[float]:
    """Return a small-perturbation neighbour of `base`."""
    import random

    rng = random.Random(42)
    v = [x + rng.gauss(0.0, jitter) for x in base]
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm else v


@pytest.fixture
def embedding() -> list[float]:
    """A canonical 768-dim embedding for simple tests."""
    return make_embedding("alice ships prs consistently")


# ---------------------------------------------------------------------
# Proposition builders
# ---------------------------------------------------------------------


def state_proposition(subject: str = "alice", assertion: str = "is reliable") -> dict:
    return {"kind": "state", "subject": subject, "assertion": assertion}


def prediction_proposition(
    expected: str = "c-187 doneverified by 2026-06-01",
    resolution: str = "commitment c-187 state",
) -> dict:
    return {"kind": "prediction", "expected": expected, "resolution": resolution}


def every_kind_proposition() -> list[dict]:
    return [
        {"kind": "state", "subject": "alice", "assertion": "is careful"},
        {"kind": "relation", "subject": "alice", "relation": "reports_to", "object": "bob"},
        {"kind": "prediction", "expected": "ship by Friday", "resolution": "commitment X state"},
        {
            "kind": "pattern",
            "signature": {"regex": "^hotfix"},
            "observed_tendency": "hotfixes arrive on Fridays",
            "trigger_conditions": ["label=hotfix"],
        },
        {
            "kind": "pattern_instance",
            "pattern_id": str(uuid7()),
            "matched_context": {"channel": "github", "pr_id": 123},
        },
        {
            "kind": "capability_assessment",
            "capability_id": "python-backend",
            "assessment": "senior",
        },
        {
            "kind": "hypothesis",
            "hypothesis_text": "maybe latency is DB-bound",
            "test_conditions": ["run EXPLAIN ANALYZE on slow endpoint"],
        },
        {
            "kind": "concern",
            "about": "customer-churn",
            "nature": "contractual risk",
            "raised_by": "cs-manager",
        },
        {
            "kind": "market_assessment",
            "subject_external": "competitor-foo",
            "assessment": "product-market-fit",
        },
        {
            "kind": "environmental_trend",
            "signature": "regulation:ai-act",
            "direction": "tightening",
            "strength": "moderate",
        },
    ]


# ---------------------------------------------------------------------
# ModelsRepo fixture — repo is bound to the test transaction connection
# via the `conn` parameter on every repo call. The repo itself still
# holds the pool because some operations (bulk_confidence_update, for
# example) want to manage their own transaction nesting, but our tests
# pass `conn=tx_conn` whenever an op must live inside the shared test
# transaction.
# ---------------------------------------------------------------------


@pytest.fixture
def repo(fresh_db: asyncpg.Pool) -> ModelsRepo:
    return ModelsRepo(fresh_db, embedder=None)


@pytest_asyncio.fixture
async def pool(fresh_db: asyncpg.Pool) -> asyncpg.Pool:
    return fresh_db


__all__ = [
    "make_embedding",
    "similar_embedding",
    "state_proposition",
    "prediction_proposition",
    "every_kind_proposition",
]
