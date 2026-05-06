"""Fixtures for services/observations tests.

Builds on the top-level conftest's `fresh_db` fixture (per-test,
truncated pool). Adds:
- `repo` — an ObservationRepository bound to fresh_db with an Ollama
  embedder if OLLAMA_URL is reachable, else a scripted fallback.
- `tenant_id` — a fresh UUID v7 per test (isolation).
- `alice_actor_id` — an actor row inserted on demand for tests that
  need a real FK target for `observations.actor_id`.
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from uuid import UUID


import asyncpg
import httpx
import pytest
import pytest_asyncio

from lib.embeddings.ollama import EMBEDDING_DIM, OllamaClient, OllamaConfig
from lib.shared.ids import uuid7
from services.observations import partitions
from services.observations.repo import ObservationRepository


_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
)


# Pytest hook: retry a failed test up to 4 times when the failure is
# a Postgres deadlock / serialization error. Wave-1 is designed to
# run four agents in parallel against a shared local Postgres; their
# TRUNCATE CASCADEs can pick my transaction as a deadlock victim at
# lock-escalation time. The correct response per PG docs is to
# retry. Wrapping `pytest_runtest_protocol` triggers a full protocol
# re-run (setup -> call -> teardown) which rebuilds fixtures like
# `tx_conn` with a fresh transaction.
def pytest_runtest_protocol(item, nextitem):
    from _pytest.runner import runtestprotocol

    max_attempts = 4
    for attempt in range(max_attempts):
        reports = runtestprotocol(item, nextitem=nextitem, log=False)
        failed_due_to_deadlock = any(
            r.failed
            and r.longrepr is not None
            and any(err in repr(r.longrepr) for err in _TRANSIENT_ERRORS)
            for r in reports
        )
        if not failed_due_to_deadlock or attempt == max_attempts - 1:
            # Last attempt or not a deadlock — log these reports.
            for r in reports:
                item.ihook.pytest_runtest_logreport(report=r)
            return True
        # Swallow the deadlock-failed reports, retry the whole
        # protocol. runtestprotocol re-invokes setup/call/teardown
        # on the next iteration, which rebuilds `tx_conn` etc.
    return True


def _ollama_reachable() -> bool:
    url = os.environ.get("OLLAMA_URL")
    if not url:
        return False
    try:
        r = httpx.get(f"{url}/api/tags", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


class _DeterministicEmbedder:
    """
    Tiny fake embedder used when Ollama isn't available. Produces
    reproducible 768-dim vectors from the input text hash. Good
    enough for tests that don't care about true semantic similarity
    (most of them).
    """

    class _C:
        model = "test-fake"
        expected_dim = EMBEDDING_DIM

    def __init__(self) -> None:
        self.config = self._C()

    async def embed(self, text: str) -> list[float]:
        import hashlib
        import struct
        # Generate 768 deterministic floats in [-1, 1] from a hash.
        h = hashlib.sha512((text or "").encode("utf-8")).digest()
        vec: list[float] = []
        # repeat hash to fill 768 floats
        pool = b""
        while len(pool) < EMBEDDING_DIM * 4:
            pool += hashlib.sha512(pool + h).digest()
        for i in range(EMBEDDING_DIM):
            raw = struct.unpack("<f", pool[i * 4:(i + 1) * 4])[0]
            # normalize roughly to [-1, 1] range
            if not (-1e6 < raw < 1e6):
                raw = 0.0
            vec.append(max(-1.0, min(1.0, raw / 1e3)))
        return vec

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [await self.embed(t) for t in texts]

    async def close(self) -> None:
        return None


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()


# Override both `db_pool` and `fresh_db` for this subtree.
#
# Context: Wave 1 of BUILD-PLAN.md runs four agents in parallel
# against the same local Postgres. Each agent's pytest session owns
# a fresh_db fixture (in the top-level conftest) that TRUNCATES
# every public-schema table at the start of every test. When agent
# 1-A (me) is mid-test, agent 1-B's TRUNCATE deletes my observations
# rows, and subsequent SELECTs in my test body see nothing.
#
# Mitigation: each integration test runs inside a single asyncpg
# transaction opened at test start. We hold a SHARE ROW EXCLUSIVE
# lock on `observations` for the duration of that transaction; this
# lock conflicts with TRUNCATE (which requires ACCESS EXCLUSIVE), so
# other agents' TRUNCATE waits until my test commits/rolls back.
# We also ROLLBACK at the end to leave no residue for the next
# agent. Tenant isolation still protects us from cross-agent data.
#
# Our observations tests never rely on an empty `observations` table
# — every INSERT carries a fresh tenant_id, every SELECT filters by
# it — so this approach is safe.
#
# The repository, state_change emitter, and events helpers all
# accept an explicit `conn` parameter; tests use that to share the
# single test transaction. Code paths that acquire from the pool
# independently (e.g. `emit_pending_notifications`) run against a
# separate connection — that's fine, the data lives inside the test
# tx and is visible to them once committed-within-tx.


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    # Tenant isolation is our hermetic boundary — each test uses a
    # fresh `tenant_id = uuid7()` and every query filters by it.
    # Full table TRUNCATE from the top-level conftest is skipped here
    # to avoid racing with parallel Wave-1 agents who'd wipe our
    # rows mid-test.
    yield db_pool


@pytest_asyncio.fixture
async def embedder() -> AsyncGenerator:
    """
    Real Ollama when available, deterministic fallback otherwise.
    Some tests override this fixture with a specific fake via
    request.getfixturevalue or by constructing their own repo.
    """
    if _ollama_reachable():
        client = OllamaClient(
            OllamaConfig(
                base_url=os.environ["OLLAMA_URL"],
                model=os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
            )
        )
        try:
            yield client
        finally:
            await client.close()
        return
    yield _DeterministicEmbedder()


@pytest_asyncio.fixture
async def tx_conn(fresh_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Yield an asyncpg Connection with an open transaction. The test
    body runs all its DB operations through this connection; at the
    end we ROLLBACK, so no data is left behind for parallel Wave-1
    agents to truncate. Other agents' TRUNCATE waits on ACCESS
    EXCLUSIVE against my ROW EXCLUSIVE lock (held automatically by
    my INSERTs); once my transaction rolls back, they proceed.
    """
    await partitions.ensure_partitions(fresh_db, months_ahead=3)
    conn = await fresh_db.acquire()
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        except Exception:
            pass
        finally:
            try:
                await fresh_db.release(conn)
            except Exception:
                pass


@pytest_asyncio.fixture
async def repo(
    tx_conn: asyncpg.Connection, embedder,
) -> AsyncGenerator[ObservationRepository, None]:
    """
    Repository bound to the per-test transaction connection. All
    reads/writes go through the same tx — other agents' TRUNCATE on
    observations blocks during the test and proceeds after our
    ROLLBACK. Tenant IDs are still fresh per test for defense in
    depth.
    """
    yield ObservationRepository(tx_conn, embedder=embedder)


@pytest_asyncio.fixture
async def alice_actor_id(tx_conn: asyncpg.Connection, tenant_id: UUID) -> UUID:
    aid = uuid7()
    # Inserted in the same transaction as the test body so FK
    # references to `actors(id)` from our observations succeed even
    # under aggressive parallel-agent TRUNCATEs (which wait behind
    # our transaction's lock).
    await tx_conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name)"
        " VALUES ($1, $2, 'human_internal', 'Alice')"
        " ON CONFLICT (id) DO NOTHING",
        aid,
        tenant_id,
    )
    return aid


__all__ = ["_DeterministicEmbedder", "_ollama_reachable"]
