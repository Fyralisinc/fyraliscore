"""Fixtures for services/ingestion tests.

Wave 2-A chose to place shared fixtures in a helper module
(`services/gateway/tests/_shared_fixtures.py` is inlined here for
simplicity — duplicating is safer than cross-conftest imports under
pytest's fixture-resolution rules).

Uses the same per-test-pool + TRUNCATE pattern as services/gateway/
tests/conftest.py.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import pathlib
import struct
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.ids import uuid7
from services.gateway.db_bootstrap import _register_codecs


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


# Retry transient cross-agent deadlocks / serialization failures per
# Wave 1-A conftest pattern.  Parallel Wave 2 agents' pytest sessions
# can steal locks from our tests' observations rows mid-test.
_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
    "UntranslatableCharacterError",
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


class _DeterministicEmbedder:
    class _C:
        model = "test-fake"
        expected_dim = EMBEDDING_DIM

    def __init__(self) -> None:
        self.config = self._C()

    async def embed(self, text: str) -> list[float]:
        h = hashlib.sha512((text or "").encode("utf-8")).digest()
        pool = b""
        while len(pool) < EMBEDDING_DIM * 4:
            pool += hashlib.sha512(pool + h).digest()
        vec: list[float] = []
        for i in range(EMBEDDING_DIM):
            raw = struct.unpack("<f", pool[i * 4 : (i + 1) * 4])[0]
            if not (-1e6 < raw < 1e6):
                raw = 0.0
            vec.append(max(-1.0, min(1.0, raw / 1e3)))
        return vec

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [await self.embed(t) for t in texts]

    async def close(self) -> None:
        return None


async def _run_migrations(conn: asyncpg.Connection) -> None:
    from lib.shared.migrations import apply_migrations_dir
    await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")


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
    tables = [r["relname"] for r in rows]
    if not tables:
        return
    table_list = ", ".join(f'"{t}"' for t in tables)
    # Retry the TRUNCATE on DeadlockDetected — orphan locks from a
    # prior test's pool teardown may still be unwinding.
    await conn.execute("SET lock_timeout = '1s'")
    for attempt in range(5):
        try:
            await conn.execute(
                f"TRUNCATE {table_list} RESTART IDENTITY CASCADE"
            )
            return
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.LockNotAvailableError,
        ):
            await asyncio.sleep(0.2 * (attempt + 1))
    # Final attempt — let exceptions propagate.
    await conn.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")


async def _wait_idle(dsn: str, max_wait_ms: float = 2000.0) -> None:
    """Wait for prior pools to finish holding server-side locks."""
    start = asyncio.get_event_loop().time()
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
        if (asyncio.get_event_loop().time() - start) * 1000 > max_wait_ms:
            return
        await asyncio.sleep(0.02)


@pytest_asyncio.fixture
async def gateway_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping ingestion integration test.")
    await _wait_idle(dsn)
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=20, init=_register_codecs
    )
    try:
        async with pool.acquire() as conn:
            await _run_migrations(conn)
            await _truncate_all(conn)
        yield pool
    finally:
        # Terminate first so any in-flight connections are force-closed
        # on the server side before we return. `close()` would wait for
        # queries to complete gracefully — that's what we want in
        # production but NOT between tests where stale locks cause
        # the next TRUNCATE to deadlock.
        try:
            pool.terminate()
        except Exception:
            pass


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()


@pytest_asyncio.fixture
async def seeded_actor(gateway_pool: asyncpg.Pool, tenant_id: UUID) -> UUID:
    actor_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Alice', 'active')
        """,
        actor_id,
        tenant_id,
    )
    return actor_id


@pytest.fixture(name="_DeterministicEmbedder")
def _deterministic_embedder_cls_fixture():
    return _DeterministicEmbedder
