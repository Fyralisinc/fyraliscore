"""Shared fixtures for services/gateway tests + services/ingestion tests.

Wave 2-A chose the **fresh_db** / TRUNCATE pattern over per-test-
transaction. Rationale:

- The Gateway acquires connections from a pool (via asyncpg pool
  semantics). Per-test-transaction works only when ALL reads/writes
  travel through the same connection; the Gateway + Ingestion stack
  touches multiple connections (pool-acquired for inserts, separate
  pool-acquired for NOTIFY, another for Wave-1 repos).
- TRUNCATE is fast (~20ms) for a freshly-populated schema, acceptable
  per test for <50 tests.
- We use a per-function pool with JSONB codecs installed so every
  `asyncpg.Record[jsonb]` is already a dict.

Concurrency note (same hazard as Wave 1-A/1-D): if other agents are
running pytest against the same Postgres instance, their TRUNCATE can
race ours. In practice Wave 2-A runs in isolation (other Wave 2
agents work on disjoint service dirs) and the full-suite run below
is sequential. If running under tight contention, rerun the suite.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import pathlib
import struct
import time
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio

from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.ids import uuid7
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.auth import create_session
from services.gateway.db_bootstrap import _register_codecs
from services.gateway.main import GatewayDeps, build_app
from services.gateway.rate_limit import RateLimiter


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


# Retry transient cross-agent deadlocks / serialization failures per
# Wave 1-A conftest pattern.
_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
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


# --------------------------------------------------------------------
# Deterministic test embedder — same shape as Wave 1 pattern
# --------------------------------------------------------------------


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


# --------------------------------------------------------------------
# Per-test pool + fresh_db (TRUNCATE)
# --------------------------------------------------------------------


async def _run_migrations(conn: asyncpg.Connection) -> None:
    migrations = sorted((REPO_ROOT / "db" / "migrations").glob("*.sql"))
    for p in migrations:
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
    tables = [r["relname"] for r in rows]
    if not tables:
        return
    table_list = ", ".join(f'"{t}"' for t in tables)
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
    await conn.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")


async def _wait_idle(dsn: str, max_wait_ms: float = 2000.0) -> None:
    """Wait for prior-test pools to release server-side locks before
    we open our own pool. See BUILD-LOG Wave 1-D §f for the hazard.
    """
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
    """Per-test pool with JSONB codec pre-installed.

    Uses `init=_register_codecs` so every connection acquired from this
    pool returns jsonb columns as dicts (Wave 2-A bootstrap pattern).
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping gateway integration test.")
    await _wait_idle(dsn)
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=10,
        init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            await _run_migrations(conn)
            await _truncate_all(conn)
        yield pool
    finally:
        # Force-terminate to release all server-side locks immediately.
        try:
            pool.terminate()
        except Exception:
            pass


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()


@pytest.fixture
def tenant_id_b() -> UUID:
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


@pytest_asyncio.fixture
async def seeded_actor_b(
    gateway_pool: asyncpg.Pool, tenant_id_b: UUID
) -> UUID:
    actor_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Bob', 'active')
        """,
        actor_id,
        tenant_id_b,
    )
    return actor_id


@pytest_asyncio.fixture
async def valid_session(
    gateway_pool: asyncpg.Pool, seeded_actor: UUID, tenant_id: UUID
) -> tuple[str, UUID]:
    """Return (bearer_token, actor_id)."""
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor, tenant_id=tenant_id
    )
    return token, ctx.actor_id


@pytest_asyncio.fixture
async def valid_session_b(
    gateway_pool: asyncpg.Pool, seeded_actor_b: UUID, tenant_id_b: UUID
) -> tuple[str, UUID]:
    """Second tenant's session — for tenant-isolation tests."""
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor_b, tenant_id=tenant_id_b
    )
    return token, ctx.actor_id


# --------------------------------------------------------------------
# App + HTTP client factory
# --------------------------------------------------------------------

SLACK_TEST_SECRET = "test-slack-signing-secret"


@pytest.fixture
def rate_limiter() -> RateLimiter:
    return RateLimiter()


@pytest.fixture(name="_DeterministicEmbedder")
def _deterministic_embedder_cls_fixture():
    """Fixture exposing the deterministic embedder class.

    Test signatures receive the class, not an instance — each test
    instantiates its own so concurrent-ingest tests don't share state.
    The underscore-prefixed name matches the class for readability.
    """
    return _DeterministicEmbedder


@pytest_asyncio.fixture
async def app_deps(
    gateway_pool: asyncpg.Pool,
    rate_limiter: RateLimiter,
):
    """Dependency bundle — ingestion uses deterministic embedder."""
    actor_repo = ActorRepo(gateway_pool)
    alias_repo = EntityAliasRepo(gateway_pool)
    embedder = _DeterministicEmbedder()
    return GatewayDeps(
        pool=gateway_pool,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,  # type: ignore[arg-type]
        rate_limiter=rate_limiter,
        slack_signing_secret=SLACK_TEST_SECRET,
    )


@pytest_asyncio.fixture
async def client(app_deps) -> AsyncGenerator[httpx.AsyncClient, None]:
    """FastAPI test client wired to the shared pool + rate limiter."""
    app = build_app(
        pool=app_deps.pool,
        actor_repo=app_deps.actor_repo,
        alias_repo=app_deps.alias_repo,
        embedder=app_deps.embedder,
        rate_limiter=app_deps.rate_limiter,
        slack_signing_secret=app_deps.slack_signing_secret,
        configure_logging=False,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as c:
        yield c


# --------------------------------------------------------------------
# Helpers for Slack signature / payload construction
# --------------------------------------------------------------------


def build_slack_payload(
    *,
    text: str = "hello",
    user: str = "U01ALICE",
    channel: str = "C01ENG",
    ts: str | None = None,
    team: str = "T01",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if ts is None:
        ts = f"{time.time():.6f}"
    event = {
        "type": "message",
        "user": user,
        "text": text,
        "ts": ts,
        "channel": channel,
    }
    if extra:
        event.update(extra)
    return {
        "team_id": team,
        "event": event,
    }


def sign_slack(
    body: bytes, *, secret: str = SLACK_TEST_SECRET, ts: str | None = None
) -> dict[str, str]:
    if ts is None:
        ts = str(int(time.time()))
    basestring = f"v0:{ts}:{body.decode('utf-8')}".encode("utf-8")
    sig = "v0=" + hmac.new(
        secret.encode("utf-8"), basestring, hashlib.sha256
    ).hexdigest()
    return {
        "X-Slack-Request-Timestamp": ts,
        "X-Slack-Signature": sig,
    }


__all__ = [
    "SLACK_TEST_SECRET",
    "build_slack_payload",
    "sign_slack",
    "_DeterministicEmbedder",
]
