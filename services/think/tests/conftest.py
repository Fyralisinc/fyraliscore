"""services/think/tests/conftest.py — per-test pool + JSONB codec +
tenant-isolated fixtures for Wave 3-B.

Mirrors the Wave 1-D / 2-A / 3-A pattern: per-test asyncpg pool with
pgvector/JSONB codec install; tenant-UUID hermetic boundary so the
shared local DB doesn't need TRUNCATEs between tests.

Most Think tests need a committed transaction (so apply_diff can
write its applied_triggers row and the cascade/anomaly flow can
proceed). For tests that need CROSS-TX concurrency (the region-lock
pair, the idempotency retry) we open explicit connections.

Cleanup strategy: after each test the fixture cascades DELETE the
rows this tenant inserted (observations, models, ..., think_runs,
applied_triggers, think_anomalies_raw, etc.). Tenant isolation does
the heavy lifting; this cleanup is mostly about applied_triggers
and think_anomalies_raw which share the same primary keys across
tests if uuids collide (rare but possible).
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7

from services.models.repo import ModelsRepo


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# ScriptedProvider — test double per BUILD-PLAN 3.B test constraints.
# ---------------------------------------------------------------------


class ScriptedProvider(LLMProvider):
    """
    Replays canned raw JSON responses (or exceptions) in order from a
    list. Each call to `_raw_call` pops the next item.

    If the item is a string, it's returned as the raw text. If it's an
    Exception subclass instance, it's raised.
    """

    def __init__(
        self,
        responses: list[str | Exception] | None = None,
        cfg: LLMConfig | None = None,
    ):
        super().__init__(
            cfg or LLMConfig(provider="anthropic", api_key="test", model="m")
        )
        self.responses: list[str | Exception] = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(
        self, *, system, user, temperature, max_tokens, schema_hint,
    ):
        self.calls.append(
            {
                "system": system, "user": user,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema_hint": schema_hint,
            }
        )
        if not self.responses:
            raise RuntimeError("ScriptedProvider has no more responses")
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    def enqueue(self, response: str | Exception) -> None:
        self.responses.append(response)


# ---------------------------------------------------------------------
# Pool + codecs
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping Think integration tests.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=15, init=_init_connection,
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _init_connection(conn: asyncpg.Connection) -> None:
    try:
        await register_vector(conn)
    except Exception:
        pass


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """Override root `fresh_db` — we rely on tenant isolation."""
    yield db_pool


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> uuid.UUID:
    return uuid7()


@pytest_asyncio.fixture
async def models_repo(fresh_db: asyncpg.Pool) -> ModelsRepo:
    return ModelsRepo(fresh_db, embedder=None)


@pytest_asyncio.fixture
async def tenant_cleanup(fresh_db: asyncpg.Pool, tenant: uuid.UUID):
    """
    After-test cleanup. Delete everything tied to `tenant` across the
    tables Think touches. Keeps the shared DB tidy for concurrent test
    runs (we don't TRUNCATE; each test uses a fresh tenant UUID).
    """
    yield
    async with fresh_db.acquire() as conn:
        # Delete in dep order (child tables first).
        await conn.execute(
            "DELETE FROM think_anomalies_raw WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM applied_triggers WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_runs WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_region_lock_log WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM model_reeval_dead_letter WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM model_reeval_queue WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_trigger_queue WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM relationship_maintenance_log WHERE tenant_id = $1", tenant,
        )
        # Core foundation tables
        await conn.execute(
            "DELETE FROM customer_commitments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM resource_deployments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM contributes_to WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM depends_on WHERE dependent_commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM constrained_by WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM commitment_contributors WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM commitments WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM goals WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM decisions WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM resource_transactions WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM resources WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM models WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM observations WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM actor_identity_mappings WHERE actor_id IN "
            "(SELECT id FROM actors WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM actors WHERE tenant_id = $1", tenant,
        )


# ---------------------------------------------------------------------
# Deterministic embeddings — same as Wave 3-A.
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


# ---------------------------------------------------------------------
# Minimal fixture builder — actors / seed observations / small model.
# Used by tests that need a "warm" tenant but NOT the full 200-obs set.
# ---------------------------------------------------------------------


@dataclass
class Fixtures:
    tenant_id: uuid.UUID
    actor_a: uuid.UUID
    actor_b: uuid.UUID
    obs_a: uuid.UUID
    obs_b: uuid.UUID
    model_high_conf: uuid.UUID | None = None
    commitment_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None


async def _insert_actor(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    display_name: str,
    type_: str = "human_internal",
) -> uuid.UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
        VALUES ($1, $2, $3, $4, 'active', now())
        """,
        aid, tenant_id, type_, display_name,
    )
    return aid


async def _insert_observation(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    actor_id: uuid.UUID | None = None,
    content_text: str = "event",
    kind: str = "signal",
    trust_tier: str = "authoritative",
    source_channel: str = "test",
    occurred_at: datetime | None = None,
    external_id: str | None = None,
    entities_mentioned: list[dict[str, Any]] | None = None,
) -> uuid.UUID:
    oid = uuid7()
    if occurred_at is None:
        occurred_at = datetime.now(timezone.utc)
    embedding = make_embedding(content_text)
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending,
           trust_tier, external_id, entities_mentioned)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, FALSE, $10, $11, $12::jsonb)
        """,
        oid, tenant_id, occurred_at, kind, source_channel, actor_id,
        json.dumps({"text": content_text}), content_text,
        embedding, trust_tier, external_id,
        json.dumps(entities_mentioned or []),
    )
    return oid


@pytest_asyncio.fixture
async def warm_fixtures(
    fresh_db: asyncpg.Pool,
    tenant: uuid.UUID,
    tenant_cleanup,
) -> Fixtures:
    """
    Small fixture: 2 actors + 2 authoritative observations. Tests that
    need a Model or Commitment create them explicitly via the repo
    path (so we exercise the real insert pipeline).
    """
    async with fresh_db.acquire() as conn:
        actor_a = await _insert_actor(conn, tenant, "Alice")
        actor_b = await _insert_actor(conn, tenant, "Bob")
        obs_a = await _insert_observation(
            conn, tenant, actor_id=actor_a,
            content_text="Alice committed to ship feature X by April 30th.",
            source_channel="slack:general",
            external_id="slack-msg-1",
        )
        obs_b = await _insert_observation(
            conn, tenant, actor_id=actor_a,
            content_text="PR #187 merged by Alice — closes commitment c-187.",
            source_channel="github:pr",
            external_id="github-pr-187",
        )
    return Fixtures(
        tenant_id=tenant,
        actor_a=actor_a,
        actor_b=actor_b,
        obs_a=obs_a,
        obs_b=obs_b,
    )
