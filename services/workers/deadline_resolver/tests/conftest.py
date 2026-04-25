"""Fixtures for services/workers/deadline_resolver tests.

Pattern:
  * per-test asyncpg pool
  * JSONB codec installed on every connection so tests can pass Python
    dicts directly where the worker / evaluators cast to jsonb
  * tenant isolation as the hermetic boundary (every test gets a fresh
    `tenant_id = uuid7()` and every query filters by it)

We do NOT truncate and we do NOT wrap everything in a rollback
transaction. The resolver acquires its own pool connection, so the
rows a test seeds must be visible across connections — which means
they must be committed, and tenant isolation does the cleanup work.

A light retry wrapper guards against the occasional TRUNCATE victim
shape from parallel agents (same pattern as the entity_resolver
tests).
"""
from __future__ import annotations

import json
import os
import pathlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


pytestmark = pytest.mark.integration


_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
    "UndefinedTableError",
    # Parallel agents' TRUNCATE can nuke our actor row between insert
    # and the commitment/observation insert that references it.
    "ForeignKeyViolationError",
    # Assertion shapes we see when a TRUNCATE deleted our rows mid-test.
    "assert 0 ==",
    "AssertionError",
)


def pytest_runtest_protocol(item, nextitem):
    """Retry up to 3 times on transient TRUNCATE-victim errors.

    Applies ONLY to tests in this subtree so unrelated collection
    (Wave 3-B think tests etc) doesn't double-run under its umbrella.
    """
    if "services/workers/deadline_resolver" not in str(item.fspath):
        return None   # let pytest default handle it
    from _pytest.runner import runtestprotocol

    max_attempts = 3
    for attempt in range(max_attempts):
        reports = runtestprotocol(item, nextitem=nextitem, log=False)
        failed = any(r.failed for r in reports)
        transient = any(
            r.failed
            and r.longrepr is not None
            and any(err in repr(r.longrepr) for err in _TRANSIENT_ERRORS)
            for r in reports
        )
        if (not failed) or (not transient) or attempt == max_attempts - 1:
            for r in reports:
                item.ihook.pytest_runtest_logreport(report=r)
            return True
    return True


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )


@pytest_asyncio.fixture
async def resolver_db() -> AsyncGenerator[asyncpg.Pool, None]:
    """Dedicated pool for deadline_resolver tests with JSONB codec."""
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping deadline_resolver tests.")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=5,
        init=_install_json_codec,
    )
    # Migrations are idempotent — apply once per pool to be safe when
    # run in isolation.
    async with pool.acquire() as conn:
        for path in sorted((REPO_ROOT / "db" / "migrations").glob("*.sql")):
            await conn.execute(path.read_text())
    try:
        yield pool
    finally:
        try:
            await pool.close()
        except Exception:
            pool.terminate()


# Override the root `fresh_db` — we don't want TRUNCATE between tests;
# tenant isolation is our boundary.
@pytest_asyncio.fixture
async def fresh_db(resolver_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    yield resolver_db


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()


@pytest.fixture
def other_tenant_id() -> UUID:
    return uuid7()


# Shared per-test cleanup: after the test body, delete any rows these
# tests may have left behind in tables shared across parallel agents.
# Especially `think_trigger_queue` — a benchmark test that enqueues a
# thousand rows will starve Wave 3-B's think worker tests if we don't
# clean up. Cleanup is per-tenant so cross-test rows belonging to
# OTHER tenants stay untouched.
@pytest_asyncio.fixture(autouse=True)
async def _deadline_resolver_tenant_cleanup(
    resolver_db: asyncpg.Pool,
    tenant_id: UUID,
    other_tenant_id: UUID,
) -> AsyncGenerator[None, None]:
    yield
    async with resolver_db.acquire() as conn:
        for t in (tenant_id, other_tenant_id):
            await conn.execute(
                "DELETE FROM think_trigger_queue WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM applied_triggers WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM resource_transactions WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM resources WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM commitments WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM models WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM observations WHERE tenant_id = $1",
                t,
            )
            await conn.execute(
                "DELETE FROM actors WHERE tenant_id = $1",
                t,
            )


# ---------------------------------------------------------------------
# Seeders
# ---------------------------------------------------------------------


_EMBEDDING_DIM = 768


def _det_embedding(text: str, dim: int = _EMBEDDING_DIM) -> list[float]:
    """Deterministic unit-length embedding — no Ollama in tests."""
    import hashlib
    import random

    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


async def _seed_actor(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    display_name: str = "Alice",
) -> UUID:
    aid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, email, status,
                metadata, specification_id, created_at, last_seen_at
            ) VALUES (
                $1, $2, 'human_internal', $3,
                $4, 'active',
                '{}'::jsonb, NULL, now(), NULL
            )
            """,
            aid,
            tenant_id,
            display_name,
            f"{display_name.lower()}@example.com",
        )
    return aid


async def _seed_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    kind: str = "signal",
    source_channel: str = "test:signal",
    content_text: str = "test obs",
    content: dict | None = None,
    actor_id: UUID | None = None,
    occurred_at: datetime | None = None,
    entities_mentioned: list[dict] | None = None,
    trust_tier: str = "authoritative",
) -> UUID:
    obs_id = uuid7()
    if occurred_at is None:
        occurred_at = datetime.now(timezone.utc)
    content = content or {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                actor_id, content, content_text,
                embedding, embedding_pending, trust_tier,
                external_id, entities_mentioned
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7::jsonb, $8,
                NULL, TRUE, $9,
                NULL, $10::jsonb
            )
            """,
            obs_id,
            tenant_id,
            occurred_at,
            kind,
            source_channel,
            actor_id,
            json.dumps(content),
            content_text,
            trust_tier,
            json.dumps(entities_mentioned or []),
        )
    return obs_id


async def _seed_prediction(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    born_from_event_id: UUID,
    evaluate_at: datetime,
    falsifier: dict,
    confidence: float = 0.6,
    scope_actors: list[UUID] | None = None,
    contributing_models: list[UUID] | None = None,
    resolution_criteria: dict | None = None,
    status: str = "active",
    created_at: datetime | None = None,
    natural: str = "Alice will ship the PR by Friday",
) -> UUID:
    """Insert a prediction Model straight via raw SQL — we need full
    control over `evaluate_at` (past values) which the ModelsRepo
    pipeline validates against.
    """
    mid = uuid7()
    emb = _det_embedding(natural)
    proposition = {
        "kind": "prediction",
        "expected": "doneverified by Friday",
        "resolution": "commitment state",
    }
    async with pool.acquire() as conn:
        # Register pgvector codec.
        from pgvector.asyncpg import register_vector
        try:
            await register_vector(conn)
        except Exception:
            pass
        # Allow the caller to pin created_at precisely.
        if created_at is not None:
            await conn.execute(
                """
                INSERT INTO models (
                    id, tenant_id, born_from_event_id,
                    proposition, "natural", embedding,
                    scope_actors, scope_entities, scope_temporal,
                    confidence, activation, falsifier,
                    signal_readings, reading_contestable,
                    supporting_event_ids, supporting_model_ids, evidential_weight,
                    status, evaluate_at, resolution_criteria,
                    contributing_models, visible_to_subjects,
                    confidence_at_assertion, activation_coefficient,
                    created_at
                ) VALUES (
                    $1, $2, $3,
                    $4::jsonb, $5, $6,
                    $7::uuid[], '[]'::jsonb, '{"type":"now"}'::jsonb,
                    $8, 1.0, $9::jsonb,
                    '[]'::jsonb, TRUE,
                    '{}'::uuid[], '{}'::uuid[], 0.5,
                    $10, $11, $12::jsonb,
                    $13::uuid[], TRUE,
                    $8, 1.0,
                    $14
                )
                """,
                mid,
                tenant_id,
                born_from_event_id,
                json.dumps(proposition),
                natural,
                emb,
                list(scope_actors or []),
                confidence,
                json.dumps(falsifier),
                status,
                evaluate_at,
                json.dumps(resolution_criteria) if resolution_criteria else None,
                list(contributing_models or []),
                created_at,
            )
        else:
            await conn.execute(
                """
                INSERT INTO models (
                    id, tenant_id, born_from_event_id,
                    proposition, "natural", embedding,
                    scope_actors, scope_entities, scope_temporal,
                    confidence, activation, falsifier,
                    signal_readings, reading_contestable,
                    supporting_event_ids, supporting_model_ids, evidential_weight,
                    status, evaluate_at, resolution_criteria,
                    contributing_models, visible_to_subjects,
                    confidence_at_assertion, activation_coefficient
                ) VALUES (
                    $1, $2, $3,
                    $4::jsonb, $5, $6,
                    $7::uuid[], '[]'::jsonb, '{"type":"now"}'::jsonb,
                    $8, 1.0, $9::jsonb,
                    '[]'::jsonb, TRUE,
                    '{}'::uuid[], '{}'::uuid[], 0.5,
                    $10, $11, $12::jsonb,
                    $13::uuid[], TRUE,
                    $8, 1.0
                )
                """,
                mid,
                tenant_id,
                born_from_event_id,
                json.dumps(proposition),
                natural,
                emb,
                list(scope_actors or []),
                confidence,
                json.dumps(falsifier),
                status,
                evaluate_at,
                json.dumps(resolution_criteria) if resolution_criteria else None,
                list(contributing_models or []),
            )
    return mid


async def _seed_commitment(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    owner_id: UUID,
    born_from_event_id: UUID,
    state: str = "active",
    due_date: datetime | None = None,
    title: str = "Ship the PR",
) -> UUID:
    cid = uuid7()
    if due_date is None:
        due_date = datetime.now(timezone.utc) + timedelta(days=7)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO commitments (
                id, tenant_id, title, description, state, owner_id,
                due_date, ambition_level, priority,
                created_at, last_state_change_at,
                created_by_event_id
            ) VALUES (
                $1, $2, $3, NULL, $4, $5,
                $6, 'base', 5,
                now(), now(),
                $7
            )
            """,
            cid,
            tenant_id,
            title,
            state,
            owner_id,
            due_date,
            born_from_event_id,
        )
    return cid


async def _seed_resource(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    kind: str = "capacity",
    identity: str = "eng-capacity",
    current_value: dict | None = None,
    metadata: dict | None = None,
    born_from_event_id: UUID | None = None,
) -> UUID:
    rid = uuid7()
    cv = current_value if current_value is not None else {
        "available_capacity": 0.5
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, description,
                current_value, valuation_confidence,
                utilization_state, controllability, temporal_character,
                metadata, created_at, last_updated_at,
                last_updated_by_event_id, archived_at
            ) VALUES (
                $1, $2, $3, $4, NULL,
                $5::jsonb, 1.0,
                'available', 'owned', 'permanent',
                $6::jsonb, now(), now(),
                $7, NULL
            )
            """,
            rid,
            tenant_id,
            kind,
            identity,
            json.dumps(cv),
            json.dumps(metadata or {}),
            born_from_event_id,
        )
    return rid


@pytest.fixture
def seeders():
    """Bundle of seeders — exposes as attrs on a simple namespace."""
    import types

    ns = types.SimpleNamespace()
    ns.actor = _seed_actor
    ns.observation = _seed_observation
    ns.prediction = _seed_prediction
    ns.commitment = _seed_commitment
    ns.resource = _seed_resource
    ns.det_embedding = _det_embedding
    return ns


__all__ = [
    "seeders",
    "resolver_db",
    "fresh_db",
    "tenant_id",
    "other_tenant_id",
]
