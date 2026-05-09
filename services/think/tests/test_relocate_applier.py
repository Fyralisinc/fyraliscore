"""Test that the Think applier and validator route a ClaimOp(op='relocate')
through to TopoRepo.relocate.

We bypass the LLM and call _apply_claim_op + _validate_claim_op directly
with a ClaimOp instance. Uses the per-test transaction fixture from
services/topology/tests/conftest.py."""
from __future__ import annotations

import os
import sys
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.errors import ValidationError
from services.models.repo import ModelsRepo
from services.think.applier import _apply_claim_op
from services.think.diff_schema import ClaimOp
from services.think.validator import _validate_claim_op
from services.topology.topo_repo import TopoRepo


# Reuse the topology conftest's tx_conn / tenant / make_model fixtures.
# To do that we just keep this test file in services/think/tests but
# point it at the topology fixtures via direct import.

@pytest_asyncio.fixture
async def db_pool():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def tx_conn(db_pool):
    from pgvector.asyncpg import register_vector
    from services.models.repo import PGVECTOR_REGISTERED_POOL_IDS

    conn = await db_pool.acquire()
    try:
        await register_vector(conn)
        PGVECTOR_REGISTERED_POOL_IDS.add(id(conn))
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
            try:
                PGVECTOR_REGISTERED_POOL_IDS.discard(id(conn))
            except Exception:
                pass
            await db_pool.release(conn)


def _hash_emb(text):
    import hashlib
    import math
    import random
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(768)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


async def _seed(conn):
    from lib.shared.ids import uuid7
    tenant = uuid7()
    actor = uuid7()
    obs = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, email, "
        "status, metadata, specification_id, created_at) VALUES "
        "($1, $2, 'human_internal', 'rel', null, 'active', "
        "'{}'::jsonb, NULL, now())",
        actor, tenant,
    )
    await conn.execute(
        "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
        "source_channel, actor_id, content, content_text, embedding, "
        "embedding_pending, trust_tier, external_id, "
        "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
        "'test:rel', $3, '{}'::jsonb, 'rel obs', NULL, TRUE, "
        "'authoritative', $4, '[]'::jsonb)",
        obs, tenant, actor, f"rel-{obs}",
    )
    a, b = uuid7(), uuid7()
    for mid, name in ((a, "alpha"), (b, "beta")):
        emb = _hash_emb(name)
        await conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id, proposition,
                "natural", embedding, scope_actors, scope_entities,
                scope_temporal, confidence, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status, confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
                $4, $5,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.6, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active', 0.6
            )
            """,
            mid, tenant, obs, name, emb,
        )
        topo = TopoRepo()
        await topo.set_initial_topo(
            conn, model_id=mid, content_embedding=_hash_emb(name),
            tenant_id=tenant, enqueue_propagation=False,
        )
    return tenant, a, b


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_claim_op_relocate_writes_event(tx_conn):
    tenant, a, b = await _seed(tx_conn)
    op = ClaimOp(
        op="relocate",
        model_id=a,
        relocate_target={
            "kind": "model_id",
            "value": str(b),
            "alpha": 0.5,
        },
        reason="halfway toward beta",
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    summary = await _apply_claim_op(
        op, tx_conn, repo, tenant,
        cause_event_id=None,
    )
    assert summary["summary"]["op"] == "relocate"
    assert summary["state_changes"] == 0  # topology mutation, not state
    # topology_events row should exist.
    row = await tx_conn.fetchrow(
        "SELECT kind, member_model_ids FROM topology_events "
        "WHERE tenant_id = $1 ORDER BY occurred_at DESC LIMIT 1",
        tenant,
    )
    assert row is not None
    assert row["kind"] == "relocate"
    assert row["member_model_ids"] == [a]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_claim_op_relocate_accepts_well_formed(tx_conn):
    """Validator's shape check on a well-formed relocate op."""
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={
            "kind": "model_id",
            "value": str(uuid4()),
            "alpha": 0.7,
        },
        reason="ok",
    )
    out = await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())
    assert out.op == "relocate"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_claim_op_relocate_rejects_missing_target(tx_conn):
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        reason="missing target",
    )
    with pytest.raises(ValidationError, match="relocate_target"):
        await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_claim_op_relocate_rejects_bad_alpha(tx_conn):
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={
            "kind": "model_id",
            "value": str(uuid4()),
            "alpha": 2.0,  # invalid
        },
        reason="bad alpha",
    )
    with pytest.raises(ValidationError, match="alpha"):
        await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())
