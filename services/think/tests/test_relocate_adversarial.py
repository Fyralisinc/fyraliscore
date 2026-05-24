"""Adversarial tests for the Think applier's relocate handling.

Targets: malformed payload (missing target, missing reason, garbage
shape), multiple relocates per diff, mixed claim_op kinds, validator
boundary cases."""
from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.models.repo import ModelsRepo
from services.think.applier import _apply_claim_op
from services.think.diff_schema import ClaimOp
from services.think.validator import _validate_claim_op
from services.topology.topo_repo import TopoRepo


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
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")  # migration 0037: defer tenant FKs
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


def _hash(text):
    import hashlib
    import math
    import random
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0, 1) for _ in range(768)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


async def _seed(conn):
    tenant = uuid7()
    actor = uuid7()
    obs = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, "
        "email, status, metadata, specification_id, created_at) "
        "VALUES ($1, $2, 'human_internal', 'adv', null, 'active', "
        "'{}'::jsonb, NULL, now())",
        actor, tenant,
    )
    await conn.execute(
        "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
        "source_channel, actor_id, content, content_text, embedding, "
        "embedding_pending, trust_tier, external_id, "
        "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
        "'test:adv', $3, '{}'::jsonb, 'adv obs', NULL, TRUE, "
        "'authoritative', $4, '[]'::jsonb)",
        obs, tenant, actor, f"adv-{obs}",
    )
    a, b = uuid7(), uuid7()
    topo = TopoRepo()
    for mid, name in ((a, "alpha"), (b, "beta")):
        emb = _hash(name)
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
        await topo.set_initial_topo(
            conn, model_id=mid, content_embedding=emb,
            tenant_id=tenant, enqueue_propagation=False,
        )
    return tenant, a, b


# =====================================================================
# Validator boundary
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validator_rejects_relocate_with_unknown_target_kind(tx_conn):
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={"kind": "phantasm", "value": str(uuid4())},
        reason="bogus",
    )
    with pytest.raises(ValidationError, match="kind"):
        await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validator_rejects_relocate_target_value_missing(tx_conn):
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={"kind": "model_id"},
        reason="missing value",
    )
    with pytest.raises(ValidationError):
        await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validator_rejects_relocate_target_garbage_shape(tx_conn):
    """relocate_target as a string instead of a dict."""
    # ClaimOp.relocate_target is `dict | None`, but Pydantic with
    # `extra="forbid"` and the field type allows anything dict-shaped.
    # A non-dict should be coerced/rejected.
    with pytest.raises(Exception):
        ClaimOp(
            op="relocate",
            model_id=uuid4(),
            relocate_target="not-a-dict",  # type: ignore[arg-type]
            reason="bad shape",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validator_accepts_relocate_with_no_reason(tx_conn):
    """`reason` is permissive on the wire — defaults to '(no reason given)'
    in applier. Validator should accept missing reason."""
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={"kind": "model_id", "value": str(uuid4())},
    )
    # Validator only checks model_id + relocate_target shape. Should pass.
    out = await _validate_claim_op(op, None, tx_conn, tenant_id=uuid4())
    assert out.op == "relocate"


# =====================================================================
# Applier behavior
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_two_relocates_in_one_diff_records_two_events(
    tx_conn,
):
    """Apply two relocate ops in sequence — both should record their
    own topology_events row."""
    tenant, a, b = await _seed(tx_conn)
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    op1 = ClaimOp(
        op="relocate", model_id=a, reason="first",
        relocate_target={"kind": "model_id", "value": str(b), "alpha": 0.5},
    )
    op2 = ClaimOp(
        op="relocate", model_id=b, reason="second",
        relocate_target={"kind": "model_id", "value": str(a), "alpha": 0.7},
    )
    await _apply_claim_op(op1, tx_conn, repo, tenant, cause_event_id=None)
    await _apply_claim_op(op2, tx_conn, repo, tenant, cause_event_id=None)

    n = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM topology_events "
        "WHERE tenant_id = $1 AND kind = 'relocate'",
        tenant,
    )
    assert n == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_missing_model_raises(tx_conn):
    tenant, a, b = await _seed(tx_conn)
    bogus = uuid4()
    op = ClaimOp(
        op="relocate", model_id=bogus, reason="ghost",
        relocate_target={"kind": "model_id", "value": str(b), "alpha": 1.0},
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="not in tenant"):
        await _apply_claim_op(
            op, tx_conn, repo, tenant, cause_event_id=None,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_target_model_in_other_tenant_raises(tx_conn):
    """Reasoning shouldn't be able to pull topo from another tenant
    via the diff path either. Critical security gate."""
    tenant_a, a, _ = await _seed(tx_conn)
    tenant_b, b_in_b, _ = await _seed(tx_conn)
    op = ClaimOp(
        op="relocate", model_id=a, reason="cross-tenant",
        relocate_target={
            "kind": "model_id", "value": str(b_in_b), "alpha": 1.0,
        },
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="not found"):
        await _apply_claim_op(
            op, tx_conn, repo, tenant_a, cause_event_id=None,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_without_reason_uses_default(tx_conn):
    """When reason is None, applier falls back to '(no reason given)'
    in the topology_events.payload — relocates always have an audit
    string, never NULL."""
    import json
    tenant, a, b = await _seed(tx_conn)
    op = ClaimOp(
        op="relocate", model_id=a, reason=None,
        relocate_target={"kind": "model_id", "value": str(b), "alpha": 1.0},
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    out = await _apply_claim_op(
        op, tx_conn, repo, tenant, cause_event_id=None,
    )
    raw_payload = await tx_conn.fetchval(
        "SELECT payload FROM topology_events "
        "WHERE tenant_id = $1 AND kind = 'relocate' "
        "ORDER BY occurred_at DESC LIMIT 1",
        tenant,
    )
    payload = (
        json.loads(raw_payload)
        if isinstance(raw_payload, str) else raw_payload
    )
    assert payload["reason"] == "(no reason given)"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_with_cause_event_id_records_it(tx_conn):
    """If the applier receives a cause_event_id, it should land in
    payload.applied_by_diff_id for the audit trail."""
    import json
    tenant, a, b = await _seed(tx_conn)
    cause = uuid7()
    op = ClaimOp(
        op="relocate", model_id=a, reason="audit",
        relocate_target={"kind": "model_id", "value": str(b), "alpha": 1.0},
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    await _apply_claim_op(op, tx_conn, repo, tenant, cause_event_id=cause)
    raw_payload = await tx_conn.fetchval(
        "SELECT payload FROM topology_events "
        "WHERE tenant_id = $1 AND kind = 'relocate' LIMIT 1",
        tenant,
    )
    payload = (
        json.loads(raw_payload)
        if isinstance(raw_payload, str) else raw_payload
    )
    assert payload["applied_by_diff_id"] == str(cause)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_returns_state_changes_zero(tx_conn):
    """A relocate is a topology mutation, not a model state_change.
    The applier should return state_changes=0 so the
    state_changes table isn't bumped."""
    tenant, a, b = await _seed(tx_conn)
    op = ClaimOp(
        op="relocate", model_id=a, reason="t",
        relocate_target={"kind": "model_id", "value": str(b), "alpha": 1.0},
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    out = await _apply_claim_op(
        op, tx_conn, repo, tenant, cause_event_id=None,
    )
    assert out["state_changes"] == 0
    assert out["summary"]["op"] == "relocate"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_with_vector_kind(tx_conn):
    """Vector target works through the applier path."""
    from lib.shared.types import TOPO_EMBEDDING_DIM
    tenant, a, _ = await _seed(tx_conn)
    target_vec = [0.0] * TOPO_EMBEDDING_DIM
    target_vec[5] = 1.0
    op = ClaimOp(
        op="relocate", model_id=a, reason="explicit vector",
        relocate_target={
            "kind": "vector", "value": target_vec, "alpha": 1.0,
        },
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    out = await _apply_claim_op(
        op, tx_conn, repo, tenant, cause_event_id=None,
    )
    assert out["summary"]["target_kind"] == "vector"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_relocate_with_neighborhood_kind_when_no_neighborhood_exists(
    tx_conn,
):
    """If the named neighborhood id doesn't exist, applier raises."""
    tenant, a, _ = await _seed(tx_conn)
    fake_nh = uuid4()
    op = ClaimOp(
        op="relocate", model_id=a, reason="ghost neighborhood",
        relocate_target={
            "kind": "neighborhood_id", "value": str(fake_nh), "alpha": 1.0,
        },
    )
    repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="not found"):
        await _apply_claim_op(
            op, tx_conn, repo, tenant, cause_event_id=None,
        )


# =====================================================================
# Pydantic ClaimOp shape
# =====================================================================


def test_claim_op_relocate_extra_field_rejected():
    """ClaimOp has extra='forbid'. Adding a new top-level field
    on a relocate op should raise."""
    with pytest.raises(Exception):
        ClaimOp(
            op="relocate",
            model_id=uuid4(),
            relocate_target={"kind": "model_id", "value": str(uuid4())},
            reason="ok",
            mystery_field=42,  # type: ignore[arg-type]
        )


def test_claim_op_relocate_with_changes_field_passes_pydantic():
    """`changes` is part of ClaimOp; a relocate op with `changes` set
    is structurally legal but semantically nonsense. Document the
    behavior — the validator/applier ignores `changes` for relocate."""
    op = ClaimOp(
        op="relocate",
        model_id=uuid4(),
        relocate_target={"kind": "model_id", "value": str(uuid4())},
        changes={"confidence": 0.9},  # nonsense for relocate
        reason="overlapping fields",
    )
    # Pydantic accepts; applier ignores `changes` on relocate. Document.
    assert op.op == "relocate"
    assert op.changes == {"confidence": 0.9}


def test_claim_op_insert_with_relocate_target_passes_pydantic():
    """Mirror: an insert op with a relocate_target field set is
    structurally legal but semantically nonsense. Document."""
    op = ClaimOp(
        op="insert",
        entry={"natural": "x"},
        relocate_target={"kind": "model_id", "value": str(uuid4())},
    )
    # Pydantic accepts. Applier ignores relocate_target on insert.
    assert op.op == "insert"


def test_claim_op_relocate_unknown_op_string_fails_pydantic():
    """The Literal['insert','update','archive','relocate'] should
    reject a custom op."""
    with pytest.raises(Exception):
        ClaimOp(
            op="teleport",  # type: ignore[arg-type]
            model_id=uuid4(),
        )
