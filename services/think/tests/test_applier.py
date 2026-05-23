"""services/think/tests/test_applier.py — applier behavior + idempotency.

Unit-ish tests over apply_diff. Many Think end-to-end concerns (region
lock, cascade, anomalies) live in test_end_to_end.py.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.ids import uuid7

from services.models.repo import ModelsRepo
from services.think.applier import (
    AlreadyAppliedError, apply_diff, hash_diff,
)
from services.think.diff_schema import (
    ClaimOp, ValidatedDiff,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_apply_single_claim_insert(fresh_db, tenant, tenant_cleanup):
    """Happy path: a single claim_op insert creates a Model + state_change."""
    from services.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        # Seed an observation for born_from_event_id.
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "ships"},
                    "natural": "x ships",
                    "embedding": make_embedding("x ships"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )
        assert len(result["claim_ops"]) == 1
        assert result["applied_model_ids"]
        # applied_triggers row present.
        outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            diff.trigger_ref,
        )
        assert outcome == "success"


async def test_apply_idempotency_second_apply_raises(fresh_db, tenant, tenant_cleanup):
    from services.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
                    "natural": "x",
                    "embedding": make_embedding("x"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.5,
                    "confidence_at_assertion": 0.5,
                }),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            await apply_diff(diff, conn, "T1", oid, models_repo=repo)

        # Second apply same trigger.
        async with conn.transaction():
            with pytest.raises(AlreadyAppliedError):
                await apply_diff(diff, conn, "T1", oid, models_repo=repo)


async def test_apply_partial_failure_rolls_back_all_ops(fresh_db, tenant, tenant_cleanup):
    """
    An op mid-apply raising rolls back the whole transaction — no
    partial state, and the applied_triggers row is rolled back with it.
    """
    from services.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
                    "natural": "x",
                    "embedding": make_embedding("x"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.5,
                    "confidence_at_assertion": 0.5,
                }),
                # Second op with invalid archive target → will fail at apply.
                ClaimOp(op="archive", model_id=uuid4(), reason="decay"),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        with pytest.raises(Exception):
            async with conn.transaction():
                await apply_diff(
                    diff, conn, "T1", oid, models_repo=repo
                )
        # After rollback: no applied_triggers row.
        existing = await conn.fetchval(
            "SELECT COUNT(*) FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )
        assert existing == 0
        # No models inserted either.
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        assert n == 0


async def test_hash_diff_is_stable():
    a = ValidatedDiff(
        trigger_ref=uuid7(),
        tenant_id=uuid7(),
        claim_ops=[
            ClaimOp(op="archive", model_id=uuid7(), reason="decay"),
        ],
    )
    h1 = hash_diff(a)
    h2 = hash_diff(a)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


async def test_hash_diff_differs_by_content():
    m1 = uuid7()
    m2 = uuid7()
    a = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=uuid7(),
        claim_ops=[ClaimOp(op="archive", model_id=m1, reason="decay")],
    )
    b = ValidatedDiff(
        trigger_ref=a.trigger_ref, tenant_id=a.tenant_id,
        claim_ops=[ClaimOp(op="archive", model_id=m2, reason="decay")],
    )
    assert hash_diff(a) != hash_diff(b)
