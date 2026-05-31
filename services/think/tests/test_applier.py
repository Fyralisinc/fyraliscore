"""services/think/tests/test_applier.py — applier behavior + idempotency.

Unit-ish tests over apply_diff. Many Think end-to-end concerns (region
lock, cascade, anomalies) live in test_end_to_end.py.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.shared.ids import uuid7

from services.models.repo import ModelsRepo
from services.think.applier import (
    AlreadyAppliedError, apply_diff, hash_diff,
)
from services.think.diff_schema import (
    ActOp, ClaimOp, EdgeOp, ResourceOp, ValidatedDiff,
)
from services.think.text_embedding import deterministic_text_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_applier_model(conn, tenant, observation_id, natural: str):
    from services.think.tests.conftest import make_embedding

    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, 0.6, 1.0, 'active', 0.6, 1.0)
        """,
        mid,
        tenant,
        observation_id,
        json.dumps({"kind": "state", "subject": natural, "assertion": "true"}),
        natural,
        make_embedding(natural),
    )
    return mid


async def test_apply_diff_acquires_tenant_model_write_lock(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    """Apply serializes the short model-write phase per tenant."""
    from services.think import region_locks

    calls = []
    original = region_locks.acquire_region_lock

    async def spy_acquire_region_lock(conn, tenant_id, entity_ids):
        calls.append(list(entity_ids))
        return await original(conn, tenant_id, entity_ids)

    monkeypatch.setattr(
        region_locks,
        "acquire_region_lock",
        spy_acquire_region_lock,
    )

    async with fresh_db.acquire() as conn:
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
        )
        async with conn.transaction():
            await apply_diff(diff, conn, trigger_kind="T1")

    assert calls[0] == [("tenant_model_write", str(tenant))]


async def test_reconciler_db_error_does_not_poison_apply_transaction(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    """Reconciler is best-effort, so its DB failures use a savepoint."""
    from services.think import reconciler

    async def failing_inner(*args, **kwargs):
        conn = args[1]
        await conn.execute("SELECT 1 / 0")

    monkeypatch.setattr(reconciler, "_reconcile_inner", failing_inner)

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            result = await reconciler.reconcile_claim_op(
                ClaimOp(op="insert", entry={"natural": "duplicate-ish claim"}),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )
            assert result.decision == "skipped"
            assert await conn.fetchval("SELECT 1") == 1


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


async def test_apply_dedupes_split_situation_members_after_reconcile(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Duplicate atomic splits can reconcile to the same Model twice."""
    from services.think.tests.conftest import _insert_observation

    natural = (
        "Atlas renewal is at risk and Atlas renewal is at risk because "
        "audit evidence is missing"
    )
    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text=natural)
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "concern",
                            "about": "Atlas renewal",
                            "nature": natural,
                            "raised_by": "test",
                        },
                        "natural": natural,
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                        "supporting_event_ids": [str(oid)],
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT proposition
            FROM models
            WHERE tenant_id = $1
              AND claim_role = 'situation'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )

    assert result["split_summary"]["compound_inputs"] == 1
    assert row is not None
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    member_ids = proposition["member_model_ids"]
    assert len(member_ids) == len(set(member_ids))
    assert len(member_ids) >= 2


async def test_apply_adds_required_situation_compositional_defaults(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Live/provider or splitter situations missing DB-required fields
    should be normalized before insert."""
    from services.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A composite execution pressure emerged.",
        )
        member_a = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Atlas operating pressure is visible.",
        )
        member_b = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Atlas delivery pressure is visible.",
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "situation",
                            "situation": "Atlas operating pressure",
                            "summary": "Atlas has linked operating pressure.",
                            "member_model_ids": [str(member_a), str(member_b)],
                            "relationship_summary": (
                                "Operating signals are linked."
                            ),
                            "status": "forming",
                        },
                        "natural": "Atlas operating pressure is forming.",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT proposition
            FROM models
            WHERE tenant_id = $1
              AND claim_role = 'situation'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )

    assert result["claim_ops"][0]["op"] == "insert"
    assert row is not None
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert proposition["pressure_type"] == "execution"
    assert proposition["shared_mechanism"]
    assert proposition["judgment_change"]
    assert proposition["open_falsifier"]


async def test_apply_claim_insert_strips_llm_invented_model_id(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Live providers sometimes put a generated model_id inside insert.entry."""
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
            oid,
            tenant,
            make_embedding("x"),
        )
        invented = uuid4()
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "model_id": str(invented),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "ships",
                        },
                        "natural": "x ships",
                        "embedding": make_embedding("x ships"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                ),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )

        inserted_model_id = UUID(result["claim_ops"][0]["model_id"])
        assert inserted_model_id != invented


async def test_apply_resolves_same_diff_invented_model_id_for_edges(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """If the LLM uses entry.model_id as a same-diff placeholder, resolve it."""
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
            oid,
            tenant,
            make_embedding("x"),
        )
        existing = await _insert_applier_model(conn, tenant, oid, "existing")
        invented = uuid4()
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "model_id": str(invented),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "ships",
                        },
                        "natural": "x ships",
                        "embedding": make_embedding("x ships"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                ),
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=invented,
                    target_model_id=existing,
                    edge_kind="supports",
                    weight=0.5,
                ),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )

        inserted_model_id = UUID(result["claim_ops"][0]["model_id"])
        row = await conn.fetchrow(
            """
            SELECT source_model_id, target_model_id
            FROM model_edges
            WHERE tenant_id = $1
              AND edge_kind = 'supports'
            """,
            tenant,
        )
        assert row is not None
        assert row["source_model_id"] == inserted_model_id
        assert row["target_model_id"] == existing


async def test_apply_resolves_same_diff_insert_refs_for_edges_and_acts(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        new_event = uuid7()
        existing_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (new_event, tenant, "new done event", make_embedding("new done event")),
                (
                    existing_event,
                    tenant,
                    "existing state",
                    make_embedding("existing state"),
                ),
            ],
        )
        existing_model = await _insert_applier_model(
            conn,
            tenant,
            existing_event,
            "existing state",
        )
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id,
            tenant,
        )
        commitment_id = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at, is_maintenance)
            VALUES ($1, $2, 'x', 'active', $3, $4, now(), TRUE)
            """,
            commitment_id,
            tenant,
            actor_id,
            existing_event,
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "done",
                        },
                        "natural": "x is done",
                        "embedding": make_embedding("x is done"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.82,
                        "confidence_at_assertion": 0.82,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "x is reopened or marked incomplete",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=new_event,
                    target_model_id=existing_model,
                    edge_kind="superseded_by",
                    confidence=0.8,
                    explanation="The new done state supersedes the older state.",
                )
            ],
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=new_event,
                    entity={
                        "id": str(commitment_id),
                        "new_state": "doneunverified",
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
                models_repo=repo,
            )
        inserted_model = UUID(result["claim_ops"][0]["model_id"])
        edge = await conn.fetchrow(
            """
            SELECT source_model_id, target_model_id FROM model_edges
            WHERE tenant_id=$1 AND edge_kind='superseded_by'
            """,
            tenant,
        )
        commitment = await conn.fetchrow(
            "SELECT state, last_confidence_basis FROM commitments WHERE id=$1",
            commitment_id,
        )

    assert edge["source_model_id"] == existing_model
    assert edge["target_model_id"] == inserted_model
    assert commitment["state"] == "doneunverified"
    assert commitment["last_confidence_basis"] == inserted_model


async def test_reconciler_folds_llm_insert_without_embedding_into_existing_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Strict live-LLM inserts omit embeddings; reconciliation still runs."""
    from services.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        old_event = uuid7()
        new_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (
                    old_event,
                    tenant,
                    "renewal risk is rising",
                    make_embedding("renewal risk is rising"),
                ),
                (
                    new_event,
                    tenant,
                    "renewal risk is rising",
                    make_embedding("renewal risk is rising"),
                ),
            ],
        )
        natural = "Atlas renewal risk is rising because audit evidence is late."
        existing_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                    '{}'::jsonb, 0.6, 1.0, 'active', 0.6, 1.0)
            """,
            existing_model,
            tenant,
            old_event,
            json.dumps({
                "kind": "concern",
                "about": "Atlas renewal",
                "nature": "audit evidence is late",
                "raised_by": "customer",
            }),
            natural,
            deterministic_text_embedding(natural),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "concern",
                            "about": "Atlas renewal",
                            "nature": "audit evidence is late",
                            "raised_by": "customer",
                        },
                        "natural": natural,
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.78,
                        "confidence_at_assertion": 0.78,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "Audit evidence is delivered and accepted by procurement",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
                models_repo=repo,
            )

        assert result["claim_ops"][0]["op"] == "update"
        assert result["claim_ops"][0]["model_id"] == str(existing_model)
        assert result["claim_ops"][0]["reconcile_decision"] == "auto_merge"
        assert result["reconcile_summary"]["auto_merge"] == 1
        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        row = await conn.fetchrow(
            """
            SELECT confidence, supporting_event_ids, signal_readings,
                   confirmed_count, last_confirmed_at
            FROM models WHERE id = $1
            """,
            existing_model,
        )

    assert model_count == 1
    assert float(row["confidence"]) == 0.78
    assert new_event in row["supporting_event_ids"]
    assert row["confirmed_count"] == 1
    assert row["last_confirmed_at"] is not None
    readings = row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "confirm"
    assert readings[-1]["source_event_id"] == str(new_event)


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


async def test_apply_drops_domain_invalid_act_op_without_rolling_back_claims(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """
    Late-discovered domain-invalid act_ops are dropped like validator
    partial failures. Valid claim_ops from the same signal still commit.
    """
    from services.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        actor_id = uuid7()
        commitment_id = uuid7()
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
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'owner', 'active')",
            actor_id,
            tenant,
        )
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at, is_maintenance)
            VALUES ($1, $2, 'ship reliability work', 'active', $3, $4, now(), TRUE)
            """,
            commitment_id,
            tenant,
            actor_id,
            oid,
        )
        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "state",
                        "subject": "reliability",
                        "assertion": "new risk surfaced",
                    },
                    "natural": "A new reliability risk surfaced.",
                    "embedding": make_embedding("reliability risk"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.7,
                    "confidence_at_assertion": 0.7,
                }),
            ],
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=oid,
                    entity={
                        "id": str(commitment_id),
                        "new_state": "blocked",
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                "T1",
                oid,
                models_repo=repo,
            )

        model_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        commitment_state = await conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1",
            commitment_id,
        )
        trigger_outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )

    assert model_count == 1
    assert commitment_state == "active"
    assert trigger_outcome == "success"
    assert result["apply_dropped_op_count"] == 1
    assert result["act_ops"][0]["op"] == "skip"
    assert result["act_ops"][0]["reason"] == "illegal_transition"


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


async def test_apply_edge_ops_add_and_retire(fresh_db, tenant, tenant_cleanup):
    from services.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A contradicts B in the operating review",
            external_id=f"edge-op-add-retire-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "A")
        b = await _insert_applier_model(conn, tenant, oid, "B")
        add_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    weight=0.8,
                    confidence=0.9,
                    evidence_event_ids=[oid],
                    explanation="A and B cannot both be true.",
                )
            ],
        )
        async with conn.transaction():
            add_result = await apply_diff(add_diff, conn, "T1", oid)

        assert len(add_result["edge_ops"]) == 1
        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, confidence,
                   evidence_event_ids, status, review_status
            FROM model_edges
            WHERE tenant_id = $1 AND edge_kind = 'contradicts'
            ORDER BY source_model_id::text
            """,
            tenant,
        )
        assert len(rows) == 2
        assert {
            (r["source_model_id"], r["target_model_id"]) for r in rows
        } == {(a, b), (b, a)}
        assert {r["confidence"] for r in rows} == {0.9}
        assert all(oid in r["evidence_event_ids"] for r in rows)

        retire_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="retire",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    reason="operator resolved the contradiction",
                )
            ],
        )
        async with conn.transaction():
            retire_result = await apply_diff(retire_diff, conn, "T1", oid)

        assert retire_result["edge_ops"][0]["retired_edges"] == 2
        statuses = await conn.fetch(
            """
            SELECT status, review_status, status_reason
            FROM model_edges
            WHERE tenant_id = $1 AND edge_kind = 'contradicts'
            """,
            tenant,
        )
        assert {(r["status"], r["review_status"]) for r in statuses} == {
            ("inert", "retired")
        }
        assert {r["status_reason"] for r in statuses} == {
            "operator resolved the contradiction"
        }


async def test_apply_edge_cycle_is_dropped_not_transaction_fatal(
    fresh_db, tenant, tenant_cleanup,
):
    from services.models.edges_repo import EdgesRepo
    from services.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A supports B, but reverse support is invalid",
            external_id=f"edge-cycle-drop-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "A")
        b = await _insert_applier_model(conn, tenant, oid, "B")
        repo = EdgesRepo()
        async with conn.transaction():
            await repo.link(
                conn,
                source=a,
                target=b,
                kind="supports",
                tenant_id=tenant,
                detected_by="think_edge_op",
                confidence=0.8,
            )

        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=b,
                    target_model_id=a,
                    edge_kind="supports",
                    confidence=0.8,
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )
        edge_count = await conn.fetchval(
            "SELECT COUNT(*) FROM model_edges WHERE tenant_id = $1",
            tenant,
        )

    assert outcome == "success"
    assert edge_count == 1
    assert result["apply_dropped_op_count"] == 1
    assert result["edge_ops"][0]["op"] == "skip"
    assert result["edge_ops"][0]["reason"] == "cycle_prevention"
