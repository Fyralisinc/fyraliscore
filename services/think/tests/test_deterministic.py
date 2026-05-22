"""services/think/tests/test_deterministic.py — deterministic handlers.

Covers spec §7 deterministic paths (T1 state_change cascade-only, T2
prediction-deadline resolution, T4 background_maintenance + model_reeval)
so the inferential path tests don't have to duplicate this plumbing.

Scenarios (Wave 3-B Outstanding #4):
  * T1 state_change returns an empty diff — no LLM, no work, just
    records idempotency.
  * T2 prediction_deadline with a `commitment_outcome` falsifier that
    the commitment survived (not in contradicting_state) → +confidence.
  * T2 prediction_deadline with a `commitment_outcome` falsifier that
    triggered (commitment entered contradicting_state) → -confidence
    (× 0.3 baseline per code).
  * T4 dispatch to `background_maintenance` sub-kind with an archive
    action → produces a claim_op archive.
  * T4 dispatch to `model_reeval` sub-kind → produces a confidence nudge
    claim_op.
  * is_authoritative true/false coverage for each trigger combination.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.shared.ids import uuid7

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.deterministic import (
    deterministic_handler,
    is_authoritative,
    _trigger_ref,
)
from services.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# is_authoritative dispatch
# =====================================================================


async def test_is_authoritative_t1_state_change_true():
    t = TriggerContext(
        kind="T1", tenant_id=uuid7(), subkind="state_change",
    )
    assert is_authoritative(t) is True


async def test_is_authoritative_t1_signal_false():
    t = TriggerContext(
        kind="T1", tenant_id=uuid7(), subkind="event_arrival",
    )
    assert is_authoritative(t) is False


async def test_is_authoritative_t2_prediction_overdue_true():
    t = TriggerContext(
        kind="T2", tenant_id=uuid7(), subkind="prediction_overdue",
    )
    assert is_authoritative(t) is True


async def test_is_authoritative_t2_prediction_deadline_true():
    t = TriggerContext(
        kind="T2", tenant_id=uuid7(), subkind="prediction_deadline",
    )
    assert is_authoritative(t) is True


async def test_is_authoritative_t3_false():
    t = TriggerContext(kind="T3", tenant_id=uuid7(), subkind="anomaly")
    assert is_authoritative(t) is False


async def test_is_authoritative_t4_background_true():
    t = TriggerContext(
        kind="T4", tenant_id=uuid7(), subkind="background_maintenance",
    )
    assert is_authoritative(t) is True


async def test_is_authoritative_t4_model_reeval_false():
    # 'model_reeval' routes through deterministic via the cascade engine
    # path, but `is_authoritative` returns False because the dispatch
    # table only allows the two named subkinds.
    t = TriggerContext(
        kind="T4", tenant_id=uuid7(), subkind="model_reeval",
    )
    assert is_authoritative(t) is False


# =====================================================================
# T1 state_change handler — empty diff, no LLM
# =====================================================================


async def test_t1_state_change_returns_empty_diff(fresh_db, tenant, tenant_cleanup):
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant, subkind="state_change",
        observation_id=uuid7(),
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    # Empty diff — cascade engine handles the actual work.
    assert diff.tenant_id == tenant
    assert diff.claim_ops == []
    assert diff.act_ops == []
    assert diff.resource_ops == []
    assert diff.trigger_ref == trigger.observation_id


# =====================================================================
# T2 prediction resolution helpers
# =====================================================================


async def _insert_prediction_model(
    conn,
    tenant_id: UUID,
    *,
    confidence: float,
    falsifier: dict,
    confirmed_count: int = 0,
    contested_count: int = 0,
    contributing: list[UUID] | None = None,
) -> UUID:
    """Insert a prediction Model + its FK dependencies directly in SQL."""
    aid = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, status) "
        "VALUES ($1, $2, 'human_internal', 'x', 'active')",
        aid, tenant_id,
    )
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
        """,
        oid, tenant_id, aid, make_embedding("x"),
    )
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient, falsifier,
           confirmed_count, contested_count, contributing_models)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                $9::jsonb, $10, 0.5, 'active', $10, 1.0, $11::jsonb,
                $12, $13, $14::uuid[])
        """,
        mid, tenant_id, oid,
        json.dumps({"kind": "prediction", "text": "x will happen"}),
        "x will happen", make_embedding("x"),
        [], "[]", "{}",
        float(confidence),
        json.dumps(falsifier),
        int(confirmed_count), int(contested_count),
        contributing or [],
    )
    return mid


async def test_t2_prediction_survived_boosts_confidence(
    fresh_db, tenant, tenant_cleanup,
):
    """Falsifier.kind='commitment_outcome' with commitment NOT in
    contradicting_state → outcome=True → confidence increases."""
    from services.acts import commitments as commitments_svc
    from services.acts import goals as goals_svc
    from lib.shared.ids import uuid7

    async with fresh_db.acquire() as conn:
        # Build a commitment in 'proposed' state.
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid, tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid, tenant_id=tenant, conn=conn,
            )
        commitment_id = c.id

        # Prediction Model whose falsifier triggers if commitment reaches
        # 'closed'. Current state is 'proposed' — prediction survived.
        mid = await _insert_prediction_model(
            conn, tenant, confidence=0.6, falsifier={
                "kind": "commitment_outcome",
                "commitment_ref": str(commitment_id),
                "contradicting_state": "closed",
            },
        )

    trigger = TriggerContext(
        kind="T2", tenant_id=tenant, subkind="prediction_overdue",
        model_id=mid,
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)

    assert len(diff.claim_ops) >= 1
    # First op updates the prediction with resolution_outcome=True.
    primary = next(op for op in diff.claim_ops if op.model_id == mid)
    assert primary.op == "update"
    assert primary.changes["resolution_outcome"] is True
    assert primary.changes["confidence"] > 0.6
    # confirmed_count bumped.
    assert primary.changes["confirmed_count"] == 1


async def test_t2_prediction_triggered_drops_confidence(
    fresh_db, tenant, tenant_cleanup,
):
    """Commitment entered the contradicting_state → outcome=False
    → prediction confidence × 0.3 (delta = -0.7 * current)."""
    from services.acts import commitments as commitments_svc
    from services.acts import goals as goals_svc
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid, tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid, tenant_id=tenant, conn=conn,
            )
            await conn.execute(
                "UPDATE commitments SET state = 'closed' WHERE id = $1",
                c.id,
            )
        commitment_id = c.id

        prior_conf = 0.6
        mid = await _insert_prediction_model(
            conn, tenant, confidence=prior_conf, falsifier={
                "kind": "commitment_outcome",
                "commitment_ref": str(commitment_id),
                "contradicting_state": "closed",
            },
        )

    trigger = TriggerContext(
        kind="T2", tenant_id=tenant, subkind="prediction_deadline",
        model_id=mid,
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)

    primary = next(op for op in diff.claim_ops if op.model_id == mid)
    assert primary.changes["resolution_outcome"] is False
    # Expected new confidence: 0.6 + (-0.7 * 0.6) = 0.18 → clipped floor 0.05.
    expected_new = max(0.05, prior_conf - 0.7 * prior_conf)
    assert abs(primary.changes["confidence"] - expected_new) < 1e-6
    assert primary.changes["contested_count"] == 1


async def test_t2_prediction_no_model_returns_empty(
    fresh_db, tenant, tenant_cleanup,
):
    """T2 trigger with no model_id → empty diff, trigger recorded."""
    trigger = TriggerContext(
        kind="T2", tenant_id=tenant, subkind="prediction_overdue",
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert diff.claim_ops == []


async def test_t2_prediction_model_not_found_returns_empty(
    fresh_db, tenant, tenant_cleanup,
):
    trigger = TriggerContext(
        kind="T2", tenant_id=tenant, subkind="prediction_overdue",
        model_id=uuid7(),
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert diff.claim_ops == []


# =====================================================================
# T4 background_maintenance + model_reeval
# =====================================================================


async def test_t4_background_maintenance_archive_suggestion(
    fresh_db, tenant, tenant_cleanup,
):
    """T4 background_maintenance carrying an archive action in
    `seed_signature` → emits claim_op archive."""
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        mid = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.5, 0.01, 'active', 0.5, 1.0)
            """,
            mid, tenant, oid,
            json.dumps({"kind": "state", "text": "stale"}),
            "stale", make_embedding("x"), [], "[]", "{}",
        )

    trigger = TriggerContext(
        kind="T4", tenant_id=tenant, subkind="background_maintenance",
        seed_signature={"action": "suggest_archival", "model_id": str(mid)},
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert len(diff.claim_ops) == 1
    archive_op = diff.claim_ops[0]
    assert archive_op.op == "archive"
    assert archive_op.model_id == mid
    assert archive_op.reason == "decay"


async def test_t4_model_reeval_nudges_confidence_down(
    fresh_db, tenant, tenant_cleanup,
):
    """T4 model_reeval with cause_kind='supporting_archived'
    → -0.05 nudge."""
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        dependent = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.6, 0.5, 'active', 0.6, 1.0)
            """,
            dependent, tenant, oid,
            json.dumps({"kind": "state", "text": "dep"}),
            "dep", make_embedding("x"), [], "[]", "{}",
        )
        cause = uuid7()

    trigger = TriggerContext(
        kind="T4", tenant_id=tenant, subkind="model_reeval",
        model_id=dependent,
        seed_signature={
            "cause_model_id": str(cause),
            "cause_kind": "supporting_archived",
        },
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert len(diff.claim_ops) == 1
    update = diff.claim_ops[0]
    assert update.op == "update"
    assert update.model_id == dependent
    # 0.6 - 0.05 = 0.55 (nudge map for supporting_archived).
    assert abs(update.changes["confidence"] - 0.55) < 1e-6


async def test_t4_model_reeval_falsifier_triggered_drops_more(
    fresh_db, tenant, tenant_cleanup,
):
    """cause_kind='falsifier_triggered_upstream' → -0.15 (strongest)."""
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        dependent = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.7, 0.5, 'active', 0.7, 1.0)
            """,
            dependent, tenant, oid,
            json.dumps({"kind": "state", "text": "dep"}),
            "dep", make_embedding("x"), [], "[]", "{}",
        )

    trigger = TriggerContext(
        kind="T4", tenant_id=tenant, subkind="model_reeval",
        model_id=dependent,
        seed_signature={
            "cause_model_id": str(uuid7()),
            "cause_kind": "falsifier_triggered_upstream",
        },
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    update = diff.claim_ops[0]
    assert abs(update.changes["confidence"] - 0.55) < 1e-6  # 0.7 - 0.15.


async def test_t4_unknown_subkind_returns_empty(
    fresh_db, tenant, tenant_cleanup,
):
    """Unknown T4 subkind → empty diff (no crash)."""
    trigger = TriggerContext(
        kind="T4", tenant_id=tenant, subkind="entity_resolution_proposal",
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert diff.claim_ops == []
    assert diff.act_ops == []
    assert diff.resource_ops == []


# =====================================================================
# Fallback path — unknown trigger kind returns empty RawDiff
# =====================================================================


async def test_deterministic_unknown_kind_returns_empty(
    fresh_db, tenant, tenant_cleanup,
):
    # TriggerKind is Literal['T1','T2','T3','T4'] at the dataclass level,
    # but runtime accepts arbitrary strings; the dispatch falls through.
    trigger = TriggerContext(  # type: ignore[call-arg]
        kind="T_UNKNOWN", tenant_id=tenant,  # type: ignore[arg-type]
    )
    bundle = ContextBundle()
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, bundle, conn)
    assert diff.claim_ops == []
    assert diff.act_ops == []
