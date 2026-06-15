from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.obligations import open_obligation, sweep_due_obligations
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import ActOp, ClaimOp, RawDiff, ValidatedDiff
from services.reasoning.think.validator import validate


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _retrieval_result(tenant_id):
    return RetrievalResult(
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        models=[],
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        pathway_results=[],
        notes={},
        model_scores={},
    )


def _obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        return json.loads(value)
    return value


async def test_due_obligation_sweep_emits_think_trigger(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    object_id = uuid7()
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            obligation_id = await open_obligation(
                conn,
                tenant_id=tenant,
                kind="policy_digest",
                object_kind="actor",
                object_id=object_id,
                due_at=datetime.now(timezone.utc) - timedelta(minutes=1),
                trigger_kind="T4",
                trigger_subkind="policy_digest",
                payload={"reason": "feedback_window_closed"},
            )
            report = await sweep_due_obligations(conn, tenant_id=tenant)

        assert report.claimed == 1
        assert report.fired == 1
        row = await conn.fetchrow(
            """
            SELECT status, fires, last_trigger_id
            FROM think_obligations
            WHERE id = $1
            """,
            obligation_id,
        )
        trigger = await conn.fetchrow(
            """
            SELECT trigger_kind, trigger_subkind, payload
            FROM think_trigger_queue
            WHERE id = $1
            """,
            report.trigger_ids[0],
        )

    assert row["status"] == "fired"
    assert row["fires"] == 1
    assert row["last_trigger_id"] == report.trigger_ids[0]
    assert trigger["trigger_kind"] == "T4"
    assert trigger["trigger_subkind"] == "policy_digest"
    assert _obj(trigger["payload"])["obligation_id"] == str(obligation_id)


async def test_validator_records_durable_drop_feedback(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        born_event = await _insert_observation(conn, tenant, content_text="loop signal")
        existing_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, activation, status, confidence_at_assertion,
              activation_coefficient
            )
            VALUES (
              $1, $2, $3, '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
              'existing state', $4, '{}'::uuid[], '[]'::jsonb, '{}'::jsonb,
              0.5, 1.0, 'active', 0.5, 1.0
            )
            """,
            existing_model,
            tenant,
            born_event,
            make_embedding("existing state"),
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(born_event),
                        "proposition": {
                            "kind": "prediction",
                            "expected": "Loop validates bad prediction feedback",
                            "resolution": "A resolution exists",
                        },
                        "natural": "Loop validates bad prediction feedback.",
                        "embedding": make_embedding("bad prediction"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.85,
                        "confidence_at_assertion": 0.85,
                    },
                ),
                ClaimOp(
                    op="update",
                    model_id=existing_model,
                    changes={"confidence": 0.45},
                ),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)
        stat = await conn.fetchrow(
            """
            SELECT dropped_count, last_payload
            FROM think_feedback_stats
            WHERE tenant_id = $1
              AND surface = 'think_validation'
              AND op_type = 'claim'
              AND op_kind = 'insert'
              AND reason = 'inadequate_falsifier'
            """,
            tenant,
        )

    assert validated.dropped_op_count == 1
    assert stat["dropped_count"] == 1
    assert "falsifier" in _obj(stat["last_payload"])["error_message"].lower()


async def test_apply_late_drop_records_durable_feedback(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    diff = ValidatedDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant,
        act_ops=[
            ActOp(
                op="create_goal",
                confidence_basis=uuid7(),
                entity={"title": "Should be skipped before apply"},
            )
        ],
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            result = await apply_diff(diff, conn, trigger_kind="T1")
        stat = await conn.fetchrow(
            """
            SELECT dropped_count, last_payload
            FROM think_feedback_stats
            WHERE tenant_id = $1
              AND surface = 'think_apply'
              AND op_type = 'act'
              AND op_kind = 'create_goal'
              AND reason = 'missing_confidence_basis'
            """,
            tenant,
        )

    assert result["apply_dropped_op_count"] == 1
    assert stat["dropped_count"] == 1
    assert "confidence_basis" in _obj(stat["last_payload"])["message"]


async def test_new_predictions_validate_into_claim_ops(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    rr = _retrieval_result(tenant)
    evaluate_at = "2026-06-25T10:00:00+00:00"
    async with fresh_db.acquire() as conn:
        born_event = await _insert_observation(conn, tenant, content_text="forecast")
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            new_predictions=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(born_event),
                        "proposition": {
                            "kind": "prediction",
                            "expected": "Atlas renewal probability recovers",
                            "resolution": "Renewal probability improves",
                        },
                        "natural": "Atlas renewal probability recovers.",
                        "embedding": make_embedding("Atlas renewal probability recovers."),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {
                            "valid_from": "2026-06-11T00:00:00+00:00",
                            "valid_until": evaluate_at,
                        },
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                        "evaluate_at": evaluate_at,
                    },
                )
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)
        async with conn.transaction():
            result = await apply_diff(
                validated,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=born_event,
            )
        model_id = result["applied_model_ids"][0]
        prediction_id = await conn.fetchval(
            """
            SELECT id
            FROM model_predictions
            WHERE tenant_id = $1 AND model_id = $2
            """,
            tenant,
            model_id,
        )

    assert len(validated.claim_ops) == 1
    assert validated.new_predictions == []
    assert prediction_id is not None
