from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.domain.models.propositions import validate_proposition
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.sage.inquiry_traces import (
    TraceContext,
    reset_trace_context,
    set_trace_context,
)
from services.reasoning.think.applier import (
    _accepted_question_policy_probe_model_ids,
    apply_diff,
)
from services.reasoning.think.diff_schema import ClaimOp, RawDiff, ResourceOp
from services.reasoning.think.lifecycle_obligations import (
    maybe_inject_lifecycle_obligations,
)
from services.reasoning.think.text_embedding import deterministic_text_embedding
from services.reasoning.think.validator import validate


async def _insert_lifecycle_model(conn, tenant, observation_id, natural: str) -> UUID:
    from services.reasoning.think.tests.conftest import make_embedding

    model_id = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, 0.72, 1.0, 'active', 0.72, 1.0)
        """,
        model_id,
        tenant,
        observation_id,
        json.dumps({"kind": "state", "subject": natural, "assertion": "true"}),
        natural,
        make_embedding(natural),
    )
    return model_id


async def _insert_lifecycle_inquiry_session(conn, tenant) -> UUID:
    session_id = uuid7()
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
          id, tenant_id, signal_ref_type, signal_ref_id,
          route, status, stop_status
        ) VALUES (
          $1, $2, 'internal', NULL,
          'DEEP_INQUIRY_PATH', 'running', 'insufficient_continue'
        )
        """,
        session_id,
        tenant,
    )
    return session_id


def test_lifecycle_obligations_injects_batch_lifecycle_surfaces() -> None:
    tenant_id = uuid7()
    trigger_ref = uuid7()
    primary_obs_id = uuid7()
    model_id = uuid7()
    customer_id = uuid7()
    fragments = [
        {
            "observation_id": str(primary_obs_id),
            "text": "Forecast says launch will slip by Friday unless approval clears.",
        },
        {
            "observation_id": str(uuid7()),
            "text": (
                "Compliance capacity is down to two hours and the approval "
                "owner is unclear."
            ),
        },
        {
            "observation_id": str(uuid7()),
            "text": "Yesterday's review felt rough around the launch narrative.",
        },
        {
            "observation_id": str(uuid7()),
            "text": "The old launch-readiness memory is stale and may be replaced.",
        },
        {
            "observation_id": str(uuid7()),
            "text": (
                "Alias ambiguity: Acme and Acme Enterprise may not be the same "
                "customer."
            ),
        },
    ]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=primary_obs_id,
        observation_ids=[primary_obs_id],
        subkind="event_batch",
        seed_occurred_at=datetime(2026, 6, 16, tzinfo=timezone.utc),
        seed_entity_ids=[{"type": "customer", "id": str(customer_id)}],
        seed_signature={"batch_signal_fragments": fragments},
    )
    bundle = ContextBundle(
        models=[
            SimpleNamespace(
                id=model_id,
                status="active",
                confidence=0.91,
                natural="Enterprise-control launch is ready once approval clears.",
                scope_actors=[],
                scope_entities=[{"type": "customer", "id": str(customer_id)}],
            )
        ]
    )
    raw = RawDiff(trigger_ref=trigger_ref, tenant_id=tenant_id)

    out = maybe_inject_lifecycle_obligations(raw, trigger, bundle)

    inserted_entries = [
        op.entry
        for op in out.claim_ops
        if op.op == "insert" and isinstance(op.entry, dict)
    ]
    prediction_entries = [
        entry
        for entry in inserted_entries
        if entry.get("proposition", {}).get("kind") == "prediction"
    ]
    assert len(prediction_entries) == 1
    assert prediction_entries[0]["resolution_criteria"]["source"] == (
        "lifecycle_obligation"
    )
    assert "2026-06-19" in prediction_entries[0]["evaluate_at"]
    assert prediction_entries[0]["supporting_event_ids"] == [
        fragments[0]["observation_id"]
    ]
    assert len(out.resource_ops) == 1
    assert out.resource_ops[0].op == "create"
    assert out.resource_ops[0].payload["kind"] == "capacity"
    assert out.resource_ops[0].payload["metadata"]["source"] == (
        "lifecycle_obligation"
    )
    assert out.resource_ops[0].payload["metadata"]["evidence_event_ids"] == [
        fragments[1]["observation_id"]
    ]
    assert any(
        "question_policy" in set(entry.get("domain_tags") or [])
        and "lifecycle_obligation" in set(entry.get("domain_tags") or [])
        for entry in inserted_entries
    )
    sidecar_entries = [
        entry for entry in inserted_entries
        if "memory_quality" in set(entry.get("domain_tags") or [])
    ]
    assert len(sidecar_entries) == 1
    assert sidecar_entries[0]["supporting_event_ids"] == [
        fragments[2]["observation_id"]
    ]
    assert sidecar_entries[0]["natural"] == fragments[2]["text"]
    assert {op.question_type for op in out.open_question_ops} == {
        "temporal_status",
        "contradiction_check",
    }
    assert all(op.model_id == model_id for op in out.open_question_ops)
    assert "lifecycle_obligations: injected" in (out.reasoning_trace or "")

    for entry in inserted_entries:
        validate_proposition(entry["proposition"])
        model_entry = {
            **entry,
            "tenant_id": tenant_id,
            "embedding": deterministic_text_embedding(entry["natural"]),
        }
        ModelCreate.model_validate(model_entry)


def test_lifecycle_obligations_ignores_plain_batches() -> None:
    tenant_id = uuid7()
    raw = RawDiff(trigger_ref=uuid7(), tenant_id=tenant_id)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=uuid7(),
        observation_ids=[uuid7()],
        subkind="event_batch",
        seed_signature={
            "batch_signal_fragments": [
                {"observation_id": str(uuid7()), "text": "Normal launch update."}
            ]
        },
    )

    out = maybe_inject_lifecycle_obligations(raw, trigger, ContextBundle())

    assert out == raw


def test_lifecycle_obligations_avoids_duplicate_core_surfaces() -> None:
    tenant_id = uuid7()
    obs_id = uuid7()
    existing_prediction = ClaimOp(
        op="insert",
        entry={
            "born_from_event_id": str(obs_id),
            "proposition": {"kind": "prediction", "expected": "Existing forecast"},
            "natural": "Existing forecast",
            "domain_tags": ["prediction"],
        },
    )
    existing_question_policy = ClaimOp(
        op="insert",
        entry={
            "born_from_event_id": str(obs_id),
            "proposition": {
                "kind": "belief",
                "claim_role": "capability",
                "subject": "question policy",
                "assessment": "Existing question policy marker.",
            },
            "natural": "Existing question-policy marker.",
            "domain_tags": ["question_policy", "lifecycle_obligation"],
        },
    )
    existing_resource = ResourceOp(
        op="create",
        payload={
            "kind": "capacity",
            "identity": "existing",
            "current_value": {},
        },
    )
    raw = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant_id,
        claim_ops=[existing_prediction, existing_question_policy],
        resource_ops=[existing_resource],
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        observation_ids=[obs_id],
        subkind="event_batch",
        seed_signature={
            "batch_signal_fragments": [
                {
                    "observation_id": str(obs_id),
                    "text": (
                        "Forecast says launch will slip by Friday. Capacity is "
                        "down to two hours and the owner is unclear."
                    ),
                }
            ]
        },
    )

    out = maybe_inject_lifecycle_obligations(raw, trigger, ContextBundle())

    prediction_count = sum(
        1
        for op in out.claim_ops
        if op.op == "insert"
        and isinstance(op.entry, dict)
        and op.entry.get("proposition", {}).get("kind") == "prediction"
    )
    question_policy_count = sum(
        1
        for op in out.claim_ops
        if op.op == "insert"
        and isinstance(op.entry, dict)
        and "question_policy" in set(op.entry.get("domain_tags") or [])
    )
    assert prediction_count == 1
    assert question_policy_count == 1
    assert len(out.resource_ops) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lifecycle_obligations_survive_validate_apply_and_feedback(
    fresh_db,
    tenant,
    tenant_cleanup,
) -> None:
    from services.reasoning.think.tests.conftest import _insert_observation

    event_time = datetime(2026, 6, 16, tzinfo=timezone.utc)
    fragment_texts = [
        "Forecast says launch will slip by Friday unless approval clears.",
        (
            "Compliance capacity is down to two hours and the approval owner "
            "is unclear."
        ),
        "Yesterday's review felt rough around the launch narrative.",
        "The old launch-readiness memory is stale and may be replaced.",
        (
            "Alias ambiguity: Acme and Acme Enterprise may not be the same "
            "customer."
        ),
    ]

    async with fresh_db.acquire() as conn:
        event_ids = [
            await _insert_observation(
                conn,
                tenant,
                content_text=text,
                occurred_at=event_time,
                external_id=f"lifecycle-obligation-{index}-{uuid7()}",
            )
            for index, text in enumerate(fragment_texts)
        ]
        anchor_model = await _insert_lifecycle_model(
            conn,
            tenant,
            event_ids[0],
            "Enterprise-control launch is ready once approval clears.",
        )
        scope_entity = {"type": "customer", "id": str(uuid7())}
        await conn.execute(
            "UPDATE models SET scope_entities = $2::jsonb WHERE id = $1",
            anchor_model,
            json.dumps([scope_entity]),
        )
        session_id = await _insert_lifecycle_inquiry_session(conn, tenant)
        trigger = TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant,
            observation_id=event_ids[0],
            observation_ids=event_ids,
            seed_occurred_at=event_time,
            seed_signature={
                "batch_signal_fragments": [
                    {"observation_id": str(event_id), "text": text}
                    for event_id, text in zip(event_ids, fragment_texts, strict=True)
                ]
            },
        )
        bundle = ContextBundle(
            models=[
                SimpleNamespace(
                    id=anchor_model,
                    status="active",
                    confidence=0.9,
                    natural="Enterprise-control launch is ready once approval clears.",
                    scope_actors=[],
                    scope_entities=[scope_entity],
                )
            ]
        )
        raw = maybe_inject_lifecycle_obligations(
            RawDiff(trigger_ref=uuid7(), tenant_id=tenant),
            trigger,
            bundle,
        )
        retrieval_result = RetrievalResult(
            trigger=trigger,
            models=[],
            observations=[],
            acts={"goals": [], "commitments": [], "decisions": []},
            resources=[],
            pathway_results=[],
            notes={},
            model_scores={},
        )
        validated = await validate(
            raw,
            retrieval_result,
            conn,
            allowed_region=None,
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        ctx = TraceContext(
            tenant_id=tenant,
            inquiry_session_id=session_id,
            pool=fresh_db,
            conn=conn,
            metadata={
                "question_primitives": ["DEPENDENCY"],
                "signal_type": "T1",
                "trigger_kind": "T1:event_batch",
                "entities": ["customer:acme"],
            },
        )
        token = set_trace_context(ctx)
        try:
            async with conn.transaction():
                result = await apply_diff(
                    validated,
                    conn,
                    trigger_kind="T1:event_batch",
                    trigger_cause_event_id=event_ids[0],
                    models_repo=repo,
                )
        finally:
            reset_trace_context(token)

        prediction_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_predictions
            WHERE tenant_id = $1
            """,
            tenant,
        )
        open_question_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_open_questions
            WHERE tenant_id = $1
              AND model_id = $2
              AND question_type = ANY($3::text[])
            """,
            tenant,
            anchor_model,
            ["temporal_status", "contradiction_check"],
        )
        lifecycle_review_truth_count = await conn.fetchval(
            """
            SELECT count(*) FROM models
            WHERE tenant_id=$1
              AND proposition->>'subject'='lifecycle review evidence'
            """,
            tenant,
        )
        lifecycle_sidecar = await conn.fetchrow(
            """
            SELECT model_id,source_event_id,reading_kind,detail
            FROM model_signal_readings
            WHERE tenant_id=$1 AND source_event_id=$2
            ORDER BY observed_at DESC LIMIT 1
            """,
            tenant,
            event_ids[2],
        )
        lifecycle_feedback = await conn.fetchrow(
            """
            SELECT model_id,cause_id,cause_type,changed_fields
            FROM audit_events
            WHERE tenant_id=$1 AND cause_id=$2
            ORDER BY occurred_at DESC LIMIT 1
            """,
            tenant,
            event_ids[2],
        )

    aggregation = result["memory_aggregation"]
    claim_summaries = result["claim_ops"]
    assert validated.dropped_op_count == 0
    assert len(result["resource_ops"]) == 1
    assert result["resource_ops"][0]["op"] == "create_resource"
    assert prediction_count >= 1
    assert open_question_count == 2
    assert aggregation["evidence_attachments"] == 1
    assert lifecycle_review_truth_count == 0
    assert lifecycle_sidecar is not None
    assert lifecycle_sidecar["model_id"] == anchor_model
    assert lifecycle_sidecar["source_event_id"] == event_ids[2]
    assert lifecycle_sidecar["reading_kind"] == "observe"
    assert lifecycle_feedback is not None
    assert lifecycle_feedback["model_id"] == anchor_model
    assert lifecycle_feedback["cause_id"] == event_ids[2]
    assert lifecycle_feedback["cause_type"] == "field_update"
    assert "signal_readings" in lifecycle_feedback["changed_fields"]
    assert any(summary.get("model_prediction_id") for summary in claim_summaries)


def test_question_policy_feedback_accepts_lifecycle_obligation_source() -> None:
    model_id = uuid7()

    out = _accepted_question_policy_probe_model_ids(
        {
            "claim_ops": [
                {
                    "model_id": str(model_id),
                    "domain_tags": ["question_policy", "lifecycle_obligation"],
                },
                {
                    "model_id": str(uuid7()),
                    "domain_tags": ["question_policy"],
                },
            ]
        }
    )

    assert out == [model_id]
