"""Think qualification tests.

These tests sit above the narrower unit/integration tests in this
package. They protect the production contract we care about:

* prompts carry the IDs and constraints the LLM needs to avoid
  hallucinating substrate mutations;
* worker trigger hydration preserves rich queue payloads;
* scripted Think runs can create useful Models end-to-end;
* mixed-quality LLM diffs are partially accepted with observability;
* duplicate claims are reconciled through the real Think path.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.models.repo import ModelsRepo
from services.observations.events import notify_scope
from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.prompt import build_prompt
from services.think.reason import think
from services.think.tests.conftest import (
    ScriptedProvider,
    _insert_actor,
    _insert_observation,
    make_embedding,
)
from services.think.worker import _populate_seed_fields


pytestmark = [pytest.mark.integration]


def _state_insert_op(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    actor_id: UUID,
    natural: str,
    confidence: float = 0.55,
    embedding: list[float] | None = None,
) -> dict:
    entry = {
        "tenant_id": str(tenant_id),
        "born_from_event_id": str(observation_id),
        "proposition": {
            "kind": "state",
            "subject": str(actor_id),
            "assertion": natural,
        },
        "natural": natural,
        "confidence": confidence,
        "confidence_at_assertion": confidence,
        "scope_actors": [str(actor_id)],
        "scope_entities": [],
        "scope_temporal": {
            "valid_from": datetime.now(timezone.utc).isoformat(),
            "valid_until": None,
        },
        "falsifier": None,
    }
    if embedding is not None:
        entry["embedding"] = embedding
    return {"op": "insert", "entry": entry}


def _diff(
    *,
    trigger_id: UUID,
    tenant_id: UUID,
    claim_ops: list[dict] | None = None,
    act_ops: list[dict] | None = None,
    resource_ops: list[dict] | None = None,
) -> str:
    return json.dumps(
        {
            "trigger_ref": str(trigger_id),
            "tenant_id": str(tenant_id),
            "claim_ops": claim_ops or [],
            "act_ops": act_ops or [],
            "resource_ops": resource_ops or [],
            "new_predictions": [],
            "reasoning_trace": "scripted qualification diff",
        }
    )


async def _seed_actor_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    content_text: str = "Alice started retention analysis for Q3 customers.",
) -> tuple[UUID, UUID]:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, 'think qualification tenant', FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
        )
        actor_id = await _insert_actor(conn, tenant_id, "Alice")
        obs_id = await _insert_observation(
            conn,
            tenant_id,
            actor_id=actor_id,
            content_text=content_text,
            source_channel="slack:message",
            external_id=f"qual-{uuid7()}",
        )
    return actor_id, obs_id


def test_prompt_contract_surfaces_ids_scope_and_topology_context() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    commitment_id = uuid7()
    neighborhood_id = uuid7()

    trigger = TriggerContext(
        kind="T6",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text="A new customer-risk neighborhood emerged.",
        topology_event_kind="emergence",
        neighborhood_id=neighborhood_id,
        member_model_ids=[model_id],
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(
                id=obs_id,
                actor_id=actor_id,
                trust_tier="authoritative",
                source_channel="slack:message",
                occurred_at=datetime.now(timezone.utc),
                content_text="Alice: Globex renewal risk is rising.",
            )
        ],
        models=[
            SimpleNamespace(
                id=model_id,
                proposition_kind="concern",
                confidence=0.82,
                activation=0.9,
                falsifier={"kind": "observation_pattern"},
                status="active",
                scope_actors=[actor_id],
                scope_entities=[
                    {"type": "commitment", "id": str(commitment_id)}
                ],
                natural="Globex renewal risk is rising.",
            )
        ],
        acts_summary={
            "goals": [],
            "commitments": [
                SimpleNamespace(
                    id=commitment_id,
                    state="active",
                    owner_id=actor_id,
                    due_date=None,
                    title="Retain Globex renewal",
                )
            ],
            "decisions": [],
        },
        topology_context={
            "seed_neighborhood_id": str(neighborhood_id),
            "neighborhoods": [
                {
                    "id": str(neighborhood_id),
                    "named_signature": "customer renewal risk",
                    "density": 0.73,
                    "member_count": 4,
                    "matched_in_bundle": 1,
                    "is_seed": True,
                }
            ],
            "recent_phase_events": [
                {
                    "kind": "emergence",
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                    "named_signature": "customer renewal risk",
                    "magnitude": 0.81,
                }
            ],
        },
    )

    pair = build_prompt(
        trigger,
        bundle,
        triggering_content="Topology event: emergence",
        reason_for_trigger="neighborhood phase shift",
    )

    assert "The eleven kinds above are the ONLY valid `kind` values" in pair.system
    assert str(obs_id) in pair.user
    assert str(actor_id) in pair.user
    assert str(commitment_id) in pair.user
    assert str(neighborhood_id) in pair.user
    assert "<actors_in_context>" in pair.user
    assert "<topology_context>" in pair.user
    assert "This is a T6 trigger" in pair.user
    assert "Do NOT invent member Model ids" in pair.user


def test_worker_payload_hydration_preserves_seed_and_topology_fields() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    event_id = uuid7()
    neighborhood_id = uuid7()
    member_id = uuid7()
    trigger = TriggerContext(kind="T6", tenant_id=tenant_id)

    _populate_seed_fields(
        trigger,
        {
            "seed_natural_text": "neighborhood emerged around churn risk",
            "seed_entity_ids": [{"type": "customer", "id": str(uuid7())}],
            "seed_occurred_at": "2026-05-18T12:00:00+00:00",
            "scope_actors": [str(actor_id), "not-a-uuid"],
            "region_spec": {"kind": "cluster", "id": "risk"},
            "topology_event_id": str(event_id),
            "topology_event_kind": "emergence",
            "neighborhood_id": str(neighborhood_id),
            "member_model_ids": [str(member_id), "bad"],
        },
    )

    assert trigger.seed_natural_text == "neighborhood emerged around churn risk"
    assert trigger.seed_entity_ids and trigger.seed_entity_ids[0]["type"] == "customer"
    assert trigger.seed_occurred_at is not None
    assert trigger.scope_actors == [actor_id]
    assert trigger.region_spec == {"kind": "cluster", "id": "risk"}
    assert trigger.topology_event_id == event_id
    assert trigger.topology_event_kind == "emergence"
    assert trigger.neighborhood_id == neighborhood_id
    assert trigger.member_model_ids == [member_id]


async def test_scripted_think_creates_model_audit_state_change_and_post_commit(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
) -> None:
    actor_id, obs_id = await _seed_actor_observation(fresh_db, tenant)
    trigger_id = uuid7()
    natural = "Alice is actively investigating Q3 retention risk."
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text=natural,
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[actor_id],
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = ScriptedProvider(
        responses=[
            _diff(
                trigger_id=trigger_id,
                tenant_id=tenant,
                claim_ops=[
                    _state_insert_op(
                        tenant_id=tenant,
                        observation_id=obs_id,
                        actor_id=actor_id,
                        natural=natural,
                    )
                ],
            )
        ]
    )

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content="Alice started retention analysis.",
    )

    assert outcome.status == "success", outcome.error
    assert outcome.ops_applied_count == 1
    async with fresh_db.acquire() as conn:
        model = await conn.fetchrow(
            """
            SELECT id, proposition_kind, "natural", confidence
            FROM models
            WHERE tenant_id = $1 AND born_from_event_id = $2
            """,
            tenant,
            obs_id,
        )
        assert model is not None
        assert model["proposition_kind"] == "state"
        assert model["natural"] == natural

        audit_count = await conn.fetchval(
            """
            SELECT count(*) FROM audit_events
            WHERE tenant_id = $1 AND model_id = $2 AND cause_type = 'create'
            """,
            tenant,
            model["id"],
        )
        state_change_count = await conn.fetchval(
            """
            SELECT count(*) FROM observations
            WHERE tenant_id = $1
              AND kind = 'state_change'
              AND content->>'entity_kind' = 'model'
              AND content->>'entity_id' = $2
            """,
            tenant,
            str(model["id"]),
        )
        post_commit_kinds = await conn.fetch(
            """
            SELECT action_kind
            FROM pending_post_commit_actions
            WHERE tenant_id = $1 AND trigger_id = $2
            ORDER BY action_kind
            """,
            tenant,
            trigger_id,
        )
        validation_errors = await conn.fetchval(
            "SELECT validation_error_count FROM think_runs WHERE id = $1",
            outcome.run_id,
        )

    assert audit_count == 1
    assert state_change_count == 1
    assert validation_errors == 0
    assert [r["action_kind"] for r in post_commit_kinds] == ["broadcast_realtime"]


async def test_think_partially_accepts_mixed_llm_diff_and_records_drop_count(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
) -> None:
    actor_id, obs_id = await _seed_actor_observation(fresh_db, tenant)
    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text="mixed-quality diff",
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[actor_id],
        seed_signature={"trigger_id": str(trigger_id)},
    )

    bad_high_conf = _state_insert_op(
        tenant_id=tenant,
        observation_id=obs_id,
        actor_id=actor_id,
        natural="Alice will definitely save the quarter.",
        confidence=0.9,
    )
    good_low_conf = _state_insert_op(
        tenant_id=tenant,
        observation_id=obs_id,
        actor_id=actor_id,
        natural="Alice is investigating retention risk.",
        confidence=0.55,
    )
    provider = ScriptedProvider(
        responses=[
            _diff(
                trigger_id=trigger_id,
                tenant_id=tenant,
                claim_ops=[bad_high_conf, good_low_conf],
            )
        ]
    )

    outcome = await think(trigger, fresh_db, llm_provider=provider)

    assert outcome.status == "success", outcome.error
    async with fresh_db.acquire() as conn:
        models = await conn.fetch(
            """
            SELECT "natural" FROM models
            WHERE tenant_id = $1 AND born_from_event_id = $2
            ORDER BY "natural"
            """,
            tenant,
            obs_id,
        )
        validation_error_count = await conn.fetchval(
            "SELECT validation_error_count FROM think_runs WHERE id = $1",
            outcome.run_id,
        )

    assert [r["natural"] for r in models] == ["Alice is investigating retention risk."]
    assert validation_error_count == 1


async def test_duplicate_claims_auto_merge_through_real_think_path(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    models_repo: ModelsRepo,
    tenant_cleanup,
) -> None:
    actor_id, obs_id = await _seed_actor_observation(fresh_db, tenant)
    seed_embedding = make_embedding("retention risk investigation")

    with notify_scope():
        existing = await models_repo.insert(
            ModelCreate(
                tenant_id=tenant,
                born_from_event_id=obs_id,
                proposition={
                    "kind": "state",
                    "subject": str(actor_id),
                    "assertion": "Alice is investigating retention risk.",
                },
                natural="Alice is investigating retention risk.",
                embedding=seed_embedding,
                scope_actors=[actor_id],
                scope_entities=[],
                scope_temporal={"valid_from": datetime.now(timezone.utc).isoformat()},
                confidence=0.5,
                confidence_at_assertion=0.5,
            )
        )

    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text="Alice is investigating retention risk.",
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[actor_id],
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = ScriptedProvider(
        responses=[
            _diff(
                trigger_id=trigger_id,
                tenant_id=tenant,
                claim_ops=[
                    _state_insert_op(
                        tenant_id=tenant,
                        observation_id=obs_id,
                        actor_id=actor_id,
                        natural="Alice is investigating retention risk.",
                        confidence=0.65,
                        embedding=seed_embedding,
                    )
                ],
            )
        ]
    )

    outcome = await think(trigger, fresh_db, llm_provider=provider)

    assert outcome.status == "success", outcome.error
    async with fresh_db.acquire() as conn:
        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        reconcile = await conn.fetchrow(
            """
            SELECT decision, matched_model_id
            FROM reconciliation_events
            WHERE tenant_id = $1 AND trigger_id = $2
            """,
            tenant,
            trigger_id,
        )
        changed_confidence = await conn.fetchval(
            "SELECT confidence FROM models WHERE id = $1",
            existing.id,
        )

    assert model_count == 1
    assert reconcile is not None
    assert reconcile["decision"] == "auto_merge"
    assert reconcile["matched_model_id"] == existing.id
    assert changed_confidence > existing.confidence
