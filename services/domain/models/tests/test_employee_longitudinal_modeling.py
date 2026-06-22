from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.actors.operating_context import (
    load_actor_operating_context,
    summarize_actor_operating_context,
)
from services.domain.models.open_questions import (
    ModelOpenQuestionCreate,
    ModelOpenQuestionsRepo,
)
from services.domain.models.repo import ModelsRepo
from services.domain.observations.events import notify_scope
from services.domain.projections import EmployeeProfileProjector, ProjectionRunner
from services.domain.projections.repo import ProjectionRepo
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.pathways import pathway_l_semantic_terms
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True)
class EmployeeLongitudinalFixture:
    alice_id: uuid.UUID
    morgan_id: uuid.UUID
    nova_customer_id: uuid.UUID
    events: dict[str, uuid.UUID] = field(default_factory=dict)
    commitments: dict[str, uuid.UUID] = field(default_factory=dict)
    models: dict[str, uuid.UUID] = field(default_factory=dict)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


async def _insert_actor(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    display_name: str,
    email: str,
) -> uuid.UUID:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
          id, tenant_id, type, display_name, email, status, metadata, created_at
        ) VALUES (
          $1, $2, 'human_internal', $3, $4, 'active', '{}'::jsonb, now()
        )
        """,
        actor_id,
        tenant_id,
        display_name,
        email,
    )
    return actor_id


async def _insert_observation(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    actor_id: uuid.UUID,
    content_text: str,
    embedding: list[float],
    occurred_at: datetime,
    entities_mentioned: Sequence[dict[str, Any]] = (),
) -> uuid.UUID:
    event_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, actor_id,
          content, content_text, embedding, embedding_pending, trust_tier,
          external_id, entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'employee-longitudinal', $4,
          $5::jsonb, $6, $7, FALSE, 'authoritative', $8, $9::jsonb
        )
        """,
        event_id,
        tenant_id,
        occurred_at,
        actor_id,
        json.dumps({"text": content_text}),
        content_text,
        embedding,
        f"employee-longitudinal-{event_id}",
        json.dumps(list(entities_mentioned)),
    )
    return event_id


async def _insert_customer_resource(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
) -> uuid.UUID:
    customer_id = uuid7()
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description, current_value,
          utilization_state, controllability, temporal_character
        ) VALUES (
          $1, $2, 'relational', 'Nova Bank', 'Customer Nova Bank',
          $3::jsonb, 'available', 'joint', 'renewable'
        )
        """,
        customer_id,
        tenant_id,
        json.dumps({"arr_cents": 1_200_000_00, "renewal_date": "2026-11-30"}),
    )
    return customer_id


async def _insert_commitment(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    owner_id: uuid.UUID,
    title: str,
    state: str,
    created_by_event_id: uuid.UUID,
    due_date: datetime,
) -> uuid.UUID:
    commitment_id = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments (
          id, tenant_id, title, state, owner_id, due_date,
          ambition_level, priority, created_by_event_id
        ) VALUES (
          $1, $2, $3, $4, $5, $6, 'base', 2, $7
        )
        """,
        commitment_id,
        tenant_id,
        title,
        state,
        owner_id,
        due_date,
        created_by_event_id,
    )
    return commitment_id


async def _seed_employee_history(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    embedding: list[float],
) -> EmployeeLongitudinalFixture:
    base = datetime(2026, 4, 1, 9, 0, tzinfo=timezone.utc)
    alice = await _insert_actor(
        conn,
        tenant_id,
        display_name="Alice Rivera",
        email="alice@company.test",
    )
    morgan = await _insert_actor(
        conn,
        tenant_id,
        display_name="Morgan Lee",
        email="morgan@company.test",
    )
    customer = await _insert_customer_resource(conn, tenant_id)
    alice_entity = {"type": "employee", "id": str(alice)}
    customer_entity = {"type": "customer", "id": str(customer)}
    event_specs = {
        "decomposition": (
            alice,
            0,
            "Alice decomposed the ambiguous Nova billing migration into reversible slices.",
        ),
        "incident_trust": (
            alice,
            16,
            "Alice repaired customer trust during the Nova payment incident.",
        ),
        "written_brief": (
            alice,
            31,
            "Alice asked for written briefs before deep architecture review.",
        ),
        "mentor": (
            alice,
            47,
            "Alice mentored Priya through the rollout without taking ownership away.",
        ),
        "context_switching": (
            alice,
            59,
            "Alice struggled when support interrupts broke her design block.",
        ),
        "interrupt_rota": (
            alice,
            72,
            "Alice asked for clarity on the interrupt rota for Nova migration.",
        ),
        "recent_design": (
            alice,
            84,
            "Alice produced the best architecture review after two quiet design blocks.",
        ),
        "recent_handoff": (
            alice,
            87,
            "Alice wrote a crisp migration handoff that support could execute.",
        ),
        "morgan_demo": (
            morgan,
            86,
            "Morgan is strongest at launch demo polish and sales-room narrative.",
        ),
    }
    events: dict[str, uuid.UUID] = {}
    for label, (actor_id, day, text) in event_specs.items():
        events[label] = await _insert_observation(
            conn,
            tenant_id,
            actor_id=actor_id,
            content_text=text,
            embedding=embedding,
            occurred_at=base + timedelta(days=day),
            entities_mentioned=[alice_entity, customer_entity],
        )

    commitments = {
        "migration_architecture": await _insert_commitment(
            conn,
            tenant_id,
            owner_id=alice,
            title="Design Nova billing migration architecture",
            state="active",
            created_by_event_id=events["decomposition"],
            due_date=base + timedelta(days=96),
        ),
        "interrupt_rota": await _insert_commitment(
            conn,
            tenant_id,
            owner_id=alice,
            title="Clarify Nova support interrupt rota",
            state="blocked",
            created_by_event_id=events["interrupt_rota"],
            due_date=base + timedelta(days=91),
        ),
        "demo_polish": await _insert_commitment(
            conn,
            tenant_id,
            owner_id=morgan,
            title="Prepare Nova launch demo polish",
            state="active",
            created_by_event_id=events["morgan_demo"],
            due_date=base + timedelta(days=94),
        ),
    }
    return EmployeeLongitudinalFixture(
        alice_id=alice,
        morgan_id=morgan,
        nova_customer_id=customer,
        events=events,
        commitments=commitments,
    )


def _model_create(
    *,
    tenant_id: uuid.UUID,
    born_from_event_id: uuid.UUID,
    assertion: str,
    embedding: list[float],
    confidence: float,
    claim_role: str,
    domain_tags: list[str],
    scope_actors: list[uuid.UUID],
    scope_entities: list[dict[str, str]],
    semantic_terms: list[str],
    supporting_event_ids: list[uuid.UUID],
    proposition: dict[str, Any] | None = None,
) -> ModelCreate:
    prop = proposition or {
        "kind": "belief",
        "claim_role": claim_role,
        "assertion": assertion,
        "domain_tags": domain_tags,
    }
    prop.setdefault("claim_role", claim_role)
    prop.setdefault("domain_tags", domain_tags)
    if claim_role == "capability":
        prop.setdefault("capability_id", semantic_terms[0] if semantic_terms else "employee-capability")
        prop.setdefault("subject", assertion.split(" ", 1)[0])
        prop.setdefault("assessment", assertion)
    if claim_role == "pattern":
        prop.setdefault("abstraction_level", "pattern")
        prop.setdefault("time_mode", "recurring")
        prop.setdefault("observed_tendency", assertion)
        prop.setdefault("assessment", assertion)
    falsifier = None
    if confidence > 0.7:
        falsifier = {
            "kind": "observation_pattern",
            "pattern": "later employee operating data contradicts this profile",
            "within_window": "45 days",
        }
    return ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition=prop,
        natural=assertion,
        embedding=embedding,
        scope_actors=scope_actors,
        scope_entities=scope_entities,
        scope_temporal={
            "type": "longitudinal",
            "valid_from": "2026-04-01T00:00:00Z",
            "valid_until": None,
        },
        confidence=confidence,
        confidence_at_assertion=confidence,
        falsifier=falsifier,
        domain_tags=domain_tags,
        semantic_terms=semantic_terms,
        supporting_event_ids=supporting_event_ids,
        evidential_weight=0.75,
    )


def _relation_proposition(
    *,
    subject: str,
    relation: str,
    object_: str,
    assertion: str,
    domain_tags: list[str],
) -> dict[str, Any]:
    return {
        "kind": "belief",
        "claim_role": "relation",
        "abstraction_level": "relationship",
        "subject": subject,
        "relation": relation,
        "object": object_,
        "assertion": assertion,
        "domain_tags": domain_tags,
    }


async def _insert_employee_models(
    repo: ModelsRepo,
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    fixture: EmployeeLongitudinalFixture,
    embedding: list[float],
) -> EmployeeLongitudinalFixture:
    alice_entity = {"type": "employee", "id": str(fixture.alice_id)}
    morgan_entity = {"type": "employee", "id": str(fixture.morgan_id)}
    customer_entity = {"type": "customer", "id": str(fixture.nova_customer_id)}
    events = fixture.events
    model_specs = {
        "decomposition": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["decomposition"],
            assertion=(
                "Alice reliably decomposes ambiguous Nova billing migrations "
                "into reversible architecture slices."
            ),
            embedding=embedding,
            confidence=0.91,
            claim_role="capability",
            domain_tags=["employee", "capacity", "architecture", "migration"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity, customer_entity],
            semantic_terms=[
                "reversible architecture slicing",
                "ambiguous migration decomposition",
                "billing migration decomposition",
            ],
            supporting_event_ids=[events["decomposition"], events["recent_design"]],
        ),
        "incident_trust": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["incident_trust"],
            assertion="Alice repairs customer trust during payment incidents without drama.",
            embedding=embedding,
            confidence=0.86,
            claim_role="capability",
            domain_tags=["employee", "customer", "incident", "trust"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity, customer_entity],
            semantic_terms=[
                "payment incident trust repair",
                "customer trust recovery",
            ],
            supporting_event_ids=[events["incident_trust"], events["recent_handoff"]],
        ),
        "written_briefs": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["written_brief"],
            assertion=(
                "Alice prefers written briefs and quiet design windows before "
                "architecture review."
            ),
            embedding=embedding,
            confidence=0.84,
            claim_role="relation",
            domain_tags=["employee", "work_style", "architecture"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["written brief preference", "quiet design window"],
            supporting_event_ids=[events["written_brief"], events["recent_design"]],
            proposition=_relation_proposition(
                subject="Alice",
                relation="prefers_work_context",
                object_="written briefs and quiet design windows",
                assertion=(
                    "Alice prefers written briefs and quiet design windows before "
                    "architecture review."
                ),
                domain_tags=["employee", "work_style", "architecture"],
            ),
        ),
        "interrupt_support": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["interrupt_rota"],
            assertion=(
                "Alice needs clarity on the support interrupt rota before the "
                "Nova migration can stay on schedule."
            ),
            embedding=embedding,
            confidence=0.83,
            claim_role="concern",
            domain_tags=["employee", "support_need", "blocked", "workload"],
            scope_actors=[fixture.alice_id],
            scope_entities=[
                alice_entity,
                {"type": "commitment", "id": str(fixture.commitments["interrupt_rota"])},
            ],
            semantic_terms=["support interrupt rota clarity", "migration support need"],
            supporting_event_ids=[events["context_switching"], events["interrupt_rota"]],
        ),
        "context_switching_risk": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["context_switching"],
            assertion=(
                "Alice burns out when context switching interrupts deep "
                "architecture work."
            ),
            embedding=embedding,
            confidence=0.77,
            claim_role="concern",
            domain_tags=["employee", "workload", "risk", "architecture"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["context switching burnout risk", "deep architecture interruption"],
            supporting_event_ids=[events["context_switching"]],
        ),
        "design_block_pattern": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["recent_design"],
            assertion=(
                "Alice's strongest architecture work follows two uninterrupted "
                "design blocks."
            ),
            embedding=embedding,
            confidence=0.80,
            claim_role="pattern",
            domain_tags=["employee", "work_pattern", "architecture"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["uninterrupted design blocks", "architecture focus pattern"],
            supporting_event_ids=[events["written_brief"], events["recent_design"]],
        ),
        "mentor": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["mentor"],
            assertion="Alice mentors Priya through rollout work without taking ownership away.",
            embedding=embedding,
            confidence=0.75,
            claim_role="relation",
            domain_tags=["employee", "mentorship", "ownership"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["ownership preserving mentorship", "rollout mentoring"],
            supporting_event_ids=[events["mentor"]],
            proposition=_relation_proposition(
                subject="Alice",
                relation="mentors_without_displacing_owner",
                object_="Priya rollout ownership",
                assertion="Alice mentors Priya through rollout work without taking ownership away.",
                domain_tags=["employee", "mentorship", "ownership"],
            ),
        ),
        "handoff": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["recent_handoff"],
            assertion="Alice writes crisp migration handoffs that support can execute.",
            embedding=embedding,
            confidence=0.73,
            claim_role="capability",
            domain_tags=["employee", "support", "handoff", "migration"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity, customer_entity],
            semantic_terms=["crisp migration handoff", "support executable handoff"],
            supporting_event_ids=[events["recent_handoff"]],
        ),
        "demo_polish_low": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["recent_handoff"],
            assertion="Alice sometimes contributes useful demo polish.",
            embedding=embedding,
            confidence=0.55,
            claim_role="capability",
            domain_tags=["employee", "demo"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["demo polish contribution"],
            supporting_event_ids=[events["recent_handoff"]],
        ),
        "stale_incident_avoidance": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["decomposition"],
            assertion="Alice avoids incident response work.",
            embedding=embedding,
            confidence=0.82,
            claim_role="concern",
            domain_tags=["employee", "incident", "deprecated"],
            scope_actors=[fixture.alice_id],
            scope_entities=[alice_entity],
            semantic_terms=["stale incident avoidance"],
            supporting_event_ids=[events["decomposition"]],
        ),
        "morgan_demo": _model_create(
            tenant_id=tenant_id,
            born_from_event_id=events["morgan_demo"],
            assertion="Morgan is strongest at launch demo polish and sales-room narrative.",
            embedding=embedding,
            confidence=0.93,
            claim_role="capability",
            domain_tags=["employee", "demo", "sales"],
            scope_actors=[fixture.morgan_id],
            scope_entities=[morgan_entity],
            semantic_terms=["launch demo polish", "sales room narrative"],
            supporting_event_ids=[events["morgan_demo"]],
        ),
    }

    inserted: dict[str, uuid.UUID] = {}
    with notify_scope():
        for label, model in model_specs.items():
            row = await repo.insert(model, conn=conn)
            inserted[label] = row.id
        await ModelOpenQuestionsRepo().insert(
            conn,
            ModelOpenQuestionCreate(
                tenant_id=tenant_id,
                model_id=inserted["interrupt_support"],
                question=(
                    "Which interrupt rota would protect architecture focus "
                    "while keeping support responsive?"
                ),
                question_type="constraint_boundary",
                rationale=(
                    "The employee profile needs the operating boundary for "
                    "support interrupts."
                ),
                priority=0.88,
                search_signature={
                    "semantic_terms": [
                        "support interrupt rota clarity",
                        "architecture focus protection",
                    ],
                    "hints": ["support rota", "interrupt policy"],
                },
                source_event_id=events["interrupt_rota"],
            ),
        )
    await repo.archive(
        inserted["stale_incident_avoidance"],
        "superseded",
        cause_event_id=events["incident_trust"],
        conn=conn,
    )
    return EmployeeLongitudinalFixture(
        alice_id=fixture.alice_id,
        morgan_id=fixture.morgan_id,
        nova_customer_id=fixture.nova_customer_id,
        events=fixture.events,
        commitments=fixture.commitments,
        models=inserted,
    )


async def _seed_profile(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> EmployeeLongitudinalFixture:
    fixture = await _seed_employee_history(tx_conn, tenant, embedding)
    return await _insert_employee_models(repo, tx_conn, tenant, fixture, embedding)


async def test_employee_operating_context_accumulates_longitudinal_profile(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> None:
    fixture = await _seed_profile(repo, tx_conn, tenant, embedding)
    await ProjectionRunner([EmployeeProfileProjector()]).run_once(
        tx_conn,
        tenant_id=tenant,
    )

    contexts = await load_actor_operating_context(
        tx_conn,
        tenant_id=tenant,
        actor_ids=[fixture.alice_id, fixture.morgan_id],
        max_models_per_actor=8,
        reference_time=datetime(2026, 6, 29, tzinfo=timezone.utc),
        observation_window=timedelta(days=21),
    )
    by_actor = {ctx.actor_id: ctx for ctx in contexts}
    alice = by_actor[fixture.alice_id]
    morgan = by_actor[fixture.morgan_id]

    assert alice.display_name == "Alice Rivera"
    assert alice.active_model_count == 8
    assert alice.active_commitment_count == 2
    assert alice.blocked_commitment_count == 1
    assert alice.recent_observation_count == 3
    assert fixture.models["decomposition"] in alice.model_ids
    assert fixture.models["incident_trust"] in alice.model_ids
    assert fixture.models["written_briefs"] in alice.model_ids
    assert fixture.models["interrupt_support"] in alice.model_ids
    assert fixture.models["demo_polish_low"] not in alice.model_ids
    assert fixture.models["stale_incident_avoidance"] not in alice.model_ids
    assert fixture.models["morgan_demo"] not in alice.model_ids

    assert any("reversible architecture slices" in item for item in alice.capabilities)
    assert any("repairs customer trust" in item for item in alice.capabilities)
    assert any("prefers written briefs" in item for item in alice.relationship_context)
    assert any("mentors Priya" in item for item in alice.relationship_context)
    assert any("support interrupt rota" in item for item in alice.support_needs)
    assert any(str(fixture.commitments["interrupt_rota"]) in item for item in alice.support_needs)
    assert any("context switching interrupts" in item for item in alice.risk_factors)
    assert any("two uninterrupted design blocks" in item for item in alice.constraints)

    assert morgan.active_model_count == 1
    assert fixture.models["morgan_demo"] in morgan.model_ids
    assert fixture.models["decomposition"] not in morgan.model_ids

    summary = summarize_actor_operating_context([alice])
    assert summary is not None
    assert "Alice Rivera" in summary
    assert "capabilities" in summary
    assert "support_needs" in summary
    assert "relationships" in summary


async def test_employee_profile_projection_preserves_evidence_span_and_roles(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> None:
    fixture = await _seed_profile(repo, tx_conn, tenant, embedding)

    processed = await ProjectionRunner([EmployeeProfileProjector()]).run_once(
        tx_conn,
        tenant_id=tenant,
    )
    snapshot = await ProjectionRepo().get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="employee_profiles",
        subject_key=f"employee:{fixture.alice_id}:profile",
    )

    assert processed >= 10
    assert snapshot is not None
    assert snapshot.confidence >= 0.86
    assert snapshot.payload["role_counts"]["capability"] == 3
    assert snapshot.payload["role_counts"]["concern"] == 2
    assert snapshot.payload["role_counts"]["relation"] == 2
    assert snapshot.payload["role_counts"]["pattern"] == 1
    assert snapshot.payload["evidence_span"]["observation_count"] >= 7
    assert snapshot.payload["evidence_span"]["first_seen"].startswith("2026-04-01")
    assert snapshot.payload["evidence_span"]["last_seen"].startswith("2026-06-27")
    assert "reversible architecture slicing" in snapshot.payload["semantic_terms"]
    assert "quiet design window" in snapshot.payload["semantic_terms"]
    assert snapshot.payload["open_questions"][0]["question_type"] == "constraint_boundary"
    assert "interrupt rota" in snapshot.payload["open_questions"][0]["question"]
    assert fixture.models["decomposition"] in snapshot.source_model_ids
    assert fixture.models["stale_incident_avoidance"] not in snapshot.source_model_ids
    assert fixture.models["morgan_demo"] not in snapshot.source_model_ids


async def test_employee_profile_is_retrievable_by_projection_and_specific_terms(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    pool: asyncpg.Pool,
    embedding: list[float],
) -> None:
    fixture = await _seed_profile(repo, tx_conn, tenant, embedding)
    await ProjectionRunner([EmployeeProfileProjector()]).run_once(
        tx_conn,
        tenant_id=tenant,
    )
    subject_key = f"employee:{fixture.alice_id}:profile"

    semantic_result = await pathway_l_semantic_terms(
        "Need reversible architecture slicing with a quiet design window.",
        tenant,
        tx_conn,
        scope_actors=[fixture.alice_id],
        limit=6,
    )
    semantic_model_ids = {model.id for model in semantic_result.models}
    assert fixture.models["decomposition"] in semantic_model_ids
    assert fixture.models["written_briefs"] in semantic_model_ids
    assert fixture.models["morgan_demo"] not in semantic_model_ids

    cfg = RetrievalConfig(
        trigger_weights_json='{"T4":{"A":1.0,"D":0.0,"G":0.0}}',
        projection_context_enabled=True,
        projection_context_max_snapshots=2,
        projection_context_max_models=8,
        semantic_terms_enabled=True,
        semantic_terms_k=8,
    )
    result = await primary_retrieve(
        TriggerContext(
            kind="T4",
            tenant_id=tenant,
            seed_natural_text=(
                "Build employee profile context for reversible architecture "
                "slicing and quiet design window."
            ),
            scope_actors=[fixture.alice_id],
        ),
        tx_conn,
        config=cfg,
        models_repo=ModelsRepo(pool, embedder=None, run_topology_on_insert=False),
    )

    result_model_ids = {model.id for model in result.models}
    assert fixture.models["decomposition"] in result_model_ids
    assert fixture.models["written_briefs"] in result_model_ids
    assert fixture.models["stale_incident_avoidance"] not in result_model_ids
    assert result.notes["projection_context"]["models_returned"] >= 2
    assert "employee_profiles" in result.notes["projection_context"]["projection_names"]
    assert any(path.notes.get("projection_first") for path in result.pathway_results)
