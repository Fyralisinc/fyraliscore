from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.actors.operating_context import load_actor_operating_context
from services.domain.models.open_questions import (
    ModelOpenQuestionCreate,
    ModelOpenQuestionsRepo,
)
from services.domain.models.repo import ModelsRepo
from services.domain.projections import ConstraintProjector, ProjectionRunner, ResourceProjector
from services.domain.projections.repo import ProjectionRepo
from services.domain.projections.types import ModelEvent, ProjectionSnapshot
from services.domain.projections import subjects as projection_subjects
from services.domain.projections.subjects import ProjectionSubjectSeed
from services.domain.observations.events import notify_scope
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True)
class UniversalCompany:
    alice_id: uuid.UUID
    bob_id: uuid.UUID
    customer_id: uuid.UUID
    goal_id: uuid.UUID
    delivery_commitment_id: uuid.UUID
    risk_commitment_id: uuid.UUID
    runway_decision_id: uuid.UUID
    event_ids: tuple[uuid.UUID, ...]


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
    entities_mentioned: list[dict[str, Any]] | None = None,
) -> uuid.UUID:
    event_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, actor_id,
          content, content_text, embedding, embedding_pending, trust_tier,
          external_id, entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'company-universals', $4,
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
        f"company-universal-{event_id}",
        json.dumps(entities_mentioned or []),
    )
    return event_id


async def _insert_customer_resource(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    identity: str = "Beacon Health",
) -> uuid.UUID:
    customer_id = uuid7()
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description, current_value,
          utilization_state, controllability, temporal_character
        ) VALUES (
          $1, $2, 'relational', $3, $4, $5::jsonb,
          'available', 'joint', 'renewable'
        )
        """,
        customer_id,
        tenant_id,
        identity,
        "Customer Beacon Health",
        json.dumps({"arr_cents": 750_000_00, "renewal_date": "2026-12-15"}),
    )
    return customer_id


async def _insert_goal(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    created_by_event_id: uuid.UUID,
) -> uuid.UUID:
    goal_id = uuid7()
    await conn.execute(
        """
        INSERT INTO goals (
          id, tenant_id, title, state, altitude, cached_health,
          cached_health_computed_at, created_by_event_id
        ) VALUES (
          $1, $2, $3, 'active', 'operational', 'healthy', now(), $4
        )
        """,
        goal_id,
        tenant_id,
        title,
        created_by_event_id,
    )
    return goal_id


async def _insert_commitment(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    state: str,
    owner_id: uuid.UUID,
    created_by_event_id: uuid.UUID,
    due_date: datetime,
    customer_id: uuid.UUID | None = None,
) -> uuid.UUID:
    commitment_id = uuid7()
    counterparty = (
        {"type": "customer_resource", "id": str(customer_id)}
        if customer_id is not None
        else None
    )
    await conn.execute(
        """
        INSERT INTO commitments (
          id, tenant_id, title, state, owner_id, due_date,
          ambition_level, priority, external_counterparty_ref, created_by_event_id
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          'base', 3, $7::jsonb, $8
        )
        """,
        commitment_id,
        tenant_id,
        title,
        state,
        owner_id,
        due_date,
        json.dumps(counterparty) if counterparty else None,
        created_by_event_id,
    )
    return commitment_id


async def _insert_decision(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    decision_text: str,
    created_by_event_id: uuid.UUID,
) -> uuid.UUID:
    decision_id = uuid7()
    await conn.execute(
        """
        INSERT INTO decisions (
          id, tenant_id, title, decision_text, state, created_by_event_id
        ) VALUES ($1, $2, $3, $4, 'active', $5)
        """,
        decision_id,
        tenant_id,
        title,
        decision_text,
        created_by_event_id,
    )
    return decision_id


async def _link_goal(conn: asyncpg.Connection, commitment_id: uuid.UUID, goal_id: uuid.UUID) -> None:
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, TRUE)
        ON CONFLICT (commitment_id, goal_id) DO NOTHING
        """,
        commitment_id,
        goal_id,
    )


async def _link_customer_commitment(
    conn: asyncpg.Connection,
    *,
    tenant_id: uuid.UUID,
    customer_id: uuid.UUID,
    commitment_id: uuid.UUID,
    revenue_at_risk_usd: int,
    criticality: str = "high",
) -> None:
    await conn.execute(
        """
        INSERT INTO customer_commitments (
          id, tenant_id, customer_resource_id, commitment_id,
          served_description, relationship_kind, revenue_at_risk_usd, criticality
        ) VALUES (
          $1, $2, $3, $4,
          'renewal delivery depends on this commitment',
          'delivers', $5, $6
        )
        ON CONFLICT (customer_resource_id, commitment_id) DO UPDATE
        SET revenue_at_risk_usd = EXCLUDED.revenue_at_risk_usd,
            criticality = EXCLUDED.criticality
        """,
        uuid7(),
        tenant_id,
        customer_id,
        commitment_id,
        revenue_at_risk_usd,
        criticality,
    )


async def _link_constrained_by(
    conn: asyncpg.Connection,
    *,
    commitment_id: uuid.UUID,
    decision_id: uuid.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO constrained_by (commitment_id, decision_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        commitment_id,
        decision_id,
    )


async def _seed_company(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    embedding: list[float],
) -> UniversalCompany:
    base = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    alice = await _insert_actor(
        conn,
        tenant_id,
        display_name="Alice Rivera",
        email="alice@company.test",
    )
    bob = await _insert_actor(
        conn,
        tenant_id,
        display_name="Bob Chen",
        email="bob@company.test",
    )
    customer = await _insert_customer_resource(conn, tenant_id)
    event_texts = [
        "Alice unblocked the Beacon escalation by pairing with support.",
        "Alice closed three Beacon support loops before the weekly review.",
        "Bob promised the Beacon migration but did not name an owner for SSO.",
        "Finance notes runway pressure requires freezing non-critical hiring.",
        "Beacon renewal depends on the migration finishing before December.",
    ]
    events = []
    for index, text in enumerate(event_texts):
        actor = alice if index in {0, 1, 4} else bob
        events.append(
            await _insert_observation(
                conn,
                tenant_id,
                actor_id=actor,
                content_text=text,
                embedding=embedding,
                occurred_at=base + timedelta(days=index),
                entities_mentioned=[
                    {"type": "customer", "id": str(customer)},
                    {"type": "employee", "id": str(actor)},
                ],
            )
        )
    goal = await _insert_goal(
        conn,
        tenant_id,
        title="Retain Beacon Health renewal",
        created_by_event_id=events[0],
    )
    delivery = await _insert_commitment(
        conn,
        tenant_id,
        title="Finish Beacon migration",
        state="active",
        owner_id=alice,
        created_by_event_id=events[0],
        due_date=base + timedelta(days=45),
        customer_id=customer,
    )
    risk = await _insert_commitment(
        conn,
        tenant_id,
        title="Clarify Beacon SSO owner",
        state="blocked",
        owner_id=bob,
        created_by_event_id=events[2],
        due_date=base + timedelta(days=14),
        customer_id=customer,
    )
    decision = await _insert_decision(
        conn,
        tenant_id,
        title="Freeze non-critical hiring",
        decision_text="Runway pressure constrains hiring until Beacon closes.",
        created_by_event_id=events[3],
    )
    await _link_goal(conn, delivery, goal)
    await _link_goal(conn, risk, goal)
    await _link_customer_commitment(
        conn,
        tenant_id=tenant_id,
        customer_id=customer,
        commitment_id=delivery,
        revenue_at_risk_usd=750_000,
        criticality="must_have",
    )
    await _link_customer_commitment(
        conn,
        tenant_id=tenant_id,
        customer_id=customer,
        commitment_id=risk,
        revenue_at_risk_usd=750_000,
        criticality="high",
    )
    await _link_constrained_by(conn, commitment_id=delivery, decision_id=decision)
    return UniversalCompany(
        alice_id=alice,
        bob_id=bob,
        customer_id=customer,
        goal_id=goal,
        delivery_commitment_id=delivery,
        risk_commitment_id=risk,
        runway_decision_id=decision,
        event_ids=tuple(events),
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
    scope_actors: list[uuid.UUID] | None = None,
    scope_entities: list[dict[str, str]] | None = None,
    proposition: dict[str, Any] | None = None,
    semantic_terms: list[str] | None = None,
    supporting_event_ids: list[uuid.UUID] | None = None,
    supporting_model_ids: list[uuid.UUID] | None = None,
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
        prop.setdefault("capability_id", domain_tags[0] if domain_tags else "capability")
        prop.setdefault("subject", assertion.split(" ", 1)[0])
        prop.setdefault("assessment", assertion)
    falsifier = None
    if confidence > 0.7:
        falsifier = {
            "kind": "observation_pattern",
            "pattern": "authoritative operating data contradicts this model",
            "within_window": "30 days",
        }
    return ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition=prop,
        natural=assertion,
        embedding=embedding,
        scope_actors=scope_actors or [],
        scope_entities=scope_entities or [],
        scope_temporal={"type": "current"},
        confidence=confidence,
        confidence_at_assertion=confidence,
        falsifier=falsifier,
        domain_tags=domain_tags,
        semantic_terms=semantic_terms or [],
        supporting_event_ids=supporting_event_ids or [],
        supporting_model_ids=supporting_model_ids or [],
    )


class CustomerHealthProjector:
    name = "customers"
    version = "v1"

    def matches(self, event: ModelEvent) -> bool:
        return event.event_type in {"model.created", "model.updated", "model.archived"} and (
            "customer" in {tag.casefold() for tag in event.domain_tags}
            or any(entity.get("type") == "customer" for entity in event.scope_entities)
        )

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> list[str]:
        del conn
        return sorted(
            {
                f"customer:{entity['id']}:health"
                for entity in event.scope_entities
                if entity.get("type") == "customer" and entity.get("id")
            }
        )

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: uuid.UUID,
        subject_key: str,
        source_event_ids: list[uuid.UUID],
    ) -> ProjectionSnapshot:
        customer_id = subject_key.split(":")[1]
        rows = await conn.fetch(
            """
            SELECT id, "natural" AS natural, confidence, claim_role,
                   domain_tags, scope_entities
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND scope_entities @> $2::jsonb
              AND domain_tags && ARRAY['customer','customers','renewal','retention','trust','churn','risk']::text[]
            ORDER BY confidence DESC, created_at DESC
            LIMIT 25
            """,
            tenant_id,
            json.dumps([{"type": "customer", "id": customer_id}]),
        )
        confidence = max((float(row["confidence"]) for row in rows), default=0.0)
        severity = "high" if any(
            "risk" in {str(tag).casefold() for tag in row["domain_tags"] or []}
            and float(row["confidence"]) >= 0.7
            for row in rows
        ) else ("medium" if rows else "none")
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload={
                "kind": "customer_health_projection",
                "subject_key": subject_key,
                "status": "active" if rows else "empty",
                "severity": severity,
                "customer_model_count": len(rows),
                "customer_models": [
                    {
                        "model_id": str(row["id"]),
                        "natural": row["natural"],
                        "confidence": float(row["confidence"]),
                        "claim_role": row["claim_role"],
                        "domain_tags": list(row["domain_tags"] or []),
                    }
                    for row in rows
                ],
            },
            confidence=confidence,
            severity=severity,
            source_model_ids=tuple(row["id"] for row in rows),
            source_event_ids=tuple(source_event_ids),
        )


async def test_employee_universal_models_capability_load_and_support_need(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> None:
    company = await _seed_company(tx_conn, tenant, embedding)
    with notify_scope():
        alice_capability = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[1],
                assertion="Alice reliably resolves Beacon escalations before review.",
                embedding=embedding,
                confidence=0.81,
                claim_role="capability",
                domain_tags=["employee", "support", "customer", "reliability"],
                scope_actors=[company.alice_id],
                scope_entities=[{"type": "customer", "id": str(company.customer_id)}],
                semantic_terms=[
                    "beacon escalation resolver",
                    "support loop closure pattern",
                ],
                supporting_event_ids=[company.event_ids[0], company.event_ids[1]],
            ),
            conn=tx_conn,
        )
        bob_support_need = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[2],
                assertion="Bob needs ownership clarity before the Beacon SSO work can move.",
                embedding=embedding,
                confidence=0.76,
                claim_role="concern",
                domain_tags=["employee", "ownership", "blocked", "support_need"],
                scope_actors=[company.bob_id],
                scope_entities=[
                    {"type": "commitment", "id": str(company.risk_commitment_id)}
                ],
                semantic_terms=[
                    "sso ownership ambiguity",
                    "blocked commitment support need",
                ],
                supporting_event_ids=[company.event_ids[2]],
            ),
            conn=tx_conn,
        )

    contexts = await load_actor_operating_context(
        tx_conn,
        tenant_id=tenant,
        actor_ids=[company.alice_id, company.bob_id],
        reference_time=datetime(2026, 6, 10, tzinfo=timezone.utc),
    )
    by_actor = {ctx.actor_id: ctx for ctx in contexts}

    alice = by_actor[company.alice_id]
    bob = by_actor[company.bob_id]
    assert alice_capability.id in alice.model_ids
    assert bob_support_need.id in bob.model_ids
    assert any("resolves Beacon escalations" in item for item in alice.capabilities)
    assert alice.active_commitment_count == 1
    assert alice.recent_observation_count >= 3
    assert bob.blocked_commitment_count == 1
    assert any("ownership clarity" in item for item in bob.support_needs)
    assert any(str(company.risk_commitment_id) in item for item in bob.support_needs)


async def test_customer_universal_links_relationship_models_to_commitments_and_projection(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> None:
    company = await _seed_company(tx_conn, tenant, embedding)
    customer_question_repo = ModelOpenQuestionsRepo()
    with notify_scope():
        customer_risk = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[4],
                assertion="Beacon renewal risk is rising because migration ownership is blocked.",
                embedding=embedding,
                confidence=0.83,
                claim_role="concern",
                domain_tags=["customer", "renewal", "risk", "commitment", "trust"],
                scope_actors=[company.alice_id, company.bob_id],
                scope_entities=[
                    {"type": "customer", "id": str(company.customer_id)},
                    {"type": "commitment", "id": str(company.risk_commitment_id)},
                ],
                semantic_terms=[
                    "beacon renewal risk",
                    "migration ownership blocker",
                    "customer trust erosion",
                ],
                supporting_event_ids=[company.event_ids[2], company.event_ids[4]],
            ),
            conn=tx_conn,
        )
        await customer_question_repo.insert(
            tx_conn,
            ModelOpenQuestionCreate(
                tenant_id=tenant,
                model_id=customer_risk.id,
                question="Who is the accountable owner for the Beacon SSO migration?",
                question_type="owner_or_decision",
                rationale="The customer risk model remains hard to act on without owner clarity.",
                priority=0.9,
                search_signature={"terms": ["Beacon", "SSO", "owner", "migration"]},
            ),
        )

    customer_scoped = await repo.search_by_scope(
        tenant_id=tenant,
        scope_entities=[{"type": "customer", "id": str(company.customer_id)}],
        conn=tx_conn,
    )
    customer_model_ids = {row.id for row in customer_scoped}
    assert customer_risk.id in customer_model_ids
    assert any(
        {"type": "commitment", "id": str(company.risk_commitment_id)}
        in row.scope_entities
        for row in customer_scoped
    )

    runner = ProjectionRunner([CustomerHealthProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)
    subject_key = f"customer:{company.customer_id}:health"
    snapshot = await ProjectionRepo().get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="customers",
        subject_key=subject_key,
    )
    questions = await customer_question_repo.list_for_model(
        tx_conn,
        tenant_id=tenant,
        model_id=customer_risk.id,
    )

    assert processed >= 1
    assert snapshot is not None
    assert snapshot.severity == "high"
    assert customer_risk.id in snapshot.source_model_ids
    assert snapshot.payload["customer_model_count"] >= 1
    assert snapshot.payload["customer_models"][0]["model_id"] == str(customer_risk.id)
    assert questions[0].question_type == "owner_or_decision"
    assert "Beacon" in questions[0].question


async def test_constraints_and_resources_project_company_operating_limits(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    embedding: list[float],
) -> None:
    company = await _seed_company(tx_conn, tenant, embedding)
    with notify_scope():
        runway_constraint = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[3],
                assertion="Runway pressure constrains non-critical hiring until Beacon closes.",
                embedding=embedding,
                confidence=0.86,
                claim_role="concern",
                domain_tags=["runway", "financial_capacity", "constraint", "hiring"],
                scope_entities=[
                    {"type": "company", "id": str(tenant)},
                    {"type": "decision", "id": str(company.runway_decision_id)},
                ],
                semantic_terms=[
                    "runway hiring constraint",
                    "beacon renewal finance dependency",
                ],
                supporting_event_ids=[company.event_ids[3], company.event_ids[4]],
            ),
            conn=tx_conn,
        )
        capacity_resource = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[1],
                assertion="Alice's escalation skill is available capacity for Beacon recovery.",
                embedding=embedding,
                confidence=0.78,
                claim_role="capability",
                domain_tags=["employee", "capacity", "resource", "customer"],
                scope_actors=[company.alice_id],
                scope_entities=[
                    {"type": "employee", "id": str(company.alice_id)},
                    {"type": "customer", "id": str(company.customer_id)},
                ],
                semantic_terms=["alice escalation capacity", "beacon recovery capacity"],
                supporting_event_ids=[company.event_ids[0], company.event_ids[1]],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector(), ResourceProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)
    projection_repo = ProjectionRepo()
    runway = await projection_repo.get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
        subject_key="company:runway",
    )
    capacity = await projection_repo.get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="resources",
        subject_key="company:capacity",
    )
    employee_capacity = await projection_repo.get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="resources",
        subject_key=f"employee:{company.alice_id}:resources",
    )

    assert processed >= 2
    assert runway is not None
    assert runway.severity == "high"
    assert runway_constraint.id in runway.source_model_ids
    assert runway.payload["constraints"][0]["model_id"] == str(runway_constraint.id)
    assert capacity is not None
    assert capacity.payload["resource_kind"] == "capacity"
    assert capacity_resource.id in capacity.source_model_ids
    assert employee_capacity is not None
    assert employee_capacity.payload["resources"][0]["model_id"] == str(capacity_resource.id)


async def test_projection_first_retrieval_surfaces_customer_universal_context(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    pool: asyncpg.Pool,
    embedding: list[float],
) -> None:
    company = await _seed_company(tx_conn, tenant, embedding)
    with notify_scope():
        customer_risk = await repo.insert(
            _model_create(
                tenant_id=tenant,
                born_from_event_id=company.event_ids[4],
                assertion="Beacon health depends on finishing the migration commitment.",
                embedding=embedding,
                confidence=0.81,
                claim_role="concern",
                domain_tags=["customer", "renewal", "risk", "commitment"],
                scope_entities=[
                    {"type": "customer", "id": str(company.customer_id)},
                    {"type": "commitment", "id": str(company.delivery_commitment_id)},
                ],
                semantic_terms=["beacon health", "migration renewal risk"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([CustomerHealthProjector()])
    await runner.run_once(tx_conn, tenant_id=tenant)
    subject_key = f"customer:{company.customer_id}:health"

    def _resolver(seed: ProjectionSubjectSeed):
        if "beacon health" not in (seed.seed_natural_text or "").casefold():
            return []
        return [("customers", subject_key)]

    projection_subjects.register_subject_resolver("company-universal-customer", _resolver)
    try:
        cfg = RetrievalConfig(
            trigger_weights_json='{"T4":{"A":1.0,"D":0.0,"G":0.0}}',
            projection_context_enabled=True,
            projection_context_max_snapshots=4,
            projection_context_max_models=4,
        )
        result = await primary_retrieve(
            TriggerContext(
                kind="T4",
                tenant_id=tenant,
                seed_natural_text="Beacon health renewal context",
            ),
            tx_conn,
            config=cfg,
            models_repo=ModelsRepo(pool, embedder=None, run_topology_on_insert=False),
        )
    finally:
        projection_subjects.reset_for_tests()

    assert customer_risk.id in {model.id for model in result.models}
    assert result.notes["projection_context"]["models_returned"] == 1
    assert "customers" in result.notes["projection_context"]["projection_names"]
    projection_path = next(
        path for path in result.pathway_results if path.notes.get("projection_first")
    )
    assert projection_path.models[0].id == customer_risk.id
