from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.entity_surfaces import (
    CommitmentProjector,
    CustomerProjector,
    DecisionProjector,
    GoalProjector,
)
from services.domain.projections.types import ModelEvent


def _event(
    *,
    claim_role: str | None = "situation",
    domain_tags: tuple[str, ...] = ("customer", "renewal"),
    scope_entities: tuple[dict[str, str], ...] = (),
) -> ModelEvent:
    tenant_id = uuid7()
    model_id = uuid7()
    return ModelEvent(
        id=uuid7(),
        tenant_id=tenant_id,
        model_id=model_id,
        event_type="model.created",
        changed_fields=("proposition",),
        proposition_kind="belief",
        claim_role=claim_role,
        domain_tags=domain_tags,
        scope_entities=scope_entities,
        semantic_snapshot={
            "id": str(model_id),
            "tenant_id": str(tenant_id),
            "domain_tags": list(domain_tags),
            "claim_role": claim_role,
        },
        previous_snapshot=None,
        source_event_id=None,
        created_at=datetime.now(timezone.utc),
    )


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple]] = []

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        return self.rows


def test_entity_projectors_match_their_entity_vocabularies() -> None:
    customer_id = str(uuid7())
    commitment_id = str(uuid7())
    goal_id = str(uuid7())
    decision_id = str(uuid7())

    assert CustomerProjector().matches(
        _event(scope_entities=({"type": "customer_resource", "id": customer_id},))
    )
    assert CommitmentProjector().matches(
        _event(
            domain_tags=("commitment",),
            scope_entities=({"type": "commitment", "id": commitment_id},),
        )
    )
    assert GoalProjector().matches(
        _event(domain_tags=("roadmap",), scope_entities=({"type": "goal", "id": goal_id},))
    )
    assert DecisionProjector().matches(
        _event(
            domain_tags=("tradeoff",),
            scope_entities=({"type": "decision", "id": decision_id},),
        )
    )
    assert not CustomerProjector().matches(
        _event(claim_role="fact", domain_tags=("reporting",), scope_entities=())
    )
    assert not CustomerProjector().matches(
        _event(claim_role="situation", domain_tags=("reporting",), scope_entities=())
    )


@pytest.mark.asyncio
async def test_entity_projectors_resolve_entity_subjects_and_tenant_fallback() -> None:
    customer_id = str(uuid7())
    event = _event(
        domain_tags=("customer", "renewal"),
        scope_entities=({"type": "customer_resource", "id": customer_id},),
    )

    subjects = await CustomerProjector().affected_subjects(None, event)  # type: ignore[arg-type]

    assert subjects == [f"customer:{customer_id}:customers"]

    goal_event = _event(domain_tags=("goal",), scope_entities=())
    fallback = await GoalProjector().affected_subjects(
        None,  # type: ignore[arg-type]
        goal_event,
    )

    assert fallback == [f"tenant:{goal_event.tenant_id}:goals"]


@pytest.mark.asyncio
async def test_entity_projector_projects_contract_payload_and_dependencies() -> None:
    projector = CustomerProjector()
    tenant_id = uuid7()
    model_id = uuid7()
    support_event_id = uuid7()
    source_event_id = uuid7()
    customer_id = uuid7()
    actor_id = uuid7()
    conn = _FakeConn(
        [
            {
                "id": model_id,
                "proposition": {
                    "kind": "belief",
                    "claim_role": "situation",
                    "summary": "Atlas renewal is at risk from usage decay.",
                },
                "natural": "Atlas renewal is at risk from usage decay.",
                "confidence": 0.81,
                "activation": 1.0,
                "claim_role": "concern",
                "domain_tags": ["customer", "renewal", "risk"],
                "scope_entities": [
                    {"type": "customer_resource", "id": str(customer_id)},
                    {"type": "commitment", "id": str(uuid7())},
                ],
                "scope_actors": [actor_id],
                "supporting_event_ids": [support_event_id],
                "created_at": datetime.now(timezone.utc),
                "evaluate_at": None,
            }
        ]
    )

    snapshot = await projector.project_subject(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        subject_key=f"customer:{customer_id}:customers",
        source_event_ids=(source_event_id,),
    )

    assert snapshot.projection_name == "customers"
    assert snapshot.subject_key == f"customer:{customer_id}:customers"
    assert snapshot.source_model_ids == (model_id,)
    assert snapshot.source_event_ids == (source_event_id, support_event_id)
    assert snapshot.confidence == 0.81
    assert snapshot.severity == "high"
    assert snapshot.payload["kind"] == "customer_projection"
    assert snapshot.payload["entity_type"] == "customer"
    assert snapshot.payload["status"] == "at_risk"
    assert snapshot.payload["health"] == "at_risk"
    assert snapshot.payload["evidence_model_ids"] == [str(model_id)]
    assert snapshot.payload["evidence_event_ids"] == [
        str(source_event_id),
        str(support_event_id),
    ]
    assert snapshot.payload["owner_actor_ids"] == [str(actor_id)]
    assert snapshot.payload["related_entity_refs"][0]["type"] == "commitment"
    assert snapshot.payload["customer_signals"][0]["model_id"] == str(model_id)


@pytest.mark.asyncio
async def test_entity_projector_queries_all_variant_entity_types() -> None:
    customer_id = uuid7()
    conn = _FakeConn([])

    await CustomerProjector().project_subject(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid7(),
        subject_key=f"customer:{customer_id}:customers",
        source_event_ids=(),
    )

    query, args = conn.calls[0]
    assert "scope_entities @>" in query
    encoded_filters = [value for value in args if isinstance(value, str)]
    assert any('"type": "customer_resource"' in value for value in encoded_filters)
    assert any('"type": "customer"' in value for value in encoded_filters)
