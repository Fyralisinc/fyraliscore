from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.decision_surfaces import DecisionSurfaceProjector
from services.domain.projections.types import ModelEvent


def _event(
    *,
    claim_role: str | None = "situation",
    domain_tags: tuple[str, ...] = ("revenue", "risk"),
    scope_entities: tuple[dict[str, str], ...] = (),
    scope_actors: tuple[str, ...] = (),
    proposition: dict | None = None,
) -> ModelEvent:
    tenant_id = uuid7()
    model_id = uuid7()
    prop = proposition or {
        "kind": "belief",
        "claim_role": claim_role,
        "situation": "Foundry renewal is blocked by controls approval",
        "pressure_type": "revenue",
    }
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
            "scope_actors": list(scope_actors),
            "proposition": prop,
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


def test_decision_surface_projector_matches_pressure_semantics() -> None:
    projector = DecisionSurfaceProjector()

    assert projector.matches(_event())
    assert projector.matches(
        _event(
            claim_role="recommendation",
            domain_tags=(),
            proposition={
                "kind": "norm",
                "claim_role": "recommendation",
                "proposed_change": {
                    "operation": "create",
                    "payload": {
                        "kind": "decision_pressure",
                        "source_pressure_type": "resource",
                    },
                },
            },
        )
    )
    assert not projector.matches(_event(claim_role="fact", domain_tags=("reporting",)))


@pytest.mark.asyncio
async def test_decision_surface_projector_resolves_pressure_subjects() -> None:
    projector = DecisionSurfaceProjector()
    customer_id = str(uuid7())
    actor_id = str(uuid7())
    event = _event(
        scope_entities=({"type": "customer", "id": customer_id},),
        scope_actors=(actor_id,),
    )

    subjects = await projector.affected_subjects(None, event)  # type: ignore[arg-type]

    assert "company:revenue:decision_surface" in subjects
    assert f"customer:{customer_id}:decision_surface" in subjects
    assert f"actor:{actor_id}:decision_surface" in subjects


@pytest.mark.asyncio
async def test_decision_surface_projector_projects_model_rows() -> None:
    projector = DecisionSurfaceProjector()
    tenant_id = uuid7()
    source_event_id = uuid7()
    actor_id = uuid7()
    customer_id = uuid7()
    model_id = uuid7()
    conn = _FakeConn(
        [
            {
                "id": model_id,
                "proposition": {
                    "kind": "belief",
                    "claim_role": "situation",
                    "situation": "Foundry controls approval blocks renewal",
                    "summary": "Security approval is blocking renewal confidence.",
                    "pressure_type": "revenue",
                    "judgment_change": "A named owner must decide next action.",
                },
                "natural": "Foundry controls approval is now blocking renewal.",
                "confidence": 0.72,
                "claim_role": "situation",
                "domain_tags": ["revenue", "risk"],
                "scope_entities": [{"type": "customer", "id": str(customer_id)}],
                "scope_actors": [actor_id],
                "created_at": datetime.now(timezone.utc),
            }
        ]
    )

    snapshot = await projector.project_subject(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        subject_key=f"customer:{customer_id}:decision_surface",
        source_event_ids=(source_event_id,),
    )

    assert snapshot.projection_name == "decision_surfaces"
    assert snapshot.subject_key == f"customer:{customer_id}:decision_surface"
    assert snapshot.source_model_ids == (model_id,)
    assert snapshot.source_event_ids == (source_event_id,)
    assert snapshot.confidence == 0.72
    assert snapshot.payload["kind"] == "decision_surface_projection"
    assert snapshot.payload["surface_state"] == "owned"
    assert snapshot.payload["decision_required"] is True
    surface = snapshot.payload["decision_surfaces"][0]
    assert surface["model_id"] == str(model_id)
    assert surface["owner_actor_id"] == str(actor_id)
    assert surface["pressure_type"] == "revenue"
    assert surface["scope_entities"] == [{"type": "customer", "id": str(customer_id)}]
    assert isinstance(surface["revisit_triggers"], dict)


@pytest.mark.asyncio
async def test_decision_surface_projector_keeps_company_entity_subject_distinct() -> None:
    projector = DecisionSurfaceProjector()
    tenant_id = uuid7()
    company_id = uuid7()
    conn = _FakeConn([])

    await projector.project_subject(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        subject_key=f"company:{company_id}:decision_surface",
        source_event_ids=(uuid7(),),
    )

    query, args = conn.calls[0]
    assert "scope_entities @>" in query
    assert args[3] == json.dumps(
        [{"type": "company", "id": str(company_id)}],
        sort_keys=True,
        default=str,
    )
