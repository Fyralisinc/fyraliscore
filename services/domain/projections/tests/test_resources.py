from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.resources import ResourceProjector
from services.domain.projections.types import ModelEvent


def _event(
    *,
    claim_role: str | None = "capability",
    domain_tags: tuple[str, ...] = ("runway", "financial_capacity"),
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


def test_resource_projector_matches_resource_semantics() -> None:
    projector = ResourceProjector()

    assert projector.matches(_event())
    assert projector.matches(_event(claim_role="concern", domain_tags=("customer",)))
    assert projector.matches(_event(domain_tags=("resource",)))
    assert projector.matches(
        _event(
            claim_role="situation",
            domain_tags=("reporting",),
            scope_entities=({"type": "employee", "id": str(uuid7())},),
        )
    )
    assert not projector.matches(_event(claim_role="fact", domain_tags=("reporting",)))


@pytest.mark.asyncio
async def test_resource_projector_resolves_kind_and_entity_subjects() -> None:
    projector = ResourceProjector()
    customer_id = str(uuid7())
    event = _event(
        domain_tags=("cash", "hiring", "customer"),
        scope_entities=({"type": "customer", "id": customer_id},),
    )

    subjects = await projector.affected_subjects(None, event)  # type: ignore[arg-type]

    assert "company:financial" in subjects
    assert "company:capacity" in subjects
    assert "company:relational" in subjects
    assert f"customer:{customer_id}:resources" in subjects


@pytest.mark.asyncio
async def test_resource_projector_uses_tenant_subject_when_no_specific_scope() -> None:
    projector = ResourceProjector()
    event = _event(domain_tags=("resource",), scope_entities=())

    subjects = await projector.affected_subjects(None, event)  # type: ignore[arg-type]

    assert subjects == [f"tenant:{event.tenant_id}:resources"]
