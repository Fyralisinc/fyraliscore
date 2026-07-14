from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.constraints import ConstraintProjector
from services.domain.projections.runtime import ProjectionRegistry
from services.domain.projections.types import ModelEvent


def _event(
    *,
    claim_role: str | None = "concern",
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


def test_constraint_projector_matches_constraint_semantics() -> None:
    projector = ConstraintProjector()

    assert projector.matches(_event())
    assert projector.matches(_event(claim_role="fact", domain_tags=("bottleneck",)))
    assert not projector.matches(_event(claim_role="fact", domain_tags=("reporting",)))


@pytest.mark.asyncio
async def test_constraint_projector_resolves_tag_and_entity_subjects() -> None:
    projector = ConstraintProjector()
    customer_id = str(uuid7())
    event = _event(
        domain_tags=("runway", "financial_capacity", "hiring"),
        scope_entities=({"type": "customer", "id": customer_id},),
    )

    subjects = await projector.affected_subjects(None, event)  # type: ignore[arg-type]

    assert "company:runway" in subjects
    assert "company:financial_capacity" in subjects
    assert "company:capacity" in subjects
    assert f"customer:{customer_id}:constraints" in subjects


@pytest.mark.asyncio
async def test_constraint_projector_uses_tenant_subject_when_no_specific_scope() -> None:
    projector = ConstraintProjector()
    event = _event(domain_tags=("constraint",), scope_entities=())

    subjects = await projector.affected_subjects(None, event)  # type: ignore[arg-type]

    assert subjects == [f"tenant:{event.tenant_id}:constraints"]


def test_projection_registry_rejects_duplicate_projector_versions() -> None:
    registry = ProjectionRegistry([ConstraintProjector()])

    with pytest.raises(ValueError):
        registry.register(ConstraintProjector())
