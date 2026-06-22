from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.domain.models.events import MODEL_EVENT_CREATED, emit_model_event, model_semantic_snapshot


def _row() -> ModelRow:
    now = datetime.now(timezone.utc)
    return ModelRow(
        id=uuid7(),
        tenant_id=uuid7(),
        born_from_event_id=uuid7(),
        proposition={
            "kind": "belief",
            "claim_role": "concern",
            "assertion": "Runway pressure is increasing.",
            "domain_tags": ["runway", "financial_capacity"],
        },
        natural="Runway pressure is increasing.",
        embedding=[0.0],
        scope_actors=[],
        scope_entities=[],
        scope_temporal={"type": "current"},
        confidence=0.82,
        activation=1.0,
        falsifier=None,
        signal_readings=[],
        reading_contestable=True,
        supporting_event_ids=[],
        supporting_model_ids=[],
        evidential_weight=0.5,
        status="active",
        archived_at=None,
        archive_reason=None,
        created_at=now,
        last_retrieved_at=None,
        retrieval_count=0,
        evaluate_at=None,
        resolution_criteria=None,
        contributing_models=[],
        visible_to_subjects=True,
        proposition_kind="belief",
        claim_role="concern",
        abstraction_level="atomic",
        time_mode="current",
        modality="inferred",
        polarity="negative",
        domain_tags=["runway", "financial_capacity"],
        memory_grammar_version="v1",
        confirmed_count=0,
        contested_count=0,
        last_confirmed_at=None,
        confidence_at_assertion=0.82,
        resolved_at=None,
        resolution_outcome=None,
        activation_coefficient=1.0,
        target_actor_id=None,
        caused_act_change_id=None,
    )


def test_model_semantic_snapshot_is_projection_neutral() -> None:
    snapshot = model_semantic_snapshot(_row())

    assert snapshot["proposition_kind"] == "belief"
    assert snapshot["claim_role"] == "concern"
    assert snapshot["domain_tags"] == ["runway", "financial_capacity"]
    assert "projection" not in snapshot


@pytest.mark.asyncio
async def test_emit_model_event_rejects_projection_specific_event_type() -> None:
    with pytest.raises(ValueError):
        await emit_model_event(
            None,  # type: ignore[arg-type]
            model=_row(),
            event_type="projection.updated",
            changed_fields=["confidence"],
        )

    assert MODEL_EVENT_CREATED == "model.created"
