from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.domain.models.events import model_semantic_snapshot
from services.domain.models.facets import (
    MODEL_FACET_SCHEMA_VERSION,
    model_facet_names,
    split_model_facets,
)


def _row(**overrides) -> ModelRow:
    now = datetime.now(timezone.utc)
    data = {
        "id": uuid7(),
        "tenant_id": uuid7(),
        "born_from_event_id": uuid7(),
        "proposition": {"kind": "belief", "assertion": "Runway is constrained."},
        "natural": "Runway is constrained.",
        "embedding": [0.1, 0.2, 0.3],
        "scope_actors": [],
        "scope_entities": [{"type": "company", "id": "acme"}],
        "scope_temporal": {"type": "current"},
        "confidence": 0.72,
        "activation": 0.8,
        "falsifier": {"kind": "observation_pattern", "pattern": "cash improves"},
        "signal_readings": [{"stance": "supports"}],
        "reading_contestable": True,
        "supporting_event_ids": [uuid7()],
        "supporting_model_ids": [uuid7()],
        "evidential_weight": 0.64,
        "status": "active",
        "archived_at": None,
        "archive_reason": None,
        "created_at": now,
        "last_retrieved_at": now - timedelta(hours=1),
        "retrieval_count": 3,
        "evaluate_at": None,
        "resolution_criteria": None,
        "contributing_models": [],
        "visible_to_subjects": True,
        "proposition_kind": "belief",
        "claim_role": "concern",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "negative",
        "domain_tags": ["runway", "constraint"],
        "semantic_terms": ["cash runway boundary", "burn multiple"],
        "memory_grammar_version": "v1",
        "confirmed_count": 2,
        "contested_count": 1,
        "last_confirmed_at": now,
        "confidence_at_assertion": 0.74,
        "resolved_at": None,
        "resolution_outcome": None,
        "activation_coefficient": 0.9,
        "target_actor_id": None,
        "caused_act_change_id": None,
    }
    data.update(overrides)
    return ModelRow(**data)


def test_split_model_facets_groups_model_row_without_projection_language() -> None:
    row = _row()
    facets = split_model_facets(row)

    assert facets.schema_version == MODEL_FACET_SCHEMA_VERSION
    assert facets.names == ("core", "semantic", "evidence", "runtime", "retrieval")
    assert facets.core.proposition == row.proposition
    assert facets.core.confidence == row.confidence
    assert facets.semantic.domain_tags == ["runway", "constraint"]
    assert facets.semantic.semantic_terms == ["cash runway boundary", "burn multiple"]
    assert facets.evidence.supporting_event_ids == row.supporting_event_ids
    assert facets.evidence.confirmed_count == 2
    assert facets.runtime.status == "active"
    assert facets.retrieval.retrieval_count == 3
    assert not facets.prediction.is_prediction
    assert not facets.recommendation.is_recommendation
    assert "projection" not in facets.model_dump(mode="json")


def test_prediction_and_recommendation_facets_are_conditional() -> None:
    prediction = _row(
        proposition={"kind": "prediction", "expected": "renewal closes"},
        proposition_kind="prediction",
        evaluate_at=datetime.now(timezone.utc) + timedelta(days=7),
        resolution_criteria={"kind": "observed_outcome"},
        contributing_models=[uuid7()],
    )
    recommendation = _row(
        proposition={"kind": "recommendation", "target_actor_id": str(uuid7())},
        proposition_kind="recommendation",
        target_actor_id=uuid7(),
    )

    assert model_facet_names(prediction) == (
        "core",
        "semantic",
        "evidence",
        "runtime",
        "retrieval",
        "prediction",
    )
    assert split_model_facets(prediction).prediction.is_prediction
    assert "recommendation" in model_facet_names(recommendation)
    assert split_model_facets(recommendation).recommendation.is_recommendation


def test_model_events_expose_lightweight_facet_metadata() -> None:
    snapshot = model_semantic_snapshot(_row())

    assert snapshot["facet_schema_version"] == MODEL_FACET_SCHEMA_VERSION
    assert snapshot["facet_names"] == [
        "core",
        "semantic",
        "evidence",
        "runtime",
        "retrieval",
    ]
    assert "projection" not in snapshot
