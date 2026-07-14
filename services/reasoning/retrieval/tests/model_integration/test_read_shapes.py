from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.shared.db import RowHydrationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.domain.models import repo as models_repo
from services.domain.models.read_shapes import (
    MODEL_ROW_SELECT_COLS,
    MODEL_ROW_SELECT_SQL,
    hydrate_model_row,
)
from services.domain.projections import repo as projections_repo
from services.reasoning.retrieval import pathways


def _selected_name(expr: str) -> str:
    if " AS " in expr:
        return expr.rsplit(" AS ", 1)[1].strip('"')
    return expr.strip('"')


def _base_record(**overrides):
    now = datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc)
    model_id = uuid7()
    tenant_id = uuid7()
    event_id = uuid7()
    record = {
        "id": model_id,
        "tenant_id": tenant_id,
        "born_from_event_id": event_id,
        "proposition": {"kind": "state", "subject": "alice"},
        "natural": "alice owns the renewal risk follow-up",
        "embedding": [0.1, 0.2, 0.3],
        "scope_actors": [],
        "scope_entities": [{"type": "customer", "id": "beacon"}],
        "scope_temporal": {"type": "now"},
        "confidence": 0.62,
        "activation": 1.0,
        "falsifier": {"kind": "observation", "description": "handoff completed"},
        "signal_readings": [{"event_id": str(event_id), "stance": "supports"}],
        "reading_contestable": True,
        "supporting_event_ids": [event_id],
        "supporting_model_ids": [],
        "evidential_weight": 0.7,
        "status": "active",
        "archived_at": None,
        "archive_reason": None,
        "created_at": now,
        "last_retrieved_at": None,
        "retrieval_count": 0,
        "evaluate_at": None,
        "resolution_criteria": {"kind": "manual"},
        "contributing_models": [],
        "visible_to_subjects": True,
        "proposition_kind": "belief",
        "claim_role": None,
        "abstraction_level": None,
        "time_mode": None,
        "modality": None,
        "polarity": None,
        "domain_tags": ["customer_success"],
        "semantic_terms": ["renewal_handoff", "risk_owner"],
        "memory_grammar_version": "v1",
        "confirmed_count": 0,
        "contested_count": 0,
        "last_confirmed_at": None,
        "confidence_at_assertion": 0.62,
        "resolved_at": None,
        "resolution_outcome": None,
        "activation_coefficient": 1.0,
        "target_actor_id": None,
        "caused_act_change_id": None,
    }
    record.update(overrides)
    return record


def test_model_row_select_cols_match_model_row_field_order() -> None:
    assert [_selected_name(expr) for expr in MODEL_ROW_SELECT_COLS] == list(
        ModelRow.model_fields
    )
    assert MODEL_ROW_SELECT_SQL == ", ".join(MODEL_ROW_SELECT_COLS)


def test_model_row_read_shape_is_shared_across_consumers() -> None:
    assert models_repo._SELECT_COLS is MODEL_ROW_SELECT_COLS
    assert models_repo._SELECT_COLS_SQL is MODEL_ROW_SELECT_SQL
    assert pathways._MODEL_SELECT_COLS is MODEL_ROW_SELECT_COLS
    assert pathways._MODEL_SELECT_SQL is MODEL_ROW_SELECT_SQL
    assert projections_repo._MODEL_SELECT_COLS is MODEL_ROW_SELECT_COLS
    assert projections_repo._MODEL_SELECT_SQL is MODEL_ROW_SELECT_SQL


def test_hydrate_model_row_coerces_json_and_vector_codecs() -> None:
    record = _base_record(
        proposition=b'{"kind": "state", "subject": "alice"}',
        scope_entities='[{"type": "customer", "id": "beacon"}]',
        scope_temporal='{"type": "now"}',
        falsifier='{"kind": "observation", "description": "handoff completed"}',
        signal_readings='[{"stance": "supports"}]',
        resolution_criteria='{"kind": "manual"}',
        embedding="[0.1, 0.2, 0.3]",
    )

    row = hydrate_model_row(record)

    assert row.proposition == {"kind": "state", "subject": "alice"}
    assert row.scope_entities == [{"type": "customer", "id": "beacon"}]
    assert row.scope_temporal == {"type": "now"}
    assert row.falsifier == {
        "kind": "observation",
        "description": "handoff completed",
    }
    assert row.signal_readings == [{"stance": "supports"}]
    assert row.resolution_criteria == {"kind": "manual"}
    assert row.embedding == [0.1, 0.2, 0.3]


def test_hydrate_model_row_converts_iterable_vectors_to_float_lists() -> None:
    row = hydrate_model_row(_base_record(embedding=("0.1", "0.2", 3)))

    assert row.embedding == [0.1, 0.2, 3.0]


def test_projection_hydrator_preserves_to_list_vector_compatibility() -> None:
    class VectorWithToList:
        def to_list(self) -> list[float]:
            return [0.4, 0.5, 0.6]

    row = projections_repo._hydrate_model(
        _base_record(embedding=VectorWithToList(), _projection_rank=1)
    )

    assert row.embedding == [0.4, 0.5, 0.6]
    assert not hasattr(row, "_projection_rank")


def test_retrieval_hydrator_strips_private_query_columns() -> None:
    row = pathways._hydrate_model(_base_record(_semantic_rank=0.9))

    assert isinstance(row.id, UUID)
    assert not hasattr(row, "_semantic_rank")


def test_repo_hydrator_wraps_validation_failures() -> None:
    with pytest.raises(RowHydrationError):
        models_repo._hydrate_row(_base_record(embedding="not-json-vector"))
