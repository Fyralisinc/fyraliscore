from __future__ import annotations

from typing import Any

import pytest

from lib.shared.ids import uuid7
from services.domain.correction_propagation import projections as projections_module
from services.domain.correction_propagation.projections import (
    ProjectionCorrectionAdapter,
)
from services.domain.correction_propagation.relations import (
    RelationCorrectionAdapter,
)


pytestmark = pytest.mark.asyncio


class _RelationConnection:
    def __init__(self, frame_rows, projection_rows) -> None:
        self.frame_rows = frame_rows
        self.projection_rows = projection_rows
        self.executed = []
        self.fetch_count = 0

    async def fetch(self, sql, *args):
        self.fetch_count += 1
        if "FROM relation_instances" in sql:
            return self.frame_rows
        if "UPDATE relation_edge_projections" in sql:
            return self.projection_rows
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "UPDATE 1"


async def test_relation_adapter_retires_invalid_and_reviews_mixed_frames() -> None:
    old_model_id = uuid7()
    surviving_model_id = uuid7()
    retired_relation_id = uuid7()
    review_relation_id = uuid7()
    projection_id = uuid7()
    conn = _RelationConnection(
        [
            {
                "id": retired_relation_id,
                "status": "accepted",
                "evidence_model_ids": [old_model_id],
                "participant_model_ids": [old_model_id],
            },
            {
                "id": review_relation_id,
                "status": "accepted",
                "evidence_model_ids": [old_model_id, surviving_model_id],
                "participant_model_ids": [surviving_model_id],
            },
        ],
        [{"id": projection_id}],
    )

    report = await RelationCorrectionAdapter().fence_for_models(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid7(),
        contaminated_model_ids=(old_model_id,),
        cause_event_id=uuid7(),
    )

    assert report.retired_relation_ids == (retired_relation_id,)
    assert report.needs_review_relation_ids == (review_relation_id,)
    assert report.retired_projection_ids == (projection_id,)
    assert [args[2] for _sql, args in conn.executed] == [
        "retired",
        "needs_review",
    ]


class _ProjectionConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.executed = []
        self.fetches = []

    async def fetch(self, sql, *args):
        self.fetches.append((sql, args))
        return self.rows

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "DELETE 1"


async def test_projection_adapter_enqueues_then_removes_contaminated_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_model_id = uuid7()
    tenant_id = uuid7()
    job_id = uuid7()
    conn = _ProjectionConnection(
        [
            {
                "projection_name": "customers",
                "projection_version": "v1",
                "subject_key": "customer:nimbus",
            }
        ]
    )
    enqueue_calls = []

    async def _enqueue(_conn, **kwargs):
        enqueue_calls.append(kwargs)
        return job_id

    monkeypatch.setattr(
        projections_module,
        "enqueue_projection_refresh_job",
        _enqueue,
    )
    report = await ProjectionCorrectionAdapter().invalidate_for_models(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        contaminated_model_ids=(old_model_id,),
        cause_event_id=uuid7(),
    )

    assert report.refresh_job_ids == (job_id,)
    assert enqueue_calls[0]["reason"] == "dependency_delta"
    assert enqueue_calls[0]["payload"]["correction_kind"] == "grounding_corrected"
    assert len(conn.executed) == 2
    assert "projection_dependencies" in conn.executed[0][0]
    assert "projection_snapshots" in conn.executed[1][0]


class _ReferentProjectionConnection(_ProjectionConnection):
    def __init__(self, *, model_rows, projection_rows) -> None:
        super().__init__(projection_rows)
        self.model_rows = model_rows

    async def fetch(self, sql, *args):
        self.fetches.append((sql, args))
        if "FROM model_scope_entities" in sql:
            return self.model_rows
        if "FROM projection_snapshots" in sql:
            return self.rows
        raise AssertionError(sql)


async def test_projection_adapter_invalidates_exact_resource_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid7()
    resource_id = uuid7()
    first_model_id = uuid7()
    second_model_id = uuid7()
    cause_event_id = uuid7()
    job_id = uuid7()
    conn = _ReferentProjectionConnection(
        model_rows=[
            {"model_id": second_model_id},
            {"model_id": first_model_id},
            {"model_id": second_model_id},
        ],
        projection_rows=[
            {
                "projection_name": "resources",
                "projection_version": "v1",
                "subject_key": f"resource:{resource_id}",
            }
        ],
    )
    enqueue_calls: list[dict[str, Any]] = []

    async def _enqueue(_conn, **kwargs):
        enqueue_calls.append(kwargs)
        return job_id

    monkeypatch.setattr(
        projections_module,
        "enqueue_projection_refresh_job",
        _enqueue,
    )

    report = await ProjectionCorrectionAdapter().invalidate_for_canonical_referent(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(resource_id),
        cause_event_id=cause_event_id,
    )

    expected_model_ids = tuple(sorted((first_model_id, second_model_id)))
    discovery_sql, discovery_args = conn.fetches[0]
    assert "JOIN models AS model" in discovery_sql
    assert "scope.tenant_id=$1" in discovery_sql
    assert "model.tenant_id=$1" in discovery_sql
    assert "model.status='active'" in discovery_sql
    assert discovery_args == (tenant_id, "resource", resource_id)
    assert conn.fetches[1][1] == (
        tenant_id,
        list(expected_model_ids),
        [str(model_id) for model_id in expected_model_ids],
    )
    assert report.refresh_job_ids == (job_id,)
    assert enqueue_calls == [
        {
            "tenant_id": tenant_id,
            "projection_name": "resources",
            "projection_version": "v1",
            "subject_key": f"resource:{resource_id}",
            "reason": "dependency_delta",
            "event_ids": (cause_event_id,),
            "payload": {
                "correction_kind": "canonical_referent_replaced",
                "canonical_referent": {
                    "type": "resource",
                    "id": str(resource_id),
                },
                "scoped_model_ids": [
                    str(model_id) for model_id in expected_model_ids
                ],
            },
        }
    ]
    assert len(conn.executed) == 2


@pytest.mark.parametrize(
    ("referent_type", "referent_id"),
    [
        ("customer", "00000000-0000-0000-0000-000000000001"),
        ("resource", "not-a-uuid"),
        ("resource", "00000000000000000000000000000001"),
        (" resource", "00000000-0000-0000-0000-000000000001"),
    ],
)
async def test_projection_adapter_fails_closed_for_unsupported_referent(
    referent_type: str,
    referent_id: str,
) -> None:
    conn = _ReferentProjectionConnection(model_rows=[], projection_rows=[])

    report = await ProjectionCorrectionAdapter().invalidate_for_canonical_referent(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid7(),
        canonical_referent_type=referent_type,
        canonical_referent_id=referent_id,
        cause_event_id=uuid7(),
    )

    assert report.invalidated_subjects == ()
    assert report.refresh_job_ids == ()
    assert conn.fetches == []
    assert conn.executed == []


async def test_projection_adapter_does_nothing_without_active_scoped_models() -> None:
    tenant_id = uuid7()
    resource_id = uuid7()
    conn = _ReferentProjectionConnection(model_rows=[], projection_rows=[])

    report = await ProjectionCorrectionAdapter().invalidate_for_canonical_referent(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=resource_id,
        cause_event_id=uuid7(),
    )

    assert report.invalidated_subjects == ()
    assert report.refresh_job_ids == ()
    assert len(conn.fetches) == 1
    assert conn.executed == []
