from __future__ import annotations

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

    async def fetch(self, _sql, *_args):
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
