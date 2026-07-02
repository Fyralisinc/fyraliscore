from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.repo import ProjectionRepo


class _FakeConn:
    def __init__(
        self,
        *,
        fetch_rows: list[list[dict[str, Any]]] | None = None,
        fetchrow_rows: list[dict[str, Any] | None] | None = None,
    ) -> None:
        self.fetch_rows = list(fetch_rows or [])
        self.fetchrow_rows = list(fetchrow_rows or [])
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((sql, args))
        return self.fetch_rows.pop(0) if self.fetch_rows else []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((sql, args))
        return self.fetchrow_rows.pop(0) if self.fetchrow_rows else None


def _projection_row(
    *,
    tenant_id,
    projection_name: str = "constraints",
    subject_key: str = "company:runway",
    source_model_ids: list[Any] | None = None,
    source_event_ids: Any = None,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "projection_name": projection_name,
        "projection_version": "v1",
        "subject_key": subject_key,
        "payload": json.dumps({"kind": "projection", "subject_key": subject_key}),
        "confidence": 0.82,
        "severity": "high",
        "source_model_ids": source_model_ids or [],
        "source_event_ids": source_event_ids or [],
        "updated_at": datetime.now(timezone.utc),
    }


def _model_row(*, tenant_id, model_id, born_from_event_id=None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    born_from_event_id = born_from_event_id or uuid7()
    return {
        "id": model_id,
        "tenant_id": tenant_id,
        "born_from_event_id": born_from_event_id,
        "proposition": json.dumps(
            {"kind": "belief", "assertion": "Runway pressure is increasing."}
        ),
        "natural": "Runway pressure is increasing.",
        "embedding": "[0.1, 0.2]",
        "scope_actors": [],
        "scope_entities": [],
        "scope_temporal": json.dumps({"type": "current"}),
        "confidence": 0.82,
        "activation": 1.0,
        "falsifier": None,
        "signal_readings": [],
        "reading_contestable": True,
        "supporting_event_ids": [],
        "supporting_model_ids": [],
        "evidential_weight": 0.5,
        "status": "active",
        "archived_at": None,
        "archive_reason": None,
        "created_at": now,
        "last_retrieved_at": None,
        "retrieval_count": 0,
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
        "domain_tags": ["runway", "financial_capacity"],
        "memory_grammar_version": "v1",
        "confirmed_count": 0,
        "contested_count": 0,
        "last_confirmed_at": None,
        "confidence_at_assertion": 0.82,
        "resolved_at": None,
        "resolution_outcome": None,
        "activation_coefficient": 1.0,
        "target_actor_id": None,
        "caused_act_change_id": None,
    }


@pytest.mark.asyncio
async def test_list_snapshots_for_subjects_normalizes_dedupes_and_hydrates() -> None:
    tenant_id = uuid7()
    model_id = uuid7()
    event_id = uuid7()
    conn = _FakeConn(
        fetch_rows=[
            [
                _projection_row(
                    tenant_id=tenant_id,
                    source_model_ids=[str(model_id)],
                    source_event_ids=json.dumps([str(event_id)]),
                )
            ]
        ]
    )

    snapshots = await ProjectionRepo().list_snapshots_for_subjects(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        subjects=[
            (" constraints ", " company:runway "),
            ("constraints", "company:runway"),
            ("", "missing-projection"),
            ("resources", ""),
        ],
        limit=10,
    )

    assert conn.fetch_calls[0][1][1:4] == (
        ["constraints"],
        ["company:runway"],
        "v1",
    )
    assert len(snapshots) == 1
    assert snapshots[0].payload == {
        "kind": "projection",
        "subject_key": "company:runway",
    }
    assert snapshots[0].source_model_ids == (model_id,)
    assert snapshots[0].source_event_ids == (event_id,)


@pytest.mark.asyncio
async def test_load_models_by_id_dedupes_and_preserves_requested_order() -> None:
    tenant_id = uuid7()
    first_id = uuid7()
    second_id = uuid7()
    conn = _FakeConn(
        fetch_rows=[
            [
                _model_row(tenant_id=tenant_id, model_id=second_id),
                _model_row(tenant_id=tenant_id, model_id=first_id),
            ]
        ]
    )

    models = await ProjectionRepo().load_models_by_id(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        model_ids=[first_id, second_id, first_id],
    )

    assert conn.fetch_calls[0][1][1] == [first_id, second_id]
    assert [model.id for model in models] == [first_id, second_id]
    assert models[0].proposition["assertion"] == "Runway pressure is increasing."
    assert models[0].embedding == [0.1, 0.2]


@pytest.mark.asyncio
async def test_list_staleness_reports_current_when_no_model_events() -> None:
    tenant_id = uuid7()
    conn = _FakeConn(fetchrow_rows=[None])

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["resources", "resources", ""],
    )

    assert len(staleness) == 1
    assert staleness[0].projection_name == "resources"
    assert staleness[0].is_stale is False
    assert staleness[0].reason == "no_model_events"
    assert conn.fetch_calls == []


@pytest.mark.asyncio
async def test_list_staleness_batches_names_and_preserves_requested_order() -> None:
    tenant_id = uuid7()
    latest_event_id = uuid7()
    checkpoint_event_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = _FakeConn(
        fetchrow_rows=[{"id": latest_event_id, "created_at": now}],
        fetch_rows=[
            [
                {
                    "projection_name": "resources",
                    "last_processed_event_id": None,
                    "last_processed_event_created_at": None,
                    "updated_at": None,
                },
                {
                    "projection_name": "constraints",
                    "last_processed_event_id": checkpoint_event_id,
                    "last_processed_event_created_at": datetime(
                        2026,
                        1,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    "updated_at": now,
                },
            ]
        ],
    )

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["resources", "constraints", "resources"],
    )

    assert conn.fetch_calls[0][1][1] == ["resources", "constraints"]
    assert "EXISTS" not in conn.fetch_calls[0][0]
    assert "FROM model_events e" not in conn.fetch_calls[0][0]
    assert [(entry.projection_name, entry.reason) for entry in staleness] == [
        ("resources", "no_snapshot"),
        ("constraints", "pending_model_events"),
    ]
    assert all(entry.latest_model_event_id == latest_event_id for entry in staleness)


@pytest.mark.asyncio
async def test_list_staleness_marks_checkpoint_current_without_event_scan() -> None:
    tenant_id = uuid7()
    latest_event_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = _FakeConn(
        fetchrow_rows=[{"id": latest_event_id, "created_at": now}],
        fetch_rows=[
            [
                {
                    "projection_name": "resources",
                    "last_processed_event_id": latest_event_id,
                    "last_processed_event_created_at": now,
                    "updated_at": now,
                },
            ]
        ],
    )

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["resources"],
    )

    assert len(staleness) == 1
    assert staleness[0].is_stale is False
    assert staleness[0].reason == "current"
    assert staleness[0].freshness_mode == "checkpoint"
    assert "model_events e" not in conn.fetch_calls[0][0]


@pytest.mark.asyncio
async def test_list_staleness_marks_delta_queue_current_without_checkpoint() -> None:
    tenant_id = uuid7()
    latest_event_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = _FakeConn(
        fetchrow_rows=[{"id": latest_event_id, "created_at": now}],
        fetch_rows=[
            [
                {
                    "projection_name": "customers",
                    "last_processed_event_id": None,
                    "last_processed_event_created_at": None,
                    "updated_at": None,
                    "snapshot_count": 3,
                    "pending_refresh_jobs": 0,
                    "failed_refresh_jobs": 0,
                },
            ]
        ],
    )

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["customers"],
    )

    assert len(staleness) == 1
    assert staleness[0].is_stale is False
    assert staleness[0].reason == "delta_queue_current"
    assert staleness[0].freshness_mode == "delta_queue"
    assert staleness[0].snapshot_count == 3


@pytest.mark.asyncio
async def test_list_staleness_marks_pending_delta_jobs_stale() -> None:
    tenant_id = uuid7()
    latest_event_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = _FakeConn(
        fetchrow_rows=[{"id": latest_event_id, "created_at": now}],
        fetch_rows=[
            [
                {
                    "projection_name": "commitments",
                    "last_processed_event_id": None,
                    "last_processed_event_created_at": None,
                    "updated_at": None,
                    "snapshot_count": 2,
                    "pending_refresh_jobs": 1,
                    "failed_refresh_jobs": 0,
                },
            ]
        ],
    )

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["commitments"],
    )

    assert staleness[0].is_stale is True
    assert staleness[0].reason == "pending_refresh_jobs"
    assert staleness[0].pending_refresh_jobs == 1


@pytest.mark.asyncio
async def test_list_staleness_marks_failed_delta_jobs_stale() -> None:
    tenant_id = uuid7()
    latest_event_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = _FakeConn(
        fetchrow_rows=[{"id": latest_event_id, "created_at": now}],
        fetch_rows=[
            [
                {
                    "projection_name": "decisions",
                    "last_processed_event_id": None,
                    "last_processed_event_created_at": None,
                    "updated_at": None,
                    "snapshot_count": 2,
                    "pending_refresh_jobs": 0,
                    "failed_refresh_jobs": 1,
                },
            ]
        ],
    )

    staleness = await ProjectionRepo().list_staleness(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        projection_names=["decisions"],
    )

    assert staleness[0].is_stale is True
    assert staleness[0].reason == "failed_refresh_jobs"
    assert staleness[0].failed_refresh_jobs == 1
