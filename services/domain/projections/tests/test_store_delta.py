from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.store import (
    dependency_refs_for_snapshot,
    enqueue_projection_refresh_job,
    fetch_events_for_models,
    replace_projection_dependencies,
)
from services.domain.projections.types import (
    ProjectionDependencyRef,
    ProjectionSnapshot,
)


class _FakeConn:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_rows: list[dict[str, Any]] = []

    async def execute(self, query: str, *args: Any) -> None:
        self.execute_calls.append((query, args))

    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> None:
        self.executemany_calls.append((query, args))

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        return self.fetch_rows

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any]:
        self.fetchrow_calls.append((query, args))
        return {"id": args[0]}


def test_dependency_refs_for_snapshot_dedupes_source_and_extra_refs() -> None:
    model_id = uuid7()
    event_id = uuid7()
    snapshot = ProjectionSnapshot(
        tenant_id=uuid7(),
        projection_name="constraints",
        projection_version="v1",
        subject_key="customer:acme",
        payload={},
        source_model_ids=(model_id, model_id),
        source_event_ids=(event_id, event_id),
    )

    refs = dependency_refs_for_snapshot(
        snapshot,
        extra_refs=(
            ProjectionDependencyRef("model", str(model_id), reason="extra"),
            ProjectionDependencyRef("scope_entity", "customer:acme", reason="frontier"),
        ),
    )

    assert [(ref.ref_kind, ref.ref_value, ref.reason) for ref in refs] == [
        ("model", str(model_id), "source_model"),
        ("model_event", str(event_id), "source_event"),
        ("scope_entity", "customer:acme", "frontier"),
    ]


@pytest.mark.asyncio
async def test_enqueue_projection_refresh_job_shapes_deduped_payload() -> None:
    conn = _FakeConn()
    tenant_id = uuid7()
    event_id = uuid7()
    scheduled_at = datetime.now(timezone.utc)

    returned = await enqueue_projection_refresh_job(
        conn,
        tenant_id=tenant_id,
        projection_name=" constraints ",
        projection_version="",
        subject_key=" customer:acme ",
        reason="dependency_delta",
        event_ids=(event_id, event_id),
        dependency_refs=(
            ProjectionDependencyRef("model", "m1", reason="first"),
            ProjectionDependencyRef("model", "m1", reason="duplicate"),
        ),
        payload={"route_reasons": ["dependency_delta"]},
        max_attempts=0,
        scheduled_at=scheduled_at,
    )

    assert conn.fetchrow_calls
    _, args = conn.fetchrow_calls[0]
    assert returned == args[0]
    assert args[1:7] == (
        tenant_id,
        "constraints",
        "v1",
        "customer:acme",
        "dependency_delta",
        [event_id],
    )
    assert json.loads(args[7]) == [
        {
            "metadata": {},
            "reason": "first",
            "ref_kind": "model",
            "ref_value": "m1",
        }
    ]
    assert json.loads(args[8]) == {"route_reasons": ["dependency_delta"]}
    assert args[9] == 1
    assert args[10] == scheduled_at


@pytest.mark.asyncio
async def test_replace_projection_dependencies_replaces_and_bulk_inserts_refs() -> None:
    conn = _FakeConn()
    model_id = uuid7()
    snapshot = ProjectionSnapshot(
        tenant_id=uuid7(),
        projection_name="resources",
        projection_version="v1",
        subject_key="company:default",
        payload={},
        source_model_ids=(model_id,),
    )

    await replace_projection_dependencies(
        conn,
        snapshot,
        extra_refs=(ProjectionDependencyRef("scope_entity", "company:default"),),
    )

    assert len(conn.execute_calls) == 1
    assert conn.execute_calls[0][1] == (
        snapshot.tenant_id,
        "resources",
        "v1",
        "company:default",
    )
    assert len(conn.executemany_calls) == 1
    rows = conn.executemany_calls[0][1]
    assert [(row[4], row[5]) for row in rows] == [
        ("model", str(model_id)),
        ("scope_entity", "company:default"),
    ]


@pytest.mark.asyncio
async def test_fetch_events_for_models_dedupes_model_ids_and_hydrates_rows() -> None:
    conn = _FakeConn()
    tenant_id = uuid7()
    model_id = uuid7()
    event_id = uuid7()
    created_at = datetime.now(timezone.utc)
    conn.fetch_rows = [
        {
            "id": event_id,
            "tenant_id": tenant_id,
            "model_id": model_id,
            "event_type": "model.updated",
            "changed_fields": ["semantic_snapshot"],
            "proposition_kind": "belief",
            "claim_role": "concern",
            "domain_tags": ["runway"],
            "scope_entities": json.dumps([{"type": "company", "id": "acme"}]),
            "semantic_snapshot": json.dumps({"domain_tags": ["runway"]}),
            "previous_snapshot": None,
            "source_event_id": None,
            "created_at": created_at,
        }
    ]

    events = await fetch_events_for_models(
        conn,
        tenant_id=tenant_id,
        model_ids=(model_id, model_id),
        limit=10,
    )

    assert len(events) == 1
    assert events[0].id == event_id
    assert events[0].scope_entities == ({"type": "company", "id": "acme"},)
    _, args = conn.fetch_calls[0]
    assert args == (tenant_id, [model_id], 10)
