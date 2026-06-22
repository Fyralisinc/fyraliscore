from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

import services.domain.projections.runtime as runtime
from lib.shared.ids import uuid7
from services.domain.projections.types import ModelEvent, ProjectionSnapshot


class _Projector:
    version = "v1"

    def __init__(
        self,
        name: str,
        *,
        fail_on_event_id=None,
        fail_in_matches: bool = False,
        snapshot_projection_name: str | None = None,
    ) -> None:
        self.name = name
        self.fail_on_event_id = fail_on_event_id
        self.fail_in_matches = fail_in_matches
        self.snapshot_projection_name = snapshot_projection_name
        self.projected_subjects: list[str] = []

    def matches(self, event: ModelEvent) -> bool:
        if self.fail_in_matches:
            raise RuntimeError("match failed")
        return True

    async def affected_subjects(self, conn, event: ModelEvent):
        return [f"{self.name}:{event.model_id}:subject"]

    async def project_subject(
        self,
        conn,
        *,
        tenant_id,
        subject_key: str,
        source_event_ids,
    ) -> ProjectionSnapshot:
        if self.fail_on_event_id in source_event_ids:
            raise RuntimeError("projection failed")
        self.projected_subjects.append(subject_key)
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.snapshot_projection_name or self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload={"status": "active"},
            source_event_ids=tuple(source_event_ids),
        )


def _event(*, tenant_id=None, model_id=None) -> ModelEvent:
    tenant_id = tenant_id or uuid7()
    model_id = model_id or uuid7()
    return ModelEvent(
        id=uuid7(),
        tenant_id=tenant_id,
        model_id=model_id,
        event_type="model.created",
        changed_fields=("proposition",),
        proposition_kind="belief",
        claim_role="concern",
        domain_tags=("runway",),
        scope_entities=(),
        semantic_snapshot={},
        previous_snapshot=None,
        source_event_id=None,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_runner_isolates_failing_projector_and_continues_others(monkeypatch):
    tenant_id = uuid7()
    failed_event = _event(tenant_id=tenant_id)
    ok_event = _event(tenant_id=tenant_id)
    failing = _Projector("bad", fail_on_event_id=failed_event.id)
    ok = _Projector("ok")
    checkpoints: list[tuple[str, Any]] = []
    snapshots: list[ProjectionSnapshot] = []

    async def _fetch(conn, *, tenant_id, projection_name, projection_version, limit):
        del conn, tenant_id, projection_version, limit
        return [failed_event] if projection_name == "bad" else [ok_event]

    async def _checkpoint(conn, *, event, projection_name, projection_version):
        del conn, projection_version
        checkpoints.append((projection_name, event.id))

    async def _snapshot(conn, snapshot):
        del conn
        snapshots.append(snapshot)

    monkeypatch.setattr(runtime, "fetch_pending_events", _fetch)
    monkeypatch.setattr(runtime, "upsert_checkpoint", _checkpoint)
    monkeypatch.setattr(runtime, "upsert_projection_snapshot", _snapshot)

    runner = runtime.ProjectionRunner([failing, ok])
    report = await runner.run_once_detailed(object(), tenant_id=tenant_id)

    assert report.processed_events == 1
    assert report.failed_events == 1
    assert [(error.projection_name, error.event_id) for error in report.errors] == [
        ("bad", failed_event.id)
    ]
    assert checkpoints == [("ok", ok_event.id)]
    assert [snapshot.projection_name for snapshot in snapshots] == ["ok"]

    processed = await runner.run_once(object(), tenant_id=tenant_id)
    assert processed == 1


@pytest.mark.asyncio
async def test_runner_stops_failed_projector_before_later_events(monkeypatch):
    tenant_id = uuid7()
    first = _event(tenant_id=tenant_id)
    second = _event(tenant_id=tenant_id)
    third = _event(tenant_id=tenant_id)
    projector = _Projector("customer_health", fail_on_event_id=second.id)
    checkpoints: list[Any] = []
    snapshots: list[ProjectionSnapshot] = []

    async def _fetch(conn, *, tenant_id, projection_name, projection_version, limit):
        del conn, tenant_id, projection_name, projection_version, limit
        return [first, second, third]

    async def _checkpoint(conn, *, event, projection_name, projection_version):
        del conn, projection_name, projection_version
        checkpoints.append(event.id)

    async def _snapshot(conn, snapshot):
        del conn
        snapshots.append(snapshot)

    monkeypatch.setattr(runtime, "fetch_pending_events", _fetch)
    monkeypatch.setattr(runtime, "upsert_checkpoint", _checkpoint)
    monkeypatch.setattr(runtime, "upsert_projection_snapshot", _snapshot)

    report = await runtime.ProjectionRunner([projector]).run_once_detailed(
        object(),
        tenant_id=tenant_id,
    )

    assert report.processed_events == 1
    assert report.failed_events == 1
    assert checkpoints == [first.id]
    assert [snapshot.source_event_ids for snapshot in snapshots] == [(first.id,)]
    assert projector.projected_subjects == [f"customer_health:{first.model_id}:subject"]


@pytest.mark.asyncio
async def test_runner_does_not_checkpoint_matches_failure(monkeypatch):
    tenant_id = uuid7()
    event = _event(tenant_id=tenant_id)
    projector = _Projector("broken", fail_in_matches=True)
    checkpoints: list[Any] = []

    async def _fetch(conn, *, tenant_id, projection_name, projection_version, limit):
        del conn, tenant_id, projection_name, projection_version, limit
        return [event]

    async def _checkpoint(conn, *, event, projection_name, projection_version):
        del conn, projection_name, projection_version
        checkpoints.append(event.id)

    monkeypatch.setattr(runtime, "fetch_pending_events", _fetch)
    monkeypatch.setattr(runtime, "upsert_checkpoint", _checkpoint)

    report = await runtime.ProjectionRunner([projector]).run_once_detailed(
        object(),
        tenant_id=tenant_id,
    )

    assert report.processed_events == 0
    assert report.failed_events == 1
    assert report.errors[0].message == "RuntimeError: match failed"
    assert checkpoints == []


@pytest.mark.asyncio
async def test_runner_rejects_mismatched_snapshot_without_checkpoint(monkeypatch):
    tenant_id = uuid7()
    event = _event(tenant_id=tenant_id)
    projector = _Projector("customers", snapshot_projection_name="constraints")
    checkpoints: list[Any] = []
    snapshots: list[ProjectionSnapshot] = []

    async def _fetch(conn, *, tenant_id, projection_name, projection_version, limit):
        del conn, tenant_id, projection_name, projection_version, limit
        return [event]

    async def _checkpoint(conn, *, event, projection_name, projection_version):
        del conn, projection_name, projection_version
        checkpoints.append(event.id)

    async def _snapshot(conn, snapshot):
        del conn
        snapshots.append(snapshot)

    monkeypatch.setattr(runtime, "fetch_pending_events", _fetch)
    monkeypatch.setattr(runtime, "upsert_checkpoint", _checkpoint)
    monkeypatch.setattr(runtime, "upsert_projection_snapshot", _snapshot)

    report = await runtime.ProjectionRunner([projector]).run_once_detailed(
        object(),
        tenant_id=tenant_id,
    )

    assert report.processed_events == 0
    assert report.failed_events == 1
    assert "returned projection 'constraints'" in report.errors[0].message
    assert checkpoints == []
    assert snapshots == []


@pytest.mark.asyncio
async def test_runner_surfaces_snapshot_write_failures(monkeypatch):
    tenant_id = uuid7()
    event = _event(tenant_id=tenant_id)
    projector = _Projector("customers")

    async def _fetch(conn, *, tenant_id, projection_name, projection_version, limit):
        del conn, tenant_id, projection_name, projection_version, limit
        return [event]

    async def _snapshot(conn, snapshot):
        del conn, snapshot
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(runtime, "fetch_pending_events", _fetch)
    monkeypatch.setattr(runtime, "upsert_projection_snapshot", _snapshot)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await runtime.ProjectionRunner([projector]).run_once_detailed(
            object(),
            tenant_id=tenant_id,
        )
