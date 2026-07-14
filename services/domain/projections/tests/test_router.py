from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

import services.domain.projections.router as router
from lib.shared.ids import uuid7
from services.domain.projections.types import (
    ModelEvent,
    ProjectionSubjectRef,
)


class _Projector:
    version = "v1"

    def __init__(
        self,
        name: str,
        *,
        subject_keys: tuple[str, ...] = (),
        matched: bool = True,
        fail: bool = False,
    ) -> None:
        self.name = name
        self.subject_keys = subject_keys
        self.matched = matched
        self.fail = fail

    def matches(self, event: ModelEvent) -> bool:
        del event
        if self.fail:
            raise RuntimeError("match exploded")
        return self.matched

    async def affected_subjects(self, conn: Any, event: ModelEvent) -> tuple[str, ...]:
        del conn, event
        return self.subject_keys

    async def project_subject(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("router should not project subjects")


def _event() -> ModelEvent:
    return ModelEvent(
        id=uuid7(),
        tenant_id=uuid7(),
        model_id=uuid7(),
        event_type="model.updated",
        changed_fields=("semantic_snapshot", "semantic_snapshot"),
        proposition_kind="Belief",
        claim_role="Concern",
        domain_tags=("Runway", "runway"),
        scope_entities=({"type": "Customer", "id": "acme"},),
        semantic_snapshot={},
        previous_snapshot=None,
        source_event_id=uuid7(),
        created_at=datetime.now(timezone.utc),
    )


def test_event_dependency_refs_and_watch_keys_are_normalized_and_deduped() -> None:
    event = _event()

    refs = router.dependency_refs_for_event(event)
    watch_keys = router.watch_keys_for_event(event)

    assert [(ref.ref_kind, ref.ref_value, ref.reason) for ref in refs] == [
        ("model", str(event.model_id), "changed_model"),
        ("model_event", str(event.id), "changed_event"),
        ("model_event", str(event.source_event_id), "source_event"),
    ]
    assert (
        "domain_tag",
        "runway",
    ) in {(key.watch_kind, key.watch_value) for key in watch_keys}
    assert (
        "scope_entity",
        "customer:acme",
    ) in {(key.watch_kind, key.watch_value) for key in watch_keys}
    assert len(watch_keys) == len(
        {(key.watch_kind, key.watch_value) for key in watch_keys}
    )


@pytest.mark.asyncio
async def test_router_enqueues_deduped_dependency_watch_and_direct_jobs(
    monkeypatch,
) -> None:
    event = _event()
    enqueued: list[dict[str, Any]] = []

    async def fake_dependency_lookup(conn, *, tenant_id, ref_kind, ref_value, limit):
        del conn, tenant_id, limit
        if (ref_kind, ref_value) == ("model", str(event.model_id)):
            return [ProjectionSubjectRef("constraints", "v1", "customer:acme")]
        return []

    async def fake_watch_lookup(conn, *, tenant_id, watch_kind, watch_value, limit):
        del conn, tenant_id, limit
        if (watch_kind, watch_value) == ("domain_tag", "runway"):
            return [ProjectionSubjectRef("constraints", "v1", "customer:acme")]
        if (watch_kind, watch_value) == ("claim_role", "concern"):
            return [ProjectionSubjectRef("resources", "v1", "company:default")]
        return []

    async def fake_enqueue(conn, **kwargs):
        del conn
        job_id = uuid7()
        enqueued.append({"id": job_id, **kwargs})
        return job_id

    monkeypatch.setattr(router, "list_projection_subjects_for_dependency", fake_dependency_lookup)
    monkeypatch.setattr(router, "list_projection_subjects_for_watch_key", fake_watch_lookup)
    monkeypatch.setattr(router, "enqueue_projection_refresh_job", fake_enqueue)

    report = await router.enqueue_refreshes_for_event(
        object(),
        event,
        [
            _Projector("constraints", subject_keys=("customer:acme",)),
            _Projector("resources", matched=False),
        ],
    )

    assert report.event_id == event.id
    assert report.direct_matches == 1
    assert report.dependency_matches == 1
    assert report.watch_matches == 2
    assert len(report.enqueued_jobs) == 2

    by_subject = {job["subject_key"]: job for job in enqueued}
    constraints_job = by_subject["customer:acme"]
    assert constraints_job["projection_name"] == "constraints"
    assert constraints_job["reason"] == "event_match"
    assert constraints_job["event_ids"] == (event.id,)
    assert constraints_job["payload"]["route_reasons"] == [
        "dependency_delta",
        "event_match",
        "watch_delta",
    ]
    assert [(ref.ref_kind, ref.ref_value) for ref in constraints_job["dependency_refs"]] == [
        ("model", str(event.model_id)),
        ("model_event", str(event.id)),
        ("model_event", str(event.source_event_id)),
    ]

    resources_job = by_subject["company:default"]
    assert resources_job["projection_name"] == "resources"
    assert resources_job["reason"] == "watch_delta"
    assert resources_job["payload"]["route_reasons"] == ["watch_delta"]


@pytest.mark.asyncio
async def test_router_filters_dependency_and_watch_hits_to_selected_projectors(
    monkeypatch,
) -> None:
    event = _event()
    enqueued: list[dict[str, Any]] = []

    async def fake_dependency_lookup(conn, *, tenant_id, ref_kind, ref_value, limit):
        del conn, tenant_id, ref_kind, ref_value, limit
        return [ProjectionSubjectRef("employee_profiles", "v1", "employee:1:profile")]

    async def fake_watch_lookup(conn, *, tenant_id, watch_kind, watch_value, limit):
        del conn, tenant_id, watch_kind, watch_value, limit
        return [ProjectionSubjectRef("resources", "v1", "company:capacity")]

    async def fake_enqueue(conn, **kwargs):
        del conn
        enqueued.append(kwargs)
        return uuid7()

    monkeypatch.setattr(router, "list_projection_subjects_for_dependency", fake_dependency_lookup)
    monkeypatch.setattr(router, "list_projection_subjects_for_watch_key", fake_watch_lookup)
    monkeypatch.setattr(router, "enqueue_projection_refresh_job", fake_enqueue)

    report = await router.enqueue_refreshes_for_event(
        object(),
        event,
        [_Projector("constraints", matched=False)],
    )

    assert report.enqueued_jobs == ()
    assert report.dependency_matches > 0
    assert report.watch_matches > 0
    assert enqueued == []


@pytest.mark.asyncio
async def test_router_isolates_projector_match_failures(monkeypatch) -> None:
    event = _event()

    async def empty_lookup(*args: Any, **kwargs: Any) -> list[ProjectionSubjectRef]:
        return []

    async def fake_enqueue(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("no jobs should be enqueued")

    monkeypatch.setattr(router, "list_projection_subjects_for_dependency", empty_lookup)
    monkeypatch.setattr(router, "list_projection_subjects_for_watch_key", empty_lookup)
    monkeypatch.setattr(router, "enqueue_projection_refresh_job", fake_enqueue)

    report = await router.enqueue_refreshes_for_event(
        object(),
        event,
        [_Projector("bad", fail=True), _Projector("miss", matched=False)],
    )

    assert report.enqueued_jobs == ()
    assert report.direct_matches == 0
    assert [(error.projection_name, error.stage, error.message) for error in report.errors] == [
        ("bad", "match", "RuntimeError: match exploded")
    ]
