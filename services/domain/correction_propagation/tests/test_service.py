from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.shared.ids import uuid7
from services.domain.correction_propagation import service as service_module
from services.domain.correction_propagation.service import (
    CorrectionPropagationService,
)


pytestmark = pytest.mark.asyncio


class _Connection:
    def __init__(self, *, old_rows, dependency_rows) -> None:
        self.old_rows = old_rows
        self.dependency_rows = dependency_rows
        self.fetches: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.fetches.append((sql, args))
        if "FROM source_semantic_interpretations" in sql:
            return self.old_rows
        if "WITH dependency_pairs AS" in sql:
            return self.dependency_rows
        raise AssertionError(f"unexpected SQL: {sql}")


class _Models:
    def __init__(self, *, changed_dependents=()) -> None:
        self.changed_dependents = set(changed_dependents)
        self.calls: list[tuple[str, object]] = []

    async def fence_for_correction(
        self,
        model_id,
        *,
        tenant_id,
        cause_event_id,
        cause_model_id,
        conn,
    ):
        self.calls.append(
            (
                "fence",
                (model_id, tenant_id, cause_event_id, cause_model_id, conn),
            )
        )
        if model_id in self.changed_dependents:
            return SimpleNamespace(id=model_id)
        return None

    async def archive(
        self,
        model_id,
        reason,
        *,
        cause_event_id,
        conn,
    ):
        self.calls.append(
            ("archive", (model_id, reason, cause_event_id, conn))
        )
        return SimpleNamespace(id=model_id)


async def test_active_predecessor_models_are_fenced_queued_then_archived(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid7()
    predecessor_trace_id = uuid7()
    successor_trace_id = uuid7()
    observation_id = uuid7()
    corrected_model_id = uuid7()
    old_model_id = uuid7()
    dependent_model_id = uuid7()
    conn = _Connection(
        old_rows=[{"id": old_model_id, "status": "active"}],
        dependency_rows=[
            {
                "dependent_model_id": dependent_model_id,
                "cause_model_id": old_model_id,
            }
        ],
    )
    models = _Models(changed_dependents=(dependent_model_id,))
    reeval_calls: list[dict[str, object]] = []

    async def _enqueue(_conn, **kwargs):
        reeval_calls.append(kwargs)
        models.calls.append(("reeval", kwargs))
        return uuid7()

    monkeypatch.setattr(service_module, "enqueue_model_reeval", _enqueue)
    service = CorrectionPropagationService(models_repo=models)  # type: ignore[arg-type]

    report = await service.propagate_direct_correction(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        predecessor_grounding_trace_id=predecessor_trace_id,
        successor_grounding_trace_id=successor_trace_id,
        cause_event_id=observation_id,
        corrected_model_id=corrected_model_id,
    )

    assert report.old_model_ids == (old_model_id,)
    assert report.archived_model_ids == (old_model_id,)
    assert report.dependent_model_ids == (dependent_model_id,)
    assert report.newly_fenced_model_ids == (dependent_model_id,)
    assert report.reeval_pairs == ((dependent_model_id, old_model_id),)
    assert reeval_calls == [
        {
            "tenant_id": tenant_id,
            "model_id": dependent_model_id,
            "cause_model_id": old_model_id,
            "cause_kind": "grounding_corrected",
        }
    ]
    assert [kind for kind, _payload in models.calls] == [
        "fence",
        "reeval",
        "archive",
    ]


async def test_replay_of_completed_fence_does_not_create_new_repair_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid7()
    old_model_id = uuid7()
    dependent_model_id = uuid7()
    conn = _Connection(
        old_rows=[{"id": old_model_id, "status": "archived"}],
        dependency_rows=[
            {
                "dependent_model_id": dependent_model_id,
                "cause_model_id": old_model_id,
            }
        ],
    )
    models = _Models()
    reeval_calls: list[dict[str, object]] = []

    async def _enqueue(_conn, **kwargs):
        reeval_calls.append(kwargs)
        return uuid7()

    monkeypatch.setattr(service_module, "enqueue_model_reeval", _enqueue)
    service = CorrectionPropagationService(models_repo=models)  # type: ignore[arg-type]

    report = await service.propagate_direct_correction(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        predecessor_grounding_trace_id=uuid7(),
        successor_grounding_trace_id=uuid7(),
        cause_event_id=uuid7(),
        corrected_model_id=uuid7(),
    )

    assert report.old_model_ids == (old_model_id,)
    assert report.archived_model_ids == ()
    assert report.newly_fenced_model_ids == ()
    assert report.reeval_pairs == ()
    assert reeval_calls == []
    assert [kind for kind, _payload in models.calls] == ["fence"]


async def test_non_correction_trace_is_a_noop() -> None:
    conn = _Connection(old_rows=[], dependency_rows=[])
    models = _Models()
    service = CorrectionPropagationService(models_repo=models)  # type: ignore[arg-type]

    report = await service.propagate_direct_correction(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid7(),
        predecessor_grounding_trace_id=None,
        successor_grounding_trace_id=uuid7(),
        cause_event_id=uuid7(),
        corrected_model_id=None,
    )

    assert report.correction_found is False
    assert conn.fetches == []
    assert models.calls == []
