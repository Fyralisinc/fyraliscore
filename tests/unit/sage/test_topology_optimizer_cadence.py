from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.sage.topology_optimizer import api, cadence
from services.reasoning.sage.topology_optimizer.types import OptimizationRunReport


def _report(session_id):
    return OptimizationRunReport(
        inquiry_session_id=session_id,
        affordance_reinforces=0,
        affordance_decays=0,
        shortcut_creates_or_bumps=0,
        shortcut_decays=0,
        negative_memory_inserts=0,
        region_refreshes=0,
        question_policy_updates=0,
        canonical_merge_candidates=(),
        canonical_split_candidates=(),
        canonical_promote_candidates=(),
        canonical_demote_candidates=(),
        metrics={"trigger_recognized": 1.0},
    )


def test_normalize_trigger_event_maps_legacy_scheduled_aliases() -> None:
    assert cadence.normalize_trigger_event(None) == cadence.SCHEDULED_TRIGGER
    assert cadence.normalize_trigger_event("") == cadence.SCHEDULED_TRIGGER
    assert cadence.normalize_trigger_event("scheduled") == cadence.SCHEDULED_TRIGGER
    assert (
        cadence.normalize_trigger_event("background_region_scan")
        == cadence.SCHEDULED_TRIGGER
    )
    assert (
        cadence.normalize_trigger_event(cadence.TRIGGER_VALIDATED_DIFF)
        == cadence.TRIGGER_VALIDATED_DIFF
    )


@pytest.mark.asyncio
async def test_run_optimization_pass_delegates_to_optimizer(monkeypatch) -> None:
    tenant_id = uuid4()
    session_id = uuid4()
    seen: dict[str, object] = {}

    class FakeOptimizer:
        def __init__(self, *, pool, tenant_id):
            seen["pool"] = pool
            seen["tenant_id"] = tenant_id

        async def optimize(self, *, inquiry_session_id, trigger_event, conn=None):
            seen["inquiry_session_id"] = inquiry_session_id
            seen["trigger_event"] = trigger_event
            seen["conn"] = conn
            return _report(inquiry_session_id)

    monkeypatch.setattr(cadence, "TopologyOptimizer", FakeOptimizer)

    pool = object()
    conn = object()
    request = cadence.OptimizationCadenceRequest(
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        trigger_event="scheduled",
        source=" route ",
    )

    report = await cadence.run_optimization_pass(
        pool=pool, request=request, conn=conn
    )

    assert report.inquiry_session_id == session_id
    assert request.trigger_event == cadence.SCHEDULED_TRIGGER
    assert request.source == "route"
    assert seen == {
        "pool": pool,
        "tenant_id": tenant_id,
        "inquiry_session_id": session_id,
        "trigger_event": cadence.SCHEDULED_TRIGGER,
        "conn": conn,
    }


@pytest.mark.asyncio
async def test_optimize_topology_wrapper_uses_cadence_request(monkeypatch) -> None:
    tenant_id = uuid4()
    session_id = uuid4()
    captured: dict[str, object] = {}

    async def fake_run_optimization_pass(*, pool, request, conn=None):
        captured["pool"] = pool
        captured["request"] = request
        captured["conn"] = conn
        return _report(request.inquiry_session_id)

    monkeypatch.setattr(api, "run_optimization_pass", fake_run_optimization_pass)

    pool = object()
    conn = object()
    report = await api.optimize_topology(
        pool=pool,
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        trigger_event="scheduled",
        conn=conn,
    )

    request = captured["request"]
    assert report.inquiry_session_id == session_id
    assert isinstance(request, cadence.OptimizationCadenceRequest)
    assert request.tenant_id == tenant_id
    assert request.inquiry_session_id == session_id
    assert request.trigger_event == cadence.SCHEDULED_TRIGGER
    assert captured["pool"] is pool
    assert captured["conn"] is conn
