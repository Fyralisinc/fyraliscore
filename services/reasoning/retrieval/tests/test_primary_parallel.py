from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

import services.reasoning.retrieval.primary as primary
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve


class _FakeAcquire:
    async def __aenter__(self) -> object:
        return _FakeConn()

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _FakePool:
    def get_max_size(self) -> int:
        return 8

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire()


class _FakeConn:
    def is_in_transaction(self) -> bool:
        return False


def _pathway_result(name: str) -> PathwayResult:
    return PathwayResult(source_pathway=name)


@pytest.mark.asyncio
async def test_primary_retrieve_fans_out_pathways_and_preserves_merge_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a_started = asyncio.Event()
    b_started = asyncio.Event()
    events: list[str] = []

    async def fake_prepare(*_args: object, **_kwargs: object):
        return [], [], None, None

    async def fake_projection(*_args: object, **kwargs: object):
        kwargs["notes"]["projection_context"] = {"enabled": True}
        kwargs["pathway_timings"].append({"stage": "projection_context"})
        return _pathway_result("projection")

    async def fake_a(*_args: object, **kwargs: object):
        events.append("A_started")
        a_started.set()
        await b_started.wait()
        kwargs["notes"]["pathways_run"].append("A")
        kwargs["pathway_timings"].append({"stage": "pathway_A"})
        return _pathway_result("A")

    async def fake_b(*_args: object, **kwargs: object):
        await a_started.wait()
        events.append("B_started")
        b_started.set()
        kwargs["notes"]["pathways_run"].append("B")
        kwargs["pathway_timings"].append({"stage": "pathway_B"})
        return _pathway_result("B")

    async def fake_l(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("L")
        kwargs["pathway_timings"].append({"stage": "pathway_L"})
        return _pathway_result("L")

    async def fake_c(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("C")
        kwargs["pathway_timings"].append({"stage": "pathway_C"})
        return _pathway_result("C")

    async def fake_g(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("G")
        kwargs["pathway_timings"].append({"stage": "pathway_G"})
        return _pathway_result("G")

    async def fake_merge(*_args: object, **kwargs: object):
        assert [
            result.source_pathway for result in kwargs["pathway_results"]
        ] == ["projection", "A", "B", "L", "C", "G"]
        return [], [], {"goals": [], "commitments": [], "decisions": []}, [], {}

    async def fake_reconsolidate(*_args: object, **kwargs: object):
        return kwargs["models"]

    monkeypatch.setattr(primary, "_prepare_effective_trigger_scope", fake_prepare)
    monkeypatch.setattr(primary, "_run_projection_context", fake_projection)
    monkeypatch.setattr(primary, "_run_pathway_a", fake_a)
    monkeypatch.setattr(primary, "_run_pathway_b", fake_b)
    monkeypatch.setattr(primary, "_run_pathway_l", fake_l)
    monkeypatch.setattr(primary, "_run_pathway_c", fake_c)
    monkeypatch.setattr(primary, "_run_pathway_g", fake_g)
    monkeypatch.setattr(primary, "_merge_primary_results", fake_merge)
    monkeypatch.setattr(primary, "_reconsolidate_primary_models", fake_reconsolidate)

    result = await asyncio.wait_for(
        primary_retrieve(
            TriggerContext(
                kind="T1",
                tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
            ),
            _FakeConn(),
            read_pool=_FakePool(),
            config=RetrievalConfig(primary_pathway_parallel_enabled=True),
        ),
        timeout=1.0,
    )

    assert events == ["A_started", "B_started"]
    assert result.notes["pathways_run"] == ["A", "B", "L", "C", "G"]
    assert [
        timing["stage"] for timing in result.notes["pathway_timings"][:6]
    ] == [
        "projection_context",
        "pathway_A",
        "pathway_B",
        "pathway_L",
        "pathway_C",
        "pathway_G",
    ]
