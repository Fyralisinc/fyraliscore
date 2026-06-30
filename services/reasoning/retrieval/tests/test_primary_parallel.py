from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

import services.reasoning.retrieval.primary as primary
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve
from services.reasoning.sage.retrieval_policy import (
    SageRouteUtility,
    build_signal_signature,
    signature_hash,
)


class _FakeAcquire:
    async def __aenter__(self) -> object:
        return _FakeConn()

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _FakePool:
    def __init__(self, max_size: int = 8) -> None:
        self.max_size = max_size

    def get_max_size(self) -> int:
        return self.max_size

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
        assert kwargs["read_pool"] is not None
        assert kwargs["read_fanout_enabled"] is True
        assert kwargs["read_fanout_budget"] is not None
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
            structural_read_fanout_enabled=True,
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
    budget_notes = result.notes["primary_read_fanout_budget"]
    assert budget_notes["max_concurrency"] == 8
    assert budget_notes["acquired"] == 6
    assert budget_notes["denied"] == 0
    assert budget_notes["peak_in_use"] >= 2


@pytest.mark.asyncio
async def test_primary_read_budget_makes_nested_pathway_a_fanout_nonblocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a_started = asyncio.Event()
    b_started = asyncio.Event()
    nested_denied: list[bool] = []

    async def fake_prepare(*_args: object, **_kwargs: object):
        return [], [], None, None

    async def fake_projection(*_args: object, **kwargs: object):
        kwargs["notes"]["projection_context"] = {"enabled": True}
        kwargs["pathway_timings"].append({"stage": "projection_context"})
        return _pathway_result("projection")

    async def fake_a(*_args: object, **kwargs: object):
        a_started.set()
        await b_started.wait()
        async with kwargs["read_fanout_budget"].connection_if_available() as conn:
            nested_denied.append(conn is None)
        kwargs["notes"]["pathways_run"].append("A")
        kwargs["pathway_timings"].append({"stage": "pathway_A"})
        return _pathway_result("A")

    async def fake_b(*_args: object, **kwargs: object):
        await a_started.wait()
        b_started.set()
        await asyncio.sleep(0.02)
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

    async def fake_merge(*_args: object, **_kwargs: object):
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
            read_pool=_FakePool(max_size=2),
            structural_read_fanout_enabled=True,
            config=RetrievalConfig(primary_pathway_parallel_enabled=True),
        ),
        timeout=1.0,
    )

    assert nested_denied == [True]
    assert result.notes["primary_read_fanout_budget"]["max_concurrency"] == 2
    assert result.notes["primary_read_fanout_budget"]["denied"] == 1


@pytest.mark.asyncio
async def test_primary_retrieve_records_sage_policy_and_bounds_semantic_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_id = UUID("00000000-0000-0000-0000-000000000099")
    observed_b_budgets: list[int] = []

    async def fake_prepare(*_args: object, **_kwargs: object):
        return [{"type": "customer", "id": "Alpen"}], [], None, None

    async def fake_projection(*_args: object, **kwargs: object):
        kwargs["notes"]["projection_context"] = {"enabled": True}
        kwargs["pathway_timings"].append({"stage": "projection_context"})
        return _pathway_result("projection")

    async def fake_a(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("A")
        kwargs["pathway_timings"].append({"stage": "pathway_A"})
        return _pathway_result("A")

    async def fake_b(*_args: object, **kwargs: object):
        sage_policy = kwargs["sage_policy"]
        assert sage_policy is not None
        observed_b_budgets.append(sage_policy.budget_for("B", 20))
        kwargs["notes"]["pathways_run"].append("B")
        kwargs["pathway_timings"].append({"stage": "pathway_B"})
        return _pathway_result("B")

    async def fake_l(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("L")
        kwargs["pathway_timings"].append({"stage": "pathway_L"})
        return _pathway_result("L")

    async def fake_d(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("D")
        kwargs["pathway_timings"].append({"stage": "pathway_D"})
        return _pathway_result("D")

    async def fake_g(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("G")
        kwargs["pathway_timings"].append({"stage": "pathway_G"})
        return _pathway_result("G")

    async def fake_merge(*_args: object, **kwargs: object):
        assert [
            result.source_pathway for result in kwargs["pathway_results"]
        ] == ["projection", "A", "B", "L", "D", "G"]
        return [], [], {"goals": [], "commitments": [], "decisions": []}, [], {}

    async def fake_reconsolidate(*_args: object, **kwargs: object):
        return kwargs["models"]

    monkeypatch.setattr(primary, "_prepare_effective_trigger_scope", fake_prepare)
    monkeypatch.setattr(primary, "_run_projection_context", fake_projection)
    monkeypatch.setattr(primary, "_run_pathway_a", fake_a)
    monkeypatch.setattr(primary, "_run_pathway_b", fake_b)
    monkeypatch.setattr(primary, "_run_pathway_l", fake_l)
    monkeypatch.setattr(primary, "_run_pathway_d", fake_d)
    monkeypatch.setattr(primary, "_run_pathway_g", fake_g)
    monkeypatch.setattr(primary, "_merge_primary_results", fake_merge)
    monkeypatch.setattr(primary, "_reconsolidate_primary_models", fake_reconsolidate)

    result = await primary_retrieve(
        TriggerContext(
            kind="T2",
            tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
            model_id=model_id,
            seed_natural_text="Enterprise SSO audit_export renewal blocker",
        ),
        _FakeConn(),
        read_pool=_FakePool(),
        config=RetrievalConfig(
            primary_pathway_parallel_enabled=True,
            semantic_k=20,
        ),
    )

    assert observed_b_budgets == [6]
    policy_notes = result.notes["sage_retrieval_policy"]
    assert policy_notes["enabled"] is True
    assert policy_notes["shadow"] is False
    b_decision = next(
        decision
        for decision in policy_notes["decisions"]
        if decision["path"] == "B"
    )
    assert b_decision["mode"] == "probe"
    assert b_decision["budget"] == 6
    assert "B" in policy_notes["applied_weights"]
    observation = result.notes["sage_retrieval_policy_observation"]
    assert observation["paths_run"] == ["A", "B", "L", "D", "G"]
    assert observation["models"] == 0


@pytest.mark.asyncio
async def test_primary_retrieve_uses_sage_route_utility_to_skip_pathway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_prepare(*_args: object, **_kwargs: object):
        return [], [], None, None

    async def fake_projection(*_args: object, **kwargs: object):
        kwargs["notes"]["projection_context"] = {"enabled": True}
        kwargs["pathway_timings"].append({"stage": "projection_context"})
        return _pathway_result("projection")

    async def fake_a(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("A")
        kwargs["pathway_timings"].append({"stage": "pathway_A"})
        return _pathway_result("A")

    async def fake_b(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("B")
        kwargs["pathway_timings"].append({"stage": "pathway_B"})
        return _pathway_result("B")

    async def fake_l(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("L")
        kwargs["pathway_timings"].append({"stage": "pathway_L"})
        return _pathway_result("L")

    async def fake_c(*_args: object, **_kwargs: object):
        raise AssertionError("pathway C should be suppressed by route utility")

    async def fake_g(*_args: object, **kwargs: object):
        kwargs["notes"]["pathways_run"].append("G")
        kwargs["pathway_timings"].append({"stage": "pathway_G"})
        return _pathway_result("G")

    async def fake_merge(*_args: object, **kwargs: object):
        assert [
            result.source_pathway for result in kwargs["pathway_results"]
        ] == ["projection", "A", "B", "L", "G"]
        return [], [], {"goals": [], "commitments": [], "decisions": []}, [], {}

    async def fake_reconsolidate(*_args: object, **kwargs: object):
        return kwargs["models"]

    trigger = TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_natural_text="Launch dependency status",
    )
    signature = build_signal_signature(trigger=trigger, projection_enabled=True)
    route_utility = SageRouteUtility(
        signature_hash=signature_hash(signature),
        path="C",
        signal_type="T1",
        attempts=6,
        wins=0,
        elapsed_ms_total=7200,
        latency_ms_p95=1300,
        budget_total=120,
        total_cost=5.0,
        total_quality_credit=-0.6,
        utility_score=-0.74,
        confidence=0.55,
    )

    monkeypatch.setattr(primary, "_prepare_effective_trigger_scope", fake_prepare)
    monkeypatch.setattr(primary, "_run_projection_context", fake_projection)
    monkeypatch.setattr(primary, "_run_pathway_a", fake_a)
    monkeypatch.setattr(primary, "_run_pathway_b", fake_b)
    monkeypatch.setattr(primary, "_run_pathway_l", fake_l)
    monkeypatch.setattr(primary, "_run_pathway_c", fake_c)
    monkeypatch.setattr(primary, "_run_pathway_g", fake_g)
    monkeypatch.setattr(primary, "_merge_primary_results", fake_merge)
    monkeypatch.setattr(primary, "_reconsolidate_primary_models", fake_reconsolidate)

    result = await primary_retrieve(
        trigger,
        _FakeConn(),
        read_pool=_FakePool(),
        config=RetrievalConfig(primary_pathway_parallel_enabled=True),
        sage_route_utilities=(route_utility,),
    )

    assert result.notes["pathways_run"] == ["A", "B", "L", "G"]
    assert any(
        item.get("pathway") == "C" and item.get("source") == "sage_retrieval_policy"
        for item in result.notes["pathways_skipped"]
        if isinstance(item, dict)
    )


@pytest.mark.asyncio
async def test_run_pathway_b_uses_sage_policy_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, int] = {}
    trigger = TriggerContext(
        kind="T2",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        model_id=UUID("00000000-0000-0000-0000-000000000099"),
        seed_natural_text="Enterprise SSO audit_export renewal blocker",
    )
    weights = primary._trigger_weights("T2", RetrievalConfig())
    assert weights is not None
    notes = {"pathways_run": [], "pathways_skipped": []}
    pathway_timings: list[dict[str, object]] = []
    _adjusted_weights, sage_policy = primary._plan_sage_primary_policy(
        trigger=trigger,
        cfg=RetrievalConfig(semantic_k=20),
        weights=weights,
        effective_seed_entities=[{"type": "customer", "id": "Alpen"}],
        effective_scope_actors=[],
        notes=notes,
    )
    assert sage_policy is not None

    async def fake_semantic(*_args: object, **kwargs: object) -> PathwayResult:
        observed["semantic_k"] = int(kwargs["k"])
        return PathwayResult(source_pathway="B")

    async def fake_tags(*_args: object, **kwargs: object) -> PathwayResult:
        observed["tag_limit"] = int(kwargs["limit"])
        return PathwayResult(source_pathway="representation_tags")

    monkeypatch.setattr(primary, "pathway_b_semantic", fake_semantic)
    monkeypatch.setattr(primary, "pathway_b_representation_tags", fake_tags)

    result = await primary._run_pathway_b(
        trigger=trigger,
        conn=_FakeConn(),  # type: ignore[arg-type]
        cfg=RetrievalConfig(semantic_k=20),
        embedder=None,
        sage_policy=sage_policy,
        effective_seed_entities=[{"type": "customer", "id": "Alpen"}],
        effective_scope_actors=[],
        t2_model_natural=None,
        t2_model_embedding=None,
        notes=notes,
        pathway_timings=pathway_timings,
    )

    assert result is not None
    assert observed == {"semantic_k": 6, "tag_limit": 20}
    assert notes["pathways_run"] == ["B"]
    assert pathway_timings[0]["stage"] == "pathway_B"
