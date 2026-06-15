from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from services.platform.execution import action_cache, inquiry
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import RetrievalAction
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "id": "acme"}],
        scope_actors=[uuid4()],
        seed_natural_text="Acme launch risk",
        seed_occurred_at=datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc),
        max_hops=2,
    )


def _model(
    *,
    model_id: UUID | None = None,
    activation: float = 0.0,
    scope_entities: list[dict[str, str]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=model_id or uuid4(),
        activation=activation,
        scope_entities=scope_entities or [],
    )


def test_action_cache_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._action_seed_entities is action_cache.action_seed_entities
    assert inquiry._action_seed_model_ids is action_cache.action_seed_model_ids
    assert (
        inquiry._bind_action_to_previous_results
        is action_cache.bind_action_to_previous_results
    )
    assert inquiry._clone_pathway_result is action_cache.clone_pathway_result
    assert inquiry._dedupe_seed_entities is action_cache.dedupe_seed_entities
    assert (
        inquiry._retrieval_action_cache_key is action_cache.retrieval_action_cache_key
    )
    assert (
        inquiry._seed_action_cache_from_baseline
        is action_cache.seed_action_cache_from_baseline
    )
    assert (
        inquiry._seed_entities_from_pathway_results
        is action_cache.seed_entities_from_pathway_results
    )
    assert (
        inquiry._seed_model_ids_from_pathway_results
        is action_cache.seed_model_ids_from_pathway_results
    )
    assert inquiry._stable_cache_value is action_cache.stable_cache_value


def test_action_seed_helpers_and_cache_key_are_stable() -> None:
    trigger = _trigger()
    seed_model_id = uuid4()
    action = RetrievalAction(
        "Q1",
        "focused_index",
        "question_answerability_scope",
        filters={
            "seed_entities": [{"type": "commitment", "id": "c1"}, "bad"],
            "seed_model_ids": [str(seed_model_id), "bad"],
            "primitive": "DEPENDENCY",
            "terms": ["soc2-risk-77", "launch"],
        },
        budget=999,
    )
    cfg = InquiryConfig(action_model_budget_limit=80)

    key = action_cache.retrieval_action_cache_key(action, trigger, cfg)

    assert action_cache.action_seed_entities(action, trigger) == [
        {"type": "commitment", "id": "c1"}
    ]
    assert action_cache.action_seed_model_ids(action) == [seed_model_id]
    assert key[0] == "focused_index"
    assert key[1] == 80
    assert key[-2] == "DEPENDENCY"
    assert key[-1] == '["soc2-risk-77","launch"]'


def test_clone_pathway_result_caps_and_preserves_notes() -> None:
    low = _model(activation=0.1)
    high = _model(activation=0.9)
    result = PathwayResult(
        models=[low, high],
        observations=[SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4())],
        acts={"commitments": [SimpleNamespace(id=uuid4())]},
        resources=[SimpleNamespace(id=uuid4())],
        source_pathway="A",
        notes={"source": "baseline"},
    )

    cloned = action_cache.clone_pathway_result(
        result,
        model_limit=1,
        observation_limit=1,
        cap_models_by_activation=True,
        note="baseline_A",
    )

    assert cloned.models == [high]
    assert len(cloned.observations) == 1
    assert cloned.acts == {"commitments": result.acts["commitments"]}
    assert cloned.resources == result.resources
    assert cloned.notes["source"] == "baseline"
    assert cloned.notes["cache_seeded_from"] == "baseline_A"
    assert cloned.notes["models_after_cache_seed_cap"] == 1
    assert cloned.notes["observations_after_cache_seed_cap"] == 1


def test_bind_action_to_previous_results_adds_deduped_scope() -> None:
    trigger = _trigger()
    model = _model(
        scope_entities=[
            {"type": "customer", "id": "acme"},
            {"type": "system", "id": "sso"},
        ]
    )
    commitment_id = uuid4()
    resource_id = uuid4()
    prior = PathwayResult(
        models=[model],
        acts={"commitments": [SimpleNamespace(id=commitment_id)]},
        resources=[SimpleNamespace(id=resource_id)],
    )
    action = RetrievalAction(
        "Q1",
        "semantic",
        "semantic_counterevidence",
        filters={
            "_bind_previous_scope": True,
            "seed_entities": [{"type": "customer", "id": "acme"}],
        },
        budget=5,
    )

    bound = action_cache.bind_action_to_previous_results(action, trigger, [prior])

    assert bound is not action
    assert bound.filters["seed_model_ids"] == [str(model.id)]
    assert bound.filters["seed_entities"] == [
        {"type": "customer", "id": "acme"},
        {"type": "system", "id": "sso"},
        {"type": "resource", "id": str(resource_id)},
        {"type": "commitment", "id": str(commitment_id)},
    ]
    assert bound.filters["_bound_scope"] == {
        "model_count": 1,
        "entity_count": 4,
    }


def test_seed_action_cache_from_baseline_reuses_matching_pathways() -> None:
    trigger = _trigger()
    cfg = InquiryConfig(
        structural_max_hops=2,
        model_edge_max_hops=2,
        action_model_budget_limit=1,
    )
    structural = PathwayResult(
        models=[_model(activation=0.1), _model(activation=0.9)],
        source_pathway="A",
    )
    model_edges = PathwayResult(models=[_model()], source_pathway="G")
    baseline = RetrievalResult(
        trigger=trigger,
        pathway_results=[structural, model_edges],
    )
    cache: dict[tuple[object, ...], PathwayResult] = {}

    notes = action_cache.seed_action_cache_from_baseline(
        cache,
        baseline,
        trigger,
        cfg,
    )

    assert notes == {
        "seeded": 2,
        "paths": ["structural:A", "model_edge:G"],
        "skipped": [],
    }
    assert len(cache) == 2
    assert all(
        result.notes["models_after_cache_seed_cap"] == 1 for result in cache.values()
    )
