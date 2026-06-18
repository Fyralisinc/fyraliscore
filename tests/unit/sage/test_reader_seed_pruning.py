from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.reader import (
    ActivatedNode,
    ReaderBudget,
    _CandidateAccumulator,
    _LearnedReadPlan,
    _edge_seed_limit,
    _explicit_seed_ids,
    _protected_counterevidence_node_ids,
)


def test_candidate_accumulator_ranks_edge_seeds_and_protects_explicit_model() -> None:
    explicit = uuid4()
    high = uuid4()
    medium = uuid4()
    low = uuid4()
    suppressed = uuid4()
    candidates = _CandidateAccumulator()
    candidates.add(explicit, 0.01, "explicit:trigger_model", source="explicit")
    candidates.add(high, 0.90, "lexical:3", source="lexical")
    candidates.add(medium, 0.50, "affordance:DEPENDENCY", source="affordance")
    candidates.add(low, 0.05, "lexical:1", source="lexical")
    candidates.add(suppressed, 0.95, "shortcut:x", source="shortcut")
    candidates.suppress(suppressed, "negative_memory:low_value")

    ranked = candidates.ranked_model_ids(limit=3, required_ids={explicit})

    assert ranked == [explicit, high, medium]
    assert suppressed not in ranked
    assert low not in ranked


def test_edge_seed_limit_respects_learned_propagation_cap() -> None:
    assert (
        _edge_seed_limit(
            ReaderBudget(activation_seed_limit=80, propagation_neighbors=120),
            _LearnedReadPlan(propagation_neighbors=32),
        )
        == 32
    )
    assert (
        _edge_seed_limit(
            ReaderBudget(activation_seed_limit=80, propagation_neighbors=48),
            _LearnedReadPlan(),
        )
        == 48
    )


def test_explicit_seed_ids_includes_trigger_and_member_models() -> None:
    model_id = uuid4()
    member_id = uuid4()
    trigger = TriggerContext(
        kind="T6",
        tenant_id=uuid4(),
        model_id=model_id,
        member_model_ids=[member_id],
    )

    assert _explicit_seed_ids(trigger) == {model_id, member_id}


def test_counterevidence_protection_is_capped_and_score_ordered() -> None:
    nodes: list[ActivatedNode] = []
    expected: list = []
    suppressed = uuid4()
    for index in range(30):
        model_id = uuid4()
        reasons = ("counterevidence:lexical",)
        if index == 29:
            model_id = suppressed
            reasons = ("counterevidence:lexical", "negative_memory:low_value")
        nodes.append(
            ActivatedNode(
                model_id=model_id,
                activation_score=float(index) / 100.0,
                activation_reasons=reasons,
                structural_features=None,
            )
        )
        if index not in {29}:
            expected.append((model_id, float(index) / 100.0))

    protected = _protected_counterevidence_node_ids(nodes, max_nodes=40)
    expected_ids = [
        model_id
        for model_id, _score in sorted(expected, key=lambda item: -item[1])[:16]
    ]

    assert list(protected) == expected_ids
    assert suppressed not in protected
