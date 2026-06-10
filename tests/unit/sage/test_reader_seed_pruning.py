from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.reader import (
    ReaderBudget,
    _CandidateAccumulator,
    _LearnedReadPlan,
    _edge_seed_limit,
    _explicit_seed_ids,
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
