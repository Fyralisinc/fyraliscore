from __future__ import annotations

from services.platform.execution import inquiry, question_policy
from services.platform.execution.types import InquiryQuestion, QuestionPolicySignal


def _question(
    question_id: str,
    primitive: str,
    *,
    score: float,
    expected_value: float = 0.7,
    expected_cost: float = 0.2,
    target: str | None = None,
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=f"{primitive}?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=expected_value,
        expected_cost=expected_cost,
        retrieval_target=target or primitive.casefold(),
        stop_condition=f"{primitive.casefold()} found or ruled out",
        score=score,
    )


def test_question_policy_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._apply_question_policy is question_policy.apply_question_policy
    assert inquiry._clamp_float is question_policy.clamp_float
    assert inquiry._policy_budget is question_policy.policy_budget
    assert inquiry._question_information_facets is (
        question_policy.question_information_facets
    )
    assert inquiry._question_marginal_score is question_policy.question_marginal_score
    assert inquiry._question_policy_budget_multiplier is (
        question_policy.question_policy_budget_multiplier
    )
    assert inquiry._question_policy_score_boost is (
        question_policy.question_policy_score_boost
    )
    assert inquiry._question_policy_success_rate is (
        question_policy.question_policy_success_rate
    )
    assert inquiry._question_target_overlap is question_policy.question_target_overlap
    assert inquiry._question_target_tokens is question_policy.question_target_tokens
    assert inquiry._select_questions is question_policy.select_questions


def test_question_marginal_score_penalizes_redundancy() -> None:
    selected = [_question("Q_CONSTRAINT", "CONSTRAINT", score=0.9)]
    redundant = _question("Q_DEP", "DEPENDENCY", score=0.8, target="constraint")
    diverse = _question("Q_OWNER", "OWNERSHIP", score=0.8, target="actor_scope")

    assert question_policy.question_marginal_score(redundant, selected) < (
        question_policy.question_marginal_score(diverse, selected)
    )
    assert question_policy.question_target_tokens(redundant) == {
        "constraint",
        "dependency",
        "out",
    }


def test_apply_question_policy_and_budget_multiplier() -> None:
    question = _question("Q_OWNER", "OWNERSHIP", score=0.5)
    signal = QuestionPolicySignal(
        signal_type="T1",
        question_primitive="OWNERSHIP",
        attempts=10,
        successes=8,
        utility_score=1.5,
        total_credit=15.0,
        total_cost=3.0,
    )

    adjusted = question_policy.apply_question_policy(
        [question],
        question_policy={"OWNERSHIP": signal},
    )[0]

    assert adjusted.score > question.score
    assert adjusted.expected_value > question.expected_value
    assert adjusted.expected_cost < question.expected_cost
    assert 0.65 <= question_policy.question_policy_budget_multiplier(signal) < 1.0
    assert question_policy.policy_budget(100, signal) < 100


def test_select_questions_prioritizes_high_value_questions() -> None:
    candidates = [
        _question("Q_OWNER", "OWNERSHIP", score=0.8, expected_value=0.72),
        _question("Q_CONSTRAINT", "CONSTRAINT", score=0.2, expected_value=0.9),
        _question("Q_LOW", "RECURRENCE", score=0.95, expected_value=0.4),
    ]

    selected = question_policy.select_questions(
        candidates,
        questions_per_round=2,
        round_index=3,
        already_asked=set(),
    )

    assert [question.primitive for question in selected] == [
        "CONSTRAINT",
        "OWNERSHIP",
    ]
    assert {question.round_index for question in selected} == {3}


def test_select_questions_preserves_owner_and_counterevidence_in_bounded_round() -> None:
    candidates = [
        _question("Q_OWNER", "OWNERSHIP", score=0.65, expected_value=0.72),
        _question(
            "Q_COUNTEREVIDENCE",
            "COUNTEREVIDENCE",
            score=0.69,
            expected_value=0.84,
            expected_cost=0.30,
        ),
        _question("Q_CONSTRAINT", "CONSTRAINT", score=0.81, expected_value=0.90),
    ]

    selected = question_policy.select_questions(
        candidates,
        questions_per_round=3,
        round_index=1,
        already_asked=set(),
    )

    assert [question.primitive for question in selected] == [
        "OWNERSHIP",
        "COUNTEREVIDENCE",
    ]
