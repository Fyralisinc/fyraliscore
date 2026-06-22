"""Question policy and selection helpers for adaptive inquiry execution."""

from __future__ import annotations

import re
from dataclasses import replace

from .types import InquiryQuestion, QuestionPolicySignal

QUESTION_MARGINAL_MIN_SCORE = 0.52
QUESTION_PRIORITY_MARGINAL_MIN_SCORE = 0.46


def clamp_float(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def question_marginal_score(
    question: InquiryQuestion,
    selected: list[InquiryQuestion],
) -> float:
    score = float(question.score)
    if not selected:
        return round(score, 4)

    selected_hypotheses = {
        hypothesis for prior in selected for hypothesis in prior.tests_hypotheses
    }
    shared_hypotheses = set(question.tests_hypotheses) & selected_hypotheses
    if shared_hypotheses:
        score -= min(0.16, 0.07 * len(shared_hypotheses))

    selected_facets = {
        facet for prior in selected for facet in question_information_facets(prior)
    }
    shared_facets = question_information_facets(question) & selected_facets
    if shared_facets:
        score -= min(0.18, 0.08 * len(shared_facets))

    target_overlap = question_target_overlap(question, selected)
    if target_overlap >= 0.50:
        score -= 0.12
    elif target_overlap >= 0.25:
        score -= 0.06

    score -= min(0.12, max(0.0, question.expected_cost - 0.22) * 0.35)
    if question.primitive == "COUNTEREVIDENCE":
        score += 0.06
    return round(score, 4)


def question_information_facets(question: InquiryQuestion) -> set[str]:
    primitive = question.primitive
    if primitive == "DEPENDENCY":
        return {"critical_path", "dependency"}
    if primitive == "COMMITMENT":
        return {"commitment", "promise"}
    if primitive == "CONSTRAINT":
        return {"constraint", "resource", "dependency"}
    if primitive == "COUNTEREVIDENCE":
        return {"counterevidence", "falsification"}
    if primitive == "OWNERSHIP":
        return {"ownership", "actor"}
    if primitive == "GOAL_IMPACT":
        return {"goal", "impact", "customer"}
    if primitive == "RECURRENCE":
        return {"recurrence", "pattern"}
    return {primitive.casefold()}


def question_target_overlap(
    question: InquiryQuestion,
    selected: list[InquiryQuestion],
) -> float:
    tokens = question_target_tokens(question)
    if not tokens:
        return 0.0
    max_overlap = 0.0
    for prior in selected:
        prior_tokens = question_target_tokens(prior)
        if not prior_tokens:
            continue
        overlap = len(tokens & prior_tokens) / max(len(tokens), 1)
        max_overlap = max(max_overlap, overlap)
    return max_overlap


def question_target_tokens(question: InquiryQuestion) -> set[str]:
    text = f"{question.retrieval_target} {question.stop_condition}".casefold()
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", text)
        if len(token) > 2
        and token not in {"and", "the", "for", "with", "found", "ruled"}
    }


def apply_question_policy(
    candidates: list[InquiryQuestion],
    *,
    question_policy: dict[str, QuestionPolicySignal],
) -> list[InquiryQuestion]:
    if not question_policy:
        return candidates
    out: list[InquiryQuestion] = []
    for question in candidates:
        signal = question_policy.get(question.primitive)
        if signal is None or signal.attempts <= 0:
            out.append(question)
            continue
        policy_boost = question_policy_score_boost(signal)
        value = clamp_float(
            question.expected_value + policy_boost * 0.35,
            0.0,
            1.0,
        )
        cost = clamp_float(
            question.expected_cost
            - max(0.0, policy_boost) * 0.12
            + max(0.0, -policy_boost) * 0.10,
            0.02,
            1.0,
        )
        score = round(value - cost + policy_boost, 4)
        out.append(
            replace(
                question,
                expected_value=value,
                expected_cost=cost,
                score=score,
            )
        )
    return out


def question_policy_score_boost(signal: QuestionPolicySignal) -> float:
    success_rate = question_policy_success_rate(signal)
    utility = float(signal.utility_score)
    raw = 0.16 * utility + 0.20 * (success_rate - 0.35)
    return clamp_float(raw, -0.24, 0.34)


def question_policy_success_rate(signal: QuestionPolicySignal) -> float:
    # `successes` is credited at reader-decision grain, so it can exceed
    # question attempts. Cap to a probability before using it for policy.
    return clamp_float(signal.successes / max(signal.attempts, 1), 0.0, 1.0)


def question_policy_budget_multiplier(
    signal: QuestionPolicySignal | None,
) -> float:
    if signal is None or signal.attempts <= 0:
        return 1.0
    success_rate = question_policy_success_rate(signal)
    utility = float(signal.utility_score)
    if utility > 0.0 and success_rate >= 0.55:
        compaction = min(0.35, 0.06 * utility + 0.18 * (success_rate - 0.55))
        return clamp_float(1.0 - compaction, 0.65, 1.0)
    if utility < -0.25 or success_rate < 0.20:
        return 0.75
    return 1.0


def policy_budget(
    value: int,
    signal: QuestionPolicySignal | None,
) -> int:
    return max(1, int(round(float(value) * question_policy_budget_multiplier(signal))))


def select_questions(
    candidates: list[InquiryQuestion],
    *,
    questions_per_round: int,
    round_index: int,
    already_asked: set[str],
) -> list[InquiryQuestion]:
    selected: list[InquiryQuestion] = []
    seen_targets: set[str] = set()

    def add(question: InquiryQuestion, *, priority: bool = False) -> bool:
        if question.primitive in already_asked:
            return False
        if question.retrieval_target in seen_targets:
            return False
        if selected:
            marginal_score = question_marginal_score(question, selected)
            floor = (
                QUESTION_PRIORITY_MARGINAL_MIN_SCORE
                if priority
                else QUESTION_MARGINAL_MIN_SCORE
            )
            if marginal_score < floor:
                return False
        selected.append(replace(question, round_index=round_index))
        seen_targets.add(question.retrieval_target)
        return True

    by_id = {q.question_id: q for q in candidates}
    priority_ids: list[str] = []
    owner = by_id.get("Q_OWNER")
    counter = by_id.get("Q_COUNTEREVIDENCE")
    owner_is_priority = (
        owner is not None
        and owner.expected_value >= 0.70
        and "OWNERSHIP" not in already_asked
    )
    counter_is_priority = (
        questions_per_round >= 2
        and counter is not None
        and counter.expected_value >= 0.82
        and "COUNTEREVIDENCE" not in already_asked
    )
    if questions_per_round <= 3 and owner_is_priority and counter_is_priority:
        for question_id in ("Q_OWNER", "Q_COUNTEREVIDENCE"):
            question = by_id.get(question_id)
            if question is not None:
                add(question, priority=True)
        if len(selected) >= 2:
            return selected
    constraint = by_id.get("Q_CONSTRAINT")
    if (
        constraint is not None
        and constraint.expected_value >= 0.86
        and "CONSTRAINT" not in already_asked
        and "Q_CONSTRAINT" not in priority_ids
    ):
        priority_ids.append("Q_CONSTRAINT")
    if owner_is_priority and "Q_OWNER" not in priority_ids:
        priority_ids.append("Q_OWNER")
    recurrence = by_id.get("Q_RECURRENCE")
    if (
        recurrence is not None
        and recurrence.expected_value >= 0.9
        and "RECURRENCE" not in already_asked
    ):
        priority_ids.append("Q_RECURRENCE")
    if counter_is_priority and "Q_COUNTEREVIDENCE" not in priority_ids:
        priority_ids.append("Q_COUNTEREVIDENCE")
    dependency = by_id.get("Q_CRITICAL_PATH")
    if (
        dependency is not None
        and dependency.expected_value >= 0.86
        and "DEPENDENCY" not in already_asked
    ):
        priority_ids.append("Q_CRITICAL_PATH")
    goal_impact = by_id.get("Q_GOAL_IMPACT")
    if (
        goal_impact is not None
        and goal_impact.expected_value >= 0.86
        and "GOAL_IMPACT" not in already_asked
    ):
        priority_ids.append("Q_GOAL_IMPACT")

    for question_id in priority_ids:
        question = by_id.get(question_id)
        if question is None:
            continue
        add(question, priority=True)
        if len(selected) >= questions_per_round:
            return selected

    for question in sorted(
        candidates, key=lambda q: (-q.score, q.expected_cost, q.question_id)
    ):
        if question.primitive in already_asked:
            continue
        if question.retrieval_target in seen_targets:
            continue
        add(question)
        if len(selected) >= questions_per_round:
            break
    return selected


__all__ = [
    "apply_question_policy",
    "clamp_float",
    "policy_budget",
    "question_information_facets",
    "question_marginal_score",
    "question_policy_budget_multiplier",
    "question_policy_score_boost",
    "question_policy_success_rate",
    "question_target_overlap",
    "question_target_tokens",
    "select_questions",
]
