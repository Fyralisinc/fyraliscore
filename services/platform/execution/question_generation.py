"""Deterministic inquiry hypotheses, unknowns, and fallback questions."""

from __future__ import annotations

from typing import Any

from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .language_signals import (
    has_broad_signal_language,
    has_commitment_language,
    has_constraint_language,
    has_dependency_language,
    has_revenue_impact_language,
    has_risk_language,
    mentions_recurrence,
)
from .question_text import (
    claim_from_text,
    question_anchors,
    question_entity_labels,
    specific_question,
)
from .routing import trigger_text
from .types import EvidenceCard, Hypothesis, InquiryQuestion


def generate_hypotheses(
    trigger: TriggerContext,
    baseline: RetrievalResult,
) -> list[Hypothesis]:
    text = trigger_text(trigger)
    lower = text.casefold()
    anchors = question_anchors(trigger)
    hypotheses: list[Hypothesis] = []
    risk = has_risk_language(lower)
    commitment = has_commitment_language(lower) or bool(
        baseline.acts.get("commitments")
    )
    if risk:
        hypotheses.append(
            Hypothesis(
                id="H1",
                claim=claim_from_text(
                    anchors.focus or text,
                    fallback="The signal describes a real operational blocker or risk.",
                ),
                confidence=0.46,
                impact_if_true="high",
                delta_type="update",
                affected_entities=tuple(
                    question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=tuple(deterministic_delta_uncertainties(lower)),
                evidence_needed=(
                    "fresh signal evidence",
                    "related active commitments",
                    "recent counterevidence",
                ),
            )
        )
    if commitment:
        hypotheses.append(
            Hypothesis(
                id="H2",
                claim=(
                    f"An active commitment, owner, or promised outcome is affected by "
                    f"{anchors.focus}."
                ),
                confidence=0.36,
                impact_if_true="medium",
                delta_type="update",
                affected_entities=tuple(
                    question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "which active commitment is affected",
                    "who owns the next action",
                    "which deadline or promised outcome is at risk",
                ),
                evidence_needed=(
                    "active commitments",
                    "commitment owners",
                    "recent owner or decision evidence",
                ),
            )
        )
    if mentions_recurrence(lower) or len(baseline.models) >= 3:
        hypotheses.append(
            Hypothesis(
                id="H3",
                claim=f"{anchors.focus} may be part of a broader recurring pattern.",
                confidence=0.29,
                impact_if_true="high" if risk else "medium",
                delta_type="create" if not baseline.models else "update",
                affected_entities=tuple(
                    question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "whether this pattern has appeared before",
                    "which prior models support or weaken the recurrence claim",
                ),
                evidence_needed=(
                    "similar prior observations",
                    "related pattern models",
                    "model edges to comparable situations",
                ),
            )
        )
    if not hypotheses:
        hypotheses.append(
            Hypothesis(
                id="H1",
                claim=f"{anchors.focus} may add localized context to existing memory.",
                confidence=0.30,
                impact_if_true="medium",
                delta_type="update",
                affected_entities=tuple(
                    question_entity_labels(trigger)
                    or ((anchors.subject,) if anchors.subject != "this signal" else ())
                ),
                uncertainty_slots=(
                    "which existing model, if any, should absorb this signal",
                    "whether this is already captured",
                ),
                evidence_needed=("nearby existing models", "recent observations"),
            )
        )
    hypotheses.append(
        Hypothesis(
            id="H0",
            claim="The signal is local noise or already captured and requires no Synthesis update.",
            confidence=0.16 if risk or commitment else 0.32,
            impact_if_true="low",
            delta_type="no_op",
            uncertainty_slots=(
                "whether the signal is already captured",
                "whether no model update is needed",
            ),
            evidence_needed=("existing matching models", "counterevidence"),
        )
    )
    return hypotheses


def initial_unknowns(trigger: TriggerContext, baseline: RetrievalResult) -> list[str]:
    unknowns: list[str] = []
    lower = trigger_text(trigger).casefold()
    if has_risk_language(lower):
        unknowns.append("whether the blocker is on the critical path")
    if has_dependency_language(lower):
        unknowns.append("whether the dependency is binding")
    if has_constraint_language(lower):
        unknowns.append("blocking constraint")
    if not baseline.acts.get("commitments"):
        unknowns.append("affected commitment")
    if not baseline.acts.get("goals"):
        unknowns.append("affected goal")
    if "owner" in lower or "who" in lower or has_risk_language(lower):
        unknowns.append("responsible owner")
    if mentions_recurrence(lower):
        unknowns.append("whether this is part of a broader recurring pattern")
    unknowns.append("counterevidence")
    return unknowns


def deterministic_delta_uncertainties(lower: str) -> list[str]:
    slots: list[str] = []
    if has_dependency_language(lower) or has_risk_language(lower):
        slots.append("whether the blocker is actually on the critical path")
    if has_constraint_language(lower):
        slots.append("which resource, policy, or capacity constraint is binding")
    if has_revenue_impact_language(lower):
        slots.append("which customer goal or revenue path is at risk")
    if has_commitment_language(lower):
        slots.append("which active commitment or promised outcome is affected")
    if "owner" in lower or "who" in lower or has_risk_language(lower):
        slots.append("who owns the next action")
    if mentions_recurrence(lower):
        slots.append("whether this has appeared before")
    slots.append("what evidence would weaken this interpretation")
    return dedupe_unknowns(slots)


def candidate_questions(
    trigger: TriggerContext,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
) -> list[InquiryQuestion]:
    text = trigger_text(trigger)
    lower = text.casefold()
    broad = has_broad_signal_language(lower)
    hids = tuple(h.id for h in hypotheses if h.id != "H0")
    anchors = question_anchors(trigger)
    out = [
        InquiryQuestion(
            question_id="Q_CRITICAL_PATH",
            question=specific_question("DEPENDENCY", anchors),
            primitive="DEPENDENCY",
            tests_hypotheses=hids[:2] or ("H1",),
            expected_value=(
                0.60
                if broad
                else (
                    0.90
                    if has_risk_language(lower) or has_dependency_language(lower)
                    else 0.55
                )
            ),
            expected_cost=0.24,
            retrieval_target="commitment_graph+recent_observations",
            stop_condition="critical-path evidence or counterevidence found",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_ACTIVE_COMMITMENT",
            question=specific_question("COMMITMENT", anchors),
            primitive="COMMITMENT",
            tests_hypotheses=("H2", "H0"),
            expected_value=(
                0.84 if broad else (0.78 if "affected commitment" in unknowns else 0.42)
            ),
            expected_cost=0.18,
            retrieval_target="active_commitments",
            stop_condition="matching active commitment found or ruled out",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_CONSTRAINT",
            question=specific_question("CONSTRAINT", anchors),
            primitive="CONSTRAINT",
            tests_hypotheses=hids[:2] or ("H1",),
            expected_value=(
                0.90
                if has_constraint_language(lower)
                else (0.76 if has_dependency_language(lower) else 0.40)
            ),
            expected_cost=0.24,
            retrieval_target="constraints+resource_edges",
            stop_condition="binding constraint identified or ruled out",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_COUNTEREVIDENCE",
            question=specific_question("COUNTEREVIDENCE", anchors),
            primitive="COUNTEREVIDENCE",
            tests_hypotheses=("H1", "H0"),
            expected_value=0.84 if has_risk_language(lower) else 0.74,
            expected_cost=0.30,
            retrieval_target="semantic_counterevidence+recent_observations",
            stop_condition="credible alternate explanation found or absent",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_OWNER",
            question=specific_question("OWNERSHIP", anchors),
            primitive="OWNERSHIP",
            tests_hypotheses=("H2",),
            expected_value=0.72 if "responsible owner" in unknowns else 0.36,
            expected_cost=0.22,
            retrieval_target="commitment_owners+actor_scope",
            stop_condition="owner identified or human validation required",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_GOAL_IMPACT",
            question=specific_question("GOAL_IMPACT", anchors),
            primitive="GOAL_IMPACT",
            tests_hypotheses=hids[:3] or ("H1",),
            expected_value=(
                0.94
                if broad
                else (
                    0.86
                    if has_revenue_impact_language(lower)
                    else (0.68 if "affected goal" in unknowns else 0.38)
                )
            ),
            expected_cost=0.20,
            retrieval_target="goal_resource_bridge",
            stop_condition="goal/customer/resource impact identified",
            score=0.0,
        ),
        InquiryQuestion(
            question_id="Q_RECURRENCE",
            question=specific_question("RECURRENCE", anchors),
            primitive="RECURRENCE",
            tests_hypotheses=("H3", "H0"),
            expected_value=(
                0.80 if broad else (0.92 if mentions_recurrence(lower) else 0.44)
            ),
            expected_cost=0.36,
            retrieval_target="pattern+model_edges",
            stop_condition="pattern support or absence established",
            score=0.0,
        ),
    ]
    if len(evidence_by_key) < 5:
        for question in out:
            score = question.expected_value - question.expected_cost + 0.15
            object.__setattr__(question, "score", round(score, 4))
    else:
        for question in out:
            score = question.expected_value - question.expected_cost
            object.__setattr__(question, "score", round(score, 4))
    return out


def dedupe_unknowns(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        clean = str(value or "").strip()
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out
