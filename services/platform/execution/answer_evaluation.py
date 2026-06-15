"""Question answer classification and inquiry sufficiency gates."""

from __future__ import annotations

import re
from datetime import datetime

from .evidence_utils import (
    declares_unrelated_to_trigger,
    evidence_supports_ownership,
    has_material_trigger_overlap,
    is_counterevidence_for_leading_hypothesis,
    is_stale_relative_to_trigger,
)
from .language_signals import (
    has_act_affecting_language,
    has_risk_language,
    mentions_recurrence,
)
from .types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    InquiryStopStatus,
    QuestionAnswer,
    SignalRoute,
    SufficiencyVerdict,
)


def classify_hypothesis_links(
    summary: str,
    hypotheses: tuple[Hypothesis, ...],
    *,
    trigger_text: str | None = None,
) -> tuple[set[str], set[str], set[str]]:
    lower = (summary or "").casefold()
    related_to_trigger = has_material_trigger_overlap(
        lower,
        (trigger_text or "").casefold(),
    ) and not declares_unrelated_to_trigger(lower)
    supports: set[str] = set()
    weakens: set[str] = set()
    contradicts: set[str] = set()
    if related_to_trigger and has_risk_language(lower):
        supports.add("H1")
        weakens.add("H0")
    if related_to_trigger and has_act_affecting_language(lower):
        supports.add("H2")
    if related_to_trigger and mentions_recurrence(lower):
        supports.add("H3")
    if related_to_trigger and any(
        word in lower for word in ("resolved", "unblocked", "not blocked", "launched")
    ):
        contradicts.add("H1")
        supports.add("H0")
    if related_to_trigger and has_premise_challenge_language(lower):
        weakens.add("H1")
    if related_to_trigger and has_missing_owner_language(lower):
        weakens.add("H2")
    known_ids = {hypothesis.id for hypothesis in hypotheses}
    return supports & known_ids, weakens & known_ids, contradicts & known_ids


def has_premise_challenge_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"not\s+the\s+only\s+blocker|one\s+blocker\s*,?\s*but|"
            r"also\s+(?:active|blocking|a\s+blocker|at\s+risk)|"
            r"additional\s+blocker|another\s+blocker|"
            r"premise\s+(?:is\s+)?(?:wrong|stale|incomplete|unsupported)|"
            r"assumption\s+(?:is\s+)?(?:wrong|stale|incomplete|unsupported)|"
            r"does\s+not\s+support|not\s+supported\s+by|unsupported\s+by|"
            r"evidence\s+(?:does\s+not|doesn't)\s+support|"
            r"marked\s+commit\s+but|crm\s+says\s+commit\s+but|"
            r"stale\s+(?:premise|assumption|status|stage|model)|"
            r"superseded\s+by|no\s+longer\s+(?:true|current|active)"
            r")\b",
            lower,
        )
    )


def has_missing_owner_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"no\s+(?:explicit|recorded|accountable)\s+owner|"
            r"owner\s+(?:is\s+)?(?:missing|unassigned|unknown|unclear|unresolved)|"
            r"missing\s+owner|unassigned\s+owner"
            r")\b",
            lower,
        )
    )


def answer_question(
    question: InquiryQuestion,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    *,
    trigger_occurred_at: datetime | None = None,
    stale_after_days: int = 30,
) -> QuestionAnswer:
    candidates = [
        card
        for card in evidence_by_key.values()
        if question.question_id in card.retrieved_for_questions
    ]
    if question.primitive == "COUNTEREVIDENCE":
        fresh_counter = [
            str(card.evidence_id)
            for card in candidates
            if is_counterevidence_for_leading_hypothesis(card)
            and not is_stale_relative_to_trigger(
                card,
                trigger_occurred_at=trigger_occurred_at,
                stale_after_days=stale_after_days,
            )
        ][:8]
        stale_counter = [
            str(card.evidence_id)
            for card in candidates
            if is_counterevidence_for_leading_hypothesis(card)
            and is_stale_relative_to_trigger(
                card,
                trigger_occurred_at=trigger_occurred_at,
                stale_after_days=stale_after_days,
            )
        ][:8]
        supporting = [
            str(card.evidence_id)
            for card in candidates
            if card.supports_hypotheses
            or card.source_type in {"commitment", "goal", "resource"}
        ][:8]
        if fresh_counter and supporting:
            status = "partially_supported"
            summary = "Retrieved both supporting evidence and fresh counterevidence."
        elif fresh_counter:
            status = "supported"
            summary = "Retrieved fresh counterevidence for this question."
        elif candidates:
            status = "inconclusive"
            summary = (
                "Only stale counterevidence was retrieved for this question."
                if stale_counter
                else "Retrieved related evidence but no credible counterevidence."
            )
        else:
            status = "unanswered"
            summary = "No usable evidence was retrieved for this question."
        unknowns = (
            ("fresh counterevidence",) if stale_counter and not fresh_counter else ()
        )
        return QuestionAnswer(
            question_id=question.question_id,
            answer_status=status,
            summary=summary,
            supporting_evidence=tuple(supporting),
            counterevidence=tuple(fresh_counter),
            new_uncertainties=unknowns,
        )

    supporting = [
        str(card.evidence_id)
        for card in candidates
        if (
            evidence_supports_ownership(card)
            if question.primitive == "OWNERSHIP"
            else card.supports_hypotheses
            or card.source_type in {"commitment", "goal", "resource"}
        )
    ][:8]
    counter = [
        str(card.evidence_id)
        for card in candidates
        if card.contradicts_hypotheses or card.weakens_hypotheses
    ][:8]
    if supporting and counter:
        status = "partially_supported"
        summary = "Retrieved both supporting evidence and counterevidence."
    elif supporting:
        status = "supported"
        summary = "Retrieved supporting evidence for this question."
    elif candidates:
        status = "inconclusive"
        summary = "Retrieved related evidence but no decisive answer."
    else:
        status = "unanswered"
        summary = "No usable evidence was retrieved for this question."
    unknowns = ()
    if question.primitive == "OWNERSHIP" and not supporting:
        unknowns = ("responsible owner",)
    return QuestionAnswer(
        question_id=question.question_id,
        answer_status=status,
        summary=summary,
        supporting_evidence=tuple(supporting),
        counterevidence=tuple(counter),
        new_uncertainties=unknowns,
    )


def resolved_unknowns_for_answer(
    question: InquiryQuestion,
    answer: QuestionAnswer,
) -> set[str]:
    if question.primitive == "COUNTEREVIDENCE":
        if (
            answer.answer_status == "unanswered"
            or "fresh counterevidence" in answer.new_uncertainties
        ):
            return set()
        return {"counterevidence"}
    if answer.answer_status not in {"supported", "partially_supported"}:
        return set()
    primitive_to_unknowns = {
        "DEPENDENCY": {"whether the blocker is on the critical path"},
        "COMMITMENT": {"affected commitment"},
        "OWNERSHIP": {"responsible owner"},
        "GOAL_IMPACT": {"affected goal"},
        "RECURRENCE": {"whether this is part of a broader recurring pattern"},
    }
    return set(primitive_to_unknowns.get(question.primitive, set()))


def sufficiency_gate(
    route: SignalRoute,
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard],
    answers: list[QuestionAnswer],
    *,
    round_index: int,
    max_rounds: int,
    unknowns: set[str],
) -> SufficiencyVerdict:
    evidence_count = len(evidence)
    answered = sum(
        1
        for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    has_support = any(card.supports_hypotheses for card in evidence)
    has_counter_check = any(
        answer.question_id == "Q_COUNTEREVIDENCE"
        and answer.answer_status != "unanswered"
        and "fresh counterevidence" not in answer.new_uncertainties
        for answer in answers
    )
    has_act = any(
        card.source_type in {"commitment", "goal", "decision", "resource"}
        for card in evidence
    )

    if route == "HUMAN_VALIDATION_PATH":
        return SufficiencyVerdict(
            "human_validation_required",
            "routing indicated a human-resolvable missing fact",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if route in {"FAST_PATH", "DETERMINISTIC_UPDATE"} and max_rounds == 0:
        status: InquiryStopStatus = (
            "sufficient_for_reasoning" if evidence_count else "no_update_needed"
        )
        return SufficiencyVerdict(
            status,
            "fast/bounded path compiled baseline context",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if evidence_count == 0:
        return SufficiencyVerdict(
            "no_update_needed",
            "no related evidence survived baseline or inquiry retrieval",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if (
        route == "DEEP_INQUIRY_PATH"
        and round_index >= max_rounds
        and "responsible owner" in unknowns
        and any(answer.question_id == "Q_OWNER" for answer in answers)
    ):
        return SufficiencyVerdict(
            "human_validation_required",
            "the affected region is visible but ownership remains unresolved",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if has_support and has_counter_check and (has_act or evidence_count >= 6):
        return SufficiencyVerdict(
            "sufficient_for_reasoning",
            "supporting evidence exists, counterevidence was checked, and an affected region is visible",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    if round_index >= max_rounds:
        return SufficiencyVerdict(
            "budget_exhausted",
            "inquiry round budget reached before all uncertainty closed",
            evidence_count,
            answered,
            tuple(sorted(unknowns)[:10]),
        )
    return SufficiencyVerdict(
        "insufficient_continue",
        "more retrieval has expected value",
        evidence_count,
        answered,
        tuple(sorted(unknowns)[:10]),
    )


__all__ = [
    "answer_question",
    "classify_hypothesis_links",
    "has_missing_owner_language",
    "has_premise_challenge_language",
    "resolved_unknowns_for_answer",
    "sufficiency_gate",
]
