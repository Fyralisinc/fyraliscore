"""Evidence minimization and reasoning context-packet assembly."""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Callable, Literal

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.synthesis.state_contract import (
    StateSource,
    compile_state_contract,
)

from .evidence_utils import (
    compact,
    estimate_tokens,
    evidence_to_dict,
    jsonable,
    material_tokens,
    timestamp_sort_value,
    trust_score,
)
from .question_generation import dedupe_unknowns
from .retrieval_learning import is_low_value_model_noise
from .routing import trigger_text
from .types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    MemoryDecisionCandidate,
    QuestionAnswer,
    SignalRoute,
    SufficiencyVerdict,
)


def rank_evidence(cards: list[EvidenceCard], *, limit: int) -> list[EvidenceCard]:
    return sorted(
        cards,
        key=lambda card: (
            -evidence_value(card),
            -timestamp_sort_value(card.timestamp),
            str(card.evidence_id),
        ),
        reverse=False,
    )[:limit]


def select_minimal_sufficient_evidence(
    cards: list[EvidenceCard],
    *,
    hypotheses: tuple[Hypothesis, ...],
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    evidence_limit: int,
) -> tuple[list[EvidenceCard], dict[str, Any]]:
    """Compress ranked evidence to the smallest writer-useful packet."""
    if not cards:
        return [], {
            "enabled": True,
            "input_count": 0,
            "selected_count": 0,
            "target_count": 0,
            "dropped_count": 0,
            "drop_ratio": 0.0,
            "protected_count": 0,
            "coverage": {},
        }

    by_id = {str(card.evidence_id): card for card in cards}
    selected: list[EvidenceCard] = []
    selected_ids: set[str] = set()
    protected_ids: set[str] = set()
    protected_reasons: dict[str, set[str]] = {}

    def add(card: EvidenceCard | None, reason: str, *, protected: bool) -> bool:
        if card is None:
            return False
        card_id = str(card.evidence_id)
        if card_id in selected_ids:
            if protected:
                protected_ids.add(card_id)
                protected_reasons.setdefault(card_id, set()).add(reason)
            return False
        if len(selected) >= max(1, int(evidence_limit)):
            return False
        selected.append(card)
        selected_ids.add(card_id)
        if protected:
            protected_ids.add(card_id)
            protected_reasons.setdefault(card_id, set()).add(reason)
        return True

    target = minimal_evidence_target(
        cards,
        questions=questions,
        answers=answers,
        route=route,
        mode=mode,
        evidence_limit=evidence_limit,
    )
    hard_cap = max(target, protected_answer_ref_count(answers))
    hard_cap = min(max(1, int(evidence_limit)), max(hard_cap, target))

    for answer in answers:
        support_cards = [
            by_id[evidence_id]
            for evidence_id in answer.supporting_evidence
            if evidence_id in by_id
        ]
        counter_cards = [
            by_id[evidence_id]
            for evidence_id in answer.counterevidence
            if evidence_id in by_id
        ]
        for card in sorted(support_cards, key=evidence_sort_key)[:2]:
            add(card, f"answer_support:{answer.question_id}", protected=True)
        for card in sorted(counter_cards, key=evidence_sort_key)[:2]:
            add(card, f"answer_counter:{answer.question_id}", protected=True)

    for question in questions:
        question_cards = [
            card
            for card in cards
            if question.question_id in card.retrieved_for_questions
        ]
        if question_cards:
            add(
                sorted(question_cards, key=evidence_sort_key)[0],
                f"question_coverage:{question.question_id}",
                protected=True,
            )

    for hypothesis in hypotheses:
        support = [card for card in cards if hypothesis.id in card.supports_hypotheses]
        if support:
            add(
                sorted(support, key=evidence_sort_key)[0],
                f"hypothesis_support:{hypothesis.id}",
                protected=True,
            )

    counters = [
        card for card in cards if card.weakens_hypotheses or card.contradicts_hypotheses
    ]
    for card in sorted(counters, key=evidence_sort_key)[:2]:
        add(card, "falsification_guard", protected=True)

    for source_type in ("commitment", "goal", "decision", "resource"):
        typed = [card for card in cards if card.source_type == source_type]
        if typed:
            add(
                sorted(typed, key=evidence_sort_key)[0],
                f"action_anchor:{source_type}",
                protected=True,
            )

    if len(selected) > hard_cap:
        selected.sort(
            key=lambda card: (
                str(card.evidence_id) not in protected_ids,
                *evidence_sort_key(card),
            )
        )
        selected = selected[:hard_cap]
        selected_ids = {str(card.evidence_id) for card in selected}

    while len(selected) < target:
        best: EvidenceCard | None = None
        best_score = float("-inf")
        for card in cards:
            card_id = str(card.evidence_id)
            if card_id in selected_ids:
                continue
            marginal = marginal_evidence_value(card, selected)
            if marginal > best_score:
                best = card
                best_score = marginal
        if best is None:
            break
        if len(selected) >= minimal_floor(questions, answers) and best_score <= 0.0:
            break
        add(best, "marginal_value", protected=False)

    selected.sort(key=evidence_sort_key)
    selected_ids = {str(card.evidence_id) for card in selected}
    coverage = {
        "questions": coverage_share(
            [question.question_id for question in questions],
            lambda question_id: any(
                question_id in card.retrieved_for_questions for card in selected
            ),
        ),
        "supported_answers": coverage_share(
            [
                answer.question_id
                for answer in answers
                if answer.answer_status in {"supported", "partially_supported"}
            ],
            lambda question_id: any(
                question_id in card.retrieved_for_questions for card in selected
            ),
        ),
        "hypotheses": coverage_share(
            [hypothesis.id for hypothesis in hypotheses],
            lambda hypothesis_id: any(
                hypothesis_id in card.supports_hypotheses
                or hypothesis_id in card.weakens_hypotheses
                or hypothesis_id in card.contradicts_hypotheses
                for card in selected
            ),
        ),
        "has_counterevidence": any(
            card.weakens_hypotheses or card.contradicts_hypotheses for card in selected
        ),
        "has_action_anchor": any(
            card.source_type in {"commitment", "goal", "decision", "resource"}
            for card in selected
        ),
    }
    return selected, {
        "enabled": True,
        "input_count": len(cards),
        "selected_count": len(selected),
        "target_count": target,
        "dropped_count": max(0, len(cards) - len(selected)),
        "drop_ratio": round(
            (len(cards) - len(selected)) / max(1, len(cards)),
            4,
        ),
        "protected_count": len(protected_ids & selected_ids),
        "protected_reasons": {
            evidence_id: sorted(reasons)
            for evidence_id, reasons in protected_reasons.items()
            if evidence_id in selected_ids
        },
        "coverage": coverage,
    }


def minimal_evidence_target(
    cards: list[EvidenceCard],
    *,
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    evidence_limit: int,
) -> int:
    supported = sum(
        1
        for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    question_count = len(questions)
    counter_bonus = (
        2
        if any(card.weakens_hypotheses or card.contradicts_hypotheses for card in cards)
        else 0
    )
    action_bonus = (
        2
        if any(
            card.source_type in {"commitment", "goal", "decision", "resource"}
            for card in cards
        )
        else 0
    )
    base = 8 if mode == "fast" or route == "FAST_PATH" else 9
    target = base + question_count * 2 + supported + counter_bonus + action_bonus
    if route == "BACKGROUND_PATH":
        target = min(target, 16)
    if mode == "fast" or route == "FAST_PATH":
        target = min(target, 14)
    else:
        target = min(target, 22)
    return min(max(1, int(evidence_limit)), max(1, target), len(cards))


def protected_answer_ref_count(answers: list[QuestionAnswer]) -> int:
    refs: set[str] = set()
    for answer in answers:
        refs.update(answer.supporting_evidence[:2])
        refs.update(answer.counterevidence[:2])
    return len(refs)


def minimal_floor(
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
) -> int:
    supported = sum(
        1
        for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    return max(4, min(12, len(questions) + supported + 2))


def evidence_sort_key(card: EvidenceCard) -> tuple[float, float, str]:
    return (
        -evidence_value(card),
        -timestamp_sort_value(card.timestamp),
        str(card.evidence_id),
    )


def marginal_evidence_value(
    card: EvidenceCard,
    selected: list[EvidenceCard],
) -> float:
    value = evidence_value(card)
    if is_low_value_model_noise(card):
        value -= 0.32
    if card.source_type == "observation":
        value += 0.08
    if "sage_reader" in card.retrieval_paths:
        value += 0.06
    if (
        card.supports_hypotheses
        or card.weakens_hypotheses
        or card.contradicts_hypotheses
    ):
        value += 0.12
    value -= redundancy_penalty(card, selected)
    return value


def redundancy_penalty(
    card: EvidenceCard,
    selected: list[EvidenceCard],
) -> float:
    if not selected:
        return 0.0
    penalty = 0.0
    card_tokens = material_tokens(card.summary.casefold())
    card_links = (
        frozenset(card.supports_hypotheses),
        frozenset(card.weakens_hypotheses),
        frozenset(card.contradicts_hypotheses),
    )
    for kept in selected:
        if card.source_ref == kept.source_ref:
            penalty += 1.0
            continue
        if card.source_type == kept.source_type and card_links == (
            frozenset(kept.supports_hypotheses),
            frozenset(kept.weakens_hypotheses),
            frozenset(kept.contradicts_hypotheses),
        ):
            penalty += 0.10
        kept_tokens = material_tokens(kept.summary.casefold())
        if card_tokens and kept_tokens:
            overlap = len(card_tokens & kept_tokens) / max(
                1,
                min(len(card_tokens), len(kept_tokens)),
            )
            if overlap >= 0.82:
                penalty += 0.45
            elif overlap >= 0.58:
                penalty += 0.16
    return min(1.25, penalty)


def coverage_share(
    items: list[str],
    predicate: Callable[[str], bool],
) -> float:
    if not items:
        return 1.0
    unique = list(dict.fromkeys(items))
    return round(
        sum(1 for item in unique if predicate(item)) / max(1, len(unique)),
        4,
    )


def evidence_value(card: EvidenceCard) -> float:
    usefulness = card.score
    usefulness += 0.35 if card.supports_hypotheses else 0.0
    usefulness += (
        0.30 if card.contradicts_hypotheses or card.weakens_hypotheses else 0.0
    )
    usefulness += (
        0.25 if card.source_type in {"commitment", "goal", "resource"} else 0.0
    )
    usefulness += trust_score(card.trust_tier)
    penalty = min(0.35, card.token_estimate / 5000.0)
    return usefulness - penalty


def state_contract_for_context_packet(
    trigger: TriggerContext,
    evidence: list[EvidenceCard],
) -> dict[str, Any]:
    sources = [
        StateSource(
            source_kind=card.source_type,
            source_ref=card.raw_content_ref or f"{card.source_type}:{card.source_ref}",
            text=card.summary,
            occurred_at=card.timestamp,
            confidence=evidence_card_confidence(card),
            metadata={
                "evidence_id": str(card.evidence_id),
                "retrieval_paths": sorted(card.retrieval_paths),
                "supports_hypotheses": sorted(card.supports_hypotheses),
                "weakens_hypotheses": sorted(card.weakens_hypotheses),
                "contradicts_hypotheses": sorted(card.contradicts_hypotheses),
                "trust_tier": card.trust_tier,
            },
        )
        for card in evidence
    ]
    return compile_state_contract(trigger_text(trigger), sources).to_dict()


def evidence_card_confidence(card: EvidenceCard) -> float:
    base = {
        "authoritative": 0.92,
        "reputable": 0.78,
        "model": 0.62,
        "low": 0.38,
    }.get(str(card.trust_tier or "").casefold(), 0.55)
    if card.contradicts_hypotheses or card.weakens_hypotheses:
        base += 0.06
    if card.supports_hypotheses:
        base += 0.04
    return round(max(0.05, min(0.99, base)), 2)


def filter_context_packet_evidence(
    evidence: list[EvidenceCard],
    mode: str,
    answers: list[QuestionAnswer],
) -> tuple[list[EvidenceCard], dict[str, Any], set[str]]:
    normalized = str(mode or "model_first").strip().lower()
    if normalized not in {"all", "model_first", "models_only"}:
        normalized = "model_first"
    model_cards = [card for card in evidence if card.source_type == "model"]
    model_ref_ids = {str(card.evidence_id) for card in model_cards}
    answer_questions_by_ref: dict[str, set[str]] = {}
    model_answer_questions: set[str] = set()
    for answer in answers:
        refs = [*answer.supporting_evidence, *answer.counterevidence]
        if any(ref in model_ref_ids for ref in refs):
            model_answer_questions.add(answer.question_id)
        for ref in refs:
            answer_questions_by_ref.setdefault(ref, set()).add(answer.question_id)

    def required_answer_ref(card: EvidenceCard) -> bool:
        question_ids = answer_questions_by_ref.get(str(card.evidence_id), set())
        return any(
            question_id not in model_answer_questions for question_id in question_ids
        )

    answer_required_ids = {
        str(card.evidence_id)
        for card in evidence
        if card.source_type != "model" and required_answer_ref(card)
    }
    if normalized == "all":
        selected = list(evidence)
        fallback_reason = None
    elif not model_cards:
        selected = list(evidence)
        fallback_reason = "no_model_evidence"
    elif normalized == "models_only":
        selected = model_cards
        fallback_reason = None
    else:
        selected = [
            card
            for card in evidence
            if card.source_type == "model"
            or card.source_type in {"commitment", "goal", "decision", "resource"}
            or bool(card.weakens_hypotheses or card.contradicts_hypotheses)
            or required_answer_ref(card)
        ]
        fallback_reason = None

    selected_ids = {id(card) for card in selected}
    suppressed = [card for card in evidence if id(card) not in selected_ids]
    return (
        selected,
        {
            "mode": normalized,
            "input_evidence_count": len(evidence),
            "packet_evidence_count": len(selected),
            "model_evidence_count": len(model_cards),
            "non_model_evidence_count": len(evidence) - len(model_cards),
            "suppressed_observation_count": sum(
                1 for card in suppressed if card.source_type == "observation"
            ),
            "suppressed_non_model_count": sum(
                1 for card in suppressed if card.source_type != "model"
            ),
            "answer_required_non_model_count": sum(
                1
                for card in selected
                if card.source_type != "model" and required_answer_ref(card)
            ),
            "fallback_reason": fallback_reason,
        },
        answer_required_ids,
    )


def compile_context_packet(
    trigger: TriggerContext,
    route: SignalRoute,
    hypotheses: tuple[Hypothesis, ...],
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    evidence: list[EvidenceCard],
    sufficiency: SufficiencyVerdict,
    *,
    token_budget: int,
    evidence_mode: str = "model_first",
) -> dict[str, Any]:
    packet_evidence, evidence_policy, answer_required_ids = (
        filter_context_packet_evidence(
            evidence,
            evidence_mode,
            answers,
        )
    )
    observation_fallback = evidence_policy.get("fallback_reason") == "no_model_evidence"
    decisive: list[dict[str, Any]] = []
    supporting_groups: dict[str, list[EvidenceCard]] = {}
    omitted: list[dict[str, Any]] = []
    used_tokens = 0
    for card in packet_evidence:
        item = evidence_to_dict(card)
        cost = int(item.get("token_estimate") or 1)
        if (
            used_tokens + cost <= token_budget
            and (
                card.contradicts_hypotheses
                or card.weakens_hypotheses
                or card.source_type in {"commitment", "goal", "decision", "resource"}
                or (card.source_type == "observation" and observation_fallback)
                or str(card.evidence_id) in answer_required_ids
            )
            and len(decisive) < 30
        ):
            decisive.append(item)
            used_tokens += cost
            if card.supports_hypotheses:
                key = ",".join(sorted(card.supports_hypotheses))
                supporting_groups.setdefault(key, []).append(card)
        else:
            if is_low_value_model_noise(card):
                omitted.append(
                    {
                        "source_ref": card.source_ref,
                        "reason": "retrieved model had no hypothesis link",
                        "expand_if": "debugging semantic recall or investigating missed classifier links",
                    }
                )
                continue
            key = ",".join(sorted(card.supports_hypotheses)) or card.source_type
            supporting_groups.setdefault(key, []).append(card)
    supporting = []
    for claim, cards in supporting_groups.items():
        shown = cards[:8]
        summary = compact("; ".join(card.summary for card in shown), 600)
        cost = estimate_tokens(summary)
        group = {
            "claim_supported": claim,
            "evidence_count": len(cards),
            "sources": sorted({card.source_type for card in cards}),
            "summary": summary,
            "evidence_ids": [str(card.evidence_id) for card in shown],
            "source_refs": [card.source_ref for card in shown],
        }
        if len(supporting) < 12 and used_tokens + cost <= token_budget:
            supporting.append(group)
            used_tokens += cost
        else:
            omitted.append(
                {
                    "group": claim,
                    "count": len(cards),
                    "reason": "context packet token budget reached",
                    "expand_if": "reasoning needs additional supporting evidence for this claim",
                }
            )
            continue
        if len(cards) > len(shown):
            omitted.append(
                {
                    "group": claim,
                    "count": len(cards) - len(shown),
                    "reason": "redundant with stronger selected evidence",
                    "expand_if": "deep reasoning needs additional provenance for this claim",
                }
            )
    background = []
    for item in background_summaries(packet_evidence):
        cost = estimate_tokens(item.get("summary", ""))
        if used_tokens + cost <= token_budget:
            background.append(item)
            used_tokens += cost
        else:
            omitted.append(
                {
                    "group": f"background:{item.get('path')}",
                    "count": item.get("count", 0),
                    "reason": "context packet token budget reached",
                    "expand_if": "debugging retrieval pathway breadth",
                }
            )
    state_contract = state_contract_for_context_packet(trigger, packet_evidence)
    important_unknowns = dedupe_unknowns(
        [
            *list(sufficiency.remaining_unknowns),
            *[
                slot
                for slot in state_contract.get("missing_slots", [])
                if slot != "premise_challenge"
            ],
        ]
    )
    return {
        "signal_summary": compact(trigger_text(trigger), 1000),
        "source_metadata": {
            "trigger_kind": trigger.kind,
            "observation_id": str(trigger.observation_id)
            if trigger.observation_id
            else None,
            "model_id": str(trigger.model_id) if trigger.model_id else None,
            "route": route,
        },
        "resolved_entities": jsonable(trigger.seed_entity_ids),
        "hypotheses": [jsonable(asdict(hypothesis)) for hypothesis in hypotheses],
        "question_path": [jsonable(asdict(question)) for question in questions],
        "question_answers": [jsonable(asdict(answer)) for answer in answers],
        "sufficiency_verdict": jsonable(asdict(sufficiency)),
        "memory_decision_candidates": [
            jsonable(asdict(candidate))
            for candidate in memory_decision_candidates(
                trigger,
                hypotheses,
                questions,
                answers,
                packet_evidence,
                sufficiency,
            )
        ],
        "candidate_state_changes": candidate_state_changes(
            hypotheses,
            packet_evidence,
            sufficiency,
        ),
        "important_unknowns": important_unknowns,
        "state_contract": state_contract,
        "answer_obligations": {
            "required_slots": state_contract.get("required_slots", []),
            "covered_slots": state_contract.get("covered_slots", []),
            "missing_slots": state_contract.get("missing_slots", []),
            "premise_status": state_contract.get("premise_check", {}).get("status"),
        },
        "tiers": {
            "decisive_evidence": decisive,
            "supporting_evidence_groups": supporting,
            "background_summaries": background,
            "omission_ledger": omitted[:12],
        },
        "budget": {
            "token_budget": token_budget,
            "estimated_tokens_used": used_tokens,
            "reservoir_evidence_count": len(evidence),
            "packet_evidence_count": len(packet_evidence),
            "evidence_policy": evidence_policy,
        },
    }


def memory_decision_candidates(
    trigger: TriggerContext,
    hypotheses: tuple[Hypothesis, ...],
    questions: list[InquiryQuestion],
    answers: list[QuestionAnswer],
    evidence: list[EvidenceCard],
    sufficiency: SufficiencyVerdict,
    *,
    max_candidates: int = 5,
) -> list[MemoryDecisionCandidate]:
    """Compile inquiry output into Think-facing memory decision candidates.

    The result is advisory: it narrows the final LLM's decision surface without
    granting permission to write. Think may accept, update, reject, merge, no-op,
    or add a missing durable op if the packet missed something material.
    """

    if not _memory_decision_candidates_enabled():
        return []
    if sufficiency.status not in {
        "sufficient_for_reasoning",
        "budget_exhausted",
        "human_validation_required",
    }:
        return []

    candidates: list[MemoryDecisionCandidate] = []
    used_ids: set[str] = set()
    hypotheses_by_id = {hypothesis.id: hypothesis for hypothesis in hypotheses}
    questions_by_hypothesis = _questions_by_hypothesis(questions)
    answers_by_question = {answer.question_id: answer for answer in answers}
    evidence_by_hypothesis = _evidence_by_hypothesis(evidence)

    def add(candidate: MemoryDecisionCandidate) -> None:
        if len(candidates) >= max_candidates:
            return
        if candidate.candidate_id in used_ids:
            return
        used_ids.add(candidate.candidate_id)
        candidates.append(candidate)

    non_noop = [
        hypothesis
        for hypothesis in hypotheses
        if hypothesis.id != "H0"
        and _op_family_for_hypothesis(hypothesis) != "no_op"
    ]
    non_noop.sort(
        key=lambda h: (
            -_hypothesis_decision_score(
                h,
                evidence_by_hypothesis.get(h.id, []),
                questions_by_hypothesis.get(h.id, []),
            ),
            h.id,
        )
    )

    for hypothesis in non_noop[:3]:
        linked_evidence = evidence_by_hypothesis.get(hypothesis.id, [])
        linked_questions = questions_by_hypothesis.get(hypothesis.id, [])
        add(
            _candidate_from_hypothesis(
                trigger,
                hypothesis,
                linked_questions,
                answers_by_question,
                linked_evidence,
            )
        )

    for hypothesis in non_noop:
        if len(candidates) >= max_candidates - 1:
            break
        linked_questions = questions_by_hypothesis.get(hypothesis.id, [])
        linked_evidence = evidence_by_hypothesis.get(hypothesis.id, [])
        if _needs_relationship_decision(hypothesis, linked_questions):
            edge_candidate = _relationship_candidate_from_hypothesis(
                trigger,
                hypothesis,
                linked_questions,
                answers_by_question,
                linked_evidence,
            )
            if edge_candidate is not None:
                add(edge_candidate)
                break

    for hypothesis in non_noop:
        if len(candidates) >= max_candidates - 1:
            break
        linked_questions = questions_by_hypothesis.get(hypothesis.id, [])
        linked_evidence = evidence_by_hypothesis.get(hypothesis.id, [])
        if _needs_act_decision(linked_questions, linked_evidence):
            add(
                _act_candidate_from_hypothesis(
                    trigger,
                    hypothesis,
                    linked_questions,
                    answers_by_question,
                    linked_evidence,
                )
            )
            break

    noop = hypotheses_by_id.get("H0")
    if noop is not None and len(candidates) < max_candidates:
        add(_noop_candidate(trigger, noop, evidence))

    return candidates[:max_candidates]


def _memory_decision_candidates_enabled() -> bool:
    raw = os.environ.get("INQUIRY_MEMORY_DECISION_CANDIDATES")
    if raw is None or raw.strip() == "":
        return True
    return raw.strip().lower() not in {"0", "false", "off", "no"}


def _candidate_from_hypothesis(
    trigger: TriggerContext,
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
    answers_by_question: dict[str, QuestionAnswer],
    evidence: list[EvidenceCard],
) -> MemoryDecisionCandidate:
    family = _op_family_for_hypothesis(hypothesis)
    evidence_model_ids = _evidence_model_ids(evidence, limit=5)
    reason = (
        f"Planner hypothesis {hypothesis.id} with "
        f"{len(_supporting_cards(evidence, hypothesis.id))} supporting and "
        f"{len(_counter_cards(evidence, hypothesis.id))} counter evidence cards"
    )
    if trigger.is_batch:
        reason = "Batch-level " + reason[0].lower() + reason[1:]
    return MemoryDecisionCandidate(
        candidate_id=_candidate_id("MDC", hypothesis.id),
        op_family=family,
        proposed_text=compact(hypothesis.claim, 260),
        target_model_ids=tuple(hypothesis.target_model_ids[:5]),
        source_observation_ids=_observation_ids_for_candidate(trigger, evidence),
        evidence_model_ids=evidence_model_ids,
        supporting_evidence_ids=_evidence_ids(
            _supporting_cards(evidence, hypothesis.id),
            limit=6,
        ),
        counterevidence_ids=_evidence_ids(
            _counter_cards(evidence, hypothesis.id),
            limit=4,
        ),
        uncertainty_slots=_decision_uncertainties(
            hypothesis.uncertainty_slots,
            questions,
        ),
        retrieval_targets=_retrieval_targets(hypothesis, questions),
        answer_summary=_answer_summary_for_questions(questions, answers_by_question),
        confidence=round(max(0.0, min(0.99, hypothesis.confidence)), 3),
        reason=reason,
    )


def _relationship_candidate_from_hypothesis(
    trigger: TriggerContext,
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
    answers_by_question: dict[str, QuestionAnswer],
    evidence: list[EvidenceCard],
) -> MemoryDecisionCandidate | None:
    model_ids = tuple(
        dict.fromkeys([*hypothesis.target_model_ids, *_evidence_model_ids(evidence, 4)])
    )
    if not model_ids and not trigger.member_model_ids and trigger.model_id is None:
        return None
    if not model_ids:
        model_ids = tuple(str(mid) for mid in trigger.member_model_ids[:4])
    if not model_ids and trigger.model_id is not None:
        model_ids = (str(trigger.model_id),)
    return MemoryDecisionCandidate(
        candidate_id=_candidate_id("MDC_EDGE", hypothesis.id),
        op_family="edge_insert",
        proposed_text=(
            "Decide whether a durable relationship/edge is warranted for: "
            + compact(hypothesis.claim, 220)
        ),
        target_model_ids=model_ids[:5],
        source_observation_ids=_observation_ids_for_candidate(trigger, evidence),
        evidence_model_ids=_evidence_model_ids(evidence, limit=5),
        supporting_evidence_ids=_evidence_ids(
            _supporting_cards(evidence, hypothesis.id),
            limit=6,
        ),
        counterevidence_ids=_evidence_ids(
            _counter_cards(evidence, hypothesis.id),
            limit=4,
        ),
        uncertainty_slots=_decision_uncertainties(
            (
                *hypothesis.uncertainty_slots,
                "whether the relationship is explicit enough to store",
            ),
            questions,
        ),
        retrieval_targets=_retrieval_targets(hypothesis, questions),
        suggested_edge_kinds=_suggested_edge_kinds(
            hypothesis,
            questions,
            evidence,
        ),
        write_preconditions=_relationship_write_preconditions(
            hypothesis,
            questions,
        ),
        answer_summary=_answer_summary_for_questions(questions, answers_by_question),
        confidence=round(max(0.0, min(0.92, hypothesis.confidence * 0.9)), 3),
        reason="Dependency/constraint questions imply a possible relationship op",
    )


def _act_candidate_from_hypothesis(
    trigger: TriggerContext,
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
    answers_by_question: dict[str, QuestionAnswer],
    evidence: list[EvidenceCard],
) -> MemoryDecisionCandidate:
    act_ids = _act_ids(evidence, limit=5)
    return MemoryDecisionCandidate(
        candidate_id=_candidate_id("MDC_ACT", hypothesis.id),
        op_family="act_update",
        proposed_text=(
            "Decide whether active goals, commitments, decisions, or resources "
            "need an update for: "
            + compact(hypothesis.claim, 220)
        ),
        target_act_ids=act_ids,
        source_observation_ids=_observation_ids_for_candidate(trigger, evidence),
        evidence_model_ids=_evidence_model_ids(evidence, limit=5),
        supporting_evidence_ids=_evidence_ids(
            _supporting_cards(evidence, hypothesis.id),
            limit=6,
        ),
        counterevidence_ids=_evidence_ids(
            _counter_cards(evidence, hypothesis.id),
            limit=4,
        ),
        uncertainty_slots=_decision_uncertainties(
            (
                *hypothesis.uncertainty_slots,
                "whether an active act/resource should change",
            ),
            questions,
        ),
        retrieval_targets=_retrieval_targets(hypothesis, questions),
        answer_summary=_answer_summary_for_questions(questions, answers_by_question),
        confidence=round(max(0.0, min(0.9, hypothesis.confidence * 0.85)), 3),
        reason="Action/resource evidence or questions imply a possible act op",
    )


def _noop_candidate(
    trigger: TriggerContext,
    hypothesis: Hypothesis,
    evidence: list[EvidenceCard],
) -> MemoryDecisionCandidate:
    unlinked = [
        card
        for card in evidence
        if not (
            card.supports_hypotheses
            or card.weakens_hypotheses
            or card.contradicts_hypotheses
        )
    ]
    reason = "Planner retained a no-op/background hypothesis"
    if trigger.is_batch:
        reason = "Batch may contain duplicate/background signals; preserve as provenance"
    return MemoryDecisionCandidate(
        candidate_id=_candidate_id("MDC", hypothesis.id),
        op_family="no_op",
        proposed_text=compact(hypothesis.claim, 240),
        source_observation_ids=_observation_ids_for_candidate(trigger, evidence),
        evidence_model_ids=_evidence_model_ids(unlinked or evidence, limit=4),
        supporting_evidence_ids=_evidence_ids(unlinked or evidence, limit=6),
        uncertainty_slots=_decision_uncertainties(
            hypothesis.uncertainty_slots
            or (
                "whether this is already captured",
                "whether no durable memory update is needed",
            ),
            [],
        ),
        retrieval_targets=tuple(hypothesis.evidence_needed[:5]),
        confidence=round(max(0.0, min(0.85, hypothesis.confidence)), 3),
        reason=reason,
    )


def _questions_by_hypothesis(
    questions: list[InquiryQuestion],
) -> dict[str, list[InquiryQuestion]]:
    out: dict[str, list[InquiryQuestion]] = {}
    for question in questions:
        for hypothesis_id in question.tests_hypotheses:
            out.setdefault(hypothesis_id, []).append(question)
    return out


def _evidence_by_hypothesis(
    evidence: list[EvidenceCard],
) -> dict[str, list[EvidenceCard]]:
    out: dict[str, list[EvidenceCard]] = {}
    for card in evidence:
        for hypothesis_id in (
            set(card.supports_hypotheses)
            | set(card.weakens_hypotheses)
            | set(card.contradicts_hypotheses)
        ):
            out.setdefault(hypothesis_id, []).append(card)
    return out


def _op_family_for_hypothesis(hypothesis: Hypothesis) -> str:
    delta = str(hypothesis.delta_type or "").strip().lower()
    if hypothesis.id == "H0" or delta == "no_op":
        return "no_op"
    if delta == "create":
        return "claim_insert"
    if delta in {"predict", "prediction"}:
        return "prediction"
    return "claim_update"


def _hypothesis_decision_score(
    hypothesis: Hypothesis,
    evidence: list[EvidenceCard],
    questions: list[InquiryQuestion],
) -> float:
    support = len(_supporting_cards(evidence, hypothesis.id)) * 0.10
    counter = len(_counter_cards(evidence, hypothesis.id)) * 0.08
    question_value = sum(max(0.0, q.expected_value - q.expected_cost) for q in questions)
    impact = {"critical": 0.24, "high": 0.18, "medium": 0.08, "low": 0.0}.get(
        str(hypothesis.impact_if_true or "").lower(),
        0.04,
    )
    return float(hypothesis.confidence) + support + counter + question_value + impact


def _needs_relationship_decision(
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
) -> bool:
    primitives = {question.primitive for question in questions}
    if primitives & {"DEPENDENCY", "CONSTRAINT", "GOAL_IMPACT", "RECURRENCE"}:
        return True
    text = " ".join([hypothesis.claim, *hypothesis.uncertainty_slots]).casefold()
    return any(
        token in text
        for token in (
            "block",
            "dependency",
            "critical path",
            "constraint",
            "caus",
            "because",
            "recurring",
            "pattern",
        )
    )


def _needs_act_decision(
    questions: list[InquiryQuestion],
    evidence: list[EvidenceCard],
) -> bool:
    if {question.primitive for question in questions} & {
        "COMMITMENT",
        "OWNERSHIP",
        "GOAL_IMPACT",
    }:
        return True
    return any(
        card.source_type in {"commitment", "goal", "decision", "resource"}
        for card in evidence
    )


def _answer_summary_for_questions(
    questions: list[InquiryQuestion],
    answers_by_question: dict[str, QuestionAnswer],
) -> str:
    parts: list[str] = []
    for question in questions[:6]:
        answer = answers_by_question.get(question.question_id)
        if answer is None:
            parts.append(f"{question.question_id}:{question.primitive}=unanswered")
            continue
        support = len(answer.supporting_evidence)
        counter = len(answer.counterevidence)
        text = (
            f"{question.question_id}:{question.primitive}="
            f"{answer.answer_status}"
        )
        if support or counter:
            text += f" support={support} counter={counter}"
        if answer.new_uncertainties:
            text += " new_uncertainties=" + "/".join(answer.new_uncertainties[:2])
        parts.append(text)
    return compact("; ".join(parts), 500)


def _suggested_edge_kinds(
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
    evidence: list[EvidenceCard],
) -> tuple[str, ...]:
    text = _relationship_hint_text(hypothesis, questions, evidence)
    kinds: list[str] = []

    def add(*values: str) -> None:
        for value in values:
            if value not in kinds:
                kinds.append(value)

    if any(token in text for token in ("contradict", "mutually exclusive")):
        add("contradicts")
    if any(
        token in text
        for token in (
            "counterevidence",
            "counter-evidence",
            "weakens",
            "undermines",
            "reduces confidence",
        )
    ):
        add("weakens")
    if any(
        token in text
        for token in (
            "block",
            "blocked",
            "blocking",
            "dependency",
            "depends on",
            "critical path",
            "waiting on",
            "waiting for",
            "constraint",
            "prevents",
            "requires",
        )
    ):
        add("blocks", "explains")
    if any(
        token in text
        for token in (
            "resolve",
            "resolved",
            "resolution",
            "unblock",
            "mitigation",
            "fixed",
            "accepted",
            "closed",
        )
    ):
        add("contributes_to_resolution")
    if any(
        token in text
        for token in (
            "early warning",
            "warning",
            "risk",
            "churn",
            "renewal risk",
            "leading indicator",
        )
    ):
        add("early_warning_for")
    if any(token in text for token in ("because", "explains", "root cause", "due to")):
        add("explains")
    if any(token in text for token in ("same issue", "same_issue", "recurring")):
        add("same_issue_as")
    if any(token in text for token in ("analog", "similar", "pattern")):
        add("analogous_to")
    add("supports")
    return tuple(kinds[:6])


def _relationship_write_preconditions(
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
) -> tuple[str, ...]:
    text = _relationship_hint_text(hypothesis, questions, [])
    preconditions = [
        "Write an edge only when both endpoints are concrete Models and the relation is decision-relevant.",
        "Use same_issue_as or analogous_to for cross-account similarity without an operational dependency.",
    ]
    if any(
        token in text
        for token in ("block", "dependency", "critical path", "constraint", "waiting")
    ):
        preconditions.insert(
            0,
            "Use blocks only when source evidence prevents or gates target progress; otherwise prefer explains/supports/no edge.",
        )
    if any(token in text for token in ("counter", "contradict", "weakens")):
        preconditions.insert(
            0,
            "Use weakens/contradicts only when the source is counterevidence against the target, not another risk signal.",
        )
    return tuple(preconditions[:4])


def _relationship_hint_text(
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
    evidence: list[EvidenceCard],
) -> str:
    parts: list[str] = [
        hypothesis.claim,
        *hypothesis.uncertainty_slots,
        hypothesis.delta_type or "",
        hypothesis.impact_if_true or "",
    ]
    for question in questions[:8]:
        parts.extend((question.primitive, question.question, question.stop_condition))
    for card in _rank_cards(evidence)[:8]:
        parts.append(card.summary)
    return " ".join(part for part in parts if part).casefold()


def _supporting_cards(
    evidence: list[EvidenceCard],
    hypothesis_id: str,
) -> list[EvidenceCard]:
    return [
        card
        for card in _rank_cards(evidence)
        if hypothesis_id in card.supports_hypotheses
    ]


def _counter_cards(
    evidence: list[EvidenceCard],
    hypothesis_id: str,
) -> list[EvidenceCard]:
    return [
        card
        for card in _rank_cards(evidence)
        if hypothesis_id in card.weakens_hypotheses
        or hypothesis_id in card.contradicts_hypotheses
    ]


def _rank_cards(cards: list[EvidenceCard]) -> list[EvidenceCard]:
    return sorted(cards, key=evidence_sort_key)


def _evidence_ids(cards: list[EvidenceCard], *, limit: int) -> tuple[str, ...]:
    return tuple(str(card.evidence_id) for card in _rank_cards(cards)[:limit])


def _evidence_model_ids(
    cards: list[EvidenceCard],
    limit: int,
) -> tuple[str, ...]:
    ids: list[str] = []
    seen: set[str] = set()
    for card in _rank_cards(cards):
        model_id = _source_ref_id_for(card, source_type="model")
        if model_id is None or model_id in seen:
            continue
        seen.add(model_id)
        ids.append(model_id)
        if len(ids) >= limit:
            break
    return tuple(ids)


def _act_ids(cards: list[EvidenceCard], *, limit: int) -> tuple[str, ...]:
    ids: list[str] = []
    seen: set[str] = set()
    for card in _rank_cards(cards):
        if card.source_type not in {"commitment", "goal", "decision", "resource"}:
            continue
        act_id = _source_ref_id_for(card, source_type=card.source_type)
        if act_id is None or act_id in seen:
            continue
        seen.add(act_id)
        ids.append(act_id)
        if len(ids) >= limit:
            break
    return tuple(ids)


def _source_ref_id_for(card: EvidenceCard, *, source_type: str) -> str | None:
    if card.source_type != source_type:
        return None
    if card.source_ref_id is not None:
        return str(card.source_ref_id)
    for raw in (card.raw_content_ref, card.source_ref):
        text = str(raw or "")
        prefix = f"{source_type}:"
        if text.startswith(prefix) and len(text) > len(prefix):
            return text.split(":", 1)[1]
    return None


def _observation_ids_for_candidate(
    trigger: TriggerContext,
    evidence: list[EvidenceCard],
) -> tuple[str, ...]:
    ids: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        if value is None:
            return
        text = str(value)
        if text in seen:
            return
        seen.add(text)
        ids.append(text)

    add(trigger.observation_id)
    for observation_id in trigger.observation_ids:
        add(observation_id)
    for card in _rank_cards(evidence):
        add(_source_ref_id_for(card, source_type="observation"))
    return tuple(ids[:12])


def _decision_uncertainties(
    slots: tuple[str, ...] | list[str],
    questions: list[InquiryQuestion],
) -> tuple[str, ...]:
    values = [str(slot) for slot in slots if str(slot).strip()]
    values.extend(
        question.stop_condition
        for question in questions
        if question.stop_condition and question.stop_condition not in values
    )
    return tuple(dedupe_unknowns(values)[:8])


def _retrieval_targets(
    hypothesis: Hypothesis,
    questions: list[InquiryQuestion],
) -> tuple[str, ...]:
    values = [
        question.retrieval_target
        for question in questions
        if question.retrieval_target
    ]
    values.extend(hypothesis.evidence_needed)
    return tuple(dedupe_unknowns(values)[:8])


def _candidate_id(prefix: str, raw: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw)
    clean = clean.strip("_") or "candidate"
    return f"{prefix}_{clean}"[:80]


def candidate_state_changes(
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard],
    sufficiency: SufficiencyVerdict,
) -> list[dict[str, Any]]:
    if sufficiency.status not in {"sufficient_for_reasoning", "budget_exhausted"}:
        return []
    changes: list[dict[str, Any]] = []
    if any(card.source_type == "commitment" for card in evidence) and any(
        "H1" in card.supports_hypotheses for card in evidence
    ):
        changes.append(
            {
                "kind": "possible_act_update",
                "target": "commitment",
                "operation": "transition_or_risk_update",
                "reason": "risk evidence touches an existing commitment",
            }
        )
    if any(hypothesis.id == "H3" for hypothesis in hypotheses) and any(
        card.source_type == "model" and "H3" in card.supports_hypotheses
        for card in evidence
    ):
        changes.append(
            {
                "kind": "possible_model",
                "target": "pattern_or_situation",
                "operation": "create_or_update",
                "reason": "retrieved evidence suggests recurrence",
            }
        )
    return changes[:5]


def background_summaries(evidence: list[EvidenceCard]) -> list[dict[str, Any]]:
    by_path: dict[str, list[EvidenceCard]] = {}
    for card in evidence:
        for path in card.retrieval_paths:
            by_path.setdefault(path, []).append(card)
    out: list[dict[str, Any]] = []
    for path, cards in sorted(by_path.items()):
        summarizable = [card for card in cards if not is_low_value_model_noise(card)]
        out.append(
            {
                "path": path,
                "count": len(cards),
                "sources": sorted({card.source_type for card in cards}),
                "summary": compact(
                    "; ".join(card.summary for card in summarizable[:5]), 500
                ),
            }
        )
    return out[:8]


__all__ = [
    "background_summaries",
    "candidate_state_changes",
    "compile_context_packet",
    "coverage_share",
    "evidence_card_confidence",
    "evidence_sort_key",
    "evidence_value",
    "filter_context_packet_evidence",
    "marginal_evidence_value",
    "memory_decision_candidates",
    "minimal_evidence_target",
    "minimal_floor",
    "protected_answer_ref_count",
    "rank_evidence",
    "redundancy_penalty",
    "select_minimal_sufficient_evidence",
    "state_contract_for_context_packet",
]
