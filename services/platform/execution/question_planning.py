"""LLM-backed inquiry question planning and normalization."""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from dataclasses import replace
from typing import Any

from lib.llm.provider import LLMProvider, using_usage_purpose
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .config import InquiryConfig
from .question_generation import candidate_questions
from .question_planning_provider import (
    question_planning_provider_metadata,
    select_question_planning_provider,
)
from .question_planning_runtime import (
    question_planning_max_tokens,
    question_planning_schema_name,
    question_planning_timeout_seconds,
    use_compact_question_planning_schema,
)
from .question_planning_schemas import (
    LLMBeliefDeltaSpec,
    LLMCompactQuestionPlan,
    LLMInquiryQuestionPlan,
    LLMInquiryQuestionSpec,
)
from .question_policy import clamp_float
from .reflective_rules import (
    ReflectiveRetrievalRule,
    apply_reflective_rules_to_questions,
    reflective_rules_note,
    reflective_rules_prompt_payload,
)
from .reconstruction_state import (
    planner_reconstruction_payload,
    reconstruction_state_note,
)
from .question_text import (
    clean_question_anchor,
    clean_question_focus_phrase,
    fallback_focus_from_delta_claim,
    is_specific_focus_phrase,
    looks_like_machine_identifier,
    question_anchors,
    specific_question,
    truncate_text,
)
from .types import EvidenceCard, Hypothesis, InquiryQuestion, ReconstructionState

ALLOWED_QUESTION_PRIMITIVES = {
    "DEPENDENCY",
    "COMMITMENT",
    "CONSTRAINT",
    "COUNTEREVIDENCE",
    "OWNERSHIP",
    "GOAL_IMPACT",
    "RECURRENCE",
}

QUESTION_ID_BY_PRIMITIVE = {
    "DEPENDENCY": "Q_CRITICAL_PATH",
    "COMMITMENT": "Q_ACTIVE_COMMITMENT",
    "CONSTRAINT": "Q_CONSTRAINT",
    "COUNTEREVIDENCE": "Q_COUNTEREVIDENCE",
    "OWNERSHIP": "Q_OWNER",
    "GOAL_IMPACT": "Q_GOAL_IMPACT",
    "RECURRENCE": "Q_RECURRENCE",
}

DEFAULT_TARGET_BY_PRIMITIVE = {
    "DEPENDENCY": "commitment_graph+recent_observations",
    "COMMITMENT": "active_commitments",
    "CONSTRAINT": "constraints+resource_edges",
    "COUNTEREVIDENCE": "semantic_counterevidence+recent_observations",
    "OWNERSHIP": "commitment_owners+actor_scope",
    "GOAL_IMPACT": "goal_resource_bridge",
    "RECURRENCE": "pattern+model_edges",
}

DEFAULT_STOP_BY_PRIMITIVE = {
    "DEPENDENCY": "critical-path evidence or counterevidence found",
    "COMMITMENT": "matching active commitment found or ruled out",
    "CONSTRAINT": "binding constraint identified or ruled out",
    "COUNTEREVIDENCE": "credible alternate explanation found or absent",
    "OWNERSHIP": "owner identified or human validation required",
    "GOAL_IMPACT": "goal/customer/resource impact identified",
    "RECURRENCE": "pattern support or absence established",
}

ALLOWED_DELTA_TYPES = {
    "create",
    "update",
    "weaken",
    "split",
    "merge",
    "supersede",
    "no_op",
}

DEFAULT_COST_BY_PRIMITIVE = {
    "DEPENDENCY": 0.24,
    "COMMITMENT": 0.18,
    "CONSTRAINT": 0.24,
    "COUNTEREVIDENCE": 0.30,
    "OWNERSHIP": 0.22,
    "GOAL_IMPACT": 0.20,
    "RECURRENCE": 0.36,
}


async def candidate_questions_for_round(
    trigger: TriggerContext,
    baseline: RetrievalResult,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
    *,
    llm_provider: LLMProvider | None,
    config: InquiryConfig,
    round_index: int,
    reflective_rules: tuple[ReflectiveRetrievalRule, ...] = (),
    reconstruction_state: ReconstructionState | None = None,
) -> tuple[list[InquiryQuestion], dict[str, Any]]:
    deterministic = candidate_questions(trigger, hypotheses, evidence_by_key, unknowns)
    deterministic, deterministic_rule_note = _apply_reflective_question_rules(
        deterministic,
        trigger,
        unknowns,
        config=config,
        reflective_rules=reflective_rules,
    )
    if trigger.kind != "T1":
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "non_t1_trigger_uses_seeded_retrieval",
            "candidate_count": len(deterministic),
            **_reconstruction_note(reconstruction_state),
            **deterministic_rule_note,
        }
    if not config.llm_question_planning_enabled:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "disabled_by_config",
            "candidate_count": len(deterministic),
            **_reconstruction_note(reconstruction_state),
            **deterministic_rule_note,
        }
    if llm_provider is None:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": "llm_provider_missing",
            "candidate_count": len(deterministic),
            **_reconstruction_note(reconstruction_state),
            **deterministic_rule_note,
        }
    planning_provider = select_question_planning_provider(llm_provider)
    provider_metadata = question_planning_provider_metadata(planning_provider)

    try:
        plan_call = generate_llm_question_plan(
            trigger,
            baseline,
            hypotheses,
            evidence_by_key,
            unknowns,
            llm_provider=planning_provider,
            config=config,
            max_tokens=question_planning_max_tokens(config, planning_provider),
            reflective_rules=reflective_rules,
            reconstruction_state=reconstruction_state,
        )
        timeout_s = question_planning_timeout_seconds(planning_provider)
        # Cost-plan 0.1: tag planning LLM spend as question_planning.
        with using_usage_purpose("question_planning"):
            if timeout_s > 0:
                plan = await asyncio.wait_for(plan_call, timeout=timeout_s)
            else:
                plan = await plan_call
        belief_delta_hypotheses = normalize_llm_belief_delta_hypotheses(
            plan.belief_deltas,
            trigger=trigger,
        )
        question_quality_notes: list[dict[str, Any]] = []
        belief_delta_questions = candidate_questions_from_belief_deltas(
            trigger,
            belief_delta_hypotheses,
            hypotheses=hypotheses,
            quality_notes=question_quality_notes,
        )
        llm_questions = normalize_llm_questions(
            plan.questions,
            hypotheses,
            trigger=trigger,
            quality_notes=question_quality_notes,
        )
        if not llm_questions and not belief_delta_questions:
            return deterministic, {
                "round": round_index,
                "mode": "deterministic_fallback",
                "reason": "llm_returned_no_valid_questions",
                "candidate_count": len(deterministic),
                "llm_rationale": plan.rationale,
                "belief_delta_count": len(belief_delta_hypotheses),
                **_reconstruction_note(reconstruction_state),
                **deterministic_rule_note,
                **provider_metadata,
            }
        primary_questions = llm_questions or belief_delta_questions
        safety_questions = (
            [*belief_delta_questions, *deterministic]
            if llm_questions
            else deterministic
        )
        merged, safety_added = merge_llm_and_safety_questions(
            primary_questions,
            safety_questions,
        )
        merged, rule_note = _apply_reflective_question_rules(
            merged,
            trigger,
            unknowns,
            config=config,
            reflective_rules=reflective_rules,
        )
        return merged, {
            "round": round_index,
            "mode": "llm" if llm_questions else "llm_delta",
            "llm_candidate_count": len(llm_questions),
            "belief_delta_count": len(belief_delta_hypotheses),
            "belief_delta_question_count": len(belief_delta_questions),
            "belief_delta_types": [
                h.delta_type for h in belief_delta_hypotheses if h.delta_type
            ],
            "belief_delta_claims": [h.claim for h in belief_delta_hypotheses[:5]],
            "safety_candidate_count": safety_added,
            "candidate_count": len(merged),
            "llm_rationale": plan.rationale,
            "llm_primitives": [q.primitive for q in llm_questions],
            "llm_schema": question_planning_schema_name(planning_provider),
            "question_quality": question_quality_summary(question_quality_notes),
            **_reconstruction_note(reconstruction_state),
            **rule_note,
            **provider_metadata,
        }
    except Exception as exc:
        return deterministic, {
            "round": round_index,
            "mode": "deterministic_fallback",
            "reason": type(exc).__name__,
            "detail": str(exc)[:240],
            "candidate_count": len(deterministic),
            **_reconstruction_note(reconstruction_state),
            **deterministic_rule_note,
            **provider_metadata,
        }


def _reconstruction_note(
    reconstruction_state: ReconstructionState | None,
) -> dict[str, Any]:
    note = reconstruction_state_note(reconstruction_state)
    return {"reconstruction_state": note} if note else {}


def _apply_reflective_question_rules(
    questions: list[InquiryQuestion],
    trigger: TriggerContext,
    unknowns: set[str],
    *,
    config: InquiryConfig,
    reflective_rules: tuple[ReflectiveRetrievalRule, ...],
) -> tuple[list[InquiryQuestion], dict[str, Any]]:
    if not reflective_rules or not config.reflective_rules_enabled:
        return questions, {}
    applied = not config.reflective_rules_shadow_only
    note = reflective_rules_note(
        reflective_rules,
        applied=applied,
        shadow_only=bool(config.reflective_rules_shadow_only),
    )
    if not applied:
        return questions, note
    return (
        apply_reflective_rules_to_questions(
            questions,
            trigger,
            unknowns=unknowns,
            rules=reflective_rules,
            score_boost=float(config.reflective_rule_score_boost),
        ),
        note,
    )


async def generate_llm_question_plan(
    trigger: TriggerContext,
    baseline: RetrievalResult,
    hypotheses: tuple[Hypothesis, ...],
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    unknowns: set[str],
    *,
    llm_provider: LLMProvider,
    config: InquiryConfig,
    max_tokens: int | None = None,
    reflective_rules: tuple[ReflectiveRetrievalRule, ...] = (),
    reconstruction_state: ReconstructionState | None = None,
) -> LLMInquiryQuestionPlan:
    active_reflective_rules = (
        reflective_rules
        if config.reflective_rules_enabled and not config.reflective_rules_shadow_only
        else ()
    )
    if use_compact_question_planning_schema(llm_provider):
        system = (
            "You compile retrieval questions for Fyralis model updates. "
            "Extract atomic belief deltas, then ask only the few specific "
            "questions needed to decide which models change. Keep text short. "
            "Never copy a full claim into a question. Return JSON only."
        )
        user = json.dumps(
            {
                "task": "compile retrieval question plan",
                "p": sorted(ALLOWED_QUESTION_PRIMITIVES),
                "types": [
                    "create",
                    "update",
                    "weaken",
                    "split",
                    "merge",
                    "supersede",
                    "no_op",
                ],
                "signal": {
                    "kind": trigger.kind,
                    "text": truncate_text(trigger_text_for_planning(trigger), 700),
                    "entities": trigger.seed_entity_ids[:8],
                    "actors": len(trigger.scope_actors),
                    "at": (
                        trigger.seed_occurred_at.isoformat()
                        if trigger.seed_occurred_at
                        else None
                    ),
                },
                "h": [
                    {
                        "id": h.id,
                        "claim": truncate_text(h.claim, 180),
                        "conf": h.confidence,
                        "impact": h.impact_if_true,
                    }
                    for h in hypotheses[:4]
                ],
                "u": sorted(unknowns)[:8],
                "base": compact_baseline_snapshot_for_question_planning(
                    baseline,
                    evidence_by_key,
                ),
                **(
                    {"recon": planner_reconstruction_payload(reconstruction_state)}
                    if reconstruction_state is not None
                    else {}
                ),
                **(
                    {
                        "learned_rules": reflective_rules_prompt_payload(
                            active_reflective_rules
                        )
                    }
                    if active_reflective_rules
                    else {}
                ),
                "rules": [
                    "d: 1-4 atomic belief deltas",
                    "q: 2-3 questions",
                    "q[].p must be one allowed primitive",
                    "q[].q must be grammatical and under 22 words",
                    "ask about missing context, counterevidence, ownership, recurrence, dependencies, or constraints",
                    "avoid questions already answered by base evidence",
                ],
            },
            default=str,
            separators=(",", ":"),
        )
        compact = await llm_provider.structured(
            system=system,
            user=user,
            schema=LLMCompactQuestionPlan,
            temperature=config.llm_question_temperature,
            max_tokens=max_tokens or config.llm_question_max_tokens,
        )
        return expand_compact_question_plan(compact)

    system = (
        "You are a bounded semantic compiler for Fyralis' model-update "
        "pipeline. First extract atomic belief-delta candidates from the "
        "signal, then choose only the few retrieval questions that will decide "
        "which existing models must be created, updated, weakened, split, "
        "merged, superseded, or left unchanged. Prefer specific, "
        "discriminating questions over broad searches. Always include "
        "counterevidence when the signal makes a material claim. Keep outputs "
        "short. Never paste a whole belief_delta claim into a question; turn "
        "it into a compact noun phrase first. Return JSON only."
    )
    user = json.dumps(
        {
            "task": (
                "Compile belief deltas and generate the next retrieval "
                "questions for this signal."
            ),
            "allowed_primitives": sorted(ALLOWED_QUESTION_PRIMITIVES),
            "allowed_delta_types": [
                "create",
                "update",
                "weaken",
                "split",
                "merge",
                "supersede",
                "no_op",
            ],
            "signal": {
                "kind": trigger.kind,
                "text": trigger_text_for_planning(trigger),
                "seed_entities": trigger.seed_entity_ids[:12],
                "scope_actor_count": len(trigger.scope_actors),
                "occurred_at": (
                    trigger.seed_occurred_at.isoformat()
                    if trigger.seed_occurred_at
                    else None
                ),
            },
            "hypotheses": [
                {
                    "id": h.id,
                    "claim": h.claim,
                    "confidence": h.confidence,
                    "impact_if_true": h.impact_if_true,
                }
                for h in hypotheses
            ],
            "unknowns": sorted(unknowns)[:12],
            "baseline_snapshot": baseline_snapshot_for_question_planning(
                baseline,
                evidence_by_key,
            ),
            **(
                {
                    "reconstruction_state": planner_reconstruction_payload(
                        reconstruction_state
                    )
                }
                if reconstruction_state is not None
                else {}
            ),
            **(
                {
                    "learned_rules": reflective_rules_prompt_payload(
                        active_reflective_rules
                    )
                }
                if active_reflective_rules
                else {}
            ),
            "guidance": [
                "Return 1 to 4 belief_deltas before questions.",
                "Each belief_delta should be an atomic claim, not a summary of the whole signal.",
                "Use uncertainty_slots to name what retrieval must resolve.",
                "Use evidence_needed to name the source/evidence type that would resolve each slot.",
                "Return 2 to 3 questions.",
                "Use primitive names exactly as provided.",
                "Each question must be one grammatical sentence under 22 words.",
                "Do not copy claim_atom verbatim into any question.",
                "Avoid question starts like 'Has <full sentence>' or 'Is <full sentence> actually'.",
                "Use expected_value for likely decision value, not topicality.",
                "Use expected_cost for retrieval breadth/cost; broad searches cost more.",
                "Avoid questions whose answer is already clear from baseline evidence.",
                "For weak chatter/no-op signals, ask narrow disambiguation and counterevidence questions only.",
            ],
        },
        default=str,
    )
    return await llm_provider.structured(
        system=system,
        user=user,
        schema=LLMInquiryQuestionPlan,
        temperature=config.llm_question_temperature,
        max_tokens=max_tokens or config.llm_question_max_tokens,
    )


def trigger_text_for_planning(trigger: TriggerContext) -> str:
    from .routing import trigger_text

    return trigger_text(trigger)


def baseline_snapshot_for_question_planning(
    baseline: RetrievalResult,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
) -> dict[str, Any]:
    cards = sorted(
        evidence_by_key.values(),
        key=lambda c: (-float(c.score), c.source_type, c.summary),
    )
    return {
        "model_count": len(baseline.models),
        "observation_count": len(baseline.observations),
        "commitment_count": len(baseline.acts.get("commitments", [])),
        "goal_count": len(baseline.acts.get("goals", [])),
        "decision_count": len(baseline.acts.get("decisions", [])),
        "top_models": [
            {
                "id": str(model.id),
                "summary": truncate_text(
                    getattr(model, "natural", "")
                    or json.dumps(
                        getattr(model, "proposition", {}) or {},
                        default=str,
                    ),
                    220,
                ),
                "confidence": getattr(model, "confidence", None),
                "score": float(baseline.model_scores.get(model.id, 0.0)),
            }
            for model in baseline.models[:10]
        ],
        "top_evidence": [
            {
                "source_type": card.source_type,
                "summary": truncate_text(card.summary, 220),
                "score": round(float(card.score), 4),
            }
            for card in cards[:12]
        ],
    }


def compact_baseline_snapshot_for_question_planning(
    baseline: RetrievalResult,
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
) -> dict[str, Any]:
    cards = sorted(
        evidence_by_key.values(),
        key=lambda c: (-float(c.score), c.source_type, c.summary),
    )
    return {
        "models": len(baseline.models),
        "obs": len(baseline.observations),
        "acts": {
            "commitments": len(baseline.acts.get("commitments", [])),
            "goals": len(baseline.acts.get("goals", [])),
            "decisions": len(baseline.acts.get("decisions", [])),
        },
        "m": [
            {
                "id": str(model.id),
                "s": truncate_text(
                    getattr(model, "natural", "")
                    or json.dumps(
                        getattr(model, "proposition", {}) or {},
                        default=str,
                    ),
                    150,
                ),
                "score": round(float(baseline.model_scores.get(model.id, 0.0)), 3),
            }
            for model in baseline.models[:6]
        ],
        "e": [
            {
                "src": card.source_type,
                "s": truncate_text(card.summary, 150),
                "score": round(float(card.score), 3),
            }
            for card in cards[:8]
        ],
    }


def expand_compact_question_plan(
    plan: LLMCompactQuestionPlan,
) -> LLMInquiryQuestionPlan:
    deltas = [
        LLMBeliefDeltaSpec(
            delta_id=delta.i,
            claim_atom=delta.claim,
            delta_type=delta.type,
            affected_entities=delta.entities,
            uncertainty_slots=delta.slots,
            evidence_needed=delta.evidence,
            impact_if_true=delta.impact,
            confidence=delta.conf,
        )
        for delta in plan.d
    ]
    questions = [
        LLMInquiryQuestionSpec(
            primitive=question.p,
            question=question.q,
            retrieval_target=None,
            expected_value=question.v,
            expected_cost=question.c,
            tests_hypotheses=[],
            stop_condition=None,
        )
        for question in plan.q
    ]
    return LLMInquiryQuestionPlan(
        rationale=plan.r,
        belief_deltas=deltas,
        questions=questions,
    )


def normalize_llm_belief_delta_hypotheses(
    specs: list[LLMBeliefDeltaSpec],
    *,
    trigger: TriggerContext,
) -> list[Hypothesis]:
    anchors = question_anchors(trigger)
    out: list[Hypothesis] = []
    seen_claims: set[str] = set()
    for index, spec in enumerate(specs[:5], start=1):
        claim = clean_question_anchor(spec.claim_atom)
        if len(claim) < 8:
            continue
        claim = truncate_text(claim, 240)
        claim_key = claim.casefold()
        if claim_key in seen_claims:
            continue
        seen_claims.add(claim_key)
        delta_type = normalize_delta_type(spec.delta_type)
        entities = clean_delta_items(spec.affected_entities, limit=8)
        if not entities and anchors.subject != "this signal":
            entities = (anchors.subject,)
        uncertainties = clean_delta_items(spec.uncertainty_slots, limit=8)
        if not uncertainties:
            uncertainties = fallback_uncertainty_slots_for_delta(delta_type)
        evidence_needed = clean_delta_items(spec.evidence_needed, limit=8)
        hypothesis_id = clean_question_anchor(spec.delta_id or "") or f"D{index}"
        hypothesis_id = re.sub(r"[^A-Za-z0-9_:-]+", "_", hypothesis_id)[:24]
        out.append(
            Hypothesis(
                id=hypothesis_id or f"D{index}",
                claim=claim,
                confidence=clamp_float(spec.confidence, 0.0, 1.0),
                impact_if_true=normalize_impact_label(spec.impact_if_true),
                delta_type=delta_type,
                target_model_ids=clean_delta_items(spec.target_model_ids, limit=5),
                affected_entities=entities,
                uncertainty_slots=uncertainties,
                evidence_needed=evidence_needed,
                source="llm_delta",
            )
        )
    return out


def candidate_questions_from_belief_deltas(
    trigger: TriggerContext,
    belief_deltas: list[Hypothesis],
    *,
    hypotheses: tuple[Hypothesis, ...],
    quality_notes: list[dict[str, Any]] | None = None,
) -> list[InquiryQuestion]:
    known_hypothesis_ids = {h.id for h in hypotheses}
    questions: list[InquiryQuestion] = []
    seen: set[tuple[str, str]] = set()
    for delta in belief_deltas:
        slots = delta.uncertainty_slots or fallback_uncertainty_slots_for_delta(
            delta.delta_type
        )
        for slot in slots[:4]:
            primitive = primitive_for_delta_slot(slot, delta.delta_type)
            question = question_from_delta_slot(delta, slot, primitive, trigger)
            question = quality_control_question_text(
                question,
                primitive,
                trigger,
                source="belief_delta",
                quality_notes=quality_notes,
                delta=delta,
                slot=slot,
            )
            key = (primitive, question.casefold())
            if key in seen:
                continue
            seen.add(key)
            expected_cost = DEFAULT_COST_BY_PRIMITIVE.get(primitive, 0.24)
            expected_value = delta_question_expected_value(delta, primitive)
            tests = tests_for_delta_question(
                primitive,
                delta,
                known_hypothesis_ids=known_hypothesis_ids,
            )
            questions.append(
                InquiryQuestion(
                    question_id=QUESTION_ID_BY_PRIMITIVE[primitive],
                    question=question,
                    primitive=primitive,
                    tests_hypotheses=tests,
                    expected_value=expected_value,
                    expected_cost=expected_cost,
                    retrieval_target=DEFAULT_TARGET_BY_PRIMITIVE[primitive],
                    stop_condition=DEFAULT_STOP_BY_PRIMITIVE[primitive],
                    score=round(expected_value - expected_cost + 0.12, 4),
                )
            )
    return sorted(
        questions,
        key=lambda q: (-q.score, q.expected_cost, q.primitive, q.question),
    )[:12]


def normalize_delta_type(value: str | None) -> str:
    delta_type = re.sub(r"[^a-z_]+", "_", str(value or "update").casefold()).strip("_")
    aliases = {
        "create_new": "create",
        "new": "create",
        "modify": "update",
        "weaken_existing": "weaken",
        "retire": "supersede",
        "obsolete": "supersede",
        "none": "no_op",
        "noop": "no_op",
        "no_update": "no_op",
    }
    delta_type = aliases.get(delta_type, delta_type)
    return delta_type if delta_type in ALLOWED_DELTA_TYPES else "update"


def normalize_impact_label(value: str | None) -> str:
    label = str(value or "medium").casefold().strip()
    if label in {"high", "medium", "low"}:
        return label
    if label in {"critical", "severe"}:
        return "high"
    if label in {"minor", "small"}:
        return "low"
    return "medium"


def clean_delta_items(
    values: list[Any] | tuple[Any, ...], *, limit: int
) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values[:limit]:
        clean = clean_question_anchor(str(value or ""))
        if not clean or looks_like_machine_identifier(clean):
            continue
        clean = truncate_text(clean, 140)
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return tuple(out)


def fallback_uncertainty_slots_for_delta(delta_type: str | None) -> tuple[str, ...]:
    if delta_type in {"weaken", "supersede"}:
        return (
            "what evidence contradicts or weakens the existing belief",
            "which prior model is now stale",
        )
    if delta_type in {"split", "merge"}:
        return (
            "which existing beliefs should be separated or combined",
            "what evidence distinguishes the competing interpretations",
        )
    if delta_type == "no_op":
        return (
            "whether this signal is already captured",
            "what evidence supports no model update",
        )
    return (
        "which existing model should change",
        "what evidence would weaken this interpretation",
        "who owns the next action",
    )


def primitive_for_delta_slot(slot: str, delta_type: str | None) -> str:
    lower = slot.casefold()
    if re.search(r"\b(owner|owns|accountable|who|assignee|responsible)\b", lower):
        return "OWNERSHIP"
    if re.search(r"\b(commitment|promise|deadline|outcome|deliverable)\b", lower):
        return "COMMITMENT"
    if re.search(r"\b(resource|policy|capacity|constraint|quota|blocked by)\b", lower):
        return "CONSTRAINT"
    if re.search(r"\b(recur|recurrence|pattern|before|similar|repeat)\b", lower):
        return "RECURRENCE"
    if re.search(r"\b(goal|customer|revenue|arr|resource|impact|risk)\b", lower):
        return "GOAL_IMPACT"
    if re.search(
        r"\b(counter|weaken|contradict|falsif|stale|supersede|wrong|obsolete)\b",
        lower,
    ):
        return "COUNTEREVIDENCE"
    if re.search(r"\b(dependency|critical path|blocker|blocking|depends)\b", lower):
        return "DEPENDENCY"
    if delta_type in {"weaken", "supersede", "no_op"}:
        return "COUNTEREVIDENCE"
    if delta_type in {"create", "update"}:
        return "DEPENDENCY"
    return "GOAL_IMPACT"


def question_from_delta_slot(
    delta: Hypothesis,
    slot: str,
    primitive: str,
    trigger: TriggerContext,
) -> str:
    subject = (
        ", ".join(delta.affected_entities[:3])
        if delta.affected_entities
        else question_anchors(trigger).subject
    )
    focus = delta_question_focus(delta, slot, trigger)
    if primitive == "OWNERSHIP":
        question = f"Who owns resolving {focus} for {subject}?"
    elif primitive == "COMMITMENT":
        question = f"Which active commitment or promised outcome would change if {focus} is true for {subject}?"
    elif primitive == "CONSTRAINT":
        question = f"Which resource, policy, or capacity constraint is blocking {focus} for {subject}?"
    elif primitive == "RECURRENCE":
        pattern_focus = focus if "pattern" in focus.casefold() else f"{focus} pattern"
        question = (
            f"Has this {pattern_focus} appeared before for {subject}, or is it new?"
        )
    elif primitive == "GOAL_IMPACT":
        question = f"Which customer goal, revenue path, or scarce resource is affected by {focus} for {subject}?"
    elif primitive == "COUNTEREVIDENCE":
        question = f"What evidence would weaken or falsify the {focus} interpretation for {subject}?"
    else:
        question = (
            f"Is {focus} a blocking dependency or critical-path issue for {subject}?"
        )
    return truncate_text(" ".join(question.split()), 240)


def delta_question_focus(
    delta: Hypothesis,
    slot: str,
    trigger: TriggerContext,
) -> str:
    candidates = [
        slot,
        *delta.evidence_needed,
        *delta.uncertainty_slots,
        delta.claim,
    ]
    for candidate in candidates:
        focus = clean_question_focus_phrase(candidate)
        if is_specific_focus_phrase(focus):
            return truncate_text(focus, 120)
    return fallback_focus_from_delta_claim(delta.claim, trigger)


def delta_question_expected_value(delta: Hypothesis, primitive: str) -> float:
    impact_boost = {"high": 0.18, "medium": 0.10, "low": 0.02}.get(
        delta.impact_if_true,
        0.10,
    )
    delta_boost = {
        "weaken": 0.08,
        "supersede": 0.08,
        "split": 0.06,
        "merge": 0.06,
        "update": 0.05,
        "create": 0.04,
        "no_op": 0.02,
    }.get(delta.delta_type or "update", 0.04)
    primitive_boost = 0.04 if primitive in {"COUNTEREVIDENCE", "OWNERSHIP"} else 0.0
    return round(
        clamp_float(
            0.55
            + float(delta.confidence) * 0.22
            + impact_boost
            + delta_boost
            + primitive_boost,
            0.0,
            0.98,
        ),
        4,
    )


def tests_for_delta_question(
    primitive: str,
    delta: Hypothesis,
    *,
    known_hypothesis_ids: set[str],
) -> tuple[str, ...]:
    preferred: tuple[str, ...]
    if primitive == "COUNTEREVIDENCE" or delta.delta_type in {
        "weaken",
        "supersede",
        "no_op",
    }:
        preferred = ("H1", "H0")
    elif primitive in {"OWNERSHIP", "COMMITMENT", "GOAL_IMPACT"}:
        preferred = ("H2", "H1")
    elif primitive == "RECURRENCE":
        preferred = ("H3", "H0")
    else:
        preferred = ("H1",)
    tests = tuple(hid for hid in preferred if hid in known_hypothesis_ids)
    return tests or tuple(sorted(known_hypothesis_ids))[:1] or ("H1",)


def quality_control_question_text(
    question: str,
    primitive: str,
    trigger: TriggerContext,
    *,
    source: str,
    quality_notes: list[dict[str, Any]] | None = None,
    delta: Hypothesis | None = None,
    slot: str | None = None,
) -> str:
    clean = " ".join(question.split()).strip()
    reason = question_quality_failure_reason(clean, primitive)
    if reason is None:
        return truncate_text(clean, 240)
    if reason == "missing_question_mark":
        repaired = punctuate_question_text(clean)
        if quality_notes is not None:
            quality_notes.append(
                {
                    "source": source,
                    "primitive": primitive,
                    "repair_reason": "punctuation_added",
                    "original": truncate_text(clean, 160),
                    "repaired": truncate_text(repaired, 160),
                }
            )
        return repaired

    repaired = repair_question_text(
        primitive,
        trigger,
        delta=delta,
        slot=slot,
    )
    if quality_notes is not None:
        quality_notes.append(
            {
                "source": source,
                "primitive": primitive,
                "repair_reason": reason,
                "original": truncate_text(clean, 160),
                "repaired": truncate_text(repaired, 160),
            }
        )
    return repaired


def punctuate_question_text(question: str) -> str:
    clean = question.rstrip(" .,:;")
    return truncate_text(f"{clean}?", 240)


def question_quality_failure_reason(question: str, primitive: str) -> str | None:
    lower = f" {question.casefold()} "
    if len(question) > 240:
        return "too_long"
    if not question.endswith("?"):
        return "missing_question_mark"
    if re.search(
        r"\b(has|is|are|does|do)\s+[A-Z][^.?!]{0,90}\s+(is|are|has|should|must|will|may)\b",
        question,
    ):
        return "nested_clause_subject"
    if primitive == "CONSTRAINT" and re.search(
        r"what resource, policy, or capacity constraint determines\s+(is|are|whether|if|should|does)\b",
        lower,
    ):
        return "constraint_template_clause_leak"
    if primitive == "DEPENDENCY" and re.search(
        r"^is\s+.+\s+(should|is|are|has|will|must|may)\s+.+actually on the critical path",
        lower.strip(),
    ):
        return "dependency_template_clause_leak"
    if primitive == "RECURRENCE" and re.search(
        r"^has\s+.+\s+(is|are|has|should|must|will|may)\s+.+appeared before",
        lower.strip(),
    ):
        return "recurrence_template_clause_leak"
    if re.search(
        r"\bblocking\s+(blocker|constraint|counterevidence|dependency|goal impact|issue type|ownership|recurrence|status)\s+for\b",
        lower,
    ):
        return "generic_focus_leak"
    if re.search(
        r"\bblocking\s+(blocker|constraint|counterevidence|dependency|goal impact|issue type|ownership|recurrence|status)\s*:",
        lower,
    ):
        return "generic_focus_leak"
    if re.search(
        r"^is\s+(blocker|constraint|dependency|issue type|status)\s+a blocking dependency",
        lower.strip(),
    ):
        return "generic_focus_leak"
    if "..." in question and len(question) > 180:
        return "truncated_clause"
    return None


def repair_question_text(
    primitive: str,
    trigger: TriggerContext,
    *,
    delta: Hypothesis | None = None,
    slot: str | None = None,
) -> str:
    if delta is None:
        return specific_question(primitive, question_anchors(trigger))
    return question_from_delta_slot(delta, slot or "", primitive, trigger)


def question_quality_summary(
    quality_notes: list[dict[str, Any]],
) -> dict[str, Any]:
    if not quality_notes:
        return {"repairs": 0}
    by_reason = Counter(
        str(note.get("repair_reason") or "unknown") for note in quality_notes
    )
    by_source = Counter(str(note.get("source") or "unknown") for note in quality_notes)
    return {
        "repairs": len(quality_notes),
        "by_reason": dict(by_reason.most_common()),
        "by_source": dict(by_source.most_common()),
        "examples": quality_notes[:3],
    }


def normalize_llm_questions(
    specs: list[LLMInquiryQuestionSpec],
    hypotheses: tuple[Hypothesis, ...],
    *,
    trigger: TriggerContext,
    quality_notes: list[dict[str, Any]] | None = None,
) -> list[InquiryQuestion]:
    hypothesis_ids = {h.id for h in hypotheses}
    fallback_hids = tuple(h.id for h in hypotheses if h.id != "H0")[:2] or ("H1",)
    out: list[InquiryQuestion] = []
    seen_primitives: set[str] = set()
    for spec in specs:
        primitive = spec.primitive.strip().upper()
        if primitive not in ALLOWED_QUESTION_PRIMITIVES:
            continue
        if primitive in seen_primitives:
            continue
        question = " ".join(spec.question.split())
        if len(question) < 8:
            continue
        question = quality_control_question_text(
            question,
            primitive,
            trigger,
            source="llm_question",
            quality_notes=quality_notes,
        )
        expected_value = clamp_float(spec.expected_value, 0.0, 1.0)
        expected_cost = clamp_float(spec.expected_cost, 0.0, 1.0)
        tests = tuple(
            hid
            for hid in spec.tests_hypotheses
            if isinstance(hid, str) and hid in hypothesis_ids
        )[:4]
        if not tests:
            tests = fallback_hids
        score = round(expected_value - expected_cost + 0.12, 4)
        out.append(
            InquiryQuestion(
                question_id=QUESTION_ID_BY_PRIMITIVE[primitive],
                question=question[:240],
                primitive=primitive,
                tests_hypotheses=tests,
                expected_value=expected_value,
                expected_cost=expected_cost,
                retrieval_target=(
                    " ".join((spec.retrieval_target or "").split())[:120]
                    or DEFAULT_TARGET_BY_PRIMITIVE[primitive]
                ),
                stop_condition=(
                    " ".join((spec.stop_condition or "").split())[:180]
                    or DEFAULT_STOP_BY_PRIMITIVE[primitive]
                ),
                score=score,
            )
        )
        seen_primitives.add(primitive)
    return out


def merge_llm_and_safety_questions(
    llm_questions: list[InquiryQuestion],
    deterministic: list[InquiryQuestion],
) -> tuple[list[InquiryQuestion], int]:
    by_primitive = {q.primitive: q for q in llm_questions}
    safety_added = 0
    for question in deterministic:
        existing = by_primitive.get(question.primitive)
        if existing is not None:
            if (
                question.score > existing.score
                or question.expected_value > existing.expected_value
            ):
                by_primitive[question.primitive] = replace(
                    existing,
                    expected_value=max(
                        existing.expected_value, question.expected_value
                    ),
                    expected_cost=min(existing.expected_cost, question.expected_cost),
                    tests_hypotheses=(
                        existing.tests_hypotheses or question.tests_hypotheses
                    ),
                    score=max(existing.score, question.score),
                )
            continue
        force_high_value_safety = question.primitive in {
            "CONSTRAINT",
            "DEPENDENCY",
            "OWNERSHIP",
            "GOAL_IMPACT",
            "RECURRENCE",
        } and (question.expected_value >= 0.86 or question.score >= 0.75)
        if question.primitive == "OWNERSHIP":
            force_high_value_safety = (
                question.expected_value >= 0.70 or question.score >= 0.62
            )
        if (
            question.primitive == "COUNTEREVIDENCE"
            or len(by_primitive) < 4
            or force_high_value_safety
        ):
            by_primitive[question.primitive] = question
            safety_added += 1
    ordered: list[InquiryQuestion] = []
    for primitive in (
        "COUNTEREVIDENCE",
        "DEPENDENCY",
        "CONSTRAINT",
        "COMMITMENT",
        "OWNERSHIP",
        "GOAL_IMPACT",
        "RECURRENCE",
    ):
        question = by_primitive.get(primitive)
        if question is not None:
            ordered.append(question)
    return ordered, safety_added
