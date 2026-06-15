"""Closed-loop learning for reflective retrieval rules.

This module turns completed inquiry traces into small, replay-tested rule packs.
The rules remain advisory policy memory: they steer future planning/retrieval,
but they do not assert facts about the company.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext

from .config import _env_bool, _env_float, _env_int
from .evidence_utils import jsonable, material_tokens, stable_hash
from .motif_utils import packet_used_evidence_ids, safe_uuid
from .reflective_rules import (
    ReflectiveRetrievalRule,
    apply_reflective_rules_to_actions,
    apply_reflective_rules_to_questions,
    reflective_signature_for,
)
from .types import EvidenceCard, InquiryQuestion, InquiryResult, RetrievalAction


@dataclass(frozen=True, slots=True)
class ReflectiveRuleCandidate:
    signature: dict[str, Any]
    rule_pack: dict[str, Any]
    credit: float
    cost: float
    rationale: str


@dataclass(frozen=True, slots=True)
class ReflectiveRuleReplayResult:
    baseline_score: float
    candidate_score: float
    utility_delta: float
    decision: str
    diagnostics: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReflectiveRuleAttribution:
    rule_id: UUID
    question_id: str
    question_primitive: str
    action_path: str | None
    action_target: str | None
    action_payload: dict[str, Any]
    evidence_refs: tuple[dict[str, Any], ...]
    evidence_count: int
    selected_evidence_count: int
    credit: float
    cost: float
    outcome_score: float


async def learn_reflective_rules(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    if await _table_missing(conn, "reflective_retrieval_rules"):
        return

    attributions = reflective_rule_attributions_from_result(result)
    if attributions and _env_bool(
        "INQUIRY_REFLECTIVE_RULE_ATTRIBUTION_ENABLED",
        True,
    ):
        await persist_reflective_rule_attributions(
            conn,
            result,
            trigger,
            attributions,
        )
        await apply_reflective_rule_credit(conn, trigger, attributions)

    if not _env_bool("INQUIRY_REFLECTIVE_RULE_LEARNING_ENABLED", True):
        return

    candidates = propose_reflective_rule_candidates(result, trigger)
    if not candidates:
        return
    for candidate in candidates[
        : _env_int("INQUIRY_REFLECTIVE_RULE_MAX_PROPOSALS", 2, minimum=1)
    ]:
        replay = replay_reflective_rule_candidate(result, trigger, candidate)
        rule_id = await upsert_reflective_rule_candidate(
            conn,
            result,
            trigger,
            candidate,
            replay,
        )
        await persist_reflective_rule_replay(
            conn,
            result,
            trigger,
            candidate,
            replay,
            rule_id=rule_id,
        )


def propose_reflective_rule_candidates(
    result: InquiryResult,
    trigger: TriggerContext,
) -> list[ReflectiveRuleCandidate]:
    if not result.questions or not result.retrieval_actions:
        return []
    if (
        result.sufficiency.status
        not in {
            "sufficient_for_reasoning",
            "no_update_needed",
            "human_validation_required",
        }
        and not result.evidence_cards
    ):
        return []

    signature = reflective_signature_for(trigger)
    selected_ids = packet_used_evidence_ids(result.context_packet)
    useful_cards = _useful_cards(result.evidence_cards, selected_ids)
    if not useful_cards:
        useful_cards = tuple(card for card in result.evidence_cards if card.score > 0)
    if not useful_cards:
        return []

    cards_by_question = _cards_by_question(useful_cards)
    actions_by_question = _actions_by_question(result.retrieval_actions)
    candidates: list[ReflectiveRuleCandidate] = []
    for question in sorted(
        result.questions,
        key=lambda q: (
            -_question_evidence_weight(cards_by_question.get(q.question_id, ())),
            -q.score,
            q.question_id,
        ),
    ):
        cards = cards_by_question.get(question.question_id, ())
        if not cards:
            continue
        actions = actions_by_question.get(question.question_id, ())
        useful_paths = _useful_paths(cards, actions)
        if not useful_paths:
            continue
        avoid_rules = _avoid_rules(result.questions, cards_by_question, question)
        terms = _semantic_terms(trigger, cards)
        rule_pack = {
            "version": 1,
            "source": "trace_reflection",
            "question_rules": [
                {
                    "prefer_primitive": question.primitive,
                    "when": "current_status_unknown",
                    "question_template": _question_template(question),
                }
            ],
            "avoid_rules": avoid_rules,
            "action_rules": [
                {
                    "primitive": question.primitive,
                    "prefer_paths": useful_paths,
                    "semantic_terms": terms,
                }
            ],
        }
        credit = _question_evidence_weight(cards)
        cost = _action_cost(actions)
        candidates.append(
            ReflectiveRuleCandidate(
                signature=signature,
                rule_pack=rule_pack,
                credit=round(credit, 4),
                cost=round(cost, 4),
                rationale=(
                    f"{question.primitive} produced useful evidence through "
                    f"{', '.join(useful_paths)}"
                ),
            )
        )
    return _dedupe_candidates(candidates)


def replay_reflective_rule_candidate(
    result: InquiryResult,
    trigger: TriggerContext,
    candidate: ReflectiveRuleCandidate,
) -> ReflectiveRuleReplayResult:
    rule = ReflectiveRetrievalRule(
        id=uuid7(),
        signature=candidate.signature,
        rule_pack=candidate.rule_pack,
        utility_score=max(0.0, candidate.credit - candidate.cost),
        success_count=1,
        match_score=1.0,
    )
    questions = list(result.questions)
    baseline_score, baseline_diag = _plan_score(
        questions,
        _actions_by_question(result.retrieval_actions),
        result,
    )
    shaped_questions = apply_reflective_rules_to_questions(
        questions,
        trigger,
        unknowns=_replay_unknowns(result, candidate),
        rules=(rule,),
        score_boost=0.18,
    )
    shaped_actions: dict[str, tuple[RetrievalAction, ...]] = {}
    original_actions = _actions_by_question(result.retrieval_actions)
    for question in shaped_questions:
        shaped_actions[question.question_id] = tuple(
            apply_reflective_rules_to_actions(
                question,
                list(original_actions.get(question.question_id, ())),
                rules=(rule,),
            )
        )
    candidate_score, candidate_diag = _plan_score(
        shaped_questions,
        shaped_actions,
        result,
    )
    delta = round(candidate_score - baseline_score, 4)
    min_delta = _env_float(
        "INQUIRY_REFLECTIVE_RULE_PROMOTION_MIN_DELTA",
        0.08,
        minimum=-10.0,
    )
    decision = "promoted" if delta >= min_delta else "rejected"
    return ReflectiveRuleReplayResult(
        baseline_score=round(baseline_score, 4),
        candidate_score=round(candidate_score, 4),
        utility_delta=delta,
        decision=decision,
        diagnostics={
            "baseline": baseline_diag,
            "candidate": candidate_diag,
            "candidate_rationale": candidate.rationale,
            "promotion_min_delta": min_delta,
        },
    )


def reflective_rule_attributions_from_result(
    result: InquiryResult,
) -> list[ReflectiveRuleAttribution]:
    selected_ids = packet_used_evidence_ids(result.context_packet)
    question_by_id = {q.question_id: q for q in result.questions}
    cards_by_question = _cards_by_question(result.evidence_cards)
    attributions: list[ReflectiveRuleAttribution] = []
    for action in result.retrieval_actions:
        rule_ids = _action_rule_ids(action)
        if not rule_ids:
            continue
        question = question_by_id.get(action.question_id)
        if question is None:
            continue
        cards = tuple(
            card
            for card in cards_by_question.get(action.question_id, ())
            if action.path in card.retrieval_paths
        )
        selected = tuple(
            card for card in cards if str(card.evidence_id) in selected_ids
        )
        if not selected and not selected_ids:
            selected = tuple(card for card in cards if card.score >= 0.65)
        credit = _question_evidence_weight(selected)
        cost = 0.08 + max(0, int(action.budget)) / 250.0
        if not cards:
            cost += 0.35
        elif len(cards) > max(3, len(selected) * 4):
            cost += min(1.2, 0.08 * (len(cards) - len(selected)))
        outcome_score = credit - cost
        evidence_refs = tuple(_evidence_ref(card) for card in cards[:16])
        for rule_id in rule_ids:
            attributions.append(
                ReflectiveRuleAttribution(
                    rule_id=rule_id,
                    question_id=action.question_id,
                    question_primitive=question.primitive,
                    action_path=action.path,
                    action_target=action.target,
                    action_payload=jsonable_action(action),
                    evidence_refs=evidence_refs,
                    evidence_count=len(cards),
                    selected_evidence_count=len(selected),
                    credit=round(credit, 4),
                    cost=round(cost, 4),
                    outcome_score=round(outcome_score, 4),
                )
            )
    return attributions


async def persist_reflective_rule_attributions(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
    attributions: list[ReflectiveRuleAttribution],
) -> None:
    if not attributions or await _table_missing(conn, "reflective_rule_attributions"):
        return
    await conn.executemany(
        """
        INSERT INTO reflective_rule_attributions (
          id, tenant_id, inquiry_session_id, rule_id,
          effect_type, question_id, question_primitive,
          action_path, action_target, action_payload,
          evidence_refs, evidence_count, selected_evidence_count,
          credit, cost, outcome_score
        ) VALUES (
          $1, $2, $3, $4,
          'action_plan', $5, $6,
          $7, $8, $9::jsonb,
          $10::jsonb, $11, $12,
          $13, $14, $15
        )
        ON CONFLICT (
          inquiry_session_id, rule_id, effect_type, question_id,
          action_path, action_target
        )
        DO UPDATE SET
          action_payload = EXCLUDED.action_payload,
          evidence_refs = EXCLUDED.evidence_refs,
          evidence_count = EXCLUDED.evidence_count,
          selected_evidence_count = EXCLUDED.selected_evidence_count,
          credit = EXCLUDED.credit,
          cost = EXCLUDED.cost,
          outcome_score = EXCLUDED.outcome_score
        """,
        [
            (
                uuid7(),
                trigger.tenant_id,
                result.session_id,
                attribution.rule_id,
                attribution.question_id,
                attribution.question_primitive,
                attribution.action_path,
                attribution.action_target,
                json.dumps(attribution.action_payload, default=str),
                json.dumps(attribution.evidence_refs, default=str),
                attribution.evidence_count,
                attribution.selected_evidence_count,
                attribution.credit,
                attribution.cost,
                attribution.outcome_score,
            )
            for attribution in attributions
        ],
    )


async def apply_reflective_rule_credit(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    attributions: list[ReflectiveRuleAttribution],
) -> None:
    if not attributions:
        return
    grouped: dict[UUID, dict[str, float]] = {}
    for attribution in attributions:
        bucket = grouped.setdefault(
            attribution.rule_id,
            {"credit": 0.0, "cost": 0.0, "outcome": 0.0},
        )
        bucket["credit"] += attribution.credit
        bucket["cost"] += attribution.cost
        bucket["outcome"] += attribution.outcome_score
    quarantine_failures = _env_int(
        "INQUIRY_REFLECTIVE_RULE_QUARANTINE_FAILURES",
        3,
        minimum=1,
    )
    quarantine_utility = _env_float(
        "INQUIRY_REFLECTIVE_RULE_QUARANTINE_UTILITY",
        -0.05,
        minimum=-10.0,
    )
    for rule_id, values in grouped.items():
        credit = round(values["credit"], 4)
        cost = round(values["cost"], 4)
        success = credit > cost
        await conn.execute(
            """
            UPDATE reflective_retrieval_rules
            SET
              success_count = success_count + CASE WHEN $3 THEN 1 ELSE 0 END,
              failure_count = failure_count + CASE WHEN $3 THEN 0 ELSE 1 END,
              total_credit = total_credit + $4,
              total_cost = total_cost + $5,
              utility_score = (
                (total_credit + $4) - (total_cost + $5)
              ) / GREATEST(success_count + failure_count + 1, 1),
              maturity = CASE
                WHEN maturity = 'quarantined'
                THEN maturity
                WHEN NOT $3
                  AND failure_count + 1 >= $6
                  AND (
                    (total_credit + $4) - (total_cost + $5)
                  ) / GREATEST(success_count + failure_count + 1, 1) <= $7
                THEN 'quarantined'
                ELSE maturity
              END,
              last_success_at = CASE WHEN $3 THEN now() ELSE last_success_at END,
              last_failure_at = CASE WHEN $3 THEN last_failure_at ELSE now() END,
              updated_at = now()
            WHERE tenant_id = $1
              AND id = $2
            """,
            trigger.tenant_id,
            rule_id,
            success,
            credit,
            cost,
            quarantine_failures,
            quarantine_utility,
        )


async def upsert_reflective_rule_candidate(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
    candidate: ReflectiveRuleCandidate,
    replay: ReflectiveRuleReplayResult,
) -> UUID | None:
    del result
    signature_hash = stable_hash(candidate.signature)
    rule_pack_hash = stable_hash(candidate.rule_pack)
    maturity = "active" if replay.decision == "promoted" else "candidate"
    utility = max(0.0, replay.utility_delta)
    rule_id = uuid7()
    row = await conn.fetchrow(
        """
        INSERT INTO reflective_retrieval_rules (
          id, tenant_id, signature, signature_hash,
          rule_pack, rule_pack_hash, maturity, utility_score,
          success_count, failure_count, total_credit, total_cost,
          last_success_at, updated_at
        ) VALUES (
          $1, $2, $3::jsonb, $4,
          $5::jsonb, $6, $7, $8,
          CASE WHEN $7 = 'active' THEN 1 ELSE 0 END,
          CASE WHEN $7 = 'active' THEN 0 ELSE 1 END,
          $9, $10,
          CASE WHEN $7 = 'active' THEN now() ELSE NULL END,
          now()
        )
        ON CONFLICT (tenant_id, signature_hash, rule_pack_hash)
        DO UPDATE SET
          maturity = CASE
            WHEN reflective_retrieval_rules.maturity = 'quarantined'
            THEN reflective_retrieval_rules.maturity
            WHEN EXCLUDED.maturity = 'active'
            THEN 'active'
            ELSE reflective_retrieval_rules.maturity
          END,
          utility_score = GREATEST(
            reflective_retrieval_rules.utility_score,
            EXCLUDED.utility_score
          ),
          success_count = reflective_retrieval_rules.success_count
            + CASE WHEN EXCLUDED.maturity = 'active' THEN 1 ELSE 0 END,
          failure_count = reflective_retrieval_rules.failure_count
            + CASE WHEN EXCLUDED.maturity = 'active' THEN 0 ELSE 1 END,
          total_credit = reflective_retrieval_rules.total_credit
            + EXCLUDED.total_credit,
          total_cost = reflective_retrieval_rules.total_cost
            + EXCLUDED.total_cost,
          last_success_at = CASE
            WHEN EXCLUDED.maturity = 'active' THEN now()
            ELSE reflective_retrieval_rules.last_success_at
          END,
          last_failure_at = CASE
            WHEN EXCLUDED.maturity = 'active'
            THEN reflective_retrieval_rules.last_failure_at
            ELSE now()
          END,
          updated_at = now()
        RETURNING id
        """,
        rule_id,
        trigger.tenant_id,
        json.dumps(candidate.signature, default=str),
        signature_hash,
        json.dumps(candidate.rule_pack, default=str),
        rule_pack_hash,
        maturity,
        utility,
        candidate.credit,
        candidate.cost,
    )
    if row is None:
        return None
    return row["id"]


async def persist_reflective_rule_replay(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
    candidate: ReflectiveRuleCandidate,
    replay: ReflectiveRuleReplayResult,
    *,
    rule_id: UUID | None,
) -> None:
    if await _table_missing(conn, "reflective_rule_replay_runs"):
        return
    await conn.execute(
        """
        INSERT INTO reflective_rule_replay_runs (
          id, tenant_id, inquiry_session_id, rule_id,
          signature_hash, rule_pack_hash,
          baseline_score, candidate_score, utility_delta,
          decision, diagnostics
        ) VALUES (
          $1, $2, $3, $4,
          $5, $6,
          $7, $8, $9,
          $10, $11::jsonb
        )
        """,
        uuid7(),
        trigger.tenant_id,
        result.session_id,
        rule_id,
        stable_hash(candidate.signature),
        stable_hash(candidate.rule_pack),
        replay.baseline_score,
        replay.candidate_score,
        replay.utility_delta,
        replay.decision,
        json.dumps(replay.diagnostics, default=str),
    )


def jsonable_action(action: RetrievalAction) -> dict[str, Any]:
    return {
        "question_id": action.question_id,
        "path": action.path,
        "target": action.target,
        "query": action.query,
        "filters": jsonable(action.filters),
        "budget": int(action.budget),
    }


def _plan_score(
    questions: list[InquiryQuestion],
    actions_by_question: dict[str, tuple[RetrievalAction, ...]],
    result: InquiryResult,
) -> tuple[float, dict[str, Any]]:
    selected_ids = packet_used_evidence_ids(result.context_packet)
    useful_cards = _useful_cards(result.evidence_cards, selected_ids)
    if not useful_cards:
        useful_cards = tuple(card for card in result.evidence_cards if card.score > 0)
    cards_by_question = _cards_by_question(useful_cards)
    question_rank = {
        question.question_id: index for index, question in enumerate(questions)
    }
    score = 0.0
    question_hits: dict[str, float] = {}
    path_hits: dict[str, float] = {}
    for question_id, cards in cards_by_question.items():
        rank = question_rank.get(question_id, len(questions))
        weight = _question_evidence_weight(cards)
        q_score = weight / (rank + 1)
        score += q_score
        question_hits[question_id] = round(q_score, 4)
        actions = actions_by_question.get(question_id, ())
        path_rank = {action.path: index for index, action in enumerate(actions)}
        for card in cards:
            best_rank = min(
                (
                    path_rank.get(path, len(actions) + 1)
                    for path in card.retrieval_paths
                ),
                default=len(actions) + 1,
            )
            p_score = max(0.0, float(card.score)) / (best_rank + 1)
            score += p_score
            path_hits[f"{question_id}:{card.evidence_id}"] = round(p_score, 4)
    if result.sufficiency.status == "sufficient_for_reasoning":
        score += 0.2
    action_cost = sum(
        max(0, int(action.budget)) / 1200.0
        for actions in actions_by_question.values()
        for action in actions
    )
    score -= action_cost
    return score, {
        "question_hits": question_hits,
        "path_hits": path_hits,
        "action_cost": round(action_cost, 4),
    }


def _actions_by_question(
    actions: tuple[RetrievalAction, ...],
) -> dict[str, tuple[RetrievalAction, ...]]:
    out: dict[str, list[RetrievalAction]] = {}
    for action in actions:
        out.setdefault(action.question_id, []).append(action)
    return {question_id: tuple(values) for question_id, values in out.items()}


def _cards_by_question(
    cards: tuple[EvidenceCard, ...],
) -> dict[str, tuple[EvidenceCard, ...]]:
    out: dict[str, list[EvidenceCard]] = {}
    for card in cards:
        for question_id in card.retrieved_for_questions:
            out.setdefault(str(question_id), []).append(card)
    return {question_id: tuple(values) for question_id, values in out.items()}


def _useful_cards(
    cards: tuple[EvidenceCard, ...],
    selected_ids: set[str],
) -> tuple[EvidenceCard, ...]:
    if selected_ids:
        selected = tuple(
            card for card in cards if str(card.evidence_id) in selected_ids
        )
        if selected:
            return selected
    return tuple(
        card
        for card in cards
        if card.supports_hypotheses
        or card.weakens_hypotheses
        or card.contradicts_hypotheses
    )


def _question_evidence_weight(cards: tuple[EvidenceCard, ...]) -> float:
    total = 0.0
    for card in cards:
        relation_bonus = 0.0
        if card.supports_hypotheses:
            relation_bonus += 0.3
        if card.weakens_hypotheses or card.contradicts_hypotheses:
            relation_bonus += 0.35
        total += max(0.1, float(card.score)) + relation_bonus
    return total


def _useful_paths(
    cards: tuple[EvidenceCard, ...],
    actions: tuple[RetrievalAction, ...],
) -> list[str]:
    action_paths = [action.path for action in actions]
    path_scores: dict[str, float] = {}
    for card in cards:
        for path in card.retrieval_paths:
            if path not in action_paths:
                continue
            path_scores[path] = path_scores.get(path, 0.0) + max(0.1, card.score)
    if not path_scores:
        return []
    return [
        path
        for path, _score in sorted(
            path_scores.items(),
            key=lambda item: (-item[1], action_paths.index(item[0]), item[0]),
        )[:4]
    ]


def _avoid_rules(
    questions: tuple[InquiryQuestion, ...],
    cards_by_question: dict[str, tuple[EvidenceCard, ...]],
    winner: InquiryQuestion,
) -> list[dict[str, Any]]:
    avoid: list[dict[str, Any]] = []
    for question in questions:
        if question.question_id == winner.question_id:
            continue
        if cards_by_question.get(question.question_id):
            continue
        if question.score < winner.score - 0.25:
            continue
        avoid.append(
            {
                "primitive": question.primitive,
                "when": "current_status_unknown",
            }
        )
    return avoid[:3]


def _semantic_terms(
    trigger: TriggerContext,
    cards: tuple[EvidenceCard, ...],
) -> list[str]:
    source = " ".join(
        [
            str(trigger.seed_natural_text or ""),
            *(card.summary for card in cards[:8]),
        ]
    )
    return sorted(material_tokens(source))[:8]


def _question_template(question: InquiryQuestion) -> str:
    primitive = question.primitive.upper()
    if primitive == "COUNTEREVIDENCE":
        return "What fresh evidence shows whether {subject} is still blocked or already resolved?"
    if primitive == "CONSTRAINT":
        return "Which active constraint is most likely to change {subject}?"
    if primitive == "DEPENDENCY":
        return "Which dependency now controls the outcome for {subject}?"
    if primitive == "OWNERSHIP":
        return "Who currently owns the next action for {subject}?"
    return question.question


def _replay_unknowns(
    result: InquiryResult,
    candidate: ReflectiveRuleCandidate,
) -> set[str]:
    unknowns = {str(value) for value in result.sufficiency.remaining_unknowns if value}
    if unknowns:
        return unknowns
    for raw in candidate.rule_pack.get("question_rules", []) or []:
        if not isinstance(raw, dict):
            continue
        primitive = str(
            raw.get("prefer_primitive") or raw.get("primitive") or ""
        ).upper()
        if primitive == "COUNTEREVIDENCE":
            unknowns.update({"current status", "counterevidence"})
        elif primitive in {"CONSTRAINT", "DEPENDENCY"}:
            unknowns.update({"critical path", "binding constraint"})
        elif primitive == "OWNERSHIP":
            unknowns.add("owner")
    return unknowns or {"current status"}


def _action_cost(actions: tuple[RetrievalAction, ...]) -> float:
    return sum(0.08 + max(0, int(action.budget)) / 500.0 for action in actions)


def _action_rule_ids(action: RetrievalAction) -> tuple[UUID, ...]:
    raw = action.filters.get("_reflective_rule_ids")
    if isinstance(raw, str):
        raw_values = [raw]
    elif isinstance(raw, list):
        raw_values = raw
    else:
        return ()
    out: list[UUID] = []
    for value in raw_values:
        rule_id = safe_uuid(value)
        if rule_id is not None:
            out.append(rule_id)
    return tuple(out)


def _evidence_ref(card: EvidenceCard) -> dict[str, Any]:
    return {
        "evidence_id": str(card.evidence_id),
        "source_type": card.source_type,
        "source_ref": card.source_ref,
        "source_ref_id": str(card.source_ref_id) if card.source_ref_id else None,
        "retrieval_paths": sorted(card.retrieval_paths),
        "score": round(float(card.score), 4),
    }


def _dedupe_candidates(
    candidates: list[ReflectiveRuleCandidate],
) -> list[ReflectiveRuleCandidate]:
    seen: set[tuple[str, str]] = set()
    out: list[ReflectiveRuleCandidate] = []
    for candidate in sorted(
        candidates,
        key=lambda item: (-(item.credit - item.cost), -item.credit, item.rationale),
    ):
        key = (
            stable_hash(candidate.signature),
            stable_hash(candidate.rule_pack),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


async def _table_missing(conn: asyncpg.Connection, table_name: str) -> bool:
    found = await conn.fetchval(f"SELECT to_regclass('public.{table_name}')")
    return found is None


__all__ = [
    "ReflectiveRuleAttribution",
    "ReflectiveRuleCandidate",
    "ReflectiveRuleReplayResult",
    "apply_reflective_rule_credit",
    "learn_reflective_rules",
    "persist_reflective_rule_attributions",
    "persist_reflective_rule_replay",
    "propose_reflective_rule_candidates",
    "reflective_rule_attributions_from_result",
    "replay_reflective_rule_candidate",
    "upsert_reflective_rule_candidate",
]
