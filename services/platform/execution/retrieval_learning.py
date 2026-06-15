"""Database-backed learned retrieval policy and motif feedback."""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext

from .config import InquiryConfig, _env_bool, _env_float, _env_int
from .evidence_utils import stable_hash
from .motif_utils import (
    action_motif_uuid,
    json_obj,
    motif_plan_from_actions,
    motif_signature_for,
    motif_signature_match_score,
    packet_used_evidence_ids,
    safe_int,
    safe_uuid,
)
from .types import (
    EvidenceCard,
    InquiryQuestion,
    InquiryResult,
    LearnedRetrievalMotif,
    QuestionPolicySignal,
    RetrievalAction,
)


@dataclass(frozen=True, slots=True)
class RetrievalMotifPenalty:
    motif_id: UUID
    question_id: str
    cost: float
    reasons: tuple[str, ...]
    selected_evidence: int = 0
    omitted_evidence: int = 0
    returned_models: int = 0
    returned_observations: int = 0


async def load_question_policy_stats(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signal_type: str,
) -> dict[str, QuestionPolicySignal]:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_question_policy_stats')"
    )
    if table_name is None:
        return {}
    rows = await conn.fetch(
        """
        SELECT signal_type, question_primitive, attempts, successes,
               utility_score, total_credit, total_cost
        FROM sage_question_policy_stats
        WHERE tenant_id = $1
          AND signal_type = $2
        """,
        tenant_id,
        signal_type,
    )
    out: dict[str, QuestionPolicySignal] = {}
    for row in rows:
        primitive = str(row["question_primitive"] or "").upper()
        if not primitive:
            continue
        out[primitive] = QuestionPolicySignal(
            signal_type=str(row["signal_type"] or signal_type),
            question_primitive=primitive,
            attempts=int(row["attempts"] or 0),
            successes=int(row["successes"] or 0),
            utility_score=float(row["utility_score"] or 0.0),
            total_credit=float(row["total_credit"] or 0.0),
            total_cost=float(row["total_cost"] or 0.0),
        )
    return out


async def load_retrieval_motifs_for_questions(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    questions: list[InquiryQuestion],
    cfg: InquiryConfig,
) -> dict[str, LearnedRetrievalMotif]:
    if not cfg.retrieval_motifs_enabled or not questions:
        return {}
    table_name = await conn.fetchval("SELECT to_regclass('public.retrieval_motifs')")
    if table_name is None:
        return {}
    primitives = sorted({q.primitive for q in questions})
    rows = await conn.fetch(
        """
        SELECT id, signature, question_primitive, plan,
               utility_score, success_count
        FROM retrieval_motifs
        WHERE tenant_id = $1
          AND question_primitive = ANY($2::text[])
          AND maturity = 'active'
          AND utility_score > 0
          AND success_count >= $3
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY utility_score DESC, success_count DESC, updated_at DESC
        LIMIT 64
        """,
        trigger.tenant_id,
        primitives,
        int(cfg.retrieval_motif_min_successes),
    )
    if not rows:
        return {}

    by_primitive: dict[str, LearnedRetrievalMotif] = {}
    current_by_primitive = {
        primitive: motif_signature_for(trigger, primitive) for primitive in primitives
    }
    for row in rows:
        primitive = str(row["question_primitive"] or "").upper()
        current = current_by_primitive.get(primitive)
        if not current:
            continue
        signature = json_obj(row["signature"])
        score = motif_signature_match_score(signature, current)
        if score < float(cfg.retrieval_motif_match_threshold):
            continue
        motif = LearnedRetrievalMotif(
            id=row["id"],
            signature=signature,
            question_primitive=primitive,
            plan=json_obj(row["plan"]),
            utility_score=float(row["utility_score"] or 0.0),
            success_count=int(row["success_count"] or 0),
            match_score=score,
        )
        prior = by_primitive.get(primitive)
        if prior is None or (
            motif.match_score,
            motif.utility_score,
            motif.success_count,
        ) > (
            prior.match_score,
            prior.utility_score,
            prior.success_count,
        ):
            by_primitive[primitive] = motif

    return {
        question.question_id: by_primitive[question.primitive]
        for question in questions
        if question.primitive in by_primitive
    }


def is_low_value_model_noise(card: EvidenceCard) -> bool:
    return (
        card.source_type == "model"
        and not card.supports_hypotheses
        and not card.weakens_hypotheses
        and not card.contradicts_hypotheses
    )


async def learn_retrieval_motifs(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    if not _env_bool("INQUIRY_RETRIEVAL_MOTIFS_LEARNING_ENABLED", True):
        return
    table_name = await conn.fetchval("SELECT to_regclass('public.retrieval_motifs')")
    if table_name is None:
        return
    actions_by_question: dict[str, list[RetrievalAction]] = {}
    for action in result.retrieval_actions:
        if action.path == "sage_reader":
            continue
        actions_by_question.setdefault(action.question_id, []).append(action)

    for question in result.questions:
        cards = [
            card
            for card in result.evidence_cards
            if question.question_id in card.retrieved_for_questions
        ]
        if not cards:
            continue
        raw_actions = actions_by_question.get(question.question_id, [])
        if not raw_actions:
            continue
        used_paths = {
            path
            for card in cards
            for path in card.retrieval_paths
            if path != "sage_reader"
        }
        useful_actions = [
            action
            for action in raw_actions
            if not used_paths or action.path in used_paths
        ]
        if len(useful_actions) < 2:
            continue
        plan = motif_plan_from_actions(useful_actions)
        if not plan.get("actions"):
            continue
        signature = motif_signature_for(trigger, question.primitive)
        signature_hash = stable_hash(signature)
        plan_hash = stable_hash(plan)
        credit = float(len(cards))
        cost = (
            0.08 * len(plan["actions"])
            + sum(float(action.get("budget") or 0) for action in plan["actions"])
            / 500.0
        )
        utility = credit - cost
        if utility <= 0:
            continue
        await conn.execute(
            """
            INSERT INTO retrieval_motifs (
              id, tenant_id, signature, signature_hash,
              question_primitive, plan, plan_hash,
              maturity, utility_score, success_count,
              total_credit, total_cost, last_success_at, updated_at
            ) VALUES (
              $1, $2, $3::jsonb, $4,
              $5, $6::jsonb, $7,
              'active', $8, 1,
              $9, $10, now(), now()
            )
            ON CONFLICT (
              tenant_id, question_primitive, signature_hash, plan_hash
            )
            DO UPDATE SET
              success_count = retrieval_motifs.success_count + 1,
              total_credit = retrieval_motifs.total_credit + EXCLUDED.total_credit,
              total_cost = retrieval_motifs.total_cost + EXCLUDED.total_cost,
              utility_score = (
                (retrieval_motifs.total_credit + EXCLUDED.total_credit)
                - (retrieval_motifs.total_cost + EXCLUDED.total_cost)
              ) / GREATEST(
                retrieval_motifs.success_count
                + retrieval_motifs.failure_count
                + 1,
                1
              ),
              maturity = CASE
                WHEN retrieval_motifs.maturity = 'quarantined'
                THEN retrieval_motifs.maturity
                ELSE 'active'
              END,
              last_success_at = now(),
              updated_at = now()
            """,
            uuid7(),
            trigger.tenant_id,
            json.dumps(signature, default=str),
            signature_hash,
            question.primitive,
            json.dumps(plan, default=str),
            plan_hash,
            utility,
            credit,
            cost,
        )


async def penalize_retrieval_motifs(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    if not _env_bool("INQUIRY_RETRIEVAL_MOTIF_FAILURE_LEARNING_ENABLED", True):
        return
    penalties = motif_failure_penalties(result)
    if not penalties:
        return
    table_name = await conn.fetchval("SELECT to_regclass('public.retrieval_motifs')")
    if table_name is None:
        return
    quarantine_failures = _env_int(
        "INQUIRY_RETRIEVAL_MOTIF_QUARANTINE_FAILURES",
        3,
        minimum=1,
    )
    quarantine_utility = _env_float(
        "INQUIRY_RETRIEVAL_MOTIF_QUARANTINE_UTILITY",
        0.0,
        minimum=-10.0,
    )
    for penalty in penalties:
        await conn.execute(
            """
            UPDATE retrieval_motifs
            SET
              failure_count = failure_count + 1,
              total_cost = total_cost + $3,
              utility_score = (
                total_credit - (total_cost + $3)
              ) / GREATEST(success_count + failure_count + 1, 1),
              maturity = CASE
                WHEN maturity = 'quarantined'
                THEN maturity
                WHEN failure_count + 1 >= $4
                  AND (
                    total_credit - (total_cost + $3)
                  ) / GREATEST(success_count + failure_count + 1, 1) <= $5
                THEN 'quarantined'
                ELSE maturity
              END,
              last_failure_at = now(),
              updated_at = now()
            WHERE tenant_id = $1
              AND id = $2
            """,
            trigger.tenant_id,
            penalty.motif_id,
            float(penalty.cost),
            quarantine_failures,
            quarantine_utility,
        )


def motif_failure_penalties(result: InquiryResult) -> list[RetrievalMotifPenalty]:
    motif_actions: dict[tuple[str, UUID], list[RetrievalAction]] = {}
    for action in result.retrieval_actions:
        motif_id = action_motif_uuid(action)
        if motif_id is None:
            continue
        motif_actions.setdefault((action.question_id, motif_id), []).append(action)
    if not motif_actions:
        return []

    used_ids = packet_used_evidence_ids(result.context_packet)
    timings = [
        note
        for note in (result.notes or {}).get("retrieval_action_timings", [])
        if isinstance(note, dict)
    ]
    output_by_motif: dict[tuple[str, UUID], dict[str, int]] = {}
    for note in timings:
        motif_id = safe_uuid(note.get("motif_id"))
        question_id = str(note.get("question_id") or "")
        if motif_id is None or not question_id:
            continue
        bucket = output_by_motif.setdefault(
            (question_id, motif_id),
            {"models": 0, "observations": 0},
        )
        bucket["models"] += safe_int(note.get("models"))
        bucket["observations"] += safe_int(note.get("observations"))

    penalties: list[RetrievalMotifPenalty] = []
    for (question_id, motif_id), actions in motif_actions.items():
        paths = {action.path for action in actions}
        cards = [
            card
            for card in result.evidence_cards
            if question_id in card.retrieved_for_questions
            and bool(card.retrieval_paths & paths)
        ]
        selected = [card for card in cards if str(card.evidence_id) in used_ids]
        omitted = [card for card in cards if str(card.evidence_id) not in used_ids]
        low_value_omitted = [card for card in omitted if is_low_value_model_noise(card)]
        outputs = output_by_motif.get((question_id, motif_id), {})
        returned_models = int(outputs.get("models") or 0)
        returned_observations = int(outputs.get("observations") or 0)
        selected_count = len(selected)
        omitted_count = len(omitted)

        reasons: list[str] = []
        if selected_count == 0 and (
            cards or returned_models >= 20 or returned_observations >= 8
        ):
            reasons.append("no_packet_evidence")
        if omitted_count >= _env_int(
            "INQUIRY_RETRIEVAL_MOTIF_NOISY_OMISSION_MIN",
            6,
            minimum=1,
        ) and omitted_count >= max(3, selected_count * 3):
            reasons.append("noisy_omission_ratio")
        if (
            returned_models
            >= _env_int(
                "INQUIRY_RETRIEVAL_MOTIF_WIDE_MODEL_THRESHOLD",
                80,
                minimum=1,
            )
            and selected_count <= 2
        ):
            reasons.append("wide_motif_selection")
        if len(low_value_omitted) >= 4:
            reasons.append("low_value_model_noise")
        if not reasons:
            continue

        raw_cost = 0.0
        if "no_packet_evidence" in reasons:
            raw_cost += 1.2
        if "noisy_omission_ratio" in reasons:
            raw_cost += min(3.0, 0.25 * omitted_count)
        if "wide_motif_selection" in reasons:
            raw_cost += min(2.5, returned_models / 80.0)
        if "low_value_model_noise" in reasons:
            raw_cost += min(2.0, 0.35 * len(low_value_omitted))
        benefit_discount = min(0.8, 0.12 * selected_count)
        cost = max(0.15, raw_cost - benefit_discount)
        penalties.append(
            RetrievalMotifPenalty(
                motif_id=motif_id,
                question_id=question_id,
                cost=round(min(6.0, cost), 4),
                reasons=tuple(sorted(set(reasons))),
                selected_evidence=selected_count,
                omitted_evidence=omitted_count,
                returned_models=returned_models,
                returned_observations=returned_observations,
            )
        )
    return penalties


__all__ = [
    "RetrievalMotifPenalty",
    "is_low_value_model_noise",
    "learn_retrieval_motifs",
    "load_question_policy_stats",
    "load_retrieval_motifs_for_questions",
    "motif_failure_penalties",
    "penalize_retrieval_motifs",
]
