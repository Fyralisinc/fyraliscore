"""Database-backed learned retrieval policy and motif feedback."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.retrieval_policy import (
    SageRouteOutcome,
    SageRouteUtility,
    build_signal_signature,
    route_utilities_from_outcomes,
)

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


def profile_prior_outcomes_from_result(result: InquiryResult) -> list[dict[str, Any]]:
    """Summarize whether SAGE profile-shaped actions predicted useful context.

    These rows are diagnostic policy telemetry, not truth. They are designed to
    be stored in inquiry notes so SAGE can later see whether a profile prior
    actually led to selected context or aligned with downstream outcome reward.
    """

    sage_notes = (result.notes or {}).get("sage_reader")
    if not isinstance(sage_notes, dict):
        return []
    policy_actions = sage_notes.get("retrieval_policy_actions")
    if not isinstance(policy_actions, dict):
        return []

    timing_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for note in (result.notes or {}).get("retrieval_action_timings", []) or []:
        if not isinstance(note, dict):
            continue
        question_id = str(note.get("question_id") or "")
        path = str(note.get("path") or "")
        target = str(note.get("target") or "")
        if question_id and path:
            timing_by_key[(question_id, path, target)] = note

    used_ids = packet_used_evidence_ids(getattr(result, "context_packet", {}) or {})
    evidence_by_question_path: dict[tuple[str, str], dict[str, int]] = {}
    for card in getattr(result, "evidence_cards", ()) or ():
        for question_id in card.retrieved_for_questions:
            for path in card.retrieval_paths:
                bucket = evidence_by_question_path.setdefault(
                    (question_id, path),
                    {"evidence": 0, "selected": 0},
                )
                bucket["evidence"] += 1
                if str(card.evidence_id) in used_ids:
                    bucket["selected"] += 1

    outcome_reward = _outcome_reward_from_result_notes(result)
    out: list[dict[str, Any]] = []
    for raw_question_id, raw_actions in policy_actions.items():
        question_id = str(raw_question_id)
        if not isinstance(raw_actions, list):
            continue
        for action_note in raw_actions:
            if not isinstance(action_note, dict):
                continue
            effect = action_note.get("company_profile")
            if not isinstance(effect, dict):
                continue
            path = str(action_note.get("path") or "")
            target = str(action_note.get("target") or "")
            timing = timing_by_key.get((question_id, path, target), {})
            evidence_bucket = evidence_by_question_path.get(
                (question_id, path),
                {"evidence": 0, "selected": 0},
            )
            skipped = (
                str(action_note.get("mode") or "") == "skip"
                or bool(timing.get("skipped"))
            )
            selected = int(evidence_bucket.get("selected") or 0)
            evidence = int(evidence_bucket.get("evidence") or 0)
            models = safe_int(timing.get("models"))
            observations = safe_int(timing.get("observations"))
            returned = bool(timing.get("returned")) or models > 0 or observations > 0
            score = _safe_float(effect.get("score"))
            out.append(
                {
                    "question_id": question_id,
                    "path": path,
                    "target": target,
                    "prior_kind": str(effect.get("kind") or ""),
                    "prior_key": str(effect.get("key") or ""),
                    "prior_score": round(score, 4),
                    "prior_confidence": round(_safe_float(effect.get("confidence")), 4),
                    "salience_only": bool(effect.get("salience_only")),
                    "authority_effect": effect.get("authority_effect", "none"),
                    "mode": str(action_note.get("mode") or ""),
                    "skipped": skipped,
                    "returned": returned,
                    "returned_models": models,
                    "returned_observations": observations,
                    "evidence_count": evidence,
                    "selected_evidence": selected,
                    "useful_context": selected > 0,
                    "outcome_reward": outcome_reward,
                    "prior_prediction_result": _profile_prior_prediction_result(
                        score=score,
                        skipped=skipped,
                        returned=returned,
                        selected_evidence=selected,
                    ),
                    "canonical_write": False,
                }
            )
    return out


async def load_sage_route_utilities(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    *,
    question_primitives: list[str] | tuple[str, ...] | None = None,
    limit: int = 128,
) -> tuple[SageRouteUtility, ...]:
    """Load compact SAGE route utility hints for this trigger family."""

    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_retrieval_route_utilities')"
    )
    if table_name is None:
        return ()
    primitives = sorted(
        {
            str(primitive or "").upper()
            for primitive in (question_primitives or ())
            if str(primitive or "").strip()
        }
    )
    if primitives:
        rows = await conn.fetch(
            """
            SELECT signature_hash, path, signal_type, subkind, question_primitive,
                   attempts, wins, skips, returned_models, returned_observations,
                   selected_evidence, elapsed_ms_total, latency_ms_p95,
                   budget_total, total_cost, total_quality_credit,
                   utility_score, confidence
            FROM sage_retrieval_route_utilities
            WHERE tenant_id = $1
              AND signal_type = $2
              AND (question_primitive IS NULL OR question_primitive = ANY($3::text[]))
              AND attempts >= 1
            ORDER BY confidence DESC, utility_score DESC, updated_at DESC
            LIMIT $4
            """,
            trigger.tenant_id,
            trigger.kind,
            primitives,
            max(1, int(limit)),
        )
    else:
        rows = await conn.fetch(
            """
            SELECT signature_hash, path, signal_type, subkind, question_primitive,
                   attempts, wins, skips, returned_models, returned_observations,
                   selected_evidence, elapsed_ms_total, latency_ms_p95,
                   budget_total, total_cost, total_quality_credit,
                   utility_score, confidence
            FROM sage_retrieval_route_utilities
            WHERE tenant_id = $1
              AND signal_type = $2
              AND attempts >= 1
            ORDER BY confidence DESC, utility_score DESC, updated_at DESC
            LIMIT $3
            """,
            trigger.tenant_id,
            trigger.kind,
            max(1, int(limit)),
        )
    return tuple(_route_utility_from_row(row) for row in rows)


async def learn_sage_route_utilities(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    """Persist SAGE route utility from an inquiry run's retrieval telemetry."""

    if not _env_bool("SAGE_ROUTE_UTILITY_LEARNING_ENABLED", True):
        return
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_retrieval_route_utilities')"
    )
    if table_name is None:
        return
    utilities = _route_utilities_from_inquiry_result(result, trigger)
    if not utilities:
        return
    await conn.executemany(
        """
        INSERT INTO sage_retrieval_route_utilities (
          tenant_id, signal_type, subkind, question_primitive, signature_hash, path,
          attempts, wins, skips, returned_models, returned_observations,
          selected_evidence, elapsed_ms_total, latency_ms_p95, budget_total,
          total_cost, total_quality_credit, utility_score, confidence,
          last_observed_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9, $10, $11,
          $12, $13, $14, $15,
          $16, $17, $18, $19,
          now(), now()
        )
        ON CONFLICT (tenant_id, signature_hash, path) DO UPDATE SET
          attempts = sage_retrieval_route_utilities.attempts + EXCLUDED.attempts,
          wins = sage_retrieval_route_utilities.wins + EXCLUDED.wins,
          skips = sage_retrieval_route_utilities.skips + EXCLUDED.skips,
          returned_models = (
            sage_retrieval_route_utilities.returned_models
            + EXCLUDED.returned_models
          ),
          returned_observations = (
            sage_retrieval_route_utilities.returned_observations
            + EXCLUDED.returned_observations
          ),
          selected_evidence = (
            sage_retrieval_route_utilities.selected_evidence
            + EXCLUDED.selected_evidence
          ),
          elapsed_ms_total = (
            sage_retrieval_route_utilities.elapsed_ms_total
            + EXCLUDED.elapsed_ms_total
          ),
          latency_ms_p95 = GREATEST(
            sage_retrieval_route_utilities.latency_ms_p95,
            EXCLUDED.latency_ms_p95
          ),
          budget_total = (
            sage_retrieval_route_utilities.budget_total + EXCLUDED.budget_total
          ),
          total_cost = sage_retrieval_route_utilities.total_cost + EXCLUDED.total_cost,
          total_quality_credit = (
            sage_retrieval_route_utilities.total_quality_credit
            + EXCLUDED.total_quality_credit
          ),
          utility_score = (
            (
              sage_retrieval_route_utilities.utility_score
              * GREATEST(sage_retrieval_route_utilities.attempts, 1)
            )
            + (EXCLUDED.utility_score * GREATEST(EXCLUDED.attempts, 1))
          ) / GREATEST(
            sage_retrieval_route_utilities.attempts + EXCLUDED.attempts,
            1
          ),
          confidence = LEAST(
            1.0,
            GREATEST(
              sage_retrieval_route_utilities.confidence,
              EXCLUDED.confidence,
              LN(1 + sage_retrieval_route_utilities.attempts + EXCLUDED.attempts)
              / LN(33)
            )
          ),
          last_observed_at = now(),
          updated_at = now()
        """,
        [_route_utility_params(trigger.tenant_id, utility) for utility in utilities],
    )


async def record_profile_prior_residuals(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> int:
    """Persist non-canonical residuals for contradicted SAGE profile priors."""

    outcomes = (result.notes or {}).get("sage_profile_prior_outcomes")
    if not isinstance(outcomes, list):
        outcomes = profile_prior_outcomes_from_result(result)
    contradicted = [
        outcome
        for outcome in outcomes
        if isinstance(outcome, dict)
        and str(outcome.get("prior_prediction_result") or "").startswith("contradicted")
    ]
    if not contradicted:
        return 0
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.model_residual_evidence')"
    )
    if table_name is None:
        return 0
    rows = []
    for outcome in contradicted:
        reason = _profile_prior_residual_reason(outcome)
        rows.append(
            (
                uuid7(),
                trigger.tenant_id,
                trigger.observation_id,
                trigger.model_id,
                "compression_uncertain",
                _profile_prior_residual_summary(outcome),
                reason,
                json.dumps(
                    {
                        "source": "sage_profile_prior_outcome",
                        "canonical_write": False,
                        "profile_prior_outcome": outcome,
                    },
                    default=str,
                ),
            )
        )
    await conn.executemany(
        """
        INSERT INTO model_residual_evidence (
          id, tenant_id, source_observation_id, model_id,
          residual_kind, compact_summary, reason, status, metadata
        ) VALUES (
          $1, $2, $3, $4,
          $5, $6, $7, 'open', $8::jsonb
        )
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    return len(rows)


async def decay_sage_route_utilities(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None = None,
    stale_after_days: int = 14,
    factor: float = 0.97,
) -> int:
    """Apply offline decay to stale SAGE route utility memory."""

    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_retrieval_route_utilities')"
    )
    if table_name is None:
        return 0
    bounded_factor = min(1.0, max(0.0, float(factor)))
    stale_days = max(1, int(stale_after_days))
    if tenant_id is None:
        status = await conn.execute(
            """
            UPDATE sage_retrieval_route_utilities
               SET utility_score = utility_score * $1,
                   confidence = GREATEST(0.0, confidence * $1),
                   updated_at = now()
             WHERE last_observed_at < now() - ($2::text || ' days')::interval
            """,
            bounded_factor,
            stale_days,
        )
    else:
        status = await conn.execute(
            """
            UPDATE sage_retrieval_route_utilities
               SET utility_score = utility_score * $1,
                   confidence = GREATEST(0.0, confidence * $1),
                   updated_at = now()
             WHERE tenant_id = $2
               AND last_observed_at < now() - ($3::text || ' days')::interval
            """,
            bounded_factor,
            tenant_id,
            stale_days,
        )
    return _execute_row_count(status)


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


def _route_utility_from_row(row: Any) -> SageRouteUtility:
    return SageRouteUtility(
        signature_hash=str(row["signature_hash"]),
        path=str(row["path"]),
        signal_type=str(row["signal_type"] or ""),
        subkind=row["subkind"],
        question_primitive=row["question_primitive"],
        attempts=int(row["attempts"] or 0),
        wins=int(row["wins"] or 0),
        skips=int(row["skips"] or 0),
        returned_models=int(row["returned_models"] or 0),
        returned_observations=int(row["returned_observations"] or 0),
        selected_evidence=int(row["selected_evidence"] or 0),
        elapsed_ms_total=int(row["elapsed_ms_total"] or 0),
        latency_ms_p95=float(row["latency_ms_p95"] or 0.0),
        budget_total=int(row["budget_total"] or 0),
        total_cost=float(row["total_cost"] or 0.0),
        total_quality_credit=float(row["total_quality_credit"] or 0.0),
        utility_score=float(row["utility_score"] or 0.0),
        confidence=float(row["confidence"] or 0.0),
    )


def _route_utility_params(
    tenant_id: UUID,
    utility: SageRouteUtility,
) -> tuple[Any, ...]:
    return (
        tenant_id,
        utility.signal_type,
        utility.subkind,
        utility.question_primitive,
        utility.signature_hash,
        utility.path,
        int(utility.attempts),
        int(utility.wins),
        int(utility.skips),
        int(utility.returned_models),
        int(utility.returned_observations),
        int(utility.selected_evidence),
        int(utility.elapsed_ms_total),
        float(utility.latency_ms_p95),
        int(utility.budget_total),
        float(utility.total_cost),
        float(utility.total_quality_credit),
        float(utility.utility_score),
        float(utility.confidence),
    )


def _route_utilities_from_inquiry_result(
    result: InquiryResult,
    trigger: TriggerContext,
) -> tuple[SageRouteUtility, ...]:
    by_signature: dict[tuple[str | None, str], list[SageRouteOutcome]] = {}
    outcome_reward = _outcome_reward_from_result_notes(result)
    question_by_id = {question.question_id: question for question in result.questions}
    action_by_key: dict[tuple[str, str, str], RetrievalAction] = {}
    for action in result.retrieval_actions:
        action_by_key.setdefault(
            (action.question_id, action.path, action.target),
            action,
        )
    selected_by_question_path: dict[tuple[str, str], int] = {}
    for card in result.evidence_cards:
        for question_id in card.retrieved_for_questions:
            for path in card.retrieval_paths:
                selected_by_question_path[(question_id, path)] = (
                    selected_by_question_path.get((question_id, path), 0) + 1
                )
    for note in (result.notes or {}).get("retrieval_action_timings", []) or []:
        if not isinstance(note, dict):
            continue
        path = str(note.get("path") or "")
        question_id = str(note.get("question_id") or "")
        if not path or path == "sage_reader":
            continue
        question = question_by_id.get(question_id)
        if question is None:
            continue
        target = str(note.get("target") or "")
        action = action_by_key.get((question_id, path, target))
        budget = int(getattr(action, "budget", 0) or 0) if action else 0
        selected = selected_by_question_path.get((question_id, path), 0)
        returned = bool(note.get("returned"))
        elapsed_ms = _safe_int(note.get("elapsed_ms"))
        models = _safe_int(note.get("models"))
        observations = _safe_int(note.get("observations"))
        signature = build_signal_signature(
            trigger=trigger,
            question_primitive=question.primitive,
            projection_enabled=False,
        )
        by_signature.setdefault(
            (signature.question_primitive, signature.signal_type),
            [],
        ).append(
            SageRouteOutcome(
                path=path,
                admitted=not bool(note.get("skipped")),
                skipped=bool(note.get("skipped")),
                elapsed_ms=elapsed_ms,
                returned_models=models,
                returned_observations=observations,
                selected_evidence=selected,
                budget=budget,
                quality_credit=_route_quality_credit(
                    selected=selected,
                    returned=returned,
                    models=models,
                    observations=observations,
                    outcome_reward=outcome_reward,
                ),
                cost_units=_route_cost_units(
                    path,
                    elapsed_ms=elapsed_ms,
                    budget=budget,
                ),
            )
        )
    primary_signature = build_signal_signature(
        trigger=trigger,
        question_primitive=None,
        projection_enabled=True,
    )
    primary_outcomes = _primary_route_outcomes_from_result_notes(
        result,
        outcome_reward=outcome_reward,
    )
    if primary_outcomes:
        by_signature.setdefault(
            (primary_signature.question_primitive, primary_signature.signal_type),
            [],
        ).extend(primary_outcomes)
    utilities: list[SageRouteUtility] = []
    for (primitive, _signal_type), outcomes in by_signature.items():
        signature = build_signal_signature(
            trigger=trigger,
            question_primitive=primitive,
            projection_enabled=primitive is None,
        )
        utilities.extend(route_utilities_from_outcomes(signature, outcomes))
    return tuple(utilities)


def _primary_route_outcomes_from_result_notes(
    result: InquiryResult,
    *,
    outcome_reward: float | None = None,
) -> tuple[SageRouteOutcome, ...]:
    outcomes: list[SageRouteOutcome] = []
    for stage_note in (result.notes or {}).get("retrieval_stage_timings", []) or []:
        if not isinstance(stage_note, dict) or stage_note.get("stage") != "primary_retrieve":
            continue
        for item in stage_note.get("primary_pathway_timings", []) or []:
            if not isinstance(item, dict):
                continue
            path = _primary_path_from_stage(str(item.get("stage") or ""))
            if not path:
                continue
            elapsed_ms = _safe_int(item.get("elapsed_ms"))
            models = _safe_int(item.get("models"))
            observations = _safe_int(item.get("observations"))
            skipped = bool(item.get("skipped"))
            outcomes.append(
                SageRouteOutcome(
                    path=path,
                    admitted=not skipped,
                    skipped=skipped,
                    elapsed_ms=elapsed_ms,
                    returned_models=models,
                    returned_observations=observations,
                    selected_evidence=0,
                    budget=0,
                    quality_credit=_route_quality_credit(
                        selected=0,
                        returned=not skipped and (models > 0 or observations > 0),
                        models=models,
                        observations=observations,
                        outcome_reward=outcome_reward,
                    ),
                    cost_units=_route_cost_units(path, elapsed_ms=elapsed_ms, budget=0),
                )
            )
    return tuple(outcomes)


def _primary_path_from_stage(stage: str) -> str | None:
    if stage == "projection_context":
        return "projection_context"
    if not stage.startswith("pathway_"):
        return None
    path = stage.removeprefix("pathway_")
    return path if path in {"A", "B", "L", "C", "D", "G"} else None


def _route_quality_credit(
    *,
    selected: int,
    returned: bool,
    models: int,
    observations: int,
    outcome_reward: float | None = None,
) -> float:
    base: float
    if selected > 0:
        base = min(4.0, 0.85 * selected)
    elif returned and (models > 0 or observations > 0):
        base = min(0.35, (models + observations) / 120.0)
    else:
        base = -0.15
    return _shape_route_credit_by_outcome(base, outcome_reward)


def _shape_route_credit_by_outcome(
    base_credit: float,
    outcome_reward: float | None,
) -> float:
    if outcome_reward is None:
        return base_credit
    reward = min(1.0, max(0.0, float(outcome_reward)))
    if base_credit > 0:
        return (base_credit * (0.25 + 1.5 * reward)) - (0.45 * (1.0 - reward))
    return base_credit - (0.25 * (1.0 - reward))


def _outcome_reward_from_result_notes(result: InquiryResult) -> float | None:
    notes = result.notes or {}
    candidates = [
        notes.get("outcome_reward_features"),
        notes.get("reward_features"),
        notes.get("outcome_features"),
    ]
    outcome_quality = notes.get("outcome_quality")
    if isinstance(outcome_quality, dict):
        candidates.append(outcome_quality.get("reward_features"))
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = candidate.get("retrieval_outcome_reward")
        if value is None:
            continue
        try:
            return min(1.0, max(0.0, float(value)))
        except (TypeError, ValueError):
            continue
    return None


def _route_cost_units(path: str, *, elapsed_ms: int, budget: int) -> float:
    base = {
        "semantic": 0.34,
        "B": 0.34,
        "temporal": 0.14,
        "C": 0.14,
        "pattern": 0.12,
        "D": 0.12,
        "structural": 0.08,
        "A": 0.08,
        "model_edge": 0.08,
        "G": 0.08,
        "semantic_terms": 0.04,
        "L": 0.04,
        "focused_index": 0.04,
        "projection_context": 0.02,
    }.get(path, 0.08)
    return base + max(0, int(elapsed_ms)) / 1000.0 + max(0, int(budget)) / 120.0


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _profile_prior_prediction_result(
    *,
    score: float,
    skipped: bool,
    returned: bool,
    selected_evidence: int,
) -> str:
    if score >= 0:
        if selected_evidence > 0:
            return "confirmed_useful_context"
        if returned:
            return "weak_unselected_context"
        return "contradicted_no_context"
    if skipped or selected_evidence == 0:
        return "confirmed_suppression"
    return "contradicted_suppressed_useful_context"


def _profile_prior_residual_reason(outcome: dict[str, Any]) -> str:
    return "|".join(
        [
            "sage_profile_prior_contradicted",
            str(outcome.get("prior_kind") or ""),
            str(outcome.get("prior_key") or ""),
            str(outcome.get("question_id") or ""),
            str(outcome.get("path") or ""),
            str(outcome.get("prior_prediction_result") or ""),
        ]
    )


def _profile_prior_residual_summary(outcome: dict[str, Any]) -> str:
    return (
        "SAGE profile prior contradicted during retrieval: "
        f"{outcome.get('prior_kind')}:{outcome.get('prior_key')} "
        f"on {outcome.get('path')} produced "
        f"{outcome.get('prior_prediction_result')}."
    )[:500]


def _execute_row_count(status: str) -> int:
    try:
        return int(str(status).split()[-1])
    except (IndexError, ValueError):
        return 0


__all__ = [
    "RetrievalMotifPenalty",
    "decay_sage_route_utilities",
    "is_low_value_model_noise",
    "learn_retrieval_motifs",
    "learn_sage_route_utilities",
    "load_question_policy_stats",
    "load_retrieval_motifs_for_questions",
    "load_sage_route_utilities",
    "motif_failure_penalties",
    "penalize_retrieval_motifs",
    "profile_prior_outcomes_from_result",
    "record_profile_prior_residuals",
]
