"""Inquiry persistence and trace emission helpers."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext

from .config import _env_float, _env_int
from .evidence_utils import jsonable as _jsonable
from .retrieval_learning import (
    learn_retrieval_motifs as _learn_retrieval_motifs,
    penalize_retrieval_motifs as _penalize_retrieval_motifs,
)
from .reflective_learning import (
    learn_reflective_rules as _learn_reflective_rules,
)
from .sage_reader_notes import (
    compact_inquiry_notes_for_persistence as _compact_inquiry_notes_for_persistence,
)
from .types import EvidenceCard, InquiryResult, RetrievalAction

_READER_ATTRIBUTION_NONSELECTED_LIMIT_DEFAULT = 16
_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE_DEFAULT = 0.55


def _reader_attribution_nonselected_limit() -> int:
    """Operational cap for trace pressure.

    Selected reader decisions are always persisted because the evaluator uses
    them for positive and negative credit. Non-selected high-score decisions are
    useful diagnostics, but at company scale they can dominate storage without
    improving the feedback loop, so deployments can tune this without a code
    deploy.
    """

    return _env_int(
        "SAGE_READER_ATTRIBUTION_NONSELECTED_LIMIT",
        _READER_ATTRIBUTION_NONSELECTED_LIMIT_DEFAULT,
    )


def _reader_attribution_nonselected_min_score() -> float:
    return _env_float(
        "SAGE_READER_ATTRIBUTION_NONSELECTED_MIN_SCORE",
        _READER_ATTRIBUTION_NONSELECTED_MIN_SCORE_DEFAULT,
    )


async def _persist_inquiry(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
    *,
    persist_full_sage_reader_notes: bool = False,
) -> None:
    table_name = await conn.fetchval("SELECT to_regclass('public.inquiry_sessions')")
    if table_name is None:
        return
    tenant_exists = await conn.fetchval(
        "SELECT 1 FROM tenants WHERE id = $1",
        trigger.tenant_id,
    )
    if tenant_exists is None:
        return
    signal_ref_type = (
        "observation"
        if trigger.observation_id
        else ("internal" if trigger.model_id is None else "internal")
    )
    signal_ref_id = trigger.observation_id or trigger.model_id
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
          id, tenant_id, signal_ref_type, signal_ref_id, route,
          status, stop_status, round_count, question_count,
          evidence_count, context_packet, notes, completed_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9,
          $10, $11::jsonb, $12::jsonb, now()
        )
        """,
        result.session_id,
        trigger.tenant_id,
        signal_ref_type,
        signal_ref_id,
        result.route,
        "completed",
        result.sufficiency.status,
        max((q.round_index for q in result.questions), default=0),
        len(result.questions),
        len(result.evidence_cards),
        json.dumps(result.context_packet, default=str),
        json.dumps(
            _compact_inquiry_notes_for_persistence(
                result.notes,
                persist_full_sage_reader_notes=persist_full_sage_reader_notes,
            ),
            default=str,
        ),
    )
    await _persist_sage_reader_activation_traces(conn, result, trigger)
    if result.questions:
        actions_by_question: dict[str, list[dict[str, Any]]] = {}
        for action in result.retrieval_actions:
            actions_by_question.setdefault(action.question_id, []).append(
                _jsonable(asdict(action))
            )
        answers_by_question = {
            answer.question_id: _jsonable(asdict(answer))
            for answer in result.question_answers
        }
        await conn.executemany(
            """
            INSERT INTO inquiry_question_runs (
              id, session_id, tenant_id, question_id, round_index,
              primitive, question, score, retrieval_actions, answer
            ) VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9::jsonb, $10::jsonb
            )
            """,
            [
                (
                    uuid7(),
                    result.session_id,
                    trigger.tenant_id,
                    question.question_id,
                    question.round_index,
                    question.primitive,
                    question.question,
                    float(question.score),
                    json.dumps(actions_by_question.get(question.question_id, [])),
                    json.dumps(answers_by_question.get(question.question_id, {})),
                )
                for question in result.questions
            ],
        )
    if not result.evidence_cards:
        await _penalize_retrieval_motifs(conn, result, trigger)
        await _learn_reflective_rules_best_effort(conn, result, trigger)
        await _emit_phase1_traces(conn, result, trigger)
        return
    await conn.executemany(
        """
        INSERT INTO inquiry_evidence_items (
          id, session_id, tenant_id, source_type, source_ref,
          source_ref_id, summary, trust_tier, occurred_at,
          retrieval_paths, retrieved_for_questions, supports_hypotheses,
          weakens_hypotheses, contradicts_hypotheses, raw_content_ref,
          token_estimate, access_scope, sensitivity, score
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9,
          $10::jsonb, $11::jsonb, $12::jsonb,
          $13::jsonb, $14::jsonb, $15,
          $16, $17, $18, $19
        )
        """,
        [
            (
                card.evidence_id,
                result.session_id,
                trigger.tenant_id,
                card.source_type,
                card.source_ref,
                card.source_ref_id,
                card.summary,
                card.trust_tier,
                card.timestamp,
                json.dumps(sorted(card.retrieval_paths)),
                json.dumps(sorted(card.retrieved_for_questions)),
                json.dumps(sorted(card.supports_hypotheses)),
                json.dumps(sorted(card.weakens_hypotheses)),
                json.dumps(sorted(card.contradicts_hypotheses)),
                card.raw_content_ref,
                card.token_estimate,
                card.access_scope,
                card.sensitivity,
                float(card.score),
            )
            for card in result.evidence_cards
        ],
    )
    await _learn_retrieval_motifs(conn, result, trigger)
    await _penalize_retrieval_motifs(conn, result, trigger)
    await _learn_reflective_rules_best_effort(conn, result, trigger)
    await _emit_phase1_traces(conn, result, trigger)


async def _learn_reflective_rules_best_effort(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    try:
        await _learn_reflective_rules(conn, result, trigger)
    except Exception as exc:  # noqa: BLE001
        import structlog

        structlog.get_logger(__name__).warning(
            "reflective_rule_learning.failed",
            session_id=str(result.session_id),
            error=str(exc),
        )


async def _persist_sage_reader_activation_traces(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_reader_activations')"
    )
    if table_name is None:
        return
    try:
        from services.reasoning.sage.reader import activation_trace_insert_params
    except Exception:  # noqa: BLE001
        return
    sage_notes = (result.notes or {}).get("sage_reader")
    if not isinstance(sage_notes, dict):
        return
    questions = sage_notes.get("questions")
    if not isinstance(questions, dict):
        return
    params: list[tuple[Any, ...]] = []
    for qnote in questions.values():
        if not isinstance(qnote, dict):
            continue
        for raw_trace in qnote.get("activations", []) or []:
            if not isinstance(raw_trace, dict):
                continue
            try:
                from services.reasoning.sage.reader import ReaderActivationTrace

                trace = ReaderActivationTrace(
                    question_id=str(raw_trace["question_id"]),
                    model_id=UUID(str(raw_trace["model_id"])),
                    activation_score=float(raw_trace["activation_score"]),
                    activation_reasons=tuple(
                        str(r) for r in raw_trace.get("activation_reasons", [])
                    ),
                    selected=bool(raw_trace.get("selected", False)),
                    selection_rank=(
                        int(raw_trace["selection_rank"])
                        if raw_trace.get("selection_rank") is not None
                        else None
                    ),
                    source_breakdown=dict(raw_trace.get("source_breakdown") or {}),
                )
            except (KeyError, TypeError, ValueError):
                continue
            params.append(
                activation_trace_insert_params(
                    tenant_id=trigger.tenant_id,
                    inquiry_session_id=result.session_id,
                    trace=trace,
                )
            )
    if not params:
        return
    await conn.executemany(
        """
        INSERT INTO sage_reader_activations (
          id, tenant_id, inquiry_session_id, question_id, model_id,
          activation_score, activation_reasons, selected, selection_rank,
          source_breakdown
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7::jsonb, $8, $9,
          $10::jsonb
        )
        ON CONFLICT (inquiry_session_id, question_id, model_id)
        DO UPDATE SET
          activation_score = EXCLUDED.activation_score,
          activation_reasons = EXCLUDED.activation_reasons,
          selected = EXCLUDED.selected,
          selection_rank = EXCLUDED.selection_rank,
          source_breakdown = EXCLUDED.source_breakdown
        """,
        params,
    )
    await _persist_sage_reader_decision_attributions(conn, result, trigger)


async def _persist_sage_reader_decision_attributions(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.sage_reader_decision_attributions')"
    )
    if table_name is None:
        return
    sage_notes = (result.notes or {}).get("sage_reader")
    if not isinstance(sage_notes, dict):
        return
    questions = sage_notes.get("questions")
    if not isinstance(questions, dict):
        return

    question_by_id = {q.question_id: q for q in result.questions}
    actions_by_question: dict[str, list[dict[str, Any]]] = {}
    for action in result.retrieval_actions:
        actions_by_question.setdefault(action.question_id, []).append(
            _jsonable(asdict(action))
        )
    evidence_by_question = _packet_evidence_refs_by_question(result.evidence_cards)
    entities = _jsonable(trigger.seed_entity_ids)
    params: list[tuple[Any, ...]] = []
    nonselected_limit = _reader_attribution_nonselected_limit()
    nonselected_min_score = _reader_attribution_nonselected_min_score()

    for qid, qnote in questions.items():
        if not isinstance(qnote, dict):
            continue
        question = question_by_id.get(str(qid))
        if question is None:
            continue
        evidence_refs = evidence_by_question.get(str(qid), [])
        nonselected_kept = 0
        for raw_trace in qnote.get("activations", []) or []:
            if not isinstance(raw_trace, dict):
                continue
            try:
                model_id = UUID(str(raw_trace["model_id"]))
                activation_score = float(raw_trace["activation_score"])
                activation_reasons = [
                    str(r) for r in raw_trace.get("activation_reasons", [])
                ]
                selected = bool(raw_trace.get("selected", False))
                selection_rank = (
                    int(raw_trace["selection_rank"])
                    if raw_trace.get("selection_rank") is not None
                    else None
                )
                source_breakdown = dict(raw_trace.get("source_breakdown") or {})
            except (KeyError, TypeError, ValueError):
                continue
            if not selected:
                if (
                    activation_score < nonselected_min_score
                    or nonselected_kept >= nonselected_limit
                ):
                    continue
                nonselected_kept += 1
            model_evidence_refs = [
                ref
                for ref in evidence_refs
                if ref.get("source_ref_id") == str(model_id)
                or ref.get("source_type") == "observation"
            ]
            params.append(
                (
                    uuid7(),
                    trigger.tenant_id,
                    result.session_id,
                    question.question_id,
                    question.primitive,
                    question.question,
                    float(question.score),
                    float(question.expected_value),
                    float(question.expected_cost),
                    trigger.kind,
                    json.dumps(entities, default=str),
                    model_id,
                    selected,
                    selection_rank,
                    activation_score,
                    json.dumps(activation_reasons, default=str),
                    json.dumps(source_breakdown, default=str),
                    json.dumps(actions_by_question.get(question.question_id, [])),
                    json.dumps(model_evidence_refs, default=str),
                    len(model_evidence_refs),
                )
            )
    if not params:
        return
    await conn.executemany(
        """
        INSERT INTO sage_reader_decision_attributions (
          id, tenant_id, inquiry_session_id,
          question_id, question_primitive, question,
          question_score, expected_value, expected_cost,
          signal_type, entities, model_id,
          selected, selection_rank, activation_score,
          activation_reasons, source_breakdown, retrieval_actions,
          projected_evidence_refs, evidence_in_packet_count
        ) VALUES (
          $1, $2, $3,
          $4, $5, $6,
          $7, $8, $9,
          $10, $11::jsonb, $12,
          $13, $14, $15,
          $16::jsonb, $17::jsonb, $18::jsonb,
          $19::jsonb, $20
        )
        ON CONFLICT (inquiry_session_id, question_id, model_id)
        DO UPDATE SET
          question_primitive = EXCLUDED.question_primitive,
          question = EXCLUDED.question,
          question_score = EXCLUDED.question_score,
          expected_value = EXCLUDED.expected_value,
          expected_cost = EXCLUDED.expected_cost,
          signal_type = EXCLUDED.signal_type,
          entities = EXCLUDED.entities,
          selected = EXCLUDED.selected,
          selection_rank = EXCLUDED.selection_rank,
          activation_score = EXCLUDED.activation_score,
          activation_reasons = EXCLUDED.activation_reasons,
          source_breakdown = EXCLUDED.source_breakdown,
          retrieval_actions = EXCLUDED.retrieval_actions,
          projected_evidence_refs = EXCLUDED.projected_evidence_refs,
          evidence_in_packet_count = EXCLUDED.evidence_in_packet_count,
          updated_at = now()
        """,
        params,
    )


def _packet_evidence_refs_by_question(
    evidence_cards: tuple[EvidenceCard, ...],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for card in evidence_cards:
        ref = {
            "evidence_id": str(card.evidence_id),
            "source_type": card.source_type,
            "source_ref": card.source_ref,
            "source_ref_id": str(card.source_ref_id) if card.source_ref_id else None,
            "score": float(card.score),
        }
        for question_id in card.retrieved_for_questions:
            out.setdefault(str(question_id), []).append(ref)
    return out


async def _emit_phase1_traces(
    conn: asyncpg.Connection,
    result: InquiryResult,
    trigger: TriggerContext,
) -> None:
    """Write Phase 1 trace rows: retrieval_plans, omitted_evidence, and
    the packet inclusion/omission outcome events.

    Best-effort by design — the emitter helpers swallow per-row errors
    with a warning so a Sage write hiccup never aborts the inquiry
    persistence path. We still wrap the whole batch in a try/except
    because an unexpected import-time error (e.g. missing migration in
    a test DB) should NOT bring the existing pipeline down.
    """
    # Local import keeps the inquiry runtime free of an import-cycle
    # risk against services.reasoning.sage and lets the trace surface stay
    # optional in environments that haven't installed migration 0084.
    try:
        from services.reasoning.sage.inquiry_traces.emitter import (
            TraceContext,
            emission_enabled,
            emit_event,
            emit_omitted_evidence,
            emit_retrieval_plan,
            reset_trace_context,
            set_trace_context,
        )
    except Exception as exc:  # noqa: BLE001 — never block the pipeline
        import structlog

        structlog.get_logger(__name__).warning(
            "sage_trace.import_failed",
            session_id=str(result.session_id),
            error=str(exc),
        )
        return

    if not emission_enabled():
        return

    # Confirm the Phase 1 tables exist before any write attempts. The
    # repo path already swallows errors, but skipping early avoids
    # adding noise to every legacy / pre-0084 deployment's logs.
    plans_table = await conn.fetchval("SELECT to_regclass('public.retrieval_plans')")
    if plans_table is None:
        return

    ctx = TraceContext(
        tenant_id=trigger.tenant_id,
        inquiry_session_id=result.session_id,
        conn=conn,
        metadata={
            "trigger_kind": getattr(trigger, "kind", None),
            "route": result.route,
        },
    )
    token = set_trace_context(ctx)
    try:
        # --- 1. retrieval_plans (one per question, revision 0) -------
        # We reuse the planning data already computed on the question +
        # the action set the planner compiled — no second pass over the
        # LLM, no new fields on the question struct.
        actions_by_question: dict[str, list[RetrievalAction]] = {}
        for action in result.retrieval_actions:
            actions_by_question.setdefault(action.question_id, []).append(action)
        for question in result.questions:
            actions = actions_by_question.get(question.question_id, [])
            paths_payload = [
                {
                    "path": a.path,
                    "target": a.target,
                    "budget": int(a.budget),
                }
                for a in actions
            ]
            intents_payload = [
                {
                    "primitive": question.primitive,
                    "question": question.question,
                    "retrieval_target": question.retrieval_target,
                    "expected_value": float(question.expected_value),
                    "expected_cost": float(question.expected_cost),
                    "tests_hypotheses": list(question.tests_hypotheses),
                }
            ]
            budgets_payload = {
                "action_count": len(actions),
                "total_budget": sum(int(a.budget) for a in actions),
            }
            success_conditions_payload = (
                [{"stop_condition": question.stop_condition}]
                if question.stop_condition
                else []
            )
            notes_payload = {
                "round_index": int(question.round_index),
                "score": round(float(question.score), 4),
            }
            await emit_retrieval_plan(
                question_id=question.question_id,
                plan_revision=0,
                intents=intents_payload,
                paths=paths_payload,
                budgets=budgets_payload,
                success_conditions=success_conditions_payload,
                notes=notes_payload,
                ctx=ctx,
            )

        # --- 2. omitted_evidence + packet inclusion/omission events --
        # The packet builder already computes which cards are decisive
        # vs. supporting (grouped) vs. omitted via the tiers structure
        # and the omission_ledger. We re-derive the per-evidence-id set
        # of "made the packet" using the same packet dict so the trace
        # stays consistent with what the LLM actually saw.
        packet = result.context_packet or {}
        tiers = packet.get("tiers", {}) or {}
        decisive_ids: set[str] = set()
        for item in tiers.get("decisive_evidence", []) or []:
            ev_id = item.get("evidence_id")
            if ev_id:
                decisive_ids.add(str(ev_id))
        grouped_ids: set[str] = set()
        for group in tiers.get("supporting_evidence_groups", []) or []:
            for ev_id in group.get("evidence_ids", []) or []:
                grouped_ids.add(str(ev_id))
        used_ids = decisive_ids | grouped_ids
        budget_used = (packet.get("budget") or {}).get(
            "estimated_tokens_used",
            0,
        )
        budget_cap = (packet.get("budget") or {}).get(
            "token_budget",
            0,
        )

        for card in result.evidence_cards:
            ev_id_str = str(card.evidence_id)
            paths_payload = [{"path": p} for p in sorted(card.retrieval_paths)]
            common_payload: dict[str, Any] = {
                "evidence_id": ev_id_str,
                "source_type": card.source_type,
                "source_ref_id": (
                    str(card.source_ref_id) if card.source_ref_id else None
                ),
                "source_ref": card.source_ref,
                "score": round(float(card.score), 4),
                "retrieval_paths": sorted(card.retrieval_paths),
            }
            if ev_id_str in used_ids:
                tier = "decisive" if ev_id_str in decisive_ids else "supporting"
                await emit_event(
                    "retrieved_evidence_used_in_packet",
                    {**common_payload, "tier": tier},
                    ctx=ctx,
                )
            else:
                # Pick the most specific omission reason we can infer
                # from the card. The packet compiler's omission_ledger
                # uses free text; we map to the closed enum so the
                # topology optimizer can group on a stable key.
                reason = _classify_omission_reason(
                    card,
                    packet_budget_cap=budget_cap,
                    packet_budget_used=budget_used,
                )
                first_question = (
                    sorted(card.retrieved_for_questions)[0]
                    if card.retrieved_for_questions
                    else None
                )
                await emit_omitted_evidence(
                    source_type=card.source_type,
                    source_ref=card.source_ref,
                    source_ref_id=card.source_ref_id,
                    question_id=first_question,
                    retrieval_paths=paths_payload,
                    omission_reason=reason,
                    reason_detail="dropped during packet compilation",
                    score=float(card.score),
                    metadata={
                        "trust_tier": card.trust_tier,
                        "retrieved_for_questions": sorted(card.retrieved_for_questions),
                        "supports_hypotheses": sorted(card.supports_hypotheses),
                        "weakens_hypotheses": sorted(card.weakens_hypotheses),
                        "contradicts_hypotheses": sorted(card.contradicts_hypotheses),
                    },
                    ctx=ctx,
                )
                await emit_event(
                    "retrieved_evidence_omitted",
                    {**common_payload, "omission_reason": reason},
                    ctx=ctx,
                )
    finally:
        reset_trace_context(token)


def _classify_omission_reason(
    card: EvidenceCard,
    *,
    packet_budget_cap: int,
    packet_budget_used: int,
) -> str:
    """Map an evidence card to one of the closed `OMISSION_REASONS`.

    The packet compiler's own logic is the source of truth for "what
    landed in the packet"; here we just produce a stable categorical
    tag for the topology optimizer. Rules:

      * model row with no hypothesis link → `generic_hub`
        (matches `_is_low_value_model_noise`)
      * cards crowded out when the packet is at/near its token cap →
        `budget_exhausted`
      * everything else → `redundant` (this is the fallback for
        supporting-evidence groups capped at N items per group, which
        is the dominant exclusion path in practice).
    """
    is_model_noise = (
        card.source_type == "model"
        and not card.supports_hypotheses
        and not card.weakens_hypotheses
        and not card.contradicts_hypotheses
    )
    if is_model_noise:
        return "generic_hub"
    if packet_budget_cap > 0 and packet_budget_used >= int(packet_budget_cap * 0.95):
        return "budget_exhausted"
    return "redundant"


__all__ = [
    "_classify_omission_reason",
    "_emit_phase1_traces",
    "_packet_evidence_refs_by_question",
    "_persist_inquiry",
    "_persist_sage_reader_activation_traces",
    "_persist_sage_reader_decision_attributions",
    "_reader_attribution_nonselected_limit",
    "_reader_attribution_nonselected_min_score",
]
