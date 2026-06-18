"""Private helpers for the Think run pipeline.

This module keeps ``reason.py`` focused on orchestration and outcome handling
while preserving the same transaction and observability behavior.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.llm.provider import LLMProvider
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import AccessContext
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.inquiry_traces.emitter import (
    TraceContext as _SageTraceContext,
    emission_enabled as _sage_emission_enabled,
    set_trace_context as _sage_set_trace_context,
)

from .cascade import CascadeEvent, CascadeResult, cascade
from .context_planner import assemble_reasoning_context, plan_context
from .debug_capture import capture as debug_capture
from .deterministic import deterministic_handler, is_authoritative
from .llm_reason import llm_reason
from .observability import (
    METRICS,
    ThinkRunRecord,
    emit,
    insert_think_run,
    update_think_run,
)
from .representation_contract import enrich_raw_diff_representation
from .region_locks import (
    RegionLockAcquisition,
    acquire_region_lock,
    region_lock_key,
)
from .validator import validate


@dataclass
class ReasoningRunState:
    context_plan: Any
    retrieval_result: Any
    reasoning_frame: Any
    bundle: Any
    allowed_region: Any
    actor_operating_summary: Any
    region_tenant_hash: int | None
    region_entity_hash: int | None
    acquisition: RegionLockAcquisition | None
    mutation_row_inserted: bool


@dataclass
class RawReasoningOutput:
    raw_diff: Any
    raw_context_use: dict[str, Any]
    allowed_region: Any
    llm_latency_ms: int | None


def _tx_health_check_enabled() -> bool:
    return os.environ.get("THINK_TX_HEALTH_CHECK", "0") == "1"


def _diff_reuse_on_tx_retry_enabled() -> bool:
    return os.environ.get("THINK_REUSE_DIFF_ON_TX_RETRY", "0").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }


def _hash_context_bundle(trigger: TriggerContext, bundle: Any) -> str:
    from .deterministic import _trigger_ref  # type: ignore

    models = sorted(
        f"{getattr(m, 'id', None)}:"
        f"{getattr(m, 'version', getattr(m, 'updated_at', ''))}"
        for m in (getattr(bundle, "models", None) or [])
    )
    observations = sorted(
        str(getattr(o, "id", None))
        for o in (getattr(bundle, "observations", None) or [])
    )
    payload = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    blob = json.dumps(
        {
            "trigger_ref": str(_trigger_ref(trigger)),
            "models": models,
            "observations": observations,
            "payload": payload,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(blob.encode()).hexdigest()


def _raw_diff_op_count(diff: Any) -> int:
    return sum(
        len(getattr(diff, name, []) or [])
        for name in (
            "claim_ops",
            "memory_lifecycle_ops",
            "relation_claim_ops",
            "relation_frame_ops",
            "edge_ops",
            "ontology_gap_ops",
            "act_ops",
            "resource_ops",
            "new_predictions",
        )
    )


_BATCH_WRAPPER_CLAIM_RE = re.compile(
    r"^\s*(?:the\s+batch\b|this\s+batch\b|batch\s+of\b|"
    r"batch[-\s]+level\b|evidence\s+window\b|the\s+window\s+wrapper\b|"
    r"future\s+plan\s+to\s+verify:\s*batch\b)"
    r"|[,;:]\s*(?:but\s+|and\s+|while\s+)?(?:the\s+batch|this\s+batch)\b",
    re.IGNORECASE,
)


def _drop_event_batch_wrapper_claims(raw_diff: Any, trigger: TriggerContext) -> Any:
    if trigger.kind != "T1":
        return raw_diff
    if trigger.subkind != "event_batch" and not trigger.member_trigger_ids:
        return raw_diff

    kept = []
    dropped = 0
    for op in getattr(raw_diff, "claim_ops", []) or []:
        entry = getattr(op, "entry", None) or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        if not isinstance(prop, dict):
            prop = {}
        natural = str(entry.get("natural") or "")
        candidates = [
            natural,
            str(prop.get("summary") or ""),
            str(prop.get("situation") or ""),
            str(prop.get("subject") or ""),
            str((prop.get("belief_address") or {}).get("subject") or "")
            if isinstance(prop.get("belief_address"), dict)
            else "",
        ]
        if getattr(op, "op", None) == "insert" and any(
            _BATCH_WRAPPER_CLAIM_RE.search(text) for text in candidates if text
        ):
            dropped += 1
            continue
        kept.append(op)

    if dropped:
        raw_diff.claim_ops = kept
        note = f"dropped {dropped} T1:event_batch wrapper claim(s)"
        trace = raw_diff.reasoning_trace or ""
        raw_diff.reasoning_trace = f"{trace}\n{note}".strip() if trace else note
        emit(
            "think.event_batch_wrapper_claims_dropped",
            trigger_ref=str(trigger.observation_id or trigger.model_id or ""),
            dropped=dropped,
        )
    return raw_diff


async def assert_tx_usable(conn: asyncpg.Connection, phase: str) -> None:
    if not _tx_health_check_enabled():
        return
    try:
        await conn.execute("SELECT 1")
    except asyncpg.PostgresError as exc:
        raise RuntimeError(
            "think transaction aborted after " f"{phase}: {type(exc).__name__}: {exc}"
        ) from exc


async def prepare_reasoning_run_state(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    access_context: AccessContext | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    expanded_region: set[tuple[str, str]] | None,
    embedder: Any | None,
) -> ReasoningRunState:
    from services.reasoning.relationships.adjudication import (
        load_candidate_for_trigger,
    )

    loaded_relationship_candidate = await load_candidate_for_trigger(conn, trigger)
    context_plan = await plan_context(
        trigger,
        conn,
        embedder=embedder,
        llm_provider=llm_provider,
    )
    inquiry_result = context_plan.inquiry_result
    retrieval_result = context_plan.retrieval_result
    reasoning_frame = context_plan.reasoning_frame
    await assert_tx_usable(conn, "context_planning")
    await _record_context_plan_observability(
        conn=conn,
        trigger=trigger,
        record=record,
        trigger_kind_full=trigger_kind_full,
        triggering_content=triggering_content,
        reason_for_trigger=reason_for_trigger,
        retrieval_result=retrieval_result,
        reasoning_frame=reasoning_frame,
        inquiry_result=inquiry_result,
        loaded_relationship_candidate=loaded_relationship_candidate,
    )
    _install_sage_inquiry_trace_context(
        conn=conn,
        trigger=trigger,
        record=record,
        trigger_kind_full=trigger_kind_full,
        inquiry_result=inquiry_result,
    )

    reasoning_context = await assemble_reasoning_context(
        context_plan,
        trigger,
        conn,
        access_context=access_context,
        expanded_region=expanded_region,
        run_id=record.id,
    )
    bundle = reasoning_context.bundle
    allowed_region = reasoning_context.allowed_region
    actor_operating_summary = reasoning_context.actor_operating_summary
    await assert_tx_usable(conn, "reasoning_context")

    th: int | None = None
    eh: int | None = None
    acquisition: RegionLockAcquisition | None = None
    mutation_row_inserted = False
    if conn.is_in_transaction():
        th, eh = region_lock_key(
            trigger.tenant_id, [(t, i) for (t, i) in allowed_region]
        )
        await insert_think_run(
            conn,
            record,
            region_tenant_hash=th,
            region_entity_hash=eh,
        )
        await update_think_run(
            conn,
            record.id,
            retrieval_model_count=len(bundle.models),
            retrieval_observation_count=len(bundle.observations),
        )
        acquisition = await acquire_region_lock(
            conn, trigger.tenant_id, [(t, i) for (t, i) in allowed_region]
        )
        locked_context = await assemble_reasoning_context(
            context_plan,
            trigger,
            conn,
            access_context=access_context,
            expanded_region=expanded_region,
            run_id=record.id,
        )
        locked_region = list(locked_context.allowed_region)
        if set(locked_region) != set(allowed_region):
            allowed_region = sorted(set(allowed_region) | set(locked_region))
            acquisition = await acquire_region_lock(
                conn,
                trigger.tenant_id,
                [(t, i) for (t, i) in allowed_region],
            )
        bundle = locked_context.bundle
        actor_operating_summary = locked_context.actor_operating_summary
        await update_think_run(
            conn,
            record.id,
            retrieval_model_count=len(bundle.models),
            retrieval_observation_count=len(bundle.observations),
        )
        mutation_row_inserted = True

    return ReasoningRunState(
        context_plan=context_plan,
        retrieval_result=retrieval_result,
        reasoning_frame=reasoning_frame,
        bundle=bundle,
        allowed_region=allowed_region,
        actor_operating_summary=actor_operating_summary,
        region_tenant_hash=th,
        region_entity_hash=eh,
        acquisition=acquisition,
        mutation_row_inserted=mutation_row_inserted,
    )


async def build_raw_reasoning_output(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    state: ReasoningRunState,
    reason_cache: dict[str, Any] | None,
) -> RawReasoningOutput:
    llm_latency_ms: int | None = None
    if is_authoritative(trigger):
        raw_diff = await deterministic_handler(trigger, state.bundle, conn)
    else:
        if llm_provider is None:
            raise ValidationError(
                "inferential trigger requires llm_provider",
                trigger_kind=trigger.kind,
            )
        bundle_hash: str | None = None
        reused_diff = None
        if reason_cache is not None and _diff_reuse_on_tx_retry_enabled():
            bundle_hash = _hash_context_bundle(trigger, state.bundle)
            if (
                reason_cache.get("bundle_hash") == bundle_hash
                and reason_cache.get("raw_diff") is not None
            ):
                reused_diff = reason_cache["raw_diff"]
        if reused_diff is not None:
            raw_diff = reused_diff
            llm_latency_ms = 0
            emit("think.diff_reused_on_tx_retry", run_id=str(record.id))
        else:
            raw_diff, llm_latency_ms = await llm_reason(
                trigger,
                state.bundle,
                llm_provider,
                triggering_content=triggering_content,
                triggering_actor_summary=state.actor_operating_summary,
                reason_for_trigger=reason_for_trigger,
                reasoning_frame=state.reasoning_frame,
            )
            if reason_cache is not None and bundle_hash is not None:
                reason_cache["bundle_hash"] = bundle_hash
                reason_cache["raw_diff"] = raw_diff

    from .auto_create_commitment import (
        maybe_inject_block_transition,
        maybe_inject_create_commitment,
        maybe_inject_customer_risk,
        maybe_inject_decision_revisit,
        maybe_inject_future_prediction,
    )
    from .bridge_inference import maybe_inject_latent_bridge
    from .capability_probes import maybe_inject_capability_probe_ops
    from .context_use import summarize_context_use
    from .deterministic import _trigger_ref  # type: ignore

    raw_diff.trigger_ref = _trigger_ref(trigger)
    raw_diff.tenant_id = trigger.tenant_id
    raw_diff = maybe_inject_create_commitment(raw_diff, trigger, state.bundle)
    raw_diff = maybe_inject_block_transition(raw_diff, trigger, state.bundle)
    raw_diff = maybe_inject_decision_revisit(raw_diff, trigger, state.bundle)
    raw_diff = maybe_inject_future_prediction(raw_diff, trigger, state.bundle)
    raw_diff = maybe_inject_customer_risk(raw_diff, trigger, state.bundle)
    raw_diff = maybe_inject_latent_bridge(raw_diff, trigger)
    raw_diff = maybe_inject_capability_probe_ops(raw_diff, trigger, state.bundle)
    raw_diff = _drop_event_batch_wrapper_claims(raw_diff, trigger)
    raw_diff = enrich_raw_diff_representation(raw_diff, trigger, state.bundle)
    raw_context_use = summarize_context_use(state.bundle, raw_diff)

    allowed_region = state.allowed_region
    for op in raw_diff.act_ops:
        if op.op == "transition_commitment":
            ent = op.entity or {}
            tid = ent.get("id")
            if tid:
                allowed_region = sorted(
                    set(allowed_region) | {("commitment", str(tid))}
                )
        elif op.op == "transition_decision":
            ent = op.entity or {}
            tid = ent.get("id")
            if tid:
                allowed_region = sorted(set(allowed_region) | {("decision", str(tid))})

    return RawReasoningOutput(
        raw_diff=raw_diff,
        raw_context_use=raw_context_use,
        allowed_region=allowed_region,
        llm_latency_ms=llm_latency_ms,
    )


async def validate_raw_reasoning_output(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    retrieval_result: Any,
    bundle: Any,
    raw: RawReasoningOutput,
    reason_cache: dict[str, Any] | None,
) -> tuple[Any, dict[str, Any]]:
    from .context_use import summarize_context_use

    validated = await validate(
        raw.raw_diff,
        retrieval_result,
        conn,
        allowed_region=raw.allowed_region,
        strict_region=True,
    )
    validated_context_use = summarize_context_use(bundle, validated)
    METRICS.observe_context_use(trigger_kind_full, validated_context_use)
    emit(
        "think.context_use",
        run_id=str(record.id),
        grade=validated_context_use.get("context_use_grade"),
        selected_context_reference_ratio=validated_context_use.get(
            "selected_context_reference_ratio"
        ),
        selected_model_reference_ratio=validated_context_use.get(
            "selected_model_reference_ratio"
        ),
        graph_selected_reference_ratio=validated_context_use.get(
            "graph_selected_reference_ratio"
        ),
        selected_context_used=validated_context_use.get("selected_context_used"),
    )
    emit(
        "think.validation_done",
        run_id=str(record.id),
        claim_ops=len(validated.claim_ops),
        memory_lifecycle_ops=len(validated.memory_lifecycle_ops),
        relation_claim_ops=len(validated.relation_claim_ops),
        relation_frame_ops=len(validated.relation_frame_ops),
        edge_ops=len(validated.edge_ops),
        ontology_gap_ops=len(validated.ontology_gap_ops),
        act_ops=len(validated.act_ops),
        resource_ops=len(validated.resource_ops),
        dropped_ops=validated.dropped_op_count,
    )
    if validated.dropped_op_count:
        emit(
            "think.validation_partial",
            run_id=str(record.id),
            dropped=validated.dropped_op_count,
            errors=validated.dropped_op_errors[:5],
        )
        if reason_cache is not None:
            total_ops = _raw_diff_op_count(raw.raw_diff)
            if total_ops > 0 and validated.dropped_op_count / total_ops > 0.5:
                reason_cache.clear()
    await update_think_run(
        conn,
        record.id,
        validation_error_count=validated.dropped_op_count,
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="validation",
        payload={
            "claim_ops": validated.claim_ops,
            "memory_lifecycle_ops": validated.memory_lifecycle_ops,
            "relation_claim_ops": validated.relation_claim_ops,
            "relation_frame_ops": validated.relation_frame_ops,
            "edge_ops": validated.edge_ops,
            "ontology_gap_ops": validated.ontology_gap_ops,
            "act_ops": validated.act_ops,
            "resource_ops": validated.resource_ops,
            "dropped_op_count": validated.dropped_op_count,
            "dropped_op_errors": list(validated.dropped_op_errors[:20]),
            "context_use": validated_context_use,
        },
    )
    return validated, validated_context_use


async def run_cascade_for_validated_act_ops(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    validated: Any,
) -> CascadeResult | None:
    if not validated.act_ops:
        return None

    seed_op = validated.act_ops[0]
    if seed_op.op == "transition_commitment":
        cid = seed_op.entity.get("id")
        new_state = seed_op.entity.get("new_state")
        if not cid:
            return None
        seed_obs = await conn.fetchval(
            """
            SELECT id FROM observations
            WHERE kind = 'state_change'
              AND tenant_id = $2
              AND entities_mentioned @> $1::jsonb
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            _entities_filter("commitment", cid),
            trigger.tenant_id,
        )
        seed_event = CascadeEvent(
            id=uuid7(),
            kind="commitment_state_change",
            entity_kind="commitment",
            entity_id=UUID(str(cid)),
            tenant_id=trigger.tenant_id,
            metadata={"new_state": new_state},
            observation_id=seed_obs,
        )
        return await cascade(seed_event, conn)

    if (
        seed_op.op == "transition_decision"
        and seed_op.entity.get("new_state") == "revisited"
    ):
        did = seed_op.entity.get("id")
        if not did:
            return None
        seed_obs = await conn.fetchval(
            """
            SELECT id FROM observations
            WHERE kind = 'state_change'
              AND tenant_id = $2
              AND entities_mentioned @> $1::jsonb
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            _entities_filter("decision", did),
            trigger.tenant_id,
        )
        seed_event = CascadeEvent(
            id=uuid7(),
            kind="decision_revisited",
            entity_kind="decision",
            entity_id=UUID(str(did)),
            tenant_id=trigger.tenant_id,
            metadata={},
            observation_id=seed_obs,
        )
        return await cascade(seed_event, conn)

    return None


async def _record_context_plan_observability(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    retrieval_result: Any,
    reasoning_frame: Any,
    inquiry_result: Any | None,
    loaded_relationship_candidate: Any,
) -> None:
    emit(
        "think.retrieval_done",
        run_id=str(record.id),
        models=len(retrieval_result.models),
        observations=len(retrieval_result.observations),
        pathways_run=retrieval_result.notes.get("pathways_run"),
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="trigger",
        payload={
            "trigger_id": str(record.trigger_id),
            "trigger_kind": trigger_kind_full,
            "observation_id": (
                str(trigger.observation_id)
                if getattr(trigger, "observation_id", None)
                else None
            ),
            "triggering_content": triggering_content,
            "reason_for_trigger": reason_for_trigger,
            "reasoning_frame": reasoning_frame.to_dict(),
            "relationship_candidate": loaded_relationship_candidate,
        },
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="retrieval",
        payload={
            "model_count": len(retrieval_result.models),
            "observation_count": len(retrieval_result.observations),
            "notes": retrieval_result.notes,
            "models": [
                {
                    "id": str(getattr(m, "id", None)),
                    "proposition_kind": getattr(m, "proposition_kind", None),
                    "confidence": getattr(m, "confidence", None),
                    "proposition": getattr(m, "proposition", None),
                    "status": getattr(m, "status", None),
                }
                for m in retrieval_result.models
            ],
            "observations": [
                {
                    "id": str(getattr(o, "id", None)),
                    "kind": getattr(o, "kind", None),
                    "source_channel": getattr(o, "source_channel", None),
                    "occurred_at": str(getattr(o, "occurred_at", None)),
                    "content_text": getattr(o, "content_text", None),
                }
                for o in retrieval_result.observations
            ],
        },
    )
    if inquiry_result is None:
        return
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="inquiry",
        payload={
            "session_id": str(inquiry_result.session_id),
            "route": inquiry_result.route,
            "hypotheses": [
                {
                    "id": h.id,
                    "claim": h.claim,
                    "confidence": h.confidence,
                    "impact_if_true": h.impact_if_true,
                }
                for h in inquiry_result.hypotheses
            ],
            "questions": [
                {
                    "question_id": q.question_id,
                    "question": q.question,
                    "primitive": q.primitive,
                    "score": q.score,
                    "round_index": q.round_index,
                }
                for q in inquiry_result.questions
            ],
            "retrieval_actions": [
                {
                    "question_id": a.question_id,
                    "path": a.path,
                    "target": a.target,
                    "budget": a.budget,
                }
                for a in inquiry_result.retrieval_actions
            ],
            "evidence_count": len(inquiry_result.evidence_cards),
        },
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="sufficiency",
        payload={
            "status": inquiry_result.sufficiency.status,
            "reason": inquiry_result.sufficiency.reason,
            "evidence_count": inquiry_result.sufficiency.evidence_count,
            "answered_questions": inquiry_result.sufficiency.answered_questions,
            "remaining_unknowns": list(inquiry_result.sufficiency.remaining_unknowns),
        },
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="context_packet",
        payload=inquiry_result.context_packet,
    )


def _install_sage_inquiry_trace_context(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    inquiry_result: Any | None,
) -> None:
    if inquiry_result is None or not _sage_emission_enabled():
        return

    sage_reader_notes = (inquiry_result.notes or {}).get("sage_reader")
    sage_signatures = (
        sage_reader_notes.get("signatures", [])
        if isinstance(sage_reader_notes, dict)
        else []
    )
    sage_question_primitives = []
    if isinstance(sage_reader_notes, dict):
        for qnote in (sage_reader_notes.get("questions") or {}).values():
            if isinstance(qnote, dict) and qnote.get("question_primitive"):
                primitive = str(qnote["question_primitive"])
                if primitive not in sage_question_primitives:
                    sage_question_primitives.append(primitive)
    _sage_set_trace_context(
        _SageTraceContext(
            tenant_id=trigger.tenant_id,
            inquiry_session_id=inquiry_result.session_id,
            conn=conn,
            metadata={
                "trigger_kind": trigger_kind_full,
                "signal_type": trigger.kind,
                "entities": [
                    str(e.get("id") or e.get("name") or e.get("type"))
                    for e in trigger.seed_entity_ids
                    if isinstance(e, dict)
                ][:12],
                "question_primitives": sage_question_primitives[:8],
                "sage_signatures": sage_signatures[:8],
                "run_id": str(record.id),
            },
        )
    )


def _entities_filter(kind: str, id_: Any) -> str:
    return json.dumps([{"type": kind, "id": str(id_)}])


__all__ = [
    "RawReasoningOutput",
    "ReasoningRunState",
    "assert_tx_usable",
    "build_raw_reasoning_output",
    "prepare_reasoning_run_state",
    "run_cascade_for_validated_act_ops",
    "validate_raw_reasoning_output",
]
