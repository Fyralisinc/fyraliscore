"""services/reasoning/think/applier.py — diff application inside a transaction.

Spec §7 "Apply in transaction". BUILD-PLAN §4 Prompt 3.B item 6.

Ordering: claim_ops first (Models may be referenced by subsequent
act_ops.confidence_basis), then act_ops, then resource_ops. Every op
runs through the existing Wave-1/2 repos via the caller's `conn`.

Idempotency: `applied_triggers` row is inserted with outcome='pending'
BEFORE any op runs. If the transaction commits, outcome is updated to
'success' in the SAME transaction. A second Think run with the same
trigger_id sees the existing row and short-circuits.

Partial-failure policy: claim apply failures remain transaction-fatal
because downstream ops may depend on newly-created Models. Domain-invalid
edge, act, and resource ops discovered late at apply time are classified
and dropped, matching the validator's partial-accept policy. Unexpected
errors still propagate and roll back the whole transaction —
applied_triggers row included.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.observability.metrics import record_doc_memory_model_minted
from lib.shared.errors import CompanyOSError, InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from lib.shared.types import ModelCreate
from lib.shared.edge_registry import EdgeRegistryError
from services.domain.acts import commitments as commitments_svc
from services.domain.acts import decisions as decisions_svc
from services.domain.acts import goals as goals_svc
from services.domain.models.propositions import ensure_situation_compositional_defaults
from services.domain.models.propositions import (
    canonicalize_proposition,
    validate_proposition,
)
from services.domain.models.repo import ModelsRepo
from services.domain.observations.state_change import emit_state_change
from services.domain.resources import deployments as deployments_svc
from services.domain.resources import repo as resources_repo
from services.domain.resources.transactions import record_transaction
from services.domain.resources.deployments import release as release_deployment

from .diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    MemoryLifecycleOp,
    OntologyGapOp,
    RawDiff,
    RelationClaimOp,
    RelationFrameOp,
    ResourceOp,
    ValidatedDiff,
)
from .evidence_support import compact_supporting_event_ids
from .observability import log_dropped_op
from .prediction_lifecycle import (
    materialize_model_prediction,
    prepare_prediction_entry,
    sync_model_prediction_resolution,
)
from .quality_gate import QualityContext, QualityVerdict, apply_verdict, score_quality
from .splitter import split_compound_claim_op
from .synthesis_decision import summarize_synthesis_decisions
from .text_embedding import deterministic_text_embedding, is_zero_embedding


def _raise_if_postgres_error(exc: Exception) -> None:
    if isinstance(exc, asyncpg.PostgresError):
        raise exc


def _doc_memory_source_from_cascade(payload: dict[str, Any] | None) -> str | None:
    """Return the document source_channel iff this apply is a document-memory mint.

    Under ratified Option A (docs/plans/document-memory-substrate.md §4.1–§4.4),
    the only document-derived Models are those minted by Think over an enriched
    T1 trigger — the trigger whose ``seed_signature`` carries the structured
    document summary. ``apply_diff`` already receives that ``seed_signature`` as
    ``parent_cascade_payload``, so a non-empty ``doc_structured_summary`` is the
    provenance marker the worker attached (and is what distinguishes a document
    Model from any other Model born_from the same observation). The carried
    ``source_channel`` is returned so the mint counter is keyed by source; a
    non-document trigger (no ``doc_structured_summary``) returns None and is NOT
    counted.
    """
    if not isinstance(payload, dict):
        return None
    structured = payload.get("doc_structured_summary")
    if not structured:
        return None
    source_channel = payload.get("source_channel")
    return source_channel if isinstance(source_channel, str) else None


_RELATION_CLAIM_SUPPORT_SUPERSEDERS = frozenset({
    "blocks",
    "contradicts",
    "weakens",
})
_NON_OVERRIDABLE_EDGE_PROVENANCE = frozenset({"manual"})


class ApplierError(CompanyOSError):
    default_code = "applier_error"


class AlreadyAppliedError(ApplierError):
    """
    The trigger_id already has a row in applied_triggers. The caller
    should short-circuit (set think_runs.status='skipped_idempotent')
    without running any ops.
    """

    default_code = "already_applied"


# ---------------------------------------------------------------------
# Diff hashing for applied_triggers.diff_hash
# ---------------------------------------------------------------------


def hash_diff(diff: ValidatedDiff | RawDiff) -> str:
    """Stable content hash of the diff for audit."""
    payload = {
        "trigger_ref": str(diff.trigger_ref),
        "tenant_id": str(diff.tenant_id),
        "claim_ops": [op.model_dump(mode="json") for op in diff.claim_ops],
        "memory_lifecycle_ops": [
            op.model_dump(mode="json") for op in diff.memory_lifecycle_ops
        ],
        "relation_claim_ops": [
            op.model_dump(mode="json") for op in diff.relation_claim_ops
        ],
        "relation_frame_ops": [
            op.model_dump(mode="json") for op in diff.relation_frame_ops
        ],
        "edge_ops": [op.model_dump(mode="json") for op in diff.edge_ops],
        "ontology_gap_ops": [
            op.model_dump(mode="json") for op in diff.ontology_gap_ops
        ],
        "act_ops": [op.model_dump(mode="json") for op in diff.act_ops],
        "resource_ops": [op.model_dump(mode="json") for op in diff.resource_ops],
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


async def check_already_applied(
    conn: asyncpg.Connection, trigger_ref: UUID
) -> str | None:
    """Cost-plan §2.2: return the prior `applied_triggers.outcome` for a trigger,
    or None if it has not been applied. Extracted from `apply_diff`'s inline
    guard so `think()` can short-circuit an already-applied trigger *before*
    paying for retrieval + LLM reasoning."""
    row = await conn.fetchrow(
        "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
        trigger_ref,
    )
    return row["outcome"] if row is not None else None


async def _record_apply_drop(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    op_type: str,
    op_kind: str,
    reason: str,
    message: str,
) -> None:
    try:
        from services.domain.feedback_stats import record_feedback_stat

        await record_feedback_stat(
            conn,
            tenant_id=tenant_id,
            surface="think_apply",
            op_type=op_type,
            op_kind=op_kind,
            outcome="dropped",
            reason=reason,
            payload={"message": message[:500]},
        )
    except asyncpg.PostgresError:
        raise


# ---------------------------------------------------------------------
# Main apply entry point
# ---------------------------------------------------------------------


async def _prepare_apply_transaction(
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    trigger_kind: str,
) -> str:
    from .region_locks import (
        acquire_region_lock as _acquire_region_lock,
        touched_entity_ids_from_diff as _touched_from_diff,
    )

    # The retrieval/validation region lock keeps the LLM inside its legal
    # evidence boundary, but concurrent diffs can still reconcile or edge-sync
    # into the same `models` rows through production side effects that were not
    # explicit in the raw diff. Serialize the short model-write critical
    # section per tenant so parallel Think runs can keep doing expensive
    # retrieval/LLM work without deadlocking while mutating the memory graph.
    await _acquire_region_lock(
        conn,
        diff.tenant_id,
        [("tenant_model_write", str(diff.tenant_id))],
    )

    diff_entities = _touched_from_diff(diff)
    if diff_entities:
        await _acquire_region_lock(conn, diff.tenant_id, diff_entities)

    diff_hash = hash_diff(diff)
    inserted = await conn.fetchrow(
        """
        INSERT INTO applied_triggers
          (trigger_id, tenant_id, applied_at, diff_hash, trigger_kind, outcome)
        VALUES ($1, $2, now(), $3, $4, 'pending')
        ON CONFLICT (trigger_id) DO NOTHING
        RETURNING outcome
        """,
        diff.trigger_ref,
        diff.tenant_id,
        diff_hash,
        trigger_kind,
    )
    if inserted is None:
        prior_outcome = await check_already_applied(conn, diff.trigger_ref)
        raise AlreadyAppliedError(
            "trigger already applied",
            trigger_id=str(diff.trigger_ref),
            prior_outcome=prior_outcome or "unknown",
        )
    return diff_hash


def _expand_claim_ops_for_splitter(
    claim_ops: list[ClaimOp],
    *,
    trigger_cause_event_id: UUID | None,
    trigger_evidence_ids: list[UUID],
) -> tuple[list[tuple[ClaimOp, ClaimOp, int | None]], dict[str, int]]:
    split_summary: dict[str, int] = {
        "compound_inputs": 0,
        "atomic_outputs": 0,
        "synthesized_situations": 0,
    }
    # Each compound op becomes a contiguous group of N atomic ops +
    # 1 synthesized situation. We track the group via `gid` so we
    # can patch member_model_ids on the situation after its atomics
    # commit. Non-compound inputs pass through with `gid=None`.
    expanded_ops: list[tuple[ClaimOp, ClaimOp, int | None]] = []
    next_gid = 0
    for src_op in claim_ops:
        src_op = _with_claim_evidence_defaults(
            src_op,
            trigger_cause_event_id=trigger_cause_event_id,
            trigger_supporting_event_ids=trigger_evidence_ids,
        )
        if src_op.op != "insert":
            expanded_ops.append((src_op, src_op, None))
            continue
        splits = split_compound_claim_op(src_op)
        if len(splits) <= 1:
            expanded_ops.append((src_op, src_op, None))
            continue
        gid = next_gid
        next_gid += 1
        split_summary["compound_inputs"] += 1
        split_summary["atomic_outputs"] += max(0, len(splits) - 1)
        split_summary["synthesized_situations"] += 1
        for split_op in splits:
            expanded_ops.append((src_op, split_op, gid))
    return expanded_ops, split_summary


async def _apply_edge_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    pending_model_ids_by_event_id: dict[UUID, UUID],
    trigger_cause_event_id: UUID | None,
    ops_summary: dict[str, Any],
) -> list[EdgeOp]:
    applied_ops: list[EdgeOp] = []
    for op in diff.edge_ops:
        op = _resolve_pending_edge_model_refs(op, pending_model_ids_by_event_id)
        if op.source_model_id == op.target_model_id:
            ops_summary["edge_ops"].append(
                {
                    "op": "skip",
                    "edge_kind": op.edge_kind,
                    "source_model_id": str(op.source_model_id),
                    "target_model_id": str(op.target_model_id),
                    "reason": "resolved_to_same_model_after_reconciliation",
                }
            )
            continue
        try:
            result = await _apply_edge_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
            )
        except (EdgeRegistryError, ValidationError) as exc:
            reason = _classify_apply_edge_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="edge",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="edge",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["edge_ops"].append(
                {
                    "op": "skip",
                    "edge_kind": op.edge_kind,
                    "source_model_id": str(op.source_model_id),
                    "target_model_id": str(op.target_model_id),
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["edge_ops"].append(result["summary"])
        applied_ops.append(op)
    return applied_ops


async def _apply_relation_claim_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    pending_model_ids_by_event_id: dict[UUID, UUID],
    trigger_cause_event_id: UUID | None,
    think_run_id: UUID | None,
    ops_summary: dict[str, Any],
) -> list[RelationClaimOp]:
    applied_ops: list[RelationClaimOp] = []
    for op in diff.relation_claim_ops:
        op = _resolve_pending_relation_claim_model_refs(
            op,
            pending_model_ids_by_event_id,
        )
        if (
            op.source_model_id is not None
            and op.target_model_id is not None
            and op.source_model_id == op.target_model_id
        ):
            ops_summary["relation_claim_ops"].append(
                {
                    "op": "skip",
                    "edge_kind": op.edge_kind,
                    "reason": "resolved_to_same_model_after_reconciliation",
                }
            )
            continue
        try:
            result = await _apply_relation_claim_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
                think_run_id=think_run_id,
            )
        except (EdgeRegistryError, ValidationError) as exc:
            reason = _classify_apply_edge_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="relation_claim",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="relation_claim",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["relation_claim_ops"].append(
                {
                    "op": "skip",
                    "edge_kind": op.edge_kind,
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["relation_claim_ops"].append(result["summary"])
        edge_summaries = result.get("edge_summaries")
        if isinstance(edge_summaries, list):
            ops_summary["edge_ops"].extend(edge_summaries)
        elif result.get("edge_summary") is not None:
            ops_summary["edge_ops"].append(result["edge_summary"])
        applied_ops.append(op)
    return applied_ops


async def _apply_relation_frame_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    pending_model_ids_by_event_id: dict[UUID, UUID],
    trigger_cause_event_id: UUID | None,
    think_run_id: UUID | None,
    ops_summary: dict[str, Any],
) -> list[RelationFrameOp]:
    applied_ops: list[RelationFrameOp] = []
    for op in diff.relation_frame_ops:
        op = _resolve_pending_relation_frame_model_refs(
            op,
            pending_model_ids_by_event_id,
        )
        try:
            result = await _apply_relation_frame_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
                think_run_id=think_run_id,
            )
        except (EdgeRegistryError, ValidationError) as exc:
            reason = _classify_apply_edge_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="relation_frame",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="relation_frame",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["relation_frame_ops"].append(
                {
                    "op": "skip",
                    "relation_kind": op.relation_kind,
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["relation_frame_ops"].append(result["summary"])
        ops_summary["edge_ops"].extend(result.get("edge_summaries") or [])
        applied_ops.append(op)
    return applied_ops


async def _apply_ontology_gap_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    pending_model_ids_by_event_id: dict[UUID, UUID],
    trigger_cause_event_id: UUID | None,
    ops_summary: dict[str, Any],
) -> list[OntologyGapOp]:
    applied_ops: list[OntologyGapOp] = []
    for op in diff.ontology_gap_ops:
        op = _resolve_pending_ontology_gap_model_refs(
            op,
            pending_model_ids_by_event_id,
        )
        if op.source_model_id == op.target_model_id:
            ops_summary["ontology_gap_ops"].append(
                {
                    "op": "skip",
                    "proposed_edge_kind": op.proposed_edge_kind,
                    "source_model_id": str(op.source_model_id),
                    "target_model_id": str(op.target_model_id),
                    "reason": "resolved_to_same_model_after_reconciliation",
                }
            )
            continue
        try:
            result = await _apply_ontology_gap_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
            )
        except ValidationError as exc:
            reason = _classify_apply_ontology_gap_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="ontology_gap",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="ontology_gap",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["ontology_gap_ops"].append(
                {
                    "op": "skip",
                    "proposed_edge_kind": op.proposed_edge_kind,
                    "source_model_id": str(op.source_model_id),
                    "target_model_id": str(op.target_model_id),
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["ontology_gap_ops"].append(result["summary"])
        applied_ops.append(op)
    return applied_ops


async def _enqueue_belief_updated_for_applied_models(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    model_ids: list[UUID],
    source_observation_id: UUID | None,
    parent_payload: dict[str, Any] | None,
) -> None:
    if not model_ids:
        return
    from services.reasoning.think.cascade import enqueue_t2_belief_updated

    for model_id in model_ids:
        await enqueue_t2_belief_updated(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            source_observation_id=source_observation_id,
            parent_payload=parent_payload,
        )


_T2_REEVALUATION_KINDS = {"prediction"}


@dataclass
class _ClaimOpsApplyResult:
    applied_model_ids: list[UUID]
    belief_updated_model_ids: list[UUID]
    pending_model_ids_by_event_id: dict[UUID, UUID]
    state_changes_emitted: int
    expanded_claim_op_count: int
    split_summary: dict[str, int]


async def _apply_claim_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    trigger_cause_event_id: UUID | None,
    trigger_evidence_ids: list[UUID],
    think_run_id: UUID | None,
    ops_summary: dict[str, Any],
    doc_memory_source: str | None = None,
) -> _ClaimOpsApplyResult:
    from .reconciler import reconcile_claim_op

    reconcile_summary: dict[str, int] = {
        "auto_merge": 0,
        "human_review": 0,
        "no_match": 0,
        "skipped": 0,
    }
    quality_summary: dict[str, int] = {
        "accept": 0,
        "needs_review": 0,
        "reject": 0,
        "downgrade_to_evidence": 0,
    }
    expanded_ops, split_summary = _expand_claim_ops_for_splitter(
        diff.claim_ops,
        trigger_cause_event_id=trigger_cause_event_id,
        trigger_evidence_ids=trigger_evidence_ids,
    )
    result = _ClaimOpsApplyResult(
        applied_model_ids=[],
        belief_updated_model_ids=[],
        pending_model_ids_by_event_id={},
        state_changes_emitted=0,
        expanded_claim_op_count=len(expanded_ops),
        split_summary=split_summary,
    )
    group_member_ids: dict[int, list[UUID]] = {}

    for original_op, expanded_op, gid in expanded_ops:
        await _apply_one_expanded_claim_op(
            original_op=original_op,
            expanded_op=expanded_op,
            split_group_id=gid,
            diff=diff,
            conn=conn,
            models_repo=models_repo,
            trigger_cause_event_id=trigger_cause_event_id,
            trigger_evidence_ids=trigger_evidence_ids,
            think_run_id=think_run_id,
            reconcile_claim_op=reconcile_claim_op,
            reconcile_summary=reconcile_summary,
            quality_summary=quality_summary,
            group_member_ids=group_member_ids,
            ops_summary=ops_summary,
            result=result,
            doc_memory_source=doc_memory_source,
        )

    ops_summary["reconcile_summary"] = reconcile_summary
    ops_summary["quality_summary"] = quality_summary
    ops_summary["split_summary"] = split_summary
    return result


async def _apply_one_expanded_claim_op(
    *,
    original_op: ClaimOp,
    expanded_op: ClaimOp,
    split_group_id: int | None,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    trigger_cause_event_id: UUID | None,
    trigger_evidence_ids: list[UUID],
    think_run_id: UUID | None,
    reconcile_claim_op: Any,
    reconcile_summary: dict[str, int],
    quality_summary: dict[str, int],
    group_member_ids: dict[int, list[UUID]],
    ops_summary: dict[str, Any],
    result: _ClaimOpsApplyResult,
    doc_memory_source: str | None = None,
) -> None:
    op = expanded_op
    recon_result = None
    verdict = None
    is_pending_situation = _patch_pending_situation_members(
        op=op,
        split_group_id=split_group_id,
        group_member_ids=group_member_ids,
        ops_summary=ops_summary,
    )
    if is_pending_situation is None:
        return

    if op.op == "insert":
        recon_result = await reconcile_claim_op(
            op,
            conn,
            tenant_id=diff.tenant_id,
            trigger_id=diff.trigger_ref,
            think_run_id=think_run_id,
        )
        reconcile_summary[recon_result.decision] += 1
        if recon_result.replacement_op is not None:
            op = recon_result.replacement_op
        else:
            verdict = score_quality(
                op,
                QualityContext(
                    reconcile_result=recon_result,
                    trigger_kind=getattr(diff.trigger_ref, "kind", None),
                    tenant_id=diff.tenant_id,
                ),
            )
            quality_summary[verdict.decision] = (
                quality_summary.get(verdict.decision, 0) + 1
            )
            op_after_verdict, side_ops = apply_verdict(op, verdict)
            _ = side_ops
            if op_after_verdict is None:
                await _record_rejected_or_downgraded_claim_op(
                    op=op,
                    conn=conn,
                    tenant_id=diff.tenant_id,
                    trigger_cause_event_id=trigger_cause_event_id,
                    trigger_evidence_ids=trigger_evidence_ids,
                    verdict=verdict,
                    recon_result=recon_result,
                    split_group_id=split_group_id,
                    ops_summary=ops_summary,
                    result=result,
                )
                return
            op = op_after_verdict

    if split_group_id is None and _should_absorb_near_duplicate(
        op, recon_result, verdict
    ):
        apply_result = await _apply_near_duplicate_absorption(
            op,
            conn,
            tenant_id=diff.tenant_id,
            cause_event_id=trigger_cause_event_id,
            trigger_supporting_event_ids=trigger_evidence_ids,
            verdict=verdict,
            recon_result=recon_result,
        )
        ops_summary["claim_ops"].append(apply_result["summary"])
        result.state_changes_emitted += apply_result.get("state_changes", 0)
        return

    if op.op == "insert" and _entry_is_situation(op.entry):
        apply_result = await _coalesce_same_event_situation_insert(
            op,
            conn,
            tenant_id=diff.tenant_id,
            cause_event_id=trigger_cause_event_id,
            trigger_supporting_event_ids=trigger_evidence_ids,
        )
        if apply_result is not None:
            _annotate_claim_result_summary(
                apply_result["summary"],
                recon_result=recon_result,
                verdict=verdict,
                split_group_id=split_group_id,
            )
            ops_summary["claim_ops"].append(apply_result["summary"])
            if apply_result.get("model_id") is not None:
                result.applied_model_ids.append(apply_result["model_id"])
            result.state_changes_emitted += apply_result.get("state_changes", 0)
            return

    is_recon_merge = (
        recon_result is not None
        and recon_result.decision == "auto_merge"
        and recon_result.replacement_op is not None
    )
    apply_result = await _apply_claim_op(
        op,
        conn,
        models_repo,
        diff.tenant_id,
        cause_event_id=trigger_cause_event_id,
        trigger_supporting_event_ids=trigger_evidence_ids,
        audit_cause_override=("reconciliation_merge" if is_recon_merge else None),
        doc_memory_source=doc_memory_source,
    )
    _annotate_claim_result_summary(
        apply_result["summary"],
        recon_result=recon_result,
        verdict=verdict,
        split_group_id=split_group_id,
    )
    _record_claim_apply_result(
        apply_result=apply_result,
        original_op=original_op,
        applied_op=op,
        split_group_id=split_group_id,
        is_pending_situation=is_pending_situation,
        group_member_ids=group_member_ids,
        ops_summary=ops_summary,
        result=result,
    )


def _patch_pending_situation_members(
    *,
    op: ClaimOp,
    split_group_id: int | None,
    group_member_ids: dict[int, list[UUID]],
    ops_summary: dict[str, Any],
) -> bool | None:
    is_pending_situation = (
        op.op == "insert"
        and isinstance(op.entry, dict)
        and op.entry.get("member_model_pending") is True
    )
    if not is_pending_situation:
        return False
    members = (
        group_member_ids.get(split_group_id, []) if split_group_id is not None else []
    )
    if not members:
        ops_summary["claim_ops"].append(
            {
                "op": "skip",
                "reason": "situation_skipped_no_atomic_members_after_quality_gate",
                "split_group_id": split_group_id,
            }
        )
        return None

    deduped_members = list(dict.fromkeys(members))
    if len(deduped_members) < 2:
        ops_summary["claim_ops"].append(
            {
                "op": "skip",
                "reason": "situation_skipped_insufficient_atomic_members_after_quality_gate",
                "split_group_id": split_group_id,
                "member_count": len(deduped_members),
            }
        )
        return None

    prop = op.entry.get("proposition") or {}
    prop["member_model_ids"] = [str(uid) for uid in deduped_members]
    op.entry["proposition"] = prop
    op.entry.pop("member_model_pending", None)
    op.entry.pop("split_reasons", None)
    return True


async def _record_rejected_or_downgraded_claim_op(
    *,
    op: ClaimOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    trigger_cause_event_id: UUID | None,
    trigger_evidence_ids: list[UUID],
    verdict: QualityVerdict,
    recon_result: Any,
    split_group_id: int | None,
    ops_summary: dict[str, Any],
    result: _ClaimOpsApplyResult,
) -> None:
    if verdict.decision == "downgrade_to_evidence":
        apply_result = await _apply_evidence_downgrade(
            op,
            conn,
            tenant_id=tenant_id,
            cause_event_id=trigger_cause_event_id,
            trigger_supporting_event_ids=trigger_evidence_ids,
            verdict=verdict,
            preferred_model_id=(
                recon_result.matched_model_id if recon_result is not None else None
            ),
        )
        if split_group_id is not None:
            apply_result["summary"]["split_group_id"] = split_group_id
        ops_summary["claim_ops"].append(apply_result["summary"])
        result.state_changes_emitted += apply_result.get("state_changes", 0)
        return

    ops_summary["claim_ops"].append(
        {
            "op": "skip",
            "reason": f"quality_gate_{verdict.decision}",
            "quality_verdict": _quality_verdict_summary(verdict),
            "split_group_id": split_group_id,
        }
    )


def _annotate_claim_result_summary(
    summary: dict[str, Any],
    *,
    recon_result: Any,
    verdict: QualityVerdict | None,
    split_group_id: int | None,
) -> None:
    if recon_result is not None and recon_result.decision != "skipped":
        summary["reconcile_decision"] = recon_result.decision
        if recon_result.matched_model_id is not None:
            summary["reconcile_matched_model_id"] = str(recon_result.matched_model_id)
        if recon_result.cosine_similarity is not None:
            summary["reconcile_cosine"] = recon_result.cosine_similarity
    if verdict is not None:
        summary["quality_decision"] = verdict.decision
        summary["quality_overall"] = verdict.overall_score
    if split_group_id is not None:
        summary["split_group_id"] = split_group_id


def _record_claim_apply_result(
    *,
    apply_result: dict[str, Any],
    original_op: ClaimOp,
    applied_op: ClaimOp,
    split_group_id: int | None,
    is_pending_situation: bool,
    group_member_ids: dict[int, list[UUID]],
    ops_summary: dict[str, Any],
    result: _ClaimOpsApplyResult,
) -> None:
    ops_summary["claim_ops"].append(apply_result["summary"])
    model_id = apply_result.get("model_id")
    if model_id is not None:
        result.applied_model_ids.append(model_id)
        if split_group_id is not None and not is_pending_situation:
            group_member_ids.setdefault(split_group_id, []).append(model_id)
        _record_pending_model_id_mappings(
            original_op=original_op,
            applied_op=applied_op,
            model_id=model_id,
            pending_model_ids_by_event_id=result.pending_model_ids_by_event_id,
        )
        if (
            applied_op.op == "insert"
            and apply_result["summary"].get("proposition_kind")
            in _T2_REEVALUATION_KINDS
        ):
            result.belief_updated_model_ids.append(model_id)
    result.state_changes_emitted += apply_result.get("state_changes", 0)


def _record_pending_model_id_mappings(
    *,
    original_op: ClaimOp,
    applied_op: ClaimOp,
    model_id: UUID,
    pending_model_ids_by_event_id: dict[UUID, UUID],
) -> None:
    if original_op.op != "insert" or not isinstance(original_op.entry, dict):
        return
    for entry_for_mapping in (
        original_op.entry,
        applied_op.entry if isinstance(applied_op.entry, dict) else None,
    ):
        if not isinstance(entry_for_mapping, dict):
            continue
        for key in ("born_from_event_id", "model_id", "id"):
            placeholder_id = _coerce_uuid(entry_for_mapping.get(key))
            if placeholder_id is not None:
                pending_model_ids_by_event_id[placeholder_id] = model_id


async def _apply_act_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    pending_model_ids_by_event_id: dict[UUID, UUID],
    trigger_cause_event_id: UUID | None,
    ops_summary: dict[str, Any],
) -> int:
    state_changes_emitted = 0
    for op in diff.act_ops:
        if op.confidence_basis in pending_model_ids_by_event_id:
            op = op.model_copy(
                update={
                    "confidence_basis": pending_model_ids_by_event_id[
                        op.confidence_basis
                    ]
                }
            )
        if op.confidence_basis is not None and not await _model_id_exists(
            conn,
            tenant_id=diff.tenant_id,
            model_id=op.confidence_basis,
        ):
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="act",
                failure_reason="missing_confidence_basis",
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="act",
                op_kind=op.op,
                reason="missing_confidence_basis",
                message=f"act_op {op.op}: confidence_basis model not found",
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(
                f"act_op {op.op}: confidence_basis model not found"
            )
            ops_summary["act_ops"].append(
                {
                    "op": "skip",
                    "act_op": op.op,
                    "reason": "missing_confidence_basis",
                    "confidence_basis": str(op.confidence_basis),
                }
            )
            continue
        try:
            result = await _apply_act_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
            )
        except (InvariantViolation, ValidationError) as exc:
            reason = _classify_apply_act_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="act",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="act",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["act_ops"].append(
                {
                    "op": "skip",
                    "act_op": op.op,
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["act_ops"].append(result["summary"])
        state_changes_emitted += result.get("state_changes", 0)
    return state_changes_emitted


async def _apply_resource_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    trigger_cause_event_id: UUID | None,
    ops_summary: dict[str, Any],
) -> int:
    state_changes_emitted = 0
    for op in diff.resource_ops:
        try:
            result = await _apply_resource_op(
                op,
                conn,
                diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
            )
        except ValidationError as exc:
            reason = _classify_apply_resource_drop_reason(exc)
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="resource",
                failure_reason=reason,
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="resource",
                op_kind=op.op,
                reason=reason,
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["resource_ops"].append(
                {
                    "op": "skip",
                    "resource_op": op.op,
                    "reason": reason,
                    "message": message,
                }
            )
            continue
        ops_summary["resource_ops"].append(result["summary"])
        state_changes_emitted += result.get("state_changes", 0)
    return state_changes_emitted


def _bounded_confidence(value: float) -> float:
    return min(0.95, max(0.05, float(value)))


def _lifecycle_confidence(
    op: MemoryLifecycleOp,
    *,
    current_confidence: float,
) -> float | None:
    if op.confidence is not None:
        return _bounded_confidence(float(op.confidence))
    if op.confidence_delta is not None:
        return _bounded_confidence(current_confidence + float(op.confidence_delta))
    if op.action == "confirm":
        return _bounded_confidence(current_confidence + 0.05)
    if op.action == "falsify":
        return _bounded_confidence(current_confidence - 0.25)
    return None


def _lifecycle_resolution_outcome(op: MemoryLifecycleOp) -> bool | None:
    if op.action == "confirm":
        return True if op.resolution_outcome is None else bool(op.resolution_outcome)
    if op.action == "falsify":
        return False if op.resolution_outcome is None else bool(op.resolution_outcome)
    return op.resolution_outcome


async def _compile_memory_lifecycle_update(
    op: MemoryLifecycleOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    now: datetime,
) -> ClaimOp:
    row = await conn.fetchrow(
        """
        SELECT confidence, confirmed_count, contested_count,
               supporting_model_ids, proposition_kind, claim_role
        FROM models
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        op.model_id,
    )
    if row is None:
        raise ValidationError("memory_lifecycle_op target model not found")

    current_confidence = float(row["confidence"] or 0.5)
    confirmed_count = int(row["confirmed_count"] or 0)
    contested_count = int(row["contested_count"] or 0)
    supporting_model_ids = _merge_event_ids(
        row["supporting_model_ids"],
        op.evidence_model_ids,
    )
    changes: dict[str, Any] = {
        "supporting_event_ids": op.evidence_event_ids,
        "supporting_model_ids": supporting_model_ids,
    }
    confidence = _lifecycle_confidence(op, current_confidence=current_confidence)
    if confidence is not None:
        changes["confidence"] = confidence

    if op.action in {"confirm", "unchanged"}:
        changes["confirmed_count"] = confirmed_count + 1
        changes["last_confirmed_at"] = now
    if op.action == "revise":
        if confidence is not None and confidence < current_confidence:
            changes["contested_count"] = contested_count + 1
        elif confidence is not None and confidence > current_confidence:
            changes["confirmed_count"] = confirmed_count + 1
            changes["last_confirmed_at"] = now
    if op.action == "falsify":
        changes["contested_count"] = contested_count + 1
        changes["resolved_at"] = now

    outcome = _lifecycle_resolution_outcome(op)
    is_prediction = (
        row["proposition_kind"] == "prediction"
        or row["claim_role"] == "prediction"
    )
    if outcome is not None and (is_prediction or op.action == "falsify"):
        changes["resolution_outcome"] = outcome
        changes.setdefault("resolved_at", now)

    return ClaimOp(op="update", model_id=op.model_id, changes=changes)


async def _apply_memory_lifecycle_ops_for_diff(
    *,
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    trigger_cause_event_id: UUID | None,
    trigger_evidence_ids: list[UUID],
    ops_summary: dict[str, Any],
) -> tuple[list[UUID], int]:
    applied_model_ids: list[UUID] = []
    state_changes_emitted = 0
    now = datetime.now(timezone.utc)
    for op in diff.memory_lifecycle_ops:
        try:
            if op.action in {"archive", "supersede"}:
                claim_op = ClaimOp(
                    op="archive",
                    model_id=op.model_id,
                    reason=op.reason or (
                        "superseded" if op.action == "supersede" else "decay"
                    ),
                )
                apply_result = await _apply_claim_archive(
                    claim_op,
                    conn,
                    models_repo,
                    cause_event_id=trigger_cause_event_id,
                )
            else:
                claim_op = await _compile_memory_lifecycle_update(
                    op,
                    conn,
                    tenant_id=diff.tenant_id,
                    now=now,
                )
                apply_result = await _apply_claim_update(
                    claim_op,
                    conn,
                    models_repo,
                    diff.tenant_id,
                    cause_event_id=trigger_cause_event_id,
                    trigger_supporting_event_ids=trigger_evidence_ids,
                    audit_cause_override=None,
                )
        except ValidationError as exc:
            message = getattr(exc, "message", str(exc))
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.action,
                op_type="memory_lifecycle",
                failure_reason="apply_validation_error",
                original_op=op,
            )
            await _record_apply_drop(
                conn,
                tenant_id=diff.tenant_id,
                op_type="memory_lifecycle",
                op_kind=op.action,
                reason="apply_validation_error",
                message=message,
            )
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["memory_lifecycle_ops"].append(
                {
                    "op": "skip",
                    "action": op.action,
                    "model_id": str(op.model_id),
                    "reason": "apply_validation_error",
                    "message": message,
                }
            )
            continue

        summary = {
            "op": "reconcile",
            "action": op.action,
            "model_id": str(op.model_id),
            "rationale": op.rationale,
            "evidence_event_ids": [
                str(event_id) for event_id in _merge_event_ids(op.evidence_event_ids)
            ],
            "evidence_model_ids": [
                str(model_id) for model_id in _merge_event_ids(op.evidence_model_ids)
            ],
            "compiled_op": apply_result["summary"].get("op"),
            "changed": apply_result["summary"].get("changed", []),
            "archive_reason": apply_result["summary"].get("reason"),
            "resolution_outcome": _lifecycle_resolution_outcome(op),
            "superseded_by_model_id": (
                str(op.superseded_by_model_id)
                if op.superseded_by_model_id is not None
                else None
            ),
        }
        ops_summary["memory_lifecycle_ops"].append(summary)
        if apply_result.get("model_id") is not None:
            applied_model_ids.append(apply_result["model_id"])
        state_changes_emitted += int(apply_result.get("state_changes", 0))
    return applied_model_ids, state_changes_emitted


async def apply_diff(
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    trigger_kind: str,
    trigger_cause_event_id: UUID | None = None,
    *,
    models_repo: ModelsRepo | None = None,
    think_run_id: UUID | None = None,
    parent_cascade_payload: dict[str, Any] | None = None,
    trigger_supporting_event_ids: list[UUID] | tuple[UUID, ...] | None = None,
) -> dict[str, Any]:
    """
    Apply a ValidatedDiff inside `conn`'s transaction. The caller MUST
    have opened the transaction (typically via `async with
    conn.transaction():`).

    Region lock: acquired here, derived from the diff itself. Two diffs
    that touch the same (tenant, scope) tuple serialize on the same
    advisory lock. Re-entrant within a transaction, so the reason.py
    path (which also acquires a broader retrieval-region lock) is
    unaffected.

    Returns a summary dict used for observability:
      { "claim_ops": N, "act_ops": N, "resource_ops": N,
        "applied_model_ids": [...], "state_changes_emitted": N,
        "diff_hash": "..." }

    Idempotency: atomically inserts into applied_triggers with
    outcome='pending' FIRST. Raises AlreadyAppliedError if the trigger_id
    already has a row — the caller handles that path.
    """
    diff_hash = await _prepare_apply_transaction(diff, conn, trigger_kind)

    applied_model_ids: list[UUID] = []
    state_changes_emitted = 0
    ops_summary: dict[str, Any] = {
        "claim_ops": [],
        "memory_lifecycle_ops": [],
        "relation_claim_ops": [],
        "relation_frame_ops": [],
        "edge_ops": [],
        "ontology_gap_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "synthesis_decisions": summarize_synthesis_decisions(diff),
        "diff_hash": diff_hash,
        "apply_dropped_op_count": 0,
        "apply_dropped_op_errors": [],
    }
    pending_model_ids_by_event_id: dict[UUID, UUID] = {}
    trigger_evidence_ids = _merge_event_ids(
        trigger_supporting_event_ids or (),
        (trigger_cause_event_id,) if trigger_cause_event_id is not None else (),
    )
    # Document-memory provenance (Option A, §4.4): when the trigger that drove
    # this apply carried a structured document summary, every Model minted here
    # is document-derived — count each successful insert at the real mint site
    # (`_apply_claim_insert`), keyed by the document's source channel.
    doc_memory_source = _doc_memory_source_from_cascade(parent_cascade_payload)

    if models_repo is None:
        models_repo = ModelsRepo(  # type: ignore[arg-type]
            pool=None,
            run_topology_on_insert=False,
        )

    # --- 1. claim_ops ---------------------------------------------
    claim_result = await _apply_claim_ops_for_diff(
        diff=diff,
        conn=conn,
        models_repo=models_repo,
        trigger_cause_event_id=trigger_cause_event_id,
        trigger_evidence_ids=trigger_evidence_ids,
        think_run_id=think_run_id,
        ops_summary=ops_summary,
        doc_memory_source=doc_memory_source,
    )
    applied_model_ids = claim_result.applied_model_ids
    pending_model_ids_by_event_id = claim_result.pending_model_ids_by_event_id
    state_changes_emitted = claim_result.state_changes_emitted

    # --- 2. memory_lifecycle_ops ----------------------------------
    lifecycle_model_ids, lifecycle_state_changes = await _apply_memory_lifecycle_ops_for_diff(
        diff=diff,
        conn=conn,
        models_repo=models_repo,
        trigger_cause_event_id=trigger_cause_event_id,
        trigger_evidence_ids=trigger_evidence_ids,
        ops_summary=ops_summary,
    )
    applied_model_ids.extend(lifecycle_model_ids)
    state_changes_emitted += lifecycle_state_changes

    # --- 3. relation_claim_ops ------------------------------------
    applied_relation_claim_ops = await _apply_relation_claim_ops_for_diff(
        diff=diff,
        conn=conn,
        pending_model_ids_by_event_id=pending_model_ids_by_event_id,
        trigger_cause_event_id=trigger_cause_event_id,
        think_run_id=think_run_id,
        ops_summary=ops_summary,
    )

    # --- 4. relation_frame_ops ------------------------------------
    applied_relation_frame_ops = await _apply_relation_frame_ops_for_diff(
        diff=diff,
        conn=conn,
        pending_model_ids_by_event_id=pending_model_ids_by_event_id,
        trigger_cause_event_id=trigger_cause_event_id,
        think_run_id=think_run_id,
        ops_summary=ops_summary,
    )

    # --- 5. edge_ops ----------------------------------------------
    applied_edge_ops = await _apply_edge_ops_for_diff(
        diff=diff,
        conn=conn,
        pending_model_ids_by_event_id=pending_model_ids_by_event_id,
        trigger_cause_event_id=trigger_cause_event_id,
        ops_summary=ops_summary,
    )

    # --- 6. ontology_gap_ops --------------------------------------
    applied_ontology_gap_ops = await _apply_ontology_gap_ops_for_diff(
        diff=diff,
        conn=conn,
        pending_model_ids_by_event_id=pending_model_ids_by_event_id,
        trigger_cause_event_id=trigger_cause_event_id,
        ops_summary=ops_summary,
    )

    # --- 7. act_ops -----------------------------------------------
    state_changes_emitted += await _apply_act_ops_for_diff(
        diff=diff,
        conn=conn,
        pending_model_ids_by_event_id=pending_model_ids_by_event_id,
        trigger_cause_event_id=trigger_cause_event_id,
        ops_summary=ops_summary,
    )

    # --- 8. resource_ops ------------------------------------------
    state_changes_emitted += await _apply_resource_ops_for_diff(
        diff=diff,
        conn=conn,
        trigger_cause_event_id=trigger_cause_event_id,
        ops_summary=ops_summary,
    )

    # --- 9. Enqueue T2:belief_updated for each new state/concern model ----
    await _enqueue_belief_updated_for_applied_models(
        conn=conn,
        tenant_id=diff.tenant_id,
        model_ids=claim_result.belief_updated_model_ids,
        source_observation_id=trigger_cause_event_id,
        parent_payload=parent_cascade_payload,
    )

    # --- 10. Mark applied_triggers success (still in same tx) ------
    ops_summary["memory_aggregation"] = _summarize_memory_aggregation(
        ops_summary,
        original_claim_op_count=len(diff.claim_ops),
        expanded_claim_op_count=claim_result.expanded_claim_op_count,
    )

    await conn.execute(
        "UPDATE applied_triggers SET outcome = 'success' WHERE trigger_id = $1",
        diff.trigger_ref,
    )

    # --- 11. Phase 1 outcome events -------------------------------
    # The apply succeeded (we just updated applied_triggers to
    # 'success'), so every model_id referenced by the validated diff
    # got "used in a valid diff" for the topology optimizer's
    # bookkeeping. We also emit one event per pair of model_ids that
    # the diff connected via an existing model_edges edge. Both emits
    # are best-effort and require an active TraceContext (installed by
    # the inquiry runtime); when none is set they are no-ops.
    feedback_diff = diff.model_copy(
        update={
            "edge_ops": applied_edge_ops,
            "memory_lifecycle_ops": diff.memory_lifecycle_ops,
            "relation_claim_ops": applied_relation_claim_ops,
            "relation_frame_ops": applied_relation_frame_ops,
            "ontology_gap_ops": applied_ontology_gap_ops,
        }
    )
    await _emit_valid_diff_outcome_events(
        feedback_diff,
        applied_model_ids=applied_model_ids,
        conn=conn,
        ops_summary=ops_summary,
        source_observation_id=trigger_cause_event_id,
    )

    return {
        **ops_summary,
        "applied_model_ids": applied_model_ids,
        "state_changes_emitted": state_changes_emitted,
        "reasoning_trace": diff.reasoning_trace,
    }


async def _emit_valid_diff_outcome_events(
    diff: ValidatedDiff,
    *,
    applied_model_ids: list[UUID],
    conn: asyncpg.Connection,
    ops_summary: dict[str, Any] | None = None,
    source_observation_id: UUID | None = None,
) -> None:
    """Emit `node_used_in_valid_diff` + `path_used_in_valid_diff`.

    Called after a ValidatedDiff applies successfully. Pure
    best-effort: every call wrapped so a Sage trace hiccup never
    rolls back the apply transaction.

    Node events: one per distinct model_id touched by the diff
    (insert results + claim_op model_ids + edge endpoints).

    Path events: one per pair of model_ids connected by an existing
    `model_edges` row. We batch the existence check in a single SQL
    query so a 20-node diff costs ~1 query, not 20*19.
    """
    # Collect every model_id the diff touched. Include applied (insert
    # results) + update/archive targets + edge endpoints + the
    # confidence_basis on act_ops (a Model that grounded the decision).
    node_ids: set[UUID] = set()
    for mid in applied_model_ids:
        if isinstance(mid, UUID):
            node_ids.add(mid)
    for op in diff.claim_ops:
        if isinstance(getattr(op, "model_id", None), UUID):
            node_ids.add(op.model_id)
    for op in diff.memory_lifecycle_ops:
        if isinstance(getattr(op, "model_id", None), UUID):
            node_ids.add(op.model_id)
        if isinstance(getattr(op, "superseded_by_model_id", None), UUID):
            node_ids.add(op.superseded_by_model_id)
        for model_id in getattr(op, "evidence_model_ids", None) or []:
            if isinstance(model_id, UUID):
                node_ids.add(model_id)
    for op in diff.edge_ops:
        if isinstance(getattr(op, "source_model_id", None), UUID):
            node_ids.add(op.source_model_id)
        if isinstance(getattr(op, "target_model_id", None), UUID):
            node_ids.add(op.target_model_id)
    for op in diff.relation_claim_ops:
        if isinstance(getattr(op, "source_model_id", None), UUID):
            node_ids.add(op.source_model_id)
        if isinstance(getattr(op, "target_model_id", None), UUID):
            node_ids.add(op.target_model_id)
        for model_id in getattr(op, "evidence_model_ids", None) or []:
            if isinstance(model_id, UUID):
                node_ids.add(model_id)
    for op in diff.relation_frame_ops:
        for participant in getattr(op, "participants", None) or []:
            model_id = getattr(participant, "model_id", None)
            if isinstance(model_id, UUID):
                node_ids.add(model_id)
        for model_id in getattr(op, "evidence_model_ids", None) or []:
            if isinstance(model_id, UUID):
                node_ids.add(model_id)
    for op in diff.ontology_gap_ops:
        if isinstance(getattr(op, "source_model_id", None), UUID):
            node_ids.add(op.source_model_id)
        if isinstance(getattr(op, "target_model_id", None), UUID):
            node_ids.add(op.target_model_id)
        for model_id in getattr(op, "evidence_model_ids", None) or []:
            if isinstance(model_id, UUID):
                node_ids.add(model_id)
    for op in diff.act_ops:
        basis = getattr(op, "confidence_basis", None)
        if isinstance(basis, UUID):
            node_ids.add(basis)

    node_ids = await _filter_existing_model_ids_for_outcome_events(
        conn,
        tenant_id=diff.tenant_id,
        model_ids=node_ids,
        applied_model_ids=applied_model_ids,
    )

    try:
        from services.reasoning.sage.inquiry_traces.emitter import (
            current_trace_context,
            emit_event,
            emission_enabled,
        )
    except Exception:  # noqa: BLE001
        await _record_edge_intelligence_valid_diff(
            diff,
            node_ids=node_ids,
            conn=conn,
            primitive=None,
            source_observation_id=source_observation_id,
        )
        return

    ctx = current_trace_context()
    ctx_meta = dict(getattr(ctx, "metadata", {}) or {}) if ctx is not None else {}
    primitives = [
        str(p) for p in (ctx_meta.get("question_primitives") or []) if p is not None
    ]
    default_primitive = primitives[0] if primitives else None

    await _record_edge_intelligence_valid_diff(
        diff,
        node_ids=node_ids,
        conn=conn,
        primitive=default_primitive,
        source_observation_id=source_observation_id,
    )

    if not emission_enabled() or ctx is None:
        return

    entities = [
        str(e) for e in (ctx_meta.get("entities") or []) if e is not None and str(e)
    ]
    signal_type = ctx_meta.get("signal_type") or ctx_meta.get("trigger_kind")

    await _emit_question_policy_valid_diff_feedback(
        diff,
        conn=conn,
        ctx=ctx,
        ops_summary=ops_summary or {},
        emit_event=emit_event,
        signal_type=str(signal_type or "unknown"),
        question_primitive=default_primitive,
        entities=entities,
    )

    for mid in sorted(node_ids, key=str):
        try:
            await emit_event(
                "node_used_in_valid_diff",
                {
                    "model_id": str(mid),
                    "signal_type": signal_type,
                    "entities": entities,
                    "question_primitive": default_primitive,
                    "signature": {
                        k: v
                        for k, v in {
                            "signal_type": signal_type,
                            "entities": entities,
                            "question_primitive": default_primitive,
                        }.items()
                        if v
                    },
                },
                ctx=ctx,
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            _raise_if_postgres_error(exc)
            import structlog

            structlog.get_logger(__name__).warning(
                "sage_trace.node_event_failed",
                model_id=str(mid),
                error=str(exc),
            )

    # Detect connected pairs by querying model_edges once with the full
    # node set on each side. Symmetric kinds are stored as two rows in
    # 0031, so we still see both directions if the diff hit either end.
    if len(node_ids) < 2:
        return
    try:
        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = ANY($2::uuid[])
              AND target_model_id = ANY($2::uuid[])
            """,
            diff.tenant_id,
            list(node_ids),
        )
    except Exception as exc:  # noqa: BLE001 — best-effort; tolerate
        _raise_if_postgres_error(exc)
        # missing/renamed table in test DBs
        import structlog

        structlog.get_logger(__name__).warning(
            "sage_trace.path_lookup_failed",
            error=str(exc),
        )
        return

    seen_pairs: set[tuple[str, str, str]] = set()
    for r in rows:
        src = str(r["source_model_id"])
        tgt = str(r["target_model_id"])
        kind = r["edge_kind"]
        key = (src, tgt, kind)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        try:
            await emit_event(
                "path_used_in_valid_diff",
                {
                    "source_model_id": src,
                    "target_model_id": tgt,
                    "edge_kind": kind,
                    "signal_type": signal_type,
                    "entities": entities,
                    "question_primitive": default_primitive,
                    "signature": {
                        k: v
                        for k, v in {
                            "signal_type": signal_type,
                            "entities": entities,
                            "question_primitive": default_primitive,
                        }.items()
                        if v
                    },
                },
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            _raise_if_postgres_error(exc)
            import structlog

            structlog.get_logger(__name__).warning(
                "sage_trace.path_event_failed",
                source=src,
                target=tgt,
                edge_kind=kind,
                error=str(exc),
            )


_QUESTION_POLICY_FEEDBACK_TAGS = frozenset({"question_policy", "capability_probe"})


def _accepted_question_policy_probe_model_ids(
    ops_summary: dict[str, Any],
) -> list[UUID]:
    model_ids: list[UUID] = []
    seen: set[UUID] = set()
    for item in ops_summary.get("claim_ops", []) or []:
        if not isinstance(item, dict):
            continue
        tags = {str(tag) for tag in (item.get("domain_tags") or [])}
        if not _QUESTION_POLICY_FEEDBACK_TAGS <= tags:
            continue
        model_id = _coerce_uuid_or_none(item.get("model_id"))
        if model_id is None or model_id in seen:
            continue
        seen.add(model_id)
        model_ids.append(model_id)
    return model_ids


async def _emit_question_policy_valid_diff_feedback(
    diff: ValidatedDiff,
    *,
    conn: asyncpg.Connection,
    ctx: Any,
    ops_summary: dict[str, Any],
    emit_event: Any,
    signal_type: str,
    question_primitive: str | None,
    entities: list[str],
) -> None:
    """Bridge accepted question-policy probe writes into SAGE policy credit."""
    model_ids = _accepted_question_policy_probe_model_ids(ops_summary)
    if not model_ids:
        return
    primitive = str(question_primitive or "DEPENDENCY").upper()
    question_id = "capability_probe:question_policy"
    question = (
        "Would asking for the missing approval owner before writing a strong "
        "relation improve precision?"
    )
    try:
        async with conn.transaction():
            table_name = await conn.fetchval(
                "SELECT to_regclass('public.sage_reader_decision_attributions')"
            )
            if table_name is None:
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
                  TRUE, 0, 1.0,
                  $13::jsonb, $14::jsonb, $15::jsonb,
                  $16::jsonb, 1
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
                  selected = TRUE,
                  selection_rank = 0,
                  activation_score = 1.0,
                  activation_reasons = EXCLUDED.activation_reasons,
                  source_breakdown = EXCLUDED.source_breakdown,
                  retrieval_actions = EXCLUDED.retrieval_actions,
                  projected_evidence_refs = EXCLUDED.projected_evidence_refs,
                  evidence_in_packet_count = EXCLUDED.evidence_in_packet_count,
                  updated_at = now()
                """,
                [
                    (
                        uuid7(),
                        diff.tenant_id,
                        ctx.inquiry_session_id,
                        question_id,
                        primitive,
                        question,
                        0.82,
                        0.74,
                        0.05,
                        signal_type,
                        json.dumps(entities, default=str),
                        model_id,
                        json.dumps(
                            [
                                "accepted_question_policy_capability_probe",
                                "valid_diff_writer_used_probe_memory",
                            ],
                            default=str,
                        ),
                        json.dumps(
                            {
                                "capability_probe": 1.0,
                                "writer_valid_diff": 1.0,
                            },
                            default=str,
                        ),
                        json.dumps(
                            [
                                {
                                    "kind": "capability_probe",
                                    "question_id": question_id,
                                    "question_primitive": primitive,
                                }
                            ],
                            default=str,
                        ),
                        json.dumps(
                            [
                                {
                                    "source_type": "model",
                                    "source_ref_id": str(model_id),
                                    "reason": "question_policy_probe_memory_applied",
                                }
                            ],
                            default=str,
                        ),
                    )
                    for model_id in model_ids
                ],
            )
    except Exception as exc:  # noqa: BLE001 — feedback bridge is best-effort
        _raise_if_postgres_error(exc)
        import structlog

        structlog.get_logger(__name__).warning(
            "sage_trace.question_policy_feedback_attribution_failed",
            error=str(exc),
        )
        return

    for model_id in model_ids:
        try:
            await emit_event(
                "reader_decision_used_in_valid_diff",
                {
                    "question_id": question_id,
                    "question_primitive": primitive,
                    "signal_type": signal_type,
                    "model_id": str(model_id),
                    "entities": entities,
                    "credit_score": 0.7,
                    "source": "accepted_question_policy_capability_probe",
                    "signature": {
                        k: v
                        for k, v in {
                            "signal_type": signal_type,
                            "entities": entities,
                            "question_primitive": primitive,
                        }.items()
                        if v
                    },
                },
                ctx=ctx,
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            _raise_if_postgres_error(exc)
            import structlog

            structlog.get_logger(__name__).warning(
                "sage_trace.question_policy_feedback_event_failed",
                model_id=str(model_id),
                error=str(exc),
            )


async def _filter_existing_model_ids_for_outcome_events(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: set[UUID],
    applied_model_ids: list[UUID],
) -> set[UUID]:
    if not model_ids:
        return set()
    try:
        rows = await conn.fetch(
            """
            SELECT id
            FROM models
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            sorted(model_ids, key=str),
        )
    except Exception as exc:  # noqa: BLE001 — outcome emission is best-effort
        _raise_if_postgres_error(exc)
        import structlog

        structlog.get_logger(__name__).warning(
            "sage_trace.model_id_filter_failed",
            error=str(exc),
        )
        return {mid for mid in applied_model_ids if isinstance(mid, UUID)}
    existing = {row["id"] for row in rows}
    return {mid for mid in model_ids if mid in existing}


async def _record_edge_intelligence_valid_diff(
    diff: ValidatedDiff,
    *,
    node_ids: set[UUID],
    conn: asyncpg.Connection,
    primitive: str | None,
    source_observation_id: UUID | None,
) -> None:
    """Record pair-level edge-learning signals after a successful diff."""
    if (
        len(node_ids) < 2
        and not diff.edge_ops
        and not diff.relation_claim_ops
        and source_observation_id is None
    ):
        return
    try:
        from lib.shared.edge_registry import EDGE_REGISTRY
        from services.reasoning.edge_intelligence import (
            EdgeIntelligenceRepo,
            PairEvidenceObservation,
            RelationEvidence,
            extract_relation_evidence,
        )
    except Exception:  # noqa: BLE001
        return

    repo = EdgeIntelligenceRepo()
    pair_primitive = primitive or "UNKNOWN"
    bounded_nodes = sorted(node_ids, key=str)[:24]

    try:
        async with conn.transaction():
            if source_observation_id is not None:
                content = await conn.fetchval(
                    """
                    SELECT content_text
                    FROM observations
                    WHERE tenant_id = $1
                      AND id = $2
                    """,
                    diff.tenant_id,
                    source_observation_id,
                )
                for relation in extract_relation_evidence(str(content or "")):
                    await repo.insert_relation_evidence(
                        conn,
                        RelationEvidence(
                            tenant_id=diff.tenant_id,
                            source_observation_id=source_observation_id,
                            subject_ref={"text": relation.subject_text},
                            object_ref={"text": relation.object_text},
                            predicate=relation.predicate,
                            edge_kind_hint=relation.edge_kind_hint,
                            evidence_text=relation.evidence_text,
                            confidence=relation.confidence,
                            extraction_method="deterministic_signal_relation",
                            metadata={"trigger_ref": str(diff.trigger_ref)},
                        ),
                    )

            for op in diff.edge_ops:
                if op.op != "add":
                    continue
                spec = EDGE_REGISTRY.get(op.edge_kind)
                direction = (
                    "source_to_target"
                    if spec is None or spec.is_directed
                    else "symmetric"
                )
                await repo.insert_relation_evidence(
                    conn,
                    RelationEvidence(
                        tenant_id=diff.tenant_id,
                        source_observation_id=(
                            op.evidence_event_ids[0]
                            if op.evidence_event_ids
                            else None
                        ),
                        source_model_id=op.source_model_id,
                        target_model_id=op.target_model_id,
                        predicate=op.edge_kind,
                        edge_kind_hint=op.edge_kind,
                        direction=direction,
                        evidence_text=op.explanation,
                        confidence=op.confidence,
                        extraction_method="think_edge_op",
                        metadata={
                            "trigger_ref": str(diff.trigger_ref),
                            "review_status": op.review_status,
                            "detected_by": op.detected_by,
                        },
                    ),
                )
                await repo.record_pair_observation(
                    conn,
                    PairEvidenceObservation(
                        tenant_id=diff.tenant_id,
                        left_model_id=op.source_model_id,
                        right_model_id=op.target_model_id,
                        primitive=pair_primitive,
                        explicit_relation_delta=1,
                        think_edge_op_delta=1,
                        directed_source_model_id=op.source_model_id,
                        directed_target_model_id=op.target_model_id,
                        edge_kind_hint=op.edge_kind,
                        metadata={"trigger_ref": str(diff.trigger_ref)},
                    ),
                )

            for op in diff.relation_claim_ops:
                if op.source_model_id is None or op.target_model_id is None:
                    continue
                direction = (
                    "source_to_target"
                    if op.direction == "unknown"
                    else op.direction
                )
                await repo.insert_relation_evidence(
                    conn,
                    RelationEvidence(
                        tenant_id=diff.tenant_id,
                        source_observation_id=(
                            op.evidence_event_ids[0]
                            if op.evidence_event_ids
                            else source_observation_id
                        ),
                        source_model_id=op.source_model_id,
                        target_model_id=op.target_model_id,
                        predicate=op.predicate,
                        edge_kind_hint=op.edge_kind,
                        direction=direction,
                        evidence_text=op.explanation or op.evidence_text,
                        confidence=op.confidence,
                        extraction_method="relation_claim_op",
                        metadata={
                            "trigger_ref": str(diff.trigger_ref),
                            "write_policy": op.write_policy,
                            "status": op.status,
                        },
                    ),
                )
                await repo.record_pair_observation(
                    conn,
                    PairEvidenceObservation(
                        tenant_id=diff.tenant_id,
                        left_model_id=op.source_model_id,
                        right_model_id=op.target_model_id,
                        primitive=pair_primitive,
                        explicit_relation_delta=1,
                        think_edge_op_delta=(
                            1 if op.write_policy == "accepted_edge" else 0
                        ),
                        directed_source_model_id=op.source_model_id,
                        directed_target_model_id=op.target_model_id,
                        edge_kind_hint=op.edge_kind,
                        metadata={"trigger_ref": str(diff.trigger_ref)},
                    ),
                )

            for op in diff.ontology_gap_ops:
                if op.source_model_id is None or op.target_model_id is None:
                    continue
                direction = (
                    "symmetric"
                    if op.directionality == "symmetric"
                    else "source_to_target"
                )
                await repo.insert_relation_evidence(
                    conn,
                    RelationEvidence(
                        tenant_id=diff.tenant_id,
                        source_observation_id=(
                            op.evidence_event_ids[0]
                            if op.evidence_event_ids
                            else source_observation_id
                        ),
                        source_model_id=op.source_model_id,
                        target_model_id=op.target_model_id,
                        predicate=op.proposed_edge_kind,
                        edge_kind_hint=op.proposed_edge_kind,
                        direction=direction,
                        evidence_text=op.relationship_summary or op.description,
                        confidence=op.confidence,
                        extraction_method="ontology_gap_op",
                        metadata={
                            "trigger_ref": str(diff.trigger_ref),
                            "proposed_edge_kind": op.proposed_edge_kind,
                            "nearest_existing_kind": op.nearest_existing_kind,
                            "parent_kind": op.parent_kind,
                        },
                    ),
                )
                await repo.record_pair_observation(
                    conn,
                    PairEvidenceObservation(
                        tenant_id=diff.tenant_id,
                        left_model_id=op.source_model_id,
                        right_model_id=op.target_model_id,
                        primitive=pair_primitive,
                        explicit_relation_delta=1,
                        directed_source_model_id=(
                            op.source_model_id
                            if direction == "source_to_target"
                            else None
                        ),
                        directed_target_model_id=(
                            op.target_model_id
                            if direction == "source_to_target"
                            else None
                        ),
                        edge_kind_hint=op.proposed_edge_kind,
                        metadata={
                            "trigger_ref": str(diff.trigger_ref),
                            "ontology_gap": {
                                "proposed_edge_kind": op.proposed_edge_kind,
                                "nearest_existing_kind": op.nearest_existing_kind,
                                "parent_kind": op.parent_kind,
                            },
                        },
                    ),
                )

            for op in diff.relation_frame_ops:
                participants_by_role: dict[str, list[UUID]] = {}
                for participant in op.participants:
                    participants_by_role.setdefault(
                        participant.role,
                        [],
                    ).append(participant.model_id)

                projection_rules = (
                    (
                        "blocker",
                        "blocked_work",
                        "blocks",
                    ),
                    (
                        "blocked_work",
                        "downstream_risk",
                        "early_warning_for",
                    ),
                    (
                        "possible_resolution",
                        "blocker",
                        "contributes_to_resolution",
                    ),
                )
                if op.relation_kind == "blocked_workstream":
                    for source_role, target_role, edge_kind in projection_rules:
                        for source_model_id in participants_by_role.get(
                            source_role,
                            [],
                        ):
                            for target_model_id in participants_by_role.get(
                                target_role,
                                [],
                            ):
                                await repo.insert_relation_evidence(
                                    conn,
                                    RelationEvidence(
                                        tenant_id=diff.tenant_id,
                                        source_observation_id=(
                                            op.evidence_event_ids[0]
                                            if op.evidence_event_ids
                                            else source_observation_id
                                        ),
                                        source_model_id=source_model_id,
                                        target_model_id=target_model_id,
                                        predicate=edge_kind,
                                        edge_kind_hint=edge_kind,
                                        direction="source_to_target",
                                        evidence_text=(
                                            op.explanation or op.evidence_text
                                        ),
                                        confidence=op.confidence,
                                        extraction_method="relation_frame_op",
                                        metadata={
                                            "trigger_ref": str(diff.trigger_ref),
                                            "relation_kind": op.relation_kind,
                                            "source_role": source_role,
                                            "target_role": target_role,
                                            "write_policy": op.write_policy,
                                            "status": op.status,
                                        },
                                    ),
                                )

                participant_model_ids = sorted(
                    set(
                        model_id
                        for values in participants_by_role.values()
                        for model_id in values
                    ),
                    key=str,
                )[:24]
                for idx, left in enumerate(participant_model_ids):
                    for right in participant_model_ids[idx + 1 :]:
                        await repo.record_pair_observation(
                            conn,
                            PairEvidenceObservation(
                                tenant_id=diff.tenant_id,
                                left_model_id=left,
                                right_model_id=right,
                                primitive=pair_primitive,
                                explicit_relation_delta=1,
                                think_edge_op_delta=(
                                    1
                                    if op.write_policy == "project_edges"
                                    and op.status == "accepted"
                                    else 0
                                ),
                                edge_kind_hint=op.relation_kind,
                                metadata={
                                    "trigger_ref": str(diff.trigger_ref),
                                    "relation_kind": op.relation_kind,
                                },
                            ),
                        )

            for idx, left in enumerate(bounded_nodes):
                for right in bounded_nodes[idx + 1 :]:
                    await repo.record_pair_observation(
                        conn,
                        PairEvidenceObservation(
                            tenant_id=diff.tenant_id,
                            left_model_id=left,
                            right_model_id=right,
                            primitive=pair_primitive,
                            co_used_valid_diff_delta=1,
                            positive_outcome_delta=1,
                            metadata={"trigger_ref": str(diff.trigger_ref)},
                        ),
                    )
    except Exception as exc:  # noqa: BLE001
        import structlog

        structlog.get_logger(__name__).warning(
            "edge_intelligence.valid_diff_record_failed",
            tenant_id=str(diff.tenant_id),
            trigger_ref=str(diff.trigger_ref),
            error=str(exc),
        )


# ---------------------------------------------------------------------
# Per-op appliers
# ---------------------------------------------------------------------


def _resolve_pending_edge_model_refs(
    op: EdgeOp,
    pending_model_ids_by_event_id: dict[UUID, UUID],
) -> EdgeOp:
    source_was_pending = op.source_model_id in pending_model_ids_by_event_id
    target_was_pending = op.target_model_id in pending_model_ids_by_event_id
    source_model_id = pending_model_ids_by_event_id.get(
        op.source_model_id,
        op.source_model_id,
    )
    target_model_id = pending_model_ids_by_event_id.get(
        op.target_model_id,
        op.target_model_id,
    )
    metadata = dict(op.metadata or {})

    # Canonical supersession direction is old -> replacement. Live LLMs
    # commonly phrase "new claim supersedes old Model" and emit
    # new -> old. When exactly one endpoint is a same-diff insert, the
    # newly inserted Model is almost always the replacement, so flip to the
    # storage/traversal direction before EdgesRepo persists it.
    if (
        op.edge_kind == "superseded_by"
        and source_was_pending
        and not target_was_pending
    ):
        source_model_id, target_model_id = target_model_id, source_model_id
        metadata.setdefault(
            "canonicalized_direction",
            "existing_model_superseded_by_same_diff_insert",
        )

    updates: dict[str, Any] = {}
    if source_model_id != op.source_model_id:
        updates["source_model_id"] = source_model_id
    if target_model_id != op.target_model_id:
        updates["target_model_id"] = target_model_id
    if metadata != (op.metadata or {}):
        updates["metadata"] = metadata
    return op.model_copy(update=updates) if updates else op


def _resolve_pending_relation_claim_model_refs(
    op: RelationClaimOp,
    pending_model_ids_by_event_id: dict[UUID, UUID],
) -> RelationClaimOp:
    updates: dict[str, Any] = {}
    if op.source_model_id in pending_model_ids_by_event_id:
        updates["source_model_id"] = pending_model_ids_by_event_id[op.source_model_id]
    if op.target_model_id in pending_model_ids_by_event_id:
        updates["target_model_id"] = pending_model_ids_by_event_id[op.target_model_id]
    if updates and "endpoint_binding_status" not in updates:
        source = updates.get("source_model_id", op.source_model_id)
        target = updates.get("target_model_id", op.target_model_id)
        if source is not None and target is not None:
            updates["endpoint_binding_status"] = "bound"
            updates["binding_confidence"] = max(float(op.binding_confidence), 0.8)
    return op.model_copy(update=updates) if updates else op


def _resolve_pending_relation_frame_model_refs(
    op: RelationFrameOp,
    pending_model_ids_by_event_id: dict[UUID, UUID],
) -> RelationFrameOp:
    participants = []
    changed = False
    for participant in op.participants:
        model_id = pending_model_ids_by_event_id.get(
            participant.model_id,
            participant.model_id,
        )
        if model_id != participant.model_id:
            changed = True
            participant = participant.model_copy(update={"model_id": model_id})
        participants.append(participant)
    if not changed:
        return op
    update: dict[str, Any] = {"participants": participants}
    if op.participant_binding_status != "bound":
        update["participant_binding_status"] = "bound"
    return op.model_copy(update=update)


def _resolve_pending_ontology_gap_model_refs(
    op: OntologyGapOp,
    pending_model_ids_by_event_id: dict[UUID, UUID],
) -> OntologyGapOp:
    updates: dict[str, Any] = {}
    source_model_id = pending_model_ids_by_event_id.get(
        op.source_model_id,
        op.source_model_id,
    )
    target_model_id = pending_model_ids_by_event_id.get(
        op.target_model_id,
        op.target_model_id,
    )
    if source_model_id != op.source_model_id:
        updates["source_model_id"] = source_model_id
    if target_model_id != op.target_model_id:
        updates["target_model_id"] = target_model_id
    evidence_model_ids = [
        pending_model_ids_by_event_id.get(model_id, model_id)
        for model_id in op.evidence_model_ids
    ]
    if evidence_model_ids != op.evidence_model_ids:
        updates["evidence_model_ids"] = evidence_model_ids
    return op.model_copy(update=updates) if updates else op


def _audit_jsonable(v: Any) -> Any:
    """Coerce a Python value into something JSON/JSONB can store."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_audit_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _audit_jsonable(x) for k, x in v.items()}
    if isinstance(v, (bytes, bytearray)):
        try:
            return json.loads(v.decode())
        except (ValueError, UnicodeDecodeError):
            return v.decode(errors="replace")
    return str(v)


def _classify_apply_act_drop_reason(exc: Exception) -> str:
    """Stable tags for domain-level act apply drops."""
    if isinstance(exc, InvariantViolation):
        return "illegal_transition"
    msg = str(getattr(exc, "message", exc)).lower()
    if "not found" in msg:
        return "missing_entity_reference"
    if "requires" in msg or "entity" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_apply_edge_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "cycle" in msg:
        return "cycle_prevention"
    if "mutually exclusive" in msg:
        return "mutually_exclusive_edge"
    if "self-edge" in msg:
        return "invalid_shape"
    if "unknown edge_kind" in msg or "reserved" in msg:
        return "invalid_edge_kind"
    if "weight" in msg:
        return "invalid_weight"
    if "confidence" in msg:
        return "invalid_confidence"
    if "detected_by" in msg or "review_status" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_apply_ontology_gap_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "self-edge" in msg:
        return "invalid_shape"
    if "constraint" in msg or "candidate" in msg:
        return "candidate_persistence_failed"
    return "unclassified"


def _classify_apply_resource_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "invalid transaction_type" in msg or "invalid kind" in msg:
        return "invalid_transaction_type"
    if "not found" in msg:
        return "missing_entity_reference"
    if "requires" in msg or "delta" in msg:
        return "invalid_shape"
    return "unclassified"


_ALLOWED_MODEL_UPDATE_COLUMNS = {
    "confidence",
    "signal_readings",
    "reading_contestable",
    "evidential_weight",
    "last_confirmed_at",
    "confirmed_count",
    "contested_count",
    "resolved_at",
    "resolution_outcome",
    "proposition",
    "domain_tags",
    "contributing_models",
    "supporting_event_ids",
    "supporting_model_ids",
}


@dataclass(slots=True)
class _ClaimUpdatePreparation:
    changes: dict[str, Any]
    changed_fields_for_summary: set[str]
    situation_merge_payload: dict[str, Any] | None = None
    resolution_update_dropped: bool = False


def _merge_event_ids(*groups: Any) -> list[UUID]:
    merged: list[UUID] = []
    seen: set[UUID] = set()
    for group in groups:
        if group is None:
            continue
        values = group if isinstance(group, (list, tuple, set)) else (group,)
        for value in values:
            uid = _coerce_uuid_or_none(value)
            if uid is None or uid in seen:
                continue
            seen.add(uid)
            merged.append(uid)
    return merged


def _merge_supporting_event_ids(*groups: Any) -> list[UUID]:
    return compact_supporting_event_ids(*groups).event_ids


async def _model_id_exists(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT 1
            FROM models
            WHERE tenant_id = $1
              AND id = $2
            """,
            tenant_id,
            model_id,
        )
    )


def _with_claim_evidence_defaults(
    op: ClaimOp,
    *,
    trigger_cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
) -> ClaimOp:
    if op.op != "insert" or not isinstance(op.entry, dict):
        return op
    entry = dict(op.entry)
    prop = dict(entry.get("proposition") or {})
    event_ids = _merge_supporting_event_ids(
        entry.get("supporting_event_ids"),
        prop.get("evidence_event_ids"),
        entry.get("born_from_event_id"),
        trigger_cause_event_id,
        trigger_supporting_event_ids,
    )
    if not event_ids:
        return op
    entry.setdefault("born_from_event_id", event_ids[0])
    entry["supporting_event_ids"] = event_ids
    if prop and (
        prop.get("claim_role") == "situation"
        or prop.get("legacy_kind") == "situation"
        or prop.get("kind") == "situation"
        or prop.get("claim_role") == "hypothesis"
    ):
        prop["evidence_event_ids"] = [
            str(uid)
            for uid in _merge_supporting_event_ids(
                prop.get("evidence_event_ids"),
                event_ids,
            )
        ]
        entry["proposition"] = prop
    return op.model_copy(update={"entry": entry})


def _coerce_update_value(column: str, value: Any) -> Any:
    """Coerce an LLM-provided model-update value to the column's type.

    The LLM sees timestamps as ISO strings in its context and echoes
    them back (booleans/ints sometimes arrive stringified too); asyncpg
    rejects mistyped parameters with a DataError that fails the whole
    run. Coerce the known column set instead, raising ValidationError
    on garbage so a bad op fails with a contract error, not a driver
    error.
    """
    if value is None:
        return None
    if column in ("last_confirmed_at", "resolved_at"):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValidationError(
                    f"apply_claim_op update: {column} is not an ISO "
                    f"timestamp: {value!r}"
                ) from exc
        raise ValidationError(
            f"apply_claim_op update: {column} must be a timestamp; "
            f"got {type(value).__name__}"
        )
    if column in ("reading_contestable", "resolution_outcome"):
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise ValidationError(
            f"apply_claim_op update: {column} must be a boolean; got {value!r}"
        )
    if column in ("confirmed_count", "contested_count"):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"apply_claim_op update: {column} must be an integer; " f"got {value!r}"
            ) from exc
    if column == "evidential_weight":
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"apply_claim_op update: {column} must be a number; " f"got {value!r}"
            ) from exc
    if column == "proposition":
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValidationError(
                    "apply_claim_op update: proposition must be a JSON object"
                ) from exc
            if isinstance(decoded, dict):
                return decoded
        raise ValidationError(
            f"apply_claim_op update: proposition must be a dict; got {value!r}"
        )
    if column == "domain_tags":
        values = value if isinstance(value, (list, tuple, set)) else (value,)
        tags = [str(tag).strip() for tag in values if str(tag).strip()]
        return tags
    return value


_EVIDENCE_TOKEN_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "because",
    "been",
    "but",
    "call",
    "case",
    "customer",
    "from",
    "has",
    "have",
    "into",
    "now",
    "that",
    "the",
    "their",
    "this",
    "with",
    "without",
}


async def _apply_evidence_downgrade(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    verdict: QualityVerdict,
    preferred_model_id: UUID | None = None,
) -> dict[str, Any]:
    """Attach a non-durable claim as evidence to an existing memory anchor.

    Low-durability signals are often real information but bad durable Models:
    one meeting felt rough, one Slack reply sounded uncertain, one customer
    repeated a known worry. The right substrate move is evidence attachment
    when an existing Model already has enough gravity, otherwise leave the
    original Observation as the durable record and avoid creating memory mass.
    """
    entry = dict(op.entry or {})
    source_event_ids = _merge_event_ids(
        entry.get("supporting_event_ids"),
        entry.get("born_from_event_id"),
        cause_event_id,
        trigger_supporting_event_ids,
    )
    source_event_id = source_event_ids[0] if source_event_ids else None
    anchor_id = await _select_evidence_anchor_model(
        conn,
        tenant_id=tenant_id,
        entry=entry,
        preferred_model_id=preferred_model_id,
    )
    if anchor_id is None:
        return {
            "summary": {
                "op": "skip",
                "decision": "downgrade_to_evidence_skipped_no_anchor",
                "reason": "quality_gate_downgrade_to_evidence",
                "detail": "no_suitable_model_anchor",
                "quality_verdict": _quality_verdict_summary(verdict),
            },
            "model_id": None,
            "state_changes": 0,
        }

    result = await _append_observe_reading(
        conn,
        tenant_id=tenant_id,
        model_id=anchor_id,
        source_event_id=source_event_id,
        supporting_event_ids=source_event_ids,
        entry=entry,
        verdict=verdict,
    )
    return result


async def _select_evidence_anchor_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    entry: dict[str, Any],
    preferred_model_id: UUID | None,
) -> UUID | None:
    if preferred_model_id is not None:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM models
            WHERE tenant_id = $1 AND id = $2 AND status = 'active'
            """,
            tenant_id,
            preferred_model_id,
        )
        if row is not None:
            return row["id"]

    scope_actors = [
        uid
        for raw in (entry.get("scope_actors") or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    ]
    scope_entities = [
        ent
        for ent in (entry.get("scope_entities") or [])
        if isinstance(ent, dict) and ent.get("id") is not None
    ]
    if not scope_actors and not scope_entities:
        return None

    clauses: list[str] = []
    params: list[Any] = [tenant_id]
    if scope_actors:
        params.append(scope_actors)
        clauses.append(f"scope_actors && ${len(params)}::uuid[]")
    for ent in scope_entities:
        params.append(json.dumps([ent], sort_keys=True, default=str))
        clauses.append(f"scope_entities @> ${len(params)}::jsonb")

    rows = await conn.fetch(
        f"""
        SELECT id, proposition, "natural", confidence, claim_role,
               abstraction_level, polarity, domain_tags
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND ({' OR '.join(clauses)})
        ORDER BY created_at DESC
        LIMIT 40
        """,
        *params,
    )
    if not rows:
        return None

    prop = _canonical_prop_for_entry(entry)
    grammar = derive_memory_grammar(
        prop,
        natural=str(entry.get("natural") or ""),
        scope_entities=scope_entities,
    )
    text = _evidence_entry_text(entry)
    best_id: UUID | None = None
    best_score = 0.0
    for row in rows:
        row_prop = _json_obj(row["proposition"])
        row_text = " ".join(
            part
            for part in (
                str(row["natural"] or ""),
                json.dumps(row_prop, sort_keys=True, default=str),
            )
            if part
        )
        lexical = _token_overlap_score(text, row_text)
        score = lexical * 0.65
        if row["claim_role"] == grammar.claim_role:
            score += 0.18
        if row["polarity"] == grammar.polarity:
            score += 0.08
        row_tags = {str(tag) for tag in (row["domain_tags"] or [])}
        grammar_tags = set(grammar.domain_tags)
        if row_tags and grammar_tags and row_tags & grammar_tags:
            score += 0.12
        if row["abstraction_level"] in {"composite", "pattern"}:
            score -= 0.05
        score += min(0.05, max(0.0, float(row["confidence"] or 0.0)) * 0.05)

        # Require at least some textual overlap. Scope alone is too blunt:
        # all memory for one customer/commitment would otherwise attract every
        # throwaway comment about that customer.
        if lexical < 0.08:
            continue
        if score > best_score:
            best_score = score
            best_id = row["id"]

    if best_id is None or best_score < 0.22:
        return None
    return best_id


async def _append_observe_reading(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    source_event_id: UUID | None,
    supporting_event_ids: list[UUID],
    entry: dict[str, Any],
    verdict: QualityVerdict,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT signal_readings, supporting_event_ids, evidential_weight
        FROM models
        WHERE tenant_id = $1 AND id = $2 AND status = 'active'
        FOR UPDATE
        """,
        tenant_id,
        model_id,
    )
    if row is None:
        return {
            "summary": {
                "op": "downgrade_to_evidence",
                "decision": "skipped_missing_anchor",
                "model_id": str(model_id),
                "quality_verdict": _quality_verdict_summary(verdict),
            },
            "model_id": None,
            "state_changes": 0,
        }

    now = datetime.now(timezone.utc)
    text = _evidence_entry_text(entry)
    reading = {
        "kind": "observe",
        "at": now.isoformat(),
        "source_event_id": str(source_event_id) if source_event_id else None,
        "confidence": float(
            entry.get("confidence", entry.get("confidence_at_assertion", 0.5)) or 0.5
        ),
        "natural": text[:500],
        "quality_decision": verdict.decision,
    }
    existing_readings = _json_list(row["signal_readings"])
    existing_readings.append(reading)

    merged_supporting_event_ids = _merge_supporting_event_ids(
        row["supporting_event_ids"],
        supporting_event_ids,
    )

    previous_state = {
        "signal_readings": _audit_jsonable(row["signal_readings"]),
        "supporting_event_ids": _audit_jsonable(row["supporting_event_ids"]),
        "evidential_weight": _audit_jsonable(row["evidential_weight"]),
    }
    evidential_weight = min(1.0, float(row["evidential_weight"] or 0.5) + 0.02)
    await conn.execute(
        """
        UPDATE models
        SET signal_readings = $3::jsonb,
            supporting_event_ids = $4::uuid[],
            evidential_weight = $5
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        model_id,
        json.dumps(existing_readings, default=str),
        merged_supporting_event_ids,
        evidential_weight,
    )

    detail = {
        "downgraded_from": "claim_op.insert",
        "natural": text[:1000],
        "proposition": _audit_jsonable(entry.get("proposition") or {}),
        "quality_verdict": _quality_verdict_summary(verdict),
    }
    await conn.execute(
        """
        INSERT INTO model_signal_readings (
            id, model_id, tenant_id, reading_kind,
            observed_at, source_event_id, detail
        ) VALUES (
            $1, $2, $3, 'observe',
            $4, $5, $6::jsonb
        )
        """,
        uuid7(),
        model_id,
        tenant_id,
        now,
        source_event_id,
        json.dumps(detail, default=str),
    )

    await emit_state_change(
        conn,
        kind="model_evidence_attached",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=source_event_id,
        entity_kind="model",
        metadata={"reading_kind": "observe", "quality_decision": verdict.decision},
    )

    from .audit import CAUSE_FIELD_UPDATE, emit_audit_event

    await emit_audit_event(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        cause_type=CAUSE_FIELD_UPDATE,
        new_state={
            "signal_readings": existing_readings,
            "supporting_event_ids": [str(uid) for uid in merged_supporting_event_ids],
            "evidential_weight": evidential_weight,
        },
        previous_state=previous_state,
        cause_id=source_event_id,
        changed_fields=[
            "signal_readings",
            "supporting_event_ids",
            "evidential_weight",
        ],
    )

    return {
        "summary": {
            "op": "downgrade_to_evidence",
            "decision": "attached_to_existing_model",
            "model_id": str(model_id),
            "source_event_id": str(source_event_id) if source_event_id else None,
            "reading_kind": "observe",
            "quality_verdict": _quality_verdict_summary(verdict),
        },
        "model_id": model_id,
        "state_changes": 1,
    }


def _should_absorb_near_duplicate(
    op: ClaimOp,
    recon_result: Any,
    verdict: QualityVerdict | None,
) -> bool:
    if (
        op.op != "insert"
        or not isinstance(op.entry, dict)
        or recon_result is None
        or recon_result.decision != "human_review"
        or recon_result.matched_model_id is None
        or verdict is None
        or verdict.decision not in {"accept", "needs_review"}
    ):
        return False

    prop = _canonical_prop_for_entry(op.entry)
    grammar = derive_memory_grammar(
        prop,
        natural=str(op.entry.get("natural") or ""),
        scope_entities=[
            ent
            for ent in (op.entry.get("scope_entities") or [])
            if isinstance(ent, dict)
        ],
    )
    if grammar.claim_role not in {"fact", "concern", "capability"}:
        return False
    if grammar.abstraction_level != "atomic":
        return False

    breakdown = dict(getattr(recon_result, "signal_breakdown", {}) or {})
    adjusted = float(
        breakdown.get(
            "adjusted_score",
            getattr(recon_result, "cosine_similarity", 0.0) or 0.0,
        )
    )
    cosine = float(getattr(recon_result, "cosine_similarity", 0.0) or 0.0)
    has_scope = bool(op.entry.get("scope_actors") or op.entry.get("scope_entities"))
    graph_boost = float(breakdown.get("graph_boost", 0.0) or 0.0)

    if has_scope and adjusted >= 0.75:
        return True
    if graph_boost >= 0.10 and adjusted >= 0.72:
        return True
    if not has_scope and cosine >= 0.95:
        return True
    return False


async def _apply_near_duplicate_absorption(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    verdict: QualityVerdict | None,
    recon_result: Any,
) -> dict[str, Any]:
    model_id = getattr(recon_result, "matched_model_id", None)
    if model_id is None:
        return {
            "summary": {
                "op": "skip",
                "reason": "near_duplicate_absorption_missing_match",
            },
            "model_id": None,
            "state_changes": 0,
        }

    entry = dict(op.entry or {})
    source_event_ids = _merge_event_ids(
        entry.get("supporting_event_ids"),
        entry.get("born_from_event_id"),
        cause_event_id,
        trigger_supporting_event_ids,
    )
    source_event_id = source_event_ids[0] if source_event_ids else None
    row = await conn.fetchrow(
        """
        SELECT signal_readings, supporting_event_ids, evidential_weight,
               confirmed_count
        FROM models
        WHERE tenant_id = $1 AND id = $2 AND status = 'active'
        FOR UPDATE
        """,
        tenant_id,
        model_id,
    )
    if row is None:
        return {
            "summary": {
                "op": "skip",
                "reason": "near_duplicate_absorption_missing_model",
                "model_id": str(model_id),
            },
            "model_id": None,
            "state_changes": 0,
        }

    now = datetime.now(timezone.utc)
    text = _evidence_entry_text(entry)
    entry_prop = entry.get("proposition") if isinstance(entry, dict) else {}
    reading = {
        "kind": "confirm",
        "at": now.isoformat(),
        "source_event_id": str(source_event_id) if source_event_id else None,
        "confidence": float(
            entry.get("confidence", entry.get("confidence_at_assertion", 0.5)) or 0.5
        ),
        "natural": text[:500],
        "reconcile_decision": getattr(recon_result, "decision", None),
        "reconcile_cosine": getattr(recon_result, "cosine_similarity", None),
        "quality_decision": verdict.decision if verdict is not None else None,
    }
    if isinstance(entry_prop, dict):
        if isinstance(entry_prop.get("contextual_frame"), dict):
            reading["contextual_frame"] = entry_prop["contextual_frame"]
        if isinstance(entry_prop.get("retrieval_tags"), list):
            reading["retrieval_tags"] = entry_prop["retrieval_tags"][:24]
    existing_readings = _json_list(row["signal_readings"])
    existing_readings.append(reading)
    supporting_event_ids = _merge_supporting_event_ids(
        row["supporting_event_ids"],
        source_event_ids,
    )
    evidential_weight = min(1.0, float(row["evidential_weight"] or 0.5) + 0.03)
    confirmed_count = int(row["confirmed_count"] or 0) + 1

    previous_state = {
        "signal_readings": _audit_jsonable(row["signal_readings"]),
        "supporting_event_ids": _audit_jsonable(row["supporting_event_ids"]),
        "evidential_weight": _audit_jsonable(row["evidential_weight"]),
        "confirmed_count": _audit_jsonable(row["confirmed_count"]),
    }
    await conn.execute(
        """
        UPDATE models
        SET signal_readings = $3::jsonb,
            supporting_event_ids = $4::uuid[],
            evidential_weight = $5,
            confirmed_count = $6,
            last_confirmed_at = $7
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        model_id,
        json.dumps(existing_readings, default=str),
        supporting_event_ids,
        evidential_weight,
        confirmed_count,
        now,
    )

    detail = {
        "absorbed_from": "claim_op.insert",
        "natural": text[:1000],
        "proposition": _audit_jsonable(entry.get("proposition") or {}),
        "quality_verdict": (
            _quality_verdict_summary(verdict) if verdict is not None else None
        ),
        "reconcile": {
            "decision": getattr(recon_result, "decision", None),
            "cosine": getattr(recon_result, "cosine_similarity", None),
            "decision_reason": getattr(recon_result, "decision_reason", None),
            "signal_breakdown": getattr(recon_result, "signal_breakdown", {}),
        },
    }
    await conn.execute(
        """
        INSERT INTO model_signal_readings (
            id, model_id, tenant_id, reading_kind,
            observed_at, source_event_id, detail
        ) VALUES (
            $1, $2, $3, 'confirm',
            $4, $5, $6::jsonb
        )
        """,
        uuid7(),
        model_id,
        tenant_id,
        now,
        source_event_id,
        json.dumps(detail, default=str),
    )

    await emit_state_change(
        conn,
        kind="model_near_duplicate_absorbed",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=source_event_id,
        entity_kind="model",
        metadata={"reading_kind": "confirm", "source": "reconciler_human_review"},
    )

    from .audit import CAUSE_FIELD_UPDATE, emit_audit_event

    await emit_audit_event(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        cause_type=CAUSE_FIELD_UPDATE,
        new_state={
            "signal_readings": existing_readings,
            "supporting_event_ids": [str(uid) for uid in supporting_event_ids],
            "evidential_weight": evidential_weight,
            "confirmed_count": confirmed_count,
            "last_confirmed_at": now.isoformat(),
        },
        previous_state=previous_state,
        cause_id=source_event_id,
        changed_fields=[
            "confirmed_count",
            "evidential_weight",
            "last_confirmed_at",
            "signal_readings",
            "supporting_event_ids",
        ],
    )

    return {
        "summary": {
            "op": "absorb_near_duplicate",
            "decision": "attached_to_matched_model",
            "model_id": str(model_id),
            "source_event_id": str(source_event_id) if source_event_id else None,
            "reading_kind": "confirm",
            "reconcile_decision": getattr(recon_result, "decision", None),
            "reconcile_matched_model_id": str(model_id),
            "reconcile_cosine": getattr(recon_result, "cosine_similarity", None),
            "quality_decision": verdict.decision if verdict is not None else None,
            "quality_overall": (verdict.overall_score if verdict is not None else None),
        },
        "model_id": model_id,
        "state_changes": 1,
    }


def _canonical_prop_for_entry(entry: dict[str, Any]) -> dict[str, Any]:
    prop = entry.get("proposition")
    if not isinstance(prop, dict):
        return {}
    try:
        return canonicalize_proposition(prop)
    except Exception:
        return dict(prop)


def _quality_verdict_summary(verdict: QualityVerdict) -> dict[str, Any]:
    return {
        "decision": verdict.decision,
        "atomicity": verdict.atomicity_score,
        "durability": verdict.durability_score,
        "kind_fit": verdict.kind_fit_score,
        "overall": verdict.overall_score,
        "rejection_reasons": list(verdict.rejection_reasons),
    }


def _evidence_entry_text(entry: dict[str, Any]) -> str:
    prop = entry.get("proposition") or {}
    parts = [str(entry.get("natural") or "")]
    if isinstance(prop, dict):
        for key in (
            "assertion",
            "summary",
            "claim",
            "nature",
            "event",
            "assessment",
            "hypothesis_text",
            "observed_tendency",
            "situation",
            "relationship_summary",
            "expected",
        ):
            value = prop.get(key)
            if isinstance(value, str):
                parts.append(value)
            elif isinstance(value, dict):
                parts.extend(str(v) for v in value.values() if isinstance(v, str))
    return " ".join(part.strip() for part in parts if part and str(part).strip())


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return list(decoded) if isinstance(decoded, list) else []
    return []


def _token_overlap_score(left: str, right: str) -> float:
    left_tokens = _evidence_tokens(left)
    right_tokens = _evidence_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0.0
    recall = len(overlap) / len(left_tokens)
    precision = len(overlap) / len(right_tokens)
    return min(1.0, 0.7 * recall + 0.3 * min(1.0, precision * 3.0))


def _evidence_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", str(text).casefold())
        if token not in _EVIDENCE_TOKEN_STOPWORDS and not token.isdigit()
    }


def _coerce_uuid_or_none(v: Any) -> UUID | None:
    if v is None:
        return None
    if isinstance(v, UUID):
        return v
    try:
        return UUID(str(v))
    except (ValueError, TypeError):
        return None


def _summarize_memory_aggregation(
    ops_summary: dict[str, Any],
    *,
    original_claim_op_count: int,
    expanded_claim_op_count: int,
) -> dict[str, Any]:
    claim_ops = [
        item for item in ops_summary.get("claim_ops", []) if isinstance(item, dict)
    ]
    inserts = [item for item in claim_ops if item.get("op") == "insert"]
    updates = [item for item in claim_ops if item.get("op") == "update"]
    archives = [item for item in claim_ops if item.get("op") == "archive"]
    situation_updates = [
        item for item in updates if item.get("internal_situation_merge") is True
    ]
    evidence_attachments = [
        item
        for item in claim_ops
        if item.get("op") == "downgrade_to_evidence"
        and item.get("decision") == "attached_to_existing_model"
    ]
    near_duplicate_absorptions = [
        item
        for item in claim_ops
        if item.get("op") == "absorb_near_duplicate"
        and item.get("decision") == "attached_to_matched_model"
    ]
    skipped = [item for item in claim_ops if item.get("op") == "skip"]
    situations = [
        item
        for item in inserts
        if item.get("claim_role") == "situation"
        or item.get("abstraction_level") == "composite"
    ]
    atomic_inserts = [item for item in inserts if item not in situations]
    expanded = max(0, int(expanded_claim_op_count))
    non_insert_absorptions = (
        len(updates)
        + len(evidence_attachments)
        + len(near_duplicate_absorptions)
        + len(skipped)
    )
    return {
        "original_claim_ops": max(0, int(original_claim_op_count)),
        "expanded_claim_ops": expanded,
        "model_inserts": len(inserts),
        "atomic_model_inserts": len(atomic_inserts),
        "situation_model_inserts": len(situations),
        "situation_model_updates": len(situation_updates),
        "situation_member_additions": sum(
            int(item.get("situation_members_added") or 0) for item in situation_updates
        ),
        "model_updates": len(updates),
        "model_archives": len(archives),
        "evidence_attachments": len(evidence_attachments),
        "near_duplicate_absorptions": len(near_duplicate_absorptions),
        "skipped_claim_writes": len(skipped),
        "memory_lifecycle_ops": len(
            ops_summary.get("memory_lifecycle_ops") or []
        ),
        "relation_claim_ops": len(ops_summary.get("relation_claim_ops") or []),
        "edge_ops": len(ops_summary.get("edge_ops") or []),
        "ontology_gap_ops": len(ops_summary.get("ontology_gap_ops") or []),
        "act_ops": len(ops_summary.get("act_ops") or []),
        "resource_ops": len(ops_summary.get("resource_ops") or []),
        "new_model_pressure": (len(inserts) / expanded if expanded else 0.0),
        "absorption_ratio": (non_insert_absorptions / expanded if expanded else 1.0),
    }


async def _append_signal_readings_sidecar_delta(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    previous_readings: Any,
    new_readings: Any,
) -> None:
    """Mirror newly appended JSONB signal readings into the typed sidecar."""
    previous = _json_list(previous_readings)
    current = _json_list(new_readings)
    if not current:
        return

    previous_counts: dict[str, int] = {}
    for reading in previous:
        key = _reading_key(reading)
        previous_counts[key] = previous_counts.get(key, 0) + 1

    rows: list[tuple[Any, ...]] = []
    now = datetime.now(timezone.utc)
    for reading in current:
        if not isinstance(reading, dict):
            continue
        key = _reading_key(reading)
        if previous_counts.get(key, 0) > 0:
            previous_counts[key] -= 1
            continue
        rows.append(
            (
                uuid7(),
                model_id,
                tenant_id,
                _normalized_reading_kind(reading.get("kind")),
                _coerce_dt(reading.get("at")) or now,
                _coerce_uuid_or_none(reading.get("source_event_id")),
                json.dumps(
                    {
                        "source": "signal_readings_jsonb_sync",
                        "reading": _audit_jsonable(reading),
                    },
                    default=str,
                ),
            )
        )
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO model_signal_readings (
            id, model_id, tenant_id, reading_kind,
            observed_at, source_event_id, detail
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        rows,
    )


def _reading_key(reading: Any) -> str:
    return json.dumps(_audit_jsonable(reading), sort_keys=True, default=str)


def _normalized_reading_kind(value: Any) -> str:
    kind = str(value or "").strip().casefold()
    if kind in {"confirm", "contest", "observe", "falsify"}:
        return kind
    return "observe"


async def _apply_situation_merge_payload(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    payload: dict[str, Any],
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID] | None = None,
    audit_cause_override: str | None = None,
) -> dict[str, Any]:
    proposition = payload.get("proposition")
    if not isinstance(proposition, dict):
        raise ValidationError("situation merge payload missing proposition")
    entry = {"proposition": dict(proposition)}
    ensure_situation_compositional_defaults(entry)
    merged_prop = canonicalize_proposition(entry["proposition"])
    validate_proposition(merged_prop)
    grammar = derive_memory_grammar(merged_prop)
    if grammar.claim_role != "situation":
        raise ValidationError("situation merge payload did not produce a situation")

    row = await conn.fetchrow(
        """
        SELECT proposition, domain_tags, supporting_event_ids
        FROM models
        WHERE tenant_id = $1 AND id = $2 AND status = 'active'
        FOR UPDATE
        """,
        tenant_id,
        model_id,
    )
    if row is None:
        return {
            "summary": {
                "situation_members_added": 0,
                "decision": "skipped_missing_model",
            },
            "state_changes": 0,
        }

    candidate_tags = [
        str(tag)
        for tag in (payload.get("candidate_domain_tags") or [])
        if str(tag).strip()
    ]
    domain_tags = _merge_string_sequence(
        row["domain_tags"] or [],
        grammar.domain_tags,
        candidate_tags,
    )
    previous_state = {
        "proposition": _audit_jsonable(row["proposition"]),
        "domain_tags": _audit_jsonable(row["domain_tags"]),
        "supporting_event_ids": _audit_jsonable(row["supporting_event_ids"]),
    }
    supporting_event_ids = _merge_supporting_event_ids(
        row["supporting_event_ids"],
        cause_event_id,
        trigger_supporting_event_ids,
    )
    await conn.execute(
        """
        UPDATE models
        SET proposition = $3::jsonb,
            domain_tags = $4::text[],
            supporting_event_ids = $5::uuid[]
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        model_id,
        json.dumps(merged_prop, sort_keys=True, default=str),
        domain_tags,
        supporting_event_ids,
    )

    from services.domain.models.repo import _sync_model_composition_members

    await _sync_model_composition_members(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        proposition=merged_prop,
        source=str(
            payload.get("composition_source")
            or audit_cause_override
            or "reconciliation_merge"
        ),
    )

    await emit_state_change(
        conn,
        kind="model_updated",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=cause_event_id,
        entity_kind="model",
        metadata={
            "columns": [
                "domain_tags",
                "model_composition_members",
                "proposition",
                "supporting_event_ids",
            ],
            "lifecycle": "situation_merge",
        },
    )

    from .audit import CAUSE_FIELD_UPDATE, emit_audit_event

    await emit_audit_event(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        cause_type=audit_cause_override or CAUSE_FIELD_UPDATE,
        new_state={
            "proposition": _audit_jsonable(merged_prop),
            "domain_tags": domain_tags,
            "supporting_event_ids": [str(uid) for uid in supporting_event_ids],
        },
        previous_state=previous_state,
        cause_id=cause_event_id,
        changed_fields=[
            "domain_tags",
            "model_composition_members",
            "proposition",
            "supporting_event_ids",
        ],
    )

    added_members = [
        str(raw)
        for raw in (payload.get("added_member_model_ids") or [])
        if _coerce_uuid_or_none(raw) is not None
    ]
    return {
        "summary": {
            "situation_members_added": len(set(added_members)),
            "decision": "situation_merged",
        },
        "state_changes": 1,
    }


def _merge_string_sequence(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, (list, tuple, set)):
            continue
        for raw in value:
            text = str(raw).strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
    return out


def _entry_is_situation(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    prop = entry.get("proposition")
    if not isinstance(prop, dict):
        return False
    try:
        grammar = derive_memory_grammar(prop)
    except Exception:
        return False
    return grammar.claim_role == "situation"


async def _coalesce_same_event_situation_insert(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
) -> dict[str, Any] | None:
    entry = dict(op.entry or {})
    source_event_ids = _merge_event_ids(
        entry.get("supporting_event_ids"),
        entry.get("born_from_event_id"),
        cause_event_id,
        trigger_supporting_event_ids,
    )
    source_event_id = source_event_ids[0] if source_event_ids else None
    if source_event_id is None:
        return None
    anchor = await _select_same_event_situation_anchor(
        conn,
        tenant_id=tenant_id,
        source_event_id=source_event_id,
        entry=entry,
    )
    if anchor is None:
        return None

    from .reconciler_situation_merge import build_situation_merge_payload

    payload = build_situation_merge_payload(
        entry=entry,
        best_row=anchor,
        source_event_id=source_event_id,
    )
    if payload is None:
        return None
    payload["composition_source"] = "same_event_situation_coalesce"
    merge_result = await _apply_situation_merge_payload(
        conn,
        tenant_id=tenant_id,
        model_id=anchor["id"],
        payload=payload,
        cause_event_id=source_event_id,
        trigger_supporting_event_ids=source_event_ids,
        audit_cause_override="reconciliation_merge",
    )
    return {
        "summary": {
            "op": "update",
            "decision": "same_event_situation_coalesced",
            "model_id": str(anchor["id"]),
            "claim_role": "situation",
            "internal_situation_merge": True,
            "situation_members_added": merge_result["summary"][
                "situation_members_added"
            ],
        },
        "model_id": anchor["id"],
        "state_changes": merge_result["state_changes"],
    }


async def _select_same_event_situation_anchor(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_event_id: UUID,
    entry: dict[str, Any],
) -> dict[str, Any] | None:
    rows = await conn.fetch(
        """
        SELECT id, proposition, "natural", scope_actors, scope_entities,
               domain_tags
        FROM models
        WHERE tenant_id = $1
          AND born_from_event_id = $2
          AND status = 'active'
          AND claim_role = 'situation'
        ORDER BY created_at ASC
        LIMIT 8
        """,
        tenant_id,
        source_event_id,
    )
    if not rows:
        return None

    candidate_prop = _canonical_prop_for_entry(entry)
    candidate_grammar = derive_memory_grammar(
        candidate_prop,
        natural=str(entry.get("natural") or ""),
        scope_entities=[
            ent for ent in (entry.get("scope_entities") or []) if isinstance(ent, dict)
        ],
    )
    candidate_text = _evidence_entry_text(entry)
    candidate_tags = set(candidate_grammar.domain_tags)
    candidate_tags.update(str(tag) for tag in (entry.get("domain_tags") or []))
    candidate_pressure = candidate_prop.get("pressure_type")

    best: dict[str, Any] | None = None
    best_score = 0.0
    for row in rows:
        row_prop = _json_obj(row["proposition"])
        if not row_prop:
            continue
        row_grammar = derive_memory_grammar(row_prop)
        if row_grammar.claim_role != "situation":
            continue
        row_text = " ".join(
            part
            for part in (
                str(row["natural"] or ""),
                json.dumps(row_prop, sort_keys=True, default=str),
            )
            if part
        )
        score = _token_overlap_score(candidate_text, row_text) * 0.45
        row_tags = set(row_grammar.domain_tags)
        row_tags.update(str(tag) for tag in (row["domain_tags"] or []))
        if row_tags and candidate_tags and row_tags & candidate_tags:
            score += 0.20
        if candidate_pressure and row_prop.get("pressure_type") == candidate_pressure:
            score += 0.18
        if _scope_overlaps(
            entry.get("scope_actors") or [],
            entry.get("scope_entities") or [],
            row["scope_actors"] or [],
            row["scope_entities"] or [],
        ):
            score += 0.22
        # Same event is already a strong prior; this floor lets two
        # splitter/direct situations with good tags coalesce even when their
        # member_model_ids are disjoint.
        if score > best_score:
            best_score = score
            best = dict(row)

    if best is None or best_score < 0.24:
        return None
    return best


def _scope_overlaps(
    left_actors: Any,
    left_entities: Any,
    right_actors: Any,
    right_entities: Any,
) -> bool:
    left_actor_ids = {
        uid
        for raw in (left_actors or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    }
    right_actor_ids = {
        uid
        for raw in (right_actors or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    }
    if left_actor_ids and right_actor_ids and left_actor_ids & right_actor_ids:
        return True

    left_entity_keys = {
        (str(ent.get("type") or ""), str(ent.get("id") or ""))
        for ent in (left_entities or [])
        if isinstance(ent, dict) and ent.get("id")
    }
    right_entity_keys = {
        (str(ent.get("type") or ""), str(ent.get("id") or ""))
        for ent in (right_entities or [])
        if isinstance(ent, dict) and ent.get("id")
    }
    return bool(
        left_entity_keys and right_entity_keys and left_entity_keys & right_entity_keys
    )


async def _apply_claim_op(
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    audit_cause_override: str | None = None,
    doc_memory_source: str | None = None,
) -> dict[str, Any]:
    if op.op == "insert":
        return await _apply_claim_insert(
            op,
            conn,
            models_repo,
            tenant_id,
            cause_event_id=cause_event_id,
            trigger_supporting_event_ids=trigger_supporting_event_ids,
            doc_memory_source=doc_memory_source,
        )
    if op.op == "update":
        return await _apply_claim_update(
            op,
            conn,
            models_repo,
            tenant_id,
            cause_event_id=cause_event_id,
            trigger_supporting_event_ids=trigger_supporting_event_ids,
            audit_cause_override=audit_cause_override,
        )
    if op.op == "archive":
        return await _apply_claim_archive(
            op,
            conn,
            models_repo,
            cause_event_id=cause_event_id,
        )
    raise ValidationError(f"unknown claim_op: {op.op!r}")


async def _apply_claim_insert(
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    doc_memory_source: str | None = None,
) -> dict[str, Any]:
    proposed = _prepare_claim_insert_model(
        op,
        tenant_id,
        cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
    )
    row = await models_repo.insert(
        proposed,
        conn=conn,
        apply_confidence_calibration=False,
    )
    # Document-memory mint counter (Option A, §4.4): the Model is now durably
    # inserted, and `doc_memory_source` is set only when this apply was driven by
    # an enriched-T1 document trigger — so this Model is document-derived. Count
    # it once per genuine insert, keyed by the document's source channel. Bumped
    # only on the success path (after `insert` returns); a non-document trigger
    # leaves `doc_memory_source` None and is never counted.
    if doc_memory_source is not None:
        record_doc_memory_model_minted(doc_memory_source)
    prediction_row_id = await materialize_model_prediction(conn, model=row)
    return {
        "summary": {
            "op": "insert",
            "model_id": str(row.id),
            "confidence": row.confidence,
            "proposition_kind": row.proposition_kind,
            "claim_role": row.claim_role,
            "abstraction_level": row.abstraction_level,
            "domain_tags": list(row.domain_tags or []),
            **(
                {"model_prediction_id": str(prediction_row_id)}
                if prediction_row_id is not None
                else {}
            ),
        },
        "model_id": row.id,
        "state_changes": 1,
    }


def _prepare_claim_insert_model(
    op: ClaimOp,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
) -> ModelCreate:
    op = _with_claim_evidence_defaults(
        op,
        trigger_cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
    )
    entry = dict(op.entry or {})
    entry.setdefault("tenant_id", tenant_id)
    entry.setdefault("confidence_at_assertion", entry.get("confidence", 0.5))
    if "born_from_event_id" not in entry and cause_event_id is not None:
        entry["born_from_event_id"] = cause_event_id
    for stray in ("title", "description", "id", "model_id"):
        entry.pop(stray, None)
    entry = prepare_prediction_entry(entry)
    ensure_situation_compositional_defaults(entry)
    if "scope_temporal" not in entry:
        entry["scope_temporal"] = {
            "valid_from": datetime.now(timezone.utc).isoformat(),
            "valid_until": None,
        }
    if "embedding" not in entry or is_zero_embedding(entry.get("embedding")):
        entry["embedding"] = deterministic_text_embedding(
            str(entry.get("natural") or entry.get("proposition") or "")
        )
    return ModelCreate.model_validate(entry)


async def _apply_claim_update(
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    audit_cause_override: str | None,
) -> dict[str, Any]:
    if op.model_id is None or not op.changes:
        raise ValidationError("apply_claim_op update: bad op")
    prepared = await _prepare_claim_update(
        op,
        conn,
        tenant_id=tenant_id,
        cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
        audit_cause_override=audit_cause_override,
    )
    if not prepared.changes and prepared.situation_merge_payload is None:
        return _skipped_claim_update_result(op, prepared)

    emitted = await _apply_claim_confidence_update(
        prepared.changes,
        op,
        conn,
        models_repo,
        cause_event_id=cause_event_id,
        audit_cause_override=audit_cause_override,
    )
    if prepared.changes:
        emitted += await _apply_model_column_updates(
            prepared.changes,
            conn,
            tenant_id=tenant_id,
            model_id=op.model_id,
            cause_event_id=cause_event_id,
            audit_cause_override=audit_cause_override,
            changed_fields_for_summary=prepared.changed_fields_for_summary,
        )
    situation_merge_summary = await _apply_claim_situation_merge_update(
        prepared,
        conn,
        tenant_id=tenant_id,
        model_id=op.model_id,
        cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
        audit_cause_override=audit_cause_override,
    )
    if situation_merge_summary is not None:
        emitted += int(situation_merge_summary["state_changes"])

    return _claim_update_result(op, prepared, situation_merge_summary, emitted)


async def _prepare_claim_update(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    audit_cause_override: str | None,
) -> _ClaimUpdatePreparation:
    raw_changes = dict(op.changes or {})
    situation_merge_payload = None
    if audit_cause_override == "reconciliation_merge":
        maybe_payload = raw_changes.pop("__situation_merge", None)
        if isinstance(maybe_payload, dict):
            situation_merge_payload = maybe_payload
    changes = {
        k: _coerce_update_value(k, v)
        for k, v in raw_changes.items()
        if k in _ALLOWED_MODEL_UPDATE_COLUMNS
    }
    user_change_keys = set(changes)
    await _merge_supporting_update_ids(
        changes,
        conn,
        tenant_id=tenant_id,
        model_id=op.model_id,
        cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
    )
    resolution_update_dropped = _drop_inconsistent_resolution_update(
        changes,
        user_change_keys,
    )
    return _ClaimUpdatePreparation(
        changes=changes,
        changed_fields_for_summary=set(changes.keys()),
        situation_merge_payload=situation_merge_payload,
        resolution_update_dropped=resolution_update_dropped,
    )


async def _merge_supporting_update_ids(
    changes: dict[str, Any],
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID | None,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
) -> None:
    if model_id is None:
        return
    supporting_update_ids = _merge_event_ids(
        changes.get("supporting_event_ids"),
        cause_event_id,
        trigger_supporting_event_ids,
    )
    if not supporting_update_ids:
        return
    row = await conn.fetchrow(
        """
        SELECT supporting_event_ids
        FROM models
        WHERE tenant_id = $1 AND id = $2 AND status = 'active'
        """,
        tenant_id,
        model_id,
    )
    existing_supporting_ids = (
        _merge_event_ids(row["supporting_event_ids"]) if row is not None else []
    )
    merged_supporting_ids = _merge_supporting_event_ids(
        existing_supporting_ids,
        supporting_update_ids,
    )
    if merged_supporting_ids != existing_supporting_ids:
        changes["supporting_event_ids"] = merged_supporting_ids
    else:
        changes.pop("supporting_event_ids", None)


def _drop_inconsistent_resolution_update(
    changes: dict[str, Any],
    user_change_keys: set[str],
) -> bool:
    resolution_keys = {"resolved_at", "resolution_outcome"} & set(changes)
    if not resolution_keys or resolution_keys == {"resolved_at", "resolution_outcome"}:
        return False
    changes.pop("resolved_at", None)
    changes.pop("resolution_outcome", None)
    if not (user_change_keys - {"resolved_at", "resolution_outcome"}):
        changes.clear()
    return True


def _skipped_claim_update_result(
    op: ClaimOp,
    prepared: _ClaimUpdatePreparation,
) -> dict[str, Any]:
    return {
        "summary": {
            "op": "skip",
            "reason": (
                "inconsistent_resolution_update"
                if prepared.resolution_update_dropped
                else "no_allowed_columns"
            ),
            "model_id": str(op.model_id),
        },
        "model_id": None,
        "state_changes": 0,
    }


async def _apply_claim_confidence_update(
    changes: dict[str, Any],
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    *,
    cause_event_id: UUID | None,
    audit_cause_override: str | None,
) -> int:
    if op.model_id is None or "confidence" not in changes:
        return 0
    await models_repo.bulk_confidence_update(
        {op.model_id: float(changes["confidence"])},
        cause_event_id=cause_event_id,
        audit_cause_override=audit_cause_override,
        conn=conn,
    )
    changes.pop("confidence")
    return 1


async def _apply_model_column_updates(
    changes: dict[str, Any],
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    cause_event_id: UUID | None,
    audit_cause_override: str | None,
    changed_fields_for_summary: set[str],
) -> int:
    from .audit import (
        CAUSE_FIELD_UPDATE,
        emit_audit_event,
    )

    pre_snapshot, previous_signal_readings = await _snapshot_model_update_columns(
        conn,
        model_id=model_id,
        changes=changes,
    )
    await _execute_model_update(conn, model_id=model_id, changes=changes)
    await _sync_model_update_relations(
        conn,
        tenant_id=tenant_id,
        model_id=model_id,
        changes=changes,
        cause_event_id=cause_event_id,
    )
    await emit_state_change(
        conn,
        kind="model_updated",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=cause_event_id,
        entity_kind="model",
        metadata={"columns": sorted(list(changes.keys()))},
    )
    await emit_audit_event(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        cause_type=audit_cause_override or CAUSE_FIELD_UPDATE,
        new_state={k: _audit_jsonable(v) for k, v in changes.items()},
        previous_state=pre_snapshot or None,
        cause_id=cause_event_id,
        changed_fields=sorted(list(changes.keys())),
    )
    await _apply_model_update_side_effects(
        conn,
        tenant_id=tenant_id,
        model_id=model_id,
        changes=changes,
        previous_signal_readings=previous_signal_readings,
        cause_event_id=cause_event_id,
        changed_fields_for_summary=changed_fields_for_summary,
    )
    return 1


async def _snapshot_model_update_columns(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    changes: dict[str, Any],
) -> tuple[dict[str, Any], Any]:
    cols_csv = ", ".join(changes.keys())
    pre_snapshot: dict[str, Any] = {}
    pre_row = await conn.fetchrow(
        f"SELECT {cols_csv} FROM models WHERE id = $1", model_id
    )
    if pre_row is not None:
        for key in changes.keys():
            pre_snapshot[key] = _audit_jsonable(pre_row[key])
    previous_signal_readings = (
        pre_row["signal_readings"]
        if pre_row is not None and "signal_readings" in changes
        else None
    )
    return pre_snapshot, previous_signal_readings


async def _execute_model_update(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    changes: dict[str, Any],
) -> None:
    set_clauses = []
    params: list[Any] = []
    for index, (key, value) in enumerate(changes.items(), start=1):
        if key in ("signal_readings", "proposition"):
            set_clauses.append(f"{key} = ${index}::jsonb")
            params.append(json.dumps(value, default=str))
        elif key in ("domain_tags",):
            set_clauses.append(f"{key} = ${index}::text[]")
            params.append(list(value) if isinstance(value, (list, tuple)) else [value])
        elif key in (
            "supporting_event_ids",
            "supporting_model_ids",
            "contributing_models",
        ):
            set_clauses.append(f"{key} = ${index}::uuid[]")
            params.append(list(value) if isinstance(value, (list, tuple)) else [value])
        elif key in ("last_confirmed_at", "resolved_at"):
            set_clauses.append(f"{key} = ${index}")
            params.append(_coerce_dt(value))
        else:
            set_clauses.append(f"{key} = ${index}")
            params.append(value)
    params.append(model_id)
    sql = f"UPDATE models SET {', '.join(set_clauses)} " f"WHERE id = ${len(params)}"
    await conn.execute(sql, *params)


async def _sync_model_update_relations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    changes: dict[str, Any],
    cause_event_id: UUID | None,
) -> None:
    if "supporting_model_ids" not in changes and "contributing_models" not in changes:
        return
    from services.domain.models.repo import _set_model_relations

    await _set_model_relations(
        conn,
        model_id=model_id,
        tenant_id=tenant_id,
        detected_by="llm_explicit",
        supports=(
            list(changes["supporting_model_ids"])
            if "supporting_model_ids" in changes
            else None
        ),
        contributes_to=(
            list(changes["contributing_models"])
            if "contributing_models" in changes
            else None
        ),
        created_by_event_id=cause_event_id,
        update_arrays=False,
    )


async def _apply_model_update_side_effects(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    changes: dict[str, Any],
    previous_signal_readings: Any,
    cause_event_id: UUID | None,
    changed_fields_for_summary: set[str],
) -> None:
    if "signal_readings" in changes:
        await _append_signal_readings_sidecar_delta(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            previous_readings=previous_signal_readings,
            new_readings=changes["signal_readings"],
        )
    if "resolution_outcome" in changes:
        synced = await sync_model_prediction_resolution(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            resolution_outcome=changes.get("resolution_outcome"),
            observation_id=cause_event_id,
        )
        if synced:
            changed_fields_for_summary.add("model_predictions")


async def _apply_relation_claim_op(
    op: RelationClaimOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    think_run_id: UUID | None,
) -> dict[str, Any]:
    from services.domain.models.edges_repo import EdgesRepo
    from services.reasoning.edge_intelligence import EdgeIntelligenceRepo, RelationClaim

    edges_repo = EdgesRepo()
    evidence_event_ids = tuple(
        _merge_event_ids(
            op.evidence_event_ids,
            (cause_event_id,) if cause_event_id is not None else (),
        )
    )
    endpoint_status = op.endpoint_binding_status
    if op.source_model_id is not None and op.target_model_id is not None:
        endpoint_status = "bound"
    repo = EdgeIntelligenceRepo()
    row = await repo.insert_relation_claim(
        conn,
        RelationClaim(
            id=op.id,
            tenant_id=tenant_id,
            source_observation_id=evidence_event_ids[0] if evidence_event_ids else None,
            think_run_id=think_run_id,
            source_model_id=op.source_model_id,
            target_model_id=op.target_model_id,
            subject_ref=op.subject_ref,
            object_ref=op.object_ref,
            predicate=op.predicate,
            edge_kind=op.edge_kind,
            direction=op.direction,
            endpoint_binding_status=endpoint_status,
            write_policy=op.write_policy,
            status=op.status,
            confidence=op.confidence,
            weight=op.weight,
            binding_confidence=op.binding_confidence,
            evidence_event_ids=evidence_event_ids,
            evidence_model_ids=tuple(_merge_event_ids(op.evidence_model_ids)),
            evidence_text=op.evidence_text,
            explanation=op.explanation,
            temporal_bounds=op.temporal_bounds,
            metadata={
                **dict(op.metadata or {}),
                "relation_claim_op": True,
                "cause_event_id": str(cause_event_id) if cause_event_id else None,
            },
        ),
    )
    edge_ids: list[UUID] = []
    edge_summary: dict[str, Any] | None = None
    retired_edge_summaries: list[dict[str, Any]] = []
    if (
        op.status == "retired"
        and op.source_model_id is not None
        and op.target_model_id is not None
    ):
        count = await edges_repo.retire(
            conn,
            source=op.source_model_id,
            target=op.target_model_id,
            kind=op.edge_kind,
            tenant_id=tenant_id,
            reason=op.explanation or "relation_claim_retired",
        )
        retired_edge_summaries.append(
            {
                "op": "retire",
                "edge_kind": op.edge_kind,
                "source_model_id": str(op.source_model_id),
                "target_model_id": str(op.target_model_id),
                "retired_edges": count,
                "source": "relation_claim_op",
            }
        )
    if (
        op.write_policy == "accepted_edge"
        and op.source_model_id is not None
        and op.target_model_id is not None
        and op.status != "retired"
    ):
        edge_metadata = {
            **dict(op.metadata or {}),
            "relation_claim_id": str(row["id"]),
            "source": "relation_claim_op",
        }
        try:
            edge_ids = await edges_repo.link(
                conn,
                source=op.source_model_id,
                target=op.target_model_id,
                kind=op.edge_kind,
                tenant_id=tenant_id,
                detected_by="think_edge_op",
                weight=op.weight,
                metadata=edge_metadata,
                created_by_event_id=cause_event_id,
                confidence=op.confidence,
                evidence_event_ids=evidence_event_ids,
                evidence_model_ids=op.evidence_model_ids,
                explanation=op.explanation or op.evidence_text,
                review_status="accepted",
            )
        except EdgeRegistryError as exc:
            if _classify_apply_edge_drop_reason(exc) != "mutually_exclusive_edge":
                raise
            retired_edge_summaries = (
                await _retire_superseded_support_edges_for_relation_claim(
                    op,
                    conn,
                    tenant_id,
                    claim_id=row["id"],
                    edges_repo=edges_repo,
                )
            )
            if not retired_edge_summaries:
                raise
            edge_metadata["superseded_edge_count"] = sum(
                int(item.get("retired_edges") or 0)
                for item in retired_edge_summaries
            )
            edge_ids = await edges_repo.link(
                conn,
                source=op.source_model_id,
                target=op.target_model_id,
                kind=op.edge_kind,
                tenant_id=tenant_id,
                detected_by="think_edge_op",
                weight=op.weight,
                metadata=edge_metadata,
                created_by_event_id=cause_event_id,
                confidence=op.confidence,
                evidence_event_ids=evidence_event_ids,
                evidence_model_ids=op.evidence_model_ids,
                explanation=op.explanation or op.evidence_text,
                review_status="accepted",
            )
        row = await repo.mark_relation_claim_decided(
            conn,
            claim_id=row["id"],
            tenant_id=tenant_id,
            status="accepted",
            accepted_edge_ids=edge_ids,
            decision_metadata={
                "reason": "accepted_relation_claim_created_edge",
                "accepted_edge_ids": [str(edge_id) for edge_id in edge_ids],
                "superseded_edges": retired_edge_summaries,
            },
        ) or row
        edge_summary = {
            "op": "add",
            "edge_kind": op.edge_kind,
            "source_model_id": str(op.source_model_id),
            "target_model_id": str(op.target_model_id),
            "edge_ids": [str(edge_id) for edge_id in edge_ids],
            "review_status": "accepted",
            "source": "relation_claim_op",
        }
    edge_summaries = [
        *retired_edge_summaries,
        *([edge_summary] if edge_summary is not None else []),
    ]
    return {
        "summary": {
            "op": op.op,
            "relation_claim_id": str(row["id"]),
            "edge_kind": op.edge_kind,
            "predicate": op.predicate,
            "source_model_id": (
                str(op.source_model_id) if op.source_model_id is not None else None
            ),
            "target_model_id": (
                str(op.target_model_id) if op.target_model_id is not None else None
            ),
            "endpoint_binding_status": row["endpoint_binding_status"],
            "write_policy": row["write_policy"],
            "status": row["status"],
            "accepted_edge_ids": [str(edge_id) for edge_id in edge_ids],
            "superseded_edge_count": sum(
                int(item.get("retired_edges") or 0)
                for item in retired_edge_summaries
            ),
        },
        "edge_summary": edge_summary,
        "edge_summaries": edge_summaries,
    }


async def _apply_relation_frame_op(
    op: RelationFrameOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    think_run_id: UUID | None,
) -> dict[str, Any]:
    from services.reasoning.edge_intelligence import (
        EdgeIntelligenceRepo,
        RelationFrame,
        RelationParticipant,
        project_relation_frame,
    )

    repo = EdgeIntelligenceRepo()
    evidence_event_ids = tuple(
        _merge_event_ids(
            op.evidence_event_ids,
            (cause_event_id,) if cause_event_id is not None else (),
        )
    )
    evidence_model_ids = tuple(_merge_event_ids(op.evidence_model_ids))
    participant_models = tuple(
        participant.model_id for participant in op.participants
    )
    frame = await repo.insert_relation_frame(
        conn,
        RelationFrame(
            id=op.id,
            tenant_id=tenant_id,
            source_observation_id=evidence_event_ids[0] if evidence_event_ids else None,
            think_run_id=think_run_id,
            relation_kind=op.relation_kind,
            status=op.status,
            participant_binding_status=op.participant_binding_status,
            write_policy=op.write_policy,
            confidence=op.confidence,
            evidence_event_ids=evidence_event_ids,
            evidence_model_ids=tuple(_merge_event_ids(evidence_model_ids, participant_models)),
            evidence_text=op.evidence_text,
            explanation=op.explanation,
            temporal_bounds=op.temporal_bounds,
            metadata={
                **dict(op.metadata or {}),
                "relation_frame_op": True,
                "cause_event_id": str(cause_event_id) if cause_event_id else None,
            },
        ),
        participants=tuple(
            RelationParticipant(
                model_id=participant.model_id,
                role=participant.role,
                binding_confidence=participant.binding_confidence,
                cardinality_group=participant.cardinality_group,
                metadata=participant.metadata,
            )
            for participant in op.participants
        ),
    )

    projection_report = None
    edge_summaries: list[dict[str, Any]] = []
    if op.write_policy == "project_edges" and op.status == "accepted":
        projection_report = await project_relation_frame(
            conn,
            tenant_id=tenant_id,
            relation_id=frame["id"],
            created_by_event_id=cause_event_id,
            repo=repo,
        )
        edge_summaries = [
            {
                "op": "add",
                "edge_kind": projection["edge_kind"],
                "source_model_id": str(projection["source_model_id"]),
                "target_model_id": str(projection["target_model_id"]),
                "edge_ids": [str(projection["edge_id"])],
                "review_status": "accepted",
                "source": "relation_frame_projection",
                "relation_instance_id": str(frame["id"]),
                "projection_rule": projection["projection_rule"],
            }
            for projection in projection_report.projections
        ]
        await repo.mark_relation_frame_decided(
            conn,
            relation_id=frame["id"],
            tenant_id=tenant_id,
            status="accepted",
            decision_metadata={
                "reason": "accepted_relation_frame_projected_edges",
                "projected_edge_ids": [
                    str(edge_id) for edge_id in projection_report.edge_ids
                ],
                "skipped": projection_report.skipped,
            },
        )

    return {
        "summary": {
            "op": op.op,
            "relation_instance_id": str(frame["id"]),
            "relation_kind": op.relation_kind,
            "status": op.status,
            "write_policy": op.write_policy,
            "participant_count": len(frame["participants"]),
            "projected_edge_count": (
                len(projection_report.edge_ids) if projection_report is not None else 0
            ),
            "projection_skipped": (
                projection_report.skipped if projection_report is not None else []
            ),
        },
        "edge_summaries": edge_summaries,
    }


async def _retire_superseded_support_edges_for_relation_claim(
    op: RelationClaimOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    claim_id: UUID,
    edges_repo: Any,
) -> list[dict[str, Any]]:
    from lib.shared.edge_registry import EDGE_REGISTRY

    if (
        op.edge_kind not in _RELATION_CLAIM_SUPPORT_SUPERSEDERS
        or op.source_model_id is None
        or op.target_model_id is None
    ):
        return []
    spec = EDGE_REGISTRY.get(op.edge_kind)
    if spec is None or "supports" not in spec.mutually_exclusive_with:
        return []

    pairs = _relation_claim_conflict_pairs(
        source=op.source_model_id,
        target=op.target_model_id,
        symmetric=not spec.is_directed,
    )
    conflicts_by_pair: list[tuple[UUID, UUID, list[asyncpg.Record]]] = []
    for source, target in pairs:
        rows = await conn.fetch(
            """
            SELECT id, source_model_id, target_model_id, edge_kind,
                   detected_by, review_status, confidence
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = ANY($4::text[])
              AND status = 'active'
              AND review_status != 'rejected'
            ORDER BY created_at ASC, id ASC
            """,
            tenant_id,
            source,
            target,
            list(spec.mutually_exclusive_with),
        )
        if rows:
            conflicts_by_pair.append((source, target, rows))

    if not conflicts_by_pair:
        return []

    for _source, _target, rows in conflicts_by_pair:
        for row in rows:
            if row["edge_kind"] != "supports":
                return []
            if row["detected_by"] in _NON_OVERRIDABLE_EDGE_PROVENANCE:
                return []

    summaries: list[dict[str, Any]] = []
    affected_targets: set[UUID] = set()
    for source, target, rows in conflicts_by_pair:
        support_rows = [row for row in rows if row["edge_kind"] == "supports"]
        if not support_rows:
            continue
        count = await edges_repo.retire(
            conn,
            source=source,
            target=target,
            kind="supports",
            tenant_id=tenant_id,
            reason=(
                "superseded_by_relation_claim:"
                f"{op.edge_kind}:{claim_id}"
            ),
        )
        if count <= 0:
            continue
        affected_targets.add(target)
        summaries.append({
            "op": "retire",
            "edge_kind": "supports",
            "source_model_id": str(source),
            "target_model_id": str(target),
            "retired_edges": int(count),
            "retired_edge_ids": [str(row["id"]) for row in support_rows],
            "reason": "superseded_by_precise_relation_claim",
            "superseded_by_edge_kind": op.edge_kind,
            "relation_claim_id": str(claim_id),
            "source": "relation_claim_op",
        })

    for target in affected_targets:
        await _refresh_supporting_model_ids_from_active_edges(
            conn,
            tenant_id=tenant_id,
            model_id=target,
        )
    return summaries


def _relation_claim_conflict_pairs(
    *,
    source: UUID,
    target: UUID,
    symmetric: bool,
) -> list[tuple[UUID, UUID]]:
    pairs = [(source, target)]
    if symmetric and source != target:
        pairs.append((target, source))
    return pairs


async def _refresh_supporting_model_ids_from_active_edges(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
) -> None:
    support_rows = await conn.fetch(
        """
        SELECT source_model_id AS model_id
        FROM model_edges
        WHERE tenant_id = $1
          AND target_model_id = $2
          AND edge_kind = 'supports'
          AND status = 'active'
        ORDER BY created_at ASC, id ASC
        """,
        tenant_id,
        model_id,
    )
    instance_rows = await conn.fetch(
        """
        SELECT target_model_id AS model_id
        FROM model_edges
        WHERE tenant_id = $1
          AND source_model_id = $2
          AND edge_kind = 'instance_of'
          AND status = 'active'
        ORDER BY created_at ASC, id ASC
        """,
        tenant_id,
        model_id,
    )
    seen: set[UUID] = set()
    supporting_model_ids: list[UUID] = []
    for row in [*support_rows, *instance_rows]:
        related_model_id = row["model_id"]
        if related_model_id in seen:
            continue
        seen.add(related_model_id)
        supporting_model_ids.append(related_model_id)

    await conn.execute(
        """
        UPDATE models
        SET supporting_model_ids = $1::uuid[]
        WHERE tenant_id = $2
          AND id = $3
        """,
        supporting_model_ids,
        tenant_id,
        model_id,
    )


async def _apply_claim_situation_merge_update(
    prepared: _ClaimUpdatePreparation,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    cause_event_id: UUID | None,
    trigger_supporting_event_ids: list[UUID],
    audit_cause_override: str | None,
) -> dict[str, Any] | None:
    if prepared.situation_merge_payload is None:
        return None
    merge_result = await _apply_situation_merge_payload(
        conn,
        tenant_id=tenant_id,
        model_id=model_id,
        payload=prepared.situation_merge_payload,
        cause_event_id=cause_event_id,
        trigger_supporting_event_ids=trigger_supporting_event_ids,
        audit_cause_override=audit_cause_override,
    )
    prepared.changed_fields_for_summary.update(
        ("proposition", "domain_tags", "model_composition_members")
    )
    return {
        "summary": merge_result["summary"],
        "state_changes": merge_result["state_changes"],
    }


def _claim_update_result(
    op: ClaimOp,
    prepared: _ClaimUpdatePreparation,
    situation_merge_summary: dict[str, Any] | None,
    emitted: int,
) -> dict[str, Any]:
    summary = situation_merge_summary["summary"] if situation_merge_summary else None
    return {
        "summary": {
            "op": "update",
            "model_id": str(op.model_id),
            "changed": sorted(prepared.changed_fields_for_summary),
            **({"claim_role": "situation"} if summary else {}),
            **(
                {
                    "internal_situation_merge": True,
                    "situation_members_added": summary["situation_members_added"],
                }
                if summary
                else {}
            ),
            **(
                {"dropped_inconsistent_resolution_update": True}
                if prepared.resolution_update_dropped
                else {}
            ),
        },
        "model_id": op.model_id,
        "state_changes": emitted,
    }


async def _apply_claim_archive(
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    if op.model_id is None or not op.reason:
        raise ValidationError("apply_claim_op archive: bad op")
    await models_repo.archive(
        op.model_id,
        op.reason,  # type: ignore[arg-type]
        cause_event_id=cause_event_id,
        conn=conn,
    )
    return {
        "summary": {
            "op": "archive",
            "model_id": str(op.model_id),
            "reason": op.reason,
        },
        "model_id": op.model_id,
        "state_changes": 1,
    }


async def _apply_edge_op(
    op: EdgeOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    from services.domain.models.edges_repo import EdgesRepo

    repo = EdgesRepo()
    if op.op == "add":
        evidence_event_ids = _merge_event_ids(
            op.evidence_event_ids,
            (cause_event_id,) if cause_event_id is not None else (),
        )
        ids = await repo.link(
            conn,
            source=op.source_model_id,
            target=op.target_model_id,
            kind=op.edge_kind,
            tenant_id=tenant_id,
            detected_by=op.detected_by or "think_edge_op",
            weight=op.weight,
            metadata=op.metadata,
            created_by_event_id=cause_event_id,
            confidence=op.confidence,
            evidence_event_ids=evidence_event_ids,
            evidence_model_ids=op.evidence_model_ids,
            explanation=op.explanation,
            review_status=op.review_status,
            decay_after=op.decay_after,
            expires_at=op.expires_at,
        )
        return {
            "summary": {
                "op": "add",
                "edge_kind": op.edge_kind,
                "source_model_id": str(op.source_model_id),
                "target_model_id": str(op.target_model_id),
                "edge_ids": [str(edge_id) for edge_id in ids],
                "review_status": op.review_status,
            },
            "state_changes": 0,
        }
    if op.op == "retire":
        count = await repo.retire(
            conn,
            source=op.source_model_id,
            target=op.target_model_id,
            kind=op.edge_kind,
            tenant_id=tenant_id,
            reason=op.reason or "edge_op_retire",
        )
        return {
            "summary": {
                "op": "retire",
                "edge_kind": op.edge_kind,
                "source_model_id": str(op.source_model_id),
                "target_model_id": str(op.target_model_id),
                "retired_edges": count,
            },
            "state_changes": 0,
        }
    raise ValidationError(f"unknown edge_op: {op.op!r}")


async def _apply_ontology_gap_op(
    op: OntologyGapOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    from services.reasoning.relationships import (
        JudgmentScores,
        RelationshipCandidatesRepo,
        RelationshipOntologyProposalsRepo,
        make_edge_type_candidate,
    )

    evidence_model_ids = tuple(
        dict.fromkeys(
            [
                op.source_model_id,
                op.target_model_id,
                *op.evidence_model_ids,
            ]
        )
    )
    evidence_event_ids = tuple(
        dict.fromkeys(
            [
                *(op.evidence_event_ids or []),
                *([cause_event_id] if cause_event_id is not None else []),
            ]
        )
    )
    candidate = make_edge_type_candidate(
        tenant_id=tenant_id,
        proposed_edge_kind=op.proposed_edge_kind,
        description=op.description,
        relationship_summary=op.relationship_summary,
        parent_kind=op.parent_kind,
        nearest_existing_kind=op.nearest_existing_kind,
        directionality=op.directionality,
        inverse_label=op.inverse_label,
        dropped_dimensions=tuple(op.dropped_dimensions),
        evidence_model_ids=evidence_model_ids,
        evidence_event_ids=evidence_event_ids,
        example_source_model_id=op.source_model_id,
        example_target_model_id=op.target_model_id,
        scores=JudgmentScores(
            impact=op.impact,
            uncertainty=op.uncertainty,
            urgency=op.urgency,
            actionability=op.actionability,
            authority_required=op.authority_required,
            novelty=op.novelty,
            confidence=op.confidence,
        ),
        source="think_ontology_gap_op",
        metadata={
            "think": {
                "op": op.op,
                "cause_event_id": str(cause_event_id) if cause_event_id else None,
            }
        },
    )
    candidates_repo = RelationshipCandidatesRepo()
    row = await candidates_repo.insert(conn, candidate)
    proposals_upserted = 0
    try:
        proposals = await (
            RelationshipOntologyProposalsRepo().aggregate_from_edge_type_candidates(
                conn,
                tenant_id=tenant_id,
            )
        )
        proposals_upserted = len(proposals)
        refreshed = await candidates_repo.get(
            conn,
            candidate_id=row["id"],
            tenant_id=tenant_id,
        )
        if refreshed is not None:
            row = refreshed
    except (
        asyncpg.UndefinedTableError,
        asyncpg.UndefinedColumnError,
    ):
        proposals_upserted = 0
    proposed = row["proposed_proposition"]["proposed_edge_kind"]
    fallback = row["metadata"].get("ontology_gap", {}).get("retrieval_fallback_kind")
    return {
        "summary": {
            "op": op.op,
            "candidate_kind": "edge_type",
            "relationship_candidate_id": str(row["id"]),
            "proposed_edge_kind": proposed,
            "source_model_id": str(op.source_model_id),
            "target_model_id": str(op.target_model_id),
            "retrieval_fallback_kind": fallback,
            "review_status": row["review_status"],
            "ontology_proposals_upserted": proposals_upserted,
        },
        "state_changes": 0,
    }


def _act_result(summary: dict[str, Any], state_changes: int) -> dict[str, Any]:
    return {"summary": summary, "state_changes": state_changes}


async def _apply_goal_act_op(
    op: ActOp,
    ent: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any] | None:
    if op.op == "create_goal":
        row = await goals_svc.create(
            title=ent["title"],
            description=ent.get("description"),
            parent_goal_id=_coerce_uuid(ent.get("parent_goal_id")),
            altitude=ent.get("altitude", "operational"),
            success_criteria=ent.get("success_criteria"),
            target_date=_coerce_dt(ent.get("target_date")),
            created_by_event_id=_coerce_uuid(
                ent.get("created_by_event_id") or cause_event_id
            ),
            tenant_id=tenant_id,
            conn=conn,
        )
        return _act_result({"op": "create_goal", "goal_id": str(row.id)}, 1)

    if op.op == "update_goal":
        # Minimal update path — bumps cached_health or target_date only.
        gid = _coerce_uuid(ent.get("id"))
        if gid is None:
            raise ValidationError("update_goal requires entity.id")
        set_clauses = []
        params: list[Any] = []
        i = 1
        if "cached_health" in ent:
            set_clauses.append(f"cached_health = ${i}")
            params.append(ent["cached_health"])
            set_clauses.append("cached_health_computed_at = now()")
            i += 1
        if "target_date" in ent:
            set_clauses.append(f"target_date = ${i}")
            params.append(_coerce_dt(ent["target_date"]))
            i += 1
        if not set_clauses:
            raise ValidationError("update_goal: nothing to change")
        params.append(gid)
        await conn.execute(
            f"UPDATE goals SET {', '.join(set_clauses)} WHERE id = ${i}",
            *params,
        )
        await emit_state_change(
            conn,
            kind="goal_updated",
            entity_id=gid,
            tenant_id=tenant_id,
            cause_event_id=cause_event_id,
            entity_kind="goal",
            metadata={
                k: v for k, v in ent.items() if k in ("cached_health", "target_date")
            },
        )
        return _act_result({"op": "update_goal", "goal_id": str(gid)}, 1)

    if op.op == "transition_goal":
        gid = _coerce_uuid(ent["id"])
        row = await goals_svc.transition(
            gid,
            ent["new_state"],
            cause_event_id=cause_event_id,
            conn=conn,
        )
        return _act_result(
            {
                "op": "transition_goal",
                "goal_id": str(row.id),
                "new_state": ent["new_state"],
            },
            1,
        )
    return None


async def _apply_commitment_act_op(
    op: ActOp,
    ent: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any] | None:
    if op.op == "create_commitment":
        row = await commitments_svc.create(
            title=ent["title"],
            description=ent.get("description"),
            initial_state=ent.get("initial_state", "proposed"),
            owner_id=_coerce_uuid(ent.get("owner_id")),
            due_date=_coerce_dt(ent.get("due_date")),
            ambition_level=ent.get("ambition_level", "base"),
            priority=int(ent.get("priority", 5)),
            success_criteria=ent.get("success_criteria"),
            contributes_to_goal_ids=[
                _coerce_uuid(x)
                if not isinstance(x, (list, tuple))
                else (_coerce_uuid(x[0]), bool(x[1]))
                for x in (ent.get("contributes_to_goal_ids") or [])
            ],
            depends_on_commitment_ids=[
                _coerce_uuid(x) for x in (ent.get("depends_on_commitment_ids") or [])
            ],
            constrained_by_decision_ids=[
                _coerce_uuid(x) for x in (ent.get("constrained_by_decision_ids") or [])
            ],
            contributors=[
                (_coerce_uuid(x[0]), x[1] if len(x) > 1 else None)
                for x in (ent.get("contributors") or [])
            ],
            external_counterparty_ref=ent.get("external_counterparty_ref"),
            estimated_capacity=ent.get("estimated_capacity"),
            created_by_event_id=_coerce_uuid(
                ent.get("created_by_event_id") or cause_event_id
            ),
            last_confidence_basis=op.confidence_basis,
            tenant_id=tenant_id,
            conn=conn,
        )
        return _act_result({"op": "create_commitment", "commitment_id": str(row.id)}, 1)

    if op.op == "transition_commitment":
        cid = _coerce_uuid(ent["id"])
        resolved = [_coerce_uuid(x) for x in (ent.get("resolved_by_event_ids") or [])]
        row = await commitments_svc.transition(
            cid,
            ent["new_state"],
            resolved_by_event_ids=resolved or None,
            last_confidence_basis=op.confidence_basis,
            cause_event_id=cause_event_id or _coerce_uuid(ent.get("cause_event_id")),
            conn=conn,
        )
        return _act_result(
            {
                "op": "transition_commitment",
                "commitment_id": str(row.id),
                "new_state": ent["new_state"],
            },
            1,
        )
    return None


async def _apply_decision_act_op(
    op: ActOp,
    ent: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any] | None:
    if op.op == "create_decision":
        # Decisions repo has `create` that matches our kwargs.
        row = await decisions_svc.create(
            title=ent["title"],
            decision_text=ent["decision_text"],
            rationale=ent.get("rationale"),
            scope=ent.get("scope"),
            revisit_triggers=ent.get("revisit_triggers"),
            created_by_event_id=_coerce_uuid(
                ent.get("created_by_event_id") or cause_event_id
            ),
            tenant_id=tenant_id,
            conn=conn,
        )
        return _act_result({"op": "create_decision", "decision_id": str(row.id)}, 1)

    if op.op == "transition_decision":
        did = _coerce_uuid(ent["id"])
        row = await decisions_svc.transition(
            did,
            ent["new_state"],
            cause_event_id=cause_event_id,
            conn=conn,
        )
        return _act_result(
            {
                "op": "transition_decision",
                "decision_id": str(row.id),
                "new_state": ent["new_state"],
            },
            1,
        )
    return None


async def _apply_commitment_edge_act_op(
    op: ActOp,
    ent: dict[str, Any],
    conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    if op.op == "add_edge_contributes_to":
        await commitments_svc.add_edge(
            "contributes_to",
            commitment_id=_coerce_uuid(ent["commitment_id"]),
            goal_id=_coerce_uuid(ent["goal_id"]),
            is_critical_path=bool(ent.get("is_critical_path", False)),
            conn=conn,
        )
        return _act_result(
            {
                "op": "add_edge_contributes_to",
                "commitment_id": str(ent["commitment_id"]),
                "goal_id": str(ent["goal_id"]),
            },
            0,
        )

    if op.op == "add_edge_depends_on":
        await commitments_svc.add_edge(
            "depends_on",
            dependent_commitment_id=_coerce_uuid(ent["dependent_commitment_id"]),
            dependency_commitment_id=_coerce_uuid(ent["dependency_commitment_id"]),
            conn=conn,
        )
        return _act_result(
            {
                "op": "add_edge_depends_on",
                "dependent": str(ent["dependent_commitment_id"]),
                "dependency": str(ent["dependency_commitment_id"]),
            },
            0,
        )

    if op.op == "add_edge_constrained_by":
        await commitments_svc.add_edge(
            "constrained_by",
            commitment_id=_coerce_uuid(ent["commitment_id"]),
            decision_id=_coerce_uuid(ent["decision_id"]),
            conn=conn,
        )
        return _act_result(
            {
                "op": "add_edge_constrained_by",
                "commitment_id": str(ent["commitment_id"]),
                "decision_id": str(ent["decision_id"]),
            },
            0,
        )
    return None


async def _apply_act_op(
    op: ActOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    ent = op.entity or {}
    result = (
        await _apply_goal_act_op(
            op, ent, conn, tenant_id, cause_event_id=cause_event_id
        )
        or await _apply_commitment_act_op(
            op, ent, conn, tenant_id, cause_event_id=cause_event_id
        )
        or await _apply_decision_act_op(
            op, ent, conn, tenant_id, cause_event_id=cause_event_id
        )
        or await _apply_commitment_edge_act_op(op, ent, conn)
    )
    if result is not None:
        return result

    raise ValidationError(f"unknown act_op: {op.op!r}")


async def _apply_resource_op(
    op: ResourceOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    if op.op == "create":
        payload = op.payload or {}
        row = await resources_repo.create(
            kind=payload["kind"],
            identity=payload["identity"],
            description=payload.get("description"),
            current_value=payload.get("current_value", {}),
            utilization_state=payload.get("utilization_state", "available"),
            controllability=payload.get("controllability", "owned"),
            temporal_character=payload.get("temporal_character", "permanent"),
            valuation_confidence=float(payload.get("valuation_confidence", 1.0)),
            metadata=payload.get("metadata"),
            created_by_event_id=_coerce_uuid(
                payload.get("created_by_event_id") or cause_event_id
            ),
            tenant_id=tenant_id,
            conn=conn,
        )
        return {
            "summary": {"op": "create_resource", "resource_id": str(row.id)},
            "state_changes": 1,
        }

    if op.op == "update":
        row = await resources_repo.update_attributes(
            op.resource_id,  # type: ignore[arg-type]
            patch=op.patch,
            metadata_patch=(op.payload or {}).get("metadata_patch"),
            description=(op.payload or {}).get("description"),
            last_updated_by_event_id=_coerce_uuid(
                (op.payload or {}).get("last_updated_by_event_id") or cause_event_id
            ),
            conn=conn,
        )
        return {
            "summary": {"op": "update_resource", "resource_id": str(row.id)},
            "state_changes": 1,
        }

    if op.op == "transaction":
        row = await record_transaction(
            op.resource_id,  # type: ignore[arg-type]
            kind=op.kind,  # type: ignore[arg-type]
            delta=op.delta,  # type: ignore[arg-type]
            occurred_at=_coerce_dt((op.payload or {}).get("occurred_at"))
            or datetime.now(timezone.utc),
            source_event_id=_coerce_uuid(
                (op.payload or {}).get("source_event_id") or cause_event_id
            ),
            conn=conn,
        )
        return {
            "summary": {"op": "resource_transaction", "kind": op.kind},
            "state_changes": 1,
        }

    if op.op == "deploy":
        row = await deployments_svc.deploy(
            op.resource_id,  # type: ignore[arg-type]
            op.commitment_id,  # type: ignore[arg-type]
            quantity=op.quantity or {},
            started_at=_coerce_dt((op.payload or {}).get("started_at")),
            source_event_id=_coerce_uuid(
                (op.payload or {}).get("source_event_id") or cause_event_id
            ),
            conn=conn,
        )
        return {
            "summary": {
                "op": "deploy_resource",
                "resource_id": str(op.resource_id),
                "commitment_id": str(op.commitment_id),
            },
            "state_changes": 1,
        }

    if op.op == "release":
        row = await release_deployment(
            (op.resource_id, op.commitment_id),  # type: ignore[arg-type]
            released_at=_coerce_dt((op.payload or {}).get("released_at")),
            actual_quantity=op.actual_quantity,
            source_event_id=_coerce_uuid(
                (op.payload or {}).get("source_event_id") or cause_event_id
            ),
            conn=conn,
        )
        return {
            "summary": {
                "op": "release_resource",
                "resource_id": str(op.resource_id),
                "commitment_id": str(op.commitment_id),
            },
            "state_changes": 1,
        }

    raise ValidationError(f"unknown resource_op: {op.op!r}")


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------


def _coerce_uuid(v: Any) -> UUID | None:
    if v is None:
        return None
    if isinstance(v, UUID):
        return v
    try:
        return UUID(str(v))
    except (ValueError, TypeError) as e:
        raise ValidationError(f"expected UUID, got {v!r}: {e}")


def _coerce_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


__all__ = [
    "apply_diff",
    "hash_diff",
    "ApplierError",
    "AlreadyAppliedError",
]
