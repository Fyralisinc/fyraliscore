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
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from lib.shared.types import ModelCreate
from lib.shared.edge_registry import EdgeRegistryError
from services.domain.acts import commitments as commitments_svc
from services.domain.acts import decisions as decisions_svc
from services.domain.acts import goals as goals_svc
from services.domain.models.propositions import ensure_situation_compositional_defaults
from services.domain.models.propositions import canonicalize_proposition, validate_proposition
from services.domain.models.repo import ModelsRepo
from services.domain.observations.state_change import emit_state_change
from services.domain.resources import deployments as deployments_svc
from services.domain.resources import repo as resources_repo
from services.domain.resources.transactions import record_transaction
from services.domain.resources.deployments import release as release_deployment

from .diff_schema import ActOp, ClaimOp, EdgeOp, RawDiff, ResourceOp, ValidatedDiff
from .observability import log_dropped_op
from .quality_gate import QualityContext, QualityVerdict, apply_verdict, score_quality
from .splitter import split_compound_claim_op
from .synthesis_decision import summarize_synthesis_decisions
from .text_embedding import deterministic_text_embedding, is_zero_embedding


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
        "edge_ops": [op.model_dump(mode="json") for op in diff.edge_ops],
        "act_ops": [op.model_dump(mode="json") for op in diff.act_ops],
        "resource_ops": [op.model_dump(mode="json") for op in diff.resource_ops],
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------
# Main apply entry point
# ---------------------------------------------------------------------


async def apply_diff(
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
    trigger_kind: str,
    trigger_cause_event_id: UUID | None = None,
    *,
    models_repo: ModelsRepo | None = None,
    think_run_id: UUID | None = None,
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

    Idempotency: inserts into applied_triggers with outcome='pending'
    FIRST. Raises AlreadyAppliedError if the trigger_id already has a
    row — the caller handles that path. The INSERT is also guarded
    against UniqueViolationError so that a race between the pre-check
    and the insert (only possible when callers somehow bypass the region
    lock) still surfaces as AlreadyAppliedError, not as a raw asyncpg
    error.
    """
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

    _diff_entities = _touched_from_diff(diff)
    if _diff_entities:
        await _acquire_region_lock(conn, diff.tenant_id, _diff_entities)

    existing = await conn.fetchrow(
        "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
        diff.trigger_ref,
    )
    if existing is not None:
        raise AlreadyAppliedError(
            "trigger already applied",
            trigger_id=str(diff.trigger_ref),
            prior_outcome=existing["outcome"],
        )

    diff_hash = hash_diff(diff)
    try:
        await conn.execute(
            """
            INSERT INTO applied_triggers
              (trigger_id, tenant_id, applied_at, diff_hash, trigger_kind, outcome)
            VALUES ($1, $2, now(), $3, $4, 'pending')
            """,
            diff.trigger_ref,
            diff.tenant_id,
            diff_hash,
            trigger_kind,
        )
    except asyncpg.exceptions.UniqueViolationError as exc:
        raise AlreadyAppliedError(
            "trigger already applied (race)",
            trigger_id=str(diff.trigger_ref),
            prior_outcome="unknown",
        ) from exc

    applied_model_ids: list[UUID] = []
    state_changes_emitted = 0
    ops_summary: dict[str, Any] = {
        "claim_ops": [],
        "edge_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "synthesis_decisions": summarize_synthesis_decisions(diff),
        "diff_hash": diff_hash,
        "apply_dropped_op_count": 0,
        "apply_dropped_op_errors": [],
    }
    pending_model_ids_by_event_id: dict[UUID, UUID] = {}

    if models_repo is None:
        models_repo = ModelsRepo(pool=None)  # type: ignore[arg-type]

    # --- 1. claim_ops ---------------------------------------------
    _belief_updated_model_ids: list[UUID] = []
    _T2_BELIEF_KINDS = {"belief", "state", "concern", "expectation"}
    # T5: reconcile each claim_op.insert before applying. If the
    # reconciler decides auto_merge, we substitute the replacement
    # update op for the original insert. human_review and no_match
    # both proceed with the original (auditing the decision in
    # `reconciliation_events` is sufficient for those cases).
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
    split_summary: dict[str, int] = {
        "compound_inputs": 0,
        "atomic_outputs": 0,
        "synthesized_situations": 0,
    }

    # ---------- Splitter expansion ----------
    # Each compound op becomes a contiguous group of N atomic ops +
    # 1 synthesized situation. We track the group via `gid` so we
    # can patch member_model_ids on the situation after its atomics
    # commit. Non-compound inputs pass through with `gid=None`.
    expanded_ops: list[tuple[ClaimOp, ClaimOp, int | None]] = []
    next_gid = 0
    for src_op in diff.claim_ops:
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
        for s in splits:
            expanded_ops.append((src_op, s, gid))

    # Track atomic model IDs per split group for situation member patching.
    group_member_ids: dict[int, list[UUID]] = {}

    for original_op, expanded_op, gid in expanded_ops:
        op = expanded_op
        recon_result = None
        verdict = None

        # Pending-situation handling: patch member_model_ids using the
        # atomic IDs from this group before any further processing.
        is_pending_situation = (
            op.op == "insert"
            and isinstance(op.entry, dict)
            and op.entry.get("member_model_pending") is True
        )
        if is_pending_situation:
            members = group_member_ids.get(gid, []) if gid is not None else []
            if not members:
                # All atomic members were dropped (rejected/downgraded).
                # Skip the situation rather than emit an empty composite.
                ops_summary["claim_ops"].append({
                    "op": "skip",
                    "reason": "situation_skipped_no_atomic_members_after_quality_gate",
                    "split_group_id": gid,
                })
                continue
            prop = op.entry.get("proposition") or {}
            deduped_members: list[UUID] = []
            seen_members: set[UUID] = set()
            for uid in members:
                if uid in seen_members:
                    continue
                seen_members.add(uid)
                deduped_members.append(uid)
            if len(deduped_members) < 2:
                ops_summary["claim_ops"].append({
                    "op": "skip",
                    "reason": "situation_skipped_insufficient_atomic_members_after_quality_gate",
                    "split_group_id": gid,
                    "member_count": len(deduped_members),
                })
                continue
            prop["member_model_ids"] = [str(uid) for uid in deduped_members]
            op.entry["proposition"] = prop
            # Strip splitter-only audit markers — ModelCreate forbids extras.
            op.entry.pop("member_model_pending", None)
            op.entry.pop("split_reasons", None)

        if op.op == "insert":
            recon_result = await reconcile_claim_op(
                op, conn,
                tenant_id=diff.tenant_id,
                trigger_id=diff.trigger_ref,
                think_run_id=think_run_id,
            )
            reconcile_summary[recon_result.decision] += 1
            if recon_result.replacement_op is not None:
                # auto_merge: substitute the confidence-update op and
                # skip the quality gate (confidence updates against an
                # existing model do not need re-scoring).
                op = recon_result.replacement_op
            else:
                # Fresh insert path: run the quality gate.
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
                if op_after_verdict is None:
                    if verdict.decision == "downgrade_to_evidence":
                        result = await _apply_evidence_downgrade(
                            op,
                            conn,
                            tenant_id=diff.tenant_id,
                            cause_event_id=trigger_cause_event_id,
                            verdict=verdict,
                            preferred_model_id=(
                                recon_result.matched_model_id
                                if recon_result is not None
                                else None
                            ),
                        )
                        if gid is not None:
                            result["summary"]["split_group_id"] = gid
                        ops_summary["claim_ops"].append(result["summary"])
                        state_changes_emitted += result.get("state_changes", 0)
                        continue
                    # rejected or downgraded — record and skip apply.
                    ops_summary["claim_ops"].append({
                        "op": "skip",
                        "reason": f"quality_gate_{verdict.decision}",
                        "quality_verdict": {
                            "decision": verdict.decision,
                            "atomicity": verdict.atomicity_score,
                            "durability": verdict.durability_score,
                            "kind_fit": verdict.kind_fit_score,
                            "overall": verdict.overall_score,
                            "rejection_reasons": verdict.rejection_reasons,
                        },
                        "split_group_id": gid,
                    })
                    continue
                op = op_after_verdict
                # side_ops currently empty (evidence path unwired); future
                # evidence emission would loop them through _apply_claim_op.

        if gid is None and _should_absorb_near_duplicate(op, recon_result, verdict):
            result = await _apply_near_duplicate_absorption(
                op,
                conn,
                tenant_id=diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
                verdict=verdict,
                recon_result=recon_result,
            )
            if gid is not None:
                result["summary"]["split_group_id"] = gid
            ops_summary["claim_ops"].append(result["summary"])
            state_changes_emitted += result.get("state_changes", 0)
            continue

        if op.op == "insert" and _entry_is_situation(op.entry):
            result = await _coalesce_same_event_situation_insert(
                op,
                conn,
                tenant_id=diff.tenant_id,
                cause_event_id=trigger_cause_event_id,
            )
            if result is not None:
                if recon_result is not None and recon_result.decision != "skipped":
                    result["summary"]["reconcile_decision"] = recon_result.decision
                if verdict is not None:
                    result["summary"]["quality_decision"] = verdict.decision
                    result["summary"]["quality_overall"] = verdict.overall_score
                if gid is not None:
                    result["summary"]["split_group_id"] = gid
                ops_summary["claim_ops"].append(result["summary"])
                if result.get("model_id") is not None:
                    applied_model_ids.append(result["model_id"])
                state_changes_emitted += result.get("state_changes", 0)
                continue

        # When the reconciler converted an insert into an update, the
        # audit chain should record the transition as
        # 'reconciliation_merge' rather than the default 'field_update'
        # / 'confidence_update'. Thread the override down.
        is_recon_merge = (
            recon_result is not None
            and recon_result.decision == "auto_merge"
            and recon_result.replacement_op is not None
        )
        result = await _apply_claim_op(
            op, conn, models_repo, diff.tenant_id,
            cause_event_id=trigger_cause_event_id,
            audit_cause_override=(
                "reconciliation_merge" if is_recon_merge else None
            ),
        )
        # Annotate the per-op summary with reconcile + quality context.
        if recon_result is not None and recon_result.decision != "skipped":
            result["summary"]["reconcile_decision"] = recon_result.decision
            if recon_result.matched_model_id is not None:
                result["summary"]["reconcile_matched_model_id"] = (
                    str(recon_result.matched_model_id)
                )
            if recon_result.cosine_similarity is not None:
                result["summary"]["reconcile_cosine"] = (
                    recon_result.cosine_similarity
                )
        if verdict is not None:
            result["summary"]["quality_decision"] = verdict.decision
            result["summary"]["quality_overall"] = verdict.overall_score
        if gid is not None:
            result["summary"]["split_group_id"] = gid
        ops_summary["claim_ops"].append(result["summary"])
        if result.get("model_id") is not None:
            applied_model_ids.append(result["model_id"])
            # Track this atomic ID for situation member patching.
            if gid is not None and not is_pending_situation:
                group_member_ids.setdefault(gid, []).append(result["model_id"])
            if original_op.op == "insert" and isinstance(original_op.entry, dict):
                for key in ("born_from_event_id", "model_id", "id"):
                    placeholder_id = _coerce_uuid(original_op.entry.get(key))
                    if placeholder_id is not None:
                        pending_model_ids_by_event_id[placeholder_id] = (
                            result["model_id"]
                        )
            if (
                op.op == "insert"
                and result["summary"].get("proposition_kind") in _T2_BELIEF_KINDS
            ):
                _belief_updated_model_ids.append(result["model_id"])
        state_changes_emitted += result.get("state_changes", 0)
    ops_summary["reconcile_summary"] = reconcile_summary
    ops_summary["quality_summary"] = quality_summary
    ops_summary["split_summary"] = split_summary

    # --- 2. edge_ops ----------------------------------------------
    for op in diff.edge_ops:
        op = _resolve_pending_edge_model_refs(op, pending_model_ids_by_event_id)
        if op.source_model_id == op.target_model_id:
            ops_summary["edge_ops"].append({
                "op": "skip",
                "edge_kind": op.edge_kind,
                "source_model_id": str(op.source_model_id),
                "target_model_id": str(op.target_model_id),
                "reason": "resolved_to_same_model_after_reconciliation",
            })
            continue
        try:
            result = await _apply_edge_op(
                op, conn, diff.tenant_id,
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
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["edge_ops"].append({
                "op": "skip",
                "edge_kind": op.edge_kind,
                "source_model_id": str(op.source_model_id),
                "target_model_id": str(op.target_model_id),
                "reason": reason,
                "message": message,
            })
            continue
        ops_summary["edge_ops"].append(result["summary"])

    # --- 3. act_ops -----------------------------------------------
    for op in diff.act_ops:
        if op.confidence_basis in pending_model_ids_by_event_id:
            op = op.model_copy(
                update={
                    "confidence_basis": pending_model_ids_by_event_id[
                        op.confidence_basis
                    ]
                }
            )
        try:
            result = await _apply_act_op(
                op, conn, diff.tenant_id,
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
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["act_ops"].append({
                "op": "skip",
                "act_op": op.op,
                "reason": reason,
                "message": message,
            })
            continue
        ops_summary["act_ops"].append(result["summary"])
        state_changes_emitted += result.get("state_changes", 0)

    # --- 4. resource_ops ------------------------------------------
    for op in diff.resource_ops:
        try:
            result = await _apply_resource_op(
                op, conn, diff.tenant_id,
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
            ops_summary["apply_dropped_op_count"] += 1
            ops_summary["apply_dropped_op_errors"].append(message)
            ops_summary["resource_ops"].append({
                "op": "skip",
                "resource_op": op.op,
                "reason": reason,
                "message": message,
            })
            continue
        ops_summary["resource_ops"].append(result["summary"])
        state_changes_emitted += result.get("state_changes", 0)

    # --- 5. Enqueue T2:belief_updated for each new state/concern model ----
    if _belief_updated_model_ids:
        from services.reasoning.think.cascade import enqueue_t2_belief_updated
        for mid in _belief_updated_model_ids:
            await enqueue_t2_belief_updated(
                conn,
                tenant_id=diff.tenant_id,
                model_id=mid,
                source_observation_id=trigger_cause_event_id,
            )

    # --- 6. Mark applied_triggers success (still in same tx) ------
    ops_summary["memory_aggregation"] = _summarize_memory_aggregation(
        ops_summary,
        original_claim_op_count=len(diff.claim_ops),
        expanded_claim_op_count=len(expanded_ops),
    )

    await conn.execute(
        "UPDATE applied_triggers SET outcome = 'success' WHERE trigger_id = $1",
        diff.trigger_ref,
    )

    return {
        **ops_summary,
        "applied_model_ids": applied_model_ids,
        "state_changes_emitted": state_changes_emitted,
        "reasoning_trace": diff.reasoning_trace,
    }


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
    "contributing_models",
    "supporting_event_ids",
    "supporting_model_ids",
}


_EVIDENCE_TOKEN_STOPWORDS = {
    "about", "after", "also", "and", "are", "because", "been", "but",
    "call", "case", "customer", "from", "has", "have", "into", "now",
    "that", "the", "their", "this", "with", "without",
}


async def _apply_evidence_downgrade(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    cause_event_id: UUID | None,
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
    source_event_id = (
        _coerce_uuid_or_none(entry.get("born_from_event_id"))
        or cause_event_id
    )
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
        uid for raw in (entry.get("scope_actors") or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    ]
    scope_entities = [
        ent for ent in (entry.get("scope_entities") or [])
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
            part for part in (
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
            entry.get("confidence", entry.get("confidence_at_assertion", 0.5))
            or 0.5
        ),
        "natural": text[:500],
        "quality_decision": verdict.decision,
    }
    existing_readings = _json_list(row["signal_readings"])
    existing_readings.append(reading)

    supporting_event_ids = [
        uid for raw in (row["supporting_event_ids"] or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    ]
    if source_event_id is not None and source_event_id not in supporting_event_ids:
        supporting_event_ids.append(source_event_id)

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
        supporting_event_ids,
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
            "supporting_event_ids": [str(uid) for uid in supporting_event_ids],
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
            ent for ent in (op.entry.get("scope_entities") or [])
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
    source_event_id = (
        _coerce_uuid_or_none(entry.get("born_from_event_id"))
        or cause_event_id
    )
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
    reading = {
        "kind": "confirm",
        "at": now.isoformat(),
        "source_event_id": str(source_event_id) if source_event_id else None,
        "confidence": float(
            entry.get("confidence", entry.get("confidence_at_assertion", 0.5))
            or 0.5
        ),
        "natural": text[:500],
        "reconcile_decision": getattr(recon_result, "decision", None),
        "reconcile_cosine": getattr(recon_result, "cosine_similarity", None),
        "quality_decision": verdict.decision if verdict is not None else None,
    }
    existing_readings = _json_list(row["signal_readings"])
    existing_readings.append(reading)
    supporting_event_ids = [
        uid for raw in (row["supporting_event_ids"] or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    ]
    if source_event_id is not None and source_event_id not in supporting_event_ids:
        supporting_event_ids.append(source_event_id)
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
            _quality_verdict_summary(verdict)
            if verdict is not None
            else None
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
            "quality_overall": (
                verdict.overall_score if verdict is not None else None
            ),
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
            "assertion", "summary", "claim", "nature", "event",
            "assessment", "hypothesis_text", "observed_tendency",
            "situation", "relationship_summary", "expected",
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
        item for item in ops_summary.get("claim_ops", [])
        if isinstance(item, dict)
    ]
    inserts = [item for item in claim_ops if item.get("op") == "insert"]
    updates = [item for item in claim_ops if item.get("op") == "update"]
    archives = [item for item in claim_ops if item.get("op") == "archive"]
    situation_updates = [
        item for item in updates
        if item.get("internal_situation_merge") is True
    ]
    evidence_attachments = [
        item for item in claim_ops
        if item.get("op") == "downgrade_to_evidence"
        and item.get("decision") == "attached_to_existing_model"
    ]
    near_duplicate_absorptions = [
        item for item in claim_ops
        if item.get("op") == "absorb_near_duplicate"
        and item.get("decision") == "attached_to_matched_model"
    ]
    skipped = [item for item in claim_ops if item.get("op") == "skip"]
    situations = [
        item for item in inserts
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
            int(item.get("situation_members_added") or 0)
            for item in situation_updates
        ),
        "model_updates": len(updates),
        "model_archives": len(archives),
        "evidence_attachments": len(evidence_attachments),
        "near_duplicate_absorptions": len(near_duplicate_absorptions),
        "skipped_claim_writes": len(skipped),
        "edge_ops": len(ops_summary.get("edge_ops") or []),
        "act_ops": len(ops_summary.get("act_ops") or []),
        "resource_ops": len(ops_summary.get("resource_ops") or []),
        "new_model_pressure": (
            len(inserts) / expanded if expanded else 0.0
        ),
        "absorption_ratio": (
            non_insert_absorptions / expanded if expanded else 1.0
        ),
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
    audit_cause_override: str | None,
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
        str(tag) for tag in (payload.get("candidate_domain_tags") or [])
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
    supporting_event_ids = [
        uid for raw in (row["supporting_event_ids"] or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    ]
    if cause_event_id is not None and cause_event_id not in supporting_event_ids:
        supporting_event_ids.append(cause_event_id)
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
        str(raw) for raw in (payload.get("added_member_model_ids") or [])
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
) -> dict[str, Any] | None:
    entry = dict(op.entry or {})
    source_event_id = (
        _coerce_uuid_or_none(entry.get("born_from_event_id"))
        or cause_event_id
    )
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

    from .reconciler import _build_situation_merge_payload

    payload = _build_situation_merge_payload(
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
            ent for ent in (entry.get("scope_entities") or [])
            if isinstance(ent, dict)
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
            part for part in (
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
        if (
            candidate_pressure
            and row_prop.get("pressure_type") == candidate_pressure
        ):
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
        uid for raw in (left_actors or [])
        if (uid := _coerce_uuid_or_none(raw)) is not None
    }
    right_actor_ids = {
        uid for raw in (right_actors or [])
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
    return bool(left_entity_keys and right_entity_keys and left_entity_keys & right_entity_keys)


async def _apply_claim_op(
    op: ClaimOp,
    conn: asyncpg.Connection,
    models_repo: ModelsRepo,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
    audit_cause_override: str | None = None,
) -> dict[str, Any]:
    if op.op == "insert":
        entry = dict(op.entry or {})
        # Ensure required ModelCreate fields.
        entry.setdefault("tenant_id", tenant_id)
        # Backfill confidence_at_assertion if missing.
        entry.setdefault("confidence_at_assertion", entry.get("confidence", 0.5))
        # Backfill born_from_event_id from the triggering observation if the
        # LLM didn't echo it (the prompt asks the LLM to populate it, but
        # DeepSeek/OpenAI providers sometimes drop it). We also strip
        # LLM-invented fields that aren't part of ModelCreate.
        if "born_from_event_id" not in entry and cause_event_id is not None:
            entry["born_from_event_id"] = cause_event_id
        for stray in ("title", "description", "id", "model_id"):
            entry.pop(stray, None)
        ensure_situation_compositional_defaults(entry)
        # scope_temporal is required; default to an open-ended present window.
        if "scope_temporal" not in entry:
            entry["scope_temporal"] = {
                "valid_from": datetime.now(timezone.utc).isoformat(),
                "valid_until": None,
            }
        # Never insert an active Model with a zero embedding. If the LLM did
        # not provide one, use a deterministic lexical fallback so semantic
        # retrieval/reconciliation/topology have a usable anchor until a
        # production embedding backfill refreshes it.
        if "embedding" not in entry or is_zero_embedding(entry.get("embedding")):
            entry["embedding"] = deterministic_text_embedding(
                str(entry.get("natural") or entry.get("proposition") or "")
            )
        proposed = ModelCreate.model_validate(entry)
        row = await models_repo.insert(
            proposed,
            conn=conn,
            apply_confidence_calibration=False,
        )
        return {
            "summary": {
                "op": "insert",
                "model_id": str(row.id),
                "confidence": row.confidence,
                "proposition_kind": row.proposition_kind,
                "claim_role": row.claim_role,
                "abstraction_level": row.abstraction_level,
                "domain_tags": list(row.domain_tags or []),
            },
            "model_id": row.id,
            "state_changes": 1,  # insert emits a state_change
        }
    if op.op == "update":
        if op.model_id is None or not op.changes:
            raise ValidationError("apply_claim_op update: bad op")
        raw_changes = dict(op.changes)
        situation_merge_payload = None
        if audit_cause_override == "reconciliation_merge":
            maybe_payload = raw_changes.pop("__situation_merge", None)
            if isinstance(maybe_payload, dict):
                situation_merge_payload = maybe_payload
        changes = {
            k: v for k, v in raw_changes.items()
            if k in _ALLOWED_MODEL_UPDATE_COLUMNS
        }
        if not changes and situation_merge_payload is None:
            raise ValidationError("apply_claim_op update: no allowed columns")
        emitted = 0
        changed_fields_for_summary = set(changes.keys())
        if "confidence" in changes:
            # bulk path handles emit_state_change + audit cleanly. Pass
            # the audit override so a reconciler-substituted update is
            # recorded as 'reconciliation_merge' rather than the default
            # 'confidence_update'.
            await models_repo.bulk_confidence_update(
                {op.model_id: float(changes["confidence"])},
                cause_event_id=cause_event_id,
                audit_cause_override=audit_cause_override,
                conn=conn,
            )
            changes.pop("confidence")
            emitted = 1
        # For remaining columns, build an UPDATE + emit a state_change
        # + emit an audit_events row. We snapshot the touched columns
        # before and after so the audit chain captures the diff.
        if changes:
            from .audit import (
                CAUSE_FIELD_UPDATE,
                emit_audit_event,
            )

            # Snapshot pre-update values for the touched columns. None
            # of the _ALLOWED_MODEL_UPDATE_COLUMNS are SQL-reserved.
            cols_csv = ", ".join(changes.keys())
            pre_snapshot: dict[str, Any] = {}
            pre_row = await conn.fetchrow(
                f"SELECT {cols_csv} FROM models WHERE id = $1",
                op.model_id,
            )
            if pre_row is not None:
                for k in changes.keys():
                    pre_snapshot[k] = _audit_jsonable(pre_row[k])
            previous_signal_readings = (
                pre_row["signal_readings"]
                if pre_row is not None and "signal_readings" in changes
                else None
            )

            set_clauses = []
            params: list[Any] = []
            i = 1
            for k, v in changes.items():
                # JSONB columns: pass a JSON string with ::jsonb cast.
                if k in (
                    "signal_readings",
                ):
                    set_clauses.append(f"{k} = ${i}::jsonb")
                    params.append(json.dumps(v, default=str))
                elif k in (
                    "supporting_event_ids",
                    "supporting_model_ids",
                    "contributing_models",
                ):
                    set_clauses.append(f"{k} = ${i}::uuid[]")
                    params.append(list(v) if isinstance(v, (list, tuple)) else [v])
                else:
                    set_clauses.append(f"{k} = ${i}")
                    params.append(v)
                i += 1
            params.append(op.model_id)
            sql = (
                f"UPDATE models SET {', '.join(set_clauses)} "
                f"WHERE id = ${i}"
            )
            await conn.execute(sql, *params)

            # S1 dual-write: mirror array changes to typed edges via
            # the chokepoint helper. update_arrays=False because the
            # UPDATE above already set the array columns; we just
            # need to converge the typed edges with the new state.
            # `instance_of` is not exposed as an LLM-controlled column
            # — pattern back-links go through promote_pattern_candidate
            # — so we only sync supports / contributes_to_resolution.
            if (
                "supporting_model_ids" in changes
                or "contributing_models" in changes
            ):
                from services.domain.models.repo import _set_model_relations

                await _set_model_relations(
                    conn,
                    model_id=op.model_id,
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

            await emit_state_change(
                conn,
                kind="model_updated",
                entity_id=op.model_id,
                tenant_id=tenant_id,
                cause_event_id=cause_event_id,
                entity_kind="model",
                metadata={"columns": sorted(list(changes.keys()))},
            )

            # Audit event: partial snapshots of just the touched fields.
            new_state = {k: _audit_jsonable(v) for k, v in changes.items()}
            await emit_audit_event(
                conn,
                model_id=op.model_id,
                tenant_id=tenant_id,
                cause_type=audit_cause_override or CAUSE_FIELD_UPDATE,
                new_state=new_state,
                previous_state=pre_snapshot or None,
                cause_id=cause_event_id,
                changed_fields=sorted(list(changes.keys())),
            )
            emitted += 1
            if "signal_readings" in changes:
                await _append_signal_readings_sidecar_delta(
                    conn,
                    tenant_id=tenant_id,
                    model_id=op.model_id,
                    previous_readings=previous_signal_readings,
                    new_readings=changes["signal_readings"],
                )
        situation_merge_summary: dict[str, Any] | None = None
        if situation_merge_payload is not None:
            merge_result = await _apply_situation_merge_payload(
                conn,
                tenant_id=tenant_id,
                model_id=op.model_id,
                payload=situation_merge_payload,
                cause_event_id=cause_event_id,
                audit_cause_override=audit_cause_override,
            )
            emitted += merge_result["state_changes"]
            situation_merge_summary = merge_result["summary"]
            changed_fields_for_summary.update(
                ("proposition", "domain_tags", "model_composition_members")
            )
        return {
            "summary": {
                "op": "update",
                "model_id": str(op.model_id),
                "changed": sorted(changed_fields_for_summary),
                **({"claim_role": "situation"} if situation_merge_summary else {}),
                **(
                    {
                        "internal_situation_merge": True,
                        "situation_members_added": situation_merge_summary[
                            "situation_members_added"
                        ],
                    }
                    if situation_merge_summary
                    else {}
                ),
            },
            "model_id": op.model_id,
            "state_changes": emitted,
        }
    if op.op == "archive":
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
    raise ValidationError(f"unknown claim_op: {op.op!r}")


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
            evidence_event_ids=op.evidence_event_ids,
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


async def _apply_act_op(
    op: ActOp,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    cause_event_id: UUID | None,
) -> dict[str, Any]:
    ent = op.entity or {}

    if op.op == "create_goal":
        row = await goals_svc.create(
            title=ent["title"],
            description=ent.get("description"),
            parent_goal_id=_coerce_uuid(ent.get("parent_goal_id")),
            altitude=ent.get("altitude", "operational"),
            success_criteria=ent.get("success_criteria"),
            target_date=_coerce_dt(ent.get("target_date")),
            created_by_event_id=_coerce_uuid(ent.get("created_by_event_id") or cause_event_id),
            tenant_id=tenant_id,
            conn=conn,
        )
        return {
            "summary": {"op": "create_goal", "goal_id": str(row.id)},
            "state_changes": 1,
        }

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
            set_clauses.append(f"cached_health_computed_at = now()")
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
        return {
            "summary": {"op": "update_goal", "goal_id": str(gid)},
            "state_changes": 1,
        }

    if op.op == "transition_goal":
        gid = _coerce_uuid(ent["id"])
        row = await goals_svc.transition(
            gid,
            ent["new_state"],
            cause_event_id=cause_event_id,
            conn=conn,
        )
        return {
            "summary": {
                "op": "transition_goal",
                "goal_id": str(row.id),
                "new_state": ent["new_state"],
            },
            "state_changes": 1,
        }

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
                _coerce_uuid(x) if not isinstance(x, (list, tuple))
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
            created_by_event_id=_coerce_uuid(ent.get("created_by_event_id") or cause_event_id),
            last_confidence_basis=op.confidence_basis,
            tenant_id=tenant_id,
            conn=conn,
        )
        return {
            "summary": {"op": "create_commitment", "commitment_id": str(row.id)},
            "state_changes": 1,
        }

    if op.op == "transition_commitment":
        cid = _coerce_uuid(ent["id"])
        resolved = [
            _coerce_uuid(x) for x in (ent.get("resolved_by_event_ids") or [])
        ]
        row = await commitments_svc.transition(
            cid,
            ent["new_state"],
            resolved_by_event_ids=resolved or None,
            last_confidence_basis=op.confidence_basis,
            cause_event_id=cause_event_id or _coerce_uuid(ent.get("cause_event_id")),
            conn=conn,
        )
        return {
            "summary": {
                "op": "transition_commitment",
                "commitment_id": str(row.id),
                "new_state": ent["new_state"],
            },
            "state_changes": 1,
        }

    if op.op == "create_decision":
        # Decisions repo has `create` that matches our kwargs.
        row = await decisions_svc.create(
            title=ent["title"],
            decision_text=ent["decision_text"],
            rationale=ent.get("rationale"),
            scope=ent.get("scope"),
            revisit_triggers=ent.get("revisit_triggers"),
            created_by_event_id=_coerce_uuid(ent.get("created_by_event_id") or cause_event_id),
            tenant_id=tenant_id,
            conn=conn,
        )
        return {
            "summary": {"op": "create_decision", "decision_id": str(row.id)},
            "state_changes": 1,
        }

    if op.op == "transition_decision":
        did = _coerce_uuid(ent["id"])
        row = await decisions_svc.transition(
            did,
            ent["new_state"],
            cause_event_id=cause_event_id,
            conn=conn,
        )
        return {
            "summary": {
                "op": "transition_decision",
                "decision_id": str(row.id),
                "new_state": ent["new_state"],
            },
            "state_changes": 1,
        }

    if op.op == "add_edge_contributes_to":
        row = await commitments_svc.add_edge(
            "contributes_to",
            commitment_id=_coerce_uuid(ent["commitment_id"]),
            goal_id=_coerce_uuid(ent["goal_id"]),
            is_critical_path=bool(ent.get("is_critical_path", False)),
            conn=conn,
        )
        return {
            "summary": {
                "op": "add_edge_contributes_to",
                "commitment_id": str(ent["commitment_id"]),
                "goal_id": str(ent["goal_id"]),
            },
            "state_changes": 0,
        }

    if op.op == "add_edge_depends_on":
        row = await commitments_svc.add_edge(
            "depends_on",
            dependent_commitment_id=_coerce_uuid(ent["dependent_commitment_id"]),
            dependency_commitment_id=_coerce_uuid(ent["dependency_commitment_id"]),
            conn=conn,
        )
        return {
            "summary": {
                "op": "add_edge_depends_on",
                "dependent": str(ent["dependent_commitment_id"]),
                "dependency": str(ent["dependency_commitment_id"]),
            },
            "state_changes": 0,
        }

    if op.op == "add_edge_constrained_by":
        row = await commitments_svc.add_edge(
            "constrained_by",
            commitment_id=_coerce_uuid(ent["commitment_id"]),
            decision_id=_coerce_uuid(ent["decision_id"]),
            conn=conn,
        )
        return {
            "summary": {
                "op": "add_edge_constrained_by",
                "commitment_id": str(ent["commitment_id"]),
                "decision_id": str(ent["decision_id"]),
            },
            "state_changes": 0,
        }

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
            kind=op.kind,    # type: ignore[arg-type]
            delta=op.delta,  # type: ignore[arg-type]
            occurred_at=_coerce_dt((op.payload or {}).get("occurred_at")) or datetime.now(timezone.utc),
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
