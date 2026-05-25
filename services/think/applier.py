"""services/think/applier.py — diff application inside a transaction.

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
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from lib.shared.edge_registry import EdgeRegistryError
from services.acts import commitments as commitments_svc
from services.acts import decisions as decisions_svc
from services.acts import goals as goals_svc
from services.models.repo import ModelsRepo
from services.observations.state_change import emit_state_change
from services.resources import deployments as deployments_svc
from services.resources import repo as resources_repo
from services.resources.transactions import record_transaction
from services.resources.deployments import release as release_deployment

from .diff_schema import ActOp, ClaimOp, EdgeOp, RawDiff, ResourceOp, ValidatedDiff
from .observability import log_dropped_op
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
        "diff_hash": diff_hash,
        "apply_dropped_op_count": 0,
        "apply_dropped_op_errors": [],
    }
    pending_model_ids_by_event_id: dict[UUID, UUID] = {}

    if models_repo is None:
        models_repo = ModelsRepo(pool=None)  # type: ignore[arg-type]

    # --- 1. claim_ops ---------------------------------------------
    _belief_updated_model_ids: list[UUID] = []
    _T2_BELIEF_KINDS = {"state", "concern", "expectation"}
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
    for original_op in diff.claim_ops:
        op = original_op
        recon_result = None
        if op.op == "insert":
            recon_result = await reconcile_claim_op(
                op, conn,
                tenant_id=diff.tenant_id,
                trigger_id=diff.trigger_ref,
                think_run_id=think_run_id,
            )
            reconcile_summary[recon_result.decision] += 1
            if recon_result.replacement_op is not None:
                op = recon_result.replacement_op
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
        # Annotate the per-op summary with reconcile context so callers
        # and tests can see what the reconciler decided.
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
        ops_summary["claim_ops"].append(result["summary"])
        if result.get("model_id") is not None:
            applied_model_ids.append(result["model_id"])
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
        from services.think.cascade import enqueue_t2_belief_updated
        for mid in _belief_updated_model_ids:
            await enqueue_t2_belief_updated(
                conn,
                tenant_id=diff.tenant_id,
                model_id=mid,
                source_observation_id=trigger_cause_event_id,
            )

    # --- 6. Mark applied_triggers success (still in same tx) ------
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
            },
            "model_id": row.id,
            "state_changes": 1,  # insert emits a state_change
        }
    if op.op == "update":
        if op.model_id is None or not op.changes:
            raise ValidationError("apply_claim_op update: bad op")
        changes = {
            k: v for k, v in op.changes.items()
            if k in _ALLOWED_MODEL_UPDATE_COLUMNS
        }
        if not changes:
            raise ValidationError("apply_claim_op update: no allowed columns")
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
        else:
            emitted = 0
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
                from services.models.repo import _set_model_relations

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
        return {
            "summary": {
                "op": "update",
                "model_id": str(op.model_id),
                "changed": sorted(list(op.changes.keys())),
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
    from services.models.edges_repo import EdgesRepo

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
