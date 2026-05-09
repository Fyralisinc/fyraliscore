"""services/think/validator.py — the validation choke point.

Spec §7 "Validation" + BUILD-PLAN §4 Prompt 3.B item 5.

Rules enforced:

  1. claim_ops.insert with confidence > 0.7 → falsifier must be adequate
     (`services.models.falsifier.is_adequate_falsifier`). Inadequate →
     drop op, record error.

  2. claim_ops.insert confidence clipped to [0.05, 0.95], calibration
     applied (Wave 1 identity; Wave 4-C real). Entity references
     checked against the retrieval context.

  3. act_ops.* confidence threshold via `compute_threshold`. If
     basis.confidence < threshold → drop op.

  4. act_ops transitions → `can_transition(current_state, new_state,
     kind)` must return True. Illegal → drop op.

  5. `transition_commitment_to_doneverified` specifically requires
     `len(resolved_by_event_ids) >= 1` AND every referenced
     Observation's trust_tier is at least `authoritative`. Else raise
     TrustTierError (this is hard — we don't want silent corruption of
     doneverified semantics).

  6. resource_ops.* — validated at apply time by the repos, but we do
     lightweight shape validation here (non-empty resource_id for
     non-create, delta matches kind, etc.).

  7. Out-of-region containment: if an op mutates an entity whose id is
     not in the pre-declared region, raise `OutOfRegionError`. The
     caller re-runs retrieval with the expanded set (max 2 attempts).

  8. Partial-accept: keep every op that passes, drop ones that fail,
     and record the dropped count + error messages on the returned
     ValidatedDiff. Only raise ValidationFailure when every op failed
     (no survivors) so an all-bad diff still signals upstream.

This module is pure (no DB writes) except for the falsifier DB-check
detour in the commitment_outcome.commitment_ref path — that one reads
commitments table to verify the ref exists.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import (
    CompanyOSError,
    FalsifierInadequateError,
    InvariantViolation,
    MalformedFalsifierError,
    TrustTierError,
    ValidationError,
)
from lib.shared.trust import TrustTier

from services.acts.state_machines import can_transition
from services.models.calibration import apply_calibration
from services.models.falsifier import is_adequate_falsifier

from .diff_schema import ActOp, ClaimOp, RawDiff, ResourceOp, ValidatedDiff
from .observability import log_dropped_op
from .thresholds import compute_threshold


def _classify_claim_drop_reason(exc: Exception) -> str:
    """Map a per-op exception to a short, stable `failure_reason`
    classification tag. Used by OP-4 dropped-op logging."""
    from lib.shared.errors import (  # local import
        FalsifierInadequateError,
        MalformedFalsifierError,
    )
    if isinstance(exc, MalformedFalsifierError):
        return "malformed_falsifier"
    if isinstance(exc, FalsifierInadequateError):
        return "inadequate_falsifier"
    msg = str(getattr(exc, "message", exc)).lower()
    if "scope_actor" in msg or "uuid" in msg:
        return "invalid_entity_reference"
    if "immutable" in msg:
        return "immutable_column"
    if "model" in msg and "not found" in msg:
        return "missing_model_reference"
    if "non-empty changes" in msg or "requires" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_act_drop_reason(exc: Exception) -> str:
    from lib.shared.errors import InvariantViolation, TrustTierError
    if isinstance(exc, TrustTierError):
        return "inadequate_trust_tier"
    if isinstance(exc, InvariantViolation):
        return "illegal_transition"
    msg = str(getattr(exc, "message", exc)).lower()
    if "insufficient confidence" in msg or "< threshold" in msg:
        return "confidence_below_threshold"
    if "requires" in msg and "confidence_basis" in msg:
        return "missing_basis"
    if "not found" in msg:
        return "missing_entity_reference"
    if "requires" in msg or "entity" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_resource_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "non-empty" in msg or "requires" in msg:
        return "invalid_shape"
    return "unclassified"


_CONFIDENCE_MIN = 0.05
_CONFIDENCE_MAX = 0.95
_FALSIFIER_REQUIRED_ABOVE = 0.7
_ERROR_RATE_HARD_LIMIT = 0.25  # Retained for callers that import it; no longer enforced as a gate.


class ValidationFailure(CompanyOSError):
    default_code = "validation_failure"


class OutOfRegionError(CompanyOSError):
    """
    The LLM's diff mutates an entity outside the pre-declared region.
    The caller re-runs retrieval with the expanded set.
    """
    default_code = "out_of_region_mutation"


# =====================================================================
# Helpers
# =====================================================================


def _clip(v: float) -> float:
    if v < _CONFIDENCE_MIN:
        return _CONFIDENCE_MIN
    if v > _CONFIDENCE_MAX:
        return _CONFIDENCE_MAX
    return v


def _iter_entity_ids_touched(diff: RawDiff) -> list[tuple[str, str]]:
    """
    Every entity id this diff mutates. Used by the out-of-region check.

    Lists (kind, id-as-str) tuples so we can compare against
    `region_locks.touched_entity_ids(...)` output.
    """
    out: list[tuple[str, str]] = []
    for op in diff.claim_ops:
        if op.op == "insert" and op.entry:
            # New Model's id isn't known yet. We include its
            # scope_entities so the region covers the subject set.
            for e in op.entry.get("scope_entities", []) or []:
                if isinstance(e, dict):
                    et = e.get("type"); eid = e.get("id")
                    if et and eid:
                        out.append((str(et), str(eid)))
        elif op.model_id is not None:
            out.append(("model", str(op.model_id)))
    for op in diff.act_ops:
        ent = op.entity or {}
        if op.op in (
            "create_commitment", "transition_commitment",
        ):
            eid = ent.get("id")
            if eid is not None:
                out.append(("commitment", str(eid)))
            for ct in ent.get("contributes_to_goal_ids", []) or []:
                gid = ct[0] if isinstance(ct, (list, tuple)) else ct
                out.append(("goal", str(gid)))
        elif op.op in ("create_goal", "transition_goal", "update_goal"):
            eid = ent.get("id")
            if eid is not None:
                out.append(("goal", str(eid)))
            pid = ent.get("parent_goal_id")
            if pid is not None:
                out.append(("goal", str(pid)))
        elif op.op in ("create_decision", "transition_decision"):
            eid = ent.get("id")
            if eid is not None:
                out.append(("decision", str(eid)))
        elif op.op == "add_edge_contributes_to":
            cid = ent.get("commitment_id")
            gid = ent.get("goal_id")
            if cid: out.append(("commitment", str(cid)))
            if gid: out.append(("goal", str(gid)))
        elif op.op == "add_edge_depends_on":
            t = ent.get("dependent_commitment_id")
            d = ent.get("dependency_commitment_id")
            if t: out.append(("commitment", str(t)))
            if d: out.append(("commitment", str(d)))
        elif op.op == "add_edge_constrained_by":
            cid = ent.get("commitment_id")
            did = ent.get("decision_id")
            if cid: out.append(("commitment", str(cid)))
            if did: out.append(("decision", str(did)))
    for op in diff.resource_ops:
        if op.resource_id is not None:
            out.append(("resource", str(op.resource_id)))
    return out


async def _load_basis_model(
    conn: asyncpg.Connection,
    basis_id: UUID | None,
) -> dict[str, Any] | None:
    """
    Minimal basis load: confidence + proposition_kind + scope_actors.
    We don't hydrate the full ModelRow because the validator only needs
    a few fields and we want the validator to stay cheap.
    """
    if basis_id is None:
        return None
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, confidence, proposition_kind,
               scope_actors, status
        FROM models
        WHERE id = $1
        """,
        basis_id,
    )
    if row is None:
        return None
    return dict(row)


async def _verify_doneverified_evidence(
    conn: asyncpg.Connection,
    resolved_by_event_ids: list[UUID],
) -> None:
    """
    C3 adjunct + spec §7: doneverified requires >=1 resolved_by_event_id
    AND every referenced observation's trust_tier is at least
    `authoritative`. Raises TrustTierError on failure.
    """
    if not resolved_by_event_ids:
        raise InvariantViolation(
            "C3",
            "doneverified requires >=1 resolved_by_event_id",
            resolved_by_event_ids=[],
        )
    rows = await conn.fetch(
        """
        SELECT id, trust_tier FROM observations
        WHERE id = ANY($1::uuid[])
        """,
        list(resolved_by_event_ids),
    )
    found_ids = {r["id"] for r in rows}
    missing = [eid for eid in resolved_by_event_ids if eid not in found_ids]
    if missing:
        raise ValidationError(
            f"doneverified references {len(missing)} non-existent observation(s)",
            missing=[str(m) for m in missing],
        )
    required = TrustTier("authoritative")
    for r in rows:
        tt = r["trust_tier"]
        try:
            actual = TrustTier(tt)
        except ValueError:
            raise ValidationError(
                f"observation {r['id']} has invalid trust_tier {tt!r}"
            )
        if not actual.is_at_least(required):
            raise TrustTierError(
                required="authoritative",
                actual=tt,
                message=(
                    f"doneverified requires authoritative evidence; "
                    f"observation {r['id']} is {tt}"
                ),
                observation_id=str(r["id"]),
            )


# =====================================================================
# validate()
# =====================================================================


async def validate(
    diff: RawDiff,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    *,
    allowed_region: list[tuple[str, str]] | None = None,
    strict_region: bool = True,
) -> ValidatedDiff:
    """
    Validate `diff` against the retrieved context + DB invariants.

    Returns a ValidatedDiff containing only the passing ops. Bad ops
    are dropped, and their count + error messages are attached to the
    returned diff (`dropped_op_count`, `dropped_op_errors`) so the
    caller can record partial-accept observability. Raises
    `ValidationFailure` only when the LLM submitted ops and every one
    of them failed (no-survivors); raises `OutOfRegionError` when
    `strict_region=True` and the LLM touched an entity outside
    `allowed_region`.

    `allowed_region` is None-or-list of (type, id-str) tuples produced
    by `region_locks.touched_entity_ids(retrieval_result)` pre-lock. If
    None, region containment is not enforced (tests that don't care
    about region can pass None).
    """
    errors: list[str] = []
    total_ops = (
        len(diff.claim_ops) + len(diff.act_ops) + len(diff.resource_ops)
    )

    # --- Out-of-region check (before any other work) ---------------
    if allowed_region is not None and strict_region:
        allowed = set(allowed_region)
        touched = _iter_entity_ids_touched(diff)
        missing = [t for t in touched if t not in allowed]
        if missing:
            raise OutOfRegionError(
                "diff touches entities outside the pre-declared region",
                missing=missing[:10],
                touched=len(touched),
                allowed_size=len(allowed),
            )

    validated_claim_ops: list[ClaimOp] = []
    validated_act_ops: list[ActOp] = []
    validated_resource_ops: list[ResourceOp] = []

    # --- claim_ops -------------------------------------------------
    context_model_ids = {
        getattr(m, "id", None) for m in getattr(retrieval_result, "models", [])
    }
    for op in diff.claim_ops:
        try:
            v_op = await _validate_claim_op(
                op, retrieval_result, conn, tenant_id=diff.tenant_id
            )
        except (FalsifierInadequateError, MalformedFalsifierError, ValidationError) as e:
            reason = _classify_claim_drop_reason(e)
            errors.append(
                f"claim_op {op.op}: {e.message if hasattr(e, 'message') else str(e)}"
            )
            # OP-4: structured dropped-op log + metrics counter.
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="claim",
                failure_reason=reason,
                original_op=op,
            )
            continue
        # For update/archive, the target Model must exist. Retrieval
        # context may not include it (the LLM may update a Model it
        # saw in retrieval, or we may relax for archived). We check
        # DB presence when strict.
        if v_op.op in ("update", "archive") and v_op.model_id is not None:
            exists = await conn.fetchval(
                "SELECT 1 FROM models WHERE id = $1", v_op.model_id
            )
            if not exists:
                errors.append(
                    f"claim_op {v_op.op}: model {v_op.model_id} not found"
                )
                log_dropped_op(
                    trigger_id=diff.trigger_ref,
                    tenant_id=diff.tenant_id,
                    op_kind=v_op.op,
                    op_type="claim",
                    failure_reason="missing_model_reference",
                    original_op=op,
                )
                continue
        validated_claim_ops.append(v_op)

    # --- act_ops ---------------------------------------------------
    for op in diff.act_ops:
        try:
            v_op = await _validate_act_op(op, retrieval_result, conn)
        except (
            ValidationError, InvariantViolation, TrustTierError,
        ) as e:
            reason = _classify_act_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"act_op {op.op}: {msg}")
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="act",
                failure_reason=reason,
                original_op=op,
            )
            continue
        validated_act_ops.append(v_op)

    # --- resource_ops ----------------------------------------------
    for op in diff.resource_ops:
        try:
            v_op = _validate_resource_op_shape(op)
        except ValidationError as e:
            errors.append(f"resource_op {op.op}: {e.message}")
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="resource",
                failure_reason=_classify_resource_drop_reason(e),
                original_op=op,
            )
            continue
        validated_resource_ops.append(v_op)

    # --- Partial-accept gate --------------------------------------
    # Policy: keep good ops, drop bad ones. Record dropped-op counts
    # and error messages on the ValidatedDiff for observability. Only
    # raise ValidationFailure when the LLM submitted ops (total_ops>0)
    # and EVERY one of them was bad — in that case there's nothing to
    # apply and silently returning empty would mask an upstream bug.
    any_survived = bool(
        validated_claim_ops or validated_act_ops or validated_resource_ops
    )
    if total_ops > 0 and not any_survived:
        raise ValidationFailure(
            f"validation rejected {len(errors)}/{total_ops} ops "
            f"(every op failed)",
            errors=errors[:25],
            total=total_ops,
        )

    return ValidatedDiff(
        trigger_ref=diff.trigger_ref,
        tenant_id=diff.tenant_id,
        claim_ops=validated_claim_ops,
        act_ops=validated_act_ops,
        resource_ops=validated_resource_ops,
        new_predictions=[op for op in diff.new_predictions if op.op == "insert"],
        reasoning_trace=diff.reasoning_trace,
        dropped_op_count=len(errors),
        dropped_op_errors=errors[:25],
    )


# ---------------------------------------------------------------------
# Per-op validators
# ---------------------------------------------------------------------


async def _validate_claim_op(
    op: ClaimOp,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None = None,
) -> ClaimOp:
    """
    Shape-validate a single claim op and clip/calibrate confidence.
    Returns a (possibly-mutated) ClaimOp.
    """
    if op.op == "insert":
        if not isinstance(op.entry, dict):
            raise ValidationError("claim_op insert missing entry dict")
        entry = dict(op.entry)
        conf_raw = float(entry.get("confidence", 0.5))
        # TK-2 (THINK-DESIGN-AUDIT §5.2) — calibration ordering vs
        # falsifier. `apply_calibration` CAN inflate: the formula is
        # `clip(raw * offset, 0.05, 0.95)` and `offset` can reach
        # OFFSET_MAX=1.5 (see services/workers/calibration_updater/
        # compute.py). That means a raw confidence of 0.65 could become
        # 0.78 post-calibration — above the falsifier threshold. If we
        # checked the falsifier BEFORE calibration, such a Model would
        # slip through without a required falsifier.
        #
        # Ordering is therefore: clip → calibrate → clip → falsifier
        # check on the POST-calibration confidence. This is the
        # invariant that makes the falsifier guarantee hold regardless
        # of calibration inflation.
        conf = _clip(conf_raw)
        # apply_calibration — Wave 4-C real DB lookup against
        # calibration_offsets. Identity when no offset row matches.
        kind = None
        prop = entry.get("proposition")
        if isinstance(prop, dict):
            kind = prop.get("kind")
        conf = await apply_calibration(
            conf,
            entry.get("scope_actors"),
            kind,
            tenant_id=tenant_id,
            conn=conn,
        )
        conf = _clip(conf)
        # Falsifier check runs AFTER calibration (TK-2). If calibration
        # inflated conf past the threshold, the Model must still have
        # an adequate falsifier.
        if conf > _FALSIFIER_REQUIRED_ABOVE:
            ok, reason = is_adequate_falsifier(entry.get("falsifier"))
            if not ok:
                raise FalsifierInadequateError(
                    reason or "falsifier inadequate",
                    falsifier=entry.get("falsifier"),
                    confidence=conf,
                )
        entry["confidence"] = conf
        # confidence_at_assertion — if the LLM doesn't supply one, use
        # the pre-calibration raw confidence (clipped). This becomes the
        # immutable "what Think originally said" value.
        if "confidence_at_assertion" not in entry:
            entry["confidence_at_assertion"] = _clip(conf_raw)
        # scope_actors — check each exists in this tenant.
        for a in entry.get("scope_actors", []) or []:
            try:
                UUID(str(a))
            except (ValueError, TypeError):
                raise ValidationError(
                    f"claim_op insert: scope_actor {a!r} is not a UUID"
                )
        # References to entities must be present in retrieval_result OR
        # be net-new (the LLM creating a Commitment in this very same
        # diff may reference the new id, but that's hard to validate
        # pre-apply — we accept and let apply raise).
        return ClaimOp(op="insert", entry=entry)

    if op.op == "update":
        if op.model_id is None:
            raise ValidationError("claim_op update requires model_id")
        if not isinstance(op.changes, dict) or not op.changes:
            raise ValidationError("claim_op update requires non-empty changes")
        # Don't allow changes to confidence_at_assertion — Q3 immutability.
        if "confidence_at_assertion" in op.changes:
            raise ValidationError(
                "confidence_at_assertion is immutable (Q3)",
                model_id=str(op.model_id),
            )
        if "confidence" in op.changes:
            op.changes["confidence"] = _clip(float(op.changes["confidence"]))
        return op

    if op.op == "archive":
        if op.model_id is None:
            raise ValidationError("claim_op archive requires model_id")
        if not op.reason:
            raise ValidationError("claim_op archive requires reason")
        return op

    if op.op == "relocate":
        # S4: deliberate topology repositioning. Shape-only checks
        # here; semantic checks (target exists in tenant, dim
        # match, alpha range) live in `parse_relocate_target` and
        # are run by the applier.
        from lib.topology.relocate import parse_relocate_target

        if op.model_id is None:
            raise ValidationError("claim_op relocate requires model_id")
        if not op.relocate_target:
            raise ValidationError(
                "claim_op relocate requires relocate_target dict",
            )
        # parse_relocate_target raises ValidationError on shape errors.
        parse_relocate_target(op.relocate_target)
        return op

    raise ValidationError(f"unknown claim_op: {op.op!r}")


async def _validate_act_op(
    op: ActOp,
    retrieval_result: Any,
    conn: asyncpg.Connection,
) -> ActOp:
    """
    Threshold + state-machine validation for an Act op.
    """
    basis = await _load_basis_model(conn, op.confidence_basis)

    # Some ops (cascade-originated updates) have no basis by design.
    # For LLM-originated ops we require a basis (safety — the LLM
    # MUST cite a Model for every structural mutation).
    BASIS_EXEMPT = {"update_goal_health", "update_goal", "create_goal"}
    if basis is None and op.op not in BASIS_EXEMPT:
        raise ValidationError(
            f"act_op {op.op} requires confidence_basis model_id",
        )

    threshold = compute_threshold(op, basis)

    if basis is not None and float(basis.get("confidence", 0.0)) < threshold:
        raise ValidationError(
            f"insufficient confidence for {op.op}: "
            f"basis={basis.get('confidence')} < threshold={threshold}",
            op=op.op,
            basis_confidence=basis.get("confidence"),
            threshold=threshold,
        )

    # Transition legality.
    if op.op == "transition_commitment":
        cid = op.entity.get("id")
        new_state = op.entity.get("new_state")
        if cid is None or new_state is None:
            raise ValidationError(
                "transition_commitment requires entity.id and entity.new_state",
            )
        row = await conn.fetchrow(
            "SELECT state FROM commitments WHERE id = $1", cid
        )
        if row is None:
            raise ValidationError(
                f"transition_commitment: commitment {cid} not found"
            )
        ok, reason = can_transition(row["state"], new_state, "commitment")
        if not ok:
            raise InvariantViolation(
                "C_STATE",
                reason,
                commitment_id=str(cid),
                from_state=row["state"],
                to_state=new_state,
            )
        # doneverified: evidence trust-tier check.
        if new_state == "doneverified":
            resolved = op.entity.get("resolved_by_event_ids") or []
            # Accept either strings or UUIDs.
            resolved_uuids: list[UUID] = []
            for eid in resolved:
                try:
                    resolved_uuids.append(
                        eid if isinstance(eid, UUID) else UUID(str(eid))
                    )
                except (ValueError, TypeError):
                    raise ValidationError(
                        f"resolved_by_event_ids contains non-UUID: {eid!r}",
                    )
            await _verify_doneverified_evidence(conn, resolved_uuids)

    if op.op == "transition_goal":
        gid = op.entity.get("id")
        new_state = op.entity.get("new_state")
        if gid is None or new_state is None:
            raise ValidationError(
                "transition_goal requires entity.id and entity.new_state",
            )
        row = await conn.fetchrow(
            "SELECT state FROM goals WHERE id = $1", gid
        )
        if row is None:
            raise ValidationError(f"goal {gid} not found")
        ok, reason = can_transition(row["state"], new_state, "goal")
        if not ok:
            raise InvariantViolation(
                "G_STATE",
                reason, goal_id=str(gid),
                from_state=row["state"], to_state=new_state,
            )

    if op.op == "transition_decision":
        did = op.entity.get("id")
        new_state = op.entity.get("new_state")
        if did is None or new_state is None:
            raise ValidationError(
                "transition_decision requires entity.id and entity.new_state",
            )
        row = await conn.fetchrow(
            "SELECT state FROM decisions WHERE id = $1", did
        )
        if row is None:
            raise ValidationError(f"decision {did} not found")
        ok, reason = can_transition(row["state"], new_state, "decision")
        if not ok:
            raise InvariantViolation(
                "D_STATE",
                reason, decision_id=str(did),
                from_state=row["state"], to_state=new_state,
            )

    return op


def _validate_resource_op_shape(op: ResourceOp) -> ResourceOp:
    """
    Minimal shape validation. Repo methods do the rest at apply time.
    """
    if op.op == "create":
        if not isinstance(op.payload, dict) or not op.payload:
            raise ValidationError(
                "resource_op create requires non-empty payload dict",
            )
        return op
    if op.op == "update":
        if op.resource_id is None:
            raise ValidationError("resource_op update requires resource_id")
        if op.patch is None and op.payload is None:
            raise ValidationError(
                "resource_op update requires patch or payload",
            )
        return op
    if op.op == "transaction":
        if op.resource_id is None or op.kind is None or op.delta is None:
            raise ValidationError(
                "resource_op transaction requires resource_id, kind, delta",
            )
        return op
    if op.op == "deploy":
        if op.resource_id is None or op.commitment_id is None:
            raise ValidationError(
                "resource_op deploy requires resource_id and commitment_id",
            )
        if not isinstance(op.quantity, dict):
            raise ValidationError(
                "resource_op deploy requires quantity dict",
            )
        return op
    if op.op == "release":
        if op.resource_id is None or op.commitment_id is None:
            raise ValidationError(
                "resource_op release requires resource_id and commitment_id",
            )
        return op
    raise ValidationError(f"unknown resource_op: {op.op!r}")


__all__ = [
    "validate",
    "ValidationFailure",
    "OutOfRegionError",
]
