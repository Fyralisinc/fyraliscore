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
     basis.confidence < threshold → neutralize the op.

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

from typing import Any, get_args
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EdgeRegistryError
from lib.shared.errors import (
    CompanyOSError,
    FalsifierInadequateError,
    InvariantViolation,
    MalformedFalsifierError,
    TrustTierError,
    ValidationError,
)
from lib.shared.types import EdgeDetectedBy
from lib.shared.trust import TrustTier

from services.acts.state_machines import can_transition
from services.acts.invariants import (
    count_revisited_constraining_decisions,
    count_unsatisfied_dependencies,
)
from services.models.calibration import apply_calibration
from services.models.falsifier import is_adequate_falsifier
from services.models.propositions import validate_proposition
from services.resources.transactions import VALID_TRANSACTION_TYPES

from .diff_schema import ActOp, ClaimOp, EdgeOp, RawDiff, ResourceOp, ValidatedDiff
from .observability import log_dropped_op
from .thresholds import compute_threshold


# Phase 1 trace emission — best-effort, gated by SAGE_TRACE_EMIT and
# the presence of a TraceContext. The classifier below maps each
# validator drop reason to the spec §15.1 event type:
#
#   bad-reference family  → 'validation_failed_due_to_bad_reference'
#   missing-evidence /    → 'validation_failed_due_to_missing_evidence'
#     unclassified
#
# When no TraceContext is installed (e.g. unit tests calling validate()
# directly without an inquiry session), every emit is a no-op.
_BAD_REFERENCE_REASONS = frozenset({
    "invalid_entity_reference",
    "missing_entity_reference",
    "missing_model_reference",
})


def _outcome_event_for_drop_reason(reason: str) -> str:
    if reason in _BAD_REFERENCE_REASONS:
        return "validation_failed_due_to_bad_reference"
    return "validation_failed_due_to_missing_evidence"


async def _emit_validation_drop_event(
    *,
    op_type: str,
    op_kind: str,
    reason: str,
    error_message: str,
) -> None:
    """Append an outcome event for a dropped op. Pure best-effort.

    Imported inline so this module stays free of a hard dependency on
    services.sage (matches the local-import pattern used elsewhere for
    optional surfaces).
    """
    try:
        from services.sage.inquiry_traces.emitter import emit_event
    except Exception:  # noqa: BLE001
        return
    event_type = _outcome_event_for_drop_reason(reason)
    await emit_event(
        event_type,
        {
            "op_type": op_type,
            "op_kind": op_kind,
            "failure_reason": reason,
            "error_message": error_message[:500],
        },
    )


_ALLOWED_EDGE_DETECTED_BY = set(get_args(EdgeDetectedBy))


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


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
    if "proposition" in msg:
        return "invalid_proposition_shape"
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
    if "invalid transaction" in msg or "invalid kind" in msg:
        return "invalid_transaction_type"
    if "non-empty" in msg or "requires" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_edge_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "unknown edge_kind" in msg or "reserved" in msg:
        return "invalid_edge_kind"
    if "weight" in msg:
        return "invalid_weight"
    if "confidence" in msg:
        return "invalid_confidence"
    if "not found" in msg:
        return "missing_model_reference"
    if "self-edge" in msg:
        return "invalid_shape"
    if "cycle" in msg:
        return "cycle_prevention"
    if "explanation" in msg:
        return "missing_explanation"
    return "unclassified"


_CONFIDENCE_MIN = 0.05
_CONFIDENCE_MAX = 0.95
_FALSIFIER_REQUIRED_ABOVE = 0.7
_ERROR_RATE_HARD_LIMIT = 0.25  # Retained for callers that import it; no longer enforced as a gate.
_DEFAULT_ABSTRACTION_LEVEL_BY_CLAIM_ROLE: dict[str, str] = {
    "fact": "atomic",
    "concern": "atomic",
    "hypothesis": "atomic",
    "prediction": "atomic",
    "capability": "atomic",
    "recommendation": "atomic",
    "relation": "relationship",
    "pattern": "pattern",
}

_UNCERTAINTY_CONFIDENCE_CAP = 0.69
_LOW_TRUST_CONFIDENCE_CAP = 0.55
_UNCERTAINTY_MARKERS = (
    "maybe",
    "probably",
    "eventually",
    "would love",
    "we'd love",
    "no promises",
    "targeting",
    "when it's ready",
    "if leadership",
    "if they",
    "if pelago",
    "otherwise",
    "aspirational",
)
_LOW_TRUST_MARKERS = (
    "apparently",
    "secondhand",
    "heard it",
    "not sure",
    "+1 to",
    "what sarah said",
)


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


def _extract_observation_text(obs: Any) -> str:
    for attr in ("content_text", "natural", "text"):
        value = getattr(obs, attr, None)
        if isinstance(value, str) and value:
            return value
    if isinstance(obs, dict):
        for key in ("content_text", "natural", "text"):
            value = obs.get(key)
            if isinstance(value, str) and value:
                return value
        content = obs.get("content")
    else:
        content = getattr(obs, "content", None)
    if isinstance(content, dict):
        value = content.get("content_text") or content.get("text")
        if isinstance(value, str):
            return value
    return ""


def _confidence_cap_for_linguistic_uncertainty(
    entry: dict[str, Any],
    retrieval_result: Any,
) -> float | None:
    """Return a deterministic cap for obviously uncertain source text.

    LLMs sometimes understand hedged/conditional language in the
    natural text but still assign high confidence to a simplified
    subclaim. The source observation is the contract boundary: if the
    triggering signal is mostly hedged, aspirational, secondhand, or
    reply-context-dependent, keep inserted Model confidence below the
    high-confidence falsifier threshold.
    """
    parts = [
        str(entry.get("natural") or ""),
        str(entry.get("proposition") or ""),
    ]
    trigger = getattr(retrieval_result, "trigger", None)
    seed = getattr(trigger, "seed_natural_text", None)
    if isinstance(seed, str):
        parts.append(seed)
    for obs in getattr(retrieval_result, "observations", []) or []:
        parts.append(_extract_observation_text(obs))
    text = " ".join(parts).lower()
    if any(marker in text for marker in _LOW_TRUST_MARKERS):
        return _LOW_TRUST_CONFIDENCE_CAP
    if any(marker in text for marker in _UNCERTAINTY_MARKERS):
        return _UNCERTAINTY_CONFIDENCE_CAP
    return None


def _repair_non_situation_abstraction_level(entry: dict[str, Any]) -> None:
    """Normalize a live-LLM grammar slip before proposition validation.

    In the memory grammar, `composite` is reserved for situation Models.
    Live providers sometimes express a multi-clause signal as
    claim_role=`concern`/`fact` plus abstraction_level=`composite`. If we
    reject that here, the downstream splitter never gets a chance to
    decompose the signal and synthesize the situation Model that should
    carry the composite meaning.
    """
    prop = entry.get("proposition")
    if not isinstance(prop, dict):
        return
    if prop.get("claim_role") == "situation" or prop.get("legacy_kind") == "situation":
        return
    role = prop.get("claim_role")
    default_level = _DEFAULT_ABSTRACTION_LEVEL_BY_CLAIM_ROLE.get(str(role))
    if default_level is None:
        return
    if prop.get("abstraction_level") in (None, default_level):
        return
    prop["abstraction_level"] = default_level


def _mark_empty_situation_members_pending(entry: dict[str, Any]) -> None:
    """Allow explicit situations with deferred member binding.

    A provider can correctly identify the composite situation before it
    has existing Model ids to cite, especially in sparse retrieval. Mark
    that as a transient pending-member shape so the splitter/applier can
    form atomics and patch concrete member_model_ids after insert.
    """
    prop = entry.get("proposition")
    if not isinstance(prop, dict):
        return
    is_situation = (
        prop.get("claim_role") == "situation"
        or prop.get("legacy_kind") == "situation"
        or prop.get("kind") == "situation"
    )
    if not is_situation:
        return
    members = prop.get("member_model_ids")
    if isinstance(members, list) and members:
        return
    prop["member_model_ids"] = []
    prop["_pending_members"] = True
    entry["member_model_pending"] = True


def _iter_entity_ids_touched(diff: RawDiff) -> list[tuple[str, str]]:
    """
    Every entity id this diff mutates. Used by the out-of-region check.

    Lists (kind, id-as-str) tuples so we can compare against
    `region_locks.touched_entity_ids(...)` output.
    """
    out: list[tuple[str, str]] = []
    pending_model_event_ids: set[UUID] = set()
    for op in diff.claim_ops:
        if op.op != "insert" or not op.entry:
            continue
        for key in ("born_from_event_id", "model_id", "id"):
            placeholder_id = _coerce_uuid(op.entry.get(key))
            if placeholder_id is not None:
                pending_model_event_ids.add(placeholder_id)

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
    for op in diff.edge_ops:
        for model_id in (op.source_model_id, op.target_model_id):
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
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
        len(diff.claim_ops)
        + len(diff.edge_ops)
        + len(diff.act_ops)
        + len(diff.resource_ops)
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
    validated_edge_ops: list[EdgeOp] = []
    validated_act_ops: list[ActOp] = []
    validated_resource_ops: list[ResourceOp] = []
    neutralized_op_count = 0

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
            err_msg = e.message if hasattr(e, "message") else str(e)
            errors.append(f"claim_op {op.op}: {err_msg}")
            # OP-4: structured dropped-op log + metrics counter.
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="claim",
                failure_reason=reason,
                original_op=op,
            )
            # Phase 1: outcome event for the self-evolution loop.
            await _emit_validation_drop_event(
                op_type="claim",
                op_kind=op.op,
                reason=reason,
                error_message=err_msg,
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
                err_msg = f"model {v_op.model_id} not found"
                errors.append(f"claim_op {v_op.op}: {err_msg}")
                log_dropped_op(
                    trigger_id=diff.trigger_ref,
                    tenant_id=diff.tenant_id,
                    op_kind=v_op.op,
                    op_type="claim",
                    failure_reason="missing_model_reference",
                    original_op=op,
                )
                await _emit_validation_drop_event(
                    op_type="claim",
                    op_kind=v_op.op,
                    reason="missing_model_reference",
                    error_message=err_msg,
                )
                continue
        validated_claim_ops.append(v_op)

    pending_claim_basis_confidence: dict[UUID, float] = {}
    for op in validated_claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        confidence = float(op.entry.get("confidence") or 0.0)
        for key in ("born_from_event_id", "model_id", "id"):
            placeholder_id = _coerce_uuid(op.entry.get(key))
            if placeholder_id is not None:
                pending_claim_basis_confidence[placeholder_id] = confidence

    # --- edge_ops --------------------------------------------------
    for op in diff.edge_ops:
        try:
            v_op = await _validate_edge_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=set(pending_claim_basis_confidence),
                pending_edge_ops=validated_edge_ops,
            )
        except (ValidationError, EdgeRegistryError) as e:
            reason = _classify_edge_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"edge_op {op.op}: {msg}")
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="edge",
                failure_reason=reason,
                original_op=op,
            )
            await _emit_validation_drop_event(
                op_type="edge",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
            )
            continue
        if v_op is None:
            neutralized_op_count += 1
            continue
        validated_edge_ops.append(v_op)

    # --- act_ops ---------------------------------------------------
    for op in diff.act_ops:
        try:
            v_op = await _validate_act_op(
                op,
                retrieval_result,
                conn,
                pending_claim_basis_confidence=pending_claim_basis_confidence,
            )
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
            await _emit_validation_drop_event(
                op_type="act",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
            )
            continue
        if v_op is None:
            neutralized_op_count += 1
            continue
        validated_act_ops.append(v_op)

    # --- resource_ops ----------------------------------------------
    for op in diff.resource_ops:
        try:
            v_op = _validate_resource_op_shape(op)
        except ValidationError as e:
            reason = _classify_resource_drop_reason(e)
            errors.append(f"resource_op {op.op}: {e.message}")
            log_dropped_op(
                trigger_id=diff.trigger_ref,
                tenant_id=diff.tenant_id,
                op_kind=op.op,
                op_type="resource",
                failure_reason=reason,
                original_op=op,
            )
            await _emit_validation_drop_event(
                op_type="resource",
                op_kind=op.op,
                reason=reason,
                error_message=e.message,
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
        validated_claim_ops
        or validated_edge_ops
        or validated_act_ops
        or validated_resource_ops
    )
    if total_ops > 0 and not any_survived and neutralized_op_count == 0:
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
        edge_ops=validated_edge_ops,
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
        uncertainty_cap = _confidence_cap_for_linguistic_uncertainty(
            entry, retrieval_result,
        )
        if uncertainty_cap is not None:
            conf = min(conf, uncertainty_cap)
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
        # Validate the proposition union here, before apply. Live LLM
        # calls can emit one malformed claim next to valid claims; the
        # validator's partial-accept contract should drop that one op
        # instead of letting the applier fail the whole transaction.
        _repair_non_situation_abstraction_level(entry)
        _mark_empty_situation_members_pending(entry)
        validate_proposition(entry.get("proposition"))
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

    raise ValidationError(f"unknown claim_op: {op.op!r}")


async def _validate_act_op(
    op: ActOp,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    *,
    pending_claim_basis_confidence: dict[UUID, float] | None = None,
) -> ActOp | None:
    """
    Threshold + state-machine validation for an Act op.
    """
    basis = await _load_basis_model(conn, op.confidence_basis)
    pending_claim_basis_confidence = pending_claim_basis_confidence or {}
    if basis is None and op.confidence_basis in pending_claim_basis_confidence:
        basis = {"confidence": pending_claim_basis_confidence[op.confidence_basis]}

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
        return None

    if op.op == "create_decision":
        ent = dict(op.entity or {})
        title = ent.get("title")
        decision_text = ent.get("decision_text")
        if not isinstance(title, str) or not title.strip():
            raise ValidationError("create_decision requires entity.title")
        if not isinstance(decision_text, str) or not decision_text.strip():
            fallback = ent.get("rationale") or title
            ent["decision_text"] = str(fallback).strip()
            ent["canonicalized_missing_decision_text"] = True
        op = op.model_copy(update={"entity": ent})

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
        if row["state"] == new_state:
            return None
        # Proposed commitments are not yet runnable work. Live LLMs can read
        # "not ready / waiting" language as paused or blocked, but the legal
        # next states are only active or closed. Treat runtime-state requests
        # against a proposed commitment as a safe no-op.
        if row["state"] == "proposed" and new_state in {"paused", "blocked"}:
            return None
        # The pure state machine allows active -> blocked, but the
        # Commitment service enforces C2 at apply time: blocked requires
        # an unsatisfied dependency or revisited constraining decision.
        # Live LLMs often use "blocked" colloquially for waiting/on-hold
        # signals. Preserve the useful transition by canonicalizing those
        # unsupported cases to paused, which is the legal ledger state for
        # social or approval-style stalls.
        if new_state == "blocked":
            n_deps = await count_unsatisfied_dependencies(conn, cid)
            n_rev = await count_revisited_constraining_decisions(conn, cid)
            if n_deps == 0 and n_rev == 0:
                if row["state"] == "paused":
                    return None
                paused_ok, paused_reason = can_transition(
                    row["state"], "paused", "commitment"
                )
                if not paused_ok:
                    raise InvariantViolation(
                        "C_STATE",
                        paused_reason,
                        commitment_id=str(cid),
                        from_state=row["state"],
                        to_state="paused",
                    )
                updated_entity = dict(op.entity)
                updated_entity["new_state"] = "paused"
                updated_entity["canonicalized_from_state"] = "blocked"
                updated_entity["canonicalization_reason"] = (
                    "blocked_without_dependency_or_revisited_decision"
                )
                op = op.model_copy(update={"entity": updated_entity})
                new_state = "paused"
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
        if row["state"] == new_state:
            return None
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
        if row["state"] == new_state:
            return None
        if row["state"] == "drafted" and new_state == "revisited":
            # Revisit is only legal for active decisions. Live LLMs sometimes
            # use "revisited" to mean a drafted decision has become salient
            # again; preserve the legal lifecycle movement instead of dropping
            # the whole bookkeeping op.
            updated_entity = dict(op.entity)
            updated_entity["new_state"] = "active"
            updated_entity["canonicalized_from_state"] = "revisited"
            updated_entity["canonicalization_reason"] = (
                "drafted_decision_cannot_be_revisited"
            )
            op = op.model_copy(update={"entity": updated_entity})
            new_state = "active"
        ok, reason = can_transition(row["state"], new_state, "decision")
        if not ok:
            raise InvariantViolation(
                "D_STATE",
                reason, decision_id=str(did),
                from_state=row["state"], to_state=new_state,
            )

    return op


async def _validate_edge_op(
    op: EdgeOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID] | None = None,
    pending_edge_ops: list[EdgeOp] | None = None,
) -> EdgeOp | None:
    from lib.shared.edge_registry import assert_writable, validate_weight

    if op.source_model_id == op.target_model_id:
        raise ValidationError(
            "edge_op self-edge not allowed",
            model_id=str(op.source_model_id),
        )
    spec = assert_writable(op.edge_kind)
    validate_weight(op.edge_kind, op.weight)
    if not (0.0 <= float(op.confidence) <= 1.0):
        raise ValidationError(
            "edge_op confidence must be in [0, 1]",
            confidence=op.confidence,
        )
    if op.detected_by and op.detected_by not in _ALLOWED_EDGE_DETECTED_BY:
        op = op.model_copy(update={"detected_by": None})
    if op.op == "add":
        explanation_required = {
            "contradicts",
            "weakens",
            "causes",
            "explains",
            "predicts",
            "blocks",
            "enables",
            "same_issue_as",
            "early_warning_for",
            "alternative_to",
        }
        if (
            op.edge_kind in explanation_required
            and not (op.explanation or "").strip()
        ):
            raise ValidationError(
                f"edge_op add {op.edge_kind!r} requires explanation",
                edge_kind=op.edge_kind,
            )
    elif op.op == "retire":
        if not (op.reason or "").strip():
            raise ValidationError("edge_op retire requires reason")
    else:
        raise ValidationError(f"unknown edge_op: {op.op!r}")

    rows = await conn.fetch(
        """
        SELECT id, status FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        [op.source_model_id, op.target_model_id],
    )
    found = {r["id"]: r["status"] for r in rows}
    pending_model_event_ids = pending_model_event_ids or set()
    missing = [
        str(mid)
        for mid in (op.source_model_id, op.target_model_id)
        if mid not in found and mid not in pending_model_event_ids
    ]
    if missing:
        raise ValidationError(
            f"edge_op references {len(missing)} missing model(s)",
            missing=missing,
        )
    if op.op == "add":
        inactive = [
            str(mid)
            for mid, status in found.items()
            if status != "active"
        ]
        if inactive:
            raise ValidationError(
                "edge_op add requires active model endpoints",
                inactive=inactive,
            )
        if await _edge_would_create_cycle(
            op,
            conn,
            tenant_id=tenant_id,
            pending_edge_ops=pending_edge_ops,
        ):
            return None

    # Touch spec so linters/tests know this was intentionally looked up.
    _ = spec
    return op


async def _edge_would_create_cycle(
    op: EdgeOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_edge_ops: list[EdgeOp] | None = None,
) -> bool:
    from lib.shared.edge_registry import cycle_scope_for

    scope = cycle_scope_for(op.edge_kind)
    if scope is None:
        return False

    pending_adjacency: dict[UUID, set[UUID]] = {}
    for pending in pending_edge_ops or []:
        if pending.op != "add" or pending.edge_kind not in scope:
            continue
        pending_adjacency.setdefault(pending.source_model_id, set()).add(
            pending.target_model_id
        )

    seen: set[UUID] = set()
    frontier: set[UUID] = {op.target_model_id}
    scope_list = list(scope)
    for _ in range(512):
        frontier -= seen
        if not frontier:
            return False
        if op.source_model_id in frontier:
            return True
        seen.update(frontier)
        rows = await conn.fetch(
            """
            SELECT target_model_id
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = ANY($2::uuid[])
              AND edge_kind = ANY($3::text[])
              AND status = 'active'
            """,
            tenant_id,
            list(frontier),
            scope_list,
        )
        next_frontier = {r["target_model_id"] for r in rows}
        for node in frontier:
            next_frontier.update(pending_adjacency.get(node, ()))
        frontier = next_frontier

    raise ValidationError(
        "edge_op cycle traversal exceeded depth guard",
        edge_kind=op.edge_kind,
    )


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
        if op.kind not in VALID_TRANSACTION_TYPES:
            raise ValidationError(
                f"resource_op transaction has invalid kind {op.kind!r}",
                kind=op.kind,
                valid=list(VALID_TRANSACTION_TYPES),
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
