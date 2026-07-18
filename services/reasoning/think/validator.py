"""services/reasoning/think/validator.py — the validation choke point.

Spec §7 "Validation" + BUILD-PLAN §4 Prompt 3.B item 5.

Rules enforced:

  1. claim_ops.insert with confidence > 0.7 → falsifier must be adequate
     (`services.domain.models.falsifier.is_adequate_falsifier`). Inadequate →
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

  7. Region containment is advisory. Retrieval regions describe the
     context Think initially saw; they do not constrain valid tenant-local
     mutations. Tenant-bound existence checks below are the hard safety
     boundary.

  8. Partial-accept: keep every op that passes, drop ones that fail,
     and record the dropped count + error messages on the returned
     ValidatedDiff. Only raise ValidationFailure when every op failed
     (no survivors) so an all-bad diff still signals upstream.

This module is pure (no DB writes) except for the falsifier DB-check
detour in the commitment_outcome.commitment_ref path — that one reads
commitments table to verify the ref exists.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Sequence, get_args
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EDGE_REGISTRY, EdgeRegistryError
from lib.shared.errors import (
    CompanyOSError,
    FalsifierInadequateError,
    InvariantViolation,
    MalformedFalsifierError,
    TrustTierError,
    ValidationError,
)
from lib.shared.types import EdgeDetectedBy, ModelArchiveReason
from lib.shared.trust import TrustTier

from services.domain.acts.state_machines import can_transition
from services.domain.acts.invariants import (
    count_revisited_constraining_decisions,
    count_unsatisfied_dependencies,
)
from services.domain.models.calibration import apply_calibration
from services.domain.models.falsifier import is_adequate_falsifier
from services.domain.models.open_questions import (
    OPEN_QUESTION_STATUSES,
    dedupe_key_for_question,
    normalize_question_text,
    normalize_question_type,
)
from services.domain.models.propositions import validate_proposition
from services.domain.resources.transactions import VALID_TRANSACTION_TYPES

from .diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    FormationResolutionOp,
    MemoryLifecycleOp,
    OntologyGapOp,
    OpenQuestionOp,
    RawDiff,
    RelationClaimOp,
    RelationFrameOp,
    ResourceOp,
    ValidatedDiff,
)
from .edge_semantics import (
    canonicalize_edge_semantics,
    enforce_edge_specificity,
    normalize_edge_review_status,
)
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
    conn: asyncpg.Connection | None = None,
    tenant_id: UUID | None = None,
    op_type: str,
    op_kind: str,
    reason: str,
    error_message: str,
) -> None:
    """Append an outcome event for a dropped op. Pure best-effort.

    Imported inline so this module stays free of a hard dependency on
    services.reasoning.sage (matches the local-import pattern used elsewhere for
    optional surfaces).
    """
    if conn is not None and tenant_id is not None:
        try:
            from services.domain.feedback_stats import record_feedback_stat

            await record_feedback_stat(
                conn,
                tenant_id=tenant_id,
                surface="think_validation",
                op_type=op_type,
                op_kind=op_kind,
                outcome="dropped",
                reason=reason,
                payload={"error_message": error_message[:500]},
            )
        except asyncpg.PostgresError:
            pass

    try:
        from services.reasoning.sage.inquiry_traces.emitter import emit_event
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
_ALLOWED_MODEL_ARCHIVE_REASONS = set(get_args(ModelArchiveReason))


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


def _classify_memory_lifecycle_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "not found" in msg or "missing model" in msg:
        return "missing_model_reference"
    if "inactive" in msg or "active" in msg:
        return "inactive_model_reference"
    if "confidence" in msg:
        return "invalid_confidence"
    if "archive reason" in msg or "registered lifecycle reason" in msg:
        return "invalid_archive_reason"
    if "evidence" in msg or "observation" in msg:
        return "missing_evidence"
    if "requires" in msg or "rationale" in msg:
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


def _classify_relation_claim_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "edge_kind" in msg:
        return "invalid_edge_kind"
    if "confidence" in msg:
        return "invalid_confidence"
    if "not found" in msg or "missing model" in msg:
        return "missing_model_reference"
    if "same source/target" in msg or "self" in msg:
        return "invalid_shape"
    if "evidence" in msg:
        return "missing_evidence"
    return "unclassified"


def _classify_relation_frame_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "not found" in msg or "missing model" in msg:
        return "missing_model_reference"
    if "active model" in msg:
        return "inactive_model_reference"
    if "confidence" in msg:
        return "invalid_confidence"
    if "participant" in msg or "role" in msg or "requires" in msg:
        return "invalid_shape"
    return "unclassified"


def _classify_ontology_gap_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "already exists" in msg:
        return "edge_kind_already_registered"
    if "fallback" in msg or "parent" in msg or "nearest" in msg:
        return "invalid_fallback_kind"
    if "not found" in msg or "missing model" in msg:
        return "missing_model_reference"
    if "active model endpoints" in msg:
        return "inactive_model_reference"
    if "self-edge" in msg or "requires" in msg or "snake_case" in msg:
        return "invalid_shape"
    if "score" in msg or "confidence" in msg:
        return "invalid_score"
    return "unclassified"


def _classify_open_question_drop_reason(exc: Exception) -> str:
    msg = str(getattr(exc, "message", exc)).lower()
    if "missing model" in msg or "not found" in msg:
        return "missing_model_reference"
    if "inactive" in msg or "active" in msg:
        return "inactive_model_reference"
    if "question" in msg and ("requires" in msg or "short" in msg):
        return "invalid_shape"
    if "priority" in msg:
        return "invalid_priority"
    if "json" in msg or "signature" in msg:
        return "invalid_shape"
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
_UNSUPPORTED_CAUSAL_HYPOTHESIS_CAP = 0.60
_THIN_CAUSAL_SITUATION_CAP = 0.68
_PARTIAL_CAUSAL_SITUATION_CAP = 0.74
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
    "otherwise",
    "aspirational",
)
_LOW_TRUST_MARKERS = (
    "apparently",
    "secondhand",
    "heard it",
    "not sure",
    "+1 to",
)


class ValidationFailure(CompanyOSError):
    default_code = "validation_failure"


class OutOfRegionError(CompanyOSError):
    """
    Legacy error for pre-2026 strict retrieval-region validation.

    The current validator treats retrieval regions as advisory and relies
    on tenant-bound existence checks for the hard safety boundary.
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


def _confidence_cap_for_causal_evidence(entry: dict[str, Any]) -> float | None:
    """Bound causal confidence until the claim cites discriminating evidence.

    Cold-start calibration is actor/kind based and therefore cannot tell a
    fluent causal explanation from a mechanism supported by independent
    observations. Keep hypotheses conservative and let later confirmation or
    empirical calibration raise them. Situations can earn a higher initial cap
    when they cite multiple observation ids, but do not receive high confidence
    merely for combining many scoped entities or member Models.
    """
    proposition = entry.get("proposition")
    if not isinstance(proposition, dict):
        return None
    role = str(proposition.get("claim_role") or "")
    if role == "hypothesis":
        return _UNSUPPORTED_CAUSAL_HYPOTHESIS_CAP
    if role != "situation":
        return None
    evidence_ids = {
        str(value)
        for value in [
            *list(entry.get("supporting_event_ids", []) or []),
            *list(proposition.get("evidence_event_ids", []) or []),
        ]
        if value
    }
    contextual_frame = proposition.get("contextual_frame")
    source_channels = {
        str(value).strip().casefold()
        for value in (
            contextual_frame.get("source_channels", [])
            if isinstance(contextual_frame, dict)
            else []
        )
        if str(value).strip()
    }
    if len(evidence_ids) < 2:
        return _THIN_CAUSAL_SITUATION_CAP
    if len(evidence_ids) < 3 or len(source_channels) < 2:
        return _PARTIAL_CAUSAL_SITUATION_CAP
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
    """Allow explicit situations with deferred/under-bound member binding.

    A provider can correctly identify the composite situation before it
    has enough existing Model ids to cite, especially in sparse retrieval.
    Mark that as a transient pending-member shape so the splitter/applier
    can form atomics and patch concrete member_model_ids after insert.
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
    if isinstance(members, list) and len(members) >= 2:
        return
    prop["member_model_ids"] = []
    prop["_pending_members"] = True
    entry["member_model_pending"] = True


def _normalize_hypothesis_assertion_alias(entry: dict[str, Any]) -> None:
    prop = entry.get("proposition")
    if not isinstance(prop, dict):
        return
    if prop.get("kind") != "belief" or prop.get("claim_role") != "hypothesis":
        return
    if isinstance(prop.get("hypothesis_text"), str) and prop["hypothesis_text"].strip():
        return
    assertion = prop.get("assertion")
    if isinstance(assertion, str) and assertion.strip():
        prop["hypothesis_text"] = assertion.strip()


def _iter_entity_ids_touched(diff: RawDiff) -> list[tuple[str, str]]:
    """
    Every entity id this diff mutates.

    Lists (kind, id-as-str) tuples so region lock/log compatibility can
    still describe what a diff touched. This is no longer a validation
    boundary.
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
                    et = e.get("type")
                    eid = e.get("id")
                    if et and eid:
                        out.append((str(et), str(eid)))
        elif op.model_id is not None:
            out.append(("model", str(op.model_id)))
    for op in diff.memory_lifecycle_ops:
        out.append(("model", str(op.model_id)))
        for model_id in op.evidence_model_ids:
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
        if op.superseded_by_model_id is not None:
            out.append(("model", str(op.superseded_by_model_id)))
    for op in diff.edge_ops:
        for model_id in (op.source_model_id, op.target_model_id):
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
    for op in diff.relation_claim_ops:
        for model_id in (op.source_model_id, op.target_model_id):
            if model_id is None or _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
    for op in diff.relation_frame_ops:
        for participant in op.participants:
            if _coerce_uuid(participant.model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(participant.model_id)))
        for model_id in op.evidence_model_ids:
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
    for op in diff.ontology_gap_ops:
        for model_id in (op.source_model_id, op.target_model_id):
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
        for model_id in op.evidence_model_ids:
            if _coerce_uuid(model_id) in pending_model_event_ids:
                continue
            out.append(("model", str(model_id)))
    for op in diff.open_question_ops:
        for model_id in (
            op.model_id,
            op.resolution_model_id,
            *op.source_model_ids,
        ):
            if model_id is None or _coerce_uuid(model_id) in pending_model_event_ids:
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
            if cid:
                out.append(("commitment", str(cid)))
            if gid:
                out.append(("goal", str(gid)))
        elif op.op == "add_edge_depends_on":
            t = ent.get("dependent_commitment_id")
            d = ent.get("dependency_commitment_id")
            if t:
                out.append(("commitment", str(t)))
            if d:
                out.append(("commitment", str(d)))
        elif op.op == "add_edge_constrained_by":
            cid = ent.get("commitment_id")
            did = ent.get("decision_id")
            if cid:
                out.append(("commitment", str(cid)))
            if did:
                out.append(("decision", str(did)))
    for op in diff.resource_ops:
        if op.resource_id is not None:
            out.append(("resource", str(op.resource_id)))
    return out


async def _load_basis_model(
    conn: asyncpg.Connection,
    basis_id: UUID | None,
    *,
    tenant_id: UUID,
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
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        basis_id,
    )
    if row is None:
        return None
    return dict(row)


async def _verify_doneverified_evidence(
    conn: asyncpg.Connection,
    resolved_by_event_ids: list[UUID],
    *,
    tenant_id: UUID,
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
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
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


@dataclass(slots=True)
class _ValidatedOpGroups:
    claim_ops: list[ClaimOp]
    memory_lifecycle_ops: list[MemoryLifecycleOp]
    relation_claim_ops: list[RelationClaimOp]
    relation_frame_ops: list[RelationFrameOp]
    edge_ops: list[EdgeOp]
    ontology_gap_ops: list[OntologyGapOp]
    open_question_ops: list[OpenQuestionOp]
    formation_resolutions: list[FormationResolutionOp]
    act_ops: list[ActOp]
    resource_ops: list[ResourceOp]
    neutralized_edge_count: int
    neutralized_act_count: int

    def failure_check_groups(self) -> tuple[Sequence[Any], ...]:
        return (
            self.claim_ops,
            self.memory_lifecycle_ops,
            self.relation_claim_ops,
            self.relation_frame_ops,
            self.edge_ops,
            self.ontology_gap_ops,
            self.open_question_ops,
            self.formation_resolutions,
            self.act_ops,
            self.resource_ops,
        )

    def to_validated_diff(self, diff: RawDiff, errors: list[str]) -> ValidatedDiff:
        return ValidatedDiff(
            trigger_ref=diff.trigger_ref,
            tenant_id=diff.tenant_id,
            claim_ops=self.claim_ops,
            memory_lifecycle_ops=self.memory_lifecycle_ops,
            relation_claim_ops=self.relation_claim_ops,
            relation_frame_ops=self.relation_frame_ops,
            edge_ops=self.edge_ops,
            ontology_gap_ops=self.ontology_gap_ops,
            open_question_ops=self.open_question_ops,
            formation_resolutions=self.formation_resolutions,
            act_ops=self.act_ops,
            resource_ops=self.resource_ops,
            new_predictions=[],
            reasoning_trace=diff.reasoning_trace,
            dropped_op_count=len(errors),
            dropped_op_errors=errors[:25],
        )


async def validate(
    diff: RawDiff,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    *,
    allowed_region: list[tuple[str, str]] | None = None,
    strict_region: bool = True,
    formation_candidate_ids: set[str] | frozenset[str] | None = None,
) -> ValidatedDiff:
    """Validate `diff` against retrieved context and DB invariants."""
    errors: list[str] = []
    claim_ops = [*diff.claim_ops, *diff.new_predictions]
    total_ops = _count_submitted_ops(diff, claim_ops)
    _enforce_region_containment(
        diff,
        claim_ops=claim_ops,
        allowed_region=allowed_region,
        strict_region=strict_region,
    )

    groups = await _validate_diff_op_groups(
        diff=diff,
        claim_ops=claim_ops,
        retrieval_result=retrieval_result,
        conn=conn,
        errors=errors,
        formation_candidate_ids=formation_candidate_ids,
    )

    _raise_if_every_op_failed(
        total_ops=total_ops,
        errors=errors,
        neutralized_op_count=groups.neutralized_edge_count + groups.neutralized_act_count,
        validated_groups=groups.failure_check_groups(),
    )

    return groups.to_validated_diff(diff, errors)


async def _validate_diff_op_groups(
    *,
    diff: RawDiff,
    claim_ops: list[ClaimOp],
    retrieval_result: Any,
    conn: asyncpg.Connection,
    errors: list[str],
    formation_candidate_ids: set[str] | frozenset[str] | None,
) -> _ValidatedOpGroups:
    validated_claim_ops = await _validate_claim_ops(
        diff, claim_ops, retrieval_result, conn, errors
    )
    memory_lifecycle_ops = await _validate_memory_lifecycle_ops(diff, conn, errors)
    pending_confidence = _pending_claim_basis_confidence(validated_claim_ops)
    pending_event_ids = set(pending_confidence)
    edge_ops, neutralized_edge_count = await _validate_edge_ops(
        diff,
        conn,
        errors,
        pending_claim_basis_confidence=pending_confidence,
    )
    relation_claim_ops = await _validate_relation_claim_ops(
        diff, conn, errors, pending_model_event_ids=pending_event_ids
    )
    relation_claim_ops = _resolve_authoritative_relation_retirements(
        relation_claim_ops
    )
    relation_frame_ops = await _validate_relation_frame_ops(
        diff, conn, errors, pending_model_event_ids=pending_event_ids
    )
    ontology_gap_ops = await _validate_ontology_gap_ops(
        diff,
        conn,
        errors,
        pending_claim_basis_confidence=pending_confidence,
    )
    open_question_ops = await _validate_open_question_ops(
        diff, conn, errors, pending_model_event_ids=pending_event_ids
    )
    formation_resolutions = await _validate_formation_resolutions(
        diff,
        conn,
        errors,
        formation_candidate_ids=formation_candidate_ids,
    )
    act_ops, neutralized_act_count = await _validate_act_ops(
        diff,
        retrieval_result,
        conn,
        errors,
        pending_claim_basis_confidence=pending_confidence,
    )
    return _ValidatedOpGroups(
        claim_ops=validated_claim_ops,
        memory_lifecycle_ops=memory_lifecycle_ops,
        relation_claim_ops=relation_claim_ops,
        relation_frame_ops=relation_frame_ops,
        edge_ops=edge_ops,
        ontology_gap_ops=ontology_gap_ops,
        open_question_ops=open_question_ops,
        formation_resolutions=formation_resolutions,
        act_ops=act_ops,
        resource_ops=await _validate_resource_ops(diff, conn, errors),
        neutralized_edge_count=neutralized_edge_count,
        neutralized_act_count=neutralized_act_count,
    )


def _count_submitted_ops(diff: RawDiff, claim_ops: list[ClaimOp]) -> int:
    return (
        len(claim_ops)
        + len(diff.memory_lifecycle_ops)
        + len(diff.relation_claim_ops)
        + len(diff.relation_frame_ops)
        + len(diff.edge_ops)
        + len(diff.ontology_gap_ops)
        + len(diff.open_question_ops)
        + len(diff.formation_resolutions)
        + len(diff.act_ops)
        + len(diff.resource_ops)
    )


def _enforce_region_containment(
    diff: RawDiff,
    *,
    claim_ops: list[ClaimOp],
    allowed_region: list[tuple[str, str]] | None,
    strict_region: bool,
) -> None:
    # Region membership is no longer a hard validation boundary. The
    # initial region is a retrieval/observability artifact; the hard
    # gates are tenant-bound existence checks and per-domain invariants.
    return


async def _record_validation_drop(
    diff: RawDiff,
    conn: asyncpg.Connection,
    *,
    op_type: str,
    op_kind: str,
    reason: str,
    error_message: str,
    original_op: Any,
) -> None:
    log_dropped_op(
        trigger_id=diff.trigger_ref,
        tenant_id=diff.tenant_id,
        op_kind=op_kind,
        op_type=op_type,
        failure_reason=reason,
        original_op=original_op,
    )
    await _emit_validation_drop_event(
        conn=conn,
        tenant_id=diff.tenant_id,
        op_type=op_type,
        op_kind=op_kind,
        reason=reason,
        error_message=error_message,
    )


async def _validate_claim_ops(
    diff: RawDiff,
    claim_ops: list[ClaimOp],
    retrieval_result: Any,
    conn: asyncpg.Connection,
    errors: list[str],
) -> list[ClaimOp]:
    validated: list[ClaimOp] = []
    for op in claim_ops:
        try:
            v_op = await _validate_claim_op(
                op, retrieval_result, conn, tenant_id=diff.tenant_id
            )
        except (FalsifierInadequateError, MalformedFalsifierError, ValidationError) as e:
            reason = _classify_claim_drop_reason(e)
            err_msg = e.message if hasattr(e, "message") else str(e)
            errors.append(f"claim_op {op.op}: {err_msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="claim",
                op_kind=op.op,
                reason=reason,
                error_message=err_msg,
                original_op=op,
            )
            continue
        if await _claim_target_missing(v_op, conn, tenant_id=diff.tenant_id):
            err_msg = f"model {v_op.model_id} not found"
            errors.append(f"claim_op {v_op.op}: {err_msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="claim",
                op_kind=v_op.op,
                reason="missing_model_reference",
                error_message=err_msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _claim_target_missing(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> bool:
    if op.op not in ("update", "archive") or op.model_id is None:
        return False
    exists = await conn.fetchval(
        "SELECT 1 FROM models WHERE tenant_id = $1 AND id = $2",
        tenant_id,
        op.model_id,
    )
    return not bool(exists)


def _pending_claim_basis_confidence(
    validated_claim_ops: list[ClaimOp],
) -> dict[UUID, float]:
    pending: dict[UUID, float] = {}
    for op in validated_claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        confidence = float(op.entry.get("confidence") or 0.0)
        for key in ("born_from_event_id", "model_id", "id"):
            placeholder_id = _coerce_uuid(op.entry.get(key))
            if placeholder_id is not None:
                pending[placeholder_id] = confidence
    return pending


async def _validate_edge_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_claim_basis_confidence: dict[UUID, float],
) -> tuple[list[EdgeOp], int]:
    validated: list[EdgeOp] = []
    neutralized_count = 0
    for op in diff.edge_ops:
        try:
            v_op = await _validate_edge_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=set(pending_claim_basis_confidence),
                pending_edge_ops=validated,
            )
        except (ValidationError, EdgeRegistryError) as e:
            reason = _classify_edge_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"edge_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="edge",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        if v_op is None:
            neutralized_count += 1
            continue
        validated.append(v_op)
    return validated, neutralized_count


async def _validate_memory_lifecycle_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
) -> list[MemoryLifecycleOp]:
    validated: list[MemoryLifecycleOp] = []
    for op in diff.memory_lifecycle_ops:
        try:
            v_op = await _validate_memory_lifecycle_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
            )
        except ValidationError as e:
            reason = _classify_memory_lifecycle_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"memory_lifecycle_op {op.action}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="memory_lifecycle",
                op_kind=op.action,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _validate_relation_claim_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_model_event_ids: set[UUID],
) -> list[RelationClaimOp]:
    validated: list[RelationClaimOp] = []
    for op in diff.relation_claim_ops:
        try:
            v_op = await _validate_relation_claim_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=pending_model_event_ids,
            )
        except (ValidationError, EdgeRegistryError) as e:
            if bool((op.metadata or {}).get("atomic_with_synthesis")):
                raise
            reason = _classify_relation_claim_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"relation_claim_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="relation_claim",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


def _resolve_authoritative_relation_retirements(
    ops: list[RelationClaimOp],
) -> list[RelationClaimOp]:
    """Prevent any producer from reasserting a relation retired in this diff."""

    retirement_keys = {
        key
        for op in ops
        if op.status == "retired"
        and op.write_policy == "no_edge"
        and (op.metadata or {}).get("relation_claim_origin")
        == "composite_correction_retirement"
        if (key := _validated_relation_identity(op)) is not None
    }
    if not retirement_keys:
        return ops
    return [
        op
        for op in ops
        if (
            _validated_relation_identity(op) not in retirement_keys
            or (
                op.status == "retired"
                and op.write_policy == "no_edge"
                and (op.metadata or {}).get("relation_claim_origin")
                == "composite_correction_retirement"
            )
        )
    ]


def _validated_relation_identity(
    op: RelationClaimOp,
) -> tuple[str, UUID, UUID] | None:
    if op.source_model_id is None or op.target_model_id is None:
        return None
    kind = {
        "dependency_constraint": "blocks",
        "enablement": "enables",
        "causal_influence": "causes",
        "predictive_indicator": "predicts",
    }.get(op.edge_kind, op.edge_kind)
    source, target = op.source_model_id, op.target_model_id
    if op.direction == "target_to_source":
        source, target = target, source
    return kind, source, target


async def _validate_relation_frame_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_model_event_ids: set[UUID],
) -> list[RelationFrameOp]:
    validated: list[RelationFrameOp] = []
    for op in diff.relation_frame_ops:
        try:
            v_op = await _validate_relation_frame_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=pending_model_event_ids,
            )
        except ValidationError as e:
            reason = _classify_relation_frame_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"relation_frame_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="relation_frame",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _validate_ontology_gap_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_claim_basis_confidence: dict[UUID, float],
) -> list[OntologyGapOp]:
    validated: list[OntologyGapOp] = []
    for op in diff.ontology_gap_ops:
        try:
            v_op = await _validate_ontology_gap_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=set(pending_claim_basis_confidence),
            )
        except (ValidationError, EdgeRegistryError) as e:
            reason = _classify_ontology_gap_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"ontology_gap_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="ontology_gap",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _validate_open_question_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_model_event_ids: set[UUID],
) -> list[OpenQuestionOp]:
    validated: list[OpenQuestionOp] = []
    for op in diff.open_question_ops:
        try:
            v_op = await _validate_open_question_op(
                op,
                conn,
                tenant_id=diff.tenant_id,
                pending_model_event_ids=pending_model_event_ids,
            )
        except ValidationError as e:
            reason = _classify_open_question_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"open_question_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="open_question",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _validate_formation_resolutions(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    formation_candidate_ids: set[str] | frozenset[str] | None,
) -> list[FormationResolutionOp]:
    validated: list[FormationResolutionOp] = []
    known_ids = formation_candidate_ids
    for op in diff.formation_resolutions:
        try:
            v_op = await _validate_formation_resolution(
                op,
                conn,
                tenant_id=diff.tenant_id,
                formation_candidate_ids=known_ids,
            )
        except ValidationError as e:
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"formation_resolution {op.candidate_id}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="formation_resolution",
                op_kind=op.resolution,
                reason="invalid_formation_resolution",
                error_message=msg,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


async def _validate_act_ops(
    diff: RawDiff,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    errors: list[str],
    *,
    pending_claim_basis_confidence: dict[UUID, float],
) -> tuple[list[ActOp], int]:
    validated: list[ActOp] = []
    neutralized_count = 0
    for op in diff.act_ops:
        try:
            v_op = await _validate_act_op(
                op,
                retrieval_result,
                conn,
                tenant_id=diff.tenant_id,
                pending_claim_basis_confidence=pending_claim_basis_confidence,
            )
        except (ValidationError, InvariantViolation, TrustTierError) as e:
            reason = _classify_act_drop_reason(e)
            msg = getattr(e, "message", None) or str(e)
            errors.append(f"act_op {op.op}: {msg}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="act",
                op_kind=op.op,
                reason=reason,
                error_message=msg,
                original_op=op,
            )
            continue
        if v_op is None:
            neutralized_count += 1
            continue
        validated.append(v_op)
    return validated, neutralized_count


async def _validate_resource_ops(
    diff: RawDiff,
    conn: asyncpg.Connection,
    errors: list[str],
) -> list[ResourceOp]:
    validated: list[ResourceOp] = []
    for op in diff.resource_ops:
        try:
            v_op = _validate_resource_op_shape(op)
        except ValidationError as e:
            reason = _classify_resource_drop_reason(e)
            errors.append(f"resource_op {op.op}: {e.message}")
            await _record_validation_drop(
                diff,
                conn,
                op_type="resource",
                op_kind=op.op,
                reason=reason,
                error_message=e.message,
                original_op=op,
            )
            continue
        validated.append(v_op)
    return validated


def _raise_if_every_op_failed(
    *,
    total_ops: int,
    errors: list[str],
    neutralized_op_count: int,
    validated_groups: tuple[list[Any], ...],
) -> None:
    any_survived = any(validated_groups)
    if total_ops > 0 and not any_survived and neutralized_op_count == 0:
        raise ValidationFailure(
            f"validation rejected {len(errors)}/{total_ops} ops "
            f"(every op failed)",
            errors=errors[:25],
            total=total_ops,
        )


# ---------------------------------------------------------------------
# Per-op validators
# ---------------------------------------------------------------------


async def _validate_claim_local_observation_evidence(
    entry: dict[str, Any],
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None,
) -> None:
    """Reject claim evidence that is not a same-tenant Observation.

    ``born_from_event_id`` is intentionally excluded: compiled reasoning uses
    it as a pending-model provenance placeholder, so treating it as evidence
    would convert an internal UUID into a false citation.
    """

    proposition = entry.get("proposition")
    proposition = proposition if isinstance(proposition, dict) else {}
    raw_ids = [
        *list(entry.get("supporting_event_ids") or []),
        *list(proposition.get("evidence_event_ids") or []),
    ]
    evidence_ids: list[UUID] = []
    seen: set[UUID] = set()
    for value in raw_ids:
        try:
            event_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"claim_op insert cites non-UUID observation evidence {value!r}"
            ) from exc
        if event_id not in seen:
            seen.add(event_id)
            evidence_ids.append(event_id)
    if not evidence_ids:
        return
    if tenant_id is None:
        raise ValidationError(
            "claim_op insert evidence cannot be authorized without tenant_id"
        )
    found = await conn.fetch(
        """
        SELECT id FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        """,
        tenant_id,
        evidence_ids,
    )
    found_ids = {row["id"] for row in found}
    missing = sorted((str(value) for value in set(evidence_ids) - found_ids))
    if missing:
        raise ValidationError(
            "claim_op insert cites missing or cross-tenant observation "
            f"evidence: {missing[:8]}"
        )


async def _validate_provisional_mention_scope(
    entry: dict[str, Any],
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None,
) -> None:
    """Reopen mention-scoped atomic authority at the final validation seam."""

    scopes = [
        item for item in (entry.get("scope_entities") or ())
        if isinstance(item, dict)
    ]
    mention_scopes = [
        item for item in scopes
        if str(item.get("type") or "").strip().casefold() == "mention"
    ]
    if not mention_scopes:
        return
    if tenant_id is None:
        raise ValidationError(
            "mention-scoped claim cannot be authorized without tenant_id"
        )
    if len(mention_scopes) != len(scopes):
        raise ValidationError(
            "mention-scoped claim cannot mix provisional and canonical scopes"
        )
    refs = {str(item.get("id") or "").strip() for item in mention_scopes}
    if len(mention_scopes) != 1 or len(refs) != 1:
        raise ValidationError(
            "mention-scoped claim requires exactly one provisional coordinate"
        )
    raw_ref = next(iter(refs))
    if not raw_ref.startswith("mention:"):
        raise ValidationError("mention scope requires mention:<uuid> identity")
    try:
        detection_id = UUID(raw_ref.removeprefix("mention:"))
    except ValueError as exc:
        raise ValidationError("mention scope requires mention:<uuid> identity") from exc

    proposition = entry.get("proposition")
    proposition = proposition if isinstance(proposition, dict) else {}
    if (
        str(proposition.get("abstraction_level") or "") != "atomic"
        or str(proposition.get("claim_role") or "") != "fact"
    ):
        raise ValidationError("mention-scoped claim must remain an atomic fact")
    mention_contract = proposition.get("mention_scope_contract")
    closed_contract = proposition.get("closed_atomic_contract")
    if not (
        isinstance(mention_contract, dict)
        and mention_contract.get("detection_ref") == raw_ref
        and mention_contract.get("canonical_identity_authority") is False
        and mention_contract.get("cross_observation_grouping_authority") is False
        and isinstance(closed_contract, dict)
        and closed_contract.get("compiler_entails_exact_text") is True
        and closed_contract.get("evidence_cardinality") == "singleton"
    ):
        raise ValidationError(
            "mention-scoped claim requires compiler-owned nonidentity authority"
        )
    if (
        entry.get("supporting_model_ids")
        or entry.get("contributing_models")
        or proposition.get("member_model_ids")
        or proposition.get("supported_relation")
    ):
        raise ValidationError(
            "mention-scoped claim cannot depend on supporting Models"
        )
    evidence_ids: set[UUID] = set()
    for raw in (
        *(entry.get("supporting_event_ids") or ()),
        *(proposition.get("evidence_event_ids") or ()),
    ):
        try:
            evidence_ids.add(raw if isinstance(raw, UUID) else UUID(str(raw)))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                "mention-scoped claim requires UUID observation evidence"
            ) from exc
    if len(evidence_ids) != 1:
        raise ValidationError(
            "mention-scoped claim requires exactly one supporting observation"
        )
    observation_id = next(iter(evidence_ids))
    row = await conn.fetchrow(
        """
        SELECT detection.source_observation_id,detection.fate,
               detection.candidate_surface,detection.mention,
               observation.content_text
        FROM entity_mention_detection_heads head
        JOIN entity_mention_detections detection
          ON detection.tenant_id=head.tenant_id
         AND detection.id=head.current_detection_id
        JOIN observations observation
          ON observation.tenant_id=detection.tenant_id
         AND observation.id=detection.source_observation_id
        WHERE head.tenant_id=$1
          AND head.current_detection_id=$2
          AND head.source_observation_id=$3
        """,
        tenant_id,
        detection_id,
        observation_id,
    )
    if row is None or row["fate"] != "detected":
        raise ValidationError(
            "mention scope is not the current detected head for its observation"
        )
    mention = row["mention"] or {}
    if isinstance(mention, str):
        try:
            mention = json.loads(mention)
        except json.JSONDecodeError as exc:
            raise ValidationError("mention scope has malformed anchor evidence") from exc
    anchor = mention.get("primary_anchor") if isinstance(mention, dict) else None
    coordinate = anchor.get("coordinate") if isinstance(anchor, dict) else None
    text = str(row["content_text"] or "")
    surface = str(
        (mention.get("surface") if isinstance(mention, dict) else None)
        or row["candidate_surface"]
        or ""
    )
    start = coordinate.get("span_start") if isinstance(coordinate, dict) else None
    end = coordinate.get("span_end") if isinstance(coordinate, dict) else None
    field_path = coordinate.get("field_path") if isinstance(coordinate, dict) else None
    if not (
        field_path == "content_text"
        and isinstance(start, int) and not isinstance(start, bool)
        and isinstance(end, int) and not isinstance(end, bool)
        and 0 <= start < end <= len(text)
        and surface
        and text[start:end] == surface
    ):
        raise ValidationError(
            "mention scope does not reconstruct an exact content_text anchor"
        )
    if " ".join(str(entry.get("natural") or "").split()) != " ".join(text.split()):
        raise ValidationError(
            "mention-scoped atomic must preserve the exact observation assertion"
        )


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
        causal_evidence_cap = _confidence_cap_for_causal_evidence(entry)
        if causal_evidence_cap is not None:
            conf = min(conf, causal_evidence_cap)
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
        _normalize_hypothesis_assertion_alias(entry)
        validate_proposition(entry.get("proposition"))
        await _validate_claim_local_observation_evidence(
            entry, conn, tenant_id=tenant_id,
        )
        await _validate_provisional_mention_scope(
            entry, conn, tenant_id=tenant_id,
        )
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
        reason = str(op.reason).strip()
        if reason not in _ALLOWED_MODEL_ARCHIVE_REASONS:
            raise ValidationError(
                "claim_op archive reason must be a registered lifecycle reason",
                reason=op.reason,
                allowed=sorted(_ALLOWED_MODEL_ARCHIVE_REASONS),
            )
        return op.model_copy(update={"reason": reason})

    raise ValidationError(f"unknown claim_op: {op.op!r}")


async def _validate_memory_lifecycle_op(
    op: MemoryLifecycleOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> MemoryLifecycleOp:
    rationale = (op.rationale or "").strip()
    if not rationale:
        raise ValidationError("memory_lifecycle_op requires rationale")
    if op.confidence is not None and not (0.0 <= float(op.confidence) <= 1.0):
        raise ValidationError("memory_lifecycle_op confidence must be in [0, 1]")
    if op.confidence_delta is not None and not (
        -0.95 <= float(op.confidence_delta) <= 0.95
    ):
        raise ValidationError(
            "memory_lifecycle_op confidence_delta must be in [-0.95, 0.95]"
        )
    if op.action in {"archive", "supersede"}:
        default_reason = "superseded" if op.action == "supersede" else "decay"
        reason = str(op.reason or default_reason).strip()
        if reason not in _ALLOWED_MODEL_ARCHIVE_REASONS:
            raise ValidationError(
                "memory_lifecycle_op archive reason must be a registered lifecycle reason",
                reason=op.reason,
                allowed=sorted(_ALLOWED_MODEL_ARCHIVE_REASONS),
            )
        op = op.model_copy(update={"reason": reason, "rationale": rationale})
    else:
        op = op.model_copy(update={"rationale": rationale})

    model_ids = [op.model_id, *op.evidence_model_ids]
    if op.superseded_by_model_id is not None:
        model_ids.append(op.superseded_by_model_id)
    rows = await conn.fetch(
        """
        SELECT id, status
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(dict.fromkeys(model_ids)),
    )
    found = {row["id"]: row["status"] for row in rows}
    missing = [str(model_id) for model_id in model_ids if model_id not in found]
    if missing:
        raise ValidationError(
            f"memory_lifecycle_op references {len(missing)} missing model(s)",
            missing=missing,
        )
    if found.get(op.model_id) != "active":
        raise ValidationError(
            "memory_lifecycle_op target model must be active",
            model_id=str(op.model_id),
            status=found.get(op.model_id),
        )
    if op.action == "supersede" and op.superseded_by_model_id is None:
        raise ValidationError("memory_lifecycle_op supersede requires superseded_by_model_id")

    broad_event_ids = set(op.evidence_event_ids)
    claim_local_event_ids = set(op.claim_local_evidence_event_ids)
    if not claim_local_event_ids <= broad_event_ids:
        raise ValidationError(
            "memory_lifecycle_op claim-local evidence must be a subset of its "
            "declared observation evidence"
        )
    if op.action == "confirm" and not claim_local_event_ids:
        raise ValidationError(
            "memory_lifecycle_op confirm requires claim-local observation evidence"
        )

    if op.evidence_event_ids:
        rows = await conn.fetch(
            """
            SELECT id
            FROM observations
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            list(dict.fromkeys(op.evidence_event_ids)),
        )
        found_events = {row["id"] for row in rows}
        missing_events = [
            str(event_id)
            for event_id in op.evidence_event_ids
            if event_id not in found_events
        ]
        if missing_events:
            raise ValidationError(
                f"memory_lifecycle_op references {len(missing_events)} missing observation(s)",
                missing=missing_events,
            )

    if (
        op.action in {"confirm", "falsify", "revise", "unchanged"}
        and not op.evidence_event_ids
        and not op.evidence_model_ids
    ):
        raise ValidationError("memory_lifecycle_op requires evidence")

    return op


async def _validate_act_op(
    op: ActOp,
    retrieval_result: Any,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_claim_basis_confidence: dict[UUID, float] | None = None,
) -> ActOp | None:
    """
    Threshold + state-machine validation for an Act op.
    """
    basis = await _load_basis_model(conn, op.confidence_basis, tenant_id=tenant_id)
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
        scope = ent.get("scope")
        if scope is not None and not isinstance(scope, dict):
            raise ValidationError("create_decision entity.scope must be an object")
        revisit_triggers = ent.get("revisit_triggers")
        if revisit_triggers is not None and not isinstance(revisit_triggers, dict):
            raise ValidationError(
                "create_decision entity.revisit_triggers must be an object"
            )
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
            "SELECT state FROM commitments WHERE tenant_id = $1 AND id = $2",
            tenant_id,
            cid,
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
            await _verify_doneverified_evidence(
                conn,
                resolved_uuids,
                tenant_id=tenant_id,
            )

    if op.op == "transition_goal":
        gid = op.entity.get("id")
        new_state = op.entity.get("new_state")
        if gid is None or new_state is None:
            raise ValidationError(
                "transition_goal requires entity.id and entity.new_state",
            )
        row = await conn.fetchrow(
            "SELECT state FROM goals WHERE tenant_id = $1 AND id = $2",
            tenant_id,
            gid,
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
            "SELECT state FROM decisions WHERE tenant_id = $1 AND id = $2",
            tenant_id,
            did,
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
    from services.reasoning.relationships.ontology_runtime import (
        resolve_edge_kind_spec,
        validate_weight_for_spec,
    )
    from services.reasoning.think.edge_semantics import (
        canonicalize_edge_semantics,
        enforce_edge_specificity,
        normalize_edge_review_status,
    )

    if op.source_model_id == op.target_model_id:
        raise ValidationError(
            "edge_op self-edge not allowed",
            model_id=str(op.source_model_id),
        )
    op = await canonicalize_edge_semantics(
        op,
        conn,
        tenant_id=tenant_id,
        pending_model_event_ids=pending_model_event_ids,
    )
    spec = await resolve_edge_kind_spec(
        conn,
        tenant_id=tenant_id,
        kind=op.edge_kind,
    )
    validate_weight_for_spec(spec, op.weight)
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
        SELECT
            id, status, "natural", proposition, scope_entities, scope_actors,
            domain_tags, claim_role
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        [op.source_model_id, op.target_model_id],
    )
    found = {r["id"]: r["status"] for r in rows}
    rows_by_id = {r["id"]: r for r in rows}
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
        op = normalize_edge_review_status(
            op,
            endpoint_models_verified=all(
                mid in found for mid in (op.source_model_id, op.target_model_id)
            ),
        )
        op = enforce_edge_specificity(
            op,
            source_model=rows_by_id.get(op.source_model_id),
            target_model=rows_by_id.get(op.target_model_id),
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


def _relation_ref_model_id(ref: Any) -> UUID | None:
    if not isinstance(ref, dict) or ref.get("kind") != "model":
        return None
    raw_model_id = ref.get("model_id")
    if raw_model_id is None:
        return None
    try:
        return UUID(str(raw_model_id))
    except (TypeError, ValueError):
        return None


def _normalize_relation_claim_endpoints(op: RelationClaimOp) -> RelationClaimOp:
    source_model_id = op.source_model_id or _relation_ref_model_id(op.subject_ref)
    target_model_id = op.target_model_id or _relation_ref_model_id(op.object_ref)
    if source_model_id is not None and target_model_id is not None:
        endpoint_binding_status = "bound"
    elif source_model_id is not None or target_model_id is not None:
        endpoint_binding_status = "partially_bound"
    else:
        endpoint_binding_status = "unbound"
    if (
        source_model_id == op.source_model_id
        and target_model_id == op.target_model_id
        and endpoint_binding_status == op.endpoint_binding_status
    ):
        return op
    return op.model_copy(
        update={
            "source_model_id": source_model_id,
            "target_model_id": target_model_id,
            "endpoint_binding_status": endpoint_binding_status,
        }
    )


async def _validate_relation_claim_op(
    op: RelationClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID],
) -> RelationClaimOp:
    from services.reasoning.relationships.ontology_runtime import (
        resolve_edge_kind_spec,
        validate_weight_for_spec,
    )

    if op.op != "upsert":
        raise ValidationError(f"unknown relation_claim_op: {op.op!r}")
    if not op.predicate.strip():
        raise ValidationError("relation_claim_op requires predicate")
    if not op.edge_kind.strip():
        raise ValidationError("relation_claim_op requires edge_kind")
    if not (0.0 <= float(op.confidence) <= 1.0):
        raise ValidationError(
            "relation_claim_op confidence must be in [0, 1]",
            confidence=op.confidence,
        )
    if not (0.0 <= float(op.binding_confidence) <= 1.0):
        raise ValidationError(
            "relation_claim_op binding_confidence must be in [0, 1]",
            binding_confidence=op.binding_confidence,
        )
    op = _normalize_relation_claim_endpoints(op)
    if (
        op.source_model_id is not None
        and op.target_model_id is not None
        and op.source_model_id == op.target_model_id
    ):
        raise ValidationError("relation_claim_op cannot use same source/target model")

    has_bound_endpoints = op.source_model_id is not None and op.target_model_id is not None
    if op.write_policy == "accepted_edge" and not has_bound_endpoints:
        raise ValidationError("accepted relation claim requires bound endpoints")
    if op.endpoint_binding_status == "bound" and not has_bound_endpoints:
        raise ValidationError("bound relation claim requires source and target models")

    if has_bound_endpoints:
        rows = await conn.fetch(
            """
            SELECT
                id, status, "natural", proposition, scope_entities, scope_actors,
                domain_tags, claim_role
            FROM models
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            [op.source_model_id, op.target_model_id],
        )
        found = {row["id"]: row["status"] for row in rows}
        rows_by_id = {row["id"]: row for row in rows}
        endpoint_models_verified = all(
            model_id in found for model_id in (op.source_model_id, op.target_model_id)
        )
        missing = [
            str(model_id)
            for model_id in (op.source_model_id, op.target_model_id)
            if model_id not in found and model_id not in pending_model_event_ids
        ]
        if missing:
            raise ValidationError(
                f"relation_claim_op references {len(missing)} missing model(s)",
                missing=missing,
            )
        inactive = [
            str(model_id)
            for model_id, status in found.items()
            if status != "active"
        ]
        if inactive:
            raise ValidationError(
                "accepted relation claim requires active model endpoints",
                inactive=inactive,
            )
        op = await _canonicalize_relation_claim_semantics(
            op,
            conn,
            tenant_id=tenant_id,
            pending_model_event_ids=pending_model_event_ids,
            endpoint_models_verified=endpoint_models_verified,
            source_model=rows_by_id.get(op.source_model_id),
            target_model=rows_by_id.get(op.target_model_id),
        )

    spec = await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=op.edge_kind)
    op = _normalize_relation_claim_weight(op, spec)
    validate_weight_for_spec(spec, op.weight)

    if op.write_policy != "no_edge" and not _relation_claim_has_evidence(op):
        raise ValidationError("relation_claim_op requires evidence or explanation")

    update: dict[str, Any] = {}
    if has_bound_endpoints and op.endpoint_binding_status != "bound":
        update["endpoint_binding_status"] = "bound"
        update["binding_confidence"] = max(float(op.binding_confidence), 0.8)
    forced_review = _relation_claim_forced_review(op)
    if forced_review and op.write_policy == "accepted_edge":
        update["write_policy"] = "needs_review"
        update["status"] = "needs_review"
    elif (
        has_bound_endpoints
        and op.write_policy in {"candidate", "needs_review"}
        and float(op.confidence) >= 0.68
        and not forced_review
        and _relation_claim_can_auto_accept_as_edge(op)
        and _relation_claim_has_evidence(op)
    ):
        update["write_policy"] = "accepted_edge"
        update["status"] = "accepted"
    elif op.write_policy == "accepted_edge":
        update["status"] = "accepted"
    elif op.status == "accepted" and op.write_policy != "accepted_edge":
        update["status"] = "candidate"
    if update:
        op = op.model_copy(update=update)
    return op


def _normalize_relation_claim_weight(
    op: RelationClaimOp,
    spec: Any,
) -> RelationClaimOp:
    weight = op.weight
    if not spec.weight_allowed:
        weight = None
    elif weight is None and spec.weight_required:
        weight = min(1.0, max(0.05, float(op.confidence)))
    if weight == op.weight:
        return op
    return op.model_copy(update={"weight": weight})


async def _canonicalize_relation_claim_semantics(
    op: RelationClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID],
    endpoint_models_verified: bool,
    source_model: Any | None = None,
    target_model: Any | None = None,
) -> RelationClaimOp:
    if op.source_model_id is None or op.target_model_id is None:
        return op
    edge_proxy = EdgeOp(
        op="add",
        source_model_id=op.source_model_id,
        target_model_id=op.target_model_id,
        edge_kind=op.edge_kind,
        weight=op.weight,
        confidence=op.confidence,
        evidence_event_ids=op.evidence_event_ids,
        evidence_model_ids=op.evidence_model_ids,
        explanation=_relation_claim_semantic_text(op),
        metadata=dict(op.metadata or {}),
        review_status=(
            "accepted"
            if op.write_policy == "accepted_edge" or op.status == "accepted"
            else "candidate"
        ),
    )
    refined = await canonicalize_edge_semantics(
        edge_proxy,
        conn,
        tenant_id=tenant_id,
        pending_model_event_ids=pending_model_event_ids,
    )
    refined = normalize_edge_review_status(
        refined,
        endpoint_models_verified=endpoint_models_verified,
    )
    refined = enforce_edge_specificity(
        refined,
        source_model=source_model,
        target_model=target_model,
    )
    update: dict[str, Any] = {}
    if refined.edge_kind != op.edge_kind:
        update["edge_kind"] = refined.edge_kind
        if op.predicate.strip() == op.edge_kind:
            update["predicate"] = refined.edge_kind
    if refined.weight != edge_proxy.weight:
        update["weight"] = refined.weight
    if refined.explanation != edge_proxy.explanation and refined.explanation:
        update["explanation"] = refined.explanation
    if refined.metadata != edge_proxy.metadata:
        update["metadata"] = refined.metadata
    if (
        refined.review_status == "accepted"
        and op.status != "retired"
        and op.write_policy != "no_edge"
        and op.write_policy != "accepted_edge"
        and not _relation_claim_forced_review(op)
    ):
        update["write_policy"] = "accepted_edge"
        update["status"] = "accepted"
    return op.model_copy(update=update) if update else op


def _relation_claim_forced_review(op: RelationClaimOp) -> bool:
    if not isinstance(op.metadata, dict):
        return False
    return op.metadata.get("review_status_downgraded_by") in {
        "edge_specificity_guard",
        "mutation_compiler_cycle_guard",
        "relation_authorization_guard",
    } or bool(op.metadata.get("mutation_compiler_cycle_guard"))


def _relation_claim_semantic_text(op: RelationClaimOp) -> str:
    return " ".join(
        part
        for part in (
            op.predicate,
            op.explanation or "",
            op.evidence_text or "",
        )
        if str(part or "").strip()
    )


def _relation_claim_can_auto_accept_as_edge(op: RelationClaimOp) -> bool:
    return op.edge_kind not in {
        "supports",
        "same_issue_as",
        "analogous_to",
        "co_occurs_with",
        "alternative_to",
    }


def _relation_claim_has_evidence(op: RelationClaimOp) -> bool:
    return bool(
        op.evidence_event_ids
        or op.evidence_model_ids
        or (op.evidence_text or "").strip()
        or (op.explanation or "").strip()
    )


async def _validate_relation_frame_op(
    op: RelationFrameOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID],
) -> RelationFrameOp:
    if op.op != "upsert":
        raise ValidationError(f"unknown relation_frame_op: {op.op!r}")
    relation_kind = _normalize_relation_role(op.relation_kind)
    if not relation_kind:
        raise ValidationError("relation_frame_op requires relation_kind")
    if len(op.participants) < 2:
        raise ValidationError("relation_frame_op requires at least two participants")
    if len(op.participants) > 12:
        raise ValidationError("relation_frame_op cannot exceed 12 participants")
    if not (0.0 <= float(op.confidence) <= 1.0):
        raise ValidationError(
            "relation_frame_op confidence must be in [0, 1]",
            confidence=op.confidence,
        )

    seen: set[tuple[UUID, str]] = set()
    model_ids: set[UUID] = set()
    participants = []
    for participant in op.participants:
        role = _normalize_relation_role(participant.role)
        if not role:
            raise ValidationError("relation_frame_op participant requires role")
        if not (0.0 <= float(participant.binding_confidence) <= 1.0):
            raise ValidationError(
                "relation_frame_op participant binding_confidence must be in [0, 1]",
                binding_confidence=participant.binding_confidence,
            )
        key = (participant.model_id, role)
        if key in seen:
            raise ValidationError("duplicate relation_frame_op participant")
        seen.add(key)
        model_ids.add(participant.model_id)
        if role != participant.role:
            participant = participant.model_copy(update={"role": role})
        participants.append(participant)
    if len(model_ids) < 2:
        raise ValidationError("relation_frame_op requires at least two distinct models")

    rows = await conn.fetch(
        """
        SELECT id, status FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(model_ids),
    )
    found = {row["id"]: row["status"] for row in rows}
    missing = [
        str(model_id)
        for model_id in model_ids
        if model_id not in found and model_id not in pending_model_event_ids
    ]
    if missing:
        raise ValidationError(
            f"relation_frame_op references {len(missing)} missing model(s)",
            missing=missing,
        )
    inactive = [
        str(model_id)
        for model_id, status in found.items()
        if status != "active"
    ]
    if inactive:
        raise ValidationError(
            "relation_frame_op requires active model participants",
            inactive=inactive,
        )

    update: dict[str, Any] = {}
    if relation_kind != op.relation_kind:
        update["relation_kind"] = relation_kind
    if participants != op.participants:
        update["participants"] = participants
    if op.write_policy == "project_edges":
        if op.participant_binding_status != "bound":
            update["participant_binding_status"] = "bound"
        if op.status != "accepted":
            update["status"] = "accepted"
    elif op.status == "accepted":
        update["status"] = "candidate"
    return op.model_copy(update=update) if update else op


def _normalize_relation_role(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:80]


_PROPOSED_EDGE_KIND_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


async def _validate_ontology_gap_op(
    op: OntologyGapOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID] | None = None,
) -> OntologyGapOp:
    if op.source_model_id == op.target_model_id:
        raise ValidationError(
            "ontology_gap_op self-edge example not allowed",
            model_id=str(op.source_model_id),
        )
    proposed = (op.proposed_edge_kind or "").strip()
    if not _PROPOSED_EDGE_KIND_RE.match(proposed):
        raise ValidationError(
            "ontology_gap_op proposed_edge_kind must be snake_case",
            proposed_edge_kind=op.proposed_edge_kind,
        )
    from services.reasoning.relationships.ontology_runtime import is_edge_kind_writable

    if await is_edge_kind_writable(conn, tenant_id=tenant_id, kind=proposed):
        raise ValidationError(
            "ontology_gap_op proposed_edge_kind already exists; use edge_ops",
            proposed_edge_kind=proposed,
        )
    if len((op.description or "").strip()) < 12:
        raise ValidationError("ontology_gap_op requires a description")
    if len((op.relationship_summary or "").strip()) < 12:
        raise ValidationError("ontology_gap_op requires relationship_summary")
    dropped = [str(item).strip() for item in op.dropped_dimensions if str(item).strip()]
    if not dropped:
        raise ValidationError(
            "ontology_gap_op requires dropped_dimensions explaining semantic loss"
        )

    for label, kind in (
        ("parent_kind", op.parent_kind),
        ("nearest_existing_kind", op.nearest_existing_kind),
    ):
        if kind is None:
            continue
        if str(kind).strip() not in EDGE_REGISTRY:
            raise ValidationError(
                f"ontology_gap_op {label} fallback edge kind is not registered",
                **{label: kind},
            )

    for label, value in (
        ("confidence", op.confidence),
        ("impact", op.impact),
        ("actionability", op.actionability),
        ("urgency", op.urgency),
        ("uncertainty", op.uncertainty),
        ("authority_required", op.authority_required),
        ("novelty", op.novelty),
    ):
        if not (0.0 <= float(value) <= 1.0):
            raise ValidationError(
                f"ontology_gap_op {label} score must be in [0, 1]",
                score=value,
            )

    model_ids = [op.source_model_id, op.target_model_id, *op.evidence_model_ids]
    rows = await conn.fetch(
        """
        SELECT id, status FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(dict.fromkeys(model_ids)),
    )
    found = {r["id"]: r["status"] for r in rows}
    pending_model_event_ids = pending_model_event_ids or set()
    missing = [
        str(mid)
        for mid in model_ids
        if mid not in found and mid not in pending_model_event_ids
    ]
    if missing:
        raise ValidationError(
            f"ontology_gap_op references {len(missing)} missing model(s)",
            missing=missing,
        )
    inactive = [
        str(mid)
        for mid in (op.source_model_id, op.target_model_id)
        if mid in found and found[mid] != "active"
    ]
    if inactive:
        raise ValidationError(
            "ontology_gap_op requires active model endpoints",
            inactive=inactive,
        )

    return op.model_copy(
        update={
            "proposed_edge_kind": proposed,
            "description": op.description.strip(),
            "relationship_summary": op.relationship_summary.strip(),
            "dropped_dimensions": dropped,
        }
    )


async def _validate_open_question_op(
    op: OpenQuestionOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_model_event_ids: set[UUID] | None = None,
) -> OpenQuestionOp:
    pending_model_event_ids = pending_model_event_ids or set()
    if op.model_id is None:
        raise ValidationError("open_question_op requires model_id")
    model_id = op.model_id
    if model_id not in pending_model_event_ids:
        row = await conn.fetchrow(
            """
            SELECT id, status
            FROM models
            WHERE tenant_id = $1
              AND id = $2
            """,
            tenant_id,
            model_id,
        )
        if row is None:
            raise ValidationError("open_question_op references missing model")
        if row["status"] != "active" and op.op == "insert":
            raise ValidationError("open_question_op insert requires active model")

    updates: dict[str, Any] = {}
    question_type = normalize_question_type(op.question_type)
    if question_type != op.question_type:
        updates["question_type"] = question_type

    for field_name in ("expected_resolution_signal", "search_signature"):
        if not isinstance(getattr(op, field_name), dict):
            raise ValidationError(f"open_question_op {field_name} must be JSON object")

    if op.op == "insert":
        question = normalize_question_text(op.question)
        if len(question) < 12:
            raise ValidationError("open_question_op insert requires question text")
        if not (0.0 <= float(op.priority) <= 1.0):
            raise ValidationError("open_question_op priority must be in [0, 1]")
        source_model_ids = _dedupe_uuid_sequence(op.source_model_ids)
        missing = await _missing_model_ids(
            conn,
            tenant_id=tenant_id,
            model_ids=source_model_ids,
            pending_model_event_ids=pending_model_event_ids,
        )
        if missing:
            raise ValidationError(
                f"open_question_op references {len(missing)} missing model(s)",
                missing=[str(mid) for mid in missing],
            )
        if question != op.question:
            updates["question"] = question
        if source_model_ids != op.source_model_ids:
            updates["source_model_ids"] = source_model_ids
        # Force normalization to run during validation so invalid all-punctuation
        # questions cannot collapse to an empty active dedupe key at apply time.
        if not dedupe_key_for_question(question):
            raise ValidationError("open_question_op question is too low-signal")
        return op.model_copy(update=updates) if updates else op

    question_id = op.question_id or op.id
    if question_id is None:
        raise ValidationError(f"open_question_op {op.op} requires question_id")
    question_row = await conn.fetchrow(
        """
        SELECT id, model_id, status
        FROM model_open_questions
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        question_id,
    )
    if question_row is None:
        raise ValidationError("open_question_op references missing open question")
    if question_row["model_id"] != model_id:
        raise ValidationError("open_question_op model_id does not match question")
    if question_row["status"] != "open":
        raise ValidationError("open_question_op target question is not open")
    if op.question_id is None:
        updates["question_id"] = question_id
    status = op.status or ("resolved" if op.op == "resolve" else "archived")
    if status == "open" or status not in OPEN_QUESTION_STATUSES:
        raise ValidationError("open_question_op terminal status is invalid")
    updates["status"] = status
    if op.resolution_model_id is not None:
        missing = await _missing_model_ids(
            conn,
            tenant_id=tenant_id,
            model_ids=[op.resolution_model_id],
            pending_model_event_ids=pending_model_event_ids,
        )
        if missing:
            raise ValidationError("open_question_op resolution_model_id not found")
    return op.model_copy(update=updates) if updates else op


async def _validate_formation_resolution(
    op: FormationResolutionOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    formation_candidate_ids: set[str] | frozenset[str] | None,
) -> FormationResolutionOp:
    candidate_id = str(op.candidate_id or "").strip()
    if not candidate_id:
        raise ValidationError("formation_resolution requires candidate_id")
    if formation_candidate_ids is not None and candidate_id not in formation_candidate_ids:
        raise ValidationError("formation_resolution references unknown candidate_id")
    if not str(op.rationale or "").strip():
        raise ValidationError("formation_resolution requires rationale")
    updates: dict[str, Any] = {}
    output_model_ids = _dedupe_uuid_sequence(op.output_model_ids or [])
    if len(output_model_ids) != len(op.output_model_ids or []):
        updates["output_model_ids"] = output_model_ids
    if output_model_ids:
        missing = await _missing_model_ids(
            conn,
            tenant_id=tenant_id,
            model_ids=output_model_ids,
            pending_model_event_ids=set(),
        )
        if missing:
            raise ValidationError("formation_resolution output_model_ids not found")
    return op.model_copy(update=updates) if updates else op


def _dedupe_uuid_sequence(values: Sequence[UUID]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


async def _missing_model_ids(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: Sequence[UUID],
    pending_model_event_ids: set[UUID],
) -> list[UUID]:
    ids = [mid for mid in dict.fromkeys(model_ids) if mid not in pending_model_event_ids]
    if not ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        ids,
    )
    found = {row["id"] for row in rows}
    return [mid for mid in ids if mid not in found]


async def _edge_would_create_cycle(
    op: EdgeOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pending_edge_ops: list[EdgeOp] | None = None,
) -> bool:
    from services.reasoning.relationships.ontology_runtime import resolve_edge_kind_spec

    spec = await resolve_edge_kind_spec(
        conn,
        tenant_id=tenant_id,
        kind=op.edge_kind,
    )
    scope = spec.cycle_scope
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
