"""services/think/thresholds.py — pure `compute_threshold` per spec §7.

Returns the minimum confidence required on an ActOp's `confidence_basis`
Model for the op to be accepted. The validator calls this once per
ActOp.

Pure function: no DB, no imports beyond stdlib / diff_schema.
"""
from __future__ import annotations

from typing import Any

from .diff_schema import ActOp


_BASELINE: dict[str, float] = {
    # Commitment ops
    "create_commitment": 0.55,
    "transition_commitment_to_active": 0.50,
    "transition_commitment_to_blocked": 0.60,
    "transition_commitment_to_paused": 0.55,
    "transition_commitment_to_doneunverified": 0.65,
    "transition_commitment_to_doneverified": 0.80,
    "transition_commitment_to_closed": 0.70,
    # Goal ops
    "create_goal": 0.50,
    "update_goal": 0.50,
    "update_goal_health": 0.0,          # deterministic cascade; no LLM basis needed
    "transition_goal": 0.55,
    # Decision ops
    "create_decision": 0.65,
    "transition_decision": 0.70,
    "transition_decision_to_revisited": 0.70,
    "transition_decision_to_archived": 0.75,
    # Edges
    "add_edge_contributes_to": 0.55,
    "add_edge_depends_on": 0.55,
    "add_edge_constrained_by": 0.55,
}

_MIN = 0.30
_MAX = 0.95


def _transition_key(op: ActOp) -> str | None:
    """
    For transition ops, the threshold depends on the target state.
    Compose the lookup key: `transition_commitment_to_<state>` etc.
    """
    if op.op == "transition_commitment":
        new_state = op.entity.get("new_state")
        if isinstance(new_state, str):
            return f"transition_commitment_to_{new_state}"
    if op.op == "transition_decision":
        new_state = op.entity.get("new_state")
        if isinstance(new_state, str):
            return f"transition_decision_to_{new_state}"
    if op.op == "transition_goal":
        new_state = op.entity.get("new_state")
        if isinstance(new_state, str):
            # Goals share one baseline for now — no per-state split.
            return "transition_goal"
    return None


def _is_external_counterparty(entity: dict[str, Any]) -> bool:
    ref = entity.get("external_counterparty_ref")
    if isinstance(ref, dict) and ref:
        return True
    return False


def _is_critical_path(entity: dict[str, Any]) -> bool:
    """
    `is_critical_path` may be set directly on the entity (when Think is
    creating a commitment with contributes_to edges that are critical)
    or via a downstream flag populated by the retrieval assembler.
    """
    if entity.get("is_critical_path") is True:
        return True
    # contributes_to list with any (goal_id, is_critical=True) tuple.
    cts = entity.get("contributes_to_goal_ids") or []
    for item in cts:
        if isinstance(item, (list, tuple)) and len(item) >= 2 and bool(item[1]):
            return True
    return False


def _first_person_override(
    op: ActOp,
    basis: dict[str, Any] | None,
) -> bool:
    """
    Spec §7 first-person override: when the confidence basis is a
    'contestation' Model whose `contesting_actor` (first element of
    scope_actors) is in the entity's scope_actors, lower the threshold
    by 0.15.

    basis is the dict-form of the Model row (or None if no basis is
    set — deterministic cascade ops).
    """
    if basis is None:
        return False
    prop_kind = basis.get("proposition_kind")
    if prop_kind != "contestation":
        return False
    basis_actors = basis.get("scope_actors") or []
    if not basis_actors:
        return False
    contesting_actor = basis_actors[0]
    entity_actors = op.entity.get("scope_actors") or op.entity.get(
        "owner_candidates"
    ) or []
    # Also check owner_id / contributors single-actor fields for a match.
    single_actors: list[Any] = []
    owner_id = op.entity.get("owner_id")
    if owner_id is not None:
        single_actors.append(owner_id)
    return any(
        str(contesting_actor) == str(a)
        for a in list(entity_actors) + single_actors
    )


def compute_threshold(
    op: ActOp,
    basis: dict[str, Any] | None,
    context: Any | None = None,
) -> float:
    """
    Baseline by op-kind + modulators:

      * external counterparty on the entity  → +0.10
      * critical path on the entity           → +0.05
      * first-person override (contestation
        basis in entity scope_actors)          → -0.15

    Clipped to [0.30, 0.95].

    Pure function. `context` is accepted for forward-compat with spec
    §7 signature but currently unused — the entity dict carries
    everything we need.
    """
    key = _transition_key(op) or op.op
    baseline = _BASELINE.get(key, 0.60)
    threshold = baseline

    entity = op.entity or {}
    if _is_external_counterparty(entity):
        threshold += 0.10
    if _is_critical_path(entity):
        threshold += 0.05
    if _first_person_override(op, basis):
        threshold -= 0.15

    if threshold < _MIN:
        return _MIN
    if threshold > _MAX:
        return _MAX
    return threshold


__all__ = ["compute_threshold"]
