"""services/think/diff_schema.py — validated diff schema.

Spec §7 "Diff schema". This is what the LLM is asked to produce, and
what `validator.py` → `applier.py` consume.

Pydantic discriminated unions on `op` so the schema hint the LLM
sees is precise. The validator downstream adds falsifier adequacy /
threshold / trust-tier / region-containment checks on top of the
pure-Pydantic shape.
"""
from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# =====================================================================
# ClaimOp — Model insert / update / archive.
# =====================================================================


class ClaimOp(BaseModel):
    """
    A mutation over the Models surface.

    - op='insert': `entry` MUST be ModelCreate-compatible dict (the
      validator wraps it in a `ModelCreate`).
    - op='update': `model_id` required; `changes` is a shallow-merge
      dict of (column → new value). Allowed columns enumerated in
      `applier.py._ALLOWED_MODEL_UPDATE_COLUMNS`.
    - op='archive': `model_id` + `reason` required; reason is a
      `ModelArchiveReason` literal.
    - op='relocate' (S4): deliberate repositioning of one Model in
      topology space. Closes the substrate's reasoning loop —
      arrangement is now a first-class diff op, not just a derived
      property of the edge graph. Required:
        * `model_id` — the Model being moved.
        * `relocate_target` — `{"kind": "model_id"|"vector"|
          "neighborhood_id", "value": <uuid|list[float]|uuid>,
          "alpha": <float in (0,1]>}`. `alpha` is the blend factor
          between the Model's current topo and the target topo
          (1.0 = full snap to target; 0.5 = halfway; etc.).
          Defaults to 1.0 if omitted (snap).
        * `reason` — short string for audit trail.
      Cascades through `topo_dirty_queue` with bounded fan-out via
      [TopoRepo.bounded_cascade](services/topology/topo_repo.py).
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["insert", "update", "archive", "relocate"]
    # For insert:
    entry: dict[str, Any] | None = None
    # For update / archive / relocate:
    model_id: UUID | None = None
    changes: dict[str, Any] | None = None
    reason: str | None = None
    # For relocate (S4):
    relocate_target: dict[str, Any] | None = None


# =====================================================================
# ActOp — Goal / Commitment / Decision create, transition, edge adds.
# =====================================================================


# Enumerated subset of the spec's act-op vocabulary that we currently
# support end-to-end. Wave 5 can add more (delete_edge, ambition_change,
# etc.) when UI needs them.
ActOpKind = Literal[
    "create_goal",
    "update_goal",
    "transition_goal",
    "create_commitment",
    "transition_commitment",
    "create_decision",
    "transition_decision",
    "add_edge_contributes_to",
    "add_edge_depends_on",
    "add_edge_constrained_by",
]


class ActOp(BaseModel):
    """
    A mutation over the Acts surface.

    `confidence_basis` is the Model id whose confidence justifies the
    Act. `compute_threshold` (services/think/thresholds.py) computes
    the minimum confidence; the validator rejects the op if
    basis.confidence < threshold.

    `entity` holds the operation-specific payload:
      - create_*:      the row to insert (fields mirror the repo signature)
      - update_*:      { id, ...changes }
      - transition_*:  { id, new_state, resolved_by_event_ids? }
      - add_edge_*:    { commitment_id, goal_id, ... } per edge kind
    """

    model_config = ConfigDict(extra="forbid")

    op: ActOpKind
    confidence_basis: UUID | None = None
    entity: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# ResourceOp — Resource create / update / deploy / release / transaction.
# =====================================================================


ResourceOpKind = Literal[
    "create",
    "transaction",
    "deploy",
    "release",
    "update",
]


class ResourceOp(BaseModel):
    """
    A mutation over the Resources surface.

    - op='create':     `payload` is the create kwargs (kind / identity /
                        current_value / ...).
    - op='update':     `resource_id` + `patch`.
    - op='transaction': `resource_id` + `kind` ('acquire'/'deploy'/...)
                        + `delta` (jsonb).
    - op='deploy':     `resource_id` + `commitment_id` + `quantity`.
    - op='release':    `resource_id` + `commitment_id` [+ `actual_quantity`].
    """

    model_config = ConfigDict(extra="forbid")

    op: ResourceOpKind
    resource_id: UUID | None = None
    commitment_id: UUID | None = None
    payload: dict[str, Any] | None = None
    patch: dict[str, Any] | None = None
    kind: str | None = None            # for op='transaction'
    delta: dict[str, Any] | None = None
    quantity: dict[str, Any] | None = None
    actual_quantity: dict[str, Any] | None = None


# =====================================================================
# ValidatedDiff — the top-level container.
# =====================================================================


class ValidatedDiff(BaseModel):
    """
    A fully validated diff ready for apply. The LLM produces this shape
    directly (via `LLMProvider.structured(schema=ValidatedDiff)`); the
    validator re-checks each op and drops the ones that fail.

    `trigger_ref` MUST be the `trigger_id` from the trigger queue row.
    This is the idempotency key that `applied_triggers` is keyed on.
    """

    model_config = ConfigDict(extra="forbid")

    trigger_ref: UUID
    tenant_id: UUID
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    act_ops: list[ActOp] = Field(default_factory=list)
    resource_ops: list[ResourceOp] = Field(default_factory=list)
    # Predictions that should be scheduled with the deadline resolver
    # post-commit. Must be ClaimOps with op='insert' and an
    # `evaluate_at` in their entry.
    new_predictions: list[ClaimOp] = Field(default_factory=list)
    # Freeform reasoning trace — stored on think_runs.ops_applied so the
    # LLM's chain-of-thought is reconstructable if debugging a bad run.
    reasoning_trace: str | None = None
    # Partial-accept bookkeeping: the validator keeps good ops and drops
    # bad ones rather than rejecting the whole diff. These fields let
    # reason.py record how many ops were dropped + why, without breaking
    # the surface the applier consumes.
    dropped_op_count: int = 0
    dropped_op_errors: list[str] = Field(default_factory=list)


# =====================================================================
# RawDiff — what the LLM produces and what deterministic handlers return
# =====================================================================


class RawDiff(BaseModel):
    """
    Pre-validation diff shape. Identical fields to ValidatedDiff but
    used to make the "before validation" stage explicit in type
    signatures. The LLM returns this; the validator converts it to a
    ValidatedDiff after filtering invalid ops.
    """

    model_config = ConfigDict(extra="forbid")

    trigger_ref: UUID
    tenant_id: UUID
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    act_ops: list[ActOp] = Field(default_factory=list)
    resource_ops: list[ResourceOp] = Field(default_factory=list)
    new_predictions: list[ClaimOp] = Field(default_factory=list)
    reasoning_trace: str | None = None


__all__ = [
    "ClaimOp",
    "ActOp",
    "ActOpKind",
    "ResourceOp",
    "ResourceOpKind",
    "ValidatedDiff",
    "RawDiff",
]
