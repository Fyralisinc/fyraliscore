"""services/think/cascade.py — cascade engine per spec §3 pseudo-code.

Called post-apply with the initial event (typically a state_change
Observation emitted by apply). BFS over downstream effects, bounded by
`max_depth=50` and a `visited` set. Each cascade step emits its own
state_change via `emit_state_change(cause_id=prior_event)`.

Three branches per §3 `compute_downstream`:
  A. Commitment state change:
     - dependents in 'blocked' whose dependency is this one AND whose
       other deps are all satisfied → transition to 'active'.
     - contributes_to Goals where is_critical_path=True → recompute
       cached_health.
     - served_by Customer Resources when new_state ∈ {doneverified,
       closed} → recompute customer health metadata + emit state_change.
  B. Decision revisited:
     - every constrained_by Commitment gets a 'flag_for_review'
       state_change (NO auto-transition).
  C. Resource terminal (archive / expire):
     - Commitments with active deployment on this Resource get a
       'resource_released' state_change (informational).

Cap: depth 50. On breach: log a `cascade_bound_violation` state_change
+ stop. Do NOT raise (a bounded-with-violation cascade is more useful
than a throw that rolls back the whole Think run).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7

from services.acts import commitments as commitments_svc
from services.acts import goals as goals_svc
from services.acts.invariants import is_unsatisfied_dependency
from services.observations.state_change import emit_state_change


_log = structlog.get_logger(__name__)

_MAX_DEPTH = 50

# ---------------------------------------------------------------------
# TK-3 — Cross-trigger cascade depth bound.
#
# The intra-BFS cascade in this module already caps at `_MAX_DEPTH=50`.
# TK-3 adds a second, cross-trigger bound: when a state_change
# observation is used to seed a NEW T1 trigger (e.g. by a future
# subscriber), the `cascade_depth` counter is carried in the new
# trigger's payload and propagated forward. `ThinkWorker._process_trigger`
# reads this counter; on a new T1 whose `cascade_depth >= MAX_CASCADE_DEPTH`
# we log a structured `cascade_bound_violation` event, mark the trigger
# failed non-retryable, and do not dispatch it.
#
# Emitters of state_change → T1 triggers must call
# `enqueue_cascade_t1(...)` below rather than building the payload by
# hand so the depth counter is propagated consistently.
# ---------------------------------------------------------------------

MAX_CASCADE_DEPTH = _MAX_DEPTH


def propagate_cascade_depth(
    parent_payload: dict[str, Any] | None,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build a new trigger payload that increments `cascade_depth` relative
    to `parent_payload`. `extra` is merged on top (useful for seed
    fields like `seed_natural_text`).

    Used by any path that emits a NEW T1 from a state_change inside a
    Think cycle. If `parent_payload` is None, the new payload starts at
    `cascade_depth=1` (the parent itself was implicitly at depth 0).
    """
    parent_depth = 0
    if parent_payload is not None:
        raw = parent_payload.get("cascade_depth")
        if isinstance(raw, int):
            parent_depth = raw
    out: dict[str, Any] = {"cascade_depth": parent_depth + 1}
    if extra:
        out.update(extra)
    return out


async def enqueue_cascade_t1(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    parent_payload: dict[str, Any] | None,
    subkind: str = "state_change",
    extra_payload: dict[str, Any] | None = None,
) -> UUID | None:
    """
    Insert a new T1 row in `think_trigger_queue` whose payload carries
    a propagated `cascade_depth`. Returns the new trigger id, or None
    if the depth budget would be exceeded (in which case a structured
    `cascade_bound_violation` event is logged and the trigger is NOT
    enqueued — this is the pre-emptive bound).

    Callers that emit state_change observations which should route
    back through Think MUST use this helper rather than INSERTing
    directly.
    """
    payload = propagate_cascade_depth(parent_payload, extra=extra_payload)
    depth = int(payload["cascade_depth"])
    if depth >= MAX_CASCADE_DEPTH:
        _log.warning(
            "cascade_bound_violation",
            stage="t1_enqueue_suppressed",
            tenant_id=str(tenant_id),
            observation_id=str(observation_id),
            cascade_depth=depth,
            max_cascade_depth=MAX_CASCADE_DEPTH,
        )
        return None
    import json as _json
    new_id = uuid7()
    await conn.execute(
        """
        INSERT INTO think_trigger_queue (
            id, tenant_id, trigger_kind, trigger_subkind,
            observation_id, payload
        ) VALUES ($1, $2, 'T1', $3, $4, $5::jsonb)
        """,
        new_id,
        tenant_id,
        subkind,
        observation_id,
        _json.dumps(payload),
    )
    return new_id


@dataclass
class CascadeEvent:
    """
    A single cascade step. `entity_kind`/`entity_id` identify the
    mutated entity; `kind` is the semantic label (e.g.
    'commitment_state_change', 'goal_health_recomputed').

    `observation_id` is the state_change Observation that corresponds
    to this step; subsequent steps chain `cause_id` = this id.
    """

    id: UUID
    kind: str
    entity_kind: str
    entity_id: UUID
    tenant_id: UUID
    metadata: dict[str, Any] = field(default_factory=dict)
    observation_id: UUID | None = None


@dataclass
class CascadeResult:
    events_visited: int
    depth_reached: int
    bound_violated: bool
    steps: list[CascadeEvent] = field(default_factory=list)


async def cascade(
    trigger_event: CascadeEvent,
    conn: asyncpg.Connection,
    *,
    max_depth: int = _MAX_DEPTH,
    tenant_id: UUID | None = None,
) -> CascadeResult:
    """
    BFS cascade starting from `trigger_event`. Always runs inside the
    caller's transaction on `conn`. Returns a CascadeResult.

    `tenant_id` is optional fallback for legacy callers; defaults to
    trigger_event.tenant_id.
    """
    if tenant_id is None:
        tenant_id = trigger_event.tenant_id

    queue: list[tuple[CascadeEvent, int]] = [(trigger_event, 0)]
    visited: set[UUID] = {trigger_event.id}
    steps: list[CascadeEvent] = [trigger_event]
    depth_reached = 0
    bound_violated = False

    while queue:
        event, depth = queue.pop(0)
        if depth >= max_depth:
            bound_violated = True
            vio_id = uuid7()
            await emit_state_change(
                conn,
                kind="cascade_bound_violation",
                entity_id=event.entity_id,
                tenant_id=tenant_id,
                cause_event_id=event.observation_id,
                entity_kind=event.entity_kind,
                metadata={
                    "max_depth": max_depth,
                    "depth_at_abort": depth,
                    "initial_event_id": str(trigger_event.id),
                },
            )
            _log.warning(
                "cascade.bound_violation",
                depth=depth, max_depth=max_depth,
                entity_id=str(event.entity_id),
            )
            break

        depth_reached = max(depth_reached, depth)

        downstream = await _compute_downstream(event, conn, tenant_id)
        for follow in downstream:
            if follow.id in visited:
                continue
            visited.add(follow.id)
            steps.append(follow)
            queue.append((follow, depth + 1))

    return CascadeResult(
        events_visited=len(visited),
        depth_reached=depth_reached,
        bound_violated=bound_violated,
        steps=steps,
    )


# =====================================================================
# compute_downstream — the three branches
# =====================================================================


async def _compute_downstream(
    event: CascadeEvent,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[CascadeEvent]:
    """
    Dispatch per `event.kind`. Each branch may call into repos (which
    themselves emit state_changes) and returns the resulting
    CascadeEvents for BFS.
    """
    if event.kind == "commitment_state_change":
        return await _branch_commitment_state(event, conn, tenant_id)
    if event.kind == "decision_revisited":
        return await _branch_decision_revisited(event, conn, tenant_id)
    if event.kind == "resource_terminal":
        return await _branch_resource_terminal(event, conn, tenant_id)
    return []


# -----------------------------------------------------------------
# Branch A — commitment state change
# -----------------------------------------------------------------


async def _branch_commitment_state(
    event: CascadeEvent,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[CascadeEvent]:
    out: list[CascadeEvent] = []
    commitment_id = event.entity_id
    new_state = event.metadata.get("new_state")

    # (a) Dependents whose sole unsatisfied dep was this one → unblock.
    if new_state == "doneverified":
        dependents = await conn.fetch(
            """
            SELECT c.id
            FROM commitments c
            JOIN depends_on d ON d.dependent_commitment_id = c.id
            WHERE d.dependency_commitment_id = $1
              AND c.state = 'blocked'
              AND c.tenant_id = $2
            """,
            commitment_id,
            tenant_id,
        )
        for dep in dependents:
            dep_id = dep["id"]
            # Check all dependencies are now satisfied.
            remaining = await conn.fetch(
                """
                SELECT dependency_commitment_id
                FROM depends_on
                WHERE dependent_commitment_id = $1
                """,
                dep_id,
            )
            any_unsatisfied = False
            for r in remaining:
                if await is_unsatisfied_dependency(conn, r["dependency_commitment_id"]):
                    any_unsatisfied = True
                    break
            if any_unsatisfied:
                continue
            # Attempt transition. `blocked → active` is legal.
            try:
                await commitments_svc.transition(
                    dep_id,
                    "active",
                    cause_event_id=event.observation_id,
                    conn=conn,
                )
                # Emit a synthetic cascade event so BFS can continue.
                cascade_ev = CascadeEvent(
                    id=uuid7(),
                    kind="commitment_state_change",
                    entity_kind="commitment",
                    entity_id=dep_id,
                    tenant_id=tenant_id,
                    metadata={"new_state": "active", "from_cascade": True},
                )
                # Link the observation chain via an explicit
                # cascade_unblock state_change (extra audit hop).
                obs_id = await emit_state_change(
                    conn,
                    kind="commitment_auto_unblocked",
                    entity_id=dep_id,
                    tenant_id=tenant_id,
                    cause_event_id=event.observation_id,
                    entity_kind="commitment",
                    metadata={"unblocked_by": str(commitment_id)},
                )
                cascade_ev.observation_id = obs_id
                out.append(cascade_ev)
            except InvariantViolation as e:
                _log.info(
                    "cascade.unblock_rejected",
                    commitment_id=str(dep_id),
                    reason=str(e.message),
                )

    # (b) Goal cached_health recompute for critical-path contributes_to.
    goal_rows = await conn.fetch(
        """
        SELECT g.id, g.cached_health
        FROM goals g
        JOIN contributes_to ct ON ct.goal_id = g.id
        WHERE ct.commitment_id = $1
          AND ct.is_critical_path = TRUE
          AND g.tenant_id = $2
        """,
        commitment_id,
        tenant_id,
    )
    for gr in goal_rows:
        gid = gr["id"]
        prior = gr["cached_health"]
        new_health = await goals_svc.recompute_cached_health(gid, conn)
        if new_health != prior:
            obs_id = await emit_state_change(
                conn,
                kind="goal_health_recomputed",
                entity_id=gid,
                tenant_id=tenant_id,
                cause_event_id=event.observation_id,
                entity_kind="goal",
                metadata={"prior": prior, "new": new_health},
            )
            out.append(
                CascadeEvent(
                    id=uuid7(),
                    kind="goal_health_recomputed",
                    entity_kind="goal",
                    entity_id=gid,
                    tenant_id=tenant_id,
                    metadata={"prior": prior, "new": new_health},
                    observation_id=obs_id,
                )
            )

    # (c) Customer Resource served_by — recompute when terminal.
    if new_state in ("doneverified", "closed"):
        customer_rows = await conn.fetch(
            """
            SELECT r.id, r.current_value
            FROM resources r
            JOIN customer_commitments cc ON cc.customer_resource_id = r.id
            WHERE cc.commitment_id = $1
              AND r.tenant_id = $2
              AND r.archived_at IS NULL
            """,
            commitment_id,
            tenant_id,
        )
        for cr in customer_rows:
            cid = cr["id"]
            # Compute current revenue_at_risk via Bridge primitive.
            from services.resources.bridge import revenue_at_risk_for_customer
            try:
                rar = await revenue_at_risk_for_customer(cid, conn=conn)
            except Exception:
                rar = None
            obs_id = await emit_state_change(
                conn,
                kind="customer_health_recomputed",
                entity_id=cid,
                tenant_id=tenant_id,
                cause_event_id=event.observation_id,
                entity_kind="customer_resource",
                metadata={
                    "trigger_commitment_id": str(commitment_id),
                    "trigger_new_state": new_state,
                    "revenue_at_risk": str(rar) if rar is not None else None,
                },
            )
            out.append(
                CascadeEvent(
                    id=uuid7(),
                    kind="customer_health_recomputed",
                    entity_kind="customer_resource",
                    entity_id=cid,
                    tenant_id=tenant_id,
                    metadata={"revenue_at_risk": str(rar) if rar else None},
                    observation_id=obs_id,
                )
            )

    return out


# -----------------------------------------------------------------
# Branch B — decision revisited
# -----------------------------------------------------------------


async def _branch_decision_revisited(
    event: CascadeEvent,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[CascadeEvent]:
    out: list[CascadeEvent] = []
    decision_id = event.entity_id

    rows = await conn.fetch(
        """
        SELECT c.id
        FROM commitments c
        JOIN constrained_by cb ON cb.commitment_id = c.id
        WHERE cb.decision_id = $1 AND c.tenant_id = $2
          AND c.state NOT IN ('doneverified', 'closed')
        """,
        decision_id,
        tenant_id,
    )
    for r in rows:
        cid = r["id"]
        obs_id = await emit_state_change(
            conn,
            kind="commitment_flagged_for_review",
            entity_id=cid,
            tenant_id=tenant_id,
            cause_event_id=event.observation_id,
            entity_kind="commitment",
            metadata={
                "reason": "decision_revisited",
                "decision_id": str(decision_id),
            },
        )
        out.append(
            CascadeEvent(
                id=uuid7(),
                kind="commitment_flagged_for_review",
                entity_kind="commitment",
                entity_id=cid,
                tenant_id=tenant_id,
                metadata={
                    "reason": "decision_revisited",
                    "decision_id": str(decision_id),
                },
                observation_id=obs_id,
            )
        )
    return out


# -----------------------------------------------------------------
# Branch C — resource terminal
# -----------------------------------------------------------------


async def _branch_resource_terminal(
    event: CascadeEvent,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[CascadeEvent]:
    out: list[CascadeEvent] = []
    resource_id = event.entity_id

    rows = await conn.fetch(
        """
        SELECT rd.commitment_id
        FROM resource_deployments rd
        WHERE rd.resource_id = $1 AND rd.released_at IS NULL
        """,
        resource_id,
    )
    for r in rows:
        cid = r["commitment_id"]
        obs_id = await emit_state_change(
            conn,
            kind="commitment_flagged_for_review",
            entity_id=cid,
            tenant_id=tenant_id,
            cause_event_id=event.observation_id,
            entity_kind="commitment",
            metadata={
                "reason": "resource_terminal",
                "resource_id": str(resource_id),
            },
        )
        out.append(
            CascadeEvent(
                id=uuid7(),
                kind="commitment_flagged_for_review",
                entity_kind="commitment",
                entity_id=cid,
                tenant_id=tenant_id,
                metadata={
                    "reason": "resource_terminal",
                    "resource_id": str(resource_id),
                },
                observation_id=obs_id,
            )
        )
    return out


__all__ = [
    "CascadeEvent",
    "CascadeResult",
    "cascade",
    "MAX_CASCADE_DEPTH",
    "propagate_cascade_depth",
    "enqueue_cascade_t1",
]
