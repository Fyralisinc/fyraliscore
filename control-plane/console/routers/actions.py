"""actions — the A3 ACTION QUEUE feature router (bounded, pull-based ops).

Roadmap A3: an operator queues a *named* remote action for a deployment; the
outbound-only agent PULLS the desired state, sees the pending action, executes
the matching handler, and acks it back on its heartbeat; the console renders the
unacked actions as drift until the agent converges.

This is deliberately a **bounded** queue: an action's ``type`` MUST be drawn from
the CLOSED :data:`lib.desired_state.ACTION_ALLOWLIST`
(``re-pull-config``/``force-reconcile``/``trigger-backfill``/``flush-dlq``). There
is NO generic "run this command" action, so the action queue can never become a
remote shell — the agent only ever runs a handler it already ships for a known
type (I2 outbound-only, no remote exec).

Invariants honored here:
  * I4 — every WRITE (POST a new action) is gated by ``deps.require_operator``
    (the operator bearer, distinct from the agent's console token).
  * I5 — every WRITE is appended to the hash-chained audit trail via
    ``deps.audit.append`` BEFORE returning.
  * I6/I2 — the queue carries no payload the agent will exec verbatim; ``params``
    are advisory inputs to a named, agent-owned handler from the closed allowlist.

Endpoints (mounted by the foundation's router-plugin loop — this file never edits
``app.py``):
  * ``POST /api/v1/deployments/{id}/actions``  (operator-authed) — enqueue an
    action ``{type, params}``; ``type`` must be in the allowlist (else 400). The
    console mints the ``id`` + ``created_at`` and appends it to
    ``pending_actions`` via ``store.put_desired`` (the durable home).
  * ``GET  /api/v1/deployments/{id}/actions`` — list pending + acked actions and
    the per-action status (``acked`` once the agent reports its id under the
    applied facet ``acked_action_ids``). Read-only; open on the operator LAN.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Body, Depends, HTTPException
from pydantic import BaseModel, Field

from lib.desired_state import ACTION_ALLOWLIST, DesiredState


def _now_iso() -> str:
    """A UTC ISO-8601 timestamp (``created_at`` for a queued action)."""
    return datetime.now(timezone.utc).isoformat()


class _ActionRequest(BaseModel):
    """The operator's POST body for enqueuing one action."""

    # extra="ignore" so a richer future body still parses; only type/params used.
    model_config = {"extra": "ignore"}

    type: str = Field(..., description="action type; MUST be in ACTION_ALLOWLIST")
    params: Dict[str, Any] = Field(default_factory=dict)


def register(app, deps) -> None:
    """Mount the A3 action-queue endpoints onto ``app`` using ``deps``."""

    @app.post(
        "/api/v1/deployments/{deployment_id}/actions",
        tags=["actions"],
        summary="Queue a bounded, allowlisted remote action for a deployment (A3).",
        dependencies=[Depends(deps.require_operator)],
    )
    def enqueue_action(
        deployment_id: str,
        body: _ActionRequest = Body(...),
    ) -> Dict[str, Any]:
        action_type = (body.type or "").strip()
        # Closed allowlist only — reject anything that is not a known, agent-owned
        # action type. This is the wall that keeps the queue from being an exec.
        if action_type not in ACTION_ALLOWLIST:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"action type {action_type!r} not allowed; "
                    f"must be one of {sorted(ACTION_ALLOWLIST)}"
                ),
            )

        # Build the action record (console mints id + created_at, never the caller).
        action = {
            "id": uuid.uuid4().hex,
            "type": action_type,
            "params": dict(body.params or {}),
            "created_at": _now_iso(),
        }

        # Append onto the deployment's DESIRED state, preserving everything else.
        existing = deps.store.get_desired(deployment_id)
        if existing is None:
            existing = DesiredState(deployment_id=deployment_id)
        pending = list(existing.pending_actions) + [action]
        updated = existing.model_copy(
            update={
                "deployment_id": deployment_id,
                "pending_actions": pending,
                "updated_by": "operator",
                "updated_at": action["created_at"],
                "reason": f"queue action {action_type}",
            }
        )
        deps.store.put_desired(deployment_id, updated)

        # I5: audit the operator write (hash-chained), best-effort, before return.
        deps.audit.append(
            {
                "actor": "operator",
                "action": "actions.enqueue",
                "target": deployment_id,
                "metadata": {
                    "action_id": action["id"],
                    "type": action_type,
                    "params": action["params"],
                },
            }
        )

        return {"ok": True, "deployment_id": deployment_id, "action": action}

    @app.get(
        "/api/v1/deployments/{deployment_id}/actions",
        tags=["actions"],
        summary="List queued actions + their ack status for a deployment (A3).",
    )
    def list_actions(deployment_id: str) -> Dict[str, Any]:
        desired = deps.store.get_desired(deployment_id)
        applied = deps.store.get_applied(deployment_id) or {}
        acked_ids = set(str(x) for x in (applied.get("acked_action_ids") or []))

        pending: List[Dict[str, Any]] = []
        if desired is not None:
            pending = list(desired.pending_actions)

        actions: List[Dict[str, Any]] = []
        for a in pending:
            aid = str(a.get("id", ""))
            actions.append(
                {
                    "id": a.get("id"),
                    "type": a.get("type"),
                    "params": a.get("params", {}),
                    "created_at": a.get("created_at"),
                    "status": "acked" if aid in acked_ids else "pending",
                }
            )

        pending_ids = [a["id"] for a in actions if a["status"] == "pending"]
        acked_list = [a["id"] for a in actions if a["status"] == "acked"]
        return {
            "deployment_id": deployment_id,
            "actions": actions,
            "pending": pending_ids,
            "acked": acked_list,
            "allowlist": sorted(ACTION_ALLOWLIST),
        }
