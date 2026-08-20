"""license — the B3 LICENSE / ENTITLEMENT feature router (console-roadmap §4, B3).

An operator suspends or re-activates a deployment's entitlement by WRITING its
``license_state`` into the deployment's DESIRED state. The outbound-only agent
PULLS that desired state, reflects it (``agent/reconcile/license.py``), and reports
``license_state_applied`` on its heartbeat; the console renders the drift until the
agent converges. There is NO inbound channel to the agent — suspension is delivered
by the same pull-based reconcile loop as config/release (I2).

Endpoints
---------
* ``POST /api/v1/deployments/{id}/license``  (operator-authed, audited — I4/I5)
  body ``{state: "active"|"suspended", reason?: str}`` — sets ``license_state`` on
  the deployment's desired state WITHOUT disturbing its config/release/actions
  (a license toggle must not silently roll back the desired config). Returns the
  updated license facet.
* ``GET  /api/v1/deployments/{id}/license``   (operator READ — open on the
  operator LAN, no token) — the current desired ``license_state`` plus what the
  agent last reported applying (``license_state_applied``) and whether they drift.

Invariants
----------
* **I4** — the WRITE is guarded by ``deps.require_operator`` (operator identity,
  distinct from the agent's console token) and scoped to one ``deployment_id``.
* **I5** — every write is hash-chain audited via ``deps.audit.append``.
* **I1** — license_state is control metadata, never customer data.

This module follows the foundation's router contract: a module-level
``register(app, deps)`` that mounts onto ``app`` and reaches everything off
``deps``; it NEVER edits ``app.py``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Body, Depends, HTTPException

from lib.desired_state import DesiredState, compute_drift
from lib.primitives import utcnow

# The closed set of license states an operator may set (mirrors the model's
# documented "active" | "suspended" domain — no free-form license_state writes).
_LICENSE_STATES = ("active", "suspended")


def _iso_now() -> str:
    """RFC3339 'now' for the desired-state provenance stamp."""
    return utcnow().isoformat()


def _license_view(desired: Optional[DesiredState], applied: Dict[str, Any]) -> Dict[str, Any]:
    """The current license facet: desired vs applied + drift, for the GET/POST body."""
    desired = desired or DesiredState()
    drift = compute_drift(desired, applied or {})
    return {
        "deployment_id": desired.deployment_id,
        "license_state": desired.license_state,
        "license_state_applied": (applied or {}).get(
            "license_state_applied"
        ),
        "drift": bool(drift.get("license")),
        "updated_by": desired.updated_by,
        "updated_at": desired.updated_at,
        "reason": desired.reason,
    }


def register(app, deps) -> None:
    """Mount the B3 license endpoints onto ``app`` using ``deps``."""

    @app.get(
        "/api/v1/deployments/{deployment_id}/license",
        tags=["license"],
        summary="Current desired license_state + applied state + drift (operator read).",
    )
    def get_license(deployment_id: str) -> Dict[str, Any]:
        # Operator READ — open on the operator LAN (only WRITES require the token,
        # per the operator-auth contract). Never 404s: a deployment with no desired
        # state yet defaults to "active" (the unset/normal entitlement).
        desired = deps.store.get_desired(deployment_id)
        applied = deps.store.get_applied(deployment_id)
        return _license_view(desired, applied)

    @app.post(
        "/api/v1/deployments/{deployment_id}/license",
        tags=["license"],
        summary="Set a deployment's license_state (active|suspended) — operator write.",
        dependencies=[Depends(deps.require_operator)],
    )
    def set_license(
        deployment_id: str,
        body: Dict[str, Any] = Body(...),
    ) -> Dict[str, Any]:
        state = str((body or {}).get("state", "")).strip().lower()
        if state not in _LICENSE_STATES:
            raise HTTPException(
                status_code=400,
                detail=f"state must be one of {list(_LICENSE_STATES)}, got {state!r}",
            )
        reason = str((body or {}).get("reason", "")).strip()

        # Read-modify-write: preserve any existing desired config / release /
        # pending actions; a license toggle must NOT roll those back. A deployment
        # with no desired state yet starts from a fresh DesiredState.
        current = deps.store.get_desired(deployment_id) or DesiredState(
            deployment_id=deployment_id
        )
        updated = current.model_copy(
            update={
                "deployment_id": deployment_id,
                "license_state": state,
                "updated_by": "operator",
                "updated_at": _iso_now(),
                "reason": reason,
            }
        )
        stored = deps.store.put_desired(deployment_id, updated)

        # I5: audit the operator write (best-effort; never rolls back the write).
        deps.audit.append(
            {
                "actor": "operator",
                "action": "license.set",
                "target": deployment_id,
                "metadata": {"license_state": state, "reason": reason},
            }
        )

        applied = deps.store.get_applied(deployment_id)
        return _license_view(stored, applied)
