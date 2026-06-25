"""desired_state — the shared DESIRED-state model for the BYOC control surface.

The console roadmap (§2, §4) turns the read-only console into a **desired-state
reconciliation surface**: an operator WRITES desired state for a deployment; the
outbound-only agent PULLS it, VERIFIES it (I6), applies it, and reports the
APPLIED facets back on its heartbeat; the console renders the DRIFT (desired ≠
applied) until they converge.

This module is the ONE shared data model both sides agree on. It is importable by
BOTH the console (which writes/stores it) and the agent (which pulls/reconciles
it) — both already ``import lib``. It carries no transport, no crypto, no I/O: it
is a pure pydantic model plus the drift computation, so it can be unit-tested in
isolation and never drags a network/crypto dependency into either side.

Invariant anchoring
-------------------
* **I1** desired state is config/control metadata, never customer data (no PII).
* **I3** desired state is *advisory*: if the console is down the agent keeps its
  last-applied state. Nothing here forces an apply; the agent decides.
* **I4** desired state is scoped per ``deployment_id``.
* **I5** every operator write of this state is audited (the console does that).
* **I6** ``desired_config_sig`` carries the detached signature + manifest so the
  agent can VERIFY a config bundle before applying it; ``pending_actions`` are
  drawn from a CLOSED :data:`ACTION_ALLOWLIST` (no generic remote ``exec``).

The model is canonical-JSON friendly: :meth:`DesiredState.to_dict` /
:meth:`DesiredState.from_dict` round-trip through plain JSON-able dicts so the
console can persist it and the agent can pull it over HTTP without surprises.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "ACTION_ALLOWLIST",
    "DesiredState",
    "compute_drift",
]

# The CLOSED set of remote actions an operator may queue for an agent to pull and
# execute (roadmap A3). Deliberately a small, named allowlist — there is NO
# generic "run this command" action, so the action queue can never become a
# remote shell. A feature agent that adds a new action type MUST extend this set
# (and the matching agent handler) explicitly.
ACTION_ALLOWLIST: frozenset[str] = frozenset(
    {
        "re-pull-config",
        "force-reconcile",
        "trigger-backfill",
        "flush-dlq",
    }
)


class DesiredState(BaseModel):
    """The operator-written DESIRED facet for a single ``deployment_id`` (§4).

    Every field is optional/defaulted so a partially-specified desired state is
    valid: an operator who only sets ``desired_release`` does not have to also
    restate the config. Extra fields are ignored so a richer future payload still
    parses on an older agent (forward-compat).
    """

    model_config = ConfigDict(extra="ignore")

    deployment_id: str = ""

    # --- desired config (roadmap A1 / A4) ---------------------------------
    # {telemetry_tier: "T1"|"T2"|"T3", interval_s, sampling, feature_flags:{}}
    desired_config: Optional[Dict[str, Any]] = None
    # Monotonic; the console bumps it on each config write so the agent (and the
    # drift view) can tell "is what I applied the latest the operator wants?".
    desired_config_version: int = 0
    # {sig, manifest, signed_by} — the detached ed25519 signature + manifest for
    # ``desired_config`` so the agent can VERIFY before applying (I6). None when
    # no config has been pushed yet.
    desired_config_sig: Optional[Dict[str, Any]] = None

    # --- desired release (roadmap A2) -------------------------------------
    desired_release: Optional[str] = None

    # --- license / entitlement (roadmap B3) -------------------------------
    license_state: str = Field(default="active")  # "active" | "suspended"

    # --- bounded pull-based action queue (roadmap A3) ---------------------
    # [{id, type, params, created_at}] ; type MUST be in ACTION_ALLOWLIST.
    pending_actions: List[Dict[str, Any]] = Field(default_factory=list)

    # --- provenance (for the audit trail / drill-down) --------------------
    updated_by: str = ""
    updated_at: str = ""
    reason: str = ""

    # -- canonical-JSON-friendly (de)serialization -------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Plain JSON-able dict of the full desired state (stable key set).

        Uses pydantic's ``model_dump`` so the shape is exactly the field set
        above — safe to persist, sign, hash, or ship over HTTP.
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DesiredState":
        """Rebuild a :class:`DesiredState` from a (possibly partial) dict.

        Unknown keys are ignored (forward-compat); missing keys take defaults.
        """
        return cls(**(data or {}))

    def pending_action_ids(self) -> List[str]:
        """The ids of the currently-pending actions (order preserved)."""
        ids: List[str] = []
        for a in self.pending_actions:
            aid = a.get("id")
            if aid is not None:
                ids.append(str(aid))
        return ids


def compute_drift(desired: "DesiredState", applied: Dict[str, Any]) -> Dict[str, Any]:
    """Compute DRIFT between the operator's desired state and what the agent
    last reported applying.

    ``applied`` is the agent-reported applied facet (as stored by the console's
    ``record_applied`` / sent on the heartbeat): the relevant keys are
    ``applied_config_version`` (int), ``applied_release`` (str|None),
    ``acked_action_ids`` (list[str]) and ``license_state_applied`` (str).

    Returns ``{config, release, actions, license}`` where:

      * ``config``  (bool)  — True if ``applied_config_version`` is behind
        ``desired_config_version`` (the agent has not yet applied the latest
        config the operator wants).
      * ``release`` (bool)  — True if a ``desired_release`` is set and differs
        from ``applied_release`` (None desired ⇒ no release drift).
      * ``actions`` (list)  — the ids of pending actions the agent has NOT yet
        acked (queued-but-unapplied remote ops).
      * ``license`` (bool)  — True if the desired ``license_state`` differs from
        the agent's ``license_state_applied``.

    A ``True`` (or non-empty ``actions``) means **drifted** — the console renders
    it until the agent's next heartbeat reports convergence (§2 step 5).
    """
    applied = applied or {}

    # config drift: the agent is behind the latest desired config version.
    applied_cfg_v = applied.get("applied_config_version", 0)
    try:
        applied_cfg_v = int(applied_cfg_v)
    except (TypeError, ValueError):
        applied_cfg_v = 0
    config_drift = applied_cfg_v < int(desired.desired_config_version)

    # release drift: a desired release is set and the agent is not on it.
    desired_rel = desired.desired_release
    applied_rel = applied.get("applied_release")
    release_drift = bool(desired_rel) and (desired_rel != applied_rel)

    # action drift: pending action ids the agent has not acked yet.
    acked = set(str(x) for x in (applied.get("acked_action_ids") or []))
    unacked = [aid for aid in desired.pending_action_ids() if aid not in acked]

    # license drift: desired license_state differs from what the agent applied.
    applied_lic = applied.get("license_state_applied", desired.license_state)
    license_drift = desired.license_state != applied_lic

    return {
        "config": config_drift,
        "release": release_drift,
        "actions": unacked,
        "license": license_drift,
    }
