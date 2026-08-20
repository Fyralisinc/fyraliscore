"""license — the B3 LICENSE / ENTITLEMENT reconcile handler (console-roadmap §4).

The operator writes a deployment's desired ``license_state`` (``active`` |
``suspended``) into its DESIRED state; this handler REFLECTS it on the agent and
reports it back as the ``license_state_applied`` facet so the console can render
license drift.

What "reflect" means (the composition contract)
-----------------------------------------------
The agent already has a SIGNED, locally-verified license (``license_check.py`` /
``Agent.is_licensed()``). A ``suspended`` desired state is an operator-side
ENTITLEMENT REVOCATION that must compose ON TOP of that local check:

    is_licensed  ==  (local signed license is valid)  AND  (not suspended)

i.e. ``suspended`` -> NOT licensed regardless of the (still-valid) license file,
and re-``active`` -> back to whatever the local license check says. Because this
handler may NOT edit ``agent.py`` (foundation-owned), it composes by wrapping the
agent's ``license_checker`` with a :class:`_SuspendableLicenseChecker` that
delegates every call to the original checker but forces ``ok=False`` while
suspended. ``Agent.is_licensed()`` / ``Agent.collect()`` consume the checker only
through ``.is_licensed()`` / ``.evaluate()``, so the override is transparent and
fully reversible (un-suspending restores the original behavior).

I3 (resilience): every step is defensive — a missing agent handle, an already
-wrapped checker, or any error degrades to "leave the agent as it is" and returns
the applied facet anyway; this handler never crashes the reconcile loop.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Dict

from lib.desired_state import DesiredState

from .registry import ReconcileContext, register

LOG = logging.getLogger("fyralis.agent.reconcile.license")

# The license_state value the agent treats as a revoked entitlement.
_SUSPENDED = "suspended"


class _SuspendableLicenseChecker:
    """Wraps the agent's real ``LicenseChecker`` and composes operator suspension.

    Delegates ``is_licensed`` / ``evaluate`` / ``license_expiry`` (and any other
    attribute) to the wrapped checker, but while ``suspended`` is True it forces
    the result to "not licensed" — the operator's revocation wins over a still
    -valid local license file. ``suspended`` is a mutable flag so a later
    re-``active`` reconcile flips the SAME wrapper back without re-wrapping.

    ``_fyralis_suspendable`` marks the instance so a re-run of the handler
    recognizes an already-installed wrapper and just toggles the flag (idempotent;
    never wraps a wrapper).
    """

    _fyralis_suspendable = True

    def __init__(self, inner: Any, *, suspended: bool = False) -> None:
        self._inner = inner
        self.suspended = bool(suspended)

    # -- the two methods the agent actually calls --------------------------

    def is_licensed(self, **kwargs) -> bool:
        if self.suspended:
            return False
        return bool(self._inner.is_licensed(**kwargs))

    def evaluate(self, **kwargs):
        status = self._inner.evaluate(**kwargs)
        if not self.suspended:
            return status
        # Force "not licensed" while preserving the rest of the status (esp.
        # expires_at, which the agent stamps onto the heartbeat) so the record
        # reflects reality minus the revoked entitlement. LicenseStatus is a
        # frozen dataclass -> rebuild via dataclasses.replace.
        try:
            return dataclasses.replace(
                status,
                ok=False,
                reason="entitlement SUSPENDED by operator (desired license_state=suspended)",
            )
        except Exception:  # pragma: no cover - status not a dataclass we know
            return status

    # -- transparent delegation for everything else (license_expiry, attrs) -

    def __getattr__(self, name: str) -> Any:
        # Only reached for attributes not found on this wrapper (so _inner /
        # suspended resolve normally and never recurse).
        return getattr(self._inner, name)


def _set_suspended(agent: Any, suspended: bool, ctx: ReconcileContext) -> None:
    """Install (or toggle) the suspendable wrapper on the agent's license_checker.

    Idempotent + reversible: wraps the real checker exactly once, then only flips
    the ``suspended`` flag on subsequent reconciles. Best-effort (I3) — any error
    is logged and swallowed so reconcile keeps running.
    """
    checker = getattr(agent, "license_checker", None)
    if checker is None:
        ctx.logger.debug("license reconcile: agent has no license_checker (skipping wrap)")
        return

    if getattr(checker, "_fyralis_suspendable", False):
        # Already our wrapper — just toggle the flag.
        if checker.suspended != suspended:
            ctx.logger.info(
                "license reconcile: %s deployment %s",
                "SUSPENDING" if suspended else "RE-ACTIVATING",
                ctx.deployment_id,
            )
        checker.suspended = suspended
        return

    # First time we need to override: wrap the real checker in place.
    agent.license_checker = _SuspendableLicenseChecker(checker, suspended=suspended)
    if suspended:
        ctx.logger.info(
            "license reconcile: SUSPENDING deployment %s (operator entitlement revocation)",
            ctx.deployment_id,
        )


def apply(desired: DesiredState, ctx: ReconcileContext) -> Dict[str, Any]:
    """Reflect the operator's desired ``license_state`` on this agent.

    * ``suspended`` -> compose a revocation so ``Agent.is_licensed()`` is False
      regardless of the local signed license (degrade cleanly, never crash — I3).
    * ``active`` (or anything else) -> restore normal local-license behavior.

    Returns ``{"license_state_applied": <state>}`` so the console can compute
    license drift on the next heartbeat.
    """
    state = (desired.license_state or "active").strip().lower()
    suspended = state == _SUSPENDED

    # The live Agent handle is passed in ctx.extra by the foundation's
    # _reconcile_ctx(). If absent (e.g. a unit test with a bare ctx), we still
    # report the applied facet — drift accounting must not depend on the wrap.
    agent = (ctx.extra or {}).get("agent")
    if agent is not None:
        try:
            _set_suspended(agent, suspended, ctx)
        except Exception as exc:  # I3: never let the wrap crash reconcile
            ctx.logger.warning(
                "license reconcile: failed to apply suspension state (%s); reporting anyway: %s",
                state,
                exc,
            )

    # Report the state we applied (normalize to the model's domain).
    applied_state = _SUSPENDED if suspended else "active"
    return {"license_state_applied": applied_state}


# Self-register at import time so reconcile.autodiscover() wires this handler
# (overrides nothing; adds the "license" concern alongside the example).
register("license", apply)
