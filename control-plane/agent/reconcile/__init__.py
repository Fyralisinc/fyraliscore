"""agent.reconcile — the agent-side desired-state reconcile handler registry.

The outbound-only agent (I2) PULLS the operator's :class:`~lib.desired_state.DesiredState`
each tick (best-effort; a 404 / any error just skips reconcile — I3), then DISPATCHES
it to a registry of HANDLERS. Each handler lives in ``agent/reconcile/<concern>.py``,
exposes::

    def apply(desired: DesiredState, ctx: ReconcileContext) -> dict:  # applied_delta

and is registered (see :func:`register` / :func:`autodiscover`). The agent merges
every handler's returned ``applied_delta`` into the next heartbeat's APPLIED facets
(``applied_config_version``, ``applied_release``, ``acked_action_ids``,
``license_state_applied``) so the console can compute drift.

The FOUNDATION ships: the registry, the dispatch, the :class:`ReconcileContext`, and an
example handler. FEATURE agents add their own ``agent/reconcile/<concern>.py`` and call
``register(...)`` (or rely on autodiscover). Config/release handlers MUST verify the
signature (I6) before applying — ``ctx.trust_root_path`` is provided for exactly that.
"""

from __future__ import annotations

from .registry import (
    ReconcileContext,
    ReconcileHandler,
    autodiscover,
    clear_registry,
    dispatch,
    list_handlers,
    merge_applied,
    register,
    registered_handlers,
)

__all__ = [
    "ReconcileContext",
    "ReconcileHandler",
    "register",
    "registered_handlers",
    "list_handlers",
    "clear_registry",
    "dispatch",
    "merge_applied",
    "autodiscover",
]
