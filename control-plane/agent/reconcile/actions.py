"""actions — the A3 ACTION QUEUE reconcile handler (agent side).

The console queues bounded, allowlisted actions into ``desired.pending_actions``
(roadmap A3). On each tick the agent pulls the desired state and dispatches it to
this handler, which:

  1. iterates the pending actions,
  2. SKIPS any already-acked one (its id is in this deployment's last-applied
     ``acked_action_ids`` — handlers are idempotent; an action runs once),
  3. for each *new* pending action whose ``type`` is in the CLOSED
     :data:`lib.desired_state.ACTION_ALLOWLIST`, runs the matching executor and
     records the id as acked,
  4. SKIPS (does not ack) any unknown/relabeled type — a future allowlist entry
     this agent does not yet handle must remain pending, never silently swallowed,
  5. returns ``{"acked_action_ids: [...]}`` — the union of newly-executed ids plus
     the already-acked ones, so the merged applied facet (and the console's drift
     view) converges.

Why this is safe (I2 outbound-only, no remote exec): there is NO generic command
executor here. Each allowlisted type maps to a *named, agent-owned* routine. In
this demo the routines are deliberately minimal — ``trigger-backfill`` /
``force-reconcile`` / ``flush-dlq`` are logged no-op stubs (the real wiring is a
later sprint), and ``re-pull-config`` flips a flag in ``ctx.extra`` so the agent's
config puller re-pulls on the next pass. The ``params`` are advisory inputs to
that routine, never code.

I3 resilience: a single executor that raises is logged and skipped — that action
is simply NOT acked (it stays pending and is retried next tick); one bad action
can never crash the reconcile loop (the registry's ``dispatch`` also guards the
whole handler).
"""

from __future__ import annotations

from typing import Any, Dict, List

from lib.desired_state import ACTION_ALLOWLIST, DesiredState

from .registry import ReconcileContext, register


# --------------------------------------------------------------------------- #
# the named, agent-owned executors (one per allowlisted type)                  #
# --------------------------------------------------------------------------- #
#
# Each executor takes (action, ctx) and performs the demo behavior for its type.
# A demo stub LOGS what it would do; ``re-pull-config`` sets a flag the agent's
# config puller honors. None of them shells out or evals — the closed allowlist
# is enforced by _EXECUTORS' key set, so an unknown type has no executor at all.


def _exec_re_pull_config(action: Dict[str, Any], ctx: ReconcileContext) -> None:
    """``re-pull-config`` — ask the agent to re-pull its signed config bundle.

    Signals the live ConfigPuller (if injected at ``ctx.extra['config_puller']``)
    to re-pull, and always sets an advisory ``repull_config_requested`` flag in
    ``ctx.extra`` so the agent loop can act on it regardless. Verifying the pulled
    bundle's signature (I6) is the ConfigPuller's job, not this action's.
    """
    ctx.extra["repull_config_requested"] = True
    puller = ctx.extra.get("config_puller")
    if puller is not None and hasattr(puller, "request_repull"):
        try:
            puller.request_repull()
        except Exception as exc:  # advisory only; flag still set
            ctx.logger.warning("re-pull-config: puller.request_repull failed: %s", exc)
    ctx.logger.info(
        "reconcile/actions: re-pull-config requested for deployment %s",
        ctx.deployment_id,
    )


def _exec_force_reconcile(action: Dict[str, Any], ctx: ReconcileContext) -> None:
    """``force-reconcile`` — demo no-op stub (logged).

    The agent already reconciles every tick; a forced reconcile is a future hook.
    Recorded as acked so the operator's queued action visibly converges.
    """
    ctx.logger.info(
        "reconcile/actions: force-reconcile (demo no-op) for deployment %s params=%s",
        ctx.deployment_id,
        action.get("params", {}),
    )


def _exec_trigger_backfill(action: Dict[str, Any], ctx: ReconcileContext) -> None:
    """``trigger-backfill`` — demo no-op stub (logged)."""
    ctx.logger.info(
        "reconcile/actions: trigger-backfill (demo no-op) for deployment %s params=%s",
        ctx.deployment_id,
        action.get("params", {}),
    )


def _exec_flush_dlq(action: Dict[str, Any], ctx: ReconcileContext) -> None:
    """``flush-dlq`` — demo no-op stub (logged)."""
    ctx.logger.info(
        "reconcile/actions: flush-dlq (demo no-op) for deployment %s params=%s",
        ctx.deployment_id,
        action.get("params", {}),
    )


# The closed type -> executor map. Its key set is (and must stay) a subset of the
# shared ACTION_ALLOWLIST; an action type with no executor here is left pending.
_EXECUTORS = {
    "re-pull-config": _exec_re_pull_config,
    "force-reconcile": _exec_force_reconcile,
    "trigger-backfill": _exec_trigger_backfill,
    "flush-dlq": _exec_flush_dlq,
}


def apply(desired: DesiredState, ctx: ReconcileContext) -> Dict[str, Any]:
    """Execute every new (unacked, allowlisted) pending action; ack the ids."""
    # Already-acked ids from this deployment's last-applied facet (idempotency).
    already_acked = set(
        str(x) for x in (ctx.extra.get("acked_action_ids") or [])
    )

    newly_acked: List[str] = []
    for action in desired.pending_actions or []:
        aid = action.get("id")
        if aid is None:
            continue
        aid_s = str(aid)
        if aid_s in already_acked or aid_s in newly_acked:
            continue  # idempotent: never run an action twice

        atype = action.get("type")
        if atype not in ACTION_ALLOWLIST:
            # Defense in depth: the console rejects these, but a tampered/legacy
            # desired blob must not slip an off-allowlist type past the agent.
            ctx.logger.warning(
                "reconcile/actions: dropping off-allowlist action %s type=%r",
                aid_s,
                atype,
            )
            continue

        executor = _EXECUTORS.get(atype)
        if executor is None:
            # Allowlisted but this agent ships no executor for it yet — leave it
            # pending (do NOT ack) so a newer agent can handle it later.
            ctx.logger.info(
                "reconcile/actions: no executor for allowlisted type %r (left pending)",
                atype,
            )
            continue

        try:
            executor(action, ctx)
        except Exception as exc:  # I3: a bad action is skipped, stays pending
            ctx.logger.warning(
                "reconcile/actions: executor for action %s (%s) raised, not acked: %s",
                aid_s,
                atype,
                exc,
            )
            continue

        newly_acked.append(aid_s)

    if not newly_acked:
        # Nothing new applied — return the already-acked set so the merged facet
        # remains stable across ticks (and the very first tick contributes []).
        if already_acked:
            return {"acked_action_ids": sorted(already_acked)}
        return {}

    # Union of previously-acked + newly-executed (order-preserving via registry's
    # merge, which de-dups). We return both so a single-handler merge is complete.
    merged = list(already_acked) + [a for a in newly_acked if a not in already_acked]
    return {"acked_action_ids": merged}


# Self-register at import time so reconcile.autodiscover() wires this handler.
register("actions", apply)
