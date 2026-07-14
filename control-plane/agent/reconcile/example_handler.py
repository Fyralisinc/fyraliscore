"""example_handler — the REFERENCE reconcile handler (the contract a feature copies).

A reconcile handler:

  1. lives in ``agent/reconcile/<concern>.py``,
  2. exposes ``def apply(desired: DesiredState, ctx: ReconcileContext) -> dict`` returning
     the APPLIED facets it changed (the ``applied_delta``),
  3. self-registers at import time with ``register("<concern>", apply)`` so
     :func:`reconcile.autodiscover` picks it up with zero wiring,
  4. for config/release: VERIFIES the signature against ``ctx.trust_root_path`` BEFORE
     applying (I6) — NEVER applies an unsigned/relabeled/wrong-key payload.

This example is intentionally INERT: it acknowledges nothing, applies nothing, and
returns an empty delta. It exists so the foundation's dispatch path is exercised by a
real registered handler and so a feature has a literal template to copy. A real handler
(e.g. ``config.py``) would read ``desired.desired_config_version`` /
``desired.desired_config_sig``, verify + apply, and return
``{"applied_config_version": <n>}``.
"""

from __future__ import annotations

from typing import Any, Dict

from lib.desired_state import DesiredState

from .registry import ReconcileContext, register


def apply(desired: DesiredState, ctx: ReconcileContext) -> Dict[str, Any]:
    """Reference no-op reconcile: observe desired state, change nothing.

    Returns an EMPTY applied_delta so it contributes nothing to the heartbeat's
    applied facets. A real handler returns the facet(s) it actually applied.
    """
    ctx.logger.debug(
        "example reconcile handler: desired_config_version=%s desired_release=%s "
        "(no-op example, applying nothing)",
        desired.desired_config_version,
        desired.desired_release,
    )
    return {}


# Self-register at import time so reconcile.autodiscover() wires this handler.
register("example", apply)
