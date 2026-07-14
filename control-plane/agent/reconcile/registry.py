"""registry — the agent's reconcile handler registry, context, and dispatch.

A handler is a callable ``apply(desired: DesiredState, ctx: ReconcileContext) -> dict``
that inspects the operator's desired state, applies whatever it owns (config, release,
license gate, queued action…), and returns an ``applied_delta`` dict — the APPLIED
facets it changed. The agent merges every handler's delta into the next heartbeat so
the console can compute drift.

Handlers are registered by name (``register("config", handler)``) so a feature can
override the foundation's example, and they are dispatched in registration order.
``autodiscover`` imports every ``agent/reconcile/<concern>.py`` (skipping private
modules) and lets each call ``register`` at import time — the zero-config path.

The dispatch is RESILIENT (I3): a handler that raises is logged and skipped; one bad
handler can never crash the agent loop or block the heartbeat.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# DesiredState comes from the shared lib (the agent's _bootstrap puts the
# control-plane root on sys.path before this package is imported).
from lib.desired_state import DesiredState

LOG = logging.getLogger("fyralis.agent.reconcile")

# A handler: apply(desired, ctx) -> applied_delta(dict).
ReconcileHandler = Callable[["DesiredState", "ReconcileContext"], Dict[str, Any]]

# The known APPLIED facet keys a handler may contribute. ``acked_action_ids`` is
# MERGED (union, order-preserving); the rest are last-writer-wins overwrites.
APPLIED_FACET_KEYS = (
    "applied_config_version",
    "applied_release",
    "acked_action_ids",
    "license_state_applied",
)

# The ordered registry: [(name, handler)]. A re-register of the same name replaces
# the handler in place (so a feature can override the example).
_REGISTRY: List[Tuple[str, ReconcileHandler]] = []


@dataclass
class ReconcileContext:
    """Everything a handler needs to apply desired state safely.

    ``trust_root_path`` is the agent's ``signing/trust_root.json`` — config/release
    handlers MUST verify a signed payload against it before applying (I6).
    ``config_dir`` is where verified config bundles are written (the same dir the
    :class:`ConfigPuller` applies into). ``deployment_id`` / ``tenant_id`` /
    ``console_url`` / ``console_token`` give the handler this deployment's identity
    and (if it needs to pull a signed bundle) the outbound channel. ``logger`` is a
    child logger. ``extra`` is an open bag a handler/agent may stash live objects in
    (e.g. an injected ConfigPuller, a license checker) without widening this dataclass.
    """

    deployment_id: str
    trust_root_path: str
    config_dir: str
    tenant_id: str = ""
    console_url: str = ""
    console_token: Optional[str] = None
    logger: logging.Logger = field(default_factory=lambda: LOG)
    extra: Dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# registry management                                                         #
# --------------------------------------------------------------------------- #


def register(name: str, handler: ReconcileHandler) -> None:
    """Register (or replace, by ``name``) a reconcile handler.

    Re-registering an existing name overwrites it IN PLACE (preserving order) so a
    feature can override the foundation's example concern.
    """
    if not callable(handler):
        raise TypeError(f"reconcile handler {name!r} must be callable")
    for i, (n, _h) in enumerate(_REGISTRY):
        if n == name:
            _REGISTRY[i] = (name, handler)
            return
    _REGISTRY.append((name, handler))


def registered_handlers() -> List[Tuple[str, ReconcileHandler]]:
    """The handlers in dispatch order (a copy)."""
    return list(_REGISTRY)


def list_handlers() -> List[str]:
    """The registered handler names in dispatch order."""
    return [n for n, _ in _REGISTRY]


def clear_registry() -> None:
    """Drop all handlers (tests / re-discovery)."""
    _REGISTRY.clear()


# --------------------------------------------------------------------------- #
# autodiscovery                                                               #
# --------------------------------------------------------------------------- #


def autodiscover() -> List[str]:
    """Import every ``agent/reconcile/<concern>.py`` so they self-``register``.

    Skips private modules (``_*``) and this ``registry`` module. A module that
    fails to import is LOGGED and SKIPPED (a broken feature handler must not crash
    the agent — I3). Returns the names of the modules imported.
    """
    pkg_path = [str(Path(__file__).resolve().parent)]
    imported: List[str] = []
    for mod_info in pkgutil.iter_modules(pkg_path):
        name = mod_info.name
        if name.startswith("_") or name == "registry":
            continue
        full = f"reconcile.{name}"
        try:
            importlib.import_module(full)
            imported.append(name)
        except Exception as exc:
            LOG.error("reconcile: handler module %s failed to import (skipped): %s", full, exc)
    return imported


# --------------------------------------------------------------------------- #
# dispatch + applied-delta merge                                              #
# --------------------------------------------------------------------------- #


def merge_applied(base: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
    """Merge a handler's ``applied_delta`` into the accumulating applied facets.

    ``acked_action_ids`` is UNIONED (order-preserving, de-duplicated) so multiple
    handlers can each ack actions; the other facets are last-writer-wins. Returns a
    new merged dict (does not mutate ``base``).
    """
    out = dict(base)
    for key, val in (delta or {}).items():
        if key == "acked_action_ids":
            seen = list(out.get("acked_action_ids", []))
            seen_set = set(str(x) for x in seen)
            for aid in val or []:
                if str(aid) not in seen_set:
                    seen.append(aid)
                    seen_set.add(str(aid))
            out["acked_action_ids"] = seen
        else:
            out[key] = val
    return out


def dispatch(desired: "DesiredState", ctx: "ReconcileContext") -> Dict[str, Any]:
    """Run every registered handler over ``desired`` and merge their applied deltas.

    RESILIENT (I3): a handler that raises is logged and skipped — it cannot crash
    the loop or block the heartbeat. Returns the merged APPLIED facets dict (which
    the agent folds into the next heartbeat).
    """
    applied: Dict[str, Any] = {}
    for name, handler in list(_REGISTRY):
        try:
            delta = handler(desired, ctx) or {}
        except Exception as exc:
            ctx.logger.warning(
                "reconcile handler %r raised (skipped, applied unchanged): %s", name, exc
            )
            continue
        if not isinstance(delta, dict):
            ctx.logger.warning(
                "reconcile handler %r returned %s, expected dict — ignored",
                name,
                type(delta).__name__,
            )
            continue
        applied = merge_applied(applied, delta)
    return applied
