"""example_router — the REFERENCE feature router (the contract a feature copies).

This file documents — by being real, mounted, working code — exactly how a feature
agent plugs into the console:

  1. Put a module under ``console/routers/<feature>.py``.
  2. Expose a module-level ``def register(app, deps): ...``.
  3. Wire endpoints onto ``app``; reach the store/signer/audit/settings off
     ``deps``; guard operator WRITES with ``deps.require_operator`` and
     agent-facing reads with ``deps.require_agent_write``.
  4. AUDIT every operator write via ``deps.audit.append({actor, action, target,
     metadata})`` (I5), and SIGN any desired config/release via
     ``deps.signer(payload, kind="config", version=...)`` (I6).

``app.py`` calls ``register(app, deps)`` once at startup. If this module fails to
import, or ``register`` raises, ``app.py`` logs it and SKIPS it — a broken feature
can never take the console down.

This example mounts a single, side-effect-free read endpoint under ``/api/v1/_example``
so the foundation's router-mount loop is provably exercised by the console tests.
Real features replace this file (or add siblings); it is intentionally trivial.
"""

from __future__ import annotations

from fastapi import Depends


def register(app, deps) -> None:
    """Mount the example feature's endpoints onto ``app`` using ``deps``."""

    @app.get(
        "/api/v1/_example/ping",
        tags=["example"],
        summary="Reference router liveness — proves the mount loop ran.",
    )
    def _example_ping() -> dict:
        # Read-only; demonstrates reaching the shared store off deps.
        return {"router": "example", "ok": True, "fleet_size": len(deps.store)}

    # An operator-WRITE shape (guarded by require_operator + audited) so the
    # reference shows the full pattern. It performs no real mutation.
    @app.post(
        "/api/v1/_example/noop",
        tags=["example"],
        summary="Reference operator-write shape (auth + audit), does nothing.",
        dependencies=[Depends(deps.require_operator)],
    )
    def _example_noop() -> dict:
        deps.audit.append(
            {
                "actor": "operator",
                "action": "example.noop",
                "target": "_example",
                "metadata": {},
            }
        )
        return {"ok": True}
