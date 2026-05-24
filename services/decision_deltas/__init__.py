"""
services/decision_deltas — first-class Decision Delta primitive.

A Decision Delta represents the "Proposed Change" UI surface. Where a recommendation
treats the proposed change as a field on a kind='recommendation' Model
row, a Decision Delta elevates the state change itself (before ->
after, falsification condition, consequence preview, evidence chain)
to a first-class object that the CEO reviews directly.

Module layout
-------------
  repo.py     — CRUD against `decision_deltas` + `decision_delta_evidence`.
  promote.py  — bridge: build a delta from an existing recommendation row.
  apply.py    — accept-and-apply: run consequence_preview side effects,
                emit ledger event.
  router.py   — FastAPI APIRouter at /v1/decision_deltas.

The router is NOT wired into services/gateway/main.py here. The
registration line is documented in the agent report; main.py is in
the forbidden zone for this agent.
"""

from services.decision_deltas import apply, promote, repo, router  # noqa: F401

__all__ = ["apply", "promote", "repo", "router"]
