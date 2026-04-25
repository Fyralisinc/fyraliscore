"""
services/contestability — Wave 4-C contestation service.

Spec: ARCHITECTURE-FINAL.md §11.

Three contestation types:

1. Direct Model contestation — actor disputes the claim in a Model.
   Produces a `contestation` Observation with
   `trust_tier='authoritative'` (first-person override; spec §11
   "Direct contestation"), enqueues a T3 trigger.
2. Reading contestation — actor disputes a `signal_readings` entry
   (sub-Model sub-claim). Marks the signal_readings entry as
   contested; records a `model_status_notes` row with kind
   `first_person_override`.
3. Implicit contestation via signal — NOT handled here. The
   Anomaly processor picks up silent disagreement (scope actor's
   Observations contradicting the Model) and routes back via T3.

Status enum decision (BUILD-LOG Deviations):
-------------------------------------------
Spec S2.1 enum is `active | archived | superseded | contested_false`.
"contested" as a live-but-contested state isn't in the enum. Wave 4-C
goes with **option (a) from BUILD-PLAN**: keep `status='active'` while
bumping `contested_count` and emitting a `contestation` Observation.
The `contested_false` enum is reserved for the Think T3 resolution
that proves the Model false, not the mid-flight "actively contested"
state. UI renders "actively contested but not yet resolved" from the
combination (contested_count > 0 AND no contested_false outcome yet).

Standing logic (services/contestability/standing.py):
----------------------------------------------------
`actor_has_standing_on_model(actor_id, model, conn)` returns True when
any of:
  * actor_id ∈ model.scope_actors
  * entity ownership (owner of a commitment or contributor in
    model.scope_entities)
  * manager-chain (stubbed; returns False in Wave 4 — wired up in
    Wave 5-A when the org-chart is available).

First-person override weights (BUILD-LOG Deviations):
----------------------------------------------------
Spec §11 values preserved exactly:
  * Primary subject (model.scope_actors[0]): confidence *= 0.3
    floor 0.15.
  * Secondary subject (any scope_actor beyond [0]): confidence *= 0.5
    floor 0.15.

Outputs
-------
- `contest_model(...)` returns `ContestationResult(observation_id,
   trigger_id, new_confidence, standing)`.
"""
from services.contestability.service import (
    ContestationError,
    ContestationInput,
    ContestationResult,
    NoStandingError,
    contest_model,
)
from services.contestability.standing import actor_has_standing_on_model

__all__ = [
    "actor_has_standing_on_model",
    "contest_model",
    "ContestationError",
    "ContestationInput",
    "ContestationResult",
    "NoStandingError",
]
