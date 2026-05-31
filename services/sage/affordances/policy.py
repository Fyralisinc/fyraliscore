"""services/sage/affordances/policy.py — Heuristic default profiles.

Pure helpers (no DB I/O) for deriving a v1 `RetrievalAffordanceProfile`
from a Model's structural shape. Used by the Phase 9 backfill +
on-insert population path before any reinforcement / decay has run.

The heuristic maps three signals to question primitives:

  1. `proposition.kind` (observation / belief / prediction / norm) —
     the epistemic stance.
  2. Memory grammar columns (`claim_role`, `modality`, `polarity`) —
     the structural role of the claim.
  3. Outgoing edges (`supporting_model_ids`, `contributing_models`) —
     evidence of participation in compositions / predictions.

Output primitives drawn from §9.2 of the spec:

    DEPENDENCY | CONSTRAINT | CAUSE | ACTION | OWNERSHIP |
    COUNTEREVIDENCE | PATTERN | GOAL_IMPACT | RECURRENCE

The mapping is deliberately *coarse* and *additive*. Later phases
(reinforcement, inquiry feedback) tune the profile; this module only
seeds reasonable defaults so retrieval planners have something to score
against from day one.

`model_row` may be either a Pydantic `ModelRow` or a dict / asyncpg
record — we read attributes with `_get()` to support all three shapes
so callers don't have to hydrate first.
"""
from __future__ import annotations

from typing import Any, Iterable
from uuid import UUID

from services.sage.affordances.types import RetrievalAffordanceProfile


# Stance -> baseline primitives. These are the primitives that almost
# always make sense for a stance; subject semantics (claim_role,
# domain_tags) add to this set.
_STANCE_BASE_PRIMITIVES: dict[str, tuple[str, ...]] = {
    "observation": ("CAUSE", "COUNTEREVIDENCE"),
    "belief": ("DEPENDENCY", "CONSTRAINT"),
    "prediction": ("GOAL_IMPACT", "RECURRENCE"),
    "norm": ("ACTION", "OWNERSHIP"),
}

# claim_role -> primitives. These reflect what the Model's subject
# semantics make it useful for answering.
_CLAIM_ROLE_PRIMITIVES: dict[str, tuple[str, ...]] = {
    "fact": ("CAUSE",),
    "concern": ("CONSTRAINT", "COUNTEREVIDENCE"),
    "hypothesis": ("CAUSE", "DEPENDENCY"),
    "prediction": ("GOAL_IMPACT",),
    "pattern": ("PATTERN", "RECURRENCE"),
    "situation": ("CONSTRAINT", "DEPENDENCY"),
    "capability": ("DEPENDENCY", "OWNERSHIP"),
    "relation": ("DEPENDENCY", "OWNERSHIP"),
    "recommendation": ("ACTION", "OWNERSHIP"),
}


def _get(model_row: Any, name: str, default: Any = None) -> Any:
    """Read attribute `name` from a Pydantic row, dict, or asyncpg Record."""
    if hasattr(model_row, name):
        return getattr(model_row, name)
    try:
        return model_row[name]
    except (KeyError, TypeError, IndexError):
        return default


def _proposition_kind(model_row: Any) -> str | None:
    """Best-effort fetch of the four-stance kind.

    Falls back to `proposition['kind']` when the generated column isn't
    hydrated (e.g. fresh `ModelCreate` shapes).
    """
    explicit = _get(model_row, "proposition_kind")
    if explicit:
        return str(explicit)
    proposition = _get(model_row, "proposition") or {}
    if isinstance(proposition, dict):
        kind = proposition.get("kind")
        if kind:
            return str(kind)
    return None


def _edges_count(model_row: Any, attr: str) -> int:
    """Length of a list-valued column or 0 if missing."""
    value = _get(model_row, attr) or []
    try:
        return len(value)
    except TypeError:
        return 0


def derive_default_profile_from_model(
    model_row: Any,
) -> RetrievalAffordanceProfile:
    """Heuristically derive a v1 affordance profile for a Model.

    Always returns a profile — even Models with sparse memory grammar
    get the stance baseline. `utility_score` is left at 0 so unreinforced
    profiles never crowd reinforced ones in `search_by_primitive`.
    """
    model_id = _get(model_row, "id")
    tenant_id = _get(model_row, "tenant_id")
    if model_id is None or tenant_id is None:
        raise ValueError("derive_default_profile_from_model requires id + tenant_id")

    if not isinstance(model_id, UUID):
        model_id = UUID(str(model_id))
    if not isinstance(tenant_id, UUID):
        tenant_id = UUID(str(tenant_id))

    primitives: set[str] = set()

    # 1. Stance baseline.
    stance = _proposition_kind(model_row)
    if stance and stance in _STANCE_BASE_PRIMITIVES:
        primitives.update(_STANCE_BASE_PRIMITIVES[stance])

    # 2. claim_role overlay (subject semantics).
    claim_role = _get(model_row, "claim_role")
    if claim_role and claim_role in _CLAIM_ROLE_PRIMITIVES:
        primitives.update(_CLAIM_ROLE_PRIMITIVES[claim_role])

    # 3. Modality nudges. `observed` claims are first-class CAUSE
    #    candidates; `normative` claims always afford ACTION.
    modality = _get(model_row, "modality")
    if modality == "observed":
        primitives.add("CAUSE")
    elif modality == "normative":
        primitives.add("ACTION")
        primitives.add("OWNERSHIP")
    elif modality == "expected":
        primitives.add("GOAL_IMPACT")

    # 4. Polarity. Negative-polarity claims are useful as COUNTEREVIDENCE
    #    when retrieved against support hypotheses.
    polarity = _get(model_row, "polarity")
    if polarity == "negative":
        primitives.add("COUNTEREVIDENCE")

    # 5. Edge-derived signals. A Model that *supports* others is also a
    #    likely DEPENDENCY answer; one that contributes to a prediction
    #    has GOAL_IMPACT affordance.
    if _edges_count(model_row, "supporting_model_ids") > 0:
        primitives.add("DEPENDENCY")
    if _edges_count(model_row, "contributing_models") > 0:
        primitives.add("GOAL_IMPACT")

    # 6. Abstraction-level overlays. Patterns and composites pull in
    #    PATTERN / RECURRENCE primitives.
    abstraction = _get(model_row, "abstraction_level")
    if abstraction == "pattern":
        primitives.add("PATTERN")
        primitives.add("RECURRENCE")
    elif abstraction == "composite":
        primitives.add("CONSTRAINT")

    # 7. Scope-actor presence implies OWNERSHIP answers.
    if _edges_count(model_row, "scope_actors") > 0:
        primitives.add("OWNERSHIP")

    return RetrievalAffordanceProfile(
        model_id=model_id,
        tenant_id=tenant_id,
        answers_question_primitives=sorted(primitives),
        supports_hypothesis_types=[],
        weakens_hypothesis_types=[],
        common_composition_types=[],
        action_affordances=[],
        activation_signatures={},
        projection_policy={},
        utility_score=0.0,
    )


__all__ = ["derive_default_profile_from_model"]
