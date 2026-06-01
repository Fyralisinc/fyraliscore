"""services.sage.intent_inferer — Phase 3: Retrieval Intent Inferer.

Spec: fyralis-sage-synthesis-self-evolution.md §7.3 (Stage B) and Phase 3
(§1499-1548). Pure-Python, rule-based v1. No DB, no LLM.

Turns ``StructuredCues`` (produced by ``services.sage.cue_extractor``)
into a list of concrete ``RetrievalIntent`` objects. Each intent is
question-conditioned: it carries the retrieval paths to traverse, a
budget (max_nodes / max_evidence), constraints (time window, scope),
and a success condition string that downstream synthesis can verify.

Mapping rules (v1, doc §1530-1538):

    DEPENDENCY     -> structural + temporal + semantic
    OWNERSHIP      -> exact + structural + actor_team_graph
    CONTRADICTION  -> counterevidence + recent_observations + structural
    PATTERN        -> semantic + pattern + model_edge
    ACTION         -> model_edge + actor_team_graph + structural
    CONSTRAINT     -> structural + model_edge
    FALSIFICATION  -> exact + recent_observations + semantic

The cue surface used to pick a kind is:
  * ``cues.relationship_clues`` — verb-like predicates
    ("depends_on", "blocks", "contradicts", "owns", "across_customers",
    "recurring", "falsifies", "is_assigned_to", ...).
  * ``cues.expected_synthesis_decision_type`` — coarse
    decision-class hints emitted by the upstream cue extractor
    (e.g. "update_commitment_risk", "create_emerging_bottleneck_model",
    "select_next_action", "test_falsifier").
  * Free-text fallback: a small set of keyword probes over
    ``question_text`` so this module remains useful even before the
    cue extractor populates its richer fields.

Mapping is intentionally additive: a single multi-faceted question
(e.g. "Who owns the SSO blocker and what's the next action?") will
produce multiple intents.

Empty-cue contract: if nothing matches we always emit a single
fallback ``test_dependency`` intent so downstream stages still have
something concrete to run. This satisfies the acceptance criterion
"every selected question has at least one retrieval intent"
(§1541).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    # Import contract: the cue extractor module lives at
    # services/sage/cue_extractor.py. It may or may not exist yet —
    # this module only needs duck-typed access to the documented
    # StructuredCues fields, so we keep the runtime import lazy.
    from services.sage.cue_extractor import StructuredCues  # noqa: F401


# ---------------------------------------------------------------------
# Public type surface
# ---------------------------------------------------------------------

IntentPrimitive = Literal[
    "find_active_commitment",
    "test_dependency",
    "find_counterevidence",
    "find_owner",
    "find_pattern_recurrence",
    "find_blocking_resource",
    "test_falsification",
    "find_authoritative_source",
    "find_action_candidates",
    "find_goal_impact",
]


RetrievalPath = Literal[
    "exact",
    "structural",
    "semantic",
    "temporal",
    "pattern",
    "counterevidence",
    "recent_observations",
    "model_edge",
    "alias_lookup",
    "actor_team_graph",
]


@dataclass(frozen=True, slots=True)
class RetrievalIntent:
    """A single question-conditioned retrieval intent.

    Fields:
      * ``intent`` — the primitive kind (drives expected_value / cost
        priors and downstream synthesis verification).
      * ``question_id`` — the upstream question identifier the intent
        was derived from. One question may produce many intents.
      * ``target`` — short human/log-readable string describing what
        we are looking for ("SSO critical path for Acme launch").
      * ``paths`` — ordered tuple of retrieval pathways to try.
      * ``budget`` — ``{"max_nodes": int, "max_evidence": int}``.
      * ``constraints`` — opaque dict; conventional keys:
        ``time_window_days``, ``scope_entities``, ``status``.
      * ``success_condition`` — natural-language predicate the
        synthesis stage can check (e.g. "≥2 counterevidence found").
      * ``expected_value`` / ``expected_cost`` — 0..1 priors used by
        the budget allocator (Phase 6) to rank intents.
    """

    intent: IntentPrimitive
    question_id: str
    target: str
    paths: tuple[RetrievalPath, ...]
    budget: dict[str, int]
    constraints: dict[str, Any]
    success_condition: str
    expected_value: float
    expected_cost: float


# ---------------------------------------------------------------------
# Default budget + per-primitive value/cost priors
# ---------------------------------------------------------------------

_DEFAULT_BUDGET: dict[str, int] = {"max_nodes": 50, "max_evidence": 20}

# Tuned-by-eye v1 priors. Higher value => synthesis benefits more
# from this intent succeeding; higher cost => more pathways /
# heavier traversal. Phase 6 will replace these with learned
# values from inquiry traces.
_PRIORS: dict[str, tuple[float, float]] = {
    "find_active_commitment":   (0.75, 0.25),
    "test_dependency":          (0.80, 0.55),
    "find_counterevidence":     (0.90, 0.45),
    "find_owner":               (0.65, 0.20),
    "find_pattern_recurrence":  (0.70, 0.60),
    "find_blocking_resource":   (0.78, 0.40),
    "test_falsification":       (0.88, 0.50),
    "find_authoritative_source":(0.55, 0.20),
    "find_action_candidates":   (0.82, 0.55),
    "find_goal_impact":         (0.72, 0.45),
}


# ---------------------------------------------------------------------
# Cue keyword vocab (v1 — intentionally small + readable)
# ---------------------------------------------------------------------

_DEPENDENCY_CLUES = frozenset({
    "depends_on", "depends-on", "depends",
    "blocks", "blocked_by", "blocked-by", "blocked",
    "critical_path", "critical-path",
    "requires", "prerequisite",
})
_OWNERSHIP_CLUES = frozenset({
    "owns", "owned_by", "owned-by",
    "assigned_to", "assigned-to", "is_assigned_to",
    "responsible_for", "responsible-for", "accountable_for",
    "who_owns", "who-owns",
})
_CONTRADICTION_CLUES = frozenset({
    "contradicts", "contradicted_by",
    "but", "however", "conflicts_with",
    "counter", "counterevidence",
    "disagrees_with", "inconsistent_with",
})
_PATTERN_CLUES = frozenset({
    "across_customers", "across-customers",
    "recurring", "repeated", "repeats", "pattern",
    "every_time", "every-time",
    "always_when", "always-when",
})
_ACTION_CLUES = frozenset({
    "next_action", "next-action",
    "should_do", "should-do",
    "next_step", "next-step",
    "what_to_do", "what-to-do",
    "decide", "select_action",
})
_CONSTRAINT_CLUES = frozenset({
    "bottleneck", "constraint", "capacity_limit",
    "limited_by", "constrained_by",
})
_FALSIFICATION_CLUES = frozenset({
    "falsifies", "falsified_by", "test_falsifier",
    "disproves", "would_disprove",
})

_DECISION_TYPE_DEPENDENCY = frozenset({
    "update_commitment_risk", "evaluate_dependency",
    "update_critical_path",
})
_DECISION_TYPE_OWNERSHIP = frozenset({
    "assign_owner", "identify_owner",
})
_DECISION_TYPE_CONTRADICTION = frozenset({
    "resolve_contradiction", "challenge_assumption",
})
_DECISION_TYPE_PATTERN = frozenset({
    "create_emerging_bottleneck_model", "create_pattern_model",
    "promote_pattern",
})
_DECISION_TYPE_ACTION = frozenset({
    "select_next_action", "propose_action", "recommend_action",
})
_DECISION_TYPE_CONSTRAINT = frozenset({
    "identify_bottleneck", "find_blocking_resource",
})
_DECISION_TYPE_FALSIFICATION = frozenset({
    "test_falsifier", "run_falsification",
})


# ---------------------------------------------------------------------
# The inferer
# ---------------------------------------------------------------------


class RetrievalIntentInferer:
    """Rule-based Retrieval Intent Inferer (Phase 3 v1).

    Stateless. Safe to share across requests / event loops.
    """

    def __init__(
        self,
        *,
        default_budget: dict[str, int] | None = None,
    ) -> None:
        self._default_budget: dict[str, int] = dict(
            default_budget if default_budget is not None else _DEFAULT_BUDGET
        )

    # ------------------------------------------------------------- public

    def infer(
        self,
        *,
        cues: Any,  # duck-typed StructuredCues
        evidence_state: dict | None,
        question_id: str,
        question_text: str,
    ) -> list[RetrievalIntent]:
        """Produce one or more ``RetrievalIntent`` for a question.

        Empty cues + no keyword hits => single fallback
        ``test_dependency`` intent (acceptance criterion §1541).
        """
        clues = _as_lower_set(_safe_attr(cues, "relationship_clues", ()))
        decision_types = _as_lower_set(
            _safe_attr(cues, "expected_synthesis_decision_type", ())
        )
        qtext = (question_text or "").lower()

        target = _build_target(cues, question_text)
        constraints = _build_constraints(cues, evidence_state)

        intents: list[RetrievalIntent] = []

        # --- DEPENDENCY -------------------------------------------------
        if (
            clues & _DEPENDENCY_CLUES
            or decision_types & _DECISION_TYPE_DEPENDENCY
            or _any_phrase(qtext, ("depend", "block", "critical path", "requires"))
        ):
            intents.append(
                self._make(
                    intent="test_dependency",
                    question_id=question_id,
                    target=target,
                    paths=("structural", "temporal", "semantic"),
                    constraints=constraints,
                    success_condition=(
                        f"evidence for or against dependency on {target!r} found"
                    ),
                )
            )

        # --- OWNERSHIP --------------------------------------------------
        if (
            clues & _OWNERSHIP_CLUES
            or decision_types & _DECISION_TYPE_OWNERSHIP
            or _any_phrase(qtext, ("who owns", "who is responsible", "assigned to"))
        ):
            intents.append(
                self._make(
                    intent="find_owner",
                    question_id=question_id,
                    target=target,
                    paths=("exact", "structural", "actor_team_graph"),
                    constraints=constraints,
                    success_condition=f"owner identified for {target!r}",
                )
            )

        # --- CONTRADICTION ----------------------------------------------
        if (
            clues & _CONTRADICTION_CLUES
            or decision_types & _DECISION_TYPE_CONTRADICTION
            or _any_phrase(qtext, ("contradict", "counter", " but ", "however"))
        ):
            intents.append(
                self._make(
                    intent="find_counterevidence",
                    question_id=question_id,
                    target=target,
                    paths=(
                        "counterevidence",
                        "recent_observations",
                        "structural",
                    ),
                    constraints=constraints,
                    success_condition=(
                        f"≥2 counterevidence items found for {target!r}"
                    ),
                )
            )

        # --- PATTERN ----------------------------------------------------
        if (
            clues & _PATTERN_CLUES
            or decision_types & _DECISION_TYPE_PATTERN
            or _any_phrase(
                qtext, ("across customers", "recurring", "pattern", "every time")
            )
        ):
            intents.append(
                self._make(
                    intent="find_pattern_recurrence",
                    question_id=question_id,
                    target=target,
                    paths=("semantic", "pattern", "model_edge"),
                    constraints=constraints,
                    success_condition=(
                        f"≥2 prior instances of pattern around {target!r} found"
                    ),
                )
            )

        # --- ACTION -----------------------------------------------------
        if (
            clues & _ACTION_CLUES
            or decision_types & _DECISION_TYPE_ACTION
            or _any_phrase(
                qtext,
                ("what should", "next step", "next action", "what do we do"),
            )
        ):
            intents.append(
                self._make(
                    intent="find_action_candidates",
                    question_id=question_id,
                    target=target,
                    paths=("model_edge", "actor_team_graph", "structural"),
                    constraints=constraints,
                    success_condition=(
                        f"≥1 viable action candidate found for {target!r}"
                    ),
                )
            )

        # --- CONSTRAINT / BOTTLENECK ------------------------------------
        if (
            clues & _CONSTRAINT_CLUES
            or decision_types & _DECISION_TYPE_CONSTRAINT
            or _any_phrase(qtext, ("bottleneck", "constraint", "limited by"))
        ):
            intents.append(
                self._make(
                    intent="find_blocking_resource",
                    question_id=question_id,
                    target=target,
                    paths=("structural", "model_edge"),
                    constraints=constraints,
                    success_condition=(
                        f"blocking resource / bottleneck identified for {target!r}"
                    ),
                )
            )

        # --- FALSIFICATION ----------------------------------------------
        if (
            clues & _FALSIFICATION_CLUES
            or decision_types & _DECISION_TYPE_FALSIFICATION
            or _any_phrase(qtext, ("falsif", "disprove"))
        ):
            intents.append(
                self._make(
                    intent="test_falsification",
                    question_id=question_id,
                    target=target,
                    paths=("exact", "recent_observations", "semantic"),
                    constraints=constraints,
                    success_condition=(
                        f"falsifier evaluated for Model(s) around {target!r}"
                    ),
                )
            )

        # --- Fallback ---------------------------------------------------
        if not intents:
            intents.append(
                self._make(
                    intent="test_dependency",
                    question_id=question_id,
                    target=target,
                    paths=("structural", "temporal", "semantic"),
                    constraints=constraints,
                    success_condition=(
                        f"evidence for or against dependency on {target!r} found"
                    ),
                )
            )

        return intents

    # ------------------------------------------------------------ internal

    def _make(
        self,
        *,
        intent: IntentPrimitive,
        question_id: str,
        target: str,
        paths: tuple[RetrievalPath, ...],
        constraints: dict[str, Any],
        success_condition: str,
    ) -> RetrievalIntent:
        value, cost = _PRIORS.get(intent, (0.6, 0.4))
        return RetrievalIntent(
            intent=intent,
            question_id=question_id,
            target=target,
            paths=paths,
            budget=dict(self._default_budget),
            constraints=dict(constraints),
            success_condition=success_condition,
            expected_value=value,
            expected_cost=cost,
        )


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def _safe_attr(obj: Any, name: str, default: Any) -> Any:
    """Duck-typed attribute fetch — works with dataclasses, Pydantic
    models, plain dicts (via __getitem__ fallback), and SimpleNamespace.
    """
    if obj is None:
        return default
    val = getattr(obj, name, None)
    if val is None and isinstance(obj, dict):
        val = obj.get(name)
    return val if val is not None else default


def _as_lower_set(values: Any) -> frozenset[str]:
    if not values:
        return frozenset()
    if isinstance(values, str):
        values = [values]
    out: set[str] = set()
    for v in values:
        if v is None:
            continue
        out.add(str(v).strip().lower().replace(" ", "_"))
    return frozenset(out)


def _any_phrase(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(n in haystack for n in needles)


def _build_target(cues: Any, question_text: str) -> str:
    """Compose a short human-readable target string.

    Preference order:
      1. explicit_entities (joined),
      2. system_mentions + goal_mentions,
      3. customer_mentions,
      4. trimmed question_text,
      5. literal "(unspecified)".
    """
    entities = list(_safe_attr(cues, "explicit_entities", []) or [])
    systems = list(_safe_attr(cues, "system_mentions", []) or [])
    goals = list(_safe_attr(cues, "goal_mentions", []) or [])
    customers = list(_safe_attr(cues, "customer_mentions", []) or [])

    if entities:
        return ", ".join(str(e) for e in entities[:4])
    composed = [*systems, *goals]
    if composed:
        return ", ".join(str(e) for e in composed[:4])
    if customers:
        return ", ".join(str(e) for e in customers[:4])
    qt = (question_text or "").strip()
    if qt:
        return qt[:80]
    return "(unspecified)"


def _build_constraints(
    cues: Any, evidence_state: dict | None
) -> dict[str, Any]:
    """Project cue time/status/scope constraints into an opaque dict
    the pathway runner can consume. Unknown fields are dropped.
    """
    out: dict[str, Any] = {}
    tc = _safe_attr(cues, "time_constraints", None)
    if isinstance(tc, dict) and tc:
        out["time_window"] = dict(tc)
    sc = _safe_attr(cues, "status_constraints", None)
    if sc:
        out["status"] = list(sc) if not isinstance(sc, str) else [sc]
    source_c = _safe_attr(cues, "source_constraints", None)
    if source_c:
        out["sources"] = (
            list(source_c) if not isinstance(source_c, str) else [source_c]
        )
    scope_entities: list[str] = []
    for fld in (
        "explicit_entities",
        "customer_mentions",
        "system_mentions",
        "goal_mentions",
    ):
        for v in _safe_attr(cues, fld, []) or []:
            if v is not None:
                scope_entities.append(str(v))
    if scope_entities:
        # dedupe preserving order
        seen: set[str] = set()
        out["scope_entities"] = [
            e for e in scope_entities if not (e in seen or seen.add(e))
        ]
    if evidence_state:
        # Pass through a narrow handle so downstream stages can avoid
        # re-retrieving already-known items.
        known = evidence_state.get("known_model_ids")
        if known:
            out["exclude_known_model_ids"] = list(known)
    return out


__all__ = [
    "IntentPrimitive",
    "RetrievalPath",
    "RetrievalIntent",
    "RetrievalIntentInferer",
]
