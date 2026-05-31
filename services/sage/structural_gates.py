"""services.sage.structural_gates — Phase 6: Heuristic Structural Gate Scorer.

Spec: ``fyralis-sage-synthesis-self-evolution.md`` §7.5 (Stage D:
Structurally Gated Propagation, lines ~539-594) and Phase 6
(lines ~1646-1683).

Pure-Python, deterministic, sync. No DB, no LLM. v1 is a hand-tuned
multiplicative heuristic; v2 (out of scope here) will replace it with
a small learned ranking model.

Wire shape
----------

Upstream produces:

  * ``RetrievalIntent.intent`` (a question primitive string — see
    ``services.sage.intent_inferer.IntentPrimitive``), and a coarse
    ``intent_kind`` label (e.g. "DEPENDENCY", "OWNERSHIP",
    "CONTRADICTION", "PATTERN", "ACTION", "CONSTRAINT",
    "FALSIFICATION", "BOTTLENECK"). Either may be passed; we accept
    both spellings.
  * A candidate graph edge with type (``edge_kind`` in the registry —
    see ``lib/shared/edge_registry.py``), confidence, last-update
    timestamp, source/target ``ModelStructuralFeatures`` (Phase 4 /
    migration 0050), per-edge ``EdgeStructuralFeatures``, trust tier
    of the source observation, and an access flag.

Downstream consumes:

  * A ``GateScore`` per edge with the final clamped multiplier and a
    breakdown by named factor for debuggability + traceability into
    Phase 7's subgraph selector.

The formula (v1, doc §1658-1668):

    score = relation_type_weight(edge_type, primitive)
          * trust_weight(trust_tier)
          * freshness_weight(updated_at)
          * role_compatibility(edge_type, primitive)
          * bridge_bonus(features.bridge_score)
          * hub_penalty(features.hub_score, primitive)
          * access_allowed
    clamp to [0, 1]
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Duck-typed at runtime so this module does not pull in pydantic /
    # the structural_features package just to be importable.
    from services.sage.structural_features.types import (
        EdgeStructuralFeatures,
        ModelStructuralFeatures,
    )


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GateInputs:
    """All the per-edge signals the scorer needs.

    Kept as a plain dataclass so callers can build one without
    importing pydantic. Optional structural-feature handles are
    permitted because Phase 4 may not have recomputed every Model
    yet; missing features degrade to neutral 1.0 multipliers rather
    than killing the edge.
    """

    edge_type: str
    edge_confidence: float
    edge_updated_at: datetime
    source_features: "ModelStructuralFeatures | None"
    target_features: "ModelStructuralFeatures | None"
    edge_features: "EdgeStructuralFeatures | None"
    source_trust_tier: str | None
    access_allowed: bool


@dataclass(frozen=True, slots=True)
class GateScore:
    """Output of one gate evaluation.

    ``components`` always contains every named factor (even when 1.0)
    so traces remain debuggable. ``reason`` is a compact one-line
    string summarising the dominant downgrade(s) and bonus(es).
    """

    score: float
    components: dict[str, float]
    reason: str


# ---------------------------------------------------------------------
# Default heuristic tables
# ---------------------------------------------------------------------

# Canonical primitive labels we recognise. We accept both the coarse
# ``intent_kind`` taxonomy (DEPENDENCY/OWNERSHIP/...) and the finer
# ``IntentPrimitive`` strings emitted by the intent inferer. Anything
# unrecognised falls through to a neutral 1.0 weight.
_PRIMITIVE_ALIASES: dict[str, str] = {
    # Coarse labels — already canonical.
    "DEPENDENCY": "DEPENDENCY",
    "OWNERSHIP": "OWNERSHIP",
    "CONTRADICTION": "CONTRADICTION",
    "PATTERN": "PATTERN",
    "ACTION": "ACTION",
    "CONSTRAINT": "CONSTRAINT",
    "BOTTLENECK": "CONSTRAINT",
    "FALSIFICATION": "FALSIFICATION",
    # IntentPrimitive (services.sage.intent_inferer) → coarse label.
    "test_dependency": "DEPENDENCY",
    "find_owner": "OWNERSHIP",
    "find_counterevidence": "CONTRADICTION",
    "find_pattern_recurrence": "PATTERN",
    "find_action_candidates": "ACTION",
    "find_blocking_resource": "CONSTRAINT",
    "test_falsification": "FALSIFICATION",
    "find_authoritative_source": "OWNERSHIP",
    "find_active_commitment": "DEPENDENCY",
    "find_goal_impact": "DEPENDENCY",
}


# (primitive, edge_type) → multiplier. Range 0.2..1.5. Anything not in
# the table defaults to 1.0 (neutral). Edge kinds are the registry
# canonical set (see lib/shared/edge_registry.py; mirrored in
# migration 0031).
_RELATION_TYPE_TABLE: dict[tuple[str, str], float] = {
    # DEPENDENCY: prefer the edges that actually express a dep edge
    # in the graph (blocks/causes/enables/predicts/supports). Down-
    # weight contradictions and pure co-occurrence noise.
    ("DEPENDENCY", "blocks"): 1.4,
    ("DEPENDENCY", "causes"): 1.3,
    ("DEPENDENCY", "enables"): 1.25,
    ("DEPENDENCY", "predicts"): 1.1,
    ("DEPENDENCY", "supports"): 1.05,
    ("DEPENDENCY", "contradicts"): 0.3,
    ("DEPENDENCY", "weakens"): 0.5,
    ("DEPENDENCY", "co_occurs_with"): 0.6,
    ("DEPENDENCY", "analogous_to"): 0.7,
    # OWNERSHIP: actor/team-style edges. The graph proper does not yet
    # have a dedicated "owns" edge_kind, so the closest registry kinds
    # are explains/instance_of (instance_of is intentionally weak for
    # ownership — it's a categorisation edge, not an authorship one).
    ("OWNERSHIP", "explains"): 1.2,
    ("OWNERSHIP", "supports"): 1.05,
    ("OWNERSHIP", "instance_of"): 0.4,
    ("OWNERSHIP", "co_occurs_with"): 0.5,
    ("OWNERSHIP", "analogous_to"): 0.5,
    # CONTRADICTION: counterevidence questions live here. Boost the
    # actually-contradictory edges, downweight supportive ones.
    ("CONTRADICTION", "contradicts"): 1.5,
    ("CONTRADICTION", "weakens"): 1.3,
    ("CONTRADICTION", "same_issue_as"): 1.1,
    ("CONTRADICTION", "supports"): 0.3,
    ("CONTRADICTION", "contributes_to_resolution"): 0.5,
    # PATTERN: recurrence / analogy-style edges.
    ("PATTERN", "instance_of"): 1.4,
    ("PATTERN", "analogous_to"): 1.3,
    ("PATTERN", "co_occurs_with"): 1.15,
    ("PATTERN", "same_issue_as"): 1.1,
    ("PATTERN", "contradicts"): 0.5,
    # ACTION: forward-causation edges (enables/causes/blocks),
    # resolution edges. Things that map to "what should we do next?".
    ("ACTION", "enables"): 1.3,
    ("ACTION", "contributes_to_resolution"): 1.3,
    ("ACTION", "causes"): 1.15,
    ("ACTION", "blocks"): 1.1,
    ("ACTION", "alternative_to"): 1.1,
    ("ACTION", "contradicts"): 0.5,
    # CONSTRAINT / BOTTLENECK: which resources block us. Same blocker
    # bias as DEPENDENCY but also values early_warning_for.
    ("CONSTRAINT", "blocks"): 1.5,
    ("CONSTRAINT", "causes"): 1.25,
    ("CONSTRAINT", "enables"): 1.1,
    ("CONSTRAINT", "early_warning_for"): 1.2,
    ("CONSTRAINT", "contradicts"): 0.6,
    ("CONSTRAINT", "co_occurs_with"): 0.7,
    # FALSIFICATION: predictions and contradictions are the load-
    # bearing edges; supports/contributes_to_resolution actively hurt.
    ("FALSIFICATION", "predicts"): 1.4,
    ("FALSIFICATION", "contradicts"): 1.4,
    ("FALSIFICATION", "weakens"): 1.2,
    ("FALSIFICATION", "supports"): 0.4,
    ("FALSIFICATION", "contributes_to_resolution"): 0.5,
}


# Role-compatibility table: a stricter, binary "is this edge_type
# semantically eligible for this primitive at all?" check. Anything
# not eligible gets multiplied by ``_ROLE_INCOMPATIBLE_PENALTY``.
# Sets are intentionally loose — false negatives here are worse than
# false positives because the relation_type table already does the
# fine-grained ranking.
_ROLE_COMPATIBLE: dict[str, frozenset[str]] = {
    "DEPENDENCY": frozenset({
        "blocks", "causes", "enables", "predicts", "supports",
        "early_warning_for", "contributes_to_resolution",
    }),
    "OWNERSHIP": frozenset({
        "explains", "supports", "instance_of",
        "contributes_to_resolution",
    }),
    "CONTRADICTION": frozenset({
        "contradicts", "weakens", "same_issue_as", "supports",
        "contributes_to_resolution",
    }),
    "PATTERN": frozenset({
        "instance_of", "analogous_to", "co_occurs_with",
        "same_issue_as", "supports", "predicts",
    }),
    "ACTION": frozenset({
        "blocks", "causes", "enables", "contributes_to_resolution",
        "alternative_to", "supports",
    }),
    "CONSTRAINT": frozenset({
        "blocks", "causes", "enables", "early_warning_for",
        "contributes_to_resolution",
    }),
    "FALSIFICATION": frozenset({
        "predicts", "contradicts", "weakens", "supports",
    }),
}

_ROLE_INCOMPATIBLE_PENALTY: float = 0.5


_TRUST_WEIGHTS: dict[str, float] = {
    "authoritative": 1.2,
    "evidenced": 1.0,
    "asserted": 0.85,
    "unverified": 0.6,
}
_TRUST_DEFAULT: float = 0.7  # for None / unknown


# Freshness bands, in days. Tuple of (max_age_days, weight). The
# scorer walks the list in order and picks the first band the edge
# falls into.
_FRESHNESS_BANDS: tuple[tuple[float, float], ...] = (
    (7.0, 1.1),
    (30.0, 1.0),
    (90.0, 0.85),
    (365.0, 0.6),
)
_FRESHNESS_STALE: float = 0.4  # > 365 days


# Hub-penalty primitives: which primitives should NOT punish hubs.
# CONSTRAINT/BOTTLENECK questions explicitly *want* the resource hub
# (it's likely the bottleneck itself); ACTION questions want to find
# high-leverage actions which often sit at hubs.
_HUB_FRIENDLY: frozenset[str] = frozenset({"CONSTRAINT", "ACTION"})


# Bonus weight names — used to keep ``components`` keys stable across
# versions so dashboards / SQL filters don't have to chase renames.
_FACTOR_NAMES: tuple[str, ...] = (
    "relation_type_weight",
    "trust_weight",
    "freshness_weight",
    "role_compatibility",
    "bridge_bonus",
    "hub_penalty",
    "access_allowed",
)


# Final clamp range — kept tight per spec (gate is a [0, 1]
# multiplier on propagated activation).
_CLAMP_MIN: float = 0.0
_CLAMP_MAX: float = 1.0


# ---------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------


class StructuralGateScorer:
    """Deterministic v1 heuristic gate scorer.

    Stateless apart from the (optional) weight overrides. Safe to
    share across requests / event loops. Construct once at startup,
    pass to whatever stage walks candidate edges (Phase 6 propagator,
    Phase 7 subgraph selector).

    ``weights`` lets callers override any of the named multiplier
    tables by passing one of the following keys:

      * ``"trust_weight"`` — overrides ``_TRUST_WEIGHTS`` (merge).
      * ``"relation_type_weight"`` — dict of ``"PRIMITIVE:edge_kind"``
        strings → float; merged on top of the defaults.
      * ``"bridge_coefficient"`` — overrides the 0.4 multiplier in
        ``bridge_bonus(b) = 1 + coeff * b``.
      * ``"hub_max_penalty"`` — overrides the 0.6 cap in
        ``hub_penalty(h) = 1 - min(cap, cap * h)``.
      * ``"role_incompatible_penalty"`` — overrides the 0.5 floor.
      * ``"trust_default"`` — overrides the None/unknown-tier weight.
      * ``"freshness_stale"`` — overrides the > 365-day weight.
    """

    def __init__(self, *, weights: dict[str, float | dict] | None = None) -> None:
        w = weights or {}

        # Trust table — start from defaults, then merge any overrides.
        trust_override = w.get("trust_weight") or {}
        if not isinstance(trust_override, dict):
            raise TypeError(
                "weights['trust_weight'] must be a dict[str, float]"
            )
        self._trust_table: dict[str, float] = {
            **_TRUST_WEIGHTS,
            **{str(k): float(v) for k, v in trust_override.items()},
        }
        self._trust_default: float = float(
            w.get("trust_default", _TRUST_DEFAULT)
        )

        # Relation-type table — accepts a flat dict keyed by
        # "PRIMITIVE:edge_kind" strings for ergonomics.
        rel_override = w.get("relation_type_weight") or {}
        if not isinstance(rel_override, dict):
            raise TypeError(
                "weights['relation_type_weight'] must be a dict"
            )
        self._relation_table: dict[tuple[str, str], float] = dict(
            _RELATION_TYPE_TABLE
        )
        for k, v in rel_override.items():
            if isinstance(k, tuple) and len(k) == 2:
                prim, kind = k
            elif isinstance(k, str) and ":" in k:
                prim, kind = k.split(":", 1)
            else:
                raise ValueError(
                    "relation_type_weight keys must be 'PRIMITIVE:edge_kind' "
                    f"or 2-tuples; got {k!r}"
                )
            self._relation_table[(str(prim).upper(), str(kind))] = float(v)

        self._bridge_coefficient: float = float(
            w.get("bridge_coefficient", 0.4)
        )
        self._hub_max_penalty: float = float(
            w.get("hub_max_penalty", 0.6)
        )
        self._role_incompatible_penalty: float = float(
            w.get("role_incompatible_penalty", _ROLE_INCOMPATIBLE_PENALTY)
        )
        self._freshness_stale: float = float(
            w.get("freshness_stale", _FRESHNESS_STALE)
        )

    # ------------------------------------------------------------- public

    def score(
        self,
        *,
        gate_inputs: GateInputs,
        question_primitive: str,
        intent_kind: str | None = None,
        now: datetime | None = None,
    ) -> GateScore:
        """Compute a multiplicative gate score for a single edge.

        ``intent_kind`` is preferred if provided (it's the coarser
        and more directly tabulated label); otherwise we fall back to
        ``question_primitive`` and try to resolve it via the alias
        table. Unknown primitives degrade gracefully to a neutral
        weight rather than raising.

        ``now`` is overridable for deterministic tests.
        """
        primitive = _canonical_primitive(intent_kind or question_primitive)
        ts_now = now or datetime.now(timezone.utc)

        components: dict[str, float] = {name: 1.0 for name in _FACTOR_NAMES}

        components["relation_type_weight"] = self._relation_type_weight(
            primitive, gate_inputs.edge_type
        )
        components["trust_weight"] = self._trust_weight(
            gate_inputs.source_trust_tier
        )
        components["freshness_weight"] = self._freshness_weight(
            gate_inputs.edge_updated_at, ts_now
        )
        components["role_compatibility"] = self._role_compatibility(
            primitive, gate_inputs.edge_type
        )
        components["bridge_bonus"] = self._bridge_bonus(
            gate_inputs.edge_features, gate_inputs.source_features,
            gate_inputs.target_features,
        )
        components["hub_penalty"] = self._hub_penalty(
            gate_inputs.source_features, gate_inputs.target_features,
            primitive,
        )
        components["access_allowed"] = 1.0 if gate_inputs.access_allowed else 0.0

        raw = 1.0
        for v in components.values():
            raw *= v
        score = _clamp(raw, _CLAMP_MIN, _CLAMP_MAX)

        return GateScore(
            score=score,
            components=components,
            reason=_format_reason(components, primitive),
        )

    # ----------------------------------------------------------- factors

    def _relation_type_weight(self, primitive: str, edge_type: str) -> float:
        return self._relation_table.get((primitive, edge_type), 1.0)

    def _trust_weight(self, tier: str | None) -> float:
        if tier is None:
            return self._trust_default
        return self._trust_table.get(tier, self._trust_default)

    def _freshness_weight(self, updated_at: datetime, now: datetime) -> float:
        # Normalise tz so subtraction is always defined. Naive ts get
        # interpreted as UTC (matches how `created_at TIMESTAMPTZ` is
        # surfaced by asyncpg in this repo).
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (now - updated_at).total_seconds() / 86400.0)
        for max_age, weight in _FRESHNESS_BANDS:
            if age_days <= max_age:
                return weight
        return self._freshness_stale

    def _role_compatibility(self, primitive: str, edge_type: str) -> float:
        eligible = _ROLE_COMPATIBLE.get(primitive)
        if eligible is None:
            # Unknown primitive — don't punish; downstream sees neutral.
            return 1.0
        if edge_type in eligible:
            return 1.0
        return self._role_incompatible_penalty

    def _bridge_bonus(
        self,
        edge_features: "EdgeStructuralFeatures | None",
        source_features: "ModelStructuralFeatures | None",
        target_features: "ModelStructuralFeatures | None",
    ) -> float:
        # Prefer per-edge bridge_likelihood when present (it is the
        # purpose-built signal); else fall back to the max of the two
        # endpoints' bridge_score, which the structural feature
        # compute layer derives from betweenness.
        b: float | None = None
        if edge_features is not None:
            b = _safe_float(getattr(edge_features, "bridge_likelihood", None))
        if b is None:
            sb = _safe_float(_get_feature(source_features, "bridge_score"))
            tb = _safe_float(_get_feature(target_features, "bridge_score"))
            candidates = [v for v in (sb, tb) if v is not None]
            b = max(candidates) if candidates else None
        if b is None:
            return 1.0
        # Clamp the input bridge score to [0, 1] before applying the
        # coefficient so a buggy upstream can't pump the multiplier.
        b = max(0.0, min(1.0, b))
        return 1.0 + self._bridge_coefficient * b

    def _hub_penalty(
        self,
        source_features: "ModelStructuralFeatures | None",
        target_features: "ModelStructuralFeatures | None",
        primitive: str,
    ) -> float:
        if primitive in _HUB_FRIENDLY:
            # CONSTRAINT/BOTTLENECK/ACTION questions explicitly want
            # hubs (the hub *is* the constraint, or it's where the
            # high-leverage action sits).
            return 1.0
        sh = _safe_float(_get_feature(source_features, "hub_score"))
        th = _safe_float(_get_feature(target_features, "hub_score"))
        hubs = [v for v in (sh, th) if v is not None]
        if not hubs:
            return 1.0
        h = max(0.0, min(1.0, max(hubs)))
        return 1.0 - min(self._hub_max_penalty, self._hub_max_penalty * h)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _canonical_primitive(label: str | None) -> str:
    """Resolve a raw primitive/intent string to a canonical coarse
    label. Unknowns return uppercased input so the relation table can
    still match user-supplied custom primitives.
    """
    if not label:
        return "UNKNOWN"
    key = label.strip()
    if key in _PRIMITIVE_ALIASES:
        return _PRIMITIVE_ALIASES[key]
    upper = key.upper()
    if upper in _PRIMITIVE_ALIASES:
        return _PRIMITIVE_ALIASES[upper]
    return upper


def _safe_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _get_feature(features: object, name: str) -> object:
    """Duck-typed attribute fetch so this module works with the
    Pydantic ``ModelStructuralFeatures`` *and* with lightweight test
    doubles (SimpleNamespace, plain dataclasses, dicts).
    """
    if features is None:
        return None
    if isinstance(features, dict):
        return features.get(name)
    return getattr(features, name, None)


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _format_reason(components: dict[str, float], primitive: str) -> str:
    """Compact human-readable explanation.

    Picks out the biggest downgrade(s) (< 0.95) and the biggest
    bonus(es) (> 1.05) so the line stays short. Always ends with a
    primitive tag so a grep over logs is easy.
    """
    if components.get("access_allowed", 1.0) == 0.0:
        return f"access denied (primitive={primitive})"

    parts: list[str] = []
    # Order: highlight role / hub / freshness / trust downgrades, then
    # relation_type/bridge bonuses. Threshold-based filter keeps the
    # line one short sentence in the common case.
    for name in (
        "role_compatibility",
        "hub_penalty",
        "freshness_weight",
        "trust_weight",
    ):
        v = components.get(name, 1.0)
        if v < 0.95:
            parts.append(f"{name} {v:.2f}")
    for name in ("relation_type_weight", "bridge_bonus"):
        v = components.get(name, 1.0)
        if v > 1.05:
            parts.append(f"{name} +{(v - 1.0):.2f}")
        elif v < 0.95:
            parts.append(f"{name} {v:.2f}")

    if not parts:
        return f"neutral (primitive={primitive})"
    return ", ".join(parts) + f" (primitive={primitive})"


__all__ = [
    "GateInputs",
    "GateScore",
    "StructuralGateScorer",
]
